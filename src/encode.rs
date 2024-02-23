mod bool_rle;
mod delta_rle;
mod table_snapshot;
use itertools::izip;

use std::{
    borrow::{Borrow, Cow},
    hash::Hash,
    sync::Arc,
};

use fxhash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::{
    clock::{Lamport, OpId, Peer, VectorClock},
    encode::delta_rle::DeltaRleEncoder,
    oplog::OpLogBuilder,
    table::RowValue,
    value::Value,
    LwwDb,
};

use self::{
    delta_rle::DeltaRleDecoder,
    table_snapshot::{decode_snapshot, encode_snapshot},
};

#[derive(Serialize, Deserialize)]
struct Final<'a> {
    str: Vec<Box<str>>,
    peers: Vec<Peer>,
    #[serde(borrow)]
    table: Cow<'a, [u8]>,
    #[serde(borrow)]
    row: Cow<'a, [u8]>,
    #[serde(borrow)]
    col: Cow<'a, [u8]>,
    #[serde(borrow)]
    value: Cow<'a, [u8]>,
    #[serde(borrow)]
    peer_idx: Cow<'a, [u8]>,
    #[serde(borrow)]
    lamport: Cow<'a, [u8]>,
}

#[derive(Serialize, Deserialize)]
struct EncodedSnapshot<'a> {
    peers: Vec<Peer>,
    #[serde(borrow)]
    tables: Vec<EncodedTable<'a>>,
}

#[derive(Serialize, Deserialize)]
struct EncodedTable<'a> {
    str: SmolStr,
    #[serde(borrow)]
    table: Cow<'a, [u8]>,
}

struct Register<T> {
    pool: Vec<T>,
    to_id: FxHashMap<T, usize>,
}

impl<T: Hash + Eq + Clone> Register<T> {
    fn new() -> Self {
        Self {
            pool: Vec::new(),
            to_id: FxHashMap::default(),
        }
    }

    fn register<Q: ?Sized>(&mut self, value: &Q) -> usize
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T>,
    {
        if let Some(&id) = self.to_id.get(value) {
            id
        } else {
            let id = self.pool.len();
            let v: T = value.to_owned();
            self.pool.push(v.clone());
            self.to_id.insert(v, id);
            id
        }
    }

    fn finish(self) -> Vec<T> {
        self.pool
    }
}

impl LwwDb {
    pub fn export_updates(&self, from: VectorClock) -> Vec<u8> {
        let mut str_pool: Register<Arc<str>> = Register::new();
        let mut peer_pool: Register<Peer> = Register::new();
        let mut table_en = DeltaRleEncoder::new();
        let mut row_en = DeltaRleEncoder::new();
        let mut col_en = DeltaRleEncoder::new();
        let mut peer_en = DeltaRleEncoder::new();
        let mut lamport_en = DeltaRleEncoder::new();
        let deleted_v = Value::Deleted;
        let mut values_en: Vec<&Value> = Vec::new();
        let mut updated_rows = FxHashSet::default();
        for (id, op) in self.oplog.iter_from(from.clone()) {
            debug_assert!(!from.includes(id));
            match op {
                crate::oplog::Op::Update {
                    table: table_name,
                    row: row_name,
                } => {
                    updated_rows.insert((table_name, row_name));
                }
                crate::oplog::Op::DeleteTable { table } => {
                    table_en.push(str_pool.register(table) as i64);
                    row_en.push(0);
                    col_en.push(0);
                    values_en.push(&deleted_v);
                    peer_en.push(peer_pool.register(&id.peer) as i64);
                    lamport_en.push(id.lamport as i64);
                }
                crate::oplog::Op::DeleteRow { table, row } => {
                    table_en.push(str_pool.register(table) as i64);
                    row_en.push(str_pool.register(row) as i64 + 1);
                    col_en.push(0);
                    values_en.push(&deleted_v);
                    peer_en.push(peer_pool.register(&id.peer) as i64);
                    lamport_en.push(id.lamport as i64);
                }
            }
        }

        for (table_name, row_name) in updated_rows {
            if let Some(table) = self.tables.get(&**table_name) {
                for RowValue {
                    col_name,
                    id,
                    value,
                } in table.iter_row(row_name)
                {
                    if !from.includes(id) {
                        let col_name: Arc<str> = col_name.into();
                        table_en.push(str_pool.register(table_name) as i64);
                        row_en.push(str_pool.register(row_name) as i64 + 1);
                        col_en.push(str_pool.register(&col_name) as i64 + 1);
                        values_en.push(value);
                        peer_en.push(peer_pool.register(&id.peer) as i64);
                        lamport_en.push(id.lamport as i64);
                    }
                }
            }
        }

        let f = Final {
            str: str_pool
                .finish()
                .into_iter()
                .map(|s| (*s).to_string().into_boxed_str())
                .collect(),
            peers: peer_pool.finish(),
            table: Cow::Owned(table_en.finish()),
            row: Cow::Owned(row_en.finish()),
            col: Cow::Owned(col_en.finish()),
            value: Cow::Owned(postcard::to_allocvec(&values_en).unwrap()),
            peer_idx: Cow::Owned(peer_en.finish()),
            lamport: Cow::Owned(lamport_en.finish()),
        };

        let ans = postcard::to_allocvec(&f).unwrap();
        zstd::encode_all(&mut ans.as_slice(), 0).unwrap()
    }

    pub fn import_updates(&mut self, bytes: &[u8]) {
        let bytes = zstd::decode_all(bytes).unwrap();
        let f = postcard::from_bytes::<Final>(&bytes).unwrap();
        let peers = f.peers;
        let str = f.str;
        let values = postcard::from_bytes::<Vec<Value>>(&f.value).unwrap();
        let table = DeltaRleDecoder::new(&f.table);
        let row = DeltaRleDecoder::new(&f.row);
        let col = DeltaRleDecoder::new(&f.col);
        let lamport = DeltaRleDecoder::new(&f.lamport);
        let peer_idx = DeltaRleDecoder::new(&f.peer_idx);

        for (i, (t, r, c, peer_idx, l)) in izip!(table, row, col, peer_idx, lamport).enumerate() {
            let table = &str[t as usize];
            let row = if r == 0 {
                None
            } else {
                Some(&str[r as usize - 1])
            };
            let col = if c == 0 {
                None
            } else {
                Some(&str[c as usize - 1])
            };
            let value = values[i].clone();
            let peer = peers[peer_idx as usize];
            let id = OpId {
                peer,
                lamport: l as Lamport,
            };

            self.apply_op(id, table, row.map(|x| &**x), col.map(|x| &**x), value);
        }
    }

    pub fn export_snapshot(&self) -> Vec<u8> {
        let mut ans: Vec<EncodedTable> = Vec::new();
        let mut peer_pool: Register<Peer> = Register::new();
        for (name, table) in self.iter_tables() {
            ans.push(EncodedTable {
                str: name.clone(),
                table: Cow::Owned(encode_snapshot(table, &mut peer_pool)),
            })
        }

        let encoded = EncodedSnapshot {
            peers: peer_pool.finish(),
            tables: ans,
        };

        postcard::to_allocvec(&encoded).unwrap()
    }

    pub fn from_snapshot(data: &[u8]) -> Self {
        let encoded: EncodedSnapshot = postcard::from_bytes(data).unwrap();
        let mut db = LwwDb::new();
        let mut oplog_builder = OpLogBuilder::default();
        for table in encoded.tables {
            let v = decode_snapshot(&table.table, &encoded.peers, |c| match c {
                table_snapshot::Change::DelTable { id } => {
                    oplog_builder.record_delete_table(id, table.str.clone());
                }
                table_snapshot::Change::DelRow { row, id } => {
                    oplog_builder.record_delete_row(id, table.str.clone(), row.clone());
                }
                table_snapshot::Change::Value { row, id } => {
                    oplog_builder.record_update(id, table.str.clone(), row.clone());
                }
            });
            db.tables.insert(table.str, v);
        }

        db.oplog = oplog_builder.build();
        db
    }

    fn apply_op(
        &mut self,
        id: OpId,
        table: &str,
        row: Option<&str>,
        col: Option<&str>,
        value: Value,
    ) {
        match value {
            Value::Deleted => match (row, col) {
                (None, None) => self.delete_table_(table, Some(id)),
                (Some(row), None) => self.delete_row_(table, row, Some(id)),
                (Some(row), Some(col)) => self.set_(table, row, col, value, Some(id)),
                (None, Some(_)) => unreachable!(),
            },
            _ => self.set_(table, row.unwrap(), col.unwrap(), value, Some(id)),
        }
    }
}

#[cfg(test)]
mod test_encode_from {
    use super::*;

    #[test]
    fn test_basic() {
        let mut db = LwwDb::new();
        db.set_("table", "a", "b", "value", None);
        db.set_("table", "a", "c", "value", None);
        db.set_("table", "a", "a", 123.0, None);
        db.set_("table", "b", "a", 124.0, None);
        db.set_("meta", "meta", "name", "Bob", None);
        db.set_("meta", "meta", "Date", "2024/02/21", None);
        let data = db.export_updates(Default::default());
        let mut new_db = LwwDb::new();
        new_db.import_updates(&data);
        assert!(
            db.table_eq(&mut new_db),
            "original: {}\nnew: {}",
            db,
            new_db
        );
        let mut c_db = LwwDb::new();
        c_db.import_updates(&new_db.export_updates(Default::default()));
        assert!(db.table_eq(&mut c_db));
    }

    #[test]
    fn test_delete() {
        let mut db = LwwDb::new();
        db.set("table", "a", "b", "value");
        db.set("table", "a", "c", "value");
        db.delete_row("table", "a");
        db.set("table", "a", "a", 123.0);
        db.set("table", "b", "a", 124.0);
        db.set("meta", "meta", "name", "Bob");
        db.set("meta", "meta", "Date", "2024/02/21");
        let data = db.export_updates(Default::default());
        let mut new_db = LwwDb::new();
        new_db.import_updates(&data);
        // println!("{}", &db);
        // println!("{}", &new_db);
        assert!(
            db.table_eq(&mut new_db),
            "original: {}\nnew: {}",
            db,
            new_db
        );
        let mut c_db = LwwDb::new();
        c_db.import_updates(&new_db.export_updates(Default::default()));
        assert!(db.table_eq(&mut c_db));
    }

    #[test]
    fn test_snapshot_basic() {
        let mut db = LwwDb::new();
        db.set_("table", "a", "b", "value", None);
        db.set_("table", "a", "c", "value", None);
        db.set_("table", "a", "a", 123.0, None);
        db.set_("table", "b", "a", 124.0, None);
        db.set_("meta", "meta", "name", "Bob", None);
        db.set_("meta", "meta", "Date", "2024/02/21", None);
        let data = db.export_snapshot();
        let mut new_db = LwwDb::from_snapshot(&data);
        println!("{}", &db);
        println!("{}", &new_db);
        assert!(db.table_eq(&mut new_db));
        let mut c_db = LwwDb::new();
        c_db.import_updates(&new_db.export_updates(Default::default()));
        assert!(db.table_eq(&mut c_db));
    }
}
