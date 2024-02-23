mod bool_rle;
mod delta_rle;
mod table_snapshot;
use itertools::izip;

use std::{
    borrow::{Borrow, Cow},
    hash::Hash,
};

use fxhash::FxHashMap;
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
    str: Vec<SmolStr>,
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

struct EncodedOp {
    table: usize,
    row: Option<usize>,
    col: Option<usize>,
    value: Value,
    peer_idx: usize,
    lamport: Lamport,
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
        let mut str_pool: Register<SmolStr> = Register::new();
        let mut peer_pool: Register<Peer> = Register::new();
        let mut ans: Vec<EncodedOp> = Vec::new();
        for (id, op) in self.oplog.iter_from(from.clone()) {
            debug_assert!(!from.includes(id));
            match op {
                crate::oplog::Op::Update {
                    table: table_name,
                    row: row_name,
                } => {
                    if let Some(table) = self.tables.get(table_name) {
                        for RowValue {
                            col_name,
                            id,
                            value,
                        } in table.iter_row(row_name)
                        {
                            if !from.includes(id) {
                                let col_name: SmolStr = col_name.into();
                                ans.push(EncodedOp {
                                    table: str_pool.register(table_name),
                                    row: Some(str_pool.register(row_name)),
                                    col: Some(str_pool.register(&col_name)),
                                    value: value.clone(),
                                    peer_idx: peer_pool.register(&id.peer),
                                    lamport: id.lamport,
                                });
                            }
                        }
                    }
                }
                crate::oplog::Op::DeleteTable { table } => ans.push(EncodedOp {
                    table: str_pool.register(table),
                    row: None,
                    col: None,
                    value: Value::Deleted,
                    peer_idx: peer_pool.register(&id.peer),
                    lamport: id.lamport,
                }),
                crate::oplog::Op::DeleteRow { table, row } => ans.push(EncodedOp {
                    table: str_pool.register(table),
                    row: Some(str_pool.register(row)),
                    col: None,
                    value: Value::Deleted,
                    peer_idx: peer_pool.register(&id.peer),
                    lamport: id.lamport,
                }),
            }
        }

        ans.sort_by(|a, b| {
            a.table
                .cmp(&b.table)
                .then_with(|| a.row.cmp(&b.row).then_with(|| a.col.cmp(&b.col)))
        });

        let mut table = DeltaRleEncoder::new();
        let mut row = DeltaRleEncoder::new();
        let mut col = DeltaRleEncoder::new();
        let mut peer = DeltaRleEncoder::new();
        let mut lamport = DeltaRleEncoder::new();
        let mut value = Vec::with_capacity(ans.len());
        for op in ans {
            table.push(op.table as i64);
            row.push(op.row.map(|x| x as i64 + 1).unwrap_or(0));
            col.push(op.col.map(|x| x as i64 + 1).unwrap_or(0));
            peer.push(op.peer_idx as i64);
            lamport.push(op.lamport.into());
            value.push(op.value);
        }

        let f = Final {
            str: str_pool.finish(),
            peers: peer_pool.finish(),
            table: Cow::Owned(table.finish()),
            row: Cow::Owned(row.finish()),
            col: Cow::Owned(col.finish()),
            value: Cow::Owned(postcard::to_allocvec(&value).unwrap()),
            peer_idx: Cow::Owned(peer.finish()),
            lamport: Cow::Owned(lamport.finish()),
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

            self.apply_op(id, table, row, col, value);
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
        table: &SmolStr,
        row: Option<&SmolStr>,
        col: Option<&SmolStr>,
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
