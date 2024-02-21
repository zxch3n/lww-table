mod delta_rle;
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
    value::Value,
    LwwDb,
};

use self::delta_rle::DeltaRleDecoder;

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

    fn get_id<Q: ?Sized>(&mut self, value: &Q) -> usize
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
                        if let Some(row) = table.rows.get(row_name) {
                            for (col, value) in row.map.iter() {
                                if !from.includes(value.id) {
                                    ans.push(EncodedOp {
                                        table: str_pool.get_id(table_name),
                                        row: Some(str_pool.get_id(row_name)),
                                        col: Some(str_pool.get_id(col)),
                                        value: value.value.clone(),
                                        peer_idx: peer_pool.get_id(&value.id.peer),
                                        lamport: value.id.lamport,
                                    });
                                }
                            }
                        }
                    }
                }
                crate::oplog::Op::DeleteTable { table } => ans.push(EncodedOp {
                    table: str_pool.get_id(table),
                    row: None,
                    col: None,
                    value: Value::Deleted,
                    peer_idx: peer_pool.get_id(&id.peer),
                    lamport: id.lamport,
                }),
                crate::oplog::Op::DeleteRow { table, row } => ans.push(EncodedOp {
                    table: str_pool.get_id(table),
                    row: Some(str_pool.get_id(row)),
                    col: None,
                    value: Value::Deleted,
                    peer_idx: peer_pool.get_id(&id.peer),
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
                (None, None) => self.delete_table(table, Some(id)),
                (Some(row), None) => self.delete_row(table, row, Some(id)),
                (Some(row), Some(col)) => self.set(table, row, col, value, Some(id)),
                (None, Some(_)) => unreachable!(),
            },
            _ => self.set(table, row.unwrap(), col.unwrap(), value, Some(id)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        let mut db = LwwDb::new();
        db.set("table", "a", "b", "value", None);
        db.set("table", "a", "c", "value", None);
        db.set("table", "a", "a", 123.0, None);
        db.set("table", "b", "a", 124.0, None);
        db.set("meta", "meta", "name", "Bob", None);
        db.set("meta", "meta", "Date", "2024/02/21", None);
        let data = db.export_updates(Default::default());
        let mut new_db = LwwDb::new();
        new_db.import_updates(&data);
        assert!(db.check_eq(&new_db));
    }
}
