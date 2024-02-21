use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::{
    clock::{Lamport, OpId, Peer},
    table::LwwTable,
    value::Value,
};

use super::{
    bool_rle::{BoolRleDecoder, BoolRleEncoder},
    delta_rle::{DeltaRleDecoder, DeltaRleEncoder},
    Register,
};
type PeerIdx = usize;

#[derive(Serialize, Deserialize)]
struct EncodedTable<'a> {
    table_deleted: Option<(PeerIdx, Lamport)>,
    row_names: Vec<SmolStr>,
    col_names: Vec<SmolStr>,

    /// BoolRle.
    /// This has the same length as rows * cols
    #[serde(borrow)]
    has_value: Cow<'a, [u8]>,

    /// All the values in the table
    ///
    /// Ordered by col and then by row.
    ///
    /// Only the one with value will encoded here.
    values: Vec<Value>,
    /// Only the one with value will encoded here
    #[serde(borrow)]
    lamport: Cow<'a, [u8]>,
    /// Only the one with value will encoded here
    #[serde(borrow)]
    peer_idx: Cow<'a, [u8]>,

    /// BoolRle.
    /// This has the same length as the row_names
    #[serde(borrow)]
    row_deleted: Cow<'a, [u8]>,
    deleted_peer_idx: Vec<PeerIdx>,
    deleted_lamport: Vec<Lamport>,
}

pub(crate) fn encode_snapshot(table: &LwwTable, peer_pool: &mut Register<Peer>) -> Vec<u8> {
    let mut has_value_encoder = BoolRleEncoder::new();
    let mut values = Vec::new();
    let mut lamport = DeltaRleEncoder::new();
    let mut peer_idx = DeltaRleEncoder::new();
    for (col_name, col) in table.cols.iter() {
        for row in table.rows.values() {
            if let Some(v) = row.map.get(col_name) {
                has_value_encoder.push(true);
                peer_idx.push(peer_pool.register(&v.id.peer) as i64);
                lamport.push(v.id.lamport as i64);
                values.push(v.value.clone());
            } else {
                has_value_encoder.push(false);
            }
        }
    }

    let mut row_deleted_encoder = BoolRleEncoder::new();
    let mut deleted_peer_idx = Vec::new();
    let mut deleted_lamport = Vec::new();
    for row in table.rows.values() {
        if let Some(d) = row.cleared_at {
            row_deleted_encoder.push(true);
            deleted_peer_idx.push(peer_pool.register(&d.peer));
            deleted_lamport.push(d.lamport);
        } else {
            row_deleted_encoder.push(false);
        }
    }

    let f = EncodedTable {
        table_deleted: table
            .removed
            .map(|x| (peer_pool.register(&x.peer), x.lamport)),
        row_names: table.rows.keys().cloned().collect(),
        col_names: table.cols.keys().cloned().collect(),
        has_value: Cow::Owned(has_value_encoder.finish()),
        values,
        lamport: Cow::Owned(lamport.finish()),
        peer_idx: Cow::Owned(peer_idx.finish()),
        row_deleted: Cow::Owned(row_deleted_encoder.finish()),
        deleted_peer_idx,
        deleted_lamport,
    };

    let data = postcard::to_allocvec(&f).unwrap();
    zstd::encode_all(&mut data.as_slice(), 0).unwrap()
}

pub(super) enum Change<'a> {
    DelTable { id: OpId },
    DelRow { row: &'a SmolStr, id: OpId },
    Value { row: &'a SmolStr, id: OpId },
}

pub(crate) fn decode_snapshot(
    encoded: &[u8],
    peers: &[Peer],
    mut on_change: impl FnMut(Change),
) -> LwwTable {
    let bytes = zstd::decode_all(encoded).unwrap();
    let f = postcard::from_bytes::<EncodedTable>(&bytes).unwrap();
    let mut table = LwwTable::new();
    if let Some(d) = f.table_deleted {
        let id = OpId {
            peer: peers[d.0],
            lamport: d.1,
        };
        on_change(Change::DelTable { id });
        table.removed = Some(id);
    }

    let mut has_value_iter = BoolRleDecoder::new(&f.has_value);
    let mut lampoort = DeltaRleDecoder::new(&f.lamport);
    let mut peer_idx = DeltaRleDecoder::new(&f.peer_idx);
    let mut value_iter = f.values.into_iter();
    for col in f.col_names {
        table
            .cols
            .entry(col.clone())
            .or_insert_with(|| crate::table::Col {
                name: col.clone().into(),
                num: 0,
            });

        let mut num = 0;
        for row in f.row_names.iter() {
            if has_value_iter.next().unwrap() {
                num += 1;
                let l = lampoort.next().unwrap();
                let p = peer_idx.next().unwrap();
                let v = value_iter.next().unwrap();
                let p = peers[p as usize];
                let id = OpId {
                    lamport: l as Lamport,
                    peer: p,
                };
                on_change(Change::Value { row, id });
                table.set(row, &col, v, id);
            }
        }

        table.cols.get_mut(&col).unwrap().num = num;
    }

    let mut row_deleted_iter = BoolRleDecoder::new(&f.row_deleted);
    for (row, (peer_idx, lamport)) in f
        .row_names
        .iter()
        .zip(f.deleted_peer_idx.iter().zip(f.deleted_lamport.iter()))
    {
        if row_deleted_iter.next().unwrap() {
            let id = OpId {
                peer: peers[*peer_idx],
                lamport: *lamport,
            };
            on_change(Change::DelRow { row, id });
            table.rows.get_mut(row).unwrap().cleared_at = Some(id);
        }
    }

    table
}
