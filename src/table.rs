use std::collections::BTreeMap;

use fxhash::FxHashMap;

use crate::{
    clock::{Lamport, OpId, RowId},
    peer_pool::PeerIdx,
    str::{AnyStr, ColStr, StrIndex, StrPool},
    value::Value,
};

#[derive(Debug, Clone, Default)]
pub struct Table {
    row_id_to_idx: FxHashMap<StrIndex, usize>,
    row_ids: Vec<StrIndex>,
    cols: FxHashMap<StrIndex<ColStr>, Column>,
}

#[derive(Debug, Clone)]
pub struct Column {
    data: Vec<Value>,
    lamport: Vec<Lamport>,
    peer: Vec<PeerIdx>,
}

pub struct ColItem {
    pub s: StrIndex<ColStr>,
    pub value: Value,
    pub lamport: Lamport,
    pub peer_idx: PeerIdx,
}

impl Table {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_row(&mut self, row_id: StrIndex, lamport: Lamport, peer: PeerIdx) {
        self.row_id_to_idx.insert(row_id, self.row_ids.len());
        self.row_ids.push(row_id);
        for (_, col) in self.cols.iter_mut() {
            col.data.push(Value::Null);
            col.lamport.push(lamport);
            col.peer.push(peer);
        }
    }

    pub fn update(&mut self, row: StrIndex, iter: &mut dyn Iterator<Item = ColItem>) {
        let row_idx = self.row_id_to_idx[&row];
        for item in iter {
            let col = self.cols.get_mut(&item.s).unwrap();
            col.data[row_idx] = item.value;
            col.lamport[row_idx] = item.lamport;
            col.peer[row_idx] = item.peer_idx;
        }
    }

    pub fn sort(&mut self, pool: &StrPool) {
        let mut vecs: Vec<&mut dyn Swappable> = self
            .cols
            .values_mut()
            .flat_map(|c| {
                [
                    (&mut c.data) as &mut dyn Swappable,
                    &mut c.lamport,
                    &mut c.peer,
                ]
            })
            .collect();
        sort_vecs_based_on_first(&mut self.row_ids, |r| pool.get(*r), &mut vecs);
        self.row_id_to_idx = self
            .row_ids
            .iter()
            .enumerate()
            .map(|(i, r)| (*r, i))
            .collect();
    }
}

fn sort_vecs_based_on_first<T, U: Ord>(
    a: &mut [T],
    f: impl Fn(&T) -> U,
    vecs: &mut [&mut dyn Swappable],
) {
    let mut indexes: Vec<usize> = (0..a.len()).collect();

    // 根据 A 数组的值排序索引
    indexes.sort_by(|&i, &j| f(&a[i]).cmp(&f(&a[j])));

    // 重新排列 A 和 B，避免重新分配
    for i in 0..indexes.len() {
        // 将每个元素交换到正确的位置
        while indexes[i] != i {
            let target = indexes[i];
            a.swap(i, target);
            for v in vecs.iter_mut() {
                v.swap_(i, target);
            }
            indexes.swap(i, target);
        }
    }
}

trait Swappable {
    fn swap_(&mut self, i: usize, j: usize);
}

impl<T> Swappable for Vec<T> {
    fn swap_(&mut self, i: usize, j: usize) {
        self.swap(i, j)
    }
}
