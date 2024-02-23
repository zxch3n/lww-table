use std::{collections::BTreeMap, sync::Arc};

use fxhash::{FxHashMap, FxHashSet};
use smol_str::SmolStr;

use crate::clock::{Lamport, OpId, Peer, VectorClock};

#[derive(Debug, Clone, Default)]
pub(crate) struct OpLog {
    str_pool: FxHashSet<Arc<str>>,
    map: FxHashMap<Peer, BTreeMap<Lamport, Op>>,
    vv: VectorClock,
    max_lamport: Lamport,
}

#[derive(Debug, Clone)]
pub(crate) enum Op {
    Update { table: Arc<str>, row: Arc<str> },
    DeleteTable { table: Arc<str> },
    DeleteRow { table: Arc<str>, row: Arc<str> },
}

#[derive(Default)]
pub(crate) struct OpLogBuilder {
    str_pool: FxHashSet<Arc<str>>,
    ops: FxHashMap<Peer, Vec<(Lamport, Op)>>,
}

fn get_or_intern(pool: &mut FxHashSet<Arc<str>>, s: &str) -> Arc<str> {
    if let Some(s) = pool.get(s) {
        s.clone()
    } else {
        let s: Arc<str> = Arc::from(s);
        pool.insert(s.clone());
        s
    }
}

impl OpLogBuilder {
    pub fn record_update(&mut self, id: OpId, table: SmolStr, row: SmolStr) {
        let table = get_or_intern(&mut self.str_pool, &table);
        let row = get_or_intern(&mut self.str_pool, &row);
        self.ops
            .entry(id.peer)
            .or_default()
            .push((id.lamport, Op::Update { table, row }));
    }

    pub(crate) fn record_delete_row(&mut self, id: OpId, table: SmolStr, row: SmolStr) {
        let table = get_or_intern(&mut self.str_pool, &table);
        let row = get_or_intern(&mut self.str_pool, &row);
        self.ops
            .entry(id.peer)
            .or_default()
            .push((id.lamport, Op::DeleteRow { table, row }));
    }

    pub(crate) fn record_delete_table(&mut self, id: OpId, table: SmolStr) {
        let table = get_or_intern(&mut self.str_pool, &table);
        self.ops
            .entry(id.peer)
            .or_default()
            .push((id.lamport, Op::DeleteTable { table }));
    }

    pub(crate) fn build(self) -> OpLog {
        let map: FxHashMap<Peer, BTreeMap<Lamport, Op>> = self
            .ops
            .into_iter()
            .map(|(peer, ops)| (peer, BTreeMap::from_iter(ops)))
            .collect();
        let vv = VectorClock {
            map: map
                .iter()
                .map(|(peer, map)| {
                    let max = map.last_key_value().map(|(k, _)| *k).unwrap();
                    (*peer, max)
                })
                .collect(),
        };

        OpLog {
            str_pool: self.str_pool,
            max_lamport: vv.iter().map(|(_, v)| *v).max().unwrap_or(0),
            vv,
            map,
        }
    }
}

impl OpLog {
    pub fn record_update(&mut self, id: OpId, table: SmolStr, row: SmolStr) {
        let table = get_or_intern(&mut self.str_pool, &table);
        let row = get_or_intern(&mut self.str_pool, &row);
        let peer = id.peer;
        let lamport = id.lamport;
        self.max_lamport = self.max_lamport.max(lamport);
        let map = self.map.entry(peer).or_default();
        map.insert(lamport, Op::Update { table, row });
        self.vv.extend_to_include(id);
    }

    pub(crate) fn record_delete_row(&mut self, id: OpId, table: SmolStr, row: SmolStr) {
        let table = get_or_intern(&mut self.str_pool, &table);
        let row = get_or_intern(&mut self.str_pool, &row);
        let peer = id.peer;
        let lamport = id.lamport;
        self.max_lamport = self.max_lamport.max(lamport);
        let map = self.map.entry(peer).or_default();
        map.insert(lamport, Op::DeleteRow { table, row });
        self.vv.extend_to_include(id);
    }

    pub(crate) fn record_delete_table(&mut self, id: OpId, table: SmolStr) {
        let table = get_or_intern(&mut self.str_pool, &table);
        let peer = id.peer;
        let lamport = id.lamport;
        self.max_lamport = self.max_lamport.max(lamport);
        let map = self.map.entry(peer).or_default();
        map.insert(lamport, Op::DeleteTable { table });
        self.vv.extend_to_include(id);
    }

    pub(crate) fn next_lamport(&self) -> u32 {
        self.max_lamport + 1
    }

    pub(crate) fn iter_from(
        &self,
        from: crate::clock::VectorClock,
    ) -> impl Iterator<Item = (OpId, &Op)> + '_ {
        self.map.iter().flat_map(move |(peer, map)| {
            let start = *from.get(peer).unwrap_or(&0);
            map.range(start..).map(move |(lamport, op)| {
                let id = OpId {
                    peer: *peer,
                    lamport: *lamport,
                };

                (id, op)
            })
        })
    }
}
