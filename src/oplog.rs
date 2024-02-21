use std::collections::BTreeMap;

use fxhash::FxHashMap;
use smol_str::SmolStr;

use crate::clock::{Lamport, OpId, Peer, VectorClock};

#[derive(Debug, Clone, Default)]
pub(crate) struct OpLog {
    map: FxHashMap<Peer, BTreeMap<Lamport, Op>>,
    vv: VectorClock,
    max_lamport: Lamport,
}

#[derive(Debug, Clone)]
pub(crate) enum Op {
    Update { table: SmolStr, row: SmolStr },
    DeleteTable { table: SmolStr },
    DeleteRow { table: SmolStr, row: SmolStr },
}

#[derive(Debug, Clone, Default)]
pub struct TableRow {
    pub table: SmolStr,
    pub row: SmolStr,
}

impl OpLog {
    pub fn record_update(&mut self, id: OpId, table: SmolStr, row: SmolStr) {
        let peer = id.peer;
        let lamport = id.lamport;
        self.max_lamport = self.max_lamport.max(lamport);
        let map = self.map.entry(peer).or_default();
        map.insert(lamport, Op::Update { table, row });
        self.vv.extend_to_include(id);
    }

    pub(crate) fn next_lamport(&self) -> u32 {
        self.max_lamport + 1
    }

    pub(crate) fn record_delete_row(&mut self, id: OpId, table: SmolStr, row: SmolStr) {
        let peer = id.peer;
        let lamport = id.lamport;
        self.max_lamport = self.max_lamport.max(lamport);
        let map = self.map.entry(peer).or_default();
        map.insert(lamport, Op::DeleteRow { table, row });
        self.vv.extend_to_include(id);
    }

    pub(crate) fn record_delete_table(&mut self, id: OpId, table: SmolStr) {
        let peer = id.peer;
        let lamport = id.lamport;
        self.max_lamport = self.max_lamport.max(lamport);
        let map = self.map.entry(peer).or_default();
        map.insert(lamport, Op::DeleteTable { table });
        self.vv.extend_to_include(id);
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
