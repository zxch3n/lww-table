use std::ops::{Deref, DerefMut};

use fxhash::FxHashMap;
use serde::{Deserialize, Serialize};

pub type Lamport = u32;
pub type Peer = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OpId {
    pub lamport: Lamport,
    pub peer: Peer,
}

#[derive(Debug, Clone, Default)]
pub struct VectorClock {
    map: FxHashMap<Peer, Lamport>,
}

impl Deref for VectorClock {
    type Target = FxHashMap<Peer, Lamport>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl DerefMut for VectorClock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl VectorClock {
    pub fn new() -> Self {
        Self {
            map: FxHashMap::default(),
        }
    }

    pub fn extend_to_include(&mut self, op: OpId) {
        let lamport = self.map.entry(op.peer).or_insert(0);
        *lamport = (*lamport).max(op.lamport);
    }

    pub fn includes(&self, op: OpId) -> bool {
        match self.map.get(&op.peer) {
            Some(lamport) => *lamport >= op.lamport,
            None => false,
        }
    }
}
