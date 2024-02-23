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

impl OpId {
    pub fn new(lamport: Lamport, peer: Peer) -> Self {
        Self { lamport, peer }
    }
}

/// Inclusive range of [OpId].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VectorClock {
    pub(crate) map: FxHashMap<Peer, Lamport>,
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

    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).unwrap()
    }

    pub fn decode(encoded: &[u8]) -> Self {
        postcard::from_bytes(encoded).unwrap()
    }
}
