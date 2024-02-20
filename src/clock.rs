use std::num::NonZeroU32;

use fxhash::FxHashMap;

pub type Seq = i32;
pub type Lamport = NonZeroU32;
pub type Peer = u64;
pub type ColId = u32;
pub type RowId = u64;

#[derive(Debug, Clone, Copy)]
pub struct OpId {
    peer: Peer,
    lamport: Lamport,
}

#[derive(Debug, Clone)]
pub struct VersionVector {
    map: FxHashMap<Peer, Seq>,
}
