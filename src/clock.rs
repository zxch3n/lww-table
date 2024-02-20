use fxhash::FxHashMap;

pub type Seq = i32;
pub type Lamport = u32;
pub type Peer = u64;
pub type RowId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct OpId {
    pub lamport: Lamport,
    pub peer: Peer,
}

#[derive(Debug, Clone)]
pub struct VersionVector {
    map: FxHashMap<Peer, Seq>,
}
