use std::collections::BTreeMap;

use fxhash::FxHashMap;

use crate::{
    clock::{Lamport, OpId, Peer, RowId},
    str::StrIndex,
};

#[derive(Debug, Clone)]
pub(crate) struct OpLog {
    map: FxHashMap<Peer, BTreeMap<OpId, RowId>>,
    max_lamport: Lamport,
}
