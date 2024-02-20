use fxhash::FxHashMap;

use crate::{
    clock::{ColId, Lamport, OpId, Peer, RowId},
    value::Value,
};

#[derive(Debug, Clone)]
pub(crate) struct OpLog {
    map: FxHashMap<Peer, Vec<Op>>,
    max_lamport: Lamport,
}

#[derive(Debug, Clone)]
pub struct Op {
    id: OpId,
    content: OpContent,
}

#[derive(Debug, Clone)]
pub enum OpContent {
    InsertRow {
        row_id: RowId,
        partial_data: FxHashMap<ColId, Value>,
    },
    InsertCol {
        col_id: ColId,
        name: String,
    },
    UpdateRow {
        row_id: RowId,
        partial_data: FxHashMap<ColId, Value>,
    },
}
