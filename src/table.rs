use std::collections::BTreeMap;

use fxhash::FxHashMap;
use smol_str::SmolStr;

use crate::{
    clock::{ColId, Lamport, RowId},
    value::Value,
};

#[derive(Debug, Clone)]
pub struct Table {
    rows: BTreeMap<RowId, Row>,
    cols: FxHashMap<ColId, SmolStr>,
}

#[derive(Debug, Clone)]
pub struct Row {
    data: FxHashMap<ColId, ValueWithLamport>,
}

#[derive(Debug, Clone)]
pub struct ValueWithLamport {
    value: Option<Value>,
    lamport: Lamport,
}
