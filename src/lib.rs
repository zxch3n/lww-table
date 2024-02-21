//! # Lightweight last-write-wins CRDT table
//!
//! - In-memory
//! - Support delta updates
//! - Can be used in WASM
//! - Can be used in real-time collaborative applications
//! - Does not support custom ordering of rows or columns
//! - Small overhead
//! - No support for indexes, joins, or complex queries
//! - No support for transactions
//! - Can be used like a KV store

use std::fmt::Display;

use clock::{OpId, Peer};
use event::Event;
use fxhash::FxHashMap;
use oplog::OpLog;
use smol_str::SmolStr;
use table::LwwTable;
use value::Value;

pub(crate) mod clock;
mod encode;
mod event;
mod oplog;
pub(crate) mod table;
pub(crate) mod value;

#[derive(Debug, Clone)]
pub struct LwwDb {
    peer: Peer,
    tables: FxHashMap<SmolStr, LwwTable>,
    oplog: OpLog,
}

impl Default for LwwDb {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for LwwDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        for (name, table) in &self.tables {
            s.push_str(&format!("Table: {}\n", name));
            s.push_str(&format!("{}\n", table));
        }

        write!(f, "{}", s)
    }
}

impl LwwDb {
    pub fn new() -> Self {
        let mut id = [0u8; 8];
        getrandom::getrandom(&mut id).unwrap();
        LwwDb {
            peer: Peer::from_be_bytes(id),
            tables: Default::default(),
            oplog: Default::default(),
        }
    }

    pub fn set_peer(&mut self, peer: Peer) {
        self.peer = peer;
    }

    pub(crate) fn check_eq(&self, other: &Self) -> bool {
        self.tables == other.tables
    }

    pub fn set(
        &mut self,
        table_str: &str,
        row: &str,
        col: &str,
        value: impl Into<Value>,
        id: Option<OpId>,
    ) {
        self.set_(table_str, row, col, value.into(), id)
    }

    fn set_(&mut self, table_str: &str, row: &str, col: &str, value: Value, id: Option<OpId>) {
        let id = id.unwrap_or_else(|| self.next_id());
        let table = if let Some(table) = self.tables.get_mut(table_str) {
            table
        } else {
            self.create_table(table_str);
            self.tables.get_mut(table_str).unwrap()
        };

        if table.set(row, col, value, id) {
            self.oplog.record_update(id, table_str.into(), row.into())
        }
    }

    pub fn delete_row(&mut self, table_str: &str, row: &str, id: Option<OpId>) {
        let id = id.unwrap_or_else(|| self.next_id());
        let table = if let Some(table) = self.tables.get_mut(table_str) {
            table
        } else {
            self.create_table(table_str);
            self.tables.get_mut(table_str).unwrap()
        };

        if table.delete_row(row, id) {
            self.oplog
                .record_delete_row(id, table_str.into(), row.into())
        }
    }

    pub fn delete_table(&mut self, table_str: &str, id: Option<OpId>) {
        let id = id.unwrap_or_else(|| self.next_id());
        let table = if let Some(table) = self.tables.get_mut(table_str) {
            table
        } else {
            self.create_table(table_str);
            self.tables.get_mut(table_str).unwrap()
        };

        if table.delete_table(id) {
            self.oplog.record_delete_table(id, table_str.into())
        }
    }

    fn next_id(&mut self) -> OpId {
        let lamport = self.oplog.next_lamport();
        OpId {
            lamport,
            peer: self.peer,
        }
    }

    pub fn create_table(&mut self, name: &str) {
        self.tables.insert(name.into(), LwwTable::new());
    }

    pub fn import(&mut self, data: &[u8]) {
        todo!()
    }

    pub fn export_snapshot(&self) -> Vec<u8> {
        todo!()
    }

    pub fn subscribe(&mut self, listener: Box<dyn Fn(&Event)>) {
        todo!()
    }
}
