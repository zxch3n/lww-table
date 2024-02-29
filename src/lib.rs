#![doc = include_str!("../README.md")]

use std::fmt::Display;

use clock::Peer;
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

pub use clock::{OpId, VectorClock};

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
        writeln!(f, "LwwDb {{")?;
        let mut s = String::new();
        for (name, table) in &self.tables {
            s.push_str(&format!("# {}\n", name));
            s.push_str(&format!("{}\n\n", table));
        }

        let s = s.trim();
        for line in s.split('\n') {
            writeln!(f, "  {}", line)?;
        }
        writeln!(f, "}}")
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

    pub fn check_eq(&mut self, other: &mut Self) -> bool {
        if self.tables.len() != other.tables.len() {
            return false;
        }

        for (name, table) in &mut self.tables {
            if let Some(other_table) = other.tables.get_mut(name) {
                if !table.check_eq(other_table) {
                    eprintln!("{}", &table);
                    eprintln!("{}", &other_table);
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }

    pub fn get_cell(&self, table_str: &str, row: &str, col: &str) -> Option<&Value> {
        self.tables
            .get(table_str)
            .and_then(|table| table.get_cell(row, col))
    }

    pub fn iter_row(
        &self,
        table_str: &str,
        row: &str,
    ) -> impl Iterator<Item = (&str, &Value)> + '_ {
        self.tables
            .get(table_str)
            .map(|table| table.iter_row(row))
            .into_iter()
            .flatten()
    }

    pub fn set(&mut self, table_str: &str, row: &str, col: &str, value: impl Into<Value>) {
        self.set_(table_str, row, col, value.into(), None)
    }

    pub(crate) fn set_(
        &mut self,
        table_str: &str,
        row: &str,
        col: &str,
        value: impl Into<Value>,
        id: Option<OpId>,
    ) {
        self.inner_set_(table_str, row, col, value.into(), id)
    }

    fn inner_set_(
        &mut self,
        table_str: &str,
        row: &str,
        col: &str,
        value: Value,
        id: Option<OpId>,
    ) {
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

    pub fn delete(&mut self, table_str: &str, row: &str, col: &str) {
        let id = self.next_id();
        let table = if let Some(table) = self.tables.get_mut(table_str) {
            table
        } else {
            self.create_table(table_str);
            self.tables.get_mut(table_str).unwrap()
        };

        if table.delete(row, col, id) {
            self.oplog.record_update(id, table_str.into(), row.into())
        }
    }

    pub fn delete_row(&mut self, table_str: &str, row: &str) {
        self.delete_row_(table_str, row, None)
    }

    pub(crate) fn delete_row_(&mut self, table_str: &str, row: &str, id: Option<OpId>) {
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

    pub fn delete_table(&mut self, table_str: &str) {
        self.delete_table_(table_str, None)
    }

    pub(crate) fn delete_table_(&mut self, table_str: &str, id: Option<OpId>) {
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

    pub fn iter_tables(&self) -> impl Iterator<Item = (&SmolStr, &LwwTable)> {
        self.tables.iter()
    }

    pub fn iter_tables_mut(&mut self) -> impl Iterator<Item = (&SmolStr, &mut LwwTable)> {
        self.tables.iter_mut()
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

    pub fn subscribe(&mut self, _listener: Box<dyn Fn(&Event)>) {
        todo!()
    }

    pub fn version(&self) -> &VectorClock {
        self.oplog.version()
    }
}
