//! # Lightweight last-write-wins CRDT table
//!
//! - In-memory
//! - Support delta updates
//! - Can be used in WASM
//! - Can be used in real-time collaborative applications
//! - Does not support custom ordering of rows or columns
//! - Small overhead per row

use clock::{ColId, RowId, VersionVector};
use event::Event;
use oplog::{Op, OpLog};
use smol_str::SmolStr;
use table::{Row, Table};

mod clock;
mod event;
mod oplog;
mod table;
mod value;

#[derive(Debug, Clone)]
pub struct LwwTable {
    table: Table,
    oplog: OpLog,
}

impl LwwTable {
    pub fn new() -> Self {
        todo!()
    }

    pub fn cols(&self) -> Vec<SmolStr> {
        todo!()
    }

    pub fn insert_row(&mut self, data: &[(SmolStr, value::Value)]) -> RowId {
        todo!()
    }

    pub fn delete_row(&mut self, row_id: u64) {
        todo!()
    }

    pub fn insert_col(&mut self, name: &str) -> ColId {
        todo!()
    }

    pub fn delete_col(&mut self, col: &str) {
        todo!()
    }

    pub fn update_row(&mut self, row_id: u64, data: &[(u64, value::Value)]) {
        todo!()
    }

    pub fn to_json(&self) -> serde_json::Value {
        todo!()
    }

    pub fn get_row(&self, row_id: u64) -> Option<&Row> {
        todo!()
    }

    pub fn import(&mut self, data: &[u8]) {
        todo!()
    }

    pub fn export_updates(&self, from: &VersionVector) -> Vec<u8> {
        todo!()
    }

    pub fn export_snapshot(&self) -> Vec<u8> {
        todo!()
    }

    pub fn subscribe(&mut self, listener: Box<dyn Fn(&Event)>) {
        todo!()
    }
}
