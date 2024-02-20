//! # Lightweight last-write-wins CRDT table
//!
//! - In-memory
//! - Support delta updates
//! - Can be used in WASM
//! - Can be used in real-time collaborative applications
//! - Does not support custom ordering of rows or columns
//! - Small overhead per row

use clock::{Peer, RowId, VersionVector};
use event::Event;
use fxhash::FxHashMap;
use oplog::OpLog;
use peer_pool::PeerPool;
use smol_str::SmolStr;
use str::{AnyStr, ColStr, StrPool};
use table::Table;

pub(crate) mod clock;
mod event;
mod oplog;
mod peer_pool;
mod str;
pub(crate) mod table;
pub(crate) mod value;

#[derive(Debug, Clone)]
pub struct LwwTable {
    peer: Peer,
    peer_pools: PeerPool,
    str_pool: StrPool,
    col_pool: StrPool<ColStr>,
    tables: FxHashMap<SmolStr, Table>,
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

    pub fn delete_col(&mut self, col: &str) {
        todo!()
    }

    pub fn update_row(&mut self, row_id: u64, data: &[(u64, value::Value)]) {
        todo!()
    }

    pub fn to_json(&self) -> serde_json::Value {
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
