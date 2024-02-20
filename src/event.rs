use smol_str::SmolStr;

use crate::value::Value;

pub enum Event {
    Insert {
        row_id: u64,
        partial_data: Vec<(SmolStr, Value)>,
    },
    Delete {
        row_id: u64,
    },
    Update {
        row_id: u64,
        partial_data: Vec<(SmolStr, Value)>,
    },
}
