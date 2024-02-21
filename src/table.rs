use crate::{clock::OpId, value::Value};
use fxhash::FxHashMap;
use smol_str::SmolStr;
use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    iter::once,
    sync::Arc,
};
use tabled::settings::Style;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LwwTable {
    pub(crate) cols: BTreeMap<SmolStr, Col>,
    pub(crate) rows: BTreeMap<SmolStr, Row>,
    /// If this is Some, the content of the table can only contain rows that inserted after the given OpId
    pub(crate) removed: Option<OpId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Col {
    pub name: Arc<str>,
    pub num: usize,
}

impl Col {
    fn fetch_dec(&mut self) -> usize {
        self.num -= 1;
        self.num
    }

    fn fetch_inc(&mut self) -> usize {
        self.num += 1;
        self.num
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row {
    /// We use Arc<str> to reduce memory usage
    pub(crate) map: FxHashMap<SmolStr, ValueAndClock>,
    /// If this is Some, the content of the row can only contain values that inserted after the given OpId
    pub(crate) cleared_at: Option<OpId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValueAndClock {
    pub id: OpId,
    pub value: Value,
}

impl Display for LwwTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let build_table = &mut self.build_table();
        let table = build_table.with(Style::modern_rounded());
        write!(f, "{}", table)
    }
}

impl LwwTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build_table(&self) -> tabled::Table {
        use tabled::builder::Builder;

        let mut builder = Builder::default();
        builder.push_record(once("Row Name").chain(self.cols.keys().map(|x| x.as_str())));
        for (row, row_data) in &self.rows {
            let mut record = Vec::new();
            record.push(row.to_string());
            for col in self.cols.keys() {
                if let Some(value) = row_data.map.get(col) {
                    record.push(value.value.to_string());
                } else {
                    record.push("".to_string());
                }
            }
            builder.push_record(record);
        }

        builder.build()
    }

    pub fn to_json(&self) -> serde_json::Value {
        todo!()
    }

    pub(crate) fn delete(&mut self, row: &str, col: &str, id: OpId) -> bool {
        let row = self.rows.entry(row.into()).or_default();
        if let Some(value) = row.map.get_mut(col) {
            if value.id > id {
                return false;
            }

            value.id = id;
            if value.value != Value::Deleted && self.cols.get_mut(col).unwrap().fetch_dec() == 0 {
                self.cols.remove(col);
            }

            value.value = Value::Deleted;
        } else {
            row.map.insert(
                col.into(),
                ValueAndClock {
                    id,
                    value: Value::Deleted,
                },
            );
        }

        true
    }

    /// Return whether successful or not
    pub(crate) fn delete_row(&mut self, row: &str, id: OpId) -> bool {
        let row = self.rows.entry(row.into()).or_default();
        if let Some(cleared_at) = row.cleared_at {
            if cleared_at > id {
                return false;
            }
        }

        row.map.retain(|c, v| {
            if v.id >= id {
                true
            } else {
                if self.cols.get_mut(c).unwrap().fetch_dec() == 0 {
                    self.cols.remove(c);
                }
                false
            }
        });
        row.cleared_at = Some(id);
        true
    }

    /// Return whether successful or not
    pub(crate) fn delete_table(&mut self, id: OpId) -> bool {
        if let Some(removed) = self.removed {
            if removed > id {
                return false;
            }
        }

        self.rows.retain(|_, row| {
            row.map.retain(|c, v| {
                if v.id >= id {
                    true
                } else {
                    if self.cols.get_mut(c).unwrap().fetch_dec() == 0 {
                        self.cols.remove(c);
                    }
                    false
                }
            });
            !row.map.is_empty()
        });

        self.removed = Some(id);
        true
    }

    /// Return whether successful or not
    pub(crate) fn set(&mut self, row: &str, col: &str, value: Value, id: OpId) -> bool {
        if value == Value::Deleted {
            return self.delete(row, col, id);
        }

        if let Some(removed) = self.removed {
            if removed > id {
                return false;
            }
        }

        if !self.rows.contains_key(row) {
            self.rows.insert(row.into(), Row::default());
        }

        if !self.cols.contains_key(col) {
            self.cols.insert(
                col.into(),
                Col {
                    name: col.into(),
                    num: 0,
                },
            );
        }

        let row = self.rows.get_mut(row).unwrap();
        if let Some(cleared_at) = row.cleared_at {
            if cleared_at > id {
                return false;
            }
        }

        if let Some(old) = row.map.get(col) {
            if old.id > id {
                return false;
            }
        } else {
            self.cols.get_mut(col).unwrap().fetch_inc();
        }

        row.map.insert(col.into(), ValueAndClock { id, value });

        true
    }

    pub fn iter_rows(&self) -> impl Iterator<Item = (&str, &Row)> {
        self.rows.iter().map(|(k, v)| (k.as_str(), v))
    }

    #[allow(unused)]
    pub(crate) fn dbg_check(&self) {
        // check the cols' numbers are correct
        for (col, col_data) in &self.cols {
            let mut num = 0;
            for row in self.rows.values() {
                if row.map.contains_key(col) {
                    num += 1;
                }
            }
            assert_eq!(num, col_data.num);
        }
    }
}

impl Row {
    pub fn iter(&self) -> impl Iterator<Item = (&SmolStr, &ValueAndClock)> {
        self.map.iter()
    }
}
