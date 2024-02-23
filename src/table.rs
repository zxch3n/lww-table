use std::{fmt::Display, iter::once};

use fxhash::FxHashMap;
use smol_str::SmolStr;

use crate::{
    clock::{Lamport, OpId, Peer},
    value::Value,
};

#[derive(Debug, Clone, Default)]
pub struct LwwTable {
    pub(crate) row_id_to_idx: FxHashMap<SmolStr, usize>,
    pub(crate) rows: Vec<Row>,
    pub(crate) cols: FxHashMap<SmolStr, Column>,
    pub(crate) removed: Option<OpId>,
}

impl Display for LwwTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let table = self.build_table();
        write!(f, "{}", table)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub(crate) row_id: SmolStr,
    pub(crate) deleted: Option<OpId>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Column {
    pub(crate) value: Vec<Value>,
    pub(crate) lamport: Vec<Lamport>,
    pub(crate) peer: Vec<Peer>,
    pub(crate) num: usize,
}

impl Column {
    pub(crate) fn with_len(len: usize) -> Column {
        Column {
            value: vec![Value::Null; len],
            lamport: vec![0; len],
            peer: vec![0; len],
            num: 0,
        }
    }
}

impl LwwTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build_table(&self) -> tabled::Table {
        let mut table = tabled::builder::Builder::default();
        table.push_record(once("row_id"));
        for row_name in &self.rows {
            table.push_record(once(row_name.row_id.as_str()));
        }

        for (col_name, col) in &self.cols {
            table.push_column(
                once(col_name.to_string()).chain(col.value.iter().map(|v| v.to_string())),
            );
        }
        table.build()
    }

    fn ensure_row(&mut self, row_id: &str) -> usize {
        if let Some(v) = self.row_id_to_idx.get(row_id) {
            return *v;
        }

        let idx = self.rows.len();
        let row_id = SmolStr::new(row_id);
        self.row_id_to_idx.insert(row_id.clone(), self.rows.len());
        self.rows.push(Row {
            row_id,
            deleted: None,
        });
        for (_, col) in self.cols.iter_mut() {
            col.value.push(Value::Null);
            col.lamport.push(0);
            col.peer.push(0);
        }

        idx
    }

    fn ensure_col(&mut self, col_name: &str) -> &mut Column {
        self.cols.entry(col_name.into()).or_insert_with(|| {
            let len = self.rows.len();
            Column {
                value: vec![Value::Null; len],
                lamport: vec![0; len],
                peer: vec![0; len],
                num: 0,
            }
        })
    }

    pub fn set(&mut self, row: &str, col: &str, v: Value, id: OpId) -> bool {
        if id.lamport == 0 {
            assert!(id.peer == 0, "lamport is 0, peer should be 0");
            assert!(v == Value::Null, "lamport is 0, value should be null");
            return false;
        }

        if let Some(removed) = self.removed {
            if id < removed {
                return false;
            }
        }

        let row_idx = self.ensure_row(row);
        if let Some(d) = self.rows[row_idx].deleted {
            if id < d {
                return false;
            }
        }

        let col = self.ensure_col(col);
        if id < OpId::new(col.lamport[row_idx], col.peer[row_idx]) {
            return false;
        }

        if col.lamport[row_idx] == 0 {
            col.num += 1;
        }

        col.value[row_idx] = v;
        col.lamport[row_idx] = id.lamport;
        col.peer[row_idx] = id.peer;
        true
    }

    pub fn check_eq(&mut self, other: &mut Self) -> bool {
        if self.rows.len() != other.rows.len() {
            eprintln!("row number not equal");
            return false;
        }

        if self.cols.len() != other.cols.len() {
            eprintln!("col number not equal");
            return false;
        }

        self.sort();
        other.sort();

        if self.rows != other.rows {
            eprintln!(
                "row not equal self:\n{:#?}\nother:\n{:#?}",
                self.rows, other.rows
            );
            return false;
        }

        if self.cols != other.cols {
            eprintln!(
                "col not equal:\n{:#?}\nother:\n{:#?}",
                self.cols, other.cols
            );
            return false;
        }

        true
    }

    pub fn delete(&mut self, row: &str, col: &str, id: OpId) -> bool {
        self.set(row, col, Value::Null, id)
    }

    pub fn delete_row(&mut self, row: &str, id: OpId) -> bool {
        let idx = self.ensure_row(row);
        let row = &mut self.rows[idx];
        if let Some(removed) = &row.deleted {
            if id < *removed {
                return false;
            }
        }

        let mut to_remove = vec![];
        for (c, col) in self.cols.iter_mut() {
            if id < OpId::new(col.lamport[idx], col.peer[idx]) {
                continue;
            }

            if col.lamport[idx] != 0 {
                col.num -= 1;
            }

            col.value[idx] = Value::Null;
            col.lamport[idx] = 0;
            col.peer[idx] = 0;
            if col.num == 0 {
                to_remove.push(c.clone());
            }
        }

        for c in to_remove {
            self.cols.remove(&c);
        }

        row.deleted = Some(id);
        true
    }

    pub fn delete_table(&mut self, id: OpId) -> bool {
        if let Some(removed) = self.removed {
            if id < removed {
                return false;
            }
        }

        // FIXME: should not clear the rows with clock > id
        self.cols.clear();
        self.rows.clear();
        self.removed = Some(id);
        true
    }

    pub fn sort(&mut self) {
        let mut vecs: Vec<&mut dyn Swappable> = self
            .cols
            .values_mut()
            .flat_map(|c| {
                [
                    (&mut c.value) as &mut dyn Swappable,
                    &mut c.lamport,
                    &mut c.peer,
                ]
            })
            .collect();
        sort_vecs_based_on_first(&mut self.rows, |r| r.row_id.as_str(), &mut vecs);
        self.row_id_to_idx = self
            .rows
            .iter()
            .enumerate()
            .map(|(i, r)| (r.row_id.clone(), i))
            .collect();
    }

    pub fn iter_row(&self, row: &str) -> impl Iterator<Item = RowValue> + '_ {
        let idx = self.row_id_to_idx.get(row);
        idx.map(|idx| {
            self.cols.iter().map(move |(col_name, col)| RowValue {
                col_name,
                id: OpId::new(col.lamport[*idx], col.peer[*idx]),
                value: &col.value[*idx],
            })
        })
        .into_iter()
        .flatten()
    }
}

pub struct RowValue<'a> {
    pub col_name: &'a str,
    pub id: OpId,
    pub value: &'a Value,
}

fn sort_vecs_based_on_first<T, U: Ord + ?Sized>(
    a: &mut [T],
    f: impl Fn(&T) -> &U,
    vecs: &mut [&mut dyn Swappable],
) {
    let mut indexes: Vec<usize> = (0..a.len()).collect();

    indexes.sort_by(|&i, &j| f(&a[i]).cmp(f(&a[j])));

    for i in 0..indexes.len() {
        while indexes[i] != i {
            let target = indexes[i];
            a.swap(i, target);
            for v in vecs.iter_mut() {
                v.swap_(i, target);
            }
            indexes.swap(i, target);
        }
    }
}

trait Swappable {
    fn swap_(&mut self, i: usize, j: usize);
}

impl<T> Swappable for Vec<T> {
    fn swap_(&mut self, i: usize, j: usize) {
        self.swap(i, j)
    }
}
