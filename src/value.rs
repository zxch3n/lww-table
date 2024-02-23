use std::{fmt::Display, hash::Hasher};

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Double(f64),
    I64(i64),
    Str(SmolStr),
    True,
    False,
    Null,
    Deleted,
}

impl std::hash::Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Double(d) => d.to_bits().hash(state),
            Value::I64(i) => i.hash(state),
            Value::Str(s) => s.hash(state),
            Value::True => true.hash(state),
            Value::False => false.hash(state),
            Value::Null => 0.hash(state),
            Value::Deleted => 1.hash(state),
        }
    }
}

impl Eq for Value {}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Double(d) => write!(f, "{}", d),
            Value::I64(d) => write!(f, "{}", d),
            Value::Str(s) => write!(f, "\"{}\"", s),
            Value::True => write!(f, "true"),
            Value::False => write!(f, "false"),
            Value::Null => write!(f, "null"),
            Value::Deleted => write!(f, "deleted"),
        }
    }
}

impl From<i64> for Value {
    fn from(f: i64) -> Self {
        Self::I64(f)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Self::Double(f)
    }
}

impl From<SmolStr> for Value {
    fn from(s: SmolStr) -> Self {
        Self::Str(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::Str(s.into())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self::Str(s.into())
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        if b {
            Self::True
        } else {
            Self::False
        }
    }
}
