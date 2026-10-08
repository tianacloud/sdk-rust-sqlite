use crate::Error;
use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use serde::{Deserialize, Serialize};

/// SQLite values preserve signed 64-bit integers and arbitrary blob bytes.
/// Values are intentionally not Debug to avoid accidentally logging query data.
#[derive(Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::Integer(v)
    }
}
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Self::Integer(i64::from(v))
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Self::Text(v.into())
    }
}
impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Self::Blob(v)
    }
}

/// Positional and named arguments are mutually exclusive.
#[derive(Clone)]
pub enum Args {
    Positional(Vec<Value>),
    Named(Vec<(String, Value)>),
}
impl Default for Args {
    fn default() -> Self {
        Self::Positional(Vec::new())
    }
}

/// One SQL statement (server rejects multiple statements).
#[derive(Clone)]
pub struct Statement {
    pub sql: String,
    pub args: Args,
}
impl Statement {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            args: Args::default(),
        }
    }
    pub fn args(mut self, args: impl IntoIterator<Item = Value>) -> Self {
        self.args = Args::Positional(args.into_iter().collect());
        self
    }
    pub fn named_args(mut self, args: impl IntoIterator<Item = (String, Value)>) -> Self {
        self.args = Args::Named(args.into_iter().collect());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Column {
    pub name: Option<String>,
    pub decltype: Option<String>,
}

/// Buffered rows and metadata. Call query to request rows, execute to omit them.
#[derive(Clone, PartialEq)]
pub struct StatementResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
    pub affected_row_count: u64,
    pub last_insert_rowid: Option<i64>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub(crate) enum WireValue {
    Null {},
    Integer { value: String },
    Float { value: f64 },
    Text { value: String },
    Blob { base64: String },
}
impl TryFrom<&Value> for WireValue {
    type Error = Error;
    fn try_from(v: &Value) -> Result<Self, Error> {
        Ok(match v {
            Value::Null => Self::Null {},
            Value::Integer(v) => Self::Integer {
                value: v.to_string(),
            },
            Value::Float(v) if v.is_finite() => Self::Float { value: *v },
            Value::Float(_) => return Err(Error::new("INVALID_ARGUMENT", false)),
            Value::Text(v) => Self::Text { value: v.clone() },
            Value::Blob(v) => Self::Blob {
                base64: STANDARD_NO_PAD.encode(v),
            },
        })
    }
}
impl TryFrom<WireValue> for Value {
    type Error = Error;
    fn try_from(v: WireValue) -> Result<Self, Error> {
        Ok(match v {
            WireValue::Null {} => Self::Null,
            WireValue::Integer { value } => {
                Self::Integer(value.parse().map_err(|_| Error::invalid())?)
            }
            WireValue::Float { value } if value.is_finite() => Self::Float(value),
            WireValue::Float { .. } => return Err(Error::invalid()),
            WireValue::Text { value } => Self::Text(value),
            WireValue::Blob { base64 } => Self::Blob(
                STANDARD_NO_PAD
                    .decode(base64)
                    .map_err(|_| Error::invalid())?,
            ),
        })
    }
}

#[derive(Serialize)]
pub(crate) struct WireStatement {
    sql: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    args: Vec<WireValue>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    named_args: Vec<NamedValue>,
    want_rows: bool,
}
#[derive(Serialize)]
struct NamedValue {
    name: String,
    value: WireValue,
}
impl WireStatement {
    pub(crate) fn new(statement: &Statement, want_rows: bool) -> Result<Self, Error> {
        // Bound before cloning/encoding caller-owned payloads, including base64 expansion.
        let mut size = statement.sql.len();
        let mut add = |name: usize, value: &Value| -> Result<(), Error> {
            let n = match value {
                Value::Text(v) => v.len(),
                Value::Blob(v) => v.len().saturating_mul(4).div_ceil(3),
                _ => 32,
            };
            size = size
                .saturating_add(name)
                .saturating_add(n)
                .saturating_add(64);
            if size > crate::MAX_BODY_BYTES {
                return Err(Error::new("REQUEST_TOO_LARGE", false));
            }
            Ok(())
        };
        match &statement.args {
            Args::Positional(v) => {
                for v in v {
                    add(0, v)?;
                }
            }
            Args::Named(v) => {
                for (name, v) in v {
                    add(name.len(), v)?;
                }
            }
        }
        if size > crate::MAX_BODY_BYTES {
            return Err(Error::new("REQUEST_TOO_LARGE", false));
        }
        let mut wire = Self {
            sql: statement.sql.clone(),
            args: Vec::new(),
            named_args: Vec::new(),
            want_rows,
        };
        match &statement.args {
            Args::Positional(args) => {
                wire.args = args
                    .iter()
                    .map(WireValue::try_from)
                    .collect::<Result<_, _>>()?
            }
            Args::Named(args) => {
                let mut names = std::collections::HashSet::new();
                for (name, value) in args {
                    if name.is_empty() || name.contains('\0') || !names.insert(name) {
                        return Err(Error::new("INVALID_ARGUMENT", false));
                    }
                    wire.named_args.push(NamedValue {
                        name: name.clone(),
                        value: WireValue::try_from(value)?,
                    });
                }
            }
        }
        Ok(wire)
    }
}

#[derive(Deserialize)]
pub(crate) struct WireResult {
    cols: Vec<Column>,
    rows: Vec<Vec<WireValue>>,
    affected_row_count: u64,
    last_insert_rowid: Option<String>,
}
impl TryFrom<WireResult> for StatementResult {
    type Error = Error;
    fn try_from(w: WireResult) -> Result<Self, Error> {
        let mut rows = Vec::with_capacity(w.rows.len());
        for row in w.rows {
            if row.len() != w.cols.len() {
                return Err(Error::invalid());
            }
            rows.push(
                row.into_iter()
                    .map(Value::try_from)
                    .collect::<Result<_, _>>()?,
            );
        }
        Ok(Self {
            columns: w.cols,
            rows,
            affected_row_count: w.affected_row_count,
            last_insert_rowid: w
                .last_insert_rowid
                .map(|s| s.parse().map_err(|_| Error::invalid()))
                .transpose()?,
        })
    }
}
