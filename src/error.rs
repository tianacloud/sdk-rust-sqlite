use std::fmt;

/// Redacted error. An unknown outcome must never be automatically replayed.
/// A known SQL error does not imply that earlier statements/effects rolled back.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    code: &'static str,
    outcome_unknown: bool,
    request_id: Option<String>,
}

impl Error {
    pub fn code(&self) -> &'static str {
        self.code
    }
    pub fn outcome_unknown(&self) -> bool {
        self.outcome_unknown
    }
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }
    pub(crate) fn new(code: &'static str, unknown: bool) -> Self {
        Self {
            code,
            outcome_unknown: unknown,
            request_id: None,
        }
    }
    pub(crate) fn diagnostic(mut self, id: Option<&str>) -> Self {
        self.request_id = id.map(str::to_owned);
        self
    }
    pub(crate) fn invalid() -> Self {
        Self::new("INVALID_RESPONSE", true)
    }
    pub(crate) fn sql(code: &str) -> Self {
        let known = match code {
            "SQLITE_ERROR" => "SQLITE_ERROR",
            "SQLITE_UNKNOWN" => "SQLITE_UNKNOWN",
            "SQLITE_BUSY" => "SQLITE_BUSY",
            "SQLITE_LOCKED" => "SQLITE_LOCKED",
            "SQLITE_CONSTRAINT" => "SQLITE_CONSTRAINT",
            "SQLITE_READONLY" => "SQLITE_READONLY",
            "SQLITE_MISMATCH" => "SQLITE_MISMATCH",
            "SQLITE_RANGE" => "SQLITE_RANGE",
            "SQLITE_TOOBIG" => "SQLITE_TOOBIG",
            "SQLITE_FULL" => "SQLITE_FULL",
            "SQLITE_ABORT" => "SQLITE_ABORT",
            "SQLITE_INTERRUPT" => "SQLITE_INTERRUPT",
            "SQLITE_AUTH" => "SQLITE_AUTH",
            "SQLITE_PERM" => "SQLITE_PERM",
            "ARGS_INVALID" => "ARGS_INVALID",
            "ARGS_BOTH_POSITIONAL_AND_NAMED" => "ARGS_BOTH_POSITIONAL_AND_NAMED",
            "SQL_NO_STATEMENT" => "SQL_NO_STATEMENT",
            "SQL_MANY_STATEMENTS" => "SQL_MANY_STATEMENTS",
            "RESULT_TOO_LARGE" => return Self::new("RESULT_TOO_LARGE", true),
            "RESPONSE_TOO_LARGE" => return Self::new("RESPONSE_TOO_LARGE", true),
            "SQLITE_IOERR" => return Self::new("SQLITE_IOERR", true),
            _ => return Self::new("SQL_ERROR", true),
        };
        Self::new(known, false)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (outcome_unknown={})",
            self.code, self.outcome_unknown
        )?;
        if let Some(id) = &self.request_id {
            write!(f, " [request_id={id}]")?;
        }
        Ok(())
    }
}
impl std::error::Error for Error {}
