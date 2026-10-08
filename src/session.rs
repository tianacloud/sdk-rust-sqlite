use crate::{
    Error, Statement, StatementResult,
    value::{WireResult, WireStatement},
};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::{Request, client::conn::http1};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::{io::Write, time::Duration};
use tiana_sdk::Client;
use tokio::task::JoinHandle;

pub const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
const MAX_BATON_BYTES: usize = 4096;
const MAX_HEADER_BYTES: usize = 32 * 1024;
pub const HRANA_HTTP_PROTOCOL: &str = "hrana-http";

#[derive(Clone, Copy, Debug)]
pub enum TransactionMode {
    Deferred,
    Immediate,
    Exclusive,
}

/// One exclusive, lazy SQLite session. Operations require `&mut self` and are
/// never replayed. Dropping a polled operation poisons the session and drops its
/// tunnel. Always explicitly finish transactions and close sessions.
pub struct Session {
    client: Client,
    state: State,
    timeout: Duration,
    autocommit: Option<bool>,
    request_id: Option<String>,
}
enum State {
    Fresh,
    Ready(Channel),
    Unusable,
    Closed,
}
struct Channel {
    sender: http1::SendRequest<Full<Bytes>>,
    driver: JoinHandle<()>,
    baton: Option<String>,
}
impl Drop for Channel {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

impl Session {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            state: State::Fresh,
            timeout: Duration::from_secs(30),
            autocommit: Some(true),
            request_id: None,
        }
    }
    pub fn with_request_timeout(mut self, timeout: Duration) -> Result<Self, Error> {
        if timeout.is_zero() || std::time::Instant::now().checked_add(timeout).is_none() {
            return Err(Error::new("INVALID_TIMEOUT", false));
        }
        self.timeout = timeout;
        Ok(self)
    }
    pub fn autocommit(&self) -> Option<bool> {
        self.autocommit
    }
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }
    pub fn is_usable(&self) -> bool {
        matches!(self.state, State::Fresh | State::Ready(_))
    }

    /// Executes one statement without returning rows (metadata is still returned).
    pub async fn execute(&mut self, statement: &Statement) -> Result<StatementResult, Error> {
        self.run(statement, false, false).await
    }
    /// Buffers rows within the response byte limit; no streaming cursor is retained.
    pub async fn query(&mut self, statement: &Statement) -> Result<StatementResult, Error> {
        self.run(statement, true, false).await
    }
    /// Executes one statement, retrieves autocommit and closes the server stream
    /// in the same pipeline. Do not use this to implicitly commit a transaction.
    pub async fn execute_and_close(
        &mut self,
        statement: &Statement,
    ) -> Result<StatementResult, Error> {
        self.run(statement, false, true).await
    }
    pub async fn begin(&mut self, mode: TransactionMode) -> Result<(), Error> {
        self.transaction_command(
            match mode {
                TransactionMode::Deferred => "BEGIN DEFERRED",
                TransactionMode::Immediate => "BEGIN IMMEDIATE",
                TransactionMode::Exclusive => "BEGIN EXCLUSIVE",
            },
            true,
        )
        .await
    }
    pub async fn commit(&mut self) -> Result<(), Error> {
        self.transaction_command("COMMIT", false).await
    }
    pub async fn rollback(&mut self) -> Result<(), Error> {
        self.transaction_command("ROLLBACK", false).await
    }
    async fn transaction_command(&mut self, sql: &str, before: bool) -> Result<(), Error> {
        self.check_usable()?;
        if self.autocommit != Some(before) {
            return Err(self.error("TRANSACTION_STATE", false));
        }
        self.execute(&Statement::new(sql)).await?;
        if self.autocommit != Some(!before) {
            self.state = State::Unusable;
            self.autocommit = None;
            return Err(self.error("INVALID_RESPONSE", true));
        }
        Ok(())
    }
    /// Releases the server stream then drops the local tunnel. Closing an unused
    /// session does no network I/O. Uncertain/aborted sessions never send again.
    /// Closing a session is not a commit operation.
    pub async fn close(&mut self) -> Result<(), Error> {
        match self.state {
            State::Closed => return Ok(()),
            State::Fresh => {
                self.state = State::Closed;
                self.autocommit = None;
                return Ok(());
            }
            State::Unusable => {
                self.autocommit = None;
                return Err(self.error("SESSION_UNUSABLE", true));
            }
            State::Ready(_) => {}
        }
        self.pipeline(
            vec![StreamRequest::Close],
            true,
            self.timeout.min(Duration::from_secs(3)),
        )
        .await?;
        Ok(())
    }
    fn check_usable(&self) -> Result<(), Error> {
        match self.state {
            State::Unusable => Err(self.error("SESSION_UNUSABLE", true)),
            State::Closed => Err(self.error("SESSION_CLOSED", false)),
            _ => Ok(()),
        }
    }
    fn error(&self, code: &'static str, unknown: bool) -> Error {
        Error::new(code, unknown).diagnostic(self.request_id())
    }

    async fn run(
        &mut self,
        statement: &Statement,
        rows: bool,
        closing: bool,
    ) -> Result<StatementResult, Error> {
        self.check_usable()?;
        let stmt =
            WireStatement::new(statement, rows).map_err(|e| e.diagnostic(self.request_id()))?;
        let mut requests = vec![
            StreamRequest::Execute { stmt },
            StreamRequest::GetAutocommit,
        ];
        if closing {
            requests.push(StreamRequest::Close);
        }
        let response = self.pipeline(requests, closing, self.timeout).await?;
        // The full response and state have already been validated before reuse.
        match response {
            Some(result) => Ok(result),
            None => Err(self.error("INVALID_RESPONSE", true)),
        }
    }

    async fn pipeline(
        &mut self,
        requests: Vec<StreamRequest>,
        closing: bool,
        timeout: Duration,
    ) -> Result<Option<StatementResult>, Error> {
        self.check_usable()?;
        let baton = match &self.state {
            State::Ready(c) => c.baton.as_deref(),
            _ => None,
        };
        let mut encoded = LimitedBuffer(Vec::new());
        serde_json::to_writer(
            &mut encoded,
            &PipelineRequest {
                baton,
                requests: &requests,
            },
        )
        .map_err(|_| self.error("REQUEST_TOO_LARGE", false))?;
        // Take ownership BEFORE any await. Cancellation drops the Channel driver
        // and leaves this Session terminal, with unknown autocommit state.
        let previous = std::mem::replace(&mut self.state, State::Unusable);
        self.autocommit = None;
        let mut sent = false;
        let operation = async {
            let mut channel = match previous {
                State::Ready(channel) => channel,
                State::Fresh => {
                    let tunnel = self
                        .client
                        .connect_with_diagnostic(
                            HRANA_HTTP_PROTOCOL.parse().expect("static protocol"),
                            |id| self.request_id = Some(id.to_owned()),
                        )
                        .await
                        .map_err(|_| Error::new("CONNECT_FAILED", false))?;
                    let (sender, connection) = http1::Builder::new()
                        .max_buf_size(MAX_HEADER_BYTES)
                        .handshake(TokioIo::new(tunnel))
                        .await
                        .map_err(|_| Error::new("CONNECT_FAILED", false))?;
                    Channel {
                        sender,
                        driver: tokio::spawn(async move {
                            let _ = connection.await;
                        }),
                        baton: None,
                    }
                }
                _ => unreachable!("checked usable before taking state"),
            };
            let req = Request::post("/v3/pipeline")
                .header("host", "localhost")
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .body(Full::new(Bytes::from(encoded.0)))
                .expect("static HTTP request");
            sent = true;
            let response = channel
                .sender
                .send_request(req)
                .await
                .map_err(|_| Error::new("TRANSPORT_ERROR", true))?;
            let status = response.status();
            if response.headers().contains_key("content-encoding") {
                return Err(Error::invalid());
            }
            let disconnect = response.headers().get_all("connection").iter().any(|v| {
                v.to_str().map_or(true, |v| {
                    v.split(',').any(|s| s.trim().eq_ignore_ascii_case("close"))
                })
            });
            if response.headers().get("content-length").is_some_and(|v| {
                v.to_str()
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_none_or(|v| v > MAX_BODY_BYTES as u64)
            }) {
                return Err(Error::new("RESPONSE_TOO_LARGE", true));
            }
            let mut body = response.into_body();
            let mut bytes = Vec::new();
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|_| Error::new("TRANSPORT_ERROR", true))?;
                if let Ok(data) = frame.into_data() {
                    if bytes.len().saturating_add(data.len()) > MAX_BODY_BYTES {
                        return Err(Error::new("RESPONSE_TOO_LARGE", true));
                    }
                    bytes.extend_from_slice(&data);
                }
            }
            if status != 200 {
                let code = serde_json::from_slice::<WireError>(&bytes).ok();
                return Err(match code.as_ref().map(|v| v.code.as_str()) {
                    Some("BATON_INVALID") => Error::new("BATON_INVALID", false),
                    Some("STREAM_EXPIRED") => Error::new("STREAM_EXPIRED", true),
                    Some("STREAM_NOT_FOUND") => Error::new("STREAM_NOT_FOUND", true),
                    Some("STREAM_LIMIT") => Error::new("STREAM_LIMIT", true),
                    Some("SERVICE_STOPPING") => Error::new("SERVICE_STOPPING", true),
                    Some("REQUEST_TOO_LARGE") => Error::new("REQUEST_TOO_LARGE", true),
                    _ => Error::new("HTTP_REJECTED", true),
                });
            }
            let envelope: Envelope =
                serde_json::from_slice(&bytes).map_err(|_| Error::invalid())?;
            if envelope.base_url.is_some()
                || envelope.results.len() != requests.len()
                || (closing && envelope.baton.is_some())
                || (!closing
                    && !envelope
                        .baton
                        .as_ref()
                        .is_some_and(|v| !v.is_empty() && v.len() <= MAX_BATON_BYTES))
            {
                return Err(Error::invalid());
            }
            let mut result = None;
            let mut sql_error = None;
            let mut autocommit = None;
            for (request, reply) in requests.iter().zip(envelope.results) {
                match (request, reply) {
                    (
                        StreamRequest::Execute { .. },
                        StreamResult::Ok {
                            response: StreamResponse::Execute { result: wire },
                        },
                    ) => result = Some(StatementResult::try_from(wire)?),
                    (StreamRequest::Execute { .. }, StreamResult::Error { error }) => {
                        sql_error = Some(Error::sql(&error.code))
                    }
                    (
                        StreamRequest::GetAutocommit,
                        StreamResult::Ok {
                            response: StreamResponse::GetAutocommit { is_autocommit },
                        },
                    ) => autocommit = Some(is_autocommit),
                    (
                        StreamRequest::Close,
                        StreamResult::Ok {
                            response: StreamResponse::Close {},
                        },
                    ) => {}
                    _ => return Err(Error::invalid()),
                }
            }
            if let Some(error) = &sql_error {
                if error.outcome_unknown() {
                    return Err(error.clone());
                }
            }
            channel.baton = envelope.baton;
            Ok((channel, result, sql_error, autocommit, disconnect))
        };
        let response = tokio::time::timeout(timeout, operation).await;
        let (channel, result, sql_error, autocommit, disconnect) = match response {
            Err(_) => return Err(self.error("TIMEOUT", sent)),
            Ok(Err(error)) => return Err(error.diagnostic(self.request_id())),
            Ok(Ok(response)) => response,
        };
        if closing {
            self.state = State::Closed;
        } else if !disconnect {
            self.state = State::Ready(channel);
            self.autocommit = autocommit;
        }
        // A fully validated response can have a known result even if HTTP closed
        // the connection. The session remains terminal; never reconnect its baton.
        if let Some(error) = sql_error {
            return Err(error.diagnostic(self.request_id()));
        }
        Ok(result)
    }
}

struct LimitedBuffer(Vec<u8>);
impl Write for LimitedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) > MAX_BODY_BYTES {
            return Err(std::io::Error::other("request bound"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
#[derive(Serialize)]
struct PipelineRequest<'a> {
    baton: Option<&'a str>,
    requests: &'a [StreamRequest],
}
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StreamRequest {
    Execute { stmt: WireStatement },
    GetAutocommit,
    Close,
}
#[derive(Deserialize)]
struct Envelope {
    baton: Option<String>,
    base_url: Option<String>,
    results: Vec<StreamResult>,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum StreamResult {
    Ok { response: StreamResponse },
    Error { error: WireError },
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum StreamResponse {
    Execute { result: WireResult },
    GetAutocommit { is_autocommit: bool },
    Close {},
}
#[derive(Deserialize)]
struct WireError {
    code: String,
}
