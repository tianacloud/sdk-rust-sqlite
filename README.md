# Tiana Rust SQLite SDK

`tiana-sdk-sqlite` provides asynchronous SQLite sessions through Tiana Gateway.
It depends on `tiana-sdk` from **sdk-rust** for the generic TLS/H2 CONNECT channel.
This repository owns the `hrana-http` routing profile, Hrana HTTP v3 pipeline,
SQL values/results, baton lifecycle and transaction state. It contains no local
SQLite engine and does not invoke a helper process.

## GitHub dependency

`tiana-sdk` is fetched directly from GitHub, pinned to the immutable revision
`c1f5c9872bdea4aaa542d33f613e92e005ea03ff` (`0.1.0-dev.5`):

```toml
tiana-sdk = { git = "https://github.com/tianacloud/sdk-rust.git", rev = "c1f5c9872bdea4aaa542d33f613e92e005ea03ff", version = "=0.1.0-dev.5" }
```

No sibling checkout, local patch or path override is required. Cargo.lock records
this HTTPS Git source and the resolved transitive dependencies. No SSH key is
required. If GitHub requires authentication, configure HTTPS credentials:

```sh
cargo build --locked --examples
```

This revision is the verified `v1.0.0` release commit and includes the
generic Protocol API and helper removal. The SQLite profile uses the dependency's
public protocol parser. No local core source or helper is required. Future main
updates require an explicit revision/lock refresh; crates.io publication remains
disabled.

## Runnable demo

`examples/sqlite.rs` is a complete executable: it creates a connection-local TEMP
table, binds an int64/text/blob row in a transaction, commits and queries it with
a named parameter, verifies rollback of another insert, then explicitly closes.
It does not create or modify persistent application tables and can be rerun.

```sh
export TIANA_ENDPOINT='ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test'
# Replace the example endpoint with your actual endpoint.
# Optional: supply TIANA_TOKEN in the process environment for authenticated access.
cargo run --locked --example sqlite
```

Omit `TIANA_TOKEN` for anonymous access. It contains the InstanceToken value;
empty or malformed values are rejected. The local copy is zeroized, but this does
not erase the original process environment. Token files are not supported.
Account access/refresh tokens are not tunnel credentials. The demo does not
perform login or acquire tokens automatically.

Optional settings:

- `TIANA_CA_FILE`: path to a PEM CA certificate bundle, replacing built-in
  roots. Set `TIANA_CA_FILE=/path/to/ca.pem`; one or more certificates are
  supported without conversion. Empty, malformed or invalid certificates fail
  before connecting; certificate-chain and hostname verification stay enabled.
- `TIANA_GATEWAY_ADDRESS`: alternate TCP destination such as `127.0.0.1:8443`.
  The endpoint still determines verified TLS SNI and CONNECT authority.

`cargo run --locked --example sqlite -- --help` prints configuration help without
connecting. Successful output:

```text
committed row: id=9223372036854775807, label=hello Tiana, blob_bytes=2
rollback verified: rows=1
session closed
```

The demo always attempts bounded explicit close after its SQL operations, even
on errors. If interrupted or close fails, server cleanup may wait for TTL; errors
are never automatically retried.

## Usage

```rust,no_run
use tiana_sdk::Client;
use tiana_sdk_sqlite::{Session, Statement, TransactionMode, Value};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
// Configure Endpoint, token and TLS trust with the generic transport builder.
let transport = Client::builder(
    "ep-00000000000000000000000000.db.example.test",
).build()?;
let mut session = Session::new(transport); // No I/O until the first operation.
session.execute(&Statement::new(
    "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)",
)).await?;
session.begin(TransactionMode::Immediate).await?;
session.execute(&Statement::new("INSERT INTO items (name) VALUES (?)")
    .args([Value::from("example")])).await?;
session.commit().await?;
let result = session.query(&Statement::new("SELECT id, name FROM items")).await?;
assert_eq!(result.columns.len(), 2);
session.close().await?;
# Ok(())
# }
```

`execute` requests metadata without rows; `query` buffers rows. Both accept one
statement with positional `.args(...)` or `.named_args([(name, value), ...])`.
`Value` supports null, signed 64-bit integer, finite float, UTF-8 text (including
NUL) and arbitrary blob bytes. Integers travel as decimal strings; blobs use
unpadded standard base64. Boolean conversion produces integer 0/1. Duplicate
named parameters and nonfinite floats are rejected before I/O. `StatementResult`
contains columns, rows, affected row count and optional last insert rowid.

A Session exclusively owns one logical connection. Methods take `&mut self`, so
no two operations can overlap on it. Multiple sessions may share a clone of the
same generic client and multiplex independent CONNECT streams. There is no pool,
SQLx adapter, server-side prepared statement cache, WebSocket transport or
streaming cursor in this initial API.

## Transactions, cancellation and errors

- `begin`, `commit` and `rollback` require a confirmed matching transaction state.
  Raw transaction SQL and savepoints are also supported. Every execute pipeline
  asks the server for `get_autocommit`; `autocommit()` is `None` after closure or
  uncertainty. A failed commit can leave a transaction open; inspect the error
  and confirmed state before explicitly rolling it back.
- No operation is automatically retried or replayed. Dropping a polled operation,
  timeouts, malformed responses, uncertain SQL errors and transport failures
  discard the connection. The same Session never reconnects its baton. Create a
  new session only after resolving any uncertain prior outcome.
- `Error::outcome_unknown()` means execution may already have committed or partly
  executed. Even `false` on a SQL error does **not** prove earlier effects rolled
  back. `BATON_INVALID` rejects the current request before execution, but the
  previous session is still lost. Error text contains only a bounded code and
  request ID; SQL, credentials, batons and server error messages are excluded.
- `close` explicitly releases a known server stream and local tunnel; it never
  commits. `execute_and_close` sends execute/get_autocommit/close in one pipeline.
  Closing an unused session performs no network I/O. Drop aborts the local
  channel without a network close handshake: server cleanup/rollback can wait
  for the server's stream TTL. Explicitly finish transactions and call close;
  cancellation is not an assertion of rollback or durability.

Requests and responses are limited to 8 MiB, HTTP headers to 32 KiB and batons to
4096 bytes. The default operation timeout is 30 seconds and explicit close is
bounded by the smaller of the configured timeout and 3 seconds. Configure it with
`Session::new(client).with_request_timeout(duration)?`. Serialization has a bounded
writer; response decoding rejects missing required fields, invalid typed values,
row-width mismatches, redirects and encoded responses. Decoded Rust data consumes
more memory than wire bytes; this API buffers results, so select bounded pages for
large queries. Database persistence, atomicity and locking remain server-owned.
There is no credential forwarding inside the inner HTTP request and no redirect.
Token acquisition/refresh remains the caller's responsibility.

## Verification

```sh
cargo test --locked
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
```

Tests traverse a local TLS/H2 Gateway fixture and validate typed data, baton
rotation, state transitions, errors, cancellation, timeouts, response validation,
limits, redaction and no replay. A separate ignored test runs against a disposable
App SQLite (with a loopback TCP-to-Unix-socket bridge):

```sh
TIANA_SQLITE_TEST_ADDRESS=127.0.0.1:18080 cargo test --locked \
  real_app_sqlite -- --ignored
```

That database must be empty and disposable; the test creates `sdk_test`. It checks
actual SQL execution, isolation, rollback, savepoints, commit, constraint errors,
close rollback and committed data across new sessions. Tests never target a
production Endpoint.
