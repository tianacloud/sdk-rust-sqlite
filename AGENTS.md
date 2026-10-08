# SQLite adapter boundary (2026-09-28)

User requested sdk-rust as generic transport and this repository as the SQLite
SDK depending on it, using sdk-go / sdk-go-sqlite as references. The user explicitly
excludes old API compatibility and migration. This was an empty repository;
branch codex/78a641a9/rust-sqlite-sdk is unborn until an authorized first commit.
Do not commit, push or publish without explicit user permission.

Own Hrana HTTP v3, hrana-http routing profile, typed parameters/results, baton and
SQLite transaction state here. Generic Endpoint/credential/TLS/H2 transport comes
only from tiana-sdk; do not fork its implementation or embed an engine/helper.
Reference source: sdk-go beb7acef46e603032a588062ebb96eaefb87fbbf,
sdk-go-sqlite ce8df62d600c6509ef7a3308b17603e68f8c73d7,
App SQLite 8e5a0e226fdc86ce1c7eb0461b4ffb056bc1ccd6 (read-only).

Session requires exclusive mutable access and owns one lazy CONNECT stream.
No pool, implicit reconnect, SQL replay, implicit commit or automatic rollback.
Operations include execute/query, begin/commit/rollback, raw savepoints, explicit
close and execute-and-close. Fetch get_autocommit with each statement and validate
all results before making the channel reusable. A known SQL error can leave the
transaction active. Unknown outcome is not retry permission. HTTP BATON_INVALID
rejects only the current operation; it cannot confirm earlier transaction state.

Move the channel out of Session before any await, leave its state unusable and
clear autocommit; restore only after a complete validated response. Channel Drop
aborts the HTTP driver and releases the CONNECT stream. This makes operation
cancellation terminal without background reuse. Dropping a session cannot perform
an async server close; server cleanup may wait for stream TTL. Explicit close
releases the stream and rolls back outstanding work according to App semantics;
only explicit COMMIT confirms commit (subject to server durability settings).

Wire format is existing /v3/pipeline, never a new storage format or server contract.
No redirects, retries, decompression, inner auth forwarding or raw server error
messages. Enforce 8 MiB serialized body/response, 32 KiB HTTP header buffer,
4096-byte batons, finite floats, int64 values, strict unpadded base64, typed result
shapes and row widths. Byte bounds limit input, not decoded heap size; results
are buffered, so callers must bound query result sizes. Timeout defaults to 30 s;
close is <=3 s. Client locks are unnecessary because operations require &mut self.
The server owns locking, atomicity, persistence and recovery; do not claim client
cancellation proves rollback. Request ID and stable redacted error codes provide
diagnostics without leaking SQL, values, tokens or batons.

Tradeoff: simple dedicated sessions and buffered results provide inspectable
transaction/cancellation semantics. Pooling, SQLx, streaming and prepared caching
would introduce separate resource/concurrency contracts and are not implemented.
Tests must cover actual TLS/H2 transport, no replay after uncertain failures,
value boundaries, baton rotation, validation, close, cancellation and transactions.
Real App SQLite validation must use a disposable local database and verify commit,
rollback, savepoints, independent sessions and error recovery. Never test on
production data or alter read-only knowledge sources.

Dependency update (2026-09-28): use GitHub HTTPS source at immutable revision
f45f14e36313e1ec5787e212de9650c16c9f3067, exact package version 0.1.0-dev.5,
with Git source recorded in Cargo.lock. This published main commit contains the
generic Protocol API and helper removal. No path dependency, patch override or
unpublished revision is allowed. Parse hrana-http through its public protocol
API. No helper is built or invoked. There is no wire/storage/transaction change.

examples/sqlite.rs must remain runnable using only this repository plus its Git
and registry dependencies. Configure endpoint explicitly, optional TIANA_TOKEN
value, optional DER trust root and dial override; never disable TLS verification.
Temporary token value buffers are zeroized; the process environment is caller-owned. Use only a TEMP table, typed parameters, explicit
commit/rollback and bounded close on both success and failure. Do not retry SQL
or print credentials. Validate on the pinned Rust toolchain, with all-target tests,
Clippy, standalone source copy (no sibling), and a disposable real App SQLite demo.


## 2026-09-28: API origin and explicit token environment

Use TIANA_API_ORIGIN as the sole API-origin environment name wherever an
origin is loaded. TIANA_MGR_ORIGIN and TIANA_AUTH_ORIGIN are ignored; do not add
compatibility aliases. Explicit API constructor parameters remain available.
Remove TIANA_TOKEN_FILE and raw token-file credential readers. CLI connections
use explicit TIANA_TOKEN (presence is authoritative: empty/malformed fails),
otherwise the selected saved account access token. SDK examples use TIANA_TOKEN;
library constructors continue to accept explicit token values. Never log tokens.
Account credential persistence and refresh locking are separate from raw token
file input and remain intact. No on-disk schema, network protocol or transaction
semantics change. Old environment names deliberately stop working without a
migration fallback. Rollback requires reverting code/docs together.

TIANA_PENDING_COMMAND_FILE, TIANA_CREDENTIALS_FILE and TIANA_GATEWAY_ADDRESS are
under review only; this change does not remove them or pending-operation state.
Keep bounded token validation, existing credential file protections, and explicit
SDK dial overrides. Verify retired names cannot override current configuration,
empty tokens fail closed, saved accounts still work, and runnable SDK examples
accept TIANA_TOKEN without reading a raw token file. No extra network round trips
or file reads may be introduced by environment resolution.


## 2026-09-28: unified CA environment name

The demo reads TIANA_CA_FILE only; TIANA_CA_DER_FILE is removed without an alias.
This is an environment-name change: preserve its existing one-DER-certificate
input and replacement of built-in trust roots. Document the encoding explicitly,
including PEM-to-DER conversion. Do not silently disable certificate validation,
add format sniffing, alter the core builder API or change the pinned Git source.
SNI/hostname checks and CONNECT identity remain Endpoint-owned. Unreadable or
invalid configured CA input fails closed with bounded diagnostics. No wire,
storage, transaction, resource limit or performance change is introduced; the
same file is read once before connecting. Revert launcher and docs together if
rolling back. Validate help, locked build/fmt/Clippy and actual demo TLS with the
new variable while an invalid old variable is present. Existing uncommitted work
is retained; no commit, push or publication is authorized.


## 2026-09-28: verified remote Tiana dependencies

User requires Tiana SDK dependencies to resolve from current GitHub main commits,
never sibling paths or unpublished local builds. Pin verified immutable commits:
sdk-go e69b9c1d3842985e0ba9fd98b2403c520e5f98f1;
sdk-go-sqlite ce8df62d600c6509ef7a3308b17603e68f8c73d7;
sdk-rust f45f14e36313e1ec5787e212de9650c16c9f3067;
sdk-python 940c26c00b9f3abec63537388c4a23b0a129a98a;
sdk-node ba52cb6ec4e751f5158b17d64b45323a82f74f81.
Go records canonical resolved versions and go.sum with GOWORK=off; Rust uses Git
rev and Cargo.lock; Python uses a PEP 508 immutable Git requirement preserved in
wheel metadata; Node uses a Git dependency and package-lock. No local replace,
path patch, workspace link, editable core package or local core tarball fallback.
This supersedes earlier unpublished-core/local-development dependency guidance.

Tradeoff: reproducible builds need network access to GitHub. This Rust adapter
uses HTTPS without an SSH key; future main changes require an explicit pin
refresh. Verify fresh
resolution in isolated source copies without sibling repositories, inspecting
module/Cargo/installed Python/npm source metadata, plus relevant tests and package
consumers. Preserve wire, transaction, no-replay, TLS and storage invariants; this
update adds no runtime network round trips or persistence format migration.
No third-party version refresh is intended. Revert manifests and locks together
for rollback. Commit/push/package publication is outside this dependency update.


## 2026-09-28: HTTPS dependency source

User requires sdk-rust to be fetched from https://github.com/tianacloud/sdk-rust.git.
Keep the existing immutable revision and package version; update Cargo.toml,
Cargo.lock and README together. No SSH credentials, local paths or fallback
source are required. Verify anonymous HTTPS resolution with Git credential
helpers and URL rewrites disabled, then locked Cargo metadata from a fresh Git
cache. Only the download transport changes; public API, runtime performance,
TLS tunnel security, wire format and database semantics are unchanged. Rollback
would require reverting the manifest and lockfile source together.

Public SQLite SDK demos and README snippets must not contain deployment-internal
domains. Read the real Endpoint from user configuration and use clearly marked
example.test placeholders in instructions. TLS fixture identities in tests are
separate from public demo configuration; never redirect demos to test endpoints.

Verification observation: the anonymous HTTPS Git endpoint returned HTTP 401 on
2026-09-28 with Git credential helpers and URL rewrites disabled. Do not claim
anonymous download works; repository accessibility must be resolved separately.
Keep the requested HTTPS source; do not switch back to SSH or a local checkout.


## 2026-09-28: PEM CA files

User removes the DER-only CA-file requirement. PEM files with one or more
CERTIFICATE blocks are the standard input, matching the other language SDKs.
This supersedes the earlier DER-only demo/conversion guidance. Keep the low-level
DER builder available for decoded certificate bytes, not as a file requirement.
The core exposes add_root_certificates_pem and validates at build time with the
existing rustls PEM reader; no new dependencies or home-grown ASN.1/PEM parser.
Empty bundles, parse failures and invalid certificates fail closed, even with
public roots enabled. Demo custom roots replace built-ins as before. Never
relax chain/hostname, TLS1.3, ALPN, SNI or authority checks; do not log CA contents.

The SQLite example parses PEM using its existing rustls dev dependency and
passes decoded certificates to the pinned remote core's DER API. This lets the
example work before publication of the new core API; production Git HTTPS pins
remain unchanged and no local dependency fallback is introduced. This is input
format decoding only, not a second transport implementation.

Parsing occurs once during client configuration, linear in the caller-supplied
bundle size, with allocations proportional to certificates/input. No file I/O
or parsing on the per-query path, network round trips, storage format, SQL,
transaction, durability, recovery or concurrency changes. File size remains
caller-controlled as before. Rollback of demo/docs together would restore the
DER-only requirement. Preserve earlier uncommitted opaque-token and adapter work.
Test multi-certificate PEM trust (including CRLF), empty/malformed/invalid/mixed
bundles, wrong-host and unrelated-root rejection, and actual TLS/H2. Verify
SQLite examples against the unchanged remote core plus isolated fixed-core
integration. No commit or push is authorized.


## 2026-09-28: published core fixes dependency refresh

User explicitly authorized commit/push of the three core SDK repositories, then
updating their SQLite adapter dependencies. sdk-rust main was pushed and read back
at 437d0e4efdf685f91b75422c4d0d3ef46ee14063. Pin this exact immutable remote Git HTTPS revision in the manifest,
README and lockfile where present. Preserve existing adapter implementation,
examples and editor swap files. No local path/link/editable-core dependency and
no third-party version refresh. This brings the opaque credential fix into this
adapter; Rust additionally receives the PEM CA builder API. Existing transport,
no-replay, transaction, storage and resource-bound contracts remain unchanged.
Rollback means restoring the previous pin and corresponding lock/doc together;
it also restores old fixed-format token rejection. Validate installed packages
against the remotely retrieved revision, with source provenance checked, plus
relevant native tests/types/builds. Direct HTTPS authentication is unavailable on
this machine; separately identified remote-transfer validation may use existing
SSH authentication with a process-scoped Git rewrite, never a product dependency
fallback or a claim of anonymous HTTPS success. The production URL remains HTTPS.
Only core repositories were authorized for commit/push; adapter changes remain
uncommitted for review. No SQLite adapter publication is authorized.

## 2026-10-08: v1.0.0 dependency and release verification

User authorizes dependency repair, installed-package and demo validation, then
squashing this repository to one commit and publishing main plus v1.0.0 only.
Use the core v1.0.0 release (Go module version; immutable Git HTTPS revision for
Node/Rust/Python), superseding the old historical pins above. Preserve third-party
versions, TLS verification, no SQL replay and transaction/storage semantics.
Use a disposable local App SQLite for demo verification; never production data.
