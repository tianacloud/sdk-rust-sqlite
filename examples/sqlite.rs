//! Run with TIANA_ENDPOINT set; see README for optional token and TLS settings.
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::{env, error::Error, fs, process::ExitCode};
use tiana_sdk::{Client, ClientBuilder, SecretToken};
use tiana_sdk_sqlite::{Session, Statement, TransactionMode, Value};
use zeroize::Zeroizing;

type DemoResult<T> = Result<T, Box<dyn Error>>;

#[tokio::main]
async fn main() -> ExitCode {
    if env::args().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Tiana SQLite demo\n\
             Required: TIANA_ENDPOINT\n\
             Optional: TIANA_TOKEN, TIANA_CA_FILE, TIANA_GATEWAY_ADDRESS\n\
             TIANA_CA_FILE accepts a PEM CA certificate bundle.\n\
             Creates a connection-local TEMP table, commits one insert, rolls back\n\
             another, queries typed values and explicitly closes the session."
        );
        return ExitCode::SUCCESS;
    }
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // SDK errors are bounded and redacted; do not print config or tokens.
            eprintln!("demo failed: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> DemoResult<()> {
    let endpoint = env::var("TIANA_ENDPOINT").map_err(|_| "TIANA_ENDPOINT must be set")?;
    let mut builder = Client::builder(endpoint);
    if let Some(token) = env::var_os("TIANA_TOKEN") {
        let token = Zeroizing::new(
            token
                .into_string()
                .map_err(|_| "TIANA_TOKEN must be UTF-8")?,
        );
        builder = builder.token(SecretToken::parse(token.as_bytes())?);
    }
    if let Some(path) = env::var_os("TIANA_CA_FILE") {
        let certificate = fs::read(path).map_err(|_| "cannot read TIANA_CA_FILE")?;
        builder = with_pem_roots(builder, &certificate)?;
    }
    if let Some(address) = env::var_os("TIANA_GATEWAY_ADDRESS") {
        builder = builder.gateway_address(
            address
                .into_string()
                .map_err(|_| "TIANA_GATEWAY_ADDRESS must be UTF-8")?,
        );
    }
    let mut session = Session::new(builder.build()?);
    let result = demonstrate(&mut session).await;
    // Always attempt bounded server close, including when demonstration SQL fails.
    let closed = session.close().await;
    if let Err(error) = result {
        if let Err(close_error) = closed {
            eprintln!("session close failed: {close_error}");
        }
        return Err(error);
    }
    closed?;
    println!("session closed");
    Ok(())
}

async fn demonstrate(session: &mut Session) -> DemoResult<()> {
    // TEMP keeps repeated demo runs isolated from persistent application tables.
    session
        .execute(&Statement::new(
            "CREATE TEMP TABLE demo_items (id INTEGER PRIMARY KEY, label TEXT, payload BLOB)",
        ))
        .await?;
    session.begin(TransactionMode::Immediate).await?;
    session
        .execute(
            &Statement::new("INSERT INTO demo_items VALUES (?, ?, ?)").args([
                Value::Integer(i64::MAX),
                Value::from("hello Tiana"),
                Value::Blob(vec![0, 255]),
            ]),
        )
        .await?;
    session.commit().await?;

    let rows = session
        .query(
            &Statement::new("SELECT id, label, payload FROM demo_items WHERE id = :id")
                .named_args([("id".to_owned(), Value::Integer(i64::MAX))]),
        )
        .await?;
    match rows.rows.as_slice() {
        [row] => match row.as_slice() {
            [Value::Integer(id), Value::Text(label), Value::Blob(payload)] => {
                if *id != i64::MAX || label != "hello Tiana" || payload != &[0, 255] {
                    return Err("unexpected demo values".into());
                }
                println!(
                    "committed row: id={id}, label={label}, blob_bytes={}",
                    payload.len()
                );
            }
            _ => return Err("unexpected demo column types".into()),
        },
        _ => return Err("unexpected demo row count".into()),
    }

    session.begin(TransactionMode::Deferred).await?;
    session
        .execute(
            &Statement::new("INSERT INTO demo_items (id) VALUES (?)").args([Value::Integer(1)]),
        )
        .await?;
    session.rollback().await?;
    let count = session
        .query(&Statement::new("SELECT count(*) FROM demo_items"))
        .await?;
    if count.rows != vec![vec![Value::Integer(1)]] {
        return Err("rollback verification failed".into());
    }
    println!("rollback verified: rows=1");
    Ok(())
}

// Decode PEM in the example so its immutable remote SDK revision remains usable.
// Certificate validation and all transport security remain owned by tiana-sdk.
fn with_pem_roots(mut builder: ClientBuilder, pem: &[u8]) -> DemoResult<ClientBuilder> {
    builder = builder.use_webpki_roots(false);
    let mut found = false;
    for certificate in CertificateDer::pem_slice_iter(pem) {
        let certificate = certificate.map_err(|_| "invalid PEM in TIANA_CA_FILE")?;
        builder = builder.add_root_certificate_der(certificate.as_ref().to_vec());
        found = true;
    }
    if !found {
        return Err("TIANA_CA_FILE must contain at least one PEM certificate".into());
    }
    Ok(builder)
}

#[cfg(test)]
#[allow(dead_code)]
#[path = "../tests/support/tls_fixtures.rs"]
mod fixtures;

#[cfg(test)]
mod ca_tests {
    use super::*;
    const ENDPOINT: &str = "ep-00000000000000000000000000.db.example.test";

    #[test]
    fn pem_bundle_is_accepted_and_invalid_input_fails_closed() {
        let cert = format!(
            "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
            fixtures::ENDPOINT_CERTIFICATE_DER_B64
        );
        let bundle = format!("{cert}{cert}");
        assert!(
            with_pem_roots(Client::builder(ENDPOINT), bundle.as_bytes())
                .unwrap()
                .build()
                .is_ok()
        );
        for invalid in [
            "",
            "not a certificate",
            "-----BEGIN CERTIFICATE-----\n!\n-----END CERTIFICATE-----\n",
        ] {
            assert!(with_pem_roots(Client::builder(ENDPOINT), invalid.as_bytes()).is_err());
        }
        let invalid_der = b"-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----\n";
        assert!(
            with_pem_roots(Client::builder(ENDPOINT), invalid_der)
                .unwrap()
                .build()
                .is_err()
        );
        let mixed = format!("{cert}-----BEGIN CERTIFICATE-----\n!\n-----END CERTIFICATE-----\n");
        assert!(with_pem_roots(Client::builder(ENDPOINT), mixed.as_bytes()).is_err());
    }
}
