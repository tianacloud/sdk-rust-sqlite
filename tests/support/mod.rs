#[allow(dead_code)]
mod tls_fixtures;
use bytes::Bytes;
use hyper::{Request, Response};
use rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tiana_sdk::{Client, SecretToken};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_rustls::TlsAcceptor;

pub enum Reply {
    Json(Value),
    Http {
        status: u16,
        headers: String,
        body: Vec<u8>,
    },
    Hang,
    Disconnect,
    Relay(String),
    Raw(Vec<u8>),
}
pub struct Gateway {
    pub client: Client,
    pub requests: Arc<Mutex<Vec<Value>>>,
    pub connects: Arc<AtomicUsize>,
    pub resets: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}
impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Gateway {
    pub async fn start(replies: Vec<Reply>) -> Self {
        let certificate = tls_fixtures::decode(tls_fixtures::ENDPOINT_CERTIFICATE_DER_B64);
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(tls_fixtures::decode(
            tls_fixtures::ENDPOINT_KEY_DER_B64,
        )));
        let mut tls =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![CertificateDer::from(certificate.clone())], key)
                .unwrap();
        tls.alpn_protocols = vec![b"h2".to_vec()];
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Client::builder("ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.service.internal.tiana.com")
            .gateway_address(listener.local_addr().unwrap().to_string())
            .use_webpki_roots(false)
            .add_root_certificate_der(certificate)
            .token(SecretToken::parse(format!("tia_{}", "A".repeat(43))).unwrap())
            .build()
            .unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let connects = Arc::new(AtomicUsize::new(0));
        let resets = Arc::new(AtomicUsize::new(0));
        let (seen, count, dropped) = (requests.clone(), connects.clone(), resets.clone());
        let replies = Arc::new(replies);
        let acceptor = TlsAcceptor::from(Arc::new(tls));
        let task = tokio::spawn(async move {
            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let (acceptor, seen, count, dropped, replies) = (
                    acceptor.clone(),
                    seen.clone(),
                    count.clone(),
                    dropped.clone(),
                    replies.clone(),
                );
                tokio::spawn(async move {
                    let tls = acceptor.accept(tcp).await.unwrap();
                    let mut h2 = h2::server::handshake(tls).await.unwrap();
                    while let Some(Ok((request, response))) = h2.accept().await {
                        count.fetch_add(1, Ordering::SeqCst);
                        tokio::spawn(serve(
                            request,
                            response,
                            seen.clone(),
                            dropped.clone(),
                            replies.clone(),
                        ));
                    }
                });
            }
        });
        Self {
            client,
            requests,
            connects,
            resets,
            task,
        }
    }
    pub async fn wait_requests(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.requests.lock().unwrap().len() < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    pub async fn wait_reset(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.resets.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
async fn serve(
    request: Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    seen: Arc<Mutex<Vec<Value>>>,
    resets: Arc<AtomicUsize>,
    replies: Arc<Vec<Reply>>,
) {
    assert_eq!(request.method(), "CONNECT");
    assert_eq!(request.headers()["tiana-database-protocol"], "hrana-http");
    assert!(request.headers().contains_key("proxy-authorization"));
    let reply = Response::builder()
        .status(200)
        .header("tiana-tunnel-version", "1")
        .header("tiana-auth-mode", "TOKEN_REQUIRED")
        .header("tiana-request-id", &request.headers()["tiana-request-id"])
        .body(())
        .unwrap();
    let mut send = respond.send_response(reply, false).unwrap();
    let mut body = request.into_body();
    if let Some(Reply::Relay(address)) = replies.first() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let tcp = tokio::net::TcpStream::connect(address).await.unwrap();
        let (mut reader, mut writer) = tcp.into_split();
        let to_app = async {
            while let Some(data) = body.data().await {
                let data = data.map_err(std::io::Error::other)?;
                writer.write_all(&data).await?;
                body.flow_control()
                    .release_capacity(data.len())
                    .map_err(std::io::Error::other)?;
            }
            writer.shutdown().await
        };
        let from_app = async {
            let mut buffer = [0; 16384];
            loop {
                let n = reader.read(&mut buffer).await?;
                if n == 0 {
                    send.send_data(Bytes::new(), true)
                        .map_err(std::io::Error::other)?;
                    break;
                }
                let mut data = Bytes::copy_from_slice(&buffer[..n]);
                while !data.is_empty() {
                    send.reserve_capacity(data.len());
                    let capacity = std::future::poll_fn(|cx| send.poll_capacity(cx))
                        .await
                        .ok_or_else(|| std::io::Error::other("stream closed"))?
                        .map_err(std::io::Error::other)?;
                    let count = capacity.min(data.len());
                    if count > 0 {
                        send.send_data(data.split_to(count), false)
                            .map_err(std::io::Error::other)?;
                    }
                }
            }
            Ok::<_, std::io::Error>(())
        };
        let _ = tokio::try_join!(to_app, from_app);
        return;
    }
    let mut pending = Vec::new();
    for reply in replies.iter() {
        let parsed = loop {
            if let Some(start) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
                let header = std::str::from_utf8(&pending[..start]).unwrap();
                assert!(header.starts_with("POST /v3/pipeline HTTP/1.1\r\n"));
                let lower = header.to_ascii_lowercase();
                assert!(!lower.contains("authorization"));
                assert!(!lower.contains("tia_"));
                let length: usize = lower
                    .lines()
                    .find_map(|v| v.strip_prefix("content-length: "))
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                if pending.len() >= start + 4 + length {
                    let value =
                        serde_json::from_slice(&pending[start + 4..start + 4 + length]).unwrap();
                    pending.drain(..start + 4 + length);
                    break value;
                }
            }
            match body.data().await {
                Some(Ok(data)) => {
                    body.flow_control().release_capacity(data.len()).unwrap();
                    pending.extend_from_slice(&data);
                }
                _ => {
                    resets.fetch_add(1, Ordering::SeqCst);
                    return;
                }
            }
        };
        seen.lock().unwrap().push(parsed);
        let bytes = match reply {
            Reply::Raw(bytes) => bytes.clone(),
            Reply::Hang => {
                let _ = body.data().await;
                resets.fetch_add(1, Ordering::SeqCst);
                return;
            }
            Reply::Disconnect => {
                send.send_reset(h2::Reason::CANCEL);
                return;
            }
            Reply::Relay(_) => unreachable!(),
            reply => {
                let (status, headers, bytes) = match reply {
                    Reply::Json(v) => (200, "", serde_json::to_vec(v).unwrap()),
                    Reply::Http {
                        status,
                        headers,
                        body,
                    } => (*status, headers.as_str(), body.clone()),
                    _ => unreachable!(),
                };
                [format!("HTTP/1.1 {status} response\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{headers}\r\n", bytes.len()).into_bytes(), bytes].concat()
            }
        };
        let mut remaining = Bytes::from(bytes);
        while !remaining.is_empty() {
            send.reserve_capacity(remaining.len().min(16384));
            let Some(Ok(capacity)) = std::future::poll_fn(|cx| send.poll_capacity(cx)).await else {
                return;
            };
            let n = capacity.min(remaining.len()).min(16384);
            if n > 0 && send.send_data(remaining.split_to(n), false).is_err() {
                return;
            }
        }
    }
    // Keep the tunnel open until the client explicitly releases it.
    let _ = body.data().await;
    resets.fetch_add(1, Ordering::SeqCst);
}
pub fn success(baton: Option<&str>, autocommit: bool, close: bool) -> Value {
    let mut results = vec![
        json!({"type":"ok","response":{"type":"execute","result":{
            "cols":[],"rows":[],"affected_row_count":1,"last_insert_rowid":"9223372036854775807"
        }}}),
        json!({"type":"ok","response":{"type":"get_autocommit","is_autocommit":autocommit}}),
    ];
    if close {
        results.push(json!({"type":"ok","response":{"type":"close"}}));
    }
    json!({"baton":baton,"base_url":null,"results":results})
}
pub fn closed() -> Value {
    json!({"baton":null,"results":[{"type":"ok","response":{"type":"close"}}]})
}
