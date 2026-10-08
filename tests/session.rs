mod support;
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};
use support::{Gateway, Reply, closed, success};
use tiana_sdk_sqlite::{MAX_BODY_BYTES, Session, Statement, TransactionMode, Value};

#[tokio::test]
async fn typed_rows_named_and_positional_args_and_rotating_baton() {
    let mut reply = success(Some("first-secret-baton"), true, false);
    reply["results"][0]["response"]["result"]["cols"] = json!([
        {"name":"nil"},{"name":"int","decltype":"INTEGER"},{"name":"float"},{"name":"text"},{"name":"blob"}
    ]);
    reply["results"][0]["response"]["result"]["rows"] = json!([[
        {"type":"null"},{"type":"integer","value":"-9223372036854775808"},
        {"type":"float","value":1.25},{"type":"text","value":"你好\u{0000}"},{"type":"blob","base64":"AP8"}
    ]]);
    let gateway = Gateway::start(vec![
        Reply::Json(reply),
        Reply::Json(success(Some("second-baton"), true, false)),
        Reply::Json(closed()),
    ])
    .await;
    let mut session = Session::new(gateway.client.clone());
    assert_eq!(session.autocommit(), Some(true));
    assert!(session.request_id().is_none());
    let values = vec![
        Value::Null,
        Value::Integer(i64::MIN),
        Value::Float(1.25),
        Value::Text("你好\0".into()),
        Value::Blob(vec![0, 255]),
    ];
    let result = session
        .query(&Statement::new("SELECT ?,?,?,?,?").args(values.clone()))
        .await
        .unwrap();
    assert!(result.rows[0] == values);
    assert_eq!(result.last_insert_rowid, Some(i64::MAX));
    assert_eq!(result.affected_row_count, 1);
    assert_eq!(result.columns[1].decltype.as_deref(), Some("INTEGER"));
    assert!(session.request_id().unwrap().starts_with("req-"));
    session
        .execute(&Statement::new("SELECT :value").named_args([("value".into(), Value::from(true))]))
        .await
        .unwrap();
    session.close().await.unwrap();
    session.close().await.unwrap();
    let requests = gateway.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[0]["baton"].is_null());
    assert_eq!(
        requests[0]["requests"][0]["stmt"]["args"][1]["value"],
        i64::MIN.to_string()
    );
    assert_eq!(
        requests[0]["requests"][0]["stmt"]["args"][4]["base64"],
        "AP8"
    );
    assert_eq!(requests[0]["requests"][0]["stmt"]["want_rows"], true);
    assert_eq!(requests[1]["baton"], "first-secret-baton");
    assert_eq!(
        requests[1]["requests"][0]["stmt"]["named_args"],
        json!([{"name":"value","value":{"type":"integer","value":"1"}}])
    );
    assert_eq!(requests[1]["requests"][0]["stmt"]["want_rows"], false);
    assert_eq!(requests[2]["baton"], "second-baton");
    assert_eq!(gateway.connects.load(Ordering::SeqCst), 1);
    assert_eq!(session.autocommit(), None);
    assert!(!session.is_usable());
}

#[tokio::test]
async fn transaction_state_is_confirmed_and_failed_commit_can_be_rolled_back() {
    let mut busy = success(Some("3"), false, false);
    busy["results"][0] =
        json!({"type":"error","error":{"code":"SQLITE_BUSY","message":"secret SQL"}});
    let gateway = Gateway::start(vec![
        Reply::Json(success(Some("1"), false, false)),
        Reply::Json(success(Some("2"), false, false)),
        Reply::Json(busy),
        Reply::Json(success(Some("4"), true, false)),
        Reply::Json(success(Some("5"), false, false)),
        Reply::Json(success(Some("6"), true, false)),
        Reply::Json(closed()),
    ])
    .await;
    let mut session = Session::new(gateway.client.clone());
    assert_eq!(
        session.commit().await.err().unwrap().code(),
        "TRANSACTION_STATE"
    );
    session.begin(TransactionMode::Immediate).await.unwrap();
    assert_eq!(session.autocommit(), Some(false));
    assert_eq!(
        session
            .begin(TransactionMode::Deferred)
            .await
            .err()
            .unwrap()
            .code(),
        "TRANSACTION_STATE"
    );
    session
        .execute(&Statement::new("INSERT INTO t VALUES (1)"))
        .await
        .unwrap();
    let err = session.commit().await.unwrap_err();
    assert_eq!(err.code(), "SQLITE_BUSY");
    assert!(!err.outcome_unknown());
    assert!(!format!("{err:?} {err}").contains("secret"));
    assert_eq!(session.autocommit(), Some(false));
    session.rollback().await.unwrap();
    assert_eq!(session.autocommit(), Some(true));
    session.begin(TransactionMode::Exclusive).await.unwrap();
    session.commit().await.unwrap();
    session.close().await.unwrap();
    let requests = gateway.requests.lock().unwrap();
    assert_eq!(requests.len(), 7);
    let sql = |i: usize| requests[i]["requests"][0]["stmt"]["sql"].as_str().unwrap();
    assert_eq!(sql(0), "BEGIN IMMEDIATE");
    assert_eq!(sql(2), "COMMIT");
    assert_eq!(sql(3), "ROLLBACK");
}

#[tokio::test]
async fn raw_sql_state_and_execute_and_close() {
    let gateway = Gateway::start(vec![
        Reply::Json(success(Some("1"), false, false)),
        Reply::Json(success(None, true, true)),
    ])
    .await;
    let mut session = Session::new(gateway.client.clone());
    session.execute(&Statement::new("BEGIN")).await.unwrap();
    assert_eq!(session.autocommit(), Some(false));
    session
        .execute_and_close(&Statement::new("COMMIT"))
        .await
        .unwrap();
    assert!(!session.is_usable());
    assert_eq!(
        session
            .execute(&Statement::new("SELECT 1"))
            .await
            .err()
            .unwrap()
            .code(),
        "SESSION_CLOSED"
    );
    assert_eq!(
        gateway.requests.lock().unwrap()[1]["requests"][2]["type"],
        "close"
    );
}

#[tokio::test]
async fn invalid_arguments_are_rejected_without_opening_or_poisoning() {
    let gateway = Gateway::start(vec![]).await;
    let mut session = Session::new(gateway.client.clone());
    let invalid = [
        Statement::new("select ?").args([Value::Float(f64::NAN)]),
        Statement::new("select ?").args([Value::Float(f64::INFINITY)]),
        Statement::new("select :x")
            .named_args([("x".into(), Value::Null), ("x".into(), Value::Null)]),
        Statement::new("select ?").args([Value::Blob(vec![0; MAX_BODY_BYTES])]),
        Statement::new("x".repeat(MAX_BODY_BYTES + 1)),
        // JSON escaping alone pushes the encoded body above the bound.
        Statement::new("\0".repeat(MAX_BODY_BYTES / 4)),
    ];
    for stmt in invalid {
        assert!(session.execute(&stmt).await.is_err());
        assert!(session.is_usable());
    }
    assert_eq!(session.autocommit(), Some(true));
    assert_eq!(gateway.connects.load(Ordering::SeqCst), 0);
    session.close().await.unwrap();
    assert_eq!(gateway.connects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropped_operation_and_timeout_reset_session_without_replay() {
    for internal_timeout in [true, false] {
        let gateway = Gateway::start(vec![Reply::Hang]).await;
        let mut session = Session::new(gateway.client.clone())
            .with_request_timeout(Duration::from_millis(if internal_timeout {
                100
            } else {
                3000
            }))
            .unwrap();
        let stmt = Statement::new("INSERT INTO t VALUES ('sensitive')");
        if internal_timeout {
            let error = session.execute(&stmt).await.err().unwrap();
            assert_eq!(error.code(), "TIMEOUT");
            assert!(error.outcome_unknown());
            assert!(error.request_id().is_some());
        } else {
            let mut operation = Box::pin(session.execute(&stmt));
            tokio::select! { result=&mut operation => panic!("unexpected completion {}",result.is_ok()), _=gateway.wait_requests(1) => {} }
            drop(operation);
        }
        assert!(!session.is_usable());
        assert_eq!(session.autocommit(), None);
        gateway.wait_reset().await;
        assert_eq!(
            session.execute(&stmt).await.err().unwrap().code(),
            "SESSION_UNUSABLE"
        );
        assert!(session.close().await.is_err());
        assert_eq!(gateway.requests.lock().unwrap().len(), 1);
        assert_eq!(gateway.connects.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn disconnect_after_request_is_unknown_and_never_reconnected() {
    let gateway = Gateway::start(vec![Reply::Disconnect]).await;
    let mut session = Session::new(gateway.client.clone());
    let error = session
        .execute(&Statement::new("INSERT INTO t VALUES(1)"))
        .await
        .err()
        .unwrap();
    assert!(error.outcome_unknown());
    assert!(!session.is_usable());
    assert!(session.execute(&Statement::new("SELECT 1")).await.is_err());
    assert_eq!(gateway.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn malformed_responses_poison_without_exposing_server_content() {
    let good = success(Some("baton-secret"), true, false);
    let mut cases = Vec::new();
    let mut v = good.clone();
    v["base_url"] = json!("https://evil.example/secret");
    cases.push(v);
    let mut v = good.clone();
    v["baton"] = json!("");
    cases.push(v);
    let mut v = good.clone();
    v["baton"] = json!("x".repeat(4097));
    cases.push(v);
    let mut v = good.clone();
    v["results"][1]["response"]["is_autocommit"] = json!(null);
    cases.push(v);
    let mut v = good.clone();
    v["results"][1]["response"]["type"] = json!("close");
    cases.push(v);
    let mut v = good.clone();
    v["results"][0]["error"] = json!({"code":"secret"});
    cases.push(v);
    let mut v = good.clone();
    v["results"][0]["response"]["result"]["affected_row_count"] = json!(null);
    cases.push(v);
    let mut v = good.clone();
    v["results"][0]["response"]["result"]["rows"] = json!([[{"type":"null"}]]);
    cases.push(v);
    let mut v = good.clone();
    v["results"][0]["response"]["result"]["last_insert_rowid"] = json!("9223372036854775808");
    cases.push(v);
    for value in [
        json!({"type":"integer","value":"9223372036854775808"}),
        json!({"type":"integer","value":7}),
        json!({"type":"blob","base64":"!!!!"}),
        json!({"type":"blob","base64":"AP8="}),
        json!({"type":"float","value":null}),
        json!({"type":"unknown","value":"secret"}),
        json!({"type":"null","value":"secret"}),
    ] {
        let mut v = good.clone();
        v["results"][0]["response"]["result"]["cols"] = json!([{"name":"x"}]);
        v["results"][0]["response"]["result"]["rows"] = json!([[value]]);
        cases.push(v);
    }
    for (case, response) in cases.into_iter().enumerate() {
        let gateway = Gateway::start(vec![Reply::Json(response)]).await;
        let mut session = Session::new(gateway.client.clone());
        let error = session
            .query(&Statement::new("SELECT 'secret'"))
            .await
            .err()
            .unwrap_or_else(|| panic!("must reject malformed response case {case}"));
        assert!(error.outcome_unknown());
        assert_eq!(error.code(), "INVALID_RESPONSE");
        assert!(!format!("{error:?} {error}").contains("secret"));
        assert!(!session.is_usable());
        assert_eq!(session.autocommit(), None);
    }
}

#[tokio::test]
async fn uncertain_sql_errors_are_terminal_but_known_errors_can_be_reused() {
    for code in [
        "SQLITE_ERROR",
        "SQLITE_CONSTRAINT",
        "RESULT_TOO_LARGE",
        "RESPONSE_TOO_LARGE",
        "SQLITE_IOERR",
        "INVENTED_secret",
    ] {
        let mut reply = success(Some("b"), true, false);
        reply["results"][0] = json!({"type":"error","error":{"code":code,"message":"private"}});
        let gateway = Gateway::start(vec![Reply::Json(reply), Reply::Json(closed())]).await;
        let mut session = Session::new(gateway.client.clone());
        let error = session
            .execute(&Statement::new("select 1"))
            .await
            .err()
            .unwrap();
        let known = matches!(code, "SQLITE_ERROR" | "SQLITE_CONSTRAINT");
        assert_eq!(error.outcome_unknown(), !known);
        assert_eq!(session.is_usable(), known);
        assert!(!format!("{error:?}").contains("private"));
        assert!(!format!("{error:?}").contains("secret"));
        if known {
            session.close().await.unwrap();
        } else {
            assert!(session.close().await.is_err());
        }
    }
}

#[tokio::test]
async fn http_rejections_bounds_and_invalid_encoding_are_terminal() {
    for (status, headers, body, expected, unknown) in [
        (
            400,
            "",
            br#"{"code":"BATON_INVALID","message":"secret"}"#.to_vec(),
            "BATON_INVALID",
            false,
        ),
        (
            503,
            "",
            br#"{"code":"STREAM_EXPIRED"}"#.to_vec(),
            "STREAM_EXPIRED",
            true,
        ),
        (
            302,
            "Location: https://example.com/\r\n",
            Vec::new(),
            "HTTP_REJECTED",
            true,
        ),
        (
            200,
            "Content-Encoding: gzip\r\n",
            Vec::new(),
            "INVALID_RESPONSE",
            true,
        ),
        (200, "", vec![0xff], "INVALID_RESPONSE", true),
        (
            200,
            "",
            vec![b' '; MAX_BODY_BYTES + 1],
            "RESPONSE_TOO_LARGE",
            true,
        ),
    ] {
        let gateway = Gateway::start(vec![Reply::Http {
            status,
            headers: headers.into(),
            body,
        }])
        .await;
        let mut session = Session::new(gateway.client.clone());
        let error = session
            .execute(&Statement::new("SELECT 1"))
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), expected);
        assert_eq!(error.outcome_unknown(), unknown);
        assert!(!session.is_usable());
    }
}

#[tokio::test]
async fn successful_http_close_preserves_result_but_never_reuses_session() {
    let gateway = Gateway::start(vec![Reply::Http {
        status: 200,
        headers: "Connection: close\r\n".into(),
        body: serde_json::to_vec(&success(Some("b"), true, false)).unwrap(),
    }])
    .await;
    let mut session = Session::new(gateway.client.clone());
    assert_eq!(
        session
            .execute(&Statement::new("SELECT 1"))
            .await
            .unwrap()
            .affected_row_count,
        1
    );
    assert!(!session.is_usable());
    assert_eq!(session.autocommit(), None);
}

#[tokio::test]
async fn unconfirmed_close_or_transaction_acknowledgement_poison_session() {
    let gateway = Gateway::start(vec![Reply::Json(success(Some("b"), true, false))]).await;
    let mut session = Session::new(gateway.client.clone());
    assert!(
        session
            .begin(TransactionMode::Deferred)
            .await
            .unwrap_err()
            .outcome_unknown()
    );
    assert!(!session.is_usable());
    let gateway = Gateway::start(vec![Reply::Json(success(Some("b"), true, true))]).await;
    let mut session = Session::new(gateway.client.clone());
    assert!(
        session
            .execute_and_close(&Statement::new("SELECT 1"))
            .await
            .err()
            .unwrap()
            .outcome_unknown()
    );
    assert!(!session.is_usable());
}

/// Run only against an explicitly started disposable App SQLite database.
#[tokio::test]
#[ignore = "requires TIANA_SQLITE_TEST_ADDRESS pointing to a disposable loopback App SQLite"]
async fn real_app_sqlite_transactions_values_and_session_isolation() {
    let address: std::net::SocketAddr = std::env::var("TIANA_SQLITE_TEST_ADDRESS")
        .expect("disposable test address")
        .parse()
        .unwrap();
    assert!(
        address.ip().is_loopback(),
        "real integration test must target a disposable loopback service"
    );
    let gateway = Gateway::start(vec![Reply::Relay(address.to_string())]).await;
    let mut first = Session::new(gateway.client.clone());
    let mut second = Session::new(gateway.client.clone());
    first
        .execute(&Statement::new(
            "CREATE TABLE sdk_test (id INTEGER PRIMARY KEY, text_value TEXT, blob_value BLOB)",
        ))
        .await
        .unwrap();
    first.begin(TransactionMode::Immediate).await.unwrap();
    first
        .execute(
            &Statement::new("INSERT INTO sdk_test VALUES (?, ?, ?)").args([
                Value::Integer(1),
                Value::from("discard"),
                Value::Blob(vec![0]),
            ]),
        )
        .await
        .unwrap();
    let count = second
        .query(&Statement::new("SELECT count(*) FROM sdk_test"))
        .await
        .unwrap();
    assert!(count.rows == vec![vec![Value::Integer(0)]]);
    first.rollback().await.unwrap();
    first.begin(TransactionMode::Immediate).await.unwrap();
    first
        .execute(
            &Statement::new("INSERT INTO sdk_test VALUES (:id, :text, :blob)").named_args([
                ("id".into(), Value::Integer(i64::MAX)),
                ("text".into(), Value::from("中文\0text")),
                ("blob".into(), Value::Blob(vec![0, 255, 128])),
            ]),
        )
        .await
        .unwrap();
    first
        .execute(&Statement::new("SAVEPOINT nested"))
        .await
        .unwrap();
    first
        .execute(&Statement::new(
            "INSERT INTO sdk_test VALUES (2, 'rolled back', NULL)",
        ))
        .await
        .unwrap();
    first
        .execute(&Statement::new("ROLLBACK TO nested"))
        .await
        .unwrap();
    first
        .execute(&Statement::new("RELEASE nested"))
        .await
        .unwrap();
    assert_eq!(first.autocommit(), Some(false));
    first.commit().await.unwrap();
    let result = second
        .query(&Statement::new(
            "SELECT id,text_value,blob_value FROM sdk_test",
        ))
        .await
        .unwrap();
    assert!(
        result.rows
            == vec![vec![
                Value::Integer(i64::MAX),
                Value::from("中文\0text"),
                Value::Blob(vec![0, 255, 128])
            ]]
    );
    let error = first
        .execute(&Statement::new(
            "INSERT INTO sdk_test VALUES (9223372036854775807,NULL,NULL)",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), "SQLITE_CONSTRAINT");
    assert!(!error.outcome_unknown());
    assert!(first.is_usable());
    first.begin(TransactionMode::Deferred).await.unwrap();
    first
        .execute(&Statement::new("INSERT INTO sdk_test VALUES (3,NULL,NULL)"))
        .await
        .unwrap();
    // Explicit close releases the stream and rolls back its uncommitted work.
    first.close().await.unwrap();
    let count = second
        .query(&Statement::new("SELECT count(*) FROM sdk_test"))
        .await
        .unwrap();
    assert!(count.rows == vec![vec![Value::Integer(1)]]);
    second.close().await.unwrap();
    assert_eq!(gateway.connects.load(Ordering::SeqCst), 2);
    let mut reopened = Session::new(gateway.client.clone());
    let count = reopened
        .query(&Statement::new("SELECT count(*) FROM sdk_test"))
        .await
        .unwrap();
    assert!(count.rows == vec![vec![Value::Integer(1)]]);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn chunked_responses_obey_body_and_header_bounds() {
    fn chunked(body: &[u8]) -> Vec<u8> {
        [
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec(),
            format!("{:x}\r\n", body.len()).into_bytes(),
            body.to_vec(),
            b"\r\n0\r\n\r\n".to_vec(),
        ]
        .concat()
    }
    let gateway = Gateway::start(vec![Reply::Raw(chunked(
        &serde_json::to_vec(&success(None, true, true)).unwrap(),
    ))])
    .await;
    let mut session = Session::new(gateway.client.clone());
    session
        .execute_and_close(&Statement::new("SELECT 1"))
        .await
        .unwrap();
    let oversized = vec![b' '; MAX_BODY_BYTES + 1];
    for (response, code) in [
        (chunked(&oversized), "RESPONSE_TOO_LARGE"),
        (
            format!(
                "HTTP/1.1 200 OK\r\nX-Long: {}\r\nContent-Length: 0\r\n\r\n",
                "x".repeat(32768)
            )
            .into_bytes(),
            "TRANSPORT_ERROR",
        ),
    ] {
        let gateway = Gateway::start(vec![Reply::Raw(response)]).await;
        let mut session = Session::new(gateway.client.clone());
        let error = session
            .execute(&Statement::new("SELECT 1"))
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), code);
        assert!(error.outcome_unknown());
        assert!(!session.is_usable());
    }
}

#[tokio::test]
async fn connection_failure_happens_before_sql_and_retains_request_id() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let client = tiana_sdk::Client::builder("ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.com")
        .gateway_address(addr.to_string())
        .build()
        .unwrap();
    let mut session = Session::new(client);
    let error = session
        .execute(&Statement::new("INSERT INTO t VALUES (1)"))
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), "CONNECT_FAILED");
    assert!(!error.outcome_unknown());
    assert_eq!(session.request_id(), error.request_id());
    assert!(error.request_id().is_some());
    assert!(!session.is_usable());
    assert_eq!(session.autocommit(), None);
}

#[tokio::test]
async fn invalid_input_preserves_existing_transaction_and_drop_releases_tunnel() {
    let gateway = Gateway::start(vec![Reply::Json(success(Some("b"), false, false))]).await;
    let mut session = Session::new(gateway.client.clone());
    session.begin(TransactionMode::Deferred).await.unwrap();
    let error = session
        .execute(&Statement::new("SELECT ?").args([Value::Float(f64::NAN)]))
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), "INVALID_ARGUMENT");
    assert!(!error.outcome_unknown());
    assert_eq!(session.autocommit(), Some(false));
    assert!(session.is_usable());
    drop(session);
    gateway.wait_reset().await;
    assert_eq!(gateway.requests.lock().unwrap().len(), 1);
}
