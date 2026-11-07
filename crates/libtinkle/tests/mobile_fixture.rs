//! Shared mobile harness regression, driven by the real native client/SDK.
use libtinkle::{Client, ClientConfig, InMemoryCursorStore, ServerTrust, TrustSource};
use serde_json::{json, Value};
use tinkle_schema::proto::{target, GetHistoryRequest, Target};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

async fn control(request: Value) -> Value {
    let stream = tokio::net::TcpStream::connect("127.0.0.1:48080")
        .await
        .unwrap();
    let mut stream = BufReader::new(stream);
    stream
        .get_mut()
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_line(&mut response).await.unwrap();
    serde_json::from_str(&response).unwrap()
}

#[tokio::test]
#[ignore = "run via scripts/with-native-fixture.py cargo test -p libtinkle --test mobile_fixture -- --ignored"]
async fn mobile_fixture_survives_faulty_control_requests() {
    for input in [&b""[..], b"\n", b"not json\n", b"\xff\n", b"{"] {
        let mut stream = tokio::net::TcpStream::connect("127.0.0.1:48080")
            .await
            .unwrap();
        stream.write_all(input).await.unwrap();
        if input.is_empty() {
            stream.shutdown().await.unwrap();
        }
        let mut response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.read_to_end(&mut response),
        )
        .await
        .expect("faulty connection should be dropped")
        .unwrap();
        assert!(response.is_empty());
        assert_eq!(
            control(json!({"op":"unknown"})).await,
            json!({"error":"unknown operation"})
        );
    }
}

#[tokio::test]
#[ignore = "run via scripts/with-native-fixture.py cargo test -p libtinkle --test mobile_fixture -- --ignored"]
async fn mobile_fixture_serves_encrypted_journal_history_and_exact_i64_cursors() {
    let fixture = control(json!({"op":"create", "deletions":["target:room:7"]})).await;
    let id = fixture["id"].as_u64().unwrap();
    let pin: Vec<u8> = serde_json::from_value(fixture["pin"].clone()).unwrap();
    let info = libtinkle::ClientInfo::new(
        "fixture".into(),
        "test".into(),
        "1".into(),
        "1".into(),
        "1".into(),
        Default::default(),
    )
    .unwrap();
    let mut config = ClientConfig::new("127.0.0.1", info);
    config.primary_port = fixture["port"].as_u64().unwrap().try_into().unwrap();
    config.fallback_port = None;
    let client = Client::new(
        config,
        TrustSource::Bootstrap(ServerTrust {
            server_public_key: pin.try_into().unwrap(),
            server_key_id: 7,
            scope: "tinkle-mobile-fixture".into(),
        }),
        InMemoryCursorStore::new(),
    );
    client.connect().await.unwrap();
    let room = Target {
        kind: Some(target::Kind::ChatId(7)),
    };
    let page = client.get_target_changes(room, 0, 100).await.unwrap();
    assert_eq!(page.changes.len(), 1);
    assert_eq!(page.changes[0].message_id, 101);
    assert_eq!(page.last_accept_seq, 1);
    let history = client
        .messages()
        .get_history(GetHistoryRequest {
            target: Some(room),
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(history.messages.is_empty());
    assert_eq!(history.more_available, Some(false));
    let position = 1i64 << 40;
    let page = client
        .get_target_changes(room, position, 100)
        .await
        .unwrap();
    assert!(page.changes.is_empty());
    assert_eq!(page.last_accept_seq, position);
    let log = control(json!({"op":"requests", "id":id})).await;
    assert_eq!(log["requests"][0]["after_accept_seq"], 0);
    assert_eq!(log["requests"][1]["after_accept_seq"], position);
    assert_eq!(log["history"], json!(["target:room:7"]));
    client.close().await;
    assert_eq!(
        control(json!({"op":"close", "id":id})).await["closed"],
        true
    );
}
