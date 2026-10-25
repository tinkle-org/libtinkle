//! Live-gateway integration: handshake, concurrent RPCs, cancellation,
//! notification interleaving, reconnect, sync recovery. Spins an in-process
//! Syntra WSS-equivalent mock gateway.

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

use libtinkle::CursorStore;
use syntra_core::proto::{
    frame::Body, rpc_result::Outcome, Frame, Notification, RpcError as ProtoRpcError, RpcRequest,
    RpcResult,
};
use syntra_core::{decode_frame, encode_frame, ConnectionState, ValidationContext};
use tinkle_schema::proto::{
    peer_input::Target as PeerTarget, update::Payload as UpdatePayload, ConnectivityChanged,
    ContentOmitted, FollowUpdatesRequest, FollowUpdatesResponse, Message as TinkleMessage,
    ObserveConnectivityRequest, ObserveConnectivityResponse, PeerInput, SendMessageRequest,
    SendMessageResponse, Update,
};

// -- Mock gateway ---------------------------------------------------------

/// Shared mock state across connections of one mock gateway.
#[derive(Debug, Default)]
struct MockLogInner {
    /// Every follow request seen, in order (including rejected ones).
    follows: Mutex<Vec<(String, u64)>>,
    /// Room streams rejected even from the origin (degraded-path tests).
    fail_rooms: Mutex<HashSet<String>>,
}

type MockLog = Arc<MockLogInner>;

fn rpc_error(id: u64, code: i32, message: &str) -> Frame {
    Frame {
        version: 1,
        request_id: id,
        body: Some(Body::Result(RpcResult {
            outcome: Some(Outcome::Error(ProtoRpcError {
                code,
                message: message.into(),
            })),
        })),
    }
}

async fn mock_gateway(listener: TcpListener, log: MockLog) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => return,
        };
        let task_log = log.clone();
        tokio::spawn(async move {
            use tokio_tungstenite::tungstenite::handshake::server::ErrorResponse;
            use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
            #[allow(clippy::result_large_err)]
            let callback = |req: &Request, mut resp: Response| -> Result<Response, ErrorResponse> {
                let offered = req
                    .headers()
                    .get("sec-websocket-protocol")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                if offered
                    .split(',')
                    .map(|s| s.trim())
                    .any(|s| s == "syntra.v1")
                {
                    resp.headers_mut()
                        .insert("sec-websocket-protocol", "syntra.v1".parse().unwrap());
                }
                Ok(resp)
            };
            let ws = match tokio_tungstenite::accept_hdr_async(stream, callback).await {
                Ok(w) => w,
                Err(_) => return,
            };
            let log = task_log.clone();
            // Server rule mirrored here: at most one follow per stream per
            // connection; room continuation with a nonzero cursor requires a
            // scope proof the client cannot mint.
            let mut followed: HashSet<String> = HashSet::new();
            let (mut write, mut read) = ws.split();
            let hello_msg = match read.next().await {
                Some(Ok(m)) => m,
                _ => return,
            };
            let bytes = hello_msg.into_data();
            let frame = match decode_frame(
                &bytes,
                ValidationContext::pre_handshake(ConnectionState::Connected),
            ) {
                Ok(f) => f,
                Err(_) => return,
            };
            let hello_ok = matches!(frame.body, Some(Body::Hello(_)));
            if !hello_ok {
                return;
            }
            let ready = Frame {
                version: 1,
                request_id: 0,
                body: Some(Body::Ready(syntra_core::proto::ServerReady {
                    connection_id: format!("mock-{}", rand::random::<u32>()),
                })),
            };
            let buf = encode_frame(
                &ready,
                ValidationContext::pre_handshake(ConnectionState::Authenticating),
            )
            .unwrap();
            if write
                .send(tokio_tungstenite::tungstenite::Message::Binary(buf.into()))
                .await
                .is_err()
            {
                return;
            }
            while let Some(msg) = read.next().await {
                let msg = match msg {
                    Ok(m) => m,
                    Err(_) => break,
                };
                if msg.is_close() {
                    break;
                }
                if msg.is_ping() || msg.is_pong() || msg.is_text() {
                    continue;
                }
                let data = msg.into_data();
                if data.is_empty() {
                    continue;
                }
                let frame = match decode_frame(&data, ValidationContext::active(1)) {
                    Ok(f) => f,
                    Err(_) => continue,
                };
                let (id, req) = match frame.body {
                    Some(Body::Request(r)) => (frame.request_id, r),
                    Some(Body::Cancel(_)) => continue,
                    _ => continue,
                };
                let resp_frame = handle_mock_request(id, req, &mut followed, &log).await;
                if resp_frame_needs_notif(&resp_frame) {
                    let obs = ObserveConnectivityResponse {
                        update: Some(ConnectivityChanged {
                            user_id: 42,
                            online: true,
                            known: true,
                            revision: 1,
                        }),
                    };
                    let mut pbuf = Vec::new();
                    obs.encode(&mut pbuf).unwrap();
                    let nf = Frame {
                        version: 1,
                        request_id: 0,
                        body: Some(Body::Notification(Notification { payload: pbuf })),
                    };
                    let nbuf = encode_frame(&nf, ValidationContext::active(1)).unwrap();
                    let _ = write
                        .send(tokio_tungstenite::tungstenite::Message::Binary(nbuf.into()))
                        .await;
                }
                if is_slow(&resp_frame) {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                let out = encode_frame(&resp_frame, ValidationContext::active(1)).unwrap();
                if write
                    .send(tokio_tungstenite::tungstenite::Message::Binary(out.into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
    }
}

fn is_slow(frame: &Frame) -> bool {
    if let Some(Body::Result(r)) = &frame.body {
        if let Some(Outcome::Payload(b)) = &r.outcome {
            return b.as_slice() == b"SLOW".as_slice();
        }
    }
    false
}

fn resp_frame_needs_notif(frame: &Frame) -> bool {
    if let Some(Body::Result(r)) = &frame.body {
        if let Some(Outcome::Payload(b)) = &r.outcome {
            return b.as_slice() == b"WITH_NOTIF".as_slice();
        }
    }
    false
}

async fn handle_mock_request(
    id: u64,
    req: RpcRequest,
    followed: &mut HashSet<String>,
    log: &MockLog,
) -> Frame {
    let result = match req.method.as_str() {
        "/example.Echo/Echo" => RpcResult {
            outcome: Some(Outcome::Payload(req.payload)),
        },
        "/example.Echo/Slow" => RpcResult {
            outcome: Some(Outcome::Payload(b"SLOW".to_vec())),
        },
        "/example.Echo/WithNotif" => RpcResult {
            outcome: Some(Outcome::Payload(b"WITH_NOTIF".to_vec())),
        },
        "/tinkle.v1.MessagesService/SendMessage" => {
            let r = SendMessageRequest::decode(&*req.payload).unwrap_or(SendMessageRequest {
                peer: None,
                message: String::new(),
                random_id: 0,
                reply_to: None,
                silent: None,
                no_forward: None,
                clear_draft: None,
                entities: vec![],
                schedule_date: None,
            });
            let msg = TinkleMessage {
                message_id: 100,
                peer: r.peer,
                from_id: 42,
                date: None,
                message: r.message,
                media: None,
                entities: vec![],
                reply_to: None,
                edit_date: None,
                reactions: None,
                replies: None,
                pinned: false,
                ttl_period: 0,
            };
            let resp = SendMessageResponse {
                message: Some(msg),
                users: vec![],
                chats: vec![],
            };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            RpcResult {
                outcome: Some(Outcome::Payload(b)),
            }
        }
        "/tinkle.v1.UpdatesService/FollowUpdates" => {
            let r = FollowUpdatesRequest::decode(&*req.payload).unwrap_or(FollowUpdatesRequest {
                stream: String::new(),
                after_cursor: 0,
            });
            let stream = if r.stream.is_empty() {
                "user:42".to_string()
            } else {
                r.stream
            };
            log.follows
                .lock()
                .unwrap()
                .push((stream.clone(), r.after_cursor));
            if followed.contains(&stream) {
                return rpc_error(id, 6, "already following");
            }
            if stream.starts_with("room:") && r.after_cursor != 0 {
                return rpc_error(id, 9, "room continuation requires scoped history proof");
            }
            if stream.starts_with("room:") && log.fail_rooms.lock().unwrap().contains(&stream) {
                return rpc_error(id, 9, "room continuation requires scoped history proof");
            }
            followed.insert(stream.clone());
            let resp = FollowUpdatesResponse {
                stream,
                head: Some(r.after_cursor),
                result: None,
            };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            RpcResult {
                outcome: Some(Outcome::Payload(b)),
            }
        }
        "/tinkle.v1.UpdatesService/ObserveConnectivity" => {
            let _ = ObserveConnectivityRequest::decode(&*req.payload);
            let resp = ObserveConnectivityResponse {
                update: Some(ConnectivityChanged {
                    user_id: 42,
                    online: true,
                    known: true,
                    revision: 1,
                }),
            };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            RpcResult {
                outcome: Some(Outcome::Payload(b)),
            }
        }
        "/tinkle.v1.MessagesService/GetHistory" | "/tinkle.v1.MessagesService/GetDialogs" => {
            let b = if req.method.contains("GetHistory") {
                let r = tinkle_schema::proto::GetHistoryResponse {
                    messages: vec![],
                    users: vec![],
                    chats: vec![],
                    count: 0,
                    offset_id_offset: None,
                };
                let mut buf = Vec::new();
                r.encode(&mut buf).unwrap();
                buf
            } else {
                let r = tinkle_schema::proto::GetDialogsResponse {
                    dialogs: vec![],
                    messages: vec![],
                    users: vec![],
                    chats: vec![],
                    next_cursor: None,
                };
                let mut buf = Vec::new();
                r.encode(&mut buf).unwrap();
                buf
            };
            RpcResult {
                outcome: Some(Outcome::Payload(b)),
            }
        }
        _ => RpcResult {
            outcome: Some(Outcome::Error(ProtoRpcError {
                code: 12,
                message: "unknown method".into(),
            })),
        },
    };
    Frame {
        version: 1,
        request_id: id,
        body: Some(Body::Result(result)),
    }
}

async fn start_mock_logged() -> (SocketAddr, MockLog) {
    let log: MockLog = Arc::new(MockLogInner::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(mock_gateway(listener, log.clone()));
    (addr, log)
}

async fn start_mock() -> SocketAddr {
    start_mock_logged().await.0
}

fn test_client(addr: SocketAddr) -> libtinkle::Client {
    let cfg = libtinkle::ClientConfig::new(format!("ws://{addr}/syntra/v1/ws")).insecure_dev();
    libtinkle::Client::new(
        cfg,
        libtinkle::Credential::from_session_token("test-token"),
        libtinkle::InMemoryCursorStore::new(),
    )
}

fn test_client_with_store(
    addr: SocketAddr,
    store: Arc<libtinkle::InMemoryCursorStore>,
) -> libtinkle::Client {
    let cfg = libtinkle::ClientConfig::new(format!("ws://{addr}/syntra/v1/ws")).insecure_dev();
    libtinkle::Client::new(
        cfg,
        libtinkle::Credential::from_session_token("test-token"),
        store,
    )
}

// -- Tests -----------------------------------------------------------------

#[tokio::test]
async fn handshake_and_send_message() {
    let addr = start_mock().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    let req = SendMessageRequest {
        peer: Some(PeerInput {
            target: Some(PeerTarget::UserId(7)),
        }),
        message: "hello tinkle".into(),
        random_id: 12345,
        reply_to: None,
        silent: None,
        no_forward: None,
        clear_draft: None,
        entities: vec![],
        schedule_date: None,
    };
    let resp = c.messages().send_message(req).await.unwrap();
    assert_eq!(resp.message.unwrap().message, "hello tinkle");
    c.close().await;
}

#[tokio::test]
async fn concurrent_out_of_order() {
    let addr = start_mock().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    let c_slow = c.clone();
    let h_slow = tokio::spawn(async move {
        c_slow
            .call_raw("/example.Echo/Slow", b"slow".to_vec(), 5000)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let c_fast = c.clone();
    let h_fast = tokio::spawn(async move {
        c_fast
            .call_raw("/example.Echo/Echo", b"fast".to_vec(), 5000)
            .await
    });
    let fast = h_fast.await.unwrap().unwrap();
    let slow = h_slow.await.unwrap().unwrap();
    assert_eq!(fast, b"fast");
    assert_eq!(slow, b"SLOW");
    c.close().await;
}

#[tokio::test]
async fn cancellation_reports_cancelled() {
    let addr = start_mock().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    let token = syntra_runtime::CancelToken::new();
    let t2 = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        t2.cancel();
    });
    let err = c
        .call_raw_cancellable("/example.Echo/Slow", b"x".to_vec(), 5000, &token)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        libtinkle::TinkleError::Syntra(syntra_core::error::SyntraError::Cancelled(_))
    ));
    c.close().await;
}

#[tokio::test]
async fn notification_interleaving_does_not_complete_rpc() {
    let addr = start_mock().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    let out = c
        .call_raw("/example.Echo/WithNotif", b"ping".to_vec(), 5000)
        .await
        .unwrap();
    assert_eq!(out, b"WITH_NOTIF");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let ev = c.next_event().await;
    let _ = ev;
    c.close().await;
}

#[tokio::test]
async fn unknown_method_maps_to_unimplemented() {
    let addr = start_mock().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    let err = c
        .call_raw("/tinkle.v1.Fake/Nope", vec![], 2000)
        .await
        .unwrap_err();
    assert_eq!(err.code(), syntra_core::error::ErrorCode::Unimplemented);
    c.close().await;
}

fn omitted_update(stream: &str, cursor: u64) -> Update {
    Update {
        event_id: String::new(),
        stream: stream.into(),
        cursor,
        occurred_at: None,
        source_seq: None,
        source_revision: None,
        payload: Some(UpdatePayload::ContentOmitted(ContentOmitted {})),
    }
}

#[tokio::test]
async fn subscribe_and_direct_validation() {
    let addr = start_mock().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    let resp = c.subscribe(vec![], vec![]).await.unwrap();
    assert_eq!(resp.stream, "user:42");
    // Direct validation only (no pipeline delivery here).
    // ContentOmitted carries no source identity by design.
    let update = omitted_update("user:42", 1);
    let v = tinkle_schema::sync::validate_update(&update).unwrap();
    assert_eq!(v.cursor, 1);
    c.close().await;
    c.reconnect().await.unwrap();
    assert!(c.is_connected().await);
    c.close().await;
}

#[tokio::test]
async fn sync_pipeline_buffers_consecutive_updates_before_ack() {
    use std::sync::Arc;
    let store = libtinkle::InMemoryCursorStore::new();
    let mgr =
        libtinkle::SyncManager::new("user:42", store.clone() as Arc<dyn libtinkle::CursorStore>);
    for cursor in 1..=3u64 {
        mgr.handle_update(omitted_update("user:42", cursor))
            .await
            .unwrap();
    }
    for expected in 1..=3u64 {
        let evt = mgr.next_event().await.unwrap();
        assert!(
            matches!(
                evt,
                libtinkle::SyncEvent::UpdateReady { cursor, .. } if cursor == expected
            ),
            "expected UpdateReady {expected}, got {evt:?}"
        );
    }
    assert_eq!(
        store.load("user:42", "user:42").await.unwrap(),
        None,
        "receipt alone must not persist"
    );
    for cursor in 1..=3u64 {
        mgr.acknowledge("user:42", cursor).await.unwrap();
        let evt = mgr.next_event().await.unwrap();
        assert!(matches!(
            evt,
            libtinkle::SyncEvent::Applied { cursor: c, .. } if c == cursor
        ));
    }
    assert_eq!(store.load("user:42", "user:42").await.unwrap(), Some(3));
}

#[tokio::test]
async fn acknowledge_without_connection_fails_closed() {
    let store = libtinkle::InMemoryCursorStore::new();
    let cfg = libtinkle::ClientConfig::new("ws://127.0.0.1:1/syntra/v1/ws").insecure_dev();
    let c = libtinkle::Client::new(
        cfg,
        libtinkle::Credential::from_session_token("t"),
        store.clone() as Arc<dyn libtinkle::CursorStore>,
    );
    // Without a connection, acknowledge fails closed (no silent advance).
    assert!(matches!(
        c.acknowledge("user:42", 1).await,
        Err(libtinkle::TinkleError::NotConnected(_))
    ));
}

// -- WS1: single-follow subscribe --------------------------------------------

fn own_follows(log: &MockLog) -> Vec<(String, u64)> {
    log.follows
        .lock()
        .unwrap()
        .iter()
        .filter(|(s, _)| s == "user:42")
        .cloned()
        .collect()
}

#[tokio::test]
async fn cold_subscribe_issues_single_own_follow() {
    let (addr, log) = start_mock_logged().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    // No set_principal_user_id: cold session resolves via probe-as-follow.
    let resp = c.subscribe(vec![], vec![]).await.unwrap();
    assert_eq!(resp.stream, "user:42");
    assert_eq!(c.principal().await, "user:42");
    let follows = own_follows(&log);
    assert_eq!(
        follows.len(),
        1,
        "exactly one own-stream follow per connection, got {follows:?}"
    );
    assert_eq!(follows[0], ("user:42".to_string(), 0));
    c.close().await;
}

#[tokio::test]
async fn resubscribe_on_same_connection_does_not_duplicate_follows() {
    let (addr, log) = start_mock_logged().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    c.subscribe(vec![], vec![]).await.unwrap();
    c.subscribe(vec![], vec![]).await.unwrap();
    assert_eq!(own_follows(&log).len(), 1);
    c.close().await;
}

#[tokio::test]
async fn room_scope_rejection_falls_back_to_origin_without_touching_checkpoint() {
    let (addr, log) = start_mock_logged().await;
    let store = libtinkle::InMemoryCursorStore::new();
    store.save("user:42", "room:7", 30).await.unwrap();
    let c = test_client_with_store(addr, store.clone());
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    let resp = c.subscribe(vec![(7, 50)], vec![]).await.unwrap();
    assert_eq!(resp.stream, "user:42");
    let room: Vec<(String, u64)> = log
        .follows
        .lock()
        .unwrap()
        .iter()
        .filter(|(s, _)| s == "room:7")
        .cloned()
        .collect();
    assert_eq!(
        room,
        vec![("room:7".to_string(), 50), ("room:7".to_string(), 0),],
        "rejected resume attempt, then origin retry"
    );
    // The fallback must not erase the stored checkpoint.
    assert_eq!(store.load("user:42", "room:7").await.unwrap(), Some(30));
    c.close().await;
}

#[tokio::test]
async fn persistently_rejected_room_degrades_without_losing_own_follow() {
    let (addr, log) = start_mock_logged().await;
    log.fail_rooms.lock().unwrap().insert("room:9".to_string());
    let c = test_client(addr);
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    let err = c.subscribe(vec![(9, 10)], vec![]).await.unwrap_err();
    assert!(
        err.to_string().contains('9'),
        "degraded room must be named, got {err}"
    );
    assert_eq!(own_follows(&log).len(), 1);
    // The scope clears: retry reconciles without duplicating the own follow.
    log.fail_rooms.lock().unwrap().clear();
    let resp = c.subscribe(vec![(9, 10)], vec![]).await.unwrap();
    assert_eq!(resp.stream, "user:42");
    assert_eq!(own_follows(&log).len(), 1);
    let room: Vec<(String, u64)> = log
        .follows
        .lock()
        .unwrap()
        .iter()
        .filter(|(s, _)| s == "room:9")
        .cloned()
        .collect();
    assert_eq!(
        room,
        vec![
            ("room:9".to_string(), 10),
            ("room:9".to_string(), 0),
            // Retry: nonzero resume rejected again, origin fallback accepted.
            ("room:9".to_string(), 10),
            ("room:9".to_string(), 0),
        ]
    );
    c.close().await;
}
