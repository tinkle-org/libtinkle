//! Live-gateway integration: handshake, concurrent RPCs, cancellation,
//! notification interleaving, reconnect, sync recovery. Spins an in-process
//! Syntra WSS-equivalent mock gateway.

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use libtinkle::CursorStore;
use syntra_core::proto::{
    frame::Body, rpc_result::Outcome, Frame, Notification, RpcError as ProtoRpcError, RpcRequest,
    RpcResult,
};
use syntra_core::{decode_frame, encode_frame, ConnectionState, ValidationContext};
use tinkle_schema::proto::follow_updates_response::Result as FollowResult;
use tinkle_schema::proto::{
    peer_input::Target as PeerTarget, update::Payload as UpdatePayload, ConnectivityChanged,
    ContentOmitted, FollowUpdatesRequest, FollowUpdatesResponse, Message as TinkleMessage,
    ObserveConnectivityRequest, ObserveConnectivityResponse, PeerInput, ReadUpdatesRequest,
    ReadUpdatesResponse, SendMessageRequest, SendMessageResponse, Update,
};

// -- Mock gateway ---------------------------------------------------------

/// Shared mock state across connections of one mock gateway.
#[derive(Debug, Default)]
struct MockLogInner {
    /// Every follow request seen, in order (including rejected ones).
    follows: Mutex<Vec<(String, u64)>>,
    /// Room streams rejected even from the origin (degraded-path tests).
    fail_rooms: Mutex<HashSet<String>>,
    /// `ReadUpdates` pages served, in order (recovery-path tests).
    reads: Mutex<Vec<(String, u64, i32)>>,
    /// Kill switches for live connections (pump-death tests).
    kill: Mutex<Vec<oneshot::Sender<()>>>,
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
            let (kill_tx, mut kill_rx) = oneshot::channel();
            log.kill.lock().unwrap().push(kill_tx);
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
            loop {
                let msg = tokio::select! {
                    msg = read.next() => match msg {
                        Some(Ok(m)) => m,
                        _ => break,
                    },
                    _ = &mut kill_rx => break,
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
                let wants_gap_notif = is_room7_follow(&req);
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
                let room7_follow = wants_gap_notif && is_ok_result(&resp_frame);
                let out = encode_frame(&resp_frame, ValidationContext::active(1)).unwrap();
                if write
                    .send(tokio_tungstenite::tungstenite::Message::Binary(out.into()))
                    .await
                    .is_err()
                {
                    break;
                }
                if room7_follow {
                    let nbuf = room7_gap_notification();
                    let _ = write
                        .send(tokio_tungstenite::tungstenite::Message::Binary(nbuf.into()))
                        .await;
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
        "/tinkle.v1.UpdatesService/ReadUpdates" => {
            let r = ReadUpdatesRequest::decode(&*req.payload).unwrap_or(ReadUpdatesRequest {
                stream: String::new(),
                after_cursor: 0,
                limit: 0,
            });
            log.reads
                .lock()
                .unwrap()
                .push((r.stream.clone(), r.after_cursor, r.limit));
            // Canned retained history: room:7 holds cursors 1..=5.
            let (updates, head) = if r.stream == "room:7" {
                let take = (r.limit.max(0) as u64).min(100);
                let mut updates = Vec::new();
                let mut c = r.after_cursor.saturating_add(1);
                while c <= 5 && (updates.len() as u64) < take {
                    updates.push(Update {
                        event_id: String::new(),
                        stream: "room:7".into(),
                        cursor: c,
                        occurred_at: None,
                        source_seq: None,
                        source_revision: None,
                        payload: Some(UpdatePayload::ContentOmitted(ContentOmitted {})),
                    });
                    c = c.saturating_add(1);
                }
                (updates, 5)
            } else {
                (Vec::new(), r.after_cursor)
            };
            let resp = ReadUpdatesResponse {
                updates,
                replay_floor: 0,
                head,
            };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
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

/// True when the request is a `FollowUpdates` for `room:7`.
fn is_room7_follow(req: &RpcRequest) -> bool {
    if req.method != "/tinkle.v1.UpdatesService/FollowUpdates" {
        return false;
    }
    FollowUpdatesRequest::decode(&*req.payload)
        .map(|r| r.stream == "room:7")
        .unwrap_or(false)
}

/// True when the frame is a successful RPC result (not an error).
fn is_ok_result(frame: &Frame) -> bool {
    matches!(
        &frame.body,
        Some(Body::Result(r)) if matches!(&r.outcome, Some(Outcome::Payload(_)))
    )
}

/// Live gap notification: cursor 5 on `room:7` with nothing applied.
/// Exercises supervisor gap detection + Syntra recovery end to end.
fn room7_gap_notification() -> Vec<u8> {
    let update = Update {
        event_id: String::new(),
        stream: "room:7".into(),
        cursor: 5,
        occurred_at: None,
        source_seq: None,
        source_revision: None,
        payload: Some(UpdatePayload::ContentOmitted(ContentOmitted {})),
    };
    let resp = FollowUpdatesResponse {
        stream: "room:7".into(),
        head: Some(5),
        result: Some(FollowResult::Update(update)),
    };
    let mut pbuf = Vec::new();
    resp.encode(&mut pbuf).unwrap();
    let nf = Frame {
        version: 1,
        request_id: 0,
        body: Some(Body::Notification(Notification { payload: pbuf })),
    };
    encode_frame(&nf, ValidationContext::active(1)).unwrap()
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

// -- WS2: supervised lifecycle -------------------------------------------------

/// Kill the most recent mock connection (simulates transport death).
fn kill_one(log: &MockLog) {
    let tx = log.kill.lock().unwrap().pop();
    if let Some(tx) = tx {
        let _ = tx.send(());
    }
}

async fn wait_for<F>(mut cond: F, what: &str)
where
    F: FnMut() -> bool,
{
    for _ in 0..100 {
        if cond() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn supervisor_reconnects_after_pump_death() {
    let (addr, log) = start_mock_logged().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    let res = c.start(vec![], vec![]).await.unwrap();
    assert_eq!(res.stream, "user:42");
    assert!(c.is_connected().await);
    assert_eq!(own_follows(&log).len(), 1);

    // Transport dies; the supervisor must re-establish without host help.
    kill_one(&log);
    wait_for(
        || own_follows(&log).len() >= 2,
        "supervisor to re-establish the follow",
    )
    .await;
    assert!(c.is_connected().await);
    assert_eq!(own_follows(&log).len(), 2);
    // Exactly one own-stream follow per connection, no duplicates.
    assert_eq!(own_follows(&log), vec![("user:42".to_string(), 0); 2]);
    // A second start() shares the supervisor instead of duplicating it.
    c.start(vec![], vec![]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(own_follows(&log).len(), 2);
    c.stop().await;
}

#[tokio::test]
async fn stop_prevents_resurrection() {
    let (addr, log) = start_mock_logged().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    c.start(vec![], vec![]).await.unwrap();
    c.stop().await;
    assert!(!c.is_connected().await);
    kill_one(&log);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    // No supervisor, no resurrection.
    assert_eq!(own_follows(&log).len(), 1);
    assert!(!c.is_connected().await);
}

#[tokio::test]
async fn current_cursor_reflects_session() {
    let (addr, _log) = start_mock_logged().await;
    let c = test_client(addr);
    assert_eq!(c.current_cursor("user:42").await, None);
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    c.start(vec![], vec![]).await.unwrap();
    assert_eq!(c.current_cursor("user:42").await, Some(0));
    c.stop().await;
    assert_eq!(c.current_cursor("user:42").await, None);
}

// -- WS6: Syntra recovery transport -------------------------------------------

#[tokio::test]
async fn supervisor_recovers_gap_over_syntra_by_default() {
    let (addr, log) = start_mock_logged().await;
    // No grpc_endpoint: recovery must use the Syntra default transport.
    let c = test_client(addr);
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    c.start(vec![(7, 0)], vec![]).await.unwrap();

    // The mock pushes live cursor 5 on room:7 with nothing applied: Gap.
    // The supervisor recovers via Syntra ReadUpdates; the host only
    // consumes events and acknowledges.
    let mut seen_gap = false;
    let mut applied = 0u64;
    for _ in 0..60 {
        let evt = tokio::time::timeout(std::time::Duration::from_secs(10), c.next_event()).await;
        match evt {
            Ok(Some(libtinkle::SyncEvent::Gap { stream, .. })) if stream == "room:7" => {
                seen_gap = true;
            }
            Ok(Some(libtinkle::SyncEvent::UpdateReady { stream, cursor, .. }))
                if stream == "room:7" =>
            {
                c.acknowledge(&stream, cursor).await.unwrap();
                applied = applied.max(cursor);
                if applied >= 5 {
                    break;
                }
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    assert!(seen_gap, "gap must surface before recovery");
    assert_eq!(applied, 5, "retained suffix must replay through recovery");
    // Recovery went through Syntra RPC pages from the durable cursor.
    let reads = log.reads.lock().unwrap().clone();
    assert!(!reads.is_empty(), "expected ReadUpdates pages, got none");
    assert!(reads.iter().all(|(s, _, _)| s == "room:7"));
    assert_eq!(reads[0].1, 0, "first page resumes from the durable cursor");
    c.stop().await;
}

#[tokio::test]
async fn updates_read_updates_uses_syntra_rpc() {
    let (addr, log) = start_mock_logged().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    let resp = c
        .updates()
        .read_updates(tinkle_schema::proto::ReadUpdatesRequest {
            stream: "room:7".into(),
            after_cursor: 2,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(resp.head, 5);
    assert_eq!(resp.replay_floor, 0);
    let cursors: Vec<u64> = resp.updates.iter().map(|u| u.cursor).collect();
    assert_eq!(cursors, vec![3, 4, 5]);
    assert_eq!(
        *log.reads.lock().unwrap(),
        vec![("room:7".to_string(), 2, 10)]
    );
    c.close().await;
}
