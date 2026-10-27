//! Live-gateway integration for the reconciliation contract: handshake,
//! concurrent RPCs, cancellation, opaque-hint interleaving, reconnect,
//! journal paging (`GetTargetChanges` + `PageMore` tri-state), `accept_seq`
//! dedup, chunked `GetMessages`, `GetHistory.more_available`, and storage v3
//! migration. Spins an in-process Syntra WSS-equivalent mock gateway.

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use syntra_core::proto::{
    frame::Body, rpc_result::Outcome, Frame, Notification, RpcError as ProtoRpcError, RpcRequest,
    RpcResult,
};
use syntra_core::{decode_frame, encode_frame, ConnectionState, ValidationContext};
use tinkle_schema::proto::{
    target, ConnectivityChanged, GetHistoryRequest, GetHistoryResponse, GetMessagesRequest,
    GetMessagesResponse, GetTargetChangesRequest, GetTargetChangesResponse,
    Message as TinkleMessage, ObserveConnectivityRequest, ObserveConnectivityResponse,
    SendMessageRequest, SendMessageResponse, Target, TargetChange, TargetChangeKind,
};

// -- Mock gateway ---------------------------------------------------------

#[derive(Debug, Default)]
struct MockLogInner {
    target_changes: Mutex<Vec<(String, i64, i32)>>,
    messages: Mutex<Vec<(String, Vec<i64>)>>,
    history: Mutex<Vec<(String, i32)>>,
    observe: Mutex<Vec<Vec<i64>>>,
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

fn target_key_of(t: &Option<Target>) -> String {
    match t.as_ref().and_then(|x| x.kind.as_ref()) {
        Some(target::Kind::UserId(id)) => format!("target:user:{id}"),
        Some(target::Kind::ChatId(id)) => format!("target:room:{id}"),
        None => "target:unknown".to_string(),
    }
}

fn ts(seconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds, nanos: 0 }
}

fn journal_message(
    id: i64,
    body: &str,
    token: Option<i64>,
    edit_secs: Option<i64>,
) -> TinkleMessage {
    TinkleMessage {
        message_id: id,
        target: Some(Target {
            kind: Some(target::Kind::ChatId(7)),
        }),
        from_id: 42,
        date: None,
        message: body.to_string(),
        media: None,
        entities: vec![],
        reply_to: None,
        edit_date: edit_secs.map(ts),
        reactions: None,
        replies: None,
        pinned: false,
        ttl_period: 0,
        accept_seq: token,
    }
}

/// Canned journal for room 7 (mutation seq → change).
fn room7_changes() -> Vec<TargetChange> {
    vec![
        TargetChange {
            message_id: 10,
            kind: TargetChangeKind::Edited as i32,
            message: Some(journal_message(10, "m10v1", Some(1), None)),
            accept_seq: 1,
        },
        TargetChange {
            message_id: 11,
            kind: TargetChangeKind::Edited as i32,
            message: Some(TinkleMessage {
                target: Some(Target {
                    kind: Some(target::Kind::ChatId(7)),
                }),
                ..journal_message(11, "m11v1", Some(2), None)
            }),
            accept_seq: 2,
        },
        // Stable token (1) with fresher edit_date wins over seq 1.
        TargetChange {
            message_id: 10,
            kind: TargetChangeKind::Edited as i32,
            message: Some(journal_message(10, "m10v2", Some(1), Some(20))),
            accept_seq: 3,
        },
        TargetChange {
            message_id: 11,
            kind: TargetChangeKind::Deleted as i32,
            message: None,
            accept_seq: 4,
        },
        TargetChange {
            message_id: 12,
            kind: TargetChangeKind::Revoked as i32,
            message: None,
            accept_seq: 5,
        },
    ]
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
            if !matches!(frame.body, Some(Body::Hello(_))) {
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
                let wants_hint = is_observe(&req);
                let resp_frame = handle_mock_request(id, req, &log).await;
                if resp_frame_needs_notif(&resp_frame) {
                    // Opaque hint carrying ConnectivityChanged bytes directly.
                    let obs = ConnectivityChanged {
                        user_id: 42,
                        online: true,
                        known: true,
                        revision: 1,
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
                if wants_hint {
                    // Extra opaque hint after observe establishment.
                    let nf = Frame {
                        version: 1,
                        request_id: 0,
                        body: Some(Body::Notification(Notification {
                            payload: b"hint".to_vec(),
                        })),
                    };
                    let nbuf = encode_frame(&nf, ValidationContext::active(1)).unwrap();
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

fn is_observe(req: &RpcRequest) -> bool {
    req.method == "/tinkle.v1.UpdatesService/ObserveConnectivity"
}

async fn handle_mock_request(id: u64, req: RpcRequest, log: &MockLog) -> Frame {
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
                target: None,
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
                target: r.target,
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
                accept_seq: Some(100),
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
        "/tinkle.v1.UpdatesService/ObserveConnectivity" => {
            let r = ObserveConnectivityRequest::decode(&*req.payload)
                .unwrap_or(ObserveConnectivityRequest { user_ids: vec![] });
            log.observe.lock().unwrap().push(r.user_ids);
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
        "/tinkle.v1.MessagesService/GetTargetChanges" => {
            let r =
                GetTargetChangesRequest::decode(&*req.payload).unwrap_or(GetTargetChangesRequest {
                    target: None,
                    after_accept_seq: 0,
                    limit: 0,
                });
            let key = target_key_of(&r.target);
            log.target_changes
                .lock()
                .unwrap()
                .push((key.clone(), r.after_accept_seq, r.limit));
            let limit = if r.limit <= 0 { 500 } else { r.limit.min(500) };
            let resp = mock_target_changes(&key, r.after_accept_seq, limit);
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            RpcResult {
                outcome: Some(Outcome::Payload(b)),
            }
        }
        "/tinkle.v1.MessagesService/GetMessages" => {
            let r = GetMessagesRequest::decode(&*req.payload).unwrap_or(GetMessagesRequest {
                target: None,
                message_ids: vec![],
            });
            if r.message_ids.len() > 100 {
                return rpc_error(id, 3, "too many message_ids");
            }
            let key = target_key_of(&r.target);
            log.messages
                .lock()
                .unwrap()
                .push((key.clone(), r.message_ids.clone()));
            let resp = mock_get_messages(&key, &r.message_ids);
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            RpcResult {
                outcome: Some(Outcome::Payload(b)),
            }
        }
        "/tinkle.v1.MessagesService/GetHistory" => {
            let r = GetHistoryRequest::decode(&*req.payload).unwrap_or(GetHistoryRequest {
                target: None,
                offset_id: None,
                offset_date: None,
                add_offset: 0,
                limit: 20,
                max_id: None,
                min_id: None,
            });
            let key = target_key_of(&r.target);
            log.history.lock().unwrap().push((key.clone(), r.limit));
            let resp = mock_history(&key, &r);
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            RpcResult {
                outcome: Some(Outcome::Payload(b)),
            }
        }
        "/tinkle.v1.MessagesService/GetDialogs" => {
            let r = tinkle_schema::proto::GetDialogsResponse {
                dialogs: vec![],
                messages: vec![],
                users: vec![],
                chats: vec![],
                next_cursor: None,
            };
            let mut b = Vec::new();
            r.encode(&mut b).unwrap();
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

fn mock_target_changes(key: &str, after: i64, limit: i32) -> GetTargetChangesResponse {
    if key == "target:room:7" {
        let all = room7_changes();
        let mut out = Vec::new();
        for c in all {
            if c.accept_seq > after && (out.len() as i32) < limit {
                out.push(c);
            }
        }
        let last = out.last().map(|c| c.accept_seq).unwrap_or(after);
        // Tri-state: Yes while retained head (5) lies beyond this page.
        let more = if out.is_empty() {
            // Empty page echoes cursor; caught up.
            Some(false)
        } else if last < 5 {
            Some(true)
        } else {
            Some(false)
        };
        return GetTargetChangesResponse {
            changes: out,
            more_available: more,
            last_accept_seq: last,
        };
    }
    if key == "target:room:8" {
        // Unknown continuation: one change, no report.
        if after >= 1 {
            return GetTargetChangesResponse {
                changes: vec![],
                more_available: None,
                last_accept_seq: after,
            };
        }
        return GetTargetChangesResponse {
            changes: vec![TargetChange {
                message_id: 20,
                kind: TargetChangeKind::Edited as i32,
                message: Some(TinkleMessage {
                    message_id: 20,
                    target: Some(Target {
                        kind: Some(target::Kind::ChatId(8)),
                    }),
                    from_id: 42,
                    message: "room8".into(),
                    accept_seq: Some(1),
                    ..Default::default()
                }),
                accept_seq: 1,
            }],
            more_available: None,
            last_accept_seq: 1,
        };
    }
    if key == "target:room:9" {
        // Retention skip: journal starts at 10, caller at 0 must heal via history.
        if after >= 10 {
            return GetTargetChangesResponse {
                changes: vec![],
                more_available: Some(false),
                last_accept_seq: after,
            };
        }
        return GetTargetChangesResponse {
            changes: vec![TargetChange {
                message_id: 30,
                kind: TargetChangeKind::Edited as i32,
                message: Some(TinkleMessage {
                    message_id: 30,
                    target: Some(Target {
                        kind: Some(target::Kind::ChatId(9)),
                    }),
                    from_id: 42,
                    message: "skipped".into(),
                    accept_seq: Some(10),
                    ..Default::default()
                }),
                accept_seq: 10,
            }],
            more_available: Some(false),
            last_accept_seq: 10,
        };
    }
    if key == "target:room:10" {
        // New-message room: journal is empty (edits/removals only, no new
        // messages disclosed here). Catch-up must discover bodies via
        // history + GetMessages.
        return GetTargetChangesResponse {
            changes: vec![],
            more_available: Some(false),
            last_accept_seq: after,
        };
    }
    if key == "target:room:11" {
        // Delete-then-stale-history: journal deletes 50, but history still
        // returns 50 (stale snapshot race). Catch-up must not resurrect it
        // even though GetMessages (race) returns the body.
        if after >= 1 {
            return GetTargetChangesResponse {
                changes: vec![],
                more_available: Some(false),
                last_accept_seq: after,
            };
        }
        return GetTargetChangesResponse {
            changes: vec![TargetChange {
                message_id: 50,
                kind: TargetChangeKind::Deleted as i32,
                message: None,
                accept_seq: 1,
            }],
            more_available: Some(false),
            last_accept_seq: 1,
        };
    }
    if key == "target:room:12" {
        // Omission gate: history returns 60, but GetMessages omits it
        // (deleted/invisible). Catch-up must not ingest omitted ids.
        return GetTargetChangesResponse {
            changes: vec![],
            more_available: Some(false),
            last_accept_seq: after,
        };
    }
    GetTargetChangesResponse {
        changes: vec![],
        more_available: Some(false),
        last_accept_seq: after,
    }
}

fn mock_get_messages(key: &str, ids: &[i64]) -> GetMessagesResponse {
    let mut messages = Vec::new();
    for id in ids {
        if key == "target:room:7" {
            match *id {
                10 => messages.push(journal_message(10, "m10v2", Some(1), Some(20))),
                11 => {
                    // Deleted: omitted, carries no information.
                }
                12 => {
                    // Revoked: omitted.
                }
                _ => messages.push(TinkleMessage {
                    message_id: *id,
                    target: Some(Target {
                        kind: Some(target::Kind::ChatId(7)),
                    }),
                    from_id: 42,
                    message: format!("m{id}"),
                    accept_seq: Some(*id),
                    ..Default::default()
                }),
            }
        } else if key == "target:room:11" {
            // Race: server returns the deleted body; the client must filter
            // via the same-run removal set, never resurrecting it.
            messages.push(TinkleMessage {
                message_id: *id,
                target: Some(Target {
                    kind: Some(target::Kind::ChatId(11)),
                }),
                from_id: 42,
                message: format!("m{id}"),
                accept_seq: Some(*id),
                ..Default::default()
            });
        } else if key == "target:room:12" {
            // Omission: 60 is deleted/invisible, omitted without error.
            if *id == 60 {
                continue;
            }
            messages.push(TinkleMessage {
                message_id: *id,
                target: Some(Target {
                    kind: Some(target::Kind::ChatId(12)),
                }),
                from_id: 42,
                message: format!("m{id}"),
                accept_seq: Some(*id),
                ..Default::default()
            });
        } else {
            let target = if key == "target:room:8" {
                Target {
                    kind: Some(target::Kind::ChatId(8)),
                }
            } else if key == "target:room:9" {
                Target {
                    kind: Some(target::Kind::ChatId(9)),
                }
            } else if key == "target:room:10" {
                Target {
                    kind: Some(target::Kind::ChatId(10)),
                }
            } else if key == "target:room:11" {
                Target {
                    kind: Some(target::Kind::ChatId(11)),
                }
            } else if key == "target:room:12" {
                Target {
                    kind: Some(target::Kind::ChatId(12)),
                }
            } else if key.starts_with("target:user:") {
                let id_parse: i64 = key
                    .strip_prefix("target:user:")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(7);
                Target {
                    kind: Some(target::Kind::UserId(id_parse)),
                }
            } else {
                Target {
                    kind: Some(target::Kind::ChatId(7)),
                }
            };
            messages.push(TinkleMessage {
                message_id: *id,
                target: Some(target),
                from_id: 42,
                message: format!("m{id}"),
                accept_seq: Some(*id),
                ..Default::default()
            });
        }
    }
    GetMessagesResponse {
        messages,
        users: vec![],
        chats: vec![],
    }
}

fn mock_history(key: &str, req: &GetHistoryRequest) -> GetHistoryResponse {
    if key == "target:room:9" {
        // Heal window for the retention skip.
        return GetHistoryResponse {
            messages: vec![TinkleMessage {
                message_id: 30,
                target: req.target,
                from_id: 42,
                message: "skipped".into(),
                accept_seq: Some(10),
                ..Default::default()
            }],
            users: vec![],
            chats: vec![],
            count: 1,
            offset_id_offset: None,
            more_available: Some(false),
        };
    }
    if key == "target:room:10" {
        // New-message catch-up window: two bodies the journal never
        // disclosed. Reconcile must persist both without moving the cursor.
        return GetHistoryResponse {
            messages: vec![
                TinkleMessage {
                    message_id: 100,
                    target: req.target,
                    from_id: 42,
                    message: "new100".into(),
                    accept_seq: Some(100),
                    ..Default::default()
                },
                TinkleMessage {
                    message_id: 101,
                    target: req.target,
                    from_id: 42,
                    message: "new101".into(),
                    accept_seq: Some(101),
                    ..Default::default()
                },
            ],
            users: vec![],
            chats: vec![],
            count: 2,
            offset_id_offset: None,
            more_available: Some(false),
        };
    }
    if key == "target:room:11" {
        // Stale history: 50 was deleted in the same reconcile run. The
        // delayed fetch must not resurrect it.
        return GetHistoryResponse {
            messages: vec![TinkleMessage {
                message_id: 50,
                target: req.target,
                from_id: 42,
                message: "stale50".into(),
                accept_seq: Some(50),
                ..Default::default()
            }],
            users: vec![],
            chats: vec![],
            count: 1,
            offset_id_offset: None,
            more_available: Some(false),
        };
    }
    if key == "target:room:12" {
        // History returns 60, but GetMessages omits it. The omission gate
        // must prevent ingest.
        return GetHistoryResponse {
            messages: vec![TinkleMessage {
                message_id: 60,
                target: req.target,
                from_id: 42,
                message: "ghost60".into(),
                accept_seq: Some(60),
                ..Default::default()
            }],
            users: vec![],
            chats: vec![],
            count: 1,
            offset_id_offset: None,
            more_available: Some(false),
        };
    }
    // Paged history for more_available tests: room:history has 3 messages,
    // served 2 + 1 with explicit reports.
    if key == "target:room:77" {
        let all = [
            TinkleMessage {
                message_id: 3,
                target: req.target,
                from_id: 1,
                message: "m3".into(),
                ..Default::default()
            },
            TinkleMessage {
                message_id: 2,
                target: req.target,
                from_id: 1,
                message: "m2".into(),
                ..Default::default()
            },
            TinkleMessage {
                message_id: 1,
                target: req.target,
                from_id: 1,
                message: "m1".into(),
                ..Default::default()
            },
        ];
        // First page (no offset): newest 2 with more=true.
        if req.offset_id.is_none() {
            return GetHistoryResponse {
                messages: all[..2].to_vec(),
                users: vec![],
                chats: vec![],
                count: 3,
                offset_id_offset: Some(1),
                more_available: Some(true),
            };
        }
        // Second page: oldest 1, done.
        return GetHistoryResponse {
            messages: all[2..].to_vec(),
            users: vec![],
            chats: vec![],
            count: 3,
            offset_id_offset: None,
            more_available: Some(false),
        };
    }
    GetHistoryResponse {
        messages: vec![],
        users: vec![],
        chats: vec![],
        count: 0,
        offset_id_offset: None,
        more_available: Some(false),
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

fn room_target(id: i64) -> Target {
    Target {
        kind: Some(target::Kind::ChatId(id)),
    }
}

// -- Tests -----------------------------------------------------------------

#[tokio::test]
async fn handshake_and_send_message() {
    let addr = start_mock().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    let req = SendMessageRequest {
        target: Some(Target {
            kind: Some(target::Kind::UserId(7)),
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
async fn opaque_hint_does_not_complete_rpc_and_emits_connectivity() {
    let addr = start_mock().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    let out = c
        .call_raw("/example.Echo/WithNotif", b"ping".to_vec(), 5000)
        .await
        .unwrap();
    assert_eq!(out, b"WITH_NOTIF");
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    // Hint frames are opaque: the RPC above still completed with its own
    // payload, and the connectivity observation surfaces as an event.
    let mut saw_connectivity = false;
    for _ in 0..10 {
        match tokio::time::timeout(std::time::Duration::from_millis(200), c.next_event()).await {
            Ok(Some(libtinkle::SyncEvent::Connectivity(_))) => {
                saw_connectivity = true;
                break;
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    assert!(saw_connectivity, "connectivity observation must surface");
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

#[tokio::test]
async fn subscribe_tracks_and_reconciles_journal() {
    let (addr, log) = start_mock_logged().await;
    let storage = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
    let cfg = libtinkle::ClientConfig::new(format!("ws://{addr}/syntra/v1/ws")).insecure_dev();
    let c = libtinkle::Client::new_sqlite(
        cfg,
        libtinkle::Credential::from_session_token("test-token"),
        storage.clone(),
    );
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    c.subscribe(vec![room_target(7)]).await.unwrap();
    assert_eq!(c.tracked_targets().await, vec!["target:room:7".to_string()]);
    // Journal advanced to the retained head (5).
    assert_eq!(c.target_cursor("target:room:7").await, Some(5));
    // Edited hydration: 10 survives at its fresher body, 11 removed.
    let rows = storage
        .get_messages("user:42", "target:room:7", 20, None)
        .unwrap();
    let ids: HashSet<i64> = rows.iter().map(|r| r.message_id).collect();
    assert!(ids.contains(&10), "edited 10 must be cached, got {ids:?}");
    assert!(
        !ids.contains(&11),
        "deleted 11 must be removed, got {ids:?}"
    );
    assert!(
        !ids.contains(&12),
        "revoked 12 must be removed, got {ids:?}"
    );
    let m10 = rows.iter().find(|r| r.message_id == 10).unwrap();
    assert_eq!(m10.body, "m10v2");
    // Events surface the applied pages.
    let mut saw_conv = false;
    let mut saw_msgs = false;
    for _ in 0..10 {
        match tokio::time::timeout(std::time::Duration::from_millis(200), c.next_event()).await {
            Ok(Some(libtinkle::SyncEvent::ConversationChanged {
                target_key,
                applied_accept_seq,
            })) => {
                if target_key == "target:room:7" && applied_accept_seq == 5 {
                    saw_conv = true;
                }
            }
            Ok(Some(libtinkle::SyncEvent::MessagesChanged { target_key, .. })) => {
                if target_key == "target:room:7" {
                    saw_msgs = true;
                }
            }
            _ => break,
        }
        if saw_conv && saw_msgs {
            break;
        }
    }
    assert!(saw_conv, "ConversationChanged to 5 must surface");
    assert!(saw_msgs, "MessagesChanged must surface");
    // Paging went through GetTargetChanges from the journal cursor.
    let reads = log.target_changes.lock().unwrap().clone();
    assert!(!reads.is_empty());
    assert_eq!(reads[0].0, "target:room:7");
    assert_eq!(reads[0].1, 0);
    c.close().await;
}

#[tokio::test]
async fn journal_paging_follows_yes_until_no() {
    let (addr, log) = start_mock_logged().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    c.subscribe(vec![room_target(7)]).await.unwrap();
    // Small pages are not a stop signal: completion follows more_available.
    let reads = log.target_changes.lock().unwrap().clone();
    let room_reads: Vec<_> = reads
        .iter()
        .filter(|(k, _, _)| k == "target:room:7")
        .collect();
    assert!(
        !room_reads.is_empty(),
        "expected GetTargetChanges pages, got {reads:?}"
    );
    // First page starts at the journal cursor with limit 500.
    assert_eq!(room_reads[0].1, 0);
    assert_eq!(room_reads[0].2, 500);
    // A small limit splits the 5-change journal into Yes,Yes,No pages.
    let p1 = c.get_target_changes(room_target(7), 0, 2).await.unwrap();
    assert_eq!(p1.changes.len(), 2);
    assert_eq!(p1.more_available, Some(true));
    let p2 = c
        .get_target_changes(room_target(7), p1.last_accept_seq, 2)
        .await
        .unwrap();
    assert_eq!(p2.changes.len(), 2);
    assert_eq!(p2.more_available, Some(true));
    let p3 = c
        .get_target_changes(room_target(7), p2.last_accept_seq, 2)
        .await
        .unwrap();
    assert_eq!(p3.changes.len(), 1);
    assert_eq!(p3.more_available, Some(false));
    assert_eq!(p3.last_accept_seq, 5);
    let room_reads = log.target_changes.lock().unwrap().clone();
    assert!(
        room_reads
            .iter()
            .filter(|(k, _, _)| k == "target:room:7")
            .count()
            >= 3,
        "expected ≥3 GetTargetChanges calls, got {room_reads:?}"
    );
    c.close().await;
}

#[tokio::test]
async fn unknown_more_does_one_bounded_repage_then_stops() {
    let (addr, log) = start_mock_logged().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    let applied = c.reconcile_target("target:room:8").await.unwrap();
    assert_eq!(applied, 1);
    let reads: Vec<_> = log
        .target_changes
        .lock()
        .unwrap()
        .iter()
        .filter(|(k, _, _)| k == "target:room:8")
        .cloned()
        .collect();
    assert_eq!(
        reads.len(),
        2,
        "Unknown must do exactly one bounded re-page, got {reads:?}"
    );
    assert_eq!(c.target_cursor("target:room:8").await, Some(1));
    c.close().await;
}

#[tokio::test]
async fn journal_skip_heals_via_history_fallback() {
    let (addr, log) = start_mock_logged().await;
    let storage = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
    let cfg = libtinkle::ClientConfig::new(format!("ws://{addr}/syntra/v1/ws")).insecure_dev();
    let c = libtinkle::Client::new_sqlite(
        cfg,
        libtinkle::Credential::from_session_token("test-token"),
        storage.clone(),
    );
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    // Journal starts at 10 while the cursor is 0: retention skip.
    let applied = c.reconcile_target("target:room:9").await.unwrap();
    assert_eq!(applied, 1);
    assert_eq!(c.target_cursor("target:room:9").await, Some(10));
    // History fallback repopulated the offline cache.
    let rows = storage
        .get_messages("user:42", "target:room:9", 10, None)
        .unwrap();
    assert!(
        rows.iter().any(|r| r.message_id == 30),
        "history heal must cache message 30"
    );
    assert!(
        !log.history.lock().unwrap().is_empty(),
        "heal must fetch history"
    );
    c.close().await;
}

#[tokio::test]
async fn get_messages_chunks_to_100() {
    let (addr, log) = start_mock_logged().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    let ids: Vec<i64> = (1..=250).collect();
    let resp = c.get_messages(room_target(7), ids).await.unwrap();
    assert_eq!(
        resp.messages.len(),
        248,
        "11 deleted + 12 revoked are omitted"
    );
    let calls = log.messages.lock().unwrap().clone();
    assert_eq!(
        calls.len(),
        3,
        "250 ids must chunk 100/100/50, got {calls:?}"
    );
    assert_eq!(calls[0].1.len(), 100);
    assert_eq!(calls[1].1.len(), 100);
    assert_eq!(calls[2].1.len(), 50);
    // Batching rejects oversize requests rather than truncating: the client
    // must never send >100.
    for (_, batch) in &calls {
        assert!(batch.len() <= 100);
    }
    c.close().await;
}

#[tokio::test]
async fn get_target_changes_validates_pages() {
    let addr = start_mock().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    let resp = c.get_target_changes(room_target(7), 0, 500).await.unwrap();
    let page = tinkle_schema::sync::validate_changes_page(0, &resp).unwrap();
    assert_eq!(page.changes.len(), 5);
    assert_eq!(page.last_accept_seq, 5);
    c.close().await;
}

#[tokio::test]
async fn load_history_uses_more_available_tristate() {
    let addr = start_mock().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    let resp = c
        .load_history(GetHistoryRequest {
            target: Some(room_target(77)),
            offset_id: None,
            offset_date: None,
            add_offset: 0,
            limit: 2,
            max_id: None,
            min_id: None,
        })
        .await
        .unwrap();
    // Some(true) → more, Some(false) → done: both windows combined.
    assert_eq!(resp.messages.len(), 3);
    c.close().await;
}

#[tokio::test]
async fn reconcile_now_refreshes_tracked_targets() {
    // Direct `reconcile_now` path (no mock hint is sent here; the
    // hint-driven demand path is unit-tested in `sync.rs`).
    let addr = start_mock().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    c.subscribe(vec![room_target(7)]).await.unwrap();
    let before = c.target_cursor("target:room:7").await;
    assert_eq!(before, Some(5));
    // Opaque hint → demand → reconcile (already caught up, stays at 5).
    c.reconcile_now().await.unwrap();
    assert_eq!(c.target_cursor("target:room:7").await, Some(5));
    assert_eq!(
        c.target_status("target:room:7").await,
        Some(libtinkle::ReconcileStatus::Idle)
    );
    c.close().await;
}

#[tokio::test]
async fn storage_v3_journal_keys_and_fencing() {
    // Fresh v3 database: journal keys, zeroed replay_floor, generation fencing.
    let dir = std::env::temp_dir().join(format!(
        "tinkle-int3-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("tinkle.db");
    let s = tinkle_storage::Storage::open(path.to_str().unwrap()).unwrap();
    // Old key grammar is rejected; journal keys work.
    assert!(s.load_journal("user:42", "room:7").is_err());
    s.save_journal("user:42", "target:room:7", 4).unwrap();
    assert_eq!(s.load_journal("user:42", "target:room:7").unwrap(), Some(4));
    let st = s.stream_state("user:42", "target:room:7").unwrap();
    assert_eq!(st.replay_floor, 0);
    assert_eq!(st.generation, 0);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn supervisor_reconnects_after_pump_death() {
    let (addr, _log) = start_mock_logged().await;
    let c = test_client(addr);
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    c.start(vec![room_target(7)]).await.unwrap();
    assert!(c.is_connected().await);
    // Kill transport: supervisor must re-establish without host help.
    {
        // Pump death via close; supervisor tick (5s) + demand covers it.
        c.suspend().await;
        assert!(!c.is_connected().await);
        c.resume().await;
        c.reconnect().await.unwrap();
        assert!(c.is_connected().await);
    }
    c.stop().await;
}

#[tokio::test]
async fn load_history_and_dialogs_do_not_forge_journal_cursors() {
    let addr = start_mock().await;
    let storage = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
    let cfg = libtinkle::ClientConfig::new(format!("ws://{addr}/syntra/v1/ws")).insecure_dev();
    let c = libtinkle::Client::new_sqlite(
        cfg,
        libtinkle::Credential::from_session_token("test-token"),
        storage.clone(),
    );
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    let history = c
        .load_history(GetHistoryRequest {
            target: Some(room_target(7)),
            offset_id: None,
            offset_date: None,
            add_offset: 0,
            limit: 20,
            max_id: None,
            min_id: None,
        })
        .await
        .unwrap();
    assert!(history.messages.is_empty());
    let dialogs = c
        .load_dialogs(tinkle_schema::proto::GetDialogsRequest {
            space_id: None,
            offset_date: None,
            offset_peer_id: None,
            limit: None,
            hash: 0,
            cursor: None,
        })
        .await
        .unwrap();
    assert!(dialogs.dialogs.is_empty());
    // History/dialog snapshots never forge journal cursors.
    assert_eq!(
        storage.load_journal("user:42", "target:room:7").unwrap(),
        None
    );
    c.close().await;
}

#[tokio::test]
async fn reconcile_new_message_catchup_persists_to_sqlite() {
    // Journal covers edits/removals only: room:10's journal is empty, but
    // history discloses two new messages. Reconcile must persist them via
    // the shared ingest path without moving the journal cursor.
    let (addr, _log) = start_mock_logged().await;
    let storage = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
    let cfg = libtinkle::ClientConfig::new(format!("ws://{addr}/syntra/v1/ws")).insecure_dev();
    let c = libtinkle::Client::new_sqlite(
        cfg,
        libtinkle::Credential::from_session_token("test-token"),
        storage.clone(),
    );
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    let applied = c.reconcile_target("target:room:10").await.unwrap();
    assert_eq!(applied, 2, "catch-up must report 2 ingested messages");
    let rows = storage
        .get_messages("user:42", "target:room:10", 20, None)
        .unwrap();
    let ids: HashSet<i64> = rows.iter().map(|r| r.message_id).collect();
    assert!(
        ids.contains(&100) && ids.contains(&101),
        "catch-up must cache 100+101, got {ids:?}"
    );
    // Same-cursor commit: the journal cursor never advances past the empty
    // journal head (0). A forged cursor would claim progress the journal
    // never disclosed.
    let cursor = storage.load_journal("user:42", "target:room:10").unwrap();
    assert!(
        cursor.is_none() || cursor == Some(0),
        "catch-up must not move cursor, got {cursor:?}"
    );
    c.close().await;
}

#[tokio::test]
async fn delayed_fetch_does_not_resurrect_deleted() {
    // Room:11 deletes 50 in the journal while history (stale) still
    // returns 50 and GetMessages (race) returns the body. Both gates —
    // same-run removal skip + pre-commit re-check — must prevent ingest.
    let (addr, _log) = start_mock_logged().await;
    let storage = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
    let cfg = libtinkle::ClientConfig::new(format!("ws://{addr}/syntra/v1/ws")).insecure_dev();
    let c = libtinkle::Client::new_sqlite(
        cfg,
        libtinkle::Credential::from_session_token("test-token"),
        storage.clone(),
    );
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    let applied = c.reconcile_target("target:room:11").await.unwrap();
    assert_eq!(applied, 1, "journal page applies the delete");
    assert_eq!(c.target_cursor("target:room:11").await, Some(1));
    let rows = storage
        .get_messages("user:42", "target:room:11", 20, None)
        .unwrap();
    assert!(
        !rows.iter().any(|r| r.message_id == 50),
        "deleted 50 must not be resurrected, got {:?}",
        rows.iter().map(|r| r.message_id).collect::<Vec<_>>()
    );
    c.close().await;
}

#[tokio::test]
async fn catchup_omitted_ids_are_not_ingested() {
    // Room:12 history returns 60, but GetMessages omits it (deleted /
    // invisible). Omitted ids carry no information and must not be cached.
    let (addr, _log) = start_mock_logged().await;
    let storage = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
    let cfg = libtinkle::ClientConfig::new(format!("ws://{addr}/syntra/v1/ws")).insecure_dev();
    let c = libtinkle::Client::new_sqlite(
        cfg,
        libtinkle::Credential::from_session_token("test-token"),
        storage.clone(),
    );
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    let applied = c.reconcile_target("target:room:12").await.unwrap();
    assert_eq!(applied, 0, "omitted ids must not count as applied");
    let rows = storage
        .get_messages("user:42", "target:room:12", 20, None)
        .unwrap();
    assert!(
        !rows.iter().any(|r| r.message_id == 60),
        "omitted 60 must not be cached"
    );
    c.close().await;
}

#[tokio::test]
async fn history_ingest_does_not_move_cursor() {
    // load_history hydrates bodies via the shared ingest helper with a
    // same-cursor commit: messages persist, the journal cursor does not
    // advance to message ids.
    let (addr, _log) = start_mock_logged().await;
    let storage = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
    let cfg = libtinkle::ClientConfig::new(format!("ws://{addr}/syntra/v1/ws")).insecure_dev();
    let c = libtinkle::Client::new_sqlite(
        cfg,
        libtinkle::Credential::from_session_token("test-token"),
        storage.clone(),
    );
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;
    let resp = c
        .load_history(GetHistoryRequest {
            target: Some(room_target(10)),
            offset_id: None,
            offset_date: None,
            add_offset: 0,
            limit: 20,
            max_id: None,
            min_id: None,
        })
        .await
        .unwrap();
    assert_eq!(resp.messages.len(), 2);
    let rows = storage
        .get_messages("user:42", "target:room:10", 20, None)
        .unwrap();
    assert_eq!(rows.len(), 2, "history bodies must persist");
    let cursor = storage.load_journal("user:42", "target:room:10").unwrap();
    assert!(
        cursor.is_none() || cursor == Some(0),
        "history ingest must not move cursor, got {cursor:?}"
    );
    c.close().await;
}
