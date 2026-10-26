//! Push-wake integration: envelope → durable SQLite message.
//!
//! Spins a minimal in-process Syntra mock gateway (handshake +
//! `GetTargetChanges` / `GetMessages` / `GetHistory` / push-token RPCs)
//! and drives `libtinkle::PushManager` through the required scenarios:
//! duplicate idempotent, cold-start persist, app/extension race fenced,
//! logout fencing, deadline expiry → deferred, and the KEY test (a
//! new-message push with an EMPTY mutation journal produces a durable
//! SQLite message).

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

use syntra_core::proto::{
    frame::Body, rpc_result::Outcome, Frame, Notification, RpcRequest, RpcResult,
};
use syntra_core::{decode_frame, encode_frame, ConnectionState, ValidationContext};
use tinkle_schema::proto::{
    target, GetHistoryRequest, GetHistoryResponse, GetMessagesRequest, GetMessagesResponse,
    GetTargetChangesRequest, GetTargetChangesResponse, LogoutResponse, RegisterPushResponse,
    Target, UnregisterPushResponse,
};

// -- Minimal mock gateway ----------------------------------------------------

fn target_key_of(t: &Option<Target>) -> String {
    match t.as_ref().and_then(|x| x.kind.as_ref()) {
        Some(target::Kind::UserId(id)) => format!("target:user:{id}"),
        Some(target::Kind::ChatId(id)) => format!("target:room:{id}"),
        None => "target:unknown".to_string(),
    }
}

async fn handle_mock_request(id: u64, req: RpcRequest) -> Frame {
    let result = match req.method.as_str() {
        "/tinkle.v1.MessagesService/GetTargetChanges" => {
            // EMPTY mutation journal for every target: the KEY scenario.
            let r =
                GetTargetChangesRequest::decode(&*req.payload).unwrap_or(GetTargetChangesRequest {
                    target: None,
                    after_accept_seq: 0,
                    limit: 0,
                });
            let resp = GetTargetChangesResponse {
                changes: vec![],
                more_available: Some(false),
                last_accept_seq: r.after_accept_seq,
            };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            RpcResult {
                outcome: Some(Outcome::Payload(b)),
            }
        }
        "/tinkle.v1.MessagesService/GetMessages" => {
            // Authoritative echo: every requested id returns a body.
            let r = GetMessagesRequest::decode(&*req.payload).unwrap_or(GetMessagesRequest {
                target: None,
                message_ids: vec![],
            });
            let key = target_key_of(&r.target);
            let (peer_kind, peer_id) = match r.target.as_ref().and_then(|t| t.kind.as_ref()) {
                Some(target::Kind::ChatId(cid)) => ("room".to_string(), *cid),
                Some(target::Kind::UserId(uid)) => ("user".to_string(), *uid),
                None => ("unknown".to_string(), 0),
            };
            let _ = (key, peer_kind, peer_id);
            let mut messages = Vec::new();
            for mid in r.message_ids {
                if mid <= 0 {
                    continue;
                }
                messages.push(tinkle_schema::proto::Message {
                    message_id: mid,
                    target: r.target,
                    from_id: 42,
                    message: format!("push-body-{mid}"),
                    accept_seq: Some(mid),
                    ..Default::default()
                });
            }
            let resp = GetMessagesResponse {
                messages,
                users: vec![],
                chats: vec![],
            };
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
            let _ = r;
            let resp = GetHistoryResponse {
                messages: vec![],
                users: vec![],
                chats: vec![],
                count: 0,
                offset_id_offset: None,
                more_available: Some(false),
            };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            RpcResult {
                outcome: Some(Outcome::Payload(b)),
            }
        }
        "/tinkle.v1.AccountService/RegisterPush" => {
            let resp = RegisterPushResponse { device: None };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            RpcResult {
                outcome: Some(Outcome::Payload(b)),
            }
        }
        "/tinkle.v1.AccountService/UnregisterPush" => {
            let resp = UnregisterPushResponse { removed: true };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            RpcResult {
                outcome: Some(Outcome::Payload(b)),
            }
        }
        "/tinkle.v1.AccountService/Logout" => {
            let resp = LogoutResponse { logged_out: true };
            let mut b = Vec::new();
            resp.encode(&mut b).unwrap();
            RpcResult {
                outcome: Some(Outcome::Payload(b)),
            }
        }
        _ => RpcResult {
            outcome: Some(Outcome::Error(syntra_core::proto::RpcError {
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

async fn mock_gateway(listener: TcpListener) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => return,
        };
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
                    connection_id: format!("mock-push-{}", rand::random::<u32>()),
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
                let msg = match read.next().await {
                    Some(Ok(m)) => m,
                    _ => break,
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
                    _ => continue,
                };
                // Opaque notifications are never sent here; the push path
                // does not depend on them.
                let _ = Notification { payload: vec![] };
                let resp_frame = handle_mock_request(id, req).await;
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

async fn start_mock() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(mock_gateway(listener));
    addr
}

fn push_client(addr: SocketAddr, storage: Arc<tinkle_storage::Storage>) -> libtinkle::Client {
    let cfg = libtinkle::ClientConfig::new(format!("ws://{addr}/syntra/v1/ws")).insecure_dev();
    libtinkle::Client::new_sqlite(
        cfg,
        libtinkle::Credential::from_session_token("test-token"),
        storage,
    )
}

fn push_envelope(event: &str, ids: Vec<i64>) -> libtinkle::PushEnvelope {
    libtinkle::PushEnvelope {
        version: libtinkle::PUSH_ENVELOPE_VERSION,
        account_user_id: Some(42),
        target_kind: Some("room".to_string()),
        target_id: Some(10),
        message_ids: ids,
        event_id: event.to_string(),
    }
}

// -- Tests -------------------------------------------------------------------

/// KEY test: a new-message push with an EMPTY mutation journal produces a
/// durable SQLite message (the bodies come from `GetMessages`, never from
/// notification text, and commit without moving the journal cursor).
#[tokio::test]
async fn push_new_message_with_empty_journal_persists() {
    let addr = start_mock().await;
    let storage = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
    let c = push_client(addr, storage.clone());
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;

    let mgr = libtinkle::PushManager::new(c.clone());
    let outcome = mgr
        .process_push(push_envelope("evt-key-1", vec![100, 101]))
        .await;
    let (target_key, ids) = match outcome {
        libtinkle::PushOutcome::Committed {
            target_key,
            message_ids,
        } => (target_key, message_ids),
        other => panic!("expected Committed, got {other:?}"),
    };
    assert_eq!(target_key, "target:room:10");
    assert_eq!(ids, vec![100, 101]);

    let rows = storage
        .get_messages("user:42", "target:room:10", 20, None)
        .unwrap();
    let got: std::collections::HashSet<i64> = rows.iter().map(|r| r.message_id).collect();
    assert!(
        got.contains(&100) && got.contains(&101),
        "push bodies must be durable, got {got:?}"
    );
    // Same-cursor commit: the empty journal never advances.
    let cursor = storage.load_journal("user:42", "target:room:10").unwrap();
    assert!(
        cursor.is_none() || cursor == Some(0),
        "push ingest must not move the journal cursor, got {cursor:?}"
    );
    c.close().await;
}

/// Duplicate delivery of the same event is idempotent: the second wake
/// replays Committed without duplicating rows.
#[tokio::test]
async fn push_duplicate_event_is_idempotent() {
    let addr = start_mock().await;
    let storage = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
    let c = push_client(addr, storage.clone());
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;

    let mgr = libtinkle::PushManager::new(c.clone());
    let env = push_envelope("evt-dup-1", vec![200]);
    let first = mgr.process_push(env.clone()).await;
    assert!(
        matches!(first, libtinkle::PushOutcome::Committed { .. }),
        "first wake must commit, got {first:?}"
    );
    let second = mgr.process_push(env).await;
    assert!(
        matches!(second, libtinkle::PushOutcome::Committed { .. }),
        "duplicate wake must replay Committed, got {second:?}"
    );
    let rows = storage
        .get_messages("user:42", "target:room:10", 20, None)
        .unwrap();
    let count = rows.iter().filter(|r| r.message_id == 200).count();
    assert_eq!(count, 1, "duplicate push must not duplicate rows");
    c.close().await;
}

/// App/extension race: two concurrent wakes for one event fence on the
/// durable claim — exactly one commits, the loser defers (never fails),
/// and the row exists exactly once.
#[tokio::test]
async fn push_app_extension_race_is_fenced() {
    let addr = start_mock().await;
    let storage = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
    let c = push_client(addr, storage.clone());
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;

    let mgr_a = libtinkle::PushManager::new(c.clone());
    let mgr_b = libtinkle::PushManager::new(c.clone());
    let env = push_envelope("evt-race-1", vec![300]);
    let (a, b) = tokio::join!(mgr_a.process_push(env.clone()), mgr_b.process_push(env));
    let mut committed = 0;
    let mut deferred = 0;
    for o in [a, b] {
        match o {
            libtinkle::PushOutcome::Committed { .. } => committed += 1,
            libtinkle::PushOutcome::Deferred { .. } => deferred += 1,
            libtinkle::PushOutcome::Failed { reason } => {
                panic!("race loser must defer, not fail: {reason}")
            }
        }
    }
    assert_eq!(committed, 1, "exactly one wake must commit");
    assert_eq!(deferred, 1, "the loser must defer");
    let rows = storage
        .get_messages("user:42", "target:room:10", 20, None)
        .unwrap();
    assert_eq!(
        rows.iter().filter(|r| r.message_id == 300).count(),
        1,
        "raced push must persist exactly once"
    );
    c.close().await;
}

/// Logout fencing: after `logout()` the same envelope defers as logged
/// out and commits nothing; the principal's rows are wiped.
#[tokio::test]
async fn push_after_logout_defers_and_wipes() {
    let addr = start_mock().await;
    let storage = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
    let c = push_client(addr, storage.clone());
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;

    let mgr = libtinkle::PushManager::new(c.clone());
    let committed = mgr.process_push(push_envelope("evt-lo-1", vec![400])).await;
    assert!(
        matches!(committed, libtinkle::PushOutcome::Committed { .. }),
        "pre-logout wake must commit, got {committed:?}"
    );
    assert!(c.logout().await.is_ok());

    let after = mgr.process_push(push_envelope("evt-lo-2", vec![401])).await;
    match after {
        libtinkle::PushOutcome::Deferred { reason } => assert_eq!(reason, "logged_out"),
        other => panic!("post-logout wake must defer logged_out, got {other:?}"),
    }
    let rows = storage
        .get_messages("user:42", "target:room:10", 20, None)
        .unwrap_or_default();
    assert!(
        rows.is_empty(),
        "logout must wipe the principal's rows, got {}",
        rows.len()
    );
    c.close().await;
}

/// An expired deadline defers without touching the network; the durable
/// job survives for the next wake.
#[tokio::test]
async fn push_deadline_expiry_defers() {
    let storage = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
    // No connection: the expired deadline must win before any dial.
    let cfg =
        libtinkle::ClientConfig::new("ws://127.0.0.1:1/syntra/v1/ws".to_string()).insecure_dev();
    let c = libtinkle::Client::new_sqlite(
        cfg,
        libtinkle::Credential::from_session_token("test-token"),
        storage.clone(),
    );
    c.set_principal_user_id(42).await;

    let mgr = libtinkle::PushManager::new(c.clone());
    let past = tokio::time::Instant::now() - std::time::Duration::from_secs(1);
    let outcome = mgr
        .process_push_with_deadline(push_envelope("evt-dl-1", vec![500]), past)
        .await;
    match outcome {
        libtinkle::PushOutcome::Deferred { reason } => assert_eq!(reason, "deadline"),
        other => panic!("expired deadline must defer, got {other:?}"),
    }
    let job = storage
        .get_background_job("user:42", "push:evt-dl-1")
        .unwrap();
    assert!(
        job.is_some(),
        "deferred push must leave a durable job for retry"
    );
    c.close().await;
}

/// Pending work survives process death: a job enqueued by one client
/// instance is visible to the next instance sharing the database file,
/// including stale-claim recovery after a killed extension.
#[tokio::test]
async fn push_pending_work_survives_process_death() {
    let dir = std::env::temp_dir().join(format!(
        "tinkle-push-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("push.db");
    let path_str = path.to_str().unwrap().to_string();

    let job_id = "push:evt-cold-1".to_string();
    {
        let storage = Arc::new(tinkle_storage::Storage::open(&path_str).unwrap());
        let job = tinkle_storage::BackgroundSyncJob {
            job_id: job_id.clone(),
            kind: libtinkle::PUSH_JOB_KIND.to_string(),
            target_key: "target:room:10".to_string(),
            state: "pending".to_string(),
            payload: br#"{"v":1}"#.to_vec(),
            attempts: 0,
            next_due: 0,
            claimed_by: None,
            updated_at: 0,
        };
        storage.enqueue_background_job("user:42", &job).unwrap();
        // Simulate an extension killed mid-wake holding the claim.
        assert!(storage
            .claim_background_job("user:42", &job_id, "push-ext-1", None, 1)
            .unwrap());
        // `storage` (and its owner) drops here: process death.
    }
    {
        let storage = Arc::new(tinkle_storage::Storage::open(&path_str).unwrap());
        let job = storage
            .get_background_job("user:42", &job_id)
            .unwrap()
            .expect("job must survive reopen");
        assert_eq!(job.state, "claimed");
        // Cold start requeues the stale lease, then the app claims it.
        // The claim landed at updated_at=1: a cutoff of 0 keeps it.
        let requeued = storage
            .requeue_stale_background_jobs("user:42", 0, 2, 2)
            .unwrap();
        assert_eq!(requeued, 0, "fresh lease must not requeue yet");
        let requeued = storage
            .requeue_stale_background_jobs("user:42", 2, 3, 3)
            .unwrap();
        assert_eq!(requeued, 1, "expired lease must requeue");
        assert!(storage
            .claim_background_job("user:42", &job_id, "push-app-1", None, 4)
            .unwrap());
        storage
            .complete_background_job("user:42", &job_id, "done", 5)
            .unwrap();
    }
    std::fs::remove_dir_all(&dir).ok();
}

/// Typed push-token lifecycle over the mock AccountService.
#[tokio::test]
async fn push_token_lifecycle_round_trip() {
    let addr = start_mock().await;
    let storage = Arc::new(tinkle_storage::Storage::open_in_memory().unwrap());
    let c = push_client(addr, storage);
    c.connect().await.unwrap();
    c.set_principal_user_id(42).await;

    c.register_push(
        "apns-token-abc".to_string(),
        libtinkle::PushTokenKind::Apns,
        "en".to_string(),
    )
    .await
    .unwrap();
    assert!(libtinkle::PushTokenKind::parse("nope").is_err());
    let removed = c
        .unregister_push("apns-token-abc".to_string())
        .await
        .unwrap();
    assert!(removed);
    assert!(c.logout().await.unwrap());
    c.close().await;
}
