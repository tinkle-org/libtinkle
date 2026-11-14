//! Authenticated encrypted presentation notifications, independent of durable reconciliation.
#[allow(dead_code)]
mod support;
use libtinkle::{Client, Enrollment, InMemoryCursorStore, TrustSource};
use prost::Message as _;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use syntra_core::secure_channel::{self, Mode, Secret32};
use syntra_runtime::{ConnectionConfig, FnHandler, HandlerOutcome};
use tinkle_schema::proto as p;

/// Fixture-only active credential; the real Returning channel proves this exact tuple/key.
struct Keys;
impl secure_channel::KeyStore for Keys {
    fn lookup(&self, c: &secure_channel::Context) -> Result<Secret32, secure_channel::Error> {
        if c.epoch != 1 || c.handle != [8; 8] {
            return Err(secure_channel::Error::Authentication);
        }
        Ok(Secret32::new([9; 32]))
    }
}

fn target() -> p::Target {
    p::Target {
        kind: Some(p::target::Kind::UserId(7)),
    }
}
fn hint(expiry: SystemTime, operation: &str) -> Vec<u8> {
    let d = expiry.duration_since(UNIX_EPOCH).unwrap();
    prost_types::Any {
        type_url: "type.googleapis.com/tinkle.v1.MessageChanged".into(),
        value: p::MessageChanged {
            target: Some(target()),
            message_id: 11,
            body_revision: 3,
            change_kind: p::TargetChangeKind::Reaction as i32,
            reaction_effect: Some(p::ReactionEffect {
                operation_id: operation.into(),
                actor_id: 42,
                expires_at: Some(prost_types::Timestamp {
                    seconds: d.as_secs() as i64,
                    nanos: d.subsec_nanos() as i32,
                }),
                reactions: vec![p::Reaction {
                    emoticon: "x".into(),
                    custom_emoji_id: 77,
                    asset_version: Some(4),
                }],
            }),
            ..Default::default()
        }
        .encode_to_vec(),
    }
    .encode_to_vec()
}

/// Poll actual RPC receipt, not arbitrary scheduler delay, before inspecting the app event queue.
async fn reconciled(pages: &AtomicUsize, before: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while pages.load(Ordering::SeqCst) <= before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
/// Drain non-presentation events while proving no additional effect escapes the SDK boundary.
async fn no_effect(client: &Client) {
    let _ = tokio::time::timeout(Duration::from_millis(200), async {
        while let Some(event) = client.next_event().await {
            assert!(!matches!(event, libtinkle::SyncEvent::ReactionEffect(_)));
        }
    })
    .await;
}
/// SDK application events must observe presentation once, while hints still demand real journal RPCs.
async fn effect_flow(mode: &str) {
    let returning = mode != "bootstrap";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let pages = Arc::new(AtomicUsize::new(0));
    let page_count = pages.clone();
    let (host_tx, mut host_rx) = tokio::sync::mpsc::channel(2);
    let connections = if mode == "reconnect" { 2 } else { 1 };
    let server = tokio::spawn(async move {
        for _ in 0..connections {
            let (stream, _) = listener.accept().await.unwrap();
            let mut security = support::server_security();
            security.mode = if returning {
                Mode::Returning
            } else {
                Mode::Bootstrap
            };
            security.store = Some(Arc::new(Keys));
            let page_count = page_count.clone();
            let handler = Arc::new(FnHandler(move |method: String, bytes: Vec<u8>, _| {
                let pages = page_count.clone();
                async move {
                    let wrapped = method == "/tinkle.v1.ConnectionService/InitConnect";
                    let (method, bytes) = if wrapped {
                        let init = p::InitConnectRequest::decode(bytes.as_slice()).unwrap();
                        (init.method, init.payload.to_vec())
                    } else {
                        (method, bytes)
                    };
                    let bytes = match method.rsplit('/').next().unwrap() {
                        "GetSessions" => p::GetSessionsResponse::default().encode_to_vec(),
                        "KeepAlive" => p::KeepAliveResponse {
                            sequence: p::KeepAliveRequest::decode(bytes.as_slice())
                                .unwrap()
                                .sequence,
                        }
                        .encode_to_vec(),
                        "ListMessageChanges" => {
                            pages.fetch_add(1, Ordering::SeqCst);
                            let r = p::ListMessageChangesRequest::decode(bytes.as_slice()).unwrap();
                            p::ListMessageChangesResponse {
                                last_accept_seq: r.after_accept_seq,
                                more_available: Some(false),
                                ..Default::default()
                            }
                            .encode_to_vec()
                        }
                        _ => {
                            return HandlerOutcome::error(
                                syntra_core::ErrorCode::Unimplemented,
                                "fixture method",
                            )
                        }
                    };
                    HandlerOutcome::Payload(if wrapped {
                        p::InitConnectResponse {
                            payload: bytes.into(),
                        }
                        .encode_to_vec()
                    } else {
                        bytes
                    })
                }
            }));
            let handle = syntra_net::tcp::accept_stream(
                stream,
                &security,
                support::factory(),
                handler,
                ConnectionConfig::default(),
                Default::default(),
            )
            .await
            .unwrap();
            host_tx.send(handle.host.clone()).await.unwrap();
            while !handle.is_closed() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            handle.close().await;
        }
    });
    let status = p::DeviceEnrollmentStatus {
        operation_id: "11111111-1111-4111-8111-111111111111".into(),
        session_id: "22222222-2222-4222-8222-222222222222".into(),
        handle: vec![8; 8].into(),
        enrollment_epoch: 1,
        scope: 1,
        state: 2,
        user_id: 42,
        account_auth_epoch: 1,
        expires_at: Some(prost_types::Timestamp {
            seconds: 4_102_444_800,
            nanos: 0,
        }),
    };
    let source = if returning {
        TrustSource::Returning {
            trust: support::trust(),
            enrollment: Enrollment::from_active_status([9; 32], &status).unwrap(),
        }
    } else {
        support::source()
    };
    let client = Client::new(config, source, InMemoryCursorStore::new());
    client.set_principal_user_id(42).await;
    client.start(vec![target()]).await.unwrap();
    let mut host = host_rx.recv().await.unwrap();
    let before = pages.load(Ordering::SeqCst);
    let original = hint(
        SystemTime::now() + Duration::from_secs(4),
        "33333333-3333-4333-8333-333333333333",
    );
    host.notify(original.clone()).await.unwrap();
    host.notify(original.clone()).await.unwrap();
    if !returning {
        reconciled(&pages, before).await;
        no_effect(&client).await;
        client.close().await;
        server.await.unwrap();
        return;
    }
    let event = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(libtinkle::SyncEvent::ReactionEffect(event)) = client.next_event().await {
                break event;
            }
        }
    })
    .await
    .expect("authenticated reaction effect never reached the app event stream");
    let effect = event.notification.reaction_effect.unwrap();
    assert_eq!(effect.actor_id, 42);
    assert_eq!(effect.operation_id, "33333333-3333-4333-8333-333333333333");
    assert_eq!(effect.reactions[0].custom_emoji_id, 77);
    assert_eq!(effect.reactions[0].asset_version, Some(4));
    reconciled(&pages, before).await;
    no_effect(&client).await;
    if mode == "reconnect" {
        client.reconnect().await.unwrap();
        host = host_rx.recv().await.unwrap();
        let before = pages.load(Ordering::SeqCst);
        // The same operation is not replayed by a new transport, even before original expiry.
        host.notify(original).await.unwrap();
        reconciled(&pages, before).await;
        no_effect(&client).await;
    }
    // Duplicate and expired effects are not app events; reconciliation remains independent.
    for deadline in [
        SystemTime::now() - Duration::from_secs(1),
        SystemTime::now() + Duration::from_secs(60),
    ] {
        let before = pages.load(Ordering::SeqCst);
        host.notify(hint(deadline, "44444444-4444-4444-8444-444444444444"))
            .await
            .unwrap();
        reconciled(&pages, before).await;
        no_effect(&client).await;
    }
    let before = pages.load(Ordering::SeqCst);
    host.notify(hint(
        SystemTime::now() + Duration::from_millis(80),
        "66666666-6666-4666-8666-666666666666",
    ))
    .await
    .unwrap();
    reconciled(&pages, before).await;
    tokio::time::sleep(Duration::from_millis(120)).await;
    no_effect(&client).await;
    // An effect queued while authenticated must not escape after local sign-out.
    host.notify(hint(
        SystemTime::now() + Duration::from_secs(4),
        "55555555-5555-4555-8555-555555555555",
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    if mode == "switch" {
        client.set_principal_user_id(99).await;
        assert_eq!(client.principal().await, "user:99");
    } else {
        let _ = client.logout().await;
        assert!(client.is_signed_out());
    }
    while let Some(event) = client.next_event().await {
        assert!(!matches!(event, libtinkle::SyncEvent::ReactionEffect(_)));
    }
    client.close().await;
    server.await.unwrap();
}

/// Real Returning presentation is observed once; expired/queued/sign-out presentation is discarded.
#[tokio::test]
async fn returning_effect_reaches_app_once_and_expiry_and_signout_do_not_replay_it() {
    effect_flow("signout").await;
}
/// Queued presentation cannot cross an account epoch, even when durable events remain drainable.
#[tokio::test]
async fn effect_queue_does_not_cross_account_switch() {
    effect_flow("switch").await;
}
/// Reconnect keeps dedup, but never turns Bootstrap plus a local principal into authenticated effects.
#[tokio::test]
async fn reconnect_dedup_and_bootstrap_admission_are_separate() {
    effect_flow("reconnect").await;
    effect_flow("bootstrap").await;
}
