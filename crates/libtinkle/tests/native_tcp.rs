//! Transport/trust boundaries exercised through published encrypted TCP APIs.
mod support;

use libtinkle::{Client, ClientConfig, Enrollment, InMemoryCursorStore, TinkleError, TrustSource};
use prost::Message;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use syntra_core::{
    proto::{frame::Body, rpc_result::Outcome, Frame, RpcResult},
    secure_channel::{self, Context, KeyStore, Mode, Secret32},
};
use syntra_runtime::{ConnectionConfig, FnHandler, HandlerOutcome};
use tokio::net::TcpListener;

/// Supply a fixed successful payload for the support adapter to correlate with its request.
fn echo() -> Frame {
    Frame {
        version: 1,
        request_id: 1,
        body: Some(Body::Result(RpcResult {
            outcome: Some(Outcome::Payload(b"ok".to_vec())),
        })),
    }
}

/// Explicit fixture ACTIVE status, independent of application enrollment tests.
fn active_enrollment(
    key: [u8; 32],
    handle: [u8; 8],
    epoch: u64,
) -> Result<Enrollment, TinkleError> {
    Enrollment::from_active_status(
        key,
        &tinkle_schema::proto::DeviceEnrollmentStatus {
            operation_id: "96007910-fd5b-4ef2-bf0c-2e4ae1d44b1b".into(),
            session_id: "95007910-fd5b-4ef2-bf0c-2e4ae1d44b1b".into(),
            handle: handle.to_vec().into(),
            enrollment_epoch: epoch,
            scope: 1,
            expires_at: Some(prost_types::Timestamp {
                seconds: 4_102_444_800,
                nanos: 0,
            }),
            state: 2,
            user_id: 42,
            account_auth_epoch: 1,
        },
    )
}

/// Keep the encrypted server connection alive until the client closes it.
async fn keep(handle: syntra_net::tcp::HostHandle) {
    while !handle.is_closed() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    handle.close().await;
}

#[test]
/// Validate default routes, reject malformed trust inputs, and keep enrollment Debug redacted.
fn default_ports_and_trust_inputs_are_explicit() {
    let config = ClientConfig::new("gateway.example.com", support::client_info());
    assert_eq!((config.primary_port, config.fallback_port), (443, Some(80)));
    config.validate().unwrap();
    assert!(
        ClientConfig::new("https://gateway.example.com", support::client_info())
            .validate()
            .is_err()
    );
    assert!(ClientConfig::new("", support::client_info())
        .validate()
        .is_err());
    for (key, handle, epoch) in [
        ([0; 32], [1; 8], 1),
        ([1; 32], [0; 8], 1),
        ([1; 32], [1; 8], 0),
    ] {
        assert!(active_enrollment(key, handle, epoch).is_err());
    }
    let enrollment = active_enrollment([41; 32], [9; 8], 1).unwrap();
    assert_eq!(format!("{enrollment:?}"), "Enrollment([redacted])");
    for scope in [String::new(), "x".repeat(256)] {
        let mut trust = support::trust();
        trust.scope = scope;
        assert!(trust.validate().is_err());
    }
    let mut trust = support::trust();
    trust.server_public_key = [0; 32];
    assert!(trust.validate().is_err());
}

#[tokio::test]
/// Permit fallback after a primary dial failure without changing encrypted server trust.
async fn dial_failure_uses_encrypted_fallback_with_same_trust() {
    let primary = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let primary_addr = primary.local_addr().unwrap();
    let fallback = TcpListener::bind("127.0.0.1:0").await.unwrap();
    drop(primary);
    let mut config = support::config(primary_addr);
    config.fallback_port = Some(fallback.local_addr().unwrap().port());
    let server = tokio::spawn(async move {
        let (stream, _) = fallback.accept().await.unwrap();
        keep(
            support::accept(stream, |_, _| async { echo() })
                .await
                .unwrap(),
        )
        .await;
    });
    let client = Client::new(config, support::source(), InMemoryCursorStore::new());
    client.connect().await.unwrap();
    assert_eq!(
        client
            .call_raw("/test.Echo/Call", vec![], 1000)
            .await
            .unwrap(),
        b"ok"
    );
    client.close().await;
    server.await.unwrap();
}

#[tokio::test]
/// Require InitConnect independently on each connection, only for the Tinkle namespace.
async fn fixture_requires_initialization_per_connection_only_for_tinkle_rpcs() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let dispatched = calls.clone();
        let mut servers = tokio::task::JoinSet::new();
        servers.spawn(async move {
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let dispatched = dispatched.clone();
                keep(
                    support::accept(stream, move |_, _| {
                        dispatched.fetch_add(1, Ordering::SeqCst);
                        async { echo() }
                    })
                    .await
                    .unwrap(),
                )
                .await;
            }
        });
        for _ in 0..2 {
            let trust = support::trust();
            let client = syntra_net::tcp::connect(
                addr,
                secure_channel::ClientConfig {
                    context: Context {
                        mode: Mode::Bootstrap,
                        carrier: secure_channel::Carrier::Tcp,
                        server_key_id: trust.server_key_id,
                        scope: trust.scope.into_bytes(),
                        handle: [0; 8],
                        epoch: 0,
                    },
                    server_public_key: trust.server_public_key,
                    authorization_key: None,
                },
                vec![],
                ConnectionConfig::default(),
                Default::default(),
            )
            .await
            .unwrap();
            let before = calls.load(Ordering::SeqCst);
            assert_eq!(
                client
                    .conn
                    .call("/tinkle.example.Echo/Call", vec![], 1_000)
                    .await
                    .unwrap(),
                b"ok"
            );
            assert_eq!(calls.load(Ordering::SeqCst), before + 1);
            assert_eq!(
                client
                    .conn
                    .call("/test.Echo/Call", vec![], 1_000)
                    .await
                    .unwrap(),
                b"ok"
            );
            let before = calls.load(Ordering::SeqCst);
            let error = client
                .conn
                .call("/tinkle.v1.MessagesService/SendMessage", vec![], 1_000)
                .await
                .unwrap_err();
            assert_eq!(error.code(), syntra_core::ErrorCode::FailedPrecondition);
            assert_eq!(calls.load(Ordering::SeqCst), before);
            let init = tinkle_schema::proto::InitConnectRequest {
                client: Some(tinkle_schema::proto::ClientInfo {
                    device_model: "fixture-device".into(),
                    platform: "test".into(),
                    os_version: "1".into(),
                    app_version: "1".into(),
                    app_build: "0001".into(),
                    ..Default::default()
                }),
                method: "/tinkle.v1.MessagesService/SendMessage".into(),
                payload: Default::default(),
            };
            let response = client
                .conn
                .call(
                    "/tinkle.v1.ConnectionService/InitConnect",
                    init.encode_to_vec(),
                    1_000,
                )
                .await
                .unwrap();
            assert_eq!(
                tinkle_schema::proto::InitConnectResponse::decode(response.as_slice())
                    .unwrap()
                    .payload
                    .as_ref(),
                b"ok"
            );
            assert_eq!(
                client
                    .conn
                    .call("/tinkle.v1.MessagesService/SendMessage", vec![], 1_000)
                    .await
                    .unwrap(),
                b"ok"
            );
            client.close().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 8);
        servers.join_next().await.unwrap().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
/// Keep healthy traffic on the primary route and block dialing while suspended.
async fn healthy_primary_never_opens_fallback_and_suspend_blocks_dial() {
    let primary = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = support::config(primary.local_addr().unwrap());
    config.fallback_port = Some(fallback.local_addr().unwrap().port());
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = primary.accept().await.unwrap();
            tokio::spawn(async move {
                keep(
                    support::accept(stream, |_, _| async { echo() })
                        .await
                        .unwrap(),
                )
                .await;
            });
        }
    });
    let client = Client::new(config, support::source(), InMemoryCursorStore::new());
    client.connect().await.unwrap();
    let first = client.connection_id().await.unwrap();
    client.suspend().await;
    assert!(!client.is_connected().await);
    assert!(client.connect().await.is_err());
    client.resume().await;
    client.reconnect().await.unwrap();
    assert_ne!(client.connection_id().await.unwrap(), first);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), fallback.accept())
            .await
            .is_err()
    );
    client.stop().await;
    assert!(!client.is_connected().await);
    server.abort();
}

#[tokio::test]
/// Trust or admission-mode rejection must neither dispatch an RPC nor probe fallback.
async fn wrong_pin_scope_key_id_and_mode_never_fall_back_or_dispatch() {
    for case in 0..4 {
        let primary = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = support::config(primary.local_addr().unwrap());
        config.fallback_port = Some(fallback.local_addr().unwrap().port());
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = primary.accept().await.unwrap();
            let result = support::accept(stream, move |_, _| {
                count.fetch_add(1, Ordering::SeqCst);
                async { echo() }
            })
            .await;
            assert!(result.is_err());
        });
        let mut trust = support::trust();
        match case {
            0 => trust.server_public_key = secure_channel::generate_server_key().unwrap().1,
            1 => trust.scope.push_str("-wrong"),
            2 => trust.server_key_id += 1,
            _ => {}
        }
        let source = if case == 3 {
            TrustSource::Returning {
                trust,
                enrollment: active_enrollment([9; 32], [8; 8], 1).unwrap(),
            }
        } else {
            TrustSource::Bootstrap(trust)
        };
        let client = Client::new(config, source, InMemoryCursorStore::new());
        assert!(client.connect().await.is_err());
        assert!(!client.is_connected().await);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), fallback.accept())
                .await
                .is_err()
        );
        server.await.unwrap();
    }
}

struct EnrollmentStore;
impl KeyStore for EnrollmentStore {
    /// Supply the fixture key only for its exact scope, handle, and enrollment epoch.
    fn lookup(&self, context: &Context) -> Result<Secret32, secure_channel::Error> {
        if context.scope != b"tinkle-fixture" || context.handle != [8; 8] || context.epoch != 9 {
            return Err(secure_channel::Error::Authentication);
        }
        Ok(Secret32::new([6; 32]))
    }
}

#[tokio::test]
/// Accept only the matching Returning key and tuple, with no bearer or alternate route.
async fn returning_enrollment_proves_key_and_tuple_without_bearer() {
    for case in 0..4 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fallback = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = support::config(listener.local_addr().unwrap());
        config.fallback_port = Some(fallback.local_addr().unwrap().port());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut security = support::server_security();
            security.mode = Mode::Returning;
            security.store = Some(Arc::new(EnrollmentStore));
            let result = syntra_net::tcp::accept_stream(
                stream,
                &security,
                support::factory(),
                Arc::new(FnHandler(|_, payload, _| async {
                    HandlerOutcome::Payload(payload)
                })),
                ConnectionConfig::default(),
                Default::default(),
            )
            .await;
            match result {
                Ok(handle) => keep(handle).await,
                Err(_) => assert_ne!(case, 0),
            }
        });
        let enrollment = active_enrollment(
            if case == 1 { [7; 32] } else { [6; 32] },
            if case == 2 { [7; 8] } else { [8; 8] },
            if case == 3 { 10 } else { 9 },
        )
        .unwrap();
        let client = Client::new(
            config,
            TrustSource::Returning {
                trust: support::trust(),
                enrollment,
            },
            InMemoryCursorStore::new(),
        );
        if case == 0 {
            client.connect().await.unwrap();
            assert_eq!(
                client
                    .call_raw("/test.Echo/Call", vec![1, 2, 3], 1000)
                    .await
                    .unwrap(),
                [1, 2, 3]
            );
            client.close().await;
        } else {
            assert!(client.connect().await.is_err());
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(30), fallback.accept())
                .await
                .is_err()
        );
        server.await.unwrap();
    }
}

#[tokio::test]
/// Replacing raw trust retires Bootstrap transport without inheriting account or watch state.
async fn raw_enrollment_replacement_retires_bootstrap_and_withdraws_application_scope() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Client::new(
        support::config(listener.local_addr().unwrap()),
        support::source(),
        InMemoryCursorStore::new(),
    );
    let server = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        for mode in [Mode::Bootstrap, Mode::Returning] {
            let (stream, _) = listener.accept().await.unwrap();
            tasks.spawn(async move {
                let mut security = support::server_security();
                security.mode = mode;
                security.store = (mode == Mode::Returning)
                    .then(|| Arc::new(EnrollmentStore) as Arc<dyn KeyStore>);
                let handle = syntra_net::tcp::accept_stream(
                    stream,
                    &security,
                    support::factory(),
                    Arc::new(FnHandler(|_, payload, _| async {
                        HandlerOutcome::Payload(payload)
                    })),
                    ConnectionConfig::default(),
                    Default::default(),
                )
                .await
                .unwrap();
                assert_eq!(handle.identity.context().mode, mode);
                keep(handle).await;
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    });
    client.set_principal_user_id(42).await;
    client.set_watch(vec![7]).await.unwrap();
    assert_eq!(client.watch_list().await, vec![7]);
    client.connect().await.unwrap();
    let first = client.connection_id().await.unwrap();
    client
        .replace_trust_source(TrustSource::Returning {
            trust: support::trust(),
            enrollment: active_enrollment([6; 32], [8; 8], 9).unwrap(),
        })
        .await
        .unwrap();
    // Raw credentials do not attest an application user or inherit its watch set.
    assert!(!client.is_connected().await);
    assert_eq!(client.principal().await, "unknown");
    assert!(client.watch_list().await.is_empty());
    assert!(client.tracked_targets().await.is_empty());
    client.connect().await.unwrap();
    assert_ne!(client.connection_id().await.unwrap(), first);
    assert_eq!(client.principal().await, "unknown");
    assert!(client.watch_list().await.is_empty());
    assert!(matches!(
        client.reconcile_target("target:room:7").await,
        Err(TinkleError::Subscription(_))
    ));
    assert_eq!(
        client
            .call_raw("/test.Echo/Call", vec![4], 1000)
            .await
            .unwrap(),
        [4]
    );
    client.close().await;
    server.await.unwrap();
}

#[tokio::test]
/// A rejected server pin must park automatic reconnect rather than retry on later ticks.
async fn supervisor_parks_after_trust_rejection_instead_of_retrying_on_ticks() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let attempts = count.clone();
    let client = Client::new(
        support::config(listener.local_addr().unwrap()),
        support::source(),
        InMemoryCursorStore::new(),
    );
    let (kill, killed) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        attempts.fetch_add(1, Ordering::SeqCst);
        let handle = support::accept(stream, |_, _| async { echo() })
            .await
            .unwrap();
        killed.await.unwrap();
        handle.close().await;
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            attempts.fetch_add(1, Ordering::SeqCst);
            let (private_key, _) = secure_channel::generate_server_key().unwrap();
            let mut security = support::server_security();
            security.private_key = private_key;
            let _ = syntra_net::tcp::accept_stream(
                stream,
                &security,
                support::factory(),
                Arc::new(FnHandler(|_, payload, _| async {
                    HandlerOutcome::Payload(payload)
                })),
                ConnectionConfig::default(),
                Default::default(),
            )
            .await;
        }
    });
    client.set_principal_user_id(42).await;
    client.start(vec![]).await.unwrap();
    kill.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while count.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert!(!client.is_connected().await);
    client.stop().await;
    server.abort();
}

#[tokio::test]
/// Once TCP is selected, handshake EOF must not trigger an alternate-port attempt.
async fn connected_peer_eof_during_handshake_never_uses_another_port() {
    let primary = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = support::config(primary.local_addr().unwrap());
    config.fallback_port = Some(fallback.local_addr().unwrap().port());
    let server = tokio::spawn(async move {
        drop(primary.accept().await.unwrap());
    });
    let client = Client::new(config, support::source(), InMemoryCursorStore::new());
    assert!(matches!(
        client.connect().await,
        Err(TinkleError::EstablishmentFailed)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), fallback.accept())
            .await
            .is_err()
    );
    server.await.unwrap();
}

#[tokio::test]
/// Losing an established route cannot authorize fallback until trust is explicitly replaced.
async fn opaque_active_loss_cannot_authorize_an_alternate_port() {
    let primary = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = support::config(primary.local_addr().unwrap());
    config.fallback_port = Some(fallback.local_addr().unwrap().port());
    let (kill, killed) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = primary.accept().await.unwrap();
        let handle = support::accept(stream, |_, _| async { echo() })
            .await
            .unwrap();
        killed.await.unwrap();
        handle.close().await;
        drop(primary);
    });
    let client = Client::new(config, support::source(), InMemoryCursorStore::new());
    client.connect().await.unwrap();
    kill.send(()).unwrap();
    server.await.unwrap();
    assert!(matches!(
        client.reconnect().await,
        Err(TinkleError::NetworkUnavailable)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), fallback.accept())
            .await
            .is_err()
    );

    // Only explicit source replacement authorizes fresh route selection.
    client
        .replace_trust_source(support::source())
        .await
        .unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = fallback.accept().await.unwrap();
        keep(
            support::accept(stream, |_, _| async { echo() })
                .await
                .unwrap(),
        )
        .await;
    });
    client.connect().await.unwrap();
    assert_eq!(
        client
            .call_raw("/test.Echo/Call", vec![], 1000)
            .await
            .unwrap(),
        b"ok"
    );
    client.close().await;
    server.await.unwrap();
}
