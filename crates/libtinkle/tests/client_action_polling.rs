//! Client supervisor scheduling over encrypted TCP, with independently held journal and action RPCs.
#![cfg(all(not(target_arch = "wasm32"), tinkle_async_actions))]

// Returning fixture uses shared trust/config helpers, not Bootstrap accept helpers.
#[allow(dead_code)]
mod support;

use libtinkle::{CancelToken, Client, OutgoingRequest, OutgoingState, SendRetry, Storage};
use prost::Message;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use syntra_core::secure_channel::{self, Mode, Secret32};
use syntra_runtime::{ConnectionConfig, FnHandler, HandlerOutcome};
use tinkle_schema::proto as p;

/// A nonzero signed key that cannot be normalized to a positive ID.
const ORIGINAL_KEY: i64 = i64::MIN;

/// Fixed synthetic Returning authority; no endpoint-derived trust enters the client.
struct Keys;

impl secure_channel::KeyStore for Keys {
    /// Accept only the enrollment tuple provisioned by this fixture.
    fn lookup(&self, context: &secure_channel::Context) -> Result<Secret32, secure_channel::Error> {
        if context.epoch != 1 || context.handle != [8; 8] {
            return Err(secure_channel::Error::Authentication);
        }
        Ok(Secret32::new([9; 32]))
    }
}

/// One stable conversation for both the journal and original send selector.
fn target() -> p::Target {
    p::Target {
        kind: Some(p::target::Kind::ChatId(7)),
    }
}

/// Pending submission and successful query differ only in authoritative lifecycle evidence.
fn status(state: i32) -> p::SendStatus {
    p::SendStatus {
        target: Some(target()),
        idempotency_key: ORIGINAL_KEY,
        state,
        revision: state as u64,
        accepted: if state == 3 {
            vec![p::SendReceipt {
                idempotency_key: ORIGINAL_KEY,
                message_id: Some(10),
                reference_withheld: Some(false),
                ..Default::default()
            }]
        } else {
            vec![]
        },
        retain_until: (state == 3).then_some(prost_types::Timestamp {
            seconds: 2_000_000_000,
            nanos: 0,
        }),
        ..Default::default()
    }
}

/// Gates remain held until after durable status assertions, not merely until a query arrives.
#[derive(Default)]
struct Gates {
    hold_journal: AtomicBool,
    journal_entered: CancelToken,
    journal_release: CancelToken,
    query_release: CancelToken,
    hydration_entered: CancelToken,
    hydration_release: CancelToken,
    send_committed: CancelToken,
    queries: AtomicUsize,
    submits: AtomicUsize,
    journal_runs: AtomicUsize,
}

/// Exercise the actual supervisor, durable action query, and status-before-hydration path.
async fn held_journal_does_not_stall_actions(timeout_first_query: bool, lose_reply: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let gates = Arc::new(Gates::default());
    let remote = gates.clone();
    let (host_tx, mut host_rx) = tokio::sync::mpsc::channel(2);
    let server = tokio::spawn(async move {
        let mut security = support::server_security();
        security.mode = Mode::Returning;
        security.store = Some(Arc::new(Keys));
        let handler_gates = remote.clone();
        let handler = Arc::new(FnHandler(
            move |method: String, payload: Vec<u8>, cancel: CancelToken| {
                let gates = handler_gates.clone();
                async move {
                    let wrapped = method.ends_with("/InitConnect");
                    let (method, payload) = if wrapped {
                        let request = p::InitConnectRequest::decode(payload.as_slice()).unwrap();
                        (request.method, request.payload.to_vec())
                    } else {
                        (method, payload)
                    };
                    let bytes = match method.rsplit('/').next().unwrap() {
                        "GetSessions" => p::GetSessionsResponse::default().encode_to_vec(),
                        "KeepAlive" => p::KeepAliveResponse {
                            sequence: p::KeepAliveRequest::decode(payload.as_slice())
                                .unwrap()
                                .sequence,
                        }
                        .encode_to_vec(),
                        "SendMedia" => {
                            let request = p::SendMediaRequest::decode(payload.as_slice()).unwrap();
                            assert_eq!(request.target, Some(target()));
                            assert_eq!(request.idempotency_key, ORIGINAL_KEY);
                            gates.submits.fetch_add(1, Ordering::SeqCst);
                            if lose_reply {
                                // The fixture records the effect, but never offers a terminal result
                                // until transport shutdown cancels the handler.
                                gates.send_committed.cancel();
                                cancel.cancelled().await;
                            }
                            p::SendMediaResponse {
                                send_status: Some(status(1)),
                                ..Default::default()
                            }
                            .encode_to_vec()
                        }
                        "GetSendStatus" => {
                            let request =
                                p::GetSendStatusRequest::decode(payload.as_slice()).unwrap();
                            assert_eq!(request.target, Some(target()));
                            assert_eq!(request.idempotency_key, ORIGINAL_KEY);
                            assert_eq!(gates.submits.load(Ordering::SeqCst), 1);
                            assert!(!lose_reply || gates.send_committed.is_cancelled());
                            // Do not let the initial interval tick settle the action before recovery holds.
                            gates.journal_entered.cancelled().await;
                            let count = gates.queries.fetch_add(1, Ordering::SeqCst);
                            if timeout_first_query && count == 0 {
                                gates.query_release.cancelled().await;
                            }
                            p::GetSendStatusResponse {
                                send_status: Some(status(3)),
                                ..Default::default()
                            }
                            .encode_to_vec()
                        }
                        "ListMessageChanges" => {
                            let request =
                                p::ListMessageChangesRequest::decode(payload.as_slice()).unwrap();
                            let held = gates.hold_journal.load(Ordering::SeqCst)
                                && request.after_accept_seq == 0;
                            if held {
                                gates.journal_runs.fetch_add(1, Ordering::SeqCst);
                                gates.journal_entered.cancel();
                                gates.journal_release.cancelled().await;
                            }
                            p::ListMessageChangesResponse {
                                changes: if held {
                                    vec![p::TargetChange {
                                        message_id: 90,
                                        accept_seq: 1,
                                        kind: p::TargetChangeKind::Deleted as i32,
                                        ..Default::default()
                                    }]
                                } else {
                                    vec![]
                                },
                                last_accept_seq: if held { 1 } else { request.after_accept_seq },
                                more_available: Some(false),
                            }
                            .encode_to_vec()
                        }
                        "GetMessages" => {
                            let req = p::GetMessagesRequest::decode(payload.as_slice()).unwrap();
                            assert_eq!(req.target, Some(target()));
                            assert_eq!(req.message_ids, [10]);
                            gates.hydration_entered.cancel();
                            gates.hydration_release.cancelled().await;
                            p::GetMessagesResponse {
                                messages: vec![p::Message {
                                    message_id: 10,
                                    target: Some(target()),
                                    from_id: 42,
                                    accept_seq: Some(1),
                                    body_revision: Some(1),
                                    message: "delayed body".into(),
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }
                            .encode_to_vec()
                        }
                        "GetHistory" => p::GetHistoryResponse {
                            more_available: Some(false),
                            ..Default::default()
                        }
                        .encode_to_vec(),
                        other => panic!("unexpected fixture method {other}"),
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
            },
        ));
        for connection in 0..if lose_reply { 2 } else { 1 } {
            let (stream, _) = listener.accept().await.unwrap();
            let handle = syntra_net::tcp::accept_stream(
                stream,
                &security,
                support::factory(),
                handler.clone(),
                ConnectionConfig::default(),
                Default::default(),
            )
            .await
            .unwrap();
            assert!(host_tx.send(handle.host.clone()).await.is_ok());
            if lose_reply && connection == 0 {
                remote.send_committed.cancelled().await;
            } else {
                while !handle.is_closed() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
            handle.close().await;
        }
    });
    let enrollment = p::DeviceEnrollmentStatus {
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
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let open_client = || {
        let storage = Arc::new(Storage::open(path.to_str().unwrap()).unwrap());
        let client = Client::new_sqlite(
            config.clone(),
            libtinkle::TrustSource::Returning {
                trust: support::trust(),
                enrollment: libtinkle::Enrollment::from_active_status([9; 32], &enrollment)
                    .unwrap(),
            },
            storage.clone(),
        );
        (client, storage)
    };
    let (mut client, mut storage) = open_client();
    client.set_principal_user_id(42).await;
    client.connect().await.unwrap();
    let mut host = host_rx.recv().await.unwrap();
    let prepared = client
        .sending()
        .enqueue(
            "send".into(),
            OutgoingRequest::Media(p::SendMediaRequest {
                target: Some(target()),
                idempotency_key: ORIGINAL_KEY,
                media: Some(p::MediaSource {
                    media: Some(p::media_source::Media::Photo(p::PhotoSource {
                        file_id: 9,
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let pending = client
        .sending()
        .resume("send".into(), SendRetry::Never, None)
        .await;
    if !lose_reply {
        assert_eq!(pending.unwrap().state, OutgoingState::ActionPending);
    } else {
        assert!(pending.is_err());
        let job = client.sending().get("send".into()).await.unwrap();
        assert!(matches!(
            job.state,
            OutgoingState::ActionUnconfirmed | OutgoingState::ActionDispatching
        ));
        if job.state == OutgoingState::ActionUnconfirmed {
            assert!(job.last_outcome.as_ref().unwrap().unknown_outcome);
        }
        assert!(
            job.response.is_empty(),
            "no authoritative command reply was received"
        );
        client.close().await;
        drop(client);
        drop(storage);
        (client, storage) = open_client();
        client.set_principal_user_id(42).await;
        assert_eq!(client.sending().get("send".into()).await.unwrap(), job);
    }
    client.start(vec![target()]).await.unwrap();
    if lose_reply {
        host = tokio::time::timeout(Duration::from_secs(2), host_rx.recv())
            .await
            .unwrap()
            .unwrap();
    }
    gates.hold_journal.store(true, Ordering::SeqCst);
    host.notify(vec![1]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), gates.journal_entered.cancelled())
        .await
        .unwrap();

    // Both assertions must finish while the journal RPC is still held (its budget is 30 seconds).
    let persisted = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let job = client.sending().get("send".into()).await.unwrap();
            if job.state == OutgoingState::ActionSucceeded {
                break job;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    tokio::time::timeout(Duration::from_secs(6), gates.hydration_entered.cancelled())
        .await
        .expect("success hydration starts after the outcome commits");
    let held_cursor = client.target_cursor("target:room:7").await;
    let held_status = client.target_status("target:room:7").await;
    let held_runs = gates.journal_runs.load(Ordering::SeqCst);

    // Release all remote handlers even if an assertion below fails; timeout is not remote rollback.
    gates.query_release.cancel();
    gates.hydration_release.cancel();
    gates.journal_release.cancel();
    let reconciled = tokio::time::timeout(Duration::from_secs(2), async {
        while client.target_cursor("target:room:7").await != Some(1)
            || client.target_status("target:room:7").await != Some(libtinkle::ReconcileStatus::Idle)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let hydrated = tokio::time::timeout(Duration::from_secs(2), async {
        while !storage
            .send_hydration_due("user:42", &prepared.source)
            .unwrap()
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    client.close().await;
    server.await.unwrap();

    let persisted =
        persisted.expect("action status must commit before releasing target reconciliation");
    assert_eq!(persisted.payload, prepared.payload);
    assert_eq!(
        p::SendMediaResponse::decode(persisted.response.as_slice())
            .unwrap()
            .send_status,
        Some(status(3))
    );
    assert_eq!(
        held_cursor.unwrap_or(0),
        0,
        "action queries cannot advance journals"
    );
    assert_eq!(held_runs, 1, "recovery must stay single-flight");
    assert_eq!(held_status, Some(libtinkle::ReconcileStatus::Reconciling));
    assert!(
        reconciled.is_ok(),
        "the original reconcile must finish after release"
    );
    assert!(
        hydrated.is_ok(),
        "body hydration must complete after release"
    );
    assert_eq!(
        storage.load_journal("user:42", "target:room:7").unwrap(),
        Some(1)
    );
    let bodies = storage
        .get_messages("user:42", "target:room:7", 10, None)
        .unwrap();
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0].message_id, 10);
    assert_eq!(bodies[0].body, "delayed body");
    assert_eq!(
        gates.submits.load(Ordering::SeqCst),
        1,
        "never replay the mutation"
    );
    assert_eq!(
        gates.queries.load(Ordering::SeqCst),
        if timeout_first_query { 2 } else { 1 }
    );
}

/// A pending action's successful query commits while target reconciliation and body hydration await I/O.
#[tokio::test]
async fn client_persists_action_status_while_target_reconcile_is_held() {
    held_journal_does_not_stall_actions(false, false).await;
}

/// A held first query times out and the next periodic query commits before the target is released.
#[tokio::test]
async fn client_action_query_timeout_progresses_while_target_reconcile_is_held() {
    held_journal_does_not_stall_actions(true, false).await;
}

/// After reopening SQLite, startup finds the original send by target/key without submitting again.
#[tokio::test]
async fn lost_send_reply_recovers_original_key_after_restart_without_resubmission() {
    held_journal_does_not_stall_actions(false, true).await;
}
