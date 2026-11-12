//! Encrypted native races: delayed history cannot recreate removed or replacement-scope bodies.
mod support;
use libtinkle::{Client, Storage};
use prost::Message as _;
use std::sync::Arc;
use tinkle_schema::proto as p;

/// Exercise deletion, rebuild/valid-snapshot replacement, and a durable external scope wipe.
#[tokio::test]
async fn delayed_history_is_fenced_before_fetch_and_cannot_resurrect_removed_bodies() {
    for mode in ["delete", "rebuild", "wipe"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = support::config(listener.local_addr().unwrap());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let ready = entered.clone();
        let gate = release.clone();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let handle = support::accept(stream, move |request, _| {
                let ready = ready.clone();
                let gate = gate.clone();
                let count = count.clone();
                async move {
                    assert!(request.method.ends_with("/GetHistory"));
                    let req = p::GetHistoryRequest::decode(request.payload.as_slice()).unwrap();
                    let first = count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
                    let message = p::Message {
                        message_id: if first { 1 } else { 2 },
                        target: req.target,
                        from_id: 42,
                        message: if first {
                            "retiredneedle"
                        } else {
                            "freshneedle"
                        }
                        .into(),
                        accept_seq: Some(if first { 100 } else { 1 }),
                        body_revision: Some(1),
                        ..Default::default()
                    };
                    if first {
                        ready.notify_one();
                        gate.acquire().await.unwrap().forget();
                    }
                    let payload = p::GetHistoryResponse {
                        messages: vec![message],
                        more_available: Some(false),
                        ..Default::default()
                    }
                    .encode_to_vec();
                    syntra_core::proto::Frame {
                        version: 1,
                        request_id: 1,
                        body: Some(syntra_core::proto::frame::Body::Result(
                            syntra_core::proto::RpcResult {
                                outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(
                                    payload,
                                )),
                            },
                        )),
                    }
                }
            })
            .await
            .unwrap();
            while !handle.is_closed() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            handle.close().await;
        });
        let storage = Arc::new(Storage::open_in_memory().unwrap());
        let client = Client::new_sqlite(config, support::source(), storage.clone());
        client.set_principal_user_id(42).await;
        client.connect().await.unwrap();
        let req = p::GetHistoryRequest {
            target: Some(p::Target {
                kind: Some(p::target::Kind::ChatId(7)),
            }),
            limit: 20,
            ..Default::default()
        };
        let caller = client.clone();
        let task = tokio::spawn(async move { caller.load_history(req).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        match mode {
            "delete" => storage
                .apply_journal_page_fenced(
                    "user:42",
                    "target:room:7",
                    &tinkle_storage::CacheOps {
                        remove_messages: vec![("target:room:7".into(), 1)],
                        ..Default::default()
                    },
                    1,
                    0,
                )
                .unwrap(),
            "rebuild" => {
                storage.mark_rebuild("user:42", "target:room:7").unwrap();
                storage
                    .install_snapshot("user:42", "target:room:7", 0, 0, None)
                    .unwrap();
            }
            "wipe" => {
                storage.clear_principal("user:42").unwrap();
                storage
                    .install_snapshot("user:42", "target:room:7", 0, 0, None)
                    .unwrap();
            }
            _ => unreachable!(),
        }
        release.add_permits(1);
        let result = task.await.unwrap();
        if mode == "delete" {
            assert!(result.unwrap().messages.is_empty());
        } else {
            assert!(matches!(result, Err(libtinkle::TinkleError::Sync(_))));
        }
        assert!(
            storage
                .get_messages("user:42", "target:room:7", 10, None)
                .unwrap()
                .is_empty(),
            "delayed {mode} response recreated a body"
        );
        assert!(storage
            .search_messages(
                "user:42",
                &tinkle_storage::MessageSearchRequest {
                    query: "retiredneedle".into(),
                    ..Default::default()
                }
            )
            .unwrap()
            .hits
            .is_empty());
        // A genuinely new authorized read/ID must not be suppressed by an unrelated removal.
        client.load_history(req).await.unwrap();
        assert_eq!(
            storage
                .search_messages(
                    "user:42",
                    &tinkle_storage::MessageSearchRequest {
                        query: "freshneedle".into(),
                        ..Default::default()
                    }
                )
                .unwrap()
                .hits
                .len(),
            1,
            "fresh {mode} history was lost"
        );
        client.close().await;
        server.await.unwrap();
    }
}

/// Retained accepted album outcomes are not network replays and cannot recreate removed
/// bodies in the offline index, in either ordering of the two unrelated sequence domains.
#[tokio::test]
async fn accepted_album_rehydration_keeps_removals_out_of_offline_fts_without_rpc_replay() {
    for (acceptance, removal) in [(100, 1), (1, 100)] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = support::config(listener.local_addr().unwrap());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let remote = calls.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let handle = support::accept(stream, move |request, _| {
                let calls = remote.clone();
                async move {
                    assert!(request.method.ends_with("/SendAlbum"));
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let req = p::SendAlbumRequest::decode(request.payload.as_slice()).unwrap();
                    let messages = req
                        .items
                        .into_iter()
                        .enumerate()
                        .map(|(i, item)| p::Message {
                            message_id: i as i64 + 1,
                            target: req.target,
                            from_id: 42,
                            message: item.message,
                            album_id: Some(7),
                            media: Some(p::Media {
                                media: Some(p::media::Media::Photo(p::Photo {
                                    file_id: 1,
                                    ..Default::default()
                                })),
                            }),
                            accept_seq: Some(acceptance + i as i64),
                            body_revision: Some(1),
                            ..Default::default()
                        })
                        .collect();
                    let payload = p::SendAlbumResponse {
                        messages,
                        ..Default::default()
                    }
                    .encode_to_vec();
                    syntra_core::proto::Frame {
                        version: 1,
                        request_id: 1,
                        body: Some(syntra_core::proto::frame::Body::Result(
                            syntra_core::proto::RpcResult {
                                outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(
                                    payload,
                                )),
                            },
                        )),
                    }
                }
            })
            .await
            .unwrap();
            while !handle.is_closed() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            handle.close().await;
        });
        let storage = Arc::new(Storage::open_in_memory().unwrap());
        let client = Client::new_sqlite(config, support::source(), storage.clone());
        client.set_principal_user_id(42).await;
        client.connect().await.unwrap();
        let target = Some(p::Target {
            kind: Some(p::target::Kind::ChatId(7)),
        });
        let items = ["retiredneedle", "survivorneedle"]
            .into_iter()
            .map(|text| p::AlbumItem {
                message: text.into(),
                media: Some(p::MediaSource {
                    media: Some(p::media_source::Media::Photo(p::PhotoSource {
                        file_id: 1,
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            })
            .collect();
        client
            .sending()
            .enqueue(
                "album".into(),
                libtinkle::OutgoingRequest::Album(p::SendAlbumRequest {
                    target,
                    items,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        client
            .sending()
            .resume("album".into(), libtinkle::SendRetry::Never, None)
            .await
            .unwrap();
        assert_eq!(
            storage
                .search_messages(
                    "user:42",
                    &tinkle_storage::MessageSearchRequest {
                        query: "retiredneedle".into(),
                        ..Default::default()
                    }
                )
                .unwrap()
                .hits
                .len(),
            1
        );
        storage
            .apply_journal_page_fenced(
                "user:42",
                "target:room:7",
                &tinkle_storage::CacheOps {
                    remove_messages: vec![("target:room:7".into(), 1)],
                    ..Default::default()
                },
                removal,
                0,
            )
            .unwrap();
        client
            .sending()
            .resume(
                "album".into(),
                libtinkle::SendRetry::ExactKeyedRequest,
                None,
            )
            .await
            .unwrap();
        assert!(storage
            .search_messages(
                "user:42",
                &tinkle_storage::MessageSearchRequest {
                    query: "retiredneedle".into(),
                    ..Default::default()
                }
            )
            .unwrap()
            .hits
            .is_empty());
        assert_eq!(
            storage
                .search_messages(
                    "user:42",
                    &tinkle_storage::MessageSearchRequest {
                        query: "survivorneedle".into(),
                        ..Default::default()
                    }
                )
                .unwrap()
                .hits
                .len(),
            1
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            storage.load_journal("user:42", "target:room:7").unwrap(),
            Some(removal)
        );
        client.close().await;
        server.await.unwrap();
    }
}

/// Capture only client diagnostics, so assertions verify the actual asynchronous error paths.
#[derive(Clone)]
struct ClientLogs(Arc<std::sync::Mutex<String>>);
impl tracing::Subscriber for ClientLogs {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "libtinkle::client"
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields<'a>(&'a mut String);
        impl tracing::field::Visit for Fields<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write as _;
                let _ = write!(self.0, "{field}={value:?};");
            }
        }
        event.record(&mut Fields(&mut self.0.lock().unwrap()));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// Untrusted GetMessages diagnostics never enter either client hydration error log.
#[tokio::test]
async fn get_messages_error_logs_emit_codes_not_remote_diagnostics() {
    use tracing::instrument::WithSubscriber as _;
    const REMOTE: &str = "UNTRUSTED_REMOTE_DIAGNOSTIC_SENTINEL";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let target = p::Target {
        kind: Some(p::target::Kind::ChatId(7)),
    };
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |request, _| async move {
            let outcome = match request.method.rsplit('/').next().unwrap() {
                "ListMessageChanges" => syntra_core::proto::rpc_result::Outcome::Payload(
                    p::ListMessageChangesResponse {
                        changes: vec![p::TargetChange {
                            message_id: 1,
                            message: Some(p::Message {
                                message_id: 1,
                                target: Some(target),
                                from_id: 42,
                                message: "body".into(),
                                accept_seq: Some(1),
                                ..Default::default()
                            }),
                            kind: p::TargetChangeKind::Edited as i32,
                            accept_seq: 1,
                            ..Default::default()
                        }],
                        last_accept_seq: 1,
                        more_available: Some(false),
                    }
                    .encode_to_vec(),
                ),
                "GetMessages" => {
                    syntra_core::proto::rpc_result::Outcome::Error(syntra_core::proto::RpcError {
                        code: 9,
                        message: REMOTE.into(),
                    })
                }
                "GetHistory" => syntra_core::proto::rpc_result::Outcome::Payload(
                    p::GetHistoryResponse {
                        messages: vec![p::Message {
                            message_id: 2,
                            target: Some(target),
                            from_id: 42,
                            message: "fresh".into(),
                            accept_seq: Some(2),
                            ..Default::default()
                        }],
                        more_available: Some(false),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                ),
                other => panic!("unexpected method {other}"),
            };
            syntra_core::proto::Frame {
                version: 1,
                request_id: 1,
                body: Some(syntra_core::proto::frame::Body::Result(
                    syntra_core::proto::RpcResult {
                        outcome: Some(outcome),
                    },
                )),
            }
        })
        .await
        .unwrap();
        while !handle.is_closed() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        handle.close().await;
    });
    let storage = Arc::new(Storage::open_in_memory().unwrap());
    let client = Client::new_sqlite(config, support::source(), storage);
    client.set_principal_user_id(42).await;
    client.connect().await.unwrap();
    let log = Arc::new(std::sync::Mutex::new(String::new()));
    client
        .subscribe(vec![target])
        .with_subscriber(ClientLogs(log.clone()))
        .await
        .unwrap();
    let captured = log.lock().unwrap().clone();
    assert!(captured.contains("edited hydration failed"), "{captured}");
    assert!(
        captured.contains("new-message catch-up fetch failed"),
        "{captured}"
    );
    assert!(captured.contains("code="));
    assert!(!captured.contains(REMOTE));
    client.close().await;
    server.await.unwrap();
}

/// Offline SDK search over a migrated, valid protobuf cache must not disclose empty-origin
/// rows from invalidated conversations, even after a replacement scope becomes valid.
#[tokio::test]
async fn offline_sdk_search_does_not_revive_migrated_global_origin_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let path = path.to_str().unwrap();
    let message = p::Message {
        message_id: 1,
        target: Some(p::Target {
            kind: Some(p::target::Kind::ChatId(7)),
        }),
        from_id: 42,
        message: "retiredneedle".into(),
        accept_seq: Some(100),
        ..Default::default()
    };
    {
        let storage = Storage::open(path).unwrap();
        storage
            .apply_journal_page_fenced(
                "user:42",
                "target:room:7",
                &tinkle_storage::CacheOps {
                    messages: vec![tinkle_storage::MessageRow {
                        conversation_key: "target:room:7".into(),
                        message_id: 1,
                        peer_kind: "room".into(),
                        peer_id: 7,
                        from_id: 42,
                        body: message.message.clone(),
                        proto: message.encode_to_vec(),
                        origin_stream: "target:room:7".into(),
                        accept_seq: Some(100),
                        edit_date_secs: None,
                        edit_date_nanos: None,
                    }],
                    ..Default::default()
                },
                0,
                0,
            )
            .unwrap();
    }
    {
        let connection = rusqlite::Connection::open(path).unwrap();
        connection.execute_batch("UPDATE messages SET origin_stream='';UPDATE stream_state SET valid=0,generation=1;UPDATE meta SET value='4' WHERE key='schema_version';").unwrap();
    }
    let storage = Arc::new(Storage::open(path).unwrap());
    let client = Client::new_sqlite(
        support::config("127.0.0.1:1".parse().unwrap()),
        support::source(),
        storage.clone(),
    );
    client.set_principal_user_id(42).await;
    let request = p::SearchRequest {
        query: "retiredneedle".into(),
        limit: 20,
        ..Default::default()
    };
    assert!(client
        .messages()
        .search(request.clone())
        .await
        .unwrap()
        .messages
        .is_empty());
    storage
        .install_snapshot("user:42", "target:room:7", 0, 0, None)
        .unwrap();
    assert!(client
        .messages()
        .search(request)
        .await
        .unwrap()
        .messages
        .is_empty());
    assert!(!client.is_connected().await);
    client.close().await;
}

/// If one hydration write fails, returning the unfiltered network page would also expose a
/// known removed body from that page. Visibility filtering must fail closed with the transaction.
#[tokio::test]
async fn history_storage_failure_cannot_bypass_a_viewer_tombstone() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, |request, _| async move {
            assert!(request.method.ends_with("/GetHistory"));
            let req = p::GetHistoryRequest::decode(request.payload.as_slice()).unwrap();
            let payload = p::GetHistoryResponse {
                messages: (1..=2)
                    .map(|id| p::Message {
                        message_id: id,
                        target: req.target,
                        from_id: 7,
                        accept_seq: Some(id),
                        message: "must not bypass visibility".into(),
                        ..Default::default()
                    })
                    .collect(),
                more_available: Some(false),
                ..Default::default()
            }
            .encode_to_vec();
            syntra_core::proto::Frame {
                version: 1,
                request_id: 1,
                body: Some(syntra_core::proto::frame::Body::Result(
                    syntra_core::proto::RpcResult {
                        outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(payload)),
                    },
                )),
            }
        })
        .await
        .unwrap();
        while !handle.is_closed() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        handle.close().await;
    });
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let storage = Arc::new(Storage::open(path.to_str().unwrap()).unwrap());
    storage
        .apply_journal_page_fenced(
            "user:42",
            "target:room:7",
            &tinkle_storage::CacheOps {
                remove_messages: vec![("target:room:7".into(), 1)],
                ..Default::default()
            },
            1,
            0,
        )
        .unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_hydration BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT,'fixture write failure'); END;").unwrap();
    let client = Client::new_sqlite(config, support::source(), storage.clone());
    client.set_principal_user_id(42).await;
    client.connect().await.unwrap();
    let result = client
        .load_history(p::GetHistoryRequest {
            target: Some(p::Target {
                kind: Some(p::target::Kind::ChatId(7)),
            }),
            limit: 10,
            ..Default::default()
        })
        .await;
    assert!(
        matches!(result, Err(libtinkle::TinkleError::CursorStore(_))),
        "{result:?}"
    );
    assert!(storage
        .get_messages("user:42", "target:room:7", 10, None)
        .unwrap()
        .is_empty());
    client.close().await;
    server.await.unwrap();
}
