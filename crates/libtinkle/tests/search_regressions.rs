//! Shared Search behavior over offline SQLite and encrypted native request dispatch.
mod support;
use libtinkle::{Client, Storage};
use prost::Message as _;
use std::sync::{Arc, Mutex};
use tinkle_schema::proto as p;

/// These scenarios deliberately occupy the global four-worker cache admission pool.
static SEARCH_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A target-local source position deliberately differs from numeric message-ID order.
fn message(id: i64, seq: i64) -> p::Message {
    p::Message {
        message_id: id,
        accept_seq: Some(seq),
        body_revision: Some(3),
        from_id: 42,
        target: Some(p::Target {
            kind: Some(p::target::Kind::ChatId(7)),
        }),
        message: "hello world".into(),
        date: Some(prost_types::Timestamp {
            seconds: seq,
            nanos: 0,
        }),
        ..Default::default()
    }
}

/// Commit full cached protobufs through the normal fenced journal transaction.
fn seed(storage: &Storage, messages: Vec<p::Message>) {
    let ops = tinkle_storage::CacheOps {
        messages: messages
            .into_iter()
            .map(|m| tinkle_storage::MessageRow {
                conversation_key: "target:room:7".into(),
                message_id: m.message_id,
                peer_kind: "room".into(),
                peer_id: 7,
                from_id: m.from_id,
                body: m.message.clone(),
                proto: m.encode_to_vec(),
                origin_stream: "target:room:7".into(),
                accept_seq: m.accept_seq,
                edit_date_secs: None,
                edit_date_nanos: None,
            })
            .collect(),
        ..Default::default()
    };
    storage
        .apply_journal_page_fenced("user:42", "target:room:7", &ops, 0, 0)
        .unwrap();
}

/// An offline client has a cache identity, no socket, and no implicit dial on Search.
async fn offline(storage: Arc<Storage>) -> Client {
    let client = Client::new_sqlite(
        support::config("127.0.0.1:1".parse().unwrap()),
        support::source(),
        storage,
    );
    client.set_principal_user_id(42).await;
    client
}

/// Shared protobuf request, not a separate local search model.
fn request() -> p::SearchRequest {
    p::SearchRequest {
        target: message(1, 1).target,
        query: "hello".into(),
        limit: 2,
        ..Default::default()
    }
}

/// Full cached results stay marked partial through paging, empty pages and local exhaustion.
#[tokio::test]
async fn offline_partial_full_bodies_source_order_paging_and_empty() {
    let _serial = SEARCH_TESTS.lock().await;
    let storage = Arc::new(Storage::open_in_memory().unwrap());
    let mut full = message(3, 30);
    full.pinned = true;
    full.entities = vec![p::Entity {
        r#type: p::EntityType::CustomEmoji as i32,
        offset: 0,
        length: 5,
        custom_emoji_id: 99,
        asset_version: Some(7),
        fallback_text: Some("hello".into()),
        ..Default::default()
    }];
    seed(
        &storage,
        vec![message(999, 10), message(100, 20), full.clone()],
    );
    let client = offline(storage.clone()).await;
    let mut req = request();
    let page = client.messages().search(req.clone()).await.unwrap();
    assert_eq!(page.messages, vec![full, message(100, 20)]);
    assert_eq!(page.source, p::SearchSource::Cache as i32);
    assert_eq!(page.coverage, p::SearchCoverage::CachedSubset as i32);
    req.cursor = page.next_cursor;
    assert!(req
        .cursor
        .as_ref()
        .unwrap()
        .starts_with("tinkle-search:v1:"));
    let mut altered = req.clone();
    altered.filter = 10;
    assert!(client.messages().search(altered).await.is_err());
    let other = offline(storage.clone()).await;
    assert!(other.messages().search(req.clone()).await.is_err());
    let page = client.messages().search(req.clone()).await.unwrap();
    assert_eq!(page.messages, vec![message(999, 10)]);
    assert_eq!(page.coverage, p::SearchCoverage::CachedSubset as i32);
    assert_eq!(page.more_available, Some(false));
    let empty = client
        .messages()
        .search(p::SearchRequest {
            query: "absent".into(),
            ..request()
        })
        .await
        .unwrap();
    assert!(empty.messages.is_empty());
    assert_eq!(empty.coverage, p::SearchCoverage::CachedSubset as i32);
    assert!(!client.is_connected().await);
    assert_eq!(
        storage.load_journal("user:42", "target:room:7").unwrap(),
        Some(0)
    );
    client.set_principal_user_id(43).await;
    client.set_principal_user_id(42).await;
    assert!(client.messages().search(req).await.is_err());
}

/// Selective predicates retain continuation at the candidate budget, not fabricated exhaustion.
#[tokio::test]
async fn selective_filters_budget_and_revision_fences() {
    let _serial = SEARCH_TESTS.lock().await;
    let storage = Arc::new(Storage::open_in_memory().unwrap());
    seed(&storage, (1..=140).map(|id| message(id, id)).collect());
    let client = offline(storage.clone()).await;
    let mut req = p::SearchRequest {
        min_date: 1000,
        ..request()
    };
    let page = client.messages().search(req.clone()).await.unwrap();
    assert!(page.messages.is_empty());
    assert_eq!(page.more_available, Some(true));
    req.cursor = page.next_cursor;
    let page = client.messages().search(req.clone()).await.unwrap();
    assert!(page.messages.is_empty());
    assert_eq!(page.more_available, Some(false));
    storage.mark_rebuild("user:42", "target:room:7").unwrap();
    assert!(client.messages().search(req).await.is_err());
    let page = client.messages().search(request()).await.unwrap();
    assert!(page.messages.is_empty());
    assert_eq!(page.coverage, p::SearchCoverage::CachedSubset as i32);
}

/// ID boundaries resolve source order, dates are inclusive, and every shared predicate is applied.
#[tokio::test]
async fn filters_windows_and_full_width_ids() {
    let _serial = SEARCH_TESTS.lock().await;
    let storage = Arc::new(Storage::open_in_memory().unwrap());
    let root = message(i64::from(i32::MAX) + 10, 10);
    let mut m = message(100, 20);
    m.pinned = true;
    m.reply_to = Some(p::MessageReplyHeader {
        thread_root_id: root.message_id,
        ..Default::default()
    });
    m.media = Some(p::Media {
        media: Some(p::media::Media::Photo(p::Photo::default())),
    });
    seed(&storage, vec![root.clone(), m.clone(), message(3, 30)]);
    let client = offline(storage).await;
    let req = p::SearchRequest {
        from_target: Some(p::Target {
            kind: Some(p::target::Kind::UserId(42)),
        }),
        thread_root_id: Some(root.message_id),
        min_date: 20,
        max_date: 20,
        min_id: Some(root.message_id),
        max_id: Some(3),
        filter: 1,
        ..request()
    };
    assert_eq!(
        client
            .messages()
            .search(req.clone())
            .await
            .unwrap()
            .messages,
        vec![m.clone()]
    );
    for filter in [2, 4, 5, 6, 7, 8, 9, 11] {
        assert!(client
            .messages()
            .search(p::SearchRequest {
                filter,
                ..req.clone()
            })
            .await
            .unwrap()
            .messages
            .is_empty());
    }
    assert_eq!(
        client
            .messages()
            .search(p::SearchRequest { filter: 10, ..req })
            .await
            .unwrap()
            .messages,
        vec![m]
    );
    let page = client
        .messages()
        .search(p::SearchRequest {
            offset_message_id: Some(root.message_id),
            add_offset: -1,
            ..request()
        })
        .await
        .unwrap();
    assert_eq!(
        page.messages
            .iter()
            .map(|m| m.message_id)
            .collect::<Vec<_>>(),
        vec![100]
    );
    let page = client
        .messages()
        .search(p::SearchRequest {
            offset_message_id: Some(3),
            add_offset: 1,
            ..request()
        })
        .await
        .unwrap();
    assert_eq!(page.messages, vec![root]);
    assert!(client
        .messages()
        .search(p::SearchRequest {
            offset_message_id: Some(404),
            ..request()
        })
        .await
        .is_err());
}

/// Build one SDK response frame without implementing transport correlation or crypto.
fn response(outcome: syntra_core::proto::rpc_result::Outcome) -> syntra_core::proto::Frame {
    syntra_core::proto::Frame {
        version: 1,
        request_id: 1,
        body: Some(syntra_core::proto::frame::Body::Result(
            syntra_core::proto::RpcResult {
                outcome: Some(outcome),
            },
        )),
    }
}

/// Shared literal substring semantics include punctuation, spaces and mid-token matches;
/// zero selects the shared default instead of leaking the old local search limit policy.
#[tokio::test]
async fn shared_literal_substrings_and_default_page_size() {
    let _serial = SEARCH_TESTS.lock().await;
    let storage = Arc::new(Storage::open_in_memory().unwrap());
    let mut m = message(1, 1);
    m.message = "preHELLO world 50%_literal café".into();
    seed(&storage, vec![m.clone()]);
    let client = offline(storage).await;
    for query in ["hello world", "50%_literal", "afé", "HELLO", ""] {
        let result = client
            .messages()
            .search(p::SearchRequest {
                query: query.into(),
                filter: if query.is_empty() { 10 } else { 0 },
                limit: 0,
                ..request()
            })
            .await;
        let page = result.unwrap();
        assert_eq!(
            page.messages.len(),
            usize::from(!query.is_empty()),
            "query {query:?}"
        );
    }
    for query in ["world hello", "50%Xliteral", "hel*", "hello OR world"] {
        assert!(client
            .messages()
            .search(p::SearchRequest {
                query: query.into(),
                ..request()
            })
            .await
            .unwrap()
            .messages
            .is_empty());
    }
}

/// New searches use remote first; cache continuations never cross the socket after reconnect,
/// remote continuations never fall back, and the SDK unwraps only its own remote handle.
#[tokio::test]
async fn backend_is_fixed_across_reconnect_and_remote_cursor_is_wrapped() {
    let _serial = SEARCH_TESTS.lock().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let storage = Arc::new(Storage::open_in_memory().unwrap());
    seed(&storage, vec![message(1, 1), message(2, 2), message(3, 3)]);
    let client = Client::new_sqlite(
        support::config(listener.local_addr().unwrap()),
        support::source(),
        storage,
    );
    client.set_principal_user_id(42).await;
    let local = client.messages().search(request()).await.unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let remote = seen.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |req, _| {
            let remote = remote.clone();
            async move {
                assert!(req.method.ends_with("/Search"));
                let r = p::SearchRequest::decode(req.payload.as_slice()).unwrap();
                remote.lock().unwrap().push(r.clone());
                if remote.lock().unwrap().len() == 3 {
                    return response(syntra_core::proto::rpc_result::Outcome::Error(
                        syntra_core::proto::RpcError {
                            code: 14,
                            message: "injected remote continuation outage".into(),
                        },
                    ));
                }
                response(syntra_core::proto::rpc_result::Outcome::Payload(
                    p::SearchResponse {
                        messages: vec![message(777, 777)],
                        next_cursor: Some("server-private".into()),
                        more_available: Some(true),
                        source: 1,
                        coverage: 1,
                        ..Default::default()
                    }
                    .encode_to_vec(),
                ))
            }
        })
        .await
        .unwrap();
        while !handle.is_closed() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        handle.close().await;
    });
    client.connect().await.unwrap();
    let page = client
        .messages()
        .search(p::SearchRequest {
            cursor: local.next_cursor,
            ..request()
        })
        .await
        .unwrap();
    assert_eq!(page.source, 2);
    assert_eq!(page.messages[0].message_id, 1);
    assert!(seen.lock().unwrap().is_empty());
    let page = client.messages().search(request()).await.unwrap();
    assert_eq!(page.messages[0].message_id, 777);
    assert_eq!(page.source, 1);
    let cursor = page.next_cursor.unwrap();
    assert_ne!(cursor, "server-private");
    client
        .messages()
        .search(p::SearchRequest {
            cursor: Some(cursor.clone()),
            ..request()
        })
        .await
        .unwrap();
    assert_eq!(
        seen.lock().unwrap()[1].cursor.as_deref(),
        Some("server-private")
    );
    assert_eq!(
        client
            .messages()
            .search(p::SearchRequest {
                cursor: Some(cursor.clone()),
                ..request()
            })
            .await
            .unwrap_err()
            .code(),
        syntra_core::ErrorCode::Unavailable
    );
    client.close().await;
    server.await.unwrap();
    assert!(client
        .messages()
        .search(p::SearchRequest {
            cursor: Some(cursor),
            ..request()
        })
        .await
        .is_err());
}

/// Serve a fixed real encrypted Search result and count actual application dispatches.
async fn remote_search_fixture(
    storage: Arc<Storage>,
    outcome: syntra_core::proto::rpc_result::Outcome,
) -> (
    Client,
    Arc<std::sync::atomic::AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Client::new_sqlite(
        support::config(listener.local_addr().unwrap()),
        support::source(),
        storage,
    );
    client.set_principal_user_id(42).await;
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dispatched = calls.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |req, _| {
            assert!(req.method.ends_with("/Search"));
            dispatched.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let outcome = outcome.clone();
            async move { response(outcome) }
        })
        .await
        .unwrap();
        while !handle.is_closed() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        handle.close().await;
    });
    (client, calls, server)
}

/// Server Unavailable may mean current READ authority is unavailable. Neither populated
/// nor empty cache may turn that observed RPC failure into successful partial results.
#[tokio::test]
async fn remote_errors_including_authority_unavailable_never_select_cache() {
    let _serial = SEARCH_TESTS.lock().await;
    for populated in [false, true] {
        for code in [1, 3, 4, 7, 9, 11, 12, 13, 14, 16] {
            let storage = Arc::new(Storage::open_in_memory().unwrap());
            if populated {
                seed(&storage, vec![message(1, 1)]);
            }
            let (client, calls, server) = remote_search_fixture(
                storage,
                syntra_core::proto::rpc_result::Outcome::Error(syntra_core::proto::RpcError {
                    code,
                    message: "injected".into(),
                }),
            )
            .await;
            client.connect().await.unwrap();
            let result = client.messages().search(request()).await;
            client.close().await;
            server.await.unwrap();
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert!(
                matches!(result, Err(libtinkle::TinkleError::Syntra(syntra_core::SyntraError::Rpc { code: actual, .. }))
            if actual == syntra_core::ErrorCode::from_i32_lossy(code).0),
                "populated={populated}, status={code}: {result:?}"
            );
        }
    }
}

/// An omitted/UNSPECIFIED server source and coverage stay unknown, not inferred from the socket.
#[tokio::test]
async fn remote_unknown_search_provenance_stays_unknown() {
    let _serial = SEARCH_TESTS.lock().await;
    let expected = p::SearchResponse {
        messages: vec![message(10, 10)],
        more_available: Some(false),
        ..Default::default()
    };
    let (client, calls, server) = remote_search_fixture(
        Arc::new(Storage::open_in_memory().unwrap()),
        syntra_core::proto::rpc_result::Outcome::Payload(expected.encode_to_vec()),
    )
    .await;
    client.connect().await.unwrap();
    let page = client.messages().search(request()).await.unwrap();
    client.close().await;
    server.await.unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(page, expected);
}

/// Animated Photo metadata identifies GIFs, never static Photos/PhotoVideo results.
#[tokio::test]
async fn cached_animated_photos_match_only_gif_content_filter() {
    let _serial = SEARCH_TESTS.lock().await;
    let storage = Arc::new(Storage::open_in_memory().unwrap());
    let mut photo = message(1, 1);
    photo.media = Some(p::Media {
        media: Some(p::media::Media::Photo(p::Photo {
            metadata: Some(p::ImageMetadata {
                animated: false,
                ..Default::default()
            }),
            ..Default::default()
        })),
    });
    let mut gif = message(2, 2);
    gif.media = Some(p::Media {
        media: Some(p::media::Media::Photo(p::Photo {
            metadata: Some(p::ImageMetadata {
                animated: true,
                ..Default::default()
            }),
            ..Default::default()
        })),
    });
    seed(&storage, vec![photo.clone(), gif.clone()]);
    let client = offline(storage).await;
    for (filter, expected) in [(1, photo.clone()), (3, photo), (6, gif)] {
        let page = client
            .messages()
            .search(p::SearchRequest {
                filter,
                ..request()
            })
            .await
            .unwrap();
        assert_eq!(page.source, p::SearchSource::Cache as i32);
        assert_eq!(page.messages, vec![expected], "filter={filter}");
    }
    assert!(!client.is_connected().await);
}

/// Mixed document attributes follow admitted sticker > video > audio > animated >
/// document precedence, independent of attribute order; round/voice flags refine only
/// the selected kind. Each query exercises shared Search over the full cached body.
#[tokio::test]
async fn cached_document_filters_use_one_admitted_kind_for_mixed_attributes() {
    use p::media_attribute::Kind;
    let _serial = SEARCH_TESTS.lock().await;
    let video = |round_video| p::MediaAttribute {
        kind: Some(Kind::Video(p::VideoMetadata {
            width: 16,
            height: 16,
            duration_ms: 1000,
            round_video,
            ..Default::default()
        })),
    };
    let audio = |voice_message| p::MediaAttribute {
        kind: Some(Kind::Audio(p::AudioMetadata {
            duration_ms: 1000,
            voice_message,
            ..Default::default()
        })),
    };
    let animated = p::MediaAttribute {
        kind: Some(Kind::Image(p::ImageMetadata {
            width: 16,
            height: 16,
            animated: true,
            ..Default::default()
        })),
    };
    let sticker = p::MediaAttribute {
        kind: Some(Kind::Sticker(p::StickerMetadata {
            width: 16,
            height: 16,
            ..Default::default()
        })),
    };
    let scenarios = [
        (
            "video+audio",
            vec![video(false), audio(false)],
            vec![0, 2, 3],
        ),
        (
            "video+voice",
            vec![video(false), audio(true)],
            vec![0, 2, 3],
        ),
        (
            "video+animated",
            vec![video(false), animated.clone()],
            vec![0, 2, 3],
        ),
        (
            "round+audio+animated",
            vec![video(true), audio(false), animated.clone()],
            vec![0, 2, 3, 9],
        ),
        ("sticker+video", vec![sticker.clone(), video(true)], vec![0]),
        (
            "sticker+video+audio+animated",
            vec![sticker, video(true), audio(true), animated.clone()],
            vec![0],
        ),
        (
            "audio+animated",
            vec![audio(false), animated.clone()],
            vec![0, 8],
        ),
        (
            "voice+animated",
            vec![audio(true), animated.clone()],
            vec![0, 7],
        ),
        ("animated", vec![animated], vec![0, 6]),
        ("document", vec![], vec![0, 4]),
    ];
    let mut mismatches = Vec::new();
    for (name, attributes, expected_filters) in scenarios {
        for reversed in [false, true] {
            let mut attributes = attributes.clone();
            if reversed {
                attributes.reverse();
            }
            let mut m = message(1, 1);
            m.media = Some(p::Media {
                media: Some(p::media::Media::Document(p::Document {
                    file_id: 1,
                    attributes,
                    ..Default::default()
                })),
            });
            let storage = Arc::new(Storage::open_in_memory().unwrap());
            seed(&storage, vec![m.clone()]);
            let client = offline(storage).await;
            for filter in [0, 1, 2, 3, 4, 6, 7, 8, 9] {
                let page = client
                    .messages()
                    .search(p::SearchRequest {
                        filter,
                        ..request()
                    })
                    .await
                    .unwrap();
                assert_eq!(page.source, p::SearchSource::Cache as i32);
                assert_eq!(page.coverage, p::SearchCoverage::CachedSubset as i32);
                let expected = if expected_filters.contains(&filter) {
                    vec![m.clone()]
                } else {
                    vec![]
                };
                if page.messages != expected {
                    mismatches.push((name, reversed, filter));
                }
            }
            assert!(!client.is_connected().await);
        }
    }
    assert!(
        mismatches.is_empty(),
        "incorrect (kind, reversed, filter) matches: {mismatches:#?}"
    );
}

/// Sender scope is a positive user, not a conversation. Invalid senders fail both offline
/// and connected before any RPC; positive UserId still dispatches normally.
#[tokio::test]
async fn invalid_sender_is_rejected_before_either_backend() {
    let _serial = SEARCH_TESTS.lock().await;
    let storage = Arc::new(Storage::open_in_memory().unwrap());
    seed(&storage, vec![message(1, 1)]);
    let (client, calls, server) = remote_search_fixture(
        storage,
        syntra_core::proto::rpc_result::Outcome::Payload(
            p::SearchResponse {
                source: 1,
                coverage: 1,
                more_available: Some(false),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
    )
    .await;
    for connected in [false, true] {
        if connected {
            client.connect().await.unwrap();
        }
        for kind in [
            Some(p::target::Kind::ChatId(42)),
            Some(p::target::Kind::UserId(0)),
            Some(p::target::Kind::UserId(-1)),
            None,
        ] {
            let result = client
                .messages()
                .search(p::SearchRequest {
                    from_target: Some(p::Target { kind }),
                    ..request()
                })
                .await;
            assert!(
                matches!(result, Err(libtinkle::TinkleError::Codec(_))),
                "connected={connected}: {result:?}"
            );
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        }
    }
    client
        .messages()
        .search(p::SearchRequest {
            from_target: Some(p::Target {
                kind: Some(p::target::Kind::UserId(42)),
            }),
            ..request()
        })
        .await
        .unwrap();
    client.close().await;
    server.await.unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

/// A queued cache read cannot publish after account/source replacement or irreversible logout.
#[test]
fn queued_search_keeps_account_source_and_logout_fences() {
    for mode in ["account", "source", "logout"] {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        rt.block_on(async {
            let _serial = SEARCH_TESTS.lock().await;
            let storage = Arc::new(Storage::open_in_memory().unwrap());
            seed(&storage, vec![message(1, 1)]);
            let client = offline(storage).await;
            let (release, hold) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || hold.recv().unwrap());
            let search = client.messages();
            let search = search.search(request());
            tokio::pin!(search);
            assert!(futures_util::poll!(&mut search).is_pending());
            match mode {
                "account" => {
                    let switch = client.set_principal_user_id(43);
                    tokio::pin!(switch);
                    assert!(futures_util::poll!(&mut switch).is_pending());
                    release.send(()).unwrap();
                    switch.await;
                }
                "source" => {
                    let switch = client.replace_trust_source(support::source());
                    tokio::pin!(switch);
                    let outcome = futures_util::poll!(&mut switch);
                    release.send(()).unwrap();
                    match outcome {
                        std::task::Poll::Pending => switch.await.unwrap(),
                        std::task::Poll::Ready(result) => result.unwrap(),
                    }
                }
                _ => {
                    let logout = client.logout();
                    tokio::pin!(logout);
                    assert!(futures_util::poll!(&mut logout).is_pending());
                    release.send(()).unwrap();
                    logout.await.unwrap();
                }
            }
            assert!(search.await.is_err(), "stale {mode} search published");
            blocker.await.unwrap();
        });
    }
}

/// Global order groups target identities rather than comparing unrelated acceptance positions.
#[tokio::test]
async fn global_cache_paging_keeps_target_local_positions_separate() {
    let _serial = SEARCH_TESTS.lock().await;
    let storage = Arc::new(Storage::open_in_memory().unwrap());
    seed(&storage, vec![message(1, 1000)]);
    let mut other = message(1, 1);
    other.target = Some(p::Target {
        kind: Some(p::target::Kind::ChatId(8)),
    });
    storage
        .apply_journal_page_fenced(
            "user:42",
            "target:room:8",
            &tinkle_storage::CacheOps {
                messages: vec![tinkle_storage::MessageRow {
                    conversation_key: "target:room:8".into(),
                    message_id: 1,
                    peer_kind: "room".into(),
                    peer_id: 8,
                    from_id: 42,
                    body: other.message.clone(),
                    proto: other.encode_to_vec(),
                    origin_stream: "target:room:8".into(),
                    accept_seq: Some(1),
                    edit_date_secs: None,
                    edit_date_nanos: None,
                }],
                ..Default::default()
            },
            0,
            0,
        )
        .unwrap();
    let client = offline(storage).await;
    let req = p::SearchRequest {
        target: None,
        limit: 1,
        ..request()
    };
    let page = client.messages().search(req.clone()).await.unwrap();
    assert_eq!(page.messages, vec![other]);
    let page = client
        .messages()
        .search(p::SearchRequest {
            cursor: page.next_cursor,
            ..req
        })
        .await
        .unwrap();
    assert_eq!(page.messages, vec![message(1, 1000)]);
}
