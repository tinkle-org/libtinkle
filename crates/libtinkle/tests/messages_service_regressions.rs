//! Typed Messages boundary regressions; invalid input must fail before transport admission.
use libtinkle::sending::OutgoingState;
use libtinkle::{
    CancelToken, Client, InMemoryCursorStore, OutgoingRequest, SendRetry, TinkleError,
};
use prost::Message as _;
use std::sync::{Arc, Mutex};
use tinkle_schema::proto as p;
mod support;

/// A nonzero operation key does not make a missing conversation valid.
#[tokio::test]
async fn send_rejects_missing_target_before_transport() {
    let client = Client::new(
        support::config("127.0.0.1:1".parse().unwrap()),
        support::source(),
        InMemoryCursorStore::new(),
    );
    let result = client
        .messages()
        .send_message(tinkle_schema::proto::SendMessageRequest {
            idempotency_key: 7,
            message: "hello".into(),
            ..Default::default()
        })
        .await;
    assert!(matches!(result, Err(TinkleError::Codec(_))), "{result:?}");
}

/// Explicit job selectors are exclusive, bounded and distinct; signed retry keys
/// stay valid while identifiers and missing targets fail before transport admission.
#[tokio::test]
async fn scheduled_selection_and_poll_close_validate_before_transport() {
    let client = Client::new(
        support::config("127.0.0.1:1".parse().unwrap()),
        support::source(),
        InMemoryCursorStore::new(),
    );
    let target = Some(p::Target {
        kind: Some(p::target::Kind::ChatId(7)),
    });
    for (ids, keys, valid) in [
        (vec![], vec![], false),
        (vec![1], vec![2], false),
        (vec![1, 1], vec![], false),
        (vec![-1], vec![], false),
        (vec![0], vec![], false),
        (vec![], vec![0], false),
        (vec![], vec![-7, -7], false),
        ((1..=101).collect(), vec![], false),
        (vec![], (1..=101).collect(), false),
        (vec![1], vec![], true),
        (vec![], vec![-7, i64::MAX], true),
    ] {
        let result = client
            .messages()
            .get_scheduled_messages(p::GetScheduledMessagesRequest {
                target,
                message_ids: ids,
                idempotency_keys: keys,
            })
            .await;
        assert_eq!(
            matches!(result, Err(TinkleError::Codec(_))),
            !valid,
            "{result:?}"
        );
    }
    for ids in [vec![], vec![0], vec![-1], vec![1, 1], (1..=101).collect()] {
        assert!(matches!(
            client
                .messages()
                .cancel_scheduled_messages(p::CancelScheduledMessagesRequest {
                    target,
                    message_ids: ids,
                })
                .await,
            Err(TinkleError::Codec(_))
        ));
    }
    for request in [
        p::ClosePollRequest {
            target,
            message_id: 0,
        },
        p::ClosePollRequest {
            target: None,
            message_id: 1,
        },
    ] {
        assert!(matches!(
            client.messages().close_poll(request).await,
            Err(TinkleError::Codec(_))
        ));
    }
}

/// DM and room draft tokens follow the same text/media/album validation and
/// durable-byte freezing in immediate and scheduled modes; no late token lookup.
#[tokio::test]
async fn draft_clear_target_send_matrix_freezes_original_pair() {
    let storage = Arc::new(libtinkle::Storage::open_in_memory().unwrap());
    let client = Client::new_sqlite(
        support::config("127.0.0.1:1".parse().unwrap()),
        support::source(),
        storage,
    );
    client.set_principal_user_id(42).await;
    for room in [false, true] {
        for scheduled in [false, true] {
            for kind in ["text", "media", "album"] {
                let target = Some(p::Target {
                    kind: Some(if room {
                        p::target::Kind::ChatId(7)
                    } else {
                        p::target::Kind::UserId(9)
                    }),
                });
                let schedule_date = scheduled.then_some(2_000_000_000);
                let media = Some(p::MediaSource {
                    media: Some(p::media_source::Media::Document(p::DocumentSource {
                        file_id: 100,
                        ..Default::default()
                    })),
                });
                let (request, bytes, codec_error) = match kind {
                    "text" => {
                        let r = p::SendMessageRequest {
                            target,
                            message: "draft".into(),
                            idempotency_key: 41,
                            schedule_date,
                            clear_draft: Some(true),
                            clear_draft_dialog_id: Some(55),
                            clear_draft_revision: Some(0),
                            ..Default::default()
                        };
                        let codec = matches!(
                            client.messages().send_message(r.clone()).await,
                            Err(TinkleError::Codec(_))
                        );
                        (OutgoingRequest::Text(r.clone()), r.encode_to_vec(), codec)
                    }
                    "media" => {
                        let r = p::SendMediaRequest {
                            target,
                            media,
                            idempotency_key: 41,
                            schedule_date,
                            clear_draft: Some(true),
                            clear_draft_dialog_id: Some(55),
                            clear_draft_revision: Some(0),
                            ..Default::default()
                        };
                        let codec = matches!(
                            client.messages().send_media(r.clone()).await,
                            Err(TinkleError::Codec(_))
                        );
                        (OutgoingRequest::Media(r.clone()), r.encode_to_vec(), codec)
                    }
                    _ => {
                        let r = p::SendAlbumRequest {
                            target,
                            items: vec![
                                p::AlbumItem {
                                    media: media.clone(),
                                    idempotency_key: 41,
                                    ..Default::default()
                                },
                                p::AlbumItem {
                                    media,
                                    idempotency_key: 42,
                                    ..Default::default()
                                },
                            ],
                            schedule_date,
                            clear_draft: Some(true),
                            clear_draft_dialog_id: Some(55),
                            clear_draft_revision: Some(0),
                            ..Default::default()
                        };
                        let codec = matches!(
                            client.messages().send_album(r.clone()).await,
                            Err(TinkleError::Codec(_))
                        );
                        (OutgoingRequest::Album(r.clone()), r.encode_to_vec(), codec)
                    }
                };
                assert!(!codec_error, "{room}/{scheduled}/{kind}");
                let job_id = format!("draft-{room}-{scheduled}-{kind}");
                let job = client
                    .sending()
                    .enqueue(job_id.clone(), request.clone())
                    .await
                    .unwrap();
                assert_eq!(job.payload, bytes);
                let retry = client.sending().enqueue(job_id, request).await.unwrap();
                assert_eq!(
                    retry.payload, bytes,
                    "retry cannot recapture a newer draft token"
                );
            }
        }
    }
    for (index, (generation, revision)) in [
        (None, Some(0)),
        (Some(0), Some(0)),
        (Some(55), None),
        (Some(55), Some(-1)),
    ]
    .into_iter()
    .enumerate()
    {
        let r = p::SendMessageRequest {
            target: Some(p::Target {
                kind: Some(p::target::Kind::ChatId(7)),
            }),
            message: "invalid".into(),
            idempotency_key: 41,
            clear_draft: Some(true),
            clear_draft_dialog_id: generation,
            clear_draft_revision: revision,
            ..Default::default()
        };
        assert!(matches!(
            client.messages().send_message(r.clone()).await,
            Err(TinkleError::Codec(_))
        ));
        assert!(client
            .sending()
            .enqueue(format!("invalid-draft-{index}"), OutgoingRequest::Text(r))
            .await
            .is_err());
    }
    assert_eq!(client.sending().jobs().await.unwrap().len(), 12);
    client.close().await;
}

/// Pin scope omission/shared/personal pass shape validation; explicit zero/unknown never dispatch.
#[tokio::test]
async fn pin_scope_presence_is_not_defaulted() {
    let client = Client::new(
        support::config("127.0.0.1:1".parse().unwrap()),
        support::source(),
        InMemoryCursorStore::new(),
    );
    for scope in [None, Some(1), Some(2), Some(0), Some(-1), Some(3)] {
        let result = client
            .messages()
            .set_message_pin(p::SetMessagePinRequest {
                target: Some(p::Target {
                    kind: Some(p::target::Kind::ChatId(7)),
                }),
                message_id: 42,
                pin_scope: scope,
                ..Default::default()
            })
            .await;
        assert_eq!(
            matches!(result, Err(TinkleError::Codec(_))),
            matches!(scope, Some(0 | -1 | 3)),
            "{scope:?}: {result:?}"
        );
    }
}

/// Preserve exact payload/key through a cancelled encrypted call and a process-style restart;
/// never replay on inspection or default resume, and hydrate without forging journal progress.
#[tokio::test]
async fn durable_send_cancel_restart_exact_retry_and_cache() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let seen = requests.clone();
    let ready = entered.clone();
    let gate = release.clone();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = seen.clone();
            let ready = ready.clone();
            let gate = gate.clone();
            let handle = support::accept(stream, move |req, _| {
                let seen = seen.clone();
                let ready = ready.clone();
                let gate = gate.clone();
                async move {
                    assert_eq!(req.method, "/tinkle.v1.MessagesService/SendMessage");
                    let request = p::SendMessageRequest::decode(req.payload.as_slice()).unwrap();
                    let first = {
                        let mut seen = seen.lock().unwrap();
                        seen.push(req.payload);
                        seen.len() == 1
                    };
                    if first {
                        ready.notify_one();
                        gate.acquire().await.unwrap().forget();
                    }
                    let response = p::SendMessageResponse {
                        message: Some(p::Message {
                            message_id: 81,
                            target: request.target,
                            from_id: 42,
                            message: request.message,
                            accept_seq: Some(9),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    syntra_core::proto::Frame {
                        version: 1,
                        request_id: 1,
                        body: Some(syntra_core::proto::frame::Body::Result(
                            syntra_core::proto::RpcResult {
                                outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(
                                    response.encode_to_vec(),
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
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let path = path.to_str().unwrap();
    let storage = Arc::new(libtinkle::Storage::open(path).unwrap());
    let client = Client::new_sqlite(config.clone(), support::source(), storage.clone());
    client.set_principal_user_id(42).await;
    client.connect().await.unwrap();
    let request = p::SendMessageRequest {
        target: Some(p::Target {
            kind: Some(p::target::Kind::ChatId(7)),
        }),
        message: "hello durable".into(),
        silent: Some(false),
        clear_draft: Some(true),
        clear_draft_dialog_id: Some(77),
        clear_draft_revision: Some(0),
        ..Default::default()
    };
    let job = client
        .sending()
        .enqueue("message-1".into(), OutgoingRequest::Text(request.clone()))
        .await
        .unwrap();
    let persisted = p::SendMessageRequest::decode(job.payload.as_slice()).unwrap();
    assert_ne!(persisted.idempotency_key, 0);
    assert_eq!(persisted.silent, Some(false));
    assert_eq!(persisted.clear_draft_dialog_id, Some(77));
    assert_eq!(persisted.clear_draft_revision, Some(0));
    assert_eq!(
        client
            .sending()
            .enqueue("message-1".into(), OutgoingRequest::Text(request.clone()))
            .await
            .unwrap()
            .payload,
        job.payload
    );
    let token = CancelToken::new();
    let cancelled = token.clone();
    let sender = client.clone();
    let task = tokio::spawn(async move {
        sender
            .sending()
            .resume("message-1".into(), SendRetry::Never, Some(cancelled))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert_eq!(
        client.sending().jobs().await.unwrap()[0].state,
        OutgoingState::Dispatching
    );
    token.cancel();
    assert!(task.await.unwrap().is_err());
    release.add_permits(1);
    let job = client
        .sending()
        .resume("message-1".into(), SendRetry::Never, None)
        .await
        .unwrap();
    assert!(matches!(
        job.state,
        OutgoingState::Dispatching | OutgoingState::Unconfirmed
    ));
    if job.state == OutgoingState::Unconfirmed {
        let outcome = job.last_outcome.as_ref().unwrap();
        assert_eq!(outcome.code, syntra_core::ErrorCode::Cancelled as i32);
        assert!(outcome.unknown_outcome);
    }
    assert_eq!(requests.lock().unwrap().len(), 1);
    client.close().await;
    drop(client);
    drop(storage);
    let storage = Arc::new(libtinkle::Storage::open(path).unwrap());
    let client = Client::new_sqlite(config, support::source(), storage.clone());
    client.set_principal_user_id(42).await;
    // Retained outcome is inspectable offline, with no dial or replay.
    assert_eq!(client.sending().jobs().await.unwrap().len(), 1);
    assert_eq!(requests.lock().unwrap().len(), 1);
    client.connect().await.unwrap();
    let accepted = client
        .sending()
        .resume("message-1".into(), SendRetry::ExactKeyedRequest, None)
        .await
        .unwrap();
    assert_eq!(accepted.state, OutgoingState::Accepted);
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests[0], requests[1]);
    }
    let cached = storage
        .get_messages("user:42", "target:room:7", 10, None)
        .unwrap();
    assert_eq!(cached.len(), 1);
    assert_eq!(cached[0].body, "hello durable");
    assert_eq!(
        storage.load_journal("user:42", "target:room:7").unwrap(),
        Some(0)
    );
    let hits = storage
        .search_messages(
            "user:42",
            &tinkle_storage::MessageSearchRequest {
                query: "durable".into(),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(hits.hits.len(), 1);
    client
        .sending()
        .resume("message-1".into(), SendRetry::ExactKeyedRequest, None)
        .await
        .unwrap();
    assert_eq!(requests.lock().unwrap().len(), 2);
    let mut conflict = request;
    conflict.message = "changed".into();
    assert!(client
        .sending()
        .enqueue("message-1".into(), OutgoingRequest::Text(conflict))
        .await
        .is_err());
    client.set_principal_user_id(43).await;
    assert!(client.sending().jobs().await.unwrap().is_empty());
    client.close().await;
    server.await.unwrap();
}

/// Every published Messages method has a real concrete typed dispatch path, not a generic stub.
#[tokio::test]
#[allow(clippy::needless_update)]
async fn all_messages_methods_cross_the_encrypted_boundary() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(std::collections::BTreeSet::new()));
    let remote = seen.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |request, _| {
            let remote = remote.clone();
            async move {
                assert!(request.method.starts_with("/tinkle.v1.MessagesService/"));
                remote.lock().unwrap().insert(request.method);
                syntra_core::proto::Frame {
                    version: 1,
                    request_id: 1,
                    body: Some(syntra_core::proto::frame::Body::Result(
                        syntra_core::proto::RpcResult {
                            outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(vec![])),
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
    let c = Client::new(config, support::source(), InMemoryCursorStore::new());
    c.set_principal_user_id(42).await;
    c.connect().await.unwrap();
    let target = Some(p::Target {
        kind: Some(p::target::Kind::ChatId(7)),
    });
    let photo = p::MediaSource {
        media: Some(p::media_source::Media::Photo(p::PhotoSource {
            file_id: 1,
            ..Default::default()
        })),
    };
    macro_rules! invoke {($name:ident,$request:ident {$($field:ident $( : $value:expr )?),* $(,)?})=>{c.messages().$name(p::$request {$($field $(: $value)?,)* ..Default::default()}).await.unwrap();};}
    invoke!(get_dialogs, GetDialogsRequest {});
    invoke!(
        get_scheduled_messages,
        GetScheduledMessagesRequest {
            target,
            idempotency_keys: vec![-7]
        }
    );
    invoke!(
        cancel_scheduled_messages,
        CancelScheduledMessagesRequest {
            target,
            message_ids: vec![1]
        }
    );
    invoke!(
        close_poll,
        ClosePollRequest {
            target,
            message_id: 1
        }
    );
    invoke!(get_history, GetHistoryRequest { target, limit: 20 });
    invoke!(
        get_messages,
        GetMessagesRequest {
            target,
            message_ids: vec![1],
        }
    );
    invoke!(list_message_changes, ListMessageChangesRequest { target });
    invoke!(
        mark_history_read,
        MarkHistoryReadRequest { target, max_id: 1 }
    );
    invoke!(
        delete_history,
        DeleteHistoryRequest {
            target,
            operation_id: "11111111-1111-4111-8111-111111111111".into()
        }
    );
    invoke!(
        delete_messages,
        DeleteMessagesRequest {
            target,
            message_ids: vec![1],
        }
    );
    invoke!(
        create_chat,
        CreateChatRequest {
            title: "Group".into(),
            user_ids: vec![1],
        }
    );
    invoke!(
        edit_chat_title,
        EditChatTitleRequest {
            chat_id: 7,
            title: "Title".into(),
        }
    );
    invoke!(
        edit_chat_photo,
        EditChatPhotoRequest {
            chat_id: 7,
            photo: Some(p::ChatPhotoSource {
                input: Some(p::chat_photo_source::Input::Empty(true))
            }),
        }
    );
    invoke!(
        update_chat_description,
        UpdateChatDescriptionRequest { chat_id: 7 }
    );
    invoke!(get_chats, GetChatsRequest { chat_ids: vec![7] });
    invoke!(get_chat_details, GetChatDetailsRequest { chat_id: 7 });
    invoke!(
        add_chat_participant,
        AddChatParticipantRequest {
            chat_id: 7,
            user_id: 1,
        }
    );
    invoke!(
        remove_chat_participant,
        RemoveChatParticipantRequest {
            chat_id: 7,
            user_id: 1,
        }
    );
    invoke!(
        edit_chat_admin,
        EditChatAdminRequest {
            chat_id: 7,
            user_id: 1,
        }
    );
    invoke!(
        edit_chat_restricted_scopes,
        EditChatRestrictedScopesRequest {
            chat_id: 7,
            user_id: 1,
        }
    );
    invoke!(set_history_ttl, SetHistoryTtlRequest { target });
    invoke!(
        set_message_pin,
        SetMessagePinRequest {
            target,
            message_id: 1,
        }
    );
    invoke!(unpin_all_messages, UnpinAllMessagesRequest { target });
    invoke!(
        send_message,
        SendMessageRequest {
            target,
            idempotency_key: 7,
            message: "hello".into(),
        }
    );
    invoke!(
        send_media,
        SendMediaRequest {
            target,
            idempotency_key: 8,
            media: Some(photo.clone()),
        }
    );
    invoke!(
        send_album,
        SendAlbumRequest {
            target,
            items: vec![
                p::AlbumItem {
                    media: Some(photo.clone()),
                    idempotency_key: 1,
                    ..Default::default()
                },
                p::AlbumItem {
                    media: Some(photo),
                    idempotency_key: 2,
                    ..Default::default()
                }
            ],
        }
    );
    invoke!(
        edit_message,
        EditMessageRequest {
            target,
            message_id: 1,
            message: Some("edit".into()),
        }
    );
    invoke!(set_typing, SetTypingRequest { target, action: 1 });
    invoke!(
        start_bot,
        StartBotRequest {
            target,
            bot: Some(p::Target {
                kind: Some(p::target::Kind::UserId(1))
            }),
            idempotency_key: 9,
        }
    );
    invoke!(search, SearchRequest { target, limit: 20 });
    invoke!(
        batch_get_message_reactions,
        BatchGetMessageReactionsRequest {
            target,
            message_ids: vec![1],
        }
    );
    invoke!(
        list_message_reactions,
        ListMessageReactionsRequest {
            target,
            message_id: 1,
            limit: 20,
        }
    );
    invoke!(
        send_reaction,
        SendReactionRequest {
            target,
            message_id: 1,
        }
    );
    invoke!(
        get_unread_mentions,
        GetUnreadMentionsRequest { target, limit: 20 }
    );
    invoke!(read_mentions, ReadMentionsRequest { target });
    invoke!(
        get_unread_reactions,
        GetUnreadReactionsRequest { target, limit: 20 }
    );
    invoke!(read_reactions, ReadReactionsRequest { target });
    invoke!(get_recent_reactions, GetRecentReactionsRequest {});
    invoke!(
        send_vote,
        SendVoteRequest {
            target,
            message_id: 1,
        }
    );
    invoke!(
        get_poll_votes,
        GetPollVotesRequest {
            target,
            message_id: 1,
            limit: 20,
        }
    );
    invoke!(
        get_poll_results,
        GetPollResultsRequest {
            target,
            message_id: 1,
        }
    );
    invoke!(
        read_thread,
        ReadThreadRequest {
            target,
            thread_root_id: 1,
        }
    );
    invoke!(
        list_thread_messages,
        ListThreadMessagesRequest {
            target,
            message_id: 1,
            limit: 20,
        }
    );
    let expected: std::collections::BTreeSet<_> = tinkle_schema::methods::ALL_METHODS
        .iter()
        .filter(|(m, _, _)| m.starts_with("/tinkle.v1.MessagesService/"))
        .map(|(m, _, _)| m.to_string())
        .collect();
    assert_eq!(expected.len(), 43);
    assert_eq!(*seen.lock().unwrap(), expected);
    c.close().await;
    server.await.unwrap();
}

/// Metadata-only bodies beat stale hydration at tied edit dates, and filtered/empty processed
/// horizons commit durably without relabeling a body revision as the journal cursor.
#[tokio::test]
async fn metadata_revisions_and_filtered_horizon_cross_encrypted_reconciliation() {
    let target = p::Target {
        kind: Some(p::target::Kind::ChatId(7)),
    };
    let old = p::Message {
        message_id: 1,
        target: Some(target),
        from_id: 42,
        message: "metadata body".into(),
        accept_seq: Some(1),
        body_revision: Some(1),
        ..Default::default()
    };
    let mut updated = old.clone();
    updated.body_revision = Some(2);
    updated.pinned = true;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let stale = old.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |request, _| {
            let updated = updated.clone();
            let stale = stale.clone();
            async move {
                let payload = match request.method.rsplit('/').next().unwrap() {
                    "ListMessageChanges" => {
                        let r = p::ListMessageChangesRequest::decode(request.payload.as_slice())
                            .unwrap();
                        if r.after_accept_seq == 0 {
                            p::ListMessageChangesResponse {
                                changes: vec![p::TargetChange {
                                    message_id: 1,
                                    kind: p::TargetChangeKind::Pin as i32,
                                    message: Some(updated),
                                    accept_seq: 5,
                                    message_revision: Some(2),
                                    body_invalidated: false,
                                }],
                                last_accept_seq: 8,
                                more_available: Some(false),
                            }
                        } else {
                            p::ListMessageChangesResponse {
                                changes: vec![],
                                last_accept_seq: 11,
                                more_available: Some(false),
                            }
                        }
                        .encode_to_vec()
                    }
                    "GetMessages" => p::GetMessagesResponse {
                        messages: vec![stale],
                        ..Default::default()
                    }
                    .encode_to_vec(),
                    "GetHistory" => p::GetHistoryResponse {
                        more_available: Some(false),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                    other => panic!("unexpected fixture method {other}"),
                };
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
    let storage = Arc::new(libtinkle::Storage::open_in_memory().unwrap());
    let ops = tinkle_storage::CacheOps {
        messages: vec![tinkle_storage::MessageRow {
            conversation_key: "target:room:7".into(),
            message_id: 1,
            peer_kind: "room".into(),
            peer_id: 7,
            from_id: 42,
            body: old.message.clone(),
            proto: old.encode_to_vec(),
            origin_stream: "target:room:7".into(),
            accept_seq: Some(1),
            edit_date_secs: None,
            edit_date_nanos: None,
        }],
        message_body_revisions: vec![(1, 1)],
        ..Default::default()
    };
    storage
        .apply_journal_page_fenced("user:42", "target:room:7", &ops, 0, 0)
        .unwrap();
    let client = Client::new_sqlite(config, support::source(), storage.clone());
    client.set_principal_user_id(42).await;
    client.connect().await.unwrap();
    client.subscribe(vec![target]).await.unwrap();
    assert_eq!(
        storage.load_journal("user:42", "target:room:7").unwrap(),
        Some(8)
    );
    let rows = storage
        .get_messages("user:42", "target:room:7", 10, None)
        .unwrap();
    let body = p::Message::decode(rows[0].proto.as_slice()).unwrap();
    assert!(body.pinned);
    assert_eq!(body.body_revision, Some(2));
    assert_eq!(body.edit_date, None);
    client.reconcile_target("target:room:7").await.unwrap();
    assert_eq!(
        storage.load_journal("user:42", "target:room:7").unwrap(),
        Some(11)
    );
    client.close().await;
    server.await.unwrap();
}
