//! Durable domain-key recovery, cancellation races and independent accepted identities.
#![cfg(tinkle_async_actions)]
mod support;
use libtinkle::{Client, OutgoingRequest, OutgoingState as S, SendRetry, Storage};
use prost::Message;
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc, Mutex,
};
use tinkle_schema::proto as p;

const UUID: &str = "11111111-1111-4111-8111-111111111111";
fn target() -> Option<p::Target> {
    Some(p::Target {
        kind: Some(p::target::Kind::ChatId(7)),
    })
}
fn deadline(state: i32) -> Option<prost_types::Timestamp> {
    (state >= 3).then_some(prost_types::Timestamp {
        seconds: 2_000_000_000,
        nanos: 0,
    })
}
fn reply(bytes: Vec<u8>) -> syntra_core::proto::Frame {
    syntra_core::proto::Frame {
        version: 1,
        request_id: 1,
        body: Some(syntra_core::proto::frame::Body::Result(
            syntra_core::proto::RpcResult {
                outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(bytes)),
            },
        )),
    }
}
fn source(file_id: i64) -> Option<p::MediaSource> {
    Some(p::MediaSource {
        media: Some(p::media_source::Media::Photo(p::PhotoSource {
            file_id,
            ..Default::default()
        })),
    })
}
fn photo() -> Option<p::ChatPhotoSource> {
    Some(p::ChatPhotoSource {
        input: Some(p::chat_photo_source::Input::Photo(p::PhotoSource {
            file_id: 9,
            ..Default::default()
        })),
    })
}
fn profile() -> p::SetProfilePhotoRequest {
    p::SetProfilePhotoRequest {
        request_id: UUID.into(),
        expected_revision: 7,
        source: Some(p::ProfilePhotoSource {
            file_id: 9,
            metadata: Some(p::ImageMetadata {
                width: 2,
                height: 2,
                format: 1,
                validation: Some(p::MediaValidation {
                    profile: 1,
                    version: 1,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
    }
}
fn asset() -> Option<p::StickerAssetSource> {
    Some(p::StickerAssetSource {
        file_id: 9,
        metadata: Some(p::StickerMetadata {
            width: 2,
            height: 2,
            format: 4,
            validation: Some(p::MediaValidation {
                profile: 1,
                version: 1,
            }),
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// Real encrypted request fixtures return owning-domain status snapshots, never cached-body authority.
#[tokio::test]
async fn all_photo_and_asset_owners_persist_original_keys_and_cancel_claimed_work() {
    for terminal in [3, 4, 5] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = support::config(listener.local_addr().unwrap());
        let phase = Arc::new(AtomicI32::new(1));
        let remote_phase = phase.clone();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let handle = support::accept(stream, move |r, _| {
                let phase = remote_phase.clone();
                let calls = recorded.clone();
                async move {
                    calls
                        .lock()
                        .unwrap()
                        .push((r.method.clone(), r.payload.clone()));
                    let state = phase.load(Ordering::SeqCst);
                    let query = r.method.contains("/Get");
                    let cancel = r.method.contains("/Cancel");
                    let failure = if state == 4 { 3 } else { 0 };
                    let photo_id = (state == 3).then_some(41);
                    let withheld = (state == 3).then_some(false);
                    macro_rules! status {
                        ($status:expr,$submit:ty,$get:ty,$cancel:ty,$field:ident) => {{
                            let bytes = if query {
                                let mut v = <$get>::default();
                                v.$field = Some($status);
                                v.encode_to_vec()
                            } else if cancel {
                                let mut v = <$cancel>::default();
                                v.$field = Some($status);
                                v.encode_to_vec()
                            } else {
                                let mut v = <$submit>::default();
                                v.$field = Some($status);
                                v.encode_to_vec()
                            };
                            reply(bytes)
                        }};
                    }
                    if r.method.contains("MessagesService") {
                        if !query && !cancel {
                            let v = p::EditChatPhotoRequest::decode(r.payload.as_slice()).unwrap();
                            assert_eq!(v.operation_id, 19);
                        } else if query {
                            let v =
                                p::GetChatPhotoUpdateRequest::decode(r.payload.as_slice()).unwrap();
                            assert_eq!((v.chat_id, v.operation_id), (7, 19));
                        } else {
                            let v = p::CancelChatPhotoUpdateRequest::decode(r.payload.as_slice())
                                .unwrap();
                            assert_eq!((v.chat_id, v.operation_id), (7, 19));
                        }
                        status!(
                            p::ChatPhotoUpdate {
                                chat_id: 7,
                                operation_id: 19,
                                state,
                                revision: state as u64,
                                failure,
                                photo_id,
                                reference_withheld: withheld,
                                retain_until: deadline(state)
                            },
                            p::EditChatPhotoResponse,
                            p::GetChatPhotoUpdateResponse,
                            p::CancelChatPhotoUpdateResponse,
                            photo_update
                        )
                    } else if r.method.contains("ChannelsService") {
                        if !query && !cancel {
                            let v = p::EditPhotoRequest::decode(r.payload.as_slice()).unwrap();
                            assert_eq!(v.request_id, UUID);
                        } else if query {
                            let v = p::GetPhotoUpdateRequest::decode(r.payload.as_slice()).unwrap();
                            assert_eq!((v.channel_id, v.request_id), (7, UUID.into()));
                        } else {
                            let v =
                                p::CancelPhotoUpdateRequest::decode(r.payload.as_slice()).unwrap();
                            assert_eq!((v.channel_id, v.request_id), (7, UUID.into()));
                        }
                        status!(
                            p::ChannelPhotoUpdate {
                                channel_id: 7,
                                request_id: UUID.into(),
                                state,
                                revision: state as u64,
                                failure,
                                photo_id,
                                reference_withheld: withheld,
                                retain_until: deadline(state)
                            },
                            p::EditPhotoResponse,
                            p::GetPhotoUpdateResponse,
                            p::CancelPhotoUpdateResponse,
                            photo_update
                        )
                    } else if r.method.contains("AccountService") {
                        if !query && !cancel {
                            let v =
                                p::SetProfilePhotoRequest::decode(r.payload.as_slice()).unwrap();
                            assert_eq!(v.expected_revision, 7);
                            assert_eq!(v.request_id, UUID);
                        }
                        status!(
                            p::ProfilePhotoUpdate {
                                request_id: UUID.into(),
                                state,
                                revision: state as u64,
                                failure,
                                photo_id,
                                reference_withheld: withheld,
                                gallery_revision: if state == 3 { 8 } else { 0 },
                                retain_until: deadline(state)
                            },
                            p::SetProfilePhotoResponse,
                            p::GetProfilePhotoUpdateResponse,
                            p::CancelProfilePhotoUpdateResponse,
                            photo_update
                        )
                    } else {
                        let asset_id = if query {
                            p::GetAssetChangeRequest::decode(r.payload.as_slice())
                                .unwrap()
                                .asset_id
                        } else if cancel {
                            p::CancelAssetChangeRequest::decode(r.payload.as_slice())
                                .unwrap()
                                .asset_id
                        } else if r.method.ends_with("/UpdateStickerAsset") {
                            let v =
                                p::UpdateStickerAssetRequest::decode(r.payload.as_slice()).unwrap();
                            assert_eq!(v.expected_version, 5);
                            assert_eq!(v.request_id, UUID);
                            Some(v.asset_id)
                        } else {
                            let v =
                                p::CreateStickerAssetRequest::decode(r.payload.as_slice()).unwrap();
                            assert_eq!(v.request_id, UUID);
                            None
                        };
                        let s = p::AssetChange {
                            request_id: UUID.into(),
                            asset_id,
                            state,
                            revision: state as u64,
                            failure,
                            accepted: (state == 3).then_some(p::AssetReference {
                                asset_id: asset_id.unwrap_or(31),
                                asset_version: 6,
                            }),
                            reference_withheld: withheld,
                            retain_until: deadline(state),
                        };
                        if r.method.ends_with("/UpdateStickerAsset") {
                            status!(
                                s,
                                p::UpdateStickerAssetResponse,
                                p::GetAssetChangeResponse,
                                p::CancelAssetChangeResponse,
                                asset_change
                            )
                        } else {
                            status!(
                                s,
                                p::CreateStickerAssetResponse,
                                p::GetAssetChangeResponse,
                                p::CancelAssetChangeResponse,
                                asset_change
                            )
                        }
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
        let db = Arc::new(Storage::open_in_memory().unwrap());
        let client = Client::new_sqlite(config, support::source(), db.clone());
        client.set_principal_user_id(42).await;
        let mut raw = p::EditPhotoRequest {
            channel_id: 7,
            photo: photo(),
            request_id: UUID.into(),
        }
        .encode_to_vec();
        // Explicit scalar presence is legal and must be preserved, not reserialized away.
        raw.extend([0x08, 7]);
        client
            .sending()
            .enqueue_chat_photo(
                "chat".into(),
                p::EditChatPhotoRequest {
                    chat_id: 7,
                    operation_id: 19,
                    photo: photo(),
                },
            )
            .await
            .unwrap();
        let channel = client
            .sending()
            .enqueue_action_raw(
                "channel".into(),
                "/tinkle.v1.ChannelsService/EditPhoto".into(),
                raw.clone(),
            )
            .await
            .unwrap();
        assert_eq!(channel.payload, raw);
        client
            .sending()
            .enqueue_profile_photo("profile".into(), profile())
            .await
            .unwrap();
        client
            .sending()
            .enqueue_create_asset(
                "create".into(),
                p::CreateStickerAssetRequest {
                    kind: 1,
                    source: asset(),
                    request_id: UUID.into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        client
            .sending()
            .enqueue_update_asset(
                "update".into(),
                p::UpdateStickerAssetRequest {
                    asset_id: 31,
                    source: asset(),
                    expected_version: 5,
                    request_id: UUID.into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(calls.lock().unwrap().is_empty());
        client.connect().await.unwrap();
        for id in ["chat", "channel", "profile", "create", "update"] {
            assert_eq!(
                client
                    .sending()
                    .resume(id.into(), SendRetry::Never, None)
                    .await
                    .unwrap()
                    .state,
                S::ActionPending
            );
        }
        phase.store(2, Ordering::SeqCst);
        macro_rules! cancel_owner {
            ($method:ident,$id:literal) => {
                assert_eq!(
                    client
                        .sending()
                        .$method($id.into(), None)
                        .await
                        .unwrap()
                        .state,
                    S::ActionProcessing
                )
            };
        }
        cancel_owner!(cancel_chat_photo_update, "chat");
        cancel_owner!(cancel_channel_photo_update, "channel");
        cancel_owner!(cancel_profile_photo_update, "profile");
        cancel_owner!(cancel_asset_change, "create");
        cancel_owner!(cancel_asset_change, "update");
        phase.store(terminal, Ordering::SeqCst);
        for id in ["chat", "channel", "profile", "create", "update"] {
            let j = client
                .sending()
                .resume(id.into(), SendRetry::ExactKeyedRequest, None)
                .await
                .unwrap();
            assert_eq!(
                j.state,
                match terminal {
                    3 => S::ActionSucceeded,
                    4 => S::ActionFailed,
                    _ => S::ActionCanceled,
                }
            );
        }
        assert_eq!(
            calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(m, _)| !m.contains("/Get") && !m.contains("/Cancel"))
                .count(),
            5
        );
        let old = client.sending().get("profile".into()).await.unwrap();
        let mut changed = profile();
        changed.expected_revision = 8;
        assert!(client
            .sending()
            .enqueue_profile_photo("profile".into(), changed)
            .await
            .is_err());
        assert_eq!(
            client
                .sending()
                .get("profile".into())
                .await
                .unwrap()
                .payload,
            old.payload
        );
        client.close().await;
        server.await.unwrap();
    }
}

fn send_status(keys: &[i64], state: i32, withheld: bool) -> p::SendStatus {
    p::SendStatus {
        target: target(),
        idempotency_key: keys[0],
        state,
        revision: if state == 1 { 1 } else { 2 },
        failure: if state == 4 { 1 } else { 0 },
        accepted: if state == 3 {
            keys.iter()
                .enumerate()
                .map(|(i, key)| p::SendReceipt {
                    idempotency_key: *key,
                    message_id: (!withheld).then_some(10 + i as i64),
                    album_id: (keys.len() > 1 && !withheld).then_some(700),
                    reference_withheld: Some(withheld),
                })
                .collect()
        } else {
            vec![]
        },
        retain_until: deadline(state),
    }
}

/// A lost response survives restart; receipt order and current-body reads never resurrect old media.
#[tokio::test]
async fn lost_album_and_single_send_recover_current_bodies_and_all_ordered_receipts() {
    for album in [false, true] {
        for withheld in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("cache.sqlite");
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let config = support::config(listener.local_addr().unwrap());
            let calls = Arc::new(Mutex::new(Vec::new()));
            let recorded = calls.clone();
            let keys = if album {
                vec![i64::MIN, 19]
            } else {
                vec![i64::MIN]
            };
            let server_keys = keys.clone();
            let server = tokio::spawn(async move {
                for attempt in 0..2 {
                    let (stream, _) = listener.accept().await.unwrap();
                    let keys = server_keys.clone();
                    let calls = recorded.clone();
                    let handle = support::accept(stream, move |r, _| {
                        let keys = keys.clone();
                        let calls = calls.clone();
                        async move {
                            calls.lock().unwrap().push(r.method.clone());
                            if attempt == 0 {
                                assert!(r.method.ends_with(if album {
                                    "/SendAlbum"
                                } else {
                                    "/SendMedia"
                                }));
                                return reply(vec![]);
                            }
                            if r.method.ends_with("/GetSendStatus") {
                                let q =
                                    p::GetSendStatusRequest::decode(r.payload.as_slice()).unwrap();
                                assert_eq!(q.target, target());
                                assert!(keys.contains(&q.idempotency_key));
                                return reply(
                                    p::GetSendStatusResponse {
                                        send_status: Some(send_status(&keys, 3, withheld)),
                                        ..Default::default()
                                    }
                                    .encode_to_vec(),
                                );
                            }
                            assert!(r.method.ends_with("/GetMessages"));
                            let q = p::GetMessagesRequest::decode(r.payload.as_slice()).unwrap();
                            assert_eq!(
                                q.message_ids,
                                (0..keys.len()).map(|i| 10 + i as i64).collect::<Vec<_>>()
                            );
                            reply(
                                p::GetMessagesResponse {
                                    messages: q
                                        .message_ids
                                        .iter()
                                        .enumerate()
                                        .map(|(i, id)| p::Message {
                                            message_id: *id,
                                            target: target(),
                                            from_id: 42,
                                            accept_seq: Some(i as i64 + 1),
                                            body_revision: Some(9),
                                            message: "edited elsewhere".into(),
                                            media: Some(p::Media {
                                                media: Some(p::media::Media::Photo(p::Photo {
                                                    file_id: 99,
                                                    representation_revision: Some(1),
                                                    ..Default::default()
                                                })),
                                            }),
                                            ..Default::default()
                                        })
                                        .collect(),
                                    ..Default::default()
                                }
                                .encode_to_vec(),
                            )
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
            let storage = Arc::new(Storage::open(path.to_str().unwrap()).unwrap());
            let client = Client::new_sqlite(config.clone(), support::source(), storage.clone());
            client.set_principal_user_id(42).await;
            client.connect().await.unwrap();
            let request = if album {
                OutgoingRequest::Album(p::SendAlbumRequest {
                    target: target(),
                    items: keys
                        .iter()
                        .map(|key| p::AlbumItem {
                            idempotency_key: *key,
                            media: source(9),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
            } else {
                OutgoingRequest::Media(p::SendMediaRequest {
                    target: target(),
                    idempotency_key: keys[0],
                    media: source(9),
                    ..Default::default()
                })
            };
            let original = client
                .sending()
                .enqueue("send".into(), request)
                .await
                .unwrap();
            assert!(client
                .sending()
                .resume("send".into(), SendRetry::Never, None)
                .await
                .is_err());
            assert_eq!(
                client.sending().get("send".into()).await.unwrap().state,
                S::ActionDispatching
            );
            client.close().await;
            drop(client);
            drop(storage);
            let storage = Arc::new(Storage::open(path.to_str().unwrap()).unwrap());
            let client = Client::new_sqlite(config, support::source(), storage.clone());
            client.set_principal_user_id(42).await;
            client.connect().await.unwrap();
            let settled = client
                .sending()
                .get_send_status("send".into(), None)
                .await
                .unwrap();
            assert_eq!(settled.state, S::ActionSucceeded);
            assert_eq!(settled.payload, original.payload);
            let s = client
                .sending()
                .retained_send_status("send".into())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                s.accepted
                    .iter()
                    .map(|r| r.idempotency_key)
                    .collect::<Vec<_>>(),
                keys
            );
            let q = client
                .messages()
                .get_send_status(
                    p::GetSendStatusRequest {
                        target: target(),
                        idempotency_key: *keys.last().unwrap(),
                    },
                    None,
                )
                .await
                .unwrap()
                .send_status
                .unwrap();
            assert_eq!(q.idempotency_key, keys[0]);
            assert_eq!(q.accepted, s.accepted);
            let messages = storage
                .get_messages("user:42", "target:room:7", 10, None)
                .unwrap();
            assert_eq!(messages.len(), if withheld { 0 } else { keys.len() });
            for row in messages {
                let body = p::Message::decode(row.proto.as_slice()).unwrap();
                assert_eq!(body.message, "edited elsewhere");
                assert!(
                    matches!(body.media.unwrap().media,Some(p::media::Media::Photo(p))if p.file_id==99)
                );
            }
            assert_eq!(
                calls
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|m| m.ends_with("/SendMedia") || m.ends_with("/SendAlbum"))
                    .count(),
                1
            );
            client.close().await;
            server.await.unwrap();
        }
    }
}

/// Invalid UUIDs, zero numeric keys, recursive unknown fields and mismatched owner methods consume no job.
#[tokio::test]
async fn invalid_domain_inputs_and_unknown_fields_never_create_jobs() {
    let storage = Arc::new(Storage::open_in_memory().unwrap());
    let client = Client::new_sqlite(
        support::config("127.0.0.1:1".parse().unwrap()),
        support::source(),
        storage,
    );
    client.set_principal_user_id(42).await;
    let mut r = profile();
    r.request_id = UUID.to_uppercase(); // This UUID has only digits; use an uppercase hexadecimal identifier explicitly.
    r.request_id = "AAAAAAAA-1111-4111-8111-111111111111".into();
    assert!(client
        .sending()
        .enqueue_profile_photo("bad".into(), r)
        .await
        .is_err());
    assert!(client
        .sending()
        .enqueue_chat_photo(
            "bad".into(),
            p::EditChatPhotoRequest {
                chat_id: 7,
                operation_id: 0,
                photo: photo()
            }
        )
        .await
        .is_err());
    let mut raw = profile().encode_to_vec();
    raw.extend([0x98, 0x06, 1]);
    assert!(client
        .sending()
        .enqueue_action_raw(
            "bad".into(),
            "/tinkle.v1.AccountService/SetProfilePhoto".into(),
            raw
        )
        .await
        .is_err());
    assert!(client.sending().jobs().await.unwrap().is_empty());
    client.close().await;
}

/// Current withheld responses stay withheld, while private remembered IDs reject later identity swaps.
#[tokio::test]
async fn accepted_identity_is_immutable_across_withholding_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("cache.sqlite");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let phase = Arc::new(AtomicI32::new(0));
    let remote = phase.clone();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let remote = remote.clone();
            let handle = support::accept(stream, move |r, _| {
                let phase = remote.clone();
                async move {
                    if r.method.ends_with("/GetMessages") {
                        return reply(p::GetMessagesResponse::default().encode_to_vec());
                    }
                    let mode = phase.load(Ordering::SeqCst);
                    let mut s = send_status(&[19], 3, mode == 1);
                    if mode == 2 {
                        s.accepted[0].message_id = Some(99);
                    }
                    if r.method.ends_with("/SendMedia") {
                        reply(
                            p::SendMediaResponse {
                                send_status: Some(s),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                        )
                    } else {
                        assert!(r.method.ends_with("/GetSendStatus"));
                        reply(
                            p::GetSendStatusResponse {
                                send_status: Some(s),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                        )
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
    let db = Arc::new(Storage::open(path.to_str().unwrap()).unwrap());
    let c = Client::new_sqlite(config.clone(), support::source(), db);
    c.set_principal_user_id(42).await;
    c.connect().await.unwrap();
    c.sending()
        .enqueue(
            "send".into(),
            OutgoingRequest::Media(p::SendMediaRequest {
                target: target(),
                idempotency_key: 19,
                media: source(9),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        c.sending()
            .resume("send".into(), SendRetry::Never, None)
            .await
            .unwrap()
            .state,
        S::ActionSucceeded
    );
    c.close().await;
    drop(c);
    let db = Arc::new(Storage::open(path.to_str().unwrap()).unwrap());
    let c = Client::new_sqlite(config, support::source(), db);
    c.set_principal_user_id(42).await;
    c.connect().await.unwrap();
    phase.store(1, Ordering::SeqCst);
    c.sending()
        .get_send_status("send".into(), None)
        .await
        .unwrap();
    let hidden = c
        .sending()
        .retained_send_status("send".into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hidden.accepted[0].message_id, None);
    assert_eq!(hidden.accepted[0].reference_withheld, Some(true));
    phase.store(2, Ordering::SeqCst);
    assert!(matches!(
        c.sending().get_send_status("send".into(), None).await,
        Err(libtinkle::SendError::Conflict)
    ));
    assert_eq!(
        c.sending()
            .retained_send_status("send".into())
            .await
            .unwrap(),
        Some(hidden)
    );
    c.close().await;
    server.await.unwrap();
}

/// Individually valid partial disclosures must not poison durable evidence when combined.
#[tokio::test]
async fn alternating_album_disclosures_validate_merged_evidence_before_commit() {
    for conflict in ["duplicate-message", "different-album", "none"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = support::config(listener.local_addr().unwrap());
        let phase = Arc::new(AtomicI32::new(0));
        let remote = phase.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let handle = support::accept(stream, move |r, _| {
                let phase = remote.clone();
                async move {
                    if r.method.ends_with("/GetMessages") {
                        return reply(p::GetMessagesResponse::default().encode_to_vec());
                    }
                    let mode = phase.load(Ordering::SeqCst);
                    let mut s = send_status(&[19, 20], 3, true);
                    let slot = usize::from(mode == 1);
                    s.accepted[slot].reference_withheld = Some(false);
                    s.accepted[slot].message_id =
                        Some(if slot == 1 && conflict == "duplicate-message" {
                            10
                        } else {
                            10 + slot as i64
                        });
                    s.accepted[slot].album_id =
                        Some(if slot == 1 && conflict == "different-album" {
                            701
                        } else {
                            700
                        });
                    if r.method.ends_with("/SendAlbum") {
                        reply(
                            p::SendAlbumResponse {
                                send_status: Some(s),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                        )
                    } else {
                        assert!(r.method.ends_with("/GetSendStatus"));
                        reply(
                            p::GetSendStatusResponse {
                                send_status: Some(s),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                        )
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
        let db = Arc::new(Storage::open_in_memory().unwrap());
        let c = Client::new_sqlite(config, support::source(), db.clone());
        c.set_principal_user_id(42).await;
        c.connect().await.unwrap();
        c.sending()
            .enqueue(
                "album".into(),
                OutgoingRequest::Album(p::SendAlbumRequest {
                    target: target(),
                    items: [19, 20]
                        .into_iter()
                        .map(|idempotency_key| p::AlbumItem {
                            idempotency_key,
                            media: source(9),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        let old = c
            .sending()
            .resume("album".into(), SendRetry::Never, None)
            .await
            .unwrap();
        let evidence = db
            .outgoing_action_evidence("user:42", &old.source, "album")
            .unwrap();
        phase.store(1, Ordering::SeqCst);
        let result = c.sending().get_send_status("album".into(), None).await;
        if conflict == "none" {
            let job = result.unwrap();
            let s = p::SendStatus::decode(
                db.outgoing_action_evidence("user:42", &job.source, "album")
                    .unwrap()
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            assert_eq!(
                s.accepted.iter().map(|r| r.message_id).collect::<Vec<_>>(),
                [Some(10), Some(11)]
            );
            // Private memory never overrides the current disclosure decision.
            let current = c
                .sending()
                .retained_send_status("album".into())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(current.accepted[0].reference_withheld, Some(true));
        } else {
            assert!(
                matches!(result, Err(libtinkle::SendError::InvalidRequest)),
                "{conflict}: {result:?}"
            );
            let retained = c.sending().get("album".into()).await.unwrap();
            assert_eq!(retained.revision, old.revision);
            assert_eq!(retained.response, old.response);
            assert_eq!(
                db.outgoing_action_evidence("user:42", &old.source, "album")
                    .unwrap(),
                evidence
            );
        }
        // A later consistent disclosure still succeeds; rejected evidence cannot poison recovery.
        phase.store(2, Ordering::SeqCst);
        c.sending()
            .get_send_status("album".into(), None)
            .await
            .unwrap();
        c.close().await;
        server.await.unwrap();
    }
}

/// A wire capacity error can follow owner admission; status failures never authorize resubmission.
#[tokio::test]
async fn resource_exhausted_keeps_submission_uncertain_and_recovery_read_only() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let phase = Arc::new(AtomicI32::new(0));
    let remote = phase.clone();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |r, _| {
            let phase = remote.clone();
            let calls = recorded.clone();
            async move {
                calls.lock().unwrap().push(r.method.clone());
                if phase.load(Ordering::SeqCst) == 0 {
                    return syntra_core::proto::Frame {
                        body: Some(syntra_core::proto::frame::Body::Result(
                            syntra_core::proto::RpcResult {
                                outcome: Some(syntra_core::proto::rpc_result::Outcome::Error(
                                    syntra_core::proto::RpcError {
                                        code: syntra_core::ErrorCode::ResourceExhausted as i32,
                                        message: "response exceeds native transport budget".into(),
                                    },
                                )),
                            },
                        )),
                        ..Default::default()
                    };
                }
                assert!(r.method.ends_with("/GetSendStatus"));
                reply(
                    p::GetSendStatusResponse {
                        send_status: Some(send_status(&[19], 3, true)),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
            }
        })
        .await
        .unwrap();
        while !handle.is_closed() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        handle.close().await;
    });
    let c = Client::new_sqlite(
        config,
        support::source(),
        Arc::new(Storage::open_in_memory().unwrap()),
    );
    c.set_principal_user_id(42).await;
    c.connect().await.unwrap();
    c.sending()
        .enqueue(
            "send".into(),
            OutgoingRequest::Media(p::SendMediaRequest {
                target: target(),
                idempotency_key: 19,
                media: source(9),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert!(c
        .sending()
        .resume("send".into(), SendRetry::Never, None)
        .await
        .is_err());
    let uncertain = c.sending().get("send".into()).await.unwrap();
    assert_eq!(uncertain.state, S::ActionUnconfirmed);
    assert!(uncertain.last_outcome.as_ref().unwrap().unknown_outcome);
    assert_eq!(
        uncertain.last_outcome.as_ref().unwrap().code,
        syntra_core::ErrorCode::ResourceExhausted as i32
    );
    assert!(c
        .sending()
        .resume("send".into(), SendRetry::ExactKeyedRequest, None)
        .await
        .is_err());
    let retained = c.sending().get("send".into()).await.unwrap();
    assert_eq!(retained.revision, uncertain.revision);
    assert_eq!(retained.last_outcome, uncertain.last_outcome);
    phase.store(1, Ordering::SeqCst);
    assert_eq!(
        c.sending()
            .resume("send".into(), SendRetry::ExactKeyedRequest, None)
            .await
            .unwrap()
            .state,
        S::ActionSucceeded
    );
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.ends_with("/SendMedia"))
            .count(),
        1
    );
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.ends_with("/GetSendStatus"))
            .count(),
        2
    );
    c.close().await;
    server.await.unwrap();
}

/// Revalidated history has no tombstone for an unknown accepted ID; an old job cannot adopt it.
#[tokio::test]
async fn recovery_retains_success_without_hydrating_across_original_history_boundary() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |r, _| async move {
            if r.method.ends_with("/SendMedia") {
                return reply(
                    p::SendMediaResponse {
                        send_status: Some(send_status(&[19], 1, false)),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                );
            }
            if r.method.ends_with("/GetSendStatus") {
                return reply(
                    p::GetSendStatusResponse {
                        send_status: Some(send_status(&[19], 3, false)),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                );
            }
            assert!(r.method.ends_with("/GetMessages"));
            reply(
                p::GetMessagesResponse {
                    messages: vec![p::Message {
                        message_id: 10,
                        target: target(),
                        from_id: 42,
                        accept_seq: Some(1),
                        message: "old accepted body".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }
                .encode_to_vec(),
            )
        })
        .await
        .unwrap();
        while !handle.is_closed() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        handle.close().await;
    });
    let db = Arc::new(Storage::open_in_memory().unwrap());
    let c = Client::new_sqlite(config, support::source(), db.clone());
    c.set_principal_user_id(42).await;
    c.connect().await.unwrap();
    c.sending()
        .enqueue(
            "send".into(),
            OutgoingRequest::Media(p::SendMediaRequest {
                target: target(),
                idempotency_key: 19,
                media: source(9),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let pending = c
        .sending()
        .resume("send".into(), SendRetry::Never, None)
        .await
        .unwrap();
    db.mark_rebuild("user:42", "target:room:7").unwrap();
    let generation = db
        .install_snapshot("user:42", "target:room:7", 5, 0, None)
        .unwrap();
    assert_ne!(generation, pending.cache_generation);
    assert!(matches!(
        c.sending().get_send_status("send".into(), None).await,
        Err(libtinkle::SendError::ScopeChanged)
    ));
    let accepted = c.sending().get("send".into()).await.unwrap();
    assert_eq!(accepted.state, S::ActionSucceeded);
    assert_eq!(accepted.cache_generation, pending.cache_generation);
    assert!(db
        .get_messages("user:42", "target:room:7", 10, None)
        .unwrap()
        .is_empty());
    assert_eq!(
        db.load_journal("user:42", "target:room:7").unwrap(),
        Some(5)
    );
    c.close().await;
    server.await.unwrap();
}

/// A status fetched for a retired account cannot update its pending row or leak into the new account.
#[tokio::test]
async fn delayed_action_status_is_fenced_by_account_replacement() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let e = entered.clone();
    let done = release.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |r, _| {
            let entered = e.clone();
            let release = done.clone();
            async move {
                if r.method.ends_with("/SendMedia") {
                    return reply(
                        p::SendMediaResponse {
                            send_status: Some(send_status(&[19], 1, false)),
                            ..Default::default()
                        }
                        .encode_to_vec(),
                    );
                }
                assert!(r.method.ends_with("/GetSendStatus"));
                entered.notify_one();
                release.notified().await;
                reply(
                    p::GetSendStatusResponse {
                        send_status: Some(send_status(&[19], 3, true)),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
            }
        })
        .await
        .unwrap();
        while !handle.is_closed() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        handle.close().await;
    });
    let db = Arc::new(Storage::open_in_memory().unwrap());
    let c = Client::new_sqlite(config, support::source(), db.clone());
    c.set_principal_user_id(42).await;
    c.connect().await.unwrap();
    c.sending()
        .enqueue(
            "send".into(),
            OutgoingRequest::Media(p::SendMediaRequest {
                target: target(),
                idempotency_key: 19,
                media: source(9),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let old = c
        .sending()
        .resume("send".into(), SendRetry::Never, None)
        .await
        .unwrap();
    let query = c.clone();
    let waiter =
        tokio::spawn(async move { query.sending().get_send_status("send".into(), None).await });
    entered.notified().await;
    c.set_principal_user_id(43).await;
    release.notify_one();
    assert!(waiter.await.unwrap().is_err());
    assert!(c.sending().jobs().await.unwrap().is_empty());
    assert_eq!(
        db.outgoing("user:42", &old.source, "send")
            .unwrap()
            .unwrap()
            .revision,
        old.revision
    );
    c.close().await;
    server.await.unwrap();
}

/// Every snapshot is a domain response; notification bytes contain demand only.
struct HintCase {
    submit: &'static str,
    query: &'static str,
    request: Vec<u8>,
    selector: Vec<u8>,
    submitted_pending: Vec<u8>,
    queried_pending: Vec<u8>,
    queried_success: Vec<u8>,
    hint: Vec<u8>,
}

fn action_hint_cases() -> Vec<HintCase> {
    let mut cases = Vec::new();
    macro_rules! case {
        ($submit:literal,$query:literal,$request:expr,$selector:expr,$response:ty,$lookup:ty,$field:ident,$snapshot:expr,$hint:ident,$notification:expr) => {{
            let mut submitted = <$response>::default();
            submitted.$field = Some(($snapshot)(1));
            let mut pending = <$lookup>::default();
            pending.$field = Some(($snapshot)(1));
            let mut success = <$lookup>::default();
            success.$field = Some(($snapshot)(3));
            cases.push(HintCase {
                submit: $submit,
                query: $query,
                request: $request.encode_to_vec(),
                selector: $selector.encode_to_vec(),
                submitted_pending: submitted.encode_to_vec(),
                queried_pending: pending.encode_to_vec(),
                queried_success: success.encode_to_vec(),
                hint: prost_types::Any {
                    type_url: concat!("type.googleapis.com/tinkle.v1.", stringify!($hint)).into(),
                    value: $notification.encode_to_vec(),
                }
                .encode_to_vec(),
            });
        }};
    }
    case!(
        "/tinkle.v1.MessagesService/SendMedia",
        "/tinkle.v1.MessagesService/GetSendStatus",
        p::SendMediaRequest {
            target: target(),
            media: source(9),
            idempotency_key: i64::MIN,
            ..Default::default()
        },
        p::GetSendStatusRequest {
            target: target(),
            idempotency_key: i64::MIN
        },
        p::SendMediaResponse,
        p::GetSendStatusResponse,
        send_status,
        |state| send_status(&[i64::MIN], state, true),
        SendStatusChanged,
        p::SendStatusChanged {
            target: target(),
            idempotency_key: i64::MIN,
            revision: 999
        }
    );
    case!(
        "/tinkle.v1.MessagesService/SendAlbum",
        "/tinkle.v1.MessagesService/GetSendStatus",
        p::SendAlbumRequest {
            target: target(),
            items: [i64::MAX, -1]
                .into_iter()
                .map(|idempotency_key| p::AlbumItem {
                    idempotency_key,
                    media: source(9),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        p::GetSendStatusRequest {
            target: target(),
            idempotency_key: i64::MAX
        },
        p::SendAlbumResponse,
        p::GetSendStatusResponse,
        send_status,
        |state| send_status(&[i64::MAX, -1], state, true),
        SendStatusChanged,
        p::SendStatusChanged {
            target: target(),
            idempotency_key: i64::MAX,
            revision: 999
        }
    );
    case!(
        "/tinkle.v1.MessagesService/EditChatPhoto",
        "/tinkle.v1.MessagesService/GetChatPhotoUpdate",
        p::EditChatPhotoRequest {
            chat_id: 7,
            photo: photo(),
            operation_id: 19
        },
        p::GetChatPhotoUpdateRequest {
            chat_id: 7,
            operation_id: 19
        },
        p::EditChatPhotoResponse,
        p::GetChatPhotoUpdateResponse,
        photo_update,
        |state| p::ChatPhotoUpdate {
            chat_id: 7,
            operation_id: 19,
            state,
            revision: if state == 1 { 1 } else { 2 },
            reference_withheld: (state == 3).then_some(true),
            retain_until: deadline(state),
            ..Default::default()
        },
        ChatPhotoUpdateChanged,
        p::ChatPhotoUpdateChanged {
            chat_id: 7,
            operation_id: 19,
            revision: 999
        }
    );
    case!(
        "/tinkle.v1.ChannelsService/EditPhoto",
        "/tinkle.v1.ChannelsService/GetPhotoUpdate",
        p::EditPhotoRequest {
            channel_id: 7,
            photo: photo(),
            request_id: UUID.into()
        },
        p::GetPhotoUpdateRequest {
            channel_id: 7,
            request_id: UUID.into()
        },
        p::EditPhotoResponse,
        p::GetPhotoUpdateResponse,
        photo_update,
        |state| p::ChannelPhotoUpdate {
            channel_id: 7,
            request_id: UUID.into(),
            state,
            revision: if state == 1 { 1 } else { 2 },
            reference_withheld: (state == 3).then_some(true),
            retain_until: deadline(state),
            ..Default::default()
        },
        ChannelPhotoUpdateChanged,
        p::ChannelPhotoUpdateChanged {
            channel_id: 7,
            request_id: UUID.into(),
            revision: 999
        }
    );
    case!(
        "/tinkle.v1.AccountService/SetProfilePhoto",
        "/tinkle.v1.AccountService/GetProfilePhotoUpdate",
        profile(),
        p::GetProfilePhotoUpdateRequest {
            request_id: UUID.into()
        },
        p::SetProfilePhotoResponse,
        p::GetProfilePhotoUpdateResponse,
        photo_update,
        |state| p::ProfilePhotoUpdate {
            request_id: UUID.into(),
            state,
            revision: if state == 1 { 1 } else { 2 },
            gallery_revision: if state == 3 { 8 } else { 0 },
            reference_withheld: (state == 3).then_some(true),
            retain_until: deadline(state),
            ..Default::default()
        },
        ProfilePhotoUpdateChanged,
        p::ProfilePhotoUpdateChanged {
            request_id: UUID.into(),
            revision: 999
        }
    );
    for update in [false, true] {
        let request_id = if update {
            "33333333-3333-4333-8333-333333333333"
        } else {
            UUID
        };
        let snapshot = |state| p::AssetChange {
            request_id: request_id.into(),
            asset_id: update.then_some(31),
            state,
            revision: if state == 1 { 1 } else { 2 },
            reference_withheld: (state == 3).then_some(true),
            retain_until: deadline(state),
            ..Default::default()
        };
        let notification = p::AssetChangeChanged {
            request_id: request_id.into(),
            asset_id: update.then_some(31),
            revision: 999,
        };
        let selector = p::GetAssetChangeRequest {
            request_id: request_id.into(),
            asset_id: update.then_some(31),
        };
        if update {
            case!(
                "/tinkle.v1.StickersService/UpdateStickerAsset",
                "/tinkle.v1.StickersService/GetAssetChange",
                p::UpdateStickerAssetRequest {
                    request_id: request_id.into(),
                    asset_id: 31,
                    expected_version: 5,
                    source: asset(),
                    ..Default::default()
                },
                selector,
                p::UpdateStickerAssetResponse,
                p::GetAssetChangeResponse,
                asset_change,
                snapshot,
                AssetChangeChanged,
                notification
            );
        } else {
            case!(
                "/tinkle.v1.StickersService/CreateStickerAsset",
                "/tinkle.v1.StickersService/GetAssetChange",
                p::CreateStickerAssetRequest {
                    request_id: request_id.into(),
                    kind: 1,
                    source: asset(),
                    ..Default::default()
                },
                selector,
                p::CreateStickerAssetResponse,
                p::GetAssetChangeResponse,
                asset_change,
                snapshot,
                AssetChangeChanged,
                notification
            );
        }
    }
    cases
}

/// In-flight hints require another read. Restart refreshes photo/asset actions, but does not
/// poll settled sends with no remaining hydration, or retransmit any mutation.
#[tokio::test]
async fn status_pushes_redemand_inflight_reads_and_restart_skips_settled_sends() {
    use std::sync::atomic::AtomicUsize;
    use syntra_core::secure_channel::{self, Mode, Secret32};
    use syntra_runtime::{ConnectionConfig, FnHandler, HandlerOutcome};
    struct Keys;
    impl secure_channel::KeyStore for Keys {
        fn lookup(&self, c: &secure_channel::Context) -> Result<Secret32, secure_channel::Error> {
            if c.epoch != 1 || c.handle != [8; 8] {
                return Err(secure_channel::Error::Authentication);
            }
            Ok(Secret32::new([9; 32]))
        }
    }
    let cases = Arc::new(action_hint_cases());
    let queries = Arc::new(
        (0..cases.len())
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>(),
    );
    let submits = Arc::new(AtomicUsize::new(0));
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::channel(8);
    let release = Arc::new(tokio::sync::Notify::new());
    let (host_tx, mut host_rx) = tokio::sync::mpsc::channel(2);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let data = cases.clone();
    let observed = queries.clone();
    let mutations = submits.clone();
    let gate = release.clone();
    let server = tokio::spawn(async move {
        for connection in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let data = data.clone();
            let observed = observed.clone();
            let mutations = mutations.clone();
            let gate = gate.clone();
            let entered = entered_tx.clone();
            let mut security = support::server_security();
            security.mode = Mode::Returning;
            security.store = Some(Arc::new(Keys));
            let handler = Arc::new(FnHandler(move |method: String, payload: Vec<u8>, _| {
                let data = data.clone();
                let observed = observed.clone();
                let mutations = mutations.clone();
                let gate = gate.clone();
                let entered = entered.clone();
                async move {
                    let wrapped = method.ends_with("/InitConnect");
                    let (method, payload) = if wrapped {
                        let r = p::InitConnectRequest::decode(payload.as_slice()).unwrap();
                        (r.method, r.payload.to_vec())
                    } else {
                        (method, payload)
                    };
                    let bytes = if method.ends_with("/GetSessions") {
                        p::GetSessionsResponse::default().encode_to_vec()
                    } else if method.ends_with("/KeepAlive") {
                        p::KeepAliveResponse {
                            sequence: p::KeepAliveRequest::decode(payload.as_slice())
                                .unwrap()
                                .sequence,
                        }
                        .encode_to_vec()
                    } else if let Some(c) = data
                        .iter()
                        .find(|c| c.submit == method && c.request == payload)
                    {
                        assert_eq!(connection, 0);
                        mutations.fetch_add(1, Ordering::SeqCst);
                        c.submitted_pending.clone()
                    } else {
                        let (index, c) = data
                            .iter()
                            .enumerate()
                            .find(|(_, c)| c.query == method && c.selector == payload)
                            .unwrap();
                        let count = observed[index].fetch_add(1, Ordering::SeqCst);
                        if connection == 0 && count == 0 {
                            entered.send(index).await.unwrap();
                            gate.notified().await;
                            c.queried_pending.clone()
                        } else {
                            c.queried_success.clone()
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
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            handle.close().await;
        }
    });
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("cache.sqlite");
    let enrollment = p::DeviceEnrollmentStatus {
        operation_id: UUID.into(),
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
    let source = || libtinkle::TrustSource::Returning {
        trust: support::trust(),
        enrollment: libtinkle::Enrollment::from_active_status([9; 32], &enrollment).unwrap(),
    };
    let storage = Arc::new(Storage::open(path.to_str().unwrap()).unwrap());
    let client = Client::new_sqlite(config.clone(), source(), storage.clone());
    client.set_principal_user_id(42).await;
    client.connect().await.unwrap();
    let host = host_rx.recv().await.unwrap();
    for (i, c) in cases.iter().enumerate() {
        let id = format!("hint-{i}");
        if c.submit.ends_with("/SendMedia") {
            client
                .sending()
                .enqueue(
                    id.clone(),
                    OutgoingRequest::Media(
                        p::SendMediaRequest::decode(c.request.as_slice()).unwrap(),
                    ),
                )
                .await
                .unwrap();
        } else if c.submit.ends_with("/SendAlbum") {
            client
                .sending()
                .enqueue(
                    id.clone(),
                    OutgoingRequest::Album(
                        p::SendAlbumRequest::decode(c.request.as_slice()).unwrap(),
                    ),
                )
                .await
                .unwrap();
        } else {
            client
                .sending()
                .enqueue_action_raw(id.clone(), c.submit.into(), c.request.clone())
                .await
                .unwrap();
        }
        assert_eq!(
            client
                .sending()
                .resume(id, SendRetry::Never, None)
                .await
                .unwrap()
                .state,
            S::ActionPending
        );
    }
    for (i, c) in cases.iter().enumerate() {
        let id = format!("hint-{i}");
        let query = client.clone();
        let key = id.clone();
        let waiter =
            tokio::spawn(async move { query.sending().resume(key, SendRetry::Never, None).await });
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx.recv())
                .await
                .unwrap(),
            Some(i)
        );
        host.notify(c.hint.clone()).await.unwrap();
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), client.next_event())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event,
            libtinkle::SyncEvent::SendStatusChanged(_)
                | libtinkle::SyncEvent::ChatPhotoUpdateChanged(_)
                | libtinkle::SyncEvent::ChannelPhotoUpdateChanged(_)
                | libtinkle::SyncEvent::ProfilePhotoUpdateChanged(_)
                | libtinkle::SyncEvent::AssetChangeChanged(_)
        ));
        assert_eq!(
            client.sending().get(id.clone()).await.unwrap().state,
            S::ActionPending
        );
        release.notify_one();
        let completed = waiter.await.unwrap().unwrap();
        assert_eq!(completed.state, S::ActionSucceeded, "{}", c.submit);
        assert_eq!(queries[i].load(Ordering::SeqCst), 2);
    }
    client.close().await;
    drop(client);
    drop(storage);
    let client = Client::new_sqlite(
        config,
        source(),
        Arc::new(Storage::open(path.to_str().unwrap()).unwrap()),
    );
    client.set_principal_user_id(42).await;
    client.start(vec![]).await.unwrap();
    let _host = host_rx.recv().await.unwrap();
    let lookups = cases.iter().zip(queries.iter());
    tokio::time::timeout(std::time::Duration::from_secs(45), async {
        while lookups
            .clone()
            .filter(|(c, _)| !c.query.ends_with("/GetSendStatus"))
            .any(|(_, n)| n.load(Ordering::SeqCst) < 3)
        {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    for (c, n) in lookups.filter(|(c, _)| c.query.ends_with("/GetSendStatus")) {
        assert_eq!(
            n.load(Ordering::SeqCst),
            2,
            "{} was already settled",
            c.submit
        );
    }
    assert_eq!(submits.load(Ordering::SeqCst), cases.len());
    for j in client.sending().jobs().await.unwrap() {
        assert_eq!(j.state, S::ActionSucceeded);
    }
    client.close().await;
    server.await.unwrap();
}
