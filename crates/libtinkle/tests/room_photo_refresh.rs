//! Native candidate boundary tests for kindless, exact-source room-photo references.
#![cfg(tinkle_async_actions)]
mod support;
use libtinkle::{Client, SendError, Storage};
use prost::Message;
use std::sync::{Arc, Mutex};
use tinkle_schema::proto as p;

/// One globally identified room and its exact current photo representation.
fn room(kind: p::ChatType) -> p::Chat {
    p::Chat {
        chat_id: 7,
        title: "room".into(),
        chat_type: kind as i32,
        photo: Some(p::ChatPhoto {
            photo_id: 17,
            file_id: 99,
            representation_revision: Some(2),
            ..Default::default()
        }),
        ..Default::default()
    }
}
/// The reference never supplies a kind, a replacement photo, or a different main.
fn reference() -> p::FileAccessContext {
    p::FileAccessContext {
        reference: Some(p::file_access_context::Reference::ChatPhoto(
            p::ChatPhotoReference {
                chat_id: 7,
                photo_id: 17,
            },
        )),
    }
}
/// Keep errors typed across the encrypted native application boundary.
fn reply(result: Result<Vec<u8>, i32>) -> syntra_core::proto::Frame {
    use syntra_core::proto::{frame::Body, rpc_result::Outcome};
    syntra_core::proto::Frame {
        version: 1,
        request_id: 1,
        body: Some(Body::Result(syntra_core::proto::RpcResult {
            outcome: Some(match result {
                Ok(bytes) => Outcome::Payload(bytes),
                Err(code) => Outcome::Error(syntra_core::proto::RpcError {
                    code,
                    message: "fixture refusal".into(),
                }),
            }),
        })),
    }
}

/// Probe channels first; only an authoritative empty collection permits the independent group read.
#[tokio::test]
async fn kindless_photo_refresh_preserves_identity_and_never_bypasses_errors() {
    for mode in [
        "group",
        "channel",
        "broadcast",
        "missing",
        "channel_permission",
        "channel_unavailable",
        "channel_notfound",
        "channel_unimplemented",
        "group_permission",
        "group_unavailable",
        "wrong_channel_id",
        "wrong_channel_kind",
        "channel_photo_replaced",
        "channel_main_replaced",
        "channel_zero_revision",
        "channel_absent_revision",
        "channel_no_photo",
        "multiple_channels",
        "group_main_replaced",
        "group_photo_replaced",
        "wrong_group_id",
        "wrong_group_kind",
        "multiple_groups",
    ] {
        let groups = mode == "group"
            || mode == "missing"
            || mode.starts_with("group_")
            || mode.starts_with("wrong_group")
            || mode == "multiple_groups";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = support::config(listener.local_addr().unwrap());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let handle = support::accept(stream, move |request, _| {
                let recorded = recorded.clone();
                async move {
                    recorded.lock().unwrap().push(request.method.clone());
                    if request.method.ends_with("/GetChannels") {
                        assert_eq!(
                            p::GetChannelsRequest::decode(request.payload.as_slice())
                                .unwrap()
                                .channel_ids,
                            vec![7]
                        );
                        let error = match mode {
                            "channel_permission" => Some(7),
                            "channel_unavailable" => Some(14),
                            "channel_notfound" => Some(5),
                            "channel_unimplemented" => Some(12),
                            _ => None,
                        };
                        if let Some(code) = error {
                            return reply(Err(code));
                        }
                        let mut chat = room(if mode == "broadcast" {
                            p::ChatType::Broadcast
                        } else {
                            p::ChatType::Channel
                        });
                        match mode {
                            "wrong_channel_id" => chat.chat_id = 8,
                            "wrong_channel_kind" => chat.chat_type = p::ChatType::Group as i32,
                            "channel_photo_replaced" => chat.photo.as_mut().unwrap().photo_id = 18,
                            "channel_main_replaced" => chat.photo.as_mut().unwrap().file_id = 100,
                            "channel_zero_revision" => {
                                chat.photo.as_mut().unwrap().representation_revision = Some(0)
                            }
                            "channel_absent_revision" => {
                                chat.photo.as_mut().unwrap().representation_revision = None
                            }
                            "channel_no_photo" => chat.photo = None,
                            _ => {}
                        }
                        let chats = if groups {
                            vec![]
                        } else if mode == "multiple_channels" {
                            vec![chat.clone(), chat]
                        } else {
                            vec![chat]
                        };
                        return reply(Ok(p::GetChannelsResponse { chats }.encode_to_vec()));
                    }
                    assert!(request.method.ends_with("/GetChats"));
                    assert!(groups);
                    assert_eq!(
                        p::GetChatsRequest::decode(request.payload.as_slice())
                            .unwrap()
                            .chat_ids,
                        vec![7]
                    );
                    if mode == "group_permission" {
                        return reply(Err(7));
                    }
                    if mode == "group_unavailable" {
                        return reply(Err(14));
                    }
                    let mut chat = room(p::ChatType::Group);
                    match mode {
                        "wrong_group_id" => chat.chat_id = 8,
                        "wrong_group_kind" => chat.chat_type = p::ChatType::Channel as i32,
                        "group_main_replaced" => chat.photo.as_mut().unwrap().file_id = 100,
                        "group_photo_replaced" => chat.photo.as_mut().unwrap().photo_id = 18,
                        _ => {}
                    }
                    let chats = if mode == "missing" {
                        vec![]
                    } else if mode == "multiple_groups" {
                        vec![chat.clone(), chat]
                    } else {
                        vec![chat]
                    };
                    reply(Ok(p::GetChatsResponse { chats }.encode_to_vec()))
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
        let result = client.refresh_media_representation(reference(), 99).await;
        if matches!(mode, "group" | "channel" | "broadcast") {
            let result = result.unwrap_or_else(|e| panic!("{mode}: {e:?}"));
            assert_eq!(result.type_url, "type.googleapis.com/tinkle.v1.ChatPhoto");
            let photo = p::ChatPhoto::decode(result.value.as_slice()).unwrap();
            assert_eq!(
                (photo.photo_id, photo.file_id, photo.representation_revision),
                (17, 99, Some(2))
            );
            let retained = p::Chat::decode(
                storage
                    .get_chat("user:42", 7)
                    .unwrap()
                    .unwrap()
                    .proto
                    .as_slice(),
            )
            .unwrap();
            assert_eq!(retained.photo, Some(photo));
        } else {
            assert!(result.is_err(), "{mode}: {result:?}");
            assert!(storage.get_chat("user:42", 7).unwrap().is_none());
            let expected = match mode {
                "channel_permission" | "group_permission" => {
                    Some(syntra_core::ErrorCode::PermissionDenied)
                }
                "channel_unavailable" | "group_unavailable" => {
                    Some(syntra_core::ErrorCode::Unavailable)
                }
                "channel_notfound" => Some(syntra_core::ErrorCode::NotFound),
                "channel_unimplemented" => Some(syntra_core::ErrorCode::Unimplemented),
                _ => None,
            };
            if let Some(expected) = expected {
                assert!(
                    matches!(result,Err(SendError::Rpc(ref e)) if e.code()==expected),
                    "{mode}: {result:?}"
                );
            }
        }
        {
            let calls = calls.lock().unwrap();
            assert_eq!(calls.len(), if groups { 2 } else { 1 }, "{mode}: {calls:?}");
            assert!(calls[0].ends_with("/GetChannels"));
            if groups {
                assert!(calls[1].ends_with("/GetChats"));
            }
        }
        client.close().await;
        server.await.unwrap();
    }
}

/// A response held across a principal replacement or durable wipe cannot hydrate either account.
#[tokio::test]
async fn delayed_group_or_channel_photo_is_fenced_before_cache_publication() {
    for (channel, wipe) in [(false, false), (true, false), (false, true), (true, true)] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = support::config(listener.local_addr().unwrap());
        let entered = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let ready = entered.clone();
        let release = gate.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let handle = support::accept(stream, move |request, _| {
                let ready = ready.clone();
                let release = release.clone();
                async move {
                    let channels = request.method.ends_with("/GetChannels");
                    if channels && !channel {
                        return reply(Ok(p::GetChannelsResponse { chats: vec![] }.encode_to_vec()));
                    }
                    assert_eq!(channels, channel);
                    if !channels {
                        assert!(request.method.ends_with("/GetChats"));
                    }
                    ready.notify_one();
                    release.acquire().await.unwrap().forget();
                    reply(Ok(if channels {
                        p::GetChannelsResponse {
                            chats: vec![room(p::ChatType::Channel)],
                        }
                        .encode_to_vec()
                    } else {
                        p::GetChatsResponse {
                            chats: vec![room(p::ChatType::Group)],
                        }
                        .encode_to_vec()
                    }))
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
        let caller = client.clone();
        let task =
            tokio::spawn(async move { caller.refresh_media_representation(reference(), 99).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        if wipe {
            storage.clear_principal("user:42").unwrap();
        } else {
            client.set_principal_user_id(99).await;
        }
        gate.add_permits(1);
        let result = task.await.unwrap();
        assert!(
            result.is_err(),
            "channel={channel}, wipe={wipe}: {result:?}"
        );
        assert!(storage.get_chat("user:42", 7).unwrap().is_none());
        assert!(storage.get_chat("user:99", 7).unwrap().is_none());
        client.close().await;
        server.await.unwrap();
    }
}

/// Independent owner discovery cannot grant the second read a fresh configured maximum budget.
#[tokio::test]
async fn channel_probe_and_group_read_share_the_configured_lookup_budget() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = support::config(listener.local_addr().unwrap());
    config.connection.max_rpc_timeout = std::time::Duration::from_millis(120);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |request, _| {
            let recorded = recorded.clone();
            async move {
                recorded.lock().unwrap().push(request.method.clone());
                tokio::time::sleep(std::time::Duration::from_millis(90)).await;
                if request.method.ends_with("/GetChannels") {
                    reply(Ok(p::GetChannelsResponse { chats: vec![] }.encode_to_vec()))
                } else {
                    assert!(request.method.ends_with("/GetChats"));
                    reply(Ok(p::GetChatsResponse {
                        chats: vec![room(p::ChatType::Group)],
                    }
                    .encode_to_vec()))
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
    let result = client.refresh_media_representation(reference(), 99).await;
    assert!(
        matches!(result,Err(SendError::Rpc(ref e)) if e.code()==syntra_core::ErrorCode::DeadlineExceeded),
        "{result:?}"
    );
    assert!(storage.get_chat("user:42", 7).unwrap().is_none());
    assert!(calls.lock().unwrap().len() <= 2);
    client.close().await;
    server.await.unwrap();
}
