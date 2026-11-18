//! Exact owner-specific action fields and ordered send receipts.
#![cfg(tinkle_async_actions)]
use prost::Message;
use tinkle_schema::proto as p;

/// Compact candidate tags retain each action's owning-domain request and status fields.
#[test]
fn action_descriptors_use_the_owning_domain() {
    let descriptor = prost_types::FileDescriptorSet::decode(p::FILE_DESCRIPTOR_SET).unwrap();
    for (name, field, tag) in [
        ("SendMediaResponse", "send_status", 4),
        ("SendAlbumResponse", "send_status", 4),
        ("EditChatPhotoResponse", "photo_update", 2),
        ("EditPhotoResponse", "photo_update", 2),
        ("SetProfilePhotoResponse", "photo_update", 3),
        ("CreateStickerAssetResponse", "asset_change", 2),
        ("UpdateStickerAssetResponse", "asset_change", 2),
        ("EditPhotoRequest", "request_id", 3),
        ("SetProfilePhotoRequest", "request_id", 3),
        ("CreateStickerAssetRequest", "request_id", 4),
        ("UpdateStickerAssetRequest", "request_id", 5),
    ] {
        let message = descriptor
            .file
            .iter()
            .flat_map(|f| &f.message_type)
            .find(|m| m.name.as_deref() == Some(name))
            .unwrap();
        assert!(
            message
                .field
                .iter()
                .any(|f| f.name.as_deref() == Some(field) && f.number == Some(tag)),
            "{name}.{field}"
        );
    }
    assert_eq!(p::SendState::Processing as i32, 2);
    assert_eq!(p::SendState::Succeeded as i32, 3);
    assert_eq!(p::SendState::Failed as i32, 4);
    assert_eq!(p::SendState::Canceled as i32, 5);
}

/// A candidate descriptor must not reintroduce the retired send-outcome replay surface.
#[test]
fn send_recovery_has_no_feed_schema() {
    let descriptor = prost_types::FileDescriptorSet::decode(p::FILE_DESCRIPTOR_SET).unwrap();
    for file in descriptor.file {
        for message in file.message_type {
            assert!(!matches!(
                message.name.as_deref(),
                Some(
                    "ListSendUpdatesRequest"
                        | "ListSendUpdatesResponse"
                        | "SendOutcomeChanged"
                        | "SendUpdateSelector"
                        | "SendUpdateResolution"
                )
            ));
        }
        for service in file.service {
            assert!(service
                .method
                .iter()
                .all(|method| method.name.as_deref() != Some("ListSendUpdates")));
        }
    }
}

/// Each exact type URL validates its own selector and positive revision, never another domain.
#[test]
fn all_domain_hints_decode_and_reject_zero_revisions() {
    let id = "11111111-1111-4111-8111-111111111111".to_owned();
    let target = Some(p::Target {
        kind: Some(p::target::Kind::ChatId(7)),
    });
    let cases = [
        (
            "SendStatusChanged",
            p::SendStatusChanged {
                target,
                idempotency_key: i64::MIN,
                revision: 1,
            }
            .encode_to_vec(),
        ),
        (
            "ChatPhotoUpdateChanged",
            p::ChatPhotoUpdateChanged {
                chat_id: 7,
                operation_id: 9,
                revision: 1,
            }
            .encode_to_vec(),
        ),
        (
            "ChannelPhotoUpdateChanged",
            p::ChannelPhotoUpdateChanged {
                channel_id: 7,
                request_id: id.clone(),
                revision: 1,
            }
            .encode_to_vec(),
        ),
        (
            "ProfilePhotoUpdateChanged",
            p::ProfilePhotoUpdateChanged {
                request_id: id.clone(),
                revision: 1,
            }
            .encode_to_vec(),
        ),
        (
            "AssetChangeChanged",
            p::AssetChangeChanged {
                request_id: id,
                asset_id: Some(7),
                revision: 1,
            }
            .encode_to_vec(),
        ),
        (
            "ProfilePhotoRepresentationChanged",
            p::ProfilePhotoRepresentationChanged {
                user_id: 42,
                photo_id: 7,
                main_file_id: 9,
                representation_revision: 1,
            }
            .encode_to_vec(),
        ),
        (
            "ChatPhotoRepresentationChanged",
            p::ChatPhotoRepresentationChanged {
                chat_id: 7,
                photo_id: 7,
                main_file_id: 9,
                representation_revision: 1,
            }
            .encode_to_vec(),
        ),
        (
            "StickerAssetRepresentationChanged",
            p::StickerAssetRepresentationChanged {
                asset: Some(p::AssetReference {
                    asset_id: 7,
                    asset_version: 3,
                }),
                main_file_id: 9,
                representation_revision: 1,
            }
            .encode_to_vec(),
        ),
    ];
    for (name, bytes) in cases {
        let mut any = prost_types::Any {
            type_url: format!("type.googleapis.com/tinkle.v1.{name}"),
            value: bytes,
        };
        assert!(
            tinkle_schema::sync::notification(&any.encode_to_vec()).is_ok(),
            "{name}"
        );
        any.value.clear();
        assert!(
            tinkle_schema::sync::notification(&any.encode_to_vec()).is_err(),
            "{name}"
        );
    }
}
