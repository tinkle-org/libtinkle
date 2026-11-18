//! Descriptor and raw-byte validation for independent representations.
#![cfg(tinkle_async_actions)]
use prost::Message;
use tinkle_schema::proto as p;

/// Edit-date fallback chooses semantics first, then merges the exact main's independent outputs.
#[test]
fn edit_dates_and_representation_revisions_advance_independently_without_body_revisions() {
    for document in [false, true] {
        let body = |date, representation, thumbnail| p::Message {
            message_id: 10,
            target: Some(p::Target {
                kind: Some(p::target::Kind::ChatId(7)),
            }),
            message: format!("caption at {date}"),
            edit_date: Some(prost_types::Timestamp {
                seconds: date,
                nanos: 0,
            }),
            media: Some(p::Media {
                media: Some(if document {
                    p::media::Media::Document(p::Document {
                        file_id: 9,
                        representation_revision: Some(representation),
                        thumbnail: Some(p::Thumbnail {
                            file_id: thumbnail,
                            width: 1,
                            height: 1,
                        }),
                        ..Default::default()
                    })
                } else {
                    p::media::Media::Photo(p::Photo {
                        file_id: 9,
                        representation_revision: Some(representation),
                        thumbnail: Some(p::Thumbnail {
                            file_id: thumbnail,
                            width: 1,
                            height: 1,
                        }),
                        ..Default::default()
                    })
                }),
            }),
            ..Default::default()
        };
        let newer_semantics = body(20, 1, 11);
        let newer_outputs = body(10, 2, 12);
        let mut expected = newer_semantics.clone();
        expected.media = newer_outputs.media.clone();
        for (old, incoming) in [
            (&newer_semantics, &newer_outputs),
            (&newer_outputs, &newer_semantics),
        ] {
            let merged = tinkle_schema::representations::merge_message(old, incoming).unwrap();
            assert_eq!(merged, expected);
            assert!(!tinkle_schema::sync::fresher_body(old, incoming));
            assert!(tinkle_schema::sync::fresher_body(old, &merged));
        }
        assert!(
            tinkle_schema::representations::merge_message(&newer_semantics, &body(10, 1, 12))
                .is_err()
        );
        let tied_outputs = body(20, 2, 12);
        assert_eq!(
            tinkle_schema::representations::merge_message(&newer_semantics, &tied_outputs).unwrap(),
            expected
        );
    }
}

/// A stale versioned body cannot enrich a frozen unversioned capture in either arrival order.
#[test]
fn absent_representation_revision_stays_frozen_across_semantic_ordering() {
    let frozen = p::Message {
        message_id: 10,
        body_revision: Some(3),
        media: Some(p::Media {
            media: Some(p::media::Media::Photo(p::Photo {
                file_id: 9,
                ..Default::default()
            })),
        }),
        ..Default::default()
    };
    let mut versioned = frozen.clone();
    versioned.body_revision = Some(2);
    if let Some(p::media::Media::Photo(photo)) = versioned.media.as_mut().unwrap().media.as_mut() {
        photo.representation_revision = Some(2);
        photo.thumbnail = Some(p::Thumbnail {
            file_id: 11,
            width: 1,
            height: 1,
        });
    }
    for (a, b) in [(&frozen, &versioned), (&versioned, &frozen)] {
        assert_eq!(
            tinkle_schema::representations::merge_message(a, b).unwrap(),
            frozen
        );
    }
}

/// A boolean replacement helper must not authorize installing an unmerged candidate that loses
/// the other clock, and inconsistent equal representation revisions are never a safe replacement.
#[test]
fn fresher_body_only_authorizes_a_standalone_safe_candidate() {
    let body = |semantic, representation, variant| p::Message {
        message_id: 10,
        target: Some(p::Target {
            kind: Some(p::target::Kind::ChatId(7)),
        }),
        body_revision: Some(semantic),
        message: format!("semantic {semantic}"),
        media: Some(p::Media {
            media: Some(p::media::Media::Photo(p::Photo {
                file_id: 9,
                representation_revision: Some(representation),
                thumbnail: Some(p::Thumbnail {
                    file_id: variant,
                    width: 1,
                    height: 1,
                }),
                ..Default::default()
            })),
        }),
        ..Default::default()
    };
    let a = body(10, 3, 13);
    let b = body(11, 2, 12);
    assert!(
        !tinkle_schema::sync::fresher_body(&a, &b),
        "raw b would lose the newer representation"
    );
    let merged = tinkle_schema::representations::merge_message(&a, &b).unwrap();
    assert_eq!(merged.body_revision, Some(11));
    let older_semantic_newer_outputs = body(9, 4, 14);
    let independent =
        tinkle_schema::representations::merge_message(&a, &older_semantic_newer_outputs).unwrap();
    assert_eq!(independent.body_revision, Some(10));
    assert!(
        matches!(independent.media.unwrap().media,Some(p::media::Media::Photo(v))if v.representation_revision==Some(4)&&v.thumbnail.unwrap().file_id==14)
    );
    assert!(tinkle_schema::sync::fresher_body(&a, &merged));
    assert!(
        !tinkle_schema::sync::fresher_body(&a, &body(9, 4, 14)),
        "an older semantic body cannot be installed standalone"
    );
    assert!(
        !tinkle_schema::sync::fresher_body(&a, &body(11, 3, 12)),
        "conflicting equal representation clocks cannot fall back to semantic-only ranking"
    );
}

/// Every new source/projection is a generated field of the candidate, never a handwritten wire tag.
#[test]
fn preview_and_representation_descriptors_match_contract() {
    let descriptors = prost_types::FileDescriptorSet::decode(p::FILE_DESCRIPTOR_SET).unwrap();
    for (name, tag) in [
        ("PhotoSource", 4),
        ("DocumentSource", 4),
        ("VideoSource", 4),
        ("AudioSource", 4),
        ("StickerSource", 6),
        ("ProfilePhotoSource", 4),
        ("StickerAssetSource", 4),
        ("Photo", 9),
        ("Document", 10),
        ("ChatPhoto", 7),
        ("UserPhoto", 5),
        ("ProfilePhoto", 8),
        ("StickerView", 11),
    ] {
        let message = descriptors
            .file
            .iter()
            .flat_map(|f| &f.message_type)
            .find(|m| m.name.as_deref() == Some(name))
            .unwrap();
        let field = message
            .field
            .iter()
            .find(|f| f.name.as_deref() == Some("thumbnail"))
            .unwrap();
        assert_eq!(field.number, Some(tag), "{name}");
        assert_eq!(field.label, Some(1), "singular {name}");
        assert_eq!(
            field.type_name.as_deref(),
            Some(if name.ends_with("Source") {
                ".tinkle.v1.ThumbnailSource"
            } else {
                ".tinkle.v1.Thumbnail"
            })
        );
        assert!(!message
            .field
            .iter()
            .any(|f| matches!(f.name.as_deref(), Some("variants" | "thumb_file_id"))));
        assert!(message.reserved_range.is_empty(), "{name} reserved ranges");
        assert!(message.reserved_name.is_empty(), "{name} reserved names");
    }
    for name in ["Thumbnail", "ThumbnailSource"] {
        let m = descriptors
            .file
            .iter()
            .flat_map(|f| &f.message_type)
            .find(|m| m.name.as_deref() == Some(name))
            .unwrap();
        assert_eq!(m.field.len(), 3);
        for (field, (name, tag, ty)) in
            m.field
                .iter()
                .zip([("file_id", 1, 3), ("width", 2, 13), ("height", 3, 13)])
        {
            assert_eq!(
                (
                    field.name.as_deref(),
                    field.number,
                    field.r#type,
                    field.label
                ),
                (Some(name), Some(tag), Some(ty), Some(1))
            );
            assert!(!field.proto3_optional.unwrap_or(false));
        }
    }
    assert!(
        !descriptors.file.iter().flat_map(|f| &f.enum_type).any(|e| e
            .name
            .as_deref()
            .is_some_and(|n| n.starts_with("MediaVariant")))
    );
    let accepted = descriptors
        .file
        .iter()
        .flat_map(|f| &f.message_type)
        .find(|m| m.name.as_deref() == Some("SendReceipt"))
        .unwrap();
    for (name, number) in [
        ("message_id", 2),
        ("reference_withheld", 4),
        ("album_id", 3),
    ] {
        assert!(accepted
            .field
            .iter()
            .any(|f| f.name.as_deref() == Some(name)
                && f.number == Some(number)
                && f.proto3_optional == Some(true)));
    }
    for (name, preview, revision) in [
        ("PhotoSource", 3, None),
        ("VideoSource", 3, None),
        ("DocumentSource", 3, None),
        ("AudioSource", 3, None),
        ("ProfilePhotoSource", 3, None),
        ("StickerAssetSource", 3, None),
        ("StickerSource", 5, None),
        ("Photo", 7, Some(8)),
        ("Document", 8, Some(9)),
        ("UserPhoto", 3, Some(4)),
        ("ChatPhoto", 5, Some(6)),
        ("ProfilePhoto", 6, Some(7)),
        ("StickerView", 9, Some(10)),
    ] {
        let message = descriptors
            .file
            .iter()
            .flat_map(|f| &f.message_type)
            .find(|m| m.name.as_deref() == Some(name))
            .unwrap();
        assert!(message
            .field
            .iter()
            .any(|f| f.name.as_deref() == Some("inline_preview") && f.number == Some(preview)));
        if let Some(number) = revision {
            assert!(message
                .field
                .iter()
                .any(|f| f.name.as_deref() == Some("representation_revision")
                    && f.number == Some(number)
                    && f.proto3_optional == Some(true)));
        }
    }
}

/// Missing is valid; a present thumbnail must name a distinct file and positive dimensions.
#[test]
fn thumbnail_presence_is_not_an_empty_placeholder() {
    use tinkle_schema::representations::validate_thumbnail;
    assert!(validate_thumbnail(9, None).is_ok());
    for (file_id, width, height) in [(0, 0, 0), (9, 1, 1), (10, 0, 1), (10, 1, 0)] {
        let thumbnail = p::Thumbnail {
            file_id,
            width,
            height,
        };
        assert!(validate_thumbnail(9, Some(&thumbnail)).is_err());
    }
    assert!(validate_thumbnail(
        9,
        Some(&p::Thumbnail {
            file_id: 10,
            width: 1,
            height: 1
        })
    )
    .is_ok());
}

/// Legal noncanonical ordering stays byte-identical; recursively unknown fields fail closed.
#[test]
fn raw_validation_does_not_canonicalize_or_drop_nested_unknown_fields() {
    let method = "/tinkle.v1.MessagesService/SendMedia";
    let mut request = p::SendMediaRequest {
        idempotency_key: i64::MIN,
        ..Default::default()
    }
    .encode_to_vec();
    request.extend([0x30, 0]);
    let original = request.clone();
    tinkle_schema::wire::validate_request(method, &request).unwrap();
    assert_eq!(request, original);
    request.extend([0x98, 0x06, 1]);
    assert!(tinkle_schema::wire::validate_request(method, &request).is_err());
    let nested = [0x12, 7, 0x0a, 5, 0x08, 9, 0x98, 0x06, 1];
    assert!(tinkle_schema::wire::validate_request(method, &nested).is_err());
    // The compact source ends at tag 4; unknown tag 5 must not be silently dropped.
    assert!(
        tinkle_schema::wire::validate_message(".tinkle.v1.PhotoSource", &[8, 9, 42, 0]).is_err()
    );
    assert!(tinkle_schema::wire::validate_message(".tinkle.v1.StickerView", &[96, 10]).is_err());
}

/// Wire admission checks structure, leaving an omitted operation key for durable core enqueue.
#[test]
fn raw_send_message_validation_allows_zero_key_without_weakening_unknown_field_rejection() {
    let request = p::SendMessageRequest {
        target: Some(p::Target {
            kind: Some(p::target::Kind::UserId(1)),
        }),
        message: "valid text".into(),
        idempotency_key: 0,
        ..Default::default()
    };
    let method = "/tinkle.v1.MessagesService/SendMessage";
    let mut bytes = request.encode_to_vec();
    tinkle_schema::wire::validate_request(method, &bytes).unwrap();
    bytes.extend([0x98, 0x06, 1]);
    assert!(tinkle_schema::wire::validate_request(method, &bytes).is_err());
}

#[test]
fn raw_send_media_accepts_poll_close_date_and_rejects_unknown_timestamp_fields() {
    use tinkle_schema::wire::{validate_message, validate_request};

    let request = p::SendMediaRequest {
        media: Some(p::MediaSource {
            media: Some(p::media_source::Media::Poll(p::PollSource {
                close_date: Some(prost_types::Timestamp {
                    seconds: 2_000_000_000,
                    nanos: 1,
                }),
                ..Default::default()
            })),
        }),
        ..Default::default()
    };
    validate_request(
        "/tinkle.v1.MessagesService/SendMedia",
        &request.encode_to_vec(),
    )
    .unwrap();
    // PollSource.close_date with valid seconds and an unknown Timestamp field 3.
    assert!(validate_message(".tinkle.v1.PollSource", &[0x3a, 4, 8, 1, 24, 1]).is_err());
}

/// Both hint types are demands only, and representation marker revisions never advance a cursor.
#[test]
fn representation_hints_and_journal_revision_presence_validate() {
    let valid = p::MessageChanged {
        target: Some(p::Target {
            kind: Some(p::target::Kind::ChatId(7)),
        }),
        message_id: 9,
        body_revision: 1,
        change_kind: p::TargetChangeKind::Representation as i32,
        representation_revision: Some(2),
        ..Default::default()
    };
    let notification = |v: &p::MessageChanged| {
        prost_types::Any {
            type_url: "type.googleapis.com/tinkle.v1.MessageChanged".into(),
            value: v.encode_to_vec(),
        }
        .encode_to_vec()
    };
    assert!(matches!(
        tinkle_schema::sync::notification(&notification(&valid)).unwrap(),
        tinkle_schema::sync::Notification::MessageChanged(_)
    ));
    let mut invalid = valid.clone();
    invalid.representation_revision = None;
    assert!(tinkle_schema::sync::notification(&notification(&invalid)).is_err());
    invalid = valid;
    invalid.change_kind = p::TargetChangeKind::Pin as i32;
    assert!(tinkle_schema::sync::notification(&notification(&invalid)).is_err());
}
