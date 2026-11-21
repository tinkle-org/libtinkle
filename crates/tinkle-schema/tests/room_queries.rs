//! Descriptor and validation checks for the room query contract.
#![cfg(tinkle_room_queries)]

use prost::Message as _;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    FileDescriptorSet, Timestamp,
};
use tinkle_schema::{proto as p, proto::room_query_target::Room, room_queries::*};

#[test]
fn room_query_messages_and_unary_methods_match_descriptor() {
    use Type::{Int32, Int64, Message};
    let set = FileDescriptorSet::decode(p::FILE_DESCRIPTOR_SET).unwrap();
    let files = || set.file.iter().filter(|f| f.package() == "tinkle.v1");
    let service = files()
        .flat_map(|f| &f.service)
        .find(|s| s.name() == "MessagesService")
        .unwrap();
    for name in ["GetRoomMembers", "GetRoomOnlineCount"] {
        let method = service.method.iter().find(|m| m.name() == name).unwrap();
        assert!(!method.client_streaming() && !method.server_streaming());
    }
    for (name, fields) in [
        (
            "RoomQueryTarget",
            &[("chat_id", Int64, ""), ("channel_id", Int64, "")][..],
        ),
        (
            "GetRoomMembersRequest",
            &[
                ("target", Message, ".tinkle.v1.RoomQueryTarget"),
                ("after_user_id", Int64, ""),
                ("limit", Int32, ""),
            ],
        ),
        (
            "GetRoomMembersResponse",
            &[
                ("members", Message, ".tinkle.v1.Participant"),
                ("next_after_user_id", Int64, ""),
            ],
        ),
        (
            "GetRoomOnlineCountRequest",
            &[("target", Message, ".tinkle.v1.RoomQueryTarget")],
        ),
        (
            "GetRoomOnlineCountResponse",
            &[("sample", Message, ".tinkle.v1.RoomOnlineCountSample")],
        ),
        (
            "RoomOnlineCountSample",
            &[
                ("count", Int32, ""),
                ("sampled_at", Message, ".google.protobuf.Timestamp"),
            ],
        ),
    ] {
        let msg = files()
            .flat_map(|f| &f.message_type)
            .find(|m| m.name() == name)
            .unwrap();
        let room = name == "RoomQueryTarget";
        assert_eq!(msg.oneof_decl.len(), usize::from(room), "{name}");
        if room {
            assert_eq!(msg.oneof_decl[0].name(), "room");
        }
        assert_eq!(msg.field.len(), fields.len(), "{name}");
        for (i, (field_name, kind, type_name)) in fields.iter().enumerate() {
            let field = &msg.field[i];
            assert_eq!(field.name(), *field_name, "{name}");
            assert_eq!(field.number(), i as i32 + 1, "{name}.{field_name}");
            assert_eq!(field.r#type(), *kind, "{name}.{field_name}");
            assert_eq!(field.type_name(), *type_name, "{name}.{field_name}");
            assert_eq!(field.oneof_index, room.then_some(0));
            let label = if *field_name == "members" {
                Label::Repeated
            } else {
                Label::Optional
            };
            assert_eq!(field.label(), label);
            assert!(!field.proto3_optional());
        }
    }
}

#[test]
fn room_requests_require_one_positive_target_and_valid_page_bounds() {
    let room = |room| Some(p::RoomQueryTarget { room });
    for (target, valid) in [
        (None, false),
        (room(None), false),
        (room(Some(Room::ChatId(0))), false),
        (room(Some(Room::ChannelId(-1))), false),
        (room(Some(Room::ChatId(1))), true),
        (room(Some(Room::ChannelId(i64::MAX))), true),
    ] {
        assert_eq!(
            validate_online_count_request(&p::GetRoomOnlineCountRequest { target }).is_ok(),
            valid
        );
        for (after_user_id, limit, bounds_valid) in [
            (0, 0, true),
            (i64::MAX, 1, true),
            (1, 200, true),
            (-1, 10, false),
            (0, -1, false),
            (0, 201, false),
        ] {
            let req = p::GetRoomMembersRequest {
                target,
                after_user_id,
                limit,
            };
            assert_eq!(
                validate_members_request(&req).is_ok(),
                valid && bounds_valid
            );
        }
    }
}

#[test]
fn members_page_enforces_order_size_and_exclusive_cursor() {
    let mut req = p::GetRoomMembersRequest {
        target: Some(p::RoomQueryTarget {
            room: Some(Room::ChannelId(7)),
        }),
        after_user_id: 3,
        limit: 2,
    };
    let check = |req: &p::GetRoomMembersRequest, ids: &[i64], next_after_user_id, expected| {
        let page = p::GetRoomMembersResponse {
            members: ids
                .iter()
                .map(|&user_id| p::Participant {
                    user_id,
                    ..Default::default()
                })
                .collect(),
            next_after_user_id,
        };
        assert_eq!(
            validate_members_page(req, &page).is_ok(),
            expected,
            "{ids:?} / {next_after_user_id}"
        );
    };
    for (ids, next, valid) in [
        (&[][..], 0, true),
        (&[4], 4, true),
        (&[4, 9], 0, true),
        (&[], 3, false),
        (&[3], 0, false),
        (&[-1], 0, false),
        (&[4, 4], 4, false),
        (&[9, 4], 0, false),
        (&[4, 9], 4, false),
        (&[4, 9], 10, false),
        (&[4], -1, false),
        (&[4, 5, 6], 6, false),
    ] {
        check(&req, ids, next, valid);
    }
    req.limit = 0;
    let ids: Vec<_> = (4..=54).collect();
    check(&req, &ids[..50], 53, true);
    check(&req, &ids, 54, false);
}

#[test]
fn count_samples_preserve_absence_and_require_valid_disclosed_values() {
    let absent = p::GetRoomOnlineCountResponse::decode(&[][..]).unwrap();
    assert!(absent.sample.is_none());
    assert_eq!(validate_online_count_response(&absent), Ok(()));

    let time = |seconds, nanos| Some(Timestamp { seconds, nanos });
    let now = time(1_700_000_000, 123);
    for (count, sampled_at, valid) in [
        (0, now, true),
        (12, now, true),
        (-1, now, false),
        (0, None, false),
        (1, time(253_402_300_800, 0), false),
        (1, time(-62_135_596_801, 0), false),
        (1, time(0, -1), false),
        (1, time(0, 1_000_000_000), false),
    ] {
        let response = p::GetRoomOnlineCountResponse {
            sample: Some(p::RoomOnlineCountSample { count, sampled_at }),
        };
        let decoded =
            p::GetRoomOnlineCountResponse::decode(response.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, response);
        assert_eq!(validate_online_count_response(&decoded).is_ok(), valid);
    }
}
