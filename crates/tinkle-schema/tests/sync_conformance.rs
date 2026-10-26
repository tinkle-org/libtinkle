//! Sync conformance for the post-BSR-#51/#52 contract.
//!
//! The durable update feed retired from the public surface: `updates.proto`
//! is connectivity-only (`ObserveConnectivity`), and reconciliation runs
//! over unary `MessagesService/GetMessages` + `GetTargetChanges` pages.
//! Covers tonic client surface presence, prost shapes for
//! `Target`/`GetMessages*`/`GetTargetChanges*`/`TargetChangeKind`, and the
//! `sync` validation helpers.

use prost::Message as _;
use tinkle_schema::methods::ALL_METHODS;
use tinkle_schema::proto::{
    target, ConnectivityChanged, GetHistoryResponse, GetMessagesRequest, GetMessagesResponse,
    GetTargetChangesRequest, GetTargetChangesResponse, Message, Target, TargetChange,
    TargetChangeKind,
};
use tinkle_schema::sync::{
    accept_seq_decision, chunk_message_ids, fresher_body, target_from_parts, target_key_for,
    validate_changes_page, validate_connectivity_notification, AcceptSeqDecision, PageMore,
    GET_HISTORY_METHOD, GET_MESSAGES_METHOD, GET_TARGET_CHANGES_METHOD,
    MAX_MESSAGE_IDS_PER_REQUEST, MAX_TARGET_CHANGES_LIMIT, OBSERVE_METHOD,
};

fn ts(seconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds, nanos: 0 }
}

fn msg(id: i64, edit_seconds: Option<i64>, accept_seq: Option<i64>) -> Message {
    Message {
        message_id: id,
        edit_date: edit_seconds.map(ts),
        accept_seq,
        ..Default::default()
    }
}

fn change(
    message_id: i64,
    kind: TargetChangeKind,
    message: Option<Message>,
    accept_seq: i64,
) -> TargetChange {
    TargetChange {
        message_id,
        kind: kind as i32,
        message,
        accept_seq,
    }
}

#[test]
fn method_constants_match_inventory() {
    assert_eq!(
        OBSERVE_METHOD,
        "/tinkle.v1.UpdatesService/ObserveConnectivity"
    );
    assert_eq!(
        GET_MESSAGES_METHOD,
        "/tinkle.v1.MessagesService/GetMessages"
    );
    assert_eq!(
        GET_TARGET_CHANGES_METHOD,
        "/tinkle.v1.MessagesService/GetTargetChanges"
    );
    assert_eq!(GET_HISTORY_METHOD, "/tinkle.v1.MessagesService/GetHistory");
    assert_eq!(MAX_MESSAGE_IDS_PER_REQUEST, 100);
    assert_eq!(MAX_TARGET_CHANGES_LIMIT, 500);
}

#[test]
fn updates_inventory_is_connectivity_only() {
    let updates: Vec<_> = ALL_METHODS
        .iter()
        .filter(|(m, _, _)| m.starts_with("/tinkle.v1.UpdatesService/"))
        .collect();
    assert_eq!(
        updates.len(),
        1,
        "updates surface must be connectivity-only"
    );
    assert_eq!(
        *updates[0],
        (
            "/tinkle.v1.UpdatesService/ObserveConnectivity",
            "ObserveConnectivityRequest",
            "ObserveConnectivityResponse"
        )
    );
    // No retired update-feed names survive anywhere in the inventory.
    for (method, req, resp) in ALL_METHODS {
        assert!(!method.contains("ReadUpdates"), "{method}");
        assert!(!method.contains("FollowUpdates"), "{method}");
        for name in [req, resp] {
            assert!(!name.contains("ReadUpdates"), "{name}");
            assert!(!name.contains("FollowUpdates"), "{name}");
            assert!(!name.contains("RebuildNeeded"), "{name}");
            assert!(!name.contains("PeerInput"), "{name}");
        }
    }
    // Reconciliation entries exist with exact paths and payload names.
    assert!(ALL_METHODS.contains(&(
        "/tinkle.v1.MessagesService/GetMessages",
        "GetMessagesRequest",
        "GetMessagesResponse"
    )));
    assert!(ALL_METHODS.contains(&(
        "/tinkle.v1.MessagesService/GetTargetChanges",
        "GetTargetChangesRequest",
        "GetTargetChangesResponse"
    )));
}

// Absence of `read_updates`/`follow_updates` on the tonic clients is
// compile-enforced: naming them fails the build (the generated
// `updates_service_client` module no longer defines them). Presence of the
// surviving client types is pinned here; the exact RPC inventory is pinned
// against the compiled file descriptor set below, which is the same ground
// truth the tonic clients are generated from.
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn tonic_clients_expose_surviving_services() {
    use tinkle_schema::proto::grpc::{
        messages_service_client::MessagesServiceClient,
        updates_service_client::UpdatesServiceClient,
    };
    fn _clients<T>(_: MessagesServiceClient<T>, _: UpdatesServiceClient<T>) {}
}

#[test]
fn updates_proto_surface_is_connectivity_only() {
    let set = prost_types::FileDescriptorSet::decode(tinkle_schema::proto::FILE_DESCRIPTOR_SET)
        .expect("descriptor set decodes");
    let updates = set
        .file
        .iter()
        .find(|f| {
            f.name
                .as_deref()
                .is_some_and(|n| n.ends_with("updates.proto"))
        })
        .expect("updates.proto in descriptor");
    // Message inventory: connectivity only, no retired envelope payloads.
    let mut messages: Vec<&str> = updates
        .message_type
        .iter()
        .map(|m| m.name.as_deref().unwrap_or(""))
        .collect();
    messages.sort_unstable();
    assert_eq!(
        messages,
        [
            "ConnectivityChanged",
            "ObserveConnectivityRequest",
            "ObserveConnectivityResponse"
        ]
    );
    assert!(updates.enum_type.is_empty(), "no enums survive retirement");
    // Service inventory: exactly one server-streaming ObserveConnectivity.
    assert_eq!(updates.service.len(), 1);
    let svc = &updates.service[0];
    assert_eq!(svc.name.as_deref(), Some("UpdatesService"));
    assert_eq!(svc.method.len(), 1);
    let method = &svc.method[0];
    assert_eq!(method.name.as_deref(), Some("ObserveConnectivity"));
    assert_eq!(method.server_streaming, Some(true));
    assert!(method.client_streaming != Some(true));
    assert_eq!(
        method.input_type.as_deref(),
        Some(".tinkle.v1.ObserveConnectivityRequest")
    );
    assert_eq!(
        method.output_type.as_deref(),
        Some(".tinkle.v1.ObserveConnectivityResponse")
    );
    // Retired RPCs are gone from every service in the package.
    for f in &set.file {
        for s in &f.service {
            for m in &s.method {
                let name = m.name.as_deref().unwrap_or("");
                assert_ne!(name, "ReadUpdates", "retired RPC survives");
                assert_ne!(name, "FollowUpdates", "retired RPC survives");
            }
        }
    }
    // Reconciliation RPCs are present on MessagesService.
    let mut found = false;
    for f in &set.file {
        for s in &f.service {
            if s.name.as_deref() == Some("MessagesService") {
                let names: Vec<&str> = s
                    .method
                    .iter()
                    .map(|m| m.name.as_deref().unwrap_or(""))
                    .collect();
                assert!(names.contains(&"GetMessages"), "{names:?}");
                assert!(names.contains(&"GetTargetChanges"), "{names:?}");
                found = true;
            }
        }
    }
    assert!(found, "MessagesService present in descriptor");
}

#[test]
fn prost_target_shape() {
    let user = Target {
        kind: Some(target::Kind::UserId(7)),
    };
    let room = Target {
        kind: Some(target::Kind::ChatId(9)),
    };
    let back = Target::decode(user.encode_to_vec().as_slice()).expect("decodes");
    assert_eq!(back.kind, Some(target::Kind::UserId(7)));
    let back = Target::decode(room.encode_to_vec().as_slice()).expect("decodes");
    assert_eq!(back.kind, Some(target::Kind::ChatId(9)));
}

#[test]
fn prost_get_messages_shape() {
    let req = GetMessagesRequest {
        target: Some(Target {
            kind: Some(target::Kind::UserId(7)),
        }),
        message_ids: vec![1, 2, 3],
    };
    let back = GetMessagesRequest::decode(req.encode_to_vec().as_slice()).expect("decodes");
    assert_eq!(back.message_ids, vec![1, 2, 3]);
    assert_eq!(
        back.target.and_then(|t| t.kind),
        Some(target::Kind::UserId(7))
    );

    let resp = GetMessagesResponse {
        messages: vec![msg(1, None, Some(4))],
        users: vec![],
        chats: vec![],
    };
    let back = GetMessagesResponse::decode(resp.encode_to_vec().as_slice()).expect("decodes");
    assert_eq!(back.messages.len(), 1);
    assert_eq!(back.messages[0].accept_seq, Some(4));
}

#[test]
fn prost_get_target_changes_shape() {
    let req = GetTargetChangesRequest {
        target: Some(Target {
            kind: Some(target::Kind::ChatId(9)),
        }),
        after_accept_seq: 10,
        limit: 100,
    };
    let back = GetTargetChangesRequest::decode(req.encode_to_vec().as_slice()).expect("decodes");
    assert_eq!(back.after_accept_seq, 10);
    assert_eq!(back.limit, 100);

    let resp = GetTargetChangesResponse {
        changes: vec![
            change(
                1,
                TargetChangeKind::Edited,
                Some(msg(1, Some(5), Some(3))),
                11,
            ),
            change(2, TargetChangeKind::Deleted, None, 12),
            change(3, TargetChangeKind::Revoked, None, 13),
        ],
        more_available: Some(true),
        last_accept_seq: 13,
    };
    let back = GetTargetChangesResponse::decode(resp.encode_to_vec().as_slice()).expect("decodes");
    assert_eq!(back.changes.len(), 3);
    assert_eq!(back.more_available, Some(true));
    assert_eq!(back.last_accept_seq, 13);
    // All four kind values exist with stable discriminants.
    assert_eq!(TargetChangeKind::Unspecified as i32, 0);
    assert_eq!(TargetChangeKind::Edited as i32, 1);
    assert_eq!(TargetChangeKind::Deleted as i32, 2);
    assert_eq!(TargetChangeKind::Revoked as i32, 3);
    assert!(back.changes[0].message.is_some());
    assert!(back.changes[1].message.is_none());
}

#[test]
fn prost_message_accept_seq_and_history_more() {
    let dated = msg(1, Some(5), Some(11));
    assert_eq!(dated.accept_seq, Some(11));
    let legacy = msg(2, None, None);
    assert_eq!(legacy.accept_seq, None);

    for more in [Some(true), Some(false), None] {
        let resp = GetHistoryResponse {
            more_available: more,
            ..Default::default()
        };
        let back = GetHistoryResponse::decode(resp.encode_to_vec().as_slice()).expect("decodes");
        assert_eq!(back.more_available, more);
    }
}

fn edited(id: i64, at: i64) -> TargetChange {
    change(
        id,
        TargetChangeKind::Edited,
        Some(msg(id, None, Some(at))),
        at,
    )
}

#[test]
fn changes_page_accepts_ordered_pages() {
    // Caught-up empty page echoes the cursor.
    for more in [Some(false), None] {
        let resp = GetTargetChangesResponse {
            changes: vec![],
            more_available: more,
            last_accept_seq: 10,
        };
        let page = validate_changes_page(10, &resp).expect("empty page validates");
        assert!(page.changes.is_empty());
        assert_eq!(page.last_accept_seq, 10);
        assert_eq!(
            page.more,
            if more == Some(false) {
                PageMore::No
            } else {
                PageMore::Unknown
            }
        );
    }
    // Non-empty page: strictly ascending, cursor at the last change.
    let resp = GetTargetChangesResponse {
        changes: vec![
            edited(1, 11),
            change(2, TargetChangeKind::Deleted, None, 12),
            change(3, TargetChangeKind::Revoked, None, 15),
        ],
        more_available: Some(true),
        last_accept_seq: 15,
    };
    let page = validate_changes_page(10, &resp).expect("ordered page validates");
    assert_eq!(page.changes.len(), 3);
    assert_eq!(page.last_accept_seq, 15);
    assert_eq!(page.more, PageMore::Yes);

    let tail = GetTargetChangesResponse {
        changes: vec![edited(4, 16)],
        more_available: Some(false),
        last_accept_seq: 16,
    };
    let page = validate_changes_page(15, &tail).expect("tail page validates");
    assert_eq!(page.more, PageMore::No);
}

#[test]
fn changes_page_rejects_violations() {
    let good = || GetTargetChangesResponse {
        changes: vec![
            edited(1, 11),
            change(2, TargetChangeKind::Deleted, None, 12),
        ],
        more_available: Some(false),
        last_accept_seq: 12,
    };
    // Non-ascending accept_seq.
    let mut bad = good();
    bad.changes[1].accept_seq = 11;
    assert!(validate_changes_page(10, &bad).is_err());
    // Duplicate accept_seq.
    let mut bad = good();
    bad.changes[1].accept_seq = 11;
    bad.last_accept_seq = 11;
    assert!(validate_changes_page(10, &bad).is_err());
    // Change at or below the requesting cursor.
    let mut bad = good();
    bad.changes[0].accept_seq = 10;
    bad.last_accept_seq = 12;
    assert!(validate_changes_page(10, &bad).is_err());
    // Cursor must equal the last change on a non-empty page.
    let mut bad = good();
    bad.last_accept_seq = 13;
    assert!(validate_changes_page(10, &bad).is_err());
    let mut bad = good();
    bad.last_accept_seq = 11;
    assert!(validate_changes_page(10, &bad).is_err());
    // Cursor must never move backward.
    let bad = good();
    assert!(validate_changes_page(13, &bad).is_err());
    // Empty page must echo the cursor exactly.
    let bad = GetTargetChangesResponse {
        changes: vec![],
        more_available: Some(false),
        last_accept_seq: 11,
    };
    assert!(validate_changes_page(10, &bad).is_err());
    // Empty page claiming more is a protocol violation.
    let bad = GetTargetChangesResponse {
        changes: vec![],
        more_available: Some(true),
        last_accept_seq: 10,
    };
    assert!(validate_changes_page(10, &bad).is_err());
    // Edited changes must carry the body.
    let bad = GetTargetChangesResponse {
        changes: vec![change(1, TargetChangeKind::Edited, None, 11)],
        more_available: None,
        last_accept_seq: 11,
    };
    assert!(validate_changes_page(10, &bad).is_err());
    // Deleted/revoked changes must not carry a body.
    for kind in [TargetChangeKind::Deleted, TargetChangeKind::Revoked] {
        let bad = GetTargetChangesResponse {
            changes: vec![change(1, kind, Some(msg(1, None, None)), 11)],
            more_available: None,
            last_accept_seq: 11,
        };
        assert!(validate_changes_page(10, &bad).is_err());
    }
    // Unspecified and unknown kinds are rejected.
    let bad = GetTargetChangesResponse {
        changes: vec![change(1, TargetChangeKind::Unspecified, None, 11)],
        more_available: None,
        last_accept_seq: 11,
    };
    assert!(validate_changes_page(10, &bad).is_err());
    let mut bad = good();
    bad.changes[0].kind = 99;
    assert!(validate_changes_page(10, &bad).is_err());
}

#[test]
fn accept_seq_decisions() {
    use AcceptSeqDecision::*;
    assert_eq!(accept_seq_decision(Some(5), Some(7)), Newer);
    assert_eq!(accept_seq_decision(Some(5), Some(5)), DuplicateOrStale);
    assert_eq!(accept_seq_decision(Some(5), Some(3)), DuplicateOrStale);
    assert_eq!(accept_seq_decision(Some(5), None), UnknownOrder);
    assert_eq!(accept_seq_decision(None, Some(3)), UnknownOrder);
    assert_eq!(accept_seq_decision(None, None), UnknownOrder);
}

#[test]
fn chunk_message_ids_batches() {
    assert!(chunk_message_ids(&[]).is_empty());
    let one: Vec<i64> = (1..=100).collect();
    assert_eq!(chunk_message_ids(&one), vec![one.clone()]);
    let over: Vec<i64> = (1..=101).collect();
    let chunks = chunk_message_ids(&over);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].len(), 100);
    assert_eq!(chunks[1], vec![101]);
    let many: Vec<i64> = (1..=250).collect();
    let chunks = chunk_message_ids(&many);
    assert_eq!(chunks.len(), 3);
    assert_eq!(
        [chunks[0].len(), chunks[1].len(), chunks[2].len()],
        [100, 100, 50]
    );
    // Order preserved across chunk boundaries.
    let flat: Vec<i64> = chunks.into_iter().flatten().collect();
    assert_eq!(flat, many);
}

#[test]
fn fresher_body_prefers_later_edit_date() {
    let undated = msg(1, None, Some(3));
    let old = msg(1, Some(10), Some(3));
    let new = msg(1, Some(20), Some(3));
    assert!(!fresher_body(&undated, &undated));
    assert!(!fresher_body(&old, &undated), "absent never beats dated");
    assert!(fresher_body(&undated, &old), "dated beats absent");
    assert!(fresher_body(&old, &new));
    assert!(!fresher_body(&new, &old));
    assert!(
        !fresher_body(&old, &msg(1, Some(10), Some(3))),
        "equal is not fresher"
    );
}

#[test]
fn target_keys_and_parts() {
    assert_eq!(target_key_for(&target::Kind::UserId(7)), "target:user:7");
    assert_eq!(target_key_for(&target::Kind::ChatId(9)), "target:room:9");

    let user = target_from_parts("user", 7).expect("user target");
    assert_eq!(user.kind, Some(target::Kind::UserId(7)));
    for roomy in ["room", "chat"] {
        let room = target_from_parts(roomy, 9).expect("room target");
        assert_eq!(room.kind, Some(target::Kind::ChatId(9)));
    }
    assert!(target_from_parts("user", 0).is_err());
    assert!(target_from_parts("room", -3).is_err());
    assert!(target_from_parts("dm", 7).is_err());
    assert!(target_from_parts("", 7).is_err());
    assert!(target_from_parts("User", 7).is_err());
}

#[test]
fn connectivity_notification_round_trip() {
    let seen = ConnectivityChanged {
        user_id: 42,
        online: true,
        known: true,
        revision: 9_007_199_254_740_993,
    };
    let back = validate_connectivity_notification(&seen.encode_to_vec()).expect("decodes");
    assert_eq!(back, seen);

    // Retraction: unknown never proves offline.
    let unknown = ConnectivityChanged {
        user_id: 42,
        online: false,
        known: false,
        revision: 7,
    };
    let back = validate_connectivity_notification(&unknown.encode_to_vec()).expect("decodes");
    assert!(!back.known);
    assert!(!back.online);
    assert_eq!(back.revision, 7);

    assert!(validate_connectivity_notification(b"\xff\xff\xff\xff\xff\xff\xff").is_err());
}
