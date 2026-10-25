//! Sync conformance for `tinkle.v1.Update`.
//!
//! Covers single-update validation, UUID discipline, position order,
//! cursor decisions, floor/head rules, rebuild handling, and
//! connectivity ephemerality.

use tinkle_schema::proto::{
    follow_updates_response::Result as FollowResult, update::Payload as UpdatePayload,
    ContentOmitted, FollowUpdatesResponse, Message, MessageContext, MessageRefresh,
    ReadStateInvalidated, Update,
};
use tinkle_schema::sync::{
    decide_cursor, position_order, validate_follow_response, validate_update, CursorDecision,
    StreamCursors, MAX_CURSOR,
};

fn update(stream: &str, cursor: u64, event_id: &str, payload: UpdatePayload) -> Update {
    Update {
        event_id: event_id.to_string(),
        stream: stream.to_string(),
        cursor,
        occurred_at: None,
        source_seq: None,
        source_revision: None,
        payload: Some(payload),
    }
}

fn refresh(stream: &str, cursor: u64, id: &str) -> Update {
    update(
        stream,
        cursor,
        id,
        UpdatePayload::MessageRefresh(MessageRefresh {
            message: Some(Message {
                message_id: 1,
                ..Default::default()
            }),
            random_id: None,
            context: Some(MessageContext {
                conversation_key: "room:1".into(),
                order_key: position_order(cursor, None),
                resolve_user_id: None,
                resolve_conversation: false,
            }),
        }),
    )
}

#[test]
fn accepts_single_message_refresh() {
    let u = refresh("user:7", 10, "550e8400-e29b-41d4-a716-446655440000");
    assert!(validate_update(&u).is_ok());
}

#[test]
fn rejects_missing_payload() {
    let u = Update {
        event_id: "550e8400-e29b-41d4-a716-446655440000".into(),
        stream: "user:7".into(),
        cursor: 3,
        occurred_at: None,
        source_seq: None,
        source_revision: None,
        payload: None,
    };
    assert!(validate_update(&u).is_err());
}

#[test]
fn content_omitted_has_no_identity() {
    let good = update(
        "room:1",
        4,
        "",
        UpdatePayload::ContentOmitted(ContentOmitted {}),
    );
    assert!(validate_update(&good).is_ok());

    let bad = update(
        "room:1",
        4,
        "leak",
        UpdatePayload::ContentOmitted(ContentOmitted {}),
    );
    assert!(validate_update(&bad).is_err());
}

#[test]
fn read_state_invalidation_validates() {
    let u = update(
        "user:7",
        5,
        "550e8400-e29b-41d4-a716-446655440001",
        UpdatePayload::ReadStateInvalidated(ReadStateInvalidated {}),
    );
    assert!(validate_update(&u).is_ok());
}

#[test]
fn follow_progress_never_advances() {
    use prost::Message as _;
    let resp = FollowUpdatesResponse {
        stream: "user:7".into(),
        head: Some(10),
        result: None,
    };
    let bytes = resp.encode_to_vec();
    let v = validate_follow_response(&bytes).expect("progress decodes");
    match v {
        tinkle_schema::sync::ValidatedFollow::Progress { stream, head } => {
            assert_eq!(stream, "user:7");
            assert_eq!(head, Some(10));
        }
        _ => panic!("expected progress"),
    }
}

#[test]
fn follow_update_validates_stream_match() {
    use prost::Message as _;
    let u = refresh("user:7", 11, "550e8400-e29b-41d4-a716-446655440002");
    let resp = FollowUpdatesResponse {
        stream: "user:7".into(),
        head: Some(11),
        result: Some(FollowResult::Update(u)),
    };
    let bytes = resp.encode_to_vec();
    assert!(validate_follow_response(&bytes).is_ok());

    let u2 = refresh("user:8", 11, "550e8400-e29b-41d4-a716-446655440003");
    let bad = FollowUpdatesResponse {
        stream: "user:7".into(),
        head: Some(11),
        result: Some(FollowResult::Update(u2)),
    };
    assert!(validate_follow_response(&bad.encode_to_vec()).is_err());
}

#[test]
fn cursor_ordering_and_overflow_safe() {
    assert_eq!(decide_cursor(5, 5), CursorDecision::Duplicate);
    assert_eq!(decide_cursor(5, 6), CursorDecision::Next);
    assert_eq!(decide_cursor(5, 8), CursorDecision::Gap);
    assert_eq!(decide_cursor(5, 3), CursorDecision::Stale);
    assert_eq!(decide_cursor(u64::MAX, 0), CursorDecision::Stale);
}

#[test]
fn cursors_fit_signed_64_and_deliveries_positive() {
    assert_eq!(MAX_CURSOR, i64::MAX as u64);
    let mut c = StreamCursors::new();
    assert!(c.commit("user:1", 0).is_err());
    assert!(c.commit("user:1", MAX_CURSOR + 1).is_err());
}

#[test]
fn floor_rules() {
    let mut c = StreamCursors::new();
    c.set_floor("user:1", 10);
    assert!(c.validate_after("user:1", 5, None).is_err());
    assert!(c.validate_after("user:1", 10, None).is_ok());
    assert!(c.validate_after("user:1", 11, Some(20)).is_ok());
    assert!(c.validate_after("user:1", 21, Some(20)).is_err());
}

#[test]
fn reset_clears_without_head_checkpoint() {
    let mut c = StreamCursors::new();
    c.hydrate("user:1", 42);
    assert_eq!(c.get("user:1"), 42);
    c.on_reset("user:1");
    assert_eq!(c.get("user:1"), 0);
}
