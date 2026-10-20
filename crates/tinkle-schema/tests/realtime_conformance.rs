//! Tinkle realtime conformance (REALTIME.md + realtime.proto).
//!
//! Covers batch grammar, UUID discipline, position order, cursor
//! decisions, floor/head rules, reset handling, presence ephemerality.

use tinkle_schema::proto::{
    realtime_event::Kind as EK, RealtimeBatch, RealtimeChatPosition, RealtimeEvent,
    RealtimeEventType,
};
use tinkle_schema::realtime::{
    decide_cursor, position_order, validate_batch, CursorDecision, StreamCursors, MAX_CURSOR,
};

fn evt(stream: &str, cursor: u64, id: &str, kind: EK, ty: RealtimeEventType) -> RealtimeEvent {
    RealtimeEvent {
        event_id: id.to_string(),
        stream: stream.to_string(),
        cursor,
        occurred_at: None,
        source_seq: None,
        source_revision: None,
        r#type: ty as i32,
        kind: Some(kind),
    }
}

#[test]
fn batch_grammar_snapshot_with_primers() {
    let s = "user:7";
    let b = RealtimeBatch {
        stream: s.into(),
        events: vec![
            evt(
                s,
                10,
                "u1",
                EK::UserHydration(Default::default()),
                RealtimeEventType::UserHydration,
            ),
            evt(
                s,
                10,
                "u1",
                EK::ChatHydration(tinkle_schema::proto::RealtimeChatHydration {
                    chat_key: "room:1".into(),
                }),
                RealtimeEventType::ChatHydration,
            ),
            evt(
                s,
                10,
                "u1",
                EK::MessageSnapshot(Default::default()),
                RealtimeEventType::MessageSnapshot,
            ),
            evt(
                s,
                10,
                "u1",
                EK::ChatPosition(RealtimeChatPosition {
                    chat_key: "room:1".into(),
                    order: position_order(10, None),
                }),
                RealtimeEventType::ChatPosition,
            ),
        ],
        final_cursor: 10,
    };
    assert!(validate_batch(&b).is_ok());
}

#[test]
fn rejects_primer_only_and_mixed_messages() {
    let s = "user:7";
    let b = RealtimeBatch {
        stream: s.into(),
        events: vec![evt(
            s,
            3,
            "u1",
            EK::UserHydration(Default::default()),
            RealtimeEventType::UserHydration,
        )],
        final_cursor: 3,
    };
    assert!(validate_batch(&b).is_err());
}

#[test]
fn redaction_singleton_has_no_identity() {
    let s = "room:1";
    let good = RealtimeBatch {
        stream: s.into(),
        events: vec![evt(
            s,
            4,
            "",
            EK::UpdateRedacted(Default::default()),
            RealtimeEventType::UpdateRedacted,
        )],
        final_cursor: 4,
    };
    assert!(validate_batch(&good).is_ok());

    let bad = RealtimeBatch {
        stream: s.into(),
        events: vec![evt(
            s,
            4,
            "uuid-leak",
            EK::UpdateRedacted(Default::default()),
            RealtimeEventType::UpdateRedacted,
        )],
        final_cursor: 4,
    };
    assert!(validate_batch(&bad).is_err());
}

#[test]
fn type_must_match_kind() {
    let s = "user:7";
    let b = RealtimeBatch {
        stream: s.into(),
        events: vec![evt(
            s,
            5,
            "u1",
            EK::UpdateRedacted(Default::default()),
            // Wrong enum on purpose.
            RealtimeEventType::MessageSnapshot,
        )],
        final_cursor: 5,
    };
    assert!(validate_batch(&b).is_err());
}

#[test]
fn cursor_ordering_and_overflow_safe() {
    assert_eq!(decide_cursor(5, 5), CursorDecision::Duplicate);
    assert_eq!(decide_cursor(5, 6), CursorDecision::Next);
    assert_eq!(decide_cursor(5, 8), CursorDecision::Gap);
    assert_eq!(decide_cursor(5, 3), CursorDecision::Stale);
    // Overflow must not create a false successor.
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
