//! Sync conformance for `tinkle.v1.Update`.
//!
//! Covers single-update validation, UUID discipline, position order,
//! cursor decisions, floor/head rules, rebuild handling, and
//! connectivity ephemerality.

use tinkle_schema::proto::{
    follow_updates_response::Result as FollowResult, update::Payload as UpdatePayload,
    ContentOmitted, FollowUpdatesResponse, Message, MessageContext, MessageRefresh,
    ReadStateInvalidated, SendAttemptFailed, Update,
};
use tinkle_schema::sync::{
    decide_cursor, is_canonical_stream, position_order, validate_follow_response,
    validate_update, CursorDecision, StreamCursors, MAX_CURSOR,
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

    // Timestamps/markers must also be absent for ContentOmitted.
    let bad_seq = Update {
        source_seq: Some(1),
        ..update(
            "room:1",
            4,
            "",
            UpdatePayload::ContentOmitted(ContentOmitted {}),
        )
    };
    assert!(validate_update(&bad_seq).is_err());

    let bad_rev = Update {
        source_revision: Some(1),
        ..update(
            "room:1",
            4,
            "",
            UpdatePayload::ContentOmitted(ContentOmitted {}),
        )
    };
    assert!(validate_update(&bad_rev).is_err());

    let bad_ts = Update {
        occurred_at: Some(prost_types::Timestamp {
            seconds: 1,
            nanos: 0,
        }),
        ..update(
            "room:1",
            4,
            "",
            UpdatePayload::ContentOmitted(ContentOmitted {}),
        )
    };
    assert!(validate_update(&bad_ts).is_err());
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
    let mut cursors = StreamCursors::new();
    cursors.hydrate("user:7", 5);
    let before = cursors.get("user:7");
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
    // Progress must never advance the applied checkpoint.
    assert_eq!(cursors.get("user:7"), before);
    assert_eq!(cursors.get("user:7"), 5);
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
    // Deliveries are positive: first delivery commits.
    assert!(c.commit("user:1", 1).is_ok());
    assert_eq!(c.get("user:1"), 1);
    // MAX_CURSOR itself fits signed 64 storage and validates.
    let max_ok = update(
        "user:1",
        MAX_CURSOR,
        "",
        UpdatePayload::ContentOmitted(ContentOmitted {}),
    );
    assert!(validate_update(&max_ok).is_ok());
    let mut cmax = StreamCursors::new();
    cmax.hydrate("user:1", MAX_CURSOR - 1);
    assert!(cmax.commit("user:1", MAX_CURSOR).is_ok());
    // validate_update rejects 0 and MAX+1.
    let zero = update(
        "user:1",
        0,
        "",
        UpdatePayload::ContentOmitted(ContentOmitted {}),
    );
    assert!(validate_update(&zero).is_err());
    let over = update(
        "user:1",
        MAX_CURSOR + 1,
        "",
        UpdatePayload::ContentOmitted(ContentOmitted {}),
    );
    assert!(validate_update(&over).is_err());
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

#[test]
fn canonical_stream_rejects_plus_and_leading_zeros() {
    assert!(is_canonical_stream("user:7"));
    assert!(is_canonical_stream("room:1"));
    assert!(!is_canonical_stream("user:+5"));
    assert!(!is_canonical_stream("user:007"));
    assert!(!is_canonical_stream("user:0"));
    assert!(!is_canonical_stream("user:"));
    assert!(!is_canonical_stream("room:00"));
    assert!(!is_canonical_stream("dm:7"));
    let bad_plus = update(
        "user:+5",
        1,
        "",
        UpdatePayload::ContentOmitted(ContentOmitted {}),
    );
    assert!(validate_update(&bad_plus).is_err());
    let bad_zeros = update(
        "room:007",
        1,
        "",
        UpdatePayload::ContentOmitted(ContentOmitted {}),
    );
    assert!(validate_update(&bad_zeros).is_err());
}

#[test]
fn event_id_requires_canonical_uuid_shape() {
    // MessageRefresh: canonical passes, malformed shapes fail.
    assert!(validate_update(&refresh("user:7", 10, "550e8400-e29b-41d4-a716-446655440000")).is_ok());
    for bad_id in [
        "-",
        "a",
        "leak",
        "550E8400-E29B-41D4-A716-446655440000",
        "550e8400e29b41d4a716446655440000",
        "550e8400-e29b-41d4-a716-44665544000",
        "00000000-0000-0000-0000-000000000000",
    ] {
        assert!(
            validate_update(&refresh("user:7", 10, bad_id)).is_err(),
            "MessageRefresh must reject event_id {bad_id:?}"
        );
    }
    // Every identity-bearing payload enforces the same rule.
    let failed_good = update(
        "user:7",
        5,
        "550e8400-e29b-41d4-a716-446655440001",
        UpdatePayload::SendAttemptFailed(SendAttemptFailed {
            message: None,
            random_id: 123,
            context: None,
        }),
    );
    assert!(validate_update(&failed_good).is_ok());
    let failed_bad = update(
        "user:7",
        5,
        "leak",
        UpdatePayload::SendAttemptFailed(SendAttemptFailed {
            message: None,
            random_id: 123,
            context: None,
        }),
    );
    assert!(validate_update(&failed_bad).is_err());
    let read_good = update(
        "user:7",
        5,
        "550e8400-e29b-41d4-a716-446655440001",
        UpdatePayload::ReadStateInvalidated(ReadStateInvalidated {}),
    );
    assert!(validate_update(&read_good).is_ok());
    let read_bad = update(
        "user:7",
        5,
        "not-a-uuid",
        UpdatePayload::ReadStateInvalidated(ReadStateInvalidated {}),
    );
    assert!(validate_update(&read_bad).is_err());
}

#[test]
fn message_context_order_key_must_match_position() {
    // Correct order_key passes (covered by refresh helper).
    let good = refresh("user:7", 10, "550e8400-e29b-41d4-a716-446655440010");
    assert!(validate_update(&good).is_ok());
    // Tampered order_key fails.
    let mut bad = refresh("user:7", 10, "550e8400-e29b-41d4-a716-446655440011");
    if let Some(UpdatePayload::MessageRefresh(m)) = bad.payload.as_mut() {
        if let Some(ctx) = m.context.as_mut() {
            ctx.order_key = "tampered".to_string();
        }
    }
    assert!(validate_update(&bad).is_err());
    // source_seq participates: correct derived key passes, stale key fails.
    let mut with_seq = refresh("user:7", 10, "550e8400-e29b-41d4-a716-446655440012");
    with_seq.source_seq = Some(7);
    if let Some(UpdatePayload::MessageRefresh(m)) = with_seq.payload.as_mut() {
        if let Some(ctx) = m.context.as_mut() {
            ctx.order_key = position_order(10, Some(7));
        }
    }
    assert!(validate_update(&with_seq).is_ok());
    let mut stale_seq = with_seq.clone();
    if let Some(UpdatePayload::MessageRefresh(m)) = stale_seq.payload.as_mut() {
        if let Some(ctx) = m.context.as_mut() {
            ctx.order_key = position_order(10, None);
        }
    }
    assert!(validate_update(&stale_seq).is_err());
    // SendAttemptFailed context is checked the same way.
    let mut failed = update(
        "user:7",
        9,
        "550e8400-e29b-41d4-a716-446655440013",
        UpdatePayload::SendAttemptFailed(SendAttemptFailed {
            message: None,
            random_id: 42,
            context: Some(MessageContext {
                conversation_key: "room:1".into(),
                order_key: position_order(9, None),
                resolve_user_id: None,
                resolve_conversation: false,
            }),
        }),
    );
    assert!(validate_update(&failed).is_ok());
    if let Some(UpdatePayload::SendAttemptFailed(m)) = failed.payload.as_mut() {
        if let Some(ctx) = m.context.as_mut() {
            ctx.order_key = "bad".to_string();
        }
    }
    assert!(validate_update(&failed).is_err());
}

#[test]
fn follow_response_rejects_out_of_range_head_for_progress_and_rebuild() {
    use prost::Message as _;
    use tinkle_schema::proto::RebuildNeeded;
    let over = MAX_CURSOR + 1;
    let progress = FollowUpdatesResponse {
        stream: "user:7".into(),
        head: Some(over),
        result: None,
    };
    assert!(validate_follow_response(&progress.encode_to_vec()).is_err());
    let rebuild = FollowUpdatesResponse {
        stream: "user:7".into(),
        head: Some(over),
        result: Some(FollowResult::RebuildNeeded(RebuildNeeded {})),
    };
    assert!(validate_follow_response(&rebuild.encode_to_vec()).is_err());
    // Update path still bounds head as well.
    let u = refresh("user:7", 11, "550e8400-e29b-41d4-a716-446655440014");
    let update_over = FollowUpdatesResponse {
        stream: "user:7".into(),
        head: Some(over),
        result: Some(FollowResult::Update(u)),
    };
    assert!(validate_follow_response(&update_over.encode_to_vec()).is_err());
}
