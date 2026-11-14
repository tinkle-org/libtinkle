//! Exact MessageChanged binding, with ephemeral presentation validated independently of body hints.
use prost::Message as _;
use tinkle_schema::{
    proto as p,
    sync::{notification, Notification},
};

/// Typed MessageChanged must not disappear into the opaque catch-all notification route.
#[test]
fn message_changed_has_a_typed_notification_binding() {
    let value = p::MessageChanged {
        target: Some(p::Target {
            kind: Some(p::target::Kind::UserId(7)),
        }),
        message_id: 1,
        body_revision: 3,
        change_kind: p::TargetChangeKind::Reaction as i32,
        ..Default::default()
    };
    let bytes = prost_types::Any {
        type_url: "type.googleapis.com/tinkle.v1.MessageChanged".into(),
        value: value.encode_to_vec(),
    }
    .encode_to_vec();
    assert!(
        !matches!(notification(&bytes).unwrap(), Notification::Hint),
        "MessageChanged was discarded as an opaque hint"
    );
}

/// Invalid presentation is discarded independently; it never suppresses a valid invalidation hint.
#[test]
fn effect_identity_choices_and_timestamps_fail_closed_without_losing_reconcile() {
    let effect = p::ReactionEffect {
        actor_id: 42,
        operation_id: "11111111-1111-4111-8111-111111111111".into(),
        expires_at: Some(prost_types::Timestamp {
            seconds: 1,
            nanos: 0,
        }),
        reactions: vec![p::Reaction {
            emoticon: "x".into(),
            custom_emoji_id: 77,
            asset_version: Some(4),
        }],
    };
    assert!(tinkle_schema::sync::valid_reaction_effect(&effect));
    for mode in 0..8 {
        let mut effect = effect.clone();
        match mode {
            0 => effect.actor_id = 0,
            1 => effect.operation_id.clear(),
            2 => effect.expires_at.as_mut().unwrap().nanos = 1_000_000_000,
            3 => effect.reactions = vec![effect.reactions[0].clone(); 9],
            4 => effect.reactions[0].asset_version = None,
            5 => effect.reactions.push(effect.reactions[0].clone()),
            6 => effect.expires_at = None,
            _ => {}
        }
        let invalidated = mode == 7;
        let value = p::MessageChanged {
            target: Some(p::Target {
                kind: Some(p::target::Kind::UserId(7)),
            }),
            message_id: 1,
            body_revision: 3,
            change_kind: p::TargetChangeKind::Reaction as i32,
            reaction_effect: Some(effect),
            body_invalidated: invalidated,
        };
        let bytes = prost_types::Any {
            type_url: "type.googleapis.com/tinkle.v1.MessageChanged".into(),
            value: value.encode_to_vec(),
        }
        .encode_to_vec();
        let Notification::MessageChanged(hint) = notification(&bytes).unwrap() else {
            panic!("typed hint lost")
        };
        assert_eq!(hint.body_revision, 3);
        assert_eq!(hint.body_invalidated, invalidated);
        assert!(hint.reaction_effect.is_none());
    }
}
