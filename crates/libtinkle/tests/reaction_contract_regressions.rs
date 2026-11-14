//! Exact caller-owned reaction retry identity and bounded private recents.
mod support;
use libtinkle::{Client, InMemoryCursorStore, TinkleError};
use prost::Message as _;
use std::sync::{Arc, Mutex};
use tinkle_schema::proto as p;

/// Restriction validation follows the actual enum and exact set; effectful TTL/forwarding needs a key.
#[tokio::test]
async fn restriction_enum_and_effectful_chat_retry_keys_validate_before_network() {
    let client = Client::new(
        support::config("127.0.0.1:1".parse().unwrap()),
        support::source(),
        InMemoryCursorStore::new(),
    );
    for scopes in [
        vec![13],
        vec![14],
        vec![15],
        vec![16],
        vec![17],
        vec![18],
        (1..=18).collect(),
    ] {
        let result = client
            .messages()
            .edit_chat_restricted_scopes(p::EditChatRestrictedScopesRequest {
                chat_id: 7,
                user_id: 9,
                restricted_scopes: scopes.clone(),
                ..Default::default()
            })
            .await;
        assert!(
            matches!(result, Err(TinkleError::NotConnected(_))),
            "{scopes:?}: {result:?}"
        );
    }
    for scopes in [vec![0], vec![19], vec![-1], vec![1, 1]] {
        assert!(matches!(
            client
                .messages()
                .edit_chat_restricted_scopes(p::EditChatRestrictedScopesRequest {
                    chat_id: 7,
                    user_id: 9,
                    restricted_scopes: scopes,
                    ..Default::default()
                })
                .await,
            Err(TinkleError::Codec(_))
        ));
    }
    for (operation_id, invalid) in [(0, true), (-1, true), (9, false)] {
        let result = client
            .messages()
            .create_chat(p::CreateChatRequest {
                title: "chat".into(),
                user_ids: vec![7],
                ttl_period: Some(60),
                operation_id,
            })
            .await;
        assert_eq!(
            matches!(result, Err(TinkleError::Codec(_))),
            invalid,
            "{result:?}"
        );
        let result = client
            .messages()
            .add_chat_participant(p::AddChatParticipantRequest {
                chat_id: 7,
                user_id: 9,
                forward_limit: 10,
                operation_id,
            })
            .await;
        assert_eq!(
            matches!(result, Err(TinkleError::Codec(_))),
            invalid,
            "{result:?}"
        );
    }
}

/// Side-effect flags require a canonical nonzero caller UUID before transport admission.
#[tokio::test]
async fn reaction_effect_identity_and_conditional_draft_pair_validate_before_network() {
    let client = Client::new(
        support::config("127.0.0.1:1".parse().unwrap()),
        support::source(),
        InMemoryCursorStore::new(),
    );
    let target = Some(p::Target {
        kind: Some(p::target::Kind::UserId(7)),
    });
    for (animate, recents) in [
        (Some(true), None),
        (None, Some(true)),
        (Some(true), Some(true)),
    ] {
        for id in ["", "invalid", "00000000-0000-0000-0000-000000000000"] {
            let result = client
                .messages()
                .send_reaction(p::SendReactionRequest {
                    target,
                    message_id: 1,
                    animate,
                    save_to_recents: recents,
                    operation_id: id.into(),
                    ..Default::default()
                })
                .await;
            assert!(matches!(result, Err(TinkleError::Codec(_))), "{result:?}");
        }
    }
    for (generation, revision, invalid) in [
        (None, None, true),
        (Some(7), None, true),
        (None, Some(0), true),
        (Some(0), Some(0), true),
        (Some(7), Some(-1), true),
        (Some(7), Some(0), false),
    ] {
        let result = client
            .messages()
            .send_message(p::SendMessageRequest {
                target,
                message: "text".into(),
                idempotency_key: 1,
                clear_draft: Some(true),
                clear_draft_dialog_id: generation,
                clear_draft_revision: revision,
                ..Default::default()
            })
            .await;
        assert_eq!(
            matches!(result, Err(TinkleError::Codec(_))),
            invalid,
            "{generation:?}/{revision:?}: {result:?}"
        );
    }
}

/// The private recents cap is checked at the typed/FFI facade; effect retries preserve exact UUID/bytes.
#[tokio::test]
async fn recents_are_bounded_and_reaction_retries_keep_the_caller_operation() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |request, _| {
            let calls = observed.clone();
            async move {
                let outcome = if request.method.ends_with("/GetRecentReactions") {
                    syntra_core::proto::rpc_result::Outcome::Payload(
                        p::GetRecentReactionsResponse {
                            reactions: vec![
                                p::Reaction {
                                    emoticon: "x".into(),
                                    ..Default::default()
                                };
                                33
                            ],
                        }
                        .encode_to_vec(),
                    )
                } else {
                    assert!(request.method.ends_with("/SendReaction"));
                    let parsed =
                        p::SendReactionRequest::decode(request.payload.as_slice()).unwrap();
                    assert_eq!(parsed.operation_id, "11111111-1111-4111-8111-111111111111");
                    calls.lock().unwrap().push(request.payload);
                    syntra_core::proto::rpc_result::Outcome::Error(syntra_core::proto::RpcError {
                        code: 14,
                        message: "unknown effect".into(),
                    })
                };
                syntra_core::proto::Frame {
                    version: 1,
                    request_id: 1,
                    body: Some(syntra_core::proto::frame::Body::Result(
                        syntra_core::proto::RpcResult {
                            outcome: Some(outcome),
                        },
                    )),
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
    let client = Client::new(config, support::source(), InMemoryCursorStore::new());
    client.set_principal_user_id(42).await;
    client.connect().await.unwrap();
    let recent = client
        .messages()
        .get_recent_reactions(p::GetRecentReactionsRequest {})
        .await;
    assert!(
        matches!(recent, Err(TinkleError::Codec(_))),
        "oversized recents were returned"
    );
    let request = p::SendReactionRequest {
        target: Some(p::Target {
            kind: Some(p::target::Kind::UserId(7)),
        }),
        message_id: 1,
        reactions: vec![p::Reaction {
            emoticon: "x".into(),
            ..Default::default()
        }],
        animate: Some(true),
        save_to_recents: Some(true),
        operation_id: "11111111-1111-4111-8111-111111111111".into(),
    };
    for _ in 0..2 {
        assert!(client
            .messages()
            .send_reaction(request.clone())
            .await
            .unwrap_err()
            .is_unknown_outcome());
    }
    let requests = calls.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
    client.close().await;
    server.await.unwrap();
}
