//! Separate-account cache isolation against an encrypted peer with caller-scoped deletion state.
//! The peer models the public visibility contract; it is not production-service acceptance.
mod support;

use libtinkle::{Client, Storage};
use prost::Message as _;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tinkle_schema::proto as p;

/// The caller's view uses its peer ID, not the sender's view of the same direct conversation.
fn target(actor: i64) -> p::Target {
    p::Target {
        kind: Some(p::target::Kind::UserId(if actor == 1 { 2 } else { 1 })),
    }
}

/// One immutable acceptance identity is visible independently in each participant's view.
fn body(actor: i64) -> p::Message {
    p::Message {
        message_id: 10,
        from_id: 1,
        target: Some(target(actor)),
        message: "privateviewneedle".into(),
        accept_seq: Some(1),
        body_revision: Some(1),
        ..Default::default()
    }
}

/// Each listener is bound by the fixture to one actor, never by a request's descriptive labels.
async fn serve(
    listener: tokio::net::TcpListener,
    actor: i64,
    deleted: Arc<AtomicBool>,
    stale_history: Arc<AtomicBool>,
) {
    for _ in 0..2 {
        let (stream, _) = listener.accept().await.unwrap();
        let deleted = deleted.clone();
        let stale_history = stale_history.clone();
        let handle = support::accept(stream, move |request, _| {
            let deleted = deleted.clone();
            let stale_history = stale_history.clone();
            async move {
                let hidden = actor == 2 && deleted.load(Ordering::SeqCst);
                let payload = match request.method.rsplit('/').next().unwrap() {
                    "GetHistory" => {
                        let req = p::GetHistoryRequest::decode(request.payload.as_slice()).unwrap();
                        assert_eq!(req.target, Some(target(actor)));
                        p::GetHistoryResponse {
                            messages: if !hidden || stale_history.load(Ordering::SeqCst) {
                                vec![body(actor)]
                            } else {
                                vec![]
                            },
                            more_available: Some(false),
                            ..Default::default()
                        }
                        .encode_to_vec()
                    }
                    "GetMessages" => {
                        let req =
                            p::GetMessagesRequest::decode(request.payload.as_slice()).unwrap();
                        assert_eq!(req.target, Some(target(actor)));
                        p::GetMessagesResponse {
                            messages: if hidden { vec![] } else { vec![body(actor)] },
                            ..Default::default()
                        }
                        .encode_to_vec()
                    }
                    "DeleteMessages" => {
                        let req =
                            p::DeleteMessagesRequest::decode(request.payload.as_slice()).unwrap();
                        assert_eq!(actor, 2);
                        assert_eq!(req.target, Some(target(actor)));
                        assert_eq!(req.message_ids, vec![10]);
                        // Even revoke=true cannot revoke another sender's message for everyone.
                        assert_eq!(req.revoke, Some(true));
                        deleted.store(true, Ordering::SeqCst);
                        p::DeleteMessagesResponse { deleted_count: 1 }.encode_to_vec()
                    }
                    "ListMessageChanges" => {
                        let req = p::ListMessageChangesRequest::decode(request.payload.as_slice())
                            .unwrap();
                        assert_eq!(req.target, Some(target(actor)));
                        p::ListMessageChangesResponse {
                            changes: if hidden && req.after_accept_seq < 2 {
                                vec![p::TargetChange {
                                    message_id: 10,
                                    kind: p::TargetChangeKind::Deleted as i32,
                                    accept_seq: 2,
                                    message_revision: Some(2),
                                    message: None,
                                    body_invalidated: false,
                                }]
                            } else {
                                vec![]
                            },
                            // The other viewer examines a filtered position without receiving deletion.
                            last_accept_seq: 2,
                            more_available: Some(false),
                        }
                        .encode_to_vec()
                    }
                    other => panic!("unexpected method {other}"),
                };
                syntra_core::proto::Frame {
                    version: 1,
                    request_id: 1,
                    body: Some(syntra_core::proto::frame::Body::Result(
                        syntra_core::proto::RpcResult {
                            outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(
                                payload,
                            )),
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
    }
}

/// Deleting incoming content affects only B, survives reconnect/reopen and target rebuild,
/// and relies on server visibility after a complete account-cache wipe has erased local evidence.
#[tokio::test]
async fn incoming_user_delete_isolated_across_network_recovery_rebuild_and_wipe() {
    let a_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a_config = support::config(a_listener.local_addr().unwrap());
    let b_config = support::config(b_listener.local_addr().unwrap());
    let deleted = Arc::new(AtomicBool::new(false));
    let stale_history = Arc::new(AtomicBool::new(false));
    let a_server = tokio::spawn(serve(a_listener, 1, deleted.clone(), stale_history.clone()));
    let b_server = tokio::spawn(serve(b_listener, 2, deleted.clone(), stale_history.clone()));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache.sqlite");
    let storage = Arc::new(Storage::open(path.to_str().unwrap()).unwrap());
    let a = Client::new_sqlite(a_config.clone(), support::source(), storage.clone());
    let b = Client::new_sqlite(b_config.clone(), support::source(), storage.clone());
    a.set_principal_user_id(1).await;
    b.set_principal_user_id(2).await;
    a.connect().await.unwrap();
    b.connect().await.unwrap();
    let history = |actor| p::GetHistoryRequest {
        target: Some(target(actor)),
        limit: 10,
        ..Default::default()
    };
    assert_eq!(a.load_history(history(1)).await.unwrap().messages.len(), 1);
    assert_eq!(b.load_history(history(2)).await.unwrap().messages.len(), 1);
    b.messages()
        .delete_messages(p::DeleteMessagesRequest {
            target: Some(target(2)),
            message_ids: vec![10],
            revoke: Some(true),
        })
        .await
        .unwrap();
    a.close().await;
    b.close().await;
    drop((a, b, storage));

    let storage = Arc::new(Storage::open(path.to_str().unwrap()).unwrap());
    let a = Client::new_sqlite(a_config, support::source(), storage.clone());
    let b = Client::new_sqlite(b_config, support::source(), storage.clone());
    a.set_principal_user_id(1).await;
    b.set_principal_user_id(2).await;
    a.connect().await.unwrap();
    b.connect().await.unwrap();
    // No push is required: fresh reconciliation obtains B's private removal marker.
    a.subscribe(vec![target(1)]).await.unwrap();
    b.subscribe(vec![target(2)]).await.unwrap();
    assert_eq!(
        storage
            .get_messages("user:1", "target:user:2", 10, None)
            .unwrap()
            .len(),
        1
    );
    assert!(storage
        .get_messages("user:2", "target:user:1", 10, None)
        .unwrap()
        .is_empty());
    assert!(b
        .get_messages(target(2), vec![10])
        .await
        .unwrap()
        .messages
        .is_empty());
    // A delayed body cannot defeat retained local removal evidence, even after revalidation.
    stale_history.store(true, Ordering::SeqCst);
    assert!(b
        .load_history(history(2))
        .await
        .unwrap()
        .messages
        .is_empty());
    storage.mark_rebuild("user:2", "target:user:1").unwrap();
    storage
        .install_snapshot("user:2", "target:user:1", 0, 0, None)
        .unwrap();
    assert!(b
        .load_history(history(2))
        .await
        .unwrap()
        .messages
        .is_empty());
    // Once local evidence is wiped, current server filtering is the authority, not a local claim.
    stale_history.store(false, Ordering::SeqCst);
    storage.clear_principal("user:2").unwrap();
    assert!(b
        .load_history(history(2))
        .await
        .unwrap()
        .messages
        .is_empty());
    b.reconcile_now().await.unwrap();
    assert!(b
        .load_history(history(2))
        .await
        .unwrap()
        .messages
        .is_empty());
    assert_eq!(a.load_history(history(1)).await.unwrap().messages.len(), 1);
    assert_eq!(
        storage
            .get_messages("user:1", "target:user:2", 10, None)
            .unwrap()
            .len(),
        1
    );
    a.close().await;
    b.close().await;
    // Offline cache search cannot reveal the caller-deleted body, while A's copy remains searchable.
    for (principal, count) in [("user:1", 1), ("user:2", 0)] {
        assert_eq!(
            storage
                .search_messages(
                    principal,
                    &tinkle_storage::MessageSearchRequest {
                        query: "privateviewneedle".into(),
                        ..Default::default()
                    }
                )
                .unwrap()
                .hits
                .len(),
            count
        );
    }
    a_server.await.unwrap();
    b_server.await.unwrap();
}
