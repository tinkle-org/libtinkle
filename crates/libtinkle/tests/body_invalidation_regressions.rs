//! Source-authorized body invalidation traverses typed validation, reconciliation and SQLite.
mod support;
use libtinkle::Client;
use prost::Message as _;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tinkle_schema::proto as p;

/// An identity-only marker advances its cursor without fetching withheld bodies;
/// older replies stay fenced while a later authorized same-revision body hydrates.
#[tokio::test]
async fn body_invalidation_reconciles_without_becoming_viewer_deletion() {
    let target = p::Target {
        kind: Some(p::target::Kind::ChatId(7)),
    };
    let old = p::Message {
        message_id: 1,
        target: Some(target),
        from_id: 10,
        message: "previously granted".into(),
        accept_seq: Some(1),
        body_revision: Some(1),
        ..Default::default()
    };
    let mut fresh = old.clone();
    fresh.message = "newly authorized".into();
    fresh.body_revision = Some(2);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let authorized = Arc::new(AtomicBool::new(false));
    let requests = Arc::new(AtomicUsize::new(0));
    let snapshots = Arc::new(AtomicBool::new(false));
    let allowed = authorized.clone();
    let counted = requests.clone();
    let initial = old.clone();
    let read_snapshots = snapshots.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |request, _| {
            let fresh = fresh.clone();
            let initial = initial.clone();
            let allowed = allowed.clone();
            let counted = counted.clone();
            let read_snapshots = read_snapshots.clone();
            async move {
                let payload = match request.method.rsplit('/').next().unwrap() {
                    "ListMessageChanges" => {
                        let req = p::ListMessageChangesRequest::decode(request.payload.as_slice())
                            .unwrap();
                        p::ListMessageChangesResponse {
                            changes: if req.after_accept_seq < 5 {
                                vec![p::TargetChange {
                                    message_id: 1,
                                    kind: p::TargetChangeKind::Edited as i32,
                                    message_revision: Some(2),
                                    accept_seq: 5,
                                    body_invalidated: true,
                                    ..Default::default()
                                }]
                            } else {
                                vec![]
                            },
                            last_accept_seq: 8,
                            more_available: Some(false),
                        }
                        .encode_to_vec()
                    }
                    "GetMessages" => {
                        counted.fetch_add(1, Ordering::SeqCst);
                        p::GetMessagesResponse {
                            messages: vec![if allowed.load(Ordering::SeqCst) {
                                fresh
                            } else {
                                initial
                            }],
                            ..Default::default()
                        }
                        .encode_to_vec()
                    }
                    "GetHistory" => p::GetHistoryResponse {
                        messages: if read_snapshots.load(Ordering::SeqCst) {
                            vec![if allowed.load(Ordering::SeqCst) {
                                fresh
                            } else {
                                initial
                            }]
                        } else {
                            vec![]
                        },
                        more_available: Some(false),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                    other => panic!("unexpected fixture method {other}"),
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
    });
    let storage = Arc::new(libtinkle::Storage::open_in_memory().unwrap());
    let ops = tinkle_storage::CacheOps {
        messages: vec![tinkle_storage::MessageRow {
            conversation_key: "target:room:7".into(),
            message_id: 1,
            peer_kind: "room".into(),
            peer_id: 7,
            from_id: 10,
            body: old.message.clone(),
            proto: old.encode_to_vec(),
            origin_stream: "target:room:7".into(),
            accept_seq: Some(1),
            edit_date_secs: None,
            edit_date_nanos: None,
        }],
        message_body_revisions: vec![(1, 1)],
        ..Default::default()
    };
    storage
        .apply_journal_page_fenced("user:42", "target:room:7", &ops, 0, 0)
        .unwrap();
    let client = Client::new_sqlite(config, support::source(), storage.clone());
    client.set_principal_user_id(42).await;
    client.connect().await.unwrap();
    client.subscribe(vec![target]).await.unwrap();
    assert_eq!(
        requests.load(Ordering::SeqCst),
        0,
        "invalidation must not request the withheld revision"
    );
    assert!(storage
        .get_messages("user:42", "target:room:7", 10, None)
        .unwrap()
        .is_empty());
    assert_eq!(
        storage.load_journal("user:42", "target:room:7").unwrap(),
        Some(8)
    );
    snapshots.store(true, Ordering::SeqCst);
    let stale = client
        .load_history(p::GetHistoryRequest {
            target: Some(target),
            limit: 20,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        stale.messages.is_empty(),
        "an old response cannot restore or disclose the evicted body"
    );
    authorized.store(true, Ordering::SeqCst);
    let current = client
        .load_history(p::GetHistoryRequest {
            target: Some(target),
            limit: 20,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(current.messages.len(), 1);
    assert_eq!(
        storage
            .get_messages("user:42", "target:room:7", 10, None)
            .unwrap()
            .len(),
        1
    );
    client.close().await;
    server.await.unwrap();
}
