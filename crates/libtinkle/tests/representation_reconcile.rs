//! Encrypted candidate reconciliation must not commit progress over conflicting representation clocks.
#![cfg(tinkle_async_actions)]
mod support;
use libtinkle::{Client, Storage};
use prost::Message;
use std::sync::Arc;
use tinkle_schema::proto as p;

/// Same target, message, main and authored clock; only the independent representation differs.
fn body(revision: u64, variant: i64) -> p::Message {
    p::Message {
        message_id: 10,
        target: Some(p::Target {
            kind: Some(p::target::Kind::ChatId(7)),
        }),
        from_id: 42,
        message: "retained caption".into(),
        accept_seq: Some(1),
        body_revision: Some(1),
        media: Some(p::Media {
            media: Some(p::media::Media::Photo(p::Photo {
                file_id: 9,
                representation_revision: Some(revision),
                thumbnail: Some(p::Thumbnail {
                    file_id: variant,
                    width: 1,
                    height: 1,
                }),
                ..Default::default()
            })),
        }),
        ..Default::default()
    }
}

/// A valid representation-only page commits its selected membership. An inconsistent equal-clock
/// page fails both coalescing and the storage-merge lane without advancing memory/SQLite cursors.
#[tokio::test]
async fn encrypted_representation_pages_preserve_membership_or_stop_progress() {
    for mode in ["advance", "coalescing_conflict", "stored_conflict"] {
        let initial = body(2, 12);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = support::config(listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let handle = support::accept(stream, move |request, _| async move {
                let bytes = match request.method.rsplit('/').next().unwrap() {
                    "ListMessageChanges" => {
                        let r = p::ListMessageChangesRequest::decode(request.payload.as_slice())
                            .unwrap();
                        let bodies = match mode {
                            "advance" => vec![body(2, 12), body(3, 13)],
                            "coalescing_conflict" => vec![body(3, 13), body(3, 14)],
                            _ => vec![body(2, 13)],
                        };
                        p::ListMessageChangesResponse {
                            changes: if r.after_accept_seq == 0 {
                                bodies
                                    .into_iter()
                                    .enumerate()
                                    .map(|(i, m)| p::TargetChange {
                                        message_id: 10,
                                        kind: p::TargetChangeKind::Representation as i32,
                                        message_revision: Some(1),
                                        representation_revision: match m
                                            .media
                                            .as_ref()
                                            .unwrap()
                                            .media
                                            .as_ref()
                                            .unwrap()
                                        {
                                            p::media::Media::Photo(v) => v.representation_revision,
                                            _ => None,
                                        },
                                        message: Some(m),
                                        accept_seq: i as i64 + 1,
                                        ..Default::default()
                                    })
                                    .collect()
                            } else {
                                vec![]
                            },
                            last_accept_seq: if mode == "stored_conflict" { 1 } else { 2 },
                            more_available: Some(false),
                        }
                        .encode_to_vec()
                    }
                    "GetMessages" => p::GetMessagesResponse::default().encode_to_vec(),
                    "GetHistory" => p::GetHistoryResponse {
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
                            outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(bytes)),
                        },
                    )),
                }
            })
            .await
            .unwrap();
            while !handle.is_closed() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            handle.close().await;
        });
        let storage = Arc::new(Storage::open_in_memory().unwrap());
        storage
            .apply_journal_page_fenced(
                "user:42",
                "target:room:7",
                &tinkle_storage::CacheOps {
                    messages: vec![tinkle_storage::MessageRow {
                        conversation_key: "target:room:7".into(),
                        message_id: 10,
                        peer_kind: "room".into(),
                        peer_id: 7,
                        from_id: 42,
                        body: initial.message.clone(),
                        proto: initial.encode_to_vec(),
                        origin_stream: "target:room:7".into(),
                        accept_seq: Some(1),
                        edit_date_secs: None,
                        edit_date_nanos: None,
                    }],
                    message_body_revisions: vec![(10, 1)],
                    files: [9, 12]
                        .into_iter()
                        .map(|file_id| tinkle_storage::FileRow {
                            scope: String::new(),
                            file_id,
                            size_bytes: 0,
                            sha256: vec![],
                            mime_type: "image/png".into(),
                            file_name: String::new(),
                            encrypted: false,
                            created_at: 0,
                            origin_stream: "target:room:7".into(),
                        })
                        .collect(),
                    message_files: [9, 12]
                        .into_iter()
                        .map(|file_id| tinkle_storage::MessageFileRef {
                            conversation_key: "target:room:7".into(),
                            message_id: 10,
                            file_scope: String::new(),
                            file_id,
                            role: if file_id == 9 { "photo" } else { "thumbnail" }.into(),
                        })
                        .collect(),
                    ..Default::default()
                },
                0,
                0,
            )
            .unwrap();
        let client = Client::new_sqlite(config, support::source(), storage.clone());
        client.set_principal_user_id(42).await;
        client.connect().await.unwrap();
        client
            .subscribe(vec![p::Target {
                kind: Some(p::target::Kind::ChatId(7)),
            }])
            .await
            .unwrap();
        let retained = p::Message::decode(
            storage
                .get_messages("user:42", "target:room:7", 10, None)
                .unwrap()[0]
                .proto
                .as_slice(),
        )
        .unwrap();
        if mode == "advance" {
            assert_eq!(retained, body(3, 13));
            assert_eq!(
                storage.load_journal("user:42", "target:room:7").unwrap(),
                Some(2)
            );
            let refs = storage
                .list_message_files("user:42", "target:room:7", 10)
                .unwrap();
            assert!(refs.iter().any(|r| r.file_id == 13));
            assert!(!refs.iter().any(|r| matches!(r.file_id, 12 | 14)));
        } else {
            assert_eq!(retained, initial, "{mode}");
            assert_eq!(
                storage.load_journal("user:42", "target:room:7").unwrap(),
                Some(0),
                "{mode}"
            );
            assert_eq!(
                client.target_cursor("target:room:7").await,
                Some(0),
                "{mode}"
            );
            assert!(client.reconcile_target("target:room:7").await.is_err());
            assert_eq!(storage.file_refcount("user:42", "", 13).unwrap(), 0);
            assert_eq!(storage.file_refcount("user:42", "", 14).unwrap(), 0);
            assert_eq!(storage.file_refcount("user:42", "", 12).unwrap(), 1);
        }
        client.close().await;
        server.await.unwrap();
    }
}
