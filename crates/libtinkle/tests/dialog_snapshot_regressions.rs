//! Delayed encrypted dialog snapshots cannot revive caller-removed views or retired cache scopes.
mod support;
use libtinkle::{Client, Storage, TinkleError};
use prost::Message as _;
use std::sync::Arc;
use tinkle_schema::proto as p;

/// A dialog response must retain its pre-request principal/cache proof, reject a removed head,
/// and propagate storage errors instead of returning unfiltered bodies or partially hydrating.
#[tokio::test]
async fn held_dialog_snapshot_is_fenced_across_deletion_rebuild_wipe_and_principal_change() {
    for mode in [
        "delete",
        "body_delete",
        "rebuild",
        "wipe",
        "switch",
        "storage",
        "other_principal",
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = support::config(listener.local_addr().unwrap());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let ready = entered.clone();
        let gate = release.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let handle = support::accept(stream, move |request, _| {
                let ready = ready.clone();
                let gate = gate.clone();
                async move {
                    assert!(request.method.ends_with("/GetDialogs"));
                    let target = Some(p::Target {
                        kind: Some(p::target::Kind::UserId(7)),
                    });
                    let response = p::GetDialogsResponse {
                        dialogs: vec![p::Dialog {
                            target,
                            top_message: if mode == "body_delete" { 2 } else { 1 },
                            unread_count: 9,
                            draft: Some(p::DraftMessage {
                                message: "retained draft".into(),
                                ..Default::default()
                            }),
                            draft_dialog_id: Some(77),
                            draft_revision: Some(0),
                            ..Default::default()
                        }],
                        messages: vec![p::Message {
                            target,
                            message_id: 1,
                            from_id: 7,
                            message: "removed body".into(),
                            accept_seq: Some(1),
                            ..Default::default()
                        }],
                        users: vec![p::User {
                            user_type: Some(p::user::UserType::Empty(p::user::Empty { id: 7 })),
                        }],
                        ..Default::default()
                    };
                    ready.notify_one();
                    gate.acquire().await.unwrap().forget();
                    syntra_core::proto::Frame {
                        version: 1,
                        request_id: 1,
                        body: Some(syntra_core::proto::frame::Body::Result(
                            syntra_core::proto::RpcResult {
                                outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(
                                    response.encode_to_vec(),
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
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite");
        let storage = Arc::new(Storage::open(path.to_str().unwrap()).unwrap());
        // No cached body exists: deletion must be caught by durable identity checks, not only
        // a revision increment from deleting a cached message or creating an unknown scope.
        storage.save_journal("user:42", "target:user:7", 0).unwrap();
        let client = Client::new_sqlite(config, support::source(), storage.clone());
        client.set_principal_user_id(42).await;
        client.connect().await.unwrap();
        let caller = client.clone();
        let task =
            tokio::spawn(async move { caller.load_dialogs(p::GetDialogsRequest::default()).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        match mode {
            "delete" | "body_delete" | "other_principal" => storage.apply_journal_page_fenced(
                if mode=="other_principal" {"user:99"} else {"user:42"},"target:user:7",
                &tinkle_storage::CacheOps {remove_messages:vec![("target:user:7".into(),1)],..Default::default()},1,0).unwrap(),
            "rebuild" => { storage.mark_rebuild("user:42","target:user:7").unwrap(); storage.install_snapshot("user:42","target:user:7",0,0,None).unwrap(); }
            "wipe" => { storage.clear_principal("user:42").unwrap(); storage.install_snapshot("user:42","target:user:7",0,0,None).unwrap(); }
            "switch" => client.set_principal_user_id(99).await,
            "storage" => rusqlite::Connection::open(&path).unwrap().execute_batch("CREATE TRIGGER fail_dialog BEFORE INSERT ON dialogs BEGIN SELECT RAISE(ABORT,'fixture failure'); END;").unwrap(),
            _=>unreachable!(),
        }
        release.add_permits(1);
        let result = task.await.unwrap();
        if mode == "other_principal" {
            let response = result.unwrap();
            assert_eq!(response.dialogs[0].unread_count, 9);
            assert_eq!(response.dialogs[0].draft_revision, Some(0));
            assert_eq!(storage.list_dialogs("user:42", 10).unwrap().len(), 1);
        } else {
            assert!(
                matches!(
                    result,
                    Err(TinkleError::Sync(_)
                        | TinkleError::SignedOut
                        | TinkleError::CursorStore(_))
                ),
                "{mode}: {result:?}"
            );
            assert!(
                storage.list_dialogs("user:42", 10).unwrap().is_empty(),
                "{mode}: stale head committed"
            );
            assert!(
                storage.get_user("user:42", 7).unwrap().is_none(),
                "{mode}: partial entity write"
            );
        }
        assert!(storage.list_dialogs("user:99", 10).unwrap().is_empty());
        client.close().await;
        server.await.unwrap();
    }
}
