//! Private continuation-lifetime, trust and corrupt-cache regressions for shared Search.
use super::*;
use crate::{ClientConfig, ClientInfo, InMemoryCursorStore, ServerTrust, TrustSource};

/// Local connection absence/loss is eligible; even a remote Unavailable diagnostic
/// claiming network loss cannot grant fallback through status-code or text matching.
#[test]
fn cache_fallback_requires_a_known_local_error_variant() {
    assert!(cache_allowed(&TinkleError::NotConnected(
        "connection absent".into()
    )));
    assert!(cache_allowed(&TinkleError::NetworkUnavailable));
    for message in [
        "offline",
        "network unavailable",
        "current READ authority unavailable",
    ] {
        assert!(!cache_allowed(&TinkleError::Syntra(
            syntra_core::SyntraError::Rpc {
                code: syntra_core::ErrorCode::Unavailable,
                raw_code: None,
                message: message.into(),
            }
        )));
    }
}

/// Test cache initialized through the same projector as history/sync, never a fake response.
fn fixture() -> (Client, Arc<Storage>) {
    let storage = Arc::new(Storage::open_in_memory().unwrap());
    let mut ops = tinkle_storage::CacheOps::default();
    for id in 1..=3 {
        let message = p::Message {
            message_id: id,
            target: Some(p::Target {
                kind: Some(p::target::Kind::ChatId(7)),
            }),
            from_id: 42,
            message: "hello".into(),
            accept_seq: Some(id),
            ..Default::default()
        };
        crate::cache_projector::merge_ops(
            &mut ops,
            crate::cache_projector::project_message_to_ops("target:room:7", &message).unwrap(),
        );
    }
    storage
        .apply_journal_page_fenced("user:42", "target:room:7", &ops, 0, 0)
        .unwrap();
    let client = Client::new_sqlite(
        ClientConfig::new(
            "localhost",
            ClientInfo::new(
                "device".into(),
                "test".into(),
                "1".into(),
                "1".into(),
                "1".into(),
                Default::default(),
            )
            .unwrap(),
        ),
        TrustSource::Bootstrap(ServerTrust {
            server_public_key: [1; 32],
            server_key_id: 7,
            scope: "test".into(),
        }),
        storage.clone(),
    );
    (client, storage)
}

/// Empty cursors are new searches; arbitrary, expired and evicted handles never reach a backend.
#[tokio::test]
async fn handles_expire_are_bounded_and_trust_rejection_is_terminal() {
    let (client, storage) = fixture();
    client.set_principal_user_id(42).await;
    let req = p::SearchRequest {
        query: "hello".into(),
        limit: 1,
        ..Default::default()
    };
    let first = search(&client, req.clone()).await.unwrap();
    let handle = first.next_cursor.unwrap();
    for _ in 0..MAX_HANDLES {
        search(&client, req.clone()).await.unwrap();
    }
    assert_eq!(
        client.search_continuations.lock().unwrap().0.len(),
        MAX_HANDLES
    );
    assert!(search(
        &client,
        p::SearchRequest {
            cursor: Some(handle),
            ..req.clone()
        }
    )
    .await
    .is_err());
    let next = search(&client, req.clone())
        .await
        .unwrap()
        .next_cursor
        .unwrap();
    {
        let mut table = client.search_continuations.lock().unwrap();
        Arc::get_mut(table.0.get_mut(&next).unwrap())
            .unwrap()
            .expires = Instant::now() - Duration::from_secs(1);
    }
    assert!(search(
        &client,
        p::SearchRequest {
            cursor: Some(next),
            ..req.clone()
        }
    )
    .await
    .is_err());
    for cursor in ["remote-raw", "tinkle-search:v1:forged"] {
        assert!(search(
            &client,
            p::SearchRequest {
                cursor: Some(cursor.into()),
                ..req.clone()
            }
        )
        .await
        .is_err());
    }
    client
        .endpoint_rejected
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(
        search(&client, req.clone()).await,
        Err(TinkleError::TrustRejected)
    ));
    client
        .endpoint_rejected
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let cacheless = Client::new(
        client.config().clone(),
        TrustSource::Bootstrap(ServerTrust {
            server_public_key: [1; 32],
            server_key_id: 7,
            scope: "test".into(),
        }),
        InMemoryCursorStore::new(),
    );
    cacheless.set_principal_user_id(42).await;
    assert!(matches!(
        search(&cacheless, req.clone()).await,
        Err(TinkleError::NotConnected(_))
    ));
    storage.clear_principal("user:42").unwrap();
    assert!(search(&client, req).await.unwrap().messages.is_empty());
}

/// A malformed cached protobuf is a typed error, not an empty successful page.
#[tokio::test]
async fn malformed_cache_is_not_a_miss_and_missing_dates_do_not_match() {
    let (client, storage) = fixture();
    client.set_principal_user_id(42).await;
    let req = p::SearchRequest {
        query: "hello".into(),
        limit: 20,
        min_date: 1,
        ..Default::default()
    };
    assert!(search(&client, req.clone())
        .await
        .unwrap()
        .messages
        .is_empty());
    let mut row = storage
        .get_messages("user:42", "target:room:7", 1, None)
        .unwrap()
        .remove(0);
    row.proto = vec![255];
    row.edit_date_secs = Some(1);
    row.edit_date_nanos = Some(0);
    storage
        .apply_journal_page_fenced(
            "user:42",
            "target:room:7",
            &tinkle_storage::CacheOps {
                messages: vec![row],
                ..Default::default()
            },
            0,
            0,
        )
        .unwrap();
    assert!(matches!(
        search(&client, p::SearchRequest { min_date: 0, ..req }).await,
        Err(TinkleError::Sync(_))
    ));
}
