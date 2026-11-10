//! Public retained account-state subscription regressions (no network or credentials).
use libtinkle::{
    Client, ClientConfig, ClientInfo, InMemoryCursorStore, ServerTrust, SessionAuthorization,
    SessionCleanup, SessionSignOutReason, TrustSource,
};

/// Construct an offline Bootstrap coordinator; no protected material is needed.
fn client() -> Client {
    Client::new(
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
        InMemoryCursorStore::new(),
    )
}

#[tokio::test]
/// Registration retains the initial snapshot, rejects a second consumer, and stop wakes next.
async fn single_consumer_initial_stop_and_reattach() {
    let client = client();
    let stream = client.subscribe_session_state().await.unwrap();
    let initial = stream.next().await.unwrap();
    assert_eq!(initial.status, client.session_status().await);
    assert!(initial.sessions.is_none());
    assert!(client.subscribe_session_state().await.is_err());
    let next = stream.next();
    tokio::pin!(next);
    assert!(futures_util::poll!(&mut next).is_pending());
    stream.stop();
    assert!(next.await.is_none());
    let replacement = client.subscribe_session_state().await.unwrap();
    assert_eq!(replacement.next().await.unwrap(), initial);
}

#[tokio::test]
/// With no listener (or an unread initial snapshot), completion remains available to late consumers.
async fn terminal_state_survives_no_listener_and_slow_listener() {
    for attached in [false, true] {
        let client = client();
        let stream = if attached {
            Some(client.subscribe_session_state().await.unwrap())
        } else {
            None
        };
        client.logout().await.unwrap();
        let stream = match stream {
            Some(stream) => stream,
            None => client.subscribe_session_state().await.unwrap(),
        };
        let state = stream.next().await.unwrap();
        assert_eq!(state.reason, Some(SessionSignOutReason::SelfLogout));
        assert_eq!(state.status.authorization, SessionAuthorization::SignedOut);
        assert_eq!(state.status.cleanup, SessionCleanup::Complete);
        assert_eq!(state.status, client.session_status().await);
        assert!(state.sessions.is_none());
    }
}

#[tokio::test]
/// A subscription must not anchor the Client or leave an outstanding foreign-style next blocked.
async fn client_release_terminates_waiter() {
    let client = client();
    let stream = client.subscribe_session_state().await.unwrap();
    stream.next().await.unwrap();
    drop(client);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .is_none()
    );
}
