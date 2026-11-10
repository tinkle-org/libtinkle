//! Real encrypted TCP review reads, typed Any hints, and uncertain confirmation ordering.
#[path = "../tests/support/mod.rs"]
#[allow(dead_code)] // Shared Bootstrap helpers are unused by this Returning-only fixture.
mod support;
use crate::{Client, InMemoryCursorStore, TrustSource};
use prost::Message;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use syntra_core::secure_channel::{self, Mode};
use syntra_runtime::{FnHandler, HandlerOutcome};
use tinkle_schema::proto::*;
use tokio::{net::TcpListener, sync::Notify};

const CURRENT: &str = "96007910-fd5b-4ef2-bf0c-2e4ae1d44b1b";
const OTHER: &str = "78e5c289-656b-4428-a091-2dddf6828465";

/// Synthetic fixture key only; the published SDK still owns channel cryptography.
struct Keys;
impl secure_channel::KeyStore for Keys {
    /// Return the fixed synthetic key for the Returning fixture.
    fn lookup(
        &self,
        _: &secure_channel::Context,
    ) -> Result<secure_channel::Secret32, secure_channel::Error> {
        Ok(secure_channel::Secret32::new([9; 32]))
    }
}

/// Response gates are outside the client and can hold a committed stale snapshot or mutation reply.
#[derive(Default)]
struct State {
    sessions: Mutex<Vec<Session>>,
    reads: AtomicUsize,
    active_reads: AtomicUsize,
    max_reads: AtomicUsize,
    hold_read: AtomicBool,
    fail_read: AtomicBool,
    read_entered: Notify,
    read_release: Notify,
    confirm_entered: Notify,
    confirm_release: Notify,
}

/// Count in-flight read handlers even when the SDK cancels a request future.
struct ReadGuard(Arc<State>);
impl Drop for ReadGuard {
    /// Decrement the in-flight read count when the handler exits.
    fn drop(&mut self) {
        self.0.active_reads.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Neutral display data, including current/confirmed eligibility and an unconfirmed other login.
fn sessions() -> Vec<Session> {
    vec![
        Session {
            id: CURRENT.into(),
            current: true,
            ..Default::default()
        },
        Session {
            id: OTHER.into(),
            unconfirmed: true,
            app_build: "00042".into(),
            ..Default::default()
        },
    ]
}

/// Accept one encrypted Returning generation using actual published transport/runtime APIs.
async fn accept(listener: TcpListener, state: Arc<State>) -> syntra_net::tcp::HostHandle {
    let (stream, _) = listener.accept().await.unwrap();
    let mut security = support::server_security();
    security.mode = Mode::Returning;
    security.store = Some(Arc::new(Keys));
    let handler = Arc::new(FnHandler(move |method: String, bytes: Vec<u8>, _| {
        let state = state.clone();
        async move {
            let wrapped = method == tinkle_schema::methods::connectionservice::INIT_CONNECT;
            let (method, bytes) = if wrapped {
                let init = InitConnectRequest::decode(bytes.as_slice()).unwrap();
                (init.method, init.payload.to_vec())
            } else {
                (method, bytes)
            };
            let payload = match method.as_str() {
                tinkle_schema::methods::accountservice::GET_SESSIONS => {
                    let inflight = state.active_reads.fetch_add(1, Ordering::SeqCst) + 1;
                    state.max_reads.fetch_max(inflight, Ordering::SeqCst);
                    let _guard = ReadGuard(state.clone());
                    let snapshot = state.sessions.lock().unwrap().clone();
                    state.reads.fetch_add(1, Ordering::SeqCst);
                    state.read_entered.notify_one();
                    if state.hold_read.swap(false, Ordering::SeqCst) {
                        state.read_release.notified().await;
                    }
                    if state.fail_read.swap(false, Ordering::SeqCst) {
                        return HandlerOutcome::error(
                            syntra_core::ErrorCode::Unavailable,
                            "fixture retry",
                        );
                    }
                    GetSessionsResponse { sessions: snapshot }.encode_to_vec()
                }
                tinkle_schema::methods::accountservice::CONFIRM_SESSION => {
                    let req = ConfirmSessionRequest::decode(bytes.as_slice()).unwrap();
                    assert_eq!(req.session_id, OTHER);
                    state.confirm_entered.notify_one();
                    state.confirm_release.notified().await;
                    ConfirmSessionResponse {
                        session: Some(Session {
                            id: OTHER.into(),
                            unconfirmed: false,
                            ..Default::default()
                        }),
                    }
                    .encode_to_vec()
                }
                tinkle_schema::methods::connectionservice::KEEP_ALIVE => {
                    let req = KeepAliveRequest::decode(bytes.as_slice()).unwrap();
                    KeepAliveResponse {
                        sequence: req.sequence,
                    }
                    .encode_to_vec()
                }
                _ => {
                    return HandlerOutcome::error(
                        syntra_core::ErrorCode::Unimplemented,
                        "fixture method",
                    )
                }
            };
            HandlerOutcome::Payload(if wrapped {
                InitConnectResponse {
                    payload: payload.into(),
                }
                .encode_to_vec()
            } else {
                payload
            })
        }
    }));
    syntra_net::tcp::accept_stream(
        stream,
        &security,
        support::factory(),
        handler,
        Default::default(),
        Default::default(),
    )
    .await
    .unwrap()
}

/// Connect without attaching a consumer, proving review work is Client-owned rather than UI-owned.
async fn fixture(state: Arc<State>) -> (Client, syntra_net::tcp::HostHandle) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Client::new(
        support::config(listener.local_addr().unwrap()),
        TrustSource::Returning {
            trust: support::trust(),
            enrollment: libtinkle::Enrollment::new([9; 32], [8; 8], 1).unwrap(),
        },
        InMemoryCursorStore::new(),
    );
    let server = tokio::spawn(accept(listener, state));
    // Production reconnect/restore paths may connect through a temporary
    // background clone. Its release must not terminate the Client's review owner.
    let connecting = client.background_clone();
    connecting.connect().await.unwrap();
    drop(connecting);
    (client, server.await.unwrap())
}

/// Wait on notifications/snapshots with a finite fixture budget, not UI polling of GetSessions.
async fn review(stream: &libtinkle::SessionStateSubscription) -> libtinkle::SessionState {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = stream.next().await.unwrap();
            if state.sessions.is_some() {
                return state;
            }
        }
    })
    .await
    .unwrap()
}

/// Emit the exact agreed google.protobuf.Any binding, with no local public protobuf envelope.
async fn hint(host: &syntra_runtime::Host) {
    host.notify(
        prost_types::Any {
            type_url: "type.googleapis.com/tinkle.v1.SessionChanged".into(),
            value: SessionChanged {
                session_id: OTHER.into(),
            }
            .encode_to_vec(),
        }
        .encode_to_vec(),
    )
    .await
    .unwrap();
}

#[tokio::test]
/// Contact and session notifications stay separate; a hint during the first read requires another read.
async fn startup_read_dirty_race_and_mixed_notification_dispatch() {
    let state = Arc::new(State::default());
    state.hold_read.store(true, Ordering::SeqCst);
    let (client, server) = fixture(state.clone()).await;
    state.read_entered.notified().await;
    *state.sessions.lock().unwrap() = sessions();
    hint(&server.host).await;
    server
        .host
        .notify(
            prost_types::Any {
                type_url: "type.googleapis.com/tinkle.v1.ConnectivityChanged".into(),
                value: ConnectivityChanged {
                    user_id: 42,
                    online: true,
                    known: true,
                    revision: 7,
                }
                .encode_to_vec(),
            }
            .encode_to_vec(),
        )
        .await
        .unwrap();
    assert!(
        matches!(tokio::time::timeout(Duration::from_secs(1), client.next_event()).await.unwrap(), Some(libtinkle::SyncEvent::Connectivity(value)) if value.user_id == 42)
    );
    state.read_release.notify_one();
    let stream = client.subscribe_session_state().await.unwrap();
    let snapshot = review(&stream).await;
    assert_eq!(snapshot.sessions, Some(sessions()));
    assert_eq!(state.reads.load(Ordering::SeqCst), 2);
    assert_eq!(state.max_reads.load(Ordering::SeqCst), 1);
    stream.stop();
    let late = client.subscribe_session_state().await.unwrap();
    assert_eq!(late.next().await.unwrap(), snapshot);
    client.stop().await;
    assert!(late.next().await.is_none());
    server.close().await;
}

#[tokio::test]
/// A retryable read failure keeps None until success, even without listeners, and does not clear keys.
async fn failed_review_read_retries_while_live_without_listener() {
    let state = Arc::new(State::default());
    state.fail_read.store(true, Ordering::SeqCst);
    *state.sessions.lock().unwrap() = sessions();
    let (client, server) = fixture(state.clone()).await;
    state.read_entered.notified().await;
    assert!(!client.is_signed_out());
    let stream = client.subscribe_session_state().await.unwrap();
    assert!(stream.next().await.unwrap().sessions.is_none());
    assert_eq!(review(&stream).await.sessions, Some(sessions()));
    assert_eq!(state.reads.load(Ordering::SeqCst), 2);
    assert_eq!(state.max_reads.load(Ordering::SeqCst), 1);
    assert!(client.is_connected().await);
    client.stop().await;
    server.close().await;
}

#[tokio::test]
/// Closing admission after dispatch cannot discard a valid confirmation result or repopulate the list.
async fn confirmation_success_survives_generation_closure() {
    let state = Arc::new(State::default());
    *state.sessions.lock().unwrap() = sessions();
    let (client, server) = fixture(state.clone()).await;
    let stream = client.subscribe_session_state().await.unwrap();
    review(&stream).await;
    let generation = client.session_generation().unwrap();
    let account = client.account();
    let confirming = account.confirm_session(OTHER);
    tokio::pin!(confirming);
    assert!(futures_util::poll!(&mut confirming).is_pending());
    tokio::time::timeout(Duration::from_secs(1), state.confirm_entered.notified())
        .await
        .unwrap();

    // Fence new admission while leaving the accepted RPC's response path intact.
    generation.retiring.store(true, Ordering::SeqCst);
    generation.open.store(false, Ordering::SeqCst);
    generation.closed.cancel();
    client.session_state.network(generation, false);
    state.confirm_release.notify_one();
    let confirmed = tokio::time::timeout(Duration::from_secs(1), confirming)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(confirmed.id, OTHER);
    assert!(!confirmed.current && !confirmed.unconfirmed);
    let snapshot = stream.next().await.unwrap();
    assert!(snapshot.sessions.is_none());
    assert_eq!(
        snapshot.status.authorization,
        crate::SessionAuthorization::Unknown
    );
    assert!(!client.is_signed_out());
    client.stop().await;
    server.close().await;
}

#[tokio::test]
/// A late successful confirmation cannot resurrect a session removed by a newer full snapshot.
async fn late_confirmation_reply_never_overwrites_fresher_snapshot() {
    let state = Arc::new(State::default());
    *state.sessions.lock().unwrap() = sessions();
    let (client, server) = fixture(state.clone()).await;
    let stream = client.subscribe_session_state().await.unwrap();
    review(&stream).await;
    let confirming = client.clone();
    let task = tokio::spawn(async move { confirming.account().confirm_session(OTHER).await });
    state.confirm_entered.notified().await;
    state.sessions.lock().unwrap().clear();
    hint(&server.host).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if stream.next().await.unwrap().sessions == Some(vec![]) {
                break;
            }
        }
    })
    .await
    .unwrap();
    state.confirm_release.notify_one();
    let committed = task.await.unwrap().unwrap();
    assert!(!committed.current && !committed.unconfirmed);
    stream.stop();
    let late = client.subscribe_session_state().await.unwrap();
    assert_eq!(late.next().await.unwrap().sessions, Some(vec![]));
    client.stop().await;
    server.close().await;
}
