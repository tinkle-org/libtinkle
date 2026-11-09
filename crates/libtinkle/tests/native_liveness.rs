//! Native application KeepAlive ownership, sequence and failure boundaries.
//! This fixture uses published Syntra TCP and does not implement core ping.
#[allow(dead_code)] // Shared SDK fixture also provides journal helpers for other tests.
mod support;
use libtinkle::{Client, Enrollment, ErrorCode, InMemoryCursorStore, TrustSource};
use prost::Message;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use syntra_core::secure_channel::{self, Context, KeyStore, Mode, Secret32};
use syntra_runtime::{ConnectionConfig, FnHandler, HandlerOutcome};
use tinkle_schema::proto::{
    DeviceEnrollmentStatus, InitConnectRequest, InitConnectResponse, KeepAliveRequest,
    KeepAliveResponse,
};
use tokio::net::TcpListener;

#[derive(Clone, Copy)]
enum Reply {
    Echo,
    Hang,
    Reject(ErrorCode),
    WrongSequence,
    Malformed,
}

#[derive(Default)]
struct Session {
    sequences: Mutex<Vec<u64>>,
    renewals: AtomicUsize,
    initialized: AtomicUsize,
    outstanding: AtomicUsize,
    maximum: AtomicUsize,
    last: AtomicU64,
    closed: AtomicBool,
    retire: syntra_runtime::CancelToken,
}

struct Pending(Arc<Session>);
impl Drop for Pending {
    /// Release the outstanding-request count even when a handler is cancelled.
    fn drop(&mut self) {
        self.0.outstanding.fetch_sub(1, Ordering::SeqCst);
    }
}

struct ActiveKeys;
impl KeyStore for ActiveKeys {
    /// Admit only the two fixture epochs with their matching handles and keys.
    fn lookup(&self, context: &Context) -> Result<Secret32, secure_channel::Error> {
        if !(1..=2).contains(&context.epoch) || context.handle != [context.epoch as u8 + 7; 8] {
            return Err(secure_channel::Error::Authentication);
        }
        Ok(Secret32::new([context.epoch as u8 + 8; 32]))
    }
}

/// Build Returning trust with active fixture credentials derived from the epoch.
fn active_source(epoch: u64) -> TrustSource {
    let status = DeviceEnrollmentStatus {
        operation_id: "96007910-fd5b-4ef2-bf0c-2e4ae1d44b1b".into(),
        session_id: "95007910-fd5b-4ef2-bf0c-2e4ae1d44b1b".into(),
        handle: vec![epoch as u8 + 7; 8].into(),
        enrollment_epoch: epoch,
        scope: 1,
        expires_at: Some(prost_types::Timestamp {
            seconds: 4_102_444_800,
            nanos: 0,
        }),
        state: 2,
        user_id: 42,
        account_auth_epoch: 1,
    };
    TrustSource::Returning {
        trust: support::trust(),
        enrollment: Enrollment::from_active_status([epoch as u8 + 8; 32], &status).unwrap(),
    }
}

struct Fixture {
    address: std::net::SocketAddr,
    sessions: Arc<Mutex<Vec<Arc<Session>>>>,
    listener: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    /// Abort the listener and its owned connection tasks when the fixture leaves scope.
    fn drop(&mut self) {
        self.listener.abort();
    }
}
impl Fixture {
    /// Start an encrypted listener recording initialization, sequence, and lease effects.
    /// The selected reply controls KeepAlive success, rejection, corruption, or cancellation.
    async fn new(mode: Mode, reply: Reply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let sessions = Arc::new(Mutex::new(Vec::new()));
        let recorded = sessions.clone();
        let listener = tokio::spawn(async move {
            let mut owners = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let session = Arc::new(Session::default());
                recorded.lock().unwrap().push(session.clone());
                owners.spawn(async move {
                    let mut security = support::server_security();
                    security.mode = mode;
                    security.store = Some(Arc::new(ActiveKeys));
                    let record = session.clone();
                    let handler = Arc::new(FnHandler(
                        move |method: String,
                              payload: Vec<u8>,
                              cancel: syntra_runtime::CancelToken| {
                            let session = record.clone();
                            async move {
                                let wrapped = method == "/tinkle.v1.ConnectionService/InitConnect";
                                let (method, payload) = if wrapped {
                                    let init =
                                        InitConnectRequest::decode(payload.as_slice()).unwrap();
                                    let info = init.client.unwrap();
                                    assert_eq!(info.app_build, "0001");
                                    assert!(
                                        !info.device_model.is_empty()
                                            && !info.platform.is_empty()
                                            && !info.os_version.is_empty()
                                            && !info.app_version.is_empty()
                                    );
                                    session.initialized.fetch_add(1, Ordering::SeqCst);
                                    (init.method, init.payload.to_vec())
                                } else {
                                    (method, payload)
                                };
                                if method != "/tinkle.v1.ConnectionService/KeepAlive" {
                                    return HandlerOutcome::error(
                                        ErrorCode::Unimplemented,
                                        "fixture method unavailable",
                                    );
                                }
                                let request = KeepAliveRequest::decode(payload.as_slice()).unwrap();
                                session.sequences.lock().unwrap().push(request.sequence);
                                let outstanding =
                                    session.outstanding.fetch_add(1, Ordering::SeqCst) + 1;
                                session.maximum.fetch_max(outstanding, Ordering::SeqCst);
                                let _pending = Pending(session.clone());
                                // Authorization precedes even duplicate lease effects.
                                if mode != Mode::Returning {
                                    return HandlerOutcome::error(
                                        ErrorCode::Unauthenticated,
                                        "Returning required",
                                    );
                                }
                                if let Reply::Reject(code) = reply {
                                    return HandlerOutcome::error(code, "rejected");
                                }
                                assert!(session.initialized.load(Ordering::SeqCst) > 0);
                                let last = session.last.load(Ordering::SeqCst);
                                if request.sequence > last {
                                    session.last.store(request.sequence, Ordering::SeqCst);
                                    session.renewals.fetch_add(1, Ordering::SeqCst);
                                }
                                if let Reply::Hang = reply {
                                    cancel.cancelled().await;
                                    return HandlerOutcome::error(
                                        ErrorCode::Cancelled,
                                        "cancelled",
                                    );
                                }
                                let bytes = match reply {
                                    Reply::Malformed => vec![0xff],
                                    Reply::WrongSequence => KeepAliveResponse {
                                        sequence: request.sequence + 1,
                                    }
                                    .encode_to_vec(),
                                    _ => KeepAliveResponse {
                                        sequence: request.sequence,
                                    }
                                    .encode_to_vec(),
                                };
                                HandlerOutcome::Payload(if wrapped {
                                    InitConnectResponse {
                                        payload: bytes.into(),
                                    }
                                    .encode_to_vec()
                                } else {
                                    bytes
                                })
                            }
                        },
                    ));
                    if let Ok(handle) = syntra_net::tcp::accept_stream(
                        stream,
                        &security,
                        support::factory(),
                        handler,
                        ConnectionConfig::default(),
                        Default::default(),
                    )
                    .await
                    {
                        tokio::select! {
                            _ = session.retire.cancelled() => {},
                            _ = async { while !handle.is_closed() { tokio::time::sleep(Duration::from_millis(1)).await; } } => {},
                        }
                        handle.close().await;
                    }
                    session.closed.store(true, Ordering::SeqCst);
                });
                while let Some(result) = owners.try_join_next() {
                    result.unwrap();
                }
            }
        });
        Self {
            address,
            sessions,
            listener,
        }
    }
    /// Target this listener using either Bootstrap trust or epoch-one Returning credentials.
    fn client(&self, mode: Mode) -> Client {
        Client::new(
            support::config(self.address),
            if mode == Mode::Returning {
                active_source(1)
            } else {
                support::source()
            },
            InMemoryCursorStore::new(),
        )
    }
    /// Retain a recorded connection's counters for assertions after transport shutdown.
    fn session(&self, index: usize) -> Arc<Session> {
        self.sessions.lock().unwrap()[index].clone()
    }
    /// Count accepted sockets, including connections that have already closed.
    fn connections(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }
}

/// Poll a fixture observation with a two-second deadline to bound regression failures.
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("bounded fixture observation");
}

/// Advance only the native scheduler after encrypted establishment. Resume the
/// real timer before awaiting socket I/O; SDK elapsed clocks are not simulated.
async fn elapse(seconds: u64) {
    tokio::task::yield_now().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(seconds)).await;
    tokio::time::resume();
    tokio::task::yield_now().await;
}

#[tokio::test]
/// Idempotent connect must keep one initialized loop with monotonic, nonoverlapping renewals.
async fn exact_monotonic_sequences_initialize_once_and_renew_once() {
    let fixture = Fixture::new(Mode::Returning, Reply::Echo).await;
    let client = fixture.client(Mode::Returning);
    client.connect().await.unwrap();
    until(|| fixture.connections() == 1).await;
    for expected in 1..=3 {
        client.connect().await.unwrap(); // Idempotent connect must not spawn/reset a loop.
        elapse(5).await;
        until(|| {
            fixture.session(0).renewals.load(Ordering::SeqCst) == expected
                && client_pending_settled(&fixture.session(0))
        })
        .await;
        // Wait for the client-side acknowledgement before advancing its timeout.
        until_pending(&client).await;
    }
    let session = fixture.session(0);
    assert_eq!(fixture.connections(), 1);
    assert_eq!(*session.sequences.lock().unwrap(), [1, 2, 3]);
    assert_eq!(session.initialized.load(Ordering::SeqCst), 1);
    assert_eq!(session.maximum.load(Ordering::SeqCst), 1);
    client.stop().await;
    until(|| session.closed.load(Ordering::SeqCst)).await;
    elapse(15).await;
    assert_eq!(session.renewals.load(Ordering::SeqCst), 3);
    assert!(!client.is_connected().await);
}

/// Check that the server has released every outstanding KeepAlive handler.
fn client_pending_settled(session: &Session) -> bool {
    session.outstanding.load(Ordering::SeqCst) == 0
}
/// Wait up to two seconds for client-side RPC acknowledgements to settle.
async fn until_pending(client: &Client) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.pending_len().await != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

/// Wait up to two seconds for a replacement connection to reach CoreReady.
async fn until_connected(client: &Client) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !client.is_connected().await {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("replacement CoreReady");
}

#[tokio::test]
/// A locally selected cache principal must not authorize Bootstrap KeepAlive traffic.
async fn bootstrap_never_sends_keepalive_even_with_a_local_cache_principal() {
    for principal in [None, Some(42)] {
        let fixture = Fixture::new(Mode::Bootstrap, Reply::Echo).await;
        let client = fixture.client(Mode::Bootstrap);
        if let Some(principal) = principal {
            client.set_principal_user_id(principal).await;
        }
        client.start(vec![]).await.unwrap();
        elapse(16).await;
        assert!(fixture.session(0).sequences.lock().unwrap().is_empty());
        assert_eq!(fixture.session(0).renewals.load(Ordering::SeqCst), 0);
        client.stop().await;
    }
}

#[tokio::test]
/// Reconnect a closed Bootstrap transport without adding account identity or lease traffic.
async fn bootstrap_closed_transport_reconnects_without_keepalive_or_account_identity() {
    let fixture = Fixture::new(Mode::Bootstrap, Reply::Echo).await;
    let client = fixture.client(Mode::Bootstrap);
    client.start(vec![]).await.unwrap();
    let principal = client.principal().await;
    fixture.session(0).retire.cancel();
    until(|| fixture.connections() == 2).await;
    until_connected(&client).await;
    assert_eq!(client.principal().await, principal);
    for index in 0..2 {
        assert!(fixture.session(index).sequences.lock().unwrap().is_empty());
    }
    client.stop().await;
}

#[tokio::test]
/// A hung KeepAlive must time out and close without overlap or replaying its sequence.
async fn one_outstanding_unknown_outcome_closes_without_replaying_observation() {
    let fixture = Fixture::new(Mode::Returning, Reply::Hang).await;
    let client = fixture.client(Mode::Returning);
    client.connect().await.unwrap();
    elapse(5).await;
    until(|| fixture.session(0).outstanding.load(Ordering::SeqCst) == 1).await;
    elapse(4).await;
    assert_eq!(*fixture.session(0).sequences.lock().unwrap(), [1]);
    elapse(1).await;
    until(|| fixture.session(0).closed.load(Ordering::SeqCst)).await;
    assert!(!client.is_connected().await);
    elapse(15).await;
    assert_eq!(*fixture.session(0).sequences.lock().unwrap(), [1]);
    assert_eq!(fixture.session(0).renewals.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.session(0).maximum.load(Ordering::SeqCst), 1);
    client.stop().await;
}

#[tokio::test]
/// Suspend, wake, and rotation must join old loops and restart sequences only on new sockets.
async fn pause_wake_and_rotation_join_old_tasks_and_reset_only_on_new_connection() {
    let fixture = Fixture::new(Mode::Returning, Reply::Hang).await;
    let client = fixture.client(Mode::Returning);
    client.set_principal_user_id(42).await;
    client.connect().await.unwrap();
    elapse(5).await;
    until(|| fixture.session(0).outstanding.load(Ordering::SeqCst) == 1).await;
    client.suspend().await;
    until(|| fixture.session(0).closed.load(Ordering::SeqCst)).await;
    elapse(12).await;
    assert_eq!(fixture.connections(), 1);
    client.resume().await;
    client.reconnect().await.unwrap();
    elapse(5).await;
    until(|| fixture.session(1).outstanding.load(Ordering::SeqCst) == 1).await;
    // Wake without a preceding suspend must also retire this socket/task.
    client.resume().await;
    until(|| fixture.session(1).closed.load(Ordering::SeqCst)).await;
    client.connect().await.unwrap();
    elapse(5).await;
    until(|| fixture.session(2).outstanding.load(Ordering::SeqCst) == 1).await;
    client.replace_trust_source(active_source(2)).await.unwrap();
    until(|| fixture.session(2).closed.load(Ordering::SeqCst)).await;
    client.connect().await.unwrap();
    elapse(5).await;
    until(|| fixture.session(3).outstanding.load(Ordering::SeqCst) == 1).await;
    client.stop().await;
    until(|| fixture.session(3).closed.load(Ordering::SeqCst)).await;
    for index in 0..4 {
        assert_eq!(*fixture.session(index).sequences.lock().unwrap(), [1]);
        assert_eq!(fixture.session(index).outstanding.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
/// Authorization rejection or invalid echoes must close transport and park automatic reconnect.
async fn terminal_authorization_and_bad_echo_park_the_existing_supervisor() {
    for reply in [
        Reply::Reject(ErrorCode::Unauthenticated),
        Reply::Reject(ErrorCode::PermissionDenied),
        Reply::WrongSequence,
        Reply::Malformed,
    ] {
        let fixture = Fixture::new(Mode::Returning, reply).await;
        let client = fixture.client(Mode::Returning);
        client.set_principal_user_id(42).await;
        client.start(vec![]).await.unwrap();
        elapse(5).await;
        until(|| fixture.session(0).closed.load(Ordering::SeqCst)).await;
        elapse(20).await;
        assert_eq!(fixture.connections(), 1);
        assert_eq!(*fixture.session(0).sequences.lock().unwrap(), [1]);
        assert!(!client.is_connected().await);
        client.stop().await;
    }
}

#[tokio::test]
/// Transient failure must establish a new supervised connection instead of retrying the old RPC.
async fn transient_failure_schedules_fresh_supervised_connection_not_same_rpc_retry() {
    let fixture = Fixture::new(Mode::Returning, Reply::Reject(ErrorCode::Unavailable)).await;
    let client = fixture.client(Mode::Returning);
    client.set_principal_user_id(42).await;
    client.start(vec![]).await.unwrap();
    elapse(5).await;
    until(|| fixture.connections() == 2).await;
    until_connected(&client).await;
    elapse(5).await;
    until(|| fixture.connections() == 3).await;
    until_connected(&client).await;
    assert_eq!(*fixture.session(0).sequences.lock().unwrap(), [1]);
    assert_eq!(*fixture.session(1).sequences.lock().unwrap(), [1]);
    client.stop().await;
    let connections = fixture.connections();
    elapse(20).await;
    assert_eq!(fixture.connections(), connections);
}

#[tokio::test]
/// Switching accounts must cancel in-flight liveness and stop using the old Returning context.
async fn account_switch_joins_task_and_parks_old_returning_context() {
    let fixture = Fixture::new(Mode::Returning, Reply::Hang).await;
    let client = fixture.client(Mode::Returning);
    client.set_principal_user_id(42).await;
    client.start(vec![]).await.unwrap();
    elapse(5).await;
    until(|| fixture.session(0).outstanding.load(Ordering::SeqCst) == 1).await;
    client.set_principal_user_id(43).await;
    until(|| fixture.session(0).closed.load(Ordering::SeqCst)).await;
    elapse(20).await;
    assert_eq!(fixture.connections(), 1);
    assert_eq!(fixture.session(0).outstanding.load(Ordering::SeqCst), 0);
    assert!(!client.is_connected().await);
    client.stop().await;
}

#[tokio::test]
/// Dropping the final client owner must retire both manual and supervised liveness tasks.
async fn dropping_last_host_owner_releases_manual_and_supervised_connections() {
    for supervised in [false, true] {
        let fixture = Fixture::new(Mode::Returning, Reply::Hang).await;
        let client = fixture.client(Mode::Returning);
        client.set_principal_user_id(42).await;
        if supervised {
            client.start(vec![]).await.unwrap();
        } else {
            client.connect().await.unwrap();
        }
        elapse(5).await;
        until(|| fixture.session(0).outstanding.load(Ordering::SeqCst) == 1).await;
        drop(client);
        until(|| fixture.session(0).closed.load(Ordering::SeqCst)).await;
        assert_eq!(fixture.session(0).outstanding.load(Ordering::SeqCst), 0);
        elapse(15).await;
        assert_eq!(fixture.connections(), 1);
    }
}

#[tokio::test]
/// Cancelling stop must leave task ownership available for a later stop to finish joining.
async fn cancelled_stop_retains_join_ownership_for_the_next_stop() {
    for yields in [0, 1, 3, 8] {
        let fixture = Fixture::new(Mode::Returning, Reply::Hang).await;
        let client = fixture.client(Mode::Returning);
        client.connect().await.unwrap();
        elapse(5).await;
        until(|| fixture.session(0).outstanding.load(Ordering::SeqCst) == 1).await;
        let stopping = client.clone();
        let stop = tokio::spawn(async move {
            stopping.stop().await;
        });
        for _ in 0..yields {
            tokio::task::yield_now().await;
        }
        stop.abort();
        let _ = stop.await;
        client.stop().await;
        until(|| fixture.session(0).closed.load(Ordering::SeqCst)).await;
        assert_eq!(fixture.session(0).outstanding.load(Ordering::SeqCst), 0);
        elapse(15).await;
        assert_eq!(*fixture.session(0).sequences.lock().unwrap(), [1]);
        assert_eq!(fixture.connections(), 1);
    }
}

#[tokio::test]
/// Foreground start after suspension must use a new socket and restart KeepAlive sequencing.
async fn foreground_start_after_suspend_uses_a_fresh_liveness_generation() {
    let fixture = Fixture::new(Mode::Returning, Reply::Echo).await;
    let client = fixture.client(Mode::Returning);
    client.set_principal_user_id(42).await;
    client.connect().await.unwrap();
    elapse(5).await;
    until(|| fixture.session(0).renewals.load(Ordering::SeqCst) == 1).await;
    until_pending(&client).await;
    client.suspend().await;
    client.start(vec![]).await.unwrap();
    elapse(5).await;
    until(|| fixture.session(1).renewals.load(Ordering::SeqCst) == 1).await;
    until_pending(&client).await;
    assert_eq!(*fixture.session(0).sequences.lock().unwrap(), [1]);
    assert_eq!(*fixture.session(1).sequences.lock().unwrap(), [1]);
    client.stop().await;
}

#[tokio::test]
/// Logout must retire liveness and sign out even when durable cleanup cannot be confirmed.
async fn logout_joins_inflight_liveness_even_when_no_secure_store_was_bound() {
    let fixture = Fixture::new(Mode::Returning, Reply::Hang).await;
    let client = fixture.client(Mode::Returning);
    client.connect().await.unwrap();
    elapse(5).await;
    until(|| fixture.session(0).outstanding.load(Ordering::SeqCst) == 1).await;
    // Raw ACTIVE material without a registered protected namespace cannot claim
    // durable clearing, but in-memory sign-out and task retirement are mandatory.
    assert!(matches!(
        client.logout().await,
        Err(libtinkle::TinkleError::SecureStore)
    ));
    assert!(client.is_signed_out());
    until(|| fixture.session(0).closed.load(Ordering::SeqCst)).await;
    assert_eq!(fixture.session(0).outstanding.load(Ordering::SeqCst), 0);
    assert!(matches!(
        client.connect().await,
        Err(libtinkle::TinkleError::SignedOut)
    ));
    elapse(20).await;
    assert_eq!(*fixture.session(0).sequences.lock().unwrap(), [1]);
    assert_eq!(fixture.connections(), 1);
}
