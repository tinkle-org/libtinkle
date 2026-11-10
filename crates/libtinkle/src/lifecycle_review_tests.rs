//! Selected admission/classification/join races against the actual published TCP SDK.
use super::*;
use crate::SecureEnrollmentStore;
use prost::Message;
use std::sync::{atomic::AtomicUsize, Condvar, Mutex};
use std::time::Duration;
use syntra_core::secure_channel::{self, Carrier, Mode, Secret32};
use syntra_runtime::{FnHandler, HandlerOutcome};
use tinkle_schema::proto::*;

#[path = "retirement_loss_tests.rs"]
mod retirement_loss_tests;

/// Synthetic Returning material is confined to this encrypted fixture.
struct Keys;
impl secure_channel::KeyStore for Keys {
    /// Resolve the synthetic Returning key shared only by this fixture.
    fn lookup(&self, _: &secure_channel::Context) -> Result<Secret32, secure_channel::Error> {
        Ok(Secret32::new([9; 32]))
    }
}

/// A synthetic retained transport has no active caller, but still needs a real joined shutdown.
fn abandoned_caller() -> CancelToken {
    let gone = CancelToken::new();
    gone.cancel();
    gone
}

/// Retain the peer task and admission counts; never print protected fixture records.
struct Fixture {
    client: Client,
    store: Option<Arc<crate::enrollment::review_tests::ReviewStore>>,
    admissions: Arc<AtomicUsize>,
    deny_heartbeat: Arc<AtomicBool>,
    deny_review_next: Arc<AtomicBool>,
    review_reads: Arc<AtomicUsize>,
    denied_calls: Arc<AtomicUsize>,
    hold_returning: Arc<AtomicBool>,
    returning_ready: Arc<tokio::sync::Notify>,
    observations: Arc<AtomicUsize>,
    server: AbortTask,
}

impl Fixture {
    /// Accept a prescribed mode sequence without guessing or implementing any wire framing.
    async fn new(modes: Vec<Mode>, enabled_observer: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (key, pin) = secure_channel::generate_server_key().unwrap();
        let trust = crate::ServerTrust {
            server_public_key: pin,
            server_key_id: 7,
            scope: "lifecycle-review".into(),
        };
        let info = crate::ClientInfo::new(
            "fixture".into(),
            "test".into(),
            "1".into(),
            "1".into(),
            "1".into(),
            Default::default(),
        )
        .unwrap();
        let mut config = ClientConfig::new("127.0.0.1", info);
        config.primary_port = listener.local_addr().unwrap().port();
        config.fallback_port = None;
        let source = if modes[0] == Mode::Returning {
            TrustSource::Returning {
                trust: trust.clone(),
                enrollment: crate::Enrollment::new([9; 32], [8; 8], 1).unwrap(),
            }
        } else {
            TrustSource::Bootstrap(trust.clone())
        };
        let client = Client::new(config, source, crate::InMemoryCursorStore::new());
        let store = if enabled_observer {
            let (store, status) = crate::enrollment::review_tests::observation_store(&client).await;
            client.bind_enrollment_store(store.clone()).await.unwrap();
            client
                .retirement
                .bind(trust, &status, store.clone(), false, true);
            Some(store)
        } else {
            None
        };
        let admissions = Arc::new(AtomicUsize::new(0));
        let accepted = admissions.clone();
        let deny_heartbeat = Arc::new(AtomicBool::new(false));
        let denial = deny_heartbeat.clone();
        let deny_review_next = Arc::new(AtomicBool::new(false));
        let review_denial = deny_review_next.clone();
        let review_reads = Arc::new(AtomicUsize::new(0));
        let reads = review_reads.clone();
        let denied_calls = Arc::new(AtomicUsize::new(0));
        let denied = denied_calls.clone();
        let hold_returning = Arc::new(AtomicBool::new(false));
        let hold_proof = hold_returning.clone();
        let returning_ready = Arc::new(tokio::sync::Notify::new());
        let proof_ready = returning_ready.clone();
        let observations = Arc::new(AtomicUsize::new(0));
        let observed = observations.clone();
        let server = AbortTask::new(tokio::spawn(async move {
            let mut peers = tokio::task::JoinSet::new();
            for mode in modes {
                let (stream, _) = listener.accept().await.unwrap();
                accepted.fetch_add(1, Ordering::SeqCst);
                let security = secure_channel::ServerConfig {
                    mode,
                    carrier: Carrier::Tcp,
                    server_key_id: 7,
                    scope: b"lifecycle-review".to_vec(),
                    private_key: Secret32::new(*key.expose()),
                    store: Some(Arc::new(Keys)),
                };
                let denial = denial.clone();
                let review_denial = review_denial.clone();
                let reads = reads.clone();
                let denied = denied.clone();
                let hold_proof = hold_proof.clone();
                let proof_ready = proof_ready.clone();
                let observed = observed.clone();
                peers.spawn(async move {
                    let handler = Arc::new(FnHandler(move |method: String, bytes: Vec<u8>, _| {
                        let denial = denial.clone();
                        let review_denial = review_denial.clone();
                        let reads = reads.clone();
                        let denied = denied.clone();
                        let observed = observed.clone();
                        async move {
                            let wrapped =
                                method == tinkle_schema::methods::connectionservice::INIT_CONNECT;
                            let (method, bytes) = if wrapped {
                                let init = InitConnectRequest::decode(bytes.as_slice()).unwrap();
                                (init.method, init.payload.to_vec())
                            } else {
                                (method, bytes)
                            };
                            if method == "/example.Access/Denied" {
                                denied.fetch_add(1, Ordering::SeqCst);
                                return HandlerOutcome::error(
                                    syntra_core::ErrorCode::PermissionDenied,
                                    "fixture per-RPC access denial",
                                );
                            }
                            if method == tinkle_schema::methods::connectionservice::KEEP_ALIVE
                                && denial.load(Ordering::SeqCst)
                            {
                                return HandlerOutcome::error(
                                    syntra_core::ErrorCode::PermissionDenied,
                                    "fixture heartbeat denial",
                                );
                            }
                            let payload = match method.as_str() {
                                tinkle_schema::methods::accountservice::GET_SESSIONS => {
                                    reads.fetch_add(1, Ordering::SeqCst);
                                    if review_denial.swap(false, Ordering::SeqCst) {
                                        return HandlerOutcome::error(
                                            syntra_core::ErrorCode::PermissionDenied,
                                            "fixture review denial",
                                        );
                                    }
                                    GetSessionsResponse { sessions: vec![] }.encode_to_vec()
                                }
                                tinkle_schema::methods::authservice::GET_ENROLLMENT_DISPOSITION => {
                                    observed.fetch_add(1, Ordering::SeqCst);
                                    let req =
                                        GetEnrollmentDispositionRequest::decode(bytes.as_slice())
                                            .unwrap();
                                    GetEnrollmentDispositionResponse {
                                        reference: req.reference,
                                        disposition: DeviceEnrollmentDisposition::Unknown as i32,
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
                                _ => vec![],
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
                    let factory: Arc<dyn syntra_net::AuthenticatorFactory> = Arc::new(
                        |_: &secure_channel::VerifiedIdentity| -> Result<
                            Arc<dyn syntra_runtime::Authenticator>,
                            syntra_runtime::AuthError,
                        > { Ok(Arc::new(syntra_runtime::AllowAnonymous)) },
                    );
                    if mode == Mode::Returning && hold_proof.load(Ordering::SeqCst) {
                        proof_ready.notified().await;
                    }
                    let handle = syntra_net::tcp::accept_stream(
                        stream,
                        &security,
                        factory,
                        handler,
                        Default::default(),
                        Default::default(),
                    )
                    .await
                    .unwrap();
                    while !handle.is_closed() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    handle.close().await;
                });
            }
            while let Some(result) = peers.join_next().await {
                result.unwrap();
            }
        }));
        Self {
            client,
            store,
            admissions,
            deny_heartbeat,
            deny_review_next,
            review_reads,
            denied_calls,
            hold_returning,
            returning_ready,
            observations,
            server,
        }
    }

    /// Settle automatic review before selecting lifecycle races.
    async fn connect(&self) {
        self.client.connect().await.unwrap();
        let stream = self.client.subscribe_session_state().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while stream.next().await.unwrap().sessions.is_none() {}
        })
        .await
        .unwrap();
        stream.stop();
        self.client.arm_retirement_schedule().await;
    }

    /// Wait for an actual UNKNOWN response and its client-side evidence classification.
    async fn unknown(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.client.session_status().await.enrollment_observation
                != crate::EnrollmentObservation::Unknown
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
/// Pending loss evidence is deferred admission, not a failed or destructive reconnect attempt.
async fn pending_loss_does_not_charge_or_park_supervisor() {
    let mut fixture = Fixture::new(
        vec![Mode::Returning, Mode::Bootstrap, Mode::Returning],
        true,
    )
    .await;
    fixture.connect().await;
    let client = &fixture.client;
    let observer = client.retirement.hold_observation().await;
    let generation = client.session_generation().unwrap();
    client
        .fail_liveness(&generation, &TinkleError::NetworkUnavailable)
        .await;
    assert!(client.retirement.has_pending_loss());
    client.reestablish().await;
    {
        let sup = client.sup_state.lock().await;
        assert!(
            !sup.reconnect_blocked,
            "pending observation permanently parked recovery"
        );
        assert_eq!(sup.reconnect_failures, 0);
        assert!(sup.next_reestablish.is_none());
    }
    assert!(
        client
            .live
            .read()
            .await
            .as_ref()
            .is_some_and(|live| Arc::ptr_eq(&live.generation, &generation)),
        "deferred admission tore down the captured generation"
    );
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 1);
    drop(observer);
    fixture.unknown().await;
    client.reestablish().await;
    assert!(client.is_connected().await);
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 3);
    assert!(fixture
        .store
        .as_ref()
        .unwrap()
        .load_active()
        .await
        .unwrap()
        .is_some());
    client.stop().await;
    fixture.server.shutdown().await;
}

#[tokio::test]
/// The observer wins the dial lock and cancels liveness, but must not erase its terminal decision.
async fn observer_cannot_cancel_terminal_failure_classification() {
    let mut fixture = Fixture::new(
        vec![Mode::Returning, Mode::Bootstrap, Mode::Returning],
        true,
    )
    .await;
    fixture.connect().await;
    let client = &fixture.client;
    let connection = client.connect_lock.lock().await;
    let observed = client.observe_restored_enrollment();
    tokio::pin!(observed);
    assert!(futures_util::poll!(&mut observed).is_pending()); // observer queued first
    let generation = client.session_generation().unwrap();
    {
        let mut live = client.live.write().await;
        let live = live.as_mut().unwrap();
        live.liveness.as_mut().unwrap().shutdown().await;
        live.liveness = Some(LivenessTask::spawn_failure(
            client.background_clone(),
            generation.clone(),
            TinkleError::Syntra(syntra_core::SyntraError::Rpc {
                code: syntra_core::ErrorCode::PermissionDenied,
                raw_code: None,
                message: "fixture denial".into(),
            }),
        ));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while generation.open.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(connection);
    observed.await.unwrap();
    fixture.unknown().await;
    assert!(
        client.sup_state.lock().await.reconnect_blocked,
        "observer cancelled the only owner of terminal classification"
    );
    client.reestablish().await;
    assert!(!client.is_connected().await);
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 2);
    assert!(!client.is_signed_out());
    assert!(fixture
        .store
        .as_ref()
        .unwrap()
        .load_active()
        .await
        .unwrap()
        .is_some());
    client.stop().await;
    fixture.server.shutdown().await;
}

#[tokio::test]
/// A late predecessor decision must not park a replacement source or its fresh generation.
async fn terminal_classification_is_fenced_from_replacement() {
    let mut fixture = Fixture::new(vec![Mode::Returning, Mode::Returning], false).await;
    fixture.connect().await;
    let client = &fixture.client;
    let previous = client.session_generation().unwrap();
    previous.record_failure(&TinkleError::TrustRejected);
    client
        .replace_trust_source(TrustSource::Returning {
            trust: client.enrollment_trust().await,
            enrollment: crate::Enrollment::new([9; 32], [8; 8], 1).unwrap(),
        })
        .await
        .unwrap();
    client.connect().await.unwrap();
    client.retain_liveness_policy(&previous).await;
    assert!(!client.sup_state.lock().await.reconnect_blocked);
    assert!(client.is_connected().await);
    assert!(!Arc::ptr_eq(
        &previous,
        &client.session_generation().unwrap()
    ));
    client.stop().await;
    fixture.server.shutdown().await;
}

#[tokio::test]
/// A wire KeepAlive denial records policy before its general denial observer is awakened.
async fn heartbeat_denial_records_policy_before_observation_wake() {
    let mut fixture = Fixture::new(
        vec![Mode::Returning, Mode::Bootstrap, Mode::Returning],
        true,
    )
    .await;
    fixture.connect().await;
    let client = &fixture.client;
    let connection = client.connect_lock.lock().await;
    let observed = client.observe_restored_enrollment();
    tokio::pin!(observed);
    assert!(futures_util::poll!(&mut observed).is_pending());
    let generation = client.session_generation().unwrap();
    fixture.deny_heartbeat.store(true, Ordering::SeqCst);
    let caller = client.for_session_generation(generation.clone());
    let error = caller
        .call_keep_alive(KeepAliveRequest { sequence: 1 }.encode_to_vec())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), syntra_core::ErrorCode::PermissionDenied);
    assert!(generation.terminal_failure.load(Ordering::SeqCst));
    assert!(client.retirement.has_pending_loss());
    // Denial already queued evidence, but heartbeat's terminal policy must
    // survive the observer winning teardown before fail_liveness completes.
    let failing = caller.fail_liveness(&generation, &error);
    tokio::pin!(failing);
    assert!(futures_util::poll!(&mut failing).is_pending());
    assert!(client.retirement.has_pending_loss());
    drop(connection);
    observed.await.unwrap();
    failing.await;
    fixture.unknown().await;
    assert!(client.sup_state.lock().await.reconnect_blocked);
    client.reestablish().await;
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 2);
    assert!(!client.is_signed_out());
    assert!(fixture
        .store
        .as_ref()
        .unwrap()
        .load_active()
        .await
        .unwrap()
        .is_some());
    client.stop().await;
    fixture.server.shutdown().await;
}

/// Keep the actual carrier's Drop unfinished beyond the SDK's shutdown grace.
#[derive(Default)]
struct DropGate {
    armed: AtomicBool,
    entered: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
}
impl DropGate {
    /// Release even after a regression assertion panics, so the runtime can join its workers.
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct ReleaseOnDrop(Arc<DropGate>);
impl Drop for ReleaseOnDrop {
    /// Release the carrier gate even when a regression assertion unwinds.
    fn drop(&mut self) {
        self.0.release();
    }
}

/// The public SDK still performs all framing and encryption on a real TCP stream.
struct HeldDropStream {
    stream: tokio::net::TcpStream,
    gate: Arc<DropGate>,
}
impl tokio::io::AsyncRead for HeldDropStream {
    /// Delegate encrypted carrier reads to the real TCP stream.
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}
impl tokio::io::AsyncWrite for HeldDropStream {
    /// Delegate encrypted carrier writes without changing their progress semantics.
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_write(cx, bytes)
    }
    /// Preserve the TCP stream's flush behavior while holding only destruction.
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }
    /// Forward shutdown so only the deliberate destructor gate delays join.
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
impl Drop for HeldDropStream {
    /// Hold armed carrier destruction until released to exercise incomplete shutdown.
    fn drop(&mut self) {
        if !self.gate.armed.load(Ordering::SeqCst) {
            return;
        }
        self.gate.entered.store(true, Ordering::SeqCst);
        let mut released = self.gate.released.lock().unwrap();
        while !*released {
            released = self.gate.changed.wait(released).unwrap();
        }
    }
}

/// Establish through a generic stream then retain a deliberately unfinished carrier owner.
async fn held_handle(client: &Client, gate: Arc<DropGate>) -> syntra_net::tcp::ClientHandle {
    let stream =
        tokio::net::TcpStream::connect((client.config.host.as_str(), client.config.primary_port))
            .await
            .unwrap();
    let security = client.trust_source.read().await.security().unwrap();
    let handle = syntra_net::tcp::connect_stream(
        HeldDropStream {
            stream,
            gate: gate.clone(),
        },
        security,
        vec![],
        client.config.connection.clone(),
        Default::default(),
    )
    .await
    .unwrap();
    gate.armed.store(true, Ordering::SeqCst);
    handle
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// Verify the exact rc.5 report using a real unfinished carrier, not a synthetic shutdown enum.
async fn rc5_incomplete_carrier_report_keeps_original_deadline() {
    let mut fixture = Fixture::new(vec![Mode::Bootstrap], false).await;
    let gate = Arc::new(DropGate::default());
    let _release = ReleaseOnDrop(gate.clone());
    let mut handle = held_handle(&fixture.client, gate.clone()).await;
    assert!(matches!(
        handle
            .shutdown_transport(Duration::from_millis(20))
            .await
            .unwrap(),
        syntra_net::TransportShutdown::ShutdownIncomplete {
            carrier_tasks: 1,
            policy_jobs: 0
        }
    ));
    assert!(!tokio::time::timeout(
        Duration::from_millis(100),
        crate::liveness::transport_joined(&mut handle)
    )
    .await
    .unwrap());
    gate.release();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !crate::liveness::transport_joined(&mut handle).await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fixture.server.shutdown().await;
}

/// Wrap a genuine SDK handle in the same native Live owner used by production teardown.
fn live_owner(client: &Client, handle: syntra_net::tcp::ClientHandle) -> Live {
    Live {
        connection_id: handle.connection_id.clone(),
        version: handle.version,
        handle,
        sync_mgr: SyncManager::new(UNKNOWN_PRINCIPAL),
        pump: AbortTask::new(tokio::spawn(async {})),
        initialized: Arc::new(Initialization::default()),
        watch_restore: None,
        generation: LiveGeneration::new(false, client.retirement.revision()),
        liveness: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// Main, provisioning and status owners must survive ShutdownIncomplete and forbid another dial.
async fn incomplete_carrier_shutdown_retains_all_socket_barriers() {
    for lane in ["main", "provisioning", "status", "establishing"] {
        let mut fixture = Fixture::new(vec![Mode::Bootstrap, Mode::Bootstrap], false).await;
        let gate = Arc::new(DropGate::default());
        let _release = ReleaseOnDrop(gate.clone());
        let client = &fixture.client;
        let handle = held_handle(client, gate.clone()).await;
        match lane {
            "main" => {
                let live = live_owner(client, handle);
                client.session_state.network(live.generation.clone(), true);
                client.transport_ok.store(true, Ordering::SeqCst);
                *client.live.write().await = Some(live);
            }
            "provisioning" => {
                *client.connect_lock.auxiliary.lock().unwrap() =
                    Some(Arc::new(AuxiliaryConnection {
                        live: Arc::new(RwLock::new(Some(live_owner(client, handle)))),
                        retiring: Arc::new(tokio::sync::Mutex::new(vec![])),
                        caller_gone: abandoned_caller(),
                        closing: AtomicBool::new(false),
                    }));
            }
            "status" => {
                client.retirement.install_status_transport(handle).await;
                client
                    .connect_lock
                    .status_open
                    .store(true, Ordering::SeqCst);
            }
            _ => {
                *client.connect_lock.establishing.lock().await = Some(handle);
            }
        }
        let outcome = tokio::time::timeout(Duration::from_secs(2), async {
            if lane == "main" {
                client.reconnect().await
            } else {
                client.connect().await
            }
        })
        .await
        .expect("shutdown grace must bound the caller");
        assert!(
            outcome.is_err(),
            "{lane}: unfinished carrier permitted a replacement"
        );
        assert!(gate.entered.load(Ordering::SeqCst));
        assert_eq!(
            fixture.admissions.load(Ordering::SeqCst),
            1,
            "{lane}: replacement TCP opened before join"
        );
        match lane {
            "main" => assert_eq!(client.retiring.lock().await.len(), 1),
            "provisioning" => assert!(client.connect_lock.auxiliary.lock().unwrap().is_some()),
            "status" => {
                assert!(client.connect_lock.status_open.load(Ordering::SeqCst));
                assert!(client.retirement.has_status_transport().await);
                // A child shares this gate but has no authority over the parent's
                // status handle. Closing its empty slot cannot attest parent join.
                let mut child = Client::new(
                    client.config.clone(),
                    TrustSource::Bootstrap(client.enrollment_trust().await),
                    crate::InMemoryCursorStore::new(),
                );
                child.connect_lock = client.connect_lock.clone();
                child.provisioning_context = Some((
                    client.retirement.clone(),
                    client.retirement.endpoint_revision(),
                ));
                child.close().await;
                assert!(client.connect_lock.status_open.load(Ordering::SeqCst));
                assert!(client.retirement.has_status_transport().await);
            }
            _ => assert!(client.connect_lock.establishing.lock().await.is_some()),
        }
        // The SDK's original shutdown deadline is sticky; retry must not renew it.
        assert!(
            tokio::time::timeout(Duration::from_millis(250), client.connect())
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(fixture.admissions.load(Ordering::SeqCst), 1);
        gate.release();
        tokio::time::timeout(Duration::from_secs(2), async {
            while client.connect().await.is_err() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("joined completion must allow a later dial");
        assert_eq!(fixture.admissions.load(Ordering::SeqCst), 2);
        client.stop().await;
        fixture.server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// Unfinished transport cleanup cannot suppress protected invalidation or become falsely Complete.
async fn logout_retries_retained_join_without_repeating_completed_storage() {
    let mut fixture = Fixture::new(vec![Mode::Bootstrap], false).await;
    let gate = Arc::new(DropGate::default());
    let _release = ReleaseOnDrop(gate.clone());
    let client = &fixture.client;
    let store = Arc::new(crate::enrollment::review_tests::ReviewStore::default());
    client.prepare_enrollment(store.clone()).await.unwrap();
    let handle = held_handle(client, gate.clone()).await;
    *client.live.write().await = Some(live_owner(client, handle));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), client.logout())
            .await
            .unwrap(),
        Err(TinkleError::NetworkUnavailable)
    ));
    assert!(client.is_signed_out());
    assert!(store.tombstone.load(Ordering::SeqCst));
    assert_eq!(store.clears.load(Ordering::SeqCst), 1);
    assert_eq!(
        client.session_status().await.cleanup,
        crate::SessionCleanup::Failed
    );
    let stream = client.subscribe_session_state().await.unwrap();
    assert_eq!(
        stream.next().await.unwrap().reason,
        Some(crate::SessionSignOutReason::SelfLogout)
    );
    assert_eq!(client.retiring.lock().await.len(), 1);
    gate.release();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(confirmed) = client.logout().await {
                assert!(!confirmed);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        client.session_status().await.cleanup,
        crate::SessionCleanup::Complete
    );
    assert_eq!(store.clears.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 1);
    fixture.server.shutdown().await;
}

/// Construct the same private Bootstrap child/context as the provisioning facade.
async fn provisioning_child(parent: &Client) -> Client {
    let mut child = Client::new(
        parent.config.clone(),
        TrustSource::Bootstrap(parent.enrollment_trust().await),
        crate::InMemoryCursorStore::new(),
    );
    child.connect_lock = parent.connect_lock.clone();
    child.endpoint_rejected = parent.endpoint_rejected.clone();
    child.provisioning_context = Some((
        parent.retirement.clone(),
        parent.retirement.endpoint_revision(),
    ));
    child
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// Context changes must fence current Live even when a different retained owner cannot join.
async fn review_round2_context_changes_fence_live_before_incomplete_join() {
    for action in ["principal", "activation", "trust", "auxiliary-policy"] {
        let mut fixture = Fixture::new(vec![Mode::Returning, Mode::Returning], false).await;
        let client = &fixture.client;
        client.set_principal_user_id(42).await;
        fixture.connect().await;
        let generation = client.session_generation().unwrap();
        let gate = Arc::new(DropGate::default());
        let _release = ReleaseOnDrop(gate.clone());
        let handle = held_handle(client, gate.clone()).await;
        if action == "auxiliary-policy" {
            *client.connect_lock.auxiliary.lock().unwrap() = Some(Arc::new(AuxiliaryConnection {
                live: Arc::new(RwLock::new(Some(live_owner(client, handle)))),
                retiring: Arc::new(tokio::sync::Mutex::new(vec![])),
                caller_gone: abandoned_caller(),
                closing: AtomicBool::new(false),
            }));
            generation.record_failure(&TinkleError::TrustRejected);
            let _connection = client.connect_lock.lock().await;
            assert!(!client.teardown_connection().await);
        } else {
            *client.connect_lock.establishing.lock().await = Some(handle);
            match action {
                "principal" => client.set_principal_user_id(43).await,
                "activation" => client
                    .retire_for_activation(
                        &client.enrollment_trust().await,
                        &client.authorization.generation().unwrap(),
                    )
                    .await
                    .unwrap(),
                _ => client
                    .replace_trust_source(TrustSource::Bootstrap(client.enrollment_trust().await))
                    .await
                    .unwrap(),
            }
        }
        assert!(
            !generation.open.load(Ordering::SeqCst),
            "{action}: context changed while old Live remained open"
        );
        assert!(generation.retiring.load(Ordering::SeqCst));
        assert!(client.live.read().await.is_none());
        assert!(!client.transport_ok.load(Ordering::SeqCst));
        assert!(!client.is_connected().await);
        assert_ne!(
            client.session_status().await.authorization,
            crate::SessionAuthorization::Active
        );
        assert!(client
            .retiring
            .lock()
            .await
            .iter()
            .any(|live| Arc::ptr_eq(&live.generation, &generation)));
        if action == "auxiliary-policy" {
            assert!(client.sup_state.lock().await.reconnect_blocked);
        }
        gate.release();
        let _connection = client.connect_lock.lock().await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while !client.teardown_connection().await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        fixture.server.shutdown().await;
    }
}

#[tokio::test]
/// Active provisioning is protected; cancelling its last caller must permit automatic parent recovery.
async fn review_round2_automatic_recovery_drains_abandoned_child_only() {
    let mut fixture = Fixture::new(
        vec![Mode::Returning, Mode::Bootstrap, Mode::Returning],
        true,
    )
    .await;
    fixture.connect().await;
    let client = &fixture.client;
    let child = client
        .provisioning_client(
            &client.enrollment_trust().await,
            &client.authorization.generation().unwrap(),
        )
        .await
        .unwrap();
    let gone = child.owner_gone();
    // Exercise the real registration. The fallback also isolates this admission
    // regression when run against the independent pre-dial slot-release defect.
    let registration = client
        .connect_lock
        .auxiliary
        .lock()
        .unwrap()
        .get_or_insert_with(|| {
            Arc::new(AuxiliaryConnection {
                live: child.live.clone(),
                retiring: child.retiring.clone(),
                caller_gone: gone.clone(),
                closing: AtomicBool::new(false),
            })
        })
        .clone();
    client.reestablish().await;
    assert!(
        client.reconnect().await.is_err(),
        "manual admission also must not steal an active child"
    );
    client.observe_restored_enrollment().await.unwrap();
    assert!(!gone.is_cancelled());
    assert!(
        child.is_connected().await,
        "automatic admission cancelled an active provisioning call"
    );
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 2);
    assert!(client
        .connect_lock
        .auxiliary
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|slot| Arc::ptr_eq(slot, &registration)));
    drop(child);
    assert!(gone.is_cancelled());
    assert!(registration.caller_gone.is_cancelled());
    assert!(!client.owner_gone().is_cancelled());
    assert!(
        client.connect_lock.auxiliary.lock().unwrap().is_some(),
        "dropping the caller is not a joined teardown"
    );
    client.reestablish().await;
    assert!(
        client.is_connected().await,
        "cancelled provisioning permanently deferred automatic recovery"
    );
    assert!(client.connect_lock.auxiliary.lock().unwrap().is_none());
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 3);
    assert_eq!(client.sup_state.lock().await.reconnect_failures, 0);
    client.stop().await;
    fixture.server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// Only the parent may join its cancelled status socket before registering/dialing a child.
async fn review_round2_provisioning_parent_drains_cancelled_status() {
    let mut fixture = Fixture::new(vec![Mode::Bootstrap, Mode::Bootstrap], false).await;
    let client = &fixture.client;
    let gate = Arc::new(DropGate::default());
    let _release = ReleaseOnDrop(gate.clone());
    client
        .retirement
        .install_status_transport(held_handle(client, gate.clone()).await)
        .await;
    client
        .connect_lock
        .status_open
        .store(true, Ordering::SeqCst);
    let child = provisioning_child(client).await;
    assert!(client.connect_provisioning_child(&child).await.is_err());
    assert!(
        gate.entered.load(Ordering::SeqCst),
        "provisioning never attempted the parent's cancelled status join"
    );
    assert!(
        client.connect_lock.auxiliary.lock().unwrap().is_none(),
        "child registered before parent status join completed"
    );
    assert!(client.retirement.has_status_transport().await);
    assert!(client.connect_lock.status_open.load(Ordering::SeqCst));
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 1);
    gate.release();
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.connect_provisioning_child(&child).await.is_err() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(child.is_connected().await);
    assert!(!client.retirement.has_status_transport().await);
    assert!(!client.connect_lock.status_open.load(Ordering::SeqCst));
    child.close().await;
    client.stop().await;
    fixture.server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// Pre-dial retirement cannot withdraw the child's registered owner, even before Live installation.
async fn review_round2_provisioning_slot_retained_during_dial_and_after_ready() {
    let mut fixture = Fixture::new(
        vec![Mode::Bootstrap, Mode::Bootstrap, Mode::Bootstrap],
        false,
    )
    .await;
    let client = &fixture.client;
    let child = provisioning_child(client).await;
    let installing = child.interests.lock().await;
    let connecting = client.connect_provisioning_child(&child);
    tokio::pin!(connecting);
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = &mut connecting => panic!("child installation must remain held"),
            _ = async { while fixture.admissions.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; } } => {},
        }
    }).await.unwrap();
    let registration = client.connect_lock.auxiliary.lock().unwrap().clone();
    assert!(
        registration
            .as_ref()
            .is_some_and(|slot| Arc::ptr_eq(&slot.live, &child.live)),
        "child's owner was cleared before its first dial"
    );
    assert!(child.live.read().await.is_none());
    drop(installing);
    connecting.await.unwrap();
    assert!(client
        .connect_lock
        .auxiliary
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|slot| Arc::ptr_eq(slot, registration.as_ref().unwrap())));
    assert!(child.is_connected().await);
    // Replace the test child's socket with a real carrier whose join stays
    // unfinished, preserving the same registered lease through pre-dial cleanup.
    let gate = Arc::new(DropGate::default());
    let _release = ReleaseOnDrop(gate.clone());
    {
        let _connection = client.connect_lock.lock().await;
        assert!(child.teardown_connection().await);
        assert!(client.connect_lock.auxiliary.lock().unwrap().is_some());
        let live = live_owner(&child, held_handle(&child, gate.clone()).await);
        child.session_state.network(live.generation.clone(), true);
        child.transport_ok.store(true, Ordering::SeqCst);
        *child.live.write().await = Some(live);
    }
    tokio::time::timeout(Duration::from_secs(2), child.close())
        .await
        .unwrap();
    assert!(
        client
            .connect_lock
            .auxiliary
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|slot| Arc::ptr_eq(slot, registration.as_ref().unwrap())),
        "incomplete close withdrew the registered join owner"
    );
    assert_eq!(child.retiring.lock().await.len(), 1);
    assert!(
        !child.owner_gone().is_cancelled(),
        "the explicit close, not object drop, ends admission"
    );
    assert!(matches!(
        child.check_call_admission(tinkle_schema::methods::authservice::RESUME_ENROLLMENT),
        Err(TinkleError::NotConnected(_))
    ));
    client.reestablish().await;
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 2);
    assert!(client.connect_lock.auxiliary.lock().unwrap().is_some());
    assert_eq!(client.sup_state.lock().await.reconnect_failures, 0);
    gate.release();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !client.is_connected().await {
            client.reestablish().await;
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        client.connect_lock.auxiliary.lock().unwrap().is_none(),
        "joined parent adoption must release the child's slot"
    );
    assert!(child.retiring.lock().await.is_empty());
    assert!(
        child.connect().await.is_err(),
        "a retired child cannot reopen outside its registered lease"
    );
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 3);
    client.stop().await;
    fixture.server.shutdown().await;
}

#[tokio::test]
/// Cancelling the provisioning constructor after SDK readiness retains both registration and handle.
async fn review_round2_cancelled_provisioning_constructor_retains_ready_owner() {
    let mut fixture = Fixture::new(vec![Mode::Bootstrap, Mode::Bootstrap], false).await;
    let client = &fixture.client;
    let child = provisioning_child(client).await;
    let gone = child.owner_gone();
    let live = child.live.clone();
    let installing = child.interests.clone().lock_owned().await;
    let parent = client.clone();
    let connecting = tokio::spawn(async move { parent.connect_provisioning_child(&child).await });
    tokio::time::timeout(
        Duration::from_secs(2),
        client.connect_lock.ready_transport.notified(),
    )
    .await
    .unwrap();
    let registration = client
        .connect_lock
        .auxiliary
        .lock()
        .unwrap()
        .clone()
        .expect("pre-install registration must remain owned");
    assert!(Arc::ptr_eq(&registration.live, &live));
    assert!(live.read().await.is_none());
    connecting.abort();
    assert!(connecting.await.unwrap_err().is_cancelled());
    assert!(gone.is_cancelled());
    assert!(registration.caller_gone.is_cancelled());
    assert!(
        client.connect_lock.establishing.lock().await.is_some(),
        "cancelled SDK-ready handle must remain join-owned"
    );
    assert!(client
        .connect_lock
        .auxiliary
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|slot| Arc::ptr_eq(slot, &registration)));
    drop(installing);
    client.reestablish().await;
    assert!(client.is_connected().await);
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 2);
    assert!(client.connect_lock.establishing.lock().await.is_none());
    assert!(client.connect_lock.auxiliary.lock().unwrap().is_none());
    assert_eq!(client.sup_state.lock().await.reconnect_failures, 0);
    client.stop().await;
    fixture.server.shutdown().await;
}

/// Denial and UNKNOWN cannot grant authorization; only fresh Returning resumes automatic review.
async fn permission_denial_recovers_review(deny_review: bool) {
    let mut fixture = Fixture::new(
        vec![Mode::Returning, Mode::Bootstrap, Mode::Returning],
        true,
    )
    .await;
    fixture.connect().await;
    let client = &fixture.client;
    client.start(vec![]).await.unwrap();
    let generation = client.session_generation().unwrap();
    let states = client.subscribe_session_state().await.unwrap();
    let observer = client.retirement.hold_observation().await;
    fixture.hold_returning.store(true, Ordering::SeqCst);
    if deny_review {
        fixture.deny_review_next.store(true, Ordering::SeqCst);
        client.session_state.dirty(Some(generation.clone()));
    } else {
        let error = client
            .call_raw("/example.Access/Denied", vec![], 1_000)
            .await
            .unwrap_err();
        assert_eq!(error.code(), syntra_core::ErrorCode::PermissionDenied);
    }
    let denied = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = states.next().await.unwrap();
            if state.status.authorization == crate::SessionAuthorization::Unknown {
                break state;
            }
        }
    })
    .await
    .unwrap();
    assert!(denied.sessions.is_none());
    assert!(
        client.retirement.has_pending_loss(),
        "PermissionDenied stranded automatic review without authenticated recovery"
    );
    drop(observer);
    fixture.unknown().await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while fixture.admissions.load(Ordering::SeqCst) != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("supervisor must attempt fresh Returning without another hint");
    assert_eq!(
        client.session_status().await.authorization,
        crate::SessionAuthorization::Unknown,
        "UNKNOWN disposition or unauthenticated TCP admission granted authorization"
    );
    assert!(!client.is_signed_out());
    assert!(fixture
        .store
        .as_ref()
        .unwrap()
        .load_active()
        .await
        .unwrap()
        .is_some());
    fixture.hold_returning.store(false, Ordering::SeqCst);
    fixture.returning_ready.notify_one();
    let recovered = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let state = states.next().await.unwrap();
            if state.sessions.is_some() {
                break state;
            }
        }
    })
    .await
    .expect("fresh Returning must resume automatic GetSessions");
    assert_eq!(
        recovered.status.authorization,
        crate::SessionAuthorization::Active
    );
    assert!(!Arc::ptr_eq(
        &client.session_generation().unwrap(),
        &generation
    ));
    assert_eq!(fixture.observations.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.review_reads.load(Ordering::SeqCst),
        2 + usize::from(deny_review)
    );
    assert_eq!(
        fixture.denied_calls.load(Ordering::SeqCst),
        usize::from(!deny_review)
    );
    states.stop();
    client.stop().await;
    fixture.server.shutdown().await;
}

#[tokio::test]
/// An ordinary PermissionDenied recovers through evidence without replaying the denied RPC.
async fn permission_denial_recovers_automatic_review() {
    permission_denial_recovers_review(false).await;
}

#[tokio::test]
/// PermissionDenied from GetSessions itself cannot permanently strand the review worker.
async fn permission_denial_during_review_recovers_automatic_review() {
    permission_denial_recovers_review(true).await;
}

/// Retain an actual joined failed Returning generation while selecting a later deferral.
async fn queue_joined_loss(client: &Client) -> Arc<LiveGeneration> {
    let generation = client.session_generation().unwrap();
    client
        .fail_liveness(&generation, &TinkleError::NetworkUnavailable)
        .await;
    let mut live = client.live.write().await;
    assert!(crate::liveness::transport_joined(&mut live.as_mut().unwrap().handle).await);
    generation
}

#[tokio::test]
/// A live child must not cause a proof storm or strand a consumed loss wake until the sixty-second tick.
async fn review_round3_child_deferral_retries_promptly_after_close() {
    let mut fixture = Fixture::new(
        vec![
            Mode::Returning,
            Mode::Bootstrap,
            Mode::Bootstrap,
            Mode::Returning,
        ],
        true,
    )
    .await;
    fixture.connect().await;
    let client = &fixture.client;
    let observer = client.retirement.hold_observation().await;
    queue_joined_loss(client).await;
    let child = provisioning_child(client).await;
    let gate = Arc::new(DropGate::default());
    let _release = ReleaseOnDrop(gate.clone());
    let handle = held_handle(&child, gate.clone()).await;
    gate.release(); // this child is active, not a deliberately stalled shutdown
    *child.live.write().await = Some(live_owner(&child, handle));
    *client.connect_lock.auxiliary.lock().unwrap() = Some(Arc::new(AuxiliaryConnection {
        live: child.live.clone(),
        retiring: child.retiring.clone(),
        caller_gone: child.owner_gone(),
        closing: AtomicBool::new(false),
    }));
    drop(observer);
    tokio::time::timeout(
        Duration::from_secs(2),
        client.retirement.scheduled_attempt_finished(),
    )
    .await
    .unwrap();
    assert!(client.retirement.has_pending_loss());
    assert_eq!(client.retirement.scheduled_attempt_count(), 1);
    assert_eq!(fixture.observations.load(Ordering::SeqCst), 0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        client.retirement.scheduled_attempt_count(),
        1,
        "active child caused immediate self-notification spin"
    );
    child.close().await;
    fixture.unknown().await; // three seconds, well before the fallback tick
    assert!(!client.retirement.has_pending_loss());
    assert_eq!(fixture.observations.load(Ordering::SeqCst), 1);
    client.reconnect().await.unwrap();
    assert!(client.is_connected().await);
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 4);
    client.stop().await;
    fixture.server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// An incomplete status join keeps its pending loss, then retries after joined completion without another minute parked.
async fn review_round3_incomplete_status_join_retries_pending_loss() {
    let mut fixture = Fixture::new(
        vec![
            Mode::Returning,
            Mode::Bootstrap,
            Mode::Bootstrap,
            Mode::Returning,
        ],
        true,
    )
    .await;
    fixture.connect().await;
    let client = &fixture.client;
    let observer = client.retirement.hold_observation().await;
    queue_joined_loss(client).await;
    let status_client = provisioning_child(client).await;
    let gate = Arc::new(DropGate::default());
    let _release = ReleaseOnDrop(gate.clone());
    client
        .retirement
        .install_status_transport(held_handle(&status_client, gate.clone()).await)
        .await;
    client
        .connect_lock
        .status_open
        .store(true, Ordering::SeqCst);
    drop(observer);
    tokio::time::timeout(
        Duration::from_secs(3),
        client.retirement.scheduled_attempt_finished(),
    )
    .await
    .unwrap();
    assert!(gate.entered.load(Ordering::SeqCst));
    assert!(client.retirement.has_pending_loss());
    assert!(client.retirement.has_status_transport().await);
    assert_eq!(fixture.observations.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 2);
    gate.release();
    fixture.unknown().await;
    assert!(!client.retirement.has_pending_loss());
    client.reconnect().await.unwrap();
    assert!(client.is_connected().await);
    assert_eq!(fixture.observations.load(Ordering::SeqCst), 1);
    client.stop().await;
    fixture.server.shutdown().await;
}

#[tokio::test]
/// Spending an attempt's original five seconds waiting for the dial gate must schedule another bounded attempt.
async fn review_round3_expired_connect_gate_wait_retries_pending_loss() {
    let mut fixture = Fixture::new(
        vec![Mode::Returning, Mode::Bootstrap, Mode::Returning],
        true,
    )
    .await;
    fixture.connect().await;
    let client = &fixture.client;
    let connection = client.connect_lock.lock().await;
    let started = std::time::Instant::now();
    client.observe_denial(&TinkleError::Syntra(syntra_core::SyntraError::Rpc {
        code: syntra_core::ErrorCode::Unauthenticated,
        raw_code: None,
        message: "fixture credential rejection".into(),
    }));
    tokio::time::timeout(
        Duration::from_secs(7),
        client.retirement.scheduled_attempt_finished(),
    )
    .await
    .unwrap();
    assert!(
        started.elapsed() >= Duration::from_secs(5),
        "the attempt did not spend its original five-second budget"
    );
    assert!(client.retirement.has_pending_loss());
    assert_eq!(fixture.observations.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 1);
    drop(connection);
    fixture.unknown().await;
    assert!(!client.retirement.has_pending_loss());
    client.reconnect().await.unwrap();
    assert!(client.is_connected().await);
    assert_eq!(fixture.observations.load(Ordering::SeqCst), 1);
    client.stop().await;
    fixture.server.shutdown().await;
}

#[tokio::test]
/// Pending retry ownership must be cancelled by suspension/local logout, never converted into a later dial.
async fn review_round3_suspend_and_logout_cancel_pending_retry() {
    for sign_out in [false, true] {
        let mut fixture = Fixture::new(vec![Mode::Returning, Mode::Bootstrap], true).await;
        fixture.connect().await;
        let client = &fixture.client;
        let observer = client.retirement.hold_observation().await;
        client.request_loss_observation(client.session_generation().unwrap());
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.retirement.scheduled_attempt_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let retiring = async {
            if sign_out {
                client.logout().await.unwrap();
            } else {
                client.suspend().await;
            }
        };
        tokio::pin!(retiring);
        assert!(futures_util::poll!(&mut retiring).is_pending());
        drop(observer);
        retiring.await;
        assert!(!client.retirement.has_pending_loss());
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::time::resume();
        tokio::task::yield_now().await;
        assert_eq!(fixture.observations.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.admissions.load(Ordering::SeqCst), 1);
        assert!(client.is_locally_suspended());
        fixture.server.shutdown().await;
    }
}
