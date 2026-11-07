//! Actual libtinkle client against externally owned Chorus production handlers.
//! No Rust transport/server stand-in and no direct authority key injection.
#![cfg(all(unix, not(target_arch = "wasm32")))]
use async_trait::async_trait;
use libtinkle::{
    Client, ClientConfig, ClientInfo, EnrollmentNamespaceSnapshot, EnrollmentObservation,
    InMemoryCursorStore, SecretBlob, SecureEnrollmentStore, SecureStoreError, SecureWrite,
    ServerTrust, SessionAuthorization, SessionCleanup, SignInDetails, TinkleError, TrustSource,
};
use prost::Message;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use syntra_core::ErrorCode;
use tinkle_schema::proto::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zeroize::Zeroizing;

/// Test host protected-store adapter: one 0600 snapshot, fsync/rename/fsync commit.
/// This tests the SDK's durable callback contract, not a production Keychain backend.
struct DiskStore {
    file: PathBuf,
    lock: Arc<Mutex<RestoreLocks>>,
    events: Mutex<Vec<SecureWrite>>,
    gate: bool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

/// OS-owned restore exclusion has no lease expiry. Cancellation IDs are shared
/// by all adapters for this namespace, so late acquisition cannot revive one.
#[derive(Default)]
struct RestoreLocks {
    held: BTreeMap<String, fs::File>,
    cancelled: BTreeSet<String>,
}

/// All CAS metadata and opaque credentials share one atomic durable snapshot.
struct Namespace {
    incarnation: String,
    revision: u64,
    cleanup_pending: bool,
    pending: BTreeSet<String>,
    rows: BTreeMap<String, Vec<u8>>,
}

impl Namespace {
    /// Revisions include a persistent incarnation, never reused by host reset.
    fn token(&self) -> String {
        format!("{}:{}", self.incarnation, self.revision)
    }
}

impl Drop for Namespace {
    fn drop(&mut self) {
        for value in self.rows.values_mut() {
            zeroize::Zeroize::zeroize(value);
        }
    }
}

impl DiskStore {
    fn new(name: &str, gate: bool) -> Arc<Self> {
        // Fresh adapters restoring the same file serialize with existing host
        // callbacks. This fixture has one host process; it is not a mobile store.
        static LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Mutex<RestoreLocks>>>>> =
            OnceLock::new();
        let file = PathBuf::from(env("TINKLE_E2E_STORE")).join(name);
        let lock = LOCKS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .entry(file.clone())
            .or_default()
            .clone();
        Arc::new(Self {
            file,
            lock,
            events: Mutex::new(Vec::new()),
            gate,
            entered: Default::default(),
            release: Default::default(),
        })
    }
    /// Every namespace mutation and install uses the same nonblocking OS lock.
    /// File ownership releases exclusion on process death, never a timer.
    fn install_lock(&self) -> Result<fs::File, SecureStoreError> {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(self.file.with_extension("install"))
            .map_err(|_| SecureStoreError)?;
        file.try_lock().map_err(|_| SecureStoreError)?;
        Ok(file)
    }
    fn read(&self) -> Result<Namespace, SecureStoreError> {
        match fs::read(&self.file) {
            Ok(raw) => {
                let (incarnation, revision, cleanup_pending, pending, rows) =
                    serde_json::from_slice(&Zeroizing::new(raw)).map_err(|_| SecureStoreError)?;
                Ok(Namespace {
                    incarnation,
                    revision,
                    cleanup_pending,
                    pending,
                    rows,
                })
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let state = Namespace {
                    incarnation: format!(
                        "native-host-{}-{}",
                        std::process::id(),
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_nanos()
                    ),
                    revision: 0,
                    cleanup_pending: false,
                    pending: BTreeSet::new(),
                    rows: BTreeMap::new(),
                };
                self.write(&state)?;
                Ok(state)
            }
            Err(_) => Err(SecureStoreError),
        }
    }
    /// File and directory fsync acknowledge both data and rename durability.
    fn write(&self, state: &Namespace) -> Result<(), SecureStoreError> {
        let raw = Zeroizing::new(
            serde_json::to_vec(&(
                &state.incarnation,
                state.revision,
                state.cleanup_pending,
                &state.pending,
                &state.rows,
            ))
            .map_err(|_| SecureStoreError)?,
        );
        let tmp = self.file.with_extension("pending");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&tmp)
            .map_err(|_| SecureStoreError)?;
        file.write_all(&raw)
            .and_then(|_| file.sync_all())
            .map_err(|_| SecureStoreError)?;
        fs::rename(&tmp, &self.file).map_err(|_| SecureStoreError)?;
        fs::File::open(self.file.parent().unwrap())
            .and_then(|dir| dir.sync_all())
            .map_err(|_| SecureStoreError)
    }
    fn load(&self, id: &str) -> Result<Option<SecretBlob>, SecureStoreError> {
        let _install = self.install_lock()?;
        let _lock = self.lock.lock().unwrap();
        let mut state = self.read()?;
        if state.cleanup_pending {
            return Err(SecureStoreError);
        }
        Ok(state.rows.remove(id).map(SecretBlob::new))
    }
    /// Retain an opaque pre-retirement snapshot for a fresh ACTIVE Returning probe.
    fn retained(&self, name: &str) -> Arc<RetainedReturningStore> {
        let retained = Self::new(name, false);
        let _install = self.install_lock().unwrap();
        let _retained_install = retained.install_lock().unwrap();
        let _lock = self.lock.lock().unwrap();
        retained.write(&self.read().unwrap()).unwrap();
        Arc::new(RetainedReturningStore(retained))
    }

    /// Explicit host reset creates a successor incarnation; old cleanup/writes
    /// must remain unable to affect it even when its numeric revision is reused.
    fn reset_namespace(&self) -> Result<(), SecureStoreError> {
        let _install = self.install_lock()?;
        let _lock = self.lock.lock().unwrap();
        let mut state = self.read()?;
        assert!(state.cleanup_pending && state.rows.is_empty());
        state.incarnation.push_str("-reset");
        state.revision = 0;
        state.cleanup_pending = false;
        self.write(&state)
    }
}

/// Negative admission probe deliberately disables status-first observation, so
/// rejection proves actual old-key Returning rather than a local tombstone.
struct RetainedReturningStore(Arc<DiskStore>);

#[async_trait]
impl SecureEnrollmentStore for RetainedReturningStore {
    async fn clear(&self) -> Result<(), SecureStoreError> {
        panic!("read-only admission probe")
    }
    async fn commit(
        &self,
        _: SecureWrite,
        _: &str,
        _: &SecretBlob,
    ) -> Result<(), SecureStoreError> {
        panic!("read-only admission probe")
    }
    async fn load_operation(&self, id: &str) -> Result<Option<SecretBlob>, SecureStoreError> {
        self.0.load(id)
    }
    async fn load_active(&self) -> Result<Option<SecretBlob>, SecureStoreError> {
        self.0.load("active")
    }
}

#[async_trait]
impl SecureEnrollmentStore for DiskStore {
    async fn begin_restore(
        &self,
        incarnation: &str,
        expected: &str,
        active: &SecretBlob,
        permit_id: &str,
    ) -> Result<bool, SecureStoreError> {
        let install = self.install_lock()?;
        let mut permits = self.lock.lock().unwrap();
        let state = self.read()?;
        if state.cleanup_pending
            || state.incarnation != incarnation
            || state.token() != expected
            || state
                .rows
                .get("active")
                .is_none_or(|current| current.as_slice() != active.expose_secret())
            || permits.cancelled.contains(permit_id)
        {
            return Ok(false);
        }
        permits.held.insert(permit_id.into(), install);
        Ok(true)
    }
    async fn end_restore(&self, permit_id: &str) -> Result<(), SecureStoreError> {
        let mut permits = self.lock.lock().unwrap();
        permits.cancelled.insert(permit_id.into());
        permits.held.remove(permit_id);
        Ok(())
    }
    async fn namespace_snapshot(
        &self,
    ) -> Result<Option<EnrollmentNamespaceSnapshot>, SecureStoreError> {
        let _install = self.install_lock()?;
        let _lock = self.lock.lock().unwrap();
        let mut state = self.read()?;
        Ok(Some(EnrollmentNamespaceSnapshot {
            incarnation: state.incarnation.clone(),
            revision: state.token(),
            cleanup_pending: state.cleanup_pending,
            active: state.rows.remove("active").map(SecretBlob::new),
            has_pending: !state.pending.is_empty(),
        }))
    }
    async fn begin_clear(&self, expected: &str) -> Result<bool, SecureStoreError> {
        let _install = self.install_lock()?;
        let _lock = self.lock.lock().unwrap();
        let mut state = self.read()?;
        if expected != state.token() {
            return Ok(false);
        }
        state.cleanup_pending = true;
        self.write(&state)?;
        Ok(true)
    }
    async fn finish_clear(&self, expected: &str) -> Result<bool, SecureStoreError> {
        let _install = self.install_lock()?;
        let _lock = self.lock.lock().unwrap();
        let mut state = self.read()?;
        if expected != state.token() || !state.cleanup_pending {
            return Ok(false);
        }
        for value in state.rows.values_mut() {
            zeroize::Zeroize::zeroize(value);
        }
        state.rows.clear();
        state.pending.clear();
        self.write(&state)?;
        Ok(true)
    }
    async fn clear(&self) -> Result<(), SecureStoreError> {
        panic!("conditional host must use begin_clear/finish_clear")
    }
    async fn commit(
        &self,
        _: SecureWrite,
        _: &str,
        _: &SecretBlob,
    ) -> Result<(), SecureStoreError> {
        panic!("conditional host must use commit_current")
    }
    async fn commit_current(
        &self,
        expected: &str,
        kind: SecureWrite,
        id: &str,
        blob: &SecretBlob,
    ) -> Result<Option<String>, SecureStoreError> {
        if kind == SecureWrite::Candidate && self.gate {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let _install = self.install_lock()?;
        let _lock = self.lock.lock().unwrap();
        let mut state = self.read()?;
        // A durable clear dominates queued/stale transactions for this namespace.
        if state.cleanup_pending || expected != state.token() {
            return Ok(None);
        }
        if (kind == SecureWrite::Operation) == state.rows.contains_key(id) {
            return Err(SecureStoreError);
        }
        state.rows.insert(id.into(), blob.expose_secret().to_vec());
        if kind == SecureWrite::Active {
            state
                .rows
                .insert("active".into(), blob.expose_secret().to_vec());
            state.pending.remove(id);
        } else {
            state.pending.insert(id.into());
        }
        state.revision += 1;
        self.write(&state)?;
        self.events.lock().unwrap().push(kind);
        Ok(Some(state.token()))
    }
    async fn load_operation(&self, id: &str) -> Result<Option<SecretBlob>, SecureStoreError> {
        self.load(id)
    }
    async fn load_active(&self) -> Result<Option<SecretBlob>, SecureStoreError> {
        self.load("active")
    }
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing fixture input {name}"))
}

/// Independently provisioned server pin; never derived from an unauthenticated socket.
fn trust() -> ServerTrust {
    let raw = env("TINKLE_E2E_SERVER_PIN");
    assert_eq!(raw.len(), 64);
    let mut pin = [0; 32];
    for (i, b) in pin.iter_mut().enumerate() {
        *b = u8::from_str_radix(&raw[2 * i..2 * i + 2], 16).unwrap();
    }
    ServerTrust {
        server_public_key: pin,
        server_key_id: env("TINKLE_E2E_SERVER_KEY_ID").parse().unwrap(),
        scope: env("TINKLE_E2E_SCOPE"),
    }
}

fn client(model: &str) -> Arc<Client> {
    client_with_trust(model, trust())
}

/// The negative probes vary only an independently provisioned trust field.
fn client_with_trust(model: &str, trust: ServerTrust) -> Arc<Client> {
    let info = ClientInfo::new(
        model.into(),
        "IOS".into(),
        "18.2".into(),
        "6.0".into(),
        "00142".into(),
        Default::default(),
    )
    .unwrap();
    let mut config = ClientConfig::new(env("TINKLE_E2E_HOST"), info);
    config.primary_port = env("TINKLE_E2E_PORT").parse().unwrap();
    config.fallback_port = None;
    config.connect_timeout = Duration::from_secs(3);
    Arc::new(Client::new(
        config,
        TrustSource::Bootstrap(trust),
        InMemoryCursorStore::new(),
    ))
}

/// Private capture HTTP only; neither phone nor OTP is placed in URLs or logs.
async fn private(path: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
    let address = env("TINKLE_E2E_CAPTURE");
    let address = address
        .strip_prefix("http://127.0.0.1:")
        .expect("loopback capture");
    let mut socket = tokio::net::TcpStream::connect(format!("127.0.0.1:{address}"))
        .await
        .unwrap();
    let token = Zeroizing::new(fs::read_to_string(env("TINKLE_E2E_CAPTURE_TOKEN_FILE")).unwrap());
    let body = Zeroizing::new(body.to_string());
    let request = Zeroizing::new(format!("POST {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", token.as_str(), body.len(), body.as_str()));
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Zeroizing::new(Vec::new());
    socket.read_to_end(&mut raw).await.unwrap();
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let code = std::str::from_utf8(&raw[..split])
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    (
        code,
        serde_json::from_slice(&raw[split + 4..]).unwrap_or(serde_json::Value::Null),
    )
}

async fn call<Req: Message, Resp: Message + Default>(
    c: &Client,
    method: &str,
    req: Req,
) -> Result<Resp, TinkleError> {
    let bytes = c.call_raw(method, req.encode_to_vec(), 3000).await?;
    Ok(Resp::decode(bytes.as_slice()).unwrap())
}

async fn sessions(c: &Client) -> Result<GetSessionsResponse, TinkleError> {
    Ok(GetSessionsResponse {
        sessions: c.account().get_sessions().await?,
    })
}

/// Drive only the actual server-owned worker, retaining fresh joined receipts.
async fn closures() -> serde_json::Value {
    let (code, result) = private("/closures", serde_json::json!({})).await;
    assert_eq!(code, 200, "production closure worker must complete");
    result
}

/// Prove a retained pre-clear key is refused by actual Returning establishment.
async fn reject_old_returning(store: Arc<RetainedReturningStore>) {
    // Respect the unchanged gateway's 2/s source admission budget. Also require
    // an actual authn denial, so a rate-limited socket cannot fake this proof.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let before = closures().await;
    let rejected = client("native-retained-negative");
    let error = rejected.restore_active_enrollment(store).await.unwrap_err();
    assert!(
        matches!(
            error,
            TinkleError::TrustRejected | TinkleError::EstablishmentFailed
        ),
        "old Returning must fail before ready"
    );
    rejected.close().await;
    let after = closures().await;
    assert_eq!(
        after["admission_denials"].as_i64().unwrap(),
        before["admission_denials"].as_i64().unwrap() + 1,
        "fresh Returning must reach the real authority and be denied"
    );
}

/// One extra production enrollment; no authority material is synthesized.
async fn enroll(model: &str, name: &str) -> (Arc<Client>, Arc<DiskStore>, DeviceEnrollmentStatus) {
    // Each enrollment uses several short-lived sockets from the same loopback
    // source. Refill the stock admission bucket without changing its limits.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let c = client(model);
    c.connect().await.unwrap();
    let store = DiskStore::new(name, false);
    let operation = c.prepare_enrollment(store.clone()).await.unwrap();
    let signed = c
        .auth()
        .sign_in(details(&c).await, &operation)
        .await
        .unwrap();
    assert!(!signed.is_new_user);
    let active = c
        .auth()
        .activate_enrollment(&signed.enrollment)
        .await
        .unwrap();
    (c, store, active)
}

/// Socket loss alone must not erase credentials. Only a separately explicit
/// foreground transition queries disposition on its own verified Bootstrap route.
async fn observe_kicked(
    c: &Client,
    store: &DiskStore,
    user: i64,
    session: &str,
    before: &serde_json::Value,
    expected_jobs: u64,
) {
    assert_eq!(before["completed"], expected_jobs);
    assert_eq!(
        before["joined_sockets"], 1,
        "worker must join the live target without client.close"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = presence(user).await;
            let events = state["events"].as_array().unwrap();
            if !c.is_connected().await
                && events
                    .iter()
                    .any(|e| e["action"] == "remove" && e["session_id"] == session)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("server-driven joined close removes the exact live generation");
    assert!(
        !c.is_signed_out(),
        "network loss is not retirement evidence"
    );
    assert!(store.load_active().await.unwrap().is_some());
    let unchanged = closures().await;
    assert_eq!(unchanged["completed"], 0);
    assert_eq!(
        unchanged["disposition_calls"], before["disposition_calls"],
        "Returning failure must not trigger a fallback probe"
    );
    c.suspend().await;
    c.resume().await;
    let status = c.session_status().await;
    assert_eq!(status.authorization, SessionAuthorization::SignedOut);
    assert_eq!(
        status.enrollment_observation,
        EnrollmentObservation::Retired
    );
    assert_eq!(status.cleanup, SessionCleanup::Complete);
    assert!(status.restart_safe_store);
    let snapshot = store.namespace_snapshot().await.unwrap().unwrap();
    assert!(snapshot.cleanup_pending && snapshot.active.is_none() && !snapshot.has_pending);
    assert!(
        store.load_active().await.is_err(),
        "tombstone rejects restoration reads"
    );
    assert!(
        store.read().unwrap().rows.is_empty(),
        "acknowledged conditional clear erases all protected records"
    );
    let after = closures().await;
    assert_eq!(
        after["disposition_calls"].as_i64().unwrap(),
        before["disposition_calls"].as_i64().unwrap() + 1
    );
    assert!(
        !c.logout().await.unwrap(),
        "retirement observation does not acknowledge Logout"
    );
    eprintln!("PASS server-driven joined close; independent explicit foreground disposition; SignedOut and acknowledged conditional namespace clear");
}

/// Read-only checkpoint of real production Redis state and private RPC evidence.
async fn presence(user: i64) -> serde_json::Value {
    let (code, state) = private("/liveness", serde_json::json!({"user_id":user})).await;
    assert_eq!(code, 200, "real presence observation must be available");
    state
}

/// Wait for asynchronous disconnect cleanup without sleeping to simulate a lease.
async fn offline_presence(user: i64) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut poll = tokio::time::interval(Duration::from_millis(50));
        loop {
            poll.tick().await;
            let state = presence(user).await;
            let events = state["events"].as_array().unwrap();
            let registered: Vec<_> = events
                .iter()
                .filter(|e| e["action"] == "register")
                .collect();
            let observed_removal = !registered.is_empty()
                && registered.iter().all(|r| {
                    events.iter().any(|e| {
                        e["action"] == "remove"
                            && ["connection_id", "gateway_id", "generation", "session_id"]
                                .iter()
                                .all(|f| e[*f] == r[*f])
                    })
                });
            if state["online"] == false
                && state["connections"] == 0
                && state["leases"].as_array().unwrap().is_empty()
                && observed_removal
            {
                return state;
            }
        }
    })
    .await
    .expect("exact lease removed after disconnect/revocation")
}

/// Verify automatic 5-second SDK heartbeats against Redis's real 15-second lease.
/// No manual KeepAlive is sent until automatic observations have been proven.
async fn observe_real_liveness(c: &Client, user: i64, session: &str) {
    let initial = presence(user).await;
    assert_eq!(initial["online"], true);
    assert_eq!(initial["connections"], 1);
    let initial_lease = &initial["leases"][0];
    assert_eq!(initial_lease["session_id"], session);
    assert_eq!(
        initial_lease["sequence"], 0,
        "RegisterConnection must precede authenticated readiness"
    );
    let started = tokio::time::Instant::now();
    let current = tokio::time::timeout(Duration::from_secs(25), async {
        let mut poll = tokio::time::interval(Duration::from_millis(100));
        loop {
            poll.tick().await;
            let state = presence(user).await;
            let count = state["events"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|e| e["action"] == "renew")
                .count();
            if started.elapsed() >= Duration::from_secs(16) && count >= 3 {
                return state;
            }
        }
    })
    .await
    .expect("actual libtinkle KeepAlive must arrive every five seconds");
    assert_eq!(
        current["online"], true,
        "online beyond original 15-second deadline"
    );
    assert_eq!(current["connections"], 1);
    let lease = &current["leases"][0];
    for field in ["connection_id", "gateway_id", "generation", "session_id"] {
        assert_eq!(
            lease[field], initial_lease[field],
            "must not reconnect to fake liveness"
        );
    }
    assert!(
        current["observed_ms"].as_i64().unwrap() > initial_lease["expires_ms"].as_i64().unwrap()
    );
    assert_eq!(
        lease["expires_ms"].as_i64().unwrap() - lease["last_pong_ms"].as_i64().unwrap(),
        15_000
    );
    assert!(
        lease["expires_ms"].as_i64().unwrap() > current["observed_ms"].as_i64().unwrap() + 10_000
    );
    let events = current["events"].as_array().unwrap();
    assert_eq!(
        events.iter().filter(|e| e["action"] == "register").count(),
        1
    );
    let renews: Vec<_> = events.iter().filter(|e| e["action"] == "renew").collect();
    for (i, e) in renews.iter().enumerate() {
        assert_eq!(
            e["sequence"].as_u64().unwrap(),
            i as u64 + 1,
            "strictly increasing positive source sequence"
        );
        for field in ["connection_id", "gateway_id", "generation", "session_id"] {
            assert_eq!(e[field], lease[field]);
        }
    }
    for pair in renews.windows(2) {
        let gap = pair[1]["at_ms"].as_i64().unwrap() - pair[0]["at_ms"].as_i64().unwrap();
        assert!(
            (4_000..15_000).contains(&gap),
            "5-second heartbeat must be real, not accelerated or beyond TTL"
        );
    }
    let sequence = lease["sequence"].as_u64().unwrap();
    let duplicate: KeepAliveResponse = call(
        c,
        "/tinkle.v1.ConnectionService/KeepAlive",
        KeepAliveRequest { sequence },
    )
    .await
    .unwrap();
    assert_eq!(duplicate.sequence, sequence, "positive sequence echo");
    let after = presence(user).await;
    assert_eq!(
        after["leases"][0], *lease,
        "duplicate must not extend actual Redis deadline"
    );
    assert_eq!(
        after["events"], current["events"],
        "duplicate must not cause another private renewal"
    );
    for invalid in [0, sequence - 1] {
        let error = call::<_, KeepAliveResponse>(
            c,
            "/tinkle.v1.ConnectionService/KeepAlive",
            KeepAliveRequest { sequence: invalid },
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
    }
    let after = presence(user).await;
    assert_eq!(after["leases"][0], *lease);
    assert_eq!(after["events"], current["events"]);
    let source_sequences: Vec<_> = renews
        .iter()
        .map(|e| e["sequence"].as_u64().unwrap())
        .collect();
    let cadence_ms: Vec<_> = renews
        .windows(2)
        .map(|p| p[1]["at_ms"].as_i64().unwrap() - p[0]["at_ms"].as_i64().unwrap())
        .collect();
    eprintln!("PASS real liveness: source_sequences={source_sequences:?}, cadence_ms={cadence_ms:?}, online beyond original TTL, Redis lease=15000ms, duplicate/backwards/zero do not renew");
}

/// The real courier accepts the queue job before returning a captured code.
async fn details(c: &Client) -> SignInDetails {
    let response = c
        .auth()
        .send_code(SendCodeRequest {
            phone_number: "+14155550100".into(),
            lang_code: "en".into(),
        })
        .await
        .unwrap();
    let (code, capture) = private(
        "/latest",
        serde_json::json!({"challenge_id":response.verification_id}),
    )
    .await;
    assert_eq!(
        code, 200,
        "private courier must have accepted the durable job"
    );
    SignInDetails::new(
        "+14155550100".into(),
        capture["code"].as_str().unwrap().into(),
        response.verification_id,
    )
    .unwrap()
}

/// Staging, durable acknowledgement, fresh Returning, session revoke and logout
/// cross actual Go authority/Vault/PG boundaries. All assertions are opt-in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Chorus Bazel native_acceptance fixture; run its run.py"]
async fn native_chorus_enrollment_and_revocation() {
    tokio::time::timeout(Duration::from_secs(90), acceptance_flow())
        .await
        .expect("bounded real-service acceptance");
}

/// A single owned flow prevents test-order or shared-account assumptions.
async fn acceptance_flow() {
    for field in ["pin", "scope", "key_id"] {
        let mut bad = trust();
        match field {
            "pin" => bad.server_public_key[0] ^= 1,
            "scope" => bad.scope.push_str(".wrong"),
            "key_id" => bad.server_key_id += 1,
            _ => unreachable!(),
        }
        let rejected = client_with_trust("native-negative", bad);
        let error = rejected.connect().await.unwrap_err();
        assert!(matches!(
            error,
            TinkleError::TrustRejected | TinkleError::EstablishmentFailed
        ));
        rejected.close().await;
    }
    let first = client("native-acceptance-one");
    first.connect().await.unwrap();
    let store = DiskStore::new("one.json", true);
    let operation = first.prepare_enrollment(store.clone()).await.unwrap();
    let input = details(&first).await;
    // An authorization failure terminates that transport. Keep this probe
    // separate from the Bootstrap connection used after staged delivery.
    let anonymous = client("native-acceptance-anonymous");
    anonymous.connect().await.unwrap();
    assert_eq!(
        sessions(&anonymous).await.unwrap_err().code(),
        ErrorCode::Unauthenticated
    );
    anonymous.close().await;
    let signing = {
        let first = first.clone();
        tokio::spawn(async move { first.auth().sign_in(input, &operation).await })
    };
    tokio::time::timeout(Duration::from_secs(5), store.entered.notified())
        .await
        .unwrap();
    assert!(
        !signing.is_finished(),
        "SignIn must await durable candidate acknowledgement"
    );
    let (code, state) = private("/state", serde_json::json!({})).await;
    assert_eq!(code, 200);
    assert_eq!(state["staged"], 1);
    assert_eq!(state["active"], 0);
    assert_eq!(
        state["staged_admission_denied"], true,
        "GetAdmission is ACTIVE only"
    );
    assert!(store.load_active().await.unwrap().is_none());
    store.release.notify_one();
    let signed = signing.await.unwrap().unwrap();
    assert!(signed.is_new_user);
    assert_eq!(
        signed.enrollment.status().state,
        DeviceEnrollmentState::Staged as i32
    );
    assert_eq!(
        sessions(&first).await.unwrap_err().code(),
        ErrorCode::Unauthenticated,
        "delivery cannot elevate Bootstrap"
    );
    let active = first
        .auth()
        .activate_enrollment(&signed.enrollment)
        .await
        .unwrap();
    assert_eq!(active.state, DeviceEnrollmentState::Active as i32);
    assert_eq!(
        *store.events.lock().unwrap(),
        vec![
            SecureWrite::Operation,
            SecureWrite::Candidate,
            SecureWrite::Active
        ]
    );
    let list = sessions(&first).await.unwrap();
    assert_eq!(list.sessions.len(), 1);
    let s = &list.sessions[0];
    assert!(s.current);
    assert_eq!(s.id, active.session_id);
    assert_eq!(s.device_model, "native-acceptance-one");
    assert_eq!(s.platform, "ios");
    assert_eq!(s.system_version, "18.2");
    assert_eq!(s.app_version, "6.0");
    assert_eq!(s.app_build, "00142");
    eprintln!("PASS candidate-ack gate, Bootstrap isolation, capability activation, Returning, InitConnect labels");
    observe_real_liveness(&first, active.user_id, &active.session_id).await;
    // Restore through a new client and a freshly constructed disk adapter.
    first.close().await;
    let removed = offline_presence(active.user_id).await;
    let events = removed["events"].as_array().unwrap();
    let registered = events.iter().find(|e| e["action"] == "register").unwrap();
    let removed = events.iter().find(|e| e["action"] == "remove").unwrap();
    for field in ["connection_id", "gateway_id", "generation", "session_id"] {
        assert_eq!(
            registered[field], removed[field],
            "teardown must remove exact registered instance"
        );
    }
    let first = client("native-acceptance-one");
    let store1 = DiskStore::new("one.json", false);
    first
        .restore_active_enrollment(store1.clone())
        .await
        .unwrap();
    assert_eq!(sessions(&first).await.unwrap().sessions.len(), 1);
    let second = client("native-acceptance-two");
    second.connect().await.unwrap();
    let store2 = DiskStore::new("two.json", false);
    let operation = second.prepare_enrollment(store2.clone()).await.unwrap();
    let signed = second
        .auth()
        .sign_in(details(&second).await, &operation)
        .await
        .unwrap();
    assert!(
        !signed.is_new_user,
        "same phone must resolve original production user"
    );
    let second_active = second
        .auth()
        .activate_enrollment(&signed.enrollment)
        .await
        .unwrap();
    assert_eq!(active.user_id, second_active.user_id);
    assert_ne!(active.session_id, second_active.session_id);
    let list = sessions(&first).await.unwrap();
    assert_eq!(list.sessions.len(), 2);
    assert_eq!(list.sessions.iter().filter(|s| s.current).count(), 1);
    let retained2 = store2.retained("old-two.json");
    assert!(first
        .account()
        .revoke_session(&second_active.session_id)
        .await
        .unwrap());
    let joined = closures().await;
    observe_kicked(
        &second,
        &store2,
        active.user_id,
        &second_active.session_id,
        &joined,
        1,
    )
    .await;
    reject_old_returning(retained2).await;
    assert_eq!(sessions(&first).await.unwrap().sessions.len(), 1);

    let (third, store3, third_active) = enroll("native-acceptance-three", "three.json").await;
    assert_eq!(third_active.user_id, active.user_id);
    let retained3 = store3.retained("old-three.json");
    assert!(first.account().revoke_other_sessions().await.unwrap());
    let joined = closures().await;
    observe_kicked(
        &third,
        &store3,
        active.user_id,
        &third_active.session_id,
        &joined,
        // Bulk revocation also supersedes the already-retired second key's
        // authority revision and requires a fresh (zero-socket) joined receipt.
        2,
    )
    .await;
    reject_old_returning(retained3).await;
    let remaining = first.account().get_sessions().await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].current);
    assert_eq!(remaining[0].id, active.session_id);
    assert!(store1.load_active().await.unwrap().is_some());
    eprintln!("PASS session facades: get_sessions, revoke_session, revoke_other_sessions; caller remains authenticated with its namespace intact");

    let (wrapped, store4, _) = enroll("native-acceptance-wrapped", "four.json").await;
    let retained4 = store4.retained("old-four.json");
    // Fresh Returning transport, whose first public request is SDK-owned
    // InitConnect(Logout). No explicit InitConnect escape hatch is used.
    wrapped.reconnect().await.unwrap();
    let wrapped_started = tokio::time::Instant::now();
    let logged: LogoutResponse = call(
        &wrapped,
        "/tinkle.v1.AccountService/Logout",
        LogoutRequest {},
    )
    .await
    .expect("healthy first-wrapped Logout must deliver a correlated response");
    assert!(logged.logged_out);
    let wrapped_elapsed = wrapped_started.elapsed();
    assert!(
        wrapped_elapsed < Duration::from_secs(3),
        "healthy wrapped Logout must finish inside the original 3s budget"
    );
    assert_eq!(closures().await["completed"], 1);
    reject_old_returning(retained4).await;
    // Offline namespace reset remains a separate regression: raw remote Logout
    // above did not clear the SDK host namespace. Closed transport cannot ack it.
    wrapped.close().await;
    assert!(store4.load_active().await.unwrap().is_some());
    assert!(!wrapped.logout().await.unwrap());
    assert!(wrapped.is_signed_out());
    let cleared = store4.namespace_snapshot().await.unwrap().unwrap();
    assert!(cleared.cleanup_pending && cleared.active.is_none() && !cleared.has_pending);
    assert!(store4.read().unwrap().rows.is_empty());
    assert!(
        store1.load_active().await.unwrap().is_some(),
        "offline clear preserves another active namespace"
    );
    assert!(matches!(
        wrapped.connect().await,
        Err(TinkleError::SignedOut)
    ));
    eprintln!("PASS actual offline Client.logout: irreversible local fence, durable namespace clear, other namespace preserved");

    let retained1 = store1.retained("old-one.json");
    let direct_started = tokio::time::Instant::now();
    assert!(
        first
            .logout()
            .await
            .expect("healthy Client.logout must acknowledge durable local clearing"),
        "healthy direct Client.logout must return Ok(true), never an unknown outcome"
    );
    let direct_elapsed = direct_started.elapsed();
    assert!(
        direct_elapsed < Duration::from_secs(3),
        "healthy direct Client.logout must finish inside 3s, including local acknowledgement"
    );
    assert!(first.is_signed_out());
    let status = first.session_status().await;
    assert_eq!(status.authorization, SessionAuthorization::SignedOut);
    assert_eq!(status.cleanup, SessionCleanup::Complete);
    assert!(status.restart_safe_store);
    let cleared = DiskStore::new("one.json", false)
        .namespace_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert!(cleared.cleanup_pending && cleared.active.is_none() && !cleared.has_pending);
    assert!(store1.read().unwrap().rows.is_empty());
    assert_eq!(closures().await["completed"], 1);
    reject_old_returning(retained1).await;
    offline_presence(active.user_id).await;
    eprintln!("PASS strict healthy Logout: direct Client.logout=Ok(true), first-wrapped correlated LogoutResponse, conditional store clear, fresh old Returning denied");
    eprintln!(
        "PASS healthy Logout <3s: direct_ms={}, first_wrapped_ms={}",
        direct_elapsed.as_millis(),
        wrapped_elapsed.as_millis()
    );
}

/// The fixture adapter must satisfy clear's stale-write fence and namespace scope.
#[tokio::test]
#[ignore = "owned acceptance host secure-store contract"]
async fn native_chorus_store_clear_namespace() {
    let one = DiskStore::new("clear-one.json", true);
    let other = DiskStore::new("clear-other.json", false);
    let blob = SecretBlob::new(vec![11, 22, 33]);
    let initial = one.namespace_snapshot().await.unwrap().unwrap().revision;
    let revision = one
        .commit_current(&initial, SecureWrite::Operation, "operation", &blob)
        .await
        .unwrap()
        .unwrap();
    let initial_other = other.namespace_snapshot().await.unwrap().unwrap().revision;
    other
        .commit_current(&initial_other, SecureWrite::Operation, "operation", &blob)
        .await
        .unwrap();
    let queued = {
        let one = one.clone();
        let revision = revision.clone();
        tokio::spawn(async move {
            one.commit_current(
                &revision,
                SecureWrite::Candidate,
                "operation",
                &SecretBlob::new(vec![44]),
            )
            .await
        })
    };
    one.entered.notified().await;
    assert!(one.begin_clear(&revision).await.unwrap());
    assert!(one.finish_clear(&revision).await.unwrap());
    one.release.notify_one();
    assert!(
        queued.await.unwrap().unwrap().is_none(),
        "queued candidate cannot restore a cleared namespace"
    );
    assert!(one.begin_clear(&revision).await.unwrap());
    assert!(one.finish_clear(&revision).await.unwrap());
    assert!(one.load_operation("operation").await.is_err());
    assert!(one.load_active().await.is_err());
    assert_eq!(
        other
            .load_operation("operation")
            .await
            .unwrap()
            .unwrap()
            .expose_secret(),
        [11, 22, 33]
    );
    assert!(one
        .commit_current(
            &revision,
            SecureWrite::Operation,
            "new",
            &SecretBlob::new(vec![55])
        )
        .await
        .unwrap()
        .is_none());
    one.reset_namespace().unwrap();
    let reset = one.namespace_snapshot().await.unwrap().unwrap();
    assert_ne!(reset.revision, revision);
    assert!(!one.begin_clear(&revision).await.unwrap());
    assert!(!one.finish_clear(&revision).await.unwrap());
    assert!(one
        .commit_current(&revision, SecureWrite::Operation, "stale", &blob)
        .await
        .unwrap()
        .is_none());
    assert!(one
        .commit_current(&reset.revision, SecureWrite::Operation, "successor", &blob)
        .await
        .unwrap()
        .is_some());
    assert!(!one.begin_clear(&revision).await.unwrap());
    assert!(one.load_operation("successor").await.unwrap().is_some());
    eprintln!("PASS offline namespace reset: durable tombstone, stale queued write rejected, successor incarnation immune to old writes/clear");

    // Exercise the new host contract using the same disk adapter and a freshly
    // reopened peer. Full-record comparison and exclusion are one transaction.
    let before = other.namespace_snapshot().await.unwrap().unwrap();
    other
        .commit_current(&before.revision, SecureWrite::Active, "operation", &blob)
        .await
        .unwrap()
        .unwrap();
    let active = other.namespace_snapshot().await.unwrap().unwrap();
    for (incarnation, revision, record) in [
        ("wrong-incarnation", active.revision.as_str(), &blob),
        (active.incarnation.as_str(), "wrong-revision", &blob),
        (
            active.incarnation.as_str(),
            active.revision.as_str(),
            &SecretBlob::new(vec![99]),
        ),
    ] {
        assert!(!other
            .begin_restore(incarnation, revision, record, "mismatch")
            .await
            .unwrap());
    }
    assert!(other
        .begin_restore(&active.incarnation, &active.revision, &blob, "install")
        .await
        .unwrap());
    let peer = DiskStore::new("clear-other.json", false);
    assert!(peer
        .commit_current(&active.revision, SecureWrite::Operation, "blocked", &blob)
        .await
        .is_err());
    assert!(peer.begin_clear(&active.revision).await.is_err());
    assert!(peer.finish_clear(&active.revision).await.is_err());
    assert!(peer.reset_namespace().is_err());
    peer.end_restore("unrelated").await.unwrap();
    assert!(
        peer.begin_clear(&active.revision).await.is_err(),
        "unrelated permit cannot release exclusion"
    );
    peer.end_restore("install").await.unwrap();
    peer.end_restore("install").await.unwrap();
    assert!(peer
        .commit_current(
            &active.revision,
            SecureWrite::Operation,
            "after-release",
            &blob
        )
        .await
        .unwrap()
        .is_some());
    let released = peer.namespace_snapshot().await.unwrap().unwrap();
    peer.end_restore("cancelled").await.unwrap();
    assert!(
        !other
            .begin_restore(
                &released.incarnation,
                &released.revision,
                &blob,
                "cancelled"
            )
            .await
            .unwrap(),
        "end_restore dominates a late begin_restore across adapters"
    );
    assert!(other
        .begin_restore(
            &released.incarnation,
            &released.revision,
            &blob,
            "next-install"
        )
        .await
        .unwrap());
    other.end_restore("next-install").await.unwrap();
    eprintln!("PASS conditional restore permits: exact incarnation/revision/ACTIVE match; cross-adapter write/clear/reset exclusion; matching idempotent release; cancellation fences late acquisition");
}
