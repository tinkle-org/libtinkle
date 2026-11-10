//! Actual libtinkle client against externally owned Chorus production handlers.
//! No Rust transport/server stand-in and no direct authority key injection.
#![cfg(all(unix, not(target_arch = "wasm32")))]
use async_trait::async_trait;
use libtinkle::{
    Client, ClientConfig, ClientInfo, EnrollmentNamespaceSnapshot, EnrollmentObservation,
    InMemoryCursorStore, SecretBlob, SecureEnrollmentStore, SecureStoreError, SecureWrite,
    ServerTrust, SessionAuthorization, SessionCleanup, SessionSignOutReason, SessionState,
    SessionStateSubscription, SignInDetails, TinkleError, TrustSource,
};
use prost::Message;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
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
    /// Fault only the acknowledgement step, retaining the durable tombstone.
    fail_finish: AtomicBool,
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
    /// Erase retained credential bytes when a decoded namespace snapshot is released.
    fn drop(&mut self) {
        for value in self.rows.values_mut() {
            zeroize::Zeroize::zeroize(value);
        }
    }
}

impl DiskStore {
    /// Open a named acceptance namespace with shared restore permits and an optional write gate.
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
            fail_finish: AtomicBool::new(false),
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
    /// Decode the durable snapshot, creating its initial incarnation when the file is absent.
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
    /// Copy one opaque record under install exclusion, rejecting cleanup-marked namespaces.
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
    /// Reject mutation because this retained namespace exists only for admission probes.
    async fn clear(&self) -> Result<(), SecureStoreError> {
        panic!("read-only admission probe")
    }
    /// Reject writes to the read-only pre-retirement credential snapshot.
    async fn commit(
        &self,
        _: SecureWrite,
        _: &str,
        _: &SecretBlob,
    ) -> Result<(), SecureStoreError> {
        panic!("read-only admission probe")
    }
    /// Read the retained operation without enabling status-first namespace observation.
    async fn load_operation(&self, id: &str) -> Result<Option<SecretBlob>, SecureStoreError> {
        self.0.load(id)
    }
    /// Read old active material so the test can prove actual server-side Returning rejection.
    async fn load_active(&self) -> Result<Option<SecretBlob>, SecureStoreError> {
        self.0.load("active")
    }
}

#[async_trait]
impl SecureEnrollmentStore for DiskStore {
    /// Pin exact active material at its current namespace token until permit release.
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
    /// Release matching install exclusion and prevent late acquisition of the cancelled ID.
    async fn end_restore(&self, permit_id: &str) -> Result<(), SecureStoreError> {
        let mut permits = self.lock.lock().unwrap();
        permits.cancelled.insert(permit_id.into());
        permits.held.remove(permit_id);
        Ok(())
    }
    /// Read active material, pending work, and cleanup state from one locked durable snapshot.
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
    /// Durably mark cleanup only if the caller still owns the current namespace token.
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
    /// Erase all records under the matching tombstone and durably retain that marker.
    async fn finish_clear(&self, expected: &str) -> Result<bool, SecureStoreError> {
        if self.fail_finish.load(Ordering::SeqCst) {
            return Err(SecureStoreError);
        }
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
    /// Fail acceptance if cleanup bypasses the conditional host contract.
    async fn clear(&self) -> Result<(), SecureStoreError> {
        panic!("conditional host must use begin_clear/finish_clear")
    }
    /// Fail acceptance if a write bypasses namespace revision comparison.
    async fn commit(
        &self,
        _: SecureWrite,
        _: &str,
        _: &SecretBlob,
    ) -> Result<(), SecureStoreError> {
        panic!("conditional host must use commit_current")
    }
    /// Publish only at the expected unmarked revision, acknowledging after file durability.
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
    /// Load the named operation through the namespace's cleanup and install fences.
    async fn load_operation(&self, id: &str) -> Result<Option<SecretBlob>, SecureStoreError> {
        self.load(id)
    }
    /// Load the active slot through the namespace's cleanup and install fences.
    async fn load_active(&self) -> Result<Option<SecretBlob>, SecureStoreError> {
        self.load("active")
    }
}

/// Require an acceptance-runner input, reporting only its name when absent.
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

/// Create a Bootstrap client using the independently provisioned acceptance server pin.
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
    private_response(&raw).expect("valid private capture HTTP/JSON response")
}

/// The private Go HTTP server may chunk responses; framing is not JSON data.
fn private_response(raw: &[u8]) -> anyhow::Result<(u16, serde_json::Value)> {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("missing HTTP headers"))?;
    let headers = std::str::from_utf8(&raw[..split])?;
    let code = headers
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("missing HTTP status"))?
        .parse()?;
    let encodings: Vec<_> = headers
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("transfer-encoding"))
        .map(|(_, value)| value.trim())
        .collect();
    let mut body = &raw[split + 4..];
    let mut decoded = Zeroizing::new(Vec::new());
    if !encodings.is_empty() {
        anyhow::ensure!(
            encodings.len() == 1 && encodings[0].eq_ignore_ascii_case("chunked"),
            "unsupported private HTTP transfer encoding"
        );
        loop {
            let end = body
                .windows(2)
                .position(|w| w == b"\r\n")
                .ok_or_else(|| anyhow::anyhow!("truncated chunk size"))?;
            let size = std::str::from_utf8(&body[..end])?
                .split(';')
                .next()
                .unwrap_or("");
            anyhow::ensure!(
                !size.is_empty() && size.bytes().all(|b| b.is_ascii_hexdigit()),
                "invalid chunk size"
            );
            let size = usize::from_str_radix(size, 16)?;
            body = &body[end + 2..];
            if size == 0 {
                // Consume optional trailers through their terminating empty line.
                while !body.starts_with(b"\r\n") {
                    let end = body
                        .windows(2)
                        .position(|w| w == b"\r\n")
                        .ok_or_else(|| anyhow::anyhow!("truncated chunk trailer"))?;
                    anyhow::ensure!(body[..end].contains(&b':'), "invalid chunk trailer");
                    body = &body[end + 2..];
                }
                anyhow::ensure!(body == b"\r\n", "extra bytes after chunked body");
                break;
            }
            decoded.extend_from_slice(
                body.get(..size)
                    .ok_or_else(|| anyhow::anyhow!("truncated chunk data"))?,
            );
            body = body
                .get(size..)
                .and_then(|rest| rest.strip_prefix(b"\r\n"))
                .ok_or_else(|| anyhow::anyhow!("missing chunk terminator"))?;
        }
        body = &decoded;
    }
    let value = if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(body)?
    };
    Ok((code, value))
}

#[test]
/// Decode plain and chunked capture responses equivalently and reject malformed chunk framing.
fn private_http_json_framing() {
    let json = br#"{"online":true,"completed":1}"#;
    let plain = [
        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", json.len()).as_bytes(),
        json,
    ]
    .concat();
    let expected = private_response(&plain).unwrap();
    let mut chunked = b"HTTP/1.1 200 OK\r\ntRaNsFeR-EnCoDiNg: Chunked\r\n\r\n".to_vec();
    for chunk in json.chunks(7) {
        chunked.extend_from_slice(format!("{:X};fixture=yes\r\n", chunk.len()).as_bytes());
        chunked.extend_from_slice(chunk);
        chunked.extend_from_slice(b"\r\n");
    }
    chunked.extend_from_slice(b"0\r\nX-Fixture: done\r\n\r\n");
    assert_eq!(private_response(&chunked).unwrap(), expected);
    assert_eq!(expected.1["online"], true);
    assert_eq!(expected.1["completed"], 1);
    for invalid in [
        b"1\r\nx".as_slice(),
        b"0\r\n",
        b"z\r\n",
        b"+1\r\nx\r\n0\r\n\r\n",
        b"1\r\nx!\r\n0\r\n\r\n",
    ] {
        let raw = [
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".as_slice(),
            invalid,
        ]
        .concat();
        assert!(private_response(&raw).is_err());
    }
}

/// Issue one typed acceptance RPC with a three-second budget and decode its response.
async fn call<Req: Message, Resp: Message + Default>(
    c: &Client,
    method: &str,
    req: Req,
) -> Result<Resp, TinkleError> {
    let bytes = c.call_raw(method, req.encode_to_vec(), 3000).await?;
    Ok(Resp::decode(bytes.as_slice()).unwrap())
}

/// Wrap the public account-session projection for acceptance assertions.
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
    // Installing a local ACTIVE record does not prove server admission.
    let active = rejected.restore_active_enrollment(store).await.unwrap();
    assert_eq!(active.state, DeviceEnrollmentState::Active as i32);
    assert!(
        !rejected.is_connected().await,
        "old Returning must fail before ready"
    );
    assert_eq!(
        rejected.session_status().await.authorization,
        SessionAuthorization::Unknown
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

/// Automatic foreground loss checks and explicit resume may race or coalesce.
/// Neither socket loss nor UNKNOWN permits cleanup: require retained exact
/// retirement proof and a separately acknowledged conditional namespace clear.
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
    // Registration includes a completed automatic observation when it wins.
    // A cancelled loss check may already have reached authn before explicit
    // foreground entry starts another; global call counts are not a proof gate.
    let states = c.subscribe_session_state().await.unwrap();
    c.suspend().await;
    c.resume().await;
    let state = session_state_until(&states, "verified retirement and durable clear", |state| {
        let status = &state.status;
        if status.authorization == SessionAuthorization::SignedOut {
            assert_eq!(
                status.enrollment_observation,
                EnrollmentObservation::Retired,
                "loss or UNKNOWN cannot substitute for exact retirement evidence"
            );
            assert_eq!(state.reason, Some(SessionSignOutReason::SessionRetired));
            assert!(
                state.sessions.is_none(),
                "retirement withdraws the account snapshot"
            );
        } else {
            assert_eq!(state.reason, None);
            assert_eq!(
                status.cleanup,
                SessionCleanup::NotStarted,
                "unfenced authorization cannot clear credentials"
            );
        }
        assert_ne!(
            status.cleanup,
            SessionCleanup::Failed,
            "durable clear failed"
        );
        status.authorization == SessionAuthorization::SignedOut
            && status.cleanup == SessionCleanup::Complete
    })
    .await;
    states.stop();
    let status = state.status;
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
    assert_eq!(after["completed"], 0);
    assert_eq!(
        after["retired_sessions"][session], true,
        "real private authority must have echoed this exact retired reference"
    );
    assert!(
        !c.logout().await.unwrap(),
        "retirement observation does not acknowledge Logout"
    );
    eprintln!("PASS server-driven joined close; retained verified automatic/foreground retirement; SignedOut and acknowledged conditional namespace clear");
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
    details_for(c, "+14155550100").await
}

/// Obtain phone proof from the real encrypted courier for an isolated account.
async fn details_for(c: &Client, phone: &str) -> SignInDetails {
    let response = c
        .auth()
        .send_code(SendCodeRequest {
            phone_number: phone.into(),
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
        phone.into(),
        capture["code"].as_str().unwrap().into(),
        response.verification_id,
    )
    .unwrap()
}

/// Staging, durable acknowledgement, fresh Returning, session revoke and logout
/// cross actual Go authority/Vault/PG boundaries. All assertions are opt-in.
// One executor orders the fresh-connection first-call qualification below;
// the independent review/state flow retains its four-worker runtime.
#[tokio::test(flavor = "current_thread")]
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
    // Demand a fresh Bootstrap transport for this negative authorization probe:
    // an already-closed sign-in socket proves neither admission nor denial.
    first.reconnect().await.unwrap();
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
    settle_initial_review(&second, &second_active.session_id, 2).await;
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
    settle_initial_review(&third, &third_active.session_id, 2).await;
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
    // Keep reconnect and immediate dispatch in one current-thread poll, without
    // Tokio's cooperative-budget yield handing initialization to GetSessions.
    // Real I/O still yields, all request deadlines remain enabled, and the Go
    // authority must verify one actual InitConnect-owned terminal transition.
    // No explicit InitConnect escape hatch or disabled review worker is used.
    let wrapped_elapsed = tokio::task::coop::unconstrained(async {
        wrapped.reconnect().await.unwrap();
        let started = tokio::time::Instant::now();
        let logged: LogoutResponse = call(
            &wrapped,
            "/tinkle.v1.AccountService/Logout",
            LogoutRequest {},
        )
        .await
        .expect("healthy first-wrapped Logout must deliver a correlated response");
        assert!(logged.logged_out);
        started.elapsed()
    })
    .await;
    assert!(
        wrapped_elapsed < Duration::from_secs(3),
        "healthy wrapped Logout must finish inside the original 3s budget"
    );
    assert_eq!(closures().await["completed"], 1);
    reject_old_returning(retained4).await;
    // The raw Logout can independently trigger verified retirement cleanup.
    // Join it before testing offline local clearing on a separate ACTIVE device.
    let wrapped_state = wrapped.subscribe_session_state().await.unwrap();
    wrapped.suspend().await;
    wrapped.resume().await;
    session_state_until(&wrapped_state, "raw Logout retirement cleanup", |state| {
        state.reason == Some(SessionSignOutReason::SessionRetired)
            && state.status.cleanup == SessionCleanup::Complete
    })
    .await;
    wrapped_state.stop();
    wrapped.close().await;
    assert!(store4.load_active().await.is_err());
    assert!(store4.read().unwrap().rows.is_empty());

    let (offline, offline_store, offline_active) =
        enroll("native-acceptance-offline", "five.json").await;
    let retained_offline = offline_store.retained("old-five.json");
    settle_initial_review(&offline, &offline_active.session_id, 2).await;
    offline.close().await;
    assert!(offline_store.load_active().await.unwrap().is_some());
    assert!(!offline.logout().await.unwrap());
    assert!(offline.is_signed_out());
    let cleared = offline_store.namespace_snapshot().await.unwrap().unwrap();
    assert!(cleared.cleanup_pending && cleared.active.is_none() && !cleared.has_pending);
    assert!(offline_store.read().unwrap().rows.is_empty());
    assert!(
        store1.load_active().await.unwrap().is_some(),
        "offline clear preserves another active namespace"
    );
    assert!(matches!(
        offline.connect().await,
        Err(TinkleError::SignedOut)
    ));
    eprintln!("PASS actual offline Client.logout: irreversible local fence, durable namespace clear, other namespace preserved");
    assert!(first
        .account()
        .revoke_session(&offline_active.session_id)
        .await
        .unwrap());
    assert_eq!(closures().await["completed"], 1);
    reject_old_returning(retained_offline).await;

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

/// Finish the new startup snapshot before a closure test selects its live target.
/// Otherwise that in-flight authorized read may legitimately close the revoked
/// socket before worker selection, obscuring this lane's exact joined-count proof.
async fn settle_initial_review(c: &Client, current: &str, count: usize) {
    let stream = c.subscribe_session_state().await.unwrap();
    session_state_until(&stream, "startup read completes before revocation", |s| {
        s.sessions.as_ref().is_some_and(|rows| {
            rows.len() == count && rows.iter().any(|row| row.id == current && row.current)
        })
    })
    .await;
    stream.stop();
}

/// Read retained state only: no explicit GetSessions may make an automatic
/// refresh assertion pass. Revisions may coalesce but cannot run backwards.
async fn session_state_until(
    stream: &SessionStateSubscription,
    description: &str,
    accept: impl Fn(&SessionState) -> bool,
) -> SessionState {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut revision = None;
        loop {
            let state = stream
                .next()
                .await
                .expect("session-state owner remains live");
            if let Some(previous) = revision {
                assert!(state.revision > previous, "state revisions must advance");
            }
            revision = Some(state.revision);
            if accept(&state) {
                break state;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("bounded session-state wait: {description}"))
}

/// Require an exact full session set with one current device, not a hint-derived row.
fn review_snapshot(state: &SessionState, current: &str, expected: &[(&str, bool)]) -> bool {
    let Some(rows) = state.sessions.as_ref() else {
        return false;
    };
    rows.len() == expected.len()
        && rows.iter().filter(|s| s.current).count() == 1
        && rows.iter().all(|s| s.current == (s.id == current))
        && expected.iter().all(|(id, unconfirmed)| {
            rows.iter()
                .any(|s| s.id == *id && s.unconfirmed == *unconfirmed)
        })
}

/// Explicitly drain the production durable worker, never synthesize a hint.
async fn session_hints() -> serde_json::Value {
    let (code, result) = private("/session-hints", serde_json::json!({})).await;
    assert_eq!(code, 200, "real durable mTLS hint worker must run");
    result
}

/// Bootstrap an isolated real account, without changing authority or admission.
async fn review_enroll(
    model: &str,
    name: &str,
    phone: &str,
    new_user: bool,
) -> (Arc<Client>, Arc<DiskStore>, DeviceEnrollmentStatus) {
    // Refill the unchanged gateway source budget between enrollment bursts.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let c = client(model);
    c.connect().await.unwrap();
    let store = DiskStore::new(name, false);
    let operation = c.prepare_enrollment(store.clone()).await.unwrap();
    let signed = c
        .auth()
        .sign_in(details_for(&c, phone).await, &operation)
        .await
        .unwrap();
    assert_eq!(signed.is_new_user, new_user);
    let active = c
        .auth()
        .activate_enrollment(&signed.enrollment)
        .await
        .unwrap();
    (c, store, active)
}

/// Exercise the published wire error, not merely a facade's local validation.
async fn confirm_error(c: &Client, target: &str, expected: ErrorCode) {
    let error = call::<_, ConfirmSessionResponse>(
        c,
        "/tinkle.v1.AccountService/ConfirmSession",
        ConfirmSessionRequest {
            session_id: target.into(),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), expected);
}

/// Wait for server-owned teardown without closing the client to make it pass.
async fn server_disconnected(c: &Client) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        while c.is_connected().await {
            poll.tick().await;
        }
    })
    .await
    .expect("production worker closes the target connection");
}

/// With a pending candidate, socket loss cannot authorize namespace cleanup.
async fn disconnected_without_retirement(c: &Client, store: &DiskStore) {
    server_disconnected(c).await;
    assert!(
        !c.is_signed_out(),
        "connection loss must remain unknown, not retired"
    );
    assert_eq!(
        c.session_status().await.authorization,
        SessionAuthorization::Unknown
    );
    assert!(store.load_active().await.unwrap().is_some());
}

/// Real public review, durable postcommit mTLS hints, and retained application
/// state. The Go owner supplies fresh PG/Vault/Redis and tears down on failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Chorus Bazel native_acceptance fixture; run its run.py"]
async fn native_chorus_session_review() {
    tokio::time::timeout(Duration::from_secs(120), session_review_flow())
        .await
        .expect("bounded real-service session review acceptance");
}

/// One flow owns every device and account so assertions never depend on order.
async fn session_review_flow() {
    let phone = "+14155550100";
    let (first, store1, active1) =
        review_enroll("review-first", "review-one.json", phone, true).await;
    let initial = sessions(&first).await.unwrap().sessions;
    assert_eq!(initial.len(), 1);
    assert!(initial[0].current && !initial[0].unconfirmed);
    // A newly constructed coordinator must fetch its authoritative snapshot on
    // restore, without waiting for a notification from an older process.
    first.close().await;
    let first = client("review-first-restored");
    first
        .restore_active_enrollment(store1.clone())
        .await
        .unwrap();
    // Foreground reconnect is a supervised lifecycle contract. Empty interests
    // install that owner without requiring unrelated conversation services.
    first.start(Vec::new()).await.unwrap();
    let states1 = first.subscribe_session_state().await.unwrap();
    session_state_until(&states1, "startup snapshot", |s| {
        review_snapshot(s, &active1.session_id, &[(&active1.session_id, false)])
    })
    .await;
    session_hints().await;

    let (second, store2, active2) =
        review_enroll("review-second", "review-two.json", phone, false).await;
    let states2 = second.subscribe_session_state().await.unwrap();
    session_state_until(&states2, "second device initially unconfirmed", |s| {
        review_snapshot(
            s,
            &active2.session_id,
            &[(&active1.session_id, false), (&active2.session_id, true)],
        )
    })
    .await;
    session_hints().await;
    session_state_until(
        &states1,
        "activation hint automatically refreshes first",
        |s| {
            review_snapshot(
                s,
                &active1.session_id,
                &[(&active1.session_id, false), (&active2.session_id, true)],
            )
        },
    )
    .await;
    confirm_error(&second, &active1.session_id, ErrorCode::FailedPrecondition).await;
    confirm_error(&second, &active2.session_id, ErrorCode::FailedPrecondition).await;
    confirm_error(&first, &active1.session_id, ErrorCode::FailedPrecondition).await;
    for malformed in ["", "not-a-session", "00000000-0000-0000-0000-000000000000"] {
        confirm_error(&first, malformed, ErrorCode::InvalidArgument).await;
    }
    confirm_error(
        &first,
        "d251c299-001f-4482-84de-b20d3f7e014c",
        ErrorCode::NotFound,
    )
    .await;

    let (foreign, foreign_store, foreign_active) = review_enroll(
        "review-foreign",
        "review-foreign.json",
        "+14155550101",
        true,
    )
    .await;
    assert_ne!(foreign_active.user_id, active1.user_id);
    let foreign_states = foreign.subscribe_session_state().await.unwrap();
    session_state_until(&foreign_states, "isolated foreign snapshot", |s| {
        review_snapshot(
            s,
            &foreign_active.session_id,
            &[(&foreign_active.session_id, false)],
        )
    })
    .await;
    confirm_error(&first, &foreign_active.session_id, ErrorCode::NotFound).await;
    confirm_error(&foreign, &active2.session_id, ErrorCode::NotFound).await;
    session_hints().await;

    let before = presence(active1.user_id).await;
    let first_lease = before["leases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l["session_id"] == active1.session_id)
        .unwrap()
        .clone();
    let confirmed = first
        .account()
        .confirm_session(&active2.session_id)
        .await
        .unwrap();
    assert_eq!(confirmed.id, active2.session_id);
    assert!(!confirmed.current && !confirmed.unconfirmed);
    let repeated = first
        .account()
        .confirm_session(&active2.session_id)
        .await
        .unwrap();
    assert_eq!(repeated.id, confirmed.id);
    assert!(!repeated.current && !repeated.unconfirmed);
    session_hints().await;
    session_state_until(
        &states2,
        "same-account confirmation hint refreshes second",
        |s| {
            review_snapshot(
                s,
                &active2.session_id,
                &[(&active1.session_id, false), (&active2.session_id, false)],
            )
        },
    )
    .await;
    session_state_until(&states1, "confirmation refreshes first", |s| {
        review_snapshot(
            s,
            &active1.session_id,
            &[(&active1.session_id, false), (&active2.session_id, false)],
        )
    })
    .await;
    let after = presence(active1.user_id).await;
    let first_leases: Vec<_> = after["leases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|l| l["session_id"] == active1.session_id)
        .collect();
    assert_eq!(
        first_leases.len(),
        1,
        "one ordinary Returning connection for review and fetch"
    );
    for field in ["connection_id", "gateway_id", "generation"] {
        assert_eq!(
            first_leases[0][field], first_lease[field],
            "review cannot open a replacement application TCP"
        );
    }
    // GetSessions must remain principal-isolated. Actual foreign notification
    // exclusion is checked on both encrypted sockets in the separate Go lane.
    assert_eq!(sessions(&foreign).await.unwrap().sessions.len(), 1);
    eprintln!("PASS real review: initial confirmed, new device unconfirmed, typed wire denials, foreign isolation, idempotent confirmation, automatic same-TCP snapshot refresh");

    let operation2 = second.prepare_enrollment(store2.clone()).await.unwrap();
    let candidate2 = second.auth().rotate_enrollment(&operation2).await.unwrap();
    let rotated2 = second
        .auth()
        .activate_enrollment(&candidate2)
        .await
        .unwrap();
    assert_eq!(rotated2.session_id, active2.session_id);
    assert!(rotated2.enrollment_epoch > active2.enrollment_epoch);
    let list = sessions(&second).await.unwrap().sessions;
    assert_eq!(list.len(), 2, "rotation cannot duplicate a session");
    assert!(list
        .iter()
        .any(|s| s.id == active2.session_id && s.current && !s.unconfirmed));
    assert_eq!(
        closures().await["completed"],
        1,
        "rotation predecessor closes independently"
    );

    first.suspend().await;
    let (third, store3, active3) =
        review_enroll("review-third", "review-three.json", phone, false).await;
    session_hints().await; // deliberately missed while first is suspended
    first.resume().await;
    session_state_until(
        &states1,
        "foreground recovers a missed activation hint",
        |s| {
            review_snapshot(
                s,
                &active1.session_id,
                &[
                    (&active1.session_id, false),
                    (&active2.session_id, false),
                    (&active3.session_id, true),
                ],
            )
        },
    )
    .await;
    first.reconnect().await.unwrap();
    session_state_until(&states1, "reconnect refetches without another hint", |s| {
        review_snapshot(
            s,
            &active1.session_id,
            &[
                (&active1.session_id, false),
                (&active2.session_id, false),
                (&active3.session_id, true),
            ],
        )
    })
    .await;
    let retained3 = store3.retained("review-old-three.json");
    let operation3 = third.prepare_enrollment(store3.clone()).await.unwrap();
    let candidate3 = third.auth().rotate_enrollment(&operation3).await.unwrap();
    assert!(
        third.is_connected().await,
        "staging preserves the current connection"
    );
    let registered3 = presence(active1.user_id).await["leases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l["session_id"] == active3.session_id)
        .expect("third live lease")
        .clone();
    let reviewing = first.account();
    let rejecting = second.account();
    let (confirmation, revoked) = tokio::join!(
        reviewing.confirm_session(&active3.session_id),
        rejecting.revoke_session(&active3.session_id)
    );
    assert!(revoked.unwrap());
    match confirmation {
        Ok(s) => assert!(!s.unconfirmed && s.id == active3.session_id),
        Err(e) => assert_eq!(e.code(), ErrorCode::NotFound),
    }
    confirm_error(&first, &active3.session_id, ErrorCode::NotFound).await;
    let joined = closures().await;
    assert_eq!(joined["completed"], 1);
    // Production request/idle authorization may close the revoked socket before
    // worker selection. Prove the exact server removal, not which owner won.
    assert!(joined["joined_sockets"].as_u64().unwrap() <= 1);
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        loop {
            poll.tick().await;
            let state = presence(active1.user_id).await;
            if state["events"].as_array().unwrap().iter().any(|e| {
                e["action"] == "remove"
                    && ["session_id", "connection_id", "gateway_id", "generation"]
                        .iter()
                        .all(|f| e[*f] == registered3[*f])
            }) {
                break;
            }
        }
    })
    .await
    .expect("server close removes the exact previously live third connection");
    disconnected_without_retirement(&third, &store3).await;
    assert!(
        third.auth().activate_enrollment(&candidate3).await.is_err(),
        "revocation cancels the staged successor"
    );
    reject_old_returning(retained3).await;
    assert!(!third.logout().await.unwrap());
    session_hints().await;
    session_state_until(
        &states1,
        "rejection removes target but preserves confirmed peers",
        |s| {
            review_snapshot(
                s,
                &active1.session_id,
                &[(&active1.session_id, false), (&active2.session_id, false)],
            )
        },
    )
    .await;
    assert_eq!(sessions(&second).await.unwrap().sessions.len(), 2);
    eprintln!("PASS rotation preserves review; foreground/reconnect recovery; concurrent confirm/revoke cannot revive; target old key and staged successor rejected while two peers survive");

    let (expired, expired_store, expired_active) =
        review_enroll("review-expired", "review-expired.json", phone, false).await;
    let reviewing = first.account();
    let (confirmation, expiry) = tokio::join!(
        reviewing.confirm_session(&expired_active.session_id),
        private(
            "/expire-session",
            serde_json::json!({"session_id": expired_active.session_id})
        )
    );
    assert_eq!(expiry.0, 200);
    if let Err(e) = confirmation {
        assert_eq!(e.code(), ErrorCode::NotFound);
    }
    confirm_error(&first, &expired_active.session_id, ErrorCode::NotFound).await;
    assert!(!sessions(&first)
        .await
        .unwrap()
        .sessions
        .iter()
        .any(|s| s.id == expired_active.session_id));
    assert!(first
        .account()
        .revoke_session(&expired_active.session_id)
        .await
        .unwrap());
    closures().await;
    server_disconnected(&expired).await;
    expired.suspend().await;
    expired.resume().await;
    let expired_states = expired.subscribe_session_state().await.unwrap();
    session_state_until(
        &expired_states,
        "late retirement consumer after expiry",
        |s| {
            s.reason == Some(SessionSignOutReason::SessionRetired)
                && s.status.cleanup == SessionCleanup::Complete
        },
    )
    .await;
    assert!(expired_store.read().unwrap().rows.is_empty());
    expired_states.stop();
    eprintln!("PASS expiry/confirmation interleaving: inactive target NotFound, never listed or revived; late consumer retains SessionRetired completion");

    let (failure, failure_store, failure_active) =
        review_enroll("review-clear-failure", "review-failure.json", phone, false).await;
    failure_store.fail_finish.store(true, Ordering::SeqCst);
    assert!(first
        .account()
        .revoke_session(&failure_active.session_id)
        .await
        .unwrap());
    closures().await;
    server_disconnected(&failure).await;
    failure.suspend().await;
    failure.resume().await;
    let failed_states = failure.subscribe_session_state().await.unwrap();
    let failed = session_state_until(
        &failed_states,
        "cleanup failure retained for late consumer",
        |s| s.status.cleanup == SessionCleanup::Failed,
    )
    .await;
    assert_eq!(failed.reason, Some(SessionSignOutReason::SessionRetired));
    assert_eq!(failed.status.authorization, SessionAuthorization::SignedOut);
    assert!(failed.sessions.is_none());
    let durable = failure_store.namespace_snapshot().await.unwrap().unwrap();
    assert!(
        durable.cleanup_pending && durable.active.is_some(),
        "failure preserves tombstone without claiming erasure"
    );
    assert!(failure_store.load_active().await.is_err());
    assert!(matches!(
        failure.connect().await,
        Err(TinkleError::SignedOut)
    ));
    failure_store.fail_finish.store(false, Ordering::SeqCst);
    assert!(
        !failure.logout().await.unwrap(),
        "cleanup retry cannot acknowledge remote Logout"
    );
    let complete = session_state_until(
        &failed_states,
        "conditional clear retry acknowledged",
        |s| s.status.cleanup == SessionCleanup::Complete,
    )
    .await;
    assert_eq!(
        complete.reason,
        Some(SessionSignOutReason::SessionRetired),
        "retry cannot replace retirement cause with SelfLogout"
    );
    assert!(failure_store.read().unwrap().rows.is_empty());
    failed_states.stop();
    eprintln!("PASS cleanup failure safety: retained SignedOut/SessionRetired/Failed, tombstone blocks restore, retry erases conditionally without remote acknowledgement");

    states2.stop();
    assert!(second.logout().await.unwrap());
    let late2 = second.subscribe_session_state().await.unwrap();
    let signed_out = session_state_until(&late2, "late self-logout consumer", |s| {
        s.status.cleanup == SessionCleanup::Complete
    })
    .await;
    assert_eq!(signed_out.reason, Some(SessionSignOutReason::SelfLogout));
    assert_eq!(
        signed_out.status.authorization,
        SessionAuthorization::SignedOut
    );
    assert!(signed_out.sessions.is_none() && signed_out.status.restart_safe_store);
    late2.stop();
    assert!(first.logout().await.unwrap());
    session_state_until(&states1, "attached self-logout consumer", |s| {
        s.reason == Some(SessionSignOutReason::SelfLogout)
            && s.status.cleanup == SessionCleanup::Complete
            && s.sessions.is_none()
    })
    .await;
    states1.stop();
    assert!(foreign.logout().await.unwrap());
    foreign_states.stop();
    closures().await;
    offline_presence(active1.user_id).await;
    offline_presence(foreign_active.user_id).await;
    assert!(store1.read().unwrap().rows.is_empty());
    assert!(store2.read().unwrap().rows.is_empty());
    assert!(foreign_store.read().unwrap().rows.is_empty());
    eprintln!("PASS self-logout state retained for attached and late consumers; all owned connections and protected namespaces cleared");
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

    // A failed acknowledgement cannot erase the marker or claim erased records.
    let failing = DiskStore::new("clear-failed.json", false);
    let original = failing.namespace_snapshot().await.unwrap().unwrap();
    let revision = failing
        .commit_current(
            &original.revision,
            SecureWrite::Operation,
            "operation",
            &blob,
        )
        .await
        .unwrap()
        .unwrap();
    let revision = failing
        .commit_current(&revision, SecureWrite::Active, "operation", &blob)
        .await
        .unwrap()
        .unwrap();
    assert!(failing.begin_clear(&revision).await.unwrap());
    failing.fail_finish.store(true, Ordering::SeqCst);
    assert!(failing.finish_clear(&revision).await.is_err());
    let reopened = DiskStore::new("clear-failed.json", false);
    let marked = reopened.namespace_snapshot().await.unwrap().unwrap();
    assert!(marked.cleanup_pending && marked.active.is_some());
    assert!(reopened.load_active().await.is_err());
    assert!(reopened
        .commit_current(&revision, SecureWrite::Operation, "late", &blob)
        .await
        .unwrap()
        .is_none());
    failing.fail_finish.store(false, Ordering::SeqCst);
    assert!(failing.finish_clear(&revision).await.unwrap());
    assert!(reopened.read().unwrap().rows.is_empty());
    eprintln!("PASS failed conditional clear: durable marker survives reopen, secret reads and late writes fenced, acknowledged retry erases records");
}
