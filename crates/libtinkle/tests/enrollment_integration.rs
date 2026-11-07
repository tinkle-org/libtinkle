//! Native encrypted enrollment, durability ordering, recovery and initialization.
#[path = "support/retirement_store.rs"]
mod retirement_store;
mod support;
use async_trait::async_trait;
use libtinkle::{
    Client, SecretBlob, SecureEnrollmentStore, SecureStoreError, SecureWrite, SignInDetails,
};
use prost::Message;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use syntra_core::{
    secure_channel::{self, Context, KeyStore, Mode, Secret32},
    ErrorCode,
};
use syntra_runtime::{ConnectionConfig, FnHandler, HandlerOutcome};
use tinkle_schema::proto::*;
use tokio::net::TcpListener;

const SESSION: &str = "96007910-fd5b-4ef2-bf0c-2e4ae1d44b1b";

/// Test-only acknowledgement adapter with one lock representing an atomic host
/// protected-store transaction. Production has no such in-memory implementation.
#[derive(Default)]
struct Store {
    clear_gate: AtomicBool,
    clear_entered: tokio::sync::Notify,
    clear_released: tokio::sync::Notify,
    active_gate: AtomicBool,
    fail_clear: AtomicBool,
    clears: std::sync::atomic::AtomicUsize,
    rows: Mutex<(HashMap<String, SecretBlob>, Option<SecretBlob>)>,
    events: Arc<Mutex<Vec<&'static str>>>,
    fail: Mutex<Option<SecureWrite>>,
    gate: AtomicBool,
    entered: tokio::sync::Notify,
    released: tokio::sync::Notify,
}
#[async_trait]
impl SecureEnrollmentStore for Store {
    async fn clear(&self) -> Result<(), SecureStoreError> {
        self.clears.fetch_add(1, Ordering::SeqCst);
        if self.clear_gate.load(Ordering::SeqCst) {
            self.clear_entered.notify_one();
            self.clear_released.notified().await;
        }
        if self.fail_clear.load(Ordering::SeqCst) {
            return Err(SecureStoreError);
        }
        let mut rows = self.rows.lock().unwrap();
        rows.0.clear();
        rows.1 = None;
        Ok(())
    }
    async fn commit(
        &self,
        kind: SecureWrite,
        id: &str,
        blob: &SecretBlob,
    ) -> Result<(), SecureStoreError> {
        if *self.fail.lock().unwrap() == Some(kind) {
            return Err(SecureStoreError);
        }
        if (kind == SecureWrite::Candidate && self.gate.load(Ordering::SeqCst))
            || (kind == SecureWrite::Active && self.active_gate.load(Ordering::SeqCst))
        {
            self.entered.notify_one();
            self.released.notified().await;
        }
        let mut rows = self.rows.lock().unwrap();
        match kind {
            SecureWrite::Operation if rows.0.contains_key(id) => return Err(SecureStoreError),
            SecureWrite::Candidate | SecureWrite::Active if !rows.0.contains_key(id) => {
                return Err(SecureStoreError)
            }
            _ => {}
        }
        rows.0
            .insert(id.into(), SecretBlob::new(blob.expose_secret().to_vec()));
        if kind == SecureWrite::Active {
            rows.1 = Some(SecretBlob::new(blob.expose_secret().to_vec()));
        }
        self.events.lock().unwrap().push(match kind {
            SecureWrite::Operation => "operation-durable",
            SecureWrite::Candidate => "candidate-durable",
            SecureWrite::Active => "active-durable",
        });
        Ok(())
    }
    async fn load_operation(&self, id: &str) -> Result<Option<SecretBlob>, SecureStoreError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .0
            .get(id)
            .map(|b| SecretBlob::new(b.expose_secret().to_vec())))
    }
    async fn load_active(&self) -> Result<Option<SecretBlob>, SecureStoreError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .1
            .as_ref()
            .map(|b| SecretBlob::new(b.expose_secret().to_vec())))
    }
}

#[derive(Default)]
struct State {
    probe_reply: ProbeReply,
    probe_control: Arc<ProbeControl>,
    bootstrap_control: Arc<ProbeControl>,
    returning_control: Arc<ProbeControl>,
    wrong_server_key: bool,
    open_bootstrap: usize,
    keepalives: usize,
    queries: Vec<DeviceEnrollmentReference>,
    retired: Vec<DeviceEnrollmentReference>,
    source_epoch: u64,
    bootstrap_admissions: usize,
    returning_attempts: usize,
    session_calls: Vec<String>,
    session_denial: Option<ErrorCode>,
    logouts: usize,
    logout_error: bool,
    lost_logout: bool,
    records: HashMap<String, (Vec<u8>, DeviceEnrollment)>,
    active: Option<DeviceEnrollment>,
    additional_active: Vec<DeviceEnrollment>,
    sign_in_account: Option<i64>,
    /// Synthetic delivery epochs exercise the full unsigned public contract.
    sign_in_epochs: Option<(u64, u64)>,
    events: Arc<Mutex<Vec<&'static str>>>,
    initializations: Vec<(Mode, String, ClientInfo)>,
    lost_sign_in: bool,
    corrupt: Option<&'static str>,
    activations: usize,
    rotations: usize,
    returning_admissions: usize,
    revoked: bool,
}

/// Wire-level negative cases supplied by the authenticated fixture authority.
#[derive(Default, Clone, Copy)]
enum ProbeReply {
    #[default]
    Normal,
    Unknown,
    Mismatch(&'static str),
    Missing,
    Unspecified,
    Future,
    Malformed,
    Rejected(ErrorCode),
    Eof,
    Timeout,
}

/// Hold one correlated response while a successor/pending activation is installed.
#[derive(Default)]
struct ProbeControl {
    hold: AtomicBool,
    entered: tokio::sync::Notify,
    released: tokio::sync::Notify,
    completed: std::sync::atomic::AtomicUsize,
}
struct Keys(Arc<Mutex<State>>);
impl KeyStore for Keys {
    fn lookup(&self, context: &Context) -> Result<Secret32, secure_channel::Error> {
        let state = self.0.lock().unwrap();
        let active = state
            .active
            .as_ref()
            .filter(|active| {
                active.handle.as_ref() == context.handle && active.enrollment_epoch == context.epoch
            })
            .or_else(|| {
                state.additional_active.iter().find(|active| {
                    active.handle.as_ref() == context.handle
                        && active.enrollment_epoch == context.epoch
                })
            })
            .ok_or(secure_channel::Error::Authentication)?;
        if state.revoked
            || active.handle.as_ref() != context.handle
            || active.enrollment_epoch != context.epoch
        {
            return Err(secure_channel::Error::Authentication);
        }
        Ok(Secret32::new(
            active.authorization_key.as_ref().try_into().unwrap(),
        ))
    }
}

fn staged(operation_id: String, epoch: u64) -> DeviceEnrollment {
    DeviceEnrollment {
        operation_id,
        session_id: SESSION.into(),
        authorization_key: vec![epoch as u8 + 5; 32].into(),
        handle: vec![epoch as u8 + 7; 8].into(),
        enrollment_epoch: epoch,
        scope: DeviceKeyScope::Account as i32,
        expires_at: Some(prost_types::Timestamp {
            seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                + 3600,
            nanos: 0,
        }),
        state: DeviceEnrollmentState::Staged as i32,
        user_id: 42,
        account_auth_epoch: 1,
    }
}
fn status(value: &DeviceEnrollment) -> DeviceEnrollmentStatus {
    DeviceEnrollmentStatus {
        operation_id: value.operation_id.clone(),
        session_id: value.session_id.clone(),
        handle: value.handle.clone(),
        enrollment_epoch: value.enrollment_epoch,
        scope: value.scope,
        expires_at: value.expires_at,
        state: value.state,
        user_id: value.user_id,
        account_auth_epoch: value.account_auth_epoch,
    }
}

/// Exact immutable reference retained from a device's protected delivery.
fn reference(value: &DeviceEnrollment) -> DeviceEnrollmentReference {
    DeviceEnrollmentReference {
        session_id: value.session_id.clone(),
        handle: value.handle.clone(),
        enrollment_epoch: value.enrollment_epoch,
        scope: value.scope,
        account_auth_epoch: value.account_auth_epoch,
    }
}
fn payload(message: impl Message) -> HandlerOutcome {
    HandlerOutcome::Payload(message.encode_to_vec())
}
fn denied() -> HandlerOutcome {
    HandlerOutcome::error(ErrorCode::Unauthenticated, "rejected")
}

fn application(state: &mut State, mode: Mode, method: &str, bytes: &[u8]) -> HandlerOutcome {
    match method {
        "/tinkle.v1.AuthService/GetEnrollmentDisposition" => {
            assert_eq!(mode, Mode::Bootstrap);
            let request = GetEnrollmentDispositionRequest::decode(bytes)
                .unwrap()
                .reference
                .unwrap();
            state.queries.push(request.clone());
            let known = state
                .records
                .values()
                .any(|(_, record)| reference(record) == request);
            let retired = known
                && (state.retired.contains(&request)
                    || state.source_epoch > request.account_auth_epoch);
            let mut response = GetEnrollmentDispositionResponse {
                reference: Some(request),
                disposition: if retired {
                    DeviceEnrollmentDisposition::Retired
                } else {
                    DeviceEnrollmentDisposition::Unknown
                } as i32,
            };
            match state.probe_reply {
                ProbeReply::Normal | ProbeReply::Eof | ProbeReply::Timeout => {}
                ProbeReply::Unknown => {
                    response.disposition = DeviceEnrollmentDisposition::Unknown as i32
                }
                ProbeReply::Missing => response.reference = None,
                ProbeReply::Unspecified => response.disposition = 0,
                ProbeReply::Future => response.disposition = 91,
                ProbeReply::Malformed => return HandlerOutcome::Payload(vec![255]),
                ProbeReply::Rejected(code) => {
                    return HandlerOutcome::error(code, "status unavailable")
                }
                ProbeReply::Mismatch(field) => {
                    let value = response.reference.as_mut().unwrap();
                    match field {
                        "session" => {
                            value.session_id = "78e5c289-656b-4428-a091-2dddf6828465".into()
                        }
                        "handle" => value.handle = vec![99; 8].into(),
                        "epoch" => value.enrollment_epoch ^= 2,
                        "scope" => value.scope = 99,
                        "account" => value.account_auth_epoch ^= 2,
                        _ => unreachable!(),
                    }
                }
            }
            payload(response)
        }
        "/tinkle.v1.AccountService/GetSessions"
        | "/tinkle.v1.AccountService/RevokeSession"
        | "/tinkle.v1.AccountService/RevokeSessions" => {
            if mode != Mode::Returning {
                return denied();
            }
            state.session_calls.push(method.to_string());
            if let Some(code) = state.session_denial {
                return HandlerOutcome::error(code, "not allowed");
            }
            match method.rsplit('/').next().unwrap() {
                "GetSessions" => payload(GetSessionsResponse {
                    sessions: vec![Session {
                        id: SESSION.into(),
                        device_model: "fixture-device".into(),
                        platform: "test".into(),
                        system_version: "1".into(),
                        app_version: "2".into(),
                        app_build: "00042".into(),
                        created_date: 1,
                        active_date: 2,
                        ip: "127.0.0.1".into(),
                        country: "".into(),
                        region: "".into(),
                        current: true,
                    }],
                }),
                "RevokeSession" => {
                    let request = RevokeSessionRequest::decode(bytes).unwrap();
                    assert_eq!(request.session_id, "78e5c289-656b-4428-a091-2dddf6828465");
                    payload(RevokeSessionResponse { revoked: true })
                }
                _ => payload(RevokeSessionsResponse { revoked: true }),
            }
        }
        "/tinkle.v1.AccountService/Logout" => {
            state.logouts += 1;
            assert_eq!(mode, Mode::Returning);
            if state.logout_error {
                return HandlerOutcome::error(ErrorCode::Unavailable, "unconfirmed revocation");
            }
            state.revoked = true;
            payload(LogoutResponse { logged_out: true })
        }
        "/tinkle.v1.ConnectionService/KeepAlive" => {
            state.keepalives += 1;
            if mode != Mode::Returning || state.active.is_none() || state.revoked {
                return denied();
            }
            let request = KeepAliveRequest::decode(bytes).unwrap();
            assert!(request.sequence > 0);
            payload(KeepAliveResponse {
                sequence: request.sequence,
            })
        }
        "/tinkle.v1.AuthService/SendCode" => {
            assert_eq!(mode, Mode::Bootstrap);
            let req = SendCodeRequest::decode(bytes).unwrap();
            assert_eq!(req.lang_code, "en");
            state.events.lock().unwrap().push("send-code");
            #[allow(deprecated)]
            payload(SendCodeResponse {
                code_type: 1,
                request_id: String::new(),
                verification_id: "verification".into(),
                expires_at: None,
            })
        }
        "/tinkle.v1.AuthService/SignIn" => {
            assert_eq!(mode, Mode::Bootstrap);
            let req = SignInRequest::decode(bytes).unwrap();
            assert_eq!(req.resume_secret.len(), 32);
            assert_eq!(req.verification_id, "verification");
            assert!(state.events.lock().unwrap().contains(&"operation-durable"));
            state.events.lock().unwrap().push("sign-in");
            let mut enrollment = staged(req.operation_id.clone(), 1);
            if let Some((enrollment_epoch, account_auth_epoch)) = state.sign_in_epochs {
                enrollment.enrollment_epoch = enrollment_epoch;
                enrollment.account_auth_epoch = account_auth_epoch;
            }
            if let Some(user_id) = state.sign_in_account {
                enrollment.user_id = user_id;
                enrollment.session_id = "78e5c289-656b-4428-a091-2dddf6828465".into();
                enrollment.handle = vec![77; 8].into();
                enrollment.authorization_key = vec![66; 32].into();
            }
            match state.corrupt {
                Some("key") => enrollment.authorization_key = vec![1; 31].into(),
                Some("handle") => enrollment.handle = vec![1; 7].into(),
                Some("scope") => enrollment.scope = 999,
                Some("state") => enrollment.state = DeviceEnrollmentState::Active as i32,
                Some("epoch") => enrollment.account_auth_epoch = 0,
                Some("expiry") => enrollment.expires_at = None,
                Some("operation") => enrollment.operation_id = SESSION.into(),
                _ => {}
            }
            state.records.insert(
                req.operation_id,
                (req.resume_secret.to_vec(), enrollment.clone()),
            );
            payload(SignInResponse {
                user: None,
                is_new_user: true,
                enrollment: Some(enrollment),
            })
        }
        "/tinkle.v1.AuthService/ResumeEnrollment" => {
            assert_eq!(mode, Mode::Bootstrap);
            state.events.lock().unwrap().push("resume");
            let req = ResumeEnrollmentRequest::decode(bytes).unwrap();
            match state.records.get(&req.operation_id) {
                Some((capability, record))
                    if capability.as_slice() == req.resume_secret.as_ref() && !state.revoked =>
                {
                    payload(ResumeEnrollmentResponse {
                        enrollment: Some(record.clone()),
                    })
                }
                _ => denied(),
            }
        }
        "/tinkle.v1.AuthService/ActivateEnrollment" => {
            assert_eq!(
                mode,
                Mode::Bootstrap,
                "STAGED Returning must never activate"
            );
            let req = ActivateEnrollmentRequest::decode(bytes).unwrap();
            let Some((capability, candidate)) = state.records.get_mut(&req.operation_id) else {
                return denied();
            };
            if capability.as_slice() != req.resume_secret.as_ref()
                || candidate.session_id != req.session_id
                || candidate.handle != req.handle
                || candidate.enrollment_epoch != req.enrollment_epoch
                || candidate.scope != req.scope
                || state.revoked
            {
                return denied();
            }
            assert!(state.events.lock().unwrap().contains(&"candidate-durable"));
            state.events.lock().unwrap().push("activation-bootstrap");
            state.activations += 1;
            candidate.state = DeviceEnrollmentState::Active as i32;
            state.active = Some(candidate.clone());
            let mut result = status(candidate);
            if state.corrupt == Some("activation-binding") {
                result.enrollment_epoch += 1;
            }
            payload(ActivateEnrollmentResponse {
                enrollment: Some(result),
            })
        }
        "/tinkle.v1.AuthService/RotateEnrollment" => {
            assert_eq!(mode, Mode::Returning);
            let req = RotateEnrollmentRequest::decode(bytes).unwrap();
            assert!(state.events.lock().unwrap().contains(&"operation-durable"));
            let Some(active) = &state.active else {
                return denied();
            };
            let record = staged(req.operation_id.clone(), active.enrollment_epoch + 1);
            state.records.insert(
                req.operation_id,
                (req.resume_secret.to_vec(), record.clone()),
            );
            state.rotations += 1;
            payload(RotateEnrollmentResponse {
                enrollment: Some(record),
            })
        }
        _ => HandlerOutcome::error(ErrorCode::Unimplemented, "fixture method unavailable"),
    }
}

async fn peek_mode(stream: &tokio::net::TcpStream) -> Mode {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut bytes = vec![0; secure_channel::MAX_WRAPPER + 2];
        loop {
            let n = stream.peek(&mut bytes).await.unwrap();
            assert!(n != 0);
            if n >= 2 {
                let len = u16::from_be_bytes(bytes[..2].try_into().unwrap()) as usize;
                assert!(len <= secure_channel::MAX_WRAPPER);
                if n >= len + 2 {
                    return Context::parse_first(&bytes[2..len + 2]).unwrap().0.mode;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

struct Fixture {
    config: libtinkle::ClientConfig,
    state: Arc<Mutex<State>>,
    store: Arc<Store>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Fixture {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = support::config(listener.local_addr().unwrap());
        let store = Arc::new(Store::default());
        let state = Arc::new(Mutex::new(State {
            events: store.events.clone(),
            ..Default::default()
        }));
        let task_state = state.clone();
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let state = task_state.clone();
                tasks.spawn(async move {
                    // Public context is routing input only. The SDK validates the
                    // entire channel and key proof after this bounded peek.
                    let mode = peek_mode(&stream).await;
                    {
                        let mut state = state.lock().unwrap();
                        if mode == Mode::Bootstrap {
                            state.bootstrap_admissions += 1;
                        } else {
                            state.returning_attempts += 1;
                        }
                    }
                    let returning_control = state.lock().unwrap().returning_control.clone();
                    let bootstrap_control = state.lock().unwrap().bootstrap_control.clone();
                    if mode == Mode::Bootstrap && bootstrap_control.hold.load(Ordering::SeqCst) {
                        bootstrap_control.entered.notify_one();
                        bootstrap_control.released.notified().await;
                    }
                    if mode == Mode::Returning && returning_control.hold.load(Ordering::SeqCst) {
                        returning_control.entered.notify_one();
                        returning_control.released.notified().await;
                    }
                    let mut security = support::server_security();
                    security.mode = mode;
                    if state.lock().unwrap().wrong_server_key {
                        security.private_key = secure_channel::generate_server_key().unwrap().0;
                    }
                    security.store = Some(Arc::new(Keys(state.clone())));
                    let host_slot =
                        Arc::new(Mutex::new(None::<std::sync::Weak<syntra_runtime::Host>>));
                    let host_ref = host_slot.clone();
                    let mut_initialized = Arc::new(AtomicBool::new(false));
                    let admission_state = state.clone();
                    let handler = Arc::new(FnHandler(move |method: String, bytes: Vec<u8>, _| {
                        let state = state.clone();
                        let host = host_ref.clone();
                        let initialized = mut_initialized.clone();
                        async move {
                            let host = loop {
                                let current = host
                                    .lock()
                                    .unwrap()
                                    .as_ref()
                                    .and_then(std::sync::Weak::upgrade);
                                if let Some(host) = current {
                                    break host;
                                }
                                tokio::task::yield_now().await;
                            };
                            let wrapped = method == "/tinkle.v1.ConnectionService/InitConnect";
                            let (method, bytes) = if wrapped {
                                let request = InitConnectRequest::decode(bytes.as_slice()).unwrap();
                                let info = request.client.unwrap();
                                assert_eq!(info.app_build, "0001");
                                state.lock().unwrap().initializations.push((
                                    mode,
                                    request.method.clone(),
                                    info,
                                ));
                                initialized.store(true, Ordering::SeqCst);
                                (request.method, request.payload.to_vec())
                            } else {
                                assert!(
                                    initialized.load(Ordering::SeqCst),
                                    "connection metadata missing"
                                );
                                (method, bytes)
                            };
                            let (outcome, probe_reply, probe_control) = {
                                let mut state = state.lock().unwrap();
                                let outcome = application(&mut state, mode, &method, &bytes);
                                if method.ends_with("/SignIn") && state.lost_sign_in {
                                    state.lost_sign_in = false;
                                    host.suspend();
                                }
                                if method.ends_with("/Logout") && state.lost_logout {
                                    host.suspend();
                                }
                                (outcome, state.probe_reply, state.probe_control.clone())
                            };
                            if method.ends_with("/GetEnrollmentDisposition") {
                                probe_control.entered.notify_one();
                                if probe_control.hold.load(Ordering::SeqCst) {
                                    probe_control.released.notified().await;
                                }
                                if matches!(probe_reply, ProbeReply::Timeout) {
                                    std::future::pending::<()>().await;
                                }
                                if matches!(probe_reply, ProbeReply::Eof) {
                                    host.suspend();
                                }
                                probe_control.completed.fetch_add(1, Ordering::SeqCst);
                            }
                            match outcome {
                                HandlerOutcome::Payload(bytes) if wrapped => {
                                    payload(InitConnectResponse {
                                        payload: bytes.into(),
                                    })
                                }
                                other => other,
                            }
                        }
                    }));
                    let result = syntra_net::tcp::accept_stream(
                        stream,
                        &security,
                        support::factory(),
                        handler,
                        ConnectionConfig::default(),
                        Default::default(),
                    )
                    .await;
                    if let Ok(handle) = result {
                        *host_slot.lock().unwrap() = Some(Arc::downgrade(&handle.host));
                        if mode == Mode::Returning {
                            let mut state = admission_state.lock().unwrap();
                            assert!(state.events.lock().unwrap().contains(&"active-durable"));
                            state.returning_admissions += 1;
                        } else {
                            admission_state.lock().unwrap().open_bootstrap += 1;
                        }
                        while !handle.is_closed() {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        handle.close().await;
                        if mode == Mode::Bootstrap {
                            admission_state.lock().unwrap().open_bootstrap -= 1;
                        }
                    }
                });
                while tasks.try_join_next().is_some() {}
            }
        });
        Self {
            config,
            state,
            store,
            task,
        }
    }
    fn client(&self) -> Client {
        Client::new(
            self.config.clone(),
            support::source(),
            libtinkle::InMemoryCursorStore::new(),
        )
    }
}
fn details() -> SignInDetails {
    SignInDetails::new(
        "+14155550123".into(),
        "123456".into(),
        "verification".into(),
    )
    .unwrap()
}

/// Atomic revisioned namespace fixture; each write advances a globally unique token.
struct FencedStore {
    inner: Arc<Store>,
    incarnation: std::sync::atomic::AtomicU64,
    state: tokio::sync::Mutex<(u64, bool, std::collections::HashSet<String>)>,
    fail_finish: AtomicBool,
    fail_restore_release: AtomicBool,
    snapshot_gate: AtomicBool,
    snapshot_entered: tokio::sync::Notify,
    snapshot_released: tokio::sync::Notify,
    restore_gate: Arc<tokio::sync::Mutex<()>>,
    restore_permits: Mutex<RestoreLocks>,
}

/// Fixture permits exclude namespace mutation and remember cancelled acquisition IDs.
#[derive(Default)]
struct RestoreLocks {
    held: HashMap<String, tokio::sync::OwnedMutexGuard<()>>,
    cancelled: std::collections::HashSet<String>,
}
impl FencedStore {
    /// Start one namespace incarnation; a production host persists this identity.
    fn new(inner: Arc<Store>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            incarnation: std::sync::atomic::AtomicU64::new(0),
            state: Default::default(),
            fail_finish: AtomicBool::new(false),
            fail_restore_release: AtomicBool::new(false),
            snapshot_gate: AtomicBool::new(false),
            snapshot_entered: Default::default(),
            snapshot_released: Default::default(),
            restore_gate: Arc::new(tokio::sync::Mutex::new(())),
            restore_permits: Default::default(),
        })
    }
    /// One immutable namespace incarnation plus a monotonically increasing revision.
    fn revision(&self, value: u64) -> String {
        format!(
            "fixture-incarnation:{}:{value}",
            self.incarnation.load(Ordering::SeqCst)
        )
    }

    /// Model an explicit host reset that cannot reuse an earlier incarnation.
    async fn reset_namespace(&self) {
        let _install = self.restore_gate.lock().await;
        let mut state = self.state.lock().await;
        self.incarnation.fetch_add(1, Ordering::SeqCst);
        *state = (0, false, Default::default());
        let mut rows = self.inner.rows.lock().unwrap();
        rows.0.clear();
        rows.1 = None;
    }
}
#[async_trait]
impl SecureEnrollmentStore for FencedStore {
    async fn begin_restore(
        &self,
        incarnation: &str,
        revision: &str,
        active: &SecretBlob,
        id: &str,
    ) -> Result<bool, SecureStoreError> {
        let install = self.restore_gate.clone().lock_owned().await;
        let state = self.state.lock().await;
        let current = self.inner.load_active().await?;
        if state.1
            || incarnation
                != format!(
                    "fixture-incarnation:{}",
                    self.incarnation.load(Ordering::SeqCst)
                )
            || revision != self.revision(state.0)
            || current
                .as_ref()
                .is_none_or(|current| current.expose_secret() != active.expose_secret())
        {
            return Ok(false);
        }
        let mut permits = self.restore_permits.lock().unwrap();
        if permits.cancelled.contains(id) {
            return Ok(false);
        }
        permits.held.insert(id.into(), install);
        Ok(true)
    }
    async fn end_restore(&self, id: &str) -> Result<(), SecureStoreError> {
        if self.fail_restore_release.load(Ordering::SeqCst) {
            return Err(SecureStoreError);
        }
        let mut permits = self.restore_permits.lock().unwrap();
        permits.cancelled.insert(id.into());
        permits.held.remove(id);
        Ok(())
    }
    async fn clear(&self) -> Result<(), SecureStoreError> {
        panic!("fenced hosts must use conditional cleanup")
    }
    async fn commit(
        &self,
        _: SecureWrite,
        _: &str,
        _: &SecretBlob,
    ) -> Result<(), SecureStoreError> {
        panic!("fenced hosts must use conditional writes")
    }
    async fn load_operation(&self, id: &str) -> Result<Option<SecretBlob>, SecureStoreError> {
        let state = self.state.lock().await;
        if state.1 {
            return Err(SecureStoreError);
        }
        self.inner.load_operation(id).await
    }
    async fn load_active(&self) -> Result<Option<SecretBlob>, SecureStoreError> {
        let state = self.state.lock().await;
        if state.1 {
            return Err(SecureStoreError);
        }
        self.inner.load_active().await
    }
    async fn namespace_snapshot(
        &self,
    ) -> Result<Option<libtinkle::EnrollmentNamespaceSnapshot>, SecureStoreError> {
        let snapshot = {
            let state = self.state.lock().await;
            libtinkle::EnrollmentNamespaceSnapshot {
                incarnation: format!(
                    "fixture-incarnation:{}",
                    self.incarnation.load(Ordering::SeqCst)
                ),
                revision: self.revision(state.0),
                cleanup_pending: state.1,
                active: self.inner.load_active().await?,
                has_pending: !state.2.is_empty(),
            }
        };
        if self.snapshot_gate.swap(false, Ordering::SeqCst) {
            self.snapshot_entered.notify_one();
            self.snapshot_released.notified().await;
        }
        Ok(Some(snapshot))
    }
    async fn commit_current(
        &self,
        expected: &str,
        kind: SecureWrite,
        id: &str,
        record: &SecretBlob,
    ) -> Result<Option<String>, SecureStoreError> {
        let _install = self.restore_gate.lock().await;
        let mut state = self.state.lock().await;
        if state.1 || expected != self.revision(state.0) {
            return Ok(None);
        }
        self.inner.commit(kind, id, record).await?;
        if kind == SecureWrite::Active {
            state.2.remove(id);
        } else {
            state.2.insert(id.into());
        }
        state.0 += 1;
        Ok(Some(self.revision(state.0)))
    }
    async fn begin_clear(&self, expected: &str) -> Result<bool, SecureStoreError> {
        let _install = self.restore_gate.lock().await;
        let mut state = self.state.lock().await;
        if expected != self.revision(state.0) {
            return Ok(false);
        }
        state.1 = true;
        Ok(true)
    }
    async fn finish_clear(&self, expected: &str) -> Result<bool, SecureStoreError> {
        let _install = self.restore_gate.lock().await;
        let state = self.state.lock().await;
        if !state.1 || expected != self.revision(state.0) {
            return Ok(false);
        }
        if self.fail_finish.load(Ordering::SeqCst) {
            return Err(SecureStoreError);
        }
        self.inner.clear().await?;
        Ok(true)
    }
}

/// Enroll through the real encrypted fixture with conditional host acknowledgements.
async fn enrolled_fenced(fixture: &Fixture) -> (Client, Arc<FencedStore>) {
    let client = fixture.client();
    let store = FencedStore::new(fixture.store.clone());
    let operation = client.prepare_enrollment(store.clone()).await.unwrap();
    let signed_in = client.auth().sign_in(details(), &operation).await.unwrap();
    client
        .auth()
        .activate_enrollment(&signed_in.enrollment)
        .await
        .unwrap();
    (client, store)
}

/// Delivery, protected-record decoding and status-first restore preserve both unsigned epochs.
#[tokio::test]
async fn full_range_epochs_survive_delivery_persistence_and_reference_query() {
    let boundaries = [1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX];
    for enrollment_epoch in boundaries {
        for account_auth_epoch in boundaries {
            let fixture = Fixture::new().await;
            fixture.state.lock().unwrap().sign_in_epochs =
                Some((enrollment_epoch, account_auth_epoch));
            let client = fixture.client();
            let store = FencedStore::new(fixture.store.clone());
            let operation = client.prepare_enrollment(store.clone()).await.unwrap();
            let delivered = client.auth().sign_in(details(), &operation).await.unwrap();
            let staged = delivered.enrollment.status().clone();
            assert_eq!(staged.enrollment_epoch, enrollment_epoch);
            assert_eq!(staged.account_auth_epoch, account_auth_epoch);
            let operation = client
                .load_enrollment_operation(store.clone(), operation.operation_id())
                .await
                .unwrap();
            let resumed = client.auth().resume_enrollment(&operation).await.unwrap();
            assert_eq!(resumed.status(), &staged);
            let active = client.auth().activate_enrollment(&resumed).await.unwrap();
            let mut expected = staged;
            expected.state = DeviceEnrollmentState::Active as i32;
            assert_eq!(active, expected);
            client.stop().await;

            let before = store.namespace_snapshot().await.unwrap().unwrap();
            let restored = fixture.client();
            assert_eq!(
                restored
                    .restore_active_enrollment(store.clone())
                    .await
                    .unwrap(),
                expected
            );
            assert_eq!(
                fixture.state.lock().unwrap().queries,
                [DeviceEnrollmentReference {
                    session_id: expected.session_id,
                    handle: expected.handle,
                    enrollment_epoch,
                    scope: expected.scope,
                    account_auth_epoch,
                }]
            );
            assert_eq!(
                restored.session_status().await.enrollment_observation,
                libtinkle::EnrollmentObservation::Unknown
            );
            restored.account().get_sessions().await.unwrap();
            let after = store.namespace_snapshot().await.unwrap().unwrap();
            assert_eq!(after.incarnation, before.incarnation);
            assert_eq!(after.revision, before.revision);
            assert!(!after.cleanup_pending);
            assert_eq!(
                after.active.unwrap().expose_secret(),
                before.active.unwrap().expose_secret()
            );
            assert_eq!(fixture.store.clears.load(Ordering::SeqCst), 0);
            restored.stop().await;
        }
    }
}

/// Zero remains invalid independently and jointly before candidate persistence or activation.
#[tokio::test]
async fn full_range_epochs_still_reject_zero_delivery() {
    for epochs in [(0, 1), (1, 0), (0, 0), (0, u64::MAX), (u64::MAX, 0)] {
        let fixture = Fixture::new().await;
        fixture.state.lock().unwrap().sign_in_epochs = Some(epochs);
        let client = fixture.client();
        let operation = client
            .prepare_enrollment(fixture.store.clone())
            .await
            .unwrap();
        assert!(matches!(
            client.auth().sign_in(details(), &operation).await,
            Err(libtinkle::TinkleError::Codec(_))
        ));
        assert!(!fixture
            .store
            .events
            .lock()
            .unwrap()
            .contains(&"candidate-durable"));
        assert!(fixture.store.load_active().await.unwrap().is_none());
        let state = fixture.state.lock().unwrap();
        assert_eq!(state.activations, 0);
        assert_eq!(state.returning_attempts, 0);
    }
}

/// Upper-half references cannot turn unknown, mismatched or future replies into cleanup.
#[tokio::test]
async fn full_range_epochs_unknown_mismatch_and_future_never_erase() {
    for high in [i64::MAX as u64 + 1, u64::MAX] {
        for epochs in [(high, 1), (1, high), (high, high)] {
            let fixture = Fixture::new().await;
            fixture.state.lock().unwrap().sign_in_epochs = Some(epochs);
            let (old, store) = enrolled_fenced(&fixture).await;
            old.stop().await;
            let expected = retire_current(&fixture);
            let before = store.namespace_snapshot().await.unwrap().unwrap();
            for reply in [
                ProbeReply::Unknown,
                ProbeReply::Mismatch("epoch"),
                ProbeReply::Mismatch("account"),
                ProbeReply::Future,
            ] {
                {
                    let mut state = fixture.state.lock().unwrap();
                    state.queries.clear();
                    state.probe_reply = reply;
                }
                let restored = fixture.client();
                let status = restored
                    .restore_active_enrollment(store.clone())
                    .await
                    .unwrap();
                assert_eq!((status.enrollment_epoch, status.account_auth_epoch), epochs);
                assert_eq!(
                    fixture.state.lock().unwrap().queries.as_slice(),
                    std::slice::from_ref(&expected)
                );
                assert_eq!(
                    restored.session_status().await.enrollment_observation,
                    libtinkle::EnrollmentObservation::Unknown
                );
                assert!(!restored.is_signed_out());
                let after = store.namespace_snapshot().await.unwrap().unwrap();
                assert_eq!(after.incarnation, before.incarnation);
                assert_eq!(after.revision, before.revision);
                assert!(!after.cleanup_pending);
                assert_eq!(
                    after.active.unwrap().expose_secret(),
                    before.active.as_ref().unwrap().expose_secret()
                );
                assert_eq!(fixture.store.clears.load(Ordering::SeqCst), 0);
                assert_eq!(fixture.state.lock().unwrap().logouts, 0);
                restored.stop().await;
            }
        }
    }
}

/// Status-first restore retires only an exact committed negative, without Returning or Logout.
#[tokio::test]
async fn observer_retired_restore_fences_and_conditionally_clears_before_returning() {
    for advanced_epoch in [false, true] {
        let fixture = Fixture::new().await;
        let (old, store) = enrolled_fenced(&fixture).await;
        old.stop().await;
        let expected = reference(fixture.state.lock().unwrap().active.as_ref().unwrap());
        {
            let mut state = fixture.state.lock().unwrap();
            state.revoked = true;
            if advanced_epoch {
                state.source_epoch = expected.account_auth_epoch + 1;
            } else {
                state.retired.push(expected.clone());
            }
        }
        let attempts = fixture.state.lock().unwrap().returning_attempts;
        let restored = fixture.client();
        let result = restored.restore_active_enrollment(store.clone()).await;
        assert!(
            matches!(result, Err(libtinkle::TinkleError::SignedOut)),
            "{result:?}"
        );
        assert!(restored.is_signed_out());
        assert!(
            store
                .namespace_snapshot()
                .await
                .unwrap()
                .unwrap()
                .cleanup_pending
        );
        assert!(fixture.store.load_active().await.unwrap().is_none());
        assert_eq!(fixture.state.lock().unwrap().queries, [expected]);
        assert_eq!(fixture.state.lock().unwrap().returning_attempts, attempts);
        assert_eq!(fixture.state.lock().unwrap().logouts, 0);
        assert!(
            !restored.logout().await.unwrap(),
            "retirement never confirms a Logout invocation"
        );
        assert!(fixture
            .client()
            .restore_active_enrollment(store)
            .await
            .is_err());
    }
}

/// Advance foreground timers only; authenticated channel clocks remain real.
async fn observer_elapse(seconds: u64) {
    tokio::task::yield_now().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(seconds)).await;
    tokio::time::resume();
    tokio::task::yield_now().await;
}

/// Bound fixture observations using real socket I/O rather than guessed sleeps.
async fn observer_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("bounded observer fixture progress");
}

/// Wait for the SDK to finish evidence comparison, not merely the server to send it.
async fn observer_wait_result(client: &Client, expected: libtinkle::EnrollmentObservation) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.session_status().await.enrollment_observation != expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("client-side observation completion");
}

/// Commit exact terminal evidence independently of the offline device.
fn retire_current(fixture: &Fixture) -> DeviceEnrollmentReference {
    let mut state = fixture.state.lock().unwrap();
    let retained = reference(state.active.as_ref().unwrap());
    state.retired.push(retained.clone());
    state.revoked = true;
    retained
}

/// Every unusable correlated response retains the protected record and pending work.
#[tokio::test]
async fn observer_unknown_mismatch_future_errors_and_eof_never_erase() {
    for reply in [
        ProbeReply::Unknown,
        ProbeReply::Unspecified,
        ProbeReply::Future,
        ProbeReply::Missing,
        ProbeReply::Malformed,
        ProbeReply::Eof,
        ProbeReply::Rejected(ErrorCode::Unimplemented),
        ProbeReply::Rejected(ErrorCode::PermissionDenied),
        ProbeReply::Rejected(ErrorCode::Unavailable),
        ProbeReply::Mismatch("session"),
        ProbeReply::Mismatch("handle"),
        ProbeReply::Mismatch("epoch"),
        ProbeReply::Mismatch("scope"),
        ProbeReply::Mismatch("account"),
    ] {
        let fixture = Fixture::new().await;
        let (old, store) = enrolled_fenced(&fixture).await;
        old.stop().await;
        retire_current(&fixture);
        fixture.state.lock().unwrap().probe_reply = reply;
        let restored = fixture.client();
        let _ = restored.restore_active_enrollment(store.clone()).await;
        assert!(!restored.is_signed_out());
        assert!(fixture.store.load_active().await.unwrap().is_some());
        assert!(
            !store
                .namespace_snapshot()
                .await
                .unwrap()
                .unwrap()
                .cleanup_pending
        );
        assert_eq!(fixture.store.clears.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.state.lock().unwrap().queries.len(), 1);
        assert_eq!(fixture.state.lock().unwrap().logouts, 0);
        restored.stop().await;
        observer_until(|| fixture.state.lock().unwrap().open_bootstrap == 0).await;
    }
}

/// A five-second status budget closes an unresponsive ephemeral connection without erasure.
#[tokio::test]
async fn observer_timeout_is_bounded_and_keeps_credentials() {
    let fixture = Fixture::new().await;
    let (old, store) = enrolled_fenced(&fixture).await;
    old.stop().await;
    retire_current(&fixture);
    fixture.state.lock().unwrap().probe_reply = ProbeReply::Timeout;
    let restored = fixture.client();
    let started = std::time::Instant::now();
    let _ = restored.restore_active_enrollment(store.clone()).await;
    assert!(started.elapsed() >= Duration::from_secs(4));
    assert!(started.elapsed() < Duration::from_secs(7));
    assert!(!restored.is_signed_out());
    assert!(fixture.store.load_active().await.unwrap().is_some());
    assert!(
        !store
            .namespace_snapshot()
            .await
            .unwrap()
            .unwrap()
            .cleanup_pending
    );
    restored.stop().await;
    observer_until(|| fixture.state.lock().unwrap().open_bootstrap == 0).await;
}

/// Denial/lookup failure cannot trigger Bootstrap, reset the timer or select another port.
#[tokio::test]
async fn observer_periodic_schedule_is_independent_of_returning_failure() {
    let mut fixture = Fixture::new().await;
    let fallback = TcpListener::bind("127.0.0.1:0").await.unwrap();
    fixture.config.fallback_port = Some(fallback.local_addr().unwrap().port());
    let (client, store) = enrolled_fenced(&fixture).await;
    assert!(fixture.state.lock().unwrap().queries.is_empty());
    let bootstrap = fixture.state.lock().unwrap().bootstrap_admissions;
    retire_current(&fixture);
    fixture.state.lock().unwrap().session_denial = Some(ErrorCode::PermissionDenied);
    assert_eq!(
        client.account().get_sessions().await.unwrap_err().code(),
        ErrorCode::PermissionDenied
    );
    assert!(client.reconnect().await.is_err());
    assert_eq!(
        fixture.state.lock().unwrap().bootstrap_admissions,
        bootstrap
    );
    observer_elapse(59).await;
    assert!(fixture.state.lock().unwrap().queries.is_empty());
    observer_elapse(1).await;
    observer_until(|| client.is_signed_out()).await;
    client.logout().await.unwrap();
    assert!(
        store
            .namespace_snapshot()
            .await
            .unwrap()
            .unwrap()
            .cleanup_pending
    );
    assert_eq!(fixture.state.lock().unwrap().queries.len(), 1);
    assert_eq!(
        fixture.state.lock().unwrap().bootstrap_admissions,
        bootstrap + 1
    );
    assert_eq!(fixture.state.lock().unwrap().logouts, 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), fallback.accept())
            .await
            .is_err()
    );
}

/// Delayed predecessor evidence must not fence a staged or installed successor.
#[tokio::test]
async fn observer_delayed_predecessor_cannot_clear_rotation_or_pending_activation() {
    for activate in [false, true] {
        let fixture = Fixture::new().await;
        let (client, store) = enrolled_fenced(&fixture).await;
        let predecessor = reference(fixture.state.lock().unwrap().active.as_ref().unwrap());
        // Commit retirement history without disrupting the already established
        // test channel, so rotation can be exercised while its query is held.
        fixture
            .state
            .lock()
            .unwrap()
            .retired
            .push(predecessor.clone());
        let control = fixture.state.lock().unwrap().probe_control.clone();
        control.hold.store(true, Ordering::SeqCst);
        observer_elapse(60).await;
        control.entered.notified().await;
        // The server's idle Core timer may retire the old foreground socket
        // while its scheduler is advanced; restore only the same Returning route.
        client.reconnect().await.unwrap();
        let operation = client.prepare_enrollment(store.clone()).await.unwrap();
        let candidate = client.auth().rotate_enrollment(&operation).await.unwrap();
        if activate {
            client.auth().activate_enrollment(&candidate).await.unwrap();
        }
        control.hold.store(false, Ordering::SeqCst);
        control.released.notify_one();
        observer_until(|| control.completed.load(Ordering::SeqCst) == 1).await;
        observer_wait_result(&client, libtinkle::EnrollmentObservation::Superseded).await;
        // A foreground call joins the same client lifecycle after the held reply.
        client.connect().await.unwrap();
        assert!(!client.is_signed_out());
        assert!(store
            .load_operation(operation.operation_id())
            .await
            .unwrap()
            .is_some());
        assert!(fixture.store.load_active().await.unwrap().is_some());
        assert!(
            !store
                .namespace_snapshot()
                .await
                .unwrap()
                .unwrap()
                .cleanup_pending
        );
        assert_eq!(fixture.store.clears.load(Ordering::SeqCst), 0);
        client.stop().await;
    }
}

/// Replace only a fixture's expiry metadata while preserving the full reference and AK bytes.
fn expired_record(blob: SecretBlob) -> SecretBlob {
    let bytes = blob.expose_secret();
    let scope_at = b"TINKLE-ENROLL-1".len() + 36 + 32 + 32 + 4;
    let scope_len = u16::from_be_bytes(bytes[scope_at..scope_at + 2].try_into().unwrap()) as usize;
    let length_at = scope_at + 2 + scope_len;
    let old_len = u32::from_be_bytes(bytes[length_at..length_at + 4].try_into().unwrap()) as usize;
    let metadata_at = length_at + 4;
    let mut status =
        DeviceEnrollmentStatus::decode(&bytes[metadata_at..metadata_at + old_len]).unwrap();
    status.expires_at = Some(prost_types::Timestamp {
        seconds: 1,
        nanos: 0,
    });
    let metadata = status.encode_to_vec();
    let mut encoded = bytes[..length_at].to_vec();
    encoded.extend_from_slice(&(metadata.len() as u32).to_be_bytes());
    encoded.extend_from_slice(&metadata);
    encoded.extend_from_slice(&bytes[metadata_at + old_len..]);
    SecretBlob::new(encoded)
}

/// Expired records supply negative lookup metadata but can never install Returning material.
#[tokio::test]
async fn observer_expired_record_is_structural_only_and_can_be_retired() {
    for retired in [false, true] {
        let fixture = Fixture::new().await;
        let (old, store) = enrolled_fenced(&fixture).await;
        old.stop().await;
        {
            let mut state = store.state.lock().await;
            state.0 += 1;
            let mut rows = fixture.store.rows.lock().unwrap();
            rows.1 = Some(expired_record(rows.1.take().unwrap()));
        }
        if retired {
            retire_current(&fixture);
        }
        let attempts = fixture.state.lock().unwrap().returning_attempts;
        let restored = fixture.client();
        let result = restored.restore_active_enrollment(store.clone()).await;
        assert!(result.is_err());
        assert_eq!(fixture.state.lock().unwrap().returning_attempts, attempts);
        assert_eq!(restored.is_signed_out(), retired);
        assert_eq!(
            fixture.store.load_active().await.unwrap().is_none(),
            retired
        );
        assert_eq!(
            store
                .namespace_snapshot()
                .await
                .unwrap()
                .unwrap()
                .cleanup_pending,
            retired
        );
        assert_eq!(fixture.state.lock().unwrap().queries.len(), 1);
        if !retired {
            // Expiry failed after the restoring binding was created, not before it.
            restored.connect().await.unwrap();
            restored
                .auth()
                .send_code(SendCodeRequest {
                    phone_number: "+14155550123".into(),
                    lang_code: "en".into(),
                })
                .await
                .unwrap();
        }
        restored.stop().await;
    }
}

/// A genuine pin rejection parks both main and observer uses until explicit recovery.
#[tokio::test]
async fn observer_pin_rejection_parks_every_use_of_the_configuration() {
    let fixture = Fixture::new().await;
    let (old, store) = enrolled_fenced(&fixture).await;
    old.stop().await;
    retire_current(&fixture);
    fixture.state.lock().unwrap().wrong_server_key = true;
    let restored = fixture.client();
    let rejection = restored.restore_active_enrollment(store.clone()).await;
    assert!(
        matches!(
            rejection,
            Err(libtinkle::TinkleError::TrustRejected | libtinkle::TinkleError::EstablishmentFailed)
        ),
        "{rejection:?}"
    );
    assert!(!restored.is_signed_out());
    let attempts = fixture.state.lock().unwrap().bootstrap_admissions;
    assert!(restored.connect().await.is_err());
    observer_elapse(60).await;
    restored.resume().await;
    assert_eq!(fixture.state.lock().unwrap().bootstrap_admissions, attempts);
    assert!(fixture.store.load_active().await.unwrap().is_some());
    fixture.state.lock().unwrap().wrong_server_key = false;
    restored
        .replace_trust_source(support::source())
        .await
        .unwrap();
    assert!(matches!(
        restored.restore_active_enrollment(store).await,
        Err(libtinkle::TinkleError::SignedOut)
    ));
}

/// Source revision and protected metadata are paired, even when trust values compare equal.
#[tokio::test]
async fn observer_restore_cannot_install_across_source_replacement_during_store_read() {
    let fixture = Fixture::new().await;
    let (old, store) = enrolled_fenced(&fixture).await;
    old.stop().await;
    store.snapshot_gate.store(true, Ordering::SeqCst);
    let client = fixture.client();
    let restoring = client.clone();
    let retained = store.clone();
    let restore = tokio::spawn(async move { restoring.restore_active_enrollment(retained).await });
    store.snapshot_entered.notified().await;
    client
        .replace_trust_source(support::source())
        .await
        .unwrap();
    store.snapshot_released.notify_one();
    assert!(matches!(
        restore.await.unwrap(),
        Err(libtinkle::TinkleError::Configuration(_))
    ));
    assert!(fixture.state.lock().unwrap().queries.is_empty());
    assert!(!client.is_connected().await);
    assert!(!client.is_signed_out());
    assert!(fixture.store.load_active().await.unwrap().is_some());
}

/// Another coordinator's pending successor supersedes a held query's store revision.
#[tokio::test]
async fn observer_stale_query_cannot_clear_another_clients_pending_operation() {
    let fixture = Fixture::new().await;
    let (client, store) = enrolled_fenced(&fixture).await;
    let retained = reference(fixture.state.lock().unwrap().active.as_ref().unwrap());
    fixture.state.lock().unwrap().retired.push(retained);
    let control = fixture.state.lock().unwrap().probe_control.clone();
    control.hold.store(true, Ordering::SeqCst);
    observer_elapse(60).await;
    control.entered.notified().await;
    let successor = fixture.client();
    let operation = successor.prepare_enrollment(store.clone()).await.unwrap();
    control.hold.store(false, Ordering::SeqCst);
    control.released.notify_one();
    observer_until(|| control.completed.load(Ordering::SeqCst) == 1).await;
    observer_wait_result(&client, libtinkle::EnrollmentObservation::Superseded).await;
    client.stop().await;
    assert!(!client.is_signed_out());
    assert!(store
        .load_operation(operation.operation_id())
        .await
        .unwrap()
        .is_some());
    assert!(
        !store
            .namespace_snapshot()
            .await
            .unwrap()
            .unwrap()
            .cleanup_pending
    );
    assert_eq!(fixture.store.clears.load(Ordering::SeqCst), 0);
}

/// Binding another pending namespace while a query is held cannot widen its cleanup scope.
#[tokio::test]
async fn observer_rechecks_bound_namespaces_after_the_response() {
    let fixture = Fixture::new().await;
    let (client, _) = enrolled_fenced(&fixture).await;
    let retained = reference(fixture.state.lock().unwrap().active.as_ref().unwrap());
    fixture.state.lock().unwrap().retired.push(retained);
    let control = fixture.state.lock().unwrap().probe_control.clone();
    control.hold.store(true, Ordering::SeqCst);
    observer_elapse(60).await;
    control.entered.notified().await;
    let other_inner = Arc::new(Store::default());
    let other = FencedStore::new(other_inner.clone());
    other.incarnation.store(91, Ordering::SeqCst);
    let successor = fixture.client();
    let operation = successor.prepare_enrollment(other.clone()).await.unwrap();
    client.bind_enrollment_store(other.clone()).await.unwrap();
    control.hold.store(false, Ordering::SeqCst);
    control.released.notify_one();
    observer_wait_result(&client, libtinkle::EnrollmentObservation::Superseded).await;
    assert!(!client.is_signed_out());
    assert!(fixture.store.load_active().await.unwrap().is_some());
    assert!(other
        .load_operation(operation.operation_id())
        .await
        .unwrap()
        .is_some());
    assert_eq!(other_inner.clears.load(Ordering::SeqCst), 0);
    client.stop().await;
}

/// Later terminal history cannot retroactively confirm a Logout whose acknowledgement was lost.
#[tokio::test]
async fn observer_never_upgrades_an_unknown_logout_invocation() {
    let fixture = Fixture::new().await;
    let (client, store) = enrolled_fenced(&fixture).await;
    fixture.state.lock().unwrap().lost_logout = true;
    store.fail_finish.store(true, Ordering::SeqCst);
    assert!(matches!(
        client.logout().await,
        Err(libtinkle::TinkleError::SecureStore)
    ));
    retire_current(&fixture);
    observer_elapse(120).await;
    assert!(fixture.state.lock().unwrap().queries.is_empty());
    store.fail_finish.store(false, Ordering::SeqCst);
    assert!(!client.logout().await.unwrap());
    assert_eq!(fixture.state.lock().unwrap().logouts, 1);
    assert!(
        store
            .namespace_snapshot()
            .await
            .unwrap()
            .unwrap()
            .cleanup_pending
    );
}

/// Failed conditional clearing stays fenced, retains its marker and never sends Logout on retry.
#[tokio::test]
async fn observer_storage_failure_keeps_fence_marker_and_local_cleanup_cause() {
    let fixture = Fixture::new().await;
    let (old, store) = enrolled_fenced(&fixture).await;
    old.stop().await;
    retire_current(&fixture);
    store.fail_finish.store(true, Ordering::SeqCst);
    let client = fixture.client();
    assert!(matches!(
        client.restore_active_enrollment(store.clone()).await,
        Err(libtinkle::TinkleError::SecureStore)
    ));
    assert!(client.is_signed_out());
    assert_eq!(
        client.session_status().await.cleanup,
        libtinkle::SessionCleanup::Failed
    );
    assert!(
        store
            .namespace_snapshot()
            .await
            .unwrap()
            .unwrap()
            .cleanup_pending
    );
    assert!(fixture
        .client()
        .restore_active_enrollment(store.clone())
        .await
        .is_err());
    store.fail_finish.store(false, Ordering::SeqCst);
    assert!(!client.logout().await.unwrap());
    assert_eq!(fixture.state.lock().unwrap().logouts, 0);
    assert!(fixture.store.load_active().await.unwrap().is_none());
}

/// A legacy store cannot enable observation or attest conditional restart-safe cleanup.
#[tokio::test]
async fn observer_unsupported_store_never_probes_or_claims_restart_safety() {
    let fixture = Fixture::new().await;
    let (old, _, _) = enrolled(&fixture).await;
    old.stop().await;
    retire_current(&fixture);
    let client = fixture.client();
    let restored = client
        .restore_active_enrollment(fixture.store.clone())
        .await
        .unwrap();
    assert_eq!(restored.state, DeviceEnrollmentState::Active as i32);
    assert!(!client.is_connected().await);
    observer_elapse(120).await;
    assert!(fixture.state.lock().unwrap().queries.is_empty());
    assert!(!client.is_signed_out());
    assert!(!client.session_status().await.restart_safe_store);
    assert!(fixture.store.load_active().await.unwrap().is_some());
    client.stop().await;
}

/// Initial dial-only fallback chooses a route shared by status-first restore and Returning.
#[tokio::test]
async fn observer_selected_fallback_is_shared_with_returning_and_later_observations() {
    let fixture = Fixture::new().await;
    let (old, store) = enrolled_fenced(&fixture).await;
    old.stop().await;
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let primary_port = reserved.local_addr().unwrap().port();
    drop(reserved);
    let mut config = fixture.config.clone();
    config.primary_port = primary_port;
    config.fallback_port = Some(fixture.config.primary_port);
    let client = Client::new(
        config,
        support::source(),
        libtinkle::InMemoryCursorStore::new(),
    );
    let control = fixture.state.lock().unwrap().probe_control.clone();
    control.hold.store(true, Ordering::SeqCst);
    let restoring = client.clone();
    let retained = store.clone();
    let restore = tokio::spawn(async move { restoring.restore_active_enrollment(retained).await });
    control.entered.notified().await;
    // The primary becomes reachable after fallback has already been selected.
    let primary = TcpListener::bind(("127.0.0.1", primary_port))
        .await
        .unwrap();
    control.hold.store(false, Ordering::SeqCst);
    control.released.notify_one();
    tokio::time::timeout(Duration::from_secs(2), restore)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(client.is_connected().await);
    assert_eq!(fixture.state.lock().unwrap().queries.len(), 1);
    retire_current(&fixture);
    observer_elapse(60).await;
    observer_until(|| client.is_signed_out()).await;
    client.logout().await.unwrap();
    assert_eq!(fixture.state.lock().unwrap().queries.len(), 2);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), primary.accept())
            .await
            .is_err()
    );
}

/// Explicit foreground entry observes first, while background time cannot create a probe.
#[tokio::test]
async fn observer_suspend_resume_is_status_first_without_a_background_connection() {
    let fixture = Fixture::new().await;
    let (client, store) = enrolled_fenced(&fixture).await;
    client.suspend().await;
    let attempts = fixture.state.lock().unwrap().returning_attempts;
    retire_current(&fixture);
    observer_elapse(120).await;
    assert!(fixture.state.lock().unwrap().queries.is_empty());
    client.resume().await;
    assert!(client.is_signed_out());
    assert_eq!(fixture.state.lock().unwrap().returning_attempts, attempts);
    assert_eq!(fixture.state.lock().unwrap().logouts, 0);
    assert!(
        store
            .namespace_snapshot()
            .await
            .unwrap()
            .unwrap()
            .cleanup_pending
    );
    observer_until(|| fixture.state.lock().unwrap().open_bootstrap == 0).await;
}

/// A held foreground observation keeps Returning parked, including an existing supervisor.
#[tokio::test]
async fn observer_foreground_probe_precedes_queued_and_supervised_returning() {
    let fixture = Fixture::new().await;
    let (client, _) = enrolled_fenced(&fixture).await;
    client.start(vec![]).await.unwrap();
    client.suspend().await;
    retire_current(&fixture);
    let attempts = fixture.state.lock().unwrap().returning_attempts;
    let control = fixture.state.lock().unwrap().probe_control.clone();
    control.hold.store(true, Ordering::SeqCst);
    let waking = client.clone();
    let resume = tokio::spawn(async move { waking.resume().await });
    control.entered.notified().await;
    assert!(client.reconnect().await.is_err());
    assert_eq!(
        fixture.state.lock().unwrap().returning_attempts,
        attempts,
        "foreground status must precede any Returning attempt"
    );
    control.hold.store(false, Ordering::SeqCst);
    control.released.notify_one();
    resume.await.unwrap();
    assert!(client.is_signed_out());
}

/// Already-foreground lifecycle calls do not create probes or postpone the original tick.
#[tokio::test]
async fn observer_repeated_foreground_calls_neither_accelerate_nor_reset_the_timer() {
    let fixture = Fixture::new().await;
    let (client, _) = enrolled_fenced(&fixture).await;
    let bootstrap = fixture.state.lock().unwrap().bootstrap_admissions;
    for seconds in [10, 10, 39] {
        observer_elapse(seconds).await;
        client.resume().await;
        assert!(fixture.state.lock().unwrap().queries.is_empty());
        assert_eq!(
            fixture.state.lock().unwrap().bootstrap_admissions,
            bootstrap
        );
    }
    observer_elapse(1).await;
    observer_wait_result(&client, libtinkle::EnrollmentObservation::Unknown).await;
    assert_eq!(fixture.state.lock().unwrap().queries.len(), 1);
    assert_eq!(
        fixture.state.lock().unwrap().bootstrap_admissions,
        bootstrap + 1
    );
    assert!(!client.is_signed_out());
    observer_until(|| fixture.state.lock().unwrap().open_bootstrap == 0).await;
    client.stop().await;
    observer_elapse(60).await;
    assert_eq!(fixture.state.lock().unwrap().queries.len(), 1);
}

/// Stop cancels and drains a host-owned probe; its late restore cannot install Returning.
#[tokio::test]
async fn observer_stop_drains_status_first_restore_and_fences_its_late_install() {
    for suspended in [false, true] {
        let fixture = Fixture::new().await;
        let (old, store) = enrolled_fenced(&fixture).await;
        old.stop().await;
        let control = fixture.state.lock().unwrap().probe_control.clone();
        control.hold.store(true, Ordering::SeqCst);
        let client = fixture.client();
        let restoring = client.clone();
        let retained = store.clone();
        let restore =
            tokio::spawn(async move { restoring.restore_active_enrollment(retained).await });
        control.entered.notified().await;
        let attempts = fixture.state.lock().unwrap().returning_attempts;
        tokio::time::timeout(Duration::from_secs(2), async {
            if suspended {
                client.suspend().await;
            } else {
                client.stop().await;
            }
        })
        .await
        .unwrap();
        assert!(restore.await.unwrap().is_err());
        observer_until(|| fixture.state.lock().unwrap().open_bootstrap == 0).await;
        assert_eq!(fixture.state.lock().unwrap().returning_attempts, attempts);
        assert!(!client.is_signed_out());
        assert!(fixture.store.load_active().await.unwrap().is_some());
        control.hold.store(false, Ordering::SeqCst);
        control.released.notify_one();
        if suspended {
            assert!(client.connect().await.is_err());
            client.resume().await;
        }
        client.connect().await.unwrap();
        client
            .auth()
            .send_code(SendCodeRequest {
                phone_number: "+14155550123".into(),
                lang_code: "en".into(),
            })
            .await
            .unwrap();
        assert_eq!(client.principal().await, "unknown");
        assert_eq!(fixture.state.lock().unwrap().returning_attempts, attempts);
        client.stop().await;
    }
}

/// A separate process reads only the retained host record and trusted public configuration.
#[tokio::test]
#[ignore = "subprocess helper for observer_offline_retirement_survives_process_restart"]
async fn observer_disk_restart_child() {
    let path = std::path::PathBuf::from(std::env::var("TINKLE_OBSERVER_PRIVATE_FIXTURE").unwrap());
    let port: u16 = std::env::var("TINKLE_OBSERVER_PORT")
        .unwrap()
        .parse()
        .unwrap();
    let pin = std::env::var("TINKLE_OBSERVER_PUBLIC_PIN").unwrap();
    let mut public_key = [0; 32];
    for (index, byte) in public_key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&pin[index * 2..index * 2 + 2], 16).unwrap();
    }
    let store = Arc::new(retirement_store::DiskStore::new(path));
    let trust = libtinkle::ServerTrust {
        server_public_key: public_key,
        server_key_id: 7,
        scope: "tinkle-fixture".into(),
    };
    let client = Client::new(
        support::config(([127, 0, 0, 1], port).into()),
        libtinkle::TrustSource::Bootstrap(trust),
        libtinkle::InMemoryCursorStore::new(),
    );
    assert!(matches!(
        client.restore_active_enrollment(store.clone()).await,
        Err(libtinkle::TinkleError::SignedOut)
    ));
    assert!(client.session_status().await.restart_safe_store);
    assert_eq!(
        client.session_status().await.cleanup,
        libtinkle::SessionCleanup::Complete
    );
    assert!(!client.logout().await.unwrap());
    assert!(
        store
            .namespace_snapshot()
            .await
            .unwrap()
            .unwrap()
            .cleanup_pending
    );
}

/// Offline retirement is detected after a real process restart; the marker survives another reopen.
#[tokio::test]
async fn observer_offline_retirement_survives_process_restart() {
    let fixture = Fixture::new().await;
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(retirement_store::DiskStore::with_events(
        directory.path().join("protected-fixture.json"),
        fixture.store.events.clone(),
    ));
    let client = fixture.client();
    let operation = client.prepare_enrollment(store.clone()).await.unwrap();
    let candidate = client.auth().sign_in(details(), &operation).await.unwrap();
    client
        .auth()
        .activate_enrollment(&candidate.enrollment)
        .await
        .unwrap();
    client.stop().await;
    drop(candidate);
    drop(operation);
    drop(client);
    retire_current(&fixture);
    let attempts = fixture.state.lock().unwrap().returning_attempts;
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--ignored", "--exact", "observer_disk_restart_child"])
        .env("TINKLE_OBSERVER_PRIVATE_FIXTURE", &store.path)
        .env(
            "TINKLE_OBSERVER_PORT",
            fixture.config.primary_port.to_string(),
        )
        .env(
            "TINKLE_OBSERVER_PUBLIC_PIN",
            support::trust()
                .server_public_key
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        );
    let result = tokio::task::spawn_blocking(move || command.status())
        .await
        .unwrap()
        .unwrap();
    assert!(result.success());
    assert_eq!(fixture.state.lock().unwrap().returning_attempts, attempts);
    assert_eq!(fixture.state.lock().unwrap().logouts, 0);
    let reopened = Arc::new(retirement_store::DiskStore::new(store.path.clone()));
    let snapshot = reopened.namespace_snapshot().await.unwrap().unwrap();
    assert!(snapshot.cleanup_pending);
    assert!(snapshot.active.is_none());
    let bootstrap = fixture.state.lock().unwrap().bootstrap_admissions;
    assert!(fixture
        .client()
        .restore_active_enrollment(reopened)
        .await
        .is_err());
    assert_eq!(
        fixture.state.lock().unwrap().bootstrap_admissions,
        bootstrap
    );
}

/// Concurrent foreground callers join one held status-first entry before Returning.
#[tokio::test]
async fn review_concurrent_resume_and_start_join_one_foreground_barrier() {
    let fixture = Fixture::new().await;
    let (client, _) = enrolled_fenced(&fixture).await;
    client.suspend().await;
    let control = fixture.state.lock().unwrap().probe_control.clone();
    control.hold.store(true, Ordering::SeqCst);
    let first_client = client.clone();
    let first = tokio::spawn(async move { first_client.resume().await });
    control.entered.notified().await;
    let attempts = fixture.state.lock().unwrap().returning_attempts;
    let second_client = client.clone();
    let second = tokio::spawn(async move { second_client.resume().await });
    let starting_client = client.clone();
    let starting = tokio::spawn(async move { starting_client.start(vec![]).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let bypassed = second.is_finished() || starting.is_finished();
    let held_attempts = fixture.state.lock().unwrap().returning_attempts;
    control.hold.store(false, Ordering::SeqCst);
    control.released.notify_one();
    first.await.unwrap();
    second.await.unwrap();
    starting.await.unwrap().unwrap();
    client.stop().await;
    assert!(!bypassed, "foreground waiters must join the held entry");
    assert_eq!(
        held_attempts, attempts,
        "Returning crossed the foreground barrier"
    );
    assert_eq!(fixture.state.lock().unwrap().queries.len(), 1);
}

/// Cancelling the waiter before any status RPC cannot complete the shared foreground entry.
#[tokio::test]
async fn review_cancelled_foreground_holder_before_rpc_is_joined_by_next_caller() {
    for start in [false, true] {
        let fixture = Fixture::new().await;
        let (client, store) = enrolled_fenced(&fixture).await;
        client.suspend().await;
        store.snapshot_gate.store(true, Ordering::SeqCst);
        let holder_client = client.clone();
        let holder = tokio::spawn(async move { holder_client.resume().await });
        store.snapshot_entered.notified().await;
        assert!(
            fixture.state.lock().unwrap().queries.is_empty(),
            "the holder must still be before its RPC"
        );
        let attempts = fixture.state.lock().unwrap().returning_attempts;
        holder.abort();
        let _ = holder.await;
        let next_client = client.clone();
        let next = tokio::spawn(async move {
            if start {
                next_client.start(vec![]).await
            } else {
                next_client.resume().await;
                Ok(())
            }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let bypassed = next.is_finished();
        let connected_early = client.reconnect().await.is_ok();
        let held_attempts = fixture.state.lock().unwrap().returning_attempts;
        let held_queries = fixture.state.lock().unwrap().queries.len();
        store.snapshot_released.notify_one();
        tokio::time::timeout(Duration::from_secs(2), next)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if !client.is_connected().await {
            client.connect().await.unwrap();
        }
        client.stop().await;
        assert!(
            !bypassed,
            "cancelled holder was treated as a completed foreground entry"
        );
        assert!(
            !connected_early,
            "Returning bypassed the unfinished status-first attempt"
        );
        assert_eq!(held_attempts, attempts);
        assert_eq!(held_queries, 0);
        assert_eq!(
            fixture.state.lock().unwrap().queries.len(),
            1,
            "foreground callers must share exactly one status RPC"
        );
    }
}

/// Explicit stop drains a detached entry and only a new foreground transition can rearm it.
#[tokio::test]
async fn review_stop_joins_detached_foreground_entry_before_rpc() {
    let fixture = Fixture::new().await;
    let (client, store) = enrolled_fenced(&fixture).await;
    client.suspend().await;
    store.snapshot_gate.store(true, Ordering::SeqCst);
    let holder_client = client.clone();
    let holder = tokio::spawn(async move { holder_client.resume().await });
    store.snapshot_entered.notified().await;
    holder.abort();
    let _ = holder.await;
    let attempts = fixture.state.lock().unwrap().returning_attempts;
    tokio::time::timeout(Duration::from_secs(2), client.stop())
        .await
        .unwrap();
    store.snapshot_released.notify_one();
    observer_elapse(60).await;
    assert!(fixture.state.lock().unwrap().queries.is_empty());
    assert_eq!(fixture.state.lock().unwrap().returning_attempts, attempts);
    client.resume().await;
    assert_eq!(fixture.state.lock().unwrap().queries.len(), 1);
    client.connect().await.unwrap();
    client.stop().await;
}

/// A real TCP first flight that never completes Bootstrap proof must park all later uses.
async fn stalled_foreground_proof_must_park(cancel_waiter: bool) {
    let mut fixture = Fixture::new().await;
    let fallback = TcpListener::bind("127.0.0.1:0").await.unwrap();
    fixture.config.fallback_port = Some(fallback.local_addr().unwrap().port());
    let (client, store, operation, _) = review_receipts(&fixture).await;
    let active = store.load_active().await.unwrap().unwrap();
    client.suspend().await;
    let control = fixture.state.lock().unwrap().bootstrap_control.clone();
    control.hold.store(true, Ordering::SeqCst);
    let admissions = fixture.state.lock().unwrap().bootstrap_admissions;
    let attempts = fixture.state.lock().unwrap().returning_attempts;
    let waking = client.clone();
    let began = std::time::Instant::now();
    let waiter = tokio::spawn(async move { waking.resume().await });
    tokio::time::timeout(Duration::from_secs(2), control.entered.notified())
        .await
        .unwrap();
    if cancel_waiter {
        waiter.abort();
        let _ = waiter.await;
        tokio::time::timeout(Duration::from_secs(7), client.resume())
            .await
            .unwrap();
    } else {
        tokio::time::timeout(Duration::from_secs(7), waiter)
            .await
            .unwrap()
            .unwrap();
    }
    let observation = client.session_status().await.enrollment_observation;
    control.hold.store(false, Ordering::SeqCst);
    control.released.notify_one();
    let returning = client.reconnect().await;
    let provisioning = client
        .auth()
        .resume_enrollment(&operation)
        .await
        .map(|_| ());
    client.resume().await; // Foreground entry alone cannot recover the parked configuration.
    let starting = client.start(vec![]).await;
    client.stop().await;
    assert!(began.elapsed() >= Duration::from_secs(4));
    assert!(began.elapsed() < Duration::from_secs(8));
    assert_eq!(
        observation,
        libtinkle::EnrollmentObservation::Parked,
        "proof timeout skipped rejection finalization"
    );
    assert!(matches!(
        returning,
        Err(libtinkle::TinkleError::TrustRejected)
    ));
    assert!(matches!(
        provisioning,
        Err(libtinkle::TinkleError::TrustRejected)
    ));
    assert!(starting.is_err());
    assert_eq!(
        fixture.state.lock().unwrap().bootstrap_admissions,
        admissions + 1
    );
    assert_eq!(fixture.state.lock().unwrap().returning_attempts, attempts);
    assert!(fixture.state.lock().unwrap().queries.is_empty());
    assert!(!client.is_signed_out());
    assert_eq!(fixture.store.clears.load(Ordering::SeqCst), 0);
    assert!(store.load_active().await.unwrap().unwrap().expose_secret() == active.expose_secret());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), fallback.accept())
            .await
            .is_err()
    );
}

/// The observation owns its proof-timeout finalizer through the absolute cutoff.
#[tokio::test]
async fn review_stalled_foreground_bootstrap_proof_parks_configuration() {
    stalled_foreground_proof_must_park(false).await;
}

/// Dropping a waiter cannot bypass timeout finalization in the retained entry task.
#[tokio::test]
async fn review_cancelled_waiter_stalled_foreground_proof_still_parks_configuration() {
    stalled_foreground_proof_must_park(true).await;
}

/// Explicit lifecycle cancellation is not proof rejection and cannot schedule a retry.
#[tokio::test]
async fn review_explicit_stop_cancels_stalled_foreground_proof_without_parking_or_retry() {
    let fixture = Fixture::new().await;
    let (client, _) = enrolled_fenced(&fixture).await;
    client.suspend().await;
    let control = fixture.state.lock().unwrap().bootstrap_control.clone();
    control.hold.store(true, Ordering::SeqCst);
    let admissions = fixture.state.lock().unwrap().bootstrap_admissions;
    let attempts = fixture.state.lock().unwrap().returning_attempts;
    let waking = client.clone();
    let waiter = tokio::spawn(async move { waking.resume().await });
    tokio::time::timeout(Duration::from_secs(2), control.entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), client.stop())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        client.session_status().await.enrollment_observation,
        libtinkle::EnrollmentObservation::Parked
    );
    assert_eq!(
        fixture.state.lock().unwrap().bootstrap_admissions,
        admissions + 1
    );
    assert_eq!(fixture.state.lock().unwrap().returning_attempts, attempts);
    assert!(fixture.state.lock().unwrap().queries.is_empty());
    control.hold.store(false, Ordering::SeqCst);
    control.released.notify_one();
    // A new explicit call can use the same trust; cancellation did not reject it.
    client.connect().await.unwrap();
    client.stop().await;
}

/// A valid UNKNOWN for account A cannot install its captured key over account B's namespace.
#[tokio::test]
async fn review_unknown_restore_rejects_another_clients_namespace_account_switch() {
    let fixture = Fixture::new().await;
    let (old, store) = enrolled_fenced(&fixture).await;
    old.stop().await;
    let control = fixture.state.lock().unwrap().probe_control.clone();
    control.hold.store(true, Ordering::SeqCst);
    let account_a = fixture.client();
    let restoring = account_a.clone();
    let retained = store.clone();
    let restore = tokio::spawn(async move { restoring.restore_active_enrollment(retained).await });
    control.entered.notified().await;
    store.reset_namespace().await;
    {
        let mut state = fixture.state.lock().unwrap();
        let active_a = state.active.as_ref().unwrap().clone();
        state.additional_active.push(active_a); // A remains valid at the authority.
        state.sign_in_account = Some(91);
    }
    let account_b = fixture.client();
    let operation = account_b.prepare_enrollment(store.clone()).await.unwrap();
    let candidate = account_b
        .auth()
        .sign_in(details(), &operation)
        .await
        .unwrap();
    account_b
        .auth()
        .activate_enrollment(&candidate.enrollment)
        .await
        .unwrap();
    assert_eq!(account_b.principal().await, "user:91");
    let active_b = store.load_active().await.unwrap().unwrap();
    let attempts = fixture.state.lock().unwrap().returning_attempts;
    control.hold.store(false, Ordering::SeqCst);
    control.released.notify_one();
    let result = restore.await.unwrap();
    let principal = account_a.principal().await;
    let connected = account_a.is_connected().await;
    account_a.stop().await;
    account_b.stop().await;
    assert!(
        result.is_err(),
        "stale account A was restored over account B"
    );
    assert_eq!(principal, "unknown");
    assert!(!connected);
    assert_eq!(fixture.state.lock().unwrap().returning_attempts, attempts);
    assert!(
        store.load_active().await.unwrap().unwrap().expose_secret() == active_b.expose_secret()
    );
    // A pre-install namespace mismatch must not leave Bootstrap admission parked.
    account_a.connect().await.unwrap();
    account_a.stop().await;
}

/// Receipts remain generation-bound across normal activation for recovery-path checks.
async fn review_receipts(
    fixture: &Fixture,
) -> (
    Client,
    Arc<FencedStore>,
    libtinkle::DurableOperation,
    libtinkle::PersistedEnrollment,
) {
    let client = fixture.client();
    let store = FencedStore::new(fixture.store.clone());
    let operation = client.prepare_enrollment(store.clone()).await.unwrap();
    let signed_in = client.auth().sign_in(details(), &operation).await.unwrap();
    client
        .auth()
        .activate_enrollment(&signed_in.enrollment)
        .await
        .unwrap();
    (client, store, operation, signed_in.enrollment)
}

/// A parked parent's three capability-bearing APIs cannot create fresh Bootstrap children.
#[tokio::test]
async fn review_parent_endpoint_park_blocks_every_provisioning_api() {
    let fixture = Fixture::new().await;
    let (client, store, operation, candidate) = review_receipts(&fixture).await;
    fixture.state.lock().unwrap().wrong_server_key = true;
    assert!(client.restore_active_enrollment(store).await.is_err());
    fixture.state.lock().unwrap().wrong_server_key = false;
    let admissions = fixture.state.lock().unwrap().bootstrap_admissions;
    let events = fixture.store.events.lock().unwrap().clone();
    let sign_in = client
        .auth()
        .sign_in(details(), &operation)
        .await
        .map(|_| ());
    let resume = client
        .auth()
        .resume_enrollment(&operation)
        .await
        .map(|_| ());
    let activate = client
        .auth()
        .activate_enrollment(&candidate)
        .await
        .map(|_| ());
    client.stop().await;
    assert_eq!(
        fixture.state.lock().unwrap().bootstrap_admissions,
        admissions,
        "parked parent dialed a provisioner"
    );
    assert_eq!(
        *fixture.store.events.lock().unwrap(),
        events,
        "a capability reached the authority"
    );
    for result in [sign_in, resume, activate] {
        assert!(matches!(result, Err(libtinkle::TinkleError::TrustRejected)));
    }
}

/// Child proof rejection parks the parent and every later child until explicit recovery.
#[tokio::test]
async fn review_provisioning_child_rejection_parks_parent_and_siblings() {
    let fixture = Fixture::new().await;
    let (client, _, operation, candidate) = review_receipts(&fixture).await;
    fixture.state.lock().unwrap().wrong_server_key = true;
    assert!(client.auth().resume_enrollment(&operation).await.is_err());
    fixture.state.lock().unwrap().wrong_server_key = false;
    let admissions = fixture.state.lock().unwrap().bootstrap_admissions;
    let ordinary = client.account().get_sessions().await.map(|_| ());
    let sibling = client
        .auth()
        .activate_enrollment(&candidate)
        .await
        .map(|_| ());
    assert!(
        matches!(ordinary, Err(libtinkle::TinkleError::TrustRejected)),
        "child rejection did not park the parent"
    );
    assert!(matches!(
        sibling,
        Err(libtinkle::TinkleError::TrustRejected)
    ));
    assert_eq!(
        fixture.state.lock().unwrap().bootstrap_admissions,
        admissions
    );
    client
        .replace_trust_source(support::source())
        .await
        .unwrap();
    client.auth().resume_enrollment(&operation).await.unwrap();
    client.stop().await;
}

/// Host exclusion spans the read-to-install/proof window rather than ending at a reread.
#[tokio::test]
async fn review_restore_permit_excludes_namespace_mutation_until_adoption_settles() {
    let fixture = Fixture::new().await;
    let (old, store) = enrolled_fenced(&fixture).await;
    old.stop().await;
    let control = fixture.state.lock().unwrap().returning_control.clone();
    control.hold.store(true, Ordering::SeqCst);
    let client = fixture.client();
    let restoring = client.clone();
    let retained = store.clone();
    let restore = tokio::spawn(async move { restoring.restore_active_enrollment(retained).await });
    control.entered.notified().await;
    assert_eq!(store.restore_permits.lock().unwrap().held.len(), 1);
    let replacing = store.clone();
    let mutation = tokio::spawn(async move { replacing.reset_namespace().await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let crossed = mutation.is_finished();
    control.hold.store(false, Ordering::SeqCst);
    control.released.notify_one();
    restore.await.unwrap().unwrap();
    mutation.await.unwrap();
    client.stop().await;
    assert!(
        !crossed,
        "namespace mutation crossed a granted install permit"
    );
    assert!(store.restore_permits.lock().unwrap().held.is_empty());
}

/// A dropped install withdraws runtime admission before releasing the host's exclusion.
#[tokio::test]
async fn review_cancelled_restore_fences_runtime_before_releasing_its_permit() {
    let fixture = Fixture::new().await;
    let (old, store) = enrolled_fenced(&fixture).await;
    old.stop().await;
    let control = fixture.state.lock().unwrap().returning_control.clone();
    control.hold.store(true, Ordering::SeqCst);
    let client = fixture.client();
    let restoring = client.clone();
    let retained = store.clone();
    let restore = tokio::spawn(async move { restoring.restore_active_enrollment(retained).await });
    control.entered.notified().await;
    let attempts = fixture.state.lock().unwrap().returning_attempts;
    restore.abort();
    let _ = restore.await;
    tokio::time::timeout(Duration::from_secs(2), store.reset_namespace())
        .await
        .unwrap();
    // The failed source is withdrawn, rather than leaving an eternal restoring fence.
    assert_eq!(client.principal().await, "user:42");
    client.connect().await.unwrap();
    assert!(client.account().get_sessions().await.is_err());
    assert_eq!(fixture.state.lock().unwrap().returning_attempts, attempts);
    assert!(!client.is_signed_out());
    assert!(store.restore_permits.lock().unwrap().held.is_empty());
    control.hold.store(false, Ordering::SeqCst);
    control.released.notify_one();
    client.stop().await;
}

/// Release failure withdraws the installed source and can be retried on this coordinator.
#[tokio::test]
async fn lifecycle_restore_release_failure_clears_its_binding_before_retry() {
    let fixture = Fixture::new().await;
    let (old, store) = enrolled_fenced(&fixture).await;
    old.stop().await;
    let client = fixture.client();
    store.fail_restore_release.store(true, Ordering::SeqCst);
    assert!(matches!(
        client.restore_active_enrollment(store.clone()).await,
        Err(libtinkle::TinkleError::SecureStore)
    ));
    assert!(!client.is_connected().await);
    assert!(!client.is_signed_out());
    store.fail_restore_release.store(false, Ordering::SeqCst);
    observer_until(|| store.restore_permits.lock().unwrap().held.is_empty()).await;
    client.connect().await.unwrap();
    client.stop().await;
    client.restore_active_enrollment(store).await.unwrap();
    assert!(client.is_connected().await);
    client.stop().await;
}

/// Cancellation before decoding/installing cannot leave the initial restoring binding parked.
#[tokio::test]
async fn lifecycle_cancelled_status_first_restore_clears_expected_binding() {
    for suspended in [false, true] {
        let fixture = Fixture::new().await;
        let (old, store) = enrolled_fenced(&fixture).await;
        old.stop().await;
        let control = fixture.state.lock().unwrap().probe_control.clone();
        control.hold.store(true, Ordering::SeqCst);
        let client = fixture.client();
        let restoring = client.clone();
        let restore = tokio::spawn(async move { restoring.restore_active_enrollment(store).await });
        control.entered.notified().await;
        restore.abort();
        if suspended {
            // Revise metadata before the cancelled future's Drop cleanup can run.
            client.suspend().await;
        }
        assert!(restore.await.unwrap_err().is_cancelled());
        control.hold.store(false, Ordering::SeqCst);
        control.released.notify_one();
        if suspended {
            client.resume().await;
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while client.connect().await.is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!client.is_signed_out());
        assert_eq!(client.principal().await, "unknown");
        client.stop().await;
    }
}

/// Failed predecessor cleanup must not invalidate a successor installed while status was pending.
#[tokio::test]
async fn lifecycle_failed_restore_preserves_replacement_binding() {
    for same_record in [false, true] {
        let fixture = Fixture::new().await;
        let (client, store) = enrolled_fenced(&fixture).await;
        client.stop().await;
        let control = fixture.state.lock().unwrap().probe_control.clone();
        control.hold.store(true, Ordering::SeqCst);
        let restoring = client.clone();
        let retained = store.clone();
        let restore =
            tokio::spawn(async move { restoring.restore_active_enrollment(retained).await });
        control.entered.notified().await;
        if same_record {
            client
                .restore_active_enrollment(store.clone())
                .await
                .unwrap();
        } else {
            fixture.state.lock().unwrap().sign_in_account = Some(91);
            let operation = client.prepare_enrollment(store.clone()).await.unwrap();
            let candidate = client.auth().sign_in(details(), &operation).await.unwrap();
            client
                .auth()
                .activate_enrollment(&candidate.enrollment)
                .await
                .unwrap();
        }
        control.hold.store(false, Ordering::SeqCst);
        control.released.notify_one();
        assert!(restore.await.unwrap().is_err());
        assert_eq!(
            client.principal().await,
            if same_record { "user:42" } else { "user:91" }
        );
        assert!(client.is_connected().await);
        client.account().get_sessions().await.unwrap();
        assert!(
            !store
                .namespace_snapshot()
                .await
                .unwrap()
                .unwrap()
                .cleanup_pending
        );
        client.stop().await;
    }
}

/// Cancellation wins even if a host completes acquisition later instead of dropping it.
#[tokio::test]
async fn review_restore_permit_release_dominates_a_late_host_acquisition() {
    let fixture = Fixture::new().await;
    let (client, store) = enrolled_fenced(&fixture).await;
    client.stop().await;
    let snapshot = store.namespace_snapshot().await.unwrap().unwrap();
    let record = snapshot.active.unwrap();
    let gate = store.restore_gate.clone().lock_owned().await;
    let acquiring = store.clone();
    let begin = tokio::spawn(async move {
        acquiring
            .begin_restore(
                &snapshot.incarnation,
                &snapshot.revision,
                &record,
                "cancelled-fixture-permit",
            )
            .await
    });
    store.end_restore("cancelled-fixture-permit").await.unwrap();
    drop(gate);
    assert!(!begin.await.unwrap().unwrap());
    assert!(store.restore_permits.lock().unwrap().held.is_empty());
}

/// A stale Client cannot erase a later namespace revision owned by another Client.
#[tokio::test]
async fn conditional_logout_cannot_clear_a_newer_clients_enrollment() {
    let fixture = Fixture::new().await;
    let store = FencedStore::new(fixture.store.clone());
    let first = fixture.client();
    first.prepare_enrollment(store.clone()).await.unwrap();
    let second = fixture.client();
    let operation = second.prepare_enrollment(store.clone()).await.unwrap();
    assert!(matches!(
        first.logout().await,
        Err(libtinkle::TinkleError::SecureStore)
    ));
    assert!(first.is_signed_out());
    assert!(store
        .load_operation(operation.operation_id())
        .await
        .unwrap()
        .is_some());
    assert_eq!(fixture.store.clears.load(Ordering::SeqCst), 0);
    assert!(!second.logout().await.unwrap());
}

/// A durable cleanup marker blocks fresh coordinators and late writes during retries.
#[tokio::test]
async fn conditional_cleanup_marker_blocks_restart_and_late_writes() {
    let fixture = Fixture::new().await;
    let store = FencedStore::new(fixture.store.clone());
    let client = fixture.client();
    client.prepare_enrollment(store.clone()).await.unwrap();
    let revision = store.namespace_snapshot().await.unwrap().unwrap().revision;
    store.fail_finish.store(true, Ordering::SeqCst);
    assert!(matches!(
        client.logout().await,
        Err(libtinkle::TinkleError::SecureStore)
    ));
    assert!(
        store
            .namespace_snapshot()
            .await
            .unwrap()
            .unwrap()
            .cleanup_pending
    );
    assert!(fixture
        .client()
        .prepare_enrollment(store.clone())
        .await
        .is_err());
    assert!(store
        .commit_current(
            &revision,
            SecureWrite::Operation,
            SESSION,
            &SecretBlob::new(vec![1])
        )
        .await
        .unwrap()
        .is_none());
    store.fail_finish.store(false, Ordering::SeqCst);
    assert!(!client.logout().await.unwrap());
    assert!(fixture.store.rows.lock().unwrap().0.is_empty());
    assert!(fixture.client().prepare_enrollment(store).await.is_err());
}

/// Even a pending old cleanup receipt cannot clear a host's new namespace incarnation.
#[tokio::test]
async fn namespace_reset_rejects_old_cleanup_receipts_and_old_writes() {
    let fixture = Fixture::new().await;
    let store = FencedStore::new(fixture.store.clone());
    let old = fixture.client();
    old.prepare_enrollment(store.clone()).await.unwrap();
    let snapshot = store.namespace_snapshot().await.unwrap().unwrap();
    store.fail_finish.store(true, Ordering::SeqCst);
    assert!(old.logout().await.is_err());
    store.reset_namespace().await;
    store.fail_finish.store(false, Ordering::SeqCst);
    let fresh = fixture.client();
    let operation = fresh.prepare_enrollment(store.clone()).await.unwrap();
    assert!(old.logout().await.is_err());
    assert!(!store.finish_clear(&snapshot.revision).await.unwrap());
    assert!(store
        .commit_current(
            &snapshot.revision,
            SecureWrite::Operation,
            SESSION,
            &SecretBlob::new(vec![1])
        )
        .await
        .unwrap()
        .is_none());
    assert!(store
        .load_operation(operation.operation_id())
        .await
        .unwrap()
        .is_some());
    fresh.logout().await.unwrap();
}

/// Transport availability, unknown account status, and cleanup completion are distinct.
#[tokio::test]
async fn session_status_distinguishes_unknown_authorization_and_cleanup() {
    use libtinkle::{SessionAuthorization, SessionCleanup, SessionNetwork};
    let fixture = Fixture::new().await;
    let initial = fixture.client().session_status().await;
    assert_eq!(initial.network, SessionNetwork::Unavailable);
    assert_eq!(initial.authorization, SessionAuthorization::Unknown);
    assert_eq!(initial.cleanup, SessionCleanup::NotStarted);
    assert!(!initial.restart_safe_store);
    let (client, _, _) = enrolled(&fixture).await;
    let active = client.session_status().await;
    assert_eq!(active.network, SessionNetwork::Available);
    assert_eq!(active.authorization, SessionAuthorization::Active);
    fixture.state.lock().unwrap().session_denial = Some(ErrorCode::PermissionDenied);
    assert!(client.account().get_sessions().await.is_err());
    let denied = client.session_status().await;
    assert_eq!(denied.network, SessionNetwork::Available);
    assert_eq!(denied.authorization, SessionAuthorization::Active);
    fixture.state.lock().unwrap().session_denial = Some(ErrorCode::Unauthenticated);
    let error = client.account().get_sessions().await.unwrap_err();
    assert_eq!(error.code(), ErrorCode::Unauthenticated);
    let unknown = client.session_status().await;
    assert_eq!(unknown.authorization, SessionAuthorization::Unknown);
    assert_eq!(unknown.cleanup, SessionCleanup::NotStarted);
    assert!(fixture.store.load_active().await.unwrap().is_some());
    fixture.store.fail_clear.store(true, Ordering::SeqCst);
    assert!(client.logout().await.is_err());
    let failed = client.session_status().await;
    assert_eq!(failed.network, SessionNetwork::Unavailable);
    assert_eq!(failed.authorization, SessionAuthorization::SignedOut);
    assert_eq!(failed.cleanup, SessionCleanup::Failed);
    fixture.store.fail_clear.store(false, Ordering::SeqCst);
    client.logout().await.unwrap();
    assert_eq!(
        client.session_status().await.cleanup,
        SessionCleanup::Complete
    );
    assert!(!client.session_status().await.restart_safe_store);
    let fenced = fixture.client();
    fenced
        .prepare_enrollment(FencedStore::new(Arc::new(Store::default())))
        .await
        .unwrap();
    assert!(fenced.session_status().await.restart_safe_store);
    fenced.logout().await.unwrap();
    assert_eq!(
        fenced.session_status().await.cleanup,
        SessionCleanup::Complete
    );
}

#[tokio::test]
async fn secure_persistence_precedes_bootstrap_activation_and_fresh_returning() {
    let fixture = Fixture::new().await;
    let client = fixture.client();
    client.connect().await.unwrap();
    let response = client
        .auth()
        .send_code(SendCodeRequest {
            phone_number: "+14155550123".into(),
            lang_code: "en".into(),
        })
        .await
        .unwrap();
    assert_eq!(response.verification_id, "verification");
    let operation = client
        .prepare_enrollment(fixture.store.clone())
        .await
        .unwrap();
    let outcome = client.auth().sign_in(details(), &operation).await.unwrap();
    assert_ne!(client.principal().await, "user:42");
    assert_eq!(
        outcome.enrollment.status().state,
        DeviceEnrollmentState::Staged as i32
    );
    assert!(
        libtinkle::Enrollment::from_active_status([7; 32], outcome.enrollment.status()).is_err(),
        "STAGED cannot construct Returning material"
    );
    assert_eq!(
        format!("{:?}", outcome.enrollment),
        "PersistedEnrollment([redacted])"
    );
    let active = client
        .auth()
        .activate_enrollment(&outcome.enrollment)
        .await
        .unwrap();
    assert_eq!(active.state, DeviceEnrollmentState::Active as i32);
    assert_eq!(client.principal().await, "user:42");
    let _: GetSessionsResponse = client
        .messages()
        .call(
            "/tinkle.v1.AccountService/GetSessions",
            &GetSessionsRequest {},
            1000,
        )
        .await
        .unwrap();
    let events = fixture.store.events.lock().unwrap().clone();
    assert_eq!(
        events,
        [
            "send-code",
            "operation-durable",
            "sign-in",
            "candidate-durable",
            "activation-bootstrap",
            "active-durable"
        ]
    );
    let initializations = fixture.state.lock().unwrap().initializations.clone();
    assert_eq!(initializations[0].1, "/tinkle.v1.AuthService/SendCode");
    assert_eq!(initializations.last().unwrap().0, Mode::Returning);
    client.close().await;
}

#[tokio::test]
async fn lost_sign_in_result_resumes_after_restart_without_sms_reuse() {
    let fixture = Fixture::new().await;
    let client = fixture.client();
    let operation = client
        .prepare_enrollment(fixture.store.clone())
        .await
        .unwrap();
    let id = operation.operation_id().to_owned();
    fixture.state.lock().unwrap().lost_sign_in = true;
    assert!(client.auth().sign_in(details(), &operation).await.is_err());
    drop(operation);
    drop(client);
    let client = fixture.client();
    let operation = client
        .load_enrollment_operation(fixture.store.clone(), &id)
        .await
        .unwrap();
    let delivered = client.auth().resume_enrollment(&operation).await.unwrap();
    client.auth().activate_enrollment(&delivered).await.unwrap();
    assert_eq!(
        fixture
            .store
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "sign-in")
            .count(),
        1
    );
    client.close().await;
    let restarted = fixture.client();
    restarted
        .restore_active_enrollment(fixture.store.clone())
        .await
        .unwrap();
    assert_eq!(restarted.principal().await, "user:42");
    restarted.close().await;
}

#[tokio::test]
async fn failed_storage_never_acknowledges_or_attempts_staged_returning() {
    for phase in [
        SecureWrite::Operation,
        SecureWrite::Candidate,
        SecureWrite::Active,
    ] {
        let fixture = Fixture::new().await;
        let client = fixture.client();
        *fixture.store.fail.lock().unwrap() = Some(phase);
        let operation = client.prepare_enrollment(fixture.store.clone()).await;
        if phase == SecureWrite::Operation {
            assert!(operation.is_err());
            continue;
        }
        let operation = operation.unwrap();
        let outcome = client.auth().sign_in(details(), &operation).await;
        if phase == SecureWrite::Candidate {
            assert!(outcome.is_err());
            assert_eq!(fixture.state.lock().unwrap().activations, 0);
            continue;
        }
        let outcome = outcome.unwrap();
        assert!(client
            .auth()
            .activate_enrollment(&outcome.enrollment)
            .await
            .is_err());
        assert_ne!(client.principal().await, "user:42");
        assert!(fixture.store.rows.lock().unwrap().1.is_none());
        *fixture.store.fail.lock().unwrap() = None;
        let resumed = client.auth().resume_enrollment(&operation).await.unwrap();
        client.auth().activate_enrollment(&resumed).await.unwrap();
        client.close().await;
    }
}

#[tokio::test]
async fn malformed_or_wrongly_bound_delivery_is_never_activated() {
    for corruption in [
        "key",
        "handle",
        "scope",
        "state",
        "epoch",
        "expiry",
        "operation",
    ] {
        let fixture = Fixture::new().await;
        fixture.state.lock().unwrap().corrupt = Some(corruption);
        let client = fixture.client();
        let operation = client
            .prepare_enrollment(fixture.store.clone())
            .await
            .unwrap();
        assert!(
            client.auth().sign_in(details(), &operation).await.is_err(),
            "{corruption}"
        );
        assert_eq!(fixture.state.lock().unwrap().activations, 0);
        assert!(fixture.store.rows.lock().unwrap().1.is_none());
    }
}

#[tokio::test]
async fn rotation_retains_old_active_slot_until_acknowledged_install() {
    let fixture = Fixture::new().await;
    let client = fixture.client();
    let operation = client
        .prepare_enrollment(fixture.store.clone())
        .await
        .unwrap();
    let first = client.auth().sign_in(details(), &operation).await.unwrap();
    client
        .auth()
        .activate_enrollment(&first.enrollment)
        .await
        .unwrap();
    let old = fixture.store.load_active().await.unwrap().unwrap();
    let operation = client
        .prepare_enrollment(fixture.store.clone())
        .await
        .unwrap();
    let next = client.auth().rotate_enrollment(&operation).await.unwrap();
    let retained = fixture.store.load_active().await.unwrap().unwrap();
    assert_eq!(old.expose_secret(), retained.expose_secret());
    assert_eq!(
        fixture
            .state
            .lock()
            .unwrap()
            .active
            .as_ref()
            .unwrap()
            .enrollment_epoch,
        1
    );
    let active = client.auth().activate_enrollment(&next).await.unwrap();
    assert_eq!(active.enrollment_epoch, 2);
    assert_eq!(active.session_id, SESSION);
    client.close().await;
}

#[tokio::test]
async fn active_restore_keeps_committed_status_despite_reconnect_rejection() {
    let fixture = Fixture::new().await;
    let client = fixture.client();
    let operation = client
        .prepare_enrollment(fixture.store.clone())
        .await
        .unwrap();
    let delivery = client.auth().sign_in(details(), &operation).await.unwrap();
    fixture.state.lock().unwrap().corrupt = Some("activation-binding");
    assert!(client
        .auth()
        .activate_enrollment(&delivery.enrollment)
        .await
        .is_err());
    assert!(fixture.store.rows.lock().unwrap().1.is_none());
    fixture.state.lock().unwrap().corrupt = None;
    client
        .auth()
        .activate_enrollment(&delivery.enrollment)
        .await
        .unwrap();
    client.close().await;
    fixture.state.lock().unwrap().revoked = true;
    let restored = fixture.client();
    let active = restored
        .restore_active_enrollment(fixture.store.clone())
        .await
        .unwrap();
    assert_eq!(active.state, DeviceEnrollmentState::Active as i32);
    assert_eq!(restored.principal().await, "user:42");
    assert!(!restored.is_connected().await);
    assert!(!restored.is_signed_out());
    assert!(fixture.store.load_active().await.unwrap().is_some());
    restored.stop().await;
}

/// Activation commits while suspended; failed reconnect does not undo that outcome.
#[tokio::test]
async fn lifecycle_suspended_activation_reports_committed_active_status() {
    let fixture = Fixture::new().await;
    let client = fixture.client();
    let operation = client
        .prepare_enrollment(fixture.store.clone())
        .await
        .unwrap();
    let candidate = client.auth().sign_in(details(), &operation).await.unwrap();
    client.suspend().await;
    let active = client
        .auth()
        .activate_enrollment(&candidate.enrollment)
        .await
        .unwrap();
    assert_eq!(active.state, DeviceEnrollmentState::Active as i32);
    assert_eq!(client.principal().await, "user:42");
    assert!(!client.is_connected().await);
    assert!(client.is_suspended().await);
    assert_eq!(fixture.state.lock().unwrap().activations, 1);
    assert!(fixture.store.load_active().await.unwrap().is_some());
    client.resume().await;
    client.connect().await.unwrap();
    assert_eq!(fixture.state.lock().unwrap().activations, 1);
    client.stop().await;
}

/// Missing host cleanup setup can be supplied after irreversible sign-out, for clearing only.
#[tokio::test]
async fn lifecycle_returning_logout_without_store_can_bind_and_retry() {
    for (conditional, advance_namespace) in [(false, false), (true, false), (true, true)] {
        let fixture = Fixture::new().await;
        let (old, store): (Client, Arc<dyn SecureEnrollmentStore>) = if conditional {
            let (old, store) = enrolled_fenced(&fixture).await;
            (old, store)
        } else {
            let (old, _, _) = enrolled(&fixture).await;
            (old, fixture.store.clone())
        };
        old.stop().await;
        let active = fixture.state.lock().unwrap().active.clone().unwrap();
        let libtinkle::TrustSource::Bootstrap(trust) = support::source() else {
            unreachable!()
        };
        let client = Client::new(
            fixture.config.clone(),
            libtinkle::TrustSource::Returning {
                trust,
                enrollment: libtinkle::Enrollment::from_active_status(
                    active.authorization_key.as_ref().try_into().unwrap(),
                    &status(&active),
                )
                .unwrap(),
            },
            libtinkle::InMemoryCursorStore::new(),
        );
        client.connect().await.unwrap();
        assert!(matches!(
            client.logout().await,
            Err(libtinkle::TinkleError::SecureStore)
        ));
        assert!(client.is_signed_out());
        assert!(!client.is_connected().await);
        assert_eq!(
            client.session_status().await.cleanup,
            libtinkle::SessionCleanup::Failed
        );
        assert_eq!(fixture.state.lock().unwrap().logouts, 1);
        client.bind_enrollment_store(store.clone()).await.unwrap();
        assert!(client
            .bind_enrollment_store(Arc::new(Store::default()))
            .await
            .is_err());
        if advance_namespace {
            let successor = fixture.client();
            let operation = successor.prepare_enrollment(store.clone()).await.unwrap();
            let expected = store.namespace_snapshot().await.unwrap().unwrap();
            assert!(matches!(
                client.logout().await,
                Err(libtinkle::TinkleError::SecureStore)
            ));
            let current = store.namespace_snapshot().await.unwrap().unwrap();
            assert_eq!(current.revision, expected.revision);
            assert!(!current.cleanup_pending);
            assert!(store
                .load_operation(operation.operation_id())
                .await
                .unwrap()
                .is_some());
            assert!(store.load_active().await.unwrap().is_some());
            assert_eq!(fixture.store.clears.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.state.lock().unwrap().logouts, 1);
            assert!(client.is_signed_out());
            assert_eq!(
                client.session_status().await.cleanup,
                libtinkle::SessionCleanup::Failed
            );
            successor.stop().await;
            continue;
        }
        assert!(client.logout().await.unwrap());
        assert!(matches!(
            client.connect().await,
            Err(libtinkle::TinkleError::SignedOut)
        ));
        assert!(matches!(
            client.prepare_enrollment(fixture.store.clone()).await,
            Err(libtinkle::TinkleError::SignedOut)
        ));
        assert!(client.is_signed_out());
        assert_eq!(
            client.session_status().await.cleanup,
            libtinkle::SessionCleanup::Complete
        );
        assert!(fixture.store.load_active().await.unwrap().is_none());
        assert_eq!(fixture.store.clears.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.state.lock().unwrap().logouts, 1);
        if conditional {
            assert!(
                store
                    .namespace_snapshot()
                    .await
                    .unwrap()
                    .unwrap()
                    .cleanup_pending
            );
        }
    }
}

#[tokio::test]
async fn candidate_receipt_is_not_available_until_async_secure_write_completes() {
    let fixture = Fixture::new().await;
    let client = fixture.client();
    let operation = client
        .prepare_enrollment(fixture.store.clone())
        .await
        .unwrap();
    fixture.store.gate.store(true, Ordering::SeqCst);
    let task_client = client.clone();
    let sign_in =
        tokio::spawn(async move { task_client.auth().sign_in(details(), &operation).await });
    fixture.store.entered.notified().await;
    assert!(!sign_in.is_finished());
    assert_eq!(fixture.state.lock().unwrap().activations, 0);
    assert_eq!(fixture.state.lock().unwrap().returning_admissions, 0);
    fixture.store.released.notify_one();
    let result = sign_in.await.unwrap().unwrap();
    client
        .auth()
        .activate_enrollment(&result.enrollment)
        .await
        .unwrap();
    assert_eq!(fixture.state.lock().unwrap().returning_admissions, 1);
    client.close().await;
}

#[tokio::test]
async fn client_initialization_is_once_per_connection_and_first_call_queue_is_bounded() {
    use syntra_core::proto::{frame::Body, rpc_result::Outcome, Frame, RpcResult};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let server_calls = calls.clone();
    let server_entered = entered.clone();
    let server_release = release.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |_, _| {
            let calls = server_calls.clone();
            let entered = server_entered.clone();
            let release = server_release.clone();
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered.notify_one();
                    release.notified().await;
                }
                #[allow(deprecated)]
                let response = SendCodeResponse {
                    verification_id: "verification".into(),
                    ..Default::default()
                }
                .encode_to_vec();
                Frame {
                    version: 1,
                    request_id: 1,
                    body: Some(Body::Result(RpcResult {
                        outcome: Some(Outcome::Payload(response)),
                    })),
                }
            }
        })
        .await
        .unwrap();
        while !handle.is_closed() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        handle.close().await;
    });
    let client = Client::new(
        config,
        support::source(),
        libtinkle::InMemoryCursorStore::new(),
    );
    client.connect().await.unwrap();
    let first_client = client.clone();
    let first = tokio::spawn(async move {
        first_client
            .auth()
            .send_code(SendCodeRequest {
                phone_number: "+14155550123".into(),
                lang_code: "en".into(),
            })
            .await
    });
    entered.notified().await;
    let queued = client
        .call_raw(
            "/tinkle.v1.AuthService/SendCode",
            SendCodeRequest {
                phone_number: "+14155550123".into(),
                lang_code: "en".into(),
            }
            .encode_to_vec(),
            10,
        )
        .await
        .unwrap_err();
    assert_eq!(queued.code(), ErrorCode::DeadlineExceeded);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "queued request must not execute after budget expiry"
    );
    release.notify_one();
    first.await.unwrap().unwrap();
    client
        .auth()
        .send_code(SendCodeRequest {
            phone_number: "+14155550123".into(),
            lang_code: "en".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "initialization never replays a target"
    );
    client.close().await;
    server.await.unwrap();
}

#[tokio::test]
async fn fresh_reconnect_installs_the_whole_metadata_snapshot_again() {
    let fixture = Fixture::new().await;
    let client = fixture.client();
    client.connect().await.unwrap();
    for _ in 0..2 {
        client
            .auth()
            .send_code(SendCodeRequest {
                phone_number: "+14155550123".into(),
                lang_code: "en".into(),
            })
            .await
            .unwrap();
    }
    assert_eq!(fixture.state.lock().unwrap().initializations.len(), 1);
    client.reconnect().await.unwrap();
    client
        .auth()
        .send_code(SendCodeRequest {
            phone_number: "+14155550123".into(),
            lang_code: "en".into(),
        })
        .await
        .unwrap();
    let info = fixture.state.lock().unwrap().initializations.clone();
    assert_eq!(info.len(), 2);
    assert_eq!(info[0].2, info[1].2);
    assert!(info[1].2.device_id.is_empty());
    client.close().await;
}

async fn enrolled(
    fixture: &Fixture,
) -> (
    Client,
    libtinkle::DurableOperation,
    libtinkle::PersistedEnrollment,
) {
    let client = fixture.client();
    let operation = client
        .prepare_enrollment(fixture.store.clone())
        .await
        .unwrap();
    let result = client.auth().sign_in(details(), &operation).await.unwrap();
    client
        .auth()
        .activate_enrollment(&result.enrollment)
        .await
        .unwrap();
    (client, operation, result.enrollment)
}

async fn assert_signed_out(client: &Client, fixture: &Fixture) {
    assert!(client.is_signed_out());
    assert!(!client.is_connected().await);
    assert_eq!(client.principal().await, "unknown");
    assert!(matches!(
        client.connect().await,
        Err(libtinkle::TinkleError::SignedOut)
    ));
    assert!(matches!(
        client.reconnect().await,
        Err(libtinkle::TinkleError::SignedOut)
    ));
    assert!(matches!(
        client
            .restore_active_enrollment(fixture.store.clone())
            .await,
        Err(libtinkle::TinkleError::SignedOut)
    ));
    assert!(matches!(
        client.prepare_enrollment(fixture.store.clone()).await,
        Err(libtinkle::TinkleError::SignedOut)
    ));
    assert!(matches!(
        client.replace_trust_source(support::source()).await,
        Err(libtinkle::TinkleError::SignedOut)
    ));
    client.resume().await;
    assert!(!client.is_connected().await);
}

#[tokio::test]
async fn offline_logout_retires_keys_and_clears_active_and_all_pending_operations() {
    let fixture = Fixture::new().await;
    let (client, operation, receipt) = enrolled(&fixture).await;
    let pending = client
        .prepare_enrollment(fixture.store.clone())
        .await
        .unwrap();
    client.stop().await;
    assert!(
        !client.logout().await.unwrap(),
        "offline revocation is unconfirmed"
    );
    assert_signed_out(&client, &fixture).await;
    assert!(fixture.store.rows.lock().unwrap().0.is_empty());
    assert!(fixture.store.load_active().await.unwrap().is_none());
    assert!(fixture.store.clears.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        client.auth().resume_enrollment(&operation).await,
        Err(libtinkle::TinkleError::SignedOut)
    ));
    assert!(matches!(
        client.auth().activate_enrollment(&receipt).await,
        Err(libtinkle::TinkleError::SignedOut)
    ));
    assert!(matches!(
        client.auth().resume_enrollment(&pending).await,
        Err(libtinkle::TinkleError::SignedOut)
    ));
    let fresh = fixture.client();
    assert!(
        fresh
            .restore_active_enrollment(fixture.store.clone())
            .await
            .is_err(),
        "durable clearing must survive a new client"
    );
}

#[tokio::test]
async fn pending_operation_logout_without_network_clears_provisioning_capability() {
    let fixture = Fixture::new().await;
    let client = fixture.client();
    let operation = client
        .prepare_enrollment(fixture.store.clone())
        .await
        .unwrap();
    let id = operation.operation_id().to_owned();
    assert!(!client.logout().await.unwrap());
    assert!(fixture.store.rows.lock().unwrap().0.is_empty());
    assert!(client.auth().resume_enrollment(&operation).await.is_err());
    assert!(fixture
        .client()
        .load_enrollment_operation(fixture.store.clone(), &id)
        .await
        .is_err());
    assert!(fixture.state.lock().unwrap().records.is_empty());
    assert_signed_out(&client, &fixture).await;
}

#[tokio::test]
async fn remote_failure_or_lost_ack_never_reverses_local_logout() {
    for lost_ack in [false, true] {
        let fixture = Fixture::new().await;
        let (client, _, _) = enrolled(&fixture).await;
        fixture.state.lock().unwrap().logout_error = !lost_ack;
        fixture.state.lock().unwrap().lost_logout = lost_ack;
        assert!(
            !client.logout().await.unwrap(),
            "remote revocation must remain unconfirmed"
        );
        assert_signed_out(&client, &fixture).await;
        assert!(fixture.store.load_active().await.unwrap().is_none());
        assert!(fixture.store.rows.lock().unwrap().0.is_empty());
        assert_eq!(fixture.state.lock().unwrap().revoked, lost_ack);
        let admissions = fixture.state.lock().unwrap().returning_admissions;
        assert!(!client.logout().await.unwrap());
        assert_eq!(fixture.state.lock().unwrap().logouts, 1);
        assert_eq!(
            fixture.state.lock().unwrap().returning_admissions,
            admissions
        );
    }
}

#[tokio::test]
async fn clear_failure_is_reported_but_cannot_restore_keys_on_the_same_client() {
    let fixture = Fixture::new().await;
    let (client, operation, receipt) = enrolled(&fixture).await;
    client.stop().await;
    fixture.store.fail_clear.store(true, Ordering::SeqCst);
    assert!(matches!(
        client.logout().await,
        Err(libtinkle::TinkleError::SecureStore)
    ));
    // Deliberately retain disk contents on failure: no false tombstone/durable claim.
    assert!(fixture.store.load_active().await.unwrap().is_some());
    assert_signed_out(&client, &fixture).await;
    assert!(client.auth().resume_enrollment(&operation).await.is_err());
    assert!(client.auth().activate_enrollment(&receipt).await.is_err());
    fixture.store.fail_clear.store(false, Ordering::SeqCst);
    assert!(!client.logout().await.unwrap());
    assert!(fixture.store.load_active().await.unwrap().is_none());
}

#[tokio::test]
async fn confirmed_remote_revocation_is_distinct_from_acknowledged_local_clearing() {
    let fixture = Fixture::new().await;
    let (client, _, _) = enrolled(&fixture).await;
    assert!(client.logout().await.unwrap());
    assert!(fixture.state.lock().unwrap().revoked);
    assert_signed_out(&client, &fixture).await;
    assert!(fixture.store.load_active().await.unwrap().is_none());
}

/// Dropping a waiter must not cancel clearing or lose the confirmed remote result.
#[tokio::test]
async fn dropped_logout_waiter_retains_cleanup_and_cumulative_confirmation() {
    let fixture = Fixture::new().await;
    let (client, _, _) = enrolled(&fixture).await;
    fixture.store.clear_gate.store(true, Ordering::SeqCst);
    let exiting = client.clone();
    let waiter = tokio::spawn(async move { exiting.logout().await });
    fixture.store.clear_entered.notified().await;
    waiter.abort();
    let _ = waiter.await;
    assert!(client.is_signed_out());
    fixture.store.clear_released.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.store.load_active().await.unwrap().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup must run independently of its waiter");
    assert!(client.logout().await.unwrap());
    assert!(client.logout().await.unwrap());
    assert_eq!(fixture.state.lock().unwrap().logouts, 1);
    assert_eq!(fixture.store.clears.load(Ordering::SeqCst), 1);
}

/// All waiters share one retained attempt; cancellation cannot strand a peer waiter.
#[tokio::test]
async fn concurrent_logout_waiters_share_one_cleanup_attempt() {
    let fixture = Fixture::new().await;
    let (client, _, _) = enrolled(&fixture).await;
    fixture.store.clear_gate.store(true, Ordering::SeqCst);
    let first_client = client.clone();
    let first = tokio::spawn(async move { first_client.logout().await });
    fixture.store.clear_entered.notified().await;
    let second_client = client.clone();
    let second = tokio::spawn(async move { second_client.logout().await });
    first.abort();
    let _ = first.await;
    fixture.store.clear_released.notify_one();
    assert!(second.await.unwrap().unwrap());
    assert_eq!(fixture.store.clears.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.state.lock().unwrap().logouts, 1);
}

/// A failed detached attempt is retryable even when nobody consumed its result.
#[tokio::test]
async fn detached_cleanup_failure_can_be_retried_directly() {
    let fixture = Fixture::new().await;
    let (client, _, _) = enrolled(&fixture).await;
    fixture.store.clear_gate.store(true, Ordering::SeqCst);
    fixture.store.fail_clear.store(true, Ordering::SeqCst);
    let exiting = client.clone();
    let waiter = tokio::spawn(async move { exiting.logout().await });
    fixture.store.clear_entered.notified().await;
    waiter.abort();
    let _ = waiter.await;
    fixture.store.clear_released.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.session_status().await.cleanup != libtinkle::SessionCleanup::Failed {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fixture.store.clear_gate.store(false, Ordering::SeqCst);
    fixture.store.fail_clear.store(false, Ordering::SeqCst);
    assert!(client.logout().await.unwrap());
    assert_eq!(fixture.state.lock().unwrap().logouts, 1);
}

/// Retrying failed local persistence must preserve remote confirmation without replay.
#[tokio::test]
async fn logout_storage_retry_keeps_remote_confirmation_without_another_mutation() {
    let fixture = Fixture::new().await;
    let (client, _, _) = enrolled(&fixture).await;
    fixture.store.fail_clear.store(true, Ordering::SeqCst);
    assert!(matches!(
        client.logout().await,
        Err(libtinkle::TinkleError::SecureStore)
    ));
    assert_eq!(fixture.state.lock().unwrap().logouts, 1);
    fixture.store.fail_clear.store(false, Ordering::SeqCst);
    assert!(client.logout().await.unwrap());
    assert!(fixture.store.load_active().await.unwrap().is_none());
    assert_eq!(fixture.state.lock().unwrap().logouts, 1);
}

/// A failed cache wipe retries the original principal even after in-memory reset.
#[tokio::test]
async fn logout_cache_retry_keeps_original_principal_and_skips_cleared_stores() {
    let fixture = Fixture::new().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cache.sqlite");
    let storage = Arc::new(libtinkle::Storage::open(path.to_str().unwrap()).unwrap());
    storage
        .save_journal("user:42", "target:user:7", 123)
        .unwrap();
    storage
        .save_journal("user:43", "target:user:7", 456)
        .unwrap();
    let database = rusqlite::Connection::open(&path).unwrap();
    database.execute_batch("CREATE TRIGGER reject_logout BEFORE DELETE ON checkpoints BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap();
    let client = Client::new_sqlite(fixture.config.clone(), support::source(), storage.clone());
    client.set_principal_user_id(42).await;
    client
        .prepare_enrollment(fixture.store.clone())
        .await
        .unwrap();
    assert!(matches!(
        client.logout().await,
        Err(libtinkle::TinkleError::CursorStore(_))
    ));
    assert_eq!(client.principal().await, "unknown");
    assert!(client.is_signed_out());
    assert_eq!(
        storage.load_journal("user:42", "target:user:7").unwrap(),
        Some(123)
    );
    database
        .execute_batch("DROP TRIGGER reject_logout;")
        .unwrap();
    assert!(!client.logout().await.unwrap());
    assert_eq!(
        storage.load_journal("user:42", "target:user:7").unwrap(),
        None
    );
    assert_eq!(
        storage.load_journal("user:43", "target:user:7").unwrap(),
        Some(456)
    );
    assert_eq!(fixture.store.clears.load(Ordering::SeqCst), 1);
}

/// Session operations use Returning, preserve descriptive fields, and do not sign out locally.
#[tokio::test]
async fn account_sessions_list_and_revoke_use_the_published_contract() {
    let fixture = Fixture::new().await;
    let (client, _, _) = enrolled(&fixture).await;
    let sessions = client.account().get_sessions().await.unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].id, SESSION);
    assert_eq!(sessions[0].app_build, "00042");
    assert!(sessions[0].current);
    assert!(client
        .account()
        .revoke_session("78e5c289-656b-4428-a091-2dddf6828465")
        .await
        .unwrap());
    assert!(client.account().revoke_other_sessions().await.unwrap());
    assert_eq!(fixture.state.lock().unwrap().session_calls.len(), 3);
    for id in [
        "",
        "42",
        "00000000-0000-0000-0000-000000000000",
        "78e5c289-656b-4428-a091-2dddf6828465 ",
    ] {
        assert!(client.account().revoke_session(id).await.is_err());
    }
    assert_eq!(fixture.state.lock().unwrap().session_calls.len(), 3);
    fixture.state.lock().unwrap().session_denial = Some(ErrorCode::PermissionDenied);
    let error = client.account().get_sessions().await.unwrap_err();
    assert_eq!(error.code(), ErrorCode::PermissionDenied);
    assert!(!client.is_signed_out());
    assert!(fixture.store.load_active().await.unwrap().is_some());
    client.logout().await.unwrap();
    assert!(matches!(
        client.account().get_sessions().await,
        Err(libtinkle::TinkleError::SignedOut)
    ));
}

/// A call queued on initialization must return SignedOut while the first call is held.
#[tokio::test]
async fn queued_initialization_call_observes_sign_out_before_dispatch() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(listener.local_addr().unwrap());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (server_entered, server_release, server_calls) =
        (entered.clone(), release.clone(), calls.clone());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handle = support::accept(stream, move |_, _| {
            let (entered, release, calls) = (
                server_entered.clone(),
                server_release.clone(),
                server_calls.clone(),
            );
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                entered.notify_one();
                release.notified().await;
                syntra_core::proto::Frame {
                    version: 1,
                    request_id: 1,
                    body: Some(syntra_core::proto::frame::Body::Result(
                        syntra_core::proto::RpcResult {
                            outcome: Some(syntra_core::proto::rpc_result::Outcome::Payload(
                                Vec::new(),
                            )),
                        },
                    )),
                }
            }
        })
        .await
        .unwrap();
        while !handle.is_closed() {
            tokio::task::yield_now().await;
        }
        handle.close().await;
    });
    let client = Client::new(
        config,
        support::source(),
        libtinkle::InMemoryCursorStore::new(),
    );
    client.connect().await.unwrap();
    let first_client = client.clone();
    let first = tokio::spawn(async move {
        first_client
            .call_raw("/tinkle.v1.AuthService/SendCode", Vec::new(), 5_000)
            .await
    });
    entered.notified().await;
    let mut queued =
        Box::pin(client.call_raw("/tinkle.v1.AuthService/SendCode", Vec::new(), 5_000));
    assert!(tokio::time::timeout(Duration::from_millis(10), &mut queued)
        .await
        .is_err());
    let exiting = client.clone();
    let logout = tokio::spawn(async move { exiting.logout().await });
    while !client.is_signed_out() {
        tokio::task::yield_now().await;
    }
    let result = tokio::time::timeout(Duration::from_secs(1), &mut queued)
        .await
        .unwrap();
    assert!(
        matches!(result, Err(libtinkle::TinkleError::SignedOut)),
        "{result:?}"
    );
    release.notify_one();
    let _ = first.await;
    logout.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.await.unwrap();
}

#[tokio::test]
async fn late_sign_in_rotation_and_active_install_cannot_repopulate_after_logout() {
    for phase in 0..3 {
        let fixture = Fixture::new().await;
        let client = if phase == 1 {
            enrolled(&fixture).await.0
        } else {
            fixture.client()
        };
        let operation = client
            .prepare_enrollment(fixture.store.clone())
            .await
            .unwrap();
        let pending = if phase == 2 {
            Some(
                client
                    .auth()
                    .sign_in(details(), &operation)
                    .await
                    .unwrap()
                    .enrollment,
            )
        } else {
            None
        };
        if phase == 2 {
            fixture.store.active_gate.store(true, Ordering::SeqCst);
        } else {
            fixture.store.gate.store(true, Ordering::SeqCst);
        }
        fixture.state.lock().unwrap().logout_error = true;
        let flow_client = client.clone();
        let flow = tokio::spawn(async move {
            match pending {
                Some(candidate) => flow_client
                    .auth()
                    .activate_enrollment(&candidate)
                    .await
                    .map(|_| ()),
                None if phase == 1 => flow_client
                    .auth()
                    .rotate_enrollment(&operation)
                    .await
                    .map(|_| ()),
                None => flow_client
                    .auth()
                    .sign_in(details(), &operation)
                    .await
                    .map(|_| ()),
            }
        });
        fixture.store.entered.notified().await;
        let exiting = client.clone();
        let logout = tokio::spawn(async move { exiting.logout().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !client.is_signed_out() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !logout.is_finished(),
            "clear must drain the pre-fence persistence transaction"
        );
        fixture.store.released.notify_one();
        assert!(matches!(
            flow.await.unwrap(),
            Err(libtinkle::TinkleError::SignedOut)
        ));
        assert!(!logout.await.unwrap().unwrap());
        assert_signed_out(&client, &fixture).await;
        assert!(fixture.store.rows.lock().unwrap().0.is_empty());
        assert!(fixture.store.load_active().await.unwrap().is_none());
    }
}
