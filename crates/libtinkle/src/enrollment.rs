//! Recoverable device enrollment with acknowledged host secure persistence.
//! No application cache or default in-memory store is used for authorization keys.
use crate::authorization::{EnrollmentEpoch, SecretCell};
use crate::{Client, Enrollment, ServerTrust, TinkleError, TrustSource};
use async_trait::async_trait;
use prost::Message;
use rand::RngCore;
use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tinkle_schema::proto::{self, DeviceEnrollmentState, DeviceEnrollmentStatus, DeviceKeyScope};
use zeroize::{Zeroize, Zeroizing};

/// Owned protected-store bytes. Non-Clone, Debug-redacted and zeroized on drop.
/// ```compile_fail
/// let secret = libtinkle::SecretBlob::new(vec![1; 32]);
/// let duplicate = secret.clone();
/// ```
pub struct SecretBlob(Zeroizing<Vec<u8>>);
impl SecretBlob {
    /// Adopt bytes loaded from platform protected storage; callers erase other copies.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(Zeroizing::new(bytes))
    }
    /// Borrow only for protected persistence/IPC. Never log or cache these bytes.
    pub fn expose_secret(&self) -> &[u8] {
        &self.0
    }
}
impl fmt::Debug for SecretBlob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretBlob([redacted])")
    }
}

/// Atomic secure-store transaction required by the enrollment coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecureWrite {
    /// Create an operation UUID/capability before any SignIn/rotation request.
    Operation,
    /// Replace that operation's record with its entire delivered candidate.
    /// Preserve any previously ACTIVE slot during rotation.
    Candidate,
    /// Atomically replace ACTIVE and the operation record after server activation.
    Active,
}

/// Sanitized platform storage failure. Diagnostic strings may contain secrets
/// and therefore do not cross this contract.
#[derive(Debug, thiserror::Error)]
#[error("protected enrollment storage unavailable")]
pub struct SecureStoreError;

/// Atomic protected-store view for restart-safe conditional writes and cleanup.
/// The revision includes a namespace incarnation and changes on every write,
/// including pending operations. It must never be reused after namespace reset.
pub struct EnrollmentNamespaceSnapshot {
    /// Stable, nonsecret namespace incarnation ID (1..=256 bytes), unique
    /// across namespaces and explicit host resets, including app extensions.
    pub incarnation: String,
    /// Opaque, nonsecret incarnation/revision token (1..=256 bytes).
    pub revision: String,
    /// Durable tombstone: restoration and every old write must be rejected.
    pub cleanup_pending: bool,
    /// Complete active record read atomically with this revision.
    pub active: Option<SecretBlob>,
    /// Recoverable operations/candidates exist besides the installed ACTIVE record.
    pub has_pending: bool,
}

/// Host Keychain/Keystore contract. Ok from commit attests the complete record
/// is durably and confidentially stored, not merely scheduled or cached. Writes
/// are atomic; Operation must reject an existing UUID, Candidate preserves the
/// active slot, Active replaces the active slot and operation record together.
/// Load returns the latest whole record. Never use the SDK's application SQLite
/// database for these bytes. There is deliberately no default implementation.
/// A store namespace belongs to one enrollment coordinator. Hosts sharing it
/// across processes must serialize transactions or use conditional writes so a
/// stale active record cannot overwrite a newer generation.
#[async_trait]
pub trait SecureEnrollmentStore: Send + Sync {
    /// Atomically compare incarnation, revision and the entire ACTIVE record,
    /// reject cleanup markers, and pin that namespace for this install permit.
    /// While granted, all writes/clears/resets must wait or reject, across Clients
    /// and processes, until end_restore. No unchecked lease expiry is permitted.
    /// False means no permission. The caller supplies a unique nonsecret permit
    /// ID so end_restore can dominate a cancelled/late begin_restore. A host that
    /// cannot provide this exclusion must fail, never emulate it with two reads.
    async fn begin_restore(
        &self,
        _incarnation: &str,
        _revision: &str,
        _active: &SecretBlob,
        _permit_id: &str,
    ) -> Result<bool, SecureStoreError> {
        Err(SecureStoreError)
    }
    /// Idempotently release/cancel this permit, including a not-yet-completed
    /// begin_restore. A lost owner must release through process-owned resources
    /// or keep mutations blocked until host recovery; never silently expire it
    /// while the old coordinator could still adopt credentials.
    async fn end_restore(&self, _permit_id: &str) -> Result<(), SecureStoreError> {
        Err(SecureStoreError)
    }
    /// Opt into conditional persistence. None is legacy support only and cannot
    /// claim restart-safe cleanup or enable automatic retirement observation.
    async fn namespace_snapshot(
        &self,
    ) -> Result<Option<EnrollmentNamespaceSnapshot>, SecureStoreError> {
        Ok(None)
    }
    /// Atomically compare the full revision, reject tombstoned namespaces, write
    /// the record, and return a fresh revision. None means superseded, no write.
    async fn commit_current(
        &self,
        _expected_revision: &str,
        _kind: SecureWrite,
        _operation_id: &str,
        _record: &SecretBlob,
    ) -> Result<Option<String>, SecureStoreError> {
        Err(SecureStoreError)
    }
    /// Persist a cleanup tombstone only if the full revision still matches.
    /// Idempotent for that revision; reject writes/loads until explicit host reset
    /// creates a new incarnation. False means superseded and changes nothing.
    async fn begin_clear(&self, _expected_revision: &str) -> Result<bool, SecureStoreError> {
        Err(SecureStoreError)
    }
    /// Erase credentials only under the matching durable tombstone. Keep the
    /// tombstone after success. False means superseded; never clear a successor.
    async fn finish_clear(&self, _expected_revision: &str) -> Result<bool, SecureStoreError> {
        Err(SecureStoreError)
    }
    /// Atomically and durably invalidate the full namespace: ACTIVE, all
    /// operations, candidates and resume capabilities. Success must dominate
    /// pending old writes (serialize them or persist a rejecting tombstone).
    /// Idempotent. On failure, callers cannot claim durable logout or permit
    /// automatic restoration after restart until the host resolves clearing.
    async fn clear(&self) -> Result<(), SecureStoreError>;
    /// Legacy acknowledged write; conditional hosts are called through commit_current.
    async fn commit(
        &self,
        kind: SecureWrite,
        operation_id: &str,
        record: &SecretBlob,
    ) -> Result<(), SecureStoreError>;
    /// Read one pending operation; tombstoned hosts must reject the read.
    async fn load_operation(
        &self,
        operation_id: &str,
    ) -> Result<Option<SecretBlob>, SecureStoreError>;
    /// Read active material; tombstoned hosts must reject the read.
    async fn load_active(&self) -> Result<Option<SecretBlob>, SecureStoreError>;
}

/// Specific host install permission; cancellation retains a bounded release owner.
pub(crate) struct RestorePermit {
    store: Arc<dyn SecureEnrollmentStore>,
    id: Option<String>,
    client: Client,
    runtime: Option<tokio::runtime::Handle>,
    pub(crate) binding: Option<Arc<crate::retirement::RetainedEnrollment>>,
    pub(crate) installed: Option<(Arc<SecretCell>, DeviceEnrollmentStatus)>,
}

impl RestorePermit {
    fn new(
        client: &Client,
        store: Arc<dyn SecureEnrollmentStore>,
        binding: Option<Arc<crate::retirement::RetainedEnrollment>>,
    ) -> Self {
        Self {
            store,
            id: None,
            client: client.background_clone(),
            runtime: tokio::runtime::Handle::try_current().ok(),
            binding,
            installed: None,
        }
    }

    /// Reserve the exact captured ACTIVE slot before installing any Returning key.
    async fn acquire(
        &mut self,
        namespace: &EnrollmentNamespaceSnapshot,
        active: &SecretBlob,
    ) -> Result<(), TinkleError> {
        let mut nonce = [0u8; 16];
        rand::rngs::OsRng
            .try_fill_bytes(&mut nonce)
            .map_err(|_| TinkleError::Configuration("entropy unavailable".into()))?;
        let id: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
        self.id = Some(id.clone());
        let granted = tokio::time::timeout(
            Duration::from_secs(10),
            self.store
                .begin_restore(&namespace.incarnation, &namespace.revision, active, &id),
        )
        .await
        .map_err(|_| storage_error())?
        .map_err(|_| storage_error())?;
        if !granted {
            return Err(storage_error());
        }
        Ok(())
    }

    async fn clear_binding(&mut self) {
        if let Some(binding) = &self.binding {
            self.client
                .clear_failed_restore(binding, self.installed.as_ref())
                .await;
            self.binding = None;
        }
    }

    /// Release only after a failed install has withdrawn its exact runtime binding.
    async fn finish<T>(&mut self, result: Result<T, TinkleError>) -> Result<T, TinkleError> {
        if result.is_err() {
            self.clear_binding().await;
        }
        if let Err(error) = self.release().await {
            self.clear_binding().await;
            return Err(error);
        }
        self.binding = None;
        result
    }

    /// Retain the ID until the host acknowledges release, including cancellation.
    async fn release(&mut self) -> Result<(), TinkleError> {
        let Some(id) = self.id.as_deref() else {
            return Ok(());
        };
        tokio::time::timeout(Duration::from_secs(10), self.store.end_restore(id))
            .await
            .map_err(|_| storage_error())?
            .map_err(|_| storage_error())?;
        self.id = None;
        Ok(())
    }
}

impl Drop for RestorePermit {
    fn drop(&mut self) {
        let binding = self.binding.take();
        let id = self.id.take();
        if binding.is_none() && id.is_none() {
            return;
        }
        let client = self.client.background_clone();
        let installed = self.installed.take();
        let store = self.store.clone();
        if let Some(runtime) = &self.runtime {
            runtime.spawn(async move {
                if let Some(binding) = binding {
                    client
                        .clear_failed_restore(&binding, installed.as_ref())
                        .await;
                }
                if let Some(id) = id {
                    let _ =
                        tokio::time::timeout(Duration::from_secs(10), store.end_restore(&id)).await;
                }
            });
        }
    }
}

/// Phone/code inputs consumed by SignIn. Never Clone or diagnostic-printable.
#[derive(Zeroize, zeroize::ZeroizeOnDrop)]
pub struct SignInDetails {
    phone_number: String,
    code: String,
    verification_id: String,
}
impl SignInDetails {
    pub fn new(
        phone_number: String,
        code: String,
        verification_id: String,
    ) -> Result<Self, TinkleError> {
        let details = Self {
            phone_number,
            code,
            verification_id,
        };
        if details.phone_number.is_empty()
            || details.phone_number.len() > 32
            || details.code.is_empty()
            || details.code.len() > 32
            || details.verification_id.is_empty()
            || details.verification_id.len() > 256
        {
            return Err(TinkleError::Codec(
                "phone, code and verification identifier required".into(),
            ));
        }
        Ok(details)
    }
    pub(crate) fn request(
        &self,
        operation: &DurableOperation,
    ) -> Result<proto::SignInRequest, TinkleError> {
        let capability = operation.record.capability.snapshot()?;
        Ok(proto::SignInRequest {
            phone_number: self.phone_number.clone(),
            code: self.code.clone(),
            verification_id: self.verification_id.clone(),
            operation_id: operation.record.operation_id.clone(),
            resume_secret: capability.as_slice().to_vec().into(),
        })
    }
}
impl fmt::Debug for SignInDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SignInDetails([redacted])")
    }
}

/// Receipt minted only after operation persistence is acknowledged (or loaded).
/// Retain the UUID in host flow state; the capability remains in protected storage.
pub struct DurableOperation {
    pub(crate) epoch: EnrollmentEpoch,
    record: Record,
    store: Arc<dyn SecureEnrollmentStore>,
}
impl DurableOperation {
    pub fn operation_id(&self) -> &str {
        &self.record.operation_id
    }
}
impl fmt::Debug for DurableOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DurableOperation([redacted])")
    }
}

/// Delivered candidate with an acknowledged secure-storage receipt. This type
/// cannot produce a Returning source. Activation is an explicit separate call.
pub struct PersistedEnrollment {
    pub(crate) epoch: EnrollmentEpoch,
    record: Record,
    store: Arc<dyn SecureEnrollmentStore>,
}
impl PersistedEnrollment {
    pub fn status(&self) -> &DeviceEnrollmentStatus {
        self.record.status.as_ref().expect("validated candidate")
    }
}
impl fmt::Debug for PersistedEnrollment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PersistedEnrollment([redacted])")
    }
}

/// SignIn's user projection and durable candidate. Does not promote Bootstrap.
pub struct SignInOutcome {
    pub user: Option<proto::User>,
    pub is_new_user: bool,
    pub enrollment: PersistedEnrollment,
}
impl fmt::Debug for SignInOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SignInOutcome([redacted])")
    }
}

struct Record {
    operation_id: String,
    capability: Arc<SecretCell>,
    trust: ServerTrust,
    status: Option<DeviceEnrollmentStatus>,
    key: Option<Arc<SecretCell>>,
}

fn invalid() -> TinkleError {
    TinkleError::Codec("invalid enrollment binding or state".into())
}
fn storage_error() -> TinkleError {
    TinkleError::SecureStore
}

/// Validate neutral operation/session identifiers without normalizing input.
pub(crate) fn uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
        && value.bytes().any(|b| b != b'0' && b != b'-')
}

fn fresh_operation(trust: ServerTrust, epoch: &EnrollmentEpoch) -> Result<Record, TinkleError> {
    trust.validate()?;
    let mut id = [0u8; 16];
    let mut capability = Zeroizing::new([0; 32]);
    rand::rngs::OsRng
        .try_fill_bytes(&mut id)
        .map_err(|_| TinkleError::Configuration("entropy unavailable".into()))?;
    rand::rngs::OsRng
        .try_fill_bytes(capability.as_mut())
        .map_err(|_| TinkleError::Configuration("entropy unavailable".into()))?;
    id[6] = (id[6] & 15) | 64;
    id[8] = (id[8] & 63) | 128;
    let hex: String = id.iter().map(|b| format!("{b:02x}")).collect();
    let operation_id = format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    );
    Ok(Record {
        operation_id,
        capability: epoch.secret(capability)?,
        trust,
        status: None,
        key: None,
    })
}

/// Erase a uniquely owned prost Bytes field immediately after adoption/encoding.
/// Generated messages are kept private to this module and never cloned here.
pub(crate) fn wipe_bytes(bytes: bytes::Bytes) {
    if let Ok(mut bytes) = bytes.try_into_mut() {
        bytes.as_mut().zeroize();
    }
}

pub(crate) fn validate_status(status: &DeviceEnrollmentStatus) -> Result<(), TinkleError> {
    validate_status_structure(status)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid())?;
    let expires = status.expires_at.ok_or_else(invalid)?;
    if (expires.seconds as u64, expires.nanos as u32) <= (now.as_secs(), now.subsec_nanos()) {
        return Err(invalid());
    }
    Ok(())
}

/// Validate retained identity and timestamp structure without granting freshness.
fn validate_status_structure(status: &DeviceEnrollmentStatus) -> Result<(), TinkleError> {
    let expires = status.expires_at.ok_or_else(invalid)?;
    if !uuid(&status.operation_id)
        || !uuid(&status.session_id)
        || status.handle.len() != 8
        || status.handle.as_ref() == [0; 8]
        || status.enrollment_epoch == 0
        || status.enrollment_epoch > i64::MAX as u64
        || status.account_auth_epoch == 0
        || status.account_auth_epoch > i64::MAX as u64
        || status.user_id <= 0
        || status.scope != DeviceKeyScope::Account as i32
        || ![
            DeviceEnrollmentState::Staged as i32,
            DeviceEnrollmentState::Active as i32,
        ]
        .contains(&status.state)
        || expires.seconds <= 0
        || expires.seconds > 253_402_300_799
        || !(0..1_000_000_000).contains(&expires.nanos)
    {
        return Err(invalid());
    }
    Ok(())
}

/// Temporary zeroizing parse result; structural inspection never installs an AK.
struct ParsedRecord {
    operation_id: String,
    capability: Zeroizing<[u8; 32]>,
    trust: ServerTrust,
    status: Option<DeviceEnrollmentStatus>,
    key: Option<Zeroizing<[u8; 32]>>,
}

/// Read only retained public metadata, including expired records, without
/// registering secret cells or constructing a Returning source.
pub(crate) fn retained_metadata(
    blob: &SecretBlob,
) -> Result<(ServerTrust, DeviceEnrollmentStatus), TinkleError> {
    let parsed = ParsedRecord::decode(blob)?;
    let status = parsed.status.ok_or_else(invalid)?;
    if status.state != DeviceEnrollmentState::Active as i32 {
        return Err(invalid());
    }
    Ok((parsed.trust, status))
}

impl Record {
    fn delivery(
        client: &Client,
        operation: &DurableOperation,
        mut delivery: proto::DeviceEnrollment,
        active_allowed: bool,
    ) -> Result<Self, TinkleError> {
        let secret = SecretBlob::new(delivery.authorization_key.to_vec());
        wipe_bytes(std::mem::take(&mut delivery.authorization_key));
        operation.epoch.check(client)?;
        let key =
            Zeroizing::new(<[u8; 32]>::try_from(secret.expose_secret()).map_err(|_| invalid())?);
        let status = DeviceEnrollmentStatus {
            operation_id: delivery.operation_id,
            session_id: delivery.session_id,
            handle: delivery.handle,
            enrollment_epoch: delivery.enrollment_epoch,
            scope: delivery.scope,
            expires_at: delivery.expires_at,
            state: delivery.state,
            user_id: delivery.user_id,
            account_auth_epoch: delivery.account_auth_epoch,
        };
        validate_status(&status)?;
        if *key == [0; 32]
            || status.operation_id != operation.operation_id()
            || (!active_allowed && status.state != DeviceEnrollmentState::Staged as i32)
        {
            return Err(invalid());
        }
        Ok(Self {
            operation_id: operation.record.operation_id.clone(),
            capability: operation.record.capability.clone(),
            trust: operation.record.trust.clone(),
            status: Some(status),
            key: Some(operation.epoch.secret(key)?),
        })
    }

    fn encode(&self) -> Result<SecretBlob, TinkleError> {
        let mut bytes = Zeroizing::new(b"TINKLE-ENROLL-1".to_vec());
        bytes.extend_from_slice(self.operation_id.as_bytes());
        bytes.extend_from_slice(self.capability.snapshot()?.as_slice());
        bytes.extend_from_slice(&self.trust.server_public_key);
        bytes.extend_from_slice(&self.trust.server_key_id.to_be_bytes());
        bytes.extend_from_slice(&(self.trust.scope.len() as u16).to_be_bytes());
        bytes.extend_from_slice(self.trust.scope.as_bytes());
        match (&self.status, &self.key) {
            (Some(status), Some(key)) => {
                let metadata = status.encode_to_vec();
                bytes.extend_from_slice(&(metadata.len() as u32).to_be_bytes());
                bytes.extend_from_slice(&metadata);
                bytes.extend_from_slice(key.snapshot()?.as_slice());
            }
            _ => bytes.extend_from_slice(&0u32.to_be_bytes()),
        }
        Ok(SecretBlob(bytes))
    }

    fn decode(blob: &SecretBlob, epoch: &EnrollmentEpoch) -> Result<Self, TinkleError> {
        let parsed = ParsedRecord::decode(blob)?;
        if let Some(status) = &parsed.status {
            validate_status(status)?;
        }
        Ok(Self {
            operation_id: parsed.operation_id,
            capability: epoch.secret(parsed.capability)?,
            trust: parsed.trust,
            status: parsed.status,
            key: parsed.key.map(|key| epoch.secret(key)).transpose()?,
        })
    }

    fn returning(&self) -> Result<TrustSource, TinkleError> {
        let status = self.status.as_ref().ok_or_else(invalid)?;
        validate_status(status)?;
        if status.state != DeviceEnrollmentState::Active as i32 {
            return Err(invalid());
        }
        Ok(TrustSource::Returning {
            trust: self.trust.clone(),
            enrollment: Enrollment::from_active_status(
                *self.key.as_ref().ok_or_else(invalid)?.snapshot()?,
                status,
            )?,
        })
    }
}

impl ParsedRecord {
    /// Parse the existing protected record format once, with structural validation only.
    fn decode(blob: &SecretBlob) -> Result<Self, TinkleError> {
        if blob.expose_secret().len() > 4096 {
            return Err(invalid());
        }
        let mut input = blob.expose_secret();
        fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8], TinkleError> {
            if input.len() < n {
                return Err(invalid());
            }
            let (value, rest) = input.split_at(n);
            *input = rest;
            Ok(value)
        }
        if take(&mut input, b"TINKLE-ENROLL-1".len())? != b"TINKLE-ENROLL-1" {
            return Err(invalid());
        }
        let operation_id = std::str::from_utf8(take(&mut input, 36)?)
            .map_err(|_| invalid())?
            .to_owned();
        let capability = Zeroizing::new(take(&mut input, 32)?.try_into().map_err(|_| invalid())?);
        let server_public_key = take(&mut input, 32)?.try_into().map_err(|_| invalid())?;
        let server_key_id =
            u32::from_be_bytes(take(&mut input, 4)?.try_into().map_err(|_| invalid())?);
        let scope_len =
            u16::from_be_bytes(take(&mut input, 2)?.try_into().map_err(|_| invalid())?) as usize;
        let scope = std::str::from_utf8(take(&mut input, scope_len)?)
            .map_err(|_| invalid())?
            .to_owned();
        let trust = ServerTrust {
            server_public_key,
            server_key_id,
            scope,
        };
        trust.validate()?;
        let n =
            u32::from_be_bytes(take(&mut input, 4)?.try_into().map_err(|_| invalid())?) as usize;
        let (status, key) = if n == 0 {
            (None, None)
        } else {
            let status =
                DeviceEnrollmentStatus::decode(take(&mut input, n)?).map_err(|_| invalid())?;
            validate_status_structure(&status)?;
            let key =
                Zeroizing::new(<[u8; 32]>::try_from(take(&mut input, 32)?).map_err(|_| invalid())?);
            if *key == [0; 32] || status.operation_id != operation_id {
                return Err(invalid());
            }
            (Some(status), Some(key))
        };
        if !input.is_empty() || !uuid(&operation_id) || *capability == [0; 32] {
            return Err(invalid());
        }
        Ok(Self {
            operation_id,
            capability,
            trust,
            status,
            key,
        })
    }
}

async fn commit(
    client: &Client,
    epoch: &EnrollmentEpoch,
    store: &dyn SecureEnrollmentStore,
    kind: SecureWrite,
    record: &Record,
) -> Result<(), TinkleError> {
    epoch.check(client)?;
    // A pending operation or ambiguous activation must fence predecessor evidence.
    client.invalidate_retirement_binding().await;
    let encoded = record.encode()?;
    let revision = client.authorization.store_revision(store);
    tokio::time::timeout(Duration::from_secs(10), async {
        if let Some(previous) = revision.as_deref() {
            let next = store
                .commit_current(previous, kind, &record.operation_id, &encoded)
                .await
                .map_err(|_| storage_error())?
                .ok_or_else(storage_error)?;
            if next.is_empty() || next.len() > 256 || next == previous {
                return Err(storage_error());
            }
            // A completed pre-fence transaction must update cleanup's expected
            // revision even when sign-out occurred while the host was writing.
            client
                .authorization
                .update_store_revision(store, revision.as_deref(), Some(next))?;
        } else {
            store
                .commit(kind, &record.operation_id, &encoded)
                .await
                .map_err(|_| storage_error())?;
        }
        Ok::<_, TinkleError>(())
    })
    .await
    .map_err(|_| storage_error())??;
    epoch.check(client)
}

impl Client {
    /// Bind one atomic host view without adopting another coordinator's later revision.
    async fn bind_namespace(
        &self,
        epoch: &EnrollmentEpoch,
        store: Arc<dyn SecureEnrollmentStore>,
    ) -> Result<Option<EnrollmentNamespaceSnapshot>, TinkleError> {
        let snapshot = tokio::time::timeout(Duration::from_secs(10), store.namespace_snapshot())
            .await
            .map_err(|_| storage_error())?
            .map_err(|_| storage_error())?;
        epoch.check(self)?;
        if let Some(value) = &snapshot {
            if value.cleanup_pending
                || value.incarnation.is_empty()
                || value.incarnation.len() > 256
                || value.revision.is_empty()
                || value.revision.len() > 256
            {
                return Err(storage_error());
            }
        }
        epoch.store(
            self,
            store,
            snapshot.as_ref().map(|value| value.incarnation.clone()),
            snapshot.as_ref().map(|value| value.revision.clone()),
        )?;
        Ok(snapshot)
    }

    /// Bind a host namespace for acknowledged logout clearing. Conditional hosts
    /// supply an atomic protected snapshot; no key is installed or written here.
    /// Enrollment/restore methods also bind their store.
    /// After sign-out, a missing store may be bound for clearing only; no key or
    /// enrollment authority is restored, and an existing cleanup binding stays fixed.
    pub async fn bind_enrollment_store(
        &self,
        store: Arc<dyn SecureEnrollmentStore>,
    ) -> Result<(), TinkleError> {
        if self.is_signed_out() {
            return self.bind_cleanup_store(store).await;
        }
        let epoch = self.authorization.generation()?;
        let _flow = self.enrollment_lock.lock().await;
        self.bind_namespace(&epoch, store).await.map(|_| ())
    }

    pub(crate) async fn enrollment_trust(&self) -> ServerTrust {
        match &*self.trust_source.read().await {
            TrustSource::Bootstrap(trust)
            | TrustSource::SignedOut(trust)
            | TrustSource::Returning { trust, .. } => trust.clone(),
        }
    }

    /// Retain a cryptographically random operation UUID and capability durably
    /// before making SignIn or RotateEnrollment available to the caller.
    pub async fn prepare_enrollment(
        &self,
        store: Arc<dyn SecureEnrollmentStore>,
    ) -> Result<DurableOperation, TinkleError> {
        let epoch = self.authorization.generation()?;
        let _flow = self.enrollment_lock.lock().await;
        self.bind_namespace(&epoch, store.clone()).await?;
        let record = fresh_operation(self.enrollment_trust().await, &epoch)?;
        commit(
            self,
            &epoch,
            store.as_ref(),
            SecureWrite::Operation,
            &record,
        )
        .await?;
        Ok(DurableOperation {
            record,
            store,
            epoch,
        })
    }

    /// Restore a pending operation after process death or an ambiguous result.
    pub async fn load_enrollment_operation(
        &self,
        store: Arc<dyn SecureEnrollmentStore>,
        operation_id: &str,
    ) -> Result<DurableOperation, TinkleError> {
        let epoch = self.authorization.generation()?;
        let _flow = self.enrollment_lock.lock().await;
        self.bind_namespace(&epoch, store.clone()).await?;
        if !uuid(operation_id) {
            return Err(invalid());
        }
        let blob =
            tokio::time::timeout(Duration::from_secs(10), store.load_operation(operation_id))
                .await
                .map_err(|_| storage_error())?
                .map_err(|_| storage_error())?
                .ok_or_else(storage_error)?;
        epoch.check(self)?;
        let record = Record::decode(&blob, &epoch)?;
        if record.operation_id != operation_id || record.trust != self.enrollment_trust().await {
            return Err(invalid());
        }
        Ok(DurableOperation {
            record,
            store,
            epoch,
        })
    }

    /// Observe the retained reference before restoring an unexpired ACTIVE key.
    /// Expired keys remain eligible only for negative observation, never Returning.
    pub async fn restore_active_enrollment(
        &self,
        store: Arc<dyn SecureEnrollmentStore>,
    ) -> Result<DeviceEnrollmentStatus, TinkleError> {
        let epoch = self.authorization.generation()?;
        let flow = self.enrollment_lock.lock().await;
        let source_revision = {
            let _source = self.connect_lock.lock().await;
            self.retirement.revision()
        };
        let snapshot = self.bind_namespace(&epoch, store.clone()).await?;
        let conditional = snapshot
            .as_ref()
            .is_some_and(|snapshot| !snapshot.has_pending);
        let blob = match snapshot {
            Some(snapshot) => snapshot.active.ok_or_else(storage_error)?,
            None => tokio::time::timeout(Duration::from_secs(10), store.load_active())
                .await
                .map_err(|_| storage_error())?
                .map_err(|_| storage_error())?
                .ok_or_else(storage_error)?,
        };
        epoch.check(self)?;
        let (trust, status) = retained_metadata(&blob)?;
        if trust != self.enrollment_trust().await {
            return Err(invalid());
        }
        let expected = {
            let _source = self.connect_lock.lock().await;
            if self.retirement.revision() != source_revision
                || trust != self.enrollment_trust().await
            {
                return Err(TinkleError::Configuration(
                    "source changed while loading enrollment".into(),
                ));
            }
            self.retirement
                .bind(trust, &status, store.clone(), true, conditional)
        };
        let mut permit = RestorePermit::new(self, store.clone(), Some(expected));
        drop(flow);
        let mut install_flow = None;
        let result = async {
            if conditional {
                let observed = self.observe_restored_enrollment().await;
                if self.is_signed_out() {
                    return observed.and(Err(TinkleError::SignedOut));
                }
                if self
                    .endpoint_rejected
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    return observed.and(Err(TinkleError::TrustRejected));
                }
                // Unknown, unsupported, malformed or unavailable observation is not
                // retirement. Only normal unexpired ACTIVE validation can install AK.
            }
            install_flow = Some(self.enrollment_lock.lock().await);
            epoch.check(self)?;
            let record = Record::decode(&blob, &epoch)?;
            self.install_enrollment(&record, &epoch, store, &mut permit)
                .await?;
            record.status.ok_or_else(invalid)
        }
        .await;
        let result = permit.finish(result).await;
        drop(install_flow);
        result
    }

    async fn install_enrollment(
        &self,
        record: &Record,
        epoch: &EnrollmentEpoch,
        store: Arc<dyn SecureEnrollmentStore>,
        permit: &mut RestorePermit,
    ) -> Result<(), TinkleError> {
        epoch.check(self)?;
        let encoded = record.encode()?;
        let snapshot = tokio::time::timeout(Duration::from_secs(10), store.namespace_snapshot())
            .await
            .map_err(|_| storage_error())?
            .map_err(|_| storage_error())?;
        match snapshot {
            Some(namespace) => {
                if namespace.cleanup_pending
                    || !self.authorization.matches_namespace(
                        store.as_ref(),
                        &namespace.incarnation,
                        &namespace.revision,
                    )
                    || namespace
                        .active
                        .as_ref()
                        .is_none_or(|active| active.expose_secret() != encoded.expose_secret())
                {
                    return Err(storage_error());
                }
                permit.acquire(&namespace, &encoded).await?;
            }
            None => {
                if self.authorization.store_revision(store.as_ref()).is_some() {
                    return Err(storage_error());
                }
                // Legacy hosts require exclusive namespace ownership themselves;
                // this reread supplies identity checking, not cross-process exclusion.
                let current = tokio::time::timeout(Duration::from_secs(10), store.load_active())
                    .await
                    .map_err(|_| storage_error())?
                    .map_err(|_| storage_error())?;
                if current
                    .as_ref()
                    .is_none_or(|active| active.expose_secret() != encoded.expose_secret())
                {
                    return Err(storage_error());
                }
            }
        }
        epoch.check(self)?;
        let source = record.returning()?;
        // Retire Bootstrap before allowing a Returning transport; never promote
        // the principal on the provisioning connection, even transiently.
        self.install_active_source(
            source,
            record.status.as_ref().ok_or_else(invalid)?.user_id,
            epoch,
            Some((record.status.as_ref().ok_or_else(invalid)?.clone(), store)),
            permit,
        )
        .await
    }

    pub(crate) async fn provisioning_client(
        &self,
        trust: &ServerTrust,
        epoch: &EnrollmentEpoch,
    ) -> Result<Client, TinkleError> {
        epoch.check(self)?;
        let endpoint_revision = self.retirement.endpoint_revision();
        if self
            .endpoint_rejected
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(TinkleError::TrustRejected);
        }
        if *trust != self.enrollment_trust().await {
            return Err(invalid());
        }
        let mut client = Client::new(
            self.config().clone(),
            TrustSource::Bootstrap(trust.clone()),
            crate::InMemoryCursorStore::new(),
        );
        client.endpoint_rejected = self.endpoint_rejected.clone();
        client.connect_lock = self.connect_lock.clone();
        client.provisioning_context = Some((self.retirement.clone(), endpoint_revision));
        let revoked = epoch.revoked();
        tokio::select! { biased;
            _ = revoked.cancelled() => return Err(TinkleError::SignedOut),
            result = client.connect() => result?,
        }
        epoch.check(self)?;
        if self
            .endpoint_rejected
            .load(std::sync::atomic::Ordering::SeqCst)
            || *trust != self.enrollment_trust().await
            || endpoint_revision != self.retirement.endpoint_revision()
        {
            client.close().await;
            return Err(TinkleError::TrustRejected);
        }
        Ok(client)
    }

    pub(crate) async fn persist_delivery(
        &self,
        operation: &DurableOperation,
        delivery: proto::DeviceEnrollment,
        active_allowed: bool,
    ) -> Result<PersistedEnrollment, TinkleError> {
        let record = Record::delivery(self, operation, delivery, active_allowed)?;
        if record.trust != self.enrollment_trust().await {
            return Err(invalid());
        }
        commit(
            self,
            &operation.epoch,
            operation.store.as_ref(),
            SecureWrite::Candidate,
            &record,
        )
        .await?;
        Ok(PersistedEnrollment {
            record,
            store: operation.store.clone(),
            epoch: operation.epoch.duplicate(),
        })
    }

    pub(crate) async fn persist_rotation(
        &self,
        operation: &DurableOperation,
        delivery: proto::DeviceEnrollment,
    ) -> Result<PersistedEnrollment, TinkleError> {
        let record = Record::delivery(self, operation, delivery, false)?;
        let previous = tokio::time::timeout(Duration::from_secs(10), operation.store.load_active())
            .await
            .map_err(|_| storage_error())?
            .map_err(|_| storage_error())?
            .ok_or_else(storage_error)?;
        operation.epoch.check(self)?;
        let previous = Record::decode(&previous, &operation.epoch)?;
        let old = previous.status.as_ref().ok_or_else(invalid)?;
        let proposed = record.status.as_ref().ok_or_else(invalid)?;
        let source = self.trust_source.read().await;
        let binding = match &*source {
            TrustSource::Returning { enrollment, .. } => Some(enrollment.binding()),
            _ => None,
        };
        if previous.trust != record.trust
            || record.trust != self.enrollment_trust_unlocked(&source)
            || old.state != DeviceEnrollmentState::Active as i32
            || binding.is_none_or(|(handle, epoch)| {
                handle.as_slice() != old.handle.as_ref() || epoch != old.enrollment_epoch
            })
            || proposed.session_id != old.session_id
            || proposed.user_id != old.user_id
            || proposed.account_auth_epoch != old.account_auth_epoch
            || proposed.enrollment_epoch <= old.enrollment_epoch
        {
            return Err(invalid());
        }
        drop(source);
        commit(
            self,
            &operation.epoch,
            operation.store.as_ref(),
            SecureWrite::Candidate,
            &record,
        )
        .await?;
        Ok(PersistedEnrollment {
            record,
            store: operation.store.clone(),
            epoch: operation.epoch.duplicate(),
        })
    }

    fn enrollment_trust_unlocked(&self, source: &TrustSource) -> ServerTrust {
        match source {
            TrustSource::Bootstrap(trust)
            | TrustSource::SignedOut(trust)
            | TrustSource::Returning { trust, .. } => trust.clone(),
        }
    }

    pub(crate) async fn activate_persisted(
        &self,
        persisted: &PersistedEnrollment,
    ) -> Result<DeviceEnrollmentStatus, TinkleError> {
        let record = &persisted.record;
        persisted.epoch.check(self)?;
        let proposed = record.status.as_ref().ok_or_else(invalid)?;
        validate_status(proposed)?;
        let client = self
            .provisioning_client(&record.trust, &persisted.epoch)
            .await?;
        let request = proto::ActivateEnrollmentRequest {
            operation_id: record.operation_id.clone(),
            session_id: proposed.session_id.clone(),
            handle: proposed.handle.clone(),
            enrollment_epoch: proposed.enrollment_epoch,
            scope: proposed.scope,
            resume_secret: record.capability.snapshot()?.as_slice().to_vec().into(),
        };
        let result: Result<SecretReply<proto::ActivateEnrollmentResponse>, TinkleError> =
            secret_call(
                &client,
                "/tinkle.v1.AuthService/ActivateEnrollment",
                request,
                &persisted.epoch,
            )
            .await;
        client.close().await;
        let status = result?.enrollment.take().ok_or_else(invalid)?;
        persisted.epoch.check(self)?;
        validate_status(&status)?;
        let mut expected = proposed.clone();
        expected.state = DeviceEnrollmentState::Active as i32;
        if status != expected {
            return Err(invalid());
        }
        let active = Record {
            operation_id: record.operation_id.clone(),
            capability: record.capability.clone(),
            trust: record.trust.clone(),
            status: Some(status.clone()),
            key: record.key.clone(),
        };
        self.retire_for_activation(&record.trust, &persisted.epoch)
            .await?;
        commit(
            self,
            &persisted.epoch,
            persisted.store.as_ref(),
            SecureWrite::Active,
            &active,
        )
        .await?;
        let mut permit = RestorePermit::new(self, persisted.store.clone(), None);
        let installed = self
            .install_enrollment(
                &active,
                &persisted.epoch,
                persisted.store.clone(),
                &mut permit,
            )
            .await;
        permit.finish(installed).await?;
        Ok(status)
    }
}

/// Private request cleanup implemented for each secret-bearing generated request.
pub(crate) trait SecretRequest: Message {
    fn wipe(&mut self);
}
impl SecretRequest for proto::SignInRequest {
    fn wipe(&mut self) {
        self.phone_number.zeroize();
        self.code.zeroize();
        self.verification_id.zeroize();
        wipe_bytes(std::mem::take(&mut self.resume_secret));
    }
}
macro_rules! secret_request {
    ($($ty:ty),+ $(,)?) => {$ (impl SecretRequest for $ty { fn wipe(&mut self) { wipe_bytes(std::mem::take(&mut self.resume_secret)); } })+};
}
secret_request!(
    proto::ResumeEnrollmentRequest,
    proto::RotateEnrollmentRequest,
    proto::ActivateEnrollmentRequest
);

pub(crate) trait SecretResponse: Message + Default {
    fn wipe(&mut self);
}
macro_rules! secret_response {
    ($($ty:ty),+ $(,)?) => {$ (impl SecretResponse for $ty { fn wipe(&mut self) {
        if let Some(enrollment) = &mut self.enrollment { wipe_bytes(std::mem::take(&mut enrollment.authorization_key)); }
    } })+};
}
secret_response!(
    proto::SignInResponse,
    proto::ResumeEnrollmentResponse,
    proto::RotateEnrollmentResponse
);
impl SecretResponse for proto::ActivateEnrollmentResponse {
    fn wipe(&mut self) {}
}

/// Erase decoded secret-bearing fields even when a reply is cancelled or
/// rejected by the generation fence before the coordinator adopts delivery.
pub(crate) struct SecretReply<T: SecretResponse>(T);
impl<T: SecretResponse> std::ops::Deref for SecretReply<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}
impl<T: SecretResponse> std::ops::DerefMut for SecretReply<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}
impl<T: SecretResponse> Drop for SecretReply<T> {
    fn drop(&mut self) {
        self.0.wipe();
    }
}

pub(crate) async fn secret_call<Req: SecretRequest, Resp: SecretResponse>(
    client: &Client,
    method: &str,
    mut request: Req,
    epoch: &EnrollmentEpoch,
) -> Result<SecretReply<Resp>, TinkleError> {
    let bytes = Zeroizing::new(request.encode_to_vec());
    request.wipe();
    epoch.valid()?;
    let revoked = epoch.revoked();
    let response = Zeroizing::new(tokio::select! { biased;
        _ = revoked.cancelled() => return Err(TinkleError::SignedOut),
        result = async { client.call_raw(method, bytes.to_vec(), 30_000).await } => result.map_err(sanitize_auth_error)?,
    });
    epoch.valid()?;
    let mut decoded = SecretReply(Resp::default());
    if decoded.0.merge(response.as_slice()).is_err() {
        return Err(TinkleError::Codec("invalid enrollment response".into()));
    }
    epoch.valid()?;
    Ok(decoded)
}

/// Enrollment peers may return untrusted diagnostic text. Preserve code/outcome
/// classification while discarding strings that could reflect sensitive inputs.
pub(crate) fn sanitize_auth_error(error: TinkleError) -> TinkleError {
    match error {
        TinkleError::Syntra(syntra_core::SyntraError::Rpc { code, raw_code, .. }) => {
            TinkleError::Syntra(syntra_core::SyntraError::Rpc {
                code,
                raw_code,
                message: "enrollment request rejected".into(),
            })
        }
        other => other,
    }
}

pub(crate) fn resume_request(
    operation: &DurableOperation,
) -> Result<proto::ResumeEnrollmentRequest, TinkleError> {
    Ok(proto::ResumeEnrollmentRequest {
        operation_id: operation.operation_id().into(),
        resume_secret: operation
            .record
            .capability
            .snapshot()?
            .as_slice()
            .to_vec()
            .into(),
    })
}
pub(crate) fn rotate_request(
    operation: &DurableOperation,
) -> Result<proto::RotateEnrollmentRequest, TinkleError> {
    Ok(proto::RotateEnrollmentRequest {
        operation_id: operation.operation_id().into(),
        resume_secret: operation
            .record
            .capability
            .snapshot()?
            .as_slice()
            .to_vec()
            .into(),
    })
}
pub(crate) fn operation_trust(operation: &DurableOperation) -> &ServerTrust {
    &operation.record.trust
}
