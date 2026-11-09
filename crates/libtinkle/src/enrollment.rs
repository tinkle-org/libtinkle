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
    /// Format the owner without revealing protected bytes.
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
    /// A caller timeout/cancellation does not cancel the retained transaction:
    /// its exact receipt is required to fence later cleanup. Never return another
    /// writer's revision as this operation's acknowledgement.
    /// Until that receipt arrives, conditional cleanup fails closed with a
    /// retryable storage error; a permanently stalled transaction needs host recovery.
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

/// Specific host install permission; cancellation retains a retrying release owner.
pub(crate) struct RestorePermit {
    store: Arc<dyn SecureEnrollmentStore>,
    id: Option<String>,
    client: Client,
    runtime: Option<tokio::runtime::Handle>,
    pub(crate) binding: Option<Arc<crate::retirement::RetainedEnrollment>>,
    pub(crate) installed: Option<(Arc<SecretCell>, DeviceEnrollmentStatus)>,
}

impl RestorePermit {
    /// Capture cleanup ownership and the current runtime before acquiring a permit.
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

    /// Withdraw only this failed restore's binding before releasing host exclusion.
    async fn clear_binding(&mut self) {
        if let Some(binding) = &self.binding {
            self.client
                .clear_failed_restore(binding, self.installed.as_ref())
                .await;
            self.binding = None;
        }
    }

    /// Release only after a failed install has withdrawn its exact runtime binding.
    async fn finish<T: Send + 'static>(
        &mut self,
        result: Result<T, TinkleError>,
    ) -> Result<T, TinkleError> {
        // Finalization is a commit point: after transfer, cancelling the waiter
        // cannot race a successful release with a second withdrawal owner.
        let mut owner = Self {
            store: self.store.clone(),
            id: self.id.take(),
            client: self.client.background_clone(),
            runtime: self.runtime.clone(),
            binding: self.binding.take(),
            installed: self.installed.take(),
        };
        tokio::spawn(async move { owner.finish_owned(result).await })
            .await
            .map_err(|_| storage_error())?
    }

    /// Finalize under one owner, withdrawing failures and preserving release errors.
    async fn finish_owned<T>(&mut self, result: Result<T, TinkleError>) -> Result<T, TinkleError> {
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
    /// Schedule withdrawal and release retries, or retire the secret at terminal shutdown.
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            // Runtime shutdown can drop the cleanup owner before withdrawal or
            // release completes. End here; host exclusion remains fail-closed.
            if let Some((secret, _)) = &self.installed {
                secret.retire();
            }
            return;
        };
        let binding = self.binding.take();
        let id = self.id.take();
        if binding.is_none() && id.is_none() {
            return;
        }
        let mut owner = Self {
            store: self.store.clone(),
            id,
            client: self.client.background_clone(),
            runtime: None,
            binding,
            installed: self.installed.take(),
        };
        runtime.spawn(async move {
            owner.clear_binding().await;
            // Each callback is bounded, but a failed release must retain its
            // exact ID and exclusion until the idempotent retry succeeds.
            while owner.release().await.is_err() {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
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
    /// Adopt bounded, nonempty phone, code and verification inputs without logging them.
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
    /// Build a SignIn request using this durable operation's current capability.
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
    /// Redact all sign-in inputs from diagnostic formatting.
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
    /// Return the public UUID used to recover this operation from protected storage.
    pub fn operation_id(&self) -> &str {
        &self.record.operation_id
    }
}
impl fmt::Debug for DurableOperation {
    /// Redact the operation's capability and stored record.
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
    /// Borrow the validated candidate status without granting Returning authority.
    pub fn status(&self) -> &DeviceEnrollmentStatus {
        self.record.status.as_ref().expect("validated candidate")
    }
}
impl fmt::Debug for PersistedEnrollment {
    /// Redact the persisted candidate's key and capability.
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
    /// Redact the sign-in projection and enrollment from diagnostics.
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

/// Return a neutral binding/state error without including protected inputs.
fn invalid() -> TinkleError {
    TinkleError::Codec("invalid enrollment binding or state".into())
}
/// Map host failure or ambiguous acknowledgement to the structured storage error.
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

/// Generate a fresh UUID and revocable capability bound to provisioned trust.
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

/// Require valid metadata and a strictly future expiry before adopting credentials.
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
        || status.account_auth_epoch == 0
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
    /// Validate and adopt one delivery under the operation's trust and authority fence.
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

    /// Encode the complete protected record while keeping temporary bytes zeroizing.
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

    /// Validate stored freshness and register decoded secrets under the current epoch.
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

    /// Construct Returning material only from a live, unexpired ACTIVE record.
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
        /// Consume exactly one bounded field, rejecting truncated records.
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

/// Persist independently of the waiter, attributing conditional revisions only by receipt.
async fn commit(
    client: &Client,
    epoch: &EnrollmentEpoch,
    store: &Arc<dyn SecureEnrollmentStore>,
    kind: SecureWrite,
    record: &Record,
) -> Result<(), TinkleError> {
    epoch.check(client)?;
    // A pending operation or ambiguous activation must fence predecessor evidence.
    client.invalidate_retirement_binding().await;
    let encoded = record.encode()?;
    let revision = client.authorization.store_revision(store.as_ref());
    // Only conditional writes need an exact revision receipt before cleanup.
    // Legacy clear must dominate old writes even when their callbacks stall.
    let writing = revision
        .as_ref()
        .map(|_| client.authorization.begin_store_write(store.as_ref()))
        .transpose()?;
    let store = store.clone();
    let owner = client.background_clone();
    let operation_id = record.operation_id.clone();
    // Do not drop the host future on waiter timeout/cancellation: only its exact
    // receipt can attribute the resulting revision. Snapshot freshness cannot.
    let task = tokio::spawn(async move {
        let _writing = writing;
        if let Some(previous) = revision.as_deref() {
            let next = store
                .commit_current(previous, kind, &operation_id, &encoded)
                .await
                .map_err(|_| storage_error())?
                .ok_or_else(storage_error)?;
            if next.is_empty() || next.len() > 256 || next == previous {
                return Err(storage_error());
            }
            // A completed pre-fence transaction must update cleanup's expected
            // revision even when sign-out occurred while the host was writing.
            owner.authorization.update_store_revision(
                store.as_ref(),
                revision.as_deref(),
                Some(next),
            )?;
        } else {
            store
                .commit(kind, &operation_id, &encoded)
                .await
                .map_err(|_| storage_error())?;
        }
        Ok::<_, TinkleError>(())
    });
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .map_err(|_| storage_error())?
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

    /// Snapshot provisioned trust without exposing any enrollment key.
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
        commit(self, &epoch, &store, SecureWrite::Operation, &record).await?;
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
            if conditional && validate_status(&status).is_err() {
                let _source = self.connect_lock.lock().await;
                if let Some(binding) = permit.binding.take() {
                    self.retirement.retain_negative_only(&binding);
                }
                return Err(invalid());
            }
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

    /// Recheck the stored ACTIVE slot and acquire host exclusion before installation.
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

    /// Establish pinned Bootstrap work under the parent's route, endpoint and logout fences.
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

    /// Acknowledge a candidate only after validating its trust and protected write.
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
            &operation.store,
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

    /// Persist a staged successor only for the current session, user and increasing epoch.
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
            &operation.store,
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

    /// Copy trust from an already-locked source without recursively acquiring its lock.
    fn enrollment_trust_unlocked(&self, source: &TrustSource) -> ServerTrust {
        match source {
            TrustSource::Bootstrap(trust)
            | TrustSource::SignedOut(trust)
            | TrustSource::Returning { trust, .. } => trust.clone(),
        }
    }

    /// Verify activation over Bootstrap, persist ACTIVE, then install under a restore permit.
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
            &persisted.store,
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
    /// Erase request-owned credentials once the encoded buffer owns the outbound copy.
    fn wipe(&mut self);
}
impl SecretRequest for proto::SignInRequest {
    /// Erase phone verification inputs and the operation capability.
    fn wipe(&mut self) {
        self.phone_number.zeroize();
        self.code.zeroize();
        self.verification_id.zeroize();
        wipe_bytes(std::mem::take(&mut self.resume_secret));
    }
}
macro_rules! secret_request {
    ($($ty:ty),+ $(,)?) => {$ (impl SecretRequest for $ty {
        /// Erase this request's operation capability.
        fn wipe(&mut self) { wipe_bytes(std::mem::take(&mut self.resume_secret)); }
    })+};
}
secret_request!(
    proto::ResumeEnrollmentRequest,
    proto::RotateEnrollmentRequest,
    proto::ActivateEnrollmentRequest
);

pub(crate) trait SecretResponse: Message + Default {
    /// Erase decoded response secrets before their temporary owner is dropped.
    fn wipe(&mut self);
}
macro_rules! secret_response {
    ($($ty:ty),+ $(,)?) => {$ (impl SecretResponse for $ty {
        /// Erase a delivered authorization key, including partially decoded responses.
        fn wipe(&mut self) {
        if let Some(enrollment) = &mut self.enrollment { wipe_bytes(std::mem::take(&mut enrollment.authorization_key)); }
    } })+};
}
secret_response!(
    proto::SignInResponse,
    proto::ResumeEnrollmentResponse,
    proto::RotateEnrollmentResponse
);
impl SecretResponse for proto::ActivateEnrollmentResponse {
    /// Activation returns only status metadata and owns no key bytes to erase.
    fn wipe(&mut self) {}
}

/// Erase decoded secret-bearing fields even when a reply is cancelled or
/// rejected by the generation fence before the coordinator adopts delivery.
pub(crate) struct SecretReply<T: SecretResponse>(T);
impl<T: SecretResponse> std::ops::Deref for SecretReply<T> {
    type Target = T;
    /// Borrow the response while its secret-erasing owner remains alive.
    fn deref(&self) -> &T {
        &self.0
    }
}
impl<T: SecretResponse> std::ops::DerefMut for SecretReply<T> {
    /// Mutably borrow the response without transferring secret cleanup ownership.
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}
impl<T: SecretResponse> Drop for SecretReply<T> {
    /// Erase response secrets on every exit, including decode failure and cancellation.
    fn drop(&mut self) {
        self.0.wipe();
    }
}

/// Execute one enrollment RPC with zeroizing buffers and pre/post authority checks.
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

/// Build a recovery request from the operation's still-live capability.
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
/// Build a rotation request without exposing the capability outside the RPC owner.
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
/// Borrow the provisioned trust captured when this durable operation was created.
pub(crate) fn operation_trust(operation: &DurableOperation) -> &ServerTrust {
    &operation.record.trust
}

#[cfg(test)]
pub(crate) mod review_tests {
    use super::*;
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Mutex,
        },
    };

    #[derive(Default)]
    pub(crate) struct ReviewStore {
        pub(crate) revision: Mutex<u64>,
        pub(crate) tombstone: AtomicBool,
        active: Mutex<Option<Vec<u8>>>,
        operations: Mutex<HashMap<String, Vec<u8>>>,
        pub(crate) clears: AtomicUsize,
        pub(crate) snapshots: AtomicUsize,
        hold_commit: AtomicBool,
        commit_entered: tokio::sync::Notify,
        commit_released: tokio::sync::Notify,
        hold_end: AtomicBool,
        end_entered: tokio::sync::Notify,
        end_released: tokio::sync::Notify,
        end_failures: AtomicUsize,
        end_ids: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl SecureEnrollmentStore for ReviewStore {
        /// Return the fixture's current conditional namespace and opaque ACTIVE bytes.
        async fn namespace_snapshot(
            &self,
        ) -> Result<Option<EnrollmentNamespaceSnapshot>, SecureStoreError> {
            self.snapshots.fetch_add(1, Ordering::SeqCst);
            Ok(Some(EnrollmentNamespaceSnapshot {
                incarnation: "review-namespace".into(),
                revision: self.revision.lock().unwrap().to_string(),
                cleanup_pending: self.tombstone.load(Ordering::SeqCst),
                active: self
                    .active
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|bytes| SecretBlob::new(bytes.clone())),
                has_pending: false,
            }))
        }
        /// Advance a matching revision before optionally withholding its acknowledgement.
        async fn commit_current(
            &self,
            expected: &str,
            _: SecureWrite,
            id: &str,
            record: &SecretBlob,
        ) -> Result<Option<String>, SecureStoreError> {
            let revision = {
                let mut revision = self.revision.lock().unwrap();
                if expected != revision.to_string() || self.tombstone.load(Ordering::SeqCst) {
                    return Ok(None);
                }
                self.operations
                    .lock()
                    .unwrap()
                    .insert(id.into(), record.expose_secret().to_vec());
                *revision += 1;
                revision.to_string()
            };
            self.commit_entered.notify_one();
            if self.hold_commit.load(Ordering::SeqCst) {
                self.commit_released.notified().await;
            }
            Ok(Some(revision))
        }
        /// Record the exact release ID and apply configured callback stalls or failures.
        async fn end_restore(&self, id: &str) -> Result<(), SecureStoreError> {
            self.end_ids.lock().unwrap().push(id.into());
            self.end_entered.notify_one();
            if self.hold_end.load(Ordering::SeqCst) {
                self.end_released.notified().await;
            }
            let mut failures = self.end_failures.load(Ordering::SeqCst);
            while failures > 0 {
                match self.end_failures.compare_exchange(
                    failures,
                    failures - 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => return Err(SecureStoreError),
                    Err(current) => failures = current,
                }
            }
            Ok(())
        }
        /// Tombstone only the fixture revision this coordinator is authorized to clear.
        async fn begin_clear(&self, expected: &str) -> Result<bool, SecureStoreError> {
            if expected != self.revision.lock().unwrap().to_string() {
                return Ok(false);
            }
            self.tombstone.store(true, Ordering::SeqCst);
            Ok(true)
        }
        /// Erase fixture records only under the matching retained tombstone.
        async fn finish_clear(&self, expected: &str) -> Result<bool, SecureStoreError> {
            if expected != self.revision.lock().unwrap().to_string()
                || !self.tombstone.load(Ordering::SeqCst)
            {
                return Ok(false);
            }
            self.clears.fetch_add(1, Ordering::SeqCst);
            self.operations.lock().unwrap().clear();
            self.active.lock().unwrap().take();
            Ok(true)
        }
        /// Reject an unconditional clear on a conditional-only fixture.
        async fn clear(&self) -> Result<(), SecureStoreError> {
            panic!("conditional only")
        }
        /// Reject a legacy write on a conditional-only fixture.
        async fn commit(
            &self,
            _: SecureWrite,
            _: &str,
            _: &SecretBlob,
        ) -> Result<(), SecureStoreError> {
            panic!("conditional only")
        }
        /// Return an owned copy of one fixture operation's protected bytes.
        async fn load_operation(&self, id: &str) -> Result<Option<SecretBlob>, SecureStoreError> {
            Ok(self
                .operations
                .lock()
                .unwrap()
                .get(id)
                .map(|bytes| SecretBlob::new(bytes.clone())))
        }
        /// Return an owned copy of the fixture's ACTIVE slot, if present.
        async fn load_active(&self) -> Result<Option<SecretBlob>, SecureStoreError> {
            Ok(self
                .active
                .lock()
                .unwrap()
                .as_ref()
                .map(|bytes| SecretBlob::new(bytes.clone())))
        }
    }

    /// Build a loopback-only client with synthetic Bootstrap or Returning material.
    pub(crate) fn client(returning: bool) -> Client {
        let trust = ServerTrust {
            server_public_key: [1; 32],
            server_key_id: 7,
            scope: "review".into(),
        };
        let source = if returning {
            TrustSource::Returning {
                trust,
                enrollment: Enrollment::new([9; 32], [8; 8], 1).unwrap(),
            }
        } else {
            TrustSource::Bootstrap(trust)
        };
        let mut config = crate::ClientConfig::new(
            "127.0.0.1",
            crate::ClientInfo::new(
                "review".into(),
                "test".into(),
                "1".into(),
                "1".into(),
                "1".into(),
                Default::default(),
            )
            .unwrap(),
        );
        config.primary_port = 0;
        config.fallback_port = None;
        Client::new(config, source, crate::InMemoryCursorStore::new())
    }

    /// Build an expired ACTIVE record for structural observation and cleanup tests.
    fn active(client: &Client, epoch: &EnrollmentEpoch) -> Record {
        let mut record = fresh_operation(
            ServerTrust {
                server_public_key: [1; 32],
                server_key_id: 7,
                scope: "review".into(),
            },
            epoch,
        )
        .unwrap();
        record.status = Some(DeviceEnrollmentStatus {
            operation_id: record.operation_id.clone(),
            session_id: "12345678-1234-4234-8234-123456789abc".into(),
            handle: vec![8; 8].into(),
            enrollment_epoch: 1,
            account_auth_epoch: 1,
            user_id: 42,
            scope: DeviceKeyScope::Account as i32,
            state: DeviceEnrollmentState::Active as i32,
            expires_at: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
        });
        record.key = Some(epoch.secret(Zeroizing::new([9; 32])).unwrap());
        epoch.check(client).unwrap();
        record
    }

    /// Validate the shared ACTIVE bytes and their pin against the deterministic SDK peer.
    #[test]
    fn review_swift_active_vector_matches_private_record_encoder_and_validates_returning() {
        use syntra_core::secure_channel::{
            self, Carrier, ClientHandshake, Mode, Secret32, ServerConfig,
        };
        let client = client(false);
        let epoch = client.authorization.generation().unwrap();
        let trust = ServerTrust {
            server_public_key: [
                0x7b, 0x0d, 0x47, 0xd9, 0x34, 0x27, 0xf8, 0x31, 0x11, 0x60, 0x78, 0x1c, 0x7c, 0x73,
                0x3f, 0xd8, 0x9f, 0x88, 0x97, 0x0a, 0xef, 0x49, 0x0d, 0x8a, 0xa0, 0xee, 0x19, 0xa4,
                0xcb, 0x8a, 0x1b, 0x14,
            ],
            server_key_id: 7,
            scope: "fixture".into(),
        };
        // rc.4 exposes no fixed-private-key derivation helper; prove the pin by
        // authenticating an actual SDK Bootstrap exchange with that private key.
        let (handshake, first) =
            ClientHandshake::start(TrustSource::Bootstrap(trust.clone()).security().unwrap())
                .unwrap();
        let (mut server, second) = secure_channel::respond(
            &ServerConfig {
                mode: Mode::Bootstrap,
                carrier: Carrier::Tcp,
                server_key_id: trust.server_key_id,
                scope: trust.scope.as_bytes().to_vec(),
                private_key: Secret32::new([0x33; 32]),
                store: None,
            },
            &first,
        )
        .unwrap();
        let mut channel = handshake.finish(&second).unwrap();
        let encrypted = channel.encrypt(b"enrollment-review-pin").unwrap();
        assert_eq!(
            server.decrypt(&encrypted).unwrap().as_slice(),
            b"enrollment-review-pin"
        );
        assert!(server.verified_identity().is_some());
        let status = DeviceEnrollmentStatus {
            operation_id: "11111111-1111-4111-8111-111111111111".into(),
            session_id: "22222222-2222-4222-8222-222222222222".into(),
            handle: vec![8; 8].into(),
            enrollment_epoch: 1,
            scope: DeviceKeyScope::Account as i32,
            expires_at: Some(prost_types::Timestamp {
                seconds: 253_402_300_799,
                nanos: 0,
            }),
            state: DeviceEnrollmentState::Active as i32,
            user_id: 42,
            account_auth_epoch: 1,
        };
        let record = Record {
            operation_id: status.operation_id.clone(),
            capability: epoch.secret(Zeroizing::new([0x11; 32])).unwrap(),
            trust,
            status: Some(status.clone()),
            key: Some(epoch.secret(Zeroizing::new([0x22; 32])).unwrap()),
        };
        let blob = record.encode().unwrap();
        let expected: String = blob
            .expose_secret()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        // Shared as Swift source so consumers need no resource-bundle or API change.
        let asset = include_str!(
            "../../../packages/swift/Tests/TinkleSDKTests/EnrollmentReviewVector.swift"
        );
        let actual = asset
            .split("static let activeRecordHex = \"")
            .nth(1)
            .unwrap()
            .split('"')
            .next()
            .unwrap();
        assert_eq!(actual, expected);
        let restored = Record::decode(&blob, &epoch).unwrap();
        assert_eq!(restored.status.as_ref(), Some(&status));
        assert!(matches!(
            restored.returning().unwrap(),
            TrustSource::Returning { .. }
        ));
    }

    /// Seed conditional storage with expired ACTIVE bytes from the private encoder.
    pub(crate) fn expired_store(client: &Client) -> Arc<ReviewStore> {
        let store = Arc::new(ReviewStore::default());
        let record = active(client, &client.authorization.generation().unwrap());
        *store.active.lock().unwrap() = Some(record.encode().unwrap().expose_secret().to_vec());
        store
    }

    /// Seed one exact source/secret binding and its permit without dialing a peer.
    async fn installed_permit(client: &Client, store: Arc<ReviewStore>) -> RestorePermit {
        let record = active(client, &client.authorization.generation().unwrap());
        let enrollment = Enrollment::new([9; 32], [8; 8], 1).unwrap();
        let secret = enrollment.secret();
        *client.trust_source.write().await = TrustSource::Returning {
            trust: record.trust.clone(),
            enrollment,
        };
        let binding = client.retirement.bind(
            record.trust,
            record.status.as_ref().unwrap(),
            store.clone(),
            false,
            true,
        );
        let mut permit = RestorePermit::new(client, store, Some(binding));
        permit.installed = Some((secret, record.status.unwrap()));
        permit.id = Some("review-permit".into());
        permit
    }

    /// Prove timeout and cancellation retain exact conditional receipts for logout retry.
    #[tokio::test(start_paused = true)]
    async fn review_late_commit_receipt_survives_timeout_and_cancellation() {
        for cancel in [false, true] {
            let client = client(false);
            let store = Arc::new(ReviewStore::default());
            store.hold_commit.store(true, Ordering::SeqCst);
            let caller = client.clone();
            let host = store.clone();
            let task = tokio::spawn(async move { caller.prepare_enrollment(host).await });
            store.commit_entered.notified().await;
            if cancel {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                tokio::time::advance(Duration::from_secs(10)).await;
                assert!(matches!(task.await.unwrap(), Err(TinkleError::SecureStore)));
            }
            assert_eq!(
                client
                    .authorization
                    .store_revision(store.as_ref())
                    .as_deref(),
                Some("0")
            );
            assert!(client.logout().await.is_err());
            assert_eq!(store.clears.load(Ordering::SeqCst), 0);
            store.commit_released.notify_one();
            while client.authorization.has_pending_store_write() {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                client
                    .authorization
                    .store_revision(store.as_ref())
                    .as_deref(),
                Some("1")
            );
            client.logout().await.unwrap();
            assert_eq!(store.clears.load(Ordering::SeqCst), 1);
        }
    }

    #[derive(Default)]
    struct StalledLegacyStore {
        // A successful legacy clear must dominate an earlier, still-pending write.
        state: Mutex<(bool, Option<Vec<u8>>)>,
        entered: tokio::sync::Notify,
        resume: tokio::sync::Notify,
        finished: tokio::sync::Notify,
        clears: AtomicUsize,
    }

    #[async_trait]
    impl SecureEnrollmentStore for StalledLegacyStore {
        /// Durably fence the fixture namespace against every pending legacy write.
        async fn clear(&self) -> Result<(), SecureStoreError> {
            *self.state.lock().unwrap() = (true, None);
            self.clears.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        /// Stall publication, then reject it if an acknowledged clear won the race.
        async fn commit(
            &self,
            _: SecureWrite,
            _: &str,
            record: &SecretBlob,
        ) -> Result<(), SecureStoreError> {
            self.entered.notify_one();
            self.resume.notified().await;
            let result = {
                let mut state = self.state.lock().unwrap();
                if state.0 {
                    Err(SecureStoreError)
                } else {
                    state.1 = Some(record.expose_secret().to_vec());
                    Ok(())
                }
            };
            self.finished.notify_one();
            result
        }
        /// Read the sole legacy fixture record without changing its clear fence.
        async fn load_operation(&self, _: &str) -> Result<Option<SecretBlob>, SecureStoreError> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .1
                .as_ref()
                .map(|bytes| SecretBlob::new(bytes.clone())))
        }
        /// Report that this operation-only fixture has no ACTIVE material.
        async fn load_active(&self) -> Result<Option<SecretBlob>, SecureStoreError> {
            Ok(None)
        }
    }

    /// Prove legacy clear progresses despite a stalled write and dominates late publication.
    #[tokio::test(start_paused = true)]
    async fn review_stalled_legacy_commit_does_not_block_logout_or_revive_after_clear() {
        for cancel in [false, true] {
            let client = client(false);
            let store = Arc::new(StalledLegacyStore::default());
            let caller = client.clone();
            let host = store.clone();
            let prepare = tokio::spawn(async move { caller.prepare_enrollment(host).await });
            store.entered.notified().await;
            if cancel {
                prepare.abort();
                assert!(prepare.await.unwrap_err().is_cancelled());
            } else {
                tokio::time::advance(Duration::from_secs(10)).await;
                assert!(matches!(
                    prepare.await.unwrap(),
                    Err(TinkleError::SecureStore)
                ));
            }
            assert!(
                !tokio::time::timeout(Duration::from_secs(1), client.logout())
                    .await
                    .unwrap()
                    .unwrap()
            );
            assert_eq!(store.clears.load(Ordering::SeqCst), 1);
            assert!(!client.authorization.has_pending_store_write());
            let status = client.session_status().await;
            assert_eq!(status.cleanup, crate::SessionCleanup::Complete);
            assert!(!status.restart_safe_store);
            store.resume.notify_one();
            store.finished.notified().await;
            let state = store.state.lock().unwrap();
            assert!(state.0);
            assert!(
                state.1.is_none(),
                "late legacy writes must respect durable clear"
            );
        }
    }

    /// Prove an exact late receipt cannot authorize clearing a newer host revision.
    #[tokio::test]
    async fn review_late_receipt_never_adopts_another_coordinators_revision() {
        let client = client(false);
        let store = Arc::new(ReviewStore::default());
        store.hold_commit.store(true, Ordering::SeqCst);
        let caller = client.clone();
        let host = store.clone();
        let task = tokio::spawn(async move { caller.prepare_enrollment(host).await });
        store.commit_entered.notified().await;
        task.abort();
        let _ = task.await;
        *store.revision.lock().unwrap() = 2;
        store.commit_released.notify_one();
        while client.authorization.has_pending_store_write() {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            client
                .authorization
                .store_revision(store.as_ref())
                .as_deref(),
            Some("1")
        );
        assert!(client.logout().await.is_err());
        assert_eq!(store.clears.load(Ordering::SeqCst), 0);
    }

    /// Prove a late receipt cannot replace a locally advanced revision expectation.
    #[tokio::test]
    async fn review_late_receipt_cannot_overwrite_a_changed_local_expected_revision() {
        let client = client(false);
        let store = Arc::new(ReviewStore::default());
        store.hold_commit.store(true, Ordering::SeqCst);
        let caller = client.clone();
        let host = store.clone();
        let task = tokio::spawn(async move { caller.prepare_enrollment(host).await });
        store.commit_entered.notified().await;
        client
            .authorization
            .update_store_revision(store.as_ref(), Some("0"), Some("local-successor".into()))
            .unwrap();
        store.commit_released.notify_one();
        assert!(matches!(task.await.unwrap(), Err(TinkleError::SecureStore)));
        assert_eq!(
            client
                .authorization
                .store_revision(store.as_ref())
                .as_deref(),
            Some("local-successor")
        );
    }

    /// Prove cancelled restoration withdraws first and retries release with the same ID.
    #[tokio::test(start_paused = true)]
    async fn review_cancelled_permit_withdraws_before_release_and_retries_same_id() {
        let client = client(false);
        let store = Arc::new(ReviewStore::default());
        let permit = installed_permit(&client, store.clone()).await;
        store.end_failures.store(2, Ordering::SeqCst);
        let connection = client.connect_lock.lock().await;
        drop(permit);
        tokio::task::yield_now().await;
        assert!(store.end_ids.lock().unwrap().is_empty());
        drop(connection);
        for _ in 0..3 {
            store.end_entered.notified().await;
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        assert!(!client.retirement.is_restoring());
        assert!(!client.has_returning_source().await);
        assert_eq!(*store.end_ids.lock().unwrap(), vec!["review-permit"; 3]);
    }

    /// Prove waiter cancellation cannot start a second rollback during release finalization.
    #[tokio::test(start_paused = true)]
    async fn review_release_finalizer_has_one_owner_after_waiter_cancellation() {
        let client = client(false);
        let store = Arc::new(ReviewStore::default());
        store.hold_end.store(true, Ordering::SeqCst);
        let mut permit = installed_permit(&client, store.clone()).await;
        let waiter = tokio::spawn(async move { permit.finish(Ok(())).await });
        store.end_entered.notified().await;
        waiter.abort();
        let _ = waiter.await;
        store.end_released.notify_one();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(20)).await;
        assert_eq!(*store.end_ids.lock().unwrap(), vec!["review-permit"]);
        assert!(client.has_returning_source().await);
    }

    /// Prove release timeout withdraws installed material without discarding its retry ID.
    #[tokio::test(start_paused = true)]
    async fn review_release_timeout_withdraws_and_keeps_id_for_retry() {
        let client = client(false);
        let store = Arc::new(ReviewStore::default());
        store.hold_end.store(true, Ordering::SeqCst);
        let mut permit = installed_permit(&client, store.clone()).await;
        let waiter = tokio::spawn(async move { permit.finish(Ok(())).await });
        store.end_entered.notified().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(matches!(
            waiter.await.unwrap(),
            Err(TinkleError::SecureStore)
        ));
        store.end_entered.notified().await;
        assert!(!client.has_returning_source().await);
        store.hold_end.store(false, Ordering::SeqCst);
        store.end_released.notify_one();
        tokio::task::yield_now().await;
        assert_eq!(*store.end_ids.lock().unwrap(), vec!["review-permit"; 2]);
    }

    /// Shut down a runtime with blocked cleanup and assert terminal erasure without leaks.
    fn shutdown_unfinished_restore(block_withdrawal: bool, pending_callback: bool) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = client(false);
        let store = Arc::new(ReviewStore::default());
        store.hold_end.store(pending_callback, Ordering::SeqCst);
        store.end_failures.store(usize::MAX, Ordering::SeqCst);
        let permit = runtime.block_on(installed_permit(&client, store.clone()));
        let secret = permit.installed.as_ref().unwrap().0.clone();
        let connection = block_withdrawal.then(|| runtime.block_on(client.connect_lock.lock()));
        runtime.block_on(async {
            drop(permit);
            if block_withdrawal {
                tokio::task::yield_now().await;
                assert!(store.end_ids.lock().unwrap().is_empty());
            } else {
                store.end_entered.notified().await;
                assert_eq!(*store.end_ids.lock().unwrap(), vec!["review-permit"]);
            }
        });
        runtime.shutdown_timeout(Duration::from_secs(1));
        assert!(
            secret.snapshot().is_err(),
            "unfinished installation key must be retired"
        );
        // Shutdown must neither recurse nor manufacture a release acknowledgement.
        assert_eq!(
            store.end_ids.lock().unwrap().len(),
            usize::from(!block_withdrawal)
        );
        drop(connection);
        let retained_store = Arc::downgrade(&store);
        drop(client);
        drop(store);
        assert!(
            retained_store.upgrade().is_none(),
            "cleanup owner leaked its store"
        );
    }

    /// Prove shutdown terminates cleanup while its host release callback is pending.
    #[test]
    fn review_runtime_shutdown_during_restore_release_callback_is_terminal() {
        shutdown_unfinished_restore(false, true);
    }

    /// Prove shutdown terminates a failed release's retry owner without recursive Drop.
    #[test]
    fn review_runtime_shutdown_during_restore_release_retry_is_terminal() {
        shutdown_unfinished_restore(false, false);
    }

    /// Prove shutdown erases installed material even while withdrawal waits for a lock.
    #[test]
    fn review_runtime_shutdown_with_blocked_restore_withdrawal_is_terminal() {
        shutdown_unfinished_restore(true, false);
    }

    /// Prove unavailable status leaves expired material observable but never admissible.
    #[tokio::test]
    async fn review_expired_restore_keeps_negative_only_metadata_after_unavailable_status() {
        let client = client(false);
        let store = expired_store(&client);
        assert!(client.restore_active_enrollment(store).await.is_err());
        assert!(!client.has_returning_source().await);
        assert!(!client.retirement.is_restoring());
        // A retained negative-only binding is still eligible for the foreground
        // observer; absence would leave the previous Unavailable result untouched.
        assert_eq!(
            client.session_status().await.enrollment_observation,
            crate::EnrollmentObservation::Unavailable
        );
        client.stop().await;
    }

    /// Prove encrypted UNKNOWN and temporary-error replies never admit or erase expired keys.
    #[tokio::test]
    async fn review_expired_restore_unknown_and_temporary_rpc_error_never_grant_admission() {
        use syntra_core::secure_channel::{self, Carrier, Mode, ServerConfig};
        for temporary_error in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (key, pin) = secure_channel::generate_server_key().unwrap();
            let trust = ServerTrust {
                server_public_key: pin,
                server_key_id: 7,
                scope: "review".into(),
            };
            let info = crate::ClientInfo::new(
                "review".into(),
                "test".into(),
                "1".into(),
                "1".into(),
                "1".into(),
                Default::default(),
            )
            .unwrap();
            let mut config = crate::ClientConfig::new("127.0.0.1", info);
            config.primary_port = listener.local_addr().unwrap().port();
            config.fallback_port = None;
            let client = Client::new(
                config,
                TrustSource::Bootstrap(trust.clone()),
                crate::InMemoryCursorStore::new(),
            );
            let mut record = active(&client, &client.authorization.generation().unwrap());
            record.trust = trust;
            let store = Arc::new(ReviewStore::default());
            *store.active.lock().unwrap() = Some(record.encode().unwrap().expose_secret().to_vec());
            let server = tokio::spawn(async move {
                let security = ServerConfig {
                    mode: Mode::Bootstrap,
                    carrier: Carrier::Tcp,
                    server_key_id: 7,
                    scope: b"review".to_vec(),
                    private_key: key,
                    store: None,
                };
                let factory: Arc<dyn syntra_net::AuthenticatorFactory> = Arc::new(
                    |_: &secure_channel::VerifiedIdentity| -> Result<
                        Arc<dyn syntra_runtime::Authenticator>,
                        syntra_runtime::AuthError,
                    > { Ok(Arc::new(syntra_runtime::AllowAnonymous)) },
                );
                let handler = Arc::new(syntra_runtime::FnHandler(
                    move |method: String, bytes: Vec<u8>, _| async move {
                        assert_eq!(method, "/tinkle.v1.ConnectionService/InitConnect");
                        let init = proto::InitConnectRequest::decode(bytes.as_slice()).unwrap();
                        assert_eq!(
                            init.method,
                            tinkle_schema::methods::authservice::GET_ENROLLMENT_DISPOSITION
                        );
                        if temporary_error {
                            return syntra_runtime::HandlerOutcome::error(
                                syntra_core::ErrorCode::Unavailable,
                                "temporary",
                            );
                        }
                        let request =
                            proto::GetEnrollmentDispositionRequest::decode(init.payload.as_ref())
                                .unwrap();
                        syntra_runtime::HandlerOutcome::Payload(
                            proto::InitConnectResponse {
                                payload: proto::GetEnrollmentDispositionResponse {
                                    reference: request.reference,
                                    disposition: proto::DeviceEnrollmentDisposition::Unknown as i32,
                                }
                                .encode_to_vec()
                                .into(),
                            }
                            .encode_to_vec(),
                        )
                    },
                ));
                let (stream, _) = listener.accept().await.unwrap();
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
            assert!(client
                .restore_active_enrollment(store.clone())
                .await
                .is_err());
            assert!(!client.has_returning_source().await);
            assert!(!client.retirement.is_restoring());
            assert_eq!(
                client.session_status().await.enrollment_observation,
                if temporary_error {
                    crate::EnrollmentObservation::Unavailable
                } else {
                    crate::EnrollmentObservation::Unknown
                }
            );
            assert!(store.active.lock().unwrap().is_some());
            assert_eq!(store.clears.load(Ordering::SeqCst), 0);
            client.stop().await;
            server.await.unwrap();
        }
    }
}
