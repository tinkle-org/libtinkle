//! Explicit acknowledged platform secure-store bridge and staged enrollment API.
use crate::{FfiError, TinkleClient};
use prost::Message;
use std::sync::Arc;

/// Opaque zeroizing Rust owner of a protected enrollment record. Foreign code
/// may copy bytes only to encrypt/store them, must erase its copies, and release
/// this handle after the platform transaction finishes. Never log these bytes.
#[derive(uniffi::Object)]
pub struct ProtectedEnrollmentRecord {
    inner: libtinkle::SecretBlob,
}
#[uniffi::export]
impl ProtectedEnrollmentRecord {
    /// Adopt decrypted bytes from Keychain/Keystore-protected storage.
    #[uniffi::constructor]
    pub fn from_secure_storage(bytes: Vec<u8>) -> Arc<Self> {
        Arc::new(Self {
            inner: libtinkle::SecretBlob::new(bytes),
        })
    }
    /// Copy for the host's protected transaction only. No persistence occurs here.
    pub fn copy_for_secure_storage(&self) -> Vec<u8> {
        self.inner.expose_secret().to_vec()
    }
}

/// Exact atomic transaction requested of platform protected storage.
#[derive(Clone, Copy, uniffi::Enum)]
pub enum SecureEnrollmentWrite {
    Operation,
    Candidate,
    Active,
}

/// Atomic protected-store snapshot. Tokens are nonsecret and must never be reused
/// after a namespace reset. The active record stays opaque and zeroizing.
#[derive(uniffi::Record)]
pub struct ProtectedEnrollmentNamespace {
    /// Stable namespace incarnation, shared by app/extension callback wrappers.
    pub incarnation: String,
    /// Revision advanced atomically by every operation/candidate/active write.
    pub revision: String,
    /// Durable tombstone blocking restoration and late writes.
    pub cleanup_pending: bool,
    /// Complete active slot captured in the same storage transaction.
    pub active: Option<Arc<ProtectedEnrollmentRecord>>,
    /// Recoverable pending work exists besides the active slot.
    pub has_pending: bool,
}

/// Implement over confidential, durable platform storage. Returning success from
/// commit acknowledges durability, never an asynchronous enqueue. Operation is
/// create-only; Candidate replaces only its operation; Active atomically installs
/// both the active slot and operation record. Preserve prior active material
/// until Active commits. No ordinary preferences/cache/SQLite implementation is
/// provided. Failures are sanitized before they return to the Rust coordinator.
/// clear is a full durable namespace invalidation, including all pending
/// capabilities; it must prevent earlier writes from publishing afterward.
#[uniffi::export(with_foreign)]
#[async_trait::async_trait]
pub trait FfiSecureEnrollmentStore: Send + Sync {
    /// Atomically compare incarnation/revision and the entire ACTIVE record,
    /// reject cleanup markers, and exclude all namespace writes/clears/resets
    /// until endRestore. False grants no permission. No unchecked lease expiry.
    /// endRestore must cancel a late beginRestore with the same caller-owned ID.
    async fn begin_restore(
        &self,
        incarnation: String,
        revision: String,
        active: Arc<ProtectedEnrollmentRecord>,
        permit_id: String,
    ) -> Result<bool, FfiError>;
    /// Idempotently release/cancel this install permit, even if acquisition is
    /// still pending. A lost owner needs process-scoped release or blocked writes
    /// until host recovery; never allow late adoption after a silent expiry.
    async fn end_restore(&self, permit_id: String) -> Result<(), FfiError>;
    /// Return None for legacy storage, which cannot attest restart-safe cleanup.
    async fn namespace_snapshot(&self) -> Result<Option<ProtectedEnrollmentNamespace>, FfiError>;
    /// Atomically compare revision, reject tombstones, commit, and advance revision.
    /// None means superseded with no write; never use an unconditional fallback.
    async fn commit_current(
        &self,
        expected_revision: String,
        kind: SecureEnrollmentWrite,
        operation_id: String,
        record: Arc<ProtectedEnrollmentRecord>,
    ) -> Result<Option<String>, FfiError>;
    /// Compare revision and persist a durable cleanup marker before acknowledging.
    async fn begin_clear(&self, expected_revision: String) -> Result<bool, FfiError>;
    /// Clear only under that matching marker; retain it to block restoration.
    async fn finish_clear(&self, expected_revision: String) -> Result<bool, FfiError>;
    /// Atomically invalidate ACTIVE and every operation/candidate/capability in
    /// this namespace. Return success only after durable clearing/tombstoning
    /// that also rejects pending old writes. Idempotent; failure is not an ack.
    async fn clear(&self) -> Result<(), FfiError>;
    /// Acknowledge an atomic durable write with the transaction semantics described above.
    async fn commit(
        &self,
        kind: SecureEnrollmentWrite,
        operation_id: String,
        record: Arc<ProtectedEnrollmentRecord>,
    ) -> Result<(), FfiError>;
    /// Load a retained operation or candidate by operation ID from protected storage.
    async fn load_operation(
        &self,
        operation_id: String,
    ) -> Result<Option<Arc<ProtectedEnrollmentRecord>>, FfiError>;
    /// Load the acknowledged active slot, returning None when no active enrollment exists.
    async fn load_active(&self) -> Result<Option<Arc<ProtectedEnrollmentRecord>>, FfiError>;
}

struct Adapter(Arc<dyn FfiSecureEnrollmentStore>);
#[async_trait::async_trait]
impl libtinkle::SecureEnrollmentStore for Adapter {
    /// Forward exact restore tokens and opaque active bytes, sanitizing host failures.
    async fn begin_restore(
        &self,
        incarnation: &str,
        revision: &str,
        active: &libtinkle::SecretBlob,
        id: &str,
    ) -> Result<bool, libtinkle::SecureStoreError> {
        self.0
            .begin_restore(
                incarnation.into(),
                revision.into(),
                Arc::new(ProtectedEnrollmentRecord {
                    inner: libtinkle::SecretBlob::new(active.expose_secret().to_vec()),
                }),
                id.into(),
            )
            .await
            .map_err(|_| libtinkle::SecureStoreError)
    }
    /// Await host permit release while hiding foreign diagnostic text from the coordinator.
    async fn end_restore(&self, id: &str) -> Result<(), libtinkle::SecureStoreError> {
        self.0
            .end_restore(id.into())
            .await
            .map_err(|_| libtinkle::SecureStoreError)
    }
    /// Preserve atomic namespace metadata and copy active bytes into a zeroizing Rust owner.
    async fn namespace_snapshot(
        &self,
    ) -> Result<Option<libtinkle::EnrollmentNamespaceSnapshot>, libtinkle::SecureStoreError> {
        self.0
            .namespace_snapshot()
            .await
            .map(|value| {
                value.map(|value| libtinkle::EnrollmentNamespaceSnapshot {
                    incarnation: value.incarnation,
                    revision: value.revision,
                    cleanup_pending: value.cleanup_pending,
                    has_pending: value.has_pending,
                    active: value.active.map(|record| {
                        libtinkle::SecretBlob::new(record.inner.expose_secret().to_vec())
                    }),
                })
            })
            .map_err(|_| libtinkle::SecureStoreError)
    }
    /// Forward conditional writes without replacing a superseded result with a legacy write.
    async fn commit_current(
        &self,
        expected: &str,
        kind: libtinkle::SecureWrite,
        operation_id: &str,
        record: &libtinkle::SecretBlob,
    ) -> Result<Option<String>, libtinkle::SecureStoreError> {
        self.0
            .commit_current(
                expected.into(),
                write_kind(kind),
                operation_id.into(),
                Arc::new(ProtectedEnrollmentRecord {
                    inner: libtinkle::SecretBlob::new(record.expose_secret().to_vec()),
                }),
            )
            .await
            .map_err(|_| libtinkle::SecureStoreError)
    }
    /// Await the host's revision-checked cleanup-marker acknowledgement.
    async fn begin_clear(&self, expected: &str) -> Result<bool, libtinkle::SecureStoreError> {
        self.0
            .begin_clear(expected.into())
            .await
            .map_err(|_| libtinkle::SecureStoreError)
    }
    /// Await conditional durable clearing, preserving rejection and sanitizing failures.
    async fn finish_clear(&self, expected: &str) -> Result<bool, libtinkle::SecureStoreError> {
        self.0
            .finish_clear(expected.into())
            .await
            .map_err(|_| libtinkle::SecureStoreError)
    }
    /// Await legacy full-namespace clearing without exposing foreign error details.
    async fn clear(&self) -> Result<(), libtinkle::SecureStoreError> {
        self.0
            .clear()
            .await
            .map_err(|_| libtinkle::SecureStoreError)
    }
    /// Transfer write intent and opaque bytes, returning only after host acknowledgement.
    async fn commit(
        &self,
        kind: libtinkle::SecureWrite,
        operation_id: &str,
        record: &libtinkle::SecretBlob,
    ) -> Result<(), libtinkle::SecureStoreError> {
        let kind = write_kind(kind);
        self.0
            .commit(
                kind,
                operation_id.into(),
                Arc::new(ProtectedEnrollmentRecord {
                    inner: libtinkle::SecretBlob::new(record.expose_secret().to_vec()),
                }),
            )
            .await
            .map_err(|_| libtinkle::SecureStoreError)
    }
    /// Adopt optional protected operation bytes and replace foreign errors with storage failure.
    async fn load_operation(
        &self,
        id: &str,
    ) -> Result<Option<libtinkle::SecretBlob>, libtinkle::SecureStoreError> {
        self.0
            .load_operation(id.into())
            .await
            .map(|v| {
                v.map(|record| libtinkle::SecretBlob::new(record.inner.expose_secret().to_vec()))
            })
            .map_err(|_| libtinkle::SecureStoreError)
    }
    /// Adopt optional protected active bytes into a zeroizing coordinator-owned record.
    async fn load_active(
        &self,
    ) -> Result<Option<libtinkle::SecretBlob>, libtinkle::SecureStoreError> {
        self.0
            .load_active()
            .await
            .map(|v| {
                v.map(|record| libtinkle::SecretBlob::new(record.inner.expose_secret().to_vec()))
            })
            .map_err(|_| libtinkle::SecureStoreError)
    }
}

/// Translate transaction intent without changing conditional-write semantics.
fn write_kind(kind: libtinkle::SecureWrite) -> SecureEnrollmentWrite {
    match kind {
        libtinkle::SecureWrite::Operation => SecureEnrollmentWrite::Operation,
        libtinkle::SecureWrite::Candidate => SecureEnrollmentWrite::Candidate,
        libtinkle::SecureWrite::Active => SecureEnrollmentWrite::Active,
    }
}

/// Receipt for a durably retained operation/capability. No public constructor.
#[derive(uniffi::Object)]
pub struct EnrollmentOperation {
    inner: libtinkle::DurableOperation,
}
#[uniffi::export]
impl EnrollmentOperation {
    /// Return the durable operation identifier without exposing its recovery capability.
    pub fn operation_id(&self) -> String {
        self.inner.operation_id().into()
    }
}

/// Securely persisted candidate, never a Returning credential. No constructor.
#[derive(uniffi::Object)]
pub struct StoredEnrollment {
    inner: libtinkle::PersistedEnrollment,
}
#[uniffi::export]
impl StoredEnrollment {
    /// Return candidate metadata without exposing its authorization key or capability.
    pub fn status(&self) -> EnrollmentStatus {
        status(self.inner.status())
    }
}

/// Secret-free projection of the exact tuple acknowledged by the server.
#[derive(uniffi::Record)]
pub struct EnrollmentStatus {
    pub operation_id: String,
    pub session_id: String,
    pub handle: Vec<u8>,
    pub enrollment_epoch: u64,
    pub scope: i32,
    pub expires_at_seconds: i64,
    pub expires_at_nanos: i32,
    pub state: i32,
    pub user_id: i64,
    pub account_auth_epoch: u64,
}
/// Preserve enrollment metadata and split expiry into seconds and nanoseconds for native hosts.
fn status(value: &tinkle_schema::proto::DeviceEnrollmentStatus) -> EnrollmentStatus {
    let expiry = value.expires_at.unwrap_or_default();
    EnrollmentStatus {
        operation_id: value.operation_id.clone(),
        session_id: value.session_id.clone(),
        handle: value.handle.to_vec(),
        enrollment_epoch: value.enrollment_epoch,
        scope: value.scope,
        expires_at_seconds: expiry.seconds,
        expires_at_nanos: expiry.nanos,
        state: value.state,
        user_id: value.user_id,
        account_auth_epoch: value.account_auth_epoch,
    }
}

/// SignIn's account projection plus a persisted staged receipt; no AK/capability.
#[derive(uniffi::Record)]
pub struct EnrollmentSignInResult {
    pub user_proto: Option<Vec<u8>>,
    pub is_new_user: bool,
    pub enrollment: Arc<StoredEnrollment>,
}

/// Verification flow metadata. Treat the identifier as private flow state.
#[derive(uniffi::Record)]
pub struct CodeDelivery {
    pub verification_id: String,
    pub code_type: i32,
    pub expires_at_seconds: Option<i64>,
}

#[uniffi::export(async_runtime = "tokio")]
impl TinkleClient {
    /// Register a namespace for full acknowledged logout invalidation without
    /// restoring credentials. Normal enrollment/restore calls bind automatically.
    pub async fn bind_secure_enrollment_store(
        &self,
        store: Arc<dyn FfiSecureEnrollmentStore>,
    ) -> Result<(), FfiError> {
        self.core_client()
            .bind_enrollment_store(Arc::new(Adapter(store)))
            .await
            .map_err(Into::into)
    }
    /// First public request is automatically InitConnect-wrapped on this connection.
    pub async fn send_code(
        &self,
        phone_number: String,
        lang_code: String,
    ) -> Result<CodeDelivery, FfiError> {
        let response = self
            .core_client()
            .auth()
            .send_code(tinkle_schema::proto::SendCodeRequest {
                phone_number,
                lang_code,
            })
            .await?;
        Ok(CodeDelivery {
            verification_id: response.verification_id,
            code_type: response.code_type,
            expires_at_seconds: response.expires_at.map(|expiry| expiry.seconds),
        })
    }

    /// Persist the operation UUID/capability before SignIn or rotation can run.
    pub async fn prepare_enrollment(
        &self,
        store: Arc<dyn FfiSecureEnrollmentStore>,
    ) -> Result<Arc<EnrollmentOperation>, FfiError> {
        Ok(Arc::new(EnrollmentOperation {
            inner: self
                .core_client()
                .prepare_enrollment(Arc::new(Adapter(store)))
                .await?,
        }))
    }
    /// Recover pending flow after an ambiguous result/process restart, without SMS reuse.
    pub async fn load_enrollment_operation(
        &self,
        store: Arc<dyn FfiSecureEnrollmentStore>,
        operation_id: String,
    ) -> Result<Arc<EnrollmentOperation>, FfiError> {
        Ok(Arc::new(EnrollmentOperation {
            inner: self
                .core_client()
                .load_enrollment_operation(Arc::new(Adapter(store)), &operation_id)
                .await?,
        }))
    }
    /// Deliver on pinned Bootstrap, then await atomic candidate persistence.
    pub async fn sign_in(
        &self,
        phone_number: String,
        code: String,
        verification_id: String,
        operation: Arc<EnrollmentOperation>,
    ) -> Result<EnrollmentSignInResult, FfiError> {
        let result = self
            .core_client()
            .auth()
            .sign_in(
                libtinkle::SignInDetails::new(phone_number, code, verification_id)?,
                &operation.inner,
            )
            .await?;
        Ok(EnrollmentSignInResult {
            user_proto: result.user.map(|user| user.encode_to_vec()),
            is_new_user: result.is_new_user,
            enrollment: Arc::new(StoredEnrollment {
                inner: result.enrollment,
            }),
        })
    }
    /// Recover and persist delivery on pinned Bootstrap using the retained operation capability.
    pub async fn resume_enrollment(
        &self,
        operation: Arc<EnrollmentOperation>,
    ) -> Result<Arc<StoredEnrollment>, FfiError> {
        Ok(Arc::new(StoredEnrollment {
            inner: self
                .core_client()
                .auth()
                .resume_enrollment(&operation.inner)
                .await?,
        }))
    }
    /// Stage and persist a successor on Returning while preserving the previous active slot.
    pub async fn rotate_enrollment(
        &self,
        operation: Arc<EnrollmentOperation>,
    ) -> Result<Arc<StoredEnrollment>, FfiError> {
        Ok(Arc::new(StoredEnrollment {
            inner: self
                .core_client()
                .auth()
                .rotate_enrollment(&operation.inner)
                .await?,
        }))
    }
    /// No acknowledgement can be sent without a StoredEnrollment receipt. Uses
    /// pinned Bootstrap and the retained capability; persists ACTIVE before fresh Returning.
    pub async fn activate_enrollment(
        &self,
        candidate: Arc<StoredEnrollment>,
    ) -> Result<EnrollmentStatus, FfiError> {
        Ok(status(
            &self
                .core_client()
                .auth()
                .activate_enrollment(&candidate.inner)
                .await?,
        ))
    }
    /// Observe conditional retained metadata before restoring an unexpired ACTIVE key.
    /// Exact retirement fences and awaits local clearing. Expired keys are never
    /// installed; UNKNOWN grants no admission. Inspect sessionStatus for cleanup acknowledgement.
    pub async fn restore_active_enrollment(
        &self,
        store: Arc<dyn FfiSecureEnrollmentStore>,
    ) -> Result<EnrollmentStatus, FfiError> {
        Ok(status(
            &self
                .core_client()
                .restore_active_enrollment(Arc::new(Adapter(store)))
                .await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libtinkle::SecureEnrollmentStore as _;

    struct PendingHost {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    /// Check each write kind crosses the adapter with its expected identifier and exact bytes.
    fn assert_write(kind: SecureEnrollmentWrite, id: &str, record: &ProtectedEnrollmentRecord) {
        let (expected_id, expected_record) = match kind {
            SecureEnrollmentWrite::Operation => ("operation-only", vec![4, 5]),
            SecureEnrollmentWrite::Candidate => ("candidate-operation", vec![6, 7, 8]),
            SecureEnrollmentWrite::Active => ("operation", vec![1, 2, 3]),
        };
        assert_eq!(id, expected_id);
        assert_eq!(record.copy_for_secure_storage(), expected_record);
    }

    #[async_trait::async_trait]
    impl FfiSecureEnrollmentStore for PendingHost {
        /// Validate the complete restore request and simulate a denied install permit.
        async fn begin_restore(
            &self,
            incarnation: String,
            revision: String,
            active: Arc<ProtectedEnrollmentRecord>,
            id: String,
        ) -> Result<bool, FfiError> {
            assert_eq!(incarnation, "fixture-incarnation");
            assert_eq!(revision, "revision:7");
            assert_eq!(active.copy_for_secure_storage(), vec![1, 2, 3]);
            assert_eq!(id, "restore-permit");
            Ok(false)
        }
        /// Validate the permit ID and inject a foreign release error for sanitization checks.
        async fn end_restore(&self, id: String) -> Result<(), FfiError> {
            assert_eq!(id, "restore-permit");
            Err(FfiError::Storage("untrusted release diagnostic".into()))
        }
        /// Return fixed atomic namespace metadata with an opaque active record.
        async fn namespace_snapshot(
            &self,
        ) -> Result<Option<ProtectedEnrollmentNamespace>, FfiError> {
            Ok(Some(ProtectedEnrollmentNamespace {
                incarnation: "fixture-incarnation".into(),
                revision: "revision:7".into(),
                cleanup_pending: false,
                has_pending: true,
                active: Some(ProtectedEnrollmentRecord::from_secure_storage(vec![9])),
            }))
        }
        /// Validate conditional write arguments and simulate superseded ACTIVE persistence.
        async fn commit_current(
            &self,
            revision: String,
            kind: SecureEnrollmentWrite,
            id: String,
            record: Arc<ProtectedEnrollmentRecord>,
        ) -> Result<Option<String>, FfiError> {
            assert_eq!(revision, "revision:7");
            assert_write(kind, &id, &record);
            Ok(match kind {
                SecureEnrollmentWrite::Active => None,
                _ => Some("revision:8".into()),
            })
        }
        /// Verify the expected revision and simulate rejection of a cleanup marker.
        async fn begin_clear(&self, revision: String) -> Result<bool, FfiError> {
            assert_eq!(revision, "revision:7");
            Ok(false)
        }
        /// Verify the expected revision and inject a sensitive foreign cleanup error.
        async fn finish_clear(&self, revision: String) -> Result<bool, FfiError> {
            assert_eq!(revision, "revision:7");
            Err(FfiError::Storage("untrusted host diagnostic".into()))
        }
        /// Inject a foreign legacy-clear failure that must not escape unsanitized.
        async fn clear(&self) -> Result<(), FfiError> {
            Err(FfiError::Storage("untrusted host clear diagnostic".into()))
        }
        /// Hold acknowledgement after validating the write, then return a foreign storage error.
        async fn commit(
            &self,
            kind: SecureEnrollmentWrite,
            id: String,
            record: Arc<ProtectedEnrollmentRecord>,
        ) -> Result<(), FfiError> {
            assert_write(kind, &id, &record);
            self.entered.notify_one();
            self.release.notified().await;
            Err(FfiError::Storage(
                "untrusted sensitive host diagnostic".into(),
            ))
        }
        /// Model a host without a retained operation record.
        async fn load_operation(
            &self,
            _: String,
        ) -> Result<Option<Arc<ProtectedEnrollmentRecord>>, FfiError> {
            Ok(None)
        }
        /// Model a host without a retained active slot.
        async fn load_active(&self) -> Result<Option<Arc<ProtectedEnrollmentRecord>>, FfiError> {
            Ok(None)
        }
    }

    /// Conditional fences and opaque records cross FFI without unconditional fallbacks.
    #[tokio::test]
    async fn adapter_preserves_namespace_and_conditional_outcomes() {
        let adapter = Adapter(Arc::new(PendingHost {
            entered: Default::default(),
            release: Default::default(),
        }));
        let snapshot = adapter.namespace_snapshot().await.unwrap().unwrap();
        assert!(!adapter
            .begin_restore(
                "fixture-incarnation",
                "revision:7",
                &libtinkle::SecretBlob::new(vec![1, 2, 3]),
                "restore-permit"
            )
            .await
            .unwrap());
        assert_eq!(
            adapter
                .end_restore("restore-permit")
                .await
                .unwrap_err()
                .to_string(),
            "protected enrollment storage unavailable"
        );
        assert_eq!(snapshot.incarnation, "fixture-incarnation");
        assert_eq!(snapshot.revision, "revision:7");
        assert!(!snapshot.cleanup_pending);
        assert!(snapshot.has_pending);
        assert_eq!(snapshot.active.unwrap().expose_secret(), &[9]);
        assert!(adapter
            .commit_current(
                "revision:7",
                libtinkle::SecureWrite::Active,
                "operation",
                &libtinkle::SecretBlob::new(vec![1, 2, 3])
            )
            .await
            .unwrap()
            .is_none());
        for (kind, id, bytes) in [
            (
                libtinkle::SecureWrite::Operation,
                "operation-only",
                vec![4, 5],
            ),
            (
                libtinkle::SecureWrite::Candidate,
                "candidate-operation",
                vec![6, 7, 8],
            ),
        ] {
            assert_eq!(
                adapter
                    .commit_current("revision:7", kind, id, &libtinkle::SecretBlob::new(bytes))
                    .await
                    .unwrap(),
                Some("revision:8".into())
            );
        }
        assert!(!adapter.begin_clear("revision:7").await.unwrap());
        assert_eq!(
            adapter
                .finish_clear("revision:7")
                .await
                .unwrap_err()
                .to_string(),
            "protected enrollment storage unavailable"
        );
    }

    #[tokio::test]
    /// Every write must await its host acknowledgement and sanitize sensitive failure text.
    async fn adapter_awaits_host_acknowledgement_and_sanitizes_failure() {
        let host = Arc::new(PendingHost {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        for (kind, id, bytes) in [
            (
                libtinkle::SecureWrite::Operation,
                "operation-only",
                vec![4, 5],
            ),
            (
                libtinkle::SecureWrite::Candidate,
                "candidate-operation",
                vec![6, 7, 8],
            ),
            (libtinkle::SecureWrite::Active, "operation", vec![1, 2, 3]),
        ] {
            let adapter = Adapter(host.clone());
            let write = tokio::spawn(async move {
                adapter
                    .commit(kind, id, &libtinkle::SecretBlob::new(bytes))
                    .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(2), host.entered.notified())
                .await
                .expect("host did not receive the expected write");
            assert!(!write.is_finished());
            host.release.notify_one();
            let error = write.await.unwrap().unwrap_err();
            assert_eq!(
                error.to_string(),
                "protected enrollment storage unavailable"
            );
        }
        let error = Adapter(host).clear().await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "protected enrollment storage unavailable"
        );
    }
}
