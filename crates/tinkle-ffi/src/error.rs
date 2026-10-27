//! Structured FFI error surface.
//!
//! Foreign sees named variants with message payloads (e.g. `Storage(String)`);
//! no Syntra, prost, or schema types cross the boundary. [`FfiError`] is built
//! from [`libtinkle::TinkleError`] via codes, preserving variant structure so
//! foreign cursor-store failures round-trip instead of panicking.
//!
//! Code mapping (never branch on text foreign-side):
//! - `Unauthenticated` / `PermissionDenied` → [`FfiError::Auth`]
//!   (re-authenticate, never blindly retry).
//! - Transient recovery codes (`Unavailable`, `DeadlineExceeded`,
//!   `Aborted`, `ResourceExhausted`, `Internal`, `Unknown`, `DataLoss`)
//!   → [`FfiError::Retryable`] (bounded retry with backoff).
//! - Everything else keeps its legacy variant (`Network`, `Sync`, …).
//!   There is no rebuild variant: per-target reconciliation reports
//!   failures as `ReconcileFailed` events and `Failed` target status,
//!   never as a terminal stream error.

/// Errors surfaced to Swift/Kotlin.
///
/// This must not use `flat_error`: foreign `FfiCursorStore` implementations
/// return `FfiError`, and UniFFI cannot lift flat errors back into Rust
/// (`rustPanic("Can't lift flat errors")`). Keeping the message payloads
/// makes store failures round-trip instead of panicking.
#[derive(Debug, Clone, thiserror::Error, uniffi::Error)]
pub enum FfiError {
    #[error("network error: {0}")]
    Network(String),
    #[error("not connected: call connect() first")]
    NotConnected,
    #[error("operation timed out")]
    Timeout,
    #[error("operation was cancelled")]
    Cancelled,
    #[error("sync error: {0}")]
    Sync(String),
    #[error("codec error: {0}")]
    Codec(String),
    #[error("cursor store error: {0}")]
    Storage(String),
    #[error("reconnect attempts exhausted")]
    ReconnectExhausted,
    #[error("internal error: {0}")]
    Internal(String),
    /// Authentication/authorization failure. Re-authenticate (or rotate the
    /// credential); never retry the same bytes in a tight loop.
    #[error("authentication error: {0}")]
    Auth(String),
    /// Transient failure: bounded retry with backoff is safe. Reconciliation
    /// reads are idempotent; sends still require their `random_id` policy.
    #[error("transient error, retry with backoff: {0}")]
    Retryable(String),
}

impl From<libtinkle::TinkleError> for FfiError {
    fn from(e: libtinkle::TinkleError) -> Self {
        match e {
            libtinkle::TinkleError::Syntra(s) => {
                let msg = s.to_string();
                match s.code() {
                    syntra_core::ErrorCode::Cancelled => Self::Cancelled,
                    // DeadlineExceeded is retryable (bounded retry with
                    // backoff), matching `GrpcStatus` + `RecoveryDecision::Retry`.
                    // `Timeout` is retained for API compat but currently unmapped.
                    syntra_core::ErrorCode::DeadlineExceeded
                    | syntra_core::ErrorCode::Unavailable
                    | syntra_core::ErrorCode::Aborted
                    | syntra_core::ErrorCode::ResourceExhausted
                    | syntra_core::ErrorCode::Internal
                    | syntra_core::ErrorCode::Unknown
                    | syntra_core::ErrorCode::DataLoss => Self::Retryable(msg),
                    syntra_core::ErrorCode::Unauthenticated
                    | syntra_core::ErrorCode::PermissionDenied => Self::Auth(msg),
                    syntra_core::ErrorCode::Unspecified
                    | syntra_core::ErrorCode::InvalidArgument
                    | syntra_core::ErrorCode::NotFound
                    | syntra_core::ErrorCode::AlreadyExists
                    | syntra_core::ErrorCode::FailedPrecondition
                    | syntra_core::ErrorCode::OutOfRange
                    | syntra_core::ErrorCode::Unimplemented => Self::Network(msg),
                }
            }
            libtinkle::TinkleError::NotConnected(_) => Self::NotConnected,
            libtinkle::TinkleError::ReconnectExhausted { .. } => Self::ReconnectExhausted,
            libtinkle::TinkleError::Sync(msg) => Self::Sync(msg),
            libtinkle::TinkleError::SyncError(e) => Self::Sync(e.to_string()),
            // Transport-level gRPC failures without a status are transient
            // by construction (connect/read framing, no status code).
            libtinkle::TinkleError::Grpc(msg) => Self::Retryable(msg),
            libtinkle::TinkleError::GrpcStatus { code, message } => match code {
                syntra_core::ErrorCode::Cancelled => Self::Cancelled,
                syntra_core::ErrorCode::Unauthenticated
                | syntra_core::ErrorCode::PermissionDenied => Self::Auth(message),
                syntra_core::ErrorCode::Unavailable
                | syntra_core::ErrorCode::DeadlineExceeded
                | syntra_core::ErrorCode::Aborted
                | syntra_core::ErrorCode::ResourceExhausted
                | syntra_core::ErrorCode::Internal
                | syntra_core::ErrorCode::Unknown
                | syntra_core::ErrorCode::DataLoss => Self::Retryable(message),
                _ => Self::Network(message),
            },
            // Malformed locally-supplied credentials are auth-domain, never
            // retryable: re-authenticate, don't back off.
            libtinkle::TinkleError::GrpcAuth(msg) => Self::Auth(msg),
            // Local setup errors (bad endpoint) are permanent and never
            // retryable: surface as internal config errors, not network.
            libtinkle::TinkleError::GrpcConfig(msg) => Self::Internal(msg),
            libtinkle::TinkleError::Codec(msg) => Self::Codec(msg),
            libtinkle::TinkleError::CursorStore(msg) => Self::Storage(msg),
            // Subscription setup failures are sync-domain failures.
            libtinkle::TinkleError::Subscription(msg) => Self::Sync(msg),
            // Per-target journal failures surface structurally as
            // `ReconcileFailed` events + `Failed` target status; as returned
            // errors they stay sync-domain with full context preserved.
            libtinkle::TinkleError::CheckpointFailed { .. } => Self::Sync(e.to_string()),
            libtinkle::TinkleError::Reconcile { .. } => Self::Sync(e.to_string()),
        }
    }
}

impl From<FfiError> for libtinkle::TinkleError {
    /// Folds a foreign store failure back into the core error type. Every
    /// cursor-store operation failure is storage-domain by construction.
    fn from(e: FfiError) -> Self {
        match e {
            FfiError::Storage(msg) => Self::CursorStore(msg),
            other => Self::CursorStore(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn syntra_error(code: syntra_core::ErrorCode) -> libtinkle::TinkleError {
        libtinkle::TinkleError::Syntra(syntra_core::error::SyntraError::Rpc {
            code,
            raw_code: None,
            message: "injected".into(),
        })
    }

    fn grpc_status(code: syntra_core::ErrorCode) -> libtinkle::TinkleError {
        libtinkle::TinkleError::GrpcStatus {
            code,
            message: "injected".into(),
        }
    }

    fn discriminant(e: &FfiError) -> &'static str {
        match e {
            FfiError::Network(_) => "Network",
            FfiError::NotConnected => "NotConnected",
            FfiError::Timeout => "Timeout",
            FfiError::Cancelled => "Cancelled",
            FfiError::Sync(_) => "Sync",
            FfiError::Codec(_) => "Codec",
            FfiError::Storage(_) => "Storage",
            FfiError::ReconnectExhausted => "ReconnectExhausted",
            FfiError::Internal(_) => "Internal",
            FfiError::Auth(_) => "Auth",
            FfiError::Retryable(_) => "Retryable",
        }
    }

    #[test]
    fn error_code_matrix_syntra_and_grpc_status() {
        use syntra_core::ErrorCode as C;
        // (code, expected FFI discriminant) — both paths must agree.
        let cases: [(C, &str); 17] = [
            (C::Unspecified, "Network"),
            (C::Cancelled, "Cancelled"),
            (C::Unknown, "Retryable"),
            (C::InvalidArgument, "Network"),
            (C::DeadlineExceeded, "Retryable"),
            (C::NotFound, "Network"),
            (C::AlreadyExists, "Network"),
            (C::PermissionDenied, "Auth"),
            (C::ResourceExhausted, "Retryable"),
            (C::FailedPrecondition, "Network"),
            (C::Aborted, "Retryable"),
            (C::OutOfRange, "Network"),
            (C::Unimplemented, "Network"),
            (C::Internal, "Retryable"),
            (C::Unavailable, "Retryable"),
            (C::DataLoss, "Retryable"),
            (C::Unauthenticated, "Auth"),
        ];
        for (code, expected) in cases {
            let syntra_ffi = FfiError::from(syntra_error(code));
            assert_eq!(
                discriminant(&syntra_ffi),
                expected,
                "Syntra {code:?} must map to {expected}"
            );
            let grpc_ffi = FfiError::from(grpc_status(code));
            assert_eq!(
                discriminant(&grpc_ffi),
                expected,
                "GrpcStatus {code:?} must map to {expected}"
            );
        }
    }
}
