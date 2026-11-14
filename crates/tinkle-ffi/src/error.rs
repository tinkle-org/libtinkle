//! Structured FFI error surface.
//!
//! Foreign sees named variants with message payloads (e.g. `Storage(String)`);
//! no Syntra, prost, or schema types cross the boundary. [`FfiError`] is built
//! from [`libtinkle::TinkleError`] via codes, preserving variant structure so
//! foreign cursor-store failures round-trip instead of panicking.
//!
//! Received RPC failures retain exact status/raw code, outcome uncertainty and conditional
//! recovery guidance. Revision conflicts require refetch, not blind mutation retry.

/// Recovery guidance does not authorize replay of an uncertain mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum FfiRpcRecovery {
    /// Refetch the current revision and reconcile user intent before another mutation.
    Refetch,
    /// Only an idempotent read or explicitly authorized exact-key retry may back off and retry.
    ConditionalRetry,
    /// Correct input or authorization; do not retry the same mutation blindly.
    DoNotRetry,
}

/// Errors surfaced to Swift/Kotlin.
///
/// This must not use `flat_error`: foreign `FfiCursorStore` implementations
/// return `FfiError`, and UniFFI cannot lift flat errors back into Rust
/// (`rustPanic("Can't lift flat errors")`). Keeping the message payloads
/// makes store failures round-trip instead of panicking.
#[derive(Debug, Clone, thiserror::Error, uniffi::Error)]
pub enum FfiError {
    /// Exact terminal RPC status. Unknown outcome is not rollback, even for a received result.
    #[error("RPC failed [{code}]: {diagnostic}")]
    Rpc {
        code: i32,
        raw_code: Option<i32>,
        diagnostic: String,
        unknown_outcome: bool,
        recovery: FfiRpcRecovery,
    },
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
    /// Local transient failure. Retry policy still depends on operation identity and uncertainty.
    #[error("transient error, retry with backoff: {0}")]
    Retryable(String),
}

impl From<libtinkle::TinkleError> for FfiError {
    /// Map core failures by typed code while preserving storage, sync, and lifecycle distinctions.
    fn from(e: libtinkle::TinkleError) -> Self {
        match e {
            libtinkle::TinkleError::SignedOut => Self::Auth(e.to_string()),
            libtinkle::TinkleError::SecureStore => Self::Storage(e.to_string()),
            libtinkle::TinkleError::Syntra(s) => {
                if let syntra_core::error::SyntraError::Rpc {
                    code,
                    raw_code,
                    message,
                } = &s
                {
                    return Self::Rpc {
                        code: *code as i32,
                        raw_code: *raw_code,
                        diagnostic: message.clone(),
                        unknown_outcome: s.is_unknown_outcome(),
                        recovery: match code {
                            syntra_core::ErrorCode::Aborted => FfiRpcRecovery::Refetch,
                            syntra_core::ErrorCode::Unavailable
                            | syntra_core::ErrorCode::DeadlineExceeded
                            | syntra_core::ErrorCode::ResourceExhausted
                            | syntra_core::ErrorCode::Internal
                            | syntra_core::ErrorCode::Unknown => FfiRpcRecovery::ConditionalRetry,
                            _ => FfiRpcRecovery::DoNotRetry,
                        },
                    };
                }
                let msg = s.to_string();
                match s.code() {
                    syntra_core::ErrorCode::Cancelled => Self::Cancelled,
                    // DeadlineExceeded is retryable (bounded retry with
                    // backoff), matching `RecoveryDecision::Retry`.
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
            libtinkle::TinkleError::NetworkUnavailable => Self::Retryable(e.to_string()),
            libtinkle::TinkleError::TrustRejected => Self::Auth(e.to_string()),
            libtinkle::TinkleError::EstablishmentFailed => Self::Network(e.to_string()),
            libtinkle::TinkleError::Configuration(msg) => Self::Internal(msg),
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

    fn discriminant(e: &FfiError) -> &'static str {
        match e {
            FfiError::Rpc { .. } => "Rpc",
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
    fn error_code_matrix_syntra() {
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
        for (code, _) in cases {
            let syntra_ffi = FfiError::from(syntra_error(code));
            assert_eq!(discriminant(&syntra_ffi), "Rpc");
            let FfiError::Rpc {
                code: got,
                raw_code,
                unknown_outcome,
                recovery,
                ..
            } = syntra_ffi
            else {
                unreachable!()
            };
            assert_eq!(got, code as i32);
            assert_eq!(raw_code, None);
            assert_eq!(unknown_outcome, syntra_error(code).is_unknown_outcome());
            if code == C::Aborted {
                assert_eq!(recovery, FfiRpcRecovery::Refetch);
            }
        }
    }

    /// Future status codes retain the exact received number and conservative outcome certainty.
    #[test]
    fn future_rpc_code_is_retained() {
        let error = FfiError::from(libtinkle::TinkleError::Syntra(
            syntra_core::error::SyntraError::Rpc {
                code: syntra_core::ErrorCode::Unknown,
                raw_code: Some(1234),
                message: "future".into(),
            },
        ));
        assert!(matches!(
            error,
            FfiError::Rpc {
                code: 2,
                raw_code: Some(1234),
                unknown_outcome: true,
                ..
            }
        ));
    }
}
