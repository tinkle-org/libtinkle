//! Typed Syntra errors (`SNTR-ERR-*`).
//!
//! The `ErrorCode` enum is the sole wire vocabulary (17 values, 0-16).
//! Clients branch on `code`, never on `message` text. Unknown future
//! codes map to `Unknown` for reporting while preserving the numeric
//! value diagnostically.

use thiserror::Error;

/// Wire error codes. Mirrors `syntra.v1.ErrorCode` (errors.proto).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ErrorCode {
    Unspecified = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl ErrorCode {
    /// Map a raw i32 (possibly from a newer minor peer) to a code.
    /// Unknown numbers become `Unknown` (SNTR-ERR-005); the original
    /// number is preserved by the caller for diagnostics.
    pub fn from_i32_lossy(v: i32) -> (Self, Option<i32>) {
        match v {
            0 => (Self::Unspecified, None),
            1 => (Self::Cancelled, None),
            2 => (Self::Unknown, None),
            3 => (Self::InvalidArgument, None),
            4 => (Self::DeadlineExceeded, None),
            5 => (Self::NotFound, None),
            6 => (Self::AlreadyExists, None),
            7 => (Self::PermissionDenied, None),
            8 => (Self::ResourceExhausted, None),
            9 => (Self::FailedPrecondition, None),
            10 => (Self::Aborted, None),
            11 => (Self::OutOfRange, None),
            12 => (Self::Unimplemented, None),
            13 => (Self::Internal, None),
            14 => (Self::Unavailable, None),
            15 => (Self::DataLoss, None),
            16 => (Self::Unauthenticated, None),
            other => (Self::Unknown, Some(other)),
        }
    }

    pub fn as_i32(self) -> i32 {
        self as i32
    }

    /// Non-binding retry guidance (rpc.md §1I, reliability.md).
    /// Labels are guidance only, never wire values.
    pub fn retry_class(self) -> RetryClass {
        match self {
            Self::Unavailable => RetryClass::ConditionallyRetryable,
            Self::DeadlineExceeded => RetryClass::UnknownOutcome,
            Self::ResourceExhausted => RetryClass::ConditionallyRetryable,
            Self::Cancelled
            | Self::InvalidArgument
            | Self::NotFound
            | Self::AlreadyExists
            | Self::FailedPrecondition
            | Self::Aborted
            | Self::OutOfRange
            | Self::Unimplemented
            | Self::DataLoss
            | Self::PermissionDenied
            | Self::Unauthenticated => RetryClass::NotSafeToRetry,
            Self::Internal | Self::Unknown | Self::Unspecified => RetryClass::UnknownOutcome,
        }
    }
}

/// Retry guidance labels (informative only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    /// Read-only / inherently idempotent; fresh request_id per attempt.
    SafeToRetry,
    /// Idempotent or operation-keyed with backoff.
    ConditionallyRetryable,
    /// Terminal result; re-issue only as a new application decision.
    NotSafeToRetry,
    /// Handler may have executed; retry only idempotent/with-key.
    UnknownOutcome,
}

/// Public typed failure: `{code, message}` (errors.proto `RpcError`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcError {
    pub code: ErrorCode,
    /// Raw numeric code when a future value mapped to Unknown.
    pub raw_code: Option<i32>,
    /// Public human-readable text. Never credentials or private diagnostics.
    pub message: String,
}

impl RpcError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            raw_code: None,
            message: message.into(),
        }
    }
}

/// All failures surfaced by the Syntra core and client.
#[derive(Debug, Error, Clone)]
pub enum SyntraError {
    /// A terminal `RpcResult{error}` from the host.
    #[error("rpc failed [{code:?}]: {message}")]
    Rpc {
        code: ErrorCode,
        raw_code: Option<i32>,
        message: String,
    },
    /// Transport loss / shutdown. Handler outcome is UNKNOWN (SNTR-REL-009).
    /// Retry only if idempotent or carrying an application operation key.
    #[error("transport unavailable (outcome unknown): {0}")]
    Unavailable(String),
    /// Local deadline expiry. Outcome unknown; same retry rule as Unavailable.
    #[error("deadline exceeded (outcome unknown): {0}")]
    DeadlineExceeded(String),
    /// Caller cancelled; re-issue only as a new decision.
    #[error("cancelled: {0}")]
    Cancelled(String),
    /// Handshake failed; connection is CLOSED (SNTR-HS-040).
    #[error("handshake failed [{code:?}]: {message}")]
    Handshake { code: ErrorCode, message: String },
    /// Local protocol violation (malformed envelope, bad method, size).
    #[error("protocol violation [{code:?}]: {message}")]
    Protocol { code: ErrorCode, message: String },
    /// Too many concurrent RPCs or saturated queues.
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
    /// Transport-level failure (WS close, TLS, IO).
    #[error("transport: {0}")]
    Transport(String),
    /// Connection closed; no new RPCs accepted.
    #[error("connection closed: {0}")]
    Closed(String),
}

impl SyntraError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Rpc { code, .. } => *code,
            Self::Unavailable(_) => ErrorCode::Unavailable,
            Self::DeadlineExceeded(_) => ErrorCode::DeadlineExceeded,
            Self::Cancelled(_) => ErrorCode::Cancelled,
            Self::Handshake { code, .. } => *code,
            Self::Protocol { code, .. } => *code,
            Self::ResourceExhausted(_) => ErrorCode::ResourceExhausted,
            Self::Transport(_) => ErrorCode::Unavailable,
            Self::Closed(_) => ErrorCode::Unavailable,
        }
    }

    /// True when the handler may have executed and the outcome is unknown.
    /// Such failures MUST NOT be blindly retried for non-idempotent ops.
    pub fn is_unknown_outcome(&self) -> bool {
        matches!(
            self,
            Self::Unavailable(_) | Self::DeadlineExceeded(_) | Self::Closed(_)
        ) || matches!(self, Self::Rpc { code, .. } if matches!(code, ErrorCode::Internal | ErrorCode::Unknown))
    }
}

impl From<RpcError> for SyntraError {
    fn from(e: RpcError) -> Self {
        SyntraError::Rpc {
            code: e.code,
            raw_code: e.raw_code,
            message: e.message,
        }
    }
}
