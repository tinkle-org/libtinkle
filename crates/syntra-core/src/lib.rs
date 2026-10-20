//! `syntra-core`: generic Syntra v1 protocol runtime.
//!
//! This crate owns Syntra mechanics only: envelope codec, validation
//! (`SNTR-WIRE-*`, `SNTR-SER-*`), version negotiation, the six-state
//! connection machine (`SNTR-HS-001`), request-id allocation, pending-RPC
//! correlation, timeouts, cancellation, typed errors, and notification
//! routing. It never parses Tinkle application semantics and never
//! decrypts application ciphertext.
//!
//! Wire sources: vendored copies of `buf.build/tinklecorp/syntra`
//! (see `PROTO_PINNING.md`). Generated prost types are data structures
//! only (ADR-010); all behavior here is implemented against the
//! specifications in `syntra/specifications/`.

pub mod codec;
pub mod error;
pub mod frame;
pub mod handshake;
pub mod ids;
pub mod method;
pub mod pending;
pub mod proto;
pub mod state;

#[cfg(feature = "runtime-tokio")]
pub mod connection;

pub use codec::{decode_frame, encode_frame, MAX_FRAME_BYTES};
pub use error::{ErrorCode, RpcError, SyntraError};
pub use frame::{validate_frame, FrameKind, ValidationContext};
pub use handshake::{
    build_hello, validate_hello_range, HANDSHAKE_TIMEOUT_DEFAULT, MAX_VERSION, MIN_VERSION,
};
pub use ids::RequestIdAllocator;
pub use method::validate_method;
pub use state::ConnectionState;

#[cfg(feature = "runtime-tokio")]
pub use connection::{Connection, ConnectionConfig};
pub use pending::PendingOutcome;
