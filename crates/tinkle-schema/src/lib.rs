//! `tinkle-schema`: Tinkle v1 schema bindings above Syntra.
//!
//! Owns Tinkle semantics only: prost types for `tinkle.v1`, canonical
//! Syntra method identifiers (`/tinkle.v1.<Service>/<Method>`), realtime
//! batch/cursor validation (`REALTIME.md`), and checkpoint helpers.
//! It never owns Syntra framing, handshake, or transport. Ciphertext, if
//! E2EE is integrated later, is constructed at this layer before entering
//! Syntra (Syntra carries it opaquely).

pub mod methods;
pub mod proto;
pub mod realtime;

pub use realtime::{validate_batch, validate_notification, CursorDecision, StreamCursors};
