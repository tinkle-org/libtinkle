//! `tinkle-schema`: Tinkle v1 schema bindings above Syntra.
//!
//! Owns Tinkle semantics only: prost types for `tinkle.v1`, canonical
//! method identifiers (`/tinkle.v1.<Service>/<Method>`), sync
//! validation for durable updates, and checkpoint helpers.
//! It never owns Syntra framing, handshake, or transport.

pub mod methods;
pub mod proto;
pub mod sync;

pub use sync::{
    is_canonical_stream, position_order, validate_batch, validate_follow_response,
    validate_notification, validate_update, CursorDecision, StreamCursors, SyncBinding, SyncError,
    ValidatedFollow, ValidatedUpdate, FOLLOW_METHOD, OBSERVE_METHOD, READ_METHOD,
};
