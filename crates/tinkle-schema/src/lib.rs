//! `tinkle-schema`: Tinkle v1 schema bindings above Syntra.
//!
//! Owns Tinkle semantics only: prost types for `tinkle.v1`, canonical
//! method identifiers (`/tinkle.v1.<Service>/<Method>`), and validation
//! for target-changes sync (per-conversation mutation pages, message
//! refetch batching, and ephemeral connectivity observations).
//! It never owns Syntra framing, handshake, or transport.

pub mod methods;
pub mod proto;
pub mod sync;

pub use sync::{
    accept_seq_decision, chunk_message_ids, fresher_body, target_from_parts, target_key_for,
    validate_changes_page, validate_connectivity_notification, AcceptSeqDecision, PageMore,
    SyncError, ValidatedChangesPage, GET_HISTORY_METHOD, GET_MESSAGES_METHOD,
    GET_TARGET_CHANGES_METHOD, MAX_MESSAGE_IDS_PER_REQUEST, MAX_TARGET_CHANGES_LIMIT,
    OBSERVE_METHOD,
};
