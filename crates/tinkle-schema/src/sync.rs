//! Target-changes sync helpers for `tinkle.v1`.
//!
//! Durable update replay (`ReadUpdates`/`FollowUpdates`), the `Update`
//! envelope, and `RebuildNeeded` retired from the public contract. Sync now
//! reconciles per-conversation mutation journals with unary
//! `MessagesService/GetTargetChanges` pages and fetches bodies with unary
//! `MessagesService/GetMessages`. Connectivity observations stay ephemeral:
//! `UpdatesService/ObserveConnectivity` notifications carry
//! `ConnectivityChanged` bytes and must be discarded on disconnect.
//!
//! This module owns method names and response validation. It never owns
//! transport framing or connection lifecycle.

/// Start connectivity observations over Syntra; the server pushes
/// `ConnectivityChanged` notifications after the correlated result.
pub const OBSERVE_METHOD: &str = "/tinkle.v1.UpdatesService/ObserveConnectivity";
/// Fetch specific messages in one conversation (unary).
pub const GET_MESSAGES_METHOD: &str = "/tinkle.v1.MessagesService/GetMessages";
/// List retained conversation mutations strictly after one accept position
/// (unary).
pub const GET_TARGET_CHANGES_METHOD: &str = "/tinkle.v1.MessagesService/GetTargetChanges";
/// Fetch conversation history windows (unary).
pub const GET_HISTORY_METHOD: &str = "/tinkle.v1.MessagesService/GetHistory";

/// Max message IDs per `GetMessages` request: larger batches are rejected
/// rather than truncated, so callers must chunk.
pub const MAX_MESSAGE_IDS_PER_REQUEST: usize = 100;
/// Max `limit` per `GetTargetChanges` request (`1..500`; zero selects the
/// deployment default).
pub const MAX_TARGET_CHANGES_LIMIT: i32 = 500;

use prost::Message as _;
use thiserror::Error;

use crate::proto::{
    target, ConnectivityChanged, GetTargetChangesResponse, Message as TinkleMessage, Target,
    TargetChange, TargetChangeKind,
};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SyncError {
    #[error("malformed payload: {0}")]
    Malformed(String),
    #[error("invalid sync data: {0}")]
    InvalidUpdate(String),
}

/// Decode one `ConnectivityChanged` observation from a Syntra notification
/// payload. Observations carry no replay cursor and must never advance sync
/// checkpoints.
pub fn validate_connectivity_notification(
    payload: &[u8],
) -> Result<ConnectivityChanged, SyncError> {
    ConnectivityChanged::decode(payload)
        .map_err(|_| SyncError::Malformed("cannot decode ConnectivityChanged".into()))
}

/// Whether a `GetTargetChanges` page reports continuation beyond
/// `last_accept_seq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageMore {
    /// Server confirms a next page exists; continue with `after_accept_seq`
    /// equal to `last_accept_seq`.
    Yes,
    /// Server confirms the caller is caught up to the retained head.
    No,
    /// The page carries no continuation report: unknown, never read as
    /// caught up.
    Unknown,
}

/// A validated `GetTargetChanges` page: changes ordered strictly ascending
/// by `accept_seq`, with the cursor to continue from.
#[derive(Debug, Clone)]
pub struct ValidatedChangesPage {
    pub changes: Vec<TargetChange>,
    pub last_accept_seq: i64,
    pub more: PageMore,
}

/// Validate one `GetTargetChanges` page against the requesting cursor.
///
/// Enforces: every change `accept_seq` is strictly ascending and strictly
/// greater than `after_accept_seq`; `last_accept_seq` never moves backward
/// (it equals the last change's `accept_seq` on a non-empty page and equals
/// `after_accept_seq` on an empty page); edited changes carry the current
/// body while deleted/revoked changes carry identity and kind only; and an
/// empty page must never claim `more_available`.
pub fn validate_changes_page(
    after_accept_seq: i64,
    resp: &GetTargetChangesResponse,
) -> Result<ValidatedChangesPage, SyncError> {
    if after_accept_seq < 0 {
        return Err(SyncError::InvalidUpdate(format!(
            "after_accept_seq must be non-negative, got {after_accept_seq}"
        )));
    }
    if resp.last_accept_seq < after_accept_seq {
        return Err(SyncError::InvalidUpdate(format!(
            "last_accept_seq {} below after_accept_seq {after_accept_seq}",
            resp.last_accept_seq
        )));
    }
    if resp.changes.is_empty() {
        if resp.last_accept_seq != after_accept_seq {
            return Err(SyncError::InvalidUpdate(format!(
                "empty page must echo after_accept_seq {after_accept_seq}, got {}",
                resp.last_accept_seq
            )));
        }
        // An empty page discloses no change and no progress; claiming more
        // exists without advancing the cursor could never terminate.
        let more = match resp.more_available {
            Some(true) => {
                return Err(SyncError::InvalidUpdate(
                    "empty page must not report more_available".into(),
                ));
            }
            Some(false) => PageMore::No,
            None => PageMore::Unknown,
        };
        return Ok(ValidatedChangesPage {
            changes: Vec::new(),
            last_accept_seq: resp.last_accept_seq,
            more,
        });
    }
    let mut prev = after_accept_seq;
    for change in &resp.changes {
        if change.accept_seq <= prev {
            return Err(SyncError::InvalidUpdate(format!(
                "changes must ascend strictly above {after_accept_seq}, got {}",
                change.accept_seq
            )));
        }
        prev = change.accept_seq;
        let kind = TargetChangeKind::try_from(change.kind).map_err(|_| {
            SyncError::InvalidUpdate(format!("unknown target change kind {}", change.kind))
        })?;
        match kind {
            TargetChangeKind::Edited => {
                if change.message.is_none() {
                    return Err(SyncError::InvalidUpdate(format!(
                        "edited change for message {} must carry the current body",
                        change.message_id
                    )));
                }
            }
            TargetChangeKind::Deleted | TargetChangeKind::Revoked => {
                if change.message.is_some() {
                    return Err(SyncError::InvalidUpdate(format!(
                        "deleted/revoked change for message {} must not carry a body",
                        change.message_id
                    )));
                }
            }
            TargetChangeKind::Unspecified => {
                return Err(SyncError::InvalidUpdate(format!(
                    "change for message {} has unspecified kind",
                    change.message_id
                )));
            }
        }
    }
    if resp.last_accept_seq != prev {
        return Err(SyncError::InvalidUpdate(format!(
            "last_accept_seq {} must equal last change accept_seq {prev}",
            resp.last_accept_seq
        )));
    }
    let more = match resp.more_available {
        Some(true) => PageMore::Yes,
        Some(false) => PageMore::No,
        None => PageMore::Unknown,
    };
    Ok(ValidatedChangesPage {
        changes: resp.changes.clone(),
        last_accept_seq: resp.last_accept_seq,
        more,
    })
}

/// Ordering of a message `accept_seq` token against the applied one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptSeqDecision {
    /// The fetched token is strictly newer than the applied one.
    Newer,
    /// The fetched token is at or below the applied one.
    DuplicateOrStale,
    /// No fetched token to rank (absent tokens never vote on dedup).
    UnknownOrder,
}

/// Rank a fetched `accept_seq` against the applied one. Tokens are
/// conversation-local and stable across edits; absent tokens order with
/// date and never count as dedup votes.
pub fn accept_seq_decision(applied: Option<i64>, got: Option<i64>) -> AcceptSeqDecision {
    match (applied, got) {
        (_, None) => AcceptSeqDecision::UnknownOrder,
        (None, Some(_)) => AcceptSeqDecision::UnknownOrder,
        (Some(a), Some(g)) => {
            if g > a {
                AcceptSeqDecision::Newer
            } else {
                AcceptSeqDecision::DuplicateOrStale
            }
        }
    }
}

/// Split message IDs into request batches of at most
/// [`MAX_MESSAGE_IDS_PER_REQUEST`], preserving order.
pub fn chunk_message_ids(ids: &[i64]) -> Vec<Vec<i64>> {
    ids.chunks(MAX_MESSAGE_IDS_PER_REQUEST)
        .map(|chunk| chunk.to_vec())
        .collect()
}

/// Whether body `b` is fresher than body `a` for the same message ID: keep
/// the copy with the later `edit_date`. An absent `edit_date` never beats a
/// dated one.
pub fn fresher_body(a: &TinkleMessage, b: &TinkleMessage) -> bool {
    match (&a.edit_date, &b.edit_date) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(x), Some(y)) => (y.seconds, y.nanos) > (x.seconds, x.nanos),
    }
}

/// Canonical storage key for a conversation target.
pub fn target_key_for(kind: &target::Kind) -> String {
    match kind {
        target::Kind::UserId(id) => format!("target:user:{id}"),
        target::Kind::ChatId(id) => format!("target:room:{id}"),
    }
}

/// Build a `Target` from a kind name and ID. Accepts `"user"` for direct
/// messages and `"room"`/`"chat"` for group conversations; IDs must be
/// positive.
pub fn target_from_parts(kind_str: &str, id: i64) -> Result<Target, SyncError> {
    if id <= 0 {
        return Err(SyncError::InvalidUpdate(format!(
            "target id must be positive, got {id}"
        )));
    }
    let kind = match kind_str {
        "user" => target::Kind::UserId(id),
        "room" | "chat" => target::Kind::ChatId(id),
        other => {
            return Err(SyncError::InvalidUpdate(format!(
                "unknown target kind '{other}'"
            )));
        }
    };
    Ok(Target { kind: Some(kind) })
}
