//! Target-changes sync helpers for `tinkle.v1`.
//!
//! Durable update replay (`ReadUpdates`/`FollowUpdates`), the `Update`
//! envelope, and `RebuildNeeded` retired from the public contract. Sync now
//! reconciles per-conversation mutation journals with unary
//! `MessagesService/ListMessageChanges` pages and fetches bodies with unary
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
pub const LIST_MESSAGE_CHANGES_METHOD: &str = "/tinkle.v1.MessagesService/ListMessageChanges";
/// Fetch conversation history windows (unary).
pub const GET_HISTORY_METHOD: &str = "/tinkle.v1.MessagesService/GetHistory";

/// Max message IDs per `GetMessages` request: larger batches are rejected
/// rather than truncated, so callers must chunk.
pub const MAX_MESSAGE_IDS_PER_REQUEST: usize = 100;
/// Max `limit` per `ListMessageChanges` request (`1..500`; zero selects the
/// deployment default).
pub const MAX_TARGET_CHANGES_LIMIT: i32 = 500;

use prost::Message as _;
use thiserror::Error;

use crate::proto::{
    target, ConnectivityChanged, ListMessageChangesResponse, Message as TinkleMessage, Target,
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
    match notification(payload)? {
        Notification::Connectivity(value) => Ok(value),
        _ => Err(SyncError::Malformed(
            "not a ConnectivityChanged notification".into(),
        )),
    }
}

/// Exact application notification binding; transport framing stays opaque to this crate.
pub enum Notification {
    /// Validated bare connectivity observation inside google.protobuf.Any.
    Connectivity(ConnectivityChanged),
    /// Validated session refresh hint, not an authoritative session projection.
    SessionChanged(crate::proto::SessionChanged),
    /// Body-free message reconciliation hint, with independently validated optional presentation.
    MessageChanged(crate::proto::MessageChanged),
    /// Existing opaque conversation reconciliation hint.
    Hint,
}

/// Dispatch by exact Any type URL, never by trying unrelated protobuf decoders.
pub fn notification(payload: &[u8]) -> Result<Notification, SyncError> {
    let Ok(any) = prost_types::Any::decode(payload) else {
        return Ok(Notification::Hint);
    };
    match any.type_url.as_str() {
        "type.googleapis.com/tinkle.v1.MessageChanged" => {
            let mut value = crate::proto::MessageChanged::decode(any.value.as_slice())
                .map_err(|_| SyncError::Malformed("invalid MessageChanged".into()))?;
            let kind = TargetChangeKind::try_from(value.change_kind)
                .map_err(|_| SyncError::InvalidUpdate("invalid message hint kind".into()))?;
            if value.message_id <= 0
                || value.body_revision <= 0
                || !matches!(
                    value.target.as_ref().and_then(|t| t.kind.as_ref()),
                    Some(target::Kind::UserId(1..) | target::Kind::ChatId(1..))
                )
                || kind == TargetChangeKind::Unspecified
                || (value.body_invalidated
                    && matches!(kind, TargetChangeKind::Deleted | TargetChangeKind::Revoked))
            {
                return Err(SyncError::InvalidUpdate(
                    "invalid message hint identity".into(),
                ));
            }
            // Bad/unsupported presentation cannot suppress valid reconciliation or create a body.
            if value.reaction_effect.as_ref().is_some_and(|e| {
                kind != TargetChangeKind::Reaction
                    || value.body_invalidated
                    || !valid_reaction_effect(e)
            }) {
                value.reaction_effect = None;
            }
            Ok(Notification::MessageChanged(value))
        }
        "type.googleapis.com/tinkle.v1.ConnectivityChanged" => {
            let value = ConnectivityChanged::decode(any.value.as_slice())
                .map_err(|_| SyncError::Malformed("invalid ConnectivityChanged".into()))?;
            if value.user_id <= 0 {
                return Err(SyncError::InvalidUpdate(
                    "connectivity user id must be positive".into(),
                ));
            }
            Ok(Notification::Connectivity(value))
        }
        "type.googleapis.com/tinkle.v1.SessionChanged" => {
            let value = crate::proto::SessionChanged::decode(any.value.as_slice())
                .map_err(|_| SyncError::Malformed("invalid SessionChanged".into()))?;
            if !session_id(&value.session_id) {
                return Err(SyncError::InvalidUpdate(
                    "invalid session hint identifier".into(),
                ));
            }
            Ok(Notification::SessionChanged(value))
        }
        _ => Ok(Notification::Hint),
    }
}

/// Validate bounded, exact effect identities without a clock or catalog lookup. Runtime admission
/// separately checks the original expiry window. Presentation never changes body/journal evidence.
pub fn valid_reaction_effect(effect: &crate::proto::ReactionEffect) -> bool {
    if effect.actor_id <= 0
        || !session_id(&effect.operation_id)
        || !(1..=8).contains(&effect.reactions.len())
        || !effect.expires_at.as_ref().is_some_and(|t| {
            (0..=253_402_300_799).contains(&t.seconds) && (0..1_000_000_000).contains(&t.nanos)
        })
    {
        return false;
    }
    let mut choices = std::collections::HashSet::new();
    effect.reactions.iter().all(|r| {
        r.custom_emoji_id >= 0
            && !r.emoticon.is_empty()
            && r.emoticon.len() <= 64
            && if r.custom_emoji_id > 0 {
                r.asset_version.is_some_and(|v| v > 0)
                    && choices.insert((r.custom_emoji_id, r.asset_version, ""))
            } else {
                r.asset_version.is_none() && choices.insert((0, None, r.emoticon.as_str()))
            }
    })
}

/// Canonical nonzero neutral session UUID, not an enrollment handle or credential.
pub fn session_id(value: &str) -> bool {
    value.len() == 36
        && value != "00000000-0000-0000-0000-000000000000"
        && value.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}

/// Whether a `ListMessageChanges` page reports continuation beyond
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

/// A validated `ListMessageChanges` page: changes ordered strictly ascending
/// by `accept_seq`, with the cursor to continue from.
#[derive(Debug, Clone)]
pub struct ValidatedChangesPage {
    pub changes: Vec<TargetChange>,
    pub last_accept_seq: i64,
    pub more: PageMore,
}

/// Validate one `ListMessageChanges` page against the requesting cursor.
///
/// Enforces: every change `accept_seq` is strictly ascending and strictly
/// greater than `after_accept_seq`; `last_accept_seq` never moves backward
/// and covers all returned markers. Filtered pages may advance the stable
/// processed horizon without a marker. Body/metadata changes carry current
/// bodies unless explicitly body-invalidated; removals carry identity only.
/// Invalidation requires a positive nonterminal revision and no body.
/// Continuation always requires progress.
pub fn validate_changes_page(
    after_accept_seq: i64,
    resp: &ListMessageChangesResponse,
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
        let more = match resp.more_available {
            Some(true) if resp.last_accept_seq == after_accept_seq => {
                return Err(SyncError::InvalidUpdate(
                    "empty page must not report more_available".into(),
                ));
            }
            Some(true) => PageMore::Yes,
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
        if change.message_id <= 0 || change.message_revision.is_some_and(|v| v <= 0) {
            return Err(SyncError::InvalidUpdate(
                "invalid message identity/revision".into(),
            ));
        }
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
        if change.body_invalidated {
            if change.message.is_some()
                || change.message_revision.is_none()
                || matches!(
                    kind,
                    TargetChangeKind::Deleted
                        | TargetChangeKind::Revoked
                        | TargetChangeKind::Unspecified
                )
            {
                return Err(SyncError::InvalidUpdate(
                    "body invalidation requires a versioned body-free nonterminal identity".into(),
                ));
            }
            continue;
        }
        match kind {
            TargetChangeKind::Edited
            | TargetChangeKind::Pin
            | TargetChangeKind::Reaction
            | TargetChangeKind::Poll
            | TargetChangeKind::Ack => match &change.message {
                None => {
                    return Err(SyncError::InvalidUpdate(format!(
                        "edited change for message {} must carry the current body",
                        change.message_id
                    )));
                }
                Some(body) if body.message_id != change.message_id => {
                    return Err(SyncError::InvalidUpdate(format!(
                        "edited body id {} must match change {}",
                        body.message_id, change.message_id
                    )));
                }
                Some(body) => {
                    if body.body_revision.is_some_and(|v| v <= 0)
                        || matches!((change.message_revision,body.body_revision),(Some(event),Some(body)) if body<event)
                    {
                        return Err(SyncError::InvalidUpdate(
                            "hydrated body revision predates its marker".into(),
                        ));
                    }
                }
            },
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
    if resp.last_accept_seq < prev {
        return Err(SyncError::InvalidUpdate(format!(
            "last_accept_seq {} must cover last change accept_seq {prev}",
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
    /// The fetched token is strictly below the applied one.
    DuplicateOrStale,
    /// No ranking from tokens alone: either side is absent, or the tokens
    /// tie (stable across edits). Callers fall back to `edit_date`
    /// (`fresher_body`); absent tokens never vote on dedup.
    UnknownOrder,
}

/// Rank message acceptance positions within one conversation. This does not rank copies of
/// the same body: use `fresher_body` for revisions/date fallback, regardless of these tokens.
pub fn accept_seq_decision(applied: Option<i64>, got: Option<i64>) -> AcceptSeqDecision {
    match (applied, got) {
        (_, None) => AcceptSeqDecision::UnknownOrder,
        (None, Some(_)) => AcceptSeqDecision::UnknownOrder,
        (Some(a), Some(g)) => {
            if g > a {
                AcceptSeqDecision::Newer
            } else if g == a {
                AcceptSeqDecision::UnknownOrder
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
/// the strictly greater body revision when both report it, otherwise the later
/// edit_date. Equal revisions/dates never replace a cached copy.
pub fn fresher_body(a: &TinkleMessage, b: &TinkleMessage) -> bool {
    if let (Some(a), Some(b)) = (a.body_revision, b.body_revision) {
        return a > 0 && b > a;
    }
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
