//! Syntra follow binding for `tinkle.v1.UpdatesService`.
//!
//! Actual gRPC carries `ReadUpdates` catch-up. Ongoing delivery uses the
//! existing Syntra WebSocket: unary subscription RPCs start server-side
//! watches, and durable changes arrive as `Notification` frames carrying
//! `FollowUpdatesResponse` or `ObserveConnectivityResponse` bytes.
//!
//! This module owns method names, routing envelopes, and payload codecs.
//! It never owns transport framing or connection lifecycle.

/// Unary catch-up over actual gRPC.
pub const READ_METHOD: &str = "/tinkle.v1.UpdatesService/ReadUpdates";
/// Start a follow watch over Syntra; server pushes `FollowUpdatesResponse`
/// notifications after the correlated result.
pub const FOLLOW_METHOD: &str = "/tinkle.v1.UpdatesService/FollowUpdates";
/// Start connectivity observations over Syntra; server pushes
/// `ObserveConnectivityResponse` notifications after the correlated result.
pub const OBSERVE_METHOD: &str = "/tinkle.v1.UpdatesService/ObserveConnectivity";

/// Follow binding envelope version.
pub const BINDING_VERSION: u32 = 1;

/// Binding descriptor shared by SDK and gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncBinding {
    /// Envelope version negotiated by the subscription RPC.
    pub version: u32,
}

impl Default for SyncBinding {
    fn default() -> Self {
        Self {
            version: BINDING_VERSION,
        }
    }
}

impl SyncBinding {
    /// Method name for starting an update follow.
    pub fn follow_method() -> &'static str {
        FOLLOW_METHOD
    }

    /// Method name for starting connectivity observations.
    pub fn observe_method() -> &'static str {
        OBSERVE_METHOD
    }

    /// Method name for gRPC catch-up.
    pub fn read_method() -> &'static str {
        READ_METHOD
    }
}

use std::collections::HashMap;

use prost::Message;
use thiserror::Error;

use crate::proto::{
    follow_updates_response::Result as FollowResult, update::Payload as UpdatePayload,
    FollowUpdatesResponse, Update,
};

/// Max signed 64-bit cursor (`0..9223372036854775807`).
pub const MAX_CURSOR: u64 = i64::MAX as u64;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SyncError {
    #[error("malformed update: {0}")]
    Malformed(String),
    #[error("invalid update: {0}")]
    InvalidUpdate(String),
    #[error("cursor out of range")]
    CursorOutOfRange,
    #[error("gap detected: expected {expected}, got {got} on {stream}")]
    Gap {
        stream: String,
        expected: u64,
        got: u64,
    },
    #[error("stale cursor: {got} <= applied {applied} on {stream}")]
    Stale {
        stream: String,
        applied: u64,
        got: u64,
    },
    #[error("rebuild required on {stream}")]
    RebuildRequired { stream: String, head: Option<u64> },
}

// Sync validation errors are `SyncError`.

/// What a validated follow payload carries.
#[derive(Debug, Clone)]
pub enum ValidatedFollow {
    /// One durable update ready for application.
    Update(Box<ValidatedUpdate>),
    /// Terminal rebuild signal.
    Rebuild { stream: String, head: Option<u64> },
    /// Progress-only heartbeat; never advances applied checkpoint.
    Progress { stream: String, head: Option<u64> },
}

/// A fully validated update ready for atomic commit.
#[derive(Debug, Clone)]
pub struct ValidatedUpdate {
    pub stream: String,
    pub cursor: u64,
    pub update: Update,
}

/// Backwards-compatible alias for the previous batch type name.
pub type ValidatedBatch = ValidatedUpdate;
/// Backwards-compatible alias for notification validation.
pub type ValidatedNotification = ValidatedFollow;

pub fn is_canonical_stream(s: &str) -> bool {
    let (kind, id) = match s.split_once(':') {
        Some(v) => v,
        None => return false,
    };
    if kind != "user" && kind != "room" {
        return false;
    }
    matches!(id.parse::<i64>(), Ok(v) if v > 0)
}

fn is_lowercase_uuid(s: &str) -> bool {
    if s.is_empty() || s.len() > 64 {
        return false;
    }
    // Accept canonical lowercase UUID/text: hex + dashes, lowercase only.
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase() || c == '-')
        && s.chars().any(|c| c.is_ascii_hexdigit())
}

/// Validate one durable `Update` before any effects.
pub fn validate_update(update: &Update) -> Result<ValidatedUpdate, SyncError> {
    if update.stream.is_empty() || !is_canonical_stream(&update.stream) {
        return Err(SyncError::InvalidUpdate(format!(
            "bad stream '{}'",
            update.stream
        )));
    }
    if update.cursor == 0 || update.cursor > MAX_CURSOR {
        return Err(SyncError::CursorOutOfRange);
    }
    let payload = update
        .payload
        .as_ref()
        .ok_or_else(|| SyncError::InvalidUpdate("update payload must be set".to_string()))?;
    match payload {
        UpdatePayload::ContentOmitted(_) => {
            if !update.event_id.is_empty() {
                return Err(SyncError::InvalidUpdate(
                    "content_omitted must not carry source identity".to_string(),
                ));
            }
            if update.occurred_at.is_some()
                || update.source_seq.is_some()
                || update.source_revision.is_some()
            {
                return Err(SyncError::InvalidUpdate(
                    "content_omitted must not carry timestamps/markers".to_string(),
                ));
            }
        }
        UpdatePayload::MessageRefresh(m) => {
            if update.event_id.is_empty() {
                return Err(SyncError::InvalidUpdate("event_id must be set".to_string()));
            }
            if !is_lowercase_uuid(&update.event_id) {
                return Err(SyncError::InvalidUpdate(
                    "event_id must be canonical".to_string(),
                ));
            }
            if m.message.is_none() {
                return Err(SyncError::InvalidUpdate(
                    "message_refresh.message must be set".to_string(),
                ));
            }
            if let Some(v) = update.source_seq {
                if v < 0 {
                    return Err(SyncError::InvalidUpdate(
                        "source_seq must be nonnegative".to_string(),
                    ));
                }
            }
            if let Some(v) = update.source_revision {
                if v < 0 {
                    return Err(SyncError::InvalidUpdate(
                        "source_revision must be nonnegative".to_string(),
                    ));
                }
            }
        }
        UpdatePayload::SendAttemptFailed(m) => {
            if update.event_id.is_empty() {
                return Err(SyncError::InvalidUpdate("event_id must be set".to_string()));
            }
            if m.random_id == 0 {
                return Err(SyncError::InvalidUpdate(
                    "random_id must be nonzero".to_string(),
                ));
            }
        }
        UpdatePayload::ReadStateInvalidated(_) => {
            if update.event_id.is_empty() {
                return Err(SyncError::InvalidUpdate("event_id must be set".to_string()));
            }
        }
    }
    Ok(ValidatedUpdate {
        stream: update.stream.clone(),
        cursor: update.cursor,
        update: update.clone(),
    })
}

/// Decode one `FollowUpdatesResponse` from a Syntra `Notification.payload`.
/// Never try multiple decoders; unsupported shapes stop without progress.
pub fn validate_notification(payload: &[u8]) -> Result<ValidatedFollow, SyncError> {
    validate_follow_response(payload)
}

pub fn validate_follow_response(payload: &[u8]) -> Result<ValidatedFollow, SyncError> {
    let resp = FollowUpdatesResponse::decode(payload)
        .map_err(|_| SyncError::Malformed("cannot decode FollowUpdatesResponse".into()))?;
    if resp.stream.is_empty() || !is_canonical_stream(&resp.stream) {
        return Err(SyncError::InvalidUpdate(format!(
            "bad stream '{}'",
            resp.stream
        )));
    }
    match resp.result {
        Some(FollowResult::Update(u)) => {
            if u.stream != resp.stream {
                return Err(SyncError::InvalidUpdate(
                    "update stream must equal response stream".into(),
                ));
            }
            if let Some(h) = resp.head {
                if h > MAX_CURSOR {
                    return Err(SyncError::CursorOutOfRange);
                }
                if u.cursor > h {
                    return Err(SyncError::InvalidUpdate("update beyond head".into()));
                }
            }
            let v = validate_update(&u)?;
            Ok(ValidatedFollow::Update(Box::new(v)))
        }
        Some(FollowResult::RebuildNeeded(_)) => Ok(ValidatedFollow::Rebuild {
            stream: resp.stream,
            head: resp.head,
        }),
        None => Ok(ValidatedFollow::Progress {
            stream: resp.stream,
            head: resp.head,
        }),
    }
}

/// Validate a single `Update` as a one-item batch (compat helper).
/// Previous batches of 1..4 events no longer exist; each update commits alone.
pub fn validate_batch(update: &Update) -> Result<ValidatedUpdate, SyncError> {
    validate_update(update)
}

/// Position `order` = cursor + source_seq as zero-padded 20-digit
/// unsigned decimals (40 ASCII digits). Absent source_seq means zero.
pub fn position_order(cursor: u64, source_seq: Option<i64>) -> String {
    let seq = source_seq.unwrap_or(0).max(0) as u64;
    format!("{cursor:020}{seq:020}")
}

/// Cursor decision for `(stream, applied, got)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorDecision {
    Duplicate,
    Next,
    Gap,
    Stale,
}

pub fn decide_cursor(applied: u64, got: u64) -> CursorDecision {
    if got == applied {
        CursorDecision::Duplicate
    } else if got < applied {
        CursorDecision::Stale
    } else if got.checked_sub(applied) == Some(1) {
        CursorDecision::Next
    } else {
        CursorDecision::Gap
    }
}

/// Per-stream last-fully-applied cursors within one principal/history
/// scope. Deduplicate by `(stream, cursor)`, not content. Connectivity
/// observations are never checkpointed; clear on disconnect.
#[derive(Debug, Default)]
pub struct StreamCursors {
    applied: HashMap<String, u64>,
    floor: HashMap<String, u64>,
}

impl StreamCursors {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, stream: &str) -> u64 {
        self.applied.get(stream).copied().unwrap_or(0)
    }

    pub fn decision(&self, stream: &str, cursor: u64) -> CursorDecision {
        decide_cursor(self.get(stream), cursor)
    }

    pub fn commit(&mut self, stream: &str, cursor: u64) -> Result<CursorDecision, SyncError> {
        if cursor == 0 || cursor > MAX_CURSOR {
            return Err(SyncError::CursorOutOfRange);
        }
        match self.decision(stream, cursor) {
            CursorDecision::Next => {
                self.applied.insert(stream.to_string(), cursor);
                Ok(CursorDecision::Next)
            }
            CursorDecision::Duplicate => Ok(CursorDecision::Duplicate),
            CursorDecision::Gap => Err(SyncError::Gap {
                stream: stream.to_string(),
                expected: self.get(stream).saturating_add(1),
                got: cursor,
            }),
            CursorDecision::Stale => Err(SyncError::Stale {
                stream: stream.to_string(),
                applied: self.get(stream),
                got: cursor,
            }),
        }
    }

    pub fn on_reset(&mut self, stream: &str) {
        self.applied.remove(stream);
    }

    pub fn hydrate(&mut self, stream: &str, cursor: u64) {
        if cursor == 0 {
            self.applied.remove(stream);
        } else {
            self.applied.insert(stream.to_string(), cursor);
        }
    }

    pub fn on_disconnect_presence_cleared(&self) {}

    pub fn set_floor(&mut self, stream: &str, floor: u64) {
        self.floor.insert(stream.to_string(), floor);
    }

    pub fn validate_after(
        &self,
        stream: &str,
        after: u64,
        head: Option<u64>,
    ) -> Result<(), SyncError> {
        if after > MAX_CURSOR {
            return Err(SyncError::CursorOutOfRange);
        }
        if let Some(f) = self.floor.get(stream) {
            if after < *f {
                return Err(SyncError::RebuildRequired {
                    stream: stream.to_string(),
                    head,
                });
            }
        }
        if let Some(h) = head {
            if after > h {
                return Err(SyncError::InvalidUpdate(format!(
                    "after {after} > head {h}"
                )));
            }
        }
        Ok(())
    }
}
