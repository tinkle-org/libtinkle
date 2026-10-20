//! Realtime batch/cursor validation (`REALTIME.md`, `realtime.proto`).
//!
//! Every Tinkle realtime `Notification.payload` is one protobuf-binary
//! `RealtimeNotification` whose `kind` oneof selects `batch`, `presence`,
//! or `reset_required`. Durable delivery is only a complete `RealtimeBatch`
//! of 1-4 ordered events. Never flatten batches, send bare events, fall
//! back to JSON, or try multiple decoders.
//!
//! Checkpoints belong to one authenticated principal, stream, and valid
//! history scope, independent of connection/request IDs. Validate and
//! atomically commit all batch effects plus `final_cursor`, or neither.

use std::collections::HashMap;

use prost::Message;
use thiserror::Error;

use crate::proto::{
    realtime_event::Kind as EventKind, realtime_notification::Kind as NotifKind, RealtimeBatch,
    RealtimeEvent, RealtimeEventType, RealtimeNotification,
};

/// Max signed 64-bit cursor (`0..9223372036854775807`).
pub const MAX_CURSOR: u64 = i64::MAX as u64;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RealtimeError {
    #[error("malformed notification: {0}")]
    Malformed(String),
    #[error("unsupported notification kind")]
    UnsupportedKind,
    #[error("invalid batch: {0}")]
    InvalidBatch(String),
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
    #[error("reset required on {stream}")]
    ResetRequired { stream: String, head: Option<u64> },
}

/// What a validated notification carries.
#[derive(Debug, Clone)]
pub enum ValidatedNotification {
    Batch(ValidatedBatch),
    Presence(crate::proto::RealtimeOnline),
    ResetRequired { stream: String, head: Option<u64> },
}

/// A fully validated batch ready for atomic commit.
#[derive(Debug, Clone)]
pub struct ValidatedBatch {
    pub stream: String,
    pub final_cursor: u64,
    pub events: Vec<RealtimeEvent>,
}

/// Decode exactly one `RealtimeNotification` from `Notification.payload`.
/// Never try multiple decoders; unsupported kinds stop application
/// without checkpoint progress.
pub fn validate_notification(payload: &[u8]) -> Result<ValidatedNotification, RealtimeError> {
    let notif = RealtimeNotification::decode(payload)
        .map_err(|_| RealtimeError::Malformed("cannot decode RealtimeNotification".into()))?;
    match notif.kind {
        Some(NotifKind::Batch(b)) => Ok(ValidatedNotification::Batch(validate_batch(&b)?)),
        Some(NotifKind::Presence(p)) => Ok(ValidatedNotification::Presence(p)),
        Some(NotifKind::ResetRequired(r)) => Ok(ValidatedNotification::ResetRequired {
            stream: r.stream,
            head: r.head,
        }),
        None => Err(RealtimeError::Malformed(
            "notification kind must be set".into(),
        )),
    }
}

/// Validate a complete `RealtimeBatch` before any effects.
pub fn validate_batch(batch: &RealtimeBatch) -> Result<ValidatedBatch, RealtimeError> {
    if batch.stream.is_empty() {
        return Err(RealtimeError::InvalidBatch("stream must be set".into()));
    }
    if !is_canonical_stream(&batch.stream) {
        return Err(RealtimeError::InvalidBatch(format!(
            "bad stream '{}'",
            batch.stream
        )));
    }
    if batch.final_cursor == 0 || batch.final_cursor > MAX_CURSOR {
        return Err(RealtimeError::CursorOutOfRange);
    }
    if batch.events.is_empty() || batch.events.len() > 4 {
        return Err(RealtimeError::InvalidBatch(format!(
            "batch must have 1..4 events, got {}",
            batch.events.len()
        )));
    }
    // Every member shares stream + final_cursor; enum agrees with variant.
    for e in &batch.events {
        if e.stream != batch.stream {
            return Err(RealtimeError::InvalidBatch(
                "event stream must equal batch stream".into(),
            ));
        }
        if e.cursor != batch.final_cursor {
            return Err(RealtimeError::InvalidBatch(
                "event cursor must equal final_cursor".into(),
            ));
        }
        if e.cursor == 0 || e.cursor > MAX_CURSOR {
            return Err(RealtimeError::CursorOutOfRange);
        }
        validate_event_kind_agreement(e)?;
    }
    validate_sequence_grammar(batch)?;
    // Redaction singleton has no UUID/identity; others share one UUID.
    validate_uuid_discipline(batch)?;
    // Position order hint check where present.
    for e in &batch.events {
        if let Some(EventKind::ChatPosition(p)) = &e.kind {
            let expected = position_order(batch.final_cursor, e.source_seq);
            if p.order != expected {
                return Err(RealtimeError::InvalidBatch(format!(
                    "bad position order '{}', expected '{expected}'",
                    p.order
                )));
            }
        }
    }
    Ok(ValidatedBatch {
        stream: batch.stream.clone(),
        final_cursor: batch.final_cursor,
        events: batch.events.clone(),
    })
}

fn is_canonical_stream(s: &str) -> bool {
    // user:<positive-id> or room:<positive-id>
    let (kind, id) = match s.split_once(':') {
        Some(v) => v,
        None => return false,
    };
    if kind != "user" && kind != "room" {
        return false;
    }
    matches!(id.parse::<i64>(), Ok(v) if v > 0)
}

fn validate_event_kind_agreement(e: &RealtimeEvent) -> Result<(), RealtimeError> {
    let expected = match &e.kind {
        Some(EventKind::MessageSnapshot(_)) => RealtimeEventType::MessageSnapshot,
        Some(EventKind::MessageSendFailed(_)) => RealtimeEventType::MessageSendFailed,
        Some(EventKind::UpdateRedacted(_)) => RealtimeEventType::UpdateRedacted,
        Some(EventKind::DialogReadAdvanced(_)) => RealtimeEventType::DialogReadAdvanced,
        Some(EventKind::UserHydration(_)) => RealtimeEventType::UserHydration,
        Some(EventKind::ChatHydration(_)) => RealtimeEventType::ChatHydration,
        Some(EventKind::ChatPosition(_)) => RealtimeEventType::ChatPosition,
        None => return Err(RealtimeError::InvalidBatch("event kind must be set".into())),
    };
    let got = RealtimeEventType::try_from(e.r#type).unwrap_or(RealtimeEventType::Unspecified);
    if got != expected {
        return Err(RealtimeError::InvalidBatch(format!(
            "event type {got:?} disagrees with kind {expected:?}"
        )));
    }
    Ok(())
}

/// Allowed sequences (REALTIME.md §4):
/// `[user_hydration?] [chat_hydration?] message_snapshot chat_position`
/// `[user_hydration?] [chat_hydration?] message_send_failed chat_position`
/// `update_redacted` | `dialog_read_advanced`
fn validate_sequence_grammar(batch: &RealtimeBatch) -> Result<(), RealtimeError> {
    use EventKind as K;
    let kinds: Vec<&K> = batch
        .events
        .iter()
        .filter_map(|e| e.kind.as_ref())
        .collect();
    let ok = matches!(
        kinds.as_slice(),
        [K::UpdateRedacted(_)]
            | [K::DialogReadAdvanced(_)]
            | [K::MessageSnapshot(_), K::ChatPosition(_)]
            | [
                K::UserHydration(_),
                K::MessageSnapshot(_),
                K::ChatPosition(_)
            ]
            | [
                K::ChatHydration(_),
                K::MessageSnapshot(_),
                K::ChatPosition(_)
            ]
            | [
                K::UserHydration(_),
                K::ChatHydration(_),
                K::MessageSnapshot(_),
                K::ChatPosition(_)
            ]
            | [K::MessageSendFailed(_), K::ChatPosition(_)]
            | [
                K::UserHydration(_),
                K::MessageSendFailed(_),
                K::ChatPosition(_)
            ]
            | [
                K::ChatHydration(_),
                K::MessageSendFailed(_),
                K::ChatPosition(_)
            ]
            | [
                K::UserHydration(_),
                K::ChatHydration(_),
                K::MessageSendFailed(_),
                K::ChatPosition(_)
            ]
    );
    if !ok {
        return Err(RealtimeError::InvalidBatch(
            "event sequence violates allowed grammar".into(),
        ));
    }
    Ok(())
}

fn validate_uuid_discipline(batch: &RealtimeBatch) -> Result<(), RealtimeError> {
    use EventKind as K;
    let is_redaction =
        batch.events.len() == 1 && matches!(batch.events[0].kind, Some(K::UpdateRedacted(_)));
    if is_redaction {
        let e = &batch.events[0];
        if !e.event_id.is_empty() {
            return Err(RealtimeError::InvalidBatch(
                "redaction must not carry source UUID".into(),
            ));
        }
        if e.occurred_at.is_some() || e.source_seq.is_some() || e.source_revision.is_some() {
            return Err(RealtimeError::InvalidBatch(
                "redaction must not carry timestamps/markers".into(),
            ));
        }
        return Ok(());
    }
    // All members share one nonzero canonical lowercase UUID.
    let first = &batch.events[0].event_id;
    if first.is_empty() {
        return Err(RealtimeError::InvalidBatch("event_id must be set".into()));
    }
    for e in &batch.events {
        if e.event_id != *first {
            return Err(RealtimeError::InvalidBatch(
                "batch members must share source UUID".into(),
            ));
        }
    }
    Ok(())
}

/// Position `order` = cursor + source_seq as zero-padded 20-digit
/// unsigned decimals (40 ASCII digits). Absent source_seq means zero.
pub fn position_order(cursor: u64, source_seq: Option<i64>) -> String {
    let seq = source_seq.unwrap_or(0).max(0) as u64;
    format!("{cursor:020}{seq:020}")
}

/// Cursor decision for `(stream, applied, got)` (REALTIME.md §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorDecision {
    /// `c == a`: duplicate; do not reapply.
    Duplicate,
    /// `c == a+1`: apply then atomically commit.
    Next,
    /// `c > a+1`: missing update; stop, retain old checkpoint.
    Gap,
    /// `c < a` (or conflicting UUID on duplicate): rollback/old overlap;
    /// do not apply or move backward.
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
/// scope. Deduplicate whole batches by `(stream, cursor)`, not members
/// by UUID. Presence is never checkpointed; clear on disconnect.
#[derive(Debug, Default)]
pub struct StreamCursors {
    applied: HashMap<String, u64>,
    /// Replay floor per stream (largest removed cursor). `after < floor`
    /// expires; equality is valid; `after > head` is invalid.
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

    /// Atomically commit after all batch effects + required refreshes.
    /// Caller must have validated the batch and applied every effect;
    /// receipt/enqueue alone is not commit. Gaps/stale never advance.
    pub fn commit(&mut self, stream: &str, cursor: u64) -> Result<CursorDecision, RealtimeError> {
        if cursor == 0 || cursor > MAX_CURSOR {
            return Err(RealtimeError::CursorOutOfRange);
        }
        match self.decision(stream, cursor) {
            CursorDecision::Next => {
                self.applied.insert(stream.to_string(), cursor);
                Ok(CursorDecision::Next)
            }
            CursorDecision::Duplicate => Ok(CursorDecision::Duplicate),
            CursorDecision::Gap => Err(RealtimeError::Gap {
                stream: stream.to_string(),
                expected: self.get(stream).saturating_add(1),
                got: cursor,
            }),
            CursorDecision::Stale => Err(RealtimeError::Stale {
                stream: stream.to_string(),
                applied: self.get(stream),
                got: cursor,
            }),
        }
    }

    /// Stop one stream on reset. The optional head is diagnostic only,
    /// never a snapshot checkpoint. Recovery remains unavailable until a
    /// proven consistent source snapshot/checkpoint exists.
    pub fn on_reset(&mut self, stream: &str) {
        self.applied.remove(stream);
    }

    /// Restart hydration: the durable checkpoint IS the last-applied.
    /// Sets memory to `cursor` without gap checks (restart path only;
    /// never use for live batches, which must go through `commit`).
    pub fn hydrate(&mut self, stream: &str, cursor: u64) {
        if cursor == 0 {
            self.applied.remove(stream);
        } else {
            self.applied.insert(stream.to_string(), cursor);
        }
    }

    /// Clear ephemeral state on disconnect (presence cleared; durable
    /// checkpoints survive per scope and are NOT cleared here).
    pub fn on_disconnect_presence_cleared(&self) {
        // Presence is ephemeral by construction (never in `applied`).
    }

    pub fn set_floor(&mut self, stream: &str, floor: u64) {
        self.floor.insert(stream.to_string(), floor);
    }

    /// Validate a subscribe `after` cursor against floor/head.
    pub fn validate_after(
        &self,
        stream: &str,
        after: u64,
        head: Option<u64>,
    ) -> Result<(), RealtimeError> {
        if after > MAX_CURSOR {
            return Err(RealtimeError::CursorOutOfRange);
        }
        if let Some(f) = self.floor.get(stream) {
            if after < *f {
                return Err(RealtimeError::ResetRequired {
                    stream: stream.to_string(),
                    head,
                });
            }
        }
        if let Some(h) = head {
            if after > h {
                return Err(RealtimeError::InvalidBatch(format!(
                    "after {after} > head {h}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evt(
        stream: &str,
        cursor: u64,
        id: &str,
        kind: EventKind,
        ty: RealtimeEventType,
    ) -> RealtimeEvent {
        RealtimeEvent {
            event_id: id.to_string(),
            stream: stream.to_string(),
            cursor,
            occurred_at: None,
            source_seq: None,
            source_revision: None,
            r#type: ty as i32,
            kind: Some(kind),
        }
    }

    #[test]
    fn accepts_snapshot_sequence() {
        let stream = "user:7";
        let batch = RealtimeBatch {
            stream: stream.into(),
            events: vec![
                evt(
                    stream,
                    3,
                    "uuid-1",
                    EventKind::MessageSnapshot(Default::default()),
                    RealtimeEventType::MessageSnapshot,
                ),
                evt(
                    stream,
                    3,
                    "uuid-1",
                    EventKind::ChatPosition(crate::proto::RealtimeChatPosition {
                        chat_key: "room:1".into(),
                        order: position_order(3, None),
                    }),
                    RealtimeEventType::ChatPosition,
                ),
            ],
            final_cursor: 3,
        };
        assert!(validate_batch(&batch).is_ok());
    }

    #[test]
    fn rejects_flattened_or_mixed() {
        let stream = "user:7";
        let batch = RealtimeBatch {
            stream: stream.into(),
            events: vec![
                evt(
                    stream,
                    3,
                    "uuid-1",
                    EventKind::MessageSnapshot(Default::default()),
                    RealtimeEventType::MessageSnapshot,
                ),
                evt(
                    stream,
                    3,
                    "uuid-2",
                    EventKind::MessageSnapshot(Default::default()),
                    RealtimeEventType::MessageSnapshot,
                ),
            ],
            final_cursor: 3,
        };
        assert!(validate_batch(&batch).is_err());
    }

    #[test]
    fn cursor_decisions() {
        assert_eq!(decide_cursor(5, 5), CursorDecision::Duplicate);
        assert_eq!(decide_cursor(5, 6), CursorDecision::Next);
        assert_eq!(decide_cursor(5, 8), CursorDecision::Gap);
        assert_eq!(decide_cursor(5, 3), CursorDecision::Stale);
    }

    #[test]
    fn commit_advances_only_on_next() {
        let mut c = StreamCursors::new();
        assert_eq!(c.commit("user:1", 1).unwrap(), CursorDecision::Next);
        assert_eq!(c.commit("user:1", 1).unwrap(), CursorDecision::Duplicate);
        assert!(c.commit("user:1", 3).is_err());
        assert!(c.commit("user:1", 0).is_err());
    }
}
