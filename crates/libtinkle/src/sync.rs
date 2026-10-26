//! Reconciliation coordination + opaque notification hints + journal cursors.
//!
//! Sync reconciles per-conversation mutation journals with unary
//! `MessagesService/GetTargetChanges` pages and fetches bodies with unary
//! `MessagesService/GetMessages`. Connectivity observations stay ephemeral
//! (`UpdatesService/ObserveConnectivity` notifications carry
//! `ConnectivityChanged` bytes and must be discarded on disconnect).
//!
//! Notification frames are OPAQUE hints: the pump decodes nothing. Each
//! frame marks reconcile demand for every tracked target (single-flight per
//! target); the supervisor (or an explicit `reconcile_now`) performs the
//! actual `GetTargetChanges` paging, validation (`validate_changes_page`),
//! projection (`cache_projector`), and atomic commit
//! (`apply_journal_page_fenced`).
//!
//! Persistent user application state lives here, outside the generic Syntra
//! core. Journal cursors persist through [`crate::CursorStore`] (and SQLite
//! when present); connectivity observations never advance a cursor.
//!
//! History-scope rule: principal changes fence via `generation` (stale pages
//! are dropped). Dropping one conversation's journal cursor via
//! `remove_target` makes its next reconcile start at 0; when the retained
//! journal skips past the dropped position, callers heal with a `GetHistory`
//! fallback instead of treating the skip as fatal (see
//! `Client::reconcile_target`).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};

use tinkle_schema::proto::{ConnectivityChanged, Target};

/// Pre-authentication principal placeholder.
pub(crate) const UNKNOWN_PRINCIPAL: &str = "unknown";

const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Internal recovery signal from the reconcile pipeline to the connection
/// supervisor. Hints only (bounded `try_send`, never blocks the pump):
/// target states persist in the manager, and the supervisor tick sweep
/// over tracked + dirty keys retries demands whose enqueue dropped.
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) enum RecoverySignal {
    /// Notification pump exited; the transport is presumed dead.
    PumpDead,
    /// Opaque hint arrived (or manual kick): reconcile this target.
    Demand { target_key: String },
    /// Durable checkpoint save failed; retry reconcile for the target.
    CheckpointFailed { target_key: String, accept_seq: i64 },
    /// Reconcile page run failed; reschedule with backoff.
    ReconcileFailed { target_key: String },
}

/// App-visible sync events.
#[derive(Debug, Clone)]
pub enum SyncEvent {
    /// One journal page applied for a conversation.
    ConversationChanged {
        target_key: String,
        applied_accept_seq: i64,
    },
    /// Message bodies changed for a conversation (edits hydrated,
    /// deletions/revoked removed).
    MessagesChanged {
        target_key: String,
        message_ids: Vec<i64>,
    },
    /// One conversation removed from tracking via `unsubscribe` (also
    /// emitted by `SyncManager::untrack_targets`). Hosts drop cached rows.
    ConversationRemoved { target_key: String },
    /// One reconcile run failed (retryable demand persists in the manager
    /// and the supervisor tick sweep reschedules).
    ReconcileFailed { target_key: String, error: String },
    /// Ephemeral connectivity observation (never checkpointed).
    Connectivity(ConnectivityChanged),
    /// Durable checkpoint save failed. Memory remains at the previous
    /// cursor; the page must be re-applied after recovery.
    CheckpointFailed {
        target_key: String,
        accept_seq: i64,
        error: String,
    },
}

/// Per-target reconcile status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReconcileStatus {
    #[default]
    Idle,
    Reconciling,
    Failed,
}

#[derive(Debug, Default)]
struct PrincipalState {
    principal: String,
    /// Fencing generation: bumped on every scope reset (account switch,
    /// disconnect) so in-flight reconcile pages validated against a stale
    /// snapshot are dropped instead of applied. Mirrors the durable
    /// `stream_state.generation` in SQLite; the two meet in
    /// `apply_journal_page_fenced`.
    generation: u64,
}

/// Manages one principal's tracked conversations + reconcile states
/// (crate-internal; apps observe [`SyncEvent`] via [`crate::Client::next_event`]).
pub struct SyncManager {
    state: Mutex<PrincipalState>,
    #[cfg(not(target_arch = "wasm32"))]
    sqlite: Mutex<Option<Arc<tinkle_storage::Storage>>>,
    /// Serializes durable checkpoint writes so concurrent reconciles for
    /// different targets never interleave a commit. Held only across the
    /// in-memory commit + store save; store I/O stays outside `state` so the
    /// notification pump is never blocked.
    checkpoint_save: Mutex<()>,
    event_tx: mpsc::Sender<SyncEvent>,
    event_rx: Mutex<mpsc::Receiver<SyncEvent>>,
    tracked: Mutex<HashMap<String, Target>>,
    status: Mutex<HashMap<String, ReconcileStatus>>,
    in_flight: Mutex<HashSet<String>>,
    /// Hints that arrived while a run was in flight for the target. The
    /// in-flight run may have fetched its last page before the change
    /// landed, so `end_reconcile` consumes this and re-demands instead of
    /// dropping the hint. Cleared on scope reset; kept across disconnects
    /// (reconnect re-demands tracked targets anyway).
    dirty: Mutex<HashSet<String>>,
    signals: Mutex<Option<mpsc::Sender<RecoverySignal>>>,
}

impl SyncManager {
    pub fn new(principal: impl Into<String>) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        Arc::new(Self {
            state: Mutex::new(PrincipalState {
                principal: principal.into(),
                generation: 0,
            }),
            #[cfg(not(target_arch = "wasm32"))]
            sqlite: Mutex::new(None),
            checkpoint_save: Mutex::new(()),
            event_tx: tx,
            event_rx: Mutex::new(rx),
            tracked: Mutex::new(HashMap::new()),
            status: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashSet::new()),
            dirty: Mutex::new(HashSet::new()),
            signals: Mutex::new(None),
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn set_sqlite_storage(&self, storage: Arc<tinkle_storage::Storage>) {
        *self.sqlite.lock().await = Some(storage);
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn sqlite_clone(&self) -> Option<Arc<tinkle_storage::Storage>> {
        self.sqlite.lock().await.clone()
    }

    pub(crate) async fn checkpoint_save_lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.checkpoint_save.lock().await
    }

    pub(crate) async fn set_signal_tx(&self, tx: mpsc::Sender<RecoverySignal>) {
        *self.signals.lock().await = Some(tx);
    }

    /// Best-effort signal enqueue. Returns true when the signal was queued
    /// or when no supervisor channel is installed (manual path: nothing to
    /// retry via signals). Returns false when the channel is full or
    /// closed; callers must retain their demand marker so the supervisor
    /// tick sweep can retry.
    pub(crate) async fn emit_signal(&self, signal: RecoverySignal) -> bool {
        if let Some(tx) = self.signals.lock().await.clone() {
            tx.try_send(signal).is_ok()
        } else {
            true
        }
    }

    pub(crate) async fn emit_event(&self, event: SyncEvent) {
        let _ = self.event_tx.try_send(event);
    }

    /// Current fencing generation.
    pub(crate) async fn generation(&self) -> u64 {
        self.state.lock().await.generation
    }

    /// Generation fence: true while `principal` and `generation` still match
    /// the live scope. Pages failing this check are stale and must be
    /// dropped without effects.
    pub(crate) async fn check_fence(&self, principal: &str, generation: u64) -> bool {
        let state = self.state.lock().await;
        state.principal == principal && state.generation == generation
    }

    /// Non-blocking event poll. Returns `None` when empty.
    pub async fn try_next_event(&self) -> Option<SyncEvent> {
        self.event_rx.lock().await.try_recv().ok()
    }

    pub async fn next_event(&self) -> Option<SyncEvent> {
        self.event_rx.lock().await.recv().await
    }

    /// Track conversations (idempotent). Unknown targets start `Idle`.
    pub(crate) async fn track_targets(&self, targets: Vec<Target>) {
        use tinkle_schema::sync::target_key_for;
        let mut tracked = self.tracked.lock().await;
        let mut status = self.status.lock().await;
        for t in targets {
            let kind = match &t.kind {
                Some(k) => k,
                None => continue,
            };
            let key = target_key_for(kind);
            tracked.insert(key.clone(), t);
            status.entry(key).or_insert(ReconcileStatus::Idle);
        }
    }

    pub(crate) async fn tracked_keys(&self) -> Vec<String> {
        self.tracked.lock().await.keys().cloned().collect()
    }

    /// Per-target reconcile status. Unknown targets report `None`.
    pub async fn target_status(&self, target_key: &str) -> Option<ReconcileStatus> {
        self.status.lock().await.get(target_key).copied()
    }

    /// Single-flight guard: `Some(generation)` when this call started the
    /// run, `None` when a run is already in flight for the target. The
    /// returned generation fences the run; pass it to `end_reconcile`.
    pub(crate) async fn begin_reconcile(&self, target_key: &str) -> Option<u64> {
        let mut flight = self.in_flight.lock().await;
        if !flight.insert(target_key.to_string()) {
            return None;
        }
        let generation = self.state.lock().await.generation;
        self.status
            .lock()
            .await
            .insert(target_key.to_string(), ReconcileStatus::Reconciling);
        Some(generation)
    }

    /// Close a run started by `begin_reconcile`. No-op when `generation`
    /// no longer matches the live scope: a stale run must not clear the
    /// fresh run's in-flight marker nor overwrite its status.
    ///
    /// Holds the in-flight guard across removal + dirty consume so a
    /// concurrent `handle_hint` check+insert cannot interleave and lose a
    /// hint. The dirty marker is retained until the follow-up demand
    /// actually enqueues (best-effort `try_send` can drop under pressure;
    /// the supervisor tick sweep retries retained markers).
    pub(crate) async fn end_reconcile(&self, target_key: &str, generation: u64, failed: bool) {
        // Hold the guard for the whole transition (see above). The emit
        // below only does a `try_send`, so holding across it is bounded.
        let mut flight = self.in_flight.lock().await;
        {
            let state = self.state.lock().await;
            if state.generation != generation {
                return;
            }
        }
        flight.remove(target_key);
        // Terminal `Failed` persists until the next run starts; `Idle`
        // means caught up. Only a new run clears `Failed`.
        self.status.lock().await.insert(
            target_key.to_string(),
            if failed {
                ReconcileStatus::Failed
            } else {
                ReconcileStatus::Idle
            },
        );
        // A hint that arrived mid-run may describe a change the run never
        // fetched; re-demand instead of dropping it. Retain the marker
        // when the enqueue fails so the tick sweep retries.
        let was_dirty = self.dirty.lock().await.contains(target_key);
        if was_dirty {
            let enqueued = self
                .emit_signal(RecoverySignal::Demand {
                    target_key: target_key.to_string(),
                })
                .await;
            if enqueued {
                self.dirty.lock().await.remove(target_key);
            }
        }
    }

    pub(crate) async fn is_reconciling(&self, target_key: &str) -> bool {
        self.in_flight.lock().await.contains(target_key)
    }

    /// Snapshot of dirty (hint-during-run) markers. The supervisor tick
    /// uses it to retry demands whose `try_send` was dropped.
    pub(crate) async fn dirty_keys(&self) -> Vec<String> {
        self.dirty.lock().await.iter().cloned().collect()
    }

    /// Untrack conversations and notify hosts so they can drop cached
    /// rows. Emits one `ConversationRemoved` per key (best-effort via the
    /// bounded event channel, like all `SyncEvent`s).
    pub(crate) async fn untrack_targets(&self, target_keys: &[String]) {
        {
            let mut tracked = self.tracked.lock().await;
            let mut status = self.status.lock().await;
            let mut flight = self.in_flight.lock().await;
            let mut dirty = self.dirty.lock().await;
            for key in target_keys {
                tracked.remove(key);
                status.remove(key);
                flight.remove(key);
                dirty.remove(key);
            }
        }
        for key in target_keys {
            self.emit_event(SyncEvent::ConversationRemoved {
                target_key: key.clone(),
            })
            .await;
        }
    }

    /// Opaque notification hint: decode nothing. Each frame marks reconcile
    /// demand for every tracked target (single-flight: a hint for an
    /// already-reconciling target is recorded dirty instead of emitting a
    /// duplicate demand; `end_reconcile` consumes it and re-demands).
    ///
    /// Best-effort: `Demand` signals use a bounded `try_send` and can drop
    /// under pressure. Dropped demands are retried by the supervisor tick
    /// sweep over tracked + dirty keys, so a change landing mid-run is
    /// eventually re-demanded rather than lost on the floor.
    ///
    /// Holds the in-flight guard across the check + dirty insert so a
    /// concurrent `end_reconcile` cannot slip between the two and lose the
    /// hint.
    pub(crate) async fn handle_hint(&self, _payload: &[u8]) {
        let keys = self.tracked_keys().await;
        // Decide while holding the guard; emit after releasing it so a
        // slow supervisor channel never blocks reconciliation.
        let mut to_dirty = Vec::new();
        let mut to_demand = Vec::new();
        {
            let flight = self.in_flight.lock().await;
            for key in keys {
                if flight.contains(&key) {
                    to_dirty.push(key);
                } else {
                    to_demand.push(key);
                }
            }
            if !to_dirty.is_empty() {
                let mut dirty = self.dirty.lock().await;
                for key in &to_dirty {
                    dirty.insert(key.clone());
                }
            }
        }
        for key in to_demand {
            self.emit_signal(RecoverySignal::Demand { target_key: key })
                .await;
        }
    }

    /// Best-effort connectivity observation (decoded by the caller via
    /// `validate_connectivity_notification`; failures are dropped, never
    /// checkpointed).
    pub(crate) async fn emit_connectivity(&self, update: ConnectivityChanged) {
        let _ = self.event_tx.try_send(SyncEvent::Connectivity(update));
    }

    pub(crate) async fn set_principal(&self, principal: String) {
        let mut state = self.state.lock().await;
        if state.principal == principal {
            return;
        }
        state.principal = principal;
        state.generation = state.generation.wrapping_add(1);
        drop(state);
        self.tracked.lock().await.clear();
        self.status.lock().await.clear();
        self.in_flight.lock().await.clear();
        self.dirty.lock().await.clear();
    }

    pub(crate) async fn on_disconnect(&self) {
        let mut state = self.state.lock().await;
        state.generation = state.generation.wrapping_add(1);
        drop(state);
        self.in_flight.lock().await.clear();
        // Keep tracked targets + last statuses so reconnect re-demands;
        // in-flight runs are fenced by the generation bump.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinkle_schema::proto::{target, Target};

    fn room_target(id: i64) -> Target {
        Target {
            kind: Some(target::Kind::ChatId(id)),
        }
    }

    #[tokio::test]
    async fn hint_demands_all_tracked_once() {
        let mgr = SyncManager::new("user:1");
        mgr.track_targets(vec![room_target(7), room_target(8)])
            .await;
        let (tx, mut rx) = mpsc::channel(16);
        mgr.set_signal_tx(tx).await;
        mgr.handle_hint(b"opaque").await;
        let mut got = [rx.try_recv().unwrap(), rx.try_recv().unwrap()];
        got.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        assert!(matches!(
            &got[0],
            RecoverySignal::Demand { target_key } if target_key == "target:room:7"
        ));
        assert!(matches!(
            &got[1],
            RecoverySignal::Demand { target_key } if target_key == "target:room:8"
        ));
        // Single-flight: reconciling targets are skipped.
        assert!(mgr.begin_reconcile("target:room:7").await.is_some());
        mgr.handle_hint(b"opaque").await;
        assert!(matches!(
            rx.try_recv().unwrap(),
            RecoverySignal::Demand { target_key } if target_key == "target:room:8"
        ));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn hint_during_reconcile_redemands_on_completion() {
        let mgr = SyncManager::new("user:1");
        mgr.track_targets(vec![room_target(7)]).await;
        let (tx, mut rx) = mpsc::channel(16);
        mgr.set_signal_tx(tx).await;
        let gen = mgr.begin_reconcile("target:room:7").await.unwrap();
        // Hint during the run emits nothing yet, but is recorded.
        mgr.handle_hint(b"opaque").await;
        assert!(rx.try_recv().is_err());
        // Completion consumes the dirty flag and re-demands.
        mgr.end_reconcile("target:room:7", gen, false).await;
        assert!(matches!(
            rx.try_recv().unwrap(),
            RecoverySignal::Demand { target_key } if target_key == "target:room:7"
        ));
        assert!(rx.try_recv().is_err());
        // A clean run emits no follow-up demand.
        let gen2 = mgr.begin_reconcile("target:room:7").await.unwrap();
        mgr.end_reconcile("target:room:7", gen2, false).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn single_flight_and_status() {
        let mgr = SyncManager::new("user:1");
        mgr.track_targets(vec![room_target(7)]).await;
        assert_eq!(
            mgr.target_status("target:room:7").await,
            Some(ReconcileStatus::Idle)
        );
        assert_eq!(mgr.target_status("target:room:9").await, None);
        let gen = mgr.begin_reconcile("target:room:7").await.unwrap();
        assert!(mgr.begin_reconcile("target:room:7").await.is_none());
        assert_eq!(
            mgr.target_status("target:room:7").await,
            Some(ReconcileStatus::Reconciling)
        );
        mgr.end_reconcile("target:room:7", gen, false).await;
        assert_eq!(
            mgr.target_status("target:room:7").await,
            Some(ReconcileStatus::Idle)
        );
        let gen2 = mgr.begin_reconcile("target:room:7").await.unwrap();
        mgr.end_reconcile("target:room:7", gen2, true).await;
        assert_eq!(
            mgr.target_status("target:room:7").await,
            Some(ReconcileStatus::Failed)
        );
    }

    #[tokio::test]
    async fn stale_end_is_noop_on_generation_mismatch() {
        let mgr = SyncManager::new("user:1");
        mgr.track_targets(vec![room_target(7)]).await;
        let stale = mgr.begin_reconcile("target:room:7").await.unwrap();
        // Scope reset clears in-flight and bumps generation.
        mgr.on_disconnect().await;
        // Fresh run starts after the reset.
        let fresh = mgr.begin_reconcile("target:room:7").await.unwrap();
        assert_ne!(stale, fresh);
        // Stale completion must not clear the fresh run nor set Failed.
        mgr.end_reconcile("target:room:7", stale, true).await;
        assert!(mgr.is_reconciling("target:room:7").await);
        assert_eq!(
            mgr.target_status("target:room:7").await,
            Some(ReconcileStatus::Reconciling)
        );
        mgr.end_reconcile("target:room:7", fresh, false).await;
        assert_eq!(
            mgr.target_status("target:room:7").await,
            Some(ReconcileStatus::Idle)
        );
    }

    #[tokio::test]
    async fn dirty_retained_when_signal_dropped() {
        let mgr = SyncManager::new("user:1");
        mgr.track_targets(vec![room_target(7)]).await;
        // No signal channel: full/closed channel simulated with capacity 0
        // is racy, so use a channel that is already full.
        let (tx, mut rx) = mpsc::channel(1);
        // Fill it so the next try_send fails.
        tx.try_send(RecoverySignal::PumpDead).unwrap();
        mgr.set_signal_tx(tx).await;
        let gen = mgr.begin_reconcile("target:room:7").await.unwrap();
        mgr.handle_hint(b"opaque").await;
        assert_eq!(mgr.dirty_keys().await, vec!["target:room:7".to_string()]);
        mgr.end_reconcile("target:room:7", gen, false).await;
        // Enqueue failed, so the marker must be retained for the tick sweep.
        assert_eq!(mgr.dirty_keys().await, vec!["target:room:7".to_string()]);
        // Drain to make room, then the next run's completion retries.
        let _ = rx.try_recv().unwrap();
        let gen2 = mgr.begin_reconcile("target:room:7").await.unwrap();
        mgr.end_reconcile("target:room:7", gen2, false).await;
        assert!(mgr.dirty_keys().await.is_empty());
    }

    #[tokio::test]
    async fn untrack_emits_conversation_removed() {
        let mgr = SyncManager::new("user:1");
        mgr.track_targets(vec![room_target(7)]).await;
        mgr.untrack_targets(&["target:room:7".to_string()]).await;
        assert!(mgr.tracked_keys().await.is_empty());
        assert_eq!(mgr.target_status("target:room:7").await, None);
        assert!(matches!(
            mgr.try_next_event().await,
            Some(SyncEvent::ConversationRemoved { target_key })
            if target_key == "target:room:7"
        ));
    }

    #[tokio::test]
    async fn set_principal_resets_and_bumps() {
        let mgr = SyncManager::new("user:1");
        mgr.track_targets(vec![room_target(7)]).await;
        let g0 = mgr.generation().await;
        mgr.set_principal("user:2".to_string()).await;
        assert_eq!(mgr.generation().await, g0.wrapping_add(1));
        assert!(mgr.tracked_keys().await.is_empty());
    }

    #[tokio::test]
    async fn fence_rejects_stale_scope() {
        let mgr = SyncManager::new("user:1");
        let g = mgr.generation().await;
        assert!(mgr.check_fence("user:1", g).await);
        assert!(!mgr.check_fence("user:2", g).await);
        mgr.set_principal("user:2".to_string()).await;
        assert!(!mgr.check_fence("user:1", g).await);
    }
}
