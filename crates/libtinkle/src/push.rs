//! Push-triggered sync: provider-neutral wake handling over durable jobs.
//!
//! Push (APNs/FCM, first via OneSignal) carries only a wake hint — never
//! message bodies or secrets. On wake the host parses the provider payload
//! into a [`PushEnvelope`] ([`parse_push_payload`]), then drives
//! [`PushManager::process_push`] (or `_with_deadline`) on a bounded
//! executor independent of the foreground supervisor/socket.
//!
//! Pipeline per wake:
//! 1. Validate envelope (unknown/missing routing → deferred account-level
//!    catch-up, never guess a target).
//! 2. Persist pending work to `background_sync_jobs` (process-safe claim
//!    via `claimed_by` + `attempts`; survives process death).
//! 3. Resolve the matching authenticated account (epoch/generation fence
//!    vs logout/switch; mismatch defers, never fetches as the wrong user).
//! 4. Fetch authoritative bodies via `get_messages`/`get_history` (NEVER
//!    trust notification text as a message row).
//! 5. Commit via the shared `ingest_messages` helper (same-cursor commit,
//!    never moves the journal cursor).
//! 6. Reconcile mutations + bounded catch-up (`reconcile_target` within
//!    the remaining deadline).
//! 7. Return [`PushOutcome::Committed`]/`Deferred`/`Failed` with
//!    exactly-once completion (`complete_background_job(done)` only after
//!    commit; deferred releases back to `pending` for retry).
//!
//! Cancellation-safe: dropping the future never corrupts SQLite
//! (transactions are atomic); a claimed-but-uncompleted job is recovered
//! via [`Storage::requeue_stale_background_jobs`](tinkle_storage::Storage::requeue_stale_background_jobs)
//! on the next wake/cold start. Deadline-aware: every network step races
//! the caller deadline; expiry persists the job and defers.
//!
//! Concurrent app/extension races are fenced twice: the durable
//! `claim_background_job` compare-and-set (one winner) plus the
//! in-memory principal/generation fence (logout/switch drops stale work).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::time::Instant;

use crate::client::Client;
use crate::errors::{RecoveryDecision, TinkleError};

/// Push envelope version produced by the current backend handoff.
/// Unknown versions defer to account-level catch-up (forward-compatible).
pub const PUSH_ENVELOPE_VERSION: u32 = 1;

/// Durable job kinds for the push path.
pub const PUSH_JOB_KIND: &str = "push";
/// Account-level fallback when routing is missing (never guess a target).
pub const PUSH_ACCOUNT_JOB_KIND: &str = "push_account_catchup";
/// Stale-claim lease before a cold start may requeue (seconds).
pub const PUSH_CLAIM_LEASE_SECS: i64 = 60;
/// Default wake budget when the host does not pass a deadline
/// (iOS NSE ≈ 30s, Android WorkManager is looser; 25s fits both).
pub const PUSH_DEFAULT_BUDGET_SECS: u64 = 25;
/// Cap on message ids accepted from one envelope (protocol bound is 100
/// per `GetMessages`; larger wakes page via reconcile catch-up).
pub const PUSH_MAX_MESSAGE_IDS: usize = 500;

static PUSH_OWNER_COUNTER: AtomicU64 = AtomicU64::new(0);

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn push_owner_id() -> String {
    let n = PUSH_OWNER_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("push-{}-{nanos}-{n}", std::process::id())
}

/// Provider-neutral push envelope (versioned).
///
/// `account_user_id` is the recipient account; `target_kind`/`target_id`
/// route to one conversation (`user` = DM, `room`/`chat` = group);
/// `message_ids` are authoritative fetch keys (never bodies);
/// `event_id` is the backend dedup id (OneSignal notification id or
/// explicit event id; empty falls back to a target+ids job key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushEnvelope {
    pub version: u32,
    pub account_user_id: Option<i64>,
    pub target_kind: Option<String>,
    pub target_id: Option<i64>,
    pub message_ids: Vec<i64>,
    pub event_id: String,
}

impl PushEnvelope {
    /// Canonical principal for the recipient account, if known.
    pub fn account_principal(&self) -> Option<String> {
        self.account_user_id
            .filter(|id| *id > 0)
            .map(|id| format!("user:{id}"))
    }

    /// Canonical target key (`target:user:<id>` / `target:room:<id>`),
    /// or `None` when routing is missing/unknown (never guess).
    pub fn target_key(&self) -> Option<String> {
        let kind = self.target_kind.as_deref()?;
        let id = self.target_id.filter(|v| *v > 0)?;
        tinkle_schema::sync::target_from_parts(kind, id)
            .ok()
            .map(|t| match t.kind {
                Some(tinkle_schema::proto::target::Kind::UserId(v)) => {
                    format!("target:user:{v}")
                }
                Some(tinkle_schema::proto::target::Kind::ChatId(v)) => {
                    format!("target:room:{v}")
                }
                None => String::new(),
            })
            .filter(|s| !s.is_empty())
    }

    /// Typed target for authoritative fetches, or `None` when routing is
    /// missing/unknown.
    pub fn target(&self) -> Option<tinkle_schema::proto::Target> {
        let kind = self.target_kind.as_deref()?;
        let id = self.target_id.filter(|v| *v > 0)?;
        tinkle_schema::sync::target_from_parts(kind, id).ok()
    }
}

/// Outcome of one push wake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushOutcome {
    /// Bodies are durable in SQLite (plus best-effort reconcile).
    Committed {
        target_key: String,
        message_ids: Vec<i64>,
    },
    /// Not yet committed; the durable job is (re)queued for retry.
    /// Reasons: `missing_routing`, `version_mismatch`, `account_mismatch`,
    /// `logged_out`, `already_claimed`, `deadline`, `transient:<detail>`,
    /// `connect:<detail>`.
    Deferred { reason: String },
    /// Terminal for this wake (never retries with the same inputs).
    /// Reasons: `no_storage`, `auth:<detail>`, `invalid:<detail>`.
    Failed { reason: String },
}

/// Canonical job id: stable for dedup (`event_id` when present).
pub fn push_job_id(envelope: &PushEnvelope) -> String {
    let event = envelope.event_id.trim();
    if !event.is_empty() {
        let mut id = format!("push:{event}");
        truncate_to_char_boundary(&mut id, 200);
        if id.len() > "push:".len() {
            return id;
        }
    }
    // Fallback: hash of the FULL sorted id list. Dropping ids past 20 or
    // truncating to 200 bytes collides distinct wakes onto one job id,
    // merging unrelated fetches and losing messages.
    let mut ids = envelope.message_ids.clone();
    ids.sort_unstable();
    ids.dedup();
    let target = envelope
        .target_key()
        .unwrap_or_else(|| "account".to_string());
    // FNV-1a/64 over target + full id list (deterministic, no new deps);
    // output stays far under the 256-byte `check_job_id` bound.
    let mut h: u64 = 0xcbf29ce484222325;
    for b in target.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h ^= 0xff;
    h = h.wrapping_mul(0x100000001b3);
    for id in &ids {
        for b in id.to_le_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h ^= 0x00;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("push:{target}:n{}:{h:016x}", ids.len())
}

/// Truncate to at most `max` bytes, backing off to a UTF-8 character
/// boundary so the result is always valid (never panics on `truncate`).
fn truncate_to_char_boundary(s: &mut String, max: usize) {
    if s.len() > max {
        let mut end = max;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
    }
}

// -- payload parsing ---------------------------------------------------------

fn json_i64(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::Number(n) => n.as_i64().filter(|x| *x > 0),
        serde_json::Value::String(s) => s.trim().parse::<i64>().ok().filter(|x| *x > 0),
        _ => None,
    }
}

fn json_u32(v: &serde_json::Value) -> Option<u32> {
    match v {
        serde_json::Value::Number(n) => n.as_u64().and_then(|x| u32::try_from(x).ok()),
        serde_json::Value::String(s) => s.trim().parse::<u32>().ok(),
        _ => None,
    }
}

fn json_str(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => {
            let t = s.trim();
            if t.is_empty() {
                None
            } else {
                Some(t.to_string())
            }
        }
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn json_id_list(v: &serde_json::Value) -> Vec<i64> {
    match v {
        serde_json::Value::Array(arr) => arr.iter().filter_map(json_i64).collect(),
        single => json_i64(single).into_iter().collect(),
    }
}

/// Collect candidate objects in provider precedence order:
/// top-level, then OneSignal `custom.a`, `custom`, `additionalData`
/// variants, `data`. The first candidate holding a key wins per field,
/// so backend-placed `additionalData` overrides nothing when top-level
/// already routes.
fn candidate_objects(root: &serde_json::Value) -> Vec<&serde_json::Map<String, serde_json::Value>> {
    let mut out = Vec::new();
    if let Some(m) = root.as_object() {
        out.push(m);
        // OneSignal iOS NSE: userInfo["custom"]["a"] holds additionalData.
        if let Some(a) = m
            .get("custom")
            .and_then(|c| c.get("a"))
            .and_then(|a| a.as_object())
        {
            out.push(a);
        }
        if let Some(c) = m.get("custom").and_then(|c| c.as_object()) {
            out.push(c);
        }
        for key in [
            "additionalData",
            "additional_data",
            "additionaldata",
            "data",
            "os_data",
        ] {
            if let Some(o) = m.get(key).and_then(|v| v.as_object()) {
                out.push(o);
            }
        }
    }
    out
}

fn find_first(
    candidates: &[&serde_json::Map<String, serde_json::Value>],
    keys: &[&str],
) -> Option<serde_json::Value> {
    for c in candidates {
        for k in keys {
            if let Some(v) = c.get(*k) {
                if !v.is_null() {
                    return Some(v.clone());
                }
            }
        }
    }
    None
}

/// Parse a provider push payload into a [`PushEnvelope`].
///
/// Accepts raw JSON bytes from any provider (APNs/FCM/OneSignal). Reads
/// only routing + fetch keys; notification display text (`aps.alert`,
/// `title`, `body`, `message`) is deliberately ignored and must NEVER be
/// persisted as a message row — bodies always come from `get_messages`.
///
/// OneSignal placement: put the handoff under Additional Data
/// (`additionalData`); iOS reads `userInfo["custom"]["a"]`, Android reads
/// `OSNotification.additionalData`. Large ids SHOULD be decimal strings
/// (JS-number precision); both strings and numbers parse.
///
/// Lenient: missing routing yields an envelope with `None` fields (the
/// manager defers to account-level catch-up); only malformed JSON fails.
pub fn parse_push_payload(bytes: &[u8]) -> Result<PushEnvelope, TinkleError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| TinkleError::Codec(format!("push payload must be UTF-8 JSON: {e}")))?;
    let root: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| TinkleError::Codec(format!("push payload must be JSON: {e}")))?;
    if !root.is_object() {
        return Err(TinkleError::Codec(
            "push payload must be a JSON object".into(),
        ));
    }
    let candidates = candidate_objects(&root);

    // Absent version defaults to v1 (back-compat); present-but-unparsable
    // must NOT fail open to v1 — defer as version_mismatch instead.
    let version = match find_first(&candidates, &["v", "version", "ver"]) {
        None => PUSH_ENVELOPE_VERSION,
        Some(raw) => json_u32(&raw).unwrap_or(u32::MAX),
    };

    let account_user_id = find_first(
        &candidates,
        &[
            "account_id",
            "accountId",
            "account",
            "recipient_id",
            "recipientId",
            "recipient",
            "user_id",
            "userId",
            "account_user_id",
        ],
    )
    .and_then(|v| json_i64(&v));

    let target_kind = find_first(
        &candidates,
        &[
            "target_kind",
            "targetKind",
            "kind",
            "peer_kind",
            "peerKind",
            "target_type",
            "targetType",
        ],
    )
    .and_then(|v| json_str(&v))
    .map(|s| s.to_lowercase())
    .filter(|s| !s.is_empty());

    let target_id = find_first(
        &candidates,
        &[
            "target_id",
            "targetId",
            "chat_id",
            "chatId",
            "peer_id",
            "peerId",
            "conversation_id",
            "conversationId",
            "target",
        ],
    )
    .and_then(|v| json_i64(&v));

    let mut message_ids = find_first(
        &candidates,
        &[
            "message_ids",
            "messageIds",
            "mids",
            "message_id",
            "messageId",
            "ids",
            "id",
        ],
    )
    .map(|v| json_id_list(&v))
    .unwrap_or_default();
    message_ids.sort_unstable();
    message_ids.dedup();
    // Retain the highest (newest) ids: the list is sorted ascending, so
    // excess lowest ids are removed from the front.
    if message_ids.len() > PUSH_MAX_MESSAGE_IDS {
        let excess = message_ids.len() - PUSH_MAX_MESSAGE_IDS;
        message_ids.drain(..excess);
    }

    let event_id = find_first(
        &candidates,
        &[
            "event_id",
            "eventId",
            "dedup_id",
            "dedupId",
            "notification_id",
            "notificationId",
            "i",
        ],
    )
    .and_then(|v| json_str(&v))
    .unwrap_or_default();

    Ok(PushEnvelope {
        version,
        account_user_id,
        target_kind,
        target_id,
        message_ids,
        event_id,
    })
}

/// Canonical target key for foreground/open-event forwarding, if routing
/// is present. Hosts use this to navigate + `reconcile_target` when the
/// user opens the notification (no fetch happens here).
pub fn push_envelope_target_key(envelope: &PushEnvelope) -> Option<String> {
    envelope.target_key()
}

// -- manager -----------------------------------------------------------------

/// Push wake driver: bounded, supervisor-independent sync for one envelope.
pub struct PushManager {
    client: Client,
}

impl PushManager {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Process one envelope with the default wake budget (25s).
    pub async fn process_push(&self, envelope: PushEnvelope) -> PushOutcome {
        self.process_push_with_deadline(
            envelope,
            Instant::now() + std::time::Duration::from_secs(PUSH_DEFAULT_BUDGET_SECS),
        )
        .await
    }

    /// Process one envelope, racing every network step against `deadline`.
    /// Cancellation-safe: dropping the future leaves the durable job
    /// `claimed`; the next wake requeues stale claims and retries.
    pub async fn process_push_with_deadline(
        &self,
        envelope: PushEnvelope,
        deadline: Instant,
    ) -> PushOutcome {
        let now = now_unix();
        // Fast path: deadline already gone — persist what we can, defer.
        if Instant::now() >= deadline {
            self.persist_for_defer(&envelope, now).await;
            return PushOutcome::Deferred {
                reason: "deadline".into(),
            };
        }

        // 1. Version gate (forward-compatible: defer, never fail open).
        if envelope.version != PUSH_ENVELOPE_VERSION {
            self.persist_account_job(&envelope, now).await;
            return PushOutcome::Deferred {
                reason: "version_mismatch".into(),
            };
        }

        // 2. Routing gate: unknown/missing target → account-level
        //    catch-up, never guess.
        let target_key = match envelope.target_key() {
            Some(k) => k,
            None => {
                self.persist_account_job(&envelope, now).await;
                return PushOutcome::Deferred {
                    reason: "missing_routing".into(),
                };
            }
        };
        let target = match envelope.target() {
            Some(t) => t,
            None => {
                self.persist_account_job(&envelope, now).await;
                return PushOutcome::Deferred {
                    reason: "missing_routing".into(),
                };
            }
        };

        // 3. Account gate: envelope must name an account and it must match
        //    the currently authenticated principal (epoch/generation fence
        //    vs logout/switch). Mismatch defers under the envelope's
        //    account so the right login retries it.
        let wanted = match envelope.account_principal() {
            Some(p) => p,
            None => {
                self.persist_account_job(&envelope, now).await;
                return PushOutcome::Deferred {
                    reason: "missing_account".into(),
                };
            }
        };
        let (mut current_principal, current_gen) = self.client.push_fence_snapshot().await;
        if current_principal == crate::sync::UNKNOWN_PRINCIPAL {
            // Cold start (NSE/worker never restored the in-memory principal):
            // adopt the envelope's account ONLY with durable evidence of
            // prior auth, so a true logout (storage wiped) still fences as
            // logged_out instead of resurrecting the session.
            let mut restored = false;
            if let Some(id) = envelope.account_user_id.filter(|v| *v > 0) {
                if let Some(storage) = self.client.sqlite_storage() {
                    let wanted_c = wanted.clone();
                    let target_c = target_key.clone();
                    let has_evidence = tokio::task::spawn_blocking(move || {
                        if let Ok(jobs) = storage.list_background_sync_jobs(&wanted_c, None) {
                            // Push-path jobs self-attest: a wake persists one
                            // even while logged out, so they must never count
                            // as prior-auth evidence (otherwise one logged-out
                            // wake resurrects the next).
                            if jobs
                                .iter()
                                .any(|j| j.kind != PUSH_JOB_KIND && j.kind != PUSH_ACCOUNT_JOB_KIND)
                            {
                                return true;
                            }
                        }
                        // Principal-wide dialog state also counts: a signed-in
                        // client may hold cached conversations without rows
                        // for this exact target.
                        if let Ok(dialogs) = storage.list_dialogs(&wanted_c, 1) {
                            if !dialogs.is_empty() {
                                return true;
                            }
                        }
                        storage
                            .get_messages(&wanted_c, &target_c, 1, None)
                            .map(|rows| !rows.is_empty())
                            .unwrap_or(false)
                    })
                    .await
                    .unwrap_or(false);
                    if has_evidence {
                        self.client.set_principal_user_id(id).await;
                        restored = true;
                    }
                }
            }
            if restored {
                current_principal = self.client.push_fence_snapshot().await.0;
            } else {
                self.persist_envelope_job(&wanted, &envelope, &target_key, now)
                    .await;
                return PushOutcome::Deferred {
                    reason: "logged_out".into(),
                };
            }
        }
        if current_principal != wanted {
            self.persist_envelope_job(&wanted, &envelope, &target_key, now)
                .await;
            return PushOutcome::Deferred {
                reason: "account_mismatch".into(),
            };
        }

        // 4. Durable enqueue (first-wins) + stale-claim recovery + claim.
        //    No SQLite (wasm / cursor-only client) cannot fence races.
        let storage = match self.client.sqlite_storage() {
            Some(s) => s,
            None => {
                return PushOutcome::Failed {
                    reason: "no_storage".into(),
                };
            }
        };
        let job_id = push_job_id(&envelope);
        let payload = serde_json::to_vec(&serde_json::json!({
            "v": envelope.version,
            "account_id": envelope.account_user_id,
            "target_kind": envelope.target_kind,
            "target_id": envelope.target_id,
            "message_ids": envelope.message_ids,
            "event_id": envelope.event_id,
        }))
        .unwrap_or_default();
        let enqueued = {
            let st = storage.clone();
            let wanted_c = wanted.clone();
            let job = tinkle_storage::BackgroundSyncJob {
                job_id: job_id.clone(),
                kind: PUSH_JOB_KIND.to_string(),
                target_key: target_key.clone(),
                state: "pending".to_string(),
                payload,
                attempts: 0,
                next_due: now,
                claimed_by: None,
                updated_at: now,
            };
            tokio::task::spawn_blocking(move || st.enqueue_background_job(&wanted_c, &job)).await
        };
        match enqueued {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if e.to_string().contains("no such table") {
                    // Pre-v4 database: proceed without durability.
                } else {
                    tracing::warn!(error = %e, job_id, "push enqueue failed; not persisted");
                    return PushOutcome::Deferred {
                        reason: format!("transient:enqueue:{e}"),
                    };
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, job_id, "push enqueue task failed; not persisted");
                return PushOutcome::Deferred {
                    reason: "transient:enqueue_task".into(),
                };
            }
        }
        // Recover claims leaked by a killed extension before claiming.
        // Best-effort: log failures but continue — a stale claim only
        // forces this wake to lose the race, never to corrupt state.
        {
            let st = storage.clone();
            let wanted_c = wanted.clone();
            match tokio::task::spawn_blocking(move || {
                st.requeue_stale_background_jobs(&wanted_c, now - PUSH_CLAIM_LEASE_SECS, now, now)
            })
            .await
            {
                Ok(Ok(n)) => {
                    if n > 0 {
                        tracing::debug!(requeued = n, "push requeued stale claims");
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "push requeue stale claims failed");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "push requeue task failed");
                }
            }
        }
        let owner = push_owner_id();
        let claimed = {
            let st = storage.clone();
            let (w, j, o) = (wanted.clone(), job_id.clone(), owner.clone());
            tokio::task::spawn_blocking(move || st.claim_background_job(&w, &j, &o, None, now))
                .await
        };
        let claimed = match claimed {
            Ok(Ok(true)) => true,
            Ok(Ok(false)) => false,
            Ok(Err(e)) => {
                if e.to_string().contains("no such table") {
                    // No durability: single attempt, no fencing.
                    true
                } else {
                    return PushOutcome::Deferred {
                        reason: format!("transient:claim:{e}"),
                    };
                }
            }
            Err(_) => {
                return PushOutcome::Deferred {
                    reason: "transient:claim_task".into(),
                };
            }
        };
        if !claimed {
            // Lost the app/extension race, or an idempotent replay.
            let done = {
                let st = storage.clone();
                let (w, j) = (wanted.clone(), job_id.clone());
                tokio::task::spawn_blocking(move || st.get_background_job(&w, &j))
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .flatten()
                    .map(|job| job.state == "done")
                    .unwrap_or(false)
            };
            if done {
                return PushOutcome::Committed {
                    target_key,
                    message_ids: envelope.message_ids.clone(),
                };
            }
            return PushOutcome::Deferred {
                reason: "already_claimed".into(),
            };
        }

        // From here we hold the claim; every early return must release —
        // but only our own claim (never steal another owner, never reopen
        // completed work).
        let release = |storage: &std::sync::Arc<tinkle_storage::Storage>,
                       wanted: &str,
                       job_id: &str,
                       owner: &str| {
            let (st, w, j, o) = (
                storage.clone(),
                wanted.to_string(),
                job_id.to_string(),
                owner.to_string(),
            );
            let due = now_unix() + 30;
            let at = now_unix();
            async move {
                let _ = tokio::task::spawn_blocking(move || {
                    let owned = match st.get_background_job(&w, &j) {
                        Ok(Some(job)) => job.claimed_by.as_deref() == Some(o.as_str()),
                        _ => false,
                    };
                    if owned {
                        let _ = st.release_background_job(&w, &j, due, at);
                    } else {
                        tracing::debug!(job_id = %j, "push release skipped: not owner");
                    }
                })
                .await;
            }
        };
        let complete = |storage: &std::sync::Arc<tinkle_storage::Storage>,
                        wanted: &str,
                        job_id: &str,
                        state: &str| {
            let (st, w, j, s) = (
                storage.clone(),
                wanted.to_string(),
                job_id.to_string(),
                state.to_string(),
            );
            let at = now_unix();
            async move {
                let _ =
                    tokio::task::spawn_blocking(move || st.complete_background_job(&w, &j, &s, at))
                        .await;
            }
        };

        if Instant::now() >= deadline {
            release(&storage, &wanted, &job_id, &owner).await;
            return PushOutcome::Deferred {
                reason: "deadline".into(),
            };
        }
        if !self.client.push_fence_check(&wanted, current_gen).await {
            release(&storage, &wanted, &job_id, &owner).await;
            return PushOutcome::Deferred {
                reason: "account_mismatch".into(),
            };
        }

        // 5. Independent connection: never borrow the foreground
        //    supervisor/socket state beyond `connect()` (idempotent).
        if !self.client.is_connected().await {
            let connect_fut = self.client.connect();
            match tokio::time::timeout_at(deadline, connect_fut).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    release(&storage, &wanted, &job_id, &owner).await;
                    return PushOutcome::Deferred {
                        reason: format!("connect:{e}"),
                    };
                }
                Err(_) => {
                    release(&storage, &wanted, &job_id, &owner).await;
                    return PushOutcome::Deferred {
                        reason: "deadline".into(),
                    };
                }
            }
        }
        // Re-snapshot generation once connected (cold extension had none).
        // Prefer the pre-connect snapshot: adopting the post-connect
        // generation re-baselines onto a fresh scope and defeats the fence.
        let (_, live_gen) = self.client.push_fence_snapshot().await;
        let fence_gen = current_gen.or(live_gen);
        if !self.client.push_fence_check(&wanted, fence_gen).await {
            release(&storage, &wanted, &job_id, &owner).await;
            return PushOutcome::Deferred {
                reason: "account_mismatch".into(),
            };
        }

        // 6. Authoritative fetch (NEVER notification text). Empty
        //    `message_ids` still reconciles mutations via step 8.
        if !envelope.message_ids.is_empty() {
            let fetched_messages;
            let fetched_users;
            let fetched_chats;
            let ids: Vec<i64> = envelope
                .message_ids
                .iter()
                .copied()
                .filter(|v| *v > 0)
                .collect();
            if ids.is_empty() {
                complete(&storage, &wanted, &job_id, "failed").await;
                return PushOutcome::Failed {
                    reason: "invalid:message_ids".into(),
                };
            }
            let fetch_fut = self.client.get_messages(target, ids);
            match tokio::time::timeout_at(deadline, fetch_fut).await {
                Ok(Ok(resp)) => {
                    fetched_messages = resp.messages;
                    fetched_users = resp.users;
                    fetched_chats = resp.chats;
                }
                Ok(Err(e)) => match e.recovery_decision() {
                    RecoveryDecision::Auth => {
                        complete(&storage, &wanted, &job_id, "failed").await;
                        return PushOutcome::Failed {
                            reason: format!("auth:{e}"),
                        };
                    }
                    RecoveryDecision::Fail => {
                        // Terminal only for genuinely permanent errors.
                        // Local/infra failures (no connection, sync or
                        // storage faults) release for retry instead of
                        // completing the job as failed.
                        if matches!(
                            e,
                            TinkleError::NotConnected(_)
                                | TinkleError::Sync(_)
                                | TinkleError::CursorStore(_)
                        ) {
                            release(&storage, &wanted, &job_id, &owner).await;
                            return PushOutcome::Deferred {
                                reason: format!("transient:{e}"),
                            };
                        }
                        complete(&storage, &wanted, &job_id, "failed").await;
                        return PushOutcome::Failed {
                            reason: format!("invalid:{e}"),
                        };
                    }
                    _ => {
                        release(&storage, &wanted, &job_id, &owner).await;
                        return PushOutcome::Deferred {
                            reason: format!("transient:{e}"),
                        };
                    }
                },
                Err(_) => {
                    release(&storage, &wanted, &job_id, &owner).await;
                    return PushOutcome::Deferred {
                        reason: "deadline".into(),
                    };
                }
            }
            if Instant::now() >= deadline {
                release(&storage, &wanted, &job_id, &owner).await;
                return PushOutcome::Deferred {
                    reason: "deadline".into(),
                };
            }
            if !self.client.push_fence_check(&wanted, fence_gen).await {
                release(&storage, &wanted, &job_id, &owner).await;
                return PushOutcome::Deferred {
                    reason: "account_mismatch".into(),
                };
            }
            // 7. Commit via the shared ingest (same-cursor, never moves
            //    the journal). Omitted ids (deleted/invisible) simply
            //    commit nothing — still Committed.
            if !fetched_messages.is_empty()
                || !fetched_users.is_empty()
                || !fetched_chats.is_empty()
            {
                let empty = std::collections::HashSet::new();
                match self
                    .client
                    .ingest_messages(
                        &wanted,
                        &target_key,
                        &fetched_messages,
                        &fetched_users,
                        &fetched_chats,
                        &empty,
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("GenerationMismatch")
                            || msg.contains("generation")
                            || msg.contains("ScopeMismatch")
                            || msg.contains("scope")
                        {
                            release(&storage, &wanted, &job_id, &owner).await;
                            return PushOutcome::Deferred {
                                reason: "account_mismatch".into(),
                            };
                        }
                        // Storage write failure: retryable unless input.
                        if msg.contains("InvalidInput") || msg.contains("invalid input") {
                            complete(&storage, &wanted, &job_id, "failed").await;
                            return PushOutcome::Failed {
                                reason: format!("invalid:{e}"),
                            };
                        }
                        release(&storage, &wanted, &job_id, &owner).await;
                        return PushOutcome::Deferred {
                            reason: format!("transient:{e}"),
                        };
                    }
                }
            }
        }

        // 8. Reconcile mutations + bounded catch-up (best-effort: bodies
        //    above are already durable, so reconcile failure still commits).
        if Instant::now() < deadline {
            let reconcile_fut = self.client.reconcile_target(&target_key);
            match tokio::time::timeout_at(deadline, reconcile_fut).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    tracing::debug!(target_key, error = %e, "push reconcile failed");
                }
                Err(_) => {
                    tracing::debug!(target_key, "push reconcile hit deadline");
                }
            }
        }

        complete(&storage, &wanted, &job_id, "done").await;
        PushOutcome::Committed {
            target_key,
            message_ids: envelope.message_ids.clone(),
        }
    }

    /// Best-effort persist for deadline paths (job survives process death).
    async fn persist_for_defer(&self, envelope: &PushEnvelope, now: i64) {
        self.persist_account_job(envelope, now).await;
    }

    async fn persist_account_job(&self, envelope: &PushEnvelope, now: i64) {
        let principal = match envelope.account_principal() {
            Some(p) => p,
            None => match self.client.sqlite_storage() {
                Some(_) => self.client.push_fence_snapshot().await.0,
                None => return,
            },
        };
        if principal == crate::sync::UNKNOWN_PRINCIPAL {
            return;
        }
        let Some(storage) = self.client.sqlite_storage() else {
            return;
        };
        let target_key = envelope.target_key().unwrap_or_default();
        let job_id = push_job_id(envelope);
        let kind = if target_key.is_empty() {
            PUSH_ACCOUNT_JOB_KIND
        } else {
            PUSH_JOB_KIND
        };
        let payload = serde_json::to_vec(&serde_json::json!({
            "v": envelope.version,
            "account_id": envelope.account_user_id,
            "target_kind": envelope.target_kind,
            "target_id": envelope.target_id,
            "message_ids": envelope.message_ids,
            "event_id": envelope.event_id,
        }))
        .unwrap_or_default();
        let job = tinkle_storage::BackgroundSyncJob {
            job_id,
            kind: kind.to_string(),
            target_key,
            state: "pending".to_string(),
            payload,
            attempts: 0,
            next_due: now,
            claimed_by: None,
            updated_at: now,
        };
        // Insert-if-absent: never upsert over a claimed/done job (would
        // reopen completed work or steal another invocation's claim).
        let _ = tokio::task::spawn_blocking(move || {
            let _ = storage.enqueue_background_job(&principal, &job);
        })
        .await;
    }

    async fn persist_envelope_job(
        &self,
        principal: &str,
        envelope: &PushEnvelope,
        target_key: &str,
        now: i64,
    ) {
        let Some(storage) = self.client.sqlite_storage() else {
            return;
        };
        let payload = serde_json::to_vec(&serde_json::json!({
            "v": envelope.version,
            "account_id": envelope.account_user_id,
            "target_kind": envelope.target_kind,
            "target_id": envelope.target_id,
            "message_ids": envelope.message_ids,
            "event_id": envelope.event_id,
        }))
        .unwrap_or_default();
        let job = tinkle_storage::BackgroundSyncJob {
            job_id: push_job_id(envelope),
            kind: PUSH_JOB_KIND.to_string(),
            target_key: target_key.to_string(),
            state: "pending".to_string(),
            payload,
            attempts: 0,
            next_due: now,
            claimed_by: None,
            updated_at: now,
        };
        let (st, p) = (storage.clone(), principal.to_string());
        // Insert-if-absent: never upsert over a claimed/done job.
        let _ = tokio::task::spawn_blocking(move || {
            let _ = st.enqueue_background_job(&p, &job);
        })
        .await;
    }

    /// Cold-start recovery: requeue stale `claimed` push jobs whose lease
    /// expired so a killed extension's work retries. Returns requeued count.
    pub async fn requeue_stale_claims(&self, principal: &str, lease_secs: i64) -> usize {
        let Some(storage) = self.client.sqlite_storage() else {
            return 0;
        };
        let now = now_unix();
        let (st, p) = (storage.clone(), principal.to_string());
        tokio::task::spawn_blocking(move || {
            st.requeue_stale_background_jobs(&p, now - lease_secs, now, now)
                .unwrap_or(0)
        })
        .await
        .unwrap_or(0)
    }
}

// -- typed push-token lifecycle + logout -------------------------------------

/// Push token platform for `register_push`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushTokenKind {
    Apns,
    ApnsVoip,
    Fcm,
}

impl PushTokenKind {
    fn proto_value(self) -> i32 {
        match self {
            Self::Apns => 1,
            Self::ApnsVoip => 2,
            Self::Fcm => 3,
        }
    }

    /// Parse `"apns"` / `"apns_voip"` (also `"voip"`) / `"fcm"`
    /// (case-insensitive). Unknown strings are codec errors, never guessed.
    pub fn parse(s: &str) -> Result<Self, TinkleError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "apns" | "apns_sandbox" => Ok(Self::Apns),
            "apns_voip" | "apns-voip" | "voip" => Ok(Self::ApnsVoip),
            "fcm" => Ok(Self::Fcm),
            other => Err(TinkleError::Codec(format!(
                "unknown push token type '{other}' (want apns/apns_voip/fcm)"
            ))),
        }
    }
}

impl Client {
    /// Register a push token (`AccountService/RegisterPush`).
    /// `token_type` is `"apns"` / `"apns_voip"` / `"fcm"`.
    /// Call after sign-in and on every token rotation; the server keeps
    /// only the latest token per device.
    pub async fn register_push(
        &self,
        token: String,
        token_type: PushTokenKind,
        lang_code: String,
    ) -> Result<(), TinkleError> {
        if token.trim().is_empty() {
            return Err(TinkleError::Codec("push token must not be blank".into()));
        }
        let req = tinkle_schema::proto::RegisterPushRequest {
            token,
            r#type: token_type.proto_value(),
            lang_code,
        };
        let _resp: tinkle_schema::proto::RegisterPushResponse = crate::services::call_typed(
            self,
            tinkle_schema::methods::accountservice::REGISTER_PUSH,
            &req,
            30_000,
        )
        .await?;
        Ok(())
    }

    /// Unregister a push token (`AccountService/UnregisterPush`).
    /// Returns the server's `removed` flag. Call before logout so a
    /// signed-out device stops receiving wakes.
    pub async fn unregister_push(&self, token: String) -> Result<bool, TinkleError> {
        if token.trim().is_empty() {
            return Err(TinkleError::Codec("push token must not be blank".into()));
        }
        let req = tinkle_schema::proto::UnregisterPushRequest { token };
        let resp: tinkle_schema::proto::UnregisterPushResponse = crate::services::call_typed(
            self,
            tinkle_schema::methods::accountservice::UNREGISTER_PUSH,
            &req,
            30_000,
        )
        .await?;
        Ok(resp.removed)
    }

    /// Logout + local cleanup: best-effort `AccountService/Logout` RPC,
    /// then wipes the principal's SQLite rows (messages, jobs, transfers),
    /// drops in-memory catch-up demand, clears tracked interests, and
    /// resets the principal to unknown (fencing in-flight push/reconcile
    /// work). Returns the server's `logged_out` flag (`false` when the RPC
    /// was skipped/failed; local wipe still happened).
    ///
    /// Recommended host order: `unregister_push(token)` → `logout()`.
    pub async fn logout(&self) -> Result<bool, TinkleError> {
        let principal = self.principal().await;
        let mut logged_out = false;
        if self.is_connected().await && principal != crate::sync::UNKNOWN_PRINCIPAL {
            let req = tinkle_schema::proto::LogoutRequest {};
            let attempt: Result<tinkle_schema::proto::LogoutResponse, TinkleError> =
                crate::services::call_typed(
                    self,
                    tinkle_schema::methods::accountservice::LOGOUT,
                    &req,
                    30_000,
                )
                .await;
            if let Ok(resp) = attempt {
                logged_out = resp.logged_out;
            }
        }
        if principal != crate::sync::UNKNOWN_PRINCIPAL {
            if let Some(storage) = self.sqlite_storage() {
                let p = principal.clone();
                tokio::task::spawn_blocking(move || storage.clear_principal(&p))
                    .await
                    .map_err(|e| TinkleError::CursorStore(format!("logout wipe task failed: {e}")))?
                    .map_err(|e| TinkleError::CursorStore(e.to_string()))?;
            }
        }
        self.reset_session_for_logout().await;
        Ok(logged_out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_top_level_and_decimal_strings() {
        let env = parse_push_payload(
            br#"{"v":1,"account_id":"42","target_kind":"room","target_id":"7","message_ids":["100","101"],"event_id":"evt-1"}"#,
        )
        .unwrap();
        assert_eq!(env.version, 1);
        assert_eq!(env.account_user_id, Some(42));
        assert_eq!(env.target_key().as_deref(), Some("target:room:7"));
        assert_eq!(env.message_ids, vec![100, 101]);
        assert_eq!(env.event_id, "evt-1");
    }

    #[test]
    fn parse_onesignal_custom_a_and_numbers() {
        // OneSignal iOS NSE shape: userInfo["custom"]["a"].
        let env = parse_push_payload(
            br#"{"custom":{"a":{"v":1,"account_id":42,"target_kind":"chat","target_id":7,"message_ids":[100],"event_id":"n1"},"i":"n1"},"aps":{"alert":"hello"}}"#,
        )
        .unwrap();
        assert_eq!(env.account_user_id, Some(42));
        // Display text in aps is ignored (never a body).
        assert_eq!(env.target_key().as_deref(), Some("target:room:7"));
        assert_eq!(env.message_ids, vec![100]);
    }

    #[test]
    fn parse_additional_data_and_missing_routing_is_lenient() {
        let env = parse_push_payload(
            br#"{"additionalData":{"v":1,"account_id":42,"message_ids":[5],"event_id":"e"}}"#,
        )
        .unwrap();
        assert_eq!(env.account_user_id, Some(42));
        assert!(env.target_key().is_none());
        assert!(env.target().is_none());
    }

    #[test]
    fn parse_rejects_non_json() {
        assert!(parse_push_payload(b"not json").is_err());
    }

    #[test]
    fn job_id_stable_on_event_id() {
        let a = PushEnvelope {
            version: 1,
            account_user_id: Some(42),
            target_kind: Some("room".into()),
            target_id: Some(7),
            message_ids: vec![2, 1],
            event_id: "evt-x".into(),
        };
        let b = a.clone();
        assert_eq!(push_job_id(&a), push_job_id(&b));
        assert!(push_job_id(&a).starts_with("push:evt-x"));
    }

    #[test]
    fn token_kind_parses_known_only() {
        assert_eq!(PushTokenKind::parse("apns").unwrap(), PushTokenKind::Apns);
        assert_eq!(
            PushTokenKind::parse("APNS_VOIP").unwrap(),
            PushTokenKind::ApnsVoip
        );
        assert_eq!(PushTokenKind::parse("fcm").unwrap(), PushTokenKind::Fcm);
        assert!(PushTokenKind::parse("sms").is_err());
    }
}
