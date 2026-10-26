//! UniFFI push adapters: provider-neutral parsing + bounded wake handling.
//!
//! The core pipeline lives in `libtinkle::push` (`PushManager`); this
//! module only maps between FFI-safe types and the async core.
//!
//! Host integration (see `docs/mobile-lifecycle.md` for the full guide):
//!
//! - iOS: `NotificationServiceExtension` (OneSignal: `OneSignalExtension`
//!   runs first, then call `parsePushPayload` on the mutated content's
//!   `userInfo` JSON and `handlePushWake` with a ~25s budget) plus
//!   background `BGAppRefreshTask` callbacks. The extension and the app
//!   open the SAME SQLite file via an App Group container
//!   (`FileManager.default.containerURL(forSecurityApplicationGroupIdentifier:)`).
//! - Android: OneSignal `INotificationServiceExtension` (foreground
//!   service path) or a `WorkManager` worker for FCM data messages with
//!   `setExpedited` + constraints; cold start calls `tinkle_ffi_init()`
//!   then opens the app-private database file shared with the UI process.
//! - Token lifecycle: `registerPush` after sign-in and on every rotation
//!   (`didRegisterForRemoteNotifications` / `onNewToken`), `unregisterPush`
//!   BEFORE `logout` so a signed-out device stops receiving wakes.
//! - Foreground open: `pushTargetKey(envelope)` returns the canonical
//!   `target:user:<id>` / `target:room:<id>` to navigate to; then call
//!   `reconcileNow()` to refresh from the durable cache.
//!
//! OneSignal is the first delivery integration: backend places the
//! envelope under Additional Data; iOS reads
//! `bestAttemptContent.userInfo["custom"]["a"]`, Android reads
//! `OSNotification.additionalData`. Large ids SHOULD be decimal strings.

use std::sync::Arc;

use crate::{FfiError, TinkleClient};

/// FFI-safe push envelope (mirrors `libtinkle::PushEnvelope`).
///
/// All ids accept decimal strings on the wire (parsed in
/// `libtinkle::parse_push_payload`); this record already carries `i64`.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiPushEnvelope {
    pub version: u32,
    pub account_user_id: Option<i64>,
    pub target_kind: Option<String>,
    pub target_id: Option<i64>,
    pub message_ids: Vec<i64>,
    pub event_id: String,
}

impl From<libtinkle::PushEnvelope> for FfiPushEnvelope {
    fn from(e: libtinkle::PushEnvelope) -> Self {
        Self {
            version: e.version,
            account_user_id: e.account_user_id,
            target_kind: e.target_kind,
            target_id: e.target_id,
            message_ids: e.message_ids,
            event_id: e.event_id,
        }
    }
}

impl From<FfiPushEnvelope> for libtinkle::PushEnvelope {
    fn from(e: FfiPushEnvelope) -> Self {
        Self {
            version: e.version,
            account_user_id: e.account_user_id,
            target_kind: e.target_kind,
            target_id: e.target_id,
            message_ids: e.message_ids,
            event_id: e.event_id,
        }
    }
}

/// FFI-safe push wake outcome (mirrors `libtinkle::PushOutcome`).
#[derive(Debug, Clone, uniffi::Enum)]
pub enum FfiPushOutcome {
    /// Bodies are durable in SQLite (plus best-effort reconcile).
    Committed {
        target_key: String,
        message_ids: Vec<i64>,
    },
    /// Not yet committed; the durable job is (re)queued for retry.
    Deferred { reason: String },
    /// Terminal for this wake (never retries with the same inputs).
    Failed { reason: String },
}

impl From<libtinkle::PushOutcome> for FfiPushOutcome {
    fn from(o: libtinkle::PushOutcome) -> Self {
        match o {
            libtinkle::PushOutcome::Committed {
                target_key,
                message_ids,
            } => Self::Committed {
                target_key,
                message_ids,
            },
            libtinkle::PushOutcome::Deferred { reason } => Self::Deferred { reason },
            libtinkle::PushOutcome::Failed { reason } => Self::Failed { reason },
        }
    }
}

/// Parse a provider push payload into an envelope.
///
/// `payload` is the raw JSON bytes: the full APNs `userInfo` dictionary
/// (iOS, including OneSignal `custom.a`), the `additionalData` JSONObject
/// string (Android/OneSignal), or the backend handoff object directly.
/// Display text (`aps.alert`, titles, bodies) is ignored by construction
/// and must NEVER be persisted as a message row.
#[uniffi::export]
pub fn parse_push_payload(payload: Vec<u8>) -> Result<FfiPushEnvelope, FfiError> {
    libtinkle::parse_push_payload(&payload)
        .map(FfiPushEnvelope::from)
        .map_err(FfiError::from)
}

/// Canonical target key for an envelope, if it routes
/// (`target:user:<id>` / `target:room:<id>`).
///
/// Foreground/open-event hook: when the user taps the notification, pass
/// the parsed envelope here to learn where to navigate, then call
/// `reconcileNow()` to refresh that conversation from the durable cache.
#[uniffi::export]
pub fn push_target_key(envelope: FfiPushEnvelope) -> Option<String> {
    libtinkle::push_envelope_target_key(&libtinkle::PushEnvelope::from(envelope))
}

#[uniffi::export(async_runtime = "tokio")]
impl TinkleClient {
    /// Bounded push wake: parse-then-commit without the foreground
    /// supervisor or socket.
    ///
    /// Drives `libtinkle::PushManager` on this client's shared SQLite file
    /// with a bounded executor (`timeout_secs`, `0` selects the 25s
    /// default; capped at 120s): ensures a connection independent of any
    /// foreground supervisor, fetches authoritative bodies via
    /// `get_messages`, commits through the shared ingest, reconciles
    /// mutations within the remaining budget, and completes the durable
    /// job exactly once.
    ///
    /// Cancellation-safe: dropping the foreign future leaves the durable
    /// job `claimed`; the next wake requeues stale claims and retries.
    pub async fn handle_push_wake(
        &self,
        envelope: FfiPushEnvelope,
        timeout_secs: u64,
    ) -> Result<FfiPushOutcome, FfiError> {
        let budget = if timeout_secs == 0 {
            libtinkle::PUSH_DEFAULT_BUDGET_SECS
        } else {
            timeout_secs.min(120)
        };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(budget);
        let mgr = libtinkle::PushManager::new(self.core_client().clone());
        Ok(mgr
            .process_push_with_deadline(libtinkle::PushEnvelope::from(envelope), deadline)
            .await
            .into())
    }

    /// Register a push token (`AccountService/RegisterPush`).
    ///
    /// `token_type` is `"apns"`, `"apns_voip"`, or `"fcm"` (unknown
    /// strings are codec errors, never guessed). Call after sign-in and
    /// on every rotation; the server keeps the latest token per device.
    pub async fn register_push(
        &self,
        token: String,
        token_type: String,
        lang_code: String,
    ) -> Result<(), FfiError> {
        let kind = libtinkle::PushTokenKind::parse(&token_type).map_err(FfiError::from)?;
        self.core_client()
            .register_push(token, kind, lang_code)
            .await
            .map_err(FfiError::from)
    }

    /// Unregister a push token (`AccountService/UnregisterPush`).
    /// Returns the server's `removed` flag. Call BEFORE `logout`.
    pub async fn unregister_push(&self, token: String) -> Result<bool, FfiError> {
        self.core_client()
            .unregister_push(token)
            .await
            .map_err(FfiError::from)
    }

    /// Logout + local cleanup: best-effort `AccountService/Logout`, then
    /// wipes the principal's SQLite rows, drops catch-up demand, and
    /// resets the session so in-flight push/reconcile work fences out.
    /// Returns the server's `logged_out` flag (`false` when the RPC was
    /// skipped or failed; the local wipe still happened).
    pub async fn logout(&self) -> Result<bool, FfiError> {
        self.core_client().logout().await.map_err(FfiError::from)
    }

    /// Foreground open-event hook: navigate to the envelope's target and
    /// refresh it. Returns the canonical target key when the envelope
    /// routes, after running a bounded reconcile pass for that
    /// conversation (`None` when the envelope carries no routing — show
    /// the conversation list instead).
    pub async fn open_push_notification(
        &self,
        envelope: FfiPushEnvelope,
    ) -> Result<Option<String>, FfiError> {
        let core_envelope = libtinkle::PushEnvelope::from(envelope);
        let key = libtinkle::push_envelope_target_key(&core_envelope);
        if let Some(ref target_key) = key {
            let _ = self.core_client().reconcile_target(target_key).await;
        }
        Ok(key)
    }
}

/// Cold-start recovery: requeue stale `claimed` push jobs whose lease
/// expired (e.g. an extension killed mid-wake) so the next wake retries
/// them. Call once at app/extension startup after opening the shared
/// database. Returns the number of jobs requeued.
#[uniffi::export(async_runtime = "tokio")]
pub async fn push_recover_stale_jobs(
    storage: Arc<crate::sqlite::SqliteStorage>,
    principal: String,
    lease_secs: i64,
) -> Result<u64, FfiError> {
    if principal.trim().is_empty() {
        return Err(FfiError::Codec("principal must not be blank".into()));
    }
    if lease_secs < 0 {
        return Err(FfiError::Codec("lease_secs must be non-negative".into()));
    }
    let native = storage.native_handle();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let requeued = tokio::task::spawn_blocking(move || {
        native
            .requeue_stale_background_jobs(&principal, now - lease_secs, now, now)
            .map_err(|e| FfiError::Storage(e.to_string()))
    })
    .await
    .map_err(|e| FfiError::Storage(format!("storage task failed: {e}")))??;
    Ok(requeued as u64)
}
