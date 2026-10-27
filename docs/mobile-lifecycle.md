# Mobile lifecycle

Mobile OSes do not permit a permanent background WebSocket. Design for
suspension, termination, and network churn; durable state survives in
the `CursorStore`, live state does not.

## States

- **Foreground**: supervised `start(targets, watch)` (or manual `connect()`
  → ACTIVE) — RPCs, subscriptions, presence live; the supervisor owns
  re-reconciliation/reconnect from here on. Hosts consume `next_event()`,
  polling `target_cursor()`/`target_status()`/`tracked_targets()` for
  authoritative progress.
- **Background suspension**: call `suspend()` (graceful close, keep
  durable cursors; supervisor blocked until `resume()`). The OS will
  suspend/kill the socket anyway; explicit close avoids half-open id reuse
  and delivers `UNAVAILABLE` locally instead of silent stall.
- **Process termination**: durable cursors + pending-send `random_id`s
  survive (store + outbox). `request_id`s, subscriptions, in-flight RPCs
  do not — they die with the connection by design.
- **Network change** (WiFi ↔ cellular, captive portal, airplane):
  transport loss → `UNAVAILABLE` (unknown outcome) → `reconnect()` with
  backoff + fresh handshake + explicit resubscribe.
- **Token expiry**: `UNAUTHENTICATED` → `rotate_credential(next)` (new
  transport + fresh handshake; re-auth on the same connection is
  forbidden). Never retry identical bytes in a tight loop.
- **App restart**: `Client::new` with the same store → `start()` (or
  `connect()` → `subscribe` with the tracked targets on the manual path);
  durable per-target accept positions resume reconciliation where it left
  off.

## Push-triggered sync after wake

Push (APNs/FCM, first via OneSignal) carries only a wake hint — never
message bodies or secrets. Notification display text (`aps.alert`,
titles, OneSignal headings) is untrusted for storage: bodies are always
fetched authoritatively via `GetMessages`/`GetHistory` and committed
through the shared `ingest_messages` helper. On wake the host parses the
provider payload into a versioned [`PushEnvelope`][push] and drives
[`PushManager::process_push`][push] on a bounded executor independent of
the foreground supervisor/socket:

```rust
use libtinkle::{Client, PushManager, PushOutcome};

// `payload`: raw provider bytes — full APNs `userInfo` on iOS (incl.
// OneSignal `custom.a`), the `additionalData` JSON string on Android,
// or the backend handoff object directly.
async fn on_push_wake(client: &Client, payload: &[u8]) {
    let envelope = match libtinkle::parse_push_payload(payload) {
        Ok(e) => e,
        Err(e) => { tracing::debug!(%e, "ignoring malformed push"); return; }
    };
    // 25s default budget (NSE ≈ 30s); pass an explicit deadline to bound tighter.
    match PushManager::new(client.clone()).process_push(envelope).await {
        PushOutcome::Committed { target_key, message_ids } => {
            // Refresh UI for `target_key` / `message_ids` from SQLite.
        }
        PushOutcome::Deferred { reason } => {
            // Durable job requeued (`already_claimed`, `deadline`,
            // `account_mismatch`, `missing_routing`, ...); foreground
            // `reconcile_now()` will pick it up.
        }
        PushOutcome::Failed { reason } => {
            // Terminal for this wake (`auth`, `invalid`, `no_storage`).
        }
    }
}
```

Pipeline per wake: validate envelope (unknown/missing routing →
deferred account-level catch-up, never guess a target) → persist pending
work to `background_sync_jobs` (process-safe claim via `claimed_by` +
`attempts`; survives process death) → resolve the matching authenticated
account (epoch/generation fence vs logout/switch) → fetch authoritative
bodies → commit via shared `ingest_messages` (same-cursor, never moves
the journal) → reconcile mutations + bounded catch-up → exactly-once
completion (`done` only after commit; deferred releases back to `pending`
for retry). Cancellation-safe (dropping the wake leaves the job
`claimed`; the next wake requeues stale claims) and deadline-aware
(expiry persists the job and defers).

UniFFI hosts use the same pipeline without touching Rust async
(`tinkle-ffi::push`):

```swift
// iOS Notification Service Extension (after OneSignalExtension runs):
let env = try parsePushPayload(payload: Data(userInfoJson.utf8))
let outcome = try await client.handlePushWake(envelope: env, timeoutSecs: 25)
// Foreground open: navigate, then refresh.
if let key = pushTargetKey(envelope: env) { showConversation(key) }
try await client.reconcileNow()
let _ = try await client.openPushNotification(envelope: env)
```

```kotlin
// Android: OneSignal INotificationServiceExtension → WorkManager worker:
val env = parsePushPayload(event.notification.additionalData.toString().toByteArray())
when (val o = client.handlePushWake(env, 25uL)) {
    is FfiPushOutcome.Committed -> refreshConversation(o.targetKey)
    is FfiPushOutcome.Deferred -> scheduleRetry()
    is FfiPushOutcome.Failed -> log(o.reason)
}
```

Push-token lifecycle (typed wrappers): `registerPush(token,
"apns"|"apns_voip"|"fcm", langCode)` after sign-in and on every rotation,
`unregisterPush(token)` BEFORE `logout()` so a signed-out device stops
receiving wakes; `logout()` then wipes the principal's SQLite rows and
fences in-flight work. Cold start calls `tinkle_ffi_init()` once, opens
the shared database (iOS: App Group container path shared by the app and
the NSE; Android: app-private file shared with the worker), then
`pushRecoverStaleJobs` to requeue leases leaked by a killed extension.

### Backend handoff envelope spec

The backend places exactly one envelope per push under provider Additional
Data (OneSignal dashboard/API: **Additional Data**; iOS reads
`userInfo["custom"]["a"]`, Android reads
`OSNotification.additionalData`). Keys accept the listed aliases;
missing routing is valid (the client defers to account-level catch-up).

| Field | Keys | Type | Notes |
|---|---|---|---|
| version | `v`, `version`, `ver` | uint | Must be `1`; unknown versions defer. |
| recipient/account | `account_id`, `accountId`, `account`, `recipient_id`, `recipient`, `user_id` | decimal-string or number | Authenticated account this wake belongs to. |
| target | `target_kind` + `target_id` (aliases: `targetKind`/`kind` + `targetId`/`chat_id`/`peer_id`) | string + decimal-string/number | `"user"` (DM) or `"room"`/`"chat"` (group); positive id. Unknown/missing → deferred, never guessed. |
| message IDs | `message_ids` (+ `messageIds`, `mids`, singulars) | array of decimal-string/number | Fetch keys for `GetMessages`; capped at 500/wake. |
| event/dedup id | `event_id` (+ `eventId`, `dedup_id`, `notification_id`, OneSignal `i`) | string | Durable job id (`push:<event_id>`); duplicates replay idempotently. |

Large ids SHOULD be decimal strings (`"4815162342"`, not `4815162342`):
platform JSON bridges (JavaScript, `NSJSONSerialization`, OneSignal
`additionalData`) lose precision past 2^53, and the parser accepts both.
iOS requires `mutable-content: 1` in the APNs payload so the
Notification Service Extension runs; the extension must call the
completion handler within ~30s (use the 25s wake budget). Android FCM
data messages use a `WorkManager` worker (not a foreground socket) to
respect Doze; keep the payload under FCM's 4KB limit (ids only, never
bodies).

Example (Additional Data value):

```json
{
  "v": 1,
  "account_id": "4815162342",
  "target_kind": "room",
  "target_id": "777",
  "message_ids": ["9001", "9002"],
  "event_id": "01J9ZQ5N7M4T6K8P0R2S4V6X8Z"
}
```

## Do not

- Do not assume a background socket survives (iOS suspends, Android
  Dozes).
- Do not treat transport receipt as application commit.
- Do not retry non-idempotent sends without the persisted `random_id`
  and unchanged payload.
- Do not reuse `request_id`s across connections.
- Do not checkpoint presence.
- Do not persist notification display text as a message row — fetch
  `GetMessages` and commit that.
- Do not guess routing: a push without a known target defers to
  account-level catch-up; the foreground reconcile fills it in.
- Do not hold a wake past its deadline: defer and let the durable job
  retry.

[push]: ../crates/libtinkle/src/push.rs
