# Mobile lifecycle

Mobile OSes do not guarantee a permanent background connection. Design for
suspension, termination, and network churn; durable state survives in
the `CursorStore`, live state does not.

## States

ACTIVE Returning transports own one application `ConnectionService.KeepAlive`
loop: first request after five seconds, five-second cadence, at most one
outstanding request, and a five-second RPC ceiling. Sequences start at one and
increase only within that connection. Success must echo the exact sequence;
errors, malformed echoes and timeouts retire the socket rather than replaying
the observation. The existing supervisor owns reconnection. Typed terminal
authorization/protocol failures park it and cannot select an alternate port.

The loop uses the normal `Client::call_raw`/InitConnect path. It is independent
of Syntra core ping; only requests actually observed and authorized by the
gateway can renew presence. Bootstrap never sends this protected request. An
idle Bootstrap OTP connection may reach the core's read timeout and reconnect;
the retained enrollment operation remains the recovery authority.

Stop, reconnect, suspend, resume/wake, credential rotation and account-context
retirement cancel and join the old connection's liveness task. Resume retires
an old socket even if suspend was not explicitly called. A local account switch
retires and parks a live Returning context until explicit authenticated trust
installation. Task owners survive cancellation of teardown so a later stop can
still join them. Dropping the last host Client owner cancels background work.

`crates/libtinkle/src/liveness.rs` retains incomplete carrier/policy shutdown
owners; no replacement dial is allowed until their join completes. A bounded
stop/suspend return is not a join acknowledgement. Admission and retirement
observation leave an active provisioning caller's slot intact; abandoned or
explicitly closing children must join before replacement.

- **Foreground**: supervised `start(targets, watch)` (or manual `connect()`
  → ACTIVE) — RPCs, subscriptions, presence live; the supervisor owns
  re-reconciliation/reconnect from here on. Hosts consume `next_event()`,
  polling `target_cursor()`/`target_status()`/`tracked_targets()` for
  authoritative progress.
- **Background suspension**: call `suspend()` (graceful close, keep
  durable cursors; supervisor blocked until `resume()`). The OS will
  suspend/kill the socket anyway; explicit close avoids half-open id reuse
  and delivers `UNAVAILABLE` locally instead of silent stall.
- **Process termination**: durable cursors + pending-send `idempotency_key`s
  survive (store + outbox). `request_id`s, subscriptions, in-flight RPCs
  do not — they die with the connection by design.
- **Network change** (WiFi ↔ cellular, captive portal, airplane):
  transport loss → `UNAVAILABLE` (unknown outcome) → `reconnect()` with
  backoff + fresh handshake + explicit resubscribe.
- **Enrollment expiry/rotation**: obtain fresh authorization through host policy,
  then `replace_trust_source(next)` (new transport + fresh channel proof).
  Store AK material separately from the application cache. Never silently
  change Returning mode to Bootstrap after a rejection.
- **App restart**: `Client::new` with the same store → `start()` (or
  `connect()` → `subscribe` with the tracked targets on the manual path);
  durable per-target accept positions resume reconciliation where it left
  off.

## Push-triggered sync after wake

Logout is distinct from transport stop. It permanently signs this Client out,
retires its key/channel/liveness work, invalidates late enrollment completions,
and requires acknowledged full-namespace secure-store clearing. Its boolean is
remote revocation confirmation only; offline or unknown acknowledgement returns
false after successful local clearing. Storage failure returns an error while
the Client stays signed out. Do not automatically restore a new coordinator
from a namespace whose clearing failed; resolve the host's durable tombstone or
deletion first. See `device-enrollment.md` for the clear callback contract.

Logout cleanup is retained independently of host waiters. Rejoining or retrying
keeps cumulative remote confirmation, the original cache principal and completed
local steps, and never repeats remote Logout. `session_status()` distinguishes
network unavailability, unknown authorization and signed-out cleanup progress.
Protected storage is still invalidated when transport join is incomplete;
cleanup then remains `Failed` until a retry finishes the retained join. Do not
repeat an acknowledged store clear or infer remote rollback from that failure.
Conditional protected-store hosts persist a revision-bound cleanup marker before
clearing; it blocks restoration and late writes after restart. Hosts without
conditional persistence cannot attest restart safety. Conditional restore and
foreground entry observe permanent retirement before Returning. Current
foreground Returning loss/denial coalesces one bounded evidence check per
generation. The 60-second offline fallback skips healthy Returning sockets.
Every status-only Bootstrap connection waits for old transport shutdown/join;
suspend/stop cancel and drain observation. Exact current negative evidence starts
local-only cleanup; expired or unknown records never become usable keys. The
canonical contract is [retirement observation](device-enrollment.md#permanent-retirement-observation).

### Retained account state

Use `Client::subscribe_session_state()` independently of message `next_event()`.
`crates/libtinkle/src/session.rs` retains lifecycle, `SelfLogout`/`SessionRetired`,
cleanup progress, and the optional full session list for one app consumer.
UniFFI's `AccountStateSubscription.next()` exposes the same projection. Call its
`stop()` on consumer teardown to unblock pending reads; stopping a subscription
does not sign out or close the connection.

The SDK refreshes session review on authenticated startup, foreground
reconnection, reconnect, and typed session-change hints, with dirty-read retry.
Hosts need no session polling timer and must not infer sign-out from network
loss. Review prompts and targeted rejection are defined in
[session review and state](device-enrollment.md#session-review-and-state).

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

Foreign hosts use the same pipeline through `tinkle-ffi::push`. Pass the raw
provider payload to `parse_push_payload`, then call `handle_push_wake` with the
decoded envelope and a bounded wake budget. `Committed` permits refreshing the
conversation; `Deferred` needs a later retry; `Failed` carries a sanitized
reason. On foreground open, use `push_target_key` for navigation and
`open_push_notification` for authoritative refresh. The consumer's generated
bindings own language names and async dispatch.

Push-token lifecycle: `register_push(token,
"apns"|"apns_voip"|"fcm", lang_code)` after sign-in and on every rotation,
`unregister_push(token)` BEFORE `logout()` so a signed-out device stops
receiving wakes; `logout()` then wipes the principal's SQLite rows and
fences in-flight work. Cold start calls `tinkle_ffi_init()` once, opens
the shared database (iOS: App Group container path shared by the app and
the NSE; Android: app-private file shared with the worker), then
`push_recover_stale_jobs` to requeue leases left by a killed extension.

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
- Do not retry non-idempotent sends without the persisted `idempotency_key`
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
