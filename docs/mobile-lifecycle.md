# Mobile lifecycle

Mobile OSes do not permit a permanent background WebSocket. Design for
suspension, termination, and network churn; durable state survives in
the `CursorStore`, live state does not.

## States

- **Foreground**: `connect()` → ACTIVE. RPCs, subscriptions, presence live.
- **Background suspension**: call `suspend()` (graceful close, keep
  durable cursors). The OS will suspend/kill the socket anyway; explicit
  close avoids half-open id reuse and delivers `UNAVAILABLE` locally
  instead of silent stall.
- **Process termination**: durable cursors + pending-send `random_id`s
  survive (store + outbox). `request_id`s, subscriptions, in-flight RPCs
  do not — they die with the connection by design.
- **Network change** (WiFi ↔ cellular, captive portal, airplane):
  transport loss → `UNAVAILABLE` (unknown outcome) → `reconnect()` with
  backoff + fresh handshake + explicit resubscribe.
- **Token expiry**: `UNAUTHENTICATED` → `rotate_credential(next)` (new
  transport + fresh handshake; re-auth on the same connection is
  forbidden). Never retry identical bytes in a tight loop.
- **App restart**: `Client::new` with the same store → `connect()` →
  `hydrate_from_store` → `subscribe` with persisted `after` cursors.

## Push-triggered sync after wake

Push (APNs/FCM) carries only a wake hint (never message bodies or
secrets). On wake:

1. `connect()` with the persisted session credential.
2. `hydrate_from_store` for own + room streams.
3. `subscribe(rooms with after, watch)` — absent means zero, never
   start-live; `after < floor` expires (reset path), `after > head`
   is invalid.
4. Validate + atomically commit batches; deduplicate by
   `(stream, cursor)`; stop on gaps; handle resets via snapshot path
   (currently reports unavailable until a fenced snapshot exists).
5. Presence is re-established live; it is never replayed from storage.

```rust
// Pseudo: push handler
async fn on_push_wake(client: &Client) -> Result<(), TinkleError> {
    client.reconnect().await?;
    Ok(())
}
```

## Do not

- Do not assume a background socket survives (iOS suspends, Android
  Dozes).
- Do not treat transport receipt as application commit.
- Do not retry non-idempotent sends without the persisted `random_id`
  and unchanged payload.
- Do not reuse `request_id`s across connections.
- Do not checkpoint presence or reset heads.
