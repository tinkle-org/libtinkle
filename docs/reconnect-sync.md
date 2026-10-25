# Reconnect and synchronization

## Reconnect

Every new transport connection requires a new Syntra handshake with
fresh credential presentation. The `request_id` space dies with the
connection; subscriptions, live cursors, and in-flight RPCs do not
survive.

On loss, all in-flight RPCs report `UNAVAILABLE` with unknown outcome
(`is_unknown_outcome()==true`). Retry only when no terminal `result`
arrived AND the operation is idempotent or carries an application
operation key (`random_id` for sends), with a fresh `request_id`, bounded
retries, and jittered backoff (`ReconnectPolicy`: 500 ms base, ×2,
30 s cap, 10 attempts default).

## Synchronization

1. Authenticate and establish the principal (`user:<id>`). Set it via
   `sign_in` or `set_principal_user_id`; checkpoints are isolated by
   principal and never reused across accounts.
2. Hydrate memory cursors from `CursorStore` (absent = zero, never
   start-live).
3. Catch up over actual gRPC `ReadUpdates{stream, after_cursor, limit}`.
   Pages arrive in increasing cursor order with `replay_floor` and `head`.
4. Validate each `Update` (stream/cursor/event_id/payload agreement)
   before any effects.
5. Explicit-store path (no native SQLite): emit `UpdateReady`; the app
   applies data plus required refreshes, then calls
   `acknowledge(stream, cursor)` to persist the checkpoint and emit
   `Applied`. Receipt alone never advances the checkpoint.
   Native SQLite path (`Client::new_sqlite`): project + commit cache and
   checkpoint atomically, then emit `Applied` directly with no explicit
   `acknowledge` needed.
6. Start Syntra follows (`FollowUpdates`, `ObserveConnectivity`) after
   the stored cursor. The server replays retained suffix before live
   delivery, covering commits between gRPC pages and subscription.
7. Deduplicate by `(stream, cursor)`: `c==a` duplicate, `c<a` stale,
   `c==a+1` next, `c>a+1` gap (stop, retain checkpoint, recover).
8. Floor/head: `after < floor` expires (rebuild path), `after == floor`
   valid, `after > head` invalid.
9. Rebuild stops one stream; head is diagnostic, not a snapshot fence.
10. Connectivity observations are ephemeral, never checkpointed, cleared
    on disconnect.
11. Sends persist `{request, random_id≠0}` before transmitting; lost
    results retry unchanged with fresh correlation.

## What this SDK does not claim

Exactly-once delivery, cross-connection `request_id` deduplication,
infinite retention, or snapshot fencing. Effectively-once behavior comes
only from application idempotency with explicit keys and duplicate handling.
