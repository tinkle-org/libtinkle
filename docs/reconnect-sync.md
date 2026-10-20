# Reconnect and synchronization

## Reconnect

Every new transport connection requires a new Syntra handshake with
fresh credential presentation. The `request_id` space dies with the
connection; subscriptions, live cursors, and in-flight RPCs do not
survive. Resumption without a negotiated RFC capability is forbidden.

On loss, all in-flight RPCs report `UNAVAILABLE` with unknown outcome
(`is_unknown_outcome()==true`). Retry only when no terminal `result`
arrived AND the operation is idempotent or carries an application
operation key (`random_id` for sends), with a fresh `request_id`, bounded
retries, and jittered backoff (`ReconnectPolicy`: 500 ms base, ×2,
30 s cap, 10 attempts default). Terminal results (including
`UNIMPLEMENTED`, `PERMISSION_DENIED`, `CANCELLED`) are never auto-retried.

```text
Attempt 1: request_id=100, payload has operation_id=ABC → loss → UNAVAILABLE (unknown)
New handshake → Attempt 2: request_id=201, same operation_id=ABC → dedup by ABC (app-defined)
```

`request_id` is transport correlation only; it is never an idempotency
key across connections.

## Synchronization

1. Hydrate memory cursors from `CursorStore` (absent = zero, never
   start-live).
2. `SubscribeBatched{own_cursor, rooms[after], watch}` — confirms
   interests, not replay completion. No Tinkle ready/hello follows.
3. Validate each complete `RealtimeBatch` (stream/cursor/UUID agreement,
   grammar, position order) before any effects.
4. Atomically commit effects + `final_cursor` + durable save, or neither.
   Complete required refetches (`GetDialogs`/`GetHistory`) before
   checkpointing invalidations; never discard pending refresh on crash.
5. Deduplicate whole batches by `(stream, cursor)` in principal/history
   scope. `c==a` duplicate (ignore), `c<a` stale (ignore, never move
   backward), `c==a+1` next (apply+commit), `c>a+1` gap (stop, retain
   checkpoint, recover — never skip).
6. Floor/head: `after < floor` expires (reset path), `after == floor`
   valid, `after > head` invalid.
7. Reset stops one stream; head is diagnostic, not a snapshot fence. No
   snapshot/fence RPC exists yet; recovery reports unavailable until a
   proven consistent source snapshot + race-free watch bootstrap exist
   (do not sample head/fetch unrelated pages/resume at head or zero).
8. Presence is ephemeral, never checkpointed, cleared on disconnect.
9. Sends persist `{request, random_id≠0}` before transmitting; lost
   results retry unchanged with fresh correlation, bounded retries.

## What this SDK does not claim

Exactly-once delivery/processing, cross-connection `request_id`
deduplication, infinite retention, or snapshot fencing. Effectively-once
behavior comes only from documented application idempotency above
Syntra, with explicit keys, windows, and duplicate handling.
