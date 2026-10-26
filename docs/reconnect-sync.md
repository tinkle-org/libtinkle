# Reconnect and synchronization

## Reconnect

Every new transport connection requires a new Syntra handshake with
fresh credential presentation. The `request_id` space dies with the
connection; subscriptions, live state, and in-flight RPCs do not
survive.

On loss, all in-flight RPCs report `UNAVAILABLE` with unknown outcome
(`is_unknown_outcome()==true`). Retry only when no terminal `result`
arrived AND the operation is idempotent or carries an application
operation key (`random_id` for sends), with a fresh `request_id`, bounded
retries, and jittered backoff (`ReconnectPolicy`: 500 ms base, ×2,
30 s cap, 10 attempts default).

Supervised apps call `start(targets, watch)` once: the SDK establishes the
session and owns reconnection from there on (hint-driven plus a
5 s watchdog tick, 10 s re-establish cooldown). Manual apps use
`connect` / `subscribe` / `reconnect` directly. `suspend()` tears down
the transport and blocks supervisor reconnects until `resume()` (or a
fresh `start`); retained events still drain via `next_event`.

## Synchronization (reconciliation contract)

Conversations are addressed by `Target` (`target::Kind::UserId` for
direct messages, `target::Kind::ChatId` for groups; kind names `"user"`
and `"room"`/`"chat"` on text surfaces). The canonical per-target key is
`target:user:<id>` / `target:room:<id>` (`target_key_for`). There is no
durable update feed and no replay cursor: each conversation carries its
own mutation journal, and sync reconciles journals per target.

1. Authenticate and establish the principal (`user:<id>`). Set it via
   `sign_in` or `set_principal_user_id`; per-target accept positions are
   isolated by principal and never reused across accounts.
2. Subscribe to conversation targets (`subscribe(Vec<Target>)`; absent
   means untracked, never start-live). Connectivity observations
   (`ObserveConnectivity`) are orthogonal: they observe presence, never
   sync state.
3. Reconcile one target with `GetTargetChanges{target, after_accept_seq,
   limit}` pages over unary RPC (Syntra unary on the live connection by
   default; direct-unary gRPC via `grpc_endpoint` is opt-in). The journal
   keeps mutations at acceptance order; `after_accept_seq` is the
   exclusive lower bound (zero requests everything retained).
4. Validate each page (`validate_changes_page`): changes ascend strictly
   above `after_accept_seq`; `last_accept_seq` never moves backward (it
   equals the last change's `accept_seq` on a non-empty page and echoes
   `after_accept_seq` on an empty one); edited changes carry the current
   body while deleted/revoked changes carry identity and kind only; an
   empty page must never claim `more_available`.
5. `more_available` is authoritative when the wire carries it: `Some(true)`
   confirms a next page (request it from `last_accept_seq`); `Some(false)`
   confirms caught up to the retained head. Absence is never read as caught
   up, yet it is not paged through forever either: the client performs one
   bounded re-page from `last_accept_seq` and then stops, treating the
   journal as unreported rather than complete. A short page never implies
   completion on its own.
6. Deduplicate by `accept_seq`: the token is conversation-local, assigned
   once on acceptance, and stable across edits of the same message. A
   fetched token strictly newer than the applied one is newer; strictly
   below is duplicate-or-stale; equal tokens (normal for edits) and any
   absent token order by `edit_date` instead (`fresher_body`). Absent
   tokens never count as dedup votes. Retention windows may drop older journal positions
   entirely: a page that starts higher than the requested bound does not
   imply the skipped past was disclosed — full-history repair stays the
   responsibility of history fetches, not of the journal alone.
7. Hydrate bodies with `GetMessages{target, message_ids}` (unary, same
   transport choice). Batches larger than `MAX_MESSAGE_IDS_PER_REQUEST`
   (100) are rejected, never truncated: callers must chunk
   (`chunk_message_ids`) preserving order. Undisclosable ids (unknown,
   deleted, revoked, below visibility) are omitted, never errors; order is
   unspecified. A fetched body shares the message's stable `accept_seq`
   token, which cannot rank copies: when two bodies for one message id
   disagree, keep the fresher by later `edit_date`; an absent `edit_date`
   never beats a dated one. Learn which edits and removals happened
   through `GetTargetChanges`, not by diffing fetched bodies.
8. Journal failures are per-target, never terminal for the session: a
   failed target parks in `Failed` status and emits
   `ReconcileFailed{target_key, error}`; the supervisor keeps retrying
   it with backoff while `reconcile_now()` triggers an immediate retry.
   There is no replay distance to bound and no rebuild to enter — no
   `max_replay_distance`, no `RebuildRequired`, no snapshot fencing.
9. History (`load_history`) and dialogs (`load_dialogs`) live outside the
   journal: they never advance or reset accept positions. Pair snapshots
   with cache hydration (`HydrateCache`) for offline reads; `NetworkOnly`
   snapshots are returned without caching, while `HydrateCache` snapshots
   commit authoritative bodies into message rows via the same-cursor path
   (the journal cursor itself never moves on either policy).
10. Connectivity observations are ephemeral, never checkpointed, cleared
    on disconnect.
11. Sends persist `{request, random_id≠0}` before transmitting; lost
    results retry unchanged with fresh correlation.

## Demand-driven contract (hosts)

- Consume `next_event()`; refresh on `ConversationChanged`/
  `MessagesChanged` (cache reads or `get_messages` hydration), drop cache
  rows on `ConversationRemoved`, and call `reconcile_now()` after
  `ReconcileFailed`. There is nothing to acknowledge: positions advance
  inside the SDK as journals reconcile.
- Poll `target_cursor(target_key)` for authoritative UI progress
  (informational events may drop under pressure; positions do not),
  `target_status(target_key)` for per-target demand state (`Idle` quiet,
  `Reconciling` in flight/demanded, `Failed` under supervisor backoff
  retry — `reconcile_now()` triggers an immediate retry), and
  `tracked_targets()` for the subscribed set.
  `None` from the cursor means never-reconciled for a tracked target (or
  no authenticated principal / no store entry); `None` from the status
  means untracked, or no live manager while offline/suspended — so hosts
  must not drop tracked conversations on a status `None` during
  reconnect.
- Journal reads (`get_messages`, `get_target_changes`) and snapshots
  (`load_history`, `load_dialogs`) never move accept positions. Pair
  snapshots with `HydrateCache` for offline reads; journal hydration
  (`GetMessages`) follows `accept_seq`/`edit_date` freshness, never
  position movement.

## What this SDK does not claim

Exactly-once delivery, cross-connection `request_id` deduplication,
infinite journal retention, or snapshot fencing. Effectively-once behavior
comes only from application idempotency with explicit keys and duplicate
handling. A journal page that starts above the requested bound discloses
nothing about the skipped past; repair it with history fetches.
