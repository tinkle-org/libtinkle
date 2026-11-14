# Reconnect and synchronization

## Reconnect

Every new transport connection requires a new Syntra handshake with
fresh channel proof under the explicit trust source. The `request_id` space dies with the
connection; subscriptions, live state, and in-flight RPCs do not
survive.

On loss, all in-flight RPCs report `UNAVAILABLE` with unknown outcome
(`is_unknown_outcome()==true`). Retry only when no terminal `result`
arrived AND the operation is idempotent or carries an application
operation key (`idempotency_key` for sends), with a fresh `request_id`, bounded
retries, and jittered backoff (`ReconnectPolicy`: 500 ms base, ×2,
30 s cap, 10 attempts default).

Supervised apps call `start(targets, watch)` once: the SDK establishes the
session and owns reconnection from there on (hint-driven plus a
5 s watchdog tick and bounded jittered backoff). Manual apps use
`connect` / `subscribe` / `reconnect` directly; manual `reconnect` makes one attempt.
Trust/configuration rejection parks automatic reconnect until explicit trust replacement.
The selected port remains fixed across reconnects; opaque post-ready loss cannot
authorize an alternate port. Trust replacement resets that route choice.
`suspend()` tears down
the transport and blocks supervisor reconnects until `resume()` (or a
fresh `start`); retained events still drain via `next_event`.

`crates/libtinkle/src/client.rs` joins the prior transport before any ordinary,
provisioning, or status connection replaces it. Current foreground Returning
loss/denial can coalesce a negative-only status check; a failure does not itself
prove retirement. The offline status timer skips a healthy socket. See
[retirement observation](device-enrollment.md#permanent-retirement-observation).

An incomplete carrier/policy shutdown retains the transport owner and blocks
another dial until a later join completes. Ordinary admission, reconnect, and
observation defer to an active provisioning caller; abandoned children are joined
before replacement. See [connection ownership](architecture.md#establishment).

## Account state and session review

`Client::subscribe_session_state()` is independent of conversation sync and
contact presence. `crates/libtinkle/src/session.rs` retains one latest account
projection for one app consumer, with local revision, lifecycle, first sign-out
reason, and `sessions: Option<Vec<Session>>`. A missing list is not a fetched
empty list. Stop the subscription to unblock `next()` and release its slot.

The SDK automatically refetches `GetSessions` on fresh Returning startup,
foreground reconnection, reconnect, and recognized `SessionChanged` hints.
Failed reads remain dirty and retry; demand during a read requires another
fetch. Confirmation results never patch the complete list. Hosts render the
retained projection and use targeted confirmation/revocation under
[the session-review contract](device-enrollment.md#session-review-and-state).

`crates/tinkle-schema/src/sync.rs` dispatches runtime notifications by exact typed
`Any` URL: `SessionChanged` demands review refresh, and `ConnectivityChanged`
updates ephemeral contact presence. Neither is a durable journal event or
authorization proof; malformed recognized payloads are discarded.

## Synchronization (reconciliation contract)

Conversations are addressed by `Target` (`target::Kind::UserId` for
direct messages, `target::Kind::ChatId` for groups; kind names `"user"`
and `"room"`/`"chat"` on text surfaces). The canonical per-target key is
`target:user:<id>` / `target:room:<id>` (`target_key_for`). There is no
durable update feed and no replay cursor: each conversation carries its
own mutation journal, and sync reconciles journals per target.

1. Authenticate and establish the principal (`user:<id>`) through acknowledged
   activation or ACTIVE secure-store restoration and fresh Returning. SignIn
   delivery alone does not establish it. Per-target accept positions are
   isolated by principal and never reused across accounts.
2. Subscribe to conversation targets (`subscribe(Vec<Target>)`; absent
   means untracked, never start-live). Connectivity observations
   (`ObserveConnectivity`) are orthogonal: they observe presence, never
   sync state.
3. Reconcile one target with `ListMessageChanges{target, after_accept_seq,
   limit}` pages over Syntra unary RPC on the encrypted native TCP connection. The journal
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
6. Order distinct messages by conversation-local `accept_seq`, assigned once on
    acceptance and stable across edits. Copies of one body compare `body_revision`
    when both report it, otherwise only `edit_date`: a later date wins and absence
    never beats a dated copy. Acceptance positions never rank body freshness.
    Winning bodies replace their file-reference set atomically; stale bodies cannot
    add references. Dialog touches select acceptance order, not numeric message IDs.
    Retention windows may drop older journal positions
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
   through `ListMessageChanges`, not by diffing fetched bodies.
   An explicit `body_invalidated=true` nonterminal marker is different from an
   omitted fetch result: it authorizes identity-only eviction at its positive
   `message_revision`, with no body or new presentation values. Reconciliation
   does not fetch that withheld body. SQLite schema v9 atomically advances the
   cursor, removes old bodies/file references, and retains a nonterminal revision
   floor. Older or unversioned cache writes cannot restore the evicted body;
   a later authorized same-or-newer revision may hydrate. This never inserts a
   permanent viewer-deletion tombstone. Explicit Deleted/Revoked markers remain
   terminal and cannot be undone by later snapshots.
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
11. Sends persist `{request, idempotency_key≠0}` before transmitting; lost
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
- Journal reads (`get_messages`, `list_message_changes`) and snapshots
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
