# Architecture

```text
Swift/Kotlin bindings        native configuration, platform secret/cursor stores
        ↓
libtinkle                   typed/raw application requests, one supervisor,
                            paged history, journals, cache projection, file/push sync
        ↓
syntra-runtime              RPC correlation, deadlines, cancellation, notifications
        ↓
syntra-net (tcp only)        single-attempt encrypted native TCP, owned pumps
        ↓
syntra-core                 framing, secure channel, pinned server + Returning proof
```

`tinkle-schema` owns application payloads and validation. It is independent of
transport selection. Its artifact/dependency update is coordinated separately.
`tinkle-storage` owns application SQLite rows, journals, transfer state and
background jobs; authorization keys never enter that database.

All three Syntra crates use exact crates.io rc.5 dependencies without local
patches. Installed versions, crate-specific release tags, upstream budget APIs,
and qualification requirements are in [dependency pinning](../PROTO_PINNING.md#sdk-pins-and-qualification).
The application/runtime boundary is
opaque protobuf payload bytes. Device enrollment maps the public schema's exact
messages into zeroizing native owners and secret-free status projections.

## Establishment

The caller provides bare host/ports and a typed `TrustSource`. Both ports share
the exact source. One 15-second budget covers TCP dial selection, channel proof
and Core readiness. The primary port defaults to 443; the optional dial-failure
fallback defaults to 80.
After socket selection, no establishment error selects another port.
The selected port remains fixed through reconnects because the TCP-only SDK
does not publicly expose its private post-ready failure classification. A
successful explicit same-deployment trust replacement authorizes fresh route selection.

`Bootstrap` and `Returning` are explicit. Returning carries a nonzero AK32,
handle8 and epoch. The key is copied only into a fresh SDK `Secret32` for each
attempt. The host owns protected enrollment storage; the typed enrollment
coordinator installs verified material and its principal. Raw
`libtinkle::Client::replace_trust_source` requires identical server trust and clears
the principal and retained application interests; it is not verified enrollment
installation or a deployment switch. See [trust replacement](security.md#secret-ownership).
No rejected Returning proof grants Bootstrap authority or persists a key.

`crates/libtinkle/src/client.rs` serializes ordinary, provisioning, and status
connections through one shared dial/retirement owner. A replacement waits for
the old transport's shutdown and join; it does not overlap a second TCP socket.
Cancellation retains join ownership for the next lifecycle operation. Status
Bootstrap performs only the negative-only query, not account work.

`crates/libtinkle/src/liveness.rs:transport_joined` accepts only complete SDK
transport shutdown. An incomplete carrier/policy shutdown or an error retains
the ordinary, establishing, provisioning, or status handle and blocks another
dial; a timeout or lifecycle method return does not attest join completion.
Later attempts recheck that owner without renewing its shutdown budget.

An active provisioning caller retains its registered slot. Ordinary admission,
reconnect, and retirement observation defer rather than abort its request.
An abandoned or explicitly closing child is joined before replacement; a late
child close cannot release a newer child's slot. These boundaries are owned by
`crates/libtinkle/src/client.rs` and `crates/libtinkle/src/retirement.rs`.

The existing application supervisor schedules reconnect attempts. The SDK's
reconnecting client is not enabled. Manual `reconnect` makes one attempt and
restores retained application interests/watch sets. Requests are never replayed
by transport reconnection.

Each ACTIVE Returning generation owns one bounded five-second application
KeepAlive task. It calls the public raw-request path, so initialization and
authorization remain in that path. A generation fence prevents a retired task
from addressing a replacement socket. Failure classification precedes supervised
recovery; terminal authorization/protocol failures remain parked. Liveness
sequence state is connection-local and is never stored in application journals.
Lifecycle retirement cancels and joins the task; cancelled teardown retains its
join owner for the next lifecycle operation. Bootstrap has no KeepAlive task.

`ClientInfo` is an immutable complete snapshot, validated before construction.
Each live connection owns a separate first-call initialization gate. Its first
public unary call is wrapped in `InitConnect` under the same caller budget.
Enrollment operation/candidate/active writes use an acknowledged external secure
store; activation is a separate pinned Bootstrap request. Only a verified ACTIVE
response plus durable active installation can produce the next Returning source.

Local logout synchronously fences admission and spawns retained, joinable cleanup.
Its single remote mutation uses only the captured Returning generation. Cache and
secure-store retries preserve completed steps and the original principal.
Conditional hosts compare a durable namespace incarnation/revision, persist a
cleanup marker, and reject stale writes or clears after replacement. Unconditional
stores cannot attest restart-safe cleanup. The account lifecycle query keeps
network availability, authorization knowledge and cleanup progress distinct.

`crates/libtinkle/src/logout.rs` invalidates protected storage independently of
whether retained transport work can finish joining. Cleanup remains `Failed`
when a join is incomplete, even if storage and cache clearing already succeeded.
Retry joins unfinished work and preserves acknowledged steps, the first sign-out
cause, and remote confirmation without replaying Logout. See
[the cleanup contract](device-enrollment.md#local-sign-out-and-remote-revocation).

`crates/libtinkle/src/retirement.rs` owns status-first restore/foreground entry,
coalesced current-generation loss/denial checks, and a 60-second offline fallback.
A healthy Returning socket skips periodic observation. Each status-only Bootstrap
connection shares the selected route/trust and a five-second evidence budget;
shutdown/join remains mandatory after that budget expires. Source-install and
protected-store revisions fence stale evidence. Stop/suspend cancel and drain
observation; exact current retirement invokes retained local-only cleanup. See
[the host contract](device-enrollment.md#permanent-retirement-observation).

## Account state and review

`crates/libtinkle/src/session.rs` owns the single retained `SessionState`
projection. Typed producers enter a bounded MPSC queue consumed by one serialized
reducer, which assigns local revisions without awaiting UI, network, or storage.
`Client::session_status()` reads the same lifecycle view delivered by
`Client::subscribe_session_state()`, not a separate event-derived status.

One `SessionStateSubscription` consumer receives the initial/latest snapshot.
Slow consumers coalesce revisions; first sign-out cause and final cleanup state
remain retained. `stop()` releases the slot and unblocks `next()`; a later
subscriber starts from current state. Network loss and generic authorization
denial do not imply `SessionRetired`.

The generation-fenced review worker fetches full `GetSessions` snapshots after
Returning establishment, on session-change demand, and after confirmation. Failed
reads stay dirty and retry with bounded backoff. Demand arriving during a read
requires another fetch; an old generation or confirmation reply cannot overwrite
a newer list. `sessions: None` means no current fetched list, not an empty list.
See [session review and state](device-enrollment.md#session-review-and-state) for
host actions and confirmation eligibility.

## Application data

### Request-budget integration contract

`crates/libtinkle/src/client.rs` selects the initial requested timeout (or runtime
default for zero), clamps it to the configured maximum, and retains that one
budget through live-connection acquisition and initialization-lock waiting.
Immediately before either the direct call or first InitConnect-wrapped call, it
passes only positive, floored remaining milliseconds to the runtime. Exhaustion
or a sub-millisecond remainder fails rather than passing zero and selecting a new default.
No lock acquisition, wrapper, or runtime handoff grants a fresh budget.

The runtime call API accepts relative milliseconds, not this client's absolute
cutoff. The original outer budget remains a conservative local bound: a result
racing that cutoff can remain unconfirmed. Reduced handoff time does not establish
an exact distributed deadline or prevent an already-admitted remote effect.

Raw `call_raw` / `call_raw_cancellable` calls retain the caller-selected timeout
policy, including raw Logout; they do not acquire the high-level three-second
Logout policy by method name. The high-level remote-eligibility contract belongs
to [device enrollment](device-enrollment.md#local-sign-out-and-remote-revocation),
with application integration in `crates/libtinkle/src/{logout,client,push}.rs`.
SDK timing and emission semantics remain in the published references in
`PROTO_PINNING.md`, not reproduced here.

The implementation boundary is `client.rs:call_budget`,
`call_initialized_with_budget`, and `remaining_timeout_ms`, with retained Logout
ownership in `logout.rs`. Forwarding the original `timeout_ms` after locks or
starting a new 3000 ms call after lifecycle waits would violate this contract.
Changed pins or budgets require fresh scoped qualification through the
[actual-application lane](../PROTO_PINNING.md#sdk-pins-and-qualification).
A healthy baseline or an older paired result does not qualify this implementation
or establish a global suspension or `logout()` wall-clock guarantee.

### Data flow

Typed request → application protobuf → Syntra RPC → encrypted TCP → response
correlation → application decode. `crates/tinkle-schema/src/sync.rs` dispatches
recognized runtime notifications by exact `google.protobuf.Any` type URL:
`SessionChanged` demands an authoritative session refresh, while
`ConnectivityChanged` is ephemeral contact presence. Neither changes authorization
or advances a message checkpoint. Malformed recognized payloads are discarded;
other payloads remain conversation reconciliation hints. Per-target `GetTargetChanges`
pages hydrate with `GetMessages`, then cache rows and accept positions commit
atomically. Paged history repair retains continuation, window budgets and scope
fences. History and dialog snapshots never forge journal progress.

## Platforms and fixtures

UniFFI wraps the native client over one process-wide Tokio runtime. Generated
Swift/Kotlin configuration uses one-use trust objects for secret inputs.
`libtinkle-web` exports portable schema checks and explicit unsupported network
status, with no native client dependency.

Rust tests and the host-only mobile fixture use published `syntra-net::tcp`
accept/connect APIs. No fixture implements channel cryptography. The mobile
fixture's separate loopback control socket is a host test harness, never an
application transport or a production SDK feature.
