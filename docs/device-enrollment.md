# Native device enrollment

The Rust `Auth` facade implements the published public `AuthService` contract.
The network binding is pinned Syntra TCP. The published schema
includes `connection.proto` and initializes every
new connection before metadata-dependent application work.

## Initialization

Construct `ClientInfo::new(device_model, platform, os_version, app_version,
app_build, options)` and pass it to `ClientConfig::new(host, info)`. All five
labels are required. Raw labels must be printable ASCII before surrounding
spaces are trimmed. Limits are 128/32/64/64/64 bytes respectively. Platform is
lowercased; build strings such as `00142` are preserved. `ClientOptions` is the
entire optional snapshot, including explicit empty/unknown values.

The Client owns an immutable snapshot. Its first public unary request on each
new transport is wrapped in `ConnectionService.InitConnect`; subsequent calls
share that connection's initialized snapshot. Concurrent first calls wait for
initialization, within their original deadline/cancellation budget. A failed or
ambiguous target is never automatically replayed. A later caller may initialize
again with the same complete snapshot. Streaming observations require a prior
public unary request. Retained observations are restored by a connection-owned,
bounded task after initialization, without delaying the confirmed target result.

## Durable enrollment flow

```rust,ignore
// secure_store: Arc<dyn SecureEnrollmentStore>, implemented by the host.
client.connect().await?;
let code = client.auth().send_code(SendCodeRequest {
    phone_number: phone.clone(), lang_code: "en".into(),
}).await?;

// Generates a random UUID and 32-byte capability; waits for durable storage.
let operation = client.prepare_enrollment(secure_store.clone()).await?;
let signed_in = client.auth().sign_in(
    SignInDetails::new(phone, sms_code, code.verification_id)?,
    &operation,
).await?;
// SignIn returns only after atomic secure candidate persistence succeeds.
// This does not authenticate the Bootstrap connection or construct Returning.
let status = client.auth().activate_enrollment(&signed_in.enrollment).await?;
// ACTIVE was verified and durably installed; a fresh Returning connection exists.
```

`SignInDetails`, `DurableOperation`, `PersistedEnrollment` and `SecretBlob` are
non-Clone, Debug-redacted secret owners. Generated secret-bearing protobufs stay
inside the facade. Owned buffers and decoded key/capability fields are erased
after use, including partial-decode error paths. Do not use raw protobuf calls
for enrollment or log raw protected records. Syntra and UniFFI own their own
wire/ABI buffers; foreign applications must erase their input/output copies.

The coordinator validates operation/session UUIDs, AK32, nonzero handle8,
positive enrollment/account epochs, ACCOUNT scope, future expiry and the exact
allowed state. SignIn and rotation delivery must be STAGED. Authorized recovery
may deliver STAGED or ACTIVE. The activation response must exactly match the
proposed session, handle, epoch, scope, user, account epoch and expiry, with
ACTIVE state. Rotation additionally requires the same securely stored active
session/user/account generation and a strictly greater enrollment epoch.

## Secure-store contract

Implement `SecureEnrollmentStore` in Rust, or `FfiSecureEnrollmentStore` in
Swift/Kotlin. There is no default memory/cache/database implementation.

| Transaction | Required effect before returning success |
|---|---|
| `Operation` | Create-only durable record of UUID, random capability and public trust |
| `Candidate` | Atomically replace that operation record with the complete delivered candidate; preserve the active slot |
| `Active` | Atomically install the complete ACTIVE record and replace that operation record |

### Conditional namespace contract

For restart-safe cleanup, implement `namespace_snapshot`, `commit_current`,
`begin_clear`, and `finish_clear` as atomic transactions over the same protected
namespace. UniFFI uses the same callbacks with `ProtectedEnrollmentNamespace`.

- The snapshot returns an immutable namespace **incarnation**, a **revision**,
  `cleanup_pending`, the complete opaque active record, and `has_pending` for
  recoverable operations/candidates besides that active record.
- Every operation, candidate or active write compares the expected revision and
  advances it. Tokens are nonempty, at most 256 bytes, and never reused, including
  after explicit namespace reset. Different callback wrappers for one namespace
  return the same incarnation. App/extension/process access shares this atomic
  revision authority.
- `commit_current` returns the next revision, or None with no mutation if stale.
  Conditional hosts never fall back to unconditional `commit`.
- `begin_clear(expected_revision)` atomically persists a durable cleanup marker
  only for that revision. It is idempotent. A marker rejects every old write and
  restoration, including after restart. False means superseded, with no change.
- `finish_clear(expected_revision)` erases secrets only under that same marker,
  retains the marker after success, and returns false without clearing when
  superseded. A failed finish leaves the marker in force. Explicit host recovery
  creates a new incarnation; the old coordinator cannot clear or write into it.

Conditional ACTIVE installation additionally requires `begin_restore` /
`end_restore` from `crates/libtinkle/src/enrollment.rs`. Atomically compare the
incarnation, revision, and complete ACTIVE record, reject cleanup markers, and
exclude writes, clearing, and namespace reset until the matching permit releases.
The exclusion applies across Clients/processes. Idempotent release/cancellation
must dominate late acquisition; a lost owner releases through process-owned
resources or leaves mutations blocked for host recovery. Two separate reads or
an unchecked expiring lease do not satisfy this contract.

Rust's default `namespace_snapshot` returns None. Foreign implementations without
conditional persistence must explicitly return nil/null and reject conditional
callbacks. These stores acknowledge `clear` but report
`session_status().restart_safe_store == false`; they cannot attest restart-safe
cleanup or enable the retirement observer. The SDK supplies no default durable
host store.

`load_operation` and `load_active` return the complete opaque record. Records
include the capability, key, tuple, status and provisioned pin/key ID/context
scope; the SDK checks that binding when restoring. Rust owns zeroizing
`SecretBlob`s. FFI passes opaque `ProtectedEnrollmentRecord` objects; hosts use
`copyForSecureStorage` only for their protected storage transaction and construct
`fromSecureStorage` only after a protected read/decryption. Release handles and
erase foreign byte copies after the transaction.

Use confidential platform storage and acknowledge only after durability, not
after scheduling an asynchronous write. Host callback errors are sanitized.
Callbacks have a ten-second budget. Each Client serializes enrollment state
transitions. A protected-store namespace belongs to one coordinator; shared
app/extension/process access must use serialized or conditional transactions so
stale writes cannot replace newer active generations. Application SQLite and
journal/cursor stores are separate and must never receive these bytes.

The API is deliberately staged. `StoredEnrollment`/`PersistedEnrollment` has no
public constructor and cannot produce a Returning key. `activate_enrollment`
requires that receipt. Activation uses a separate, single-attempt pinned
Bootstrap client with the original operation capability. The parent transport is
retired and joined before that connection opens; fresh Returning waits for the
Bootstrap transport to join. It never attempts Returning with STAGED material or
promotes Bootstrap's principal. `crates/libtinkle/src/client.rs` retains these
transport owners even if a host waiter is cancelled.

An active provisioning caller owns its registered child slot. Ordinary
connect/reconnect and retirement observation defer rather than cancel that
capability-bearing request. Once the caller is abandoned or the child explicitly
closes, its retained transport must join before another dial. Incomplete
carrier/policy shutdown retains the owner and blocks replacement, including
after a bounded wait returns; a later join attempt does not renew the SDK's
shutdown budget.

After a verified activation, the previous transport/key is retired even if the
local ACTIVE write fails. Automatic reconnect is parked. Only after ACTIVE
persistence succeeds does the SDK install fresh Returning and its application
principal. An activation or persistence failure must be recovered with the
retained operation, rather than by inventing a new operation or repeating SMS.

## Local sign-out and remote revocation

`crates/libtinkle/src/{logout,client,push}.rs` capture one remote-eligibility
budget of `min(3s, config.connection.max_rpc_timeout)` at high-level Logout entry, before the irreversible
fence and before lifecycle, connection, or initialization waits, retaining it with
the first cleanup cause. Concurrent callers join that owner; neither joining,
waiter cancellation, local cleanup retry, nor a later Logout renews the budget or
changes a retirement-only cause into remote Logout. The remote mutation is
attempted only on the captured Returning generation while positive time remains, passing
only floored remaining milliseconds through initialization and runtime handoff.

The budget limits eligibility for the remote attempt, **not durable local
clearing or total `logout()` wall time**. Always retain/join local cleanup even
when the remote budget expires; host persistence and acknowledged invalidation
may outlast three seconds. Expiry/loss can leave the remote outcome unknown and
does not prove rollback. Raw request APIs keep their selected timeout rather than
implicitly using this high-level policy; see
[request-budget integration](architecture.md#request-budget-integration-contract).

`crates/libtinkle/src/logout.rs` attaches the retained budget to the first cleanup
cause; `client.rs` consumes it without restarting the call budget after lifecycle
waits. See [qualification requirements](../PROTO_PINNING.md#sdk-pins-and-qualification)
for the separate verification lanes and evidence requirements.

`Client::logout()` first irreversibly fences this Client's authorization
generation, cancels provisioning requests, and erases registered SDK-owned AK
and capability cells, including those retained by operation/candidate receipts.
It stops the supervisor and joins live liveness work. Its only post-fence RPC is
a bounded, best-effort Logout on the previously authenticated channel; it never
dials or falls back to Bootstrap for that attempt. The channel is then retired.

Cleanup runs in a retained task. Concurrent callers join the same attempt;
dropping a Rust/foreign waiter does not cancel it. Local failures can be retried
without reconnecting or repeating Logout. Retries retain the original cache
principal, cumulative remote confirmation, and already acknowledged local steps.
Calls waiting on InitConnect are cancelled by the sign-out fence and recheck
admission after taking the initialization lock.

Every store used by prepare/load/restore is retained for invalidation. Advanced
hosts supplying ACTIVE material directly must bind their namespace first with
`bind_enrollment_store` (`bindSecureEnrollmentStore` over FFI). Without a bound
store, raw Returning material cannot claim acknowledged durable logout.

The `SecureEnrollmentStore::clear` / `FfiSecureEnrollmentStore.clear`
callback must atomically invalidate ACTIVE and **all** pending operation,
candidate and resume-capability records in the namespace. Success requires
durability and must dominate earlier writes, using serialized transactions or
a durable tombstone/conditional-write barrier. It is idempotent. The coordinator
waits for pre-fence persistence and checks generation before/after each write;
late results cannot recreate credentials or install Returning after clearing.

Logout outcomes are deliberately separate:

- `Ok(true)`: required local clearing and transport joins completed, and remote
  revocation was confirmed.
- `Ok(false)`: required local clearing and transport joins completed; remote
  revocation is unconfirmed (offline, rejected, lost reply, or unknown outcome).
- `Err`: in-memory sign-out remains in force and cleanup is `Failed`; not every
  required local step completed. Errors are sanitized, and `logout()` on this
  Client retries unfinished cleanup without another remote mutation.

`crates/libtinkle/src/logout.rs:finish_logout` does not let incomplete
carrier/policy shutdown suppress protected-store invalidation. A store may have
acknowledged durable clearing while cleanup still reports `Failed` because a
retained transport has not joined. Retrying rechecks that owner and skips
acknowledged storage/cache work; it preserves the first sign-out reason and
remote outcome without renewing either the remote or SDK shutdown budget.

`session_status()` (also on `TinkleClient`) is a network-free lifecycle query:
network Available/Unavailable, authorization Unknown/Active/SignedOut, and cleanup
NotStarted/Pending/Complete/Failed. A generic PermissionDenied or Unauthenticated
RPC makes authorization unknown; it does not erase credentials. Contact
connectivity events remain a separate API. An unconditional clear acknowledgement is
not a conditional restart-safety attestation; require `restart_safe_store` and
cleanup Complete for that claim.

`is_signed_out` / `isSignedOut` remains true after a cleanup failure. This Client
cannot connect, resume authentication, replace trust, restore ACTIVE, or reuse
old enrollment receipts. Deliberate sign-in requires a new Client. If a host
clear callback fails without persisting a tombstone, no SDK can truthfully claim
durable deletion across restart: the application must resolve invalidation and
block automatic restoration before constructing a new authenticated coordinator.

## Session review and state

`crates/libtinkle/src/session.rs` provides
`Client::subscribe_session_state() -> Result<SessionStateSubscription, TinkleError>`.
It registers one application consumer atomically with the latest retained state.
A second active consumer returns a subscription error. `next().await` returns
the initial/latest snapshot, coalescing intermediate revisions for a slow reader.
Cancelling a pending `next()` consumes no snapshot and does not unregister the
consumer; use `stop()` when the consumer is finished.

All lifecycle and review producers use one bounded MPSC ingress and serialized
reducer. `session_status()` reads that same authoritative lifecycle projection.
The subscription does not keep the Client alive or wait for host UI callbacks.
`stop()` synchronously releases the slot and unblocks pending `next()` calls with
`None`; dropping it has the same effect. Client stop/shutdown or release also
ends waiting. A later subscription receives the retained current state.

```rust,no_run
use libtinkle::{Client, SessionState, TinkleError};

async fn observe_account(
    client: &Client,
    mut render: impl FnMut(SessionState),
) -> Result<(), TinkleError> {
    let subscription = client.subscribe_session_state().await?;
    while let Some(state) = subscription.next().await {
        render(state); // Render the snapshot; do not reconstruct state from events.
    }
    Ok(())
}
```

| `SessionState` field | Meaning |
| --- | --- |
| `revision` | Monotonic local projection revision, not a server journal checkpoint |
| `status` | Network, authorization, cleanup, protected-store capability, and last enrollment observation |
| `reason` | First irreversible sign-out cause: `SelfLogout` or `SessionRetired`; cleanup retries never replace it |
| `sessions: Option<Vec<Session>>` | Complete authoritative `GetSessions` snapshot; `None` means no current fetched list, while `Some(vec![])` is a fetched empty list |

The SDK fetches `GetSessions` on each authenticated Returning establishment,
including startup, foreground reconnection, and reconnect. Recognized
`SessionChanged` hints mark the list dirty, and confirmation marks it dirty before
and after dispatch. Reads run sequentially on the same ordinary Returning socket.
Failed reads remain dirty and retry with bounded backoff. A hint during a read
requires another fetch; stale-generation responses and confirmation replies
cannot replace the full retained list. No host polling loop is required.

`crates/libtinkle/src/services.rs` owns the account facade; native hosts use the
corresponding `TinkleClient` methods in `crates/tinkle-ffi/src/account.rs`:

| Host action | Rust facade | Result / rule |
| --- | --- | --- |
| Display devices | `client.account().get_sessions()` | Secret-free session records, including `current`, `unconfirmed`, and descriptive device fields |
| “Yes, this was me” | `confirm_session(session_id)` | Confirm one other ACTIVE session; returns its commit-point projection, not a replacement list |
| “No, this was not me” | `revoke_session(session_id)` | Revoke only the selected session, including its staged replacements |
| Explicitly remove all other devices | `revoke_other_sessions()` | Separate action; preserves the caller's session |

Offer a review prompt only for `unconfirmed && !current`. A caller is eligible
to confirm only when its own current session is confirmed (`!unconfirmed`);
the server must enforce this against ACTIVE authority at the effect boundary. An
unconfirmed session still has ordinary access but cannot confirm itself or
another device. The enrollment that created the account starts confirmed;
other new sessions start unconfirmed. Review state belongs to the session and
does not reset or create another row on rotation.

`ConfirmSession` returns `FailedPrecondition` for an unconfirmed caller or current
target, `NotFound` for unknown, foreign, revoked, or expired targets, and
`InvalidArgument` for malformed input. Caller/workload denial and dependency
errors retain their typed categories. Repeating confirmation of an already
confirmed eligible target is idempotent, but the SDK never automatically replays
confirmation or revocation after an unknown outcome. Refresh authoritatively;
neither a denial nor a lost response proves retirement or rollback.

Account state is separate from contact connectivity and message synchronization.
`crates/tinkle-schema/src/sync.rs` recognizes exact typed `Any` notifications:
`type.googleapis.com/tinkle.v1.SessionChanged` only demands a session read, and
`type.googleapis.com/tinkle.v1.ConnectivityChanged` only updates ephemeral
presence. A hint cannot grant access, populate a session row,
erase credentials, or advance a durable message checkpoint.

## Enrollment recovery

After a lost SignIn result or process restart:

```rust,ignore
let operation = client.load_enrollment_operation(secure_store.clone(), &operation_id).await?;
let candidate = client.auth().resume_enrollment(&operation).await?;
client.auth().activate_enrollment(&candidate).await?;
```

For a normally persisted active session, call
`restore_active_enrollment(secure_store).await`; only ACTIVE, unexpired,
correctly bound material can establish Returning. The server still controls
revocation/admission. Expired, revoked or replaced keys cannot be restored by
changing client-side labels or mode.

For rotation, prepare a fresh durable operation, call
`auth().rotate_enrollment(&operation)`, then explicitly activate the returned
receipt. Delivery uses the current Returning connection; activation uses
Bootstrap. The prior secure active slot is preserved through staging.

## Swift/Kotlin surface

`ClientConfigFfi` requires `ClientInfoFfi`, with all five labels and a complete
`ClientOptionsFfi`. `TinkleClient` exports `sendCode`, `prepareEnrollment`,
`loadEnrollmentOperation`, `signIn`, `resumeEnrollment`, `rotateEnrollment`,
`activateEnrollment` and `restoreActiveEnrollment`. `EnrollmentOperation` and
`StoredEnrollment` are opaque object handles. SignIn returns a user protobuf
projection, `isNewUser`, and the stored receipt; no key/capability is returned
in a diagnostic-printable record. The public FFI trust constructor is Bootstrap;
Returning is installed through validated secure-store restoration or activation.

This package supplies the asynchronous persistence contract, not an automatic
Keychain/Keystore implementation. Hosts must implement that contract before
enrollment can progress; there is no implicit persistence acknowledgement.

## Permanent retirement observation

`crates/libtinkle/src/retirement.rs` uses the published
`AuthService/GetEnrollmentDisposition` API from the
[current schema pin](../PROTO_PINNING.md#current-published-schema-pin).
The request contains one reference:
session UUID, handle, enrollment epoch, scope and original account-auth epoch.
The response must echo all fields and contain RETIRED. Correlation belongs to
the channel/RPC; there is no application query identifier. An authenticated
authority supplies negative evidence only from a committed exact terminal
binding or a strictly advanced authoritative source epoch. UNKNOWN is no grant.

Observation is foreground-only and seeks evidence separately from Returning:

- Conditional ACTIVE restore performs status-first observation before attempting
  Returning. Expired records are parsed structurally for observation without
  registering an AK or constructing Returning. UNKNOWN never makes them usable.
- Explicit foreground transitions (`resume`/fresh `start`) observe first. The
  offline fallback is armed before Returning, with its first tick after 60
  seconds. Periodic ticks skip a healthy authenticated Returning socket rather
  than disconnect it to query status.
  Concurrent foreground callers wait on the same entry barrier before unblocking
  Returning; the foreground flag alone never signals completion of that barrier.
  The bounded entry task survives cancellation of its waiter, including while
  reading the namespace before any RPC. Later callers join its retained result;
  only explicit lifecycle retirement cancels and drains the entry owner.
- Current foreground Returning loss, `Unauthenticated`, or terminal KeepAlive
  failure coalesces one immediate bounded observation per generation. An ordinary
  per-RPC `PermissionDenied` withdraws authorization knowledge without interrupting
  a healthy Returning connection or scheduling observation. Stale generations, local
  suspension, sign-out, unsupported storage, and parked endpoint proof cannot
  schedule it. The failure itself never authorizes erasure or another route.
  Repeated calls while already foreground do not reset the entry barrier or
  fallback timer. Suspend/stop cancel and join observation; none runs in the
  background.

Each observation retains one five-second absolute evidence budget through lock
waiting, old-transport teardown, storage snapshot, dial/proof,
InitConnect-wrapped status RPC, and evidence recheck. The old ordinary/provisioning
transport must join before status Bootstrap opens. That connection's only
application method is GetEnrollmentDisposition, on the selected TCP route and
exact trust, including a fallback selected by an initial dial failure.

The observer never changes the main mode or uses the general provisioning
client. There is no permanent second connection or extra public port. The shared
dial gate is retained through status shutdown/join; a successor Returning dial
cannot overlap it, even after the evidence budget expires. Incomplete shutdown
retains the status handle and its admission barrier for a later join attempt;
neither timeout nor method return releases that barrier. Observation also defers
while a provisioning caller actively owns its child slot.

Genuine endpoint rejection parks every use until explicit `replace_trust_source`.
An unverified Bootstrap first-flight EOF/timeout also parks conservatively because
it can conceal pin rejection.

Before local cleanup, the SDK compares the full reference, source installation
revision, protected namespace incarnation/revision, active record and pending
work again. New operations invalidate predecessor observations. A staged or
unknown activation, successor rotation, source replacement or another Client's
write makes old evidence unusable. Conditional clearing cannot cross namespace
reset. Cleanup is retained/joinable, fences before awaiting, and never sends
remote Logout or upgrades an earlier unknown Logout outcome.

UNKNOWN, unspecified/future enums, missing/mismatched references, unsupported
methods, errors, malformed replies, EOF and timeouts never erase credentials.
`session_status().enrollment_observation` exposes NotObserved, Unknown, Retired,
Superseded, Unavailable, Parked or Unsupported independently of authorization,
network availability and cleanup acknowledgement. Unconditional stores cannot enable
automatic observation or attest restart-safe cleanup.

Integration coverage uses encrypted SDK accept/connect fixtures, including held
responses during rotation and a separate-process restart reading a fsynced
test-only private-file namespace. That fixture asserts CAS/tombstone persistence;
its presence does not establish execution, platform Keychain/Keystore security,
or production authority acceptance.

## Published schema checks

Run from the original libtinkle workspace with the pinned Rust toolchain and
published Cargo dependency. Required CI uses ordinary `--locked` checks without
Buf or schema-candidate generation:

```sh
cargo test --locked --workspace --all-features --verbose
cargo clippy --locked --workspace --all-targets --all-features --no-deps -- -D warnings
bash scripts/generate-bindings.sh --check
```

Regenerate checked-in Swift/Kotlin bindings with
`bash scripts/generate-bindings.sh --write`, then rerun `--check`.

`scripts/test-schema-candidate.sh` remains optional for future-schema development,
not release qualification. See [public pinning](../PROTO_PINNING.md) for that
workflow. No generated protobuf source or vendored `.proto` enters production
directories.

To exercise the shared mobile fixture in the original workspace:

```sh
python3 scripts/with-native-fixture.py --port 0 cargo test --locked -p libtinkle \
  --test mobile_fixture -- --ignored
```

## Verification boundaries

SDK unit tests, encrypted TCP interoperability fixtures, and Swift/Kotlin consumer
checks do not establish production-server acceptance or platform protected-storage
security. Externally orchestrated acceptance requires separate qualification
against the exact source and published pins. Test presence is not execution
evidence; see [qualification requirements](../PROTO_PINNING.md#sdk-pins-and-qualification).
