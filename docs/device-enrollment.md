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

Rust's default snapshot returns None for legacy hosts. Foreign implementations
must explicitly return nil/null for that mode and reject the conditional
callbacks. Such hosts keep the legacy acknowledged-clear behavior but report
`session_status().restart_safe_store == false`: they cannot claim restart-safe
cleanup or enable the retirement observer. There is no default durable
host implementation.

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
Bootstrap client with the original operation capability. It never attempts
Returning with STAGED material and never promotes Bootstrap's principal.

After a verified activation, the previous transport/key is retired even if the
local ACTIVE write fails. Automatic reconnect is parked. Only after ACTIVE
persistence succeeds does the SDK install fresh Returning and its application
principal. An activation or persistence failure must be recovered with the
retained operation, rather than by inventing a new operation or repeating SMS.

## Local sign-out and remote revocation

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

The new `SecureEnrollmentStore::clear` / `FfiSecureEnrollmentStore.clear`
callback must atomically invalidate ACTIVE and **all** pending operation,
candidate and resume-capability records in the namespace. Success requires
durability and must dominate earlier writes, using serialized transactions or
a durable tombstone/conditional-write barrier. It is idempotent. The coordinator
waits for pre-fence persistence and checks generation before/after each write;
late results cannot recreate credentials or install Returning after clearing.

Logout outcomes are deliberately separate:

- `Ok(true)`: local durable clearing acknowledged and remote revocation confirmed.
- `Ok(false)`: local durable clearing acknowledged; remote revocation is
  unconfirmed (offline, rejected, lost reply or unknown outcome).
- `Err`: in-memory sign-out remains in force, but durable cleanup is not claimed.
  Clear errors are sanitized and can be retried with `logout()` on this Client.

`session_status()` (also on `TinkleClient`) is a network-free lifecycle query:
network Available/Unavailable, authorization Unknown/Active/SignedOut, and cleanup
NotStarted/Pending/Complete/Failed. A generic PermissionDenied or Unauthenticated
RPC makes authorization unknown; it does not erase credentials. Contact
connectivity events remain a separate API. A legacy clear acknowledgement is
not a conditional restart-safety attestation; require `restart_safe_store` and
cleanup Complete for that claim.

`client.account()` provides `get_sessions`, `revoke_session`, and
`revoke_other_sessions`. Native hosts call those methods directly on
`TinkleClient` and receive secret-free `AccountSession` records, including the
neutral session UUID and build identifier. Invalid UUIDs fail before I/O;
revocations are single-attempt operations with unknown-outcome semantics.

`is_signed_out` / `isSignedOut` remains true after a cleanup failure. This Client
cannot connect, resume authentication, replace trust, restore ACTIVE, or reuse
old enrollment receipts. Deliberate sign-in requires a new Client. If a host
clear callback fails without persisting a tombstone, no SDK can truthfully claim
durable deletion across restart: the application must resolve invalidation and
block automatic restoration before constructing a new authenticated coordinator.

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

The observer uses the published `AuthService/GetEnrollmentDisposition` API from
schema #57, available in BSR prost artifact
`0.5.0-20261107080548-dc41c2199c76.2`. The request contains one reference:
session UUID, handle, enrollment epoch, scope and original account-auth epoch.
The response must echo all fields and contain RETIRED. Correlation belongs to
the channel/RPC; there is no application query identifier. An authenticated
authority supplies negative evidence only from a committed exact terminal
binding or a strictly advanced authoritative source epoch. UNKNOWN is no grant.

Scheduling is independent of Returning outcomes:

- Conditional ACTIVE restore performs status-first observation before attempting
  Returning. Expired records are parsed structurally for observation without
  registering an AK or constructing Returning. UNKNOWN never makes them usable.
- Explicit foreground transitions (`resume`/fresh `start`) observe first. The
  schedule is armed before Returning, with its first periodic tick after 60
  seconds. It remains fixed across successes, failures, reconnects and rotation.
  Concurrent foreground callers wait on the same entry barrier before unblocking
  Returning; the foreground flag alone never signals completion of that barrier.
  The bounded entry task survives cancellation of its waiter, including while
  reading the namespace before any RPC. Later callers join its retained result;
  only explicit lifecycle retirement cancels and drains the entry owner.
- Returning authentication/lookup/RPC failure never starts a Bootstrap probe,
  advances a tick, resets backoff, or authorizes another route. Repeated calls
  while already foreground do not reset the observer. Suspend/stop cancel and
  join it; no observation runs in the background.

Each observation owns a short-lived encrypted Bootstrap connection and a
five-second absolute budget covering storage snapshot, route lock/dial, proof,
InitConnect-wrapped status RPC and evidence recheck. Its only application method
is GetEnrollmentDisposition. It shares the main selected TCP route and exact
trust, including a fallback selected by an initial dial failure. It never changes
the main mode or uses the general provisioning client. There is no permanent
second connection or extra public port. Genuine endpoint rejection parks every
use until explicit `replace_trust_source`. An unverified Bootstrap first-flight
EOF/timeout also parks conservatively because it can conceal pin rejection.

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
network availability and cleanup acknowledgement. Legacy hosts cannot enable
automatic observation or attest restart-safe cleanup.

Integration coverage uses encrypted SDK accept/connect fixtures, including held
responses during rotation and a separate-process restart reading a fsynced
test-only private-file namespace. That fixture proves CAS/tombstone persistence,
not platform Keychain/Keystore security or production authority acceptance.

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
python3 scripts/with-native-fixture.py cargo test --locked -p libtinkle \
  --test mobile_fixture -- --ignored
```

## External client-api acceptance entry point

Parent integration must start the actual Go client-api and provide trusted pin
configuration independently. This SDK never starts or builds Go fixtures.
Set `TINKLE_E2E_HOST`, `TINKLE_E2E_PORT`, `TINKLE_E2E_SERVER_PIN` (64 hex),
`TINKLE_E2E_SERVER_KEY_ID`, `TINKLE_E2E_SCOPE`, all five
`TINKLE_E2E_DEVICE_MODEL`, `TINKLE_E2E_PLATFORM`, `TINKLE_E2E_OS_VERSION`,
`TINKLE_E2E_APP_VERSION`, `TINKLE_E2E_APP_BUILD`, and the fixture phone/language
`TINKLE_E2E_PHONE` / `TINKLE_E2E_LANG_CODE`. Then run:

```sh
cargo test --locked -p libtinkle --test client_api_external -- --ignored
```

This opt-in lane checks pinned Bootstrap, InitConnect-wrapped SendCode, and a
nonempty verification identifier. It does not claim full real-service enrollment
acceptance. That later lane must exercise the actual authn/protected-store path.
