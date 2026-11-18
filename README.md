# libtinkle

Native Rust client runtime for Tinkle, with a UniFFI C ABI bridge. Application
RPCs, history, cache hydration, journal reconciliation and file transfers use
encrypted Syntra TCP. The SDK uses exact rc.5 pins for
`syntra-core`, `syntra-runtime`, and TCP-only `syntra-net`; see
[dependency pins, upstream release links, and qualification requirements](PROTO_PINNING.md#sdk-pins-and-qualification).
`Cargo.toml` / `Cargo.lock` own the installed pins.

## Native configuration

```rust,no_run
use libtinkle::{Client, ClientConfig, ClientInfo, InMemoryCursorStore, ServerTrust, TrustSource};

// Provision these public trust values through a trusted application channel.
let trust = ServerTrust {
    server_public_key: provisioned_pin,
    server_key_id: provisioned_key_id,
    scope: provisioned_scope,
};
let client = Client::new(
    ClientConfig::new("gateway.example.com", ClientInfo::new(
        device_model, platform, os_version, app_version, app_build, Default::default())?),
    TrustSource::Bootstrap(trust),
    InMemoryCursorStore::new(),
);
client.connect().await?;
```

`ClientConfig` fields:

| Field | Default / meaning |
|---|---|
| `host: String` | Bare hostname or IP, required; no URL |
| `client_info: ClientInfo` | All five device/app labels plus the entire immutable optional snapshot |
| `primary_port: u16` | `443` |
| `fallback_port: Option<u16>` | `Some(80)` |
| `connect_timeout: Duration` | 10 seconds per dial, within one 15-second establishment budget |
| `connection: syntra_runtime::ConnectionConfig` | Published runtime defaults |
| `reconnect: ReconnectPolicy` | Supervisor backoff: 500 ms base, ×2, 30-second cap, 10 attempts |

Both ports use the same host, pin, scope, key ID, and mode. Only an initial TCP
dial failure permits trying the fallback port. Failure after TCP selection but
before authenticated readiness parks automatic reconnect; post-ready recovery
uses the supervisor's typed failure policy without changing the selected port.
There is no HTTP/TLS wrapper or alternate network transport. Custom ports use the
same encryption and trust checks as the defaults.

Socket selection is retained across reconnects and suspend/resume. The TCP-only
SDK does not expose its private post-ready failure category, so opaque channel
loss cannot authorize trying another port. Explicit `replace_trust_source`
resets port selection; callers can then establish afresh under that source.

## Trust and enrollment

- `TrustSource::Bootstrap(ServerTrust)` explicitly selects Bootstrap with an
  empty Core credential and no authorization key.
- `TrustSource::Returning { trust, enrollment }` requires validated ACTIVE
  material. The normal flow obtains it through activation or protected-store
  restoration. `Enrollment::from_active_status` rejects STAGED status.
- `Enrollment` is not Clone, redacts Debug and zeroizes on drop. Syntra receives
  a fresh zeroizing key owner for each establishment. Neither the application
  cache nor cursor storage persists keys. Hosts load/store the returned tuple
  in their protected platform secret store, bound to the provisioned trust.
- `replace_trust_source(next).await` retires the connection and installs a raw
  source only with identical `ServerTrust` (pin, key ID, and scope). It clears
  the application principal and retained targets, watches, and pending catch-up
  state; raw credentials do not establish a verified user identity. A different
  deployment requires a new client with a separate cache/cursor namespace.
  Normal enrollment/rotation uses the typed coordinator described below.
  Rejected Returning proof never changes to Bootstrap automatically.

The typed `Auth` facade supports SendCode, SignIn, ResumeEnrollment,
RotateEnrollment and ActivateEnrollment. Prepare a durable operation before
SignIn/rotation; persist the delivered candidate through the acknowledged secure
store; activate explicitly over pinned Bootstrap; then establish fresh Returning.
The first public unary call on every connection is InitConnect-wrapped. See
[device enrollment](docs/device-enrollment.md) for the exact staged API, storage
contract, recovery path and runnable checks.

The published schema includes enrollment disposition, session review, and typed
session-change hints. Release qualification uses the published Cargo pin and
ordinary `--locked` checks, without Buf or schema candidates in required CI.
See [public pins](PROTO_PINNING.md#current-published-schema-pin).

`client.account()` exposes `get_sessions()`, `confirm_session(session_id)`,
`revoke_session(session_id)`, and `revoke_other_sessions()`. UniFFI exports these
directly on `TinkleClient`. For a review prompt, “Yes” confirms that session;
“No” revokes only that session, not every other device. Eligibility and typed
errors are defined in [session review and state](docs/device-enrollment.md#session-review-and-state).

`logout()` fences authorization before awaiting anything, then joins retained
cleanup. Dropping a waiter leaves cleanup running. Retrying a local failure
preserves the original cache principal and cumulative remote confirmation, with
no repeated remote mutation. `session_status()` distinguishes transport
availability, unknown authorization, signed-out state and cleanup progress.
Restart-safe cleanup additionally requires a conditional secure-store host and
`cleanup == Complete`; unconditional stores report `restart_safe_store == false`.
An incomplete transport join leaves cleanup `Failed` even when protected storage
was already invalidated. Retry `logout()` to join unfinished work without
repeating the remote mutation or acknowledged clearing.

Conditional ACTIVE restoration observes permanent retirement before attempting
Returning. Foreground entry and coalesced loss/denial of the current Returning
generation also request evidence. The offline fallback timer never disconnects
a healthy Returning socket. Status-only Bootstrap replaces the old connection
only after its transport work has joined, on the same selected route and trust.
Exact current RETIRED evidence starts local-only cleanup; unknown/error outcomes
retain credentials. See [retirement observation](docs/device-enrollment.md#permanent-retirement-observation).

`Client::subscribe_session_state()` returns the sole `SessionStateSubscription`.
Its `next()` yields the initial/latest retained `SessionState`, including lifecycle,
first sign-out reason, and an optional complete session list. `stop()` unblocks
waiters and releases the consumer slot. The SDK refreshes the list automatically;
hosts do not need a polling loop or a second connection. This account stream is
separate from `next_event()` and contact connectivity.

## Application lifecycle and synchronization

Use `start(targets)` for supervised operation, or manual `connect`, `subscribe`
and `reconnect`. `reconnect` performs one attempt; the existing supervisor alone
owns automatic retry scheduling. Trust/configuration and unverified establishment failures park
automatic reconnect until an explicit trust replacement. `suspend` closes the
connection and suppresses dialing; `resume` permits the supervisor to reconnect.
`stop`/`shutdown` fence live state while keeping durable application state.
Incomplete carrier/policy shutdown retains its owner and blocks another dial
until join completion. Ordinary admission and observation do not abort an active
provisioning caller; abandoned children are joined before replacement. See
[connection ownership](docs/architecture.md#establishment).

Typed/raw request interfaces, `load_history`, `load_dialogs`, `get_messages`,
`list_message_changes`, file transfers and push-driven synchronization share the
native channel. The application coordinator retains paged history continuation,
per-target journals, cache/cursor atomic commits, scope fencing, deduplication
and bounded catch-up. Consume `next_event()` and inspect `target_cursor`,
`target_status` and `tracked_targets`. Unsafe mutations are never auto-replayed.
See [reconnect/sync](docs/reconnect-sync.md) and [mobile lifecycle](docs/mobile-lifecycle.md).

## Native C ABI

`crates/tinkle-ffi` exposes the shared core through UniFFI `0.31.2`.
This repository provides Rust source. Consumer repositories own native builds,
language binding generation, packaging, and application integration.

Consumers pin the source revision and matching UniFFI generator, build an
unstripped library for metadata extraction, and generate their bindings from
that exact build. Their generated runtime handles UniFFI record serialization,
object ownership, async operations, and callbacks.

`ClientConfigFfi { client_info, host, primary_port, fallback_port }` uses bare TCP addresses.
`ServerTrustFfi { server_public_key, server_key_id, scope }` carries public trust.
`TrustSourceFfi.bootstrap(trust)` produces the one-use provisioning source.
`FfiSecureEnrollmentStore` supplies acknowledged atomic platform persistence;
opaque `EnrollmentOperation` and `StoredEnrollment` objects guard the staged
flow. Activation or `restore_active_enrollment` installs Returning only from
validated ACTIVE records. Hosts must erase their foreign secret byte copies.

Run `cargo test --locked -p tinkle-ffi` for the Rust adapter tests. Generated
bindings and ABI execution are verified by consumers against their own builds.
See [native integration](docs/platform-ios-android-wasm.md).

## Managed media preparation

Normal `Sending::prepare_attachment` uses the host installed with
`configure_media_preparer` (UniFFI `configure_native_media_preparer`) for native
normalization. The separate decoder-only configuration does not replace this
encoder. Rust independently checks the closed normalized output; no mobile video
decoder is implemented here. Hosts may return `NativeMediaOutput.thumbnail_frame`
(`FfiNativeMediaOutput.thumbnail_frame`): display-oriented sRGB RGBA8 from the
normalized video, tightly packed, at most 320 pixels per axis, fitted without
upscaling. Unsupported color/HDR frames must be omitted, not misrepresented.

Shared Rust derives a display-only JPEG preview (longest edge at most 40 pixels,
at most 4096 bytes, aspect fitted without upscaling) and regular PNG thumbnail
from normalized images, or from that optional host video frame. JPEG previews
composite alpha on white; main and PNG alpha/animation normalization are unchanged.
`NativePreparedMedia.tiny_preview` and the FFI local-result accessors expose the
exact display bytes. The immutable attachment manifest retains the preview and
a separate PNG path/digest before upload; restart reuses both artifacts without
reading the import or running a codec. Older manifests without these fields
resume without generating them. Principal cleanup removes both owned paths.

Managed preparation uploads main first with the stable `:main` key, checkpoints
its confirmed identity, then uploads optional PNG with the independent
`:thumbnail` key. Errors, including unknown thumbnail outcomes, propagate rather
than silently returning a changed main-only result. Explicit
`Sending::upload_attachment_thumbnail` (UniFFI
`sending_upload_attachment_thumbnail`) retries only the retained PNG after
confirmed main completion. It never changes a submitted send's exact payload.
The legacy result exposes main/thumbnail FileInfo separately and retains its
existing main-only MediaSource declaration. Local upload completion is not backend
validation or publication readiness.

### Asynchronous application actions candidate

The [isolated candidate](PROTO_PINNING.md#optional-future-schema-development)
enables the new fields on ordinary `prepare_attachment`,
`prepare_native_attachment` and `adopt_native_attachment` calls. A retained
preparation key fixes the exact recipe before upload.
Its main BeginUpload declares IMAGE or VIDEO processing explicitly; documents,
audio and voice omit processing intent. The separately supplied regular PNG also
declares IMAGE; published-pin thumbnail declarations remain absent. Server-returned
representations are never automatically re-uploaded, so this introduces no client
thumbnail-generation/upload recursion. Upload
intents compare the exact serialized declaration, including optional presence.
The returned MediaSource includes exact InlinePreview and completed regular
THUMBNAIL inputs, when present. Regular inputs are <=256 KiB, <=320 per axis and
lower-pixel than the main; a one-pixel main has none. Incompatible retained keys
cannot be converted or regenerated by selecting another build capability.
The native result's `inline_preview` helper (also exported as InlinePreview
protobuf bytes on the FFI result) supplies actual preview dimensions for strict
profile/catalog source builders; those sources still require their original
purpose-specific main metadata, normalization bounds and validation declarations.
The candidate manifest stores JPEG bytes once, reconstructs the exact canonical
prepared source declaration and verifies its digest, staying within the existing
16-KiB intent budget. This happens before send enqueue, not by reserializing a
submitted request. Published builds retain support for their own v1 manifests and
reject candidate v4/v5. Candidate preparation requires v5 with the reset wire
identity, media contract, and settings suffix; v4 and older manifests are rejected
before retained paths or protobufs are used, not converted. Exact identifiers and
build-mode requirements are in the [pinning guide](PROTO_PINNING.md#optional-future-schema-development).

Ordinary `Sending::enqueue` retains sends and albums with their existing numeric
idempotency keys. `enqueue_chat_photo` retains the positive numeric operation ID;
`enqueue_channel_photo`, `enqueue_profile_photo`, `enqueue_create_asset` and
`enqueue_update_asset` retain the caller's canonical lowercase nonzero request
UUID. All persist the exact original CAS, source and keys before transmission.
`enqueue_action_raw` retains original photo/asset request bytes after recursive
unknown-field rejection, including legal noncanonical field ordering/presence.
Reusing a local job key with different bytes fails before dispatch. Scheduling,
text and non-file-backed sends retain their existing semantics, not wire-number
compatibility across the global candidate renumbering.

ActionPrepared, ActionDispatching, ActionUnconfirmed, ActionPending,
ActionProcessing, ActionSucceeded, ActionFailed and ActionCanceled are distinct
from the original send/scheduling states. Pending and processing
acknowledgements hydrate no message, draft or file membership. The backend owns
automatic completion. After submission, `resume` queries original domain keys
instead of replaying the mutation. Owner-specific `get_send_status`/`cancel_send`,
`get_chat_photo_update`/`cancel_chat_photo_update`,
`get_channel_photo_update`/`cancel_channel_photo_update`,
`get_profile_photo_update`/`cancel_profile_photo_update` and
`get_asset_change`/`cancel_asset_change` take the durable job ID. Typed service
facades also expose the schema's direct query/cancel requests. Claimed work may
remain PROCESSING after cancellation; a timeout proves no durable outcome. A
successful query retains the validated original receipt before any body read.
SUCCEEDED requires every ordered send receipt with exact original keys and
explicit `reference_withheld` decisions, including when the
original typed result is withheld or expired. Per-call identity disclosure may
change without advancing the terminal operation revision; withheld slots never
reuse an earlier disclosed reference. The domain-specific `retained_*` methods
and matching `sending_retained_*` FFI adapters expose retained status without
guessing accepted IDs from file IDs. Offline inspection is not fresh permission
evidence. Querying any album member returns the first original key as canonical
and the complete original ordered receipt list, never a reordered or partial group.

For sends/albums, current bodies are fetched through GetMessages using only
disclosed accepted message targets/IDs, even if the original main/caption changed.
The original request and accepted receipt remain unchanged by that current-body
read. Original typed result bodies are never replayed into cache by action
recovery. Missing current items do not collapse album slots, synthesize a partial
original result, establish deletion, or advance a journal. Current bodies/file
refs commit under the pre-fetch scope/tombstone/revision fences. SUCCEEDED with
no disclosed original result stays successful, including hidden current bodies,
and cannot authorize another send. On supervised Returning connections, bounded
periodic recovery uses `GetSendStatus` with the original target and key (the first
key for an album). It covers unresolved sends and successful sends with unfinished
hydration, including retained jobs on startup or reconnect. It never resubmits.
`SendStatusChanged` is a read hint, not a receipt. Manual clients call the explicit
recovery methods themselves.

Representation journal markers use existing GetMessages reconciliation.
Semantic body revisions and representation revisions compare independently only
for the same owning capture and immutable main. Cache merges and exact file-ref
replacement run inside the same scope/tombstone-fenced SQLite transaction. Legacy
captures stay frozen; stale responses cannot transfer a representation set to a
replacement main. Avatar enrichment may advance only an otherwise identical
complete user/chat projection, not replace unversioned entity semantics.
Conflicting equal message/representation clocks fail projection or storage before
the batch's journal progress can commit. Schema-aware mergers return both the
selected body and its semantic revision metadata; SQLite persists that selected
clock, never the incoming batch's clock on a hybrid. `fresher_body` authorizes
installing its candidate unchanged only when that standalone body preserves both
clocks; callers needing a hybrid use `merge_message` and install its returned body.
Action hints and representation notifications are source-fenced fetch demands,
not receipts or file grants. `refresh_media_representation` fetches exact chat
photo, profile entry or asset-version projections using existing query methods;
it returns a typed Any and narrowly refreshes matching cached avatars. There is
no new durable catalog/gallery cache or representation RPC. Hosts query known
owning references on startup/reconnect even without a hint.
Action-status reads retain a connection-local demand counter independently of
best-effort app event delivery. A hint arriving during a durable query causes one
additional authoritative read, including after a cancellation decision, under the
original query budget. Direct typed status reads also refetch once; direct cancel
calls are not resubmitted. Continued hint churn and missing notifications remain
covered by the bounded periodic retained-job recovery after startup/reconnect.
Kindless chat-photo references query Channels.GetChannels first, then
Messages.GetChats only after an authoritative empty channel result, using the
same ID and one bounded lookup budget. Errors (including policy denial,
unavailability, NotFound and Unimplemented) never select the other owner.
Nonempty responses must match the queried room kind/ID and original photo/main
identities with a positive representation revision; replacements do not fall
back. The GetChannels response uses the existing `chats` projection field. This
requires the gateway channel lookup implementation, including empty results for
non-channel IDs; an undeployed stub fails closed.

UniFFI uses ordinary preparation/adoption/send methods, domain-specific durable
enqueue/query/cancel/inspection adapters, representation refresh and domain hint
payloads. New photo/asset action adapters fail with Unsupported in published-pin
builds, before dispatch. Binding metadata and native libraries must be built
together; consumer execution and backend workers remain separate qualification.

The current isolated candidate exposes one optional `thumbnail`, not a role or
variant list. `ThumbnailSource` declares one completed uploaded file with positive
width/height; `Thumbnail` reports the same shape with measured dimensions. Missing
is valid and differs from a present record with zero dimensions. Inline tiny JPEG
preview bytes remain separate. Photo/document/video/audio/sticker inputs and
profile/catalog sources all use this shared shape; voice accepts no thumbnail.
Cache enrichment compares independent representation revisions only within the
same immutable source identity and replaces exact file membership atomically.

Candidate UniFFI preparation returns `FfiUploadedThumbnail`, pairing the ordinary
`FileInfo` protobuf with a typed `FfiThumbnailSource` for that exact file ID.
`media_source_with_thumbnail`, `chat_photo_source_with_thumbnail`,
`profile_photo_source_with_thumbnail`, and
`sticker_asset_source_with_thumbnail` explicitly assemble new sources and reject
unknown fields against the selected descriptor before decoding. They never edit
retained requests or keys. `thumbnail_from_protobuf` returns the shared owned
`FfiThumbnail` projection.
These helpers are unsupported against the unchanged published pin. The candidate
preparation record layout differs from the published compatibility layout; rebuild
bindings and native libraries together. This repository has no binding generator
or committed Swift/Kotlin bindings; consumer generation and device execution are
separate requirements.

This clean candidate cutover requires a fresh isolated SQLite cache, plus a fresh
server database target provisioned by its owner. The intentional global wire
break includes auth and bot contracts. The reset SQLite marker rejects older
candidate caches before retained protobuf decoding or network use; manifest v5
also rejects pre-renumber preparation state. See the
[candidate contract and clean cutover gate](PROTO_PINNING.md#optional-future-schema-development)
for numbering, encoding identities, build-mode support, and qualification
requirements. No automatic disposal, migration, rekey, uncertain-action replay
or account wipe is performed.
The published registry coordinate and ordinary build lane remain unchanged;
the candidate is opt-in only.

SQLite action numbers 7..13 are incompatible experimental data. Opening a job or
inventory containing those values fails explicitly. No migration reinterprets
their payload, uncertainty or keys. Only disposable candidate data may be removed
with `Storage::discard_incompatible_candidate_actions`, scoped to one principal;
this loses local evidence and does not cancel remote work. Never use disposal to
rekey uncertain real actions. New actions use distinct discriminants 14..21.

The shared uploader accepts negotiated candidate parts of exactly 64, 128, 256 or
512 KiB. Boundaries are retained per immutable upload intent across restart and
checked against subsequent server snapshots. All mains and regular thumbnails use
this path, with at most the server's allowed parallelism and the existing local
byte/task caps. A 2-GiB file at 512 KiB needs 4096 parts and is not rejected by an
artificially smaller part-count limit. Audio, voice and raw documents omit media
inspection intent; uploading them never requires an image/video worker.

## Logout

`logout().await` fences the Client permanently, erases SDK-owned key/capability
cells, joins liveness work, and clears every bound protected namespace through
the acknowledged `SecureEnrollmentStore::clear` callback. Late enrollment
responses cannot revive it. `Ok(true)` confirms remote revocation; `Ok(false)`
means remote revocation is unconfirmed but required local clearing and transport
joins completed. Cleanup errors leave `is_signed_out()` true and do not attest
complete cleanup, even if protected-store invalidation already succeeded.
Resolve failed host invalidation before allowing restoration after restart.
Use a new Client for deliberate sign-in. The
[high-level Logout contract](docs/device-enrollment.md#local-sign-out-and-remote-revocation)
requires a single remote-eligibility budget of up to three seconds, captured before fencing
or lifecycle waits, retained without renewal on join/retry. Local durable clearing
is independent and may outlast three seconds. The
[qualification requirements](PROTO_PINNING.md#sdk-pins-and-qualification) do not imply
a global three-second wall-clock guarantee for `logout()`.
Raw calls retain their selected timeout; see the
[request-budget integration contract](docs/architecture.md#request-budget-integration-contract).

## Portable schema checks

Browser networking is unsupported. `libtinkle-web` has no dependency on the
native client. It exports `network_supported() == false`, an explicit unsupported
`tinkle_connect()`, and portable connectivity schema validation. Portable schema
checks remain available in `tinkle-schema`.

## Checks

```sh
cargo test --locked --workspace --all-features --verbose
cargo clippy --locked --workspace --all-targets --all-features --no-deps -- -D warnings
cargo fmt --all -- --check
python3 -m unittest discover -s tests -p 'test_*.py' -v
cargo check --locked -p libtinkle-web -p tinkle-schema --target wasm32-unknown-unknown
```

`scripts/test-schema-candidate.sh` is optional tooling for future-schema
development, not release qualification. See [public pinning](PROTO_PINNING.md).

The published schema dependency is
`=0.5.0-20261112172535-6099d89af63f.2`. Locked Rust checks and host transport
fixtures qualify only their executed lanes.
Production-server acceptance and consumer application execution require separate evidence.

`Messages::get_scheduled_messages`, `cancel_scheduled_messages`, and
`close_poll` have corresponding named mobile protobuf-byte adapters. Scheduled
lookup requires a target and exactly one collection of 1–100 distinct positive
message IDs or nonzero signed idempotency keys; it is explicit lookup, not list
discovery. Job descriptors do not enter history or the durable sending queue.
Cancellation selects explicit owned IDs; any album item selects its whole pending
group. Only proven canceled states are returned, with no body, and errors may
follow partial effects. Neither a timeout nor an empty canceled set proves rollback.
These facades do not automatically replay cancellation or close mutations.

Rust integration fixtures use the published SDK for encrypted native TCP,
including trust failures and configurable primary/fallback ports. The host-only
`native-fixture` example is also built on that SDK.
`scripts/with-native-fixture.py <consumer command...>` starts it on the host
and cleans it up. Its separate loopback JSON control socket allocates fixtures
and reads application request logs; it is test harness code only.

For host tests, use `--port 0` to avoid fixed-port collisions in Linux's ephemeral
range. The wrapper passes the OS-assigned control port to the consumer through
`TINKLE_NATIVE_FIXTURE_CONTROL_PORT`, which `crates/libtinkle/tests/mobile_fixture.rs`
reads. The default control port is 48080; see
[Rust transport fixtures](docs/platform-ios-android-wasm.md#rust-transport-fixtures).

```sh
python3 scripts/with-native-fixture.py --port 0 cargo test --locked -p libtinkle --test mobile_fixture -- --ignored
```

The native examples `send-message` and `subscribe` require `TINKLE_HOST`,
`TINKLE_SERVER_PIN` (64 hex characters), `TINKLE_SERVER_KEY_ID`, `TINKLE_SCOPE`,
all five `TINKLE_DEVICE_MODEL` / `TINKLE_PLATFORM` / `TINKLE_OS_VERSION` /
`TINKLE_APP_VERSION` / `TINKLE_APP_BUILD`, and `TINKLE_SECURE_STORE_HELPER`.
Optional `TINKLE_PORT` accepts a nonzero TCP port (1–65535); an explicit port
disables fallback. Without it, the examples use primary 443 and fallback 80,
available only after an initial primary dial failure before socket selection.
The `TINKLE_SECURE_STORE_HELPER` executable receives `load-active` and returns the SDK's opaque
record from protected platform storage on stdout. It must also implement `clear`,
returning exit status zero only after full durable namespace invalidation. These
examples do not perform enrollment writes. Production hosts use the Rust or FFI
callback contract directly.
