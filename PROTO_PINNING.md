# Public dependency pinning

The SDK uses prost data bindings from the public BSR Cargo index:

```toml
[registries.buf]
index = "sparse+https://buf.build/gen/cargo/public/"
```

There is no tonic dependency, local production protobuf build, or vendored
`.proto` tree. `tinkle-schema::proto` re-exports the prost artifact; canonical
method inventory and descriptor checks validate the application boundary.

## SDK pins and qualification

The coordinated pins are exact crates.io `=1.0.0-rc.5` for all three
Syntra crates, retaining `default-features = false, features = ["tcp"]` for
`syntra-net`. The published Go interoperability reference is `v1.0.0-rc.3`.
Read `Cargo.toml` / `Cargo.lock` for exact installed versions/checksums.

| Authority | Published reference |
| --- | --- |
| Protocol and Go integration | [Go rc.3 README](https://github.com/tinkle-org/syntra-go/blob/v1.0.0-rc.3/README.md) and [connections](https://github.com/tinkle-org/syntra-go/blob/v1.0.0-rc.3/CONNECTION.md) |
| Rust core release | [syntra-core-v1.0.0-rc.5](https://github.com/tinkle-org/syntra-rs/tree/syntra-core-v1.0.0-rc.5) and [crate](https://crates.io/crates/syntra-core/1.0.0-rc.5) |
| Rust runtime release | [syntra-runtime-v1.0.0-rc.5](https://github.com/tinkle-org/syntra-rs/tree/syntra-runtime-v1.0.0-rc.5) and [runtime API](https://docs.rs/syntra-runtime/1.0.0-rc.5/syntra_runtime/) |
| Rust TCP release | [syntra-net-v1.0.0-rc.5](https://github.com/tinkle-org/syntra-rs/tree/syntra-net-v1.0.0-rc.5), [crate](https://crates.io/crates/syntra-net/1.0.0-rc.5), and [connection ownership](https://github.com/tinkle-org/syntra-rs/blob/syntra-net-v1.0.0-rc.5/CONNECTION.md) |

These are crate-specific release tags, not a workspace-wide release tag.
SDK mechanics remain upstream; the application budget integration contract is in
[architecture](docs/architecture.md#request-budget-integration-contract).

`Cargo.lock` records these crates.io checksums:

| Crate | SHA-256 |
| --- | --- |
| `syntra-core` | `3515ad52dbb7f438afdfe32c7b1a84f6fbb417ff49f9b977dcb92dcb7ebbdb69` |
| `syntra-runtime` | `0cf04931473f8948d1c94ba19d81832474c3b8664e5a118ce04d6c5e375abfd5` |
| `syntra-net` | `72e9fc72c2efe8ba24ecd281b376d24f94107eee8f8b7052445581365891028b` |

Use the pinned toolchain and ordinary published dependencies, without source
overrides. These are verification commands, not recorded execution results:

```sh
cargo metadata --locked --all-features --format-version 1
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets --all-features
cargo test --locked --workspace --all-features --verbose
cargo clippy --locked --workspace --all-targets --all-features --no-deps -- -D warnings
cargo check --locked -p libtinkle-web -p tinkle-schema --target wasm32-unknown-unknown
python3 scripts/with-native-fixture.py --port 0 cargo test --locked -p libtinkle --test mobile_fixture -- --ignored
```

Rust unit tests and encrypted TCP interoperability fixtures are separate from
consumer-owned ABI execution and externally orchestrated production-server acceptance
for enrollment, Logout, session review, and retained state. Changed pins or
budgets require fresh scoped qualification in each applicable lane. Keep exact
execution results with the run's evidence; do not transfer an older result to
changed source or pins.
Retain revisions/dirty baseline, toolchains, resolved
versions and checksums/features, commands/filters, timestamps, invocation IDs,
exit results, skips, and raw evidence paths.

## Current published schema pin

`Cargo.toml` and `Cargo.lock` pin the published prost artifact
`=0.5.0-20261112172535-6099d89af63f.2` and its checksum. BSR commit
`6099d89af63f4813bba67870d08500ea` maps to schema Git revision
`e0038da40641a39ee90e8d4b95a8c3f36e213044`. This baseline includes the public
naming/field migration and scheduled-message/poll contracts used by the SDK;
no source override or candidate generation is required. Go consumers own their
independent published coordinates in their manifests.

Required Rust CI and source qualification use
the same published dependency and committed lockfile with `--locked`. They do not
depend on a schema checkout or a Cargo registry patch.
The descriptor tests check the complete method inventory and exact message shapes.

## Optional future-schema development

The candidate generator uses `buf.build/community/neoeinstein-prost:v0.5.0`,
revision 2, with `bytes=.` and prost/prost-types `=0.14.4`. It emits only into a
temporary artifact directory, preserves the generated package exports, and
records the exact descriptor SHA-256. A registry patch in the copied workspace
must resolve to that generated manifest. No candidate release coordinate is
assigned or published. This is an opt-in integration aid, not release qualification.

```sh
./scripts/test-schema-candidate.sh --async-actions --output /absolute/temp/candidate
```

Asynchronous application actions require the explicit candidate capability:

```sh
python3 scripts/schema-candidate.py --schema /absolute/schema --async-actions \
  --output /absolute/temp/media-candidate -- cargo test --locked --workspace --all-features
```

The helper always sets
`TINKLE_SCHEMA_CANDIDATE_WIRE_ID=compact-wire-reset-v1` in the copied workspace,
independently of asynchronous actions. With `--async-actions`, it also sets
`TINKLE_SCHEMA_CANDIDATE_ASYNC_ACTIONS=1` and
`TINKLE_SCHEMA_CANDIDATE_MEDIA_CONTRACT=singular-thumbnail-compact-wire-reset-v1`.
The shared build script, `scripts/schema-cfg.rs`, enables
`cfg(tinkle_schema_candidate)` for the wire identity and `cfg(tinkle_async_actions)`
for the action capability. The older action capability alone fails at build time.
Ordinary published-pin builds, including `--all-features`, enable neither cfg;
do not set these variables against the published artifact.

`tinkle-storage` supports the reset wire identity without asynchronous actions.
The `libtinkle` and `tinkle-ffi` build scripts explicitly reject that nonasync
runtime configuration because its adapter still uses removed media fields.
The helper checks the whole workspace before running the requested command, so
use `--async-actions` for the workspace workflow above. Candidate metadata records
the wire identity, descriptor digest, media contract, and action opt-in separately
from release coordinates. Generated prost files and the candidate lockfile remain
outside this repository.

The generator snapshots schema source/configuration outside the checkout before
building the descriptor and generating Rust from that same snapshot. It rejects
source changes during snapshot/generation; wait for schema writes to finish before
the final run. Evidence records source and descriptor digests, the contract and the
isolated manifest path. No generated public protocol source is copied into this
repository or used by ordinary checks.

The approved candidate renumbers fields throughout the public schema consecutively
from 1 within each message, with no reserved names/ranges or placeholder fields.
This is an intentional global protobuf wire break, including auth and bot
contracts, not just media. Preview and representation-revision tags are not
preserved. Do not mix pre-renumber payloads or peers with the renumbered candidate.
The published schema and Syntra pins above remain unchanged; this schema is
available only through the opt-in candidate workflow, not ordinary builds.

The candidate retains singular `ThumbnailSource` and `Thumbnail`, each with
`file_id: int64` at 1 and positive `width`/`height: uint32` at 2/3, and no media
variant enum/list. Descriptor assertions in
`crates/tinkle-schema/tests/representations.rs` and
`crates/tinkle-schema/tests/auth_contract.rs` distinguish candidate shapes from
the unchanged published pin. Final qualification requires fresh execution
evidence; these assertions are not test results.

SQLite records `compact-wire-reset-v1` under the `media_encoding` key in `meta` through
`crates/tinkle-storage/src/lib.rs`, independently of the async-actions flag.
It rejects older markers, including `singular-thumbnail-v1` and
`published-media-v1`, before retained protobuf decoding or network use.
This is an explicit compatibility epoch, not a descriptor digest.
Candidate preparation in `crates/libtinkle/src/sending.rs` requires manifest v5
with `wire_identity: compact-wire-reset-v1`; v4 and older manifests are rejected
before retained paths or protobufs are used. Async preparation additionally
requires `media_contract: singular-thumbnail-compact-wire-reset-v1` and the
matching settings suffix. Published builds retain their own v1 manifest behavior
and reject candidate v4/v5 manifests. No old payloads are translated or relabelled.

### Clean media cutover gate

The renumbered candidate requires a fresh isolated application cache and
coordinated fresh server media state. An unmarked database with retained bodies,
file membership, cached files, transfer intents, queued background work or outgoing
actions is not an empty cache. Opening it fails without relabelling or deleting its
state; the encoding check runs again under SQLite's write lock before stamping an
empty cache. Previously stamped pre-renumber candidate caches are rejected by
their old marker. Published builds keep their existing unmarked-cache compatibility.

Record the selected disposable media/cache namespace and explicit clean-reset
decision before candidate execution. Keep incompatible databases and artifacts
isolated for inspection and resolution of uncertain actions. Do not translate old
protobufs, change original keys/CAS/upload proofs, or replay mutations against a
fresh namespace. An explicit test-data disposal decision does not cancel any
remote operation. Fresh PostgreSQL/media test state is permitted only as a separate
coordinated setup step; the SDK and candidate generator perform no server reset,
account wipe, protected-enrollment clear or deployment action.

An actual published schema SDK and separately qualified pin upgrade are required
before enabling this capability in ordinary consumer builds. Normal and candidate
FFI exports differ: generate bindings and native libraries from the same source,
cfg and pinned UniFFI toolchain. Optional record defaults are not mixed-build ABI
compatibility.

See [device enrollment](docs/device-enrollment.md#published-schema-checks) for
SDK checks and their verification boundaries.

## Published artifact integration

For a subsequent upgrade, retrieve the actual published prost
coordinate rather than constructing a version:

```sh
buf registry sdk version --module=buf.build/tinkle-org/schema --plugin=buf.build/community/neoeinstein-prost
```

Update the workspace dependency with that returned coordinate, update Cargo.lock,
and run ordinary workspace checks against the released artifact.
Descriptor tests must match the full method inventory and exact
SignIn/enrollment/InitConnect fields.

Keep the three Syntra dependencies aligned with the
[SDK pins](#sdk-pins-and-qualification), without source
patches. A changed manifest/lockfile requires new qualification. Mobile protected
storage, network/trust distribution, capacity, deployment, and server-side
admission, revocation, and restore safety require separate qualification. The SDK
expects authoritative [retirement evidence](docs/device-enrollment.md#permanent-retirement-observation);
its checks do not establish server-side rollback protection.
Rust host tests and consumer application execution have distinct evidence scopes;
see [integration verification](docs/platform-ios-android-wasm.md#consumer-qualification).
