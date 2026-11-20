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

Use the latest published prost artifact with the exact pin
`=0.5.0-20261118031328-a03ec89f7e36.2`. BSR module commit
`a03ec89f7e3640b5bcd5b86c5bc297f1` maps to schema Git revision
`45c4e3d0e7901e680aa2830de2946156939c5199`. The Syntra pins above are unchanged.
Go consumers own their independent published coordinates in their manifests.

Ordinary registry builds with this pin and matching SDK source use
`compact-wire-reset-v1` and the [asynchronous action APIs](README.md#asynchronous-application-actions),
including with `--all-features`.
Before qualifying a build, verify that `Cargo.toml` and `Cargo.lock` resolve this
exact dependency for `--locked` CI. A checkout that still resolves the previous
pin does not exercise this contract. Registry builds need no extra environment
variables, schema checkout, Buf invocation, or Cargo registry patch. Do not set
`TINKLE_SCHEMA_CANDIDATE_*` for these builds.

### Wire identity and retained preparation

The published schema numbers fields consecutively from 1 within each message,
with no reserved names or ranges and no placeholder fields. This is an
intentional global protobuf wire break, including auth and bot contracts.
Preview and representation-revision tags are not preserved. Do not mix
pre-renumber peers or payloads with this release.

`ThumbnailSource` and `Thumbnail` are singular, each with `file_id: int64` at
tag 1 and positive `width` and `height` (`uint32`) at tags 2 and 3. There is no
media variant enum or list. The descriptor tests check the full method inventory
and exact message shapes, including those in
`crates/tinkle-schema/tests/representations.rs` and
`crates/tinkle-schema/tests/auth_contract.rs`.

`crates/tinkle-storage/src/lib.rs` records `compact-wire-reset-v1` under
`meta.media_encoding`. It rejects older markers, including
`singular-thumbnail-v1` and `published-media-v1`, before retained protobuf
decoding or network use. The marker is a compatibility epoch, not a descriptor
digest.

`crates/libtinkle/src/sending.rs` requires attachment manifest v5 with
`wire_identity: compact-wire-reset-v1`,
`media_contract: singular-thumbnail-compact-wire-reset-v1`, and a settings
string ending in `:singular-thumbnail-compact-wire-reset-v1`. Older manifests
and mismatched v5 manifests fail before retained paths or protobufs are used.
Old payloads are never translated or relabelled.

### Clean media cutover gate

Before switching wire epochs, coordinate clients and servers:

- Start with a fresh, isolated application cache and artifact namespace.
- If the host persists cursors separately, start with a fresh host cursor
  namespace. Do not carry forward old checkpoints.
- Have the server owner provision fresh, compatible database and media state as
  a separate step. Do not connect to peers using the previous wire epoch.

An unmarked SQLite database with retained bodies, file membership, cached files,
transfer intents, queued background work, or outgoing actions is not empty.
Opening it fails without relabelling or deleting its state. The encoding check
runs again under SQLite's write lock before an empty cache is stamped.

Record the selected namespaces and the explicit cutover decision. Keep incompatible
databases and artifacts isolated so uncertain actions can be investigated in their
original environment. Do not change original keys, CAS values, or upload proofs,
or replay mutations against fresh state. Disposing of test data does not cancel
remote work. Neither the SDK nor the local generator resets server state, wipes
accounts, clears protected enrollment, or deploys anything as part of cutover.

Generate UniFFI bindings from the exact native build with the pinned UniFFI
toolchain. Do not mix bindings and libraries across the cutover; optional record
defaults do not make the builds ABI compatible.

## Optional future-schema development

For unpublished schema changes, pass `--async-actions` explicitly before any
local generation or build work. The flag is required for the local workflow,
not for registry builds.

```sh
./scripts/test-schema-candidate.sh --async-actions --output /absolute/temp/candidate
```

To select a schema checkout and run a specific command in the isolated workspace:

```sh
python3 scripts/schema-candidate.py --schema /absolute/schema --async-actions \
  --output /absolute/temp/media-candidate -- cargo test --locked --workspace --all-features
```

`scripts/schema-candidate.py` snapshots the schema and Buf configuration outside
the checkout, then builds the descriptor and generates Rust from that same
snapshot. It rejects source changes during snapshot or generation. It uses
`buf.build/community/neoeinstein-prost:v0.5.0`, revision 2, with `bytes=.`, and
prost/prost-types `=0.14.4`.

The helper checks that the registry patch resolves to the generated manifest in
the copied workspace. The patch, generated prost files, and candidate lockfile
stay outside this repository. `candidate.json` records the source and descriptor
SHA-256 digests, wire identity, media contract, and action opt-in as local evidence.
The generated package version matches the registry pin solely to make the patch
resolve; it does not assign a release coordinate or qualify the published artifact.

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
The pin and checks listed here do not claim live-server, backend-worker, or
simulator/device execution.
