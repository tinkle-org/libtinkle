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
./scripts/test-schema-candidate.sh --output /absolute/temp/candidate
```

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
