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

The coordinated pins are exact crates.io `=1.0.0-rc.4` for all three
Syntra crates, retaining `default-features = false, features = ["tcp"]` for
`syntra-net`. The companion gateway uses published Go `v1.0.0-rc.3`.
Read `Cargo.toml` / `Cargo.lock` for exact installed versions/checksums.

| Authority | Published reference |
| --- | --- |
| Protocol and Go integration | [Go rc.3 README](https://github.com/tinkle-org/syntra-go/blob/v1.0.0-rc.3/README.md) and [connections](https://github.com/tinkle-org/syntra-go/blob/v1.0.0-rc.3/CONNECTION.md) |
| Rust core release | [syntra-core-v1.0.0-rc.4](https://github.com/tinkle-org/syntra-rs/tree/syntra-core-v1.0.0-rc.4) |
| Rust runtime release | [syntra-runtime-v1.0.0-rc.4](https://github.com/tinkle-org/syntra-rs/tree/syntra-runtime-v1.0.0-rc.4) and [runtime budget API](https://docs.rs/syntra-runtime/1.0.0-rc.4/syntra_runtime/elapsed/struct.ElapsedDeadline.html) |
| Rust TCP release | [syntra-net-v1.0.0-rc.4](https://github.com/tinkle-org/syntra-rs/tree/syntra-net-v1.0.0-rc.4) and [connection ownership](https://github.com/tinkle-org/syntra-rs/blob/syntra-net-v1.0.0-rc.4/CONNECTION.md) |

These are crate-specific release tags, not a workspace-wide `v1.0.0-rc.4` tag.
SDK mechanics remain upstream; the application budget integration contract is in
[architecture](docs/architecture.md#request-budget-integration-contract).
The rc.4 consumers paired with Go rc.3 passed
[scoped application request-budget qualification](../chorus/src/controllers/client-api/tests/native_acceptance/README.md#current-qualification).
The acceptance README identifies the externally retained evidence bundle with
tested source fingerprints, published dependency checksums, commands, and results.
This closes the request-wide budget gap for that consumer implementation and those
pins, not broader standalone checks, CI status, or rollout readiness.
The dated rc.3 results below remain historical, not rc.4 evidence.

Use the pinned toolchain and ordinary published dependencies, without source
overrides. Run standalone checks separately; the paired result does not establish
their outcome:

```sh
cargo metadata --locked --all-features --format-version 1
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets --all-features
cargo test --locked --workspace --all-features --verbose
cargo clippy --locked --workspace --all-targets --all-features --no-deps -- -D warnings
bash scripts/generate-bindings.sh --check
cargo check --locked -p libtinkle-web -p tinkle-schema --target wasm32-unknown-unknown
python3 scripts/with-native-fixture.py cargo test --locked -p libtinkle --test mobile_fixture -- --ignored
```

Use the separate [Chorus actual-application lane](../chorus/src/controllers/client-api/tests/native_acceptance/README.md#run)
for the baseline fixture; cache-only paired regressions and reproduction commands
remain in that external evidence bundle. Changed pins or budgets require fresh
scoped qualification. Retain revisions/dirty baseline, toolchains, resolved
versions and checksums/features, commands/filters, timestamps, invocation IDs,
exit results, skips, and raw evidence paths.

## Current published schema pin

The published dependency is
`tinkle-org_schema_community_neoeinstein-prost` version
`0.5.0-20261107080548-dc41c2199c76.2`, with Cargo checksum
`4f345088e5cc4dbebbf72860bc48afdec2297fc7f70caca2f0c787fb340d3eb0`.
BSR commit `dc41c2199c764d89b816408e3cb39235` was published from schema Git
commit `2b15bb6db2e282f8f962742a800cab1f15ab54c5` (schema PR #57).
It includes enrollment, session management, initialization, KeepAlive and
`AuthService/GetEnrollmentDisposition` with its exact reference and response types.

The coordinate was retrieved, not constructed:

```console
$ buf registry sdk version --module=buf.build/tinkle-org/schema:dc41c2199c764d89b816408e3cb39235 --plugin=buf.build/community/neoeinstein-prost:v0.5.0
0.5.0-20261107080548-dc41c2199c76.2
```

Required Rust CI, native packaging, binding parity and release qualification use
the same published dependency and committed lockfile with `--locked`. They do not
depend on a schema checkout, a Cargo registry patch or `SCHEMA_CANDIDATE_REF`.
The descriptor tests check the complete method inventory and exact message shapes.

## Historical rc.3 published-pin validation (2026-11-07)

These historical checks ran with Rust Syntra rc.3, not the rc.4 pins above,
in the original, unpatched libtinkle workspace on macOS arm64
with Rust/Cargo 1.99.1, including the existing dirty enrollment and workflow-test
changes. The downloaded `.crate` SHA-256 matches the Cargo checksum above.
`cargo metadata --locked --all-features --format-version 1` confirmed the schema
package resolves from the public BSR index, Syntra remains crates.io rc.3 with
only `syntra-net`'s `tcp` feature enabled, and no tonic package is present.

The deliberate lockfile update was:

```sh
cargo update -p tinkle-org_schema_community_neoeinstein-prost --precise 0.5.0-20261107080548-dc41c2199c76.2
```

Only that package's version and checksum changed in `Cargo.lock`.

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo check --locked --workspace --all-targets --all-features` | Passed |
| `cargo test --locked --workspace --all-features` | 229 runtime tests passed, 6 ignored; one compile-fail doctest passed, one doctest ignored |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo check --locked -p libtinkle-web -p tinkle-schema --target wasm32-unknown-unknown` | Passed |
| `cargo +1.91 check --locked --offline --workspace --all-targets` | Passed with the declared MSRV |
| `python3 scripts/with-native-fixture.py cargo test --locked -p libtinkle --test mobile_fixture -- --ignored` | Both host TCP fixture tests passed, none ignored |
| `bash scripts/generate-bindings.sh --check` | All four Swift/Kotlin binding files matched; no regeneration needed |
| `python3 -m unittest discover -s tests -p 'test_*.py' -v` | 128 passed |
| `actionlint .github/workflows/ci.yml` | Passed |
| `python3 scripts/sdk-ci.py toolchain` | Passed |

The workspace test also invokes the otherwise-ignored restart child helper in a
separate process: that additional invocation passed (1/1). The initial workflow
test run had one failure because CI still selected a schema-candidate copy;
removing that selection made the existing published-dependency assertion pass.
All post-change checks above passed with zero failures.

The mobile fixture is a host-only Rust SDK/server harness. These results do not
establish installed iOS/Android consumer acceptance, Keychain/Keystore durability,
or actual Chorus production-handler acceptance. External acceptance tests remained
ignored in the ordinary workspace run above. The separate published-only Chorus
production-handler run **passed** on 2026-11-07,
08:52:05–08:52:38 UTC, invocation `ca7630bb-35ae-4054-a7bd-5958e5200ca4`.
That run used Go rc.2/Rust rc.3, with direct Logout `Ok(true)` in 184 ms and a
first-wrapped correlated response in 125 ms. Its first attempt had a stale
restore-probe assertion corrected only in the fixture. Historical raw evidence:
`/var/folders/xv/g_j4t2793xdbd_wnr290xgyw0000gn/T/opencode/native-published-logout-20261107-fixed/`
(`published-sdk-evidence.json`, `strict-logout-evidence.txt`). These are historical
temporary-file references, not an archive or an availability guarantee.

The later recorded precommit run on 2026-11-07, 16:52:40–16:53:16 UTC,
invocation `e409fd86-d039-43dc-911a-62b683684623`, also used the old pins and
reported healthy direct/wrapped responses in 189 ms / 64 ms. Evidence reference:
`/var/folders/xv/g_j4t2793xdbd_wnr290xgyw0000gn/T/opencode/native-precommit-20261107/`.
Neither run qualifies the new request-wide budget contract. Mobile OS storage,
network/trust, capacity, deployment, and the mandatory
[authn restore gate](../chorus/src/services/authn/README.md#authn-restore-safety-gate)
remain separate.

## Optional future-schema development

The candidate generator uses `buf.build/community/neoeinstein-prost:v0.5.0`,
revision 2, with `bytes=.` and prost/prost-types `=0.14.4`. It emits only into a
temporary artifact directory, preserves the generated package exports, and
records the exact descriptor SHA-256. A registry patch in the copied workspace
must resolve to that generated manifest. No candidate release coordinate is
assigned or published. This is an opt-in integration aid, not release qualification.

```sh
./scripts/test-schema-candidate.sh --output /absolute/temp/candidate
./scripts/test-schema-candidate.sh --output /absolute/temp/candidate --bindings
```

See [device enrollment](docs/device-enrollment.md) for full checks and external
client-api acceptance instructions.

## Published artifact integration

For a later publication, retrieve the actual prost
coordinate rather than constructing a version:

```sh
buf registry sdk version --module=buf.build/tinkle-org/schema --plugin=buf.build/community/neoeinstein-prost
```

Update the workspace dependency with that returned coordinate, update Cargo.lock,
and run ordinary workspace checks plus binding parity against the released
artifact. Descriptor tests must match the full method inventory and exact
SignIn/enrollment/InitConnect fields.

Keep the three Syntra dependencies aligned with the
[SDK pins](#sdk-pins-and-qualification), without source
patches. A changed manifest/lockfile requires new qualification; the historical
schema-only lockfile update above is not evidence for a later SDK upgrade.
