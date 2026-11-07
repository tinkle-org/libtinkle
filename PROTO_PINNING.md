# Public schema pinning

The SDK uses prost data bindings from the public BSR Cargo index:

```toml
[registries.buf]
index = "sparse+https://buf.build/gen/cargo/public/"
```

There is no tonic dependency, local production protobuf build, or vendored
`.proto` tree. `tinkle-schema::proto` re-exports the prost artifact; canonical
method inventory and descriptor checks validate the application boundary.

## Current published pin

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

## Local published-pin validation (2026-11-07)

These checks ran in the original, unpatched libtinkle workspace on macOS arm64
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
ignored in the ordinary workspace run above. The parent subsequently ran the
separate published-only Chorus production-handler lane: **PASSED**, 2026-11-07
08:52:05–08:52:38 UTC, invocation `ca7630bb-35ae-4054-a7bd-5958e5200ca4`.
[Chorus evidence](../chorus/src/controllers/client-api/SYNTRA_E2E.md#published-application-acceptance-2026-11-07)
records the first attempt's stale probe assertion, fixture-only correction, raw
pins/checksums, and passing rerun. Mobile OS storage, network/trust, capacity,
production deployment, and the operator authn-restore gate remain separate.

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

Syntra remains exact crates.io `syntra-core`, `syntra-runtime`, and `syntra-net`
`=1.0.0-rc.3`. `syntra-net` has default features disabled and enables only `tcp`.
