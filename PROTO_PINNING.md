# Public schema pinning

The SDK uses prost data bindings from the public BSR Cargo index:

```toml
[registries.buf]
index = "sparse+https://buf.build/gen/cargo/public/"
```

There is no tonic dependency, local production protobuf build, or vendored
`.proto` tree. `tinkle-schema::proto` re-exports the prost artifact; canonical
method inventory and descriptor checks validate the application boundary.

## Current release gate

The published dependency is
`tinkle-org_schema_community_neoeinstein-prost` version
`0.5.0-20261106161143-2094a3c360bc.2`, with Cargo checksum
`e6265932e1cdd0595280fe04a4b8dd6802e487013a5d183f6dc9223f4d4eefd1`.
It contains the enrollment, session-management, initialization and KeepAlive
contracts. Before the disposition continuation, the normal unpatched baseline
passed with this artifact (173 runtime tests plus one compile-fail doctest).
The retained-cleanup/session slice also passed unpatched (187 runtime tests plus
that doctest) before adding the new disposition types.

`AuthService/GetEnrollmentDisposition` is implemented against its exact
unpublished schema through the isolated candidate workflow. Current source and
the complete method inventory require that new contract; an ordinary unpatched
build with the retained published pin cannot compile this continuation yet.
The production pin stays unchanged until a reviewed publication/consumer repin.
Do not infer this API from enrollment-status messages or invent a coordinate.

The candidate generator uses `buf.build/community/neoeinstein-prost:v0.5.0`,
revision 2, with `bytes=.` and prost/prost-types `=0.14.4`. It emits only into a
temporary artifact directory, preserves the generated package exports, and
records the exact descriptor SHA-256. A registry patch in the copied workspace
must resolve to that generated manifest. No candidate release coordinate is
assigned or published.

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
