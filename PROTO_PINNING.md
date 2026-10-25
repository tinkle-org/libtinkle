# Proto pinning (BSR Rust SDKs via public Cargo index)

This workspace consumes BSR-generated bindings directly, with no
vendored `.proto` copies and no local `prost-build` codegen. The public
index needs no token. Authenticated-index credentials are unnecessary
for these public modules.

Registry (`.cargo/config.toml`):

```toml
[registries.buf]
index = "sparse+https://buf.build/gen/cargo/public/"
```

## Syntra (published crates, no BSR binding)

- Syntra protocol types now come from published `syntra-core`
  `=1.0.0-rc.1` (with `syntra-runtime` / `syntra-ws` at the same pin).
- Syntra proto runtime: `prost` `0.13.5` internally. Tinkle uses BSR prost `0.14`.
- Boundary is opaque `Vec<u8>`: Tinkle payload bytes cross into Syntra
  frames without sharing prost types.

## Tinkle (`buf.build/tinkle-org/schema`)

- Module commit: `a444aece5d594f5ca3308858cb33c879`
- Prost data crate: `tinkle-org_schema_community_neoeinstein-prost`
  pinned `0.5.0-20261025011045-a444aece5d59.2`
  (`buf registry sdk version --module=buf.build/tinkle-org/schema --plugin=buf.build/community/neoeinstein-prost`)
- Tonic client crate: `tinkle-org_schema_community_neoeinstein-tonic`
  pinned `0.5.0-20261025011045-a444aece5d59.4`
  (`buf registry sdk version --module=buf.build/tinkle-org/schema --plugin=buf.build/community/neoeinstein-tonic`)
- Runtime: `prost` `0.14`, `tonic` `0.14`.
- Method identifiers (`tinkle-schema::methods`, 172 entries) verified
  against these service definitions. Failures use Syntra `RpcResult.error`.
- `UpdatesService`: `ReadUpdates` (unary gRPC catch-up), `FollowUpdates`
  and `ObserveConnectivity` (server-streaming; ongoing delivery over
  Syntra notifications via the follow binding in `tinkle-schema::sync`).

## Upgrading

Syntra upgrades: first update the exact version requirements for
`syntra-core`, `syntra-runtime`, and `syntra-ws` in `Cargo.toml`,
then run `cargo update` and `cargo test`.

Tinkle binding only:

```sh
buf registry sdk version --module=buf.build/tinkle-org/schema --plugin=buf.build/community/neoeinstein-prost
buf registry sdk version --module=buf.build/tinkle-org/schema --plugin=buf.build/community/neoeinstein-tonic
```

Copy the versions verbatim into the workspace `Cargo.toml`. Do not
construct them by hand. `cargo update -p <crate>` then `cargo test --workspace`.
