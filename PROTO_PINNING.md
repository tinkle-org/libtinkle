# Proto pinning (BSR Rust SDKs via public Cargo index)

This workspace consumes BSR-generated prost bindings directly, with no
vendored `.proto` copies and no local `prost-build` codegen. The public
index needs no token. Authenticated-index credentials are unnecessary
for these public modules.

Registry (`.cargo/config.toml`):

```toml
[registries.buf]
index = "sparse+https://buf.build/gen/cargo/public/"
```

## Syntra (`buf.build/tinklecorp/syntra`)

- Plugin: `buf.build/connectrpc/rust` for inspection, but the Rust
  dependency is the prost data crate `buf.build/community/neoeinstein-prost`.
- Crate: `tinklecorp_syntra_community_neoeinstein-prost`
- Pinned version: `0.5.0-20261020002338-abd0f913f41c.2`
  (`buf registry sdk version --module=buf.build/tinklecorp/syntra --plugin=buf.build/community/neoeinstein-prost`)
- Module commit: `abd0f913f41c4a80bec862296b8f1338`
- Runtime: `prost` / `prost-types` `0.14.1` (from the generated crate).
- Role: data structures only. Protocol behavior lives in `syntra-core`
  and never in generated helpers.

## Tinkle (`buf.build/tinklecorp/tinkle-proto`)

- Plugin: same `community/neoeinstein-prost` prost data plugin.
- Crate: `tinklecorp_tinkle-proto_community_neoeinstein-prost`
- Pinned version: `0.5.0-20261020173106-64b4706c3194.2`
  (`buf registry sdk version --module=buf.build/tinklecorp/tinkle-proto --plugin=buf.build/community/neoeinstein-prost`)
- Module commit: `64b4706c319441c3a36f9bde125d5594`
- Runtime: same `prost` `0.14.1`.
- Method identifiers (`tinkle-schema::methods`, 169 entries) were verified
  against these service definitions. Failures use Syntra `RpcResult.error`.

## Related SDKs inspected (not used)

- `tinklecorp_syntra_connectrpc_rust` (`0.9.0-20261020002338-abd0f913f41c.3`)
  and `tinklecorp_tinkle-proto_connectrpc_rust`
  (`0.9.0-20261020173106-64b4706c3194.3`): Connect-RPC client runtime on
  `buffa` / `connectrpc`. Rejected here because this SDK must implement
  Syntra transport itself and stay on prost data-only bindings.
- `tinklecorp_*_anthropics_buffa` message crates: underlying buffa
  messages for the connectrpc SDKs. Same reason.

## Upgrading

```sh
buf registry sdk version --module=buf.build/tinklecorp/syntra --plugin=buf.build/community/neoeinstein-prost
buf registry sdk version --module=buf.build/tinklecorp/tinkle-proto --plugin=buf.build/community/neoeinstein-prost
```

Copy the two versions verbatim into the workspace `Cargo.toml`. Do not
construct them by hand. `cargo update -p <crate>` then `cargo test --workspace`.
