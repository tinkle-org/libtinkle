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

## Syntra (published crates, no BSR binding)

- Syntra protocol types now come from published `syntra-core`
  `=1.0.0-rc.1` (with `syntra-runtime` / `syntra-ws` at the same pin).
  Local prototype crates `crates/syntra-core` and `crates/syntra-ws`
  were removed.
- Syntra proto runtime: `prost` `0.13.5` internally (see `Cargo.lock`:
  `syntra-core` / `syntra-runtime` / `syntra-ws` depend on
  `prost 0.13.5`). Tinkle still uses BSR prost `0.14` (below).
- Boundary is opaque `Vec<u8>`: Tinkle payload bytes cross into Syntra
  frames without sharing prost types. `prost 0.13.5` is the Syntra
  internal runtime; `prost 0.14` (locked to `0.14.4` per `Cargo.lock`)
  is the Tinkle workspace requirement — distinct roles, no type mixing.

## Tinkle (`buf.build/tinklecorp/tinkle-proto`)

- Plugin: same `community/neoeinstein-prost` prost data plugin.
- Crate: `tinklecorp_tinkle-proto_community_neoeinstein-prost`
- Pinned version: `0.5.0-20261020173106-64b4706c3194.2`
  (`buf registry sdk version --module=buf.build/tinklecorp/tinkle-proto --plugin=buf.build/community/neoeinstein-prost`)
- Module commit: `64b4706c319441c3a36f9bde125d5594`
- Runtime: `prost` `0.14` (locked to `0.14.4` per `Cargo.lock`).
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

Syntra upgrades: first update the exact version requirements for
`syntra-core`, `syntra-runtime`, and `syntra-ws` in `Cargo.toml`,
then run `cargo update` and `cargo test`.

Tinkle binding only:

```sh
buf registry sdk version --module=buf.build/tinklecorp/tinkle-proto --plugin=buf.build/community/neoeinstein-prost
```

Copy the version verbatim into the workspace `Cargo.toml`. Do not
construct it by hand. `cargo update -p <crate>` then `cargo test --workspace`.
