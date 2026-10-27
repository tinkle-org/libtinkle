//! BSR-generated prost bindings for `tinkle.v1`.
//!
//! Data structures only (BSR module `buf.build/tinkle-org/schema`,
//! `community/neoeinstein-prost` plugin, via the public Cargo index).
//! Application behavior (validation, cursors, method dispatch) lives in
//! sibling modules. See `PROTO_PINNING.md` for pinned versions.

pub use tinkle_org_schema_community_neoeinstein_prost::tinkle::v1::*;

/// Tonic gRPC clients for `tinkle.v1` (BSR `community/neoeinstein-tonic`).
/// Native only; browsers use Syntra follow binding for delivery.
#[cfg(not(target_arch = "wasm32"))]
pub mod grpc {
    pub use tinkle_org_schema_community_neoeinstein_tonic::tinkle::v1::tonic::*;
}
