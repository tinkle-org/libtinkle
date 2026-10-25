//! Actual gRPC catch-up for `ReadUpdates`.
//!
//! Ongoing delivery uses Syntra WebSocket notifications. Catch-up uses
//! real gRPC to `tinkle.v1.UpdatesService/ReadUpdates` with the session
//! credential as bearer metadata. Responses re-enter the same
//! validation/application pipeline as live follows.

#[cfg(not(target_arch = "wasm32"))]
use tonic::{metadata::MetadataValue, transport::Channel};

#[allow(unused_imports)]
use tinkle_schema::proto::{ReadUpdatesRequest, ReadUpdatesResponse};

#[cfg(not(target_arch = "wasm32"))]
use crate::errors::TinkleError;

#[cfg(not(target_arch = "wasm32"))]
fn bearer(token: &[u8]) -> Result<MetadataValue<tonic::metadata::Ascii>, TinkleError> {
    let s = std::str::from_utf8(token)
        .map_err(|e| TinkleError::Grpc(format!("credential must be ASCII: {e}")))?;
    format!("Bearer {s}")
        .parse()
        .map_err(|e| TinkleError::Grpc(format!("invalid bearer: {e}")))
}

#[cfg(not(target_arch = "wasm32"))]
/// Read one bounded replay page over actual gRPC.
///
/// `endpoint` is the gRPC base URL (e.g. `https://gateway.example.com`).
/// `token` is the opaque session credential, sent only as `authorization`
/// metadata, never in the URL or body.
pub async fn read_updates_grpc(
    endpoint: &str,
    token: &[u8],
    req: ReadUpdatesRequest,
    timeout_ms: u32,
) -> Result<ReadUpdatesResponse, TinkleError> {
    let channel = Channel::from_shared(endpoint.to_string())
        .map_err(|e| TinkleError::Grpc(format!("invalid endpoint: {e}")))?
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_millis(timeout_ms.max(1) as u64))
        .connect()
        .await
        .map_err(|e| TinkleError::Grpc(format!("connect: {e}")))?;

    let bearer = bearer(token)?;
    let mut client =
        tinkle_schema::proto::grpc::updates_service_client::UpdatesServiceClient::new(channel);
    let mut request = tonic::Request::new(req);
    request.metadata_mut().insert("authorization", bearer);
    let resp = client
        .read_updates(request)
        .await
        .map_err(|e| TinkleError::Grpc(format!("read_updates: {e}")))?;
    Ok(resp.into_inner())
}

#[cfg(target_arch = "wasm32")]
pub async fn read_updates_grpc(
    _endpoint: &str,
    _token: &[u8],
    _req: tinkle_schema::proto::ReadUpdatesRequest,
    _timeout_ms: u32,
) -> Result<tinkle_schema::proto::ReadUpdatesResponse, crate::errors::TinkleError> {
    Err(crate::errors::TinkleError::Grpc(
        "gRPC catch-up unavailable on wasm; use Syntra follow binding".into(),
    ))
}
