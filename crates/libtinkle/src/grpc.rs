//! Native-only tonic unary helper for `GetTargetChanges` / `GetMessages`.
//!
//! Syntra (`call_raw` with `GET_TARGET_CHANGES_METHOD` / `GET_MESSAGES_METHOD`)
//! is the primary transport; this module covers the opt-in dedicated gRPC
//! path against `grpc_endpoint` (native only). Wasm keeps the stub shape
//! compiling and always reports unavailable.

#[cfg(not(target_arch = "wasm32"))]
use tonic::{metadata::MetadataValue, transport::Channel};

#[cfg(not(target_arch = "wasm32"))]
use zeroize::Zeroizing;

#[allow(unused_imports)]
use tinkle_schema::proto::{GetMessagesRequest, GetMessagesResponse};

#[allow(unused_imports)]
use tinkle_schema::proto::{GetTargetChangesRequest, GetTargetChangesResponse};

#[allow(unused_imports)]
use tinkle_schema::proto::{
    AbortUploadRequest, AbortUploadResponse, BeginUploadRequest, BeginUploadResponse,
    CompleteUploadRequest, CompleteUploadResponse, DownloadPartRequest, DownloadPartResponse,
    GetFileInfoRequest, GetFileInfoResponse, GetUploadStatusRequest, GetUploadStatusResponse,
    UploadPartRequest, UploadPartResponse,
};

#[cfg(not(target_arch = "wasm32"))]
use crate::errors::TinkleError;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn bearer(token: &[u8]) -> Result<MetadataValue<tonic::metadata::Ascii>, TinkleError> {
    let s = std::str::from_utf8(token)
        .map_err(|e| TinkleError::GrpcAuth(format!("credential must be ASCII: {e}")))?;
    let header: Zeroizing<String> = Zeroizing::new(format!("Bearer {s}"));
    header
        .parse()
        .map_err(|e| TinkleError::GrpcAuth(format!("invalid bearer: {e}")))
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn connect_channel(endpoint: &str, timeout_ms: u32) -> Result<Channel, TinkleError> {
    Channel::from_shared(endpoint.to_string())
        .map_err(|e| TinkleError::GrpcConfig(format!("invalid endpoint: {e}")))?
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_millis(timeout_ms.max(1) as u64))
        .connect()
        .await
        .map_err(|e| TinkleError::Grpc(format!("connect: {e}")))
}

#[cfg(not(target_arch = "wasm32"))]
fn grpc_code(code: tonic::Code) -> syntra_core::error::ErrorCode {
    use syntra_core::error::ErrorCode as C;
    match code {
        tonic::Code::Ok => C::Unspecified,
        tonic::Code::Cancelled => C::Cancelled,
        tonic::Code::Unknown => C::Unknown,
        tonic::Code::InvalidArgument => C::InvalidArgument,
        tonic::Code::DeadlineExceeded => C::DeadlineExceeded,
        tonic::Code::NotFound => C::NotFound,
        tonic::Code::AlreadyExists => C::AlreadyExists,
        tonic::Code::PermissionDenied => C::PermissionDenied,
        tonic::Code::ResourceExhausted => C::ResourceExhausted,
        tonic::Code::FailedPrecondition => C::FailedPrecondition,
        tonic::Code::Aborted => C::Aborted,
        tonic::Code::OutOfRange => C::OutOfRange,
        tonic::Code::Unimplemented => C::Unimplemented,
        tonic::Code::Internal => C::Internal,
        tonic::Code::Unavailable => C::Unavailable,
        tonic::Code::DataLoss => C::DataLoss,
        tonic::Code::Unauthenticated => C::Unauthenticated,
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn get_target_changes_on_channel(
    channel: &Channel,
    token: &[u8],
    req: GetTargetChangesRequest,
) -> Result<GetTargetChangesResponse, TinkleError> {
    let bearer = bearer(token)?;
    let mut client =
        tinkle_schema::proto::grpc::messages_service_client::MessagesServiceClient::new(
            channel.clone(),
        );
    let mut request = tonic::Request::new(req);
    request.metadata_mut().insert("authorization", bearer);
    let resp = client
        .get_target_changes(request)
        .await
        .map_err(|s| TinkleError::GrpcStatus {
            code: grpc_code(s.code()),
            message: s.message().to_string(),
        })?;
    Ok(resp.into_inner())
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn get_messages_on_channel(
    channel: &Channel,
    token: &[u8],
    req: GetMessagesRequest,
) -> Result<GetMessagesResponse, TinkleError> {
    let bearer = bearer(token)?;
    let mut client =
        tinkle_schema::proto::grpc::messages_service_client::MessagesServiceClient::new(
            channel.clone(),
        );
    let mut request = tonic::Request::new(req);
    request.metadata_mut().insert("authorization", bearer);
    let resp = client
        .get_messages(request)
        .await
        .map_err(|s| TinkleError::GrpcStatus {
            code: grpc_code(s.code()),
            message: s.message().to_string(),
        })?;
    Ok(resp.into_inner())
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn get_target_changes_grpc(
    endpoint: &str,
    token: &[u8],
    req: GetTargetChangesRequest,
    timeout_ms: u32,
) -> Result<GetTargetChangesResponse, TinkleError> {
    let _ = bearer(token)?;
    let channel = connect_channel(endpoint, timeout_ms).await?;
    get_target_changes_on_channel(&channel, token, req).await
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn get_messages_grpc(
    endpoint: &str,
    token: &[u8],
    req: GetMessagesRequest,
    timeout_ms: u32,
) -> Result<GetMessagesResponse, TinkleError> {
    let _ = bearer(token)?;
    let channel = connect_channel(endpoint, timeout_ms).await?;
    get_messages_on_channel(&channel, token, req).await
}

#[cfg(not(target_arch = "wasm32"))]
macro_rules! files_unary {
    ($on_channel:ident, $grpc_fn:ident, $req_ty:ty, $resp_ty:ty, $method:ident) => {
        pub async fn $on_channel(
            channel: &Channel,
            token: &[u8],
            req: $req_ty,
        ) -> Result<$resp_ty, TinkleError> {
            let bearer = bearer(token)?;
            let mut client =
                tinkle_schema::proto::grpc::files_service_client::FilesServiceClient::new(
                    channel.clone(),
                );
            let mut request = tonic::Request::new(req);
            request.metadata_mut().insert("authorization", bearer);
            let resp = client
                .$method(request)
                .await
                .map_err(|s| TinkleError::GrpcStatus {
                    code: grpc_code(s.code()),
                    message: s.message().to_string(),
                })?;
            Ok(resp.into_inner())
        }

        pub async fn $grpc_fn(
            endpoint: &str,
            token: &[u8],
            req: $req_ty,
            timeout_ms: u32,
        ) -> Result<$resp_ty, TinkleError> {
            let _ = bearer(token)?;
            let channel = connect_channel(endpoint, timeout_ms).await?;
            $on_channel(&channel, token, req).await
        }
    };
}

#[cfg(not(target_arch = "wasm32"))]
files_unary!(
    begin_upload_on_channel,
    begin_upload_grpc,
    BeginUploadRequest,
    BeginUploadResponse,
    begin_upload
);

#[cfg(not(target_arch = "wasm32"))]
files_unary!(
    upload_part_on_channel,
    upload_part_grpc,
    UploadPartRequest,
    UploadPartResponse,
    upload_part
);

#[cfg(not(target_arch = "wasm32"))]
files_unary!(
    get_upload_status_on_channel,
    get_upload_status_grpc,
    GetUploadStatusRequest,
    GetUploadStatusResponse,
    get_upload_status
);

#[cfg(not(target_arch = "wasm32"))]
files_unary!(
    complete_upload_on_channel,
    complete_upload_grpc,
    CompleteUploadRequest,
    CompleteUploadResponse,
    complete_upload
);

#[cfg(not(target_arch = "wasm32"))]
files_unary!(
    abort_upload_on_channel,
    abort_upload_grpc,
    AbortUploadRequest,
    AbortUploadResponse,
    abort_upload
);

#[cfg(not(target_arch = "wasm32"))]
files_unary!(
    get_file_info_on_channel,
    get_file_info_grpc,
    GetFileInfoRequest,
    GetFileInfoResponse,
    get_file_info
);

#[cfg(not(target_arch = "wasm32"))]
files_unary!(
    download_part_on_channel,
    download_part_grpc,
    DownloadPartRequest,
    DownloadPartResponse,
    download_part
);

#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
pub async fn get_target_changes_grpc(
    _endpoint: &str,
    _token: &[u8],
    _req: tinkle_schema::proto::GetTargetChangesRequest,
    _timeout_ms: u32,
) -> Result<tinkle_schema::proto::GetTargetChangesResponse, crate::errors::TinkleError> {
    Err(crate::errors::TinkleError::Grpc(
        "gRPC unavailable on wasm; use Syntra transport".into(),
    ))
}

#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
pub async fn get_messages_grpc(
    _endpoint: &str,
    _token: &[u8],
    _req: tinkle_schema::proto::GetMessagesRequest,
    _timeout_ms: u32,
) -> Result<tinkle_schema::proto::GetMessagesResponse, crate::errors::TinkleError> {
    Err(crate::errors::TinkleError::Grpc(
        "gRPC unavailable on wasm; use Syntra transport".into(),
    ))
}

#[cfg(target_arch = "wasm32")]
macro_rules! files_unavailable {
    ($grpc_fn:ident, $req_ty:ty, $resp_ty:ty) => {
        #[allow(dead_code)]
        pub async fn $grpc_fn(
            _endpoint: &str,
            _token: &[u8],
            _req: $req_ty,
            _timeout_ms: u32,
        ) -> Result<$resp_ty, crate::errors::TinkleError> {
            Err(crate::errors::TinkleError::Grpc(
                "gRPC unavailable on wasm; use Syntra transport".into(),
            ))
        }
    };
}

#[cfg(target_arch = "wasm32")]
files_unavailable!(
    begin_upload_grpc,
    tinkle_schema::proto::BeginUploadRequest,
    tinkle_schema::proto::BeginUploadResponse
);

#[cfg(target_arch = "wasm32")]
files_unavailable!(
    upload_part_grpc,
    tinkle_schema::proto::UploadPartRequest,
    tinkle_schema::proto::UploadPartResponse
);

#[cfg(target_arch = "wasm32")]
files_unavailable!(
    get_upload_status_grpc,
    tinkle_schema::proto::GetUploadStatusRequest,
    tinkle_schema::proto::GetUploadStatusResponse
);

#[cfg(target_arch = "wasm32")]
files_unavailable!(
    complete_upload_grpc,
    tinkle_schema::proto::CompleteUploadRequest,
    tinkle_schema::proto::CompleteUploadResponse
);

#[cfg(target_arch = "wasm32")]
files_unavailable!(
    abort_upload_grpc,
    tinkle_schema::proto::AbortUploadRequest,
    tinkle_schema::proto::AbortUploadResponse
);

#[cfg(target_arch = "wasm32")]
files_unavailable!(
    get_file_info_grpc,
    tinkle_schema::proto::GetFileInfoRequest,
    tinkle_schema::proto::GetFileInfoResponse
);

#[cfg(target_arch = "wasm32")]
files_unavailable!(
    download_part_grpc,
    tinkle_schema::proto::DownloadPartRequest,
    tinkle_schema::proto::DownloadPartResponse
);

#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
pub(crate) fn bearer(_token: &[u8]) -> Result<(), crate::errors::TinkleError> {
    Err(crate::errors::TinkleError::Grpc(
        "gRPC unavailable on wasm; use Syntra transport".into(),
    ))
}
