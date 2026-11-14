//! Durable sending adapters. Request bytes are decoded to concrete types before enqueue.
use crate::{FfiError, TinkleClient};
use libtinkle::sending::{OutgoingJob, OutgoingState};
use tinkle_schema::proto as p;

/// Retained state, independent of whether the foreign waiter remains alive.
#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum FfiOutgoingState {
    Prepared,
    Dispatching,
    Unconfirmed,
    Accepted,
    Rejected,
    Transient,
    AcceptedPending,
}

/// Retained keyed request/result. Response is the named method's protobuf only when Accepted.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiOutgoingJob {
    pub job_id: String,
    pub target_key: String,
    pub method: String,
    pub request: Vec<u8>,
    pub state: FfiOutgoingState,
    pub response: Vec<u8>,
    pub last_outcome: Option<FfiOutgoingOutcome>,
    pub revision: i64,
}

/// Last observed failure; it does not authorize replay or establish rollback.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiOutgoingOutcome {
    pub code: i32,
    pub raw_code: Option<i32>,
    pub unknown_outcome: bool,
}
impl From<OutgoingJob> for FfiOutgoingJob {
    fn from(j: OutgoingJob) -> Self {
        Self {
            job_id: j.job_id,
            target_key: j.target_key,
            method: j.method,
            request: j.payload,
            response: j.response,
            last_outcome: j.last_outcome.map(|o| FfiOutgoingOutcome {
                code: o.code,
                raw_code: o.raw_code,
                unknown_outcome: o.unknown_outcome,
            }),
            revision: j.revision,
            state: match j.state {
                OutgoingState::Prepared => FfiOutgoingState::Prepared,
                OutgoingState::Dispatching => FfiOutgoingState::Dispatching,
                OutgoingState::Unconfirmed => FfiOutgoingState::Unconfirmed,
                OutgoingState::Accepted => FfiOutgoingState::Accepted,
                OutgoingState::Rejected => FfiOutgoingState::Rejected,
                OutgoingState::Transient => FfiOutgoingState::Transient,
                OutgoingState::AcceptedPending => FfiOutgoingState::AcceptedPending,
            },
        }
    }
}

/// Lightweight inventory entry; list operations do not copy all message bodies/results across FFI.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiOutgoingJobSummary {
    pub job_id: String,
    pub target_key: String,
    pub method: String,
    pub state: FfiOutgoingState,
    pub revision: i64,
}

impl From<libtinkle::sending::OutgoingJobSummary> for FfiOutgoingJobSummary {
    fn from(j: libtinkle::sending::OutgoingJobSummary) -> Self {
        Self {
            job_id: j.job_id,
            target_key: j.target_key,
            method: j.method,
            revision: j.revision,
            state: match j.state {
                OutgoingState::Prepared => FfiOutgoingState::Prepared,
                OutgoingState::Dispatching => FfiOutgoingState::Dispatching,
                OutgoingState::Unconfirmed => FfiOutgoingState::Unconfirmed,
                OutgoingState::Accepted => FfiOutgoingState::Accepted,
                OutgoingState::Rejected => FfiOutgoingState::Rejected,
                OutgoingState::Transient => FfiOutgoingState::Transient,
                OutgoingState::AcceptedPending => FfiOutgoingState::AcceptedPending,
            },
        }
    }
}

/// Explicit retry selection. Never use a fresh random ID after an uncertain result.
#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum FfiSendRetry {
    Never,
    ExactKeyedRequest,
}

/// Typed authoring/preparation failures, including unsupported non-document media.
#[derive(Debug, Clone, Copy, thiserror::Error, uniffi::Error)]
pub enum FfiMediaError {
    #[error("media exceeds preparation budget")]
    Budget,
    #[error("invalid media")]
    Invalid,
    #[error("media I/O failed")]
    Io,
    #[error("media source changed")]
    SourceChanged,
    #[error("media preparation cancelled")]
    Cancelled,
    #[error("media preparation busy")]
    Busy,
    #[error("unsupported media; choose document explicitly to preserve original bytes")]
    Unsupported,
    #[error("media codec failed")]
    Codec,
}
impl From<libtinkle::media::MediaError> for FfiMediaError {
    fn from(e: libtinkle::media::MediaError) -> Self {
        use libtinkle::media::MediaError as E;
        match e {
            E::Budget => Self::Budget,
            E::Invalid => Self::Invalid,
            E::Io => Self::Io,
            E::SourceChanged => Self::SourceChanged,
            E::Cancelled => Self::Cancelled,
            E::Busy => Self::Busy,
            E::Unsupported => Self::Unsupported,
            E::Codec => Self::Codec,
        }
    }
}

/// Structured coordinator errors; RPC failures carry the ordinary FFI error, never replay advice.
#[derive(Debug, Clone, thiserror::Error, uniffi::Error)]
pub enum FfiSendError {
    #[error("native SQLite required")]
    NoStorage,
    #[error("current principal required")]
    NoPrincipal,
    #[error("sending scope changed")]
    ScopeChanged,
    #[error("sending busy")]
    Busy,
    #[error("outgoing job not found")]
    NotFound,
    #[error("outgoing job conflict")]
    Conflict,
    #[error("outgoing storage failed")]
    Storage,
    #[error("invalid outgoing request")]
    InvalidRequest,
    #[error("secure randomness failed")]
    Random,
    #[error("media preparation failed: {error}")]
    Media { error: FfiMediaError },
    #[error("send RPC failed: {error}")]
    Rpc { error: FfiError },
}
impl From<libtinkle::SendError> for FfiSendError {
    fn from(e: libtinkle::SendError) -> Self {
        use libtinkle::SendError as E;
        match e {
            E::NoStorage => Self::NoStorage,
            E::NoPrincipal => Self::NoPrincipal,
            E::ScopeChanged => Self::ScopeChanged,
            E::Busy => Self::Busy,
            E::NotFound => Self::NotFound,
            E::Conflict => Self::Conflict,
            E::Storage => Self::Storage,
            E::InvalidRequest => Self::InvalidRequest,
            E::Random => Self::Random,
            E::Media(error) => Self::Media {
                error: error.into(),
            },
            E::Rpc(error) => Self::Rpc {
                error: error.into(),
            },
        }
    }
}

/// Check size before protobuf decoding; mobile owns only bounded application payloads here.
fn decode<M: prost::Message + Default>(request: &[u8]) -> Result<M, FfiSendError> {
    if request.len() > 262144 {
        return Err(FfiSendError::InvalidRequest);
    }
    M::decode(request).map_err(|_| FfiSendError::InvalidRequest)
}

#[uniffi::export(async_runtime = "tokio")]
impl TinkleClient {
    /// Persist a SendMessageRequest; zero idempotency_key asks Rust for a stable secure operation key.
    pub async fn sending_enqueue_message(
        &self,
        job_id: String,
        request: Vec<u8>,
    ) -> Result<FfiOutgoingJob, FfiSendError> {
        Ok(self
            .core_client()
            .sending()
            .enqueue(
                job_id,
                libtinkle::OutgoingRequest::Text(decode::<p::SendMessageRequest>(&request)?),
            )
            .await
            .map_err(FfiSendError::from)?
            .into())
    }
    /// Persist a SendMediaRequest containing completed main/variant file IDs.
    pub async fn sending_enqueue_media(
        &self,
        job_id: String,
        request: Vec<u8>,
    ) -> Result<FfiOutgoingJob, FfiSendError> {
        Ok(self
            .core_client()
            .sending()
            .enqueue(
                job_id,
                libtinkle::OutgoingRequest::Media(decode::<p::SendMediaRequest>(&request)?),
            )
            .await
            .map_err(FfiSendError::from)?
            .into())
    }
    /// Persist one atomic SendAlbumRequest with distinct retained per-item keys.
    pub async fn sending_enqueue_album(
        &self,
        job_id: String,
        request: Vec<u8>,
    ) -> Result<FfiOutgoingJob, FfiSendError> {
        Ok(self
            .core_client()
            .sending()
            .enqueue(
                job_id,
                libtinkle::OutgoingRequest::Album(decode::<p::SendAlbumRequest>(&request)?),
            )
            .await
            .map_err(FfiSendError::from)?
            .into())
    }
    /// Attempt/resume a retained job, with explicit permission for uncertain exact-key retries.
    pub async fn sending_resume(
        &self,
        job_id: String,
        retry: FfiSendRetry,
        control: std::sync::Arc<crate::transfers::TransferControl>,
    ) -> Result<FfiOutgoingJob, FfiSendError> {
        let mut lease = control
            .begin(crate::transfers::FfiTransferPhase::Sending)
            .map_err(|error| FfiSendError::Rpc { error })?;
        let retry = match retry {
            FfiSendRetry::Never => libtinkle::SendRetry::Never,
            FfiSendRetry::ExactKeyedRequest => libtinkle::SendRetry::ExactKeyedRequest,
        };
        let result = self
            .core_client()
            .sending()
            .resume(job_id, retry, Some(control.token()))
            .await
            .map_err(FfiSendError::from);
        lease.finish(result.is_ok(), None);
        Ok(result?.into())
    }
    /// Inspect all retained jobs without network activity; accepted results are not replayed.
    pub async fn sending_jobs(&self) -> Result<Vec<FfiOutgoingJobSummary>, FfiSendError> {
        Ok(self
            .core_client()
            .sending()
            .jobs()
            .await
            .map_err(FfiSendError::from)?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Read one retained request/result offline, without replaying a mutation.
    pub async fn sending_get_job(&self, job_id: String) -> Result<FfiOutgoingJob, FfiSendError> {
        Ok(self
            .core_client()
            .sending()
            .get(job_id)
            .await
            .map_err(FfiSendError::from)?
            .into())
    }
    /// Remove only a settled/never-dispatched job. Uncertain jobs remain retained.
    pub async fn sending_discard(&self, job_id: String) -> Result<bool, FfiSendError> {
        self.core_client()
            .sending()
            .discard(job_id)
            .await
            .map_err(Into::into)
    }
}
