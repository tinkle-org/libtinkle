//! Native path APIs and explicit one-use cancellation/progress owners. Large files stay on disk.
use crate::sending::{FfiMediaError, FfiSendError};
use crate::{FfiError, TinkleClient};
use prost::Message as _;
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

/// Decode exactly the caller's reference. Empty/malformed contexts never become uploader reads.
fn decode_file_context(bytes: &[u8]) -> Result<libtinkle::FileAccessContext, FfiError> {
    if bytes.is_empty() || bytes.len() > 4096 {
        return Err(FfiError::Codec("invalid file access context".into()));
    }
    let context = libtinkle::FileAccessContext::decode(bytes)
        .map_err(|_| FfiError::Codec("invalid file access context".into()))?;
    use tinkle_schema::proto::file_access_context::Reference;
    let valid = match context.reference.as_ref() {
        Some(Reference::Message(m)) => {
            m.message_id > 0
                && matches!(
                    m.target.as_ref().and_then(|t| t.kind.as_ref()),
                    Some(
                        tinkle_schema::proto::target::Kind::UserId(1..)
                            | tinkle_schema::proto::target::Kind::ChatId(1..)
                    )
                )
        }
        Some(Reference::ProfilePhoto(p)) => p.user_id > 0 && p.photo_id > 0,
        Some(Reference::StickerAsset(a)) => a.asset_id > 0 && a.asset_version > 0,
        Some(Reference::ChatPhoto(p)) => p.chat_id > 0 && p.photo_id > 0,
        None => false,
    };
    if !valid {
        return Err(FfiError::Codec("invalid file access context".into()));
    }
    Ok(context)
}

/// Local operation phase, not proof that a remote mutation rolled back or a message was delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum FfiTransferPhase {
    Ready,
    Preparing,
    Uploading,
    Downloading,
    Sending,
    Complete,
    Failed,
    Interrupted,
}

/// Honest progress snapshot. Byte counts are absent until known, never estimated from elapsed time.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiTransferProgress {
    pub phase: FfiTransferPhase,
    pub cancellation_requested: bool,
    pub completed_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
}

/// A one-use explicit cancellation channel. Cancelling a download affects only this consumer.
/// Keep this handle while awaiting an operation; cancel() stops local work, not remote effects.
#[derive(uniffi::Object)]
pub struct TransferControl {
    token: libtinkle::CancelToken,
    used: AtomicBool,
    progress: Mutex<FfiTransferProgress>,
}
#[uniffi::export]
impl TransferControl {
    /// Allocate a fresh cancellation/progress owner for one path operation or sending attempt.
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            token: Default::default(),
            used: AtomicBool::new(false),
            progress: Mutex::new(FfiTransferProgress {
                phase: FfiTransferPhase::Ready,
                cancellation_requested: false,
                completed_bytes: None,
                total_bytes: None,
            }),
        })
    }
    /// Request local cancellation. A completion already committed may still win the race.
    pub fn cancel(&self) {
        self.token.cancel();
    }
    /// Read current phase/counts without a callback retained across a foreign runtime lifetime.
    pub fn progress(&self) -> FfiTransferProgress {
        let mut p = self
            .progress
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        p.cancellation_requested = self.token.is_cancelled();
        p
    }
}
impl TransferControl {
    pub(crate) fn token(&self) -> libtinkle::CancelToken {
        self.token.clone()
    }
    /// Single-flight and single-use: a cancelled owner cannot accidentally cancel a later operation.
    pub(crate) fn begin(
        self: &Arc<Self>,
        phase: FfiTransferPhase,
    ) -> Result<TransferLease, FfiError> {
        if self.token.is_cancelled() {
            return Err(FfiError::Cancelled);
        }
        if self.used.swap(true, Ordering::AcqRel) {
            return Err(FfiError::Codec("transfer control already used".into()));
        }
        self.progress
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .phase = phase;
        Ok(TransferLease {
            owner: self.clone(),
            finished: false,
        })
    }
}

/// A dropped waiter requests cooperative local cancellation and records an uncertain interruption.
pub(crate) struct TransferLease {
    owner: Arc<TransferControl>,
    finished: bool,
}
impl TransferLease {
    pub(crate) fn finish(&mut self, success: bool, size: Option<u64>) {
        let mut p = self
            .owner
            .progress
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        p.phase = if success {
            FfiTransferPhase::Complete
        } else {
            FfiTransferPhase::Failed
        };
        if success {
            p.completed_bytes = size;
            p.total_bytes = size;
        }
        self.finished = true;
    }
}
impl Drop for TransferLease {
    fn drop(&mut self) {
        if !self.finished {
            self.owner.cancel();
            self.owner
                .progress
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .phase = FfiTransferPhase::Interrupted;
        }
    }
}

/// Exact upload declaration. Digest absence/presence is preserved by the core upload manager.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiUploadOptions {
    pub idempotency_key: String,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub sha256: Option<Vec<u8>>,
}
impl From<FfiUploadOptions> for libtinkle::UploadParams {
    fn from(v: FfiUploadOptions) -> Self {
        Self {
            idempotency_key: v.idempotency_key,
            file_name: v.file_name,
            mime_type: v.mime_type,
            sha256: v.sha256,
        }
    }
}

/// Explicit preparation intent. Video without a qualified native codec is Unsupported, not a document.
#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum FfiMediaKind {
    Document,
    Image,
    Voice {
        sample_rate: u32,
        channels: u8,
        bits_per_sample: u8,
    },
    Video,
    Audio,
}
impl From<FfiMediaKind> for libtinkle::sending::AttachmentKind {
    fn from(v: FfiMediaKind) -> Self {
        match v {
            FfiMediaKind::Document => Self::Document,
            FfiMediaKind::Image => Self::Native(libtinkle::media::NativeMediaKind::Image),
            FfiMediaKind::Video => Self::Native(libtinkle::media::NativeMediaKind::Video),
            FfiMediaKind::Audio => Self::Native(libtinkle::media::NativeMediaKind::Audio),
            FfiMediaKind::Voice {
                sample_rate,
                channels,
                bits_per_sample,
            } => Self::Native(libtinkle::media::NativeMediaKind::Voice(
                libtinkle::media::PcmFormat {
                    sample_rate,
                    channels,
                    bits_per_sample,
                },
            )),
        }
    }
}

/// Completed main/thumbnail metadata and MediaSource protobuf; never full-size media bytes.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiPreparedAttachment {
    pub file_info: Vec<u8>,
    pub thumbnail_file_info: Option<Vec<u8>>,
    pub media_source: Vec<u8>,
    pub source_sha256: Vec<u8>,
    pub output_sha256: Vec<u8>,
    pub preparation_profile: String,
}

/// Durable main-only handoff; paths are SDK-owned and cleared with the principal.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiAttachmentManifest {
    pub input_path: String,
    pub settings: String,
    pub artifact_path: String,
    pub source_sha256: Vec<u8>,
    pub output_sha256: Vec<u8>,
    pub preparation_profile: String,
    pub mime_type: String,
    pub media_source: Vec<u8>,
    pub native_facts: Option<crate::video::FfiNativeMediaOutput>,
}

/// Durable transfer observations. Download prefix bytes are diagnostic until server metadata
/// and digest verification complete. Upload managers currently report phase, not per-part bytes.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiTransferCheckpoint {
    pub job_id: String,
    pub kind: String,
    pub file_id: i64,
    pub state: String,
    pub retained_prefix_bytes: i64,
    /// Confirmed upload part count; not bytes and not a server resume authority.
    pub uploaded_parts: Option<u64>,
}

#[uniffi::export(async_runtime = "tokio")]
impl TinkleClient {
    /// Install the real host encoder for normal attachment preparation; never capture this client
    /// inside the callback. Clearing it does not abort native work already retained by an invocation.
    pub fn configure_native_media_preparer(
        &self,
        preparer: Arc<dyn crate::video::FfiPlatformMediaPreparer>,
    ) -> Result<(), FfiError> {
        self.core_client()
            .configure_media_preparer(Some(Arc::new(ConfiguredPreparer(preparer))))
            .map_err(Into::into)
    }
    /// Release the configured callback. Retained manifests remain resumable without an encoder.
    pub fn clear_native_media_preparer(&self) -> Result<(), FfiError> {
        self.core_client()
            .configure_media_preparer(None)
            .map_err(Into::into)
    }
    /// Read retained preparation identity without starting a codec or network request.
    pub async fn sending_attachment_manifest(
        &self,
        key: String,
    ) -> Result<Option<FfiAttachmentManifest>, FfiSendError> {
        Ok(self
            .core_client()
            .sending()
            .attachment_manifest(key)
            .await
            .map_err(FfiSendError::from)?
            .map(|m| FfiAttachmentManifest {
                input_path: m.input_path,
                settings: m.settings,
                artifact_path: m.artifact_path,
                source_sha256: m.source_sha256.to_vec(),
                output_sha256: m.output_sha256.to_vec(),
                preparation_profile: m.profile,
                mime_type: m.mime_type,
                media_source: m.media_source,
                native_facts: m.native_facts.as_ref().map(Into::into),
            }))
    }
    /// Download at most the core's bounded byte-convenience limit using only this exact context.
    pub async fn files_download_bytes_with_context(
        &self,
        file_id: i64,
        context: Vec<u8>,
        control: Arc<TransferControl>,
    ) -> Result<Vec<u8>, FfiError> {
        let context = decode_file_context(&context)?;
        let mut lease = control.begin(FfiTransferPhase::Downloading)?;
        let result = self
            .core_client()
            .download_manager()
            .download_bytes_with_context(file_id, context, Some(control.token()))
            .await
            .map_err(FfiError::from);
        lease.finish(result.is_ok(), result.as_ref().ok().map(|v| v.len() as u64));
        result
    }

    /// Stream to disk under exactly the encoded FileAccessContext; denial never retries as uploader.
    pub async fn files_download_to_path_with_context(
        &self,
        file_id: i64,
        path: String,
        context: Vec<u8>,
        cache_directory: Option<String>,
        control: Arc<TransferControl>,
    ) -> Result<String, FfiError> {
        let context = decode_file_context(&context)?;
        let mut lease = control.begin(FfiTransferPhase::Downloading)?;
        let mut manager = self.core_client().download_manager();
        if let Some(dir) = cache_directory {
            manager = manager.with_cache_dir(dir.into());
        }
        let result = manager
            .download_to_path_with_context(
                file_id,
                Path::new(&path),
                context,
                Some(control.token()),
            )
            .await
            .map_err(FfiError::from);
        let size = if let Ok(path) = &result {
            tokio::fs::metadata(path).await.ok().map(|v| v.len())
        } else {
            None
        };
        lease.finish(result.is_ok(), size);
        result.map(|p| p.to_string_lossy().into_owned())
    }
    /// Stream a native path through the Rust snapshot/resume upload pipeline; returns FileInfo bytes.
    pub async fn files_upload_path(
        &self,
        path: String,
        options: FfiUploadOptions,
        control: Arc<TransferControl>,
    ) -> Result<Vec<u8>, FfiError> {
        let mut lease = control.begin(FfiTransferPhase::Uploading)?;
        let result = self
            .core_client()
            .upload_manager()
            .upload_path(options.into(), Path::new(&path), Some(control.token()))
            .await
            .map_err(FfiError::from);
        lease.finish(
            result.is_ok(),
            result
                .as_ref()
                .ok()
                .and_then(|f| u64::try_from(f.size_bytes).ok()),
        );
        Ok(result?.encode_to_vec())
    }
    /// Verify a streamed download and atomically publish the destination. No file-sized FFI buffer.
    pub async fn files_download_to_path(
        &self,
        file_id: i64,
        path: String,
        cache_directory: Option<String>,
        control: Arc<TransferControl>,
    ) -> Result<String, FfiError> {
        let mut lease = control.begin(FfiTransferPhase::Downloading)?;
        let mut manager = self.core_client().download_manager();
        if let Some(dir) = cache_directory {
            manager = manager.with_cache_dir(dir.into());
        }
        let result = manager
            .download_to_path(file_id, Path::new(&path), Some(control.token()))
            .await
            .map_err(FfiError::from);
        let size = if let Ok(path) = &result {
            tokio::fs::metadata(path).await.ok().map(|m| m.len())
        } else {
            None
        };
        lease.finish(result.is_ok(), size);
        result.map(|p| p.to_string_lossy().into_owned())
    }
    /// Prepare real native input and upload completed main/thumbnail files for a typed media send.
    pub async fn sending_prepare_attachment(
        &self,
        key: String,
        path: String,
        kind: FfiMediaKind,
        control: Arc<TransferControl>,
    ) -> Result<FfiPreparedAttachment, FfiSendError> {
        let mut lease = control
            .begin(FfiTransferPhase::Preparing)
            .map_err(|error| FfiSendError::Rpc { error })?;
        let result = self
            .core_client()
            .sending()
            .prepare_attachment(
                key,
                Path::new(&path),
                libtinkle::sending::AttachmentKind::from(kind),
                Some(control.token()),
            )
            .await
            .map_err(FfiSendError::from);
        lease.finish(
            result.is_ok(),
            result
                .as_ref()
                .ok()
                .and_then(|v| u64::try_from(v.file.size_bytes).ok()),
        );
        let v = result?;
        Ok(FfiPreparedAttachment {
            file_info: v.file.encode_to_vec(),
            thumbnail_file_info: v.thumbnail.map(|f| f.encode_to_vec()),
            media_source: v.source.encode_to_vec(),
            source_sha256: v.source_sha256.to_vec(),
            output_sha256: v.output_sha256.to_vec(),
            preparation_profile: v.preparation_profile,
        })
    }

    /// Persist a real native encoder's main-only output before upload. Retries use the retained
    /// artifact without invoking the callback; no client-wide callback retention is needed.
    pub async fn sending_prepare_native_attachment(
        &self,
        key: String,
        path: String,
        kind: crate::video::FfiNativeMediaKind,
        preparer: Arc<dyn crate::video::FfiPlatformMediaPreparer>,
        control: Arc<TransferControl>,
    ) -> Result<FfiPreparedAttachment, FfiSendError> {
        let mut lease = control
            .begin(FfiTransferPhase::Preparing)
            .map_err(|error| FfiSendError::Rpc { error })?;
        let result = self
            .core_client()
            .sending()
            .prepare_native_attachment(
                key,
                Path::new(&path),
                kind.into(),
                Arc::new(crate::video::NativeMediaPreparerAdapter(preparer, kind)),
                Some(control.token()),
            )
            .await
            .map_err(FfiSendError::from);
        lease.finish(
            result.is_ok(),
            result
                .as_ref()
                .ok()
                .and_then(|v| u64::try_from(v.file.size_bytes).ok()),
        );
        let v = result?;
        Ok(FfiPreparedAttachment {
            file_info: v.file.encode_to_vec(),
            thumbnail_file_info: None,
            media_source: v.source.encode_to_vec(),
            source_sha256: v.source_sha256.to_vec(),
            output_sha256: v.output_sha256.to_vec(),
            preparation_profile: v.preparation_profile,
        })
    }

    /// Adopt the exact retained native artifact, verifying its copied hash and persisting its
    /// recipe/facts under this account and trust before uploading. No codec callback is invoked.
    pub async fn sending_adopt_native_attachment(
        &self,
        key: String,
        source_path: String,
        prepared: Arc<crate::video::FfiPreparedNativeMedia>,
        control: Arc<TransferControl>,
    ) -> Result<FfiPreparedAttachment, FfiSendError> {
        let mut lease = control
            .begin(FfiTransferPhase::Preparing)
            .map_err(|error| FfiSendError::Rpc { error })?;
        let result = self
            .core_client()
            .sending()
            .adopt_native_attachment(
                key,
                Path::new(&source_path),
                prepared.retained_native_media(),
                Some(control.token()),
            )
            .await
            .map_err(FfiSendError::from);
        lease.finish(
            result.is_ok(),
            result
                .as_ref()
                .ok()
                .and_then(|v| u64::try_from(v.file.size_bytes).ok()),
        );
        let v = result?;
        Ok(FfiPreparedAttachment {
            file_info: v.file.encode_to_vec(),
            thumbnail_file_info: None,
            media_source: v.source.encode_to_vec(),
            source_sha256: v.source_sha256.to_vec(),
            output_sha256: v.output_sha256.to_vec(),
            preparation_profile: v.preparation_profile,
        })
    }
    /// Read one durable transfer observation on a blocking worker, fenced to this principal.
    pub async fn files_transfer_checkpoint(
        &self,
        job_id: String,
    ) -> Result<Option<FfiTransferCheckpoint>, FfiSendError> {
        let job = self
            .core_client()
            .file_transfer_checkpoint(job_id)
            .await
            .map_err(FfiSendError::from)?;
        Ok(job.map(|j| FfiTransferCheckpoint {
            job_id: j.job_id,
            kind: j.kind.clone(),
            file_id: j.file_id,
            state: j.state,
            retained_prefix_bytes: if j.kind == "download" {
                j.resume_offset
            } else {
                0
            },
            uploaded_parts: if j.kind == "upload" {
                u64::try_from(j.resume_offset).ok()
            } else {
                None
            },
        }))
    }
}

/// Configured callback uses the invocation's purpose, not a purpose captured by a prior call.
struct ConfiguredPreparer(Arc<dyn crate::video::FfiPlatformMediaPreparer>);
#[async_trait::async_trait]
impl libtinkle::media::PlatformMediaPreparer for ConfiguredPreparer {
    async fn prepare(
        &self,
        input: Arc<libtinkle::media::NativeMediaInput>,
    ) -> Result<libtinkle::media::NativeMediaOutput, libtinkle::media::MediaError> {
        use crate::video::FfiNativeMediaKind as F;
        use libtinkle::media::NativeMediaKind as N;
        let kind = match input.kind {
            N::Image => F::Image,
            N::Video => F::Video,
            N::Audio => F::Audio,
            N::Voice(f) => F::Voice {
                sample_rate: f.sample_rate,
                channels: f.channels,
                bits_per_sample: f.bits_per_sample,
            },
        };
        libtinkle::media::PlatformMediaPreparer::prepare(
            &crate::video::NativeMediaPreparerAdapter(self.0.clone(), kind),
            input,
        )
        .await
    }
}

/// Keep unsupported media typed at the foreign boundary rather than flattening it to an RPC error.
impl From<FfiMediaError> for FfiSendError {
    fn from(error: FfiMediaError) -> Self {
        Self::Media { error }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Malformed/empty references fail at the FFI boundary and never become uploader reads.
    #[test]
    fn exact_file_context_decode_has_no_owner_fallback() {
        assert!(decode_file_context(&[]).is_err());
        assert!(decode_file_context(&[255]).is_err());
        let context = libtinkle::FileAccessContext {
            reference: Some(
                tinkle_schema::proto::file_access_context::Reference::StickerAsset(
                    tinkle_schema::proto::StickerAssetReference {
                        asset_id: 7,
                        asset_version: 3,
                    },
                ),
            ),
        };
        assert_eq!(
            decode_file_context(&context.encode_to_vec()).unwrap(),
            context
        );
        let invalid = libtinkle::FileAccessContext {
            reference: Some(
                tinkle_schema::proto::file_access_context::Reference::StickerAsset(
                    tinkle_schema::proto::StickerAssetReference {
                        asset_id: 7,
                        asset_version: 0,
                    },
                ),
            ),
        };
        assert!(decode_file_context(&invalid.encode_to_vec()).is_err());
        for (chat_id, photo_id) in [(7, 3), (0, 3), (-1, 3), (7, 0), (7, -1)] {
            let context = libtinkle::FileAccessContext {
                reference: Some(
                    tinkle_schema::proto::file_access_context::Reference::ChatPhoto(
                        tinkle_schema::proto::ChatPhotoReference { chat_id, photo_id },
                    ),
                ),
            };
            assert_eq!(
                decode_file_context(&context.encode_to_vec()).is_ok(),
                chat_id > 0 && photo_id > 0
            );
        }
    }

    /// Cancellation is an explicit monotonic per-consumer channel; counts remain unknown.
    #[test]
    fn cancellation_and_progress_are_one_use_and_truthful() {
        let control = TransferControl::new();
        let lease = control.begin(FfiTransferPhase::Downloading).unwrap();
        let snapshot = control.progress();
        assert_eq!(snapshot.phase, FfiTransferPhase::Downloading);
        assert_eq!(snapshot.completed_bytes, None);
        assert!(control.begin(FfiTransferPhase::Uploading).is_err());
        control.cancel();
        assert!(control.token().is_cancelled());
        drop(lease);
        assert_eq!(control.progress().phase, FfiTransferPhase::Interrupted);
        assert!(control.progress().cancellation_requested);
        assert!(matches!(
            control.begin(FfiTransferPhase::Uploading),
            Err(FfiError::Cancelled)
        ));
    }

    /// A completion can win a concurrent cancellation, without falsely promising rollback.
    #[test]
    fn committed_completion_keeps_actual_size() {
        let control = TransferControl::new();
        let mut lease = control.begin(FfiTransferPhase::Uploading).unwrap();
        control.cancel();
        lease.finish(true, Some(123));
        drop(lease);
        let snapshot = control.progress();
        assert_eq!(snapshot.phase, FfiTransferPhase::Complete);
        assert_eq!(snapshot.completed_bytes, Some(123));
        assert!(snapshot.cancellation_requested);
    }
}
