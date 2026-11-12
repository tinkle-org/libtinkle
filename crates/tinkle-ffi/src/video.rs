//! Retained native video read leases and the narrow asynchronous platform decoder bridge.
use crate::{FfiError, FfiMediaError, TinkleClient};
use libtinkle::media as m;
use std::sync::Arc;

/// Local processing purpose, independent of a filename, MIME guess or upload intent.
#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum FfiNativeMediaKind {
    Image,
    Video,
    Audio,
    Voice {
        sample_rate: u32,
        channels: u8,
        bits_per_sample: u8,
    },
}
impl From<FfiNativeMediaKind> for m::NativeMediaKind {
    fn from(kind: FfiNativeMediaKind) -> Self {
        match kind {
            FfiNativeMediaKind::Image => Self::Image,
            FfiNativeMediaKind::Video => Self::Video,
            FfiNativeMediaKind::Audio => Self::Audio,
            FfiNativeMediaKind::Voice {
                sample_rate,
                channels,
                bits_per_sample,
            } => Self::Voice(m::PcmFormat {
                sample_rate,
                channels,
                bits_per_sample,
            }),
        }
    }
}

/// SDK-owned source/destination lease. Retain through encoder finalization AND teardown.
#[derive(uniffi::Object)]
pub struct FfiNativeMediaInput {
    inner: Arc<m::NativeMediaInput>,
    kind: FfiNativeMediaKind,
}
#[uniffi::export]
impl FfiNativeMediaInput {
    pub fn source_path(&self) -> String {
        self.inner.source_path().to_string_lossy().into_owned()
    }
    pub fn output_path(&self) -> String {
        self.inner.output_path().to_string_lossy().into_owned()
    }
    pub fn size_bytes(&self) -> u64 {
        self.inner.size_bytes
    }
    pub fn kind(&self) -> FfiNativeMediaKind {
        self.kind
    }
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
    pub fn max_output_bytes(&self) -> u64 {
        if matches!(self.kind, FfiNativeMediaKind::Image) {
            m::IMAGE_MAIN_BYTES
        } else {
            self.inner.limits.max_output_bytes
        }
    }
    pub fn max_duration_ms(&self) -> u64 {
        self.inner.limits.max_duration_ms
    }
    pub fn max_frames(&self) -> u32 {
        self.inner.limits.max_frames
    }
    pub fn deadline_ms(&self) -> u32 {
        self.inner.limits.deadline_ms
    }
}

/// Shared fitting result, no platform-specific resizing policy.
#[derive(Debug, Clone, Copy, uniffi::Record)]
pub struct FfiMediaDimensions {
    pub width: u32,
    pub height: u32,
}
#[uniffi::export]
pub fn media_video_dimensions(
    width: u32,
    height: u32,
) -> Result<FfiMediaDimensions, FfiMediaError> {
    let (width, height) = m::video_output_dimensions(width, height)?;
    Ok(FfiMediaDimensions { width, height })
}
#[uniffi::export]
pub fn media_image_dimensions(
    width: u32,
    height: u32,
    attempt: u32,
) -> Result<FfiMediaDimensions, FfiMediaError> {
    let (width, height) = m::image_output_dimensions(width, height, attempt)?;
    Ok(FfiMediaDimensions { width, height })
}

/// Small measured facts for the exact closed output; no arbitrary output path or media buffer.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiNativeMediaOutput {
    pub width: u32,
    pub height: u32,
    pub frame_count: u32,
    pub duration_ms: u64,
    pub audio_channels: u8,
    pub audio_sample_rate: u32,
    pub audio_samples: u64,
}
impl From<FfiNativeMediaOutput> for m::NativeMediaOutput {
    fn from(f: FfiNativeMediaOutput) -> Self {
        Self {
            width: f.width,
            height: f.height,
            frame_count: f.frame_count,
            duration_ms: f.duration_ms,
            audio_channels: f.audio_channels,
            audio_sample_rate: f.audio_sample_rate,
            audio_samples: f.audio_samples,
            waveform: Vec::new(),
        }
    }
}
impl From<&m::NativeMediaOutput> for FfiNativeMediaOutput {
    fn from(f: &m::NativeMediaOutput) -> Self {
        Self {
            width: f.width,
            height: f.height,
            frame_count: f.frame_count,
            duration_ms: f.duration_ms,
            audio_channels: f.audio_channels,
            audio_sample_rate: f.audio_sample_rate,
            audio_samples: f.audio_samples,
        }
    }
}

/// Real local encoder. Completion must join all native work, even after cooperative cancellation.
/// Only the SDK destination may be written; decoder-only adapters cannot implement this contract.
#[uniffi::export(rust, foreign)]
#[async_trait::async_trait]
pub trait FfiPlatformMediaPreparer: Send + Sync {
    async fn prepare(
        &self,
        input: Arc<FfiNativeMediaInput>,
    ) -> Result<FfiNativeMediaOutput, FfiMediaError>;
}

/// Kept public within the crate for the sending owner's configuration integration.
pub(crate) struct NativeMediaPreparerAdapter(
    pub Arc<dyn FfiPlatformMediaPreparer>,
    pub FfiNativeMediaKind,
);
#[async_trait::async_trait]
impl m::PlatformMediaPreparer for NativeMediaPreparerAdapter {
    async fn prepare(
        &self,
        input: Arc<m::NativeMediaInput>,
    ) -> Result<m::NativeMediaOutput, m::MediaError> {
        self.0
            .prepare(Arc::new(FfiNativeMediaInput {
                inner: input,
                kind: self.1,
            }))
            .await
            .map(Into::into)
            .map_err(Into::into)
    }
}

/// Stable owned result. Path lifetime is this object; upload/adoption must retain it until copied.
#[derive(uniffi::Object)]
pub struct FfiPreparedNativeMedia {
    inner: m::NativePreparedMedia,
}
impl FfiPreparedNativeMedia {
    /// Rust-only adoption handoff. Retains the frozen file, hashes, recipe and facts without
    /// copying file-sized buffers or invoking the encoder again. The adopter must verify the
    /// output hash and bind/persist it atomically under its captured authenticated principal.
    pub fn retained_native_media(&self) -> m::NativePreparedMedia {
        self.inner.clone()
    }
}
#[uniffi::export]
impl FfiPreparedNativeMedia {
    pub fn path(&self) -> String {
        self.inner.main.path().to_string_lossy().into_owned()
    }
    pub fn source_sha256(&self) -> Vec<u8> {
        self.inner.main.source_sha256.to_vec()
    }
    pub fn output_sha256(&self) -> Vec<u8> {
        self.inner.main.output_sha256.to_vec()
    }
    pub fn recipe(&self) -> String {
        self.inner.main.profile.into()
    }
    pub fn mime_type(&self) -> String {
        self.inner.main.mime_type.into()
    }
    pub fn facts(&self) -> FfiNativeMediaOutput {
        (&self.inner.output).into()
    }
    pub fn waveform(&self) -> Vec<u8> {
        self.inner.output.waveform.clone()
    }
}

/// LOCAL only; does not upload, create thumbnails for publication, or persist a send intent.
#[uniffi::export(async_runtime = "tokio")]
pub async fn prepare_native_media(
    path: String,
    kind: FfiNativeMediaKind,
    preparer: Arc<dyn FfiPlatformMediaPreparer>,
) -> Result<Arc<FfiPreparedNativeMedia>, FfiMediaError> {
    let inner = m::prepare_native_media(
        std::path::Path::new(&path),
        kind.into(),
        Arc::new(NativeMediaPreparerAdapter(preparer, kind)),
        None,
    )
    .await?;
    Ok(Arc::new(FfiPreparedNativeMedia { inner }))
}

/// The fixed limits of this invocation, not a host-selected expansion of SDK budgets.
#[derive(Debug, Clone, Copy, uniffi::Record)]
pub struct FfiVideoDecodeLimits {
    pub max_input_bytes: u64,
    pub max_pixels: u64,
    pub max_frames: u32,
    pub max_duration_ms: u64,
    pub thumbnail_edge: u32,
    pub deadline_ms: u32,
}
impl From<m::VideoDecodeLimits> for FfiVideoDecodeLimits {
    fn from(v: m::VideoDecodeLimits) -> Self {
        Self {
            max_input_bytes: v.max_input_bytes,
            max_pixels: v.max_pixels,
            max_frames: v.max_frames,
            max_duration_ms: v.max_duration_ms,
            thumbnail_edge: v.thumbnail_edge,
            deadline_ms: v.deadline_ms,
        }
    }
}

/// Opaque, invocation-owned read lease. Retain this object until native codec work actually
/// completes, even after cancellation/timeout. Never modify, move or delete source_path().
/// Retaining the pathname alone does not retain the source file or bounded worker admission.
#[derive(uniffi::Object)]
pub struct FfiVideoInput {
    inner: Arc<m::VideoInput>,
    cancel: libtinkle::CancelToken,
}
#[uniffi::export]
impl FfiVideoInput {
    /// Read-only frozen local input. This is not a caller path and is not a transfer destination.
    pub fn source_path(&self) -> String {
        self.inner.path().to_string_lossy().into_owned()
    }
    /// SHA-256 of exactly the frozen source; common Rust verifies it again after the callback.
    pub fn source_sha256(&self) -> Vec<u8> {
        self.inner.source_sha256.to_vec()
    }
    /// Source length; no full-size video bytes cross this interface.
    pub fn size_bytes(&self) -> u64 {
        self.inner.size_bytes
    }
    /// Cooperative cancellation, including timeout and dropped waiters. Stop/join native work,
    /// but keep this lease until that work really stops reading the source.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

/// Header probing alone is explicitly unqualified and is rejected by common Rust preparation.
#[derive(Debug, Clone, Copy, uniffi::Enum)]
pub enum FfiVideoEvidence {
    HeaderOnly,
    NativeDecoded,
}

/// Bounded display-oriented sRGB RGBA8 thumbnail. Tightly packed rows, no full-size decoded frame.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiDecodedVideoFrame {
    pub width: u32,
    pub height: u32,
    pub rgba: Vec<u8>,
}

/// Actual facts measured from a fully decoded MP4 stream, not inferred from a container header.
/// NativeDecoded requires full-stream validation and a measured frame count. Unsupported color,
/// HDR or alpha handling must return Unsupported (or omit a thumbnail), never silently flatten it.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiDecodedVideo {
    pub width: u32,
    pub height: u32,
    pub duration_ms: u64,
    pub frame_count: u32,
    pub has_alpha: Option<bool>,
    pub bit_depth: Option<u8>,
    pub evidence: FfiVideoEvidence,
    pub thumbnail: Option<FfiDecodedVideoFrame>,
}

/// Implement with AVFoundation or MediaExtractor/MediaCodec on native-owned execution queues.
/// Bound allocations/queues, observe cancellation/deadline, and retain input until actual native
/// completion. Do not remux/re-encode the source or retain a TinkleClient cycle in this callback.
#[uniffi::export(rust, foreign)]
#[async_trait::async_trait]
pub trait FfiPlatformVideoDecoder: Send + Sync {
    /// Decode/validate the frozen MP4 and optionally return one bounded, oriented thumbnail.
    async fn inspect_and_decode(
        &self,
        input: Arc<FfiVideoInput>,
        limits: FfiVideoDecodeLimits,
    ) -> Result<FfiDecodedVideo, FfiMediaError>;
}

/// Keep foreign callback ownership with the selected decoder and each in-flight invocation.
struct VideoDecoderAdapter(Arc<dyn FfiPlatformVideoDecoder>);
#[async_trait::async_trait]
impl m::PlatformVideoDecoder for VideoDecoderAdapter {
    async fn inspect_and_decode(
        &self,
        input: Arc<m::VideoInput>,
        limits: m::VideoDecodeLimits,
        cancel: libtinkle::CancelToken,
    ) -> Result<m::DecodedVideo, m::MediaError> {
        let input = Arc::new(FfiVideoInput {
            inner: input,
            cancel,
        });
        let result = self
            .0
            .inspect_and_decode(input, limits.into())
            .await
            .map_err(m::MediaError::from)?;
        let evidence = match result.evidence {
            FfiVideoEvidence::HeaderOnly => m::MediaEvidence::HeaderOnly,
            FfiVideoEvidence::NativeDecoded => m::MediaEvidence::HostDecoded,
        };
        if let Some(frame) = &result.thumbnail {
            if frame.width > limits.thumbnail_edge
                || frame.height > limits.thumbnail_edge
                || frame.rgba.len() as u64 != u64::from(frame.width) * u64::from(frame.height) * 4
            {
                return Err(m::MediaError::Invalid);
            }
        }
        Ok(m::DecodedVideo {
            facts: m::MediaFacts {
                format: m::EncodedFormat::Mp4,
                width: Some(result.width),
                height: Some(result.height),
                duration_ms: Some(result.duration_ms),
                frame_count: Some(result.frame_count),
                animated: result.frame_count > 1,
                positive_frame_durations: None,
                loop_count: None,
                has_alpha: result.has_alpha,
                bit_depth: result.bit_depth,
                lossless: false,
                evidence,
            },
            thumbnail: result.thumbnail.map(|f| m::DecodedVideoFrame {
                width: f.width,
                height: f.height,
                rgba: f.rgba,
            }),
        })
    }
}

impl From<FfiMediaError> for m::MediaError {
    fn from(e: FfiMediaError) -> Self {
        match e {
            FfiMediaError::Budget => Self::Budget,
            FfiMediaError::Invalid => Self::Invalid,
            FfiMediaError::Io => Self::Io,
            FfiMediaError::SourceChanged => Self::SourceChanged,
            FfiMediaError::Cancelled => Self::Cancelled,
            FfiMediaError::Busy => Self::Busy,
            FfiMediaError::Unsupported => Self::Unsupported,
            FfiMediaError::Codec => Self::Codec,
        }
    }
}

#[uniffi::export]
impl TinkleClient {
    /// Select the native decoder used by sending_prepare_attachment(Video); installation is real,
    /// not a capability flag. Each operation retains its selected callback through completion.
    pub fn configure_video_decoder(
        &self,
        decoder: Arc<dyn FfiPlatformVideoDecoder>,
    ) -> Result<(), FfiError> {
        self.core_client()
            .configure_video_decoder(Some(Arc::new(VideoDecoderAdapter(decoder))))
            .map_err(Into::into)
    }
    /// Remove the configured decoder. Future video preparation returns typed Unsupported;
    /// existing native work keeps its captured callback/read lease until actual completion.
    pub fn clear_video_decoder(&self) -> Result<(), FfiError> {
        self.core_client()
            .configure_video_decoder(None)
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// All bridge preparations share Rust's two retained video-worker slots.
    /// Keep this guard until every callback-owned VideoInput has been dropped.
    static PREPARATIONS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Test source ownership without adding a platform codec or a file-sized FFI buffer.
    struct Source(std::path::PathBuf);
    impl Source {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("tinkle-video-bridge-{}-{id}", std::process::id()));
            std::fs::write(&path, b"synthetic decoder input").unwrap();
            Self(path)
        }
    }
    impl Drop for Source {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    struct Decoder {
        header_only: bool,
        retain: Mutex<Option<Arc<FfiVideoInput>>>,
    }
    #[async_trait::async_trait]
    impl FfiPlatformVideoDecoder for Decoder {
        async fn inspect_and_decode(
            &self,
            input: Arc<FfiVideoInput>,
            limits: FfiVideoDecodeLimits,
        ) -> Result<FfiDecodedVideo, FfiMediaError> {
            assert!(input.size_bytes() <= limits.max_input_bytes);
            assert_eq!(input.source_sha256().len(), 32);
            assert!(!input.is_cancelled());
            *self.retain.lock().unwrap() = Some(input);
            Ok(FfiDecodedVideo {
                width: 1,
                height: 1,
                duration_ms: 100,
                frame_count: 1,
                has_alpha: Some(false),
                bit_depth: Some(8),
                evidence: if self.header_only {
                    FfiVideoEvidence::HeaderOnly
                } else {
                    FfiVideoEvidence::NativeDecoded
                },
                thumbnail: Some(FfiDecodedVideoFrame {
                    width: 1,
                    height: 1,
                    rgba: vec![0, 0, 0, 255],
                }),
            })
        }
    }

    /// A callback's header-only claim cannot become a qualified video or authoring input.
    #[tokio::test]
    async fn bridge_rejects_header_only_and_cancels_retained_input() {
        let _preparation = PREPARATIONS.lock().await;
        let source = Source::new();
        let host = Arc::new(Decoder {
            header_only: true,
            retain: Mutex::new(None),
        });
        let result = m::prepare_video_with_decoder(
            &source.0,
            Arc::new(VideoDecoderAdapter(host.clone())),
            None,
        )
        .await;
        assert!(matches!(result, Err(m::MediaError::Unsupported)));
        let input = host.retain.lock().unwrap().take().unwrap();
        assert!(input.is_cancelled());
        assert!(std::path::Path::new(&input.source_path()).is_file());
        let path = input.source_path();
        drop(input);
        assert!(!std::path::Path::new(&path).exists());
    }

    /// Common Rust validates/encodes the RGBA thumbnail and preserves exact frozen main bytes.
    #[tokio::test]
    async fn bridge_native_evidence_retains_read_lease_and_common_thumbnail_validation() {
        let _preparation = PREPARATIONS.lock().await;
        let source = Source::new();
        let host = Arc::new(Decoder {
            header_only: false,
            retain: Mutex::new(None),
        });
        let prepared = m::prepare_video_with_decoder(
            &source.0,
            Arc::new(VideoDecoderAdapter(host.clone())),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(prepared.path()).unwrap(),
            b"synthetic decoder input"
        );
        assert_eq!(
            prepared.main_facts.as_ref().unwrap().evidence,
            m::MediaEvidence::HostDecoded
        );
        assert!(prepared
            .thumbnail
            .as_ref()
            .unwrap()
            .starts_with(b"\x89PNG\r\n\x1a\n"));
        let input = host.retain.lock().unwrap().take().unwrap();
        let path = input.source_path();
        drop(prepared);
        assert!(std::path::Path::new(&path).is_file());
        drop(input);
        assert!(!std::path::Path::new(&path).exists());
    }

    /// Decoder-only configuration cannot substitute for the normal sending encoder contract.
    #[tokio::test]
    async fn normal_video_preparation_never_substitutes_a_decoder_for_the_encoder() {
        let _preparation = PREPARATIONS.lock().await;
        let source = Source::new();
        let info = libtinkle::ClientInfo::new(
            "test-device".into(),
            "test".into(),
            "1".into(),
            "1".into(),
            "1".into(),
            Default::default(),
        )
        .unwrap();
        let client = libtinkle::Client::new_sqlite(
            libtinkle::ClientConfig::new("127.0.0.1", info),
            libtinkle::TrustSource::Bootstrap(libtinkle::ServerTrust {
                server_public_key: [7; 32],
                server_key_id: 1,
                scope: "video-test".into(),
            }),
            Arc::new(libtinkle::Storage::open_in_memory().unwrap()),
        );
        client.set_principal_user_id(1).await;
        assert!(matches!(
            client
                .sending()
                .prepare_attachment("video".into(), &source.0, m::MediaKind::Video, None)
                .await,
            Err(libtinkle::SendError::Media(m::MediaError::Unsupported))
        ));
        let host = Arc::new(Decoder {
            header_only: true,
            retain: Mutex::new(None),
        });
        client
            .configure_video_decoder(Some(Arc::new(VideoDecoderAdapter(host.clone()))))
            .unwrap();
        assert!(matches!(
            client
                .sending()
                .prepare_attachment("video".into(), &source.0, m::MediaKind::Video, None)
                .await,
            Err(libtinkle::SendError::Media(m::MediaError::Unsupported))
        ));
        assert!(host.retain.lock().unwrap().is_none());
        client.configure_video_decoder(None).unwrap();
        client.close().await;
    }
}
