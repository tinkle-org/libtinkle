//! Shared, deterministic client-side preparation. Never infer content from encrypted
//! server metadata. `PREPARATION_PROFILE` plus source SHA-256 identifies the work;
//! callers must also retain output SHA-256 when resuming an upload.
//!
//! Pure Rust PNG/WebP/JPEG decoding, lossless PNG/WebP encoding and integer PCM
//! FLAC encoding require no mobile OS codec. Animation uses a pinned bundled
//! libwebp mux and independently verifies every lossless composited frame.
//! PNG color-management chunks and unsupported depths retain their original bytes.
//! Android API 24 supports static lossless WebP decoding; PNG is the portable
//! thumbnail. FLAC playback's portable Android floor is mono/stereo <=48kHz;
//! 24-bit and higher rates require consumer qualification, never silent reduction.
//! Browser networking remains unsupported.
//! FFmpeg is an explicit desktop-only adapter, never a required mobile executable.

use image::{DynamicImage, ImageDecoder, ImageEncoder, ImageFormat, ImageReader};
use sha2::{Digest, Sha256};
use std::io::Cursor;

#[cfg(not(target_arch = "wasm32"))]
#[path = "native_preparation.rs"]
mod native_preparation;
#[cfg(not(target_arch = "wasm32"))]
pub use native_preparation::*;

// Admission is retained by the input lease, including during native cancellation/join.
#[cfg(not(target_arch = "wasm32"))]
static NATIVE_MEDIA_WORKERS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

/// Versioned algorithm/limits; bump when output selection or codec settings change.
pub const PREPARATION_PROFILE: &str = "tinkle-media-v3-image02510-flac051-webpmux020";
/// Byte convenience/codec input ceiling. Documents larger than this use prepare_path.
pub const MAX_MEDIA_BYTES: usize = 32 * 1024 * 1024;
/// Bounded decoded pixel count (RGBA16 worst-case 128 MiB per decode).
pub const MAX_IMAGE_PIXELS: u64 = 16 * 1024 * 1024;

/// Synchronous callers share the same codec admission as native path workers.
static CODEC_ADMISSION: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
struct CodecPermit;
impl CodecPermit {
    /// Finite lock-free admission, using atomic operations available at workspace MSRV.
    fn acquire() -> Result<Self, MediaError> {
        use std::sync::atomic::Ordering::{AcqRel, Acquire};
        for _ in 0..8 {
            let count = CODEC_ADMISSION.load(Acquire);
            if count >= 2 {
                return Err(MediaError::Busy);
            }
            if CODEC_ADMISSION
                .compare_exchange(count, count + 1, AcqRel, Acquire)
                .is_ok()
            {
                return Ok(Self);
            }
        }
        Err(MediaError::Busy)
    }
}
impl Drop for CodecPermit {
    fn drop(&mut self) {
        CODEC_ADMISSION.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Typed preparation failures; no user paths, metadata or codec diagnostics escape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MediaError {
    #[error("media exceeds preparation budget")]
    Budget,
    #[error("invalid or malformed media")]
    Invalid,
    #[error("media I/O failed")]
    Io,
    #[error("media source changed")]
    SourceChanged,
    #[error("media preparation cancelled")]
    Cancelled,
    #[error("media worker unavailable")]
    Busy,
    #[error("native media codec unavailable on this platform")]
    Unsupported,
    #[error("native media codec failed or exceeded its budget")]
    Codec,
}

/// Exact signed, interleaved little-endian integer PCM configuration; no resampling.
#[derive(Debug, Clone, Copy)]
pub struct PcmFormat {
    pub sample_rate: u32,
    pub channels: u8,
    /// Supported integer widths: 8, 16 and 24. Float/32-bit PCM is rejected.
    pub bits_per_sample: u8,
}

/// Explicit caller intent. Documents bypass all sniffing and transformation.
#[derive(Debug, Clone, Copy)]
pub enum MediaKind {
    Document,
    Image,
    Voice(PcmFormat),
    Video,
}

/// Preservation is an intentional output, not a claim a codec transformed anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparationDisposition {
    ExactOriginal,
    Lossless,
    PreservedUnsupported,
    /// Actual native encode; audio/video encoding is not described as lossless or compression.
    NativeEncoded,
}

/// Container identity is independent of animation, bit depth and validation evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodedFormat {
    Png,
    WebP,
    Jpeg,
    Gif,
    Heif,
    Flac,
    Mp4,
}

/// Header inspection alone is never enough to author a media object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaEvidence {
    HeaderOnly,
    FullyDecoded,
    SelfEncoded,
    HostDecoded,
}

/// Measured facts about the exact output bytes. Loop count is total iterations
/// (0 = infinite), not GIF's count of repetitions after the initial iteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaFacts {
    pub format: EncodedFormat,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub animated: bool,
    pub frame_count: Option<u32>,
    /// Duration of one full iteration; no browser delay clamping is applied.
    pub duration_ms: Option<u64>,
    /// Present for animated images: every original frame has a positive delay.
    pub positive_frame_durations: Option<bool>,
    pub loop_count: Option<u32>,
    pub has_alpha: Option<bool>,
    /// Decoded channel precision; palette indices expand to their 8-bit colors.
    pub bit_depth: Option<u8>,
    pub lossless: bool,
    pub evidence: MediaEvidence,
}
impl MediaFacts {
    /// Container/animation subset of STRICT_LOSSLESS v1. Object-specific size,
    /// dimensions and duration budgets still belong to the authoring facade.
    pub fn strict_lossless_v1(&self) -> bool {
        self.evidence != MediaEvidence::HeaderOnly
            && self.lossless
            && (!self.animated || self.positive_frame_durations == Some(true))
            && (self.format == EncodedFormat::WebP
                || (self.format == EncodedFormat::Png && !self.animated))
    }
}

/// Owned, bounded convenience result. Full-size native documents use PreparedMediaPath.
#[derive(Debug)]
pub struct PreparedMedia {
    pub bytes: Vec<u8>,
    pub mime_type: &'static str,
    pub source_sha256: [u8; 32],
    pub output_sha256: [u8; 32],
    pub profile: &'static str,
    /// Always PNG; longest edge <=320, never upscaled. None for unsupported media.
    pub thumbnail: Option<Vec<u8>>,
    pub disposition: PreparationDisposition,
    pub main_facts: Option<MediaFacts>,
    pub thumbnail_facts: Option<MediaFacts>,
}

/// Prepare bounded bytes synchronously. Native callers should dispatch with prepare_path,
/// which bounds blocking work; this function never belongs on a UI/async executor thread.
pub fn prepare_bytes(input: &[u8], kind: MediaKind) -> Result<PreparedMedia, MediaError> {
    if input.len() > MAX_MEDIA_BYTES {
        return Err(MediaError::Budget);
    }
    let _permit = if matches!(kind, MediaKind::Image | MediaKind::Voice(_)) {
        Some(CodecPermit::acquire()?)
    } else {
        None
    };
    let source_sha256 = Sha256::digest(input).into();
    let (bytes, mime_type, thumbnail, disposition) = match kind {
        MediaKind::Document => (
            input.to_vec(),
            "application/octet-stream",
            None,
            PreparationDisposition::ExactOriginal,
        ),
        MediaKind::Video => return Err(MediaError::Unsupported),
        MediaKind::Voice(format) => (
            encode_pcm(input, format)?,
            "audio/flac",
            None,
            PreparationDisposition::Lossless,
        ),
        MediaKind::Image => prepare_image(input)?,
    };
    let output_sha256 = Sha256::digest(&bytes).into();
    let main_facts = match kind {
        MediaKind::Image => match inspect_image(&bytes) {
            Ok(facts) => Some(facts),
            Err(MediaError::Unsupported) => None,
            Err(e) => return Err(e),
        },
        MediaKind::Voice(_) => Some(inspect_flac(&bytes)?),
        _ => None,
    };
    let thumbnail_facts = thumbnail.as_ref().map(|t| inspect_image(t)).transpose()?;
    Ok(PreparedMedia {
        bytes,
        mime_type,
        source_sha256,
        output_sha256,
        profile: PREPARATION_PROFILE,
        thumbnail,
        disposition,
        main_facts,
        thumbnail_facts,
    })
}

type ImageOutput = (
    Vec<u8>,
    &'static str,
    Option<Vec<u8>>,
    PreparationDisposition,
);

/// Decoder limits are installed before any raster allocation.
fn reader(input: &[u8], format: ImageFormat) -> ImageReader<Cursor<&[u8]>> {
    let mut reader = ImageReader::with_format(Cursor::new(input), format);
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(16384);
    limits.max_image_height = Some(16384);
    limits.max_alloc = Some(MAX_IMAGE_PIXELS * 8);
    reader.limits(limits);
    reader
}

fn prepare_image(input: &[u8]) -> Result<ImageOutput, MediaError> {
    // HEIF/HEIC is a common mobile import, but this pure-Rust profile has no
    // qualified decoder. Native path preparation may supply one; do not guess at pixels.
    if input.len() >= 16
        && &input[4..8] == b"ftyp"
        && matches!(
            &input[8..12],
            b"heic" | b"heix" | b"hevc" | b"hevx" | b"mif1" | b"msf1"
        )
    {
        return Err(MediaError::Unsupported);
    }
    let format = image::guess_format(input).map_err(|_| MediaError::Invalid)?;
    let mime = match format {
        ImageFormat::Png => "image/png",
        ImageFormat::WebP => "image/webp",
        ImageFormat::Jpeg => "image/jpeg",
        ImageFormat::Gif => "image/gif",
        _ => "application/octet-stream",
    };
    if !matches!(
        format,
        ImageFormat::Png | ImageFormat::WebP | ImageFormat::Jpeg | ImageFormat::Gif
    ) {
        return Err(MediaError::Unsupported);
    }
    let mut decoder = reader(input, format)
        .into_decoder()
        .map_err(|_| MediaError::Invalid)?;
    let (width, height) = decoder.dimensions();
    if u64::from(width) * u64::from(height) > MAX_IMAGE_PIXELS {
        return Err(MediaError::Budget);
    }
    // Do not flatten GIF/APNG/animated WebP or discard their loop/timing metadata.
    let animated = match format {
        ImageFormat::Gif => true,
        ImageFormat::Png => png_chunks(input)?.iter().any(|tag| tag == b"acTL"),
        ImageFormat::WebP => webp_chunks(input)?
            .iter()
            .any(|(tag, _)| matches!(tag, b"ANIM" | b"ANMF")),
        _ => false,
    };
    if animated {
        let managed = decoder
            .icc_profile()
            .map_err(|_| MediaError::Invalid)?
            .is_some()
            || (format == ImageFormat::Gif && input.windows(11).any(|v| v == b"ICCRGBG1012"))
            || (format == ImageFormat::Png
                && png_chunks(input)?
                    .iter()
                    .any(|t| matches!(t, b"gAMA" | b"cHRM" | b"cICP" | b"iCCP")));
        if managed {
            let _ = scan_animation(input, format, |_| Ok(()))?;
            return Err(MediaError::Unsupported);
        }
        return prepare_animation(input, format, mime);
    }
    let orientation = decoder.orientation().map_err(|_| MediaError::Invalid)?;
    let unsupported_color = matches!(
        decoder.original_color_type(),
        image::ExtendedColorType::Cmyk8 | image::ExtendedColorType::Cmyk16
    );
    let icc = decoder.icc_profile().map_err(|_| MediaError::Invalid)?;
    let mut image = DynamicImage::from_decoder(decoder).map_err(|_| MediaError::Invalid)?;
    image.apply_orientation(orientation);
    // Embedded color management has semantics beyond this encoder's ICC support.
    // Preserve the complete original rather than silently changing its color space.
    let managed = unsupported_color
        || icc.is_some()
        || (format == ImageFormat::Png
            && png_chunks(input)?.iter().any(|tag| {
                matches!(
                    tag,
                    b"gAMA" | b"cHRM" | b"sRGB" | b"cICP" | b"iCCP" | b"mDCv" | b"cLLi"
                )
            }));
    let thumbnail = if managed {
        None
    } else {
        let thumb = if image.width().max(image.height()) > 320 {
            image.thumbnail(320, 320)
        } else {
            image.clone()
        };
        Some(encode_png(&thumb)?)
    };
    if managed
        || !matches!(
            image.color(),
            image::ColorType::Rgb8
                | image::ColorType::Rgba8
                | image::ColorType::L8
                | image::ColorType::La8
        )
    {
        if format == ImageFormat::Png
            && input.len() <= 10_000_000
            && image.width() <= 10_000
            && image.height() <= 10_000
            && orientation == image::metadata::Orientation::NoTransforms
        {
            return Ok((
                input.to_vec(),
                mime,
                thumbnail,
                PreparationDisposition::ExactOriginal,
            ));
        }
        return Err(MediaError::Unsupported);
    }
    for attempt in 0..=64 {
        let (width, height) = image_output_dimensions(image.width(), image.height(), attempt)?;
        let resized;
        let image = if (width, height) == (image.width(), image.height()) {
            &image
        } else {
            resized = image.resize_exact(width, height, image::imageops::FilterType::Triangle);
            &resized
        };
        let png = match encode_png(image) {
            Ok(png) => png,
            Err(MediaError::Budget) => continue,
            Err(e) => return Err(e),
        };
        let mut best = (png, "image/png");
        if matches!(
            image.color(),
            image::ColorType::Rgb8 | image::ColorType::Rgba8
        ) && image.width() <= 16383
            && image.height() <= 16383
        {
            let mut webp = LimitedOutput(Vec::new());
            let encoded = image::codecs::webp::WebPEncoder::new_lossless(&mut webp)
                .write_image(
                    image.as_bytes(),
                    image.width(),
                    image.height(),
                    image.color().into(),
                )
                .map_err(encode_limit_error);
            match encoded {
                Ok(()) => {}
                Err(MediaError::Budget) => continue,
                Err(e) => return Err(e),
            }
            // Verify even fully transparent RGB values, not only displayed pixels.
            let decoded = image::load_from_memory_with_format(&webp.0, ImageFormat::WebP)
                .map_err(|_| MediaError::Codec)?;
            use image::GenericImageView;
            if decoded.dimensions() == image.dimensions()
                && decoded.pixels().eq(image.pixels())
                && webp.0.len() < best.0.len()
            {
                best = (webp.0, "image/webp");
            }
        }
        if best.0.len() <= 10_000_000 {
            return Ok((best.0, best.1, thumbnail, PreparationDisposition::Lossless));
        }
    }
    Err(MediaError::Budget)
}

/// Parse chunk framing without allocating per payload; malformed lengths fail closed.
fn png_chunks(input: &[u8]) -> Result<Vec<[u8; 4]>, MediaError> {
    if !input.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Err(MediaError::Invalid);
    }
    let mut offset = 8usize;
    let mut tags = Vec::new();
    while offset < input.len() {
        let start = offset;
        let header = input.get(offset..offset + 8).ok_or(MediaError::Invalid)?;
        let len =
            u32::from_be_bytes(header[..4].try_into().map_err(|_| MediaError::Invalid)?) as usize;
        let tag = header[4..8].try_into().map_err(|_| MediaError::Invalid)?;
        offset = offset
            .checked_add(12)
            .and_then(|n| n.checked_add(len))
            .ok_or(MediaError::Invalid)?;
        if offset > input.len() || tags.len() >= 4096 {
            return Err(MediaError::Budget);
        }
        let stored = u32::from_be_bytes(
            input[offset - 4..offset]
                .try_into()
                .map_err(|_| MediaError::Invalid)?,
        );
        if crc32fast::hash(&input[start + 4..offset - 4]) != stored {
            return Err(MediaError::Invalid);
        }
        tags.push(tag);
        if tag == *b"IEND" {
            if offset != input.len() {
                return Err(MediaError::Invalid);
            }
            return Ok(tags);
        }
    }
    Err(MediaError::Invalid)
}

fn encode_png(image: &DynamicImage) -> Result<Vec<u8>, MediaError> {
    let mut output = LimitedOutput(Vec::new());
    image::codecs::png::PngEncoder::new_with_quality(
        &mut output,
        image::codecs::png::CompressionType::Best,
        image::codecs::png::FilterType::Adaptive,
    )
    .write_image(
        image.as_bytes(),
        image.width(),
        image.height(),
        image.color().into(),
    )
    .map_err(encode_limit_error)?;
    Ok(output.0)
}

/// Enforce encoded output budgets during writes, not after a potentially huge Vec grows.
struct LimitedOutput(Vec<u8>);
/// The only I/O in the memory encoder is its bounded output writer.
fn encode_limit_error(error: image::ImageError) -> MediaError {
    if matches!(error, image::ImageError::IoError(_)) {
        MediaError::Budget
    } else {
        MediaError::Codec
    }
}
impl std::io::Write for LimitedOutput {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if self
            .0
            .len()
            .checked_add(data.len())
            .is_none_or(|n| n > MAX_MEDIA_BYTES)
        {
            return Err(std::io::Error::other("encoded media exceeds budget"));
        }
        self.0.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Stream full composited frames, never collecting full animation rasters in RAM.
fn animation_frames(input: &[u8], format: ImageFormat) -> Result<image::Frames<'_>, MediaError> {
    use image::AnimationDecoder;
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(MAX_IMAGE_PIXELS * 8);
    Ok(match format {
        ImageFormat::Gif => {
            let mut decoder = image::codecs::gif::GifDecoder::new(Cursor::new(input))
                .map_err(|_| MediaError::Invalid)?;
            decoder.set_limits(limits).map_err(|_| MediaError::Budget)?;
            decoder.into_frames()
        }
        ImageFormat::Png => {
            let mut decoder = image::codecs::png::PngDecoder::new(Cursor::new(input))
                .map_err(|_| MediaError::Invalid)?;
            decoder.set_limits(limits).map_err(|_| MediaError::Budget)?;
            decoder
                .apng()
                .map_err(|_| MediaError::Invalid)?
                .into_frames()
        }
        ImageFormat::WebP => {
            let mut decoder = image::codecs::webp::WebPDecoder::new(Cursor::new(input))
                .map_err(|_| MediaError::Invalid)?;
            decoder.set_limits(limits).map_err(|_| MediaError::Budget)?;
            decoder.into_frames()
        }
        _ => return Err(MediaError::Unsupported),
    })
}

/// A compact proof retains every delay and complete RGBA digest, including hidden
/// RGB under alpha zero. Full-frame no-blend output makes disposal effects explicit.
#[derive(PartialEq, Eq)]
struct FrameProof {
    digest: [u8; 32],
    delay: (u32, u32),
}
struct AnimationSummary {
    width: u32,
    height: u32,
    proofs: Vec<FrameProof>,
    duration: Option<u64>,
    first_opaque: bool,
    alpha: bool,
    thumbnail: Vec<u8>,
}
fn scan_animation(
    input: &[u8],
    format: ImageFormat,
    mut visit: impl FnMut(&image::Frame) -> Result<(), MediaError>,
) -> Result<AnimationSummary, MediaError> {
    let mut summary = AnimationSummary {
        width: 0,
        height: 0,
        proofs: Vec::new(),
        duration: Some(0),
        first_opaque: false,
        alpha: false,
        thumbnail: Vec::new(),
    };
    let mut pixels = 0u64;
    for frame in animation_frames(input, format)? {
        let frame = frame.map_err(|_| MediaError::Invalid)?;
        let image = frame.buffer();
        let (width, height) = image.dimensions();
        let count = u64::from(width) * u64::from(height);
        pixels = pixels.checked_add(count).ok_or(MediaError::Budget)?;
        if count == 0
            || count > MAX_IMAGE_PIXELS
            || pixels > MAX_IMAGE_PIXELS * 4
            || summary.proofs.len() >= 512
        {
            return Err(MediaError::Budget);
        }
        let alpha = image.pixels().any(|p| p.0[3] != 255);
        if summary.proofs.is_empty() {
            summary.width = width;
            summary.height = height;
            summary.first_opaque = !alpha;
            let thumb = DynamicImage::ImageRgba8(image.clone());
            summary.thumbnail = encode_png(&if width.max(height) > 320 {
                thumb.thumbnail(320, 320)
            } else {
                thumb
            })?;
        } else if width != summary.width || height != summary.height {
            return Err(MediaError::Invalid);
        }
        summary.alpha |= alpha;
        let delay = frame.delay().numer_denom_ms();
        summary.duration = summary.duration.and_then(|n| {
            if delay.1 > 0 && delay.0 % delay.1 == 0 {
                n.checked_add(u64::from(delay.0 / delay.1))
            } else {
                None
            }
        });
        summary.proofs.push(FrameProof {
            digest: Sha256::digest(image.as_raw()).into(),
            delay,
        });
        visit(&frame)?;
    }
    if summary.proofs.is_empty() {
        return Err(MediaError::Invalid);
    }
    Ok(summary)
}

/// GIF finite values count repetitions, WebP/APNG count total iterations. Values
/// not representable by WebP (65536 iterations) stay in the original container.
fn gif_control(input: &[u8]) -> Result<(u32, bool), MediaError> {
    let mut options = gif::DecodeOptions::new();
    options.set_color_output(gif::ColorOutput::Indexed);
    options.set_memory_limit(gif::MemoryLimit::Bytes(
        std::num::NonZeroU64::new(MAX_IMAGE_PIXELS * 4).ok_or(MediaError::Budget)?,
    ));
    let mut decoder = options
        .read_info(Cursor::new(input))
        .map_err(|_| MediaError::Invalid)?;
    let width = decoder.width();
    let height = decoder.height();
    let mut last_clears = false;
    let mut count = 0usize;
    let mut pixels = 0u64;
    while let Some(frame) = decoder.read_next_frame().map_err(|_| MediaError::Invalid)? {
        count += 1;
        pixels = pixels
            .checked_add(u64::from(frame.width) * u64::from(frame.height))
            .ok_or(MediaError::Budget)?;
        if count > 512 || pixels > MAX_IMAGE_PIXELS * 4 {
            return Err(MediaError::Budget);
        }
        last_clears = frame.dispose == gif::DisposalMethod::Background
            && frame.left == 0
            && frame.top == 0
            && frame.width == width
            && frame.height == height;
    }
    let loops = match decoder.repeat() {
        gif::Repeat::Infinite => 0,
        gif::Repeat::Finite(n) => u32::from(n) + 1,
    };
    let reader = decoder.into_inner();
    if reader
        .get_ref()
        .position()
        .saturating_sub(reader.buffer().len() as u64)
        != input.len() as u64
    {
        return Err(MediaError::Invalid);
    }
    Ok((loops, last_clears))
}

/// Strict RIFF chunk framing; never infer animation/losslessness by byte substring.
type ContainerChunks<'a> = Vec<([u8; 4], &'a [u8])>;
fn webp_chunks(input: &[u8]) -> Result<ContainerChunks<'_>, MediaError> {
    if input.len() < 12
        || &input[..4] != b"RIFF"
        || &input[8..12] != b"WEBP"
        || (u32::from_le_bytes(input[4..8].try_into().map_err(|_| MediaError::Invalid)?) as usize)
            .checked_add(8)
            != Some(input.len())
    {
        return Err(MediaError::Invalid);
    }
    chunks(&input[12..])
}
fn chunks(mut bytes: &[u8]) -> Result<ContainerChunks<'_>, MediaError> {
    let mut result = Vec::new();
    while !bytes.is_empty() {
        let header = bytes.get(..8).ok_or(MediaError::Invalid)?;
        let len =
            u32::from_le_bytes(header[4..8].try_into().map_err(|_| MediaError::Invalid)?) as usize;
        let end = 8usize.checked_add(len).ok_or(MediaError::Budget)?;
        let next = end.checked_add(len % 2).ok_or(MediaError::Budget)?;
        if !len.is_multiple_of(2) && bytes.get(end) != Some(&0) {
            return Err(MediaError::Invalid);
        }
        if result.len() >= 2048 {
            return Err(MediaError::Budget);
        }
        result.push((
            header[..4].try_into().map_err(|_| MediaError::Invalid)?,
            bytes.get(8..end).ok_or(MediaError::Invalid)?,
        ));
        bytes = bytes.get(next..).ok_or(MediaError::Invalid)?;
    }
    Ok(result)
}
fn animation_loops(input: &[u8], format: ImageFormat) -> Result<u32, MediaError> {
    match format {
        ImageFormat::Gif => Ok(gif_control(input)?.0),
        ImageFormat::WebP => webp_chunks(input)?
            .iter()
            .find_map(|(tag, data)| {
                if tag == b"ANIM" && data.len() == 6 {
                    Some(u32::from(u16::from_le_bytes([data[4], data[5]])))
                } else {
                    None
                }
            })
            .ok_or(MediaError::Invalid),
        ImageFormat::Png => {
            let _ = png_chunks(input)?;
            let mut offset = 8;
            while offset < input.len() {
                let len = u32::from_be_bytes(
                    input[offset..offset + 4]
                        .try_into()
                        .map_err(|_| MediaError::Invalid)?,
                ) as usize;
                if &input[offset + 4..offset + 8] == b"acTL" && len == 8 {
                    return Ok(u32::from_be_bytes(
                        input[offset + 12..offset + 16]
                            .try_into()
                            .map_err(|_| MediaError::Invalid)?,
                    ));
                }
                offset += len + 12;
            }
            Err(MediaError::Invalid)
        }
        _ => Err(MediaError::Unsupported),
    }
}

/// C mux ownership stays local to this worker. PushFrame copies its input, and
/// assembly output is released by WebPDataClear, never by Rust's allocator.
struct WebpMux(std::ptr::NonNull<libwebp_sys::WebPMux>);
impl Drop for WebpMux {
    fn drop(&mut self) {
        // SAFETY: unique nonnull allocation from WebPMuxNew; no aliases escape this type.
        unsafe {
            libwebp_sys::WebPMuxDelete(self.0.as_ptr());
        }
    }
}
impl WebpMux {
    fn new(loops: u32) -> Result<Self, MediaError> {
        if loops > 65535 {
            return Err(MediaError::Unsupported);
        }
        // SAFETY: constructor takes no pointers and returns an owned mux or null.
        let mux = Self(
            std::ptr::NonNull::new(unsafe { libwebp_sys::WebPMuxNew() })
                .ok_or(MediaError::Codec)?,
        );
        let params = libwebp_sys::WebPMuxAnimParams {
            bgcolor: 0,
            loop_count: loops as i32,
        };
        // SAFETY: mux is live and exclusive; params remains valid through this copying call.
        if unsafe { libwebp_sys::WebPMuxSetAnimationParams(mux.0.as_ptr(), &params) }
            != libwebp_sys::WEBP_MUX_OK
        {
            return Err(MediaError::Codec);
        }
        Ok(mux)
    }
    fn push(&mut self, frame: &image::Frame) -> Result<usize, MediaError> {
        let delay = frame.delay().numer_denom_ms();
        if delay.1 == 0
            || !delay.0.is_multiple_of(delay.1)
            || !(1..=0xff_ffff).contains(&(delay.0 / delay.1))
        {
            return Err(MediaError::Unsupported);
        }
        let image = frame.buffer();
        let mut bytes = LimitedOutput(Vec::new());
        image::codecs::webp::WebPEncoder::new_lossless(&mut bytes)
            .write_image(
                image.as_raw(),
                image.width(),
                image.height(),
                image::ExtendedColorType::Rgba8,
            )
            .map_err(encode_limit_error)?;
        let info = libwebp_sys::WebPMuxFrameInfo {
            bitstream: libwebp_sys::WebPData {
                bytes: bytes.0.as_ptr(),
                size: bytes.0.len(),
            },
            x_offset: 0,
            y_offset: 0,
            duration: (delay.0 / delay.1) as i32,
            id: libwebp_sys::WEBP_CHUNK_ANMF,
            dispose_method: libwebp_sys::WEBP_MUX_DISPOSE_NONE,
            blend_method: libwebp_sys::WEBP_MUX_NO_BLEND,
            pad: [0],
        };
        // SAFETY: input pointers refer to live owned bytes; copy_data=1 forbids retaining them.
        if unsafe { libwebp_sys::WebPMuxPushFrame(self.0.as_ptr(), &info, 1) }
            != libwebp_sys::WEBP_MUX_OK
        {
            return Err(MediaError::Codec);
        }
        Ok(bytes.0.len() + 32)
    }
    fn finish(self) -> Result<Vec<u8>, MediaError> {
        struct Data(libwebp_sys::WebPData);
        impl Drop for Data {
            fn drop(&mut self) {
                // SAFETY: null-initialized or initialized by Assemble; C owns this allocation.
                unsafe {
                    libwebp_sys::WebPDataClear(&mut self.0);
                }
            }
        }
        let mut data = Data(libwebp_sys::WebPData {
            bytes: std::ptr::null(),
            size: 0,
        });
        // SAFETY: mux is live; data points to writable, correctly initialized storage.
        if unsafe { libwebp_sys::WebPMuxAssemble(self.0.as_ptr(), &mut data.0) }
            != libwebp_sys::WEBP_MUX_OK
        {
            return Err(MediaError::Codec);
        }
        if data.0.bytes.is_null() || data.0.size > MAX_MEDIA_BYTES {
            return Err(MediaError::Budget);
        }
        // SAFETY: successful Assemble supplies size readable bytes, retained until Data drops.
        Ok(unsafe { std::slice::from_raw_parts(data.0.bytes, data.0.size) }.to_vec())
    }
}

/// Encode complete no-blend keyframes, not an optimizer that merges duplicate
/// frames or rounds delays. Independently decode to verify compositing/disposal,
/// all RGBA bytes, exact delays, count and finite/infinite loop semantics.
fn prepare_animation(
    input: &[u8],
    format: ImageFormat,
    _mime: &'static str,
) -> Result<ImageOutput, MediaError> {
    let loops = animation_loops(input, format)?;
    let summary = scan_animation(input, format, |_| Ok(()))?;
    if summary.proofs.len() == 1 {
        let mut raster = None;
        scan_animation(input, format, |frame| {
            raster = Some(DynamicImage::ImageRgba8(frame.buffer().clone()));
            Ok(())
        })?;
        let raster = raster.ok_or(MediaError::Invalid)?;
        for attempt in 0..=64 {
            let (width, height) = image_output_dimensions(summary.width, summary.height, attempt)?;
            let bytes = match encode_png(&raster.resize_exact(
                width,
                height,
                image::imageops::FilterType::Triangle,
            )) {
                Ok(bytes) => bytes,
                Err(MediaError::Budget) => continue,
                Err(e) => return Err(e),
            };
            if bytes.len() <= 10_000_000 {
                return Ok((
                    bytes,
                    "image/png",
                    Some(summary.thumbnail),
                    PreparationDisposition::Lossless,
                ));
            }
        }
        return Err(MediaError::Budget);
    }
    if summary.width > 16383
        || summary.height > 16383
        || loops > 65535
        || (format == ImageFormat::Gif
            && !(loops == 1 || summary.first_opaque || gif_control(input)?.1))
    {
        return Err(MediaError::Unsupported);
    }
    for attempt in 0..=64 {
        let (width, height) = image_output_dimensions(summary.width, summary.height, attempt)?;
        let mut mux = WebpMux::new(loops)?;
        let mut size = 0usize;
        let mut proofs = Vec::new();
        let encoded = scan_animation(input, format, |frame| {
            let resized;
            let frame = if (width, height) == frame.buffer().dimensions() {
                frame
            } else {
                resized = image::Frame::from_parts(
                    image::imageops::resize(
                        frame.buffer(),
                        width,
                        height,
                        image::imageops::FilterType::Triangle,
                    ),
                    0,
                    0,
                    frame.delay(),
                );
                &resized
            };
            proofs.push(FrameProof {
                digest: Sha256::digest(frame.buffer().as_raw()).into(),
                delay: frame.delay().numer_denom_ms(),
            });
            size = size
                .checked_add(mux.push(frame)?)
                .ok_or(MediaError::Budget)?;
            if size > 10_000_000 {
                return Err(MediaError::Budget);
            }
            Ok(())
        });
        match encoded {
            Err(MediaError::Budget) => continue,
            Err(e) => return Err(e),
            Ok(_) => {}
        }
        let bytes = mux.finish()?;
        if bytes.len() > 10_000_000 {
            continue;
        }
        let check = scan_animation(&bytes, ImageFormat::WebP, |_| Ok(()))?;
        if check.width != width
            || check.height != height
            || check.proofs != proofs
            || animation_loops(&bytes, ImageFormat::WebP)? != loops
        {
            return Err(MediaError::Codec);
        }
        return Ok((
            bytes,
            "image/webp",
            Some(summary.thumbnail),
            PreparationDisposition::Lossless,
        ));
    }
    Err(MediaError::Budget)
}

/// Independently inspect exact output bytes; these facts are not server authority.
fn inspect_image(input: &[u8]) -> Result<MediaFacts, MediaError> {
    if input.len() >= 16 && &input[4..8] == b"ftyp" {
        let mut facts = video_header_facts();
        facts.format = EncodedFormat::Heif;
        return Ok(facts);
    }
    let format = image::guess_format(input).map_err(|_| MediaError::Unsupported)?;
    // Sniffing recognizes containers whose decoders are intentionally not compiled in.
    if !matches!(
        format,
        ImageFormat::Png | ImageFormat::WebP | ImageFormat::Gif | ImageFormat::Jpeg
    ) {
        return Err(MediaError::Unsupported);
    }
    let decoder = reader(input, format)
        .into_decoder()
        .map_err(|_| MediaError::Invalid)?;
    let (width, height) = decoder.dimensions();
    if u64::from(width) * u64::from(height) > MAX_IMAGE_PIXELS {
        return Err(MediaError::Budget);
    }
    let mut lossless = format == ImageFormat::Png;
    let animated = match format {
        ImageFormat::Gif => true,
        ImageFormat::Png => png_chunks(input)?.iter().any(|v| v == b"acTL"),
        ImageFormat::WebP => {
            let riff = webp_chunks(input)?;
            lossless = riff.iter().any(|(t, _)| t == b"VP8L")
                || (riff.iter().any(|(t, _)| t == b"ANMF")
                    && riff.iter().filter(|(t, _)| t == b"ANMF").try_fold(
                        true,
                        |ok, (_, d)| {
                            let nested = chunks(d.get(16..).ok_or(MediaError::Invalid)?)?;
                            Ok::<_, MediaError>(ok && nested.iter().any(|(tag, _)| tag == b"VP8L"))
                        },
                    )?);
            riff.iter().any(|(t, _)| t == b"ANIM")
        }
        _ => false,
    };
    let encoded = match format {
        ImageFormat::Png => EncodedFormat::Png,
        ImageFormat::WebP => EncodedFormat::WebP,
        ImageFormat::Gif => EncodedFormat::Gif,
        ImageFormat::Jpeg => EncodedFormat::Jpeg,
        _ => return Err(MediaError::Unsupported),
    };
    if animated {
        let summary = scan_animation(input, format, |_| Ok(()))?;
        return Ok(MediaFacts {
            format: encoded,
            width: Some(width),
            height: Some(height),
            animated: summary.proofs.len() > 1,
            frame_count: Some(summary.proofs.len() as u32),
            duration_ms: summary.duration,
            positive_frame_durations: Some(
                summary
                    .proofs
                    .iter()
                    .all(|p| p.delay.0 > 0 && p.delay.1 > 0),
            ),
            loop_count: Some(animation_loops(input, format)?),
            has_alpha: Some(summary.alpha),
            bit_depth: Some(8),
            lossless,
            evidence: MediaEvidence::FullyDecoded,
        });
    }
    let color = decoder.color_type();
    let image = DynamicImage::from_decoder(decoder).map_err(|_| MediaError::Invalid)?;
    let has_alpha = match &image {
        DynamicImage::ImageRgba16(p) => p.pixels().any(|p| p.0[3] != u16::MAX),
        DynamicImage::ImageLumaA16(p) => p.pixels().any(|p| p.0[1] != u16::MAX),
        _ if color.has_alpha() => {
            use image::GenericImageView;
            image.pixels().any(|(_, _, p)| p.0[3] != 255)
        }
        _ => false,
    };
    Ok(MediaFacts {
        format: encoded,
        width: Some(width),
        height: Some(height),
        animated: false,
        frame_count: Some(1),
        duration_ms: None,
        positive_frame_durations: None,
        loop_count: None,
        has_alpha: Some(has_alpha),
        bit_depth: Some((color.bits_per_pixel() / u16::from(color.channel_count())) as u8),
        lossless,
        evidence: MediaEvidence::FullyDecoded,
    })
}
fn inspect_flac(input: &[u8]) -> Result<MediaFacts, MediaError> {
    let header = input.get(..42).ok_or(MediaError::Invalid)?;
    if &header[..4] != b"fLaC" {
        return Err(MediaError::Invalid);
    }
    let word = u64::from_be_bytes(header[18..26].try_into().map_err(|_| MediaError::Invalid)?);
    let rate = word >> 44;
    if rate == 0 {
        return Err(MediaError::Invalid);
    }
    Ok(MediaFacts {
        format: EncodedFormat::Flac,
        width: None,
        height: None,
        animated: false,
        frame_count: None,
        duration_ms: Some((word & ((1 << 36) - 1)) * 1000 / rate),
        positive_frame_durations: None,
        loop_count: None,
        has_alpha: None,
        bit_depth: Some(((word >> 36) & 31) as u8 + 1),
        lossless: true,
        evidence: MediaEvidence::SelfEncoded,
    })
}
fn video_header_facts() -> MediaFacts {
    MediaFacts {
        format: EncodedFormat::Mp4,
        width: None,
        height: None,
        animated: false,
        frame_count: None,
        duration_ms: None,
        positive_frame_durations: None,
        loop_count: None,
        has_alpha: None,
        bit_depth: None,
        lossless: false,
        evidence: MediaEvidence::HeaderOnly,
    }
}

/// Bounded common ISO-BMFF header inspection. It does not decode video, verify
/// sample tables, qualify a codec/profile or attest playable/valid full content.
/// Authoring must use a qualified PlatformVideoDecoder, never these facts alone.
pub fn inspect_video_bytes(input: &[u8]) -> Result<MediaFacts, MediaError> {
    if input.len() > MAX_MEDIA_BYTES {
        return Err(MediaError::Budget);
    }
    let boxes = mp4_boxes(input)?;
    let brand = boxes
        .first()
        .filter(|(tag, data)| tag == b"ftyp" && data.len() >= 8)
        .ok_or(MediaError::Unsupported)?;
    if !matches!(
        &brand.1[..4],
        b"isom" | b"iso2" | b"mp41" | b"mp42" | b"avc1" | b"M4V " | b"qt  "
    ) {
        return Err(MediaError::Unsupported);
    }
    let mut facts = video_header_facts();
    if let Some((_, moov)) = boxes.iter().find(|(tag, _)| tag == b"moov") {
        for (tag, data) in mp4_boxes(moov)? {
            if &tag == b"mvhd" {
                let (scale, ticks) = match data.first() {
                    Some(0) if data.len() >= 20 => (
                        u32::from_be_bytes(
                            data[12..16].try_into().map_err(|_| MediaError::Invalid)?,
                        ),
                        u64::from(u32::from_be_bytes(
                            data[16..20].try_into().map_err(|_| MediaError::Invalid)?,
                        )),
                    ),
                    Some(1) if data.len() >= 32 => (
                        u32::from_be_bytes(
                            data[20..24].try_into().map_err(|_| MediaError::Invalid)?,
                        ),
                        u64::from_be_bytes(
                            data[24..32].try_into().map_err(|_| MediaError::Invalid)?,
                        ),
                    ),
                    _ => return Err(MediaError::Invalid),
                };
                facts.duration_ms = ticks
                    .checked_mul(1000)
                    .and_then(|n| n.checked_div(u64::from(scale)));
            }
            if &tag == b"trak" && facts.width.is_none() {
                for (tag, data) in mp4_boxes(data)? {
                    if &tag == b"tkhd" {
                        let minimum = if data.first() == Some(&1) { 96 } else { 84 };
                        if data.len() < minimum {
                            return Err(MediaError::Invalid);
                        }
                        let width = u32::from_be_bytes(
                            data[data.len() - 8..data.len() - 4]
                                .try_into()
                                .map_err(|_| MediaError::Invalid)?,
                        ) >> 16;
                        let height = u32::from_be_bytes(
                            data[data.len() - 4..]
                                .try_into()
                                .map_err(|_| MediaError::Invalid)?,
                        ) >> 16;
                        if width > 0 && height > 0 {
                            facts.width = Some(width);
                            facts.height = Some(height);
                        }
                    }
                }
            }
        }
    }
    Ok(facts)
}

/// Inspect only explicitly bounded atom levels, with checked 32/64-bit lengths.
fn mp4_boxes(mut bytes: &[u8]) -> Result<ContainerChunks<'_>, MediaError> {
    let mut result = Vec::new();
    while !bytes.is_empty() {
        let header = bytes.get(..8).ok_or(MediaError::Invalid)?;
        let length = u32::from_be_bytes(header[..4].try_into().map_err(|_| MediaError::Invalid)?);
        let (length, header_len) = match length {
            0 => (bytes.len(), 8),
            1 => (
                usize::try_from(u64::from_be_bytes(
                    bytes
                        .get(8..16)
                        .ok_or(MediaError::Invalid)?
                        .try_into()
                        .map_err(|_| MediaError::Invalid)?,
                ))
                .map_err(|_| MediaError::Budget)?,
                16,
            ),
            n => (n as usize, 8),
        };
        if result.len() >= 1024 || length < header_len {
            return Err(MediaError::Invalid);
        }
        result.push((
            header[4..8].try_into().map_err(|_| MediaError::Invalid)?,
            bytes.get(header_len..length).ok_or(MediaError::Invalid)?,
        ));
        bytes = bytes.get(length..).ok_or(MediaError::Invalid)?;
    }
    Ok(result)
}

/// Owned read lease passed to a platform decoder. Foreign wrappers must retain
/// this object until AVFoundation/MediaCodec work REALLY completes, even if the
/// Rust/foreign future is cancelled. Retaining just its pathname is unsound.
#[cfg(not(target_arch = "wasm32"))]
pub struct VideoInput {
    source: std::sync::Arc<tempfile::NamedTempFile>,
    // Foreign native work retains admission with its read lease, even after timeout.
    _permit: tokio::sync::SemaphorePermit<'static>,
    pub source_sha256: [u8; 32],
    pub size_bytes: u64,
}
#[cfg(not(target_arch = "wasm32"))]
impl VideoInput {
    /// Read-only source contract; never truncate, rewrite, move or delete this file.
    pub fn path(&self) -> &std::path::Path {
        self.source.path()
    }
}

/// Native work budgets. The host must bound its own decoder queues, cancellation,
/// allocations and thread-affine native objects; Rust cannot stop a foreign codec
/// by dropping its future. No full-size frame/media buffers cross the FFI boundary.
#[derive(Debug, Clone, Copy)]
pub struct VideoDecodeLimits {
    pub max_input_bytes: u64,
    pub max_pixels: u64,
    pub max_frames: u32,
    pub max_duration_ms: u64,
    pub thumbnail_edge: u32,
    pub deadline_ms: u32,
}
impl Default for VideoDecodeLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 256 * 1024 * 1024,
            max_pixels: MAX_IMAGE_PIXELS,
            max_frames: 18000,
            max_duration_ms: 600000,
            thumbnail_edge: 320,
            deadline_ms: 10000,
        }
    }
}

/// Optional decoded, display-oriented sRGB RGBA8 thumbnail (not a full-size
/// frame). Hosts must return Unsupported/None rather than silently losing HDR,
/// alpha or color management they cannot handle. Row layout is tightly packed.
#[derive(Debug)]
pub struct DecodedVideoFrame {
    pub width: u32,
    pub height: u32,
    pub rgba: Vec<u8>,
}
/// Decoder-supplied facts are accepted only with HostDecoded evidence and actual
/// dimensions, duration and full-stream frame count, not a metadata-only success.
#[derive(Debug)]
pub struct DecodedVideo {
    pub facts: MediaFacts,
    pub thumbnail: Option<DecodedVideoFrame>,
}

/// Narrow platform integration contract, NOT an implemented iOS/Android codec.
/// Swift owns AVFoundation scheduling/retention; Kotlin owns MediaExtractor/
/// MediaCodec resources and API-24 fallbacks. Implementations must fully validate
/// the source before HostDecoded, join/cancel native work, retain VideoInput
/// through native completion, and marshal thread-affine objects on their owner.
/// Do not remux/re-encode here: common Rust returns the original snapshot exactly.
#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
pub trait PlatformVideoDecoder: Send + Sync {
    async fn inspect_and_decode(
        &self,
        input: std::sync::Arc<VideoInput>,
        limits: VideoDecodeLimits,
        cancel: syntra_runtime::CancelToken,
    ) -> Result<DecodedVideo, MediaError>;
}

/// Validate a real host decoder's bounded result and return exact original video
/// bytes plus an optional encoded PNG thumbnail. No codec means Unsupported;
/// header-only facts never become an authoring result or a "lossless re-encode".
#[cfg(not(target_arch = "wasm32"))]
pub async fn prepare_video_with_decoder(
    path: &std::path::Path,
    decoder: std::sync::Arc<dyn PlatformVideoDecoder>,
    cancel: Option<syntra_runtime::CancelToken>,
) -> Result<PreparedMediaPath, MediaError> {
    prepare_video_with_decoder_inner(
        path,
        decoder,
        cancel,
        SnapshotPolicy::new(Some(VideoDecodeLimits::default().max_input_bytes)),
        #[cfg(test)]
        None,
    )
    .await
}

#[cfg(not(target_arch = "wasm32"))]
async fn prepare_video_with_decoder_inner(
    path: &std::path::Path,
    decoder: std::sync::Arc<dyn PlatformVideoDecoder>,
    cancel: Option<syntra_runtime::CancelToken>,
    policy: SnapshotPolicy,
    #[cfg(test)] gate: Option<std::sync::Arc<SnapshotGate>>,
) -> Result<PreparedMediaPath, MediaError> {
    let permit = NATIVE_MEDIA_WORKERS
        .try_acquire()
        .map_err(|_| MediaError::Busy)?;
    let limits = VideoDecodeLimits::default();
    let cancel = cancel.unwrap_or_default();
    struct Guard {
        token: syntra_runtime::CancelToken,
        armed: bool,
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            if self.armed {
                self.token.cancel();
            }
        }
    }
    let mut guard = Guard {
        token: cancel.clone(),
        armed: true,
    };
    if cancel.is_cancelled() {
        return Err(MediaError::Cancelled);
    }
    if tokio::fs::metadata(path)
        .await
        .map_err(|_| MediaError::Io)?
        .len()
        > limits.max_input_bytes
    {
        return Err(MediaError::Budget);
    }
    #[cfg(test)]
    if let Some(gate) = gate {
        gate.entered.notify_one();
        gate.release.notified().await;
    }
    let prepared =
        prepare_path_with_policy(path, MediaKind::Document, Some(cancel.clone()), policy).await?;
    let size_bytes = tokio::fs::metadata(prepared.path())
        .await
        .map_err(|_| MediaError::Io)?
        .len();
    let input = std::sync::Arc::new(VideoInput {
        source: prepared.output.clone(),
        source_sha256: prepared.source_sha256,
        size_bytes,
        _permit: permit,
    });
    if input.size_bytes > limits.max_input_bytes {
        return Err(MediaError::Budget);
    }
    let result = tokio::select! { biased;
        _ = cancel.cancelled() => return Err(MediaError::Cancelled),
        result = tokio::time::timeout(std::time::Duration::from_millis(u64::from(limits.deadline_ms)), decoder.inspect_and_decode(input.clone(), limits, cancel.clone())) => result.map_err(|_| MediaError::Budget)??,
    };
    if result.facts.evidence != MediaEvidence::HostDecoded
        || result.facts.format != EncodedFormat::Mp4
    {
        return Err(MediaError::Unsupported);
    }
    let width = result
        .facts
        .width
        .filter(|v| *v > 0)
        .ok_or(MediaError::Invalid)?;
    let height = result
        .facts
        .height
        .filter(|v| *v > 0)
        .ok_or(MediaError::Invalid)?;
    if u64::from(width) * u64::from(height) > limits.max_pixels
        || result
            .facts
            .frame_count
            .is_none_or(|n| n == 0 || n > limits.max_frames)
        || result
            .facts
            .duration_ms
            .is_none_or(|n| n == 0 || n > limits.max_duration_ms)
    {
        return Err(MediaError::Budget);
    }
    let frame = result.thumbnail;
    let verify = input.clone();
    let verify_cancel = cancel.clone();
    let (thumbnail, thumbnail_facts) = tokio::task::spawn_blocking(move || {
        let thumbnail = if let Some(frame) = frame {
            if frame.width == 0
                || frame.height == 0
                || frame.width > limits.thumbnail_edge.min(width)
                || frame.height > limits.thumbnail_edge.min(height)
                || frame.rgba.len() as u64 != u64::from(frame.width) * u64::from(frame.height) * 4
            {
                return Err(MediaError::Invalid);
            }
            Some(encode_png(&DynamicImage::ImageRgba8(
                image::RgbaImage::from_raw(frame.width, frame.height, frame.rgba)
                    .ok_or(MediaError::Invalid)?,
            ))?)
        } else {
            None
        };
        let thumbnail_facts = thumbnail.as_ref().map(|t| inspect_image(t)).transpose()?;
        if verify_cancel.is_cancelled() {
            return Err(MediaError::Cancelled);
        }
        // The host gets a read lease only. Verify it did not mutate source bytes.
        use std::io::Read;
        let mut reader = verify.source.reopen().map_err(|_| MediaError::Io)?;
        let mut hash = Sha256::new();
        let mut buffer = [0; 65536];
        let mut size = 0;
        loop {
            if verify_cancel.is_cancelled() {
                return Err(MediaError::Cancelled);
            }
            let n = reader.read(&mut buffer).map_err(|_| MediaError::Io)?;
            if n == 0 {
                break;
            }
            size += n as u64;
            if size > verify.size_bytes {
                return Err(MediaError::SourceChanged);
            }
            hash.update(&buffer[..n]);
        }
        if size != verify.size_bytes || <[u8; 32]>::from(hash.finalize()) != verify.source_sha256 {
            return Err(MediaError::SourceChanged);
        }
        Ok::<_, MediaError>((thumbnail, thumbnail_facts))
    })
    .await
    .map_err(|_| MediaError::Codec)??;
    guard.armed = false;
    Ok(PreparedMediaPath {
        output: prepared.output,
        source_sha256: prepared.source_sha256,
        output_sha256: prepared.output_sha256,
        profile: "tinkle-video-passthrough-v1",
        mime_type: "video/mp4",
        thumbnail,
        disposition: PreparationDisposition::ExactOriginal,
        main_facts: Some(result.facts),
        thumbnail_facts,
    })
}

/// Integer-only FLAC path. No conversion to float, channel remix or sample-rate change.
fn encode_pcm(input: &[u8], format: PcmFormat) -> Result<Vec<u8>, MediaError> {
    use flacenc::{component::BitRepr, error::Verify};
    if !(1..=2).contains(&format.channels)
        || !(8000..=192000).contains(&format.sample_rate)
        || !matches!(format.bits_per_sample, 8 | 16 | 24)
    {
        return Err(MediaError::Invalid);
    }
    let stride = usize::from(format.bits_per_sample / 8);
    if input.is_empty()
        || !input
            .len()
            .is_multiple_of(stride * usize::from(format.channels))
    {
        return Err(MediaError::Invalid);
    }
    let samples: Vec<i32> = input
        .chunks_exact(stride)
        .map(|s| match stride {
            1 => i32::from(s[0] as i8),
            2 => i32::from(i16::from_le_bytes([s[0], s[1]])),
            _ => i32::from_le_bytes([s[0], s[1], s[2], if s[2] & 128 != 0 { 255 } else { 0 }]),
        })
        .collect();
    let config = flacenc::config::Encoder::default()
        .into_verified()
        .map_err(|_| MediaError::Codec)?;
    let source = flacenc::source::MemSource::from_samples(
        &samples,
        usize::from(format.channels),
        usize::from(format.bits_per_sample),
        format.sample_rate as usize,
    );
    let mut stream = flacenc::encode_with_fixed_block_size(&config, source, config.block_size)
        .map_err(|_| MediaError::Codec)?;
    // STREAMINFO describes the fixed nominal block size, not the short final
    // frame (which may contain <16 samples without adding synthetic PCM).
    stream
        .stream_info_mut()
        .set_block_sizes(config.block_size, config.block_size)
        .map_err(|_| MediaError::Codec)?;
    let mut sink = flacenc::bitsink::ByteSink::new();
    stream.write(&mut sink).map_err(|_| MediaError::Codec)?;
    if sink.as_slice().len() > MAX_MEDIA_BYTES {
        return Err(MediaError::Budget);
    }
    Ok(sink.as_slice().to_vec())
}

/// Native disk result owns its private temporary artifact until the caller releases it.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct PreparedMediaPath {
    output: std::sync::Arc<tempfile::NamedTempFile>,
    pub source_sha256: [u8; 32],
    pub output_sha256: [u8; 32],
    pub profile: &'static str,
    pub mime_type: &'static str,
    pub thumbnail: Option<Vec<u8>>,
    pub disposition: PreparationDisposition,
    pub main_facts: Option<MediaFacts>,
    pub thumbnail_facts: Option<MediaFacts>,
}
#[cfg(not(target_arch = "wasm32"))]
impl PreparedMediaPath {
    /// Valid only while this owner lives. upload_path snapshots it independently.
    pub fn path(&self) -> &std::path::Path {
        self.output.path()
    }
}

/// Bounded native worker admission. Large documents are copied/hashed with a 64 KiB
/// buffer, without codec loading. Other inputs are explicitly capped at 32 MiB.
#[cfg(not(target_arch = "wasm32"))]
pub async fn prepare_path(
    path: &std::path::Path,
    kind: MediaKind,
    cancel: Option<syntra_runtime::CancelToken>,
) -> Result<PreparedMediaPath, MediaError> {
    prepare_path_with_policy(path, kind, cancel, SnapshotPolicy::new(None)).await
}

/// Client preparation uses its storage directory, never the imported source's parent.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn prepare_path_in(
    path: &std::path::Path,
    scratch_root: &std::path::Path,
    kind: MediaKind,
    cancel: Option<syntra_runtime::CancelToken>,
) -> Result<PreparedMediaPath, MediaError> {
    let mut policy = SnapshotPolicy::new(None);
    policy.scratch_root = scratch_root.to_path_buf();
    prepare_path_with_policy(path, kind, cancel, policy).await
}

/// Internal copy policy is independent of transform intent: video can pass exact
/// original bytes while still enforcing its own opened-file snapshot ceiling.
#[cfg(not(target_arch = "wasm32"))]
struct SnapshotPolicy {
    max_input_bytes: Option<u64>,
    scratch_root: std::path::PathBuf,
    #[cfg(test)]
    audit: Option<std::sync::Arc<SnapshotAudit>>,
}
#[cfg(not(target_arch = "wasm32"))]
impl SnapshotPolicy {
    fn new(max_input_bytes: Option<u64>) -> Self {
        Self {
            max_input_bytes,
            scratch_root: std::env::temp_dir(),
            #[cfg(test)]
            audit: None,
        }
    }
}

/// Per-operation scheduling/metrics used only by actual TOCTOU regressions.
#[cfg(all(test, not(target_arch = "wasm32")))]
#[derive(Default)]
pub struct SnapshotGate {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}
#[cfg(all(test, not(target_arch = "wasm32")))]
#[derive(Default)]
pub struct SnapshotAudit {
    pub created: std::sync::atomic::AtomicU64,
    pub written: std::sync::atomic::AtomicU64,
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub async fn test_video_snapshot_race(
    path: &std::path::Path,
    decoder: std::sync::Arc<dyn PlatformVideoDecoder>,
    gate: std::sync::Arc<SnapshotGate>,
    audit: std::sync::Arc<SnapshotAudit>,
) -> Result<PreparedMediaPath, MediaError> {
    let mut policy = SnapshotPolicy::new(Some(VideoDecodeLimits::default().max_input_bytes));
    policy.audit = Some(audit);
    prepare_video_with_decoder_inner(path, decoder, None, policy, Some(gate)).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn prepare_path_with_policy(
    path: &std::path::Path,
    kind: MediaKind,
    cancel: Option<syntra_runtime::CancelToken>,
    policy: SnapshotPolicy,
) -> Result<PreparedMediaPath, MediaError> {
    if matches!(kind, MediaKind::Video) {
        return Err(MediaError::Unsupported);
    }
    static WORKERS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);
    let permit = WORKERS.try_acquire().map_err(|_| MediaError::Busy)?;
    let path = path.to_path_buf();
    let token = cancel.unwrap_or_default();
    // Join the blocking worker even after cancellation. A dropped future leaves
    // the permit and temporary artifact with the worker until its bounded exit.
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        use std::io::{Read, Seek, Write};
        let mut file = std::fs::File::open(path).map_err(|_| MediaError::Io)?;
        let before = file.metadata().map_err(|_| MediaError::Io)?;
        let maximum = if matches!(kind, MediaKind::Document) {
            policy.max_input_bytes
        } else {
            Some(
                policy
                    .max_input_bytes
                    .unwrap_or(MAX_MEDIA_BYTES as u64)
                    .min(MAX_MEDIA_BYTES as u64),
            )
        };
        if !before.is_file() {
            return Err(MediaError::Invalid);
        }
        // The pathname may have changed after the advisory video stat. Check the
        // actual opened file before even creating an output, not after copying it.
        if maximum.is_some_and(|limit| before.len() > limit) {
            return Err(MediaError::Budget);
        }
        let mut output =
            tempfile::NamedTempFile::new_in(&policy.scratch_root).map_err(|_| MediaError::Io)?;
        #[cfg(test)]
        if let Some(audit) = &policy.audit {
            audit
                .created
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        let mut hash = Sha256::new();
        let mut buffer = [0; 65536];
        let mut len = 0u64;
        loop {
            if token.is_cancelled() {
                return Err(MediaError::Cancelled);
            }
            let n = file.read(&mut buffer).map_err(|_| MediaError::Io)?;
            if n == 0 {
                break;
            }
            len = len.checked_add(n as u64).ok_or(MediaError::Budget)?;
            // Enforce the same ceiling on growth/read progress BEFORE each write.
            if maximum.is_some_and(|limit| len > limit) {
                return Err(MediaError::Budget);
            }
            if len > before.len() {
                return Err(MediaError::SourceChanged);
            }
            hash.update(&buffer[..n]);
            output.write_all(&buffer[..n]).map_err(|_| MediaError::Io)?;
            #[cfg(test)]
            if let Some(audit) = &policy.audit {
                audit
                    .written
                    .fetch_add(n as u64, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let source_sha256: [u8; 32] = hash.finalize().into();
        file.rewind().map_err(|_| MediaError::Io)?;
        let mut check = Sha256::new();
        let mut proof = (&mut file).take(before.len().saturating_add(1));
        loop {
            if token.is_cancelled() {
                return Err(MediaError::Cancelled);
            }
            let n = proof.read(&mut buffer).map_err(|_| MediaError::Io)?;
            if n == 0 {
                break;
            }
            check.update(&buffer[..n]);
        }
        if len != before.len()
            || source_sha256 != <[u8; 32]>::from(check.finalize())
            || before.modified().ok()
                != file.metadata().map_err(|_| MediaError::Io)?.modified().ok()
        {
            return Err(MediaError::SourceChanged);
        }
        let (output_sha256, mime_type, thumbnail, disposition, main_facts, thumbnail_facts) =
            if matches!(kind, MediaKind::Document) {
                (
                    source_sha256,
                    "application/octet-stream",
                    None,
                    PreparationDisposition::ExactOriginal,
                    None,
                    None,
                )
            } else {
                output.rewind().map_err(|_| MediaError::Io)?;
                let mut input = Vec::new();
                output.read_to_end(&mut input).map_err(|_| MediaError::Io)?;
                let result = prepare_bytes(&input, kind)?;
                if token.is_cancelled() {
                    return Err(MediaError::Cancelled);
                }
                output
                    .as_file_mut()
                    .set_len(0)
                    .map_err(|_| MediaError::Io)?;
                output.rewind().map_err(|_| MediaError::Io)?;
                output
                    .write_all(&result.bytes)
                    .map_err(|_| MediaError::Io)?;
                (
                    result.output_sha256,
                    result.mime_type,
                    result.thumbnail,
                    result.disposition,
                    result.main_facts,
                    result.thumbnail_facts,
                )
            };
        output.as_file().sync_all().map_err(|_| MediaError::Io)?;
        Ok(PreparedMediaPath {
            output: std::sync::Arc::new(output),
            source_sha256,
            output_sha256,
            profile: PREPARATION_PROFILE,
            mime_type,
            thumbnail,
            disposition,
            main_facts,
            thumbnail_facts,
        })
    })
    .await
    .map_err(|_| MediaError::Codec)?
}

/// Explicit desktop FFmpeg adapter. It never searches PATH or invokes a shell.
/// iOS/Android must supply a separately qualified native-codec integration; an
/// installed host binary is not evidence that an application can spawn it there.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct FfmpegAdapter {
    executable: std::path::PathBuf,
}

#[cfg(not(target_arch = "wasm32"))]
impl FfmpegAdapter {
    /// Require a host-provisioned absolute executable. Availability is checked by
    /// the real child process on each operation, not inferred from the pathname.
    pub fn new(executable: std::path::PathBuf) -> Result<Self, MediaError> {
        if cfg!(any(target_os = "ios", target_os = "android")) {
            return Err(MediaError::Unsupported);
        }
        if !executable.is_absolute() || !executable.is_file() {
            return Err(MediaError::Unsupported);
        }
        Ok(Self {
            executable: executable
                .canonicalize()
                .map_err(|_| MediaError::Unsupported)?,
        })
    }

    /// Remux the first video and optional first audio stream into MP4 using packet
    /// copy (no transcoding), and extract one <=320px PNG frame. Incompatible input
    /// codecs/containers fail explicitly. Input is a private <=256 MiB snapshot;
    /// commands have 10s wall budgets, one codec thread, restricted protocols and
    /// a 256 MiB output ceiling. This is not an OS sandbox for hostile codec bugs.
    pub async fn prepare_video(
        &self,
        source: &std::path::Path,
        cancel: Option<syntra_runtime::CancelToken>,
    ) -> Result<PreparedMediaPath, MediaError> {
        static WORKERS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
        let permit = WORKERS.try_acquire().map_err(|_| MediaError::Busy)?;
        let source = source.to_path_buf();
        let adapter = self.clone();
        let token = cancel.unwrap_or_default();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            use std::io::{Read, Seek, Write};
            const MAX_VIDEO_BYTES: u64 = 256 * 1024 * 1024;
            if token.is_cancelled() {
                return Err(MediaError::Cancelled);
            }
            let directory = tempfile::tempdir().map_err(|_| MediaError::Io)?;
            let mut input = std::fs::File::open(source).map_err(|_| MediaError::Io)?;
            let metadata = input.metadata().map_err(|_| MediaError::Io)?;
            if !metadata.is_file() || metadata.len() > MAX_VIDEO_BYTES {
                return Err(MediaError::Budget);
            }
            let mut snapshot =
                tempfile::NamedTempFile::new_in(directory.path()).map_err(|_| MediaError::Io)?;
            let mut hash = Sha256::new();
            let mut buffer = [0; 65536];
            let mut size = 0;
            loop {
                if token.is_cancelled() {
                    return Err(MediaError::Cancelled);
                }
                let n = input.read(&mut buffer).map_err(|_| MediaError::Io)?;
                if n == 0 {
                    break;
                }
                size += n as u64;
                if size > metadata.len() {
                    return Err(MediaError::SourceChanged);
                }
                hash.update(&buffer[..n]);
                snapshot
                    .write_all(&buffer[..n])
                    .map_err(|_| MediaError::Io)?;
            }
            let source_sha256 = hash.finalize().into();
            input.rewind().map_err(|_| MediaError::Io)?;
            let mut check = Sha256::new();
            let mut proof = (&mut input).take(metadata.len().saturating_add(1));
            loop {
                if token.is_cancelled() {
                    return Err(MediaError::Cancelled);
                }
                let n = proof.read(&mut buffer).map_err(|_| MediaError::Io)?;
                if n == 0 {
                    break;
                }
                check.update(&buffer[..n]);
            }
            if size != metadata.len()
                || <[u8; 32]>::from(check.finalize()) != source_sha256
                || metadata.modified().ok()
                    != input
                        .metadata()
                        .map_err(|_| MediaError::Io)?
                        .modified()
                        .ok()
            {
                return Err(MediaError::SourceChanged);
            }
            let output = tempfile::Builder::new()
                .suffix(".mp4")
                .tempfile()
                .map_err(|_| MediaError::Io)?;
            let thumbnail = directory.path().join("frame.png");
            let common = [
                "-nostdin",
                "-v",
                "error",
                "-y",
                "-max_alloc",
                "16777216",
                "-threads",
                "1",
                "-protocol_whitelist",
                "file,pipe",
                "-format_whitelist",
                "mov,matroska,webm",
                "-i",
            ];
            let mut remux = adapter.command(directory.path());
            remux
                .args(common)
                .arg(snapshot.path())
                .args([
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a:0?",
                    "-map_metadata",
                    "-1",
                    "-c",
                    "copy",
                    "-fs",
                    "268435456",
                    "-f",
                    "mp4",
                ])
                .arg(output.path());
            run_child(remux, &token)?;
            if output
                .as_file()
                .metadata()
                .map_err(|_| MediaError::Io)?
                .len()
                >= MAX_VIDEO_BYTES
            {
                return Err(MediaError::Budget);
            }
            // `-fs` may finish successfully after truncating at a packet boundary.
            // Verify selected encoded packet hashes on both sides before calling
            // the remux lossless, rather than trusting exit status or file size.
            let original_hash = directory.path().join("original.hash");
            let remux_hash = directory.path().join("remux.hash");
            for (input, digest) in [
                (snapshot.path(), original_hash.as_path()),
                (output.path(), remux_hash.as_path()),
            ] {
                let mut verify = adapter.command(directory.path());
                verify
                    .args(common)
                    .arg(input)
                    .args([
                        "-map",
                        "0:v:0",
                        "-map",
                        "0:a:0?",
                        "-c",
                        "copy",
                        "-hash",
                        "sha256",
                        "-f",
                        "streamhash",
                    ])
                    .arg(digest);
                run_child(verify, &token)?;
            }
            if std::fs::read(original_hash).map_err(|_| MediaError::Io)?
                != std::fs::read(remux_hash).map_err(|_| MediaError::Io)?
            {
                return Err(MediaError::Codec);
            }
            let mut frame = adapter.command(directory.path());
            frame
                .args(common)
                .arg(snapshot.path())
                .args([
                    "-map",
                    "0:v:0",
                    "-frames:v",
                    "1",
                    "-threads",
                    "1",
                    "-vf",
                    "scale=w=min(320\\,iw):h=min(320\\,ih):force_original_aspect_ratio=decrease",
                    "-fs",
                    "1048576",
                    "-c:v",
                    "png",
                    "-f",
                    "image2",
                ])
                .arg(&thumbnail);
            run_child(frame, &token)?;
            let thumbnail = std::fs::read(thumbnail).map_err(|_| MediaError::Codec)?;
            if thumbnail.len() > 1024 * 1024 {
                return Err(MediaError::Budget);
            }
            let image = image::load_from_memory_with_format(&thumbnail, ImageFormat::Png)
                .map_err(|_| MediaError::Codec)?;
            if image.width().max(image.height()) > 320 {
                return Err(MediaError::Codec);
            }
            let mut file = output.reopen().map_err(|_| MediaError::Io)?;
            file.rewind().map_err(|_| MediaError::Io)?;
            let mut hash = Sha256::new();
            loop {
                if token.is_cancelled() {
                    return Err(MediaError::Cancelled);
                }
                let n = file.read(&mut buffer).map_err(|_| MediaError::Io)?;
                if n == 0 {
                    break;
                }
                hash.update(&buffer[..n]);
            }
            output.as_file().sync_all().map_err(|_| MediaError::Io)?;
            let thumbnail_facts = Some(inspect_image(&thumbnail)?);
            Ok(PreparedMediaPath {
                output: std::sync::Arc::new(output),
                source_sha256,
                output_sha256: hash.finalize().into(),
                profile: "tinkle-video-v1-ffmpeg-copy-mp4",
                mime_type: "video/mp4",
                thumbnail: Some(thumbnail),
                disposition: PreparationDisposition::PreservedUnsupported,
                // Packet-copy verification proves no selected packet was lost;
                // it does not qualify all video decoding or profile metadata.
                main_facts: Some(video_header_facts()),
                thumbnail_facts,
            })
        })
        .await
        .map_err(|_| MediaError::Codec)?
    }

    /// No inherited environment, stdin, diagnostic streams, or shell expansion.
    fn command(&self, directory: &std::path::Path) -> std::process::Command {
        let mut command = std::process::Command::new(&self.executable);
        command
            .env_clear()
            .current_dir(directory)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        command
    }
}

/// Poll a real child within a wall budget, always killing AND reaping on failure.
#[cfg(not(target_arch = "wasm32"))]
fn run_child(
    mut command: std::process::Command,
    token: &syntra_runtime::CancelToken,
) -> Result<(), MediaError> {
    let mut child = command.spawn().map_err(|_| MediaError::Unsupported)?;
    let cutoff = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let error = if token.is_cancelled() {
            Some(MediaError::Cancelled)
        } else if std::time::Instant::now() >= cutoff {
            Some(MediaError::Budget)
        } else {
            None
        };
        if let Some(error) = error {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                return if status.success() {
                    Ok(())
                } else {
                    Err(MediaError::Codec)
                }
            }
            Ok(None) => std::thread::sleep(std::time::Duration::from_millis(10)),
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(MediaError::Codec);
            }
        }
    }
}
