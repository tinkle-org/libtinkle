//! Local native encoding with SDK-owned source/output leases. No upload or cache mutation.
use super::*;
use std::{io::Read, path::Path, sync::Arc};

/// Versioned processing recipe, not an assertion of server validation or deterministic bytes.
pub const NATIVE_PREPARATION_PROFILE: &str = "tinkle-native-media-v2";
/// General image MAIN limits are decimal bytes and per-axis pixels, not allocation ceilings.
pub const IMAGE_MAIN_BYTES: u64 = 10_000_000;
pub const IMAGE_MAIN_AXIS: u32 = 10_000;

/// Explicit purpose. Audio and recorded PCM voice remain distinct after encoding.
#[derive(Debug, Clone, Copy)]
pub enum NativeMediaKind {
    Image,
    Video,
    Audio,
    Voice(PcmFormat),
}

/// Resource limits do not expand the public media profile. Native codecs must check between calls.
#[derive(Debug, Clone, Copy)]
pub struct NativePreparationLimits {
    pub max_input_bytes: u64,
    pub max_output_bytes: u64,
    pub max_duration_ms: u64,
    pub max_frames: u32,
    pub deadline_ms: u32,
}
impl Default for NativePreparationLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 256 * 1024 * 1024,
            max_output_bytes: 256 * 1024 * 1024,
            max_duration_ms: 600_000,
            max_frames: 18_000,
            deadline_ms: 120_000,
        }
    }
}

/// Source and writable destination ownership last until the actual native join, not waiter exit.
pub struct NativeMediaInput {
    source: Arc<tempfile::NamedTempFile>,
    directory: tempfile::TempDir,
    _permit: tokio::sync::SemaphorePermit<'static>,
    pub source_sha256: [u8; 32],
    pub size_bytes: u64,
    pub kind: NativeMediaKind,
    pub limits: NativePreparationLimits,
    cancel: syntra_runtime::CancelToken,
}
impl NativeMediaInput {
    /// Private frozen file, read only; retaining the path alone does not retain this lease.
    pub fn source_path(&self) -> &Path {
        self.source.path()
    }
    /// Only this destination is accepted. Hosts never supply an arbitrary output pathname.
    pub fn output_path(&self) -> std::path::PathBuf {
        self.directory.path().join("main.mp4")
    }
    /// Cooperative cancellation must be followed by native teardown before callback completion.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

/// Measured output facts from native encoding and complete output decode, not generic host evidence.
/// Image alpha and animation must be preserved; audio tracks must be AAC-LC only.
#[derive(Debug, Clone)]
pub struct NativeMediaOutput {
    pub width: u32,
    pub height: u32,
    pub frame_count: u32,
    pub duration_ms: u64,
    pub audio_channels: u8,
    pub audio_sample_rate: u32,
    pub audio_samples: u64,
    /// Voice only: 100 unsigned linear-amplitude peak bins, derived from source PCM in Rust.
    pub waveform: Vec<u8>,
}

/// Native encoders must fully close/join readers, writers and finalization before returning.
/// This callback writes only the invocation's output path and returns small owned facts, not media buffers.
#[async_trait::async_trait]
pub trait PlatformMediaPreparer: Send + Sync {
    async fn prepare(&self, input: Arc<NativeMediaInput>) -> Result<NativeMediaOutput, MediaError>;
}

/// Stable owned artifact: persist/adopt these exact bytes once; never encode again on upload retry.
#[derive(Clone)]
pub struct NativePreparedMedia {
    pub main: PreparedMediaPath,
    pub kind: NativeMediaKind,
    pub output: NativeMediaOutput,
}

/// Integer-pixel, no-upscale fitting. Both orientations use the same long/short edge policy.
/// Even rounding is required for 4:2:0; aspect error is bounded by one output pixel per axis.
pub fn video_output_dimensions(width: u32, height: u32) -> Result<(u32, u32), MediaError> {
    if width < 2 || height < 2 {
        return Err(MediaError::Unsupported);
    }
    let long = width.max(height);
    let short = width.min(height);
    let (numerator, denominator) = if u64::from(long) * 1080 > u64::from(short) * 1920 {
        (1920.min(long), long)
    } else {
        (1080.min(short), short)
    };
    let fit = |v: u32| ((u64::from(v) * u64::from(numerator) / u64::from(denominator)) as u32) & !1;
    let result = (fit(width), fit(height));
    if result.0 == 0 || result.1 == 0 {
        return Err(MediaError::Unsupported);
    }
    Ok(result)
}

/// Shared retry schedule for lossless encoding: shrink dimensions, never change encoder quality.
/// Decode/downsample into at most 16 Mi pixels (64 MiB RGBA8), including 100-Mpixel sources.
/// This resource-driven initial resize is intentional even for highly compressible sources.
pub fn image_output_dimensions(
    width: u32,
    height: u32,
    attempt: u32,
) -> Result<(u32, u32), MediaError> {
    if width == 0 || height == 0 || attempt > 64 {
        return Err(MediaError::Budget);
    }
    let long = width.max(height);
    let mut edge = long.min(IMAGE_MAIN_AXIS);
    let fit_at =
        |v: u32, edge: u32| (u64::from(v) * u64::from(edge) / u64::from(long)).max(1) as u32;
    // Integer search avoids float overflow/rounding and also handles extreme aspect ratios.
    let (mut low, mut high) = (1, edge);
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        if u64::from(fit_at(width, mid)) * u64::from(fit_at(height, mid)) <= MAX_IMAGE_PIXELS {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    edge = low;
    for _ in 0..attempt {
        edge = (edge * 3 / 4).max(1);
    }
    Ok((fit_at(width, edge), fit_at(height, edge)))
}

/// Cancellation signals, but never aborts a foreign future: its native resources must join first.
struct CancelOnDrop(syntra_runtime::CancelToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Encode on a retained worker. A cancelled/dropped waiter cannot delete input/output under a codec.
/// Source verification precedes output inspection, including on callbacks that report errors.
/// Completion can win a late cancellation race. An artifact is local evidence, not upload permission.
pub async fn prepare_native_media(
    path: &Path,
    kind: NativeMediaKind,
    preparer: Arc<dyn PlatformMediaPreparer>,
    cancel: Option<syntra_runtime::CancelToken>,
) -> Result<NativePreparedMedia, MediaError> {
    prepare_native_media_inner(path, kind, preparer, cancel, std::env::temp_dir()).await
}

/// Prepare using an existing host-owned private writable directory (e.g. Android cacheDir).
/// The root must be absolute and remain available through actual native completion and release
/// of the returned artifact. No source parent, global TMPDIR, or fallback root is used.
pub async fn prepare_native_media_in(
    path: &Path,
    scratch_root: &Path,
    kind: NativeMediaKind,
    preparer: Arc<dyn PlatformMediaPreparer>,
    cancel: Option<syntra_runtime::CancelToken>,
) -> Result<NativePreparedMedia, MediaError> {
    if !scratch_root.is_absolute() {
        return Err(MediaError::Invalid);
    }
    prepare_native_media_inner(path, kind, preparer, cancel, scratch_root.to_path_buf()).await
}

async fn prepare_native_media_inner(
    path: &Path,
    kind: NativeMediaKind,
    preparer: Arc<dyn PlatformMediaPreparer>,
    cancel: Option<syntra_runtime::CancelToken>,
    scratch_root: std::path::PathBuf,
) -> Result<NativePreparedMedia, MediaError> {
    let token = cancel.unwrap_or_default();
    let _guard = CancelOnDrop(token.clone());
    if token.is_cancelled() {
        return Err(MediaError::Cancelled);
    }
    if let NativeMediaKind::Voice(format) = kind {
        if format.bits_per_sample != 16
            || !(1..=2).contains(&format.channels)
            || !(8000..=48000).contains(&format.sample_rate)
        {
            return Err(MediaError::Unsupported);
        }
    }
    let permit = NATIVE_MEDIA_WORKERS
        .try_acquire()
        .map_err(|_| MediaError::Busy)?;
    let limits = NativePreparationLimits::default();
    let mut policy = SnapshotPolicy::new(Some(limits.max_input_bytes));
    policy.scratch_root = scratch_root.clone();
    let prepared =
        prepare_path_with_policy(path, MediaKind::Document, Some(token.clone()), policy).await?;
    if token.is_cancelled() {
        return Err(MediaError::Cancelled);
    }
    let size_bytes = prepared
        .output
        .as_file()
        .metadata()
        .map_err(|_| MediaError::Io)?
        .len();
    if let NativeMediaKind::Voice(format) = kind {
        let stride = u64::from(format.channels) * 2;
        if size_bytes == 0 || size_bytes % stride != 0 {
            return Err(MediaError::Invalid);
        }
        if size_bytes / stride * 1000 > u64::from(format.sample_rate) * limits.max_duration_ms {
            return Err(MediaError::Budget);
        }
    }
    let (directory, permit) = tokio::task::spawn_blocking(move || {
        // The worker keeps admission even if its waiter is dropped during directory I/O.
        let permit = permit;
        let mut builder = tempfile::Builder::new();
        builder.prefix("tinkle-media-");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o700));
        }
        let directory = builder
            .tempdir_in(scratch_root)
            .map_err(|_| MediaError::Io)?;
        Ok::<_, MediaError>((directory, permit))
    })
    .await
    .map_err(|_| MediaError::Codec)??;
    let input = Arc::new(NativeMediaInput {
        source: prepared.output,
        source_sha256: prepared.source_sha256,
        size_bytes,
        kind,
        limits,
        directory,
        _permit: permit,
        cancel: token.clone(),
    });
    // Retained task is deliberate: dropping its JoinHandle does not abort native work.
    let worker_token = token.clone();
    let mut worker = tokio::spawn(async move {
        let token = worker_token;
        if token.is_cancelled() {
            return Err(MediaError::Cancelled);
        }
        if matches!(kind, NativeMediaKind::Image) {
            let lease = input.clone();
            if let Some(result) =
                tokio::task::spawn_blocking(move || prepare_shared_animation(&lease))
                    .await
                    .map_err(|_| MediaError::Codec)??
            {
                if token.is_cancelled() {
                    return Err(MediaError::Cancelled);
                }
                return Ok(result);
            }
        }
        let result = preparer.prepare(input.clone()).await;
        let check = input.clone();
        // Do not use cancellation to skip the source-integrity check after host execution.
        tokio::task::spawn_blocking(move || {
            if hash_file(check.source.as_file(), check.size_bytes)? != check.source_sha256 {
                return Err(MediaError::SourceChanged);
            }
            Ok::<_, MediaError>(())
        })
        .await
        .map_err(|_| MediaError::Codec)??;
        if token.is_cancelled() {
            return Err(MediaError::Cancelled);
        }
        let mut facts = result?;
        validate_native_facts(kind, &facts, limits)?;
        if let NativeMediaKind::Voice(format) = kind {
            if facts.audio_samples != size_bytes / (u64::from(format.channels) * 2)
                || facts.audio_channels != format.channels
                || facts.audio_sample_rate != format.sample_rate
            {
                return Err(MediaError::Invalid);
            }
        }
        let path = input.output_path();
        let meta = std::fs::symlink_metadata(&path).map_err(|_| MediaError::Io)?;
        if !meta.is_file() || meta.file_type().is_symlink() {
            return Err(MediaError::Invalid);
        }
        let ceiling = if matches!(kind, NativeMediaKind::Image) {
            IMAGE_MAIN_BYTES
        } else {
            limits.max_output_bytes
        };
        if meta.len() == 0 || meta.len() > ceiling {
            return Err(MediaError::Budget);
        }
        let mut policy = SnapshotPolicy::new(Some(ceiling));
        // The random snapshot must outlive the callback directory, separate from its destination.
        policy.scratch_root = input
            .source_path()
            .parent()
            .ok_or(MediaError::Io)?
            .to_path_buf();
        let mut main =
            prepare_path_with_policy(&path, MediaKind::Document, Some(token.clone()), policy)
                .await?;
        let artifact = main.output.clone();
        let measured = facts.clone();
        let image_facts = tokio::task::spawn_blocking(move || {
            verify_output_container(artifact.as_file(), kind, &measured)
        })
        .await
        .map_err(|_| MediaError::Codec)??;
        // The exact stable snapshot is the artifact; foreign code never owns this new output path.
        main.source_sha256 = input.source_sha256;
        main.profile = NATIVE_PREPARATION_PROFILE;
        main.disposition = PreparationDisposition::NativeEncoded;
        main.main_facts = image_facts;
        main.mime_type = match kind {
            NativeMediaKind::Image => "image/png",
            NativeMediaKind::Video => "video/mp4",
            NativeMediaKind::Audio | NativeMediaKind::Voice(_) => "audio/mp4",
        };
        if let NativeMediaKind::Voice(format) = kind {
            let source = input.source.clone();
            facts.waveform =
                tokio::task::spawn_blocking(move || pcm_waveform(source.as_file(), format))
                    .await
                    .map_err(|_| MediaError::Codec)??;
        } else if !facts.waveform.is_empty() {
            return Err(MediaError::Invalid);
        }
        if token.is_cancelled() {
            return Err(MediaError::Cancelled);
        }
        Ok(NativePreparedMedia {
            main,
            kind,
            output: facts,
        })
    });
    let failure = tokio::select! { biased;
        _ = token.cancelled() => MediaError::Cancelled,
        _ = tokio::time::sleep(std::time::Duration::from_millis(u64::from(limits.deadline_ms))) => MediaError::Budget,
        result = &mut worker => return result.map_err(|_| MediaError::Codec)?,
    };
    token.cancel();
    // Bounded cooperative join. A stuck OS call stays quarantined in the retained task with its
    // lease/admission; neither cancellation nor timeout authorizes deleting files underneath it.
    let _ = tokio::time::timeout(std::time::Duration::from_millis(250), &mut worker).await;
    Err(failure)
}

/// Preserve animation with the shared compositing/mux implementation, never a platform first-frame
/// thumbnail API. Scan only container headers before deciding whether bounded whole-input decoding fits.
fn prepare_shared_animation(
    input: &NativeMediaInput,
) -> Result<Option<NativePreparedMedia>, MediaError> {
    use std::io::{Seek, SeekFrom, Write};
    let mut source = input.source.reopen().map_err(|_| MediaError::Io)?;
    let mut header = [0; 12];
    if source.read(&mut header).map_err(|_| MediaError::Io)? < 12 {
        return Ok(None);
    }
    let gif = header.starts_with(b"GIF87a") || header.starts_with(b"GIF89a");
    let png = header.starts_with(b"\x89PNG\r\n\x1a\n");
    let webp = &header[..4] == b"RIFF" && &header[8..12] == b"WEBP";
    let mut animated = gif;
    if png || webp {
        let mut offset = if png { 8 } else { 12 };
        for _ in 0..4096 {
            if offset >= input.size_bytes {
                break;
            }
            source
                .seek(SeekFrom::Start(offset))
                .map_err(|_| MediaError::Io)?;
            let mut chunk = [0; 8];
            source
                .read_exact(&mut chunk)
                .map_err(|_| MediaError::Invalid)?;
            let length = if png {
                u32::from_be_bytes(chunk[..4].try_into().map_err(|_| MediaError::Invalid)?)
            } else {
                u32::from_le_bytes(chunk[4..].try_into().map_err(|_| MediaError::Invalid)?)
            };
            if (png && &chunk[4..] == b"acTL") || (webp && &chunk[..4] == b"ANIM") {
                animated = true;
                break;
            }
            offset = offset
                .checked_add(u64::from(length) + if png { 12 } else { 8 + u64::from(length % 2) })
                .ok_or(MediaError::Invalid)?;
            if offset > input.size_bytes {
                return Err(MediaError::Invalid);
            }
        }
    }
    if !animated {
        return Ok(None);
    }
    if input.size_bytes > MAX_MEDIA_BYTES as u64 {
        return Err(MediaError::Budget);
    }
    source
        .seek(SeekFrom::Start(0))
        .map_err(|_| MediaError::Io)?;
    let mut bytes = Vec::new();
    source
        .take(MAX_MEDIA_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| MediaError::Io)?;
    if bytes.len() as u64 != input.size_bytes
        || <[u8; 32]>::from(Sha256::digest(&bytes)) != input.source_sha256
    {
        return Err(MediaError::SourceChanged);
    }
    if input.is_cancelled() {
        return Err(MediaError::Cancelled);
    }
    let result = prepare_bytes(&bytes, MediaKind::Image)?;
    let facts = result.main_facts.as_ref().ok_or(MediaError::Invalid)?;
    let output = NativeMediaOutput {
        width: facts.width.ok_or(MediaError::Invalid)?,
        height: facts.height.ok_or(MediaError::Invalid)?,
        frame_count: facts.frame_count.ok_or(MediaError::Invalid)?,
        duration_ms: facts.duration_ms.unwrap_or(0),
        audio_channels: 0,
        audio_sample_rate: 0,
        audio_samples: 0,
        waveform: Vec::new(),
    };
    let mut file =
        tempfile::NamedTempFile::new_in(input.source_path().parent().ok_or(MediaError::Io)?)
            .map_err(|_| MediaError::Io)?;
    file.write_all(&result.bytes).map_err(|_| MediaError::Io)?;
    file.as_file().sync_all().map_err(|_| MediaError::Io)?;
    if input.is_cancelled() {
        return Err(MediaError::Cancelled);
    }
    Ok(Some(NativePreparedMedia {
        kind: NativeMediaKind::Image,
        output,
        main: PreparedMediaPath {
            output: Arc::new(file),
            source_sha256: input.source_sha256,
            output_sha256: result.output_sha256,
            profile: NATIVE_PREPARATION_PROFILE,
            mime_type: result.mime_type,
            thumbnail: result.thumbnail,
            disposition: result.disposition,
            main_facts: result.main_facts,
            thumbnail_facts: result.thumbnail_facts,
        },
    }))
}

/// Inspect the stable owned bytes independently of callback claims. Native adapters additionally
/// decode complete output streams; this parser does not manufacture decoded-evidence metadata.
fn verify_output_container(
    file: &std::fs::File,
    kind: NativeMediaKind,
    facts: &NativeMediaOutput,
) -> Result<Option<MediaFacts>, MediaError> {
    use std::io::{Seek, SeekFrom};
    let mut file = file.try_clone().map_err(|_| MediaError::Io)?;
    file.seek(SeekFrom::Start(0)).map_err(|_| MediaError::Io)?;
    if matches!(kind, NativeMediaKind::Image) {
        let mut bytes = Vec::new();
        file.take(IMAGE_MAIN_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| MediaError::Io)?;
        if bytes.len() as u64 > IMAGE_MAIN_BYTES {
            return Err(MediaError::Budget);
        }
        let chunks = png_chunks(&bytes)?;
        if chunks.contains(b"acTL") {
            return Err(MediaError::Unsupported);
        }
        let header = bytes.get(16..29).ok_or(MediaError::Invalid)?;
        let width = u32::from_be_bytes(header[..4].try_into().map_err(|_| MediaError::Invalid)?);
        let height = u32::from_be_bytes(header[4..8].try_into().map_err(|_| MediaError::Invalid)?);
        if width != facts.width || height != facts.height {
            return Err(MediaError::Invalid);
        }
        if u64::from(width) * u64::from(height) > MAX_IMAGE_PIXELS {
            return Err(MediaError::Budget);
        }
        return inspect_image(&bytes).map(Some);
    }
    let length = file.metadata().map_err(|_| MediaError::Io)?.len();
    let mut offset = 0u64;
    let mut moov = None;
    let mut media = false;
    let mut count = 0;
    while offset < length {
        let mut header = [0; 8];
        file.read_exact(&mut header)
            .map_err(|_| MediaError::Invalid)?;
        let encoded_size =
            u32::from_be_bytes(header[..4].try_into().map_err(|_| MediaError::Invalid)?);
        let (size, header_length) = match encoded_size {
            0 => (length - offset, 8),
            1 => {
                let mut extended = [0; 8];
                file.read_exact(&mut extended)
                    .map_err(|_| MediaError::Invalid)?;
                (u64::from_be_bytes(extended), 16)
            }
            size => (u64::from(size), 8),
        };
        if size < header_length
            || offset.checked_add(size).is_none_or(|n| n > length)
            || count > 1024
        {
            return Err(MediaError::Invalid);
        }
        match &header[4..8] {
            b"ftyp" if offset == 0 => {}
            b"moov" if moov.is_none() && size <= 4 * 1024 * 1024 => {
                let mut bytes = vec![0; (size - header_length) as usize];
                file.read_exact(&mut bytes)
                    .map_err(|_| MediaError::Invalid)?;
                moov = Some(bytes);
            }
            b"mdat" => media = true,
            b"free" | b"wide" => {}
            _ => return Err(MediaError::Unsupported),
        }
        offset += size;
        count += 1;
        file.seek(SeekFrom::Start(offset))
            .map_err(|_| MediaError::Io)?;
    }
    if !media {
        return Err(MediaError::Invalid);
    }
    let moov = moov.ok_or(MediaError::Invalid)?;
    let mut videos = 0;
    let mut audios = 0;
    for (_, track) in mp4_boxes(&moov)?
        .into_iter()
        .filter(|(tag, _)| tag == b"trak")
    {
        let mdia = child(track, b"mdia")?;
        let handler = child(mdia, b"hdlr")?
            .get(8..12)
            .ok_or(MediaError::Invalid)?;
        let stsd = child(child(child(mdia, b"minf")?, b"stbl")?, b"stsd")?;
        if stsd.get(4..8) != Some(&1u32.to_be_bytes()) {
            return Err(MediaError::Unsupported);
        }
        let entries = mp4_boxes(stsd.get(8..).ok_or(MediaError::Invalid)?)?;
        if entries.len() != 1 {
            return Err(MediaError::Unsupported);
        }
        let (tag, entry) = entries[0];
        match handler {
            b"vide" if tag == *b"avc1" => {
                videos += 1;
                let width = u16::from_be_bytes(
                    entry
                        .get(24..26)
                        .ok_or(MediaError::Invalid)?
                        .try_into()
                        .map_err(|_| MediaError::Invalid)?,
                ) as u32;
                let height = u16::from_be_bytes(
                    entry
                        .get(26..28)
                        .ok_or(MediaError::Invalid)?
                        .try_into()
                        .map_err(|_| MediaError::Invalid)?,
                ) as u32;
                if ![(facts.width, facts.height), (facts.height, facts.width)]
                    .contains(&(width, height))
                {
                    return Err(MediaError::Invalid);
                }
                let config = child(entry.get(78..).ok_or(MediaError::Invalid)?, b"avcC")?;
                verify_avc8(config)?;
            }
            b"soun" if tag == *b"mp4a" => {
                audios += 1;
                let config = child(entry.get(28..).ok_or(MediaError::Invalid)?, b"esds")?;
                verify_aac_lc(config, facts)?;
            }
            _ => return Err(MediaError::Unsupported),
        }
    }
    let expected_videos = usize::from(matches!(kind, NativeMediaKind::Video));
    if videos != expected_videos || audios != usize::from(facts.audio_channels > 0) {
        return Err(MediaError::Invalid);
    }
    Ok(None)
}

/// Cross-language fixture validation only. The native fixture must have already fully decoded these
/// exact files; this checks Rust's independent profile parser, not a claim of a Rust video decoder.
#[cfg(test)]
pub fn test_verify_native_output(
    path: &Path,
    kind: NativeMediaKind,
    facts: &NativeMediaOutput,
) -> Result<(), MediaError> {
    validate_native_facts(kind, facts, NativePreparationLimits::default())?;
    let file = std::fs::File::open(path).map_err(|_| MediaError::Io)?;
    verify_output_container(&file, kind, facts).map(|_| ())
}

/// A required unique child; duplicate boxes do not qualify a profile.
fn child<'a>(bytes: &'a [u8], name: &[u8; 4]) -> Result<&'a [u8], MediaError> {
    let mut children = mp4_boxes(bytes)?.into_iter().filter(|(tag, _)| tag == name);
    let result = children.next().ok_or(MediaError::Invalid)?.1;
    if children.next().is_some() {
        return Err(MediaError::Invalid);
    }
    Ok(result)
}

/// Bounded descriptor parsing, not an arbitrary search for an AAC-looking byte sequence.
fn verify_aac_lc(bytes: &[u8], facts: &NativeMediaOutput) -> Result<(), MediaError> {
    fn descriptor(bytes: &[u8], tag: u8) -> Result<&[u8], MediaError> {
        if bytes.first() != Some(&tag) {
            return Err(MediaError::Invalid);
        }
        let mut size = 0usize;
        for i in 1..=4 {
            let b = *bytes.get(i).ok_or(MediaError::Invalid)?;
            size = (size << 7) | usize::from(b & 127);
            if b & 128 == 0 {
                return bytes.get(i + 1..i + 1 + size).ok_or(MediaError::Invalid);
            }
        }
        Err(MediaError::Invalid)
    }
    let es = descriptor(bytes.get(4..).ok_or(MediaError::Invalid)?, 3)?;
    if es.get(2) != Some(&0) {
        return Err(MediaError::Unsupported);
    }
    let decoder = descriptor(es.get(3..).ok_or(MediaError::Invalid)?, 4)?;
    if decoder.first() != Some(&0x40) {
        return Err(MediaError::Unsupported);
    }
    let asc = descriptor(decoder.get(13..).ok_or(MediaError::Invalid)?, 5)?;
    if asc.len() < 2 || asc[0] >> 3 != 2 {
        return Err(MediaError::Unsupported);
    }
    let mut bits = MediaBits { bytes: asc, bit: 5 };
    let index = bits.read(4)?;
    let rate = if index == 15 {
        bits.read(24)?
    } else {
        *[
            96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350,
        ]
        .get(index as usize)
        .ok_or(MediaError::Unsupported)?
    };
    if rate != facts.audio_sample_rate || bits.read(4)? != u32::from(facts.audio_channels) {
        return Err(MediaError::Invalid);
    }
    Ok(())
}

/// Verify each SPS's precision/chroma, not just the avcC profile label or decoded pixel request.
fn verify_avc8(config: &[u8]) -> Result<(), MediaError> {
    if config.len() < 7
        || config[0] != 1
        || ![66, 77, 100].contains(&config[1])
        || config[5] & 31 == 0
    {
        return Err(MediaError::Unsupported);
    }
    let mut offset = 6;
    for _ in 0..config[5] & 31 {
        let length = u16::from_be_bytes(
            config
                .get(offset..offset + 2)
                .ok_or(MediaError::Invalid)?
                .try_into()
                .map_err(|_| MediaError::Invalid)?,
        ) as usize;
        offset += 2;
        let sps = config
            .get(offset..offset + length)
            .ok_or(MediaError::Invalid)?;
        offset += length;
        if sps.len() < 5 || sps[0] & 31 != 7 || sps[1] != config[1] {
            return Err(MediaError::Unsupported);
        }
        let bytes: Vec<_> = sps[1..]
            .iter()
            .enumerate()
            .filter_map(|(i, &b)| {
                (!(i >= 2 && b == 3 && sps[i - 1] == 0 && sps[i] == 0)).then_some(b)
            })
            .collect();
        let mut bits = MediaBits {
            bytes: &bytes,
            bit: 24,
        };
        bits.ue()?;
        if sps[1] == 100 && (bits.ue()? != 1 || bits.ue()? != 0 || bits.ue()? != 0) {
            return Err(MediaError::Unsupported);
        }
    }
    if config.get(offset).is_none_or(|count| *count == 0) {
        return Err(MediaError::Invalid);
    }
    Ok(())
}

/// Bounded bit prefix reader shared by H.264 SPS and AAC AudioSpecificConfig inspection.
struct MediaBits<'a> {
    bytes: &'a [u8],
    bit: usize,
}
impl MediaBits<'_> {
    fn read(&mut self, count: usize) -> Result<u32, MediaError> {
        if count > 24 || self.bit + count > self.bytes.len() * 8 {
            return Err(MediaError::Invalid);
        }
        let mut value = 0;
        for _ in 0..count {
            value = value << 1 | u32::from((self.bytes[self.bit / 8] >> (7 - self.bit % 8)) & 1);
            self.bit += 1;
        }
        Ok(value)
    }
    fn ue(&mut self) -> Result<u32, MediaError> {
        let mut zeros = 0;
        while self.read(1)? == 0 {
            zeros += 1;
            if zeros > 23 {
                return Err(MediaError::Invalid);
            }
        }
        Ok((1 << zeros) - 1 + self.read(zeros)?)
    }
}

/// Fixed purpose checks are separate from native output decode and the server's byte validation.
fn validate_native_facts(
    kind: NativeMediaKind,
    f: &NativeMediaOutput,
    limits: NativePreparationLimits,
) -> Result<(), MediaError> {
    match kind {
        NativeMediaKind::Image => {
            if f.width == 0
                || f.height == 0
                || f.width > IMAGE_MAIN_AXIS
                || f.height > IMAGE_MAIN_AXIS
            {
                return Err(MediaError::Budget);
            }
            // This first native image adapter emits static PNG only; animation must use the
            // shared lossless animation encoder, never silently become a static image.
            if f.frame_count != 1 || f.audio_channels != 0 {
                return Err(MediaError::Unsupported);
            }
        }
        NativeMediaKind::Video => {
            if f.width.max(f.height) > 1920
                || f.width.min(f.height) > 1080
                || f.width < 2
                || f.height < 2
                || !f.width.is_multiple_of(2)
                || !f.height.is_multiple_of(2)
                || f.frame_count == 0
                || f.frame_count > limits.max_frames
            {
                return Err(MediaError::Budget);
            }
        }
        NativeMediaKind::Audio | NativeMediaKind::Voice(_) => {
            if f.width != 0 || f.height != 0 || f.frame_count != 0 || f.audio_channels == 0 {
                return Err(MediaError::Invalid);
            }
        }
    }
    if !matches!(kind, NativeMediaKind::Image)
        && (f.duration_ms == 0 || f.duration_ms > limits.max_duration_ms)
    {
        return Err(MediaError::Budget);
    }
    if f.audio_channels > 2
        || (f.audio_channels > 0
            && (!(8000..=48000).contains(&f.audio_sample_rate) || f.audio_samples == 0))
    {
        return Err(MediaError::Unsupported);
    }
    Ok(())
}

/// Open-handle, streaming verification; no file-sized buffer or host-chosen source pathname.
fn hash_file(file: &std::fs::File, expected: u64) -> Result<[u8; 32], MediaError> {
    use std::io::{Seek, SeekFrom};
    let mut file = file.try_clone().map_err(|_| MediaError::Io)?;
    file.seek(SeekFrom::Start(0)).map_err(|_| MediaError::Io)?;
    let mut hash = Sha256::new();
    let mut size = 0u64;
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer).map_err(|_| MediaError::Io)?;
        if n == 0 {
            break;
        }
        size = size.checked_add(n as u64).ok_or(MediaError::Budget)?;
        if size > expected {
            return Err(MediaError::SourceChanged);
        }
        hash.update(&buffer[..n]);
    }
    if size != expected {
        return Err(MediaError::SourceChanged);
    }
    Ok(hash.finalize().into())
}

/// Bounded shared waveform: 100 temporal peak bins, unsigned linear amplitude 0..255.
/// Integer PCM only, no channel averaging that would cancel opposite-polarity voices.
fn pcm_waveform(file: &std::fs::File, format: PcmFormat) -> Result<Vec<u8>, MediaError> {
    use std::io::{Seek, SeekFrom};
    if format.bits_per_sample != 16
        || !(1..=2).contains(&format.channels)
        || !(8000..=48000).contains(&format.sample_rate)
    {
        return Err(MediaError::Unsupported);
    }
    let mut file =
        std::io::BufReader::with_capacity(65536, file.try_clone().map_err(|_| MediaError::Io)?);
    file.seek(SeekFrom::Start(0)).map_err(|_| MediaError::Io)?;
    let bytes = file.get_ref().metadata().map_err(|_| MediaError::Io)?.len();
    let stride = u64::from(format.channels) * 2;
    if bytes == 0 || bytes % stride != 0 {
        return Err(MediaError::Invalid);
    }
    let frames = bytes / stride;
    let mut bins = vec![0u8; 100];
    let mut sample = [0; 2];
    for i in 0..bytes / 2 {
        file.read_exact(&mut sample).map_err(|_| MediaError::Io)?;
        let level = (u32::from(i16::from_le_bytes(sample).unsigned_abs()) * 255 / 32768) as u8;
        let bin = ((i / u64::from(format.channels)) * 100 / frames).min(99) as usize;
        bins[bin] = bins[bin].max(level);
    }
    Ok(bins)
}
