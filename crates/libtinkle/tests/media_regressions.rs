//! Actual codec fixtures with coordinated access to process-wide admission.
#[path = "../src/media.rs"]
#[allow(dead_code)] // Compile the module directly to exercise its test-only snapshot controls.
mod media;

use image::ImageEncoder;
/// All codec callers share one gate; individual tests still exercise the real two-slot limit.
static MEDIA_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Generate the 100-Mpixel native fixture with one scanline, not a 400-MB fixture raster.
#[test]
#[ignore = "writes an explicitly selected disposable native-fixture path"]
fn generate_100mp_native_image_fixture() {
    let path = std::env::var_os("TINKLE_MEDIA_100MP_FIXTURE_PATH").expect("fixture output path");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    let mut encoder = png::Encoder::new(file, 10_000, 10_000);
    encoder.set_color(png::ColorType::Rgba);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder.write_header().unwrap();
    let mut stream = writer.stream_writer().unwrap();
    use std::io::Write;
    let row: Vec<u8> = [20, 40, 60, 128].repeat(10_000);
    for _ in 0..10_000 {
        stream.write_all(&row).unwrap();
    }
    stream.finish().unwrap();
    writer.finish().unwrap();
}

/// Read actual native-encoded fixtures retained by NativeMediaPreparationTests.swift.
#[test]
#[ignore = "requires explicitly retained native codec output artifacts"]
fn verify_real_native_encoder_profiles() {
    let directory = std::path::PathBuf::from(
        std::env::var_os("TINKLE_NATIVE_CODEC_ARTIFACT_DIR").expect("native fixture directory"),
    );
    for (name, kind, width, height, frames) in [
        (
            "voice",
            media::NativeMediaKind::Voice(media::PcmFormat {
                sample_rate: 48000,
                channels: 1,
                bits_per_sample: 16,
            }),
            0,
            0,
            0,
        ),
        ("audio", media::NativeMediaKind::Audio, 0, 0, 0),
        ("video", media::NativeMediaKind::Video, 1080, 1920, 4),
    ] {
        let facts = media::NativeMediaOutput {
            width,
            height,
            frame_count: frames,
            duration_ms: 400,
            audio_channels: 1,
            audio_sample_rate: 48000,
            audio_samples: 19200,
            waveform: vec![],
        };
        assert_eq!(
            media::test_verify_native_output(&directory.join(format!("{name}.mp4")), kind, &facts),
            Ok(()),
            "native {name} profile"
        );
    }
}

/// The codec fixture supplies real AAC bytes; this checks shared PCM waveform/owned-path adoption,
/// not another encoder implementation. Native codec integrity needs consumer-owned qualification.
#[tokio::test]
#[ignore = "requires explicitly retained native AAC output"]
async fn native_voice_waveform_from_real_aac_fixture() {
    let _test = MEDIA_TESTS.lock().await;
    struct Encoded(std::path::PathBuf);
    #[async_trait::async_trait]
    impl media::PlatformMediaPreparer for Encoded {
        async fn prepare(
            &self,
            input: std::sync::Arc<media::NativeMediaInput>,
        ) -> Result<media::NativeMediaOutput, media::MediaError> {
            std::fs::copy(&self.0, input.output_path()).map_err(|_| media::MediaError::Io)?;
            Ok(media::NativeMediaOutput {
                width: 0,
                height: 0,
                frame_count: 0,
                duration_ms: 400,
                audio_channels: 1,
                audio_sample_rate: 48000,
                audio_samples: 19200,
                waveform: vec![],
            })
        }
    }
    let directory = std::path::PathBuf::from(
        std::env::var_os("TINKLE_NATIVE_CODEC_ARTIFACT_DIR").expect("native fixture directory"),
    );
    let source = tempfile::NamedTempFile::new().unwrap();
    let pcm: Vec<u8> = (0..19200)
        .flat_map(|i| {
            (((f64::from(i) * 2.0 * std::f64::consts::PI * 440.0 / 48000.0).sin() * 16000.0) as i16)
                .to_le_bytes()
        })
        .collect();
    std::fs::write(source.path(), &pcm).unwrap();
    let result = media::prepare_native_media(
        source.path(),
        media::NativeMediaKind::Voice(media::PcmFormat {
            sample_rate: 48000,
            channels: 1,
            bits_per_sample: 16,
        }),
        std::sync::Arc::new(Encoded(directory.join("voice.mp4"))),
        None,
    )
    .await
    .unwrap();
    assert_eq!(result.main.mime_type, "audio/mp4");
    assert_eq!(result.main.source_sha256, libtinkle::sha256(&pcm));
    assert_eq!(result.output.waveform.len(), 100);
    assert!(result
        .output
        .waveform
        .iter()
        .all(|n| (120..=125).contains(n)));
    assert_ne!(result.main.source_sha256, result.main.output_sha256);
}

/// Public dimension budgets are independent of the decoder allocation guard.
#[test]
fn native_recipe_fits_both_axes_and_portrait_without_upscale() {
    for (w, h) in [(10_000, 10_000), (20_000, 5000), (1, 100_000_000)] {
        let (mut previous_w, mut previous_h) = (w, h);
        for attempt in 0..10 {
            let (width, height) = media::image_output_dimensions(w, h, attempt).unwrap();
            assert!(width <= previous_w && height <= previous_h);
            assert!(width <= 10_000 && height <= 10_000);
            assert!(u64::from(width) * u64::from(height) <= media::MAX_IMAGE_PIXELS);
            previous_w = width;
            previous_h = height;
        }
    }
    assert_eq!(
        media::video_output_dimensions(3840, 2160).unwrap(),
        (1920, 1080)
    );
    assert_eq!(
        media::video_output_dimensions(2160, 3840).unwrap(),
        (1080, 1920)
    );
    assert_eq!(
        media::video_output_dimensions(640, 480).unwrap(),
        (640, 480)
    );
    assert_eq!(
        media::video_output_dimensions(1920, 1920).unwrap(),
        (1080, 1080)
    );
}

/// Real PNG bytes stand in for the native callback here; this tests ownership, not a mobile codec.
struct NativePngPreparer {
    mutate_source: bool,
    quota: bool,
}
#[async_trait::async_trait]
impl media::PlatformMediaPreparer for NativePngPreparer {
    async fn prepare(
        &self,
        input: std::sync::Arc<media::NativeMediaInput>,
    ) -> Result<media::NativeMediaOutput, media::MediaError> {
        if self.mutate_source {
            std::fs::write(input.source_path(), b"changed").unwrap();
        }
        if self.quota {
            std::fs::File::create(input.output_path())
                .unwrap()
                .set_len(media::IMAGE_MAIN_BYTES + 1)
                .unwrap();
        } else {
            let mut output = std::fs::File::create(input.output_path()).unwrap();
            image::codecs::png::PngEncoder::new(&mut output)
                .write_image(&[20, 40, 60, 128], 1, 1, image::ExtendedColorType::Rgba8)
                .unwrap();
        }
        Ok(media::NativeMediaOutput {
            width: 1,
            height: 1,
            frame_count: 1,
            duration_ms: 0,
            audio_channels: 0,
            audio_sample_rate: 0,
            audio_samples: 0,
            waveform: vec![],
        })
    }
}

/// Imported files need only read access; scratch is explicit, invocation-local and never a
/// predictable destination in either the source directory or the root itself.
#[cfg(unix)]
#[tokio::test]
async fn native_explicit_scratch_root_isolated_from_readonly_source_and_poisoned_names() {
    use std::{
        os::unix::fs::{symlink, PermissionsExt},
        sync::Arc,
    };
    let _test = MEDIA_TESTS.lock().await;
    let imports = tempfile::tempdir().unwrap();
    let source = imports.path().join("import.png");
    std::fs::write(&source, b"read-only import").unwrap();
    std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o400)).unwrap();
    std::fs::set_permissions(imports.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let victim = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(victim.path(), b"do not truncate").unwrap();
    let poisoned = scratch.path().join("main.mp4");
    symlink(victim.path(), &poisoned).unwrap();

    struct PrivateRoot(std::path::PathBuf);
    #[async_trait::async_trait]
    impl media::PlatformMediaPreparer for PrivateRoot {
        async fn prepare(
            &self,
            input: Arc<media::NativeMediaInput>,
        ) -> Result<media::NativeMediaOutput, media::MediaError> {
            assert_eq!(input.source_path().parent(), Some(self.0.as_path()));
            let output = input.output_path();
            let directory = output.parent().unwrap();
            assert_eq!(directory.parent(), Some(self.0.as_path()));
            assert_ne!(directory, self.0);
            assert!(!output.exists());
            assert_eq!(
                directory.metadata().unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                input.source_path().metadata().unwrap().permissions().mode() & 0o777,
                0o600
            );
            NativePngPreparer {
                mutate_source: false,
                quota: false,
            }
            .prepare(input)
            .await
        }
    }
    let preparer = Arc::new(PrivateRoot(scratch.path().to_path_buf()));
    let first = media::prepare_native_media_in(
        &source,
        scratch.path(),
        media::NativeMediaKind::Image,
        preparer.clone(),
        None,
    )
    .await
    .unwrap();
    let second = media::prepare_native_media_in(
        &source,
        scratch.path(),
        media::NativeMediaKind::Image,
        preparer,
        None,
    )
    .await
    .unwrap();
    assert_eq!(first.main.path().parent(), Some(scratch.path()));
    assert_ne!(first.main.path(), second.main.path());
    assert_eq!(
        first.main.source_sha256,
        libtinkle::sha256(b"read-only import")
    );
    let artifact_path = first.main.path().to_path_buf();
    let retained = first.clone();
    drop(first);
    assert!(artifact_path.is_file());
    drop(retained);
    drop(second);
    assert!(!artifact_path.exists());
    assert_eq!(std::fs::read(victim.path()).unwrap(), b"do not truncate");
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 1);
    assert_eq!(std::fs::read(&source).unwrap(), b"read-only import");
    assert_eq!(std::fs::read_dir(imports.path()).unwrap().count(), 1);
    std::fs::set_permissions(imports.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
}

#[tokio::test]
async fn native_explicit_scratch_root_errors_never_fall_back_to_source_or_system_temp() {
    let _test = MEDIA_TESTS.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::write(&source, b"source").unwrap();
    let host = || {
        std::sync::Arc::new(NativePngPreparer {
            mutate_source: true,
            quota: true,
        })
    };
    assert!(matches!(
        media::prepare_native_media_in(
            &source,
            std::path::Path::new("relative"),
            media::NativeMediaKind::Image,
            host(),
            None
        )
        .await,
        Err(media::MediaError::Invalid)
    ));
    for root in [directory.path().join("missing"), source.clone()] {
        assert!(matches!(
            media::prepare_native_media_in(
                &source,
                &root,
                media::NativeMediaKind::Image,
                host(),
                None
            )
            .await,
            Err(media::MediaError::Io)
        ));
    }
    assert_eq!(std::fs::read(&source).unwrap(), b"source");
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn native_owned_artifact_source_precedes_output_quota_and_precancel() {
    let _test = MEDIA_TESTS.lock().await;
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"frozen source").unwrap();
    let prepare = |mutate_source, quota| {
        std::sync::Arc::new(NativePngPreparer {
            mutate_source,
            quota,
        })
    };
    let artifact = media::prepare_native_media(
        source.path(),
        media::NativeMediaKind::Image,
        prepare(false, false),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        artifact.main.source_sha256,
        libtinkle::sha256(b"frozen source")
    );
    assert_ne!(artifact.main.source_sha256, artifact.main.output_sha256);
    assert_eq!(
        artifact.main.disposition,
        media::PreparationDisposition::NativeEncoded
    );
    let path = artifact.main.path().to_path_buf();
    assert!(path.exists());
    let retained = artifact.clone();
    drop(artifact);
    assert!(path.exists());
    drop(retained);
    assert!(!path.exists());
    assert!(matches!(
        media::prepare_native_media(
            source.path(),
            media::NativeMediaKind::Image,
            prepare(false, true),
            None
        )
        .await,
        Err(media::MediaError::Budget)
    ));
    assert!(matches!(
        media::prepare_native_media(
            source.path(),
            media::NativeMediaKind::Image,
            prepare(true, true),
            None
        )
        .await,
        Err(media::MediaError::SourceChanged)
    ));
    let cancel = libtinkle::CancelToken::new();
    cancel.cancel();
    assert!(matches!(
        media::prepare_native_media(
            source.path(),
            media::NativeMediaKind::Image,
            prepare(true, true),
            Some(cancel)
        )
        .await,
        Err(media::MediaError::Cancelled)
    ));
}

/// A foreign callback that cannot stop is quarantined, not aborted or allowed to free its files.
#[tokio::test]
async fn native_cancel_has_bounded_join_and_retains_unjoined_lease() {
    let _test = MEDIA_TESTS.lock().await;
    struct Held {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        lease: std::sync::Mutex<Option<std::sync::Arc<media::NativeMediaInput>>>,
    }
    #[async_trait::async_trait]
    impl media::PlatformMediaPreparer for Held {
        async fn prepare(
            &self,
            input: std::sync::Arc<media::NativeMediaInput>,
        ) -> Result<media::NativeMediaOutput, media::MediaError> {
            *self.lease.lock().unwrap() = Some(input);
            self.entered.notify_one();
            self.release.notified().await;
            Err(media::MediaError::Cancelled)
        }
    }
    let host = std::sync::Arc::new(Held {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        lease: Default::default(),
    });
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"frozen source").unwrap();
    let path = source.path().to_owned();
    let scratch = tempfile::tempdir().unwrap();
    let scratch_path = scratch.path().to_path_buf();
    let cancel = libtinkle::CancelToken::new();
    let h = host.clone();
    let token = cancel.clone();
    let work = tokio::spawn(async move {
        media::prepare_native_media_in(
            &path,
            &scratch_path,
            media::NativeMediaKind::Video,
            h,
            Some(token),
        )
        .await
    });
    host.entered.notified().await;
    cancel.cancel();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(2), work)
            .await
            .unwrap()
            .unwrap(),
        Err(media::MediaError::Cancelled)
    ));
    let lease = host.lease.lock().unwrap().take().unwrap();
    let frozen = lease.source_path().to_owned();
    let destination = lease.output_path();
    assert_eq!(frozen.parent(), Some(scratch.path()));
    assert_eq!(destination.parent().unwrap().parent(), Some(scratch.path()));
    let joined = std::sync::Arc::downgrade(&lease);
    assert!(lease.is_cancelled() && frozen.is_file());
    assert!(destination.parent().unwrap().is_dir());
    host.release.notify_one();
    drop(lease);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while joined.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native worker must release its entire admission lease before the next test");
    assert!(!frozen.exists());
    assert!(!destination.parent().unwrap().exists());
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

/// Animation markers inside metadata are not RIFF chunk tags or animation evidence.
#[test]
fn static_webp_with_anim_in_metadata_remains_static() {
    let _test = MEDIA_TESTS.blocking_lock();
    let mut input = Vec::new();
    image::codecs::webp::WebPEncoder::new_lossless(&mut input)
        .write_image(&[1, 2, 3, 255], 1, 1, image::ExtendedColorType::Rgba8)
        .unwrap();
    let mut extended = input[..12].to_vec();
    extended.extend_from_slice(b"VP8X");
    extended.extend_from_slice(&10u32.to_le_bytes());
    extended.extend_from_slice(&[0x04, 0, 0, 0, 0, 0, 0, 0, 0, 0]); // XMP flag, 1x1 canvas.
    extended.extend_from_slice(&input[12..]);
    input = extended;
    input.extend_from_slice(b"XMP ");
    input.extend_from_slice(&4u32.to_le_bytes());
    input.extend_from_slice(b"ANIM");
    let size = (input.len() - 8) as u32;
    input[4..8].copy_from_slice(&size.to_le_bytes());
    let prepared = media::prepare_bytes(&input, media::MediaKind::Image).unwrap();
    let facts = prepared.main_facts.unwrap();
    assert!(!facts.animated);
    assert_eq!(facts.frame_count, Some(1));
    assert_eq!(facts.duration_ms, None);
    assert_eq!(facts.loop_count, None);
    assert!(prepared.thumbnail.is_some());
}

/// Unsupported imports are explicit failures, never implicit document media.
#[test]
fn unsupported_image_decoder_preserves_bytes_without_facts() {
    let _test = MEDIA_TESTS.blocking_lock();
    for input in [b"BMunsupported bmp".as_slice(), b"II*\0unsupported tiff"] {
        assert!(matches!(
            media::prepare_bytes(input, media::MediaKind::Image),
            Err(media::MediaError::Unsupported)
        ));
    }
}

/// Replace the pathname after the real advisory preflight; reject the newly
/// opened oversized file before even creating a temp, not after copying 256 MiB.
#[tokio::test]
async fn video_ceiling_is_enforced_by_the_snapshot_worker() {
    let _test = MEDIA_TESTS.lock().await;
    struct NeverDecode(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    #[async_trait::async_trait]
    impl media::PlatformVideoDecoder for NeverDecode {
        async fn inspect_and_decode(
            &self,
            _: std::sync::Arc<media::VideoInput>,
            _: media::VideoDecodeLimits,
            _: libtinkle::CancelToken,
        ) -> Result<media::DecodedVideo, media::MediaError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(media::MediaError::Unsupported)
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    std::fs::write(&path, b"small").unwrap();
    let gate = std::sync::Arc::new(media::SnapshotGate::default());
    let audit = std::sync::Arc::new(media::SnapshotAudit::default());
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let decoder = std::sync::Arc::new(NeverDecode(calls.clone()));
    let input = path.clone();
    let g = gate.clone();
    let a = audit.clone();
    let mut task =
        tokio::spawn(async move { media::test_video_snapshot_race(&input, decoder, g, a).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    let replacement = directory.path().join("replacement");
    std::fs::File::create(&replacement)
        .unwrap()
        .set_len(media::VideoDecodeLimits::default().max_input_bytes + 1)
        .unwrap();
    std::fs::rename(replacement, &path).unwrap();
    gate.release.notify_one();
    let result = tokio::time::timeout(std::time::Duration::from_secs(3), &mut task).await;
    if result.is_err() {
        task.abort();
        let _ = task.await;
    }
    assert!(
        matches!(result, Ok(Ok(Err(media::MediaError::Budget)))),
        "worker failed to reject before copying"
    );
    assert_eq!(audit.created.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(audit.written.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn lossless_image_preserves_transparent_pixels_and_never_upscales() {
    let _test = MEDIA_TESTS.blocking_lock();
    let pixels = vec![
        1, 2, 3, 0, 40, 50, 60, 128, 7, 8, 9, 255, 100, 120, 130, 255,
    ];
    let mut png = Vec::new();
    image::codecs::png::PngEncoder::new(&mut png)
        .write_image(&pixels, 2, 2, image::ExtendedColorType::Rgba8)
        .unwrap();
    let result = media::prepare_bytes(&png, media::MediaKind::Image).unwrap();
    let decoded = image::load_from_memory(&result.bytes).unwrap().to_rgba8();
    assert_eq!(decoded.as_raw(), &pixels);
    let thumb = image::load_from_memory(&result.thumbnail.unwrap()).unwrap();
    assert_eq!((thumb.width(), thumb.height()), (2, 2));
    assert_eq!(result.source_sha256, libtinkle::sha256(&png));
    assert_eq!(result.profile, media::PREPARATION_PROFILE);
}

/// Incompressible RGBA must resize to fit MAIN, never switch to a lossy quality setting.
#[test]
fn image_byte_budget_resizes_pixels_instead_of_lowering_quality() {
    let _test = MEDIA_TESTS.blocking_lock();
    let mut seed = 0x6d2b79f5u32;
    let pixels: Vec<u8> = (0..1600 * 1600 * 4)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed as u8
        })
        .collect();
    let mut png = Vec::new();
    image::codecs::png::PngEncoder::new(&mut png)
        .write_image(&pixels, 1600, 1600, image::ExtendedColorType::Rgba8)
        .unwrap();
    assert!(png.len() > 10_000_000);
    let prepared = media::prepare_bytes(&png, media::MediaKind::Image).unwrap();
    assert!(prepared.bytes.len() <= 10_000_000 && prepared.main_facts.as_ref().unwrap().lossless);
    let decoded = image::load_from_memory(&prepared.bytes).unwrap().to_rgba8();
    assert!(decoded.width() < 1600 && decoded.height() < 1600);
    let original =
        image::DynamicImage::ImageRgba8(image::RgbaImage::from_raw(1600, 1600, pixels).unwrap());
    let expected = original
        .resize_exact(
            decoded.width(),
            decoded.height(),
            image::imageops::FilterType::Triangle,
        )
        .to_rgba8();
    assert_eq!(decoded, expected);
}

#[test]
fn image_thumbnail_is_bounded_and_documents_are_exact() {
    let _test = MEDIA_TESTS.blocking_lock();
    let pixels = vec![13; 800 * 400 * 3];
    let mut png = Vec::new();
    image::codecs::png::PngEncoder::new(&mut png)
        .write_image(&pixels, 800, 400, image::ExtendedColorType::Rgb8)
        .unwrap();
    let result = media::prepare_bytes(&png, media::MediaKind::Image).unwrap();
    let thumb = image::load_from_memory(&result.thumbnail.unwrap()).unwrap();
    assert_eq!((thumb.width(), thumb.height()), (320, 160));
    let doc = media::prepare_bytes(&png, media::MediaKind::Document).unwrap();
    assert_eq!(doc.bytes, png);
    assert!(doc.thumbnail.is_none());
}

#[test]
fn pcm_flac_preserves_samples_and_configuration() {
    let _test = MEDIA_TESTS.blocking_lock();
    let samples: Vec<i32> = (0..16384).map(|i| ((i * 997) % 65536) - 32768).collect();
    let pcm: Vec<u8> = samples
        .iter()
        .flat_map(|s| (*s as i16).to_le_bytes())
        .collect();
    let output = media::prepare_bytes(
        &pcm,
        media::MediaKind::Voice(media::PcmFormat {
            sample_rate: 48000,
            channels: 2,
            bits_per_sample: 16,
        }),
    )
    .unwrap();
    let mut reader = claxon::FlacReader::new(std::io::Cursor::new(output.bytes)).unwrap();
    assert_eq!(reader.streaminfo().sample_rate, 48000);
    assert_eq!(reader.streaminfo().bits_per_sample, 16);
    assert_eq!(reader.streaminfo().channels, 2);
    assert_eq!(
        reader.samples().collect::<Result<Vec<_>, _>>().unwrap(),
        samples
    );
}

#[test]
fn malformed_pcm_and_image_are_rejected() {
    let _test = MEDIA_TESTS.blocking_lock();
    assert!(media::prepare_bytes(b"bad", media::MediaKind::Image).is_err());
    assert!(media::prepare_bytes(
        b"bad",
        media::MediaKind::Voice(media::PcmFormat {
            sample_rate: 48000,
            channels: 2,
            bits_per_sample: 16,
        })
    )
    .is_err());
}

/// Animation normalizes to lossless WebP, including loop/timing/disposal, also on the native path.
#[tokio::test]
async fn animated_gif_is_never_flattened() {
    let _test = MEDIA_TESTS.lock().await;
    use image::{Delay, Frame, Rgba, RgbaImage};
    let mut gif = Vec::new();
    {
        let mut encoder = image::codecs::gif::GifEncoder::new(&mut gif);
        encoder
            .set_repeat(image::codecs::gif::Repeat::Finite(3))
            .unwrap();
        encoder
            .encode_frames([
                Frame::from_parts(
                    RgbaImage::from_pixel(3, 2, Rgba([255, 0, 0, 255])),
                    0,
                    0,
                    Delay::from_numer_denom_ms(30, 1),
                ),
                Frame::from_parts(
                    RgbaImage::from_pixel(3, 2, Rgba([0, 0, 255, 0])),
                    0,
                    0,
                    Delay::from_numer_denom_ms(170, 1),
                ),
            ])
            .unwrap();
    }
    let result = media::prepare_bytes(&gif, media::MediaKind::Image).unwrap();
    assert_ne!(result.bytes, gif);
    assert_eq!(result.disposition, media::PreparationDisposition::Lossless);
    assert_eq!(result.mime_type, "image/webp");
    let thumb = image::load_from_memory(result.thumbnail.as_ref().unwrap()).unwrap();
    assert_eq!((thumb.width(), thumb.height()), (3, 2));
    let facts = result.main_facts.as_ref().unwrap();
    assert!(facts.animated);
    assert_eq!(facts.frame_count, Some(2));
    assert_eq!(facts.duration_ms, Some(200));
    assert_eq!(facts.loop_count, Some(4));
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), &gif).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let prepared = media::prepare_native_media_in(
        source.path(),
        scratch.path(),
        media::NativeMediaKind::Image,
        std::sync::Arc::new(NativePngPreparer {
            mutate_source: true,
            quota: true,
        }),
        None,
    )
    .await
    .unwrap();
    assert_eq!(prepared.main.mime_type, "image/webp");
    assert_eq!(prepared.output.frame_count, 2);
    assert_eq!(prepared.output.duration_ms, 200);
    assert_eq!(prepared.main.source_sha256, libtinkle::sha256(&gif));
    assert!(prepared.main.main_facts.as_ref().unwrap().animated);
    assert_eq!(prepared.main.path().parent(), Some(scratch.path()));
    drop(prepared);
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

/// Larger GIF fixtures normalize only after independent composited-frame verification.
/// Duplicate frames remain distinct and finite GIF repetitions become total WebP iterations.
#[test]
fn animated_gif_normalizes_losslessly_without_dropping_duplicate_frames() {
    let _test = MEDIA_TESTS.blocking_lock();
    use std::borrow::Cow;
    let palette: Vec<u8> = (0..=255u8)
        .flat_map(|n| [n, n.wrapping_mul(3), n.wrapping_mul(7)])
        .collect();
    let mut input = Vec::new();
    {
        let mut encoder = gif::Encoder::new(&mut input, 128, 128, &palette).unwrap();
        encoder.set_repeat(gif::Repeat::Finite(2)).unwrap();
        for n in 0..6u8 {
            let mut frame = gif::Frame {
                width: 128,
                height: 128,
                delay: 3 + u16::from(n),
                dispose: if n == 2 {
                    gif::DisposalMethod::Previous
                } else if n == 3 {
                    gif::DisposalMethod::Background
                } else {
                    gif::DisposalMethod::Keep
                },
                buffer: Cow::Owned(
                    (0..128 * 128)
                        .map(|i| (i as u8).wrapping_add(n / 2))
                        .collect(),
                ),
                ..Default::default()
            };
            if n == 4 {
                frame.transparent = Some(1);
            }
            encoder.write_frame(&frame).unwrap();
        }
    }
    let output = media::prepare_bytes(&input, media::MediaKind::Image).unwrap();
    assert_eq!(output.mime_type, "image/webp");
    assert!(output.bytes.len() < input.len());
    let facts = output.main_facts.as_ref().unwrap();
    assert_eq!(facts.frame_count, Some(6));
    assert_eq!(facts.loop_count, Some(3));
    use image::AnimationDecoder;
    let original = image::codecs::gif::GifDecoder::new(Cursor::new(&input))
        .unwrap()
        .into_frames()
        .collect_frames()
        .unwrap();
    let normalized = image::codecs::webp::WebPDecoder::new(Cursor::new(&output.bytes))
        .unwrap()
        .into_frames()
        .collect_frames()
        .unwrap();
    assert_eq!(original.len(), normalized.len());
    for (a, b) in original.iter().zip(&normalized) {
        assert_eq!(a.delay(), b.delay());
        assert_eq!(a.buffer(), b.buffer());
    }
    let thumb = image::load_from_memory(output.thumbnail.as_ref().unwrap()).unwrap();
    assert_eq!((thumb.width(), thumb.height()), (128, 128));
    assert_eq!(
        output.thumbnail_facts.as_ref().unwrap().frame_count,
        Some(1)
    );
    let again = media::prepare_bytes(&output.bytes, media::MediaKind::Image).unwrap();
    assert_eq!(again.bytes, output.bytes);
    assert!(again.main_facts.as_ref().unwrap().animated);
    assert!(again.thumbnail.is_some());
}

/// APNG remains an animation while its list thumbnail is a bounded static PNG.
#[test]
fn preserved_apng_has_static_thumbnail_and_complete_facts() {
    let _test = MEDIA_TESTS.blocking_lock();
    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut bytes, 4, 2);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        encoder.set_animated(2, 3).unwrap();
        let mut writer = encoder.write_header().unwrap();
        writer.set_frame_delay(3, 100).unwrap();
        writer.write_image_data(&[30; 32]).unwrap();
        writer.set_frame_delay(17, 100).unwrap();
        writer.write_image_data(&[90; 32]).unwrap();
        writer.finish().unwrap();
    }
    let result = media::prepare_bytes(&bytes, media::MediaKind::Image).unwrap();
    assert_ne!(result.bytes, bytes);
    let facts = result.main_facts.as_ref().unwrap();
    assert_eq!(facts.frame_count, Some(2));
    assert_eq!(facts.duration_ms, Some(200));
    assert_eq!(facts.loop_count, Some(3));
    assert!(
        facts.strict_lossless_v1(),
        "APNG must normalize to lossless animated WebP"
    );
    assert_eq!(
        result.thumbnail_facts.as_ref().unwrap().frame_count,
        Some(1)
    );
}

/// Decoding the raster is insufficient if the final container checksum/trailer is corrupt.
#[test]
fn image_container_trailers_are_validated() {
    let _test = MEDIA_TESTS.blocking_lock();
    let mut png = Vec::new();
    image::codecs::png::PngEncoder::new(&mut png)
        .write_image(&[10, 20, 30, 40], 1, 1, image::ExtendedColorType::Rgba8)
        .unwrap();
    *png.last_mut().unwrap() ^= 1;
    assert!(media::prepare_bytes(&png, media::MediaKind::Image).is_err());
}

/// JPEG EXIF orientation applies to generated thumbnails, not just width/height labels.
#[test]
fn exif_orientation_changes_thumbnail_pixels() {
    let _test = MEDIA_TESTS.blocking_lock();
    let pixels = image::RgbImage::from_fn(8, 4, |x, _| {
        if x < 4 {
            image::Rgb([240, 0, 0])
        } else {
            image::Rgb([0, 0, 240])
        }
    });
    let mut jpeg = Vec::new();
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg, 100)
        .write_image(pixels.as_raw(), 8, 4, image::ExtendedColorType::Rgb8)
        .unwrap();
    let exif: &[u8] = b"Exif\0\0II\x2a\0\x08\0\0\0\x01\0\x12\x01\x03\0\x01\0\0\0\x06\0\0\0\0\0\0\0";
    let mut oriented = jpeg[..2].to_vec();
    oriented.extend_from_slice(&[255, 225]);
    oriented.extend_from_slice(&((exif.len() + 2) as u16).to_be_bytes());
    oriented.extend_from_slice(exif);
    oriented.extend_from_slice(&jpeg[2..]);
    let result = media::prepare_bytes(&oriented, media::MediaKind::Image).unwrap();
    let thumbnail = image::load_from_memory(&result.thumbnail.unwrap())
        .unwrap()
        .to_rgb8();
    let expected = image::load_from_memory(&jpeg).unwrap().rotate90().to_rgb8();
    assert_eq!(thumbnail, expected);
}

/// High-depth source pixels stay exact rather than being reduced to an 8-bit WebP.
#[test]
fn sixteen_bit_png_is_preserved() {
    let _test = MEDIA_TESTS.blocking_lock();
    let mut image = image::ImageBuffer::<image::Rgba<u16>, Vec<u16>>::new(2, 1);
    image.put_pixel(0, 0, image::Rgba([1, 65534, 12345, 0]));
    image.put_pixel(1, 0, image::Rgba([65535, 1025, 3, 32767]));
    let mut png = Cursor::new(Vec::new());
    image::DynamicImage::ImageRgba16(image)
        .write_to(&mut png, image::ImageFormat::Png)
        .unwrap();
    let result = media::prepare_bytes(png.get_ref(), media::MediaKind::Image).unwrap();
    assert_eq!(result.bytes, *png.get_ref());
}

use std::io::Cursor;

/// Signed 24-bit integer minima/maxima survive without quantization or resampling.
#[test]
fn twenty_four_bit_flac_is_exact() {
    let _test = MEDIA_TESTS.blocking_lock();
    let samples = [-8388608i32, -1, 0, 1, 8388607];
    let pcm: Vec<u8> = samples
        .iter()
        .flat_map(|s| s.to_le_bytes()[..3].to_vec())
        .collect();
    let result = media::prepare_bytes(
        &pcm,
        media::MediaKind::Voice(media::PcmFormat {
            channels: 1,
            sample_rate: 44100,
            bits_per_sample: 24,
        }),
    )
    .unwrap();
    let mut decoder = claxon::FlacReader::new(Cursor::new(result.bytes)).unwrap();
    assert_eq!(decoder.streaminfo().bits_per_sample, 24);
    assert_eq!(
        decoder.samples().collect::<Result<Vec<_>, _>>().unwrap(),
        samples
    );
}

/// Documents larger than all codec input budgets still use the streaming exact-byte path.
#[tokio::test]
async fn large_document_disk_preparation_and_cancel() {
    let _test = MEDIA_TESTS.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("document");
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(40 * 1024 * 1024).unwrap();
    drop(file);
    let prepared = media::prepare_path(&path, media::MediaKind::Document, None)
        .await
        .unwrap();
    assert_eq!(
        std::fs::metadata(prepared.path()).unwrap().len(),
        40 * 1024 * 1024
    );
    assert_eq!(prepared.source_sha256, prepared.output_sha256);
    let token = libtinkle::CancelToken::new();
    token.cancel();
    assert!(matches!(
        media::prepare_path(&path, media::MediaKind::Document, Some(token)).await,
        Err(media::MediaError::Cancelled)
    ));
}

/// The opt-in adapter actually runs FFmpeg; absent binaries are an explicit skip lane.
#[tokio::test]
async fn real_ffmpeg_remux_and_thumbnail() {
    let _test = MEDIA_TESTS.lock().await;
    let executable = std::env::var_os("TINKLE_TEST_FFMPEG").map(std::path::PathBuf::from);
    let Some(executable) = executable else {
        eprintln!("SKIP real FFmpeg: set TINKLE_TEST_FFMPEG to an absolute executable");
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.mp4");
    let status = std::process::Command::new(&executable)
        .args([
            "-nostdin",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=red:s=64x32:r=2:d=1",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
        ])
        .arg(&source)
        .status()
        .unwrap();
    assert!(status.success());
    let output = media::FfmpegAdapter::new(executable.clone())
        .unwrap()
        .prepare_video(&source, None)
        .await
        .unwrap();
    let thumbnail = image::load_from_memory(output.thumbnail.as_ref().unwrap()).unwrap();
    assert_eq!((thumbnail.width(), thumbnail.height()), (64, 32));
    let frames = |path: &std::path::Path| {
        std::process::Command::new(&executable)
            .args(["-nostdin", "-v", "error", "-i"])
            .arg(path)
            .args([
                "-map", "0:v:0", "-f", "rawvideo", "-pix_fmt", "rgb24", "pipe:1",
            ])
            .output()
            .unwrap()
            .stdout
    };
    assert_eq!(frames(&source), frames(output.path()));
}

/// Common video header facts are informative only, never a qualified authoring result.
#[test]
fn video_header_inspection_does_not_claim_decode_or_reencoding() {
    let _test = MEDIA_TESTS.blocking_lock();
    let bytes = [
        0, 0, 0, 20, b'f', b't', b'y', b'p', b'i', b's', b'o', b'm', 0, 0, 0, 0, b'i', b's', b'o',
        b'm',
    ];
    let facts = media::inspect_video_bytes(&bytes).unwrap();
    assert_eq!(facts.evidence, media::MediaEvidence::HeaderOnly);
    assert!(!facts.strict_lossless_v1());
    assert!(matches!(
        media::prepare_bytes(&bytes, media::MediaKind::Video),
        Err(media::MediaError::Unsupported)
    ));
}

/// A metadata-only native callback remains Unsupported even if it supplies dimensions.
#[tokio::test]
async fn metadata_only_host_video_decoder_cannot_author_pass_through() {
    let _test = MEDIA_TESTS.lock().await;
    struct HeaderOnly;
    #[async_trait::async_trait]
    impl media::PlatformVideoDecoder for HeaderOnly {
        async fn inspect_and_decode(
            &self,
            input: std::sync::Arc<media::VideoInput>,
            _limits: media::VideoDecodeLimits,
            _cancel: libtinkle::CancelToken,
        ) -> Result<media::DecodedVideo, media::MediaError> {
            let bytes = std::fs::read(input.path()).unwrap();
            Ok(media::DecodedVideo {
                facts: media::inspect_video_bytes(&bytes)?,
                thumbnail: None,
            })
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("video.mp4");
    std::fs::write(
        &path,
        [
            0, 0, 0, 20, b'f', b't', b'y', b'p', b'i', b's', b'o', b'm', 0, 0, 0, 0, b'i', b's',
            b'o', b'm',
        ],
    )
    .unwrap();
    assert!(matches!(
        media::prepare_video_with_decoder(&path, std::sync::Arc::new(HeaderOnly), None).await,
        Err(media::MediaError::Unsupported)
    ));
}

/// This is a host-retention contract test, not an implemented platform decoder.
/// Timeout cannot free the source/admission while foreign native work retains its input.
#[tokio::test]
async fn foreign_video_read_lease_retains_admission_after_timeout() {
    let _test = MEDIA_TESTS.lock().await;
    #[derive(Default)]
    struct Retained {
        inputs: std::sync::Mutex<Vec<std::sync::Arc<media::VideoInput>>>,
        entered: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl media::PlatformVideoDecoder for Retained {
        async fn inspect_and_decode(
            &self,
            input: std::sync::Arc<media::VideoInput>,
            _limits: media::VideoDecodeLimits,
            _cancel: libtinkle::CancelToken,
        ) -> Result<media::DecodedVideo, media::MediaError> {
            self.inputs.lock().unwrap().push(input);
            self.entered.notify_one();
            std::future::pending().await
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    std::fs::write(&path, b"opaque video fixture").unwrap();
    let decoder = std::sync::Arc::new(Retained::default());
    let tasks: Vec<_> = (0..2)
        .map(|_| {
            let path = path.clone();
            let decoder = decoder.clone();
            tokio::spawn(
                async move { media::prepare_video_with_decoder(&path, decoder, None).await },
            )
        })
        .collect();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let entered = decoder.entered.notified();
            if decoder.inputs.lock().unwrap().len() == 2 {
                break;
            }
            entered.await;
        }
    })
    .await
    .expect("both native workers must enter before advancing their deadlines");
    assert_eq!(decoder.inputs.lock().unwrap().len(), 2);
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_secs(11)).await;
    for task in tasks {
        assert!(matches!(
            task.await.unwrap(),
            Err(media::MediaError::Budget)
        ));
    }
    assert!(decoder
        .inputs
        .lock()
        .unwrap()
        .iter()
        .all(|input| input.path().exists()));
    assert!(matches!(
        media::prepare_video_with_decoder(&path, decoder.clone(), None).await,
        Err(media::MediaError::Busy)
    ));
    decoder.inputs.lock().unwrap().clear();
}
