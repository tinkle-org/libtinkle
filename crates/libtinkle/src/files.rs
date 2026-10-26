//! Rust-owned file transfer managers.
//!
//! - [`FileManager`]: canonical `(scope, file_id)` identity, shared ref
//!   registration (same `file_id` across messages/entities shares one
//!   `files` row), pin / active-reader honoring eviction, refcounts via
//!   `Storage::file_refcount`.
//! - [`UploadManager`]: `BeginUpload` → `UploadPart` → `CompleteUpload`
//!   with PR #54 semantics (decide SOLELY on `BeginUploadResponse.status`,
//!   preserve the EXACT original request incl. optional presence for
//!   idempotency retries, fixed part size, durable part acks,
//!   snapshot-consistent resume, digest-error split, ambiguous-timeout
//!   status checks, `file_transfers` persistence, zero-byte fast path).
//! - [`DownloadManager`]: `GetFileInfo` → shared cache lookup /
//!   join-in-flight → `DownloadPart` ranges → size + SHA-256 verify →
//!   atomic publish (temp file + rename on the same filesystem).
//!
//! File refs are projected alongside messages/entities by
//! `cache_projector` (see `project_message_files_to_ops` /
//! `project_entity_files_to_ops`); ingest paths (`ingest_messages`,
//! journal hydration) therefore carry file refs in the same fenced
//! transaction as bodies.

use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};

use tinkle_schema::proto::{
    BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest, DownloadPartResponse, FileInfo,
    GetFileInfoResponse, GetUploadStatusRequest, Message, StickerView, UploadStatus, User,
};

use crate::errors::TinkleError;

#[cfg(not(target_arch = "wasm32"))]
use tinkle_storage::Storage;

// ---------------------------------------------------------------------------
// SHA-256 (vendored, no new deps so `--locked` stays green).
// ---------------------------------------------------------------------------

fn sha256_compress(h: &mut [u32; 8], block: &[u8; 64]) {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut w = [0u32; 64];
    for i in 0..16 {
        w[i] = u32::from_be_bytes([
            block[4 * i],
            block[4 * i + 1],
            block[4 * i + 2],
            block[4 * i + 3],
        ]);
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16]
            .wrapping_add(s0)
            .wrapping_add(w[i - 7])
            .wrapping_add(s1);
    }
    let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
        (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
    for i in 0..64 {
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ ((!e) & g);
        let t1 = hh
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(K[i])
            .wrapping_add(w[i]);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let t2 = s0.wrapping_add(maj);
        hh = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);
    }
    h[0] = h[0].wrapping_add(a);
    h[1] = h[1].wrapping_add(b);
    h[2] = h[2].wrapping_add(c);
    h[3] = h[3].wrapping_add(d);
    h[4] = h[4].wrapping_add(e);
    h[5] = h[5].wrapping_add(f);
    h[6] = h[6].wrapping_add(g);
    h[7] = h[7].wrapping_add(hh);
}

/// Compute SHA-256 over `data` (FIPS 180-4). Used to verify downloads and
/// to synthesize the retry digest when a `CompleteUpload`-supplied digest
/// was wrong but the `BeginUpload` declaration was absent.
///
/// Full 64-byte blocks are hashed in place; only the final padded tail
/// (one or two blocks) is copied.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    // Initial hash values.
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let (chunks, rem) = data.as_chunks::<64>();
    for block in chunks {
        sha256_compress(&mut h, block);
    }
    let bit_len = (data.len() as u64).wrapping_mul(8);
    if rem.len() < 56 {
        let mut block = [0u8; 64];
        block[..rem.len()].copy_from_slice(rem);
        block[rem.len()] = 0x80;
        block[56..].copy_from_slice(&bit_len.to_be_bytes());
        sha256_compress(&mut h, &block);
    } else {
        let mut first = [0u8; 64];
        first[..rem.len()].copy_from_slice(rem);
        first[rem.len()] = 0x80;
        sha256_compress(&mut h, &first);
        let mut second = [0u8; 64];
        second[56..].copy_from_slice(&bit_len.to_be_bytes());
        sha256_compress(&mut h, &second);
    }
    let mut out = [0u8; 32];
    for (i, v) in h.iter().enumerate() {
        out[4 * i..4 * i + 4].copy_from_slice(&v.to_be_bytes());
    }
    out
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let val = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    let mut i = 0;
    while i < bytes.len() {
        let hi = val(bytes[i])?;
        let lo = val(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// Canonical identity + file-ref extraction.
// ---------------------------------------------------------------------------

/// Global scope for shared file records. Same `file_id` across messages /
/// entities shares one `files` row (first-wins immutable metadata), with
/// per-message / per-entity refs in `message_files` / `entity_files`.
pub const SHARED_FILE_SCOPE: &str = "";

/// Conservative fallback when `GetFileInfoResponse.max_download_part_bytes`
/// is absent (non-conforming server). Only non-positive limits are rejected
/// in that case; any positive limit up to this bound is requestable.
pub const FALLBACK_MAX_DOWNLOAD_PART_BYTES: i32 = 64 * 1024;

/// Roles used for message file refs.
pub const ROLE_PHOTO: &str = "photo";
pub const ROLE_PHOTO_VARIANT: &str = "photo_variant";
pub const ROLE_DOCUMENT: &str = "document";
pub const ROLE_DOCUMENT_VARIANT: &str = "document_variant";
pub const ROLE_AVATAR: &str = "avatar";
pub const ROLE_AVATAR_VARIANT: &str = "avatar_variant";
pub const ROLE_STICKER: &str = "sticker";
pub const ROLE_STICKER_THUMB: &str = "sticker_thumb";

fn valid_file_id(id: i64) -> bool {
    id > 0
}

/// All `(file_id, role)` pairs referenced by one message body:
/// `Photo.file_id` + `Photo.variants`, `Document.file_id` + variants.
pub fn extract_message_files(msg: &Message) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    let media = match msg.media.as_ref().and_then(|m| m.media.as_ref()) {
        Some(m) => m,
        None => return out,
    };
    match media {
        tinkle_schema::proto::media::Media::Photo(p) => {
            if valid_file_id(p.file_id) {
                out.push((p.file_id, ROLE_PHOTO.to_string()));
            }
            for v in &p.variants {
                if valid_file_id(v.file_id) {
                    out.push((v.file_id, ROLE_PHOTO_VARIANT.to_string()));
                }
            }
        }
        tinkle_schema::proto::media::Media::Document(d) => {
            if valid_file_id(d.file_id) {
                out.push((d.file_id, ROLE_DOCUMENT.to_string()));
            }
            for v in &d.variants {
                if valid_file_id(v.file_id) {
                    out.push((v.file_id, ROLE_DOCUMENT_VARIANT.to_string()));
                }
            }
        }
        _ => {}
    }
    out
}

/// All `(file_id, role)` pairs referenced by one user: `UserPhoto.file_id`
/// + variants. Empty users carry no photo.
pub fn extract_user_files(user: &User) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    let data = match user.user_type.as_ref() {
        Some(tinkle_schema::proto::user::UserType::Data(d)) => d,
        _ => return out,
    };
    if let Some(photo) = data.photo.as_ref() {
        if valid_file_id(photo.file_id) {
            out.push((photo.file_id, ROLE_AVATAR.to_string()));
        }
        for v in &photo.variants {
            if valid_file_id(v.file_id) {
                out.push((v.file_id, ROLE_AVATAR_VARIANT.to_string()));
            }
        }
    }
    out
}

/// Entity id for a user row (0 when the user id is not positive).
pub fn user_entity_id(user: &User) -> i64 {
    match user.user_type.as_ref() {
        Some(tinkle_schema::proto::user::UserType::Data(d)) if d.id > 0 => d.id,
        Some(tinkle_schema::proto::user::UserType::Empty(e)) if e.id > 0 => e.id,
        _ => 0,
    }
}

/// All `(file_id, role)` pairs for one sticker view: `file_id` +
/// `thumb_file_id`.
pub fn extract_sticker_files(sticker: &StickerView) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    if valid_file_id(sticker.file_id) {
        out.push((sticker.file_id, ROLE_STICKER.to_string()));
    }
    if valid_file_id(sticker.thumb_file_id) && sticker.thumb_file_id != sticker.file_id {
        out.push((sticker.thumb_file_id, ROLE_STICKER_THUMB.to_string()));
    }
    out
}

#[cfg(not(target_arch = "wasm32"))]
fn file_row_for(file_id: i64, origin_stream: &str) -> tinkle_storage::FileRow {
    tinkle_storage::FileRow {
        scope: SHARED_FILE_SCOPE.to_string(),
        file_id,
        size_bytes: 0,
        sha256: Vec::new(),
        mime_type: String::new(),
        file_name: String::new(),
        encrypted: false,
        created_at: 0,
        origin_stream: origin_stream.to_string(),
    }
}

/// Project file refs for one message into `CacheOps` (shared `files` rows
/// first-wins + idempotent `message_files` refs). Callers merge the result
/// alongside message bodies in the same fenced transaction.
#[cfg(not(target_arch = "wasm32"))]
pub fn project_message_files_to_ops(
    target_key: &str,
    message_id: i64,
    msg: &Message,
) -> tinkle_storage::CacheOps {
    let mut ops = tinkle_storage::CacheOps::default();
    if message_id <= 0 {
        return ops;
    }
    let mut seen: HashSet<(i64, String)> = HashSet::new();
    for (fid, role) in extract_message_files(msg) {
        if !seen.insert((fid, role.clone())) {
            continue;
        }
        ops.files.push(file_row_for(fid, target_key));
        ops.message_files.push(tinkle_storage::MessageFileRef {
            conversation_key: target_key.to_string(),
            message_id,
            file_scope: SHARED_FILE_SCOPE.to_string(),
            file_id: fid,
            role,
        });
    }
    ops
}

/// Project entity file refs (user avatars) into `CacheOps`. Chat photos
/// carry no `file_id` (`ChatPhoto` is `{photo_id, has_video}`), so chats
/// contribute no refs today; the hook is kept so future photo-carrying
/// chat shapes reuse the same shared-record path.
#[cfg(not(target_arch = "wasm32"))]
pub fn project_entity_files_to_ops(
    users: &[User],
    chats: &[tinkle_schema::proto::Chat],
) -> tinkle_storage::CacheOps {
    let mut ops = tinkle_storage::CacheOps::default();
    let _ = chats;
    let mut seen_files: HashSet<i64> = HashSet::new();
    let mut seen_entities: HashSet<i64> = HashSet::new();
    for u in users {
        let eid = user_entity_id(u);
        if eid <= 0 {
            continue;
        }
        // Replace marker even when the new ref set is empty so avatar
        // removals clear stale refs transactionally.
        if seen_entities.insert(eid) {
            ops.replace_entity_files.push(("user".to_string(), eid));
        }
        for (fid, role) in extract_user_files(u) {
            if seen_files.insert(fid) {
                // Origin is pinned globally for entity hydration when the
                // caller commits outside a journal page; journal callers
                // overwrite with the page target via `with_origin`.
                ops.files.push(file_row_for(fid, ""));
            }
            ops.entity_files.push(tinkle_storage::EntityFileRef {
                kind: "user".to_string(),
                entity_id: eid,
                file_scope: SHARED_FILE_SCOPE.to_string(),
                file_id: fid,
                role,
            });
        }
    }
    ops
}

/// Project sticker-pack file refs (stickers carry `file_id` +
/// `thumb_file_id`) into `CacheOps` with `kind = "sticker"`.
#[cfg(not(target_arch = "wasm32"))]
pub fn project_sticker_files_to_ops(stickers: &[StickerView]) -> tinkle_storage::CacheOps {
    let mut ops = tinkle_storage::CacheOps::default();
    let mut seen: HashSet<i64> = HashSet::new();
    let mut seen_entities: HashSet<i64> = HashSet::new();
    for s in stickers {
        if s.id <= 0 {
            continue;
        }
        if seen_entities.insert(s.id) {
            ops.replace_entity_files.push(("sticker".to_string(), s.id));
        }
        for (fid, role) in extract_sticker_files(s) {
            if seen.insert(fid) {
                ops.files.push(file_row_for(fid, ""));
            }
            ops.entity_files.push(tinkle_storage::EntityFileRef {
                kind: "sticker".to_string(),
                entity_id: s.id,
                file_scope: SHARED_FILE_SCOPE.to_string(),
                file_id: fid,
                role,
            });
        }
    }
    ops
}

/// Rewrite placeholder origins (`""`) to a journal target so the batch
/// passes `apply_journal_page_fenced` validation
/// (`file origin_stream must equal page target_key`).
/// Kept for journal callers that commit outside `merge_ops`.
#[allow(dead_code)]
#[cfg(not(target_arch = "wasm32"))]
pub fn with_origin(
    mut ops: tinkle_storage::CacheOps,
    target_key: &str,
) -> tinkle_storage::CacheOps {
    for f in &mut ops.files {
        if f.origin_stream.is_empty() {
            f.origin_stream = target_key.to_string();
        }
    }
    ops
}

// ---------------------------------------------------------------------------
// FileManager.
// ---------------------------------------------------------------------------

/// Rust-owned file identity + eviction manager.
///
/// Canonical identity is `(scope, file_id)`; message/entity ingest reuses
/// shared records (scope `""`) so the same `file_id` across messages and
/// entities shares one `files` row. Eviction honors `pinned` cache rows and
/// the active-reader counts; refcounts come from storage (`file_refcount`).
#[derive(Clone)]
pub struct FileManager {
    #[cfg(not(target_arch = "wasm32"))]
    storage: Option<Arc<Storage>>,
    active: Arc<Mutex<HashMap<(String, i64), u64>>>,
}

impl std::fmt::Debug for FileManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileManager").finish_non_exhaustive()
    }
}

impl Default for FileManager {
    fn default() -> Self {
        Self::new()
    }
}

impl FileManager {
    /// Manager without durable storage (wasm / tests): refcounts read 0,
    /// eviction is a no-op, active-reader tracking still works.
    pub fn new() -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            storage: None,
            active: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Manager backed by shared native SQLite storage.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_storage(storage: Arc<Storage>) -> Self {
        Self {
            storage: Some(storage),
            active: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Total live refs (`message_files` + `entity_files`) for one file.
    /// Returns 0 without storage (nothing durable to count).
    pub fn refcount(&self, principal: &str, scope: &str, file_id: i64) -> Result<i64, TinkleError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(s) = self.storage.as_ref() {
            return s
                .file_refcount(principal, scope, file_id)
                .map_err(|e| TinkleError::CursorStore(e.to_string()));
        }
        let _ = (principal, scope, file_id);
        Ok(0)
    }

    /// Mark `(scope, file_id)` as actively read (open stream / decode).
    /// Eviction skips active readers even when unpinned. Counts nest:
    /// each `begin_read` needs a matching `end_read`.
    pub fn begin_read(&self, scope: &str, file_id: i64) {
        if let Ok(mut g) = self.active.lock() {
            *g.entry((scope.to_string(), file_id)).or_insert(0) += 1;
        }
    }

    /// Release one active-reader hold; the key is removed at zero.
    pub fn end_read(&self, scope: &str, file_id: i64) {
        if let Ok(mut g) = self.active.lock() {
            let key = (scope.to_string(), file_id);
            match g.get_mut(&key) {
                Some(c) if *c <= 1 => {
                    g.remove(&key);
                }
                Some(c) => {
                    *c -= 1;
                }
                None => {}
            }
        }
    }

    /// True while at least one reader holds `(scope, file_id)`.
    pub fn is_reading(&self, scope: &str, file_id: i64) -> bool {
        self.active
            .lock()
            .map(|g| g.get(&(scope.to_string(), file_id)).copied().unwrap_or(0) > 0)
            .unwrap_or(false)
    }

    /// Pure eviction predicate (testable without storage): never evict
    /// pinned rows or active readers.
    pub fn should_evict(&self, scope: &str, file_id: i64, pinned: bool) -> bool {
        if pinned {
            return false;
        }
        !self.is_reading(scope, file_id)
    }

    /// Resolve `relative_path` strictly under `cache_dir`. Returns `None`
    /// for empty, absolute, or non-normal paths (`..`, `.`, prefixes,
    /// root components) and for joins that escape `cache_dir` after
    /// lexical resolution. Callers skip the bytes delete on `None` but
    /// still apply the authoritative DB delete.
    #[cfg(not(target_arch = "wasm32"))]
    fn safe_cache_path(
        cache_dir: &std::path::Path,
        relative_path: &str,
    ) -> Option<std::path::PathBuf> {
        use std::path::Component;
        if relative_path.is_empty() {
            return None;
        }
        let rel = std::path::Path::new(relative_path);
        if rel.is_absolute() {
            return None;
        }
        for c in rel.components() {
            if !matches!(c, Component::Normal(_)) {
                return None;
            }
        }
        let joined = cache_dir.join(rel);
        // Lexical containment: a `Normal`-only relative join cannot escape
        // via `..`, but symlinked prefixes could still resolve outside, so
        // verify against the canonical cache dir when both sides resolve.
        if let (Ok(canonical_dir), Ok(canonical_joined)) =
            (cache_dir.canonicalize(), joined.canonicalize())
        {
            if !canonical_joined.starts_with(&canonical_dir) {
                return None;
            }
            return Some(canonical_joined);
        }
        // Target may not exist (already evicted): fall back to the lexical
        // join, which is safe because every component was `Normal`.
        Some(joined)
    }

    /// Evict unpinned, unread cache rows for one principal. Returns the
    /// number of `file_cache_entries` rows removed (bytes on disk are
    /// best-effort removed when `cache_dir` is supplied and the stored
    /// `relative_path` resolves under it).
    ///
    /// Refcount is advisory: rows with live refs are kept when
    /// `keep_referenced` is set (default), so a re-download is avoided.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn evict_unpinned(
        &self,
        principal: &str,
        cache_dir: Option<&std::path::Path>,
        keep_referenced: bool,
    ) -> Result<usize, TinkleError> {
        let storage = match self.storage.as_ref() {
            Some(s) => s.clone(),
            None => return Ok(0),
        };
        let entries = storage
            .list_file_cache_entries(principal)
            .map_err(|e| TinkleError::CursorStore(e.to_string()))?;
        let mut removed = 0usize;
        for e in entries {
            if e.pinned || self.is_reading(&e.scope, e.file_id) {
                continue;
            }
            if keep_referenced {
                let refs = storage
                    .file_refcount(principal, &e.scope, e.file_id)
                    .map_err(|e| TinkleError::CursorStore(e.to_string()))?;
                if refs > 0 {
                    continue;
                }
            }
            // Best-effort bytes delete; DB row delete is authoritative.
            // Unsafe paths never touch the filesystem.
            if let Some(dir) = cache_dir {
                if let Some(p) = Self::safe_cache_path(dir, &e.relative_path) {
                    let _ = std::fs::remove_file(p);
                }
            }
            storage
                .delete_file_cache_entry(principal, &e.scope, e.file_id)
                .map_err(|e| TinkleError::CursorStore(e.to_string()))?;
            removed += 1;
        }
        Ok(removed)
    }

    /// Wasm eviction: no filesystem or storage scan; active-reader and pin
    /// checks still apply via [`Self::should_evict`].
    #[cfg(target_arch = "wasm32")]
    pub fn evict_unpinned(&self, _principal: &str) -> Result<usize, TinkleError> {
        Ok(0)
    }
}

// ---------------------------------------------------------------------------
// UploadManager (PR #54 semantics).
// ---------------------------------------------------------------------------

/// Upload inputs (bytes in). `sha256` is the optional client declaration
/// fixed for the session lifetime; `None` vs `Some` presence is preserved
/// exactly across idempotency retries.
#[derive(Debug, Clone)]
pub struct UploadParams {
    pub idempotency_key: String,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub sha256: Option<Vec<u8>>,
    pub encrypted: bool,
}

impl UploadParams {
    pub fn new(idempotency_key: String, data_len: usize) -> Self {
        let _ = data_len;
        Self {
            idempotency_key,
            file_name: None,
            mime_type: None,
            sha256: None,
            encrypted: false,
        }
    }
}

/// Decision decoded SOLELY from `BeginUploadResponse.status` (never from
/// absent/zero config): ACTIVE carries config, COMPLETED carries the file,
/// ABORTED/EXPIRED need a fresh key.
#[derive(Debug)]
pub enum BeginDecision {
    Active { upload_id: String, part_size: i64 },
    Completed(FileInfo),
    NeedFreshKey,
}

/// Decode `BeginUploadResponse` by status only (PR #54: never infer
/// terminal state from absent config).
pub fn decide_begin_status(resp: &BeginUploadResponse) -> Result<BeginDecision, TinkleError> {
    let status = UploadStatus::try_from(resp.status)
        .map_err(|_| TinkleError::Codec(format!("unknown upload status {}", resp.status)))?;
    match status {
        UploadStatus::Active => {
            let part_size = resp.part_size_bytes.ok_or_else(|| {
                TinkleError::Codec("BeginUpload ACTIVE must carry part_size_bytes".to_string())
            })?;
            if part_size <= 0 {
                return Err(TinkleError::Codec(
                    "BeginUpload ACTIVE part_size_bytes must be positive".to_string(),
                ));
            }
            if resp.upload_id.is_empty() {
                return Err(TinkleError::Codec(
                    "BeginUpload ACTIVE must carry upload_id".to_string(),
                ));
            }
            if resp.file.is_some() {
                return Err(TinkleError::Codec(
                    "BeginUpload ACTIVE must not carry file".to_string(),
                ));
            }
            Ok(BeginDecision::Active {
                upload_id: resp.upload_id.clone(),
                part_size,
            })
        }
        UploadStatus::Completed => {
            let file = resp.file.clone().ok_or_else(|| {
                TinkleError::Codec("BeginUpload COMPLETED must carry file".to_string())
            })?;
            Ok(BeginDecision::Completed(file))
        }
        UploadStatus::Aborted | UploadStatus::Expired => Ok(BeginDecision::NeedFreshKey),
        UploadStatus::Unspecified => Err(TinkleError::Codec(
            "BeginUpload returned UPLOAD_STATUS_UNSPECIFIED".to_string(),
        )),
    }
}

/// Number of parts for `size` bytes with fixed `part_size`.
pub fn part_count(size_bytes: i64, part_size: i64) -> Result<i64, TinkleError> {
    if size_bytes < 0 {
        return Err(TinkleError::Codec("size_bytes must be non-negative".into()));
    }
    if part_size <= 0 {
        return Err(TinkleError::Codec("part_size must be positive".into()));
    }
    if size_bytes == 0 {
        return Ok(0);
    }
    Ok(div_ceil_i64(size_bytes, part_size))
}

fn div_ceil_i64(a: i64, b: i64) -> i64 {
    let d = a / b;
    let r = a % b;
    if r != 0 && ((r > 0) == (b > 0)) {
        d + 1
    } else {
        d
    }
}

/// Byte range `[offset, offset+len)` for one part index (fixed part size;
/// final part completes the declared total).
pub fn part_byte_range(
    part_index: i64,
    part_size: i64,
    total_size: i64,
) -> Result<(i64, usize), TinkleError> {
    let n = part_count(total_size, part_size)?;
    if part_index < 0 || part_index >= n {
        return Err(TinkleError::Codec(format!(
            "part_index {part_index} out of range 0..{n}"
        )));
    }
    let offset = part_index * part_size;
    let len = (total_size - offset).min(part_size) as usize;
    Ok((offset, len))
}

/// Expand snapshot `received_ranges` into a set of confirmed part indexes,
/// clamped to `0..part_count` (server ranges are authoritative; out-of-range
/// entries are ignored, never trusted blindly).
pub fn received_set(
    ranges: &[tinkle_schema::proto::PartIndexRange],
    size_bytes: i64,
    part_size: i64,
) -> HashSet<i64> {
    let mut out = HashSet::new();
    let n = part_count(size_bytes, part_size).unwrap_or(0);
    for r in ranges {
        let s = r.start_index.max(0);
        let e = r.end_index.min(n);
        let mut i = s;
        while i < e {
            out.insert(i);
            i += 1;
        }
    }
    out
}

#[allow(dead_code)]
fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Durable upload session persisted in `file_transfers.session_json`
/// (opaque to storage; owned by this module). Encoding is a stable
/// `|`-separated format (no serde dependency):
/// `v1|upload_id|part_size|size|received_csv|begin_has_sha|begin_sha_hex|
/// complete_sha_hex|file_name|mime_type|encrypted`.
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct UploadSession {
    upload_id: String,
    part_size: i64,
    size_bytes: i64,
    received: HashSet<i64>,
    begin_has_sha: bool,
    begin_sha: Vec<u8>,
    complete_sha: Option<Vec<u8>>,
    file_name: Option<String>,
    mime_type: Option<String>,
    encrypted: bool,
}

#[allow(dead_code)]
impl UploadSession {
    fn encode(&self) -> String {
        let mut idx: Vec<i64> = self.received.iter().copied().collect();
        idx.sort_unstable();
        let csv = idx
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "v1|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            self.upload_id,
            self.part_size,
            self.size_bytes,
            csv,
            if self.begin_has_sha { 1 } else { 0 },
            hex_encode(&self.begin_sha),
            self.complete_sha
                .as_ref()
                .map(|b| hex_encode(b))
                .unwrap_or_default(),
            self.file_name.clone().unwrap_or_default().replace('|', "/"),
            self.mime_type.clone().unwrap_or_default().replace('|', "/"),
            if self.encrypted { 1 } else { 0 },
        )
    }

    fn decode(s: &str) -> Option<Self> {
        let mut parts = s.split('|');
        if parts.next()? != "v1" {
            return None;
        }
        let upload_id = parts.next()?.to_string();
        let part_size: i64 = parts.next()?.parse().ok()?;
        let size_bytes: i64 = parts.next()?.parse().ok()?;
        let csv = parts.next().unwrap_or("");
        let mut received = HashSet::new();
        if !csv.is_empty() {
            for tok in csv.split(',') {
                if let Ok(i) = tok.parse::<i64>() {
                    received.insert(i);
                }
            }
        }
        let begin_has_sha = parts.next()? == "1";
        let begin_sha = hex_decode(parts.next().unwrap_or("")).unwrap_or_default();
        let complete_hex = parts.next().unwrap_or("");
        let complete_sha = if complete_hex.is_empty() {
            None
        } else {
            Some(hex_decode(complete_hex)?)
        };
        let file_name_raw = parts.next().unwrap_or("").to_string();
        let mime_raw = parts.next().unwrap_or("").to_string();
        let encrypted = parts.next().unwrap_or("0") == "1";
        Some(Self {
            upload_id,
            part_size,
            size_bytes,
            received,
            begin_has_sha,
            begin_sha,
            complete_sha,
            file_name: if file_name_raw.is_empty() {
                None
            } else {
                Some(file_name_raw)
            },
            mime_type: if mime_raw.is_empty() {
                None
            } else {
                Some(mime_raw)
            },
            encrypted,
        })
    }
}

#[allow(dead_code)]
fn upload_job_id(idempotency_key: &str) -> String {
    format!("upload:{idempotency_key}")
}

/// True when `e` leaves the handler outcome ambiguous (timeout / loss /
/// unknown): callers must status-check instead of blindly retrying a
/// non-idempotent step. `UploadPart`/`CompleteUpload` retries are idempotent
/// by content, so the check only decides whether the step already landed.
pub fn is_ambiguous(e: &TinkleError) -> bool {
    e.is_unknown_outcome()
}

/// Classify a `CompleteUpload` failure as a digest mismatch: the protocol
/// has no dedicated digest code, so both the code (`InvalidArgument` /
/// `FailedPrecondition`) AND a digest-specific detail (`digest` / `sha256`
/// / `checksum`) must match. All other failures stay on the normal path.
fn is_digest_error(e: &TinkleError) -> bool {
    use syntra_core::error::ErrorCode as C;
    fn detail_matches(msg: &str) -> bool {
        let lower = msg.to_ascii_lowercase();
        lower.contains("digest") || lower.contains("sha256") || lower.contains("checksum")
    }
    match e {
        TinkleError::Syntra(syntra_core::error::SyntraError::Rpc { code, message, .. }) => {
            (*code == C::InvalidArgument || *code == C::FailedPrecondition)
                && detail_matches(message)
        }
        TinkleError::GrpcStatus { code, message } => {
            (*code == C::InvalidArgument || *code == C::FailedPrecondition)
                && detail_matches(message)
        }
        _ => false,
    }
}

/// Grouped params for durable upload-session persistence (clippy
/// `too_many_arguments`: session fields travel together).
#[cfg(not(target_arch = "wasm32"))]
struct PersistSessionArgs<'a> {
    idempotency_key: &'a str,
    params: &'a UploadParams,
    upload_id: &'a str,
    part_size: i64,
    size_bytes: i64,
    received: &'a HashSet<i64>,
    complete_sha: Option<Vec<u8>>,
}

/// Rust-owned upload manager. Holds a `Client` clone (cheap) and optional
/// durable storage for `file_transfers` resume.
#[derive(Clone)]
pub struct UploadManager {
    client: crate::client::Client,
}

impl UploadManager {
    pub fn new(client: crate::client::Client) -> Self {
        Self { client }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn storage(&self) -> Option<Arc<Storage>> {
        self.client.sqlite_storage()
    }

    /// Upload `data` with PR #54 semantics. Returns the committed `FileInfo`.
    ///
    /// - Builds ONE `BeginUploadRequest` and preserves it EXACTLY (incl.
    ///   `Option` presence) across idempotency retries.
    /// - Decides SOLELY on `BeginUploadResponse.status`.
    /// - Zero-byte fast path: COMPLETED carries the file, no parts.
    /// - Fixed part size from the ACTIVE response; durable acks after every
    ///   confirmed part; snapshot-consistent resume via `GetUploadStatus`.
    /// - Digest split: `CompleteUpload`-supplied mismatch retries with the
    ///   computed digest; `BeginUpload`-declared mismatch aborts and reports
    ///   fatal (fresh key required).
    /// - Ambiguous timeouts status-check instead of double-applying.
    pub async fn upload_bytes(
        &self,
        params: UploadParams,
        data: Vec<u8>,
    ) -> Result<FileInfo, TinkleError> {
        if params.idempotency_key.is_empty() {
            return Err(TinkleError::Codec(
                "idempotency_key must be non-empty".into(),
            ));
        }
        if let Some(sha) = params.sha256.as_ref() {
            if sha.len() != 32 {
                return Err(TinkleError::Codec("sha256 must be 32 bytes".into()));
            }
        }
        let size = data.len() as i64;
        // EXACT original request (presence preserved): rebuilt byte-identical
        // on every idempotency retry below.
        let begin_req = BeginUploadRequest {
            idempotency_key: params.idempotency_key.clone(),
            size_bytes: size,
            file_name: params.file_name.clone(),
            mime_type: params.mime_type.clone(),
            sha256: params.sha256.clone().map(Into::into),
            encrypted: params.encrypted,
        };
        let active_key = params.idempotency_key.clone();
        let active_req = begin_req.clone();
        // ABORTED/EXPIRED → the caller must retry with a fresh key. The
        // SDK never derives one: the idempotency key is the caller's
        // contract, and silently swapping it could double-apply server-side.
        let begin_resp = match self.client.begin_upload(active_req.clone()).await {
            Ok(r) => r,
            Err(e) if is_ambiguous(&e) => {
                // Ambiguous Begin: the session may exist server-side.
                // Retry the IDENTICAL request (same key + same presence);
                // the server returns the same session.
                self.client.begin_upload(active_req.clone()).await?
            }
            Err(e) => return Err(e),
        };
        match decide_begin_status(&begin_resp)? {
            BeginDecision::Completed(file) => {
                #[cfg(not(target_arch = "wasm32"))]
                self.clear_session(&active_key).await;
                Ok(file)
            }
            BeginDecision::NeedFreshKey => Err(TinkleError::Sync(
                "upload session aborted/expired; retry with a fresh idempotency_key".into(),
            )),
            BeginDecision::Active {
                upload_id,
                part_size,
            } => {
                self.run_active_session(
                    &active_key,
                    &active_req,
                    &params,
                    upload_id,
                    part_size,
                    &data,
                )
                .await
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn clear_session(&self, idempotency_key: &str) {
        if let Some(s) = self.storage() {
            if let Ok(principal) = self.principal_or_unknown().await {
                let job_id = upload_job_id(idempotency_key);
                let s2 = s.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    s2.delete_file_transfer(&principal, &job_id)
                })
                .await;
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn principal_or_unknown(&self) -> Result<String, TinkleError> {
        Ok(self.client.principal().await)
    }

    async fn run_active_session(
        &self,
        idempotency_key: &str,
        begin_req: &BeginUploadRequest,
        params: &UploadParams,
        upload_id: String,
        part_size: i64,
        data: &[u8],
    ) -> Result<FileInfo, TinkleError> {
        let size = data.len() as i64;
        let n = part_count(size, part_size)?;
        // Zero-byte fast path: no parts; complete immediately.
        if n == 0 {
            let complete_sha: Option<Vec<u8>> = params.sha256.clone();
            match self
                .client
                .complete_upload(CompleteUploadRequest {
                    upload_id: upload_id.clone(),
                    sha256: complete_sha.clone().map(Into::into),
                })
                .await
            {
                Ok(r) => {
                    let file = r.file.ok_or_else(|| {
                        TinkleError::Codec("CompleteUpload must carry file".to_string())
                    })?;
                    #[cfg(not(target_arch = "wasm32"))]
                    self.clear_session(idempotency_key).await;
                    return Ok(file);
                }
                Err(e) if is_ambiguous(&e) => {
                    // Ambiguous Complete → status check.
                    let st = self
                        .client
                        .get_upload_status(GetUploadStatusRequest {
                            upload_id: upload_id.clone(),
                            page_token: None,
                            page_size: None,
                        })
                        .await?;
                    if UploadStatus::try_from(st.status).unwrap_or(UploadStatus::Unspecified)
                        == UploadStatus::Completed
                    {
                        let file = st.file.ok_or_else(|| {
                            TinkleError::Codec("GetUploadStatus COMPLETED must carry file".into())
                        })?;
                        #[cfg(not(target_arch = "wasm32"))]
                        self.clear_session(idempotency_key).await;
                        return Ok(file);
                    }
                    return Err(e);
                }
                Err(e) => return Err(e),
            }
        }
        // Load durable received set (resume) when storage is present.
        #[cfg(not(target_arch = "wasm32"))]
        let mut received: HashSet<i64> = {
            let mut r = self
                .load_session_received(idempotency_key, &upload_id)
                .await;
            // Snapshot-consistent resume: union with server-confirmed ranges
            // across ALL pages (same snapshot token chain).
            let server = self.snapshot_received(&upload_id, size, part_size).await?;
            r.extend(server);
            // Fixed part size: server echo must match negotiation.
            // (Checked inside snapshot_received; mismatch errors there.)
            self.persist_session(PersistSessionArgs {
                idempotency_key,
                params,
                upload_id: &upload_id,
                part_size,
                size_bytes: size,
                received: &r,
                complete_sha: params.sha256.clone(),
            })
            .await;
            r
        };
        #[cfg(target_arch = "wasm32")]
        let mut received: HashSet<i64> = {
            let _ = begin_req;
            // Wasm still resume-checks the server snapshot for this run.
            let mut r = HashSet::new();
            let server = self.snapshot_received(&upload_id, size, part_size).await?;
            r.extend(server);
            r
        };

        // Upload missing parts in index order.
        for idx in 0..n {
            if received.contains(&idx) {
                continue;
            }
            let (offset, len) = part_byte_range(idx, part_size, size)?;
            let chunk = data[offset as usize..offset as usize + len].to_vec();
            let part_sha = sha256(&chunk).to_vec();
            let req = tinkle_schema::proto::UploadPartRequest {
                upload_id: upload_id.clone(),
                part_index: idx,
                data: chunk.into(),
                sha256: Some(part_sha.into()),
            };
            match self.client.upload_part(req.clone()).await {
                Ok(_) => {
                    received.insert(idx);
                    #[cfg(not(target_arch = "wasm32"))]
                    self.persist_session(PersistSessionArgs {
                        idempotency_key,
                        params,
                        upload_id: &upload_id,
                        part_size,
                        size_bytes: size,
                        received: &received,
                        complete_sha: params.sha256.clone(),
                    })
                    .await;
                }
                Err(e) if is_ambiguous(&e) => {
                    // Ambiguous part → status check: confirmed? continue.
                    let server = self.snapshot_received(&upload_id, size, part_size).await?;
                    if server.contains(&idx) {
                        received.insert(idx);
                    } else {
                        // Not confirmed: retry once with the identical bytes.
                        self.client.upload_part(req).await?;
                        received.insert(idx);
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    self.persist_session(PersistSessionArgs {
                        idempotency_key,
                        params,
                        upload_id: &upload_id,
                        part_size,
                        size_bytes: size,
                        received: &received,
                        complete_sha: params.sha256.clone(),
                    })
                    .await;
                }
                Err(e) => return Err(e),
            }
        }
        // Complete with the caller-supplied digest when present (else the
        // computed digest of the exact bytes we sent).
        let computed = sha256(data).to_vec();
        let complete_sha: Option<Vec<u8>> = params.sha256.clone().or(Some(computed.clone()));
        match self
            .client
            .complete_upload(CompleteUploadRequest {
                upload_id: upload_id.clone(),
                sha256: complete_sha.clone().map(Into::into),
            })
            .await
        {
            Ok(r) => {
                let file = r
                    .file
                    .ok_or_else(|| TinkleError::Codec("CompleteUpload must carry file".into()))?;
                #[cfg(not(target_arch = "wasm32"))]
                self.clear_session(idempotency_key).await;
                Ok(file)
            }
            Err(e) if is_ambiguous(&e) => {
                let st = self
                    .client
                    .get_upload_status(GetUploadStatusRequest {
                        upload_id: upload_id.clone(),
                        page_token: None,
                        page_size: None,
                    })
                    .await?;
                if UploadStatus::try_from(st.status).unwrap_or(UploadStatus::Unspecified)
                    == UploadStatus::Completed
                {
                    let file = st.file.ok_or_else(|| {
                        TinkleError::Codec("GetUploadStatus COMPLETED must carry file".into())
                    })?;
                    #[cfg(not(target_arch = "wasm32"))]
                    self.clear_session(idempotency_key).await;
                    return Ok(file);
                }
                // Still active → parts were fine, retry Complete once.
                let r = self
                    .client
                    .complete_upload(CompleteUploadRequest {
                        upload_id: upload_id.clone(),
                        sha256: complete_sha.clone().map(Into::into),
                    })
                    .await?;
                let file = r
                    .file
                    .ok_or_else(|| TinkleError::Codec("CompleteUpload must carry file".into()))?;
                #[cfg(not(target_arch = "wasm32"))]
                self.clear_session(idempotency_key).await;
                Ok(file)
            }
            Err(e) if is_digest_error(&e) => {
                let begin_declared = begin_req.sha256.as_ref().map(|b| b.to_vec());
                if begin_declared.is_some() {
                    // Begin-declared digest unsatisfiable: fatal. Abort the
                    // session so it cannot leak, then require a fresh key.
                    let _ = self
                        .client
                        .abort_upload(tinkle_schema::proto::AbortUploadRequest {
                            upload_id: upload_id.clone(),
                        })
                        .await;
                    #[cfg(not(target_arch = "wasm32"))]
                    self.clear_session(idempotency_key).await;
                    return Err(TinkleError::Sync(
                        "upload digest mismatch against BeginUpload-declared sha256 (fatal); abort + new idempotency_key required".into(),
                    ));
                }
                // Complete-supplied mismatch is retryable: retry once with the
                // computed digest of the exact bytes uploaded.
                let r = self
                    .client
                    .complete_upload(CompleteUploadRequest {
                        upload_id: upload_id.clone(),
                        sha256: Some(computed.into()),
                    })
                    .await?;
                let file = r
                    .file
                    .ok_or_else(|| TinkleError::Codec("CompleteUpload must carry file".into()))?;
                #[cfg(not(target_arch = "wasm32"))]
                self.clear_session(idempotency_key).await;
                Ok(file)
            }
            Err(e) => Err(e),
        }
    }

    /// Snapshot-consistent `GetUploadStatus` enumeration: follows
    /// `next_page_token` until exhausted, unions all `received_ranges`,
    /// and enforces the fixed part size (server echo must match).
    async fn snapshot_received(
        &self,
        upload_id: &str,
        size_bytes: i64,
        part_size: i64,
    ) -> Result<HashSet<i64>, TinkleError> {
        let mut out = HashSet::new();
        let mut token: Option<String> = None;
        loop {
            let resp = self
                .client
                .get_upload_status(GetUploadStatusRequest {
                    upload_id: upload_id.to_string(),
                    page_token: token.clone(),
                    page_size: None,
                })
                .await?;
            if let Some(ps) = resp.part_size_bytes {
                if ps != part_size {
                    return Err(TinkleError::Sync(format!(
                        "upload part size changed mid-session: negotiated {part_size}, snapshot {ps}"
                    )));
                }
            }
            out.extend(received_set(&resp.received_ranges, size_bytes, part_size));
            match resp.next_page_token {
                Some(t) if !t.is_empty() => token = Some(t),
                _ => break,
            }
        }
        Ok(out)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn load_session_received(&self, idempotency_key: &str, upload_id: &str) -> HashSet<i64> {
        let storage = match self.storage() {
            Some(s) => s,
            None => return HashSet::new(),
        };
        let principal = self.client.principal().await;
        let job_id = upload_job_id(idempotency_key);
        let job = {
            let s = storage.clone();
            let p = principal.clone();
            match tokio::task::spawn_blocking(move || s.get_file_transfer(&p, &job_id)).await {
                Ok(Ok(Some(j))) => j,
                _ => return HashSet::new(),
            }
        };
        // Only resume the same remote session; a rotated upload_id means the
        // durable record belongs to a previous attempt.
        if job.upload_id != upload_id {
            return HashSet::new();
        }
        UploadSession::decode(&job.session_json)
            .map(|sess| sess.received)
            .unwrap_or_default()
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn persist_session(&self, args: PersistSessionArgs<'_>) {
        let storage = match self.storage() {
            Some(s) => s,
            None => return,
        };
        let principal = self.client.principal().await;
        if principal == crate::sync::UNKNOWN_PRINCIPAL {
            return;
        }
        let PersistSessionArgs {
            idempotency_key,
            params,
            upload_id,
            part_size,
            size_bytes,
            received,
            complete_sha,
        } = args;
        let sess = UploadSession {
            upload_id: upload_id.to_string(),
            part_size,
            size_bytes,
            received: received.clone(),
            begin_has_sha: params.sha256.is_some(),
            begin_sha: params.sha256.clone().unwrap_or_default(),
            complete_sha,
            file_name: params.file_name.clone(),
            mime_type: params.mime_type.clone(),
            encrypted: params.encrypted,
        };
        let job = tinkle_storage::FileTransferJob {
            job_id: upload_job_id(idempotency_key),
            kind: "upload".to_string(),
            file_scope: SHARED_FILE_SCOPE.to_string(),
            file_id: 0,
            upload_id: upload_id.to_string(),
            state: "active".to_string(),
            session_json: sess.encode(),
            temp_path: String::new(),
            resume_offset: received.len() as i64,
            updated_at: now_secs(),
        };
        let s2 = storage.clone();
        let _ =
            tokio::task::spawn_blocking(move || s2.upsert_file_transfer(&principal, &job)).await;
    }
}

// ---------------------------------------------------------------------------
// DownloadManager.
// ---------------------------------------------------------------------------

/// Effective per-part limit: advertised bound when present, conservative
/// fallback otherwise. Only non-positive limits are rejected in fallback.
pub fn effective_download_limit(resp: &GetFileInfoResponse) -> Result<i32, TinkleError> {
    match resp.max_download_part_bytes {
        Some(v) => {
            if v <= 0 {
                Err(TinkleError::Codec(
                    "max_download_part_bytes must be positive".into(),
                ))
            } else {
                Ok(v)
            }
        }
        None => Ok(FALLBACK_MAX_DOWNLOAD_PART_BYTES),
    }
}

/// Verify reassembled bytes against the authoritative `FileInfo`
/// (size + SHA-256). Corrupt bytes are a `Sync` error (never silently kept).
pub fn verify_downloaded_bytes(data: &[u8], file: &FileInfo) -> Result<(), TinkleError> {
    if data.len() as i64 != file.size_bytes {
        return Err(TinkleError::Sync(format!(
            "download size mismatch: got {}, want {}",
            data.len(),
            file.size_bytes
        )));
    }
    if file.sha256.len() != 32 {
        return Err(TinkleError::Codec(
            "FileInfo.sha256 must be 32 bytes".into(),
        ));
    }
    let got = sha256(data);
    if got.as_slice() != file.sha256.as_ref() {
        return Err(TinkleError::Sync(
            "download corrupt: sha256 mismatch".into(),
        ));
    }
    Ok(())
}

/// Validate one `DownloadPartResponse` against the request echo and file
/// bounds: echo must match, the range must not overflow, and `eof` must be
/// true exactly at end-of-file. Callers continue from `offset + len(data)`.
pub fn validate_download_part(
    resp: &DownloadPartResponse,
    want_offset: i64,
    file_size: i64,
) -> Result<(), TinkleError> {
    if resp.offset != want_offset {
        return Err(TinkleError::Sync(format!(
            "download echo mismatch: got offset {}, want {want_offset}",
            resp.offset
        )));
    }
    let len = resp.data.len() as i64;
    if want_offset < 0 || want_offset > file_size {
        return Err(TinkleError::Codec("download offset out of range".into()));
    }
    let end = want_offset
        .checked_add(len)
        .ok_or_else(|| TinkleError::Codec("download offset+len overflow".into()))?;
    if len < 0 || end > file_size {
        return Err(TinkleError::Sync("download overflow past file size".into()));
    }
    // Progress rule: a successful call with a positive limit and a nonempty
    // remainder must return at least one byte.
    if file_size > want_offset && len == 0 && !resp.eof {
        return Err(TinkleError::Sync(
            "download made no progress without eof".into(),
        ));
    }
    let at_end = end == file_size;
    if resp.eof != at_end {
        return Err(TinkleError::Sync(format!(
            "download eof mismatch at offset {want_offset}+{len}/{}: eof={}",
            file_size, resp.eof
        )));
    }
    Ok(())
}

// Join-in-flight: concurrent consumers of the same (principal, file_id)
// share one network download; per-consumer cancel only releases that
// consumer.
struct SharedDownload {
    notify: tokio::sync::Notify,
    result: Mutex<Option<Result<Vec<u8>, TinkleError>>>,
    /// Exactly one winner runs the shared fetch (atomic compare-exchange).
    claim: AtomicBool,
}

impl SharedDownload {
    /// Store the outcome (first wins), wake waiters, and remove the
    /// matching map entry. Fully synchronous: safe to call in `Drop`.
    /// Poisoned locks are recovered via `into_inner`, never panics.
    fn resolve(
        shared: &Arc<SharedDownload>,
        key: &InflightKey,
        outcome: Result<Vec<u8>, TinkleError>,
    ) {
        {
            let mut r = shared.result.lock().unwrap_or_else(|e| e.into_inner());
            if r.is_none() {
                *r = Some(outcome);
            }
        }
        shared.notify.notify_waiters();
        {
            let mut g = inflight().lock().unwrap_or_else(|e| e.into_inner());
            if let Some(cur) = g.get(key) {
                if Arc::ptr_eq(cur, shared) {
                    g.remove(key);
                }
            }
        }
    }
}

/// Guarantees a shared download always resolves: if the leader path
/// exits without normal completion (panic, task abort, or future drop),
/// the guard publishes an error result, wakes waiters, and removes the
/// in-flight entry — all synchronously, since `Drop` cannot await.
/// The normal path resolves explicitly, then disarms the guard.
struct LeaderGuard {
    shared: Arc<SharedDownload>,
    key: InflightKey,
    armed: bool,
}

impl LeaderGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        SharedDownload::resolve(
            &self.shared,
            &self.key,
            Err(TinkleError::Sync(
                "download leader dropped before completion".into(),
            )),
        );
    }
}

type InflightKey = (String, i64);
type InflightMap = HashMap<InflightKey, Arc<SharedDownload>>;

fn inflight() -> &'static Mutex<InflightMap> {
    static MAP: OnceLock<Mutex<InflightMap>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Rust-owned download manager. Holds a `Client` clone (cheap).
///
/// When `cache_dir` is configured (native only), verified bytes are served
/// from the SQLite `file_cache_entries` + on-disk cache and every verified
/// download atomically publishes its cache copy before returning.
#[derive(Clone)]
pub struct DownloadManager {
    client: crate::client::Client,
    #[cfg(not(target_arch = "wasm32"))]
    cache_dir: Option<std::path::PathBuf>,
}

impl DownloadManager {
    pub fn new(client: crate::client::Client) -> Self {
        Self {
            client,
            #[cfg(not(target_arch = "wasm32"))]
            cache_dir: None,
        }
    }

    /// Configure the on-disk file cache directory (native only). The
    /// directory is created on first cache publish; lookups work as soon as
    /// it (and the SQLite rows) exist.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_cache_dir(mut self, cache_dir: std::path::PathBuf) -> Self {
        self.cache_dir = Some(cache_dir);
        self
    }

    /// Update the cache directory after construction (native only).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn set_cache_dir(&mut self, cache_dir: Option<std::path::PathBuf>) {
        self.cache_dir = cache_dir;
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn storage(&self) -> Option<Arc<Storage>> {
        self.client.sqlite_storage()
    }

    /// Deterministic cache filename for one file (stable across restarts so
    /// lookups hit). Flat layout avoids deep trees; collisions are
    /// impossible because `file_id` is unique within the shared scope.
    #[cfg(not(target_arch = "wasm32"))]
    fn default_cache_relative(file_id: i64) -> String {
        format!("file-{file_id}.bin")
    }

    /// Verified cache lookup: complete entry + on-disk bytes that still
    /// verify against `file` (size + SHA-256). Any mismatch is a miss
    /// (never served, never fatal); hits refresh `last_access`.
    #[cfg(not(target_arch = "wasm32"))]
    async fn cached_bytes(&self, file_id: i64, file: &FileInfo) -> Option<Vec<u8>> {
        let storage = self.storage()?;
        let cache_dir = self.cache_dir.clone()?;
        if file.size_bytes < 0 {
            return None;
        }
        let principal = self.client.principal().await;
        if principal == crate::sync::UNKNOWN_PRINCIPAL {
            return None;
        }
        let entry = {
            let s = storage.clone();
            let p = principal.clone();
            let q = p.clone();
            tokio::task::spawn_blocking(move || {
                s.get_file_cache_entry(&q, SHARED_FILE_SCOPE, file_id)
            })
            .await
            .ok()?
            .ok()??
        };
        if entry.availability != "complete" || entry.actual_size != file.size_bytes {
            return None;
        }
        // Reuse the stored path when safe; otherwise fall back to the
        // deterministic default (older rows may predate it).
        let rel = if entry.relative_path.is_empty() {
            Self::default_cache_relative(file_id)
        } else {
            entry.relative_path.clone()
        };
        let path = FileManager::safe_cache_path(&cache_dir, &rel)?;
        let bytes = tokio::task::spawn_blocking(move || std::fs::read(&path))
            .await
            .ok()?
            .ok()?;
        if verify_downloaded_bytes(&bytes, file).is_err() {
            return None;
        }
        // Best-effort LRU touch.
        {
            let s = storage.clone();
            let mut touched = entry;
            touched.last_access = now_secs();
            let _ = tokio::task::spawn_blocking(move || {
                s.upsert_file_cache_entry(&principal, &touched)
            })
            .await;
        }
        Some(bytes)
    }

    /// Atomically publish verified `bytes` into the cache and upsert the
    /// complete `file_cache_entries` row. Reuses the existing safe
    /// `relative_path` when present, else the deterministic default.
    #[cfg(not(target_arch = "wasm32"))]
    async fn publish_cache_entry(&self, file_id: i64, file: &FileInfo, bytes: &[u8]) {
        let storage = match self.storage() {
            Some(s) => s,
            None => return,
        };
        let cache_dir = match self.cache_dir.clone() {
            Some(d) => d,
            None => return,
        };
        let principal = self.client.principal().await;
        if principal == crate::sync::UNKNOWN_PRINCIPAL {
            return;
        }
        let rel = {
            let s = storage.clone();
            let p = principal.clone();
            let existing: Option<tinkle_storage::FileCacheEntry> =
                match tokio::task::spawn_blocking(move || {
                    s.get_file_cache_entry(&p, SHARED_FILE_SCOPE, file_id)
                })
                .await
                {
                    Ok(Ok(e)) => e,
                    _ => None,
                };
            match existing {
                Some(e)
                    if !e.relative_path.is_empty()
                        && FileManager::safe_cache_path(&cache_dir, &e.relative_path).is_some() =>
                {
                    (e.relative_path, e.pinned)
                }
                Some(e) => (Self::default_cache_relative(file_id), e.pinned),
                None => (Self::default_cache_relative(file_id), false),
            }
        };
        let (relative_path, pinned) = rel;
        let path = match FileManager::safe_cache_path(&cache_dir, &relative_path) {
            Some(p) => p,
            None => return,
        };
        if let Some(parent) = path.parent() {
            let _ = tokio::task::spawn_blocking({
                let parent = parent.to_path_buf();
                move || std::fs::create_dir_all(&parent)
            })
            .await;
        }
        let bytes_owned = bytes.to_vec();
        let published = tokio::task::spawn_blocking(move || publish_atomic(&path, &bytes_owned))
            .await
            .map(|r| r.is_ok())
            .unwrap_or(false);
        if !published {
            return;
        }
        let entry = tinkle_storage::FileCacheEntry {
            scope: SHARED_FILE_SCOPE.to_string(),
            file_id,
            relative_path,
            actual_size: file.size_bytes,
            availability: "complete".to_string(),
            last_access: now_secs(),
            pinned,
        };
        let s2 = storage.clone();
        let _ = tokio::task::spawn_blocking(move || s2.upsert_file_cache_entry(&principal, &entry))
            .await;
    }

    /// Download one file to memory, verifying size + SHA-256.
    ///
    /// Flow: `GetFileInfo` → shared cache lookup / join-in-flight →
    /// `DownloadPart` ranges from `offset + len(data)` → verify → return.
    /// `cancel` is per-consumer: cancelling one waiter never cancels the
    /// shared download or other consumers (cancel isolation).
    pub async fn download_bytes(
        &self,
        file_id: i64,
        cancel: Option<crate::CancelToken>,
    ) -> Result<Vec<u8>, TinkleError> {
        if file_id <= 0 {
            return Err(TinkleError::Codec("file_id must be positive".into()));
        }
        // Fast cancel before any I/O.
        if let Some(t) = cancel.as_ref() {
            if t.is_cancelled() {
                return Err(TinkleError::Syntra(
                    syntra_core::error::SyntraError::Cancelled("download cancelled".into()),
                ));
            }
        }
        // Join-in-flight, keyed by (principal, file_id): concurrent
        // consumers of the same file share one network download. The map
        // lock is blocking (never held across await), so cleanup in `Drop`
        // stays synchronous.
        let key = (self.client.principal().await, file_id);
        let shared = {
            let mut g = inflight().lock().unwrap_or_else(|e| e.into_inner());
            if let Some(s) = g.get(&key) {
                s.clone()
            } else {
                let s = Arc::new(SharedDownload {
                    notify: tokio::sync::Notify::new(),
                    result: Mutex::new(None),
                    claim: AtomicBool::new(false),
                });
                g.insert(key.clone(), s.clone());
                s
            }
        };
        // Exactly one caller wins leadership via atomic compare-exchange.
        // The winner runs the shared fetch with NO consumer cancellation
        // token (one consumer's cancel must never kill the shared
        // download); every consumer (winner included) then waits below,
        // racing only its own token. A `LeaderGuard` covers abnormal exit
        // (panic, task abort, future drop): it publishes an error, wakes
        // waiters, and removes the entry. Wasm futures are not `Send`, so
        // the winner fetches inline there.
        if shared
            .claim
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let runner = self.clone();
                let done = shared.clone();
                let done_key = key.clone();
                tokio::spawn(async move {
                    let mut guard = LeaderGuard {
                        shared: done.clone(),
                        key: done_key.clone(),
                        armed: true,
                    };
                    let outcome = runner.fetch_bytes(file_id, None).await;
                    SharedDownload::resolve(&done, &done_key, outcome);
                    guard.disarm();
                });
            }
            #[cfg(target_arch = "wasm32")]
            {
                let mut guard = LeaderGuard {
                    shared: shared.clone(),
                    key: key.clone(),
                    armed: true,
                };
                let outcome = self.fetch_bytes(file_id, None).await;
                SharedDownload::resolve(&shared, &key, outcome);
                guard.disarm();
            }
        }
        // Wait for the shared outcome. The notification is registered and
        // enabled BEFORE checking the result, so a completion that lands
        // between the check and the wait cannot be missed. Each waiter
        // receives a clone of the original `TinkleError`, preserving its
        // variant and details for error classification.
        loop {
            let notified = shared.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(res) = shared
                .result
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
            {
                return match res {
                    Ok(b) => Ok(b.clone()),
                    Err(e) => Err(e.clone()),
                };
            }
            match cancel.as_ref() {
                Some(t) => {
                    tokio::select! {
                        _ = &mut notified => {},
                        _ = t.cancelled() => {
                            return Err(TinkleError::Syntra(
                                syntra_core::error::SyntraError::Cancelled(
                                    "download cancelled".into(),
                                ),
                            ));
                        }
                    }
                }
                None => notified.await,
            }
        }
    }

    async fn fetch_bytes(
        &self,
        file_id: i64,
        cancel: Option<crate::CancelToken>,
    ) -> Result<Vec<u8>, TinkleError> {
        let check_cancel = |c: &Option<crate::CancelToken>| -> Result<(), TinkleError> {
            if let Some(t) = c.as_ref() {
                if t.is_cancelled() {
                    return Err(TinkleError::Syntra(
                        syntra_core::error::SyntraError::Cancelled("download cancelled".into()),
                    ));
                }
            }
            Ok(())
        };
        check_cancel(&cancel)?;
        let info = self
            .client
            .get_file_info(tinkle_schema::proto::GetFileInfoRequest { file_id })
            .await?;
        let file = info
            .file
            .clone()
            .ok_or_else(|| TinkleError::Codec("GetFileInfo must carry file".to_string()))?;
        if file.file_id != file_id {
            return Err(TinkleError::Sync("GetFileInfo file_id mismatch".into()));
        }
        let limit = effective_download_limit(&info)?;
        if file.size_bytes < 0 {
            return Err(TinkleError::Codec(
                "FileInfo.size_bytes must be non-negative".into(),
            ));
        }
        // Verified SQLite file-cache lookup: serve complete, verified bytes
        // without touching the network (never serve unverified bytes).
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(hit) = self.cached_bytes(file_id, &file).await {
            return Ok(hit);
        }
        // Downloads always start at offset 0: no durable prefix bytes exist
        // to restore, so a nonzero resume offset would skip bytes required
        // by the final full-file verification. No resume offsets are
        // persisted (a stale offset would mislead recovery into skipping
        // bytes it cannot reproduce).
        let mut offset: i64 = 0;
        // Cap the initial reservation: a malicious or corrupt `size_bytes`
        // must not abort the process with a huge upfront allocation. The
        // buffer still grows to the verified size as parts arrive.
        const MAX_INITIAL_CAP: usize = 1_048_576;
        let initial_cap = (file.size_bytes.max(0) as usize).min(MAX_INITIAL_CAP);
        let mut out = Vec::with_capacity(initial_cap);
        loop {
            check_cancel(&cancel)?;
            let req = tinkle_schema::proto::DownloadPartRequest {
                file_id,
                offset,
                limit,
            };
            let resp = match self.client.download_part(req).await {
                Ok(r) => r,
                Err(e) if is_ambiguous(&e) => {
                    // Ambiguous part: re-request the SAME range (idempotent by
                    // offset); the server returns identical bytes.
                    check_cancel(&cancel)?;
                    self.client
                        .download_part(tinkle_schema::proto::DownloadPartRequest {
                            file_id,
                            offset,
                            limit,
                        })
                        .await?
                }
                Err(e) => return Err(e),
            };
            validate_download_part(&resp, offset, file.size_bytes)?;
            let len = resp.data.len() as i64;
            let eof = resp.eof;
            out.extend_from_slice(&resp.data);
            // Continue from offset + len(data) (never offset + limit).
            offset += len;
            if eof {
                break;
            }
            if offset >= file.size_bytes {
                break;
            }
        }
        verify_downloaded_bytes(&out, &file)?;
        // Atomically publish the verified bytes into the file cache before
        // returning so the next consumer hits the cache.
        #[cfg(not(target_arch = "wasm32"))]
        self.publish_cache_entry(file_id, &file, &out).await;
        Ok(out)
    }

    /// Native-only: download to `final_path` atomically (temp file in the
    /// same directory + rename, so readers never see a partial file).
    /// Verifies size + SHA-256 before publish. Returns the final path.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn download_to_path(
        &self,
        file_id: i64,
        final_path: &std::path::Path,
        cancel: Option<crate::CancelToken>,
    ) -> Result<std::path::PathBuf, TinkleError> {
        let bytes = self.download_bytes(file_id, cancel).await?;
        // `publish_atomic` is synchronous (write + fsync + rename): keep it
        // off the async executor like `publish_cache_entry` does.
        let path_owned = final_path.to_path_buf();
        tokio::task::spawn_blocking(move || publish_atomic(&path_owned, &bytes))
            .await
            .map_err(|e| TinkleError::Sync(format!("publish task failed: {e}")))?
            .map_err(|e| TinkleError::Sync(e.to_string()))?;
        Ok(final_path.to_path_buf())
    }

    /// Crash recovery (native): drop `file_transfers/download` jobs whose
    /// `temp_path` vanished, and best-effort remove orphan `*.part` files
    /// under `cache_dir` with no durable job. Returns cleaned count.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn recover_downloads(
        &self,
        principal: &str,
        cache_dir: &std::path::Path,
    ) -> Result<usize, TinkleError> {
        let storage = match self.storage() {
            Some(s) => s,
            None => return Ok(0),
        };
        let jobs = {
            let s = storage.clone();
            let p = principal.to_string();
            tokio::task::spawn_blocking(move || s.list_file_transfers(&p, Some("download")))
                .await
                .map_err(|e| TinkleError::CursorStore(format!("recover task: {e}")))?
                .map_err(|e| TinkleError::CursorStore(e.to_string()))?
        };
        let mut cleaned = 0usize;
        for j in &jobs {
            // Missing temp file with nonzero progress → reset to 0.
            if !j.temp_path.is_empty()
                && !std::path::Path::new(&j.temp_path).exists()
                && j.resume_offset > 0
            {
                let s = storage.clone();
                let p = principal.to_string();
                let mut fixed = j.clone();
                fixed.resume_offset = 0;
                fixed.updated_at = now_secs();
                let _ =
                    tokio::task::spawn_blocking(move || s.upsert_file_transfer(&p, &fixed)).await;
                cleaned += 1;
            }
            // Completed-unrecorded: job says complete but cache entry is not
            // complete → mark for re-download (reset to active/0).
            if j.state == "complete" {
                let entry = {
                    let s = storage.clone();
                    let p = principal.to_string();
                    let scope = j.file_scope.clone();
                    let fid = j.file_id;
                    tokio::task::spawn_blocking(move || s.get_file_cache_entry(&p, &scope, fid))
                        .await
                        .map_err(|e| TinkleError::CursorStore(format!("recover task: {e}")))?
                        .map_err(|e| TinkleError::CursorStore(e.to_string()))?
                };
                let done = matches!(entry.map(|e| e.availability), Some(a) if a == "complete");
                if !done {
                    let s = storage.clone();
                    let p = principal.to_string();
                    let mut fixed = j.clone();
                    fixed.state = "active".to_string();
                    fixed.resume_offset = 0;
                    fixed.updated_at = now_secs();
                    let _ = tokio::task::spawn_blocking(move || s.upsert_file_transfer(&p, &fixed))
                        .await;
                    cleaned += 1;
                }
            }
        }
        // Orphan temps: best-effort sweep, never fatal. Durable jobs never
        // claim the short-lived `publish_atomic` temps, so age-gate the
        // sweep: `.part` files younger than the threshold may be active
        // in-progress publishes and must not be deleted.
        const ORPHAN_PART_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(15 * 60);
        if let Ok(rd) = std::fs::read_dir(cache_dir) {
            for ent in rd.flatten() {
                let p = ent.path();
                if p.extension().and_then(|e| e.to_str()) != Some("part") {
                    continue;
                }
                let pstr = p.to_string_lossy().to_string();
                let still_claimed = jobs.iter().any(|jj| jj.temp_path == pstr);
                if still_claimed {
                    continue;
                }
                // Unknown age (metadata/IO failure) is treated as young:
                // never delete a file that might still be written.
                let is_old = ent
                    .metadata()
                    .and_then(|m| m.modified())
                    .map(|t| t.elapsed().unwrap_or_default() >= ORPHAN_PART_MAX_AGE)
                    .unwrap_or(false);
                if !is_old {
                    continue;
                }
                let _ = std::fs::remove_file(&p);
                cleaned += 1;
            }
        }
        Ok(cleaned)
    }
}

/// Atomic publish: write `bytes` to a unique temp file in the same
/// directory as `final_path`, `fsync`, then rename over the destination.
/// Same-filesystem rename guarantees readers never observe a partial file.
/// Each publish uses `create_new` + a per-process nonce so concurrent
/// publishers (same pid, same filename) never share one temp pathname.
#[cfg(not(target_arch = "wasm32"))]
pub fn publish_atomic(final_path: &std::path::Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    static PUBLISH_NONCE: AtomicU64 = AtomicU64::new(0);
    let parent = final_path.parent().unwrap_or(std::path::Path::new("."));
    // Same directory ⇒ same filesystem ⇒ atomic rename.
    let fname = final_path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "download".to_string());
    let pid = std::process::id();
    // Bound the create_new retry loop: nonce uniqueness makes collisions
    // near-impossible; retries only cover pid-reuse + nonce-wrap races.
    for _ in 0..16 {
        let nonce = PUBLISH_NONCE.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let tmp = parent.join(format!(".{fname}.{pid}.{nanos}.{nonce}.part"));
        let mut f = match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        };
        let result = (|| -> Result<(), std::io::Error> {
            f.write_all(bytes)?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(&tmp, final_path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        return result;
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "publish_atomic could not claim a unique temp file",
    ))
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn file_info(id: i64, bytes: &[u8]) -> FileInfo {
        FileInfo {
            file_id: id,
            size_bytes: bytes.len() as i64,
            sha256: sha256(bytes).to_vec().into(),
            mime_type: None,
            file_name: None,
            encrypted: false,
            created_at: None,
        }
    }

    #[test]
    fn sha256_matches_empty_vector() {
        // SHA-256("") = e3b0c442...
        assert_eq!(
            hex_encode(&sha256(&[])),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn begin_decision_keys_on_status_only() {
        // ACTIVE with config → Active (config present required).
        let active = BeginUploadResponse {
            upload_id: "u1".into(),
            part_size_bytes: Some(1024),
            max_parallel_parts: Some(4),
            expires_at: None,
            status: UploadStatus::Active as i32,
            file: None,
        };
        assert!(matches!(
            decide_begin_status(&active).unwrap(),
            BeginDecision::Active { .. }
        ));
        // ACTIVE without config → Codec (never infer from absent config).
        let bad = BeginUploadResponse {
            part_size_bytes: None,
            ..active.clone()
        };
        assert!(decide_begin_status(&bad).is_err());
        // COMPLETED carries file, no upload.
        let done_file = file_info(9, b"hi");
        let done = BeginUploadResponse {
            upload_id: "u1".into(),
            part_size_bytes: None,
            max_parallel_parts: None,
            expires_at: None,
            status: UploadStatus::Completed as i32,
            file: Some(done_file.clone()),
        };
        let got = decide_begin_status(&done).unwrap();
        match got {
            BeginDecision::Completed(f) => assert_eq!(f.file_id, 9),
            _ => panic!("want Completed"),
        }
        // COMPLETED without file → Codec.
        let done_nofile = BeginUploadResponse {
            file: None,
            ..done.clone()
        };
        assert!(decide_begin_status(&done_nofile).is_err());
        // ABORTED / EXPIRED → fresh key.
        for st in [UploadStatus::Aborted, UploadStatus::Expired] {
            let r = BeginUploadResponse {
                status: st as i32,
                upload_id: "u1".into(),
                part_size_bytes: None,
                max_parallel_parts: None,
                expires_at: None,
                file: None,
            };
            assert!(matches!(
                decide_begin_status(&r).unwrap(),
                BeginDecision::NeedFreshKey
            ));
        }
    }

    #[test]
    fn part_splitting_is_fixed_and_exact() {
        assert_eq!(part_count(0, 5).unwrap(), 0);
        assert_eq!(part_count(12, 5).unwrap(), 3);
        assert_eq!(part_byte_range(0, 5, 12).unwrap(), (0, 5));
        assert_eq!(part_byte_range(2, 5, 12).unwrap(), (10, 2));
        assert!(part_byte_range(3, 5, 12).is_err());
    }

    #[test]
    fn received_ranges_coalesce_snapshot_consistent() {
        use tinkle_schema::proto::PartIndexRange;
        let ranges = vec![
            PartIndexRange {
                start_index: 0,
                end_index: 2,
            },
            PartIndexRange {
                start_index: 5,
                end_index: 6,
            },
        ];
        let set = received_set(&ranges, 100, 5);
        assert!(set.contains(&0) && set.contains(&1) && set.contains(&5));
        assert!(!set.contains(&2));
        // Out-of-range clamped, never trusted blindly.
        let wild = vec![PartIndexRange {
            start_index: 0,
            end_index: 9999,
        }];
        let set2 = received_set(&wild, 12, 5);
        assert_eq!(set2.len(), 3);
    }

    #[test]
    fn download_verify_rejects_corrupt_and_wrong_size() {
        let good = b"hello world!".to_vec();
        let info = file_info(7, &good);
        assert!(verify_downloaded_bytes(&good, &info).is_ok());
        let mut bad = good.clone();
        bad[0] ^= 1;
        assert!(verify_downloaded_bytes(&bad, &info).is_err());
        assert!(verify_downloaded_bytes(b"short", &info).is_err());
    }

    #[test]
    fn download_part_validation_continues_from_offset_plus_len() {
        let size = 12i64;
        // First 5 bytes, not eof.
        let r1 = DownloadPartResponse {
            data: b"hello".to_vec().into(),
            offset: 0,
            eof: false,
        };
        assert!(validate_download_part(&r1, 0, size).is_ok());
        // Continue from offset+len(data)=5, not offset+limit.
        let r2 = DownloadPartResponse {
            data: b" worl".to_vec().into(),
            offset: 5,
            eof: false,
        };
        assert!(validate_download_part(&r2, 5, size).is_ok());
        // Final 2 bytes with eof.
        let r3 = DownloadPartResponse {
            data: b"d!".to_vec().into(),
            offset: 10,
            eof: true,
        };
        assert!(validate_download_part(&r3, 10, size).is_ok());
        // Echo mismatch rejected.
        let echo = DownloadPartResponse {
            data: b"x".to_vec().into(),
            offset: 99,
            eof: false,
        };
        assert!(validate_download_part(&echo, 5, size).is_err());
        // Overflow rejected.
        let over = DownloadPartResponse {
            data: vec![0u8; 10].into(),
            offset: 10,
            eof: true,
        };
        assert!(validate_download_part(&over, 10, size).is_err());
        // Eof mismatch rejected (at end but eof=false).
        let noeof = DownloadPartResponse {
            data: b"d!".to_vec().into(),
            offset: 10,
            eof: false,
        };
        assert!(validate_download_part(&noeof, 10, size).is_err());
    }

    #[test]
    fn effective_limit_falls_back_conservatively() {
        let absent = GetFileInfoResponse {
            file: None,
            max_download_part_bytes: None,
        };
        assert_eq!(
            effective_download_limit(&absent).unwrap(),
            FALLBACK_MAX_DOWNLOAD_PART_BYTES
        );
        let bad = GetFileInfoResponse {
            file: None,
            max_download_part_bytes: Some(0),
        };
        assert!(effective_download_limit(&bad).is_err());
        let ok = GetFileInfoResponse {
            file: None,
            max_download_part_bytes: Some(1024),
        };
        assert_eq!(effective_download_limit(&ok).unwrap(), 1024);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn shared_refs_reuse_one_record_across_messages_and_entities() {
        use tinkle_schema::proto::{media, Media, Photo, User as PUser};
        // Two messages sharing file_id 50 in the same conversation.
        let msg = |id: i64| Message {
            message_id: id,
            target: None,
            from_id: 1,
            date: None,
            message: format!("m{id}"),
            media: Some(Media {
                media: Some(media::Media::Photo(Photo {
                    id: 1,
                    access_hash: 0,
                    width: 1,
                    height: 1,
                    file_id: 50,
                    metadata: None,
                    variants: vec![],
                })),
            }),
            entities: vec![],
            reply_to: None,
            edit_date: None,
            reactions: None,
            replies: None,
            pinned: false,
            ttl_period: 0,
            accept_seq: None,
        };
        let m10 = msg(10);
        let m11 = msg(11);
        let ops10 = project_message_files_to_ops("target:room:7", 10, &m10);
        let ops11 = project_message_files_to_ops("target:room:7", 11, &m11);
        // Same (scope,file_id) identity for both.
        assert_eq!(ops10.files[0].scope, "");
        assert_eq!(ops10.files[0].file_id, 50);
        assert_eq!(ops11.files[0].file_id, 50);
        assert_eq!(ops10.files[0].scope, ops11.files[0].scope);
        // Entity (user avatar) with the SAME file_id shares the record.
        let user = PUser {
            user_type: Some(tinkle_schema::proto::user::UserType::Data(
                tinkle_schema::proto::user::UserData {
                    id: 42,
                    phone_number: None,
                    username: None,
                    photo: Some(tinkle_schema::proto::UserPhoto {
                        file_id: 50,
                        variants: vec![],
                    }),
                    name: None,
                    about: None,
                    verified: None,
                    premium: None,
                    fake: None,
                    scam: None,
                    bot: None,
                    deleted: None,
                },
            )),
        };
        let eops = project_entity_files_to_ops(std::slice::from_ref(&user), &[]);
        assert_eq!(eops.entity_files[0].file_id, 50);
        assert_eq!(eops.entity_files[0].file_scope, "");
        // Storage first-wins: replay with different metadata keeps first.
        #[cfg(not(target_arch = "wasm32"))]
        {
            let s = tinkle_storage::Storage::open_in_memory().unwrap();
            let mut ops = tinkle_storage::CacheOps::default();
            ops.messages.push(tinkle_storage::MessageRow {
                conversation_key: "target:room:7".into(),
                message_id: 10,
                peer_kind: "room".into(),
                peer_id: 7,
                from_id: 1,
                body: "m10".into(),
                proto: vec![1],
                origin_stream: "target:room:7".into(),
                accept_seq: Some(1),
                edit_date_secs: None,
                edit_date_nanos: None,
            });
            ops.files.extend(ops10.files.clone());
            ops.message_files.extend(ops10.message_files.clone());
            ops.files.extend(ops11.files.clone());
            ops.message_files.extend(ops11.message_files.clone());
            let mut eops2 = eops.clone();
            for f in &mut eops2.files {
                f.origin_stream = "target:room:7".into();
            }
            ops.files.extend(eops2.files.clone());
            ops.entity_files.extend(eops2.entity_files.clone());
            s.apply_journal_page_fenced("user:1", "target:room:7", &ops, 1, 0)
                .unwrap();
            // 2 message refs + 1 entity ref = 3.
            assert_eq!(s.file_refcount("user:1", "", 50).unwrap(), 3);
        }
    }

    #[test]
    fn upload_session_round_trips_durably() {
        let mut received = HashSet::new();
        received.insert(0);
        received.insert(2);
        let sess = UploadSession {
            upload_id: "upl-1".into(),
            part_size: 5,
            size_bytes: 12,
            received: received.clone(),
            begin_has_sha: true,
            begin_sha: vec![9u8; 32],
            complete_sha: None,
            file_name: Some("a|b".into()),
            mime_type: None,
            encrypted: false,
        };
        let enc = sess.encode();
        let dec = UploadSession::decode(&enc).unwrap();
        assert_eq!(dec.upload_id, "upl-1");
        assert_eq!(dec.received, received);
        assert!(dec.begin_has_sha);
    }

    #[test]
    fn file_manager_honors_pin_and_active_reader() {
        let mgr = FileManager::new();
        assert!(mgr.should_evict("", 1, false));
        assert!(!mgr.should_evict("", 1, true));
        mgr.begin_read("", 2);
        assert!(!mgr.should_evict("", 2, false));
        mgr.end_read("", 2);
        assert!(mgr.should_evict("", 2, false));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn atomic_publish_writes_same_filesystem_atomically() {
        let dir = std::env::temp_dir().join(format!("tinkle-pub-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let final_path = dir.join("file.bin");
        let _ = std::fs::remove_file(&final_path);
        publish_atomic(&final_path, b"abc123").unwrap();
        assert_eq!(std::fs::read(&final_path).unwrap(), b"abc123");
        // Overwrite is atomic too (no partial readers).
        publish_atomic(&final_path, b"zz").unwrap();
        assert_eq!(std::fs::read(&final_path).unwrap(), b"zz");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
