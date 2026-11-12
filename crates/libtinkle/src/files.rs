//! Rust-owned file transfer managers.
//!
//! - [`FileManager`]: canonical `(scope, file_id)` identity, shared ref
//!   registration (same `file_id` across messages/entities shares one
//!   `files` row), pin / active-reader honoring eviction, refcounts via
//!   `Storage::file_refcount`.
//! - [`UploadManager`]: immutable private source snapshot, exact Begin protobuf
//!   and streaming source proof, bounded parallel parts, authoritative server
//!   interval snapshots and completion-status recovery. Native jobs persist
//!   before transmission; storage failures are returned, never ignored.
//! - [`DownloadManager`]: `GetFileInfo` → shared cache lookup /
//!   join-in-flight → `DownloadPart` ranges → size + SHA-256 verify →
//!   native fsynced disk prefixes → streaming verification → atomic publish.
//!   Byte APIs are bounded conveniences, never the native path implementation.
//!
//! Native cache directories must be host-owned/private and separately namespaced
//! across deployments. A cancelled waiter does not prove a remote mutation undone.
//!
//! File refs are projected alongside messages/entities by
//! `cache_projector` (see `project_message_files_to_ops` /
//! `project_entity_files_to_ops`); ingest paths (`ingest_messages`,
//! journal hydration) therefore carry file refs in the same fenced
//! transaction as bodies.

use std::collections::{HashMap, HashSet};
use std::sync::{atomic::Ordering, Arc, Mutex, OnceLock};

use tinkle_schema::proto::{
    BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest, DownloadPartResponse, FileInfo,
    GetFileInfoResponse, GetUploadStatusRequest, Message, StickerView, UploadStatus, User,
};

use crate::errors::TinkleError;
use sha2::{Digest, Sha256};
#[cfg(target_arch = "wasm32")]
use std::sync::atomic::AtomicBool;
/// Exact application reference for non-owner reads, never an inferred grant.
pub use tinkle_schema::proto::FileAccessContext;

#[cfg(not(target_arch = "wasm32"))]
use tinkle_storage::Storage;

// ---------------------------------------------------------------------------
// SHA-256: RustCrypto in production; legacy implementation retained as a test oracle.
// ---------------------------------------------------------------------------

#[cfg(test)]
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
    Sha256::digest(data).into()
}

#[cfg(test)]
fn legacy_sha256(data: &[u8]) -> [u8; 32] {
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

#[cfg(test)]
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

/// Maximum owned payload of the byte convenience APIs (8 MiB).
pub const MAX_CONVENIENCE_BYTES: usize = 8 * 1024 * 1024;
/// Hard local part allocation ceiling. Negotiation cannot enlarge this budget.
pub const MAX_TRANSFER_PART_BYTES: usize = 1024 * 1024;
/// Shared process admission for payload-bearing parts (upload and download).
pub const MAX_TRANSFER_PARTS: usize = 8;
#[cfg(not(target_arch = "wasm32"))]
static TRANSFER_PARTS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(MAX_TRANSFER_PARTS);
#[cfg(not(target_arch = "wasm32"))]
static TRANSFER_JOBS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);
#[cfg(not(target_arch = "wasm32"))]
static DISK_WORKERS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(16);

/// Sanitized local transfer errors never include caller paths or payloads.
fn transfer_error(message: &str) -> TinkleError {
    TinkleError::Sync(message.into())
}
#[cfg(not(target_arch = "wasm32"))]
fn io_error(error: std::io::Error) -> TinkleError {
    transfer_error(&format!("transfer disk I/O: {:?}", error.kind()))
}
fn check_cancel(token: Option<&crate::CancelToken>) -> Result<(), TinkleError> {
    if token.is_some_and(|t| t.is_cancelled()) {
        return Err(TinkleError::Syntra(
            syntra_core::error::SyntraError::Cancelled("transfer cancelled".into()),
        ));
    }
    Ok(())
}

/// Blocking work keeps its own admission even when its async waiter is dropped.
#[cfg(not(target_arch = "wasm32"))]
async fn disk<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, TinkleError> + Send + 'static,
) -> Result<T, TinkleError> {
    let permit = DISK_WORKERS
        .try_acquire()
        .map_err(|_| transfer_error("disk workers busy"))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|_| transfer_error("transfer disk worker failed"))?
}

/// Cancellation only affects the local pipeline, never asserts protocol rollback.
#[cfg(not(target_arch = "wasm32"))]
struct CancelOnDrop(crate::CancelToken);
#[cfg(not(target_arch = "wasm32"))]
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Private immutable snapshot; source mutations cannot change already scheduled parts.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
struct FrozenSource {
    file: Arc<tempfile::NamedTempFile>,
    size: u64,
    digest: [u8; 32],
}
#[cfg(not(target_arch = "wasm32"))]
impl FrozenSource {
    fn capture(
        path: &std::path::Path,
        scratch_root: &std::path::Path,
        token: &crate::CancelToken,
    ) -> Result<Self, TinkleError> {
        use std::io::{Read, Seek, Write};
        let mut input = std::fs::File::open(path).map_err(io_error)?;
        let before = input.metadata().map_err(io_error)?;
        if !before.is_file() {
            return Err(transfer_error("source must be a regular file"));
        }
        let mut snapshot = tempfile::NamedTempFile::new_in(scratch_root).map_err(io_error)?;
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 65536];
        let mut size = 0u64;
        loop {
            check_cancel(Some(token))?;
            let n = input.read(&mut buffer).map_err(io_error)?;
            if n == 0 {
                break;
            }
            size = size
                .checked_add(n as u64)
                .ok_or_else(|| transfer_error("source too large"))?;
            if size > before.len() {
                return Err(transfer_error("source changed during preparation"));
            }
            snapshot.write_all(&buffer[..n]).map_err(io_error)?;
            hash.update(&buffer[..n]);
        }
        let digest: [u8; 32] = hash.finalize().into();
        input.rewind().map_err(io_error)?;
        let (checked_size, checked_digest) =
            hash_reader(&mut input.take(before.len().saturating_add(1)), Some(token))?;
        let after = std::fs::metadata(path).map_err(io_error)?;
        if size != before.len()
            || checked_size != size
            || checked_digest != digest
            || before.modified().ok() != after.modified().ok()
        {
            return Err(transfer_error("source changed during preparation"));
        }
        snapshot.as_file().sync_all().map_err(io_error)?;
        Ok(Self {
            file: Arc::new(snapshot),
            size,
            digest,
        })
    }
    fn read_part(&self, offset: u64, len: usize) -> Result<Vec<u8>, TinkleError> {
        use std::io::{Read, Seek, SeekFrom};
        if len > MAX_TRANSFER_PART_BYTES {
            return Err(transfer_error("part exceeds memory budget"));
        }
        let mut file = self.file.reopen().map_err(io_error)?;
        file.seek(SeekFrom::Start(offset)).map_err(io_error)?;
        let mut bytes = vec![0; len];
        file.read_exact(&mut bytes).map_err(io_error)?;
        Ok(bytes)
    }
}

/// Streaming digest uses a fixed 64 KiB working buffer, independent of file size.
#[cfg(not(target_arch = "wasm32"))]
fn hash_reader(
    reader: &mut impl std::io::Read,
    token: Option<&crate::CancelToken>,
) -> Result<(u64, [u8; 32]), TinkleError> {
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    let mut size = 0u64;
    loop {
        check_cancel(token)?;
        let n = reader.read(&mut buffer).map_err(io_error)?;
        if n == 0 {
            break;
        }
        size = size
            .checked_add(n as u64)
            .ok_or_else(|| transfer_error("file too large"))?;
        hash.update(&buffer[..n]);
    }
    Ok((size, hash.finalize().into()))
}
#[cfg(not(target_arch = "wasm32"))]
fn verify_file_proof(file: &FileInfo, size: i64, digest: &[u8; 32]) -> Result<(), TinkleError> {
    if file.file_id <= 0 || file.size_bytes != size || file.sha256.as_ref() != digest {
        return Err(transfer_error("file identity/size/digest mismatch"));
    }
    Ok(())
}

/// Captured principal, source revision and socket. No stage may drift to a new
/// connection/account. Durable workers own lifecycle exclusion, while immediate
/// credential revocation never queues behind their synchronous disk work.
#[cfg(not(target_arch = "wasm32"))]
struct TransferScope {
    client: crate::Client,
    principal: String,
    namespace: String,
    epoch: u64,
    revision: u64,
    authority: crate::authorization::EnrollmentEpoch,
    generation: Arc<crate::liveness::LiveGeneration>,
}
#[cfg(not(target_arch = "wasm32"))]
impl TransferScope {
    async fn capture(client: &crate::Client) -> Result<Arc<Self>, TinkleError> {
        // Validate inherited sending scope before reading a principal or touching
        // source/job state. Current values must never renew a stale operation.
        let (epoch, revision) = client.transfer_application_fence()?;
        let authority = client.authorization.generation()?;
        let principal = client.principal().await;
        let namespace = {
            let source = client.trust_source.read().await;
            let trust = match &*source {
                crate::TrustSource::Bootstrap(t) | crate::TrustSource::SignedOut(t) => t,
                crate::TrustSource::Returning { trust, .. } => trust,
            };
            let mut hash = Sha256::new();
            hash.update(trust.server_public_key);
            hash.update(trust.server_key_id.to_be_bytes());
            hash.update([trust.scope.len() as u8]);
            hash.update(trust.scope.as_bytes());
            hash.update(principal.as_bytes());
            hex_encode(&hash.finalize())
        };
        let generation = client
            .session_state
            .generation()
            .ok_or_else(|| transfer_error("transfer requires a live connection"))?;
        let scope = Arc::new(Self {
            client: client.for_application_scope(epoch, revision),
            principal,
            namespace,
            epoch,
            revision,
            authority,
            generation,
        });
        scope.check()?;
        Ok(scope)
    }
    fn check(&self) -> Result<(), TinkleError> {
        self.authority.valid()?;
        self.check_lifecycle()
    }
    fn check_lifecycle(&self) -> Result<(), TinkleError> {
        // Commit calls this under both lifecycle gates; the inherited fence is
        // revalidated, not replaced, including for retained blocking workers.
        let (epoch, revision) = self.client.transfer_application_fence()?;
        if self.epoch != epoch
            || self.revision != revision
            || !self.generation.open.load(Ordering::SeqCst)
            || self.generation.retiring.load(Ordering::SeqCst)
            || self.client.owner_gone().is_cancelled()
        {
            return Err(transfer_error(
                "transfer lifecycle changed; resume explicitly",
            ));
        }
        Ok(())
    }
    fn caller(&self) -> crate::Client {
        // Preserve any inherited application fence; never replace an old
        // sending-scoped clone's fence with newly observed current values.
        self.client.for_session_generation(self.generation.clone())
    }
    /// Blocking-worker-only commit gate: account/source transitions take this
    /// same connection gate. The worker (not its cancellable async waiter) owns
    /// exclusion until synchronous publication/SQLite completes. No DB lock is
    /// held while waiting for admission. Shutdown must join workers outside the
    /// client's connection gate, never while holding that gate itself.
    fn commit<T>(&self, work: impl FnOnce() -> Result<T, TinkleError>) -> Result<T, TinkleError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| transfer_error("transfer runtime unavailable"))?;
        runtime.block_on(async {
            let _gate = self.client.connect_lock.lock().await;
            let _commit = self.client.checkpoint_save.lock().await;
            self.check()?;
            // Logout fences credentials immediately without waiting on disk I/O.
            // Its cache wipe takes this connection gate, so admitted writes drain
            // before the wipe and cannot resurrect rows afterwards. Never hold
            // the synchronous authorization mutex across SQLite/fsync.
            let result = work();
            self.check()?;
            result
        })
    }
    async fn load_job(
        &self,
        id: &str,
    ) -> Result<Option<tinkle_storage::FileTransferJob>, TinkleError> {
        self.check()?;
        let Some(storage) = self.client.sqlite_storage() else {
            return Ok(None);
        };
        if self.principal == crate::sync::UNKNOWN_PRINCIPAL {
            return Err(transfer_error("durable transfer requires a principal"));
        }
        let principal = self.principal.clone();
        let id = id.to_string();
        disk(move || {
            storage
                .get_file_transfer(&principal, &id)
                .map_err(|_| transfer_error("transfer store read failed"))
        })
        .await
    }
    async fn save_job(
        self: &Arc<Self>,
        job: tinkle_storage::FileTransferJob,
    ) -> Result<(), TinkleError> {
        self.check()?;
        let Some(storage) = self.client.sqlite_storage() else {
            return Ok(());
        };
        if self.principal == crate::sync::UNKNOWN_PRINCIPAL {
            return Err(transfer_error("durable transfer requires a principal"));
        }
        let scope = self.clone();
        disk(move || {
            scope.commit(|| {
                storage
                    .upsert_file_transfer(&scope.principal, &job)
                    .map_err(|_| transfer_error("transfer store write failed"))
            })
        })
        .await?;
        self.check()
    }

    /// Freeze an upload's canonical intent atomically before any remote request.
    async fn claim_upload_job(
        self: &Arc<Self>,
        job: tinkle_storage::FileTransferJob,
    ) -> Result<tinkle_storage::transfer_intent::TransferIntentCheckpoint, TinkleError> {
        self.check()?;
        let Some(storage) = self.client.sqlite_storage() else {
            return Err(transfer_error(
                "managed upload requires SQLite intent storage",
            ));
        };
        if self.principal == crate::sync::UNKNOWN_PRINCIPAL {
            return Err(transfer_error("durable transfer requires a principal"));
        }
        let scope = self.clone();
        let checkpoint = disk(move || {
            scope.commit(|| {
                storage
                    .create_or_compare_file_transfer_intent(
                        &scope.principal,
                        &job,
                        &job.session_json,
                    )
                    .map_err(|_| transfer_error("upload intent conflict or store unavailable"))
            })
        })
        .await?;
        self.check()?;
        Ok(checkpoint)
    }

    /// Preserve the original proof/incarnation through concurrent same-intent
    /// progress. Never recreate deleted work or regress a terminal checkpoint.
    async fn save_upload_job(
        self: &Arc<Self>,
        job: tinkle_storage::FileTransferJob,
        intent: &mut tinkle_storage::transfer_intent::TransferIntentCheckpoint,
    ) -> Result<(), TinkleError> {
        self.check()?;
        let storage = self
            .client
            .sqlite_storage()
            .ok_or_else(|| transfer_error("managed upload requires SQLite intent storage"))?;
        let original = intent.clone();
        let scope = self.clone();
        let checkpoint = disk(move || {
            scope.commit(|| {
                storage
                    .advance_upload_intent(&scope.principal, &original, &job)
                    .map_err(|_| transfer_error("upload checkpoint changed or store unavailable"))
            })
        })
        .await?;
        self.check()?;
        *intent = checkpoint;
        Ok(())
    }
}

/// Compact server-authoritative interval snapshot; no allocation per acknowledged part.
#[cfg(not(target_arch = "wasm32"))]
struct UploadSnapshot {
    ranges: Vec<(i64, i64)>,
    completed: Option<FileInfo>,
}
#[cfg(not(target_arch = "wasm32"))]
impl UploadSnapshot {
    fn contains(&self, i: i64) -> bool {
        self.ranges.iter().any(|&(a, b)| a <= i && i < b)
    }
}
#[cfg(not(target_arch = "wasm32"))]
async fn upload_snapshot(
    client: &crate::Client,
    id: &str,
    size: i64,
    part_size: i64,
) -> Result<UploadSnapshot, TinkleError> {
    let mut result = UploadSnapshot {
        ranges: Vec::new(),
        completed: None,
    };
    let count = part_count(size, part_size)?;
    let mut token = None;
    let mut seen = HashSet::new();
    for _ in 0..1024 {
        let response = client
            .get_upload_status(GetUploadStatusRequest {
                upload_id: id.into(),
                page_token: token,
                page_size: Some(256),
            })
            .await?;
        if response.size_bytes != size {
            return Err(transfer_error("upload snapshot size changed"));
        }
        match UploadStatus::try_from(response.status) {
            Ok(UploadStatus::Completed) if seen.is_empty() => {
                result.completed = Some(
                    response
                        .file
                        .ok_or_else(|| transfer_error("COMPLETED lacks file"))?,
                );
                return Ok(result);
            }
            Ok(UploadStatus::Active)
                if response.part_size_bytes == Some(part_size) && response.file.is_none() => {}
            _ => {
                return Err(transfer_error(
                    "upload snapshot terminal/invalid; resume explicitly",
                ))
            }
        }
        for r in response.received_ranges {
            if r.start_index < 0
                || r.end_index <= r.start_index
                || r.end_index > count
                || result
                    .ranges
                    .last()
                    .is_some_and(|&(_, b)| r.start_index < b)
            {
                return Err(transfer_error("invalid upload snapshot ranges"));
            }
            if result.ranges.len() >= 65536 {
                return Err(transfer_error("upload snapshot range budget exceeded"));
            }
            result.ranges.push((r.start_index, r.end_index));
        }
        match response.next_page_token {
            Some(t) if !t.is_empty() => {
                if t.len() > 4096 || !seen.insert(t.clone()) {
                    return Err(transfer_error("upload snapshot token cycle/oversize"));
                }
                token = Some(t);
            }
            _ => return Ok(result),
        }
    }
    Err(transfer_error("upload snapshot page budget exceeded"))
}

/// Upload inputs (bytes in). `sha256` is the optional client declaration
/// fixed for the session lifetime; `None` vs `Some` presence is preserved
/// exactly across idempotency retries.
#[derive(Debug, Clone)]
pub struct UploadParams {
    pub idempotency_key: String,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub sha256: Option<Vec<u8>>,
}

impl UploadParams {
    pub fn new(idempotency_key: String, data_len: usize) -> Self {
        let _ = data_len;
        Self {
            idempotency_key,
            file_name: None,
            mime_type: None,
            sha256: None,
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
#[cfg(any(test, target_arch = "wasm32"))]
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
#[cfg(test)]
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
#[cfg(test)]
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
#[cfg(target_arch = "wasm32")]
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
        _ => false,
    }
}

/// Rust-owned managed upload coordinator. Requires a SQLite-backed Client so
/// canonical intent can be claimed before every first remote request. Hosts
/// managing their own intent persistence may use the raw FilesService methods.
#[derive(Clone)]
pub struct UploadManager {
    client: crate::client::Client,
    #[cfg(all(test, not(target_arch = "wasm32")))]
    intent_barrier: Option<Arc<tokio::sync::Barrier>>,
}

impl UploadManager {
    /// Freeze the source into a private disk snapshot before BeginUpload. All parts
    /// and retries read that snapshot, never a mutable caller pathname. Reusing a
    /// durable job requires the identical source digest and exact Begin protobuf.
    /// Cancellation stops local work; it does not attest remote rollback/abort.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn upload_path(
        &self,
        params: UploadParams,
        path: &std::path::Path,
        cancel: Option<crate::CancelToken>,
    ) -> Result<FileInfo, TinkleError> {
        let token = cancel.unwrap_or_default();
        let gone = self.client.owner_gone();
        tokio::select! { biased;
            _ = token.cancelled() => Err(TinkleError::Syntra(syntra_core::error::SyntraError::Cancelled("upload cancelled".into()))),
            _ = gone.cancelled() => Err(transfer_error("client released")),
            result = self.upload_path_run(params, path, Some(token.clone())) => result,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn upload_path_run(
        &self,
        params: UploadParams,
        path: &std::path::Path,
        cancel: Option<crate::CancelToken>,
    ) -> Result<FileInfo, TinkleError> {
        if params.idempotency_key.is_empty()
            || params.idempotency_key.len() > 256
            || params.file_name.as_ref().is_some_and(|v| v.len() > 1024)
            || params.mime_type.as_ref().is_some_and(|v| v.len() > 256)
        {
            return Err(transfer_error("invalid upload parameter bounds"));
        }
        let _admission = TRANSFER_JOBS
            .try_acquire()
            .map_err(|_| transfer_error("transfer admission full"))?;
        check_cancel(cancel.as_ref())?;
        let scope = TransferScope::capture(&self.client).await?;
        let storage = scope
            .client
            .sqlite_storage()
            .ok_or_else(|| transfer_error("managed upload requires SQLite intent storage"))?;
        let stopped = cancel.unwrap_or_default();
        let _cancel_on_drop = CancelOnDrop(crate::CancelToken::new());
        let token = _cancel_on_drop.0.clone();
        let input = path.to_path_buf();
        let source = disk(move || {
            let root = storage
                .temporary_directory()
                .map_err(|_| transfer_error("scratch directory unavailable"))?;
            FrozenSource::capture(&input, &root, &token)
        })
        .await?;
        scope.check()?;
        let size = i64::try_from(source.size).map_err(|_| transfer_error("source too large"))?;
        let begin = BeginUploadRequest {
            idempotency_key: params.idempotency_key.clone(),
            size_bytes: size,
            file_name: params.file_name,
            mime_type: params.mime_type,
            sha256: params.sha256.map(Into::into),
        };
        if begin.idempotency_key.is_empty() || begin.idempotency_key.len() > 256 {
            return Err(transfer_error("invalid idempotency key"));
        }
        if begin
            .sha256
            .as_ref()
            .is_some_and(|v| v.as_ref() != source.digest)
        {
            return Err(transfer_error("source digest differs from declared digest"));
        }
        use prost::Message as _;
        let proof = serde_json::json!({"version": 2, "begin": hex_encode(&begin.encode_to_vec()),
            "source_sha256": hex_encode(&source.digest), "profile": "exact-bytes-v1"})
        .to_string();
        let job_id = upload_job_id(&begin.idempotency_key);
        #[cfg(test)]
        if let Some(barrier) = &self.intent_barrier {
            barrier.wait().await;
        }
        let mut job = tinkle_storage::FileTransferJob {
            job_id,
            kind: "upload".into(),
            file_scope: SHARED_FILE_SCOPE.into(),
            file_id: 0,
            upload_id: String::new(),
            state: "begin".into(),
            session_json: proof,
            temp_path: String::new(),
            resume_offset: 0,
            updated_at: now_secs(),
        };
        let mut intent = scope.claim_upload_job(job.clone()).await?;
        let caller = scope.caller();
        let response = match caller.begin_upload(begin.clone()).await {
            Err(e) if is_ambiguous(&e) => {
                scope.check()?;
                check_cancel(Some(&stopped))?;
                caller.begin_upload(begin.clone()).await?
            }
            r => r?,
        };
        scope.check()?;
        let (upload_id, part_size) = match decide_begin_status(&response)? {
            BeginDecision::Completed(file) => {
                verify_file_proof(&file, size, &source.digest)?;
                job.state = "complete".into();
                job.file_id = file.file_id;
                scope.save_upload_job(job, &mut intent).await?;
                return Ok(file);
            }
            BeginDecision::NeedFreshKey => {
                return Err(transfer_error(
                    "upload terminal; fresh idempotency key required",
                ))
            }
            BeginDecision::Active {
                upload_id,
                part_size,
            } => (upload_id, part_size),
        };
        if part_size > MAX_TRANSFER_PART_BYTES as i64 {
            return Err(transfer_error(
                "negotiated upload part exceeds local memory budget",
            ));
        }
        let parallel = response
            .max_parallel_parts
            .filter(|n| *n > 0)
            .ok_or_else(|| transfer_error("ACTIVE upload lacks positive parallel limit"))?
            .min(MAX_TRANSFER_PARTS as i32) as usize;
        let count = part_count(size, part_size)?;
        job.upload_id = upload_id.clone();
        job.state = "active".into();
        scope.save_upload_job(job.clone(), &mut intent).await?;
        let snapshot = upload_snapshot(&caller, &upload_id, size, part_size).await?;
        if let Some(file) = snapshot.completed {
            verify_file_proof(&file, size, &source.digest)?;
            job.state = "complete".into();
            job.file_id = file.file_id;
            scope.save_upload_job(job, &mut intent).await?;
            return Ok(file);
        }
        // Local acknowledgements are diagnostic only. ONLY this complete server
        // snapshot can suppress transmission after restart or an unknown outcome.
        use futures_util::{stream::FuturesUnordered, StreamExt};
        let mut pending = FuturesUnordered::new();
        let mut next = 0;
        loop {
            scope.check()?;
            check_cancel(Some(&stopped))?;
            while pending.len() < parallel && next < count {
                let index = next;
                next += 1;
                if snapshot.contains(index) {
                    continue;
                }
                let source = source.clone();
                let caller = caller.clone();
                let upload_id = upload_id.clone();
                let token = stopped.clone();
                pending.push(async move {
                    let permit = TRANSFER_PARTS
                        .acquire()
                        .await
                        .map_err(|_| transfer_error("part admission closed"))?;
                    check_cancel(Some(&token))?;
                    let (offset, len) = part_byte_range(index, part_size, size)?;
                    let bytes = disk(move || source.read_part(offset as u64, len)).await?;
                    let request = tinkle_schema::proto::UploadPartRequest {
                        upload_id,
                        part_index: index,
                        sha256: Some(sha256(&bytes).to_vec().into()),
                        data: bytes.into(),
                    };
                    let result = caller.upload_part(request.clone()).await;
                    let result = match result {
                        Err(e) if is_ambiguous(&e) => {
                            check_cancel(Some(&token))?;
                            let status =
                                upload_snapshot(&caller, &request.upload_id, size, part_size)
                                    .await?;
                            if status.contains(index) || status.completed.is_some() {
                                Ok(())
                            } else {
                                caller
                                    .upload_part(request)
                                    .await
                                    .map(|r| r.part_index)
                                    .and_then(|i| {
                                        if i == index {
                                            Ok(())
                                        } else {
                                            Err(transfer_error("UploadPart echo mismatch"))
                                        }
                                    })
                            }
                        }
                        Ok(r) if r.part_index == index => Ok(()),
                        Ok(_) => Err(transfer_error("UploadPart echo mismatch")),
                        Err(e) => Err(e),
                    };
                    drop(permit);
                    result
                });
            }
            match pending.next().await {
                Some(result) => {
                    result?;
                    job.resume_offset += 1;
                    scope.save_upload_job(job.clone(), &mut intent).await?;
                }
                None => break,
            }
        }
        scope.check()?;
        check_cancel(Some(&stopped))?;
        job.state = "completing".into();
        scope.save_upload_job(job.clone(), &mut intent).await?;
        let request = CompleteUploadRequest {
            upload_id: upload_id.clone(),
            sha256: Some(source.digest.to_vec().into()),
        };
        let result = caller.complete_upload(request.clone()).await;
        let file = match result {
            Ok(r) => r
                .file
                .ok_or_else(|| transfer_error("CompleteUpload lacks file"))?,
            Err(e) if is_ambiguous(&e) => {
                scope.check()?;
                check_cancel(Some(&stopped))?;
                let snapshot = upload_snapshot(&caller, &upload_id, size, part_size).await?;
                match snapshot.completed {
                    Some(file) => file,
                    None => caller
                        .complete_upload(request)
                        .await?
                        .file
                        .ok_or_else(|| transfer_error("CompleteUpload lacks file"))?,
                }
            }
            Err(e) => return Err(e),
        };
        verify_file_proof(&file, size, &source.digest)?;
        job.state = "complete".into();
        job.file_id = file.file_id;
        scope.save_upload_job(job, &mut intent).await?;
        Ok(file)
    }

    /// Bounded convenience wrapper. Native large-file callers must use upload_path.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn upload_bytes(
        &self,
        params: UploadParams,
        data: Vec<u8>,
    ) -> Result<FileInfo, TinkleError> {
        if data.len() > MAX_CONVENIENCE_BYTES {
            return Err(transfer_error("byte upload exceeds convenience limit"));
        }
        let storage = self.client.sqlite_storage();
        let temp = disk(move || {
            use std::io::Write;
            let root = match storage {
                Some(storage) => storage
                    .temporary_directory()
                    .map_err(|_| transfer_error("scratch directory unavailable"))?,
                None => std::env::temp_dir(),
            };
            let mut temp = tempfile::NamedTempFile::new_in(root).map_err(io_error)?;
            temp.write_all(&data).map_err(io_error)?;
            Ok(temp)
        })
        .await?;
        self.upload_path(params, temp.path(), None).await
    }

    pub fn new(client: crate::client::Client) -> Self {
        Self {
            client,
            #[cfg(all(test, not(target_arch = "wasm32")))]
            intent_barrier: None,
        }
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
    #[cfg(target_arch = "wasm32")]
    pub async fn upload_bytes(
        &self,
        params: UploadParams,
        data: Vec<u8>,
    ) -> Result<FileInfo, TinkleError> {
        if data.len() > MAX_CONVENIENCE_BYTES {
            return Err(transfer_error("byte upload exceeds convenience limit"));
        }
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

    #[cfg(target_arch = "wasm32")]
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
        if part_size > MAX_TRANSFER_PART_BYTES as i64 || n > 65536 {
            return Err(transfer_error("upload part/snapshot budget exceeded"));
        }
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
    #[cfg(target_arch = "wasm32")]
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
#[cfg(any(test, target_arch = "wasm32"))]
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
#[cfg(target_arch = "wasm32")]
struct SharedDownload {
    notify: tokio::sync::Notify,
    result: Mutex<Option<Result<Vec<u8>, TinkleError>>>,
    /// Exactly one winner runs the shared fetch (atomic compare-exchange).
    claim: AtomicBool,
}

#[cfg(target_arch = "wasm32")]
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
#[cfg(target_arch = "wasm32")]
struct LeaderGuard {
    shared: Arc<SharedDownload>,
    key: InflightKey,
    armed: bool,
}

#[cfg(target_arch = "wasm32")]
impl LeaderGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(target_arch = "wasm32")]
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

#[cfg(target_arch = "wasm32")]
type InflightKey = (String, i64);
#[cfg(target_arch = "wasm32")]
type InflightMap = HashMap<InflightKey, Arc<SharedDownload>>;

#[cfg(target_arch = "wasm32")]
fn inflight() -> &'static Mutex<InflightMap> {
    static MAP: OnceLock<Mutex<InflightMap>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Rust-owned download manager. Holds a `Client` clone (cheap).
///
/// When `cache_dir` is configured (native only), verified bytes are served
/// from the SQLite `file_cache_entries` + on-disk cache and every verified
/// download atomically publishes its cache copy before returning.
#[cfg(not(target_arch = "wasm32"))]
struct DiskArtifact {
    path: std::path::PathBuf,
    size: u64,
    digest: [u8; 32],
    // Uncached artifacts disappear with the final waiter, not before its publish.
    _temporary: Option<tempfile::TempPath>,
}

#[cfg(not(target_arch = "wasm32"))]
type DownloadKey = (String, u64, usize, i64, Vec<u8>, Option<std::path::PathBuf>);

/// Domain-separate owner omission from a present exact protobuf reference.
#[cfg(not(target_arch = "wasm32"))]
fn context_bytes(context: Option<FileAccessContext>) -> Vec<u8> {
    use prost::Message as _;
    match context {
        None => vec![0],
        Some(context) => {
            let mut bytes = vec![1];
            bytes.extend(context.encode_to_vec());
            bytes
        }
    }
}
#[cfg(not(target_arch = "wasm32"))]
struct DiskFlight {
    result: tokio::sync::watch::Receiver<Option<Result<Arc<DiskArtifact>, TinkleError>>>,
    stop: crate::CancelToken,
    waiters: std::sync::atomic::AtomicUsize,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
struct DownloadGroup {
    flights: Mutex<HashMap<DownloadKey, Arc<DiskFlight>>>,
    closing: std::sync::atomic::AtomicBool,
    join: tokio::sync::Mutex<()>,
}
#[cfg(not(target_arch = "wasm32"))]
impl Drop for DownloadGroup {
    fn drop(&mut self) {
        for flight in self
            .flights
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            flight.stop.cancel();
            if let Some(task) = flight
                .task
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
            {
                task.abort();
            }
        }
    }
}

/// Registry holds only weak client/group identities, never cross-deployment work.
/// Each Client coordinator owns a separate bounded map; fresh Clients do not join
/// on an untrusted textual principal or a colliding server-local file ID.
#[cfg(not(target_arch = "wasm32"))]
fn download_group(client: &crate::Client) -> Arc<DownloadGroup> {
    use std::sync::Weak;
    type Registry = Vec<(
        Weak<crate::authorization::Authorization>,
        Weak<DownloadGroup>,
    )>;
    static GROUPS: OnceLock<Mutex<Registry>> = OnceLock::new();
    let mut groups = GROUPS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    groups.retain(|(client, group)| client.strong_count() > 0 && group.strong_count() > 0);
    for (owner, group) in groups.iter() {
        if owner.ptr_eq(&Arc::downgrade(&client.authorization)) {
            if let Some(group) = group.upgrade() {
                return group;
            }
        }
    }
    let group = Arc::new(DownloadGroup::default());
    groups.push((
        Arc::downgrade(&client.authorization),
        Arc::downgrade(&group),
    ));
    group
}
#[cfg(not(target_arch = "wasm32"))]
struct DownloadWaiter(Arc<DiskFlight>);
#[cfg(not(target_arch = "wasm32"))]
impl Drop for DownloadWaiter {
    fn drop(&mut self) {
        if self.0.waiters.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.stop.cancel();
        }
    }
}

/// Put unfinished shutdown ownership back if its foreign/Rust waiter is dropped.
#[cfg(not(target_arch = "wasm32"))]
struct DownloadJoin {
    flight: Arc<DiskFlight>,
    task: Option<tokio::task::JoinHandle<()>>,
}
#[cfg(not(target_arch = "wasm32"))]
impl Drop for DownloadJoin {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            *self.flight.task.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
        }
    }
}

/// Sync the directory entry as well as file contents before durable acknowledgement.
#[cfg(not(target_arch = "wasm32"))]
fn sync_directory(path: &std::path::Path) -> Result<(), TinkleError> {
    std::fs::File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(io_error)
}

/// Rust-owned native disk download pipeline with bounded byte convenience APIs.
/// Restart-durable prefixes require a SQLite-backed Client and a private cache
/// directory. Otherwise transfer still streams through a private temporary file,
/// but a new attempt starts at zero rather than claiming unavailable progress.
#[derive(Clone)]
pub struct DownloadManager {
    client: crate::client::Client,
    #[cfg(not(target_arch = "wasm32"))]
    cache_dir: Option<std::path::PathBuf>,
    #[cfg(not(target_arch = "wasm32"))]
    group: Arc<DownloadGroup>,
}

impl DownloadManager {
    /// Stop new admission for this Client's shared group, cancel workers and join
    /// them. Interrupted teardown retains each join owner for the next shutdown.
    /// Call outside the Client connection gate; commits acquire that same gate.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn shutdown(&self) {
        self.group.closing.store(true, Ordering::SeqCst);
        let _join = self.group.join.lock().await;
        let flights: Vec<_> = self
            .group
            .flights
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        for flight in &flights {
            flight.stop.cancel();
        }
        for flight in flights {
            let task = flight.task.lock().unwrap_or_else(|e| e.into_inner()).take();
            let mut owner = DownloadJoin { flight, task };
            if let Some(task) = owner.task.as_mut() {
                let _ = task.await;
            }
            owner.task = None;
        }
        self.group
            .flights
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// At most 8 MiB is returned in memory. Large consumers must use download_to_path.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn download_bytes(
        &self,
        file_id: i64,
        cancel: Option<crate::CancelToken>,
    ) -> Result<Vec<u8>, TinkleError> {
        self.download_bytes_scoped(file_id, None, cancel).await
    }

    /// Read under exactly this reference; denial never selects owner authorization
    /// or another waiter's reference. The byte convenience limit still applies.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn download_bytes_with_context(
        &self,
        file_id: i64,
        context: FileAccessContext,
        cancel: Option<crate::CancelToken>,
    ) -> Result<Vec<u8>, TinkleError> {
        self.download_bytes_scoped(file_id, Some(context), cancel)
            .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn download_bytes_scoped(
        &self,
        file_id: i64,
        context: Option<FileAccessContext>,
        cancel: Option<crate::CancelToken>,
    ) -> Result<Vec<u8>, TinkleError> {
        check_cancel(cancel.as_ref())?;
        let scope = TransferScope::capture(&self.client).await?;
        let artifact = self
            .download_artifact(file_id, context, cancel.clone(), true, scope.clone())
            .await?;
        if artifact.size > MAX_CONVENIENCE_BYTES as u64 {
            return Err(transfer_error("byte download exceeds convenience limit"));
        }
        let data = disk(move || {
            use std::io::Read;
            check_cancel(cancel.as_ref())?;
            let mut file = std::fs::File::open(&artifact.path).map_err(io_error)?;
            let mut bytes = Vec::new();
            file.by_ref()
                .take(MAX_CONVENIENCE_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(io_error)?;
            if bytes.len() as u64 != artifact.size {
                return Err(transfer_error("download artifact changed"));
            }
            if sha256(&bytes) != artifact.digest {
                return Err(transfer_error("download artifact digest changed"));
            }
            Ok(bytes)
        })
        .await?;
        scope.check()?;
        Ok(data)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn download_artifact(
        &self,
        file_id: i64,
        context: Option<FileAccessContext>,
        cancel: Option<crate::CancelToken>,
        byte_api: bool,
        scope: Arc<TransferScope>,
    ) -> Result<Arc<DiskArtifact>, TinkleError> {
        if file_id <= 0 {
            return Err(transfer_error("file_id must be positive"));
        }
        check_cancel(cancel.as_ref())?;
        scope.check()?;
        // Each waiter rechecks its own current reference BEFORE any flight/cache.
        let caller = scope.caller();
        let token = cancel.clone().unwrap_or_default();
        let info = tokio::select! { biased;
            _ = token.cancelled() => return Err(TinkleError::Syntra(syntra_core::error::SyntraError::Cancelled("download cancelled".into()))),
            result = caller.get_file_info(tinkle_schema::proto::GetFileInfoRequest { file_id, file_access_context: context }) => result?,
        };
        scope.check()?;
        let file = info
            .file
            .as_ref()
            .ok_or_else(|| transfer_error("GetFileInfo lacks file"))?;
        if file.file_id != file_id || file.size_bytes < 0 || file.sha256.len() != 32 {
            return Err(transfer_error("invalid FileInfo"));
        }
        if byte_api && file.size_bytes > MAX_CONVENIENCE_BYTES as i64 {
            return Err(transfer_error("byte download exceeds convenience limit"));
        }
        use prost::Message as _;
        let mut context_key = context_bytes(context);
        context_key.extend(file.encode_to_vec());
        let key = (
            scope.namespace.clone(),
            scope.epoch,
            Arc::as_ptr(&scope.generation) as usize,
            file_id,
            context_key,
            self.cache_dir.clone(),
        );
        let flight = {
            let mut flights = self.group.flights.lock().unwrap_or_else(|e| e.into_inner());
            if self.group.closing.load(Ordering::SeqCst) {
                return Err(transfer_error("download group shut down"));
            }
            flights.retain(|_, f| {
                f.waiters.load(Ordering::Acquire) > 0
                    || f.task
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .as_ref()
                        .is_some_and(|t| !t.is_finished())
            });
            if let Some(flight) = flights.get(&key) {
                // A drained/cancelling worker retains its join owner. Never start
                // a second writer on its resumable file until that worker exits.
                if flight.stop.is_cancelled() {
                    return Err(transfer_error("download stopping; retry after join"));
                }
                if flight.waiters.load(Ordering::Acquire) >= 64 {
                    return Err(transfer_error("download waiter admission full"));
                }
                flight.waiters.fetch_add(1, Ordering::AcqRel);
                flight.clone()
            } else {
                if flights.len() >= 8 {
                    return Err(transfer_error("download admission full"));
                }
                let permit = TRANSFER_JOBS
                    .try_acquire()
                    .map_err(|_| transfer_error("transfer admission full"))?;
                let (sender, receiver) = tokio::sync::watch::channel(None);
                let flight = Arc::new(DiskFlight {
                    result: receiver,
                    stop: crate::CancelToken::new(),
                    waiters: std::sync::atomic::AtomicUsize::new(1),
                    task: Mutex::new(None),
                });
                let stop = flight.stop.clone();
                let mut runner = self.clone();
                let captured = scope.clone();
                runner.client = scope.client.background_clone();
                let task = tokio::spawn(async move {
                    let _permit = permit;
                    // Never drop a disk future on cancellation: its blocking
                    // writer must finish before another worker may resume it.
                    let result = runner
                        .fetch_disk(file_id, context, info, captured, stop)
                        .await;
                    sender.send_replace(Some(result.map(Arc::new)));
                });
                *flight.task.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
                flights.insert(key, flight.clone());
                flight
            }
        };
        let _waiter = DownloadWaiter(flight.clone());
        let mut receiver = flight.result.clone();
        let token = cancel.unwrap_or_default();
        loop {
            scope.check()?;
            check_cancel(Some(&token))?;
            if let Some(result) = receiver.borrow().as_ref() {
                return result.clone();
            }
            tokio::select! { biased;
                _ = token.cancelled() => return Err(TinkleError::Syntra(syntra_core::error::SyntraError::Cancelled("download cancelled".into()))),
                changed = receiver.changed() => if changed.is_err() { return Err(transfer_error("download worker failed")); },
            }
        }
    }

    /// Resume only a durable prefix whose hash matches its job and FileInfo.
    /// Data fsync precedes each SQLite offset write; verification precedes rename.
    #[cfg(not(target_arch = "wasm32"))]
    async fn fetch_disk(
        &self,
        file_id: i64,
        context: Option<FileAccessContext>,
        info: GetFileInfoResponse,
        scope: Arc<TransferScope>,
        stop: crate::CancelToken,
    ) -> Result<DiskArtifact, TinkleError> {
        use prost::Message as _;
        let caller = scope.caller();
        scope.check()?;
        let limit = effective_download_limit(&info)?.min(MAX_TRANSFER_PART_BYTES as i32);
        let file = info
            .file
            .ok_or_else(|| transfer_error("GetFileInfo lacks file"))?;
        if file.file_id != file_id || file.size_bytes < 0 || file.sha256.len() != 32 {
            return Err(transfer_error("invalid FileInfo"));
        }
        let root = self.cache_dir.clone();
        if root.as_ref().is_some_and(|p| p.to_str().is_none()) {
            return Err(transfer_error("durable cache path must be UTF-8"));
        }
        let identity = format!(
            "{}-{file_id}-{}-{}",
            scope.namespace,
            hex_encode(&file.sha256),
            hex_encode(&sha256(&context_bytes(context)))
        );
        let relative = format!("{identity}.bin");
        let temp_prefix = format!("transfer-{identity}-");
        let job_id = format!("download:{identity}");
        let proof = hex_encode(&file.encode_to_vec());
        let old = scope.load_job(&job_id).await?;
        let setup_root = root.clone();
        let setup_relative = relative.clone();
        let setup_proof = proof.clone();
        let expected = file.clone();
        let setup_stop = stop.clone();
        let (mut output, temporary, mut offset, mut hash, path, cache_hit) = disk(move || {
            use std::io::{Read, Seek, SeekFrom};
            if let Some(root) = setup_root.as_ref() {
                std::fs::create_dir_all(root).map_err(io_error)?;
                let final_path = root.join(&setup_relative);
                if let Ok(mut existing) = std::fs::symlink_metadata(&final_path).and_then(|m| {
                    if m.file_type().is_file() {
                        std::fs::File::open(&final_path)
                    } else {
                        Err(std::io::Error::other(
                            "cache artifact is not a regular file",
                        ))
                    }
                }) {
                    let (size, digest) = hash_reader(&mut existing, Some(&setup_stop))?;
                    if verify_file_proof(&expected, size as i64, &digest).is_ok() {
                        return Ok((existing, None, size as i64, Sha256::new(), final_path, true));
                    }
                }
            }
            // A job can name only a direct child of the configured private root;
            // no stored arbitrary path is opened, truncated, renamed or removed.
            if let (Some(root), Some(old)) = (setup_root.as_ref(), old) {
                let old_path = std::path::PathBuf::from(&old.temp_path);
                let parent_matches = old_path.parent() == Some(root.as_path());
                if parent_matches
                    && old_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with(&temp_prefix) && n.ends_with(".part"))
                    && old.session_json.starts_with(&(setup_proof.clone() + ":"))
                    && old.resume_offset >= 0
                    && old.resume_offset <= expected.size_bytes
                    && std::fs::symlink_metadata(&old_path).is_ok_and(|m| m.file_type().is_file())
                {
                    let mut output = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&old_path)
                        .map_err(io_error)?;
                    output
                        .try_lock()
                        .map_err(|_| transfer_error("download temp is owned by another writer"))?;
                    if output.metadata().map_err(io_error)?.len() >= old.resume_offset as u64 {
                        let mut hash = Sha256::new();
                        let mut remaining = old.resume_offset as u64;
                        let mut buffer = [0; 65536];
                        while remaining > 0 {
                            check_cancel(Some(&setup_stop))?;
                            let want = remaining.min(buffer.len() as u64) as usize;
                            output.read_exact(&mut buffer[..want]).map_err(io_error)?;
                            hash.update(&buffer[..want]);
                            remaining -= want as u64;
                        }
                        let prefix = hex_encode(&hash.clone().finalize());
                        if old.session_json == format!("{setup_proof}:{prefix}") {
                            output.set_len(old.resume_offset as u64).map_err(io_error)?;
                            output
                                .seek(SeekFrom::Start(old.resume_offset as u64))
                                .map_err(io_error)?;
                            return Ok((output, None, old.resume_offset, hash, old_path, false));
                        }
                    }
                }
            }
            let temporary = match setup_root.as_ref() {
                Some(root) => tempfile::Builder::new()
                    .prefix(&temp_prefix)
                    .suffix(".part")
                    .tempfile_in(root),
                None => tempfile::NamedTempFile::new(),
            }
            .map_err(io_error)?;
            let (output, temp) = temporary.into_parts();
            output
                .try_lock()
                .map_err(|_| transfer_error("download temp lock failed"))?;
            let (temporary, path) = if setup_root.is_some() {
                (None, temp.keep().map_err(|e| io_error(e.error))?)
            } else {
                let path = temp.to_path_buf();
                (Some(temp), path)
            };
            if let Some(root) = setup_root.as_ref() {
                output.sync_all().map_err(io_error)?;
                sync_directory(root)?;
            }
            Ok((output, temporary, 0, Sha256::new(), path, false))
        })
        .await?;
        let mut job = tinkle_storage::FileTransferJob {
            job_id,
            kind: "download".into(),
            file_scope: SHARED_FILE_SCOPE.into(),
            file_id,
            upload_id: String::new(),
            state: "active".into(),
            session_json: format!("{proof}:{}", hex_encode(&hash.clone().finalize())),
            temp_path: path.to_string_lossy().into_owned(),
            resume_offset: offset,
            updated_at: now_secs(),
        };
        if !cache_hit {
            scope.save_job(job.clone()).await?;
        }
        while !cache_hit && offset < file.size_bytes {
            scope.check()?;
            check_cancel(Some(&stop))?;
            let _part = TRANSFER_PARTS
                .acquire()
                .await
                .map_err(|_| transfer_error("part admission closed"))?;
            let request = tinkle_schema::proto::DownloadPartRequest {
                file_id,
                offset,
                limit,
                file_access_context: context,
            };
            let response = match caller.download_part(request).await {
                Err(e) if is_ambiguous(&e) => {
                    scope.check()?;
                    caller.download_part(request).await?
                }
                r => r?,
            };
            scope.check()?;
            check_cancel(Some(&stop))?;
            validate_download_part(&response, offset, file.size_bytes)?;
            if response.data.len() > limit as usize {
                return Err(transfer_error("download part exceeds requested bound"));
            }
            let next = offset + response.data.len() as i64;
            let (new_output, new_hash) = disk(move || {
                use std::io::Write;
                output.write_all(&response.data).map_err(io_error)?;
                output.sync_all().map_err(io_error)?;
                hash.update(&response.data);
                Ok((output, hash))
            })
            .await?;
            output = new_output;
            hash = new_hash;
            offset = next;
            scope.check()?;
            job.resume_offset = offset;
            job.session_json = format!("{proof}:{}", hex_encode(&hash.clone().finalize()));
            job.updated_at = now_secs();
            scope.save_job(job.clone()).await?;
        }
        if !cache_hit {
            verify_file_proof(&file, offset, &hash.finalize().into())?;
        }
        let final_path = match root {
            Some(root) => root.join(&relative),
            None => path.clone(),
        };
        let source_path = path.clone();
        let publish_path = final_path.clone();
        let captured = scope.clone();
        disk(move || {
            output.sync_all().map_err(io_error)?;
            captured.commit(|| {
                if source_path != publish_path {
                    std::fs::rename(&source_path, &publish_path).map_err(io_error)?;
                }
                sync_directory(publish_path.parent().unwrap_or(std::path::Path::new(".")))
            })?;
            drop(output);
            Ok(())
        })
        .await?;
        if self.cache_dir.is_some() {
            if let Some(storage) = self.storage() {
                let captured = scope.clone();
                let size = file.size_bytes;
                disk(move || {
                    captured.commit(|| {
                        let old = storage
                            .get_file_cache_entry(&captured.principal, SHARED_FILE_SCOPE, file_id)
                            .map_err(|_| transfer_error("cache read failed"))?;
                        storage
                            .upsert_file_cache_entry(
                                &captured.principal,
                                &tinkle_storage::FileCacheEntry {
                                    scope: SHARED_FILE_SCOPE.into(),
                                    file_id,
                                    relative_path: relative,
                                    actual_size: size,
                                    availability: "complete".into(),
                                    last_access: now_secs(),
                                    pinned: old.is_some_and(|e| e.pinned),
                                },
                            )
                            .map_err(|_| transfer_error("cache write failed"))
                    })
                })
                .await?;
            }
        }
        job.state = "complete".into();
        job.temp_path = final_path.to_string_lossy().into_owned();
        job.session_json = format!("{proof}:{}", hex_encode(&file.sha256));
        job.updated_at = now_secs();
        scope.save_job(job).await?;
        Ok(DiskArtifact {
            path: final_path,
            size: file.size_bytes as u64,
            digest: file
                .sha256
                .as_ref()
                .try_into()
                .map_err(|_| transfer_error("invalid file digest"))?,
            _temporary: temporary,
        })
    }

    pub fn new(client: crate::client::Client) -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            group: download_group(&client),
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

    /// Download one file to memory, verifying size + SHA-256.
    ///
    /// Flow: `GetFileInfo` → shared cache lookup / join-in-flight →
    /// `DownloadPart` ranges from `offset + len(data)` → verify → return.
    /// `cancel` is per-consumer: cancelling one waiter never cancels the
    /// shared download or other consumers (cancel isolation).
    #[cfg(target_arch = "wasm32")]
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

    #[cfg(target_arch = "wasm32")]
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
            .get_file_info(tinkle_schema::proto::GetFileInfoRequest {
                file_id,
                file_access_context: None,
            })
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
        if file.size_bytes > MAX_CONVENIENCE_BYTES as i64 {
            return Err(transfer_error("byte download exceeds convenience limit"));
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
                file_access_context: None,
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
                            file_access_context: None,
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
        self.download_to_path_scoped(file_id, final_path, None, cancel)
            .await
    }

    /// Publish using exactly this current application reference, never a substituted
    /// owner's grant. Durable prefix/cache identities include the reference.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn download_to_path_with_context(
        &self,
        file_id: i64,
        final_path: &std::path::Path,
        context: FileAccessContext,
        cancel: Option<crate::CancelToken>,
    ) -> Result<std::path::PathBuf, TinkleError> {
        self.download_to_path_scoped(file_id, final_path, Some(context), cancel)
            .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn download_to_path_scoped(
        &self,
        file_id: i64,
        final_path: &std::path::Path,
        context: Option<FileAccessContext>,
        cancel: Option<crate::CancelToken>,
    ) -> Result<std::path::PathBuf, TinkleError> {
        check_cancel(cancel.as_ref())?;
        let scope = TransferScope::capture(&self.client).await?;
        let artifact = self
            .download_artifact(file_id, context, cancel.clone(), false, scope.clone())
            .await?;
        scope.check()?;
        check_cancel(cancel.as_ref())?;
        let path = final_path.to_path_buf();
        let scope2 = scope.clone();
        disk(move || {
            use std::io::{Read, Write};
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(std::path::Path::new("."));
            let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(io_error)?;
            let mut input = std::fs::File::open(&artifact.path).map_err(io_error)?;
            let mut buffer = [0; 65536];
            let mut hash = Sha256::new();
            let mut size = 0u64;
            loop {
                scope2.check()?;
                check_cancel(cancel.as_ref())?;
                let n = input.read(&mut buffer).map_err(io_error)?;
                if n == 0 {
                    break;
                }
                size += n as u64;
                if size > artifact.size {
                    return Err(transfer_error("download artifact size changed"));
                }
                hash.update(&buffer[..n]);
                temp.write_all(&buffer[..n]).map_err(io_error)?;
            }
            if size != artifact.size || <[u8; 32]>::from(hash.finalize()) != artifact.digest {
                return Err(transfer_error(
                    "download artifact changed before publication",
                ));
            }
            temp.as_file().sync_all().map_err(io_error)?;
            scope2.commit(|| {
                check_cancel(cancel.as_ref())?;
                temp.persist(&path).map_err(|e| io_error(e.error))?;
                sync_directory(parent)
            })
        })
        .await?;
        scope.check()?;
        Ok(final_path.to_path_buf())
    }

    /// Recovery is now lazy per requested file: fetch_disk validates the durable
    /// prefix and repairs a published-but-unrecorded artifact before returning it.
    /// This compatibility hook validates its scope and reports zero swept files.
    /// It deliberately never sweeps `.part` files: a principal-local job list
    /// cannot prove another account/process has relinquished a temp file.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn recover_downloads(
        &self,
        principal: &str,
        cache_dir: &std::path::Path,
    ) -> Result<usize, TinkleError> {
        self.client.authorization.generation()?.valid()?;
        if principal == crate::sync::UNKNOWN_PRINCIPAL
            || principal != self.client.principal().await
            || self.cache_dir.as_deref() != Some(cache_dir)
        {
            return Err(transfer_error("download recovery scope mismatch"));
        }
        Ok(0)
    }
}

/// Atomic publish: write `bytes` to a unique temp file in the same
/// directory as `final_path`, `fsync`, then rename over the destination.
/// Same-filesystem rename guarantees readers never observe a partial file.
/// Each publish uses `create_new` + a per-process nonce so concurrent
/// publishers (same pid, same filename) never share one temp pathname.
#[cfg(all(test, not(target_arch = "wasm32")))]
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

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "files_lifecycle_regressions.rs"]
mod lifecycle_regressions;

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "files_intent_regressions.rs"]
mod intent_regressions;

#[cfg(test)]
mod tests {
    use super::*;

    /// Cache/flight keys retain exact chat and photo scope, including owner omission.
    #[test]
    fn chat_photo_context_keys_are_exact() {
        let key = |chat_id, photo_id| {
            context_bytes(Some(FileAccessContext {
                reference: Some(
                    tinkle_schema::proto::file_access_context::Reference::ChatPhoto(
                        tinkle_schema::proto::ChatPhotoReference { chat_id, photo_id },
                    ),
                ),
            }))
        };
        assert_ne!(key(7, 3), key(8, 3));
        assert_ne!(key(7, 3), key(7, 4));
        assert_ne!(key(7, 3), context_bytes(None));
    }

    fn file_info(id: i64, bytes: &[u8]) -> FileInfo {
        FileInfo {
            file_id: id,
            size_bytes: bytes.len() as i64,
            sha256: sha256(bytes).to_vec().into(),
            mime_type: None,
            file_name: None,
            created_at: None,
        }
    }

    #[test]
    fn sha256_matches_empty_vector() {
        assert_eq!(sha256(b"abc"), legacy_sha256(b"abc"));
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
            ..Default::default()
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
                        ..Default::default()
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
            let mut second = ops.messages[0].clone();
            second.message_id = 11;
            second.body = "m11".into();
            ops.messages.push(second);
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

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn upload_snapshot_uses_storage_root_not_source_parent_and_retains_lease() {
        let imports = tempfile::tempdir().unwrap();
        let source = imports.path().join("source.bin");
        std::fs::write(&source, b"original").unwrap();
        let private = tempfile::tempdir().unwrap();
        let storage =
            tinkle_storage::Storage::open(private.path().join("cache.sqlite").to_str().unwrap())
                .unwrap();
        let root = storage.temporary_directory().unwrap();
        let snapshot = FrozenSource::capture(&source, &root, &crate::CancelToken::new()).unwrap();
        let path = snapshot.file.path().to_path_buf();
        assert_eq!(path.parent(), Some(root.as_path()));
        std::fs::write(&source, b"modified").unwrap();
        assert_eq!(snapshot.read_part(0, 8).unwrap(), b"original");
        let retained = snapshot.clone();
        drop(snapshot);
        assert!(path.exists());
        drop(retained);
        assert!(!path.exists());
        assert_eq!(std::fs::read_dir(imports.path()).unwrap().count(), 1);
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
