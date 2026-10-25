//! Shared native SQLite cache + checkpoints for libtinkle.
//!
//! Rust owns migrations, schema, transactions, and queries. Swift/Kotlin
//! access the same file through UniFFI (`tinkle-ffi`). This crate exposes
//! only owned, FFI-friendly values and never leaks SQL in errors.
//!
//! Threading: `Storage` holds one `Mutex<Connection>`. `rusqlite::Connection`
//! is `Send` but not `Sync`; the mutex makes `Storage` `Send + Sync`.
//! Calls are short, bounded, and synchronous. Async callers must run them
//! via `spawn_blocking` (see `tinkle-ffi`) so Tokio workers are not blocked.

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection, OptionalExtension};
use thiserror::Error;

/// Version 2 adds `origin_stream` provenance to messages/dialogs, a
/// `read_state_pending` flag to dialogs, and the shared `entity_provenance`
/// table for users/chats.
const SCHEMA_VERSION: i64 = 2;

/// Errors surfaced from storage. Messages are safe for FFI; no SQL text.
#[derive(Debug, Error, Clone)]
pub enum StorageError {
    #[error("storage unavailable: {0}")]
    Unavailable(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("cursor scope mismatch: expected checkpoint {expected}, found {found} on {stream}")]
    ScopeMismatch {
        stream: String,
        expected: u64,
        found: u64,
    },
    #[error("storage corrupt: {0}")]
    Corrupt(String),
}

impl From<rusqlite::Error> for StorageError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Unavailable(e.to_string())
    }
}

fn check_cursor(cursor: u64) -> Result<(), StorageError> {
    if cursor == 0 || cursor > i64::MAX as u64 {
        return Err(StorageError::InvalidInput(
            "cursor must be 1..9223372036854775807".into(),
        ));
    }
    Ok(())
}

fn check_principal(principal: &str) -> Result<(), StorageError> {
    let (kind, id) = principal.split_once(':').ok_or_else(|| {
        StorageError::InvalidInput("principal must be user:<id> or room:<id>".into())
    })?;
    if kind != "user" && kind != "room" {
        return Err(StorageError::InvalidInput(
            "principal kind must be user/room".into(),
        ));
    }
    match id.parse::<i64>() {
        Ok(v) if v > 0 => Ok(()),
        _ => Err(StorageError::InvalidInput(
            "principal id must be positive".into(),
        )),
    }
}

fn check_stream(stream: &str) -> Result<(), StorageError> {
    check_principal(stream)
        .map_err(|_| StorageError::InvalidInput("stream must be user:<id> or room:<id>".into()))
}

/// Provenance origin: a valid stream, or `""` for legacy/global rows.
/// `""` pins the row: scoped rebuilds never delete it.
fn check_origin(origin: &str) -> Result<(), StorageError> {
    if origin.is_empty() {
        return Ok(());
    }
    check_stream(origin).map_err(|_| {
        StorageError::InvalidInput("origin_stream must be '' or user:<id>/room:<id>".into())
    })
}

/// Cached message row. `proto` is the complete `tinkle.v1.Message` bytes.
/// `origin_stream` is the update stream that produced this row; `""` is a
/// legacy/global pin that scoped rebuilds never delete.
#[derive(Debug, Clone)]
pub struct MessageRow {
    pub conversation_key: String,
    pub message_id: i64,
    pub peer_kind: String,
    pub peer_id: i64,
    pub from_id: i64,
    pub body: String,
    pub proto: Vec<u8>,
    pub origin_stream: String,
}

/// Cached dialog row. `proto` is the complete `tinkle.v1.Dialog` bytes.
/// `origin_stream` scopes the row to its producing update stream (`""` =
/// legacy/global pin, never deleted by scoped rebuilds).
/// `read_state_pending` means an invalidation committed with the checkpoint
/// and no authoritative refresh has cleared it yet; hosts should refresh.
#[derive(Debug, Clone)]
pub struct DialogRow {
    pub peer_kind: String,
    pub peer_id: i64,
    pub top_message: i64,
    pub unread_count: i32,
    pub read_inbox_max_id: i64,
    pub read_outbox_max_id: i64,
    pub pinned: bool,
    pub proto: Vec<u8>,
    pub origin_stream: String,
    pub read_state_pending: bool,
}

/// Cached user row. `proto` is the complete `tinkle.v1.User` bytes.
#[derive(Debug, Clone)]
pub struct UserRow {
    pub user_id: i64,
    pub proto: Vec<u8>,
}

/// Cached chat row. `proto` is the complete `tinkle.v1.Chat` bytes.
#[derive(Debug, Clone)]
pub struct ChatRow {
    pub chat_id: i64,
    pub proto: Vec<u8>,
}

/// Cache writes applied atomically with one checkpoint advance.
/// `read_state_scopes` lists streams whose dialogs must be marked
/// `read_state_pending` in the same transaction (the invalidation payload
/// carries no dialog identity, so the mark is stream-scoped).
#[derive(Debug, Default, Clone)]
pub struct CacheOps {
    pub messages: Vec<MessageRow>,
    pub dialogs: Vec<DialogRow>,
    pub users: Vec<UserRow>,
    pub chats: Vec<ChatRow>,
    pub read_state_scopes: Vec<String>,
}

/// Durable per-stream validity + replay bounds.
#[derive(Debug, Clone)]
pub struct StreamState {
    pub valid: bool,
    pub replay_floor: u64,
    pub head: Option<u64>,
}

/// Shared SQLite storage.
pub struct Storage {
    inner: Mutex<Connection>,
}

impl Storage {
    pub fn open(path: &str) -> Result<Self, StorageError> {
        if path.is_empty() {
            return Err(StorageError::InvalidInput(
                "db path must not be empty".into(),
            ));
        }
        let conn = Connection::open(Path::new(path))?;
        let s = Self {
            inner: Mutex::new(conn),
        };
        s.init()?;
        Ok(s)
    }

    pub fn open_in_memory() -> Result<Self, StorageError> {
        let conn = Connection::open_in_memory()?;
        let s = Self {
            inner: Mutex::new(conn),
        };
        s.init()?;
        Ok(s)
    }

    fn init(&self) -> Result<(), StorageError> {
        let conn = self
            .inner
            .lock()
            .map_err(|_| StorageError::Corrupt("storage lock poisoned during init".into()))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;
             PRAGMA busy_timeout=5000;",
        )?;
        // Base tables in v1 shape. v2 columns/indexes/tables land via
        // `migrate_1_to_2`, which runs for fresh databases too (on empty
        // tables), so the initial batch never references columns that an
        // existing v1 database does not have yet.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS checkpoints(
               principal TEXT NOT NULL, stream TEXT NOT NULL, cursor INTEGER NOT NULL,
               PRIMARY KEY(principal, stream));
             CREATE TABLE IF NOT EXISTS stream_state(
               principal TEXT NOT NULL, stream TEXT NOT NULL,
               valid INTEGER NOT NULL DEFAULT 1,
               replay_floor INTEGER NOT NULL DEFAULT 0,
               head INTEGER,
               PRIMARY KEY(principal, stream));
             CREATE TABLE IF NOT EXISTS messages(
               principal TEXT NOT NULL, conversation_key TEXT NOT NULL,
               message_id INTEGER NOT NULL, peer_kind TEXT NOT NULL, peer_id INTEGER NOT NULL,
               from_id INTEGER NOT NULL, body TEXT NOT NULL, proto BLOB NOT NULL,
               PRIMARY KEY(principal, conversation_key, message_id));
             CREATE INDEX IF NOT EXISTS idx_messages_conv
               ON messages(principal, conversation_key, message_id);
             CREATE TABLE IF NOT EXISTS dialogs(
               principal TEXT NOT NULL, peer_kind TEXT NOT NULL, peer_id INTEGER NOT NULL,
               top_message INTEGER NOT NULL DEFAULT 0, unread_count INTEGER NOT NULL DEFAULT 0,
               read_inbox_max_id INTEGER NOT NULL DEFAULT 0,
               read_outbox_max_id INTEGER NOT NULL DEFAULT 0,
               pinned INTEGER NOT NULL DEFAULT 0, proto BLOB NOT NULL,
               PRIMARY KEY(principal, peer_kind, peer_id));
             CREATE TABLE IF NOT EXISTS users(
               principal TEXT NOT NULL, user_id INTEGER NOT NULL, proto BLOB NOT NULL,
               PRIMARY KEY(principal, user_id));
             CREATE TABLE IF NOT EXISTS chats(
               principal TEXT NOT NULL, chat_id INTEGER NOT NULL, proto BLOB NOT NULL,
               PRIMARY KEY(principal, chat_id));",
        )?;
        // Schema version fencing + migration.
        let version: Option<String> = conn
            .query_row(
                "SELECT value FROM meta WHERE key='schema_version'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        match version {
            // Fresh database (tables just created empty): migrate to current.
            None => {
                Self::migrate_1_to_2(&conn)?;
            }
            Some(v) => {
                let parsed: i64 = v
                    .parse()
                    .map_err(|_| StorageError::Corrupt("bad schema_version in meta".into()))?;
                if parsed == 1 {
                    Self::migrate_1_to_2(&conn)?;
                } else if parsed != SCHEMA_VERSION {
                    return Err(StorageError::Corrupt(format!(
                        "unsupported schema_version {parsed}, expected {SCHEMA_VERSION}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// v1 → v2: provenance columns, dialog pending flag, and the shared
    /// provenance table. Existing rows keep `origin_stream = ''`, i.e. a
    /// global pin that scoped rebuilds never delete; existing users/chats
    /// get explicit `''` provenance pins for the same reason.
    fn migrate_1_to_2(conn: &Connection) -> Result<(), StorageError> {
        conn.execute_batch(
            "ALTER TABLE messages ADD COLUMN origin_stream TEXT NOT NULL DEFAULT '';
             ALTER TABLE dialogs ADD COLUMN origin_stream TEXT NOT NULL DEFAULT '';
             ALTER TABLE dialogs ADD COLUMN read_state_pending INTEGER NOT NULL DEFAULT 0;
             CREATE TABLE IF NOT EXISTS entity_provenance(
               principal TEXT NOT NULL, kind TEXT NOT NULL, entity_id INTEGER NOT NULL,
               stream TEXT NOT NULL,
               PRIMARY KEY(principal, kind, entity_id, stream));
             CREATE INDEX IF NOT EXISTS idx_messages_origin
               ON messages(principal, origin_stream);
             CREATE INDEX IF NOT EXISTS idx_dialogs_origin
               ON dialogs(principal, origin_stream);
             INSERT OR IGNORE INTO entity_provenance(principal, kind, entity_id, stream)
               SELECT principal, 'user', user_id, '' FROM users;
             INSERT OR IGNORE INTO entity_provenance(principal, kind, entity_id, stream)
               SELECT principal, 'chat', chat_id, '' FROM chats;
             UPDATE meta SET value='2' WHERE key='schema_version';",
        )?;
        Ok(())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Corrupt("storage lock poisoned".into()))
    }

    // -- checkpoints (CursorStore-compatible) -----------------------------

    pub fn load_checkpoint(
        &self,
        principal: &str,
        stream: &str,
    ) -> Result<Option<u64>, StorageError> {
        check_principal(principal)?;
        check_stream(stream)?;
        let conn = self.lock()?;
        let v: Option<i64> = conn
            .query_row(
                "SELECT cursor FROM checkpoints WHERE principal=?1 AND stream=?2",
                params![principal, stream],
                |r| r.get(0),
            )
            .optional()?;
        Ok(v.map(|c| c as u64))
    }

    pub fn save_checkpoint(
        &self,
        principal: &str,
        stream: &str,
        cursor: u64,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_stream(stream)?;
        check_cursor(cursor)?;
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO checkpoints(principal, stream, cursor) VALUES(?1,?2,?3)
             ON CONFLICT(principal, stream) DO UPDATE SET cursor=excluded.cursor",
            params![principal, stream, cursor as i64],
        )?;
        conn.execute(
            "INSERT INTO stream_state(principal, stream, valid, replay_floor)
             VALUES(?1,?2,1,0)
             ON CONFLICT(principal, stream) DO UPDATE SET valid=1",
            params![principal, stream],
        )?;
        Ok(())
    }

    pub fn remove_stream(&self, principal: &str, stream: &str) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_stream(stream)?;
        let conn = self.lock()?;
        conn.execute(
            "DELETE FROM checkpoints WHERE principal=?1 AND stream=?2",
            params![principal, stream],
        )?;
        Ok(())
    }

    /// Remove every row owned by `principal`: checkpoints, stream_state,
    /// and the cached messages/dialogs/users/chats. Logout-scoped wipe so a
    /// signed-out principal leaves no offline bodies behind.
    pub fn clear_principal(&self, principal: &str) -> Result<(), StorageError> {
        check_principal(principal)?;
        let conn = self.lock()?;
        for table in [
            "checkpoints",
            "stream_state",
            "messages",
            "dialogs",
            "users",
            "chats",
            "entity_provenance",
        ] {
            let sql = format!("DELETE FROM {table} WHERE principal=?1");
            conn.execute(&sql, params![principal])?;
        }
        Ok(())
    }

    // -- stream validity / replay bounds -----------------------------------

    /// Invalidate one stream's cache scope. Messages/dialogs carrying
    /// this stream's origin are deleted; legacy `''`-origin rows are pinned
    /// and survive. Users/chats are deleted only when no provenance
    /// reference remains (another stream or a global `''` refresh pin),
    /// so rebuilding one room never evicts an entity still used elsewhere.
    pub fn mark_rebuild(&self, principal: &str, stream: &str) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_stream(stream)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM checkpoints WHERE principal=?1 AND stream=?2",
            params![principal, stream],
        )?;
        tx.execute(
            "INSERT INTO stream_state(principal, stream, valid, replay_floor)
             VALUES(?1,?2,0,0)
             ON CONFLICT(principal, stream) DO UPDATE SET valid=0",
            params![principal, stream],
        )?;
        tx.execute(
            "DELETE FROM messages WHERE principal=?1 AND origin_stream=?2",
            params![principal, stream],
        )?;
        tx.execute(
            "DELETE FROM dialogs WHERE principal=?1 AND origin_stream=?2",
            params![principal, stream],
        )?;
        tx.execute(
            "DELETE FROM entity_provenance WHERE principal=?1 AND stream=?2",
            params![principal, stream],
        )?;
        for (table, kind, id_col) in [("users", "user", "user_id"), ("chats", "chat", "chat_id")] {
            let sql = format!(
                "DELETE FROM {table} WHERE principal=?1 AND NOT EXISTS (
                   SELECT 1 FROM entity_provenance
                   WHERE principal=?1 AND kind=?2 AND entity_id={table}.{id_col})"
            );
            tx.execute(&sql, params![principal, kind])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn stream_state(&self, principal: &str, stream: &str) -> Result<StreamState, StorageError> {
        check_principal(principal)?;
        check_stream(stream)?;
        let conn = self.lock()?;
        let row: Option<(i64, i64, Option<i64>)> = conn
            .query_row(
                "SELECT valid, replay_floor, head FROM stream_state WHERE principal=?1 AND stream=?2",
                params![principal, stream],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        Ok(match row {
            None => StreamState {
                valid: true,
                replay_floor: 0,
                head: None,
            },
            Some((valid, floor, head)) => StreamState {
                valid: valid != 0,
                replay_floor: floor as u64,
                head: head.map(|h| h as u64),
            },
        })
    }

    pub fn set_replay_bounds(
        &self,
        principal: &str,
        stream: &str,
        floor: u64,
        head: Option<u64>,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_stream(stream)?;
        if floor > i64::MAX as u64 {
            return Err(StorageError::InvalidInput("floor out of range".into()));
        }
        if let Some(h) = head {
            if h > i64::MAX as u64 {
                return Err(StorageError::InvalidInput("head out of range".into()));
            }
        }
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO stream_state(principal, stream, valid, replay_floor, head)
             VALUES(?1,?2,1,?3,?4)
             ON CONFLICT(principal, stream)
             DO UPDATE SET replay_floor=excluded.replay_floor, head=excluded.head",
            params![principal, stream, floor as i64, head.map(|h| h as i64)],
        )?;
        Ok(())
    }

    // -- atomic sync application -------------------------------------------
    //
    // Crash invariant: cache mutations and the checkpoint advance commit in
    // one SQLite transaction. A crash before commit replays the cursor; a
    // crash after commit skips it. UI state is rebuilt from this database.

    pub fn apply_sync_batch(
        &self,
        principal: &str,
        stream: &str,
        cursor: u64,
        expected_prev: u64,
        ops: &CacheOps,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_stream(stream)?;
        check_cursor(cursor)?;
        // Enforce contiguity here as well; SyncManager already classifies,
        // but the database must never commit a gap.
        let expected_next = expected_prev.saturating_add(1);
        if cursor != expected_next && !(expected_prev == 0 && cursor == 0) {
            let found: u64 = self.load_checkpoint(principal, stream)?.unwrap_or(0);
            return Err(StorageError::ScopeMismatch {
                stream: stream.to_string(),
                expected: expected_next,
                found,
            });
        }
        // Bound one transaction to keep it short and avoid holding the mutex
        // across network I/O (callers resolve refreshes before calling here).
        if ops.messages.len() > 512
            || ops.dialogs.len() > 256
            || ops.users.len() > 512
            || ops.chats.len() > 256
            || ops.read_state_scopes.len() > 64
        {
            return Err(StorageError::InvalidInput("sync batch too large".into()));
        }
        for m in &ops.messages {
            check_origin(&m.origin_stream)?;
        }
        for d in &ops.dialogs {
            check_origin(&d.origin_stream)?;
        }
        for scope in &ops.read_state_scopes {
            check_stream(scope)?;
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        // Fence: checkpoint must still equal expected_prev and scope valid.
        let current: Option<i64> = tx
            .query_row(
                "SELECT cursor FROM checkpoints WHERE principal=?1 AND stream=?2",
                params![principal, stream],
                |r| r.get(0),
            )
            .optional()?;
        let current = current.map(|c| c as u64).unwrap_or(0);
        if current != expected_prev {
            return Err(StorageError::ScopeMismatch {
                stream: stream.to_string(),
                expected: expected_prev,
                found: current,
            });
        }
        let valid: Option<i64> = tx
            .query_row(
                "SELECT valid FROM stream_state WHERE principal=?1 AND stream=?2",
                params![principal, stream],
                |r| r.get(0),
            )
            .optional()?;
        if valid == Some(0) {
            return Err(StorageError::Corrupt(format!(
                "stream {stream} requires rebuild; refusing to commit"
            )));
        }
        for m in &ops.messages {
            // Peer-less messages (optional Message.peer absent) project as
            // ("unknown", 0) with a conversation_key derived from context or
            // principal. Accept those so the checkpoint can advance; dialog
            // identity still requires a real peer (see projector, which skips
            // the dialog touch when the peer is unknown).
            let peer_ok = m.peer_id != 0 || m.peer_kind == "unknown";
            if m.message_id <= 0 || !peer_ok || m.proto.len() > 2_000_000 {
                return Err(StorageError::InvalidInput("bad message row".into()));
            }
            tx.execute(
                "INSERT INTO messages(principal, conversation_key, message_id, peer_kind, peer_id, from_id, body, proto, origin_stream)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)
                 ON CONFLICT(principal, conversation_key, message_id)
                 DO UPDATE SET peer_kind=excluded.peer_kind, peer_id=excluded.peer_id,
                   from_id=excluded.from_id, body=excluded.body, proto=excluded.proto,
                   origin_stream=excluded.origin_stream",
                params![
                    principal,
                    m.conversation_key,
                    m.message_id,
                    m.peer_kind,
                    m.peer_id,
                    m.from_id,
                    m.body,
                    m.proto,
                    m.origin_stream
                ],
            )?;
        }
        for d in &ops.dialogs {
            if d.peer_id == 0 || d.proto.len() > 1_000_000 {
                return Err(StorageError::InvalidInput("bad dialog row".into()));
            }
            // Dialog entries in a sync batch are top_message touches from the
            // projector: preserve unread/read/pinned columns and never move
            // top_message backwards (edits carry older ids). Proto follows
            // the winning top_message so the blob stays coherent.
            // Full dialog refreshes (unread/read/pinned) go through
            // `upsert_dialog`, not this path.
            tx.execute(
                "INSERT INTO dialogs(principal, peer_kind, peer_id, top_message, unread_count,
                   read_inbox_max_id, read_outbox_max_id, pinned, proto, origin_stream)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
                 ON CONFLICT(principal, peer_kind, peer_id)
                 DO UPDATE SET top_message=max(top_message, excluded.top_message),
                   proto=CASE WHEN excluded.top_message > top_message
                     THEN excluded.proto ELSE proto END,
                   origin_stream=excluded.origin_stream",
                params![
                    principal,
                    d.peer_kind,
                    d.peer_id,
                    d.top_message,
                    d.unread_count,
                    d.read_inbox_max_id,
                    d.read_outbox_max_id,
                    d.pinned as i32,
                    d.proto,
                    d.origin_stream
                ],
            )?;
        }
        for u in &ops.users {
            if u.user_id <= 0 || u.proto.len() > 1_000_000 {
                return Err(StorageError::InvalidInput("bad user row".into()));
            }
            tx.execute(
                "INSERT INTO users(principal, user_id, proto) VALUES(?1,?2,?3)
                 ON CONFLICT(principal, user_id) DO UPDATE SET proto=excluded.proto",
                params![principal, u.user_id, u.proto],
            )?;
        }
        for c in &ops.chats {
            if c.chat_id <= 0 || c.proto.len() > 1_000_000 {
                return Err(StorageError::InvalidInput("bad chat row".into()));
            }
            tx.execute(
                "INSERT INTO chats(principal, chat_id, proto) VALUES(?1,?2,?3)
                 ON CONFLICT(principal, chat_id) DO UPDATE SET proto=excluded.proto",
                params![principal, c.chat_id, c.proto],
            )?;
        }
        // Provenance: batch-touched entities stay reachable until every
        // referencing stream rebuilds. One row per (entity, stream).
        for u in &ops.users {
            tx.execute(
                "INSERT OR IGNORE INTO entity_provenance(principal, kind, entity_id, stream)
                 VALUES(?1,'user',?2,?3)",
                params![principal, u.user_id, stream],
            )?;
        }
        for c in &ops.chats {
            tx.execute(
                "INSERT OR IGNORE INTO entity_provenance(principal, kind, entity_id, stream)
                 VALUES(?1,'chat',?2,?3)",
                params![principal, c.chat_id, stream],
            )?;
        }
        // Read-state invalidation marks commit atomically with the
        // checkpoint below. The payload carries no dialog identity, so the
        // mark is stream-scoped; `''`-origin legacy rows are marked too
        // (safe over-marking: the next authoritative refresh clears it).
        for scope in &ops.read_state_scopes {
            tx.execute(
                "UPDATE dialogs SET read_state_pending=1
                 WHERE principal=?1 AND (origin_stream=?2 OR origin_stream='')",
                params![principal, scope],
            )?;
        }
        tx.execute(
            "INSERT INTO checkpoints(principal, stream, cursor) VALUES(?1,?2,?3)
             ON CONFLICT(principal, stream) DO UPDATE SET cursor=excluded.cursor",
            params![principal, stream, cursor as i64],
        )?;
        tx.execute(
            "INSERT INTO stream_state(principal, stream, valid, replay_floor)
             VALUES(?1,?2,1,0)
             ON CONFLICT(principal, stream) DO UPDATE SET valid=1",
            params![principal, stream],
        )?;
        tx.commit()?;
        Ok(())
    }

    // -- offline queries ----------------------------------------------------

    pub fn get_messages(
        &self,
        principal: &str,
        conversation_key: &str,
        limit: u32,
        before_id: Option<i64>,
    ) -> Result<Vec<MessageRow>, StorageError> {
        check_principal(principal)?;
        if conversation_key.is_empty() || conversation_key.len() > 128 {
            return Err(StorageError::InvalidInput("bad conversation_key".into()));
        }
        let limit = (limit.clamp(1, 500)) as i64;
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT conversation_key, message_id, peer_kind, peer_id, from_id, body, proto,
                    origin_stream
             FROM messages WHERE principal=?1 AND conversation_key=?2
             AND (?3 IS NULL OR message_id < ?3)
             ORDER BY message_id DESC LIMIT ?4",
        )?;
        let rows = stmt.query_map(
            params![principal, conversation_key, before_id, limit],
            |r| {
                Ok(MessageRow {
                    conversation_key: r.get(0)?,
                    message_id: r.get(1)?,
                    peer_kind: r.get(2)?,
                    peer_id: r.get(3)?,
                    from_id: r.get(4)?,
                    body: r.get(5)?,
                    proto: r.get(6)?,
                    origin_stream: r.get(7)?,
                })
            },
        )?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        out.reverse();
        Ok(out)
    }

    pub fn list_dialogs(
        &self,
        principal: &str,
        limit: u32,
    ) -> Result<Vec<DialogRow>, StorageError> {
        check_principal(principal)?;
        let limit = (limit.clamp(1, 500)) as i64;
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT peer_kind, peer_id, top_message, unread_count, read_inbox_max_id,
                    read_outbox_max_id, pinned, proto, origin_stream, read_state_pending
             FROM dialogs WHERE principal=?1 ORDER BY pinned DESC, top_message DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![principal, limit], |r| {
            Ok(DialogRow {
                peer_kind: r.get(0)?,
                peer_id: r.get(1)?,
                top_message: r.get(2)?,
                unread_count: r.get(3)?,
                read_inbox_max_id: r.get(4)?,
                read_outbox_max_id: r.get(5)?,
                pinned: r.get::<_, i32>(6)? != 0,
                proto: r.get(7)?,
                origin_stream: r.get(8)?,
                read_state_pending: r.get::<_, i32>(9)? != 0,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn get_user(&self, principal: &str, user_id: i64) -> Result<Option<UserRow>, StorageError> {
        check_principal(principal)?;
        if user_id <= 0 {
            return Err(StorageError::InvalidInput(
                "user_id must be positive".into(),
            ));
        }
        let conn = self.lock()?;
        let v: Option<Vec<u8>> = conn
            .query_row(
                "SELECT proto FROM users WHERE principal=?1 AND user_id=?2",
                params![principal, user_id],
                |r| r.get(0),
            )
            .optional()?;
        Ok(v.map(|proto| UserRow { user_id, proto }))
    }

    pub fn get_chat(&self, principal: &str, chat_id: i64) -> Result<Option<ChatRow>, StorageError> {
        check_principal(principal)?;
        if chat_id <= 0 {
            return Err(StorageError::InvalidInput(
                "chat_id must be positive".into(),
            ));
        }
        let conn = self.lock()?;
        let v: Option<Vec<u8>> = conn
            .query_row(
                "SELECT proto FROM chats WHERE principal=?1 AND chat_id=?2",
                params![principal, chat_id],
                |r| r.get(0),
            )
            .optional()?;
        Ok(v.map(|proto| ChatRow { chat_id, proto }))
    }

    /// Authoritative user write (e.g. GetUsers refresh). Pins the entity
    /// globally (`''` provenance) so stream-scoped rebuilds never evict an
    /// entity the snapshots still reference.
    pub fn upsert_user(&self, principal: &str, row: &UserRow) -> Result<(), StorageError> {
        check_principal(principal)?;
        if row.user_id <= 0 || row.proto.is_empty() || row.proto.len() > 1_000_000 {
            return Err(StorageError::InvalidInput("bad user row".into()));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO users(principal, user_id, proto) VALUES(?1,?2,?3)
             ON CONFLICT(principal, user_id) DO UPDATE SET proto=excluded.proto",
            params![principal, row.user_id, row.proto],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO entity_provenance(principal, kind, entity_id, stream)
             VALUES(?1,'user',?2,'')",
            params![principal, row.user_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Authoritative chat write (e.g. GetDialogs refresh). Pins globally;
    /// see [`Self::upsert_user`].
    pub fn upsert_chat(&self, principal: &str, row: &ChatRow) -> Result<(), StorageError> {
        check_principal(principal)?;
        if row.chat_id <= 0 || row.proto.is_empty() || row.proto.len() > 1_000_000 {
            return Err(StorageError::InvalidInput("bad chat row".into()));
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO chats(principal, chat_id, proto) VALUES(?1,?2,?3)
             ON CONFLICT(principal, chat_id) DO UPDATE SET proto=excluded.proto",
            params![principal, row.chat_id, row.proto],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO entity_provenance(principal, kind, entity_id, stream)
             VALUES(?1,'chat',?2,'')",
            params![principal, row.chat_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Authoritative dialog write (e.g. GetDialogs refresh): overwrites all
    /// columns including unread/read/pinned. Contrast with
    /// [`Self::apply_sync_batch`], where dialog entries are top_message-only
    /// touches that preserve those columns. Callers must supply a current
    /// snapshot; this does not MAX-clamp top_message.
    /// Clears `read_state_pending`: only an authoritative refresh — never a
    /// sync batch touch — may clear the flag.
    pub fn upsert_dialog(&self, principal: &str, row: &DialogRow) -> Result<(), StorageError> {
        check_principal(principal)?;
        if row.peer_id == 0 || row.proto.len() > 1_000_000 {
            return Err(StorageError::InvalidInput("bad dialog row".into()));
        }
        check_origin(&row.origin_stream)?;
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO dialogs(principal, peer_kind, peer_id, top_message, unread_count,
               read_inbox_max_id, read_outbox_max_id, pinned, proto, origin_stream,
               read_state_pending)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,0)
             ON CONFLICT(principal, peer_kind, peer_id)
             DO UPDATE SET top_message=excluded.top_message, unread_count=excluded.unread_count,
               read_inbox_max_id=excluded.read_inbox_max_id,
               read_outbox_max_id=excluded.read_outbox_max_id,
               pinned=excluded.pinned, proto=excluded.proto,
               origin_stream=excluded.origin_stream, read_state_pending=0",
            params![
                principal,
                row.peer_kind,
                row.peer_id,
                row.top_message,
                row.unread_count,
                row.read_inbox_max_id,
                row.read_outbox_max_id,
                row.pinned as i32,
                row.proto,
                row.origin_stream
            ],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(id: i64) -> MessageRow {
        MessageRow {
            conversation_key: "room:7".into(),
            message_id: id,
            peer_kind: "room".into(),
            peer_id: 7,
            from_id: 42,
            body: format!("m{id}"),
            proto: vec![1, 2, 3],
            origin_stream: "room:7".into(),
        }
    }

    fn dlg(peer_id: i64, top: i64) -> DialogRow {
        DialogRow {
            peer_kind: "room".into(),
            peer_id,
            top_message: top,
            unread_count: 0,
            read_inbox_max_id: 0,
            read_outbox_max_id: 0,
            pinned: false,
            proto: vec![4, 5, 6],
            origin_stream: "room:7".into(),
            read_state_pending: false,
        }
    }

    #[test]
    fn checkpoints_round_trip_and_principal_isolation() {
        let s = Storage::open_in_memory().unwrap();
        assert_eq!(s.load_checkpoint("user:1", "user:1").unwrap(), None);
        s.save_checkpoint("user:1", "user:1", 3).unwrap();
        assert_eq!(s.load_checkpoint("user:1", "user:1").unwrap(), Some(3));
        assert_eq!(s.load_checkpoint("user:2", "user:2").unwrap(), None);
        s.clear_principal("user:1").unwrap();
        assert_eq!(s.load_checkpoint("user:1", "user:1").unwrap(), None);
    }

    #[test]
    fn atomic_batch_commits_cache_and_checkpoint_together() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(msg(10));
        s.apply_sync_batch("user:1", "room:7", 1, 0, &ops).unwrap();
        assert_eq!(s.load_checkpoint("user:1", "room:7").unwrap(), Some(1));
        let got = s.get_messages("user:1", "room:7", 10, None).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].message_id, 10);
        // Gap must not commit partial cache writes.
        let mut ops2 = CacheOps::default();
        ops2.messages.push(msg(11));
        let err = s
            .apply_sync_batch("user:1", "room:7", 3, 2, &ops2)
            .unwrap_err();
        assert!(matches!(err, StorageError::ScopeMismatch { .. }));
        assert_eq!(s.load_checkpoint("user:1", "room:7").unwrap(), Some(1));
        assert_eq!(
            s.get_messages("user:1", "room:7", 10, None).unwrap().len(),
            1
        );
    }

    #[test]
    fn rebuild_fences_commits_until_checkpoint_resumes() {
        let s = Storage::open_in_memory().unwrap();
        s.save_checkpoint("user:1", "user:1", 5).unwrap();
        s.mark_rebuild("user:1", "user:1").unwrap();
        assert!(!s.stream_state("user:1", "user:1").unwrap().valid);
        assert_eq!(s.load_checkpoint("user:1", "user:1").unwrap(), None);
        let ops = CacheOps::default();
        assert!(s.apply_sync_batch("user:1", "user:1", 1, 0, &ops).is_err());
        // A fresh valid checkpoint re-arms the stream.
        s.save_checkpoint("user:1", "user:1", 1).unwrap();
        assert!(s.stream_state("user:1", "user:1").unwrap().valid);
    }

    #[test]
    fn rebuild_resets_cache_until_snapshot_recommits() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(msg(10));
        ops.dialogs.push(dlg(7, 10));
        s.apply_sync_batch("user:1", "room:7", 1, 0, &ops).unwrap();
        assert_eq!(
            s.get_messages("user:1", "room:7", 10, None).unwrap().len(),
            1
        );
        assert_eq!(s.list_dialogs("user:1", 10).unwrap().len(), 1);
        // Rebuild discards history: stale rows must not stay readable.
        s.mark_rebuild("user:1", "room:7").unwrap();
        assert!(s
            .get_messages("user:1", "room:7", 10, None)
            .unwrap()
            .is_empty());
        assert!(s.list_dialogs("user:1", 10).unwrap().is_empty());
        assert_eq!(s.load_checkpoint("user:1", "room:7").unwrap(), None);
        assert!(!s.stream_state("user:1", "room:7").unwrap().valid);
        // A fresh snapshot commit re-arms the stream and restores readability.
        s.save_checkpoint("user:1", "room:7", 1).unwrap();
        let mut ops2 = CacheOps::default();
        ops2.messages.push(msg(11));
        s.apply_sync_batch("user:1", "room:7", 2, 1, &ops2).unwrap();
        assert_eq!(
            s.get_messages("user:1", "room:7", 10, None).unwrap().len(),
            1
        );
    }

    #[test]
    fn rebuild_is_stream_scoped() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops7 = CacheOps::default();
        ops7.messages.push(msg(10));
        ops7.dialogs.push(dlg(7, 10));
        ops7.users.push(UserRow {
            user_id: 42,
            proto: vec![1],
        });
        ops7.chats.push(ChatRow {
            chat_id: 7,
            proto: vec![2],
        });
        s.apply_sync_batch("user:1", "room:7", 1, 0, &ops7).unwrap();

        let mut ops8 = CacheOps::default();
        let mut m8 = msg(20);
        m8.conversation_key = "room:8".into();
        m8.peer_id = 8;
        m8.origin_stream = "room:8".into();
        ops8.messages.push(m8);
        let mut d8 = dlg(8, 20);
        d8.origin_stream = "room:8".into();
        ops8.dialogs.push(d8);
        ops8.users.push(UserRow {
            user_id: 42,
            proto: vec![1],
        });
        ops8.chats.push(ChatRow {
            chat_id: 7,
            proto: vec![2],
        });
        s.apply_sync_batch("user:1", "room:8", 1, 0, &ops8).unwrap();

        s.mark_rebuild("user:1", "room:7").unwrap();

        // room:7 scope gone...
        assert!(s
            .get_messages("user:1", "room:7", 10, None)
            .unwrap()
            .is_empty());
        assert_eq!(s.load_checkpoint("user:1", "room:7").unwrap(), None);
        assert!(!s.stream_state("user:1", "room:7").unwrap().valid);
        // ...room:8 scope intact, including shared entities still referenced.
        assert_eq!(
            s.get_messages("user:1", "room:8", 10, None).unwrap().len(),
            1
        );
        let dialogs = s.list_dialogs("user:1", 10).unwrap();
        assert_eq!(dialogs.len(), 1);
        assert_eq!(dialogs[0].peer_id, 8);
        assert!(s.get_user("user:1", 42).unwrap().is_some());
        assert!(s.get_chat("user:1", 7).unwrap().is_some());
        assert_eq!(s.load_checkpoint("user:1", "room:8").unwrap(), Some(1));
    }

    #[test]
    fn shared_entity_deleted_after_last_reference() {
        let s = Storage::open_in_memory().unwrap();
        // User seen only in room:7's history: no other provenance.
        let mut ops = CacheOps::default();
        ops.users.push(UserRow {
            user_id: 42,
            proto: vec![1],
        });
        s.apply_sync_batch("user:1", "room:7", 1, 0, &ops).unwrap();
        assert!(s.get_user("user:1", 42).unwrap().is_some());
        s.mark_rebuild("user:1", "room:7").unwrap();
        assert!(s.get_user("user:1", 42).unwrap().is_none());
    }

    #[test]
    fn global_pin_survives_stream_rebuild() {
        let s = Storage::open_in_memory().unwrap();
        // Authoritative refresh pins globally: the snapshot still references
        // this entity even after a stream rebuild.
        s.upsert_user(
            "user:1",
            &UserRow {
                user_id: 42,
                proto: vec![1],
            },
        )
        .unwrap();
        s.upsert_chat(
            "user:1",
            &ChatRow {
                chat_id: 7,
                proto: vec![2],
            },
        )
        .unwrap();
        s.mark_rebuild("user:1", "room:7").unwrap();
        assert!(s.get_user("user:1", 42).unwrap().is_some());
        assert!(s.get_chat("user:1", 7).unwrap().is_some());
    }

    #[test]
    fn migration_1_to_2_pins_legacy_rows() {
        let dir = std::env::temp_dir().join(format!(
            "tinkle-mig-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tinkle.db");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta(key, value) VALUES('schema_version', '1');
                 CREATE TABLE checkpoints(principal TEXT NOT NULL, stream TEXT NOT NULL, cursor INTEGER NOT NULL, PRIMARY KEY(principal, stream));
                 CREATE TABLE stream_state(principal TEXT NOT NULL, stream TEXT NOT NULL, valid INTEGER NOT NULL DEFAULT 1, replay_floor INTEGER NOT NULL DEFAULT 0, head INTEGER, PRIMARY KEY(principal, stream));
                 CREATE TABLE messages(principal TEXT NOT NULL, conversation_key TEXT NOT NULL, message_id INTEGER NOT NULL, peer_kind TEXT NOT NULL, peer_id INTEGER NOT NULL, from_id INTEGER NOT NULL, body TEXT NOT NULL, proto BLOB NOT NULL, PRIMARY KEY(principal, conversation_key, message_id));
                 CREATE TABLE dialogs(principal TEXT NOT NULL, peer_kind TEXT NOT NULL, peer_id INTEGER NOT NULL, top_message INTEGER NOT NULL DEFAULT 0, unread_count INTEGER NOT NULL DEFAULT 0, read_inbox_max_id INTEGER NOT NULL DEFAULT 0, read_outbox_max_id INTEGER NOT NULL DEFAULT 0, pinned INTEGER NOT NULL DEFAULT 0, proto BLOB NOT NULL, PRIMARY KEY(principal, peer_kind, peer_id));
                 CREATE TABLE users(principal TEXT NOT NULL, user_id INTEGER NOT NULL, proto BLOB NOT NULL, PRIMARY KEY(principal, user_id));
                 CREATE TABLE chats(principal TEXT NOT NULL, chat_id INTEGER NOT NULL, proto BLOB NOT NULL, PRIMARY KEY(principal, chat_id));
                 INSERT INTO users(principal, user_id, proto) VALUES('user:1', 42, X'0102');
                 INSERT INTO messages(principal, conversation_key, message_id, peer_kind, peer_id, from_id, body, proto) VALUES('user:1', 'room:7', 10, 'room', 7, 42, 'm10', X'010203');",
            )
            .unwrap();
        }
        let s = Storage::open(path.to_str().unwrap()).unwrap();
        // Legacy rows readable with a global pin...
        let msgs = s.get_messages("user:1", "room:7", 10, None).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].origin_stream, "");
        // ...and survive a scoped rebuild of another stream's history.
        s.mark_rebuild("user:1", "room:7").unwrap();
        assert!(s.get_user("user:1", 42).unwrap().is_some());
        assert_eq!(
            s.get_messages("user:1", "room:7", 10, None).unwrap().len(),
            1
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_state_pending_commits_atomically_and_clears_on_refresh() {
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(msg(10));
        ops.dialogs.push(dlg(7, 10));
        s.apply_sync_batch("user:1", "room:7", 1, 0, &ops).unwrap();
        assert!(!s.list_dialogs("user:1", 10).unwrap()[0].read_state_pending);

        // Invalidation commits atomically with the checkpoint advance.
        let mut inv = CacheOps::default();
        inv.read_state_scopes.push("room:7".into());
        s.apply_sync_batch("user:1", "room:7", 2, 1, &inv).unwrap();
        assert_eq!(s.load_checkpoint("user:1", "room:7").unwrap(), Some(2));
        assert!(s.list_dialogs("user:1", 10).unwrap()[0].read_state_pending);

        // A later message touch must not clear the flag...
        let mut touch = CacheOps::default();
        touch.messages.push(msg(11));
        touch.dialogs.push(dlg(7, 11));
        s.apply_sync_batch("user:1", "room:7", 3, 2, &touch)
            .unwrap();
        assert!(s.list_dialogs("user:1", 10).unwrap()[0].read_state_pending);

        // ...only the authoritative refresh clears it.
        s.upsert_dialog("user:1", &dlg(7, 11)).unwrap();
        assert!(!s.list_dialogs("user:1", 10).unwrap()[0].read_state_pending);
    }

    #[test]
    fn rejects_bad_inputs_without_sql_leak() {
        let s = Storage::open_in_memory().unwrap();
        assert!(s.load_checkpoint("bad", "user:1").is_err());
        assert!(s.save_checkpoint("user:1", "user:1", 0).is_err());
        assert!(s.get_messages("user:1", "", 10, None).is_err());
    }

    #[test]
    fn peerless_message_advances_checkpoint_without_dialog() {
        // Optional Message.peer absent projects ("unknown", 0): the message
        // row commits under its derived conversation_key and the checkpoint
        // advances; no dialog touch is stored (no valid dialog identity).
        let s = Storage::open_in_memory().unwrap();
        let mut ops = CacheOps::default();
        ops.messages.push(MessageRow {
            conversation_key: "room:7".into(),
            message_id: 10,
            peer_kind: "unknown".into(),
            peer_id: 0,
            from_id: 42,
            body: "m10".into(),
            proto: vec![1, 2, 3],
            origin_stream: "room:7".into(),
        });
        s.apply_sync_batch("user:1", "room:7", 1, 0, &ops).unwrap();
        assert_eq!(s.load_checkpoint("user:1", "room:7").unwrap(), Some(1));
        assert_eq!(
            s.get_messages("user:1", "room:7", 10, None).unwrap().len(),
            1
        );
        assert!(s.list_dialogs("user:1", 10).unwrap().is_empty());
        // A zero peer with a concrete kind is still rejected.
        let mut bad = CacheOps::default();
        bad.messages.push(MessageRow {
            conversation_key: "room:7".into(),
            message_id: 11,
            peer_kind: "room".into(),
            peer_id: 0,
            from_id: 42,
            body: "m11".into(),
            proto: vec![1, 2, 3],
            origin_stream: "room:7".into(),
        });
        assert!(s.apply_sync_batch("user:1", "room:7", 2, 1, &bad).is_err());
    }

    #[test]
    fn contiguity_check_does_not_overflow_at_max() {
        // expected_prev = u64::MAX must not panic (debug overflow); it is a
        // fence mismatch, reported as ScopeMismatch without committing.
        let s = Storage::open_in_memory().unwrap();
        let ops = CacheOps::default();
        let err = s
            .apply_sync_batch("user:1", "room:7", 1, u64::MAX, &ops)
            .unwrap_err();
        assert!(matches!(err, StorageError::ScopeMismatch { .. }));
        assert_eq!(s.load_checkpoint("user:1", "room:7").unwrap(), None);
    }
}
