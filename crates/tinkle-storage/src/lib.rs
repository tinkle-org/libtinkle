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

const SCHEMA_VERSION: i64 = 1;

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

/// Cached message row. `proto` is the complete `tinkle.v1.Message` bytes.
#[derive(Debug, Clone)]
pub struct MessageRow {
    pub conversation_key: String,
    pub message_id: i64,
    pub peer_kind: String,
    pub peer_id: i64,
    pub from_id: i64,
    pub body: String,
    pub proto: Vec<u8>,
}

/// Cached dialog row. `proto` is the complete `tinkle.v1.Dialog` bytes.
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
#[derive(Debug, Default, Clone)]
pub struct CacheOps {
    pub messages: Vec<MessageRow>,
    pub dialogs: Vec<DialogRow>,
    pub users: Vec<UserRow>,
    pub chats: Vec<ChatRow>,
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
        // Schema version fencing.
        let version: Option<String> = conn
            .query_row(
                "SELECT value FROM meta WHERE key='schema_version'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        match version {
            None => {
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('schema_version', ?1)",
                    params![SCHEMA_VERSION.to_string()],
                )?;
            }
            Some(v) => {
                let parsed: i64 = v
                    .parse()
                    .map_err(|_| StorageError::Corrupt("bad schema_version in meta".into()))?;
                if parsed != SCHEMA_VERSION {
                    return Err(StorageError::Corrupt(format!(
                        "unsupported schema_version {parsed}, expected {SCHEMA_VERSION}"
                    )));
                }
            }
        }
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
        ] {
            let sql = format!("DELETE FROM {table} WHERE principal=?1");
            conn.execute(&sql, params![principal])?;
        }
        Ok(())
    }

    // -- stream validity / replay bounds -----------------------------------

    pub fn mark_rebuild(&self, principal: &str, stream: &str) -> Result<(), StorageError> {
        check_principal(principal)?;
        check_stream(stream)?;
        let conn = self.lock()?;
        conn.execute(
            "DELETE FROM checkpoints WHERE principal=?1 AND stream=?2",
            params![principal, stream],
        )?;
        conn.execute(
            "INSERT INTO stream_state(principal, stream, valid, replay_floor)
             VALUES(?1,?2,0,0)
             ON CONFLICT(principal, stream) DO UPDATE SET valid=0",
            params![principal, stream],
        )?;
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
        if cursor != expected_prev.saturating_add(1) && !(expected_prev == 0 && cursor == 0) {
            // Enforce contiguity here as well; SyncManager already classifies,
            // but the database must never commit a gap.
            if cursor != expected_prev + 1 {
                let found: u64 = self.load_checkpoint(principal, stream)?.unwrap_or(0);
                return Err(StorageError::ScopeMismatch {
                    stream: stream.to_string(),
                    expected: expected_prev.saturating_add(1),
                    found,
                });
            }
        }
        // Bound one transaction to keep it short and avoid holding the mutex
        // across network I/O (callers resolve refreshes before calling here).
        if ops.messages.len() > 512
            || ops.dialogs.len() > 256
            || ops.users.len() > 512
            || ops.chats.len() > 256
        {
            return Err(StorageError::InvalidInput("sync batch too large".into()));
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
            if m.message_id <= 0 || m.peer_id == 0 || m.proto.len() > 2_000_000 {
                return Err(StorageError::InvalidInput("bad message row".into()));
            }
            tx.execute(
                "INSERT INTO messages(principal, conversation_key, message_id, peer_kind, peer_id, from_id, body, proto)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
                 ON CONFLICT(principal, conversation_key, message_id)
                 DO UPDATE SET peer_kind=excluded.peer_kind, peer_id=excluded.peer_id,
                   from_id=excluded.from_id, body=excluded.body, proto=excluded.proto",
                params![
                    principal,
                    m.conversation_key,
                    m.message_id,
                    m.peer_kind,
                    m.peer_id,
                    m.from_id,
                    m.body,
                    m.proto
                ],
            )?;
        }
        for d in &ops.dialogs {
            if d.peer_id == 0 || d.proto.len() > 1_000_000 {
                return Err(StorageError::InvalidInput("bad dialog row".into()));
            }
            tx.execute(
                "INSERT INTO dialogs(principal, peer_kind, peer_id, top_message, unread_count,
                   read_inbox_max_id, read_outbox_max_id, pinned, proto)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)
                 ON CONFLICT(principal, peer_kind, peer_id)
                 DO UPDATE SET top_message=excluded.top_message, unread_count=excluded.unread_count,
                   read_inbox_max_id=excluded.read_inbox_max_id,
                   read_outbox_max_id=excluded.read_outbox_max_id,
                   pinned=excluded.pinned, proto=excluded.proto",
                params![
                    principal,
                    d.peer_kind,
                    d.peer_id,
                    d.top_message,
                    d.unread_count,
                    d.read_inbox_max_id,
                    d.read_outbox_max_id,
                    d.pinned as i32,
                    d.proto
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
            "SELECT conversation_key, message_id, peer_kind, peer_id, from_id, body, proto
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
                    read_outbox_max_id, pinned, proto
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

    pub fn upsert_user(&self, principal: &str, row: &UserRow) -> Result<(), StorageError> {
        check_principal(principal)?;
        if row.user_id <= 0 || row.proto.is_empty() || row.proto.len() > 1_000_000 {
            return Err(StorageError::InvalidInput("bad user row".into()));
        }
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO users(principal, user_id, proto) VALUES(?1,?2,?3)
             ON CONFLICT(principal, user_id) DO UPDATE SET proto=excluded.proto",
            params![principal, row.user_id, row.proto],
        )?;
        Ok(())
    }

    pub fn upsert_chat(&self, principal: &str, row: &ChatRow) -> Result<(), StorageError> {
        check_principal(principal)?;
        if row.chat_id <= 0 || row.proto.is_empty() || row.proto.len() > 1_000_000 {
            return Err(StorageError::InvalidInput("bad chat row".into()));
        }
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO chats(principal, chat_id, proto) VALUES(?1,?2,?3)
             ON CONFLICT(principal, chat_id) DO UPDATE SET proto=excluded.proto",
            params![principal, row.chat_id, row.proto],
        )?;
        Ok(())
    }

    pub fn upsert_dialog(&self, principal: &str, row: &DialogRow) -> Result<(), StorageError> {
        check_principal(principal)?;
        if row.peer_id == 0 || row.proto.len() > 1_000_000 {
            return Err(StorageError::InvalidInput("bad dialog row".into()));
        }
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO dialogs(principal, peer_kind, peer_id, top_message, unread_count,
               read_inbox_max_id, read_outbox_max_id, pinned, proto)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)
             ON CONFLICT(principal, peer_kind, peer_id)
             DO UPDATE SET top_message=excluded.top_message, unread_count=excluded.unread_count,
               read_inbox_max_id=excluded.read_inbox_max_id,
               read_outbox_max_id=excluded.read_outbox_max_id,
               pinned=excluded.pinned, proto=excluded.proto",
            params![
                principal,
                row.peer_kind,
                row.peer_id,
                row.top_message,
                row.unread_count,
                row.read_inbox_max_id,
                row.read_outbox_max_id,
                row.pinned as i32,
                row.proto
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
    fn rejects_bad_inputs_without_sql_leak() {
        let s = Storage::open_in_memory().unwrap();
        assert!(s.load_checkpoint("bad", "user:1").is_err());
        assert!(s.save_checkpoint("user:1", "user:1", 0).is_err());
        assert!(s.get_messages("user:1", "", 10, None).is_err());
    }
}
