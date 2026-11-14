//! SDK-internal, snapshot-fenced candidates in conversation source order, without snippets.

use super::*;

/// Opaque local scan position; never a server cursor or journal checkpoint.
#[derive(Debug, Clone)]
pub struct SearchPosition(String, i64, i64);

/// Bounded cached protobuf and its post-consumption scan position.
pub struct SearchCandidate {
    pub target_key: String,
    pub message_id: i64,
    pub sender_id: i64,
    pub accept_seq: i64,
    pub proto: Vec<u8>,
    pub position: SearchPosition,
}

/// Constant-only SQL choices let SQLite seek the source-order index without sorting
/// whole cached protobufs or scanning preceding cursor pages. All values remain bound.
fn scan_sql(target: bool, after: bool, ascending: bool) -> String {
    let target = if target {
        "AND m.conversation_key=?2"
    } else {
        "AND ?2 IS NULL"
    };
    let (direction, comparison) = if ascending {
        ("ASC", ">")
    } else {
        ("DESC", "<")
    };
    let cursor = if after {
        format!("AND (m.conversation_key,m.accept_seq,m.message_id){comparison}(?3,?4,?5)")
    } else {
        "AND ?3 IS NULL".into()
    };
    format!("SELECT m.conversation_key,m.message_id,m.from_id,m.accept_seq,m.proto,length(m.proto)
        FROM messages m JOIN stream_state s ON s.principal=m.principal AND s.stream=m.conversation_key AND s.valid=1
        WHERE m.principal=?1 {target} AND m.accept_seq>0 AND m.message_id>0
        AND NOT EXISTS(SELECT 1 FROM message_tombstones t WHERE t.principal=m.principal AND t.conversation_key=m.conversation_key AND t.message_id=m.message_id)
        {cursor} ORDER BY m.conversation_key {direction},m.accept_seq {direction},m.message_id {direction} LIMIT ?6")
}

impl Storage {
    /// Capture a cache-only revision, never changing any journal progress.
    pub fn capture_search_snapshot(
        &self,
        principal: &str,
    ) -> Result<MessageSearchSnapshot, StorageError> {
        check_principal(principal)?;
        let conn = self.lock()?;
        Self::search_snapshot(&conn, principal)
    }

    /// Resolve a target-local boundary only from a valid, non-tombstoned cached body.
    pub fn search_reference(
        &self,
        snapshot: &MessageSearchSnapshot,
        target: &str,
        id: i64,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        check_target_key(target)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        if Self::search_snapshot(&tx, &snapshot.principal)? != *snapshot {
            return Err(StorageError::SearchChanged);
        }
        tx.query_row("SELECT m.proto,length(m.proto) FROM messages m
            JOIN stream_state s ON s.principal=m.principal AND s.stream=m.conversation_key AND s.valid=1
            WHERE m.principal=?1 AND m.conversation_key=?2 AND m.message_id=?3
            AND NOT EXISTS(SELECT 1 FROM message_tombstones t WHERE t.principal=m.principal AND t.conversation_key=m.conversation_key AND t.message_id=m.message_id)",
            params![snapshot.principal,target,id], |r| {
                let size: i64 = r.get(1)?;
                if size > 2_000_000 { return Err(rusqlite::Error::InvalidQuery); }
                r.get(0)
            }).optional().map_err(Into::into)
    }

    /// Scan at most sixteen bodies under one revision. Source positions are comparable
    /// only within a target; global cache traversal groups targets lexically, not by rank.
    /// The unicode61 token FTS cannot supply arbitrary literal-substring candidates;
    /// the SDK applies that predicate and typed filters after this bounded read.
    pub fn search_candidates(
        &self,
        snapshot: &MessageSearchSnapshot,
        target: Option<&str>,
        after: Option<&SearchPosition>,
        ascending: bool,
        limit: u32,
    ) -> Result<Vec<SearchCandidate>, StorageError> {
        if !(1..=16).contains(&limit) {
            return Err(StorageError::InvalidInput("bad scan limit".into()));
        }
        if let Some(target) = target {
            check_target_key(target)?;
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        if Self::search_snapshot(&tx, &snapshot.principal)? != *snapshot {
            return Err(StorageError::SearchChanged);
        }
        let sql = scan_sql(target.is_some(), after.is_some(), ascending);
        let mut stmt = tx.prepare(&sql)?;
        let mut rows = stmt.query(params![
            snapshot.principal,
            target,
            after.map(|p| &p.0),
            after.map(|p| p.1),
            after.map(|p| p.2),
            limit
        ])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let size: i64 = row.get(5)?;
            if size > 2_000_000 {
                return Err(StorageError::Corrupt(
                    "search message exceeds cache budget".into(),
                ));
            }
            let target_key: String = row.get(0)?;
            out.push(SearchCandidate {
                position: SearchPosition(target_key.clone(), row.get(3)?, row.get(1)?),
                target_key,
                message_id: row.get(1)?,
                sender_id: row.get(2)?,
                accept_seq: row.get(3)?,
                proto: row.get(4)?,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Source-order traversal uses the additive key-only index, never a temporary body sort.
    #[test]
    fn source_scan_uses_index_for_both_directions_and_global_continuation() {
        let storage = Storage::open_in_memory().unwrap();
        let conn = storage.lock().unwrap();
        for target in [false, true] {
            for after in [false, true] {
                for ascending in [false, true] {
                    let mut stmt = conn
                        .prepare(&format!(
                            "EXPLAIN QUERY PLAN {}",
                            scan_sql(target, after, ascending)
                        ))
                        .unwrap();
                    let plan = stmt
                        .query_map(
                            params![
                                "user:42",
                                target.then_some("target:room:7"),
                                after.then_some("target:room:7"),
                                after.then_some(2),
                                after.then_some(2),
                                16
                            ],
                            |r| r.get::<_, String>(3),
                        )
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap()
                        .join("\n");
                    assert!(plan.contains("idx_messages_search_source"), "{plan}");
                    assert!(!plan.contains("TEMP B-TREE"), "{plan}");
                }
            }
        }
    }
}
