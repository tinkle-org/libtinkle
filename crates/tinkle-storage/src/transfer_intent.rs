//! Additive, lazily initialized transfer intent/CAS extension for existing caches.
//! Registration and checkpoints use immediate SQLite transactions, not process
//! locks. Parent-row deletion cascades with ordinary principal cleanup. An opaque
//! incarnation fences delete/recreate ABA; it is not an authorization credential.
use super::{
    check_file_scope, check_job_id, check_principal, check_transfer_kind, FileTransferJob, Storage,
    StorageError,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

/// Sanitized intent failures never expose SQL or canonical source/filename data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TransferIntentError {
    #[error("transfer intent conflicts with retained input")]
    Conflict,
    #[error("transfer checkpoint changed")]
    Changed,
    #[error("invalid transfer intent/checkpoint")]
    Invalid,
    #[error("transfer intent storage unavailable")]
    Unavailable,
}
impl From<rusqlite::Error> for TransferIntentError {
    fn from(_: rusqlite::Error) -> Self {
        Self::Unavailable
    }
}
impl From<StorageError> for TransferIntentError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::IntentConflict => Self::Conflict,
            StorageError::InvalidInput(_) => Self::Invalid,
            _ => Self::Unavailable,
        }
    }
}

impl From<TransferIntentError> for StorageError {
    fn from(error: TransferIntentError) -> Self {
        match error {
            TransferIntentError::Conflict | TransferIntentError::Changed => Self::IntentConflict,
            TransferIntentError::Invalid => Self::InvalidInput("invalid transfer intent".into()),
            TransferIntentError::Unavailable => {
                Self::Unavailable("transfer intent unavailable".into())
            }
        }
    }
}

/// Owned checkpoint with a private immutable proof and local incarnation. Only
/// storage constructs tokens; every CAS compares proof, incarnation, state and revision.
#[derive(Clone)]
pub struct TransferIntentCheckpoint {
    pub job: FileTransferJob,
    pub revision: u64,
    proof: String,
    incarnation: Vec<u8>,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS file_transfer_intents_v1(
 principal TEXT NOT NULL, job_id TEXT NOT NULL, proof TEXT NOT NULL,
 incarnation BLOB NOT NULL DEFAULT(randomblob(16)) CHECK(length(incarnation)=16),
 revision INTEGER NOT NULL DEFAULT 0 CHECK(typeof(revision)='integer' AND revision>=0),
 writing_revision INTEGER,
 PRIMARY KEY(principal,job_id),
 FOREIGN KEY(principal,job_id) REFERENCES file_transfers(principal,job_id) ON DELETE CASCADE
);
CREATE TRIGGER IF NOT EXISTS file_transfer_intent_guard_v1 BEFORE UPDATE ON file_transfers
WHEN EXISTS(SELECT 1 FROM file_transfer_intents_v1 i WHERE i.principal=OLD.principal AND i.job_id=OLD.job_id)
BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM file_transfer_intents_v1 i
  WHERE i.principal=OLD.principal AND i.job_id=OLD.job_id AND i.writing_revision=i.revision)
  THEN RAISE(ABORT,'transfer conditional update required') END;
 SELECT CASE WHEN NEW.principal!=OLD.principal OR NEW.job_id!=OLD.job_id OR NEW.kind!=OLD.kind
  OR NEW.file_scope!=OLD.file_scope OR (OLD.kind='upload' AND NEW.session_json!=(SELECT proof FROM file_transfer_intents_v1 i WHERE i.principal=OLD.principal AND i.job_id=OLD.job_id))
  THEN RAISE(ABORT,'immutable transfer intent') END;
 SELECT CASE WHEN OLD.state IN ('complete','aborted','expired') AND NEW.state!=OLD.state
  THEN RAISE(ABORT,'terminal transfer checkpoint') END;
END;
CREATE TRIGGER IF NOT EXISTS file_transfer_intent_revision_v1 AFTER UPDATE ON file_transfers
WHEN EXISTS(SELECT 1 FROM file_transfer_intents_v1 i WHERE i.principal=NEW.principal AND i.job_id=NEW.job_id)
BEGIN
 UPDATE file_transfer_intents_v1 SET revision=revision+1,writing_revision=NULL
  WHERE principal=NEW.principal AND job_id=NEW.job_id;
END;";

/// Validate metadata before SQLite allocation/admission. Proofs are opaque here;
/// upload callers supply the canonical source SHA plus exact Begin protobuf bytes.
fn validate(
    principal: &str,
    job: &FileTransferJob,
    proof: &str,
) -> Result<(), TransferIntentError> {
    check_principal(principal)?;
    check_job_id(&job.job_id)?;
    check_transfer_kind(&job.kind)?;
    check_file_scope(&job.file_scope)?;
    if proof.is_empty()
        || proof.len() > 16384
        || job.file_id < 0
        || job.resume_offset < 0
        || job.updated_at < 0
        || job.session_json.len() > 1_000_000
        || job.upload_id.len() > 4096
        || job.temp_path.len() > 16384
        || rank(&job.state).is_none()
        || (job.kind == "upload" && job.session_json != proof)
        || (job.state == "complete" && job.file_id <= 0)
    {
        return Err(TransferIntentError::Invalid);
    }
    Ok(())
}
/// Local checkpoint phase order; terminal alternatives share a rank but cannot replace each other.
fn rank(state: &str) -> Option<u8> {
    match state {
        "begin" => Some(0),
        "active" => Some(1),
        "completing" => Some(2),
        "complete" | "aborted" | "expired" => Some(3),
        _ => None,
    }
}
/// Terminal checkpoints may be observed again, never returned to an active phase.
fn terminal(state: &str) -> bool {
    matches!(state, "complete" | "aborted" | "expired")
}

/// Read one complete token inside the caller's transaction/connection guard.
fn read(
    conn: &Connection,
    principal: &str,
    id: &str,
) -> Result<Option<TransferIntentCheckpoint>, TransferIntentError> {
    let result = conn.query_row("SELECT j.kind,j.file_scope,j.file_id,j.upload_id,j.state,j.session_json,j.temp_path,j.resume_offset,j.updated_at,i.proof,i.incarnation,i.revision
        FROM file_transfers j JOIN file_transfer_intents_v1 i USING(principal,job_id) WHERE j.principal=?1 AND j.job_id=?2",params![principal,id], |r| {
        Ok(TransferIntentCheckpoint { job: FileTransferJob { job_id:id.into(),kind:r.get(0)?,file_scope:r.get(1)?,file_id:r.get(2)?,upload_id:r.get(3)?,state:r.get(4)?,session_json:r.get(5)?,temp_path:r.get(6)?,resume_offset:r.get(7)?,updated_at:r.get(8)? },
            proof:r.get(9)?,incarnation:r.get(10)?,revision:r.get::<_,i64>(11)? as u64 })
    }).optional()?;
    if let Some(checkpoint) = &result {
        validate(principal, &checkpoint.job, &checkpoint.proof)?;
        if checkpoint.incarnation.len() != 16 || checkpoint.revision > i64::MAX as u64 {
            return Err(TransferIntentError::Invalid);
        }
    }
    Ok(result)
}

impl Storage {
    /// SDK-owned artifact directory for one principal. Names derive only from the database
    /// identity and validated principal; callers must never store authorization material here.
    pub fn preparation_directory(
        &self,
        principal: &str,
    ) -> Result<std::path::PathBuf, StorageError> {
        check_principal(principal)?;
        let conn = self.lock()?;
        let id: String = conn.query_row(
            "SELECT value FROM meta WHERE key='search_database_id'",
            [],
            |r| r.get(0),
        )?;
        if id.is_empty() || !id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
            return Err(StorageError::Corrupt("invalid artifact namespace".into()));
        }
        let root = match conn.path().filter(|p| !p.is_empty()) {
            Some(path) => std::path::PathBuf::from(format!("{path}.preparations")),
            None => std::env::temp_dir().join(format!("tinkle-preparations-{id}")),
        };
        Ok(root.join(principal.replace(':', "-")))
    }

    /// Atomically create or compare an immutable transfer intent BEFORE any RPC.
    /// Identical registrations return the retained checkpoint without resetting it.
    /// Existing legacy uploads are adopted only if their complete canonical proof matches.
    pub fn create_or_compare_file_transfer_intent(
        &self,
        principal: &str,
        initial: &FileTransferJob,
        proof: &str,
    ) -> Result<TransferIntentCheckpoint, TransferIntentError> {
        self.claim_intent(principal, initial, proof, None)
    }

    /// Preparation publication also compares the pre-work durable principal wipe fence.
    pub fn create_or_compare_preparation_intent(
        &self,
        principal: &str,
        incarnation: u64,
        initial: &FileTransferJob,
        proof: &str,
    ) -> Result<TransferIntentCheckpoint, TransferIntentError> {
        if !initial.job_id.starts_with("prepare:") {
            return Err(TransferIntentError::Invalid);
        }
        self.claim_intent(principal, initial, proof, Some(incarnation))
    }

    /// One claim transaction serves ordinary transfers and fenced preparation parents.
    fn claim_intent(
        &self,
        principal: &str,
        initial: &FileTransferJob,
        proof: &str,
        incarnation: Option<u64>,
    ) -> Result<TransferIntentCheckpoint, TransferIntentError> {
        validate(principal, initial, proof)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(expected) = incarnation {
            if Self::cache_incarnation(&tx, principal)? != expected {
                return Err(TransferIntentError::Changed);
            }
        }
        tx.execute_batch(SCHEMA)?;
        if initial.job_id.starts_with("prepare:") {
            let count:i64=tx.query_row("SELECT count(*) FROM file_transfers WHERE principal=?1 AND job_id LIKE 'prepare:%' AND job_id!=?2",params![principal,initial.job_id],|r|r.get(0))?;
            if count >= 256 {
                return Err(TransferIntentError::Invalid);
            }
        }
        tx.execute("INSERT INTO file_transfers(principal,job_id,kind,file_scope,file_id,upload_id,state,session_json,temp_path,resume_offset,updated_at)
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11) ON CONFLICT(principal,job_id) DO NOTHING",
            params![principal,initial.job_id,initial.kind,initial.file_scope,initial.file_id,initial.upload_id,initial.state,initial.session_json,initial.temp_path,initial.resume_offset,initial.updated_at])?;
        let compatible: bool = tx.query_row("SELECT kind=?3 AND file_scope=?4 AND (?3!='upload' OR session_json=?5) AND (?3!='download' OR file_id=?6)
            FROM file_transfers WHERE principal=?1 AND job_id=?2",params![principal,initial.job_id,initial.kind,initial.file_scope,proof,initial.file_id],|r|r.get(0))?;
        if !compatible {
            return Err(TransferIntentError::Conflict);
        }
        tx.execute("INSERT INTO file_transfer_intents_v1(principal,job_id,proof) VALUES(?1,?2,?3) ON CONFLICT(principal,job_id) DO NOTHING",params![principal,initial.job_id,proof])?;
        let retained =
            read(&tx, principal, &initial.job_id)?.ok_or(TransferIntentError::Changed)?;
        if retained.proof != proof {
            return Err(TransferIntentError::Conflict);
        }
        tx.commit()?;
        Ok(retained)
    }

    /// Load registered state, never derive permission or remote acknowledgements from it.
    pub fn file_transfer_intent(
        &self,
        principal: &str,
        id: &str,
    ) -> Result<Option<TransferIntentCheckpoint>, TransferIntentError> {
        check_principal(principal)?;
        check_job_id(id)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(SCHEMA)?;
        let result = read(&tx, principal, id)?;
        tx.commit()?;
        Ok(result)
    }

    /// Conditional row update: match immutable proof/incarnation AND exact state/revision.
    /// No missing/deleted row is recreated. Legacy unconditional upserts cannot mutate
    /// registered rows because only this transaction arms their update guard.
    pub fn compare_and_swap_file_transfer(
        &self,
        principal: &str,
        expected: &TransferIntentCheckpoint,
        next: &FileTransferJob,
    ) -> Result<Option<TransferIntentCheckpoint>, TransferIntentError> {
        validate(principal, next, &expected.proof)?;
        let old = &expected.job;
        if next.job_id != old.job_id
            || next.kind != old.kind
            || next.file_scope != old.file_scope
            || rank(&next.state) < rank(&old.state)
            || (terminal(&old.state) && next.state != old.state)
            || (old.file_id > 0 && next.file_id != old.file_id)
            || (!old.upload_id.is_empty() && next.upload_id != old.upload_id)
            || (old.kind == "upload" && next.resume_offset < old.resume_offset)
        {
            return Err(TransferIntentError::Conflict);
        }
        let revision = i64::try_from(expected.revision)
            .ok()
            .filter(|n| *n < i64::MAX)
            .ok_or(TransferIntentError::Invalid)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if tx.execute("UPDATE file_transfer_intents_v1 SET writing_revision=revision WHERE principal=?1 AND job_id=?2 AND proof=?3 AND incarnation=?4 AND revision=?5",
            params![principal,old.job_id,expected.proof,expected.incarnation,revision])? !=1 { return Ok(None); }
        let changed=tx.execute("UPDATE file_transfers SET file_id=?3,upload_id=?4,state=?5,session_json=?6,temp_path=?7,resume_offset=?8,updated_at=?9
            WHERE principal=?1 AND job_id=?2 AND state=?10 AND session_json=?11 AND file_id=?12 AND upload_id=?13 AND temp_path=?14 AND resume_offset=?15 AND updated_at=?16 AND kind=?17 AND file_scope=?18",
            params![principal,next.job_id,next.file_id,next.upload_id,next.state,next.session_json,next.temp_path,next.resume_offset,next.updated_at,
                old.state,old.session_json,old.file_id,old.upload_id,old.temp_path,old.resume_offset,old.updated_at,old.kind,old.file_scope])?;
        if changed != 1 {
            return Ok(None);
        }
        let result = read(&tx, principal, &next.job_id)?;
        tx.commit()?;
        Ok(result)
    }

    /// Merge diagnostic upload checkpoints monotonically within the SAME registered
    /// incarnation. Concurrent identical workers may advance; terminal state wins.
    /// Server snapshots, never these local counters, decide which parts to send.
    pub fn advance_upload_intent(
        &self,
        principal: &str,
        original: &TransferIntentCheckpoint,
        requested: &FileTransferJob,
    ) -> Result<TransferIntentCheckpoint, TransferIntentError> {
        validate(principal, requested, &original.proof)?;
        if requested.kind != "upload"
            || requested.job_id != original.job.job_id
            || requested.file_scope != original.job.file_scope
        {
            return Err(TransferIntentError::Conflict);
        }
        for _ in 0..16 {
            let current = self
                .file_transfer_intent(principal, &requested.job_id)?
                .ok_or(TransferIntentError::Changed)?;
            if current.proof != original.proof || current.incarnation != original.incarnation {
                return Err(TransferIntentError::Conflict);
            }
            if !current.job.upload_id.is_empty()
                && !requested.upload_id.is_empty()
                && current.job.upload_id != requested.upload_id
            {
                return Err(TransferIntentError::Conflict);
            }
            if rank(&requested.state) < rank(&current.job.state) {
                return Ok(current);
            }
            if terminal(&current.job.state) && requested.state != current.job.state {
                return Err(TransferIntentError::Conflict);
            }
            let mut next = requested.clone();
            if next.upload_id.is_empty() {
                next.upload_id = current.job.upload_id.clone();
            }
            next.resume_offset = next.resume_offset.max(current.job.resume_offset);
            next.updated_at = next.updated_at.max(current.job.updated_at);
            if let Some(result) = self.compare_and_swap_file_transfer(principal, &current, &next)? {
                return Ok(result);
            }
        }
        Err(TransferIntentError::Changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    fn job(proof: &str) -> FileTransferJob {
        FileTransferJob {
            job_id: "upload:key".into(),
            kind: "upload".into(),
            file_scope: String::new(),
            file_id: 0,
            upload_id: String::new(),
            state: "begin".into(),
            session_json: proof.into(),
            temp_path: String::new(),
            resume_offset: 0,
            updated_at: 1,
        }
    }

    /// Both independent connections observe absence, then race the actual DB claim.
    #[test]
    fn two_connection_claim_preserves_one_canonical_proof_across_restart() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cache.sqlite");
        let a = Storage::open(path.to_str().unwrap()).unwrap();
        let b = Storage::open(path.to_str().unwrap()).unwrap();
        assert!(a
            .get_file_transfer("user:1", "upload:key")
            .unwrap()
            .is_none());
        assert!(b
            .get_file_transfer("user:1", "upload:key")
            .unwrap()
            .is_none());
        let gate = Arc::new(Barrier::new(3));
        let mut tasks = Vec::new();
        for (storage, proof) in [(a, "canonical-a"), (b, "canonical-b")] {
            let gate = gate.clone();
            tasks.push(std::thread::spawn(move || {
                gate.wait();
                storage.create_or_compare_file_transfer_intent("user:1", &job(proof), proof)
            }));
        }
        gate.wait();
        let results: Vec<_> = tasks.into_iter().map(|t| t.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Err(TransferIntentError::Conflict)))
                .count(),
            1
        );
        let winner = results.into_iter().find_map(Result::ok).unwrap();
        let reopened = Storage::open(path.to_str().unwrap()).unwrap();
        let retained = reopened
            .create_or_compare_file_transfer_intent("user:1", &job(&winner.proof), &winner.proof)
            .unwrap();
        assert_eq!(retained.proof, winner.proof);
        assert_eq!(retained.incarnation, winner.incarnation);
        let mut wrong = job("different");
        wrong.state = "active".into();
        assert!(
            reopened.upsert_file_transfer("user:1", &wrong).is_err(),
            "legacy upsert bypassed immutable intent"
        );
        assert_eq!(
            reopened
                .get_file_transfer("user:1", "upload:key")
                .unwrap()
                .unwrap()
                .session_json,
            winner.proof
        );
    }

    /// CAS compares state/revision, identical registration never resets completion,
    /// and late same-proof upload progress cannot downgrade terminal state.
    #[test]
    fn same_proof_cas_and_terminal_merge_are_monotonic() {
        let storage = Storage::open_in_memory().unwrap();
        let initial = job("proof");
        let first = storage
            .create_or_compare_file_transfer_intent("user:1", &initial, "proof")
            .unwrap();
        let other = storage
            .create_or_compare_file_transfer_intent("user:1", &initial, "proof")
            .unwrap();
        let mut active = initial.clone();
        active.state = "active".into();
        active.upload_id = "remote".into();
        active.resume_offset = 1;
        let advanced = storage
            .compare_and_swap_file_transfer("user:1", &first, &active)
            .unwrap()
            .unwrap();
        assert!(storage
            .compare_and_swap_file_transfer("user:1", &other, &active)
            .unwrap()
            .is_none());
        let mut complete = active.clone();
        complete.state = "complete".into();
        complete.file_id = 9;
        let completed = storage
            .advance_upload_intent("user:1", &advanced, &complete)
            .unwrap();
        let late = storage
            .advance_upload_intent("user:1", &other, &active)
            .unwrap();
        assert_eq!(late.job.state, "complete");
        assert_eq!(late.revision, completed.revision);
        let repeated = storage
            .create_or_compare_file_transfer_intent("user:1", &initial, "proof")
            .unwrap();
        assert_eq!(repeated.job.state, "complete");
        assert_eq!(repeated.revision, completed.revision);
        assert!(storage.upsert_file_transfer("user:1", &active).is_err());
    }

    /// Ordinary principal cleanup also removes its sidecar; old tokens cannot
    /// cross delete/recreate ABA even if the new canonical proof is identical.
    #[test]
    fn deleted_checkpoint_is_never_recreated_by_a_late_worker() {
        let storage = Storage::open_in_memory().unwrap();
        let initial = job("proof");
        let old = storage
            .create_or_compare_file_transfer_intent("user:1", &initial, "proof")
            .unwrap();
        storage.clear_principal("user:1").unwrap();
        let mut active = initial.clone();
        active.state = "active".into();
        assert!(storage
            .advance_upload_intent("user:1", &old, &active)
            .is_err());
        let fresh = storage
            .create_or_compare_file_transfer_intent("user:1", &initial, "proof")
            .unwrap();
        assert_ne!(fresh.incarnation, old.incarnation);
        assert!(storage
            .compare_and_swap_file_transfer("user:1", &old, &active)
            .unwrap()
            .is_none());
        assert!(storage
            .advance_upload_intent("user:1", &old, &active)
            .is_err());
    }

    /// A retained preparation publisher cannot recreate its manifest after an external wipe.
    #[test]
    fn preparation_claim_compares_the_durable_principal_incarnation() {
        let s = Storage::open_in_memory().unwrap();
        let incarnation = s.principal_cache_incarnation("user:1").unwrap();
        let mut initial = job("manifest");
        initial.job_id = "prepare:key".into();
        s.clear_principal("user:1").unwrap();
        assert!(matches!(
            s.create_or_compare_preparation_intent("user:1", incarnation, &initial, "manifest"),
            Err(TransferIntentError::Changed)
        ));
        let current = s.principal_cache_incarnation("user:1").unwrap();
        assert!(s
            .create_or_compare_preparation_intent("user:1", current, &initial, "manifest")
            .is_ok());
        assert!(s
            .file_transfer_intent("user:2", "prepare:key")
            .unwrap()
            .is_none());
    }
}
