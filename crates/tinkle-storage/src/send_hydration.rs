//! Durable completion for body hydration tied to the exact accepted send receipt.
use super::*;

pub(super) const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS send_hydration_done(
 principal TEXT NOT NULL, source TEXT NOT NULL, job_id TEXT NOT NULL, revision INTEGER NOT NULL,
 PRIMARY KEY(principal,source,job_id), FOREIGN KEY(principal,source,job_id)
 REFERENCES outgoing_jobs(principal,source,job_id) ON DELETE CASCADE);";

impl Storage {
    /// A successful receipt's local revision is durable hydration demand until acknowledged.
    pub fn send_hydration_due(
        &self,
        principal: &str,
        source: &str,
    ) -> Result<Vec<String>, StorageError> {
        check_principal(principal)?;
        let conn = self.lock()?;
        let mut stmt = conn.prepare("SELECT j.job_id FROM outgoing_jobs j WHERE j.principal=?1 AND j.source=?2 AND j.state=16
            AND j.method IN ('/tinkle.v1.MessagesService/SendMedia','/tinkle.v1.MessagesService/SendAlbum')
            AND NOT EXISTS(SELECT 1 FROM send_hydration_done d WHERE d.principal=j.principal AND d.source=j.source
                AND d.job_id=j.job_id AND d.revision=j.revision) ORDER BY j.job_id LIMIT 256")?;
        let rows = stmt.query_map(params![principal, source], |r| r.get(0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// A withdrawn/withheld receipt or a retired target needs no body write.
    pub fn complete_send_hydration(
        &self,
        principal: &str,
        job: &OutgoingJob,
    ) -> Result<(), StorageError> {
        check_principal(principal)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        Self::finish_send_hydration(&tx, principal, job)?;
        tx.commit()?;
        Ok(())
    }

    /// Refuse stale body work before any message or completion marker becomes visible.
    pub(super) fn finish_send_hydration(
        tx: &Transaction<'_>,
        principal: &str,
        job: &OutgoingJob,
    ) -> Result<(), StorageError> {
        if job.state != OutgoingState::ActionSucceeded
            || job.cache_incarnation != Self::cache_incarnation(tx, principal)?
        {
            return Err(StorageError::CacheScopeChanged);
        }
        let retained: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM outgoing_jobs WHERE principal=?1 AND source=?2
            AND job_id=?3 AND state=16 AND revision=?4 AND response=?5 AND cache_incarnation=?6 AND cache_generation=?7 AND target_key=?8 AND method=?9 AND payload=?10)",
            params![principal,job.source,job.job_id,job.revision,job.response,job.cache_incarnation as i64,job.cache_generation as i64,job.target_key,job.method,job.payload], |r| r.get(0))?;
        if !retained {
            return Err(StorageError::IntentConflict);
        }
        tx.execute("INSERT INTO send_hydration_done VALUES(?1,?2,?3,?4)
            ON CONFLICT(principal,source,job_id) DO UPDATE SET revision=excluded.revision WHERE revision<>excluded.revision",
            params![principal,job.source,job.job_id,job.revision])?;
        Ok(())
    }
}
