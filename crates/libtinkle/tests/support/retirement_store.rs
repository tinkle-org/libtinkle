//! Test-only private-file namespace for cross-process CAS and tombstone checks.
//! This is not a platform secure-store implementation or encryption acceptance.

use async_trait::async_trait;
use libtinkle::{
    EnrollmentNamespaceSnapshot, SecretBlob, SecureEnrollmentStore, SecureStoreError, SecureWrite,
};
use serde_json::{json, Value};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
};

/// Each instance/process shares the same durable lock, incarnation and revision.
pub struct DiskStore {
    pub path: PathBuf,
    events: Option<std::sync::Arc<std::sync::Mutex<Vec<&'static str>>>>,
    restore_permits: Arc<Mutex<DiskRestoreLocks>>,
}

/// OS-owned install locks release on process death, never by unchecked expiry.
#[derive(Default)]
struct DiskRestoreLocks {
    held: std::collections::HashMap<String, File>,
    cancelled: std::collections::HashSet<String>,
}

impl DiskStore {
    /// Reopen an existing namespace in another process, without in-memory state.
    pub fn new(path: PathBuf) -> Self {
        // Cancellation must survive adapter replacement until this process exits.
        static PERMITS: OnceLock<
            Mutex<std::collections::HashMap<PathBuf, Arc<Mutex<DiskRestoreLocks>>>>,
        > = OnceLock::new();
        let restore_permits = PERMITS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .entry(path.clone())
            .or_default()
            .clone();
        Self {
            path,
            events: None,
            restore_permits,
        }
    }

    /// Report acknowledged writes to the parent gateway's ordering assertions.
    pub fn with_events(
        path: PathBuf,
        events: std::sync::Arc<std::sync::Mutex<Vec<&'static str>>>,
    ) -> Self {
        Self {
            events: Some(events),
            ..Self::new(path)
        }
    }
    /// Compare and publish under an OS lock, fsyncing both file and rename directory.
    fn transaction<T>(
        &self,
        operation: impl FnOnce(&mut Value) -> Result<T, SecureStoreError>,
    ) -> Result<T, SecureStoreError> {
        // Mutation may reject rather than wait while another process installs.
        let _install = self.install_lock()?;
        self.transaction_unpinned(operation)
    }

    /// Acquire nonblocking exclusion shared by every install and namespace operation.
    fn install_lock(&self) -> Result<File, SecureStoreError> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        options.mode(0o600);
        let install = options
            .open(self.path.with_extension("install"))
            .map_err(|_| SecureStoreError)?;
        install.try_lock().map_err(|_| SecureStoreError)?;
        Ok(install)
    }

    /// The caller already owns install exclusion; the record lock serializes file publication.
    fn transaction_unpinned<T>(
        &self,
        operation: impl FnOnce(&mut Value) -> Result<T, SecureStoreError>,
    ) -> Result<T, SecureStoreError> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        options.mode(0o600);
        let lock = options
            .open(self.path.with_extension("lock"))
            .map_err(|_| SecureStoreError)?;
        lock.lock().map_err(|_| SecureStoreError)?;
        let mut state = match std::fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&zeroize::Zeroizing::new(bytes))
                .map_err(|_| SecureStoreError)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => json!({
                "incarnation": format!("private-fixture-{:032x}", rand::random::<u128>()),
                "revision": 0u64, "marker": false, "active": null, "pending": [], "operations": {},
            }),
            Err(_) => return Err(SecureStoreError),
        };
        let result = operation(&mut state)?;
        let bytes =
            zeroize::Zeroizing::new(serde_json::to_vec(&state).map_err(|_| SecureStoreError)?);
        let temporary = self.path.with_extension("next");
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&temporary).map_err(|_| SecureStoreError)?;
        file.write_all(&bytes).map_err(|_| SecureStoreError)?;
        file.sync_all().map_err(|_| SecureStoreError)?;
        std::fs::rename(temporary, &self.path).map_err(|_| SecureStoreError)?;
        File::open(self.path.parent().ok_or(SecureStoreError)?)
            .map_err(|_| SecureStoreError)?
            .sync_all()
            .map_err(|_| SecureStoreError)?;
        Ok(result)
    }
}

/// Stable incarnation plus an increasing revision, compared without normalization.
fn revision(state: &Value) -> Result<String, SecureStoreError> {
    Ok(format!(
        "{}:{}",
        state["incarnation"].as_str().ok_or(SecureStoreError)?,
        state["revision"].as_u64().ok_or(SecureStoreError)?
    ))
}

/// Adopt one opaque private-file value without exposing it to diagnostics.
fn record(value: &Value) -> Result<Option<SecretBlob>, SecureStoreError> {
    if value.is_null() {
        return Ok(None);
    }
    serde_json::from_value::<Vec<u8>>(value.clone())
        .map(SecretBlob::new)
        .map(Some)
        .map_err(|_| SecureStoreError)
}

#[async_trait]
impl SecureEnrollmentStore for DiskStore {
    async fn begin_restore(
        &self,
        incarnation: &str,
        expected: &str,
        active: &SecretBlob,
        id: &str,
    ) -> Result<bool, SecureStoreError> {
        let install = self.install_lock()?;
        let valid = self.transaction_unpinned(|state| {
            Ok(state["incarnation"].as_str() == Some(incarnation)
                && revision(state)? == expected
                && state["marker"] == false
                && record(&state["active"])?
                    .is_some_and(|current| current.expose_secret() == active.expose_secret()))
        })?;
        if !valid {
            return Ok(false);
        }
        let mut permits = self.restore_permits.lock().unwrap();
        if permits.cancelled.contains(id) {
            return Ok(false);
        }
        permits.held.insert(id.into(), install);
        Ok(true)
    }
    async fn end_restore(&self, id: &str) -> Result<(), SecureStoreError> {
        let mut permits = self.restore_permits.lock().unwrap();
        permits.cancelled.insert(id.into());
        permits.held.remove(id);
        Ok(())
    }
    async fn namespace_snapshot(
        &self,
    ) -> Result<Option<EnrollmentNamespaceSnapshot>, SecureStoreError> {
        self.transaction(|state| {
            Ok(Some(EnrollmentNamespaceSnapshot {
                incarnation: state["incarnation"]
                    .as_str()
                    .ok_or(SecureStoreError)?
                    .into(),
                revision: revision(state)?,
                cleanup_pending: state["marker"].as_bool().ok_or(SecureStoreError)?,
                active: record(&state["active"])?,
                has_pending: !state["pending"]
                    .as_array()
                    .ok_or(SecureStoreError)?
                    .is_empty(),
            }))
        })
    }
    async fn commit_current(
        &self,
        expected: &str,
        kind: SecureWrite,
        id: &str,
        blob: &SecretBlob,
    ) -> Result<Option<String>, SecureStoreError> {
        let result = self.transaction(|state| {
            if revision(state)? != expected || state["marker"] == true {
                return Ok(None);
            }
            let operations = state["operations"]
                .as_object_mut()
                .ok_or(SecureStoreError)?;
            if (kind == SecureWrite::Operation) == operations.contains_key(id) {
                return Err(SecureStoreError);
            }
            let value = serde_json::to_value(blob.expose_secret()).map_err(|_| SecureStoreError)?;
            operations.insert(id.into(), value.clone());
            if kind == SecureWrite::Active {
                state["active"] = value;
            }
            let pending = state["pending"].as_array_mut().ok_or(SecureStoreError)?;
            pending.retain(|value| value.as_str() != Some(id));
            if kind != SecureWrite::Active {
                pending.push(Value::String(id.into()));
            }
            state["revision"] =
                Value::from(state["revision"].as_u64().ok_or(SecureStoreError)? + 1);
            Ok(Some(revision(state)?))
        })?;
        if result.is_some() {
            if let Some(events) = &self.events {
                events.lock().unwrap().push(match kind {
                    SecureWrite::Operation => "operation-durable",
                    SecureWrite::Candidate => "candidate-durable",
                    SecureWrite::Active => "active-durable",
                });
            }
        }
        Ok(result)
    }
    async fn begin_clear(&self, expected: &str) -> Result<bool, SecureStoreError> {
        self.transaction(|state| {
            if revision(state)? != expected {
                return Ok(false);
            }
            state["marker"] = Value::Bool(true);
            Ok(true)
        })
    }
    async fn finish_clear(&self, expected: &str) -> Result<bool, SecureStoreError> {
        self.transaction(|state| {
            if revision(state)? != expected || state["marker"] != true {
                return Ok(false);
            }
            state["operations"] = json!({});
            state["active"] = Value::Null;
            state["pending"] = json!([]);
            Ok(true)
        })
    }
    async fn clear(&self) -> Result<(), SecureStoreError> {
        Err(SecureStoreError)
    }
    async fn commit(
        &self,
        _: SecureWrite,
        _: &str,
        _: &SecretBlob,
    ) -> Result<(), SecureStoreError> {
        Err(SecureStoreError)
    }
    async fn load_operation(&self, id: &str) -> Result<Option<SecretBlob>, SecureStoreError> {
        self.transaction(|state| {
            if state["marker"] == true {
                return Err(SecureStoreError);
            }
            record(&state["operations"][id])
        })
    }
    async fn load_active(&self) -> Result<Option<SecretBlob>, SecureStoreError> {
        self.transaction(|state| {
            if state["marker"] == true {
                return Err(SecureStoreError);
            }
            record(&state["active"])
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn restore_release_and_cancellation_are_shared_across_adapters() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("namespace.json");
        let first = DiskStore::new(path.clone());
        let initial = first.namespace_snapshot().await.unwrap().unwrap();
        let active = SecretBlob::new(vec![1, 2, 3]);
        let pending = first
            .commit_current(
                &initial.revision,
                SecureWrite::Operation,
                "operation",
                &active,
            )
            .await
            .unwrap()
            .unwrap();
        let current = first
            .commit_current(&pending, SecureWrite::Active, "operation", &active)
            .await
            .unwrap()
            .unwrap();
        assert!(first
            .begin_restore(&initial.incarnation, &current, &active, "install")
            .await
            .unwrap());
        let peer = DiskStore::with_events(path.clone(), Default::default());
        assert!(peer.begin_clear(&current).await.is_err());
        let unrelated = DiskStore::new(directory.path().join("other.json"));
        unrelated.end_restore("install").await.unwrap();
        assert!(peer.begin_clear(&current).await.is_err());
        peer.end_restore("unrelated").await.unwrap();
        assert!(peer.begin_clear(&current).await.is_err());
        peer.end_restore("install").await.unwrap();
        peer.end_restore("install").await.unwrap();
        assert_eq!(
            first.namespace_snapshot().await.unwrap().unwrap().revision,
            current
        );

        peer.end_restore("late-install").await.unwrap();
        drop(first);
        drop(peer);
        let reopened = DiskStore::new(path);
        for id in ["install", "late-install"] {
            assert!(!reopened
                .begin_restore(&initial.incarnation, &current, &active, id)
                .await
                .unwrap());
        }
        assert!(reopened.begin_clear(&current).await.unwrap());
        assert!(reopened.finish_clear(&current).await.unwrap());
    }
}
