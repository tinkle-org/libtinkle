//! Retained local cleanup; dropping a waiter never abandons sign-out work.

use crate::{Client, TinkleError};
use futures_util::future::{BoxFuture, FutureExt, Shared};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct CleanupStore {
    store: Arc<dyn crate::SecureEnrollmentStore>,
    revision: Option<String>,
}

/// Shared completion of one independently running cleanup task.
pub(crate) type CleanupAttempt = Shared<BoxFuture<'static, Result<bool, TinkleError>>>;

/// The first cause is retained across retries; observation never confirms Logout.
#[derive(Clone)]
enum CleanupCause {
    Logout,
    Retired { principal: String },
}

/// A joinable attempt and progress retained across local-storage retries.
#[derive(Default)]
pub(crate) struct LogoutCleanup {
    status: Mutex<crate::SessionCleanup>,
    attempt: Mutex<Option<CleanupAttempt>>,
    cause: Mutex<Option<CleanupCause>>,
    progress: tokio::sync::Mutex<CleanupProgress>,
    /// Only for a host-provisioned Returning source that never bound a store.
    clear_only: Mutex<Option<CleanupStore>>,
}

/// Preserve the original cache scope and never repeat the remote mutation.
#[derive(Default)]
struct CleanupProgress {
    principal: Option<String>,
    confirmed: bool,
    cache_cleared: bool,
    cleared_stores: Vec<usize>,
}

impl LogoutCleanup {
    /// Query retained progress even when no host currently awaits completion.
    pub(crate) fn status(&self) -> crate::SessionCleanup {
        *self.status.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Spawn before the first await; all concurrent callers join the same attempt.
    pub(crate) fn join(
        &self,
        client: Client,
    ) -> Shared<BoxFuture<'static, Result<bool, TinkleError>>> {
        self.join_cause(client, CleanupCause::Logout)
    }

    /// Retire locally from exact negative evidence, never issuing remote Logout.
    pub(crate) fn join_retirement(&self, client: Client, principal: String) -> CleanupAttempt {
        self.join_cause(client, CleanupCause::Retired { principal })
    }

    /// Schedule or rejoin with the original operation's immutable cause.
    fn join_cause(&self, client: Client, requested: CleanupCause) -> CleanupAttempt {
        let mut attempt = self.attempt.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(current) = attempt.as_ref() {
            // A detached task can finish without anyone polling the shared join
            // future. Its retained status, rather than peek alone, permits retry.
            if self.status() != crate::SessionCleanup::Failed
                && !matches!(current.peek(), Some(Err(_)))
            {
                return current.clone();
            }
        }
        *self.status.lock().unwrap_or_else(|e| e.into_inner()) = crate::SessionCleanup::Pending;
        let cause = self
            .cause
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert(requested)
            .clone();
        // Pair the first cleanup cause with the irreversible fence. Automatic
        // retirement must not expose a fenced client before retaining its cause.
        client.begin_signed_out();
        let task = tokio::spawn(async move {
            let result = client.finish_logout(&cause).await;
            *client
                .logout_cleanup
                .status
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = if result.is_ok() {
                crate::SessionCleanup::Complete
            } else {
                crate::SessionCleanup::Failed
            };
            result
        });
        let next = async move {
            task.await
                .map_err(|_| TinkleError::CursorStore("retained sign-out task failed".into()))?
        }
        .boxed()
        .shared();
        *attempt = Some(next.clone());
        next
    }
}

impl Client {
    /// Supply missing logout cleanup only; this cannot grant enrollment authority.
    pub(crate) async fn bind_cleanup_store(
        &self,
        store: Arc<dyn crate::SecureEnrollmentStore>,
    ) -> Result<(), TinkleError> {
        let _flow = self.enrollment_lock.lock().await;
        let (needs_store, stores) = self.authorization.stores();
        if !self.is_signed_out() || !needs_store || !stores.is_empty() {
            return Err(TinkleError::SignedOut);
        }
        if self
            .logout_cleanup
            .clear_only
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
        {
            return Err(TinkleError::SecureStore);
        }
        let snapshot = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            store.namespace_snapshot(),
        )
        .await
        .map_err(|_| TinkleError::SecureStore)?
        .map_err(|_| TinkleError::SecureStore)?;
        if snapshot.as_ref().is_some_and(|snapshot| {
            snapshot.incarnation.is_empty()
                || snapshot.incarnation.len() > 256
                || snapshot.revision.is_empty()
                || snapshot.revision.len() > 256
        }) {
            return Err(TinkleError::SecureStore);
        }
        *self
            .logout_cleanup
            .clear_only
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(CleanupStore {
            store,
            revision: snapshot.map(|snapshot| snapshot.revision),
        });
        Ok(())
    }

    /// Complete only unfinished local steps, keeping the first transport outcome.
    async fn finish_logout(&self, cause: &CleanupCause) -> Result<bool, TinkleError> {
        self.pause_retirement_schedule().await;
        let mut progress = self.logout_cleanup.progress.lock().await;
        if progress.principal.is_none() {
            let (principal, confirmed) = self
                .retire_signed_out_transport(matches!(cause, CleanupCause::Logout))
                .await;
            let principal = match cause {
                CleanupCause::Logout => principal,
                CleanupCause::Retired { principal } => principal.clone(),
            };
            progress.principal = Some(principal);
            progress.confirmed |= confirmed;
        }
        let principal = progress
            .principal
            .as_deref()
            .expect("captured logout scope");
        let cache = if progress.cache_cleared {
            Ok(())
        } else if principal != crate::sync::UNKNOWN_PRINCIPAL {
            self.logout_wipe_and_reset(principal).await
        } else {
            self.reset_session_for_logout().await;
            Ok(())
        };
        progress.cache_cleared |= cache.is_ok();
        // Drain pre-fence persistence before invalidating its namespace.
        let _flow = self.enrollment_lock.lock().await;
        let (needs_store, bound_stores) = self.authorization.stores();
        let mut stores: Vec<_> = bound_stores
            .into_iter()
            .map(|store| CleanupStore {
                revision: self.authorization.store_revision(store.as_ref()),
                store,
            })
            .collect();
        if let Some(store) = self
            .logout_cleanup
            .clear_only
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            stores.push(store);
        }
        let mut failed = needs_store && stores.is_empty();
        for (index, binding) in stores.iter().enumerate() {
            if progress.cleared_stores.contains(&index) {
                continue;
            }
            let store = &binding.store;
            let clear = async {
                if let Some(revision) = &binding.revision {
                    if !store.begin_clear(revision).await? || !store.finish_clear(revision).await? {
                        return Err(crate::SecureStoreError);
                    }
                    Ok(())
                } else {
                    store.clear().await
                }
            };
            match tokio::time::timeout(std::time::Duration::from_secs(10), clear).await {
                Ok(Ok(())) => progress.cleared_stores.push(index),
                _ => failed = true,
            }
        }
        if failed {
            return Err(TinkleError::SecureStore);
        }
        cache?;
        Ok(progress.confirmed)
    }
}
