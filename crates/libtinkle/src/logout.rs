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
    /// Clock failure disables remote work but cannot bypass local cleanup.
    Logout {
        budget: Option<syntra_runtime::elapsed::ElapsedDeadline>,
    },
    Retired {
        principal: String,
    },
}

/// A joinable attempt and progress retained across local-storage retries.
#[derive(Default)]
pub(crate) struct LogoutCleanup {
    status: Mutex<crate::SessionCleanup>,
    attempt: Mutex<Option<CleanupAttempt>>,
    cause: Mutex<Option<CleanupCause>>,
    progress: tokio::sync::Mutex<CleanupProgress>,
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
        let budget = client.call_budget(3_000).ok();
        self.join_cause(client, CleanupCause::Logout { budget })
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
        self.authorization.bind_clear_only(
            store,
            snapshot
                .as_ref()
                .map(|snapshot| snapshot.incarnation.clone()),
            snapshot.map(|snapshot| snapshot.revision),
        )
    }

    /// Complete only unfinished local steps, keeping the first transport outcome.
    async fn finish_logout(&self, cause: &CleanupCause) -> Result<bool, TinkleError> {
        self.pause_retirement_schedule().await;
        let mut progress = self.logout_cleanup.progress.lock().await;
        if progress.principal.is_none() {
            let budget = match cause {
                CleanupCause::Logout { budget } => budget.as_ref(),
                CleanupCause::Retired { .. } => None,
            };
            let (principal, confirmed) = self.retire_signed_out_transport(budget).await;
            let principal = match cause {
                CleanupCause::Logout { .. } => principal,
                CleanupCause::Retired { principal } => principal.clone(),
            };
            progress.principal = Some(principal);
            progress.confirmed |= confirmed;
        }
        // Drain pre-fence persistence before invalidating its namespace.
        let _flow = self.enrollment_lock.lock().await;
        // An unresolved conditional write still needs its exact receipt. Keep
        // cleanup retryable; never clear/adopt an inferred or successor revision.
        let pending_write = self.authorization.has_pending_store_write();
        let (needs_store, bound_stores) = self.authorization.stores();
        let stores: Vec<_> = bound_stores
            .into_iter()
            .map(|store| CleanupStore {
                revision: self.authorization.store_revision(store.as_ref()),
                store,
            })
            .collect();
        let mut failed = pending_write || (needs_store && stores.is_empty());
        if !pending_write {
            for (index, binding) in stores.iter().enumerate() {
                if progress.cleared_stores.contains(&index) {
                    continue;
                }
                let store = &binding.store;
                let clear = async {
                    if let Some(revision) = &binding.revision {
                        if !store.begin_clear(revision).await?
                            || !store.finish_clear(revision).await?
                        {
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
        }
        drop(_flow);
        // Credential invalidation must not queue behind cache locks or SQLite.
        // Bound the attempt so a later caller can retry unfinished local work.
        let cache = if !progress.cache_cleared {
            let principal = progress
                .principal
                .as_deref()
                .expect("captured logout scope");
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                if principal != crate::sync::UNKNOWN_PRINCIPAL {
                    self.logout_wipe_and_reset(principal).await
                } else {
                    self.reset_session_for_logout().await;
                    Ok(())
                }
            })
            .await
            .map_err(|_| TinkleError::CursorStore("sign-out cache cleanup timed out".into()))
            .and_then(|result| result)
        } else {
            Ok(())
        };
        progress.cache_cleared |= cache.is_ok();
        if failed {
            return Err(TinkleError::SecureStore);
        }
        cache?;
        Ok(progress.confirmed)
    }
}

#[cfg(test)]
mod review_tests {
    use super::*;
    use crate::enrollment::review_tests::{client, ReviewStore};
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn pending_store_write_defers_credentials_but_not_cache_cleanup() {
        for known_principal in [false, true] {
            let storage = Arc::new(crate::Storage::open(":memory:").unwrap());
            let base = client(false);
            let client = Client::new_sqlite(
                base.config().clone(),
                crate::TrustSource::Bootstrap(base.enrollment_trust().await),
                storage.clone(),
            );
            if known_principal {
                client.set_principal_user_id(42).await;
                client
                    .ingest_messages(
                        "user:42",
                        "target:user:42",
                        crate::client::IngestBatch {
                            messages: &[],
                            users: &[],
                            chats: &[tinkle_schema::proto::Chat {
                                chat_id: 42,
                                ..Default::default()
                            }],
                            exclude: &Default::default(),
                        },
                        client.reconcile_epoch(),
                    )
                    .await
                    .unwrap();
                assert!(storage.get_chat("user:42", 42).unwrap().is_some());
            }
            client.set_watch(vec![7]).await.unwrap();
            let store = Arc::new(ReviewStore::default());
            client.bind_enrollment_store(store.clone()).await.unwrap();
            let writing = client
                .authorization
                .begin_store_write(store.as_ref())
                .unwrap();

            for _ in 0..2 {
                assert!(matches!(
                    client.logout().await,
                    Err(TinkleError::SecureStore)
                ));
                assert!(client.authorization.has_pending_store_write());
                assert_eq!(store.clears.load(Ordering::SeqCst), 0);
                assert!(!store.tombstone.load(Ordering::SeqCst));
                assert_eq!(
                    client.logout_cleanup.status(),
                    crate::SessionCleanup::Failed
                );
                assert!(client.logout_cleanup.progress.lock().await.cache_cleared);
                assert_eq!(client.principal().await, crate::sync::UNKNOWN_PRINCIPAL);
                assert!(client.watch_list().await.is_empty());
                assert!(storage.get_chat("user:42", 42).unwrap().is_none());
            }

            drop(writing);
            client.logout().await.unwrap();
            assert_eq!(store.clears.load(Ordering::SeqCst), 1);
            assert!(store.tombstone.load(Ordering::SeqCst));
            assert_eq!(
                client.logout_cleanup.status(),
                crate::SessionCleanup::Complete
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn review_credentials_clear_before_stalled_cache_and_later_attempt_retries() {
        let client = client(true);
        let store = Arc::new(ReviewStore::default());
        client.bind_enrollment_store(store.clone()).await.unwrap();
        // Transport retirement has already captured this scope. Hold the lock
        // needed by cache reset, not one needed by protected-store invalidation.
        client.logout_cleanup.progress.lock().await.principal = Some("user:42".into());
        let connection = client.connect_lock.lock().await;
        let caller = client.clone();
        let first = tokio::spawn(async move { caller.logout().await });
        while store.clears.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert!(store.tombstone.load(Ordering::SeqCst));
        assert!(!first.is_finished());
        tokio::time::advance(std::time::Duration::from_secs(10)).await;
        assert!(matches!(
            first.await.unwrap(),
            Err(TinkleError::CursorStore(_))
        ));
        assert_eq!(
            client.logout_cleanup.status(),
            crate::SessionCleanup::Failed
        );
        drop(connection);
        client.logout().await.unwrap();
        assert_eq!(
            client.logout_cleanup.status(),
            crate::SessionCleanup::Complete
        );
        assert_eq!(store.clears.load(Ordering::SeqCst), 1);
        assert_eq!(client.principal().await, crate::sync::UNKNOWN_PRINCIPAL);
    }

    #[tokio::test]
    async fn review_first_logout_clears_credentials_before_draining_cache_writer() {
        use crate::client::IngestBatch;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logout-first-call.db");
        let storage = Arc::new(crate::Storage::open(path.to_str().unwrap()).unwrap());
        let base = client(true);
        let client = Client::new_sqlite(
            base.config().clone(),
            crate::TrustSource::Returning {
                trust: base.enrollment_trust().await,
                enrollment: crate::Enrollment::new([9; 32], [8; 8], 1).unwrap(),
            },
            storage.clone(),
        );
        client.set_principal_user_id(42).await;
        let store = Arc::new(ReviewStore::default());
        client.bind_enrollment_store(store.clone()).await.unwrap();
        let epoch = client.reconcile_epoch();
        let chats = [tinkle_schema::proto::Chat {
            chat_id: 42,
            ..Default::default()
        }];
        let exclude = Default::default();
        client
            .ingest_messages(
                "user:42",
                "target:user:42",
                IngestBatch {
                    messages: &[],
                    users: &[],
                    chats: &chats,
                    exclude: &exclude,
                },
                epoch,
            )
            .await
            .unwrap();
        let blocker = rusqlite::Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let write = client.ingest_messages(
            "user:42",
            "target:user:42",
            IngestBatch {
                messages: &[],
                users: &[],
                chats: &chats,
                exclude: &exclude,
            },
            epoch,
        );
        tokio::pin!(write);
        assert!(futures_util::poll!(&mut write).is_pending());
        assert!(client
            .logout_cleanup
            .progress
            .lock()
            .await
            .principal
            .is_none());
        let (written, logged_out, ()) = tokio::join!(write, client.logout(), async {
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while store.clears.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("first logout must clear credentials without draining cache first");
            assert!(client.is_signed_out());
            assert!(store.tombstone.load(Ordering::SeqCst));
            assert_eq!(
                client.logout_cleanup.status(),
                crate::SessionCleanup::Pending
            );
            // Only now allow the pre-fence writer to commit. Wipe still has to
            // drain its gate before it can erase the old principal's cache.
            blocker.execute_batch("COMMIT").unwrap();
        });
        written.unwrap();
        logged_out.unwrap();
        assert!(storage.get_chat("user:42", 42).unwrap().is_none());
        assert_eq!(client.principal().await, crate::sync::UNKNOWN_PRINCIPAL);
        assert_eq!(store.clears.load(Ordering::SeqCst), 1);
        assert_eq!(
            client.logout_cleanup.status(),
            crate::SessionCleanup::Complete
        );
    }

    #[tokio::test]
    async fn review_tombstoned_clear_only_binding_counts_without_enrollment_authority() {
        let client = client(true);
        assert!(client.logout().await.is_err());
        let store = Arc::new(ReviewStore::default());
        store.tombstone.store(true, Ordering::SeqCst);
        client.bind_enrollment_store(store.clone()).await.unwrap();
        assert!(client.session_status().await.restart_safe_store);
        assert!(client.authorization.generation().is_err());
        assert!(client.prepare_enrollment(store.clone()).await.is_err());
        assert!(client
            .restore_active_enrollment(store.clone())
            .await
            .is_err());
        client.logout().await.unwrap();
        assert_eq!(
            client.session_status().await.cleanup,
            crate::SessionCleanup::Complete
        );
        assert_eq!(store.clears.load(Ordering::SeqCst), 1);
    }
}
