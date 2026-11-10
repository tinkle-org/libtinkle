//! Negative-only foreground enrollment observation, serialized with all owned sockets.

use crate::liveness::AbortTask;
use crate::{Client, SecureEnrollmentStore, ServerTrust, TinkleError, TrustSource};
use futures_util::future::{BoxFuture, FutureExt, Shared};
use prost::Message;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tinkle_schema::proto::{
    DeviceEnrollmentDisposition, DeviceEnrollmentReference, DeviceEnrollmentStatus,
    GetEnrollmentDispositionRequest, GetEnrollmentDispositionResponse, InitConnectRequest,
    InitConnectResponse,
};

/// Offline fallback cadence; healthy authenticated Returning sockets skip these checks.
pub const RETIREMENT_OBSERVATION_INTERVAL: Duration = Duration::from_secs(60);
/// Absolute ceiling for snapshot, route selection, proof, RPC and evidence recheck.
pub const RETIREMENT_OBSERVATION_BUDGET: Duration = Duration::from_secs(5);
const DISPOSITION_METHOD: &str = tinkle_schema::methods::authservice::GET_ENROLLMENT_DISPOSITION;

/// Last independently authorized observation; unknown/error states neither grant
/// account admission nor authorize credential erasure.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum EnrollmentObservation {
    /// No observation has completed on this coordinator.
    #[default]
    NotObserved,
    /// No usable permanent-retirement evidence was returned.
    Unknown,
    /// Exact current retirement was accepted; cleanup has its own status.
    Retired,
    /// The reference, local installation or protected namespace was superseded.
    Superseded,
    /// No usable response was obtained within the observation budget.
    Unavailable,
    /// Endpoint proof is rejected or unverified; explicit recovery is required.
    Parked,
    /// The protected host does not support conditional observation/cleanup.
    Unsupported,
}

/// One exact protected enrollment paired with the local source installation.
#[derive(Clone)]
pub(crate) struct RetainedEnrollment {
    pub(crate) trust: ServerTrust,
    pub(crate) reference: DeviceEnrollmentReference,
    pub(crate) user_id: i64,
    pub(crate) store: Arc<dyn SecureEnrollmentStore>,
    /// Stable across lifecycle-only clones, fresh for every source/restore binding.
    identity: Arc<()>,
    install_revision: u64,
    restoring: bool,
    negative_only: bool,
    enabled: bool,
}

/// Source changes and binding replacement share connect_lock with their callers.
#[derive(Default)]
struct BindingState {
    revision: u64,
    endpoint_revision: u64,
    current: Option<Arc<RetainedEnrollment>>,
}

/// The independently owned entry returns cleanup ownership without awaiting it.
type ForegroundAttempt =
    Shared<BoxFuture<'static, Result<Option<crate::logout::CleanupAttempt>, TinkleError>>>;

/// Completion belongs to the foreground interval, never to an individual waiter.
struct ForegroundEntry {
    stopped: syntra_runtime::CancelToken,
    completion: ForegroundAttempt,
}

/// One bounded observation at a time, owned independently of the main supervisor.
#[derive(Default)]
pub(crate) struct RetirementObserver {
    session: Option<Arc<crate::session::SessionCoordinator>>,
    observation: Mutex<EnrollmentObservation>,
    binding: Mutex<BindingState>,
    foreground: AtomicBool,
    foreground_cancel: Mutex<syntra_runtime::CancelToken>,
    task: tokio::sync::Mutex<Option<AbortTask>>,
    observing: tokio::sync::Mutex<()>,
    /// Repeated foreground entry waits for the same status-first barrier.
    foreground_entry: tokio::sync::Mutex<Option<ForegroundEntry>>,
    /// Accepted current Returning loss/denial; its own teardown may set retiring.
    loss: Mutex<Option<Arc<crate::liveness::LiveGeneration>>>,
    checked_loss: Mutex<std::sync::Weak<crate::liveness::LiveGeneration>>,
    loss_wake: tokio::sync::Notify,
    /// Retain shutdown/join ownership if an observer, suspend, or foreign waiter is cancelled.
    status_transport: tokio::sync::Mutex<Option<syntra_net::tcp::ClientHandle>>,
    /// Test-only scheduling observations; no production metrics or public API.
    #[cfg(test)]
    scheduled_attempts: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    scheduled_finished: tokio::sync::Notify,
}

impl RetirementObserver {
    /// Select the end of a scheduled attempt, including a pre-dial deferral.
    #[cfg(test)]
    pub(crate) async fn scheduled_attempt_finished(&self) {
        self.scheduled_finished.notified().await;
    }
    /// Inspect actual scheduler dispatches without inferring progress from socket EOF.
    #[cfg(test)]
    pub(crate) fn scheduled_attempt_count(&self) -> usize {
        self.scheduled_attempts.load(Ordering::SeqCst)
    }
    /// Install a real stalled SDK transport for cancellation/join-ownership regressions.
    #[cfg(test)]
    pub(crate) async fn install_status_transport(&self, handle: syntra_net::tcp::ClientHandle) {
        *self.status_transport.lock().await = Some(handle);
    }
    /// Inspect retained status ownership independently of the admission barrier.
    #[cfg(test)]
    pub(crate) async fn has_status_transport(&self) -> bool {
        self.status_transport.lock().await.is_some()
    }
    /// Hold observation admission so a supervisor interleaving can be selected deterministically.
    #[cfg(test)]
    pub(crate) async fn hold_observation(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.observing.lock().await
    }
    /// Wire every observation into the same account projection as authorization and cleanup.
    pub(crate) fn new(session: Arc<crate::session::SessionCoordinator>) -> Self {
        Self {
            session: Some(session),
            ..Self::default()
        }
    }
    /// Serialize foreground entry with cancellation; failures never call this.
    fn enter_foreground(&self) -> bool {
        let mut cancelled = self
            .foreground_cancel
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self.foreground.swap(true, Ordering::SeqCst) {
            return false;
        }
        *cancelled = syntra_runtime::CancelToken::new();
        true
    }

    /// Cancel host-driven status-first calls as well as the periodic owner.
    fn leave_foreground(&self) {
        let cancelled = self
            .foreground_cancel
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.foreground.store(false, Ordering::SeqCst);
        cancelled.cancel();
        self.loss.lock().unwrap_or_else(|e| e.into_inner()).take();
        // Retain metadata for the next foreground interval, but fence in-flight
        // observations and protected reads from the interval being retired.
        let mut state = self.binding.lock().unwrap_or_else(|e| e.into_inner());
        state.revision = state.revision.wrapping_add(1);
        if let Some(current) = &state.current {
            let mut retained = (**current).clone();
            retained.install_revision = state.revision;
            state.current = Some(Arc::new(retained));
        }
    }

    /// Capture the cancellation token belonging to this foreground interval.
    fn foreground_token(&self) -> Option<syntra_runtime::CancelToken> {
        let cancelled = self
            .foreground_cancel
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.foreground
            .load(Ordering::SeqCst)
            .then(|| cancelled.clone())
    }
    /// Query completion independently of contact connectivity and cleanup progress.
    pub(crate) fn observation(&self) -> EnrollmentObservation {
        *self.observation.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Accepted current loss must settle before Returning, even after its own teardown fence.
    pub(crate) fn has_pending_loss(&self) -> bool {
        // Never hold the loss lock while reading other owners: invalidation
        // takes binding before loss. Withdraw only the exact snapshotted demand,
        // not a replacement registered while those owners were being inspected.
        let candidate = self.loss.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let Some(candidate) = candidate else {
            return false;
        };
        let revision = self.revision();
        let current = self
            .session
            .as_ref()
            .and_then(|session| session.generation());
        let mut loss = self.loss.lock().unwrap_or_else(|e| e.into_inner());
        let obsolete = !self.foreground.load(Ordering::SeqCst)
            || candidate.source_revision != revision
            || current
                .as_ref()
                .is_none_or(|current| !Arc::ptr_eq(current, &candidate));
        if obsolete
            && loss
                .as_ref()
                .is_some_and(|generation| Arc::ptr_eq(generation, &candidate))
        {
            loss.take();
        }
        loss.is_some()
    }

    /// External resource release may accelerate one pending attempt; the worker never self-notifies.
    pub(crate) fn wake_pending_loss(&self) {
        if self.has_pending_loss() {
            self.loss_wake.notify_one();
        }
    }

    /// Retry local admission at 1/2/4/5 seconds, not every poll and not the offline sixty-second cadence.
    fn loss_retry_delay(failures: u32) -> Duration {
        Duration::from_secs((1u64 << failures.saturating_sub(1).min(3)).min(5))
    }

    /// Publish only a secret-free lifecycle result, never a retained reference.
    fn record(&self, result: EnrollmentObservation) {
        *self.observation.lock().unwrap_or_else(|e| e.into_inner()) = result;
        if let Some(session) = &self.session {
            session.observation(result);
        }
    }
    /// Invalidate predecessor evidence before a source/persistence transition.
    pub(crate) fn invalidate(&self) {
        let mut state = self.binding.lock().unwrap_or_else(|e| e.into_inner());
        state.revision = state.revision.wrapping_add(1);
        state.current = None;
        self.loss.lock().unwrap_or_else(|e| e.into_inner()).take();
    }

    /// Pair metadata with the source while the caller holds connect_lock.
    pub(crate) fn bind(
        &self,
        trust: ServerTrust,
        status: &DeviceEnrollmentStatus,
        store: Arc<dyn SecureEnrollmentStore>,
        restoring: bool,
        enabled: bool,
    ) -> Arc<RetainedEnrollment> {
        if !enabled {
            self.record(EnrollmentObservation::Unsupported);
        }
        let mut state = self.binding.lock().unwrap_or_else(|e| e.into_inner());
        state.revision = state.revision.wrapping_add(1);
        let binding = Arc::new(RetainedEnrollment {
            trust,
            reference: reference(status),
            user_id: status.user_id,
            store,
            identity: Arc::new(()),
            install_revision: state.revision,
            restoring,
            negative_only: false,
            enabled,
        });
        state.current = Some(binding.clone());
        binding
    }

    /// Capture the source revision before any asynchronous protected-store read.
    pub(crate) fn revision(&self) -> u64 {
        self.binding
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .revision
    }

    /// Only explicit trust recovery advances the endpoint configuration identity.
    pub(crate) fn reset_endpoint(&self, rejected: &AtomicBool) {
        let mut state = self.binding.lock().unwrap_or_else(|e| e.into_inner());
        state.endpoint_revision = state.endpoint_revision.wrapping_add(1);
        rejected.store(false, Ordering::SeqCst);
    }

    /// Capture configuration identity independently of key/operation rotations.
    pub(crate) fn endpoint_revision(&self) -> u64 {
        self.binding
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .endpoint_revision
    }

    /// A late proof timeout cannot park an explicitly recovered configuration.
    fn park_endpoint(&self, expected: u64, rejected: &AtomicBool) -> bool {
        let state = self.binding.lock().unwrap_or_else(|e| e.into_inner());
        if state.endpoint_revision != expected {
            return false;
        }
        rejected.store(true, Ordering::SeqCst);
        true
    }

    /// Retain the full reference without holding a lock across network I/O.
    fn current(&self) -> Option<Arc<RetainedEnrollment>> {
        self.binding
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .current
            .clone()
    }

    /// Main admission stays parked until status-first restoration has settled.
    pub(crate) fn is_restoring(&self) -> bool {
        self.current().is_some_and(|binding| binding.restoring)
    }

    /// Cancel ownership survives stop/suspend revisions, but never a genuine rebind.
    /// The caller holds connect_lock; comparison and withdrawal also share the
    /// metadata lock with lifecycle-only revisions.
    pub(crate) fn clear_restoring_binding(&self, expected: &Arc<RetainedEnrollment>) {
        let mut state = self.binding.lock().unwrap_or_else(|e| e.into_inner());
        if state.current.as_ref().is_some_and(|current| {
            current.restoring && Arc::ptr_eq(&current.identity, &expected.identity)
        }) {
            state.revision = state.revision.wrapping_add(1);
            state.current = None;
        }
    }

    /// An expired record keeps lookup metadata, but releases the admission barrier.
    pub(crate) fn retain_negative_only(&self, expected: &Arc<RetainedEnrollment>) {
        let mut state = self.binding.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(current) = state.current.as_ref().filter(|current| {
            current.restoring && Arc::ptr_eq(&current.identity, &expected.identity)
        }) {
            let mut retained = (**current).clone();
            state.revision = state.revision.wrapping_add(1);
            retained.install_revision = state.revision;
            retained.restoring = false;
            retained.negative_only = true;
            state.current = Some(Arc::new(retained));
        }
    }

    /// Withdraw only this cancelled/unacknowledged install before its host permit
    /// is released. A newer source/reference is never fenced by the old guard.
    pub(crate) fn fence_restore(
        &self,
        trust: &ServerTrust,
        status: &DeviceEnrollmentStatus,
    ) -> bool {
        let mut state = self.binding.lock().unwrap_or_else(|e| e.into_inner());
        let Some(current) = state.current.as_ref() else {
            return false;
        };
        if current.trust != *trust
            || current.reference != reference(status)
            || current.user_id != status.user_id
        {
            return false;
        }
        let mut pending = (**current).clone();
        state.revision = state.revision.wrapping_add(1);
        pending.install_revision = state.revision;
        pending.restoring = true;
        state.current = Some(Arc::new(pending));
        true
    }

    /// Arc identity also fences revision wraparound while an old query is retained.
    pub(crate) fn is_current(&self, binding: &Arc<RetainedEnrollment>) -> bool {
        let state = self.binding.lock().unwrap_or_else(|e| e.into_inner());
        state.revision == binding.install_revision
            && state
                .current
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, binding))
    }
}

/// Project the complete immutable lookup input; it carries no admission authority.
fn reference(status: &DeviceEnrollmentStatus) -> DeviceEnrollmentReference {
    DeviceEnrollmentReference {
        session_id: status.session_id.clone(),
        handle: status.handle.clone(),
        enrollment_epoch: status.enrollment_epoch,
        scope: status.scope,
        account_auth_epoch: status.account_auth_epoch,
    }
}

impl Client {
    /// With the dial gate held, adopt a cancelled observer's retained join owner.
    /// An active observer is never interrupted by an unrelated connect request.
    pub(crate) async fn drain_cancelled_status(&self) -> Result<(), TinkleError> {
        let _observer = self
            .retirement
            .observing
            .try_lock()
            .map_err(|_| TinkleError::NotConnected("status observation pending".into()))?;
        // A provisioning child has a distinct observer and cannot drain its parent's handle.
        if self.provisioning_context.is_some() {
            return Err(TinkleError::NotConnected(
                "parent status retirement pending".into(),
            ));
        }
        if !self.join_status_transport().await {
            return Err(TinkleError::NetworkUnavailable);
        }
        self.retirement.wake_pending_loss();
        Ok(())
    }
    /// Queue at most one immediate bounded check on an already-authorized foreground observer.
    /// This only schedules read-only evidence work; it never grants admission or replays a mutation.
    pub(crate) fn request_loss_observation(
        &self,
        generation: Arc<crate::liveness::LiveGeneration>,
    ) {
        if !generation.returning
            || generation.source_revision != self.retirement.revision()
            || generation.retiring.load(Ordering::SeqCst)
            || self.is_signed_out()
            || self.is_locally_suspended()
            || self.endpoint_rejected.load(Ordering::SeqCst)
            || !self.retirement.foreground.load(Ordering::SeqCst)
            || self
                .retirement
                .current()
                .is_none_or(|binding| !binding.enabled)
        {
            return;
        }
        if self
            .session_state
            .generation()
            .as_ref()
            .is_none_or(|current| !Arc::ptr_eq(current, &generation))
        {
            return;
        }
        let mut loss = self
            .retirement
            .loss
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut checked = self
            .retirement
            .checked_loss
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // A generation can request only one check, including concurrent denial and EOF.
        if checked
            .upgrade()
            .is_some_and(|g| Arc::ptr_eq(&g, &generation))
        {
            return;
        }
        *checked = Arc::downgrade(&generation);
        *loss = Some(generation);
        self.retirement.loss_wake.notify_one();
    }
    /// Pending writes cannot leave predecessor observations eligible for cleanup.
    pub(crate) async fn invalidate_retirement_binding(&self) {
        let _source = self.connect_lock.lock().await;
        self.retirement.invalidate();
    }

    /// Start the fixed schedule before attempting Returning, without an immediate tick.
    pub(crate) async fn arm_retirement_schedule(&self) {
        if self.is_signed_out() {
            return;
        }
        self.retirement.enter_foreground();
        if let Some(stopped) = self.retirement.foreground_token() {
            self.arm_retirement_schedule_for(&stopped, &self.owner_gone())
                .await;
        }
    }

    /// A retained entry may arm only its own still-live interval, never revive a stopped one.
    async fn arm_retirement_schedule_for(
        &self,
        stopped: &syntra_runtime::CancelToken,
        owner_gone: &syntra_runtime::CancelToken,
    ) {
        let mut owner = self.retirement.task.lock().await;
        if stopped.is_cancelled()
            || self.is_signed_out()
            || owner.is_some()
            || self
                .retirement
                .current()
                .is_none_or(|binding| !binding.enabled)
        {
            return;
        }
        let first = tokio::time::Instant::now() + RETIREMENT_OBSERVATION_INTERVAL;
        let client = self.background_clone();
        let gone = owner_gone.clone();
        let revoked = self.authorization.revoked.clone();
        let stopped = stopped.clone();
        *owner = Some(AbortTask::new(tokio::spawn(async move {
            let mut ticks = tokio::time::interval_at(first, RETIREMENT_OBSERVATION_INTERVAL);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut retry = None;
            let mut failures = 0u32;
            loop {
                let caller_gone = if client.retirement.has_pending_loss() {
                    client.provisioning_caller_gone()
                } else {
                    None
                };
                let periodic = tokio::select! { biased;
                    _ = gone.cancelled() => break,
                    _ = revoked.cancelled() => break,
                    _ = stopped.cancelled() => break,
                    _ = ticks.tick() => true,
                    _ = client.retirement.loss_wake.notified() => false,
                    _ = async { match retry { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending::<()>().await } } => false,
                    _ = async { match caller_gone { Some(gone) => gone.cancelled().await, None => std::future::pending::<()>().await } } => false,
                };
                if client.is_locally_suspended()
                    || (!periodic && !client.retirement.has_pending_loss())
                {
                    retry = None;
                    failures = 0;
                    continue;
                }
                // Never await cleanup from this task: its owner may join us.
                #[cfg(test)]
                client
                    .retirement
                    .scheduled_attempts
                    .fetch_add(1, Ordering::SeqCst);
                let result = client.observe_retirement().await;
                if client.retirement.has_pending_loss() {
                    failures = failures.saturating_add(1);
                    retry = Some(
                        tokio::time::Instant::now()
                            + RetirementObserver::loss_retry_delay(failures),
                    );
                } else {
                    failures = 0;
                    retry = None;
                }
                #[cfg(test)]
                client.retirement.scheduled_finished.notify_one();
                if matches!(result, Ok(Some(_))) {
                    break;
                }
            }
        })));
    }

    /// Explicit suspension/stop is the only lifecycle action that stops this schedule.
    pub(crate) async fn pause_retirement_schedule(&self) {
        let mut entry = self.retirement.foreground_entry.lock().await;
        self.retirement.leave_foreground();
        let mut owner = self.retirement.task.lock().await;
        if let Some(task) = owner.as_mut() {
            task.shutdown().await;
        }
        *owner = None;
        drop(owner);
        // A status-first restore is owned by its host waiter, not the periodic
        // task. Drain that cancelled attempt before lifecycle retirement returns.
        {
            let _drained = self.retirement.observing.lock().await;
        }
        if let Some(previous) = entry.take() {
            // The entry task never awaits cleanup, so cleanup may safely join it.
            let _ = previous.completion.await;
        }
        // Bootstrap children share the dial gate, not the parent's status owner.
        // Their empty status slot must never clear the parent's retained barrier.
        if self.provisioning_context.is_none()
            && self.connect_lock.status_open.load(Ordering::SeqCst)
        {
            let _connection = self.connect_lock.lock().await;
            self.join_status_transport().await;
        }
    }

    /// Caller holds the shared dial gate; retain the handle through cancelled shutdown awaits.
    async fn join_status_transport(&self) -> bool {
        let mut status = self.retirement.status_transport.lock().await;
        self.join_status_handle(&mut status).await
    }

    /// Keep both handle and shared barrier until rc.5 attests carrier/policy join completion.
    async fn join_status_handle(&self, status: &mut Option<syntra_net::tcp::ClientHandle>) -> bool {
        if !crate::liveness::join_retained_transport(status).await {
            return false;
        }
        self.connect_lock.status_open.store(false, Ordering::SeqCst);
        true
    }

    /// Only an explicit foreground transition authorizes status-first observation.
    /// Repeated foreground calls and Returning errors cannot reset the timer.
    pub(crate) async fn foreground_retirement(&self) -> Result<(), TinkleError> {
        let completion = {
            let mut entry = self.retirement.foreground_entry.lock().await;
            if self.is_signed_out() {
                return Err(TinkleError::SignedOut);
            }
            if let Some(current) = entry.as_ref().filter(|entry| !entry.stopped.is_cancelled()) {
                current.completion.clone()
            } else {
                if !self.retirement.enter_foreground() {
                    return Ok(());
                }
                let stopped = self
                    .retirement
                    .foreground_token()
                    .expect("entered foreground interval");
                let scope = stopped.clone();
                let gone = self.owner_gone();
                let client = self.background_clone();
                let clock = syntra_runtime::elapsed::ElapsedClock::new();
                let deadline = clock.now() + RETIREMENT_OBSERVATION_BUDGET;
                // Spawn and retain completion before the caller can yield again.
                let task = tokio::spawn(async move {
                    tokio::select! { biased;
                        _ = gone.cancelled() => Ok(None),
                        result = async {
                            clock.timeout_until(deadline,
                                client.arm_retirement_schedule_for(&scope, &gone)
                            ).await.map_err(|_| TinkleError::NetworkUnavailable)?;
                            if scope.is_cancelled() { return Ok(None); }
                            // The observer owns this same cutoff and must run its
                            // proof-rejection finalizer after the timeout fires.
                            client.observe_retirement_until(clock.clone(), deadline).await
                        } => result,
                    }
                });
                let completion = async move {
                    task.await.map_err(|_| {
                        TinkleError::NotConnected("foreground observation task failed".into())
                    })?
                }
                .boxed()
                .shared();
                *entry = Some(ForegroundEntry {
                    stopped,
                    completion: completion.clone(),
                });
                completion
            }
        };
        if let Some(cleanup) = completion.await? {
            cleanup.await?;
            return Err(TinkleError::SignedOut);
        }
        if self.is_signed_out() {
            self.logout_cleanup.join(self.background_clone()).await?;
            return Err(TinkleError::SignedOut);
        }
        Ok(())
    }

    /// Observe without granting authorization, after retiring and joining the previous socket.
    async fn observe_retirement(
        &self,
    ) -> Result<Option<crate::logout::CleanupAttempt>, TinkleError> {
        let clock = syntra_runtime::elapsed::ElapsedClock::new();
        let deadline = clock.now() + RETIREMENT_OBSERVATION_BUDGET;
        self.observe_retirement_until(clock, deadline).await
    }

    /// Foreground entry carries its original absolute budget through storage and RPC work.
    async fn observe_retirement_until(
        &self,
        clock: syntra_runtime::elapsed::ElapsedClock,
        deadline: std::time::Instant,
    ) -> Result<Option<crate::logout::CleanupAttempt>, TinkleError> {
        let _single = clock
            .timeout_until(deadline, self.retirement.observing.lock())
            .await
            .map_err(|_| {
                self.retirement.record(EnrollmentObservation::Unavailable);
                TinkleError::NetworkUnavailable
            })?;
        self.observe_retirement_locked_until(clock, deadline).await
    }

    /// Caller owns observing; an install barrier cannot skip the new binding's check.
    /// Retain accepted loss through incomplete teardown without replacing newer demand.
    async fn observe_retirement_locked_until(
        &self,
        clock: syntra_runtime::elapsed::ElapsedClock,
        deadline: std::time::Instant,
    ) -> Result<Option<crate::logout::CleanupAttempt>, TinkleError> {
        if self.is_signed_out()
            || !self.retirement.foreground.load(Ordering::SeqCst)
            || self.endpoint_rejected.load(Ordering::SeqCst)
        {
            return Ok(None);
        }
        let Some(binding) = self.retirement.current() else {
            self.retirement
                .loss
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            return Ok(None);
        };
        let Some(stopped) = self.retirement.foreground_token() else {
            return Ok(None);
        };
        if !binding.enabled {
            self.retirement.record(EnrollmentObservation::Unsupported);
            return Ok(None);
        }
        let endpoint_revision = self.retirement.endpoint_revision();
        // Keep the shared dial gate through status shutdown/join, not merely
        // establishment: no Returning dial may overlap this temporary socket.
        let mut connection = Some(
            clock
                .timeout_until(deadline, self.connect_lock.lock())
                .await
                .map_err(|_| TinkleError::NetworkUnavailable)?,
        );
        // Periodic/negative-only observation is also admission work, not
        // permission to cancel a caller's active capability-bearing request.
        if self.active_provisioning_child() {
            return Ok(None);
        }
        if !self.join_status_transport().await {
            return Err(TinkleError::NetworkUnavailable);
        }
        if stopped.is_cancelled() || !self.retirement_source_matches(&binding).await {
            return Ok(None);
        }
        let loss = self
            .retirement
            .loss
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(expected) = loss.as_ref() {
            // Admission already rejected local retirement. Our own teardown may
            // now have fenced this accepted generation during an earlier attempt.
            if expected.source_revision != self.retirement.revision() {
                return Ok(None);
            }
            if self
                .session_state
                .generation()
                .as_ref()
                .is_some_and(|current| !Arc::ptr_eq(current, expected))
            {
                return Ok(None);
            }
        } else if !binding.restoring
            && self
                .session_state
                .generation()
                .is_some_and(|g| g.returning && g.open.load(Ordering::SeqCst))
        {
            // Healthy Returning sockets are never interrupted by a periodic status check.
            return Ok(None);
        }
        if !matches!(
            clock
                .timeout_until(deadline, self.teardown_connection())
                .await,
            Ok(true)
        ) {
            if let Some(expected) = loss {
                // Retry accepted demand, never a newer request registered while
                // joining. Foreground/source/current checks still fence retries.
                self.retirement
                    .loss
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get_or_insert(expected);
            }
            return Err(TinkleError::NetworkUnavailable);
        }
        let mut ephemeral = self.retirement.status_transport.lock().await;
        let mut unverified_socket = false;
        let mut superseded = false;
        let mut cancelled = false;
        let observed = tokio::select! { biased;
            _ = stopped.cancelled() => { cancelled = true; Ok(None) },
            result = async { clock
            .timeout_until(deadline, async {
                let Some(snapshot) = binding
                    .store
                    .namespace_snapshot()
                    .await
                    .map_err(|_| TinkleError::SecureStore)?
                else {
                    return Ok(None);
                };
                if snapshot.cleanup_pending
                    || snapshot.has_pending
                    || !self.authorization.matches_namespace(
                        binding.store.as_ref(),
                        &snapshot.incarnation,
                        &snapshot.revision,
                    )
                    || !self.authorization.only_namespace(&snapshot.incarnation)
                {
                    return Ok(None);
                }
                let Some(active) = snapshot.active.as_ref() else {
                    return Ok(None);
                };
                let (trust, status) = crate::enrollment::retained_metadata(active)?;
                if trust != binding.trust
                    || reference(&status) != binding.reference
                    || status.user_id != binding.user_id
                {
                    return Ok(None);
                }
                {
                    if !self.retirement_source_matches(&binding).await {
                        return Ok(None);
                    }
                    let remaining = deadline.saturating_duration_since(clock.now());
                    let budget = syntra_net::EstablishmentBudget::new(clock.clone(), remaining)
                        .map_err(TinkleError::from_network)?;
                    let stream = self.dial_selected_tcp(&budget).await?;
                    self.connect_lock.status_open.store(true, Ordering::SeqCst);
                    unverified_socket = true;
                    let security = TrustSource::Bootstrap(binding.trust.clone()).security()?;
                    *ephemeral = Some(
                        syntra_net::tcp::connect_stream_with_budget(
                            stream,
                            security,
                            Vec::new(),
                            self.config().connection.clone(),
                            Default::default(),
                            budget,
                        )
                        .await
                        .map_err(|error| {
                            let error = self.classify_establishment(error);
                            // Bootstrap carries no device proof. An unverified
                            // first-flight EOF can conceal a pin rejection; park
                            // this configuration without treating it as retirement.
                            if matches!(error, TinkleError::EstablishmentFailed) {
                                self.endpoint_rejected.store(true, Ordering::SeqCst);
                                self.authorization.set_authentication_known(false);
                            }
                            error
                        })?,
                    );
                    unverified_socket = false;
                }
                let request = InitConnectRequest {
                    client: Some(self.config().client_info.proto()),
                    method: DISPOSITION_METHOD.into(),
                    payload: GetEnrollmentDispositionRequest {
                        reference: Some(binding.reference.clone()),
                    }
                    .encode_to_vec()
                    .into(),
                };
                let remaining_ms = deadline
                    .saturating_duration_since(clock.now())
                    .as_millis()
                    .min(u32::MAX as u128) as u32;
                if remaining_ms == 0 {
                    return Err(TinkleError::NetworkUnavailable);
                }
                let bytes = ephemeral
                    .as_ref()
                    .expect("established status connection")
                    .conn
                    .call(
                        "/tinkle.v1.ConnectionService/InitConnect",
                        request.encode_to_vec(),
                        remaining_ms,
                    )
                    .await?;
                let initialized = InitConnectResponse::decode(bytes.as_slice())
                    .map_err(|_| TinkleError::Codec("invalid status initialization".into()))?;
                let response =
                    GetEnrollmentDispositionResponse::decode(initialized.payload.as_ref())
                        .map_err(|_| TinkleError::Codec("invalid enrollment disposition".into()))?;
                if response.reference.as_ref() != Some(&binding.reference)
                    || response.disposition != DeviceEnrollmentDisposition::Retired as i32
                {
                    return Ok(None);
                }
                // Evidence may fence credentials even while carrier cleanup is
                // unfinished. The retained barrier still prevents any new dial.
                let _joined = self.join_status_handle(&mut ephemeral).await;
                // Restore enrollment -> connection lock order before protected
                // evidence recheck. Unfinished status I/O remains admission-fenced.
                drop(connection.take());
                let _flow = self.enrollment_lock.lock().await;
                let _source = self.connect_lock.lock().await;
                if !self.retirement_source_matches(&binding).await {
                    superseded = true;
                    return Ok(None);
                }
                let Some(current) = binding
                    .store
                    .namespace_snapshot()
                    .await
                    .map_err(|_| TinkleError::SecureStore)?
                else {
                    return Ok(None);
                };
                if current.cleanup_pending
                    || current.has_pending
                    || current.incarnation != snapshot.incarnation
                    || current.revision != snapshot.revision
                    || !self.authorization.only_namespace(&current.incarnation)
                {
                    superseded = true;
                    return Ok(None);
                }
                let Some(active) = current.active.as_ref() else {
                    return Ok(None);
                };
                let (trust, status) = crate::enrollment::retained_metadata(active)?;
                if trust != binding.trust
                    || reference(&status) != binding.reference
                    || status.user_id != binding.user_id
                    || clock.now() >= deadline
                {
                    return Ok(None);
                }
                // Fence and retain cleanup without an intervening await. A failed CAS
                // leaves this coordinator signed out and never clears another revision.
                self.retirement.record(EnrollmentObservation::Retired);
                Ok(Some(self.logout_cleanup.join_retirement(
                    self.background_clone(),
                    format!("user:{}", binding.user_id),
                )))
            })
            .await
            .map_err(|_| TinkleError::NetworkUnavailable)
            .and_then(|result| result) } => result,
        };
        if unverified_socket
            && !cancelled
            && self
                .retirement
                .park_endpoint(endpoint_revision, &self.endpoint_rejected)
        {
            // Cancellation at the absolute proof deadline is equally ambiguous.
            self.authorization.set_authentication_known(false);
        }
        let outcome = match &observed {
            Ok(Some(_)) => EnrollmentObservation::Retired,
            Ok(None)
                if self.is_signed_out()
                    && self.retirement.observation() == EnrollmentObservation::Retired =>
            {
                EnrollmentObservation::Retired
            }
            Ok(None) if superseded || !self.retirement.is_current(&binding) => {
                EnrollmentObservation::Superseded
            }
            Ok(None) => EnrollmentObservation::Unknown,
            Err(_) if self.endpoint_rejected.load(Ordering::SeqCst) => {
                EnrollmentObservation::Parked
            }
            Err(_) => EnrollmentObservation::Unavailable,
        };
        self.retirement.record(outcome);
        // An incomplete/error report leaves the exact handle and barrier retained.
        if !self.join_status_handle(&mut ephemeral).await
            && observed.is_ok()
            && !self.is_signed_out()
        {
            return Err(TinkleError::NetworkUnavailable);
        }
        observed
    }

    /// Check source and retained reference together under connect_lock.
    async fn retirement_source_matches(&self, binding: &Arc<RetainedEnrollment>) -> bool {
        if self.is_signed_out()
            || !self.retirement.foreground.load(Ordering::SeqCst)
            || self.endpoint_rejected.load(Ordering::SeqCst)
            || !self.retirement.is_current(binding)
        {
            return false;
        }
        match &*self.trust_source.read().await {
            TrustSource::Returning { trust, enrollment } => {
                *trust == binding.trust
                    && enrollment.binding()
                        == (
                            binding
                                .reference
                                .handle
                                .as_ref()
                                .try_into()
                                .unwrap_or([0; 8]),
                            binding.reference.enrollment_epoch,
                        )
            }
            TrustSource::Bootstrap(trust) => {
                (binding.restoring || binding.negative_only) && *trust == binding.trust
            }
            TrustSource::SignedOut(_) => false,
        }
    }

    /// Host restore awaits local acknowledgement; periodic work only retains it.
    pub(crate) async fn observe_restored_enrollment(&self) -> Result<(), TinkleError> {
        if self.is_suspended().await {
            return Err(TinkleError::NotConnected("restoration is suspended".into()));
        }
        self.arm_retirement_schedule().await;
        let observed = {
            let _single = self.retirement.observing.lock().await;
            let clock = syntra_runtime::elapsed::ElapsedClock::new();
            let deadline = clock.now() + RETIREMENT_OBSERVATION_BUDGET;
            self.observe_retirement_locked_until(clock, deadline)
                .await?
        };
        if let Some(cleanup) = observed {
            cleanup.await?;
            return Err(TinkleError::SignedOut);
        }
        if self.is_signed_out() {
            self.logout_cleanup.join(self.background_clone()).await?;
            return Err(TinkleError::SignedOut);
        }
        Ok(())
    }
}

#[cfg(test)]
mod review_tests {
    use super::*;
    use crate::enrollment::review_tests::{client, expired_store};

    #[test]
    /// Deferred admission uses a bounded exponential timer, never immediate self-notification.
    fn loss_retry_delay_is_exponential_and_capped() {
        let seconds: Vec<_> = [1, 2, 3, 4, 5, u32::MAX]
            .into_iter()
            .map(|failures| RetirementObserver::loss_retry_delay(failures).as_secs())
            .collect();
        assert_eq!(seconds, [1, 2, 4, 5, 5, 5]);
    }

    #[test]
    /// Replacement fences obsolete demand; teardown alone cannot withdraw already-accepted current demand.
    fn pending_loss_withdraws_obsolete_generation_without_proof() {
        let client = client(false);
        client.retirement.enter_foreground();
        let revision = client.retirement.revision();
        let old = crate::liveness::LiveGeneration::new(true, revision);
        client.session_state.network(old.clone(), true);
        *client.retirement.loss.lock().unwrap() = Some(old.clone());
        assert!(client.retirement.has_pending_loss());
        let current = crate::liveness::LiveGeneration::new(true, revision);
        client.session_state.network(current.clone(), true);
        assert!(!client.retirement.has_pending_loss());
        *client.retirement.loss.lock().unwrap() = Some(current.clone());
        old.retiring.store(true, Ordering::SeqCst);
        assert!(client.retirement.has_pending_loss());
        current.retiring.store(true, Ordering::SeqCst);
        assert!(client.retirement.has_pending_loss());
        client.retirement.invalidate();
        assert!(!client.retirement.has_pending_loss());
        assert!(!client.is_signed_out());
    }

    #[tokio::test]
    /// Expired credentials retain negative-only lookup metadata without blocking future admission.
    async fn review_expired_restore_retains_observable_metadata_without_admission_barrier() {
        let client = client(false);
        let store = expired_store(&client);
        assert!(client.restore_active_enrollment(store).await.is_err());
        let binding = client
            .retirement
            .current()
            .expect("negative lookup retained");
        assert!(binding.negative_only);
        assert!(!binding.restoring);
        assert!(client.retirement_source_matches(&binding).await);
        assert!(!client.has_returning_source().await);
        client.stop().await;
    }

    #[tokio::test]
    /// A busy periodic observer must not count as completion of a new status-first restore.
    async fn review_status_first_restore_waits_for_busy_periodic_observer() {
        let client = client(false);
        let store = expired_store(&client);
        let observing = client.retirement.observing.lock().await;
        let caller = client.clone();
        let host = store.clone();
        let restore = tokio::spawn(async move { caller.restore_active_enrollment(host).await });
        while !client.retirement.is_restoring() {
            tokio::task::yield_now().await;
        }
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(
            !restore.is_finished(),
            "busy observation is not a status acknowledgement"
        );
        assert_eq!(store.snapshots.load(Ordering::SeqCst), 1);
        drop(observing);
        assert!(restore.await.unwrap().is_err());
        assert!(store.snapshots.load(Ordering::SeqCst) >= 2);
        client.stop().await;
    }
}
