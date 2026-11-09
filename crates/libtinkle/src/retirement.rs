//! Negative-only enrollment observation on independently scheduled foreground work.

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

/// Foreground status cadence, independent of KeepAlive and Returning outcomes.
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
    observation: Mutex<EnrollmentObservation>,
    binding: Mutex<BindingState>,
    foreground: AtomicBool,
    foreground_cancel: Mutex<syntra_runtime::CancelToken>,
    task: tokio::sync::Mutex<Option<AbortTask>>,
    observing: tokio::sync::Mutex<()>,
    /// Repeated foreground entry waits for the same status-first barrier.
    foreground_entry: tokio::sync::Mutex<Option<ForegroundEntry>>,
}

impl RetirementObserver {
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

    /// Publish only a secret-free lifecycle result, never a retained reference.
    fn record(&self, result: EnrollmentObservation) {
        *self.observation.lock().unwrap_or_else(|e| e.into_inner()) = result;
    }
    /// Invalidate predecessor evidence before a source/persistence transition.
    pub(crate) fn invalidate(&self) {
        let mut state = self.binding.lock().unwrap_or_else(|e| e.into_inner());
        state.revision = state.revision.wrapping_add(1);
        state.current = None;
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
            loop {
                tokio::select! { biased;
                    _ = gone.cancelled() => break,
                    _ = revoked.cancelled() => break,
                    _ = stopped.cancelled() => break,
                    _ = ticks.tick() => {},
                }
                // Never await cleanup from this task: its owner may join us.
                if matches!(client.observe_retirement().await, Ok(Some(_))) {
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

    /// An observation never mutates the main trust source, principal or connection.
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
        let mut ephemeral = None;
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
                    let _source = self.connect_lock.lock().await;
                    if !self.retirement_source_matches(&binding).await {
                        return Ok(None);
                    }
                    let remaining = deadline.saturating_duration_since(clock.now());
                    let budget = syntra_net::EstablishmentBudget::new(clock.clone(), remaining)
                        .map_err(TinkleError::from_network)?;
                    let stream = self.dial_selected_tcp(&budget).await?;
                    unverified_socket = true;
                    let security = TrustSource::Bootstrap(binding.trust.clone()).security()?;
                    ephemeral = Some(
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
        if let Some(mut handle) = ephemeral {
            handle.suspend();
            let _ = clock
                .timeout_until(deadline, handle.shutdown_transport(Duration::from_secs(1)))
                .await;
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
