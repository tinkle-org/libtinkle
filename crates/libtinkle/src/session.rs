//! Queryable account lifecycle, distinct from connectivity notifications.

use crate::liveness::LiveGeneration;
use crate::{Client, TinkleError};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Weak};
use syntra_runtime::CancelToken;
use tokio::sync::{mpsc, Notify};

/// Current transport availability; loss alone says nothing about retirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionNetwork {
    /// A live encrypted connection is available.
    Available,
    /// No usable connection is currently observed.
    Unavailable,
}

/// What this coordinator knows about its current authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionAuthorization {
    /// No current authenticated Returning proof, or an account RPC was denied.
    Unknown,
    /// The current Returning channel authenticated successfully.
    Active,
    /// This coordinator has irreversibly fenced local authorization.
    SignedOut,
}

/// Retained local cleanup progress; separate from remote revocation confirmation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SessionCleanup {
    /// Local sign-out has not started.
    #[default]
    NotStarted,
    /// An independently owned cleanup attempt is running.
    Pending,
    /// Every required local step acknowledged completion.
    Complete,
    /// Local cleanup needs a retry; authorization remains fenced.
    Failed,
}

/// Account lifecycle query. No error code or network loss implies retirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionStatus {
    /// Availability of the current foreground transport.
    pub network: SessionNetwork,
    /// Current authorization knowledge, including irreversible local sign-out.
    pub authorization: SessionAuthorization,
    /// Progress of acknowledged local cleanup.
    pub cleanup: SessionCleanup,
    /// All bound hosts support conditional writes and durable cleanup tombstones.
    /// Restart-safe completion requires this and cleanup == Complete.
    pub restart_safe_store: bool,
    /// Last independent enrollment observation; it never confirms a Logout invocation.
    pub enrollment_observation: crate::EnrollmentObservation,
}

/// Immutable first cause of this Client's irreversible authorization fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionSignOutReason {
    /// The host requested local sign-out.
    SelfLogout,
    /// Exact, current authority evidence permanently retired this enrollment.
    SessionRetired,
}

/// Retained account projection. None means no current review snapshot, not an empty list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionState {
    /// Monotonic local projection revision; it is not a server journal checkpoint.
    pub revision: u64,
    /// The same authoritative view returned by Client::session_status.
    pub status: SessionStatus,
    /// First sign-out cause, retained through cleanup failure and retries.
    pub reason: Option<SessionSignOutReason>,
    /// Complete GetSessions snapshot, never synthesized from a hint or confirmation reply.
    pub sessions: Option<Vec<tinkle_schema::proto::Session>>,
}

/// Typed, bounded ingress to the sole serialized account reducer.
enum Transition {
    Network(Arc<LiveGeneration>, bool),
    Authentication(bool),
    Denial(Arc<LiveGeneration>),
    SignedOut(SessionSignOutReason),
    Cleanup(SessionCleanup),
    Store(bool),
    Observation(crate::EnrollmentObservation),
    Review(Arc<LiveGeneration>, u64, Vec<tinkle_schema::proto::Session>),
    Dirty(Option<Arc<LiveGeneration>>),
}

/// Single latest-value mailbox: slow consumers coalesce revisions, never terminal state.
struct Mailbox {
    latest: Mutex<Option<SessionState>>,
    changed: Notify,
    stopped: CancelToken,
}

/// One application consumer, independent of Client ownership and network workers.
pub struct SessionStateSubscription {
    mailbox: Arc<Mailbox>,
    gone: CancelToken,
    next_lock: tokio::sync::Mutex<()>,
}

impl SessionStateSubscription {
    /// Await the initial/current snapshot or a newer revision. Cancellation consumes nothing.
    pub async fn next(&self) -> Option<SessionState> {
        let _single = tokio::select! { biased;
            _ = self.mailbox.stopped.cancelled() => return None,
            _ = self.gone.cancelled() => return None,
            guard = self.next_lock.lock() => guard,
        };
        loop {
            let changed = self.mailbox.changed.notified();
            if self.mailbox.stopped.is_cancelled() || self.gone.is_cancelled() {
                return None;
            }
            if let Some(state) = self
                .mailbox
                .latest
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                return Some(state);
            }
            tokio::select! { biased;
                _ = self.mailbox.stopped.cancelled() => return None,
                _ = self.gone.cancelled() => return None,
                _ = changed => {},
            }
        }
    }

    /// Release the single-consumer slot immediately and unblock all pending next calls.
    pub fn stop(&self) {
        self.mailbox.stopped.cancel();
    }
}

impl Drop for SessionStateSubscription {
    /// Dropping a subscription has the same local effect as stop, never protocol cancellation.
    fn drop(&mut self) {
        self.stop();
    }
}

/// Client-lifetime MPSC owner. Its reducer never awaits I/O, UI, or a consumer.
/// Producers drain under one short lock so even a synchronous security fence is
/// immediately reflected in queries. There is no detached lifecycle task to self-join.
pub(crate) struct SessionCoordinator {
    tx: mpsc::Sender<Transition>,
    owner: Mutex<Reducer>,
    review_wake: Notify,
    review_task: Mutex<Option<crate::liveness::AbortTask>>,
}

/// Only this owner consumes ingress, assigns revisions, and registers consumers.
struct Reducer {
    rx: mpsc::Receiver<Transition>,
    state: SessionState,
    generation: Option<Arc<LiveGeneration>>,
    consumer: Weak<Mailbox>,
    dirty: bool,
    demand: u64,
}

impl SessionCoordinator {
    /// Allocate bounded ingress without requiring an entered async runtime at construction.
    pub(crate) fn new(signed_out: bool) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(1);
        Arc::new(Self {
            tx,
            owner: Mutex::new(Reducer {
                rx,
                state: SessionState {
                    revision: 0,
                    reason: None,
                    sessions: None,
                    status: SessionStatus {
                        network: SessionNetwork::Unavailable,
                        authorization: if signed_out {
                            SessionAuthorization::SignedOut
                        } else {
                            SessionAuthorization::Unknown
                        },
                        cleanup: SessionCleanup::NotStarted,
                        restart_safe_store: false,
                        enrollment_observation: crate::EnrollmentObservation::NotObserved,
                    },
                },
                generation: None,
                consumer: Weak::new(),
                dirty: false,
                demand: 0,
            }),
            review_wake: Notify::new(),
            review_task: Mutex::new(None),
        })
    }

    /// Every producer uses the same bounded MPSC ingress and serialized drain.
    fn submit(&self, transition: Transition) {
        let mut owner = self.owner.lock().unwrap_or_else(|e| e.into_inner());
        // The sole consumer drains before releasing the lock: capacity is always
        // available, with no allocation or async admission wait for security events.
        if let Err(error) = self.tx.try_send(transition) {
            // Preserve FIFO even if an already accepted producer filled ingress.
            while let Ok(accepted) = owner.rx.try_recv() {
                owner.apply(accepted);
            }
            owner.apply(error.into_inner());
        }
        while let Ok(transition) = owner.rx.try_recv() {
            owner.apply(transition);
        }
        drop(owner);
        self.review_wake.notify_one();
    }

    /// Snapshot and registration share the reducer lock; an update cannot fall between them.
    fn subscribe(&self, gone: CancelToken) -> Result<SessionStateSubscription, TinkleError> {
        let mut owner = self.owner.lock().unwrap_or_else(|e| e.into_inner());
        if owner
            .consumer
            .upgrade()
            .is_some_and(|consumer| !consumer.stopped.is_cancelled())
        {
            return Err(TinkleError::Subscription(
                "session state already has an active consumer".into(),
            ));
        }
        let mailbox = Arc::new(Mailbox {
            latest: Mutex::new(Some(owner.state.clone())),
            changed: Notify::new(),
            stopped: CancelToken::new(),
        });
        owner.consumer = Arc::downgrade(&mailbox);
        Ok(SessionStateSubscription {
            mailbox,
            gone,
            next_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Inspect retained state without network/storage access or a lifecycle queue wait.
    #[cfg(test)]
    fn snapshot(&self) -> SessionState {
        self.owner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .state
            .clone()
    }
    /// Status queries do not clone or allocate the retained review list.
    fn status(&self) -> SessionStatus {
        self.owner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .state
            .status
            .clone()
    }
    /// Lifecycle termination releases a foreign next waiter without destroying retained state.
    pub(crate) fn stop_consumer(&self) {
        let owner = self.owner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(consumer) = owner.consumer.upgrade() {
            consumer.stopped.cancel();
        }
    }
    /// Capture identity only, never credentials, for current-channel denial handling.
    pub(crate) fn generation(&self) -> Option<Arc<LiveGeneration>> {
        self.owner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .generation
            .clone()
    }
    /// Update only a matching socket; delayed disconnects cannot retire a successor.
    pub(crate) fn network(&self, generation: Arc<LiveGeneration>, available: bool) {
        self.submit(Transition::Network(generation, available));
    }
    /// Withdraw authentication on denial; only connection establishment grants it.
    pub(crate) fn authentication(&self, known: bool) {
        self.submit(Transition::Authentication(known));
    }
    /// A late denial cannot withdraw a successor connection's authorization knowledge.
    pub(crate) fn denial(&self, generation: Arc<LiveGeneration>) {
        self.submit(Transition::Denial(generation));
    }
    /// Test inspection of the authoritative proof projection, not a parallel authentication flag.
    #[cfg(test)]
    pub(crate) fn authenticated(&self) -> bool {
        self.snapshot().status.authorization == SessionAuthorization::Active
    }
    /// Record the first fence cause before any cleanup await.
    pub(crate) fn signed_out(&self, reason: SessionSignOutReason) {
        self.submit(Transition::SignedOut(reason));
    }
    /// Publish independently owned cleanup progress, including failures and retries.
    pub(crate) fn cleanup(&self, status: SessionCleanup) {
        self.submit(Transition::Cleanup(status));
    }
    /// Publish the protected namespace's conditional-cleanup capability.
    pub(crate) fn store(&self, safe: bool) {
        self.submit(Transition::Store(safe));
    }
    /// Record negative-only evidence separately from cleanup and authorization.
    pub(crate) fn observation(&self, value: crate::EnrollmentObservation) {
        self.submit(Transition::Observation(value));
    }
    /// Coalesce hints; a hint received during a fetch leaves another fetch due.
    pub(crate) fn dirty(&self, generation: Option<Arc<LiveGeneration>>) {
        self.submit(Transition::Dirty(generation));
    }

    /// Start one supervised read worker. It never retains the host ownership anchor.
    pub(crate) fn start_review(self: &Arc<Self>, client: &Client) {
        let mut task = self.review_task.lock().unwrap_or_else(|e| e.into_inner());
        if task.is_some() {
            return;
        }
        let caller = client.background_clone();
        let gone = client.owner_gone();
        let coordinator = self.clone();
        *task = Some(crate::liveness::AbortTask::new(tokio::spawn(async move {
            tokio::select! { biased;
                _ = gone.cancelled() => {},
                _ = coordinator.review_loop(caller) => {},
            }
        })));
    }

    /// Fetch sequentially with bounded failure backoff. Generation closure cancels old read work.
    async fn review_loop(&self, client: Client) {
        let mut failures = 0u32;
        loop {
            let wake = self.review_wake.notified();
            let fetch = {
                let mut owner = self.owner.lock().unwrap_or_else(|e| e.into_inner());
                if owner.dirty && owner.state.status.authorization == SessionAuthorization::Active {
                    let fetch = owner
                        .generation
                        .clone()
                        .filter(|g| g.open.load(Ordering::SeqCst))
                        .map(|g| (g, owner.demand));
                    if fetch.is_some() {
                        owner.dirty = false;
                    }
                    fetch
                } else {
                    None
                }
            };
            let Some((generation, demand)) = fetch else {
                wake.await;
                continue;
            };
            let caller = client.for_session_generation(generation.clone());
            let account = caller.account();
            let result = tokio::select! { biased;
                _ = generation.closed.cancelled() => { failures = 0; continue; },
                result = account.get_sessions() => result,
            };
            match result {
                Ok(sessions) => {
                    failures = 0;
                    self.submit(Transition::Review(generation, demand, sessions));
                }
                Err(_) => {
                    self.dirty(Some(generation.clone()));
                    failures = failures.saturating_add(1);
                    let delay = std::time::Duration::from_secs((1u64 << failures.min(5)).min(30));
                    tokio::select! { biased;
                        _ = generation.closed.cancelled() => { failures = 0; },
                        _ = tokio::time::sleep(delay) => {},
                    }
                }
            }
        }
    }
}

impl Reducer {
    /// Reduce one typed transition and replace the latest mailbox atomically with the snapshot.
    fn apply(&mut self, event: Transition) {
        let previous = self.state.clone();
        let signed_out = self.state.status.authorization == SessionAuthorization::SignedOut;
        match event {
            Transition::Network(generation, true)
                if !signed_out && generation.open.load(Ordering::SeqCst) =>
            {
                self.generation = Some(generation);
                self.state.status.network = SessionNetwork::Available;
                self.state.status.authorization = SessionAuthorization::Unknown;
                self.state.sessions = None;
                self.dirty = true;
                self.demand = self.demand.saturating_add(1);
            }
            Transition::Network(generation, false) if self.matches(&generation) => {
                self.state.status.network = SessionNetwork::Unavailable;
                if !signed_out {
                    self.state.status.authorization = SessionAuthorization::Unknown;
                }
                self.state.sessions = None;
            }
            Transition::Authentication(known) if !signed_out => {
                self.state.status.authorization = if known
                    && self
                        .generation
                        .as_ref()
                        .is_some_and(|g| g.returning && g.open.load(Ordering::SeqCst))
                {
                    SessionAuthorization::Active
                } else {
                    SessionAuthorization::Unknown
                };
            }
            Transition::Denial(generation) if !signed_out && self.matches(&generation) => {
                self.state.status.authorization = SessionAuthorization::Unknown;
                self.state.sessions = None;
                self.dirty = true;
                self.demand = self.demand.saturating_add(1);
            }
            Transition::SignedOut(reason) => {
                self.state.reason.get_or_insert(reason);
                self.state.status.authorization = SessionAuthorization::SignedOut;
                self.state.status.network = SessionNetwork::Unavailable;
                self.state.sessions = None;
                self.dirty = false;
            }
            Transition::Cleanup(status) => self.state.status.cleanup = status,
            Transition::Store(safe) => self.state.status.restart_safe_store = safe,
            Transition::Observation(value) => self.state.status.enrollment_observation = value,
            Transition::Dirty(generation)
                if !signed_out && generation.as_ref().is_none_or(|g| self.matches(g)) =>
            {
                self.dirty = true;
                self.demand = self.demand.saturating_add(1);
            }
            Transition::Review(generation, demand, sessions)
                if !signed_out
                    && self.matches(&generation)
                    && generation.open.load(Ordering::SeqCst)
                    && self.state.status.authorization == SessionAuthorization::Active =>
            {
                // A newer hint/confirmation fences this old snapshot; refetch instead.
                if self.demand == demand {
                    self.state.sessions = Some(sessions);
                } else {
                    self.dirty = true;
                }
            }
            _ => {}
        }
        if self.state != previous {
            self.state.revision = self.state.revision.saturating_add(1);
            if let Some(consumer) = self
                .consumer
                .upgrade()
                .filter(|c| !c.stopped.is_cancelled())
            {
                *consumer.latest.lock().unwrap_or_else(|e| e.into_inner()) =
                    Some(self.state.clone());
                consumer.changed.notify_one();
            }
        }
    }
    /// Arc identity fences wraparound and successor connections using the same enrollment.
    fn matches(&self, generation: &Arc<LiveGeneration>) -> bool {
        self.generation
            .as_ref()
            .is_some_and(|g| Arc::ptr_eq(g, generation))
    }
}

impl Client {
    /// Inspect the same serialized authoritative view delivered to the app consumer.
    pub async fn session_status(&self) -> SessionStatus {
        self.session_state.status()
    }
    /// Register one application consumer, atomically including the latest retained snapshot.
    pub async fn subscribe_session_state(&self) -> Result<SessionStateSubscription, TinkleError> {
        self.session_state.subscribe(self.owner_gone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Enter one synthetic Returning generation without constructing an authorization key.
    fn active(owner: &SessionCoordinator) -> Arc<LiveGeneration> {
        let generation = LiveGeneration::new(true, 0);
        owner.network(generation.clone(), true);
        owner.authentication(true);
        generation
    }

    #[tokio::test]
    /// Concurrent producers have one monotonic owner; a slow consumer cannot delay a terminal cause.
    async fn concurrent_producers_retain_terminal_state_and_first_cause() {
        let coordinator = SessionCoordinator::new(false);
        let generation = active(&coordinator);
        let stream = coordinator.subscribe(CancelToken::new()).unwrap();
        let mut threads = Vec::new();
        for _ in 0..8 {
            let owner = coordinator.clone();
            let generation = generation.clone();
            threads.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    owner.dirty(Some(generation.clone()));
                    owner.authentication(true);
                    owner.store(true);
                }
            }));
        }
        coordinator.signed_out(SessionSignOutReason::SessionRetired);
        coordinator.cleanup(SessionCleanup::Failed);
        for thread in threads {
            thread.join().unwrap();
        }
        coordinator.signed_out(SessionSignOutReason::SelfLogout);
        coordinator.cleanup(SessionCleanup::Pending);
        coordinator.cleanup(SessionCleanup::Complete);
        let state = stream.next().await.unwrap();
        assert_eq!(state, coordinator.snapshot());
        assert_eq!(state.reason, Some(SessionSignOutReason::SessionRetired));
        assert_eq!(state.status.authorization, SessionAuthorization::SignedOut);
        assert_eq!(state.status.cleanup, SessionCleanup::Complete);
        assert!(state.revision >= 5);
    }

    #[test]
    /// A hint during a fetch and a late old-generation response cannot replace fresh review state.
    fn dirty_fetch_and_generation_fences_preserve_snapshot() {
        let owner = SessionCoordinator::new(false);
        let first = active(&owner);
        let demand = owner.owner.lock().unwrap().demand;
        owner.dirty(Some(first.clone()));
        owner.submit(Transition::Review(first.clone(), demand, vec![]));
        assert!(owner.snapshot().sessions.is_none());
        assert!(owner.owner.lock().unwrap().dirty);
        let second = active(&owner);
        let current = owner.owner.lock().unwrap().demand;
        owner.submit(Transition::Review(second.clone(), current, vec![]));
        assert_eq!(owner.snapshot().sessions, Some(vec![]));
        let before = owner.snapshot();
        owner.denial(first.clone());
        owner.network(first.clone(), false);
        owner.submit(Transition::Review(first, current, vec![Default::default()]));
        assert_eq!(owner.snapshot(), before);
        owner.signed_out(SessionSignOutReason::SelfLogout);
        owner.submit(Transition::Review(
            second,
            current,
            vec![Default::default()],
        ));
        assert!(owner.snapshot().sessions.is_none());
        assert_eq!(
            owner.snapshot().status.authorization,
            SessionAuthorization::SignedOut
        );
    }

    #[tokio::test]
    /// Denial withdraws the list, retains refresh demand, and fences reads begun before denial.
    async fn denial_invalidates_review_and_preserves_refresh_demand() {
        let owner = SessionCoordinator::new(false);
        let generation = active(&owner);
        let demand = owner.owner.lock().unwrap().demand;
        let sessions = vec![tinkle_schema::proto::Session {
            id: "78e5c289-656b-4428-a091-2dddf6828465".into(),
            ..Default::default()
        }];
        owner.submit(Transition::Review(
            generation.clone(),
            demand,
            sessions.clone(),
        ));
        let stream = owner.subscribe(CancelToken::new()).unwrap();
        let before = stream.next().await.unwrap();
        assert_eq!(before.sessions, Some(sessions.clone()));
        // Model the review worker having consumed the current fetch demand.
        owner.owner.lock().unwrap().dirty = false;

        owner.denial(generation.clone());
        let denied = stream.next().await.unwrap();
        assert_eq!(denied.status.authorization, SessionAuthorization::Unknown);
        assert!(denied.sessions.is_none());
        assert!(denied.revision > before.revision);
        assert_eq!(denied.status.network, SessionNetwork::Available);
        assert_eq!(denied.status.cleanup, SessionCleanup::NotStarted);
        assert_eq!(denied.reason, None);
        {
            let reducer = owner.owner.lock().unwrap();
            assert!(reducer.dirty);
            assert!(reducer.demand > demand);
        }
        owner.submit(Transition::Review(
            generation.clone(),
            demand,
            sessions.clone(),
        ));
        assert_eq!(owner.snapshot(), denied);
        owner.authentication(true);
        owner.submit(Transition::Review(
            generation.clone(),
            demand,
            sessions.clone(),
        ));
        assert!(owner.snapshot().sessions.is_none());
        let current = owner.owner.lock().unwrap().demand;
        owner.submit(Transition::Review(generation, current, sessions.clone()));
        assert_eq!(owner.snapshot().sessions, Some(sessions));
    }

    #[tokio::test]
    /// Saturated ingress and an unread consumer mailbox cannot lose final cleanup state.
    async fn full_ingress_drains_in_order_without_dropping_terminal_state() {
        let owner = SessionCoordinator::new(false);
        active(&owner);
        let stream = owner.subscribe(CancelToken::new()).unwrap();
        assert!(owner
            .tx
            .try_send(Transition::Cleanup(SessionCleanup::Pending))
            .is_ok());
        owner.cleanup(SessionCleanup::Complete);
        owner.signed_out(SessionSignOutReason::SessionRetired);
        let state = stream.next().await.unwrap();
        assert_eq!(state.status.cleanup, SessionCleanup::Complete);
        assert_eq!(state.status.authorization, SessionAuthorization::SignedOut);
        assert_eq!(state.reason, Some(SessionSignOutReason::SessionRetired));
    }
}
