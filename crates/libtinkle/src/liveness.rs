//! Native Returning-only application liveness. Core transport ping is independent.
//! Each owned task issues one observed KeepAlive at a time, never replays a
//! failed sequence, and delegates recovery to the existing client supervisor.
use crate::{Client, TinkleError};
use prost::Message;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use syntra_runtime::CancelToken;

pub(crate) const KEEP_ALIVE_METHOD: &str = "/tinkle.v1.ConnectionService/KeepAlive";
pub(crate) const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// One bounded join attempt. rc.5 retains unfinished carrier/policy ownership in
/// the handle; neither an incomplete report nor an error permits another socket.
pub(crate) async fn transport_joined(handle: &mut syntra_net::tcp::ClientHandle) -> bool {
    handle.suspend();
    matches!(
        handle.shutdown_transport(Duration::from_secs(1)).await,
        Ok(syntra_net::TransportShutdown::Complete)
    )
}

/// Retain a pre-install/status handle through incomplete reports and cancelled join waiters.
pub(crate) async fn join_retained_transport(
    slot: &mut Option<syntra_net::tcp::ClientHandle>,
) -> bool {
    if let Some(handle) = slot.as_mut() {
        if !transport_joined(handle).await {
            return false;
        }
    }
    slot.take();
    true
}

/// Public Client clones own this anchor; background callers do not retain it.
/// Dropping the last host owner cancels background work instead of forming a
/// Client → Live → task → Client retention cycle.
#[derive(Default)]
pub(crate) struct ClientOwner {
    pub(crate) gone: CancelToken,
}
impl Drop for ClientOwner {
    /// Cancel background observers when the last host ownership anchor is released.
    fn drop(&mut self) {
        self.gone.cancel();
    }
}

/// Local identity of exactly one socket/Core generation, never a credential.
pub(crate) struct LiveGeneration {
    pub(crate) open: AtomicBool,
    /// Local lifecycle retirement must never masquerade as unexpected authenticated loss.
    pub(crate) retiring: AtomicBool,
    pub(crate) returning: bool,
    /// Local source/binding installation identity, distinct from the protocol enrollment epoch.
    pub(crate) source_revision: u64,
    pub(crate) closed: CancelToken,
    pub(crate) classified: AtomicBool,
    /// Sticky decision, retained even if observational teardown cancels this worker.
    pub(crate) terminal_failure: AtomicBool,
}
impl LiveGeneration {
    /// Allocate fresh open/closed fences with failure classification still pending.
    pub(crate) fn new(returning: bool, source_revision: u64) -> Arc<Self> {
        Arc::new(Self {
            open: AtomicBool::new(true),
            retiring: AtomicBool::new(false),
            returning,
            source_revision,
            closed: CancelToken::new(),
            classified: AtomicBool::new(false),
            terminal_failure: AtomicBool::new(false),
        })
    }

    /// Record terminal policy before any wake or await; a later carrier-loss result cannot undo it.
    pub(crate) fn record_failure(&self, error: &TinkleError) {
        if !matches!(error.recovery_decision(), crate::RecoveryDecision::Retry) {
            self.terminal_failure.store(true, Ordering::SeqCst);
        }
    }
}

/// Abort safety and joined explicit retirement for auxiliary connection tasks.
pub(crate) struct AbortTask(Option<tokio::task::JoinHandle<()>>);
impl AbortTask {
    /// Adopt an auxiliary task so dropping its owner cannot silently detach it.
    pub(crate) fn new(task: tokio::task::JoinHandle<()>) -> Self {
        Self(Some(task))
    }
    /// Abort and join while retaining the handle if the shutdown waiter is cancelled.
    pub(crate) async fn shutdown(&mut self) {
        if let Some(task) = self.0.as_mut() {
            task.abort();
            let _ = task.await;
        }
        self.0 = None;
    }
}
impl Drop for AbortTask {
    /// Abort any task that explicit shutdown has not yet joined.
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

/// Live-owned task. Explicit lifecycle retirement cancels and joins; Drop is
/// only a final abort safety net if the entire client owner is dropped.
pub(crate) struct LivenessTask {
    stop: CancelToken,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl LivenessTask {
    /// Run a selected failure under the same cancellable owner as the actual heartbeat loop.
    #[cfg(test)]
    pub(crate) fn spawn_failure(
        client: Client,
        generation: Arc<LiveGeneration>,
        error: TinkleError,
    ) -> Self {
        let stop = CancelToken::new();
        let stopped = stop.clone();
        let task = tokio::spawn(async move {
            tokio::select! { biased;
                _ = stopped.cancelled() => {},
                _ = client.fail_liveness(&generation, &error) => {},
            }
        });
        Self {
            stop,
            task: Some(task),
        }
    }
    /// Own one generation-bound loop, cancellable by lifecycle stop or loss of host ownership.
    pub(crate) fn spawn(
        client: Client,
        generation: Arc<LiveGeneration>,
        owner_gone: CancelToken,
    ) -> Self {
        let stop = CancelToken::new();
        let stopped = stop.clone();
        let task = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = stopped.cancelled() => {},
                _ = owner_gone.cancelled() => {},
                _ = run(client, generation) => {},
            }
        });
        Self {
            stop,
            task: Some(task),
        }
    }

    /// Cancel and join the local loop; this does not prove cancellation of a dispatched remote RPC.
    pub(crate) async fn shutdown(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.as_mut() {
            let _ = task.await;
        }
        self.task = None;
    }
}
impl Drop for LivenessTask {
    /// Cancel and abort as a final safety net when no lifecycle caller remains to join.
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Probe sequentially, deferring local admission barriers and retiring only the captured generation.
async fn run(client: Client, generation: Arc<LiveGeneration>) {
    let mut ticks = tokio::time::interval_at(
        tokio::time::Instant::now() + KEEP_ALIVE_INTERVAL,
        KEEP_ALIVE_INTERVAL,
    );
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut sequence = 0u64;
    loop {
        tokio::select! {
            biased;
            _ = generation.closed.cancelled() => {
                client.fail_liveness(&generation, &TinkleError::NetworkUnavailable).await;
                return;
            },
            _ = ticks.tick() => {},
        }
        if client.is_suspended().await {
            return;
        }
        if !generation.open.load(Ordering::SeqCst) {
            client
                .fail_liveness(&generation, &TinkleError::NetworkUnavailable)
                .await;
            return;
        }
        let Some(next) = sequence.checked_add(1) else {
            client
                .fail_liveness(
                    &generation,
                    &TinkleError::Configuration("KeepAlive sequence exhausted".into()),
                )
                .await;
            return;
        };
        sequence = next;
        let request = tinkle_schema::proto::KeepAliveRequest { sequence };
        // FIFO initialization admission does not spend the heartbeat RPC budget.
        // A restoration barrier defers admission; it is not transport failure.
        let Some(result) = client.call_keep_alive(request.encode_to_vec()).await else {
            continue;
        };
        let result = match result {
            Ok(bytes) => tinkle_schema::proto::KeepAliveResponse::decode(bytes.as_slice())
                .map_err(|_| TinkleError::Codec("invalid KeepAlive response".into()))
                .and_then(|response| {
                    if response.sequence == sequence {
                        Ok(())
                    } else {
                        Err(TinkleError::Codec("KeepAlive sequence mismatch".into()))
                    }
                }),
            Err(TinkleError::NotConnected(_)) => Err(TinkleError::NetworkUnavailable),
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            client.fail_liveness(&generation, &error).await;
            return;
        }
        if !generation.open.load(Ordering::SeqCst) {
            client
                .fail_liveness(&generation, &TinkleError::NetworkUnavailable)
                .await;
            return;
        }
    }
}
