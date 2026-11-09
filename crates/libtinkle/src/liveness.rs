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

/// Public Client clones own this anchor; background callers do not retain it.
/// Dropping the last host owner cancels background work instead of forming a
/// Client → Live → task → Client retention cycle.
#[derive(Default)]
pub(crate) struct ClientOwner {
    pub(crate) gone: CancelToken,
}
impl Drop for ClientOwner {
    fn drop(&mut self) {
        self.gone.cancel();
    }
}

/// Local identity of exactly one socket/Core generation, never a credential.
pub(crate) struct LiveGeneration {
    pub(crate) open: AtomicBool,
    pub(crate) returning: bool,
    pub(crate) closed: CancelToken,
    pub(crate) classified: AtomicBool,
}
impl LiveGeneration {
    pub(crate) fn new(returning: bool) -> Arc<Self> {
        Arc::new(Self {
            open: AtomicBool::new(true),
            returning,
            closed: CancelToken::new(),
            classified: AtomicBool::new(false),
        })
    }
}

/// Abort safety and joined explicit retirement for auxiliary connection tasks.
pub(crate) struct AbortTask(Option<tokio::task::JoinHandle<()>>);
impl AbortTask {
    pub(crate) fn new(task: tokio::task::JoinHandle<()>) -> Self {
        Self(Some(task))
    }
    pub(crate) async fn shutdown(&mut self) {
        if let Some(task) = self.0.as_mut() {
            task.abort();
            let _ = task.await;
        }
        self.0 = None;
    }
}
impl Drop for AbortTask {
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

    pub(crate) async fn shutdown(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.as_mut() {
            let _ = task.await;
        }
        self.task = None;
    }
}
impl Drop for LivenessTask {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

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
        // The shared call path bounds local waits too. No dispatched heartbeat
        // means contention, not evidence that this connection failed.
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
