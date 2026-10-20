//! Pending-RPC table: correlation, bounded outstanding, cancellation.
//!
//! Each RPC uses a nonzero initiator-chosen `request_id` unique among
//! concurrent RPCs. Exactly one terminal `RpcResult` with the same id
//! completes the RPC. `cancel` for unknown/completed ids is ignored;
//! duplicate outstanding ids are rejected with `FAILED_PRECONDITION`;
//! `result` for an unknown id is ignored with a diagnostic (no error storm).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_channel::oneshot;

use crate::error::{ErrorCode, SyntraError};
use crate::proto::RpcResult;

/// Outcome delivered to one waiter.
#[derive(Debug)]
pub enum PendingOutcome {
    Result(RpcResult),
    TransportLost,
}

/// One outstanding RPC.
#[derive(Debug)]
struct Slot {
    tx: oneshot::Sender<PendingOutcome>,
    method: String,
}

/// Bounded correlation table.
#[derive(Debug, Default)]
pub struct PendingTable {
    inner: Mutex<HashMap<u64, Slot>>,
    max_outstanding: usize,
}

impl PendingTable {
    pub fn new(max_outstanding: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            max_outstanding,
        })
    }

    /// Register a new RPC. Rejects duplicate outstanding ids and excess
    /// concurrency (`RESOURCE_EXHAUSTED` / `FAILED_PRECONDITION`).
    pub fn insert(
        self: &Arc<Self>,
        id: u64,
        method: String,
    ) -> Result<oneshot::Receiver<PendingOutcome>, SyntraError> {
        let mut g = self.inner.lock().unwrap();
        if g.contains_key(&id) {
            return Err(SyntraError::Protocol {
                code: ErrorCode::FailedPrecondition,
                message: "duplicate outstanding request_id".to_string(),
            });
        }
        if g.len() >= self.max_outstanding {
            return Err(SyntraError::ResourceExhausted(
                "too many outstanding RPCs".to_string(),
            ));
        }
        let (tx, rx) = oneshot::channel();
        g.insert(id, Slot { tx, method });
        Ok(rx)
    }

    pub fn is_pending(&self, id: u64) -> bool {
        self.inner.lock().unwrap().contains_key(&id)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }

    /// Complete one RPC with its terminal `result`. Unknown ids are
    /// ignored with a diagnostic (no error storm). Returns true if a
    /// waiter existed.
    pub fn complete(&self, id: u64, result: RpcResult) -> bool {
        let slot = self.inner.lock().unwrap().remove(&id);
        match slot {
            Some(s) => {
                let _ = s.tx.send(PendingOutcome::Result(result));
                true
            }
            None => {
                tracing::debug!(request_id = id, "result for unknown request_id; ignoring");
                false
            }
        }
    }

    /// Remove one waiter without completing (deadline/cancel-abandon).
    /// A late `result` afterwards is ignored as unknown (completion wins
    /// races; abandonment does not cancel the host without `cancel`).
    pub fn abandon(&self, id: u64) {
        self.inner.lock().unwrap().remove(&id);
    }

    /// Drain all waiters on transport loss/close. Each gets
    /// `TransportLost`, which the connection maps to `UNAVAILABLE`
    /// with unknown outcome (SNTR-REL-009).
    pub fn drain_transport_lost(&self) {
        let slots: Vec<Slot> = {
            let mut g = self.inner.lock().unwrap();
            g.drain().map(|(_, s)| s).collect()
        };
        for s in slots {
            let _ = s.tx.send(PendingOutcome::TransportLost);
        }
    }

    #[allow(dead_code)]
    pub fn method_of(&self, id: u64) -> Option<String> {
        self.inner
            .lock()
            .unwrap()
            .get(&id)
            .map(|s| s.method.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_outstanding() {
        let t = PendingTable::new(16);
        let _rx = t.insert(7, "/a/B/C".into()).unwrap();
        assert!(t.insert(7, "/a/B/C".into()).is_err());
    }

    #[test]
    fn enforces_bound() {
        let t = PendingTable::new(1);
        let _rx = t.insert(1, "/a/B/C".into()).unwrap();
        assert!(matches!(
            t.insert(2, "/a/B/C".into()),
            Err(SyntraError::ResourceExhausted(_))
        ));
    }
}
