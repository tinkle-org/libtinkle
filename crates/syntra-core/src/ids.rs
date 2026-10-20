//! Request-id allocation (`SNTR-RPC-002`, clarified in rpc.md §1B).
//!
//! `request_id` is the `uint64` correlation field of the enclosing
//! `Frame`, not a field of `RpcRequest`. It is initiator-chosen,
//! nonzero for RPC, unique among *outstanding* RPCs on that connection
//! only, reusable after terminal `result`, and dies with the connection.
//! It MUST NOT be used for cross-connection deduplication; durable
//! continuity uses the application operation key in the payload.

use std::sync::atomic::{AtomicU64, Ordering};

/// Allocates nonzero ids unique among outstanding RPCs.
///
/// The allocator hands out monotonically increasing ids (wrapping).
/// Callers MUST check the pending table and skip ids still in use
/// (see `PendingTable::is_pending`), because reuse-after-completion is
/// permitted but duplicate-*outstanding* is rejected with
/// `FAILED_PRECONDITION`.
#[derive(Debug, Default)]
pub struct RequestIdAllocator {
    next: AtomicU64,
}

impl RequestIdAllocator {
    pub fn new() -> Self {
        // Start at 1; 0 is never valid for RPC.
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Return the next candidate id (nonzero). The caller must verify
    /// it is not currently outstanding and retry on collision (wraparound).
    pub fn next_candidate(&self) -> u64 {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        // fetch_add returns the previous value; skip 0 on wrap.
        if id == 0 {
            self.next.fetch_add(1, Ordering::Relaxed)
        } else {
            id
        }
    }

    /// Reset the id space. Called only when the connection dies;
    /// the id space dies with the connection (SNTR-RPL-020).
    pub fn reset(&self) {
        self.next.store(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_zero() {
        let a = RequestIdAllocator::new();
        for _ in 0..10_000 {
            assert_ne!(a.next_candidate(), 0);
        }
    }
}
