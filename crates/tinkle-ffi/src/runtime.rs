//! Process-wide Tokio runtime for the FFI layer.
//!
//! `libtinkle` spawns background tasks (`tokio::spawn` for the
//! notification pump) and relies on Tokio timers. Foreign callers drive
//! our async methods on their own executors, so we keep one shared
//! multi-thread runtime alive for the process. The host app calls
//! [`tinkle_ffi_init`] once at startup (and [`init`] is also invoked
//! lazily from [`crate::TinkleClient::new`], so construction never runs
//! without a runtime in place).

use std::sync::OnceLock;

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn build() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("tinkle-ffi")
        .build()
        .expect("tinkle-ffi: failed to build process-wide Tokio runtime")
}

/// Ensure the process-wide runtime exists. Idempotent; cheap after the
/// first call.
pub fn init() {
    let _ = RUNTIME.get_or_init(build);
}

/// Foreign-callable init. Swift/Kotlin apps call this once at startup.
#[uniffi::export]
pub fn tinkle_ffi_init() {
    init();
}
