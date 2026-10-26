package com.tinkle.sdk

/** Entry point for the shared engine. Call before using any generated API. */
public object TinkleSdk {
    private val initialized: Unit by lazy {
        // UniFFI 0.32.2 does not automatically call this for a single-crate binding.
        // It checks contract/function checksums and registers foreign callbacks.
        uniffiEnsureInitialized()
        tinkleFfiInit()
    }

    /** Thread-safe and idempotent; fails immediately on a bindings/library mismatch. */
    @JvmStatic
    public fun initialize() {
        initialized
    }
}
