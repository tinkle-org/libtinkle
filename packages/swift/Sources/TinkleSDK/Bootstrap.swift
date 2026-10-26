// Staging copies the matching canonical bindings/swift/TinkleSDK.swift into
// this directory in a disposable package. Keep that file generated and untracked
// in libtinkle; the published distribution includes its release-matched copy.
#if !os(iOS)
#error("TinkleSDK currently supports iOS devices and iOS simulators only.")
#endif
