//! FFI-safe client configuration.

/// Record mirrored into [`libtinkle::ClientConfig`]. All fields owned.
#[derive(Debug, Clone, uniffi::Record)]
pub struct ClientConfigFfi {
    /// Gateway URL, e.g. `wss://gateway.example.com/syntra/v1/ws`.
    pub url: String,
    /// Allow `ws://` (development loopback/isolated nets only).
    pub allow_insecure: bool,
    /// Optional user-agent string.
    pub user_agent: Option<String>,
}

impl From<ClientConfigFfi> for libtinkle::ClientConfig {
    fn from(c: ClientConfigFfi) -> Self {
        let mut cfg = Self::new(c.url);
        if c.allow_insecure {
            cfg = cfg.insecure_dev();
        }
        cfg.user_agent = c.user_agent;
        cfg
    }
}
