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
    /// gRPC base endpoint for `ReadUpdates` catch-up.
    pub grpc_endpoint: Option<String>,
}

impl From<ClientConfigFfi> for libtinkle::ClientConfig {
    fn from(c: ClientConfigFfi) -> Self {
        let mut cfg = Self::new(c.url);
        if c.allow_insecure {
            cfg = cfg.insecure_dev();
        }
        cfg.user_agent = c.user_agent;
        if let Some(ep) = c.grpc_endpoint {
            // Blank means unset: an empty/whitespace endpoint would
            // enable catch-up that can never succeed. `From` cannot
            // fail, so an invalid non-blank endpoint is likewise left
            // unset (client falls back to follow-only); fallible
            // callers should use `validate_grpc_endpoint` +
            // `with_grpc_endpoint` to fail fast instead.
            let ep = ep.trim().to_string();
            if !ep.is_empty()
                && libtinkle::ClientConfig::validate_grpc_endpoint(&ep, cfg.allow_insecure).is_ok()
            {
                cfg.grpc_endpoint = Some(ep);
            }
        }
        cfg
    }
}
