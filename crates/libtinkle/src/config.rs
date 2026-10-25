//! Client configuration + credential handling.
//!
//! Credentials are opaque secret bytes presented only as
//! `ClientHello.credential`. They are redacted from `Debug`, never
//! logged, never placed in URLs, never persisted by this crate, and
//! zeroized on drop.

use std::time::Duration;
use zeroize::{Zeroize, ZeroizeOnDrop};

use syntra_runtime::ConnectionConfig;

use crate::errors::TinkleError;

/// Opaque credential (e.g. session token bytes). `Debug` is redacted.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct Credential {
    bytes: Vec<u8>,
}

impl Credential {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub fn from_session_token(token: &str) -> Self {
        Self {
            bytes: token.as_bytes().to_vec(),
        }
    }

    /// Borrow secret bytes for the handshake only. Callers MUST NOT log
    /// or retain the returned slice beyond the handshake.
    /// crate-internal: only the connection engine may touch secrets.
    pub(crate) fn expose_secret(&self) -> &[u8] {
        &self.bytes
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credential([redacted])")
    }
}

/// Reconnect policy: bounded exponential backoff with jitter.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Initial delay after first loss.
    pub base_delay: Duration,
    /// Maximum delay cap.
    pub max_delay: Duration,
    /// Maximum reconnect attempts (None = unbounded; callers SHOULD bound).
    pub max_attempts: Option<u32>,
    /// Multiplier per attempt.
    pub multiplier: f64,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            max_attempts: Some(10),
            multiplier: 2.0,
        }
    }
}

impl ReconnectPolicy {
    pub(crate) fn delay_for(&self, attempt: u32) -> Duration {
        let mut d = self.base_delay.as_millis() as f64 * self.multiplier.powi(attempt as i32);
        let max = self.max_delay.as_millis() as f64;
        if d > max {
            d = max;
        }
        // Full jitter: random in [d/2, d].
        let jitter: f64 = rand::random::<f64>() * 0.5 + 0.5;
        Duration::from_millis((d * jitter) as u64)
    }
}

/// High-level client configuration.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// gRPC base endpoint for `ReadUpdates` catch-up, e.g. `https://gateway.example.com`.
    /// When `None`, catch-up must go through the follow binding only.
    /// Prefer `with_grpc_endpoint` over direct assignment: the builder
    /// validates scheme/`https` up front, while a directly assigned
    /// value is only checked when catch-up runs.
    pub grpc_endpoint: Option<String>,
    /// WSS URL, e.g. `wss://gateway.example.com/syntra/v1/ws`.
    pub url: String,
    /// Allow `ws://` (development loopback/isolated nets only).
    pub allow_insecure: bool,
    /// Syntra connection tuning.
    pub connection: ConnectionConfig,
    /// Reconnect tuning.
    pub reconnect: ReconnectPolicy,
    /// Default per-RPC timeout_ms when callers pass 0 is
    /// `connection.default_timeout` (bounded, documented).
    pub user_agent: Option<String>,
}

impl ClientConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            grpc_endpoint: None,
            url: url.into(),
            allow_insecure: false,
            connection: ConnectionConfig::default(),
            reconnect: ReconnectPolicy::default(),
            user_agent: None,
        }
    }

    /// Validate a gRPC catch-up endpoint without storing it. Shared by
    /// the builder and fallible constructors so misconfig fails fast
    /// with `GrpcConfig` (FailedPrecondition) instead of surfacing as a
    /// retried `Grpc` (Unavailable) at catch-up time.
    pub fn validate_grpc_endpoint(endpoint: &str, allow_insecure: bool) -> Result<(), TinkleError> {
        if endpoint.trim().is_empty() {
            return Err(TinkleError::GrpcConfig(
                "grpc_endpoint must not be blank".into(),
            ));
        }
        let lower = endpoint.to_ascii_lowercase();
        if allow_insecure {
            if !(lower.starts_with("https://") || lower.starts_with("http://")) {
                return Err(TinkleError::GrpcConfig(
                    "grpc_endpoint must include http:// or https:// scheme".into(),
                ));
            }
        } else if !lower.starts_with("https://") {
            return Err(TinkleError::GrpcConfig(
                "grpc_endpoint must use https:// unless allow_insecure".into(),
            ));
        }
        Ok(())
    }

    /// gRPC endpoint for catch-up (`ReadUpdates` over actual gRPC).
    ///
    /// Fails fast: blank endpoints, missing schemes, and non-`https`
    /// endpoints without `allow_insecure` return `GrpcConfig`
    /// (FailedPrecondition) here instead of silently degrading to
    /// follow-only at subscribe time. Call after `insecure_dev` when a
    /// dev `http://` endpoint is intended.
    pub fn with_grpc_endpoint(
        mut self,
        endpoint: impl Into<String>,
    ) -> Result<Self, TinkleError> {
        let endpoint = endpoint.into();
        Self::validate_grpc_endpoint(&endpoint, self.allow_insecure)?;
        self.grpc_endpoint = Some(endpoint);
        Ok(self)
    }

    /// Development-only plaintext. Production MUST use `wss://`.
    pub fn insecure_dev(mut self) -> Self {
        self.allow_insecure = true;
        self
    }

    pub(crate) fn ws_config(&self) -> syntra_ws::WsConfig {
        let mut cfg = if self.allow_insecure {
            syntra_ws::WsConfig::insecure_dev(self.url.clone())
        } else {
            syntra_ws::WsConfig::secure(self.url.clone())
        };
        cfg.connect_timeout = Duration::from_secs(10);
        cfg.close_grace = Duration::from_secs(2);
        cfg
    }
}
