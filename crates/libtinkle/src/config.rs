//! Native TCP endpoints and caller-owned server/enrollment trust inputs.

use std::time::Duration;
use syntra_core::secure_channel::{self, Carrier, Context, Mode, Secret32};
use syntra_runtime::ConnectionConfig;
use zeroize::Zeroize;

use crate::TinkleError;

/// Public server identity provisioned by the host through a trusted channel.
/// Scope bytes are exact UTF-8; no normalization or endpoint-derived trust.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerTrust {
    /// Provisioned X25519 server public-key pin (32 bytes).
    pub server_public_key: [u8; 32],
    /// Server-selected key identifier bound into the channel context.
    pub server_key_id: u32,
    /// Exact application routing/security scope, 1..=255 UTF-8 bytes.
    pub scope: String,
}

impl ServerTrust {
    /// Validate the pin and exact scope before any network I/O.
    pub fn validate(&self) -> Result<(), TinkleError> {
        if self.server_public_key == [0; 32] || self.scope.is_empty() || self.scope.len() > 255 {
            return Err(TinkleError::Configuration(
                "nonzero server pin and 1..=255 byte scope required".into(),
            ));
        }
        Ok(())
    }
}

/// Returned authorization enrollment. The host stores this separately in its
/// protected secret store, never in the application cache/cursor database.
/// Not Clone; every temporary key copy used by Syntra is zeroized on drop.
pub struct Enrollment {
    authorization_key: std::sync::Arc<crate::authorization::SecretCell>,
    handle: [u8; 8],
    epoch: u64,
}

impl Enrollment {
    /// Share the revocable key cell with the owning authorization coordinator.
    pub(crate) fn secret(&self) -> std::sync::Arc<crate::authorization::SecretCell> {
        self.authorization_key.clone()
    }
    /// Return the public handle and epoch identifying this installed enrollment.
    pub(crate) fn binding(&self) -> ([u8; 8], u64) {
        (self.handle, self.epoch)
    }
    /// Adopt host-provisioned ACTIVE material. STAGED/unknown/expired status is
    /// rejected; normal enrollment obtains this only after Bootstrap activation.
    pub fn from_active_status(
        authorization_key: [u8; 32],
        status: &tinkle_schema::proto::DeviceEnrollmentStatus,
    ) -> Result<Self, TinkleError> {
        let authorization_key = zeroize::Zeroizing::new(authorization_key);
        crate::enrollment::validate_status(status)?;
        if status.state != tinkle_schema::proto::DeviceEnrollmentState::Active as i32 {
            return Err(TinkleError::Configuration(
                "Returning requires ACTIVE enrollment".into(),
            ));
        }
        Self::new(
            *authorization_key,
            status
                .handle
                .as_ref()
                .try_into()
                .map_err(|_| TinkleError::Codec("invalid active handle".into()))?,
            status.enrollment_epoch,
        )
    }

    /// Retain nonzero key, handle, and epoch values in a revocable zeroizing owner.
    pub(crate) fn new(
        authorization_key: [u8; 32],
        handle: [u8; 8],
        epoch: u64,
    ) -> Result<Self, TinkleError> {
        let authorization_key = zeroize::Zeroizing::new(authorization_key);
        if *authorization_key == [0; 32] || handle == [0; 8] || epoch == 0 {
            return Err(TinkleError::Configuration(
                "nonzero enrollment key, handle and epoch required".into(),
            ));
        }
        let enrollment = Self {
            authorization_key: crate::authorization::SecretCell::new(authorization_key),
            handle,
            epoch,
        };
        Ok(enrollment)
    }
}
impl Drop for Enrollment {
    /// Revoke shared key access and erase this owner's handle and epoch.
    fn drop(&mut self) {
        self.authorization_key.retire();
        self.handle.zeroize();
        self.epoch.zeroize();
    }
}

impl std::fmt::Debug for Enrollment {
    /// Redact all enrollment material from diagnostic formatting.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Enrollment([redacted])")
    }
}

/// Explicit channel mode and trust, supplied from host provisioning/AK storage.
/// Bootstrap never silently replaces a rejected Returning enrollment.
#[derive(Debug)]
pub enum TrustSource {
    /// Public trust only; never an automatic Bootstrap fallback after logout.
    SignedOut(ServerTrust),
    /// Pinned server proof without authorization-key possession.
    Bootstrap(ServerTrust),
    /// Pinned server proof plus possession of a returned enrollment key.
    Returning {
        /// Public identity to which the host bound this enrollment.
        trust: ServerTrust,
        /// Protected enrollment material loaded by the host.
        enrollment: Enrollment,
    },
}

impl TrustSource {
    /// Build exact TCP channel context, rejecting sign-out and retired Returning keys.
    pub(crate) fn security(&self) -> Result<secure_channel::ClientConfig, TinkleError> {
        let (trust, mode, handle, epoch, authorization_key) = match self {
            Self::SignedOut(_) => return Err(TinkleError::SignedOut),
            Self::Bootstrap(trust) => (trust, Mode::Bootstrap, [0; 8], 0, None),
            Self::Returning { trust, enrollment } => (
                trust,
                Mode::Returning,
                enrollment.handle,
                enrollment.epoch,
                Some(Secret32::new(*enrollment.authorization_key.snapshot()?)),
            ),
        };
        trust.validate()?;
        Ok(secure_channel::ClientConfig {
            context: Context {
                mode,
                carrier: Carrier::Tcp,
                server_key_id: trust.server_key_id,
                scope: trust.scope.as_bytes().to_vec(),
                handle,
                epoch,
            },
            server_public_key: trust.server_public_key,
            authorization_key,
        })
    }
}

/// Reconnect scheduling owned solely by the application supervisor.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Delay floor before jitter, sampled by the supervisor scheduler.
    pub base_delay: Duration,
    /// Maximum exponential delay before jitter.
    pub max_delay: Duration,
    /// Maximum consecutive failed attempts; None leaves retries unbounded.
    pub max_attempts: Option<u32>,
    /// Finite exponential multiplier, at least one.
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
        let d = (self.base_delay.as_secs_f64() * self.multiplier.powi(attempt.min(1024) as i32))
            .min(self.max_delay.as_secs_f64());
        Duration::from_secs_f64(d * (rand::random::<f64>() * 0.5 + 0.5))
    }
}

/// Native encrypted TCP configuration. Both ports use the same host and exact
/// trust source. Ports are configurable for deployments and ephemeral fixtures;
/// there is no plaintext, HTTP, TLS or trust-bypass option.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Complete immutable metadata required by ConnectionService.InitConnect.
    pub client_info: crate::ClientInfo,
    /// Bare DNS hostname or IP address without a scheme, path or port.
    pub host: String,
    /// Initial preferred TCP port, 443 by default.
    pub primary_port: u16,
    /// Initial dial-failure alternative, TCP 80 by default.
    pub fallback_port: Option<u16>,
    /// Published Syntra RPC/runtime limits.
    pub connection: ConnectionConfig,
    /// Scheduling policy for the single application supervisor.
    pub reconnect: ReconnectPolicy,
    /// Per-dial ceiling inside the shared 15-second establishment budget.
    pub connect_timeout: Duration,
}

impl ClientConfig {
    /// Default native route: TCP 443, then TCP 80 on dial failure only.
    pub fn new(host: impl Into<String>, client_info: crate::ClientInfo) -> Self {
        Self {
            client_info,
            host: host.into(),
            primary_port: 443,
            fallback_port: Some(80),
            connection: ConnectionConfig::default(),
            reconnect: ReconnectPolicy::default(),
            connect_timeout: Duration::from_secs(10),
        }
    }

    /// Reject invalid native routes/runtime limits before network I/O.
    pub fn validate(&self) -> Result<(), TinkleError> {
        if self.host.is_empty()
            || self.host.trim() != self.host
            || self.host.chars().any(char::is_whitespace)
            || (self.host.contains(':') && self.host.parse::<std::net::Ipv6Addr>().is_err())
            || self.host.contains(['/', '?', '#', '@'])
            || self.host.contains("://")
            || self.primary_port == 0
            || self.fallback_port == Some(0)
            || self.connect_timeout.is_zero()
            || self.connect_timeout > Duration::from_secs(15)
            || self.reconnect.base_delay.is_zero()
            || self.reconnect.max_delay < self.reconnect.base_delay
            || self.reconnect.max_delay > Duration::from_secs(86_400)
            || !self.reconnect.multiplier.is_finite()
            || self.reconnect.multiplier < 1.0
        {
            return Err(TinkleError::Configuration(
                "invalid native TCP configuration".into(),
            ));
        }
        self.connection
            .validate()
            .map_err(|_| TinkleError::Configuration("invalid connection limits".into()))
    }
}
