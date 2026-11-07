//! Native TCP configuration and one-use secret trust objects for FFI.

use crate::FfiError;
use std::sync::{Arc, Mutex};

/// Both ports use one host and the exact same pin, scope and channel mode.
#[derive(Debug, Clone, uniffi::Record)]
pub struct ClientConfigFfi {
    pub client_info: ClientInfoFfi,
    pub host: String,
    pub primary_port: u16,
    pub fallback_port: Option<u16>,
}

/// Entire optional descriptive snapshot; missing labels are explicitly empty.
#[derive(Debug, Clone, uniffi::Record)]
pub struct ClientOptionsFfi {
    pub agent: String,
    pub device_id: String,
    pub install_id: String,
    pub accept_language: String,
    pub time_zone: String,
    pub user_agent: String,
}

/// All five device/application labels are required before any native connection.
#[derive(Debug, Clone, uniffi::Record)]
pub struct ClientInfoFfi {
    pub device_model: String,
    pub platform: String,
    pub os_version: String,
    pub app_version: String,
    pub app_build: String,
    pub options: ClientOptionsFfi,
}

impl ClientInfoFfi {
    fn core(self) -> Result<libtinkle::ClientInfo, FfiError> {
        libtinkle::ClientInfo::new(
            self.device_model,
            self.platform,
            self.os_version,
            self.app_version,
            self.app_build,
            libtinkle::ClientOptions {
                agent: self.options.agent,
                device_id: self.options.device_id,
                install_id: self.options.install_id,
                accept_language: self.options.accept_language,
                time_zone: self.options.time_zone,
                user_agent: self.options.user_agent,
            },
        )
        .map_err(Into::into)
    }
}

/// Provisioned public server identity, independent of the transport address.
#[derive(Debug, Clone, uniffi::Record)]
pub struct ServerTrustFfi {
    pub server_public_key: Vec<u8>,
    pub server_key_id: u32,
    pub scope: String,
}

impl ServerTrustFfi {
    fn core(self) -> Result<libtinkle::ServerTrust, FfiError> {
        let trust = libtinkle::ServerTrust {
            server_public_key: self
                .server_public_key
                .try_into()
                .map_err(|_| FfiError::Internal("server pin must be 32 bytes".into()))?,
            server_key_id: self.server_key_id,
            scope: self.scope,
        };
        trust.validate().map_err(FfiError::from)?;
        Ok(trust)
    }
}

/// One-use Bootstrap trust owner. Returning is installed only through validated
/// ACTIVE secure-store restoration or the acknowledged enrollment coordinator.
#[derive(uniffi::Object)]
pub struct TrustSourceFfi(Mutex<Option<libtinkle::TrustSource>>);

#[uniffi::export]
impl TrustSourceFfi {
    #[uniffi::constructor]
    pub fn bootstrap(trust: ServerTrustFfi) -> Result<Arc<Self>, FfiError> {
        Ok(Arc::new(Self(Mutex::new(Some(
            libtinkle::TrustSource::Bootstrap(trust.core()?),
        )))))
    }
}

impl TrustSourceFfi {
    pub(crate) fn take(&self) -> Result<libtinkle::TrustSource, FfiError> {
        self.0
            .lock()
            .map_err(|_| FfiError::Internal("trust source unavailable".into()))?
            .take()
            .ok_or_else(|| FfiError::Internal("trust source already consumed".into()))
    }
}

pub(crate) fn validated_core_config(
    c: ClientConfigFfi,
) -> Result<libtinkle::ClientConfig, FfiError> {
    let mut cfg = libtinkle::ClientConfig::new(c.host, c.client_info.core()?);
    cfg.primary_port = c.primary_port;
    cfg.fallback_port = c.fallback_port;
    cfg.validate().map_err(FfiError::from)?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trust() -> ServerTrustFfi {
        ServerTrustFfi {
            server_public_key: vec![1; 32],
            server_key_id: 7,
            scope: "ffi-test".into(),
        }
    }

    #[test]
    fn trust_objects_transfer_ownership_once() {
        let source = TrustSourceFfi::bootstrap(trust()).unwrap();
        assert!(matches!(
            source.take().unwrap(),
            libtinkle::TrustSource::Bootstrap(_)
        ));
        assert!(source.take().is_err());
    }
}
