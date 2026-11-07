//! Immutable, validated client snapshot for per-connection InitConnect.
use crate::TinkleError;

/// Optional descriptive labels; omitted values replace previous labels with unknown.
#[derive(Debug, Clone, Default)]
pub struct ClientOptions {
    pub agent: String,
    pub device_id: String,
    pub install_id: String,
    pub accept_language: String,
    pub time_zone: String,
    pub user_agent: String,
}

/// Complete client snapshot. All fields are validated before a connection can
/// use them. A Client owns an immutable copy; changing labels requires a new Client.
#[derive(Debug, Clone)]
pub struct ClientInfo(tinkle_schema::proto::ClientInfo);

fn label(raw: String, maximum: usize, required: bool) -> Result<String, TinkleError> {
    if !raw.bytes().all(|byte| (0x20..=0x7e).contains(&byte)) {
        return Err(TinkleError::Configuration(
            "client labels must be printable ASCII".into(),
        ));
    }
    let value = raw.trim_matches(' ');
    if value.len() > maximum || (required && value.is_empty()) {
        return Err(TinkleError::Configuration(
            "client label missing or exceeds its byte limit".into(),
        ));
    }
    Ok(value.to_owned())
}

impl ClientInfo {
    /// Supply all five required device/app labels and an entire optional snapshot.
    pub fn new(
        device_model: String,
        platform: String,
        os_version: String,
        app_version: String,
        app_build: String,
        options: ClientOptions,
    ) -> Result<Self, TinkleError> {
        Ok(Self(tinkle_schema::proto::ClientInfo {
            device_model: label(device_model, 128, true)?,
            platform: label(platform, 32, true)?.to_ascii_lowercase(),
            os_version: label(os_version, 64, true)?,
            app_version: label(app_version, 64, true)?,
            app_build: label(app_build, 64, true)?,
            agent: label(options.agent, 256, false)?,
            device_id: label(options.device_id, 128, false)?,
            install_id: label(options.install_id, 128, false)?,
            accept_language: label(options.accept_language, 64, false)?,
            time_zone: label(options.time_zone, 64, false)?,
            user_agent: label(options.user_agent, 1024, false)?,
        }))
    }

    pub(crate) fn proto(&self) -> tinkle_schema::proto::ClientInfo {
        self.0.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn labels_validate_raw_ascii_before_trimming_and_preserve_build_identifiers() {
        for bad in ["", "   ", "\tios", "ios\n", "é"] {
            assert!(ClientInfo::new(
                "phone".into(),
                bad.into(),
                "18".into(),
                "1".into(),
                "00142".into(),
                Default::default()
            )
            .is_err());
        }
        let info = ClientInfo::new(
            " phone ".into(),
            " IOS ".into(),
            "18".into(),
            "1".into(),
            "00142".into(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(info.proto().platform, "ios");
        assert_eq!(info.proto().app_build, "00142");
        assert!(ClientInfo::new(
            "x".repeat(129),
            "ios".into(),
            "18".into(),
            "1".into(),
            "1".into(),
            Default::default()
        )
        .is_err());
    }
}
