//! Example host adapter: restore an ACTIVE opaque record through a platform
//! secure-store helper executable. No plaintext key file or command-line key.
//!
//! Both `send-message` and `subscribe` require `TINKLE_HOST` (bare host),
//! `TINKLE_SERVER_PIN` (64 hex characters), `TINKLE_SERVER_KEY_ID`, `TINKLE_SCOPE`,
//! `TINKLE_DEVICE_MODEL`, `TINKLE_PLATFORM`, `TINKLE_OS_VERSION`,
//! `TINKLE_APP_VERSION`, `TINKLE_APP_BUILD`, and `TINKLE_SECURE_STORE_HELPER`.
//! Optional `TINKLE_PORT` is a nonzero u16 TCP port and disables fallback dialing.
//! Without it, the SDK defaults to TCP 443 with TCP 80 as dial-failure fallback.
use libtinkle::{Client, ClientConfig, InMemoryCursorStore, ServerTrust, TrustSource};
use zeroize::Zeroize;

struct PlatformStore(String);
#[async_trait::async_trait]
impl libtinkle::SecureEnrollmentStore for PlatformStore {
    async fn clear(&self) -> Result<(), libtinkle::SecureStoreError> {
        let output = tokio::process::Command::new(&self.0)
            .arg("clear")
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|_| libtinkle::SecureStoreError)?;
        let _stdout = libtinkle::SecretBlob::new(output.stdout);
        let _stderr = libtinkle::SecretBlob::new(output.stderr);
        if output.status.success() {
            Ok(())
        } else {
            Err(libtinkle::SecureStoreError)
        }
    }
    async fn commit(
        &self,
        _: libtinkle::SecureWrite,
        _: &str,
        _: &libtinkle::SecretBlob,
    ) -> Result<(), libtinkle::SecureStoreError> {
        Err(libtinkle::SecureStoreError) // These examples only restore; never acknowledge a write.
    }
    async fn load_operation(
        &self,
        _: &str,
    ) -> Result<Option<libtinkle::SecretBlob>, libtinkle::SecureStoreError> {
        Err(libtinkle::SecureStoreError)
    }
    async fn load_active(
        &self,
    ) -> Result<Option<libtinkle::SecretBlob>, libtinkle::SecureStoreError> {
        let mut output = tokio::process::Command::new(&self.0)
            .arg("load-active")
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|_| libtinkle::SecureStoreError)?;
        output.stderr.zeroize();
        let record = libtinkle::SecretBlob::new(output.stdout);
        if !output.status.success() {
            return Err(libtinkle::SecureStoreError);
        }
        Ok(Some(record))
    }
}

pub async fn client() -> anyhow::Result<Client> {
    let pin = std::env::var("TINKLE_SERVER_PIN")?;
    anyhow::ensure!(
        pin.len() == 64 && pin.is_ascii(),
        "server pin must be 64 hex characters"
    );
    let mut server_public_key = [0; 32];
    for (i, byte) in server_public_key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&pin[i * 2..i * 2 + 2], 16)?;
    }
    let trust = ServerTrust {
        server_public_key,
        server_key_id: std::env::var("TINKLE_SERVER_KEY_ID")?.parse()?,
        scope: std::env::var("TINKLE_SCOPE")?,
    };
    trust.validate()?;
    let mut config = ClientConfig::new(
        std::env::var("TINKLE_HOST")?,
        libtinkle::ClientInfo::new(
            std::env::var("TINKLE_DEVICE_MODEL")?,
            std::env::var("TINKLE_PLATFORM")?,
            std::env::var("TINKLE_OS_VERSION")?,
            std::env::var("TINKLE_APP_VERSION")?,
            std::env::var("TINKLE_APP_BUILD")?,
            Default::default(),
        )?,
    );
    match std::env::var("TINKLE_PORT") {
        Ok(port) => {
            config.primary_port = port.parse::<std::num::NonZeroU16>()?.get();
            config.fallback_port = None;
        }
        Err(std::env::VarError::NotPresent) => {}
        Err(error) => return Err(error.into()),
    }
    let client = Client::new(
        config,
        TrustSource::Bootstrap(trust),
        InMemoryCursorStore::new(),
    );
    client
        .restore_active_enrollment(std::sync::Arc::new(PlatformStore(std::env::var(
            "TINKLE_SECURE_STORE_HELPER",
        )?)))
        .await?;
    Ok(client)
}
