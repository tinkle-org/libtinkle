//! Irreversible local sign-out fence and revocable SDK-owned secret cells.
use crate::{Client, SecureEnrollmentStore, TinkleError, TrustSource};
use std::sync::{Arc, Mutex, Weak};
use zeroize::Zeroizing;

/// Shared only by sealed owners; logout erases the cell even while a host
/// retains an operation/candidate object. No Clone or diagnostic secret export.
pub(crate) struct SecretCell(Mutex<Option<Zeroizing<[u8; 32]>>>);
impl SecretCell {
    pub(crate) fn new(bytes: Zeroizing<[u8; 32]>) -> Arc<Self> {
        Arc::new(Self(Mutex::new(Some(bytes))))
    }
    pub(crate) fn snapshot(&self) -> Result<Zeroizing<[u8; 32]>, TinkleError> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|v| Zeroizing::new(**v))
            .ok_or(TinkleError::SignedOut)
    }
    pub(crate) fn retire(&self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).take();
    }
}

/// One callback object bound to a durable namespace incarnation and revision.
struct StoreBinding {
    store: Arc<dyn SecureEnrollmentStore>,
    incarnation: Option<String>,
    revision: Option<String>,
}

struct State {
    authentication_known: bool,
    generation: u64,
    signed_out: bool,
    needs_store: bool,
    stores: Vec<StoreBinding>,
    secrets: Vec<Weak<SecretCell>>,
}

pub(crate) struct Authorization {
    state: Mutex<State>,
    pub(crate) revoked: syntra_runtime::CancelToken,
}
impl Authorization {
    pub(crate) fn new(source: &TrustSource) -> Arc<Self> {
        let value = Arc::new(Self {
            state: Mutex::new(State {
                authentication_known: false,
                generation: 0,
                signed_out: matches!(source, TrustSource::SignedOut(_)),
                needs_store: false,
                stores: Vec::new(),
                secrets: Vec::new(),
            }),
            revoked: Default::default(),
        });
        if let TrustSource::Returning { enrollment, .. } = source {
            let _ = value.register_source(0, enrollment.secret());
        }
        if value.is_signed_out() {
            value.revoked.cancel();
        }
        value
    }
    pub(crate) fn is_signed_out(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .signed_out
    }
    /// Only a fresh authenticated Returning connection establishes this knowledge.
    pub(crate) fn set_authentication_known(&self, known: bool) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.authentication_known = known && !state.signed_out;
    }
    /// Unknown authorization is never evidence that credentials should be erased.
    pub(crate) fn authentication_known(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .authentication_known
    }
    /// Legacy callback contracts cannot attest conditional restart-safe cleanup.
    pub(crate) fn restart_safe_store(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        !state.stores.is_empty()
            && state
                .stores
                .iter()
                .all(|binding| binding.revision.is_some())
    }
    pub(crate) fn generation(self: &Arc<Self>) -> Result<EnrollmentEpoch, TinkleError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.signed_out {
            return Err(TinkleError::SignedOut);
        }
        Ok(EnrollmentEpoch {
            owner: self.clone(),
            generation: state.generation,
        })
    }
    pub(crate) fn with_generation<T>(
        &self,
        generation: u64,
        f: impl FnOnce() -> T,
    ) -> Result<T, TinkleError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.signed_out || state.generation != generation {
            return Err(TinkleError::SignedOut);
        }
        Ok(f())
    }
    pub(crate) fn register_source(
        &self,
        generation: u64,
        secret: Arc<SecretCell>,
    ) -> Result<(), TinkleError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.signed_out || state.generation != generation {
            secret.retire();
            return Err(TinkleError::SignedOut);
        }
        state.secrets.push(Arc::downgrade(&secret));
        state.needs_store = true;
        Ok(())
    }
    fn register_secret(
        &self,
        generation: u64,
        secret: &Arc<SecretCell>,
    ) -> Result<(), TinkleError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.signed_out || state.generation != generation {
            secret.retire();
            return Err(TinkleError::SignedOut);
        }
        state.secrets.retain(|value| value.strong_count() != 0);
        state.secrets.push(Arc::downgrade(secret));
        Ok(())
    }
    pub(crate) fn sign_out(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.signed_out {
            state.signed_out = true;
            state.generation = state.generation.wrapping_add(1);
        }
        for secret in state
            .secrets
            .drain(..)
            .filter_map(|secret| secret.upgrade())
        {
            secret.retire();
        }
        self.revoked.cancel();
    }
    pub(crate) fn stores(&self) -> (bool, Vec<Arc<dyn SecureEnrollmentStore>>) {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        (
            state.needs_store,
            state
                .stores
                .iter()
                .map(|binding| binding.store.clone())
                .collect(),
        )
    }

    /// Snapshot the revision captured by this coordinator, never a fresh host value.
    pub(crate) fn store_revision(&self, store: &dyn SecureEnrollmentStore) -> Option<String> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .stores
            .iter()
            .find(|existing| std::ptr::eq(existing.store.as_ref(), store))
            .and_then(|binding| binding.revision.clone())
    }

    /// A snapshot must belong to this coordinator's captured incarnation and revision.
    pub(crate) fn matches_namespace(
        &self,
        store: &dyn SecureEnrollmentStore,
        incarnation: &str,
        revision: &str,
    ) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.stores.iter().any(|binding| {
            std::ptr::eq(binding.store.as_ref(), store)
                && binding.incarnation.as_deref() == Some(incarnation)
                && binding.revision.as_deref() == Some(revision)
        })
    }

    /// Automatic evidence must not authorize cleanup of an unrelated/legacy namespace.
    pub(crate) fn only_namespace(&self, incarnation: &str) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        !state.stores.is_empty()
            && state
                .stores
                .iter()
                .all(|binding| binding.incarnation.as_deref() == Some(incarnation))
    }

    /// Advance only the locally expected revision, including drained pre-fence writes.
    pub(crate) fn update_store_revision(
        &self,
        store: &dyn SecureEnrollmentStore,
        expected: Option<&str>,
        next: Option<String>,
    ) -> Result<(), TinkleError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let index = state
            .stores
            .iter()
            .position(|existing| std::ptr::eq(existing.store.as_ref(), store))
            .ok_or(TinkleError::SecureStore)?;
        if state.stores[index].revision.as_deref() != expected {
            return Err(TinkleError::SecureStore);
        }
        let incarnation = state.stores[index].incarnation.clone();
        for (current, binding) in state.stores.iter_mut().enumerate() {
            if current == index || (incarnation.is_some() && binding.incarnation == incarnation) {
                binding.revision = next.clone();
            }
        }
        Ok(())
    }
}

/// Sealed, client-specific authority generation. Never serialized as a claim.
pub(crate) struct EnrollmentEpoch {
    owner: Arc<Authorization>,
    pub(crate) generation: u64,
}
impl EnrollmentEpoch {
    pub(crate) fn valid(&self) -> Result<(), TinkleError> {
        self.owner.with_generation(self.generation, || ())
    }
    pub(crate) fn check(&self, client: &Client) -> Result<(), TinkleError> {
        if !Arc::ptr_eq(&self.owner, &client.authorization) {
            return Err(TinkleError::SignedOut);
        }
        self.owner.with_generation(self.generation, || ())
    }
    pub(crate) fn duplicate(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            generation: self.generation,
        }
    }
    pub(crate) fn revoked(&self) -> syntra_runtime::CancelToken {
        self.owner.revoked.clone()
    }
    pub(crate) fn secret(
        &self,
        bytes: Zeroizing<[u8; 32]>,
    ) -> Result<Arc<SecretCell>, TinkleError> {
        let secret = SecretCell::new(bytes);
        self.owner.register_secret(self.generation, &secret)?;
        Ok(secret)
    }
    /// Bind callback aliases atomically to one durable namespace revision.
    pub(crate) fn store(
        &self,
        client: &Client,
        store: Arc<dyn SecureEnrollmentStore>,
        incarnation: Option<String>,
        revision: Option<String>,
    ) -> Result<(), TinkleError> {
        self.check(client)?;
        let mut state = self.owner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.signed_out || state.generation != self.generation {
            return Err(TinkleError::SignedOut);
        }
        // A rejected bind must not register a legacy/unconditional clear path.
        for binding in &state.stores {
            let same_object = Arc::ptr_eq(&binding.store, &store);
            let same_namespace = incarnation.is_some() && binding.incarnation == incarnation;
            if (same_object && binding.incarnation.is_some() && binding.incarnation != incarnation)
                || ((same_object || same_namespace)
                    && binding.revision.is_some()
                    && binding.revision != revision)
            {
                return Err(TinkleError::SecureStore);
            }
        }
        if let Some(binding) = state
            .stores
            .iter_mut()
            .find(|binding| Arc::ptr_eq(&binding.store, &store))
        {
            binding.incarnation = incarnation;
            binding.revision = revision;
        } else {
            state.stores.push(StoreBinding {
                store,
                incarnation,
                revision,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Callback aliases are distinct objects; their shared namespace is host-provided.
    struct Callback;
    #[async_trait::async_trait]
    impl SecureEnrollmentStore for Callback {
        async fn clear(&self) -> Result<(), crate::SecureStoreError> {
            unreachable!()
        }
        async fn commit(
            &self,
            _: crate::SecureWrite,
            _: &str,
            _: &crate::SecretBlob,
        ) -> Result<(), crate::SecureStoreError> {
            unreachable!()
        }
        async fn load_operation(
            &self,
            _: &str,
        ) -> Result<Option<crate::SecretBlob>, crate::SecureStoreError> {
            unreachable!()
        }
        async fn load_active(&self) -> Result<Option<crate::SecretBlob>, crate::SecureStoreError> {
            unreachable!()
        }
    }

    /// Fresh foreign callback wrappers share revisions and cannot downgrade fencing.
    #[test]
    fn callback_aliases_share_revisions_and_rejected_binds_are_not_registered() {
        let info = crate::ClientInfo::new(
            "device".into(),
            "test".into(),
            "1".into(),
            "1".into(),
            "1".into(),
            Default::default(),
        )
        .unwrap();
        let client = Client::new(
            crate::ClientConfig::new("localhost", info),
            TrustSource::Bootstrap(crate::ServerTrust {
                server_public_key: [1; 32],
                server_key_id: 7,
                scope: "test".into(),
            }),
            crate::InMemoryCursorStore::new(),
        );
        let epoch = client.authorization.generation().unwrap();
        let first: Arc<dyn SecureEnrollmentStore> = Arc::new(Callback);
        let alias: Arc<dyn SecureEnrollmentStore> = Arc::new(Callback);
        for callback in [&first, &alias] {
            epoch
                .store(
                    &client,
                    callback.clone(),
                    Some("namespace".into()),
                    Some("revision:0".into()),
                )
                .unwrap();
        }
        client
            .authorization
            .update_store_revision(
                first.as_ref(),
                Some("revision:0"),
                Some("revision:1".into()),
            )
            .unwrap();
        assert_eq!(
            client
                .authorization
                .store_revision(alias.as_ref())
                .as_deref(),
            Some("revision:1")
        );
        assert!(epoch
            .store(
                &client,
                Arc::new(Callback),
                Some("namespace".into()),
                Some("revision:2".into())
            )
            .is_err());
        assert_eq!(client.authorization.stores().1.len(), 2);
        assert!(epoch.store(&client, alias, None, None).is_err());
        assert!(client.authorization.restart_safe_store());
    }

    #[test]
    fn sign_out_erases_retained_receipt_cells_and_source_material() {
        let trust = crate::ServerTrust {
            server_public_key: [1; 32],
            server_key_id: 7,
            scope: "test".into(),
        };
        let enrollment = crate::Enrollment::new([9; 32], [8; 8], 1).unwrap();
        let source = TrustSource::Returning { trust, enrollment };
        let owner = Authorization::new(&source);
        let epoch = owner.generation().unwrap();
        let retained = epoch.secret(Zeroizing::new([7; 32])).unwrap();
        assert!(source.security().is_ok());
        assert!(retained.snapshot().is_ok());
        owner.sign_out();
        assert!(retained.snapshot().is_err());
        assert!(matches!(source.security(), Err(TinkleError::SignedOut)));
        assert!(epoch.valid().is_err());
        assert!(owner.generation().is_err());
    }
}
