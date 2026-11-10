//! Irreversible local sign-out fence and revocable SDK-owned secret cells.
use crate::{Client, SecureEnrollmentStore, TinkleError, TrustSource};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, Weak,
};
use zeroize::Zeroizing;

/// Shared only by sealed owners; logout erases the cell even while a host
/// retains an operation/candidate object. No Clone or diagnostic secret export.
pub(crate) struct SecretCell(Mutex<Option<Zeroizing<[u8; 32]>>>);
impl SecretCell {
    /// Retain key bytes in a shared zeroizing cell that logout can revoke.
    pub(crate) fn new(bytes: Zeroizing<[u8; 32]>) -> Arc<Self> {
        Arc::new(Self(Mutex::new(Some(bytes))))
    }
    /// Copy live key bytes into a zeroizing owner, or reject an already retired cell.
    pub(crate) fn snapshot(&self) -> Result<Zeroizing<[u8; 32]>, TinkleError> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|v| Zeroizing::new(**v))
            .ok_or(TinkleError::SignedOut)
    }
    /// Erase the retained key and permanently prevent further snapshots.
    pub(crate) fn retire(&self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).take();
    }
}

/// One callback object bound to a durable namespace incarnation and revision.
struct StoreBinding {
    store: Arc<dyn SecureEnrollmentStore>,
    aliases: Vec<Arc<dyn SecureEnrollmentStore>>,
    incarnation: Option<String>,
    revision: Option<String>,
    writing: Arc<AtomicBool>,
}

/// Keeps an ambiguous host transaction single-owned until its exact receipt arrives.
pub(crate) struct StoreWrite(Arc<AtomicBool>);
impl Drop for StoreWrite {
    /// Release single-write ownership when the acknowledged transaction owner finishes.
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl StoreBinding {
    /// Match a registered callback object or one of its known namespace aliases.
    fn owns(&self, store: &dyn SecureEnrollmentStore) -> bool {
        std::ptr::eq(self.store.as_ref(), store)
            || self
                .aliases
                .iter()
                .any(|alias| std::ptr::eq(alias.as_ref(), store))
    }
}

struct State {
    generation: u64,
    signed_out: bool,
    needs_store: bool,
    stores: Vec<StoreBinding>,
    secrets: Vec<Weak<SecretCell>>,
}

pub(crate) struct Authorization {
    state: Mutex<State>,
    session: Arc<crate::session::SessionCoordinator>,
    pub(crate) revoked: syntra_runtime::CancelToken,
}
impl Authorization {
    /// Initialize the sign-out fence and register any supplied Returning key for revocation.
    #[cfg(test)]
    pub(crate) fn new(source: &TrustSource) -> Arc<Self> {
        Self::with_session(
            source,
            crate::session::SessionCoordinator::new(matches!(source, TrustSource::SignedOut(_))),
        )
    }
    /// Share the account reducer without giving it access to secrets or protected callbacks.
    pub(crate) fn with_session(
        source: &TrustSource,
        session: Arc<crate::session::SessionCoordinator>,
    ) -> Arc<Self> {
        let value = Arc::new(Self {
            session,
            state: Mutex::new(State {
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
    /// Read the irreversible local sign-out fence independently of transport state.
    pub(crate) fn is_signed_out(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .signed_out
    }
    /// Only a fresh authenticated Returning connection establishes this knowledge.
    pub(crate) fn set_authentication_known(&self, known: bool) {
        self.session.authentication(known);
    }
    /// Legacy callback contracts cannot attest conditional restart-safe cleanup.
    #[cfg(test)]
    pub(crate) fn restart_safe_store(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        !state.stores.is_empty()
            && state
                .stores
                .iter()
                .all(|binding| binding.revision.is_some())
    }
    /// Test inspection of the proof knowledge feeding the account reducer.
    #[cfg(test)]
    pub(crate) fn authentication_known(&self) -> bool {
        self.session.authenticated()
    }
    /// Capture client-specific enrollment authority unless local sign-out has fenced it.
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
    /// Run a synchronous action under the state lock only for the still-live generation.
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
    /// Register source key revocation and require durable cleanup; erase stale registrations.
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
    /// Track a receipt's live secret for logout, retiring it if its generation is stale.
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
    /// Fence enrollment, erase every reachable registered secret, and cancel revoked work.
    pub(crate) fn sign_out(&self, reason: crate::SessionSignOutReason) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.session.signed_out(reason);
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
    /// Snapshot the cleanup requirement and one callback owner per registered namespace.
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

    /// Reserve the registered namespace's single outstanding host-write receipt.
    pub(crate) fn begin_store_write(
        &self,
        store: &dyn SecureEnrollmentStore,
    ) -> Result<StoreWrite, TinkleError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let binding = state
            .stores
            .iter()
            .find(|binding| binding.owns(store))
            .ok_or(TinkleError::SecureStore)?;
        if binding.writing.swap(true, Ordering::AcqRel) {
            return Err(TinkleError::SecureStore);
        }
        Ok(StoreWrite(binding.writing.clone()))
    }

    /// Report whether any namespace still awaits its exact host-write acknowledgement.
    pub(crate) fn has_pending_store_write(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stores
            .iter()
            .any(|binding| binding.writing.load(Ordering::Acquire))
    }

    /// Snapshot the revision captured by this coordinator, never a fresh host value.
    pub(crate) fn store_revision(&self, store: &dyn SecureEnrollmentStore) -> Option<String> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .stores
            .iter()
            .find(|existing| existing.owns(store))
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
            binding.owns(store)
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
            .position(|existing| existing.owns(store))
            .ok_or(TinkleError::SecureStore)?;
        if state.stores[index].revision.as_deref() != expected {
            return Err(TinkleError::SecureStore);
        }
        state.stores[index].revision = next;
        self.session
            .store(!state.stores.is_empty() && state.stores.iter().all(|s| s.revision.is_some()));
        Ok(())
    }

    /// Register cleanup authority after fencing, never an enrollment generation.
    pub(crate) fn bind_clear_only(
        &self,
        store: Arc<dyn SecureEnrollmentStore>,
        incarnation: Option<String>,
        revision: Option<String>,
    ) -> Result<(), TinkleError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.signed_out || !state.needs_store || !state.stores.is_empty() {
            return Err(TinkleError::SignedOut);
        }
        state.stores.push(StoreBinding {
            store,
            aliases: Vec::new(),
            incarnation,
            revision,
            writing: Default::default(),
        });
        self.session
            .store(state.stores.iter().all(|s| s.revision.is_some()));
        Ok(())
    }
}

/// Sealed, client-specific authority generation. Never serialized as a claim.
pub(crate) struct EnrollmentEpoch {
    owner: Arc<Authorization>,
    pub(crate) generation: u64,
}
impl EnrollmentEpoch {
    /// Reject authority invalidated by local sign-out or generation replacement.
    pub(crate) fn valid(&self) -> Result<(), TinkleError> {
        self.owner.with_generation(self.generation, || ())
    }
    /// Require both the originating client coordinator and its current live generation.
    pub(crate) fn check(&self, client: &Client) -> Result<(), TinkleError> {
        if !Arc::ptr_eq(&self.owner, &client.authorization) {
            return Err(TinkleError::SignedOut);
        }
        self.owner.with_generation(self.generation, || ())
    }
    /// Retain the same sealed authority without creating or advancing a generation.
    pub(crate) fn duplicate(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            generation: self.generation,
        }
    }
    /// Share the coordinator's irreversible sign-out cancellation signal.
    pub(crate) fn revoked(&self) -> syntra_runtime::CancelToken {
        self.owner.revoked.clone()
    }
    /// Create a revocable zeroizing secret registered to this live enrollment generation.
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
            let same_object = binding.owns(store.as_ref());
            let same_namespace = incarnation.is_some() && binding.incarnation == incarnation;
            if (same_object || same_namespace) && binding.writing.load(Ordering::Acquire) {
                return Err(TinkleError::SecureStore);
            }
            if (same_object && binding.incarnation.is_some() && binding.incarnation != incarnation)
                || ((same_object || same_namespace)
                    && binding.revision.is_some()
                    && binding.revision != revision)
            {
                return Err(TinkleError::SecureStore);
            }
        }
        // A legacy callback may later opt into a namespace already known through
        // another wrapper. Merge its retained identity rather than clear twice.
        if let (Some(namespace), Some(object)) = (
            state
                .stores
                .iter()
                .position(|binding| incarnation.is_some() && binding.incarnation == incarnation),
            state
                .stores
                .iter()
                .position(|binding| binding.owns(store.as_ref())),
        ) {
            if namespace != object {
                let previous = state.stores.remove(object);
                let namespace = namespace - usize::from(object < namespace);
                state.stores[namespace].aliases.push(previous.store);
                state.stores[namespace].aliases.extend(previous.aliases);
            }
        }
        if let Some(binding) = state.stores.iter_mut().find(|binding| {
            binding.owns(store.as_ref())
                || (incarnation.is_some() && binding.incarnation == incarnation)
        }) {
            if !binding.owns(store.as_ref()) {
                binding.aliases.push(store);
            }
            binding.incarnation = incarnation;
            binding.revision = revision;
        } else {
            state.stores.push(StoreBinding {
                store,
                aliases: Vec::new(),
                incarnation,
                revision,
                writing: Default::default(),
            });
        }
        self.owner
            .session
            .store(!state.stores.is_empty() && state.stores.iter().all(|s| s.revision.is_some()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Callback aliases are distinct objects; their shared namespace is host-provided.
    #[derive(Default)]
    struct Callback(Arc<std::sync::atomic::AtomicUsize>);
    #[async_trait::async_trait]
    impl SecureEnrollmentStore for Callback {
        /// Count each namespace clear acknowledged by this callback alias.
        async fn clear(&self) -> Result<(), crate::SecureStoreError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        /// Accept the fixture cleanup marker without introducing additional storage state.
        async fn begin_clear(&self, _: &str) -> Result<bool, crate::SecureStoreError> {
            Ok(true)
        }
        /// Acknowledge conditional cleanup through the same counted clear callback.
        async fn finish_clear(&self, _: &str) -> Result<bool, crate::SecureStoreError> {
            self.clear().await?;
            Ok(true)
        }
        /// Reject writes because these tests exercise callback registration and cleanup only.
        async fn commit(
            &self,
            _: crate::SecureWrite,
            _: &str,
            _: &crate::SecretBlob,
        ) -> Result<(), crate::SecureStoreError> {
            unreachable!()
        }
        /// Reject operation reads outside the registration-only fixture contract.
        async fn load_operation(
            &self,
            _: &str,
        ) -> Result<Option<crate::SecretBlob>, crate::SecureStoreError> {
            unreachable!()
        }
        /// Reject restoration reads outside the registration-only fixture contract.
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
        let first: Arc<dyn SecureEnrollmentStore> = Arc::new(Callback::default());
        let alias: Arc<dyn SecureEnrollmentStore> = Arc::new(Callback::default());
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
                Arc::new(Callback::default()),
                Some("namespace".into()),
                Some("revision:2".into())
            )
            .is_err());
        assert_eq!(client.authorization.stores().1.len(), 1);
        assert!(epoch.store(&client, alias, None, None).is_err());
        assert!(client.authorization.restart_safe_store());
        for _ in 0..2 {
            epoch
                .store(&client, Arc::new(Callback::default()), None, None)
                .unwrap();
        }
        assert_eq!(client.authorization.stores().1.len(), 3);
        assert!(!client.authorization.restart_safe_store());
        assert!(!client.authorization.only_namespace("namespace"));
    }

    #[tokio::test]
    /// Clear conditional namespace aliases once, but retain separate legacy callback obligations.
    async fn review_logout_clears_namespace_aliases_once_and_distinct_legacy_stores_each() {
        let client = crate::enrollment::review_tests::client(false);
        let epoch = client.authorization.generation().unwrap();
        let conditional = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for _ in 0..2 {
            epoch
                .store(
                    &client,
                    Arc::new(Callback(conditional.clone())),
                    Some("namespace".into()),
                    Some("0".into()),
                )
                .unwrap();
        }
        let legacy = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for _ in 0..2 {
            epoch
                .store(&client, Arc::new(Callback(legacy.clone())), None, None)
                .unwrap();
        }
        client.logout().await.unwrap();
        client.logout().await.unwrap();
        assert_eq!(conditional.load(Ordering::SeqCst), 1);
        assert_eq!(legacy.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    /// Upgrading a legacy alias must merge namespace identity without permitting fence downgrade.
    async fn review_legacy_callback_opt_in_merges_an_existing_namespace_without_downgrade() {
        let client = crate::enrollment::review_tests::client(false);
        let epoch = client.authorization.generation().unwrap();
        let clears = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let legacy: Arc<dyn SecureEnrollmentStore> = Arc::new(Callback(clears.clone()));
        let conditional: Arc<dyn SecureEnrollmentStore> = Arc::new(Callback(clears.clone()));
        epoch.store(&client, legacy.clone(), None, None).unwrap();
        for store in [conditional, legacy.clone()] {
            epoch
                .store(&client, store, Some("namespace".into()), Some("0".into()))
                .unwrap();
        }
        assert_eq!(client.authorization.stores().1.len(), 1);
        assert_eq!(
            client
                .authorization
                .store_revision(legacy.as_ref())
                .as_deref(),
            Some("0")
        );
        assert!(epoch.store(&client, legacy, None, None).is_err());
        client.logout().await.unwrap();
        assert_eq!(clears.load(Ordering::SeqCst), 1);
    }

    #[test]
    /// Sign-out must erase both source keys and secrets retained by previously issued receipts.
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
        owner.sign_out(crate::SessionSignOutReason::SelfLogout);
        assert!(retained.snapshot().is_err());
        assert!(matches!(source.security(), Err(TinkleError::SignedOut)));
        assert!(epoch.valid().is_err());
        assert!(owner.generation().is_err());
    }
}
