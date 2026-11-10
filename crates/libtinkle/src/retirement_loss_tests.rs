//! Accepted loss demand survives bounded main-transport retirement, not lifecycle fences.
use super::*;

/// Install a real Returning carrier without background heartbeat/review races.
async fn install_loss_carrier(client: &Client, gate: Arc<DropGate>) -> Arc<LiveGeneration> {
    let mut live = live_owner(client, held_handle(client, gate).await);
    let generation = LiveGeneration::new(true, client.retirement.revision());
    live.generation = generation.clone();
    client.session_state.network(generation.clone(), true);
    client.transport_ok.store(true, Ordering::SeqCst);
    *client.live.write().await = Some(live);
    client.arm_retirement_schedule().await;
    generation
}

/// Bound a scheduler completion wait independently of the five-second observation budget.
async fn loss_attempt_finished(client: &Client) {
    tokio::time::timeout(
        Duration::from_secs(7),
        client.retirement.scheduled_attempt_finished(),
    )
    .await
    .expect("bounded loss attempt must finish");
}

/// UNKNOWN consumes accepted demand after join, without granting admission or erasing storage.
async fn assert_loss_retried(fixture: &Fixture) {
    fixture.unknown().await;
    assert!(!fixture.client.retirement.has_pending_loss());
    assert_eq!(fixture.observations.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 2);
    assert!(!fixture.client.is_signed_out());
    assert!(fixture
        .store
        .as_ref()
        .unwrap()
        .load_active()
        .await
        .unwrap()
        .is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// An observation timeout after its own generation fence retains demand and retries before the minute tick.
async fn retirement_loss_teardown_timeout_retries_after_release() {
    let mut fixture = Fixture::new(vec![Mode::Returning, Mode::Bootstrap], true).await;
    let client = &fixture.client;
    let gate = Arc::new(DropGate::default());
    let _release = ReleaseOnDrop(gate.clone());
    let generation = install_loss_carrier(client, gate.clone()).await;
    let establishing = client.connect_lock.establishing.lock().await;
    client.request_loss_observation(generation.clone());
    loss_attempt_finished(client).await;
    assert!(generation.retiring.load(Ordering::SeqCst));
    assert!(
        client.retirement.has_pending_loss(),
        "timeout discarded accepted loss"
    );
    assert_eq!(fixture.observations.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 1);
    gate.release();
    drop(establishing);
    assert_loss_retried(&fixture).await;
    client.stop().await;
    fixture.server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// ShutdownIncomplete retains both the main join owner and accepted demand for a prompt bounded retry.
async fn retirement_loss_false_join_retries_after_release() {
    let mut fixture = Fixture::new(vec![Mode::Returning, Mode::Bootstrap], true).await;
    let client = &fixture.client;
    let gate = Arc::new(DropGate::default());
    let _release = ReleaseOnDrop(gate.clone());
    let generation = install_loss_carrier(client, gate.clone()).await;
    client.request_loss_observation(generation.clone());
    loss_attempt_finished(client).await;
    assert!(gate.entered.load(Ordering::SeqCst));
    assert!(generation.retiring.load(Ordering::SeqCst));
    assert_eq!(client.retiring.lock().await.len(), 1);
    assert!(
        client.retirement.has_pending_loss(),
        "incomplete join discarded accepted loss"
    );
    assert_eq!(fixture.admissions.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        client.retirement.scheduled_attempt_count(),
        1,
        "retry spun without backoff"
    );
    gate.release();
    assert_loss_retried(&fixture).await;
    client.stop().await;
    fixture.server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// Neither timeout nor incomplete join may overwrite demand registered during predecessor teardown.
async fn retirement_loss_failed_teardown_preserves_newer_demand() {
    for timeout in [true, false] {
        let mut fixture = Fixture::new(vec![Mode::Returning, Mode::Bootstrap], true).await;
        let client = &fixture.client;
        let gate = Arc::new(DropGate::default());
        let _release = ReleaseOnDrop(gate.clone());
        let previous = install_loss_carrier(client, gate.clone()).await;
        let mut establishing = Some(client.connect_lock.establishing.lock().await);
        client.request_loss_observation(previous.clone());
        tokio::time::timeout(Duration::from_secs(2), async {
            while !previous.retiring.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let newer = LiveGeneration::new(true, client.retirement.revision());
        client.session_state.network(newer.clone(), true);
        client.request_loss_observation(newer);
        if !timeout {
            // Let teardown reach the held carrier and return ShutdownIncomplete.
            drop(establishing.take());
        }
        loss_attempt_finished(client).await;
        assert!(
            client.retirement.has_pending_loss(),
            "predecessor restore replaced current demand"
        );
        gate.release();
        drop(establishing);
        assert_loss_retried(&fixture).await;
        client.stop().await;
        fixture.server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// Suspension, source invalidation and replacement generation still withdraw retained accepted loss.
async fn retirement_loss_retained_demand_obeys_lifecycle_fences() {
    for fence in ["suspend", "source", "generation"] {
        let mut fixture = Fixture::new(vec![Mode::Returning, Mode::Bootstrap], true).await;
        let client = &fixture.client;
        let gate = Arc::new(DropGate::default());
        let _release = ReleaseOnDrop(gate.clone());
        let generation = install_loss_carrier(client, gate.clone()).await;
        client.request_loss_observation(generation.clone());
        loss_attempt_finished(client).await;
        assert!(
            client.retirement.has_pending_loss(),
            "{fence}: no retained demand to fence"
        );
        match fence {
            "suspend" => client.suspend().await,
            "source" => client.invalidate_retirement_binding().await,
            _ => client.session_state.network(
                LiveGeneration::new(true, client.retirement.revision()),
                true,
            ),
        }
        assert!(
            !client.retirement.has_pending_loss(),
            "{fence}: stale demand survived"
        );
        client.request_loss_observation(generation);
        assert!(
            !client.retirement.has_pending_loss(),
            "{fence}: stale loss readmitted"
        );
        gate.release();
        client.retirement.wake_pending_loss();
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_eq!(
            fixture.observations.load(Ordering::SeqCst),
            0,
            "{fence}: stale evidence dial"
        );
        assert_eq!(fixture.admissions.load(Ordering::SeqCst), 1);
        client.stop().await;
        fixture.server.shutdown().await;
    }
}

#[tokio::test]
/// A fresh request from an already-retiring current generation remains ineligible.
async fn retirement_loss_fresh_retiring_generation_is_not_admitted() {
    let mut fixture = Fixture::new(vec![Mode::Returning, Mode::Bootstrap], true).await;
    fixture.connect().await;
    let client = &fixture.client;
    let generation = client.session_generation().unwrap();
    generation.retiring.store(true, Ordering::SeqCst);
    client.request_loss_observation(generation);
    assert!(!client.retirement.has_pending_loss());
    assert_eq!(fixture.observations.load(Ordering::SeqCst), 0);
    client.stop().await;
    fixture.server.shutdown().await;
}
