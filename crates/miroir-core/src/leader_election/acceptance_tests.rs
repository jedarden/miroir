//! Acceptance tests for Mode B leader-only singleton coordinator (P6.4).
//!
//! These tests verify the key acceptance criteria from plan §14.5:
//! 1. Exactly one leader across multiple pods at any instant
//! 2. Leader failover promotes a new leader within lease_ttl_s
//! 3. Mode B operations resume from the last committed phase after leader loss
//! 4. Leader metrics (miroir_leader) are consistent across all pods

use crate::config::LeaderElectionConfig;
use crate::leader_election::LeaderElection;
use crate::task_store::{
    ModeBOperation, SqliteTaskStore, TaskStore,
    mode_b_status, mode_b_type,
};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

/// Get current time in milliseconds since Unix epoch.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Test configuration for leader election.
fn test_config() -> LeaderElectionConfig {
    LeaderElectionConfig {
        enabled: true,
        lease_ttl_s: 10,
        renew_interval_s: 3,
    }
}

/// Test configuration with short TTL for faster tests.
fn fast_test_config() -> LeaderElectionConfig {
    LeaderElectionConfig {
        enabled: true,
        lease_ttl_s: 2, // Short TTL for faster failover testing
        renew_interval_s: 1,
    }
}

/// Create a test store with migrations applied.
async fn test_store() -> Arc<SqliteTaskStore> {
    let store = SqliteTaskStore::open_in_memory().unwrap();
    store.migrate().unwrap();
    Arc::new(store)
}

/// Simulate multiple pods competing for leadership.
struct MockPod {
    id: String,
    leader_election: LeaderElection,
    metrics: std::collections::HashMap<String, f64>,
}

impl MockPod {
    fn new(id: String, task_store: Arc<dyn TaskStore>, config: LeaderElectionConfig) -> Self {
        let leader_election = LeaderElection::new(task_store, id.clone(), config);
        Self {
            id,
            leader_election,
            metrics: std::collections::HashMap::new(),
        }
    }

    /// Try to acquire leadership for a scope.
    async fn try_acquire(&self, scope: &str) -> bool {
        self.leader_election
            .try_acquire_async(scope)
            .await
            .unwrap_or(false)
    }

    /// Check if this pod is the leader for a scope.
    fn is_leader(&self, scope: &str) -> bool {
        self.leader_election.is_leader(scope)
    }

    /// Get the current leader for a scope.
    async fn get_leader(&self, scope: &str) -> Option<String> {
        self.leader_election.get_holder(scope).unwrap_or(None)
    }

    /// Renew the lease if we hold it.
    async fn renew(&self, scope: &str) -> bool {
        self.leader_election
            .renew_async(scope)
            .await
            .unwrap_or(false)
    }

    /// Step down from leadership.
    async fn step_down(&self, scope: &str) -> bool {
        self.leader_election
            .step_down_async(scope)
            .await
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Acceptance Test 1: Exactly one leader across multiple pods
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ac1_three_pods_exactly_one_leader() {
    let store = test_store().await;
    let config = test_config();

    // Create 3 pods
    let pod1 = MockPod::new("pod-1".to_string(), store.clone(), config.clone());
    let pod2 = MockPod::new("pod-2".to_string(), store.clone(), config.clone());
    let pod3 = MockPod::new("pod-3".to_string(), store.clone(), config.clone());

    let scope = "test-single-leader";

    // All pods try to acquire leadership
    let pod1_acquired = pod1.try_acquire(scope).await;
    let pod2_acquired = pod2.try_acquire(scope).await;
    let pod3_acquired = pod3.try_acquire(scope).await;

    // Exactly one should have acquired
    let acquired_count = [pod1_acquired, pod2_acquired, pod3_acquired]
        .iter()
        .filter(|&&x| x)
        .count();
    assert_eq!(
        acquired_count, 1,
        "exactly one pod should acquire leadership, got {}",
        acquired_count
    );

    // Identify the leader
    let leader_id = pod1.get_leader(scope).await.unwrap();
    assert!(leader_id == "pod-1" || leader_id == "pod-2" || leader_id == "pod-3");

    // Verify all pods agree on who the leader is
    assert_eq!(pod1.get_leader(scope).await, Some(leader_id.clone()));
    assert_eq!(pod2.get_leader(scope).await, Some(leader_id.clone()));
    assert_eq!(pod3.get_leader(scope).await, Some(leader_id.clone()));

    // Verify only the leader pod reports itself as leader
    if leader_id.as_str() == "pod-1" {
        assert!(pod1.is_leader(scope));
        assert!(!pod2.is_leader(scope));
        assert!(!pod3.is_leader(scope));
    } else if leader_id.as_str() == "pod-2" {
        assert!(!pod1.is_leader(scope));
        assert!(pod2.is_leader(scope));
        assert!(!pod3.is_leader(scope));
    } else {
        assert!(!pod1.is_leader(scope));
        assert!(!pod2.is_leader(scope));
        assert!(pod3.is_leader(scope));
    }
}

#[tokio::test]
async fn ac2_leader_failover_promotes_new_leader() {
    let store = test_store().await;
    let config = fast_test_config(); // Use fast config for quicker test

    // Create 3 pods
    let pod1 = MockPod::new("pod-1".to_string(), store.clone(), config.clone());
    let pod2 = MockPod::new("pod-2".to_string(), store.clone(), config.clone());
    let pod3 = MockPod::new("pod-3".to_string(), store.clone(), config.clone());

    let scope = "test-failover";

    // Pod 1 acquires leadership
    assert!(pod1.try_acquire(scope).await);
    assert_eq!(pod1.get_leader(scope).await, Some("pod-1".to_string()));

    // Pod 1 steps down (simulating crash)
    assert!(pod1.step_down(scope).await);
    assert!(!pod1.is_leader(scope));

    // Wait a bit for the lease to expire
    sleep(Duration::from_millis(100)).await;

    // Pod 2 should now be able to acquire leadership
    assert!(pod2.try_acquire(scope).await);
    assert_eq!(pod2.get_leader(scope).await, Some("pod-2".to_string()));

    // Verify pod 2 is the leader
    assert!(pod2.is_leader(scope));
    assert!(!pod1.is_leader(scope));
    assert!(!pod3.is_leader(scope));

    // Pod 2 steps down
    assert!(pod2.step_down(scope).await);

    // Pod 3 should now be able to acquire leadership
    assert!(pod3.try_acquire(scope).await);
    assert_eq!(pod3.get_leader(scope).await, Some("pod-3".to_string()));
}

#[tokio::test]
async fn ac3_leader_renewal_prevents_stealing() {
    let store = test_store().await;
    let config = test_config();

    let pod1 = MockPod::new("pod-1".to_string(), store.clone(), config.clone());
    let pod2 = MockPod::new("pod-2".to_string(), store.clone(), config.clone());

    let scope = "test-renewal";

    // Pod 1 acquires leadership
    assert!(pod1.try_acquire(scope).await);
    assert_eq!(pod1.get_leader(scope).await, Some("pod-1".to_string()));

    // Pod 1 renews the lease
    assert!(pod1.renew(scope).await);
    assert_eq!(pod1.get_leader(scope).await, Some("pod-1".to_string()));

    // Pod 2 cannot steal the lease (not expired)
    assert!(!pod2.try_acquire(scope).await);

    // Pod 1 is still the leader
    assert!(pod1.is_leader(scope));
    assert!(!pod2.is_leader(scope));
}

// ---------------------------------------------------------------------------
// Acceptance Test 2: Reshard phase recovery after leader loss
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ac4_reshard_phase_recovery_after_leader_loss() {
    let store = test_store().await;
    let config = test_config();

    let pod1 = MockPod::new("pod-1".to_string(), store.clone(), config.clone());
    let pod2 = MockPod::new("pod-2".to_string(), store.clone(), config.clone());

    let scope = "reshard:products";

    // Pod 1 acquires leadership
    assert!(pod1.try_acquire(scope).await);

    // Simulate reshard phase 3 (verify) by persisting state
    let operation = ModeBOperation {
        operation_id: "reshard-products-123".to_string(),
        operation_type: mode_b_type::RESHARD.to_string(),
        scope: scope.to_string(),
        phase: "verify".to_string(), // Phase 3
        phase_started_at: 1000,
        created_at: 500,
        updated_at: 1500,
        state_json: r#"{"shadow_index":"products_shadow","backfilled_docs":5000}"#.to_string(),
        error: None,
        status: mode_b_status::RUNNING.to_string(),
        index_uid: Some("products".to_string()),
        old_shards: Some(4),
        target_shards: Some(8),
        shadow_index: Some("products_shadow".to_string()),
        documents_backfilled: Some(5000),
        total_documents: Some(10000),
    };

    store.upsert_mode_b_operation(&operation).unwrap();

    // Pod 1 crashes (steps down)
    pod1.step_down(scope).await;

    // Pod 2 acquires leadership
    assert!(pod2.try_acquire(scope).await);

    // Pod 2 should recover the persisted state and resume from phase 3
    let recovered = pod2
        .leader_election
        .recover_mode_b_operation(scope)
        .unwrap();

    assert!(recovered.is_some(), "should recover operation state");
    let recovered_op = recovered.unwrap();

    // Verify we're resuming from phase 3, not phase 1
    assert_eq!(recovered_op.phase, "verify");
    assert_eq!(recovered_op.index_uid, Some("products".to_string()));
    assert_eq!(recovered_op.old_shards, Some(4));
    assert_eq!(recovered_op.target_shards, Some(8));
    assert_eq!(recovered_op.shadow_index, Some("products_shadow".to_string()));
    assert_eq!(recovered_op.documents_backfilled, Some(5000));
}

#[tokio::test]
async fn ac5_reshard_multiple_phases_persisted_correctly() {
    let store = test_store().await;
    let config = test_config();

    let pod1 = MockPod::new("pod-1".to_string(), store.clone(), config.clone());
    let scope = "reshard:orders";

    // Pod 1 acquires leadership
    assert!(pod1.try_acquire(scope).await);

    // Simulate progressing through phases
    let phases = vec![
        ("shadow", r#"{"shadow_index":"orders_shadow"}"#),
        ("backfill", r#"{"shadow_index":"orders_shadow","backfilled_docs":2500}"#),
        ("verify", r#"{"shadow_index":"orders_shadow","backfilled_docs":5000,"verified":true}"#),
        ("swap", r#"{"shadow_index":"orders_shadow","backfilled_docs":5000,"swapped":true}"#),
        ("cleanup", r#"{"shadow_index":"orders_shadow","cleanup_complete":false}"#),
    ];

    for (i, (phase, state_json)) in phases.iter().enumerate() {
        let operation = ModeBOperation {
            operation_id: format!("reshard-orders-{}", i),
            operation_type: mode_b_type::RESHARD.to_string(),
            scope: scope.to_string(),
            phase: phase.to_string(),
            phase_started_at: 1000 + (i as i64 * 100),
            created_at: 500,
            updated_at: 1500 + (i as i64 * 100),
            state_json: state_json.to_string(),
            error: None,
            status: mode_b_status::RUNNING.to_string(),
            index_uid: Some("orders".to_string()),
            old_shards: Some(2),
            target_shards: Some(4),
            shadow_index: Some("orders_shadow".to_string()),
            documents_backfilled: Some(2500 * (i as i64 + 1)),
            total_documents: Some(5000),
        };

        pod1.leader_election
            .persist_mode_b_operation(&operation)
            .unwrap();
    }

    // Verify we can recover the latest phase
    let recovered = pod1
        .leader_election
        .recover_mode_b_operation(scope)
        .unwrap()
        .unwrap();

    assert_eq!(recovered.phase, "cleanup");
    assert!(recovered.state_json.contains("cleanup_complete"));
}

// ---------------------------------------------------------------------------
// Acceptance Test 3: 2PC settings broadcast phase recovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ac6_settings_broadcast_phase_recovery_after_leader_loss() {
    let store = test_store().await;
    let config = test_config();

    let pod1 = MockPod::new("pod-1".to_string(), store.clone(), config.clone());
    let pod2 = MockPod::new("pod-2".to_string(), store.clone(), config.clone());

    let scope = "settings_broadcast:products";

    // Pod 1 acquires leadership
    assert!(pod1.try_acquire(scope).await);

    // Simulate 2PC phase 2 (verify) by persisting state
    let operation = ModeBOperation {
        operation_id: "settings-broadcast-products-456".to_string(),
        operation_type: mode_b_type::SETTINGS_BROADCAST.to_string(),
        scope: scope.to_string(),
        phase: "verify".to_string(), // Phase 2
        phase_started_at: 2000,
        created_at: 1500,
        updated_at: 2500,
        state_json: r#"{"proposed_version":5,"acked_nodes":["node-1","node-2"],"pending_nodes":["node-3"]}"#.to_string(),
        error: None,
        status: mode_b_status::RUNNING.to_string(),
        index_uid: Some("products".to_string()),
        old_shards: None,
        target_shards: None,
        shadow_index: None,
        documents_backfilled: None,
        total_documents: None,
    };

    pod1.leader_election
        .persist_mode_b_operation(&operation)
        .unwrap();

    // Pod 1 crashes
    pod1.step_down(scope).await;

    // Pod 2 acquires leadership
    assert!(pod2.try_acquire(scope).await);

    // Pod 2 should recover and resume from phase 2 (verify), not phase 1 (propose)
    let recovered = pod2
        .leader_election
        .recover_mode_b_operation(scope)
        .unwrap()
        .unwrap();

    assert_eq!(recovered.phase, "verify");
    assert_eq!(recovered.operation_type, mode_b_type::SETTINGS_BROADCAST);
    assert!(recovered.state_json.contains("proposed_version"));
    assert!(recovered.state_json.contains("acked_nodes"));

    // Verify we didn't restart from phase 1
    assert_ne!(recovered.phase, "propose");
}

#[tokio::test]
async fn ac7_settings_broadcast_all_phases_persisted() {
    let store = test_store().await;
    let config = test_config();

    let pod1 = MockPod::new("pod-1".to_string(), store.clone(), config.clone());
    let scope = "settings_broadcast:users";

    // Pod 1 acquires leadership
    assert!(pod1.try_acquire(scope).await);

    // Simulate 2PC phases
    let phases = vec![
        ("propose", r#"{"proposed_version":3,"target_nodes":["node-1","node-2","node-3"]}"#),
        ("verify", r#"{"proposed_version":3,"acked_nodes":["node-1","node-2"],"pending_nodes":["node-3"]}"#),
        ("commit", r#"{"proposed_version":3,"committed_nodes":["node-1","node-2","node-3"]}"#),
    ];

    for (i, (phase, state_json)) in phases.iter().enumerate() {
        let operation = ModeBOperation {
            operation_id: format!("settings-broadcast-users-{}", i),
            operation_type: mode_b_type::SETTINGS_BROADCAST.to_string(),
            scope: scope.to_string(),
            phase: phase.to_string(),
            phase_started_at: 1000 + (i as i64 * 500),
            created_at: 500,
            updated_at: 1500 + (i as i64 * 500),
            state_json: state_json.to_string(),
            error: None,
            status: mode_b_status::RUNNING.to_string(),
            index_uid: Some("users".to_string()),
            old_shards: None,
            target_shards: None,
            shadow_index: None,
            documents_backfilled: None,
            total_documents: None,
        };

        pod1.leader_election
            .persist_mode_b_operation(&operation)
            .unwrap();
    }

    // Verify final phase
    let recovered = pod1
        .leader_election
        .recover_mode_b_operation(scope)
        .unwrap()
        .unwrap();

    assert_eq!(recovered.phase, "commit");
}

// ---------------------------------------------------------------------------
// Acceptance Test 4: Leader metrics consistency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ac8_leader_metrics_sum_is_one_across_pods() {
    let store = test_store().await;
    let config = test_config();

    let pod1 = MockPod::new("pod-1".to_string(), store.clone(), config.clone());
    let pod2 = MockPod::new("pod-2".to_string(), store.clone(), config.clone());
    let pod3 = MockPod::new("pod-3".to_string(), store.clone(), config.clone());

    let scope = "test-metrics";

    // All pods try to acquire
    pod1.try_acquire(scope).await;
    pod2.try_acquire(scope).await;
    pod3.try_acquire(scope).await;

    // Get metrics from all pods
    let metrics1 = pod1.leader_election.metrics().await;
    let metrics2 = pod2.leader_election.metrics().await;
    let metrics3 = pod3.leader_election.metrics().await;

    // Sum of leader_status should be exactly 1
    let sum = metrics1.leader_status.get(scope).unwrap_or(&0.0)
        + metrics2.leader_status.get(scope).unwrap_or(&0.0)
        + metrics3.leader_status.get(scope).unwrap_or(&0.0);

    assert_eq!(sum, 1.0, "miroir_leader sum across all pods should be 1, got {}", sum);
}

#[tokio::test]
async fn ac9_leader_metrics_transient_zero_during_failover() {
    let store = test_store().await;
    let config = fast_test_config();

    let pod1 = MockPod::new("pod-1".to_string(), store.clone(), config.clone());
    let pod2 = MockPod::new("pod-2".to_string(), store.clone(), config.clone());

    let scope = "test-metrics-failover";

    // Pod 1 acquires
    pod1.try_acquire(scope).await;

    // Get initial metrics
    let metrics1_initial = pod1.leader_election.metrics().await;
    assert_eq!(
        metrics1_initial.leader_status.get(scope).unwrap_or(&0.0),
        &1.0
    );

    // Pod 1 steps down
    pod1.step_down(scope).await;

    // Briefly, there should be no leader (transient 0)
    let metrics1_after = pod1.leader_election.metrics().await;
    assert_eq!(
        metrics1_after.leader_status.get(scope).unwrap_or(&0.0),
        &0.0
    );

    // Pod 2 acquires
    pod2.try_acquire(scope).await;

    // Now pod 2 should be leader
    let metrics2_final = pod2.leader_election.metrics().await;
    assert_eq!(
        metrics2_final.leader_status.get(scope).unwrap_or(&0.0),
        &1.0
    );
}

// ---------------------------------------------------------------------------
// Additional: Multiple concurrent operations with different scopes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ac10_multiple_concurrent_operations_different_scopes() {
    let store = test_store().await;
    let config = test_config();

    let pod1 = MockPod::new("pod-1".to_string(), store.clone(), config.clone());
    let pod2 = MockPod::new("pod-2".to_string(), store.clone(), config.clone());

    // Pod 1 leads reshard, pod 2 leads rebalance
    let reshard_scope = "reshard:products";
    let rebalance_scope = "rebalance";

    assert!(pod1.try_acquire(reshard_scope).await);
    assert!(pod2.try_acquire(rebalance_scope).await);

    // Both should be leaders for their respective scopes
    assert!(pod1.is_leader(reshard_scope));
    assert!(!pod2.is_leader(reshard_scope));
    assert!(!pod1.is_leader(rebalance_scope));
    assert!(pod2.is_leader(rebalance_scope));

    // Verify both operations can persist state independently
    let reshard_op = ModeBOperation {
        operation_id: "reshard-products-1".to_string(),
        operation_type: mode_b_type::RESHARD.to_string(),
        scope: reshard_scope.to_string(),
        phase: "backfill".to_string(),
        phase_started_at: 1000,
        created_at: 500,
        updated_at: 1500,
        state_json: r#"{"shadow_index":"products_shadow"}"#.to_string(),
        error: None,
        status: mode_b_status::RUNNING.to_string(),
        index_uid: Some("products".to_string()),
        old_shards: Some(4),
        target_shards: Some(8),
        shadow_index: Some("products_shadow".to_string()),
        documents_backfilled: Some(1000),
        total_documents: Some(10000),
    };

    let rebalance_op = ModeBOperation {
        operation_id: "rebalance-1".to_string(),
        operation_type: mode_b_type::REBALANCE.to_string(),
        scope: rebalance_scope.to_string(),
        phase: "migrating".to_string(),
        phase_started_at: 2000,
        created_at: 1500,
        updated_at: 2500,
        state_json: r#"{"shards_migrated":2}"#.to_string(),
        error: None,
        status: mode_b_status::RUNNING.to_string(),
        index_uid: None,
        old_shards: None,
        target_shards: None,
        shadow_index: None,
        documents_backfilled: None,
        total_documents: None,
    };

    pod1.leader_election
        .persist_mode_b_operation(&reshard_op)
        .unwrap();
    pod2.leader_election
        .persist_mode_b_operation(&rebalance_op)
        .unwrap();

    // Verify both can be recovered
    let recovered_reshard = pod1
        .leader_election
        .recover_mode_b_operation(reshard_scope)
        .unwrap()
        .unwrap();
    assert_eq!(recovered_reshard.phase, "backfill");

    let recovered_rebalance = pod2
        .leader_election
        .recover_mode_b_operation(rebalance_scope)
        .unwrap()
        .unwrap();
    assert_eq!(recovered_rebalance.phase, "migrating");
}

// ---------------------------------------------------------------------------
// Lease expiration handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ac11_expired_lease_allows_new_leader() {
    let store = test_store().await;
    let config = fast_test_config(); // 2 second TTL

    let pod1 = MockPod::new("pod-1".to_string(), store.clone(), config.clone());
    let pod2 = MockPod::new("pod-2".to_string(), store.clone(), config.clone());

    let scope = "test-expiration";

    // Pod 1 acquires leadership
    assert!(pod1.try_acquire(scope).await);

    // Wait for lease to expire (2 seconds + buffer)
    sleep(Duration::from_secs(3)).await;

    // Pod 2 should now be able to acquire leadership
    assert!(pod2.try_acquire(scope).await);
    assert_eq!(pod2.get_leader(scope).await, Some("pod-2".to_string()));

    // Pod 1 should no longer be leader
    assert!(!pod1.is_leader(scope));
}

#[tokio::test]
async fn ac12_stale_leader_cannot_renew_expired_lease() {
    let store = test_store().await;
    let config = fast_test_config(); // 2 second TTL

    let pod1 = MockPod::new("pod-1".to_string(), store.clone(), config.clone());
    let pod2 = MockPod::new("pod-2".to_string(), store.clone(), config.clone());

    let scope = "test-stale-renewal";

    // Pod 1 acquires leadership
    assert!(pod1.try_acquire(scope).await);

    // Wait for lease to expire
    sleep(Duration::from_secs(3)).await;

    // Pod 1 tries to renew (should fail)
    assert!(!pod1.renew(scope).await);

    // Pod 2 acquires
    assert!(pod2.try_acquire(scope).await);

    // Pod 1 tries to renew again (should still fail)
    assert!(!pod1.renew(scope).await);
}
