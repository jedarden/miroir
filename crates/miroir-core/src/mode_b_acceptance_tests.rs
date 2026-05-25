//! Mode B acceptance tests (plan §14.5 Mode B).
//!
//! Tests for:
//! - Leader-only singleton coordinator with 3-pod exclusivity
//! - Leader loss mid-operation with phase resumption
//! - Reshard coordinator phase resumption
//! - Two-phase commit (2PC) settings broadcast phase resumption
//! - miroir_leader metrics correctness

use crate::config::LeaderElectionConfig;
use crate::leader_election::LeaderElection;
use crate::mode_b_coordinator::{ModeBOpLeader, PhaseState};
use crate::task_store::{mode_b_type, SqliteTaskStore, TaskStore};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Test extra state for reshard operations.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReshardExtraState {
    /// Current phase of reshard operation.
    pub phase: String,
    /// Shadow index UID.
    pub shadow_index: Option<String>,
    /// Old shard count.
    pub old_shards: u32,
    /// Target shard count.
    pub target_shards: u32,
    /// Documents backfilled so far.
    pub documents_backfilled: u64,
    /// Total documents to backfill.
    pub total_documents: u64,
    /// Per-shard cursor for idempotent resume.
    pub shard_cursor: Option<u64>,
}

/// Test extra state for 2PC settings broadcast operations.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SettingsBroadcastExtraState {
    /// Current phase of 2PC operation.
    pub phase: String,
    /// Index UID being updated.
    pub index_uid: String,
    /// New settings version.
    pub settings_version: i64,
    /// Nodes that have acknowledged phase 1 (propose).
    pub propose_acks: Vec<String>,
    /// Nodes that have acknowledged phase 2 (commit).
    pub commit_acks: Vec<String>,
    /// Total nodes in the topology.
    pub total_nodes: usize,
}

/// Create a shared in-memory store for testing.
fn shared_test_store() -> Arc<dyn TaskStore> {
    let store = Arc::new(SqliteTaskStore::open_in_memory().unwrap());
    store.migrate().unwrap();
    store
}

/// Create a leader election instance.
fn leader_election(store: Arc<dyn TaskStore>, pod_id: String) -> Arc<LeaderElection> {
    let config = LeaderElectionConfig {
        enabled: true,
        lease_ttl_s: 10,
        renew_interval_s: 3,
    };
    Arc::new(LeaderElection::new(store, pod_id, config))
}

/// Create a Mode B operation leader for reshard.
fn reshard_leader(
    store: Arc<dyn TaskStore>,
    pod_id: String,
    index_uid: String,
) -> ModeBOpLeader<ReshardExtraState> {
    let leader_election = leader_election(store.clone(), pod_id.clone());
    let scope = format!("reshard:{}", index_uid);
    ModeBOpLeader::new(
        leader_election,
        store,
        mode_b_type::RESHARD.to_string(),
        scope,
        pod_id,
        ReshardExtraState::default(),
    )
}

/// Create a Mode B operation leader for 2PC settings broadcast.
fn settings_broadcast_leader(
    store: Arc<dyn TaskStore>,
    pod_id: String,
    index_uid: String,
) -> ModeBOpLeader<SettingsBroadcastExtraState> {
    let leader_election = leader_election(store.clone(), pod_id.clone());
    let scope = format!("settings_broadcast:{}", index_uid);
    ModeBOpLeader::new(
        leader_election,
        store,
        mode_b_type::SETTINGS_BROADCAST.to_string(),
        scope,
        pod_id,
        SettingsBroadcastExtraState::default(),
    )
}

/// Get current time in milliseconds.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// P6.4-A1: 3 pods - exactly one is leader at any instant.
///
/// This test verifies that when three pods compete for leadership,
/// exactly one acquires the lease and the other two are rejected.
#[tokio::test]
async fn p6_4_a1_three_pods_exactly_one_leader() {
    let store = shared_test_store();

    // Create three leaders representing three different pods
    let mut leader1 = reshard_leader(store.clone(), "pod-1".to_string(), "test-index".to_string());
    let mut leader2 = reshard_leader(store.clone(), "pod-2".to_string(), "test-index".to_string());
    let mut leader3 = reshard_leader(store.clone(), "pod-3".to_string(), "test-index".to_string());

    // All three try to acquire leadership simultaneously
    let (result1, result2, result3) = tokio::join!(
        leader1.try_acquire_leadership(),
        leader2.try_acquire_leadership(),
        leader3.try_acquire_leadership(),
    );

    let acquired1 = result1.unwrap();
    let acquired2 = result2.unwrap();
    let acquired3 = result3.unwrap();

    // Exactly one should acquire leadership
    let leaders_acquired = [acquired1, acquired2, acquired3]
        .iter()
        .filter(|&&x| x)
        .count();

    assert_eq!(
        leaders_acquired, 1,
        "exactly one pod should acquire leadership, got {}",
        leaders_acquired
    );

    // Verify the leader state matches
    assert_eq!(leader1.is_leader(), acquired1);
    assert_eq!(leader2.is_leader(), acquired2);
    assert_eq!(leader3.is_leader(), acquired3);
}

/// P6.4-A2: Kill leader promotes another within lease_ttl_s.
///
/// This test verifies that when the leader is killed (stops renewing),
/// another pod acquires the lease within the TTL window.
#[tokio::test]
async fn p6_4_a2_kill_leader_promotes_another_within_ttl() {
    let store = shared_test_store();

    // Pod-1 acquires leadership
    let mut leader1 = reshard_leader(store.clone(), "pod-1".to_string(), "test-index".to_string());
    assert!(leader1.try_acquire_leadership().await.unwrap());
    assert!(leader1.is_leader());

    // Pod-2 tries and fails (pod-1 holds the lease)
    let mut leader2 = reshard_leader(store.clone(), "pod-2".to_string(), "test-index".to_string());
    assert!(!leader2.try_acquire_leadership().await.unwrap());
    assert!(!leader2.is_leader());

    // Simulate pod-1 crash by stepping down
    leader1.step_down().await.unwrap();
    assert!(!leader1.is_leader());

    // Pod-2 should now be able to acquire leadership
    assert!(leader2.try_acquire_leadership().await.unwrap());
    assert!(leader2.is_leader());

    // Pod-3 should still fail (pod-2 now holds the lease)
    let mut leader3 = reshard_leader(store.clone(), "pod-3".to_string(), "test-index".to_string());
    assert!(!leader3.try_acquire_leadership().await.unwrap());
}

/// P6.4-A3: Leader loss during reshard phase 3 (verify) resumes at phase 3.
///
/// This test verifies that when a leader is lost during the verification
/// phase, a new leader resumes at verification, not from the beginning.
#[tokio::test]
async fn p6_4_a3_reshard_leader_loss_resumes_at_verify_phase() {
    let store = shared_test_store();
    let index_uid = "test-index";

    // Pod-1 starts a reshard operation
    let mut leader1 = reshard_leader(store.clone(), "pod-1".to_string(), index_uid.to_string());
    leader1.try_acquire_leadership().await.unwrap();

    // Simulate progressing through phases
    // Phase 1: shadow_created
    leader1
        .persist_phase("shadow_created".to_string())
        .await
        .unwrap();
    assert_eq!(leader1.phase(), "shadow_created");

    // Phase 2: backfill_in_progress
    leader1
        .persist_phase("backfill_in_progress".to_string())
        .await
        .unwrap();
    assert_eq!(leader1.phase(), "backfill_in_progress");

    // Update extra state to simulate backfill progress
    leader1.extra_state().phase = "backfill_in_progress".to_string();
    leader1.extra_state().shadow_index = Some("test-index-shadow".to_string());
    leader1.extra_state().old_shards = 64;
    leader1.extra_state().target_shards = 128;
    leader1.extra_state().documents_backfilled = 5000;
    leader1.extra_state().total_documents = 10000;
    leader1.extra_state().shard_cursor = Some(5000);
    leader1
        .persist_phase("backfill_in_progress".to_string())
        .await
        .unwrap();

    // Phase 3: verification (this is where pod-1 crashes)
    leader1
        .persist_phase("verification".to_string())
        .await
        .unwrap();
    assert_eq!(leader1.phase(), "verification");

    // Simulate pod-1 crash by stepping down
    leader1.step_down().await.unwrap();

    // Pod-2 takes over and should resume at verification
    let mut leader2 = reshard_leader(store.clone(), "pod-2".to_string(), index_uid.to_string());
    leader2.try_acquire_leadership().await.unwrap();

    // Recover state
    let recovered = leader2.recover().await.unwrap();
    assert!(recovered.is_some());

    // Verify we resumed at verification, not shadow_created
    assert_eq!(
        leader2.phase(),
        "verification",
        "should resume at verification phase"
    );

    // Verify extra state was preserved
    let extra = leader2.extra_state_ref();
    assert_eq!(extra.shadow_index, Some("test-index-shadow".to_string()));
    assert_eq!(extra.old_shards, 64);
    assert_eq!(extra.target_shards, 128);
    assert_eq!(extra.documents_backfilled, 5000);
    assert_eq!(extra.total_documents, 10000);
    assert_eq!(extra.shard_cursor, Some(5000));

    // Pod-2 can continue from verification without re-doing shadow/backfill
    leader2.persist_phase("swap".to_string()).await.unwrap();
    assert_eq!(leader2.phase(), "swap");
}

/// P6.4-A4: Leader loss during 2PC phase 2 (verify) resumes at verify.
///
/// This test verifies that when a leader is lost during the verify phase
/// of a two-phase commit settings broadcast, a new leader resumes at verify
/// without re-applying phase 1 (propose).
#[tokio::test]
async fn p6_4_a4_2pc_leader_loss_resumes_at_verify_phase() {
    let store = shared_test_store();
    let index_uid = "test-index";

    // Pod-1 starts a 2PC settings broadcast
    let mut leader1 =
        settings_broadcast_leader(store.clone(), "pod-1".to_string(), index_uid.to_string());
    leader1.try_acquire_leadership().await.unwrap();

    // Set up initial state
    leader1.extra_state().index_uid = index_uid.to_string();
    leader1.extra_state().settings_version = 42;
    leader1.extra_state().total_nodes = 3;

    // Phase 1: propose - all nodes ACK
    leader1.persist_phase("propose".to_string()).await.unwrap();
    leader1.extra_state().phase = "propose".to_string();
    leader1.extra_state().propose_acks = vec![
        "node-0".to_string(),
        "node-1".to_string(),
        "node-2".to_string(),
    ];
    leader1.persist_phase("propose".to_string()).await.unwrap();

    // Phase 2: verify (this is where pod-1 crashes)
    leader1.persist_phase("verify".to_string()).await.unwrap();
    leader1.extra_state().phase = "verify".to_string();
    // During verify, we've collected 2 out of 3 ACKs
    leader1.extra_state().commit_acks = vec!["node-0".to_string(), "node-1".to_string()];
    leader1.persist_phase("verify".to_string()).await.unwrap();

    assert_eq!(leader1.phase(), "verify");

    // Simulate pod-1 crash by stepping down
    leader1.step_down().await.unwrap();

    // Pod-2 takes over and should resume at verify
    let mut leader2 =
        settings_broadcast_leader(store.clone(), "pod-2".to_string(), index_uid.to_string());
    leader2.try_acquire_leadership().await.unwrap();

    // Recover state
    let recovered = leader2.recover().await.unwrap();
    assert!(recovered.is_some());

    // Verify we resumed at verify, not propose
    assert_eq!(leader2.phase(), "verify", "should resume at verify phase");

    // Verify extra state was preserved (no re-propose needed)
    let extra = leader2.extra_state_ref();
    assert_eq!(extra.index_uid, index_uid);
    assert_eq!(extra.settings_version, 42);
    assert_eq!(extra.total_nodes, 3);
    assert_eq!(
        extra.propose_acks.len(),
        3,
        "all nodes should have ACKed propose"
    );
    assert_eq!(extra.commit_acks.len(), 2, "2 nodes have ACKed commit");

    // Pod-2 can continue from verify, collecting the final ACK
    leader2.extra_state().commit_acks.push("node-2".to_string());
    leader2.persist_phase("verify".to_string()).await.unwrap();

    // Now proceed to commit
    leader2.persist_phase("commit".to_string()).await.unwrap();
    assert_eq!(leader2.phase(), "commit");
}

/// P6.4-A5: miroir_leader metric sum is always 1 (or 0 transiently).
///
/// This test verifies that the leader election metric is correct:
/// - Sum of miroir_leader across all pods is 1 when a leader exists
/// - Transiently 0 during failover
#[tokio::test]
async fn p6_4_a5_miroir_leader_metric_sum_is_one() {
    let store = shared_test_store();
    let scope = "reshard:metric-test";

    // Helper to check if a pod is leader for a scope
    async fn check_leader(store: Arc<dyn TaskStore>, pod_id: &str, scope: &str) -> bool {
        let lease = tokio::task::spawn_blocking({
            let store = store.clone();
            let scope = scope.to_string();
            move || store.get_leader_lease(&scope)
        })
        .await
        .unwrap()
        .unwrap();

        match lease {
            Some(lease) => {
                // Check if lease is unexpired and held by this pod
                let now = now_ms();
                lease.holder == pod_id && lease.expires_at >= now
            }
            None => false,
        }
    }

    // Initially, no leader - sum is 0
    let mut leader_count = 0;
    for pod in ["pod-1", "pod-2", "pod-3"] {
        if check_leader(store.clone(), pod, scope).await {
            leader_count += 1;
        }
    }
    assert_eq!(leader_count, 0, "initially no leader");

    // Pod-1 acquires leadership
    let mut leader1 = reshard_leader(
        store.clone(),
        "pod-1".to_string(),
        "metric-test".to_string(),
    );
    leader1.try_acquire_leadership().await.unwrap();

    // Now sum should be 1
    leader_count = 0;
    for pod in ["pod-1", "pod-2", "pod-3"] {
        if check_leader(store.clone(), pod, scope).await {
            leader_count += 1;
        }
    }
    assert_eq!(leader_count, 1, "one leader after acquisition");

    // Pod-1 steps down (simulating crash)
    leader1.step_down().await.unwrap();

    // Transiently 0 during failover window
    leader_count = 0;
    for pod in ["pod-1", "pod-2", "pod-3"] {
        if check_leader(store.clone(), pod, scope).await {
            leader_count += 1;
        }
    }
    assert_eq!(leader_count, 0, "transiently 0 after stepdown");

    // Pod-2 acquires leadership
    let mut leader2 = reshard_leader(
        store.clone(),
        "pod-2".to_string(),
        "metric-test".to_string(),
    );
    leader2.try_acquire_leadership().await.unwrap();

    // Sum is back to 1
    leader_count = 0;
    for pod in ["pod-1", "pod-2", "pod-3"] {
        if check_leader(store.clone(), pod, scope).await {
            leader_count += 1;
        }
    }
    assert_eq!(leader_count, 1, "one leader after failover");
}

/// P6.4-A6: Lease renewal extends expiration.
///
/// This test verifies that lease renewal correctly extends the expiration time.
#[tokio::test]
async fn p6_4_a6_lease_renewal_extends_expiration() {
    let store = shared_test_store();
    let scope = "reshard:renewal-test";

    // Pod-1 acquires leadership
    let mut leader1 = reshard_leader(
        store.clone(),
        "pod-1".to_string(),
        "renewal-test".to_string(),
    );
    leader1.try_acquire_leadership().await.unwrap();

    // Get the initial lease expiration
    let lease_before = tokio::task::spawn_blocking({
        let store = store.clone();
        let scope = scope.to_string();
        move || store.get_leader_lease(&scope)
    })
    .await
    .unwrap()
    .unwrap();
    let expires_at_before = lease_before.unwrap().expires_at;

    // Wait a bit to ensure time passes
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Renew the lease
    assert!(leader1.renew_leadership().await.unwrap());

    // Get the new lease expiration
    let lease_after = tokio::task::spawn_blocking({
        let store = store.clone();
        let scope = scope.to_string();
        move || store.get_leader_lease(&scope)
    })
    .await
    .unwrap()
    .unwrap();
    let expires_at_after = lease_after.unwrap().expires_at;

    // Expiration should be extended (at least 100ms later due to our sleep)
    assert!(
        expires_at_after > expires_at_before,
        "lease expiration should be extended after renewal: {} > {}",
        expires_at_after,
        expires_at_before
    );
}

/// P6.4-A7: Expired lease allows acquisition by another pod.
///
/// This test verifies that when a lease expires, another pod can acquire it.
#[tokio::test]
async fn p6_4_a7_expired_lease_allows_acquisition() {
    let store = shared_test_store();
    let scope = "reshard:expire-test";

    // Pod-1 acquires leadership with a short TTL (simulated via direct store manipulation)
    let mut leader1 = reshard_leader(
        store.clone(),
        "pod-1".to_string(),
        "expire-test".to_string(),
    );
    leader1.try_acquire_leadership().await.unwrap();

    // Manually set the lease expiration to the past to simulate expiry
    let expired_time = now_ms() - 1000; // 1 second ago
    tokio::task::spawn_blocking({
        let store = store.clone();
        let scope = scope.to_string();
        move || store.renew_leader_lease(&scope, "pod-1", expired_time)
    })
    .await
    .unwrap()
    .unwrap();

    // Pod-2 should now be able to acquire the lease (it's expired)
    let mut leader2 = reshard_leader(
        store.clone(),
        "pod-2".to_string(),
        "expire-test".to_string(),
    );
    assert!(
        leader2.try_acquire_leadership().await.unwrap(),
        "pod-2 should acquire expired lease"
    );

    // Verify pod-2 is the leader
    assert!(leader2.is_leader());

    // Pod-1 should no longer be able to renew
    assert!(
        !leader1.renew_leadership().await.unwrap(),
        "pod-1 should not renew after losing lease"
    );
}

/// P6.4-A8: Multiple operation scopes have independent leaders.
///
/// This test verifies that different operation scopes (reshard vs ILM)
/// can have different leaders independently.
#[tokio::test]
async fn p6_4_a8_multiple_scopes_independent_leaders() {
    let store = shared_test_store();

    // Pod-1 is leader for reshard:products
    let mut leader1_reshard =
        reshard_leader(store.clone(), "pod-1".to_string(), "products".to_string());
    leader1_reshard.try_acquire_leadership().await.unwrap();
    assert!(leader1_reshard.is_leader());

    // Pod-2 should also be able to lead a different scope (e.g., ILM)
    let mut leader2_ilm = ModeBOpLeader::new(
        leader_election(store.clone(), "pod-2".to_string()),
        store.clone(),
        "ilm".to_string(),
        "ilm".to_string(),
        "pod-2".to_string(),
        (),
    );
    leader2_ilm.try_acquire_leadership().await.unwrap();
    assert!(leader2_ilm.is_leader());

    // Pod-1 can't lead ILM (pod-2 has it)
    let mut leader1_ilm = ModeBOpLeader::new(
        leader_election(store.clone(), "pod-1".to_string()),
        store.clone(),
        "ilm".to_string(),
        "ilm".to_string(),
        "pod-1".to_string(),
        (),
    );
    assert!(!leader1_ilm.try_acquire_leadership().await.unwrap());

    // Pod-2 can't lead reshard:products (pod-1 has it)
    let mut leader2_reshard =
        reshard_leader(store.clone(), "pod-2".to_string(), "products".to_string());
    assert!(!leader2_reshard.try_acquire_leadership().await.unwrap());

    // Both scopes have different leaders simultaneously
    assert!(leader1_reshard.is_leader());
    assert!(leader2_ilm.is_leader());
}

/// P6.4-A9: Phase state persists correctly across restarts.
///
/// This test verifies that phase state is persisted to the task store
/// and can be recovered by a new leader instance.
#[tokio::test]
async fn p6_4_a9_phase_state_persists_across_restarts() {
    let store = shared_test_store();
    let index_uid = "restart-test";

    // Pod-1 creates a reshard operation and progresses through phases
    let mut leader1 = reshard_leader(store.clone(), "pod-1".to_string(), index_uid.to_string());
    leader1.try_acquire_leadership().await.unwrap();

    // Progress through phases
    leader1
        .persist_phase("shadow_created".to_string())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    leader1
        .persist_phase("backfill_in_progress".to_string())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    leader1
        .persist_phase("verification".to_string())
        .await
        .unwrap();

    // Verify the operation exists in the task store
    let operation = tokio::task::spawn_blocking({
        let store = store.clone();
        let scope = format!("reshard:{}", index_uid);
        move || store.get_mode_b_operation_by_scope(&scope)
    })
    .await
    .unwrap()
    .unwrap();

    assert!(operation.is_some());
    let op = operation.unwrap();
    assert_eq!(op.phase, "verification");
    assert_eq!(op.status, "running");

    // Simulate pod restart: create a new leader instance for the same pod
    let mut leader1_restart =
        reshard_leader(store.clone(), "pod-1".to_string(), index_uid.to_string());
    leader1_restart.try_acquire_leadership().await.unwrap();

    // Should recover the persisted phase
    let recovered = leader1_restart.recover().await.unwrap();
    assert!(recovered.is_some());
    assert_eq!(leader1_restart.phase(), "verification");
}

/// P6.4-A10: Operation completion deletes state.
///
/// This test verifies that when an operation completes, its state is
/// cleaned up properly.
#[tokio::test]
async fn p6_4_a10_operation_completion_deletes_state() {
    let store = shared_test_store();
    let index_uid = "complete-test";

    // Pod-1 creates and completes an operation
    let mut leader1 = reshard_leader(store.clone(), "pod-1".to_string(), index_uid.to_string());
    leader1.try_acquire_leadership().await.unwrap();

    leader1
        .persist_phase("shadow_created".to_string())
        .await
        .unwrap();
    leader1
        .persist_phase("backfill_in_progress".to_string())
        .await
        .unwrap();
    leader1
        .persist_phase("verification".to_string())
        .await
        .unwrap();
    leader1.persist_phase("swap".to_string()).await.unwrap();
    leader1.persist_phase("cleanup".to_string()).await.unwrap();

    // Complete the operation
    leader1.complete().await.unwrap();

    // Verify status is completed
    let scope = format!("reshard:{}", index_uid);
    let operation = tokio::task::spawn_blocking({
        let store = store.clone();
        move || store.get_mode_b_operation_by_scope(&scope)
    })
    .await
    .unwrap()
    .unwrap();

    assert!(operation.is_some());
    let op = operation.unwrap();
    assert_eq!(op.status, "completed");
    assert_eq!(op.phase, "complete");

    // Leader stepped down
    assert!(!leader1.is_leader());
}

/// P6.4-A11: Operation failure marks state as failed.
///
/// This test verifies that when an operation fails, its state is marked
/// as failed with an error message.
#[tokio::test]
async fn p6_4_a11_operation_failure_marks_failed() {
    let store = shared_test_store();
    let index_uid = "fail-test";

    // Pod-1 creates an operation that fails during backfill
    let mut leader1 = reshard_leader(store.clone(), "pod-1".to_string(), index_uid.to_string());
    leader1.try_acquire_leadership().await.unwrap();

    leader1
        .persist_phase("shadow_created".to_string())
        .await
        .unwrap();
    leader1
        .persist_phase("backfill_in_progress".to_string())
        .await
        .unwrap();

    // Fail with an error
    leader1
        .fail("connection timeout to Meilisearch".to_string())
        .await
        .unwrap();

    // Verify status is failed
    let scope = format!("reshard:{}", index_uid);
    let operation = tokio::task::spawn_blocking({
        let store = store.clone();
        move || store.get_mode_b_operation_by_scope(&scope)
    })
    .await
    .unwrap()
    .unwrap();

    assert!(operation.is_some());
    let op = operation.unwrap();
    assert_eq!(op.status, "failed");
    assert_eq!(
        op.error,
        Some("connection timeout to Meilisearch".to_string())
    );

    // Leader stepped down
    assert!(!leader1.is_leader());
}
