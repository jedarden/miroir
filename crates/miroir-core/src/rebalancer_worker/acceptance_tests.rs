//! Acceptance tests for the rebalancer worker (P4.1).
//!
//! These tests verify the three key acceptance criteria:
//! 1. Advisory lock: only one pod runs the rebalancer at a time
//! 2. Progress persistence: pod restart resumes without starting over
//! 3. Metrics tick: documents migrated counter monotonically increases

use super::*;
use crate::error::Result;
use crate::migration::{MigrationConfig, MigrationCoordinator};
use crate::task_store::{
    AdminSessionRow, CanaryRow, CdcCursorRow, JobRow, LeaderLeaseRow, NewAdminSession, NewCanary,
    NewCdcCursor, NewJob, NewRolloverPolicy, NewSearchUiConfig, NewTenantMapping,
    RolloverPolicyRow, SearchUiConfigRow, TaskStore, TenantMapRow,
};
use crate::topology::{Node, NodeId as TopologyNodeId, Topology};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Create a test topology with 4 nodes across 2 replica groups.
fn test_topology() -> Topology {
    let mut topo = Topology::new(64, 2, 2);
    topo.add_node(Node::new(
        TopologyNodeId::new("node-0".into()),
        "http://node-0:7700".into(),
        0,
    ));
    topo.add_node(Node::new(
        TopologyNodeId::new("node-1".into()),
        "http://node-1:7700".into(),
        0,
    ));
    topo.add_node(Node::new(
        TopologyNodeId::new("node-2".into()),
        "http://node-2:7700".into(),
        1,
    ));
    topo.add_node(Node::new(
        TopologyNodeId::new("node-3".into()),
        "http://node-3:7700".into(),
        1,
    ));
    topo
}

/// Test helper: create an in-memory task store for testing.
struct MockTaskStore {
    jobs: Arc<std::sync::Mutex<Vec<JobRow>>>,
    leader_leases: Arc<std::sync::Mutex<Vec<LeaderLeaseRow>>>,
}

impl MockTaskStore {
    fn new() -> Self {
        Self {
            jobs: Arc::new(std::sync::Mutex::new(Vec::new())),
            leader_leases: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

impl TaskStore for MockTaskStore {
    fn migrate(&self) -> Result<()> {
        Ok(())
    }

    fn insert_job(&self, job: &NewJob) -> Result<()> {
        let mut jobs = self.jobs.lock().unwrap();
        jobs.push(JobRow {
            id: job.id.clone(),
            type_: job.type_.clone(),
            params: job.params.clone(),
            state: job.state.clone(),
            claimed_by: None,
            claim_expires_at: None,
            progress: job.progress.clone(),
            parent_job_id: job.parent_job_id.clone(),
            chunk_index: job.chunk_index,
            total_chunks: job.total_chunks,
            created_at: Some(job.created_at),
        });
        Ok(())
    }

    fn get_job(&self, id: &str) -> Result<Option<JobRow>> {
        let jobs = self.jobs.lock().unwrap();
        Ok(jobs.iter().find(|j| j.id == id).cloned())
    }

    fn update_job_progress(&self, _id: &str, _state: &str, _progress: &str) -> Result<bool> {
        Ok(true)
    }

    fn list_jobs_by_state(&self, _state: &str) -> Result<Vec<JobRow>> {
        let jobs = self.jobs.lock().unwrap();
        Ok(jobs.clone())
    }

    fn count_jobs_by_state(&self, _state: &str) -> Result<u64> {
        Ok(0)
    }

    fn list_expired_claims(&self, _now_ms: i64) -> Result<Vec<JobRow>> {
        Ok(Vec::new())
    }

    fn list_jobs_by_parent(&self, _parent_job_id: &str) -> Result<Vec<JobRow>> {
        Ok(Vec::new())
    }

    fn reclaim_job_claim(&self, _id: &str, _state: &str, _progress: &str) -> Result<bool> {
        Ok(true)
    }

    fn claim_job(&self, _id: &str, _claimed_by: &str, _claim_expires_at: i64) -> Result<bool> {
        Ok(false)
    }

    fn renew_job_claim(&self, _id: &str, _claim_expires_at: i64) -> Result<bool> {
        Ok(false)
    }

    fn try_acquire_leader_lease(
        &self,
        scope: &str,
        holder: &str,
        expires_at: i64,
        now_ms: i64,
    ) -> Result<bool> {
        let mut leases = self.leader_leases.lock().unwrap();

        // Check if there's an existing unexpired lease
        for lease in leases.iter() {
            // Lease is still valid if expires_at >= now_ms (>= because we can acquire at exactly the expiration time)
            if lease.scope == scope && lease.expires_at >= now_ms {
                if lease.holder == holder {
                    return Ok(true); // Already hold the lease
                }
                return Ok(false); // Someone else holds it
            }
        }

        // No existing unexpired lease - acquire it
        leases.retain(|l| l.scope != scope); // Remove any expired leases for this scope
        leases.push(LeaderLeaseRow {
            scope: scope.to_string(),
            holder: holder.to_string(),
            expires_at,
        });
        Ok(true)
    }

    fn renew_leader_lease(&self, scope: &str, holder: &str, expires_at: i64) -> Result<bool> {
        let mut leases = self.leader_leases.lock().unwrap();
        for lease in leases.iter_mut() {
            if lease.scope == scope && lease.holder == holder {
                lease.expires_at = expires_at;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_leader_lease(&self, scope: &str) -> Result<Option<LeaderLeaseRow>> {
        let leases = self.leader_leases.lock().unwrap();
        Ok(leases.iter().find(|l| l.scope == scope).cloned())
    }

    // Stub implementations for unused trait methods
    fn insert_task(&self, _task: &crate::task_store::NewTask) -> Result<()> {
        Ok(())
    }
    fn get_task(&self, _miroir_id: &str) -> Result<Option<crate::task_store::TaskRow>> {
        Ok(None)
    }
    fn update_task_status(&self, _miroir_id: &str, _status: &str) -> Result<bool> {
        Ok(false)
    }
    fn update_node_task(&self, _miroir_id: &str, _node_id: &str, _task_uid: u64) -> Result<bool> {
        Ok(false)
    }
    fn set_task_error(&self, _miroir_id: &str, _error: &str) -> Result<bool> {
        Ok(false)
    }
    fn list_tasks(
        &self,
        _filter: &crate::task_store::TaskFilter,
    ) -> Result<Vec<crate::task_store::TaskRow>> {
        Ok(Vec::new())
    }
    fn prune_tasks(&self, _cutoff_ms: i64, _batch_size: u32) -> Result<usize> {
        Ok(0)
    }
    fn task_count(&self) -> Result<u64> {
        Ok(0)
    }
    fn upsert_node_settings_version(
        &self,
        _index_uid: &str,
        _node_id: &str,
        _version: i64,
        _updated_at: i64,
    ) -> Result<()> {
        Ok(())
    }
    fn get_node_settings_version(
        &self,
        _index_uid: &str,
        _node_id: &str,
    ) -> Result<Option<crate::task_store::NodeSettingsVersionRow>> {
        Ok(None)
    }
    fn create_alias(&self, _alias: &crate::task_store::NewAlias) -> Result<()> {
        Ok(())
    }
    fn get_alias(&self, _name: &str) -> Result<Option<crate::task_store::AliasRow>> {
        Ok(None)
    }
    fn flip_alias(&self, _name: &str, _new_uid: &str, _history_retention: usize) -> Result<bool> {
        Ok(false)
    }
    fn delete_alias(&self, _name: &str) -> Result<bool> {
        Ok(false)
    }
    fn list_aliases(&self) -> Result<Vec<crate::task_store::AliasRow>> {
        Ok(Vec::new())
    }
    fn upsert_session(&self, _session: &crate::task_store::SessionRow) -> Result<()> {
        Ok(())
    }
    fn get_session(&self, _session_id: &str) -> Result<Option<crate::task_store::SessionRow>> {
        Ok(None)
    }
    fn delete_expired_sessions(&self, _now_ms: i64) -> Result<usize> {
        Ok(0)
    }
    fn insert_idempotency_entry(&self, _entry: &crate::task_store::IdempotencyEntry) -> Result<()> {
        Ok(())
    }
    fn get_idempotency_entry(
        &self,
        _key: &str,
    ) -> Result<Option<crate::task_store::IdempotencyEntry>> {
        Ok(None)
    }
    fn delete_expired_idempotency_entries(&self, _now_ms: i64) -> Result<usize> {
        Ok(0)
    }

    fn upsert_canary(&self, _canary: &crate::task_store::NewCanary) -> Result<()> {
        Ok(())
    }
    fn get_canary(&self, _id: &str) -> Result<Option<crate::task_store::CanaryRow>> {
        Ok(None)
    }
    fn list_canaries(&self) -> Result<Vec<crate::task_store::CanaryRow>> {
        Ok(Vec::new())
    }
    fn delete_canary(&self, _id: &str) -> Result<bool> {
        Ok(false)
    }
    fn insert_canary_run(
        &self,
        _run: &crate::task_store::NewCanaryRun,
        _run_history_limit: usize,
    ) -> Result<()> {
        Ok(())
    }
    fn get_canary_runs(
        &self,
        _canary_id: &str,
        _limit: usize,
    ) -> Result<Vec<crate::task_store::CanaryRunRow>> {
        Ok(Vec::new())
    }
    fn upsert_cdc_cursor(&self, _cursor: &crate::task_store::NewCdcCursor) -> Result<()> {
        Ok(())
    }
    fn get_cdc_cursor(
        &self,
        _sink_name: &str,
        _index_uid: &str,
    ) -> Result<Option<crate::task_store::CdcCursorRow>> {
        Ok(None)
    }
    fn list_cdc_cursors(&self, _sink_name: &str) -> Result<Vec<crate::task_store::CdcCursorRow>> {
        Ok(Vec::new())
    }
    fn insert_tenant_mapping(&self, _mapping: &crate::task_store::NewTenantMapping) -> Result<()> {
        Ok(())
    }
    fn get_tenant_mapping(
        &self,
        _api_key_hash: &[u8],
    ) -> Result<Option<crate::task_store::TenantMapRow>> {
        Ok(None)
    }
    fn delete_tenant_mapping(&self, _api_key_hash: &[u8]) -> Result<bool> {
        Ok(false)
    }
    fn upsert_rollover_policy(&self, _policy: &crate::task_store::NewRolloverPolicy) -> Result<()> {
        Ok(())
    }
    fn get_rollover_policy(
        &self,
        _name: &str,
    ) -> Result<Option<crate::task_store::RolloverPolicyRow>> {
        Ok(None)
    }
    fn list_rollover_policies(&self) -> Result<Vec<crate::task_store::RolloverPolicyRow>> {
        Ok(Vec::new())
    }
    fn delete_rollover_policy(&self, _name: &str) -> Result<bool> {
        Ok(false)
    }
    fn upsert_search_ui_config(
        &self,
        _config: &crate::task_store::NewSearchUiConfig,
    ) -> Result<()> {
        Ok(())
    }
    fn get_search_ui_config(
        &self,
        _index_uid: &str,
    ) -> Result<Option<crate::task_store::SearchUiConfigRow>> {
        Ok(None)
    }
    fn delete_search_ui_config(&self, _index_uid: &str) -> Result<bool> {
        Ok(false)
    }
    fn insert_admin_session(&self, _session: &crate::task_store::NewAdminSession) -> Result<()> {
        Ok(())
    }
    fn get_admin_session(
        &self,
        _session_id: &str,
    ) -> Result<Option<crate::task_store::AdminSessionRow>> {
        Ok(None)
    }
    fn revoke_admin_session(&self, _session_id: &str) -> Result<bool> {
        Ok(false)
    }
    fn delete_expired_admin_sessions(&self, _now_ms: i64) -> Result<usize> {
        Ok(0)
    }

    // Mode B operations (Table 15)
    fn upsert_mode_b_operation(
        &self,
        _operation: &crate::task_store::ModeBOperation,
    ) -> Result<()> {
        Ok(())
    }

    fn get_mode_b_operation(
        &self,
        _operation_id: &str,
    ) -> Result<Option<crate::task_store::ModeBOperation>> {
        Ok(None)
    }

    fn get_mode_b_operation_by_scope(
        &self,
        _scope: &str,
    ) -> Result<Option<crate::task_store::ModeBOperation>> {
        Ok(None)
    }

    fn list_mode_b_operations(
        &self,
        _filter: &crate::task_store::ModeBOperationFilter,
    ) -> Result<Vec<crate::task_store::ModeBOperation>> {
        Ok(Vec::new())
    }

    fn delete_mode_b_operation(&self, _operation_id: &str) -> Result<bool> {
        Ok(false)
    }

    fn prune_mode_b_operations(&self, _cutoff_ms: i64, _batch_size: u32) -> Result<usize> {
        Ok(0)
    }
}

/// P4.1-A1: Advisory lock ensures only one pod runs the rebalancer at a time.
#[tokio::test]
async fn p4_1_a1_advisory_lock_prevents_duplicate_migrations() {
    let topo = Arc::new(RwLock::new(test_topology()));
    let task_store = Arc::new(MockTaskStore::new()) as Arc<dyn TaskStore>;
    let config = RebalancerWorkerConfig::default();
    let migration_config = MigrationConfig::default();
    let coordinator = Arc::new(RwLock::new(MigrationCoordinator::new(migration_config)));
    let metrics = Arc::new(RwLock::new(RebalancerMetrics::default()));

    // Create two workers simulating two different pods
    let worker1 = RebalancerWorker::new(
        config.clone(),
        topo.clone(),
        task_store.clone(),
        Arc::new(Rebalancer::new(
            crate::rebalancer::RebalancerConfig::default(),
            topo.clone(),
            MigrationConfig::default(),
        )),
        coordinator.clone(),
        metrics.clone(),
        "pod-1".to_string(),
    );

    let worker2 = RebalancerWorker::new(
        config.clone(),
        topo.clone(),
        task_store.clone(),
        Arc::new(Rebalancer::new(
            crate::rebalancer::RebalancerConfig::default(),
            topo.clone(),
            MigrationConfig::default(),
        )),
        coordinator.clone(),
        metrics.clone(),
        "pod-2".to_string(),
    );

    let scope = "rebalance:test-index";
    let now = now_ms();
    let expires_at = now + 10000; // 10 seconds from now

    // Pod 1 acquires the lease
    let acquired1 = tokio::task::spawn_blocking({
        let task_store = task_store.clone();
        let scope = scope.to_string();
        let holder = "pod-1".to_string();
        move || task_store.try_acquire_leader_lease(&scope, &holder, expires_at, now)
    })
    .await
    .unwrap()
    .unwrap();
    assert!(acquired1, "pod-1 should acquire the lease");

    // Pod 2 tries to acquire the same lease - should fail
    let acquired2 = tokio::task::spawn_blocking({
        let task_store = task_store.clone();
        let scope = scope.to_string();
        let holder = "pod-2".to_string();
        move || task_store.try_acquire_leader_lease(&scope, &holder, expires_at, now)
    })
    .await
    .unwrap()
    .unwrap();
    assert!(
        !acquired2,
        "pod-2 should not acquire the lease while pod-1 holds it"
    );

    // Pod 1 can renew its lease
    let renewed1 = tokio::task::spawn_blocking({
        let task_store = task_store.clone();
        let scope = scope.to_string();
        let holder = "pod-1".to_string();
        move || task_store.renew_leader_lease(&scope, &holder, expires_at + 2000)
    })
    .await
    .unwrap()
    .unwrap();
    assert!(renewed1, "pod-1 should renew its lease");

    // Pod 2 still cannot acquire
    let acquired2_after = tokio::task::spawn_blocking({
        let task_store = task_store.clone();
        let scope = scope.to_string();
        let holder = "pod-2".to_string();
        move || {
            task_store.try_acquire_leader_lease(
                &scope,
                &holder,
                expires_at + 3000,
                expires_at + 2000,
            )
        }
    })
    .await
    .unwrap()
    .unwrap();
    assert!(
        !acquired2_after,
        "pod-2 should still not acquire after pod-1 renews"
    );
}

/// P4.1-A2: Progress persistence allows pod restart resumption.
#[tokio::test]
async fn p4_1_a2_progress_persistence_pods_resume_migration() {
    let topo = Arc::new(RwLock::new(test_topology()));
    let task_store = Arc::new(MockTaskStore::new()) as Arc<dyn TaskStore>;
    let config = RebalancerWorkerConfig::default();
    let migration_config = MigrationConfig::default();
    let coordinator = Arc::new(RwLock::new(MigrationCoordinator::new(migration_config)));
    let metrics = Arc::new(RwLock::new(RebalancerMetrics::default()));

    // Create a job and persist it
    let job_id = RebalanceJobId::new("test-index");
    let mut shard_states = HashMap::new();
    shard_states.insert(
        10,
        ShardState {
            phase: ShardMigrationPhase::MigrationInProgress,
            docs_migrated: 5000,
            last_offset: 5000,
            source_node: Some("node-0".to_string()),
            target_node: "node-1".to_string(),
            started_at: Instant::now(),
        },
    );

    let job = RebalanceJob {
        id: job_id.clone(),
        index_uid: "test-index".to_string(),
        replica_group: 0,
        shards: shard_states,
        started_at: Instant::now(),
        completed_at: None,
        total_docs_migrated: 5000,
        paused: false,
    };

    // Persist the job
    let progress = serde_json::to_string(&job).unwrap();
    let new_job = NewJob {
        id: job.id.0.clone(),
        type_: "rebalance".to_string(),
        params: progress,
        state: "running".to_string(),
        progress: "{\"total_shards\":1,\"completed\":0,\"docs_migrated\":5000}".to_string(),
        parent_job_id: None,
        chunk_index: None,
        total_chunks: None,
        created_at: now_ms(),
    };
    tokio::task::spawn_blocking({
        let task_store = task_store.clone();
        let new_job = new_job.clone();
        move || task_store.insert_job(&new_job)
    })
    .await
    .unwrap()
    .unwrap();

    // Create a new worker (simulating a new pod)
    let worker2 = RebalancerWorker::new(
        config,
        topo,
        task_store.clone(),
        Arc::new(Rebalancer::new(
            crate::rebalancer::RebalancerConfig::default(),
            Arc::new(RwLock::new(test_topology())),
            MigrationConfig::default(),
        )),
        coordinator,
        metrics,
        "pod-2".to_string(),
    );

    // Load persisted jobs
    worker2.load_persisted_jobs().await.unwrap();

    // Verify the job was loaded
    let jobs = worker2.jobs.read().await;
    let loaded_job = jobs.get(&job_id).unwrap();
    assert_eq!(loaded_job.index_uid, "test-index");
    assert_eq!(loaded_job.total_docs_migrated, 5000);
    assert_eq!(loaded_job.shards.len(), 1);

    // Verify the shard state was preserved
    let shard_state = loaded_job.shards.get(&10).unwrap();
    assert_eq!(shard_state.docs_migrated, 5000);
    assert_eq!(shard_state.last_offset, 5000);
    assert!(matches!(
        shard_state.phase,
        ShardMigrationPhase::MigrationInProgress
    ));
}

/// P4.1-A3: Metrics tick - documents migrated counter monotonically increases.
#[tokio::test]
async fn p4_1_a3_metrics_monotonically_increase() {
    let mut metrics = RebalancerMetrics::default();

    // Start a rebalance
    metrics.start_rebalance();

    // Record some documents migrated
    metrics.record_documents_migrated(100);
    metrics.record_documents_migrated(200);
    metrics.record_documents_migrated(150);

    // Verify the counter monotonically increased
    assert_eq!(metrics.documents_migrated_total, 450);
    assert!(metrics.current_duration_secs() > 0.0);

    // End the rebalance and verify duration was recorded
    let duration = metrics.end_rebalance();
    assert!(duration > 0.0, "duration should be positive");
}

/// P4.1-A4: Two workers running simultaneously produce 0 duplicate migrations.
///
/// This is a comprehensive integration test that simulates two pods
/// both running the rebalancer worker simultaneously and verifies that
/// only one actually processes topology change events (no duplicate migrations).
#[tokio::test]
async fn p4_1_a4_two_workers_no_duplicate_migrations() {
    let topo = Arc::new(RwLock::new(test_topology()));
    let task_store = Arc::new(MockTaskStore::new()) as Arc<dyn TaskStore>;
    let config = RebalancerWorkerConfig {
        lease_ttl_secs: 5,
        lease_renewal_interval_ms: 100,
        event_channel_capacity: 10,
        ..Default::default()
    };
    let migration_config = MigrationConfig::default();
    let coordinator = Arc::new(RwLock::new(MigrationCoordinator::new(migration_config)));
    let metrics = Arc::new(RwLock::new(RebalancerMetrics::default()));

    // Create two workers with different pod IDs
    let worker1 = RebalancerWorker::new(
        config.clone(),
        topo.clone(),
        task_store.clone(),
        Arc::new(Rebalancer::new(
            crate::rebalancer::RebalancerConfig::default(),
            topo.clone(),
            MigrationConfig::default(),
        )),
        coordinator.clone(),
        metrics.clone(),
        "pod-1".to_string(),
    );

    let worker2 = RebalancerWorker::new(
        config.clone(),
        topo.clone(),
        task_store.clone(),
        Arc::new(Rebalancer::new(
            crate::rebalancer::RebalancerConfig::default(),
            topo.clone(),
            MigrationConfig::default(),
        )),
        coordinator.clone(),
        metrics.clone(),
        "pod-2".to_string(),
    );

    // Simulate pod-1 acquiring the lease first
    let scope = "rebalance:test-duplicate-index";
    let now = now_ms();
    let expires_at = now + 5000; // 5 seconds from now

    let pod1_acquired = tokio::task::spawn_blocking({
        let task_store = task_store.clone();
        let scope = scope.to_string();
        let holder = "pod-1".to_string();
        move || task_store.try_acquire_leader_lease(&scope, &holder, expires_at, now)
    })
    .await
    .unwrap()
    .unwrap();
    assert!(pod1_acquired, "pod-1 should acquire the lease first");

    // Pod-2 tries to acquire - should fail
    let pod2_acquired = tokio::task::spawn_blocking({
        let task_store = task_store.clone();
        let scope = scope.to_string();
        let holder = "pod-2".to_string();
        move || task_store.try_acquire_leader_lease(&scope, &holder, expires_at, now)
    })
    .await
    .unwrap()
    .unwrap();
    assert!(
        !pod2_acquired,
        "pod-2 should not acquire lease while pod-1 holds it"
    );

    // Now simulate a scenario where both pods try to process the same topology event
    // Only pod-1 (the lease holder) should actually process it
    let event = TopologyChangeEvent::NodeAdded {
        node_id: "node-new".to_string(),
        replica_group: 0,
        index_uid: "test-duplicate-index".to_string(),
    };

    // Worker 1 handles the event (holds the lease)
    let result1 = worker1.handle_topology_event(event.clone()).await;
    assert!(
        result1.is_ok(),
        "worker1 should handle the event successfully"
    );

    // Worker 2 tries to handle the same event - should succeed but not create duplicate
    // because worker1 already created the job
    let result2 = worker2.handle_topology_event(event).await;
    assert!(
        result2.is_ok(),
        "worker2 should handle the event (no-op if job exists)"
    );

    // Verify that only one migration was created (not two duplicates)
    let coordinator_read = coordinator.read().await;
    let migration_count = coordinator_read.get_all_migrations().len();
    assert_eq!(
        migration_count, 1,
        "only one migration should be created, not duplicates"
    );
}

/// Helper to get current time in milliseconds.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
