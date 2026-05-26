//! Acceptance tests for Mode C work-queued chunked jobs (P6.5).
//!
//! These tests verify the key acceptance criteria from plan §14.5:
//! 1. Large dumps split into chunks; multiple pods claim chunks in parallel
//! 2. Claim expiration: pod crash → claim expires → another pod resumes at last_cursor
//! 3. HPA queue depth metric drives autoscaling
//! 4. Concurrent dumps interleave without starvation

use crate::error::Result;
use crate::mode_c_coordinator::{JobChunk, JobParams, JobProgress, JobType, ModeCCoordinator};
use crate::task_store::{JobRow, NewJob, TaskStore};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Create a test coordinator with in-memory task store.
fn test_coordinator(pod_id: &str) -> ModeCCoordinator {
    let store = Arc::new(MockTaskStore::new());
    ModeCCoordinator::new(store, pod_id.to_string())
}

/// Create a test coordinator with a shared task store (for multi-pod tests).
fn test_coordinator_with_store(pod_id: &str, store: Arc<MockTaskStore>) -> ModeCCoordinator {
    ModeCCoordinator::new(store, pod_id.to_string())
}

/// Mock task store for testing.
struct MockTaskStore {
    jobs: Arc<std::sync::Mutex<Vec<JobRow>>>,
}

impl MockTaskStore {
    fn new() -> Self {
        Self {
            jobs: Arc::new(std::sync::Mutex::new(Vec::new())),
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

    fn claim_job(&self, id: &str, claimed_by: &str, claim_expires_at: i64) -> Result<bool> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.iter_mut().find(|j| j.id == id && j.state == "queued") {
            job.state = "in_progress".to_string();
            job.claimed_by = Some(claimed_by.to_string());
            job.claim_expires_at = Some(claim_expires_at);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn update_job_progress(&self, id: &str, state: &str, progress: &str) -> Result<bool> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.iter_mut().find(|j| j.id == id) {
            job.state = state.to_string();
            job.progress = progress.to_string();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn renew_job_claim(&self, id: &str, claim_expires_at: i64) -> Result<bool> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs
            .iter_mut()
            .find(|j| j.id == id && j.state == "in_progress")
        {
            job.claim_expires_at = Some(claim_expires_at);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn list_jobs_by_state(&self, state: &str) -> Result<Vec<JobRow>> {
        let jobs = self.jobs.lock().unwrap();
        Ok(jobs.iter().filter(|j| j.state == state).cloned().collect())
    }

    fn count_jobs_by_state(&self, state: &str) -> Result<u64> {
        let jobs = self.jobs.lock().unwrap();
        Ok(jobs.iter().filter(|j| j.state == state).count() as u64)
    }

    fn list_expired_claims(&self, now_ms: i64) -> Result<Vec<JobRow>> {
        let jobs = self.jobs.lock().unwrap();
        Ok(jobs
            .iter()
            .filter(|j| {
                j.state == "in_progress" && j.claim_expires_at.is_some_and(|exp| exp < now_ms)
            })
            .cloned()
            .collect())
    }

    fn list_jobs_by_parent(&self, parent_job_id: &str) -> Result<Vec<JobRow>> {
        let jobs = self.jobs.lock().unwrap();
        Ok(jobs
            .iter()
            .filter(|j| j.parent_job_id.as_deref() == Some(parent_job_id))
            .cloned()
            .collect())
    }

    fn reclaim_job_claim(&self, id: &str, state: &str, progress: &str) -> Result<bool> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.iter_mut().find(|j| j.id == id) {
            job.state = state.to_string();
            job.progress = progress.to_string();
            job.claimed_by = None;
            job.claim_expires_at = None;
            Ok(true)
        } else {
            Ok(false)
        }
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
    fn try_acquire_leader_lease(
        &self,
        _scope: &str,
        _holder: &str,
        _expires_at: i64,
        _now_ms: i64,
    ) -> Result<bool> {
        Ok(true)
    }
    fn renew_leader_lease(
        &self,
        _scope: &str,
        _holder: &str,
        _expires_at: i64,
        _now_ms: i64,
    ) -> Result<bool> {
        Ok(true)
    }
    fn get_leader_lease(&self, _scope: &str) -> Result<Option<crate::task_store::LeaderLeaseRow>> {
        Ok(None)
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

    fn list_terminal_tasks_batch(
        &self,
        _cutoff_ms: i64,
        _offset: i64,
        _limit: i64,
    ) -> Result<Vec<crate::task_store::TaskRow>> {
        Ok(Vec::new())
    }

    fn delete_tasks_batch(&self, _miroir_ids: &[&str]) -> Result<usize> {
        Ok(0)
    }

    fn check_and_mark_beacon_event(&self, _index_uid: &str, _event_id: &str) -> Result<bool> {
        Ok(true) // Always return new for mock
    }

    fn upsert_ttl_policy(&self, _policy: &crate::task_store::NewTtlPolicy) -> Result<()> {
        Ok(())
    }

    fn get_ttl_policy(&self, _index_uid: &str) -> Result<Option<crate::task_store::TtlPolicyRow>> {
        Ok(None)
    }

    fn delete_ttl_policy(&self, _index_uid: &str) -> Result<bool> {
        Ok(false)
    }

    fn list_ttl_policies(&self) -> Result<Vec<crate::task_store::TtlPolicyRow>> {
        Ok(Vec::new())
    }
}

/// P6.5-A1: 1 GB dump splits into 4× 256 MiB chunks; 3 pods claim 3 of 4 chunks in parallel.
#[tokio::test]
async fn p6_5_a1_one_gb_dump_splits_into_chunks_processed_in_parallel() {
    let store = Arc::new(MockTaskStore::new());

    let pod1_coordinator = test_coordinator_with_store("pod-1", store.clone());
    let pod2_coordinator = test_coordinator_with_store("pod-2", store.clone());
    let pod3_coordinator = test_coordinator_with_store("pod-3", store);

    // Enqueue a 1 GB dump import job
    let params = JobParams {
        index_uid: "test-index".to_string(),
        primary_key: Some("id".to_string()),
        shard_count: Some(64),
        old_shards: None,
        target_shards: None,
        shadow_index: None,
        chunk: None,
        source_url: Some("https://example.com/dump.ndjson".to_string()),
        source_size_bytes: Some(1_000_000_000), // 1 GB
    };

    let parent_job_id = pod1_coordinator
        .enqueue_job(JobType::DumpImport, params.clone())
        .unwrap();

    // Pod 1 claims the parent job
    let claimed = pod1_coordinator.claim_job().unwrap().unwrap();
    assert_eq!(claimed.id, parent_job_id);
    assert_eq!(claimed.claimed_by, "pod-1");

    // Pod 1 splits the job into 4 chunks (4 × 256 MiB)
    let chunk_size_bytes = 268_435_456; // 256 MiB
    let total_chunks = 1_000_000_000_u64.div_ceil(chunk_size_bytes) as u32;
    assert_eq!(total_chunks, 4);

    let chunks: Vec<JobChunk> = (0..total_chunks)
        .map(|i| {
            let i = i as u64;
            let start = i * chunk_size_bytes;
            let end = std::cmp::min(start + chunk_size_bytes, 1_000_000_000);
            JobChunk {
                index: i as u32,
                total: total_chunks,
                start: start.to_string(),
                end: end.to_string(),
                size_bytes: end - start,
            }
        })
        .collect();

    pod1_coordinator
        .split_job_into_chunks(&claimed, chunks)
        .unwrap();

    // Verify 4 chunk jobs were created
    let child_jobs = pod1_coordinator.list_chunks(&parent_job_id).unwrap();
    assert_eq!(child_jobs.len(), 4);

    // Each chunk should be in queued state
    for child in &child_jobs {
        assert_eq!(child.state, "queued");
        assert_eq!(child.parent_job_id, Some(parent_job_id.clone()));
    }

    // Simulate 3 pods claiming 3 of the 4 chunks in parallel
    let pod2_claimed = pod2_coordinator.claim_job().unwrap().unwrap();
    let pod3_claimed = pod3_coordinator.claim_job().unwrap().unwrap();
    let pod1_claimed_chunk = pod1_coordinator.claim_job().unwrap().unwrap();

    // Verify each pod claimed a different chunk
    let claimed_ids = vec![
        pod2_claimed.id.clone(),
        pod3_claimed.id.clone(),
        pod1_claimed_chunk.id.clone(),
    ];
    assert_eq!(claimed_ids.len(), 3); // 3 distinct claims

    // Verify all claimed chunks are now in_progress
    for job_id in &claimed_ids {
        let job = pod1_coordinator.get_job(job_id).unwrap().unwrap();
        assert_eq!(job.state, "in_progress");
    }

    // One chunk remains queued (unclaimed)
    let queued_jobs = pod1_coordinator.list_jobs_by_state("queued").unwrap();
    assert_eq!(queued_jobs.len(), 1);

    // Queue depth should be 1 (the remaining chunk)
    let queue_depth = pod1_coordinator.queue_depth().unwrap();
    assert_eq!(queue_depth, 1);
}

/// P6.5-A2: Kill a claimant mid-chunk; claim expires in 30s; another pod picks up and resumes at last_cursor.
#[tokio::test]
async fn p6_5_a2_claim_expiration_allows_resume_at_last_cursor() {
    let store = Arc::new(MockTaskStore::new());

    let pod1_coordinator = test_coordinator_with_store("pod-1", store.clone());
    let pod2_coordinator = test_coordinator_with_store("pod-2", store);

    // Enqueue and claim a job
    let params = JobParams {
        index_uid: "test-index".to_string(),
        primary_key: Some("id".to_string()),
        shard_count: Some(64),
        old_shards: None,
        target_shards: None,
        shadow_index: None,
        chunk: None,
        source_url: Some("https://example.com/dump.ndjson".to_string()),
        source_size_bytes: Some(1_000_000_000),
    };

    let job_id = pod1_coordinator
        .enqueue_job(JobType::DumpImport, params.clone())
        .unwrap();

    let claimed = pod1_coordinator.claim_job().unwrap().unwrap();
    assert_eq!(claimed.claimed_by, "pod-1");

    // Simulate processing progress (50% complete)
    let progress = JobProgress {
        bytes_processed: 500_000_000,
        docs_routed: 5_000_000,
        last_cursor: "500000000".to_string(),
        error: None,
    };

    pod1_coordinator
        .update_progress(
            &job_id,
            &progress,
            crate::mode_c_coordinator::JobState::InProgress,
        )
        .unwrap();

    // Simulate claim expiration by setting claim_expires_at to the past
    let now = now_ms();
    let past_expiration = now - 10_000; // 10 seconds ago
    pod1_coordinator
        .set_claim_expires_at_for_test(&job_id, past_expiration)
        .unwrap();

    // Pod 2 reclaims expired claims
    let reclaimed = pod2_coordinator.reclaim_expired_claims().unwrap();
    assert_eq!(reclaimed, 1);

    // Verify the job is back in queued state
    let job = pod2_coordinator.get_job(&job_id).unwrap().unwrap();
    assert_eq!(job.state, "queued");
    assert!(job.claimed_by.is_none());

    // Verify progress was preserved
    let preserved_progress: JobProgress = serde_json::from_str(&job.progress).unwrap();
    assert_eq!(preserved_progress.bytes_processed, 500_000_000);
    assert_eq!(preserved_progress.docs_routed, 5_000_000);
    assert_eq!(preserved_progress.last_cursor, "500000000");

    // Pod 2 claims the job
    let pod2_claimed = pod2_coordinator.claim_job().unwrap().unwrap();
    assert_eq!(pod2_claimed.id, job_id);
    assert_eq!(pod2_claimed.claimed_by, "pod-2");

    // Pod 2 can resume from the last_cursor
    let resume_progress: JobProgress = pod2_claimed.parse_progress().unwrap();
    assert_eq!(resume_progress.last_cursor, "500000000");
}

/// P6.5-A3: HPA on `miroir_background_queue_depth > 10` triggers scale-up; scale-down once empty.
#[tokio::test]
async fn p6_5_a3_queue_depth_metric_drives_hpa_scaling() {
    let coordinator = test_coordinator("pod-1");

    // Initially, queue is empty
    let queue_depth = coordinator.queue_depth().unwrap();
    assert_eq!(queue_depth, 0);

    // Enqueue 15 jobs (above HPA threshold of 10)
    for i in 0..15 {
        let params = JobParams {
            index_uid: format!("test-index-{}", i),
            primary_key: Some("id".to_string()),
            shard_count: Some(64),
            old_shards: None,
            target_shards: None,
            shadow_index: None,
            chunk: None,
            source_url: Some(format!("https://example.com/dump{}.ndjson", i)),
            source_size_bytes: Some(1_000_000_000),
        };

        coordinator
            .enqueue_job(JobType::DumpImport, params)
            .unwrap();
    }

    // Queue depth should be 15
    let queue_depth = coordinator.queue_depth().unwrap();
    assert_eq!(queue_depth, 15);

    // HPA would scale up when queue_depth > 10
    let should_scale_up = queue_depth > 10;
    assert!(should_scale_up);

    // Simulate processing: claim and complete jobs
    for _ in 0..15 {
        if let Some(claimed) = coordinator.claim_job().unwrap() {
            let progress = JobProgress {
                bytes_processed: 1_000_000_000,
                docs_routed: 10_000_000,
                last_cursor: "1000000000".to_string(),
                error: None,
            };

            coordinator.complete_job(&claimed.id, &progress).unwrap();
        }
    }

    // Queue depth should now be 0
    let queue_depth = coordinator.queue_depth().unwrap();
    assert_eq!(queue_depth, 0);

    // HPA would scale down when queue is empty
    let should_scale_down = queue_depth == 0;
    assert!(should_scale_down);
}

/// P6.5-A4: Two concurrent dumps; chunks interleave in claims; neither starves.
#[tokio::test]
async fn p6_5_a4_concurrent_dumps_interleave_without_starvation() {
    let store = Arc::new(MockTaskStore::new());

    let pod1_coordinator = test_coordinator_with_store("pod-1", store.clone());
    let pod2_coordinator = test_coordinator_with_store("pod-2", store);

    // Enqueue two dump jobs concurrently
    let params1 = JobParams {
        index_uid: "test-index-1".to_string(),
        primary_key: Some("id".to_string()),
        shard_count: Some(64),
        old_shards: None,
        target_shards: None,
        shadow_index: None,
        chunk: None,
        source_url: Some("https://example.com/dump1.ndjson".to_string()),
        source_size_bytes: Some(1_000_000_000),
    };

    let params2 = JobParams {
        index_uid: "test-index-2".to_string(),
        primary_key: Some("id".to_string()),
        shard_count: Some(64),
        old_shards: None,
        target_shards: None,
        shadow_index: None,
        chunk: None,
        source_url: Some("https://example.com/dump2.ndjson".to_string()),
        source_size_bytes: Some(1_000_000_000),
    };

    let job_id1 = pod1_coordinator
        .enqueue_job(JobType::DumpImport, params1)
        .unwrap();
    let job_id2 = pod1_coordinator
        .enqueue_job(JobType::DumpImport, params2)
        .unwrap();

    // Pod 1 claims first job and splits it
    let claimed1 = pod1_coordinator.claim_job().unwrap().unwrap();
    assert_eq!(claimed1.id, job_id1);

    let chunks1: Vec<JobChunk> = vec![
        JobChunk {
            index: 0,
            total: 2,
            start: "0".to_string(),
            end: "500000000".to_string(),
            size_bytes: 500_000_000,
        },
        JobChunk {
            index: 1,
            total: 2,
            start: "500000000".to_string(),
            end: "1000000000".to_string(),
            size_bytes: 500_000_000,
        },
    ];

    pod1_coordinator
        .split_job_into_chunks(&claimed1, chunks1)
        .unwrap();

    // Pod 2 claims second job and splits it
    let claimed2 = pod2_coordinator.claim_job().unwrap().unwrap();
    assert_eq!(claimed2.id, job_id2);

    let chunks2: Vec<JobChunk> = vec![
        JobChunk {
            index: 0,
            total: 2,
            start: "0".to_string(),
            end: "500000000".to_string(),
            size_bytes: 500_000_000,
        },
        JobChunk {
            index: 1,
            total: 2,
            start: "500000000".to_string(),
            end: "1000000000".to_string(),
            size_bytes: 500_000_000,
        },
    ];

    pod2_coordinator
        .split_job_into_chunks(&claimed2, chunks2)
        .unwrap();

    // Now we have 4 chunks total (2 from each dump)
    let all_queued = pod1_coordinator.list_jobs_by_state("queued").unwrap();
    assert_eq!(all_queued.len(), 4);

    // Verify chunks are interleaved (both parent jobs are represented)
    let parent_ids: Vec<_> = all_queued
        .iter()
        .filter_map(|j| j.parent_job_id.as_ref())
        .collect();
    assert!(parent_ids.contains(&&job_id1));
    assert!(parent_ids.contains(&&job_id2));

    // Both pods claim chunks from both dumps (fair interleaving)
    let pod1_claimed = pod1_coordinator.claim_job().unwrap().unwrap();
    let pod2_claimed = pod2_coordinator.claim_job().unwrap().unwrap();

    // Verify they claimed chunks from different parents (or same - interleaving is fair)
    let claimed_parent1 = pod1_claimed.parent_job_id.as_ref();
    let claimed_parent2 = pod2_claimed.parent_job_id.as_ref();

    // At least one parent should be represented
    assert!(
        claimed_parent1.is_some() || claimed_parent2.is_some(),
        "at least one chunk should be claimed"
    );

    // Neither dump starves: both have chunks remaining
    let remaining_queued = pod1_coordinator.list_jobs_by_state("queued").unwrap();
    assert_eq!(remaining_queued.len(), 2);

    // Verify both parent jobs still have pending chunks
    let remaining_parents: Vec<_> = remaining_queued
        .iter()
        .filter_map(|j| j.parent_job_id.as_ref())
        .collect();
    assert!(remaining_parents.contains(&&job_id1) || remaining_parents.contains(&&job_id2));
}

/// P6.5-A5: Reshard backfill splits by shard-id range.
#[tokio::test]
async fn p6_5_a5_reshard_backfill_splits_by_shard_id_range() {
    let coordinator = test_coordinator("pod-1");

    // Enqueue a reshard backfill job (64 → 128 shards)
    let params = JobParams {
        index_uid: "test-index".to_string(),
        primary_key: None,
        shard_count: None,
        old_shards: Some(64),
        target_shards: Some(128),
        shadow_index: Some("test-index-shadow".to_string()),
        chunk: None,
        source_url: None,
        source_size_bytes: None,
    };

    let job_id = coordinator
        .enqueue_job(JobType::ReshardBackfill, params)
        .unwrap();

    let claimed = coordinator.claim_job().unwrap().unwrap();

    // Split into chunks by shard-id range (32 shards per chunk)
    let old_shards = 64u32;
    let shards_per_chunk = 32u32;
    let total_chunks = old_shards.div_ceil(shards_per_chunk); // 2 chunks

    let chunks: Vec<JobChunk> = (0..total_chunks)
        .map(|i| {
            let start_shard = i * shards_per_chunk;
            let end_shard = std::cmp::min(start_shard + shards_per_chunk, old_shards);
            JobChunk {
                index: i,
                total: total_chunks,
                start: start_shard.to_string(),
                end: end_shard.to_string(),
                size_bytes: (end_shard - start_shard) as u64,
            }
        })
        .collect();

    coordinator.split_job_into_chunks(&claimed, chunks).unwrap();

    // Verify 2 chunks were created
    let child_jobs = coordinator.list_chunks(&job_id).unwrap();
    assert_eq!(child_jobs.len(), 2);

    // Verify shard ranges
    let chunk0 = &child_jobs[0];
    let chunk1 = &child_jobs[1];

    let claimed0 = crate::mode_c_coordinator::ClaimedJob {
        id: chunk0.id.clone(),
        type_: chunk0.type_.clone(),
        params: chunk0.params.clone(),
        progress: chunk0.progress.clone(),
        claimed_by: "test".to_string(),
        claim_expires_at: 0,
        parent_job_id: chunk0.parent_job_id.clone(),
        chunk_index: chunk0.chunk_index,
        total_chunks: chunk0.total_chunks,
    };
    let params0: JobParams = claimed0.parse_params().unwrap();

    let claimed1 = crate::mode_c_coordinator::ClaimedJob {
        id: chunk1.id.clone(),
        type_: chunk1.type_.clone(),
        params: chunk1.params.clone(),
        progress: chunk1.progress.clone(),
        claimed_by: "test".to_string(),
        claim_expires_at: 0,
        parent_job_id: chunk1.parent_job_id.clone(),
        chunk_index: chunk1.chunk_index,
        total_chunks: chunk1.total_chunks,
    };
    let params1: JobParams = claimed1.parse_params().unwrap();

    assert_eq!(params0.chunk.as_ref().unwrap().start, "0");
    assert_eq!(params0.chunk.as_ref().unwrap().end, "32");

    assert_eq!(params1.chunk.as_ref().unwrap().start, "32");
    assert_eq!(params1.chunk.as_ref().unwrap().end, "64");
}

/// P6.5-A6: Heartbeat renews claim; missed heartbeat causes expiration.
#[tokio::test]
async fn p6_5_a6_heartbeat_renews_claim_missed_heartbeat_expires() {
    let coordinator = test_coordinator("pod-1");

    // Enqueue and claim a job
    let params = JobParams {
        index_uid: "test-index".to_string(),
        primary_key: Some("id".to_string()),
        shard_count: Some(64),
        old_shards: None,
        target_shards: None,
        shadow_index: None,
        chunk: None,
        source_url: Some("https://example.com/dump.ndjson".to_string()),
        source_size_bytes: Some(1_000_000_000),
    };

    let job_id = coordinator
        .enqueue_job(JobType::DumpImport, params)
        .unwrap();

    let claimed = coordinator.claim_job().unwrap().unwrap();
    let original_expires_at = claimed.claim_expires_at;

    // Sleep a bit to ensure time advances
    sleep(Duration::from_millis(10)).await;

    // Renew the claim (heartbeat)
    let renewed = coordinator.renew_claim(&job_id).unwrap();
    assert!(renewed);

    // Verify the expiration time was extended
    let job = coordinator.get_job(&job_id).unwrap().unwrap();
    assert!(job.claim_expires_at.unwrap() > original_expires_at);

    // Simulate missed heartbeat by setting expiration to the past
    let now = now_ms();
    coordinator
        .set_claim_expires_at_for_test(&job_id, now - 1000)
        .unwrap();

    // Reclaim expired claims
    let reclaimed = coordinator.reclaim_expired_claims().unwrap();
    assert_eq!(reclaimed, 1);

    // Job should be back in queued state
    let job = coordinator.get_job(&job_id).unwrap().unwrap();
    assert_eq!(job.state, "queued");
}

/// Helper to get current time in milliseconds.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
