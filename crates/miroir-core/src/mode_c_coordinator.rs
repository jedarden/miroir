//! Mode C work-queued chunked jobs coordinator (plan §14.5 Mode C).
//!
//! Any pod can claim a queued job via compare-and-swap. Jobs have claim TTL
//! with heartbeats; expired claims are released for reclamation.
//!
//! Large jobs are split into chunks on input boundaries by the first pod
//! that picks them up. Each chunk is an independent job with a parent reference.
//!
//! Applied to:
//! - §13.9 streaming dump import — chunks on NDJSON line boundaries
//! - §13.1 reshard backfill — partitions by shard-id range
//!
//! ## HPA Queue Depth Metric (plan §14.4)
//!
//! The coordinator provides a queue depth metric for Horizontal Pod Autoscaler:
//!
//! ```text
//! miroir:jobs:_queued (Redis set)
//! SCARD miroir:jobs:_queued = miroir_background_queue_depth
//! ```
//!
//! The HPA can be configured to scale on this external metric:
//!
//! ```yaml
//! metrics:
//! - type: External
//!   external:
//!     metric:
//!       name: miroir_background_queue_depth
//!     target:
//!       type: AverageValue
//!       averageValue: 10
//! ```
//!
//! Example HPA configuration that scales up when queue depth > 10:
//! ```yaml
//! apiVersion: autoscaling/v2
//! kind: HorizontalPodAutoscaler
//! metadata:
//!   name: miroir-worker-hpa
//! spec:
//!   scaleTargetRef:
//!     apiVersion: apps/v1
//!     kind: Deployment
//!     name: miroir-worker
//!   minReplicas: 2
//!   maxReplicas: 10
//!   metrics:
//!   - type: External
//!     external:
//!       metric:
//!         name: miroir_background_queue_depth
//!       target:
//!         type: AverageValue
//!         averageValue: 10
//! ```

use crate::error::{MiroirError, Result};
use crate::task_store::{JobRow, NewJob, TaskStore};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn, error};

/// Job states (plan §14.5 Mode C).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum JobState {
    /// Job is queued and waiting to be claimed.
    Queued = 0,
    /// Job is claimed and in progress.
    InProgress = 1,
    /// Job completed successfully.
    Completed = 2,
    /// Job failed.
    Failed = 3,
}

impl JobState {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(Self::Queued),
            "in_progress" => Some(Self::InProgress),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    /// Convert to string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

/// Job types supported by Mode C coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobType {
    /// Streaming dump import (plan §13.9).
    DumpImport = 0,
    /// Reshard backfill (plan §13.1).
    ReshardBackfill = 1,
}

impl JobType {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "dump_import" => Some(Self::DumpImport),
            "reshard_backfill" => Some(Self::ReshardBackfill),
            _ => None,
        }
    }

    /// Convert to string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DumpImport => "dump_import",
            Self::ReshardBackfill => "reshard_backfill",
        }
    }
}

/// Job progress tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobProgress {
    /// Bytes processed so far (for dump import).
    pub bytes_processed: u64,
    /// Documents routed so far (for dump import).
    pub docs_routed: u64,
    /// Last cursor position for idempotent resume.
    pub last_cursor: String,
    /// Any error message.
    pub error: Option<String>,
}

impl Default for JobProgress {
    fn default() -> Self {
        Self {
            bytes_processed: 0,
            docs_routed: 0,
            last_cursor: String::new(),
            error: None,
        }
    }
}

/// Chunk specification for a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobChunk {
    /// Chunk index (0-based).
    pub index: u32,
    /// Total number of chunks.
    pub total: u32,
    /// Starting position (cursor or byte offset).
    pub start: String,
    /// Ending position (cursor or byte offset).
    pub end: String,
    /// Estimated size in bytes.
    pub size_bytes: u64,
}

/// Job parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobParams {
    /// Index UID being operated on.
    pub index_uid: String,
    /// Primary key field (for dump import).
    pub primary_key: Option<String>,
    /// Shard count (for dump import routing).
    pub shard_count: Option<u32>,
    /// Old shard count (for reshard backfill).
    pub old_shards: Option<u32>,
    /// Target shard count (for reshard backfill).
    pub target_shards: Option<u32>,
    /// Shadow index UID (for reshard backfill).
    pub shadow_index: Option<String>,
    /// Chunk specification (if this is a chunk).
    pub chunk: Option<JobChunk>,
    /// Source data location (for dump import).
    pub source_url: Option<String>,
    /// Source data size (for dump import).
    pub source_size_bytes: Option<u64>,
}

/// Mode C job coordinator.
#[derive(Clone)]
pub struct ModeCCoordinator {
    /// Task store for job persistence.
    task_store: Arc<dyn TaskStore>,
    /// Pod ID for claiming jobs.
    pod_id: String,
    /// Claim TTL in milliseconds (default 30s).
    claim_ttl_ms: i64,
    /// Heartbeat interval in milliseconds (default 10s).
    heartbeat_interval_ms: i64,
    /// Default chunk size in bytes (default 256 MiB).
    default_chunk_size_bytes: u64,
}

impl ModeCCoordinator {
    /// Create a new Mode C coordinator.
    pub fn new(
        task_store: Arc<dyn TaskStore>,
        pod_id: String,
    ) -> Self {
        Self {
            task_store,
            pod_id,
            claim_ttl_ms: 30_000,  // 30 seconds
            heartbeat_interval_ms: 10_000,  // 10 seconds
            default_chunk_size_bytes: 268_435_456,  // 256 MiB
        }
    }

    /// Set the claim TTL.
    pub fn with_claim_ttl_ms(mut self, ttl_ms: i64) -> Self {
        self.claim_ttl_ms = ttl_ms;
        self
    }

    /// Set the heartbeat interval.
    pub fn with_heartbeat_interval_ms(mut self, interval_ms: i64) -> Self {
        self.heartbeat_interval_ms = interval_ms;
        self
    }

    /// Set the default chunk size.
    pub fn with_chunk_size_bytes(mut self, size_bytes: u64) -> Self {
        self.default_chunk_size_bytes = size_bytes;
        self
    }

    /// Get the default chunk size in bytes.
    pub fn default_chunk_size_bytes(&self) -> u64 {
        self.default_chunk_size_bytes
    }

    /// Enqueue a new job.
    pub fn enqueue_job(
        &self,
        type_: JobType,
        params: JobParams,
    ) -> Result<String> {
        let job_id = format!("{}-{}", type_.as_str(), uuid::Uuid::new_v4());
        let params_json = serde_json::to_string(&params)
            .map_err(|e| MiroirError::TaskStore(format!("failed to serialize params: {}", e)))?;
        let progress = JobProgress::default();
        let progress_json = serde_json::to_string(&progress)
            .map_err(|e| MiroirError::TaskStore(format!("failed to serialize progress: {}", e)))?;

        let new_job = NewJob {
            id: job_id.clone(),
            type_: type_.as_str().to_string(),
            params: params_json,
            state: JobState::Queued.as_str().to_string(),
            progress: progress_json,
            parent_job_id: None,
            chunk_index: None,
            total_chunks: None,
            created_at: now_ms(),
        };

        self.task_store.insert_job(&new_job)?;

        debug!(
            job_id = %job_id,
            job_type = %type_.as_str(),
            "enqueued new Mode C job"
        );

        Ok(job_id)
    }

    /// Try to claim a queued job.
    ///
    /// Returns the claimed job if successful, or None if no jobs are available.
    pub fn claim_job(&self) -> Result<Option<ClaimedJob>> {
        // List queued jobs
        let queued_jobs = self.task_store.list_jobs_by_state(JobState::Queued.as_str())?;

        if queued_jobs.is_empty() {
            return Ok(None);
        }

        // Try to claim the first available job
        let now = now_ms();
        let claim_expires_at = now + self.claim_ttl_ms;

        for job in queued_jobs {
            if self.task_store.claim_job(&job.id, &self.pod_id, claim_expires_at)? {
                // Successfully claimed
                debug!(
                    job_id = %job.id,
                    pod_id = %self.pod_id,
                    "claimed Mode C job"
                );

                return Ok(Some(ClaimedJob {
                    id: job.id.clone(),
                    type_: job.type_.clone(),
                    params: job.params.clone(),
                    progress: job.progress.clone(),
                    claimed_by: self.pod_id.clone(),
                    claim_expires_at,
                    parent_job_id: job.parent_job_id.clone(),
                    chunk_index: job.chunk_index,
                    total_chunks: job.total_chunks,
                }));
            }
        }

        // All queued jobs were claimed by another pod
        Ok(None)
    }

    /// Renew a job claim (heartbeat).
    ///
    /// Returns true if the claim was renewed, false if we lost the claim.
    pub fn renew_claim(&self, job_id: &str) -> Result<bool> {
        let now = now_ms();
        let claim_expires_at = now + self.claim_ttl_ms;

        let renewed = self.task_store.renew_job_claim(job_id, claim_expires_at)?;

        if !renewed {
            warn!(
                job_id = %job_id,
                pod_id = %self.pod_id,
                "failed to renew job claim - may have lost ownership"
            );
        }

        Ok(renewed)
    }

    /// Update job progress.
    pub fn update_progress(
        &self,
        job_id: &str,
        progress: &JobProgress,
        state: JobState,
    ) -> Result<()> {
        let progress_json = serde_json::to_string(progress)
            .map_err(|e| MiroirError::TaskStore(format!("failed to serialize progress: {}", e)))?;

        self.task_store.update_job_progress(job_id, state.as_str(), &progress_json)?;

        debug!(
            job_id = %job_id,
            state = %state.as_str(),
            bytes_processed = progress.bytes_processed,
            "updated job progress"
        );

        Ok(())
    }

    /// Complete a job successfully.
    pub fn complete_job(&self, job_id: &str, progress: &JobProgress) -> Result<()> {
        self.update_progress(job_id, progress, JobState::Completed)?;

        info!(
            job_id = %job_id,
            "completed Mode C job"
        );

        Ok(())
    }

    /// Fail a job.
    pub fn fail_job(&self, job_id: &str, progress: &JobProgress, error: String) -> Result<()> {
        let mut failed_progress = progress.clone();
        failed_progress.error = Some(error.clone());

        let progress_json = serde_json::to_string(&failed_progress)
            .map_err(|e| MiroirError::TaskStore(format!("failed to serialize progress: {}", e)))?;

        self.task_store.update_job_progress(job_id, JobState::Failed.as_str(), &progress_json)?;

        error!(
            job_id = %job_id,
            error = %error,
            "failed Mode C job"
        );

        Ok(())
    }

    /// Split a large job into chunks and enqueue them.
    ///
    /// Called by the first pod that picks up a large job. The original job
    /// transitions to "delegated" state and child chunk jobs are created.
    pub fn split_job_into_chunks(
        &self,
        job: &ClaimedJob,
        chunk_specs: Vec<JobChunk>,
    ) -> Result<Vec<String>> {
        let params: JobParams = serde_json::from_str(&job.params)
            .map_err(|e| MiroirError::TaskStore(format!("failed to deserialize params: {}", e)))?;

        let total_chunks = chunk_specs.len() as u32;
        let mut chunk_job_ids = Vec::new();

        // Mark the parent job as delegated (in_progress with special progress)
        let delegated_progress = JobProgress {
            bytes_processed: 0,
            docs_routed: 0,
            last_cursor: "delegated".to_string(),
            error: None,
        };
        self.update_progress(&job.id, &delegated_progress, JobState::InProgress)?;

        // Create chunk jobs
        for (idx, chunk) in chunk_specs.iter().enumerate() {
            let mut chunk_params = params.clone();
            chunk_params.chunk = Some(chunk.clone());

            let chunk_job_id = format!("{}-chunk-{}", job.id, idx);
            let params_json = serde_json::to_string(&chunk_params)
                .map_err(|e| MiroirError::TaskStore(format!("failed to serialize chunk params: {}", e)))?;
            let progress = JobProgress::default();
            let progress_json = serde_json::to_string(&progress)
                .map_err(|e| MiroirError::TaskStore(format!("failed to serialize progress: {}", e)))?;

            let new_job = NewJob {
                id: chunk_job_id.clone(),
                type_: job.type_.clone(),
                params: params_json,
                state: JobState::Queued.as_str().to_string(),
                progress: progress_json,
                parent_job_id: Some(job.id.clone()),
                chunk_index: Some(idx as i64),
                total_chunks: Some(total_chunks as i64),
                created_at: now_ms(),
            };

            self.task_store.insert_job(&new_job)?;
            chunk_job_ids.push(chunk_job_id);
        }

        info!(
            parent_job_id = %job.id,
            chunk_count = total_chunks,
            "split job into chunks"
        );

        Ok(chunk_job_ids)
    }

    /// Reclaim expired claims.
    ///
    /// Returns the number of claims reclaimed.
    /// Preserves job progress for idempotent resume.
    pub fn reclaim_expired_claims(&self) -> Result<usize> {
        let now = now_ms();
        let expired_jobs = self.task_store.list_expired_claims(now)?;

        let mut reclaimed = 0;
        for job in expired_jobs {
            // Preserve the existing progress for idempotent resume
            // The job.progress field contains the last_cursor and other state
            // needed by the next pod to resume from where the previous pod left off
            let progress_json = job.progress.clone();

            // Clear claim and reset to queued
            self.task_store.reclaim_job_claim(&job.id, JobState::Queued.as_str(), &progress_json)?;

            debug!(
                job_id = %job.id,
                previous_claimant = ?job.claimed_by,
                "reclaimed expired job claim"
            );

            reclaimed += 1;
        }

        if reclaimed > 0 {
            info!(
                count = reclaimed,
                "reclaimed expired job claims"
            );
        }

        Ok(reclaimed)
    }

    /// Get the queue depth (number of queued jobs).
    ///
    /// Used for HPA scaling per plan §14.4.
    pub fn queue_depth(&self) -> Result<u64> {
        self.task_store.count_jobs_by_state(JobState::Queued.as_str())
    }

    /// Get job by ID.
    pub fn get_job(&self, job_id: &str) -> Result<Option<JobRow>> {
        self.task_store.get_job(job_id)
    }

    /// List all chunks for a parent job.
    pub fn list_chunks(&self, parent_job_id: &str) -> Result<Vec<JobRow>> {
        self.task_store.list_jobs_by_parent(parent_job_id)
    }

    /// List jobs by state.
    pub fn list_jobs_by_state(&self, state: &str) -> Result<Vec<JobRow>> {
        self.task_store.list_jobs_by_state(state)
    }

    /// Set the claim expiration time for a job (test helper).
    ///
    /// This allows tests to simulate time passing without actually waiting.
    /// WARNING: This should only be used in tests!
    #[cfg(test)]
    pub fn set_claim_expires_at_for_test(&self, job_id: &str, expires_at: i64) -> Result<()> {
        self.task_store.renew_job_claim(job_id, expires_at)?;
        Ok(())
    }
}

/// A claimed job being processed by a pod.
#[derive(Debug, Clone)]
pub struct ClaimedJob {
    /// Job ID.
    pub id: String,
    /// Job type.
    pub type_: String,
    /// Job parameters (JSON).
    pub params: String,
    /// Job progress (JSON).
    pub progress: String,
    /// Pod that claimed this job.
    pub claimed_by: String,
    /// When the claim expires (UNIX ms).
    pub claim_expires_at: i64,
    /// Parent job ID if this is a chunk.
    pub parent_job_id: Option<String>,
    /// Chunk index if this is a chunk.
    pub chunk_index: Option<i64>,
    /// Total chunks if this is part of a chunked job.
    pub total_chunks: Option<i64>,
}

impl ClaimedJob {
    /// Parse the job parameters.
    pub fn parse_params(&self) -> Result<JobParams> {
        serde_json::from_str(&self.params)
            .map_err(|e| MiroirError::TaskStore(format!("failed to deserialize params: {}", e)))
    }

    /// Parse the current progress.
    pub fn parse_progress(&self) -> Result<JobProgress> {
        serde_json::from_str(&self.progress)
            .map_err(|e| MiroirError::TaskStore(format!("failed to deserialize progress: {}", e)))
    }

    /// Check if this is a chunk job.
    pub fn is_chunk(&self) -> bool {
        self.parent_job_id.is_some()
    }

    /// Check if the claim is about to expire (within 5 seconds).
    pub fn claim_expiring_soon(&self) -> bool {
        let now = now_ms();
        self.claim_expires_at - now < 5_000
    }
}

/// Get current UNIX timestamp in milliseconds.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_store::SqliteTaskStore;

    fn test_coordinator() -> ModeCCoordinator {
        let store = Arc::new(SqliteTaskStore::open_in_memory().unwrap());
        store.migrate().unwrap();
        ModeCCoordinator::new(store, "test-pod".to_string())
    }

    #[test]
    fn test_job_state_roundtrip() {
        assert_eq!(JobState::from_str("queued"), Some(JobState::Queued));
        assert_eq!(JobState::from_str("in_progress"), Some(JobState::InProgress));
        assert_eq!(JobState::from_str("completed"), Some(JobState::Completed));
        assert_eq!(JobState::from_str("failed"), Some(JobState::Failed));
        assert_eq!(JobState::from_str("unknown"), None);

        assert_eq!(JobState::Queued.as_str(), "queued");
        assert_eq!(JobState::InProgress.as_str(), "in_progress");
        assert_eq!(JobState::Completed.as_str(), "completed");
        assert_eq!(JobState::Failed.as_str(), "failed");
    }

    #[test]
    fn test_job_type_roundtrip() {
        assert_eq!(JobType::from_str("dump_import"), Some(JobType::DumpImport));
        assert_eq!(JobType::from_str("reshard_backfill"), Some(JobType::ReshardBackfill));
        assert_eq!(JobType::from_str("unknown"), None);

        assert_eq!(JobType::DumpImport.as_str(), "dump_import");
        assert_eq!(JobType::ReshardBackfill.as_str(), "reshard_backfill");
    }

    #[test]
    fn test_enqueue_and_claim_job() {
        let coord = test_coordinator();

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

        let job_id = coord.enqueue_job(JobType::DumpImport, params).unwrap();

        // Claim the job
        let claimed = coord.claim_job().unwrap();
        assert!(claimed.is_some());
        let claimed = claimed.unwrap();
        assert_eq!(claimed.id, job_id);
        assert_eq!(claimed.claimed_by, "test-pod");

        // Parse params
        let parsed_params = claimed.parse_params().unwrap();
        assert_eq!(parsed_params.index_uid, "test-index");
        assert_eq!(parsed_params.primary_key, Some("id".to_string()));
    }

    #[test]
    fn test_claim_renewal() {
        let coord = test_coordinator();

        let params = JobParams {
            index_uid: "test-index".to_string(),
            primary_key: None,
            shard_count: None,
            old_shards: None,
            target_shards: None,
            shadow_index: None,
            chunk: None,
            source_url: None,
            source_size_bytes: None,
        };

        let job_id = coord.enqueue_job(JobType::ReshardBackfill, params).unwrap();
        let claimed = coord.claim_job().unwrap().unwrap();

        // Add a small delay to ensure time advances
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Renew the claim
        let renewed = coord.renew_claim(&job_id).unwrap();
        assert!(renewed);

        // Get the job and verify claim was extended
        let job = coord.get_job(&job_id).unwrap().unwrap();
        assert!(job.claim_expires_at.unwrap() > claimed.claim_expires_at);
    }

    #[test]
    fn test_split_job_into_chunks() {
        let coord = test_coordinator();

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

        let job_id = coord.enqueue_job(JobType::DumpImport, params).unwrap();
        let claimed = coord.claim_job().unwrap().unwrap();

        // Create 3 chunks
        let chunks = vec![
            JobChunk {
                index: 0,
                total: 3,
                start: "0".to_string(),
                end: "333333333".to_string(),
                size_bytes: 333_333_333,
            },
            JobChunk {
                index: 1,
                total: 3,
                start: "333333333".to_string(),
                end: "666666666".to_string(),
                size_bytes: 333_333_333,
            },
            JobChunk {
                index: 2,
                total: 3,
                start: "666666666".to_string(),
                end: "1000000000".to_string(),
                size_bytes: 333_333_334,
            },
        ];

        let chunk_ids = coord.split_job_into_chunks(&claimed, chunks).unwrap();
        assert_eq!(chunk_ids.len(), 3);

        // Verify chunks are queued
        let child_jobs = coord.list_chunks(&job_id).unwrap();
        assert_eq!(child_jobs.len(), 3);

        for (idx, child) in child_jobs.iter().enumerate() {
            assert_eq!(child.state, "queued");
            assert_eq!(child.parent_job_id, Some(job_id.clone()));
            assert_eq!(child.chunk_index, Some(idx as i64));
            assert_eq!(child.total_chunks, Some(3));
        }
    }

    #[test]
    fn test_complete_and_fail_job() {
        let coord = test_coordinator();

        let params = JobParams {
            index_uid: "test-index".to_string(),
            primary_key: None,
            shard_count: None,
            old_shards: None,
            target_shards: None,
            shadow_index: None,
            chunk: None,
            source_url: None,
            source_size_bytes: None,
        };

        let job_id = coord.enqueue_job(JobType::DumpImport, params.clone()).unwrap();
        let claimed = coord.claim_job().unwrap().unwrap();

        // Complete the job
        let progress = JobProgress {
            bytes_processed: 1_000_000,
            docs_routed: 10_000,
            last_cursor: "1000000".to_string(),
            error: None,
        };
        coord.complete_job(&job_id, &progress).unwrap();

        let job = coord.get_job(&job_id).unwrap().unwrap();
        assert_eq!(job.state, "completed");

        // Test failure
        let job_id2 = coord.enqueue_job(JobType::DumpImport, params).unwrap();
        let claimed2 = coord.claim_job().unwrap().unwrap();

        let fail_progress = JobProgress::default();
        coord.fail_job(&job_id2, &fail_progress, "test error".to_string()).unwrap();

        let job2 = coord.get_job(&job_id2).unwrap().unwrap();
        assert_eq!(job2.state, "failed");
    }

    #[test]
    fn test_queue_depth() {
        let coord = test_coordinator();

        let params = JobParams {
            index_uid: "test-index".to_string(),
            primary_key: None,
            shard_count: None,
            old_shards: None,
            target_shards: None,
            shadow_index: None,
            chunk: None,
            source_url: None,
            source_size_bytes: None,
        };

        assert_eq!(coord.queue_depth().unwrap(), 0);

        coord.enqueue_job(JobType::DumpImport, params.clone()).unwrap();
        coord.enqueue_job(JobType::DumpImport, params.clone()).unwrap();
        coord.enqueue_job(JobType::DumpImport, params).unwrap();

        assert_eq!(coord.queue_depth().unwrap(), 3);

        // Claim one job
        coord.claim_job().unwrap();
        assert_eq!(coord.queue_depth().unwrap(), 2);
    }

    #[test]
    fn test_claimed_job_is_chunk() {
        let coord = test_coordinator();

        let params = JobParams {
            index_uid: "test-index".to_string(),
            primary_key: None,
            shard_count: None,
            old_shards: None,
            target_shards: None,
            shadow_index: None,
            chunk: None,
            source_url: None,
            source_size_bytes: None,
        };

        // Parent job
        let parent_id = coord.enqueue_job(JobType::DumpImport, params).unwrap();
        let claimed = coord.claim_job().unwrap().unwrap();
        assert!(!claimed.is_chunk());

        // Create chunks
        let chunks = vec![
            JobChunk {
                index: 0,
                total: 1,
                start: "0".to_string(),
                end: "1000".to_string(),
                size_bytes: 1000,
            },
        ];
        coord.split_job_into_chunks(&claimed, chunks).unwrap();

        // Get the chunk job
        let child_jobs = coord.list_chunks(&parent_id).unwrap();
        assert_eq!(child_jobs.len(), 1);

        // Claim the chunk
        let chunk_job = coord.get_job(&child_jobs[0].id).unwrap().unwrap();
        // We need to parse it as a ClaimedJob
        let claimed_chunk = ClaimedJob {
            id: chunk_job.id.clone(),
            type_: chunk_job.type_.clone(),
            params: chunk_job.params.clone(),
            progress: chunk_job.progress.clone(),
            claimed_by: "test-pod".to_string(),
            claim_expires_at: chunk_job.claim_expires_at.unwrap_or(0),
            parent_job_id: chunk_job.parent_job_id.clone(),
            chunk_index: chunk_job.chunk_index,
            total_chunks: chunk_job.total_chunks,
        };

        assert!(claimed_chunk.is_chunk());
    }
}
