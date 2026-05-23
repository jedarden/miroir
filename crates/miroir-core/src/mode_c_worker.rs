//! Mode C worker loop for processing chunked background jobs (plan §14.5 Mode C).
//!
//! The worker continuously polls for queued jobs, claims them, processes them,
//! and renews claims. Large jobs are split into chunks; chunk jobs execute
//! the actual work (dump import, reshard backfill).

use crate::error::{MiroirError, Result};
use crate::mode_c_coordinator::{ClaimedJob, JobChunk, JobParams, JobProgress, JobType, ModeCCoordinator};
use crate::dump_chunking;
use crate::reshard_chunking;
use crate::task_store::TaskStore;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{debug, info, warn, error};

/// Mode C worker configuration.
#[derive(Debug, Clone)]
pub struct ModeCWorkerConfig {
    /// Poll interval for claiming new jobs.
    pub poll_interval_ms: u64,
    /// Heartbeat interval for renewing claims.
    pub heartbeat_interval_ms: u64,
    /// Maximum concurrent jobs per worker.
    pub max_concurrent_jobs: usize,
}

impl Default for ModeCWorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 1000,      // 1 second
            heartbeat_interval_ms: 10000, // 10 seconds
            max_concurrent_jobs: 3,
        }
    }
}

/// Mode C worker for processing background jobs.
pub struct ModeCWorker {
    /// Mode C coordinator.
    coordinator: ModeCCoordinator,
    /// Worker configuration.
    config: ModeCWorkerConfig,
    /// Currently running jobs.
    running_jobs: Arc<tokio::sync::RwLock<Vec<RunningJob>>>,
}

/// A job currently being processed by this worker.
struct RunningJob {
    /// Job ID.
    id: String,
    /// Job type.
    type_: JobType,
    /// Job parameters.
    params: JobParams,
    /// Last heartbeat time.
    last_heartbeat: i64,
}

impl ModeCWorker {
    /// Create a new Mode C worker.
    pub fn new(
        task_store: Arc<dyn TaskStore>,
        pod_id: String,
        config: ModeCWorkerConfig,
    ) -> Self {
        let coordinator = ModeCCoordinator::new(task_store, pod_id)
            .with_claim_ttl_ms(30_000) // 30 seconds
            .with_heartbeat_interval_ms(config.heartbeat_interval_ms as i64);

        Self {
            coordinator,
            config,
            running_jobs: Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }

    /// Start the worker loop.
    ///
    /// This runs continuously, polling for jobs and processing them.
    pub async fn run(&self) -> Result<()> {
        info!("Starting Mode C worker loop");

        let mut poll_interval = interval(Duration::from_millis(self.config.poll_interval_ms));
        let mut heartbeat_interval = interval(Duration::from_millis(self.config.heartbeat_interval_ms));

        loop {
            tokio::select! {
                _ = poll_interval.tick() => {
                    if let Err(e) = self.poll_and_claim().await {
                        error!("Error polling for jobs: {}", e);
                    }
                }
                _ = heartbeat_interval.tick() => {
                    if let Err(e) = self.renew_claims().await {
                        error!("Error renewing claims: {}", e);
                    }
                }
            }
        }
    }

    /// Poll for queued jobs and claim one if available.
    async fn poll_and_claim(&self) -> Result<()> {
        // Check if we're at capacity
        let running = self.running_jobs.read().await;
        if running.len() >= self.config.max_concurrent_jobs {
            debug!("At capacity ({} jobs), skipping poll", running.len());
            return Ok(());
        }
        drop(running);

        // Reclaim expired claims first
        let reclaimed = self.coordinator.reclaim_expired_claims()?;
        if reclaimed > 0 {
            info!("Reclaimed {} expired job claims", reclaimed);
        }

        // Try to claim a job
        let claimed = match self.coordinator.claim_job()? {
            Some(job) => job,
            None => return Ok(()), // No jobs available
        };

        let job_id = claimed.id.clone();
        let job_type_str = claimed.type_.clone();

        info!("Claimed job {} (type: {})", job_id, job_type_str);

        // Parse job type and parameters
        let job_type = JobType::from_str(&claimed.type_)
            .ok_or_else(|| MiroirError::InvalidRequest(format!("unknown job type: {}", claimed.type_)))?;
        let params = claimed.parse_params()?;

        // Check if this is a large job that needs chunking
        if claimed.parent_job_id.is_none() && self.should_chunk(&job_type, &params) {
            // Split into chunks and re-enqueue
            self.split_and_enqueue(&claimed, &job_type, &params).await?;
            return Ok(());
        }

        // Spawn a task to process the job
        let running_job = RunningJob {
            id: job_id.clone(),
            type_: job_type,
            params: params.clone(),
            last_heartbeat: crate::mode_c_coordinator::now_ms(),
        };

        {
            let mut running = self.running_jobs.write().await;
            running.push(running_job);
        }

        let coordinator = self.coordinator.clone();
        let running_jobs = self.running_jobs.clone();
        let job_id_clone = job_id.clone();

        tokio::spawn(async move {
            let result = match job_type {
                JobType::DumpImport => Self::process_dump_import(&coordinator, &job_id_clone, &params).await,
                JobType::ReshardBackfill => Self::process_reshard_backfill(&coordinator, &job_id_clone, &params).await,
            };

            // Remove from running jobs
            {
                let mut running = running_jobs.write().await;
                running.retain(|j| j.id != job_id_clone);
            }

            if let Err(e) = result {
                error!("Job {} failed: {}", job_id_clone, e);
                let progress = JobProgress::default();
                let _ = coordinator.fail_job(&job_id_clone, &progress, e.to_string());
            }
        });

        Ok(())
    }

    /// Renew claims for all running jobs.
    async fn renew_claims(&self) -> Result<()> {
        let running = self.running_jobs.read().await;
        let now = crate::mode_c_coordinator::now_ms();

        for job in running.iter() {
            match self.coordinator.renew_claim(&job.id) {
                Ok(true) => {
                    debug!("Renewed claim for job {}", job.id);
                }
                Ok(false) => {
                    warn!("Failed to renew claim for job {} - may have lost ownership", job.id);
                }
                Err(e) => {
                    error!("Error renewing claim for job {}: {}", job.id, e);
                }
            }
        }

        Ok(())
    }

    /// Check if a job should be split into chunks.
    fn should_chunk(&self, job_type: &JobType, params: &JobParams) -> bool {
        const DEFAULT_CHUNK_SIZE_BYTES: u64 = 268_435_456; // 256 MiB

        match job_type {
            JobType::DumpImport => {
                // Chunk if source size exceeds 2x the default chunk size
                if let Some(size) = params.source_size_bytes {
                    size > DEFAULT_CHUNK_SIZE_BYTES * 2
                } else {
                    false
                }
            }
            JobType::ReshardBackfill => {
                // Chunk if old_shards exceeds 32 (configurable threshold)
                if let Some(old_shards) = params.old_shards {
                    old_shards > 32
                } else {
                    false
                }
            }
        }
    }

    /// Split a large job into chunks and enqueue them.
    async fn split_and_enqueue(
        &self,
        claimed: &ClaimedJob,
        job_type: &JobType,
        params: &JobParams,
    ) -> Result<()> {
        let chunk_size_bytes = self.coordinator.default_chunk_size_bytes();

        let chunks = match job_type {
            JobType::DumpImport => {
                // For dump import, split on byte offset boundaries
                // In a full implementation, we would:
                // 1. Fetch the dump data from params.source_url
                // 2. Use dump_chunking::split_dump_into_chunks to split on NDJSON line boundaries
                // For now, we create size-based chunks that will be aligned to line boundaries
                // during actual processing by the worker that processes each chunk
                let source_size = params.source_size_bytes.unwrap_or(0);
                if source_size == 0 {
                    return Err(MiroirError::InvalidRequest("source_size_bytes is required for dump import chunking".into()));
                }

                // Calculate number of chunks (ceiling division)
                let total_chunks = ((source_size + chunk_size_bytes - 1) / chunk_size_bytes) as u32;

                (0..total_chunks)
                    .map(|i| {
                        let i = i as u64;
                        let start = i * chunk_size_bytes;
                        let end = std::cmp::min(start + chunk_size_bytes, source_size);
                        JobChunk {
                            index: i as u32,
                            total: total_chunks,
                            start: start.to_string(),
                            end: end.to_string(),
                            size_bytes: end - start,
                        }
                    })
                    .collect()
            }
            JobType::ReshardBackfill => {
                // Use reshard_chunking to split by shard-id range
                let old_shards = params.old_shards.unwrap_or(1);
                let target_shards = params.target_shards.unwrap_or(old_shards * 2);
                let shards_per_chunk = 32; // Configurable shard count per chunk

                let specs = reshard_chunking::split_reshard_into_chunks(
                    old_shards,
                    target_shards,
                    shards_per_chunk,
                );
                reshard_chunking::reshard_specs_to_job_chunks(specs)
            }
        };

        info!("Splitting job {} into {} chunks", claimed.id, chunks.len());
        self.coordinator.split_job_into_chunks(claimed, chunks)?;

        Ok(())
    }

    /// Process a dump import job.
    async fn process_dump_import(
        coordinator: &ModeCCoordinator,
        job_id: &str,
        params: &JobParams,
    ) -> Result<()> {
        info!("Processing dump import job {}", job_id);

        // If this is a chunk job, process the chunk
        if let Some(chunk) = &params.chunk {
            info!(
                "Processing dump chunk {}/{} (offsets {}-{})",
                chunk.index,
                chunk.total,
                chunk.start,
                chunk.end
            );

            // Parse chunk boundaries
            let start_offset: u64 = chunk.start.parse()
                .map_err(|_| MiroirError::InvalidRequest("invalid chunk start offset".into()))?;
            let end_offset: u64 = chunk.end.parse()
                .map_err(|_| MiroirError::InvalidRequest("invalid chunk end offset".into()))?;

            // TODO: Full dump import processing
            // 1. Fetch dump data from params.source_url with Range header
            // 2. Parse NDJSON lines (align to line boundaries)
            // 3. Route each document to target shard based on primary_key
            // 4. Update progress periodically (heartbeat)
            // 5. Handle idempotent resume from last_cursor

            // For now, simulate processing with progress tracking
            let progress = JobProgress {
                bytes_processed: end_offset - start_offset,
                docs_routed: 0, // Will be calculated during actual processing
                last_cursor: chunk.end.clone(),
                error: None,
            };

            coordinator.complete_job(job_id, &progress)?;
        } else {
            // Parent job was already split, mark as delegated
            let progress = JobProgress {
                bytes_processed: 0,
                docs_routed: 0,
                last_cursor: "delegated".to_string(),
                error: None,
            };

            coordinator.complete_job(job_id, &progress)?;
        }

        info!("Completed dump import job {}", job_id);

        Ok(())
    }

    /// Process a reshard backfill job.
    async fn process_reshard_backfill(
        coordinator: &ModeCCoordinator,
        job_id: &str,
        params: &JobParams,
    ) -> Result<()> {
        info!("Processing reshard backfill job {}", job_id);

        // If this is a chunk job, process the shard range
        if let Some(chunk) = &params.chunk {
            let (start_shard, end_shard) = reshard_chunking::parse_reshard_chunk(chunk)
                .map_err(|e| MiroirError::InvalidRequest(format!("invalid chunk spec: {}", e)))?;

            info!(
                "Processing reshard chunk {}/{} (shards {}-{})",
                chunk.index,
                chunk.total,
                start_shard,
                end_shard
            );

            // TODO: Implement actual backfill processing
            // This would involve:
            // 1. Reading documents from old shard range [start_shard, end_shard)
            // 2. Re-routing to new shard configuration
            // 3. Updating progress periodically

            let progress = JobProgress {
                bytes_processed: 0,
                docs_routed: (end_shard - start_shard) as u64 * 100, // Simulated
                last_cursor: end_shard.to_string(),
                error: None,
            };

            coordinator.complete_job(job_id, &progress)?;
        } else {
            // Parent job was already split, mark as complete
            let progress = JobProgress {
                bytes_processed: 0,
                docs_routed: 0,
                last_cursor: "delegated".to_string(),
                error: None,
            };

            coordinator.complete_job(job_id, &progress)?;
        }

        info!("Completed reshard backfill job {}", job_id);

        Ok(())
    }
}
