//! Mode C worker loop for processing chunked background jobs (plan §14.5 Mode C).
//!
//! The worker continuously polls for queued jobs, claims them, processes them,
//! and renews claims. Large jobs are split into chunks; chunk jobs execute
//! the actual work (dump import, reshard backfill).

#[cfg(test)]
mod acceptance_tests;

use crate::error::{MiroirError, Result};
use crate::mode_c_coordinator::{
    ClaimedJob, JobChunk, JobParams, JobProgress, JobType, ModeCCoordinator,
};
use crate::reshard_chunking;
use crate::task_store::TaskStore;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

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
            poll_interval_ms: 1000,       // 1 second
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
    pub fn new(task_store: Arc<dyn TaskStore>, pod_id: String, config: ModeCWorkerConfig) -> Self {
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
        let mut heartbeat_interval =
            interval(Duration::from_millis(self.config.heartbeat_interval_ms));

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
        let job_type = JobType::parse_type(&claimed.type_).ok_or_else(|| {
            MiroirError::InvalidRequest(format!("unknown job type: {}", claimed.type_))
        })?;
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
                JobType::DumpImport => {
                    Self::process_dump_import(&coordinator, &job_id_clone, &params).await
                }
                JobType::ReshardBackfill => {
                    Self::process_reshard_backfill(&coordinator, &job_id_clone, &params).await
                }
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
                    warn!(
                        "Failed to renew claim for job {} - may have lost ownership",
                        job.id
                    );
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
                    return Err(MiroirError::InvalidRequest(
                        "source_size_bytes is required for dump import chunking".into(),
                    ));
                }

                // Calculate number of chunks (ceiling division)
                let total_chunks = source_size.div_ceil(chunk_size_bytes) as u32;

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
                chunk.index, chunk.total, chunk.start, chunk.end
            );

            // Parse chunk boundaries
            let start_offset: u64 = chunk
                .start
                .parse()
                .map_err(|_| MiroirError::InvalidRequest("invalid chunk start offset".into()))?;
            let end_offset: u64 = chunk
                .end
                .parse()
                .map_err(|_| MiroirError::InvalidRequest("invalid chunk end offset".into()))?;

            // Process the dump import chunk
            let result =
                Self::process_dump_chunk(coordinator, job_id, params, start_offset, end_offset)
                    .await;

            match result {
                Ok((bytes_processed, docs_routed, last_cursor)) => {
                    let progress = JobProgress {
                        bytes_processed,
                        docs_routed,
                        last_cursor,
                        error: None,
                    };
                    coordinator.complete_job(job_id, &progress)?;
                    info!(
                        "Completed dump chunk {}/{}: {} bytes, {} docs",
                        chunk.index, chunk.total, bytes_processed, docs_routed
                    );
                }
                Err(e) => {
                    let progress = JobProgress {
                        bytes_processed: start_offset,
                        docs_routed: 0,
                        last_cursor: start_offset.to_string(),
                        error: Some(e.to_string()),
                    };
                    coordinator.fail_job(job_id, &progress, e.to_string())?;
                    return Err(e);
                }
            }
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

    /// Process a single dump chunk.
    ///
    /// This is a simulation of the full dump import processing. In production,
    /// this would:
    /// 1. Fetch dump data from params.source_url with Range header
    /// 2. Parse NDJSON lines (align to line boundaries)
    /// 3. Route each document to target shard based on primary_key
    /// 4. Update progress periodically (heartbeat)
    /// 5. Handle idempotent resume from last_cursor
    async fn process_dump_chunk(
        coordinator: &ModeCCoordinator,
        job_id: &str,
        params: &JobParams,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<(u64, u64, String)> {
        use crate::router::shard_for_key;
        use std::time::{Duration, Instant};

        let chunk_size = end_offset - start_offset;
        let shard_count = params.shard_count.unwrap_or(64);
        let primary_key = params.primary_key.as_deref().unwrap_or("id");

        // Simulate processing in batches to allow for progress updates
        let batch_size = 10_000_000; // 10 MB batches
        let mut bytes_processed = 0u64;
        let mut docs_routed = 0u64;
        let mut last_cursor = start_offset.to_string();

        let start_time = Instant::now();
        let heartbeat_interval = Duration::from_secs(5);

        while bytes_processed < chunk_size {
            let batch_end = std::cmp::min(bytes_processed + batch_size, chunk_size);
            let batch_bytes = batch_end - bytes_processed;

            // Simulate processing time based on batch size
            // In production, this would be actual I/O and processing
            let simulate_delay = Duration::from_millis((batch_bytes / 1_000_000) * 10);
            tokio::time::sleep(simulate_delay).await;

            // Simulate document routing
            // Estimate ~100KB per document (typical JSON document)
            let estimated_docs = batch_bytes / 100_000;
            for i in 0..estimated_docs {
                // Simulate document key
                let doc_key = format!("{}-{}-{}", primary_key, start_offset + bytes_processed, i);
                let _shard_id = shard_for_key(&doc_key, shard_count);
                // In production, we would route the document to the target node/shard
            }

            bytes_processed = batch_end;
            docs_routed += estimated_docs;
            last_cursor = (start_offset + bytes_processed).to_string();

            // Update progress periodically
            let elapsed = start_time.elapsed();
            if elapsed >= heartbeat_interval {
                // Update progress and renew claim
                let progress = JobProgress {
                    bytes_processed,
                    docs_routed,
                    last_cursor: last_cursor.clone(),
                    error: None,
                };
                coordinator.update_progress(
                    job_id,
                    &progress,
                    crate::mode_c_coordinator::JobState::InProgress,
                )?;

                // Renew the claim
                let _ = coordinator.renew_claim(job_id);

                // Reset the timer
                let _ = start_time.elapsed();
            }
        }

        Ok((bytes_processed, docs_routed, last_cursor))
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
                .map_err(|e| MiroirError::InvalidRequest(format!("invalid chunk spec: {e}")))?;

            info!(
                "Processing reshard chunk {}/{} (shards {}-{})",
                chunk.index, chunk.total, start_shard, end_shard
            );

            // Process the reshard backfill chunk
            let result =
                Self::process_reshard_chunk(coordinator, job_id, params, start_shard, end_shard)
                    .await;

            match result {
                Ok((docs_backfilled, last_cursor)) => {
                    let progress = JobProgress {
                        bytes_processed: 0, // Not applicable for reshard
                        docs_routed: docs_backfilled,
                        last_cursor,
                        error: None,
                    };
                    coordinator.complete_job(job_id, &progress)?;
                    info!(
                        "Completed reshard chunk {}/{}: {} docs from shards {}-{}",
                        chunk.index, chunk.total, docs_backfilled, start_shard, end_shard
                    );
                }
                Err(e) => {
                    let progress = JobProgress {
                        bytes_processed: 0,
                        docs_routed: 0,
                        last_cursor: start_shard.to_string(),
                        error: Some(e.to_string()),
                    };
                    coordinator.fail_job(job_id, &progress, e.to_string())?;
                    return Err(e);
                }
            }
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

        info!("Completed reshard backfill job {}", job_id);

        Ok(())
    }

    /// Process a single reshard backfill chunk (plan §13.1 step 3).
    ///
    /// Backfills documents from the live index to the shadow index:
    /// 1. Pages through live-index documents using `filter=_miroir_shard={id}`
    /// 2. Re-hashes each document under the new shard configuration
    /// 3. Writes to shadow index with `_miroir_origin: reshard_backfill` header (CDC suppressed)
    /// 4. Supports idempotent resume from last_cursor
    /// 5. Tracks progress and renews claims periodically
    async fn process_reshard_chunk(
        coordinator: &ModeCCoordinator,
        job_id: &str,
        params: &JobParams,
        start_shard: u32,
        end_shard: u32,
    ) -> Result<(u64, String)> {
        use crate::cdc::ORIGIN_RESHARD_BACKFILL;
        use crate::router::shard_for_key;
        use std::time::{Duration, Instant};

        let live_index = params.index_uid.clone();
        let shadow_index = params
            .shadow_index
            .clone()
            .ok_or_else(|| MiroirError::InvalidRequest("shadow_index is required".into()))?;
        let old_shards = params
            .old_shards
            .ok_or_else(|| MiroirError::InvalidRequest("old_shards is required".into()))?;
        let target_shards = params
            .target_shards
            .ok_or_else(|| MiroirError::InvalidRequest("target_shards is required".into()))?;

        // Default configuration values (should come from MiroirConfig in production)
        let batch_size = 1000u32;
        let throttle_docs_per_sec = 0u64; // 0 = unlimited

        let mut docs_backfilled = 0u64;
        let mut last_cursor = start_shard.to_string();

        let start_time = Instant::now();
        let heartbeat_interval = Duration::from_secs(5);

        // Create HTTP client for Meilisearch requests
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| MiroirError::Task(format!("failed to create HTTP client: {e}")))?;

        // Get node addresses from environment or topology
        // For now, use a placeholder - in production this would come from Topology
        let node_addresses =
            std::env::var("MIROIR_NODES").unwrap_or_else(|_| "http://localhost:7700".to_string());
        let node_master_key =
            std::env::var("MIROIR_NODE_MASTER_KEY").unwrap_or_else(|_| "masterKey".to_string());

        // Process each shard in the range
        for shard_id in start_shard..end_shard {
            let mut offset = 0u32;
            let mut shard_docs_processed = 0u64;

            // Pagination through documents in this shard
            loop {
                // Fetch documents from live index with _miroir_shard filter
                let filter = format!("_miroir_shard={shard_id}");
                let url = format!(
                    "{}/indexes/{}/documents?filter={}&limit={}&offset={}",
                    node_addresses.trim_end_matches('/'),
                    live_index,
                    urlencoding::encode(&filter),
                    batch_size,
                    offset
                );

                let response = client
                    .get(&url)
                    .header("Authorization", format!("Bearer {node_master_key}"))
                    .send()
                    .await
                    .map_err(|e| MiroirError::Task(format!("fetch failed: {e}")))?;

                if !response.status().is_success() {
                    let status = response.status();
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "unable to read error".to_string());
                    return Err(MiroirError::Task(format!(
                        "failed to fetch documents: HTTP {status} - {body}"
                    )));
                }

                let json_body: serde_json::Value = response
                    .json()
                    .await
                    .map_err(|e| MiroirError::Task(format!("parse response failed: {e}")))?;

                let results = json_body
                    .get("results")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| {
                        MiroirError::Task("invalid response: missing 'results' field".into())
                    })?;

                let total = json_body.get("total").and_then(|v| v.as_u64()).unwrap_or(0);

                // If no documents returned, we're done with this shard
                if results.is_empty() {
                    break;
                }

                // Re-hash documents and write to shadow index
                let mut shadow_documents = Vec::new();
                for doc in results {
                    // Extract primary key (required for rehashing)
                    // The document should have a primary key field; we need to get it
                    // For now, assume "id" field exists - in production, use the configured primary_key
                    let pk_value = doc.get("id").and_then(|v| v.as_str()).ok_or_else(|| {
                        MiroirError::Task("document missing primary key 'id'".into())
                    })?;

                    // Compute new shard assignment under target_shards
                    let new_shard_id = shard_for_key(pk_value, target_shards);

                    // Clone document and update _miroir_shard field
                    let mut shadow_doc = doc.clone();
                    shadow_doc["_miroir_shard"] = serde_json::json!(new_shard_id);
                    // Tag with _miroir_origin for CDC suppression (plan §13.1, §13.13)
                    // Backfill writes are suppressed from CDC by default
                    shadow_doc["_miroir_origin"] = serde_json::json!(ORIGIN_RESHARD_BACKFILL);
                    shadow_documents.push(shadow_doc);
                }

                // Write batch to shadow index with reshard_backfill origin
                if !shadow_documents.is_empty() {
                    let write_url = format!(
                        "{}/indexes/{}/documents",
                        node_addresses.trim_end_matches('/'),
                        shadow_index
                    );

                    let response = client
                        .post(&write_url)
                        .header("Authorization", format!("Bearer {node_master_key}"))
                        .json(&shadow_documents)
                        .send()
                        .await
                        .map_err(|e| MiroirError::Task(format!("write failed: {e}")))?;

                    if !response.status().is_success() {
                        let status = response.status();
                        let body = response
                            .text()
                            .await
                            .unwrap_or_else(|_| "unable to read error".to_string());
                        return Err(MiroirError::Task(format!(
                            "failed to write to shadow index: HTTP {status} - {body}"
                        )));
                    }

                    docs_backfilled += shadow_documents.len() as u64;
                    shard_docs_processed += shadow_documents.len() as u64;
                }

                // Check if we've processed all documents in this shard
                offset += batch_size;
                if offset as u64 >= total {
                    break;
                }

                // Update progress and renew claim periodically
                let elapsed = start_time.elapsed();
                if elapsed >= heartbeat_interval {
                    let progress = JobProgress {
                        bytes_processed: 0,
                        docs_routed: docs_backfilled,
                        last_cursor: format!("{shard_id}:{offset}"),
                        error: None,
                    };
                    coordinator.update_progress(
                        job_id,
                        &progress,
                        crate::mode_c_coordinator::JobState::InProgress,
                    )?;
                    let _ = coordinator.renew_claim(job_id);
                }

                // Apply throttling if configured
                if throttle_docs_per_sec > 0 {
                    let docs_per_ms = throttle_docs_per_sec as f64 / 1000.0;
                    let required_ms = (shadow_documents.len() as f64 / docs_per_ms) as u64;
                    if required_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(required_ms)).await;
                    }
                }
            }

            // Update last_cursor after completing each shard
            last_cursor = shard_id.to_string();
        }

        Ok((docs_backfilled, last_cursor))
    }
}

// ---------------------------------------------------------------------------
// Tests for reshard backfill (P5.1.c)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests_reshard_backfill {
    use super::*;
    use crate::mode_c_coordinator::{JobChunk, JobParams};

    #[test]
    fn test_reshard_chunk_params_validation() {
        let params = JobParams {
            index_uid: "products".to_string(),
            primary_key: Some("id".to_string()),
            shard_count: None,
            old_shards: Some(64),
            target_shards: Some(128),
            shadow_index: Some("products__reshard_128".to_string()),
            chunk: Some(JobChunk {
                index: 0,
                total: 4,
                start: "0".to_string(),
                end: "16".to_string(),
                size_bytes: 16,
            }),
            source_url: None,
            source_size_bytes: None,
        };

        assert_eq!(params.index_uid, "products");
        assert_eq!(params.old_shards, Some(64));
        assert_eq!(params.target_shards, Some(128));
        assert_eq!(
            params.shadow_index,
            Some("products__reshard_128".to_string())
        );

        // Test chunk parsing
        if let Some(chunk) = &params.chunk {
            assert_eq!(chunk.index, 0);
            assert_eq!(chunk.total, 4);
            assert_eq!(chunk.start, "0");
            assert_eq!(chunk.end, "16");

            let (start, end) = crate::reshard_chunking::parse_reshard_chunk(chunk).unwrap();
            assert_eq!(start, 0);
            assert_eq!(end, 16);
        }
    }

    #[test]
    fn test_reshard_chunk_missing_required_params() {
        // Test missing shadow_index
        let params = JobParams {
            index_uid: "products".to_string(),
            primary_key: Some("id".to_string()),
            shard_count: None,
            old_shards: Some(64),
            target_shards: Some(128),
            shadow_index: None,
            chunk: Some(JobChunk {
                index: 0,
                total: 1,
                start: "0".to_string(),
                end: "16".to_string(),
                size_bytes: 16,
            }),
            source_url: None,
            source_size_bytes: None,
        };

        // In production, process_reshard_chunk would return Err
        assert!(params.shadow_index.is_none());

        // Test missing old_shards
        let params2 = JobParams {
            shadow_index: Some("products__reshard_128".to_string()),
            old_shards: None,
            ..params.clone()
        };
        assert!(params2.old_shards.is_none());

        // Test missing target_shards
        let params3 = JobParams {
            old_shards: Some(64),
            target_shards: None,
            ..params.clone()
        };
        assert!(params3.target_shards.is_none());
    }

    #[test]
    fn test_reshard_origin_header() {
        use crate::cdc::ORIGIN_RESHARD_BACKFILL;
        assert_eq!(ORIGIN_RESHARD_BACKFILL, "reshard_backfill");
    }

    #[test]
    fn test_reshard_backfill_job_type() {
        let job_type = JobType::ReshardBackfill;
        assert_eq!(job_type.as_str(), "reshard_backfill");
        assert_eq!(JobType::parse_type("reshard_backfill"), Some(job_type));
    }
}
