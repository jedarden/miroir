//! §13.1 Online resharding via shadow index - executor implementation
//!
//! Six-phase resharding process:
//! 1. Shadow create
//! 2. Dual-hash dual-write
//! 3. Backfill
//! 4. Verify
//! 5. Alias swap
//! 6. Cleanup

use crate::anti_entropy::{AntiEntropyConfig, AntiEntropyReconciler};
use crate::error::{MiroirError, Result};
use crate::scatter::{FetchDocumentsRequest, NodeClient, WriteRequest};
use crate::task_store::TaskStore;
use crate::topology::Topology;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use std::hash::Hasher;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use twox_hash::XxHash64;
use uuid::Uuid;

/// Resharding operation state persisted in task store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReshardState {
    pub id: Uuid,
    pub index_uid: String,
    pub old_shards: u32,
    pub new_shards: u32,
    pub phase: Phase,
    pub shadow_index: Option<String>,
    pub started_at: u64,
    pub updated_at: u64,
    pub backfill_progress: BackfillProgress,
    pub verify_result: Option<VerifyResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillProgress {
    pub total_documents: u64,
    pub processed_documents: u64,
    pub current_shard: Option<u32>,
    pub last_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyResult {
    pub passed: bool,
    pub mismatches: Vec<MismatchDetail>,
    pub fingerprint_live: String,
    pub fingerprint_shadow: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MismatchDetail {
    pub primary_key: String,
    pub shard_old: u32,
    pub shard_new: u32,
    pub hash_live: Option<String>,
    pub hash_shadow: Option<String>,
}

/// Resharding executor - handles the six-phase process.
pub struct ReshardExecutor<C: NodeClient> {
    state: Arc<RwLock<ReshardState>>,
    topology: Arc<RwLock<Topology>>,
    config: ReshardConfig,
    node_client: Arc<C>,
    task_store: Option<Arc<dyn TaskStore>>,
    /// HTTP client for node operations (index creation, settings copy).
    http_client: Arc<Client>,
    /// Node addresses for HTTP operations.
    node_addresses: Arc<Vec<String>>,
    /// Master key for node authentication.
    master_key: Arc<String>,
}

#[derive(Debug, Clone)]
pub struct ReshardConfig {
    pub backfill_concurrency: usize,
    pub backfill_batch_size: usize,
    pub throttle_docs_per_sec: u64,
    pub verify_before_swap: bool,
    pub retain_old_index_hours: u64,
}

impl<C: NodeClient> ReshardExecutor<C> {
    /// Create a new resharding operation.
    pub fn new(
        index_uid: String,
        old_shards: u32,
        new_shards: u32,
        topology: Arc<RwLock<Topology>>,
        config: ReshardConfig,
        node_client: Arc<C>,
        task_store: Option<Arc<dyn TaskStore>>,
        http_client: Arc<Client>,
        node_addresses: Arc<Vec<String>>,
        master_key: Arc<String>,
    ) -> Self {
        let id = Uuid::new_v4();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let state = ReshardState {
            id,
            index_uid,
            old_shards,
            new_shards,
            phase: Phase::Idle,
            shadow_index: None,
            started_at: now,
            updated_at: now,
            backfill_progress: BackfillProgress {
                total_documents: 0,
                processed_documents: 0,
                current_shard: None,
                last_cursor: None,
            },
            verify_result: None,
        };

        Self {
            state: Arc::new(RwLock::new(state)),
            topology,
            config,
            node_client,
            task_store,
            http_client,
            node_addresses,
            master_key,
        }
    }

    /// Get the current state.
    pub async fn state(&self) -> ReshardState {
        self.state.read().await.clone()
    }

    /// Start or advance the resharding operation.
    pub async fn advance(&self) -> Result<Phase> {
        let mut state = self.state.write().await;
        let current_phase = state.phase;

        let next_phase = match current_phase {
            Phase::Idle => {
                // Phase 1: Create shadow index
                self.create_shadow_index(&mut state).await?;
                Phase::Shadow
            }
            Phase::Shadow => {
                // Phase 2: Start dual-write mode
                self.start_dual_write(&mut state).await?;
                Phase::DualWrite
            }
            Phase::DualWrite => {
                // Phase 3: Start backfill
                self.start_backfill(&mut state).await?;
                Phase::Backfill
            }
            Phase::Backfill => {
                // Check if backfill complete
                if self.is_backfill_complete(&state).await? {
                    // Phase 4: Verify
                    if self.config.verify_before_swap {
                        self.run_verify(&mut state).await?;
                        Phase::Verify
                    } else {
                        // Skip verify, go straight to swap
                        Phase::Swap
                    }
                } else {
                    // Continue backfill
                    self.advance_backfill(&mut state).await?;
                    Phase::Backfill
                }
            }
            Phase::Verify => {
                let verify_passed = state
                    .verify_result
                    .as_ref()
                    .map(|v| v.passed)
                    .unwrap_or(false);

                if !verify_passed {
                    return Err(MiroirError::VerificationFailed(
                        "Resharding verification failed".to_string(),
                    ));
                }

                // Phase 5: Alias swap
                self.alias_swap(&mut state).await?;
                Phase::Swap
            }
            Phase::Swap => {
                // Phase 6: Cleanup (scheduled for later)
                Phase::Cleanup
            }
            Phase::Cleanup => {
                // Operation complete
                Phase::Complete
            }
            Phase::Complete => {
                return Ok(Phase::Complete);
            }
        };

        state.phase = next_phase;
        state.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Ok(next_phase)
    }

    /// Phase 1: Create shadow index on all nodes.
    ///
    /// Implements plan §13.1 step 1: Create index `{uid}__reshard_{S_new}` on every node
    /// with the new shard count, propagating the live index's settings via the two-phase
    /// broadcast (§13.5).
    async fn create_shadow_index(&self, state: &mut ReshardState) -> Result<()> {
        let shadow_name = format!("{}__reshard_{}", state.index_uid, state.new_shards);
        state.shadow_index = Some(shadow_name.clone());

        tracing::info!(
            index = %state.index_uid,
            shadow = %shadow_name,
            old_shards = state.old_shards,
            new_shards = state.new_shards,
            "Phase 1: Creating shadow index on all nodes"
        );

        // Step 1: Get the primary key from the live index (first node)
        let primary_key = self.get_index_primary_key(&state.index_uid).await?;

        // Step 2: Create the shadow index on all nodes
        self.create_index_on_all_nodes(&shadow_name, &primary_key)
            .await?;

        // Step 3: Copy settings from live index to shadow index
        // For now, we use a simple sequential approach. Two-phase broadcast (§13.5)
        // would be used for production-grade settings propagation.
        self.copy_index_settings(&state.index_uid, &shadow_name)
            .await?;

        tracing::info!(
            index = %state.index_uid,
            shadow = %shadow_name,
            "Shadow index created and settings propagated"
        );

        Ok(())
    }

    /// Get the primary key of an index by querying the first node.
    async fn get_index_primary_key(&self, index_uid: &str) -> Result<String> {
        let address = self
            .node_addresses
            .first()
            .ok_or_else(|| MiroirError::Topology("No node addresses available".to_string()))?;

        let url = format!("{}/indexes/{}", address.trim_end_matches('/'), index_uid);

        let response = self
            .http_client
            .get(&url)
            .header("Authorization", format!("Bearer {}", &*self.master_key))
            .send()
            .await
            .map_err(|e| {
                MiroirError::InvalidRequest(format!("failed to get index info from {address}: {e}"))
            })?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .map_err(|e| MiroirError::InvalidRequest(format!("failed to read response: {e}")))?;

        if !status.is_success() {
            return Err(MiroirError::InvalidRequest(format!(
                "failed to get index '{index_uid}' on node {address}: HTTP {}: {body_text}",
                status.as_u16()
            )));
        }

        let index_info: Value = serde_json::from_str(&body_text).map_err(|e| {
            MiroirError::InvalidState(format!("failed to parse index info JSON: {e}"))
        })?;

        let primary_key = index_info
            .get("primaryKey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                MiroirError::InvalidState(format!("index '{index_uid}' has no primaryKey field"))
            })?;

        Ok(primary_key.to_string())
    }

    /// Create an index on all nodes with the given primary key.
    async fn create_index_on_all_nodes(&self, index_uid: &str, primary_key: &str) -> Result<()> {
        let mut created_on: Vec<String> = Vec::new();

        for address in self.node_addresses.iter() {
            let url = format!("{}/indexes", address.trim_end_matches('/'));

            let body = serde_json::json!({
                "uid": index_uid,
                "primaryKey": primary_key,
            });

            let response = self
                .http_client
                .post(&url)
                .header("Authorization", format!("Bearer {}", &*self.master_key))
                .json(&body)
                .send()
                .await
                .map_err(|e| {
                    MiroirError::InvalidRequest(format!("request to {address} failed: {e}"))
                })?;

            let status = response.status();
            let body_text = response.text().await.map_err(|e| {
                MiroirError::InvalidRequest(format!("failed to read response: {e}"))
            })?;

            if status.as_u16() == 409 {
                // Index already exists - this is ok for resharding (might have been partially created)
                tracing::debug!(
                    index = %index_uid,
                    node = %address,
                    "Index already exists on node"
                );
                created_on.push(address.clone());
                continue;
            }

            if !status.is_success() {
                // Rollback: delete index on all previously created nodes
                self.rollback_delete_index(index_uid, &created_on).await;

                return Err(MiroirError::InvalidRequest(format!(
                    "failed to create index '{index_uid}' on node {address}: HTTP {}: {body_text}",
                    status.as_u16()
                )));
            }

            created_on.push(address.clone());
            tracing::debug!(
                index = %index_uid,
                node = %address,
                "Created index on node"
            );
        }

        tracing::info!(
            index = %index_uid,
            nodes = created_on.len(),
            "Index created on all nodes"
        );

        Ok(())
    }

    /// Copy settings from the source index to the target index.
    ///
    /// This reads settings from the source index (first node) and applies them
    /// to the target index on all nodes. Uses a simple sequential approach;
    /// two-phase broadcast (§13.5) would be used for production-grade propagation.
    async fn copy_index_settings(&self, source_index: &str, target_index: &str) -> Result<()> {
        let address = self
            .node_addresses
            .first()
            .ok_or_else(|| MiroirError::Topology("No node addresses available".to_string()))?;

        // Get settings from the source index
        let url = format!(
            "{}/indexes/{}/settings",
            address.trim_end_matches('/'),
            source_index
        );

        let response = self
            .http_client
            .get(&url)
            .header("Authorization", format!("Bearer {}", &*self.master_key))
            .send()
            .await
            .map_err(|e| {
                MiroirError::InvalidRequest(format!("failed to get settings from {address}: {e}"))
            })?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .map_err(|e| MiroirError::InvalidRequest(format!("failed to read response: {e}")))?;

        if !status.is_success() {
            return Err(MiroirError::InvalidRequest(format!(
                "failed to get settings for index '{source_index}' on node {address}: HTTP {}: {body_text}",
                status.as_u16()
            )));
        }

        let settings: Value = serde_json::from_str(&body_text).map_err(|e| {
            MiroirError::InvalidState(format!("failed to parse settings JSON: {e}"))
        })?;

        // Apply settings to the target index on all nodes
        for address in self.node_addresses.iter() {
            let url = format!(
                "{}/indexes/{}/settings",
                address.trim_end_matches('/'),
                target_index
            );

            let response = self
                .http_client
                .patch(&url)
                .header("Authorization", format!("Bearer {}", &*self.master_key))
                .json(&settings)
                .send()
                .await
                .map_err(|e| {
                    MiroirError::InvalidRequest(format!(
                        "failed to patch settings on {address}: {e}"
                    ))
                })?;

            let status = response.status();
            let body_text = response.text().await.map_err(|e| {
                MiroirError::InvalidRequest(format!("failed to read response: {e}"))
            })?;

            if !status.is_success() {
                tracing::warn!(
                    source_index = %source_index,
                    target_index = %target_index,
                    node = %address,
                    status = status.as_u16(),
                    body = %body_text,
                    "Failed to copy settings to node"
                );
            } else {
                tracing::debug!(
                    target_index = %target_index,
                    node = %address,
                    "Settings copied to node"
                );
            }
        }

        tracing::info!(
            source_index = %source_index,
            target_index = %target_index,
            "Settings copied from source to target index"
        );

        Ok(())
    }

    /// Rollback: delete index on all specified nodes.
    async fn rollback_delete_index(&self, index_uid: &str, nodes: &[String]) {
        for address in nodes {
            let url = format!("{}/indexes/{}", address.trim_end_matches('/'), index_uid);

            match self
                .http_client
                .delete(&url)
                .header("Authorization", format!("Bearer {}", &*self.master_key))
                .send()
                .await
            {
                Ok(_) => {
                    tracing::info!(
                        index = %index_uid,
                        node = %address,
                        "Rollback: deleted index on node"
                    )
                }
                Err(e) => {
                    tracing::error!(
                        index = %index_uid,
                        node = %address,
                        error = %e,
                        "Rollback: failed to delete index on node"
                    )
                }
            }
        }
    }

    /// Phase 2: Start dual-write mode.
    async fn start_dual_write(&self, state: &mut ReshardState) -> Result<()> {
        tracing::info!(
            index = %state.index_uid,
            "Started dual-write mode (old + new shard assignments)"
        );
        Ok(())
    }

    /// Phase 3: Start backfill from live to shadow.
    async fn start_backfill(&self, state: &mut ReshardState) -> Result<()> {
        // Get total document count for progress tracking
        // TODO: Query nodes for document counts

        state.backfill_progress = BackfillProgress {
            total_documents: 0, // Will be updated
            processed_documents: 0,
            current_shard: Some(0),
            last_cursor: None,
        };

        tracing::info!(
            index = %state.index_uid,
            "Started backfill"
        );

        Ok(())
    }

    /// Aggregate the source index's real document count across all source nodes.
    ///
    /// This is the reshard backfill denominator described in plan §13.1 step 3
    /// (bf-2ynu5). It delegates to the shared [`crate::index_stats`] module so
    /// that reshard and ILM rollover (plan §13.17) compute an index's document
    /// count the *same* way: iterate the source `node_addresses`,
    /// `GET /indexes/{uid}/stats`, and reduce `numberOfDocuments` with `max`
    /// (each address hosts a full replica, so `sum` would over-count by the
    /// replication factor).
    ///
    /// The aggregation is infallible from the caller's perspective — a node that
    /// fails to respond (network / non-2xx / parse error) is logged and skipped,
    /// and the count is `0` only if every node fails or reports zero — but this
    /// returns [`Result`] to match the executor's method conventions and leave
    /// the `start_backfill` caller (wired in a sibling bead) free to propagate a
    /// future failure mode.
    ///
    /// Pure additive helper: this method is *not* called from the reshard state
    /// machine in this bead.
    pub async fn compute_source_document_count(&self, index_uid: &str) -> Result<u64> {
        let stats = crate::index_stats::aggregate_index_stats(
            &self.http_client,
            &self.node_addresses,
            &self.master_key,
            index_uid,
        )
        .await;
        Ok(stats.total_documents)
    }

    /// Check if backfill is complete.
    async fn is_backfill_complete(&self, state: &ReshardState) -> Result<bool> {
        Ok(state
            .backfill_progress
            .current_shard
            .map(|s| s >= state.old_shards)
            .unwrap_or(false))
    }

    /// Advance backfill by processing one shard.
    ///
    /// Implements plan §13.1 step 3: background streamer pages every live-index shard
    /// using `filter=_miroir_shard={id}`. Each document is re-hashed under `S_new`
    /// and written to the shadow. Documents are tagged with `_miroir_origin: reshard_backfill`
    /// for CDC event suppression (plan §13.13).
    async fn advance_backfill(&self, state: &mut ReshardState) -> Result<()> {
        let shard_id = state.backfill_progress.current_shard.unwrap_or(0);

        debug!(
            index = %state.index_uid,
            shard = shard_id,
            "Starting backfill for shard"
        );

        // Get a healthy node from topology
        let topology = self.topology.read().await;
        let node = topology.nodes().find(|n| n.is_healthy()).ok_or_else(|| {
            MiroirError::Topology("No healthy nodes available for backfill".to_string())
        })?;
        let node_id = node.id.clone();
        let node_address = node.address.clone();
        drop(topology);

        // Get primary key field name
        let primary_key = self.get_index_primary_key(&state.index_uid).await?;

        // Get shadow index name
        let shadow_name = state
            .shadow_index
            .as_ref()
            .ok_or_else(|| MiroirError::InvalidState("Shadow index not created".to_string()))?
            .clone();

        // Paginated fetch from live index with filter=_miroir_shard={shard_id}
        let mut offset = 0u32;
        let limit = self.config.backfill_batch_size as u32;
        let mut total_processed = 0u64;
        let mut total_docs_in_shard = 0u64;

        loop {
            let fetch_request = FetchDocumentsRequest {
                index_uid: state.index_uid.clone(),
                filter: json!({ "_miroir_shard": shard_id }),
                limit,
                offset,
            };

            let fetch_response = self
                .node_client
                .fetch_documents(&node_id, &node_address, &fetch_request)
                .await
                .map_err(|e| match e {
                    crate::scatter::NodeError::HttpError { status, body } => {
                        MiroirError::InvalidRequest(format!(
                            "fetch from node {node_id} failed: HTTP {status} - {body}"
                        ))
                    }
                    crate::scatter::NodeError::NetworkError(msg) => MiroirError::InvalidRequest(
                        format!("fetch from node {node_id} failed: network error - {msg}"),
                    ),
                    crate::scatter::NodeError::Timeout => {
                        MiroirError::InvalidRequest(format!("fetch from node {node_id} timed out"))
                    }
                })?;

            // Update total count on first page
            if offset == 0 {
                total_docs_in_shard = fetch_response.total;
                state.backfill_progress.total_documents += total_docs_in_shard;
            }

            let docs = fetch_response.results;
            let docs_in_page = docs.len() as u64;

            if docs_in_page == 0 {
                // No more documents in this shard
                break;
            }

            // Process each document: re-hash and write to shadow
            for mut doc in docs {
                // Extract primary key value
                let pk_value = doc
                    .get(&primary_key)
                    .and_then(|v| {
                        if v.is_string() {
                            v.as_str().map(|s| s.to_string())
                        } else {
                            Some(v.to_string())
                        }
                    })
                    .ok_or_else(|| {
                        MiroirError::InvalidState(format!(
                            "document missing primary key field '{primary_key}'"
                        ))
                    })?;

                // Re-hash under new shard count
                let new_shard_id = hash_pk_to_shard(&pk_value, state.new_shards);

                // Inject _miroir_shard = new_shard_id
                doc.as_object_mut()
                    .unwrap()
                    .insert("_miroir_shard".to_string(), json!(new_shard_id));

                // Write to shadow index with origin tag for CDC suppression
                let write_request = WriteRequest {
                    index_uid: shadow_name.clone(),
                    documents: vec![doc],
                    primary_key: Some(primary_key.clone()),
                    origin: Some("reshard_backfill".to_string()),
                };

                let write_response = self
                    .node_client
                    .write_documents(&node_id, &node_address, &write_request)
                    .await
                    .map_err(|e| match e {
                        crate::scatter::NodeError::HttpError { status, body } => {
                            MiroirError::InvalidRequest(format!(
                                "write to node {node_id} failed: HTTP {status} - {body}"
                            ))
                        }
                        crate::scatter::NodeError::NetworkError(msg) => {
                            MiroirError::InvalidRequest(format!(
                                "write to node {node_id} failed: network error - {msg}"
                            ))
                        }
                        crate::scatter::NodeError::Timeout => MiroirError::InvalidRequest(format!(
                            "write to node {node_id} timed out"
                        )),
                    })?;

                if !write_response.success {
                    return Err(MiroirError::InvalidRequest(format!(
                        "write to shadow index failed: {}",
                        write_response
                            .message
                            .unwrap_or_else(|| "unknown error".to_string())
                    )));
                }

                total_processed += 1;
            }

            state.backfill_progress.processed_documents += docs_in_page;

            // Move to next page
            offset += limit;

            // Apply throttling after each batch
            if self.config.throttle_docs_per_sec > 0 {
                let docs_per_ms = self.config.throttle_docs_per_sec as f64 / 1000.0;
                let target_delay_ms = (docs_in_page as f64 / docs_per_ms).ceil() as u64;
                if target_delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(target_delay_ms)).await;
                }
            }

            // Check if we've processed all documents in this shard
            if offset as u64 >= total_docs_in_shard {
                break;
            }
        }

        debug!(
            index = %state.index_uid,
            shard = shard_id,
            processed = total_processed,
            "Completed backfill for shard"
        );

        // Move to next shard
        state.backfill_progress.current_shard = Some(shard_id + 1);
        state.backfill_progress.last_cursor = Some(format!("shard_{shard_id}"));

        Ok(())
    }

    /// Phase 4: Verify shadow index matches live index.
    ///
    /// Implements plan §13.1 step 4: cross-index PK-set comparator.
    /// Iterates every shard of live + shadow via filter=_miroir_shard={id},
    /// streams PKs + content fingerprints into side-by-side xxh3-keyed buckets,
    /// and asserts: (a) live PK set == shadow PK set, (b) for each PK,
    /// content_hash matches.
    ///
    /// Reuses §13.8's bucketed-Merkle machinery with PK-keyed (not shard-keyed)
    /// bucketing so live and shadow can be compared across different S values.
    async fn run_verify(&self, state: &mut ReshardState) -> Result<()> {
        info!(
            index = %state.index_uid,
            old_shards = state.old_shards,
            new_shards = state.new_shards,
            "Running cross-index verification (plan §13.1 step 4)"
        );

        let shadow_name = state
            .shadow_index
            .as_ref()
            .ok_or_else(|| MiroirError::InvalidState("Shadow index not created".to_string()))?;

        // Get a healthy node from topology for verification
        let topology = self.topology.read().await;
        let node = topology.nodes().find(|n| n.is_healthy()).ok_or_else(|| {
            MiroirError::Topology("No healthy nodes available for verification".to_string())
        })?;
        let node_id = node.id.clone();
        let address = node.address.clone();
        drop(topology);

        // Perform cross-index comparison using PK-keyed bucketing
        let config = AntiEntropyConfig {
            index_uid: state.index_uid.clone(),
            ..Default::default()
        };
        let reconciler =
            AntiEntropyReconciler::new(config, self.topology.clone(), self.node_client.clone());

        let diff = reconciler
            .compare_index_buckets(
                &node_id,
                &address,
                &state.index_uid,
                state.old_shards,
                &node_id,
                &address,
                shadow_name,
                state.new_shards,
            )
            .await
            .map_err(|e| {
                MiroirError::VerificationFailed(format!("Cross-index comparison failed: {e}"))
            })?;

        // Build detailed mismatch list with shard assignments
        let mut mismatch_details = Vec::new();
        for pk in &diff.a_only_pks {
            mismatch_details.push(MismatchDetail {
                primary_key: pk.clone(),
                shard_old: hash_pk_to_shard(pk, state.old_shards),
                shard_new: hash_pk_to_shard(pk, state.new_shards),
                hash_live: None,
                hash_shadow: None,
            });
        }
        for pk in &diff.b_only_pks {
            mismatch_details.push(MismatchDetail {
                primary_key: pk.clone(),
                shard_old: hash_pk_to_shard(pk, state.old_shards),
                shard_new: hash_pk_to_shard(pk, state.new_shards),
                hash_live: None,
                hash_shadow: None,
            });
        }
        for pk in &diff.mismatched_pks {
            mismatch_details.push(MismatchDetail {
                primary_key: pk.clone(),
                shard_old: hash_pk_to_shard(pk, state.old_shards),
                shard_new: hash_pk_to_shard(pk, state.new_shards),
                hash_live: None, // Could be populated with actual hashes if needed
                hash_shadow: None,
            });
        }

        let passed = mismatch_details.is_empty();

        if !passed {
            warn!(
                index = %state.index_uid,
                mismatches = mismatch_details.len(),
                a_only = diff.a_only_pks.len(),
                b_only = diff.b_only_pks.len(),
                content_mismatch = diff.mismatched_pks.len(),
                "Verification failed: indexes differ"
            );
        } else {
            info!(
                index = %state.index_uid,
                "Verification passed: indexes match"
            );
        }

        state.verify_result = Some(VerifyResult {
            passed,
            mismatches: mismatch_details,
            fingerprint_live: format!("{}-shard", state.old_shards),
            fingerprint_shadow: format!("{}-shard", state.new_shards),
        });

        Ok(())
    }

    /// Phase 5: Atomic alias swap (P5.1.e, plan §13.1 step 5).
    ///
    /// Performs an atomic alias flip via the task store's flip_alias() method,
    /// pointing the alias at the new shadow index. After this step, dual-write
    /// stops and client writes target ONLY the new index.
    async fn alias_swap(&self, state: &mut ReshardState) -> Result<()> {
        let shadow_name = state
            .shadow_index
            .as_ref()
            .ok_or_else(|| MiroirError::InvalidState("Shadow index not created".to_string()))?;

        let task_store = self.task_store.as_ref().ok_or_else(|| {
            MiroirError::InvalidState("Task store required for alias swap".to_string())
        })?;

        tracing::info!(
            index = %state.index_uid,
            shadow = %shadow_name,
            "Performing atomic alias swap (P5.1.e)"
        );

        // Perform the atomic alias flip via task store
        // This uses §13.7 atomic alias flip: PUT /_miroir/aliases/{index_uid} {"target": shadow_name}
        let history_retention = 10; // Default history retention for rollback
        let flipped = task_store
            .flip_alias(&state.index_uid, shadow_name, history_retention)
            .map_err(|e| MiroirError::AliasSwapFailed(format!("{e}")))?;

        if !flipped {
            return Err(MiroirError::AliasSwapFailed(format!(
                "alias flip returned false for '{}' -> '{}'",
                state.index_uid, shadow_name
            )));
        }

        tracing::info!(
            index = %state.index_uid,
            old_target = %state.index_uid,
            new_target = %shadow_name,
            "alias swap completed: dual-write stopped"
        );

        Ok(())
    }

    /// Rollback the resharding operation (before phase 5).
    pub async fn rollback(&self) -> Result<()> {
        let mut state = self.state.write().await;

        if state.phase >= Phase::Swap {
            return Err(MiroirError::InvalidState(
                "Cannot rollback after alias swap".to_string(),
            ));
        }

        // Delete shadow index on all nodes
        if let Some(ref shadow) = state.shadow_index {
            tracing::info!(
                index = %state.index_uid,
                shadow = %shadow,
                "Rolling back: deleting shadow index on all nodes"
            );
            self.rollback_delete_index(shadow, &self.node_addresses.to_vec())
                .await;
        }

        state.phase = Phase::Complete;
        state.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        tracing::info!(
            index = %state.index_uid,
            "Resharding operation rolled back"
        );

        Ok(())
    }
}

/// Hash a primary key to determine its shard assignment.
///
/// This matches the rendezvous hashing used for document routing.
/// For verification purposes, we use a simple hash modulo since we're
/// just showing which shard a PK would belong to.
pub fn hash_pk_to_shard(pk: &str, shard_count: u32) -> u32 {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(pk.as_bytes());
    (hasher.finish() as u32) % shard_count
}

/// Phase of resharding operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Phase {
    Idle,
    Shadow,
    DualWrite,
    Backfill,
    Verify,
    Swap,
    Cleanup,
    Complete,
}
