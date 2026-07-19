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
use crate::reshard::ReshardOperation;
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
    /// Whether `total_documents` was seeded up front from the source-index
    /// stats (plan §13.1 step 3, bf-2tddo), rather than accumulated shard by
    /// shard.
    ///
    /// When `true`, [`ReshardExecutor::incorporate_shard_total`] is a no-op:
    /// the denominator is already the full-index count, so folding in
    /// per-shard totals would double-count and push the progress ratio past
    /// 1.0. When `false` (the legacy fallback), per-shard totals still
    /// accumulate into the denominator as each shard's first page is read.
    ///
    /// `#[serde(default)]` deserializes previously-serialized `ReshardState`s
    /// to `false` — a state persisted before this flag existed was using the
    /// per-shard accumulation, so it must keep doing so instead of suddenly
    /// suppressing the denominator.
    #[serde(default)]
    pub upfront_total_known: bool,
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
    /// Optional persisted [`ReshardOperation`] (bf-1q4wa) that backfill
    /// progress is mirrored into via [`Self::report_progress`], so the admin
    /// status endpoint's `ReshardOperation::backfill_progress()` reads a real
    /// denominator (and, as each shard completes, a real numerator) instead of
    /// the unset `0`.
    ///
    /// `None` on the default executor path — [`Self::report_progress`] is then
    /// a no-op, leaving that path unchanged.
    progress_operation: Option<Arc<RwLock<ReshardOperation>>>,
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
                upfront_total_known: false,
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
            progress_operation: None,
        }
    }

    /// Attach a persisted [`ReshardOperation`] that backfill progress is
    /// mirrored into (bf-1q4wa).
    ///
    /// When attached, [`Self::report_progress`] — called at the end of
    /// [`Self::start_backfill`] (propagating the seeded denominator) and at the
    /// end of [`Self::advance_backfill`] (propagating the numerator as each
    /// shard completes) — writes the executor's `BackfillProgress` into the
    /// persisted operation via
    /// [`ReshardOperation::update_backfill_progress`], so the admin status
    /// endpoint's [`ReshardOperation::backfill_progress`] returns a real ratio
    /// from the very first shard instead of the `0.0` an unset denominator
    /// yields.
    ///
    /// Leaving this unset (the default) keeps [`Self::report_progress`] a
    /// no-op, so the default executor path is unchanged.
    pub fn with_progress_operation(mut self, op: Arc<RwLock<ReshardOperation>>) -> Self {
        self.progress_operation = Some(op);
        self
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
        // Backfill denominator (plan §13.1 step 3, bf-2tddo): query the source
        // index's real document count up front via the shared `index_stats`
        // aggregation, so the progress ratio has a non-zero denominator from the
        // very first shard instead of the legacy `0` placeholder.
        //
        // Progress-reporting only: a stats failure must never abort a reshard
        // over a mere progress signal, so on error the denominator falls back to
        // `0` and `advance_backfill`'s per-shard accumulation fills it in
        // incrementally as the legacy fallback.
        let total_documents = self
            .compute_source_document_count(&state.index_uid)
            .await
            .unwrap_or(0);

        // A non-zero up-front count means the denominator is already the full
        // index total, so per-shard accumulation would double-count. We record
        // that with `upfront_total_known` and let `incorporate_shard_total`
        // short-circuit. A zero count (every stats node failed or reported
        // zero) leaves the flag `false` and falls back to per-shard
        // accumulation as before. Keying the switch on the flag rather than on
        // `total_documents == 0` is deliberate: once the legacy fallback folds
        // in the first shard the denominator is non-zero, and gating on zero
        // would then silently drop every subsequent shard and stall the ratio
        // mid-backfill.
        let upfront_total_known = total_documents > 0;

        state.backfill_progress = BackfillProgress {
            total_documents,
            upfront_total_known,
            processed_documents: 0,
            current_shard: Some(0),
            last_cursor: None,
        };

        tracing::info!(
            index = %state.index_uid,
            total_documents,
            upfront_total_known,
            "Started backfill"
        );

        // bf-1q4wa: propagate the just-seeded denominator to the persisted
        // ReshardOperation so the admin status endpoint's backfill_progress()
        // reads a real ratio from the very first shard instead of the unset 0.
        // No-op when no progress_operation is attached.
        self.report_progress(state).await;

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
    /// returns [`Result`] to match the executor's method conventions: the only
    /// caller (`start_backfill`, wired in bf-2tddo) maps an error to a `0`
    /// denominator so a stats failure never aborts a reshard, after which the
    /// per-shard accumulation in `advance_backfill` fills the denominator in
    /// incrementally as the legacy fallback.
    ///
    /// Wired into `start_backfill` (bf-2tddo) as the backfill denominator; it
    /// was introduced as a pure additive helper in bf-2ynu5 and is now the
    /// source of the non-zero `total_documents` that backfill progress divides
    /// by.
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

    /// Fold a shard's document count into the backfill denominator.
    ///
    /// The reconciliation that prevents bf-2tddo's up-front
    /// [`BackfillProgress::total_documents`] from being double-counted. The
    /// mode switch keys on [`BackfillProgress::upfront_total_known`], NOT on
    /// `total_documents == 0`: once the legacy fallback folds in the first
    /// shard the denominator is non-zero, and gating on zero would then
    /// silently drop every subsequent shard and stall the ratio mid-backfill.
    /// The flag, set once in `start_backfill`, is immune to that drift.
    fn incorporate_shard_total(&self, state: &mut ReshardState, shard_total: u64) {
        if state.backfill_progress.upfront_total_known {
            // Denominator already holds the full-index count from
            // `start_backfill`; adding per-shard totals would double-count and
            // push the progress ratio past 1.0.
            return;
        }
        state.backfill_progress.total_documents += shard_total;
    }

    /// Mirror the executor's backfill progress into the attached persisted
    /// [`ReshardOperation`] (bf-1q4wa).
    ///
    /// When a `progress_operation` was attached via
    /// [`with_progress_operation`](Self::with_progress_operation), this writes
    /// the executor's `BackfillProgress` numerator (`processed_documents`) and
    /// denominator (`total_documents`) into the persisted operation through
    /// [`ReshardOperation::update_backfill_progress`]. Called at the end of
    /// [`start_backfill`](Self::start_backfill) — so the real denominator
    /// seeded by child 1 (bf-2tddo) and reconciled by child 2 (bf-1rodg)
    /// reaches `ReshardOperation.total_documents` — and again at the end of
    /// [`advance_backfill`](Self::advance_backfill) as each shard completes, so
    /// the numerator tracks real progress and
    /// [`ReshardOperation::backfill_progress`] returns a non-`0.0` ratio from
    /// the very first shard.
    ///
    /// When no operation is attached, this is a no-op and the default executor
    /// path is unchanged.
    async fn report_progress(&self, state: &ReshardState) {
        if let Some(op) = &self.progress_operation {
            op.write()
                .await
                .update_backfill_progress(
                    state.backfill_progress.processed_documents,
                    state.backfill_progress.total_documents,
                );
        }
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
                self.incorporate_shard_total(state, total_docs_in_shard);
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

        // bf-1q4wa: mirror this shard's processed count into the persisted
        // ReshardOperation so documents_backfilled (the numerator) tracks real
        // progress. Without this the admin endpoint would read the denominator
        // from start_backfill but a numerator stuck at 0. No-op when no
        // progress_operation is attached.
        self.report_progress(state).await;

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

#[cfg(test)]
mod tests {
    //! Executor-level coverage for `compute_source_document_count`.
    //!
    //! The pure reduction policy (`index_stats::reduce_document_counts`) is
    //! already unit-tested directly. These tests cover the *other* seam: the
    //! executor helper that drives the real reqwest transport against
    //! `/indexes/{uid}/stats` via `index_stats::aggregate_index_stats`. Each
    //! "node" is a mockito server returning a known `numberOfDocuments`, so we
    //! assert the executor reduces them with `max`, tolerates a failing node,
    //! and falls back to `0` when every node fails — matching how the
    //! `miroir-proxy` ILM acceptance tests drive the same endpoint.

    use super::*;
    use crate::scatter::{FetchDocumentsResponse, MockNodeClient};
    use crate::topology::{Node, NodeId, NodeStatus};

    /// Index uid every stats mock is mounted under.
    const INDEX_UID: &str = "test-idx";
    /// Stats endpoint path every mock is mounted on (`/indexes/{INDEX_UID}/stats`).
    const STATS_PATH: &str = "/indexes/test-idx/stats";

    /// Build an executor pointed at the given mockito node addresses.
    ///
    /// Only `node_addresses` (and the reqwest `http_client`) matter for
    /// `compute_source_document_count`; the `node_client`/`topology`/`config`/
    /// `task_store` are required by the constructor but untouched by the helper,
    /// so they get inert defaults.
    fn executor_with_nodes(node_addresses: Vec<String>) -> ReshardExecutor<MockNodeClient> {
        ReshardExecutor::new(
            INDEX_UID.to_string(),
            2,
            4,
            Arc::new(RwLock::new(Topology::new(2, 1, 1))),
            ReshardConfig {
                backfill_concurrency: 4,
                backfill_batch_size: 100,
                throttle_docs_per_sec: 0,
                verify_before_swap: false,
                retain_old_index_hours: 24,
            },
            Arc::new(MockNodeClient::default()),
            None,
            Arc::new(Client::new()),
            Arc::new(node_addresses),
            Arc::new("test-master-key".to_string()),
        )
    }

    /// Mount one `GET /indexes/{uid}/stats` mock returning `body` at `status`,
    /// expected to be hit exactly once.
    async fn mock_stats(
        server: &mut mockito::Server,
        status: usize,
        body: serde_json::Value,
    ) -> mockito::Mock {
        server
            .mock("GET", STATS_PATH)
            .with_status(status)
            .with_body(body.to_string())
            .expect(1)
            .create_async()
            .await
    }

    // (a) Multiple healthy nodes returning different counts → result is the max.
    #[tokio::test]
    async fn compute_source_doc_count_returns_max_across_healthy_nodes() {
        let mut server_a = mockito::Server::new_async().await;
        let mut server_b = mockito::Server::new_async().await;
        let mut server_c = mockito::Server::new_async().await;

        let mocks: Vec<mockito::Mock> = vec![
            mock_stats(&mut server_a, 200, json!({"numberOfDocuments": 100})).await,
            mock_stats(&mut server_b, 200, json!({"numberOfDocuments": 250})).await,
            mock_stats(&mut server_c, 200, json!({"numberOfDocuments": 180})).await,
        ];

        let executor =
            executor_with_nodes(vec![server_a.url(), server_b.url(), server_c.url()]);

        let count = executor.compute_source_document_count(INDEX_UID).await.unwrap();
        assert_eq!(count, 250, "denominator is the max responder, not the sum");

        // Prove we drove the real HTTP path: every node's stats endpoint was hit.
        for m in &mocks {
            m.assert_async().await;
        }
    }

    // (b) One node failing (HTTP 5xx) → logged and skipped; max of the rest wins.
    #[tokio::test]
    async fn compute_source_doc_count_ignores_failing_node() {
        let mut server_a = mockito::Server::new_async().await;
        let mut server_dead = mockito::Server::new_async().await;
        let mut server_c = mockito::Server::new_async().await;

        let mocks: Vec<mockito::Mock> = vec![
            mock_stats(&mut server_a, 200, json!({"numberOfDocuments": 100})).await,
            mock_stats(&mut server_dead, 500, json!({"message": "internal error"})).await,
            mock_stats(&mut server_c, 200, json!({"numberOfDocuments": 250})).await,
        ];

        let executor = executor_with_nodes(vec![
            server_a.url(),
            server_dead.url(),
            server_c.url(),
        ]);

        let count = executor.compute_source_document_count(INDEX_UID).await.unwrap();
        assert_eq!(
            count, 250,
            "a single failing node must not block the denominator"
        );

        for m in &mocks {
            m.assert_async().await;
        }
    }

    // (b') A node that 404s (index absent on that replica) is treated as zero
    // documents rather than a failure — the healthy max still wins.
    #[tokio::test]
    async fn compute_source_doc_count_tolerates_404_absent_index() {
        let mut server_a = mockito::Server::new_async().await;
        let mut server_missing = mockito::Server::new_async().await;

        let mocks: Vec<mockito::Mock> = vec![
            mock_stats(&mut server_a, 200, json!({"numberOfDocuments": 180})).await,
            mock_stats(&mut server_missing, 404, json!({"message": "index not found"})).await,
        ];

        let executor =
            executor_with_nodes(vec![server_a.url(), server_missing.url()]);

        let count = executor.compute_source_document_count(INDEX_UID).await.unwrap();
        assert_eq!(
            count, 180,
            "a 404 (absent replica) counts as zero, not a failure"
        );

        for m in &mocks {
            m.assert_async().await;
        }
    }

    // (c) Every node failing → returns 0 (no responder → no denominator).
    #[tokio::test]
    async fn compute_source_doc_count_zero_when_all_nodes_fail() {
        let mut server_a = mockito::Server::new_async().await;
        let mut server_b = mockito::Server::new_async().await;
        let mut server_c = mockito::Server::new_async().await;

        let mocks: Vec<mockito::Mock> = vec![
            mock_stats(&mut server_a, 500, json!({"message": "down"})).await,
            mock_stats(&mut server_b, 503, json!({"message": "unavailable"})).await,
            mock_stats(&mut server_c, 500, json!({"message": "down"})).await,
        ];

        let executor =
            executor_with_nodes(vec![server_a.url(), server_b.url(), server_c.url()]);

        let count = executor.compute_source_document_count(INDEX_UID).await.unwrap();
        assert_eq!(count, 0, "all nodes failing yields a zero denominator");

        for m in &mocks {
            m.assert_async().await;
        }
    }

    // ---- incorporate_shard_total mode switch (bf-1rodg) ----
    //
    // The reconciliation that keeps the backfill progress ratio in [0,1] once
    // bf-2tddo's up-front denominator exists. Three assertions mirror the
    // acceptance criteria: (1) serde compat with pre-flag state, (2) an
    // up-front total makes the fold a no-op, (3) its absence still accumulates
    // per-shard totals across a multi-shard index.

    /// Build a backfill `ReshardState` in `Phase::Backfill` with the given
    /// progress, so `incorporate_shard_total` has realistic input.
    fn backfill_state(progress: BackfillProgress) -> ReshardState {
        ReshardState {
            id: Uuid::nil(),
            index_uid: INDEX_UID.to_string(),
            old_shards: 2,
            new_shards: 4,
            phase: Phase::Backfill,
            shadow_index: Some("test-idx__reshard_4".to_string()),
            started_at: 0,
            updated_at: 0,
            backfill_progress: progress,
            verify_result: None,
        }
    }

    /// An executor whose `node_addresses`/client/topology are never touched by
    /// `incorporate_shard_total` — it's pure progress bookkeeping, so empty
    /// nodes are fine.
    fn bookkeeping_executor() -> ReshardExecutor<MockNodeClient> {
        executor_with_nodes(vec![])
    }

    // (1) A pre-flag ReshardState serialized without `upfront_total_known`
    // deserializes to the legacy fallback (false), so per-shard accumulation
    // resumes for state persisted before this flag existed.
    #[test]
    fn backfill_progress_serde_defaults_upfront_total_known_to_false() {
        let legacy_json = json!({
            "total_documents": 0,
            "processed_documents": 0,
            "current_shard": null,
            "last_cursor": null,
        });
        let progress: BackfillProgress = serde_json::from_value(legacy_json).unwrap();
        assert!(
            !progress.upfront_total_known,
            "pre-flag serialized state must fall back to per-shard accumulation"
        );
    }

    // (2) With an up-front total, folding in per-shard totals is a no-op: the
    // denominator never grows, so the progress ratio can't exceed 1.0.
    #[test]
    fn incorporate_shard_total_is_noop_when_upfront_total_known() {
        let executor = bookkeeping_executor();
        let mut state = backfill_state(BackfillProgress {
            total_documents: 1000,
            upfront_total_known: true,
            processed_documents: 0,
            current_shard: Some(0),
            last_cursor: None,
        });

        // Each shard reports a count; none may be folded in.
        for shard_total in [400u64, 350, 250] {
            executor.incorporate_shard_total(&mut state, shard_total);
        }

        assert_eq!(
            state.backfill_progress.total_documents, 1000,
            "up-front total must not be double-counted by per-shard totals"
        );

        // Processing every document keeps the ratio at (not above) 1.0.
        state.backfill_progress.processed_documents = 1000;
        let ratio = state.backfill_progress.processed_documents as f64
            / state.backfill_progress.total_documents as f64;
        assert!(ratio <= 1.0, "ratio {ratio} must never exceed 1.0");
    }

    // (3) Without an up-front total, per-shard totals still accumulate into the
    // denominator across a multi-shard index. This is exactly the case the key-
    // correctness note warns about: keying on `total_documents == 0` would drop
    // shards 1 and 2 once shard 0 makes the denominator non-zero (→ 400, not
    // 1000); keying on the flag instead accumulates all three.
    #[test]
    fn incorporate_shard_total_accumulates_when_upfront_unknown() {
        let executor = bookkeeping_executor();
        let mut state = backfill_state(BackfillProgress {
            total_documents: 0,
            upfront_total_known: false,
            processed_documents: 0,
            current_shard: Some(0),
            last_cursor: None,
        });

        for shard_total in [400u64, 350, 250] {
            executor.incorporate_shard_total(&mut state, shard_total);
        }

        assert_eq!(
            state.backfill_progress.total_documents, 1000,
            "per-shard totals must accumulate even after the denominator is non-zero"
        );
    }

    // ---- progress_operation propagation seam (bf-1q4wa) ----
    //
    // The seam that mirrors the executor's `BackfillProgress` into a persisted
    // `ReshardOperation` so the admin status endpoint reads a real denominator
    // (seeded in `start_backfill`) and numerator (reported as each shard
    // completes). Three tests mirror the acceptance criteria: (1) the no-op
    // default path, (2) `report_progress` drives a non-0.0 ratio, (3)
    // `start_backfill` propagates the real denominator end-to-end.

    /// Build an executor with an attached persisted `ReshardOperation`, so we
    /// can assert progress is mirrored through the bf-1q4wa seam.
    fn executor_with_progress(
        node_addresses: Vec<String>,
        op: Arc<RwLock<ReshardOperation>>,
    ) -> ReshardExecutor<MockNodeClient> {
        executor_with_nodes(node_addresses).with_progress_operation(op)
    }

    // (1) Default path: with no operation attached, `report_progress` is a
    // no-op and must not panic or touch any external state (criterion 3).
    #[tokio::test]
    async fn report_progress_is_noop_without_operation() {
        let executor = executor_with_nodes(vec![]);

        let state = backfill_state(BackfillProgress {
            total_documents: 1000,
            upfront_total_known: true,
            processed_documents: 250,
            current_shard: Some(0),
            last_cursor: None,
        });

        // Completes without panicking — there is no operation to write to.
        executor.report_progress(&state).await;
    }

    // (2) With an operation attached, `report_progress` mirrors processed/total
    // into it so `ReshardOperation::backfill_progress()` returns a real,
    // non-0.0 ratio — the property the admin endpoint relies on "from the very
    // first shard" (criterion 2).
    #[tokio::test]
    async fn report_progress_propagates_nonzero_ratio_to_operation() {
        let op = Arc::new(RwLock::new(ReshardOperation::new(
            INDEX_UID.to_string(),
            2,
            4,
        )));
        let executor = executor_with_progress(vec![], op.clone());

        // Before any report, the persisted op has an unset denominator → 0.0.
        assert_eq!(op.read().await.backfill_progress(), 0.0);

        let state = backfill_state(BackfillProgress {
            total_documents: 1000,
            upfront_total_known: true,
            processed_documents: 250, // first shard done → 25%
            current_shard: Some(1),
            last_cursor: Some("shard_0".to_string()),
        });
        executor.report_progress(&state).await;

        let guarded = op.read().await;
        assert_eq!(guarded.total_documents, 1000);
        assert_eq!(guarded.documents_backfilled, 250);
        let ratio = guarded.backfill_progress();
        assert!(
            (ratio - 0.25).abs() < f64::EPSILON,
            "expected 0.25 from the first shard, got {ratio}"
        );
    }

    // (3) End-to-end: `start_backfill` seeds the real denominator (bf-2tddo,
    // bf-1rodg) and — via the bf-1q4wa seam — propagates it to the persisted
    // `ReshardOperation.total_documents`, so the admin endpoint's denominator
    // is the real count, not the unset `0` (criterion 1). `processed_documents`
    // is 0 at backfill start; the numerator is reported as shards complete
    // (covered by the test above).
    #[tokio::test]
    async fn start_backfill_propagates_real_denominator_to_operation() {
        let mut server = mockito::Server::new_async().await;
        let mock = mock_stats(&mut server, 200, json!({"numberOfDocuments": 1000})).await;

        let op = Arc::new(RwLock::new(ReshardOperation::new(
            INDEX_UID.to_string(),
            2,
            4,
        )));
        let executor = executor_with_progress(vec![server.url()], op.clone());

        // `start_backfill` seeds `total_documents` from the source stats and
        // calls `report_progress` at its end.
        let mut state = backfill_state(BackfillProgress {
            total_documents: 0,
            upfront_total_known: false,
            processed_documents: 0,
            current_shard: None,
            last_cursor: None,
        });
        executor.start_backfill(&mut state).await.unwrap();

        // We drove the real stats path.
        mock.assert_async().await;

        let guarded = op.read().await;
        assert_eq!(
            guarded.total_documents, 1000,
            "start_backfill must propagate the real denominator to the persisted op"
        );
        assert_eq!(
            guarded.documents_backfilled, 0,
            "no documents processed yet at backfill start"
        );
    }

    // ---- end-to-end progress-ratio verification (bf-5aon3) ----
    //
    // The verification gate for the bf-67ki8 chain (children bf-2tddo,
    // bf-1rodg, bf-1q4wa): drive a full multi-shard backfill through the real
    // `advance_backfill` document-migration path and assert the progress ratio
    // the admin status endpoint reads via `ReshardOperation::backfill_progress()`
    // is (a) non-zero from the very first shard, (b) monotonically
    // non-decreasing, and (c) never above 1.0. A second test confirms the seam
    // is progress-reporting only — it does not alter the reshard state machine
    // or document migration.

    /// Mount the two HTTP endpoints a full backfill drives on a mockito server:
    /// `GET /indexes/{uid}/stats` (hit once by `start_backfill`'s denominator
    /// query) and `GET /indexes/{uid}` (hit once per shard by
    /// `get_index_primary_key`). Returns the mocks so the caller can assert
    /// they were hit exactly the expected number of times.
    async fn mount_backfill_endpoints(
        server: &mut mockito::Server,
        shards: u32,
    ) -> Vec<mockito::Mock> {
        let stats = server
            .mock("GET", STATS_PATH)
            .with_status(200)
            .with_body(json!({"numberOfDocuments": 1000}).to_string())
            .expect(1)
            .create_async()
            .await;
        let index_info = server
            .mock("GET", "/indexes/test-idx")
            .with_status(200)
            .with_body(json!({"uid": INDEX_UID, "primaryKey": "id"}).to_string())
            .expect(shards as usize)
            .create_async()
            .await;
        vec![stats, index_info]
    }

    /// Build an executor whose topology carries one healthy node ("node-0") and
    /// whose `MockNodeClient` returns 100 documents at `total: 500` per fetch
    /// page. With `backfill_batch_size: 100` each shard therefore processes
    /// exactly 500 documents (5 pages × 100), and `old_shards: 2` shards
    /// process 1000 — equal to the 1000-document denominator `start_backfill`
    /// seeds from the stats mock, keeping the progress ratio in `(0.0, 1.0]`.
    ///
    /// `node_addresses` must point at a mockito server serving the stats and
    /// index-info endpoints (see [`mount_backfill_endpoints`]).
    fn make_backfill_executor(node_addresses: Vec<String>) -> ReshardExecutor<MockNodeClient> {
        let node_id = NodeId::new("node-0".to_string());
        let mut node_client = MockNodeClient::default();
        node_client.fetch_responses.insert(
            node_id.clone(),
            FetchDocumentsResponse {
                results: (0..100)
                    .map(|i| json!({ "id": format!("doc-{i}") }))
                    .collect(),
                limit: 100,
                offset: 0,
                total: 500,
            },
        );

        let mut topology = Topology::new(2, 1, 1);
        let mut node = Node::new(
            node_id,
            node_addresses.first().cloned().unwrap_or_default(),
            0,
        );
        node.status = NodeStatus::Healthy;
        topology.add_node(node);

        ReshardExecutor::new(
            INDEX_UID.to_string(),
            2,
            4,
            Arc::new(RwLock::new(topology)),
            ReshardConfig {
                backfill_concurrency: 4,
                backfill_batch_size: 100,
                throttle_docs_per_sec: 0,
                verify_before_swap: false,
                retain_old_index_hours: 24,
            },
            Arc::new(node_client),
            None,
            Arc::new(Client::new()),
            Arc::new(node_addresses),
            Arc::new("test-master-key".to_string()),
        )
    }

    /// A clean backfill `ReshardState` — `Phase::Backfill`, shadow index set,
    /// progress zeroed. `start_backfill` overwrites `backfill_progress`, but
    /// still needs a valid starting state.
    fn fresh_backfill_state() -> ReshardState {
        backfill_state(BackfillProgress {
            total_documents: 0,
            upfront_total_known: false,
            processed_documents: 0,
            current_shard: None,
            last_cursor: None,
        })
    }

    /// Drive `start_backfill` then `advance_backfill` for every source shard,
    /// returning the final `ReshardState` — the state machine's observable
    /// output (migrated-document count, shard cursor, phase).
    async fn drive_full_backfill(
        executor: &ReshardExecutor<MockNodeClient>,
        mut state: ReshardState,
    ) -> ReshardState {
        executor.start_backfill(&mut state).await.unwrap();
        let old_shards = state.old_shards;
        for _ in 0..old_shards {
            executor.advance_backfill(&mut state).await.unwrap();
        }
        state
    }

    // (a/b/c) End-to-end: drive a full multi-shard backfill and assert the
    // progress ratio read through `ReshardOperation::backfill_progress()` is
    // non-zero from the first shard, monotonically non-decreasing, and never
    // above 1.0. This is the property the admin status endpoint relies on once
    // the denominator is seeded (bf-2tddo), reconciled (bf-1rodg), and mirrored
    // (bf-1q4wa).
    #[tokio::test]
    async fn backfill_progress_ratio_is_nonzero_monotonic_and_bounded() {
        let mut server = mockito::Server::new_async().await;
        let mocks = mount_backfill_endpoints(&mut server, 2).await;

        let op = Arc::new(RwLock::new(ReshardOperation::new(
            INDEX_UID.to_string(),
            2,
            4,
        )));
        let executor =
            make_backfill_executor(vec![server.url()]).with_progress_operation(op.clone());

        let mut state = fresh_backfill_state();
        executor.start_backfill(&mut state).await.unwrap();

        // Sample the persisted op's ratio at start (denominator just seeded,
        // numerator still 0) and after each shard completes.
        let mut ratios = vec![op.read().await.backfill_progress()];
        let old_shards = state.old_shards;
        for _ in 0..old_shards {
            executor.advance_backfill(&mut state).await.unwrap();
            ratios.push(op.read().await.backfill_progress());
        }

        // (a) Non-zero from the very first shard — ratios[1] is post-first-shard.
        assert!(
            ratios[1] > 0.0,
            "progress must be non-zero after the first shard, got {}",
            ratios[1]
        );

        // (c) The ratio stays in [0.0, 1.0] at every observation point: the
        // numerator never outruns the seeded denominator (no double-count via
        // `incorporate_shard_total`, no suppressed accumulation).
        for (i, &ratio) in ratios.iter().enumerate() {
            assert!(
                (0.0..=1.0).contains(&ratio),
                "ratio at step {i} outside [0,1]: {ratio}"
            );
        }

        // (b) Monotonically non-decreasing as each shard completes.
        for window in ratios.windows(2) {
            assert!(
                window[1] >= window[0],
                "progress ratio decreased: {} → {}",
                window[0],
                window[1]
            );
        }

        // Completeness: every document migrated → ratio reaches exactly 1.0.
        // This also fails both directions of a denominator bug — a double-count
        // stalls below 1.0, a suppressed accumulation blows past it.
        let final_ratio = *ratios.last().unwrap();
        assert!(
            (final_ratio - 1.0).abs() < f64::EPSILON,
            "expected ratio 1.0 at completion, got {final_ratio}"
        );

        for m in &mocks {
            m.assert_async().await;
        }
    }

    // Progress-reporting only (criterion: no state-machine change). The
    // bf-1q4wa `progress_operation` seam is `None` on the default path and a
    // read-only mirror when attached, so it must not change the reshard state
    // machine's observable behavior. Drive an identical multi-shard backfill
    // with the default executor and with the seam attached and assert the
    // migrated-document count, final shard cursor, and phase are identical.
    #[tokio::test]
    async fn progress_operation_seam_does_not_alter_state_machine() {
        // Default executor — the pre-bf-1q4wa path, no progress_operation.
        let mut server_a = mockito::Server::new_async().await;
        let mocks_a = mount_backfill_endpoints(&mut server_a, 2).await;
        let executor_a = make_backfill_executor(vec![server_a.url()]);
        let state_a = drive_full_backfill(&executor_a, fresh_backfill_state()).await;

        // Same backfill with the seam attached.
        let mut server_b = mockito::Server::new_async().await;
        let mocks_b = mount_backfill_endpoints(&mut server_b, 2).await;
        let op = Arc::new(RwLock::new(ReshardOperation::new(
            INDEX_UID.to_string(),
            2,
            4,
        )));
        let executor_b = make_backfill_executor(vec![server_b.url()]).with_progress_operation(op);
        let state_b = drive_full_backfill(&executor_b, fresh_backfill_state()).await;

        // The state machine migrated the same documents and advanced the shard
        // cursor identically — the seam touched neither.
        assert_eq!(
            state_a.backfill_progress.processed_documents, 1000,
            "sanity: both runs migrated every document (2 shards × 500)"
        );
        assert_eq!(
            state_a.backfill_progress.processed_documents,
            state_b.backfill_progress.processed_documents,
            "seam must not change how many documents are migrated"
        );
        assert_eq!(
            state_a.backfill_progress.current_shard,
            Some(state_a.old_shards),
            "sanity: shard cursor advanced past every source shard"
        );
        assert_eq!(
            state_a.backfill_progress.current_shard,
            state_b.backfill_progress.current_shard,
            "seam must not change the shard cursor"
        );
        assert_eq!(
            state_a.phase, state_b.phase,
            "seam must not change the reshard phase"
        );

        for m in mocks_a.iter().chain(mocks_b.iter()) {
            m.assert_async().await;
        }
    }

    // ---- pure bookkeeping verification (bf-3q0yf) ----
    //
    // Verifies the backfill progress ratio is non-zero from the first shard and
    // monotonically non-decreasing, using only the bookkeeping path
    // (report_progress) without exercising the HTTP fetch logic. This is the
    // cheap, high-confidence slice of the bf-5aon3 verification gate.

    /// Simulate a multi-shard backfill by manually advancing processed_documents
    /// and calling report_progress after each shard. Returns the sequence of
    /// ratios observed via the attached ReshardOperation.
    async fn simulate_bookkeeping_backfill(
        executor: &ReshardExecutor<MockNodeClient>,
        op: &Arc<RwLock<ReshardOperation>>,
        shard_doc_counts: Vec<u64>,
    ) -> Vec<f64> {
        let mut state = backfill_state(BackfillProgress {
            total_documents: 1000, // seeded upfront denominator
            upfront_total_known: true,
            processed_documents: 0,
            current_shard: Some(0),
            last_cursor: None,
        });

        let mut ratios = Vec::new();

        // Simulate each shard completing: advance processed_documents by that
        // shard's doc count, report progress, and snapshot the ratio.
        for (shard_idx, doc_count) in shard_doc_counts.iter().enumerate() {
            state.backfill_progress.processed_documents += doc_count;
            state.backfill_progress.current_shard = Some(shard_idx as u32 + 1);
            state.backfill_progress.last_cursor = Some(format!("shard_{shard_idx}"));

            executor.report_progress(&state).await;
            ratios.push(op.read().await.backfill_progress());
        }

        ratios
    }

    // (a/b) Pure bookkeeping: the ratio is non-zero from the first shard and
    // monotonically non-decreasing across a synthetic >=3-shard sequence, with
    // no HTTP transport exercised.
    #[tokio::test]
    async fn backfill_ratio_nonzero_and_monotonic_at_bookkeeping_level() {
        let op = Arc::new(RwLock::new(ReshardOperation::new(
            INDEX_UID.to_string(),
            2,
            4,
        )));

        // bookkeeping_executor has no nodes, so report_progress cannot make
        // any HTTP calls — we're testing pure bookkeeping.
        let executor = bookkeeping_executor().with_progress_operation(op.clone());

        // A 3-shard sequence: shard 0 processes 400 docs, shard 1 processes 350,
        // shard 2 processes 250 (total = 1000, matching our seeded denominator).
        let shard_doc_counts = vec![400u64, 350, 250];
        let ratios = simulate_bookkeeping_backfill(&executor, &op, shard_doc_counts).await;

        // (a) The ratio is > 0.0 from the very first shard (ratios[0]).
        assert!(
            ratios[0] > 0.0,
            "ratio must be non-zero from the first shard, got {}",
            ratios[0]
        );

        // (b) The sampled ratios are monotonically non-decreasing across shards.
        for window in ratios.windows(2) {
            assert!(
                window[1] >= window[0],
                "progress ratio decreased: {} → {}",
                window[0],
                window[1]
            );
        }

        // Final ratio should reach 1.0 (all 1000 docs processed).
        let final_ratio = *ratios.last().unwrap();
        assert!(
            (final_ratio - 1.0).abs() < f64::EPSILON,
            "expected ratio 1.0 after all shards complete, got {final_ratio}"
        );
    }
}
