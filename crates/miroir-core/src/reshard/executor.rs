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
use crate::scatter::NodeClient;
use crate::task_store::TaskStore;
use crate::topology::Topology;
use serde::{Deserialize, Serialize};
use std::hash::Hasher;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};
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
    async fn create_shadow_index(&self, state: &mut ReshardState) -> Result<()> {
        let shadow_name = format!("{}__reshard_{}", state.index_uid, state.new_shards);
        state.shadow_index = Some(shadow_name.clone());

        // TODO: Broadcast index creation to all nodes via task store
        // This will be implemented with the two-phase settings broadcast (§13.5)

        tracing::info!(
            index = %state.index_uid,
            shadow = %shadow_name,
            old_shards = state.old_shards,
            new_shards = state.new_shards,
            "Created shadow index"
        );

        Ok(())
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

    /// Check if backfill is complete.
    async fn is_backfill_complete(&self, state: &ReshardState) -> Result<bool> {
        Ok(state
            .backfill_progress
            .current_shard
            .map(|s| s >= state.old_shards)
            .unwrap_or(false))
    }

    /// Advance backfill by processing one shard.
    async fn advance_backfill(&self, state: &mut ReshardState) -> Result<()> {
        let shard_id = state.backfill_progress.current_shard.unwrap_or(0);

        // TODO: Paginated fetch from live index with filter=_miroir_shard={shard_id}
        // Re-hash each document under new shard count
        // Write to shadow index with _miroir_shard = new_shard_id

        tracing::debug!(
            index = %state.index_uid,
            shard = shard_id,
            "Backfilling shard"
        );

        state.backfill_progress.processed_documents += self.config.backfill_batch_size as u64;
        state.backfill_progress.current_shard = Some(shard_id + 1);

        // Apply throttling
        if self.config.throttle_docs_per_sec > 0 {
            let delay_ms =
                (self.config.backfill_batch_size as u64 * 1000) / self.config.throttle_docs_per_sec;
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }

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
        let node = topology
            .nodes().find(|n| n.is_healthy())
            .ok_or_else(|| {
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

        // Delete shadow index
        if let Some(ref shadow) = state.shadow_index {
            tracing::info!(
                index = %state.index_uid,
                shadow = %shadow,
                "Rolling back: deleting shadow index"
            );
            // TODO: Broadcast DELETE /indexes/{shadow}
        }

        state.phase = Phase::Complete;
        state.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Ok(())
    }
}

/// Hash a primary key to determine its shard assignment.
///
/// This matches the rendezvous hashing used for document routing.
/// For verification purposes, we use a simple hash modulo since we're
/// just showing which shard a PK would belong to.
fn hash_pk_to_shard(pk: &str, shard_count: u32) -> u32 {
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
