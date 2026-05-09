//! §13.1 Online resharding via shadow index - executor implementation
//!
//! Six-phase resharding process:
//! 1. Shadow create
//! 2. Dual-hash dual-write
//! 3. Backfill
//! 4. Verify
//! 5. Alias swap
//! 6. Cleanup

use crate::error::{MiroirError, Result};
use crate::topology::Topology;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
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
pub struct ReshardExecutor {
    state: Arc<RwLock<ReshardState>>,
    topology: Arc<RwLock<Topology>>,
    config: ReshardConfig,
}

#[derive(Debug, Clone)]
pub struct ReshardConfig {
    pub backfill_concurrency: usize,
    pub backfill_batch_size: usize,
    pub throttle_docs_per_sec: u64,
    pub verify_before_swap: bool,
    pub retain_old_index_hours: u64,
}

impl ReshardExecutor {
    /// Create a new resharding operation.
    pub fn new(
        index_uid: String,
        old_shards: u32,
        new_shards: u32,
        topology: Arc<RwLock<Topology>>,
        config: ReshardConfig,
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
                let verify_passed = state.verify_result.as_ref()
                    .map(|v| v.passed)
                    .unwrap_or(false);

                if !verify_passed {
                    return Err(MiroirError::VerificationFailed(
                        "Resharding verification failed".to_string()
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
        Ok(state.backfill_progress.current_shard
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
            let delay_ms = (self.config.backfill_batch_size as u64 * 1000)
                / self.config.throttle_docs_per_sec;
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }

        Ok(())
    }

    /// Phase 4: Verify shadow index matches live index.
    async fn run_verify(&self, state: &mut ReshardState) -> Result<()> {
        tracing::info!(
            index = %state.index_uid,
            "Running cross-index verification"
        );

        let mismatches = Vec::new();

        // For each shard in both old and new indexes:
        // 1. Fetch all primary keys
        // 2. Compare content hashes
        // 3. Report mismatches

        // TODO: Implement bucketed Merkle comparison
        // This reuses §13.8's bucketed-Merkle machinery

        state.verify_result = Some(VerifyResult {
            passed: mismatches.is_empty(),
            mismatches,
            fingerprint_live: "".to_string(), // TODO: compute actual fingerprint
            fingerprint_shadow: "".to_string(),
        });

        Ok(())
    }

    /// Phase 5: Atomic alias swap.
    async fn alias_swap(&self, state: &mut ReshardState) -> Result<()> {
        let shadow_name = state.shadow_index.as_ref()
            .ok_or_else(|| MiroirError::InvalidState("Shadow index not created".to_string()))?;

        tracing::info!(
            index = %state.index_uid,
            shadow = %shadow_name,
            "Performing atomic alias swap"
        );

        // TODO: Use §13.7 atomic alias flip
        // PUT /_miroir/aliases/{index_uid} {"target": shadow_name}

        Ok(())
    }

    /// Rollback the resharding operation (before phase 5).
    pub async fn rollback(&self) -> Result<()> {
        let mut state = self.state.write().await;

        if state.phase >= Phase::Swap {
            return Err(MiroirError::InvalidState(
                "Cannot rollback after alias swap".to_string()
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
