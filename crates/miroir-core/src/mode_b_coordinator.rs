//! Mode B leader-only singleton coordinator (plan §14.5).
//!
//! Provides leader election and phase state persistence for all Mode B operations:
//! - Reshard coordinator (plan §13.1)
//! - Phase 4 rebalancer (plan §13.2)
//! - Alias flip serializer (plan §13.7)
//! - Two-phase settings broadcast (plan §13.5)
//! - ILM evaluator (plan §13.17)
//! - Scoped-key rotation (plan §13.21)
//!
//! All Mode B operations are designed to be idempotent and safe to resume at
//! phase boundaries. When a leader is lost, a new leader reads the persisted
//! phase state from the task store and resumes from the last committed phase.

use crate::error::{MiroirError, Result};
use crate::leader_election::LeaderElection;
use crate::task_store::{ModeBOperation, ModeBOperationFilter, TaskStore, mode_b_status, mode_b_type};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info, warn, error};

/// Phase state for a Mode B operation.
///
/// Each operation type has its own phase enum, but they all share common
/// properties: phase name, started timestamp, and optional error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseState {
    /// Current phase name (operation-specific).
    pub phase: String,
    /// Phase started at (UNIX ms).
    pub phase_started_at: i64,
    /// Error message if phase failed.
    pub error: Option<String>,
}

impl PhaseState {
    /// Create a new phase state.
    pub fn new(phase: String) -> Self {
        let now = millis_now();
        Self {
            phase,
            phase_started_at: now,
            error: None,
        }
    }

    /// Transition to a new phase.
    pub fn advance(&mut self, new_phase: String) {
        self.phase = new_phase;
        self.phase_started_at = millis_now();
        self.error = None;
    }

    /// Mark phase as failed.
    pub fn fail(&mut self, error: String) {
        self.error = Some(error);
    }
}

/// Leader state for a Mode B operation.
///
/// Combines leader election with phase state persistence.
pub struct ModeBOpLeader<E> {
    /// Leader election service.
    leader_election: Arc<LeaderElection>,
    /// Task store for phase persistence.
    task_store: Arc<dyn TaskStore>,
    /// Operation type (reshard, rebalance, etc.).
    operation_type: String,
    /// Lease scope (e.g., "reshard:my-index", "ilm").
    scope: String,
    /// Pod ID.
    pod_id: String,
    /// Phase state (in-memory copy of persisted state).
    phase_state: PhaseState,
    /// Whether we are currently the leader.
    is_leader: bool,
    /// Extra state for the operation (reshard state, ILM state, etc.).
    extra_state: E,
}

impl<E: Serialize + for<'de> Deserialize<'de>> ModeBOpLeader<E> {
    /// Create a new Mode B operation leader.
    pub fn new(
        leader_election: Arc<LeaderElection>,
        task_store: Arc<dyn TaskStore>,
        operation_type: String,
        scope: String,
        pod_id: String,
        extra_state: E,
    ) -> Self {
        let phase_state = PhaseState::new("idle".to_string());
        Self {
            leader_election,
            task_store,
            operation_type,
            scope,
            pod_id,
            phase_state,
            is_leader: false,
            extra_state,
        }
    }

    /// Try to acquire the leader lease for this operation.
    ///
    /// Returns `Ok(true)` if we are now the leader, `Ok(false)` if another
    /// pod holds the lease, or `Err` if acquisition failed.
    pub async fn try_acquire_leadership(&mut self) -> Result<bool> {
        let acquired = self.leader_election.try_acquire_async(&self.scope).await?;
        self.is_leader = acquired;

        if acquired {
            info!(
                operation_type = %self.operation_type,
                scope = %self.scope,
                pod_id = %self.pod_id,
                "acquired Mode B leader lease"
            );

            // Try to recover existing operation state
            if let Some(existing) = self.task_store.get_mode_b_operation_by_scope(&self.scope)? {
                // Resume from existing phase state
                self.phase_state = PhaseState {
                    phase: existing.phase,
                    phase_started_at: existing.phase_started_at,
                    error: existing.error,
                };
                info!(
                    operation_type = %self.operation_type,
                    scope = %self.scope,
                    phase = %self.phase_state.phase,
                    "resumed Mode B operation from persisted phase"
                );
            } else {
                // New operation - persist initial state
                self.persist_phase("idle".to_string()).await?;
            }
        }

        Ok(acquired)
    }

    /// Renew the leader lease.
    ///
    /// Returns `Ok(true)` if renewed successfully, `Ok(false)` if we lost
    /// leadership to another pod, or `Err` if renewal failed.
    pub async fn renew_leadership(&mut self) -> Result<bool> {
        if !self.is_leader {
            return Ok(false);
        }

        let renewed = self.leader_election.renew_async(&self.scope).await?;

        if !renewed {
            warn!(
                operation_type = %self.operation_type,
                scope = %self.scope,
                "lost Mode B leader lease during renewal"
            );
            self.is_leader = false;
        }

        Ok(renewed)
    }

    /// Step down from leadership.
    ///
    /// Releases the lease voluntarily. Returns `Ok(true)` if we held the
    /// lease and stepped down, `Ok(false)` if we didn't hold it.
    pub async fn step_down(&mut self) -> Result<bool> {
        let held = self.leader_election.step_down_async(&self.scope).await?;
        self.is_leader = false;
        Ok(held)
    }

    /// Check if we are currently the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }

    /// Get the current phase.
    pub fn phase(&self) -> &str {
        &self.phase_state.phase
    }

    /// Get a mutable reference to the extra state.
    pub fn extra_state(&mut self) -> &mut E {
        &mut self.extra_state
    }

    /// Get a reference to the extra state.
    pub fn extra_state_ref(&self) -> &E {
        &self.extra_state
    }

    /// Persist a phase transition.
    ///
    /// Should be called after each phase boundary so that a new leader can
    /// resume from the last committed phase.
    pub async fn persist_phase(&mut self, new_phase: String) -> Result<()> {
        self.phase_state.advance(new_phase.clone());

        let operation = ModeBOperation {
            operation_id: format!("{}:{}", self.scope, self.pod_id),
            operation_type: self.operation_type.clone(),
            scope: self.scope.clone(),
            phase: new_phase,
            phase_started_at: self.phase_state.phase_started_at,
            created_at: millis_now(),
            updated_at: millis_now(),
            state_json: serde_json::to_string(&self.extra_state)
                .map_err(|e| MiroirError::TaskStore(format!("failed to serialize extra state: {}", e)))?,
            error: self.phase_state.error.clone(),
            status: mode_b_status::RUNNING.to_string(),
            // Default values (reshard-specific)
            index_uid: None,
            old_shards: None,
            target_shards: None,
            shadow_index: None,
            documents_backfilled: None,
            total_documents: None,
        };

        self.task_store.upsert_mode_b_operation(&operation)?;

        debug!(
            operation_type = %self.operation_type,
            scope = %self.scope,
            phase = %self.phase_state.phase,
            "persisted Mode B operation phase"
        );

        Ok(())
    }

    /// Mark the operation as failed.
    pub async fn fail(&mut self, error: String) -> Result<()> {
        self.phase_state.fail(error.clone());

        let operation = ModeBOperation {
            operation_id: format!("{}:{}", self.scope, self.pod_id),
            operation_type: self.operation_type.clone(),
            scope: self.scope.clone(),
            phase: self.phase_state.phase.clone(),
            phase_started_at: self.phase_state.phase_started_at,
            created_at: millis_now(),
            updated_at: millis_now(),
            state_json: serde_json::to_string(&self.extra_state)
                .map_err(|e| MiroirError::TaskStore(format!("failed to serialize extra state: {}", e)))?,
            error: Some(error),
            status: mode_b_status::FAILED.to_string(),
            index_uid: None,
            old_shards: None,
            target_shards: None,
            shadow_index: None,
            documents_backfilled: None,
            total_documents: None,
        };

        self.task_store.upsert_mode_b_operation(&operation)?;

        // Step down from leadership on failure
        let _ = self.step_down().await;

        Ok(())
    }

    /// Mark the operation as completed.
    pub async fn complete(&mut self) -> Result<()> {
        let operation = ModeBOperation {
            operation_id: format!("{}:{}", self.scope, self.pod_id),
            operation_type: self.operation_type.clone(),
            scope: self.scope.clone(),
            phase: "complete".to_string(),
            phase_started_at: self.phase_state.phase_started_at,
            created_at: millis_now(),
            updated_at: millis_now(),
            state_json: serde_json::to_string(&self.extra_state)
                .map_err(|e| MiroirError::TaskStore(format!("failed to serialize extra state: {}", e)))?,
            error: None,
            status: mode_b_status::COMPLETED.to_string(),
            index_uid: None,
            old_shards: None,
            target_shards: None,
            shadow_index: None,
            documents_backfilled: None,
            total_documents: None,
        };

        self.task_store.upsert_mode_b_operation(&operation)?;

        info!(
            operation_type = %self.operation_type,
            scope = %self.scope,
            "Mode B operation completed"
        );

        // Step down from leadership
        let _ = self.step_down().await;

        Ok(())
    }

    /// Delete the operation state.
    pub async fn delete(&self) -> Result<bool> {
        let operation_id = format!("{}:{}", self.scope, self.pod_id);
        self.task_store.delete_mode_b_operation(&operation_id)
    }

    /// Recover the operation state from the task store.
    ///
    /// Called by a new leader to read the persisted phase state and resume
    /// from the last committed phase boundary.
    pub async fn recover(&mut self) -> Result<Option<ModeBOperation>> {
        let existing = self.task_store.get_mode_b_operation_by_scope(&self.scope)?;

        if let Some(ref op) = existing {
            // Resume phase state
            self.phase_state = PhaseState {
                phase: op.phase.clone(),
                phase_started_at: op.phase_started_at,
                error: op.error.clone(),
            };

            // Resume extra state if present
            if !op.state_json.is_empty() {
                self.extra_state = serde_json::from_str(&op.state_json)
                    .map_err(|e| MiroirError::TaskStore(format!("failed to deserialize extra state: {}", e)))?;
            }

            info!(
                operation_type = %self.operation_type,
                scope = %self.scope,
                phase = %op.phase,
                "recovered Mode B operation state"
            );
        }

        Ok(existing)
    }
}

/// Get current time in milliseconds since Unix epoch.
fn millis_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LeaderElectionConfig;
    use crate::task_store::SqliteTaskStore;

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    struct TestExtraState {
        count: u32,
        name: String,
    }

    fn test_mode_b_leader() -> ModeBOpLeader<TestExtraState> {
        let store = Arc::new(SqliteTaskStore::open_in_memory().unwrap());
        store.migrate().unwrap();

        let config = LeaderElectionConfig {
            enabled: true,
            lease_ttl_s: 10,
            renew_interval_s: 3,
        };

        let leader_election = Arc::new(LeaderElection::new(
            store.clone(),
            "test-pod".to_string(),
            config,
        ));

        ModeBOpLeader::new(
            leader_election,
            store,
            mode_b_type::RESHARD.to_string(),
            "reshard:test-index".to_string(),
            "test-pod".to_string(),
            TestExtraState::default(),
        )
    }

    #[tokio::test]
    async fn test_acquire_leadership() {
        let mut leader = test_mode_b_leader();
        assert!(leader.try_acquire_leadership().await.unwrap());
        assert!(leader.is_leader());
        assert_eq!(leader.phase(), "idle");
    }

    #[tokio::test]
    async fn test_persist_phase() {
        let mut leader = test_mode_b_leader();
        leader.try_acquire_leadership().await.unwrap();

        leader.persist_phase("shadow_created".to_string()).await.unwrap();
        assert_eq!(leader.phase(), "shadow_created");

        // Verify persistence
        let recovered = leader.task_store.get_mode_b_operation_by_scope("reshard:test-index").unwrap();
        assert!(recovered.is_some());
        let recovered = recovered.unwrap();
        assert_eq!(recovered.phase, "shadow_created");
    }

    #[tokio::test]
    async fn test_recover_state() {
        // Create a shared store for both leader instances
        let store = Arc::new(SqliteTaskStore::open_in_memory().unwrap());
        store.migrate().unwrap();

        let config = LeaderElectionConfig {
            enabled: true,
            lease_ttl_s: 10,
            renew_interval_s: 3,
        };

        // Create first leader instance
        let leader_election1 = Arc::new(LeaderElection::new(
            store.clone(),
            "test-pod".to_string(),
            config.clone(),
        ));
        let mut leader = ModeBOpLeader::new(
            leader_election1,
            store.clone(),
            mode_b_type::RESHARD.to_string(),
            "reshard:test-index".to_string(),
            "test-pod".to_string(),
            TestExtraState::default(),
        );
        leader.try_acquire_leadership().await.unwrap();

        // Set some extra state
        leader.extra_state().count = 42;
        leader.extra_state().name = "test".to_string();

        // Persist a phase
        leader.persist_phase("backfill_in_progress".to_string()).await.unwrap();

        // Create a new leader instance (simulating pod restart)
        let leader_election2 = Arc::new(LeaderElection::new(
            store.clone(),
            "test-pod".to_string(),
            config,
        ));
        let mut leader2 = ModeBOpLeader::new(
            leader_election2,
            store,
            mode_b_type::RESHARD.to_string(),
            "reshard:test-index".to_string(),
            "test-pod".to_string(),
            TestExtraState::default(),
        );
        leader2.try_acquire_leadership().await.unwrap();

        // Recover state
        let recovered = leader2.recover().await.unwrap();
        assert!(recovered.is_some());

        // Verify phase state
        assert_eq!(leader2.phase(), "backfill_in_progress");

        // Verify extra state
        assert_eq!(leader2.extra_state_ref().count, 42);
        assert_eq!(leader2.extra_state_ref().name, "test");
    }

    #[tokio::test]
    async fn test_fail_operation() {
        let mut leader = test_mode_b_leader();
        leader.try_acquire_leadership().await.unwrap();

        leader.fail("test error".to_string()).await.unwrap();

        // Verify status is failed
        let recovered = leader.task_store.get_mode_b_operation_by_scope("reshard:test-index").unwrap();
        assert!(recovered.is_some());
        let recovered = recovered.unwrap();
        assert_eq!(recovered.status, mode_b_status::FAILED);
        assert_eq!(recovered.error, Some("test error".to_string()));

        // Should have stepped down from leadership
        assert!(!leader.is_leader());
    }

    #[tokio::test]
    async fn test_complete_operation() {
        let mut leader = test_mode_b_leader();
        leader.try_acquire_leadership().await.unwrap();

        leader.complete().await.unwrap();

        // Verify status is completed
        let recovered = leader.task_store.get_mode_b_operation_by_scope("reshard:test-index").unwrap();
        assert!(recovered.is_some());
        let recovered = recovered.unwrap();
        assert_eq!(recovered.status, mode_b_status::COMPLETED);
        assert_eq!(recovered.phase, "complete");

        // Should have stepped down from leadership
        assert!(!leader.is_leader());
    }

    #[tokio::test]
    async fn test_phase_state_transitions() {
        let mut phase = PhaseState::new("idle".to_string());
        assert_eq!(phase.phase, "idle");
        assert!(phase.error.is_none());

        phase.advance("shadow_created".to_string());
        assert_eq!(phase.phase, "shadow_created");

        phase.fail("test error".to_string());
        assert_eq!(phase.error, Some("test error".to_string()));
    }
}
