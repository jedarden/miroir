//! Scoped-key rotation coordinator (plan §13.21).
//!
//! Manages the rotation of scoped encryption keys for the search UI.
//! Uses leader-only singleton coordination (plan §14.5) to ensure only one pod
//! performs key rotation for a given index at a time.

use crate::error::{MiroirError, Result};
use crate::leader_election::LeaderElection;
use crate::mode_b_coordinator::ModeBOpLeader;
use crate::task_store::TaskStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// Scoped-key rotation coordinator with leader-only singleton coordination (plan §14.5).
///
/// Acquires a per-index leader lease (scope: "search_ui_key_rotation:<index>") and persists
/// phase state so that a new leader can resume from the last committed phase.
pub struct ScopedKeyRotationCoordinator {
    /// Mode B operation leader with phase state persistence.
    leader: ModeBOpLeader<ScopedKeyRotationExtraState>,
}

/// Extra state for scoped-key rotation operations persisted to mode_b_operations.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ScopedKeyRotationExtraState {
    /// Index UID for this rotation.
    pub index_uid: String,
    /// Old key hash (SHA256 of the old key).
    pub old_key_hash: Option<String>,
    /// New key hash (SHA256 of the new key).
    pub new_key_hash: String,
    /// Distribution progress (node_id -> received new key).
    pub distribution_progress: HashMap<String, bool>,
    /// Drain progress in seconds (how long we've been draining the old key).
    pub drain_progress_s: u64,
    /// Target drain duration in seconds.
    pub drain_target_s: u64,
}

/// Phases of the scoped-key rotation process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum RotationPhase {
    Idle = 0,
    GeneratingNewKey = 1,
    DistributingNewKey = 2,
    DrainingOldKey = 3,
    CleaningUp = 4,
    Complete = 5,
    Failed = 6,
}

impl RotationPhase {
    /// Get the phase name as a string.
    pub fn name(&self) -> &str {
        match self {
            RotationPhase::Idle => "idle",
            RotationPhase::GeneratingNewKey => "generating_new_key",
            RotationPhase::DistributingNewKey => "distributing_new_key",
            RotationPhase::DrainingOldKey => "draining_old_key",
            RotationPhase::CleaningUp => "cleaning_up",
            RotationPhase::Complete => "complete",
            RotationPhase::Failed => "failed",
        }
    }

    /// Parse a phase name string into a RotationPhase.
    pub fn from_name(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "idle" => RotationPhase::Idle,
            "generating_new_key" => RotationPhase::GeneratingNewKey,
            "distributing_new_key" => RotationPhase::DistributingNewKey,
            "draining_old_key" => RotationPhase::DrainingOldKey,
            "cleaning_up" => RotationPhase::CleaningUp,
            "complete" => RotationPhase::Complete,
            "failed" => RotationPhase::Failed,
            _ => {
                warn!("unknown rotation phase '{}', defaulting to Idle", name);
                RotationPhase::Idle
            }
        }
    }
}

impl ScopedKeyRotationCoordinator {
    /// Create a new scoped-key rotation coordinator.
    pub fn new(
        leader_election: Arc<LeaderElection>,
        task_store: Arc<dyn TaskStore>,
        index_uid: String,
        new_key_hash: String,
        drain_target_s: u64,
        pod_id: String,
    ) -> Self {
        let scope = format!("search_ui_key_rotation:{}", index_uid);

        let extra_state = ScopedKeyRotationExtraState {
            index_uid,
            old_key_hash: None,
            new_key_hash,
            distribution_progress: HashMap::new(),
            drain_progress_s: 0,
            drain_target_s,
        };

        let leader = ModeBOpLeader::new(
            leader_election,
            task_store,
            crate::task_store::mode_b_type::SCOPED_KEY_ROTATION.to_string(),
            scope,
            pod_id,
            extra_state,
        );

        Self { leader }
    }

    /// Try to acquire leadership for this key rotation.
    ///
    /// Returns `Ok(true)` if we are now the leader, `Ok(false)` if another
    /// pod holds the lease, or `Err` if acquisition failed.
    pub async fn try_acquire_leadership(&mut self) -> Result<bool> {
        self.leader.try_acquire_leadership().await
    }

    /// Renew the leader lease.
    ///
    /// Returns `Ok(true)` if renewed successfully, `Ok(false)` if we lost
    /// leadership to another pod, or `Err` if renewal failed.
    pub async fn renew_leadership(&mut self) -> Result<bool> {
        self.leader.renew_leadership().await
    }

    /// Check if we are currently the leader.
    pub fn is_leader(&self) -> bool {
        self.leader.is_leader()
    }

    /// Get the current phase.
    pub fn phase(&self) -> &str {
        self.leader.phase()
    }

    /// Get the extra state (mutable).
    pub fn extra_state(&mut self) -> &mut ScopedKeyRotationExtraState {
        self.leader.extra_state()
    }

    /// Get the extra state (immutable).
    pub fn extra_state_ref(&self) -> &ScopedKeyRotationExtraState {
        self.leader.extra_state_ref()
    }

    /// Advance to the next phase and persist state.
    ///
    /// Should be called after each phase boundary so that a new leader can
    /// resume from the last committed phase.
    pub async fn advance_phase(&mut self, new_phase: RotationPhase) -> Result<()> {
        let phase_name = new_phase.name().to_string();
        self.leader.persist_phase(phase_name).await
    }

    /// Set the old key hash and advance to generating phase.
    pub async fn start_rotation(&mut self, old_key_hash: String) -> Result<()> {
        self.leader.extra_state().old_key_hash = Some(old_key_hash);
        self.leader
            .persist_phase(RotationPhase::GeneratingNewKey.name().to_string())
            .await
    }

    /// Update distribution progress for a node.
    pub async fn update_distribution_progress(
        &mut self,
        node_id: String,
        received: bool,
    ) -> Result<()> {
        self.leader
            .extra_state()
            .distribution_progress
            .insert(node_id, received);
        self.leader
            .persist_phase(RotationPhase::DistributingNewKey.name().to_string())
            .await
    }

    /// Check if all nodes have received the new key.
    pub fn distribution_complete(&self) -> bool {
        self.leader
            .extra_state_ref()
            .distribution_progress
            .values()
            .all(|&v| v)
    }

    /// Update drain progress and persist.
    pub async fn update_drain_progress(&mut self, progress_s: u64) -> Result<()> {
        self.leader.extra_state().drain_progress_s = progress_s;
        self.leader
            .persist_phase(RotationPhase::DrainingOldKey.name().to_string())
            .await
    }

    /// Check if drain is complete.
    pub fn drain_complete(&self) -> bool {
        self.leader.extra_state_ref().drain_progress_s
            >= self.leader.extra_state_ref().drain_target_s
    }

    /// Mark the operation as failed and step down from leadership.
    pub async fn fail(&mut self, error: String) -> Result<()> {
        self.leader
            .persist_phase(RotationPhase::Failed.name().to_string())
            .await?;
        self.leader.fail(error).await
    }

    /// Mark the operation as completed and step down from leadership.
    pub async fn complete(&mut self) -> Result<()> {
        self.leader.complete().await
    }

    /// Recover the operation state from the task store.
    ///
    /// Called by a new leader to read the persisted phase state and resume
    /// from the last committed phase boundary.
    pub async fn recover(&mut self) -> Result<Option<RotationPhase>> {
        let existing = self.leader.recover().await?;

        if let Some(ref op) = existing {
            // Parse phase string back to RotationPhase enum
            let phase = RotationPhase::from_name(&op.phase);

            info!(
                index_uid = %self.leader.extra_state_ref().index_uid,
                phase = %op.phase,
                "recovered scoped-key rotation from persisted phase"
            );

            return Ok(Some(phase));
        }

        Ok(None)
    }

    /// Delete the operation state after completion.
    pub async fn delete(&self) -> Result<bool> {
        self.leader.delete().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LeaderElectionConfig;
    use crate::task_store::SqliteTaskStore;

    #[test]
    fn test_rotation_phase_names() {
        assert_eq!(RotationPhase::Idle.name(), "idle");
        assert_eq!(RotationPhase::GeneratingNewKey.name(), "generating_new_key");
        assert_eq!(
            RotationPhase::DistributingNewKey.name(),
            "distributing_new_key"
        );
        assert_eq!(RotationPhase::DrainingOldKey.name(), "draining_old_key");
        assert_eq!(RotationPhase::CleaningUp.name(), "cleaning_up");
        assert_eq!(RotationPhase::Complete.name(), "complete");
        assert_eq!(RotationPhase::Failed.name(), "failed");
    }

    #[test]
    fn test_rotation_phase_from_name() {
        assert_eq!(RotationPhase::from_name("idle"), RotationPhase::Idle);
        assert_eq!(
            RotationPhase::from_name("GENERATING_NEW_KEY"),
            RotationPhase::GeneratingNewKey
        );
        assert_eq!(
            RotationPhase::from_name("distributing_new_key"),
            RotationPhase::DistributingNewKey
        );
        assert_eq!(
            RotationPhase::from_name("draining_old_key"),
            RotationPhase::DrainingOldKey
        );
        assert_eq!(
            RotationPhase::from_name("cleaning_up"),
            RotationPhase::CleaningUp
        );
        assert_eq!(
            RotationPhase::from_name("complete"),
            RotationPhase::Complete
        );
        assert_eq!(RotationPhase::from_name("failed"), RotationPhase::Failed);
    }

    #[tokio::test]
    async fn test_scoped_key_rotation_coordinator() {
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

        let mut coordinator = ScopedKeyRotationCoordinator::new(
            leader_election,
            store,
            "test-index".to_string(),
            "new_key_hash".to_string(),
            120,
            "test-pod".to_string(),
        );

        // Try to acquire leadership
        assert!(coordinator.try_acquire_leadership().await.unwrap());
        assert!(coordinator.is_leader());

        // Start rotation
        coordinator
            .start_rotation("old_key_hash".to_string())
            .await
            .unwrap();
        assert_eq!(coordinator.phase(), RotationPhase::GeneratingNewKey.name());

        // Update distribution progress
        coordinator
            .update_distribution_progress("node-1".to_string(), true)
            .await
            .unwrap();
        coordinator
            .update_distribution_progress("node-2".to_string(), true)
            .await
            .unwrap();

        // Check distribution complete
        assert!(coordinator.distribution_complete());

        // Update drain progress
        coordinator.update_drain_progress(120).await.unwrap();
        assert!(coordinator.drain_complete());

        // Complete
        coordinator.complete().await.unwrap();
    }
}
