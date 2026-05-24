//! Replica group addition state machine.
//!
//! Implements the group addition flow from plan §2:
//! 1. Provision new nodes; assign replica_group: G_new in config
//! 2. Mark new group initializing; queries NOT routed here
//! 3. Background sync: for each shard, copy all docs from any healthy existing group
//! 4. When all shards synced, mark group active — queries begin routing in round-robin
//! 5. Existing groups continue serving queries throughout (zero read interruption)

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::migration::ShardId;

/// Unique identifier for a group addition operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupAdditionId(pub u64);

impl fmt::Display for GroupAdditionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Phase of group addition process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupAdditionPhase {
    /// Initial phase: group provisioned, waiting for sync to start.
    Initializing,
    /// Background sync in progress: copying docs from existing groups.
    Syncing,
    /// All shards synced, ready to mark group active.
    SyncComplete,
    /// Group is active and serving queries.
    Active,
    /// Group addition failed.
    Failed(String),
}

impl fmt::Display for GroupAdditionPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Initializing => write!(f, "initializing"),
            Self::Syncing => write!(f, "syncing"),
            Self::SyncComplete => write!(f, "sync_complete"),
            Self::Active => write!(f, "active"),
            Self::Failed(msg) => write!(f, "failed({msg})"),
        }
    }
}

/// Per-shard sync state within a group addition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardSyncState {
    /// Waiting for sync to begin.
    Pending,
    /// Syncing documents from source group.
    Syncing { docs_copied: u64, source_group: u32 },
    /// Sync complete for this shard.
    Complete { docs_copied: u64, source_group: u32 },
    /// Sync failed for this shard.
    Failed { reason: String, source_group: u32 },
}

impl fmt::Display for ShardSyncState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Syncing {
                docs_copied,
                source_group,
            } => {
                write!(f, "syncing({docs_copied} copied from group {source_group})")
            }
            Self::Complete {
                docs_copied,
                source_group,
            } => {
                write!(
                    f,
                    "complete({docs_copied} copied from group {source_group})"
                )
            }
            Self::Failed {
                reason,
                source_group,
            } => {
                write!(f, "failed({reason}, from group {source_group})")
            }
        }
    }
}

/// Configuration for group addition behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupAdditionConfig {
    /// Maximum time to wait for sync to complete before failing.
    pub sync_timeout: Duration,
    /// Page size for document pagination during sync.
    pub sync_page_size: u32,
    /// Maximum concurrent shard syncs.
    pub max_concurrent_syncs: usize,
}

impl Default for GroupAdditionConfig {
    fn default() -> Self {
        Self {
            sync_timeout: Duration::from_secs(3600), // 1 hour
            sync_page_size: 1000,
            max_concurrent_syncs: 4,
        }
    }
}

/// Error type for group addition operations.
#[derive(Debug, thiserror::Error)]
pub enum GroupAdditionError {
    #[error("group {0} not found")]
    GroupNotFound(u32),
    #[error("shard {0} sync is not in a valid state for this transition (current: {1})")]
    InvalidTransition(ShardId, String),
    #[error("sync timeout exceeded for group {0}")]
    SyncTimeout(u32),
    #[error("no healthy source groups available for sync")]
    NoHealthySourceGroups,
    #[error("group addition {0} not found")]
    NotFound(GroupAdditionId),
    #[error("group {0} is not in initializing state")]
    GroupNotInitializing(u32),
    #[error("sync verification failed: new group has {new} docs, source has {source_docs} docs (variance: {variance}%)")]
    VerificationFailed {
        new: u64,
        source_docs: u64,
        variance: f64,
    },
}

/// Tracks the state of a group addition operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupAdditionState {
    pub id: GroupAdditionId,
    pub group_id: u32,
    pub phase: GroupAdditionPhase,
    /// Per-shard sync state.
    pub shard_states: HashMap<ShardId, ShardSyncState>,
    /// Source group selected for each shard (round-robin across active groups).
    pub shard_sources: HashMap<ShardId, u32>,
    #[serde(skip)]
    pub started_at: Option<Instant>,
    #[serde(skip)]
    pub completed_at: Option<Instant>,
}

/// The group addition coordinator manages replica group addition state transitions.
pub struct GroupAdditionCoordinator {
    config: GroupAdditionConfig,
    additions: HashMap<GroupAdditionId, GroupAdditionState>,
    next_id: u64,
}

impl GroupAdditionCoordinator {
    pub fn new(config: GroupAdditionConfig) -> Self {
        Self {
            config,
            additions: HashMap::new(),
            next_id: 0,
        }
    }

    /// Begin a new group addition operation.
    pub fn begin_addition(
        &mut self,
        group_id: u32,
        shard_count: u32,
        source_groups: &[u32],
    ) -> Result<GroupAdditionId, GroupAdditionError> {
        if source_groups.is_empty() {
            return Err(GroupAdditionError::NoHealthySourceGroups);
        }

        let id = GroupAdditionId(self.next_id);
        self.next_id += 1;

        // Assign source group for each shard using round-robin
        let mut shard_states = HashMap::new();
        let mut shard_sources = HashMap::new();

        for shard_id in 0..shard_count {
            let source_idx = shard_id as usize % source_groups.len();
            let source_group = source_groups[source_idx];
            shard_states.insert(ShardId(shard_id), ShardSyncState::Pending);
            shard_sources.insert(ShardId(shard_id), source_group);
        }

        let state = GroupAdditionState {
            id,
            group_id,
            phase: GroupAdditionPhase::Initializing,
            shard_states,
            shard_sources,
            started_at: Some(Instant::now()),
            completed_at: None,
        };

        self.additions.insert(id, state);
        Ok(id)
    }

    /// Start background sync for a group addition.
    pub fn begin_sync(&mut self, id: GroupAdditionId) -> Result<(), GroupAdditionError> {
        let state = self
            .additions
            .get_mut(&id)
            .ok_or(GroupAdditionError::NotFound(id))?;

        if !matches!(state.phase, GroupAdditionPhase::Initializing) {
            return Err(GroupAdditionError::InvalidTransition(
                ShardId(0),
                format!("expected Initializing, got {}", state.phase),
            ));
        }

        state.phase = GroupAdditionPhase::Syncing;

        // Mark all shards as syncing
        for (shard_id, &source_group) in &state.shard_sources {
            if let Some(shard_state) = state.shard_states.get_mut(shard_id) {
                *shard_state = ShardSyncState::Syncing {
                    docs_copied: 0,
                    source_group,
                };
            }
        }

        Ok(())
    }

    /// Record progress for a shard sync.
    pub fn shard_sync_progress(
        &mut self,
        id: GroupAdditionId,
        shard: ShardId,
        docs_copied: u64,
    ) -> Result<(), GroupAdditionError> {
        let state = self
            .additions
            .get_mut(&id)
            .ok_or(GroupAdditionError::NotFound(id))?;

        let shard_state = state.shard_states.get_mut(&shard).ok_or_else(|| {
            GroupAdditionError::InvalidTransition(shard, "shard not in addition".into())
        })?;

        match shard_state {
            ShardSyncState::Syncing { docs_copied: d, .. } => {
                *d = docs_copied;
            }
            _ => {
                return Err(GroupAdditionError::InvalidTransition(
                    shard,
                    shard_state.to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Mark a shard sync as complete.
    pub fn shard_sync_complete(
        &mut self,
        id: GroupAdditionId,
        shard: ShardId,
        docs_copied: u64,
    ) -> Result<(), GroupAdditionError> {
        let state = self
            .additions
            .get_mut(&id)
            .ok_or(GroupAdditionError::NotFound(id))?;

        let source_group = *state.shard_sources.get(&shard).ok_or_else(|| {
            GroupAdditionError::InvalidTransition(shard, "no source group".into())
        })?;

        let shard_state = state.shard_states.get_mut(&shard).ok_or_else(|| {
            GroupAdditionError::InvalidTransition(shard, "shard not in addition".into())
        })?;

        match shard_state {
            ShardSyncState::Syncing { .. } => {
                *shard_state = ShardSyncState::Complete {
                    docs_copied,
                    source_group,
                };
            }
            _ => {
                return Err(GroupAdditionError::InvalidTransition(
                    shard,
                    shard_state.to_string(),
                ));
            }
        }

        // Check if all shards are done syncing
        let all_complete = state
            .shard_states
            .values()
            .all(|s| matches!(s, ShardSyncState::Complete { .. }));

        if all_complete {
            state.phase = GroupAdditionPhase::SyncComplete;
        }

        Ok(())
    }

    /// Mark a shard sync as failed.
    pub fn shard_sync_failed(
        &mut self,
        id: GroupAdditionId,
        shard: ShardId,
        reason: String,
    ) -> Result<(), GroupAdditionError> {
        let state = self
            .additions
            .get_mut(&id)
            .ok_or(GroupAdditionError::NotFound(id))?;

        let source_group = *state.shard_sources.get(&shard).ok_or_else(|| {
            GroupAdditionError::InvalidTransition(shard, "no source group".into())
        })?;

        let shard_state = state.shard_states.get_mut(&shard).ok_or_else(|| {
            GroupAdditionError::InvalidTransition(shard, "shard not in addition".into())
        })?;

        *shard_state = ShardSyncState::Failed {
            reason,
            source_group,
        };

        Ok(())
    }

    /// Mark the group as active after sync is complete and verified.
    pub fn mark_group_active(&mut self, id: GroupAdditionId) -> Result<(), GroupAdditionError> {
        let state = self
            .additions
            .get_mut(&id)
            .ok_or(GroupAdditionError::NotFound(id))?;

        if !matches!(state.phase, GroupAdditionPhase::SyncComplete) {
            return Err(GroupAdditionError::InvalidTransition(
                ShardId(0),
                format!("expected SyncComplete, got {}", state.phase),
            ));
        }

        state.phase = GroupAdditionPhase::Active;
        state.completed_at = Some(Instant::now());

        Ok(())
    }

    /// Fail a group addition operation.
    pub fn fail_addition(
        &mut self,
        id: GroupAdditionId,
        reason: String,
    ) -> Result<(), GroupAdditionError> {
        let state = self
            .additions
            .get_mut(&id)
            .ok_or(GroupAdditionError::NotFound(id))?;

        state.phase = GroupAdditionPhase::Failed(reason);
        state.completed_at = Some(Instant::now());

        Ok(())
    }

    /// Get the current state of a group addition.
    pub fn get_state(&self, id: GroupAdditionId) -> Option<&GroupAdditionState> {
        self.additions.get(&id)
    }

    /// Get the current state of a group addition (mutable).
    pub fn get_state_mut(&mut self, id: GroupAdditionId) -> Option<&mut GroupAdditionState> {
        self.additions.get_mut(&id)
    }

    /// Get all group addition states.
    pub fn get_all_additions(&self) -> &HashMap<GroupAdditionId, GroupAdditionState> {
        &self.additions
    }

    /// Get the group addition config.
    pub fn config(&self) -> &GroupAdditionConfig {
        &self.config
    }

    /// Check if a group is currently being added (in progress).
    pub fn is_group_adding(&self, group_id: u32) -> bool {
        self.additions.values().any(|a| {
            a.group_id == group_id
                && matches!(
                    a.phase,
                    GroupAdditionPhase::Initializing | GroupAdditionPhase::Syncing
                )
        })
    }

    /// Calculate sync progress percentage for a group addition.
    pub fn sync_progress(&self, id: GroupAdditionId) -> Option<f64> {
        let state = self.additions.get(&id)?;
        if state.shard_states.is_empty() {
            return Some(0.0);
        }

        let total_shards = state.shard_states.len();
        let complete_shards = state
            .shard_states
            .values()
            .filter(|s| matches!(s, ShardSyncState::Complete { .. }))
            .count();

        Some((complete_shards as f64 / total_shards as f64) * 100.0)
    }

    /// Get the source group for a shard.
    pub fn get_shard_source(&self, id: GroupAdditionId, shard: ShardId) -> Option<u32> {
        let state = self.additions.get(&id)?;
        state.shard_sources.get(&shard).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(id: u32) -> ShardId {
        ShardId(id)
    }

    #[test]
    fn test_begin_addition() {
        let config = GroupAdditionConfig::default();
        let mut coord = GroupAdditionCoordinator::new(config);

        let source_groups = vec![0, 1];
        let id = coord.begin_addition(2, 64, &source_groups).unwrap();

        let state = coord.get_state(id).unwrap();
        assert_eq!(state.group_id, 2);
        assert_eq!(state.phase, GroupAdditionPhase::Initializing);
        assert_eq!(state.shard_states.len(), 64);

        // Check source groups are round-robin assigned
        assert_eq!(coord.get_shard_source(id, shard(0)), Some(0));
        assert_eq!(coord.get_shard_source(id, shard(1)), Some(1));
        assert_eq!(coord.get_shard_source(id, shard(2)), Some(0));
    }

    #[test]
    fn test_begin_addition_no_source_groups() {
        let config = GroupAdditionConfig::default();
        let mut coord = GroupAdditionCoordinator::new(config);

        let result = coord.begin_addition(1, 64, &[]);
        assert!(matches!(
            result,
            Err(GroupAdditionError::NoHealthySourceGroups)
        ));
    }

    #[test]
    fn test_begin_sync() {
        let config = GroupAdditionConfig::default();
        let mut coord = GroupAdditionCoordinator::new(config);

        let id = coord.begin_addition(1, 8, &[0]).unwrap();
        coord.begin_sync(id).unwrap();

        let state = coord.get_state(id).unwrap();
        assert_eq!(state.phase, GroupAdditionPhase::Syncing);

        // All shards should be in syncing state
        for shard_state in state.shard_states.values() {
            assert!(matches!(shard_state, ShardSyncState::Syncing { .. }));
        }
    }

    #[test]
    fn test_shard_sync_progress() {
        let config = GroupAdditionConfig::default();
        let mut coord = GroupAdditionCoordinator::new(config);

        let id = coord.begin_addition(1, 8, &[0]).unwrap();
        coord.begin_sync(id).unwrap();

        coord.shard_sync_progress(id, shard(0), 100).unwrap();
        coord.shard_sync_progress(id, shard(1), 200).unwrap();

        let state = coord.get_state(id).unwrap();
        if let ShardSyncState::Syncing { docs_copied, .. } =
            state.shard_states.get(&shard(0)).unwrap()
        {
            assert_eq!(*docs_copied, 100);
        } else {
            panic!("Expected Syncing state");
        }
    }

    #[test]
    fn test_shard_sync_complete() {
        let config = GroupAdditionConfig::default();
        let mut coord = GroupAdditionCoordinator::new(config);

        let id = coord.begin_addition(1, 4, &[0, 1]).unwrap();
        coord.begin_sync(id).unwrap();

        // Complete 3 out of 4 shards
        coord.shard_sync_complete(id, shard(0), 500).unwrap();
        coord.shard_sync_complete(id, shard(1), 600).unwrap();
        coord.shard_sync_complete(id, shard(2), 400).unwrap();

        let state = coord.get_state(id).unwrap();
        // Phase should still be syncing (not all complete)
        assert_eq!(state.phase, GroupAdditionPhase::Syncing);

        // Complete the last shard
        coord.shard_sync_complete(id, shard(3), 550).unwrap();

        let state = coord.get_state(id).unwrap();
        // Phase should now be sync_complete
        assert_eq!(state.phase, GroupAdditionPhase::SyncComplete);
    }

    #[test]
    fn test_mark_group_active() {
        let config = GroupAdditionConfig::default();
        let mut coord = GroupAdditionCoordinator::new(config);

        let id = coord.begin_addition(1, 4, &[0]).unwrap();
        coord.begin_sync(id).unwrap();

        // Complete all shards
        for s in 0..4 {
            coord.shard_sync_complete(id, shard(s), 100).unwrap();
        }

        coord.mark_group_active(id).unwrap();

        let state = coord.get_state(id).unwrap();
        assert_eq!(state.phase, GroupAdditionPhase::Active);
        assert!(state.completed_at.is_some());
    }

    #[test]
    fn test_mark_group_active_before_sync_complete() {
        let config = GroupAdditionConfig::default();
        let mut coord = GroupAdditionCoordinator::new(config);

        let id = coord.begin_addition(1, 4, &[0]).unwrap();
        coord.begin_sync(id).unwrap();

        // Try to mark active before sync is complete
        let result = coord.mark_group_active(id);
        assert!(matches!(
            result,
            Err(GroupAdditionError::InvalidTransition(_, _))
        ));
    }

    #[test]
    fn test_shard_sync_failed() {
        let config = GroupAdditionConfig::default();
        let mut coord = GroupAdditionCoordinator::new(config);

        let id = coord.begin_addition(1, 4, &[0]).unwrap();
        coord.begin_sync(id).unwrap();

        coord
            .shard_sync_failed(id, shard(1), "source unavailable".to_string())
            .unwrap();

        let state = coord.get_state(id).unwrap();
        if let ShardSyncState::Failed { reason, .. } = state.shard_states.get(&shard(1)).unwrap() {
            assert_eq!(reason, "source unavailable");
        } else {
            panic!("Expected Failed state");
        }
    }

    #[test]
    fn test_fail_addition() {
        let config = GroupAdditionConfig::default();
        let mut coord = GroupAdditionCoordinator::new(config);

        let id = coord.begin_addition(1, 4, &[0]).unwrap();

        coord
            .fail_addition(id, "out of memory".to_string())
            .unwrap();

        let state = coord.get_state(id).unwrap();
        assert_eq!(
            state.phase,
            GroupAdditionPhase::Failed("out of memory".to_string())
        );
        assert!(state.completed_at.is_some());
    }

    #[test]
    fn test_is_group_adding() {
        let config = GroupAdditionConfig::default();
        let mut coord = GroupAdditionCoordinator::new(config);

        let id = coord.begin_addition(2, 4, &[0]).unwrap();

        // Group 2 should be adding
        assert!(coord.is_group_adding(2));

        // After starting sync, still adding
        coord.begin_sync(id).unwrap();
        assert!(coord.is_group_adding(2));

        // After marking complete, no longer adding
        for s in 0..4 {
            coord.shard_sync_complete(id, shard(s), 100).unwrap();
        }
        coord.mark_group_active(id).unwrap();
        assert!(!coord.is_group_adding(2));
    }

    #[test]
    fn test_sync_progress() {
        let config = GroupAdditionConfig::default();
        let mut coord = GroupAdditionCoordinator::new(config);

        let id = coord.begin_addition(1, 10, &[0]).unwrap();
        coord.begin_sync(id).unwrap();

        assert_eq!(coord.sync_progress(id), Some(0.0));

        // Complete 5 out of 10 shards
        for s in 0..5 {
            coord.shard_sync_complete(id, shard(s), 100).unwrap();
        }

        assert_eq!(coord.sync_progress(id), Some(50.0));

        // Complete remaining shards
        for s in 5..10 {
            coord.shard_sync_complete(id, shard(s), 100).unwrap();
        }

        assert_eq!(coord.sync_progress(id), Some(100.0));
    }

    #[test]
    fn test_display_impls() {
        assert_eq!(format!("{}", GroupAdditionId(42)), "42");
        assert_eq!(
            format!("{}", GroupAdditionPhase::Initializing),
            "initializing"
        );
        assert_eq!(format!("{}", GroupAdditionPhase::Syncing), "syncing");
        assert_eq!(
            format!("{}", GroupAdditionPhase::SyncComplete),
            "sync_complete"
        );
        assert_eq!(format!("{}", GroupAdditionPhase::Active), "active");
        assert_eq!(
            format!("{}", GroupAdditionPhase::Failed("oops".into())),
            "failed(oops)"
        );

        assert_eq!(format!("{}", ShardSyncState::Pending), "pending");
        assert_eq!(
            format!(
                "{}",
                ShardSyncState::Syncing {
                    docs_copied: 100,
                    source_group: 0
                }
            ),
            "syncing(100 copied from group 0)"
        );
        assert_eq!(
            format!(
                "{}",
                ShardSyncState::Complete {
                    docs_copied: 500,
                    source_group: 1
                }
            ),
            "complete(500 copied from group 1)"
        );
        assert_eq!(
            format!(
                "{}",
                ShardSyncState::Failed {
                    reason: "timeout".into(),
                    source_group: 0
                }
            ),
            "failed(timeout, from group 0)"
        );
    }

    #[test]
    fn test_invalid_transitions() {
        let config = GroupAdditionConfig::default();
        let mut coord = GroupAdditionCoordinator::new(config);

        let id = coord.begin_addition(1, 4, &[0]).unwrap();

        // shard_sync_progress before syncing should fail
        let err = coord.shard_sync_progress(id, shard(0), 100).unwrap_err();
        assert!(matches!(err, GroupAdditionError::InvalidTransition(_, _)));

        // NotFound for invalid addition
        let err = coord.begin_sync(GroupAdditionId(999)).unwrap_err();
        assert!(matches!(err, GroupAdditionError::NotFound(_)));
    }

    #[test]
    fn test_round_robin_source_assignment() {
        let config = GroupAdditionConfig::default();
        let mut coord = GroupAdditionCoordinator::new(config);

        let source_groups = vec![0, 1, 2];
        let id = coord.begin_addition(3, 10, &source_groups).unwrap();

        // Verify round-robin pattern
        assert_eq!(coord.get_shard_source(id, shard(0)), Some(0));
        assert_eq!(coord.get_shard_source(id, shard(1)), Some(1));
        assert_eq!(coord.get_shard_source(id, shard(2)), Some(2));
        assert_eq!(coord.get_shard_source(id, shard(3)), Some(0));
        assert_eq!(coord.get_shard_source(id, shard(4)), Some(1));
        assert_eq!(coord.get_shard_source(id, shard(5)), Some(2));
    }
}
