//! Shard migration cutover state machine.
//!
//! Implements the node-addition migration flow from plan §4 with explicit state
//! transitions and a race-window-safe cutover sequence.
//!
//! ## Race window analysis (plan §15 OP#1)
//!
//! The dangerous window is between "mark node active" (routing changes to new-node-only)
//! and "delete migrated shard from old node." A document written during dual-write that
//! succeeded on OLD but failed on NEW — and arrived after the last migration page —
//! would be deleted from OLD without ever reaching NEW.
//!
//! ## Solution: quiesce-then-verify cutover
//!
//! Instead of the naïve sequence (mark active → stop dual-write → delete old), we use:
//!
//! 1. Stop dual-write (no new writes go to either node for affected shards)
//! 2. Drain: wait for all in-flight writes to both OLD and NEW to complete
//! 3. Delta migration: re-read affected shards from OLD (catches anything written since
//!    the last migration page) and write deltas to NEW
//! 4. Mark node active (routing switches to NEW-only)
//! 5. Delete migrated shard from OLD
//!
//! Step 3 is the key: it closes the race window by ensuring NEW has a complete picture
//! before we commit the routing change. The cost is one extra pagination pass over each
//! migrated shard — bounded by the number of docs written during the migration window.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Unique identifier for a shard migration operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MigrationId(pub u64);

impl fmt::Display for MigrationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifier for a physical node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

// Type alias for external use (rebalancer, etc.)
pub type MigrationNodeId = NodeId;

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifier for a logical shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardId(pub u32);

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s{}", self.0)
    }
}

/// Per-shard migration state within a node-addition migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardMigrationState {
    /// Waiting for background migration to begin.
    Pending,
    /// Background pagination is reading docs from source and writing to target.
    Migrating {
        docs_copied: u64,
        pages_remaining: u32,
    },
    /// Background migration complete, awaiting cutover.
    MigrationComplete { docs_copied: u64 },
    /// Dual-write stopped, in-flight writes draining.
    Draining {
        in_flight_count: u32,
        docs_copied: u64,
    },
    /// Delta pass: re-reading source to catch stragglers written during migration.
    DeltaPass {
        docs_copied: u64,
        delta_docs_copied: u64,
    },
    /// Node is active for this shard; old replica data deleted.
    Active,
    /// Migration failed at this phase.
    Failed { phase: String, reason: String },
}

impl fmt::Display for ShardMigrationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Migrating {
                docs_copied,
                pages_remaining,
            } => {
                write!(
                    f,
                    "migrating({docs_copied} copied, {pages_remaining} pages left)"
                )
            }
            Self::MigrationComplete { docs_copied } => {
                write!(f, "migration_complete({docs_copied} copied)")
            }
            Self::Draining {
                in_flight_count,
                docs_copied,
            } => {
                write!(
                    f,
                    "draining({in_flight_count} in-flight, {docs_copied} copied)"
                )
            }
            Self::DeltaPass {
                docs_copied,
                delta_docs_copied,
            } => {
                write!(f, "delta_pass({docs_copied} + {delta_docs_copied} copied)")
            }
            Self::Active => write!(f, "active"),
            Self::Failed { phase, reason } => write!(f, "failed({phase}: {reason})"),
        }
    }
}

/// Overall migration phase for a node addition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationPhase {
    /// Computing which shards move to the new node.
    ComputingAssignments,
    /// Dual-write active; background migration in progress.
    DualWriteMigrating,
    /// Background migration done; beginning cutover.
    CutoverBegin,
    /// Stopping dual-write; waiting for in-flight writes to settle.
    CutoverDraining,
    /// Re-reading source to catch docs written during migration.
    CutoverDeltaPass,
    /// Marking new node active; switching routing.
    CutoverActivate,
    /// Deleting migrated shard data from old nodes.
    CutoverCleanup,
    /// All shards migrated; migration complete.
    Complete,
    /// Migration failed.
    Failed(String),
}

impl fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ComputingAssignments => write!(f, "computing_assignments"),
            Self::DualWriteMigrating => write!(f, "dual_write_migrating"),
            Self::CutoverBegin => write!(f, "cutover_begin"),
            Self::CutoverDraining => write!(f, "cutover_draining"),
            Self::CutoverDeltaPass => write!(f, "cutover_delta_pass"),
            Self::CutoverActivate => write!(f, "cutover_activate"),
            Self::CutoverCleanup => write!(f, "cutover_cleanup"),
            Self::Complete => write!(f, "complete"),
            Self::Failed(msg) => write!(f, "failed({msg})"),
        }
    }
}

/// A single document write targeting a shard during migration.
#[derive(Debug, Clone)]
pub struct InFlightWrite {
    pub doc_id: String,
    pub shard: ShardId,
    pub target_nodes: Vec<NodeId>,
    pub completed_nodes: HashSet<NodeId>,
    pub failed_nodes: HashMap<NodeId, String>,
    pub submitted_at: Instant,
}

// Serialize Instant as a placeholder bool (present/absent).
// Instant is monotonic and not meaningfully serializable across processes;
// on deserialize, reconstruct as Instant::now().
mod instant_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Instant;

    pub fn serialize<S>(instant: &Option<Instant>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        instant.is_some().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Instant>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let present = bool::deserialize(deserializer)?;
        Ok(if present { Some(Instant::now()) } else { None })
    }
}

/// Configuration for migration cutover behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Maximum time to wait for in-flight writes to drain during cutover.
    pub drain_timeout: Duration,
    /// Whether to perform the delta pass (re-read source after stopping dual-write).
    /// Disabling this saves a pagination pass but opens the race window — only safe
    /// when anti-entropy is enabled as a safety net.
    pub skip_delta_pass: bool,
    /// Whether anti-entropy is enabled — used to determine if skip_delta_pass is safe.
    pub anti_entropy_enabled: bool,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            drain_timeout: Duration::from_secs(30),
            skip_delta_pass: false,
            anti_entropy_enabled: true,
        }
    }
}

/// Error type for migration operations.
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error(
        "anti-entropy is disabled and delta pass is skipped — documents may be lost at cutover"
    )]
    UnsafeCutoverNoAntiEntropy,
    #[error("drain timeout exceeded: {0} in-flight writes still pending")]
    DrainTimeout(u32),
    #[error("shard {0} is not in a valid state for this transition (current: {1})")]
    InvalidTransition(ShardId, String),
    #[error("migration {0} not found")]
    NotFound(MigrationId),
    #[error("delta pass failed for shard {0}: {1}")]
    DeltaPassFailed(ShardId, String),
}

/// Tracks the state of a node-addition migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationState {
    pub id: MigrationId,
    pub new_node: NodeId,
    pub replica_group: u32,
    pub phase: MigrationPhase,
    pub affected_shards: HashMap<ShardId, ShardMigrationState>,
    /// Maps shard → old node that currently owns it.
    pub old_owners: HashMap<ShardId, NodeId>,
    #[serde(with = "instant_serde")]
    pub started_at: Option<Instant>,
    #[serde(with = "instant_serde")]
    pub completed_at: Option<Instant>,
}

/// The migration coordinator manages shard migration state transitions.
pub struct MigrationCoordinator {
    config: MigrationConfig,
    migrations: HashMap<MigrationId, MigrationState>,
    next_id: u64,
    /// In-flight writes being tracked for drain during cutover.
    in_flight: Vec<InFlightWrite>,
}

impl MigrationCoordinator {
    pub fn new(config: MigrationConfig) -> Self {
        Self {
            config,
            migrations: HashMap::new(),
            next_id: 0,
            in_flight: Vec::new(),
        }
    }

    /// Validate migration safety before starting. Returns an error if the configuration
    /// would allow data loss at the cutover boundary.
    pub fn validate_safety(&self) -> Result<(), MigrationError> {
        if self.config.skip_delta_pass && !self.config.anti_entropy_enabled {
            return Err(MigrationError::UnsafeCutoverNoAntiEntropy);
        }
        Ok(())
    }

    /// Begin a new node-addition migration.
    pub fn begin_migration(
        &mut self,
        new_node: NodeId,
        replica_group: u32,
        affected_shards: HashMap<ShardId, NodeId>,
    ) -> Result<MigrationId, MigrationError> {
        self.validate_safety()?;

        let id = MigrationId(self.next_id);
        self.next_id += 1;

        let shard_states: HashMap<ShardId, ShardMigrationState> = affected_shards
            .keys()
            .map(|&shard| (shard, ShardMigrationState::Pending))
            .collect();

        let state = MigrationState {
            id,
            new_node,
            replica_group,
            phase: MigrationPhase::ComputingAssignments,
            affected_shards: shard_states,
            old_owners: affected_shards,
            started_at: Some(Instant::now()),
            completed_at: None,
        };

        self.migrations.insert(id, state);
        Ok(id)
    }

    /// Transition to dual-write + background migration phase.
    pub fn begin_dual_write(&mut self, id: MigrationId) -> Result<(), MigrationError> {
        let state = self
            .migrations
            .get_mut(&id)
            .ok_or(MigrationError::NotFound(id))?;
        state.phase = MigrationPhase::DualWriteMigrating;
        for shard_state in state.affected_shards.values_mut() {
            if *shard_state == ShardMigrationState::Pending {
                *shard_state = ShardMigrationState::Migrating {
                    docs_copied: 0,
                    pages_remaining: 0,
                };
            }
        }
        Ok(())
    }

    /// Record that a shard's background migration completed.
    pub fn shard_migration_complete(
        &mut self,
        id: MigrationId,
        shard: ShardId,
        docs_copied: u64,
    ) -> Result<(), MigrationError> {
        let state = self
            .migrations
            .get_mut(&id)
            .ok_or(MigrationError::NotFound(id))?;
        let shard_state = state.affected_shards.get_mut(&shard).ok_or_else(|| {
            MigrationError::InvalidTransition(shard, "shard not in migration".into())
        })?;

        match shard_state {
            ShardMigrationState::Migrating { .. } => {
                *shard_state = ShardMigrationState::MigrationComplete { docs_copied };
            }
            _ => {
                return Err(MigrationError::InvalidTransition(
                    shard,
                    shard_state.to_string(),
                ));
            }
        }

        // Check if all shards are done migrating
        let all_complete = state
            .affected_shards
            .values()
            .all(|s| matches!(s, ShardMigrationState::MigrationComplete { .. }));

        if all_complete {
            state.phase = MigrationPhase::CutoverBegin;
        }

        Ok(())
    }

    /// Begin the cutover sequence: stop dual-write and drain in-flight writes.
    pub fn begin_cutover(&mut self, id: MigrationId) -> Result<MigrationPhase, MigrationError> {
        let state = self
            .migrations
            .get_mut(&id)
            .ok_or(MigrationError::NotFound(id))?;

        if !matches!(state.phase, MigrationPhase::CutoverBegin) {
            return Err(MigrationError::InvalidTransition(
                ShardId(0),
                format!("expected CutoverBegin, got {}", state.phase),
            ));
        }

        // Transition all shards to Draining
        let total_in_flight = self.in_flight.len() as u32;
        for (shard, shard_state) in state.affected_shards.iter_mut() {
            match shard_state {
                ShardMigrationState::MigrationComplete { docs_copied } => {
                    *shard_state = ShardMigrationState::Draining {
                        in_flight_count: total_in_flight,
                        docs_copied: *docs_copied,
                    };
                }
                _ => {
                    return Err(MigrationError::InvalidTransition(
                        *shard,
                        shard_state.to_string(),
                    ));
                }
            }
        }

        state.phase = MigrationPhase::CutoverDraining;
        Ok(state.phase.clone())
    }

    /// Register an in-flight write for tracking during drain.
    pub fn register_in_flight(&mut self, write: InFlightWrite) {
        self.in_flight.push(write);
    }

    /// Acknowledge completion of a write to a specific node.
    pub fn ack_write(&mut self, doc_id: &str, node: &NodeId) {
        for write in &mut self.in_flight {
            if write.doc_id == doc_id {
                write.completed_nodes.insert(node.clone());
            }
        }
    }

    /// Mark a write as failed on a specific node.
    pub fn fail_write(&mut self, doc_id: &str, node: &NodeId, reason: String) {
        for write in &mut self.in_flight {
            if write.doc_id == doc_id {
                write.failed_nodes.insert(node.clone(), reason.clone());
            }
        }
    }

    /// Check if all in-flight writes have completed (drained).
    pub fn is_drained(&self) -> bool {
        self.in_flight
            .iter()
            .all(|w| w.completed_nodes.len() + w.failed_nodes.len() == w.target_nodes.len())
    }

    /// Complete the drain and move to delta pass or activation.
    pub fn complete_drain(&mut self, id: MigrationId) -> Result<MigrationPhase, MigrationError> {
        // First check phase exists without holding mutable borrow
        let phase = self
            .migrations
            .get(&id)
            .ok_or(MigrationError::NotFound(id))?
            .phase
            .clone();

        if !matches!(phase, MigrationPhase::CutoverDraining) {
            return Err(MigrationError::InvalidTransition(
                ShardId(0),
                format!("expected CutoverDraining, got {}", phase),
            ));
        }

        // Check drain status
        if !self.is_drained() {
            let remaining = self
                .in_flight
                .iter()
                .filter(|w| w.completed_nodes.len() + w.failed_nodes.len() < w.target_nodes.len())
                .count() as u32;
            return Err(MigrationError::DrainTimeout(remaining));
        }

        // Collect docs that need delta pass
        let needs_delta = self.collect_delta_candidates(id)?;
        let skip_delta = self.config.skip_delta_pass;

        // Now get mutable borrow to update state
        let state = self
            .migrations
            .get_mut(&id)
            .ok_or(MigrationError::NotFound(id))?;

        if skip_delta {
            // Skip delta pass — safe only if anti-entropy is enabled
            state.phase = MigrationPhase::CutoverActivate;
        } else if needs_delta.is_empty() {
            state.phase = MigrationPhase::CutoverActivate;
        } else {
            state.phase = MigrationPhase::CutoverDeltaPass;
            for (_shard, shard_state) in state.affected_shards.iter_mut() {
                if let ShardMigrationState::Draining { docs_copied, .. } = shard_state {
                    *shard_state = ShardMigrationState::DeltaPass {
                        docs_copied: *docs_copied,
                        delta_docs_copied: 0,
                    };
                }
            }
        }

        self.in_flight.clear();

        // If going to activate, do that now (drop mutable borrow first)
        let next_phase = state.phase.clone();
        if matches!(next_phase, MigrationPhase::CutoverActivate) {
            let _ = state;
            self.activate_shards(id)?;
            // Return the new phase after activation
            return Ok(self
                .migrations
                .get(&id)
                .map(|s| s.phase.clone())
                .unwrap_or(MigrationPhase::CutoverCleanup));
        }

        Ok(next_phase)
    }

    /// Identify writes that need the delta pass — those that succeeded on OLD but
    /// failed (or never reached) NEW.
    fn collect_delta_candidates(
        &self,
        id: MigrationId,
    ) -> Result<HashMap<ShardId, Vec<String>>, MigrationError> {
        let state = self
            .migrations
            .get(&id)
            .ok_or(MigrationError::NotFound(id))?;
        let mut candidates: HashMap<ShardId, Vec<String>> = HashMap::new();

        for write in &self.in_flight {
            let old_owner = match state.old_owners.get(&write.shard) {
                Some(owner) => owner,
                None => continue,
            };

            let succeeded_on_old = write.completed_nodes.contains(old_owner);
            let succeeded_on_new = write.completed_nodes.contains(&state.new_node);

            // Doc is on OLD but not on NEW — delta pass must catch it
            if succeeded_on_old && !succeeded_on_new {
                candidates
                    .entry(write.shard)
                    .or_default()
                    .push(write.doc_id.clone());
            }
        }

        Ok(candidates)
    }

    /// Record that the delta pass completed for a shard.
    pub fn shard_delta_complete(
        &mut self,
        id: MigrationId,
        shard: ShardId,
        delta_docs: u64,
    ) -> Result<(), MigrationError> {
        let state = self
            .migrations
            .get_mut(&id)
            .ok_or(MigrationError::NotFound(id))?;
        let shard_state = state.affected_shards.get_mut(&shard).ok_or_else(|| {
            MigrationError::InvalidTransition(shard, "shard not in migration".into())
        })?;

        match shard_state {
            ShardMigrationState::DeltaPass { docs_copied, .. } => {
                *shard_state = ShardMigrationState::MigrationComplete {
                    docs_copied: *docs_copied + delta_docs,
                };
            }
            _ => {
                return Err(MigrationError::InvalidTransition(
                    shard,
                    shard_state.to_string(),
                ));
            }
        }

        // Check if all shards done with delta
        let all_complete = state
            .affected_shards
            .values()
            .all(|s| matches!(s, ShardMigrationState::MigrationComplete { .. }));

        if all_complete {
            state.phase = MigrationPhase::CutoverActivate;
            self.activate_shards(id)?;
        }

        Ok(())
    }

    /// Mark all affected shards as active on the new node.
    fn activate_shards(&mut self, id: MigrationId) -> Result<(), MigrationError> {
        let state = self
            .migrations
            .get_mut(&id)
            .ok_or(MigrationError::NotFound(id))?;

        for shard_state in state.affected_shards.values_mut() {
            match shard_state {
                ShardMigrationState::MigrationComplete { .. }
                | ShardMigrationState::Draining { .. } => {
                    *shard_state = ShardMigrationState::Active;
                }
                _ => {}
            }
        }

        if matches!(state.phase, MigrationPhase::CutoverActivate) {
            state.phase = MigrationPhase::CutoverCleanup;
        }

        Ok(())
    }

    /// Complete the migration by deleting migrated shard data from old nodes.
    pub fn complete_cleanup(&mut self, id: MigrationId) -> Result<(), MigrationError> {
        let state = self
            .migrations
            .get_mut(&id)
            .ok_or(MigrationError::NotFound(id))?;

        if !matches!(state.phase, MigrationPhase::CutoverCleanup) {
            return Err(MigrationError::InvalidTransition(
                ShardId(0),
                format!("expected CutoverCleanup, got {}", state.phase),
            ));
        }

        state.phase = MigrationPhase::Complete;
        state.completed_at = Some(Instant::now());
        Ok(())
    }

    /// Get the current state of a migration.
    pub fn get_state(&self, id: MigrationId) -> Option<&MigrationState> {
        self.migrations.get(&id)
    }

    /// Check if a write should go to both old and new node (dual-write phase).
    pub fn is_dual_write_active(&self, shard: ShardId) -> bool {
        self.migrations.values().any(|m| {
            matches!(m.phase, MigrationPhase::DualWriteMigrating)
                && matches!(
                    m.affected_shards.get(&shard),
                    Some(ShardMigrationState::Migrating { .. })
                )
        })
    }

    /// Get the migration config.
    pub fn config(&self) -> &MigrationConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(s: &str) -> NodeId {
        NodeId(s.to_string())
    }

    fn shard(id: u32) -> ShardId {
        ShardId(id)
    }

    #[test]
    fn test_safe_cutover_with_delta_pass() {
        let config = MigrationConfig {
            anti_entropy_enabled: false,
            skip_delta_pass: false,
            ..Default::default()
        };
        let mut coord = MigrationCoordinator::new(config);

        let affected = HashMap::from([(shard(0), node("old-0")), (shard(1), node("old-0"))]);

        let mid = coord.begin_migration(node("new-0"), 0, affected).unwrap();
        coord.begin_dual_write(mid).unwrap();

        // Simulate background migration completing
        coord.shard_migration_complete(mid, shard(0), 500).unwrap();
        coord.shard_migration_complete(mid, shard(1), 300).unwrap();

        // Register an in-flight write that succeeded on OLD but not NEW.
        // The write must be marked as failed on NEW so is_drained() sees
        // completed + failed == target count.
        coord.register_in_flight(InFlightWrite {
            doc_id: "doc-at-boundary".into(),
            shard: shard(0),
            target_nodes: vec![node("old-0"), node("new-0")],
            completed_nodes: HashSet::from([node("old-0")]),
            failed_nodes: HashMap::from([(node("new-0"), "write failed".into())]),
            submitted_at: Instant::now(),
        });

        // Cutover
        coord.begin_cutover(mid).unwrap();

        // The drain sees the in-flight write completed (on old, not on new)
        // Delta pass should be triggered
        let phase = coord.complete_drain(mid).unwrap();
        assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

        // Delta pass catches the straggler
        coord.shard_delta_complete(mid, shard(0), 1).unwrap();
        // Shard 1 had no stragglers, but needs delta complete too
        coord.shard_delta_complete(mid, shard(1), 0).unwrap();

        // Now activation and cleanup
        let state = coord.get_state(mid).unwrap();
        assert_eq!(state.phase, MigrationPhase::CutoverCleanup);

        coord.complete_cleanup(mid).unwrap();
        let state = coord.get_state(mid).unwrap();
        assert_eq!(state.phase, MigrationPhase::Complete);
    }

    #[test]
    fn test_unsafe_cutover_refused_without_anti_entropy() {
        let config = MigrationConfig {
            anti_entropy_enabled: false,
            skip_delta_pass: true,
            ..Default::default()
        };
        let mut coord = MigrationCoordinator::new(config);

        let affected = HashMap::from([(shard(0), node("old-0"))]);
        let result = coord.begin_migration(node("new-0"), 0, affected);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, MigrationError::UnsafeCutoverNoAntiEntropy));
    }

    #[test]
    fn test_skip_delta_pass_allowed_with_anti_entropy() {
        let config = MigrationConfig {
            anti_entropy_enabled: true,
            skip_delta_pass: true,
            ..Default::default()
        };
        let mut coord = MigrationCoordinator::new(config);

        let affected = HashMap::from([(shard(0), node("old-0"))]);
        let mid = coord.begin_migration(node("new-0"), 0, affected).unwrap();
        coord.begin_dual_write(mid).unwrap();
        coord.shard_migration_complete(mid, shard(0), 100).unwrap();

        coord.begin_cutover(mid).unwrap();

        // With skip_delta_pass=true and AE enabled, drain goes straight to activate
        let phase = coord.complete_drain(mid).unwrap();
        assert_eq!(phase, MigrationPhase::CutoverCleanup);

        coord.complete_cleanup(mid).unwrap();
        assert_eq!(
            coord.get_state(mid).unwrap().phase,
            MigrationPhase::Complete
        );
    }

    #[test]
    fn test_drain_timeout_blocks_cutover() {
        let config = MigrationConfig {
            anti_entropy_enabled: true,
            skip_delta_pass: true,
            ..Default::default()
        };
        let mut coord = MigrationCoordinator::new(config);

        let affected = HashMap::from([(shard(0), node("old-0"))]);
        let mid = coord.begin_migration(node("new-0"), 0, affected).unwrap();
        coord.begin_dual_write(mid).unwrap();
        coord.shard_migration_complete(mid, shard(0), 100).unwrap();
        coord.begin_cutover(mid).unwrap();

        // Register an in-flight write that hasn't completed on either node
        coord.register_in_flight(InFlightWrite {
            doc_id: "stuck-doc".into(),
            shard: shard(0),
            target_nodes: vec![node("old-0"), node("new-0")],
            completed_nodes: HashSet::new(),
            failed_nodes: HashMap::new(),
            submitted_at: Instant::now(),
        });

        // Drain should fail — write still in flight
        let result = coord.complete_drain(mid);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            MigrationError::DrainTimeout(1)
        ));
    }

    #[test]
    fn test_display_impls() {
        assert_eq!(format!("{}", MigrationId(42)), "42");
        assert_eq!(format!("{}", NodeId("n1".into())), "n1");
        assert_eq!(format!("{}", ShardId(7)), "s7");
        assert_eq!(format!("{}", ShardMigrationState::Pending), "pending");
        assert_eq!(
            format!("{}", ShardMigrationState::Migrating { docs_copied: 100, pages_remaining: 3 }),
            "migrating(100 copied, 3 pages left)"
        );
        assert_eq!(
            format!("{}", ShardMigrationState::MigrationComplete { docs_copied: 500 }),
            "migration_complete(500 copied)"
        );
        assert_eq!(
            format!("{}", ShardMigrationState::Draining { in_flight_count: 2, docs_copied: 500 }),
            "draining(2 in-flight, 500 copied)"
        );
        assert_eq!(
            format!("{}", ShardMigrationState::DeltaPass { docs_copied: 500, delta_docs_copied: 3 }),
            "delta_pass(500 + 3 copied)"
        );
        assert_eq!(format!("{}", ShardMigrationState::Active), "active");
        assert_eq!(
            format!("{}", ShardMigrationState::Failed { phase: "cutover".into(), reason: "oops".into() }),
            "failed(cutover: oops)"
        );

        assert_eq!(format!("{}", MigrationPhase::ComputingAssignments), "computing_assignments");
        assert_eq!(format!("{}", MigrationPhase::DualWriteMigrating), "dual_write_migrating");
        assert_eq!(format!("{}", MigrationPhase::CutoverBegin), "cutover_begin");
        assert_eq!(format!("{}", MigrationPhase::CutoverDraining), "cutover_draining");
        assert_eq!(format!("{}", MigrationPhase::CutoverDeltaPass), "cutover_delta_pass");
        assert_eq!(format!("{}", MigrationPhase::CutoverActivate), "cutover_activate");
        assert_eq!(format!("{}", MigrationPhase::CutoverCleanup), "cutover_cleanup");
        assert_eq!(format!("{}", MigrationPhase::Complete), "complete");
        assert_eq!(format!("{}", MigrationPhase::Failed("err".into())), "failed(err)");
    }

    #[test]
    fn test_ack_and_fail_write_tracking() {
        let config = MigrationConfig::default();
        let mut coord = MigrationCoordinator::new(config);

        let affected = HashMap::from([(shard(0), node("old-0"))]);
        let mid = coord.begin_migration(node("new-0"), 0, affected).unwrap();
        coord.begin_dual_write(mid).unwrap();
        coord.shard_migration_complete(mid, shard(0), 100).unwrap();

        coord.register_in_flight(InFlightWrite {
            doc_id: "doc-1".into(),
            shard: shard(0),
            target_nodes: vec![node("old-0"), node("new-0")],
            completed_nodes: HashSet::new(),
            failed_nodes: HashMap::new(),
            submitted_at: Instant::now(),
        });

        assert!(!coord.is_drained());

        coord.ack_write("doc-1", &node("old-0"));
        coord.fail_write("doc-1", &node("new-0"), "timeout".into());
        assert!(coord.is_drained());
    }

    #[test]
    fn test_invalid_transitions() {
        let config = MigrationConfig::default();
        let mut coord = MigrationCoordinator::new(config);

        let affected = HashMap::from([(shard(0), node("old-0"))]);
        let mid = coord.begin_migration(node("new-0"), 0, affected).unwrap();

        // shard_migration_complete on Pending shard should fail
        let err = coord.shard_migration_complete(mid, shard(0), 10).unwrap_err();
        assert!(matches!(err, MigrationError::InvalidTransition(_, _)));

        // NotFound for invalid migration
        let err = coord.begin_cutover(MigrationId(999)).unwrap_err();
        assert!(matches!(err, MigrationError::NotFound(_)));

        // complete_drain on non-draining phase
        let err = coord.complete_drain(mid).unwrap_err();
        assert!(matches!(err, MigrationError::InvalidTransition(_, _)));

        // complete_cleanup on wrong phase
        let err = coord.complete_cleanup(mid).unwrap_err();
        assert!(matches!(err, MigrationError::InvalidTransition(_, _)));
    }

    #[test]
    fn test_migration_config_access() {
        let config = MigrationConfig::default();
        let coord = MigrationCoordinator::new(config.clone());
        assert_eq!(coord.config().drain_timeout, config.drain_timeout);
    }

    #[test]
    fn test_dual_write_tracking() {
        let config = MigrationConfig::default();
        let mut coord = MigrationCoordinator::new(config);

        let affected = HashMap::from([(shard(5), node("old-0"))]);
        let mid = coord.begin_migration(node("new-0"), 0, affected).unwrap();
        coord.begin_dual_write(mid).unwrap();

        // Shard 5 is in dual-write
        assert!(coord.is_dual_write_active(shard(5)));
        // Shard 99 is not being migrated
        assert!(!coord.is_dual_write_active(shard(99)));

        // After migration completes, shard 5 is no longer dual-write
        coord.shard_migration_complete(mid, shard(5), 100).unwrap();
        assert!(!coord.is_dual_write_active(shard(5)));
    }
}
