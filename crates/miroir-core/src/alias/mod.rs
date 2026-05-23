//! Atomic index aliases for blue-green reindexing (plan §13.7).
//!
//! This module implements the alias layer that allows atomic index flips
//! without downtime. Aliases resolve to one or more concrete Meilisearch
//! index UIDs, supporting both single-target (writable) and multi-target
//! (read-only, used by ILM) aliases.
//! Uses leader-only singleton coordination (plan §14.5) to ensure only one pod
//! performs an alias flip at a time for a given alias name.

use crate::error::{MiroirError, Result};
use crate::leader_election::LeaderElection;
use crate::mode_b_coordinator::ModeBOpLeader;
use crate::task_store::TaskStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Alias kind: single-target (writable) or multi-target (read-only, ILM-managed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AliasKind {
    Single,
    Multi,
}

/// A single alias record from the task store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alias {
    /// Alias name (the key clients use).
    pub name: String,
    /// `single` or `multi`.
    pub kind: AliasKind,
    /// Current target UID (only set when kind=single).
    pub current_uid: Option<String>,
    /// Target UIDs as JSON array (only set when kind=multi).
    pub target_uids: Option<Vec<String>>,
    /// Generation incremented on each flip.
    pub generation: u64,
    /// Created at timestamp.
    pub created_at: u64,
    /// Last updated timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<u64>,
}

impl Alias {
    /// Create a new single-target alias.
    pub fn new_single(name: String, target_uid: String) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            name,
            kind: AliasKind::Single,
            current_uid: Some(target_uid),
            target_uids: None,
            generation: 0,
            created_at: now,
            updated_at: Some(now),
        }
    }

    /// Create a new multi-target alias.
    pub fn new_multi(name: String, target_uids: Vec<String>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            name,
            kind: AliasKind::Multi,
            current_uid: None,
            target_uids: Some(target_uids),
            generation: 0,
            created_at: now,
            updated_at: Some(now),
        }
    }

    /// Check if this alias is multi-target (read-only, ILM-managed).
    pub fn is_multi_target(&self) -> bool {
        matches!(self.kind, AliasKind::Multi)
    }

    /// Get the effective target UIDs for this alias.
    pub fn targets(&self) -> Result<Vec<String>> {
        match self.kind {
            AliasKind::Single => {
                let uid = self.current_uid.as_ref()
                    .ok_or_else(|| MiroirError::InvalidState("single alias missing current_uid".into()))?;
                Ok(vec![uid.clone()])
            }
            AliasKind::Multi => {
                let uids = self.target_uids.as_ref()
                    .ok_or_else(|| MiroirError::InvalidState("multi alias missing target_uids".into()))?;
                Ok(uids.clone())
            }
        }
    }

    /// Flip this alias to a new target (single-target only).
    pub fn flip(&mut self, new_target: String) -> Result<()> {
        if self.kind != AliasKind::Single {
            return Err(MiroirError::InvalidState("cannot flip multi-target alias".into()));
        }
        self.current_uid = Some(new_target);
        self.generation += 1;
        self.updated_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs());
        Ok(())
    }

    /// Update multi-target alias UIDs (ILM-only).
    pub fn update_targets(&mut self, new_targets: Vec<String>) -> Result<()> {
        if self.kind != AliasKind::Multi {
            return Err(MiroirError::InvalidState("cannot update_targets on single-target alias".into()));
        }
        self.target_uids = Some(new_targets);
        self.generation += 1;
        self.updated_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs());
        Ok(())
    }
}

/// In-memory alias registry with task-store persistence.
#[derive(Clone)]
pub struct AliasRegistry {
    aliases: Arc<RwLock<HashMap<String, Alias>>>,
}

impl AliasRegistry {
    /// Create a new empty alias registry.
    pub fn new() -> Self {
        Self {
            aliases: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new alias registry and load from task store.
    pub async fn load_from_store(task_store: &dyn TaskStore) -> Result<Self> {
        let registry = Self::new();
        registry.sync_from_store(task_store).await?;
        Ok(registry)
    }

    /// Sync aliases from the task store into memory.
    pub async fn sync_from_store(&self, task_store: &dyn TaskStore) -> Result<()> {
        let rows = task_store.list_aliases()?;
        let mut aliases = self.aliases.write().await;

        // Clear and reload from store
        aliases.clear();
        for row in rows {
            let alias = Alias {
                name: row.name.clone(),
                kind: match row.kind.as_str() {
                    "single" => AliasKind::Single,
                    "multi" => AliasKind::Multi,
                    _ => return Err(MiroirError::InvalidState(format!("invalid alias kind: {}", row.kind))),
                },
                current_uid: row.current_uid,
                target_uids: row.target_uids,
                generation: row.version as u64,
                created_at: row.created_at as u64,
                updated_at: None, // Task store doesn't track updated_at separately
            };
            aliases.insert(row.name, alias);
        }

        info!("loaded {} aliases from task store", aliases.len());
        Ok(())
    }

    /// Resolve an index UID or alias name to concrete target UIDs.
    ///
    /// If `input` is not a known alias, returns it as-is (treat as concrete UID).
    pub async fn resolve(&self, input: &str) -> Vec<String> {
        let aliases = self.aliases.read().await;
        match aliases.get(input) {
            Some(alias) => alias.targets().unwrap_or_else(|_| vec![input.to_string()]),
            None => vec![input.to_string()],
        }
    }

    /// Check if an input is an alias (vs a concrete UID).
    pub async fn is_alias(&self, input: &str) -> bool {
        self.aliases.read().await.contains_key(input)
    }

    /// Check if an input is a multi-target alias (for write rejection).
    pub async fn is_multi_target_alias(&self, input: &str) -> bool {
        self.aliases.read().await
            .get(input)
            .map(|a| a.is_multi_target())
            .unwrap_or(false)
    }

    /// Get a single alias by name.
    pub async fn get(&self, name: &str) -> Option<Alias> {
        self.aliases.read().await.get(name).cloned()
    }

    /// List all aliases.
    pub async fn list(&self) -> Vec<Alias> {
        self.aliases.read().await.values().cloned().collect()
    }

    /// Create or update an alias.
    pub async fn upsert(&self, alias: Alias) -> Result<()> {
        let mut aliases = self.aliases.write().await;
        aliases.insert(alias.name.clone(), alias);
        Ok(())
    }

    /// Delete an alias.
    pub async fn delete(&self, name: &str) -> Result<bool> {
        let mut aliases = self.aliases.write().await;
        Ok(aliases.remove(name).is_some())
    }

    /// Flip a single-target alias atomically.
    pub async fn flip(&self, name: &str, new_target: String) -> Result<()> {
        let mut aliases = self.aliases.write().await;
        let alias = aliases.get_mut(name)
            .ok_or_else(|| MiroirError::NotFound(format!("alias '{}' not found", name)))?;
        alias.flip(new_target)?;
        Ok(())
    }

    /// Update a multi-target alias (ILM use only).
    pub async fn update_multi(&self, name: &str, new_targets: Vec<String>) -> Result<()> {
        let mut aliases = self.aliases.write().await;
        let alias = aliases.get_mut(name)
            .ok_or_else(|| MiroirError::NotFound(format!("alias '{}' not found", name)))?;
        alias.update_targets(new_targets)?;
        Ok(())
    }
}

impl Default for AliasRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Alias flip coordinator with leader-only singleton coordination (plan §14.5).
///
/// Acquires a per-alias leader lease (scope: "alias_flip:<name>") and persists
/// phase state so that a new leader can resume from the last committed phase.
pub struct AliasFlipCoordinator {
    /// Mode B operation leader with phase state persistence.
    leader: ModeBOpLeader<AliasFlipExtraState>,
}

/// Extra state for alias flip operations persisted to mode_b_operations.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AliasFlipExtraState {
    /// Old index UID (before flip).
    pub old_uid: Option<String>,
    /// New index UID (after flip).
    pub new_uid: String,
    /// History retention count (for rollback).
    pub history_retention: usize,
    /// Generation number (incremented on each flip).
    pub generation: u64,
}

impl AliasFlipCoordinator {
    /// Create a new alias flip coordinator.
    pub fn new(
        leader_election: Arc<LeaderElection>,
        task_store: Arc<dyn TaskStore>,
        alias_name: String,
        new_uid: String,
        pod_id: String,
    ) -> Self {
        let scope = format!("alias_flip:{}", alias_name);

        let extra_state = AliasFlipExtraState {
            new_uid,
            history_retention: 10,
            generation: 0,
            ..Default::default()
        };

        let leader = ModeBOpLeader::new(
            leader_election,
            task_store,
            crate::task_store::mode_b_type::ALIAS_FLIP.to_string(),
            scope,
            pod_id,
            extra_state,
        );

        Self { leader }
    }

    /// Try to acquire leadership for this alias flip.
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
    pub fn extra_state(&mut self) -> &mut AliasFlipExtraState {
        self.leader.extra_state()
    }

    /// Get the extra state (immutable).
    pub fn extra_state_ref(&self) -> &AliasFlipExtraState {
        self.leader.extra_state_ref()
    }

    /// Advance to the next phase and persist state.
    ///
    /// Should be called after each phase boundary so that a new leader can
    /// resume from the last committed phase.
    pub async fn advance_phase(&mut self, new_phase: &str) -> Result<()> {
        self.leader.persist_phase(new_phase.to_string()).await
    }

    /// Perform the alias flip operation.
    pub async fn flip(&mut self, old_uid: String) -> Result<()> {
        self.leader.extra_state().old_uid = Some(old_uid);
        self.leader.extra_state().generation += 1;
        self.leader.persist_phase("flipped".to_string()).await
    }

    /// Mark the operation as failed and step down from leadership.
    pub async fn fail(&mut self, error: String) -> Result<()> {
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
    pub async fn recover(&mut self) -> Result<Option<String>> {
        let existing = self.leader.recover().await?;

        if let Some(ref op) = existing {
            info!(
                new_uid = %self.leader.extra_state_ref().new_uid,
                generation = self.leader.extra_state_ref().generation,
                phase = %op.phase,
                "recovered alias flip from persisted phase"
            );

            return Ok(Some(op.phase.clone()));
        }

        Ok(None)
    }

    /// Delete the operation state after completion.
    pub async fn delete(&self) -> Result<bool> {
        self.leader.delete().await
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod acceptance_tests;
