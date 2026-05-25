//! Two-phase settings broadcast with verification (plan §13.5).
//!
//! This module implements the propose/verify/commit flow for settings changes,
//! replacing the sequential apply-with-rollback approach.
//! Uses leader-only singleton coordination (plan §14.5) to ensure only one pod
//! orchestrates the broadcast for a given index.

use crate::error::{MiroirError, Result};
use crate::leader_election::LeaderElection;
use crate::mode_b_coordinator::ModeBOpLeader;
use crate::task_store::TaskStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Current phase of a settings broadcast.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum BroadcastPhase {
    Idle = 0,
    Propose = 1,
    Verify = 2,
    Commit = 3,
}

/// Status of an ongoing settings broadcast.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastStatus {
    /// Index UID.
    pub index: String,
    /// Current phase.
    pub phase: BroadcastPhase,
    /// Proposed settings fingerprint.
    pub proposed_fingerprint: Option<String>,
    /// Per-node task UIDs from Phase 1.
    pub node_task_uids: HashMap<String, u64>,
    /// Per-node verification results from Phase 2.
    pub node_hashes: HashMap<String, String>,
    /// Whether verification succeeded.
    pub verify_ok: bool,
    /// Settings version after commit.
    pub settings_version: Option<u64>,
    /// Error message if any.
    pub error: Option<String>,
}

/// Settings broadcast coordinator.
pub struct SettingsBroadcast {
    /// In-flight broadcasts (index -> status).
    in_flight: Arc<RwLock<HashMap<String, BroadcastStatus>>>,
    /// Global settings version (incremented on successful commit).
    settings_version: Arc<RwLock<u64>>,
    /// Per-(index, node) settings version (for X-Miroir-Min-Settings-Version).
    node_settings_version: Arc<RwLock<HashMap<(String, String), u64>>>,
    /// Task store for persistent version tracking.
    task_store: Option<Arc<dyn TaskStore>>,
}

impl SettingsBroadcast {
    /// Create a new settings broadcast coordinator.
    pub fn new() -> Self {
        Self {
            in_flight: Arc::new(RwLock::new(HashMap::new())),
            settings_version: Arc::new(RwLock::new(0)),
            node_settings_version: Arc::new(RwLock::new(HashMap::new())),
            task_store: None,
        }
    }

    /// Create a new settings broadcast coordinator with task store.
    pub fn with_task_store(task_store: Arc<dyn TaskStore>) -> Self {
        Self {
            in_flight: Arc::new(RwLock::new(HashMap::new())),
            settings_version: Arc::new(RwLock::new(0)),
            node_settings_version: Arc::new(RwLock::new(HashMap::new())),
            task_store: Some(task_store),
        }
    }

    /// Get the current global settings version.
    pub async fn current_version(&self) -> u64 {
        *self.settings_version.read().await
    }

    /// Get the per-(index, node) settings version.
    /// Checks in-memory cache first, then task store if available.
    pub async fn node_version(&self, index: &str, node_id: &str) -> u64 {
        // Check in-memory cache first
        let versions = self.node_settings_version.read().await;
        if let Some(&version) = versions.get(&(index.to_string(), node_id.to_string())) {
            return version;
        }
        drop(versions);

        // Fall back to task store if available
        if let Some(ref store) = self.task_store {
            if let Ok(Some(row)) = store.get_node_settings_version(index, node_id) {
                // Update cache
                let mut versions = self.node_settings_version.write().await;
                versions.insert((index.to_string(), node_id.to_string()), row.version as u64);
                return row.version as u64;
            }
        }

        0
    }

    /// Get the minimum settings version across all nodes for an index.
    /// Used for client-pinned freshness (X-Miroir-Min-Settings-Version).
    pub async fn min_node_version(&self, index: &str, node_ids: &[String]) -> Option<u64> {
        let mut min_version: Option<u64> = None;
        for node_id in node_ids {
            let version = self.node_version(index, node_id).await;
            min_version = Some(match min_version {
                None => version,
                Some(current) if version < current => version,
                Some(current) => current,
            });
        }
        min_version
    }

    /// Check if a node's settings version meets the minimum required version.
    /// Returns false if the node's version is below the floor.
    pub async fn node_version_meets_floor(&self, index: &str, node_id: &str, floor: u64) -> bool {
        self.node_version(index, node_id).await >= floor
    }

    /// Start a new settings broadcast (Phase 1: Propose).
    ///
    /// The caller should:
    /// 1. PATCH /indexes/{uid}/settings on each node in parallel
    /// 2. Collect task_uid from each response
    /// 3. Call `enter_verify` with the task UIDs
    pub async fn start_propose(&self, index: String, settings: &Value) -> Result<String> {
        let mut in_flight = self.in_flight.write().await;

        if in_flight.contains_key(&index) {
            return Err(MiroirError::InvalidState(format!(
                "settings broadcast already in flight for index '{index}'"
            )));
        }

        let fingerprint = fingerprint_settings(settings);
        let status = BroadcastStatus {
            index: index.clone(),
            phase: BroadcastPhase::Propose,
            proposed_fingerprint: Some(fingerprint),
            node_task_uids: HashMap::new(),
            node_hashes: HashMap::new(),
            verify_ok: false,
            settings_version: None,
            error: None,
        };

        in_flight.insert(index.clone(), status);
        Ok(index)
    }

    /// Enter Phase 2: Verify.
    ///
    /// The caller should:
    /// 1. Wait for all node tasks to succeed
    /// 2. GET /indexes/{uid}/settings from each node
    /// 3. Compute SHA256 of canonical JSON for each
    /// 4. Call `verify_hashes` with the results
    pub async fn enter_verify(
        &self,
        index: &str,
        node_task_uids: HashMap<String, u64>,
    ) -> Result<()> {
        let mut in_flight = self.in_flight.write().await;
        let status = in_flight
            .get_mut(index)
            .ok_or_else(|| MiroirError::NotFound(format!("index '{index}'")))?;

        if status.phase != BroadcastPhase::Propose {
            return Err(MiroirError::InvalidState("expected Propose phase".into()));
        }

        status.phase = BroadcastPhase::Verify;
        status.node_task_uids = node_task_uids;
        Ok(())
    }

    /// Verify per-node settings hashes.
    ///
    /// Returns `Ok(())` if all hashes match the proposed fingerprint.
    /// Returns `Err` if any mismatch (caller should retry or abort).
    pub async fn verify_hashes(
        &self,
        index: &str,
        node_hashes: HashMap<String, String>,
        expected_fingerprint: &str,
    ) -> Result<()> {
        let mut in_flight = self.in_flight.write().await;
        let status = in_flight
            .get_mut(index)
            .ok_or_else(|| MiroirError::NotFound(format!("index '{index}'")))?;

        if status.phase != BroadcastPhase::Verify {
            return Err(MiroirError::InvalidState("expected Verify phase".into()));
        }

        status.node_hashes = node_hashes.clone();

        // Check all hashes match the expected fingerprint.
        for (node, hash) in &node_hashes {
            if hash != expected_fingerprint {
                status.error = Some(format!(
                    "node '{node}' hash mismatch: expected {expected_fingerprint}, got {hash}"
                ));
                status.verify_ok = false;
                return Err(MiroirError::SettingsDivergence);
            }
        }

        status.verify_ok = true;
        Ok(())
    }

    /// Enter Phase 3: Commit.
    ///
    /// Increments the global settings version and stamps all affected nodes.
    pub async fn commit(&self, index: &str) -> Result<u64> {
        let mut in_flight = self.in_flight.write().await;
        let status = in_flight
            .get_mut(index)
            .ok_or_else(|| MiroirError::NotFound(format!("index '{index}'")))?;

        if status.phase != BroadcastPhase::Verify {
            return Err(MiroirError::InvalidState("expected Verify phase".into()));
        }

        if !status.verify_ok {
            return Err(MiroirError::InvalidState("verification failed".into()));
        }

        // Increment global settings version.
        let mut version = self.settings_version.write().await;
        *version += 1;
        let new_version = *version;
        drop(version);

        // Update per-node versions for all nodes that verified successfully.
        let mut node_versions = self.node_settings_version.write().await;
        let now = now_ms();
        for node_id in status.node_hashes.keys() {
            node_versions.insert((index.to_string(), node_id.clone()), new_version);

            // Persist to task store if available
            if let Some(ref store) = self.task_store {
                let _ = store.upsert_node_settings_version(index, node_id, new_version as i64, now);
            }
        }

        status.phase = BroadcastPhase::Commit;
        status.settings_version = Some(new_version);

        // Remove from in-flight map after a short delay (caller should do this).
        Ok(new_version)
    }

    /// Complete the broadcast and remove from in-flight tracking.
    pub async fn complete(&self, index: &str) -> Result<()> {
        let mut in_flight = self.in_flight.write().await;
        in_flight
            .remove(index)
            .ok_or_else(|| MiroirError::NotFound(format!("index '{index}'")))?;
        Ok(())
    }

    /// Abort a broadcast (on error).
    pub async fn abort(&self, index: &str, error: String) -> Result<()> {
        let mut in_flight = self.in_flight.write().await;
        if let Some(status) = in_flight.get_mut(index) {
            status.error = Some(error);
        }
        in_flight
            .remove(index)
            .ok_or_else(|| MiroirError::NotFound(format!("index '{index}'")))?;
        Ok(())
    }

    /// Get the status of an in-flight broadcast.
    pub async fn get_status(&self, index: &str) -> Option<BroadcastStatus> {
        self.in_flight.read().await.get(index).cloned()
    }

    /// Check if a broadcast is in-flight for an index.
    pub async fn is_in_flight(&self, index: &str) -> bool {
        self.in_flight.read().await.contains_key(index)
    }
}

impl Default for SettingsBroadcast {
    fn default() -> Self {
        Self::new()
    }
}

/// Settings broadcast coordinator with leader-only singleton coordination (plan §14.5).
///
/// Acquires a per-index leader lease (scope: "settings_broadcast:<index>") and persists
/// phase state so that a new leader can resume from the last committed phase.
pub struct SettingsBroadcastCoordinator {
    /// Mode B operation leader with phase state persistence.
    leader: ModeBOpLeader<SettingsBroadcastExtraState>,
}

/// Extra state for settings broadcast operations persisted to mode_b_operations.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SettingsBroadcastExtraState {
    /// Proposed settings fingerprint.
    pub proposed_fingerprint: Option<String>,
    /// Per-node task UIDs from Phase 1 (propose).
    pub node_task_uids: HashMap<String, u64>,
    /// Per-node verification results from Phase 2 (verify).
    pub node_hashes: HashMap<String, String>,
    /// Settings version after commit.
    pub settings_version: Option<u64>,
    /// Index UID for this broadcast.
    pub index_uid: String,
}

impl SettingsBroadcastCoordinator {
    /// Create a new settings broadcast coordinator.
    pub fn new(
        leader_election: Arc<LeaderElection>,
        task_store: Arc<dyn TaskStore>,
        index_uid: String,
        pod_id: String,
    ) -> Self {
        let scope = format!("settings_broadcast:{index_uid}");

        let extra_state = SettingsBroadcastExtraState {
            index_uid,
            ..Default::default()
        };

        let leader = ModeBOpLeader::new(
            leader_election,
            task_store,
            crate::task_store::mode_b_type::SETTINGS_BROADCAST.to_string(),
            scope,
            pod_id,
            extra_state,
        );

        Self { leader }
    }

    /// Try to acquire leadership for this settings broadcast.
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
    pub fn extra_state(&mut self) -> &mut SettingsBroadcastExtraState {
        self.leader.extra_state()
    }

    /// Get the extra state (immutable).
    pub fn extra_state_ref(&self) -> &SettingsBroadcastExtraState {
        self.leader.extra_state_ref()
    }

    /// Advance to the next phase and persist state.
    ///
    /// Should be called after each phase boundary so that a new leader can
    /// resume from the last committed phase.
    pub async fn advance_phase(&mut self, new_phase: BroadcastPhase) -> Result<()> {
        let phase_name = format!("{new_phase:?}");
        self.leader.persist_phase(phase_name.to_lowercase()).await
    }

    /// Start Phase 1: Propose.
    pub async fn start_propose(&mut self, settings: &Value) -> Result<()> {
        let fp = fingerprint_settings(settings);
        self.leader.extra_state().proposed_fingerprint = Some(fp);
        self.leader.persist_phase("propose".to_string()).await
    }

    /// Enter Phase 2: Verify with node task UIDs.
    pub async fn enter_verify(&mut self, node_task_uids: HashMap<String, u64>) -> Result<()> {
        self.leader.extra_state().node_task_uids = node_task_uids;
        self.leader.persist_phase("verify".to_string()).await
    }

    /// Verify per-node settings hashes.
    pub async fn verify_hashes(&mut self, node_hashes: HashMap<String, String>) -> Result<()> {
        // Check all hashes match the proposed fingerprint
        if let Some(ref expected) = self.leader.extra_state_ref().proposed_fingerprint {
            for (node, hash) in &node_hashes {
                if hash != expected {
                    return Err(MiroirError::SettingsDivergence);
                }
            }
        }

        self.leader.extra_state().node_hashes = node_hashes;
        self.leader.persist_phase("verify".to_string()).await
    }

    /// Enter Phase 3: Commit.
    pub async fn commit(&mut self, new_version: u64) -> Result<()> {
        self.leader.extra_state().settings_version = Some(new_version);
        self.leader.persist_phase("commit".to_string()).await
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
    pub async fn recover(&mut self) -> Result<Option<BroadcastPhase>> {
        let existing = self.leader.recover().await?;

        if let Some(ref op) = existing {
            // Parse phase string back to BroadcastPhase enum
            let phase = match op.phase.to_lowercase().as_str() {
                "idle" => BroadcastPhase::Idle,
                "propose" => BroadcastPhase::Propose,
                "verify" => BroadcastPhase::Verify,
                "commit" => BroadcastPhase::Commit,
                _ => BroadcastPhase::Idle,
            };

            info!(
                index_uid = %self.leader.extra_state_ref().index_uid,
                phase = %op.phase,
                "recovered settings broadcast from persisted phase"
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

/// Get current time in milliseconds since Unix epoch.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Compute a fingerprint (SHA256) of settings as canonical JSON.
/// Exported for use by the proxy layer during two-phase broadcast verification.
pub fn fingerprint_settings(settings: &Value) -> String {
    // Canonicalize: sort object keys, no extra whitespace.
    let canonical = if settings.is_object() {
        if let Some(obj) = settings.as_object() {
            // Collect and sort keys.
            let mut sorted_entries: Vec<_> = obj.iter().collect();
            sorted_entries.sort_by_key(|&(k, _)| k);
            // Reconstruct as a Map with sorted keys.
            let mut sorted_map = serde_json::Map::new();
            for (key, value) in sorted_entries {
                sorted_map.insert(key.clone(), value.clone());
            }
            serde_json::to_string(&sorted_map).unwrap_or_default()
        } else {
            serde_json::to_string(settings).unwrap_or_default()
        }
    } else {
        serde_json::to_string(settings).unwrap_or_default()
    };

    // SHA256 hash.
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_fingerprint_settings() {
        let settings1 = json!({"rankingRules": ["words", "typo"], "stopWords": ["the", "a"]});
        let settings2 = json!({"stopWords": ["the", "a"], "rankingRules": ["words", "typo"]});

        // Order-independent canonicalization should produce same fingerprint.
        let fp1 = fingerprint_settings(&settings1);
        let fp2 = fingerprint_settings(&settings2);
        assert_eq!(fp1, fp2);
    }

    #[tokio::test]
    async fn test_broadcast_full_flow() {
        let broadcast = SettingsBroadcast::new();

        // Start propose.
        let index = "products".to_string();
        let settings = json!({"rankingRules": ["words"]});
        let fp = fingerprint_settings(&settings);
        broadcast
            .start_propose(index.clone(), &settings)
            .await
            .unwrap();

        // Enter verify with node task UIDs.
        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-1".to_string(), 100);
        node_tasks.insert("node-2".to_string(), 101);
        broadcast.enter_verify(&index, node_tasks).await.unwrap();

        // Verify hashes.
        let mut node_hashes = HashMap::new();
        node_hashes.insert("node-1".to_string(), fp.clone());
        node_hashes.insert("node-2".to_string(), fp.clone());
        broadcast
            .verify_hashes(&index, node_hashes, &fp)
            .await
            .unwrap();

        // Commit.
        let new_version = broadcast.commit(&index).await.unwrap();
        assert_eq!(new_version, 1);

        // Complete.
        broadcast.complete(&index).await.unwrap();
        assert!(!broadcast.is_in_flight(&index).await);
    }

    #[tokio::test]
    async fn test_broadcast_hash_mismatch() {
        let broadcast = SettingsBroadcast::new();

        let index = "products".to_string();
        let settings = json!({"rankingRules": ["words"]});
        let fp = fingerprint_settings(&settings);
        broadcast
            .start_propose(index.clone(), &settings)
            .await
            .unwrap();

        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-1".to_string(), 100);
        broadcast.enter_verify(&index, node_tasks).await.unwrap();

        // Wrong hash.
        let mut node_hashes = HashMap::new();
        node_hashes.insert("node-1".to_string(), "wrong_hash".to_string());
        let result = broadcast.verify_hashes(&index, node_hashes, &fp).await;
        assert!(matches!(result, Err(MiroirError::SettingsDivergence)));

        // Status should reflect the error.
        let status = broadcast.get_status(&index).await;
        assert!(status.unwrap().error.is_some());
    }

    #[tokio::test]
    async fn test_node_version_tracking() {
        let broadcast = SettingsBroadcast::new();

        // Initially zero.
        assert_eq!(broadcast.node_version("products", "node-1").await, 0);

        // After commit, version should be tracked.
        let index = "products".to_string();
        let settings = json!({"rankingRules": ["words"]});
        let fp = fingerprint_settings(&settings);
        broadcast
            .start_propose(index.clone(), &settings)
            .await
            .unwrap();

        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-1".to_string(), 100);
        broadcast.enter_verify(&index, node_tasks).await.unwrap();

        let mut node_hashes = HashMap::new();
        node_hashes.insert("node-1".to_string(), fp.clone());
        broadcast
            .verify_hashes(&index, node_hashes, &fp)
            .await
            .unwrap();

        broadcast.commit(&index).await.unwrap();

        // Node version should now be 1.
        assert_eq!(broadcast.node_version("products", "node-1").await, 1);
    }
}
