//! Two-phase settings broadcast with verification (plan §13.5).
//!
//! This module implements the propose/verify/commit flow for settings changes,
//! replacing the sequential apply-with-rollback approach.

use crate::error::{MiroirError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

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
}

impl SettingsBroadcast {
    /// Create a new settings broadcast coordinator.
    pub fn new() -> Self {
        Self {
            in_flight: Arc::new(RwLock::new(HashMap::new())),
            settings_version: Arc::new(RwLock::new(0)),
            node_settings_version: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the current global settings version.
    pub async fn current_version(&self) -> u64 {
        *self.settings_version.read().await
    }

    /// Get the per-(index, node) settings version.
    pub async fn node_version(&self, index: &str, node_id: &str) -> u64 {
        let versions = self.node_settings_version.read().await;
        *versions.get(&(index.to_string(), node_id.to_string())).unwrap_or(&0)
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
                "settings broadcast already in flight for index '{}'",
                index
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
        let status = in_flight.get_mut(index)
            .ok_or_else(|| MiroirError::NotFound(format!("index '{}'", index)))?;

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
        let status = in_flight.get_mut(index)
            .ok_or_else(|| MiroirError::NotFound(format!("index '{}'", index)))?;

        if status.phase != BroadcastPhase::Verify {
            return Err(MiroirError::InvalidState("expected Verify phase".into()));
        }

        status.node_hashes = node_hashes.clone();

        // Check all hashes match the expected fingerprint.
        for (node, hash) in &node_hashes {
            if hash != expected_fingerprint {
                status.error = Some(format!(
                    "node '{}' hash mismatch: expected {}, got {}",
                    node, expected_fingerprint, hash
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
        let status = in_flight.get_mut(index)
            .ok_or_else(|| MiroirError::NotFound(format!("index '{}'", index)))?;

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
        for node_id in status.node_hashes.keys() {
            node_versions.insert((index.to_string(), node_id.clone()), new_version);
        }

        status.phase = BroadcastPhase::Commit;
        status.settings_version = Some(new_version);

        // Remove from in-flight map after a short delay (caller should do this).
        Ok(new_version)
    }

    /// Complete the broadcast and remove from in-flight tracking.
    pub async fn complete(&self, index: &str) -> Result<()> {
        let mut in_flight = self.in_flight.write().await;
        in_flight.remove(index)
            .ok_or_else(|| MiroirError::NotFound(format!("index '{}'", index)))?;
        Ok(())
    }

    /// Abort a broadcast (on error).
    pub async fn abort(&self, index: &str, error: String) -> Result<()> {
        let mut in_flight = self.in_flight.write().await;
        if let Some(status) = in_flight.get_mut(index) {
            status.error = Some(error);
        }
        in_flight.remove(index)
            .ok_or_else(|| MiroirError::NotFound(format!("index '{}'", index)))?;
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

/// Compute a fingerprint (SHA256) of settings as canonical JSON.
fn fingerprint_settings(settings: &Value) -> String {
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
        broadcast.start_propose(index.clone(), &settings).await.unwrap();

        // Enter verify with node task UIDs.
        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-1".to_string(), 100);
        node_tasks.insert("node-2".to_string(), 101);
        broadcast.enter_verify(&index, node_tasks).await.unwrap();

        // Verify hashes.
        let mut node_hashes = HashMap::new();
        node_hashes.insert("node-1".to_string(), fp.clone());
        node_hashes.insert("node-2".to_string(), fp.clone());
        broadcast.verify_hashes(&index, node_hashes, &fp).await.unwrap();

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
        broadcast.start_propose(index.clone(), &settings).await.unwrap();

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
        broadcast.start_propose(index.clone(), &settings).await.unwrap();

        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-1".to_string(), 100);
        broadcast.enter_verify(&index, node_tasks).await.unwrap();

        let mut node_hashes = HashMap::new();
        node_hashes.insert("node-1".to_string(), fp.clone());
        broadcast.verify_hashes(&index, node_hashes, &fp).await.unwrap();

        broadcast.commit(&index).await.unwrap();

        // Node version should now be 1.
        assert_eq!(broadcast.node_version("products", "node-1").await, 1);
    }
}
