//! Settings drift reconciler background task (plan §13.5).
//!
//! Detects and repairs settings drift across nodes:
//! - Mode A rendezvous-partitioned for the drift check (plan §14.5, §14.6)
//! - Each pod polls a subset of (index, node) settings-hash pairs via rendezvous hashing
//! - Every `settings_drift_check.interval_s` (default 5 min), hash each node's settings and repair mismatches
//! - Catches out-of-band changes (operator SSH'd to a node and called PATCH directly)
//!
//! Mode A coordination: Each pod owns a subset of (index, node) pairs based on rendezvous hashing.
//! The pair key for rendezvous is "index_uid:node_address" to ensure even distribution.

use crate::error::{MiroirError, Result};
#[cfg(feature = "peer-discovery")]
use crate::mode_a_coordinator::ModeACoordinator as ActualModeACoordinator;
use crate::settings::{fingerprint_settings, SettingsBroadcast};
use crate::task_store::TaskStore;

// Type alias for ModeACoordinator that becomes a dummy type when feature is disabled
#[cfg(feature = "peer-discovery")]
type ModeACoordinator = ActualModeACoordinator;

#[cfg(not(feature = "peer-discovery"))]
struct ModeACoordinator;

#[cfg(not(feature = "peer-discovery"))]
impl ModeACoordinator {
    // Dummy methods for when peer-discovery is disabled
    pub async fn refresh_peers(&self) -> std::result::Result<usize, String> {
        Ok(1)
    }

    pub async fn owns_task(&self, _miroir_id: &str) -> std::result::Result<bool, String> {
        Ok(true)
    }
}
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Configuration for the drift reconciler worker.
#[derive(Debug, Clone)]
pub struct DriftReconcilerConfig {
    /// Interval between drift checks in seconds.
    pub interval_s: u64,
    /// Whether to automatically repair drift.
    pub auto_repair: bool,
}

impl Default for DriftReconcilerConfig {
    fn default() -> Self {
        Self {
            interval_s: 300, // 5 minutes
            auto_repair: true,
        }
    }
}

/// Settings drift reconciler background worker.
///
/// Runs as a Tokio task, uses Mode A rendezvous hashing to partition
/// drift checks across pods, and periodically checks for settings drift.
pub struct DriftReconciler {
    config: DriftReconcilerConfig,
    settings_broadcast: Arc<SettingsBroadcast>,
    task_store: Arc<dyn TaskStore>,
    node_addresses: Vec<String>,
    node_master_key: String,
    pod_id: String,
    /// Mode A coordinator for partitioning drift checks (plan §14.5 Mode A).
    mode_a_coordinator: Option<Arc<ModeACoordinator>>,
}

impl DriftReconciler {
    /// Create a new drift reconciler worker.
    pub fn new(
        config: DriftReconcilerConfig,
        settings_broadcast: Arc<SettingsBroadcast>,
        task_store: Arc<dyn TaskStore>,
        node_addresses: Vec<String>,
        node_master_key: String,
        pod_id: String,
    ) -> Self {
        Self {
            config,
            settings_broadcast,
            task_store,
            node_addresses,
            node_master_key,
            pod_id,
            mode_a_coordinator: None,
        }
    }

    /// Set the Mode A coordinator for partitioning drift checks (plan §14.5 Mode A).
    pub fn with_mode_a_coordinator(mut self, coordinator: Arc<ModeACoordinator>) -> Self {
        self.mode_a_coordinator = Some(coordinator);
        self
    }

    /// Start the background worker.
    ///
    /// This runs in a loop using Mode A coordination (plan §14.5):
    /// 1. Refresh peer set
    /// 2. Run drift checks on owned (index, node) pairs
    /// 3. Wait for configured interval
    /// 4. Repeat
    ///
    /// No leader election is used — each pod independently checks its
    /// rendezvous-owned (index, node) pairs.
    pub async fn run(&self) {
        info!(
            pod_id = %self.pod_id,
            "drift reconciler starting (Mode A coordination)"
        );

        let client = Client::new();
        let interval = Duration::from_secs(self.config.interval_s);

        loop {
            // Refresh peer set for Mode A coordination
            if let Some(ref coordinator) = self.mode_a_coordinator {
                match coordinator.refresh_peers().await {
                    Ok(peer_count) => {
                        debug!(peer_count, "refreshed peer set for drift reconciler");
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to refresh peer set, using cached peers");
                    }
                }
            }

            // Run drift check on owned pairs
            if let Err(e) = self.check_and_repair_all_indexes(&client).await {
                error!(error = %e, "drift check cycle failed");
            }

            // Wait for next interval
            tokio::time::sleep(interval).await;
        }
    }

    /// Check all indexes for drift and repair if needed.
    ///
    /// Uses Mode A coordination to filter (index, node) pairs to only those
    /// owned by this pod via rendezvous hashing.
    async fn check_and_repair_all_indexes(&self, client: &Client) -> Result<()> {
        // Get all indexes from the first node
        let first_address = self
            .node_addresses
            .first()
            .ok_or_else(|| MiroirError::InvalidState("no nodes configured".into()))?;

        let indexes = self.list_indexes(client, first_address).await?;

        // Check each index for drift
        for index in indexes {
            if let Err(e) = self.check_and_repair_index(client, &index).await {
                error!(index = %index, error = %e, "failed to check/repair index");
            }
        }

        Ok(())
    }

    /// Check a single index for drift and repair if needed.
    ///
    /// Uses Mode A coordination to only check (index, node) pairs owned by this pod.
    /// Each pair is keyed as "index_uid:node_address" for rendezvous hashing.
    async fn check_and_repair_index(&self, client: &Client, index: &str) -> Result<()> {
        // Get settings from all nodes
        let mut node_settings: HashMap<String, Value> = HashMap::new();
        let mut node_hashes: HashMap<String, String> = HashMap::new();

        for address in &self.node_addresses {
            // Mode A coordination: only check pairs we own
            // Key is "index_uid:node_address" for rendezvous hashing
            let pair_key = format!("{}:{}", index, address);

            if let Some(ref coordinator) = self.mode_a_coordinator {
                // Check if we own this (index, node) pair
                let owns_pair = coordinator.owns_task(&pair_key).await.unwrap_or(true); // Default to true if no coordinator
                if !owns_pair {
                    debug!(index = %index, node = %address, "skipping (index, node) pair not owned by this pod");
                    continue;
                }
            }

            let path = format!("/indexes/{}/settings", index);
            match self.get_settings(client, address, &path).await {
                Ok(settings) => {
                    let hash = fingerprint_settings(&settings);
                    node_settings.insert(address.clone(), settings);
                    node_hashes.insert(address.clone(), hash);
                }
                Err(e) => {
                    warn!(node = %address, index = %index, error = %e, "failed to get settings");
                }
            }
        }

        if node_settings.is_empty() {
            debug!(index = %index, "no nodes returned settings for owned pairs, skipping drift check");
            return Ok(());
        }

        // Find the most common hash (consensus)
        let mut hash_counts: HashMap<String, usize> = HashMap::new();
        for hash in node_hashes.values() {
            *hash_counts.entry(hash.clone()).or_insert(0) += 1;
        }

        let consensus_hash = hash_counts
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(hash, _)| hash);

        let consensus_hash = match consensus_hash {
            Some(hash) => hash,
            None => return Ok(()), // No consensus, can't determine drift
        };

        // Check for drift
        let mut drifted_nodes: Vec<String> = Vec::new();
        for (address, hash) in &node_hashes {
            if hash != &consensus_hash {
                drifted_nodes.push(address.clone());
            }
        }

        if !drifted_nodes.is_empty() {
            warn!(
                index = %index,
                drifted_nodes = ?drifted_nodes,
                "settings drift detected"
            );

            if self.config.auto_repair {
                // Get the consensus settings from a healthy node
                let consensus_settings = node_settings
                    .iter()
                    .find(|(_addr, settings)| {
                        let hash = fingerprint_settings(settings);
                        &hash == &consensus_hash
                    })
                    .map(|(_, settings)| settings);

                if let Some(consensus_settings) = consensus_settings {
                    // Repair drifted nodes
                    for address in &drifted_nodes {
                        if let Err(e) = self
                            .repair_node_settings(client, address, index, &consensus_settings)
                            .await
                        {
                            error!(node = %address, index = %index, error = %e, "failed to repair settings");
                        } else {
                            info!(node = %address, index = %index, "repaired settings drift");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Repair settings on a single node by applying the consensus settings.
    async fn repair_node_settings(
        &self,
        client: &Client,
        address: &str,
        index: &str,
        settings: &Value,
    ) -> Result<()> {
        let path = format!("/indexes/{}/settings", index);
        let url = format!("{}{}", address.trim_end_matches('/'), path);

        let response = client
            .patch(&url)
            .header("Authorization", format!("Bearer {}", self.node_master_key))
            .json(settings)
            .send()
            .await
            .map_err(|e| MiroirError::InvalidState(format!("request failed: {}", e)))?;

        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            Err(MiroirError::InvalidState(format!(
                "repair failed: HTTP {} — {}",
                status, text
            )))
        }
    }

    /// List all indexes from a node.
    async fn list_indexes(&self, client: &Client, address: &str) -> Result<Vec<String>> {
        let url = format!("{}/indexes", address.trim_end_matches('/'));

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.node_master_key))
            .send()
            .await
            .map_err(|e| MiroirError::InvalidState(format!("request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(MiroirError::InvalidState(format!(
                "list indexes failed: HTTP {}",
                response.status()
            )));
        }

        let json: Value = response
            .json()
            .await
            .map_err(|e| MiroirError::InvalidState(format!("parse response: {}", e)))?;

        let indexes = json
            .get("results")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.get("uid").and_then(|uid| uid.as_str()))
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Ok(indexes)
    }

    /// Get settings from a node.
    async fn get_settings(&self, client: &Client, address: &str, path: &str) -> Result<Value> {
        let url = format!("{}{}", address.trim_end_matches('/'), path);

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.node_master_key))
            .send()
            .await
            .map_err(|e| MiroirError::InvalidState(format!("request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(MiroirError::InvalidState(format!(
                "get settings failed: HTTP {}",
                response.status()
            )));
        }

        response
            .json()
            .await
            .map_err(|e| MiroirError::InvalidState(format!("parse response: {}", e)))
    }
}

/// Get current time in milliseconds since Unix epoch.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_drift_reconciler_config_default() {
        let config = DriftReconcilerConfig::default();
        assert_eq!(config.interval_s, 300);
        assert!(config.auto_repair);
    }
}
