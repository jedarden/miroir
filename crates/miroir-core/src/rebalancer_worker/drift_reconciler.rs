//! Settings drift reconciler background task (plan §13.5).
//!
//! Detects and repairs settings drift across nodes:
//! - Runs as Mode B leader for the broadcast
//! - Mode A rendezvous-partitioned for the drift check (plan §14.6)
//! - Every `settings_drift_check.interval_s` (default 5 min), hash each node's settings and repair mismatches
//! - Catches out-of-band changes (operator SSH'd to a node and called PATCH directly)

use crate::error::{MiroirError, Result};
use crate::settings::{fingerprint_settings, SettingsBroadcast};
use crate::task_store::TaskStore;
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
    /// Leader lease TTL in seconds.
    pub lease_ttl_secs: u64,
    /// Lease renewal interval in milliseconds.
    pub lease_renewal_interval_ms: u64,
}

impl Default for DriftReconcilerConfig {
    fn default() -> Self {
        Self {
            interval_s: 300,     // 5 minutes
            auto_repair: true,
            lease_ttl_secs: 10,
            lease_renewal_interval_ms: 2000,
        }
    }
}

/// Settings drift reconciler background worker.
///
/// Runs as a Tokio task, acquires a leader lease, and periodically checks
/// for settings drift across all nodes for all indexes.
pub struct DriftReconciler {
    config: DriftReconcilerConfig,
    settings_broadcast: Arc<SettingsBroadcast>,
    task_store: Arc<dyn TaskStore>,
    node_addresses: Vec<String>,
    node_master_key: String,
    pod_id: String,
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
        }
    }

    /// Start the background worker.
    ///
    /// This runs in a loop:
    /// 1. Try to acquire leader lease (scope: drift_reconciler)
    /// 2. If acquired, run drift checks and repairs
    /// 3. Renew lease periodically
    /// 4. If lease lost, go back to step 1
    pub async fn run(&self) {
        info!(
            pod_id = %self.pod_id,
            "drift reconciler starting"
        );

        let scope = "drift_reconciler";
        let client = Client::new();

        loop {
            let now_ms = now_ms();
            let expires_at = now_ms + (self.config.lease_ttl_secs * 1000) as i64;

            // Try to acquire leader lease
            match tokio::task::spawn_blocking({
                let task_store = self.task_store.clone();
                let scope = scope.to_string();
                let pod_id = self.pod_id.clone();
                move || {
                    task_store.try_acquire_leader_lease(&scope, &pod_id, expires_at, now_ms)
                }
            })
            .await
            {
                Ok(Ok(true)) => {
                    info!(scope = %scope, pod_id = %self.pod_id, "acquired leader lease");

                    // We are the leader - run drift check cycle
                    if let Err(e) = self.run_check_cycle(&client).await {
                        error!(error = %e, "drift check cycle failed");
                    }
                }
                Ok(Ok(false)) => {
                    debug!(scope = %scope, "leader lease already held");
                }
                Ok(Err(e)) => {
                    error!(scope = %scope, error = %e, "failed to acquire leader lease");
                }
                Err(e) => {
                    error!(scope = %scope, error = %e, "spawn_blocking task failed");
                }
            }

            // Wait before retrying
            tokio::time::sleep(Duration::from_millis(
                self.config.lease_renewal_interval_ms,
            ))
            .await;
        }
    }

    /// Run a single drift check and repair cycle.
    async fn run_check_cycle(&self, client: &Client) -> Result<()> {
        let scope = "drift_reconciler";
        let mut lease_renewal = tokio::time::interval(Duration::from_millis(
            self.config.lease_renewal_interval_ms,
        ));

        // Run drift check immediately on acquiring lease
        self.check_and_repair_all_indexes(client).await?;

        // Then wait for interval or lease expiry
        let check_interval = tokio::time::sleep(Duration::from_secs(self.config.interval_s));

        tokio::select! {
            _ = lease_renewal.tick() => {
                // Renew lease
                let now_ms = now_ms();
                let expires_at = now_ms + (self.config.lease_ttl_secs * 1000) as i64;

                match tokio::task::spawn_blocking({
                    let task_store = self.task_store.clone();
                    let scope = scope.to_string();
                    let pod_id = self.pod_id.clone();
                    move || {
                        task_store.renew_leader_lease(&scope, &pod_id, expires_at)
                    }
                })
                .await
                {
                    Ok(Ok(true)) => {
                        debug!(scope = %scope, "renewed leader lease");
                    }
                    Ok(Ok(false)) => {
                        info!(scope = %scope, "lost leader lease");
                        return Ok(());
                    }
                    Ok(Err(e)) => {
                        error!(scope = %scope, error = %e, "failed to renew leader lease");
                        return Err(e.into());
                    }
                    Err(e) => {
                        error!(scope = %scope, error = %e, "spawn_blocking task failed");
                        return Err(MiroirError::InvalidState(format!("spawn_blocking task failed: {}", e)));
                    }
                }
            }
            _ = check_interval => {
                // Interval passed - run drift check
                self.check_and_repair_all_indexes(client).await?;
            }
        }

        Ok(())
    }

    /// Check all indexes for drift and repair if needed.
    async fn check_and_repair_all_indexes(&self, client: &Client) -> Result<()> {
        // Get all indexes from the first node
        let first_address = self.node_addresses.first()
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
    async fn check_and_repair_index(&self, client: &Client, index: &str) -> Result<()> {
        // Get settings from all nodes
        let mut node_settings: HashMap<String, Value> = HashMap::new();
        let mut node_hashes: HashMap<String, String> = HashMap::new();

        for address in &self.node_addresses {
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
            warn!(index = %index, "no nodes returned settings, skipping drift check");
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
                        if let Err(e) = self.repair_node_settings(client, address, index, &consensus_settings).await {
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
        assert_eq!(config.lease_ttl_secs, 10);
        assert_eq!(config.lease_renewal_interval_ms, 2000);
    }
}
