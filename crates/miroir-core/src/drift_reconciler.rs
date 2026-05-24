//! Drift reconciler background worker (plan §13.5).
//!
//! Detects and repairs settings drift across nodes caused by out-of-band changes
//! (e.g., operator SSH'd to a node and called PATCH directly).
//!
//! Runs every `settings_drift_check.interval_s` seconds (default 5 min), hashing
//! each node's settings and repairing mismatches. Uses Mode B leader election
//! for horizontal scaling.

use crate::error::{MiroirError, Result};
use crate::settings::fingerprint_settings;
use crate::task_store::TaskStore;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Callback type for recording drift repair metrics.
pub type DriftRepairMetrics = Arc<dyn Fn(&str, &str) + Send + Sync>;

/// Configuration for the drift reconciler.
#[derive(Clone)]
pub struct DriftReconcilerConfig {
    /// Check interval in seconds.
    pub interval_s: u64,
    /// Whether to auto-repair detected drift.
    pub auto_repair: bool,
    /// Node master key for authentication.
    pub node_master_key: String,
    /// Node addresses to check.
    pub node_addresses: Vec<String>,
    /// Leader election scope for Mode B scaling.
    pub leader_scope: String,
    /// This pod's ID for leader election.
    pub pod_id: String,
}

/// Drift reconciler background worker.
pub struct DriftReconciler {
    config: DriftReconcilerConfig,
    client: Client,
    task_store: Arc<dyn TaskStore>,
    /// Indexes to check (empty = all indexes).
    indexes: Arc<RwLock<Vec<String>>>,
    /// Callback for recording drift repair metrics.
    metrics_callback: Option<DriftRepairMetrics>,
}

impl DriftReconciler {
    /// Create a new drift reconciler.
    pub fn new(config: DriftReconcilerConfig, task_store: Arc<dyn TaskStore>) -> Self {
        Self::with_metrics(config, task_store, None)
    }

    /// Create a new drift reconciler with metrics callback.
    pub fn with_metrics(
        config: DriftReconcilerConfig,
        task_store: Arc<dyn TaskStore>,
        metrics_callback: Option<DriftRepairMetrics>,
    ) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            config,
            client,
            task_store,
            indexes: Arc::new(RwLock::new(Vec::new())),
            metrics_callback,
        }
    }

    /// Start the drift reconciler background task.
    pub async fn run(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(self.config.interval_s));
        let mut leader_election_interval = tokio::time::interval(Duration::from_secs(3));

        info!(
            interval_s = self.config.interval_s,
            auto_repair = self.config.auto_repair,
            "drift reconciler started"
        );

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if self.is_leader_async().await {
                        if let Err(e) = self.check_and_repair().await {
                            error!(error = %e, "drift check failed");
                        }
                    }
                }
                _ = leader_election_interval.tick() => {
                    // Renew leader lease
                    let _ = self.renew_leader_lease();
                }
            }
        }
    }

    /// Check if this pod is the leader (Mode B leader election).
    fn is_leader(&self) -> bool {
        let now = now_ms();
        let lease_ttl = now + (self.config.interval_s as i64 * 1000 * 2);

        self.task_store
            .try_acquire_leader_lease(
                &self.config.leader_scope,
                &self.config.pod_id,
                lease_ttl,
                now,
            )
            .unwrap_or(false)
    }

    /// Check if this pod is the leader asynchronously (for use in async context).
    async fn is_leader_async(&self) -> bool {
        self.is_leader()
    }

    /// Renew the leader lease.
    fn renew_leader_lease(&self) {
        let now = now_ms();
        let lease_ttl = now + (self.config.interval_s as i64 * 1000 * 2);

        let _ = self.task_store.renew_leader_lease(
            &self.config.leader_scope,
            &self.config.pod_id,
            lease_ttl,
        );
    }

    /// Check all nodes for drift and repair if configured.
    async fn check_and_repair(&self) -> Result<()> {
        debug!("starting drift check");

        // Get list of indexes to check (from first node)
        let indexes = self.list_indexes().await?;
        let indexes_to_check: Vec<_> = if self.indexes.read().await.is_empty() {
            indexes
        } else {
            let filter = self.indexes.read().await.clone();
            indexes.into_iter().filter(|i| filter.contains(i)).collect()
        };

        let mut total_mismatches = 0u64;
        let mut total_repairs = 0u64;

        for index in &indexes_to_check {
            match self.check_index_drift(index).await? {
                DriftCheckResult::NoDrift => {
                    debug!(index = %index, "no drift detected");
                }
                DriftCheckResult::DriftDetected { mismatches } => {
                    total_mismatches += mismatches.len() as u64;
                    warn!(
                        index = %index,
                        mismatches = mismatches.len(),
                        "drift detected"
                    );

                    if self.config.auto_repair {
                        for (node_id, address) in &mismatches {
                            match self.repair_node_settings(index, address, node_id).await {
                                Ok(_) => {
                                    total_repairs += 1;
                                    info!(index = %index, node = %node_id, "drift repaired");
                                }
                                Err(e) => {
                                    error!(index = %index, node = %node_id, error = %e, "drift repair failed");
                                }
                            }
                        }
                    }
                }
                DriftCheckResult::Error(e) => {
                    error!(index = %index, error = %e, "drift check error");
                }
            }
        }

        if total_mismatches > 0 {
            info!(total_mismatches, total_repairs, "drift check complete");
        }

        Ok(())
    }

    /// List all indexes from the first node.
    async fn list_indexes(&self) -> Result<Vec<String>> {
        let first_address = self
            .config
            .node_addresses
            .first()
            .ok_or_else(|| MiroirError::Topology("no nodes configured".into()))?;

        let url = format!("{}/indexes", first_address.trim_end_matches('/'));
        let response = self
            .client
            .get(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.config.node_master_key),
            )
            .send()
            .await
            .map_err(|e| MiroirError::Task(format!("failed to list indexes: {}", e)))?;

        if !response.status().is_success() {
            return Err(MiroirError::Task(format!(
                "failed to list indexes: HTTP {}",
                response.status()
            )));
        }

        let json: Value = response
            .json()
            .await
            .map_err(|e| MiroirError::Task(format!("failed to parse indexes: {}", e)))?;

        let results = json
            .get("results")
            .and_then(|v| v.as_array())
            .ok_or_else(|| MiroirError::Task("invalid indexes response".into()))?;

        Ok(results
            .iter()
            .filter_map(|v| {
                v.get("uid")
                    .and_then(|uid| uid.as_str())
                    .map(|s| s.to_string())
            })
            .collect())
    }

    /// Check a single index for drift across all nodes.
    async fn check_index_drift(&self, index: &str) -> Result<DriftCheckResult> {
        let mut node_settings: Vec<(String, String, Value)> = Vec::new();

        // Fetch settings from all nodes
        for (node_id, address) in self.node_addresses_with_ids() {
            let url = format!(
                "{}/indexes/{}/settings",
                address.trim_end_matches('/'),
                index
            );
            match self
                .client
                .get(&url)
                .header(
                    "Authorization",
                    format!("Bearer {}", self.config.node_master_key),
                )
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    if let Ok(settings) = resp.json::<Value>().await {
                        node_settings.push((node_id, address, settings));
                    }
                }
                Ok(resp) => {
                    return Ok(DriftCheckResult::Error(MiroirError::Task(format!(
                        "node {} returned HTTP {}",
                        node_id,
                        resp.status()
                    ))));
                }
                Err(e) => {
                    return Ok(DriftCheckResult::Error(MiroirError::Task(format!(
                        "node {} request failed: {}",
                        node_id, e
                    ))));
                }
            }
        }

        if node_settings.is_empty() {
            return Ok(DriftCheckResult::NoDrift);
        }

        // Compute fingerprint for each node's settings
        let mut fingerprints: Vec<(String, String, String)> = Vec::new();
        for (node_id, address, settings) in &node_settings {
            let fp = fingerprint_settings(settings);
            fingerprints.push((node_id.clone(), address.clone(), fp));
        }

        // Check for mismatches (compare all to first node's fingerprint)
        let first_fp = &fingerprints
            .first()
            .ok_or_else(|| MiroirError::Task("no fingerprints".into()))?
            .2;
        let mismatches: Vec<(String, String)> = fingerprints
            .iter()
            .filter(|(_, _, fp)| fp != first_fp)
            .map(|(node_id, address, _)| (node_id.clone(), address.clone()))
            .collect();

        if mismatches.is_empty() {
            Ok(DriftCheckResult::NoDrift)
        } else {
            Ok(DriftCheckResult::DriftDetected { mismatches })
        }
    }

    /// Repair settings on a drifted node by copying from the first node.
    async fn repair_node_settings(
        &self,
        index: &str,
        drifted_address: &str,
        drifted_node_id: &str,
    ) -> Result<()> {
        // Get correct settings from the first healthy node
        let first_address = self
            .config
            .node_addresses
            .first()
            .ok_or_else(|| MiroirError::Topology("no nodes configured".into()))?;

        let url = format!(
            "{}/indexes/{}/settings",
            first_address.trim_end_matches('/'),
            index
        );
        let response = self
            .client
            .get(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.config.node_master_key),
            )
            .send()
            .await
            .map_err(|e| {
                MiroirError::Task(format!("failed to fetch settings for repair: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(MiroirError::Task(format!(
                "failed to fetch settings for repair: HTTP {}",
                response.status()
            )));
        }

        let correct_settings: Value = response.json().await.map_err(|e| {
            MiroirError::Task(format!("failed to parse settings for repair: {}", e))
        })?;

        // PATCH the drifted node with correct settings
        let patch_url = format!(
            "{}/indexes/{}/settings",
            drifted_address.trim_end_matches('/'),
            index
        );
        let patch_response = self
            .client
            .patch(&patch_url)
            .header(
                "Authorization",
                format!("Bearer {}", self.config.node_master_key),
            )
            .json(&correct_settings)
            .send()
            .await
            .map_err(|e| MiroirError::Task(format!("failed to repair settings: {}", e)))?;

        if !patch_response.status().is_success() {
            return Err(MiroirError::Task(format!(
                "failed to repair settings: HTTP {}",
                patch_response.status()
            )));
        }

        // Record metrics if callback is set
        if let Some(ref callback) = self.metrics_callback {
            callback(index, drifted_node_id);
        }

        Ok(())
    }

    /// Get node addresses with their IDs.
    fn node_addresses_with_ids(&self) -> Vec<(String, String)> {
        self.config
            .node_addresses
            .iter()
            .enumerate()
            .map(|(i, addr)| (format!("node-{}", i), addr.clone()))
            .collect()
    }
}

/// Result of a drift check.
enum DriftCheckResult {
    NoDrift,
    DriftDetected { mismatches: Vec<(String, String)> },
    Error(MiroirError),
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

    #[test]
    fn test_node_addresses_with_ids() {
        let config = DriftReconcilerConfig {
            interval_s: 300,
            auto_repair: true,
            node_master_key: "test".to_string(),
            node_addresses: vec![
                "http://node1:7700".to_string(),
                "http://node2:7700".to_string(),
            ],
            leader_scope: "drift_reconciler".to_string(),
            pod_id: "pod-1".to_string(),
        };

        let reconciler = DriftReconciler::new(
            config,
            Arc::new(crate::task_store::SqliteTaskStore::open_in_memory().unwrap()),
        );

        let addresses = reconciler.node_addresses_with_ids();
        assert_eq!(addresses.len(), 2);
        assert_eq!(addresses[0].0, "node-0");
        assert_eq!(addresses[0].1, "http://node1:7700");
        assert_eq!(addresses[1].0, "node-1");
        assert_eq!(addresses[1].1, "http://node2:7700");
    }
}
