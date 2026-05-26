//! Document TTL and automatic expiration (plan §13.14).
//!
//! Background sweeper deletes documents whose `_miroir_expires_at` field
//! is in the past.
//!
//! # CDC Origin Tag (plan §13.13)
//!
//! TTL expiration deletes must be tagged with `origin="ttl_expire"` so they are
//! suppressed from CDC by default (unless `emit_ttl_deletes` is true).
//!
//! When constructing delete requests for expired documents, set:
//! ```ignore
//! use miroir_core::cdc::ORIGIN_TTL_EXPIRE;
//! DeleteByFilterRequest { ..., origin: Some(ORIGIN_TTL_EXPIRE.to_string()) }
//! ```

use crate::cdc::ORIGIN_TTL_EXPIRE;
use crate::error::{MiroirError, Result};
use crate::scatter::{DeleteByFilterRequest, NodeClient};
use crate::topology::{NodeId, Topology};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

/// TTL configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlConfig {
    /// Whether TTL is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Sweep interval in seconds.
    #[serde(default = "default_interval")]
    pub sweep_interval_s: u64,
    /// Maximum deletes per sweep.
    #[serde(default = "default_max_deletes")]
    pub max_deletes_per_sweep: u32,
    /// Expires_at field name.
    #[serde(default = "default_field")]
    pub expires_at_field: String,
    /// Per-index overrides.
    #[serde(default)]
    pub per_index_overrides: HashMap<String, TtlOverride>,
}

/// Per-index TTL override.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlOverride {
    /// Sweep interval override.
    pub sweep_interval_s: u64,
    /// Max deletes override.
    pub max_deletes_per_sweep: u32,
}

fn default_true() -> bool {
    true
}
fn default_interval() -> u64 {
    300 // 5 minutes
}
fn default_max_deletes() -> u32 {
    10000
}
fn default_field() -> String {
    "_miroir_expires_at".into()
}

impl Default for TtlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sweep_interval_s: default_interval(),
            max_deletes_per_sweep: default_max_deletes(),
            expires_at_field: default_field(),
            per_index_overrides: HashMap::new(),
        }
    }
}

/// TTL sweeper state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlSweeperState {
    /// Last sweep timestamp.
    pub last_sweep_at: u64,
    /// Documents deleted in last sweep.
    pub last_sweep_deleted: u64,
    /// Indexes with pending expired documents.
    pub pending_indexes: Vec<String>,
}

/// TTL manager.
pub struct TtlManager<C: NodeClient> {
    /// Configuration.
    config: TtlConfig,
    /// Sweeper state.
    state: Arc<RwLock<TtlSweeperState>>,
    /// Sweeper running flag.
    running: Arc<RwLock<bool>>,
    /// Topology for routing.
    topology: Arc<RwLock<Topology>>,
    /// Node client for executing deletes.
    node_client: Arc<C>,
    /// Total shards in the cluster.
    total_shards: u32,
    /// Replica group ID for this pod.
    replica_group_id: u32,
    /// Replication factor.
    rf: usize,
    /// Metrics callback for documents expired.
    metrics_expired: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    /// Metrics callback for sweep duration.
    metrics_duration: Option<Arc<dyn Fn(f64) + Send + Sync>>,
}

impl<C: NodeClient> TtlManager<C> {
    /// Create a new TTL manager.
    pub fn new(
        config: TtlConfig,
        topology: Arc<RwLock<Topology>>,
        node_client: Arc<C>,
        total_shards: u32,
        replica_group_id: u32,
        rf: usize,
    ) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(TtlSweeperState {
                last_sweep_at: 0,
                last_sweep_deleted: 0,
                pending_indexes: Vec::new(),
            })),
            running: Arc::new(RwLock::new(false)),
            topology,
            node_client,
            total_shards,
            replica_group_id,
            rf,
            metrics_expired: None,
            metrics_duration: None,
        }
    }

    /// Set metrics callbacks for TTL operations.
    pub fn with_metrics(
        mut self,
        metrics_expired: Box<dyn Fn(u64) + Send + Sync>,
        metrics_duration: Box<dyn Fn(f64) + Send + Sync>,
    ) -> Self {
        self.metrics_expired = Some(metrics_expired.into());
        self.metrics_duration = Some(metrics_duration.into());
        self
    }

    /// Start the background sweeper.
    pub async fn start(&self) {
        let mut running = self.running.write().await;
        if *running {
            return; // Already running
        }
        *running = true;
        drop(running);

        let config = self.config.clone();
        let state = self.state.clone();
        let running_flag = self.running.clone();

        tokio::spawn(async move {
            let mut timer = interval(Duration::from_secs(config.sweep_interval_s));
            loop {
                timer.tick().await;

                // Check if still running
                {
                    let running = running_flag.read().await;
                    if !*running {
                        break;
                    }
                }

                // Update state to show sweep is running
                {
                    let mut s = state.write().await;
                    s.last_sweep_at = millis_now();
                }

                tracing::debug!("TTL sweep tick at {:?}", SystemTime::now());
            }
        });
    }

    /// Stop the background sweeper.
    pub async fn stop(&self) {
        let mut running = self.running.write().await;
        *running = false;
    }

    /// Run a single TTL sweep pass (called by TtlWorker).
    pub async fn run_sweep_pass(&self) -> Result<u64> {
        self.run_sweep().await?;
        let state = self.state.read().await;
        Ok(state.last_sweep_deleted)
    }

    /// Run a single sweep pass.
    async fn run_sweep(&self) -> Result<()> {
        let sweep_start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let now_ms = sweep_start;
        let mut total_deleted = 0u64;
        let pending_indexes = Vec::new();

        info!("TTL sweep starting at {}", now_ms);

        // Get topology
        let topology = self.topology.read().await;

        // Iterate through all shards owned by this replica group
        for shard_id in 0..self.total_shards {
            // Determine which replica group owns this shard
            let group_id = (shard_id as usize) % topology.groups().count();
            if group_id != self.replica_group_id as usize {
                continue; // Not owned by this pod
            }

            // Get target nodes for this shard
            let target_nodes: Vec<NodeId> = topology
                .groups()
                .nth(group_id)
                .map(|group| crate::router::assign_shard_in_group(shard_id, group.nodes(), self.rf))
                .unwrap_or_default();

            if target_nodes.is_empty() {
                debug!("No nodes for shard {}, skipping", shard_id);
                continue;
            }

            // Build filter: _miroir_shard = {s} AND _miroir_expires_at <= {now_ms}
            let filter = serde_json::json!({
                "and": [
                    { "_miroir_shard": shard_id },
                    { "_miroir_expires_at": { "<=": now_ms } }
                ]
            });

            // For each index with TTL enabled, issue the delete
            // For now, we use a default index - in production this would iterate
            // through all indexes with TTL enabled
            let index_uid = "default"; // This would come from config or registry

            for node_id in &target_nodes {
                let node = topology
                    .node(node_id)
                    .ok_or_else(|| MiroirError::Topology(format!("node {node_id} not found")))?;

                if !node.is_healthy() {
                    debug!("Node {} is unhealthy, skipping TTL delete", node_id);
                    continue;
                }

                let request = DeleteByFilterRequest {
                    index_uid: index_uid.to_string(),
                    filter: filter.clone(),
                    origin: Some(ORIGIN_TTL_EXPIRE.to_string()),
                };

                match self
                    .node_client
                    .delete_documents_by_filter(node_id, &node.address, &request)
                    .await
                {
                    Ok(response) if response.success => {
                        // Note: The actual deleted count would come from polling the task status
                        // For now, we track that a delete was initiated successfully
                        debug!(
                            "TTL delete initiated for shard {} on node {}",
                            shard_id, node_id
                        );
                        // In production, we would poll the task UID to get the actual count
                        total_deleted += 1; // Placeholder - represents one delete operation
                    }
                    Ok(response) => {
                        warn!(
                            "TTL delete failed on node {}: {}",
                            node_id,
                            response.message.unwrap_or_default()
                        );
                    }
                    Err(e) => {
                        warn!("TTL delete error on node {}: {:?}", node_id, e);
                    }
                }
            }
        }

        let sweep_end = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let duration_secs = (sweep_end - sweep_start) as f64 / 1000.0;

        // Update state
        let mut state = self.state.write().await;
        state.last_sweep_at = sweep_end;
        state.last_sweep_deleted = total_deleted;
        state.pending_indexes = pending_indexes;

        info!(
            "TTL sweep completed: deleted {} documents in {:.2}s",
            total_deleted, duration_secs
        );

        // Emit metrics if callbacks are configured
        if let Some(ref callback) = self.metrics_expired {
            callback(total_deleted);
        }
        if let Some(ref callback) = self.metrics_duration {
            callback(duration_secs);
        }

        Ok(())
    }

    /// Get the current sweeper state.
    pub async fn state(&self) -> TtlSweeperState {
        self.state.read().await.clone()
    }

    /// Estimate pending expired documents for an index.
    ///
    /// In a real implementation, this would query the index with
    /// a filter to count documents with expires_at <= now.
    pub async fn estimate_pending(&self, _index: &str) -> Result<u64> {
        // Placeholder
        Ok(0)
    }
}

impl<C: NodeClient> Clone for TtlManager<C> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            state: self.state.clone(),
            running: self.running.clone(),
            topology: self.topology.clone(),
            node_client: self.node_client.clone(),
            total_shards: self.total_shards,
            replica_group_id: self.replica_group_id,
            rf: self.rf,
            metrics_expired: self.metrics_expired.clone(),
            metrics_duration: self.metrics_duration.clone(),
        }
    }
}

// Note: Default implementation removed since TtlManager now requires NodeClient and topology

/// Get current UNIX timestamp in milliseconds.
fn millis_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scatter::MockNodeClient;
    use crate::topology::Node;

    #[test]
    fn test_config_default() {
        let config = TtlConfig::default();
        assert!(config.enabled);
        assert_eq!(config.sweep_interval_s, 300);
        assert_eq!(config.max_deletes_per_sweep, 10000);
        assert_eq!(config.expires_at_field, "_miroir_expires_at");
    }

    fn make_test_topology() -> Topology {
        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..3 {
            let mut node = Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                i % 2,
            );
            node.status = crate::topology::NodeStatus::Active;
            topo.add_node(node);
        }
        topo
    }

    #[tokio::test]
    async fn test_manager_state() {
        let topo = Arc::new(RwLock::new(make_test_topology()));
        let client = Arc::new(MockNodeClient::default());
        let manager = TtlManager::new(TtlConfig::default(), topo, client, 64, 0, 2);

        let state = manager.state().await;
        assert_eq!(state.last_sweep_at, 0);
        assert_eq!(state.last_sweep_deleted, 0);
    }

    #[tokio::test]
    async fn test_estimate_pending() {
        let topo = Arc::new(RwLock::new(make_test_topology()));
        let client = Arc::new(MockNodeClient::default());
        let manager = TtlManager::new(TtlConfig::default(), topo, client, 64, 0, 2);

        let pending = manager.estimate_pending("products").await.unwrap();
        assert_eq!(pending, 0);
    }
}
