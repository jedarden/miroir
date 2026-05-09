//! Shared application state for the proxy server.

use miroir_core::config::MiroirConfig;
use miroir_core::topology::{Node, NodeId, NodeStatus, Topology};
use miroir_core::{MiroirError, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::client::NodeClient;
use crate::middleware::Metrics;

/// Shared application state.
#[derive(Clone)]
pub struct ProxyState {
    /// Miroir configuration.
    pub config: Arc<MiroirConfig>,

    /// Cluster topology.
    pub topology: Arc<RwLock<Topology>>,

    /// HTTP client for node communication.
    pub client: Arc<NodeClient>,

    /// Query sequence counter for round-robin group selection.
    pub query_seq: Arc<AtomicU64>,

    /// Master key for client authentication.
    pub master_key: Arc<String>,

    /// Admin API key.
    pub admin_key: Arc<String>,

    /// Prometheus metrics.
    pub metrics: Arc<Metrics>,
}

impl ProxyState {
    /// Create a new proxy state from configuration.
    pub fn new(config: MiroirConfig) -> Result<Self> {
        // Build topology from config nodes
        let mut topology = Topology::new(config.shards, config.replication_factor as usize);

        for node_config in &config.nodes {
            let node = Node::new(
                NodeId::new(node_config.id.clone()),
                node_config.address.clone(),
                node_config.replica_group,
            );
            topology.add_node(node);
        }

        // Validate topology matches config
        if topology.replica_group_count() != config.replica_groups {
            return Err(MiroirError::Config(format!(
                "Topology has {} groups but config specifies {}",
                topology.replica_group_count(),
                config.replica_groups
            )));
        }

        let client = Arc::new(NodeClient::new(
            config.node_master_key.clone(),
            &config.server,
        ));

        // Use master_key from config (already loaded with env var override)
        let master_key = Arc::new(config.master_key.clone());
        let admin_key = Arc::new(config.admin.api_key.clone());
        let metrics = Arc::new(Metrics::new());

        Ok(Self {
            config: Arc::new(config),
            topology: Arc::new(RwLock::new(topology)),
            client,
            query_seq: Arc::new(AtomicU64::new(0)),
            master_key,
            admin_key,
            metrics,
        })
    }

    /// Increment and get the next query sequence number.
    pub fn next_query_seq(&self) -> u64 {
        self.query_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the current topology.
    pub async fn topology(&self) -> Topology {
        self.topology.read().await.clone()
    }

    /// Check if a master key is valid.
    #[allow(dead_code)]
    pub fn is_valid_master_key(&self, key: &str) -> bool {
        !self.master_key.is_empty() && self.master_key.as_str() == key
    }

    /// Check if an admin key is valid.
    #[allow(dead_code)]
    pub fn is_valid_admin_key(&self, key: &str) -> bool {
        !self.admin_key.is_empty() && self.admin_key.as_str() == key
    }

    /// Get node health status for topology endpoint.
    pub async fn get_node_health(&self) -> Vec<NodeHealth> {
        let topology = self.topology.read().await;
        let mut health = Vec::new();

        for node in topology.nodes() {
            health.push(NodeHealth {
                id: node.id.as_str().to_string(),
                address: node.address.clone(),
                replica_group: node.replica_group,
                status: node.status,
                is_healthy: node.is_healthy(),
            });
        }

        health
    }

    /// Get shard assignment for topology endpoint.
    pub async fn get_shard_assignments(&self) -> Vec<ShardAssignment> {
        use miroir_core::router;

        let topology = self.topology.read().await;
        let mut assignments = Vec::new();

        for group in topology.groups() {
            for shard_id in 0..self.config.shards {
                let nodes = router::assign_shard_in_group(shard_id, group.nodes(), topology.rf());

                assignments.push(ShardAssignment {
                    shard_id,
                    replica_group: group.id,
                    nodes: nodes.iter().map(|n| n.as_str().to_string()).collect(),
                });
            }
        }

        assignments
    }
}

/// Node health information for topology endpoint.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeHealth {
    pub id: String,
    pub address: String,
    pub replica_group: u32,
    pub status: NodeStatus,
    pub is_healthy: bool,
}

/// Shard assignment information for topology endpoint.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ShardAssignment {
    pub shard_id: u32,
    pub replica_group: u32,
    pub nodes: Vec<String>,
}
