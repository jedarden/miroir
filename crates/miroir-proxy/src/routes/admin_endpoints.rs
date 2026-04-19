//! Admin API endpoints for topology, readiness, shards, and metrics.

use axum::{
    extract::{FromRef, State},
    http::StatusCode,
    Json,
    response::{IntoResponse, Response},
};
use miroir_core::{
    config::MiroirConfig,
    router,
    task_registry::InMemoryTaskRegistry,
    topology::{Node, NodeId, Topology},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::info;
use reqwest::Client;

/// Version state with cache for fetching Meilisearch version.
#[derive(Clone)]
pub struct VersionState {
    pub node_master_key: String,
    pub node_addresses: Vec<String>,
    pub version_cache: Arc<RwLock<Option<String>>>,
    pub last_cache_update: Arc<RwLock<Option<std::time::Instant>>>,
    pub cache_ttl_secs: u64,
}

impl VersionState {
    pub fn new(node_master_key: String, node_addresses: Vec<String>) -> Self {
        Self {
            node_master_key,
            node_addresses,
            version_cache: Arc::new(RwLock::new(None)),
            last_cache_update: Arc::new(RwLock::new(None)),
            cache_ttl_secs: 60,
        }
    }

    /// Fetch version from a healthy node, using cache if within TTL.
    pub async fn get_version(&self) -> Result<String, StatusCode> {
        // Check cache first
        {
            let cache = self.version_cache.read().await;
            let last_update = self.last_cache_update.read().await;
            if let (Some(ref cached), Some(last)) = (cache.as_ref(), last_update.as_ref()) {
                if last.elapsed().as_secs() < self.cache_ttl_secs {
                    return Ok((**cached).clone());
                }
            }
        }

        // Cache miss or expired - fetch from a node
        let client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        for address in &self.node_addresses {
            let url = format!("{}/version", address.trim_end_matches('/'));
            let response = client
                .get(&url)
                .header("Authorization", format!("Bearer {}", self.node_master_key))
                .send()
                .await;

            if let Ok(resp) = response {
                if resp.status().is_success() {
                    if let Ok(body) = resp.text().await {
                        // Update cache
                        *self.version_cache.write().await = Some(body.clone());
                        *self.last_cache_update.write().await = Some(std::time::Instant::now());
                        return Ok(body);
                    }
                }
            }
        }

        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}

/// Shared application state for admin endpoints.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<MiroirConfig>,
    pub topology: Arc<RwLock<Topology>>,
    pub ready: Arc<RwLock<bool>>,
    pub metrics: super::super::middleware::Metrics,
    pub version_state: VersionState,
    pub task_registry: Arc<InMemoryTaskRegistry>,
}

impl AppState {
    pub fn new(
        config: MiroirConfig,
        metrics: super::super::middleware::Metrics,
    ) -> Self {
        // Build initial topology from config
        let mut topology = Topology::new(
            config.shards,
            config.replica_groups,
            config.replication_factor as usize,
        );

        for node_config in &config.nodes {
            let node = Node::new(
                NodeId::new(node_config.id.clone()),
                node_config.address.clone(),
                node_config.replica_group,
            );
            // Start nodes in Joining state - health checker will promote to Active
            topology.add_node(node);
        }

        let version_state = VersionState::new(
            config.node_master_key.clone(),
            config.nodes.iter().map(|n| n.address.clone()).collect(),
        );

        Self {
            config: Arc::new(config),
            topology: Arc::new(RwLock::new(topology)),
            ready: Arc::new(RwLock::new(false)),
            metrics,
            version_state,
            task_registry: Arc::new(InMemoryTaskRegistry::new()),
        }
    }

    /// Mark the service as ready (all nodes reachable).
    pub async fn mark_ready(&self) {
        *self.ready.write().await = true;
        info!("Service marked as ready");
    }

    /// Check if a covering quorum is reachable.
    pub async fn check_covering_quorum(&self) -> bool {
        let topo = self.topology.read().await;
        let node_map = topo.node_map();

        // For each replica group, check if we have enough healthy nodes
        for group in topo.groups() {
            let healthy = group.healthy_nodes(&node_map);
            let required = (topo.rf() + 1) / 2; // Simple majority for quorum
            if healthy.len() < required {
                return false;
            }
        }

        true
    }
}

/// Response for GET /_miroir/topology (plan §10 JSON shape).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyResponse {
    pub shards: u32,
    pub replication_factor: u32,
    pub nodes: Vec<NodeInfo>,
    pub degraded_node_count: u32,
    pub rebalance_in_progress: bool,
    pub fully_covered: bool,
}

/// Per-node information in the topology response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub status: String,
    pub shard_count: u32,
    pub last_seen_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response for GET /_miroir/shards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardsResponse {
    pub shards: HashMap<String, Vec<String>>, // shard_id -> list of node IDs
}

/// GET /_miroir/topology — full cluster state per plan §10.
pub async fn get_topology<S>(State(state): State<S>) -> Result<Json<TopologyResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);
    let topo = state.topology.read().await;

    // Count degraded nodes
    let degraded_count = topo.nodes().filter(|n| !n.is_healthy()).count() as u32;

    // Build node info list
    let nodes: Vec<NodeInfo> = topo
        .nodes()
        .map(|n| NodeInfo {
            id: n.id.as_str().to_string(),
            status: format!("{:?}", n.status).to_lowercase(),
            shard_count: 0, // TODO: compute from routing table
            last_seen_ms: 0, // TODO: track last health check time
            error: None,     // TODO: populate from last health check error
        })
        .collect();

    // Check if fully covered
    let fully_covered = degraded_count == 0;

    let response = TopologyResponse {
        shards: topo.shards,
        replication_factor: topo.rf() as u32,
        nodes,
        degraded_node_count: degraded_count,
        rebalance_in_progress: false, // TODO: track rebalance state
        fully_covered,
    };

    Ok(Json(response))
}

/// GET /_miroir/shards — shard → node mapping table.
pub async fn get_shards<S>(State(state): State<S>) -> Result<Json<ShardsResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);
    let topo = state.topology.read().await;
    let mut shards = HashMap::new();

    // Build shard -> node mapping using rendezvous hash
    for shard_id in 0..topo.shards {
        let mut node_ids = Vec::new();

        // Collect nodes from all replica groups for this shard
        for group in topo.groups() {
            let assigned = router::assign_shard_in_group(shard_id, group.nodes(), topo.rf());
            for node_id in assigned {
                node_ids.push(node_id.as_str().to_string());
            }
        }

        shards.insert(shard_id.to_string(), node_ids);
    }

    Ok(Json(ShardsResponse { shards }))
}

/// GET /_miroir/ready — readiness probe (503 during startup, 200 once ready).
pub async fn get_ready<S>(State(state): State<S>) -> Result<&'static str, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);
    let ready = *state.ready.read().await;

    if ready {
        Ok("")
    } else {
        // Not yet marked ready - check if covering quorum exists
        let has_quorum = state.check_covering_quorum().await;
        if has_quorum {
            // Auto-mark ready on first successful quorum check
            state.mark_ready().await;
            Ok("")
        } else {
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

/// GET /_miroir/metrics — admin-key-gated Prometheus metrics.
pub async fn get_metrics<S>(State(state): State<S>) -> Response
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);
    match state.metrics.encode_metrics() {
        Ok(metrics) => metrics.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to encode metrics");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology_response_serialization() {
        let response = TopologyResponse {
            shards: 64,
            replication_factor: 2,
            nodes: vec![
                NodeInfo {
                    id: "meili-0".to_string(),
                    status: "healthy".to_string(),
                    shard_count: 32,
                    last_seen_ms: 100,
                    error: None,
                },
                NodeInfo {
                    id: "meili-1".to_string(),
                    status: "degraded".to_string(),
                    shard_count: 32,
                    last_seen_ms: 5000,
                    error: Some("connection refused".to_string()),
                },
            ],
            degraded_node_count: 1,
            rebalance_in_progress: false,
            fully_covered: false,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"shards\":64"));
        assert!(json.contains("\"replication_factor\":2"));
        assert!(json.contains("\"degraded_node_count\":1"));
        assert!(json.contains("\"fully_covered\":false"));
        assert!(json.contains("\"status\":\"healthy\""));
        assert!(json.contains("\"error\":\"connection refused\""));
    }

    #[test]
    fn test_shards_response_serialization() {
        let mut shards = HashMap::new();
        shards.insert("0".to_string(), vec!["node-0".to_string(), "node-1".to_string()]);
        shards.insert("1".to_string(), vec!["node-1".to_string(), "node-0".to_string()]);

        let response = ShardsResponse { shards };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"0\""));
        assert!(json.contains("\"node-0\""));
    }

    #[test]
    fn test_node_info_with_optional_error() {
        let info = NodeInfo {
            id: "test".to_string(),
            status: "healthy".to_string(),
            shard_count: 10,
            last_seen_ms: 100,
            error: None,
        };

        let json = serde_json::to_string(&info).unwrap();
        // error field should not be present when None
        assert!(!json.contains("error"));
    }

    #[test]
    fn test_node_info_with_error() {
        let info = NodeInfo {
            id: "test".to_string(),
            status: "failed".to_string(),
            shard_count: 10,
            last_seen_ms: 100,
            error: Some("timeout".to_string()),
        };

        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"error\":\"timeout\""));
    }
}
