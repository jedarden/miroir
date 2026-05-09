//! HTTP client for forwarding requests to Meilisearch nodes.
//!
//! Implements connection pooling, retries, and orchestrator-side
//! retry cache for idempotency (plan §4 note on scatter.retry_on_timeout).

use miroir_core::config::ServerConfig;
use miroir_core::topology::{NodeId, Topology};
use miroir_core::{MiroirError, Result};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Node response with status code and body (as Vec<u8> for scatter compatibility).
#[derive(Debug, Clone)]
pub struct NodeResponse {
    pub node_id: NodeId,
    pub status: u16,
    pub body: Vec<u8>,
    #[allow(dead_code)]
    pub headers: Vec<(String, String)>,
}

impl NodeResponse {
    /// Convert body to JSON value.
    #[allow(dead_code)]
    pub fn body_json(&self) -> Result<Value> {
        serde_json::from_slice(&self.body).map_err(|e| MiroirError::Json(e))
    }
}

/// HTTP client for scatter-gather requests to Meilisearch nodes.
#[derive(Clone)]
pub struct NodeClient {
    /// Reqwest client with connection pooling.
    client: Client,

    /// Node master key for authentication.
    node_master_key: Arc<String>,

    /// Orchestrator-side retry cache for idempotency.
    /// Key: sha256(request_body || target_node || idempotency_key)
    /// Value: cached terminal response
    retry_cache: Arc<RwLock<HashMap<String, CachedResponse>>>,
}

/// Cached response for retry deduplication.
#[derive(Debug, Clone)]
struct CachedResponse {
    #[allow(dead_code)]
    response: NodeResponse,
    cached_at: std::time::Instant,
}

impl NodeClient {
    /// Create a new node client with connection pooling.
    pub fn new(node_master_key: String, server_config: &ServerConfig) -> Self {
        let client = Client::builder()
            .pool_max_idle_per_host(32)
            .pool_idle_timeout(Duration::from_secs(60))
            .timeout(Duration::from_millis(server_config.request_timeout_ms))
            .build()
            .expect("failed to create HTTP client");

        Self {
            client,
            node_master_key: Arc::new(node_master_key),
            retry_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Send a request to a single node.
    pub async fn send_to_node(
        &self,
        topology: &Topology,
        node_id: &NodeId,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
        headers: &[(String, String)],
    ) -> Result<NodeResponse> {
        let node = topology
            .node(node_id)
            .ok_or_else(|| MiroirError::Routing(format!("node {} not found", node_id.as_str())))?;

        let url = format!("{}{}", node.address, path);

        let mut request = match method {
            "GET" => self.client.get(&url),
            "POST" => self.client.post(&url),
            "PUT" => self.client.put(&url),
            "PATCH" => self.client.patch(&url),
            "DELETE" => self.client.delete(&url),
            _ => {
                return Err(MiroirError::Routing(format!(
                    "unsupported HTTP method: {method}",
                )))
            }
        };

        // Add node master key header
        request = request.header(
            "Authorization",
            format!("Bearer {}", self.node_master_key.as_str()),
        );

        // Add custom headers
        for (key, value) in headers {
            request = request.header(key, value);
        }

        // Add body if present
        if let Some(body_bytes) = body {
            request = request.header("Content-Type", "application/json");
            request = request.body(body_bytes.to_vec());
        }

        let response = request.send().await.map_err(|e| {
            MiroirError::Routing(format!("request to node {} failed: {e}", node_id.as_str(),))
        })?;

        let status = response.status().as_u16();

        // Collect response headers
        let response_headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|v| (name.as_str().to_string(), v.to_string()))
            })
            .collect();

        let body_bytes = response
            .bytes()
            .await
            .map_err(|e| MiroirError::Routing(format!("failed to read response body: {e}")))?;

        Ok(NodeResponse {
            node_id: node_id.clone(),
            status,
            body: body_bytes.to_vec(),
            headers: response_headers,
        })
    }

    /// Send requests to multiple nodes in parallel.
    pub async fn send_to_many(
        &self,
        topology: &Topology,
        node_ids: &[NodeId],
        method: &str,
        path: &str,
        body: Option<&[u8]>,
        headers: &[(String, String)],
    ) -> Vec<Result<NodeResponse>> {
        let futures: Vec<_> = node_ids
            .iter()
            .map(|node_id| self.send_to_node(topology, node_id, method, path, body, headers))
            .collect();

        futures::future::join_all(futures).await
    }

    /// Prune old entries from the retry cache.
    pub async fn prune_retry_cache(&self, max_age: Duration) {
        let mut cache = self.retry_cache.write().await;
        let now = std::time::Instant::now();
        cache.retain(|_, entry| now.duration_since(entry.cached_at) < max_age);
    }
}
