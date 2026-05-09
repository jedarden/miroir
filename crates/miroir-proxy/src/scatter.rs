//! HTTP scatter-gather execution for Meilisearch nodes.

use crate::client::NodeClient;
use crate::retry_cache::{CachedResponse, RetryCache};
use async_trait::async_trait;
use miroir_core::config::UnavailableShardPolicy;
use miroir_core::scatter::{NodeResponse, Scatter, ScatterRequest, ScatterResponse};
use miroir_core::topology::{NodeId, Topology};
use miroir_core::{MiroirError, Result};
use std::time::Duration;
use tokio::time::timeout;

/// HTTP scatter implementation using NodeClient.
#[derive(Clone)]
pub struct HttpScatter {
    client: NodeClient,
    node_timeout_ms: u64,
    /// Retry cache for idempotency (plan §4).
    retry_cache: Option<RetryCache>,
}

impl HttpScatter {
    pub fn new(client: NodeClient, node_timeout_ms: u64) -> Self {
        Self {
            client,
            node_timeout_ms,
            retry_cache: None,
        }
    }

    /// Create a new HttpScatter with retry cache enabled.
    pub fn with_retry_cache(client: NodeClient, node_timeout_ms: u64, retry_cache: RetryCache) -> Self {
        Self {
            client,
            node_timeout_ms,
            retry_cache: Some(retry_cache),
        }
    }

    /// Send a request to a single node with retry cache lookup.
    async fn send_to_node_with_cache(
        &self,
        topology: &Topology,
        node_id: &NodeId,
        method: &str,
        path: &str,
        body: &[u8],
        headers: &[(String, String)],
    ) -> Result<miroir_core::scatter::NodeResponse> {
        // Check retry cache first if enabled
        if let Some(ref cache) = self.retry_cache {
            let cache_key = RetryCache::cache_key(body, node_id.as_str(), None);

            if let Some(cached) = cache.get(&cache_key).await {
                // Return cached response
                return Ok(NodeResponse {
                    node_id: node_id.clone(),
                    body: cached.body,
                    status: cached.status,
                    headers: Vec::new(),
                });
            }
        }

        // No cache hit, send actual request
        let timeout_dur = Duration::from_millis(self.node_timeout_ms);
        let result = timeout(timeout_dur, async {
            self.client
                .send_to_node(topology, node_id, method, path, Some(body), headers)
                .await
        })
        .await;

        match result {
            Ok(Ok(resp)) => {
                let body_bytes = resp.body;
                let status = resp.status;

                // Cache successful responses
                if let Some(ref cache) = self.retry_cache {
                    // Cache only 2xx responses
                    if (200..300).contains(&status) {
                        let cache_key = RetryCache::cache_key(body, node_id.as_str(), None);
                        let cached = CachedResponse {
                            body: body_bytes.clone(),
                            status,
                            cached_at: std::time::Instant::now(),
                        };
                        cache.put(cache_key, cached).await;
                    }
                }

                Ok(miroir_core::scatter::NodeResponse {
                    node_id: node_id.clone(),
                    body: body_bytes,
                    status,
                    headers: resp.headers,
                })
            }
            _ => Err(MiroirError::Routing(format!(
                "request to node {} timed out",
                node_id.as_str()
            ))),
        }
    }
}

#[async_trait]
impl Scatter for HttpScatter {
    async fn scatter(
        &self,
        topology: &Topology,
        nodes: Vec<NodeId>,
        request: ScatterRequest,
        policy: UnavailableShardPolicy,
    ) -> Result<ScatterResponse> {
        // Fan out requests to all nodes in parallel
        let futures: Vec<_> = nodes
            .iter()
            .map(|node_id| {
                let node_id = node_id.clone();
                let method = request.method.clone();
                let path = request.path.clone();
                let body = request.body.clone();
                let headers = request.headers.clone();
                let this = self.clone();
                let topo = topology.clone();

                async move {
                    match this.send_to_node_with_cache(
                        &topo,
                        &node_id,
                        &method,
                        &path,
                        &body,
                        &headers,
                    )
                    .await
                    {
                        Ok(resp) => Ok(resp),
                        Err(_) => Err(node_id),
                    }
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        let mut successful = Vec::new();
        let mut failed = Vec::new();

        for result in results {
            match result {
                Ok(resp) => successful.push(resp),
                Err(node_id) => failed.push(node_id),
            }
        }

        // Check if we should fail based on policy
        if policy == UnavailableShardPolicy::Error && !failed.is_empty() {
            return Err(MiroirError::Routing(format!(
                "{} nodes failed: {:?}",
                failed.len(),
                failed
            )));
        }

        Ok(ScatterResponse {
            responses: successful,
            failed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miroir_core::topology::{Node, Topology};

    #[tokio::test]
    async fn test_http_scatter_empty_nodes() {
        let client = NodeClient::new("test-key".to_string(), &Default::default());
        let scatter = HttpScatter::new(client, 1000);
        let topology = Topology::new(64, 1);

        let request = ScatterRequest {
            body: Vec::new(),
            headers: Vec::new(),
            method: "GET".to_string(),
            path: "/".to_string(),
        };

        let result = scatter
            .scatter(&topology, Vec::new(), request, UnavailableShardPolicy::Partial)
            .await
            .unwrap();

        assert!(result.responses.is_empty());
        assert!(result.failed.is_empty());
    }

    #[tokio::test]
    async fn test_http_scatter_timeout_handling() {
        let client = NodeClient::new("test-key".to_string(), &Default::default());
        let scatter = HttpScatter::new(client, 1); // 1ms timeout
        let mut topology = Topology::new(64, 1);

        // Add a node that will timeout
        topology.add_node(Node::new(
            NodeId::new("test-node".to_string()),
            "http://invalid.example.com:7700".to_string(),
            0,
        ));

        let request = ScatterRequest {
            body: Vec::new(),
            headers: Vec::new(),
            method: "GET".to_string(),
            path: "/".to_string(),
        };

        let result = scatter
            .scatter(
                &topology,
                vec![NodeId::new("test-node".to_string())],
                request,
                UnavailableShardPolicy::Partial,
            )
            .await
            .unwrap();

        assert!(result.responses.is_empty());
        assert_eq!(result.failed.len(), 1);
    }

    #[tokio::test]
    async fn test_http_scatter_error_policy() {
        let client = NodeClient::new("test-key".to_string(), &Default::default());
        let scatter = HttpScatter::new(client, 1);
        let mut topology = Topology::new(64, 1);

        topology.add_node(Node::new(
            NodeId::new("test-node".to_string()),
            "http://invalid.example.com:7700".to_string(),
            0,
        ));

        let request = ScatterRequest {
            body: Vec::new(),
            headers: Vec::new(),
            method: "GET".to_string(),
            path: "/".to_string(),
        };

        let result = scatter
            .scatter(
                &topology,
                vec![NodeId::new("test-node".to_string())],
                request,
                UnavailableShardPolicy::Error,
            )
            .await;

        assert!(result.is_err());
    }
}
