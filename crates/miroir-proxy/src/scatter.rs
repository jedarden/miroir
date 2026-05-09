//! HTTP scatter-gather execution for Meilisearch nodes.

use crate::client::NodeClient;
use async_trait::async_trait;
use miroir_core::config::UnavailableShardPolicy;
use miroir_core::scatter::{NodeResponse, Scatter, ScatterRequest, ScatterResponse};
use miroir_core::topology::{NodeId, Topology};
use miroir_core::{MiroirError, Result};
use serde_json::Value;
use std::time::Duration;
use tokio::time::timeout;

/// HTTP scatter implementation using NodeClient.
pub struct HttpScatter {
    client: NodeClient,
    node_timeout_ms: u64,
}

impl HttpScatter {
    pub fn new(client: NodeClient, node_timeout_ms: u64) -> Self {
        Self {
            client,
            node_timeout_ms,
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
        let timeout_dur = Duration::from_millis(self.node_timeout_ms);

        // Fan out requests to all nodes in parallel
        let futures: Vec<_> = nodes
            .iter()
            .map(|node_id| {
                let client = self.client.clone();
                let topo = topology.clone();
                let req = request.clone();
                let node_id = node_id.clone();

                async move {
                    let result = timeout(timeout_dur, async move {
                        client
                            .send_to_node(
                                &topo,
                                &node_id,
                                &req.method,
                                &req.path,
                                Some(&req.body),
                                &req.headers,
                            )
                            .await
                    })
                    .await;

                    match result {
                        Ok(Ok(resp)) => {
                            let body_bytes = serde_json::to_vec(&resp.body).unwrap_or_default();
                            Ok(NodeResponse {
                                node_id,
                                body: body_bytes,
                                status: resp.status,
                                headers: resp.headers,
                            })
                        }
                        _ => Err(node_id),
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
        let topology = Topology::new(1);

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
        let mut topology = Topology::new(1);

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
        let mut topology = Topology::new(1);

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
