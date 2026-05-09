//! Scatter orchestration: fan-out logic and covering set builder.

use async_trait::async_trait;
use crate::config::UnavailableShardPolicy;
use crate::topology::{NodeId, Topology};
use crate::Result;

/// Scatter orchestrator: fans out requests to the covering set.
#[async_trait]
pub trait Scatter: Send + Sync {
    /// Execute a scatter request to multiple nodes.
    ///
    /// Returns a map of node ID to response. Failed nodes are omitted
    /// based on the unavailable shard policy.
    async fn scatter(
        &self,
        topology: &Topology,
        nodes: Vec<NodeId>,
        request: ScatterRequest,
        policy: UnavailableShardPolicy,
    ) -> Result<ScatterResponse>;
}

/// A scatter request to be sent to each node.
#[derive(Debug, Clone)]
pub struct ScatterRequest {
    /// Request body (JSON or raw bytes).
    pub body: Vec<u8>,

    /// Request headers.
    pub headers: Vec<(String, String)>,

    /// HTTP method.
    pub method: String,

    /// Request path.
    pub path: String,
}

/// Response from a scatter operation.
#[derive(Debug, Clone)]
pub struct ScatterResponse {
    /// Responses from successful nodes.
    pub responses: Vec<NodeResponse>,

    /// Nodes that failed or timed out.
    pub failed: Vec<NodeId>,
}

/// Response from a single node.
#[derive(Debug, Clone)]
pub struct NodeResponse {
    /// Node that responded.
    pub node_id: NodeId,

    /// Response body.
    pub body: Vec<u8>,

    /// HTTP status code.
    pub status: u16,

    /// Response headers.
    pub headers: Vec<(String, String)>,
}

/// Default stub implementation of Scatter.
#[derive(Debug, Clone, Default)]
pub struct StubScatter;

#[async_trait]
impl Scatter for StubScatter {
    async fn scatter(
        &self,
        _topology: &Topology,
        _nodes: Vec<NodeId>,
        _request: ScatterRequest,
        _policy: UnavailableShardPolicy,
    ) -> Result<ScatterResponse> {
        Ok(ScatterResponse {
            responses: Vec::new(),
            failed: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::UnavailableShardPolicy;

    #[test]
    fn test_scatter_request_creation() {
        let request = ScatterRequest {
            body: b"test body".to_vec(),
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            method: "POST".to_string(),
            path: "/search".to_string(),
        };

        assert_eq!(request.body, b"test body");
        assert_eq!(request.headers.len(), 1);
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/search");
    }

    #[tokio::test]
    async fn test_stub_scatter_returns_empty_response() {
        let scatter = StubScatter;
        let topology = Topology::new(64, 1); // 64 shards, RF=1
        let nodes = vec![NodeId::new("node1".to_string())];
        let request = ScatterRequest {
            body: Vec::new(),
            headers: Vec::new(),
            method: "GET".to_string(),
            path: "/".to_string(),
        };

        let result = scatter
            .scatter(&topology, nodes, request, UnavailableShardPolicy::Partial)
            .await
            .unwrap();

        assert!(result.responses.is_empty());
        assert!(result.failed.is_empty());
    }

    #[test]
    fn test_scatter_response_empty() {
        let response = ScatterResponse {
            responses: Vec::new(),
            failed: Vec::new(),
        };

        assert!(response.responses.is_empty());
        assert!(response.failed.is_empty());
    }

    #[test]
    fn test_scatter_response_with_data() {
        let node_response = NodeResponse {
            node_id: NodeId::new("node1".to_string()),
            body: b"response".to_vec(),
            status: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        };

        let response = ScatterResponse {
            responses: vec![node_response],
            failed: vec![NodeId::new("node2".to_string())],
        };

        assert_eq!(response.responses.len(), 1);
        assert_eq!(response.failed.len(), 1);
        assert_eq!(response.responses[0].node_id.as_str(), "node1");
        assert_eq!(response.responses[0].status, 200);
        assert_eq!(response.failed[0].as_str(), "node2");
    }

    #[test]
    fn test_node_response_creation() {
        let response = NodeResponse {
            node_id: NodeId::new("test-node".to_string()),
            body: b"test body".to_vec(),
            status: 200,
            headers: vec![("X-Custom".to_string(), "value".to_string())],
        };

        assert_eq!(response.node_id.as_str(), "test-node");
        assert_eq!(response.body, b"test body");
        assert_eq!(response.status, 200);
        assert_eq!(response.headers.len(), 1);
        assert_eq!(response.headers[0].0, "X-Custom");
    }

    #[tokio::test]
    async fn test_stub_scatter_with_empty_nodes() {
        let scatter = StubScatter;
        let topology = Topology::new(64, 1); // 64 shards, RF=1
        let nodes: Vec<NodeId> = Vec::new();
        let request = ScatterRequest {
            body: Vec::new(),
            headers: Vec::new(),
            method: "GET".to_string(),
            path: "/".to_string(),
        };

        let result = scatter
            .scatter(&topology, nodes, request, UnavailableShardPolicy::Partial)
            .await
            .unwrap();

        assert!(result.responses.is_empty());
        assert!(result.failed.is_empty());
    }

    #[tokio::test]
    async fn test_stub_scatter_with_multiple_nodes() {
        let scatter = StubScatter;
        let mut topology = Topology::new(64, 1); // 64 shards, RF=1

        let node1 = NodeId::new("node1".to_string());
        let node2 = NodeId::new("node2".to_string());
        let node3 = NodeId::new("node3".to_string());

        topology.add_node(crate::topology::Node::new(
            node1.clone(),
            "http://node1".to_string(),
            0,
        ));
        topology.add_node(crate::topology::Node::new(
            node2.clone(),
            "http://node2".to_string(),
            0,
        ));
        topology.add_node(crate::topology::Node::new(
            node3.clone(),
            "http://node3".to_string(),
            0,
        ));

        let nodes = vec![node1, node2, node3];
        let request = ScatterRequest {
            body: Vec::new(),
            headers: Vec::new(),
            method: "POST".to_string(),
            path: "/search".to_string(),
        };

        let result = scatter
            .scatter(&topology, nodes, request, UnavailableShardPolicy::Partial)
            .await
            .unwrap();

        assert!(result.responses.is_empty());
        assert!(result.failed.is_empty());
    }
}
