//! Scatter orchestration: fan-out logic and covering set builder.

use async_trait::async_trait;
use crate::config::UnavailableShardPolicy;
use crate::topology::{NodeId, Topology};
use crate::router::{covering_set, query_group};
use crate::{MiroirError, Result};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

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

/// A search request to be sent to nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    /// Request body (JSON or raw bytes).
    pub body: Vec<u8>,

    /// Request headers.
    pub headers: Vec<(String, String)>,

    /// HTTP method.
    pub method: String,

    /// Request path.
    pub path: String,
}

/// A page of hits from a single shard (re-export from merger for convenience).
pub type ShardHitPage = crate::merger::ShardHitPage;

/// Plan for a search scatter operation.
///
/// This structure captures all the information needed to execute a search
/// across the covering set, including the chosen replica group, target shards,
/// and the shard-to-node mapping. It's designed to be serializable for use
/// in explain endpoints and for testing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScatterPlan {
    /// The replica group chosen for this query (query_seq % RG).
    pub chosen_group: u32,

    /// Target shards for this search (initially all 0..S, used for §13.4 narrowing).
    pub target_shards: Vec<u32>,

    /// Mapping from shard ID to the node that should serve it.
    pub shard_to_node: HashMap<u32, NodeId>,

    /// Deadline for the scatter operation in milliseconds.
    pub deadline_ms: u32,

    /// Whether hedging is eligible (reserved for §13.2 Phase 5).
    pub hedging_eligible: bool,
}

impl ScatterPlan {
    /// Create a new scatter plan.
    pub fn new(
        chosen_group: u32,
        target_shards: Vec<u32>,
        shard_to_node: HashMap<u32, NodeId>,
        deadline_ms: u32,
    ) -> Self {
        Self {
            chosen_group,
            target_shards,
            shard_to_node,
            deadline_ms,
            hedging_eligible: false,
        }
    }

    /// Get the unique nodes in this plan (deduplicated).
    pub fn nodes(&self) -> Vec<NodeId> {
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for node_id in self.shard_to_node.values() {
            if seen.insert(node_id) {
                result.push(node_id.clone());
            }
        }
        result
    }

    /// Get the number of shards in this plan.
    pub fn shard_count(&self) -> usize {
        self.shard_to_node.len()
    }
}

/// HTTP client for communicating with Meilisearch nodes.
///
/// This trait defines the interface for making HTTP requests to nodes.
/// It's intentionally minimal to allow for both real HTTP clients (reqwest)
/// and mock clients for testing.
#[async_trait]
pub trait NodeClient: Send + Sync {
    /// Send a request to a single node.
    ///
    /// Returns the response body, status code, and headers.
    async fn send_to_node(
        &self,
        topology: &Topology,
        node_id: &NodeId,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
        headers: &[(String, String)],
    ) -> Result<NodeResponse>;
}

/// Plan a search scatter operation.
///
/// This is a pure function that constructs the scatter plan without
/// performing any I/O. It:
/// 1. Selects the replica group via query_seq % RG
/// 2. Builds the covering set for the chosen group
/// 3. Creates the shard-to-node mapping
///
/// # Arguments
/// * `topology` - The cluster topology
/// * `query_seq` - The query sequence number for load balancing
/// * `rf` - The replication factor
/// * `shard_count` - The total number of shards
///
/// # Returns
/// A `ScatterPlan` containing all information needed to execute the scatter.
pub fn plan_search_scatter(
    topology: &Topology,
    query_seq: u64,
    rf: usize,
    shard_count: u32,
) -> Result<ScatterPlan> {
    let replica_groups = topology.replica_group_count();
    if replica_groups == 0 {
        return Err(MiroirError::Routing(
            "No replica groups available".to_string(),
        ));
    }

    // Select the replica group
    let chosen_group = query_group(query_seq, replica_groups);

    // Get the group
    let group = topology
        .group(chosen_group)
        .ok_or_else(|| MiroirError::Routing(format!("Group {} not found", chosen_group)))?;

    // Build the covering set
    let covering_nodes = covering_set(shard_count, group, rf, query_seq);

    // Build shard-to-node mapping
    let mut shard_to_node = HashMap::new();
    for (shard_id, node_id) in (0..shard_count).zip(covering_nodes.iter().cycle()) {
        shard_to_node.insert(shard_id, node_id.clone());
    }

    // Target shards: initially all shards (0..S)
    let target_shards: Vec<u32> = (0..shard_count).collect();

    // Default deadline: 5000ms
    let deadline_ms = 5000;

    Ok(ScatterPlan::new(
        chosen_group,
        target_shards,
        shard_to_node,
        deadline_ms,
    ))
}

/// Execute a scatter search using the provided plan.
///
/// This function fans out the search request to all nodes in the plan,
/// respecting deadlines and handling partial failures gracefully.
///
/// # Arguments
/// * `plan` - The scatter plan from `plan_search_scatter`
/// * `client` - The NodeClient implementation for making HTTP requests
/// * `req` - The search request to send
///
/// # Returns
/// A vector of `ShardHitPage` responses, one per node in the plan.
/// Failed nodes are represented as `ShardHitPage` with `success: false`.
pub async fn execute_scatter<C: NodeClient>(
    plan: &ScatterPlan,
    client: &C,
    req: SearchRequest,
    topology: &Topology,
) -> Result<Vec<ShardHitPage>> {
    use tokio::time::{timeout, Duration};
    use serde_json::Value;
    use std::sync::Arc;

    // Get the unique nodes to send requests to
    let nodes = plan.nodes();

    // Clone data needed for async blocks
    let deadline_ms = plan.deadline_ms;
    let topology = Arc::new(topology.clone());
    let client = Arc::new(client);

    // Fan out requests to all nodes in parallel
    let futures: Vec<_> = nodes
        .into_iter()
        .map(|node_id| {
            let node_id = node_id.clone();
            let method = req.method.clone();
            let path = req.path.clone();
            let body = req.body.clone();
            let headers = req.headers.clone();
            let topology = Arc::clone(&topology);
            let client = Arc::clone(&client);

            async move {
                let deadline = Duration::from_millis(deadline_ms as u64);

                match timeout(
                    deadline,
                    client.send_to_node(&topology, &node_id, &method, &path, Some(&body), &headers),
                )
                .await
                {
                    Ok(Ok(resp)) => {
                        // Success: parse the response body as JSON
                        let body: Value = serde_json::from_slice(&resp.body)
                            .unwrap_or_else(|_| Value::Null);

                        Ok(ShardHitPage {
                            body,
                            success: true,
                        })
                    }
                    Ok(Err(_)) | Err(_) => {
                        // Failure: return an unsuccessful shard page
                        Ok(ShardHitPage {
                            body: Value::Null,
                            success: false,
                        })
                    }
                }
            }
        })
        .collect();

    // Execute all futures concurrently and collect results
    let shard_pages: Result<Vec<_>> = futures::future::join_all(futures)
        .await
        .into_iter()
        .collect();

    shard_pages
}

/// Default stub implementation of Scatter.
#[derive(Debug, Clone, Default)]
pub struct StubScatter;

/// Mock NodeClient for testing.
#[derive(Debug, Clone, Default)]
pub struct MockNodeClient {
    /// Optional response to return (if None, returns error)
    response: Option<serde_json::Value>,
    /// Optional delay before responding (for testing deadlines)
    delay_ms: Option<u64>,
}

impl MockNodeClient {
    /// Create a new mock client that returns the given response.
    pub fn with_response(response: serde_json::Value) -> Self {
        Self {
            response: Some(response),
            delay_ms: None,
        }
    }

    /// Create a new mock client that delays before responding.
    pub fn with_delay(response: serde_json::Value, delay_ms: u64) -> Self {
        Self {
            response: Some(response),
            delay_ms: Some(delay_ms),
        }
    }

    /// Create a new mock client that always returns an error.
    pub fn with_error() -> Self {
        Self {
            response: None,
            delay_ms: None,
        }
    }
}

#[async_trait]
impl NodeClient for MockNodeClient {
    async fn send_to_node(
        &self,
        _topology: &Topology,
        _node_id: &NodeId,
        _method: &str,
        _path: &str,
        _body: Option<&[u8]>,
        _headers: &[(String, String)],
    ) -> Result<NodeResponse> {
        use std::time::Duration;

        // Add delay if configured
        if let Some(delay) = self.delay_ms {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }

        if let Some(ref response) = self.response {
            let body = serde_json::to_vec(response).unwrap_or_default();
            Ok(NodeResponse {
                node_id: NodeId::new("mock".to_string()),
                body,
                status: 200,
                headers: Vec::new(),
            })
        } else {
            Err(MiroirError::Routing("Mock error".to_string()))
        }
    }
}

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

    // === Tests for ScatterPlan and search scatter API ===

    #[test]
    fn test_scatter_plan_creation() {
        let chosen_group = 0;
        let target_shards = vec![0, 1, 2, 3];
        let mut shard_to_node = HashMap::new();
        shard_to_node.insert(0, NodeId::new("node1".to_string()));
        shard_to_node.insert(1, NodeId::new("node2".to_string()));
        shard_to_node.insert(2, NodeId::new("node3".to_string()));
        shard_to_node.insert(3, NodeId::new("node4".to_string()));

        let plan = ScatterPlan::new(
            chosen_group,
            target_shards,
            shard_to_node.clone(),
            5000,
        );

        assert_eq!(plan.chosen_group, chosen_group);
        assert_eq!(plan.target_shards.len(), 4);
        assert_eq!(plan.shard_to_node.len(), 4);
        assert_eq!(plan.deadline_ms, 5000);
        assert!(!plan.hedging_eligible);
    }

    #[test]
    fn test_scatter_plan_nodes_deduplication() {
        let mut shard_to_node = HashMap::new();
        let node1 = NodeId::new("node1".to_string());
        let node2 = NodeId::new("node2".to_string());

        // Map multiple shards to the same node
        shard_to_node.insert(0, node1.clone());
        shard_to_node.insert(1, node1.clone());
        shard_to_node.insert(2, node1.clone());
        shard_to_node.insert(3, node2.clone());
        shard_to_node.insert(4, node2.clone());

        let plan = ScatterPlan::new(
            0,
            vec![0, 1, 2, 3, 4],
            shard_to_node,
            5000,
        );

        // Should return only unique nodes
        let nodes = plan.nodes();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&node1));
        assert!(nodes.contains(&node2));
    }

    #[test]
    fn test_scatter_plan_shard_count() {
        let mut shard_to_node = HashMap::new();
        for i in 0..10 {
            shard_to_node.insert(i, NodeId::new(format!("node{}", i % 3)));
        }

        let plan = ScatterPlan::new(
            0,
            (0..10).collect(),
            shard_to_node,
            5000,
        );

        assert_eq!(plan.shard_count(), 10);
    }

    #[test]
    fn test_plan_search_scatter_pure_function() {
        let mut topology = Topology::new(64, 2); // 64 shards, RF=2

        // Add nodes to group 0
        for i in 0..3 {
            topology.add_node(crate::topology::Node::new(
                NodeId::new(format!("node-{}", i)),
                format!("http://node-{}", i),
                0,
            ));
        }

        // This is a pure function - no async, no I/O
        let plan = plan_search_scatter(&topology, 42, 2, 64).unwrap();

        assert_eq!(plan.chosen_group, 42 % 1); // Only 1 group
        assert_eq!(plan.target_shards.len(), 64); // All shards
        assert_eq!(plan.shard_to_node.len(), 64); // All shards mapped
        assert_eq!(plan.deadline_ms, 5000);
    }

    #[test]
    fn test_plan_search_scatter_with_multiple_groups() {
        let mut topology = Topology::new(64, 2); // 64 shards, RF=2

        // Add nodes to 3 groups
        for group_id in 0..3 {
            for i in 0..2 {
                topology.add_node(crate::topology::Node::new(
                    NodeId::new(format!("node-g{}-{}", group_id, i)),
                    format!("http://node-g{}-{}", group_id, i),
                    group_id,
                ));
            }
        }

        // Test with different query_seq values
        let plan1 = plan_search_scatter(&topology, 0, 2, 64).unwrap();
        let plan2 = plan_search_scatter(&topology, 1, 2, 64).unwrap();
        let plan3 = plan_search_scatter(&topology, 2, 2, 64).unwrap();

        // Should round-robin through groups
        assert_eq!(plan1.chosen_group, 0);
        assert_eq!(plan2.chosen_group, 1);
        assert_eq!(plan3.chosen_group, 2);
    }

    #[test]
    fn test_plan_search_scatter_empty_topology() {
        let topology = Topology::new(64, 2); // No groups

        let result = plan_search_scatter(&topology, 0, 2, 64);

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_scatter_with_mock_client() {
        let mut topology = Topology::new(64, 2);

        // Add a test node
        topology.add_node(crate::topology::Node::new(
            NodeId::new("test-node".to_string()),
            "http://test-node".to_string(),
            0,
        ));

        // Create a simple plan
        let mut shard_to_node = HashMap::new();
        shard_to_node.insert(0, NodeId::new("test-node".to_string()));
        shard_to_node.insert(1, NodeId::new("test-node".to_string()));

        let plan = ScatterPlan::new(
            0,
            vec![0, 1],
            shard_to_node,
            5000,
        );

        // Create a mock client
        let response_body = serde_json::json!({
            "hits": [{"id": "1", "title": "Test"}],
            "estimatedTotalHits": 1,
            "processingTimeMs": 10,
        });
        let client = MockNodeClient::with_response(response_body);

        // Execute the scatter
        let req = SearchRequest {
            body: b"{}".to_vec(),
            headers: vec![],
            method: "POST".to_string(),
            path: "/search".to_string(),
        };

        let results = execute_scatter(&plan, &client, req, &topology).await.unwrap();

        // Should get one result per unique node
        assert_eq!(results.len(), 1);
        assert!(results[0].success);
        assert_eq!(results[0].body["hits"][0]["id"], "1");
    }

    #[tokio::test]
    async fn test_execute_scatter_partial_failure() {
        let mut topology = Topology::new(64, 2);

        // Add test nodes
        topology.add_node(crate::topology::Node::new(
            NodeId::new("node1".to_string()),
            "http://node1".to_string(),
            0,
        ));
        topology.add_node(crate::topology::Node::new(
            NodeId::new("node2".to_string()),
            "http://node2".to_string(),
            0,
        ));

        // Create a plan with multiple nodes
        let mut shard_to_node = HashMap::new();
        shard_to_node.insert(0, NodeId::new("node1".to_string()));
        shard_to_node.insert(1, NodeId::new("node2".to_string()));

        let plan = ScatterPlan::new(
            0,
            vec![0, 1],
            shard_to_node,
            5000,
        );

        // Create a mock client that always fails
        let client = MockNodeClient::with_error();

        // Execute the scatter
        let req = SearchRequest {
            body: b"{}".to_vec(),
            headers: vec![],
            method: "POST".to_string(),
            path: "/search".to_string(),
        };

        let results = execute_scatter(&plan, &client, req, &topology).await.unwrap();

        // Should get failed results for both nodes
        assert_eq!(results.len(), 2);
        assert!(!results[0].success);
        assert!(!results[1].success);
    }

    #[tokio::test]
    async fn test_execute_scatter_deadline_exceeded() {
        let mut topology = Topology::new(64, 2);

        // Add a test node
        topology.add_node(crate::topology::Node::new(
            NodeId::new("slow-node".to_string()),
            "http://slow-node".to_string(),
            0,
        ));

        // Create a plan with a short deadline
        let mut shard_to_node = HashMap::new();
        shard_to_node.insert(0, NodeId::new("slow-node".to_string()));

        let plan = ScatterPlan::new(
            0,
            vec![0],
            shard_to_node,
            10, // 10ms deadline
        );

        // Create a mock client that delays longer than the deadline
        let response_body = serde_json::json!({"hits": []});
        let client = MockNodeClient::with_delay(response_body, 100); // 100ms delay

        // Execute the scatter
        let req = SearchRequest {
            body: b"{}".to_vec(),
            headers: vec![],
            method: "POST".to_string(),
            path: "/search".to_string(),
        };

        let results = execute_scatter(&plan, &client, req, &topology).await.unwrap();

        // Should get a failed result due to timeout
        assert_eq!(results.len(), 1);
        assert!(!results[0].success);
    }

    #[test]
    fn test_search_request_creation() {
        let req = SearchRequest {
            body: b"test body".to_vec(),
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            method: "POST".to_string(),
            path: "/search".to_string(),
        };

        assert_eq!(req.body, b"test body");
        assert_eq!(req.headers.len(), 1);
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/search");
    }

    #[test]
    fn test_scatter_plan_serialization() {
        let mut shard_to_node = HashMap::new();
        shard_to_node.insert(0, NodeId::new("node1".to_string()));
        shard_to_node.insert(1, NodeId::new("node2".to_string()));

        let plan = ScatterPlan::new(
            0,
            vec![0, 1],
            shard_to_node,
            5000,
        );

        // Should be serializable
        let json = serde_json::to_string(&plan).unwrap();
        assert!(json.contains("\"chosen_group\":0"));
        assert!(json.contains("\"deadline_ms\":5000"));

        // Should be deserializable
        let deserialized: ScatterPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.chosen_group, plan.chosen_group);
        assert_eq!(deserialized.deadline_ms, plan.deadline_ms);
    }
