//! Scatter orchestration: fan-out logic and covering set builder.

use crate::config::UnavailableShardPolicy;
use crate::merger::ShardHitPage;
use crate::router::{covering_set, query_group};
use crate::topology::{NodeId, Topology};
use crate::Result;
use serde_json::Value;
use std::collections::HashMap;

/// Scatter plan: the exact shard→node mapping for a search query.
///
/// Separating the plan from execution makes §13.20 `/explain` cheap —
/// the explain path generates the plan and returns it without touching any node.
#[derive(Debug, Clone)]
pub struct ScatterPlan {
    /// Chosen replica group for this query (query_seq % RG).
    pub chosen_group: u32,

    /// Target shards to query (for §13.4 narrowing — initially all 0..S).
    pub target_shards: Vec<u32>,

    /// Resolved covering set: shard ID → node ID.
    pub shard_to_node: HashMap<u32, NodeId>,

    /// Deadline for the query in milliseconds.
    pub deadline_ms: u32,

    /// Whether hedging is eligible (reserved for §13.2 Phase 5).
    pub hedging_eligible: bool,
}

/// HTTP client for communicating with a Meilisearch node.
///
/// This is the seam between `miroir-core` (pure, no network) and
/// `miroir-proxy` (HTTP client). Injecting it via a trait means unit tests
/// can provide a fake client; production binds `reqwest` via the trait impl.
#[allow(async_fn_in_trait)]
pub trait NodeClient: Send + Sync {
    /// Execute a search request on a single node.
    ///
    /// Returns the raw JSON response from the node.
    async fn search_node(
        &self,
        node: &NodeId,
        address: &str,
        request: &SearchRequest,
    ) -> std::result::Result<Value, NodeError>;
}

/// Error from a single node during scatter.
#[derive(Debug, Clone)]
pub enum NodeError {
    /// Node timed out.
    Timeout,
    /// Node returned an error response.
    HttpError { status: u16, body: String },
    /// Network or connection error.
    NetworkError(String),
}

/// A search request to be sent to each node in the covering set.
#[derive(Debug, Clone)]
pub struct SearchRequest {
    /// Index UID being queried.
    pub index_uid: String,

    /// Search query (q parameter).
    pub query: Option<String>,

    /// Offset for pagination.
    pub offset: usize,

    /// Limit for pagination.
    pub limit: usize,

    /// Filter expression.
    pub filter: Option<Value>,

    /// Facets to compute.
    pub facets: Option<Vec<String>>,

    /// Whether to return ranking scores.
    pub ranking_score: bool,

    /// Raw JSON body for the search request (captures any other parameters).
    pub body: Value,
}

/// Result of a scatter operation.
#[derive(Debug)]
pub struct ScatterResult {
    /// Responses from successfully contacted nodes.
    pub shard_pages: Vec<ShardHitPage>,

    /// Errors from nodes that failed (shard ID → error).
    pub failed_shards: HashMap<u32, NodeError>,

    /// Whether the response is partial (some shards failed).
    pub partial: bool,

    /// Whether any node exceeded the deadline.
    pub deadline_exceeded: bool,
}

/// Construct a scatter plan for a search query.
///
/// This is a pure function — no async, no I/O. It selects the replica group,
/// computes the covering set, and maps each shard to its target node.
///
/// # Arguments
/// * `topology` - Current cluster topology
/// * `query_seq` - Query sequence number for group selection and load balancing
/// * `rf` - Replication factor (redundant with topology.rf, kept for explicitness)
/// * `shard_count` - Number of shards to query (typically topology.shards)
///
/// # Returns
/// A `ScatterPlan` containing the covering set and metadata for execution.
pub fn plan_search_scatter(
    topology: &Topology,
    query_seq: u64,
    rf: usize,
    shard_count: u32,
) -> ScatterPlan {
    let chosen_group = query_group(query_seq, topology.replica_group_count());

    // Get the target group
    let group = match topology.group(chosen_group) {
        Some(g) => g,
        None => {
            // Invalid group ID — return empty plan (should not happen with valid topology)
            return ScatterPlan {
                chosen_group,
                target_shards: Vec::new(),
                shard_to_node: HashMap::new(),
                deadline_ms: 0,
                hedging_eligible: false,
            };
        }
    };

    // Compute covering set: one node per shard within the chosen group
    let _covering = covering_set(shard_count, group, rf, query_seq);

    // Build shard → node mapping
    let mut shard_to_node = HashMap::new();
    for shard_id in 0..shard_count {
        let replicas = crate::router::assign_shard_in_group(shard_id, group.nodes(), rf);
        // Rotate through replicas for intra-group load balancing
        let selected = replicas[(query_seq as usize) % replicas.len()].clone();
        shard_to_node.insert(shard_id, selected);
    }

    // Initially target all shards
    let target_shards: Vec<u32> = (0..shard_count).collect();

    // Default deadline: 5 seconds (configurable in production)
    let deadline_ms = 5000;

    // Hedging is eligible when we have multiple nodes in the group (reserved for §13.2)
    let hedging_eligible = group.node_count() > 1;

    ScatterPlan {
        chosen_group,
        target_shards,
        shard_to_node,
        deadline_ms,
        hedging_eligible,
    }
}

/// Execute a scatter operation against the covering set.
///
/// Fans out the search request to all nodes in the plan, handling partial
/// failures according to the unavailable shard policy.
///
/// # Arguments
/// * `plan` - Scatter plan from `plan_search_scatter`
/// * `client` - HTTP client for communicating with nodes
/// * `req` - Search request to execute
/// * `topology` - Current topology (for resolving node addresses)
/// * `policy` - Policy for handling unavailable shards
///
/// # Returns
/// A `ScatterResult` containing successful responses and any errors.
pub async fn execute_scatter<C: NodeClient>(
    plan: ScatterPlan,
    client: &C,
    req: SearchRequest,
    topology: &Topology,
    policy: UnavailableShardPolicy,
) -> Result<ScatterResult> {
    use std::collections::HashMap;

    // Group requests by unique node (scatter happens once per node, not per shard)
    let mut node_to_shards: HashMap<NodeId, Vec<u32>> = HashMap::new();
    for (&shard_id, node_id) in &plan.shard_to_node {
        if plan.target_shards.contains(&shard_id) {
            node_to_shards
                .entry(node_id.clone())
                .or_default()
                .push(shard_id);
        }
    }

    let mut shard_pages = Vec::new();
    let mut failed_shards = HashMap::new();
    let mut deadline_exceeded = false;

    // Execute requests in parallel (one per unique node)
    let mut tasks = Vec::new();
    for (node_id, shards) in node_to_shards {
        let node = match topology.node(&node_id) {
            Some(n) => n.clone(),
            None => {
                // Node not found in topology — mark all its shards as failed
                for shard_id in shards {
                    failed_shards.insert(
                        shard_id,
                        NodeError::NetworkError("node not in topology".to_string()),
                    );
                }
                continue;
            }
        };

        let client_ref = client;
        let req_clone = req.clone();
        let node_id_clone = node_id.clone();

        tasks.push(async move {
            let result = client_ref
                .search_node(&node_id_clone, &node.address, &req_clone)
                .await;

            (node_id_clone, shards, result)
        });
    }

    // Await all tasks
    let results = futures_util::future::join_all(tasks).await;

    for (_node_id, shards, result) in results {
        match result {
            Ok(body) => {
                // Create a ShardHitPage for each shard served by this node
                for _shard_id in shards {
                    shard_pages.push(ShardHitPage { body: body.clone() });
                }
            }
            Err(NodeError::Timeout) => {
                deadline_exceeded = true;
                for shard_id in shards {
                    failed_shards.insert(shard_id, NodeError::Timeout);
                }
            }
            Err(e) => {
                for shard_id in shards {
                    failed_shards.insert(shard_id, e.clone());
                }
            }
        }
    }

    // Determine if response is partial
    let partial = !failed_shards.is_empty();

    // Apply unavailable shard policy
    match policy {
        UnavailableShardPolicy::Error => {
            if !failed_shards.is_empty() {
                return Err(crate::error::MiroirError::Routing(format!(
                    "{} shard(s) unavailable",
                    failed_shards.len()
                )));
            }
        }
        UnavailableShardPolicy::Partial => {
            // Return partial results (already done)
        }
        UnavailableShardPolicy::Fallback => {
            // Reserved for §13.2 Phase 5: query other replica groups for failed shards
            // For now, treat as Partial
        }
    }

    Ok(ScatterResult {
        shard_pages,
        failed_shards,
        partial,
        deadline_exceeded,
    })
}

/// Stubs for testing (no actual network calls).

/// Mock `NodeClient` for testing.
#[derive(Debug, Clone, Default)]
pub struct MockNodeClient {
    /// Optional pre-programmed responses per node ID.
    pub responses: HashMap<NodeId, Value>,

    /// Optional pre-programmed errors per node ID.
    pub errors: HashMap<NodeId, NodeError>,

    /// Optional delay for simulating slow nodes.
    pub delay_ms: u64,
}

impl NodeClient for MockNodeClient {
    async fn search_node(
        &self,
        node: &NodeId,
        _address: &str,
        _request: &SearchRequest,
    ) -> std::result::Result<Value, NodeError> {
        // Simulate network delay if configured
        // Note: actual sleep requires tokio runtime; this is a no-op placeholder
        let _ = self.delay_ms;

        // Check for pre-programmed error
        if let Some(err) = self.errors.get(node) {
            return Err(err.clone());
        }

        // Return pre-programmed response or default empty response
        Ok(self.responses.get(node).cloned().unwrap_or_else(|| {
            serde_json::json!({
                "hits": [],
                "estimatedTotalHits": 0,
                "processingTimeMs": 0,
                "facetDistribution": {},
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{Node, NodeId};

    fn make_test_topology() -> Topology {
        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            let mut node = Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            );
            node.status = crate::topology::NodeStatus::Active;
            topo.add_node(node);
        }
        topo
    }

    #[test]
    fn test_plan_search_scatter_pure_function() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);

        assert_eq!(plan.chosen_group, 0);
        assert_eq!(plan.target_shards.len(), 64);
        assert_eq!(plan.shard_to_node.len(), 64);
        assert_eq!(plan.deadline_ms, 5000);
        assert!(plan.hedging_eligible);
    }

    #[test]
    fn test_plan_search_scatter_query_group_rotation() {
        let topo = make_test_topology();

        // query_seq 0 → group 0
        let plan0 = plan_search_scatter(&topo, 0, 2, 64);
        assert_eq!(plan0.chosen_group, 0);

        // query_seq 1 → group 1
        let plan1 = plan_search_scatter(&topo, 1, 2, 64);
        assert_eq!(plan1.chosen_group, 1);

        // query_seq 2 → group 0
        let plan2 = plan_search_scatter(&topo, 2, 2, 64);
        assert_eq!(plan2.chosen_group, 0);
    }

    #[test]
    fn test_plan_search_scatter_shard_to_node_mapping() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);

        // All shards should be mapped to a node
        for shard_id in 0..64 {
            assert!(
                plan.shard_to_node.contains_key(&shard_id),
                "Shard {} not in mapping",
                shard_id
            );
        }

        // All nodes should be from group 0
        let g0 = topo.group(0).unwrap();
        for (_shard_id, node_id) in &plan.shard_to_node {
            assert!(
                g0.nodes().contains(node_id),
                "Node {:?} not in group 0",
                node_id
            );
        }
    }

    #[test]
    fn test_plan_search_scatter_hedging_eligibility() {
        let mut topo = Topology::new(64, 1, 1);
        // Single node group
        topo.add_node(Node::new(
            NodeId::new("node-0".to_string()),
            "http://node-0:7700".to_string(),
            0,
        ));

        let plan = plan_search_scatter(&topo, 0, 1, 64);
        assert!(!plan.hedging_eligible);

        // Multi-node group
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        assert!(plan.hedging_eligible);
    }

    #[tokio::test]
    async fn test_execute_scatter_with_mock_client() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);

        let mut client = MockNodeClient::default();
        client.responses.insert(
            NodeId::new("node-0".to_string()),
            serde_json::json!({
                "hits": [{"id": "doc1", "title": "Test"}],
                "estimatedTotalHits": 1,
                "processingTimeMs": 5,
            }),
        );

        let req = SearchRequest {
            index_uid: "test".to_string(),
            query: Some("test".to_string()),
            offset: 0,
            limit: 10,
            filter: None,
            facets: None,
            ranking_score: false,
            body: serde_json::json!({}),
        };

        let result = execute_scatter(plan, &client, req, &topo, UnavailableShardPolicy::Partial)
            .await
            .unwrap();

        assert!(!result.partial);
        assert!(!result.deadline_exceeded);
        assert_eq!(result.shard_pages.len(), 64); // One page per shard
        assert!(result.failed_shards.is_empty());
    }

    #[tokio::test]
    async fn test_execute_scatter_partial_failure() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);

        let mut client = MockNodeClient::default();
        // Make node-0 fail
        client.errors.insert(
            NodeId::new("node-0".to_string()),
            NodeError::Timeout,
        );
        client.responses.insert(
            NodeId::new("node-1".to_string()),
            serde_json::json!({
                "hits": [],
                "estimatedTotalHits": 0,
                "processingTimeMs": 0,
            }),
        );

        let req = SearchRequest {
            index_uid: "test".to_string(),
            query: Some("test".to_string()),
            offset: 0,
            limit: 10,
            filter: None,
            facets: None,
            ranking_score: false,
            body: serde_json::json!({}),
        };

        let result = execute_scatter(plan, &client, req, &topo, UnavailableShardPolicy::Partial)
            .await
            .unwrap();

        assert!(result.partial);
        assert!(!result.failed_shards.is_empty());
        // Some shards should still succeed (those on node-1 and node-2)
        assert!(!result.shard_pages.is_empty());
    }

    #[tokio::test]
    async fn test_execute_scatter_error_policy() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);

        let mut client = MockNodeClient::default();
        client.errors.insert(
            NodeId::new("node-0".to_string()),
            NodeError::Timeout,
        );

        let req = SearchRequest {
            index_uid: "test".to_string(),
            query: Some("test".to_string()),
            offset: 0,
            limit: 10,
            filter: None,
            facets: None,
            ranking_score: false,
            body: serde_json::json!({}),
        };

        let result = execute_scatter(plan, &client, req, &topo, UnavailableShardPolicy::Error).await;

        assert!(result.is_err());
    }

    #[test]
    fn test_node_error_variants() {
        let timeout = NodeError::Timeout;
        assert!(matches!(timeout, NodeError::Timeout));

        let http_err = NodeError::HttpError {
            status: 500,
            body: "Internal Server Error".to_string(),
        };
        assert!(matches!(http_err, NodeError::HttpError { .. }));

        let net_err = NodeError::NetworkError("connection refused".to_string());
        assert!(matches!(net_err, NodeError::NetworkError(_)));
    }
}
