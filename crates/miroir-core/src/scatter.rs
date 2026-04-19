//! Scatter orchestration: fan-out logic and covering set builder.

use crate::config::UnavailableShardPolicy;
use crate::merger::{MergeInput, MergedSearchResult, MergeStrategy, ShardHitPage};
use crate::router::{covering_set, query_group};
use crate::topology::{NodeId, Topology};
use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Scatter plan: the exact shard→node mapping for a search query.
#[derive(Debug, Clone)]
pub struct ScatterPlan {
    pub chosen_group: u32,
    pub target_shards: Vec<u32>,
    pub shard_to_node: HashMap<u32, NodeId>,
    pub deadline_ms: u32,
    pub hedging_eligible: bool,
}

// ---------------------------------------------------------------------------
// §15 OP#4: Global-IDF preflight (dfs_query_then_fetch pattern)
// ---------------------------------------------------------------------------

/// Per-term document frequency from a single shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermStats {
    pub df: u64,
}

/// Preflight request: gather term-frequency statistics from a shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightRequest {
    pub index_uid: String,
    pub terms: Vec<String>,
    pub filter: Option<Value>,
}

/// Response from a shard's preflight query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightResponse {
    pub total_docs: u64,
    pub avg_doc_length: f64,
    pub term_stats: HashMap<String, TermStats>,
}

/// Aggregated global term statistics after coordinator aggregation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalTermStats {
    pub df: u64,
    pub idf: f64,
}

/// Aggregated global IDF data computed at the coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalIdf {
    pub total_docs: u64,
    pub avg_doc_length: f64,
    pub terms: HashMap<String, GlobalTermStats>,
}

impl GlobalIdf {
    /// Aggregate per-shard preflight responses into global IDF.
    pub fn from_preflight_responses(responses: &[PreflightResponse]) -> Self {
        let mut total_docs = 0u64;
        let mut total_length = 0.0f64;
        let mut term_df: HashMap<String, u64> = HashMap::new();

        for resp in responses {
            total_docs += resp.total_docs;
            total_length += resp.avg_doc_length * resp.total_docs as f64;
            for (term, stats) in &resp.term_stats {
                *term_df.entry(term.clone()).or_insert(0) += stats.df;
            }
        }

        let avg_doc_length = if total_docs > 0 {
            total_length / total_docs as f64
        } else {
            0.0
        };

        let n = total_docs as f64;
        let terms = term_df
            .into_iter()
            .map(|(term, df)| {
                let idf = if df == 0 {
                    0.0
                } else {
                    ((n - df as f64 + 0.5) / (df as f64 + 0.5)).ln() + 1.0
                };
                (term, GlobalTermStats { df, idf })
            })
            .collect();

        Self { total_docs, avg_doc_length, terms }
    }
}

// ---------------------------------------------------------------------------
// NodeClient trait
// ---------------------------------------------------------------------------

/// HTTP client for communicating with a Meilisearch node.
#[allow(async_fn_in_trait)]
pub trait NodeClient: Send + Sync {
    async fn search_node(
        &self,
        node: &NodeId,
        address: &str,
        request: &SearchRequest,
    ) -> std::result::Result<Value, NodeError>;

    /// Execute a preflight request (OP#4 global-IDF phase).
    async fn preflight_node(
        &self,
        _node: &NodeId,
        _address: &str,
        _request: &PreflightRequest,
    ) -> std::result::Result<PreflightResponse, NodeError> {
        Ok(PreflightResponse { total_docs: 0, avg_doc_length: 0.0, term_stats: HashMap::new() })
    }
}

#[derive(Debug, Clone)]
pub enum NodeError {
    Timeout,
    HttpError { status: u16, body: String },
    NetworkError(String),
}

#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub index_uid: String,
    pub query: Option<String>,
    pub offset: usize,
    pub limit: usize,
    pub filter: Option<Value>,
    pub facets: Option<Vec<String>>,
    pub ranking_score: bool,
    pub body: Value,
    /// Global IDF data from the preflight phase (OP#4).
    pub global_idf: Option<GlobalIdf>,
}

#[derive(Debug)]
pub struct ScatterResult {
    pub shard_pages: Vec<ShardHitPage>,
    pub failed_shards: HashMap<u32, NodeError>,
    pub partial: bool,
    pub deadline_exceeded: bool,
}

pub fn plan_search_scatter(
    topology: &Topology,
    query_seq: u64,
    rf: usize,
    shard_count: u32,
) -> ScatterPlan {
    let chosen_group = query_group(query_seq, topology.replica_group_count());

    let group = match topology.group(chosen_group) {
        Some(g) => g,
        None => {
            return ScatterPlan {
                chosen_group, target_shards: Vec::new(),
                shard_to_node: HashMap::new(), deadline_ms: 0, hedging_eligible: false,
            };
        }
    };

    let _covering = covering_set(shard_count, group, rf, query_seq);

    let mut shard_to_node = HashMap::new();
    for shard_id in 0..shard_count {
        let replicas = crate::router::assign_shard_in_group(shard_id, group.nodes(), rf);
        let selected = replicas[(query_seq as usize) % replicas.len()].clone();
        shard_to_node.insert(shard_id, selected);
    }

    ScatterPlan {
        chosen_group,
        target_shards: (0..shard_count).collect(),
        shard_to_node,
        deadline_ms: 5000,
        hedging_eligible: group.node_count() > 1,
    }
}

pub async fn execute_scatter<C: NodeClient>(
    plan: ScatterPlan,
    client: &C,
    req: SearchRequest,
    topology: &Topology,
    policy: UnavailableShardPolicy,
) -> Result<ScatterResult> {
    let mut node_to_shards: HashMap<NodeId, Vec<u32>> = HashMap::new();
    for (&shard_id, node_id) in &plan.shard_to_node {
        if plan.target_shards.contains(&shard_id) {
            node_to_shards.entry(node_id.clone()).or_default().push(shard_id);
        }
    }

    let mut shard_pages = Vec::new();
    let mut failed_shards = HashMap::new();
    let mut deadline_exceeded = false;

    let mut tasks = Vec::new();
    for (node_id, shards) in node_to_shards {
        let node = match topology.node(&node_id) {
            Some(n) => n.clone(),
            None => {
                for shard_id in shards {
                    failed_shards.insert(shard_id, NodeError::NetworkError("node not in topology".to_string()));
                }
                continue;
            }
        };
        let client_ref = client;
        let req_clone = req.clone();
        let node_id_clone = node_id.clone();
        tasks.push(async move {
            let result = client_ref.search_node(&node_id_clone, &node.address, &req_clone).await;
            (node_id_clone, shards, result)
        });
    }

    let results = futures_util::future::join_all(tasks).await;

    for (_node_id, shards, result) in results {
        match result {
            Ok(body) => {
                for _shard_id in shards {
                    shard_pages.push(ShardHitPage { body: body.clone() });
                }
            }
            Err(NodeError::Timeout) => {
                deadline_exceeded = true;
                for shard_id in shards { failed_shards.insert(shard_id, NodeError::Timeout); }
            }
            Err(e) => {
                for shard_id in shards { failed_shards.insert(shard_id, e.clone()); }
            }
        }
    }

    let partial = !failed_shards.is_empty();

    match policy {
        UnavailableShardPolicy::Error => {
            if !failed_shards.is_empty() {
                return Err(crate::error::MiroirError::Routing(format!("{} shard(s) unavailable", failed_shards.len())));
            }
        }
        UnavailableShardPolicy::Partial => {}
        UnavailableShardPolicy::Fallback => {}
    }

    Ok(ScatterResult { shard_pages, failed_shards, partial, deadline_exceeded })
}

pub async fn scatter_gather_search<C: NodeClient>(
    plan: ScatterPlan,
    client: &C,
    req: SearchRequest,
    topology: &Topology,
    policy: UnavailableShardPolicy,
    strategy: &dyn MergeStrategy,
) -> Result<MergedSearchResult> {
    let scatter_result = execute_scatter(plan, client, req.clone(), topology, policy).await?;

    let mut shard_pages = scatter_result.shard_pages;
    if scatter_result.partial {
        for shard_id in scatter_result.failed_shards.keys() {
            shard_pages.push(ShardHitPage {
                body: serde_json::json!({"success": false, "message": format!("shard {} unavailable", shard_id)}),
            });
        }
    }

    let merge_input = MergeInput {
        shard_hits: shard_pages,
        offset: req.offset,
        limit: req.limit,
        client_requested_score: req.ranking_score,
        facets: req.facets.clone(),
    };

    strategy.merge(merge_input)
}

// ---------------------------------------------------------------------------
// OP#4: Global-IDF preflight execution
// ---------------------------------------------------------------------------

/// Extract unique query terms from a search query string.
pub fn extract_query_terms(query: &Option<String>) -> Vec<String> {
    match query {
        Some(q) if !q.is_empty() => {
            let mut seen = std::collections::HashSet::new();
            let mut terms = Vec::new();
            for term in q.split_whitespace() {
                let lower = term.to_lowercase();
                if seen.insert(lower.clone()) { terms.push(lower); }
            }
            terms
        }
        _ => Vec::new(),
    }
}

/// Execute the preflight phase: gather term frequencies from all shards.
pub async fn execute_preflight<C: NodeClient>(
    plan: &ScatterPlan,
    client: &C,
    req: &PreflightRequest,
    topology: &Topology,
) -> Result<GlobalIdf> {
    if req.terms.is_empty() {
        return Ok(GlobalIdf { total_docs: 0, avg_doc_length: 0.0, terms: HashMap::new() });
    }

    let mut node_to_shards: HashMap<NodeId, Vec<u32>> = HashMap::new();
    for (&shard_id, node_id) in &plan.shard_to_node {
        if plan.target_shards.contains(&shard_id) {
            node_to_shards.entry(node_id.clone()).or_default().push(shard_id);
        }
    }

    let mut tasks = Vec::new();
    for (node_id, _) in node_to_shards {
        let node = match topology.node(&node_id) {
            Some(n) => n.clone(),
            None => continue,
        };
        let client_ref = client;
        let req_clone = req.clone();
        let nid = node_id.clone();
        tasks.push(async move { client_ref.preflight_node(&nid, &node.address, &req_clone).await });
    }

    let results = futures_util::future::join_all(tasks).await;
    let responses: Vec<PreflightResponse> = results.into_iter().filter_map(|r| r.ok()).collect();
    Ok(GlobalIdf::from_preflight_responses(&responses))
}

/// Execute a full dfs_query_then_fetch search (OP#4 global-IDF preflight).
pub async fn dfs_query_then_fetch_search<C: NodeClient>(
    plan: ScatterPlan,
    client: &C,
    req: SearchRequest,
    topology: &Topology,
    policy: UnavailableShardPolicy,
    strategy: &dyn MergeStrategy,
) -> Result<MergedSearchResult> {
    let preflight_req = PreflightRequest {
        index_uid: req.index_uid.clone(),
        terms: extract_query_terms(&req.query),
        filter: req.filter.clone(),
    };
    let global_idf = execute_preflight(&plan, client, &preflight_req, topology).await?;
    let mut search_req = req;
    search_req.global_idf = Some(global_idf);
    scatter_gather_search(plan, client, search_req, topology, policy, strategy).await
}

// ---------------------------------------------------------------------------
// Mock client
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct MockNodeClient {
    pub responses: HashMap<NodeId, Value>,
    pub preflight_responses: HashMap<NodeId, PreflightResponse>,
    pub errors: HashMap<NodeId, NodeError>,
    pub delay_ms: u64,
}

impl NodeClient for MockNodeClient {
    async fn search_node(
        &self, node: &NodeId, _address: &str, _request: &SearchRequest,
    ) -> std::result::Result<Value, NodeError> {
        let _ = self.delay_ms;
        if let Some(err) = self.errors.get(node) { return Err(err.clone()); }
        Ok(self.responses.get(node).cloned().unwrap_or_else(|| {
            serde_json::json!({"hits": [], "estimatedTotalHits": 0, "processingTimeMs": 0, "facetDistribution": {}})
        }))
    }

    async fn preflight_node(
        &self, node: &NodeId, _address: &str, _request: &PreflightRequest,
    ) -> std::result::Result<PreflightResponse, NodeError> {
        if let Some(err) = self.errors.get(node) { return Err(err.clone()); }
        Ok(self.preflight_responses.get(node).cloned().unwrap_or_else(|| {
            PreflightResponse { total_docs: 1000, avg_doc_length: 50.0, term_stats: HashMap::new() }
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
            let mut node = Node::new(NodeId::new(format!("node-{i}")), format!("http://node-{i}:7700"), rg);
            node.status = crate::topology::NodeStatus::Active;
            topo.add_node(node);
        }
        topo
    }

    fn make_req() -> SearchRequest {
        SearchRequest {
            index_uid: "test".into(), query: Some("test".into()),
            offset: 0, limit: 10, filter: None, facets: None,
            ranking_score: false, body: serde_json::json!({}), global_idf: None,
        }
    }

    #[test]
    fn test_plan_pure_function() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        assert_eq!(plan.chosen_group, 0);
        assert_eq!(plan.target_shards.len(), 64);
        assert!(plan.hedging_eligible);
    }

    #[test]
    fn test_plan_group_rotation() {
        let topo = make_test_topology();
        assert_eq!(plan_search_scatter(&topo, 0, 2, 64).chosen_group, 0);
        assert_eq!(plan_search_scatter(&topo, 1, 2, 64).chosen_group, 1);
    }

    #[test]
    fn test_plan_shard_mapping() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        for s in 0..64 { assert!(plan.shard_to_node.contains_key(&s)); }
        let g0 = topo.group(0).unwrap();
        for (_, nid) in &plan.shard_to_node { assert!(g0.nodes().contains(nid)); }
    }

    #[test]
    fn test_plan_hedging() {
        let mut topo = Topology::new(64, 1, 1);
        topo.add_node(Node::new(NodeId::new("n0".into()), "http://n0:7700".into(), 0));
        assert!(!plan_search_scatter(&topo, 0, 1, 64).hedging_eligible);
        assert!(plan_search_scatter(&make_test_topology(), 0, 2, 64).hedging_eligible);
    }

    #[tokio::test]
    async fn test_scatter_mock() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        let mut c = MockNodeClient::default();
        c.responses.insert(NodeId::new("node-0"), serde_json::json!({"hits": [{"id": "doc1"}], "estimatedTotalHits": 1, "processingTimeMs": 5}));
        let r = execute_scatter(plan, &c, make_req(), &topo, UnavailableShardPolicy::Partial).await.unwrap();
        assert!(!r.partial);
        assert_eq!(r.shard_pages.len(), 64);
    }

    #[tokio::test]
    async fn test_scatter_partial() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        let mut c = MockNodeClient::default();
        c.errors.insert(NodeId::new("node-0"), NodeError::Timeout);
        let r = execute_scatter(plan, &c, make_req(), &topo, UnavailableShardPolicy::Partial).await.unwrap();
        assert!(r.partial);
    }

    #[tokio::test]
    async fn test_scatter_error_policy() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        let mut c = MockNodeClient::default();
        c.errors.insert(NodeId::new("node-0"), NodeError::Timeout);
        assert!(execute_scatter(plan, &c, make_req(), &topo, UnavailableShardPolicy::Error).await.is_err());
    }

    #[test]
    fn test_plan_invalid_group() {
        assert!(plan_search_scatter(&Topology::new(64, 0, 1), 0, 1, 64).shard_to_node.is_empty());
    }

    #[tokio::test]
    async fn test_scatter_node_not_in_topo() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        let r = execute_scatter(plan, &MockNodeClient::default(), make_req(), &Topology::new(64, 2, 2), UnavailableShardPolicy::Partial).await.unwrap();
        assert!(r.partial);
    }

    #[tokio::test]
    async fn test_sg_rrf() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        let mut c = MockNodeClient::default();
        c.responses.insert(NodeId::new("node-0"), serde_json::json!({"hits": [{"id": "a", "_rankingScore": 0.9}], "estimatedTotalHits": 1, "processingTimeMs": 5}));
        let s = crate::merger::RrfStrategy::default_strategy();
        let r = scatter_gather_search(plan, &c, make_req(), &topo, UnavailableShardPolicy::Partial, &s).await.unwrap();
        assert!(!r.degraded);
    }

    #[tokio::test]
    async fn test_sg_degraded() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        let mut c = MockNodeClient::default();
        c.responses.insert(NodeId::new("node-0"), serde_json::json!({"hits": [{"id": "a"}], "estimatedTotalHits": 1, "processingTimeMs": 5}));
        c.errors.insert(NodeId::new("node-2"), NodeError::Timeout);
        let s = crate::merger::RrfStrategy::default_strategy();
        assert!(scatter_gather_search(plan, &c, make_req(), &topo, UnavailableShardPolicy::Partial, &s).await.unwrap().degraded);
    }

    #[test]
    fn test_extract_query_terms() {
        assert_eq!(extract_query_terms(&Some("hello world hello".into())), vec!["hello", "world"]);
        assert!(extract_query_terms(&None).is_empty());
    }

    #[test]
    fn test_global_idf_aggregation() {
        let resp = vec![
            PreflightResponse { total_docs: 50000, avg_doc_length: 50.0, term_stats: HashMap::from([("a".into(), TermStats { df: 5000 })]) },
            PreflightResponse { total_docs: 50000, avg_doc_length: 60.0, term_stats: HashMap::from([("a".into(), TermStats { df: 4500 })]) },
        ];
        let g = GlobalIdf::from_preflight_responses(&resp);
        assert_eq!(g.total_docs, 100000);
        assert!((g.avg_doc_length - 55.0).abs() < 0.001);
        assert_eq!(g.terms.get("a").unwrap().df, 9500);
    }

    #[test]
    fn test_global_idf_empty() {
        let g = GlobalIdf::from_preflight_responses(&[]);
        assert_eq!(g.total_docs, 0);
        assert!(g.terms.is_empty());
    }

    #[tokio::test]
    async fn test_execute_preflight() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        let mut c = MockNodeClient::default();
        c.preflight_responses.insert(NodeId::new("node-0"), PreflightResponse {
            total_docs: 30000, avg_doc_length: 50.0,
            term_stats: HashMap::from([("search".into(), TermStats { df: 3000 })]),
        });
        c.preflight_responses.insert(NodeId::new("node-1"), PreflightResponse {
            total_docs: 30000, avg_doc_length: 55.0,
            term_stats: HashMap::from([("search".into(), TermStats { df: 2500 })]),
        });
        c.preflight_responses.insert(NodeId::new("node-2"), PreflightResponse {
            total_docs: 40000, avg_doc_length: 52.0,
            term_stats: HashMap::from([("search".into(), TermStats { df: 4000 })]),
        });
        let req = PreflightRequest { index_uid: "test".into(), terms: vec!["search".into()], filter: None };
        let g = execute_preflight(&plan, &c, &req, &topo).await.unwrap();
        assert_eq!(g.total_docs, 100000);
        assert_eq!(g.terms.get("search").unwrap().df, 9500);
    }

    #[tokio::test]
    async fn test_dfs_query_then_fetch() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        let mut c = MockNodeClient::default();
        c.responses.insert(NodeId::new("node-0"), serde_json::json!({"hits": [{"id": "a", "_rankingScore": 0.9}], "estimatedTotalHits": 1, "processingTimeMs": 5}));
        c.preflight_responses.insert(NodeId::new("node-0"), PreflightResponse {
            total_docs: 50000, avg_doc_length: 50.0,
            term_stats: HashMap::from([("test".into(), TermStats { df: 500 })]),
        });
        let s = crate::merger::RrfStrategy::default_strategy();
        let r = dfs_query_then_fetch_search(plan, &c, make_req(), &topo, UnavailableShardPolicy::Partial, &s).await.unwrap();
        assert!(!r.degraded);
        assert!(!r.hits.is_empty());
    }
}
