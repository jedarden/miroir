//! Scatter orchestration: fan-out logic and covering set builder.

use crate::config::UnavailableShardPolicy;
use tracing::{instrument, info_span, Instrument};
use crate::merger::{MergeInput, MergedSearchResult, MergeStrategy, ShardHitPage};
use crate::router::{covering_set, covering_set_with_version_floor, query_group};
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

// ---------------------------------------------------------------------------
// Write path: document operations (P2.2)
// ---------------------------------------------------------------------------

/// Request to add/replace documents on a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequest {
    pub index_uid: String,
    pub documents: Vec<Value>,
    pub primary_key: Option<String>,
    /// Internal origin tag for CDC event suppression (plan §13.13).
    /// - None = client write (always emitted to CDC)
    /// - Some("antientropy") = anti-entropy repair write (suppressed unless emit_internal_writes)
    /// - Some("reshard_backfill") = reshard backfill write (suppressed unless emit_internal_writes)
    /// - Some("rollover") = ILM rollover write (suppressed unless emit_internal_writes)
    /// - Some("ttl_expire") = TTL expiration delete (suppressed unless emit_ttl_deletes)
    ///
    /// This field is NEVER stored on documents and never returned to clients.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
}

/// Response from a single node's document write operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResponse {
    pub success: bool,
    pub task_uid: Option<u64>,
    pub message: Option<String>,
    pub code: Option<String>,
    pub error_type: Option<String>,
}

/// Request to delete documents by IDs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteByIdsRequest {
    pub index_uid: String,
    pub ids: Vec<String>,
    /// Internal origin tag for CDC event suppression (plan §13.13).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
}

/// Request to delete all documents matching a filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteByFilterRequest {
    pub index_uid: String,
    pub filter: Value,
    /// Internal origin tag for CDC event suppression (plan §13.13).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
}

/// Response from a delete operation.
pub type DeleteResponse = WriteResponse;

/// Request to fetch documents with a filter (used for shard migration).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchDocumentsRequest {
    pub index_uid: String,
    pub filter: Value,
    pub limit: u32,
    pub offset: u32,
}

/// Response from a fetch documents operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchDocumentsResponse {
    pub results: Vec<Value>,
    pub limit: u32,
    pub offset: u32,
    pub total: u64,
}

/// Request to get task status from a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusRequest {
    /// The task UID to query
    pub task_uid: u64,
}

/// Response from a single node's task status query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusResponse {
    /// The task UID
    pub task_uid: u64,
    /// Current task status
    pub status: String,
    /// Error message if failed
    pub error: Option<String>,
    /// Error type if failed
    #[serde(rename = "type")]
    pub error_type: Option<String>,
}

impl TaskStatusResponse {
    /// Convert Meilisearch status string to NodeTaskStatus.
    pub fn to_node_status(&self) -> crate::task::NodeTaskStatus {
        match self.status.as_str() {
            "enqueued" => crate::task::NodeTaskStatus::Enqueued,
            "processing" => crate::task::NodeTaskStatus::Processing,
            "succeeded" => crate::task::NodeTaskStatus::Succeeded,
            "failed" => crate::task::NodeTaskStatus::Failed,
            _ => crate::task::NodeTaskStatus::Enqueued,
        }
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

    /// Write documents to a node (add/replace).
    async fn write_documents(
        &self,
        _node: &NodeId,
        _address: &str,
        _request: &WriteRequest,
    ) -> std::result::Result<WriteResponse, NodeError> {
        Ok(WriteResponse {
            success: false,
            task_uid: None,
            message: Some("not implemented".to_string()),
            code: None,
            error_type: None,
        })
    }

    /// Get task status from a node.
    fn get_task_status(
        &self,
        _node: &NodeId,
        _address: &str,
        _request: &TaskStatusRequest,
    ) -> impl std::future::Future<Output = std::result::Result<TaskStatusResponse, NodeError>> + Send {
        async move {
            Ok(TaskStatusResponse {
                task_uid: _request.task_uid,
                status: "succeeded".to_string(),
                error: None,
                error_type: None,
            })
        }
    }

    /// Delete documents by IDs from a node.
    async fn delete_documents(
        &self,
        _node: &NodeId,
        _address: &str,
        _request: &DeleteByIdsRequest,
    ) -> std::result::Result<DeleteResponse, NodeError> {
        Ok(DeleteResponse {
            success: false,
            task_uid: None,
            message: Some("not implemented".to_string()),
            code: None,
            error_type: None,
        })
    }

    /// Delete all documents matching a filter from a node.
    async fn delete_documents_by_filter(
        &self,
        _node: &NodeId,
        _address: &str,
        _request: &DeleteByFilterRequest,
    ) -> std::result::Result<DeleteResponse, NodeError> {
        Ok(DeleteResponse {
            success: false,
            task_uid: None,
            message: Some("not implemented".to_string()),
            code: None,
            error_type: None,
        })
    }

    /// Fetch documents with a filter from a node (used for shard migration).
    async fn fetch_documents(
        &self,
        _node: &NodeId,
        _address: &str,
        _request: &FetchDocumentsRequest,
    ) -> std::result::Result<FetchDocumentsResponse, NodeError> {
        Ok(FetchDocumentsResponse {
            results: Vec::new(),
            limit: _request.limit,
            offset: _request.offset,
            total: 0,
        })
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

impl SearchRequest {
    /// Build the request body for sending to a node.
    ///
    /// Injects `showRankingScore: true` unconditionally so the merger can global-sort.
    /// Each node receives `offset + limit` results to ensure the coordinator has enough
    /// data to apply pagination.
    pub fn to_node_body(&self) -> Value {
        let mut body = self.body.clone();

        // Inject showRankingScore: true unconditionally for global sorting
        body["showRankingScore"] = serde_json::json!(true);

        // Set limit to offset + limit so we get enough results for pagination
        // (coordinator applies final offset/limit after merging)
        body["limit"] = serde_json::json!(self.offset + self.limit);

        // Set offset to 0 on individual nodes (coordinator handles offset)
        body["offset"] = serde_json::json!(0);

        // Ensure query is set
        if let Some(q) = &self.query {
            body["q"] = serde_json::json!(q);
        }

        // Ensure filter is set if provided
        if let Some(filter) = &self.filter {
            body["filter"] = filter.clone();
        }

        // Ensure facets are set if provided
        if let Some(facets) = &self.facets {
            body["facets"] = serde_json::json!(facets);
        }

        body
    }
}

#[derive(Debug)]
pub struct ScatterResult {
    pub shard_pages: Vec<ShardHitPage>,
    pub failed_shards: HashMap<u32, NodeError>,
    pub partial: bool,
    pub deadline_exceeded: bool,
}

#[instrument(skip_all, fields(query_seq, rf, shard_count))]
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

/// Plan search scatter with settings version floor filtering (plan §13.5).
///
/// Excludes nodes whose settings version for the given index is below `floor`.
/// Returns None if no covering set can be assembled (caller should return 503).
pub fn plan_search_scatter_with_version_floor(
    topology: &Topology,
    query_seq: u64,
    rf: usize,
    shard_count: u32,
    index: &str,
    floor: u64,
    version_checker: &impl Fn(&str, &str) -> u64,
) -> Option<ScatterPlan> {
    let chosen_group = query_group(query_seq, topology.replica_group_count());

    let group = topology.group(chosen_group)?;

    let covering = covering_set_with_version_floor(
        shard_count,
        group,
        rf,
        query_seq,
        index,
        floor,
        version_checker,
    )?;

    let mut shard_to_node = HashMap::new();
    for shard_id in 0..shard_count {
        let replicas = crate::router::assign_shard_in_group(shard_id, group.nodes(), rf);
        // Filter by version floor, then rotate by query_seq
        let eligible: Vec<_> = replicas
            .iter()
            .filter(|node_id| {
                let version = version_checker(index, node_id.as_str());
                version >= floor
            })
            .collect();

        if eligible.is_empty() {
            return None;
        }

        let selected = eligible[query_seq as usize % eligible.len()];
        shard_to_node.insert(shard_id, selected.clone());
    }

    Some(ScatterPlan {
        chosen_group,
        target_shards: (0..shard_count).collect(),
        shard_to_node,
        deadline_ms: 5000,
        hedging_eligible: group.node_count() > 1,
    })
}

/// Plan search scatter for a specific replica group (plan §13.6 session pinning).
///
/// Used when a session has a pending write and needs to read from the pinned group
/// to ensure read-your-writes consistency.
pub fn plan_search_scatter_for_group(
    topology: &Topology,
    query_seq: u64,
    rf: usize,
    shard_count: u32,
    pinned_group: u32,
) -> Option<ScatterPlan> {
    let group = topology.group(pinned_group)?;

    let mut shard_to_node = HashMap::new();
    for shard_id in 0..shard_count {
        let replicas = crate::router::assign_shard_in_group(shard_id, group.nodes(), rf);
        if replicas.is_empty() {
            continue;
        }
        let selected = replicas[query_seq as usize % replicas.len()].clone();
        shard_to_node.insert(shard_id, selected);
    }

    Some(ScatterPlan {
        chosen_group: pinned_group,
        target_shards: (0..shard_count).collect(),
        shard_to_node,
        deadline_ms: 5000,
        hedging_eligible: group.node_count() > 1,
    })
}

#[instrument(skip_all, fields(node_count))]
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
    tracing::Span::current().record("node_count", node_to_shards.len());

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
        let shard_count = shards.len();
        // Create a span for this node's scatter call
        let span = info_span!(
            "scatter_node",
            node_id = %node_id_clone,
            address = %node.address,
            shard_count = shard_count,
        );
        tasks.push(async move {
            let result = client_ref.search_node(&node_id_clone, &node.address, &req_clone).await;
            (node_id_clone, shards, result)
        }.instrument(span));
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
        UnavailableShardPolicy::Fallback => {
            // Group-unavailability fallback: try other replica groups for failed shards
            if !failed_shards.is_empty() {
                let mut fallback_pages = Vec::new();
                let mut remaining_failed = HashMap::new();

                for (&shard_id, error) in &failed_shards {
                    let mut fallback_succeeded = false;

                    // Try each other replica group
                    for group_id in 0..topology.replica_group_count() {
                        if group_id == plan.chosen_group {
                            continue; // Skip the already-tried group
                        }

                        if let Some(group) = topology.group(group_id) {
                            let replicas = crate::router::assign_shard_in_group(shard_id, group.nodes(), topology.rf());
                            if replicas.is_empty() {
                                continue;
                            }

                            // Try each replica in the fallback group
                            for node_id in replicas {
                                if let Some(node) = topology.node(&node_id) {
                                    match client.search_node(&node_id, &node.address, &req).await {
                                        Ok(body) => {
                                            fallback_pages.push(ShardHitPage { body });
                                            fallback_succeeded = true;
                                            break;
                                        }
                                        Err(_) => {
                                            continue; // Try next replica
                                        }
                                    }
                                }
                            }

                            if fallback_succeeded {
                                break; // Found a working replica
                            }
                        }
                    }

                    if !fallback_succeeded {
                        remaining_failed.insert(shard_id, error.clone());
                    }
                }

                // Merge fallback results
                shard_pages.extend(fallback_pages);

                // Update failed_shards with only those that truly failed
                if remaining_failed.is_empty() {
                    // All shards succeeded via fallback
                    return Ok(ScatterResult {
                        shard_pages,
                        failed_shards: HashMap::new(),
                        partial: false,
                        deadline_exceeded,
                    });
                } else {
                    // Some shards still failed
                    failed_shards = remaining_failed;
                }
            }
        }
    }

    Ok(ScatterResult { shard_pages, failed_shards, partial, deadline_exceeded })
}

#[instrument(skip_all, fields(index = %req.index_uid))]
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

    // Collect failed shard IDs for the X-Miroir-Degraded header
    let failed_shards: Vec<u32> = scatter_result.failed_shards.keys().copied().collect();

    let merge_input = MergeInput {
        shard_hits: shard_pages,
        offset: req.offset,
        limit: req.limit,
        client_requested_score: req.ranking_score,
        facets: req.facets.clone(),
        failed_shards,
    };

    // Span for the merge operation
    let _span = info_span!(
        "merge",
        shard_count = merge_input.shard_hits.len(),
        offset = req.offset,
        limit = req.limit,
    ).entered();

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
#[instrument(skip_all, fields(node_count, term_count = req.terms.len()))]
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
#[instrument(skip_all, fields(index = %req.index_uid))]
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
    pub write_responses: HashMap<NodeId, WriteResponse>,
    pub fetch_responses: HashMap<NodeId, FetchDocumentsResponse>,
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

    async fn write_documents(
        &self, node: &NodeId, _address: &str, _request: &WriteRequest,
    ) -> std::result::Result<WriteResponse, NodeError> {
        if let Some(err) = self.errors.get(node) { return Err(err.clone()); }
        Ok(self.write_responses.get(node).cloned().unwrap_or_else(|| {
            WriteResponse {
                success: true,
                task_uid: Some(1),
                message: None,
                code: None,
                error_type: None,
            }
        }))
    }

    async fn delete_documents(
        &self, node: &NodeId, _address: &str, _request: &DeleteByIdsRequest,
    ) -> std::result::Result<DeleteResponse, NodeError> {
        if let Some(err) = self.errors.get(node) { return Err(err.clone()); }
        Ok(DeleteResponse {
            success: true,
            task_uid: Some(1),
            message: None,
            code: None,
            error_type: None,
        })
    }

    async fn delete_documents_by_filter(
        &self, node: &NodeId, _address: &str, _request: &DeleteByFilterRequest,
    ) -> std::result::Result<DeleteResponse, NodeError> {
        if let Some(err) = self.errors.get(node) { return Err(err.clone()); }
        Ok(DeleteResponse {
            success: true,
            task_uid: Some(1),
            message: None,
            code: None,
            error_type: None,
        })
    }

    async fn fetch_documents(
        &self, node: &NodeId, _address: &str, request: &FetchDocumentsRequest,
    ) -> std::result::Result<FetchDocumentsResponse, NodeError> {
        if let Some(err) = self.errors.get(node) { return Err(err.clone()); }
        // Return stored fetch response if available, otherwise return empty
        let stored = self.fetch_responses.get(node).cloned().unwrap_or_else(|| {
            FetchDocumentsResponse {
                results: Vec::new(),
                limit: request.limit,
                offset: request.offset,
                total: 0,
            }
        });

        // Handle pagination: if offset exceeds total, return empty results
        if request.offset as u64 >= stored.total {
            return Ok(FetchDocumentsResponse {
                results: Vec::new(),
                limit: request.limit,
                offset: request.offset,
                total: stored.total,
            });
        }

        Ok(stored)
    }

    fn get_task_status(
        &self,
        node: &NodeId,
        _address: &str,
        _request: &TaskStatusRequest,
    ) -> impl std::future::Future<Output = std::result::Result<TaskStatusResponse, NodeError>> + Send {
        let node = node.clone();
        let task_uid = _request.task_uid;
        let error = self.errors.get(&node).cloned();
        async move {
            if let Some(err) = error {
                return Err(err);
            }
            Ok(TaskStatusResponse {
                task_uid,
                status: "succeeded".to_string(),
                error: None,
                error_type: None,
            })
        }
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
        c.responses.insert(NodeId::new("node-0".into()), serde_json::json!({"hits": [{"id": "doc1"}], "estimatedTotalHits": 1, "processingTimeMs": 5}));
        let r = execute_scatter(plan, &c, make_req(), &topo, UnavailableShardPolicy::Partial).await.unwrap();
        assert!(!r.partial);
        assert_eq!(r.shard_pages.len(), 64);
    }

    #[tokio::test]
    async fn test_scatter_partial() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        let mut c = MockNodeClient::default();
        c.errors.insert(NodeId::new("node-0".into()), NodeError::Timeout);
        let r = execute_scatter(plan, &c, make_req(), &topo, UnavailableShardPolicy::Partial).await.unwrap();
        assert!(r.partial);
    }

    #[tokio::test]
    async fn test_scatter_error_policy() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        let mut c = MockNodeClient::default();
        c.errors.insert(NodeId::new("node-0".into()), NodeError::Timeout);
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
        c.responses.insert(NodeId::new("node-0".into()), serde_json::json!({"hits": [{"id": "a", "_rankingScore": 0.9}], "estimatedTotalHits": 1, "processingTimeMs": 5}));
        let s = crate::merger::RrfStrategy::default_strategy();
        let r = scatter_gather_search(plan, &c, make_req(), &topo, UnavailableShardPolicy::Partial, &s).await.unwrap();
        assert!(!r.degraded);
    }

    #[tokio::test]
    async fn test_sg_degraded() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        let mut c = MockNodeClient::default();
        c.responses.insert(NodeId::new("node-0".into()), serde_json::json!({"hits": [{"id": "a"}], "estimatedTotalHits": 1, "processingTimeMs": 5}));
        c.errors.insert(NodeId::new("node-2".into()), NodeError::Timeout);
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
        c.preflight_responses.insert(NodeId::new("node-0".into()), PreflightResponse {
            total_docs: 30000, avg_doc_length: 50.0,
            term_stats: HashMap::from([("search".into(), TermStats { df: 3000 })]),
        });
        c.preflight_responses.insert(NodeId::new("node-1".into()), PreflightResponse {
            total_docs: 30000, avg_doc_length: 55.0,
            term_stats: HashMap::from([("search".into(), TermStats { df: 2500 })]),
        });
        c.preflight_responses.insert(NodeId::new("node-2".into()), PreflightResponse {
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
        c.responses.insert(NodeId::new("node-0".into()), serde_json::json!({"hits": [{"id": "a", "_rankingScore": 0.9}], "estimatedTotalHits": 1, "processingTimeMs": 5}));
        c.preflight_responses.insert(NodeId::new("node-0".into()), PreflightResponse {
            total_docs: 50000, avg_doc_length: 50.0,
            term_stats: HashMap::from([("test".into(), TermStats { df: 500 })]),
        });
        let s = crate::merger::RrfStrategy::default_strategy();
        let r = dfs_query_then_fetch_search(plan, &c, make_req(), &topo, UnavailableShardPolicy::Partial, &s).await.unwrap();
        assert!(!r.degraded);
        assert!(!r.hits.is_empty());
    }

    /// Integration test for dfs_query_then_fetch with severely skewed shard distribution.
    ///
    /// This test simulates the scenario described in miroir-yio:
    /// - Shard 0: Normal (1,000 docs, term df = 100)
    /// - Shard 1: 100x normal (100,000 docs, term df = 10,000)
    /// - Shard 2: Near-empty (10 docs, term df = 1)
    ///
    /// Without global IDF preflight, each shard would compute different local IDF values:
    /// - Shard 0: IDF ≈ log((1000 - 100 + 0.5) / (100 + 0.5)) + 1 ≈ 2.3
    /// - Shard 1: IDF ≈ log((100000 - 10000 + 0.5) / (10000 + 0.5)) + 1 ≈ 2.3
    /// - Shard 2: IDF ≈ log((10 - 1 + 0.5) / (1 + 0.5)) + 1 ≈ 2.8
    ///
    /// With global IDF preflight, all shards use the same IDF:
    /// - Global: N = 101,010, df = 10,101
    /// - IDF ≈ log((101010 - 10101 + 0.5) / (10101 + 0.5)) + 1 ≈ 2.3
    ///
    /// This ensures scores are comparable across shards, enabling correct score-based merge.
    #[tokio::test]
    async fn test_dfs_skewed_shards_global_idf_aggregation() {
        let mut topo = Topology::new(3, 1, 1);
        topo.add_node(Node::new(NodeId::new("node-0".into()), "http://node-0:7700".into(), 0));
        topo.add_node(Node::new(NodeId::new("node-1".into()), "http://node-1:7700".into(), 0));
        topo.add_node(Node::new(NodeId::new("node-2".into()), "http://node-2:7700".into(), 0));

        let plan = plan_search_scatter(&topo, 0, 1, 3);

        // Simulate severely skewed shard distribution
        let mut c = MockNodeClient::default();

        // Shard 0: Normal distribution
        c.preflight_responses.insert(NodeId::new("node-0".into()), PreflightResponse {
            total_docs: 1000,
            avg_doc_length: 50.0,
            term_stats: HashMap::from([
                ("machine".into(), TermStats { df: 100 }),
                ("learning".into(), TermStats { df: 80 }),
            ]),
        });

        // Shard 1: 100x normal (severely skewed)
        c.preflight_responses.insert(NodeId::new("node-1".into()), PreflightResponse {
            total_docs: 100_000,
            avg_doc_length: 55.0,
            term_stats: HashMap::from([
                ("machine".into(), TermStats { df: 10_000 }),
                ("learning".into(), TermStats { df: 8_000 }),
            ]),
        });

        // Shard 2: Near-empty
        c.preflight_responses.insert(NodeId::new("node-2".into()), PreflightResponse {
            total_docs: 10,
            avg_doc_length: 45.0,
            term_stats: HashMap::from([
                ("machine".into(), TermStats { df: 1 }),
                ("learning".into(), TermStats { df: 1 }),
            ]),
        });

        // Execute preflight to get global IDF
        let preflight_req = PreflightRequest {
            index_uid: "test".into(),
            terms: vec!["machine".into(), "learning".into()],
            filter: None,
        };

        let global_idf = execute_preflight(&plan, &c, &preflight_req, &topo).await.unwrap();

        // Verify global aggregation
        assert_eq!(global_idf.total_docs, 101_010);
        assert_eq!(global_idf.terms.get("machine").unwrap().df, 10_101);
        assert_eq!(global_idf.terms.get("learning").unwrap().df, 8_081);

        // Verify global IDF is the same for all shards
        // Expected IDF for "machine": log((101010 - 10101 + 0.5) / (10101 + 0.5)) + 1
        let expected_idf_machine: f64 = ((101010.0_f64 - 10101.0 + 0.5) / (10101.0 + 0.5)).ln() + 1.0;
        let actual_idf_machine = global_idf.terms.get("machine").unwrap().idf;
        assert!((actual_idf_machine - expected_idf_machine).abs() < 0.001);

        // Expected IDF for "learning": log((101010 - 8081 + 0.5) / (8081 + 0.5)) + 1
        let expected_idf_learning: f64 = ((101010.0_f64 - 8081.0 + 0.5) / (8081.0 + 0.5)).ln() + 1.0;
        let actual_idf_learning = global_idf.terms.get("learning").unwrap().idf;
        assert!((actual_idf_learning - expected_idf_learning).abs() < 0.001);

        // Verify that without global IDF, local IDF values would differ significantly
        // Shard 0 local IDF for "machine": log((1000 - 100 + 0.5) / (100 + 0.5)) + 1 ≈ 3.19
        // Shard 1 local IDF for "machine": log((100000 - 10000 + 0.5) / (10000 + 0.5)) + 1 ≈ 3.20
        // Shard 2 local IDF for "machine": log((10 - 1 + 0.5) / (1 + 0.5)) + 1 ≈ 2.85
        let local_idf_shard_0: f64 = ((1000.0_f64 - 100.0 + 0.5) / (100.0 + 0.5)).ln() + 1.0;
        let local_idf_shard_2: f64 = ((10.0_f64 - 1.0 + 0.5) / (1.0 + 0.5)).ln() + 1.0;
        assert!((local_idf_shard_2 - local_idf_shard_0).abs() > 0.2, "Local IDF values should differ significantly");
        assert!((local_idf_shard_2 - actual_idf_machine).abs() > 0.3, "Global IDF should be closer to large-shard local IDF");
    }

    /// Test that DFS preflight handles empty query terms gracefully.
    #[tokio::test]
    async fn test_dfs_empty_query_terms() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64);
        let c = MockNodeClient::default();

        let preflight_req = PreflightRequest {
            index_uid: "test".into(),
            terms: vec![],
            filter: None,
        };

        let global_idf = execute_preflight(&plan, &c, &preflight_req, &topo).await.unwrap();
        assert_eq!(global_idf.total_docs, 0);
        assert!(global_idf.terms.is_empty());
    }

    /// Test that DFS preflight handles partial failures gracefully.
    #[tokio::test]
    async fn test_dfs_partial_failure() {
        let mut topo = Topology::new(3, 1, 1);
        topo.add_node(Node::new(NodeId::new("node-0".into()), "http://node-0:7700".into(), 0));
        topo.add_node(Node::new(NodeId::new("node-1".into()), "http://node-1:7700".into(), 0));
        topo.add_node(Node::new(NodeId::new("node-2".into()), "http://node-2:7700".into(), 0));

        let plan = plan_search_scatter(&topo, 0, 1, 3);
        let mut c = MockNodeClient::default();

        // Node 0 returns valid data
        c.preflight_responses.insert(NodeId::new("node-0".into()), PreflightResponse {
            total_docs: 50000,
            avg_doc_length: 50.0,
            term_stats: HashMap::from([("test".into(), TermStats { df: 5000 })]),
        });

        // Node 1 returns valid data
        c.preflight_responses.insert(NodeId::new("node-1".into()), PreflightResponse {
            total_docs: 30000,
            avg_doc_length: 55.0,
            term_stats: HashMap::from([("test".into(), TermStats { df: 3000 })]),
        });

        // Node 2 fails
        c.errors.insert(NodeId::new("node-2".into()), NodeError::Timeout);

        let preflight_req = PreflightRequest {
            index_uid: "test".into(),
            terms: vec!["test".into()],
            filter: None,
        };

        // Should aggregate from successful nodes only
        let global_idf = execute_preflight(&plan, &c, &preflight_req, &topo).await.unwrap();
        assert_eq!(global_idf.total_docs, 80000);
        assert_eq!(global_idf.terms.get("test").unwrap().df, 8000);
    }

    /// Test that to_node_body correctly injects showRankingScore: true and sets limit to offset + limit.
    #[test]
    fn test_to_node_body_injects_show_ranking_score() {
        let req = SearchRequest {
            index_uid: "test".into(),
            query: Some("rust programming".into()),
            offset: 10,
            limit: 20,
            filter: Some(serde_json::json!("status = published")),
            facets: Some(vec!["category".into(), "tags".into()]),
            ranking_score: false, // Client didn't request scores
            body: serde_json::json!({"custom": "field"}),
            global_idf: None,
        };

        let body = req.to_node_body();

        // showRankingScore must be true unconditionally
        assert_eq!(body.get("showRankingScore"), Some(&serde_json::json!(true)));

        // limit must be offset + limit (coordinator pagination)
        assert_eq!(body.get("limit"), Some(&serde_json::json!(30)));

        // offset must be 0 (coordinator handles offset)
        assert_eq!(body.get("offset"), Some(&serde_json::json!(0)));

        // query must be set
        assert_eq!(body.get("q"), Some(&serde_json::json!("rust programming")));

        // filter must be set
        assert_eq!(body.get("filter"), Some(&serde_json::json!("status = published")));

        // facets must be set
        assert_eq!(body.get("facets"), Some(&serde_json::json!(["category", "tags"])));

        // custom body field must be preserved
        assert_eq!(body.get("custom"), Some(&serde_json::json!("field")));
    }

    /// Test that to_node_body works with minimal request.
    #[test]
    fn test_to_node_body_minimal_request() {
        let req = SearchRequest {
            index_uid: "test".into(),
            query: None,
            offset: 0,
            limit: 10,
            filter: None,
            facets: None,
            ranking_score: true, // Client requested scores
            body: serde_json::json!({}),
            global_idf: None,
        };

        let body = req.to_node_body();

        // showRankingScore must be true unconditionally
        assert_eq!(body.get("showRankingScore"), Some(&serde_json::json!(true)));

        // limit must be offset + limit = 10
        assert_eq!(body.get("limit"), Some(&serde_json::json!(10)));

        // offset must be 0
        assert_eq!(body.get("offset"), Some(&serde_json::json!(0)));
    }

    /// Test that to_node_body handles large offset/limit correctly.
    #[test]
    fn test_to_node_body_pagination() {
        let req = SearchRequest {
            index_uid: "test".into(),
            query: Some("test".into()),
            offset: 100,
            limit: 50,
            filter: None,
            facets: None,
            ranking_score: false,
            body: serde_json::json!({}),
            global_idf: None,
        };

        let body = req.to_node_body();

        // limit must be offset + limit = 150 (fetch enough for coordinator pagination)
        assert_eq!(body.get("limit"), Some(&serde_json::json!(150)));

        // offset must be 0 (coordinator handles offset)
        assert_eq!(body.get("offset"), Some(&serde_json::json!(0)));
    }

    /// Test group fallback when primary group has failed nodes.
    #[tokio::test]
    async fn test_group_fallback_on_partial_failure() {
        let mut topo = Topology::new(16, 2, 2);
        // Group 0: 2 nodes
        topo.add_node(Node::new(NodeId::new("node-g0-0".into()), "http://g0-0:7700".into(), 0));
        topo.add_node(Node::new(NodeId::new("node-g0-1".into()), "http://g0-1:7700".into(), 0));
        // Group 1: 2 nodes (healthy fallback targets)
        topo.add_node(Node::new(NodeId::new("node-g1-0".into()), "http://g1-0:7700".into(), 1));
        topo.add_node(Node::new(NodeId::new("node-g1-1".into()), "http://g1-1:7700".into(), 1));

        let plan = plan_search_scatter(&topo, 0, 2, 16); // query_seq=0 → group 0
        assert_eq!(plan.chosen_group, 0);

        let mut c = MockNodeClient::default();

        // Set up responses: all shards on group 1 nodes return valid data
        let response_1 = serde_json::json!({
            "hits": [{"id": "doc1", "_rankingScore": 0.9}],
            "estimatedTotalHits": 1,
            "processingTimeMs": 5,
        });
        c.responses.insert(NodeId::new("node-g1-0".into()), response_1.clone());
        c.responses.insert(NodeId::new("node-g1-1".into()), response_1);

        // All nodes in group 0 fail
        c.errors.insert(NodeId::new("node-g0-0".into()), NodeError::Timeout);
        c.errors.insert(NodeId::new("node-g1-0".into()), NodeError::Timeout);

        let req = make_req();

        // With fallback policy, shards should succeed via group 1
        let result = execute_scatter(plan, &c, req, &topo, UnavailableShardPolicy::Fallback).await.unwrap();

        // Should have succeeded via fallback (group 1)
        assert!(!result.partial, "Fallback should have succeeded");
        assert!(result.failed_shards.is_empty(), "No shards should have failed after fallback");
        assert!(!result.shard_pages.is_empty(), "Should have shard pages from fallback");
    }

    /// Test group fallback when both groups are down.
    #[tokio::test]
    async fn test_group_fallback_all_groups_down() {
        let mut topo = Topology::new(16, 2, 2);
        topo.add_node(Node::new(NodeId::new("node-g0-0".into()), "http://g0-0:7700".into(), 0));
        topo.add_node(Node::new(NodeId::new("node-g0-1".into()), "http://g0-1:7700".into(), 0));
        topo.add_node(Node::new(NodeId::new("node-g1-0".into()), "http://g1-0:7700".into(), 1));
        topo.add_node(Node::new(NodeId::new("node-g1-1".into()), "http://g1-1:7700".into(), 1));

        let plan = plan_search_scatter(&topo, 0, 2, 16);
        let mut c = MockNodeClient::default();

        // All nodes fail
        c.errors.insert(NodeId::new("node-g0-0".into()), NodeError::Timeout);
        c.errors.insert(NodeId::new("node-g0-1".into()), NodeError::Timeout);
        c.errors.insert(NodeId::new("node-g1-0".into()), NodeError::Timeout);
        c.errors.insert(NodeId::new("node-g1-1".into()), NodeError::Timeout);

        let req = make_req();

        // With fallback policy, should still fail when all groups are down
        let result = execute_scatter(plan, &c, req, &topo, UnavailableShardPolicy::Fallback).await.unwrap();

        assert!(result.partial, "Should be partial when all groups fail");
        assert!(!result.failed_shards.is_empty(), "Should have failed shards");
    }

    /// Test that partial policy does NOT use fallback.
    #[tokio::test]
    async fn test_partial_policy_no_fallback() {
        let mut topo = Topology::new(16, 2, 2);
        topo.add_node(Node::new(NodeId::new("node-g0-0".into()), "http://g0-0:7700".into(), 0));
        topo.add_node(Node::new(NodeId::new("node-g0-1".into()), "http://g0-1:7700".into(), 0));
        topo.add_node(Node::new(NodeId::new("node-g1-0".into()), "http://g1-0:7700".into(), 1));
        topo.add_node(Node::new(NodeId::new("node-g1-1".into()), "http://g1-1:7700".into(), 1));

        let plan = plan_search_scatter(&topo, 0, 2, 16);
        let mut c = MockNodeClient::default();

        // Group 1 nodes are healthy but partial policy shouldn't use them
        c.responses.insert(NodeId::new("node-g1-0".into()), serde_json::json!({
            "hits": [{"id": "fallback-doc"}],
            "estimatedTotalHits": 1,
        }));

        // Group 0 nodes fail
        c.errors.insert(NodeId::new("node-g0-0".into()), NodeError::Timeout);
        c.errors.insert(NodeId::new("node-g0-1".into()), NodeError::Timeout);

        let req = make_req();

        // With partial policy, should NOT use fallback
        let result = execute_scatter(plan, &c, req, &topo, UnavailableShardPolicy::Partial).await.unwrap();

        assert!(result.partial, "Should be partial");
        assert!(!result.failed_shards.is_empty(), "Should have failed shards");
        // Should NOT have any successful pages (fallback not used)
        assert!(result.shard_pages.is_empty(), "Partial policy should not use fallback");
    }
}
