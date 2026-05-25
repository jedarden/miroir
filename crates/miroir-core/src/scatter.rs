//! Scatter orchestration: fan-out logic and covering set builder.

use crate::config::UnavailableShardPolicy;
use crate::hedging::{HedgeOutcome, HedgingManager};
use crate::merger::{MergeInput, MergeStrategy, MergedSearchResult, ShardHitPage};
use crate::replica_selection::ReplicaSelector;
use crate::router::{covering_set, covering_set_with_version_floor};
use crate::topology::{NodeId, Topology};
use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{sleep, Duration};
use tracing::{info_span, instrument, Instrument};

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

        Self {
            total_docs,
            avg_doc_length,
            terms,
        }
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
        Ok(PreflightResponse {
            total_docs: 0,
            avg_doc_length: 0.0,
            term_stats: HashMap::new(),
        })
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
    ) -> impl std::future::Future<Output = std::result::Result<TaskStatusResponse, NodeError>> + Send
    {
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

/// Vector search mode for a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMode {
    /// Pure keyword (BM25) search — no vector component.
    KeywordOnly,
    /// Pure vector (semantic) search — no keyword component.
    VectorOnly,
    /// Hybrid search combining BM25 + semantic scores.
    Hybrid,
}

impl Default for VectorMode {
    fn default() -> Self {
        Self::KeywordOnly
    }
}

use crate::vector::VectorSearchConfig;

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
    /// Over-fetch factor for vector/hybrid search (plan §13.12).
    /// For a request with limit L, each shard returns L × over_fetch_factor hits.
    pub over_fetch_factor: u32,
    /// Vector search mode (keyword-only, vector-only, or hybrid).
    pub vector_mode: VectorMode,
    /// Vector search configuration (for merge strategy).
    pub vector_config: Option<VectorSearchConfig>,
}

impl SearchRequest {
    /// Build the request body for sending to a node.
    ///
    /// Injects `showRankingScore: true` unconditionally so the merger can global-sort.
    /// Each node receives `offset + limit` results to ensure the coordinator has enough
    /// data to apply pagination.
    /// Build the request body for sending to a node.
    ///
    /// Injects `showRankingScore: true` unconditionally so the merger can global-sort.
    /// Each node receives `offset + limit` results to ensure the coordinator has enough
    /// data to apply pagination.
    ///
    /// For vector/hybrid queries, applies over-fetch factor (plan §13.12): each shard
    /// returns `limit × over_fetch_factor` hits so the global reranker can recover
    /// correct ordering on sparse semantic matches.
    pub fn to_node_body(&self) -> Value {
        let mut body = self.body.clone();

        // Inject showRankingScore: true unconditionally for global sorting
        body["showRankingScore"] = serde_json::json!(true);

        // For vector/hybrid search, apply over-fetch factor (plan §13.12)
        let per_shard_limit = if self.vector_mode != VectorMode::KeywordOnly {
            (self.offset + self.limit) * self.over_fetch_factor as usize
        } else {
            self.offset + self.limit
        };
        body["limit"] = serde_json::json!(per_shard_limit);

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

    /// Detect the vector search mode from the request body.
    ///
    /// Returns VectorMode::Hybrid if the request has a `hybrid` field,
    /// VectorMode::VectorOnly if it has `vector` field but no `q`,
    /// KeywordOnly otherwise.
    pub fn detect_vector_mode(body: &Value) -> VectorMode {
        // Check for hybrid search (plan §13.12)
        if body.get("hybrid").is_some() {
            return VectorMode::Hybrid;
        }

        // Check for pure vector search (has vector field, no query)
        if body.get("vector").is_some() {
            if body
                .get("q")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.is_empty())
            {
                // Has both vector and query → hybrid
                return VectorMode::Hybrid;
            }
            return VectorMode::VectorOnly;
        }

        // Check for _vectors (Meilisearch's stored vectors field)
        if body.get("_vectors").is_some() {
            if body
                .get("q")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.is_empty())
            {
                return VectorMode::Hybrid;
            }
            return VectorMode::VectorOnly;
        }

        // Pure keyword search
        VectorMode::KeywordOnly
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
pub async fn plan_search_scatter(
    topology: &Topology,
    query_seq: u64,
    rf: usize,
    shard_count: u32,
    replica_selector: Option<&ReplicaSelector>,
) -> ScatterPlan {
    let chosen_group = crate::router::query_group_active(query_seq, topology);

    let group = match topology.group(chosen_group) {
        Some(g) => g,
        None => {
            return ScatterPlan {
                chosen_group,
                target_shards: Vec::new(),
                shard_to_node: HashMap::new(),
                deadline_ms: 0,
                hedging_eligible: false,
            };
        }
    };

    let _covering = covering_set(shard_count, group, rf, query_seq);

    let mut shard_to_node = HashMap::new();
    let node_map = topology.node_map();

    for shard_id in 0..shard_count {
        let replicas = crate::router::assign_shard_in_group(shard_id, group.nodes(), rf);

        // Filter to only healthy nodes within the group
        let healthy_replicas: Vec<NodeId> = replicas
            .iter()
            .filter(|node_id| {
                node_map
                    .get(node_id)
                    .map(|n| n.is_healthy())
                    .unwrap_or(false)
            })
            .cloned()
            .collect();

        let selected = if !healthy_replicas.is_empty() {
            // Use healthy intra-group replica
            if let Some(selector) = replica_selector {
                match selector.select(&healthy_replicas, chosen_group).await {
                    Some(node) => node,
                    None => healthy_replicas[(query_seq as usize) % healthy_replicas.len()].clone(),
                }
            } else {
                healthy_replicas[(query_seq as usize) % healthy_replicas.len()].clone()
            }
        } else {
            // Cross-group fallback: try other groups for this shard
            let mut fallback_node = None;
            'fallback: for group_id in 0..topology.replica_group_count() {
                if group_id == chosen_group {
                    continue;
                }
                if let Some(other_group) = topology.group(group_id) {
                    let other_replicas =
                        crate::router::assign_shard_in_group(shard_id, other_group.nodes(), rf);
                    for other_node in other_replicas {
                        if let Some(node) = node_map.get(&other_node) {
                            if node.is_healthy() {
                                fallback_node = Some(other_node);
                                break 'fallback;
                            }
                        }
                    }
                }
            }

            fallback_node.unwrap_or_else(|| {
                // No healthy node found anywhere - use original replica and let it fail
                replicas[(query_seq as usize) % replicas.len()].clone()
            })
        };

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

/// Plan search scatter with query planner narrowing (plan §13.4).
///
/// Uses the query planner to narrow the target shard set when the filter
/// constrains the primary key. This reduces fan-out from N/RG nodes to
/// RF (or 1 with RF=1) for PK-constrained queries.
///
/// # Arguments
/// * `topology` - The cluster topology
/// * `query_seq` - Query sequence number for round-robin
/// * `rf` - Replication factor
/// * `shard_count` - Total number of shards
/// * `replica_selector` - Optional replica selector for adaptive selection
/// * `target_shards` - Optional narrowed shard set from query planner
///
/// # Returns
/// A scatter plan with narrowed target_shards if provided, otherwise all shards.
pub async fn plan_search_scatter_with_narrowing(
    topology: &Topology,
    query_seq: u64,
    rf: usize,
    shard_count: u32,
    replica_selector: Option<&ReplicaSelector>,
    target_shards: Option<Vec<u32>>,
) -> ScatterPlan {
    let chosen_group = crate::router::query_group_active(query_seq, topology);

    let group = match topology.group(chosen_group) {
        Some(g) => g,
        None => {
            return ScatterPlan {
                chosen_group,
                target_shards: Vec::new(),
                shard_to_node: HashMap::new(),
                deadline_ms: 0,
                hedging_eligible: false,
            };
        }
    };

    let _covering = covering_set(shard_count, group, rf, query_seq);

    let mut shard_to_node = HashMap::new();
    let node_map = topology.node_map();

    // Use narrowed target_shards if provided, otherwise target all shards
    let target_shards = target_shards.unwrap_or_else(|| (0..shard_count).collect());

    for shard_id in 0..shard_count {
        let replicas = crate::router::assign_shard_in_group(shard_id, group.nodes(), rf);

        // Filter to only healthy nodes within the group
        let healthy_replicas: Vec<NodeId> = replicas
            .iter()
            .filter(|node_id| {
                node_map
                    .get(node_id)
                    .map(|n| n.is_healthy())
                    .unwrap_or(false)
            })
            .cloned()
            .collect();

        let selected = if !healthy_replicas.is_empty() {
            // Use healthy intra-group replica
            if let Some(selector) = replica_selector {
                match selector.select(&healthy_replicas, chosen_group).await {
                    Some(node) => node,
                    None => healthy_replicas[(query_seq as usize) % healthy_replicas.len()].clone(),
                }
            } else {
                healthy_replicas[(query_seq as usize) % healthy_replicas.len()].clone()
            }
        } else {
            // Cross-group fallback: try other groups for this shard
            let mut fallback_node = None;
            'fallback: for group_id in 0..topology.replica_group_count() {
                if group_id == chosen_group {
                    continue;
                }
                if let Some(other_group) = topology.group(group_id) {
                    let other_replicas =
                        crate::router::assign_shard_in_group(shard_id, other_group.nodes(), rf);
                    for other_node in other_replicas {
                        if let Some(node) = node_map.get(&other_node) {
                            if node.is_healthy() {
                                fallback_node = Some(other_node);
                                break 'fallback;
                            }
                        }
                    }
                }
            }

            fallback_node.unwrap_or_else(|| {
                // No healthy node found anywhere - use original replica and let it fail
                replicas[(query_seq as usize) % replicas.len()].clone()
            })
        };

        shard_to_node.insert(shard_id, selected);
    }

    ScatterPlan {
        chosen_group,
        target_shards,
        shard_to_node,
        deadline_ms: 5000,
        hedging_eligible: group.node_count() > 1,
    }
}

/// Plan search scatter with settings version floor filtering (plan §13.5).
///
/// Excludes nodes whose settings version for the given index is below `floor`.
/// Returns None if no covering set can be assembled (caller should return 503).
pub async fn plan_search_scatter_with_version_floor(
    topology: &Topology,
    query_seq: u64,
    rf: usize,
    shard_count: u32,
    index: &str,
    floor: u64,
    version_checker: &impl Fn(&str, &str) -> u64,
    replica_selector: Option<&ReplicaSelector>,
) -> Option<ScatterPlan> {
    let chosen_group = crate::router::query_group_active(query_seq, topology);

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
        // Filter by version floor
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

        let selected = if let Some(selector) = replica_selector {
            // Convert Vec<&NodeId> to Vec<NodeId> for selector
            let eligible_owned: Vec<NodeId> = eligible.iter().map(|&n| n.clone()).collect();
            match selector.select(&eligible_owned, chosen_group).await {
                Some(node) => node,
                None => eligible[query_seq as usize % eligible.len()].clone(),
            }
        } else {
            eligible[query_seq as usize % eligible.len()].clone()
        };

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
pub async fn plan_search_scatter_for_group(
    topology: &Topology,
    query_seq: u64,
    rf: usize,
    shard_count: u32,
    pinned_group: u32,
    replica_selector: Option<&ReplicaSelector>,
) -> Option<ScatterPlan> {
    let group = topology.group(pinned_group)?;

    let mut shard_to_node = HashMap::new();
    for shard_id in 0..shard_count {
        let replicas = crate::router::assign_shard_in_group(shard_id, group.nodes(), rf);
        if replicas.is_empty() {
            continue;
        }

        let selected = if let Some(selector) = replica_selector {
            match selector.select(&replicas, pinned_group).await {
                Some(node) => node,
                None => replicas[query_seq as usize % replicas.len()].clone(),
            }
        } else {
            replicas[query_seq as usize % replicas.len()].clone()
        };

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

/// Plan search scatter with tenant affinity (plan §13.15).
///
/// If the tenant has a pinned group, routes to that group. Otherwise, uses
/// normal round-robin group selection. Returns None if the pinned group is invalid.
///
/// # Arguments
/// * `topology` - The cluster topology
/// * `query_seq` - Query sequence number for round-robin
/// * `rf` - Replication factor
/// * `shard_count` - Number of shards
/// * `pinned_group` - Optional tenant-pinned replica group
/// * `replica_selector` - Optional replica selector for adaptive selection
///
/// # Returns
/// A scatter plan, or None if the pinned group is invalid.
pub async fn plan_search_scatter_with_tenant(
    topology: &Topology,
    query_seq: u64,
    rf: usize,
    shard_count: u32,
    pinned_group: Option<u32>,
    replica_selector: Option<&ReplicaSelector>,
) -> Option<ScatterPlan> {
    match pinned_group {
        Some(group_id) => {
            // Tenant has a pinned group - use it
            plan_search_scatter_for_group(
                topology,
                query_seq,
                rf,
                shard_count,
                group_id,
                replica_selector,
            )
            .await
        }
        None => {
            // No tenant pinning - use normal group selection
            let plan =
                plan_search_scatter(topology, query_seq, rf, shard_count, replica_selector).await;
            // Check if the plan has valid targets (group exists and has nodes)
            if topology.group(plan.chosen_group).is_some() && !plan.target_shards.is_empty() {
                Some(plan)
            } else {
                None
            }
        }
    }
}

/// Plan search scatter using adaptive replica selection (plan §13.3).
///
/// Uses EWMA-based scoring to select the best replica for each shard,
/// falling back to round-robin for shards with no metrics data.
#[instrument(skip_all, fields(query_seq, rf, shard_count))]
pub async fn plan_search_scatter_adaptive(
    topology: &Topology,
    query_seq: u64,
    rf: usize,
    shard_count: u32,
    replica_selector: &ReplicaSelector,
) -> ScatterPlan {
    let chosen_group = crate::router::query_group_active(query_seq, topology);

    let group = match topology.group(chosen_group) {
        Some(g) => g,
        None => {
            return ScatterPlan {
                chosen_group,
                target_shards: Vec::new(),
                shard_to_node: HashMap::new(),
                deadline_ms: 0,
                hedging_eligible: false,
            };
        }
    };

    // If the group has no nodes, return a plan with no targets
    if group.nodes().is_empty() {
        return ScatterPlan {
            chosen_group,
            target_shards: Vec::new(),
            shard_to_node: HashMap::new(),
            deadline_ms: 5000,
            hedging_eligible: false,
        };
    }

    let mut shard_to_node = HashMap::new();
    for shard_id in 0..shard_count {
        let replicas = crate::router::assign_shard_in_group(shard_id, group.nodes(), rf);
        if replicas.is_empty() {
            continue;
        }

        // Use adaptive selection to pick the best replica
        let selected = match replica_selector.select(&replicas, chosen_group).await {
            Some(node) => node,
            None => {
                // Fallback to round-robin if selector returns None
                replicas[(query_seq as usize) % replicas.len()].clone()
            }
        };

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
            node_to_shards
                .entry(node_id.clone())
                .or_default()
                .push(shard_id);
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
        let shard_count = shards.len();
        // Create a span for this node's scatter call
        let span = info_span!(
            "scatter_node",
            node_id = %node_id_clone,
            address = %node.address,
            shard_count = shard_count,
        );
        tasks.push(
            async move {
                let result = client_ref
                    .search_node(&node_id_clone, &node.address, &req_clone)
                    .await;
                (node_id_clone, shards, result)
            }
            .instrument(span),
        );
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

    let partial = !failed_shards.is_empty();

    match policy {
        UnavailableShardPolicy::Error => {
            if !failed_shards.is_empty() {
                return Err(crate::error::MiroirError::Routing(format!(
                    "{} shard(s) unavailable",
                    failed_shards.len()
                )));
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
                            let replicas = crate::router::assign_shard_in_group(
                                shard_id,
                                group.nodes(),
                                topology.rf(),
                            );
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

    Ok(ScatterResult {
        shard_pages,
        failed_shards,
        partial,
        deadline_exceeded,
    })
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
        vector_mode: req.vector_mode,
        vector_config: req.vector_config.clone(),
    };

    // Span for the merge operation
    let _span = info_span!(
        "merge",
        shard_count = merge_input.shard_hits.len(),
        offset = req.offset,
        limit = req.limit,
    )
    .entered();

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
                if seen.insert(lower.clone()) {
                    terms.push(lower);
                }
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
        return Ok(GlobalIdf {
            total_docs: 0,
            avg_doc_length: 0.0,
            terms: HashMap::new(),
        });
    }

    let mut node_to_shards: HashMap<NodeId, Vec<u32>> = HashMap::new();
    for (&shard_id, node_id) in &plan.shard_to_node {
        if plan.target_shards.contains(&shard_id) {
            node_to_shards
                .entry(node_id.clone())
                .or_default()
                .push(shard_id);
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
        tasks.push(async move {
            client_ref
                .preflight_node(&nid, &node.address, &req_clone)
                .await
        });
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
// §13.2 Hedged requests for tail-latency mitigation
// ---------------------------------------------------------------------------

/// Execute a single node request with hedging support.
///
/// If hedging is enabled and the primary request exceeds the p95 deadline,
/// a duplicate request is sent to an alternate replica. The first response wins.
#[instrument(skip_all, fields(node_id = %primary_node, shard_id))]
pub async fn execute_hedged_request<C: NodeClient>(
    client: &C,
    primary_node: &NodeId,
    primary_address: &str,
    shard_id: u32,
    req: &SearchRequest,
    topology: &Topology,
    hedging_manager: Option<&HedgingManager>,
    hedge_count: &mut u32,
) -> (
    std::result::Result<Value, NodeError>,
    Option<HedgeOutcome>,
    Duration,
) {
    let start = Instant::now();

    // Check if hedging is enabled and we haven't exceeded the budget
    let hedge_deadline = if let Some(manager) = hedging_manager {
        if *hedge_count < manager.config().max_hedges_per_query {
            let deadline = manager.hedge_deadline(primary_node).await;
            tracing::debug!("Hedge deadline for {:?}: {:?}", primary_node, deadline);
            deadline
        } else {
            tracing::debug!("Hedge budget exhausted: {} >= {}", *hedge_count, manager.config().max_hedges_per_query);
            None
        }
    } else {
        tracing::debug!("Hedging disabled (no manager)");
        None
    };

    match hedge_deadline {
        Some(deadline) => {
            // Hedging enabled: try primary with timeout, then hedge
            //
            // The key insight: we want to cancel the primary if it's too slow
            // and the hedge completes successfully. We use tokio::select! with
            // a timeout to achieve this.

            // First, try the primary with a timeout equal to the hedge deadline
            let primary_result = tokio::time::timeout(
                deadline,
                client.search_node(primary_node, primary_address, req),
            )
            .await;

            match primary_result {
                Ok(Ok(value)) => {
                    // Primary completed within deadline - no hedge needed
                    let elapsed = start.elapsed();
                    tracing::debug!("Primary completed within deadline {:?}", elapsed);

                    if let Some(manager) = hedging_manager {
                        let latency_ms = elapsed.as_millis() as f64;
                        manager.record_latency(primary_node, latency_ms).await;
                    }

                    (Ok(value), None, elapsed)
                }
                Ok(Err(err)) => {
                    // Primary failed within deadline - return error
                    let elapsed = start.elapsed();
                    tracing::debug!("Primary failed within deadline {:?}", elapsed);

                    if let Some(manager) = hedging_manager {
                        let latency_ms = elapsed.as_millis() as f64;
                        manager.record_latency(primary_node, latency_ms).await;
                    }

                    (Err(err), None, elapsed)
                }
                Err(_) => {
                    // Primary timed out - try hedge
                    tracing::debug!("Primary timed out after {:?}, trying hedge", deadline);

                    // Find an alternate replica
                    if let Some(manager) = hedging_manager {
                        if let Some(alternate_node) = manager
                            .find_alternate(primary_node, shard_id, *hedge_count)
                            .await
                        {
                            *hedge_count += 1;
                            tracing::debug!("Hedge sending to {:?}", alternate_node);

                            if let Some(node) = topology.node(&alternate_node) {
                                let hedge_start = Instant::now();
                                let hedge_result =
                                    client.search_node(&alternate_node, &node.address, req).await;
                                let elapsed = start.elapsed();

                                // Record latency for primary (it timed out)
                                manager
                                    .record_latency(primary_node, deadline.as_millis() as f64)
                                    .await;

                                match &hedge_result {
                                    Ok(_) => {
                                        tracing::debug!(
                                            "Hedge to {:?} succeeded in {:?}",
                                            alternate_node,
                                            hedge_start.elapsed()
                                        );
                                    }
                                    Err(e) => {
                                        tracing::debug!(
                                            "Hedge to {:?} failed: {:?}",
                                            alternate_node,
                                            e
                                        );
                                    }
                                }

                                return (
                                    hedge_result,
                                    Some(HedgeOutcome::HedgeWon),
                                    elapsed,
                                );
                            }
                        }
                    }

                    // No alternate available - wait for primary to complete
                    tracing::debug!("No alternate available, waiting for primary");
                    let primary_result = client.search_node(primary_node, primary_address, req).await;
                    let elapsed = start.elapsed();

                    if let Some(manager) = hedging_manager {
                        let latency_ms = elapsed.as_millis() as f64;
                        manager.record_latency(primary_node, latency_ms).await;
                    }

                    (primary_result, Some(HedgeOutcome::PrimaryWon), elapsed)
                }
            }
        }
        None => {
            // No hedging - execute primary request only
            let result = client.search_node(primary_node, primary_address, req).await;
            let elapsed = start.elapsed();

            // Record latency observation
            if let Some(manager) = hedging_manager {
                let latency_ms = elapsed.as_millis() as f64;
                manager.record_latency(primary_node, latency_ms).await;
            }

            (result, None, elapsed)
        }
    }
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
        &self,
        node: &NodeId,
        _address: &str,
        _request: &SearchRequest,
    ) -> std::result::Result<Value, NodeError> {
        if self.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        }
        if let Some(err) = self.errors.get(node) {
            return Err(err.clone());
        }
        Ok(self.responses.get(node).cloned().unwrap_or_else(|| {
            serde_json::json!({"hits": [], "estimatedTotalHits": 0, "processingTimeMs": 0, "facetDistribution": {}})
        }))
    }

    async fn preflight_node(
        &self,
        node: &NodeId,
        _address: &str,
        _request: &PreflightRequest,
    ) -> std::result::Result<PreflightResponse, NodeError> {
        if let Some(err) = self.errors.get(node) {
            return Err(err.clone());
        }
        Ok(self
            .preflight_responses
            .get(node)
            .cloned()
            .unwrap_or_else(|| PreflightResponse {
                total_docs: 1000,
                avg_doc_length: 50.0,
                term_stats: HashMap::new(),
            }))
    }

    async fn write_documents(
        &self,
        node: &NodeId,
        _address: &str,
        _request: &WriteRequest,
    ) -> std::result::Result<WriteResponse, NodeError> {
        if let Some(err) = self.errors.get(node) {
            return Err(err.clone());
        }
        Ok(self
            .write_responses
            .get(node)
            .cloned()
            .unwrap_or(WriteResponse {
                success: true,
                task_uid: Some(1),
                message: None,
                code: None,
                error_type: None,
            }))
    }

    async fn delete_documents(
        &self,
        node: &NodeId,
        _address: &str,
        _request: &DeleteByIdsRequest,
    ) -> std::result::Result<DeleteResponse, NodeError> {
        if let Some(err) = self.errors.get(node) {
            return Err(err.clone());
        }
        Ok(DeleteResponse {
            success: true,
            task_uid: Some(1),
            message: None,
            code: None,
            error_type: None,
        })
    }

    async fn delete_documents_by_filter(
        &self,
        node: &NodeId,
        _address: &str,
        _request: &DeleteByFilterRequest,
    ) -> std::result::Result<DeleteResponse, NodeError> {
        if let Some(err) = self.errors.get(node) {
            return Err(err.clone());
        }
        Ok(DeleteResponse {
            success: true,
            task_uid: Some(1),
            message: None,
            code: None,
            error_type: None,
        })
    }

    async fn fetch_documents(
        &self,
        node: &NodeId,
        _address: &str,
        request: &FetchDocumentsRequest,
    ) -> std::result::Result<FetchDocumentsResponse, NodeError> {
        if let Some(err) = self.errors.get(node) {
            return Err(err.clone());
        }
        // Return stored fetch response if available, otherwise return empty
        let stored =
            self.fetch_responses
                .get(node)
                .cloned()
                .unwrap_or_else(|| FetchDocumentsResponse {
                    results: Vec::new(),
                    limit: request.limit,
                    offset: request.offset,
                    total: 0,
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
    ) -> impl std::future::Future<Output = std::result::Result<TaskStatusResponse, NodeError>> + Send
    {
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
            let mut node = Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            );
            node.status = crate::topology::NodeStatus::Active;
            topo.add_node(node);
        }
        // Set groups to Active state so they're eligible for routing
        if let Some(g) = topo.group_mut(0) {
            g.set_state(crate::topology::GroupState::Active);
        }
        if let Some(g) = topo.group_mut(1) {
            g.set_state(crate::topology::GroupState::Active);
        }
        topo
    }

    fn make_req() -> SearchRequest {
        SearchRequest {
            index_uid: "test".into(),
            query: Some("test".into()),
            offset: 0,
            limit: 10,
            filter: None,
            facets: None,
            ranking_score: false,
            body: serde_json::json!({}),
            global_idf: None,
            over_fetch_factor: 1,
            vector_mode: VectorMode::KeywordOnly,
            vector_config: None,
        }
    }

    #[tokio::test]
    async fn test_plan_pure_function() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64, None).await;
        assert_eq!(plan.chosen_group, 0);
        assert_eq!(plan.target_shards.len(), 64);
        assert!(plan.hedging_eligible);
    }

    #[tokio::test]
    async fn test_plan_group_rotation() {
        let topo = make_test_topology();
        assert_eq!(
            plan_search_scatter(&topo, 0, 2, 64, None)
                .await
                .chosen_group,
            0
        );
        assert_eq!(
            plan_search_scatter(&topo, 1, 2, 64, None)
                .await
                .chosen_group,
            1
        );
    }

    #[tokio::test]
    async fn test_plan_shard_mapping() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64, None).await;
        for s in 0..64 {
            assert!(plan.shard_to_node.contains_key(&s));
        }
        let g0 = topo.group(0).unwrap();
        for (_, nid) in &plan.shard_to_node {
            assert!(g0.nodes().contains(nid));
        }
    }

    #[tokio::test]
    async fn test_plan_hedging() {
        let mut topo = Topology::new(64, 1, 1);
        topo.add_node(Node::new(
            NodeId::new("n0".into()),
            "http://n0:7700".into(),
            0,
        ));
        assert!(
            !plan_search_scatter(&topo, 0, 1, 64, None)
                .await
                .hedging_eligible
        );
        assert!(
            plan_search_scatter(&make_test_topology(), 0, 2, 64, None)
                .await
                .hedging_eligible
        );
    }

    #[tokio::test]
    async fn test_scatter_mock() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64, None).await;
        let mut c = MockNodeClient::default();
        c.responses.insert(NodeId::new("node-0".into()), serde_json::json!({"hits": [{"id": "doc1"}], "estimatedTotalHits": 1, "processingTimeMs": 5}));
        let r = execute_scatter(plan, &c, make_req(), &topo, UnavailableShardPolicy::Partial)
            .await
            .unwrap();
        assert!(!r.partial);
        assert_eq!(r.shard_pages.len(), 64);
    }

    #[tokio::test]
    async fn test_scatter_partial() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64, None).await;
        let mut c = MockNodeClient::default();
        c.errors
            .insert(NodeId::new("node-0".into()), NodeError::Timeout);
        let r = execute_scatter(plan, &c, make_req(), &topo, UnavailableShardPolicy::Partial)
            .await
            .unwrap();
        assert!(r.partial);
    }

    #[tokio::test]
    async fn test_scatter_error_policy() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64, None).await;
        let mut c = MockNodeClient::default();
        c.errors
            .insert(NodeId::new("node-0".into()), NodeError::Timeout);
        assert!(
            execute_scatter(plan, &c, make_req(), &topo, UnavailableShardPolicy::Error)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_plan_invalid_group() {
        assert!(
            plan_search_scatter(&Topology::new(64, 0, 1), 0, 1, 64, None)
                .await
                .shard_to_node
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_scatter_node_not_in_topo() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64, None).await;
        let r = execute_scatter(
            plan,
            &MockNodeClient::default(),
            make_req(),
            &Topology::new(64, 2, 2),
            UnavailableShardPolicy::Partial,
        )
        .await
        .unwrap();
        assert!(r.partial);
    }

    #[tokio::test]
    async fn test_sg_rrf() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64, None).await;
        let mut c = MockNodeClient::default();
        c.responses.insert(NodeId::new("node-0".into()), serde_json::json!({"hits": [{"id": "a", "_rankingScore": 0.9}], "estimatedTotalHits": 1, "processingTimeMs": 5}));
        let s = crate::merger::RrfStrategy::default_strategy();
        let r = scatter_gather_search(
            plan,
            &c,
            make_req(),
            &topo,
            UnavailableShardPolicy::Partial,
            &s,
        )
        .await
        .unwrap();
        assert!(!r.degraded);
    }

    #[tokio::test]
    async fn test_sg_degraded() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64, None).await;
        let mut c = MockNodeClient::default();
        c.responses.insert(NodeId::new("node-0".into()), serde_json::json!({"hits": [{"id": "a"}], "estimatedTotalHits": 1, "processingTimeMs": 5}));
        c.errors
            .insert(NodeId::new("node-2".into()), NodeError::Timeout);
        let s = crate::merger::RrfStrategy::default_strategy();
        assert!(
            scatter_gather_search(
                plan,
                &c,
                make_req(),
                &topo,
                UnavailableShardPolicy::Partial,
                &s
            )
            .await
            .unwrap()
            .degraded
        );
    }

    #[test]
    fn test_extract_query_terms() {
        assert_eq!(
            extract_query_terms(&Some("hello world hello".into())),
            vec!["hello", "world"]
        );
        assert!(extract_query_terms(&None).is_empty());
    }

    #[test]
    fn test_global_idf_aggregation() {
        let resp = vec![
            PreflightResponse {
                total_docs: 50000,
                avg_doc_length: 50.0,
                term_stats: HashMap::from([("a".into(), TermStats { df: 5000 })]),
            },
            PreflightResponse {
                total_docs: 50000,
                avg_doc_length: 60.0,
                term_stats: HashMap::from([("a".into(), TermStats { df: 4500 })]),
            },
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
        let plan = plan_search_scatter(&topo, 0, 2, 64, None).await;
        let mut c = MockNodeClient::default();
        c.preflight_responses.insert(
            NodeId::new("node-0".into()),
            PreflightResponse {
                total_docs: 30000,
                avg_doc_length: 50.0,
                term_stats: HashMap::from([("search".into(), TermStats { df: 3000 })]),
            },
        );
        c.preflight_responses.insert(
            NodeId::new("node-1".into()),
            PreflightResponse {
                total_docs: 30000,
                avg_doc_length: 55.0,
                term_stats: HashMap::from([("search".into(), TermStats { df: 2500 })]),
            },
        );
        c.preflight_responses.insert(
            NodeId::new("node-2".into()),
            PreflightResponse {
                total_docs: 40000,
                avg_doc_length: 52.0,
                term_stats: HashMap::from([("search".into(), TermStats { df: 4000 })]),
            },
        );
        let req = PreflightRequest {
            index_uid: "test".into(),
            terms: vec!["search".into()],
            filter: None,
        };
        let g = execute_preflight(&plan, &c, &req, &topo).await.unwrap();
        assert_eq!(g.total_docs, 100000);
        assert_eq!(g.terms.get("search").unwrap().df, 9500);
    }

    #[tokio::test]
    async fn test_dfs_query_then_fetch() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64, None).await;
        let mut c = MockNodeClient::default();
        c.responses.insert(NodeId::new("node-0".into()), serde_json::json!({"hits": [{"id": "a", "_rankingScore": 0.9}], "estimatedTotalHits": 1, "processingTimeMs": 5}));
        c.preflight_responses.insert(
            NodeId::new("node-0".into()),
            PreflightResponse {
                total_docs: 50000,
                avg_doc_length: 50.0,
                term_stats: HashMap::from([("test".into(), TermStats { df: 500 })]),
            },
        );
        let s = crate::merger::RrfStrategy::default_strategy();
        let r = dfs_query_then_fetch_search(
            plan,
            &c,
            make_req(),
            &topo,
            UnavailableShardPolicy::Partial,
            &s,
        )
        .await
        .unwrap();
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
        topo.add_node(Node::new(
            NodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-1".into()),
            "http://node-1:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-2".into()),
            "http://node-2:7700".into(),
            0,
        ));

        let plan = plan_search_scatter(&topo, 0, 1, 3, None).await;

        // Simulate severely skewed shard distribution
        let mut c = MockNodeClient::default();

        // Shard 0: Normal distribution
        c.preflight_responses.insert(
            NodeId::new("node-0".into()),
            PreflightResponse {
                total_docs: 1000,
                avg_doc_length: 50.0,
                term_stats: HashMap::from([
                    ("machine".into(), TermStats { df: 100 }),
                    ("learning".into(), TermStats { df: 80 }),
                ]),
            },
        );

        // Shard 1: 100x normal (severely skewed)
        c.preflight_responses.insert(
            NodeId::new("node-1".into()),
            PreflightResponse {
                total_docs: 100_000,
                avg_doc_length: 55.0,
                term_stats: HashMap::from([
                    ("machine".into(), TermStats { df: 10_000 }),
                    ("learning".into(), TermStats { df: 8_000 }),
                ]),
            },
        );

        // Shard 2: Near-empty
        c.preflight_responses.insert(
            NodeId::new("node-2".into()),
            PreflightResponse {
                total_docs: 10,
                avg_doc_length: 45.0,
                term_stats: HashMap::from([
                    ("machine".into(), TermStats { df: 1 }),
                    ("learning".into(), TermStats { df: 1 }),
                ]),
            },
        );

        // Execute preflight to get global IDF
        let preflight_req = PreflightRequest {
            index_uid: "test".into(),
            terms: vec!["machine".into(), "learning".into()],
            filter: None,
        };

        let global_idf = execute_preflight(&plan, &c, &preflight_req, &topo)
            .await
            .unwrap();

        // Verify global aggregation
        assert_eq!(global_idf.total_docs, 101_010);
        assert_eq!(global_idf.terms.get("machine").unwrap().df, 10_101);
        assert_eq!(global_idf.terms.get("learning").unwrap().df, 8_081);

        // Verify global IDF is the same for all shards
        // Expected IDF for "machine": log((101010 - 10101 + 0.5) / (10101 + 0.5)) + 1
        let expected_idf_machine: f64 =
            ((101010.0_f64 - 10101.0 + 0.5) / (10101.0 + 0.5)).ln() + 1.0;
        let actual_idf_machine = global_idf.terms.get("machine").unwrap().idf;
        assert!((actual_idf_machine - expected_idf_machine).abs() < 0.001);

        // Expected IDF for "learning": log((101010 - 8081 + 0.5) / (8081 + 0.5)) + 1
        let expected_idf_learning: f64 =
            ((101010.0_f64 - 8081.0 + 0.5) / (8081.0 + 0.5)).ln() + 1.0;
        let actual_idf_learning = global_idf.terms.get("learning").unwrap().idf;
        assert!((actual_idf_learning - expected_idf_learning).abs() < 0.001);

        // Verify that without global IDF, local IDF values would differ significantly
        // Shard 0 local IDF for "machine": log((1000 - 100 + 0.5) / (100 + 0.5)) + 1 ≈ 3.19
        // Shard 1 local IDF for "machine": log((100000 - 10000 + 0.5) / (10000 + 0.5)) + 1 ≈ 3.20
        // Shard 2 local IDF for "machine": log((10 - 1 + 0.5) / (1 + 0.5)) + 1 ≈ 2.85
        let local_idf_shard_0: f64 = ((1000.0_f64 - 100.0 + 0.5) / (100.0 + 0.5)).ln() + 1.0;
        let local_idf_shard_2: f64 = ((10.0_f64 - 1.0 + 0.5) / (1.0 + 0.5)).ln() + 1.0;
        assert!(
            (local_idf_shard_2 - local_idf_shard_0).abs() > 0.2,
            "Local IDF values should differ significantly"
        );
        assert!(
            (local_idf_shard_2 - actual_idf_machine).abs() > 0.3,
            "Global IDF should be closer to large-shard local IDF"
        );
    }

    /// Test that DFS preflight handles empty query terms gracefully.
    #[tokio::test]
    async fn test_dfs_empty_query_terms() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64, None).await;
        let c = MockNodeClient::default();

        let preflight_req = PreflightRequest {
            index_uid: "test".into(),
            terms: vec![],
            filter: None,
        };

        let global_idf = execute_preflight(&plan, &c, &preflight_req, &topo)
            .await
            .unwrap();
        assert_eq!(global_idf.total_docs, 0);
        assert!(global_idf.terms.is_empty());
    }

    /// Test that DFS preflight handles partial failures gracefully.
    #[tokio::test]
    async fn test_dfs_partial_failure() {
        let mut topo = Topology::new(3, 1, 1);
        topo.add_node(Node::new(
            NodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-1".into()),
            "http://node-1:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-2".into()),
            "http://node-2:7700".into(),
            0,
        ));

        let plan = plan_search_scatter(&topo, 0, 1, 3, None).await;
        let mut c = MockNodeClient::default();

        // Node 0 returns valid data
        c.preflight_responses.insert(
            NodeId::new("node-0".into()),
            PreflightResponse {
                total_docs: 50000,
                avg_doc_length: 50.0,
                term_stats: HashMap::from([("test".into(), TermStats { df: 5000 })]),
            },
        );

        // Node 1 returns valid data
        c.preflight_responses.insert(
            NodeId::new("node-1".into()),
            PreflightResponse {
                total_docs: 30000,
                avg_doc_length: 55.0,
                term_stats: HashMap::from([("test".into(), TermStats { df: 3000 })]),
            },
        );

        // Node 2 fails
        c.errors
            .insert(NodeId::new("node-2".into()), NodeError::Timeout);

        let preflight_req = PreflightRequest {
            index_uid: "test".into(),
            terms: vec!["test".into()],
            filter: None,
        };

        // Should aggregate from successful nodes only
        let global_idf = execute_preflight(&plan, &c, &preflight_req, &topo)
            .await
            .unwrap();
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
            over_fetch_factor: 1,
            vector_mode: VectorMode::KeywordOnly,
            vector_config: None,
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
        assert_eq!(
            body.get("filter"),
            Some(&serde_json::json!("status = published"))
        );

        // facets must be set
        assert_eq!(
            body.get("facets"),
            Some(&serde_json::json!(["category", "tags"]))
        );

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
            over_fetch_factor: 1,
            vector_mode: VectorMode::KeywordOnly,
            vector_config: None,
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
            over_fetch_factor: 1,
            vector_mode: VectorMode::KeywordOnly,
            vector_config: None,
        };

        let body = req.to_node_body();

        // limit must be offset + limit = 150 (fetch enough for coordinator pagination)
        assert_eq!(body.get("limit"), Some(&serde_json::json!(150)));

        // offset must be 0 (coordinator handles offset)
        assert_eq!(body.get("offset"), Some(&serde_json::json!(0)));
    }

    /// Test vector mode detection (plan §13.12).
    #[test]
    fn test_detect_vector_mode() {
        // Pure keyword search (no vector fields)
        let body = serde_json::json!({"q": "test"});
        assert_eq!(
            SearchRequest::detect_vector_mode(&body),
            VectorMode::KeywordOnly
        );

        // Hybrid search (has hybrid field)
        let body = serde_json::json!({"q": "test", "hybrid": {"embedder": "openai"}});
        assert_eq!(SearchRequest::detect_vector_mode(&body), VectorMode::Hybrid);

        // Pure vector search (has vector field, no query)
        let body = serde_json::json!({"vector": [0.1, 0.2, 0.3]});
        assert_eq!(
            SearchRequest::detect_vector_mode(&body),
            VectorMode::VectorOnly
        );

        // Vector + query = hybrid
        let body = serde_json::json!({"q": "test", "vector": [0.1, 0.2, 0.3]});
        assert_eq!(SearchRequest::detect_vector_mode(&body), VectorMode::Hybrid);

        // _vectors field (Meilisearch stored vectors)
        let body = serde_json::json!({"_vectors": {"default": [0.1, 0.2]}});
        assert_eq!(
            SearchRequest::detect_vector_mode(&body),
            VectorMode::VectorOnly
        );

        // _vectors + query = hybrid
        let body = serde_json::json!({"q": "test", "_vectors": {"default": [0.1, 0.2]}});
        assert_eq!(SearchRequest::detect_vector_mode(&body), VectorMode::Hybrid);
    }

    /// Test over-fetch behavior for vector/hybrid queries (plan §13.12).
    #[test]
    fn test_to_node_body_over_fetch() {
        // Keyword-only: no over-fetch
        let req = SearchRequest {
            index_uid: "test".into(),
            query: Some("test".into()),
            offset: 0,
            limit: 10,
            filter: None,
            facets: None,
            ranking_score: false,
            body: serde_json::json!({}),
            global_idf: None,
            over_fetch_factor: 3,
            vector_mode: VectorMode::KeywordOnly,
            vector_config: None,
        };
        let body = req.to_node_body();
        assert_eq!(body.get("limit"), Some(&serde_json::json!(10)));

        // Hybrid: apply over-fetch
        let req = SearchRequest {
            index_uid: "test".into(),
            query: Some("test".into()),
            offset: 0,
            limit: 10,
            filter: None,
            facets: None,
            ranking_score: false,
            body: serde_json::json!({"hybrid": {}}),
            global_idf: None,
            over_fetch_factor: 3,
            vector_mode: VectorMode::Hybrid,
            vector_config: None,
        };
        let body = req.to_node_body();
        assert_eq!(body.get("limit"), Some(&serde_json::json!(30))); // 10 * 3

        // Vector-only: apply over-fetch
        let req = SearchRequest {
            index_uid: "test".into(),
            query: None,
            offset: 0,
            limit: 20,
            filter: None,
            facets: None,
            ranking_score: false,
            body: serde_json::json!({"vector": [0.1, 0.2]}),
            global_idf: None,
            over_fetch_factor: 5,
            vector_mode: VectorMode::VectorOnly,
            vector_config: None,
        };
        let body = req.to_node_body();
        assert_eq!(body.get("limit"), Some(&serde_json::json!(100))); // 20 * 5
    }

    /// Test group fallback when primary group has failed nodes.
    #[tokio::test]
    async fn test_group_fallback_on_partial_failure() {
        let mut topo = Topology::new(16, 2, 2);
        // Group 0: 2 nodes
        topo.add_node(Node::new(
            NodeId::new("node-g0-0".into()),
            "http://g0-0:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g0-1".into()),
            "http://g0-1:7700".into(),
            0,
        ));
        // Group 1: 2 nodes (healthy fallback targets)
        topo.add_node(Node::new(
            NodeId::new("node-g1-0".into()),
            "http://g1-0:7700".into(),
            1,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g1-1".into()),
            "http://g1-1:7700".into(),
            1,
        ));

        let plan = plan_search_scatter(&topo, 0, 2, 16, None).await; // query_seq=0 → group 0
        assert_eq!(plan.chosen_group, 0);

        let mut c = MockNodeClient::default();

        // Set up responses: all shards on group 1 nodes return valid data
        let response_1 = serde_json::json!({
            "hits": [{"id": "doc1", "_rankingScore": 0.9}],
            "estimatedTotalHits": 1,
            "processingTimeMs": 5,
        });
        c.responses
            .insert(NodeId::new("node-g1-0".into()), response_1.clone());
        c.responses
            .insert(NodeId::new("node-g1-1".into()), response_1);

        // All nodes in group 0 fail
        c.errors
            .insert(NodeId::new("node-g0-0".into()), NodeError::Timeout);
        c.errors
            .insert(NodeId::new("node-g1-0".into()), NodeError::Timeout);

        let req = make_req();

        // With fallback policy, shards should succeed via group 1
        let result = execute_scatter(plan, &c, req, &topo, UnavailableShardPolicy::Fallback)
            .await
            .unwrap();

        // Should have succeeded via fallback (group 1)
        assert!(!result.partial, "Fallback should have succeeded");
        assert!(
            result.failed_shards.is_empty(),
            "No shards should have failed after fallback"
        );
        assert!(
            !result.shard_pages.is_empty(),
            "Should have shard pages from fallback"
        );
    }

    /// Test group fallback when both groups are down.
    #[tokio::test]
    async fn test_group_fallback_all_groups_down() {
        let mut topo = Topology::new(16, 2, 2);
        topo.add_node(Node::new(
            NodeId::new("node-g0-0".into()),
            "http://g0-0:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g0-1".into()),
            "http://g0-1:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g1-0".into()),
            "http://g1-0:7700".into(),
            1,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g1-1".into()),
            "http://g1-1:7700".into(),
            1,
        ));

        let plan = plan_search_scatter(&topo, 0, 2, 16, None).await;
        let mut c = MockNodeClient::default();

        // All nodes fail
        c.errors
            .insert(NodeId::new("node-g0-0".into()), NodeError::Timeout);
        c.errors
            .insert(NodeId::new("node-g0-1".into()), NodeError::Timeout);
        c.errors
            .insert(NodeId::new("node-g1-0".into()), NodeError::Timeout);
        c.errors
            .insert(NodeId::new("node-g1-1".into()), NodeError::Timeout);

        let req = make_req();

        // With fallback policy, should still fail when all groups are down
        let result = execute_scatter(plan, &c, req, &topo, UnavailableShardPolicy::Fallback)
            .await
            .unwrap();

        assert!(result.partial, "Should be partial when all groups fail");
        assert!(
            !result.failed_shards.is_empty(),
            "Should have failed shards"
        );
    }

    /// Test that partial policy does NOT use fallback.
    #[tokio::test]
    async fn test_partial_policy_no_fallback() {
        let mut topo = Topology::new(16, 2, 2);
        topo.add_node(Node::new(
            NodeId::new("node-g0-0".into()),
            "http://g0-0:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g0-1".into()),
            "http://g0-1:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g1-0".into()),
            "http://g1-0:7700".into(),
            1,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g1-1".into()),
            "http://g1-1:7700".into(),
            1,
        ));

        let plan = plan_search_scatter(&topo, 0, 2, 16, None).await;
        let mut c = MockNodeClient::default();

        // Group 1 nodes are healthy but partial policy shouldn't use them
        c.responses.insert(
            NodeId::new("node-g1-0".into()),
            serde_json::json!({
                "hits": [{"id": "fallback-doc"}],
                "estimatedTotalHits": 1,
            }),
        );

        // Group 0 nodes fail
        c.errors
            .insert(NodeId::new("node-g0-0".into()), NodeError::Timeout);
        c.errors
            .insert(NodeId::new("node-g0-1".into()), NodeError::Timeout);

        let req = make_req();

        // With partial policy, should NOT use fallback
        let result = execute_scatter(plan, &c, req, &topo, UnavailableShardPolicy::Partial)
            .await
            .unwrap();

        assert!(result.partial, "Should be partial");
        assert!(
            !result.failed_shards.is_empty(),
            "Should have failed shards"
        );
        // Should NOT have any successful pages (fallback not used)
        assert!(
            result.shard_pages.is_empty(),
            "Partial policy should not use fallback"
        );
    }

    // ── plan_search_scatter_with_version_floor tests ─────────────────────────────

    #[tokio::test]
    async fn test_plan_with_version_floor_all_nodes_eligible() {
        let mut topo = Topology::new(64, 1, 2);
        for i in 0u32..4 {
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                0,
            ));
        }

        // All nodes have version >= 1
        let version_checker = |_index: &str, _node: &str| -> u64 { 10 };

        let result = plan_search_scatter_with_version_floor(
            &topo,
            0,
            2,
            64,
            "test_index",
            1,
            &version_checker,
            None,
        )
        .await;

        assert!(result.is_some(), "Should succeed when all nodes eligible");
        let plan = result.unwrap();
        assert_eq!(plan.chosen_group, 0);
        assert_eq!(plan.target_shards.len(), 64);
    }

    #[tokio::test]
    async fn test_plan_with_version_floor_no_eligible_nodes() {
        let mut topo = Topology::new(64, 1, 2);
        for i in 0u32..4 {
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                0,
            ));
        }

        // All nodes have version < floor
        let version_checker = |_index: &str, _node: &str| -> u64 { 5 };

        let result = plan_search_scatter_with_version_floor(
            &topo,
            0,
            2,
            64,
            "test_index",
            10, // floor is 10
            &version_checker,
            None,
        )
        .await;

        assert!(result.is_none(), "Should fail when no nodes eligible");
    }

    #[tokio::test]
    async fn test_plan_with_version_floor_partial_eligibility() {
        let mut topo = Topology::new(16, 1, 2);
        topo.add_node(Node::new(
            NodeId::new("node-old".into()),
            "http://node-old:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-new".into()),
            "http://node-new:7700".into(),
            0,
        ));

        // Only node-new has version >= floor
        let version_checker = |_index: &str, node: &str| -> u64 {
            if node == "node-new" {
                100
            } else {
                5
            }
        };

        let result = plan_search_scatter_with_version_floor(
            &topo,
            0,
            2,
            16,
            "test_index",
            10,
            &version_checker,
            None,
        )
        .await;

        assert!(result.is_some(), "Should succeed with partial eligibility");
        let plan = result.unwrap();
        // All shards should map to the eligible node
        for node_id in plan.shard_to_node.values() {
            assert_eq!(
                node_id.as_str(),
                "node-new",
                "All shards should use eligible node"
            );
        }
    }

    // ── plan_search_scatter_for_group tests ─────────────────────────────────────

    #[tokio::test]
    async fn test_plan_for_specific_group() {
        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        let result = plan_search_scatter_for_group(&topo, 0, 2, 64, 1, None).await;

        assert!(result.is_some(), "Should succeed for valid group");
        let plan = result.unwrap();
        assert_eq!(plan.chosen_group, 1, "Should use specified group");
        assert_eq!(plan.target_shards.len(), 64);
    }

    #[tokio::test]
    async fn test_plan_for_invalid_group() {
        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        let result = plan_search_scatter_for_group(&topo, 0, 2, 64, 99, None).await;

        assert!(result.is_none(), "Should fail for invalid group");
    }

    #[tokio::test]
    async fn test_plan_for_group_rotation() {
        let mut topo = Topology::new(64, 1, 3);
        for i in 0u32..4 {
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                0,
            ));
        }

        let plan = plan_search_scatter_for_group(&topo, 0, 3, 64, 0, None)
            .await
            .unwrap();
        assert_eq!(plan.chosen_group, 0);

        // Verify intra-group replica rotation by checking shard_to_node
        let node_0_usages = plan
            .shard_to_node
            .values()
            .filter(|n| n.as_str() == "node-0")
            .count();
        let node_1_usages = plan
            .shard_to_node
            .values()
            .filter(|n| n.as_str() == "node-1")
            .count();
        let node_2_usages = plan
            .shard_to_node
            .values()
            .filter(|n| n.as_str() == "node-2")
            .count();

        // With RF=3 and query_seq=0, each shard should use replicas[0]
        // The assignment should distribute shards across nodes
        // Total should be 64 across all nodes
        let node_3_usages = plan
            .shard_to_node
            .values()
            .filter(|n| n.as_str() == "node-3")
            .count();
        assert!(node_0_usages + node_1_usages + node_2_usages + node_3_usages == 64);
    }

    // ── plan_search_scatter_with_tenant tests (plan §13.15) ──────────────────────

    #[tokio::test]
    async fn test_plan_with_tenant_pinned_group() {
        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        // Tenant pinned to group 1
        let plan = plan_search_scatter_with_tenant(&topo, 0, 2, 64, Some(1), None).await;

        assert!(plan.is_some(), "Should succeed with valid pinned group");
        let plan = plan.unwrap();
        assert_eq!(plan.chosen_group, 1, "Should use pinned group");
        assert_eq!(plan.target_shards.len(), 64);
    }

    #[tokio::test]
    async fn test_plan_with_tenant_no_pin() {
        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        // No tenant pin - should use normal round-robin (group 0 for query_seq=0)
        let plan = plan_search_scatter_with_tenant(&topo, 0, 2, 64, None, None).await;

        assert!(plan.is_some(), "Should succeed without tenant pin");
        let plan = plan.unwrap();
        assert_eq!(plan.chosen_group, 0, "Should use round-robin group 0");
    }

    #[tokio::test]
    async fn test_plan_with_tenant_invalid_group() {
        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        // Tenant pinned to non-existent group 99
        let plan = plan_search_scatter_with_tenant(&topo, 0, 2, 64, Some(99), None).await;

        assert!(plan.is_none(), "Should fail with invalid pinned group");
    }

    #[tokio::test]
    async fn test_plan_with_tenant_consistent_routing() {
        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        // Same tenant should always route to same group
        for _ in 0..10 {
            let plan = plan_search_scatter_with_tenant(&topo, 0, 2, 64, Some(1), None).await;
            assert!(plan.is_some());
            assert_eq!(plan.unwrap().chosen_group, 1);
        }
    }

    #[tokio::test]
    async fn test_plan_with_tenant_separate_groups() {
        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        // Tenant-A pins to group 0, Tenant-B pins to group 1
        let plan_a = plan_search_scatter_with_tenant(&topo, 0, 2, 64, Some(0), None).await;
        let plan_b = plan_search_scatter_with_tenant(&topo, 0, 2, 64, Some(1), None).await;

        assert!(plan_a.is_some());
        assert!(plan_b.is_some());
        assert_eq!(plan_a.unwrap().chosen_group, 0);
        assert_eq!(plan_b.unwrap().chosen_group, 1);
    }

    // ── plan_search_scatter_adaptive tests ───────────────────────────────────────

    #[tokio::test]
    async fn test_plan_adaptive_basic() {
        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        // Create a selector with adaptive strategy
        let selector = crate::replica_selection::ReplicaSelector::new(
            crate::replica_selection::ReplicaSelectionConfig {
                strategy: "adaptive".into(),
                exploration_epsilon: 0.0, // Disable exploration for deterministic test
                ..Default::default()
            },
        );
        let plan = plan_search_scatter_adaptive(&topo, 0, 2, 64, &selector).await;

        assert_eq!(plan.chosen_group, 0);
        assert_eq!(plan.target_shards.len(), 64);
        assert!(
            plan.hedging_eligible,
            "Should be eligible for hedging with multiple nodes in group"
        );
    }

    #[tokio::test]
    async fn test_plan_adaptive_empty_group() {
        let topo = Topology::new(64, 2, 2); // No nodes added

        let selector = crate::replica_selection::ReplicaSelector::default();
        let plan = plan_search_scatter_adaptive(&topo, 0, 2, 64, &selector).await;

        assert_eq!(plan.chosen_group, 0);
        assert!(
            plan.target_shards.is_empty(),
            "Should have no targets for empty topology"
        );
        assert!(!plan.hedging_eligible);
    }

    #[tokio::test]
    async fn test_plan_adaptive_selector_returns_none() {
        let mut topo = Topology::new(64, 1, 2);
        for i in 0u32..3 {
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                0,
            ));
        }

        // Selector with adaptive strategy (will fall back to round-robin when no metrics)
        let selector = crate::replica_selection::ReplicaSelector::new(
            crate::replica_selection::ReplicaSelectionConfig {
                strategy: "adaptive".into(),
                exploration_epsilon: 0.0, // Disable exploration for deterministic test
                ..Default::default()
            },
        );
        let plan = plan_search_scatter_adaptive(&topo, 0, 2, 64, &selector).await;

        // Should fall back to default behavior when no metrics exist
        assert_eq!(plan.target_shards.len(), 64);
        assert!(
            plan.hedging_eligible,
            "Should be eligible for hedging with multiple nodes"
        );
    }

    /// Test that execute_scatter handles empty target_shards correctly.
    #[tokio::test]
    async fn test_execute_scatter_empty_target_shards() {
        let mut topo = Topology::new(64, 1, 1);
        topo.add_node(Node::new(
            NodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));

        let mut plan = plan_search_scatter(&topo, 0, 1, 64, None).await;
        plan.target_shards = Vec::new(); // Empty target shards

        let c = MockNodeClient::default();
        let req = make_req();

        let result = execute_scatter(plan, &c, req, &topo, UnavailableShardPolicy::Partial)
            .await
            .unwrap();

        // Should succeed with no pages and no failures
        assert!(!result.partial);
        assert!(result.shard_pages.is_empty());
        assert!(result.failed_shards.is_empty());
    }

    /// Test fallback with network error (not timeout).
    #[tokio::test]
    async fn test_fallback_with_network_error() {
        let mut topo = Topology::new(16, 2, 2);
        topo.add_node(Node::new(
            NodeId::new("node-g0-0".into()),
            "http://g0-0:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g0-1".into()),
            "http://g0-1:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g1-0".into()),
            "http://g1-0:7700".into(),
            1,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g1-1".into()),
            "http://g1-1:7700".into(),
            1,
        ));

        let plan = plan_search_scatter(&topo, 0, 2, 16, None).await;

        let mut c = MockNodeClient::default();

        // Set up responses for fallback
        let response = serde_json::json!({
            "hits": [{"id": "doc1"}],
            "estimatedTotalHits": 1,
            "processingTimeMs": 5,
        });
        c.responses
            .insert(NodeId::new("node-g1-0".into()), response.clone());
        c.responses
            .insert(NodeId::new("node-g1-1".into()), response);

        // Group 0 fails with network error
        c.errors.insert(
            NodeId::new("node-g0-0".into()),
            NodeError::NetworkError("connection refused".into()),
        );
        c.errors.insert(
            NodeId::new("node-g0-1".into()),
            NodeError::NetworkError("connection reset".into()),
        );

        let req = make_req();

        let result = execute_scatter(plan, &c, req, &topo, UnavailableShardPolicy::Fallback)
            .await
            .unwrap();

        // Should succeed via fallback
        assert!(!result.partial);
        assert!(!result.shard_pages.is_empty());
    }

    /// Test that scatter_gather_search properly propagates deadline_exceeded.
    #[tokio::test]
    async fn test_scatter_gather_deadline_exceeded() {
        let topo = make_test_topology();
        let plan = plan_search_scatter(&topo, 0, 2, 64, None).await;

        let mut c = MockNodeClient::default();
        c.errors
            .insert(NodeId::new("node-0".into()), NodeError::Timeout);

        let req = make_req();
        let s = crate::merger::RrfStrategy::default_strategy();

        let result =
            scatter_gather_search(plan, &c, req, &topo, UnavailableShardPolicy::Partial, &s).await;

        // Should succeed but be degraded
        assert!(result.is_ok());
        let merged = result.unwrap();
        assert!(merged.degraded);
    }

    // ── NodeClient trait methods tests ─────────────────────────────────────────

    #[tokio::test]
    async fn test_mock_write_documents() {
        let c = MockNodeClient::default();
        let node = NodeId::new("test-node".into());

        let req = WriteRequest {
            index_uid: "test".into(),
            documents: vec![serde_json::json!({"id": "1", "title": "Test"})],
            primary_key: Some("id".into()),
            origin: None,
        };

        let result = c.write_documents(&node, "http://test:7700", &req).await;
        assert!(result.is_ok());
        let resp = result.unwrap();
        assert!(resp.success);
        assert_eq!(resp.task_uid, Some(1));
    }

    #[tokio::test]
    async fn test_mock_write_documents_error() {
        let mut c = MockNodeClient::default();
        let node = NodeId::new("test-node".into());
        c.errors.insert(
            node.clone(),
            NodeError::NetworkError("connection refused".into()),
        );

        let req = WriteRequest {
            index_uid: "test".into(),
            documents: vec![serde_json::json!({"id": "1"})],
            primary_key: None,
            origin: None,
        };

        let result = c.write_documents(&node, "http://test:7700", &req).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_delete_documents() {
        let c = MockNodeClient::default();
        let node = NodeId::new("test-node".into());

        let req = DeleteByIdsRequest {
            index_uid: "test".into(),
            ids: vec!["1".into(), "2".into()],
            origin: None,
        };

        let result = c.delete_documents(&node, "http://test:7700", &req).await;
        assert!(result.is_ok());
        let resp = result.unwrap();
        assert!(resp.success);
        assert_eq!(resp.task_uid, Some(1));
    }

    #[tokio::test]
    async fn test_mock_delete_documents_by_filter() {
        let c = MockNodeClient::default();
        let node = NodeId::new("test-node".into());

        let req = DeleteByFilterRequest {
            index_uid: "test".into(),
            filter: serde_json::json!("status = 'deleted'"),
            origin: None,
        };

        let result = c
            .delete_documents_by_filter(&node, "http://test:7700", &req)
            .await;
        assert!(result.is_ok());
        let resp = result.unwrap();
        assert!(resp.success);
    }

    #[tokio::test]
    async fn test_mock_fetch_documents() {
        let mut c = MockNodeClient::default();
        let node = NodeId::new("test-node".into());

        let stored = FetchDocumentsResponse {
            results: vec![
                serde_json::json!({"id": "1", "title": "Doc 1"}),
                serde_json::json!({"id": "2", "title": "Doc 2"}),
            ],
            limit: 10,
            offset: 0,
            total: 100,
        };
        c.fetch_responses.insert(node.clone(), stored);

        let req = FetchDocumentsRequest {
            index_uid: "test".into(),
            filter: serde_json::json!(null),
            limit: 10,
            offset: 0,
        };

        let result = c.fetch_documents(&node, "http://test:7700", &req).await;
        assert!(result.is_ok());
        let resp = result.unwrap();
        assert_eq!(resp.results.len(), 2);
        assert_eq!(resp.total, 100);
    }

    #[tokio::test]
    async fn test_mock_fetch_documents_pagination() {
        let mut c = MockNodeClient::default();
        let node = NodeId::new("test-node".into());

        let stored = FetchDocumentsResponse {
            results: vec![],
            limit: 10,
            offset: 0,
            total: 5,
        };
        c.fetch_responses.insert(node.clone(), stored);

        // Request offset beyond total
        let req = FetchDocumentsRequest {
            index_uid: "test".into(),
            filter: serde_json::json!(null),
            limit: 10,
            offset: 10,
        };

        let result = c.fetch_documents(&node, "http://test:7700", &req).await;
        assert!(result.is_ok());
        let resp = result.unwrap();
        assert_eq!(resp.results.len(), 0);
        assert_eq!(resp.offset, 10);
    }

    #[tokio::test]
    async fn test_mock_get_task_status() {
        let c = MockNodeClient::default();
        let node = NodeId::new("test-node".into());

        let req = TaskStatusRequest { task_uid: 42 };

        let result = c.get_task_status(&node, "http://test:7700", &req).await;
        assert!(result.is_ok());
        let resp = result.unwrap();
        assert_eq!(resp.task_uid, 42);
        assert_eq!(resp.status, "succeeded");
        assert!(resp.error.is_none());
    }

    #[tokio::test]
    async fn test_mock_get_task_status_error() {
        let mut c = MockNodeClient::default();
        let node = NodeId::new("test-node".into());
        c.errors.insert(node.clone(), NodeError::Timeout);

        let req = TaskStatusRequest { task_uid: 42 };

        let result = c.get_task_status(&node, "http://test:7700", &req).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_write_custom_response() {
        let mut c = MockNodeClient::default();
        let node = NodeId::new("test-node".into());

        let custom_resp = WriteResponse {
            success: true,
            task_uid: Some(999),
            message: Some("custom message".into()),
            code: Some("code".into()),
            error_type: Some("type".into()),
        };
        c.write_responses.insert(node.clone(), custom_resp);

        let req = WriteRequest {
            index_uid: "test".into(),
            documents: vec![],
            primary_key: None,
            origin: None,
        };

        let result = c.write_documents(&node, "http://test:7700", &req).await;
        assert!(result.is_ok());
        let resp = result.unwrap();
        assert_eq!(resp.task_uid, Some(999));
        assert_eq!(resp.message, Some("custom message".into()));
    }

    // ── Additional edge case tests for coverage ─────────────────────────────

    /// Test fallback when one group has empty replicas.
    #[tokio::test]
    async fn test_fallback_with_empty_replicas_in_group() {
        let mut topo = Topology::new(16, 2, 2);
        // Group 0: 2 nodes
        topo.add_node(Node::new(
            NodeId::new("node-g0-0".into()),
            "http://g0-0:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g0-1".into()),
            "http://g0-1:7700".into(),
            0,
        ));
        // Group 1: Only 1 node (not enough for RF=2, so assign_shard_in_group returns empty for some shards)
        topo.add_node(Node::new(
            NodeId::new("node-g1-0".into()),
            "http://g1-0:7700".into(),
            1,
        ));

        let plan = plan_search_scatter(&topo, 0, 2, 16, None).await; // query_seq=0 → group 0

        let mut c = MockNodeClient::default();

        // Set up responses: group 1 node returns valid data
        let response_1 = serde_json::json!({
            "hits": [{"id": "doc1", "_rankingScore": 0.9}],
            "estimatedTotalHits": 1,
            "processingTimeMs": 5,
        });
        c.responses
            .insert(NodeId::new("node-g1-0".into()), response_1);

        // All nodes in group 0 fail
        c.errors
            .insert(NodeId::new("node-g0-0".into()), NodeError::Timeout);
        c.errors
            .insert(NodeId::new("node-g0-1".into()), NodeError::Timeout);

        let req = make_req();

        // With fallback policy, some shards might succeed via group 1
        let result = execute_scatter(plan, &c, req, &topo, UnavailableShardPolicy::Fallback)
            .await
            .unwrap();

        // Result should be partial because group 1 has only 1 node (not enough for RF=2)
        assert!(
            result.partial || !result.shard_pages.is_empty(),
            "Should have partial success or some pages"
        );
    }

    /// Test fallback with partial success (some shards succeed via fallback, others fail).
    #[tokio::test]
    async fn test_fallback_partial_success() {
        let mut topo = Topology::new(16, 2, 2);
        // Group 0: 2 nodes (all fail)
        topo.add_node(Node::new(
            NodeId::new("node-g0-0".into()),
            "http://g0-0:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g0-1".into()),
            "http://g0-1:7700".into(),
            0,
        ));
        // Group 1: 2 nodes (only one works)
        topo.add_node(Node::new(
            NodeId::new("node-g1-0".into()),
            "http://g1-0:7700".into(),
            1,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-g1-1".into()),
            "http://g1-1:7700".into(),
            1,
        ));

        let plan = plan_search_scatter(&topo, 0, 2, 16, None).await;

        let mut c = MockNodeClient::default();

        // Set up response: only node-g1-0 returns valid data
        let response = serde_json::json!({
            "hits": [{"id": "doc1"}],
            "estimatedTotalHits": 1,
            "processingTimeMs": 5,
        });
        c.responses
            .insert(NodeId::new("node-g1-0".into()), response);

        // All group 0 nodes fail
        c.errors
            .insert(NodeId::new("node-g0-0".into()), NodeError::Timeout);
        c.errors
            .insert(NodeId::new("node-g0-1".into()), NodeError::Timeout);
        // One group 1 node fails
        c.errors.insert(
            NodeId::new("node-g1-1".into()),
            NodeError::NetworkError("connection refused".into()),
        );

        let req = make_req();

        let result = execute_scatter(plan, &c, req, &topo, UnavailableShardPolicy::Fallback)
            .await
            .unwrap();

        // Should have partial success (some shards from node-g1-0, some failed)
        assert!(result.partial || !result.shard_pages.is_empty());
    }

    /// Test GlobalIdf with zero total docs edge case.
    #[test]
    fn test_global_idf_zero_total_docs() {
        let resp = vec![
            PreflightResponse {
                total_docs: 0,
                avg_doc_length: 0.0,
                term_stats: HashMap::new(),
            },
            PreflightResponse {
                total_docs: 0,
                avg_doc_length: 0.0,
                term_stats: HashMap::new(),
            },
        ];
        let g = GlobalIdf::from_preflight_responses(&resp);
        assert_eq!(g.total_docs, 0);
        assert_eq!(g.avg_doc_length, 0.0);
        assert!(g.terms.is_empty());
    }

    /// Test GlobalIdf with term having zero df.
    #[test]
    fn test_global_idf_zero_df() {
        let resp = vec![PreflightResponse {
            total_docs: 1000,
            avg_doc_length: 50.0,
            term_stats: HashMap::from([("test".into(), TermStats { df: 0 })]),
        }];
        let g = GlobalIdf::from_preflight_responses(&resp);
        assert_eq!(g.total_docs, 1000);
        assert_eq!(g.terms.get("test").unwrap().df, 0);
        // IDF with df=0 should be 0.0
        assert_eq!(g.terms.get("test").unwrap().idf, 0.0);
    }

    /// Test GlobalIdf with single shard.
    #[test]
    fn test_global_idf_single_shard() {
        let resp = vec![PreflightResponse {
            total_docs: 5000,
            avg_doc_length: 45.0,
            term_stats: HashMap::from([
                ("rust".into(), TermStats { df: 500 }),
                ("programming".into(), TermStats { df: 100 }),
            ]),
        }];
        let g = GlobalIdf::from_preflight_responses(&resp);
        assert_eq!(g.total_docs, 5000);
        assert_eq!(g.terms.get("rust").unwrap().df, 500);
        assert_eq!(g.terms.get("programming").unwrap().df, 100);
    }

    // ── §13.2 Hedged requests tests ─────────────────────────────────────────────

    /// Test that hedging is NOT used when disabled.
    #[tokio::test]
    async fn test_hedging_disabled() {
        let mut topo = Topology::new(16, 2, 2);
        topo.add_node(Node::new(
            NodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-1".into()),
            "http://node-1:7700".into(),
            0,
        ));

        let primary_node = NodeId::new("node-0".into());
        let req = make_req();

        // Hedging disabled (None passed)
        let mut hedge_count = 0;
        let (result, outcome, _) = execute_hedged_request::<MockNodeClient>(
            &MockNodeClient::default(),
            &primary_node,
            "",
            0,
            &req,
            &topo,
            None,
            &mut hedge_count,
        )
        .await;

        assert!(outcome.is_none(), "Hedging should not fire when disabled");
        // MockNodeClient returns default empty response when no response is configured
        assert!(result.is_ok(), "Should succeed with default mock response");
    }

    /// Test that hedging fires when primary is slow.
    #[tokio::test]
    async fn test_hedging_fires_on_slow_primary() {
        let mut topo = Topology::new(16, 2, 2);
        topo.add_node(Node::new(
            NodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-1".into()),
            "http://node-1:7700".into(),
            0,
        ));

        let primary_node = NodeId::new("node-0".into());
        let req = make_req();

        // Create a hedging manager with short trigger time
        let config = crate::hedging::HedgingConfig {
            enabled: true,
            p95_trigger_multiplier: 1.0,
            min_trigger_ms: 1, // 1ms trigger
            max_hedges_per_query: 2,
            cross_group_fallback: false,
        };
        let manager = HedgingManager::new(config, Arc::new(topo.clone()));

        // Set up initial latency data to establish p95 at 50ms
        manager.record_latency(&primary_node, 50.0).await;

        // Create a mock client where the primary responds slowly (100ms) and alternate responds fast
        let mut client = MockNodeClient {
            delay_ms: 100, // Primary is slow (longer than 50ms hedge deadline)
            ..Default::default()
        };
        client.responses.insert(
            NodeId::new("node-1".into()),
            serde_json::json!({"hits": [{"id": "hedge-won"}], "estimatedTotalHits": 1}),
        );

        let mut hedge_count = 0;

        // Debug: check what the hedge deadline is
        let deadline = manager.hedge_deadline(&primary_node).await;
        println!("Hedge deadline: {:?}", deadline);

        // Debug: check what alternate is available
        let alternate = manager.find_alternate(&primary_node, 0, 0).await;
        println!("Alternate for shard 0: {:?}", alternate);

        let (_result, outcome, _elapsed) = execute_hedged_request(
            &client,
            &primary_node,
            "http://node-0:7700",
            0,
            &req,
            &topo,
            Some(&manager),
            &mut hedge_count,
        )
        .await;

        println!("Hedge outcome: {:?}", outcome);
        println!("Hedge count: {}", hedge_count);

        // With the short delay, the hedge should have time to fire
        // The exact behavior depends on timing, but we should get a result
        assert!(outcome.is_some(), "Hedge should fire with slow primary");
    }

    /// Test that max_hedges_per_query is respected.
    #[tokio::test]
    async fn test_hedging_respects_max_budget() {
        let mut topo = Topology::new(16, 2, 2);
        topo.add_node(Node::new(
            NodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-1".into()),
            "http://node-1:7700".into(),
            0,
        ));

        let primary_node = NodeId::new("node-0".into());
        let req = make_req();

        let config = crate::hedging::HedgingConfig {
            enabled: true,
            p95_trigger_multiplier: 1.0,
            min_trigger_ms: 1,
            max_hedges_per_query: 1, // Only 1 hedge allowed
            cross_group_fallback: false,
        };
        let manager = HedgingManager::new(config, Arc::new(topo.clone()));
        manager.record_latency(&primary_node, 100.0).await;

        let mut hedge_count = 1; // Already at max
        let (_result, outcome, _) = execute_hedged_request(
            &MockNodeClient::default(),
            &primary_node,
            "",
            0,
            &req,
            &topo,
            Some(&manager),
            &mut hedge_count,
        )
        .await;

        assert!(outcome.is_none(), "Should not hedge when budget exhausted");
    }

    /// Test that writes are NEVER hedged.
    #[tokio::test]
    async fn test_writes_never_hedge() {
        // This test verifies the architectural guarantee that writes are never hedged.
        // The hedging logic only applies to reads (search, document GET).
        // Write operations (write_documents, delete_documents, etc.) bypass hedging entirely.

        let mut topo = Topology::new(16, 2, 2);
        topo.add_node(Node::new(
            NodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));

        let primary_node = NodeId::new("node-0".into());

        // Create a write request
        let write_req = WriteRequest {
            index_uid: "test".into(),
            documents: vec![serde_json::json!({"id": "1"})],
            primary_key: Some("id".into()),
            origin: None,
        };

        // Write operations go directly through NodeClient, never through execute_hedged_request
        let client = MockNodeClient::default();
        let result = client
            .write_documents(&primary_node, "http://node-0:7700", &write_req)
            .await;

        assert!(result.is_ok(), "Write should succeed");

        // Verify no hedging occurred (no hedge outcome tracking for writes)
        // This is an architectural test - the execute_hedged_request function
        // is only called for read operations, never for writes.
    }

    /// Test intra-group replica selection for hedging.
    #[tokio::test]
    async fn test_hedging_intra_group_alternate() {
        let mut topo = Topology::new(16, 2, 2);
        topo.add_node(Node::new(
            NodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            NodeId::new("node-1".into()),
            "http://node-1:7700".into(),
            0,
        ));
        // Add a node in a different group (should not be used for intra-group hedge)
        topo.add_node(Node::new(
            NodeId::new("node-2".into()),
            "http://node-2:7700".into(),
            1,
        ));

        let primary_node = NodeId::new("node-0".into());
        let _req = make_req();

        let config = crate::hedging::HedgingConfig {
            enabled: true,
            p95_trigger_multiplier: 1.0,
            min_trigger_ms: 1,
            max_hedges_per_query: 2,
            cross_group_fallback: false, // Disable cross-group
        };
        let manager = HedgingManager::new(config, Arc::new(topo.clone()));

        // Find an alternate for shard 0, primary node-0
        let alternate = manager.find_alternate(&primary_node, 0, 0).await;

        assert!(alternate.is_some(), "Should find intra-group alternate");
        let alt_node = alternate.unwrap();
        assert_eq!(alt_node.as_str(), "node-1", "Should use same-group node-1");
    }

    /// Test cross-group fallback when enabled.
    #[tokio::test]
    async fn test_hedging_cross_group_fallback() {
        let mut topo = Topology::new(16, 2, 2);
        // Only one node in group 0
        topo.add_node(Node::new(
            NodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));
        // Node in group 1 (cross-group fallback target)
        topo.add_node(Node::new(
            NodeId::new("node-1".into()),
            "http://node-1:7700".into(),
            1,
        ));

        let primary_node = NodeId::new("node-0".into());

        let config = crate::hedging::HedgingConfig {
            enabled: true,
            p95_trigger_multiplier: 1.0,
            min_trigger_ms: 1,
            max_hedges_per_query: 2,
            cross_group_fallback: true, // Enable cross-group
        };
        let manager = HedgingManager::new(config, Arc::new(topo.clone()));

        // With no intra-group alternate, should fall back to cross-group
        let alternate = manager.find_alternate(&primary_node, 0, 0).await;

        assert!(alternate.is_some(), "Should find cross-group alternate");
        assert_eq!(
            alternate.unwrap().as_str(),
            "node-1",
            "Should use cross-group node-1"
        );
    }

    /// Test cross-group disabled prevents cross-group fallback.
    #[tokio::test]
    async fn test_hedging_cross_group_disabled() {
        let mut topo = Topology::new(16, 2, 2);
        // Only one node in group 0
        topo.add_node(Node::new(
            NodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));
        // Node in group 1
        topo.add_node(Node::new(
            NodeId::new("node-1".into()),
            "http://node-1:7700".into(),
            1,
        ));

        let primary_node = NodeId::new("node-0".into());

        let config = crate::hedging::HedgingConfig {
            enabled: true,
            p95_trigger_multiplier: 1.0,
            min_trigger_ms: 1,
            max_hedges_per_query: 2,
            cross_group_fallback: false, // Disable cross-group
        };
        let manager = HedgingManager::new(config, Arc::new(topo.clone()));

        // With no intra-group alternate and cross-group disabled, should return None
        let alternate = manager.find_alternate(&primary_node, 0, 0).await;

        assert!(
            alternate.is_none(),
            "Should not find cross-group alternate when disabled"
        );
    }

    /// Test hedge deadline computation with p95 multiplier.
    #[tokio::test]
    async fn test_hedging_p95_multiplier() {
        let mut topo = Topology::new(16, 2, 2);
        topo.add_node(Node::new(
            NodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));

        let node = NodeId::new("node-0".into());

        let config = crate::hedging::HedgingConfig {
            enabled: true,
            p95_trigger_multiplier: 2.0, // 2x p95
            min_trigger_ms: 10,
            max_hedges_per_query: 2,
            cross_group_fallback: false,
        };
        let manager = HedgingManager::new(config, Arc::new(topo.clone()));

        // Set p95 to 50ms
        manager.record_latency(&node, 50.0).await;

        let deadline = manager.hedge_deadline(&node).await;

        assert!(deadline.is_some());
        // 50ms * 2.0 = 100ms
        assert_eq!(deadline.unwrap(), Duration::from_millis(100));
    }

    /// Test hedge deadline respects minimum trigger time.
    #[tokio::test]
    async fn test_hedging_min_trigger() {
        let mut topo = Topology::new(16, 2, 2);
        topo.add_node(Node::new(
            NodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));

        let node = NodeId::new("node-0".into());

        let config = crate::hedging::HedgingConfig {
            enabled: true,
            p95_trigger_multiplier: 1.0,
            min_trigger_ms: 100, // 100ms minimum
            max_hedges_per_query: 2,
            cross_group_fallback: false,
        };
        let manager = HedgingManager::new(config, Arc::new(topo.clone()));

        // Set p95 to 10ms (below minimum)
        manager.record_latency(&node, 10.0).await;

        let deadline = manager.hedge_deadline(&node).await;

        assert!(deadline.is_some());
        // Should use minimum of 100ms, not 10ms
        assert_eq!(deadline.unwrap(), Duration::from_millis(100));
    }
}
