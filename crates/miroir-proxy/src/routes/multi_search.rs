//! Multi-search batch API endpoint (plan §13.11).
#![allow(dead_code)]

use axum::{
    extract::{FromRef, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use miroir_core::{
    config::UnavailableShardPolicy,
    merger::{MergeStrategy, ScoreMergeStrategy},
    multi_search::{MultiSearchExecutor, MultiSearchResponse, SearchResultData},
    scatter::{
        dfs_query_then_fetch_search, plan_search_scatter_with_narrowing, NodeClient, SearchRequest,
    },
    shadow::ShadowOperation,
    topology::Topology,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, instrument};

/// Multi-search state.
#[derive(Clone)]
pub struct MultiSearchState {
    pub config: Arc<miroir_core::config::MiroirConfig>,
    pub topology: Arc<RwLock<Topology>>,
    pub node_master_key: String,
    pub metrics: crate::middleware::Metrics,
    pub alias_registry: Arc<miroir_core::alias::AliasRegistry>,
    pub replica_selector: Arc<miroir_core::replica_selection::ReplicaSelector>,
    pub query_planner: Arc<miroir_core::query_planner::QueryPlanner>,
    pub shadow_manager: Option<Arc<miroir_core::shadow::ShadowManager>>,
}

/// Multi-search request (plan §13.11).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MultiSearchRequest {
    pub queries: Vec<SingleSearchQuery>,
}

/// A single query in the batch.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SingleSearchQuery {
    pub index_uid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub q: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ranking_score: Option<bool>,
    #[serde(flatten)]
    pub rest: Value,
}

/// Search response (matches Meilisearch response format).
#[derive(Debug, Clone, Serialize, Default)]
pub struct SearchResponse {
    pub hits: Vec<serde_json::Value>,
    pub estimated_total_hits: u64,
    pub limit: usize,
    pub offset: usize,
    pub processing_time_ms: u64,
    pub query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet_distribution:
        Option<std::collections::BTreeMap<String, std::collections::BTreeMap<String, u64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub degraded: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_shards: Option<Vec<u32>>,
}

/// Node client implementation using the HTTP client.
#[derive(Clone)]
pub struct ProxyNodeClient {
    client: Arc<crate::client::HttpClient>,
    metrics: Arc<crate::middleware::Metrics>,
}

impl ProxyNodeClient {
    pub fn new(
        client: Arc<crate::client::HttpClient>,
        metrics: crate::middleware::Metrics,
    ) -> Self {
        Self {
            client,
            metrics: Arc::new(metrics),
        }
    }
}

#[allow(async_fn_in_trait)]
impl NodeClient for ProxyNodeClient {
    async fn search_node(
        &self,
        node: &miroir_core::topology::NodeId,
        address: &str,
        request: &SearchRequest,
    ) -> std::result::Result<Value, miroir_core::scatter::NodeError> {
        let start = Instant::now();
        let result = self.client.search_node(node, address, request).await;
        let elapsed = start.elapsed().as_secs_f64();
        self.metrics
            .record_node_request_duration(node.as_str(), "search", elapsed);
        if let Err(ref e) = result {
            self.metrics.inc_node_errors(node.as_str(), error_label(e));
        }
        result
    }

    async fn preflight_node(
        &self,
        node: &miroir_core::topology::NodeId,
        address: &str,
        request: &miroir_core::scatter::PreflightRequest,
    ) -> std::result::Result<miroir_core::scatter::PreflightResponse, miroir_core::scatter::NodeError>
    {
        let start = Instant::now();
        let result = self.client.preflight_node(node, address, request).await;
        let elapsed = start.elapsed().as_secs_f64();
        self.metrics
            .record_node_request_duration(node.as_str(), "preflight", elapsed);
        if let Err(ref e) = result {
            self.metrics.inc_node_errors(node.as_str(), error_label(e));
        }
        result
    }
}

fn error_label(e: &miroir_core::scatter::NodeError) -> &'static str {
    match e {
        miroir_core::scatter::NodeError::NetworkError(_) => "network",
        miroir_core::scatter::NodeError::HttpError { .. } => "http",
        miroir_core::scatter::NodeError::Timeout => "timeout",
    }
}

/// POST /multi-search — execute multiple searches in a single batch.
///
/// Plan §13.11: Reduces round-trips for search UIs that need results + facets
/// from multiple queries per page render. Each query runs in parallel.
#[instrument(skip_all, fields(query_count = body.queries.len()))]
pub async fn multi_search<S>(
    State(state): State<MultiSearchState>,
    headers: HeaderMap,
    Json(body): Json<MultiSearchRequest>,
) -> Result<Json<MultiSearchResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    MultiSearchState: FromRef<S>,
{
    if !state.config.multi_search.enabled {
        return Err(StatusCode::NOT_IMPLEMENTED);
    }

    if body.queries.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    if body.queries.len() > state.config.multi_search.max_queries_per_batch as usize {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Record multi-search metrics (plan §13.11)
    let query_count = body.queries.len() as u64;
    state
        .metrics
        .observe_multisearch_queries_per_batch(query_count);
    state.metrics.inc_multisearch_batches_total();

    // Extract X-Miroir-Over-Fetch header (plan §13.12)
    // Per-request override of vector_search.over_fetch_factor
    let over_fetch_factor = headers
        .get("X-Miroir-Over-Fetch")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|&f| f >= 1)
        .unwrap_or_else(|| state.config.vector_search.over_fetch_factor);

    let executor = MultiSearchExecutor::new(state.config.multi_search.clone());

    // Get topology and policy once for all queries
    let topology = state.topology.read().await;
    let policy = match state.config.scatter.unavailable_shard_policy.as_str() {
        "partial" => UnavailableShardPolicy::Partial,
        "error" => UnavailableShardPolicy::Error,
        "fallback" => UnavailableShardPolicy::Fallback,
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // Create node client
    let http_client = Arc::new(crate::client::HttpClient::new(
        state.node_master_key.clone(),
        state.config.scatter.node_timeout_ms,
    ));
    let node_client = ProxyNodeClient::new(http_client, state.metrics.clone());
    let strategy = ScoreMergeStrategy::new();

    // Convert MultiSearchRequest to core MultiSearchRequest
    // Resolve aliases for each query (plan §13.7)
    let mut queries_with_resolutions = Vec::new();

    // Clone queries for shadowing before they're consumed
    let original_queries_for_shadow = body.queries.clone();

    for mut q in body.queries {
        // Resolve alias to concrete index UID(s)
        let (effective_index, resolved_targets) = if state.config.aliases.enabled {
            let targets = state.alias_registry.resolve(&q.index_uid).await;
            state.metrics.inc_alias_resolution(&q.index_uid);
            if targets != vec![q.index_uid.clone()] {
                // It's an alias
                (q.index_uid.clone(), targets)
            } else {
                // Not an alias
                (q.index_uid.clone(), vec![q.index_uid.clone()])
            }
        } else {
            (q.index_uid.clone(), vec![q.index_uid.clone()])
        };

        // For multi-target aliases, we use the first target for the query
        // (multi-target alias fanout is handled by expanding queries in future)
        q.index_uid = effective_index;

        let filter_str = q.filter.as_ref().and_then(|v| {
            if v.is_null() || v.is_string() && v.as_str().map(|s| s.is_empty()).unwrap_or(false) {
                None
            } else {
                serde_json::to_string(v).ok()
            }
        });

        queries_with_resolutions.push((q, filter_str, resolved_targets));
    }

    // Capture shadow config and manager before executor (state will be moved into closure)
    let (shadow_config_for_later, shadow_manager_for_later) =
        if let Some(ref sm) = state.shadow_manager {
            (Some(state.config.shadow.clone()), Some(sm.clone()))
        } else {
            (None, None)
        };

    let core_request = miroir_core::multi_search::MultiSearchRequest {
        queries: queries_with_resolutions
            .into_iter()
            .map(|(q, filter_str, _resolved_targets)| {
                miroir_core::multi_search::SearchQuery {
                    index_uid: q.index_uid,
                    q: q.q,
                    filter: filter_str,
                    limit: q.limit,
                    offset: q.offset,
                    other: {
                        let mut map = std::collections::HashMap::new();
                        if let Some(sort) = q.sort {
                            map.insert("sort".to_string(), serde_json::to_value(sort).unwrap());
                        }
                        if let Some(facets) = q.facets {
                            map.insert("facets".to_string(), serde_json::to_value(facets).unwrap());
                        }
                        if let Some(ranking_score) = q.ranking_score {
                            map.insert(
                                "rankingScore".to_string(),
                                serde_json::to_value(ranking_score).unwrap(),
                            );
                        }
                        // Add any additional fields from rest
                        if let Ok(obj) = serde_json::from_value::<
                            std::collections::HashMap<String, Value>,
                        >(q.rest)
                        {
                            for (k, v) in obj {
                                map.entry(k).or_insert(v);
                            }
                        }
                        map
                    },
                }
            })
            .collect(),
    };

    // Execute multi-search with scatter-gather
    // Clone state fields before closure to avoid moving state
    let config_for_executor = state.config.clone();
    let replica_selector_for_executor = state.replica_selector.clone();
    let query_planner_for_executor = state.query_planner.clone();
    let metrics_for_executor = state.metrics.clone();
    let over_fetch_factor_for_executor = over_fetch_factor;

    let response = executor
        .execute(core_request, move |query| {
            let topology = topology.clone();
            let node_client = node_client.clone();
            let config = config_for_executor.clone();
            let strategy = strategy;
            let policy = policy;
            let replica_selector = replica_selector_for_executor.clone();
            let query_planner = query_planner_for_executor.clone();
            let metrics = metrics_for_executor.clone();
            let over_fetch_factor = over_fetch_factor_for_executor;

            async move {
                let start = Instant::now();

                // Determine if we should use adaptive selection
                let use_adaptive = config.replica_selection.strategy == "adaptive";
                let replica_selector_ref = if use_adaptive {
                    Some(replica_selector.as_ref())
                } else {
                    None
                };

                // Use query planner to narrow target shards (plan §13.4)
                let filter_str = query.filter.as_ref().filter(|s| !s.is_empty()).cloned();
                let query_plan = query_planner
                    .plan(&query.index_uid, &filter_str, config.shards)
                    .await;

                // Record query planner metrics
                metrics.inc_query_plan_narrowable(query_plan.narrowed);
                if query_plan.narrowed {
                    metrics.observe_query_plan_fanout(query_plan.target_shards.len() as u32);
                    let ratio = query_plan.target_shards.len() as f64 / config.shards as f64;
                    metrics.set_query_plan_narrowing_ratio(ratio);
                } else {
                    metrics.observe_query_plan_fanout(config.shards);
                    metrics.set_query_plan_narrowing_ratio(1.0);
                }

                // Plan scatter with narrowed target shards
                let target_shards = if query_plan.narrowed {
                    Some(query_plan.target_shards)
                } else {
                    None
                };

                let plan = plan_search_scatter_with_narrowing(
                    &topology,
                    0,
                    config.replication_factor as usize,
                    config.shards,
                    replica_selector_ref,
                    target_shards,
                )
                .await;

                // Build search request
                let filter_value = query
                    .filter
                    .as_ref()
                    .and_then(|s| serde_json::from_str::<Value>(s).ok());

                // Detect vector search mode (plan §13.12)
                let vector_mode =
                    SearchRequest::detect_vector_mode(&serde_json::json!(query.other));

                let search_req = SearchRequest {
                    index_uid: query.index_uid.clone(),
                    query: query.q.clone(),
                    offset: query.offset.unwrap_or(0),
                    limit: query.limit.unwrap_or(20),
                    filter: filter_value,
                    facets: query
                        .other
                        .get("facets")
                        .and_then(|v| serde_json::from_value::<Vec<String>>(v.clone()).ok()),
                    ranking_score: query
                        .other
                        .get("rankingScore")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                    body: serde_json::json!(query.other),
                    global_idf: None,
                    over_fetch_factor,
                    vector_mode,
                    vector_config: None,
                };

                // Execute DFS query-then-fetch
                match dfs_query_then_fetch_search(
                    plan,
                    &node_client,
                    search_req,
                    &topology,
                    policy,
                    &strategy as &dyn MergeStrategy,
                )
                .await
                {
                    Ok(result) => {
                        // Strip internal fields from hits
                        let mut hits = result.hits;
                        for hit in &mut hits {
                            if let Some(obj) = hit.as_object_mut() {
                                obj.remove("_miroir_shard");
                            }
                        }

                        let response_limit = query.limit.unwrap_or(20);
                        let response_offset = query.offset.unwrap_or(0);

                        let search_response = SearchResponse {
                            hits,
                            estimated_total_hits: result.estimated_total_hits,
                            limit: response_limit,
                            offset: response_offset,
                            processing_time_ms: result.processing_time_ms,
                            query: query.q,
                            facet_distribution: result.facet_distribution,
                            degraded: if result.degraded { Some(true) } else { None },
                            failed_shards: if !result.failed_shards.is_empty() {
                                Some(result.failed_shards)
                            } else {
                                None
                            },
                        };

                        debug!(
                            index = %query.index_uid,
                            duration_ms = start.elapsed().as_millis(),
                            hits = search_response.hits.len(),
                            "multi-search query completed"
                        );

                        Ok(SearchResultData {
                            body: serde_json::to_value(search_response).unwrap(),
                        })
                    }
                    Err(e) => {
                        debug!(
                            index = %query.index_uid,
                            error = %e,
                            "multi-search query failed"
                        );
                        Err(e)
                    }
                }
            }
        })
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "multi-search execution failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Record partial failures metric (plan §13.11)
    let has_failures = response.results.iter().any(|r| !r.is_success());
    if has_failures {
        state.metrics.inc_multisearch_partial_failures();
    }

    // Shadow each query in the batch to configured targets (plan §13.16)
    // This is done asynchronously after returning the primary response
    if let (Some(ref shadow_manager), Some(ref shadow_config)) =
        (shadow_manager_for_later, shadow_config_for_later)
    {
        let shadow_mgr = shadow_manager.clone();
        let config = shadow_config.clone();
        let original_queries = original_queries_for_shadow;

        tokio::spawn(async move {
            if !config.enabled {
                return;
            }

            let targets = config.targets;

            // Shadow each query in the batch
            for query in original_queries {
                for target in &targets {
                    // Check if this target has multi_search operation enabled
                    if !target.operations.iter().any(|op| op == "multi_search") {
                        continue;
                    }

                    let shadow_target = miroir_core::shadow::ShadowTarget {
                        name: target.name.clone(),
                        url: target.url.clone(),
                        api_key_env: target.api_key_env.clone(),
                        sample_rate: target.sample_rate,
                        operations: target
                            .operations
                            .clone()
                            .into_iter()
                            .filter_map(|op| match op.as_str() {
                                "search" => Some(ShadowOperation::Search),
                                "multi_search" => Some(ShadowOperation::MultiSearch),
                                "explain" => Some(ShadowOperation::Explain),
                                _ => None,
                            })
                            .collect(),
                    };

                    // Check if this request should be shadowed
                    if !shadow_mgr.should_shadow(&shadow_target) {
                        continue;
                    }

                    // Build the request body for this individual query
                    let request_body = serde_json::json!({
                        "q": query.q,
                        "filter": query.filter,
                        "limit": query.limit,
                        "offset": query.offset,
                        "facets": query.facets,
                        "rankingScore": query.ranking_score,
                    });

                    // Shadow the request (with empty hits since we don't have them in multi_search response)
                    let result = shadow_mgr
                        .shadow_search(
                            &shadow_target,
                            &query.index_uid,
                            &request_body,
                            0, // No latency info in multi_search
                            &[],
                        )
                        .await;

                    match result {
                        Ok(_) => {
                            tracing::debug!(
                                target = shadow_target.name,
                                index = %query.index_uid,
                                "multi_search shadow request completed"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                target = shadow_target.name,
                                index = %query.index_uid,
                                error = %e,
                                "multi_search shadow request failed"
                            );
                        }
                    }
                }
            }
        });
    }

    Ok(Json(response))
}

pub fn router<S>() -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
    MultiSearchState: FromRef<S>,
{
    axum::Router::new().route("/", axum::routing::post(multi_search::<S>))
}
