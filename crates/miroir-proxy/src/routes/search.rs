//! Search route handler with DFS (Distributed Frequency Search) support.

use axum::extract::{Extension, Path};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::Json;
use miroir_core::api_error::{MeilisearchError, MiroirCode};
use miroir_core::config::UnavailableShardPolicy;
use miroir_core::merger::ScoreMergeStrategy;
use miroir_core::scatter::{
    dfs_query_then_fetch_search, plan_search_scatter, plan_search_scatter_with_version_floor, SearchRequest, NodeClient,
};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info_span, instrument, warn};

use crate::routes::admin_endpoints::{AppState, parse_rate_limit};

/// Hash a value for logging (obfuscates sensitive data like IPs).
fn hash_for_log(value: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Node client implementation using the HTTP client.
///
/// Wraps each node call with Prometheus metrics recording for
/// `miroir_node_request_duration_seconds` and `miroir_node_errors_total`.
pub struct ProxyNodeClient {
    client: Arc<crate::client::HttpClient>,
    metrics: crate::middleware::Metrics,
}

impl ProxyNodeClient {
    pub fn new(client: Arc<crate::client::HttpClient>, metrics: crate::middleware::Metrics) -> Self {
        Self { client, metrics }
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
        self.metrics.record_node_request_duration(node.as_str(), "search", elapsed);
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
    ) -> std::result::Result<miroir_core::scatter::PreflightResponse, miroir_core::scatter::NodeError> {
        let start = Instant::now();
        let result = self.client.preflight_node(node, address, request).await;
        let elapsed = start.elapsed().as_secs_f64();
        self.metrics.record_node_request_duration(node.as_str(), "preflight", elapsed);
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

pub fn router<S>() -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    axum::Router::new()
        .route("/:index", axum::routing::post(search_handler))
}

/// Search request body.
#[derive(Deserialize)]
struct SearchRequestBody {
    q: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
    filter: Option<Value>,
    facets: Option<Vec<String>>,
    #[serde(rename = "rankingScore")]
    ranking_score: Option<bool>,
    #[serde(flatten)]
    rest: Value,
}

impl std::fmt::Debug for SearchRequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchRequestBody")
            .field("q", &"[redacted]")
            .field("offset", &self.offset)
            .field("limit", &self.limit)
            .field("filter", &"[redacted]")
            .field("facets", &self.facets)
            .field("ranking_score", &self.ranking_score)
            .finish_non_exhaustive()
    }
}

/// Search handler with DFS global-IDF preflight (OP#4).
///
/// This handler implements the `dfs_query_then_fetch` pattern:
/// 1. **Preflight phase**: Send term-frequency query to all shards, aggregate
///    global document frequencies at the coordinator.
/// 2. **Search phase**: Send the search query with global IDF attached so that
///    scoring uses corpus-wide statistics instead of per-shard local IDF.
///
/// Returns `X-Miroir-Degraded: shards=X,Y,Z` header when any shards are unavailable.
/// Strips `_miroir_shard` from all hits; strips `_rankingScore` unless client
/// explicitly requested it.
#[instrument(skip_all, fields(index = %index))]
async fn search_handler(
    Path(index): Path<String>,
    Extension(state): Extension<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SearchRequestBody>,
) -> Result<Response, StatusCode> {
    let start = Instant::now();
    let client_requested_score = body.ranking_score.unwrap_or(false);

    // Extract source IP from X-Forwarded-For or X-Real-IP (trust proxy)
    let source_ip = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .or_else(|| headers.get("x-real-ip").and_then(|v| v.to_str().ok()))
        .unwrap_or("unknown")
        .trim()
        .to_string();

    // Check rate limit for search UI (plan §4)
    let (limit, window_seconds) = match parse_rate_limit(&state.config.search_ui.rate_limit.per_ip) {
        Ok(parsed) => parsed,
        Err(e) => {
            warn!(error = %e, "invalid search_ui.rate_limit.per_ip config, using default");
            (60, 60) // Default: 60 requests per minute
        }
    };

    let backend = state.config.search_ui.rate_limit.backend.as_str();
    if backend == "redis" {
        if let Some(ref redis) = state.redis_store {
            match redis.check_rate_limit_search_ui(&source_ip, limit, window_seconds) {
                Ok((allowed, _wait_seconds)) => {
                    if !allowed {
                        warn!(
                            source_ip_hash = hash_for_log(&source_ip),
                            "search UI rate limited (redis)"
                        );
                        return Err(StatusCode::TOO_MANY_REQUESTS);
                    }
                    // Allowed, proceed
                }
                Err(e) => {
                    warn!(error = %e, "failed to check search UI rate limit");
                    // Continue anyway on error (fail-open)
                }
            }
        }
    } else if backend == "local" {
        let (allowed, _wait_seconds) = state.local_search_ui_rate_limiter.check(
            &source_ip,
            limit,
            window_seconds * 1000,
        );
        if !allowed {
            warn!(
                source_ip_hash = hash_for_log(&source_ip),
                "search UI rate limited (local backend)"
            );
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }
    }

    // Get the scoped key for this index (plan §13.21).
    // If a scoped key exists, use primary_key (or previous_key during rotation overlap).
    // If no scoped key exists yet, fall back to node_master_key for initial setup.
    let search_key = if let Some(ref redis) = state.redis_store {
        if let Ok(Some(sk)) = redis.get_search_ui_scoped_key(&index) {
            // Refresh scoped-key beacon so the rotation leader knows this pod is serving
            // requests for this index at the current generation (plan §13.21).
            let _ = redis.observe_search_ui_scoped_key(
                &state.pod_id,
                &index,
                sk.generation,
            );

            // Use primary_key; previous_key is the overlap fallback (both are valid in Meilisearch)
            sk.primary_key
        } else {
            // No scoped key yet — fall back to node_master_key for initial setup
            state.config.node_master_key.clone()
        }
    } else {
        // No Redis store — fall back to node_master_key (single-pod dev mode)
        state.config.node_master_key.clone()
    };

    // Extract X-Miroir-Min-Settings-Version header (plan §13.5)
    let min_settings_version = headers
        .get("X-Miroir-Min-Settings-Version")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    // Use live topology from shared state (updated by health checker)
    let topo = state.topology.read().await;
    let policy = match state.config.scatter.unavailable_shard_policy.as_str() {
        "partial" => UnavailableShardPolicy::Partial,
        "error" => UnavailableShardPolicy::Error,
        "fallback" => UnavailableShardPolicy::Fallback,
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // Plan scatter using live topology (span for plan construction)
    let plan = {
        let _plan_span = tracing::info_span!(
            "scatter_plan",
            replica_groups = state.config.replica_groups,
            shards = state.config.shards,
            rf = state.config.replication_factor,
            min_settings_version,
        ).entered();

        // If client provided a min settings version floor, use version-filtered planning
        if let Some(floor) = min_settings_version {
            // Clone the settings broadcast for version checking
            let settings_broadcast = state.settings_broadcast.clone();
            let plan_result = plan_search_scatter_with_version_floor(
                &topo,
                0,
                state.config.replication_factor as usize,
                state.config.shards,
                &index,
                floor,
                &move |idx, node_id| {
                    // Use a blocking task wrapper since we're in a sync context
                    let sb = settings_broadcast.clone();
                    let idx = idx.to_string();
                    let node_id = node_id.to_string();
                    tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(async {
                            sb.node_version(&idx, &node_id).await
                        })
                    })
                },
            );

            match plan_result {
                Some(p) => p,
                None => {
                    // No covering set could be assembled after filtering by version floor
                    let err = MeilisearchError::new(
                        MiroirCode::SettingsVersionStale,
                        format!(
                            "no covering set available for settings version floor {} on index '{}'",
                            floor, index
                        ),
                    );
                    return Err(StatusCode::SERVICE_UNAVAILABLE);
                }
            }
        } else {
            // No version floor requested, use normal planning
            plan_search_scatter(&topo, 0, state.config.replication_factor as usize, state.config.shards)
        }
    };
    let node_count = plan.shard_to_node.len() as u64;

    // Record scatter fan-out size before executing
    state.metrics.record_scatter_fan_out(node_count);

    // Build search request
    let search_req = SearchRequest {
        index_uid: index.clone(),
        query: body.q,
        offset: body.offset.unwrap_or(0),
        limit: body.limit.unwrap_or(20),
        filter: body.filter,
        facets: body.facets,
        ranking_score: client_requested_score,
        body: body.rest,
        global_idf: None,
    };

    // Create node client with the scoped key (or node_master_key as fallback)
    let http_client = Arc::new(crate::client::HttpClient::new(
        search_key,
        state.config.scatter.node_timeout_ms,
    ));
    let client = ProxyNodeClient::new(http_client, state.metrics.clone());

    // Use score-based merge strategy (OP#4: requires global IDF)
    let strategy = ScoreMergeStrategy::new();

    // Execute DFS query-then-fetch
    let ttl_enabled = state.config.ttl.enabled;
    let mut result = dfs_query_then_fetch_search(
        plan,
        &client,
        search_req,
        &topo,
        policy,
        &strategy,
        ttl_enabled,
    )
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "search failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Drop topology lock before building response
    drop(topo);

    // Strip internal fields from hits
    for hit in &mut result.hits {
        strip_internal_fields(hit, client_requested_score);
    }

    // Build response body
    let mut body = serde_json::json!({
        "hits": result.hits,
        "estimatedTotalHits": result.estimated_total_hits,
        "processingTimeMs": result.processing_time_ms,
    });

    // Only include facetDistribution if facets were requested
    if let Some(facets) = &result.facet_distribution {
        body["facetDistribution"] = serde_json::to_value(facets).unwrap_or(Value::Null);
    }

    // Build response with optional headers
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json");

    // Add X-Miroir-Settings-Inconsistent header if a broadcast is in flight (plan §13.5)
    if state.settings_broadcast.is_in_flight(&index).await {
        response = response.header("X-Miroir-Settings-Inconsistent", "true");
    }

    // Add X-Miroir-Settings-Version header if we have a version for this index
    let current_version = state.settings_broadcast.current_version().await;
    if current_version > 0 {
        response = response.header("X-Miroir-Settings-Version", current_version.to_string());
    }

    if result.degraded {
        state.metrics.inc_scatter_partial_responses();
    }

    if result.degraded && !result.failed_shards.is_empty() {
        let mut sorted_shards = result.failed_shards.clone();
        sorted_shards.sort();
        let shard_ids = sorted_shards.iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        response = response.header("X-Miroir-Degraded", format!("shards={}", shard_ids));
    } else if result.degraded {
        response = response.header("X-Miroir-Degraded", "partial");
    }

    let response = response
        .body(axum::body::Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();

    // Structured log entry (plan §10 shape)
    // request_id and pod_id are included from the middleware span via
    // .with_current_span(true) on the JSON subscriber layer.
    tracing::info!(
        target: "miroir.search",
        index = %index,
        duration_ms = start.elapsed().as_millis() as u64,
        node_count = node_count,
        estimated_hits = result.estimated_total_hits,
        degraded = result.degraded,
        "search completed"
    );

    Ok(response)
}

/// Strip `_miroir_shard` from all hits (always).
/// Strip `_rankingScore` unless the client explicitly requested it.
pub fn strip_internal_fields(hit: &mut Value, client_requested_score: bool) {
    if let Some(obj) = hit.as_object_mut() {
        obj.remove("_miroir_shard");
        if !client_requested_score {
            obj.remove("_rankingScore");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_request_debug_redaction() {
        let body = SearchRequestBody {
            q: Some("sensitive user query about private data".to_string()),
            offset: Some(0),
            limit: Some(20),
            filter: Some(serde_json::json!({"email": "user@example.com"})),
            facets: Some(vec!["category".to_string()]),
            ranking_score: Some(false),
            rest: serde_json::json!({}),
        };
        let debug_output = format!("{:?}", body);

        assert!(
            !debug_output.contains("sensitive"),
            "Debug output should not contain raw query text"
        );
        assert!(
            !debug_output.contains("user@example.com"),
            "Debug output should not contain filter values"
        );
        assert!(
            debug_output.contains("[redacted]"),
            "Debug output should show [redacted] for sensitive fields"
        );
    }
}
