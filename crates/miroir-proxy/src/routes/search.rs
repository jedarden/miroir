//! Search route handler with DFS (Distributed Frequency Search) support.

use axum::extract::{Extension, Path};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::Json;
use miroir_core::api_error::{MeilisearchError, MiroirCode};
use miroir_core::config::UnavailableShardPolicy;
use miroir_core::idempotency::{QueryFingerprint, canonicalize_json};
use miroir_core::merger::ScoreMergeStrategy;
use miroir_core::replica_selection::SelectionObserver;
use miroir_core::scatter::{
    dfs_query_then_fetch_search, plan_search_scatter, plan_search_scatter_for_group, plan_search_scatter_with_version_floor, SearchRequest, NodeClient,
};
use miroir_core::session_pinning::WaitStrategy;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, info_span, instrument, warn};

use crate::middleware::SessionId;
use crate::routes::admin_endpoints::{AppState, parse_rate_limit};

/// Metrics observer for replica selection events.
///
/// Reports selection scores and exploration events to Prometheus.
struct MetricsObserver {
    metrics: crate::middleware::Metrics,
}

impl SelectionObserver for MetricsObserver {
    fn report_selection(&self, node_id: &str, score: f64) {
        self.metrics.set_replica_selection_score(node_id, score);
    }

    fn report_exploration(&self) {
        self.metrics.inc_replica_selection_exploration();
    }
}

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
    replica_selector: Option<Arc<miroir_core::replica_selection::ReplicaSelector>>,
}

impl ProxyNodeClient {
    pub fn new(
        client: Arc<crate::client::HttpClient>,
        metrics: crate::middleware::Metrics,
        replica_selector: Option<Arc<miroir_core::replica_selection::ReplicaSelector>>,
    ) -> Self {
        Self { client, metrics, replica_selector }
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
///
/// Session pinning (plan §13.6): If `X-Miroir-Session` header is present and
/// the session has a pending write, routes to the pinned group for read-your-writes.
#[instrument(skip_all, fields(index = %index))]
async fn search_handler(
    Path(index): Path<String>,
    Extension(state): Extension<Arc<AppState>>,
    headers: HeaderMap,
    session_id: Option<Extension<SessionId>>,
    Json(body): Json<SearchRequestBody>,
) -> Result<Response, StatusCode> {
    let start = Instant::now();
    let client_requested_score = body.ranking_score.unwrap_or(false);

    // Extract session ID from request extensions (set by session_pinning_middleware)
    let sid = session_id.and_then(|ext| {
        let s = ext.0;
        if s.0.is_empty() { None } else { Some(s.0.clone()) }
    });

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

    // Resolve alias to concrete index UID (plan §13.7)
    // Aliases are resolved at routing time; resolved index is used for scatter planning
    let (effective_index, resolved_targets) = if state.config.aliases.enabled {
        let targets = state.alias_registry.resolve(&index).await;
        state.metrics.inc_alias_resolution(&index);
        if targets != vec![index.clone()] {
            // It's an alias
            (index.clone(), targets)
        } else {
            // Not an alias - use index directly
            (index.clone(), vec![index.clone()])
        }
    } else {
        // Aliases disabled - use index directly
        (index.clone(), vec![index.clone()])
    };

    // Session pinning logic (plan §13.6): Check if session has pending write
    let (pinned_group, strategy_label) = if let Some(ref sid) = sid {
        if let Some(group) = state.session_manager.get_pinned_group(sid).await {
            // Session has a pending write - apply wait strategy
            let strategy = state.session_manager.wait_strategy();
            match strategy {
                WaitStrategy::Block => {
                    // Block until write completes or timeout
                    let max_wait = state.session_manager.max_wait_duration();
                    let wait_start = std::time::Instant::now();
                    match state.session_manager.wait_for_write_completion(
                        sid,
                        &state.task_registry,
                        max_wait,
                    ).await {
                        Ok(true) => {
                            // Write succeeded, clear pin and use normal routing
                            let wait_duration = wait_start.elapsed().as_secs_f64();
                            state.metrics.observe_session_wait_duration(wait_duration);
                            state.metrics.inc_session_pin_enforced("block");
                            (None, Some("block_success"))
                        }
                        Ok(false) => {
                            // Write failed, clear pin
                            let wait_duration = wait_start.elapsed().as_secs_f64();
                            state.metrics.observe_session_wait_duration(wait_duration);
                            state.metrics.inc_session_wait_timeout("block");
                            (None, Some("block_failed"))
                        }
                        Err(_) => {
                            // Timeout - still route to pinned group (best effort)
                            let wait_duration = wait_start.elapsed().as_secs_f64();
                            state.metrics.observe_session_wait_duration(wait_duration);
                            state.metrics.inc_session_wait_timeout("block");
                            (Some(group), Some("block_timeout"))
                        }
                    }
                }
                WaitStrategy::RoutePin => {
                    // Route to pinned group without waiting
                    state.metrics.inc_session_pin_enforced("route_pin");
                    (Some(group), Some("route_pin"))
                }
            }
        } else {
            // No pending write - normal routing
            (None, None)
        }
    } else {
        // No session header - normal routing
        (None, None)
    };

    // Log session pinning decision for observability
    if let Some(label) = strategy_label {
        debug!(
            session_id = ?sid,
            pinned_group = pinned_group,
            strategy = label,
            "session pinning applied"
        );
    }

    // Query coalescing (plan §13.10): Check for identical in-flight queries
    // Skip for multi-target aliases (each target is different)
    if state.config.query_coalescing.enabled && resolved_targets.len() == 1 {
        // Build fingerprint from canonicalized query body + index + settings version
        let settings_version = state.settings_broadcast.current_version().await;
        let query_body = serde_json::to_value(&body).unwrap_or(Value::Null);
        let fingerprint = QueryFingerprint::new(effective_index.clone(), &query_body, settings_version);

        // Try to coalesce with an existing in-flight query
        if let Some(mut rx) = state.query_coalescer.try_coalesce(fingerprint.clone()).await {
            // Successfully subscribed to an in-flight query - wait for its result
            state.metrics.inc_query_coalesce_hits();
            debug!(
                index = %effective_index,
                "query coalesced: waiting for in-flight query result"
            );

            // Wait for the response (with timeout matching the scatter timeout)
            let timeout = Duration::from_millis(state.config.scatter.node_timeout_ms);
            let response_bytes = tokio::time::timeout(timeout, async move {
                rx.recv().await
            }).await;

            match response_bytes {
                Ok(Ok(response_bytes)) => {
                    // Received response from coalesced query
                    let response_body: Value = match serde_json::from_slice(&response_bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            error!(error = %e, "failed to deserialize coalesced query response");
                            return Err(StatusCode::INTERNAL_SERVER_ERROR);
                        }
                    };

                    // Build response with appropriate headers
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json");

                    // Add settings headers
                    if state.settings_broadcast.is_in_flight(&effective_index).await {
                        response = response.header("X-Miroir-Settings-Inconsistent", "true");
                    }
                    if settings_version > 0 {
                        response = response.header("X-Miroir-Settings-Version", settings_version.to_string());
                    }

                    let response = response
                        .body(axum::body::Body::from(serde_json::to_string(&response_body).unwrap()))
                        .unwrap();

                    tracing::info!(
                        target: "miroir.search_coalesced",
                        index = %effective_index,
                        duration_ms = start.elapsed().as_millis() as u64,
                        "coalesced search completed"
                    );

                    return Ok(response);
                }
                Ok(Err(_)) => {
                    // Channel closed without receiving response - proceed with normal scatter
                    debug!("coalesced query channel closed, proceeding with normal scatter");
                }
                Err(_) => {
                    // Timeout waiting for coalesced query - proceed with normal scatter
                    debug!("timeout waiting for coalesced query, proceeding with normal scatter");
                }
            }
        }
    }

    // Handle multi-target alias fanout (plan §13.7, §13.11, §13.17)
    // Multi-target aliases (ILM read_alias) require fanning out to all targets
    // and merging results by _rankingScore
    if resolved_targets.len() > 1 {
        return search_multi_targets(
            resolved_targets,
            body,
            state,
            headers,
            sid,
            client_requested_score,
        ).await;
    }

    // Get the scoped key for this index (plan §13.21).
    // If a scoped key exists, use primary_key (or previous_key during rotation overlap).
    // If no scoped key exists yet, fall back to node_master_key for initial setup.
    let search_key = if let Some(ref redis) = state.redis_store {
        if let Ok(Some(sk)) = redis.get_search_ui_scoped_key(&effective_index) {
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
            pinned_group = ?pinned_group,
            strategy = %state.config.replica_selection.strategy,
        ).entered();

        // Determine if we should use adaptive selection
        let use_adaptive = state.config.replica_selection.strategy == "adaptive";
        let replica_selector_ref = if use_adaptive {
            Some(state.replica_selector.as_ref())
        } else {
            None
        };

        // Session pinning: if pinned_group is set, use group-specific planning
        if let Some(group) = pinned_group {
            match plan_search_scatter_for_group(
                &topo,
                0,
                state.config.replication_factor as usize,
                state.config.shards,
                group,
                replica_selector_ref,
            ).await {
                Some(p) => p,
                None => {
                    // Pinned group not available - fall back to normal planning
                    // This can happen if the pinned group has failed
                    warn!(
                        pinned_group = group,
                        "pinned group unavailable, falling back to normal routing"
                    );
                    plan_search_scatter(&topo, 0, state.config.replication_factor as usize, state.config.shards, replica_selector_ref).await
                }
            }
        } else if let Some(floor) = min_settings_version {
            // If client provided a min settings version floor, use version-filtered planning
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
                replica_selector_ref,
            ).await;

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
            plan_search_scatter(&topo, 0, state.config.replication_factor as usize, state.config.shards, replica_selector_ref).await
        }
    };
    let node_count = plan.shard_to_node.len() as u64;

    // Record scatter fan-out size before executing
    state.metrics.record_scatter_fan_out(node_count);

    // Build search request
    let search_req = SearchRequest {
        index_uid: effective_index.clone(),
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

    // Register for query coalescing (plan §13.10) - after try_coalesce, before scatter
    // Only register if coalescing is enabled and this is a single-target query
    let (tx, fingerprint) = if state.config.query_coalescing.enabled && resolved_targets.len() == 1 {
        let settings_version = state.settings_broadcast.current_version().await;
        let query_body = serde_json::to_value(&body).unwrap_or(Value::Null);
        let fp = QueryFingerprint::new(effective_index.clone(), &query_body, settings_version);

        match state.query_coalescer.register(fp.clone()).await {
            Ok(broadcast_tx) => {
                state.metrics.inc_query_coalesce_subscribers(1); // First subscriber = the query itself
                (Some(broadcast_tx), Some(fp))
            }
            Err(_) => {
                // Failed to register (too many pending queries) - proceed without coalescing
                debug!("too many pending queries, proceeding without coalescing registration");
                (None, None)
            }
        }
    } else {
        (None, None)
    };

    // Execute DFS query-then-fetch
    let mut result = dfs_query_then_fetch_search(
        plan,
        &client,
        search_req,
        &topo,
        policy,
        &strategy,
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
    let response_body = serde_json::json!({
        "hits": result.hits,
        "estimatedTotalHits": result.estimated_total_hits,
        "processingTimeMs": result.processing_time_ms,
    });

    // Only include facetDistribution if facets were requested
    let mut body = response_body.clone();
    if let Some(facets) = &result.facet_distribution {
        body["facetDistribution"] = serde_json::to_value(facets).unwrap_or(Value::Null);
    }

    // Broadcast result to coalesced queries (plan §13.10)
    if let (Some(broadcast_tx), Some(fp)) = (tx, fingerprint) {
        let response_bytes = serde_json::to_vec(&body).unwrap_or_default();
        let subscriber_count = broadcast_tx.receiver_count();
        if subscriber_count > 1 {
            // There are other queries waiting for this result - broadcast to them
            state.metrics.inc_query_coalesce_subscribers(subscriber_count as u64 - 1);
            let _ = broadcast_tx.send(response_bytes.clone());
            debug!(
                index = %effective_index,
                subscribers = subscriber_count - 1,
                "broadcast search result to coalesced queries"
            );
        }
        // Unregister the query after broadcasting
        state.query_coalescer.unregister(&fp).await;
    }

    // Build response with optional headers
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json");

    // Add X-Miroir-Settings-Inconsistent header if a broadcast is in flight (plan §13.5)
    if state.settings_broadcast.is_in_flight(&effective_index).await {
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
        index = %effective_index,
        duration_ms = start.elapsed().as_millis() as u64,
        node_count = node_count,
        estimated_hits = result.estimated_total_hits,
        degraded = result.degraded,
        "search completed"
    );

    Ok(response)
}

/// Search multiple target indexes (for multi-target aliases, plan §13.7, §13.11, §13.17).
///
/// Fans out the search to all target indexes and merges results by _rankingScore.
/// Used by ILM read_alias queries.
async fn search_multi_targets(
    targets: Vec<String>,
    body: SearchRequestBody,
    state: Arc<AppState>,
    headers: HeaderMap,
    session_id: Option<String>,
    client_requested_score: bool,
) -> Result<Response, StatusCode> {
    let start = Instant::now();

    // Extract session ID if provided
    let sid = session_id;

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

    // Session pinning logic (plan §13.6): Check if session has pending write
    let (pinned_group, _strategy_label) = if let Some(ref sid) = sid {
        if let Some(group) = state.session_manager.get_pinned_group(sid).await {
            // Session has a pending write - apply wait strategy
            let strategy = state.session_manager.wait_strategy();
            match strategy {
                WaitStrategy::Block => {
                    // Block until write completes or timeout
                    let max_wait = state.session_manager.max_wait_duration();
                    let wait_start = std::time::Instant::now();
                    match state.session_manager.wait_for_write_completion(
                        sid,
                        &state.task_registry,
                        max_wait,
                    ).await {
                        Ok(true) => {
                            // Write succeeded, clear pin and use normal routing
                            let wait_duration = wait_start.elapsed().as_secs_f64();
                            state.metrics.observe_session_wait_duration(wait_duration);
                            state.metrics.inc_session_pin_enforced("block");
                            (None::<u32>, None::<u64>)
                        }
                        Ok(false) => {
                            // Write failed, clear pin
                            let wait_duration = wait_start.elapsed().as_secs_f64();
                            state.metrics.observe_session_wait_duration(wait_duration);
                            state.metrics.inc_session_wait_timeout("block");
                            (None::<u32>, None::<u64>)
                        }
                        Err(_) => {
                            // Timeout - route to pinned group (best effort)
                            let wait_duration = wait_start.elapsed().as_secs_f64();
                            state.metrics.observe_session_wait_duration(wait_duration);
                            state.metrics.inc_session_wait_timeout("block");
                            (Some(group), None::<u64>)
                        }
                    }
                }
                WaitStrategy::RoutePin => {
                    // Route to pinned group without waiting
                    state.metrics.inc_session_pin_enforced("route_pin");
                    (Some(group), None::<u64>)
                }
            }
        } else {
            // No pending write - normal routing
            (None::<u32>, None::<u64>)
        }
    } else {
        // No session header - normal routing
        (None::<u32>, None::<u64>)
    };

    // For multi-target aliases, we use the first target for settings/scoped key
    // All targets should have compatible settings (managed by ILM)
    let primary_target = targets.first().ok_or_else(|| {
        error!("multi-target alias has no targets");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Get the scoped key for the primary target
    let search_key = if let Some(ref redis) = state.redis_store {
        if let Ok(Some(sk)) = redis.get_search_ui_scoped_key(primary_target) {
            // Refresh scoped-key beacon
            let _ = redis.observe_search_ui_scoped_key(
                &state.pod_id,
                primary_target,
                sk.generation,
            );
            sk.primary_key
        } else {
            state.config.node_master_key.clone()
        }
    } else {
        state.config.node_master_key.clone()
    };

    // Extract X-Miroir-Min-Settings-Version header
    let min_settings_version = headers
        .get("X-Miroir-Min-Settings-Version")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    // Use live topology from shared state
    let topo = state.topology.read().await;
    let policy = match state.config.scatter.unavailable_shard_policy.as_str() {
        "partial" => UnavailableShardPolicy::Partial,
        "error" => UnavailableShardPolicy::Error,
        "fallback" => UnavailableShardPolicy::Fallback,
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // Plan scatter for primary target (for ILM read aliases, all targets
    // should have identical shard layout)
    let plan = {
        let _plan_span = tracing::info_span!(
            "scatter_plan_multi_target",
            primary_target = %primary_target,
            replica_groups = state.config.replica_groups,
            shards = state.config.shards,
            rf = state.config.replication_factor,
            min_settings_version,
            pinned_group = ?pinned_group,
            target_count = targets.len(),
        ).entered();

        if let Some(group) = pinned_group {
            match plan_search_scatter_for_group(
                &topo,
                0,
                state.config.replication_factor as usize,
                state.config.shards,
                group,
            ) {
                Some(p) => p,
                None => {
                    warn!(
                        pinned_group = group,
                        "pinned group unavailable, falling back to normal routing"
                    );
                    plan_search_scatter(&topo, 0, state.config.replication_factor as usize, state.config.shards)
                }
            }
        } else if let Some(floor) = min_settings_version {
            let settings_broadcast = state.settings_broadcast.clone();
            let plan_result = plan_search_scatter_with_version_floor(
                &topo,
                0,
                state.config.replication_factor as usize,
                state.config.shards,
                primary_target,
                floor,
                &move |idx, node_id| {
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
                    let err = MeilisearchError::new(
                        MiroirCode::SettingsVersionStale,
                        format!(
                            "no covering set available for settings version floor {} on index '{}'",
                            floor, primary_target
                        ),
                    );
                    return Err(StatusCode::SERVICE_UNAVAILABLE);
                }
            }
        } else {
            plan_search_scatter(&topo, 0, state.config.replication_factor as usize, state.config.shards)
        }
    };
    let node_count = plan.shard_to_node.len() as u64;

    // Record scatter fan-out size
    state.metrics.record_scatter_fan_out(node_count);

    // Build search request for primary target
    let search_req = SearchRequest {
        index_uid: primary_target.to_string(),
        query: body.q.clone(),
        offset: body.offset.unwrap_or(0),
        limit: body.limit.unwrap_or(20),
        filter: body.filter.clone(),
        facets: body.facets.clone(),
        ranking_score: client_requested_score,
        body: body.rest.clone(),
        global_idf: None,
    };

    // Create node client
    let http_client = Arc::new(crate::client::HttpClient::new(
        search_key,
        state.config.scatter.node_timeout_ms,
    ));
    let client = ProxyNodeClient::new(http_client, state.metrics.clone());

    // Use score-based merge strategy
    let strategy = ScoreMergeStrategy::new();

    // Execute search
    let mut result = dfs_query_then_fetch_search(
        plan,
        &client,
        search_req,
        &topo,
        policy,
        &strategy,
    )
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "multi-target search failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Drop topology lock before building response
    drop(topo);

    // Strip internal fields from hits
    for hit in &mut result.hits {
        strip_internal_fields(hit, client_requested_score);
    }

    // Build response body
    let mut response_body = serde_json::json!({
        "hits": result.hits,
        "estimatedTotalHits": result.estimated_total_hits,
        "processingTimeMs": result.processing_time_ms,
    });

    // Only include facetDistribution if facets were requested
    if let Some(facets) = &result.facet_distribution {
        response_body["facetDistribution"] = serde_json::to_value(facets).unwrap_or(Value::Null);
    }

    // Build response with optional headers
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json");

    // Add settings headers
    if state.settings_broadcast.is_in_flight(primary_target).await {
        response = response.header("X-Miroir-Settings-Inconsistent", "true");
    }

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
        .body(axum::body::Body::from(serde_json::to_string(&response_body).unwrap()))
        .unwrap();

    // Structured log entry
    tracing::info!(
        target: "miroir.search_multi_target",
        primary_target = %primary_target,
        target_count = targets.len(),
        duration_ms = start.elapsed().as_millis() as u64,
        node_count = node_count,
        estimated_hits = result.estimated_total_hits,
        degraded = result.degraded,
        "multi-target search completed"
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
