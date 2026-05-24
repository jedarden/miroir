//! Search route handler with DFS (Distributed Frequency Search) support.

use axum::body::Body;
use axum::extract::{Extension, Path};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::Json;
use miroir_core::api_error::{MeilisearchError, MiroirCode};
use miroir_core::config::UnavailableShardPolicy;
use miroir_core::idempotency::QueryFingerprint;
use miroir_core::merger::ScoreMergeStrategy;
use miroir_core::replica_selection::SelectionObserver;
use miroir_core::scatter::{
    dfs_query_then_fetch_search, plan_search_scatter, plan_search_scatter_for_group,
    plan_search_scatter_with_version_floor, NodeClient, SearchRequest, VectorMode,
};
use miroir_core::session_pinning::WaitStrategy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

use crate::routes::admin_endpoints::{parse_rate_limit, AppState};

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
        Self {
            client,
            metrics,
            replica_selector,
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

pub fn router<S>() -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    axum::Router::new().route("/:index", axum::routing::post(search_handler))
}

/// Search request body.
#[derive(Deserialize, Serialize)]
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
#[tracing::instrument(skip(state, headers, body))]
async fn search_handler(
    Path(index): Path<String>,
    Extension(state): Extension<Arc<AppState>>,
    session_id: Option<Extension<crate::middleware::SessionId>>,
    jwt_claims: Option<Extension<crate::auth::JwtClaimsExtension>>,
    headers: HeaderMap,
    Json(body): Json<SearchRequestBody>,
) -> Response<Body> {
    let start = Instant::now();
    let client_requested_score = body.ranking_score.unwrap_or(false);

    // Extract session ID from request extensions (set by session_pinning_middleware)
    let sid = session_id
        .map(|ext| ext.0)
        .filter(|s| !s.as_str().is_empty());

    // TODO: Extract source IP from headers - need to add back HeaderMap extraction
    let source_ip = "unknown".to_string();

    // Check rate limit for search UI (plan §4)
    let (limit, window_seconds) = match parse_rate_limit(&state.config.search_ui.rate_limit.per_ip)
    {
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
                        return Response::builder()
                            .status(StatusCode::TOO_MANY_REQUESTS)
                            .body(axum::body::Body::empty())
                            .unwrap();
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
        let (allowed, _wait_seconds) =
            state
                .local_search_ui_rate_limiter
                .check(&source_ip, limit, window_seconds * 1000);
        if !allowed {
            warn!(
                source_ip_hash = hash_for_log(&source_ip),
                "search UI rate limited (local backend)"
            );
            return Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .body(axum::body::Body::empty())
                .unwrap();
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
        if let Some(group) = state.session_manager.get_pinned_group(sid.as_str()).await {
            // Session has a pending write - apply wait strategy
            let strategy = state.session_manager.wait_strategy();
            match strategy {
                WaitStrategy::Block => {
                    // Block until write completes or timeout
                    let max_wait = state.session_manager.max_wait_duration();
                    let wait_start = std::time::Instant::now();
                    match state
                        .session_manager
                        .wait_for_write_completion(sid.as_str(), &state.task_registry, max_wait)
                        .await
                    {
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

    // Tenant affinity resolution (plan §13.15): Resolve tenant from headers
    // This happens after session pinning - session pin wins on conflict
    let (tenant_pinned_group, tenant_id) = if state.config.tenant_affinity.enabled {
        // Convert HeaderMap to HashMap for tenant resolution
        let mut headers_map = std::collections::HashMap::new();
        for (name, value) in headers.iter() {
            if let (Some(name_str), Ok(value_str)) = (name.as_str(), value.to_str()) {
                headers_map.insert(name_str.to_string(), value_str.to_string());
            }
        }

        // This is a read operation - writes always fan out to all groups
        match state
            .tenant_affinity_manager
            .resolve_from_headers(&headers_map, false)
            .await
        {
            Ok(resolution) => {
                let tid = resolution.tenant_id.clone();
                if let Some(group) = resolution.pinned_group {
                    // Record tenant affinity metrics
                    if let Some(ref tenant) = tid {
                        state
                            .metrics
                            .inc_tenant_queries(tenant, &group.to_string());
                        state
                            .metrics
                            .set_tenant_pinned_groups(tenant, group);
                    }
                    debug!(
                        tenant_id = ?tid,
                        pinned_group = group,
                        reason = %resolution.reason,
                        "tenant affinity applied"
                    );
                    (Some(group), tid)
                } else {
                    (None, tid)
                }
            }
            Err(e) => {
                // Tenant resolution failed (e.g., reject policy)
                warn!(error = %e, "tenant affinity resolution failed");
                return Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body(axum::body::Body::from(format!("Tenant not allowed: {e}")))
                    .unwrap();
            }
        }
    } else {
        (None, None)
    };

    // Combine session pin and tenant affinity: session pin wins on conflict (plan §13.15)
    let effective_pinned_group = if pinned_group.is_some() {
        if tenant_pinned_group.is_some() {
            // Record override metric
            state.metrics.inc_tenant_session_pin_override();
        }
        pinned_group
    } else {
        tenant_pinned_group
    };

    // Query coalescing (plan §13.10): Check for identical in-flight queries
    // Skip for multi-target aliases (each target is different)
    if state.config.query_coalescing.enabled && resolved_targets.len() == 1 {
        // Build fingerprint from canonicalized query body + index + settings version
        let settings_version = state.settings_broadcast.current_version().await;
        let query_json = serde_json::to_string(&body).unwrap_or_default();
        let fingerprint = QueryFingerprint {
            index: effective_index.clone(),
            query_json,
            settings_version,
        };

        // Try to coalesce with an existing in-flight query
        if let Some(mut rx) = state
            .query_coalescer
            .try_coalesce(fingerprint.clone())
            .await
        {
            // Successfully subscribed to an in-flight query - wait for its result
            state.metrics.inc_query_coalesce_hits();
            debug!(
                index = %effective_index,
                "query coalesced: waiting for in-flight query result"
            );

            // Wait for the response (with timeout matching the scatter timeout)
            let timeout = Duration::from_millis(state.config.scatter.node_timeout_ms);
            let response_bytes =
                tokio::time::timeout(timeout, async move { rx.recv().await }).await;

            match response_bytes {
                Ok(Ok(response_bytes)) => {
                    // Received response from coalesced query
                    let response_body: Value = match serde_json::from_slice(&response_bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            error!(error = %e, "failed to deserialize coalesced query response");
                            return Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body(axum::body::Body::empty())
                                .unwrap();
                        }
                    };

                    // Build response with appropriate headers
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json");

                    // Add settings headers
                    if state
                        .settings_broadcast
                        .is_in_flight(&effective_index)
                        .await
                    {
                        response = response.header("X-Miroir-Settings-Inconsistent", "true");
                    }
                    if settings_version > 0 {
                        response = response
                            .header("X-Miroir-Settings-Version", settings_version.to_string());
                    }

                    let response = response
                        .body(axum::body::Body::from(
                            serde_json::to_string(&response_body).unwrap(),
                        ))
                        .unwrap();

                    tracing::info!(
                        target: "miroir.search_coalesced",
                        index = %effective_index,
                        duration_ms = start.elapsed().as_millis() as u64,
                        "coalesced search completed"
                    );

                    return response;
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

    // Extract X-Miroir-Min-Settings-Version header (plan §13.5)
    // Extract early for multi-target search path
    let min_settings_version = headers
        .get("X-Miroir-Min-Settings-Version")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    // Extract X-Miroir-Over-Fetch header (plan §13.12)
    // Per-request override of vector_search.over_fetch_factor
    let over_fetch_factor = headers
        .get("X-Miroir-Over-Fetch")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|&f| f >= 1)
        .unwrap_or_else(|| state.config.vector_search.over_fetch_factor);

    // Handle multi-target alias fanout (plan §13.7, §13.11, §13.17)
    // Multi-target aliases (ILM read_alias) require fanning out to all targets
    // and merging results by _rankingScore
    if resolved_targets.len() > 1 {
        // Need to create a new Extension wrapper for the nested call
        // Clone the Arc for the multi-target search
        return search_multi_targets(
            resolved_targets,
            body,
            Extension(state.clone()),
            sid.map(|s| s.as_str().to_string()),
            client_requested_score,
            min_settings_version,
        )
        .await;
    }

    // Get the scoped key for this index (plan §13.21).
    // If a scoped key exists, use primary_key (or previous_key during rotation overlap).
    // If no scoped key exists yet, fall back to node_master_key for initial setup.
    let search_key = if let Some(ref redis) = state.redis_store {
        if let Ok(Some(sk)) = redis.get_search_ui_scoped_key(&effective_index) {
            // Refresh scoped-key beacon so the rotation leader knows this pod is serving
            // requests for this index at the current generation (plan §13.21).
            let _ = redis.observe_search_ui_scoped_key(&state.pod_id, &index, sk.generation);

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
        _ => {
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(axum::body::Body::empty())
                .unwrap()
        }
    };

    // Plan scatter using live topology (span for plan construction)
    let plan = {
        // Determine if we should use adaptive selection
        let use_adaptive = state.config.replica_selection.strategy == "adaptive";
        let replica_selector_ref = if use_adaptive {
            Some(state.replica_selector.as_ref())
        } else {
            None
        };

        // Session pinning or tenant affinity: if effective_pinned_group is set, use group-specific planning
        if let Some(group) = effective_pinned_group {
            let _plan_span = tracing::info_span!(
                "scatter_plan",
                replica_groups = state.config.replica_groups,
                shards = state.config.shards,
                rf = state.config.replication_factor,
                min_settings_version,
                pinned_group = ?effective_pinned_group,
                tenant_id = ?tenant_id,
                strategy = %state.config.replica_selection.strategy,
            )
            .entered();
            drop(_plan_span); // Drop span before await
            match plan_search_scatter_for_group(
                &topo,
                0,
                state.config.replication_factor as usize,
                state.config.shards,
                group,
                replica_selector_ref,
            )
            .await
            {
                Some(p) => p,
                None => {
                    // Pinned group not available - fall back to normal planning
                    // This can happen if the pinned group has failed
                    warn!(
                        pinned_group = group,
                        "pinned group unavailable, falling back to normal routing"
                    );
                    plan_search_scatter(
                        &topo,
                        0,
                        state.config.replication_factor as usize,
                        state.config.shards,
                        replica_selector_ref,
                    )
                    .await
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
                        tokio::runtime::Handle::current()
                            .block_on(async { sb.node_version(&idx, &node_id).await })
                    })
                },
                replica_selector_ref,
            )
            .await;

            match plan_result {
                Some(p) => p,
                None => {
                    // No covering set could be assembled after filtering by version floor
                    let _err = MeilisearchError::new(
                        MiroirCode::SettingsVersionStale,
                        format!(
                            "no covering set available for settings version floor {} on index '{}'",
                            floor, index
                        ),
                    );
                    return Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(axum::body::Body::empty())
                        .unwrap();
                }
            }
        } else {
            // No version floor requested, use normal planning
            plan_search_scatter(
                &topo,
                0,
                state.config.replication_factor as usize,
                state.config.shards,
                replica_selector_ref,
            )
            .await
        }
    };
    let node_count = plan.shard_to_node.len() as u64;

    // Record scatter fan-out size before executing
    state.metrics.record_scatter_fan_out(node_count);

    // Apply filter injection from JWT claims (plan §13.21)
    // When a JWT has an injected_filter claim, AND it with any user-supplied filter
    let filter = if let Some(Extension(jwt_ext)) = jwt_claims {
        if let Some(ref injected_filter) = jwt_ext.0.injected_filter {
            // JWT has an injected filter - AND it with user-supplied filter
            match body.filter {
                None => Some(serde_json::from_str(injected_filter).unwrap_or_else(|_| {
                    // If parse fails, treat as string filter
                    serde_json::json!(injected_filter)
                })),
                Some(ref user_filter) => {
                    // Combine filters: (user_filter) AND (injected_filter)
                    // Meilisearch filter syntax: ["user_filter", "injected_filter"]
                    Some(serde_json::json!([user_filter, injected_filter]))
                }
            }
        } else {
            // No injected filter, use user-supplied filter as-is
            body.filter
        }
    } else {
        // No JWT claims, use user-supplied filter as-is
        body.filter
    };

    // Build search request
    // Clone facets for fingerprinting before moving into SearchRequest
    let facets_clone = body.facets.clone();
    let rest_body = body.rest.clone(); // Clone before body is partially moved

    // Detect vector search mode from request body (plan §13.12)
    let vector_mode = SearchRequest::detect_vector_mode(&rest_body);

    // Apply over-fetch factor for vector/hybrid queries (plan §13.12)
    let effective_over_fetch = if vector_mode != VectorMode::KeywordOnly {
        // Record over-fetch metric (plan §13.12)
        state.metrics.inc_vector_search_over_fetched();
        // Record merge strategy metric
        state
            .metrics
            .inc_vector_merge_strategy(&state.config.vector_search.merge_strategy);
        over_fetch_factor
    } else {
        1 // No over-fetch for pure keyword queries
    };

    let search_req = SearchRequest {
        index_uid: effective_index.clone(),
        query: body.q,
        offset: body.offset.unwrap_or(0),
        limit: body.limit.unwrap_or(20),
        filter,
        facets: body.facets,
        ranking_score: client_requested_score,
        body: rest_body,
        global_idf: None,
        over_fetch_factor: effective_over_fetch,
        vector_mode,
    };

    // Create node client with the scoped key (or node_master_key as fallback)
    let http_client = Arc::new(crate::client::HttpClient::new(
        search_key,
        state.config.scatter.node_timeout_ms,
    ));
    let client = ProxyNodeClient::new(http_client, state.metrics.clone(), None);

    // Use score-based merge strategy (OP#4: requires global IDF)
    let strategy = ScoreMergeStrategy::new();

    // Register for query coalescing (plan §13.10) - after try_coalesce, before scatter
    // Only register if coalescing is enabled and this is a single-target query
    let (tx, fingerprint) = if state.config.query_coalescing.enabled && resolved_targets.len() == 1
    {
        let settings_version = state.settings_broadcast.current_version().await;
        // Reconstruct body for fingerprinting (use cloned facets)
        let fingerprint_body = SearchRequestBody {
            q: search_req.query.clone(),
            offset: Some(search_req.offset),
            limit: Some(search_req.limit),
            filter: search_req.filter.clone(),
            facets: facets_clone,
            ranking_score: Some(search_req.ranking_score),
            rest: search_req.body.clone(),
        };
        let query_json = serde_json::to_string(&fingerprint_body).unwrap_or_default();
        let fp = QueryFingerprint {
            index: effective_index.clone(),
            query_json,
            settings_version,
        };

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
    let mut result = match dfs_query_then_fetch_search(
        plan, &client, search_req, &topo, policy, &strategy,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "search failed");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(axum::body::Body::empty())
                .unwrap();
        }
    };

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
            state
                .metrics
                .inc_query_coalesce_subscribers(subscriber_count as u64 - 1);
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
    if state
        .settings_broadcast
        .is_in_flight(&effective_index)
        .await
    {
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
        let shard_ids = sorted_shards
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        response = response.header("X-Miroir-Degraded", format!("shards={}", shard_ids));
    } else if result.degraded {
        response = response.header("X-Miroir-Degraded", "partial");
    }

    let response = response
        .body(axum::body::Body::from(
            serde_json::to_string(&body).unwrap(),
        ))
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

    response
}

/// Search multiple target indexes (for multi-target aliases, plan §13.7, §13.11, §13.17).
///
/// Fans out the search to all target indexes and merges results by _rankingScore.
/// Used by ILM read_alias queries.
async fn search_multi_targets(
    targets: Vec<String>,
    body: SearchRequestBody,
    Extension(state): Extension<Arc<AppState>>,
    session_id: Option<String>,
    client_requested_score: bool,
    min_settings_version: Option<u64>,
) -> Response<Body> {
    let start = Instant::now();

    // TODO: Extract source IP from headers
    let source_ip = "unknown".to_string();

    // Check rate limit for search UI (plan §4)
    let (limit, window_seconds) = match parse_rate_limit(&state.config.search_ui.rate_limit.per_ip)
    {
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
                        return Response::builder()
                            .status(StatusCode::TOO_MANY_REQUESTS)
                            .body(axum::body::Body::empty())
                            .unwrap();
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
        let (allowed, _wait_seconds) =
            state
                .local_search_ui_rate_limiter
                .check(&source_ip, limit, window_seconds * 1000);
        if !allowed {
            warn!(
                source_ip_hash = hash_for_log(&source_ip),
                "search UI rate limited (local backend)"
            );
            return Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .body(axum::body::Body::empty())
                .unwrap();
        }
    }

    // Session pinning logic (plan §13.6): Check if session has pending write
    let (pinned_group, _strategy_label) = if let Some(sid) = session_id {
        if let Some(group) = state.session_manager.get_pinned_group(sid.as_str()).await {
            // Session has a pending write - apply wait strategy
            let strategy = state.session_manager.wait_strategy();
            match strategy {
                WaitStrategy::Block => {
                    // Block until write completes or timeout
                    let max_wait = state.session_manager.max_wait_duration();
                    let wait_start = std::time::Instant::now();
                    match state
                        .session_manager
                        .wait_for_write_completion(sid.as_str(), &state.task_registry, max_wait)
                        .await
                    {
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
    let primary_target = match targets.first() {
        Some(t) => t,
        None => {
            error!("multi-target alias has no targets");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(axum::body::Body::empty())
                .unwrap();
        }
    };

    // Get the scoped key for the primary target
    let search_key = if let Some(ref redis) = state.redis_store {
        if let Ok(Some(sk)) = redis.get_search_ui_scoped_key(primary_target) {
            // Refresh scoped-key beacon
            let _ =
                redis.observe_search_ui_scoped_key(&state.pod_id, primary_target, sk.generation);
            sk.primary_key
        } else {
            state.config.node_master_key.clone()
        }
    } else {
        state.config.node_master_key.clone()
    };

    // Use live topology from shared state
    let topo = state.topology.read().await;
    let policy = match state.config.scatter.unavailable_shard_policy.as_str() {
        "partial" => UnavailableShardPolicy::Partial,
        "error" => UnavailableShardPolicy::Error,
        "fallback" => UnavailableShardPolicy::Fallback,
        _ => {
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(axum::body::Body::empty())
                .unwrap()
        }
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
        )
        .entered();
        drop(_plan_span); // Drop span before await

        if let Some(group) = pinned_group {
            match plan_search_scatter_for_group(
                &topo,
                0,
                state.config.replication_factor as usize,
                state.config.shards,
                group,
                None,
            )
            .await
            {
                Some(p) => p,
                None => {
                    warn!(
                        pinned_group = group,
                        "pinned group unavailable, falling back to normal routing"
                    );
                    plan_search_scatter(
                        &topo,
                        0,
                        state.config.replication_factor as usize,
                        state.config.shards,
                        None,
                    )
                    .await
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
                        tokio::runtime::Handle::current()
                            .block_on(async { sb.node_version(&idx, &node_id).await })
                    })
                },
                None,
            )
            .await;

            match plan_result {
                Some(p) => p,
                None => {
                    let _err = MeilisearchError::new(
                        MiroirCode::SettingsVersionStale,
                        format!(
                            "no covering set available for settings version floor {} on index '{}'",
                            floor, primary_target
                        ),
                    );
                    return Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(axum::body::Body::empty())
                        .unwrap();
                }
            }
        } else {
            plan_search_scatter(
                &topo,
                0,
                state.config.replication_factor as usize,
                state.config.shards,
                None,
            )
            .await
        }
    };
    let node_count = plan.shard_to_node.len() as u64;

    // Record scatter fan-out size
    state.metrics.record_scatter_fan_out(node_count);

    // Detect vector search mode from request body (plan §13.12)
    let vector_mode = SearchRequest::detect_vector_mode(&body.rest);

    // Apply over-fetch factor for vector/hybrid queries (plan §13.12)
    let over_fetch_factor = state.config.vector_search.over_fetch_factor;
    let effective_over_fetch = if vector_mode != VectorMode::KeywordOnly {
        // Record over-fetch metric (plan §13.12)
        state.metrics.inc_vector_search_over_fetched();
        // Record merge strategy metric
        state
            .metrics
            .inc_vector_merge_strategy(&state.config.vector_search.merge_strategy);
        over_fetch_factor
    } else {
        1 // No over-fetch for pure keyword queries
    };

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
        over_fetch_factor: effective_over_fetch,
        vector_mode,
    };

    // Create node client
    let http_client = Arc::new(crate::client::HttpClient::new(
        search_key,
        state.config.scatter.node_timeout_ms,
    ));
    let client = ProxyNodeClient::new(http_client, state.metrics.clone(), None);

    // Use score-based merge strategy
    let strategy = ScoreMergeStrategy::new();

    // Execute search
    let mut result = match dfs_query_then_fetch_search(
        plan, &client, search_req, &topo, policy, &strategy,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "multi-target search failed");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(axum::body::Body::empty())
                .unwrap();
        }
    };

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
        let shard_ids = sorted_shards
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        response = response.header("X-Miroir-Degraded", format!("shards={}", shard_ids));
    } else if result.degraded {
        response = response.header("X-Miroir-Degraded", "partial");
    }

    let response = response
        .body(axum::body::Body::from(
            serde_json::to_string(&response_body).unwrap(),
        ))
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

    response
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
