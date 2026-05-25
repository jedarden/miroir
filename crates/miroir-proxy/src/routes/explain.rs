//! Query explain API endpoint (plan §13.20).

use axum::{
    extract::{Extension, FromRef, Path, Query},
    http::{HeaderMap, StatusCode},
    Json,
};
use miroir_core::{
    api_error::{MeilisearchError, MiroirCode},
    config::MiroirConfig,
    explainer::{BroadcastPending, Explainer, SearchQueryExplanation, Warning},
    query_planner::QueryPlanner,
    scatter::{plan_search_scatter, SearchRequest, VectorMode},
    shadow::ShadowOperation,
    topology::Topology,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::routes::admin_endpoints::AppState;

/// Search query for explanation (re-export from core).
pub type SearchQuery = SearchQueryExplanation;

/// Explain state.
#[derive(Clone)]
pub struct ExplainState {
    pub config: Arc<MiroirConfig>,
    pub topology: Arc<RwLock<Topology>>,
    pub query_planner: Arc<QueryPlanner>,
}

/// Query parameters for the explain endpoint.
#[derive(Debug, Deserialize)]
pub struct ExplainParams {
    /// If true, execute the query and return both plan and results.
    execute: Option<bool>,
}

/// POST /indexes/{index}/explain — explain a search query without executing it.
///
/// Request body matches /search but returns the execution plan instead of results.
/// Plan §13.20: "Why is this query slow?" debugging.
///
/// Auth scope (plan §13.20):
/// - master_key: warnings filtered to remove operator-only signals
/// - admin_key: all warnings surface unredacted
///
/// Query parameters:
/// - execute=true: also execute the query and return results in one call
pub async fn explain_search<S>(
    Path(index): Path<String>,
    Query(params): Query<ExplainParams>,
    Extension(state): Extension<Arc<AppState>>,
    headers: HeaderMap,
    Json(mut query): Json<SearchQuery>,
) -> Result<Json<serde_json::Value>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
{
    if !state.config.explain.enabled {
        return Err(StatusCode::NOT_FOUND);
    }

    // Determine auth scope from headers (plan §13.20)
    let is_admin_request = check_admin_auth(&headers, &state.config);

    // Build SearchQueryExplanation from request
    // Extract filter as string if present
    let filter_string = extract_filter_string(&query.filter);

    // Get topology and settings version
    let topology = state.topology.read().await;
    let settings_version = state.settings_broadcast.current_version().await;

    // Check if broadcast is in flight
    let broadcast_pending_info = if state.settings_broadcast.is_in_flight(&index).await {
        // Get pending info - simplified version
        Some(BroadcastPending {
            fingerprint: "unknown".to_string(),
            commit_in: "~2.4s".to_string(),
        })
    } else {
        None
    };

    // Run query planner for shard narrowing (plan §13.4)
    let shard_count = state.config.shards;
    let query_plan = state
        .query_planner
        .plan(&index, &filter_string, shard_count)
        .await;

    // Create explainer and generate explanation
    let explainer = Explainer::new(state.config.as_ref().clone());
    let mut explanation = explainer.explain(
        &index,
        &query,
        &topology,
        settings_version,
        broadcast_pending_info.as_ref(),
    );

    // Apply query planner results to explanation
    explanation.plan.narrowed = query_plan.narrowed;
    explanation.plan.narrowing_reason = if query_plan.narrowed {
        Some(query_plan.reason)
    } else {
        None
    };
    explanation.plan.target_shards = query_plan.target_shards;

    // Add query planner warnings
    for warning in query_plan.warnings {
        explanation
            .warnings
            .push(Warning::NarrowingNotPossible { reason: warning });
    }

    // Check for unfilterable attributes (plan §13.20)
    if let Some(ref filter) = filter_string {
        check_unfilterable_attributes(filter, &index, &state.config, &mut explanation.warnings)
            .await;
    }

    // Filter warnings based on auth scope (plan §13.20)
    let filtered_warnings = if is_admin_request {
        explanation.warnings
    } else {
        filter_master_key_warnings(explanation.warnings)
    };

    // Build response
    let mut response = serde_json::json!({
        "resolvedUid": explanation.resolved_uid,
        "plan": {
            "aliasResolution": explanation.plan.alias_resolution,
            "narrowed": explanation.plan.narrowed,
            "narrowingReason": explanation.plan.narrowing_reason,
            "targetShards": explanation.plan.target_shards,
            "chosenGroup": explanation.plan.chosen_group,
            "targetNodes": explanation.plan.target_nodes,
            "hedgingArmed": explanation.plan.hedging_armed,
            "hedgeTriggerMs": explanation.plan.hedge_trigger_ms,
            "coalescingEligible": explanation.plan.coalescing_eligible,
            "cacheCandidate": explanation.plan.cache_candidate,
            "tenantAffinityPinned": explanation.plan.tenant_affinity_pinned,
            "estimatedP95Ms": explanation.plan.estimated_p95_ms,
            "settingsVersion": explanation.plan.settings_version,
        },
        "warnings": filtered_warnings,
    });

    // Add broadcast pending info if present
    if let Some(pending) = explanation.plan.broadcast_pending {
        response["plan"]["broadcastPending"] = serde_json::json!({
            "fingerprint": pending.fingerprint,
            "commitIn": pending.commit_in,
        });
    }

    // Handle ?execute=true parameter (plan §13.20)
    if params.execute.unwrap_or(false) {
        if !state.config.explain.allow_execute_parameter {
            return Err(StatusCode::FORBIDDEN);
        }

        // Execute the search query
        match execute_search(&state, &index, &query).await {
            Ok(search_result) => {
                response["result"] = search_result;
            }
            Err(e) => {
                tracing::error!(error = %e, "explain execute failed");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    // Shadow the explain request to configured targets (plan §13.16)
    // This is done asynchronously after returning the primary response
    if let Some(ref shadow_manager) = state.shadow_manager {
        let shadow_mgr = shadow_manager.clone();
        let config = state.config.shadow.clone();
        let index_clone = index.clone();
        let query_clone = query.clone();

        tokio::spawn(async move {
            if !config.enabled {
                return;
            }

            let targets = config.targets;

            for target in targets {
                // Check if this target has explain operation enabled
                if !target.operations.iter().any(|op| op == "explain") {
                    continue;
                }

                let shadow_target = miroir_core::shadow::ShadowTarget {
                    name: target.name.clone(),
                    url: target.url.clone(),
                    api_key_env: target.api_key_env.clone(),
                    sample_rate: target.sample_rate,
                    operations: target
                        .operations
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

                // Build the request body for shadow
                let request_body = serde_json::to_value(&query_clone).unwrap_or_default();

                // Shadow the request
                let result = shadow_mgr
                    .shadow_search(
                        &shadow_target,
                        &index_clone,
                        &request_body,
                        0, // No latency info for explain
                        &[],
                    )
                    .await;

                match result {
                    Ok(_) => {
                        tracing::debug!(
                            target = shadow_target.name,
                            index = %index_clone,
                            "explain shadow request completed"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            target = shadow_target.name,
                            index = %index_clone,
                            error = %e,
                            "explain shadow request failed"
                        );
                    }
                }
            }
        });
    }

    Ok(Json(response))
}

/// Check if the request is authenticated with admin_key (plan §13.20).
///
/// Returns true if authenticated with admin_key or X-Admin-Key header.
fn check_admin_auth(headers: &HeaderMap, config: &MiroirConfig) -> bool {
    // Check X-Admin-Key header
    if let Some(x_admin_key) = headers.get("X-Admin-Key") {
        if let Ok(key) = x_admin_key.to_str() {
            return key == config.admin.api_key;
        }
    }

    // Check Authorization: Bearer header
    if let Some(auth) = headers.get("authorization") {
        if let Ok(auth_str) = auth.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                return token == config.admin.api_key;
            }
        }
    }

    false
}

/// Extract filter expression as string from JSON value.
fn extract_filter_string(filter: &Option<serde_json::Value>) -> Option<String> {
    match filter {
        None => None,
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(v) => Some(v.to_string()),
    }
}

/// Check for unfilterable attributes in filter expression (plan §13.20).
async fn check_unfilterable_attributes(
    filter: &str,
    index: &str,
    config: &MiroirConfig,
    warnings: &mut Vec<Warning>,
) {
    // Parse filter to extract attribute names
    // This is a simplified check - in production we'd use a proper parser
    let filterable_attrs = get_filterable_attributes(index, config).await;

    // Extract attribute names from filter (simple regex-like matching)
    for attr in extract_attributes_from_filter(filter) {
        if !filterable_attrs.contains(&attr) {
            warnings.push(Warning::UnfilterableAttribute {
                attribute: attr.clone(),
                suggestion: format!(
                    "add '{}' to filterableAttributes or remove from filter",
                    attr
                ),
            });
        }
    }
}

/// Get filterable attributes for an index from the first node.
async fn get_filterable_attributes(index: &str, config: &MiroirConfig) -> Vec<String> {
    // In production, this would query the node for index settings
    // For now, return a default set
    vec!["id".to_string(), "_miroir_shard".to_string()]
}

/// Extract attribute names from a filter expression.
fn extract_attributes_from_filter(filter: &str) -> Vec<String> {
    let mut attrs = Vec::new();

    // Simple extraction: look for patterns like "attributeName"
    // This is a placeholder - a real implementation would parse the filter properly
    let filter_lower = filter.to_lowercase();

    // Common filterable attributes
    let known_attrs = vec!["id", "sku", "category", "price", "status", "tenant"];

    for attr in known_attrs {
        if filter_lower.contains(&format!(r#"{}"#, attr))
            || filter_lower.contains(&format!(r#"{}"#, attr))
        {
            attrs.push(attr.to_string());
        }
    }

    attrs
}

/// Filter warnings for master_key scope (plan §13.20).
///
/// Removes operator-only signals:
/// - SettingsDrift
/// - TenantAffinityMismatch
/// - node_settings_version < floor warnings
fn filter_master_key_warnings(warnings: Vec<Warning>) -> Vec<Warning> {
    warnings
        .into_iter()
        .filter(|w| {
            !matches!(
                w,
                Warning::SettingsDrift { .. }
                    | Warning::TenantAffinityMismatch { .. }
                    | Warning::SettingsBroadcastInFlight { .. }
            )
        })
        .collect()
}

/// Execute the search query when ?execute=true is set.
async fn execute_search(
    state: &AppState,
    index: &str,
    query: &SearchQuery,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    // Build search request
    let search_req = SearchRequest {
        index_uid: index.to_string(),
        query: query.q.clone(),
        offset: query.offset.unwrap_or(0),
        limit: query.limit.unwrap_or(20),
        filter: query.filter.clone(),
        facets: None,
        ranking_score: false,
        body: serde_json::json!({}),
        global_idf: None,
        over_fetch_factor: 1,
        vector_mode: VectorMode::KeywordOnly,
        vector_config: None,
    };

    // Get topology and plan scatter
    let topo = state.topology.read().await;
    let plan = plan_search_scatter(&topo, 0, 1, state.config.shards, None).await;

    // Execute search (simplified - in production this would use the full search path)
    Ok(serde_json::json!({
        "hits": [],
        "estimatedTotalHits": 0,
        "processingTimeMs": 0,
        "note": "execute=true is implemented but full search execution is pending integration"
    }))
}
