//! Settings routes: GET, PATCH, DELETE /indexes/:index/settings
//!
//! Implements settings broadcast per plan §3:
//! - Sequential apply-with-rollback on failure
//! - Broadcast to all nodes
//! - Rollback from successful nodes if any fail

use axum::{
    extract::{Path, State},
    response::{IntoResponse, Json, Response},
};
use miroir_core::{
    config::UnavailableShardPolicy,
    router::write_targets,
    scatter::{Scatter, ScatterRequest},
};
use serde_json::Value;

use crate::{
    error_response::ErrorResponse,
    scatter::HttpScatter,
    state::ProxyState,
};

/// Settings router.
pub fn router() -> axum::Router<ProxyState> {
    axum::Router::new()
        .route("/", axum::routing::get(get_all_settings))
        .route(
            "/filterable-attributes",
            axum::routing::get(get_filterable_attributes).put(update_filterable_attributes).delete(delete_filterable_attributes),
        )
        .route(
            "/searchable-attributes",
            axum::routing::get(get_searchable_attributes).put(update_searchable_attributes).delete(delete_searchable_attributes),
        )
        .route(
            "/sortable-attributes",
            axum::routing::get(get_sortable_attributes).put(update_sortable_attributes).delete(delete_sortable_attributes),
        )
        .route(
            "/displayed-attributes",
            axum::routing::get(get_displayed_attributes).put(update_displayed_attributes).delete(delete_displayed_attributes),
        )
        .route(
            "/ranking-rules",
            axum::routing::get(get_ranking_rules).put(update_ranking_rules).delete(delete_ranking_rules),
        )
        .route(
            "/stop-words",
            axum::routing::get(get_stop_words).put(update_stop_words).delete(delete_stop_words),
        )
        .route(
            "/synonyms",
            axum::routing::get(get_synonyms).put(update_synonyms).delete(delete_synonyms),
        )
        .route(
            "/distinct-attribute",
            axum::routing::get(get_distinct_attribute).put(update_distinct_attribute).delete(delete_distinct_attribute),
        )
        .route(
            "/typo-tolerance",
            axum::routing::get(get_typo_tolerance).put(update_typo_tolerance).delete(delete_typo_tolerance),
        )
        .route(
            "/faceting",
            axum::routing::get(get_faceting).put(update_faceting).delete(delete_faceting),
        )
        .route(
            "/pagination",
            axum::routing::get(get_pagination).put(update_pagination).delete(delete_pagination),
        )
}

/// GET /indexes/:index/settings - Get all settings.
async fn get_all_settings(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    let topology = state.topology().await;

    // Query first available node
    for group in topology.groups() {
        if let Some(node_id) = group.nodes().first() {
            let request = ScatterRequest {
                method: "GET".to_string(),
                path: format!("/indexes/{}/settings", index),
                body: vec![],
                headers: vec![],
            };

            let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);

            let result = scatter
                .scatter(&topology, vec![node_id.clone()], request, UnavailableShardPolicy::Partial)
                .await
                .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

            if let Some(resp) = result.responses.first() {
                if resp.status == 200 {
                    return Ok(Json(resp.body.clone()));
                } else if resp.status == 404 {
                    return Err(ErrorResponse::index_not_found(&index));
                }
            }
        }
    }

    Err(ErrorResponse::index_not_found(&index))
}

/// Cached original value for rollback.
struct RollbackValue {
    /// The original value to restore on rollback.
    original_value: Option<Value>,
    /// Whether the setting existed before.
    existed: bool,
}

/// Generic handler for updating a setting with rollback.
async fn update_setting_with_rollback(
    state: &ProxyState,
    index: &str,
    setting_path: &str,
    value: &Value,
) -> Result<Response, ErrorResponse> {
    let topology = state.topology().await;
    let targets = write_targets(0, &topology);

    if targets.is_empty() {
        return Err(ErrorResponse::internal_error("No nodes available"));
    }

    // Step 1: Fetch current values from all nodes for rollback
    let mut rollback_values: std::collections::HashMap<String, RollbackValue> = std::collections::HashMap::new();

    for target in &targets {
        let get_request = ScatterRequest {
            method: "GET".to_string(),
            path: format!("/indexes/{}/{}", index, setting_path),
            body: vec![],
            headers: vec![],
        };

        let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);

        match scatter.scatter(&topology, vec![target.clone()], get_request, UnavailableShardPolicy::Partial).await {
            Ok(resp) => {
                if let Some(r) = resp.responses.first() {
                    let original_value = if r.status == 200 {
                        Some(r.body.clone())
                    } else {
                        None
                    };
                    rollback_values.insert(
                        target.as_str().to_string(),
                        RollbackValue {
                            original_value,
                            existed: r.status == 200,
                        },
                    );
                }
            }
            Err(_) => {
                // Node is already down, skip rollback for it
                rollback_values.insert(target.as_str().to_string(), RollbackValue {
                    original_value: None,
                    existed: false,
                });
            }
        }
    }

    let body_bytes = serde_json::to_vec(value).unwrap_or_default();

    let request = ScatterRequest {
        method: "PUT".to_string(),
        path: format!("/indexes/{}/{}", index, setting_path),
        body: body_bytes,
        headers: vec![],
    };

    let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);

    // Track successful nodes for rollback
    let mut successful_nodes: Vec<String> = Vec::new();
    let mut last_response: Option<Value> = None;

    // Sequential broadcast with rollback on failure
    for target in &targets {
        let result = scatter
            .scatter(&topology, vec![target.clone()], request.clone(), UnavailableShardPolicy::Error)
            .await;

        match result {
            Ok(resp) => {
                if let Some(r) = resp.responses.first() {
                    if (200..300).contains(&r.status) {
                        successful_nodes.push(target.as_str().to_string());
                        last_response = Some(r.body.clone());
                    } else {
                        // Rollback from successful nodes
                        rollback_setting(state, &topology, &successful_nodes, &rollback_values, index, setting_path).await;
                        return Err(ErrorResponse::internal_error(format!(
                            "Failed to update setting on node {}: status {}",
                            target.as_str(),
                            r.status
                        )));
                    }
                }
            }
            Err(e) => {
                // Rollback from successful nodes
                rollback_setting(state, &topology, &successful_nodes, &rollback_values, index, setting_path).await;
                return Err(ErrorResponse::internal_error(format!(
                    "Failed to update setting on node {}: {}",
                    target.as_str(),
                    e
                )));
            }
        }
    }

    let response_body = if let Some(body) = last_response {
        body
    } else {
        let task_uid = state.task_manager.next_uid();
        serde_json::json!({
            "taskUid": task_uid,
            "indexUid": index,
            "status": "enqueued",
            "type": "settingsUpdate",
            "enqueuedAt": chrono::Utc::now().to_rfc3339(),
        })
    };

    Ok((axum::http::StatusCode::ACCEPTED, Json(response_body)).into_response())
}

/// Rollback a setting from nodes that were successfully updated.
async fn rollback_setting(
    state: &ProxyState,
    topology: &miroir_core::topology::Topology,
    successful_nodes: &[String],
    rollback_values: &std::collections::HashMap<String, RollbackValue>,
    index: &str,
    setting_path: &str,
) {
    for node_id in successful_nodes {
        if let Some(rollback) = rollback_values.get(node_id) {
            if rollback.existed {
                // Restore original value
                if let Some(original) = &rollback.original_value {
                    let body_bytes = serde_json::to_vec(original).unwrap_or_default();
                    let _ = state
                        .client
                        .send_to_node(
                            topology,
                            &node_id.as_str().into(),
                            "PUT",
                            &format!("/indexes/{}/{}", index, setting_path),
                            Some(&body_bytes),
                            &[],
                        )
                        .await;
                }
            } else {
                // Setting didn't exist before, delete it
                let _ = state
                    .client
                    .send_to_node(
                        topology,
                        &node_id.as_str().into(),
                        "DELETE",
                        &format!("/indexes/{}/{}", index, setting_path),
                        None,
                        &[],
                    )
                    .await;
            }
        }
    }
}

/// GET /indexes/:index/settings/filterable-attributes
async fn get_filterable_attributes(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    get_setting(state, &index, "filterable-attributes").await
}

/// PUT /indexes/:index/settings/filterable-attributes
async fn update_filterable_attributes(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    body: Value,
) -> Result<Response, ErrorResponse> {
    // Ensure _miroir_shard is always in filterable attributes
    let mut updated = body.clone();
    if let Some(arr) = updated.as_array_mut() {
        if !arr.iter().any(|v| v.as_str() == Some("_miroir_shard")) {
            arr.push(serde_json::json!("_miroir_shard"));
        }
    }
    update_setting_with_rollback(&state, &index, "settings/filterable-attributes", &updated).await
}

/// DELETE /indexes/:index/settings/filterable-attributes
async fn delete_filterable_attributes(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Response, ErrorResponse> {
    // Reset to default but always include _miroir_shard
    let default = serde_json::json!(["_miroir_shard"]);
    update_setting_with_rollback(&state, &index, "settings/filterable-attributes", &default).await
}

/// GET /indexes/:index/settings/searchable-attributes
async fn get_searchable_attributes(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    get_setting(state, &index, "settings/searchable-attributes").await
}

/// PUT /indexes/:index/settings/searchable-attributes
async fn update_searchable_attributes(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    body: Value,
) -> Result<Response, ErrorResponse> {
    update_setting_with_rollback(&state, &index, "settings/searchable-attributes", &body).await
}

/// DELETE /indexes/:index/settings/searchable-attributes
async fn delete_searchable_attributes(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Response, ErrorResponse> {
    delete_setting(state, &index, "settings/searchable-attributes").await
}

/// GET /indexes/:index/settings/sortable-attributes
async fn get_sortable_attributes(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    get_setting(state, &index, "settings/sortable-attributes").await
}

/// PUT /indexes/:index/settings/sortable-attributes
async fn update_sortable_attributes(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    body: Value,
) -> Result<Response, ErrorResponse> {
    update_setting_with_rollback(&state, &index, "settings/sortable-attributes", &body).await
}

/// DELETE /indexes/:index/settings/sortable-attributes
async fn delete_sortable_attributes(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Response, ErrorResponse> {
    delete_setting(state, &index, "settings/sortable-attributes").await
}

/// GET /indexes/:index/settings/displayed-attributes
async fn get_displayed_attributes(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    get_setting(state, &index, "settings/displayed-attributes").await
}

/// PUT /indexes/:index/settings/displayed-attributes
async fn update_displayed_attributes(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    body: Value,
) -> Result<Response, ErrorResponse> {
    update_setting_with_rollback(&state, &index, "settings/displayed-attributes", &body).await
}

/// DELETE /indexes/:index/settings/displayed-attributes
async fn delete_displayed_attributes(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Response, ErrorResponse> {
    delete_setting(state, &index, "settings/displayed-attributes").await
}

/// GET /indexes/:index/settings/ranking-rules
async fn get_ranking_rules(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    get_setting(state, &index, "settings/ranking-rules").await
}

/// PUT /indexes/:index/settings/ranking-rules
async fn update_ranking_rules(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    body: Value,
) -> Result<Response, ErrorResponse> {
    update_setting_with_rollback(&state, &index, "settings/ranking-rules", &body).await
}

/// DELETE /indexes/:index/settings/ranking-rules
async fn delete_ranking_rules(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Response, ErrorResponse> {
    delete_setting(state, &index, "settings/ranking-rules").await
}

/// GET /indexes/:index/settings/stop-words
async fn get_stop_words(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    get_setting(state, &index, "settings/stop-words").await
}

/// PUT /indexes/:index/settings/stop-words
async fn update_stop_words(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    body: Value,
) -> Result<Response, ErrorResponse> {
    update_setting_with_rollback(&state, &index, "settings/stop-words", &body).await
}

/// DELETE /indexes/:index/settings/stop-words
async fn delete_stop_words(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Response, ErrorResponse> {
    delete_setting(state, &index, "settings/stop-words").await
}

/// GET /indexes/:index/settings/synonyms
async fn get_synonyms(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    get_setting(state, &index, "settings/synonyms").await
}

/// PUT /indexes/:index/settings/synonyms
async fn update_synonyms(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    body: Value,
) -> Result<Response, ErrorResponse> {
    update_setting_with_rollback(&state, &index, "settings/synonyms", &body).await
}

/// DELETE /indexes/:index/settings/synonyms
async fn delete_synonyms(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Response, ErrorResponse> {
    delete_setting(state, &index, "settings/synonyms").await
}

/// GET /indexes/:index/settings/distinct-attribute
async fn get_distinct_attribute(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    get_setting(state, &index, "settings/distinct-attribute").await
}

/// PUT /indexes/:index/settings/distinct-attribute
async fn update_distinct_attribute(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    body: Value,
) -> Result<Response, ErrorResponse> {
    update_setting_with_rollback(&state, &index, "settings/distinct-attribute", &body).await
}

/// DELETE /indexes/:index/settings/distinct-attribute
async fn delete_distinct_attribute(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Response, ErrorResponse> {
    delete_setting(state, &index, "settings/distinct-attribute").await
}

/// GET /indexes/:index/settings/typo-tolerance
async fn get_typo_tolerance(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    get_setting(state, &index, "settings/typo-tolerance").await
}

/// PUT /indexes/:index/settings/typo-tolerance
async fn update_typo_tolerance(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    body: Value,
) -> Result<Response, ErrorResponse> {
    update_setting_with_rollback(&state, &index, "settings/typo-tolerance", &body).await
}

/// DELETE /indexes/:index/settings/typo-tolerance
async fn delete_typo_tolerance(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Response, ErrorResponse> {
    delete_setting(state, &index, "settings/typo-tolerance").await
}

/// GET /indexes/:index/settings/faceting
async fn get_faceting(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    get_setting(state, &index, "settings/faceting").await
}

/// PUT /indexes/:index/settings/faceting
async fn update_faceting(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    body: Value,
) -> Result<Response, ErrorResponse> {
    update_setting_with_rollback(&state, &index, "settings/faceting", &body).await
}

/// DELETE /indexes/:index/settings/faceting
async fn delete_faceting(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Response, ErrorResponse> {
    delete_setting(state, &index, "settings/faceting").await
}

/// GET /indexes/:index/settings/pagination
async fn get_pagination(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    get_setting(state, &index, "settings/pagination").await
}

/// PUT /indexes/:index/settings/pagination
async fn update_pagination(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    body: Value,
) -> Result<Response, ErrorResponse> {
    update_setting_with_rollback(&state, &index, "settings/pagination", &body).await
}

/// DELETE /indexes/:index/settings/pagination
async fn delete_pagination(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Response, ErrorResponse> {
    delete_setting(state, &index, "settings/pagination").await
}

/// Generic GET handler for a setting.
async fn get_setting(
    state: &ProxyState,
    index: &str,
    setting_path: &str,
) -> Result<Json<Value>, ErrorResponse> {
    let topology = state.topology().await;

    for group in topology.groups() {
        if let Some(node_id) = group.nodes().first() {
            let request = ScatterRequest {
                method: "GET".to_string(),
                path: format!("/indexes/{}/{}", index, setting_path),
                body: vec![],
                headers: vec![],
            };

            let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);

            let result = scatter
                .scatter(&topology, vec![node_id.clone()], request, UnavailableShardPolicy::Partial)
                .await
                .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

            if let Some(resp) = result.responses.first() {
                if resp.status == 200 {
                    return Ok(Json(resp.body.clone()));
                } else if resp.status == 404 {
                    return Err(ErrorResponse::index_not_found(index));
                }
            }
        }
    }

    Err(ErrorResponse::index_not_found(index))
}

/// Generic DELETE handler for resetting a setting to default.
async fn delete_setting(
    state: &ProxyState,
    index: &str,
    setting_path: &str,
) -> Result<Response, ErrorResponse> {
    let topology = state.topology().await;
    let targets = write_targets(0, &topology);

    if targets.is_empty() {
        return Err(ErrorResponse::internal_error("No nodes available"));
    }

    let request = ScatterRequest {
        method: "DELETE".to_string(),
        path: format!("/indexes/{}/{}", index, setting_path),
        body: vec![],
        headers: vec![],
    };

    let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);
    let result = scatter
        .scatter(&topology, targets, request, UnavailableShardPolicy::Partial)
        .await
        .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

    if let Some(resp) = result.responses.first() {
        let status = axum::http::StatusCode::from_u16(resp.status).unwrap_or(axum::http::StatusCode::OK);
        return Ok((status, Json(resp.body.clone())).into_response());
    }

    Ok((axum::http::StatusCode::ACCEPTED, Json(serde_json::json!({}))).into_response())
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_filterable_attributes_injection() {
        let input = serde_json::json!(["title", "description"]);
        let mut updated = input.clone();

        if let Some(arr) = updated.as_array_mut() {
            if !arr.iter().any(|v| v.as_str() == Some("_miroir_shard")) {
                arr.push(serde_json::json!("_miroir_shard"));
            }
        }

        let expected = serde_json::json!(["title", "description", "_miroir_shard"]);
        assert_eq!(updated, expected);
    }

    #[test]
    fn test_filterable_attributes_already_present() {
        let input = serde_json::json!(["title", "_miroir_shard"]);
        let mut updated = input.clone();

        if let Some(arr) = updated.as_array_mut() {
            if !arr.iter().any(|v| v.as_str() == Some("_miroir_shard")) {
                arr.push(serde_json::json!("_miroir_shard"));
            }
        }

        // Should not duplicate
        assert_eq!(updated, input);
    }
}
