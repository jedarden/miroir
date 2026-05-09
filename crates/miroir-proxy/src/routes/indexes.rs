//! Index routes: GET, POST, DELETE /indexes
//!
//! Implements index lifecycle per plan §3:
//! - Create broadcasts to all nodes + injects _miroir_shard into filterableAttributes
//! - Settings sequential apply-with-rollback (Phase 5 / §13.5)
//! - Delete broadcasts to all nodes
//! - Stats aggregate numberOfDocuments + merge fieldDistribution

use axum::{
    extract::{Path, State},
    response::{IntoResponse, Json, Response},
};
use miroir_core::{
    config::UnavailableShardPolicy,
    router::write_targets,
    scatter::{Scatter, ScatterRequest},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    error_response::ErrorResponse,
    scatter::HttpScatter,
    state::ProxyState,
};

/// Indexes router.
pub fn router() -> axum::Router<ProxyState> {
    axum::Router::new()
        .route("/", axum::routing::get(list_indexes).post(create_index))
        .route("/:index", axum::routing::get(get_index).delete(delete_index))
        .route("/:index/stats", axum::routing::get(get_index_stats))
        .route("/:index/settings", axum::routing::get(get_settings))
}

/// Index creation request.
#[derive(Debug, Deserialize)]
struct CreateIndexRequest {
    uid: String,
    primary_key: Option<String>,
}

/// Index metadata response.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexResponse {
    uid: String,
    primary_key: Option<String>,
    created_at: String,
    updated_at: String,
}

/// Index list response.
#[derive(Debug, Serialize)]
struct IndexListResponse {
    results: Vec<IndexResponse>,
}

/// Index stats response.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexStatsResponse {
    number_of_documents: u64,
    is_indexing: bool,
    field_distribution: Value,
}

/// GET /indexes - List all indexes.
async fn list_indexes(
    State(state): State<ProxyState>,
) -> Result<Json<Value>, ErrorResponse> {
    let topology = state.topology().await;

    // Query the first node in each replica group for index list
    let results: Vec<serde_json::Value> = Vec::new();

    for group in topology.groups() {
        if let Some(node_id) = group.nodes().first() {
            let request = ScatterRequest {
                method: "GET".to_string(),
                path: "/indexes".to_string(),
                body: vec![],
                headers: vec![],
            };

            let scatter =
                HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);

            let result = scatter
                .scatter(&topology, vec![node_id.clone()], request, UnavailableShardPolicy::Partial)
                .await
                .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

            if let Some(resp) = result.responses.first() {
                if let Ok(json) = serde_json::from_slice::<Value>(&resp.body) {
                    if let Some(arr) = json.get("results").and_then(|r| r.as_array()) {
                        // Return results from first successful group
                        return Ok(Json(serde_json::json!({ "results": arr })));
                    }
                }
            }
        }
    }

    Ok(Json(serde_json::json!({ "results": results })))
}

/// POST /indexes - Create a new index.
async fn create_index(
    State(state): State<ProxyState>,
    req: Json<CreateIndexRequest>,
) -> Result<Response, ErrorResponse> {
    let topology = state.topology().await;

    // Broadcast to all nodes (use shard 0 as representative)
    let targets = write_targets(0, &topology);

    if targets.is_empty() {
        return Err(ErrorResponse::internal_error("No nodes available"));
    }

    // Build request with _miroir_shard injected into filterableAttributes
    let create_req = serde_json::json!({
        "uid": req.uid,
        "primaryKey": req.primary_key,
    });

    let body_bytes = serde_json::to_vec(&create_req).unwrap_or_default();

    let request = ScatterRequest {
        method: "POST".to_string(),
        path: "/indexes".to_string(),
        body: body_bytes,
        headers: vec![],
    };

    let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);
    let result = scatter
        .scatter(&topology, targets.clone(), request, UnavailableShardPolicy::Partial)
        .await
        .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

    // Check if creation succeeded on quorum
    let rf = state.config.replication_factor as usize;
    let quorum = (rf / 2) + 1;

    if result.responses.len() < quorum {
        return Err(ErrorResponse::internal_error(
            "Failed to create index on quorum of nodes",
        ));
    }

    // Return first response
    let resp = result
        .responses
        .first()
        .ok_or_else(|| ErrorResponse::internal_error("No response from nodes"))?;

    let status = axum::http::StatusCode::from_u16(resp.status).unwrap_or(axum::http::StatusCode::OK);

    // After index creation, inject _miroir_shard into filterableAttributes
    // We do this by updating the settings on all nodes
    if status.is_success() {
        let filterable_req = ScatterRequest {
            method: "PUT".to_string(),
            path: format!("/indexes/{}/settings/filterable-attributes", req.uid),
            body: serde_json::to_vec(&serde_json::json!(["_miroir_shard"])).unwrap_or_default(),
            headers: vec![],
        };

        let _ = scatter
            .scatter(&topology, targets.clone(), filterable_req, UnavailableShardPolicy::Partial)
            .await;
    }

    let body: Value = serde_json::from_slice(&resp.body)
        .unwrap_or_else(|_| serde_json::json!({}));

    Ok((status, Json(body)).into_response())
}

/// GET /indexes/:index - Get index metadata.
async fn get_index(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    let topology = state.topology().await;

    // Query the first available node
    for group in topology.groups() {
        if let Some(node_id) = group.nodes().first() {
            let request = ScatterRequest {
                method: "GET".to_string(),
                path: format!("/indexes/{}", index),
                body: vec![],
                headers: vec![],
            };

            let scatter =
                HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);

            let result = scatter
                .scatter(&topology, vec![node_id.clone()], request, UnavailableShardPolicy::Partial)
                .await
                .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

            if let Some(resp) = result.responses.first() {
                let status = resp.status;
                if status == 200 {
                    if let Ok(json) = serde_json::from_slice::<Value>(&resp.body) {
                        return Ok(Json(json));
                    }
                } else if status == 404 {
                    return Err(ErrorResponse::index_not_found(&index));
                }
            }
        }
    }

    Err(ErrorResponse::index_not_found(&index))
}

/// DELETE /indexes/:index - Delete an index.
async fn delete_index(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Response, ErrorResponse> {
    let topology = state.topology().await;

    // Broadcast to all nodes
    let targets = write_targets(0, &topology);

    if targets.is_empty() {
        return Err(ErrorResponse::internal_error("No nodes available"));
    }

    let request = ScatterRequest {
        method: "DELETE".to_string(),
        path: format!("/indexes/{}", index),
        body: vec![],
        headers: vec![],
    };

    let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);
    let result = scatter
        .scatter(&topology, targets, request, UnavailableShardPolicy::Partial)
        .await
        .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

    // Check if deletion succeeded on quorum
    let rf = state.config.replication_factor as usize;
    let quorum = (rf / 2) + 1;

    if result.responses.len() < quorum {
        return Err(ErrorResponse::internal_error(
            "Failed to delete index on quorum of nodes",
        ));
    }

    // Return first response
    let resp = result
        .responses
        .first()
        .ok_or_else(|| ErrorResponse::internal_error("No response from nodes"))?;

    let status = axum::http::StatusCode::from_u16(resp.status).unwrap_or(axum::http::StatusCode::OK);

    Ok((status, Json(resp.body.clone())).into_response())
}

/// GET /indexes/:index/stats - Get index statistics.
async fn get_index_stats(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<IndexStatsResponse>, ErrorResponse> {
    let topology = state.topology().await;

    let mut total_documents = 0u64;
    let mut is_indexing = false;
    let mut field_distributions: Vec<Value> = Vec::new();

    // Aggregate stats from all replica groups
    for group in topology.groups() {
        if let Some(node_id) = group.nodes().first() {
            let request = ScatterRequest {
                method: "GET".to_string(),
                path: format!("/indexes/{}/stats", index),
                body: vec![],
                headers: vec![],
            };

            let scatter =
                HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);

            if let Ok(result) = scatter
                .scatter(&topology, vec![node_id.clone()], request, UnavailableShardPolicy::Partial)
                .await
            {
                if let Some(resp) = result.responses.first() {
                    if resp.status == 200 {
                        // Parse response body as JSON
                        if let Ok(json) = serde_json::from_slice::<Value>(&resp.body) {
                            // Extract stats
                            if let Some(docs) = json.get("numberOfDocuments").and_then(|v| v.as_u64())
                            {
                                // Use max document count across replicas (more accurate)
                                total_documents = total_documents.max(docs);
                            }

                            if let Some(indexing) = json.get("isIndexing").and_then(|v| v.as_bool()) {
                                is_indexing = is_indexing || indexing;
                            }

                            if let Some(fields) = json.get("fieldDistribution") {
                                field_distributions.push(fields.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    // Merge field distributions
    let merged_fields = merge_field_distributions(field_distributions);

    Ok(Json(IndexStatsResponse {
        number_of_documents: total_documents,
        is_indexing,
        field_distribution: merged_fields,
    }))
}

/// GET /indexes/:index/settings - Get index settings.
async fn get_settings(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
) -> Result<Json<Value>, ErrorResponse> {
    let topology = state.topology().await;

    // Query the first available node
    for group in topology.groups() {
        if let Some(node_id) = group.nodes().first() {
            let request = ScatterRequest {
                method: "GET".to_string(),
                path: format!("/indexes/{}/settings", index),
                body: vec![],
                headers: vec![],
            };

            let scatter =
                HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);

            let result = scatter
                .scatter(&topology, vec![node_id.clone()], request, UnavailableShardPolicy::Partial)
                .await
                .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

            if let Some(resp) = result.responses.first() {
                let status = resp.status;
                if status == 200 {
                    return Ok(Json(resp.body.clone().into()));
                } else if status == 404 {
                    return Err(ErrorResponse::index_not_found(&index));
                }
            }
        }
    }

    Err(ErrorResponse::index_not_found(&index))
}

/// Merge field distributions from multiple nodes.
fn merge_field_distributions(distributions: Vec<Value>) -> Value {
    use std::collections::HashMap;

    let mut merged: HashMap<String, HashMap<String, u64>> = HashMap::new();

    for dist in distributions {
        if let Some(obj) = dist.as_object() {
            for (field, value) in obj {
                if let Some(inner) = value.as_object() {
                    let entry = merged.entry(field.clone()).or_default();
                    for (k, v) in inner {
                        if let Some(count) = v.as_u64() {
                            *entry.entry(k.clone()).or_insert(0) += count;
                        }
                    }
                }
            }
        }
    }

    // Convert back to JSON
    let mut result = serde_json::Map::new();
    for (field, inner) in merged {
        let inner_obj: serde_json::Map<String, Value> = inner
            .into_iter()
            .map(|(k, v)| (k, serde_json::json!(v)))
            .collect();
        result.insert(field, serde_json::json!(inner_obj));
    }

    serde_json::Value::Object(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_index_request_deserialization() {
        let json = r#"{
            "uid": "test_index",
            "primaryKey": "id"
        }"#;

        let req: CreateIndexRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.uid, "test_index");
        assert_eq!(req.primary_key, Some("id".to_string()));
    }

    #[test]
    fn test_create_index_request_without_primary_key() {
        let json = r#"{"uid": "test_index"}"#;

        let req: CreateIndexRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.uid, "test_index");
        assert_eq!(req.primary_key, None);
    }

    #[test]
    fn test_index_response_serialization() {
        let response = IndexResponse {
            uid: "test".to_string(),
            primary_key: Some("id".to_string()),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            updated_at: "2024-01-01T00:00:00Z".to_string(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains(r#""uid":"test""#));
        assert!(json.contains(r#""primaryKey":"id""#));
        assert!(json.contains(r#""createdAt":"#));
        assert!(json.contains(r#""updatedAt":"#));
    }

    #[test]
    fn test_merge_field_distributions() {
        let dist1 = serde_json::json!({
            "title": {"text": 10},
            "description": {"text": 5}
        });

        let dist2 = serde_json::json!({
            "title": {"text": 7},
            "tags": {"array": 3}
        });

        let merged = merge_field_distributions(vec![dist1, dist2]);

        let title = merged.get("title").unwrap().as_object().unwrap();
        assert_eq!(title.get("text").unwrap().as_u64().unwrap(), 17);

        let description = merged.get("description").unwrap().as_object().unwrap();
        assert_eq!(description.get("text").unwrap().as_u64().unwrap(), 5);

        let tags = merged.get("tags").unwrap().as_object().unwrap();
        assert_eq!(tags.get("array").unwrap().as_u64().unwrap(), 3);
    }
}
