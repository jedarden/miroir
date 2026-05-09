//! Document routes: POST, PUT, DELETE, GET /documents
//!
//! Implements the write path per plan §2:
//! - Hash primary key to get shard ID
//! - Inject _miroir_shard field
//! - Fan out to RG × RF nodes
//! - Per-group quorum (floor(RF/2)+1)
//! - X-Miroir-Degraded header on any group missing quorum
//! - 503 miroir_no_quorum only when no group met quorum

use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Json, Response},
};
use miroir_core::{
    config::UnavailableShardPolicy,
    router::{shard_for_key, write_targets},
    scatter::{Scatter, ScatterRequest},
};
use serde_json::Value;

use crate::{
    error_response::ErrorResponse,
    scatter::HttpScatter,
    state::ProxyState,
};

/// Documents router.
pub fn router() -> axum::Router<ProxyState> {
    axum::Router::new()
        .route("/:index", axum::routing::post(add_documents))
        .route("/:index/documents", axum::routing::post(add_documents))
        .route("/:index/documents", axum::routing::put(update_documents))
        .route("/:index/documents", axum::routing::delete(delete_documents))
        .route("/:index/documents/:id", axum::routing::get(get_document))
        .route("/:index/documents/:id", axum::routing::delete(delete_document))
}

/// Extract the primary key field from documents or headers.
/// First checks the index settings, then falls back to headers and defaults.
async fn get_primary_key(
    state: &ProxyState,
    index: &str,
    documents: &[Value],
    headers: &HeaderMap,
) -> Result<String, ErrorResponse> {
    // First, try to get primary key from index settings
    let topology = state.topology().await;

    for group in topology.groups() {
        if let Some(node_id) = group.nodes().first() {
            let request = ScatterRequest {
                method: "GET".to_string(),
                path: format!("/indexes/{}", index),
                body: vec![],
                headers: vec![],
            };

            let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);

            if let Ok(result) = scatter.scatter(&topology, vec![node_id.clone()], request, UnavailableShardPolicy::Partial).await {
                if let Some(resp) = result.responses.first() {
                    if resp.status == 200 {
                        if let Ok(json) = serde_json::from_slice::<Value>(&resp.body) {
                            if let Some(pk) = json.get("primaryKey").and_then(|v| v.as_str()) {
                                return Ok(pk.to_string());
                            }
                        }
                    }
                }
            }
        }
    }

    // Check for primary key in query string/header
    if let Some(pk) = headers.get("X-Meiroil-Primary-Key") {
        if let Ok(pk_str) = pk.to_str() {
            return Ok(pk_str.to_string());
        }
    }

    // Try to infer from first document
    if let Some(doc) = documents.first() {
        // Common primary key field names to try
        for candidate in &["id", "Id", "ID", "_id", "key", "Key", "pk"] {
            if doc.get(*candidate).is_some() {
                return Ok(candidate.to_string());
            }
        }
    }

    // Default to "id"
    Ok("id".to_string())
}

/// POST /:index/documents - Add or replace documents.
async fn add_documents(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Vec<Value>>,
) -> Result<Response, ErrorResponse> {
    let topology = state.topology().await;

    // Get primary key for the index
    let primary_key = get_primary_key(&state, &index, &body, &headers).await?;

    // Inject _miroir_shard into each document and group by shard
    let mut docs_by_shard: std::collections::HashMap<u32, Vec<Value>> = std::collections::HashMap::new();

    for mut doc in body {
        let pk_value = doc
            .get(primary_key.as_str())
            .and_then(|v| v.as_str())
            .ok_or_else(|| ErrorResponse::invalid_request(format!("Missing primary key field '{primary_key}'")))?;

        let shard_id = shard_for_key(pk_value, state.config.shards);

        // Inject _miroir_shard field
        if let Some(obj) = doc.as_object_mut() {
            obj.insert("_miroir_shard".to_string(), Value::Number(shard_id.into()));
        }

        docs_by_shard.entry(shard_id).or_default().push(doc);
    }

    // For each shard, scatter write to all RG × RF nodes
    let mut all_responses: Vec<Value> = Vec::new();
    let mut any_degraded = false;
    let mut any_success = false;

    for (shard_id, docs) in docs_by_shard {
        let targets = write_targets(shard_id, &topology);

        if targets.is_empty() {
            return Err(ErrorResponse::no_quorum(shard_id));
        }

        // Build request body
        let body_bytes = serde_json::to_vec(&docs).unwrap_or_default();

        let request = ScatterRequest {
            method: "POST".to_string(),
            path: format!("/indexes/{}/documents", index),
            body: body_bytes,
            headers: vec![],
        };

        let scatter = HttpScatter::with_retry_cache(
            (*state.client).clone(),
            state.config.server.request_timeout_ms,
            (*state.retry_cache).clone(),
        );
        let result = scatter
            .scatter(&topology, targets, request, UnavailableShardPolicy::Partial)
            .await
            .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

        // Check quorum per replica group
        let rf = state.config.replication_factor as usize;
        let quorum = (rf / 2) + 1;

        // Group responses by replica group
        let mut groups: std::collections::HashMap<u32, usize> = std::collections::HashMap::new();
        for resp in &result.responses {
            let node = topology.node(&resp.node_id).unwrap();
            *groups.entry(node.replica_group).or_insert(0) += 1;
        }

        // Check if each group met quorum
        for (_group_id, count) in &groups {
            if *count < quorum {
                any_degraded = true;
            } else {
                any_success = true;
            }
        }

        // Merge responses
        for resp in result.responses {
            if let Ok(json) = serde_json::from_slice::<Value>(&resp.body) {
                all_responses.push(json);
            }
        }
    }

    // If no group met quorum, return 503
    if !any_success {
        return Err(ErrorResponse::no_quorum(0));
    }

    // Build response with proper task UID
    let task_uid = state.task_manager.next_uid();
    let response_body = serde_json::json!({
        "taskUid": task_uid,
        "indexUid": index,
        "status": "enqueued",
        "type": "documentAdditionOrUpdate",
        "enqueuedAt": chrono::Utc::now().to_rfc3339(),
    });

    let mut builder = Response::builder().status(202);

    // Add degraded header if any group was degraded
    if any_degraded {
        if let Ok(val) = HeaderValue::from_str("true") {
            builder = builder.header("X-Miroir-Degraded", val);
        }
    }

    Ok(builder.body(Json(response_body).into_response().into_body()).unwrap())
}

/// PUT /:index/documents - Update documents.
async fn update_documents(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Vec<Value>>,
) -> Result<Response, ErrorResponse> {
    // Same logic as POST, just different type
    add_documents(State(state), Path(index), headers, Json(body)).await
}

/// DELETE /:index/documents - Delete documents by batch.
async fn delete_documents(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    Json(body): Json<Value>,
) -> Result<Response, ErrorResponse> {
    let topology = state.topology().await;

    // Extract filter or IDs from request body
    let ids = body
        .get("ids")
        .and_then(|v| v.as_array())
        .ok_or_else(|| ErrorResponse::invalid_request("Missing 'ids' field in delete request"))?;

    // Group by shard
    let mut docs_by_shard: std::collections::HashMap<u32, Vec<String>> = std::collections::HashMap::new();

    for id_val in ids {
        let id = id_val
            .as_str()
            .ok_or_else(|| ErrorResponse::invalid_request("ID must be a string"))?;

        let shard_id = shard_for_key(id, state.config.shards);
        docs_by_shard.entry(shard_id).or_default().push(id.to_string());
    }

    // For each shard, scatter delete to all RG × RF nodes
    let mut any_degraded = false;
    let mut any_success = false;

    for (shard_id, ids) in docs_by_shard {
        let targets = write_targets(shard_id, &topology);

        if targets.is_empty() {
            return Err(ErrorResponse::no_quorum(shard_id));
        }

        let body_bytes = serde_json::to_vec(&serde_json::json!({ "ids": ids })).unwrap_or_default();

        let request = ScatterRequest {
            method: "POST".to_string(),
            path: format!("/indexes/{}/documents/delete", index),
            body: body_bytes,
            headers: vec![],
        };

        let scatter = HttpScatter::with_retry_cache(
            (*state.client).clone(),
            state.config.server.request_timeout_ms,
            (*state.retry_cache).clone(),
        );
        let result = scatter
            .scatter(&topology, targets, request, UnavailableShardPolicy::Partial)
            .await
            .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

        // Check quorum per replica group
        let rf = state.config.replication_factor as usize;
        let quorum = (rf / 2) + 1;

        let mut groups: std::collections::HashMap<u32, usize> = std::collections::HashMap::new();
        for resp in &result.responses {
            let node = topology.node(&resp.node_id).unwrap();
            *groups.entry(node.replica_group).or_insert(0) += 1;
        }

        for (_group_id, count) in &groups {
            if *count < quorum {
                any_degraded = true;
            } else {
                any_success = true;
            }
        }
    }

    if !any_success {
        return Err(ErrorResponse::no_quorum(0));
    }

    let task_uid = 1;
    let response_body = serde_json::json!({
        "taskUid": task_uid,
        "indexUid": index,
        "status": "enqueued",
        "type": "documentDeletion",
        "enqueuedAt": chrono::Utc::now().to_rfc3339(),
    });

    let mut builder = Response::builder().status(202);

    if any_degraded {
        if let Ok(val) = HeaderValue::from_str("true") {
            builder = builder.header("X-Miroir-Degraded", val);
        }
    }

    Ok(builder.body(Json(response_body).into_response().into_body()).unwrap())
}

/// DELETE /:index/documents/:id - Delete a single document.
async fn delete_document(
    State(state): State<ProxyState>,
    Path((index, id)): Path<(String, String)>,
) -> Result<Response, ErrorResponse> {
    let topology = state.topology().await;

    let shard_id = shard_for_key(&id, state.config.shards);
    let targets = write_targets(shard_id, &topology);

    if targets.is_empty() {
        return Err(ErrorResponse::no_quorum(shard_id));
    }

    let body_bytes =
        serde_json::to_vec(&serde_json::json!({ "ids": [id] })).unwrap_or_default();

    let request = ScatterRequest {
        method: "POST".to_string(),
        path: format!("/indexes/{}/documents/delete", index),
        body: body_bytes,
        headers: vec![],
    };

    let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);
    let result = scatter
        .scatter(&topology, targets, request, UnavailableShardPolicy::Partial)
        .await
        .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

    // Check quorum
    let rf = state.config.replication_factor as usize;
    let quorum = (rf / 2) + 1;

    let mut groups: std::collections::HashMap<u32, usize> = std::collections::HashMap::new();
    for resp in &result.responses {
        let node = topology.node(&resp.node_id).unwrap();
        *groups.entry(node.replica_group).or_insert(0) += 1;
    }

    let mut any_degraded = false;
    let mut any_success = false;

    for (_group_id, count) in &groups {
        if *count < quorum {
            any_degraded = true;
        } else {
            any_success = true;
        }
    }

    if !any_success {
        return Err(ErrorResponse::no_quorum(shard_id));
    }

    let task_uid = state.task_manager.next_uid();
    let response_body = serde_json::json!({
        "taskUid": task_uid,
        "indexUid": index,
        "status": "enqueued",
        "type": "documentDeletion",
        "enqueuedAt": chrono::Utc::now().to_rfc3339(),
    });

    let mut builder = Response::builder().status(202);

    if any_degraded {
        if let Ok(val) = HeaderValue::from_str("true") {
            builder = builder.header("X-Miroir-Degraded", val);
        }
    }

    Ok(builder.body(Json(response_body).into_response().into_body()).unwrap())
}

/// GET /:index/documents/:id - Get a single document by ID.
async fn get_document(
    State(state): State<ProxyState>,
    Path((index, id)): Path<(String, String)>,
) -> Result<Response, ErrorResponse> {
    let topology = state.topology().await;

    // For GET, we only need to query one replica group
    // Use the query group (round-robin)
    let query_seq = state.next_query_seq();
    let group_id = miroir_core::router::query_group(query_seq, state.config.replica_groups);

    let group = topology
        .group(group_id)
        .ok_or_else(|| ErrorResponse::internal_error(format!("Group {} not found", group_id)))?;

    let _shard_id = shard_for_key(&id, state.config.shards);
    let rf = state.config.replication_factor as usize;

    // Build covering set for this shard
    let covering = miroir_core::router::covering_set(1, group, rf, query_seq);

    // Query the node responsible for this shard
    let target = covering
        .first()
        .ok_or_else(|| ErrorResponse::internal_error("No nodes in covering set"))?;

    let request = ScatterRequest {
        method: "GET".to_string(),
        path: format!("/indexes/{}/documents/{}", index, id),
        body: vec![],
        headers: vec![],
    };

    let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);
    let result = scatter
        .scatter(&topology, vec![target.clone()], request, UnavailableShardPolicy::Error)
        .await
        .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

    if result.responses.is_empty() {
        return Err(ErrorResponse::document_not_found(&id));
    }

    let resp = &result.responses[0];

    // Parse response body as JSON and strip _miroir_shard
    let mut body: Value = serde_json::from_slice(&resp.body)
        .unwrap_or_else(|_| serde_json::json!({}));
    if let Some(obj) = body.as_object_mut() {
        obj.remove("_miroir_shard");
    }

    let status = StatusCode::from_u16(resp.status).unwrap_or(StatusCode::OK);
    Ok((status, Json(body)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_shard_hashing() {
        // Test that same ID always maps to same shard
        let id1 = "test-doc-123";
        let id2 = "test-doc-123";
        let id3 = "different-doc";

        // These should be deterministic
        let shard1 = shard_for_key(id1, 64);
        let shard2 = shard_for_key(id2, 64);
        let shard3 = shard_for_key(id3, 64);

        assert_eq!(shard1, shard2, "Same ID should map to same shard");
        assert_ne!(shard1, shard3, "Different IDs should likely map to different shards");
    }

    #[test]
    fn test_document_shard_uniformity() {
        // Test that documents distribute reasonably evenly
        let mut shard_counts = vec![0usize; 64];

        for i in 0..1000 {
            let id = format!("doc-{}", i);
            let shard = shard_for_key(&id, 64);
            shard_counts[shard as usize] += 1;
        }

        // Check that each shard got at least some documents
        // (with 1000 docs and 64 shards, most should get at least 10-20)
        let min_count = *shard_counts.iter().min().unwrap();
        let max_count = *shard_counts.iter().max().unwrap();

        assert!(min_count >= 5, "Minimum shard count too low: {}", min_count);
        assert!(max_count <= 30, "Maximum shard count too high: {}", max_count);
    }
}
