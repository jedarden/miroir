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

/// POST /:index/documents - Add or replace documents.
async fn add_documents(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    headers: HeaderMap,
    body: Vec<Value>,
) -> Result<Response, ErrorResponse> {
    let topology = state.topology().await;

    // Get primary key for the index (for now, assume it's in the document or use default)
    // In production, we'd query the index settings to get the primary key field
    let primary_key = get_primary_key(&body, &headers).unwrap_or("id");

    // Inject _miroir_shard into each document and group by shard
    let mut docs_by_shard: std::collections::HashMap<u32, Vec<Value>> = std::collections::HashMap::new();

    for mut doc in body {
        let pk_value = doc
            .get(primary_key)
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

        let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);
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
        for (group_id, count) in &groups {
            if *count < quorum {
                any_degraded = true;
            } else {
                any_success = true;
            }
        }

        // Merge responses
        for resp in result.responses {
            all_responses.push(resp.body);
        }
    }

    // If no group met quorum, return 503
    if !any_success {
        return Err(ErrorResponse::no_quorum(0));
    }

    // Build response
    let task_uid = 1; // TODO: proper task ID generation
    let mut response_body = serde_json::json!({
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
    body: Vec<Value>,
) -> Result<Response, ErrorResponse> {
    // Same logic as POST, just different type
    add_documents(state, Path(index), headers, body).await
}

/// DELETE /:index/documents - Delete documents by batch.
async fn delete_documents(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    body: Value,
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

        let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);
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

        for (group_id, count) in &groups {
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
    let mut response_body = serde_json::json!({
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

    let task_uid = 1;
    let mut response_body = serde_json::json!({
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

    let shard_id = shard_for_key(&id, state.config.shards);
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

    // Strip _miroir_shard from response
    let mut body = resp.body.clone();
    if let Some(obj) = body.as_object_mut() {
        obj.remove("_miroir_shard");
    }

    let status = StatusCode::from_u16(resp.status).unwrap_or(StatusCode::OK);
    Ok((status, Json(body)).into_response())
}

/// Extract the primary key field from documents or headers.
fn get_primary_key(_documents: &[Value], headers: &HeaderMap) -> Option<String> {
    // Check for primary key in query string/header
    // For now, default to "id"
    // In production, we'd query the index settings
    if let Some(pk) = headers.get("X-Meiroil-Primary-Key") {
        pk.to_str().ok().map(|s| s.to_string())
    } else {
        Some("id".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_primary_key_default() {
        let headers = HeaderMap::new();
        let documents = vec![];
        let pk = get_primary_key(&documents, &headers);
        assert_eq!(pk, Some("id".to_string()));
    }

    #[test]
    fn test_get_primary_key_from_header() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Meiroil-Primary-Key", "user_id".parse().unwrap());
        let documents = vec![];
        let pk = get_primary_key(&documents, &headers);
        assert_eq!(pk, Some("user_id".to_string()));
    }
}
