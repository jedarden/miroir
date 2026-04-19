//! Document write path: add, replace, and delete documents.
//!
//! Implements P2.2 write path:
//! - Primary key extraction on the hot path
//! - `_miroir_shard` injection
//! - Reserved field rejection
//! - Two-rule quorum
//!
//! Implements P2.5 task reconciliation:
//! - Collects per-node task UIDs
//! - Registers Miroir task ID (mtask-<uuid>)
//! - Returns mtask ID to client

use axum::extract::{Extension, Path, Query};
use axum::response::{IntoResponse, Response};
use axum::http::{StatusCode, header};
use axum::{Json, Router};
use miroir_core::api_error::{MiroirCode, MeilisearchError};
use miroir_core::router::{shard_for_key, write_targets};
use miroir_core::scatter::{DeleteByIdsRequest, DeleteByFilterRequest, NodeClient, WriteRequest, WriteResponse};
use miroir_core::task::TaskRegistry;
use miroir_core::topology::{Topology, NodeId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::client::HttpClient;
use crate::routes::admin_endpoints::AppState;

/// Document write parameters from query string.
#[derive(Debug, Deserialize)]
pub struct DocumentsParams {
    primaryKey: Option<String>,
}

/// Task response (Meilisearch-compatible).
#[derive(Debug, Serialize)]
pub struct TaskResponse {
    taskUid: u64,
    indexUid: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "type")]
    error_type: Option<String>,
}

/// Response for write operations.
#[derive(Debug, Serialize)]
pub struct DocumentsWriteResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    taskUid: Option<String>, // Changed to String to hold mtask-<uuid>
    #[serde(skip_serializing_if = "Option::is_none")]
    indexUid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "type")]
    error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    link: Option<String>,
}

/// Header name for degraded write responses.
pub const HEADER_MIROIR_DEGRADED: &str = "X-Miroir-Degraded";

/// Quorum tracking state for write operations.
#[derive(Debug, Default)]
struct QuorumState {
    /// Per-group ACK counts: group_id -> successful_ack_count
    group_acks: HashMap<u32, usize>,
    /// Per-group total node counts: group_id -> total_nodes_attempted
    group_totals: HashMap<u32, usize>,
    /// Groups that met quorum: group_id -> true
    groups_met_quorum: HashMap<u32, bool>,
    /// Total degraded groups count
    degraded_groups: u32,
}

impl QuorumState {
    /// Record a write attempt to a node.
    fn record_attempt(&mut self, group_id: u32, _node_id: &NodeId) {
        *self.group_totals.entry(group_id).or_insert(0) += 1;
    }

    /// Record a successful write ACK from a node.
    fn record_success(&mut self, group_id: u32, _node_id: &NodeId) {
        *self.group_acks.entry(group_id).or_insert(0) += 1;
    }

    /// Record a failed write attempt from a node.
    fn record_failure(&mut self, _group_id: u32) {
        // Track that this group had a failure
        // Degraded is determined after checking quorum
    }

    /// Check if a group has met quorum: floor(RF/2) + 1 ACKs required.
    fn check_group_quorum(&mut self, group_id: u32, rf: usize) -> bool {
        let acks = *self.group_acks.get(&group_id).unwrap_or(&0);
        let quorum = (rf / 2) + 1;
        let met = acks >= quorum;
        *self.groups_met_quorum.entry(group_id).or_insert(false) = met;
        met
    }

    /// Count how many groups met quorum.
    fn count_quorum_groups(&self) -> usize {
        self.groups_met_quorum.values().filter(|&&v| v).count()
    }

    /// Count degraded groups (groups that exist but didn't meet quorum).
    fn count_degraded_groups(&mut self, replica_group_count: u32, rf: usize) -> u32 {
        let mut degraded = 0u32;
        for group_id in 0..replica_group_count {
            if !self.check_group_quorum(group_id, rf) {
                // Only count as degraded if we attempted to write to this group
                if self.group_totals.contains_key(&group_id) {
                    degraded += 1;
                }
            }
        }
        degraded
    }
}

/// Build router for document endpoints.
pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/", axum::routing::post(post_documents))
        .route("/", axum::routing::put(put_documents))
        .route("/", axum::routing::delete(delete_documents))
        .route("/:id", axum::routing::delete(delete_document_by_id))
}

/// POST /indexes/{uid}/documents - Add documents.
async fn post_documents(
    Path(index): Path<String>,
    Query(params): Query<DocumentsParams>,
    Extension(state): Extension<Arc<AppState>>,
    Json(documents): Json<Vec<Value>>,
) -> std::result::Result<Response, MeilisearchError> {
    write_documents_impl(index, params.primaryKey, documents, &state).await
}

/// PUT /indexes/{uid}/documents - Replace documents.
async fn put_documents(
    Path(index): Path<String>,
    Query(params): Query<DocumentsParams>,
    Extension(state): Extension<Arc<AppState>>,
    Json(documents): Json<Vec<Value>>,
) -> std::result::Result<Response, MeilisearchError> {
    write_documents_impl(index, params.primaryKey, documents, &state).await
}

/// DELETE /indexes/{uid}/documents - Delete by IDs or filter.
async fn delete_documents(
    Path(index): Path<String>,
    Extension(state): Extension<Arc<AppState>>,
    Json(body): Json<Value>,
) -> std::result::Result<Response, MeilisearchError> {
    // Try to parse as delete by filter first
    if let Some(filter) = body.get("filter") {
        let req = DeleteByFilterRequest {
            index_uid: index.clone(),
            filter: filter.clone(),
        };
        return delete_by_filter_impl(index, req, &state).await;
    }

    // Try to parse as delete by IDs
    if let Some(ids) = body.get("ids").and_then(|v| v.as_array()) {
        let ids: Vec<String> = ids
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        if !ids.is_empty() {
            let req = DeleteByIdsRequest {
                index_uid: index.clone(),
                ids,
            };
            return delete_by_ids_impl(index, req, &state).await;
        }
    }

    // If we get here, the request body is malformed
    Err(MeilisearchError::new(
        MiroirCode::ReservedField,
        "delete request must include either 'filter' or 'ids' field",
    ))
}

/// DELETE /indexes/{uid}/documents/{id} - Delete single document by ID.
async fn delete_document_by_id(
    Path((index, id)): Path<(String, String)>,
    Extension(state): Extension<Arc<AppState>>,
) -> std::result::Result<Response, MeilisearchError> {
    let req = DeleteByIdsRequest {
        index_uid: index.clone(),
        ids: vec![id],
    };
    delete_by_ids_impl(index, req, &state).await
}

/// Implementation for write documents (POST/PUT).
async fn write_documents_impl(
    index: String,
    primary_key: Option<String>,
    mut documents: Vec<Value>,
    state: &AppState,
) -> std::result::Result<Response, MeilisearchError> {
    if documents.is_empty() {
        return Err(MeilisearchError::new(
            MiroirCode::PrimaryKeyRequired,
            "cannot write empty document batch",
        ));
    }

    // 1. Extract primary key from first document if not provided
    let primary_key = primary_key.or_else(|| {
        documents
            .first()
            .and_then(|doc| extract_primary_key(doc))
    });

    let primary_key = primary_key.ok_or_else(|| {
        MeilisearchError::new(
            MiroirCode::PrimaryKeyRequired,
            format!("primary key required for index `{}`", index),
        )
    })?;

    // 2. Validate all documents have the primary key and check for reserved field
    for (i, doc) in documents.iter().enumerate() {
        // Check for reserved field BEFORE checking primary key (per acceptance criteria)
        if doc.get("_miroir_shard").is_some() {
            return Err(MeilisearchError::new(
                MiroirCode::ReservedField,
                "document contains reserved field `_miroir_shard`",
            ));
        }

        if doc.get(&primary_key).is_none() {
            return Err(MeilisearchError::new(
                MiroirCode::PrimaryKeyRequired,
                format!(
                    "document at index {} missing primary key field `{}`",
                    i, primary_key
                ),
            ));
        }
    }

    // 3. Inject _miroir_shard into each document
    let topology = state.topology.read().await;
    let shard_count = topology.shards;
    let rf = topology.rf();
    let replica_group_count = topology.replica_group_count();

    for doc in &mut documents {
        if let Some(pk_value) = doc.get(&primary_key).and_then(|v| v.as_str()) {
            let shard_id = shard_for_key(pk_value, shard_count);
            doc["_miroir_shard"] = serde_json::json!(shard_id);
        }
    }

    // 4. Group documents by target nodes (per-batch grouping for efficient fan-out)
    let node_documents = group_documents_by_shard(&documents, &primary_key, &topology)?;

    // 5. Fan out to nodes and track quorum
    let client = HttpClient::new(
        state.config.node_master_key.clone(),
        state.config.scatter.node_timeout_ms,
    );

    let mut quorum_state = QuorumState::default();
    let mut node_task_uids: HashMap<String, u64> = HashMap::new();

    // For each shard, write to all RF nodes in each replica group
    for (shard_id, docs) in node_documents {
        let targets = write_targets(shard_id, &topology);

        if targets.is_empty() {
            return Err(MeilisearchError::new(
                MiroirCode::ShardUnavailable,
                format!("no available nodes for shard {}", shard_id),
            ));
        }

        // Track which groups we're targeting for this shard

        for node_id in targets {
            let node = topology
                .node(&node_id)
                .ok_or_else(|| MeilisearchError::new(MiroirCode::ShardUnavailable, "node not found in topology"))?;

            let group_id = node.replica_group;
            quorum_state.record_attempt(group_id, &node_id);

            let req = WriteRequest {
                index_uid: index.clone(),
                documents: docs.clone(),
                primary_key: Some(primary_key.clone()),
            };

            match client.write_documents(&node_id, &node.address, &req).await {
                Ok(resp) if resp.success => {
                    quorum_state.record_success(group_id, &node_id);
                    if let Some(task_uid) = resp.task_uid {
                        node_task_uids.insert(node_id.as_str().to_string(), task_uid);
                    }
                }
                Ok(resp) => {
                    // Non-success response (validation error, etc.)
                    return Ok(build_json_error_response(build_error_response(resp)));
                }
                Err(_) => {
                    quorum_state.record_failure(group_id);
                }
            }
        }
    }

    // 6. Apply two-rule quorum logic
    let degraded_groups = quorum_state.count_degraded_groups(replica_group_count, rf);
    let quorum_groups = quorum_state.count_quorum_groups();

    // Write success if at least one group met quorum
    if quorum_groups == 0 {
        return Err(MeilisearchError::new(
            MiroirCode::NoQuorum,
            "no replica group met quorum",
        ));
    }

    // 7. Register Miroir task with collected node task UIDs
    let miroir_task = state
        .task_registry
        .register_with_metadata(
            node_task_uids.clone(),
            Some(index.clone()),
            Some("documentAdditionOrUpdate".to_string()),
        )
        .map_err(|e| MeilisearchError::new(
            MiroirCode::ShardUnavailable,
            format!("failed to register task: {}", e),
        ))?;

    // Build success response with degraded header and mtask ID
    build_response_with_degraded_header(
        DocumentsWriteResponse {
            taskUid: Some(miroir_task.miroir_id),
            indexUid: Some(index.clone()),
            status: Some("enqueued".to_string()),
            error: None,
            error_type: None,
            code: None,
            link: None,
        },
        degraded_groups,
    )
}

/// Implementation for delete by IDs.
async fn delete_by_ids_impl(
    index: String,
    req: DeleteByIdsRequest,
    state: &AppState,
) -> std::result::Result<Response, MeilisearchError> {
    if req.ids.is_empty() {
        return Err(MeilisearchError::new(
            MiroirCode::PrimaryKeyRequired,
            "cannot delete empty ID list",
        ));
    }

    let topology = state.topology.read().await;
    let rf = topology.rf();
    let replica_group_count = topology.replica_group_count();

    // Group IDs by target shard for independent per-shard routing
    let mut shard_ids: HashMap<u32, Vec<String>> = HashMap::new();
    for id in &req.ids {
        let shard_id = shard_for_key(id, topology.shards);
        shard_ids.entry(shard_id).or_default().push(id.clone());
    }

    let client = HttpClient::new(
        state.config.node_master_key.clone(),
        state.config.scatter.node_timeout_ms,
    );

    let mut quorum_state = QuorumState::default();
    let mut node_task_uids: HashMap<String, u64> = HashMap::new();

    // For each shard, write to all RF nodes in each replica group
    for (shard_id, ids) in shard_ids {
        let targets = write_targets(shard_id, &topology);

        if targets.is_empty() {
            return Err(MeilisearchError::new(
                MiroirCode::ShardUnavailable,
                format!("no available nodes for shard {}", shard_id),
            ));
        }

        for node_id in targets {
            let node = topology
                .node(&node_id)
                .ok_or_else(|| MeilisearchError::new(MiroirCode::ShardUnavailable, "node not found in topology"))?;

            let group_id = node.replica_group;
            quorum_state.record_attempt(group_id, &node_id);

            let delete_req = DeleteByIdsRequest {
                index_uid: index.clone(),
                ids: ids.clone(),
            };

            match client.delete_documents(&node_id, &node.address, &delete_req).await {
                Ok(resp) if resp.success => {
                    quorum_state.record_success(group_id, &node_id);
                    if let Some(task_uid) = resp.task_uid {
                        node_task_uids.insert(node_id.as_str().to_string(), task_uid);
                    }
                }
                Ok(resp) => {
                    return Ok(build_json_error_response(build_error_response(resp)));
                }
                Err(_) => {
                    quorum_state.record_failure(group_id);
                }
            }
        }
    }

    // Apply two-rule quorum logic
    let degraded_groups = quorum_state.count_degraded_groups(replica_group_count, rf);
    let quorum_groups = quorum_state.count_quorum_groups();

    // Write success if at least one group met quorum
    if quorum_groups == 0 {
        return Err(MeilisearchError::new(
            MiroirCode::NoQuorum,
            "no replica group met quorum",
        ));
    }

    // Register Miroir task with collected node task UIDs
    let miroir_task = state
        .task_registry
        .register_with_metadata(
            node_task_uids.clone(),
            Some(index.clone()),
            Some("documentDeletion".to_string()),
        )
        .map_err(|e| MeilisearchError::new(
            MiroirCode::ShardUnavailable,
            format!("failed to register task: {}", e),
        ))?;

    build_response_with_degraded_header(
        DocumentsWriteResponse {
            taskUid: Some(miroir_task.miroir_id),
            indexUid: Some(index.clone()),
            status: Some("enqueued".to_string()),
            error: None,
            error_type: None,
            code: None,
            link: None,
        },
        degraded_groups,
    )
}

/// Implementation for delete by filter (broadcast to all nodes).
async fn delete_by_filter_impl(
    index: String,
    req: DeleteByFilterRequest,
    state: &AppState,
) -> std::result::Result<Response, MeilisearchError> {
    let topology = state.topology.read().await;
    let rf = topology.rf();
    let replica_group_count = topology.replica_group_count();

    let client = HttpClient::new(
        state.config.node_master_key.clone(),
        state.config.scatter.node_timeout_ms,
    );

    let mut quorum_state = QuorumState::default();
    let mut node_task_uids: HashMap<String, u64> = HashMap::new();

    // Broadcast to all nodes (cannot shard-route for filters)
    for node in topology.nodes() {
        let group_id = node.replica_group;
        quorum_state.record_attempt(group_id, &node.id);

        match client
            .delete_documents_by_filter(&node.id, &node.address, &req)
            .await
        {
            Ok(resp) if resp.success => {
                quorum_state.record_success(group_id, &node.id);
                if let Some(task_uid) = resp.task_uid {
                    node_task_uids.insert(node.id.as_str().to_string(), task_uid);
                }
            }
            Ok(resp) => {
                return Ok(build_json_error_response(build_error_response(resp)));
            }
            Err(_) => {
                quorum_state.record_failure(group_id);
            }
        }
    }

    // Apply two-rule quorum logic
    let degraded_groups = quorum_state.count_degraded_groups(replica_group_count, rf);
    let quorum_groups = quorum_state.count_quorum_groups();

    // Write success if at least one group met quorum
    if quorum_groups == 0 {
        return Err(MeilisearchError::new(
            MiroirCode::NoQuorum,
            "no replica group met quorum",
        ));
    }

    // Register Miroir task with collected node task UIDs
    let miroir_task = state
        .task_registry
        .register_with_metadata(
            node_task_uids.clone(),
            Some(index.clone()),
            Some("documentDeletion".to_string()),
        )
        .map_err(|e| MeilisearchError::new(
            MiroirCode::ShardUnavailable,
            format!("failed to register task: {}", e),
        ))?;

    build_response_with_degraded_header(
        DocumentsWriteResponse {
            taskUid: Some(miroir_task.miroir_id),
            indexUid: Some(index.clone()),
            status: Some("enqueued".to_string()),
            error: None,
            error_type: None,
            code: None,
            link: None,
        },
        degraded_groups,
    )
}

/// Extract primary key from a document by checking common field names.
///
/// Tries fields in order: id, pk, key, _id.
fn extract_primary_key(doc: &Value) -> Option<String> {
    ["id", "pk", "key", "_id"]
        .iter()
        .find(|&&key| doc.get(key).is_some())
        .map(|&s| s.to_string())
}

/// Group documents by their target shard for fan-out optimization.
///
/// Returns a map of shard_id -> documents to send to that shard.
/// The caller then fans out each shard's documents to all RF nodes in each group.
///
/// This per-batch grouping minimizes HTTP fan-out count (critical at scale).
fn group_documents_by_shard(
    documents: &[Value],
    primary_key: &str,
    topology: &Topology,
) -> std::result::Result<HashMap<u32, Vec<Value>>, MeilisearchError> {
    let mut shard_documents: HashMap<u32, Vec<Value>> = HashMap::new();

    for doc in documents {
        let pk_value = doc
            .get(primary_key)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                MeilisearchError::new(
                    MiroirCode::PrimaryKeyRequired,
                    "primary key value must be a string",
                )
            })?;

        let shard_id = shard_for_key(pk_value, topology.shards);
        shard_documents
            .entry(shard_id)
            .or_default()
            .push(doc.clone());
    }

    Ok(shard_documents)
}

/// Build an error response from a node error.
fn build_error_response(resp: WriteResponse) -> DocumentsWriteResponse {
    DocumentsWriteResponse {
        taskUid: resp.task_uid.map(|uid| uid.to_string()),
        indexUid: None,
        status: None,
        error: resp.message,
        error_type: resp.error_type,
        code: resp.code,
        link: None,
    }
}

/// Build a success response with optional X-Miroir-Degraded header.
fn build_response_with_degraded_header(
    response: DocumentsWriteResponse,
    degraded_groups: u32,
) -> std::result::Result<Response, MeilisearchError> {
    let body = serde_json::to_string(&response).map_err(|e| {
        MeilisearchError::new(
            MiroirCode::ShardUnavailable,
            format!("failed to serialize response: {}", e),
        )
    })?;

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json");

    // Add X-Miroir-Degraded header if any groups were degraded
    if degraded_groups > 0 {
        builder = builder.header(HEADER_MIROIR_DEGRADED, format!("groups={}", degraded_groups));
    }

    Ok(builder
        .body(axum::body::Body::from(body))
        .map_err(|e| MeilisearchError::new(
            MiroirCode::ShardUnavailable,
            format!("failed to build response: {}", e),
        ))?)
}

/// Build an error response as JSON (for forwarded node errors).
fn build_json_error_response(resp: DocumentsWriteResponse) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        Json(resp),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_primary_key_common_fields() {
        let doc_with_id = serde_json::json!({"id": "test123", "name": "Test"});
        assert_eq!(extract_primary_key(&doc_with_id), Some("id".to_string()));

        let doc_with_pk = serde_json::json!({"pk": "test456", "name": "Test"});
        assert_eq!(extract_primary_key(&doc_with_pk), Some("pk".to_string()));

        let doc_with_key = serde_json::json!({"key": "test789", "name": "Test"});
        assert_eq!(extract_primary_key(&doc_with_key), Some("key".to_string()));

        let doc_with__id = serde_json::json!({"_id": "test000", "name": "Test"});
        assert_eq!(extract_primary_key(&doc_with__id), Some("_id".to_string()));
    }

    #[test]
    fn test_extract_primary_key_no_common_field() {
        let doc = serde_json::json!({"name": "Test", "value": 42});
        assert_eq!(extract_primary_key(&doc), None);
    }

    #[test]
    fn test_extract_primary_key_priority() {
        // Should return "id" first even if other fields exist
        let doc = serde_json::json!({"id": "test", "pk": "other", "key": "another"});
        assert_eq!(extract_primary_key(&doc), Some("id".to_string()));
    }
}
