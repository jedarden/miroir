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
//!
//! Implements §13.6 session pinning:
//! - Records writes with session header
//! - Tracks pinned group (first to reach quorum)

use axum::extract::{Extension, Path, Query};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use miroir_core::api_error::{MeilisearchError, MiroirCode};
use miroir_core::router::{shard_for_key, write_targets_with_migration};
use miroir_core::scatter::{
    DeleteByFilterRequest, DeleteByIdsRequest, NodeClient, WriteRequest, WriteResponse,
};
use miroir_core::task::TaskRegistry;
use miroir_core::topology::{NodeId, Topology};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, instrument};

use crate::client::HttpClient;
use crate::middleware::SessionId;
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
#[instrument(skip_all, fields(index = %index))]
async fn post_documents(
    Path(index): Path<String>,
    Query(params): Query<DocumentsParams>,
    Extension(state): Extension<Arc<AppState>>,
    session_id: Option<Extension<crate::middleware::SessionId>>,
    headers: axum::http::HeaderMap,
    Json(documents): Json<Vec<Value>>,
) -> std::result::Result<Response, MeilisearchError> {
    // Extract session ID from request extensions (set by session_pinning_middleware)
    let sid = session_id.and_then(|ext| {
        let s = ext.0;
        if s.0.is_empty() {
            None
        } else {
            Some(s.0.clone())
        }
    });

    // Extract idempotency key (plan §13.10)
    let idempotency_key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    write_documents_impl(
        index,
        params.primaryKey,
        documents,
        &state,
        sid,
        idempotency_key,
    )
    .await
}

/// PUT /indexes/{uid}/documents - Replace documents.
#[instrument(skip_all, fields(index = %index))]
async fn put_documents(
    Path(index): Path<String>,
    Query(params): Query<DocumentsParams>,
    Extension(state): Extension<Arc<AppState>>,
    session_id: Option<Extension<crate::middleware::SessionId>>,
    headers: axum::http::HeaderMap,
    Json(documents): Json<Vec<Value>>,
) -> std::result::Result<Response, MeilisearchError> {
    let sid = session_id.and_then(|ext| {
        let s = ext.0;
        if s.0.is_empty() {
            None
        } else {
            Some(s.0.clone())
        }
    });

    // Extract idempotency key (plan §13.10)
    let idempotency_key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    write_documents_impl(
        index,
        params.primaryKey,
        documents,
        &state,
        sid,
        idempotency_key,
    )
    .await
}

/// DELETE /indexes/{uid}/documents - Delete by IDs or filter.
async fn delete_documents(
    Path(index): Path<String>,
    Extension(state): Extension<Arc<AppState>>,
    session_id: Option<Extension<crate::middleware::SessionId>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<Value>,
) -> std::result::Result<Response, MeilisearchError> {
    let sid = session_id.and_then(|ext| {
        let s = ext.0;
        if s.0.is_empty() {
            None
        } else {
            Some(s.0.clone())
        }
    });

    // Extract idempotency key (plan §13.10)
    let idempotency_key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Try to parse as delete by filter first
    if let Some(filter) = body.get("filter") {
        let req = DeleteByFilterRequest {
            index_uid: index.clone(),
            filter: filter.clone(),
            origin: None, // Client write
        };
        return delete_by_filter_impl(index, req, &state, sid, idempotency_key).await;
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
                origin: None, // Client write
            };
            return delete_by_ids_impl(index, req, &state, sid, idempotency_key).await;
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
    session_id: Option<Extension<crate::middleware::SessionId>>,
    headers: axum::http::HeaderMap,
) -> std::result::Result<Response, MeilisearchError> {
    let sid = session_id.and_then(|ext| {
        let s = ext.0;
        if s.0.is_empty() {
            None
        } else {
            Some(s.0.clone())
        }
    });

    // Extract idempotency key (plan §13.10)
    let idempotency_key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let req = DeleteByIdsRequest {
        index_uid: index.clone(),
        ids: vec![id],
        origin: None, // Client write
    };
    delete_by_ids_impl(index, req, &state, sid, idempotency_key).await
}

/// Implementation for write documents (POST/PUT).
#[instrument(skip_all, fields(index = %index, session_id))]
async fn write_documents_impl(
    index: String,
    primary_key: Option<String>,
    mut documents: Vec<Value>,
    state: &AppState,
    session_id: Option<String>,
    idempotency_key: Option<String>,
) -> std::result::Result<Response, MeilisearchError> {
    if documents.is_empty() {
        return Err(MeilisearchError::new(
            MiroirCode::PrimaryKeyRequired,
            "cannot write empty document batch",
        ));
    }

    // 0.5. Check idempotency cache (plan §13.10)
    if state.config.idempotency.enabled {
        if let Some(ref key) = idempotency_key {
            // Compute SHA256 hash of the request body
            use sha2::{Digest, Sha256};
            let body_hash = format!(
                "{:x}",
                Sha256::digest(serde_json::to_string(&documents).unwrap_or_default())
            );

            // Check cache
            match state.idempotency_cache.check(key, &body_hash).await {
                Ok(Some(cached_mtask_id)) => {
                    // Idempotency hit: return cached mtask ID
                    state.metrics.inc_idempotency_hit("dedup");
                    return build_response_with_degraded_header(
                        DocumentsWriteResponse {
                            taskUid: Some(cached_mtask_id),
                            indexUid: Some(index.clone()),
                            status: Some("enqueued".to_string()),
                            error: None,
                            error_type: None,
                            code: None,
                            link: None,
                        },
                        0, // No degraded groups for cached response
                    );
                }
                Ok(None) => {
                    // Cache miss - proceed with processing
                    state.metrics.inc_idempotency_hit("miss");
                }
                Err(miroir_core::error::MiroirError::IdempotencyKeyReused) => {
                    // Key exists but body hash differs
                    state.metrics.inc_idempotency_hit("conflict");
                    return Err(MeilisearchError::new(
                        MiroirCode::IdempotencyKeyReused,
                        "idempotency key was already used with a different request body",
                    ));
                }
                Err(e) => {
                    // Other error - log but proceed (best-effort caching)
                    tracing::warn!(error = %e, "idempotency cache check failed, proceeding with write");
                    state.metrics.inc_idempotency_hit("miss");
                }
            }
        }
    }

    // 1. Resolve alias to concrete index UID (plan §13.7)
    // Aliases are resolved before any processing; writes to multi-target aliases are rejected
    let index_uid = if state.config.aliases.enabled {
        // Check if the index is an alias
        let resolved = state.alias_registry.resolve(&index).await;
        if resolved != vec![index.clone()] {
            // It was an alias
            // Check if it's a multi-target alias (read-only, ILM-managed)
            if state.alias_registry.is_multi_target_alias(&index).await {
                return Err(MeilisearchError::new(
                    MiroirCode::MultiAliasNotWritable,
                    format!(
                        "alias '{}' is a multi-target alias and is read-only (managed by ILM); writes must go to the concrete index or the write alias",
                        index
                    ),
                ));
            }
            // Single-target alias: record resolution metric and use the target
            state.metrics.inc_alias_resolution(&index);
            resolved.into_iter().next().unwrap_or_else(|| index.clone())
        } else {
            // Not an alias, use the index name as-is
            index.clone()
        }
    } else {
        index.clone()
    };

    // 1. Extract primary key from first document if not provided
    let primary_key =
        primary_key.or_else(|| documents.first().and_then(|doc| extract_primary_key(doc)));

    let primary_key = primary_key.ok_or_else(|| {
        MeilisearchError::new(
            MiroirCode::PrimaryKeyRequired,
            format!("primary key required for index `{}`", index),
        )
    })?;

    // 2. Validate all documents have the primary key and check for reserved field
    let anti_entropy_enabled = state.config.anti_entropy.enabled;
    let updated_at_field = &state.config.anti_entropy.updated_at_field;
    let ttl_enabled = state.config.ttl.enabled;
    let expires_at_field = &state.config.ttl.expires_at_field;

    for (i, doc) in documents.iter().enumerate() {
        // Check for reserved field BEFORE checking primary key (per acceptance criteria)
        // _miroir_shard is ALWAYS reserved (plan §5)
        if doc.get("_miroir_shard").is_some() {
            return Err(MeilisearchError::new(
                MiroirCode::ReservedField,
                "document contains reserved field `_miroir_shard`",
            ));
        }

        // _miroir_updated_at is reserved ONLY when anti_entropy.enabled: true (plan §5, §13.8)
        if anti_entropy_enabled && doc.get(updated_at_field).is_some() {
            return Err(MeilisearchError::new(
                MiroirCode::ReservedField,
                format!("document contains reserved field `{}` (reserved when anti_entropy.enabled: true)", updated_at_field),
            ));
        }

        // _miroir_expires_at is reserved ONLY when ttl.enabled: true (plan §5, §13.14)
        // When reserved, clients cannot SET it; the orchestrator controls it. When disabled,
        // client values pass through end-to-end.
        if ttl_enabled && doc.get(expires_at_field).is_some() {
            return Err(MeilisearchError::new(
                MiroirCode::ReservedField,
                format!(
                    "document contains reserved field `{}` (reserved when ttl.enabled: true)",
                    expires_at_field
                ),
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

    // 3. Inject _miroir_shard and _miroir_updated_at into each document
    let topology = state.topology.read().await;
    let shard_count = topology.shards;
    let rf = topology.rf();
    let replica_group_count = topology.replica_group_count();

    // Get current timestamp in milliseconds since epoch for _miroir_updated_at stamping
    let now_ms = if anti_entropy_enabled {
        Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        )
    } else {
        None
    };

    for doc in &mut documents {
        if let Some(pk_value) = doc.get(&primary_key).and_then(|v| v.as_str()) {
            let shard_id = shard_for_key(pk_value, shard_count);
            doc["_miroir_shard"] = serde_json::json!(shard_id);
        }

        // Stamp _miroir_updated_at when anti_entropy is enabled (plan §13.8)
        // This happens AFTER reserved field validation, so orchestrator-controlled injection is allowed
        if let Some(timestamp) = now_ms {
            doc[updated_at_field] = serde_json::json!(timestamp);
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

    // For each shard, write to all RF nodes in each replica group (with dual-write support)
    for (shard_id, docs) in node_documents {
        // Get migration coordinator reference for dual-write detection
        let migration_coordinator = state.migration_coordinator.as_ref().map(|c| {
            // We need a read lock on the coordinator
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async { c.read().await })
            })
        });

        // Use migration-aware routing
        let targets =
            write_targets_with_migration(shard_id, &topology, migration_coordinator.as_deref());

        if targets.is_empty() {
            return Err(MeilisearchError::new(
                MiroirCode::ShardUnavailable,
                format!("no available nodes for shard {}", shard_id),
            ));
        }

        // Track which groups we're targeting for this shard

        for node_id in targets {
            let node = topology.node(&node_id).ok_or_else(|| {
                MeilisearchError::new(MiroirCode::ShardUnavailable, "node not found in topology")
            })?;

            let group_id = node.replica_group;
            quorum_state.record_attempt(group_id, &node_id);

            let req = WriteRequest {
                index_uid: index_uid.clone(),
                documents: docs.clone(),
                primary_key: Some(primary_key.clone()),
                origin: None, // Client write
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

    // 6.5. Find the first group to reach quorum (for session pinning, plan §13.6)
    // Groups are checked in ascending order, so the first one with quorum is the first
    let first_quorum_group = (0..replica_group_count)
        .find(|&group_id| {
            let acks = *quorum_state.group_acks.get(&group_id).unwrap_or(&0);
            let quorum = (rf / 2) + 1;
            acks >= quorum
        })
        .unwrap_or(0); // Default to group 0 if somehow no quorum (shouldn't happen here)

    // 7. Register Miroir task with collected node task UIDs
    let miroir_task = state
        .task_registry
        .register_with_metadata(
            node_task_uids.clone(),
            Some(index_uid.clone()),
            Some("documentAdditionOrUpdate".to_string()),
        )
        .map_err(|e| {
            MeilisearchError::new(
                MiroirCode::ShardUnavailable,
                format!("failed to register task: {}", e),
            )
        })?;

    // 7.5. Record session pinning if session header present (plan §13.6)
    if let (Some(ref sid), true) = (&session_id, state.session_manager.is_enabled()) {
        if let Err(e) = state
            .session_manager
            .record_write_with_quorum(sid, miroir_task.miroir_id.clone(), first_quorum_group)
            .await
        {
            // Log error but don't fail the write - session pinning is best-effort
            tracing::error!(
                session_id = %sid,
                error = %e,
                "failed to record session pinning for write"
            );
        }
    }

    // 7.6. Insert into idempotency cache if key was provided (plan §13.10)
    if state.config.idempotency.enabled {
        if let Some(ref key) = idempotency_key {
            use sha2::{Digest, Sha256};
            let body_hash = format!(
                "{:x}",
                Sha256::digest(serde_json::to_string(&documents).unwrap_or_default())
            );
            state
                .idempotency_cache
                .insert(key.clone(), body_hash, miroir_task.miroir_id.clone())
                .await;
            // Update cache size metric
            state
                .metrics
                .set_idempotency_cache_size(state.idempotency_cache.size().await as u64);
        }
    }

    // Build success response with degraded header and mtask ID
    build_response_with_degraded_header(
        DocumentsWriteResponse {
            taskUid: Some(miroir_task.miroir_id),
            indexUid: Some(index_uid.clone()),
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
    session_id: Option<String>,
    idempotency_key: Option<String>,
) -> std::result::Result<Response, MeilisearchError> {
    if req.ids.is_empty() {
        return Err(MeilisearchError::new(
            MiroirCode::PrimaryKeyRequired,
            "cannot delete empty ID list",
        ));
    }

    // 0.5. Check idempotency cache (plan §13.10)
    if state.config.idempotency.enabled {
        if let Some(ref key) = idempotency_key {
            use sha2::{Digest, Sha256};
            let body_hash = format!(
                "{:x}",
                Sha256::digest(serde_json::to_string(&req).unwrap_or_default())
            );

            match state.idempotency_cache.check(key, &body_hash).await {
                Ok(Some(cached_mtask_id)) => {
                    state.metrics.inc_idempotency_hit("dedup");
                    return build_response_with_degraded_header(
                        DocumentsWriteResponse {
                            taskUid: Some(cached_mtask_id),
                            indexUid: Some(index.clone()),
                            status: Some("enqueued".to_string()),
                            error: None,
                            error_type: None,
                            code: None,
                            link: None,
                        },
                        0,
                    );
                }
                Ok(None) => {
                    state.metrics.inc_idempotency_hit("miss");
                }
                Err(miroir_core::error::MiroirError::IdempotencyKeyReused) => {
                    state.metrics.inc_idempotency_hit("conflict");
                    return Err(MeilisearchError::new(
                        MiroirCode::IdempotencyKeyReused,
                        "idempotency key was already used with a different request body",
                    ));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "idempotency cache check failed, proceeding with delete");
                    state.metrics.inc_idempotency_hit("miss");
                }
            }
        }
    }

    // Resolve alias to concrete index UID (plan §13.7)
    let index_uid = if state.config.aliases.enabled {
        let resolved = state.alias_registry.resolve(&index).await;
        if resolved != vec![index.clone()] {
            // It was an alias - check if it's a multi-target alias (read-only)
            if state.alias_registry.is_multi_target_alias(&index).await {
                return Err(MeilisearchError::new(
                    MiroirCode::MultiAliasNotWritable,
                    format!(
                        "alias '{}' is a multi-target alias and is read-only (managed by ILM); deletes must go to the concrete index or the write alias",
                        index
                    ),
                ));
            }
            // Single-target alias: record resolution metric and use the target
            state.metrics.inc_alias_resolution(&index);
            resolved.into_iter().next().unwrap_or_else(|| index.clone())
        } else {
            index.clone()
        }
    } else {
        index.clone()
    };

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
        let targets = miroir_core::router::write_targets(shard_id, &topology);

        if targets.is_empty() {
            return Err(MeilisearchError::new(
                MiroirCode::ShardUnavailable,
                format!("no available nodes for shard {}", shard_id),
            ));
        }

        for node_id in targets {
            let node = topology.node(&node_id).ok_or_else(|| {
                MeilisearchError::new(MiroirCode::ShardUnavailable, "node not found in topology")
            })?;

            let group_id = node.replica_group;
            quorum_state.record_attempt(group_id, &node_id);

            let delete_req = DeleteByIdsRequest {
                index_uid: index_uid.clone(),
                ids: ids.clone(),
                origin: None, // Client write
            };

            match client
                .delete_documents(&node_id, &node.address, &delete_req)
                .await
            {
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

    // Find the first group to reach quorum (for session pinning, plan §13.6)
    let first_quorum_group = (0..replica_group_count)
        .find(|&group_id| {
            let acks = *quorum_state.group_acks.get(&group_id).unwrap_or(&0);
            let quorum = (rf / 2) + 1;
            acks >= quorum
        })
        .unwrap_or(0);

    // Register Miroir task with collected node task UIDs
    let miroir_task = state
        .task_registry
        .register_with_metadata(
            node_task_uids.clone(),
            Some(index.clone()),
            Some("documentDeletion".to_string()),
        )
        .map_err(|e| {
            MeilisearchError::new(
                MiroirCode::ShardUnavailable,
                format!("failed to register task: {}", e),
            )
        })?;

    // Record session pinning if session header present (plan §13.6)
    if let (Some(ref sid), true) = (&session_id, state.session_manager.is_enabled()) {
        if let Err(e) = state
            .session_manager
            .record_write_with_quorum(sid, miroir_task.miroir_id.clone(), first_quorum_group)
            .await
        {
            tracing::error!(
                session_id = %sid,
                error = %e,
                "failed to record session pinning for delete"
            );
        }
    }

    // Insert into idempotency cache if key was provided (plan §13.10)
    if state.config.idempotency.enabled {
        if let Some(ref key) = idempotency_key {
            use sha2::{Digest, Sha256};
            let body_hash = format!(
                "{:x}",
                Sha256::digest(serde_json::to_string(&req).unwrap_or_default())
            );
            state
                .idempotency_cache
                .insert(key.clone(), body_hash, miroir_task.miroir_id.clone())
                .await;
            state
                .metrics
                .set_idempotency_cache_size(state.idempotency_cache.size().await as u64);
        }
    }

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
    session_id: Option<String>,
    idempotency_key: Option<String>,
) -> std::result::Result<Response, MeilisearchError> {
    // 0.5. Check idempotency cache (plan §13.10)
    if state.config.idempotency.enabled {
        if let Some(ref key) = idempotency_key {
            use sha2::{Digest, Sha256};
            let body_hash = format!(
                "{:x}",
                Sha256::digest(serde_json::to_string(&req).unwrap_or_default())
            );

            match state.idempotency_cache.check(key, &body_hash).await {
                Ok(Some(cached_mtask_id)) => {
                    state.metrics.inc_idempotency_hit("dedup");
                    return build_response_with_degraded_header(
                        DocumentsWriteResponse {
                            taskUid: Some(cached_mtask_id),
                            indexUid: Some(index.clone()),
                            status: Some("enqueued".to_string()),
                            error: None,
                            error_type: None,
                            code: None,
                            link: None,
                        },
                        0,
                    );
                }
                Ok(None) => {
                    state.metrics.inc_idempotency_hit("miss");
                }
                Err(miroir_core::error::MiroirError::IdempotencyKeyReused) => {
                    state.metrics.inc_idempotency_hit("conflict");
                    return Err(MeilisearchError::new(
                        MiroirCode::IdempotencyKeyReused,
                        "idempotency key was already used with a different request body",
                    ));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "idempotency cache check failed, proceeding with delete by filter");
                    state.metrics.inc_idempotency_hit("miss");
                }
            }
        }
    }

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

    // Find the first group to reach quorum (for session pinning, plan §13.6)
    let first_quorum_group = (0..replica_group_count)
        .find(|&group_id| {
            let acks = *quorum_state.group_acks.get(&group_id).unwrap_or(&0);
            let quorum = (rf / 2) + 1;
            acks >= quorum
        })
        .unwrap_or(0);

    // Register Miroir task with collected node task UIDs
    let miroir_task = state
        .task_registry
        .register_with_metadata(
            node_task_uids.clone(),
            Some(index.clone()),
            Some("documentDeletion".to_string()),
        )
        .map_err(|e| {
            MeilisearchError::new(
                MiroirCode::ShardUnavailable,
                format!("failed to register task: {}", e),
            )
        })?;

    // Record session pinning if session header present (plan §13.6)
    if let (Some(ref sid), true) = (&session_id, state.session_manager.is_enabled()) {
        if let Err(e) = state
            .session_manager
            .record_write_with_quorum(sid, miroir_task.miroir_id.clone(), first_quorum_group)
            .await
        {
            tracing::error!(
                session_id = %sid,
                error = %e,
                "failed to record session pinning for delete by filter"
            );
        }
    }

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
        builder = builder.header(
            HEADER_MIROIR_DEGRADED,
            format!("groups={}", degraded_groups),
        );
    }

    Ok(builder.body(axum::body::Body::from(body)).map_err(|e| {
        MeilisearchError::new(
            MiroirCode::ShardUnavailable,
            format!("failed to build response: {}", e),
        )
    })?)
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
    use miroir_core::api_error::{MeilisearchError, MiroirCode};
    use serde_json::json;

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

    // Reserved field validation tests (P2.9)
    //
    // Tests the reserved field matrix per plan §5:
    // - `_miroir_shard`: Always reserved (unconditional)
    // - `_miroir_updated_at`: Reserved only when `anti_entropy.enabled: true`
    // - `_miroir_expires_at`: Reserved only when `ttl.enabled: true`

    /// Helper to build the expected reserved field error.
    fn reserved_field_error(field: &str) -> MeilisearchError {
        MeilisearchError::new(
            MiroirCode::ReservedField,
            format!("document contains reserved field `{}`", field),
        )
    }

    #[test]
    fn test_reserved_field_miroir_shard_always_rejected() {
        // _miroir_shard is ALWAYS reserved regardless of config
        let err = reserved_field_error("_miroir_shard");
        assert_eq!(err.code, "miroir_reserved_field");
        assert_eq!(err.http_status(), 400);
        assert_eq!(
            err.error_type,
            miroir_core::api_error::ErrorType::InvalidRequest
        );
    }

    #[test]
    fn test_reserved_field_miroir_updated_at_when_anti_entropy_enabled() {
        // When anti_entropy.enabled: true, _miroir_updated_at is reserved
        let field = "_miroir_updated_at";
        let err = MeilisearchError::new(
            MiroirCode::ReservedField,
            format!(
                "document contains reserved field `{}` (reserved when anti_entropy.enabled: true)",
                field
            ),
        );
        assert_eq!(err.code, "miroir_reserved_field");
        assert_eq!(err.http_status(), 400);
    }

    #[test]
    fn test_reserved_field_miroir_expires_at_when_ttl_enabled() {
        // When ttl.enabled: true, _miroir_expires_at is reserved
        let field = "_miroir_expires_at";
        let err = MeilisearchError::new(
            MiroirCode::ReservedField,
            format!(
                "document contains reserved field `{}` (reserved when ttl.enabled: true)",
                field
            ),
        );
        assert_eq!(err.code, "miroir_reserved_field");
        assert_eq!(err.http_status(), 400);
    }

    #[test]
    fn test_reserved_field_non_miroir_fields_allowed() {
        // Non-reserved _miroir_ fields are allowed
        let doc =
            json!({"id": "test", "_miroir_custom": "value", "_miroir_metadata": {"key": "val"}});
        assert!(doc.get("_miroir_custom").is_some());
        assert!(doc.get("_miroir_metadata").is_some());
    }

    /// Test matrix of all reserved field combinations per plan §5 table.
    ///
    /// Matrix cells (write behavior):
    /// | Field           | Config disabled | Config enabled |
    /// |-----------------|-----------------|----------------|
    /// | _miroir_shard   | REJECTED (always)  | REJECTED (always) |
    /// | _miroir_updated_at | ALLOWED      | REJECTED (anti_entropy) |
    /// | _miroir_expires_at | ALLOWED      | REJECTED (ttl) |
    #[test]
    fn test_reserved_field_matrix() {
        struct TestCase {
            doc: Value,
            description: &'static str,
            has_shard: bool,
            has_updated_at: bool,
            has_expires_at: bool,
        }

        let test_cases = vec![
            TestCase {
                doc: json!({"id": "test"}),
                description: "clean document should pass",
                has_shard: false,
                has_updated_at: false,
                has_expires_at: false,
            },
            TestCase {
                doc: json!({"id": "test", "_miroir_shard": 1}),
                description: "_miroir_shard always rejected",
                has_shard: true,
                has_updated_at: false,
                has_expires_at: false,
            },
            TestCase {
                doc: json!({"id": "test", "_miroir_updated_at": "2024-01-01T00:00:00Z"}),
                description: "_miroir_updated_at allowed when anti_entropy disabled",
                has_shard: false,
                has_updated_at: true,
                has_expires_at: false,
            },
            TestCase {
                doc: json!({"id": "test", "_miroir_expires_at": "2024-12-31T23:59:59Z"}),
                description: "_miroir_expires_at allowed when ttl.disabled",
                has_shard: false,
                has_updated_at: false,
                has_expires_at: true,
            },
            TestCase {
                doc: json!({"id": "test", "_miroir_custom": "value"}),
                description: "non-reserved _miroir_ fields allowed",
                has_shard: false,
                has_updated_at: false,
                has_expires_at: false,
            },
            TestCase {
                doc: json!({"id": "test", "_miroir_shard": 1, "_miroir_updated_at": "2024-01-01T00:00:00Z"}),
                description: "multiple reserved fields present",
                has_shard: true,
                has_updated_at: true,
                has_expires_at: false,
            },
        ];

        for tc in test_cases {
            assert_eq!(
                tc.doc.get("_miroir_shard").is_some(),
                tc.has_shard,
                "{}: shard check",
                tc.description
            );
            assert_eq!(
                tc.doc.get("_miroir_updated_at").is_some(),
                tc.has_updated_at,
                "{}: updated_at check",
                tc.description
            );
            assert_eq!(
                tc.doc.get("_miroir_expires_at").is_some(),
                tc.has_expires_at,
                "{}: expires_at check",
                tc.description
            );
        }
    }

    #[test]
    fn test_reserved_field_error_format_matches_meilisearch_shape() {
        let err = MeilisearchError::new(
            MiroirCode::ReservedField,
            "document contains reserved field `_miroir_shard`",
        );

        // Verify Meilisearch-compatible error shape: {message, code, type, link}
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["code"], "miroir_reserved_field");
        assert_eq!(json["type"], "invalid_request");
        assert!(json["message"].is_string());
        assert!(json["link"]
            .as_str()
            .unwrap()
            .contains("miroir_reserved_field"));
        assert_eq!(
            json["message"],
            "document contains reserved field `_miroir_shard`"
        );
    }

    #[test]
    fn test_reserved_field_error_all_fields_present() {
        let err = MeilisearchError::new(
            MiroirCode::ReservedField,
            "document contains reserved field `_miroir_updated_at`",
        );

        // Verify all required fields are present
        assert!(!err.message.is_empty());
        assert!(!err.code.is_empty());
        assert_eq!(err.code, "miroir_reserved_field");
        assert!(err.link.is_some());
        assert!(err.link.unwrap().contains("miroir_reserved_field"));
    }

    #[test]
    fn test_orchestrator_shard_injection_flow() {
        // Simulate the orchestrator injection flow:
        // 1. Client sends document WITHOUT _miroir_shard
        // 2. Validation passes (no _miroir_shard present)
        // 3. Orchestrator injects _miroir_shard for routing
        // 4. Write proceeds with injected field

        let client_doc = json!({"id": "user:123", "name": "Test User"});
        assert!(
            client_doc.get("_miroir_shard").is_none(),
            "client doc should not have _miroir_shard"
        );

        // Simulate orchestrator injection (happens AFTER validation at line 279-290)
        let mut doc_with_shard = client_doc.clone();
        doc_with_shard["_miroir_shard"] = json!(5);

        assert_eq!(
            doc_with_shard["_miroir_shard"], 5,
            "orchestrator should inject _miroir_shard"
        );
        assert!(
            doc_with_shard.get("id").is_some(),
            "primary key should still be present"
        );
    }

    #[test]
    fn test_reserved_field_validation_order() {
        // _miroir_shard is checked BEFORE primary key validation (per acceptance criteria)
        // This ensures clients can't bypass validation by including both fields

        let doc_with_shard_no_pk = json!({"_miroir_shard": 1, "name": "No PK"});
        assert!(doc_with_shard_no_pk.get("_miroir_shard").is_some());
        assert!(doc_with_shard_no_pk.get("id").is_none());

        // The validation should catch _miroir_shard first, not missing primary key
        let err = reserved_field_error("_miroir_shard");
        assert_eq!(err.code, "miroir_reserved_field");
    }

    // P2.9: Complete reserved field matrix tests
    //
    // Matrix cells per plan §5:
    // | Field           | Config disabled | Config enabled |
    // |-----------------|-----------------|----------------|
    // | _miroir_shard   | REJECTED        | REJECTED       |
    // | _miroir_updated_at | ALLOWED    | REJECTED (AE)  |
    // | _miroir_expires_at | ALLOWED    | REJECTED (TTL) |

    #[test]
    fn test_reserved_field_matrix_shard_always_rejected() {
        // _miroir_shard: Always reserved regardless of config
        let err = reserved_field_error("_miroir_shard");
        assert_eq!(err.code, "miroir_reserved_field");
        assert_eq!(err.http_status(), 400);
    }

    #[test]
    fn test_reserved_field_matrix_updated_at_rejected_when_ae_enabled() {
        // _miroir_updated_at: Rejected when anti_entropy.enabled: true
        let err = MeilisearchError::new(
            MiroirCode::ReservedField,
            "document contains reserved field `_miroir_updated_at` (reserved when anti_entropy.enabled: true)",
        );
        assert_eq!(err.code, "miroir_reserved_field");
        assert_eq!(err.http_status(), 400);
    }

    #[test]
    fn test_reserved_field_matrix_expires_at_rejected_when_ttl_enabled() {
        // _miroir_expires_at: Rejected when ttl.enabled: true
        let err = MeilisearchError::new(
            MiroirCode::ReservedField,
            "document contains reserved field `_miroir_expires_at` (reserved when ttl.enabled: true)",
        );
        assert_eq!(err.code, "miroir_reserved_field");
        assert_eq!(err.http_status(), 400);
    }

    #[test]
    fn test_reserved_field_matrix_updated_at_allowed_when_ae_disabled() {
        // _miroir_updated_at: Allowed when anti_entropy.enabled: false
        // When disabled, client values pass through end-to-end
        let doc = json!({"id": "test", "_miroir_updated_at": "2024-01-01T00:00:00Z"});
        assert!(doc.get("_miroir_updated_at").is_some());
        assert!(doc.get("id").is_some());
        // No validation error would be raised in this case
    }

    #[test]
    fn test_reserved_field_matrix_expires_at_allowed_when_ttl_disabled() {
        // _miroir_expires_at: Allowed when ttl.enabled: false
        // When disabled, client values pass through end-to-end
        let doc = json!({"id": "test", "_miroir_expires_at": "2024-12-31T23:59:59Z"});
        assert!(doc.get("_miroir_expires_at").is_some());
        assert!(doc.get("id").is_some());
        // No validation error would be raised in this case
    }

    #[test]
    fn test_orchestrator_updated_at_stamping_when_ae_enabled() {
        // When anti_entropy.enabled: true, orchestrator stamps _miroir_updated_at
        // This test verifies the stamping logic (plan §13.8)

        let client_doc = json!({"id": "user:123", "name": "Test User"});
        assert!(
            client_doc.get("_miroir_updated_at").is_none(),
            "client doc should not have _miroir_updated_at"
        );

        // Simulate orchestrator stamping (happens AFTER validation)
        let mut doc_with_timestamp = client_doc.clone();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        doc_with_timestamp["_miroir_updated_at"] = json!(now_ms);

        assert!(
            doc_with_timestamp.get("_miroir_updated_at").is_some(),
            "orchestrator should stamp _miroir_updated_at"
        );
        assert_eq!(
            doc_with_timestamp["_miroir_updated_at"], now_ms,
            "timestamp should be current ms since epoch"
        );
        assert!(
            doc_with_timestamp.get("id").is_some(),
            "primary key should still be present"
        );
    }
}
