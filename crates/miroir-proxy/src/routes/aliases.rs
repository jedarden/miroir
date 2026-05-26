//! Atomic index alias management endpoints (plan §13.7).
#![allow(dead_code)]

use crate::middleware::Metrics;
use axum::{
    extract::{FromRef, Path, State},
    http::StatusCode,
    Json,
};
use miroir_core::{alias::AliasKind, config::MiroirConfig, task_store::TaskStore};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Check if an index exists on all Meilisearch nodes.
///
/// Returns Ok(()) if the index exists on all nodes, or an error if any node
/// does not have the index or is unreachable.
async fn check_index_exists_on_all_nodes(
    node_addresses: &[String],
    master_key: &str,
    index_uid: &str,
) -> Result<(), String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| format!("failed to create HTTP client: {e}"))?;

    // Check each node
    for address in node_addresses {
        let base = address.trim_end_matches('/');
        let url = format!("{base}/indexes/{index_uid}");

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {master_key}"))
            .send()
            .await
            .map_err(|e| format!("node {address} unreachable: {e}"))?;

        let status = response.status();
        if status.as_u16() == 404 {
            return Err(format!(
                "index '{index_uid}' does not exist on node {address}"
            ));
        } else if !status.is_success() {
            let body_text = response
                .text()
                .await
                .unwrap_or_else(|e| format!("(failed to read error body: {e})"));
            return Err(format!(
                "node {address} returned HTTP {} for index '{index_uid}': {}",
                status.as_u16(),
                body_text.trim()
            ));
        }
    }

    Ok(())
}

/// Alias management state.
#[derive(Clone)]
pub struct AliasState {
    pub config: Arc<MiroirConfig>,
    pub task_store: Option<Arc<dyn TaskStore>>,
    pub metrics: Metrics,
}

/// Request body for POST /_miroir/aliases.
#[derive(Debug, Deserialize)]
pub struct CreateAliasRequest {
    /// Single target (creates single-target alias)
    pub target: Option<String>,
    /// Multiple targets (creates multi-target alias)
    pub targets: Option<Vec<String>>,
}

/// Request body for PUT /_miroir/aliases/{name}.
#[derive(Debug, Deserialize)]
pub struct UpdateAliasRequest {
    /// New target for single-target alias flip
    pub target: Option<String>,
    /// New targets for multi-target alias update (ILM-only)
    pub targets: Option<Vec<String>>,
}

/// Response for GET /_miroir/aliases/{name}.
#[derive(Debug, Serialize)]
pub struct GetAliasResponse {
    pub name: String,
    pub kind: String,
    pub current_uid: Option<String>,
    pub target_uids: Option<Vec<String>>,
    pub version: u64,
    pub created_at: u64,
    pub history: Vec<AliasHistoryEntry>,
}

#[derive(Debug, Serialize)]
pub struct AliasHistoryEntry {
    pub uid: String,
    pub flipped_at: u64,
}

/// Response for LIST /_miroir/aliases.
#[derive(Debug, Serialize)]
pub struct ListAliasesResponse {
    pub aliases: Vec<AliasInfo>,
}

#[derive(Debug, Serialize)]
pub struct AliasInfo {
    pub name: String,
    pub kind: String,
    pub current_uid: Option<String>,
    pub target_uids: Option<Vec<String>>,
    pub version: u64,
}

/// Error response for 409 conflicts.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

/// POST /_miroir/aliases/{name} — create a new alias.
///
/// Request body:
/// - Single-target: `{"target": "products_v3"}`
/// - Multi-target: `{"targets": ["logs-2026-01-01", "logs-2026-01-02"]}`
///
/// Plan §13.7: Atomic index aliases for blue-green reindexing.
pub async fn create_alias<S>(
    State(state): State<AliasState>,
    Path(name): Path<String>,
    Json(body): Json<CreateAliasRequest>,
) -> Result<Json<GetAliasResponse>, (StatusCode, Json<ErrorResponse>)>
where
    S: Clone + Send + Sync + 'static,
    AliasState: FromRef<S>,
{
    if !state.config.aliases.enabled {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorResponse {
                code: "feature_disabled".to_string(),
                message: "aliases feature is disabled".to_string(),
            }),
        ));
    }

    let task_store = state.task_store.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                code: "task_store_unavailable".to_string(),
                message: "task store required for aliases".to_string(),
            }),
        )
    })?;

    // Determine alias kind from request body
    let (kind, current_uid, target_uids) = match (&body.target, &body.targets) {
        (Some(target), None) => (AliasKind::Single, Some(target.clone()), None),
        (None, Some(targets)) => (AliasKind::Multi, None, Some(targets.clone())),
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    code: "invalid_request".to_string(),
                    message: "must provide either 'target' (single) or 'targets' (multi)"
                        .to_string(),
                }),
            ));
        }
    };

    // Validate target existence if required
    if state.config.aliases.require_target_exists {
        let node_addresses: Vec<String> = state
            .config
            .nodes
            .iter()
            .map(|n| n.address.clone())
            .collect();
        let master_key = &state.config.node_master_key;

        match &kind {
            AliasKind::Single => {
                if let Some(target) = &body.target {
                    if let Err(e) =
                        check_index_exists_on_all_nodes(&node_addresses, master_key, target).await
                    {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                code: "index_not_found".to_string(),
                                message: e,
                            }),
                        ));
                    }
                }
            }
            AliasKind::Multi => {
                if let Some(targets) = &body.targets {
                    for target in targets {
                        if let Err(e) =
                            check_index_exists_on_all_nodes(&node_addresses, master_key, target)
                                .await
                        {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                Json(ErrorResponse {
                                    code: "index_not_found".to_string(),
                                    message: e,
                                }),
                            ));
                        }
                    }
                }
            }
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Check for conflicts with ILM-managed aliases
    if let Ok(Some(existing)) = task_store.get_alias(&name) {
        if existing.kind == "multi" {
            // Multi-target aliases are ILM-managed and cannot be created by operators
            return Err((
                StatusCode::CONFLICT,
                Json(ErrorResponse {
                    code: "alias_exists_ilm_managed".to_string(),
                    message: format!(
                        "alias '{name}' exists and is managed by ILM policy; use ILM API to modify"
                    ),
                }),
            ));
        }
    }

    let new_alias = miroir_core::task_store::NewAlias {
        name: name.clone(),
        kind: if matches!(kind, AliasKind::Single) {
            "single".to_string()
        } else {
            "multi".to_string()
        },
        current_uid,
        target_uids,
        version: 1,
        created_at: now as i64,
        history: vec![],
    };

    task_store.create_alias(&new_alias).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                code: "alias_creation_failed".to_string(),
                message: format!("failed to create alias: {e}"),
            }),
        )
    })?;

    Ok(Json(GetAliasResponse {
        name: new_alias.name,
        kind: new_alias.kind,
        current_uid: new_alias.current_uid,
        target_uids: new_alias.target_uids,
        version: new_alias.version as u64,
        created_at: new_alias.created_at as u64,
        history: vec![],
    }))
}

/// GET /_miroir/aliases/{name} — get alias details including history.
pub async fn get_alias<S>(
    State(state): State<AliasState>,
    Path(name): Path<String>,
) -> Result<Json<GetAliasResponse>, (StatusCode, Json<ErrorResponse>)>
where
    S: Clone + Send + Sync + 'static,
    AliasState: FromRef<S>,
{
    if !state.config.aliases.enabled {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorResponse {
                code: "feature_disabled".to_string(),
                message: "aliases feature is disabled".to_string(),
            }),
        ));
    }

    let task_store = state.task_store.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                code: "task_store_unavailable".to_string(),
                message: "task store required for aliases".to_string(),
            }),
        )
    })?;

    let alias = task_store.get_alias(&name).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                code: "alias_lookup_failed".to_string(),
                message: format!("failed to lookup alias: {e}"),
            }),
        )
    })?;

    match alias {
        Some(alias) => {
            let history = alias
                .history
                .into_iter()
                .map(|entry| AliasHistoryEntry {
                    uid: entry.uid,
                    flipped_at: entry.flipped_at as u64,
                })
                .collect();

            Ok(Json(GetAliasResponse {
                name: alias.name,
                kind: alias.kind,
                current_uid: alias.current_uid,
                target_uids: alias.target_uids,
                version: alias.version as u64,
                created_at: alias.created_at as u64,
                history,
            }))
        }
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                code: "alias_not_found".to_string(),
                message: format!("alias '{name}' not found"),
            }),
        )),
    }
}

/// PUT /_miroir/aliases/{name} — update an alias (flip single or update multi).
///
/// Request body for single-target flip:
/// - `{"target": "products_v4"}`
///
/// Request body for multi-target update (ILM-only):
/// - `{"targets": ["logs-2026-01-03", "logs-2026-01-02"]}`
pub async fn update_alias<S>(
    State(state): State<AliasState>,
    Path(name): Path<String>,
    Json(body): Json<UpdateAliasRequest>,
) -> Result<Json<GetAliasResponse>, (StatusCode, Json<ErrorResponse>)>
where
    S: Clone + Send + Sync + 'static,
    AliasState: FromRef<S>,
{
    if !state.config.aliases.enabled {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorResponse {
                code: "feature_disabled".to_string(),
                message: "aliases feature is disabled".to_string(),
            }),
        ));
    }

    let task_store = state.task_store.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                code: "task_store_unavailable".to_string(),
                message: "task store required for aliases".to_string(),
            }),
        )
    })?;

    // Get existing alias
    let existing = task_store.get_alias(&name).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                code: "alias_lookup_failed".to_string(),
                message: format!("failed to lookup alias: {e}"),
            }),
        )
    })?;

    let existing = existing.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                code: "alias_not_found".to_string(),
                message: format!("alias '{name}' not found"),
            }),
        )
    })?;

    // Handle single-target alias flip
    if existing.kind == "single" {
        let new_target = body.target.ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    code: "invalid_request".to_string(),
                    message: "single-target alias requires 'target' field".to_string(),
                }),
            )
        })?;

        // Validate target existence if required
        if state.config.aliases.require_target_exists {
            let node_addresses: Vec<String> = state
                .config
                .nodes
                .iter()
                .map(|n| n.address.clone())
                .collect();
            let master_key = &state.config.node_master_key;

            if let Err(e) =
                check_index_exists_on_all_nodes(&node_addresses, master_key, &new_target).await
            {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        code: "index_not_found".to_string(),
                        message: e,
                    }),
                ));
            }
        }

        // Perform the atomic flip
        task_store
            .flip_alias(
                &name,
                &new_target,
                state.config.aliases.history_retention as usize,
            )
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        code: "alias_flip_failed".to_string(),
                        message: format!("failed to flip alias: {e}"),
                    }),
                )
            })?;

        // Record alias flip metric
        state.metrics.inc_alias_flip(&name);

        // Get updated alias
        let updated = task_store
            .get_alias(&name)
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        code: "alias_lookup_failed".to_string(),
                        message: format!("failed to lookup updated alias: {e}"),
                    }),
                )
            })?
            .unwrap();

        let history = updated
            .history
            .into_iter()
            .map(|entry| AliasHistoryEntry {
                uid: entry.uid,
                flipped_at: entry.flipped_at as u64,
            })
            .collect();

        Ok(Json(GetAliasResponse {
            name: updated.name,
            kind: updated.kind,
            current_uid: updated.current_uid,
            target_uids: updated.target_uids,
            version: updated.version as u64,
            created_at: updated.created_at as u64,
            history,
        }))
    } else {
        // Handle multi-target alias update (ILM-only)
        // Reject operator edits to ILM-managed multi-target aliases
        Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                code: "miroir_multi_alias_not_writable".to_string(),
                message: "multi-target aliases are managed exclusively by ILM; use the ILM policy API to modify".to_string(),
            }),
        ))
    }
}

/// DELETE /_miroir/aliases/{name} — delete an alias.
pub async fn delete_alias<S>(
    State(state): State<AliasState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)>
where
    S: Clone + Send + Sync + 'static,
    AliasState: FromRef<S>,
{
    if !state.config.aliases.enabled {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorResponse {
                code: "feature_disabled".to_string(),
                message: "aliases feature is disabled".to_string(),
            }),
        ));
    }

    let task_store = state.task_store.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                code: "task_store_unavailable".to_string(),
                message: "task store required for aliases".to_string(),
            }),
        )
    })?;

    let deleted = task_store.delete_alias(&name).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                code: "alias_deletion_failed".to_string(),
                message: format!("failed to delete alias: {e}"),
            }),
        )
    })?;

    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                code: "alias_not_found".to_string(),
                message: format!("alias '{name}' not found"),
            }),
        ))
    }
}

/// GET /_miroir/aliases — list all aliases.
pub async fn list_aliases<S>(
    State(state): State<AliasState>,
) -> Result<Json<ListAliasesResponse>, (StatusCode, Json<ErrorResponse>)>
where
    S: Clone + Send + Sync + 'static,
    AliasState: FromRef<S>,
{
    if !state.config.aliases.enabled {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorResponse {
                code: "feature_disabled".to_string(),
                message: "aliases feature is disabled".to_string(),
            }),
        ));
    }

    let task_store = state.task_store.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                code: "task_store_unavailable".to_string(),
                message: "task store required for aliases".to_string(),
            }),
        )
    })?;

    let aliases = task_store.list_aliases().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                code: "alias_list_failed".to_string(),
                message: format!("failed to list aliases: {e}"),
            }),
        )
    })?;

    let alias_infos: Vec<AliasInfo> = aliases
        .into_iter()
        .map(|alias| AliasInfo {
            name: alias.name,
            kind: alias.kind,
            current_uid: alias.current_uid,
            target_uids: alias.target_uids,
            version: alias.version as u64,
        })
        .collect();

    Ok(Json(ListAliasesResponse {
        aliases: alias_infos,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_alias_request_single() {
        let json = r#"{"target": "products_v3"}"#;
        let req: CreateAliasRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.target, Some("products_v3".to_string()));
        assert!(req.targets.is_none());
    }

    #[test]
    fn test_create_alias_request_multi() {
        let json = r#"{"targets": ["logs-2026-01-01", "logs-2026-01-02"]}"#;
        let req: CreateAliasRequest = serde_json::from_str(json).unwrap();
        assert_eq!(
            req.targets,
            Some(vec![
                "logs-2026-01-01".to_string(),
                "logs-2026-01-02".to_string()
            ])
        );
        assert!(req.target.is_none());
    }

    #[test]
    fn test_update_alias_request() {
        let json = r#"{"target": "products_v4"}"#;
        let req: UpdateAliasRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.target, Some("products_v4".to_string()));
        assert!(req.targets.is_none());
    }

    #[test]
    fn test_get_alias_response_serialization() {
        let response = GetAliasResponse {
            name: "products".to_string(),
            kind: "single".to_string(),
            current_uid: Some("products_v3".to_string()),
            target_uids: None,
            version: 5,
            created_at: 1704067200,
            history: vec![
                AliasHistoryEntry {
                    uid: "products_v2".to_string(),
                    flipped_at: 1704067200,
                },
                AliasHistoryEntry {
                    uid: "products_v1".to_string(),
                    flipped_at: 1703980800,
                },
            ],
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains(r#""name":"products""#));
        assert!(json.contains(r#""kind":"single""#));
        assert!(json.contains(r#""current_uid":"products_v3""#));
        assert!(json.contains(r#""version":5"#));
        assert!(json.contains(r#""history""#));
    }

    #[test]
    fn test_list_aliases_response_serialization() {
        let response = ListAliasesResponse {
            aliases: vec![
                AliasInfo {
                    name: "products".to_string(),
                    kind: "single".to_string(),
                    current_uid: Some("products_v3".to_string()),
                    target_uids: None,
                    version: 5,
                },
                AliasInfo {
                    name: "logs".to_string(),
                    kind: "multi".to_string(),
                    current_uid: None,
                    target_uids: Some(vec![
                        "logs-2026-01-01".to_string(),
                        "logs-2026-01-02".to_string(),
                    ]),
                    version: 1,
                },
            ],
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains(r#""name":"products""#));
        assert!(json.contains(r#""kind":"single""#));
        assert!(json.contains(r#""name":"logs""#));
        assert!(json.contains(r#""kind":"multi""#));
    }

    #[test]
    fn test_error_response_serialization() {
        let error = ErrorResponse {
            code: "miroir_multi_alias_not_writable".to_string(),
            message: "multi-target aliases are managed exclusively by ILM".to_string(),
        };

        let json = serde_json::to_string(&error).unwrap();
        assert!(json.contains(r#""code":"miroir_multi_alias_not_writable""#));
        assert!(json.contains(r#""message":"multi-target aliases are managed exclusively by ILM""#));
    }
}
