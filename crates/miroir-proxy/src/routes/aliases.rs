//! Atomic index alias management endpoints (plan §13.7).

use axum::{
    extract::{FromRef, Path, State},
    http::StatusCode,
    Json,
};
use miroir_core::{
    alias::{Alias, AliasKind},
    config::MiroirConfig,
    task_store::TaskStore,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Alias management state.
#[derive(Clone)]
pub struct AliasState {
    pub config: Arc<MiroirConfig>,
    pub task_registry: Arc<miroir_core::task_registry::TaskRegistryImpl>,
}

/// Request body for PUT /_miroir/aliases/{name}.
#[derive(Debug, Deserialize)]
pub struct UpdateAliasRequest {
    pub target: String,
}

/// Response for GET /_miroir/aliases/{name}.
#[derive(Debug, Serialize)]
pub struct GetAliasResponse {
    pub name: String,
    pub kind: String,
    pub current_uid: Option<String>,
    pub target_uids: Option<Vec<String>>,
    pub version: u64,
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

/// GET /_miroir/aliases/{name} — get alias details.
pub async fn get_alias<S>(
    State(state): State<AliasState>,
    Path(name): Path<String>,
) -> Result<Json<GetAliasResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AliasState: FromRef<S>,
{
    if !state.config.aliases.enabled {
        return Err(StatusCode::NOT_IMPLEMENTED);
    }

    // TODO: Look up alias from task store
    let alias = state.task_registry.get_alias(&name);

    match alias {
        Ok(Some(alias)) => Ok(Json(GetAliasResponse {
            name: alias.name.clone(),
            kind: match alias.kind {
                AliasKind::Single => "single".to_string(),
                AliasKind::Multi => "multi".to_string(),
            },
            current_uid: alias.current_uid,
            target_uids: alias.target_uids.map(|uids| {
                uids.into_iter().collect()
            }),
            version: alias.generation,
        })),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// PUT /_miroir/aliases/{name} — create or update an alias (atomic flip).
///
/// Plan §13.7: Atomic alias flip for blue-green deployments.
pub async fn update_alias<S>(
    State(state): State<AliasState>,
    Path(name): Path<String>,
    Json(body): Json<UpdateAliasRequest>,
) -> Result<Json<AliasInfo>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AliasState: FromRef<S>,
{
    if !state.config.aliases.enabled {
        return Err(StatusCode::NOT_IMPLEMENTED);
    }

    // Validate target exists
    // TODO: Check if target index exists

    // Create or update alias
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u64;
    let alias = Alias {
        name: name.clone(),
        kind: AliasKind::Single,
        current_uid: Some(body.target),
        target_uids: None,
        generation: 1,
        created_at: now,
        updated_at: now,
    };

    // TODO: Persist to task store
    let _ = state.task_registry.put_alias(&alias);

    Ok(Json(AliasInfo {
        name: alias.name,
        kind: "single".to_string(),
        current_uid: alias.current_uid,
        target_uids: None,
        version: alias.generation,
    }))
}

/// DELETE /_miroir/aliases/{name} — delete an alias.
pub async fn delete_alias<S>(
    State(state): State<AliasState>,
    Path(name): Path<String>,
) -> Result<StatusCode, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AliasState: FromRef<S>,
{
    if !state.config.aliases.enabled {
        return Err(StatusCode::NOT_IMPLEMENTED);
    }

    // TODO: Delete from task store
    let _ = state.task_registry.delete_alias(&name);

    Ok(StatusCode::NO_CONTENT)
}

/// GET /_miroir/aliases — list all aliases.
pub async fn list_aliases<S>(
    State(state): State<AliasState>,
) -> Result<Json<ListAliasesResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AliasState: FromRef<S>,
{
    if !state.config.aliases.enabled {
        return Err(StatusCode::NOT_IMPLEMENTED);
    }

    // TODO: List aliases from task store
    let aliases = state.task_registry.list_aliases().unwrap_or_default();

    let alias_infos: Vec<AliasInfo> = aliases
        .into_iter()
        .map(|alias| AliasInfo {
            name: alias.name,
            kind: match alias.kind {
                AliasKind::Single => "single".to_string(),
                AliasKind::Multi => "multi".to_string(),
            },
            current_uid: alias.current_uid,
            target_uids: alias.target_uids.map(|uids| {
                uids.into_iter().collect()
            }),
            version: alias.generation,
        })
        .collect();

    Ok(Json(ListAliasesResponse {
        aliases: alias_infos,
    }))
}
