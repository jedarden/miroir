//! §13.18 Canary routes
#![allow(dead_code)]

use axum::{
    extract::{FromRef, Path, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use miroir_core::{
    canary::{CanaryAssertion, QueryCapture, SearchQuery},
    task_store::{NewCanary, TaskStore},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// Canary management state.
#[derive(Clone)]
pub struct CanaryState {
    pub store: Arc<dyn TaskStore>,
    pub capture: Arc<QueryCapture>,
}

/// Canary creation request
#[derive(Debug, Deserialize)]
pub struct CreateCanaryRequest {
    pub name: String,
    pub index_uid: String,
    pub interval_s: i64,
    #[serde(flatten)]
    pub query: serde_json::Value,
    pub assertions: Vec<serde_json::Value>,
}

/// Canary status response
#[derive(Debug, Serialize)]
pub struct CanaryStatusResponse {
    pub canaries: Vec<CanaryInfo>,
}

#[derive(Debug, Serialize)]
pub struct CanaryInfo {
    pub id: String,
    pub name: String,
    pub index_uid: String,
    pub interval_s: i64,
    pub enabled: bool,
    pub created_at: i64,
    pub last_run: Option<CanaryRunInfo>,
}

#[derive(Debug, Serialize)]
pub struct CanaryRunInfo {
    pub ran_at: i64,
    pub status: String,
    pub latency_ms: i64,
    pub failed_assertions: usize,
}

/// Capture request
#[derive(Debug, Deserialize)]
pub struct CaptureRequest {
    pub max_queries: Option<usize>,
}

/// Capture response
#[derive(Debug, Serialize)]
pub struct CaptureResponse {
    pub capture_id: String,
    pub queries: Vec<CapturedQueryInfo>,
}

#[derive(Debug, Serialize)]
pub struct CapturedQueryInfo {
    pub index_uid: String,
    pub query: serde_json::Value,
    pub timestamp: i64,
}

/// Create a new canary
pub async fn create_canary<S>(
    State(state): State<CanaryState>,
    Json(req): Json<CreateCanaryRequest>,
) -> Result<Json<serde_json::Value>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    CanaryState: FromRef<S>,
{
    let id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp_millis();

    // Parse query
    let query: SearchQuery = serde_json::from_value(serde_json::json!(req.query)).map_err(|e| {
        tracing::error!(error = %e, "Invalid canary query");
        StatusCode::BAD_REQUEST
    })?;

    // Parse assertions
    let assertions: Vec<CanaryAssertion> = req
        .assertions
        .into_iter()
        .map(serde_json::from_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            tracing::error!(error = %e, "Invalid canary assertion");
            StatusCode::BAD_REQUEST
        })?;

    // Create NewCanary for storage
    let new_canary = NewCanary {
        id: id.clone(),
        name: req.name.clone(),
        index_uid: req.index_uid.clone(),
        interval_s: req.interval_s,
        query_json: serde_json::to_string(&query).unwrap_or_default(),
        assertions_json: serde_json::to_string(&assertions).unwrap_or_default(),
        enabled: true,
        created_at: now,
    };

    state.store.upsert_canary(&new_canary).map_err(|e| {
        tracing::error!(error = %e, "Failed to store canary");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(serde_json::json!({
        "id": id,
        "status": "created"
    })))
}

/// Get canary status
pub async fn get_canary_status<S>(
    State(state): State<CanaryState>,
) -> Result<Json<CanaryStatusResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    CanaryState: FromRef<S>,
{
    let canaries = state.store.list_canaries().map_err(|e| {
        tracing::error!(error = %e, "Failed to list canaries");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let canary_infos: Vec<CanaryInfo> = canaries
        .into_iter()
        .map(|canary| {
            let runs = state
                .store
                .get_canary_runs(&canary.id, 1)
                .unwrap_or_default();
            let last_run = runs.first().map(|r| CanaryRunInfo {
                ran_at: r.ran_at,
                status: r.status.clone(),
                latency_ms: r.latency_ms,
                failed_assertions: r
                    .failed_assertions_json
                    .as_ref()
                    .map(|j| {
                        serde_json::from_str::<Vec<serde_json::Value>>(j)
                            .map(|v| v.len())
                            .unwrap_or(0)
                    })
                    .unwrap_or(0),
            });

            CanaryInfo {
                id: canary.id,
                name: canary.name,
                index_uid: canary.index_uid,
                interval_s: canary.interval_s,
                enabled: canary.enabled,
                created_at: canary.created_at,
                last_run,
            }
        })
        .collect();

    Ok(Json(CanaryStatusResponse {
        canaries: canary_infos,
    }))
}

/// Get a specific canary
pub async fn get_canary<S>(
    State(state): State<CanaryState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    CanaryState: FromRef<S>,
{
    let canary = state
        .store
        .get_canary(&id)
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to get canary");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let runs = state.store.get_canary_runs(&id, 100).unwrap_or_default();

    Ok(Json(serde_json::json!({
        "id": canary.id,
        "name": canary.name,
        "index_uid": canary.index_uid,
        "interval_s": canary.interval_s,
        "enabled": canary.enabled,
        "created_at": canary.created_at,
        "query": canary.query_json,
        "assertions": canary.assertions_json,
        "runs": runs
    })))
}

/// Update a canary
pub async fn update_canary<S>(
    State(state): State<CanaryState>,
    Path(id): Path<String>,
    Json(req): Json<CreateCanaryRequest>,
) -> Result<Json<serde_json::Value>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    CanaryState: FromRef<S>,
{
    // Verify canary exists
    let _existing = state
        .store
        .get_canary(&id)
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to get canary");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Parse query
    let query: SearchQuery = serde_json::from_value(serde_json::json!(req.query)).map_err(|e| {
        tracing::error!(error = %e, "Invalid canary query");
        StatusCode::BAD_REQUEST
    })?;

    // Parse assertions
    let assertions: Vec<CanaryAssertion> = req
        .assertions
        .into_iter()
        .map(serde_json::from_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            tracing::error!(error = %e, "Invalid canary assertion");
            StatusCode::BAD_REQUEST
        })?;

    // Create NewCanary for storage
    let new_canary = NewCanary {
        id: id.clone(),
        name: req.name.clone(),
        index_uid: req.index_uid.clone(),
        interval_s: req.interval_s,
        query_json: serde_json::to_string(&query).unwrap_or_default(),
        assertions_json: serde_json::to_string(&assertions).unwrap_or_default(),
        enabled: true,
        created_at: _existing.created_at,
    };

    state.store.upsert_canary(&new_canary).map_err(|e| {
        tracing::error!(error = %e, "Failed to store canary");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(serde_json::json!({
        "id": id,
        "status": "updated"
    })))
}

/// Delete a canary
pub async fn delete_canary<S>(
    State(state): State<CanaryState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    CanaryState: FromRef<S>,
{
    state.store.delete_canary(&id).map_err(|e| {
        tracing::error!(error = %e, "Failed to delete canary");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(StatusCode::NO_CONTENT)
}

/// Start capturing queries for canary creation
pub async fn start_capture<S>(
    State(state): State<CanaryState>,
    Json(req): Json<CaptureRequest>,
) -> Result<Json<CaptureResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    CanaryState: FromRef<S>,
{
    let capture_id = Uuid::new_v4().to_string();
    let _max_queries = req.max_queries.unwrap_or(100);

    // Clear previous captures
    state.capture.clear().await;

    Ok(Json(CaptureResponse {
        capture_id,
        queries: Vec::new(),
    }))
}

/// Get captured queries
pub async fn get_captured<S>(
    State(state): State<CanaryState>,
) -> Result<Json<serde_json::Value>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    CanaryState: FromRef<S>,
{
    let queries = state.capture.get_captured().await;

    let query_infos: Vec<CapturedQueryInfo> = queries
        .iter()
        .map(|q| CapturedQueryInfo {
            index_uid: q.index_uid.clone(),
            query: serde_json::to_value(&q.query).unwrap_or_default(),
            timestamp: q.timestamp,
        })
        .collect();

    Ok(Json(serde_json::json!({
        "queries": query_infos
    })))
}

/// Create a canary from a captured query
pub async fn create_from_capture<S>(
    State(state): State<CanaryState>,
    Path(index_uid): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    CanaryState: FromRef<S>,
{
    let queries = state.capture.get_captured().await;

    // Find the first captured query matching the index UID
    let captured = queries
        .iter()
        .find(|q| q.index_uid == index_uid)
        .ok_or(StatusCode::NOT_FOUND)?;

    let id = Uuid::new_v4().to_string();

    // Create a default canary from the captured query
    let canary = miroir_core::canary::create_canary(
        id.clone(),
        format!("canary-{}", captured.index_uid),
        captured.index_uid.clone(),
        3600, // Default: run every hour
        captured.query.clone(),
        vec![
            CanaryAssertion::MinHits { value: 1 },
            CanaryAssertion::MaxP95Ms { value: 500 },
        ],
    )
    .map_err(|e| {
        tracing::error!(error = %e, "Failed to create canary");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state.store.upsert_canary(&canary).map_err(|e| {
        tracing::error!(error = %e, "Failed to store canary");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(serde_json::json!({
        "id": id,
        "status": "created"
    })))
}

/// Router for canary endpoints.
pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    CanaryState: FromRef<S>,
{
    Router::new()
        .route("/canaries", post(create_canary::<S>))
        .route("/canaries", get(get_canary_status::<S>))
        .route("/canaries/:id", get(get_canary::<S>))
        .route("/canaries/:id", put(update_canary::<S>))
        .route("/canaries/:id", delete(delete_canary::<S>))
        .route("/canaries/capture", post(start_capture::<S>))
        .route("/canaries/captured", get(get_captured::<S>))
        .route(
            "/canaries/from-capture/:index",
            post(create_from_capture::<S>),
        )
}
