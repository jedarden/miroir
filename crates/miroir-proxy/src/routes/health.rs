//! Health check endpoints: /health, /version, /_miroir/ready
#![allow(dead_code)]

use crate::error_response::ErrorResponse;
use crate::routes::admin_endpoints::AppState;
use axum::{extract::FromRef, extract::State, Json};
use serde::Serialize;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
}

#[derive(Serialize)]
pub struct VersionResponse {
    pub version: String,
    pub commit: String,
    pub build_date: String,
}

/// GET /health - Public health check endpoint.
pub async fn get_health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "available".to_string(),
    })
}

/// GET /version - Public version endpoint.
pub async fn get_version() -> Json<VersionResponse> {
    Json(VersionResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit: option_env!("GIT_COMMIT").unwrap_or("unknown").to_string(),
        build_date: option_env!("BUILD_DATE").unwrap_or("unknown").to_string(),
    })
}

/// GET /_miroir/ready - Readiness check endpoint.
///
/// Returns 200 if the proxy is ready to serve requests.
pub async fn get_ready<S>(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ErrorResponse>
where
    AppState: FromRef<S>,
    S: Send + Sync,
{
    let topology = state.topology.read().await;

    // Check if we have any healthy nodes
    let healthy_count = topology.nodes().filter(|n| n.is_healthy()).count();

    if healthy_count == 0 {
        return Err(ErrorResponse::new(
            "No healthy nodes available",
            "miroir_not_ready",
        ));
    }

    Ok(Json(serde_json::json!({
        "status": "ready",
        "healthy_nodes": healthy_count,
        "total_nodes": topology.nodes().count(),
    })))
}
