use axum::extract::{FromRef, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;
use super::admin_endpoints::AppState;

#[derive(Serialize)]
pub struct VersionResponse {
    pub version: String,
}

/// GET /version — returns Meilisearch version from any healthy node.
/// Caches at ~60s TTL per plan §10.
pub async fn get_version<S>(State(state): State<S>) -> Result<Json<VersionResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);
    let version = app_state.version_state.get_version().await?;
    Ok(Json(VersionResponse { version }))
}
