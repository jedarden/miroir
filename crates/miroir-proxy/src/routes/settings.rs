//! Settings proxy — falls through to index-level settings handlers.
//!
//! Meilisearch settings are at `/indexes/{uid}/settings/...`, which are handled
//! by the indexes router. This module is kept for any future global settings
//! paths that don't belong to a specific index.

use axum::extract::Path;
use axum::http::StatusCode;
use axum::{routing::any, Json, Router};

pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new().route("/*path", any(settings_handler))
}

async fn settings_handler(
    Path(_path): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // Index-level settings (PATCH /indexes/{uid}/settings) are handled by
    // the indexes router. Any request reaching here is a non-index settings
    // path that doesn't exist in Meilisearch.
    Err(StatusCode::NOT_FOUND)
}
