use axum::extract::Path;
use axum::{http::StatusCode, Json};
use axum::{routing::any, Router};

pub fn router() -> Router {
    Router::new().route("/*path", any(settings_handler))
}

async fn settings_handler(
    Path(_path): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    Err(StatusCode::NOT_IMPLEMENTED)
}
