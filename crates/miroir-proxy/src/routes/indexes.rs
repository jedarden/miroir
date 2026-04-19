use axum::extract::Path;
use axum::{http::StatusCode, Json};
use axum::{routing::any, Router};

pub fn router() -> Router {
    Router::new()
        .route("/", any(indexes_handler))
        .route("/:index", any(indexes_handler))
        .route("/:index/:sub", any(indexes_handler))
}

async fn indexes_handler(
    Path(_path): Path<Vec<String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    Err(StatusCode::NOT_IMPLEMENTED)
}
