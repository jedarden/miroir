use axum::extract::Path;
use axum::{http::StatusCode, Json};
use axum::{routing::any, Router};

pub fn router() -> Router {
    Router::new()
        .route("/", any(documents_handler))
        .route("/:index", any(documents_handler))
        .route("/:index/:document_id", any(documents_handler))
}

async fn documents_handler(
    Path(_path): Path<Vec<String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    Err(StatusCode::NOT_IMPLEMENTED)
}
