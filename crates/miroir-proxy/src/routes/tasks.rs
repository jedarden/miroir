use axum::extract::Path;
use axum::{http::StatusCode, Json};
use axum::{routing::any, Router};

pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new().route("/:index/:task_uid", any(tasks_handler))
}

async fn tasks_handler(
    Path(_path): Path<Vec<String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    Err(StatusCode::NOT_IMPLEMENTED)
}
