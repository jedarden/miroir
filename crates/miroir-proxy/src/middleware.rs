//! Tracing/logging + Prometheus middleware

use axum::{extract::Request, middleware::Next, response::Response};

#[allow(dead_code)]
pub async fn tracing_middleware(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let response = next.run(req).await;
    tracing::info!(method = %method, uri = %uri, status = response.status().as_u16());
    response
}

#[allow(dead_code)]
pub async fn prometheus_middleware(req: Request, next: Next) -> Response {
    // Prometheus metrics stub - to be implemented in Phase 2
    next.run(req).await
}
