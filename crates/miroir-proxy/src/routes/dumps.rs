//! Dump import routes (plan §13.9).
//!
//! Admin API endpoints for streaming routed dump import:
//! - `POST /_miroir/dumps/import` — start a dump import
//! - `GET /_miroir/dumps/import/{id}/status` — get import status

use axum::extract::{Extension, FromRef, Path};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use miroir_core::api_error::{MeilisearchError, MiroirCode};
use miroir_core::config::Config;
use miroir_core::dump_import::{DumpImportManager, DumpImportPhase, DumpImportStatus};
use miroir_core::topology::Topology;
use serde_json::Value;
use std::sync::Arc;

use crate::client::HttpClient;
use crate::middleware::Metrics;

/// Request body for starting a dump import.
#[derive(serde::Deserialize)]
struct DumpImportRequest {
    /// Index UID to import into.
    index_uid: String,
    /// Primary key field name.
    primary_key: String,
    /// Number of shards for the index.
    shard_count: u32,
    /// Dump file contents (base64-encoded or raw NDJSON).
    dump_data: String,
}

/// Response for starting a dump import.
#[derive(serde::Serialize)]
struct DumpImportResponse {
    /// Import task ID.
    miroir_task_id: String,
    /// Status endpoint for this import.
    status_url: String,
}

/// Create dump import routes.
pub fn routes<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    crate::routes::admin_endpoints::AppState: FromRef<S>,
{
    Router::new()
        .route("/import", post(start_import::<S>))
        .route("/import/:id/status", get(get_status::<S>))
}

/// POST /_miroir/dumps/import
///
/// Start a streaming routed dump import.
async fn start_import<S>(
    Extension(state): Extension<crate::routes::admin_endpoints::AppState>,
    Json(req): Json<DumpImportRequest>,
) -> Result<Json<DumpImportResponse>, MeilisearchError>
where
    S: Clone + Send + Sync + 'static,
    crate::routes::admin_endpoints::AppState: FromRef<S>,
{
    // Validate request
    if req.index_uid.is_empty() {
        return Err(MeilisearchError::new(
            MiroirCode::InvalidRequest,
            "index_uid is required",
        ));
    }

    if req.primary_key.is_empty() {
        return Err(MeilisearchError::new(
            MiroirCode::InvalidRequest,
            "primary_key is required",
        ));
    }

    if req.shard_count == 0 {
        return Err(MeilisearchError::new(
            MiroirCode::InvalidRequest,
            "shard_count must be > 0",
        ));
    }

    // Decode dump data (assume base64 if it looks like it, otherwise treat as raw)
    let dump_data = if looks_like_base64(&req.dump_data) {
        match base64_decode(&req.dump_data) {
            Ok(data) => data,
            Err(e) => {
                return Err(MeilisearchError::new(
                    MiroirCode::InvalidRequest,
                    format!("invalid base64 dump_data: {}", e),
                ))
            }
        }
    } else {
        req.dump_data.into_bytes()
    };

    let bytes_read = dump_data.len() as u64;

    // Create HTTP client
    let master_key = state.config.master_key.clone();
    let http_client = HttpClient::new(master_key, 30000); // 30s timeout

    // Create dump import manager
    let manager = DumpImportManager::new(
        state.config.dump_import.clone(),
        state.topology.clone(),
        http_client,
    );

    // Start the import
    let import_id = manager
        .start_import(
            req.index_uid.clone(),
            dump_data,
            req.primary_key.clone(),
            req.shard_count,
        )
        .await
        .map_err(|e| {
            MeilisearchError::new(
                MiroirCode::InternalError,
                format!("failed to start import: {}", e),
            )
        })?;

    // Record metrics
    state.metrics.inc_dump_import_bytes_read(bytes_read);
    state
        .metrics
        .set_dump_import_phase(&req.index_uid, &import_id, DumpImportPhase::Reading as u8);

    tracing::info!(
        "Started dump import {} for index {} (shard_count={}, primary_key={}, bytes={})",
        import_id,
        req.index_uid,
        req.shard_count,
        req.primary_key,
        bytes_read
    );

    let status_url = format!("/_miroir/dumps/import/{}/status", import_id);

    Ok(Json(DumpImportResponse {
        miroir_task_id: import_id,
        status_url,
    }))
}

/// GET /_miroir/dumps/import/:id/status
///
/// Get the status of an in-progress or completed dump import.
async fn get_status<S>(
    Extension(state): Extension<crate::routes::admin_endpoints::AppState>,
    Path(id): Path<String>,
) -> Result<Json<DumpImportStatus>, MeilisearchError>
where
    S: Clone + Send + Sync + 'static,
    crate::routes::admin_endpoints::AppState: FromRef<S>,
{
    // Create HTTP client (we only need it for the manager constructor)
    let master_key = state.config.master_key.clone();
    let http_client = HttpClient::new(master_key, 30000);

    let manager = DumpImportManager::new(
        state.config.dump_import.clone(),
        state.topology.clone(),
        http_client,
    );

    let status = manager.get_status(&id).await.ok_or_else(|| {
        MeilisearchError::new(
            MiroirCode::NotFound,
            format!("import task not found: {}", id),
        )
    })?;

    // Record metrics from status
    state
        .metrics
        .inc_dump_import_documents_routed(status.documents_processed);

    // Calculate and update import rate (docs per second)
    let now_ms = millis_now();
    let elapsed_secs = if now_ms > status.phase_started_at {
        (now_ms - status.phase_started_at) as f64 / 1000.0
    } else {
        0.0
    };
    if elapsed_secs > 0.0 && status.documents_processed > 0 {
        let rate = status.documents_processed as f64 / elapsed_secs;
        state.metrics.set_dump_import_rate(rate);
    }

    // Update phase metric
    if let Ok(phase_num) = status.phase.parse::<u8>() {
        state
            .metrics
            .set_dump_import_phase(&status.index_uid, &id, phase_num);
    }

    Ok(Json(status))
}

/// Check if a string looks like base64-encoded data.
fn looks_like_base64(s: &str) -> bool {
    // Base64 strings are typically multiples of 4 and only contain A-Za-z0-9+/
    if s.len() % 4 != 0 {
        return false;
    }

    s.chars()
        .all(|c| c.is_alphanumeric() || c == '+' || c == '/' || c == '=')
}

/// Decode a base64 string.
fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| format!("base64 decode failed: {}", e))
}

/// Get current UNIX timestamp in milliseconds.
fn millis_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_looks_like_base64() {
        assert!(looks_like_base64("SGVsbG8gV29ybGQ=")); // "Hello World"
        assert!(!looks_like_base64("Hello World"));
        assert!(!looks_like_base64("not base64!"));
        assert!(looks_like_base64("eyJpZCI6ICIxIn0=")); // JSON
    }

    #[test]
    fn test_base64_decode() {
        let encoded = "SGVsbG8gV29ybGQ=";
        let decoded = base64_decode(encoded).unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), "Hello World");
    }

    #[test]
    fn test_base64_decode_invalid() {
        let result = base64_decode("invalid!base64");
        assert!(result.is_err());
    }
}
