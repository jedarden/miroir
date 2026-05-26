//! Dump import routes (plan §13.9).
//!
//! Admin API endpoints for streaming routed dump import:
//! - `POST /_miroir/dumps/import` — start a dump import (multipart)
//! - `GET /_miroir/dumps/import/{id}/status` — get import status

use axum::extract::{Extension, FromRef, Multipart, Path};
use axum::routing::{get, post};
use axum::{Json, Router};
use miroir_core::api_error::{MeilisearchError, MiroirCode};
use miroir_core::dump_import::{DumpImportManager, DumpImportPhase, DumpImportStatus};

use crate::client::HttpClient;

/// Multipart field names for dump import.
const FIELD_INDEX_UID: &str = "index_uid";
const FIELD_PRIMARY_KEY: &str = "primary_key";
const FIELD_SHARD_COUNT: &str = "shard_count";
const FIELD_DUMP_FILE: &str = "dump_file";

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
///
/// Requires multipart/form-data with fields: index_uid, primary_key, shard_count, dump_file (file).
async fn start_import<S>(
    Extension(state): Extension<crate::routes::admin_endpoints::AppState>,
    mut multipart: Multipart,
) -> Result<Json<DumpImportResponse>, MeilisearchError>
where
    S: Clone + Send + Sync + 'static,
    crate::routes::admin_endpoints::AppState: FromRef<S>,
{
    // Handle multipart form data
    let mut index_uid = None;
    let mut primary_key = None;
    let mut shard_count = None;
    let mut dump_data = None;

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        MeilisearchError::new(MiroirCode::InvalidRequest, format!("multipart error: {e}"))
    })? {
        let name = field.name().unwrap_or("").to_string();

        match name.as_str() {
            FIELD_INDEX_UID => {
                let value = field.text().await.map_err(|e| {
                    MeilisearchError::new(
                        MiroirCode::InvalidRequest,
                        format!("error reading index_uid: {e}"),
                    )
                })?;
                index_uid = Some(value);
            }
            FIELD_PRIMARY_KEY => {
                let value = field.text().await.map_err(|e| {
                    MeilisearchError::new(
                        MiroirCode::InvalidRequest,
                        format!("error reading primary_key: {e}"),
                    )
                })?;
                primary_key = Some(value);
            }
            FIELD_SHARD_COUNT => {
                let value = field.text().await.map_err(|e| {
                    MeilisearchError::new(
                        MiroirCode::InvalidRequest,
                        format!("error reading shard_count: {e}"),
                    )
                })?;
                let count = value.parse::<u32>().map_err(|_| {
                    MeilisearchError::new(
                        MiroirCode::InvalidRequest,
                        "shard_count must be a number".to_string(),
                    )
                })?;
                shard_count = Some(count);
            }
            FIELD_DUMP_FILE => {
                let filename = field.file_name().map(|s| s.to_string());
                let data = field.bytes().await.map_err(|e| {
                    MeilisearchError::new(
                        MiroirCode::InvalidRequest,
                        format!("error reading dump_file: {e}"),
                    )
                })?;
                tracing::debug!(
                    filename,
                    size = data.len(),
                    "Received dump file via multipart upload"
                );
                dump_data = Some(data.to_vec());
            }
            _ => {
                tracing::warn!(unknown_field = %name, "Ignoring unknown field in multipart dump import");
            }
        }
    }

    let index_uid = index_uid.ok_or_else(|| {
        MeilisearchError::new(MiroirCode::InvalidRequest, "index_uid is required")
    })?;
    let primary_key = primary_key.ok_or_else(|| {
        MeilisearchError::new(MiroirCode::InvalidRequest, "primary_key is required")
    })?;
    let shard_count = shard_count.ok_or_else(|| {
        MeilisearchError::new(MiroirCode::InvalidRequest, "shard_count is required")
    })?;
    let dump_data = dump_data.ok_or_else(|| {
        MeilisearchError::new(MiroirCode::InvalidRequest, "dump_file is required")
    })?;

    // Validate request
    if index_uid.is_empty() {
        return Err(MeilisearchError::new(
            MiroirCode::InvalidRequest,
            "index_uid is required",
        ));
    }

    if primary_key.is_empty() {
        return Err(MeilisearchError::new(
            MiroirCode::InvalidRequest,
            "primary_key is required",
        ));
    }

    if shard_count == 0 {
        return Err(MeilisearchError::new(
            MiroirCode::InvalidRequest,
            "shard_count must be > 0",
        ));
    }

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
            index_uid.clone(),
            dump_data,
            primary_key.clone(),
            shard_count,
        )
        .await
        .map_err(|e| {
            MeilisearchError::new(
                MiroirCode::InternalError,
                format!("failed to start import: {e}"),
            )
        })?;

    // Record metrics
    state.metrics.inc_dump_import_bytes_read(bytes_read);
    state
        .metrics
        .set_dump_import_phase(&index_uid, &import_id, DumpImportPhase::Reading as u8);

    tracing::info!(
        "Started dump import {} for index {} (shard_count={}, primary_key={}, bytes={})",
        import_id,
        index_uid,
        shard_count,
        primary_key,
        bytes_read
    );

    let status_url = format!("/_miroir/dumps/import/{import_id}/status");

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
        MeilisearchError::new(MiroirCode::NotFound, format!("import task not found: {id}"))
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

/// Get current UNIX timestamp in milliseconds.
fn millis_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {}
