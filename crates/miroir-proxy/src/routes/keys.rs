//! Keys management endpoints: CRUD with broadcast to all nodes.
//!
//! Implements P2.4:
//! - `POST /keys` — create key on every node (all-or-nothing)
//! - `PATCH /keys/{key}` — update key on every node (sequential with rollback)
//! - `DELETE /keys/{key}` — delete key on every node (all-or-nothing)
//! - `GET /keys` — list keys (proxy to first node)
//! - `GET /keys/{key}` — get key (proxy to first node)

use axum::extract::{Extension, Path};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use miroir_core::api_error::{MeilisearchError, MiroirCode};
use miroir_core::config::Config;
use serde_json::Value;
use std::sync::Arc;

use crate::routes::indexes::MeilisearchClient;

/// Collect all node addresses from config.
fn all_node_addresses(config: &Config) -> Vec<String> {
    config.nodes.iter().map(|n| n.address.clone()).collect()
}

/// Try to forward a Meilisearch error; fall back to a Miroir error.
fn forward_or_miroir(_status: u16, body: &str, fallback_msg: &str) -> MeilisearchError {
    if let Some(meili_err) = MeilisearchError::forwarded(body) {
        meili_err
    } else {
        MeilisearchError::new(MiroirCode::NoQuorum, fallback_msg)
    }
}

pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/", post(create_key_handler).get(list_keys_handler))
        .route(
            "/:key",
            get(get_key_handler)
                .patch(update_key_handler)
                .delete(delete_key_handler),
        )
}

// ---------------------------------------------------------------------------
// POST /keys — create key (all-or-nothing broadcast)
// ---------------------------------------------------------------------------

async fn create_key_handler(
    Extension(config): Extension<Arc<Config>>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, MeilisearchError> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes = all_node_addresses(&config);
    let mut created_on: Vec<String> = Vec::new();
    let mut first_response: Option<Value> = None;

    for address in &nodes {
        match client.post_raw(address, "/keys", &body).await {
            Ok((status, text)) if (200..300).contains(&status) => {
                if first_response.is_none() {
                    first_response = serde_json::from_str(&text).ok();
                }
                created_on.push(address.clone());
            }
            Ok((status, text)) => {
                // Rollback: delete key on all previously created nodes
                rollback_delete_key(&client, &body, &created_on).await;
                let msg = format!("key creation failed on {address}: HTTP {status} — {text}");
                return Err(forward_or_miroir(status, &text, &msg));
            }
            Err(e) => {
                rollback_delete_key(&client, &body, &created_on).await;
                return Err(MeilisearchError::new(
                    MiroirCode::NoQuorum,
                    format!("key creation failed on {address}: {e}"),
                ));
            }
        }
    }

    Ok(Json(
        first_response.unwrap_or(serde_json::json!({"status": "created"})),
    ))
}

/// Rollback by deleting the key from nodes where it was successfully created.
async fn rollback_delete_key(client: &MeilisearchClient, body: &Value, nodes: &[String]) {
    // Try to get the key UID from the creation body or extract it
    let key_or_name = body
        .get("uid")
        .or(body.get("name"))
        .or(body.get("key"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if key_or_name.is_empty() {
        tracing::warn!("key rollback: cannot determine key identifier for rollback");
        return;
    }

    for address in nodes {
        let path = format!("/keys/{key_or_name}");
        match client.delete_raw(address, &path).await {
            Ok(_) => tracing::info!(node = %address, "key rollback: deleted key"),
            Err(e) => {
                tracing::error!(node = %address, error = %e, "key rollback: failed to delete key")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PATCH /keys/{key} — update key (sequential broadcast with rollback)
// ---------------------------------------------------------------------------

async fn update_key_handler(
    Path(key): Path<String>,
    Extension(config): Extension<Arc<Config>>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, MeilisearchError> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes = all_node_addresses(&config);
    let path = format!("/keys/{key}");

    // Snapshot current key state from all nodes
    let mut snapshots: Vec<(String, Value)> = Vec::new();
    for address in &nodes {
        match client.get_raw(address, &path).await {
            Ok((status, text)) if (200..300).contains(&status) => {
                let snapshot: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                snapshots.push((address.clone(), snapshot));
            }
            Ok((status, text)) => {
                return Err(forward_or_miroir(
                    status,
                    &text,
                    &format!("failed to snapshot key on {address}: HTTP {status}"),
                ));
            }
            Err(e) => {
                return Err(MeilisearchError::new(
                    MiroirCode::NoQuorum,
                    format!("failed to snapshot key on {address}: {e}"),
                ));
            }
        }
    }

    // Apply update sequentially
    let mut applied: Vec<String> = Vec::new();
    let mut first_response: Option<Value> = None;

    for (address, _snapshot) in &snapshots {
        match client.patch_raw(address, &path, &body).await {
            Ok((status, text)) if (200..300).contains(&status) => {
                if first_response.is_none() {
                    first_response = serde_json::from_str(&text).ok();
                }
                applied.push(address.clone());
            }
            Ok((status, text)) => {
                rollback_key_update(&client, &path, &snapshots, &applied).await;
                let msg = format!("key update failed on {address}: HTTP {status} — {text}");
                return Err(forward_or_miroir(status, &text, &msg));
            }
            Err(e) => {
                rollback_key_update(&client, &path, &snapshots, &applied).await;
                return Err(MeilisearchError::new(
                    MiroirCode::NoQuorum,
                    format!("key update failed on {address}: {e}"),
                ));
            }
        }
    }

    Ok(Json(
        first_response.unwrap_or(serde_json::json!({"status": "updated"})),
    ))
}

/// Rollback key updates by restoring pre-change snapshots.
async fn rollback_key_update(
    client: &MeilisearchClient,
    path: &str,
    snapshots: &[(String, Value)],
    applied: &[String],
) {
    for address in applied {
        if let Some((_, snapshot)) = snapshots.iter().find(|(a, _)| a == address) {
            match client.patch_raw(address, path, snapshot).await {
                Ok((_status, _text)) if (200..300).contains(&_status) => {
                    tracing::info!(node = %address, "key rollback succeeded");
                }
                Ok((status, _text)) => {
                    tracing::error!(node = %address, status, "key rollback failed");
                }
                Err(e) => {
                    tracing::error!(node = %address, error = %e, "key rollback failed");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// DELETE /keys/{key} — delete key (all-or-nothing broadcast)
// ---------------------------------------------------------------------------

async fn delete_key_handler(
    Path(key): Path<String>,
    Extension(config): Extension<Arc<Config>>,
) -> Result<Json<Value>, MeilisearchError> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes = all_node_addresses(&config);
    let path = format!("/keys/{key}");
    let mut first_response: Option<Value> = None;
    let mut errors: Vec<String> = Vec::new();

    for address in &nodes {
        match client.delete_raw(address, &path).await {
            Ok((status, text)) if (200..300).contains(&status) => {
                if first_response.is_none() {
                    first_response = serde_json::from_str(&text).ok();
                }
            }
            Ok((status, text)) => {
                errors.push(format!("{address}: HTTP {status} — {text}"));
            }
            Err(e) => {
                errors.push(format!("{address}: {e}"));
            }
        }
    }

    if !errors.is_empty() && first_response.is_none() {
        return Err(MeilisearchError::new(
            MiroirCode::NoQuorum,
            format!("key deletion failed on all nodes: {}", errors.join("; ")),
        ));
    }

    if !errors.is_empty() {
        // Hash the key identifier for correlation without logging the raw value (plan §10: no PII).
        let key_hash = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut h);
            format!("{:016x}", h.finish())
        };
        tracing::warn!(key_hash = %key_hash, errors = errors.len(), "key deletion partially failed");
    }

    Ok(Json(
        first_response.unwrap_or(serde_json::json!({"status": "deleted"})),
    ))
}

// ---------------------------------------------------------------------------
// GET /keys — list keys (proxy to first node)
// ---------------------------------------------------------------------------

async fn list_keys_handler(
    Extension(config): Extension<Arc<Config>>,
) -> Result<Json<Value>, StatusCode> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let address = config
        .nodes
        .first()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let (status, text) = client
        .get_raw(&address.address, "/keys")
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "list keys failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    if (200..300).contains(&status) {
        Ok(Json(serde_json::from_str(&text).unwrap_or(Value::Null)))
    } else {
        Err(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
    }
}

// ---------------------------------------------------------------------------
// GET /keys/{key} — get key (proxy to first node)
// ---------------------------------------------------------------------------

async fn get_key_handler(
    Path(key): Path<String>,
    Extension(config): Extension<Arc<Config>>,
) -> Result<Json<Value>, StatusCode> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let address = config
        .nodes
        .first()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let path = format!("/keys/{key}");
    let (status, text) = client.get_raw(&address.address, &path).await.map_err(|e| {
        tracing::error!(error = %e, "get key failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if (200..300).contains(&status) {
        Ok(Json(serde_json::from_str(&text).unwrap_or(Value::Null)))
    } else {
        Err(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
    }
}
