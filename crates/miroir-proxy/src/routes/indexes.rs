//! Index lifecycle endpoints: create, delete, stats, settings broadcast.
//!
//! Implements P2.4 and P5.5 §13.5:
//! - `POST /indexes` — create index on every node; auto-add `_miroir_shard` to
//!   `filterableAttributes`; rollback on partial failure
//! - `DELETE /indexes/{uid}` — broadcast delete to every node
//! - `GET /indexes/{uid}/stats` — fan out, sum numberOfDocuments (logical count),
//!   merge fieldDistribution
//! - `PATCH /indexes/{uid}/settings/*` — two-phase settings broadcast with verification
//! - `GET /indexes/{uid}/settings/*` — proxy read from first node
//! - `GET /stats` — global stats across all indexes

use axum::extract::{Extension, Path};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::future::join_all;
use miroir_core::api_error::{MeilisearchError, MiroirCode};
use miroir_core::config::Config;
use miroir_core::error::MiroirError;
use miroir_core::scatter::{PreflightRequest, PreflightResponse, TermStats};
use miroir_core::settings::{BroadcastPhase, SettingsBroadcast};
use miroir_core::topology::Topology;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use sha2::{Digest, Sha256};
use tokio::time::{timeout, Duration};

use crate::routes::{admin_endpoints::AppState, documents};

/// Convert MiroirError to MeilisearchError.
fn convert_miroir_error(e: MiroirError) -> MeilisearchError {
    match e {
        MiroirError::SettingsDivergence => MeilisearchError::new(
            MiroirCode::NoQuorum,
            "settings divergence detected across nodes",
        ),
        MiroirError::NotFound(msg) => MeilisearchError::new(
            MiroirCode::NoQuorum,
            format!("not found: {}", msg),
        ),
        MiroirError::InvalidState(msg) => MeilisearchError::new(
            MiroirCode::NoQuorum,
            format!("invalid state: {}", msg),
        ),
        _ => MeilisearchError::new(
            MiroirCode::NoQuorum,
            format!("settings broadcast error: {}", e),
        ),
    }
}

/// Node client for communicating with Meilisearch.
#[derive(Clone)]
pub struct MeilisearchClient {
    client: Client,
    master_key: String,
}

impl MeilisearchClient {
    pub fn new(master_key: String) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(10000))
            .build()
            .expect("Failed to create HTTP client");

        Self { client, master_key }
    }

    fn auth_header(&self) -> (&str, String) {
        ("Authorization", format!("Bearer {}", self.master_key))
    }

    /// POST to a node — generic broadcast helper.
    pub async fn post_raw(
        &self,
        address: &str,
        path: &str,
        body: &Value,
    ) -> Result<(u16, String), String> {
        let url = format!("{}{}", address.trim_end_matches('/'), path);
        let resp = self
            .client
            .post(&url)
            .header(self.auth_header().0, &self.auth_header().1)
            .json(body)
            .send()
            .await
            .map_err(|e| format!("request failed: {}", e))?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(|e| format!("read body: {}", e))?;
        Ok((status, text))
    }

    /// PATCH to a node — generic broadcast helper.
    pub async fn patch_raw(
        &self,
        address: &str,
        path: &str,
        body: &Value,
    ) -> Result<(u16, String), String> {
        let url = format!("{}{}", address.trim_end_matches('/'), path);
        let resp = self
            .client
            .patch(&url)
            .header(self.auth_header().0, &self.auth_header().1)
            .json(body)
            .send()
            .await
            .map_err(|e| format!("request failed: {}", e))?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(|e| format!("read body: {}", e))?;
        Ok((status, text))
    }

    /// DELETE on a node — generic helper.
    pub async fn delete_raw(
        &self,
        address: &str,
        path: &str,
    ) -> Result<(u16, String), String> {
        let url = format!("{}{}", address.trim_end_matches('/'), path);
        let resp = self
            .client
            .delete(&url)
            .header(self.auth_header().0, &self.auth_header().1)
            .send()
            .await
            .map_err(|e| format!("request failed: {}", e))?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(|e| format!("read body: {}", e))?;
        Ok((status, text))
    }

    /// GET from a node — generic helper.
    pub async fn get_raw(
        &self,
        address: &str,
        path: &str,
    ) -> Result<(u16, String), String> {
        let url = format!("{}{}", address.trim_end_matches('/'), path);
        let resp = self
            .client
            .get(&url)
            .header(self.auth_header().0, &self.auth_header().1)
            .send()
            .await
            .map_err(|e| format!("request failed: {}", e))?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(|e| format!("read body: {}", e))?;
        Ok((status, text))
    }

    /// Get index statistics from Meilisearch.
    pub async fn get_index_stats(
        &self,
        address: &str,
        index_uid: &str,
    ) -> Result<Value, Box<dyn std::error::Error>> {
        let url = format!("{}/indexes/{}/stats", address.trim_end_matches('/'), index_uid);
        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(format!("Failed to get stats: {}", response.status()).into());
        }

        response.json().await.map_err(|e| e.into())
    }

    /// Get document frequency for a single term by searching.
    pub async fn get_term_df(
        &self,
        address: &str,
        index_uid: &str,
        term: &str,
        filter: &Option<Value>,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let url = format!(
            "{}/indexes/{}/search",
            address.trim_end_matches('/'),
            index_uid
        );

        let mut body = serde_json::json!({
            "q": term,
            "limit": 0,
        });

        if let Some(f) = filter {
            body["filter"] = f.clone();
        }

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(format!("DF lookup failed: HTTP {}", response.status()).into());
        }

        let json: Value = response.json().await?;
        json.get("estimatedTotalHits")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| "Failed to parse estimatedTotalHits".into())
    }

    /// Estimate average document length by sampling a few documents.
    pub async fn estimate_avg_doc_length(
        &self,
        address: &str,
        index_uid: &str,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let url = format!(
            "{}/indexes/{}/documents",
            address.trim_end_matches('/'),
            index_uid
        );

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .query(&[("limit", "10")])
            .send()
            .await?;

        if !response.status().is_success() {
            return Ok(500.0);
        }

        let json: Value = response.json().await?;
        let results = json.get("results").and_then(|v| v.as_array());

        if let Some(docs) = results {
            if docs.is_empty() {
                return Ok(500.0);
            }

            let mut total_length = 0u64;
            let mut field_count = 0u64;

            for doc in docs {
                if let Some(obj) = doc.as_object() {
                    for (_key, value) in obj {
                        if let Some(s) = value.as_str() {
                            total_length += s.len() as u64;
                            field_count += 1;
                        }
                    }
                }
            }

            if field_count > 0 {
                return Ok(total_length as f64 / field_count as f64);
            }
        }

        Ok(500.0)
    }
}

/// Collect all healthy node addresses from config.
fn all_node_addresses(config: &Config) -> Vec<String> {
    config.nodes.iter().map(|n| n.address.clone()).collect()
}

/// Compute a fingerprint (SHA256) of settings as canonical JSON.
///
/// Canonical JSON sorts object keys to ensure consistent fingerprints
/// regardless of key ordering in the input.
fn fingerprint_settings(settings: &Value) -> String {
    // Canonicalize: sort object keys, no extra whitespace.
    let canonical = if settings.is_object() {
        if let Some(obj) = settings.as_object() {
            // Collect and sort keys.
            let mut sorted_entries: Vec<_> = obj.iter().collect();
            sorted_entries.sort_by_key(|&(k, _)| k);
            // Reconstruct as a Map with sorted keys.
            let mut sorted_map = serde_json::Map::new();
            for (key, value) in sorted_entries {
                sorted_map.insert(key.clone(), value.clone());
            }
            serde_json::to_string(&sorted_map).unwrap_or_default()
        } else {
            serde_json::to_string(settings).unwrap_or_default()
        }
    } else {
        serde_json::to_string(settings).unwrap_or_default()
    };

    // SHA256 hash.
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/", post(create_index_handler).get(list_indexes_handler))
        .route(
            "/:index",
            get(get_index_handler)
                .patch(update_index_handler)
                .delete(delete_index_handler),
        )
        .route("/:index/stats", get(get_index_stats_handler))
        .route(
            "/:index/settings",
            get(get_settings_handler).patch(update_settings_handler),
        )
        .route(
            "/:index/settings/*subpath",
            get(get_settings_subpath_handler).patch(update_settings_subpath_handler),
        )
        .route("/:index/_preflight", post(preflight_handler))
        .nest("/:index/documents", documents::router::<S>())
}

// ---------------------------------------------------------------------------
// POST /indexes — create index (broadcast + _miroir_shard)
// ---------------------------------------------------------------------------

async fn create_index_handler(
    Extension(_state): Extension<Arc<AppState>>,
    Extension(config): Extension<Arc<Config>>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, MeilisearchError> {
    let uid = body
        .get("uid")
        .and_then(|v| v.as_str())
        .ok_or_else(|| MeilisearchError::new(
            MiroirCode::PrimaryKeyRequired,
            "index creation requires a `uid` field",
        ))?;

    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes = all_node_addresses(&config);
    let mut created_on: Vec<String> = Vec::new();
    let mut first_response: Option<Value> = None;

    // Phase 1: Create index on every node sequentially
    for address in &nodes {
        match client.post_raw(address, "/indexes", &body).await {
            Ok((status, text)) if status >= 200 && status < 300 => {
                if first_response.is_none() {
                    first_response = serde_json::from_str(&text).ok();
                }
                created_on.push(address.clone());
            }
            Ok((status, text)) => {
                // Rollback: delete index on all previously created nodes
                rollback_delete_index(&client, uid, &created_on).await;
                let msg = format!(
                    "index creation failed on node {}: HTTP {} — {}",
                    address, status, text
                );
                return Err(forward_or_miroir(status, &text, &msg));
            }
            Err(e) => {
                rollback_delete_index(&client, uid, &created_on).await;
                return Err(MeilisearchError::new(
                    MiroirCode::NoQuorum,
                    format!("index creation failed on node {}: {}", address, e),
                ));
            }
        }
    }

    // Phase 2: Add `_miroir_shard` to filterableAttributes on every node.
    // Read current filterableAttributes from first node, merge `_miroir_shard`,
    // then broadcast the merged list to all nodes.
    let mut merged_attrs: Vec<Value> = vec![serde_json::json!("_miroir_shard")];

    if let Some(first_addr) = nodes.first() {
        match client.get_raw(first_addr, &format!("/indexes/{}/settings", uid)).await {
            Ok((status, text)) if status >= 200 && status < 300 => {
                if let Ok(settings) = serde_json::from_str::<Value>(&text) {
                    if let Some(existing) = settings.get("filterableAttributes").and_then(|v| v.as_array()) {
                        for attr in existing {
                            let attr_str = attr.as_str().unwrap_or("");
                            if attr_str != "_miroir_shard" && !attr_str.is_empty() {
                                merged_attrs.push(attr.clone());
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let filterable_patch = serde_json::json!({
        "filterableAttributes": merged_attrs
    });

    let mut patch_ok: Vec<String> = Vec::new();
    for address in &nodes {
        let path = format!("/indexes/{}/settings", uid);
        match client.patch_raw(address, &path, &filterable_patch).await {
            Ok((_status, _text)) if _status >= 200 && _status < 300 => {
                patch_ok.push(address.clone());
            }
            Ok((status, _text)) => {
                tracing::warn!(
                    node = %address,
                    status,
                    "failed to set _miroir_shard filterable"
                );
            }
            Err(e) => {
                tracing::warn!(
                    node = %address,
                    error = %e,
                    "failed to set _miroir_shard filterable"
                );
            }
        }
    }

    if patch_ok.len() != nodes.len() {
        tracing::warn!(
            created = patch_ok.len(),
            total = nodes.len(),
            "_miroir_shard filterableAttributes not set on all nodes"
        );
    }

    tracing::info!(
        index_uid = uid,
        nodes = nodes.len(),
        "index created on all nodes"
    );

    Ok(Json(first_response.unwrap_or(serde_json::json!({"uid": uid, "status": "created"}))))
}

async fn rollback_delete_index(client: &MeilisearchClient, uid: &str, nodes: &[String]) {
    for address in nodes {
        let path = format!("/indexes/{}", uid);
        match client.delete_raw(address, &path).await {
            Ok(_) => tracing::info!(node = %address, "rollback: deleted index"),
            Err(e) => tracing::error!(node = %address, error = %e, "rollback: failed to delete index"),
        }
    }
}

// ---------------------------------------------------------------------------
// GET /indexes — list indexes (proxy to first node)
// ---------------------------------------------------------------------------

async fn list_indexes_handler(
    Extension(config): Extension<Arc<Config>>,
) -> Result<Json<Value>, StatusCode> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let address = config.nodes.first().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let (status, text) = client.get_raw(&address.address, "/indexes").await.map_err(|e| {
        tracing::error!(error = %e, "list indexes failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if status >= 200 && status < 300 {
        let json: Value = serde_json::from_str(&text).unwrap_or(serde_json::json!({"results": []}));
        Ok(Json(json))
    } else {
        Err(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
    }
}

// ---------------------------------------------------------------------------
// GET /indexes/{uid} — get single index (proxy)
// ---------------------------------------------------------------------------

async fn get_index_handler(
    Path(index): Path<String>,
    Extension(config): Extension<Arc<Config>>,
) -> Result<Json<Value>, StatusCode> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let address = config.nodes.first().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let path = format!("/indexes/{}", index);
    let (status, text) = client.get_raw(&address.address, &path).await.map_err(|e| {
        tracing::error!(error = %e, "get index failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if status >= 200 && status < 300 {
        Ok(Json(serde_json::from_str(&text).unwrap_or(Value::Null)))
    } else {
        Err(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
    }
}

// ---------------------------------------------------------------------------
// PATCH /indexes/{uid} — update index metadata (broadcast with rollback)
// ---------------------------------------------------------------------------

async fn update_index_handler(
    Path(index): Path<String>,
    Extension(_state): Extension<Arc<AppState>>,
    Extension(config): Extension<Arc<Config>>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, MeilisearchError> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes = all_node_addresses(&config);
    let path = format!("/indexes/{}", index);

    // Snapshot current index state from all nodes before applying changes
    let mut snapshots: Vec<(String, Value)> = Vec::new();
    for address in &nodes {
        match client.get_raw(address, &path).await {
            Ok((status, text)) if status >= 200 && status < 300 => {
                let snapshot: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                snapshots.push((address.clone(), snapshot));
            }
            Ok((status, text)) => {
                return Err(forward_or_miroir(
                    status,
                    &text,
                    &format!("failed to snapshot index on {}: HTTP {}", address, status),
                ));
            }
            Err(e) => {
                return Err(MeilisearchError::new(
                    MiroirCode::NoQuorum,
                    format!("failed to snapshot index on {}: {}", address, e),
                ));
            }
        }
    }

    // Apply update sequentially to each node
    let mut applied: Vec<String> = Vec::new();
    let mut first_response: Option<Value> = None;

    for (address, _) in &snapshots {
        match client.patch_raw(address, &path, &body).await {
            Ok((status, text)) if status >= 200 && status < 300 => {
                if first_response.is_none() {
                    first_response = serde_json::from_str(&text).ok();
                }
                applied.push(address.clone());
            }
            Ok((status, text)) => {
                rollback_index_update(&client, &path, &snapshots, &applied).await;
                let msg = format!(
                    "index update failed on {}: HTTP {} — {}",
                    address, status, text
                );
                return Err(forward_or_miroir(status, &text, &msg));
            }
            Err(e) => {
                rollback_index_update(&client, &path, &snapshots, &applied).await;
                return Err(MeilisearchError::new(
                    MiroirCode::NoQuorum,
                    format!("index update failed on {}: {}", address, e),
                ));
            }
        }
    }

    Ok(Json(first_response.unwrap_or(serde_json::json!({"uid": index, "status": "updated"}))))
}

/// Rollback index metadata updates by restoring pre-change snapshots.
async fn rollback_index_update(
    client: &MeilisearchClient,
    path: &str,
    snapshots: &[(String, Value)],
    applied: &[String],
) {
    for address in applied {
        if let Some((_, snapshot)) = snapshots.iter().find(|(a, _)| a == address) {
            match client.patch_raw(address, path, snapshot).await {
                Ok((_status, _text)) if _status >= 200 && _status < 300 => {
                    tracing::info!(node = %address, "index update rollback succeeded");
                }
                Ok((status, _text)) => {
                    tracing::error!(
                        node = %address,
                        status,
                        "index update rollback failed"
                    );
                }
                Err(e) => {
                    tracing::error!(node = %address, error = %e, "index update rollback failed");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// DELETE /indexes/{uid} — broadcast delete
// ---------------------------------------------------------------------------

async fn delete_index_handler(
    Path(index): Path<String>,
    Extension(_state): Extension<Arc<AppState>>,
    Extension(config): Extension<Arc<Config>>,
) -> Result<Json<Value>, MeilisearchError> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes = all_node_addresses(&config);
    let mut first_response: Option<Value> = None;
    let mut errors: Vec<String> = Vec::new();

    for address in &nodes {
        let path = format!("/indexes/{}", index);
        match client.delete_raw(address, &path).await {
            Ok((status, text)) if status >= 200 && status < 300 => {
                if first_response.is_none() {
                    first_response = serde_json::from_str(&text).ok();
                }
            }
            Ok((status, text)) => {
                errors.push(format!("{}: HTTP {} — {}", address, status, text));
            }
            Err(e) => {
                errors.push(format!("{}: {}", address, e));
            }
        }
    }

    if !errors.is_empty() && first_response.is_none() {
        return Err(MeilisearchError::new(
            MiroirCode::NoQuorum,
            format!("index deletion failed on all nodes: {}", errors.join("; ")),
        ));
    }

    if !errors.is_empty() {
        tracing::warn!(
            index_uid = %index,
            errors = errors.len(),
            "index deletion partially failed"
        );
    }

    Ok(Json(first_response.unwrap_or(serde_json::json!({"taskUid": 0, "status": "enqueued"}))))
}

// ---------------------------------------------------------------------------
// GET /indexes/{uid}/stats — fan out, aggregate
// ---------------------------------------------------------------------------

async fn get_index_stats_handler(
    Path(index): Path<String>,
    Extension(_state): Extension<Arc<AppState>>,
    Extension(config): Extension<Arc<Config>>,
) -> Result<Json<Value>, MeilisearchError> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes = all_node_addresses(&config);

    let mut total_docs: u64 = 0;
    let mut field_distribution: HashMap<String, u64> = HashMap::new();
    let mut success_count = 0;

    for address in &nodes {
        match client.get_index_stats(address, &index).await {
            Ok(stats) => {
                success_count += 1;
                if let Some(n) = stats.get("numberOfDocuments").and_then(|v| v.as_u64()) {
                    total_docs += n;
                }
                if let Some(fd) = stats.get("fieldDistribution").and_then(|v| v.as_object()) {
                    for (field, count) in fd {
                        if let Some(c) = count.as_u64() {
                            *field_distribution.entry(field.clone()).or_insert(0) += c;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(node = %address, error = %e, "stats fan-out failed");
            }
        }
    }

    if success_count == 0 {
        return Err(MeilisearchError::new(
            MiroirCode::NoQuorum,
            format!("stats unavailable for index `{}`: all nodes failed", index),
        ));
    }

    // Compute logical doc count: total_docs / (RG × RF)
    let rg = config.replica_groups as u64;
    let rf = config.replication_factor as u64;
    let divisor = rg * rf;
    let logical_docs = if divisor > 0 { total_docs / divisor } else { total_docs };

    Ok(Json(serde_json::json!({
        "numberOfDocuments": logical_docs,
        "isIndexing": false,
        "fieldDistribution": field_distribution,
    })))
}

// ---------------------------------------------------------------------------
// GET /stats — global stats across all indexes
// ---------------------------------------------------------------------------

pub async fn global_stats_handler(
    Extension(_state): Extension<Arc<AppState>>,
    Extension(config): Extension<Arc<Config>>,
) -> Result<Json<Value>, MeilisearchError> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes = all_node_addresses(&config);

    // Get list of indexes from first node
    let first_address = nodes.first().ok_or_else(|| MeilisearchError::new(
        MiroirCode::NoQuorum,
        "no nodes configured",
    ))?;

    let (status, text) = client.get_raw(first_address, "/indexes").await.map_err(|e| {
        MeilisearchError::new(MiroirCode::NoQuorum, format!("failed to list indexes: {}", e))
    })?;

    if status < 200 || status >= 300 {
        return Err(MeilisearchError::new(MiroirCode::NoQuorum, "failed to list indexes"));
    }

    let indexes: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    let index_list = indexes
        .get("results")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut total_docs: u64 = 0;
    let mut total_field_distribution: HashMap<String, u64> = HashMap::new();

    for idx in &index_list {
        if let Some(uid) = idx.get("uid").and_then(|v| v.as_str()) {
            for address in &nodes {
                match client.get_index_stats(address, uid).await {
                    Ok(stats) => {
                        if let Some(n) = stats.get("numberOfDocuments").and_then(|v| v.as_u64()) {
                            total_docs += n;
                        }
                        if let Some(fd) = stats.get("fieldDistribution").and_then(|v| v.as_object()) {
                            for (field, count) in fd {
                                if let Some(c) = count.as_u64() {
                                    *total_field_distribution.entry(field.clone()).or_insert(0) += c;
                                }
                            }
                        }
                    }
                    Err(_) => {}
                }
            }
        }
    }

    let rg = config.replica_groups as u64;
    let rf = config.replication_factor as u64;
    let divisor = rg * rf;
    let logical_docs = if divisor > 0 { total_docs / divisor } else { total_docs };

    Ok(Json(serde_json::json!({
        "databaseSize": 0,
        "lastUpdate": "",
        "indexes": {},
        "numberOfDocuments": logical_docs,
        "fieldDistribution": total_field_distribution,
    })))
}

// ---------------------------------------------------------------------------
// Settings: PATCH /indexes/{uid}/settings — two-phase broadcast with verification (§13.5)
// ---------------------------------------------------------------------------

async fn update_settings_handler(
    Path(index): Path<String>,
    Extension(state): Extension<Arc<AppState>>,
    Extension(config): Extension<Arc<Config>>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, MeilisearchError> {
    two_phase_settings_broadcast(&state, &config, &index, "/settings", &body).await
}

async fn update_settings_subpath_handler(
    Path((index, subpath)): Path<(String, String)>,
    Extension(state): Extension<Arc<AppState>>,
    Extension(config): Extension<Arc<Config>>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, MeilisearchError> {
    let path = format!("/settings/{}", subpath);
    two_phase_settings_broadcast(&state, &config, &index, &path, &body).await
}

/// Two-phase settings broadcast (§13.5):
/// Phase 1 (Propose): PATCH all nodes in parallel, collect task UIDs
/// Phase 2 (Verify): GET settings from all nodes, verify SHA256 fingerprints
/// Phase 3 (Commit): Increment settings_version, persist to task store
///
/// On hash mismatch, retry with exponential backoff up to max_repair_retries.
/// If unrepairable, raise MiroirSettingsDivergence alert and freeze writes.
async fn two_phase_settings_broadcast(
    state: &AppState,
    config: &Config,
    index: &str,
    settings_path: &str,
    body: &Value,
) -> Result<Json<Value>, MeilisearchError> {
    // Use sequential strategy for rollback compatibility
    if config.settings_broadcast.strategy == "sequential" {
        return update_settings_broadcast_legacy(&config, index, settings_path, body).await;
    }

    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes = all_node_addresses(config);
    let full_path = format!("/indexes/{}{}", index, settings_path);

    // Check if a broadcast is already in flight
    if state.settings_broadcast.is_in_flight(index).await {
        return Err(MeilisearchError::new(
            MiroirCode::IndexAlreadyExists,
            format!("settings broadcast already in flight for index '{}'", index),
        ));
    }

    // Compute expected fingerprint of proposed settings
    let expected_fingerprint = fingerprint_settings(body);

    // Set phase to Propose (1)
    state.metrics.set_settings_broadcast_phase(index, 1);

    // Phase 1: Propose - PATCH all nodes in parallel
    let propose_fut = async {
        let mut node_task_uids = HashMap::new();
        let mut first_response: Option<Value> = None;
        let mut errors: Vec<String> = Vec::new();

        for address in &nodes {
            match client.patch_raw(address, &full_path, body).await {
                Ok((status, text)) if status >= 200 && status < 300 => {
                    if first_response.is_none() {
                        first_response = serde_json::from_str(&text).ok();
                    }
                    // Extract taskUid if present in response
                    if let Ok(resp) = serde_json::from_str::<Value>(&text) {
                        if let Some(task_uid) = resp.get("taskUid").and_then(|v| v.as_u64()) {
                            node_task_uids.insert(address.clone(), task_uid);
                        }
                    }
                }
                Ok((status, text)) => {
                    errors.push(format!("{}: HTTP {} — {}", address, status, text));
                }
                Err(e) => {
                    errors.push(format!("{}: {}", address, e));
                }
            }
        }

        (node_task_uids, first_response, errors)
    };

    let (node_task_uids, first_response, propose_errors) = propose_fut.await;

    if !propose_errors.is_empty() {
        state.metrics.clear_settings_broadcast_phase(index);
        return Err(MeilisearchError::new(
            MiroirCode::NoQuorum,
            format!("Phase 1 propose failed: {}", propose_errors.join("; ")),
        ));
    }

    // Start broadcast tracking
    state.settings_broadcast.start_propose(index.to_string(), body).await
        .map_err(convert_miroir_error)?;

    // Set phase to Verify (2)
    state.metrics.set_settings_broadcast_phase(index, 2);

    // Wait for all node tasks to complete (with timeout)
    let verify_timeout = Duration::from_secs(config.settings_broadcast.verify_timeout_s);

    // Define verify logic as a closure that can be called multiple times
    // Uses parallel execution for performance (P5.5.b)
    let run_verify = || {
        let client = client.clone();
        let nodes = nodes.clone();
        let index = index.to_string();
        let settings_path = settings_path.to_string();
        async move {
            // Parallel verification: spawn GET requests to all nodes concurrently
            let verify_tasks: Vec<_> = nodes.iter().map(|address| {
                let client = client.clone();
                let address = address.clone();
                let path = format!("/indexes/{}{}", index, settings_path);
                async move {
                    (address.clone(), client.get_raw(&address, &path).await)
                }
            }).collect();

            let results: Vec<(String, Result<(u16, String), String>)> = join_all(verify_tasks).await;

            let mut node_hashes = HashMap::new();
            let mut verify_errors: Vec<String> = Vec::new();

            for (address, result) in results {
                match result {
                    Ok((status, text)) if status >= 200 && status < 300 => {
                        if let Ok(settings) = serde_json::from_str::<Value>(&text) {
                            let hash = fingerprint_settings(&settings);
                            node_hashes.insert(address, hash);
                        }
                    }
                    Ok((status, text)) => {
                        verify_errors.push(format!("{}: HTTP {} — {}", address, status, text));
                    }
                    Err(e) => {
                        verify_errors.push(format!("{}: {}", address, e));
                    }
                }
            }

            (node_hashes, verify_errors)
        }
    };

    let (mut node_hashes, verify_errors) = timeout(verify_timeout, run_verify())
        .await
        .map_err(|_| {
            MeilisearchError::new(
                MiroirCode::Timeout,
                "Phase 2 verify timed out",
            )
        })?;

    if !verify_errors.is_empty() {
        state.settings_broadcast.abort(
            index,
            format!("Phase 2 verify failed: {}", verify_errors.join("; ")),
        ).await.ok();
        return Err(MeilisearchError::new(
            MiroirCode::NoQuorum,
            format!("Phase 2 verify failed: {}", verify_errors.join("; ")),
        ));
    }

    // Enter verify phase and check hashes
    state.settings_broadcast.enter_verify(index, node_task_uids.clone()).await
        .map_err(convert_miroir_error)?;

    // Retry loop with exponential backoff for hash mismatches
    let mut retry_count = 0u32;
    let max_retries = config.settings_broadcast.max_repair_retries;

    loop {
        match state.settings_broadcast.verify_hashes(
            index,
            node_hashes.clone(),
            &expected_fingerprint,
        ).await {
            Ok(()) => break,
            Err(miroir_core::error::MiroirError::SettingsDivergence) => {
                state.metrics.inc_settings_hash_mismatch();
                retry_count += 1;
                if retry_count > max_retries {
                    state.settings_broadcast.abort(
                        index,
                        format!("max repair retries ({}) exceeded", max_retries),
                    ).await.ok();

                    // Freeze writes on this index if configured
                    if config.settings_broadcast.freeze_writes_on_unrepairable {
                        state.metrics.freeze_index_writes(index);
                        tracing::error!(
                            index = %index,
                            retries = max_retries,
                            "settings divergence unrepairable - freezing writes on index"
                        );
                    }

                    // Raise MiroirSettingsDivergence alert
                    state.metrics.raise_settings_divergence_alert(index);

                    return Err(MeilisearchError::new(
                        MiroirCode::NoQuorum,
                        format!("settings divergence detected after {} retries - writes frozen on index", max_retries),
                    ));
                }

                // Exponential backoff: 2^retry_count seconds, max 60s
                let backoff_ms = 1000 * (1u64 << (retry_count - 1).min(5));
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;

                // Identify mismatched nodes and reissue PATCH to them
                let mismatched_nodes: Vec<_> = node_hashes.iter()
                    .filter(|(_, hash)| hash.as_str() != expected_fingerprint)
                    .map(|(node, _)| node.clone())
                    .collect();

                tracing::warn!(
                    index = %index,
                    retry = retry_count,
                    mismatched_nodes = ?mismatched_nodes,
                    "settings hash mismatch detected - reissuing PATCH to mismatched nodes"
                );

                // Reissue PATCH to mismatched nodes (repair)
                let mut repair_errors: Vec<String> = Vec::new();
                for address in &mismatched_nodes {
                    match client.patch_raw(address, &full_path, body).await {
                        Ok((status, _text)) if status >= 200 && status < 300 => {
                            tracing::info!(
                                node = %address,
                                index = %index,
                                retry = retry_count,
                                "successfully reissued settings PATCH"
                            );
                        }
                        Ok((status, text)) => {
                            repair_errors.push(format!("{}: HTTP {} — {}", address, status, text));
                        }
                        Err(e) => {
                            repair_errors.push(format!("{}: {}", address, e));
                        }
                    }
                }

                if !repair_errors.is_empty() {
                    state.settings_broadcast.abort(
                        index,
                        format!("repair reissue failed: {}", repair_errors.join("; ")),
                    ).await.ok();
                    return Err(MeilisearchError::new(
                        MiroirCode::NoQuorum,
                        format!("repair reissue failed: {}", repair_errors.join("; ")),
                    ));
                }

                // Re-run verify phase after repair
                let (new_hashes, new_errors) = run_verify().await;
                if !new_errors.is_empty() {
                    state.settings_broadcast.abort(
                        index,
                        format!("re-verify failed: {}", new_errors.join("; ")),
                    ).await.ok();
                    return Err(MeilisearchError::new(
                        MiroirCode::NoQuorum,
                        format!("re-verify failed: {}", new_errors.join("; ")),
                    ));
                }
                node_hashes = new_hashes;
            }
            Err(e) => {
                state.settings_broadcast.abort(index, e.to_string()).await.ok();
                return Err(MeilisearchError::new(
                    MiroirCode::NoQuorum,
                    e.to_string(),
                ));
            }
        }
    }

    // Phase 3: Commit - increment settings version
    let new_version = state.settings_broadcast.commit(index).await
        .map_err(convert_miroir_error)?;

    // Update settings version metric
    state.metrics.set_settings_version(index, new_version);
    state.metrics.clear_settings_broadcast_phase(index);

    tracing::info!(
        index = %index,
        settings_version = new_version,
        nodes = nodes.len(),
        "settings broadcast committed successfully"
    );

    // Complete and remove from in-flight tracking
    state.settings_broadcast.complete(index).await.ok();

    Ok(Json(first_response.unwrap_or(serde_json::json!({
        "taskUid": 0,
        "status": "enqueued",
        "settingsVersion": new_version,
    }))))
}

/// Legacy sequential settings broadcast: apply to nodes one-by-one, rollback on failure.
///
/// Kept for rollback compatibility when strategy: sequential.
async fn update_settings_broadcast_legacy(
    config: &Config,
    index: &str,
    settings_path: &str,
    body: &Value,
) -> Result<Json<Value>, MeilisearchError> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes = all_node_addresses(config);
    let full_path = format!("/indexes/{}{}", index, settings_path);

    // Snapshot current settings from all nodes before applying changes
    let mut snapshots: Vec<(String, Value)> = Vec::new();
    for address in &nodes {
        match client.get_raw(address, &full_path).await {
            Ok((status, text)) if status >= 200 && status < 300 => {
                let snapshot: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                snapshots.push((address.clone(), snapshot));
            }
            Ok((status, text)) => {
                return Err(forward_or_miroir(
                    status,
                    &text,
                    &format!("failed to snapshot settings on {}: HTTP {}", address, status),
                ));
            }
            Err(e) => {
                return Err(MeilisearchError::new(
                    MiroirCode::NoQuorum,
                    format!("failed to snapshot settings on {}: {}", address, e),
                ));
            }
        }
    }

    // Apply settings sequentially
    let mut applied: Vec<String> = Vec::new();
    let mut first_response: Option<Value> = None;

    for (address, _snapshot) in &snapshots {
        match client.patch_raw(address, &full_path, body).await {
            Ok((status, text)) if status >= 200 && status < 300 => {
                if first_response.is_none() {
                    first_response = serde_json::from_str(&text).ok();
                }
                applied.push(address.clone());
            }
            Ok((status, text)) => {
                // Rollback all previously applied nodes
                rollback_settings(&client, &full_path, &snapshots, &applied).await;
                let msg = format!(
                    "settings update failed on {}: HTTP {} — {}",
                    address, status, text
                );
                return Err(forward_or_miroir(status, &text, &msg));
            }
            Err(e) => {
                rollback_settings(&client, &full_path, &snapshots, &applied).await;
                return Err(MeilisearchError::new(
                    MiroirCode::NoQuorum,
                    format!("settings update failed on {}: {}", address, e),
                ));
            }
        }
    }

    Ok(Json(first_response.unwrap_or(serde_json::json!({"taskUid": 0, "status": "enqueued"}))))
}

/// Rollback settings on previously-applied nodes using pre-change snapshots.
async fn rollback_settings(
    client: &MeilisearchClient,
    full_path: &str,
    snapshots: &[(String, Value)],
    applied: &[String],
) {
    for address in applied {
        // Find the snapshot for this address
        if let Some((_, snapshot)) = snapshots.iter().find(|(a, _)| a == address) {
            match client.patch_raw(address, full_path, snapshot).await {
                Ok((_status, _text)) if _status >= 200 && _status < 300 => {
                    tracing::info!(node = %address, "settings rollback succeeded");
                }
                Ok((status, _text)) => {
                    tracing::error!(
                        node = %address,
                        status,
                        "settings rollback failed"
                    );
                }
                Err(e) => {
                    tracing::error!(node = %address, error = %e, "settings rollback failed");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// GET /indexes/{uid}/settings — proxy to first node
// ---------------------------------------------------------------------------

async fn get_settings_handler(
    Path(index): Path<String>,
    Extension(config): Extension<Arc<Config>>,
) -> Result<Json<Value>, StatusCode> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let address = config.nodes.first().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let path = format!("/indexes/{}/settings", index);
    let (status, text) = client.get_raw(&address.address, &path).await.map_err(|e| {
        tracing::error!(error = %e, "get settings failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if status >= 200 && status < 300 {
        Ok(Json(serde_json::from_str(&text).unwrap_or(Value::Null)))
    } else {
        Err(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
    }
}

async fn get_settings_subpath_handler(
    Path((index, subpath)): Path<(String, String)>,
    Extension(config): Extension<Arc<Config>>,
) -> Result<Json<Value>, StatusCode> {
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let address = config.nodes.first().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let path = format!("/indexes/{}/settings/{}", index, subpath);
    let (status, text) = client.get_raw(&address.address, &path).await.map_err(|e| {
        tracing::error!(error = %e, "get settings subpath failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if status >= 200 && status < 300 {
        Ok(Json(serde_json::from_str(&text).unwrap_or(Value::Null)))
    } else {
        Err(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
    }
}

// ---------------------------------------------------------------------------
// POST /indexes/{uid}/_preflight — DFS preflight
// ---------------------------------------------------------------------------

async fn preflight_handler(
    Path(index): Path<String>,
    Extension(config): Extension<Arc<Config>>,
    Extension(_topology): Extension<Arc<Topology>>,
    Json(body): Json<PreflightRequest>,
) -> Result<Json<PreflightResponse>, StatusCode> {
    let node = config
        .nodes
        .first()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let client = MeilisearchClient::new(config.node_master_key.clone());

    let total_docs = client
        .get_index_stats(&node.address, &index)
        .await
        .and_then(|v| {
            v.get("numberOfDocuments")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| "Failed to parse numberOfDocuments".into())
        })
        .map_err(|e| {
            tracing::error!(error = %e, "failed to get index stats");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let avg_doc_length = client
        .estimate_avg_doc_length(&node.address, &index)
        .await
        .unwrap_or(500.0);

    let mut term_stats = HashMap::new();
    for term in &body.terms {
        match client.get_term_df(&node.address, &index, term, &body.filter).await {
            Ok(df) => {
                term_stats.insert(term.clone(), TermStats { df });
            }
            Err(e) => {
                tracing::warn!(term_len = term.len(), error = %e, "preflight DF lookup failed");
            }
        }
    }

    Ok(Json(PreflightResponse {
        total_docs,
        avg_doc_length,
        term_stats,
    }))
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

/// Try to forward a Meilisearch error from a node response; fall back to a Miroir error.
fn forward_or_miroir(_status: u16, body: &str, fallback_msg: &str) -> MeilisearchError {
    if let Some(meili_err) = MeilisearchError::forwarded(body) {
        meili_err
    } else {
        MeilisearchError::new(MiroirCode::NoQuorum, fallback_msg)
    }
}
