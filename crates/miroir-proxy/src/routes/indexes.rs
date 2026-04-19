//! Index lifecycle endpoints: create, delete, stats, settings broadcast.
//!
//! Implements P2.4:
//! - `POST /indexes` — create index on every node; auto-add `_miroir_shard` to
//!   `filterableAttributes`; rollback on partial failure
//! - `DELETE /indexes/{uid}` — broadcast delete to every node
//! - `GET /indexes/{uid}/stats` — fan out, sum numberOfDocuments (logical count),
//!   merge fieldDistribution
//! - `PATCH /indexes/{uid}/settings/*` — sequential settings broadcast with rollback
//! - `GET /indexes/{uid}/settings/*` — proxy read from first node
//! - `GET /stats` — global stats across all indexes

use axum::extract::{Extension, Path};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use miroir_core::api_error::{MeilisearchError, MiroirCode};
use miroir_core::config::Config;
use miroir_core::scatter::{PreflightRequest, PreflightResponse, TermStats};
use miroir_core::topology::Topology;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::routes::{admin_endpoints::AppState, documents};

/// Node client for communicating with Meilisearch.
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
            Ok((status, text)) => {
                tracing::warn!(
                    "failed to set _miroir_shard filterable on {}: HTTP {} — {}",
                    address, status, text
                );
            }
            Err(e) => {
                tracing::warn!(
                    "failed to set _miroir_shard filterable on {}: {}",
                    address, e
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
        tracing::error!("list indexes failed: {}", e);
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
        tracing::error!("get index failed: {}", e);
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
                Ok((status, text)) => {
                    tracing::error!(
                        node = %address,
                        status,
                        "index update rollback failed: {}",
                        text
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
                tracing::warn!("stats fan-out failed for {}: {}", address, e);
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
// Settings: PATCH /indexes/{uid}/settings — sequential broadcast with rollback
// ---------------------------------------------------------------------------

async fn update_settings_handler(
    Path(index): Path<String>,
    Extension(_state): Extension<Arc<AppState>>,
    Extension(config): Extension<Arc<Config>>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, MeilisearchError> {
    update_settings_broadcast(&config, &index, "/settings", &body).await
}

async fn update_settings_subpath_handler(
    Path((index, subpath)): Path<(String, String)>,
    Extension(_state): Extension<Arc<AppState>>,
    Extension(config): Extension<Arc<Config>>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, MeilisearchError> {
    let path = format!("/settings/{}", subpath);
    update_settings_broadcast(&config, &index, &path, &body).await
}

/// Sequential settings broadcast: apply to nodes one-by-one, rollback on failure.
///
/// Before applying, snapshots current settings from each node so rollback is lossless.
async fn update_settings_broadcast(
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
                Ok((status, text)) => {
                    tracing::error!(
                        node = %address,
                        status,
                        "settings rollback failed: {}",
                        text
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
        tracing::error!("get settings failed: {}", e);
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
        tracing::error!("get settings subpath failed: {}", e);
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
            tracing::error!("Failed to get index stats: {}", e);
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
