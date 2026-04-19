use axum::extract::Path;
use axum::http::StatusCode;
use axum::{routing::any, Extension, Json, Router};
use miroir_core::config::Config;
use miroir_core::scatter::{PreflightRequest, PreflightResponse, TermStats};
use miroir_core::topology::Topology;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Node client for communicating with Meilisearch.
pub struct MeilisearchClient {
    client: Client,
    master_key: String,
}

impl MeilisearchClient {
    /// Create a new Meilisearch client.
    pub fn new(master_key: String) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(5000))
            .build()
            .expect("Failed to create HTTP client");

        Self { client, master_key }
    }

    /// Get index statistics from Meilisearch.
    pub async fn get_index_stats(
        &self,
        address: &str,
        index_uid: &str,
    ) -> Result<u64, Box<dyn std::error::Error>> {
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

        let json: Value = response.json().await?;
        json.get("numberOfDocuments")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| "Failed to parse numberOfDocuments".into())
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
            return Err(format!("Failed to search for term '{}': {}", term, response.status()).into());
        }

        let json: Value = response.json().await?;
        json.get("estimatedTotalHits")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| "Failed to parse estimatedTotalHits".into())
    }

    /// Estimate average document length by sampling a few documents.
    /// This is a best-effort estimate since Meilisearch doesn't expose avg doc length directly.
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
            // Return a default if we can't sample
            return Ok(500.0);
        }

        let json: Value = response.json().await?;
        let results = json.get("results").and_then(|v| v.as_array());

        if let Some(docs) = results {
            if docs.is_empty() {
                return Ok(500.0);
            }

            // Calculate average length by summing all field values' lengths
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

pub fn router() -> Router {
    Router::new()
        .route("/:index/_preflight", axum::routing::post(preflight_handler))
        .route("/", any(indexes_handler))
        .route("/:index", any(indexes_handler))
}

async fn indexes_handler(
    Path(_path): Path<Vec<String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    Err(StatusCode::NOT_IMPLEMENTED)
}

/// Preflight handler for gathering term statistics.
///
/// This endpoint implements the shard-side of the DFS (Distributed Frequency Search)
/// preflight phase. It:
/// 1. Gets total document count from index stats
/// 2. For each query term, performs a search to get document frequency
/// 3. Estimates average document length
/// 4. Returns aggregated term statistics
async fn preflight_handler(
    Path(index): Path<String>,
    Extension(config): Extension<Arc<Config>>,
    Extension(_topology): Extension<Arc<Topology>>,
    Json(body): Json<PreflightRequest>,
) -> Result<Json<PreflightResponse>, StatusCode> {
    // Use the first node from config for the preflight query
    let node = config
        .nodes
        .first()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let client = MeilisearchClient::new(config.node_master_key.clone());

    // Get total documents
    let total_docs = client
        .get_index_stats(&node.address, &index)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get index stats: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Estimate average document length (cached or estimated)
    let avg_doc_length = client
        .estimate_avg_doc_length(&node.address, &index)
        .await
        .unwrap_or(500.0);

    // Get document frequency for each term
    let mut term_stats = HashMap::new();

    for term in &body.terms {
        match client.get_term_df(&node.address, &index, term, &body.filter).await {
            Ok(df) => {
                term_stats.insert(term.clone(), TermStats { df });
            }
            Err(e) => {
                tracing::warn!("Failed to get DF for term '{}': {}", term, e);
                // Continue with other terms even if one fails
            }
        }
    }

    tracing::debug!(
        "Preflight for index '{}': {} docs, {} terms",
        index,
        total_docs,
        term_stats.len()
    );

    Ok(Json(PreflightResponse {
        total_docs,
        avg_doc_length,
        term_stats,
    }))
}
