//! HTTP client for communicating with Meilisearch nodes.

use miroir_core::scatter::{NodeClient, NodeError, PreflightRequest, PreflightResponse, SearchRequest, TermStats};
use miroir_core::topology::NodeId;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

/// HTTP client implementation for node communication.
pub struct HttpClient {
    client: Client,
    master_key: String,
    timeout_ms: u64,
}

impl HttpClient {
    /// Create a new HTTP client.
    pub fn new(master_key: String, timeout_ms: u64) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            master_key,
            timeout_ms,
        }
    }

    /// Build the search URL for a node and index.
    fn search_url(&self, address: &str, index_uid: &str) -> String {
        format!("{}/indexes/{}/search", address.trim_end_matches('/'), index_uid)
    }

    /// Build the preflight URL for a node and index.
    fn preflight_url(&self, address: &str, index_uid: &str) -> String {
        format!("{}/indexes/{}/_preflight", address.trim_end_matches('/'), index_uid)
    }
}

#[allow(async_fn_in_trait)]
impl NodeClient for HttpClient {
    async fn search_node(
        &self,
        _node: &NodeId,
        address: &str,
        request: &SearchRequest,
    ) -> std::result::Result<Value, NodeError> {
        let url = self.search_url(address, &request.index_uid);

        // Build the request body with global_idf if present
        let mut body = request.body.clone();

        // Inject global IDF into the request if present
        if let Some(global_idf) = &request.global_idf {
            body["_miroir_global_idf"] = serde_json::to_value(global_idf)
                .map_err(|e| NodeError::NetworkError(format!("Failed to serialize global_idf: {}", e)))?;
        }

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .json(&body)
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("Request failed: {}", e)))?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .map_err(|e| NodeError::NetworkError(format!("Failed to read response: {}", e)))?;

        if !status.is_success() {
            return Err(NodeError::HttpError {
                status: status.as_u16(),
                body: body_text,
            });
        }

        serde_json::from_str(&body_text).map_err(|e| {
            NodeError::NetworkError(format!("Failed to parse JSON response: {}", e))
        })
    }

    async fn preflight_node(
        &self,
        _node: &NodeId,
        address: &str,
        request: &PreflightRequest,
    ) -> std::result::Result<PreflightResponse, NodeError> {
        let base = address.trim_end_matches('/');

        // 1. Get total docs from Meilisearch stats endpoint
        let stats_url = format!("{}/indexes/{}/stats", base, request.index_uid);
        let stats_resp = self
            .client
            .get(&stats_url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("Stats request failed: {}", e)))?;

        if !stats_resp.status().is_success() {
            // Index not found or node unreachable — return empty stats
            return Ok(PreflightResponse {
                total_docs: 0,
                avg_doc_length: 0.0,
                term_stats: HashMap::new(),
            });
        }

        let stats_body: Value = stats_resp
            .json()
            .await
            .map_err(|e| NodeError::NetworkError(format!("Failed to parse stats: {}", e)))?;

        let total_docs = stats_body
            .get("numberOfDocuments")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        // 2. Get DF for each term via search with limit=0
        let mut term_stats = HashMap::new();
        let search_url = format!("{}/indexes/{}/search", base, request.index_uid);
        for term in &request.terms {
            let search_body = serde_json::json!({"q": term, "limit": 0});

            let search_resp = self
                .client
                .post(&search_url)
                .header("Authorization", format!("Bearer {}", self.master_key))
                .json(&search_body)
                .send()
                .await
                .map_err(|e| NodeError::NetworkError(format!("DF search failed for '{}': {}", term, e)))?;

            if search_resp.status().is_success() {
                let body: Value = search_resp
                    .json()
                    .await
                    .map_err(|e| NodeError::NetworkError(format!("Failed to parse DF response: {}", e)))?;
                let df = body
                    .get("estimatedTotalHits")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                term_stats.insert(term.clone(), TermStats { df });
            }
        }

        // 3. Estimate avg doc length (Meilisearch doesn't expose this directly;
        //    use a default. The BM25 score is mainly sensitive to IDF, not avgdl.)
        let avg_doc_length = 500.0;

        Ok(PreflightResponse {
            total_docs,
            avg_doc_length,
            term_stats,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_url_construction() {
        let client = HttpClient::new("test-key".into(), 5000);
        assert_eq!(
            client.search_url("http://localhost:7700", "my_index"),
            "http://localhost:7700/indexes/my_index/search"
        );
        assert_eq!(
            client.search_url("http://localhost:7700/", "my_index"),
            "http://localhost:7700/indexes/my_index/search"
        );
    }

    #[test]
    fn test_preflight_url_construction() {
        let client = HttpClient::new("test-key".into(), 5000);
        assert_eq!(
            client.preflight_url("http://localhost:7700", "my_index"),
            "http://localhost:7700/indexes/my_index/_preflight"
        );
    }
}
