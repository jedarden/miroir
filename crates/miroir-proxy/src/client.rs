//! HTTP client for communicating with Meilisearch nodes.

use miroir_core::scatter::{
    DeleteByFilterRequest, DeleteByIdsRequest, DeleteResponse, FetchDocumentsRequest, NodeClient,
    NodeError, PreflightRequest, PreflightResponse, SearchRequest, TaskStatusRequest,
    TaskStatusResponse, TermStats, WriteRequest, WriteResponse,
};
use miroir_core::topology::NodeId;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// HTTP client implementation for node communication.
pub struct HttpClient {
    client: Client,
    master_key: String,
}

impl HttpClient {
    /// Create a new HTTP client.
    pub fn new(master_key: String, timeout_ms: u64) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .expect("Failed to create HTTP client");

        Self { client, master_key }
    }

    /// Build the search URL for a node and index.
    fn search_url(&self, address: &str, index_uid: &str) -> String {
        format!(
            "{}/indexes/{}/search",
            address.trim_end_matches('/'),
            index_uid
        )
    }

    /// Build the preflight URL for a node and index.
    #[allow(dead_code)]
    fn preflight_url(&self, address: &str, index_uid: &str) -> String {
        format!(
            "{}/indexes/{}/_preflight",
            address.trim_end_matches('/'),
            index_uid
        )
    }

    /// Build the documents URL for a node and index.
    fn documents_url(&self, address: &str, index_uid: &str) -> String {
        format!(
            "{}/indexes/{}/documents",
            address.trim_end_matches('/'),
            index_uid
        )
    }

    /// Build the task URL for a node.
    fn task_url(&self, address: &str, task_uid: u64) -> String {
        format!("{}/tasks/{}", address.trim_end_matches('/'), task_uid)
    }

    /// Static version of task_url for use in async blocks.
    fn task_url_static(address: &str, task_uid: u64) -> String {
        format!("{}/tasks/{}", address.trim_end_matches('/'), task_uid)
    }
}

#[allow(async_fn_in_trait)]
impl NodeClient for HttpClient {
    async fn search_node(
        &self,
        node: &NodeId,
        address: &str,
        request: &SearchRequest,
    ) -> std::result::Result<Value, NodeError> {
        let span = tracing::info_span!(
            "node_call",
            node_id = %node,
            address = %address,
            operation = "search",
            index = %request.index_uid,
        );
        let _guard = span.enter();

        let start = Instant::now();
        let url = self.search_url(address, &request.index_uid);

        let mut body = request.to_node_body();

        if let Some(global_idf) = &request.global_idf {
            body["_miroir_global_idf"] = serde_json::to_value(global_idf).map_err(|e| {
                NodeError::NetworkError(format!("Failed to serialize global_idf: {e}"))
            })?;
        }

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                tracing::warn!(
                    target: "miroir.node",
                    duration_ms = start.elapsed().as_millis() as u64,
                    error = %e,
                    "node call failed"
                );
                NodeError::NetworkError(format!("Request failed: {e}"))
            })?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .map_err(|e| NodeError::NetworkError(format!("Failed to read response: {e}")))?;

        let duration_ms = start.elapsed().as_millis() as u64;

        if !status.is_success() {
            tracing::debug!(
                target: "miroir.node",
                duration_ms,
                status = status.as_u16(),
                "node call error response"
            );
            return Err(NodeError::HttpError {
                status: status.as_u16(),
                body: body_text,
            });
        }

        tracing::debug!(
            target: "miroir.node",
            duration_ms,
            "node call completed"
        );

        serde_json::from_str(&body_text)
            .map_err(|e| NodeError::NetworkError(format!("Failed to parse JSON response: {e}")))
    }

    async fn write_documents(
        &self,
        node: &NodeId,
        address: &str,
        request: &WriteRequest,
    ) -> std::result::Result<WriteResponse, NodeError> {
        let start = Instant::now();
        let url = self.documents_url(address, &request.index_uid);

        tracing::debug!(
            target: "miroir.node",
            node_id = %node,
            address = %address,
            index = %request.index_uid,
            operation = "write_documents",
            "node call started"
        );

        let mut query_params = Vec::new();
        if let Some(pk) = &request.primary_key {
            query_params.push(("primaryKey", pk.as_str()));
        }

        let mut req_builder = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .json(&request.documents);

        if !query_params.is_empty() {
            req_builder = req_builder.query(&query_params);
        }

        let response = req_builder
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("Request failed: {e}")))?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .map_err(|e| NodeError::NetworkError(format!("Failed to read response: {e}")))?;

        if !status.is_success() {
            // Try to parse as Meilisearch error
            if let Ok(meili_err) = serde_json::from_str::<Value>(&body_text) {
                return Ok(WriteResponse {
                    success: false,
                    task_uid: None,
                    message: meili_err
                        .get("message")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    code: meili_err
                        .get("code")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    error_type: meili_err
                        .get("type")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                });
            }
            return Err(NodeError::HttpError {
                status: status.as_u16(),
                body: body_text,
            });
        }

        // Parse successful response
        let json: Value = serde_json::from_str(&body_text)
            .map_err(|e| NodeError::NetworkError(format!("Failed to parse JSON response: {e}")))?;

        let duration_ms = start.elapsed().as_millis() as u64;
        tracing::debug!(
            target: "miroir.node",
            node_id = %node,
            address = %address,
            operation = "write_documents",
            duration_ms,
            status = status.as_u16(),
            "node call completed"
        );

        Ok(WriteResponse {
            success: true,
            task_uid: json.get("taskUid").and_then(|v| v.as_u64()),
            message: None,
            code: None,
            error_type: None,
        })
    }

    async fn delete_documents(
        &self,
        node: &NodeId,
        address: &str,
        request: &DeleteByIdsRequest,
    ) -> std::result::Result<DeleteResponse, NodeError> {
        let start = Instant::now();
        let url = self.documents_url(address, &request.index_uid);

        tracing::debug!(
            target: "miroir.node",
            node_id = %node,
            address = %address,
            index = %request.index_uid,
            operation = "delete_documents",
            "node call started"
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .json(&request.ids)
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("Request failed: {e}")))?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .map_err(|e| NodeError::NetworkError(format!("Failed to read response: {e}")))?;

        let duration_ms = start.elapsed().as_millis() as u64;
        tracing::debug!(
            target: "miroir.node",
            node_id = %node,
            address = %address,
            operation = "delete_documents",
            duration_ms,
            status = status.as_u16(),
            "node call completed"
        );

        if !status.is_success() {
            // Try to parse as Meilisearch error
            if let Ok(meili_err) = serde_json::from_str::<Value>(&body_text) {
                return Ok(DeleteResponse {
                    success: false,
                    task_uid: None,
                    message: meili_err
                        .get("message")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    code: meili_err
                        .get("code")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    error_type: meili_err
                        .get("type")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                });
            }
            return Err(NodeError::HttpError {
                status: status.as_u16(),
                body: body_text,
            });
        }

        // Parse successful response
        let json: Value = serde_json::from_str(&body_text)
            .map_err(|e| NodeError::NetworkError(format!("Failed to parse JSON response: {e}")))?;

        Ok(DeleteResponse {
            success: true,
            task_uid: json.get("taskUid").and_then(|v| v.as_u64()),
            message: None,
            code: None,
            error_type: None,
        })
    }

    async fn delete_documents_by_filter(
        &self,
        node: &NodeId,
        address: &str,
        request: &DeleteByFilterRequest,
    ) -> std::result::Result<DeleteResponse, NodeError> {
        let start = Instant::now();
        let url = format!(
            "{}/indexes/{}/documents/delete",
            address.trim_end_matches('/'),
            request.index_uid
        );

        tracing::debug!(
            target: "miroir.node",
            node_id = %node,
            address = %address,
            index = %request.index_uid,
            operation = "delete_by_filter",
            "node call started"
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .json(&request.filter)
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("Request failed: {e}")))?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .map_err(|e| NodeError::NetworkError(format!("Failed to read response: {e}")))?;

        let duration_ms = start.elapsed().as_millis() as u64;
        tracing::debug!(
            target: "miroir.node",
            node_id = %node,
            address = %address,
            operation = "delete_by_filter",
            duration_ms,
            status = status.as_u16(),
            "node call completed"
        );

        if !status.is_success() {
            // Try to parse as Meilisearch error
            if let Ok(meili_err) = serde_json::from_str::<Value>(&body_text) {
                return Ok(DeleteResponse {
                    success: false,
                    task_uid: None,
                    message: meili_err
                        .get("message")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    code: meili_err
                        .get("code")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    error_type: meili_err
                        .get("type")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                });
            }
            return Err(NodeError::HttpError {
                status: status.as_u16(),
                body: body_text,
            });
        }

        // Parse successful response
        let json: Value = serde_json::from_str(&body_text)
            .map_err(|e| NodeError::NetworkError(format!("Failed to parse JSON response: {e}")))?;

        Ok(DeleteResponse {
            success: true,
            task_uid: json.get("taskUid").and_then(|v| v.as_u64()),
            message: None,
            code: None,
            error_type: None,
        })
    }

    async fn preflight_node(
        &self,
        node: &NodeId,
        address: &str,
        request: &PreflightRequest,
    ) -> std::result::Result<PreflightResponse, NodeError> {
        let start = Instant::now();
        let base = address.trim_end_matches('/');

        tracing::debug!(
            target: "miroir.node",
            node_id = %node,
            address = %address,
            index = %request.index_uid,
            operation = "preflight",
            term_count = request.terms.len(),
            "node call started"
        );

        // 1. Get total docs from Meilisearch stats endpoint
        let stats_url = format!("{}/indexes/{}/stats", base, request.index_uid);
        let stats_resp = self
            .client
            .get(&stats_url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("Stats request failed: {e}")))?;

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
            .map_err(|e| NodeError::NetworkError(format!("Failed to parse stats: {e}")))?;

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
                .map_err(|e| NodeError::NetworkError(format!("DF search failed: {e}")))?;

            if search_resp.status().is_success() {
                let body: Value = search_resp.json().await.map_err(|e| {
                    NodeError::NetworkError(format!("Failed to parse DF response: {e}"))
                })?;
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

        let duration_ms = start.elapsed().as_millis() as u64;
        tracing::debug!(
            target: "miroir.node",
            node_id = %node,
            address = %address,
            operation = "preflight",
            duration_ms,
            total_docs,
            "node call completed"
        );

        Ok(PreflightResponse {
            total_docs,
            avg_doc_length,
            term_stats,
        })
    }

    fn get_task_status(
        &self,
        node: &NodeId,
        address: &str,
        request: &TaskStatusRequest,
    ) -> impl std::future::Future<Output = std::result::Result<TaskStatusResponse, NodeError>> + Send
    {
        let task_uid = request.task_uid;
        let url = Self::task_url_static(address, task_uid);
        let master_key = self.master_key.clone();
        let client = self.client.clone();

        async move {
            let response = client
                .get(&url)
                .header("Authorization", format!("Bearer {master_key}"))
                .send()
                .await
                .map_err(|e| NodeError::NetworkError(format!("Request failed: {e}")))?;

            let status = response.status();
            let body_text = response
                .text()
                .await
                .map_err(|e| NodeError::NetworkError(format!("Failed to read response: {e}")))?;

            if !status.is_success() {
                return Err(NodeError::HttpError {
                    status: status.as_u16(),
                    body: body_text,
                });
            }

            // Parse successful response
            let json: Value = serde_json::from_str(&body_text).map_err(|e| {
                NodeError::NetworkError(format!("Failed to parse JSON response: {e}"))
            })?;

            Ok(TaskStatusResponse {
                task_uid,
                status: json
                    .get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("enqueued")
                    .to_string(),
                error: json
                    .get("error")
                    .and_then(|v| v.get("message"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                error_type: json
                    .get("error")
                    .and_then(|v| v.get("type"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            })
        }
    }
}

impl miroir_core::group_sync_worker::SyncNodeClient for HttpClient {
    async fn fetch_documents(
        &self,
        node: &NodeId,
        address: &str,
        request: &FetchDocumentsRequest,
    ) -> std::result::Result<serde_json::Value, String> {
        let url = self.documents_url(address, &request.index_uid);
        let filter_json = serde_json::to_string(&request.filter)
            .map_err(|e| format!("Failed to serialize filter: {e}"))?;

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .query(&[
                ("filter", &filter_json),
                ("limit", &request.limit.to_string()),
                ("offset", &request.offset.to_string()),
            ])
            .send()
            .await
            .map_err(|e| format!("Request failed: {e}"))?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .map_err(|e| format!("Failed to read response: {e}"))?;

        if !status.is_success() {
            return Err(format!("HTTP {}: {}", status.as_u16(), body_text));
        }

        serde_json::from_str(&body_text).map_err(|e| format!("Failed to parse JSON: {e}"))
    }

    async fn write_documents(
        &self,
        node: &NodeId,
        address: &str,
        index_uid: &str,
        documents: Vec<serde_json::Value>,
    ) -> std::result::Result<(), String> {
        let url = self.documents_url(address, index_uid);

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .json(&documents)
            .send()
            .await
            .map_err(|e| format!("Request failed: {e}"))?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .map_err(|e| format!("Failed to read response: {e}"))?;

        if !status.is_success() {
            return Err(format!("HTTP {}: {}", status.as_u16(), body_text));
        }

        Ok(())
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
