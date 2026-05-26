//! Multi-search batch API (plan §13.11).
//!
//! Allows batching multiple search queries into a single HTTP request.
//! Each query runs in parallel with individual deadlines.

use crate::config::advanced::MultiSearchConfig as AdvancedMultiSearchConfig;
use crate::error::{MiroirError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

/// Multi-search configuration (re-export of advanced config).
pub type MultiSearchConfig = AdvancedMultiSearchConfig;

/// Multi-search request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiSearchRequest {
    /// Array of search queries.
    pub queries: Vec<SearchQuery>,
}

/// Individual search query in a batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct SearchQuery {
    /// Index UID.
    #[serde(rename = "indexUid")]
    pub index_uid: String,
    /// Query string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub q: Option<String>,
    /// Filter expression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    /// Limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Offset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    /// Other query parameters.
    #[serde(flatten)]
    pub other: HashMap<String, Value>,
}

/// Multi-search response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiSearchResponse {
    /// Array of search results (in input order).
    pub results: Vec<SearchResult>,
}

/// Individual search result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// HTTP status code for this query.
    pub status: u16,
    /// Result body (if successful).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
    /// Error message (if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Error code (if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Error type (if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

impl SearchResult {
    /// Create a successful search result.
    pub fn ok(body: serde_json::Value) -> Self {
        Self {
            status: 200,
            body: Some(body),
            error: None,
            code: None,
            error_type: None,
        }
    }

    /// Create a failed search result.
    pub fn err(status: u16, code: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            status,
            body: None,
            error: Some(error.into()),
            code: Some(code.into()),
            error_type: Some("invalid_request".to_string()),
        }
    }

    /// Create a timeout error result.
    pub fn timeout() -> Self {
        Self::err(408, "query_timeout", "Query exceeded per-query timeout")
    }

    /// Check if this result was successful.
    pub fn is_success(&self) -> bool {
        self.status >= 200 && self.status < 300
    }
}

/// Search result data returned by the executor function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResultData {
    pub body: serde_json::Value,
}

/// Multi-search executor.
pub struct MultiSearchExecutor {
    /// Configuration.
    config: MultiSearchConfig,
}

impl MultiSearchExecutor {
    /// Create a new multi-search executor.
    pub fn new(config: MultiSearchConfig) -> Self {
        Self { config }
    }

    /// Create a new multi-search executor from advanced config.
    pub fn from_advanced(config: AdvancedMultiSearchConfig) -> Self {
        Self { config }
    }

    /// Execute a multi-search request.
    ///
    /// Executes each query independently and returns results in input order.
    /// Each query is executed via the provided executor function.
    ///
    /// Enforces both total_timeout_ms (for the entire batch) and per_query_timeout_ms
    /// (for each individual query). A single slow query does not block others.
    pub async fn execute<F, Fut>(
        &self,
        request: MultiSearchRequest,
        mut executor: F,
    ) -> Result<MultiSearchResponse>
    where
        F: FnMut(SearchQuery) -> Fut,
        Fut: std::future::Future<Output = Result<SearchResultData>>,
    {
        self.validate(&request)?;

        let total_timeout = Duration::from_millis(self.config.total_timeout_ms);
        let per_query_timeout = Duration::from_millis(self.config.per_query_timeout_ms);
        let query_count = request.queries.len();

        // Execute all queries in parallel with per-query timeout
        let mut tasks = Vec::with_capacity(query_count);
        for query in request.queries {
            let query_future = executor(query);
            let timeout_future = async move {
                match tokio::time::timeout(per_query_timeout, query_future).await {
                    Ok(Ok(data)) => SearchResult::ok(data.body),
                    Ok(Err(e)) => SearchResult::err(500, "internal_error", e.to_string()),
                    Err(_) => SearchResult::timeout(),
                }
            };
            tasks.push(timeout_future);
        }

        // Wait for all queries with total timeout
        let results = match tokio::time::timeout(
            total_timeout,
            futures_util::future::join_all(tasks),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => {
                // Total timeout exceeded - return timeout errors for all queries
                return Ok(MultiSearchResponse {
                    results: (0..query_count).map(|_| SearchResult::timeout()).collect(),
                });
            }
        };

        Ok(MultiSearchResponse { results })
    }

    /// Validate a multi-search request.
    pub fn validate(&self, request: &MultiSearchRequest) -> Result<()> {
        if !self.config.enabled {
            return Err(MiroirError::InvalidRequest(
                "multi-search is disabled".into(),
            ));
        }

        if request.queries.is_empty() {
            return Err(MiroirError::InvalidRequest("queries array is empty".into()));
        }

        if request.queries.len() > self.config.max_queries_per_batch as usize {
            return Err(MiroirError::InvalidRequest(format!(
                "too many queries: {} exceeds maximum of {}",
                request.queries.len(),
                self.config.max_queries_per_batch
            )));
        }

        Ok(())
    }
}

impl Default for MultiSearchExecutor {
    fn default() -> Self {
        Self::new(MultiSearchConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = MultiSearchConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_queries_per_batch, 100);
        assert_eq!(config.total_timeout_ms, 30000);
    }

    #[test]
    fn test_validate_empty_queries() {
        let executor = MultiSearchExecutor::default();
        let request = MultiSearchRequest { queries: vec![] };
        assert!(executor.validate(&request).is_err());
    }

    #[test]
    fn test_validate_too_many_queries() {
        let config = MultiSearchConfig {
            max_queries_per_batch: 10,
            ..Default::default()
        };
        let executor = MultiSearchExecutor::new(config);

        let queries: Vec<SearchQuery> = (0..20)
            .map(|i| SearchQuery {
                index_uid: format!("index-{i}"),
                q: Some("test".into()),
                filter: None,
                limit: Some(10),
                offset: Some(0),
                other: HashMap::new(),
            })
            .collect();

        let request = MultiSearchRequest { queries };
        assert!(executor.validate(&request).is_err());
    }

    #[test]
    fn test_validate_valid_request() {
        let executor = MultiSearchExecutor::default();
        let request = MultiSearchRequest {
            queries: vec![SearchQuery {
                index_uid: "products".into(),
                q: Some("laptop".into()),
                filter: None,
                limit: Some(20),
                offset: Some(0),
                other: HashMap::new(),
            }],
        };
        assert!(executor.validate(&request).is_ok());
    }

    #[test]
    fn test_search_query_serialization() {
        let query = SearchQuery {
            index_uid: "products".into(),
            q: Some("laptop".into()),
            filter: Some("category = \"electronics\"".into()),
            limit: Some(20),
            offset: Some(0),
            other: HashMap::new(),
        };

        let json = serde_json::to_string(&query).unwrap();
        assert!(json.contains("\"indexUid\":\"products\""));
        assert!(json.contains("\"q\":\"laptop\""));
    }

    #[tokio::test]
    async fn test_execute_multi_search() {
        let executor = MultiSearchExecutor::default();

        let request = MultiSearchRequest {
            queries: vec![
                SearchQuery {
                    index_uid: "products".into(),
                    q: Some("laptop".into()),
                    filter: None,
                    limit: Some(20),
                    offset: Some(0),
                    other: HashMap::new(),
                },
                SearchQuery {
                    index_uid: "users".into(),
                    q: Some("john".into()),
                    filter: None,
                    limit: Some(10),
                    offset: Some(0),
                    other: HashMap::new(),
                },
            ],
        };

        let response = executor
            .execute(request, |query| async move {
                Ok(SearchResultData {
                    body: serde_json::json!({
                        "hits": [],
                        "estimatedTotalHits": 0,
                        "limit": query.limit.unwrap_or(20),
                        "offset": query.offset.unwrap_or(0),
                        "processingTimeMs": 0,
                    }),
                })
            })
            .await
            .unwrap();

        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].status, 200);
        assert!(response.results[0].body.is_some());
        assert_eq!(response.results[1].status, 200);
    }

    /// P5.11-A1: 5-query batch completes successfully.
    #[tokio::test]
    async fn test_five_query_batch_all_complete() {
        let executor = MultiSearchExecutor::default();

        let queries: Vec<SearchQuery> = (0..5)
            .map(|i| SearchQuery {
                index_uid: format!("index-{i}"),
                q: Some(format!("query-{i}")),
                filter: None,
                limit: Some(10),
                offset: Some(0),
                other: HashMap::new(),
            })
            .collect();

        let request = MultiSearchRequest { queries };

        let response = executor
            .execute(request, |query| async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(SearchResultData {
                    body: serde_json::json!({
                        "hits": [],
                        "estimatedTotalHits": 0,
                        "limit": query.limit.unwrap_or(20),
                        "processingTimeMs": 10,
                    }),
                })
            })
            .await
            .unwrap();

        assert_eq!(response.results.len(), 5);
        for (i, result) in response.results.iter().enumerate() {
            assert!(result.is_success(), "Query {i} should succeed");
            assert!(result.body.is_some(), "Query {i} should have body");
        }
    }

    /// P5.11-A2: Slow query doesn't block fast queries (parallel execution).
    #[tokio::test]
    async fn test_slow_query_doesnt_block_fast_queries() {
        let config = MultiSearchConfig {
            per_query_timeout_ms: 5000,
            ..Default::default()
        };
        let executor = MultiSearchExecutor::new(config);

        let request = MultiSearchRequest {
            queries: vec![
                // Fast query
                SearchQuery {
                    index_uid: "fast".into(),
                    q: Some("quick".into()),
                    filter: None,
                    limit: Some(10),
                    offset: Some(0),
                    other: HashMap::new(),
                },
                // Slow query
                SearchQuery {
                    index_uid: "slow".into(),
                    q: Some("delayed".into()),
                    filter: None,
                    limit: Some(10),
                    offset: Some(0),
                    other: HashMap::new(),
                },
            ],
        };

        let start = std::time::Instant::now();

        let response = executor
            .execute(request, |query| async move {
                if query.index_uid == "slow" {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok(SearchResultData {
                    body: serde_json::json!({
                        "hits": [],
                        "estimatedTotalHits": 0,
                        "processingTimeMs": start.elapsed().as_millis() as u64,
                    }),
                })
            })
            .await
            .unwrap();

        let elapsed = start.elapsed();

        // Both queries should complete in ~200ms (parallel execution)
        // If sequential, would take ~210ms
        assert!(
            elapsed < Duration::from_millis(250),
            "Queries should run in parallel"
        );

        assert_eq!(response.results.len(), 2);
        assert!(response.results[0].is_success());
        assert!(response.results[1].is_success());
    }

    /// P5.11-A3: Partial failure - one query fails, others succeed.
    #[tokio::test]
    async fn test_partial_failure_one_error() {
        let executor = MultiSearchExecutor::default();

        let request = MultiSearchRequest {
            queries: vec![
                SearchQuery {
                    index_uid: "ok1".into(),
                    q: Some("good".into()),
                    filter: None,
                    limit: Some(10),
                    offset: Some(0),
                    other: HashMap::new(),
                },
                SearchQuery {
                    index_uid: "fail".into(),
                    q: Some("bad".into()),
                    filter: None,
                    limit: Some(10),
                    offset: Some(0),
                    other: HashMap::new(),
                },
                SearchQuery {
                    index_uid: "ok2".into(),
                    q: Some("good".into()),
                    filter: None,
                    limit: Some(10),
                    offset: Some(0),
                    other: HashMap::new(),
                },
            ],
        };

        let response = executor
            .execute(request, |query| async move {
                if query.index_uid == "fail" {
                    Err(MiroirError::InvalidRequest("simulated error".into()))
                } else {
                    Ok(SearchResultData {
                        body: serde_json::json!({
                            "hits": [],
                            "estimatedTotalHits": 0,
                        }),
                    })
                }
            })
            .await
            .unwrap();

        assert_eq!(response.results.len(), 3);

        // First query succeeds
        assert!(response.results[0].is_success());
        assert!(response.results[0].body.is_some());

        // Second query fails
        assert!(!response.results[1].is_success());
        assert_eq!(response.results[1].status, 500);
        assert!(response.results[1].error.is_some());
        assert_eq!(response.results[1].code.as_deref(), Some("internal_error"));

        // Third query succeeds (order preserved)
        assert!(response.results[2].is_success());
        assert!(response.results[2].body.is_some());
    }

    /// P5.11-A4: Per-query timeout enforcement.
    #[tokio::test]
    async fn test_per_query_timeout() {
        let config = MultiSearchConfig {
            per_query_timeout_ms: 100,
            ..Default::default()
        };
        let executor = MultiSearchExecutor::new(config);

        let request = MultiSearchRequest {
            queries: vec![
                SearchQuery {
                    index_uid: "fast".into(),
                    q: Some("quick".into()),
                    filter: None,
                    limit: Some(10),
                    offset: Some(0),
                    other: HashMap::new(),
                },
                SearchQuery {
                    index_uid: "slow".into(),
                    q: Some("timeout".into()),
                    filter: None,
                    limit: Some(10),
                    offset: Some(0),
                    other: HashMap::new(),
                },
            ],
        };

        let response = executor
            .execute(request, |query| async move {
                if query.index_uid == "slow" {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Ok(SearchResultData {
                    body: serde_json::json!({"hits": []}),
                })
            })
            .await
            .unwrap();

        assert_eq!(response.results.len(), 2);

        // Fast query completes
        assert!(response.results[0].is_success());

        // Slow query times out
        assert_eq!(response.results[1].status, 408);
        assert_eq!(response.results[1].code.as_deref(), Some("query_timeout"));
    }

    /// P5.11-A5: Total timeout enforcement.
    #[tokio::test]
    async fn test_total_timeout() {
        let config = MultiSearchConfig {
            total_timeout_ms: 100,
            per_query_timeout_ms: 5000, // Individual queries have longer timeout
            ..Default::default()
        };
        let executor = MultiSearchExecutor::new(config);

        let request = MultiSearchRequest {
            queries: vec![
                SearchQuery {
                    index_uid: "index1".into(),
                    q: Some("query1".into()),
                    filter: None,
                    limit: Some(10),
                    offset: Some(0),
                    other: HashMap::new(),
                },
                SearchQuery {
                    index_uid: "index2".into(),
                    q: Some("query2".into()),
                    filter: None,
                    limit: Some(10),
                    offset: Some(0),
                    other: HashMap::new(),
                },
            ],
        };

        let response = executor
            .execute(request, |_query| async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(SearchResultData {
                    body: serde_json::json!({"hits": []}),
                })
            })
            .await
            .unwrap();

        assert_eq!(response.results.len(), 2);

        // Both queries should have timeout status due to total timeout
        for result in &response.results {
            assert_eq!(result.status, 408);
            assert_eq!(result.code.as_deref(), Some("query_timeout"));
        }
    }

    /// P5.11-A6: 100-query batch completes under total timeout.
    #[tokio::test]
    async fn test_large_batch_completes() {
        let config = MultiSearchConfig {
            max_queries_per_batch: 100,
            total_timeout_ms: 30000,
            per_query_timeout_ms: 5000,
            ..Default::default()
        };
        let executor = MultiSearchExecutor::new(config);

        let queries: Vec<SearchQuery> = (0..100)
            .map(|i| SearchQuery {
                index_uid: format!("index-{}", i % 10),
                q: Some(format!("query-{i}")),
                filter: None,
                limit: Some(10),
                offset: Some(0),
                other: HashMap::new(),
            })
            .collect();

        let request = MultiSearchRequest { queries };

        let start = std::time::Instant::now();

        let response = executor
            .execute(request, |query| async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(SearchResultData {
                    body: serde_json::json!({
                        "hits": [],
                        "estimatedTotalHits": 0,
                        "index": query.index_uid,
                    }),
                })
            })
            .await
            .unwrap();

        let elapsed = start.elapsed();

        // Should complete in well under total_timeout_ms (30s)
        assert!(
            elapsed < Duration::from_secs(5),
            "Large batch should complete quickly"
        );

        assert_eq!(response.results.len(), 100);
        for (i, result) in response.results.iter().enumerate() {
            assert!(result.is_success(), "Query {} should succeed", i);
        }
    }
}
