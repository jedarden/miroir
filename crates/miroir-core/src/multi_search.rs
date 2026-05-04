//! Multi-search batch API (plan §13.11).
//!
//! Allows batching multiple search queries into a single HTTP request.

use crate::error::{MiroirError, Result};
use crate::config::advanced::MultiSearchConfig as AdvancedMultiSearchConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;

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
pub struct SearchQuery {
    /// Index UID.
    pub indexUid: String,
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

        // Execute all queries in parallel
        let mut tasks = Vec::with_capacity(request.queries.len());
        for query in request.queries {
            tasks.push(executor(query));
        }

        let results = futures_util::future::join_all(tasks).await;

        // Convert results to SearchResults
        let search_results: Vec<SearchResult> = results
            .into_iter()
            .map(|r| match r {
                Ok(data) => SearchResult {
                    status: 200,
                    body: Some(data.body),
                    error: None,
                    code: None,
                },
                Err(e) => SearchResult {
                    status: 500,
                    body: None,
                    error: Some(e.to_string()),
                    code: Some("internal_error".to_string()),
                },
            })
            .collect();

        Ok(MultiSearchResponse {
            results: search_results,
        })
    }

    /// Validate a multi-search request.
    pub fn validate(&self, request: &MultiSearchRequest) -> Result<()> {
        if !self.config.enabled {
            return Err(MiroirError::InvalidRequest("multi-search is disabled".into()));
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
        let request = MultiSearchRequest {
            queries: vec![],
        };
        assert!(executor.validate(&request).is_err());
    }

    #[test]
    fn test_validate_too_many_queries() {
        let mut config = MultiSearchConfig::default();
        config.max_queries_per_batch = 10;
        let executor = MultiSearchExecutor::new(config);

        let queries: Vec<SearchQuery> = (0..20).map(|i| SearchQuery {
            indexUid: format!("index-{}", i),
            q: Some("test".into()),
            filter: None,
            limit: Some(10),
            offset: Some(0),
            other: HashMap::new(),
        }).collect();

        let request = MultiSearchRequest { queries };
        assert!(executor.validate(&request).is_err());
    }

    #[test]
    fn test_validate_valid_request() {
        let executor = MultiSearchExecutor::default();
        let request = MultiSearchRequest {
            queries: vec![SearchQuery {
                indexUid: "products".into(),
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
            indexUid: "products".into(),
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
                    indexUid: "products".into(),
                    q: Some("laptop".into()),
                    filter: None,
                    limit: Some(20),
                    offset: Some(0),
                    other: HashMap::new(),
                },
                SearchQuery {
                    indexUid: "users".into(),
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
}
