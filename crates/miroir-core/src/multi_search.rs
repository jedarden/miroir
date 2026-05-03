//! Multi-search batch API (plan §13.11).
//!
//! Allows batching multiple search queries into a single HTTP request.

use crate::error::{MiroirError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Multi-search configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiSearchConfig {
    /// Whether multi-search is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Maximum queries per batch.
    #[serde(default = "default_max")]
    pub max_queries_per_batch: usize,
    /// Total timeout for all queries (ms).
    #[serde(default = "default_total_timeout")]
    pub total_timeout_ms: u64,
    /// Per-query timeout (ms).
    #[serde(default = "default_query_timeout")]
    pub per_query_timeout_ms: u64,
}

fn default_true() -> bool {
    true
}
fn default_max() -> usize {
    100
}
fn default_total_timeout() -> u64 {
    30000
}
fn default_query_timeout() -> u64 {
    30000
}

impl Default for MultiSearchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_queries_per_batch: default_max(),
            total_timeout_ms: default_total_timeout(),
            per_query_timeout_ms: default_query_timeout(),
        }
    }
}

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

    /// Execute a multi-search request.
    ///
    /// This is a stub - the real implementation would:
    /// 1. Validate the request
    /// 2. Scatter each query independently
    /// 3. Merge results per-query
    /// 4. Return results in input order
    pub async fn execute(
        &self,
        _request: MultiSearchRequest,
    ) -> Result<MultiSearchResponse> {
        // Stub implementation
        Ok(MultiSearchResponse {
            results: vec![],
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

        if request.queries.len() > self.config.max_queries_per_batch {
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
        let config = MultiSearchConfig {
            max_queries_per_batch: 10,
            ..Default::default()
        };
        let executor = MultiSearchExecutor::new(config);

        let queries: Vec<SearchQuery> = (0..20).map(|i| SearchQuery {
            indexUid: format!("index-{}", i),
            q: Some("test".into()),
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
}
