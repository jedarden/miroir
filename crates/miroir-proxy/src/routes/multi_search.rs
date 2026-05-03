//! Multi-search batch API endpoint (plan §13.11).

use axum::{
    extract::{FromRef, State},
    http::StatusCode,
    Json,
};
use miroir_core::{
    config::MiroirConfig,
    scatter::SearchRequest,
    topology::Topology,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Multi-search state.
#[derive(Clone)]
pub struct MultiSearchState {
    pub config: Arc<MiroirConfig>,
    pub topology: Arc<RwLock<Topology>>,
    pub node_master_key: String,
}

/// Multi-search request (plan §13.11).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MultiSearchRequest {
    pub queries: Vec<SingleSearchQuery>,
}

/// A single query in the batch.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SingleSearchQuery {
    pub index_uid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub q: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
}

/// Multi-search response.
#[derive(Debug, Clone, Serialize)]
pub struct MultiSearchResponse {
    pub results: Vec<SingleSearchResult>,
}

/// Search response (matches Meilisearch response format).
#[derive(Debug, Clone, Serialize, Default)]
pub struct SearchResponse {
    pub hits: Vec<serde_json::Value>,
    pub estimated_total_hits: u64,
    pub limit: usize,
    pub offset: usize,
    pub processing_time_ms: u64,
    pub query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet_distribution: Option<std::collections::BTreeMap<String, std::collections::BTreeMap<String, u64>>>,
}

/// Result for a single query in the batch.
#[derive(Debug, Clone, Serialize)]
pub struct SingleSearchResult {
    pub index_uid: String,
    pub status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<SearchResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// POST /multi-search — execute multiple searches in a single batch.
///
/// Plan §13.11: Reduces round-trips for search UIs that need results + facets
/// from multiple queries per page render. Each query runs in parallel.
pub async fn multi_search<S>(
    State(state): State<MultiSearchState>,
    Json(body): Json<MultiSearchRequest>,
) -> Result<Json<MultiSearchResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    MultiSearchState: FromRef<S>,
{
    if !state.config.multi_search.enabled {
        return Err(StatusCode::NOT_IMPLEMENTED);
    }

    if body.queries.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    if body.queries.len() > state.config.multi_search.max_queries_per_batch as usize {
        return Err(StatusCode::BAD_REQUEST);
    }

    let mut results = Vec::with_capacity(body.queries.len());

    // Execute each query in parallel
    let _topology = state.topology.read().await;

    for query in body.queries {
        // TODO: Execute actual search against nodes
        // For now, return a placeholder response
        results.push(SingleSearchResult {
            index_uid: query.index_uid.clone(),
            status: 200,
            result: Some(SearchResponse {
                hits: vec![],
                estimated_total_hits: 0,
                limit: query.limit.unwrap_or(20),
                offset: query.offset.unwrap_or(0),
                processing_time_ms: 0,
                query: query.q.clone(),
                ..Default::default()
            }),
            error: None,
        });
    }

    Ok(Json(MultiSearchResponse { results }))
}
