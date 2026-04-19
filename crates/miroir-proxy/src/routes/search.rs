//! Search route handler with DFS (Distributed Frequency Search) support.

use axum::extract::Path;
use axum::http::StatusCode;
use axum::{Extension, Json, response::Response};
use miroir_core::config::{Config, UnavailableShardPolicy};
use miroir_core::merger::ScoreMergeStrategy;
use miroir_core::scatter::{
    dfs_query_then_fetch_search, plan_search_scatter, SearchRequest, NodeClient,
};
use miroir_core::topology::Topology;
use serde_json::Value;
use std::sync::Arc;

/// Node client implementation using the HTTP client.
pub struct ProxyNodeClient {
    client: Arc<crate::client::HttpClient>,
}

impl ProxyNodeClient {
    pub fn new(client: Arc<crate::client::HttpClient>) -> Self {
        Self { client }
    }
}

#[allow(async_fn_in_trait)]
impl NodeClient for ProxyNodeClient {
    async fn search_node(
        &self,
        node: &miroir_core::topology::NodeId,
        address: &str,
        request: &SearchRequest,
    ) -> std::result::Result<Value, miroir_core::scatter::NodeError> {
        self.client.search_node(node, address, request).await
    }

    async fn preflight_node(
        &self,
        node: &miroir_core::topology::NodeId,
        address: &str,
        request: &miroir_core::scatter::PreflightRequest,
    ) -> std::result::Result<miroir_core::scatter::PreflightResponse, miroir_core::scatter::NodeError> {
        self.client.preflight_node(node, address, request).await
    }
}

pub fn router<S>() -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    axum::Router::new()
        .route("/:index", axum::routing::post(search_handler))
}

/// Search request body.
#[derive(Debug, serde::Deserialize)]
struct SearchRequestBody {
    q: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
    filter: Option<Value>,
    facets: Option<Vec<String>>,
    #[serde(rename = "rankingScore")]
    ranking_score: Option<bool>,
    #[serde(flatten)]
    rest: Value,
}

/// Search handler with DFS global-IDF preflight (OP#4).
///
/// This handler implements the `dfs_query_then_fetch` pattern:
/// 1. **Preflight phase**: Send term-frequency query to all shards, aggregate
///    global document frequencies at the coordinator.
/// 2. **Search phase**: Send the search query with global IDF attached so that
///    scoring uses corpus-wide statistics instead of per-shard local IDF.
///
/// This produces globally-comparable scores across shards with skewed document
/// distributions, enabling score-based merge with τ ≥ 0.95.
///
/// Returns `X-Miroir-Degraded: shards=X,Y,Z` header when any shards are unavailable.
async fn search_handler(
    Path(index): Path<String>,
    Extension(config): Extension<Arc<Config>>,
    Extension(_topology): Extension<Arc<Topology>>,
    Json(body): Json<SearchRequestBody>,
) -> Result<Response, StatusCode> {
    // Build topology from config
    let mut topo = Topology::new(config.shards, config.replica_groups, config.replication_factor as usize);
    for node in &config.nodes {
        topo.add_node(miroir_core::topology::Node::new(
            miroir_core::topology::NodeId::new(node.id.clone()),
            node.address.clone(),
            node.replica_group,
        ));
    }

    // Parse unavailable shard policy
    let policy = match config.scatter.unavailable_shard_policy.as_str() {
        "partial" => UnavailableShardPolicy::Partial,
        "error" => UnavailableShardPolicy::Error,
        "fallback" => UnavailableShardPolicy::Fallback,
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // Plan scatter
    let plan = plan_search_scatter(&topo, 0, config.replication_factor as usize, config.shards);

    // Build search request
    let search_req = SearchRequest {
        index_uid: index.clone(),
        query: body.q,
        offset: body.offset.unwrap_or(0),
        limit: body.limit.unwrap_or(20),
        filter: body.filter,
        facets: body.facets,
        ranking_score: body.ranking_score.unwrap_or(false),
        body: body.rest,
        global_idf: None, // Will be populated by dfs_query_then_fetch_search
    };

    // Create node client
    let http_client = Arc::new(crate::client::HttpClient::new(
        config.node_master_key.clone(),
        config.scatter.node_timeout_ms,
    ));
    let client = ProxyNodeClient::new(http_client);

    // Use score-based merge strategy (OP#4: requires global IDF)
    let strategy = ScoreMergeStrategy::new();

    // Execute DFS query-then-fetch
    let result = dfs_query_then_fetch_search(
        plan,
        &client,
        search_req,
        &topo,
        policy,
        &strategy,
    )
    .await
    .map_err(|e| {
        tracing::error!("Search failed: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Build response body
    let body = serde_json::json!({
        "hits": result.hits,
        "estimatedTotalHits": result.estimated_total_hits,
        "processingTimeMs": result.processing_time_ms,
        "facetDistribution": result.facet_distribution,
    });

    // Build response with optional X-Miroir-Degraded header
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json");

    // Add X-Miroir-Degraded header if the result is degraded
    // The header format is: X-Miroir-Degraded: shards=3,7,11
    // This indicates which shards had zero live replicas
    if result.degraded && !result.failed_shards.is_empty() {
        // Sort shard IDs for deterministic output
        let mut sorted_shards = result.failed_shards.clone();
        sorted_shards.sort();
        let shard_ids = sorted_shards.iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        response = response.header("X-Miroir-Degraded", format!("shards={}", shard_ids));
    } else if result.degraded {
        response = response.header("X-Miroir-Degraded", "partial");
    }

    let response = response
        .body(axum::body::Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();

    Ok(response)
}
