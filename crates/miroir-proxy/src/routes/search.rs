//! Search route: POST /indexes/:index/search
//!
//! Implements the read path per plan §2:
//! - Pick group via query_seq % RG
//! - Build intra-group covering set
//! - Scatter search to covering set nodes
//! - Merge by _rankingScore
//! - Strip _miroir_shard always + _rankingScore if not requested
//! - Aggregate facets + estimatedTotalHits
//! - Report max processingTimeMs
//! - Group fallback when covering set has holes

use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue},
    response::{IntoResponse, Json, Response},
};
use miroir_core::{
    config::UnavailableShardPolicy,
    merger::{Merger, MergerImpl, MergedResult, ShardResponse},
    router::{covering_set, query_group},
    scatter::{Scatter, ScatterRequest},
};
use serde_json::Value;

use crate::{
    error_response::ErrorResponse,
    scatter::HttpScatter,
    state::ProxyState,
};

/// Search router.
pub fn router() -> axum::Router {
    axum::Router::new().route("/:index/search", axum::routing::post(search))
}

/// Search request body (Meilisearch format).
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchRequest {
    q: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    filter: Option<serde_json::Value>,
    sort: Option<Vec<String>>,
    facets: Option<Vec<String>>,
    #[serde(rename = "attributesToRetrieve")]
    attributes_to_retrieve: Option<Vec<String>>,
    #[serde(rename = "attributesToCrop")]
    attributes_to_crop: Option<Vec<String>>,
    #[serde(rename = "cropLength")]
    crop_length: Option<usize>,
    #[serde(rename = "cropMarker")]
    crop_marker: Option<String>,
    #[serde(rename = "highlightPreTag")]
    highlight_pre_tag: Option<String>,
    #[serde(rename = "highlightPostTag")]
    highlight_post_tag: Option<String>,
    #[serde(rename = "showMatchesPosition")]
    show_matches_position: Option<bool>,
    #[serde(rename = "showRankingScore")]
    show_ranking_score: Option<bool>,
    #[serde(rename = "rankingScoreThreshold")]
    ranking_score_threshold: Option<f64>,
    #[serde(rename = "matchingStrategy")]
    matching_strategy: Option<String>,
}

/// Search response body (Meilisearch format).
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchResponse {
    hits: Vec<Value>,
    query: String,
    limit: usize,
    offset: usize,
    estimated_total_hits: u64,
    processing_time_ms: u64,
    facet_distribution: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ranking_score_threshold: Option<f64>,
}

/// POST /indexes/:index/search - Search documents.
async fn search(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    req: Json<SearchRequest>,
) -> Result<Response, ErrorResponse> {
    let topology = state.topology().await;
    let query_seq = state.next_query_seq();

    // Pick replica group for this query
    let group_id = query_group(query_seq, state.config.replica_groups);

    let group = topology
        .group(group_id)
        .ok_or_else(|| ErrorResponse::internal_error(format!("Group {} not found", group_id)))?;

    // Build covering set for all shards
    let rf = state.config.replication_factor as usize;
    let shard_count = state.config.shards;
    let covering = covering_set(shard_count, group, rf, query_seq);

    // Build request body for nodes
    let req_body = serde_json::to_vec(req.0).unwrap_or_default();

    let request = ScatterRequest {
        method: "POST".to_string(),
        path: format!("/indexes/{}/search", index),
        body: req_body,
        headers: vec![],
    };

    // Scatter search to all nodes in covering set
    let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);
    let result = scatter
        .scatter(&topology, covering, request, UnavailableShardPolicy::Partial)
        .await
        .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

    // Build shard responses for merger
    let mut shard_responses: Vec<ShardResponse> = Vec::new();
    let mut any_degraded = false;

    // Group responses by node
    let mut responses_by_node: std::collections::HashMap<String, Value> = std::collections::HashMap::new();

    for resp in result.responses {
        let node_id = resp.node_id.as_str().to_string();
        responses_by_node.insert(node_id, resp.body);
    }

    // For each shard, find the response from its assigned node
    for (shard_id, node_id) in covering.iter().enumerate() {
        let node_id_str = node_id.as_str().to_string();

        if let Some(body) = responses_by_node.get(&node_id_str) {
            shard_responses.push(ShardResponse {
                shard_id: shard_id as u32,
                body: body.clone(),
                success: true,
            });
        } else {
            // No response from this node's shard
            shard_responses.push(ShardResponse {
                shard_id: shard_id as u32,
                body: serde_json::json!({}),
                success: false,
            });
            any_degraded = true;
        }
    }

    // Check if we failed completely
    let successful_count = shard_responses.iter().filter(|s| s.success).count();
    if successful_count == 0 {
        return Err(ErrorResponse::internal_error("All shards failed"));
    }

    // Merge results
    let offset = req.offset.unwrap_or(0);
    let limit = req.limit.unwrap_or(20);
    let client_requested_score = req.show_ranking_score.unwrap_or(false);

    let merger = MergerImpl;
    let merged: MergedResult = merger
        .merge(shard_responses, offset, limit, client_requested_score)
        .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

    // Check if any shards failed (degraded mode)
    let degraded = any_degraded || merged.degraded;

    // Build response
    let search_response = SearchResponse {
        hits: merged.hits,
        query: req.q.unwrap_or_default(),
        limit,
        offset,
        estimated_total_hits: merged.total_hits,
        processing_time_ms: merged.processing_time_ms,
        facet_distribution: if merged.facets.as_object().map_or(false, |o| !o.is_empty()) {
            Some(merged.facets)
        } else {
            None
        },
        ranking_score_threshold: req.ranking_score_threshold,
    };

    let mut builder = Response::builder().status(200);

    // Add degraded header if any shard failed
    if degraded {
        if let Ok(val) = HeaderValue::from_str("true") {
            builder = builder.header("X-Miroir-Degraded", val);
        }
    }

    Ok(builder
        .body(Json(search_response).into_response().into_body())
        .unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_request_deserialization() {
        let json = r#"{
            "q": "test",
            "limit": 10,
            "offset": 0,
            "showRankingScore": true
        }"#;

        let req: SearchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.q, Some("test".to_string()));
        assert_eq!(req.limit, Some(10));
        assert_eq!(req.offset, Some(0));
        assert_eq!(req.show_ranking_score, Some(true));
    }

    #[test]
    fn test_search_request_defaults() {
        let json = r#"{"q": "test"}"#;

        let req: SearchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.q, Some("test".to_string()));
        assert_eq!(req.limit, None);
        assert_eq!(req.offset, None);
        assert_eq!(req.show_ranking_score, None);
    }

    #[test]
    fn test_search_response_serialization() {
        let response = SearchResponse {
            hits: vec![serde_json::json!({"id": "1", "title": "Test"})],
            query: "test".to_string(),
            limit: 20,
            offset: 0,
            estimated_total_hits: 100,
            processing_time_ms: 15,
            facet_distribution: Some(serde_json::json!({"color": {"red": 10}})),
            ranking_score_threshold: Some(0.5),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains(r#""hits":[{"#));
        assert!(json.contains(r#""query":"test""#));
        assert!(json.contains(r#""limit":20"#));
        assert!(json.contains(r#""offset":0"#));
        assert!(json.contains(r#""estimatedTotalHits":100"#));
        assert!(json.contains(r#""processingTimeMs":15"#));
        assert!(json.contains(r#""facetDistribution":{"#));
        assert!(json.contains(r#""rankingScoreThreshold":0.5"#));
    }
}
