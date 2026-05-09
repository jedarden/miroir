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
    http::HeaderValue,
    response::{IntoResponse, Json, Response},
};
use miroir_core::{
    config::UnavailableShardPolicy,
    merger::{Merger, MergerImpl, MergedResult, ShardResponse as CoreShardResponse},
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
pub fn router() -> axum::Router<ProxyState> {
    axum::Router::new().route("/:index/search", axum::routing::post(search))
}

/// Search request body (Meilisearch format).
#[derive(Debug, serde::Deserialize, serde::Serialize)]
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

/// Attempt a search with a specific replica group.
///
/// Returns (shard_responses, any_degraded, failed_shard_ids)
async fn search_with_group(
    state: &ProxyState,
    topology: &miroir_core::topology::Topology,
    group_id: u32,
    index: &str,
    req_body: &[u8],
    query_seq: u64,
    _limit: usize,
    _offset: usize,
    _client_requested_score: bool,
    _facets: Option<Vec<String>>,
) -> Result<(Vec<CoreShardResponse>, bool, Vec<u32>), ErrorResponse> {
    let group = topology
        .group(group_id)
        .ok_or_else(|| ErrorResponse::internal_error(format!("Group {} not found", group_id)))?;

    // Build covering set for all shards
    let rf = state.config.replication_factor as usize;
    let shard_count = state.config.shards;
    let covering = covering_set(shard_count, group, rf, query_seq);

    let request = ScatterRequest {
        method: "POST".to_string(),
        path: format!("/indexes/{}/search", index),
        body: req_body.to_vec(),
        headers: vec![],
    };

    // Scatter search to all nodes in covering set
    let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);
    let result = scatter
        .scatter(topology, covering.clone(), request, UnavailableShardPolicy::Partial)
        .await
        .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

    // Build shard responses for merger
    let mut shard_responses: Vec<CoreShardResponse> = Vec::new();
    let mut any_degraded = false;
    let mut failed_shard_ids: Vec<u32> = Vec::new();

    // Group responses by node
    let mut responses_by_node: std::collections::HashMap<String, serde_json::Value> = std::collections::HashMap::new();

    for resp in result.responses {
        let node_id = resp.node_id.as_str().to_string();
        responses_by_node.insert(node_id, resp.body.into());
    }

    // For each shard, find the response from its assigned node
    for (shard_id, node_id) in covering.iter().enumerate() {
        let node_id_str = node_id.as_str().to_string();

        if let Some(body) = responses_by_node.get(&node_id_str) {
            shard_responses.push(CoreShardResponse {
                shard_id: shard_id as u32,
                body: body.clone(),
                success: true,
            });
        } else {
            // No response from this node's shard
            shard_responses.push(CoreShardResponse {
                shard_id: shard_id as u32,
                body: serde_json::json!({}),
                success: false,
            });
            failed_shard_ids.push(shard_id as u32);
            any_degraded = true;
        }
    }

    Ok((shard_responses, any_degraded, failed_shard_ids))
}

/// POST /indexes/:index/search - Search documents.
async fn search(
    State(state): State<ProxyState>,
    Path(index): Path<String>,
    req: Json<SearchRequest>,
) -> Result<Response, ErrorResponse> {
    let topology = state.topology().await;
    let query_seq = state.next_query_seq();

    // Extract query before moving req.0
    let query_value = req.q.clone();

    // Build request body for nodes
    let req_body = serde_json::to_vec(&req.0).unwrap_or_default();

    let offset = req.offset.unwrap_or(0);
    let limit = req.limit.unwrap_or(20);
    let client_requested_score = req.show_ranking_score.unwrap_or(false);
    let facets = req.facets.clone();

    // Try the primary group first
    let primary_group_id = query_group(query_seq, state.config.replica_groups);
    let (mut shard_responses, mut any_degraded, mut failed_shard_ids) = search_with_group(
        &state,
        &topology,
        primary_group_id,
        &index,
        &req_body,
        query_seq,
        limit,
        offset,
        client_requested_score,
        facets.clone(),
    )
    .await?;

    // If we have failed shards and there are other replica groups, try group fallback
    if !failed_shard_ids.is_empty() && state.config.replica_groups > 1 {
        let replica_groups = state.config.replica_groups;
        let mut fallback_attempts = 0;
        const MAX_FALLBACK_ATTEMPTS: u32 = 2; // Limit fallback attempts to avoid cascading

        // Try other groups for the failed shards
        for fallback_offset in 1..replica_groups as u64 {
            if fallback_attempts >= MAX_FALLBACK_ATTEMPTS {
                break;
            }

            let fallback_group_id = (primary_group_id as u64 + fallback_offset) % replica_groups as u64;
            if fallback_group_id == primary_group_id as u64 {
                continue;
            }

            // Try the fallback group for all failed shards
            let (fallback_responses, fallback_degraded, fallback_failed) = search_with_group(
                &state,
                &topology,
                fallback_group_id as u32,
                &index,
                &req_body,
                query_seq,
                limit,
                offset,
                client_requested_score,
                req.facets.clone(),
            )
            .await?;

            // Merge the successful responses from fallback into our main responses
            for (i, shard_resp) in shard_responses.iter_mut().enumerate() {
                let shard_id = i as u32;
                if !shard_resp.success && failed_shard_ids.contains(&shard_id) {
                    // Try to find a successful response from fallback
                    if let Some(fallback_resp) = fallback_responses.iter().find(|r| r.shard_id == shard_id && r.success) {
                        shard_resp.body = fallback_resp.body.clone();
                        shard_resp.success = true;
                        // Remove from failed list
                        failed_shard_ids.retain(|&id| id != shard_id);
                    }
                }
            }

            if fallback_degraded {
                any_degraded = true;
            }

            // Update failed_shard_ids with any new failures from fallback
            for &shard_id in &fallback_failed {
                if !failed_shard_ids.contains(&shard_id) {
                    failed_shard_ids.push(shard_id);
                }
            }

            fallback_attempts += 1;

            // If we've recovered all failed shards, stop trying fallbacks
            if failed_shard_ids.is_empty() {
                break;
            }
        }
    }

    // Check if we failed completely
    let successful_count = shard_responses.iter().filter(|s| s.success).count();
    if successful_count == 0 {
        return Err(ErrorResponse::internal_error("All shards failed"));
    }

    // Merge results
    let merger = MergerImpl;
    let merged: MergedResult = merger
        .merge(shard_responses, offset, limit, client_requested_score)
        .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

    // Check if any shards failed (degraded mode)
    let degraded = any_degraded || merged.degraded;

    // Build response
    let search_response = SearchResponse {
        hits: merged.hits,
        query: query_value.unwrap_or_default(),
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
