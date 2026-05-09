//! Search read path: scatter-gather with result merging.

use crate::scatter::HttpScatter;
use crate::state::ProxyState;
use miroir_core::config::UnavailableShardPolicy;
use miroir_core::merger::{Merger, MergerImpl, ShardResponse};
use miroir_core::router;
use miroir_core::scatter::ScatterRequest;
use miroir_core::topology::{NodeId, Topology};
use miroir_core::{MiroirError, Result};
use serde_json::{json, Value};
use std::collections::HashMap;

/// Search executor for scatter-gather queries.
pub struct SearchExecutor {
    state: ProxyState,
    scatter: HttpScatter,
    merger: MergerImpl,
}

impl SearchExecutor {
    pub fn new(state: ProxyState) -> Self {
        let node_timeout_ms = state.config.scatter.node_timeout_ms;
        let scatter = HttpScatter::new(state.client.clone(), node_timeout_ms);

        Self {
            state,
            scatter,
            merger: MergerImpl,
        }
    }

    /// Execute a search query across the covering set.
    pub async fn search(
        &self,
        index: &str,
        query: Value,
        offset: usize,
        limit: usize,
    ) -> Result<SearchResult> {
        let topology = self.state.topology().await;
        let shard_count = self.state.config.shards;
        let rf = self.state.config.replication_factor as usize;
        let replica_groups = topology.replica_group_count();

        // Select query group
        let query_seq = self.state.next_query_seq();
        let group_id = router::query_group(query_seq, replica_groups);

        let group = topology
            .group(group_id)
            .ok_or_else(|| MiroirError::Routing(format!("Group {} not found", group_id)))?;

        // Build covering set
        let covering = router::covering_set(shard_count, group, rf, query_seq);

        // Deduplicate nodes
        let unique_nodes: std::collections::HashSet<_> = covering.into_iter().collect();

        // Prepare search query
        let mut query_with_score = query.clone();
        if let Some(obj) = query_with_score.as_object_mut() {
            obj.insert("showRankingScore".to_string(), json!(true));
        }

        let body = serde_json::to_vec(&query_with_score).unwrap();
        let path = format!("/indexes/{}/search", index);

        let request = ScatterRequest {
            body,
            headers: vec![],
            method: "POST".to_string(),
            path,
        };

        // Get policy from config
        let policy = match self.state.config.scatter.unavailable_shard_policy.as_str() {
            "error" => UnavailableShardPolicy::Error,
            "fallback" => UnavailableShardPolicy::Fallback,
            _ => UnavailableShardPolicy::Partial,
        };

        // Scatter to covering set
        let response = self
            .scatter
            .scatter(&topology, unique_nodes.into_iter().collect(), request, policy)
            .await?;

        // Convert node responses to shard responses
        let mut shard_responses: Vec<ShardResponse> = Vec::new();
        let mut degraded_shards = Vec::new();

        for node_resp in response.responses {
            // Parse response as shard response (all shards from this node)
            shard_responses.push(ShardResponse {
                shard_id: 0, // We'll merge all responses together
                body: serde_json::from_slice(&node_resp.body).unwrap_or(json!({})),
                success: true,
            });
        }

        for failed_node in &response.failed {
            degraded_shards.push(failed_node.as_str().to_string());
        }

        // Check if client requested ranking score
        let client_requested_score = query
            .get("showRankingScore")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Merge results
        let merged = self
            .merger
            .merge(shard_responses, offset, limit, client_requested_score)?;

        // Build response
        let mut result = json!({
            "hits": merged.hits,
            "processingTimeMs": merged.processing_time_ms,
            "query": query,
        });

        if !merged.facets.is_null() {
            if let Some(obj) = result.as_object_mut() {
                obj.insert("facetDistribution".to_string(), merged.facets);
            }
        }

        // Add estimatedTotalHits if present
        if merged.total_hits > 0 {
            if let Some(obj) = result.as_object_mut() {
                obj.insert("estimatedTotalHits".to_string(), json!(merged.total_hits));
            }
        }

        Ok(SearchResult {
            body: result,
            degraded: merged.degraded,
            degraded_shards,
        })
    }
}

/// Result of a search operation.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub body: Value,
    pub degraded: bool,
    pub degraded_shards: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use miroir_core::config::MiroirConfig;

    #[tokio::test]
    async fn test_search_result_creation() {
        let result = SearchResult {
            body: json!({"hits": []}),
            degraded: false,
            degraded_shards: vec![],
        };

        assert_eq!(result.body["hits"].as_array().unwrap().len(), 0);
        assert!(!result.degraded);
    }

    fn create_test_executor() -> SearchExecutor {
        let config = MiroirConfig {
            shards: 64,
            replication_factor: 2,
            ..Default::default()
        };

        let state = ProxyState::new(config).unwrap();
        SearchExecutor::new(state)
    }
}
