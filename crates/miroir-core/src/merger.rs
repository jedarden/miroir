//! Result merger: combines shard results into a single response.

use crate::Result;
use serde_json::Value;
use std::collections::HashMap;

/// Result merger: combines responses from multiple shards.
pub trait Merger: Send + Sync {
    /// Merge search results from multiple shards.
    ///
    /// Takes the raw JSON responses from each shard and produces
    /// a merged result with global sorting, offset/limit applied,
    /// and facet aggregation.
    fn merge(
        &self,
        shard_responses: Vec<ShardResponse>,
        offset: usize,
        limit: usize,
        client_requested_score: bool,
    ) -> Result<MergedResult>;
}

/// Response from a single shard.
#[derive(Debug, Clone)]
pub struct ShardResponse {
    /// Shard identifier.
    pub shard_id: u32,

    /// Raw JSON response from the node.
    pub body: Value,

    /// Whether this shard succeeded.
    pub success: bool,
}

/// Merged search result.
#[derive(Debug, Clone)]
pub struct MergedResult {
    /// Merged hits (globally sorted, offset/limit applied).
    pub hits: Vec<Value>,

    /// Aggregated facets.
    pub facets: Value,

    /// Estimated total hits (sum of shard totals).
    pub total_hits: u64,

    /// Processing time in milliseconds.
    pub processing_time_ms: u64,

    /// Whether the response is degraded (some shards failed).
    pub degraded: bool,
}

/// Default implementation of Merger.
#[derive(Debug, Clone, Default)]
pub struct MergerImpl;

impl Merger for MergerImpl {
    fn merge(
        &self,
        shard_responses: Vec<ShardResponse>,
        offset: usize,
        limit: usize,
        client_requested_score: bool,
    ) -> Result<MergedResult> {
        // Filter to only successful responses
        let successful_shards: Vec<_> = shard_responses.iter().filter(|s| s.success).collect();

        // Check if any shards failed (degraded mode)
        let degraded = successful_shards.len() < shard_responses.len();

        // Collect all hits with their ranking scores
        let mut all_hits: Vec<(f64, Value)> = Vec::new();

        for shard in &successful_shards {
            if let Some(hits) = shard.body.get("hits").and_then(|h| h.as_array()) {
                for hit in hits {
                    // Extract ranking score
                    let score = hit
                        .get("_rankingScore")
                        .and_then(|s| s.as_f64())
                        .unwrap_or(0.0);

                    all_hits.push((score, hit.clone()));
                }
            }
        }

        // Sort globally by score descending
        all_hits.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        // Apply offset and limit
        let page: Vec<Value> = all_hits
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(_, mut hit)| {
                // Strip _miroir_shard (always removed)
                if let Some(obj) = hit.as_object_mut() {
                    obj.remove("_miroir_shard");
                }

                // Strip _rankingScore if client didn't request it
                if !client_requested_score {
                    if let Some(obj) = hit.as_object_mut() {
                        obj.remove("_rankingScore");
                    }
                }

                hit
            })
            .collect();

        // Aggregate facets across all shards
        let facets = merge_facets(&successful_shards);

        // Sum estimated total hits
        let total_hits: u64 = successful_shards
            .iter()
            .filter_map(|s| s.body.get("estimatedTotalHits").and_then(|v| v.as_u64()))
            .sum();

        // Max processing time across all shards
        let processing_time_ms: u64 = successful_shards
            .iter()
            .filter_map(|s| s.body.get("processingTimeMs").and_then(|v| v.as_u64()))
            .max()
            .unwrap_or(0);

        Ok(MergedResult {
            hits: page,
            facets,
            total_hits,
            processing_time_ms,
            degraded,
        })
    }
}

/// Merge facet distributions from all shards.
///
/// Facets are nested objects like `{"color": {"red": 10, "blue": 5}}`.
/// We sum counts for each facet value across all shards.
fn merge_facets(shards: &[&ShardResponse]) -> Value {
    let mut merged_facets: HashMap<String, HashMap<String, u64>> = HashMap::new();

    for shard in shards {
        if let Some(facets) = shard
            .body
            .get("facetDistribution")
            .and_then(|f| f.as_object())
        {
            for (facet_name, facet_values) in facets {
                if let Some(values_obj) = facet_values.as_object() {
                    let entry = merged_facets.entry(facet_name.clone()).or_default();

                    for (value, count) in values_obj {
                        let count_val = count.as_u64().unwrap_or(0);
                        *entry.entry(value.clone()).or_insert(0) += count_val;
                    }
                }
            }
        }
    }

    // Convert back to JSON structure
    let mut result = serde_json::Map::new();
    for (facet_name, values) in merged_facets {
        let values_obj: serde_json::Map<String, Value> = values
            .into_iter()
            .map(|(k, v)| (k, Value::Number(v.into())))
            .collect();
        result.insert(facet_name, Value::Object(values_obj));
    }

    Value::Object(result)
}

/// Stub implementation that returns empty results.
#[derive(Debug, Clone, Default)]
pub struct StubMerger;

impl Merger for StubMerger {
    fn merge(
        &self,
        _shard_responses: Vec<ShardResponse>,
        _offset: usize,
        _limit: usize,
        _client_requested_score: bool,
    ) -> Result<MergedResult> {
        Ok(MergedResult {
            hits: Vec::new(),
            facets: serde_json::json!({}),
            total_hits: 0,
            processing_time_ms: 0,
            degraded: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_hit(score: f64, id: &str) -> Value {
        serde_json::json!({
            "id": id,
            "_rankingScore": score,
            "_miroir_shard": 0,
        })
    }

    fn create_shard_response(shard_id: u32, hits: Vec<Value>, total: u64) -> ShardResponse {
        ShardResponse {
            shard_id,
            body: serde_json::json!({
                "hits": hits,
                "estimatedTotalHits": total,
                "processingTimeMs": 10,
                "facetDistribution": {},
            }),
            success: true,
        }
    }

    #[test]
    fn test_global_sort_by_ranking_score() {
        let merger = MergerImpl;

        let hits1 = vec![
            create_hit(0.5, "doc1"),
            create_hit(0.9, "doc2"),
            create_hit(0.3, "doc3"),
        ];
        let hits2 = vec![
            create_hit(0.7, "doc4"),
            create_hit(0.1, "doc5"),
            create_hit(0.8, "doc6"),
        ];

        let shards = vec![
            create_shard_response(0, hits1, 3),
            create_shard_response(1, hits2, 3),
        ];

        let result = merger.merge(shards, 0, 10, false).unwrap();

        // Check global ordering: should be doc2(0.9), doc6(0.8), doc4(0.7), doc1(0.5), doc3(0.3), doc5(0.1)
        assert_eq!(result.hits.len(), 6);
        assert_eq!(result.hits[0]["id"], "doc2");
        assert_eq!(result.hits[1]["id"], "doc6");
        assert_eq!(result.hits[2]["id"], "doc4");
        assert_eq!(result.hits[3]["id"], "doc1");
        assert_eq!(result.hits[4]["id"], "doc3");
        assert_eq!(result.hits[5]["id"], "doc5");
    }

    #[test]
    fn test_offset_and_limit_applied_after_merge() {
        let merger = MergerImpl;

        let hits = vec![
            create_hit(0.9, "doc1"),
            create_hit(0.8, "doc2"),
            create_hit(0.7, "doc3"),
            create_hit(0.6, "doc4"),
            create_hit(0.5, "doc5"),
        ];

        let shards = vec![create_shard_response(0, hits, 5)];

        let result = merger.merge(shards, 2, 2, false).unwrap();

        // Should skip first 2, take next 2
        assert_eq!(result.hits.len(), 2);
        assert_eq!(result.hits[0]["id"], "doc3");
        assert_eq!(result.hits[1]["id"], "doc4");
    }

    #[test]
    fn test_ranking_score_stripped_when_not_requested() {
        let merger = MergerImpl;

        let hits = vec![create_hit(0.9, "doc1"), create_hit(0.8, "doc2")];

        let shards = vec![create_shard_response(0, hits, 2)];

        let result = merger.merge(shards, 0, 10, false).unwrap();

        // _rankingScore should be stripped
        assert!(result.hits[0].get("_rankingScore").is_none());
        assert!(result.hits[1].get("_rankingScore").is_none());
    }

    #[test]
    fn test_ranking_score_included_when_requested() {
        let merger = MergerImpl;

        let hits = vec![create_hit(0.9, "doc1"), create_hit(0.8, "doc2")];

        let shards = vec![create_shard_response(0, hits, 2)];

        let result = merger.merge(shards, 0, 10, true).unwrap();

        // _rankingScore should be present
        assert_eq!(result.hits[0]["_rankingScore"], 0.9);
        assert_eq!(result.hits[1]["_rankingScore"], 0.8);
    }

    #[test]
    fn test_miroir_shard_always_stripped() {
        let merger = MergerImpl;

        let hits = vec![create_hit(0.9, "doc1")];

        let shards = vec![create_shard_response(0, hits, 1)];

        let result = merger.merge(shards, 0, 10, true).unwrap();

        // _miroir_shard should always be stripped
        assert!(result.hits[0].get("_miroir_shard").is_none());
    }

    #[test]
    fn test_facet_counts_summed_across_shards() {
        let merger = MergerImpl;

        let shard1 = serde_json::json!({
            "hits": [],
            "estimatedTotalHits": 0,
            "processingTimeMs": 10,
            "facetDistribution": {
                "color": {
                    "red": 10,
                    "blue": 5,
                },
                "size": {
                    "large": 8,
                }
            }
        });

        let shard2 = serde_json::json!({
            "hits": [],
            "estimatedTotalHits": 0,
            "processingTimeMs": 15,
            "facetDistribution": {
                "color": {
                    "red": 7,
                    "green": 3,
                },
                "size": {
                    "large": 4,
                    "small": 6,
                }
            }
        });

        let shards = vec![
            ShardResponse {
                shard_id: 0,
                body: shard1,
                success: true,
            },
            ShardResponse {
                shard_id: 1,
                body: shard2,
                success: true,
            },
        ];

        let result = merger.merge(shards, 0, 10, false).unwrap();

        let facets = result.facets;
        let color = facets.get("color").unwrap().as_object().unwrap();
        assert_eq!(color.get("red").unwrap().as_u64().unwrap(), 17); // 10 + 7
        assert_eq!(color.get("blue").unwrap().as_u64().unwrap(), 5); // only in shard1
        assert_eq!(color.get("green").unwrap().as_u64().unwrap(), 3); // only in shard2

        let size = facets.get("size").unwrap().as_object().unwrap();
        assert_eq!(size.get("large").unwrap().as_u64().unwrap(), 12); // 8 + 4
        assert_eq!(size.get("small").unwrap().as_u64().unwrap(), 6); // only in shard2
    }

    #[test]
    fn test_estimated_total_hits_summed() {
        let merger = MergerImpl;

        let shards = vec![
            create_shard_response(0, vec![], 100),
            create_shard_response(1, vec![], 150),
            create_shard_response(2, vec![], 75),
        ];

        let result = merger.merge(shards, 0, 10, false).unwrap();

        assert_eq!(result.total_hits, 325); // 100 + 150 + 75
    }

    #[test]
    fn test_processing_time_max_across_shards() {
        let merger = MergerImpl;

        let shard1 = serde_json::json!({
            "hits": [],
            "estimatedTotalHits": 0,
            "processingTimeMs": 10,
        });

        let shard2 = serde_json::json!({
            "hits": [],
            "estimatedTotalHits": 0,
            "processingTimeMs": 25,
        });

        let shard3 = serde_json::json!({
            "hits": [],
            "estimatedTotalHits": 0,
            "processingTimeMs": 15,
        });

        let shards = vec![
            ShardResponse {
                shard_id: 0,
                body: shard1,
                success: true,
            },
            ShardResponse {
                shard_id: 1,
                body: shard2,
                success: true,
            },
            ShardResponse {
                shard_id: 2,
                body: shard3,
                success: true,
            },
        ];

        let result = merger.merge(shards, 0, 10, false).unwrap();

        assert_eq!(result.processing_time_ms, 25); // max(10, 25, 15)
    }

    #[test]
    fn test_degraded_flag_when_shard_fails() {
        let merger = MergerImpl;

        let hits = vec![create_hit(0.9, "doc1")];

        let shards = vec![
            create_shard_response(0, hits.clone(), 1),
            ShardResponse {
                shard_id: 1,
                body: serde_json::json!({}),
                success: false,
            },
        ];

        let result = merger.merge(shards, 0, 10, false).unwrap();

        assert!(result.degraded);
        assert_eq!(result.hits.len(), 1);
    }

    #[test]
    fn test_not_degraded_when_all_succeed() {
        let merger = MergerImpl;

        let shards = vec![
            create_shard_response(0, vec![], 0),
            create_shard_response(1, vec![], 0),
        ];

        let result = merger.merge(shards, 0, 10, false).unwrap();

        assert!(!result.degraded);
    }

    #[test]
    fn test_empty_shards_returns_empty_result() {
        let merger = MergerImpl;

        let result = merger.merge(vec![], 0, 10, false).unwrap();

        assert!(result.hits.is_empty());
        assert_eq!(result.total_hits, 0);
        assert_eq!(result.facets.as_object().unwrap().len(), 0);
        assert!(!result.degraded);
    }

    #[test]
    fn test_facet_keys_unique_to_one_shard_preserved() {
        let merger = MergerImpl;

        let shard1 = serde_json::json!({
            "hits": [],
            "estimatedTotalHits": 0,
            "processingTimeMs": 10,
            "facetDistribution": {
                "category": {
                    "electronics": 20,
                    "books": 15,
                }
            }
        });

        let shard2 = serde_json::json!({
            "hits": [],
            "estimatedTotalHits": 0,
            "processingTimeMs": 10,
            "facetDistribution": {
                "category": {
                    "clothing": 30,
                    "food": 10,
                }
            }
        });

        let shards = vec![
            ShardResponse {
                shard_id: 0,
                body: shard1,
                success: true,
            },
            ShardResponse {
                shard_id: 1,
                body: shard2,
                success: true,
            },
        ];

        let result = merger.merge(shards, 0, 10, false).unwrap();

        let category = result.facets.get("category").unwrap().as_object().unwrap();
        assert_eq!(category.get("electronics").unwrap().as_u64().unwrap(), 20);
        assert_eq!(category.get("books").unwrap().as_u64().unwrap(), 15);
        assert_eq!(category.get("clothing").unwrap().as_u64().unwrap(), 30);
        assert_eq!(category.get("food").unwrap().as_u64().unwrap(), 10);
    }

    #[test]
    fn test_missing_facet_distribution_handled_gracefully() {
        let merger = MergerImpl;

        let shard_with_facets = serde_json::json!({
            "hits": [],
            "estimatedTotalHits": 0,
            "processingTimeMs": 10,
            "facetDistribution": {
                "color": {"red": 5}
            }
        });

        let shard_without_facets = serde_json::json!({
            "hits": [],
            "estimatedTotalHits": 0,
            "processingTimeMs": 10,
        });

        let shards = vec![
            ShardResponse {
                shard_id: 0,
                body: shard_with_facets,
                success: true,
            },
            ShardResponse {
                shard_id: 1,
                body: shard_without_facets,
                success: true,
            },
        ];

        let result = merger.merge(shards, 0, 10, false).unwrap();

        // Should still have facets from the shard that provided them
        let color = result.facets.get("color").unwrap().as_object().unwrap();
        assert_eq!(color.get("red").unwrap().as_u64().unwrap(), 5);
    }

    #[test]
    fn test_offset_exceeds_total_hits() {
        let merger = MergerImpl;

        let hits = vec![create_hit(0.9, "doc1"), create_hit(0.8, "doc2")];

        let shards = vec![create_shard_response(0, hits, 2)];

        let result = merger.merge(shards, 10, 10, false).unwrap();

        assert!(result.hits.is_empty());
    }

    #[test]
    fn test_limit_exceeds_available_hits() {
        let merger = MergerImpl;

        let hits = vec![create_hit(0.9, "doc1"), create_hit(0.8, "doc2")];

        let shards = vec![create_shard_response(0, hits, 2)];

        let result = merger.merge(shards, 0, 10, false).unwrap();

        assert_eq!(result.hits.len(), 2);
    }
}
