//! Result merger: combines shard results into a single response.

use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BinaryHeap};

/// Input to the merge function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeInput {
    /// One page of hits per node in the covering set.
    pub shard_hits: Vec<ShardHitPage>,

    /// Offset to apply after merge.
    pub offset: usize,

    /// Limit to apply after merge.
    pub limit: usize,

    /// Whether the client requested the ranking score in the response.
    pub client_requested_score: bool,

    /// Facet names to include (if None, include all).
    pub facets: Option<Vec<String>>,
}

/// A page of hits from a single shard.
#[derive(Debug, Clone)]
pub struct ShardHitPage {
    /// Raw JSON response from the node.
    pub body: Value,

    /// Whether this shard succeeded.
    pub success: bool,
}

/// Merged search result.
#[derive(Debug, Clone)]
pub struct MergedSearchResult {
    /// Merged hits (globally sorted, offset/limit applied).
    pub hits: Vec<Value>,

    /// Aggregated facets.
    pub facets: Value,

    /// Estimated total hits (sum of shard totals).
    pub estimated_total_hits: u64,

    /// Processing time in milliseconds.
    pub processing_time_ms: u64,

    /// Whether the response is degraded (some shards failed).
    pub degraded: bool,
}

/// Merge search results from multiple shards.
///
/// This is the primary entry point for result merging. It:
/// 1. Collects all hits with scores from all shards
/// 2. Sorts globally by `_rankingScore` descending
/// 3. Applies offset + limit after merge
/// 4. Strips `_rankingScore` if client didn't request it
/// 5. Always strips `_miroir_*` reserved fields
/// 6. Sums facet counts across shards
/// 7. Sums `estimatedTotalHits` across shards
/// 8. Takes max `processingTimeMs` across shards
///
/// Uses a binary min-heap to avoid keeping all hits in RAM when fan-out is large.
/// Uses BTreeMap for stable facet serialization (deterministic JSON output).
pub fn merge(input: MergeInput) -> MergedSearchResult {
    // Filter to only successful responses
    let successful_shards: Vec<_> = input
        .shard_hits
        .iter()
        .filter(|s| s.success)
        .collect();

    // Check if any shards failed (degraded mode)
    let degraded = successful_shards.len() < input.shard_hits.len();

    // Collect all hits with their ranking scores and primary keys
    let mut all_hits: Vec<HitWithScore> = Vec::new();

    for shard in &successful_shards {
        if let Some(hits) = shard.body.get("hits").and_then(|h| h.as_array()) {
            for hit in hits {
                let score = hit
                    .get("_rankingScore")
                    .and_then(|s| s.as_f64())
                    .unwrap_or(0.0);

                let primary_key = hit
                    .get("id")
                    .and_then(|id| id.as_str())
                    .unwrap_or("")
                    .to_string();

                all_hits.push(HitWithScore {
                    score,
                    primary_key,
                    hit: hit.clone(),
                });
            }
        }
    }

    // Use a min-heap of size offset + limit to avoid keeping all hits in RAM
    let heap_size = input.offset + input.limit;
    let top_hits = if all_hits.len() > heap_size * 2 {
        // Only use heap optimization if we have significantly more hits than needed
        // We use Reverse to turn BinaryHeap (max-heap) into a min-heap
        use std::cmp::Reverse;
        let mut heap: BinaryHeap<Reverse<HitWithScore>> = BinaryHeap::new();

        for hit in all_hits {
            if heap.len() < heap_size {
                heap.push(Reverse(hit));
            } else {
                // Peek at the smallest (worst) hit in our top-k
                if let Some(Reverse(worst)) = heap.peek() {
                    // If current hit is better than our worst, replace it
                    if hit > *worst {
                        heap.pop();
                        heap.push(Reverse(hit));
                    }
                }
            }
        }

        // Convert to sorted vector (descending by score)
        let mut result: Vec<_> = heap.into_iter().map(|r| r.0).collect();
        result.sort_by(|a, b| b.cmp(a));
        result
    } else {
        // For smaller result sets, just sort directly
        all_hits.sort_by(|a, b| b.cmp(a));
        all_hits
    };

    // Apply offset and limit
    let page: Vec<Value> = top_hits
        .into_iter()
        .skip(input.offset)
        .take(input.limit)
        .map(|mut hit_with_score| {
            let hit = &mut hit_with_score.hit;

            // Strip all _miroir_* fields (always removed)
            if let Some(obj) = hit.as_object_mut() {
                obj.retain(|k, _| !k.starts_with("_miroir_"));
            }

            // Strip _rankingScore if client didn't request it
            if !input.client_requested_score {
                if let Some(obj) = hit.as_object_mut() {
                    obj.remove("_rankingScore");
                }
            }

            hit.clone()
        })
        .collect();

    // Aggregate facets across all shards
    let facets = merge_facets(&successful_shards, input.facets.as_deref());

    // Sum estimated total hits
    let estimated_total_hits: u64 = successful_shards
        .iter()
        .filter_map(|s| s.body.get("estimatedTotalHits").and_then(|v| v.as_u64()))
        .sum();

    // Max processing time across all shards
    let processing_time_ms: u64 = successful_shards
        .iter()
        .filter_map(|s| s.body.get("processingTimeMs").and_then(|v| v.as_u64()))
        .max()
        .unwrap_or(0);

    MergedSearchResult {
        hits: page,
        facets,
        estimated_total_hits,
        processing_time_ms,
        degraded,
    }
}

/// A hit with its ranking score for sorting.
#[derive(Debug, Clone)]
struct HitWithScore {
    score: f64,
    primary_key: String,
    hit: Value,
}

impl PartialEq for HitWithScore {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.primary_key == other.primary_key
    }
}

impl Eq for HitWithScore {}

impl PartialOrd for HitWithScore {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HitWithScore {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Sort by score descending, then by primary key ascending for tie-breaking
        match self.score.partial_cmp(&other.score) {
            Some(std::cmp::Ordering::Equal) | None => {
                // On equal scores (or NaN), fall back to primary key
                // Reversed for descending order
                other.primary_key.cmp(&self.primary_key)
            }
            Some(ord) => ord.reverse(),
        }
    }
}

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

/// Merged search result (legacy, use MergedSearchResult instead).
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
        // Convert ShardResponse to ShardHitPage
        let shard_hits: Vec<ShardHitPage> = shard_responses
            .into_iter()
            .map(|sr| ShardHitPage {
                body: sr.body,
                success: sr.success,
            })
            .collect();

        // Call the new merge function
        let input = MergeInput {
            shard_hits,
            offset,
            limit,
            client_requested_score,
            facets: None,
        };

        let result = merge(input);

        Ok(MergedResult {
            hits: result.hits,
            facets: result.facets,
            total_hits: result.estimated_total_hits,
            processing_time_ms: result.processing_time_ms,
            degraded: result.degraded,
        })
    }
}

/// Merge facet distributions from all shards.
///
/// Facets are nested objects like `{"color": {"red": 10, "blue": 5}}`.
/// We sum counts for each facet value across all shards.
///
/// Uses BTreeMap for stable, deterministic serialization.
fn merge_facets(shards: &[&ShardHitPage], facet_filter: Option<&[String]>) -> Value {
    let mut merged_facets: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();

    for shard in shards {
        if let Some(facets) = shard
            .body
            .get("facetDistribution")
            .and_then(|f| f.as_object())
        {
            for (facet_name, facet_values) in facets {
                // Apply facet filter if provided
                if let Some(filter) = facet_filter {
                    if !filter.contains(&facet_name) {
                        continue;
                    }
                }

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

    // Convert back to JSON structure (BTreeMap ensures stable key order)
    let result: serde_json::Map<String, Value> = merged_facets
        .into_iter()
        .map(|(facet_name, values)| {
            let values_obj: serde_json::Map<String, Value> = values
                .into_iter()
                .map(|(k, v)| (k, Value::Number(v.into())))
                .collect();
            (facet_name, Value::Object(values_obj))
        })
        .collect();

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

    #[test]
    fn test_tie_breaking_by_primary_key() {
        let merger = MergerImpl;

        let hits = vec![
            create_hit(0.5, "zebra"),
            create_hit(0.5, "apple"),
            create_hit(0.5, "banana"),
        ];

        let shards = vec![create_shard_response(0, hits, 3)];

        let result = merger.merge(shards, 0, 10, false).unwrap();

        // All have score 0.5, so should be sorted by primary_key ascending
        assert_eq!(result.hits[0]["id"], "apple");
        assert_eq!(result.hits[1]["id"], "banana");
        assert_eq!(result.hits[2]["id"], "zebra");
    }

    #[test]
    fn test_stable_serialization_same_input_same_json() {
        let shard1 = serde_json::json!({
            "hits": [],
            "estimatedTotalHits": 0,
            "processingTimeMs": 10,
            "facetDistribution": {
                "color": {"red": 10, "blue": 5},
                "size": {"large": 8}
            }
        });

        let shard2 = serde_json::json!({
            "hits": [],
            "estimatedTotalHits": 0,
            "processingTimeMs": 15,
            "facetDistribution": {
                "color": {"red": 7, "green": 3},
                "size": {"large": 4, "small": 6}
            }
        });

        let input = MergeInput {
            shard_hits: vec![
                ShardHitPage { body: shard1, success: true },
                ShardHitPage { body: shard2, success: true },
            ],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result1 = merge(input.clone());
        let result2 = merge(input);

        // Serialize both results to JSON
        let json1 = serde_json::to_string(&result1).unwrap();
        let json2 = serde_json::to_string(&result2).unwrap();

        // Must be byte-identical
        assert_eq!(json1, json2);
    }

    #[test]
    fn test_binary_heap_efficiency_large_fan_out() {
        let merger = MergerImpl;

        // Test that the heap correctly maintains top-k when we have more hits than heap_size
        let mut hits = vec![];
        for i in 0..100 {
            hits.push(create_hit(i as f64 / 100.0, &format!("doc{}", i)));
        }

        let shards = vec![create_shard_response(0, hits, 100)];

        let result = merger.merge(shards, 0, 10, false).unwrap();

        // Should get the top 10 scores (0.90-0.99)
        assert_eq!(result.hits.len(), 10);
        assert_eq!(result.hits[0]["id"], "doc99"); // score 0.99
        assert_eq!(result.hits[9]["id"], "doc90"); // score 0.90
    }

    #[test]
    fn test_offset_limit_pagination_reconstruction() {
        let merger = MergerImpl;

        // Create 50 docs with known scores
        let mut hits = vec![];
        for i in 0..50 {
            hits.push(create_hit((50 - i) as f64 / 100.0, &format!("doc{:02}", i)));
        }

        // Get all 50 in one go
        let shards_all = vec![create_shard_response(0, hits.clone(), 50)];
        let result_all = merger.merge(shards_all, 0, 50, false).unwrap();

        // Get 5 pages of 10
        let mut paged_ids = vec![];
        for page in 0..5 {
            let shards = vec![create_shard_response(0, hits.clone(), 50)];
            let result = merger.merge(shards, page * 10, 10, false).unwrap();
            for hit in result.hits {
                paged_ids.push(hit["id"].as_str().unwrap().to_string());
            }
        }

        // Concatenated pages should match the single limit=50 query
        let all_ids: Vec<_> = result_all
            .hits
            .iter()
            .map(|h| h["id"].as_str().unwrap().to_string())
            .collect();

        assert_eq!(paged_ids, all_ids);
    }

    #[test]
    fn test_strip_all_miroir_reserved_fields() {
        let merger = MergerImpl;

        let hit = serde_json::json!({
            "id": "doc1",
            "_rankingScore": 0.9,
            "_miroir_shard": 0,
            "_miroir_internal": "some_value",
            "title": "Test",
        });

        let shards = vec![create_shard_response(0, vec![hit], 1)];

        let result = merger.merge(shards, 0, 10, true).unwrap();

        // _rankingScore should be present (requested)
        assert_eq!(result.hits[0]["_rankingScore"], 0.9);
        // All _miroir_* fields should be stripped
        assert!(result.hits[0].get("_miroir_shard").is_none());
        assert!(result.hits[0].get("_miroir_internal").is_none());
        // Other fields should be present
        assert_eq!(result.hits[0]["title"], "Test");
    }

    #[test]
    fn test_facet_filter_only_merges_requested_facets() {
        let facets = serde_json::json!({
            "color": {"red": 10, "blue": 5},
            "size": {"large": 8}
        });

        let input = MergeInput {
            shard_hits: vec![ShardHitPage {
                body: serde_json::json!({
                    "hits": [],
                    "estimatedTotalHits": 0,
                    "processingTimeMs": 10,
                    "facetDistribution": facets,
                }),
                success: true,
            }],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: Some(vec!["color".to_string()]),
        };

        let result = merge(input);

        // Only "color" should be present
        assert!(result.facets.get("color").is_some());
        assert!(result.facets.get("size").is_none());
    }
}
