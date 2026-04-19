//! Result merger: combines shard results into a single response.

use crate::Result;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::cmp::Ordering;

/// Input to the merge operation.
#[derive(Debug, Clone)]
pub struct MergeInput {
    /// One response page per node in the covering set.
    pub shard_hits: Vec<ShardHitPage>,

    /// Original offset from the client request.
    pub offset: usize,

    /// Original limit from the client request.
    pub limit: usize,

    /// Whether the client requested scores in the response.
    pub client_requested_score: bool,

    /// Facet names requested (for filtering which facets to return).
    pub facets: Option<Vec<String>>,
}

/// Response from a single shard (node).
#[derive(Debug, Clone)]
pub struct ShardHitPage {
    /// Raw JSON response from the node.
    pub body: Value,
}

/// Merged search result.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MergedSearchResult {
    /// Merged hits (globally sorted, offset/limit applied).
    pub hits: Vec<Value>,

    /// Aggregated facet distribution.
    pub facet_distribution: Option<BTreeMap<String, BTreeMap<String, u64>>>,

    /// Estimated total hits (sum of shard totals).
    pub estimated_total_hits: u64,

    /// Processing time in milliseconds (max across covering set).
    pub processing_time_ms: u64,

    /// Whether the response is degraded (some shards had errors).
    pub degraded: bool,
}

/// RRF constant k.
///
/// This is the denominator constant used in Reciprocal Rank Fusion.
/// The value 60 is the default recommended in the RRF literature and
/// is used by OpenSearch for hybrid search.
const RRF_K: u32 = 60;

/// A document with its accumulated RRF score.
#[derive(Debug, Clone)]
struct RRFDocument {
    /// Accumulated RRF score across all shards.
    rrf_score: f64,

    /// Primary key for tie-breaking.
    primary_key: String,

    /// The hit document (JSON object) from the highest-ranking shard.
    hit: Map<String, Value>,
}

impl PartialEq for RRFDocument {
    fn eq(&self, other: &Self) -> bool {
        self.rrf_score == other.rrf_score && self.primary_key == other.primary_key
    }
}

impl Eq for RRFDocument {}

impl PartialOrd for RRFDocument {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RRFDocument {
    fn cmp(&self, other: &Self) -> Ordering {
        // Primary sort: RRF score descending (higher score = better rank)
        match self.rrf_score.partial_cmp(&other.rrf_score) {
            Some(Ordering::Equal) => {
                // Secondary sort: primary key ascending for deterministic tie-breaking
                self.primary_key.cmp(&other.primary_key)
            }
            Some(ord) => ord.reverse(),
            None => {
                // NaN case: treat as lowest score
                if self.rrf_score.is_nan() && !other.rrf_score.is_nan() {
                    Ordering::Less
                } else if !self.rrf_score.is_nan() && other.rrf_score.is_nan() {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
        }
    }
}

/// Merge search results from multiple shards into a single response.
///
/// This is a pure function with no side effects, making it testable
/// without a network and ensuring deterministic output.
pub fn merge(input: MergeInput) -> Result<MergedSearchResult> {
    let mut estimated_total_hits = 0u64;
    let mut max_processing_time = 0u64;
    let mut degraded = false;

    // Collect all hits with their ranks from all shards.
    // Use a map to aggregate RRF scores for documents appearing in multiple shards.
    let mut rrf_map: std::collections::HashMap<String, RRFDocument> = std::collections::HashMap::new();

    for shard_page in &input.shard_hits {
        let body = &shard_page.body;

        // Check for degraded response.
        if let Some(serde_json::Value::Bool(false)) = body.get("success") {
            degraded = true;
            continue;
        }

        // Extract estimated total hits.
        if let Some(Value::Number(n)) = body.get("estimatedTotalHits") {
            if let Some(n) = n.as_u64() {
                estimated_total_hits = estimated_total_hits.saturating_add(n);
            }
        }

        // Extract processing time.
        if let Some(Value::Number(n)) = body.get("processingTimeMs") {
            if let Some(n) = n.as_u64() {
                max_processing_time = max_processing_time.max(n);
            }
        }

        // Extract hits with ranks (position in shard's results).
        if let Some(Value::Array(hits)) = body.get("hits") {
            for (rank, hit) in hits.iter().enumerate() {
                if let Value::Object(ref map) = hit {
                    let map = map.clone();

                    // Extract primary key for deduplication.
                    let primary_key = map
                        .get("id")
                        .or_else(|| map.get("pk"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    // Compute RRF contribution: 1 / (k + rank)
                    // rank is 0-based, so we add 1 to convert to 1-based for RRF formula
                    let rrf_contribution = 1.0 / ((RRF_K as f64) + (rank as f64) + 1.0);

                    // Aggregate RRF scores across shards.
                    use std::collections::hash_map::Entry;
                    match rrf_map.entry(primary_key.clone()) {
                        Entry::Vacant(e) => {
                            e.insert(RRFDocument {
                                rrf_score: rrf_contribution,
                                primary_key,
                                hit: map,
                            });
                        }
                        Entry::Occupied(mut e) => {
                            // Document appears in multiple shards: sum RRF contributions.
                            e.get_mut().rrf_score += rrf_contribution;
                        }
                    }
                }
            }
        }
    }

    // Convert map to vec and sort by RRF score descending.
    let mut merged_docs: Vec<_> = rrf_map.into_values().collect();
    merged_docs.sort();

    // Apply offset + limit.
    let skip = input.offset;
    let take = input.limit;
    let paginated_hits: Vec<_> = merged_docs
        .into_iter()
        .skip(skip)
        .take(take)
        .collect();

    // Strip reserved fields and rebuild hits.
    let mut hits = Vec::with_capacity(paginated_hits.len());
    for mut doc in paginated_hits {
        // Strip _rankingScore if not requested (RRF doesn't use original scores).
        if !input.client_requested_score {
            doc.hit.remove("_rankingScore");
        }

        // Always strip _miroir_* fields.
        doc.hit.retain(|k, _| !k.starts_with("_miroir_"));

        hits.push(Value::Object(doc.hit));
    }

    // Merge facets.
    let facet_distribution = merge_facets(&input.shard_hits, input.facets.as_deref());

    Ok(MergedSearchResult {
        hits,
        facet_distribution,
        estimated_total_hits,
        processing_time_ms: max_processing_time,
        degraded,
    })
}

/// Merge facet distributions from multiple shards.
///
/// Uses BTreeMap for stable ordering (deterministic serialization).
fn merge_facets(
    shard_pages: &[ShardHitPage],
    requested_facets: Option<&[String]>,
) -> Option<BTreeMap<String, BTreeMap<String, u64>>> {
    let mut merged: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();

    for shard_page in shard_pages {
        let body = &shard_page.body;

        // Meilisearch uses "facetDistribution" for facet results.
        if let Some(Value::Object(facets)) = body.get("facetDistribution") {
            for (facet_name, facet_values) in facets {
                // Skip if not requested (if a filter was provided).
                if let Some(requested) = requested_facets {
                    if !requested.iter().any(|f| f == facet_name) {
                        continue;
                    }
                }

                if let Value::Object(values_map) = facet_values {
                    let merged_facet = merged.entry(facet_name.clone()).or_default();
                    for (value, count) in values_map {
                        if let Value::Number(n) = count {
                            if let Some(n) = n.as_u64() {
                                *merged_facet.entry(value.clone()).or_insert(0) += n;
                            }
                        }
                    }
                }
            }
        }
    }

    if merged.is_empty() {
        None
    } else {
        Some(merged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_hit(id: &str, score: f64, shard: u32) -> Value {
        json!({
            "id": id,
            "title": format!("Document {}", id),
            "_rankingScore": score,
            "_miroir_shard": shard,
        })
    }

    #[allow(dead_code)]
    fn make_hit_ranked(id: &str, shard: u32) -> Value {
        json!({
            "id": id,
            "title": format!("Document {}", id),
            "_rankingScore": 0.5, // RRF ignores score, only rank matters
            "_miroir_shard": shard,
        })
    }

    fn make_shard_response(
        hits: Vec<Value>,
        total_hits: u64,
        processing_time: u64,
    ) -> ShardHitPage {
        ShardHitPage {
            body: json!({
                "hits": hits,
                "estimatedTotalHits": total_hits,
                "processingTimeMs": processing_time,
                "facetDistribution": {},
            }),
        }
    }

    #[test]
    fn test_merge_basic() {
        let input = MergeInput {
            shard_hits: vec![make_shard_response(
                vec![
                    make_hit("doc1", 0.9, 0),
                    make_hit("doc2", 0.7, 0),
                ],
                100,
                15,
            )],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result = merge(input).unwrap();
        assert_eq!(result.hits.len(), 2);
        assert_eq!(result.estimated_total_hits, 100);
        assert_eq!(result.processing_time_ms, 15);
        assert!(!result.degraded);

        // Score should be stripped.
        let hit = &result.hits[0];
        assert!(hit.get("_rankingScore").is_none());
        assert!(hit.get("_miroir_shard").is_none());
        assert_eq!(hit.get("id").unwrap(), "doc1");
    }

    #[test]
    fn test_merge_global_sort() {
        let input = MergeInput {
            shard_hits: vec![
                make_shard_response(vec![make_hit("doc1", 0.5, 0)], 50, 10),
                make_shard_response(vec![make_hit("doc2", 0.9, 1)], 50, 10),
                make_shard_response(vec![make_hit("doc3", 0.7, 2)], 50, 10),
            ],
            offset: 0,
            limit: 10,
            client_requested_score: true,
            facets: None,
        };

        let result = merge(input).unwrap();
        assert_eq!(result.hits.len(), 3);

        // RRF: all docs are at rank 0 in their respective shards.
        // With equal ranks, tie-break by primary key (alphabetically).
        assert_eq!(result.hits[0].get("id").unwrap(), "doc1");
        assert_eq!(result.hits[1].get("id").unwrap(), "doc2");
        assert_eq!(result.hits[2].get("id").unwrap(), "doc3");
    }

    #[test]
    fn test_merge_rrf_rank_ordering() {
        // Test RRF: higher rank (lower position) contributes more to RRF score.
        // shard0: [low_rank_doc (rank 0), mid_rank_doc (rank 10)]
        // shard1: [high_rank_doc (rank 0)]
        // shard2: [mid_rank_doc (rank 0) - same doc appears in two shards!]
        //
        // Expected RRF scores:
        // - low_rank_doc: 1/(60+0+1) = 1/61 (only in shard0)
        // - high_rank_doc: 1/(60+0+1) = 1/61 (only in shard1)
        // - mid_rank_doc: 1/(60+10+1) + 1/(60+0+1) = 1/71 + 1/61 (rank 10 in shard0, rank 0 in shard2)
        //
        // mid_rank_doc should win because it appears in multiple shards.

        let mut shard0_hits = vec![];
        let mut shard1_hits = vec![];
        let mut shard2_hits = vec![];

        // Build shard0: low_rank_doc at position 0, mid_rank_doc at position 10
        shard0_hits.push(make_hit("low_rank_doc", 0.1, 0));
        for i in 0..9 {
            shard0_hits.push(make_hit(&format!("filler_0_{}", i), 0.5, 0));
        }
        shard0_hits.push(make_hit("mid_rank_doc", 0.2, 0));

        // shard1: high_rank_doc at position 0
        shard1_hits.push(make_hit("high_rank_doc", 0.3, 1));

        // shard2: mid_rank_doc at position 0 (same doc appears again!)
        shard2_hits.push(make_hit("mid_rank_doc", 0.4, 2));

        let input = MergeInput {
            shard_hits: vec![
                make_shard_response(shard0_hits, 100, 10),
                make_shard_response(shard1_hits, 100, 10),
                make_shard_response(shard2_hits, 100, 10),
            ],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result = merge(input).unwrap();

        // mid_rank_doc should be first because it appears in multiple shards
        // (RRF contributions sum: rank 10 in shard0 + rank 0 in shard2)
        assert_eq!(result.hits[0].get("id").unwrap(), "mid_rank_doc");

        // low_rank_doc and high_rank_doc both at rank 0 in their shards
        // Tie-break by primary key alphabetically
        assert_eq!(result.hits[1].get("id").unwrap(), "high_rank_doc");
        assert_eq!(result.hits[2].get("id").unwrap(), "low_rank_doc");
    }

    #[test]
    fn test_merge_rrf_duplicate_handling() {
        // Test that the same document appearing in multiple shards
        // gets its RRF score summed.
        //
        // doc1 appears at rank 0 in shard0 and rank 5 in shard1
        // doc2 appears at rank 0 in shard2
        //
        // RRF(doc1) = 1/(60+0+1) + 1/(60+5+1) = 1/61 + 1/66
        // RRF(doc2) = 1/(60+0+1) = 1/61
        //
        // doc1 should rank higher.

        let shard0_hits = vec![make_hit("doc1", 0.1, 0)];
        let mut shard1_hits = vec![];
        let shard2_hits = vec![make_hit("doc2", 0.9, 2)];

        // Add filler hits to shard1 to make doc1 appear at rank 5
        for i in 0..5 {
            shard1_hits.push(make_hit(&format!("filler_1_{}", i), 0.5, 1));
        }
        shard1_hits.push(make_hit("doc1", 0.2, 1));

        let input = MergeInput {
            shard_hits: vec![
                make_shard_response(shard0_hits, 50, 10),
                make_shard_response(shard1_hits, 50, 10),
                make_shard_response(shard2_hits, 50, 10),
            ],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result = merge(input).unwrap();

        // doc1 should rank higher despite lower score because
        // it appears in multiple shards and gets RRF contribution boost
        assert_eq!(result.hits[0].get("id").unwrap(), "doc1");
        assert_eq!(result.hits[1].get("id").unwrap(), "doc2");
    }

    #[test]
    fn test_merge_offset_limit() {
        let input = MergeInput {
            shard_hits: vec![make_shard_response(
                vec![
                    make_hit("doc1", 0.9, 0),
                    make_hit("doc2", 0.8, 0),
                    make_hit("doc3", 0.7, 0),
                    make_hit("doc4", 0.6, 0),
                    make_hit("doc5", 0.5, 0),
                ],
                100,
                10,
            )],
            offset: 1,
            limit: 2,
            client_requested_score: false,
            facets: None,
        };

        let result = merge(input).unwrap();
        assert_eq!(result.hits.len(), 2);
        assert_eq!(result.hits[0].get("id").unwrap(), "doc2");
        assert_eq!(result.hits[1].get("id").unwrap(), "doc3");
    }

    #[test]
    fn test_merge_preserves_score_when_requested() {
        // RRF doesn't use scores for ranking, but we still preserve
        // the original score field when requested.
        let input = MergeInput {
            shard_hits: vec![make_shard_response(
                vec![make_hit("doc1", 0.9, 0)],
                50,
                10,
            )],
            offset: 0,
            limit: 10,
            client_requested_score: true,
            facets: None,
        };

        let result = merge(input).unwrap();
        assert_eq!(
            result.hits[0].get("_rankingScore").unwrap().as_f64(),
            Some(0.9)
        );
    }

    #[test]
    fn test_merge_rrf_ignores_scores() {
        // Test that RRF ordering is based on rank, not score.
        // Even though doc3 has highest score, it's ranked lower in its shard.
        //
        // shard0: doc2 at rank 0 with score 0.9 (same rank, tie-break by id)
        // shard1: doc1 at rank 0 with score 0.1 (same rank, tie-break by id)
        // shard2: doc3 at rank 2 (position 2) with score 1.0 (lower rank)
        //
        // RRF scores:
        // - doc1: 1/61 (rank 0)
        // - doc2: 1/61 (rank 0)
        // - filler: 1/62 (rank 1)
        // - doc3: 1/63 (rank 2)
        //
        // Ordering: doc1, doc2 (tie-break alphabetically), filler, then doc3

        let shard0_hits = vec![make_hit("doc2", 0.9, 0)]; // High score, rank 0
        let shard1_hits = vec![make_hit("doc1", 0.1, 0)]; // Low score, rank 0
        let shard2_hits = vec![
            make_hit("filler", 0.5, 2),
            make_hit("filler2", 0.5, 2),
            make_hit("doc3", 1.0, 2),  // Highest score, but rank 2
        ];

        let input = MergeInput {
            shard_hits: vec![
                make_shard_response(shard0_hits, 50, 10),
                make_shard_response(shard1_hits, 50, 10),
                make_shard_response(shard2_hits, 50, 10),
            ],
            offset: 0,
            limit: 10,
            client_requested_score: true,
            facets: None,
        };

        let result = merge(input).unwrap();

        // doc1 and doc2 both at rank 0, tie-break alphabetically
        assert_eq!(result.hits[0].get("id").unwrap(), "doc1");
        assert_eq!(result.hits[1].get("id").unwrap(), "doc2");
        // filler and filler2 at rank 1
        assert_eq!(result.hits[2].get("id").unwrap(), "filler");
        assert_eq!(result.hits[3].get("id").unwrap(), "filler2");
        // doc3 at rank 2, comes last despite highest score
        assert_eq!(result.hits[4].get("id").unwrap(), "doc3");
    }

    #[test]
    fn test_merge_strips_miroir_fields() {
        let input = MergeInput {
            shard_hits: vec![make_shard_response(
                vec![json!({
                    "id": "doc1",
                    "title": "Test",
                    "_rankingScore": 0.9,
                    "_miroir_shard": 0,
                    "_miroir_node": "node-1",
                    "_miroir_group": 1,
                })],
                50,
                10,
            )],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result = merge(input).unwrap();
        let hit = &result.hits[0];

        assert!(hit.get("_rankingScore").is_none());
        assert!(hit.get("_miroir_shard").is_none());
        assert!(hit.get("_miroir_node").is_none());
        assert!(hit.get("_miroir_group").is_none());

        // Non-reserved fields preserved.
        assert_eq!(hit.get("id").unwrap(), "doc1");
        assert_eq!(hit.get("title").unwrap(), "Test");
    }

    #[test]
    fn test_merge_facets() {
        let shard1 = ShardHitPage {
            body: json!({
                "hits": [],
                "estimatedTotalHits": 100,
                "processingTimeMs": 10,
                "facetDistribution": {
                    "category": {
                        "electronics": 50,
                        "books": 30,
                    },
                    "brand": {
                        "apple": 20,
                    },
                },
            }),
        };

        let shard2 = ShardHitPage {
            body: json!({
                "hits": [],
                "estimatedTotalHits": 100,
                "processingTimeMs": 15,
                "facetDistribution": {
                    "category": {
                        "electronics": 40,
                        "clothing": 25,
                    },
                    "brand": {
                        "samsung": 15,
                    },
                },
            }),
        };

        let input = MergeInput {
            shard_hits: vec![shard1, shard2],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result = merge(input).unwrap();
        let facets = result.facet_distribution.unwrap();

        // Check category merging.
        let category = facets.get("category").unwrap();
        assert_eq!(category.get("electronics"), Some(&90));
        assert_eq!(category.get("books"), Some(&30));
        assert_eq!(category.get("clothing"), Some(&25));

        // Check brand merging.
        let brand = facets.get("brand").unwrap();
        assert_eq!(brand.get("apple"), Some(&20));
        assert_eq!(brand.get("samsung"), Some(&15));
    }

    #[test]
    fn test_merge_facets_filter() {
        let shard = ShardHitPage {
            body: json!({
                "hits": [],
                "estimatedTotalHits": 100,
                "processingTimeMs": 10,
                "facetDistribution": {
                    "category": {"electronics": 50},
                    "brand": {"apple": 20},
                },
            }),
        };

        let input = MergeInput {
            shard_hits: vec![shard],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: Some(vec!["category".to_string()]),
        };

        let result = merge(input).unwrap();
        let facets = result.facet_distribution.unwrap();

        assert!(facets.contains_key("category"));
        assert!(!facets.contains_key("brand"));
    }

    #[test]
    fn test_merge_estimated_total_hits_sum() {
        let input = MergeInput {
            shard_hits: vec![
                make_shard_response(vec![], 100, 10),
                make_shard_response(vec![], 150, 15),
                make_shard_response(vec![], 200, 20),
            ],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result = merge(input).unwrap();
        assert_eq!(result.estimated_total_hits, 450);
    }

    #[test]
    fn test_merge_processing_time_max() {
        let input = MergeInput {
            shard_hits: vec![
                make_shard_response(vec![], 100, 10),
                make_shard_response(vec![], 100, 25),
                make_shard_response(vec![], 100, 15),
            ],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result = merge(input).unwrap();
        assert_eq!(result.processing_time_ms, 25);
    }

    #[test]
    fn test_merge_tie_breaking() {
        let input = MergeInput {
            shard_hits: vec![
                make_shard_response(vec![make_hit("zebra", 0.5, 0)], 50, 10),
                make_shard_response(vec![make_hit("apple", 0.5, 1)], 50, 10),
            ],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result = merge(input).unwrap();
        // RRF: both docs at rank 0 in their shards, equal RRF scores.
        // Tie-break by primary key (apple < zebra lexicographically).
        assert_eq!(result.hits[0].get("id").unwrap(), "apple");
        assert_eq!(result.hits[1].get("id").unwrap(), "zebra");
    }

    #[test]
    fn test_merge_degraded_flag() {
        let failed_shard = ShardHitPage {
            body: json!({
                "success": false,
                "message": "node unavailable",
            }),
        };

        let input = MergeInput {
            shard_hits: vec![
                make_shard_response(vec![make_hit("doc1", 0.9, 0)], 50, 10),
                failed_shard,
            ],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result = merge(input).unwrap();
        assert!(result.degraded);
    }

    #[test]
    fn test_stable_serialization() {
        let shard1 = ShardHitPage {
            body: json!({
                "hits": [make_hit("doc1", 0.9, 0)],
                "estimatedTotalHits": 100,
                "processingTimeMs": 10,
                "facetDistribution": {
                    "category": {"electronics": 50, "books": 30},
                },
            }),
        };

        let shard2 = ShardHitPage {
            body: json!({
                "hits": [make_hit("doc2", 0.8, 1)],
                "estimatedTotalHits": 100,
                "processingTimeMs": 15,
                "facetDistribution": {
                    "brand": {"apple": 20},
                },
            }),
        };

        let input = MergeInput {
            shard_hits: vec![shard1.clone(), shard2.clone()],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result1 = merge(input.clone()).unwrap();
        let result2 = merge(input).unwrap();

        // Serialize both to JSON.
        let json1 = serde_json::to_value(&result1).unwrap();
        let json2 = serde_json::to_value(&result2).unwrap();

        // Byte-identical.
        assert_eq!(
            serde_json::to_vec(&json1).unwrap(),
            serde_json::to_vec(&json2).unwrap()
        );
    }

    #[test]
    fn test_page_reconstruction() {
        // Test that pages of 10 reconstruct a single limit=50 result.
        let mut all_hits = Vec::new();
        for i in 0..50 {
            all_hits.push(make_hit(&format!("doc{:02}", i), (50 - i) as f64 / 100.0, 0));
        }

        let input = MergeInput {
            shard_hits: vec![make_shard_response(all_hits, 50, 10)],
            offset: 0,
            limit: 50,
            client_requested_score: false,
            facets: None,
        };

        let full_result = merge(input.clone()).unwrap();
        assert_eq!(full_result.hits.len(), 50);

        // Now fetch in pages of 10 and verify they match.
        for page in 0..5 {
            let page_input = MergeInput {
                shard_hits: input.shard_hits.clone(),
                offset: page * 10,
                limit: 10,
                client_requested_score: false,
                facets: None,
            };
            let page_result = merge(page_input).unwrap();

            let start = page * 10;
            let end = start + 10;
            assert_eq!(
                page_result.hits,
                full_result.hits[start..end],
                "Page {} mismatch",
                page
            );
        }
    }
}
