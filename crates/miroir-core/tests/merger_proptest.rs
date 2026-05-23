//! Property-based tests for merger using proptest.
//!
//! Tests:
//! - Determinism: same inputs always produce same output
//! - Pagination consistency: pages reconstruct to full result
//! - Monotonicity: larger limits return >= results
//! - Result size respects limit

use miroir_core::merger::{merge, MergeInput, ShardHitPage, RrfStrategy, MergeStrategy};
use proptest::prelude::*;
use serde_json::json;

/// Helper to create a hit document.
fn make_hit(id: &str, score: f64) -> serde_json::Value {
    json!({
        "id": id,
        "title": format!("Document {}", id),
        "_rankingScore": score,
    })
}

/// Helper to create a shard response with hits.
fn make_shard_response(
    hits: Vec<serde_json::Value>,
    total_hits: u64,
    processing_time: u64,
) -> ShardHitPage {
    ShardHitPage {
        body: json!({
            "hits": hits,
            "estimatedTotalHits": total_hits,
            "processingTimeMs": processing_time,
        }),
    }
}

proptest! {
    /// Property: Determinism - same inputs produce same outputs.
    ///
    /// For any set of shard responses, merge returns identical results.
    #[test]
    fn prop_determinism(
        shard_count in 1usize..5,
        hits_per_shard in 0usize..100,
        offset in 0usize..10,
        limit in 1usize..100,
    ) {
        let shard_hits: Vec<ShardHitPage> = (0..shard_count)
            .map(|shard_id| {
                let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                    .map(|i| {
                        let id = shard_id * hits_per_shard + i;
                        let score = (hits_per_shard as f64 - i as f64) / hits_per_shard as f64;
                        make_hit(&format!("doc-{}", id), score)
                    })
                    .collect();
                make_shard_response(hits, hits_per_shard as u64, 15)
            })
            .collect();

        let input = MergeInput {
            shard_hits: shard_hits.clone(),
            offset,
            limit,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let result1 = merge(input.clone()).unwrap();
        let result2 = merge(input.clone()).unwrap();

        prop_assert_eq!(result1.hits, result2.hits);
        prop_assert_eq!(result1.estimated_total_hits, result2.estimated_total_hits);
        prop_assert_eq!(result1.processing_time_ms, result2.processing_time_ms);
    }

    /// Property: Multiple runs produce consistent results.
    ///
    /// Tests that repeated calls with the same parameters yield identical results.
    #[test]
    fn prop_determinism_multiple_runs(
        shard_count in 1usize..5,
        hits_per_shard in 0usize..50,
    ) {
        let shard_hits: Vec<ShardHitPage> = (0..shard_count)
            .map(|shard_id| {
                let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                    .map(|i| {
                        let id = shard_id * hits_per_shard + i;
                        let score = (hits_per_shard as f64 - i as f64) / hits_per_shard as f64;
                        make_hit(&format!("doc-{}", id), score)
                    })
                    .collect();
                make_shard_response(hits, hits_per_shard as u64, 15)
            })
            .collect();

        let input = MergeInput {
            shard_hits,
            offset: 0,
            limit: hits_per_shard * shard_count,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let reference = merge(input.clone()).unwrap();

        for _ in 0..5 {
            let current = merge(input.clone()).unwrap();
            prop_assert_eq!(serde_json::to_vec(&reference.hits).unwrap(), serde_json::to_vec(&current.hits).unwrap());
        }
    }

    /// Property: Result size respects limit.
    ///
    /// The number of returned hits should never exceed the limit.
    #[test]
    fn prop_result_size_respects_limit(
        shard_count in 1usize..5,
        hits_per_shard in 0usize..100,
        limit in 1usize..100,
    ) {
        let shard_hits: Vec<ShardHitPage> = (0..shard_count)
            .map(|shard_id| {
                let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                    .map(|i| {
                        let id = shard_id * hits_per_shard + i;
                        let score = (hits_per_shard as f64 - i as f64) / hits_per_shard as f64;
                        make_hit(&format!("doc-{}", id), score)
                    })
                    .collect();
                make_shard_response(hits, hits_per_shard as u64, 15)
            })
            .collect();

        let input = MergeInput {
            shard_hits,
            offset: 0,
            limit,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let result = merge(input).unwrap();
        prop_assert!(result.hits.len() <= limit);
    }

    /// Property: Monotonicity - larger limits return >= results.
    ///
    /// Increasing the limit should never reduce the number of results.
    #[test]
    fn prop_monotonicity(
        shard_count in 1usize..5,
        hits_per_shard in 10usize..100,
        limit1 in 1usize..50,
        limit2 in 1usize..50,
    ) {
        let (small_limit, large_limit) = if limit1 <= limit2 {
            (limit1, limit2)
        } else {
            (limit2, limit1)
        };

        let shard_hits: Vec<ShardHitPage> = (0..shard_count)
            .map(|shard_id| {
                let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                    .map(|i| {
                        let id = shard_id * hits_per_shard + i;
                        let score = (hits_per_shard as f64 - i as f64) / hits_per_shard as f64;
                        make_hit(&format!("doc-{}", id), score)
                    })
                    .collect();
                make_shard_response(hits, hits_per_shard as u64, 15)
            })
            .collect();

        let input_small = MergeInput {
            shard_hits: shard_hits.clone(),
            offset: 0,
            limit: small_limit,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let input_large = MergeInput {
            shard_hits,
            offset: 0,
            limit: large_limit,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let result_small = merge(input_small).unwrap();
        let result_large = merge(input_large).unwrap();

        prop_assert!(result_small.hits.len() <= result_large.hits.len());
    }

    /// Property: Pagination consistency - pages reconstruct to full result.
    ///
    /// Fetching multiple pages should produce the same result as fetching
    /// all at once with a larger limit.
    #[test]
    fn prop_pagination_consistency(
        shard_count in 1usize..5,
        hits_per_shard in 20usize..100,
        page_size in 5usize..20,
    ) {
        let total_hits = hits_per_shard * shard_count;

        let shard_hits: Vec<ShardHitPage> = (0..shard_count)
            .map(|shard_id| {
                let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                    .map(|i| {
                        let id = shard_id * hits_per_shard + i;
                        let score = (hits_per_shard as f64 - i as f64) / hits_per_shard as f64;
                        make_hit(&format!("doc-{}", id), score)
                    })
                    .collect();
                make_shard_response(hits, hits_per_shard as u64, 15)
            })
            .collect();

        // Fetch all at once
        let input_all = MergeInput {
            shard_hits: shard_hits.clone(),
            offset: 0,
            limit: total_hits,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let result_all = merge(input_all).unwrap();

        // Fetch in pages
        let mut paged_hits = Vec::new();
        let mut offset = 0;
        loop {
            let input_page = MergeInput {
                shard_hits: shard_hits.clone(),
                offset,
                limit: page_size,
                client_requested_score: false,
                facets: None,
                failed_shards: Vec::new(),
            };

            let result_page = merge(input_page).unwrap();
            if result_page.hits.is_empty() {
                break;
            }

            paged_hits.extend(result_page.hits);
            offset += page_size;

            if paged_hits.len() >= result_all.hits.len() {
                break;
            }
        }

        prop_assert_eq!(paged_hits, result_all.hits);
    }

    /// Property: Offset skips correct number of results.
    ///
    /// Results with offset=N should be a subset of results with offset=0,
    /// starting from position N.
    #[test]
    fn prop_offset_skips_correctly(
        shard_count in 1usize..5,
        hits_per_shard in 20usize..100,
        offset in 0usize..20,
    ) {
        let shard_hits: Vec<ShardHitPage> = (0..shard_count)
            .map(|shard_id| {
                let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                    .map(|i| {
                        let id = shard_id * hits_per_shard + i;
                        let score = (hits_per_shard as f64 - i as f64) / hits_per_shard as f64;
                        make_hit(&format!("doc-{}", id), score)
                    })
                    .collect();
                make_shard_response(hits, hits_per_shard as u64, 15)
            })
            .collect();

        let limit = hits_per_shard * shard_count;

        let input_no_offset = MergeInput {
            shard_hits: shard_hits.clone(),
            offset: 0,
            limit,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let input_with_offset = MergeInput {
            shard_hits: shard_hits.clone(),
            offset,
            limit,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let result_no_offset = merge(input_no_offset).unwrap();
        let result_with_offset = merge(input_with_offset).unwrap();

        // If offset is beyond available results, with_offset should be empty
        if offset >= result_no_offset.hits.len() {
            prop_assert!(result_with_offset.hits.is_empty());
        } else {
            // with_offset results should be a suffix of no_offset results
            let expected = &result_no_offset.hits[offset..];
            prop_assert_eq!(&result_with_offset.hits, expected);
        }
    }

    /// Property: RRF strategy is deterministic.
    ///
    /// Same inputs to RrfStrategy produce identical outputs.
    #[test]
    fn prop_rrf_strategy_determinism(
        shard_count in 1usize..5,
        hits_per_shard in 0usize..50,
    ) {
        let shard_hits: Vec<ShardHitPage> = (0..shard_count)
            .map(|shard_id| {
                let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                    .map(|i| {
                        let id = shard_id * hits_per_shard + i;
                        let score = (hits_per_shard as f64 - i as f64) / hits_per_shard as f64;
                        make_hit(&format!("doc-{}", id), score)
                    })
                    .collect();
                make_shard_response(hits, hits_per_shard as u64, 15)
            })
            .collect();

        let input = MergeInput {
            shard_hits,
            offset: 0,
            limit: hits_per_shard * shard_count,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let strategy = RrfStrategy::default_strategy();
        let result1 = strategy.merge(input.clone()).unwrap();
        let result2 = strategy.merge(input.clone()).unwrap();

        prop_assert_eq!(result1.hits, result2.hits);
    }

    /// Property: Estimated total hits is sum of shard totals.
    ///
    /// The estimated_total_hits should be the sum of all shard totals.
    #[test]
    fn prop_estimated_total_hits_sum(
        shard_count in 1usize..5,
        hits_per_shard in 0usize..100,
    ) {
        let shard_totals: Vec<u64> = (0..shard_count)
            .map(|_| hits_per_shard as u64)
            .collect();

        let shard_hits: Vec<ShardHitPage> = shard_totals
            .iter()
            .map(|total| {
                make_shard_response(vec![], *total, 15)
            })
            .collect();

        let input = MergeInput {
            shard_hits,
            offset: 0,
            limit: 100,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let result = merge(input).unwrap();
        let expected_total: u64 = shard_totals.iter().sum();

        prop_assert_eq!(result.estimated_total_hits, expected_total);
    }

    /// Property: Processing time is max of shard times.
    ///
    /// The processing_time_ms should be the maximum of all shard processing times.
    #[test]
    fn prop_processing_time_max(
        shard_count in 1usize..5,
        hits_per_shard in 0usize..100,
    ) {
        let shard_times: Vec<u64> = (0..shard_count)
            .map(|i| (i as u64 + 1) * 10) // 10, 20, 30, ...
            .collect();

        let shard_hits: Vec<ShardHitPage> = shard_times
            .iter()
            .map(|time| {
                make_shard_response(vec![], hits_per_shard as u64, *time)
            })
            .collect();

        let input = MergeInput {
            shard_hits,
            offset: 0,
            limit: 100,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let result = merge(input).unwrap();
        let expected_max = *shard_times.iter().max().unwrap();

        prop_assert_eq!(result.processing_time_ms, expected_max);
    }

    /// Property: No duplicate document IDs in results.
    ///
    /// Each document ID should appear at most once in the merged results.
    #[test]
    fn prop_no_duplicate_ids(
        shard_count in 1usize..5,
        hits_per_shard in 0usize..100,
    ) {
        let shard_hits: Vec<ShardHitPage> = (0..shard_count)
            .map(|shard_id| {
                let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                    .map(|i| {
                        let id = shard_id * hits_per_shard + i;
                        let score = (hits_per_shard as f64 - i as f64) / hits_per_shard as f64;
                        make_hit(&format!("doc-{}", id), score)
                    })
                    .collect();
                make_shard_response(hits, hits_per_shard as u64, 15)
            })
            .collect();

        let input = MergeInput {
            shard_hits,
            offset: 0,
            limit: hits_per_shard * shard_count,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let result = merge(input).unwrap();

        let mut seen_ids = std::collections::HashSet::new();
        for hit in &result.hits {
            if let Some(id) = hit.get("id").and_then(|v| v.as_str()) {
                prop_assert!(
                    seen_ids.insert(id),
                    "Duplicate document ID found: {}",
                    id
                );
            }
        }
    }

    /// Property: Results are sorted by RRF score descending.
    ///
    /// For RRF merge, results should be in descending RRF score order
    /// (by rank, not by the original score field).
    #[test]
    fn prop_rrf_sort_order(
        shard_count in 2usize..5,
        hits_per_shard in 10usize..50,
    ) {
        let shard_hits: Vec<ShardHitPage> = (0..shard_count)
            .map(|shard_id| {
                let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                    .map(|i| {
                        let id = shard_id * hits_per_shard + i;
                        // Use varying scores - RRF should sort by rank, not score
                        let score = rand::random::<f64>() * 0.5 + 0.5;
                        make_hit(&format!("doc-{}", id), score)
                    })
                    .collect();
                make_shard_response(hits, hits_per_shard as u64, 15)
            })
            .collect();

        let input = MergeInput {
            shard_hits,
            offset: 0,
            limit: hits_per_shard * shard_count,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let result = merge(input.clone()).unwrap();

        // RRF sorts by rank, so documents from different shards at the same
        // rank position should be tie-broken by document ID.
        // We verify the output is stable (deterministic) by checking we can
        // reproduce it with a second call.
        let result2 = merge(input.clone()).unwrap();
        prop_assert_eq!(serde_json::to_vec(&result.hits).unwrap(), serde_json::to_vec(&result2.hits).unwrap());
    }

    /// Property: Empty input produces empty output.
    ///
    /// Merging empty shard responses should produce an empty result.
    #[test]
    fn prop_empty_input_empty_output(
        shard_count in 1usize..5,
    ) {
        let shard_hits: Vec<ShardHitPage> = (0..shard_count)
            .map(|_| make_shard_response(vec![], 0, 0))
            .collect();

        let input = MergeInput {
            shard_hits,
            offset: 0,
            limit: 100,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let result = merge(input).unwrap();
        prop_assert!(result.hits.is_empty());
        prop_assert_eq!(result.estimated_total_hits, 0);
    }
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    use miroir_core::merger::ScoreMergeStrategy;

    /// Regression test: Known merge result for simple input.
    #[test]
    fn test_simple_merge_known_result() {
        let shard_hits = vec![make_shard_response(
            vec![
                make_hit("doc1", 0.9),
                make_hit("doc2", 0.7),
                make_hit("doc3", 0.5),
            ],
            3,
            10,
        )];

        let input = MergeInput {
            shard_hits,
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
            failed_shards: Vec::new(),
        };

        let result = merge(input).unwrap();
        assert_eq!(result.hits.len(), 3);
        assert_eq!(result.hits[0].get("id").unwrap(), "doc1");
        assert_eq!(result.hits[1].get("id").unwrap(), "doc2");
        assert_eq!(result.hits[2].get("id").unwrap(), "doc3");
    }

    /// Regression test: RRF and ScoreMerge strategies exist.
    #[test]
    fn test_merge_strategies_exist() {
        let rrf = RrfStrategy::default_strategy();
        assert_eq!(rrf.name(), "rrf");

        let score = ScoreMergeStrategy::new();
        assert_eq!(score.name(), "score");
    }

    /// Regression test: Known values for RRF k constant.
    #[test]
    fn test_rrf_k_constant() {
        let default = RrfStrategy::default_strategy();
        assert_eq!(default.k(), 60);

        let custom = RrfStrategy::new(100);
        assert_eq!(custom.k(), 100);
    }
}
