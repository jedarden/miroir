//! Result merger: combines shard results into a single response.
//!
//! Supports pluggable merge strategies via the [`MergeStrategy`] trait.
//! The default strategy is Reciprocal Rank Fusion (RRF) with k=60.

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

// ---------------------------------------------------------------------------
// Merge strategy trait
// ---------------------------------------------------------------------------

/// Pluggable merge strategy for combining shard results.
///
/// Implementations define how hits from multiple shards are combined
/// into a single globally-ranked response. The default strategy is
/// [`RrfStrategy`] (Reciprocal Rank Fusion).
pub trait MergeStrategy: Send + Sync {
    /// Merge search results from multiple shards into a single response.
    fn merge(&self, input: MergeInput) -> Result<MergedSearchResult>;

    /// Strategy name (for logging and `/explain` output).
    fn name(&self) -> &'static str;
}

/// Box reference to a merge strategy (for polymorphic dispatch).
pub type DynMergeStrategy = dyn MergeStrategy;

// ---------------------------------------------------------------------------
// RRF strategy
// ---------------------------------------------------------------------------

/// Default RRF constant k.
///
/// The value 60 is recommended in the RRF literature and used by
/// OpenSearch for hybrid search. Smaller values amplify rank
/// differences; larger values flatten them.
pub const DEFAULT_RRF_K: u32 = 60;

/// Reciprocal Rank Fusion merge strategy.
///
/// Each document's contribution from a shard is `1 / (k + rank + 1)`
/// where rank is the 0-based position. Documents appearing in
/// multiple shards have their contributions summed. Results are
/// sorted by total RRF score descending, with deterministic
/// tie-breaking on primary key.
#[derive(Debug, Clone)]
pub struct RrfStrategy {
    k: u32,
}

impl RrfStrategy {
    /// Create a new RRF strategy with the given k constant.
    pub fn new(k: u32) -> Self {
        Self { k: k.max(1) }
    }

    /// Create with the default k=60.
    pub fn default_strategy() -> Self {
        Self::new(DEFAULT_RRF_K)
    }

    /// Return the configured k value.
    pub fn k(&self) -> u32 {
        self.k
    }
}

impl Default for RrfStrategy {
    fn default() -> Self {
        Self::default_strategy()
    }
}

impl MergeStrategy for RrfStrategy {
    fn merge(&self, input: MergeInput) -> Result<MergedSearchResult> {
        rrf_merge(&self.k, input)
    }

    fn name(&self) -> &'static str {
        "rrf"
    }
}

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

/// Merge search results using the default RRF strategy (k=60).
///
/// This is a convenience wrapper around [`RrfStrategy`] for callers
/// that don't need to customise the strategy.
pub fn merge(input: MergeInput) -> Result<MergedSearchResult> {
    rrf_merge(&DEFAULT_RRF_K, input)
}

/// Merge search results with a specific strategy.
///
/// Use this when the strategy is selected from config or when you
/// need a non-default RRF k value.
pub fn merge_with_strategy(
    strategy: &dyn MergeStrategy,
    input: MergeInput,
) -> Result<MergedSearchResult> {
    strategy.merge(input)
}

/// Core RRF merge implementation.
fn rrf_merge(k: &u32, input: MergeInput) -> Result<MergedSearchResult> {
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

                    // RRF contribution: 1 / (k + rank + 1)
                    // rank is 0-based, so +1 converts to 1-based position.
                    let rrf_contribution = 1.0 / ((*k as f64) + (rank as f64) + 1.0);

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

/// Score-based merge strategy (OP#4 global-IDF).
///
/// This merge strategy is correct **only when** the preflight phase has
/// provided global IDF so that scores are comparable across shards. It sorts
/// all hits globally by `_rankingScore` descending, with deterministic
/// tie-breaking on primary key.
///
/// Without global IDF, this strategy will produce incorrect rankings because
/// shard-local scores are not comparable across shards with different document
/// distributions.
///
/// Use with [`dfs_query_then_fetch_search`] in the scatter module.
#[derive(Debug, Clone, Copy)]
pub struct ScoreMergeStrategy;

impl ScoreMergeStrategy {
    /// Create a new score-based merge strategy.
    pub fn new() -> Self {
        Self
    }
}

impl Default for ScoreMergeStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl MergeStrategy for ScoreMergeStrategy {
    fn merge(&self, input: MergeInput) -> Result<MergedSearchResult> {
        score_merge(input)
    }

    fn name(&self) -> &'static str {
        "score"
    }
}

/// Core score-based merge implementation (OP#4 global-IDF).
///
/// This merge strategy is correct when the preflight phase has provided
/// global IDF so that scores are comparable across shards. It sorts all
/// hits globally by `_rankingScore` descending, with deterministic tie-breaking
/// on primary key.
///
/// Without global IDF, this strategy will produce incorrect rankings because
/// shard-local scores are not comparable across shards with different document
/// distributions.
fn score_merge(input: MergeInput) -> Result<MergedSearchResult> {
    let mut estimated_total_hits = 0u64;
    let mut max_processing_time = 0u64;
    let mut degraded = false;
    let mut all_hits = Vec::new();

    // Collect all hits from all shards.
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

        // Extract hits.
        if let Some(Value::Array(hits)) = body.get("hits") {
            for hit in hits {
                if let Value::Object(map) = hit {
                    all_hits.push(map.clone());
                }
            }
        }
    }

    // Sort by score descending, then by primary key ascending for tie-breaking.
    all_hits.sort_by(|a, b| {
        let score_a = a.get("_rankingScore")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let score_b = b.get("_rankingScore")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        // Extract primary keys for tie-breaking.
        let pk_a = a.get("id")
            .or_else(|| a.get("pk"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let pk_b = b.get("id")
            .or_else(|| b.get("pk"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Primary sort: score descending (higher score = better rank)
        match score_a.partial_cmp(&score_b) {
            Some(Ordering::Equal) => {
                // Secondary sort: primary key ascending for deterministic tie-breaking
                pk_a.cmp(pk_b)
            }
            Some(ord) => ord.reverse(),
            None => {
                // NaN case: treat as lowest score
                if score_a.is_nan() && !score_b.is_nan() {
                    Ordering::Less
                } else if !score_a.is_nan() && score_b.is_nan() {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
        }
    });

    // Apply offset + limit.
    let paginated_hits: Vec<_> = all_hits
        .into_iter()
        .skip(input.offset)
        .take(input.limit)
        .collect();

    // Strip reserved fields and rebuild hits.
    let mut hits = Vec::with_capacity(paginated_hits.len());
    for mut hit in paginated_hits {
        // Strip _rankingScore if not requested.
        if !input.client_requested_score {
            hit.remove("_rankingScore");
        }

        // Always strip _miroir_* fields.
        hit.retain(|k, _| !k.starts_with("_miroir_"));

        hits.push(Value::Object(hit));
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

    // -----------------------------------------------------------------------
    // Trait / strategy tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_rrf_strategy_default_matches_free_function() {
        let input = MergeInput {
            shard_hits: vec![make_shard_response(
                vec![make_hit("doc1", 0.9, 0), make_hit("doc2", 0.7, 0)],
                100,
                15,
            )],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let strategy = RrfStrategy::default_strategy();
        let via_trait = strategy.merge(input.clone()).unwrap();
        let via_free = merge(input).unwrap();

        assert_eq!(via_trait.hits, via_free.hits);
        assert_eq!(via_trait.estimated_total_hits, via_free.estimated_total_hits);
    }

    #[test]
    fn test_rrf_strategy_custom_k() {
        // With k=1, rank 0 gets 1/(1+0+1) = 0.5
        // With k=60, rank 0 gets 1/(60+0+1) ≈ 0.0164
        // Both should produce the same ordering for a single shard.
        let input = MergeInput {
            shard_hits: vec![make_shard_response(
                vec![make_hit("a", 0.9, 0), make_hit("b", 0.5, 0)],
                50,
                10,
            )],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let strategy_k1 = RrfStrategy::new(1);
        let result = strategy_k1.merge(input).unwrap();
        assert_eq!(result.hits[0].get("id").unwrap(), "a");
        assert_eq!(result.hits[1].get("id").unwrap(), "b");
    }

    #[test]
    fn test_merge_with_strategy_dispatches() {
        let input = MergeInput {
            shard_hits: vec![make_shard_response(
                vec![make_hit("doc1", 0.9, 0)],
                50,
                10,
            )],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let strategy = RrfStrategy::default_strategy();
        let result = merge_with_strategy(&strategy, input).unwrap();
        assert_eq!(result.hits.len(), 1);
        assert_eq!(strategy.name(), "rrf");
    }

    #[test]
    fn test_rrf_strategy_k_clamped_to_one() {
        let strategy = RrfStrategy::new(0);
        assert_eq!(strategy.k(), 1);
    }

    // -----------------------------------------------------------------------
    // Core merge tests
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // RRF correctness properties
    // -----------------------------------------------------------------------

    #[test]
    fn test_rrf_cross_shard_replication_boost() {
        // Key RRF property: a document appearing in multiple shards
        // gets a higher merged score than a single-shard document,
        // even if it ranks lower within each shard.
        //
        // doc-replicated: rank 5 in shard0, rank 5 in shard1
        //   RRF = 1/(60+5+1) + 1/(60+5+1) = 2/66 ≈ 0.0303
        // doc-single: rank 0 in shard2
        //   RRF = 1/(60+0+1) = 1/61 ≈ 0.0164
        //
        // doc-replicated should rank higher.

        let mut shard0 = vec![];
        let mut shard1 = vec![];
        for i in 0..5 {
            shard0.push(make_hit(&format!("filler0-{}", i), 0.5, 0));
            shard1.push(make_hit(&format!("filler1-{}", i), 0.5, 1));
        }
        shard0.push(make_hit("doc-replicated", 0.1, 0));
        shard1.push(make_hit("doc-replicated", 0.1, 1));

        let shard2 = vec![make_hit("doc-single", 0.99, 2)];

        let strategy = RrfStrategy::default_strategy();
        let result = strategy
            .merge(MergeInput {
                shard_hits: vec![
                    make_shard_response(shard0, 100, 10),
                    make_shard_response(shard1, 100, 10),
                    make_shard_response(shard2, 100, 10),
                ],
                offset: 0,
                limit: 20,
                client_requested_score: false,
                facets: None,
            })
            .unwrap();

        let ids: Vec<_> = result.hits.iter().filter_map(|h| h.get("id").and_then(|v| v.as_str())).collect();
        let rep_pos = ids.iter().position(|&id| id == "doc-replicated").unwrap();
        let single_pos = ids.iter().position(|&id| id == "doc-single").unwrap();
        assert!(
            rep_pos < single_pos,
            "Replicated doc at pos {} should rank above single doc at pos {}",
            rep_pos,
            single_pos
        );
    }

    #[test]
    fn test_rrf_immune_to_score_scale() {
        // RRF uses rank only: scores of 0.001 vs 0.999 don't affect ordering.
        // Two shards with wildly different score ranges should produce the
        // same merge as two shards with uniform scores.
        let shard_a = ShardHitPage {
            body: json!({
                "hits": [
                    {"id": "a1", "_rankingScore": 0.001},
                    {"id": "a2", "_rankingScore": 0.002},
                ],
                "estimatedTotalHits": 2,
                "processingTimeMs": 5,
            }),
        };
        let shard_b = ShardHitPage {
            body: json!({
                "hits": [
                    {"id": "b1", "_rankingScore": 0.999},
                    {"id": "b2", "_rankingScore": 0.998},
                ],
                "estimatedTotalHits": 2,
                "processingTimeMs": 5,
            }),
        };

        let strategy = RrfStrategy::default_strategy();
        let result = strategy
            .merge(MergeInput {
                shard_hits: vec![shard_a, shard_b],
                offset: 0,
                limit: 10,
                client_requested_score: false,
                facets: None,
            })
            .unwrap();

        // All at rank 0 or 1 — tie-break alphabetically within each rank tier.
        // rank 0: a1, b1 → sort by id → a1, b1
        // rank 1: a2, b2 → sort by id → a2, b2
        let ids: Vec<_> = result.hits.iter().filter_map(|h| h.get("id").and_then(|v| v.as_str())).collect();
        assert_eq!(ids, vec!["a1", "b1", "a2", "b2"]);
    }

    #[test]
    fn test_rrf_deterministic_with_same_input() {
        // RRF merge is a pure function: same input always produces same output.
        let shard = make_shard_response(
            (0..100).map(|i| make_hit(&format!("doc{}", i), (100 - i) as f64 / 100.0, 0)).collect(),
            1000,
            10,
        );
        let input = MergeInput {
            shard_hits: vec![shard; 5],
            offset: 0,
            limit: 50,
            client_requested_score: false,
            facets: None,
        };

        let strategy = RrfStrategy::default_strategy();
        let r1 = strategy.merge(input.clone()).unwrap();
        let r2 = strategy.merge(input).unwrap();

        assert_eq!(
            serde_json::to_vec(&r1).unwrap(),
            serde_json::to_vec(&r2).unwrap()
        );
    }

    #[test]
    fn test_rrf_default_impl() {
        let via_default: RrfStrategy = Default::default();
        let via_constructor = RrfStrategy::default_strategy();
        assert_eq!(via_default.k(), via_constructor.k());
    }

    #[test]
    fn test_merge_pk_field_as_primary_key() {
        let shard = ShardHitPage {
            body: json!({
                "hits": [{"pk": "doc-pk", "title": "Test"}],
                "estimatedTotalHits": 1,
                "processingTimeMs": 5,
            }),
        };
        let input = MergeInput {
            shard_hits: vec![shard],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };
        let result = merge(input).unwrap();
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].get("pk").unwrap(), "doc-pk");
    }

    #[test]
    fn test_rrf_document_equality() {
        use super::Ordering;
        let a = super::RRFDocument {
            rrf_score: 1.0,
            primary_key: "doc1".into(),
            hit: Map::new(),
        };
        let b = super::RRFDocument {
            rrf_score: 1.0,
            primary_key: "doc1".into(),
            hit: Map::new(),
        };
        assert_eq!(a, b);

        let c = super::RRFDocument {
            rrf_score: 1.0,
            primary_key: "doc2".into(),
            hit: Map::new(),
        };
        assert_ne!(a, c);

        // NaN: both NaN → Equal
        let nan_a = super::RRFDocument {
            rrf_score: f64::NAN,
            primary_key: "x".into(),
            hit: Map::new(),
        };
        let nan_b = super::RRFDocument {
            rrf_score: f64::NAN,
            primary_key: "x".into(),
            hit: Map::new(),
        };
        assert_eq!(nan_a.cmp(&nan_b), Ordering::Equal);

        // NaN vs real: NaN is Less
        let real = super::RRFDocument {
            rrf_score: 1.0,
            primary_key: "x".into(),
            hit: Map::new(),
        };
        assert_eq!(nan_a.cmp(&real), Ordering::Less);
        assert_eq!(real.cmp(&nan_a), Ordering::Greater);
    }

    // -----------------------------------------------------------------------
    // Score-based merge tests (OP#4 global-IDF)
    // -----------------------------------------------------------------------

    #[test]
    fn test_score_merge_strategy_exists() {
        let strategy = ScoreMergeStrategy::new();
        assert_eq!(strategy.name(), "score");
    }

    #[test]
    fn test_score_merge_basic() {
        let strategy = ScoreMergeStrategy::new();
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

        let result = strategy.merge(input).unwrap();
        assert_eq!(result.hits.len(), 2);
        assert_eq!(result.hits[0].get("id").unwrap(), "doc1");
        assert_eq!(result.hits[1].get("id").unwrap(), "doc2");
        assert_eq!(result.estimated_total_hits, 100);
    }

    #[test]
    fn test_score_merge_global_sorting() {
        // Test that score-based merge sorts globally by score.
        // With global IDF (simulated here by consistent scores across shards),
        // doc with highest score should rank first regardless of shard.
        let strategy = ScoreMergeStrategy::new();
        let input = MergeInput {
            shard_hits: vec![
                // Shard 0: low scores
                make_shard_response(
                    vec![
                        make_hit("doc-low-1", 0.3, 0),
                        make_hit("doc-low-2", 0.2, 0),
                    ],
                    50,
                    10,
                ),
                // Shard 1: high scores (these should rank higher)
                make_shard_response(
                    vec![
                        make_hit("doc-high-1", 0.9, 1),
                        make_hit("doc-high-2", 0.8, 1),
                    ],
                    50,
                    10,
                ),
            ],
            offset: 0,
            limit: 10,
            client_requested_score: true,
            facets: None,
        };

        let result = strategy.merge(input).unwrap();
        assert_eq!(result.hits.len(), 4);

        // Should be sorted by score descending globally
        assert_eq!(result.hits[0].get("id").unwrap(), "doc-high-1");
        assert_eq!(result.hits[1].get("id").unwrap(), "doc-high-2");
        assert_eq!(result.hits[2].get("id").unwrap(), "doc-low-1");
        assert_eq!(result.hits[3].get("id").unwrap(), "doc-low-2");
    }

    #[test]
    fn test_score_merge_tie_breaking() {
        // Test deterministic tie-breaking on primary key when scores are equal.
        let strategy = ScoreMergeStrategy::new();
        let input = MergeInput {
            shard_hits: vec![
                make_shard_response(
                    vec![
                        make_hit("zebra", 0.5, 0),
                        make_hit("apple", 0.5, 0),
                    ],
                    50,
                    10,
                ),
            ],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result = strategy.merge(input).unwrap();
        // Both docs have same score, should tie-break alphabetically
        assert_eq!(result.hits[0].get("id").unwrap(), "apple");
        assert_eq!(result.hits[1].get("id").unwrap(), "zebra");
    }

    #[test]
    fn test_score_merge_offset_limit() {
        let strategy = ScoreMergeStrategy::new();
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

        let result = strategy.merge(input).unwrap();
        assert_eq!(result.hits.len(), 2);
        assert_eq!(result.hits[0].get("id").unwrap(), "doc2");
        assert_eq!(result.hits[1].get("id").unwrap(), "doc3");
    }

    #[test]
    fn test_score_merge_preserves_score_when_requested() {
        let strategy = ScoreMergeStrategy::new();
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

        let result = strategy.merge(input).unwrap();
        assert_eq!(
            result.hits[0].get("_rankingScore").unwrap().as_f64(),
            Some(0.9)
        );
    }

    #[test]
    fn test_score_merge_strips_score_when_not_requested() {
        let strategy = ScoreMergeStrategy::new();
        let input = MergeInput {
            shard_hits: vec![make_shard_response(
                vec![make_hit("doc1", 0.9, 0)],
                50,
                10,
            )],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result = strategy.merge(input).unwrap();
        assert!(result.hits[0].get("_rankingScore").is_none());
    }

    /// Integration test: skewed corpus scenario with global-IDF preflight.
    ///
    /// This simulates the scenario described in P12.OP4:
    /// - Shard 0 has 1000 docs, term "rust" appears in 100 (df=100)
    /// - Shard 1 has 100 docs, term "rust" appears in 50 (df=50)
    ///
    /// Without global IDF:
    /// - Local IDF(shard 0) = log((1000-100+0.5)/(100+0.5)+1) ≈ 2.2
    /// - Local IDF(shard 1) = log((100-50+0.5)/(50+0.5)+1) ≈ 0.7
    ///
    /// With global IDF:
    /// - Global N = 1100, global df = 150
    /// - Global IDF = log((1100-150+0.5)/(150+0.5)+1) ≈ 1.8
    ///
    /// A document with tf=3 for "rust" in each shard:
    /// - Shard 0 score (local IDF): ~3 * 2.2 = 6.6
    /// - Shard 1 score (local IDF): ~3 * 0.7 = 2.1
    ///
    /// Without global IDF, shard 0 doc ranks higher despite shard 1 having
    /// much higher term density (50/100 vs 100/1000).
    ///
    /// With global IDF, both shards use IDF=1.8, and the doc with higher
    /// term density (normalized by document length) ranks correctly.
    #[test]
    fn test_score_merge_skewed_corpus_integration() {
        // Simulate global IDF applied (scores are now comparable)
        let strategy = ScoreMergeStrategy::new();

        // Doc in large shard with high term frequency but low density
        let doc_large_shard = json!({
            "id": "doc-large",
            "title": "Rust in Large Shard",
            "_rankingScore": 0.75, // After global-IDF normalization
        });

        // Doc in small shard with lower term frequency but high density
        let doc_small_shard = json!({
            "id": "doc-small",
            "title": "Rust in Small Shard",
            "_rankingScore": 0.85, // After global-IDF normalization
        });

        // With global IDF, the small shard doc should rank higher
        // because its term density is higher (50/100 vs 100/1000)
        let input = MergeInput {
            shard_hits: vec![
                ShardHitPage {
                    body: json!({
                        "hits": [doc_large_shard],
                        "estimatedTotalHits": 1000,
                        "processingTimeMs": 10,
                    }),
                },
                ShardHitPage {
                    body: json!({
                        "hits": [doc_small_shard],
                        "estimatedTotalHits": 100,
                        "processingTimeMs": 5,
                    }),
                },
            ],
            offset: 0,
            limit: 10,
            client_requested_score: true,
            facets: None,
        };

        let result = strategy.merge(input).unwrap();
        assert_eq!(result.hits.len(), 2);

        // Small shard doc should rank first (higher score after global IDF)
        assert_eq!(result.hits[0].get("id").unwrap(), "doc-small");
        assert_eq!(result.hits[1].get("id").unwrap(), "doc-large");

        // Scores should be preserved
        assert_eq!(
            result.hits[0].get("_rankingScore").unwrap().as_f64(),
            Some(0.85)
        );
        assert_eq!(
            result.hits[1].get("_rankingScore").unwrap().as_f64(),
            Some(0.75)
        );
    }

    /// Test that demonstrates the failure mode without global IDF.
    ///
    /// This shows what happens when scores are NOT comparable across shards:
    /// score-based merge produces incorrect rankings, while RRF at least
    /// produces consistent (though not optimal) results.
    #[test]
    fn test_score_merge_without_global_idf_fails() {
        // Simulate the bug: shard-local IDF produces incomparable scores
        let strategy = ScoreMergeStrategy::new();

        // Shard 0: large shard, inflated local IDF (shard has term rarity)
        let doc_shard0 = json!({
            "id": "doc-inflated",
            "title": "Document in Large Shard",
            "_rankingScore": 0.95, // Inflated due to high local IDF
        });

        // Shard 1: small shard, deflated local IDF (term is common here)
        let doc_shard1 = json!({
            "id": "doc-deflated",
            "title": "Document in Small Shard",
            "_rankingScore": 0.60, // Deflated due to low local IDF
        });

        let input = MergeInput {
            shard_hits: vec![
                ShardHitPage {
                    body: json!({
                        "hits": [doc_shard0],
                        "estimatedTotalHits": 10000,
                        "processingTimeMs": 15,
                    }),
                },
                ShardHitPage {
                    body: json!({
                        "hits": [doc_shard1],
                        "estimatedTotalHits": 100,
                        "processingTimeMs": 5,
                    }),
                },
            ],
            offset: 0,
            limit: 10,
            client_requested_score: true,
            facets: None,
        };

        let result = strategy.merge(input).unwrap();

        // Without global IDF, score-based merge trusts the inflated scores
        assert_eq!(result.hits[0].get("id").unwrap(), "doc-inflated");

        // This is WRONG: doc-deflated has much higher term density but
        // ranks lower due to shard-local IDF skew.
        //
        // The solution is the preflight phase (dfs_query_then_fetch_search)
        // which computes global IDF so scores are comparable.
    }

    #[test]
    fn test_score_merge_default_impl() {
        let via_default: ScoreMergeStrategy = Default::default();
        let via_constructor = ScoreMergeStrategy::new();
        assert_eq!(via_default.name(), via_constructor.name());
    }

    #[test]
    fn test_score_merge_empty_input() {
        let strategy = ScoreMergeStrategy::new();
        let input = MergeInput {
            shard_hits: vec![],
            offset: 0,
            limit: 10,
            client_requested_score: false,
            facets: None,
        };

        let result = strategy.merge(input).unwrap();
        assert_eq!(result.hits.len(), 0);
        assert_eq!(result.estimated_total_hits, 0);
    }

    // -----------------------------------------------------------------------
    // P12.OP4 RRF skew validation
    // -----------------------------------------------------------------------

    /// Validates the P12.OP4 finding: RRF merge with extreme shard skew
    /// produces incorrect global rankings because it gives equal weight
    /// to all shards regardless of their size.
    ///
    /// Scenario: 10 shards where shard 0 has 93K docs (93%) and shard 9
    /// has 10 docs (0.01%). RRF assigns identical scores to rank-0 hits
    /// from all shards, so a mediocre hit from the tiny shard ranks
    /// equally with the best hit from the dominant shard.
    ///
    /// Benchmark result (10K queries, skewed corpus):
    ///   Score merge: τ = 0.79  (95% CI [0.787, 0.801]) — FAIL
    ///   RRF merge:   τ = 0.14  (95% CI [0.134, 0.140]) — FAIL
    ///
    /// Conclusion: RRF alone does NOT solve cross-shard comparability.
    /// Global-IDF preflight (dfs_query_then_fetch) is required.
    #[test]
    fn test_rrf_skewed_shards_equal_weight_problem() {
        // Shard 0 (dominant): doc-best should be the global #1 result.
        // It has the highest score and appears in the shard with 93% of docs.
        let shard_dominant = make_shard_response(
            vec![
                make_hit("doc-best", 0.95, 0),      // True global #1
                make_hit("doc-good", 0.90, 0),       // True global #2
                make_hit("doc-ok", 0.85, 0),         // True global #3
                make_hit("doc-mediocre", 0.70, 0),   // True global #4
                make_hit("doc-weak", 0.60, 0),       // True global #5
            ],
            93_000,
            10,
        );

        // Shard 9 (tiny, 10 docs): due to local IDF skew, irrelevant docs
        // can appear at rank 0 with inflated local scores.
        let shard_tiny = make_shard_response(
            vec![
                make_hit("doc-irrelevant", 0.98, 9), // Inflated local IDF → high score
                make_hit("doc-noise", 0.92, 9),
            ],
            10,
            2,
        );

        let strategy = RrfStrategy::default_strategy();
        let result = strategy
            .merge(MergeInput {
                shard_hits: vec![shard_dominant, shard_tiny],
                offset: 0,
                limit: 10,
                client_requested_score: true,
                facets: None,
            })
            .unwrap();

        let ids: Vec<_> = result
            .hits
            .iter()
            .filter_map(|h| h.get("id").and_then(|v| v.as_str()))
            .collect();

        // RRF gives equal rank weight to both shards.
        // Rank 0 from dominant shard: 1/61 ≈ 0.0164
        // Rank 0 from tiny shard:     1/61 ≈ 0.0164 (identical!)
        //
        // Tie-breaking falls to primary key (alphabetical), NOT relevance.
        // doc-best and doc-irrelevant both get RRF score 1/61.
        // Alphabetically: doc-best < doc-irrelevant → doc-best wins the tie.
        //
        // But doc-irrelevant still ranks above doc-good, doc-ok, doc-mediocre,
        // and doc-weak — all of which are more relevant globally.
        assert_eq!(ids[0], "doc-best");      // Tie-break win (alphabetical)
        assert_eq!(ids[1], "doc-irrelevant"); // Tie-break loss, but still rank 2!

        // doc-irrelevant (globally irrelevant) ranks ABOVE doc-good (global #2)
        let irrelevant_pos = ids.iter().position(|&id| id == "doc-irrelevant").unwrap();
        let good_pos = ids.iter().position(|&id| id == "doc-good").unwrap();
        assert!(
            irrelevant_pos < good_pos,
            "RRF skew bug: irrelevant doc (pos {}) ranks above doc-good (pos {})",
            irrelevant_pos,
            good_pos,
        );
    }

    /// Computes Kendall tau between two rankings (document ID lists).
    /// Used to validate merge quality against ground truth.
    fn kendall_tau(ranking1: &[String], ranking2: &[String]) -> f64 {
        let pos1: std::collections::HashMap<&str, usize> = ranking1
            .iter()
            .enumerate()
            .map(|(i, id)| (id.as_str(), i))
            .collect();
        let pos2: std::collections::HashMap<&str, usize> = ranking2
            .iter()
            .enumerate()
            .map(|(i, id)| (id.as_str(), i))
            .collect();

        let common: Vec<&str> = pos1
            .keys()
            .filter(|k| pos2.contains_key(*k))
            .map(|k| *k)
            .collect();

        if common.len() < 2 {
            return 0.0;
        }

        let r2_positions: Vec<usize> = common.iter().map(|id| pos2[id]).collect();
        let (_, discordant) = count_inversions(&r2_positions);
        let n = common.len();
        let total = n * (n - 1) / 2;
        let concordant = total - discordant;
        (concordant as f64 - discordant as f64) / total as f64
    }

    fn count_inversions(arr: &[usize]) -> (Vec<usize>, usize) {
        if arr.len() <= 1 {
            return (arr.to_vec(), 0);
        }
        let mid = arr.len() / 2;
        let (left, inv_l) = count_inversions(&arr[..mid]);
        let (right, inv_r) = count_inversions(&arr[mid..]);

        let mut merged = Vec::with_capacity(arr.len());
        let mut inv = inv_l + inv_r;
        let (mut i, mut j) = (0, 0);

        while i < left.len() && j < right.len() {
            if left[i] <= right[j] {
                merged.push(left[i]);
                i += 1;
            } else {
                merged.push(right[j]);
                inv += left.len() - i;
                j += 1;
            }
        }
        merged.extend_from_slice(&left[i..]);
        merged.extend_from_slice(&right[j..]);
        (merged, inv)
    }

    /// End-to-end validation: RRF merge on skewed shards produces τ < 0.95
    /// against ground truth (single-index ranking).
    ///
    /// This is a scaled-down version of the 10K-query Python benchmark.
    #[test]
    fn test_rrf_skewed_shards_tau_below_threshold() {
        let k = DEFAULT_RRF_K;

        // Build 5 shards with skewed sizes: [100, 500, 2000, 5000, 10000]
        // Ground truth: all 17600 docs in one index, sorted by score.
        let mut all_docs: Vec<(String, f64)> = Vec::new();
        let mut shard_docs: Vec<Vec<(String, f64)>> = vec![vec![], vec![], vec![], vec![], vec![]];
        let shard_sizes = [100, 500, 2000, 5000, 10000];

        let mut rng = simple_rng(42);
        for (shard_id, &size) in shard_sizes.iter().enumerate() {
            for i in 0..size {
                // Deterministic pseudo-random scores
                let score = fake_bm25_score(shard_id, i, &mut rng);
                let doc_id = format!("s{}-d{:06}", shard_id, i);
                all_docs.push((doc_id.clone(), score));
                shard_docs[shard_id].push((doc_id, score));
            }
        }

        // Ground truth: global sort by score descending
        all_docs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let ground_truth: Vec<String> = all_docs.iter().take(100).map(|(id, _)| id.clone()).collect();

        // Per-shard: sort locally (simulates local BM25 with local IDF)
        for docs in &mut shard_docs {
            docs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        }

        // RRF merge using the actual Rust merger
        let shard_pages: Vec<ShardHitPage> = shard_docs
            .iter()
            .map(|docs| {
                let hits: Vec<Value> = docs
                    .iter()
                    .take(200)
                    .map(|(id, score)| {
                        json!({
                            "id": id,
                            "_rankingScore": score,
                        })
                    })
                    .collect();
                ShardHitPage {
                    body: json!({
                        "hits": hits,
                        "estimatedTotalHits": docs.len(),
                        "processingTimeMs": 10,
                    }),
                }
            })
            .collect();

        let strategy = RrfStrategy::new(k);
        let result = strategy
            .merge(MergeInput {
                shard_hits: shard_pages,
                offset: 0,
                limit: 100,
                client_requested_score: true,
                facets: None,
            })
            .unwrap();

        let rrf_ranking: Vec<String> = result
            .hits
            .iter()
            .filter_map(|h| h.get("id").and_then(|v| v.as_str()).map(String::from))
            .collect();

        let tau = kendall_tau(&ground_truth, &rrf_ranking);

        // RRF with skewed shards should produce τ well below 0.95.
        assert!(
            tau < 0.95,
            "RRF tau = {:.4}, expected < 0.95 with skewed shards",
            tau,
        );
    }

    /// Simple deterministic PRNG for reproducible test scores.
    fn simple_rng(seed: u64) -> impl FnMut() -> f64 {
        let mut state = seed;
        move || {
            state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            (state >> 33) as f64 / (1u64 << 31) as f64
        }
    }

    /// Simulates a BM25-like score with shard-dependent IDF skew.
    fn fake_bm25_score(shard_id: usize, _doc_idx: usize, rng: &mut impl FnMut() -> f64) -> f64 {
        let tf = 1.0 + rng() * 10.0;
        // Larger shards have lower IDF for common terms (simulating skew)
        let shard_weight = match shard_id {
            0 => 0.3,
            1 => 0.5,
            2 => 0.7,
            3 => 0.9,
            4 => 1.0,
            _ => 0.5,
        };
        tf * shard_weight + rng() * 0.5
    }
}
