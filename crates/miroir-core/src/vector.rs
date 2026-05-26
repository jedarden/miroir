//! Vector and hybrid search sharding (plan §13.12).
//!
//! Handles over-fetching and merging for vector/hybrid search across shards.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Vector search configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchConfig {
    /// Whether vector search is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Over-fetch factor (per-shard limit = requested limit × factor).
    #[serde(default = "default_over_fetch")]
    pub over_fetch_factor: u32,
    /// Merge strategy: "convex" or "rrf".
    #[serde(default = "default_merge_strategy")]
    pub merge_strategy: String,
    /// Default hybrid alpha (for convex combination).
    #[serde(default = "default_alpha")]
    pub hybrid_alpha_default: f64,
    /// RRF constant (for Reciprocal Rank Fusion).
    #[serde(default = "default_rrf_k")]
    pub rrf_k: u32,
}

fn default_true() -> bool {
    true
}
fn default_over_fetch() -> u32 {
    3
}
fn default_merge_strategy() -> String {
    "convex".to_string()
}
fn default_alpha() -> f64 {
    0.5
}
fn default_rrf_k() -> u32 {
    60
}

impl Default for VectorSearchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            over_fetch_factor: default_over_fetch(),
            merge_strategy: default_merge_strategy(),
            hybrid_alpha_default: default_alpha(),
            rrf_k: default_rrf_k(),
        }
    }
}

/// Merge strategy for combining results from multiple shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    /// Convex combination: (1 - α) · bm25 + α · semantic
    Convex,
    /// Reciprocal Rank Fusion
    Rrf,
}

impl MergeStrategy {
    /// Parse from string.
    pub fn parse_strategy(s: &str) -> Option<Self> {
        match s {
            "convex" => Some(MergeStrategy::Convex),
            "rrf" => Some(MergeStrategy::Rrf),
            _ => None,
        }
    }
}

/// A search hit with scores from multiple sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorHit {
    /// Primary key of the document.
    pub pk: String,
    /// BM25 ranking score.
    pub ranking_score: f64,
    /// Semantic score (if present).
    pub semantic_score: Option<f64>,
    /// Combined global score.
    pub combined_score: f64,
    /// Source shard.
    pub shard_id: u32,
}

impl VectorHit {
    /// Create a new hit from BM25 only.
    pub fn bm25_only(pk: String, ranking_score: f64, shard_id: u32) -> Self {
        Self {
            pk,
            ranking_score,
            semantic_score: None,
            combined_score: ranking_score,
            shard_id,
        }
    }

    /// Create a new hybrid hit.
    pub fn hybrid(pk: String, ranking_score: f64, semantic_score: f64, shard_id: u32) -> Self {
        Self {
            pk,
            ranking_score,
            semantic_score: Some(semantic_score),
            combined_score: ranking_score, // Will be recomputed during merge
            shard_id,
        }
    }

    /// Merge with convex combination.
    pub fn merge_convex(&mut self, alpha: f64) {
        if let Some(semantic) = self.semantic_score {
            self.combined_score = (1.0 - alpha) * self.ranking_score + alpha * semantic;
        }
    }

    /// Get RRF score for a given rank.
    pub fn rrf_score(rank: usize, k: u32) -> f64 {
        1.0 / (k as f64 + rank as f64)
    }
}

/// Vector search merger — combines over-fetched results from multiple shards.
#[derive(Debug, Clone)]
pub struct VectorMerger {
    /// Merge strategy.
    strategy: MergeStrategy,
    /// Hybrid alpha (for convex).
    alpha: f64,
    /// RRF constant.
    rrf_k: u32,
}

impl VectorMerger {
    /// Create a new vector merger.
    pub fn new(config: &VectorSearchConfig) -> Self {
        let strategy =
            MergeStrategy::parse_strategy(&config.merge_strategy).unwrap_or(MergeStrategy::Convex);
        Self {
            strategy,
            alpha: config.hybrid_alpha_default,
            rrf_k: config.rrf_k,
        }
    }

    /// Merge hits from multiple shards into a single ranked list.
    ///
    /// Input: Vec of (shard_id, hits from that shard)
    /// Output: Globally ranked hits, truncated to `limit`
    pub fn merge(&self, shard_hits: Vec<(u32, VectorHit)>, limit: usize) -> Vec<VectorHit> {
        match self.strategy {
            MergeStrategy::Convex => self.merge_convex(shard_hits, limit),
            MergeStrategy::Rrf => self.merge_rrf(shard_hits, limit),
        }
    }

    /// Convex combination merge.
    fn merge_convex(&self, mut shard_hits: Vec<(u32, VectorHit)>, limit: usize) -> Vec<VectorHit> {
        // Apply convex combination to each hit
        for (_, hit) in &mut shard_hits {
            hit.merge_convex(self.alpha);
        }

        // Sort by combined score descending
        shard_hits.sort_by(|a, b| {
            b.1.combined_score
                .partial_cmp(&a.1.combined_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Deduplicate by PK (keep highest score)
        let mut deduped = HashMap::new();
        for (_, hit) in shard_hits {
            deduped
                .entry(hit.pk.clone())
                .and_modify(|e: &mut VectorHit| {
                    if hit.combined_score > e.combined_score {
                        *e = hit.clone();
                    }
                })
                .or_insert(hit);
        }

        // Convert back to vec and re-sort
        let mut result: Vec<_> = deduped.into_values().collect();
        result.sort_by(|a, b| {
            b.combined_score
                .partial_cmp(&a.combined_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        result.truncate(limit);
        result
    }

    /// RRF (Reciprocal Rank Fusion) merge.
    fn merge_rrf(&self, shard_hits: Vec<(u32, VectorHit)>, limit: usize) -> Vec<VectorHit> {
        // Group hits by PK and accumulate RRF scores
        let mut rrf_scores: HashMap<String, f64> = HashMap::new();
        let mut hit_data: HashMap<String, VectorHit> = HashMap::new();

        // First, sort each shard's hits by their original ranking score
        let mut per_shard: HashMap<u32, Vec<VectorHit>> = HashMap::new();
        for (shard_id, hit) in shard_hits {
            per_shard.entry(shard_id).or_default().push(hit);
        }

        // Compute RRF scores
        for (_shard_id, mut hits) in per_shard {
            // Sort by ranking_score descending (original per-shard ranking)
            hits.sort_by(|a, b| {
                b.ranking_score
                    .partial_cmp(&a.ranking_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            for (rank, hit) in hits.into_iter().enumerate() {
                let pk = hit.pk.clone();
                let rrf_score = VectorHit::rrf_score(rank, self.rrf_k);
                *rrf_scores.entry(pk.clone()).or_insert(0.0) += rrf_score;

                hit_data.entry(pk).or_insert(hit);
            }
        }

        // Build result with RRF scores
        let mut result: Vec<VectorHit> = hit_data
            .into_iter()
            .map(|(pk, mut hit)| {
                hit.combined_score = *rrf_scores.get(&pk).unwrap_or(&0.0);
                hit
            })
            .collect();

        // Sort by RRF score descending
        result.sort_by(|a, b| {
            b.combined_score
                .partial_cmp(&a.combined_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        result.truncate(limit);
        result
    }

    /// Compute the per-shard limit given a requested limit.
    pub fn per_shard_limit(&self, requested_limit: usize, over_fetch_factor: u32) -> usize {
        requested_limit * over_fetch_factor as usize
    }
}

impl Default for VectorMerger {
    fn default() -> Self {
        Self::new(&VectorSearchConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = VectorSearchConfig::default();
        assert!(config.enabled);
        assert_eq!(config.over_fetch_factor, 3);
        assert_eq!(config.merge_strategy, "convex");
        assert_eq!(config.hybrid_alpha_default, 0.5);
        assert_eq!(config.rrf_k, 60);
    }

    #[test]
    fn test_merge_strategy_from_str() {
        assert_eq!(
            MergeStrategy::parse_strategy("convex"),
            Some(MergeStrategy::Convex)
        );
        assert_eq!(
            MergeStrategy::parse_strategy("rrf"),
            Some(MergeStrategy::Rrf)
        );
        assert_eq!(MergeStrategy::parse_strategy("unknown"), None);
    }

    #[test]
    fn test_vector_hit_bm25_only() {
        let hit = VectorHit::bm25_only("doc1".to_string(), 0.8, 5);
        assert_eq!(hit.pk, "doc1");
        assert_eq!(hit.ranking_score, 0.8);
        assert!(hit.semantic_score.is_none());
        assert_eq!(hit.combined_score, 0.8);
        assert_eq!(hit.shard_id, 5);
    }

    #[test]
    fn test_vector_hit_hybrid() {
        let mut hit = VectorHit::hybrid("doc1".to_string(), 0.6, 0.9, 5);
        assert_eq!(hit.pk, "doc1");
        assert_eq!(hit.ranking_score, 0.6);
        assert_eq!(hit.semantic_score, Some(0.9));
        assert_eq!(hit.shard_id, 5);

        hit.merge_convex(0.5);
        assert!((hit.combined_score - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_rrf_score() {
        let score = VectorHit::rrf_score(0, 60);
        assert!((score - 1.0 / 60.0).abs() < 0.0001);

        let score = VectorHit::rrf_score(10, 60);
        assert!((score - 1.0 / 70.0).abs() < 0.0001);
    }

    #[test]
    fn test_merge_convex_basic() {
        let merger = VectorMerger {
            strategy: MergeStrategy::Convex,
            alpha: 0.5,
            rrf_k: 60,
        };

        let hits = vec![
            (0, VectorHit::hybrid("doc1".to_string(), 0.8, 0.6, 0)),
            (0, VectorHit::hybrid("doc2".to_string(), 0.7, 0.9, 0)),
            (1, VectorHit::hybrid("doc1".to_string(), 0.75, 0.65, 1)),
            (1, VectorHit::hybrid("doc3".to_string(), 0.9, 0.5, 1)),
        ];

        let result = merger.merge_convex(hits, 10);

        // Should deduplicate doc1, keeping the highest combined score
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].pk, "doc2"); // (0.7 + 0.9) / 2 = 0.8 (highest)
                                          // doc1 and doc3 both have score 0.7; order between them is unstable
        let pks: Vec<_> = result.iter().map(|h| h.pk.as_str()).collect();
        assert!(pks[1..].contains(&"doc1"));
        assert!(pks[1..].contains(&"doc3"));
    }

    #[test]
    fn test_merge_rrf_basic() {
        let merger = VectorMerger {
            strategy: MergeStrategy::Rrf,
            alpha: 0.5,
            rrf_k: 60,
        };

        let hits = vec![
            (0, VectorHit::bm25_only("doc1".to_string(), 0.9, 0)),
            (0, VectorHit::bm25_only("doc2".to_string(), 0.8, 0)),
            (0, VectorHit::bm25_only("doc3".to_string(), 0.7, 0)),
            (1, VectorHit::bm25_only("doc2".to_string(), 0.95, 1)),
            (1, VectorHit::bm25_only("doc4".to_string(), 0.85, 1)),
        ];

        let result = merger.merge_rrf(hits, 10);

        // doc2 appears in both shards, gets summed RRF scores
        assert!(result.iter().any(|h| h.pk == "doc2"));
        let doc2 = result.iter().find(|h| h.pk == "doc2").unwrap();
        // Rank 1 in shard 0 (after doc1): 1/61, rank 0 in shard 1: 1/60
        let expected = 1.0 / 61.0 + 1.0 / 60.0;
        assert!((doc2.combined_score - expected).abs() < 0.0001);
    }

    #[test]
    fn test_per_shard_limit() {
        let merger = VectorMerger::default();
        assert_eq!(merger.per_shard_limit(10, 3), 30);
        assert_eq!(merger.per_shard_limit(100, 2), 200);
    }

    #[test]
    fn test_merge_limits_output() {
        let merger = VectorMerger::default();
        let hits: Vec<_> = (0..200)
            .map(|i| {
                (
                    i % 10,
                    VectorHit::bm25_only(format!("doc{}", i), 1.0 - (i as f64) * 0.001, i % 10),
                )
            })
            .collect();

        let result = merger.merge_convex(hits, 50);
        assert_eq!(result.len(), 50);
    }
}
