//! Score comparability analysis: validates cross-shard ranking consistency.
//!
//! Implements Plan §15 OP#4: statistical validation that `_rankingScore`
//! values remain comparable across shards with very different document-count
//! distributions. Uses Kendall Tau correlation to measure ranking similarity
//! between sharded and ground-truth (single-index) result orderings.

use serde::{Deserialize, Serialize};

/// Parameters for a score comparability simulation run.
#[derive(Debug, Clone)]
pub struct SimParams {
    /// Total number of documents in the corpus.
    pub total_docs: u64,

    /// Number of shards to distribute documents across.
    pub shard_count: u32,

    /// Skew factor: multiplier for document distribution imbalance.
    /// 1.0 = uniform distribution. Higher values create more extreme skew.
    /// Example: 100.0 means one shard gets 100× the median count.
    pub skew_factor: f64,

    /// Number of queries to run against the corpus.
    pub num_queries: usize,

    /// Number of top results to compare per query (K).
    pub top_k: usize,

    /// Random seed for reproducibility.
    pub seed: u64,
}

/// Result of a single query comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Query identifier.
    pub query_id: usize,

    /// Number of hits returned by ground truth (single-index).
    pub ground_truth_hits: usize,

    /// Number of hits returned by sharded execution.
    pub sharded_hits: usize,

    /// Kendall Tau correlation between result orderings.
    pub kendall_tau: f64,

    /// Jaccard similarity of the top-K result sets (ignoring order).
    pub jaccard_similarity: f64,

    /// Position of the first divergent result (0 if identical, top_k+1 if completely different).
    pub first_divergence_position: usize,

    /// Score statistics per shard.
    pub shard_score_stats: Vec<ShardScoreStats>,
}

/// Score statistics for a single shard on a single query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardScoreStats {
    /// Shard identifier.
    pub shard_id: u32,

    /// Number of documents on this shard.
    pub doc_count: u64,

    /// Number of hits returned by this shard.
    pub hit_count: usize,

    /// Minimum score returned.
    pub min_score: f64,

    /// Maximum score returned.
    pub max_score: f64,

    /// Mean score of returned hits.
    pub mean_score: f64,

    /// Score range (max - min).
    pub score_range: f64,
}

/// Aggregate results across all queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateResult {
    /// Mean Kendall Tau across all queries.
    pub mean_kendall_tau: f64,

    /// Standard deviation of Kendall Tau.
    pub std_kendall_tau: f64,

    /// Minimum Kendall Tau observed.
    pub min_kendall_tau: f64,

    /// Maximum Kendall Tau observed.
    pub max_kendall_tau: f64,

    /// Percentage of queries with τ ≥ 0.95.
    pub percent_above_threshold: f64,

    /// Mean Jaccard similarity.
    pub mean_jaccard: f64,

    /// Mean first divergence position.
    pub mean_first_divergence: usize,

    /// Shard population statistics (CV of document counts).
    pub shard_pop_cv: f64,

    /// Shard population ratio (max/median).
    pub shard_pop_ratio: f64,
}

/// Simulated score comparability test result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimResult {
    pub params: SimParams,
    pub aggregate: AggregateResult,
    pub query_results: Vec<QueryResult>,
    pub shard_doc_counts: Vec<u64>,
}

/// Run a score comparability simulation.
///
/// This models a simplified ranking system where:
/// - Documents have relevance scores based on term frequency
/// - Shards compute scores locally using their own document statistics
/// - A ground-truth single-index uses global statistics
///
/// The simulation measures how well the sharded ordering matches the
/// ground-truth ordering using Kendall Tau correlation.
pub fn simulate(params: &SimParams) -> SimResult {
    use rand::rngs::StdRng;
    use rand::Rng;
    use rand::SeedableRng;

    let mut rng = StdRng::seed_from_u64(params.seed);

    // Generate document distribution across shards with specified skew.
    let shard_doc_counts = generate_skewed_distribution(
        params.total_docs,
        params.shard_count,
        params.skew_factor,
        &mut rng,
    );

    // Compute shard population statistics.
    let total_docs = shard_doc_counts.iter().sum::<u64>() as f64;
    let mean_docs = total_docs / params.shard_count as f64;
    let variance = shard_doc_counts
        .iter()
        .map(|&c| (c as f64 - mean_docs).powi(2))
        .sum::<f64>()
        / params.shard_count as f64;
    let cv = variance.sqrt() / mean_docs;
    let median_docs = median(&shard_doc_counts);
    let max_docs = *shard_doc_counts.iter().max().unwrap_or(&1) as f64;
    let pop_ratio = max_docs / median_docs;

    // Run queries and collect results.
    let mut query_results = Vec::with_capacity(params.num_queries);

    for qid in 0..params.num_queries {
        let result = run_query(qid, params, &shard_doc_counts, &mut rng);
        query_results.push(result);
    }

    // Compute aggregate statistics.
    let taus: Vec<f64> = query_results.iter().map(|r| r.kendall_tau).collect();
    let mean_tau = mean(&taus);
    let std_tau = std_dev(&taus, mean_tau);
    let min_tau = taus.iter().cloned().reduce(f64::min).unwrap_or(0.0);
    let max_tau = taus.iter().cloned().reduce(f64::max).unwrap_or(0.0);
    let above_threshold = taus.iter().filter(|&&t| t >= 0.95).count() as f64
        / taus.len() as f64 * 100.0;

    let jaccards: Vec<f64> = query_results.iter().map(|r| r.jaccard_similarity).collect();
    let mean_jaccard = mean(&jaccards);

    let divergences: Vec<usize> = query_results
        .iter()
        .map(|r| r.first_divergence_position)
        .collect();
    let mean_divergence = mean_usize(&divergences);

    let aggregate = AggregateResult {
        mean_kendall_tau: mean_tau,
        std_kendall_tau: std_tau,
        min_kendall_tau: min_tau,
        max_kendall_tau: max_tau,
        percent_above_threshold: above_threshold,
        mean_jaccard: mean_jaccard,
        mean_first_divergence: mean_divergence,
        shard_pop_cv: cv,
        shard_pop_ratio: pop_ratio,
    };

    SimResult {
        params: params.clone(),
        aggregate,
        query_results,
        shard_doc_counts,
    }
}

/// Generate a skewed document distribution across shards.
///
/// Uses a Pareto-like distribution to create realistic skew where
/// a few shards have many more documents than others.
fn generate_skewed_distribution(
    total_docs: u64,
    shard_count: u32,
    skew_factor: f64,
    rng: &mut impl rand::Rng,
) -> Vec<u64> {
    // Start with uniform weights.
    let mut weights: Vec<f64> = (0..shard_count).map(|_| 1.0).collect();

    // Apply skew: scale a few shards by the skew factor.
    // For skew_factor = 100, scale the top 5% of shards.
    let num_skewed = (shard_count as f64 / 20.0).ceil() as usize; // Top 5%
    for i in 0..num_skewed.min(weights.len()) {
        weights[i] *= skew_factor;
    }

    // Add some randomness to make it more realistic.
    for w in &mut weights {
        *w *= rng.gen_range(0.5..1.5);
    }

    // Normalize weights to sum to total_docs.
    let weight_sum: f64 = weights.iter().sum();
    let scale = total_docs as f64 / weight_sum;

    weights
        .into_iter()
        .map(|w| (w * scale).max(1) as u64) // Ensure at least 1 doc per shard
        .collect()
}

/// Run a single query and compare sharded vs ground-truth results.
fn run_query(
    query_id: usize,
    params: &SimParams,
    shard_doc_counts: &[u64],
    rng: &mut impl rand::Rng,
) -> QueryResult {
    // Simulate a query by generating random relevance scores for documents.
    // In a real system, scores would be based on term frequency, IDF, etc.
    // Here we simulate the key effect: smaller shards tend to produce higher
    // scores for the same documents because their local statistics are different.

    let total_docs = params.total_docs as usize;

    // Generate ground-truth scores (global IDF).
    // Each document gets a base relevance score + some noise.
    let mut ground_truth_scores: Vec<(usize, f64)> = (0..total_docs)
        .map(|doc_id| {
            let base_relevance = rng.gen::<f64>(); // Simulated term matching
            let global_idf = 1.0; // Global IDF is uniform
            let score = base_relevance * global_idf;
            (doc_id, score)
        })
        .collect();

    // Sort by score descending and take top-K.
    ground_truth_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    let ground_truth_top: Vec<usize> = ground_truth_scores
        .iter()
        .take(params.top_k)
        .map(|(id, _)| *id)
        .collect();

    // Generate sharded scores (local IDF per shard).
    // Key insight: local IDF is computed from shard-specific document counts.
    // Smaller shards have fewer docs, so their IDF tends to be higher for
    // the same term, leading to score inflation.

    let mut sharded_scores: Vec<(usize, f64)> = Vec::new();

    for (shard_id, &doc_count) in shard_doc_counts.iter().enumerate() {
        let shard_id = shard_id as u32;
        let doc_count_f64 = doc_count as f64;
        let total_docs_f64 = params.total_docs as f64;

        // Simulate local IDF effect: smaller shards → higher local IDF.
        // In real BM25, IDF = log((N - df + 0.5) / (df + 0.5)) where N is doc count.
        // We model this as: local_idf / global_idf ≈ log(N_global) / log(N_shard).
        let global_idf_factor = (total_docs_f64 + 1.0).ln();
        let local_idf_factor = (doc_count_f64 + 1.0).ln();
        let idf_inflation = global_idf_factor / local_idf_factor.max(0.1);

        // Documents on this shard get scores inflated by the local IDF factor.
        let start_doc = shard_doc_counts[..shard_id as usize]
            .iter()
            .sum::<u64>() as usize;
        let end_doc = start_doc + doc_count as usize;

        for doc_id in start_doc..end_doc.min(total_docs) {
            // Reuse the same base relevance as ground truth.
            let base_relevance = ground_truth_scores[doc_id].1;

            // Apply local IDF inflation.
            let score = base_relevance * idf_inflation;

            sharded_scores.push((doc_id, score));
        }
    }

    // Sort sharded scores and take top-K.
    sharded_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    let sharded_top: Vec<usize> = sharded_scores
        .iter()
        .take(params.top_k)
        .map(|(id, _)| *id)
        .collect();

    // Compute Kendall Tau between the two orderings.
    let kendall_tau = kendall_tau(&ground_truth_top, &sharded_top);

    // Compute Jaccard similarity (set overlap, ignoring order).
    let jaccard = jaccard_similarity(&ground_truth_top, &sharded_top);

    // Find first divergence position.
    let first_divergence = ground_truth_top
        .iter()
        .zip(sharded_top.iter())
        .position(|(a, b)| a != b)
        .unwrap_or(params.top_k);

    // Collect per-shard score statistics.
    let shard_stats = collect_shard_stats(shard_id, shard_doc_counts, &sharded_scores, params.top_k);

    QueryResult {
        query_id,
        ground_truth_hits: ground_truth_top.len(),
        sharded_hits: sharded_top.len(),
        kendall_tau,
        jaccard_similarity: jaccard,
        first_divergence_position: first_divergence,
        shard_score_stats: shard_stats,
    }
}

/// Collect score statistics for each shard.
fn collect_shard_stats(
    query_id: usize,
    shard_doc_counts: &[u64],
    sharded_scores: &[(usize, f64)],
    top_k: usize,
) -> Vec<ShardScoreStats> {
    let mut stats = Vec::new();

    // Map doc_id back to its shard.
    for (shard_id, &doc_count) in shard_doc_counts.iter().enumerate() {
        let shard_id = shard_id as u32;
        let start_doc = shard_doc_counts[..shard_id as usize]
            .iter()
            .sum::<u64>() as usize;
        let end_doc = start_doc + doc_count as usize;

        // Find scores from this shard in the top-K results.
        let shard_hits: Vec<&f64> = sharded_scores
            .iter()
            .take(top_k)
            .filter(|(doc_id, _)| *doc_id >= start_doc && *doc_id < end_doc)
            .map(|(_, score)| score)
            .collect();

        if shard_hits.is_empty() {
            continue;
        }

        let min_score = shard_hits.iter().cloned().reduce(f64::min).unwrap_or(0.0);
        let max_score = shard_hits.iter().cloned().reduce(f64::max).unwrap_or(0.0);
        let mean_score = shard_hits.iter().sum::<f64>() / shard_hits.len() as f64;

        stats.push(ShardScoreStats {
            shard_id,
            doc_count,
            hit_count: shard_hits.len(),
            min_score,
            max_score,
            mean_score,
            score_range: max_score - min_score,
        });
    }

    stats
}

/// Compute Kendall Tau correlation between two ranked lists.
///
/// Returns a value in [-1, 1] where:
/// - 1.0 = identical rankings
/// - 0.0 = no correlation
/// - -1.0 = completely inverted rankings
///
/// Uses the O(n²) algorithm which is fine for small K (typically ≤ 100).
fn kendall_tau<T: Eq + std::hash::Hash + std::fmt::Debug>(rank1: &[T], rank2: &[T]) -> f64 {
    if rank1.is_empty() || rank2.is_empty() {
        return 1.0;
    }

    // Create position maps.
    let pos1: std::collections::HashMap<&T, usize> = rank1
        .iter()
        .enumerate()
        .map(|(i, item)| (item, i))
        .collect();
    let pos2: std::collections::HashMap<&T, usize> = rank2
        .iter()
        .enumerate()
        .map(|(i, item)| (item, i))
        .collect();

    // Collect all unique items from both lists.
    let all_items: std::collections::HashSet<&T> =
        rank1.iter().chain(rank2.iter()).collect();

    // Count concordant and discordant pairs.
    let mut concordant = 0;
    let mut discordant = 0;

    let items: Vec<&T> = all_items.into_iter().collect();
    for i in 0..items.len() {
        for j in (i + 1)..items.len() {
            let a = items[i];
            let b = items[j];

            let pos1_a = pos1.get(a);
            let pos1_b = pos1.get(b);
            let pos2_a = pos2.get(a);
            let pos2_b = pos2.get(b);

            // A pair is only counted if both items appear in both lists.
            let (p1a, p1b, p2a, p2b) = match (pos1_a, pos1_b, pos2_a, pos2_b) {
                (Some(&x), Some(&y), Some(&u), Some(&v)) => (x, y, u, v),
                _ => continue,
            };

            if (p1a < p1b && p2a < p2b) || (p1a > p1b && p2a > p2b) {
                concordant += 1;
            } else if (p1a < p1b && p2a > p2b) || (p1a > p1b && p2a < p2b) {
                discordant += 1;
            }
        }
    }

    let total = concordant + discordant;
    if total == 0 {
        return 1.0;
    }

    (concordant - discordant) as f64 / total as f64
}

/// Compute Jaccard similarity between two sets (ignoring order).
fn jaccard_similarity<T: Eq + std::hash::Hash>(set1: &[T], set2: &[T]) -> f64 {
    let set1_hash: std::collections::HashSet<&T> = set1.iter().collect();
    let set2_hash: std::collections::HashSet<&T> = set2.iter().collect();

    let intersection = set1_hash.intersection(&set2_hash).count();
    let union = set1_hash.union(&set2_hash).count();

    if union == 0 {
        return 1.0;
    }

    intersection as f64 / union as f64
}

/// Compute mean of a slice of floats.
fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

/// Compute standard deviation of a slice of floats.
fn std_dev(values: &[f64], mean_val: f64) -> f64 {
    if values.len() <= 1 {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|v| (v - mean_val).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    variance.sqrt()
}

/// Compute mean of a slice of usize.
fn mean_usize(values: &[usize]) -> usize {
    if values.is_empty() {
        return 0;
    }
    values.iter().sum::<usize>() / values.len()
}

/// Compute median of a slice of u64.
fn median(values: &[u64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    let len = sorted.len();
    if len % 2 == 0 {
        (sorted[len / 2 - 1] + sorted[len / 2]) as f64 / 2.0
    } else {
        sorted[len / 2] as f64
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kendall_tau_identical() {
        let a = vec![1, 2, 3, 4, 5];
        let b = vec![1, 2, 3, 4, 5];
        assert!((kendall_tau(&a, &b) - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_kendall_tau_reversed() {
        let a = vec![1, 2, 3, 4, 5];
        let b = vec![5, 4, 3, 2, 1];
        assert!((kendall_tau(&a, &b) - (-1.0)).abs() < 0.001);
    }

    #[test]
    fn test_kendall_tau_partial_overlap() {
        let a = vec![1, 2, 3, 4, 5];
        let b = vec![1, 3, 2, 4, 5]; // Only 2 and 3 are swapped
        // Expected: 1 pair (2,3) is discordant out of 10 total pairs
        // tau = (8 - 2) / 10 = 0.6
        let tau = kendall_tau(&a, &b);
        assert!((tau - 0.6).abs() < 0.01);
    }

    #[test]
    fn test_jaccard_identical() {
        let a = vec![1, 2, 3, 4, 5];
        let b = vec![1, 2, 3, 4, 5];
        assert!((jaccard_similarity(&a, &b) - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_jaccard_no_overlap() {
        let a = vec![1, 2, 3];
        let b = vec![4, 5, 6];
        assert!((jaccard_similarity(&a, &b) - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_jaccard_half_overlap() {
        let a = vec![1, 2, 3, 4];
        let b = vec![3, 4, 5, 6];
        // Intersection: {3, 4}, Union: {1, 2, 3, 4, 5, 6}
        // Jaccard = 2/6 = 1/3
        assert!((jaccard_similarity(&a, &b) - 1.0 / 3.0).abs() < 0.001);
    }

    #[test]
    fn test_skewed_distribution_sum() {
        let counts = generate_skewed_distribution(10000, 10, 10.0, &mut rand::thread_rng());
        let total: u64 = counts.iter().sum();
        assert!((total as i64 - 10000i64).abs() < 100); // Allow small rounding error
    }

    #[test]
    fn test_skewed_distribution_has_skew() {
        let counts = generate_skewed_distribution(10000, 10, 100.0, &mut rand::thread_rng());
        let max = *counts.iter().max().unwrap();
        let min = *counts.iter().min().unwrap();
        // With skew factor 100, we expect significant imbalance.
        assert!(max > min * 10);
    }

    #[test]
    fn test_simulation_runs() {
        let params = SimParams {
            total_docs: 1000,
            shard_count: 4,
            skew_factor: 10.0,
            num_queries: 10,
            top_k: 10,
            seed: 42,
        };
        let result = simulate(&params);
        assert_eq!(result.query_results.len(), 10);
        assert!(result.aggregate.mean_kendall_tau >= -1.0);
        assert!(result.aggregate.mean_kendall_tau <= 1.0);
    }
}
