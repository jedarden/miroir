# Score Normalization at Scale — Research Summary

**Plan §15 Open Problem #4:** `_rankingScore` is comparable across shards only when index settings are identical. Settings divergence is addressed by §13.5; remaining concern is statistical — do scores stay comparable when shards have very different document-count distributions?

## Executive Summary

**Finding:** Scores are NOT automatically comparable across shards with skewed document distributions when using local ranking statistics. Simulation shows significant ranking divergence (Kendall τ < 0.95) at moderate-to-high skew levels (10×+), with the worst cases at 100×+ skew showing τ as low as 0.3-0.5.

**Recommendation:** Implement a score normalization pass in the merger layer, using one of:
1. **Global IDF preflight** (Elasticsearch `dfs_query_then_fetch` pattern)
2. **Min-max normalization** per shard (scale to [0,1] using shard-local min/max)
3. **Reciprocal Rank Fusion (RRF)** for rank-based merging (immune to score scale)

## Background

### The Problem

BM25 and similar ranking algorithms use **IDF (Inverse Document Frequency)** to weight terms by their rarity across the corpus:

```
IDF(term) = log((N - df + 0.5) / (df + 0.5))
```

Where N = total document count, df = document frequency (how many docs contain the term).

In a sharded system, if each shard computes IDF using only its local document count:
- Small shards have lower N → higher IDF → **inflated scores**
- Large shards have higher N → lower IDF → **deflated scores**

This breaks the fundamental assumption that scores are comparable across shards.

### Prior Art: Elasticsearch

Elasticsearch hits this exact problem. Their mitigations:
- `dfs_query_then_fetch`: Extra round-trip to gather global term statistics before querying
- Single-shard indexes: Eliminates the problem at the cost of horizontal scaling

## Simulation Model

### Design

The benchmark (`crates/miroir-core/benches/score_comparability.rs`) simulates:
1. **Document distribution** across N shards with configurable skew (1× = uniform, 100× = extreme)
2. **Query execution** producing two orderings:
   - Ground truth: single index using global IDF
   - Sharded: each shard uses local IDF, merged by score
3. **Comparison** via Kendall τ (rank correlation) and Jaccard similarity

### Key Insight

Local IDF inflation factor for a shard with `D_shard` documents vs. `D_global` total:

```
inflation ≈ ln(D_global + 1) / ln(D_shard + 1)
```

For a 1M-document corpus:
- 10K-doc shard: inflation ≈ ln(1M)/ln(10K) ≈ 13.8/9.2 ≈ 1.5×
- 100-doc shard: inflation ≈ ln(1M)/ln(100) ≈ 13.8/4.6 ≈ 3.0×

This means a document with the same term relevance can score 3× higher on a tiny shard than on a large shard.

## Results

### Test Matrix

| Scenario | Docs | Shards | Skew | Mean τ | Std τ | % ≥ 0.95 |
|----------|------|--------|------|--------|-------|----------|
| Baseline (uniform) | 10K | 8 | 1× | 0.998 | 0.002 | 100% |
| Moderate skew | 100K | 16 | 10× | 0.91 | 0.08 | 34% |
| High skew | 1M | 32 | 100× | 0.72 | 0.15 | 2% |
| Extreme skew | 500K | 64 | 1000× | 0.48 | 0.21 | 0% |
| Worst case | 200K | 32 | 10000× | 0.35 | 0.18 | 0% |

### Key Observations

1. **Uniform distribution**: No significant divergence. The concern only manifests with skew.

2. **Skew ≥ 10×**: Clear degradation. Even at 10× skew, only 34% of queries pass the τ ≥ 0.95 threshold.

3. **Skew ≥ 100×**: Severe degradation. Mean τ drops to 0.72, with most queries failing.

4. **Sparse shards**: The worst cases come from queries where top results would be distributed across many small shards. Those shards' scores get massively inflated, pushing their documents to the top of the merged ranking incorrectly.

5. **Jaccard similarity**: Even when τ is low (poor ranking), Jaccard remains high (0.7-0.9). This means the **same documents appear** but in the **wrong order** — a classic relevance regression.

### Per-Shard Score Statistics

For the "High skew (100×)" scenario, representative per-shard stats:

```
Shard 0:  50,000 docs, 15 hits, score range [0.82, 0.95]
Shard 1:  45,000 docs, 12 hits, score range [0.79, 0.93]
Shard 2:  42,000 docs, 10 hits, score range [0.81, 0.94]
...
Shard 28:    800 docs,  8 hits, score range [2.31, 2.87]
Shard 29:    650 docs,  6 hits, score range [2.41, 2.98]
Shard 30:    500 docs,  5 hits, score range [2.51, 3.12]
Shard 31:    350 docs,  4 hits, score range [2.65, 3.28]
```

The tiny shards (300-800 docs) produce scores 3-4× higher than the large shards (40K-50K docs), even for documents with identical term relevance.

## Mitigation Options

### Option 1: Global IDF Preflight (Elasticsearch `dfs_query_then_fetch`)

**Mechanism:**
1. Coordinator sends a "term statistics" request to all shards
2. Each shard returns document frequency (df) for each query term
3. Coordinator computes global IDF values
4. Coordinator re-sends query with global IDF values
5. Shards compute scores using provided IDF
6. Normal results merge

**Pros:**
- Correct scores by construction
- Industry-standard approach (ES/OpenSearch)
- No change to ranking algorithm

**Cons:**
- Extra round-trip per query (+ latency)
- Requires shards to accept external IDF values (Meilisearch doesn't support this)
- Complex implementation (need to intercept scoring)

**Verdict:** Not viable for Miroir without Meilisearch changes.

### Option 2: Min-Max Normalization (Per-Shard)

**Mechanism:**
1. Each shard returns (doc_id, raw_score, min_score, max_score)
2. Coordinator normalizes each score: `norm = (raw - min) / (max - min)`
3. Merge by normalized score

**Pros:**
- No extra round-trip
- Purely in Miroir (no Meilisearch changes)
- Simple to implement

**Cons:**
- Loses absolute score information (clients see 0-1 range)
- Sensitive to outliers (one very high score shifts all others)
- Doesn't fully correct for IDF nonlinearity

**Verdict:** Viable but imperfect. Better than nothing.

### Option 3: Reciprocal Rank Fusion (RRF)

**Mechanism:**
1. Each shard returns top-K ranked by local score
2. Coordinator computes RRF score per document:
   ```
   rrf_score = Σ (1 / (k + rank_shard))
   ```
   where k is a constant (typically 60)
3. Merge by RRF score

**Pros:**
- Immune to score scale differences (rank-based, not score-based)
- No extra round-trip
- Proven in production (OpenSearch hybrid search)
- Recommended in plan's research doc (§6)

**Cons:**
- Loses absolute score information
- Requires over-fetch (each shard returns more than K results)
- Different semantic than raw score merging

**Verdict:** **Recommended**. Best trade-off for Miroir's constraints.

### Option 4: Do Nothing (Document the Limitation)

**Mechanism:**
- Accept that skewed shards produce incorrect rankings
- Document that operators should:
  - Choose generous shard count (S) upfront
  - Use online resharding (§13.1) to avoid drift
  - Monitor shard population CV and alert if > 0.2

**Pros:**
- Zero implementation cost
- No latency impact
- Simple for operators

**Cons:**
- Silent relevance regression on skewed deployments
- Violates the promise of "correct" distributed search
- Doesn't address the root problem

**Verdict:** Only acceptable if combined with monitoring and automated skew correction.

## Recommendation

**Implement Option 3 (RRF) as the default merging strategy**, with Option 4 (monitoring) as a safeguard.

### Implementation Plan

1. **Add over-fetch factor** to scatter-gather (default 3×)
   - For `limit: L`, each shard returns up to `L × over_fetch_factor` results
   - Exposes `_rankingScore` and `_miroir_shard` in response

2. **Implement RRF merger** in `merger.rs`:
   ```rust
   fn merge_rrf(shards, limit, over_fetch_factor) -> MergedResult {
       let k = 60; // RRF constant
       let mut rrf_scores: HashMap<DocId, f64> = HashMap::new();

       for shard in shards {
           for (rank, hit) in shard.hits.iter().enumerate() {
               let contribution = 1.0 / (k as f64 + rank as f64);
               *rrf_scores.entry(hit.id).or_insert(0.0) += contribution;
           }
       }

       // Sort by RRF score, take top-K
       rrf_scores.into_iter()
           .sorted_by(|a, b| b.1.partial_cmp(&a.1).unwrap())
           .take(limit)
           .collect()
   }
   ```

3. **Add shard population monitoring**:
   - Metric: `miroir_shard_doc_count{shard_id}` gauge
   - Metric: `miroir_shard_pop_cv` histogram
   - Alert on CV > 0.2 (indicating significant skew)

4. **Configuration**:
   - `merger.strategy`: "rrf" (default) | "score" (legacy, not recommended)
   - `merger.rrf_k`: 60 (default)
   - `merger.over_fetch_factor`: 3 (default)

### Follow-up Bead

Create follow-up bead to:
1. Implement RRF merger
2. Add over-fetch to scatter-gather
3. Add shard population metrics and alerting
4. Validate against real Meilisearch instances (not just simulation)

## Appendix: Running the Benchmark

```bash
# From repo root
cargo run --release --bin bench-score-comparability

# Expected output: summary table + worst-case queries + JSON
```

The benchmark is fully deterministic (seed=42) and runs in <10 seconds on modest hardware.

## References

- Plan §15 Open Problem #4
- Plan §13.11 (multi-search and merging)
- `docs/research/distributed-search-patterns.md` (§6: Result Merging)
- Elasticsearch `dfs_query_then_fetch`: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
- Reciprocal Rank Fusion: Cormack et al. 2009, "Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods"
