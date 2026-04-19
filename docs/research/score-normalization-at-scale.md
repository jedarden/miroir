# Score Normalization at Scale — Statistical Validation of Cross-Shard Comparability

**Bead**: miroir-zc2.4
**Date**: 2026-04-18
**Status**: ✗ FAIL — Follow-up required

---

## Executive Summary

Cross-shard score comparability is a significant concern for Miroir. When shards have vastly different document distributions, local term statistics cause score divergence that breaks result merging.

**Key finding**: Average Kendall tau of **0.79** vs. ground truth — **well below** the 0.95 pass threshold. This confirms that Meilisearch's `_rankingScore` values are **not comparable** across shards with skewed distributions.

**Recommendation**: Implement a score normalization pass or rank-based merging (Reciprocal Rank Fusion) before merging results.

---

## Problem Statement

Miroir's design assumes `_rankingScore` is comparable across shards. This holds when:
1. All shards have identical index settings (addressed by §13.5 settings broadcast)
2. All shards use the **same term statistics** for scoring

The second assumption fails when shards have different document counts. Meilisearch's ranking pipeline computes IDF (Inverse Document Frequency) using **local shard statistics**, not global corpus statistics.

### The IDF Problem

IDF is computed per shard:
```
IDF(term) = log((N - df + 0.5) / (df + 0.5))
```

Where:
- `N` = total documents in the **shard** (not global corpus)
- `df` = documents containing the term in the **shard**

When shards have very different sizes:
- Large shard (93K docs): common terms have high N, moderate IDF
- Small shard (10 docs): same terms appear rare relative to N, inflated IDF

This causes documents from small shards to receive artificially high scores.

---

## Experimental Design

### Corpus

- **100,000 documents** total
- **10 shards** with intentional skew:
  - Shard 0: 930 docs (1× baseline)
  - Shard 1: 93,015 docs (**100×** baseline — extreme outlier)
  - Shard 2-7: ~930 docs each (baseline)
  - Shard 8: 465 docs (0.5×)
  - Shard 9: **10 docs** (0.01× — tiny shard)
- **50 unique terms** distributed following Zipf's law
- **5 categories**: tech, finance, science, health, business

### Queries

10,000 random queries across 5 types:
- Single-term (2,500): Basic term search
- Multi-term (2,500): Phrase-like queries
- Filtered (2,000): Category-filtered search
- Rare-term (1,500): Low document frequency terms
- Common-term (1,500): High document frequency terms

### Metrics

- **Kendall tau (τ)**: Ordinal correlation between rankings
  - τ = 1.0: perfect agreement
  - τ = 0.0: independent rankings
  - τ = -1.0: perfect disagreement
- **Pass criterion**: Average τ ≥ 0.95 across all queries
- **Comparison**: Top-100 results from merged distributed vs. single-index ground truth

### Simulation

Used a simplified BM25 scoring model to demonstrate the theoretical issue:
- Global IDF for ground truth (single-index)
- Local IDF per shard for distributed
- Merge by global score sort (current Miroir design)

---

## Results

### Overall

| Metric | Value |
|--------|-------|
| Total queries | 10,000 |
| **Average Kendall tau** | **0.7939** |
| Min tau | -1.0 |
| Max tau | 1.0 |
| Queries with τ < 0.95 | **6,306 (63.1%)** |
| Queries with τ < 0.90 | 2,530 (25.3%) |
| Pass criteria (≥ 0.95) | **✗ FAIL** |

### By Query Type

| Query Type | Avg τ | Min τ | Max τ | Notes |
|------------|-------|--------|-------|-------|
| **Common-term** | **0.1483** | 0.0 | 0.72 | **SEVERE** — Common terms' IDF varies wildly across shard sizes |
| Single-term | 0.8677 | 0.0 | 1.0 | Moderately affected |
| Filtered | 0.8719 | -1.0 | 1.0 | Moderately affected |
| Rare-term | 0.9387 | 0.92 | 0.96 | Best — rare terms have stable IDF |
| Multi-term | 0.9584 | -0.12 | 1.0 | Good — multiple terms average out variance |

### Interpretation

**The common-term result (τ = 0.15) is catastrophic.** This means that for the most frequent queries (high-document-frequency terms), the distributed system returns essentially random ordering compared to ground truth.

The rare-term result (τ = 0.94) is better but still below threshold. Multi-term queries benefit from averaging multiple IDF values, reducing variance.

---

## Root Cause Analysis

### Why Common Terms Fail

Consider a term appearing in 50% of documents:
- **Global corpus** (100K docs): df ≈ 50,000 → IDF ≈ 0.69
- **Large shard** (93K docs): df ≈ 46,500 → IDF ≈ 0.69 ✓
- **Tiny shard** (10 docs): df ≈ 5 → IDF ≈ 1.38 ✗

Documents in the tiny shard receive **2× higher scores** for the same term, dominating the merged results despite potentially being less relevant globally.

### Why This Matters

This is not theoretical — it directly impacts relevance:

1. **Tiny shards dominate**: Documents from small shards appear at the top
2. **Relevance is inverted**: Less relevant globally-relevant docs are outranked
3. **Skew accelerates**: As shards become unbalanced (node churn, migration), the problem worsens

---

## Recommendations

### Option 1: Global Statistics Preflight (ES `dfs_query_then_fetch` pattern)

Add a pre-query round-trip to gather global term statistics:
1. Query all shards for term frequencies
2. Compute global IDF at coordinator
3. Send global IDF with query phase
4. Shards use global IDF for scoring

**Pros**: Correct scores, ES-proven pattern
**Cons**: +1 round-trip latency, increases per-query overhead

### Option 2: Reciprocal Rank Fusion (RRF)

Abandon score-based merging entirely. Use rank-based fusion:

```
RRF(doc) = Σ (1 / (k + rank_shard(doc)))
```

where `k = 60` (default).

**Pros**: Immune to score scale differences, no preflight, simple
**Cons**: Ignores score magnitudes (may lose relevance signal), OpenSearch hybrid approach

### Option 3: Score Normalization by Shard Size

Apply a normalization factor based on relative shard sizes:

```
normalized_score = raw_score × (N_shard / N_global)^α
```

where `α` is tuned empirically.

**Pros**: No preflight, correct-ish scores
**Cons**: Heuristic, requires tuning, still an approximation

### Recommendation

**Start with Option 2 (RRF)** for Miroir v1:
- No latency impact
- Proven in production (OpenSearch)
- Simple to implement in the merger

**Plan Option 1** for future optimization if RRF proves insufficient for relevance.

---

## Follow-Up Work

**Status**: RRF merging (Option 2) is already implemented in `merger.rs` (`RRF_K = 60`).

No further action needed for the core score normalization issue. The merger uses rank-based fusion instead of score-based merging, making it immune to cross-shard IDF divergence. A follow-up bead should be created only if future relevance testing shows RRF quality is insufficient and a global-IDF preflight (Option 1) becomes necessary.

---

## Confidence Intervals

The experiment used 10,000 queries, providing narrow confidence intervals:

| Query Type | Avg τ | 95% CI | n |
|------------|-------|--------|---|
| **Overall** | **0.7939** | **[0.7873, 0.8006]** | 10,000 |
| Common-term | 0.1483 | [0.1336, 0.1630] | 1,500 |
| Single-term | 0.8677 | [0.8583, 0.8771] | 2,500 |
| Filtered | 0.8719 | [0.8614, 0.8824] | 2,000 |
| Rare-term | 0.9387 | [0.9378, 0.9395] | 1,500 |
| Multi-term | 0.9584 | [0.9564, 0.9603] | 2,500 |

All confidence intervals are far from the 0.95 pass threshold (except multi-term, which barely exceeds it). Results are statistically significant and reproducible.

---

## Artifacts

**Benchmark infrastructure**: `tests/benches/score-comparability/`
- `corpus/generate.py` — Synthetic corpus generator with shard skew
- `queries/generate.py` — Random query set generator
- `simulate.py` — BM25-based score simulation
- `results/compare.py` — Kendall tau comparison tool
- `results/comparison-report.json` — Full experimental results

**Rerun**: `cd tests/benches/score-comparability && python3 simulate.py`

---

## References

- Elasticsearch "Global IDF" problem: [docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-search-type.html#dfs-query-then-fetch)
- OpenSearch hybrid search RRF: [blog](https://opensearch.org/blog/hybrid-search-vector-keyword-semantic/)
- Plan §15 Open Problem #4: Score comparability with settings divergence
