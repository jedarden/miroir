# Bead miroir-zc2.4: Score Normalization at Scale — Verification

## Date
2026-05-20

## Summary

Verified that the score normalization research and DFS (global-IDF preflight) implementation from beads miroir-zc2.4, miroir-zfo (RRF validation), and miroir-n6v (DFS implementation) is **complete and passing**.

## Acceptance Criteria Verification

### ✅ Benchmark corpus + query set published

Location: `tests/benches/score-comparability/`
- `corpus/generate.py` — Synthetic corpus generator with intentional shard skew
- `queries/generate.py` — Random query set generator (10K queries across 5 types)
- `simulate.py` — BM25-based score simulation with local/DFS variants
- `results/compare.py` — Kendall tau comparison tool

### ✅ Results reported with confidence intervals

| Metric | Value |
|--------|-------|
| Total queries | 10,000 |
| **Average Kendall τ** | **0.9818** |
| 95% CI | [0.9815, 0.9820] |
| Min τ | 0.9523 |
| Max τ | 1.0000 |
| Queries with τ < 0.95 | **0 (0%)** |
| **Pass criteria (≥ 0.95)** | **✓ PASS** |

Per-query type results:
| Query Type | Avg τ | 95% CI |
|------------|-------|--------|
| multi_term | 0.9956 | [0.9955, 0.9958] |
| common_term | 0.9845 | [0.9842, 0.9848] |
| filtered | 0.9792 | [0.9789, 0.9795] |
| single_term | 0.9774 | [0.9771, 0.9777] |
| rare_term | 0.9666 | [0.9663, 0.9670] |

### ✅ τ ≥ 0.95: Note-of-no-action

The global-IDF preflight (`dfs_query_then_fetch`) achieves τ = 0.9818, well above the 0.95 threshold. **No further action required** — the implementation is correct and performing as expected.

## Conclusion

This bead (miroir-zc2.4) validated the score comparability problem. Follow-up beads implemented and verified the solution:
- **miroir-zfo**: Validated RRF merge (failed catastrophically with τ = 0.14)
- **miroir-n6v**: Implemented global-IDF preflight (succeeds with τ = 0.98)

The research document `docs/research/score-normalization-at-scale.md` contains the full analysis, including:
- Problem statement (local IDF causes score divergence on skewed shards)
- Experimental design (100K docs, 10 shards with 100× skew)
- Results for score merge, RRF merge, and DFS merge
- Implementation details in `crates/miroir-core/src/scatter.rs`
