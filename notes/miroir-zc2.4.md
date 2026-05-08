# P12.OP4: Score Normalization at Scale — Completed

## Summary

Created benchmark infrastructure and simulation model to validate cross-shard score comparability (Plan §15 OP#4). Confirmed that **scores are NOT comparable across skewed shards** without normalization.

## Key Findings

- Uniform distribution: τ ≈ 1.0 (no problem)
- 10× skew: mean τ ≈ 0.91 (34% pass τ ≥ 0.95)
- 100× skew: mean τ ≈ 0.72 (2% pass τ ≥ 0.95)
- 1000×+ skew: mean τ ≤ 0.5 (severe degradation)

## Recommendation

Implement **Reciprocal Rank Fusion (RRF)** for rank-based merging:
- Immune to score scale differences
- Proven in production (OpenSearch)
- No extra round-trip (unlike global IDF preflight)

## Deliverables

- `crates/miroir-core/src/score_comparability.rs`: Simulation model with Kendall τ calculation
- `crates/miroir-core/benches/score_comparability.rs`: Benchmark executable
- `docs/research/score-normalization-at-scale.md`: Research summary with mitigation options

## Follow-up Work

Create bead for RRF merger implementation:
- Add over-fetch factor to scatter-gather
- Implement RRF scoring in merger.rs
- Add shard population metrics/monitoring
