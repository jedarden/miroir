# P1.6 Property + Benchmark Tests Verification

## Summary

Verified that all P1.6 acceptance criteria were already implemented in the codebase.

## Acceptance Criteria Verification

### 1. `cargo bench -p miroir-core` runs all criterion benches and reports timing ✓

**Files:**
- `crates/miroir-core/benches/router_bench.rs` - Rendezvous assignment benchmarks targeting §8 goals
- `crates/miroir-core/benches/merger_bench.rs` - Merger benchmarks (1000 hits, 3 shards)

**Benchmarks include:**
- `shard_for_key` single and batch (10K docs)
- `assign_shard_in_group` single and all shards (64)
- Full routing pipeline (10K docs through hash → shard → assign)
- Varying shard counts (8, 16, 32, 64, 128, 256)
- Varying node counts (2, 3, 4, 5, 8, 10)
- Varying replication factors (1, 2, 3, 5)
- Merger with varying hit counts (100, 500, 1000, 5000, 10000)
- Merger with varying shard counts (1, 2, 3, 5, 10)
- Pagination, facets, score preservation, degraded responses

### 2. `cargo test -p miroir-core` runs property tests with 1024 cases per property ✓

**File:** `crates/miroir-core/tests/router_proptest.rs`

**Property tests (all with 1024 cases via proptest.toml):**
- Determinism (single and multiple runs)
- Minimal reshuffling bounds on node add/remove
- Uniformity (shards distribute evenly across nodes)
- `assign_shard_in_group` returns exactly rf nodes
- All returned nodes are from input set
- No duplicate nodes in assignment
- Score function uses different values for different inputs
- `shard_for_key` determinism and valid range

**Configuration:** `proptest.toml` sets `cases = 1024` for both default and CI profiles.

### 3. Phase 8 CI includes `cargo bench --no-run` to compile benches on every build ✓

**File:** `k8s/argo-workflows/miroir-ci.yaml` line 124

```yaml
# Plan §8 Phase 1: compile benchmarks on every build (without running)
cargo bench --no-run
```

## Test Results

All 12 proptest properties pass:
- prop_assign_no_duplicates
- prop_assign_nodes_from_input
- prop_assign_returns_rf_nodes
- prop_determinism
- prop_determinism_multiple_runs
- prop_reshuffle_bound_on_add
- prop_reshuffle_bound_on_remove
- prop_score_different_inputs
- prop_shard_for_key_determinism
- prop_shard_for_key_valid_range
- prop_uniformity
- regression_tests::test_shard_for_key_known_values

All benchmarks compile successfully.
