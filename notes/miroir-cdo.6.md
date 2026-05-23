# P1.6 Property + Benchmark Tests Verification

## Summary
Verified that all property tests and criterion benchmarks for the router are in place and functional.

## Acceptance Criteria Met

### 1. Criterion Benchmarks ✓
- `cargo bench -p miroir-core --bench router_bench` runs successfully
- 9 benchmarks covering:
  - `shard_for_key_single`: 28 ns avg
  - `shard_for_key_10k_docs`: 268 µs avg
  - `assign_shard_in_group_single`: 114 ns avg
  - `assign_shard_in_group_64_shards`: 12.2 µs avg
  - `full_routing_10k_docs`: 272 µs avg (well under 1ms target)
  - Varying shard counts: 8, 16, 32, 64, 128, 256
  - Varying node counts: 2, 3, 4, 5, 8, 10
  - Varying replication factors: 1, 2, 3, 5
  - `score_single`: 23 ns avg

### 2. Property Tests ✓
- `cargo test -p miroir-core --test router_proptest` runs successfully
- 12 property tests covering:
  - Determinism (single and multiple runs)
  - shard_for_key determinism and valid range
  - Minimal reshuffling bounds on add/remove
  - Uniformity across nodes
  - Return count validation (exactly RF nodes)
  - Node from input validation
  - No duplicates validation
  - Score function differentiation
- `proptest.toml` configured for 1024 cases per property

### 3. CI Integration ✓
- `k8s/argo-workflows/miroir-ci.yaml` line 124 includes `cargo bench --no-run`
- Benchmarks compile on every CI build

## Files Verified
- `crates/miroir-core/benches/router_bench.rs` - Criterion benchmarks
- `crates/miroir-core/tests/router_proptest.rs` - Proptest property tests
- `crates/miroir-core/src/router.rs` - Router implementation with tests
- `proptest.toml` - Proptest configuration (1024 cases)
