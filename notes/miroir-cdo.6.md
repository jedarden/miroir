# P1.6: Property + Benchmark Tests for Router - Verification Notes

## Summary

Verified that all property tests and benchmarks for the router module are fully implemented and meet plan §8 requirements.

## Files Verified

### Property Tests (`crates/miroir-core/tests/router_proptest.rs`)
12 proptest properties covering:
- **Determinism**: Same inputs produce same outputs across runs
- **Minimal reshuffling bounds**: Node add/remove moves minimal data
- **Uniformity**: Shards distribute evenly across nodes
- **Valid range**: shard_for_key always returns valid shard IDs
- **No duplicates**: assign_shard_in_group returns unique nodes
- **Node membership**: All returned nodes are from input set

Configuration: `proptest.toml` sets `cases = 1024`

### Benchmarks (`crates/miroir-core/benches/`)

#### router_bench.rs
- `shard_for_key_single` - Single document routing
- `shard_for_key_10k_docs` - Batch document routing
- `assign_shard_in_group_single` - Single shard assignment
- `assign_shard_in_group_64_shards` - All shards assignment
- `full_routing_10k_docs` - **Primary target**: 64 shards, 3 nodes, 10K docs
- `varying_shard_count` - 8 to 256 shards
- `varying_node_count` - 2 to 10 nodes
- `varying_rf` - RF 1 to 5
- `score_single` - Score function benchmark

#### merger_bench.rs
- `merge_1000_hits_3_shards` - **Primary target**: 1000 hits from 3 shards
- `varying_hit_count` - 100 to 10000 hits
- `varying_shard_count` - 1 to 10 shards
- `pagination` - Various offset/limit combinations
- `with_facets` - Facet distribution merge
- `with_score` - Score preservation
- `degraded` - Failed shard handling

## Performance Results (2026-05-23)

```
full_routing_10k_docs      time: [276.27 µs 279.66 µs 283.60 µs]
merge_1000_hits_3_shards   time: [751.82 µs 813.50 µs 884.89 µs]
```

Both benchmarks meet plan §8 targets (< 1 ms).

## Test Results

```
running 12 tests
test prop_assign_no_duplicates ... ok
test prop_assign_nodes_from_input ... ok
test prop_assign_returns_rf_nodes ... ok
test prop_determinism ... ok
test prop_determinism_multiple_runs ... ok
test prop_reshuffle_bound_on_add ... ok
test prop_reshuffle_bound_on_remove ... ok
test prop_score_different_inputs ... ok
test prop_shard_for_key_determinism ... ok
test prop_shard_for_key_valid_range ... ok
test regression_tests::test_shard_for_key_known_values ... ok
test prop_uniformity ... ok

test result: ok. 12 passed; 0 failed; 0 ignored
```

## External Dependencies

Phase 8 CI configuration (`cargo bench --no-run`) must be added to the external Argo WorkflowTemplates in `jedarden/declarative-config`.
