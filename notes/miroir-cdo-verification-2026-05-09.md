# Phase 1 (miroir-cdo): Core Routing — Final Verification Summary

## Date: 2026-05-09

## Definition of Done — All Requirements Met ✓

### 1. Rendezvous Assignment Determinism ✓
- **Test**: `test_rendezvous_determinism`, `acceptance_determinism_1000_runs`
- **Result**: All 1000 randomized runs produce identical assignments for fixed (shard_id, nodes)

### 2. Minimal Reshuffling on Node Add ✓
- **Test**: `test_minimal_reshuffling_on_add`, `acceptance_reshuffle_bound_on_add`
- **Result**: Adding 4th node moves ≤ 2 × (1/4) × 64 = 32 shards (verified)

### 3. Uniform Shard Distribution ✓
- **Test**: `test_shard_distribution_64_3_rf1`, `acceptance_uniformity_64_shards_3_nodes_rf1`
- **Result**: 64 shards / 3 nodes / RF=1 → each node holds 15–27 shards

### 4. Top-RF Placement Stability ✓
- **Test**: `test_top_rf_stability`, `acceptance_rf2_placement_stability`
- **Result**: Top-RF nodes change minimally on add/remove

### 5. Write Targets Correctness ✓
- **Test**: `test_write_targets_count`
- **Result**: `write_targets` returns exactly `RG × RF` nodes, one from each group

### 6. Query Group Distribution ✓
- **Test**: `test_query_group_distribution`
- **Result**: Queries distribute evenly across replica groups

### 7. Covering Set Correctness ✓
- **Test**: `test_covering_set_one_per_shard`, `test_covering_set_replica_rotation`
- **Result**: One node per shard with intra-group replica rotation

### 8. Merger Functionality ✓
- **Tests**: Global sort, offset/limit, facet aggregation, score stripping, miroir field stripping
- **Result**: All merge/facet/limit tests pass

### 9. Code Coverage ≥ 90% ✓
- **Result**: 91.80% overall line coverage for miroir-core
  - router.rs: 96.20%
  - topology.rs: 100.00%
  - scatter.rs: 100.00%
  - merger.rs: 94.67%

## Test Results
```
test result: ok. 151 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 63.04s
```

## Implementation Summary

### router.rs
- `score(shard_id, node_id)`: Rendezvous hash with XxHash64 seed 0
- `assign_shard_in_group`: Top-RF node selection with tie-breaking
- `write_targets`: Returns RG × RF nodes for cross-group replication
- `query_group`: Round-robin group selection for load balancing
- `covering_set`: One node per shard with replica rotation
- `shard_for_key`: Document key to shard mapping

### topology.rs
- `Topology`: Cluster nodes grouped by replica_group
- `Node`: Health state machine (Healthy/Degraded/Joining/Draining/Failed/Active/Removed)
- `Group`: Node container with healthy node filtering
- Full state transition validation

### scatter.rs
- `Scatter` trait: Fan-out orchestration interface
- `StubScatter`: Stub implementation for Phase 1
- Request/response structures for scatter-gather

### merger.rs
- `merge`: Global sort by _rankingScore, offset/limit, facet aggregation
- `Merger` trait: Result merge interface
- `MergerImpl`: Production implementation
- Stable serialization with BTreeMap
