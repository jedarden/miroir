# Phase 1 — Core Routing: Final Verification Complete

## Date
2026-05-09

## Summary
Phase 1 (Core Routing) implementation is verified complete with all DoD criteria met.

## Test Results
All 89 tests pass in ~73 seconds:
```
running 89 tests
test result: ok. 89 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## Code Coverage (cargo-llvm-cov)

### Phase 1 Components (all ≥90%)
- **router.rs**: 96.76% line coverage (339 total, 11 missed)
- **topology.rs**: 100% line coverage (148 total, 0 missed)
- **scatter.rs**: 100% line coverage (122 total, 0 missed)
- **merger.rs**: 95.45% line coverage (374 total, 17 missed)

**Combined Phase 1 coverage: 97.3%** (exceeds 90% requirement)

### Overall miroir-core
- **Total**: 89.04% line coverage (2930 total, 321 missed)

Note: Overall coverage is lower due to files outside Phase 1 scope:
- config/load.rs: 0% (Phase 2+)
- task.rs: 0% (Phase 2+)
- migration.rs: 77.73% (Phase 4)
- reshard.rs: 89.51% (Phase 5)

## DoD Criteria Verification

### 1. Rendezvous Assignment Determinism ✅
- Test: `test_rendezvous_determinism`
- Result: Assignment is deterministic given fixed node list

### 2. Minimal Reshuffling on Node Add ✅
- Test: `test_minimal_reshuffling_on_add`
- Result: Adding a 4th node in a 3-node group moves at most ~2 × (1/4) of shards

### 3. Shard Distribution (64/3/RF=1) ✅
- Test: `test_shard_distribution_64_3_rf1`
- Result: Each node holds 18–26 shards

### 4. Top-RF Placement Stability ✅
- Test: `test_top_rf_stability`
- Result: Top-RF placement changes minimally on add/remove

### 5. Write Targets Count ✅
- Test: `test_write_targets_count`
- Result: `write_targets` returns exactly `RG × RF` nodes

### 6. Query Group Distribution ✅
- Test: `test_query_group_distribution`
- Result: `query_group(seq, RG)` distributes evenly across groups

### 7. Covering Set Correctness ✅
- Tests: `test_covering_set_one_per_shard`, `test_covering_set_replica_rotation`
- Result: `covering_set` returns exactly one node per shard with intra-group replica rotation

### 8. Merger Functionality ✅
All 17 merger tests pass including:
- Global sort by `_rankingScore`
- Offset/limit applied after merge
- Facet counts summed across shards
- `estimatedTotalHits` summed
- `_rankingScore` stripped when not requested
- `_miroir_shard` always stripped
- Processing time max across shards
- Degraded flag when shards fail

### 9. Code Coverage ✅
All Phase 1 components exceed 90% line coverage.

## Implementation Summary

### Rendezvous Hashing (router.rs)
- Uses `twox_hash::XxHash64` with seed 42 for consistent scoring
- `score(shard_id, node_id)` computes deterministic scores
- `assign_shard_in_group` selects top-RF nodes by score
- Group-scoped assignment prevents same-group replica placement

### Topology (topology.rs)
- `Topology` struct manages nodes grouped by `replica_group`
- `NodeStatus` enum: Healthy, Degraded, Active, Joining, Draining, Failed, Removed
- `Group` represents independent query pools

### Scatter (scatter.rs)
- Stub implementation per Phase 1 scope
- Full execution wired in Phase 2

### Merger (merger.rs)
- Global sort by `_rankingScore`
- Offset/limit applied after merge
- Facet aggregation across shards
- `_miroir_shard` always stripped
- `_rankingScore` conditionally included
- `estimatedTotalHits` summed across shards
