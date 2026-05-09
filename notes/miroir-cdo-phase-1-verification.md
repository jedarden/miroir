# Phase 1 — Core Routing: Verification Complete

## Date
2026-05-09

## Summary
Phase 1 (Core Routing) implementation is complete with all DoD criteria met.

## Files Verified
- `crates/miroir-core/src/router.rs` — Rendezvous hash-based routing and shard assignment
- `crates/miroir-core/src/topology.rs` — Topology management: node registry, groups, and health state
- `crates/miroir-core/src/scatter.rs` — Scatter orchestration (stub implementation per Phase 1 scope)
- `crates/miroir-core/src/merger.rs` — Result merger: combines shard results into a single response

## DoD Criteria Verification

### 1. Rendezvous Assignment Determinism ✅
- **Test**: `test_rendezvous_determinism`
- **Result**: Assignment is deterministic given fixed node list

### 2. Minimal Reshuffling on Node Add ✅
- **Test**: `test_minimal_reshuffling_on_add`
- **Result**: Adding a 4th node in a 3-node group moves at most ~2 × (1/4) of shards

### 3. Shard Distribution (64/3/RF=1) ✅
- **Test**: `test_shard_distribution_64_3_rf1`
- **Result**: Each node holds 18–26 shards

### 4. Top-RF Placement Stability ✅
- **Test**: `test_top_rf_stability`
- **Result**: Top-RF placement changes minimally on add/remove

### 5. Write Targets Count ✅
- **Test**: `test_write_targets_count`
- **Result**: `write_targets` returns exactly `RG × RF` nodes

### 6. Query Group Distribution ✅
- **Test**: `test_query_group_distribution`
- **Result**: `query_group(seq, RG)` distributes evenly across groups

### 7. Covering Set Correctness ✅
- **Tests**: `test_covering_set_one_per_shard`, `test_covering_set_replica_rotation`
- **Result**: `covering_set` returns exactly one node per shard with intra-group replica rotation

### 8. Merger Functionality ✅
- **Tests**: All `merger::tests` including:
  - `test_global_sort_by_ranking_score`
  - `test_offset_and_limit_applied_after_merge`
  - `test_facet_counts_summed_across_shards`
  - `test_estimated_total_hits_summed`
  - `test_ranking_score_stripped_when_not_requested`
  - `test_miroir_shard_always_stripped`
- **Result**: All merge/facet/limit tests pass

### 9. Code Coverage ✅
- **router.rs**: 96.76% line coverage (328/339 lines)
- **topology.rs**: 100% line coverage (142/142 lines)
- **merger.rs**: 95.45% line coverage (357/374 lines)
- **scatter.rs**: 0% (stub implementation per Phase 1 scope)
- **Combined Phase 1 coverage**: 96.7% (exceeds 90% requirement)

## Test Results
All 82 tests pass in ~100 seconds:
```
running 82 tests
test result: ok. 82 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## Key Implementation Details

### Rendezvous Hashing (router.rs)
- Uses `twox_hash::XxHash64` with seed 42 for consistent scoring
- `score(shard_id, node_id)` computes deterministic scores
- `assign_shard_in_group` selects top-RF nodes by score
- Group-scoped assignment prevents same-group replica placement

### Topology (topology.rs)
- `Topology` struct manages nodes grouped by `replica_group`
- `NodeStatus` enum: Healthy, Joining, Draining, Failed
- `Group` represents independent query pools

### Merger (merger.rs)
- Global sort by `_rankingScore`
- Offset/limit applied after merge
- Facet aggregation across shards
- `_miroir_shard` always stripped
- `_rankingScore` conditionally included
- `estimatedTotalHits` summed across shards

## Notes
- Scatter implementation is intentionally stubbed per Phase 1 scope (wired in Phase 2)
- All tests use deterministic fixtures for reproducibility
- Coverage measured with `cargo-llvm-cov`
