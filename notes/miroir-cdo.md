# Phase 1 (miroir-cdo): Core Routing — Final Verification Summary

## Date
2026-05-09

## Definition of Done Verification

All acceptance criteria from plan §8 have been verified:

### Router Correctness Tests (router.rs)

- ✅ **Rendezvous determinism**: Same (shard_id, nodes) → identical Vec<NodeId> across 1000 randomized runs
  - Test: `acceptance_determinism_1000_runs`
  
- ✅ **Minimal reshuffling on add**: 64 shards, 3→4 nodes → at most 2 × (1/4) × 64 edges differ
  - Test: `acceptance_reshuffle_bound_on_add`
  
- ✅ **Minimal reshuffling on remove**: 64 shards, 4→3 nodes → ~RF × S / Ng edges differ
  - Test: `acceptance_reshuffle_bound_on_remove`
  
- ✅ **Uniform distribution**: 64 shards, 3 nodes, RF=1 → each node holds 18–26 shards
  - Test: `acceptance_uniformity_64_shards_3_nodes_rf1`
  
- ✅ **RF=2 placement stability**: Top-2 nodes change minimally on add/remove
  - Test: `acceptance_rf2_placement_stability`
  
- ✅ **write_targets returns RG × RF nodes**: Exactly one node from each replica group
  - Test: `test_write_targets_count`
  
- ✅ **query_group distributes evenly**: Round-robin distributes queries uniformly
  - Test: `test_query_group_distribution`
  
- ✅ **covering_set returns one node per shard**: With intra-group replica rotation
  - Tests: `test_covering_set_one_per_shard`, `test_covering_set_replica_rotation`
  
- ✅ **shard_for_key uses seed 0**: Matches known fixture values
  - Test: `acceptance_shard_for_key_fixture`

### Result Merger Tests (merger.rs)

- ✅ **Global sort by _rankingScore**: Descending order with tie-breaking
  - Test: `test_global_sort_by_ranking_score`
  
- ✅ **Offset and limit applied after merge**: Pagination works correctly
  - Test: `test_offset_and_limit_applied_after_merge`
  
- ✅ **_rankingScore stripping**: Removed when not requested by client
  - Tests: `test_ranking_score_stripped_when_not_requested`, `test_ranking_score_included_when_requested`
  
- ✅ **_miroir_shard always stripped**: Reserved fields removed
  - Test: `test_strip_all_miroir_reserved_fields`
  
- ✅ **Facet aggregation**: Counts summed across shards
  - Test: `test_facet_counts_summed_across_shards`
  
- ✅ **estimatedTotalHits summed**: Across all shards
  - Test: `test_estimated_total_hits_summed`
  
- ✅ **processingTimeMs max**: Slowest shard time reported
  - Test: `test_processing_time_max_across_shards`

### Coverage

- ✅ **miroir-core ≥ 90% line coverage**: 91.80% overall (via cargo-llvm-cov)
  - router.rs: 96.20%
  - topology.rs: 100.00%
  - scatter.rs: 100.00%
  - merger.rs: 94.67%

## Test Results

All 151 tests pass successfully:
```
test result: ok. 151 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## Implementation Summary

The Phase 1 core routing implementation provides:

1. **Rendezvous hashing (HRW)** with twox-hash and seed 0 to match Meilisearch Enterprise
2. **Deterministic shard assignment** with minimal reshuffling on topology changes
3. **Group-scoped assignment** preventing both replicas from landing in the same group
4. **Write target calculation** returning exactly RG × RF nodes
5. **Query distribution** via round-robin group selection
6. **Covering set calculation** with intra-group replica rotation
7. **Result merging** with global sort, facet aggregation, and reserved field stripping

## Critical Implementation Details

1. **Hash seed**: Uses seed 0 (XxHash64::with_seed(0)) to match Meilisearch Enterprise
2. **Canonical order**: (shard_id, node_id) - this ordering is critical for consistency
3. **Tie-breaking**: Lexicographic by node_id when hash scores collide
4. **Group isolation**: Hashing is scoped to intra-group node lists

## Final Verification (2026-05-09)

### Latest Test Results
All 169 tests pass successfully (97.61s execution time):
```
test result: ok. 169 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

### Latest Coverage Report (lcov)
| File | Coverage | Lines |
|------|----------|-------|
| router.rs | 96% | 481/500 |
| topology.rs | 100% | 421/421 |
| scatter.rs | 100% | 121/121 |
| merger.rs | 94% | 551/582 |
| **Overall** | **96%** | **1574/1624** |

## Status

Phase 1 (miroir-cdo) Core Routing is **COMPLETE** and verified.

### Ready for Phase 2
All core routing primitives are implemented, tested, and verified. The foundation is in place for:
- §2 write path (write_targets)
- §2 read path (query_group, covering_set)
- §4 rebalancer (minimal reshuffling property)
- §13.3 adaptive selection (covering_set)
- §13.4 query planner (covering_set, merge)
- §13.8 anti-entropy (deterministic assignment)
- §14.5 Mode A shard-partitioned ownership (group-scoped assignment)
