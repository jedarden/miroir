# Phase 1 - Core Routing: Completion Summary

## Bead: miroir-cdo

**Status**: ✅ COMPLETE

## Definition of Done Verification

### Router Module (router.rs)
- ✅ **Determinism**: `test_determinism()` verifies same inputs produce same output
- ✅ **Minimal reshuffle on add**: `test_reshuffle_bound_on_add()` verifies adding 4th node to 3-node group moves at most ~2 × (1/4) shards
- ✅ **Uniformity**: `test_uniformity()` verifies 64 shards / 3 nodes / RF=1 → each node holds 17-26 shards
- ✅ **RF=2 placement stability**: `test_rf2_placement_stability()` and `test_reshuffle_bound_on_remove()` verify minimal changes on add/remove
- ✅ **write_targets**: `test_write_targets_returns_rg_x_rf_nodes()` and `test_write_targets_one_per_group()` verify RG × RF nodes, one per group
- ✅ **query_group**: `test_query_group_uniform_distribution()` verifies even distribution (chi-square test)
- ✅ **covering_set**: `test_covering_set_covers_all_shards()`, `test_covering_set_size_bound()`, `test_covering_set_determinism()`, `test_covering_set_rotates_replicas()` verify correct coverage
- ✅ **shard_for_key**: `test_shard_for_key_fixture()` verifies hash-based routing

### Topology Module (topology.rs)
- ✅ **Topology struct**: Holds nodes grouped by `replica_group`
- ✅ **Node health state machine**: Full state transition table (Joining → Active → Draining → Removed, with Degraded/Failed states)
- ✅ **Group isolation**: `each_group_holds_exactly_its_configured_nodes()` verifies groups are independent
- ✅ **YAML serialization**: `deserialize_plan_s4_yaml_example()` verifies plan §4 format

### Scatter Module (scatter.rs)
- ✅ **Fan-out orchestration**: `plan_search_scatter()` builds shard→node mapping
- ✅ **Execution primitives**: `execute_scatter()` fans out to nodes with mock client support
- ✅ **OP#4 Global-IDF preflight**: `execute_preflight()` and `dfs_query_then_fetch_search()` implement the preflight phase
- ✅ **Session pinning**: `plan_search_scatter_for_group()` supports pinned group reads
- ✅ **Settings version floor**: `plan_search_scatter_with_version_floor()` implements plan §13.5

### Merger Module (merger.rs)
- ✅ **RRF merge**: Default strategy with k=60, fully tested
- ✅ **Score-based merge**: For OP#4 global-IDF queries
- ✅ **Global sort by score**: `test_score_merge_global_sorting()`
- ✅ **Offset/limit**: `test_merge_offset_limit()`, `test_page_reconstruction()`
- ✅ **Facet aggregation**: `test_merge_facets()`, `test_merge_facets_filter()`
- ✅ **Field stripping**: `test_merge_strips_miroir_fields()`
- ✅ **Tie-breaking**: Deterministic by primary key
- ✅ **Degraded handling**: `test_merge_degraded_flag()`

## Test Results

```
running 15 tests (router)
test result: ok. 15 passed

running 24 tests (topology)
test result: ok. 24 passed

running 25 tests (scatter)
test result: ok. 25 passed

running 39 tests (merger)
test result: ok. 39 passed

Total: 103 Phase 1 tests, all passing
```

## Implementation Details

### Rendezvous Hashing (HRW)
- Uses `twox_hash::XxHash64` with seed 0 (matches Meilisearch Enterprise)
- Scores are `hash(shard_id, node_id)` → 64-bit integer
- Top-RF nodes selected by score descending, with lexicographic tie-breaking

### Properties Verified
1. **Determinism**: All pods compute identical assignments given fixed topology
2. **Minimal reshuffling**: Adding node moves ~1/(Ng+1) of shards
3. **Group isolation**: Intra-group hashing prevents both replicas in same group

### Dependencies
- `twox-hash` for rendezvous hashing
- `serde` for serialization
- `tokio` for async execution

## Notes

Phase 1 - Core Routing was already fully implemented with comprehensive test coverage. The rendezvous hashing, topology management, scatter orchestration, and result merger primitives are all in place and passing all tests.

The implementation correctly handles:
- Deterministic shard assignment without coordination
- Minimal data movement on topology changes
- Group-scoped assignment for replica isolation
- Global sort with RRF and score-based merge
- OP#4 global-IDF preflight for cross-shard score comparability
