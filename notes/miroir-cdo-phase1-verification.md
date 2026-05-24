# Phase 1 (miroir-cdo): Core Routing Verification Summary

## Definition of Done Status

All Phase 1 Core Routing requirements have been verified complete:

### Router Tests (router.rs) - All Pass ✅
1. ✅ Rendezvous assignment is deterministic given fixed node list (`test_rendezvous_determinism`)
2. ✅ Adding a 4th node in a 3-node group moves at most ~2 × (1/4) of shards (`test_minimal_reshuffling_on_add`)
3. ✅ 64 shards / 3 nodes / RF=1 → each node holds 18–26 shards (`test_shard_distribution_64_3_rf1`)
4. ✅ Top-RF placement changes minimally on add / remove (`test_top_rf_stability`)
5. ✅ `write_targets` returns exactly `RG × RF` nodes (`test_write_targets_count`)
6. ✅ `query_group(seq, RG)` distributes evenly (`test_query_group_distribution`)
7. ✅ `covering_set` within a group returns exactly one node per shard (`test_covering_set_one_per_shard`)
8. ✅ `covering_set` handles intra-group replica rotation (`test_covering_set_replica_rotation`)

### Merger Tests (merger.rs) - All Pass ✅
- Global sort by ranking score ✅
- Offset and limit applied after merge ✅
- Ranking score stripped when not requested ✅
- Ranking score included when requested ✅
- `_miroir_shard` always stripped ✅
- Facet counts summed across shards ✅
- Estimated total hits summed ✅
- Processing time max across shards ✅
- Degraded flag when shard fails ✅
- Facet keys unique to one shard preserved ✅
- Missing facet distribution handled gracefully ✅

### Coverage (via lcov.info) ✅
- **router.rs**: 95.4% (357/374 lines)
- **topology.rs**: 100% (142/142 lines)
- **scatter.rs**: 100% (122/122 lines)
- **merger.rs**: 96.8% (328/339 lines)

All core routing files meet the ≥ 90% coverage requirement.

## Implementation Details

### router.rs
- `score(shard, node)`: Rendezvous scoring with XxHash64 (seed=42)
- `assign_shard_in_group()`: RF assignment within a single replica group
- `write_targets()`: Returns RG × RF nodes (one from each group)
- `query_group()`: Round-robin group selection for queries
- `covering_set()`: One node per shard with intra-group replica rotation
- `shard_for_key()`: Primary key to shard mapping

### topology.rs
- `NodeId`: Unique node identifier
- `NodeStatus`: Health state machine (Healthy/Degraded/Draining/Failed/Joining/Active/Removed)
- `Node`: Node metadata with health tracking
- `Group`: Replica group with node list
- `Topology`: Cluster-wide topology with groups and nodes

### scatter.rs
- `Scatter` trait: Async fan-out orchestration
- `StubScatter`: Stubbed implementation (wired in Phase 2)

### merger.rs
- `Merger` trait: Result merge interface
- `MergerImpl`: Full implementation with global sort, facet aggregation, offset/limit
- `StubMerger`: Empty result stub

## Test Results
- 105/106 tests pass in miroir-core
- 1 unrelated test failure: `config::load::tests::test_from_yaml_valid_config` (config validation issue, not routing)
- All 23 router/topology/scatter/merger tests pass
