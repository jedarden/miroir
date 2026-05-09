# Phase 1 (miroir-cdo): Core Routing Final Verification

**Date:** 2026-05-09
**Bead ID:** miroir-cdo

## Definition of Done - All Requirements Met ✅

### Router Tests (26 tests pass) ✅
1. ✅ Rendezvous assignment is deterministic given fixed node list
2. ✅ Adding a 4th node in a 3-node group moves at most ~2 × (1/4) of shards
3. ✅ 64 shards / 3 nodes / RF=1 → each node holds 18–26 shards
4. ✅ Top-RF placement changes minimally on add / remove
5. ✅ `write_targets` returns exactly `RG × RF` nodes
6. ✅ `query_group(seq, RG)` distributes evenly
7. ✅ `covering_set` within a group returns exactly one node per shard
8. ✅ `covering_set` handles intra-group replica rotation

### Merger Tests (15 tests pass) ✅
- Global sort by ranking score ✅
- Offset and limit applied after merge ✅
- Ranking score stripped when not requested ✅
- Ranking score included when requested ✅
- `_miroir_shard` always stripped ✅
- Facet counts summed across shards ✅
- Estimated total hits summed ✅
- Processing time max across shards ✅
- Degraded flag when shard fails ✅
- Empty shards handled gracefully ✅
- Offset/limit edge cases ✅

### Topology Tests (39 tests pass) ✅
- Node health state machine (7 states) ✅
- State transitions validated ✅
- Write eligibility rules ✅
- Group operations ✅
- Topology queries ✅

### Scatter Tests (7 tests pass) ✅
- Stub implementation for Phase 2 wiring ✅
- Request/Response structures ✅

### Coverage (via lcov.info) ✅
- **router.rs**: 96.76% (328/339 lines)
- **topology.rs**: 100% (142/142 lines)
- **scatter.rs**: 100% (122/122 lines)
- **merger.rs**: 95.45% (357/374 lines)

All Phase 1 components exceed 90% coverage.

## Implementation Summary

### router.rs
- `score(shard, node)`: Rendezvous scoring with XxHash64 (seed=0)
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
- **Total**: 87 Phase 1 tests (26 router + 15 merger + 7 scatter + 39 topology)
- **Status**: All pass ✅
- **Coverage**: All Phase 1 files exceed 90% ✅

## Conclusion
Phase 1 Core Routing is complete and verified. The implementation provides deterministic, coordination-free routing primitives that all higher layers depend on.
