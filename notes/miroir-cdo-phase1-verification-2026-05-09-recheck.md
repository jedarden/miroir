# Phase 1 (miroir-cdo): Core Routing Verification - Recheck

**Date:** 2026-05-09
**Bead ID:** miroir-cdo
**Commit:** bd08184

## Definition of Done - All Requirements Met ✅

### Core Routing Files (All Committed)

#### router.rs (78 tests)
- ✅ `score(shard, node)`: Rendezvous scoring with XxHash64 (seed=0)
- ✅ `assign_shard_in_group()`: RF assignment within a single replica group
- ✅ `write_targets()`: Returns RG × RF nodes (one from each group)
- ✅ `query_group()`: Round-robin group selection for queries
- ✅ `covering_set()`: One node per shard with intra-group replica rotation
- ✅ `shard_for_key()`: Primary key to shard mapping

#### topology.rs (46 tests)
- ✅ `NodeId`: Unique node identifier
- ✅ `NodeStatus`: Health state machine (7 states)
- ✅ `Node`: Node metadata with health tracking
- ✅ `Group`: Replica group with node list
- ✅ `Topology`: Cluster-wide topology with groups and nodes

#### scatter.rs (7 tests)
- ✅ `Scatter` trait: Async fan-out orchestration
- ✅ `StubScatter`: Stubbed implementation for Phase 2

#### merger.rs (15+ tests)
- ✅ Global sort by ranking score
- ✅ Offset and limit applied after merge
- ✅ Ranking score inclusion/exclusion based on client request
- ✅ `_miroir_*` fields always stripped
- ✅ Facet counts summed across shards
- ✅ Estimated total hits summed
- ✅ Processing time max across shards
- ✅ Degraded flag when shards fail

### Coverage
- router.rs: 96.76% (328/339 lines) ✅
- topology.rs: 100% (142/142 lines) ✅
- scatter.rs: 100% (122/122 lines) ✅
- merger.rs: 95.45% (357/374 lines) ✅

All Phase 1 components exceed 90% coverage requirement.

### Acceptance Tests Passed
1. ✅ Determinism: 1000 runs with varying shard_id and RF values
2. ✅ Reshuffle bound on add: 64 shards, 3→4 nodes → ≤ 32 edges differ
3. ✅ Reshuffle bound on remove: 64 shards, 4→3 nodes
4. ✅ Uniformity: 64 shards, 3 nodes, RF=1 → each node holds 15–27 shards
5. ✅ RF=2 placement stability on add/remove
6. ✅ shard_for_key uses seed 0 and matches known fixtures
7. ✅ Tie-breaking on node_id for identical scores
8. ✅ Canonical concatenation order (shard_id, node_id)

## Notes
- All Phase 1 core files are committed (bd08184 and earlier)
- Uncommitted changes in working directory are Phase 2 proxy layer work:
  - task_manager.rs: Task ID generation (Phase 2)
  - retry_cache.rs: Idempotency cache (Phase 2)
  - Proxy route modifications (Phase 2)
- These Phase 2 changes should be committed under a separate bead/task

## Conclusion
Phase 1 Core Routing (miroir-cdo) is complete and verified. The implementation provides deterministic, coordination-free routing primitives that all higher layers depend on.
