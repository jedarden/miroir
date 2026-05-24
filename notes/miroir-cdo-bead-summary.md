# Phase 1 (miroir-cdo): Core Routing - Bead Summary

**Status**: ✅ COMPLETE

## Overview

Phase 1 Core Routing implements the deterministic, coordination-free routing primitives that form the foundation for all distributed operations in Miroir.

## Implementation Summary

### Core Components

1. **router.rs** (96.20% line coverage)
   - `score()` - Rendezvous hashing with twox-hash (seed 0)
   - `assign_shard_in_group()` - Intra-group shard assignment
   - `write_targets()` - Returns RG × RF nodes for writes
   - `query_group()` - Round-robin group selection
   - `covering_set()` - One node per shard for queries
   - `shard_for_key()` - Document key to shard mapping

2. **topology.rs** (100.00% line coverage)
   - `Topology` - Cluster topology with groups and nodes
   - `Node` - Node representation with health state
   - `NodeStatus` - Health state machine (Joining → Active → Draining → Removed)
   - `Group` - Replica group with node membership

3. **merger.rs** (94.67% line coverage)
   - `merge()` - Global sort by _rankingScore, offset/limit, facet aggregation
   - Binary min-heap for efficient top-k selection
   - BTreeMap for stable facet serialization
   - Reserved field stripping (_miroir_*, _rankingScore)

4. **scatter.rs** (100.00% line coverage)
   - `Scatter` trait - Fan-out orchestration interface
   - `StubScatter` - Stub implementation for Phase 2 wiring

## Test Results

- **Total tests**: 151 passed, 0 failed
- **Line coverage**: 91.80% (exceeds 90% requirement)

### Key Acceptance Tests

1. **Determinism**: 1000 randomized runs, all identical assignments
2. **Minimal reshuffling**: Adding 4th node moves ≤ 50% of shards
3. **Uniform distribution**: 64 shards / 3 nodes → 15-27 shards per node
4. **RF=2 stability**: Top-2 nodes change minimally on topology change
5. **Group isolation**: One node from each replica group for writes
6. **Query distribution**: Round-robin verified across 1000 queries
7. **Covering set**: Exactly one node per shard with replica rotation
8. **Result merging**: Global sort, pagination, facets, stripping all verified

## Commit History

- `963059c` - Phase 1 (miroir-cdo): Core Routing — Final verification complete
- `f513bf0` - Phase 1 (miroir-cdo): Core Routing — Final verification summary
- `acd0f62` - Phase 1 (miroir-cdo): Core Routing - Final verification
- `c2a7660` - Phase 1 (miroir-cdo): Core Routing — Final verification complete

## Key Properties Delivered

1. **Determinism**: All pods compute identical assignments given fixed topology
2. **Minimal reshuffling**: Adding node moves ~1/(Ng+1) of shards
3. **Group isolation**: Replicas never land in same group

## Foundation For

- Phase 2: Write path (scatter to RG × RF nodes)
- Phase 3: Read path (covering set query + merge)
- Phase 4: Rebalancer (minimal migration on topology change)
- Phase 13+: Adaptive selection, query planner, anti-entropy
