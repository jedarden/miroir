# Phase 1 — Core Routing Verification Summary

## Task: Implement Core Routing Primitives (rendezvous hash, topology, covering set)

## Date: 2026-05-23

## Status: COMPLETE (All components verified)

## Components Verified

### 1. router.rs (`crates/miroir-core/src/router.rs`)

**Implemented Functions:**
- `score(shard_id, node_id)` - HRW scoring with XxHash64 (seed=0)
- `assign_shard_in_group(shard_id, group_nodes, rf)` - RF nodes per group
- `write_targets(shard_id, topology)` - RG × RF nodes for writes
- `query_group(query_seq, replica_groups)` - Round-robin group selection
- `covering_set(shard_count, group, rf, query_seq)` - One node per shard
- `shard_for_key(primary_key, shard_count)` - Hash key to shard

**Tests Passing (15/15):**
- ✅ test_determinism - Same inputs always produce same output
- ✅ test_reshuffle_bound_on_add - Adding node moves ≤ 2×(1/4) of shards
- ✅ test_reshuffle_bound_on_remove - Remove respects expected bound
- ✅ test_uniformity - 64 shards / 3 nodes / RF=1 → 18-26 shards per node
- ✅ test_rf2_placement_stability - RF=2 placement stable on add/remove
- ✅ test_shard_for_key_fixture - Known test vectors match
- ✅ test_tie_breaking - Deterministic tie-breaking
- ✅ test_score_canonical_order - Hash order verified
- ✅ test_write_targets_returns_rg_x_rf_nodes - Exactly RG × RF nodes
- ✅ test_write_targets_one_per_group - One node per group
- ✅ test_covering_set_covers_all_shards - All shards covered
- ✅ test_covering_set_size_bound - Bounded by Ng
- ✅ test_covering_set_determinism - Identical for identical topologies
- ✅ test_query_group_uniform_distribution - Chi-square test passes
- ✅ test_covering_set_rotates_replicas - Replica rotation by query_seq

### 2. topology.rs (`crates/miroir-core/src/topology.rs`)

**Implemented Types:**
- `NodeId` - Unique node identifier
- `NodeStatus` - Health state machine (7 states, legal transitions)
- `Node` - Meilisearch node with address, status, replica_group
- `Group` - Replica group with node list
- `Topology` - Cluster topology with serialization

**Tests Passing (26/26):**
- ✅ All YAML deserialization tests
- ✅ All group iteration tests
- ✅ All state machine transition tests
- ✅ All write eligibility tests
- ✅ All node lookup and management tests

### 3. merger.rs (`crates/miroir-core/src/merger.rs`)

**Implemented:**
- `MergeStrategy` trait with pluggable strategies
- `RrfStrategy` (k=60 default) - Reciprocal Rank Fusion
- `ScoreMergeStrategy` - Global-IDF score-based merge
- Facet merging with BTreeMap for stable ordering
- Proper handling of offset, limit, _rankingScore, _miroir_* fields

**Tests Passing (39/39):**
- ✅ All RRF merge tests
- ✅ All score-based merge tests
- ✅ All facet tests
- ✅ RRF skew validation (τ < 0.95 with skewed shards)
- ✅ Global-IDF integration tests

### 4. scatter.rs (`crates/miroir-core/src/scatter.rs`)

**Implemented:**
- `ScatterPlan` - Shard→node mapping for queries
- `NodeClient` trait with stubbed methods
- `plan_search_scatter()` - Pure function planning
- `execute_scatter()` - Fan-out execution
- `scatter_gather_search()` - Full scatter-gather-merge
- `dfs_query_then_fetch_search()` - OP#4 global-IDF preflight
- `GlobalIdf` aggregation from preflight responses

**Tests Passing (25/25):**
- ✅ All plan tests (pure function, group rotation, shard mapping)
- ✅ All execution tests (scatter, partial, error policy)
- ✅ All scatter-gather tests (RRF, degraded)
- ✅ All preflight tests (empty query, partial failure)
- ✅ All DFS tests (skewed shards, global IDF aggregation)

## Definition of Done - All Items Verified

- [x] Rendezvous assignment is deterministic given fixed node list (verified by test)
- [x] Adding a 4th node in a 3-node group moves at most ~2 × (1/4) of shards (verified by test)
- [x] 64 shards / 3 nodes / RF=1 → each node holds 18–26 shards (verified by test)
- [x] Top-RF placement changes minimally on add / remove (verified by test)
- [x] `write_targets` returns exactly `RG × RF` nodes
- [x] `query_group(seq, RG)` distributes evenly (verified by chi-square test)
- [x] `covering_set` within a group returns exactly one node per shard
- [x] `merger` passes the merge/facet/limit tests in plan §8
- [x] Comprehensive test coverage (105 tests across 4 modules)

## Notes

All Phase 1 core routing primitives were already implemented in the codebase. This verification confirms:

1. **Correctness**: All properties of HRW (determinism, minimal reshuffle, uniformity) are verified by tests
2. **Completeness**: All required functions from plan §2 and §4 are present
3. **Test Coverage**: 105 tests covering edge cases, tie-breaking, state machines, and merge strategies
4. **Integration**: scatter.rs properly integrates router, topology, and merger for full query paths

The implementation uses:
- `twox_hash::XxHash64` with seed=0 (matching Meilisearch Enterprise)
- Proper lexicographic tie-breaking for determinism
- Group-scoped assignment preventing same-group replica placement
- RRF with k=60 (literature-backed default)
- BTreeMap for stable facet serialization

No code changes were required. Phase 1 is complete.
