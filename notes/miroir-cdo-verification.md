# Phase 1 — Core Routing: Final Verification

## Date
2026-05-09

## Definition of Done Verification

### ✅ All Criteria Met

1. **Rendezvous assignment determinism** ✅
   - `acceptance_determinism_1000_runs`: Verifies identical assignments across 1000 runs
   - `test_rendezvous_determinism`: Basic determinism check

2. **Minimal reshuffling on node add** ✅
   - `acceptance_reshuffle_bound_on_add`: 64 shards, 3→4 nodes moves ≤ 32 shards (2 × 1/4 × 64)
   - `test_minimal_reshuffling_on_add`: General case verification

3. **Uniform shard distribution** ✅
   - `acceptance_uniformity_64_shards_3_nodes_rf1`: 64 shards, 3 nodes, RF=1 → each node holds 15–27 shards
   - `test_shard_distribution_64_3_rf1`: Same test with debug output

4. **Top-RF placement stability** ✅
   - `acceptance_rf2_placement_stability`: RF=2 placement changes minimally on add/remove
   - `test_top_rf_stability`: Top-RF changes verification
   - `acceptance_reshuffle_bound_on_remove`: Remove node behavior

5. **write_targets returns RG × RF nodes** ✅
   - `test_write_targets_count`: Verifies exact count, uniqueness, and per-group contribution

6. **query_group distributes evenly** ✅
   - `test_query_group_distribution`: Round-robin distribution verification

7. **covering_set returns one node per shard** ✅
   - `test_covering_set_one_per_shard`: Exact shard count verification
   - `test_covering_set_replica_rotation`: Intra-group replica rotation

8. **merger functionality** ✅
   - All merger tests pass: global sort, offset/limit, facet aggregation, field stripping
   - `test_global_sort_by_ranking_score`
   - `test_offset_and_limit_applied_after_merge`
   - `test_facet_counts_summed_across_shards`
   - `test_miroir_shard_always_stripped`
   - `test_ranking_score_stripped_when_not_requested`
   - `test_offset_limit_pagination_reconstruction`
   - `test_stable_serialization_same_input_same_json`
   - And 15+ more merger tests

9. **miroir-core ≥ 90% line coverage** ✅
   - router.rs: 96.20% line coverage
   - topology.rs: 100.00% line coverage
   - scatter.rs: 100.00% line coverage
   - merger.rs: 94.67% line coverage
   - **Total miroir-core: 91.80% line coverage**

## Test Results
- **Total tests**: 151 passed, 0 failed
- All router tests passing (18+ tests)
- All topology tests passing (35+ tests)
- All scatter tests passing (5+ tests)
- All merger tests passing (20+ tests)

## Implementation Summary

### router.rs
- `score(shard_id, node_id)`: XxHash64 with seed 0 (matches Meilisearch Enterprise)
- `assign_shard_in_group()`: Deterministic HRW assignment
- `write_targets()`: Returns RG × RF nodes for writes
- `query_group()`: Round-robin group selection
- `covering_set()`: One node per shard with replica rotation
- `shard_for_key()`: Primary key to shard mapping

### topology.rs
- `NodeId`: Unique node identifier
- `NodeStatus`: 7-state machine with valid transitions
- `Node`: Health state management, write eligibility
- `Group`: Replica group with node list
- `Topology`: Cluster-level groups, nodes, RF, shards

### scatter.rs
- `Scatter` trait: Fan-out orchestration (stubbed for Phase 2 wiring)
- `StubScatter`: Default stub implementation
- Data structures: `ScatterRequest`, `ScatterResponse`, `NodeResponse`

### merger.rs
- `merge()`: Global sort, offset/limit, facet aggregation
- Field stripping: `_miroir_*` always, `_rankingScore` conditional
- Binary heap optimization for large fan-out
- `Merger` trait with `MergerImpl` and `StubMerger`

## Conclusion
Phase 1 Core Routing is **complete** and meets all Definition of Done criteria.
The implementation provides deterministic, coordination-free routing with
minimal reshuffling on topology changes — the foundation for all subsequent
phases.
