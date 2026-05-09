# Phase 1 — Core Routing: Verification Summary

## Status: COMPLETE ✓

All Definition of Done requirements verified.

## DoD Checklist

- [x] Rendezvous assignment is deterministic given fixed node list (verified by `test_rendezvous_determinism` and `acceptance_determinism_1000_runs`)
- [x] Adding a 4th node in a 3-node group moves at most ~2 × (1/4) of shards (verified by `acceptance_reshuffle_bound_on_add`)
- [x] 64 shards / 3 nodes / RF=1 → each node holds 15–27 shards (verified by `test_shard_distribution_64_3_rf1`)
- [x] Top-RF placement changes minimally on add / remove (verified by `acceptance_rf2_placement_stability` and `acceptance_reshuffle_bound_on_remove`)
- [x] `write_targets` returns exactly `RG × RF` nodes, one from each group (verified by `test_write_targets_count`)
- [x] `query_group(seq, RG)` distributes evenly (verified by `test_query_group_distribution`)
- [x] `covering_set` within a group returns exactly one node per shard (verified by `test_covering_set_one_per_shard`)
- [x] `merger` passes the merge/facet/limit tests (verified by 25+ merger tests)
- [x] 92 tests for router/topology/scatter/merger modules
- [x] All 169 miroir-core tests pass

## Implementation Summary

### router.rs
- `score(shard_id, node_id)` — Rendezvous hash using XxHash64 with seed 0
- `assign_shard_in_group()` — Deterministic assignment with tie-breaking
- `write_targets()` — Returns RG × RF nodes for writes
- `query_group()` — Round-robin group selection
- `covering_set()` — One node per shard with replica rotation
- `shard_for_key()` — Key-based shard routing

### topology.rs
- `Topology` struct with groups, nodes, RF, shards
- `Node` health state machine (Healthy/Active/Degraded/Joining/Draining/Failed/Removed)
- `Group` with healthy node filtering
- Write eligibility rules per node status

### scatter.rs
- `Scatter` trait for fan-out orchestration
- `StubScatter` for Phase 1 (wired in Phase 2)
- Request/response types for scatter operations

### merger.rs
- Global sort by `_rankingScore` descending
- Offset/limit applied after merge
- BTreeMap for deterministic facet serialization
- `_rankingScore` and `_miroir_*` field stripping
- `estimatedTotalHits` summation
- Binary heap optimization for large result sets

## Test Coverage (2025-05-09 Verification)

All 169 tests pass in 99.32s.

### Code Coverage (cargo-llvm-cov)

- **router.rs**: 96.20% lines, 97.44% regions, 98.33% functions
- **topology.rs**: 100% lines, 100% regions, 100% functions
- **scatter.rs**: 100% lines, 100% regions, 100% functions
- **merger.rs**: 94.67% lines, 96.83% regions, 91.84% functions

All Phase 1 components exceed 90% line coverage requirement.

### Overall miroir-core

- 88.46% lines (includes future phase modules: hedging.rs 0%, replica_selection.rs 0%, query_planner.rs 65.82%, migration.rs 77.73%)
- Phase 1 components only: ≥94.67% lines
