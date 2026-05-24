# Phase 1 — Core Routing Final Verification (2026-05-09)

Bead ID: miroir-cdo
Date: 2026-05-09

## Summary

Phase 1 Core Routing implementation verified as complete. All 151 tests pass with 91.80% line coverage, exceeding the 90% requirement.

## Verification Results

### Test Execution
```
running 151 tests
test result: ok. 151 passed; 0 failed; 0 ignored; finished in 70.70s
```

### Coverage (cargo-llvm-cov)
```
Filename                      Regions    Missed Regions     Cover   Functions  Missed Functions  Executed       Lines      Missed Lines     Cover
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
router.rs                      1016                26    97.44%          60                 1    98.33%         500                19    96.20%
topology.rs                     776                 0   100.00%          70                 0   100.00%         421                 0   100.00%
scatter.rs                      214                 0   100.00%          11                 0   100.00%         121                 0   100.00%
merger.rs                       977                31    96.83%          49                 4    91.84%         582                31    94.67%
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
TOTAL                           5672               423    92.54%         396                33    91.67%        3770               309    91.80%
```

## Definition of Done Compliance

| Requirement | Status | Test |
|------------|--------|------|
| Rendezvous assignment is deterministic | ✅ | `test_rendezvous_determinism`, `acceptance_determinism_1000_runs` |
| Adding 4th node moves ≤ 2×(1/4) of shards | ✅ | `test_minimal_reshuffling_on_add`, `acceptance_reshuffle_bound_on_add` |
| 64 shards / 3 nodes / RF=1 → 18-26 shards each | ✅ | `test_shard_distribution_64_3_rf1`, `acceptance_uniformity_64_shards_3_nodes_rf1` |
| Top-RF placement changes minimally | ✅ | `test_top_rf_stability`, `acceptance_rf2_placement_stability` |
| `write_targets` returns RG × RF nodes | ✅ | `test_write_targets_count` |
| `query_group` distributes evenly | ✅ | `test_query_group_distribution` |
| `covering_set` returns one node per shard | ✅ | `test_covering_set_one_per_shard`, `test_covering_set_replica_rotation` |
| Merger passes merge/facet/limit tests | ✅ | 19 comprehensive merger tests |
| miroir-core ≥ 90% line coverage | ✅ | **91.80%** |

## Implementation Summary

### router.rs
- `score(shard_id, node_id)`: Rendezvous hashing with XxHash64::with_seed(0)
- `assign_shard_in_group()`: Deterministic shard assignment within a group
- `write_targets()`: Returns RG × RF nodes for writes
- `query_group()`: Round-robin group selection for queries
- `covering_set()`: One node per shard within chosen group
- `shard_for_key()`: Key-to-shard mapping

### topology.rs
- `Topology`: Cluster topology with nodes grouped by replica_group
- `NodeStatus`: Health state machine (Healthy/Active/Degraded/Joining/Draining/Failed/Removed)
- `Group`: Replica group with node list
- `NodeId`: Unique node identifier

### scatter.rs
- `Scatter` trait: Fan-out orchestration interface
- `StubScatter`: Stub implementation (wired in Phase 2)

### merger.rs
- `merge()`: Global sort by _rankingScore, offset/limit, facet aggregation
- `_rankingScore` stripping when not requested
- `_miroir_*` fields always stripped
- Binary heap optimization for large fan-out
- BTreeMap for stable facet serialization

## Retrospective

### What worked
- All core routing primitives implemented correctly
- Comprehensive test coverage with deterministic tests
- Rendezvous hashing provides minimal reshuffling on topology changes
- Group-scoped assignment ensures replica isolation
- Pure-function design enables thorough unit testing

### What didn't
- No issues encountered during verification

### Surprise
- Coverage exceeded 90% requirement without additional work

### Reusable pattern
- Rendezvous hashing with XxHash64::with_seed(0) for deterministic assignment
- Group-scoped assignment for replica isolation
- Pure-function design for testability
- Node health state machine for cluster management
