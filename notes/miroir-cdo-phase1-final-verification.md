# Phase 1 (miroir-cdo): Core Routing — Final Verification Summary

**Date**: 2026-05-09
**Session**: Final verification of Phase 1 Core Routing implementation

## Definition of Done Checklist

### ✅ Router Correctness (plan §8)

1. **Rendezvous assignment determinism**: ✅ PASSED
   - `test_rendezvous_determinism`: Same (shard_id, nodes) → identical Vec<NodeId>
   - `acceptance_determinism_1000_runs`: 1000 randomized runs, all deterministic

2. **Minimal reshuffling on node add**: ✅ PASSED
   - `test_minimal_reshuffling_on_add`: Adding 4th node moves ≤ 50% of shards
   - `acceptance_reshuffle_bound_on_add`: 64 shards, 3→4 nodes → ≤ 32 edges differ

3. **Uniform shard distribution**: ✅ PASSED
   - `test_shard_distribution_64_3_rf1`: 64 shards / 3 nodes / RF=1 → 15-27 shards per node
   - `acceptance_uniformity_64_shards_3_nodes_rf1`: Each node holds 15-27 shards

4. **Top-RF placement stability**: ✅ PASSED
   - `test_top_rf_stability`: Top-RF nodes change minimally on add/remove
   - `acceptance_rf2_placement_stability`: RF=2 placement stable on node change
   - `acceptance_reshuffle_bound_on_remove`: 4→3 nodes, ~RF × S / Ng edges differ

5. **write_targets returns RG × RF nodes**: ✅ PASSED
   - `test_write_targets_count`: Exactly RG × RF nodes returned
   - `test_group_scoped_assignment`: One node from each replica group

6. **query_group distributes evenly**: ✅ PASSED
   - `test_query_group_distribution`: Round-robin distribution verified

7. **covering_set returns one node per shard**: ✅ PASSED
   - `test_covering_set_one_per_shard`: Exactly one node per shard
   - `test_covering_set_replica_rotation`: Intra-group replica rotation works

### ✅ Result Merger (plan §8)

1. **Global sort by _rankingScore**: ✅ PASSED
   - `test_global_sort_by_ranking_score`: Descending order verified

2. **Offset/limit after merge**: ✅ PASSED
   - `test_offset_and_limit_applied_after_merge`: Pagination works correctly

3. **_rankingScore stripping**: ✅ PASSED
   - `test_ranking_score_stripped_when_not_requested`: Strips when not requested
   - `test_ranking_score_included_when_requested`: Includes when requested

4. **_miroir_shard stripping**: ✅ PASSED
   - `test_miroir_shard_always_stripped`: Always stripped
   - `test_strip_all_miroir_reserved_fields`: All _miroir_* fields stripped

5. **Facet aggregation**: ✅ PASSED
   - `test_facet_counts_summed_across_shards`: Sums counts across shards
   - `test_facet_filter_only_merges_requested_facets`: Filter works

6. **estimatedTotalHits summation**: ✅ PASSED
   - `test_estimated_total_hits_summed`: Correctly summed

7. **processingTimeMs max**: ✅ PASSED
   - `test_processing_time_max_across_shards`: Max taken correctly

8. **Tie-breaking**: ✅ PASSED
   - `test_tie_breaking_by_primary_key`: Primary key ascending for ties

9. **Degraded mode**: ✅ PASSED
   - `test_degraded_flag_when_shard_fails`: Flag set when shards fail
   - `test_not_degraded_when_all_succeed`: Flag not set when all succeed

10. **Stable serialization**: ✅ PASSED
    - `test_stable_serialization_same_input_same_json`: BTreeMap ensures deterministic JSON

### ✅ Topology (plan §2)

- `topology.rs`: 100.00% line coverage
- All state transitions tested
- Write eligibility rules tested
- Group isolation tested

### ✅ Scatter (plan §2)

- `scatter.rs`: 100.00% line coverage
- Stub implementation for Phase 2 wiring

## Coverage Report (2026-05-09)

```
Filename                      Regions    Missed Regions     Cover   Functions  Missed Functions  Executed       Lines      Missed Lines     Cover    Branches   Missed Branches     Cover
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
anti_entropy.rs                    56                 0   100.00%           7                 0   100.00%          70                 0   100.00%           0                 0         -
config.rs                         293                26    91.13%          30                 5    83.33%         306                18    94.12%           0                 0         -
config/advanced.rs                166                16    90.36%          32                 2    93.75%         288                20    93.06%           0                 0         -
config/load.rs                    159                77    51.57%           9                 2    77.78%         140                28    80.00%           0                 0         -
config/validate.rs                 86                27    68.60%           1                 0   100.00%         108                46    57.41%           0                 0         -
merger.rs                         977                31    96.83%          49                 4    91.84%         582                31    94.67%           0                 0         -
migration.rs                      721               163    77.39%          43                12    72.09%         467               104    77.73%           0                 0         -
reshard.rs                        455                47    89.67%          36                 7    80.56%         324                34    89.51%           0                 0         -
router.rs                        1016                26    97.44%          60                 1    98.33%         500                19    96.20%           0                 0         -
scatter.rs                        214                 0   100.00%          11                 0   100.00%         121                 0   100.00%           0                 0         -
score_comparability.rs            589                10    98.30%          32                 0   100.00%         325                 9    97.23%           0                 0         -
task.rs                           164                 0   100.00%          16                 0   100.00%         118                 0   100.00%           0                 0         -
topology.rs                       776                 0   100.00%          70                 0   100.00%         421                 0   100.00%           0                 0         -
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
TOTAL                            5672               423    92.54%         396                33    91.67%        3770               309    91.80%           0                 0         -
```

**Overall `miroir-core` line coverage: 91.80%** (exceeds 90% requirement)

## Test Results

**Total tests run**: 151
**Passed**: 151
**Failed**: 0
**Ignored**: 0

### Breakdown by module:
- **router.rs**: 26 tests passed
- **topology.rs**: 45 tests passed
- **merger.rs**: 22 tests passed
- **scatter.rs**: 6 tests passed

## Key Implementation Details

### Rendezvous Hashing (HRW)
- Uses `twox_hash::XxHash64` with seed 0 (matches Meilisearch Enterprise)
- Canonical order: `(shard_id, node_id)` - critical for determinism
- Tie-breaking: lexicographic by `node_id` for identical scores

### Group Isolation
- Hashing scoped to intra-group node lists
- Prevents both replicas of a shard from landing in the same group
- `write_targets()` returns exactly `RG × RF` nodes, one from each group

### Minimal Reshuffling
- Adding a node to a group moves ~1/(Ng+1) of that group's docs
- Top-RF placement changes minimally on add/remove

### Result Merger
- Uses binary min-heap for efficient top-k selection
- BTreeMap for stable facet serialization
- Always strips `_miroir_*` reserved fields
- Conditionally strips `_rankingScore` based on client request

## Status: COMPLETE ✅

All Phase 1 Core Routing requirements have been implemented and verified.
The foundation is ready for Phase 2 (write path) and Phase 3 (read path).
