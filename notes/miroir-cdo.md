# Phase 1 — Core Routing Final Verification

Bead ID: miroir-cdo
Date: 2026-05-09

## Summary

Phase 1 Core Routing implementation complete and verified. All 151 tests pass, covering deterministic, coordination-free routing primitives that everything else depends on.

## Verification Session (2026-05-09)

### What Worked

- All core routing primitives (router.rs, topology.rs, scatter.rs, merger.rs) verified as complete
- All 151 tests pass (18 router tests, 19 merger tests, 29 topology tests, 6 scatter tests, plus other modules)
- Rendezvous hashing (HRW) with XxHash64::with_seed(0) provides deterministic, minimal-reshuffling shard assignment
- Group-scoped assignment prevents both replicas of a shard from landing in the same group
- Node health state machine fully implemented with all required states
- Merger handles all edge cases: empty shards, degraded mode, pagination, facets

### What Didn't

- No issues encountered; the implementation was already complete and correct

### Surprise

- cargo-llvm-cov was available and showed 91.80% line coverage, exceeding the 90% requirement
- Phase 1 modules specifically have excellent coverage: router.rs (96.20%), topology.rs (100%), scatter.rs (100%), merger.rs (94.67%)

### Reusable Pattern

- Rendezvous hashing with XxHash64::with_seed(0) for deterministic shard assignment (matches Meilisearch Enterprise)
- Group-scoped assignment to ensure replica isolation
- Pure-function design for merger enables comprehensive unit testing without mocks
- Node health state machine: Healthy/Active (serving), Degraded (intermittent), Joining/Draining (transitional), Failed/Removed (unavailable)
- BTreeMap for stable JSON serialization in facet aggregation

## DoD Status

| Requirement | Status |
|------------|--------|
| Rendezvous assignment is deterministic | ✅ Verified |
| Adding 4th node moves ≤ 2×(1/4) of shards | ✅ Verified |
| 64 shards / 3 nodes / RF=1 → 18-26 shards each | ✅ Verified |
| Top-RF placement changes minimally | ✅ Verified |
| `write_targets` returns RG × RF nodes | ✅ Verified |
| `query_group` distributes evenly | ✅ Verified |
| `covering_set` returns one node per shard | ✅ Verified |
| Merger passes merge/facet/limit tests | ✅ Verified |
| miroir-core ≥ 90% line coverage | ✅ **91.80%** (via cargo-llvm-cov) |

## Coverage Report (cargo-llvm-cov)

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

## Test Results

```
running 151 tests
test result: ok. 151 passed; 0 failed; 0 ignored; finished in 106.38s
```
