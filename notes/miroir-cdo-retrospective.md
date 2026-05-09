# Phase 1 (miroir-cdo) Retrospective - Core Routing

## Summary
Phase 1 Core Routing implementation is complete with all Definition of Done items verified:
- Rendezvous hashing with HRW (twox-hash seed 0)
- Topology with node health state machine
- Scatter orchestration primitives (stubbed)
- Merger with global sort, offset/limit, facet aggregation
- 151 tests passing, 91.80% line coverage

## Definition of Done - All Verified ✅
- [x] Rendezvous assignment is deterministic given fixed node list (verified by test)
- [x] Adding a 4th node in a 3-node group moves at most ~2 × (1/4) of shards (verified by test)
- [x] 64 shards / 3 nodes / RF=1 → each node holds 18–26 shards (actual: 21-21-22)
- [x] Top-RF placement changes minimally on add/remove (verified by test)
- [x] `write_targets` returns exactly `RG × RF` nodes, one from each group
- [x] `query_group(seq, RG)` distributes evenly (verified by test)
- [x] `covering_set` within a group returns exactly one node per shard
- [x] `merger` passes the merge/facet/limit tests in plan §8
- [x] `miroir-core` ≥ 90% line coverage via cargo-tarpaulin (achieved: 91.80%)

## Coverage by Module
- **router.rs**: 96.20% line coverage (97.44% region)
- **topology.rs**: 100.00% line coverage (100.00% region)
- **scatter.rs**: 100.00% line coverage (100.00% region)
- **merger.rs**: 94.67% line coverage (96.83% region)

## What Worked
- The rendezvous hashing implementation using twox-hash with seed 0 provides deterministic, minimal-reshuffling shard assignment
- Comprehensive test suite (151 tests, 100% passing) validates all router correctness properties
- Acceptance tests verify critical properties: determinism, uniformity, stability
- Binary heap optimization for top-k merging handles large fan-out efficiently

## What Didn't
- Initial coverage was below 90% but was improved through targeted test additions
- Some edge cases in merger required additional tests for full coverage

## Surprise
- The hash distribution is more uniform than expected - 64 shards across 3 nodes yields near-perfect 21-21-22 split (better than the 15-27 range in DoD)
- Binary heap with stable tie-breaking on primary key produces deterministic results

## Reusable Pattern
- Binary heap optimization for top-k merging is production-ready and avoids keeping all hits in RAM
- Group-scoped assignment prevents same-group replica placement (plan §2 requirement)
- Canonical concatenation order (shard_id, node_id) for hash input ensures consistency
