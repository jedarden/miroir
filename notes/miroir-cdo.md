# Phase 1 — Core Routing Retrospective

Bead ID: miroir-cdo

## Summary

Phase 1 Core Routing implementation complete. Implements deterministic, coordination-free routing primitives that everything else depends on.

## What Worked

- The core routing primitives (router.rs, topology.rs, scatter.rs, merger.rs) were already well-implemented with comprehensive test coverage
- All 89 tests pass (18 router tests, 14 merger tests, plus topology, scatter, and other modules)
- Rendezvous hashing (HRW) with twox-hash provides deterministic, minimal-reshuffling shard assignment
- Group-scoped assignment prevents both replicas of a shard from landing in the same group
- Node health state machine extended with Degraded, Active, and Removed states

## What Didn't

- No issues encountered; the implementation was already complete and correct

## Surprise

- The overall miroir-core package line coverage is 89.02%, slightly below the 90% target. However, the Phase 1 components themselves all exceed 90% coverage:
  - router.rs: 96.76%
  - topology.rs: 100%
  - scatter.rs: 100%
  - merger.rs: 95.45%
- The lower overall coverage is due to other files outside Phase 1 scope (config/load.rs at 0%, task.rs at 0%)

## Reusable Pattern

- Rendezvous hashing with twox-hash (seed=42) for deterministic shard assignment
- Group-scoped assignment to ensure replica isolation
- Pure-function design for merger enables comprehensive unit testing without mocks
- Node health state machine: Healthy/Active (serving), Degraded (intermittent), Joining/Draining (transitional), Failed/Removed (unavailable)
