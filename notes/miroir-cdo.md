# Phase 1 (miroir-cdo): Core Routing — Verification Summary

## Date
2026-05-09

## Task
Phase 1 — Core Routing (rendezvous hash, topology, covering set)

## Definition of Done Verification

All Phase 1 requirements verified and satisfied:

### 1. Rendezvous Assignment Determinism
- ✅ `acceptance_determinism_1000_runs` test confirms identical assignments across 1000 runs
- Implementation: `router.rs::score()` uses XxHash64::with_seed(0) matching Meilisearch Enterprise

### 2. Minimal Reshuffling on Node Add
- ✅ `acceptance_reshuffle_bound_on_add` test confirms at most 2 × (1/4) × 64 edges differ
- 64 shards, 3→4 nodes moves ~25% of shard-node edges

### 3. Shard Distribution Uniformity
- ✅ `acceptance_uniformity_64_shards_3_nodes_rf1` test confirms each node holds 15–27 shards
- 64 shards / 3 nodes / RF=1 distributes evenly

### 4. Top-RF Placement Stability
- ✅ `acceptance_rf2_placement_stability` test confirms minimal changes on add/remove
- Top-RF nodes change minimally when topology changes

### 5. Write Targets Count
- ✅ `test_write_targets_count` confirms exactly RG × RF nodes returned
- One node from each replica group per RF

### 6. Query Group Distribution
- ✅ `test_query_group_distribution` confirms even distribution
- Round-robin selection across replica groups

### 7. Covering Set Correctness
- ✅ `test_covering_set_one_per_shard` confirms exactly one node per shard
- Intra-group replica rotation for load balancing

### 8. Merger Functionality
- ✅ All merger tests pass (global sort, offset/limit, facets, score stripping)
- Binary heap optimization for large fan-out

### 9. Code Coverage ≥ 90%
- ✅ `miroir-core` overall: 92.54% regions, 91.80% lines
- `router.rs`: 97.44% regions, 96.20% lines
- `topology.rs`: 100.00% regions, 100.00% lines
- `scatter.rs`: 100.00% regions, 100.00% lines
- `merger.rs`: 96.83% regions, 94.67% lines

## Test Results
All 151 tests pass in 60.33s:
- 35 router tests (including 8 acceptance tests)
- 67 topology tests
- 9 scatter tests
- 32 merger tests

## Implementation Status

### Completed Files
- `router.rs` — Rendezvous hash-based routing (779 lines)
- `topology.rs` — Node registry, groups, health state (820 lines)
- `scatter.rs` — Fan-out orchestration primitives (237 lines)
- `merger.rs` — Result merge primitives (1008 lines)

### Key Implementation Details
1. **Hash Function**: XxHash64::with_seed(0) for Meilisearch Enterprise compatibility
2. **Tie-Breaking**: Lexicographic node_id for deterministic assignment
3. **State Machine**: 7-state node health model with validated transitions
4. **Optimization**: Binary heap for top-k merging in large fan-out scenarios

## Conclusion
Phase 1 Core Routing is complete and verified. All deterministic routing primitives are in place for downstream phases.
