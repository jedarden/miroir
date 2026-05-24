# P1.1 Rendezvous Hash Primitives - Verification Complete

## Task Status
**COMPLETE** - All three required functions were already implemented in `/home/coding/miroir/crates/miroir-core/src/router.rs`

## Implementations Verified

### 1. `score(shard_id: u32, node_id: &str) -> u64`
- Uses `XxHash64::with_seed(0)` (line 15)
- Canonical concatenation order: `(shard_id, node_id)` (lines 16-17)
- Returns `u64` hash value (line 18)

### 2. `assign_shard_in_group(shard_id: u32, group_nodes: &[NodeId], rf: usize) -> Vec<NodeId>`
- Computes score for each node in the group (line 30)
- Sorts by score descending (line 33)
- Breaks ties lexicographically on node_id (line 34)
- Returns top `rf` nodes (lines 37-40)

### 3. `shard_for_key(primary_key: &str, shard_count: u32) -> u32`
- Uses `XxHash64::with_seed(0)` (line 73)
- Hashes primary_key (line 74)
- Returns `hash % shard_count` as u32 (line 75)

## Acceptance Tests (All Passed ✓)

1. **Determinism** - 1000 randomized runs all produce identical assignments
2. **Reshuffle bound on add** - 64 shards, 3→4 nodes: ≤ 32 edges differ
3. **Reshuffle bound on remove** - 64 shards, 4→3 nodes: ~32 edges differ
4. **Uniformity** - 64 shards, 3 nodes, RF=1: each node holds 15-27 shards
5. **RF=2 placement stability** - Top-2 nodes change minimally on add/remove
6. **shard_for_key fixture** - Verified against known test vectors
7. **Tie-breaking** - Lexicographic on node_id for identical scores
8. **Canonical order** - (shard_id, node_id) produces different hash than (node_id, shard_id)

## Test Results
```
running 26 tests
test result: ok. 26 passed; 0 failed; 0 ignored
```

## Key Design Constraints Verified
- Hash function: `twox-hash` (XxHash family) ✓
- Seed: 0 (matches Meilisearch Enterprise) ✓
- Concatenation order: (shard_id, node_id) ✓
- Group-scoped assignment: prevents same-group replicas ✓
- Tie-breaking: lexicographic on node_id ✓

## Files Modified
None - implementations were already correct and complete.
