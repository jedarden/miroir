# P1.2 Topology type + node state machine - Completion Summary

## Task
Implement `miroir_core::topology` with complete state machine and YAML deserialization support.

## Implementation Status: ✅ COMPLETE

The topology implementation is fully complete with all acceptance criteria met:

### Core Types Implemented
- `Topology` struct with `shards`, `replica_groups`, `rf`, and `nodes`
- `Node` struct with `id`, `address`, `replica_group`, and `status`
- `NodeStatus` enum with all required states: Healthy, Degraded, Draining, Failed, Joining, Active, Removed

### Helper Methods
- ✅ `Topology::groups()` - Returns iterator over all replica groups
- ✅ `Topology::group(g: u32)` - Get a specific group by ID
- ✅ `Group::nodes()` - Get nodes in a group
- ✅ `Group::healthy_nodes()` - Get only healthy nodes in a group

### State Machine
Complete state transition validation with all legal transitions:
- (new) → Joining (POST /_miroir/nodes)
- Joining → Active (Migration complete)
- Active → Draining (POST /_miroir/nodes/{id}/drain)
- Draining → Removed (Migration complete)
- Active/Draining → Failed (Health check detects failure)
- Failed → Active (Health check recovery)
- Active/Failed ↔ Degraded (Partial health)
- Healthy ↔ Active (Bidirectional synonyms)

All illegal transitions are rejected (e.g., Joining → Draining, Removed → Active).

### Write Eligibility
`Node::is_write_eligible_for(shard_id, status)` correctly implements routing eligibility:
- Healthy/Active/Degraded: Always eligible
- Joining/Failed/Removed: Never eligible
- Draining: Eligible only for shards not being actively migrated

### Testing
All 41 topology tests pass:
- 13 state transition tests (legal and illegal)
- 8 write eligibility tests covering all statuses
- 4 YAML deserialization tests
- 16 structural and functional tests

### Verification
```bash
cargo test topology --lib
# test result: ok. 41 passed; 0 failed; 0 ignored
```

## Files Modified
- `crates/miroir-core/src/topology.rs` - Complete implementation (41 passing tests)

## Previous Commits
- 7aabf62 "P1.2 Topology type + node state machine - Add YAML deserialization test"
- Earlier commits in Phase 1 (miroir-cdo) series

## Conclusion
The topology implementation is production-ready with comprehensive state machine validation, complete test coverage, and proper YAML deserialization support for the plan §4 configuration format.
