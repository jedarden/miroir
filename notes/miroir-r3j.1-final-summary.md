# P3.1 TaskStore trait + SQLite backend (tables 1-7) - Final Summary

## Task Overview
Implement the TaskStore trait in miroir-core and SQLite backend for the first 7 tables from plan §4 "Task store schema".

## Tables Implemented (1-7)

1. **tasks** — Miroir task registry
   - JSON node_tasks field using serde_json::Value
   - Full CRUD operations with status updates
   - Error tracking support

2. **node_settings_version**
   - Per-(index, node) settings freshness tracking
   - Composite primary key (index_uid, node_id)
   - Timestamp tracking for updates

3. **aliases**
   - Single and multi-target alias support
   - JSON array for target_uids (multi-target)
   - History tracking with retention bounds
   - Atomic flip operations via upsert

4. **sessions**
   - Read-your-writes session pins
   - Group pinning with min_settings_version
   - TTL-based expiry
   - Nullable last_write_mtask_id and last_write_at

5. **idempotency_cache**
   - BLOB for body_sha256 (32 raw bytes)
   - Write deduplication
   - TTL-based expiry with pruning

6. **jobs**
   - Background job queue with claim semantics
   - claim_expires_at updated by heartbeat (10s)
   - Progress tracking via JSON field
   - State transitions: queued → in_progress → completed/failed

7. **leader_lease**
   - Advisory-lock substitute for SQLite
   - Holder identification with expiry
   - Used for singleton-coordinator coordination

## Implementation Details

### Schema Initialization
- Idempotent migrations using `CREATE TABLE IF NOT EXISTS`
- Schema version tracking in `schema_version` table
- WAL mode enabled for better concurrency
- PRAGMA busy_timeout = 5000 to avoid deadlocks

### Type Safety
- Custom enums for TaskStatus, AliasKind, JobState
- Proper Display/FromStr implementations for all enums
- JSON serialization for complex fields (node_tasks, target_uids, history, progress)
- BLOB type for binary data (body_sha256, api_key_hash)

### Concurrency
- Arc<Mutex<Connection>> for shared access
- Transaction support for job dequeue (claim operation)
- WAL mode allows concurrent readers
- 5-second busy timeout prevents deadlocks

## Test Coverage

All 14 tests pass:
- ✅ test_initialize_schema - Schema version tracking and idempotent initialization
- ✅ test_tasks_crud - Full CRUD operations for tasks table
- ✅ test_node_settings_version - Version tracking per (index, node)
- ✅ test_aliases_single_target - Single-target alias operations
- ✅ test_aliases_multi_target - Multi-target alias operations
- ✅ test_sessions - Session upsert, get, delete operations
- ✅ test_idempotency_cache - Cache check, record, and prune
- ✅ test_jobs - Job enqueue, dequeue, status updates
- ✅ test_leader_lease - Lease acquisition, renewal, release
- ✅ test_concurrent_writes - 10 concurrent task insertions without deadlock
- ✅ test_health_check - Database health verification
- ✅ test_persistence - Data survives DB close/reopen
- ✅ test_task_with_error - Error field serialization
- ✅ test_task_filter_by_status - Filtering and pagination

## Acceptance Criteria Met

1. ✅ **CRUD operations** - Every table round-trips correctly
2. ✅ **Idempotent migrations** - Opening existing DB doesn't re-run migrations
3. ✅ **Concurrent writes** - WAL mode + busy_timeout prevents deadlocks
4. ✅ **Table size budget** - Schema designed for 100MB task registry cache

## Files Modified

- `crates/miroir-core/src/task_store/mod.rs` - Trait definition and exports
- `crates/miroir-core/src/task_store/schema.rs` - All 14 table schemas
- `crates/miroir-core/src/task_store/sqlite.rs` - SQLite backend implementation
- `crates/miroir-core/src/task_store/sqlite_tests.rs` - Comprehensive test suite
- `crates/miroir-core/src/task_store/error.rs` - Error types
- `crates/miroir-core/Cargo.toml` - Dependencies (rusqlite, tokio, async-trait)

## Verification

```bash
cargo test -p miroir-core --features task-store --lib task_store::sqlite_tests
```

All 14 tests pass in 0.06s.

## Notes

- Tables 8-14 (canaries, cdc_cursors, tenant_map, etc.) are defined in schema.rs
  but will be implemented alongside their respective Phase 5 features
- Redis backend is stubbed but not yet implemented
- The trait is defined in miroir-core (not miroir-proxy) to allow consumption by
  miroir-ctl for diagnostics without pulling in the proxy binary
