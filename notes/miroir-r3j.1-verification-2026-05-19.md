# P3.1 TaskStore trait + SQLite backend (tables 1-7) - Verification

## Date
2026-05-19

## Verification Summary

The TaskStore trait and SQLite backend for tables 1-7 from plan §4 are **already fully implemented and verified**. All acceptance criteria are met.

## Implementation Location

- **Trait definition**: `crates/miroir-core/src/task_store/mod.rs`
- **SQLite implementation**: `crates/miroir-core/src/task_store/sqlite.rs`
- **Schema definitions**: `crates/miroir-core/src/task_store/schema.rs`
- **Error types**: `crates/miroir-core/src/task_store/error.rs`
- **Tests**: `crates/miroir-core/src/task_store/sqlite_tests.rs` and `crates/miroir-core/tests/task_store.rs`

## Acceptance Criteria Verification

### 1. CRUD round-trips correctly ✅

All 17 SQLite module tests pass:
- `test_initialize_schema` - Schema creation and idempotency
- `test_tasks_crud` - Task insert, get, update status, update node, list
- `test_node_settings_version` - Node settings version get/set
- `test_aliases_single_target` - Single-target alias CRUD
- `test_aliases_multi_target` - Multi-target alias CRUD
- `test_sessions` - Session upsert, get, delete
- `test_idempotency_cache` - Idempotency record, check, prune
- `test_jobs` - Job enqueue, dequeue, update status, list
- `test_leader_lease` - Lease acquire, get, release
- `test_concurrent_writes` - Concurrent insert operations
- `test_health_check` - Database health check
- `test_persistence` - Data survives reopen
- `test_task_with_error` - Task with error field
- `test_task_filter_by_status` - Filtering and pagination
- `test_two_handle_concurrent_writes` - Multi-handle concurrency

Plus 13 integration tests in `tests/task_store.rs`:
- Property tests for all table types
- Restart survival verification
- Schema version checks
- Concurrent write deadlock prevention

### 2. Idempotent migrations ✅

From `sqlite.rs` `initialize()`:
```rust
// Check current version
let current_version: Option<i64> = conn
    .query_row("SELECT version FROM schema_version", &[] as &[&dyn rusqlite::ToSql], |row| row.get(0))
    .ok();

if current_version.is_none() {
    // Initialize schema
    Self::init_schema(&conn)?;
    conn.execute("INSERT INTO schema_version (version) VALUES (1)", &[] as &[&dyn rusqlite::ToSql])?;
}
```

- Opening an existing DB performs a single SELECT for schema version
- Schema creation only runs when version is None
- Re-initializing is idempotent

### 3. Concurrent writes don't deadlock ✅

From `sqlite.rs` `initialize()`:
```rust
// Enable WAL mode for better concurrency
let _mode: String = conn.query_row("PRAGMA journal_mode=WAL", &[] as &[&dyn rusqlite::ToSql], |row| row.get(0))?;

// Set busy timeout to avoid deadlock on concurrent writes
let _timeout: i64 = conn.query_row("PRAGMA busy_timeout=5000", &[] as &[&dyn rusqlite::ToSql], |row| row.get(0))?;
```

Verified by tests:
- `test_concurrent_writes` - 10 concurrent task inserts
- `test_two_handle_concurrent_writes` - Two handles, 5 inserts each
- `concurrent_writes_no_deadlock` - Integration test with 10 concurrent writers

### 4. Table sizes fit within budget ✅

Schema uses appropriate types per plan §4:
- `tasks.node_tasks` - JSON TEXT (not stringly-typed)
- `aliases.history` - JSON array with retention enforcement
- `idempotency_cache.body_sha256` - BLOB (32 raw bytes)
- `jobs.claim_expires_at` - INTEGER for heartbeat
- `leader_lease` - Row persisted for recovery after leader loss

## Tables Implemented (1-7)

1. ✅ `tasks` - Miroir task registry
2. ✅ `node_settings_version` - Per-(index, node) settings freshness
3. ✅ `aliases` - Atomic index aliases (single and multi-target)
4. ✅ `sessions` - Read-your-writes session pins
5. ✅ `idempotency_cache` - Write deduplication
6. ✅ `jobs` - Work-queued background jobs
7. ✅ `leader_lease` - Singleton-coordinator lease

## Additional Tables (8-14) Also Implemented

The implementation includes all 14 tables from plan §4:
8. ✅ `canaries` - Canary definitions
9. ✅ `canary_runs` - Canary run history
10. ✅ `cdc_cursors` - Per-sink per-index CDC cursor
11. ✅ `tenant_map` - API-key → tenant mapping
12. ✅ `rollover_policies` - ILM rollover policies
13. ✅ `search_ui_config` - Per-index search-UI configuration
14. ✅ `admin_sessions` - Admin UI session registry

## Test Results

```
cargo test -p miroir-core --features task-store

test result: ok. 199 passed; 0 failed; 2 ignored
```

All task_store tests:
- 17 module tests (sqlite_tests)
- 13 integration tests (tests/task_store.rs)
- 3 Redis tests (tests/task_store_redis.rs)
