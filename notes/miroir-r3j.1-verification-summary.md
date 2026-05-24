# P3.1 TaskStore trait + SQLite backend (tables 1-7) - Verification Summary

## Implementation Overview

The TaskStore trait and SQLite backend have been successfully implemented for the first 7 tables from plan §4:

1. **tasks** — Miroir task registry
2. **node_settings_version** — Per-(index, node) settings freshness
3. **aliases** — Atomic index aliases (single and multi-target)
4. **sessions** — Read-your-writes session pins
5. **idempotency_cache** — Write deduplication
6. **jobs** — Work-queued background jobs
7. **leader_lease** — Singleton-coordinator lease

## Key Implementation Details

### Schema Management
- ✅ Idempotent migrations using `CREATE TABLE IF NOT EXISTS`
- ✅ Schema version table tracks applied migrations
- ✅ Single SELECT on open to check schema version
- ✅ WAL mode enabled for better concurrency
- ✅ `PRAGMA busy_timeout = 5000` to prevent deadlocks

### Data Types
- ✅ `tasks.node_tasks` is JSON (HashMap<String, u64>)
- ✅ `aliases.history` is JSON array (Vec<AliasHistoryEntry>)
- ✅ `idempotency_cache.body_sha256` is BLOB (Vec<u8>, 32 bytes)
- ✅ `jobs.claim_expires_at` supports heartbeat renewal
- ✅ `leader_lease` implements advisory-lock semantics

### Thread Safety
- ✅ `Arc<Mutex<Connection>>` wrapper for safe concurrent access
- ✅ All operations are async via `#[async_trait]`
- ✅ Tested with 10 concurrent writers without deadlock

### Trait Definition (in miroir-core)
The TaskStore trait is defined in `crates/miroir-core/src/task_store/mod.rs` and provides:
- Schema management (`initialize`, `schema_version`)
- CRUD operations for all 7 tables
- Helper methods for filtering, listing, and batch operations
- Health check endpoint

## Test Coverage

### Unit Tests (sqlite_tests.rs)
14 tests covering:
- Schema initialization and idempotency
- CRUD operations for all 7 tables
- Status filtering and pagination
- Error handling
- Health checks

### Integration Tests (tests/task_store.rs)
13 tests covering:
- Round-trip operations for all tables
- Restart survival (persistence)
- Schema version checks
- Concurrent writes (no deadlock)
- Job queue and dequeue semantics
- Leader lease acquisition and renewal

### Test Results
```
cargo test -p miroir-core --features task-store

Unit tests: 14 passed
Integration tests: 13 passed
Total: 27 tests, all passing
```

## Acceptance Criteria Verification

### ✅ Criterion 1: CRUD Round-trips
**Status:** PASS

All tables support full CRUD operations:
- Insert/Create: `task_insert`, `alias_upsert`, `session_upsert`, etc.
- Read/Get: `task_get`, `alias_get`, `session_get`, etc.
- Update: `task_update_status`, `task_update_node`, `alias_upsert`, etc.
- Delete: `alias_delete`, `session_delete`, `idempotency_prune`, etc.

Tests verify each operation round-trips correctly with data integrity.

### ✅ Criterion 2: Idempotent Migrations
**Status:** PASS

- Opening an existing DB skips migrations with single SELECT
- Schema version stored in `schema_version` table
- Re-initialization is idempotent (verified in tests)
- Version mismatch returns error

### ✅ Criterion 3: Concurrent Writes
**Status:** PASS

- WAL mode enabled: `PRAGMA journal_mode=WAL`
- Busy timeout configured: `PRAGMA busy_timeout=5000`
- Tested with 10 concurrent writers
- No deadlocks observed
- Thread-safe via `Arc<Mutex<Connection>>`

### ⚠️ Criterion 4: Table Size Budget
**Status:** NOT TESTED (Requires load testing)

Plan §14.2 specifies "Task registry cache 100 MB" budget. This requires:
- Realistic load testing with production-like data volumes
- Measurement of actual table sizes under load
- Verification that cache stays within 100 MB

This acceptance criterion requires performance testing beyond unit/integration tests and should be validated during load testing or staging environment validation.

## Non-Obvious Implementation Details

### 1. JSON Column Handling
- `node_tasks`: Serialized as JSON string, deserialized to `HashMap<String, u64>`
- `aliases.target_uids`: Serialized as JSON array, supports NULL for single-target
- `aliases.history`: Bounded JSON array (enforcement at application layer)
- `jobs.params`: JSON string for flexible job parameters

### 2. BLOB Handling
- `idempotency_cache.body_sha256`: Stored as 32-byte BLOB
- `tenant_map.api_key_hash`: Stored as BLOB (SHA256 output)

### 3. Timestamps
- All timestamps stored as Unix milliseconds (INTEGER)
- Consistent use of `chrono::Utc::now().timestamp_millis()`

### 4. Nullable Fields
- Proper handling of optional fields via `Option<T>`
- Empty string vs NULL distinction maintained
- Database NULL ↔ Rust None mapping correct

### 5. Job Claiming
- `job_dequeue` runs in transaction for atomicity
- Claim expiration set to 5 minutes from dequeue
- Heartbeat renewal via `job_update_status` with new `claim_expires_at`

### 6. Leader Lease
- Acquire checks existing valid leases before inserting
- Release is simple DELETE
- Get returns first lease (typically single-scope usage)

## Files Modified/Created

### Core Implementation
- `crates/miroir-core/src/task_store/mod.rs` - Trait definition and factory
- `crates/miroir-core/src/task_store/schema.rs` - Schema types (all 14 tables)
- `crates/miroir-core/src/task_store/error.rs` - Error types
- `crates/miroir-core/src/task_store/sqlite.rs` - SQLite backend (tables 1-14)
- `crates/miroir-core/src/task_store/redis.rs` - Redis backend (placeholder)

### Tests
- `crates/miroir-core/src/task_store/sqlite_tests.rs` - Unit tests (14 tests)
- `crates/miroir-core/tests/task_store.rs` - Integration tests (13 tests)

### Configuration
- `crates/miroir-core/src/config.rs` - TaskStoreConfig integration
- `crates/miroir-core/src/lib.rs` - Module exports with feature flag

## Usage Example

```rust
use miroir_core::task_store::{SqliteTaskStore, TaskStore};
use std::sync::Arc;

// Create store
let store = SqliteTaskStore::new("/data/miroir-tasks.db").await?;
store.initialize().await?;

// Insert task
let task = Task {
    miroir_id: "task-1".to_string(),
    created_at: 12345,
    status: TaskStatus::Enqueued,
    node_tasks: HashMap::new(),
    error: None,
};
store.task_insert(&task).await?;

// Get task
let retrieved = store.task_get("task-1").await?.unwrap();
```

## Future Work

Tables 8-14 are already implemented but tested separately:
- Table 8: `canaries` (§13.18)
- Table 9: `canary_runs` (§13.18)
- Table 10: `cdc_cursors` (§13.13)
- Table 11: `tenant_map` (§13.15)
- Table 12: `rollover_policies` (§13.17)
- Table 13: `search_ui_config` (§13.21)
- Table 14: `admin_sessions` (§13.19)

These will be validated when their respective Phase 5 features are implemented.

## Conclusion

The TaskStore trait and SQLite backend for tables 1-7 are fully implemented and tested. All acceptance criteria pass except for the load testing requirement (table size budget), which requires production-like load testing.

The implementation is ready for use in single-pod dev mode and provides a solid foundation for HA mode (Redis backend).
