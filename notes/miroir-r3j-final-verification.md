# Phase 3 (miroir-r3j): Final Verification — Complete

## Date
2026-05-09

## Summary

Phase 3 (Task Registry + Persistence) was already fully implemented in the codebase. This final verification confirms all Definition of Done criteria are met.

## Definition of Done — All Complete ✅

### 1. rusqlite-backed store initializing every table idempotently at startup ✅
**Location:** `crates/miroir-core/src/task_store/sqlite.rs`
- `SqliteTaskStore::new()` creates/opens SQLite database
- `initialize()` calls `init_schema()` with `CREATE TABLE IF NOT EXISTS`
- WAL mode enabled for better concurrency
- Schema version tracked in `schema_version` table

### 2. Redis-backed store mirrors the same API (trait `TaskStore`) ✅
**Location:** `crates/miroir-core/src/task_store/redis.rs`
- Implements `TaskStore` trait with 50+ methods
- All 14 tables mapped to Redis hashes with `_index` secondary sets
- Runtime backend selection via `create_task_store()` factory

### 3. Migrations/versioning with `schema_version` row ✅
**Location:** `crates/miroir-core/src/task_store/schema.rs`
- `SCHEMA_VERSION` constant (currently 1)
- Version check on initialization rejects mismatched schemas
- Both SQLite and Redis backends track version

### 4. Property tests on SQLite backend ✅
**Location:** `crates/miroir-core/tests/task_store.rs`
- Round-trip tests for all table types
- Upsert semantics validation
- Proptest for task filtering
- 650+ lines of comprehensive test coverage

### 5. Integration test for pod restart simulation ✅
**Location:** `crates/miroir-core/tests/task_store.rs::restart_survival`
- Creates store, inserts data, closes connection
- Reopens and verifies data survived
- Tests task persistence and status updates

### 6. Redis-backend integration test with testcontainers ✅
**Location:** `crates/miroir-core/tests/task_store_redis.rs`
- Uses `testcontainers` for real Redis instance
- Tests leader lease, idempotency, rate limiting, CDC overflow, scoped keys
- 440+ lines of Redis-specific tests

### 7. `miroir:tasks:_index`-style iteration for list endpoints ✅
**Location:** `crates/miroir-core/src/task_store/redis.rs`
- `index_key()` method generates `_index` set keys
- All `*_list()` methods use `smembers()` for O(cardinality) queries
- No `SCAN` required

### 8. Helm schema enforces `taskStore.backend: redis` + `replicas > 1` ✅
**Location:** `charts/miroir/values.schema.json` (lines 142-186)
- Enforces Redis when `replicas > 1`
- Enforces Redis when `hpa.enabled: true`
- Clear error messages explaining constraints

### 9. Redis memory accounting validated against representative load ✅
**Location:** `docs/redis-memory-accounting.md`
- Complete keyspace inventory
- Per-entry size calculations
- Representative load: ~2.8 MB baseline
- Scaling recommendations provided

## Implementation Summary

### Files Implemented
- `crates/miroir-core/src/task_store/mod.rs` — Unified TaskStore trait (298 lines)
- `crates/miroir-core/src/task_store/schema.rs` — All 14 table definitions
- `crates/miroir-core/src/task_store/sqlite.rs` — SQLite backend (1,535 lines)
- `crates/miroir-core/src/task_store/redis.rs` — Redis backend (926 lines)
- `crates/miroir-core/src/task_store/error.rs` — Error types

### Test Coverage
- `tests/task_store.rs` — SQLite property tests (650 lines)
- `tests/task_store_redis.rs` — Redis integration tests (440 lines)

### Documentation
- `docs/redis-memory-accounting.md` — Complete memory analysis
- Inline documentation in all source files

## Conclusion

Phase 3 is complete and production-ready. The task store provides:
- Dual backend support (SQLite for single-pod, Redis for multi-pod)
- All 14 tables from plan §4
- Comprehensive test coverage
- HA-ready architecture for Phase 6

All future features (plan §13 advanced capabilities, §14 HA modes) can consume this persistence layer.
