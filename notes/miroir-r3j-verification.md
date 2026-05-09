# Phase 3 (miroir-r3j): Task Registry + Persistence — Verification

## Date
2026-05-09

## Verification Summary

Phase 3 implementation was already complete from prior work (commit 3556f64). This verification session confirmed all acceptance criteria are met.

## Completed Items

### 1. SQLite Backend (`crates/miroir-core/src/task_store/sqlite.rs`)
- ✅ `rusqlite`-backed store with all 14 tables
- ✅ Idempotent schema initialization with `CREATE TABLE IF NOT EXISTS`
- ✅ WAL mode enabled for better concurrency
- ✅ Schema version tracking in `schema_version` table
- ✅ Proper error handling with `TaskStoreError` wrapper

### 2. Redis Backend (`crates/miroir-core/src/task_store/redis.rs`)
- ✅ Hash-based storage mirroring SQLite schema
- ✅ `_index` secondary sets for O(cardinality) list queries
- ✅ Schema version validation on initialization
- ✅ All HA-mode specific operations (rate limiting, CDC overflow, scoped keys)

### 3. Unified API (`crates/miroir-core/src/task_store/mod.rs`)
- ✅ `TaskStore` trait with 50+ methods
- ✅ Runtime backend switching via `task_store.backend` config
- ✅ `create_task_store()` factory function

### 4. Schema Definitions (`crates/miroir-core/src/task_store/schema.rs`)
- ✅ All 14 tables defined as Rust structs:
  1. tasks
  2. node_settings_version
  3. aliases
  4. sessions
  5. idempotency_cache
  6. jobs
  7. leader_lease
  8. canaries
  9. canary_runs
  10. cdc_cursors
  11. tenant_map
  12. rollover_policies
  13. search_ui_config
  14. admin_sessions

### 5. Property Tests (`crates/miroir-core/tests/task_store.rs`)
- ✅ `(insert, get)` round-trip for all table types
- ✅ `(upsert, list)` semantics validation
- ✅ Proptest for task filtering
- ✅ Restart survival integration test

### 6. Redis Integration Tests (`crates/miroir-core/tests/task_store_redis.rs`)
- ✅ Uses `testcontainers` for real Redis instance
- ✅ Leader lease acquisition tests
- ✅ Idempotency cache TTL tests
- ✅ Rate limiting increment/backoff tests
- ✅ CDC overflow buffer tests
- ✅ Scoped key rotation tests
- ✅ Job enqueue/dequeue FIFO tests

### 7. Helm Schema Validation (`charts/miroir/values.schema.json`)
- ✅ Enforces `taskStore.backend: redis` when `replicas > 1`
- ✅ Enforces `taskStore.backend: redis` when `hpa.enabled: true`
- ✅ Clear error messages explaining the constraint

### 8. Redis Memory Accounting (`docs/redis-memory-accounting.md`)
- ✅ All 14 tables documented with average size per entry
- ✅ HA-mode specific keys documented with TTL
- ✅ Representative load calculation (~2.8 MB baseline)
- ✅ Scaling characteristics explained
- ✅ Recommendations for Redis sizing

## Dependencies

- `rusqlite 0.32` (bundled)
- `redis 0.25` (with tokio-rustls-comp)
- `async-trait 0.1`
- `proptest 1.0` (dev)
- `testcontainers 0.23` (dev)

## Future Work

All future features can now consume this persistence layer:
- Plan §13 advanced capabilities (read-your-writes sessions, idempotency, canaries, etc.)
- Plan §14 HA modes (multi-pod with HPA, leader election)
