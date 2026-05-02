# Phase 3 Verification Summary

## Task: Task Registry + Persistence (SQLite schema, Redis mirror)

**Bead:** miroir-r3j
**Status:** COMPLETE (previously implemented)
**Verified:** 2025-05-02

## Definition of Done Verification

### ✅ rusqlite-backed store initializing every table idempotently at startup
- **File:** `crates/miroir-core/src/task_store/sqlite.rs` (2,536 lines)
- **Implementation:** `SqliteTaskStore::open()` + `migrate()` initializes all 14 tables
- **Migrations:** `crates/miroir-core/src/migrations/001_initial.sql` (tables 1-7), `002_feature_tables.sql` (tables 8-14)
- **Tests:** 36 tests passing, including `migration_is_idempotent`, `wal_mode_enabled`

### ✅ Redis-backed store mirrors the same API, chosen at runtime by task_store.backend
- **File:** `crates/miroir-core/src/task_store/redis.rs` (3,884 lines)
- **Implementation:** `RedisTaskStore` implements full `TaskStore` trait
- **Feature-gated:** `redis-store` feature flag
- **Tests:** 33+ async integration tests using testcontainers

### ✅ Migrations/versioning: schema version recorded
- **File:** `crates/miroir-core/src/schema_migrations.rs`
- **Implementation:** MigrationRegistry tracks version, pending_migrations(), validate_version()
- **Error:** `SchemaVersionAhead` fails startup if store is ahead of binary
- **Tests:** `schema_version_recorded`, `schema_version_ahead_fails`

### ✅ Property tests on SQLite backend
- **Tests:** 7 proptest tests with 50 cases each
- **Coverage:** task_insert_get_roundtrip, node_settings_version_upsert_roundtrip, alias_single_roundtrip, task_insert_list_visible, idempotency_roundtrip, canary_upsert_list_roundtrip, rollover_policy_upsert_list_roundtrip

### ✅ Integration test: restart resilience
- **Tests:** `task_survives_store_reopen`, `all_tables_survive_store_reopen`
- **Coverage:** All 14 tables survive close/reopen cycle
- **Implementation:** Uses tempfile for on-disk SQLite testing

### ✅ Redis-backend integration test
- **Tests:** 33+ async tests covering all tables plus rate limiting, CDC overflow, Pub/Sub
- **Infrastructure:** testcontainers for Redis
- **Coverage:** Leases, idempotency dedup, alias history, canaries, sessions, jobs, CDC cursors, tenant map, rollover policies, search UI config, admin sessions

### ✅ miroir:tasks:_index-style iteration
- **Implementation:** Redis uses `_index` sets for O(1) list operations
- **No SCAN:** List operations iterate over index sets, not SCAN

### ✅ Helm schema enforcement
- **File:** `charts/miroir/values.schema.json`
- **Rules:**
  - Rule 1: `miroir.replicas > 1` requires `taskStore.backend: redis`
  - Rule 2: `hpa.enabled` requires `replicas >= 2` AND `taskStore.backend: redis`
  - Rule 3: `search_ui.rate_limit.backend` must be redis when replicas > 1
  - Rule 4: `admin_ui.rate_limit.backend` must be redis when replicas > 1
- **Verified:** `helm lint charts/miroir` passes

### ✅ Redis memory accounting (plan §14.7)
- **Documentation:** `docs/redis-memory.md` (377 lines)
- **Coverage:** All 14 tables + auxiliary keys with size estimates
- **Example:** 10k tasks workload ≈ 3.5 MB raw (~6 MB with overhead)
- **Metric:** `miroir_cdc_redis_memory_bytes` Prometheus metric

## Test Results

```
cargo test -p miroir-core --lib task_store
running 36 tests
test result: ok. 36 passed; 0 failed; 0 ignored; 0 measured; 247 filtered out; finished in 0.72s
```

## Architecture

```
TaskStore trait (task_store/mod.rs)
├── SqliteTaskStore (task_store/sqlite.rs)
│   ├── rusqlite with WAL mode
│   ├── 14 tables via migrations
│   ├── Property tests (proptest)
│   └── Restart resilience tests
└── RedisTaskStore (task_store/redis.rs)
    ├── redis-rs with connection-manager
    ├── 14 hash + _index patterns
    ├── Async operations
    └── testcontainers integration
```

## Previous Commits

- `ba70cd2` P3: Complete Phase 3 — Task Registry + Persistence (SQLite + Redis)
- `ac80d1f` P3: Phase 3 Task Registry + Persistence — COMPLETE
- `3df603a` P3.3: Add StreamExt import and property tests for Redis task store

## Conclusion

Phase 3 is fully implemented and tested. All 14 tables are defined with both SQLite and Redis backends. The implementation supports single-pod (SQLite) and multi-pod HA (Redis) deployments with proper schema validation and migration support.
