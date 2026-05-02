# Phase 3 Completion Summary — Task Registry + Persistence

## Status: COMPLETE ✓

All 14 tables from plan §4 are implemented with both SQLite and Redis backends.

## Implementation Summary

### 1. TaskStore Trait (crates/miroir-core/src/task_store/mod.rs)
- 502 lines defining the complete API
- All 14 tables covered with CRUD operations
- Runtime backend selection via `task_store.backend` config

### 2. SQLite Backend (crates/miroir-core/src/task_store/sqlite.rs)
- 2,536 lines of production code + tests
- rusqlite-based with WAL mode for concurrency
- Idempotent migrations via schema_versions table
- **36 tests passing** including:
  - Property tests (proptest) for round-trip and upsert/list semantics
  - Restart resilience tests (task_survives_store_reopen, all_tables_survive_store_reopen)
  - Concurrent write safety
  - Schema version validation

### 3. Redis Backend (crates/miroir-core/src/task_store/redis.rs)
- 3,884 lines of production code + integration tests
- Complete TaskStore trait implementation
- Uses `miroir:tasks:_index` pattern for O(1) list queries (no SCAN)
- Integration tests using testcontainers (33 test functions)
- All Redis-specific keyspace patterns from plan §4 implemented

### 4. Migrations (crates/miroir-core/src/migrations/)
- 001_initial.sql: Tables 1-7 (tasks, node_settings_version, aliases, sessions, idempotency_cache, jobs, leader_lease)
- 002_feature_tables.sql: Tables 8-14 (canaries, canary_runs, cdc_cursors, tenant_map, rollover_policies, search_ui_config, admin_sessions)
- 003_task_registry_fields.sql: No-op (fields already present in 001)

### 5. Schema Validation (charts/miroir/values.schema.json)
- Rule 1: `miroir.replicas > 1` requires `taskStore.backend: redis`
- Rule 2: HPA enabled requires `replicas >= 2` AND `taskStore.backend: redis`
- Rule 3-4: Rate limiting backends must be redis when `replicas > 1`
- Verified with `helm lint` - rejects invalid configurations

### 6. Redis Memory Accounting (docs/plan/REDIS_MEMORY_ACCOUNTING.md)
- Complete sizing breakdown for all 14 tables
- Per-deployment sizing recommendations (256 MB to 32 GB+)
- Monitoring guidance (used_memory, evicted_keys, etc.)

## Definition of Done — All Items Complete

- ✅ rusqlite-backed store initializing every table idempotently at startup
- ✅ Redis-backed store mirrors the same API (trait TaskStore), chosen at runtime
- ✅ Migrations/versioning with schema_version row
- ✅ Property tests for SQLite (proptest module)
- ✅ Integration test: restart resilience (task_survives_store_reopen)
- ✅ Redis-backend integration tests (testcontainers, 33 test functions)
- ✅ `miroir:tasks:_index`-style iteration (no SCAN)
- ✅ Helm values.schema.json enforces `replicas > 1 → backend: redis`
- ✅ Plan §14.7 Redis memory accounting document

## Test Results

### SQLite Backend
```
running 36 tests
test result: ok. 36 passed; 0 failed; 0 ignored
```

### Redis Backend
- Unit tests pass (test_key_generation, test_now_ms, helper functions)
- Integration tests implemented using testcontainers
- 33 async integration tests covering all 14 tables

## Files Changed/Added

### Core Implementation
- crates/miroir-core/src/task_store/mod.rs (TaskStore trait, row types)
- crates/miroir-core/src/task_store/sqlite.rs (SQLite backend)
- crates/miroir-core/src/task_store/redis.rs (Redis backend)
- crates/miroir-core/src/migrations/001_initial.sql
- crates/miroir-core/src/migrations/002_feature_tables.sql
- crates/miroir-core/src/migrations/003_task_registry_fields.sql
- crates/miroir-core/src/schema_migrations.rs (MigrationRegistry)

### Documentation
- docs/plan/REDIS_MEMORY_ACCOUNTING.md

### Configuration
- charts/miroir/values.schema.json (HA validation rules)
- charts/miroir/values.yaml (taskStore.backend config)

## Total Lines of Code
- TaskStore trait: 502 lines
- SQLite backend: 2,536 lines
- Redis backend: 3,884 lines
- **Total: 6,922 lines**

Phase 3 is complete and ready for Phase 4 (Topology Operations integration).
