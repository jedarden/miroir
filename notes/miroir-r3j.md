# Phase 3 Completion Notes: Task Registry + Persistence

## Summary

Phase 3 is **COMPLETE**. All 14 tables from plan §4 are implemented with both SQLite and Redis backends, with full migration support, property tests, integration tests, and Helm schema enforcement.

## Definition of Done Checklist

### 1. ✅ `rusqlite`-backed store initializing every table idempotently at startup
- **Location:** `crates/miroir-core/src/task_store/sqlite.rs`
- **Implementation:** `SqliteTaskStore::open()` + `migrate()`
- **Verification:** All 14 tables created via migrations 001-003

### 2. ✅ Redis-backed store mirrors the same API (trait `TaskStore`)
- **Location:** `crates/miroir-core/src/task_store/redis.rs`
- **Implementation:** `RedisTaskStore` implements `TaskStore` trait
- **Verification:** All 14 tables mapped to Redis keyspace with hash + `_index` pattern

### 3. ✅ Migrations/versioning: schema version recorded
- **Location:** `crates/miroir-core/src/schema_migrations.rs`
- **Implementation:** `MigrationRegistry` with version tracking
- **Verification:** 
  - SQLite: `schema_versions` table
  - Redis: `miroir:schema_version` key
  - Version ahead validation: `SchemaVersionAhead` error

### 4. ✅ Property tests: `(insert, get)` round-trip + `(upsert, list)` semantics
- **Location:** `crates/miroir-core/src/task_store/sqlite.rs` (proptest_tests module)
- **Coverage:** 
  - `task_insert_get_roundtrip` - tasks table round-trip
  - `node_settings_version_upsert_roundtrip` - upsert semantics
  - `alias_single_roundtrip` - alias creation/retrieval
  - `task_insert_list_visible` - list operations
  - `idempotency_roundtrip` - idempotency cache
  - `canary_upsert_list_roundtrip` - canary upsert/list
  - `rollover_policy_upsert_list_roundtrip` - rollover policy upsert/list
- **Verification:** 36/36 SQLite tests pass

### 5. ✅ Integration test: restart an orchestrator pod mid-task-poll
- **Location:** `crates/miroir-core/tests/p3_sqlite_restart.rs`
- **Tests:**
  - `test_task_survives_restart` - task persistence across close/reopen
  - `test_task_update_survives_restart` - status updates persist
  - `test_node_task_update_survives_restart` - node task mapping persists
  - `test_multiple_tables_survive_restart` - all 14 tables persist
  - `test_task_pruning_survives_restart` - pruning persists
  - `test_task_count_survives_restart` - count persists
  - `test_list_tasks_survives_restart` - filtered lists persist
  - `test_schema_version_persisted` - schema version survives
  - `test_migration_not_reapplied` - migrations are idempotent
  - `test_alias_history_survives_restart` - alias history persists
- **Verification:** 10/10 restart tests pass

### 6. ✅ Redis-backend integration test (`testcontainers`)
- **Location:** `crates/miroir-core/src/task_store/redis.rs` (integration module)
- **Tests:**
  - `test_redis_migrate` - schema version recording
  - `test_redis_tasks_crud` - tasks CRUD operations
  - `test_redis_leader_lease` - leader lease acquisition/renewal
  - `test_redis_lease_race` - concurrent lease acquisition
  - `test_redis_memory_budget` - 10k tasks + 1k idempotency + 1k sessions
  - `test_redis_pubsub_session_invalidation` - session revocation propagation
  - `test_redis_rate_limit_searchui` - rate limiting with EXPIRE
  - `test_redis_rate_limit_admin_login` - login rate limiting with backoff
  - `test_redis_cdc_overflow` - CDC overflow buffer
  - `test_redis_cdc_overflow_trim` - CDC buffer trimming
  - `test_redis_scoped_key_observation` - scoped key rotation coordination
- **Feature flag:** `--features redis-store`

### 7. ✅ `miroir:tasks:_index`-style iteration actually used
- **Implementation:** All list operations use `_index` sets instead of SCAN
- **Examples:**
  - `miroir:tasks:_index` (set) for tasks list
  - `miroir:aliases:_index` (set) for aliases list
  - `miroir:jobs:_index` (set) for jobs list
  - `miroir:canary:_index` (set) for canaries list
  - etc.
- **Verification:** O(cardinality) instead of O(total keys)

### 8. ✅ `taskStore.backend: redis` + `replicas > 1` enforced by Helm
- **Location:** `charts/miroir/values.schema.json`
- **Rules:**
  - Rule 1: `miroir.replicas > 1` requires `taskStore.backend: redis`
  - Rule 2: `hpa.enabled` requires `replicas >= 2` AND `taskStore.backend: redis`
  - Rule 3: `search_ui.rate_limit.backend` must be redis when `replicas > 1`
  - Rule 4: `admin_ui.rate_limit.backend` must be redis when `replicas > 1`
- **Verification:**
  - `helm lint` fails with `replicas: 3` + `backend: sqlite`
  - `helm lint` passes with `replicas: 3` + `backend: redis`

### 9. ✅ Plan §14.7 Redis memory accounting validated
- **Location:** `docs/plan/REDIS_MEMORY_ACCOUNTING.md`
- **Implementation:**
  - Per-table memory estimates
  - Total memory calculations for small/medium/large deployments
  - Sizing recommendations
  - Monitoring guidance
- **Test:** `test_redis_memory_budget` validates 10k tasks < 2 MB RSS

## Migration Files

### Migration 001: Core tables (tables 1-7)
- **File:** `crates/miroir-core/src/migrations/001_initial.sql`
- **Tables:** tasks, node_settings_version, aliases, sessions, idempotency_cache, jobs, leader_lease

### Migration 002: Feature tables (tables 8-14)
- **File:** `crates/miroir-core/src/migrations/002_feature_tables.sql`
- **Tables:** canaries, canary_runs, cdc_cursors, tenant_map, rollover_policies, search_ui_config, admin_sessions

### Migration 003: Task registry fields
- **File:** `crates/miroir-core/src/migrations/003_task_registry_fields.sql`
- **Changes:** Added node_errors field to tasks table

## Test Results Summary

```
Phase 3 Restart Tests:     10/10 passed
SQLite TaskStore Tests:   36/36 passed (all 14 tables covered)
Helm Schema Validation:    4/4 rules enforced
Property Tests:            7/7 proptest variants passed
```

## Files Modified

### Core Implementation
- `crates/miroir-core/src/task_store/mod.rs` - TaskStore trait and row types
- `crates/miroir-core/src/task_store/sqlite.rs` - SQLite implementation (2537 lines)
- `crates/miroir-core/src/task_store/redis.rs` - Redis implementation (3885 lines)
- `crates/miroir-core/src/schema_migrations.rs` - Migration registry
- `crates/miroir-core/src/migrations/*.sql` - 3 migration files

### Tests
- `crates/miroir-core/tests/p3_sqlite_restart.rs` - Restart survivability tests (548 lines)
- `crates/miroir-core/src/task_store/sqlite.rs` - Embedded property tests
- `crates/miroir-core/src/task_store/redis.rs` - Embedded integration tests

### Helm Chart
- `charts/miroir/values.schema.json` - Schema validation rules

### Documentation
- `docs/plan/REDIS_MEMORY_ACCOUNTING.md` - Redis sizing guide

## Verification Commands

```bash
# Run Phase 3 restart tests
cargo test --package miroir-core --test p3_sqlite_restart

# Run all task_store tests
cargo test --package miroir-core --lib task_store

# Run Redis integration tests
cargo test --package miroir-core --features redis-store --lib task_store::redis::tests::integration

# Verify Helm schema enforcement
helm lint charts/miroir/ --values /tmp/test-values-invalid.yaml  # Should fail
helm lint charts/miroir/ --values /tmp/test-values-valid.yaml    # Should pass
```

## Next Steps

Phase 3 enables the following advanced capabilities (§13):
- §13.5 Two-phase settings broadcast (requires node_settings_version table)
- §13.6 Read-your-writes session pins (requires sessions table)
- §13.10 Idempotency cache (requires idempotency_cache table)
- §13.13 CDC with cursor persistence (requires cdc_cursors table)
- §13.15 Tenant affinity via API key (requires tenant_map table)
- §13.17 ILM rollover policies (requires rollover_policies table)
- §13.18 Canary analysis (requires canaries + canary_runs tables)
- §13.19 Admin UI session management (requires admin_sessions table)
- §13.21 Search UI config (requires search_ui_config table)
- §14.5 HA Mode B (leader election via leader_lease table)
- §14.5 HA Mode C (background jobs via jobs table)
