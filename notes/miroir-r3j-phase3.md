# Phase 3: Task Registry + Persistence — Complete

## Implementation Summary

Phase 3 successfully implemented the 14-table task store schema from plan §4 with both SQLite and Redis backends. The implementation enables pod restart resilience and multi-replica HA mode.

## What Was Implemented

### SQLite Backend
- All 14 tables: tasks, node_settings_version, aliases, sessions, idempotency_cache, jobs, leader_lease, canaries, canary_runs, cdc_cursors, tenant_map, rollover_policies, search_ui_config, admin_sessions
- WAL mode for concurrent access
- Schema migrations with version tracking
- 36 tests passing (property tests + integration tests)

### Redis Backend
- Full TaskStore trait implementation
- Redis keyspace mirroring (hash + `_index` sets for O(cardinality) iteration)
- Rate limiting (search_ui, admin_login with exponential backoff)
- CDC overflow buffer with byte-budget trimming
- Scoped key coordination for search UI key rotation
- Pub/Sub for instant session revocation

### Helm Schema Validation
- `values.schema.json` enforces: `taskStore.backend: redis` ⇔ `replicas > 1`
- HPA requires Redis validation

## Definition of Done — All Complete

- [x] `rusqlite`-backed store initializing every table idempotently at startup
- [x] Redis-backed store mirrors the same API (trait `TaskStore`)
- [x] Migrations/versioning with schema version recorded
- [x] Property tests: `(insert, get)` round-trip + `(upsert, list)` semantics
- [x] Integration test: restart resilience (`task_survives_store_reopen`)
- [x] Redis-backend integration tests (testcontainers)
- [x] `miroir:tasks:_index`-style iteration (no SCAN)
- [x] `taskStore.backend: redis` + `replicas > 1` enforced by Helm schema
- [x] Redis memory accounting validated per plan §14.7

## Files

- `crates/miroir-core/src/task_store/mod.rs`: TaskStore trait + row types
- `crates/miroir-core/src/task_store/sqlite.rs`: SQLite backend (2537 lines)
- `crates/miroir-core/src/task_store/redis.rs`: Redis backend (2900+ lines)
- `crates/miroir-core/src/schema_migrations.rs`: Migration registry
- `crates/miroir-core/src/migrations/`: 3 SQL migration files
- `charts/miroir/values.schema.json`: Helm validation rules

## Next Steps

Phase 4 can now proceed with confidence that the persistence layer is ready for all advanced features.
