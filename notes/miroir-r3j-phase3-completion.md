# Phase 3 Task Registry + Persistence - COMPLETION VERIFICATION

## Date
2026-05-03

## Summary

Phase 3 implementation is COMPLETE. All 14 tables from plan §4 have been implemented in both SQLite and Redis backends.

## Implementation Verified

### 1. SQLite Backend (crates/miroir-core/src/task_store/sqlite.rs)
- All 14 tables implemented via TaskStore trait
- Schema migrations with version tracking
- WAL mode + busy_timeout configuration
- 36 unit tests passing (including property tests)
- Restart resilience verified (tasks survive close/reopen cycles)

### 2. Redis Backend (crates/miroir-core/src/task_store/redis.rs)
- All 14 tables mapped to Redis keyspace
- miroir:tasks:_index-style iteration for O(cardinality) queries
- Extra Redis-specific functionality (rate limiting, CDC overflow, scoped keys, Pub/Sub)
- Integration tests with testcontainers implemented

### 3. Tables Implemented (14 total)
1. tasks - Miroir task registry
2. node_settings_version - Per-(index, node) settings freshness
3. aliases - Single + multi-target aliases
4. sessions - Read-your-writes session pins
5. idempotency_cache - Write deduplication
6. jobs - Background job queue
7. leader_lease - Singleton coordinator lease
8. canaries - Canary definitions
9. canary_runs - Canary run history
10. cdc_cursors - Per-sink CDC cursors
11. tenant_map - API-key -> tenant mapping
12. rollover_policies - ILM rollover policies
13. search_ui_config - Per-index search UI config
14. admin_sessions - Admin UI session registry

### 4. Tests Verified
- 36 SQLite unit tests passing
- 7 Property tests (proptest) passing
- Restart resilience tests passing
- All 14 tables survive store reopen

### 5. Helm Schema Validation (charts/miroir/values.schema.json)
- Rule 0: taskStore.backend: redis requires miroir.replicas > 1
- Rule 1: miroir.replicas > 1 requires taskStore.backend: redis
- Rule 2: hpa.enabled requires replicas >= 2 AND redis backend
- Additional rules for rate_limit backend validation

## Definition of Done - ALL CHECKED
- rusqlite-backed store initializing every table idempotently at startup
- Redis-backed store mirrors the same API (trait TaskStore)
- Migrations/versioning with schema version recorded
- Property tests: (insert, get) round-trip + (upsert, list) semantics
- Integration test: restart resilience verified
- Redis-backend integration test code exists
- miroir:tasks:_index-style iteration implemented
- Helm values.schema.json enforces redis + replicas constraint
