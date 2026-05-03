# Phase 3 — Task Registry + Persistence — Completion Summary

## Overview

Phase 3 has been fully implemented and verified. The 14-table task store schema from plan §4 is complete with both SQLite and Redis backends, enabling pod restart resilience and multi-replica HA mode.

## Implementation Summary

### 1. Core SQLite Backend (`crates/miroir-core/src/task_store/sqlite.rs`)
- 2,537 lines implementing all 14 tables
- WAL mode + busy_timeout configuration
- Full CRUD operations for all tables
- Idempotent migrations via `schema_versions` table
- Comprehensive test coverage (36 unit tests + property tests)

### 2. Redis Backend (`crates/miroir-core/src/task_store/redis.rs`)
- 3,885 lines implementing all 14 tables in Redis
- Hash + `_index` set pattern for O(1) lookups and O(n) listing
- Async operations with ConnectionManager
- Integration tests with testcontainers
- Redis-specific keys: rate limiting, CDC overflow, scoped keys

### 3. Migrations (`crates/miroir-core/src/schema_migrations.rs`)
- Version 1: Core tables (1-7)
- Version 2: Feature tables (8-14)
- Version 3: Task registry fields (no-op)
- Schema version validation (prevents downgrades)

### 4. Helm Schema Validation (`charts/miroir/values.schema.json`)
- Rule 0: `taskStore.backend: redis` requires `replicas > 1`
- Rule 1: `replicas > 1` requires `taskStore.backend: redis`
- Rule 2: HPA enabled requires `replicas >= 2` + Redis
- Rules 3-4: Rate limiting must use Redis in multi-replica

### 5. Documentation (`docs/plan/REDIS_MEMORY_ACCOUNTING.md`)
- Per-key memory estimates for all 14 tables
- Redis-specific key accounting
- Deployment sizing matrix (256MB to 32GB+)
- Monitoring recommendations

## Definition of Done — All Complete

- ✅ `rusqlite`-backed store with idempotent table initialization
- ✅ Redis-backed store mirroring `TaskStore` trait
- ✅ Migrations/versioning with schema version tracking
- ✅ Property tests for round-trip and list semantics
- ✅ Integration test for pod restart resilience
- ✅ Redis backend integration tests (testcontainers)
- ✅ `miroir:tasks:_index`-style iteration (no SCAN)
- ✅ Helm schema validation for Redis + replicas enforcement
- ✅ Redis memory accounting documentation

## Test Results

All tests passing:
- cargo test --package miroir-core --lib task_store: 36 passed
- cargo test --package miroir-proxy --test p3_phase3_task_registry: 12 passed

## Retrospective

### What Worked
- TaskStore trait made backends interchangeable
- Property tests caught edge cases in JSON serialization
- Migration system prevented schema drift
- Helm schema validation caught misconfigurations at deploy time

### What Didn't
- Redis async complexity required dedicated runtime threads for blocking calls
- Testcontainers setup required Docker daemon

### Surprises
- Redis hash overhead is ~100 bytes per key, more than expected
- rusqlite doesn't have native JSON support, had to use serde_json

### Reusable Patterns
- Trait-based backends enable future stores (PostgreSQL, etcd)
- Migration registry with BTreeMap ensures version ordering
- Helm allOf rules enable composable validation logic
