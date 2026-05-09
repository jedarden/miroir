# Phase 3 — Task Registry + Persistence (miroir-r3j) — COMPLETION SUMMARY

## Bead: miroir-r3j

## Task Completed

Phase 3 — Task Registry + Persistence (SQLite schema, Redis mirror)

## Work Summary

The Phase 3 task store implementation was already complete in the codebase. This bead involved verification and documentation of the existing implementation.

### What Was Already Implemented

1. **14-Table SQLite Schema** (`crates/miroir-core/src/task_store/sqlite.rs`)
   - All 14 tables from plan §4 implemented
   - Idempotent initialization with WAL mode
   - Schema version tracking

2. **Redis Backend** (`crates/miroir-core/src/task_store/redis.rs`)
   - Mirrors the same `TaskStore` trait
   - `_index` pattern for O(cardinality) list queries
   - Redis-specific operations (rate limiting, CDC overflow, scoped keys)

3. **Schema Definitions** (`crates/miroir-core/src/task_store/schema.rs`)
   - All 14 table types defined
   - Enums for TaskStatus, JobStatus, AliasKind, etc.
   - SCHEMA_VERSION constant

4. **Comprehensive Test Suite**
   - Property tests with proptest (`tests/task_store.rs`)
   - Integration tests with testcontainers (`tests/task_store_redis.rs`)
   - Restart survival test

5. **Helm Schema Enforcement** (`charts/miroir/values.schema.json`)
   - `replicas > 1` requires `taskStore.backend: redis`
   - HPA enforces `replicas >= 2` and `backend: redis`

### What Was Added

1. **Redis Memory Accounting Document** (`docs/notes/phase3-task-store-verification.md`)
   - Detailed per-table memory analysis
   - Representative load calculation (20 kQPS scenario)
   - Redis sizing recommendations
   - Memory monitoring guidance

2. **DoD Verification** (`docs/notes/phase3-task-store-verification.md`)
   - Complete checklist verification
   - Links to code locations
   - Proof that all requirements are met

## Definition of Done — ALL MET ✅

- ✅ `rusqlite`-backed store initializing every table idempotently at startup
- ✅ Redis-backed store mirrors the same API (trait `TaskStore`), runtime backend selection
- ✅ Migrations/versioning: schema version recorded, incompatibility detected loudly
- ✅ Property tests: `(insert, get)` round-trip + `(upsert, list)` semantics on SQLite
- ✅ Integration test: restart survival (open/close SQLite handle between operations)
- ✅ Redis-backend integration test (`testcontainers`) exercising leases, idempotency, alias history
- ✅ `miroir:tasks:_index`-style iteration used for list endpoints (no `SCAN`)
- ✅ `taskStore.backend: redis` + `replicas > 1` enforced by Helm `values.schema.json`
- ✅ Plan §14.7 Redis memory accounting validated against representative load

## Files Modified

- `docs/notes/phase3-task-store-verification.md` — Created
- `docs/notes/miroir-r3j-phase3-summary.md` — Created

## Retrospective

### What Worked

- The existing implementation was comprehensive and well-structured
- The trait-based abstraction (`TaskStore`) makes backend switching seamless
- Test coverage is excellent, including both property tests and integration tests
- Helm schema validation prevents misconfiguration

### What Didn't

- No issues encountered — the implementation was already complete

### Surprise

- The `_index` pattern was already consistently used across all Redis list operations
- The Helm schema validation was more sophisticated than expected, with conditional enforcement

### Reusable Pattern

- For future database-backed features: use the trait pattern with SQLite/Redis backends
- Always include `_index` secondary sets in Redis for O(n) list operations without SCAN
- Use Helm `values.schema.json` with `allOf` + `if/then` for conditional validation

## Next Steps

Phase 3 is complete. The task registry is ready for use by:
- §13 advanced capabilities (all 14 tables are cross-referenced)
- §14 HA mode (Redis backend supports multi-pod deployments)

No additional work required for this bead.
