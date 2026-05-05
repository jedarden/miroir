# Phase 3 Retrospective — miroir-r3j

**Task:** Task Registry + Persistence (SQLite schema, Redis mirror)
**Status:** Complete
**Date:** 2026-05-05

## Summary

Implemented the full task store with SQLite and Redis backends covering all 14 tables from plan §4.

## What Worked

1. **Migration-first approach** — Creating migrations first with the schema_migrations module made version tracking straightforward and idempotent.
2. **Trait abstraction** — Defining TaskStore trait once and implementing it for both SQLite and Redis kept the APIs consistent.
3. **Property tests with proptest** — Found edge cases in JSON serialization (node_tasks, node_errors, history arrays) that unit tests wouldn't have caught.
4. **Redis index sets** — Using `*_index` sets for O(1) listing avoided SCAN and made list operations predictable.

## What Didn't

1. **Initial schema design** — Had to add `node_errors` field in migration 003; better to have included it in 001_initial.sql.
2. **Redis EXPIRE vs manual pruning** — Initially tried manual pruning for sessions/idempotency, but Redis EXPIRE is simpler and more reliable.

## Surprises

1. **rusqlite JSON handling** — No native JSON type; had to serialize/deserialize manually for node_tasks and history fields.
2. **Redis async in sync context** — Needed `block_on` helper with dedicated thread runtime to avoid "runtime inside runtime" panics.
3. **SQLite subquery DELETE LIMIT** — Not directly supported; needed rowid IN (SELECT rowid ... LIMIT) pattern.

## Reusable Pattern

For future multi-backend store implementations:
- Define trait first with all CRUD operations
- Use migration registry pattern (versioned SQL files)
- For Redis: use `table:_index` sets for listing, avoid SCAN
- For SQLite: use WAL mode + busy_timeout for concurrent access
- Property tests are worth the setup for JSON-heavy schemas

## Deliverables

- SqliteTaskStore (2551 lines) with all 14 tables
- RedisTaskStore (3939 lines) with all 14 tables  
- 3 migration files with version tracking
- REDIS_MEMORY_ACCOUNTING.md (305 lines)
- Helm values.schema.json enforcing replicas > 1 → redis
- 12 Phase 3 DoD tests + 36 SQLite unit tests
