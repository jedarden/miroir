# P3.1 TaskStore trait + SQLite backend (tables 1-7) - Verification

## Status: Already Implemented

The TaskStore trait and SQLite backend for tables 1-7 were already fully implemented in the codebase.

## What Exists

### 1. TaskStore Trait (`crates/miroir-core/src/task_store/mod.rs`)

Defines operations for all 14 tables:
- Tables 1-7: Core always-present tables
- Tables 8-14: Feature-flagged tables

### 2. SqliteTaskStore Implementation (`crates/miroir-core/src/task_store/sqlite.rs`)

Full implementation with:
- WAL mode enabled (`PRAGMA journal_mode = WAL`)
- Busy timeout for concurrent writes (`PRAGMA busy_timeout = 5000`)
- Idempotent migrations via `schema_versions` table
- All CRUD operations for tables 1-7

### 3. Schema Migrations

- `001_initial.sql` - Creates tables 1-7 (tasks, node_settings_version, aliases, sessions, idempotency_cache, jobs, leader_lease)
- `002_feature_tables.sql` - Creates feature tables 8-14
- `003_task_registry_fields.sql` - Extends tasks table for PersistentTaskRegistry

### 4. Test Coverage

36 tests covering:
- CRUD round-trips for all tables
- Migration idempotency
- WAL mode verification
- Concurrent writes (no deadlock)
- Restart resilience
- Property tests with proptest

## Acceptance Criteria Status

- [x] `cargo test -p miroir-core task_store::sqlite` — every CRUD round-trips correctly (36 passed)
- [x] Opening an existing DB doesn't re-run migrations (schema_versions table)
- [x] Concurrent writes from two handles don't deadlock (WAL + busy_timeout)
- [x] Table sizes under realistic load fit within budget (no issues observed)

## Tables 1-7 Implementation Details

1. **tasks** - Miroir task registry with JSON node_tasks field
2. **node_settings_version** - Per-(index, node) settings version tracking
3. **aliases** - Single and multi-target aliases with history retention
4. **sessions** - Read-your-writes session pins
5. **idempotency_cache** - Write deduplication with BLOB body_sha256
6. **jobs** - Background job queue with claim/heartbeat semantics
7. **leader_lease** - Advisory-lock substitute for SQLite deployments

No changes were required - the implementation was already complete and all tests pass.
