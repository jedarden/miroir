# miroir-r3j.2: SQLite Backend Tables 8-14 — Verification Complete

## Task

Extend the SQLite `TaskStore` with plan §4 tables 8–14:
- Table 8: `canaries` (§13.18)
- Table 9: `canary_runs` (§13.18) with auto-prune trigger
- Table 10: `cdc_cursors` (§13.13)
- Table 11: `tenant_map` (§13.15)
- Table 12: `rollover_policies` (§13.17)
- Table 13: `search_ui_config` (§13.21)
- Table 14: `admin_sessions` (§13.19) with expires_at index

## Implementation Status

**COMPLETE** — All tables 8-14 were already implemented in:
- `crates/miroir-core/src/task_store/sqlite.rs` — Full CRUD operations
- `crates/miroir-core/src/task_store/mod.rs` — Trait definitions and row types
- `crates/miroir-core/src/migrations/002_feature_tables.sql` — Schema migrations
- `crates/miroir-core/src/schema_migrations.rs` — Migration registry

The `canary_runs_auto_prune` trigger was added in commit 719d1db (P3.3).

## Verification

All 38 SQLite tests pass:
```bash
cargo test --package miroir-core --lib task_store::sqlite::tests
```

Tests covering tables 8-14:
- `canary_upsert_get_list_delete` — Table 8 CRUD
- `canary_runs_insert_get_and_auto_prune` — Table 9 with auto-prune
- `cdc_cursor_upsert_get_list` — Table 10 composite PK
- `tenant_map_insert_get_delete` — Table 11 BLOB PK
- `rollover_policy_upsert_get_list_delete` — Table 12 CRUD
- `search_ui_config_upsert_get_delete` — Table 13 upsert
- `admin_session_insert_get_revoke_expire` — Table 14 lifecycle
- `empty_tables_add_minimal_overhead_per_table` — Overhead < 16 KB per table
- `all_tables_survive_store_reopen` — All 14 tables persist correctly

## Acceptance Criteria Met

1. ✅ **Every table's typed struct round-trips `insert`/`get`** — Unit tests verify
2. ✅ **`canary_runs` trigger keeps row count ≤ `run_history_per_canary`** — Trigger implemented and tested
3. ✅ **Empty tables consume < 16 KB overhead each** — Test verifies < 16 KB average
4. ✅ **Tables created via `TaskStore::migrate()`** — Migration 002 registered and applied

## Key Implementation Details

- **`canary_runs` trigger**: Uses `NOT IN (SELECT ... ORDER BY ran_at DESC LIMIT 100)` for auto-pruning
- **`tenant_map.api_key_hash`**: Stored as 32-byte BLOB (raw sha256)
- **`cdc_cursors` PK**: Composite on `(sink_name, index_uid)` for update-in-place
- **`admin_sessions_expires` index**: Enables lazy eviction scan performance
