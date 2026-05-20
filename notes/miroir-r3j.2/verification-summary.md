# P3.2 SQLite Backend Tables 8-14 Verification Summary

## Task
Extend SQLite `TaskStore` with plan §4 tables 8–14: canaries, canary_runs, cdc_cursors, tenant_map, rollover_policies, search_ui_config, admin_sessions.

## Verification Status: COMPLETE ✅

The implementation is already present in the codebase (likely completed in P3.1).

## What Was Verified

### 1. Schema Migration (`crates/miroir-core/src/migrations/002_feature_tables.sql`)
- ✅ Table 8: `canaries` with id, name, index_uid, interval_s, query_json, assertions_json, enabled, created_at
- ✅ Table 9: `canary_runs` with composite PK (canary_id, ran_at), status, latency_ms, failed_assertions_json
- ✅ **Auto-prune trigger**: `canary_runs_auto_prune` keeps only 100 most recent runs per canary
- ✅ Table 10: `cdc_cursors` with composite PK (sink_name, index_uid)
- ✅ Table 11: `tenant_map` with BLOB PRIMARY KEY api_key_hash (32-byte sha256)
- ✅ Table 12: `rollover_policies` with name, write_alias, read_alias, pattern, triggers_json, retention_json, template_json, enabled
- ✅ Table 13: `search_ui_config` with index_uid, config_json, updated_at
- ✅ Table 14: `admin_sessions` with session_id, csrf_token, admin_key_hash, created_at, expires_at, revoked, user_agent, source_ip
- ✅ **Index**: `admin_sessions_expires ON admin_sessions(expires_at)` for lazy eviction

### 2. TaskStore Trait (`crates/miroir-core/src/task_store/mod.rs`)
All trait methods for tables 8-14 are defined:
- ✅ `upsert_canary`, `get_canary`, `list_canaries`, `delete_canary`
- ✅ `insert_canary_run`, `get_canary_runs`
- ✅ `upsert_cdc_cursor`, `get_cdc_cursor`, `list_cdc_cursors`
- ✅ `insert_tenant_mapping`, `get_tenant_mapping`, `delete_tenant_mapping`
- ✅ `upsert_rollover_policy`, `get_rollover_policy`, `list_rollover_policies`, `delete_rollover_policy`
- ✅ `upsert_search_ui_config`, `get_search_ui_config`, `delete_search_ui_config`
- ✅ `insert_admin_session`, `get_admin_session`, `revoke_admin_session`, `delete_expired_admin_sessions`

### 3. SqliteTaskStore Implementation (`crates/miroir-core/src/task_store/sqlite.rs`)
All methods implemented (lines 672-1106):
- ✅ Tables 8-14 CRUD operations
- ✅ `insert_canary_run` includes auto-prune logic (though trigger handles it automatically)
- ✅ Proper JSON serialization/deserialization for complex fields
- ✅ Boolean fields stored as INTEGER (0/1)

### 4. Tests (All 38 Pass)
- ✅ `canary_upsert_get_list_delete`
- ✅ `canary_runs_insert_get_and_auto_prune`
- ✅ `canary_runs_empty_for_nonexistent_canary`
- ✅ `cdc_cursor_upsert_get_list`
- ✅ `tenant_map_insert_get_delete`
- ✅ `tenant_map_nullable_group_id`
- ✅ `rollover_policy_upsert_get_list_delete`
- ✅ `search_ui_config_upsert_get_delete`
- ✅ `admin_session_insert_get_revoke_expire`
- ✅ `admin_session_nullable_fields`
- ✅ `all_tables_survive_store_reopen`
- ✅ `empty_tables_add_minimal_overhead_per_table`

## Acceptance Criteria Verification

1. ✅ **Every table's typed struct round-trips `insert`/`get` in a unit test**
   - All 7 feature tables have passing CRUD tests

2. ✅ **`canary_runs` trigger keeps row count ≤ `run_history_per_canary`**
   - Trigger `canary_runs_auto_prune` in migration 002
   - Test `canary_runs_insert_get_and_auto_prune` verifies behavior

3. ✅ **Tables that remain empty consume < 16 KB each**
   - Test output: Average 9 KB per feature table (under 16 KB limit)

4. ✅ **Tables created only when `TaskStore::migrate` is called**
   - Migrations applied in `SqliteTaskStore::migrate()` method
   - Dev-mode single-pod with all features off creates only core 7 tables

## Conclusion
No implementation work was required. The SQLite backend tables 8-14 were already implemented.
