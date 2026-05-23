# P5.7 §13.7: Atomic Index Aliases - Verification Notes

## Overview
This bead was a verification task. The atomic index alias feature (plan §13.7) was already implemented in previous commits:
- c670d09: Fix alias admin API routes and reorganize alias module
- 821dea3: Complete alias acceptance tests
- 823fdd0: Add atomic index alias integration tests
- f564f3d: Add alias flip metrics emission

## Acceptance Criteria Verification

All 5 acceptance criteria verified as complete:

1. **Single-target alias resolution** ✅
   - Both writes and reads resolve to the target index
   - Tests: `create_single_target_alias_writes_and_reads_resolve`, `single_target_alias_resolves_reads_and_writes`

2. **Atomic alias flipping** ✅
   - New writes land on new target
   - In-flight requests complete against old target without error
   - Tests: `flip_alias_new_writes_use_new_target`, `atomic_flip_redirects_writes_without_tearing`

3. **Multi-target alias behavior** ✅
   - Reads fan out to all targets
   - Writes return 409 with `miroir_multi_alias_not_writable` error code
   - Tests: `create_multi_target_alias_reads_fanout`, `write_to_multi_target_alias_returns_not_writable`, `multi_target_alias_fans_out_reads_and_rejects_writes`

4. **ILM-managed alias protection** ✅
   - Operator edits to multi-target aliases return 409
   - Only ILM can modify multi-target aliases via `update_multi` method
   - Tests: `operator_edit_of_ilm_multi_alias_rejected`, `multi_target_alias_rejects_flip_operation`

5. **History retention** ✅
   - 11th flip evicts the oldest entry
   - Configurable via `aliases.history_retention` (default: 10)
   - Tests: `flip_alias_history_retention`, `history_retention_evicts_oldest_on_11th_flip`

## Implementation Components

**Core Library** (`crates/miroir-core/src/alias/mod.rs`):
- `Alias` enum: `Single` or `Multi` kind
- `AliasRegistry`: In-memory registry with task-store persistence
- Methods: `resolve()`, `flip()`, `update_multi()`, `is_multi_target_alias()`

**TaskStore Interface** (`crates/miroir-core/src/task_store/mod.rs`):
- `create_alias()`, `get_alias()`, `flip_alias()`, `delete_alias()`, `list_aliases()`
- `NewAlias`, `AliasRow`, `AliasHistoryEntry` types

**SQLite Backend** (`crates/miroir-core/src/task_store/sqlite.rs`):
- Full implementation with history retention bounded by `history_retention` parameter
- Atomic flip using transaction

**Admin API** (`crates/miroir-proxy/src/routes/aliases.rs`):
- `POST /_miroir/aliases/{name}` - Create single or multi-target alias
- `GET /_miroir/aliases` - List all aliases
- `GET /_miroir/aliases/{name}` - Get alias with history
- `PUT /_miroir/aliases/{name}` - Flip single-target alias
- `DELETE /_miroir/aliases/{name}` - Delete alias

**Configuration** (`AliasesConfig` in `advanced.rs`):
- `enabled: true` - Feature flag
- `history_retention: 10` - Last-N flips to retain
- `require_target_exists: true` - Validate target index existence

**Metrics** (`middleware.rs`):
- `miroir_alias_resolutions_total{alias}` - Per-resolution counter
- `miroir_alias_flips_total{alias}` - Per-flip counter

**Integration Points**:
- Documents route: Resolves aliases, rejects multi-target writes with 409
- Search route: Resolves aliases for queries, fans out to multi-targets
- Multi-search route: Resolves aliases per query
- Main proxy: Loads aliases from task store on startup via `sync_from_store()`

## Test Coverage

**Acceptance Tests** (`crates/miroir-core/tests/p13_7_alias_acceptance_tests.rs`):
- 17 tests covering all acceptance criteria
- All passing

**Unit Tests** (`crates/miroir-core/src/alias/mod.rs`):
- 28 tests covering core functionality
- All passing

## Key Design Decisions

1. **Resolution at routing time**: Aliases are resolved once at request routing, so in-flight requests complete against the target captured at route time (no torn reads).

2. **Shared state via task store**: All pods read from the same task store, ensuring consistent alias resolution across the cluster.

3. **History as JSON array**: Stored in `aliases.history` column, bounded by `history_retention` config (default 10). Last-N flips retained for debugging and rollback.

4. **Multi-target aliases are read-only**: Writes to multi-target aliases return 409 `miroir_multi_alias_not_writable`, with message pointing to the owning ILM policy.

5. **Metrics for observability**: Per-alias counters for resolutions and flips enable monitoring alias usage and flip frequency.
