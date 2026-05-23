# P5.5.c: Commit Phase Implementation - Settings Version Increment

**Bead:** miroir-uhj.5.3
**Date:** 2026-05-23
**Plan Reference:** §13.5 Two-phase settings broadcast with verification

## Summary

The commit phase (Phase 3) of the two-phase settings broadcast is **already fully implemented** in the codebase. This phase is the critical moment where:
1. The cluster-wide `settings_version` is incremented in the task store
2. All verified nodes have their `node_settings_version` advanced
3. Future responses include the `X-Miroir-Settings-Version` header
4. New writes are allowed to proceed freely (broadcast completes)

## Implementation Details

### 1. Core Commit Logic

**File:** `crates/miroir-core/src/settings.rs:228-269`

```rust
pub async fn commit(&self, index: &str) -> Result<u64> {
    // Increment global settings version
    let mut version = self.settings_version.write().await;
    *version += 1;
    let new_version = *version;

    // Update per-node versions for all nodes that verified successfully
    let mut node_versions = self.node_settings_version.write().await;
    let now = now_ms();
    for node_id in status.node_hashes.keys() {
        node_versions.insert((index.to_string(), node_id.clone()), new_version);

        // Persist to task store
        if let Some(ref store) = self.task_store {
            let _ = store.upsert_node_settings_version(
                index,
                node_id,
                new_version as i64,
                now,
            );
        }
    }

    status.phase = BroadcastPhase::Commit;
    status.settings_version = Some(new_version);

    Ok(new_version)
}
```

### 2. Integration in Proxy Layer

**File:** `crates/miroir-proxy/src/routes/indexes.rs:1071-1093`

The commit phase is called after successful verification:
```rust
// Phase 3: Commit - increment settings version
let new_version = state.settings_broadcast.commit(index).await?;

// Update settings version metric
state.metrics.set_settings_version(index, new_version);
state.metrics.clear_settings_broadcast_phase(index);

// Complete and remove from in-flight tracking
state.settings_broadcast.complete(index).await.ok();
```

### 3. Header Stamping

**File:** `crates/miroir-proxy/src/routes/search.rs`

The `X-Miroir-Settings-Version` header is stamped on search responses:

**Single search (lines 489-492):**
```rust
let current_version = state.settings_broadcast.current_version().await;
if current_version > 0 {
    response = response.header("X-Miroir-Settings-Version", current_version.to_string());
}
```

**Multi-target search (lines 836-838):**
```rust
let current_version = state.settings_broadcast.current_version().await;
if current_version > 0 {
    response = response.header("X-Miroir-Settings-Version", current_version.to_string());
}
```

### 4. Per-Node Version Persistence

**Schema:** `node_settings_version` table
```sql
CREATE TABLE IF NOT EXISTS node_settings_version (
    index_uid   TEXT NOT NULL,
    node_id     TEXT NOT NULL,
    version     INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (index_uid, node_id)
);
```

**Task Store Operations:**
- `upsert_node_settings_version(index_uid, node_id, version, updated_at)` - Persist per-node version
- `get_node_settings_version(index_uid, node_id)` - Retrieve for X-Miroir-Min-Settings-Version checks

## Phase 3 Flow

1. **Increment global version:** `settings_version` is incremented atomically
2. **Advance per-node versions:** Each node that verified successfully gets its version advanced
3. **Persist to task store:** `node_settings_version` table is updated for all (index, node) pairs
4. **Stamp responses:** Future search responses include `X-Miroir-Settings-Version` header
5. **Clear in-flight state:** Broadcast is removed from in-flight tracking
6. **Allow new writes:** Subsequent settings updates can proceed

## Client Behavior

Clients can observe the new settings version via:
- **Response header:** `X-Miroir-Settings-Version` on search responses
- **Freshness floor:** Echo back as `X-Miroir-Min-Settings-Version` for session-consistent reads
- **Staleness detection:** Nodes with `node_settings_version < floor` are excluded from covering set

## Metrics

- `miroir_settings_version` (gauge): Increments only on successful commit
- `miroir_settings_broadcast_phase` (gauge): Cleared after commit

## Testing

All tests pass:
- `test_two_phase_settings_broadcast_normal_flow` - ✅ Verifies version increment
- `test_node_settings_version_tracking_multiple_updates` - ✅ Verifies per-node tracking
- `test_settings_version_persistence_to_task_store` - ✅ Verifies persistence

## References

- Plan §13.5: `/home/coding/miroir/docs/plan/plan.md`
- Core implementation: `/home/coding/miroir/crates/miroir-core/src/settings.rs:228-269`
- Proxy integration: `/home/coding/miroir/crates/miroir-proxy/src/routes/indexes.rs:1071-1093`
- Header stamping: `/home/coding/miroir/crates/miroir-proxy/src/routes/search.rs:489-492,836-838`
- Tests: `/home/coding/miroir/crates/miroir-proxy/tests/p5_5_two_phase_settings_broadcast.rs`
