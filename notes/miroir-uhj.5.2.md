# P5.5.b: Verify Phase Implementation - Parallel Settings Hash Verification

**Bead:** miroir-uhj.5.2
**Date:** 2026-05-23
**Plan Reference:** §13.5 Two-phase settings broadcast with verification

## Summary

Implemented the verify phase (Phase 2) of the two-phase settings broadcast with **parallel hash verification**. This ensures that all nodes have identical settings by computing SHA256 hashes of canonical JSON settings from each node and comparing them against the proposed settings hash.

## Changes Made

### 1. Parallel Verify Phase Implementation

**File:** `crates/miroir-proxy/src/routes/indexes.rs`

**Key Change:** Converted the sequential verify loop to parallel execution using `futures_util::future::join_all`.

**Before (Sequential):**
```rust
for address in &nodes {
    let path = format!("/indexes/{}{}", index, settings_path);
    match client.get_raw(address, &path).await {
        // ... handle result
    }
}
```

**After (Parallel):**
```rust
// Spawn GET requests to all nodes concurrently
let verify_tasks: Vec<_> = nodes.iter().map(|address| {
    let client = client.clone();
    let address = address.clone();
    let path = format!("/indexes/{}{}", index, settings_path);
    async move {
        (address.clone(), client.get_raw(&address, &path).await)
    }
}).collect();

let results = join_all(verify_tasks).await;

// Process results
for (address, result) in results {
    // ... handle result
}
```

### 2. Added Import

Added `use futures_util::future::join_all;` to support parallel execution.

## Phase 2 Flow

1. **Parallel GET requests:** All nodes are queried concurrently via `GET /indexes/{uid}/settings`
2. **Hash computation:** Each response is converted to canonical JSON and hashed with SHA256
3. **Hash comparison:** All hashes are compared against the expected fingerprint
4. **Mismatch handling:**
   - On mismatch: Reissue PATCH to mismatched nodes with exponential backoff
   - After max_repair_retries (default 3): Freeze writes on index and raise MiroirSettingsDivergence alert

## Existing Features (Already Implemented)

The following features were already implemented in the codebase:

- **Exponential backoff retry:** `backoff_ms = 1000 * (1u64 << (retry_count - 1).min(5))`
- **Metrics tracking:** All required metrics are implemented in `middleware.rs`
  - `set_settings_broadcast_phase(index, phase)`
  - `inc_settings_hash_mismatch()`
  - `freeze_index_writes(index)`
  - `raise_settings_divergence_alert(index)`
  - `set_settings_version(index, version)`
  - `clear_settings_broadcast_phase(index)`

## Performance Improvement

For a 10-node cluster with 50ms GET latency:
- **Sequential:** 10 × 50ms = 500ms
- **Parallel:** max(50ms) = 50ms
- **Speedup:** 10x for the verify phase

## Testing

All existing tests pass:
- `test_two_phase_settings_broadcast_normal_flow` - ✅
- `test_two_phase_settings_broadcast_hash_mismatch_retry` - ✅
- `test_node_settings_version_tracking_multiple_updates` - ✅
- `test_min_node_version_calculation` - ✅
- `test_settings_version_persistence_to_task_store` - ✅
- `test_two_phase_strategy_config` - ✅
- `test_drift_check_config` - ✅
- `test_legacy_sequential_strategy_compatibility` - ✅

## Integration with Plan §13.5

The implementation follows the plan specification:

```
Phase 2 — Verify (parallel):
    for each node:
        actual      = GET /indexes/{uid}/settings
        actual_hash = sha256(canonical_json(actual))
    all hashes must equal sha256(canonical_json(proposed))

Phase 3 — Commit:
    if verify ok: increment settings_version in task store;
                  stamp X-Miroir-Settings-Version on future responses
    if diverge:   reissue settings with exponential backoff (repair);
                  after max_retries, freeze writes on that index and
                  raise MiroirSettingsDivergence alert.
```

## References

- Plan §13.5: `/home/coding/miroir/docs/plan/plan.md`
- Implementation: `/home/coding/miroir/crates/miroir-proxy/src/routes/indexes.rs:889-945`
- Metrics: `/home/coding/miroir/crates/miroir-proxy/src/middleware.rs:1610-1650`
- Tests: `/home/coding/miroir/crates/miroir-proxy/tests/p5_5_two_phase_settings_broadcast.rs`
