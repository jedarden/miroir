# P5.5.b: Verify Phase Implementation

## Summary

The verify phase of 2PC (two-phase commit) for settings broadcast is fully implemented in `crates/miroir-proxy/src/routes/indexes.rs` in the `two_phase_settings_broadcast` function.

## Implementation Details

### Phase 2: Verify (lines 887-1056)

The verify phase performs the following steps:

1. **Read-back from all nodes**: For each node in parallel:
   - `GET /indexes/{uid}/settings` to fetch the actual settings
   - Compute `actual_hash = sha256(canonical_json(actual))`
   - The canonical JSON ensures key-order independence

2. **Hash comparison**: 
   - All node hashes must equal `sha256(canonical_json(proposed))`
   - The `fingerprint_settings` function (lines 277-305) computes canonical JSON hashes

3. **Exponential backoff repair** (lines 946-1056):
   - On hash mismatch, identify mismatched nodes
   - Reissue PATCH to mismatched nodes only (repair)
   - Retry with exponential backoff: `2^(retry_count-1)` seconds (max 60s)
   - Re-verify after each repair attempt

4. **Failure handling** (lines 960-978):
   - After `max_repair_retries` (default 3): freeze writes on index
   - Raise `MiroirSettingsDivergence` alert
   - Abort the broadcast

### Configuration

The behavior is controlled by `SettingsBroadcastConfig` in `crates/miroir-core/src/config/advanced.rs`:

```rust
pub struct SettingsBroadcastConfig {
    pub verify_timeout_s: u64,           // Default: 60
    pub max_repair_retries: u32,         // Default: 3
    pub freeze_writes_on_unrepairable: bool,  // Default: true
}
```

### Test Coverage

All 8 tests in `crates/miroir-proxy/tests/p5_5_two_phase_settings_broadcast.rs` pass:
- `test_two_phase_settings_broadcast_normal_flow`: Normal flow verification
- `test_two_phase_settings_broadcast_hash_mismatch_retry`: Hash mismatch with retry
- `test_node_settings_version_tracking_multiple_updates`: Version tracking
- `test_min_node_version_calculation`: Min version calculation
- `test_settings_version_persistence_to_task_store`: Persistence
- `test_drift_check_config`: Drift check configuration
- `test_two_phase_strategy_config`: 2PC strategy configuration
- `test_legacy_sequential_strategy_compatibility`: Legacy fallback

## Code Fix

A minor fix was applied to avoid unnecessary String clone in hash comparison (line 991):
- Before: `filter(|(_, hash)| *hash != expected_fingerprint)`
- After: `filter(|(_, hash)| hash.as_str() != expected_fingerprint)`

## Anti-Entropy Worker

The anti-entropy worker (`crates/miroir-core/src/rebalancer_worker/anti_entropy_worker.rs`) was added to perform periodic drift detection and repair, complementing the real-time verification in 2PC.
