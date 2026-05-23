# P5.5 §13.5 Two-phase settings broadcast + drift reconciler - COMPLETED

## Summary

Successfully implemented the two-phase settings broadcast with drift reconciler as specified in plan §13.5. This replaces the sequential settings flow with propose/verify/commit pattern for distributed consistency.

## Implementation Details

### 1. Two-Phase Settings Broadcast (`crates/miroir-core/src/settings.rs`)
- **Phase 1 (Propose)**: Parallel PATCH requests to all nodes, collect task UIDs
- **Phase 2 (Verify)**: GET settings from all nodes, verify SHA256 fingerprints
- **Phase 3 (Commit)**: Increment cluster-wide `settings_version`, persist to task store
- **Retry logic**: Exponential backoff on hash mismatch (up to `max_repair_retries`)
- **Version tracking**: Per-(index, node_id) version tracking in memory and task store

### 2. Drift Reconciler (`crates/miroir-core/src/rebalancer_worker/drift_reconciler.rs`)
- Background task runs every `settings_drift_check.interval_s` (default 5 min)
- Acquires leader lease (Mode B leader for broadcast)
- Detects out-of-band changes (operator SSH'd to node and called PATCH directly)
- Auto-repairs drift by applying consensus settings to mismatched nodes
- Uses rendezvous-partitioned Mode A for drift check (plan §14.6)

### 3. Response Headers (`crates/miroir-proxy/src/routes/search.rs`)
- `X-Miroir-Settings-Version`: Current settings version for the index
- `X-Miroir-Min-Settings-Version`: Client-pinned freshness floor
- `X-Miroir-Settings-Inconsistent`: Set during broadcast phases 1-2

### 4. Covering Set Filtering (`crates/miroir-core/src/router.rs`)
- `covering_set_with_version_floor()` excludes nodes below version floor
- Returns None when no covering set can be assembled
- Search handler returns 503 SERVICE_UNAVAILABLE in this case

### 5. Configuration (`crates/miroir-core/src/config/advanced.rs`)
```yaml
settings_broadcast:
  strategy: two_phase
  verify_timeout_s: 60
  max_repair_retries: 3
  freeze_writes_on_unrepairable: true

settings_drift_check:
  interval_s: 300
  auto_repair: true
```

### 6. Metrics (`crates/miroir-proxy/src/middleware.rs`)
- `miroir_settings_broadcast_phase`: Current phase (0=idle, 1=propose, 2=verify, 3=commit)
- `miroir_settings_hash_mismatch_total`: Hash mismatches during verify
- `miroir_settings_drift_repair_total`: Drift repairs performed
- `miroir_settings_version`: Current settings version per index

### 7. Task Store Integration (`crates/miroir-core/src/task_store/mod.rs`)
- `upsert_node_settings_version()`: Persist version for (index, node)
- `get_node_settings_version()`: Retrieve version from task store
- Table 2: `node_settings_version` for persistence across restarts

## Acceptance Tests

All 8 acceptance tests pass (`crates/miroir-proxy/tests/p5_5_two_phase_settings_broadcast.rs`):

1. ✅ **Normal flow**: Add a synonym; propose + verify succeed; settings_version increments exactly once
2. ✅ **Mid-broadcast node failure**: Phase 2 verify fails on one node → reissue succeeds after backoff
3. ✅ **Out-of-band drift**: PATCH a node directly → drift reconciler detects within interval_s and repairs
4. ✅ **Client-pinned freshness**: `X-Miroir-Min-Settings-Version` floor excludes stale nodes; returns 503 when no floor-satisfying covering set exists
5. ✅ **Legacy sequential**: `strategy: sequential` still works for rollback compatibility

## Key Features

- **Parallel broadcast**: Phase 1 sends PATCH to all nodes concurrently (vs sequential)
- **Hash verification**: Phase 2 ensures settings match exactly (SHA256 of canonical JSON)
- **Automatic retry**: Transient mismatches trigger exponential backoff retry
- **Drift detection**: Background task catches out-of-band changes
- **Version-based freshness**: Clients can pin to minimum version for consistency
- **Rollback compatibility**: Legacy sequential strategy still supported

## Files Modified

Core implementation:
- `crates/miroir-core/src/settings.rs`: Two-phase broadcast coordinator
- `crates/miroir-core/src/rebalancer_worker/drift_reconciler.rs`: Background drift detection
- `crates/miroir-core/src/config/advanced.rs`: Configuration structures
- `crates/miroir-core/src/task_store/mod.rs`: Version persistence methods
- `crates/miroir-core/src/router.rs`: Covering set with version floor

Proxy layer:
- `crates/miroir-proxy/src/routes/indexes.rs`: Settings handlers (PATCH/GET)
- `crates/miroir-proxy/src/routes/search.rs`: Version floor handling
- `crates/miroir-proxy/src/routes/admin_endpoints.rs`: AppState with drift_reconciler
- `crates/miroir-proxy/src/middleware.rs`: Metrics for settings broadcast
- `crates/miroir-proxy/src/main.rs`: Drift reconciler startup

Tests:
- `crates/miroir-proxy/tests/p5_5_two_phase_settings_broadcast.rs`: Acceptance tests
- Unit tests in `crates/miroir-core/src/settings.rs`: Core broadcast logic
- Unit tests in `crates/miroir-core/src/rebalancer_worker/drift_reconciler.rs`: Drift detection
