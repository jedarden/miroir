# P4.1 Rebalancer Background Worker - Implementation Summary

## Task Completed

The rebalancer background worker with advisory lock was already implemented in the codebase. Verified all acceptance criteria pass.

## Implementation Location

- `crates/miroir-core/src/rebalancer_worker/mod.rs` - Main worker implementation
- `crates/miroir-core/src/rebalancer_worker/acceptance_tests.rs` - Acceptance tests

## Key Components

1. **Advisory Lock** (Leader Lease)
   - Uses `try_acquire_leader_lease` with scope `rebalance:<index>`
   - Only one pod can hold the lease at a time
   - Lease renewal every 2 seconds (configurable)
   - TTL of 10 seconds (configurable)

2. **Topology Change Events**
   - `NodeAdded` - Triggers shard migration to new node
   - `NodeDraining` - Triggers shard migration away from draining node
   - `NodeFailed` - Marks node as failed
   - `NodeRecovered` - Marks node as active

3. **Shard Migration State Machine**
   ```
   Idle → DualWriteStarted → MigrationInProgress → MigrationComplete
   → DualWriteStopped → OldReplicaDeleted → Idle
   ```

4. **Progress Persistence**
   - Jobs persisted to `jobs` table in task store
   - Each shard tracks: phase, docs_migrated, last_offset
   - `load_persisted_jobs()` loads state on startup

5. **Metrics (Plan §10)**
   - `miroir_rebalance_in_progress` - Gauge (0 or 1)
   - `miroir_rebalance_documents_migrated_total` - Counter (monotonically increasing)
   - `miroir_rebalance_duration_seconds` - Histogram (per-shard migration time)

## Acceptance Tests Verified

1. **P4.1-A1**: Advisory lock prevents duplicate migrations ✓
2. **P4.1-A2**: Progress persistence allows pod restart resumption ✓
3. **P4.1-A3**: Metrics monotonically increase ✓
4. **P4.1-A4**: Two workers produce 0 duplicate migrations ✓

## Integration

- Started as background task in `main.rs` (line 320-337)
- Loads persisted jobs on startup
- Metrics callback wired up in `admin_endpoints.rs`
- Health checker syncs metrics to Prometheus

## Configuration

```rust
RebalancerWorkerConfig {
    max_concurrent_migrations: 4,  // Plan §14.2 memory budget
    lease_ttl_secs: 10,
    lease_renewal_interval_ms: 2000,
    migration_batch_size: 1000,
    migration_batch_delay_ms: 100,
    event_channel_capacity: 100,
}
```

## Test Results

All 24 rebalancer worker tests pass:
- 4 acceptance tests (P4.1-A1 through P4.1-A4)
- 6 anti-entropy worker tests
- 7 settings broadcast tests
- 7 other unit tests
