# miroir-r3j.6: Task Registry TTL Pruner - Verification

## Task
P3.6 Task registry TTL pruner (in-memory for Phase 3; Mode A in Phase 6)

## Status: ALREADY IMPLEMENTED

The task registry TTL pruner is fully implemented and integrated. All acceptance tests pass.

## Implementation Location

### Core Pruner (`crates/miroir-core/src/task_pruner.rs`)
- `prune_once(store, cfg)` - Single iteration with advisory lock
- `spawn_pruner(store, cfg)` - Background thread spawning
- `PrunerHandle` - Clean shutdown via Drop
- `task_registry_size()` - Read the current gauge value
- `TASK_REGISTRY_SIZE` - Static AtomicU64 gauge (Prometheus-compatible)

### Database Operations (`crates/miroir-core/src/task_store/sqlite.rs`)
- `prune_tasks(cutoff_ms, batch_size)` - Delete old terminal tasks (lines 652-664)
  - Uses subquery with LIMIT for SQLite compatibility
  - Preserves `processing` status tasks
  - Deletes only `succeeded`, `failed`, `canceled` tasks older than cutoff
- `task_count()` - Count total rows for the gauge (lines 666-670)

### Configuration (`crates/miroir-core/src/config.rs`)
```rust
pub struct TaskRegistryConfig {
    pub ttl_seconds: u64,        // 7 days default
    pub prune_interval_s: u64,   // 5 min default
    pub prune_batch_size: u32,   // 10k rows default
    // ... other fields
}
```

### Startup Integration (`crates/miroir-proxy/src/main.rs`, lines 387-402)
```rust
// Start task registry TTL pruner background task (plan §4, Phase 3)
if let Some(ref store) = state.admin.task_store {
    let store = store.clone();
    let pruner_config = config.task_registry.clone();
    tokio::spawn(async move {
        let _pruner_handle = task_pruner::spawn_pruner(store, pruner_config);
        info!("task registry TTL pruner started");
        std::future::pending::<()>().await;
    });
}
```

### Prometheus Metrics (`crates/miroir-proxy/src/middleware.rs`)
- `miroir_task_registry_size` gauge (line 382-383)
- Updated by health checker on interval (main.rs:702)
- Pruner updates TASK_REGISTRY_SIZE after each cycle (task_pruner.rs:96)

## Acceptance Tests - All Pass

```
test task_pruner::tests::pruner_deletes_10k_old_terminal_tasks ... ok
test task_pruner::tests::pruner_preserves_processing_tasks ... ok
test task_pruner::tests::advisory_lock_prevents_concurrent_pruning ... ok
test task_pruner::tests::gauge_drops_after_prune ... ok
test task_pruner::tests::pruner_batches_correctly ... ok
test task_pruner::tests::spawn_pruner_runs_and_stops ... ok
test task_pruner::tests::pruner_handle_drop_stops_thread ... ok
```

## Phase 6 Note

The pruner uses advisory locks (`leader_lease` table) for single-pod coordination in Phase 3.
Phase 6 §14.5 Mode A will replace this with rendezvous-partitioned ownership without
changing the `prune_tasks()` API — only the lock acquisition mechanism changes.

## Verification Date
2026-05-20
