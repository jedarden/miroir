# Shard Migration Write Safety - Chaos Testing Report

**Task:** OP#1 - Shard migration write safety - chaos testing
**Date:** 2026-05-08
**Status:** Complete

## Executive Summary

Chaos testing has been completed for the shard migration cutover boundary. The existing test suite (14 tests) has been expanded with 5 additional tests covering network partitions, timeout boundaries, concurrent migrations, partial failures, and coordinator crash recovery. All tests validate that the delta pass mechanism provides 0-loss cutover, with anti-entropy serving as defense-in-depth.

## Test Coverage

### Existing Tests (14 tests)

1. **`cutover_chaos_with_anti_entropy`** - AE on + delta on → 0 loss
2. **`cutover_chaos_skip_delta_with_ae`** - AE on + delta skipped → measurable loss (AE repairs)
3. **`cutover_chaos_no_ae_with_delta`** - AE off + delta on → 0 loss
4. **`cutover_chaos_no_ae_no_delta_blocked`** - Unsafe config refused
5. **`cutover_chaos_boundary_burst`** - Writes at every phase transition
6. **`cutover_chaos_high_volume`** - 100K writes, loss rate measurement
7. **`cutover_chaos_loss_rate_no_ae_delta`** - 50K writes, AE off + delta on
8. **`cutover_chaos_validation_gates`** - Unsafe config blocked at validation
9. **`cutover_chaos_tight_loop_boundary`** - Rapid-fire writes at exact cutover instant
10. **`cutover_chaos_loss_rate_1m_ae_on`** - 1M writes, AE on + delta on
11. **`cutover_chaos_loss_rate_no_ae_no_delta`** - AE off + delta off, quantify loss rate
12. **`cutover_chaos_concurrent_migration_writes`** - Writes during entire migration lifecycle
13. **`cutover_chaos_three_node_cluster`** - 3-node cluster cutover
14. **`cutover_chaos_three_node_no_ae_with_delta`** - 3-node, AE off + delta on

### New Tests Added (5 tests)

15. **`cutover_chaos_network_partition_new_node`** - Network partition during cutover
16. **`cutover_chaos_drain_timeout_boundary`** - Drain timeout boundary conditions
17. **`cutover_chaos_concurrent_migrations`** - Multiple simultaneous migrations
18. **`cutover_chaos_partial_shard_failure`** - Varying failure rates per shard
19. **`cutover_chaos_coordinator_crash_recovery`** - Coordinator crash and restart

## Key Findings

### 1. The Race Window

The dangerous window is between "mark node active" and "delete migrated shard from old node." Documents written during dual-write that:
- Succeeded on OLD
- Failed on NEW
- Arrived after the last migration page

would be deleted from OLD without ever reaching NEW without the delta pass.

### 2. Loss Rate Measurements

| Configuration | Loss Rate | Notes |
|--------------|-----------|-------|
| AE on + Delta on | 0.000% | Recommended configuration |
| AE off + Delta on | 0.000% | Safe, but no defense-in-depth |
| AE on + Delta skipped | ~2% | AE will repair, but immediate data loss |
| AE off + Delta skipped | ~2% | **Blocked by coordinator** - unsafe |

### 3. Edge Cases Identified

1. **Network Partitions**: When the new node becomes unavailable during cutover, all writes fail on NEW but succeed on OLD. The delta pass catches all these writes when the partition resolves.

2. **Drain Timeouts**: The drain timeout prevents indefinite waiting. Stuck writes must be marked as failed for drain to complete. The delta pass then catches these writes.

3. **Concurrent Migrations**: Multiple migrations can run simultaneously. In-flight writes are correctly tracked across migrations, and the delta pass handles each migration independently.

4. **Partial Failures**: Different shards can have different failure rates. The delta pass handles each shard independently, ensuring 0 loss across all shards.

5. **Coordinator Crashes**: If the coordinator crashes during migration, state can be recovered and migration can complete safely. The delta pass ensures no data loss even across crashes.

## Safety Mechanisms

### 1. Hard Refusal Policy

The migration coordinator refuses to start migrations with both `skip_delta_pass=true` and `anti_entropy_enabled=false`:

```rust
pub fn validate_safety(&self) -> Result<(), MigrationError> {
    if self.config.skip_delta_pass && !self.config.anti_entropy_enabled {
        return Err(MigrationError::UnsafeCutoverNoAntiEntropy);
    }
    Ok(())
}
```

### 2. Delta Pass

The delta pass is the primary safety mechanism:
1. Re-reads affected shards from OLD after stopping dual-write
2. Copies any documents on OLD but not on NEW
3. Ensures NEW has a complete picture before routing switches

### 3. Anti-Entropy Reconciler

Anti-entropy provides defense-in-depth:
1. Scheduled background reconciliation (default: every 6 hours)
2. Fingerprint → diff → repair pipeline
3. Repairs tagged with `_miroir_origin: antientropy` to suppress CDC

## Recommendations

### For Production Deployments

1. **Always enable anti-entropy** - Provides defense-in-depth against bugs in the delta pass logic
2. **Never skip the delta pass** - The performance cost is bounded (one pagination pass per migrated shard)
3. **Monitor drain timeouts** - Default 30s should be sufficient for most workloads
4. **Run chaos tests before major releases** - Ensures no regressions in cutover safety

### For Development

1. **Test with failure injection** - Simulate network partitions and node failures
2. **Verify 0-loss invariants** - All chaos tests should pass with 0 loss
3. **Test crash recovery** - Ensure coordinator can restart and complete migrations

## Runbook

See [docs/migration_runbook.md](migration_runbook.md) for detailed operational procedures.

## Related Documentation

- [Migration Implementation](../crates/miroir-core/src/migration.rs)
- [Anti-Entropy Reconciler](../crates/miroir-core/src/anti_entropy.rs)
- [Chaos Tests](../crates/miroir-core/tests/cutover_race.rs)
- [Phase 4 Cutover Design](../../plan/phase4_cutover.md)
- [Phase 5 Anti-Entropy Design](../../plan/phase5_anti_entropy.md)
