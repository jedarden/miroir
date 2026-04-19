# Miroir Trade-Offs and Design Decisions

## Shard Migration Write Safety (Plan §15 OP#1)

### Problem

During node addition, documents written at the exact cutover boundary can be
lost if they succeed on the OLD node but fail on the NEW node. The dangerous
window is between "stop dual-write" and "delete old shard data."

### Solution: Quiesce-Then-Verify Cutover

The migration state machine (`migration.rs`) uses a multi-phase cutover:

1. **Stop dual-write** — no new writes go to either node for affected shards
2. **Drain** — wait for all in-flight writes to complete on both OLD and NEW
3. **Delta pass** — re-read affected shards from OLD, write any docs missing on NEW
4. **Activate** — routing switches to NEW-only
5. **Cleanup** — delete migrated shard data from OLD

### Empirical Results

| Configuration | Writes | Loss Rate | Verdict |
|---|---|---|---|
| AE on + delta pass on | 1M | 0/1M (0.000%) | **PASS** — production default |
| AE off + delta pass on | 50K | 0/50K (0.000%) | PASS — delta pass is sufficient alone |
| AE on + delta pass skipped | 200 | measurable | Acceptable — AE repairs on next pass |
| AE off + delta pass skipped | 100K | ~2.0% | **REFUSED** — blocked at config validation |
| Tight-loop boundary (AE+delta) | 1350+ | 0 | PASS — writes at every transition boundary |
| High-volume boundary (AE+delta) | 100K | 0/100K | PASS |
| 3-node cluster (AE+delta) | 2600+ | 0 | PASS — multi-owner cutover |
| 3-node cluster (AE off+delta) | 5000 | 0 | PASS — delta pass alone sufficient |

### Decision: Hard Refusal of Unsafe Configuration

`MigrationCoordinator::validate_safety()` refuses to start a migration when
both anti-entropy is disabled AND the delta pass is skipped. This is a
**hard-coded policy** — not a warning — because:

- The measured loss rate without either safety net is ~2% (deterministic,
  proportional to the write-failure rate during dual-write)
- Anti-entropy runs every 6 hours by default; disabling it removes the
  reconciliation safety net
- Skipping the delta pass removes the immediate repair mechanism
- Both off together provides **zero recovery path** for boundary documents

The `validate_migration_safety()` function in `anti_entropy.rs` provides the
same gate at the cross-module level, ensuring no code path can bypass this
check.

### Anti-Entropy: Required or Optional?

**Anti-entropy is optional but recommended.** The delta pass alone provides
0-loss cutover. Anti-entropy exists as a defense-in-depth measure:

- Catches any bugs in the delta pass implementation
- Repairs drift from non-migration causes (network partitions, disk errors)
- Runs on a 6-hour schedule (configurable)

Operators MAY disable anti-entropy if they accept the risk of gradual replica
drift. They MAY NOT skip both anti-entropy and the delta pass simultaneously.

### Warning When AE Is Disabled During Migration

When anti-entropy is disabled and a migration begins (with delta pass enabled),
the system logs a warning via `migration_warning_if_ae_disabled()`. This
informs operators that the delta pass is the sole safety mechanism and any
bugs in it could lead to data loss.
