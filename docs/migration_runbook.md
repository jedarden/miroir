# Shard Migration Runbook

This runbook provides operational guidance for safely performing shard migrations in the miroir system.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Pre-Migration Checklist](#pre-migration-checklist)
3. [Migration Procedure](#migration-procedure)
4. [Anti-Entropy Configurations](#anti-entropy-configurations)
5. [Rollback Procedures](#rollback-procedures)
6. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)

---

## Prerequisites

### System Requirements

- **Cluster Health**: All nodes must be healthy before starting migration
- **Capacity**: New node must have sufficient capacity for migrated shards
- **Network**: Stable network between all nodes
- **Anti-Entropy**: Recommended to be enabled (see [Configurations](#anti-entropy-configurations))

### Configuration

```toml
# Migration configuration
[migration]
drain_timeout = "30s"           # Maximum time to wait for in-flight writes
skip_delta_pass = false          # Always false for safety
anti_entropy_enabled = true      # Recommended: true

# Anti-entropy configuration
[anti_entropy]
enabled = true                   # Recommended: true
schedule_cron = "0 */6 * * *"    # Every 6 hours
shards_per_pass = 0              # 0 = all shards
max_read_concurrency = 2
fingerprint_batch_size = 1000
auto_repair = true
```

---

## Pre-Migration Checklist

### 1. Cluster Health Check

```bash
# Check cluster health
miroir-ctl cluster health

# Verify all nodes are healthy
miroir-ctl nodes list

# Check current shard distribution
miroir-ctl shards list
```

**Expected Result**: All nodes show `Healthy` status, no failed shards.

### 2. Capacity Planning

```bash
# Estimate storage requirements
miroir-ctl reshard simulate \
  --index products \
  --new-shards 256 \
  --docs-avg-size 10kb
```

**Expected Result**: New node has sufficient capacity for migrated shards + 20% buffer.

### 3. Backup Verification

```bash
# Verify backups are current
miroir-ctl backup status

# Check last backup time
miroir-ctl backup list | tail -1
```

**Expected Result**: Last backup completed within RPO window.

### 4. Anti-Entropy Status

```bash
# Check anti-entropy status
miroir-ctl anti-entropy status

# Verify last run
miroir-ctl anti-entropy history | tail -5
```

**Expected Result**: Anti-entropy enabled, last run completed successfully.

### 5. Schedule Window Check

```bash
# Verify current time is within allowed window
miroir-ctl reshard check-window \
  --schedule-window off-peak
```

**Expected Result**: Current time is within allowed window (or use `--force` if emergency).

---

## Migration Procedure

### Step 1: Initiate Migration

```bash
miroir-ctl reshard start \
  --index products \
  --new-shards 256 \
  --throttle 10000 \
  --schedule-window off-peak
```

**Expected Output**:
```
Migration started: ID=42
Phase: ComputingAssignments
Affected shards: 64 (old nodes: old-0, old-1)
```

### Step 2: Monitor Dual-Write Phase

```bash
# Watch migration progress
miroir-ctl reshard watch --index products

# Check in-flight writes
miroir-ctl reshard stats --index products
```

**Expected Behavior**:
- Dual-write active to both old and new nodes
- Background migration copying documents
- Storage amplification = 2.0× (expected)
- Write latency increased by ~10-20%

**Warning Signs**:
- Write latency > 2× baseline
- High failure rate on new node
- Background migration stuck

### Step 3: Initiate Cutover

```bash
# When background migration completes, initiate cutover
miroir-ctl reshard cutover --index products
```

**Expected Behavior**:
1. **CutoverBegin**: Background migration complete
2. **CutoverDraining**: Waiting for in-flight writes (≤ 30s)
3. **CutoverDeltaPass**: Re-reading source shards for stragglers
4. **CutoverActivate**: New node active, routing switched
5. **CutoverCleanup**: Old shard data deleted

### Step 4: Verify Migration

```bash
# Verify migration completed
miroir-ctl reshard status --index products

# Check for data loss
miroir-ctl anti-entropy verify --index products --shards 0-63

# Verify routing
miroir-ctl routing test --index products --samples 1000
```

**Expected Result**:
- Status: `Complete`
- Data loss: 0 documents
- Routing: 100% to new node for migrated shards

### Step 5: Post-Migration Cleanup

```bash
# Trigger anti-entropy pass to verify
miroir-ctl anti-entropy run --index products --shards 0-63

# Monitor cluster health
miroir-ctl cluster health

# Verify storage reclaimed
miroir-ctl nodes stats --node old-0
```

**Expected Result**:
- Anti-entropy finds 0 divergences
- Cluster healthy
- Old node storage decreased by migrated shard size

---

## Anti-Entropy Configurations

### Configuration A: Anti-Entropy Enabled (Recommended)

**Safety**: 0-loss with defense-in-depth
**Performance**: Minor overhead (6-hourly reconciliation)

```toml
[migration]
drain_timeout = "30s"
skip_delta_pass = false          # Delta pass provides primary safety
anti_entropy_enabled = true      # AE provides defense-in-depth

[anti_entropy]
enabled = true
schedule_cron = "0 */6 * * *"
auto_repair = true
```

**Migration Flow**:
1. Dual-write + background migration
2. Stop dual-write, drain in-flight writes
3. Delta pass catches stragglers → 0 loss
4. Anti-entropy scheduled to catch any bugs in delta pass
5. New node active, routing switched

**Recovery**: If delta pass has bugs, anti-entropy will repair within 6 hours.

### Configuration B: Anti-Entropy Disabled (Not Recommended)

**Safety**: 0-loss IF delta pass works correctly
**Performance**: No background reconciliation overhead

```toml
[migration]
drain_timeout = "30s"
skip_delta_pass = false          # Delta pass is ONLY safety mechanism
anti_entropy_enabled = false     # No defense-in-depth
```

**Migration Flow**:
1. Dual-write + background migration
2. Stop dual-write, drain in-flight writes
3. Delta pass catches stragglers → 0 loss (IF no bugs)
4. New node active, routing switched
5. NO background reconciliation

**Warning**: Any bugs in delta pass logic will cause permanent data loss.

**Recommendation**: Only use this configuration if:
- You have comprehensive test coverage
- You can tolerate potential data loss
- You run chaos tests before every deployment

### Configuration C: Skip Delta Pass (Only with AE Enabled)

**Safety**: 0-loss after anti-entropy runs (up to 6 hours)
**Performance**: Faster cutover, but immediate data loss until AE runs

```toml
[migration]
drain_timeout = "30s"
skip_delta_pass = true           # Skip delta pass
anti_entropy_enabled = true      # AE is ONLY safety mechanism

[anti_entropy]
enabled = true
schedule_cron = "0 */6 * * *"    # Or more frequent
auto_repair = true
```

**Migration Flow**:
1. Dual-write + background migration
2. Stop dual-write, drain in-flight writes
3. NO delta pass → stragglers lost
4. New node active, routing switched
5. Anti-entropy repairs within 6 hours

**Warning**: Documents will be lost for up to 6 hours (until AE runs).

**Recommendation**: Only use this configuration if:
- You can tolerate temporary data loss
- You need faster cutover
- You increase AE frequency to hourly or less

---

## Rollback Procedures

### Scenario 1: Migration Failed During Dual-Write

**Symptoms**: High failure rate on new node, migration stuck

**Action**: Abort and retry

```bash
# Abort migration
miroir-ctl reshard abort --index products

# Verify old node still serving
miroir-ctl routing test --index products

# Retry after fixing issue
miroir-ctl reshard start --index products ...
```

**Data Loss**: 0 (old node still serving)

### Scenario 2: Migration Failed During Cutover

**Symptoms**: Drain timeout, delta pass failed

**Action**: Manual intervention required

```bash
# Check migration state
miroir-ctl reshard status --index products

# If drain timeout, check for stuck writes
miroir-ctl writes list --stuck

# Mark stuck writes as failed
miroir-ctl writes fail --doc-id <id> --reason "timeout"

# Retry cutover
miroir-ctl reshard cutover --index products
```

**Data Loss**: 0 (delta pass will catch stragglers)

### Scenario 3: Migration Failed After Activation

**Symptoms**: New node not serving, routing issues

**Action**: Emergency rollback

```bash
# Stop new node
miroir-ctl nodes drain --node new-3

# Revert routing to old node
miroir-ctl routing revert --index products --shards 0-63

# Verify data integrity
miroir-ctl anti-entropy run --index products --shards 0-63
```

**Data Loss**: Potential (if delta pass missed stragglers)

---

## Monitoring and Troubleshooting

### Key Metrics

| Metric | Healthy | Warning | Critical |
|--------|---------|---------|----------|
| Write latency | < 2× baseline | 2-5× baseline | > 5× baseline |
| In-flight writes | < 1000 | 1000-10000 | > 10000 |
| Drain time | < 10s | 10-30s | > 30s |
| Delta pass docs | < 100 | 100-1000 | > 1000 |
| AE divergences | 0 | 1-10 | > 10 |

### Troubleshooting Guide

> **For comprehensive troubleshooting**: See the [Troubleshooting Guide](../troubleshooting.md) for common issues and the [Diagnostic Playbook](diagnostics.md) for systematic diagnosis.

#### High Write Latency

**Symptoms**: Write latency increased by > 2× during dual-write

**Diagnosis**:
```bash
# Check write paths
miroir-ctl tracing list --operation write

# Check node health
miroir-ctl nodes health --detailed
```

**Solutions**:
- Reduce throttle rate
- Check network latency
- Verify new node capacity

#### Drain Timeout

**Symptoms**: Migration stuck at CutoverDraining phase

**Diagnosis**:
```bash
# Check stuck writes
miroir-ctl writes list --stuck

# Check drain timeout
miroir-ctl reshard config --show drain_timeout
```

**Solutions**:
- Mark stuck writes as failed
- Increase drain timeout
- Check for network issues

#### High Delta Pass Count

**Symptoms**: Delta pass copying > 1000 documents

**Diagnosis**:
```bash
# Check delta pass details
miroir-ctl reshard status --index products --show delta

# Check dual-write failure rate
miroir-ctl reshard stats --index products --show failures
```

**Solutions**:
- Investigate dual-write failures
- Check new node health
- Verify network stability

#### Anti-Entropy Divergences

**Symptoms**: Anti-entropy finding divergences after migration

**Diagnosis**:
```bash
# Check AE details
miroir-ctl anti-entropy history --index products --detailed

# Check specific shards
miroir-ctl anti-entropy verify --index products --shards 0-63
```

**Solutions**:
- Run AE with auto-repair
- Investigate delta pass logic
- Review migration logs

---

## Emergency Contacts

| Role | Contact |
|------|---------|
| On-call Engineer | on-call@miroir.io |
| Database Lead | db-lead@miroir.io |
| Infrastructure Lead | infra-lead@miroir.io |

---

## Related Documentation

- [Chaos Testing Report](chaos_testing_report.md)
- [Migration Implementation](../crates/miroir-core/src/migration.rs)
- [Anti-Entropy Reconciler](../crates/miroir-core/src/anti_entropy.rs)
- [Phase 4 Cutover Design](../../plan/phase4_cutover.md)
- [Phase 5 Anti-Entropy Design](../../plan/phase5_anti_entropy.md)
