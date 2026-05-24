# Miroir Trade-Offs and Design Decisions

## Resharding (S-Change) vs Node Scaling (N-Change) (Plan §15 OP#3)

### Core Distinction

Miroir supports two orthogonal scaling dimensions with very different cost profiles:

| Dimension | Parameter | What Changes | Routing Impact |
|-----------|-----------|--------------|----------------|
| **Node scaling** | N (node count) | Which nodes host each shard | Rendezvous hash reassigns shards to new/remaining nodes |
| **Resharding** | S (shard count) | How the hash space is divided | Every document's shard assignment changes: `hash(pk) % S` |

**Key insight:** Node scaling is lightweight; resharding is heavy.

### Node Scaling (N-Change)

**When to use:** Add or remove nodes to adjust capacity, throughput, or fault tolerance.

**Cost:** Minimal — only the affected fraction of documents moves.

**Mechanism:**
- Rendezvous hashing assigns shards to nodes based on a stable ordering
- Adding a node: only `~1/(N_new)` of documents move (those whose top-ranked node changes)
- Removing a node: only that node's documents migrate to surviving nodes
- Migration uses dual-write with delta-pass cutover (see migration write safety below)

**Example:** Adding a 5th node to a 4-node group moves ~20% of documents (1/5). The other 80% stay untouched.

**Constraints:** None — fully elastic, can be done anytime.

**When this is insufficient:** When individual shards are too large for a single node, you need more shards (S-change), not more nodes.

### Resharding (S-Change)

**When to use:** Increase the logical shard count when individual shards are too large.

**Cost:** High — transient 2× storage amplification and 2× write amplification during dual-write phase.

**Mechanism:** Six-phase shadow-index operation (§13.1):
1. Shadow create: new index with new S
2. Dual-hash dual-write: every write routes to both old and new S
3. Backfill: stream all documents to shadow index
4. Verify: cross-index PK-set comparison
5. Alias swap: atomic cutover
6. Cleanup: retain old index for rollback, then delete

**Example:** Resharding from S=64 to S=128 temporarily doubles storage from 200 GB to 400 GB (× RG), and write throughput from 4,000 writes/sec to 8,000 writes/sec (× RF × RG).

**Constraints:** Requires significant headroom and off-peak scheduling. See `docs/benchmarks/resharding-load.md` for empirical data.

**When to avoid:** If you can instead add nodes to your current shard layout. Prefer N-change over S-change.

### Decision Matrix

| Symptom | Solution | Why |
|---------|----------|-----|
| Cluster CPU/memory saturated | Add nodes (N-change) | Spreads load across more machines |
| Shard too large for one node | Reshard (S-change) | Need more hash buckets to split large shards |
| Need more write throughput | Add replica groups (RG-scale) | Groups are independent; more groups = parallel writes |
| Need more read throughput | Add nodes or groups | Both help; groups add fault tolerance too |
| Individual shard > node disk | Reshard (S-change) | Only S-change reduces per-shard size |

### Capacity Planning Guidance

**Choosing S at index creation:**
```
S = max_nodes_per_group_ever × 8
```

This formula ensures you never need to reshard as your cluster grows within a group. Each group's rendezvous assignment is scoped to its own node list, so adding groups doesn't consume S headroom.

**Example:** A cluster starting at 2 nodes per group that might grow to 60 nodes per group should use S ≥ 480 (or 512 for a round power of two). Node fleet elasticity is unlimited within that S.

**Why the "× 8" factor?**
- Provides enough shard granularity to evenly distribute documents as nodes are added
- Avoids hot spots where some nodes host many large shards and others host few small ones
- Each node typically hosts 8-16 shards, which balances per-shard metadata overhead with distribution quality

**If you must reshard:**
1. Schedule during off-peak hours (use `miroir-ctl reshard start --schedule-window off-peak`)
2. Set backfill throttle conservatively: aim for peak total writes ≤ 3× normal
3. Ensure 2× storage headroom before starting
4. Monitor progress via `miroir-ctl reshard status`

### Empirical Validation

See `docs/benchmarks/resharding-load.md` for comprehensive benchmark results:
- Storage amplification: exactly 2.0× (all scenarios)
- Dual-write amplification: exactly 2.0× (all scenarios)
- Peak write amplification: varies by corpus and throttle (can exceed 500× for low-write corpora with aggressive backfill)

**Bottom line:** The "choose S generously" guidance remains the recommended default because online resharding is expensive. Treat §13.1 as a remediation path, not a license to under-provision.

---

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
