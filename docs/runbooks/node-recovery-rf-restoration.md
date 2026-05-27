# Node Recovery and RF Restoration Runbook

> Runbook for recovering failed nodes and restoring replication factor within replica groups.
> Part of plan §2 — Topology changes (unplanned node failure recovery).

## Overview

When a node fails, Miroir automatically detects the failure and stops routing writes to it. For clusters with `replication_factor > 1`, surviving replicas continue serving reads. This runbook covers the recovery process and automatic RF (replication factor) restoration.

## Prerequisites

- Miroir cluster with `replication_factor > 1` (recommended for production)
- Failed node pod can be restarted or replaced
- Network connectivity between nodes
- Sufficient capacity on surviving nodes for temporary cross-group fallback

## Node Failure Behavior

### What Happens Automatically

1. **Health check detects failure** (health check interval: 10s)
   - Node marked as `Failed` in topology
   - Writes stop routing to the failed node
   - Alerts fired (if configured)

2. **Read behavior during failure**
   - If RF > 1 within the group: surviving replicas serve reads
   - If the failed node held the only intra-group replica for a shard: reads fall back to a healthy group

3. **Write behavior during failure**
   - Writes continue to healthy nodes
   - RF degradation occurs for shards assigned to the failed node

## Node Recovery Procedure

### Option 1: Pod Restart (Same PVC)

Use this when the pod is crashed but the PVC is healthy.

```bash
# Get the statefulset and pod name
kubectl get statefulset -n <namespace>
kubectl get pods -n <namespace> -l app=meilisearch

# Delete the failed pod - StatefulSet will recreate it with the same PVC
kubectl delete pod <failed-pod-name> -n <namespace>

# Watch for the pod to restart
kubectl get pods -n <namespace> -w
```

### Option 2: PVC Replacement (Data Loss)

Use this when the PVC is corrupted. The node will rehydrate from peer replicas via RF restoration.

```bash
# Get the PVC name
kubectl get pvc -n <namespace> -l app=meilisearch

# Delete the PVC AND the pod
kubectl delete pvc <pvc-name> -n <namespace>
kubectl delete pod <failed-pod-name> -n <namespace>

# The StatefulSet will create a new PVC and pod
# Watch for the pod to start
kubectl get pods -n <namespace> -w
```

### Option 3: Node Replacement (New Node)

Use this when replacing hardware or migrating to a new node.

```bash
# Add the new node via miroir-ctl
miroir-ctl node add \
  --id <new-node-id> \
  --address <new-node-address> \
  --replica-group <group-id>

# Remove the failed node
miroir-ctl node remove <failed-node-id>
```

## RF Restoration Process

Once the node recovers (pod restarts with healthy PVC or new PVC is created), RF restoration happens **automatically**:

### Phase 1: Node Marked as Restoring

```bash
# Check node status
miroir-ctl node status <node-id>

# Expected output:
# Status: restoring
# RF Restoration Progress:
#   Shards: 0/64
#   Documents Migrated: 0
#   Progress: 0.0%
```

During this phase:
- Node accepts **writes only** (dual-write from source replicas)
- Node does **not serve reads**
- Background replication copies data from surviving replicas

### Phase 2: Background Replication

For each shard that the recovered node should own:
1. Miroir identifies a healthy source replica within the same group
2. Documents are paged using `filter=_miroir_shard={id}` to avoid full scans
3. Documents are written to the recovered node
4. Progress is tracked in the node state machine

```bash
# Monitor progress
watch -n 5 'miroir-ctl node status <node-id>'
```

### Phase 3: Cutover to Active

Once all shards are replicated:
- Node status transitions from `Restoring` → `Active`
- Node begins serving reads for its assigned shards
- Cross-group fallback (if any) is no longer needed
- Normal RF is restored within the group

```bash
# Verify node is active
miroir-ctl node status <node-id>

# Expected output:
# Status: active
# (No RF restoration progress section)
```

## Timing Estimates

| Cluster Size | RF | Data Size | Est. Restore Time |
|--------------|-----|-----------|-------------------|
| 3 nodes      | 2   | 10 GB     | 5-15 minutes      |
| 3 nodes      | 2   | 100 GB    | 30-60 minutes     |
| 5 nodes      | 3   | 100 GB    | 20-40 minutes     |
| 5 nodes      | 3   | 1 TB      | 3-6 hours         |

**Factors affecting restore time:**
- Network bandwidth between nodes
- Document size and count
- `migration_batch_size` configuration (default: 1000)
- `migration_batch_delay_ms` throttling (default: 0ms)

## Monitoring

### Key Metrics

```bash
# Check cluster health
miroir-ctl cluster health

# Check all node statuses
miroir-ctl nodes list

# Check specific node status with RF restore progress
miroir-ctl node status <node-id>

# Check rebalancer status
miroir-ctl rebalance status
```

### Prometheus Metrics

If Prometheus scraping is enabled:

```promql
# Active rebalance jobs
miroir_rebalancer_active_jobs

# Documents migrated
miroir_rebalancer_docs_migrated_total

# Rebalance duration
miroir_rebalancer_duration_seconds
```

## Troubleshooting

### RF Restoration Stuck

**Symptom:** Node stays in `Restoring` status, progress not advancing.

**Diagnosis:**
```bash
# Check rebalancer status for errors
miroir-ctl rebalance status

# Check node logs
kubectl logs <pod-name> -n <namespace> | grep -i "rf.restore\|restoration"

# Check for migration errors
kubectl logs <pod-name> -n <namespace> | grep -i "migration.*failed"
```

**Common Causes:**
1. **Source replica unavailable** - No healthy source in the same group
   - **Solution:** Recover another node in the group first, or add a temporary node

2. **Network issues** - High latency or packet loss between nodes
   - **Solution:** Check network connectivity, `kubectl exec -it <pod> -- ping <other-pod>`

3. **Insufficient capacity** - Target node disk full
   - **Solution:** Check PVC usage, expand if needed

4. **Rebalancer worker not running** - Crash or panic
   - **Solution:** Check proxy pod logs, restart if needed

### Node Never Transitions to Active

**Symptom:** RF restoration shows 100% but node stays `Restoring`.

**Diagnosis:**
```bash
# Verify all shards are complete
miroir-ctl rebalance status

# Check for straggler shards
kubectl logs <pod-name> -n <namespace> | grep "shard.*complete"
```

**Solution:** This should not happen in normal operation. If it does:
1. Check the rebalancer worker logs for errors
2. Try marking the node active manually (last resort):
   ```bash
   # This bypasses safety checks - only do this if you're certain restoration is complete
   kubectl exec -it <proxy-pod> -- curl -X POST "http://localhost:7700/_miroir/nodes/<node-id>/activate"
   ```

### Data Loss After Recovery

**Symptom:** Document count is lower after recovery.

**Diagnosis:**
```bash
# Run anti-entropy verification
miroir-ctl anti-entropy verify --index <index-uid> --shards 0-63

# Check for divergences
miroir-ctl anti-entropy status
```

**Solution:** Run anti-entropy repair:
```bash
miroir-ctl anti-entropy run --index <index-uid> --auto-repair
```

### Recovery Takes Too Long

**Symptom:** RF restoration progressing slower than expected.

**Diagnosis:**
```bash
# Check migration batch size and delay
kubectl exec -it <proxy-pod> -- env | grep MIGRATION

# Check network bandwidth
kubectl exec -it <pod> -- curl -o /dev/null -s -w "%{speed_download}\n" http://<other-pod>:7700/health
```

**Solutions:**
1. Increase `migration_batch_size` (default: 1000) via config
2. Decrease `migration_batch_delay_ms` (default: 0) to reduce throttling
3. Check for network throttling on the pod
4. Verify disk I/O is not saturated

## Configuration

### Relevant Settings

```toml
[rebalancer]
# Maximum concurrent migrations (shards) per job
max_concurrent_migrations = 5

# Migration batch size (documents per page)
migration_batch_size = 1000

# Delay between batches (ms) - 0 = no throttling
migration_batch_delay_ms = 0

# Auto-rebalance on node recovery
auto_rebalance_on_recovery = true

[migration]
# Drain timeout for cutover
drain_timeout = "30s"

# Skip delta pass (NOT recommended)
skip_delta_pass = false
```

### Tuning for Faster Recovery

```toml
[rebalancer]
max_concurrent_migrations = 10  # Increase concurrency
migration_batch_size = 5000      # Larger batches
migration_batch_delay_ms = 0     # No throttling
```

**Warning:** Increasing concurrency and batch size increases memory and network usage. Monitor cluster health during recovery.

## Prevention

### Reducing Node Failures

1. **Resource requests/limits** - Ensure pods have sufficient CPU/memory
2. **Liveness/readiness probes** - Configure appropriate timeouts
3. **Pod disruption budgets** - Prevent voluntary disruptions during updates
4. **Anti-affinity** - Spread replicas across different nodes/zones

```yaml
# Example: Pod anti-affinity
spec:
  affinity:
    podAntiAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          podAffinityTerm:
            labelSelector:
              matchExpressions:
                - key: app
                  operator: In
                  values:
                    - meilisearch
            topologyKey: kubernetes.io/hostname
```

### Regular Health Checks

```bash
# Set up a cron job to check cluster health
*/5 * * * * miroir-ctl cluster health || echo "Cluster unhealthy" | mail -s "Miroir Alert" admin@example.com
```

## Related Documentation

- [Migration Runbook](migration_runbook.md) — Shard migration procedures
- [Troubleshooting Guide](../troubleshooting.md) — Common issues
- [Plan §2](../plan/plan.md) — Topology changes and failure handling
