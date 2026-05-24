# Diagnostic Playbook

This playbook provides a systematic approach to diagnosing issues in a Miroir cluster. Run these steps in order when investigating any problem.

## Prerequisites

Set up your environment:
```bash
export MIROIR_URL="https://miroir.example.com"
export MIROIR_KEY="your-admin-key"
export NAMESPACE="search"  # adjust if needed
```

## Step 1: Check Cluster Health

### 1.1 Verify all pods are running
```bash
kubectl get pods -n $NAMESPACE
```

**Expected output**: All pods in `Running` state, Ready 1/1 or 2/2.

**Common issues**:
- Pods in `Pending` → resource constraints, scheduler issues
- Pods in `CrashLoopBackOff` → config errors, OOM kills
- Pods with `Ready: 0/1` → startup probe failing, dependency unavailable

### 1.2 Check recent pod restarts
```bash
kubectl get pods -n $NAMESPACE -o json | jq -r '.items[] | "\(.metadata.name): \(.status.containerStatuses[0].restartCount) restarts"'
```

**Action**: Investigate pods with > 3 restarts in the last hour.

### 1.3 Check resource usage
```bash
kubectl top pods -n $NAMESPACE
kubectl top nodes
```

**Action**: If CPU/memory limits are hit, consider scaling up or adjusting limits.

## Step 2: Check Miroir Topology

### 2.1 Get topology overview
```bash
curl -s "$MIROIR_URL/_miroir/topology?key=$MIROIR_KEY" | jq '.'
```

**Expected output**:
```json
{
  "shards": 128,
  "replication_factor": 2,
  "nodes": [
    {"node_id": "node-0", "status": "active", "shards": [...]},
    {"node_id": "node-1", "status": "active", "shards": [...]},
    {"node_id": "node-2", "status": "active", "shards": [...]}
  ]
}
```

**Common issues**:
- `status: "degraded"` → node is unreachable or unhealthy
- `status: "draining"` → node migration in progress
- `shards: []` → node has no assigned shards (newly added)

### 2.2 Check for degraded shards
```bash
curl -s "$MIROIR_URL/_miroir/topology?key=$MIROIR_KEY" | jq '
  .nodes as $nodes |
  .shards as $total |
  ($nodes | map(.shards | length) | add) as $assigned |
  "Assigned: \($assigned)/\($total*3) (RF × \($nodes | length))",
  "Degraded nodes: \([.nodes[] | select(.status != "active")] | length)"
'
```

**Action**: Any degraded nodes need investigation (see Step 4).

### 2.3 Verify node agreement on topology
```bash
for i in 0 1 2; do
  echo "=== node-$i ==="
  kubectl exec -n $NAMESPACE miroir-$i -- \
    curl -s localhost:7700/_miroir/topology | jq '.shards, .replication_factor'
done
```

**Expected**: All nodes report the same shard count and RF.

**Action**: If nodes disagree, restart coordinator pod to force reconciliation.

## Step 3: Check Metrics

### 3.1 Get metrics summary
```bash
curl -s "$MIROIR_URL/_miroir/metrics?key=$MIROIR_KEY" | jq '
{
  degraded_shards: .degraded_shards // 0,
  task_queue_depth: .task_queue_depth // 0,
  search_latency_p99: .search_latency_p99_ms // 0,
  write_latency_p99: .write_latency_p99_ms // 0,
  cdc_lag_seconds: .cdc_lag_seconds // 0
}
'
```

**Key thresholds**:
- `degraded_shards > 0` → investigate node health
- `task_queue_depth > 1000` → task processing bottleneck
- `search_latency_p99 > 1000` → slow queries, need optimization
- `cdc_lag_seconds > 300` → CDC falling behind

### 3.2 Check Prometheus metrics (if available)
```bash
# Via Prometheus API
curl -s "http://prometheus:9090/api/v1/query?query=miroir_degraded_shards" | jq '.data.result[0].value[1]'

# Via pod metrics endpoint
kubectl exec -n $NAMESPACE miroir-0 -- curl -s localhost:9091/metrics | grep miroir_
```

## Step 4: Check Logs for Errors

### 4.1 Get recent errors from all pods
```bash
for pod in $(kubectl get pods -n $NAMESPACE -l app=miroir -o name); do
  echo "=== $pod ==="
  kubectl logs -n $NAMESPACE $pod --tail=100 | jq -rc 'select(.level=="ERROR")' || true
  echo ""
done
```

**Common error patterns**:
- `connection refused` → peer pod down or network issue
- `timeout` → slow query, overloaded node
- `hash mismatch` → potential data corruption (run anti-entropy)
- `lease expired` → leader election contention

### 4.2 Check coordinator logs for topology changes
```bash
kubectl logs -n $NAMESPACE -l app=miroir,role=coordinator --tail=200 | \
  jq -rc 'select(.message | test("topology|node|shard"))'
```

### 4.3 Check for crash loop patterns
```bash
kubectl logs -n $NAMESPACE miroir-0 --previous --tail=100 | \
  jq -rc 'select(.level=="ERROR" or .level=="FATAL")' || true
```

## Step 5: Check Task Status

### 5.1 List stuck or long-running tasks
```bash
curl -s "$MIROIR_URL/_miroir/tasks?key=$MIROIR_KEY&status=processing" | \
  jq -r '.tasks[] | "\(.miroir_id) (\(.task_type // "unknown")): \(.created_at)"'
```

**Action**: Investigate tasks running > 1 hour.

### 5.2 Get detailed task status
```bash
miroir-ctl task status --task-id <miroir_task_id> --verbose
```

### 5.3 Check task registry health
```bash
# SQLite mode
kubectl exec -n $NAMESPACE miroir-0 -- \
  sqlite3 /data/miroir.db "SELECT status, COUNT(*) FROM tasks GROUP BY status;"

# Redis mode
kubectl exec -n $NAMESPACE redis-0 -- \
  redis-cli --scan --pattern "miroir:tasks:*" | wc -l
```

## Step 6: Check Anti-Entropy Status

### 6.1 Last AE run time
```bash
curl -s "$MIROIR_URL/_miroir/anti-entropy/status?key=$MIROIR_KEY" | \
  jq '{last_run: .last_run_at, next_run: .next_run_at, divergences_found: .divergences_found}'
```

**Action**: If `last_run_at` is > 24 hours ago, AE may be stuck.

### 6.2 Check for divergence
```bash
curl -s "$MIROIR_URL/_miroir/anti-entropy/divergence?key=$MIROIR_KEY" | \
  jq '.divergent_shards | length'
```

**Action**: Any divergent shards should trigger an AE run.

## Step 7: Check External Dependencies

### 7.1 Check Redis connectivity
```bash
kubectl exec -n $NAMESPACE miroir-0 -- \
  redis-cli -h redis-headless ping
```

**Expected**: `PONG`

### 7.2 Check Meilisearch backend connectivity
```bash
for i in 0 1 2; do
  echo "=== miroir-$i ==="
  kubectl exec -n $NAMESPACE miroir-$i -- \
    curl -s http://localhost:7700/health | jq '.status'
done
```

**Expected**: `"available"`

### 7.3 Check network policies
```bash
kubectl get networkpolicy -n $NAMESPACE
kubectl describe networkpolicy miroir-allow-peer -n $NAMESPACE
```

## Step 8: Run Self-Diagnostics

### 8.1 Miroir self-check endpoint
```bash
curl -s "$MIROIR_URL/_miroir/health?key=$MIROIR_KEY" | jq '.'
```

**Expected output**:
```json
{
  "status": "healthy",
  "checks": {
    "topology": "ok",
    "task_store": "ok",
    "coordinator_leader": "ok",
    "peers_connected": "ok"
  }
}
```

### 8.2 Run canary tests
```bash
# List configured canaries
curl -s "$MIROIR_URL/_miroir/canaries?key=$MIROIR_KEY" | \
  jq -r '.canaries[] | .id'

# Trigger a canary run
curl -X POST "$MIROIR_URL/_miroir/canaries/search-health/run?key=$MIROIR_KEY"
```

## Decision Tree

Based on findings, follow this tree:

```
Are any pods not running?
├─ Yes → Check pod logs (Step 4), describe pod for events
└─ No → Continue

Are any nodes degraded?
├─ Yes → Check node logs, verify network, restart if needed
└─ No → Continue

Is task queue depth > 1000?
├─ Yes → Check for stuck tasks (Step 5), scale workers if needed
└─ No → Continue

Is search latency high?
├─ Yes → Check query patterns, consider query optimization
└─ No → Continue

Any errors in logs?
├─ Yes → Investigate specific error pattern
└─ No → Issue may be external, check dependencies (Step 7)
```

## Escalation Checklist

Before escalating, gather:

1. **Topology output** (Step 2.1)
2. **Recent errors** (Step 4.1)
3. **Stuck tasks** (Step 5.1)
4. **Metrics snapshot** (Step 3.1)
5. **Pod status** (Step 1.1)

Attach these to your GitHub issue or support ticket.

## Prevention: Regular Health Checks

Set up a cron job or monitoring alert to run this daily:

```bash
#!/bin/bash
# daily-health-check.sh

# Quick health check
HEALTH=$(curl -s "$MIROIR_URL/_miroir/health?key=$MIROIR_KEY")
STATUS=$(echo $HEALTH | jq -r '.status')

if [ "$STATUS" != "healthy" ]; then
  echo "UNHEALTHY: $HEALTH"
  # Send alert
fi
```

## Related Documentation

- [Common Issues Guide](../troubleshooting.md)
- [Node Drain Runbook](../runbooks/node-drain.md)
- [Migration Runbook](../migration_runbook.md)
- [Metrics Reference](../operations/metrics.md)
