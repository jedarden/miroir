# Common Issues & Troubleshooting

This guide covers the most common issues encountered when running Miroir in production, along with their symptoms, causes, and fixes.

## Quick Diagnostics

Before diving into specific issues, run the [diagnostic playbook](diagnostics.md) to gather baseline information about your cluster's health.

## Common Issues

### Error: "primary key required"

#### Symptom
Client sees:
```json
HTTP 400 {
  "code": "miroir_primary_key_required",
  "message": "Miroir requires an explicit primary key at index creation"
}
```

#### Cause
The index was created without a `primaryKey` field. Miroir cannot route documents without knowing the primary key in advance.

#### Fix
```bash
curl -X POST https://miroir/indexes \
  -H "Authorization: Bearer $KEY" \
  -d '{
    "uid": "myindex",
    "primaryKey": "id"
  }'
```

#### Why this differs from Meilisearch
Meilisearch can infer the primary key from the first document batch. Miroir cannot — it needs to hash the PK *before* any node sees it to determine which shard owns the document. Explicit `primaryKey` at index creation is required.

---

### Search returns fewer results than expected

#### Symptom
Search queries return fewer results than known document count, especially after node failures or during migrations.

#### Cause
A replica holding a shard is degraded or unreachable. Miroir's cross-reference mechanism skips degraded replicas to avoid returning incomplete or stale results, which can reduce result counts when RF > 1.

#### Fix
1. Check topology for degraded nodes:
   ```bash
   curl -s https://miroir/_miroir/topology | jq '.nodes[] | select(.status != "active")'
   ```

2. Check for degraded shards:
   ```bash
   curl -s https://miroir/_miroir/metrics | jq '.degraded_shards'
   ```

3. If a node is degraded, check its logs:
   ```bash
   kubectl logs miroir-0 --tail=100 | jq 'select(.level=="ERROR")'
   ```

4. Restart the degraded pod if it's stuck:
   ```bash
   kubectl delete pod miroir-0
   ```

#### Prevention
- Set up canaries to proactively detect search degradation
- Monitor the `miroir_degraded_shards` metric
- Ensure proper resource limits to prevent OOM kills

---

### Task polling stuck at "processing"

#### Symptom
`miroir-ctl task status` shows a task stuck in "processing" state indefinitely, even though the operation appears complete.

#### Cause
The task coordinator lost track of per-node task status. This can happen when:
- A node crashes during task execution
- Network partition prevents status updates
- Task registry checkpoint is delayed

#### Fix
1. Check per-node task status:
   ```bash
   miroir-ctl task status --task-id <miroir_task_id> --verbose
   ```

2. Identify which node(s) have incomplete status:
   ```bash
   kubectl logs miroir-0 --tail=100 | grep "<miroir_task_id>"
   kubectl logs miroir-1 --tail=100 | grep "<miroir_task_id>"
   ```

3. If all nodes have completed but the task is stuck, force-complete the task:
   ```bash
   miroir-ctl task complete --task-id <miroir_task_id>
   ```

4. If a node crashed and cannot recover, mark its tasks as failed:
   ```bash
   miroir-ctl task fail --task-id <miroir_task_id> --node <node_id> --reason "node crashed"
   ```

#### Prevention
- Enable task registry checkpointing (default: every 100 tasks)
- Monitor task queue depth via `miroir_task_queue_depth` metric
- Set task timeouts appropriate to your workload

---

### Node drain blocked: "insufficient replicas"

#### Symptom
```bash
$ miroir-ctl node drain node-1
Error: Cannot drain node-1: removing it would drop replication factor below minimum
```

#### Cause
Draining a node would leave some shards with fewer replicas than the minimum RF. This is a safety check to prevent data loss.

#### Fix
1. Check current RF configuration:
   ```bash
   curl -s https://miroir/_miroir/topology | jq '.replication_factor'
   ```

2. Add a new node first:
   ```bash
   kubectl scale statefulset miroir --replicas=4
   # Wait for node-3 to be ready
   kubectl wait --for=condition=ready pod/miroir-3
   ```

3. Then retry the drain:
   ```bash
   miroir-ctl node drain node-1
   ```

#### Alternative: Force drain (dangerous)
If you must drain without sufficient replicas, use `--force`:
```bash
miroir-ctl node drain node-1 --force
```
This will reduce RF for affected shards during migration. Only use this if:
- You can tolerate reduced redundancy temporarily
- Anti-entropy is enabled to repair divergence later

---

### Migration stuck after coordinator crash

#### Symptom
A shard migration (reshard, rebalance, node drain) was in progress when the coordinator pod crashed. After restart, the migration is stuck and cannot complete or rollback.

#### Cause
The coordinator stores migration state in the task store. If it crashes during state transitions, the migration may be left in an inconsistent state.

#### Fix
1. Check migration status:
   ```bash
   miroir-ctl reshard status --operation-id <operation_id>
   ```

2. If stuck in "in_progress" with no activity, recover the migration:
   ```bash
   miroir-ctl reshard recover --operation-id <operation_id>
   ```

3. If recovery fails, you may need to force-complete:
   ```bash
   # This skips remaining delta pass and anti-entropy
   miroir-ctl reshard complete --operation-id <operation_id> --force
   ```

4. Run anti-entropy manually to repair any divergence:
   ```bash
   miroir-ctl anti-entropy run --index-uid <affected_index>
   ```

#### Prevention
- Enable task store persistence (Redis mode for HA)
- Set coordinator leader election timeout appropriately
- Monitor coordinator pod health via liveness probes

---

### High memory usage on Redis

#### Symptom
Redis memory usage grows continuously, potentially triggering OOM kills.

#### Cause
The most common causes are:
1. Idempotency cache entries not expiring
2. Task registry not pruning terminal tasks
3. Session entries not being cleaned up

#### Fix
1. Check Redis memory breakdown:
   ```bash
   redis-cli INFO memory | grep used_memory_human
   redis-cli --bigkeys --pattern "miroir:*"
   ```

2. Check largest key categories:
   ```bash
   redis-cli --scan --pattern "miroir:tasks:*" | wc -l  # task count
   redis-cli --scan --pattern "miroir:idemp:*" | wc -l   # idempotency entries
   redis-cli --scan --pattern "miroir:session:*" | wc -l # sessions
   ```

3. Manually trigger cleanup if pruner is stuck:
   ```bash
   # Prune old terminal tasks
   miroir-ctl task prune --older-than 24h

   # Clear expired idempotency entries
   redis-cli --scan --pattern "miroir:idemp:*" | xargs redis-cli DEL
   ```

4. Adjust pruner intervals if needed:
   ```yaml
   # config.toml
   [task_store.prune]
   interval_seconds = 300  # run every 5 minutes
   task_retention_days = 7
   ```

#### Prevention
- Monitor Redis memory usage via `redis_used_memory` metric
- Set `maxmemory` and `maxmemory-policy allkeys-lru` on Redis
- Ensure pruner is running (check logs for "Pruning terminal tasks" messages)

---

### Index creation fails with "hash routing error"

#### Symptom
```bash
$ curl -X POST https://miroir/indexes -d '{"uid": "test", "primaryKey": "id"}'
HTTP 500 {"code": "hash_routing_error", "message": "unable to determine shard assignment"}
```

#### Cause
This typically happens when:
1. The topology view is inconsistent across nodes
2. The shard count is 0 or not configured
3. The primary key field is missing from schema validation

#### Fix
1. Check topology consistency:
   ```bash
   curl -s https://miroir/_miroir/topology | jq '.shards, .replication_factor, .nodes | length'
   ```

2. Verify all nodes agree on shard count:
   ```bash
   for pod in miroir-0 miroir-1 miroir-2; do
     echo "$pod:"
     kubectl exec $pod -- curl -s localhost:7700/_miroir/topology | jq '.shards'
   done
   ```

3. If nodes disagree, restart the coordinator to force topology reconciliation:
   ```bash
   kubectl delete pod -l app=miroir,role=coordinator
   ```

#### Prevention
- Use leader election to ensure single coordinator writer
- Monitor topology change log for conflicts

---

### Alias flip returns "wrong kind"

#### Symptom
```bash
$ miroir-ctl alias flip prod-logs logs-v2
Error: Alias 'prod-logs' is a multi-target alias, cannot flip
```

#### Cause
You're trying to flip an alias that was created as a "multi" alias (for cross-index search) rather than a "single" alias (for atomic index swap).

#### Fix
1. Check the alias type:
   ```bash
   miroir-ctl alias get prod-logs
   ```

2. If you need a swappable pointer, delete and recreate as a single alias:
   ```bash
   miroir-ctl alias delete prod-logs
   miroir-ctl alias create prod-logs --kind single --current-uid logs-v1
   ```

3. For cross-index search, use a separate multi alias:
   ```bash
   miroir-ctl alias create search-all --kind multi --target-uids logs-v1,metrics-v1
   ```

#### Prevention
- Use descriptive alias names to distinguish single vs multi
- Document alias conventions in your runbooks

---

### Search timeout during shard migration

#### Symptom
Search queries timeout or return 503 errors during active shard migrations, especially for large indexes.

#### Cause
During migration, some queries may be routed to nodes that are still warming up migrated shards, or to nodes under heavy load from migration work.

#### Fix
1. Check if migration is active:
   ```bash
   miroir-ctl reshard list --status in_progress
   ```

2. Temporarily increase query timeout:
   ```bash
   curl -X POST https://miroir/indexes/myindex/search \
     -H "Query-Timeout: 30" \
     -d '{"q": "test"}'
   ```

3. If timeouts persist, pause the migration:
   ```bash
   miroir-ctl reshard pause --operation-id <operation_id>
   ```

4. Resume during off-peak hours:
   ```bash
   miroir-ctl reshard resume --operation-id <operation_id>
   ```

#### Prevention
- Schedule large migrations during low-traffic periods
- Use `--throttle` flag on reshard to limit CPU usage
- Monitor search latency during migrations

---

### CDC cursor out of sync

#### Symptom
CDC events arrive with stale or duplicate sequence numbers, or events are missing entirely.

#### Cause
The CDC cursor stored in Redis is out of sync with the actual event stream. This can happen if:
- The sink was down during a period of high write activity
- A cursor update failed silently
- The sink was reset without clearing the cursor

#### Fix
1. Check current cursor position:
   ```bash
   miroir-ctl cdc cursor --sink-name elasticsearch --index-uid myindex
   ```

2. Compare to Meilisearch event stream:
   ```bash
   # On a Meilisearch node
   curl -s http://localhost:7700/indexes/myindex/cdc/events | jq '.events | length'
   ```

3. If cursor is behind, reset it to force re-sync from a checkpoint:
   ```bash
   miroir-ctl cdc reset-cursor --sink-name elasticsearch --index-uid myindex --confirm
   ```

4. For large gaps, consider a full re-index:
   ```bash
   miroir-ctl dump export --index-uid myindex --output /data/myindex.dump
   miroir-ctl dump import --sink-name elasticsearch --input /data/myindex.dump
   ```

#### Prevention
- Monitor CDC lag via `miroir_cdc_lag_seconds` metric
- Set up alerts for cursor stall detection
- Use idempotent sinks to handle duplicate events gracefully

---

## Getting Help

If you don't see your issue listed here:

1. Run the [diagnostic playbook](diagnostics.md) and gather the output
2. Search [existing GitHub issues](https://github.com/jedarden/miroir/issues)
3. Open a new issue with:
   - Miroir version
   - Diagnostic output
   - Relevant logs (sanitized)
   - Steps to reproduce
