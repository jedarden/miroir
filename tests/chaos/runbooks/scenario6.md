# Runbook: Kill a node mid-rebalance

**Scenario ID:** chaos_scenario_6_kill_mid_rebalance

## Expected Result

Rebalancer pauses, resumes on recovery; no data loss. Write operation may fail or succeed partially.

## Precondition Check

- 3-node cluster with RF=2
- All nodes healthy
- Active rebalance operation in progress (node addition, drain, or bulk document load)

## Manual Reproduction Steps

```bash
# Start the RF=2 cluster
cd /path/to/miroir
docker-compose -f examples/docker-compose-dev-rf2.yml -p miroir-manual-s6 up -d

# Wait for cluster to be healthy
curl http://localhost:7710/health

# Create a test index
curl -X POST 'http://localhost:7710/indexes' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"uid": "manual-s6", "primaryKey": "id"}'

# Start a large document load (simulates rebalance traffic)
# Use a script to add 1000+ documents

# While load is in progress, kill node-1
docker stop miroir-manual-s6_meili-1_1

# Check the task status
curl 'http://localhost:7710/tasks?uids=1' \
  -H 'Authorization: Bearer dev-key'

# Restart node-1
docker start miroir-manual-s6_meili-1_1

# Wait for recovery
sleep 10

# Check document count
curl -X POST 'http://localhost:7710/indexes/manual-s6/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "", "limit": 1000}'

# Cleanup
docker-compose -f examples/docker-compose-dev-rf2.yml -p miroir-manual-s6 down -v
```

## Expected Observables

### Metrics

- `miroir_rebalancer_active_migrations` - Non-zero before failure, may pause during failure
- `miroir_rebalancer_paused_total` - Increases when node fails
- `miroir_node_health_status{node="meili-1"}` - Goes to 0 (down), then back to 1
- `miroir_task_errors_total` - May increase if write task fails

### Rebalancer Behavior

1. Rebalance starts (e.g., bulk document load or node addition)
2. Node failure detected mid-operation
3. Rebalancer pauses migrations involving failed node
4. Healthy migrations continue
5. Node recovers
6. Rebalancer resumes from checkpoint
7. Migrations complete successfully

### Task Status

- Task may show `succeeded`, `failed`, or `processing`
- Failed tasks can be retried via client
- Documents written before failure are preserved

## Recovery Procedure

```bash
# Restart the failed node
docker start <container-name>

# Wait for node to be healthy
curl http://localhost:7710/health

# Check rebalancer status
curl 'http://localhost:7710/_miroir/rebalancer/status' \
  -H 'Authorization: Bearer $ADMIN_API_KEY'

# If rebalancer is paused, resume it manually
curl -X POST 'http://localhost:7710/_miroir/rebalancer/resume' \
  -H 'Authorization: Bearer $ADMIN_API_KEY'

# Run anti-entropy to ensure consistency
curl -X POST 'http://localhost:7710/_miroir/anti-entropy/start' \
  -H 'Authorization: Bearer $ADMIN_API_KEY'

# Monitor anti-entropy progress
curl 'http://localhost:7710/_miroir/anti-entropy/status' \
  -H 'Authorization: Bearer $ADMIN_API_KEY'
```

## How This Differs on HA (2+ Miroir replicas)

With multiple Miroir replicas:

- Rebalancer runs in leader-elected mode (only one replica is leader)
- If leader replica fails, another replica takes over
- Rebalancer state is persisted in task store (Redis or SQLite)
- Recovery resumes from last checkpoint
- No duplicate migrations (leader election prevents split-brain)

## Notes

- Rebalancer is designed to tolerate failures gracefully
- Each migration has a checkpoint; can be resumed after failure
- Task store (Redis/SQLite) provides durability across restarts
- Leader election ensures only one rebalancer is active at a time
- Anti-entropy can verify and repair any inconsistencies after recovery
- This scenario validates:
  - Rebalancer pause/resume logic
  - Task store durability
  - Leader election correctness
  - No data loss or corruption
- Common during:
  - Node failures during scale-out/scale-in
  - Network partitions during rebalance
  - Pod evictions during bulk operations
