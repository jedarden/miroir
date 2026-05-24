# Runbook: Restart a killed node

**Scenario ID:** chaos_scenario_5_restart_node

## Expected Result

Miroir detects recovery within health check interval (default 5s) and resumes routing. No data loss; searches and writes work normally after recovery.

## Precondition Check

- 3-node cluster with RF=2
- All nodes healthy
- Test index with 500 documents indexed
- Kill and restart one node

## Manual Reproduction Steps

```bash
# Start the RF=2 cluster
cd /path/to/miroir
docker-compose -f examples/docker-compose-dev-rf2.yml -p miroir-manual-s5 up -d

# Wait for cluster to be healthy
curl http://localhost:7710/health

# Create test index and add documents

# Kill node-1
docker stop miroir-manual-s5_meili-1_1

# Verify searches still work (RF=2 provides coverage)
curl -X POST 'http://localhost:7710/indexes/test/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "content", "limit": 500}'

# Restart node-1
docker start miroir-manual-s5_meili-1_1

# Wait for health check to detect recovery (default: 5s interval)
sleep 10

# Verify searches work with full node set
curl -X POST 'http://localhost:7710/indexes/test/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "content", "limit": 500}'

# Add a new document to verify routing to recovered node
curl -X POST 'http://localhost:7710/indexes/test/documents' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '[{"id": "after-recovery", "title": "After Recovery"}]'

# Search for the new document
curl -X POST 'http://localhost:7710/indexes/test/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "After Recovery"}'

# Cleanup
docker-compose -f examples/docker-compose-dev-rf2.yml -p miroir-manual-s5 down -v
```

## Expected Observables

### Metrics

- `miroir_node_health_status{node="meili-1"}` - Goes to 0 (down), then back to 1 (up)
- `miroir_node_requests_total{node="meili-1"}` - Drops to 0, then increases after recovery
- `miroir_router_search_errors_total` - Should NOT increase
- `miroir_anti_entropy_migrations_total` - May increase if anti-entropy runs

### Node State Transition

1. Node is `Active` (healthy)
2. Node is killed → state becomes `Failed`
3. Health check marks node as unhealthy
4. Router stops routing to failed node
5. Node is restarted
6. Health check detects recovery (within 5s)
7. Node state returns to `Active`
8. Router resumes routing to recovered node

### Client Errors

- No search failures during outage (RF=2 provides coverage)
- No search failures after recovery
- New documents can be added and searched

## Recovery Procedure

```bash
# Restart the failed node
docker start <container-name>

# Verify node is running
docker ps | grep meili-1

# Check Miroir health endpoint
curl http://localhost:7710/health

# Verify node is receiving traffic
# Watch metrics for the node
curl http://localhost:9090/api/v1/query?query=miroir_node_requests_total{node=\"meili-1\"}
```

## How This Differs on HA (2+ Miroir replicas)

With multiple Miroir replicas:

- Same backend behavior (node recovery is independent)
- Each Miroir replica detects recovery independently
- Health check interval applies per replica
- No difference in recovery time

## Notes

- Health check interval is configurable via `health_check_interval_seconds`
- Default is 5 seconds; adjust for faster/slower recovery detection
- Anti-entropy may run after node recovery to ensure data consistency
- If node was down for extended period, consider:
  - Running manual anti-entropy via `miroir-ctl anti-entropy start`
  - Checking for missed writes via task log reconciliation
  - Monitoring for any shard inconsistencies
- This scenario is common during:
  - Rolling upgrades (restart nodes one at a time)
  - Node maintenance (patching, config changes)
  - Crash recovery (OOM killed, segfault)
