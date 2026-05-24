# Runbook: Kill 1 of 3 nodes (RF=2)

**Scenario ID:** chaos_scenario_1_kill_one_node_rf2

## Expected Result

Continuous search; degraded writes warn via header (though with RF=2 and one node down, surviving replicas cover all shards, so degraded header may not appear).

## Precondition Check

- 3-node cluster with RF=2
- All nodes healthy
- Test index with 500 documents indexed
- All documents searchable

## Manual Reproduction Steps

```bash
# Start the RF=2 cluster
cd /path/to/miroir
docker-compose -f examples/docker-compose-dev-rf2.yml -p miroir-manual-s1 up -d

# Wait for Miroir to be healthy
curl http://localhost:7710/health

# Create test index and add documents
curl -X POST 'http://localhost:7710/indexes' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "uid": "manual-s1",
    "primaryKey": "id"
  }'

# Add 500 documents (use a script or the miroir-ctl)

# Kill node-1
docker stop miroir-manual-s1_meili-1_1

# Run searches
curl -X POST 'http://localhost:7710/indexes/manual-s1/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "content", "limit": 500}'

# Check for degraded header
curl -I -X POST 'http://localhost:7710/indexes/manual-s1/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "content"}'

# Cleanup
docker-compose -f examples/docker-compose-dev-rf2.yml -p miroir-manual-s1 down -v
```

## Expected Observables

### Metrics

- `miroir_router_search_latency_*` - May increase slightly as requests retry
- `miroir_node_requests_total{node="meili-1"}` - Drops to zero (node is down)
- `miroir_node_requests_total{node="meili-0"}` and `{node="meili-2"}` - Increase (take over load)

### Headers

- `X-Miroir-Degraded` - Should NOT appear (RF=2 provides full coverage with one node down)

### Client Errors

- No search failures
- All 500 documents returned
- Write operations succeed

## Recovery Procedure

```bash
# Restart the killed node
docker start miroir-manual-s1_meili-1_1

# Wait for health check to detect recovery (default: 5s interval)
# Miroir will automatically resume routing to the recovered node

# Verify routing is restored
curl http://localhost:7710/health
```

## How This Differs on HA (2+ Miroir replicas)

With multiple Miroir replicas:

- Client requests are load-balanced across replicas
- If one Miroir replica fails, others continue serving
- No client-visible downtime
- Failed replica restarts and rejoins the cluster automatically

## Notes

- RF=2 means each shard exists on 2 nodes
- Losing 1 of 3 nodes means all shards still have at least 1 replica
- This is the "happy path" failure scenario - minimal impact
- Monitor `miroir_node_health_status` for real-time node state
