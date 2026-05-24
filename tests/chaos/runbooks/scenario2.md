# Runbook: Kill 2 of 3 nodes (RF=2)

**Scenario ID:** chaos_scenario_2_kill_two_nodes_rf2

## Expected Result

Shard loss; 503 (Service Unavailable) or partial results per policy.

## Precondition Check

- 3-node cluster with RF=2
- All nodes healthy
- Test index with 500 documents indexed
- All documents searchable

## Manual Reproduction Steps

```bash
# Start the RF=2 cluster
cd /path/to/miroir
docker-compose -f examples/docker-compose-dev-rf2.yml -p miroir-manual-s2 up -d

# Wait for Miroir to be healthy
curl http://localhost:7710/health

# Create test index and add documents (use miroir-ctl or script)

# Kill node-1 and node-2
docker stop miroir-manual-s2_meili-1_1
docker stop miroir-manual-s2_meili-2_1

# Run searches
curl -X POST 'http://localhost:7710/indexes/manual-s2/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "content"}'

# Check response status and headers
curl -i -X POST 'http://localhost:7710/indexes/manual-s2/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "content"}'

# Cleanup
docker-compose -f examples/docker-compose-dev-rf2.yml -p miroir-manual-s2 down -v
```

## Expected Observables

### Metrics

- `miroir_router_search_errors_total{reason="unavailable_shard"}` - Increases
- `miroir_router_search_degraded_total` - Increases
- `miroir_node_requests_total{node="meili-0"}` - Spikes (sole survivor)
- `miroir_node_requests_total{node="meili-1"}` and `{node="meili-2"}` - Zero (down)

### Headers

- `X-Miroir-Degraded` - MUST appear (indicates partial results)
- HTTP status may be 503 or 200 with degraded results

### Client Errors

- Some searches may fail with 503
- Successful searches return partial results (< 500 documents)
- Degraded header always present on successful partial results

## Recovery Procedure

```bash
# Restart one killed node (node-1)
docker start miroir-manual-s2_meili-1_1

# Wait for health check to detect recovery (default: 5s interval)
# Verify cluster is recovering
curl http://localhost:7710/health

# Searches should now return full results
curl -X POST 'http://localhost:7710/indexes/manual-s2/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "content", "limit": 500}'
```

## How This Differs on HA (2+ Miroir replicas)

With multiple Miroir replicas:

- Same backend behavior (partial results or 503)
- Client may be routed to different Miroir instances
- Load balancer health checks prevent routing to failed Miroir replicas
- No additional backend redundancy - this is about Miroir itself, not Meilisearch nodes

## Notes

- RF=2 with 2 nodes down means many shards lose both replicas
- Remaining node (meili-0) can only serve documents it holds
- This is the "graceful degradation" scenario - partial results better than none
- Per plan §1 principle 5: degrade rather than fail entirely
- Alert on `miroir_router_search_degraded_total` increasing
- Consider RF=3 for deployments requiring tolerance of 2-node failures
