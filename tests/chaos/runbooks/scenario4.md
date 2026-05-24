# Runbook: Network delay (tc netem) on one node

**Scenario ID:** chaos_scenario_4_netem_delay

## Expected Result

Searches slow by at most max shard latency; no errors. With 500ms added delay, searches should complete in < 2 seconds total.

## Precondition Check

- 3-node cluster running
- All nodes healthy
- Test index with 500 documents indexed
- Docker containers have `CAP_NET_ADMIN` capability (required for tc netem)

## Manual Reproduction Steps

```bash
# Start the cluster
cd /path/to/miroir
docker-compose -f examples/docker-compose-dev.yml -p miroir-manual-s4 up -d

# Wait for cluster to be healthy
curl http://localhost:7700/health

# Apply 500ms delay to meili-0
docker exec miroir-manual-s4_meili-0_1 \
  tc qdisc add dev eth0 root netem delay 500ms

# Run searches and measure latency
time curl -X POST 'http://localhost:7700/indexes/test/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "content"}'

# Remove delay
docker exec miroir-manual-s4_meili-0_1 \
  tc qdisc del dev eth0 root

# Verify latency returns to baseline
time curl -X POST 'http://localhost:7700/indexes/test/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "content"}'

# Cleanup
docker-compose -f examples/docker-compose-dev.yml -p miroir-manual-s4 down -v
```

## Expected Observables

### Metrics

- `miroir_router_search_latency_seconds_bucket` - Latency increases
- `miroir_node_request_duration_seconds{node="meili-0"}` - Increases by ~500ms
- `miroir_router_search_timeout_total` - Should NOT increase
- `miroir_router_search_errors_total` - Should NOT increase

### Client Errors

- No search failures
- All searches complete successfully
- Latency increases but remains within timeout threshold

## Recovery Procedure

```bash
# Remove the network delay
docker exec <container-name> tc qdisc del dev eth0 root

# Verify latency returns to baseline
# Monitor metrics for recovery
curl http://localhost:9090/api/v1/query?query=miroir_router_search_latency_seconds
```

## How This Differs on HA (2+ Miroir replicas)

With multiple Miroir replicas:

- Same behavior (backend node delay affects all replicas)
- No additional benefit from multiple Miroir instances
- Consider replica-aware routing if network partitions are common
- In cross-region deployments, use replica groups to route to nearest region

## Notes

- `tc netem` simulates network latency, packet loss, duplication, and more
- This test validates timeout configuration is appropriate
- Per plan §8: searches should complete in < 2× baseline latency
- Common causes of real network delay:
  - Network congestion
  - Cross-region traffic
  - Oversubscribed links
  - DNS delays
- If latency is consistently high, consider:
  - Increasing timeout values
  - Adding replica groups in same region as clients
  - Using CDN or edge caching
  - Investigating network path (traceroute, mtr)
