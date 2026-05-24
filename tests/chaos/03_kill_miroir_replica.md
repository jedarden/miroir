# Chaos Test 03: Kill 1 of 2 Miroir Replicas

## Objective
Verify that with multiple Miroir replicas behind a load balancer, killing one replica causes zero client-visible downtime (load balancer fails over seamlessly).

## Prerequisites

For this test, you need a multi-replica Miroir deployment. Update the docker-compose file:

```bash
# Use the multi-replica compose file
cd /home/coding/miroir/examples
docker-compose -f docker-compose-dev-multi-replica.yml up -d
```

Or manually set up:
- 2 Miroir replicas (miroir-1, miroir-2)
- Load balancer (traefik/nginx) on port 7700
- Health checks configured

## Test Steps

### 1. Setup: Create test index
```bash
# Create index through load balancer
curl -X POST 'http://localhost:7700/indexes' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "uid": "chaos_test_03",
    "primaryKey": "id"
  }'

# Add documents
curl -X POST 'http://localhost:7700/indexes/chaos_test_03/documents' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '[
    {"id": 1, "title": "Replica Test 1"},
    {"id": 2, "title": "Replica Test 2"},
    {"id": 3, "title": "Replica Test 3"}
  ]'

sleep 5
```

### 2. Baseline: Verify both replicas accessible
```bash
# Check replica 1 directly
echo "=== Replica 1 ==="
curl -X GET 'http://localhost:7701/health' -H 'Authorization: Bearer dev-key' | jq '.'

# Check replica 2 directly
echo "=== Replica 2 ==="
curl -X GET 'http://localhost:7702/health' -H 'Authorization: Bearer dev-key' | jq '.'

# Check through load balancer
echo "=== Load Balancer ==="
curl -X GET 'http://localhost:7700/health' -H 'Authorization: Bearer dev-key' | jq '.'
```

### 3. Baseline: Continuous requests to verify load balancing
```bash
# Make 10 requests to see distribution
for i in {1..10}; do
  echo "Request $i:"
  curl -X GET 'http://localhost:7700/health' \
    -H 'Authorization: Bearer dev-key' \
    -H 'X-Debug-Replica: true' \
    -s | jq '.replica // "unknown"'
  sleep 0.5
done

# Expected: Requests distributed across both replicas
```

### 4. Kill one Miroir replica
```bash
# Identify which replica to kill
echo "Killing replica 1..."
docker stop miroir-replica-1
docker kill miroir-replica-1

# Verify it's down
curl -X GET 'http://localhost:7701/health' \
  -H 'Authorization: Bearer dev-key' \
  --max-time 2 \
  || echo "Replica 1 is down (expected)"
```

### 5. IMMEDIATE test: Zero downtime verification
```bash
# Run rapid continuous searches IMMEDIATELY after kill
echo "=== Running 50 rapid searches immediately after kill ==="
FAILED=0
SUCCESS=0

for i in {1..50}; do
  HTTP_CODE=$(curl -X POST 'http://localhost:7700/indexes/chaos_test_03/search' \
    -H 'Authorization: Bearer dev-key' \
    -H 'Content-Type: application/json' \
    --data-binary '{"q": "replica"}' \
    -w '%{http_code}' \
    -o /dev/null \
    -s \
    --max-time 5)

  if [ "$HTTP_CODE" = "200" ]; then
    ((SUCCESS++))
    echo "Search $i: ✓ (200)"
  else
    ((FAILED++))
    echo "Search $i: ✗ ($HTTP_CODE)"
  fi

  sleep 0.1
done

echo ""
echo "Results: $SUCCESS successful, $FAILED failed"

# Expected: All 50 requests succeed (0 failed) or very brief blip (< 3)
```

### 6. Sustained operations test
```bash
# Run sustained operations for 30 seconds
echo "=== Running sustained operations for 30 seconds ==="
START=$(date +%s)
END=$((START + 30))

FAILED=0
TOTAL=0

while [ $(date +%s) -lt $END ]; do
  ((TOTAL++))

  # Mix of reads and writes
  if [ $((TOTAL % 3)) -eq 0 ]; then
    # Write
    HTTP_CODE=$(curl -X POST 'http://localhost:7700/indexes/chaos_test_03/documents' \
      -H 'Authorization: Bearer dev-key' \
      -H 'Content-Type: application/json' \
      --data-binary "{\"id\": $TOTAL, \"title\": \"Sustained Test $TOTAL\"}" \
      -w '%{http_code}' \
      -o /dev/null \
      -s \
      --max-time 5)
  else
    # Read
    HTTP_CODE=$(curl -X POST 'http://localhost:7700/indexes/chaos_test_03/search' \
      -H 'Authorization: Bearer dev-key' \
      -H 'Content-Type: application/json' \
      --data-binary '{"q": "sustained"}' \
      -w '%{http_code}' \
      -o /dev/null \
      -s \
      --max-time 5)
  fi

  if [ "$HTTP_CODE" != "200" ] && [ "$HTTP_CODE" != "202" ]; then
    ((FAILED++))
    echo "Request $TOTAL failed: $HTTP_CODE"
  fi

  sleep 0.5
done

echo ""
echo "Sustained test complete: $TOTAL requests, $FAILED failed"

# Expected: 0 failed requests
```

### 7. Verify load balancer failover
```bash
# All requests should now go to remaining replica
echo "=== Verifying all requests go to replica 2 ==="
for i in {1..10}; do
  curl -X GET 'http://localhost:7700/health' \
    -H 'Authorization: Bearer dev-key' \
    -H 'X-Debug-Replica: true' \
    -s | jq '.replica // "unknown"'
  sleep 0.5
done

# Expected: All requests served by replica 2
```

### 8. Recovery: Restart killed replica
```bash
echo "=== Restarting replica 1 ==="
docker start miroir-replica-1

# Wait for health check
sleep 15

# Verify both replicas healthy
curl -X GET 'http://localhost:7701/health' -H 'Authorization: Bearer dev-key' | jq '.'
curl -X GET 'http://localhost:7702/health' -H 'Authorization: Bearer dev-key' | jq '.'
```

### 9. Verify load balancing restored
```bash
echo "=== Verifying load balancing restored ==="
for i in {1..10}; do
  curl -X GET 'http://localhost:7700/health' \
    -H 'Authorization: Bearer dev-key' \
    -H 'X-Debug-Replica: true' \
    -s | jq '.replica // "unknown"'
  sleep 0.5
done

# Expected: Requests distributed across both replicas again
```

## Expected Results

| Metric | Expected Value |
|--------|---------------|
| Immediate searches (50) | 100% success rate (or ≥ 98%) |
| Sustained operations (30s) | 0 failed requests |
| Time to failover | < 1 second (health check interval) |
| Client-visible errors | 0 |

## Success Criteria

- [ ] 0 client-visible errors during replica kill
- [ ] Immediate failover (< 1 second)
- [ ] All 50 immediate searches succeed (or ≥ 98%)
- [ ] Sustained operations complete with 0 failures
- [ ] Load balancer correctly routes to healthy replica
- [ ] Load balancing restored after replica recovery

## Load Balancer Configuration Notes

For proper zero-downtime failover, ensure:

**Health Check Configuration:**
```yaml
# traefik.yml example
healthCheck:
  path: /health
  interval: 5s
  timeout: 2s
  failures: 2
```

**Or nginx:**
```nginx
check interval=5000 rise=2 fall=3 timeout=2000;
```

The health check interval determines failover speed. 5s interval = ~5-10s failover.

## Cleanup
```bash
# Delete test index
curl -X DELETE 'http://localhost:7700/indexes/chaos_test_03' \
  -H 'Authorization: Bearer dev-key'

# Ensure both replicas running
docker start miroir-replica-1 || true
docker start miroir-replica-2 || true

# Stop multi-replica stack
docker-compose -f docker-compose-dev-multi-replica.yml down -v
```
