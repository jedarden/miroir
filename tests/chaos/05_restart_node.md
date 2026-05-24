# Chaos Test 05: Restart Killed Node

## Objective
Verify that Miroir detects a restarted node within the health check interval and properly reintegrates it into the cluster.

## Prerequisites
- 3-node Meilisearch cluster running
- Miroir orchestrator healthy
- Test index with data

## Test Steps

### 1. Setup: Create test index and capture initial state
```bash
# Create index
curl -X POST 'http://localhost:7700/indexes' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "uid": "chaos_test_05",
    "primaryKey": "id"
  }'

# Add documents
curl -X POST 'http://localhost:7700/indexes/chaos_test_05/documents' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '[
    {"id": 1, "title": "Recovery Test 1"},
    {"id": 2, "title": "Recovery Test 2"},
    {"id": 3, "title": "Recovery Test 3"}
  ]'

sleep 5

# Capture initial cluster state
echo "=== Initial Cluster State ==="
curl -X GET 'http://localhost:7700/health' -H 'Authorization: Bearer dev-key' | jq '.'

# Note the health check interval from config
# (typically 30-60 seconds)
```

### 2. Verify: All nodes healthy
```bash
echo "=== Checking individual node health ==="

# Check each Meilisearch node directly
for i in 0 1 2; do
  PORT=$((7701 + i))
  echo "Node meili-$i (port $PORT):"
  curl -X GET "http://localhost:$PORT/health" \
    -H 'Authorization: Bearer dev-node-key' \
    -w '\nStatus: %{http_code}\n' \
    -s | jq '.status' || echo "Failed"
done

# Expected: All nodes return "available"
```

### 3. Kill one node
```bash
echo "=== Killing node meili-1 ==="
docker stop miroir-meili-1
docker kill miroir-meili-1

# Verify it's down
echo "Verifying node is down..."
for i in {1..5}; do
  if ! docker ps | grep -q miroir-meili-1; then
    echo "✓ Node meili-1 is down"
    break
  fi
  sleep 1
done
```

### 4. Monitor: Miroir detects failure
```bash
echo "=== Monitoring Miroir failure detection ==="
echo "Waiting for Miroir to detect the failed node..."
echo "(This may take up to the health check interval)"

DETECTED=false
START=$(date +%s)

while [ $(( $(date +%s) - START )) -lt 120 ]; do
  HEALTH=$(curl -X GET 'http://localhost:7700/health' \
    -H 'Authorization: Bearer dev-key' \
    -s | jq '.')

  echo "[$(date +%H:%M:%S)] $HEALTH"

  # Check if node is marked as down
  if echo "$HEALTH" | jq -e '.nodes.meili-1.status == "unavailable"' >/dev/null 2>&1; then
    echo "✓ Miroir detected node failure!"
    DETECTED=true
    break
  fi

  sleep 5
done

if [ "$DETECTED" = false ]; then
  echo "⚠️  Miroir did not detect node failure within 120s"
  echo "This may indicate health check interval is longer than expected"
fi

# Note the detection time for comparison
DETECTION_TIME=$(( $(date +%s) - START ))
echo "Detection time: ${DETECTION_TIME}s"
```

### 5. Verify: System operates with one node down
```bash
echo "=== Verifying continued operation with one node down ==="
curl -X POST 'http://localhost:7700/indexes/chaos_test_05/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "recovery"}' \
  | jq '.'

# Expected: Search succeeds (RF=2)
```

### 6. Restart the killed node
```bash
echo "=== Restarting node meili-1 ==="
docker start miroir-meili-1

# Wait for container to be running
echo "Waiting for container to start..."
for i in {1..30}; do
  if docker ps | grep -q miroir-meili-1; then
    echo "✓ Container is running"
    break
  fi
  sleep 1
done

# Wait for Meilisearch to be healthy
echo "Waiting for Meilisearch to be healthy..."
for i in {1..30}; do
  if curl -X GET 'http://localhost:7702/health' \
    -H 'Authorization: Bearer dev-node-key' \
    --max-time 2 \
    -s >/dev/null 2>&1; then
    echo "✓ Meilisearch is healthy"
    break
  fi
  sleep 1
done
```

### 7. Monitor: Miroir detects recovery
```bash
echo "=== Monitoring Miroir recovery detection ==="
echo "Waiting for Miroir to detect the recovered node..."
echo "(Should take ≤ health check interval)"

RECOVERED=false
START=$(date +%s)

while [ $(( $(date +%s) - START )) -lt 120 ]; do
  HEALTH=$(curl -X GET 'http://localhost:7700/health' \
    -H 'Authorization: Bearer dev-key' \
    -s | jq '.')

  echo "[$(date +%H:%M:%S)] $HEALTH"

  # Check if node is marked as available
  if echo "$HEALTH" | jq -e '.nodes.meili-1.status == "available"' >/dev/null 2>&1; then
    echo "✓ Miroir detected node recovery!"
    RECOVERED=true
    break
  fi

  sleep 5
done

if [ "$RECOVERED" = false ]; then
  echo "⚠️  Miroir did not detect node recovery within 120s"
fi

# Note the recovery time
RECOVERY_TIME=$(( $(date +%s) - START ))
echo "Recovery time: ${RECOVERY_TIME}s"
```

### 8. Verify: Full cluster health
```bash
echo "=== Verifying full cluster health ==="
curl -X GET 'http://localhost:7700/health' -H 'Authorization: Bearer dev-key' | jq '.'

# Check each node directly
echo "Individual node health:"
for i in 0 1 2; do
  PORT=$((7701 + i))
  echo "Node meili-$i:"
  curl -X GET "http://localhost:$PORT/health" \
    -H 'Authorization: Bearer dev-node-key' \
    -s | jq '.status' || echo "Failed"
done

# Expected: All nodes show "available"
```

### 9. Verify: Operations work normally
```bash
echo "=== Testing normal operations ==="

# Search
echo "Search test:"
curl -X POST 'http://localhost:7700/indexes/chaos_test_05/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "recovery"}' \
  | jq '{hits: .hits | length, totalHits: .totalHits}'

# Write
echo "Write test:"
curl -X POST 'http://localhost:7700/indexes/chaos_test_05/documents' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  -i \
  --data-binary '{"id": 4, "title": "Post-Recovery Document"}'

sleep 3

# Verify write
curl -X POST 'http://localhost:7700/indexes/chaos_test_05/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "post-recovery"}' \
  | jq '.'

# Expected: All operations work normally
```

### 10. Verify: Data consistency across nodes
```bash
echo "=== Verifying data consistency ==="

# Get document count from Miroir
MIROIR_COUNT=$(curl -X POST 'http://localhost:7700/indexes/chaos_test_05/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "", "limit": 100}' \
  -s | jq '.totalHits')

echo "Miroir reports $MIROIR_COUNT documents"

# Query each node directly to verify replication
for i in 0 1 2; do
  PORT=$((7701 + i))
  echo "Node meili-$i (port $PORT):"

  COUNT=$(curl -X POST "http://localhost:$PORT/indexes/chaos_test_05/search" \
    -H 'Authorization: Bearer dev-node-key' \
    -H 'Content-Type: application/json' \
    --data-binary '{"q": "", "limit": 100}' \
    -s | jq '.totalHits // "error"')

  echo "  Documents: $COUNT"
done

# Expected: All nodes report the same count (4 documents)
```

## Expected Results

| Phase | Expected Behavior | Timeframe |
|-------|------------------|-----------|
| Failure detection | Miroir marks node as unavailable | ≤ health check interval |
| Degraded operation | Searches/writes continue (RF=2) | Immediate |
| Recovery detection | Miroir marks node as available | ≤ health check interval |
| Full operation | All operations normal | Immediate after recovery |

## Success Criteria

- [ ] Miroir detects node failure within health check interval
- [ ] Miroir detects node recovery within health check interval
- [ ] System continues operating during degraded state
- [ ] All operations return to normal after recovery
- [ ] Data is consistent across all nodes after recovery
- [ ] No manual intervention required

## Health Check Configuration

The health check interval is configured in Miroir's config file:

```yaml
# dev-config.yaml
health_check:
  interval: 30s  # How often to check nodes
  timeout: 5s    # Timeout for individual check
  failures: 2    # Failures before marking unavailable
```

Adjust these values to test different detection scenarios.

## Cleanup
```bash
# Delete test index
curl -X DELETE 'http://localhost:7700/indexes/chaos_test_05' \
  -H 'Authorization: Bearer dev-key'

# Ensure all nodes are running
docker start miroir-meili-0 || true
docker start miroir-meili-1 || true
docker start miroir-meili-2 || true
```
