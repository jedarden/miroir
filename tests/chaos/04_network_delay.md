# Chaos Test 04: Network Delay 500ms

## Objective
Verify that introducing network latency (500ms delay) on one node causes search operations to slow down but not fail, testing timeout handling and circuit breaker behavior.

## Prerequisites
- Linux host with `tc` (traffic control) installed
- 3-node Meilisearch cluster running
- Miroir orchestrator healthy
- Docker bridge network identified

## Test Steps

### 1. Setup: Create test index
```bash
curl -X POST 'http://localhost:7700/indexes' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "uid": "chaos_test_04",
    "primaryKey": "id"
  }'

# Add documents
curl -X POST 'http://localhost:7700/indexes/chaos_test_04/documents' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '[
    {"id": 1, "title": "Network Test 1", "category": "A"},
    {"id": 2, "title": "Network Test 2", "category": "B"},
    {"id": 3, "title": "Network Test 3", "category": "A"},
    {"id": 4, "title": "Network Test 4", "category": "B"},
    {"id": 5, "title": "Network Test 5", "category": "C"}
  ]'

sleep 5
```

### 2. Baseline: Measure normal latency
```bash
echo "=== Baseline Latency (10 searches) ==="
for i in {1..10}; do
  TIME=$(curl -X POST 'http://localhost:7700/indexes/chaos_test_04/search' \
    -H 'Authorization: Bearer dev-key' \
    -H 'Content-Type: application/json' \
    --data-binary '{"q": "network"}' \
    -w '\nTotal time: %{time_total}s\n' \
    -o /dev/null \
    -s)

  echo "Search $i: $TIME"
done

# Calculate average
# Expected: < 100ms per search under normal conditions
```

### 3. Identify Docker network interface
```bash
# Find the bridge network for meili-1
docker inspect miroir-meili-1 | jq '.[0].NetworkSettings.Networks | keys | .[0]'

# Get the network interface name
NETWORK_NAME=$(docker inspect miroir-meili-1 | jq -r '.[0].NetworkSettings.Networks | keys | .[0]')
echo "Network: $NETWORK_NAME"

# Get the veth interface (might need adjustment based on setup)
ip addr | grep -A 2 "veth" | head -20
```

### 4. Apply network delay to meili-1
```bash
# Get the container's network interface
CONTAINER_PID=$(docker inspect miroir-meili-1 | jq '.[0].State.Pid')
echo "Container PID: $CONTAINER_PID"

# Get the interface inside the container's network namespace
# We'll use tc on the host targeting the veth interface

# First, find the veth pair for meili-1
VETH_IF=$(ip link | grep -B 1 "^[0-9]*: veth.*@if${CONTAINER_PID}:" | head -1 | sed 's/^[0-9]*: //; s/:@.*//')

if [ -z "$VETH_IF" ]; then
  # Alternative: find by container ID
  CONTAINER_ID=$(docker ps -qf "name=miroir-meili-1")
  VETH_IF=$(ip link | grep "veth" | grep -i "$CONTAINER_ID" | head -1 | sed 's/^[0-9]*: //; s/:.*//')
fi

echo "Using interface: $VETH_IF"

# Apply 500ms delay with 25ms jitter
sudo tc qdisc add dev $VETH_IF root netem delay 500ms 25ms

# Verify
sudo tc qdisc show dev $VETH_IF

echo "Network delay applied: 500ms ± 25ms on $VETH_IF"
```

### 5. Test: Searches with network delay
```bash
echo "=== Searches with 500ms delay (10 searches) ==="
for i in {1..10}; do
  START=$(date +%s%N)
  HTTP_CODE=$(curl -X POST 'http://localhost:7700/indexes/chaos_test_04/search' \
    -H 'Authorization: Bearer dev-key' \
    -H 'Content-Type: application/json' \
    --data-binary '{"q": "network"}' \
    -w '%{http_code}' \
    -o /tmp/search_result_$i.json \
    --max-time 10 \
    -s)
  END=$(date +%s%N)

  ELAPSED=$(( (END - START) / 1000000 ))
  HITS=$(jq '.hits | length' /tmp/search_result_$i.json)

  if [ "$HTTP_CODE" = "200" ]; then
    echo "Search $i: ✓ (${ELAPSED}ms, $HITS hits)"
  else
    echo "Search $i: ✗ HTTP $HTTP_CODE (${ELAPSED}ms)"
  fi
done

# Expected: All searches succeed but take longer (500ms - 2000ms)
# Expected: No timeout errors
```

### 6. Test: Parallel searches (concurrent load)
```bash
echo "=== Parallel searches (20 concurrent) ==="
START=$(date +%s)

for i in {1..20}; do
  (
    curl -X POST 'http://localhost:7700/indexes/chaos_test_04/search' \
      -H 'Authorization: Bearer dev-key' \
      -H 'Content-Type: application/json' \
      --data-binary '{"q": "network"}' \
      -w "Search $i: %{http_code} in %{time_total}s\n" \
      -o /dev/null \
      --max-time 10 \
      -s
  ) &
done

wait

END=$(date +%s)
echo "Total time: $((END - START))s for 20 concurrent searches"

# Expected: All complete, may take longer due to queuing
```

### 7. Test: Timeout boundary
```bash
echo "=== Testing timeout boundaries ==="

# Search with very short timeout (should timeout)
echo "Short timeout test (1s):"
curl -X POST 'http://localhost:7700/indexes/chaos_test_04/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "network"}' \
  -w '\nHTTP: %{http_code}, Time: %{time_total}s\n' \
  --max-time 1 \
  -v

# Expected: May timeout or succeed if hedging works

# Search with adequate timeout (should succeed)
echo "Normal timeout test (10s):"
curl -X POST 'http://localhost:7700/indexes/chaos_test_04/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "network"}' \
  -w '\nHTTP: %{http_code}, Time: %{time_total}s\n' \
  --max-time 10 \
  -s | jq '.'

# Expected: Always succeeds
```

### 8. Test: Circuit breaker behavior
```bash
echo "=== Testing circuit breaker (rapid requests) ==="

# Make 50 rapid requests to trigger circuit breaker if it exists
FAILED=0
SUCCESS=0
TIMEOUT=0

for i in {1..50}; do
  RESPONSE=$(curl -X POST 'http://localhost:7700/indexes/chaos_test_04/search' \
    -H 'Authorization: Bearer dev-key' \
    -H 'Content-Type: application/json' \
    --data-binary '{"q": "network"}' \
    -w '\n%{http_code}\n' \
    --max-time 5 \
    -s 2>&1)

  HTTP_CODE=$(echo "$RESPONSE" | tail -1)

  if [ "$HTTP_CODE" = "200" ]; then
    ((SUCCESS++))
  elif [ "$HTTP_CODE" = "000" ]; then
    ((TIMEOUT++))
  else
    ((FAILED++))
  fi

  sleep 0.05
done

echo "Results: $SUCCESS success, $FAILED failed, $TIMEOUT timeout"

# Expected: Most succeed, circuit may open after consecutive timeouts
```

### 9. Cleanup: Remove network delay
```bash
echo "=== Removing network delay ==="

# Remove the tc rule
sudo tc qdisc del dev $VETH_IF root

# Verify removal
sudo tc qdisc show dev $VETH_IF

echo "Network delay removed from $VETH_IF"
```

### 10. Verify: Normal operation restored
```bash
echo "=== Post-recovery latency check (10 searches) ==="
for i in {1..10}; do
  TIME=$(curl -X POST 'http://localhost:7700/indexes/chaos_test_04/search' \
    -H 'Authorization: Bearer dev-key' \
    -H 'Content-Type: application/json' \
    --data-binary '{"q": "network"}' \
    -w '\nTotal time: %{time_total}s\n' \
    -o /dev/null \
    -s)

  echo "Search $i: $TIME"
done

# Expected: Latency returns to baseline (< 100ms)
```

## Expected Results

| Metric | Expected Value |
|--------|---------------|
| Single search latency | 500ms - 2000ms (increased from baseline) |
| Timeout errors | 0 (with adequate timeout) |
| Success rate | 100% (with 10s timeout) |
| Parallel searches | All complete, may queue |
| Post-recovery latency | Returns to baseline |

## Success Criteria

- [ ] All searches succeed with 500ms delay (no permanent failures)
- [ ] Latency increases proportionally to delay (500ms - 2000ms)
- [ ] No data loss or corruption
- [ ] Circuit breaker activates if configured (optional)
- [ ] Normal latency restored after removing delay
- [ ] No timeout errors with 10s timeout

## Troubleshooting

**tc command not found:**
```bash
# Install on Debian/Ubuntu
sudo apt-get install iproute2

# Install on RHEL/CentOS
sudo yum install iproute
```

**Cannot find veth interface:**
```bash
# List all veth interfaces
ip link | grep veth

# Use container PID to find
docker inspect miroir-meili-1 | jq '.[0].State.Pid'

# Alternative: apply delay inside container
docker exec miroir-meili-1 tc qdisc add dev eth0 root netem delay 500ms
```

**Cleanup if stuck:**
```bash
# Remove all tc rules
sudo tc qdisc del dev $VETH_IF root 2>/dev/null || true

# Or reset all
docker exec miroir-meili-1 tc qdisc del dev eth0 root 2>/dev/null || true
```

## Cleanup
```bash
# Remove network delay (if not already done)
sudo tc qdisc del dev $VETH_IF root 2>/dev/null || true

# Delete test index
curl -X DELETE 'http://localhost:7700/indexes/chaos_test_04' \
  -H 'Authorization: Bearer dev-key'

# Clean up temp files
rm -f /tmp/search_result_*.json
```
