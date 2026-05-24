# Chaos Test 06: Kill Node Mid-Rebalance

## Objective
Verify that killing a node during a rebalance operation pauses the rebalance, resumes on restart, and causes no data loss.

## Prerequisites
- 3-node Meilisearch cluster running
- Miroir orchestrator healthy
- Understanding of how to trigger a rebalance

## Test Steps

### 1. Setup: Create test index with initial data
```bash
# Create index
curl -X POST 'http://localhost:7700/indexes' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "uid": "chaos_test_06",
    "primaryKey": "id"
  }'

# Add initial documents
echo "Adding initial documents..."
for i in {1..100}; do
  curl -X POST 'http://localhost:7700/indexes/chaos_test_06/documents' \
    -H 'Authorization: Bearer dev-key' \
    -H 'Content-Type: application/json' \
    --data-binary "{\"id\": $i, \"title\": \"Initial Doc $i\", \"batch\": 1}" \
    -s >/dev/null
done

sleep 10

# Verify initial count
INITIAL_COUNT=$(curl -X POST 'http://localhost:7700/indexes/chaos_test_06/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "", "limit": 200}' \
  -s | jq '.totalHits')

echo "Initial document count: $INITIAL_COUNT"

# Expected: 100 documents
```

### 2. Trigger a rebalance operation
```bash
echo "=== Triggering rebalance ==="

# Method 1: Add a new node (if supported)
# This would trigger automatic rebalancing

# Method 2: Use Miroir's rebalance API (if exposed)
curl -X POST 'http://localhost:7700/admin/rebalance' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "strategy": "rendezvous",
    "shards": 64
  }' \
  -v

# Method 3: Change replica count (triggers rebalance)
curl -X PATCH 'http://localhost:7700/indexes/chaos_test_06/settings' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "replicationFactor": 3
  }' \
  -v

# Get the rebalance task ID
REBALANCE_TASK=$(curl -X POST 'http://localhost:7700/tasks' \
  -H 'Authorization: Bearer dev-key' \
  -s | jq '.uid // empty')

echo "Rebalance initiated. Task: $REBALANCE_TASK"
```

### 3. Monitor rebalance progress
```bash
echo "=== Monitoring rebalance progress ==="

# Check rebalance status
for i in {1..10}; do
  echo "Check $i:"

  STATUS=$(curl -X GET 'http://localhost:7700/tasks' \
    -H 'Authorization: Bearer dev-key' \
    -s | jq ".results[] | select(.uid == $REBALANCE_TASK) | .status")

  echo "  Status: $STATUS"

  # Wait for rebalance to be actively running
  if [ "$STATUS" = "\"processing\"" ] || [ "$STATUS = "\"enqueued\"" ]; then
    echo "  ✓ Rebalance is running"
    break
  fi

  sleep 2
done

# Give rebalance time to make progress
echo "Letting rebalance progress for 5 seconds..."
sleep 5
```

### 4. Kill a node mid-rebalance
```bash
echo "=== KILLING NODE MID-REBALANCE ==="

# Kill meili-1 while rebalance is in progress
docker stop miroir-meili-1
docker kill miroir-meili-1

echo "Node meili-1 killed during rebalance!"
```

### 5. Monitor: Rebalance behavior
```bash
echo "=== Monitoring rebalance after node kill ==="

# Check task status immediately
sleep 2

TASK_STATUS=$(curl -X GET "http://localhost:7700/tasks/$REBALANCE_TASK" \
  -H 'Authorization: Bearer dev-key' \
  -s 2>/dev/null | jq '.status // "unknown"')

echo "Task status after kill: $TASK_STATUS"

# Expected behaviors:
# - "paused": Rebalance paused waiting for node
# - "failed": Rebalance failed
# - "processing": Rebalance continuing with remaining nodes

# Monitor for 30 seconds
for i in {1..6}; do
  sleep 5

  HEALTH=$(curl -X GET 'http://localhost:7700/health' \
    -H 'Authorization: Bearer dev-key' \
    -s | jq '.')

  echo "[$i] Health: $HEALTH"

  # Check task status
  TASK_STATUS=$(curl -X GET "http://localhost:7700/tasks/$REBALANCE_TASK" \
    -H 'Authorization: Bearer dev-key' \
    -s 2>/dev/null | jq '.status // "unknown"')

  echo "[$i] Task: $TASK_STATUS"
done
```

### 6. Verify: Data integrity during pause
```bash
echo "=== Verifying data integrity while rebalance paused ==="

# Get document count
CURRENT_COUNT=$(curl -X POST 'http://localhost:7700/indexes/chaos_test_06/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "", "limit": 200}' \
  -s | jq '.totalHits')

echo "Document count during pause: $CURRENT_COUNT"

# Verify we can still search
echo "Search test during pause:"
curl -X POST 'http://localhost:7700/indexes/chaos_test_06/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "initial"}' \
  -s | jq '{hits: .hits | length, totalHits: .totalHits}'

# Expected:
# - Count equals initial count (no data loss)
# - Searches still work
```

### 7. Restart the killed node
```bash
echo "=== Restarting killed node ==="
docker start miroir-meili-1

# Wait for health
echo "Waiting for node to be healthy..."
for i in {1..30}; do
  if curl -X GET 'http://localhost:7702/health' \
    -H 'Authorization: Bearer dev-node-key' \
    --max-time 2 \
    -s >/dev/null 2>&1; then
    echo "✓ Node is healthy"
    break
  fi
  sleep 1
done
```

### 8. Monitor: Rebalance resumes
```bash
echo "=== Monitoring rebalance resumption ==="

# Watch for task to resume
for i in {1..12}; do
  sleep 5

  TASK_STATUS=$(curl -X GET "http://localhost:7700/tasks/$REBALANCE_TASK" \
    -H 'Authorization: Bearer dev-key' \
    -s 2>/dev/null | jq '.status // "unknown"')

  echo "[$i] Task status: $TASK_STATUS"

  # Check if it completed
  if [ "$TASK_STATUS" = "\"succeeded\"" ]; then
    echo "✓ Rebalance completed successfully!"
    break
  fi

  # Check if it resumed processing
  if [ "$TASK_STATUS" = "\"processing\"" ]; then
    echo "✓ Rebalance resumed processing"
  fi
done

# Wait a bit more for completion
sleep 10
```

### 9. Verify: Rebalance completed
```bash
echo "=== Final rebalance status ==="

FINAL_STATUS=$(curl -X GET "http://localhost:7700/tasks/$REBALANCE_TASK" \
  -H 'Authorization: Bearer dev-key' \
  -s | jq '.')

echo "$FINAL_STATUS" | jq '.'

# Check for success
if echo "$FINAL_STATUS" | jq -e '.status == "succeeded"' >/dev/null; then
  echo "✓ Rebalance succeeded"
else
  echo "⚠️  Rebalance did not succeed. Status: $(echo "$FINAL_STATUS" | jq -r '.status')"
fi
```

### 10. Verify: No data loss
```bash
echo "=== Verifying no data loss ==="

# Get final document count
FINAL_COUNT=$(curl -X POST 'http://localhost:7700/indexes/chaos_test_06/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "", "limit": 200}' \
  -s | jq '.totalHits')

echo "Initial count: $INITIAL_COUNT"
echo "Final count: $FINAL_COUNT"

if [ "$FINAL_COUNT" -eq "$INITIAL_COUNT" ]; then
  echo "✓ No data loss!"
else
  echo "✗ DATA LOSS DETECTED!"
  echo "  Lost $((INITIAL_COUNT - FINAL_COUNT)) documents"
fi

# Verify specific documents
echo "Verifying specific documents exist:"
for doc_id in 1 50 100; do
  FOUND=$(curl -X POST 'http://localhost:7700/indexes/chaos_test_06/search' \
    -H 'Authorization: Bearer dev-key' \
    -H 'Content-Type: application/json' \
    --data-binary "{\"q\": \"\", \"filter\": \"id = $doc_id\"}" \
    -s | jq '.hits | length')

  if [ "$FOUND" -gt 0 ]; then
    echo "  ✓ Document $doc_id exists"
  else
    echo "  ✗ Document $doc_id MISSING"
  fi
done
```

### 11. Verify: Data distributed correctly
```bash
echo "=== Verifying shard distribution ==="

# Check each node directly
for i in 0 1 2; do
  PORT=$((7701 + i))
  echo "Node meili-$i:"

  COUNT=$(curl -X POST "http://localhost:$PORT/indexes/chaos_test_06/search" \
    -H 'Authorization: Bearer dev-node-key' \
    -H 'Content-Type: application/json' \
    --data-binary '{"q": "", "limit": 200}' \
    -s | jq '.totalHits // "error"')

  echo "  Documents on node: $COUNT"
done

# Expected: Documents distributed according to RF and shard assignment
```

### 12. Verify: System fully operational
```bash
echo "=== Testing system is fully operational ==="

# Search
echo "Search test:"
curl -X POST 'http://localhost:7700/indexes/chaos_test_06/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "initial", "limit": 10}' \
  -s | jq '.hits | length'

# Add new document
echo "Write test:"
curl -X POST 'http://localhost:7700/indexes/chaos_test_06/documents' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  -i \
  --data-binary '{"id": 999, "title": "Post-Rebalance Doc"}'

sleep 3

# Verify write
curl -X POST 'http://localhost:7700/indexes/chaos_test_06/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "post-rebalance"}' \
  -s | jq '.'

# Expected: All operations work normally
```

## Expected Results

| Phase | Expected Behavior |
|-------|-------------------|
| Rebalance start | Task status: "processing" or "enqueued" |
| Node kill mid-rebalance | Task pauses or continues gracefully |
| During pause | No data loss, searches still work |
| Node restart | Task resumes automatically |
| Completion | Task status: "succeeded" |
| Final state | No data loss, all data accessible |

## Success Criteria

- [ ] Rebalance pauses or continues gracefully when node killed
- [ ] No data loss during or after rebalance
- [ ] Rebalance resumes automatically when node returns
- [ ] Rebalance completes successfully
- [ ] All operations (read/write) work after recovery
- [ ] Data correctly distributed across nodes

## Rebalance Trigger Methods

If Miroir doesn't have a direct rebalance API, you can trigger rebalance by:

1. **Adding a node**: Add a new Meilisearch node to the cluster
2. **Changing RF**: Modify replication factor settings
3. **Resharding**: Change the number of shards
4. **Manual trigger**: Use miroir-ctl to initiate rebalance

## Cleanup
```bash
# Delete test index
curl -X DELETE 'http://localhost:7700/indexes/chaos_test_06' \
  -H 'Authorization: Bearer dev-key'

# Ensure all nodes are running
docker start miroir-meili-0 || true
docker start miroir-meili-1 || true
docker start miroir-meili-2 || true
```
