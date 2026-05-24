# Chaos Test 01: Kill 1 of 3 Nodes (RF=2)

## Objective
Verify that with replication factor 2, the system continues operating when one node fails, with degraded writes warning clients via header.

## Preconditions
- 3-node Meilisearch cluster running (RF=2)
- Miroir orchestrator healthy
- Test index with documents indexed

## Test Steps

### 1. Setup: Create test index with data
```bash
# Create index
curl -X POST 'http://localhost:7700/indexes' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "uid": "chaos_test_01",
    "primaryKey": "id"
  }'

# Add documents
curl -X POST 'http://localhost:7700/indexes/chaos_test_01/documents' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '[
    {"id": 1, "title": "Document 1", "shard_hint": "node-0"},
    {"id": 2, "title": "Document 2", "shard_hint": "node-1"},
    {"id": 3, "title": "Document 3", "shard_hint": "node-2"}
  ]'

# Wait for indexing
sleep 5
```

### 2. Verify baseline: Search works
```bash
curl -X POST 'http://localhost:7700/indexes/chaos_test_01/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "document"}'

# Expected: 3 hits, no degradation header
```

### 3. Kill one Meilisearch node
```bash
# Kill meili-1
docker stop miroir-meili-1
docker kill miroir-meili-1

# Verify it's down
docker ps | grep miroir-meili-1  # Should return nothing
```

### 4. Continuous search test (during failure)
```bash
# Run continuous searches while node is down
for i in {1..20}; do
  echo "Search $i:"
  curl -X POST 'http://localhost:7700/indexes/chaos_test_01/search' \
    -H 'Authorization: Bearer dev-key' \
    -H 'Content-Type: application/json' \
    --data-binary '{"q": "document"}' \
    -w '\nHTTP Status: %{http_code}\n' \
    -s
  sleep 1
done

# Expected: All searches succeed (200 OK)
# Expected: May see degraded writes warning header on write operations
```

### 5. Test degraded writes (should warn via header)
```bash
# Attempt to add documents during degraded state
curl -X POST 'http://localhost:7700/indexes/chaos_test_01/documents' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  -i \
  --data-binary '[{"id": 4, "title": "Document 4 (degraded)"}]'

# Expected: 202 Accepted (task created)
# Expected: Warning header: X-Miroir-Degraded-Writes or similar
```

### 6. Verify data consistency
```bash
# Wait for task completion
sleep 5

# Search for all documents
curl -X POST 'http://localhost:7700/indexes/chaos_test_01/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": ""}' \
  | jq '.totalHits'

# Expected: 4 documents (all writes succeeded)
```

### 7. Cleanup: Restart node
```bash
docker start miroir-meili-1

# Wait for health
sleep 10

# Verify cluster recovery
curl -X GET 'http://localhost:7700/health' -H 'Authorization: Bearer dev-key'
```

## Expected Results

| Operation | Expected Behavior |
|-----------|-------------------|
| Search (read) | All queries succeed, 200 OK |
| Write (add documents) | Succeeds with 202, includes degradation warning header |
| Data consistency | No data loss, all documents retrievable |
| Performance | Slight latency increase, but functional |

## Success Criteria

- [ ] All 20 continuous searches succeed (200 OK)
- [ ] Write operations complete with degradation warning header
- [ ] No data loss (all 4 documents retrievable)
- [ ] Node restart detected by Miroir within health check interval
- [ ] Cluster returns to healthy state after node recovery

## Cleanup
```bash
# Delete test index
curl -X DELETE 'http://localhost:7700/indexes/chaos_test_01' \
  -H 'Authorization: Bearer dev-key'

# Ensure node is running
docker start miroir-meili-1 || true
```
