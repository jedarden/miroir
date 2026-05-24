# Chaos Test 02: Kill 2 of 3 Nodes (RF=2)

## Objective
Verify that with 2 of 3 nodes down (RF=2), the system properly handles shard loss with appropriate error responses (503 or partial results per policy).

## Preconditions
- 3-node Meilisearch cluster running (RF=2)
- Miroir orchestrator healthy
- Test index with documents distributed across shards

## Test Steps

### 1. Setup: Create test index with distributed data
```bash
# Create index
curl -X POST 'http://localhost:7700/indexes' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "uid": "chaos_test_02",
    "primaryKey": "id"
  }'

# Add documents (will distribute across shards)
for i in {1..20}; do
  curl -X POST "http://localhost:7700/indexes/chaos_test_02/documents" \
    -H 'Authorization: Bearer dev-key' \
    -H 'Content-Type: application/json' \
    --data-binary "{\"id\": $i, \"title\": \"Document $i\", \"batch\": $((i/10+1))}"
done

# Wait for indexing
sleep 10
```

### 2. Verify baseline: All documents searchable
```bash
curl -X POST 'http://localhost:7700/indexes/chaos_test_02/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "", "limit": 100}' \
  | jq '{totalHits: .totalHits, retrieved: .hits | length}'

# Expected: 20 total hits
```

### 3. Kill first Meilisearch node
```bash
docker stop miroir-meili-0
docker kill miroir-meili-0

echo "Node 0 killed. System should still be operational (RF=2)."
sleep 5
```

### 4. Verify: Still operational (1 node down)
```bash
curl -X POST 'http://localhost:7700/indexes/chaos_test_02/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "", "limit": 100}' \
  -w '\nHTTP Status: %{http_code}\n'

# Expected: 200 OK, all data still available (RF=2 means replicas exist)
```

### 5. Kill second Meilisearch node (critical failure)
```bash
docker stop miroir-meili-1
docker kill miroir-meili-1

echo "CRITICAL: 2 of 3 nodes down. Some shards may be unavailable."
sleep 5
```

### 6. Test: Shard loss behavior
```bash
# Attempt search - behavior depends on policy
echo "=== Testing search with 2 nodes down ==="
curl -X POST 'http://localhost:7700/indexes/chaos_test_02/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "", "limit": 100}' \
  -w '\nHTTP Status: %{http_code}\n' \
  -D -

# Expected behaviors (per policy):
# Option A: 503 Service Unavailable with shard loss error
# Option B: Partial results with warning header indicating degraded response
```

### 7. Test: Write behavior during shard loss
```bash
echo "=== Testing write with 2 nodes down ==="
curl -X POST 'http://localhost:7700/indexes/chaos_test_02/documents' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  -i \
  --data-binary '{"id": 999, "title": "Emergency Document"}'

# Expected: Either 503 (write rejected) or 202 with warning (write queued/failed)
```

### 8. Verify error message format
```bash
# Capture full error response
RESPONSE=$(curl -X POST 'http://localhost:7700/indexes/chaos_test_02/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "document"}' \
  -s)

echo "$RESPONSE" | jq '.'

# Expected error structure (if 503):
# {
#   "message": "shard unavailable: insufficient replicas for shard X",
#   "code": "shard_unavailable",
#   "type": "system_error",
#   "link": "https://docs.miroir.dev/errors/shard_unavailable"
# }
```

### 9. Recovery: Restart nodes
```bash
echo "=== Recovery: Restarting nodes ==="
docker start miroir-meili-0
docker start miroir-meili-1

# Wait for cluster recovery
echo "Waiting for cluster recovery..."
sleep 30

# Verify health
curl -X GET 'http://localhost:7700/health' -H 'Authorization: Bearer dev-key' | jq '.'
```

### 10. Verify: Full functionality restored
```bash
curl -X POST 'http://localhost:7700/indexes/chaos_test_02/search' \
  -H 'Authorization: Bearer dev-key' \
  -H 'Content-Type: application/json' \
  --data-binary '{"q": "", "limit": 100}' \
  | jq '{totalHits: .totalHits, retrieved: .hits | length}'

# Expected: 20 total hits, full functionality restored
```

## Expected Results

| State | Search Behavior | Write Behavior |
|-------|-----------------|----------------|
| 1 node down | 200 OK, full results | 202 with degradation warning |
| 2 nodes down | 503 OR partial results | 503 OR 202 with error |

## Success Criteria

- [ ] System operational with 1 node down (RF=2)
- [ ] System properly handles 2 nodes down per policy (503 or partial)
- [ ] Error messages include clear shard availability information
- [ ] All nodes recover gracefully on restart
- [ ] Full functionality restored after recovery

## Policy Notes

The expected behavior for 2 nodes down depends on Miroir's configuration policy:

**Fail-Closed Policy (Recommended for consistency)**:
- Returns 503 Service Unavailable
- Clear error message indicating shard loss
- No partial results served

**Fail-Open Policy (For availability)**:
- Returns partial results from available shards
- Includes warning header: `X-Miroir-Partial-Results: true`
- Response includes `availableShards` count

## Cleanup
```bash
# Delete test index
curl -X DELETE 'http://localhost:7700/indexes/chaos_test_02' \
  -H 'Authorization: Bearer dev-key'

# Ensure all nodes are running
docker start miroir-meili-0 || true
docker start miroir-meili-1 || true
docker start miroir-meili-2 || true
```
