# Migrating from Meilisearch: Dump and Reload

**Use this option if:** Your existing Meilisearch index is **under 10 GB** and you can tolerate brief downtime during the export/import.

**Migration time:** 1-2 hours for 10 GB (network and disk dependent)

---

## Overview

1. Export a dump from your existing Meilisearch instance
2. Deploy Miroir
3. Import the dump via Miroir's streaming router (default) — documents are routed to their owning shards during import
4. Fall back to broadcast mode only if Miroir cannot reconstruct your dump variant

---

## Preconditions

- [ ] Existing Meilisearch instance is accessible and healthy
- [ ] Target Miroir cluster is deployed with sufficient capacity (existing corpus size + 20% buffer)
- [ ] Dump version is compatible with Miroir's Meilisearch version (check `GET /version` on both)
- [ ] Network connectivity between old instance and Miroir cluster
- [ ] Admin API key for Miroir

**Capacity check:**

```bash
# Check existing index size
curl https://old-meili.example.com/indexes \
  -H "Authorization: Bearer <master-key>"

# Estimate required storage (corpus + 20% buffer)
# If old corpus is 8 GB, provision at least 10 GB per Miroir node
```

---

## Step-by-Step

### Step 1: Export dump from existing Meilisearch

```bash
# Trigger dump creation
curl -X POST https://old-meili.example.com/dumps \
  -H "Authorization: Bearer <master-key>"

# Response: {"uid":"20240524-123456","status":"enqueued","taskUid":42}

# Poll for completion
curl https://old-meili.example.com/tasks/42 \
  -H "Authorization: Bearer <master-key>"

# When status is "succeeded", note the dump file path
# Download the dump
curl https://old-meili.example.com/dumps/20240524-123456/download \
  -H "Authorization: Bearer <master-key>" \
  --output meilisearch-export.dump
```

**Expected time:** ~5-10 minutes per GB

---

### Step 2: Deploy Miroir

If Miroir is not yet deployed:

```bash
# Add Helm repo
helm repo add miroir https://jedarden.github.io/miroir
helm repo update

# Create namespace and secrets
kubectl create namespace search
kubectl -n search create secret generic miroir-secrets \
  --from-literal=masterKey="<strong-key>" \
  --from-literal=nodeMasterKey="<node-key>" \
  --from-literal=adminApiKey="<admin-key>"
kubectl -n search create secret generic meilisearch-secrets \
  --from-literal=masterKey="<node-key>"

# Install (adjust replica count based on corpus size)
helm install search miroir/miroir \
  --namespace search \
  --values my-values.yaml \
  --set meilisearch.replicas=3 \
  --wait
```

**Verify deployment:**

```bash
kubectl get pods -n search
# All pods should be Running

curl https://search.example.com/health
# {"status":"available"}
```

---

### Step 3: Import dump via Miroir (streaming mode)

**Streaming mode (default, recommended):** Documents are routed to their owning shards during import. No cross-cluster broadcast, no post-import rebalance.

```bash
# Import the dump
curl -X POST https://search.example.com/_miroir/dumps/import \
  -H "Authorization: Bearer <admin-key>" \
  -F "dump=@meilisearch-export.dump" \
  -F "indexUid=myindex"

# Response: {"miroir_task_id":"mtask-00123"}

# Monitor progress
curl https://search.example.com/_miroir/dumps/import/mtask-00123/status \
  -H "Authorization: Bearer <admin-key>"

# Or use miroir-ctl
miroir-ctl task status mtask-00123
```

**Progression:**

| Phase | Description |
|-------|-------------|
| `Parsing` | Reading dump metadata and settings |
| `SettingsBroadcast` | Applying index settings via two-phase broadcast |
| `StreamingDocuments` | Routing documents to owning shards |
| `Complete` | Import finished successfully |

**Expected time:** ~1-2 hours for 10 GB (depends on network and cluster size)

---

### Step 4: Verification

```bash
# Verify document counts match
curl https://old-meili.example.com/indexes/myindex/stats \
  -H "Authorization: Bearer <master-key>" | jq '.numberOfDocuments'

curl https://search.example.com/indexes/myindex/stats \
  -H "Authorization: Bearer <miroir-key>" | jq '.numberOfDocuments'

# Sample query comparison
curl -X POST https://old-meili.example.com/indexes/myindex/search \
  -H "Authorization: Bearer <search-key>" \
  -H "Content-Type: application/json" \
  -d '{"q": "test", "limit": 10}'

curl -X POST https://search.example.com/indexes/myindex/search \
  -H "Authorization: Bearer <miroir-key>" \
  -H "Content-Type: application/json" \
  -d '{"q": "test", "limit": 10}'

# Results should match (ordering may differ slightly due to distributed merge)
```

---

### Step 5: Update application configuration

Update your application to point to Miroir:

```python
# Before
client = meilisearch.Client('https://old-meili.example.com', 'key')

# After
client = meilisearch.Client('https://search.example.com', 'miroir-key')
```

```typescript
// Before
const client = new MeiliSearch({ host: 'https://old-meili.example.com', apiKey: 'key' })

// After
const client = new MeiliSearch({ host: 'https://search.example.com', apiKey: 'miroir-key' })
```

```go
// Before
client := meilisearch.NewClient(meilisearch.ClientConfig{
  Host: "https://old-meili.example.com",
  APIKey: "key",
})

// After
client := meilisearch.NewClient(meilisearch.ClientConfig{
  Host: "https://search.example.com",
  APIKey: "miroir-key",
})
```

---

## Fallback: Broadcast Mode

If Miroir cannot fully reconstruct your dump variant (e.g., custom dump format from a Meilisearch fork), fall back to broadcast mode:

**Warning:** Broadcast mode imports the dump to **every node**, transiently placing 100% of the corpus on each node. This requires manual rebalancing afterward.

```bash
# Set broadcast mode via Helm values
helm upgrade search miroir/miroir \
  --namespace search \
  --values my-values.yaml \
  --set miroir.dump_import.mode=broadcast

# Or modify ConfigMap directly
kubectl edit configmap miroir-config -n search
# Set: miroir.dump_import.mode: broadcast

# Restart proxy pods
kubectl rollout restart deployment miroir-proxy -n search

# Import (now using broadcast mode)
curl -X POST https://search.example.com/_miroir/dumps/import \
  -H "Authorization: Bearer <admin-key>" \
  -F "dump=@meilisearch-export.dump" \
  -F "indexUid=myindex"

# After import completes, rebalance to delete non-owning copies
miroir-ctl rebalance start --index myindex
miroir-ctl rebalance status --watch
```

---

## Rollback

If verification fails or you need to roll back:

```bash
# Point application back to old instance
# (revert SDK configuration changes)

# Delete imported index from Miroir
curl -X DELETE https://search.example.com/indexes/myindex \
  -H "Authorization: Bearer <admin-key>"
```

---

## Troubleshooting

### Import stuck at `SettingsBroadcast`

**Cause:** Two-phase settings broadcast waiting for all nodes to acknowledge.

**Solution:**

```bash
# Check node health
miroir-ctl status

# Verify all nodes are healthy
kubectl get pods -n search -l app=meilisearch

# If a node is degraded, fix it first
kubectl describe pod <pod-name> -n search
```

### Import fails with "incompatible dump format"

**Cause:** Dump format from Meilisearch version not supported by Miroir's nodes.

**Solution:** Check Meilisearch versions match:

```bash
# Old instance
curl https://old-meili.example.com/version

# Miroir nodes
kubectl exec -n search <pod-name> -- curl http://localhost:7700/version
```

If versions differ significantly, either:
1. Upgrade old instance to match Miroir's version before exporting dump
2. Use **re-index** migration instead (see `from-meilisearch-reindex.md`)

### Document counts don't match after import

**Cause:** Streaming router may have failed to route some documents.

**Solution:**

```bash
# Check import task for errors
miroir-ctl task status mtask-00123

# Re-run import if errors found
# (Idempotent — duplicate documents are ignored)

# Or run anti-entropy to detect and repair divergences
miroir-ctl anti-entropy run --index myindex
```

---

## See Also

- [Plan §13.9 — Streaming routed dump import](../plan/plan.md#139-streaming-routed-dump-import)
- [Re-index migration](from-meilisearch-reindex.md) — for large corpora
- [Live cutover migration](from-meilisearch-live-cutover.md) — for zero-downtime
- [Troubleshooting Guide](../troubleshooting.md) — common issues and solutions
