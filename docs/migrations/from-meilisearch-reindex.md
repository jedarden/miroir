# Migrating from Meilisearch: Re-index from Source

**Use this option if:** Your existing Meilisearch index is **large (> 10 GB)** or you want clean shard distribution from the start.

**Migration time:** Varies by indexing pipeline speed and corpus size

---

## Overview

1. Deploy Miroir (alongside or separately from the old instance)
2. Point your indexing pipeline at the Miroir endpoint
3. Re-index from your source data (database, queue, etc.)
4. Verify results match
5. Switch read traffic to Miroir
6. Decommission old instance

---

## Why Re-index?

**Advantages:**
- **Clean shard distribution** — Documents are distributed evenly from the start
- **No downtime** — Old instance continues serving traffic during re-index
- **No dump format compatibility issues** — Works regardless of Meilisearch version
- **Fresh start** — No accumulated fragmentation or stale data

**Trade-offs:**
- **Longer migration time** — Need to re-index the entire corpus
- **Source data access required** — Need access to original data source (DB, queue, etc.)
- **Temporary resource usage** — Both clusters running during migration

---

## Preconditions

- [ ] Access to original data source (database, message queue, object storage)
- [ ] Indexing pipeline can be reconfigured to point to a new endpoint
- [ ] Sufficient capacity in Miroir cluster for full corpus
- [ ] Network connectivity between indexing pipeline and Miroir
- [ ] Admin API key for Miroir

**Capacity planning:**

```bash
# Estimate required storage (existing corpus + 20% buffer)
# If old corpus is 50 GB, provision at least 60 GB per Miroir node
# Account for indexing overhead during migration (temporary +15-20%)
```

---

## Step-by-Step

### Step 1: Deploy Miroir

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
  --set meilisearch.replicas=5 \
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

### Step 2: Create indexes in Miroir

Recreate your indexes with the same schema:

```bash
# For each index in your old instance:
curl -X POST https://search.example.com/indexes \
  -H "Authorization: Bearer <admin-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "uid": "products",
    "primaryKey": "product_id"
  }'

# Copy settings from old instance
curl https://old-meili.example.com/indexes/products/settings \
  -H "Authorization: Bearer <master-key>" | \
  curl -X PATCH https://search.example.com/indexes/products/settings \
    -H "Authorization: Bearer <admin-key>" \
    -H "Content-Type: application/json" \
    -d @-
```

---

### Step 3: Re-index from source

Point your indexing pipeline to Miroir:

**Example: Database-based indexing**

```python
# Before
client = meilisearch.Client('https://old-meili.example.com', 'key')

# After
client = meilisearch.Client('https://search.example.com', 'miroir-key')

# Run your indexing job
for batch in fetch_from_database():
    client.index('products').add_documents(batch)
```

**Example: Queue-based indexing (Kafka)**

```bash
# Update consumer configuration to point to Miroir
# Then restart or redeploy consumers

# Or dual-write to both during transition
producer.send('indexing-topic', {
  'endpoints': [
    'https://old-meili.example.com',
    'https://search.example.com'
  ],
  'documents': batch
})
```

**Example: Object storage (S3) bulk import**

```bash
# If your corpus lives in S3, stream directly to Miroir
aws s3 cp s3://my-bucket/documents.jsonl - | \
  curl -X POST https://search.example.com/indexes/products/documents \
    -H "Authorization: Bearer <miroir-key>" \
    -H "Content-Type: application/json" \
    --data-binary @-
```

**Monitor progress:**

```bash
# Check task status
curl https://search.example.com/tasks?limit=1 \
  -H "Authorization: Bearer <admin-key>"

# Or use miroir-ctl
miroir-ctl task list --limit 5
```

---

### Step 4: Verification

```bash
# Compare document counts
curl https://old-meili.example.com/indexes/products/stats \
  -H "Authorization: Bearer <master-key>" | jq '.numberOfDocuments'

curl https://search.example.com/indexes/products/stats \
  -H "Authorization: Bearer <miroir-key>" | jq '.numberOfDocuments'

# Sample query comparison
curl -X POST https://old-meili.example.com/indexes/products/search \
  -H "Authorization: Bearer <search-key>" \
  -H "Content-Type: application/json" \
  -d '{"q": "laptop", "limit": 10}' | jq '.hits | length'

curl -X POST https://search.example.com/indexes/products/search \
  -H "Authorization: Bearer <miroir-key>" \
  -H "Content-Type: application/json" \
  -d '{"q": "laptop", "limit": 10}' | jq '.hits | length'
```

**Tip:** Use `miroir-ctl verify` to check shard coverage:

```bash
miroir-ctl verify
# All shards: 64/64 covered | RF=2 satisfied
```

---

### Step 5: Switch read traffic

Once counts and sample queries match:

```python
# Update your application configuration
client = meilisearch.Client('https://search.example.com', 'miroir-key')

# Deploy the change
```

**Monitor metrics during cutover:**

```bash
# Watch request latency and error rate
kubectl top pods -n search -l app=miroir-proxy

# Check Miroir metrics
curl https://search.example.com/_miroir/metrics | grep search_duration_seconds
```

---

### Step 6: Decommission old instance

After read traffic has been stable for at least 24 hours:

```bash
# Stop writes to old instance
# (Your indexing pipeline should now only write to Miroir)

# Verify no ongoing tasks on old instance
curl https://old-meili.example.com/tasks?statuses=processing,succeeded \
  -H "Authorization: Bearer <master-key>"

# Decommission old instance
kubectl delete deployment old-meilisearch -n old-namespace
# Or shut down your old server
```

---

## Rollback

If issues arise after switching read traffic:

```bash
# Point application back to old instance
# (revert SDK configuration changes)

# Resume writes to old instance if needed
```

No data loss — the old instance retains its full corpus until you decommission it.

---

## Performance Tips

### Accelerate re-indexing

**Increase batch size:**

```python
# Default batch size (often 1000 documents)
client.index('products').add_documents(documents, batch_size=1000)

# Increase for faster ingestion (watch memory)
client.index('products').add_documents(documents, batch_size=5000)
```

**Parallelize indexing:**

```python
from concurrent.futures import ThreadPoolExecutor

def index_batch(batch):
    client.index('products').add_documents(batch)

with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(index_batch, batches)
```

**Temporarily disable search-time features:**

```bash
# Disable typo tolerance and ranking rules during bulk import
curl -X PATCH https://search.example.com/indexes/products/settings \
  -H "Authorization: Bearer <admin-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "typoTolerance": {"enabled": false},
    "rankingRules": ["words:"]}
'

# Re-enable after import completes
curl -X PATCH https://search.example.com/indexes/products/settings \
  -H "Authorization: Bearer <admin-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "typoTolerance": {"enabled": true},
    "rankingRules": ["words", "typo", "proximity", "attribute", "sort", "exactness"]}
'
```

---

## Troubleshooting

### Re-indexing slower than expected

**Check indexing batch size:**

```bash
# Monitor active tasks
miroir-ctl task list --status processing
```

**Check node CPU/disk:**

```bash
kubectl top pods -n search -l app=meilisearch
kubectl exec -n search <pod-name> -- iostat -x 1
```

**Solution:** Increase batch size, add more workers, or scale up nodes.

### Out-of-memory during indexing

**Cause:** Batch size too large or documents contain large fields.

**Solution:**

```bash
# Reduce batch size
# Or enable pagination in indexing pipeline

# Scale up Meilisearch pods temporarily
kubectl scale statefulset search-meili -n search --replicas=5
```

### Search results differ from old instance

**Cause:** Settings mismatch or different Meilisearch versions.

**Solution:**

```bash
# Compare settings side-by-side
diff <(curl -s https://old-meili.example.com/indexes/products/settings -H "Authorization: Bearer <master-key>") \
     <(curl -s https://search.example.com/indexes/products/settings -H "Authorization: Bearer <miroir-key>")

# Check Meilisearch versions
curl https://old-meili.example.com/version
curl https://search.example.com/version
```

### Node failures during re-index

**Cause:** Insufficient resources or network issues.

**Solution:**

```bash
# Check degraded nodes
miroir-ctl status

# View per-node task status
miroir-ctl task status <task-id>

# If a node is degraded, rebalance will redistribute its shards
miroir-ctl rebalance status --watch
```

---

## See Also

- [Plan §11 — Onboarding](../plan/plan.md#11-onboarding)
- [Dump-reload migration](from-meilisearch-dump.md) — for smaller corpora
- [Live cutover migration](from-meilisearch-live-cutover.md) — for zero-downtime
- [Troubleshooting Guide](../troubleshooting.md) — common issues and solutions
