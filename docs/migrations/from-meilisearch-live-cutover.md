# Migrating from Meilisearch: Live Cutover

**Use this option if:** You need **zero downtime** and can afford to run both clusters in parallel during migration.

**Migration time:** 2-4 hours for setup + verification, plus cutover window

---

## Overview

1. Deploy Miroir alongside the existing Meilisearch instance
2. Configure dual-write (both old and new receive writes)
3. Backfill historical data to Miroir (optional but recommended)
4. Switch read traffic to Miroir; verify
5. Switch write traffic to Miroir only
6. Decommission old instance

This approach guarantees no data loss and allows rollback at any step.

---

## Why Live Cutover?

**Advantages:**
- **Zero downtime** — Old instance continues serving throughout
- **Instant rollback** — Can revert to old instance at any point
- **Data consistency** — Dual-write ensures both clusters stay in sync
- **Low risk** — Can verify Miroir under real traffic before full cutover

**Trade-offs:**
- **Longer migration window** — Both clusters running for extended period
- **Higher resource usage** — Need capacity for 2× corpus during migration
- **Dual-write complexity** — Need to handle partial failures

---

## Preconditions

- [ ] Sufficient capacity to run both clusters during migration
- [ ] Ability to modify write path to dual-write
- [ ] Network connectivity between both clusters and your application
- [ ] Admin API key for Miroir
- [ ] Monitoring in place to detect issues during cutover

**Capacity planning:**

```bash
# Plan for 2× storage during migration
# If old corpus is 30 GB, provision at least 60 GB per Miroir node
# Account for write amplification during dual-write period

# Temporary resource scaling
kubectl scale statefulset search-meili -n search --replicas=5
```

---

## Step-by-Step

### Step 1: Deploy Miroir alongside old instance

```bash
# Add Helm repo
helm repo add miroir https://jedarden.github.io/miroir
helm repo update

# Create namespace for Miroir (or use existing)
kubectl create namespace search-new

# Create secrets
kubectl -n search-new create secret generic miroir-secrets \
  --from-literal=masterKey="<strong-key>" \
  --from-literal=nodeMasterKey="<node-key>" \
  --from-literal=adminApiKey="<admin-key>"
kubectl -n search-new create secret generic meilisearch-secrets \
  --from-literal=masterKey="<node-key>"

# Install Miroir
helm install search-new miroir/miroir \
  --namespace search-new \
  --values my-values.yaml \
  --wait
```

**Verify deployment:**

```bash
kubectl get pods -n search-new
curl https://search-new.example.com/health
# {"status":"available"}
```

---

### Step 2: Create indexes and copy settings

```bash
# For each index in the old instance:
curl -X POST https://search-new.example.com/indexes \
  -H "Authorization: Bearer <admin-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "uid": "products",
    "primaryKey": "product_id"
  }'

# Copy settings from old instance
curl https://old-meili.example.com/indexes/products/settings \
  -H "Authorization: Bearer <master-key>" | \
  curl -X PATCH https://search-new.example.com/indexes/products/settings \
    -H "Authorization: Bearer <admin-key>" \
    -H "Content-Type: application/json" \
    -d @-
```

---

### Step 3: Configure dual-write

Update your indexing pipeline to write to both instances:

**Example: Python SDK**

```python
import meilisearch

# Initialize both clients
old_client = meilisearch.Client('https://old-meili.example.com', 'old-key')
new_client = meilisearch.Client('https://search-new.example.com', 'miroir-key')

def add_documents_dual(index_uid, documents):
    """Write to both clusters with error handling."""
    old_index = old_client.index(index_uid)
    new_index = new_client.index(index_uid)

    # Write to old instance (primary during migration)
    old_task = old_index.add_documents(documents)

    # Write to Miroir (will become primary)
    new_task = new_index.add_documents(documents)

    # Log both task IDs for reconciliation
    return {
        'old_task_id': old_task.task_uid,
        'new_task_id': new_task.task_uid
    }

def update_document_dual(index_uid, document_id, document):
    """Update on both clusters."""
    old_index = old_client.index(index_uid)
    new_index = new_client.index(index_uid)

    # Parallel writes for latency
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        old_future = executor.submit(old_index.update_documents, [document])
        new_future = executor.submit(new_index.update_documents, [document])

        # Check for errors
        for future in concurrent.futures.as_completed([old_future, new_future]):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Dual-write failed: {e}")
                # Implement retry logic or alerting
```

**Example: Go SDK**

```go
package main

import (
    "github.com/meilisearch/meilisearch-go"
)

type DualWriteClient struct {
    Old *meilisearch.Client
    New *meilisearch.Client
}

func (d *DualWriteClient) AddDocuments(indexUID string, documents []interface{}) error {
    oldIndex := d.Old.Index(indexUID)
    newIndex := d.New.Index(indexUID)

    // Write to both
    oldTask, err := oldIndex.AddDocuments(documents)
    if err != nil {
        return fmt.Errorf("old instance write failed: %w", err)
    }

    newTask, err := newIndex.AddDocuments(documents)
    if err != nil {
        return fmt.Errorf("new instance write failed: %w", err)
    }

    log.Printf("Dual-write: old_task=%d, new_task=%d", oldTask.TaskUID, newTask.TaskUID)
    return nil
}
```

**Deploy the dual-write changes:**

```bash
# Update your indexing service
# (Kubernetes deployment, systemd service, etc.)

# Verify both instances receiving writes
curl https://old-meili.example.com/tasks?limit=5 -H "Authorization: Bearer <master-key>"
curl https://search-new.example.com/tasks?limit=5 -H "Authorization: Bearer <miroir-key>"
```

---

### Step 4: Backfill historical data (optional but recommended)

If you have historical data that isn't being actively updated, backfill it to Miroir:

**Option A: Dump import (if old instance < 10 GB)**

```bash
# Export from old instance
curl -X POST https://old-meili.example.com/dumps \
  -H "Authorization: Bearer <master-key>"

# Download and import to Miroir
curl -X POST https://search-new.example.com/_miroir/dumps/import \
  -H "Authorization: Bearer <admin-key>" \
  -F "dump=@meilisearch-export.dump" \
  -F "indexUid=products"
```

**Option B: Re-index from source**

```bash
# Point your ETL job at Miroir
# (See from-meilisearch-reindex.md for details)
```

---

### Step 5: Switch read traffic to Miroir

Once Miroir has caught up (document counts match), switch read traffic:

```python
# Update your application configuration
# Before
SEARCH_CLIENT = meilisearch.Client('https://old-meili.example.com', 'search-key')

# After
SEARCH_CLIENT = meilisearch.Client('https://search-new.example.com', 'miroir-key')
```

**Monitor during cutover:**

```bash
# Watch for degraded responses
curl -X POST https://search-new.example.com/indexes/products/search \
  -H "Authorization: Bearer <miroir-key>" \
  -H "Content-Type: application/json" \
  -d '{"q": "test"}' \
  -v 2>&1 | grep -i "x-miroir-degraded"

# If X-Miroir-Degraded header appears, some nodes are missing
# Check cluster health:
miroir-ctl status

# Monitor metrics
kubectl top pods -n search-new -l app=miroir-proxy
curl https://search-new.example.com/_miroir/metrics | grep search_requests_total
```

**Expected metrics to watch:**

| Metric | Description | Warning threshold |
|--------|-------------|-------------------|
| `search_duration_seconds` | Query latency | p95 > 2× baseline |
| `search_requests_total{status="500"}` | Error rate | > 0.1% |
| `degraded_node_count` | Missing nodes | > 0 |
| `scatter_gather_failed_total` | Shard queries failed | increasing |

---

### Step 6: Verify and monitor

After switching read traffic, monitor for at least 24 hours:

```bash
# Compare result counts (should match)
curl -X POST https://old-meili.example.com/indexes/products/search \
  -H "Authorization: Bearer <search-key>" \
  -H "Content-Type: application/json" \
  -d '{"q": "laptop", "limit": 100}' | jq '.estimatedTotalHits'

curl -X POST https://search-new.example.com/indexes/products/search \
  -H "Authorization: Bearer <miroir-key>" \
  -H "Content-Type: application/json" \
  -d '{"q": "laptop", "limit": 100}' | jq '.estimatedTotalHits'

# Monitor error rates
curl https://search-new.example.com/_miroir/metrics | grep -E "error|failure"
```

---

### Step 7: Switch write traffic to Miroir only

Once read traffic is stable:

```python
# Update your indexing pipeline
# Before: dual-write to both
# After: write to Miroir only

new_client = meilisearch.Client('https://search-new.example.com', 'miroir-key')
new_client.index('products').add_documents(documents)
```

**Monitor write metrics:**

```bash
# Check task completion
miroir-ctl task list --limit 10

# Verify no backlog
curl https://search-new.example.com/tasks?statuses=enqueued,processing \
  -H "Authorization: Bearer <admin-key>" | jq '.length'
```

---

### Step 8: Decommission old instance

After write traffic has been stable for 24-48 hours:

```bash
# Verify no ongoing dependencies
# (Check logs, dashboards, alerts)

# Stop writes to old instance
# (Already done in Step 7)

# Graceful shutdown
kubectl delete deployment old-meilisearch -n old-namespace

# Or if standalone server:
systemctl stop meilisearch
```

---

## Rollback

At any point before Step 8, you can roll back:

### Rollback read traffic

```python
# Revert to old instance
SEARCH_CLIENT = meilisearch.Client('https://old-meili.example.com', 'search-key')
```

### Rollback write traffic

```python
# Resume dual-write (or revert to old-only)
old_client = meilisearch.Client('https://old-meili.example.com', 'old-key')
new_client = meilisearch.Client('https://search-new.example.com', 'miroir-key')

# Continue dual-write while investigating
```

**Rollback decision matrix:**

| Symptom | Action |
|---------|--------|
| `X-Miroir-Degraded` header present | Check node health; rollback if degraded nodes > 0 |
| p95 latency > 3× baseline | Rollback reads; investigate |
| Error rate > 1% | Immediate rollback |
| Missing search results | Verify counts; rollback if mismatch |
| OOM errors | Scale up or rollback |

---

## Degraded Mode Operation

If Miroir enters degraded state (nodes missing), the `X-Miroir-Degraded` response header indicates which shards are affected:

```bash
curl -X POST https://search-new.example.com/indexes/products/search \
  -H "Authorization: Bearer <miroir-key>" \
  -H "Content-Type: application/json" \
  -d '{"q": "test"}' -v 2>&1 | grep -i "x-miroir-degraded"

# Example output:
# X-Miroir-Degraded: missing_shards=12,17,33; nodes=meili-2,meili-5
```

**Actions:**

1. Check node health: `miroir-ctl status`
2. If transient failure, wait for recovery
3. If permanent failure, trigger rebalance: `miroir-ctl rebalance start`
4. If degraded during cutover window, rollback to old instance

---

## Troubleshooting

### Dual-write failures on Miroir

**Cause:** Network issues or Miroir node degradation.

**Solution:**

```bash
# Check Miroir health
miroir-ctl status

# Retry failed writes
# (Implement exponential backoff in your indexing pipeline)

# If persistent, scale Miroir or rollback writes to old instance
```

### Read queries returning incomplete results

**Cause:** Miroir in degraded state or backfill incomplete.

**Solution:**

```bash
# Check for degraded header
curl -X POST https://search-new.example.com/indexes/products/search \
  -H "Authorization: Bearer <miroir-key>" \
  -H "Content-Type: application/json" \
  -d '{"q": "*"}' -v 2>&1 | grep -i "x-miroir-degraded"

# Compare document counts
curl https://old-meili.example.com/indexes/products/stats -H "Authorization: Bearer <master-key>"
curl https://search-new.example.com/indexes/products/stats -H "Authorization: Bearer <miroir-key>"

# If counts don't match, complete backfill or rollback
```

### High latency after cutover

**Cause:** Insufficient capacity or unoptimized queries.

**Solution:**

```bash
# Check resource usage
kubectl top pods -n search-new

# View query metrics
curl https://search-new.example.com/_miroir/metrics | grep search_duration_seconds

# If CPU/disk saturated, scale up
kubectl scale statefulset search-meili -n search-new --replicas=7
```

### Writes falling behind during dual-write

**Cause:** Write throughput exceeds Miroir capacity.

**Solution:**

```bash
# Check task queue depth
curl https://search-new.example.com/tasks?statuses=enqueued,processing \
  -H "Authorization: Bearer <admin-key>" | jq '.length'

# If backlog growing, scale Miroir or pause writes to old instance
# (Continue writing to Miroir only once caught up)
```

---

## Checklist

### Pre-migration
- [ ] Miroir deployed and healthy
- [ ] Indexes created with matching settings
- [ ] Dual-write implemented and tested
- [ ] Monitoring dashboards ready

### During migration
- [ ] Dual-write enabled and verified
- [ ] Historical data backfilled (optional)
- [ ] Document counts match between clusters
- [ ] Read traffic switched to Miroir
- [ ] Metrics stable for 24+ hours
- [ ] Write traffic switched to Miroir only

### Post-migration
- [ ] Old instance decommissioned
- [ ] DNS/LoadBalancer updated
- [ ] Monitoring updated to remove old instance
- [ ] Documentation updated

---

## See Also

- [Plan §11 — Onboarding](../plan/plan.md#11-onboarding)
- [Dump-reload migration](from-meilisearch-dump.md) — for smaller corpora
- [Re-index migration](from-meilisearch-reindex.md) — for clean slate
- [Troubleshooting Guide](../troubleshooting.md) — common issues and solutions
