# Startup Master Key Rotation (Maintenance Window Required)

> **This is NOT the zero-downtime flow.** The zero-downtime rotation applies to
> admin-scoped child keys (`nodeMasterKey`) — see
> `miroir-ctl key rotate-node-master --dry-run`. This runbook covers rotating
> `MEILI_MASTER_KEY`, the startup env var fixed at Meilisearch process start.

## Background (plan §9)

Meilisearch CE has exactly one **startup master key** per process, supplied via
`MEILI_MASTER_KEY`. It is fixed for the life of the process and cannot be
rotated without a restart. All admin-scoped child keys created via `POST /keys`
are validated against this startup master key.

## Prerequisites

- A maintenance window (Meilisearch will be briefly unavailable during pod restarts)
- `kubectl` access to the cluster with write permissions on the target namespace
- The new master key value (generate with `openssl rand -hex 32`)
- Current `nodeMasterKey` value (needed to recreate admin-scoped child keys)

## Steps

### 1. Generate a new startup master key

```bash
NEW_MASTER=$(openssl rand -hex 32)
echo "New master key: $NEW_MASTER"
```

### 2. Update the secret

**Option A — K8s Secret:**

```bash
kubectl -n search patch secret miroir-keys \
  -p "{\"stringData\":{\"nodeMasterKey\":\"$NEW_MASTER\"}}"
```

**Option B — ExternalSecret / OpenBao:**

Update the secret at the external source (e.g., OpenBao KV path
`kv/search/miroir`, property `node_master_key`). Wait for ESO to sync.

### 3. Rolling restart Meilisearch StatefulSet (one pod at a time)

```bash
# Check current StatefulSet name and replica count
kubectl -n search get statefulset

# Rolling restart — one pod at a time to minimize downtime
kubectl -n search rollout restart statefulset/meilisearch
kubectl -n search rollout status statefulset/meilisearch
```

During this phase:
- Each Meilisearch pod restarts with the new `MEILI_MASTER_KEY`
- Admin-scoped child keys created under the old master key are **invalidated**
- Miroir pods cannot authenticate until new admin-scoped keys are created

### 4. Create a new admin-scoped child key on each node

Once all Meilisearch pods are running with the new master key, create a new
admin-scoped key that Miroir will use:

```bash
# For each Meilisearch pod (e.g., meili-0, meili-1, meili-2):
for i in 0 1 2; do
  curl -s -X POST "http://meili-${i}.search.svc:7700/keys" \
    -H "Authorization: Bearer $NEW_MASTER" \
    -H "Content-Type: application/json" \
    -d '{
      "name": "miroir-node-master",
      "description": "Admin-scoped key for Miroir orchestrator",
      "actions": ["*"],
      "indexes": ["*"]
    }' | jq -r '.key'
done
```

Capture the key value from the first node's response (all nodes should produce
the same key when using identical creation parameters, but Meilisearch generates
unique keys — use the value from any single node and recreate on others).

**Important:** `POST /keys` returns the full key value **only once**. Save it.

If keys differ across nodes, note each one and run the zero-downtime rotation
flow for each to converge on a single key.

### 5. Update Miroir's secret with the new admin-scoped key

```bash
# Use the key value captured in step 4
kubectl -n search patch secret miroir-keys \
  -p "{\"stringData\":{\"nodeMasterKey\":\"$ADMIN_SCOPED_KEY\"}}"
```

### 6. Rolling restart Miroir pods

```bash
kubectl -n search rollout restart deployment/miroir
kubectl -n search rollout status deployment/miroir
```

### 7. Verify

```bash
# Check Miroir health
curl -s http://miroir.search.svc:7700/health

# Check topology (requires admin key)
curl -s http://miroir.search.svc:7700/_miroir/topology \
  -H "Authorization: Bearer $MIROIR_ADMIN_API_KEY" | jq .

# Run a test search to confirm end-to-end
curl -s http://miroir.search.svc:7700/indexes/test-index/search \
  -H "Authorization: Bearer $MIROIR_MASTER_KEY" \
  -d '{"q": ""}'
```

## Rollback

If the new master key causes issues:

1. Patch the secret back to the old master key value
2. Rolling restart Meilisearch StatefulSet again
3. Recreate admin-scoped child keys under the old master
4. Update Miroir's secret and restart Miroir pods

## Cadence

- Rotate on suspected compromise (immediately)
- Rotate proactively every 90 days
- Coordinate with `nodeMasterKey` zero-downtime rotation (can chain: startup
  master rotation → zero-downtime child key rotation)

## See Also

- `miroir-ctl key rotate-node-master --dry-run` — zero-downtime child key rotation
- Plan §9 — full secrets handling documentation
