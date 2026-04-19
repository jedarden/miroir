# nodeMasterKey Zero-Downtime Rotation

> Rotates the admin-scoped child key (`nodeMasterKey`) that Miroir uses to
> authenticate with Meilisearch nodes. **No maintenance window required.**
>
> This is NOT the startup master key (`MEILI_MASTER_KEY`). For that, see
> [startup-master-key-rotation.md](startup-master-key-rotation.md).

## Background (plan §9)

Meilisearch allows multiple admin-scoped keys (created via `POST /keys` with
`actions: ["*"]`, `indexes: ["*"]`) to coexist. The `nodeMasterKey` in Miroir
config is one such key. Because old and new keys are both valid until the old
one is explicitly deleted, rotation is zero-downtime.

## Prerequisites

- `miroir-ctl` binary built from this repo
- Admin API key (`MIROIR_ADMIN_API_KEY` env var, credentials file, or `--admin-key`)
- Current `nodeMasterKey` value (`--current-key` or `MIROIR_NODE_MASTER_KEY` env var)
- Miroir admin API reachable (default `http://localhost:8080`, override with `--api-url`)
- `kubectl` access to update the K8s secret and restart Miroir pods

## Quick Start

```bash
# Dry-run — prints the plan without executing
miroir-ctl key rotate-node-master --dry-run \
  --current-key "$MIROIR_NODE_MASTER_KEY"

# Live rotation with auto-discovered nodes
miroir-ctl key rotate-node-master \
  --current-key "$MIROIR_NODE_MASTER_KEY"

# Live rotation with explicit nodes
miroir-ctl key rotate-node-master \
  --current-key "$MIROIR_NODE_MASTER_KEY" \
  --node http://meili-0.search.svc:7700 \
  --node http://meili-1.search.svc:7700 \
  --node http://meili-2.search.svc:7700
```

## What the CLI Does (4 steps)

### Step 1 — Create new admin-scoped key on every Meilisearch node

`POST /keys` with `actions: ["*"]`, `indexes: ["*"]`. If any node fails, the
CLI rolls back by deleting the new key from all nodes where creation succeeded.

### Step 2 — Print K8s Secret update instructions

The CLI prints a `kubectl patch secret` command. Apply it:

```bash
kubectl -n search patch secret miroir-keys \
  -p '{"stringData":{"nodeMasterKey":"<new-key>"}}'
```

Or update your ExternalSecret / OpenBao source and wait for ESO to sync.

### Step 3 — Rolling restart Miroir pods

```bash
kubectl -n search rollout restart deployment/miroir
kubectl -n search rollout status deployment/miroir
```

During rollout, pods with the old key and pods with the new key both
authenticate against Meilisearch — no downtime.

The CLI pauses and waits for you to confirm all pods are running.

### Step 4 — Delete old admin-scoped key

The CLI finds the old key UID via `GET /keys` (matching by prefix) and deletes
it from all Meilisearch nodes with `DELETE /keys/{uid}`.

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--dry-run` | false | Print plan without executing |
| `--current-key` | env `MIROIR_NODE_MASTER_KEY` | Current key (required) |
| `--node` | auto-discovered | Meilisearch node URLs (repeatable) |
| `--key-name` | `miroir-node-master` | Name for the new key |
| `--expires-at` | none | Optional ISO 8601 expiration |
| `--namespace` | `search` | K8s namespace |
| `--secret-name` | `miroir-keys` | K8s Secret name |
| `--yes` | false | Skip confirmation prompts |

## Manual Steps (if CLI is unavailable)

1. **Create new key** on each Meilisearch node:
   ```bash
   for i in 0 1 2 3; do
     curl -s -X POST "http://meili-${i}.search.svc:7700/keys" \
       -H "Authorization: Bearer $CURRENT_KEY" \
       -H "Content-Type: application/json" \
       -d '{"name":"miroir-node-master","description":"rotated key","actions":["*"],"indexes":["*"]}' \
       | jq '{uid,key}'
   done
   ```

2. **Update secret** with the new key value from step 1.

3. **Rolling restart** Miroir deployment.

4. **Delete old key** — list keys, find the old one by prefix match, delete by UID:
   ```bash
   curl -s http://meili-0.search.svc:7700/keys \
     -H "Authorization: Bearer $NEW_KEY" | jq '.results[] | {uid,key,name}'
   # Then DELETE /keys/{old-uid} on each node
   ```

## Verification

```bash
# Confirm Miroir is healthy
curl -s http://miroir.search.svc:7700/health

# Check topology
miroir-ctl status

# Test search
curl -s http://miroir.search.svc:7700/indexes/test-index/search \
  -H "Authorization: Bearer $MIROIR_MASTER_KEY" \
  -d '{"q": ""}'
```

## Cadence

- Rotate on suspected compromise (immediately)
- Rotate proactively every 90 days
- Chain after startup-master rotation (see below)

## Relationship to Startup Master Rotation

If you have just rotated `MEILI_MASTER_KEY` (see
[startup-master-key-rotation.md](startup-master-key-rotation.md)), the new
Meilisearch nodes have no admin-scoped child keys yet. Create one using the
new master key, then run this zero-downtime flow to rotate it.

## See Also

- [startup-master-key-rotation.md](startup-master-key-rotation.md) — startup master (requires maintenance window)
- Plan §9 — full secrets handling documentation
- `miroir-ctl key rotate-node-master --dry-run` — preview the rotation plan
