# Scoped Key Rotation for Search UI

> Rotates the scoped Meilisearch keys used by the Search UI feature.
> **Zero-downtime, leader-coordinated rotation across all Miroir pods.**
>
> Part of plan §13.21 — Default search interface (end-user search UI).

## Background (plan §13.21)

The Search UI (`/ui/search/{index}`) never holds a Meilisearch master or node key.
Instead, Miroir holds a **scoped search-only key** for each index with Search UI enabled.
This key is:
- Created via `POST /keys` with `actions: ["search"]` scoped to a single index
- Automatically rotated before expiry by a leader-elected pod
- Coordinated across all pods via Redis shared state and observation beacons

## Prerequisites

- Miroir proxy running with `search_ui.enabled: true`
- Redis task store configured (required for coordination)
- Admin API key for manual rotation (optional — automatic is default)
- Index with Search UI enabled

## Configuration

Key rotation behavior is controlled by these config values (defaults shown):

```yaml
search_ui:
  enabled: true
  scoped_key_max_age_days: 60              # Key hard expiration
  scoped_key_rotate_before_expiry_days: 30  # Rotation trigger (must be < max_age_days)
  scoped_key_rotation_drain_s: 120          # Wait time for straggler pods
```

**Important**: `scoped_key_rotate_before_expiry_days` must be **less than**
`scoped_key_max_age_days`. The Helm chart's `values.schema.json` enforces this
at install time.

## How Automatic Rotation Works

### 1. Leader Election

One pod acquires a leader lease for the index: `search_ui_key_rotation:<index>`.
Only the leader drives rotation (Mode B, §14.5).

### 2. Timing Gate Check

Every hour, the leader checks if rotation is needed:
```
key_age >= (scoped_key_max_age_days - scoped_key_rotate_before_expiry_days)
```
With defaults (60d max, 30d before expiry), rotation triggers when the key is 30 days old.

### 3. Mint New Key

The leader creates a new scoped key via `POST /keys` on all Meilisearch nodes.

### 4. Update Shared State

Redis hash `miroir:search_ui_scoped_key:<index>` is updated:
```json
{
  "primary_uid": "<new-key-uid>",
  "previous_uid": "<old-key-uid>",
  "rotated_at": 1712345678901,
  "generation": 2
}
```

### 5. Observation Beacon

Every pod writes `miroir:search_ui_scoped_key_observed:<pod>:<index>` with a 60s TTL,
refreshing on each use. This tells the leader which pods have seen the new generation.

### 6. Revocation Safety Gate

Before deleting the old key, the leader:
1. Gets the live peer set from peer discovery
2. Checks that every live peer has observed the new generation
3. Waits up to `scoped_key_rotation_drain_s` (default 120s) for stragglers

### 7. Revoke Old Key

Once all pods confirm observation, the leader calls `DELETE /keys/{old-uid}` on all
Meilisearch nodes and clears `previous_uid` from the Redis hash.

## Manual Rotation

### Via Admin UI

1. Navigate to `/ui/search/{index}` in your browser
2. Click "Rotate Scoped Key" in the index settings
3. Optionally enable "Force rotation" to bypass the timing gate
4. Confirm — rotation runs in the background

### Via HTTP API

```bash
# Check if rotation is needed (respects timing gate)
curl -X POST "http://miroir.example.com/_miroir/ui/search/products/rotate-scoped-key" \
  -H "Authorization: Bearer $MIROIR_ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"force": false}'
# Response: {"status":"skipped","index_uid":"products","generation":1,...}

# Force immediate rotation (bypasses timing gate)
curl -X POST "http://miroir.example.com/_miroir/ui/search/products/rotate-scoped-key" \
  -H "Authorization: Bearer $MIROIR_ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"force": true}'
# Response: {"status":"rotated","index_uid":"products","generation":2,"previous_uid_revoked":"old-uid-123"}
```

### Response Fields

| Field | Description |
|-------|-------------|
| `status` | `rotated`, `skipped`, or `drain_pending` |
| `index_uid` | Index name |
| `generation` | New generation number (monotonic counter) |
| `previous_uid_revoked` | Old key UID (only present if revocation completed) |
| `error` | Error message if status is `drain_pending` |

## Timing and Cadence

| Config | Default | Meaning |
|-------|---------|---------|
| `scoped_key_max_age_days` | 60 | Keys expire after 60 days |
| `scoped_key_rotate_before_expiry_days` | 30 | Rotate when key is 30 days old |
| `scoped_key_rotation_drain_s` | 120 | Wait up to 120s for all pods to observe |

**Rotation window**: With defaults, keys are active for ~30 days before rotation.
The old key remains valid during the ~120s drain period, then is revoked.

## Monitoring

### Redis State

```bash
# Check current scoped key for an index
redis-cli --no-auth-warning -h $REDIS_HOST HGETALL "miroir:search_ui_scoped_key:products"

# Check which pods have observed the current generation
redis-cli --no-auth-warning -h $REDIS_HOST KEYS "miroir:search_ui_scoped_key_observed:*:products"
```

### Logs

The leader pod logs rotation progress:
```
INFO new scoped key minted, waiting for pod observation index=products generation=2
INFO all live pods observed new generation, revoking previous key index=products generation=2
INFO previous scoped key revoked index=products previous_uid=old-uid-123
```

## Troubleshooting

### Rotation Stuck in `drain_pending`

**Symptom**: Manual rotation returns `drain_pending` with unobserved pods.

**Causes**:
- A pod is down and not refreshing its beacon
- Network partition preventing beacon writes
- Pod crashed before observing the new generation

**Resolution**:
1. Check live pods: `redis-cli KEYS "miroir:search_ui_scoped_key_observed:*"`
2. Restart stuck pods: `kubectl rollout restart deployment/miroir`
3. On restart, pods read the fresh hash and skip the old UID
4. Retry rotation after all pods are healthy

### Old Key Still Accepted After Rotation

**Expected behavior**: During the drain period (default 120s), both old and new keys work.

**If this persists beyond drain_s**:
- Check Redis hash: `previous_uid` should be cleared
- Check Meilisearch: `GET /keys` should not list the old UID
- Manual cleanup: `DELETE /keys/{old-uid}` on each Meilisearch node

### Key Rotation Loop

**Symptom**: Continuous rotation every hour.

**Cause**: `scoped_key_rotate_before_expiry_days >= scoped_key_max_age_days`

**Resolution**: Fix config to satisfy the constraint:
```yaml
search_ui:
  scoped_key_max_age_days: 60
  scoped_key_rotate_before_expiry_days: 30  # Must be < 60
```

## Verification

After rotation completes:

```bash
# Verify Redis state (previous_uid should be cleared)
redis-cli HGETALL "miroir:search_ui_scoped_key:products"

# Verify old key is deleted from Meilisearch
curl -s "http://meili-0.search.svc:7700/keys" \
  -H "Authorization: Bearer $NODE_MASTER_KEY" | jq '.results[] | select(.key | contains("old-key-prefix"))'

# Test Search UI still works
open "http://miroir.example.com/ui/search/products"
```

## See Also

- Plan §13.21 — Default search interface (full architecture)
- Plan §9 — Secrets handling (JWT rotation, master key rotation)
- `docs/runbooks/node-master-key-rotation.md` — Rotating the node master key
- `docs/ctl/ui.md` — Admin UI CLI reference
