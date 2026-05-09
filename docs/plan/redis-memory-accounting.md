# Redis Memory Accounting (Plan §14.7)

This document validates the Redis memory usage for the Miroir task store when running in HA mode with multiple replicas.

## Redis Keyspace Overview

The Redis backend for Miroir's task store uses the following keyspace patterns:

### Table Keys (Hash + Index Set)

Each of the 14 tables is stored as:
- `miroir:<table>:<id>` → Hash containing the serialized record
- `miroir:<table>:_index` → Set containing all IDs in the table

Tables:
1. `tasks` - Task registry
2. `node_settings_version` - Per-(index, node) settings freshness
3. `aliases` - Single and multi-target aliases
4. `sessions` - Read-your-writes session pins
5. `idempotency_cache` - Write dedup cache
6. `jobs` - Background jobs
7. `leader_lease` - Coordinator lease
8. `canaries` - Canary definitions
9. `canary_runs` - Canary run history
10. `cdc_cursors` - CDC cursors
11. `tenant_map` - API key → tenant mapping
12. `rollover_policies` - ILM rollover policies
13. `search_ui_config` - Search UI configuration
14. `admin_sessions` - Admin UI sessions

### Special-Purpose Keys

#### Rate Limiting (§13.21)
- `miroir:ratelimit:searchui:<ip>` - Per-IP search UI rate limit counter (EXPIRE after window)
- `miroir:ratelimit:adminlogin:<ip>` - Admin login rate limit counter
- `miroir:ratelimit:backoff:<ip>` - Per-IP backoff flag (EXPIRE after backoff duration)

#### CDC Overflow (§13.13)
- `miroir:cdc:overflow:<sink>` - CDC overflow buffer (1 GiB per sink default)

#### Scoped Key Rotation (§13.21)
- `miroir:search_ui_scoped_key:<index>` - Current scoped key for an index
- `miroir:search_ui_scoped_key_observed:<pod>:<index>` - Pod observation marker

#### Job Queue (§14.5)
- `miroir:jobs:enqueued` - List of enqueued job IDs

#### Admin Session Revocation (§13.19)
- `miroir:admin_session:revoked` - Pub/Sub channel for instant logout propagation

## Memory Sizing Formula

### Per-Record Overhead

Redis has the following per-key overhead:
- Key: ~100 bytes (including key length and overhead)
- Value: varies by type
- Hash entry: ~50 bytes per field
- Set entry: ~50 bytes per member

### Estimated Memory per Table

| Table | Avg Record Size | Est. Count | Memory (approx) |
|-------|----------------|------------|-----------------|
| tasks | 500 bytes | 10,000 | ~5 MB |
| node_settings_version | 100 bytes | 500 | ~50 KB |
| aliases | 200 bytes | 100 | ~20 KB |
| sessions | 150 bytes | 1,000 | ~150 KB |
| idempotency_cache | 1 KB | 10,000 | ~10 MB (TTL 1h) |
| jobs | 400 bytes | 1,000 | ~400 KB |
| leader_lease | 200 bytes | 1 | ~200 bytes |
| canaries | 300 bytes | 50 | ~15 KB |
| canary_runs | 200 bytes | 10,000 | ~2 MB |
| cdc_cursors | 100 bytes | 100 | ~10 KB |
| tenant_map | 500 bytes | 100 | ~50 KB |
| rollover_policies | 300 bytes | 50 | ~15 KB |
| search_ui_config | 2 KB | 100 | ~200 KB |
| admin_sessions | 150 bytes | 100 | ~15 KB |
| **Subtotal** | - | - | **~18 MB** |

### Rate Limiting Memory

Search UI rate limiter (§13.21):
- Per-IP bucket: ~100 bytes
- Active IPs: ~10,000 (production estimate)
- TTL: 60 seconds (configurable via `search_ui.rate_limit.redis_ttl_s`)
- **Memory: ~1 MB** (steady state, with TTL)

Admin login rate limiter:
- Per-IP bucket: ~100 bytes
- Active IPs: ~100
- TTL: 300 seconds (5 minutes)
- **Memory: ~10 KB**

Backoff markers:
- Per-IP backoff: ~100 bytes
- Active backoffs: ~100
- TTL: variable (typically 60-300 seconds)
- **Memory: ~10 KB**

### CDC Overflow Memory

- Per-sink buffer: 1 GiB (configurable via `cdc.buffer.redis_bytes`)
- Typical sinks: 1-3
- **Memory: 1-3 GiB** (only when CDC is enabled and overflow occurs)

### Scoped Key Rotation Memory

- Per-index key: ~200 bytes
- Indices: ~100
- Per-pod observation markers: 100 bytes × (pods × indices)
- Pods (HPA max): 10
- **Memory: ~20 KB + 100 KB = ~120 KB**

### Total Estimated Memory Usage

| Component | Memory |
|-----------|--------|
| Tables (steady state) | ~18 MB |
| Rate limiting | ~1 MB |
| Scoped key rotation | ~120 KB |
| **Subtotal (without CDC overflow)** | **~19 MB** |
| CDC overflow (per sink) | 1 GiB (optional) |

### Production Sizing Recommendation

For a production deployment with CDC disabled:
- **Minimum Redis memory: 64 MB** (provides headroom for bursts)
- **Recommended Redis memory: 128-256 MB** (comfortable headroom)

For a production deployment with CDC enabled:
- **Per-sink buffer: 1 GiB** (configurable)
- **Minimum Redis memory: 1 GiB + 64 MB per sink**
- **Recommended Redis memory: 2 GiB** (for single-sink deployments)

## Validation Script

The following script can be used to validate Redis memory usage in a running deployment:

```bash
# Connect to Redis and get memory info
redis-cli INFO memory

# Get memory usage for all Miroir keys
redis-cli --scan --pattern 'miroir:*' | xargs redis-cli MEMORY USAGE

# Get total memory used by Miroir
redis-cli --scan --pattern 'miroir:*' | xargs redis-cli MEMORY USAGE | awk '{sum+=$1} END {print sum}'
```

## Helm Chart Defaults

The Helm chart (see `charts/miroir/values.yaml`) sets the following defaults:

```yaml
redis:
  resources:
    limits:
      cpu: 500m
      memory: 256Mi
    requests:
      cpu: 100m
      memory: 128Mi
```

These defaults are appropriate for deployments without CDC overflow. For CDC-enabled deployments, increase the memory limit to at least 2 GiB.

## References

- Plan §4: Task store schema
- Plan §13.13: CDC cursors and overflow
- Plan §13.19: Admin sessions and Pub/Sub revocation
- Plan §13.21: Search UI rate limiting and scoped key rotation
- Plan §14.7: Deployment sizing matrix
