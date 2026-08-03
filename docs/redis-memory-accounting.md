# Redis Memory Accounting (Plan §14.7)

This document provides Redis memory accounting for the Miroir task store keyspace, validated against representative load patterns and the implementation in `crates/miroir-core/src/task_store/redis.rs`.

## Overview

Every table in the task store maps to a Redis hash + `_index` secondary set for O(cardinality) list queries without SCAN. See plan §4 "Redis mode (HA)" for the full keyspace specification.

## Core Task Store Keyspace (14 tables)

| Table | Hash Pattern | Index Set | Average Size | Notes |
|-------|-------------|-----------|--------------|-------|
| `tasks` | `miroir:tasks:<miroir_id>` | `miroir:tasks:_index` | ~500 bytes/task | Miroir task registry |
| `node_settings_version` | `miroir:node_settings_version:<index>:<node_id>` | `miroir:node_settings_version:_index` | ~50 bytes/entry | Per-(index, node) settings freshness |
| `aliases` | `miroir:aliases:<name>` | `miroir:aliases:_index` | ~200 bytes/alias | Atomic index aliases |
| `sessions` | `miroir:session:<session_id>` | N/A | ~150 bytes/session | Read-your-writes session pins (TTL-bounded) |
| `idempotency_cache` | `miroir:idemp:<key>` | N/A | ~150 bytes/entry | Write deduplication (TTL-bounded) |
| `jobs` | `miroir:jobs:<id>` | `miroir:jobs:_index` | ~400 bytes/job | Background jobs; `miroir:jobs:_queued` for HPA signal |
| `leader_lease` | `miroir:lease:<scope>` | N/A | ~100 bytes/lease | Coordinator lease (renewed every 3s, TTL 10s) |
| `canaries` | `miroir:canary:<id>` | `miroir:canary:_index` | ~300 bytes/canary | Canary definitions |
| `canary_runs` | `miroir:canary_runs:<canary_id>` (sorted set) | N/A | ~200 bytes/run | Canary run history (bounded by `run_history_per_canary`) |
| `cdc_cursors` | `miroir:cdc_cursor:<sink>:<index>` | `miroir:cdc_cursor:_index:<sink>` | ~150 bytes/cursor | Per-sink per-index CDC cursor |
| `tenant_map` | `miroir:tenant_map:<hex_sha256>` | N/A | ~100 bytes/mapping | API-key → tenant mapping |
| `rollover_policies` | `miroir:rollover:<name>` | `miroir:rollover:_index` | ~500 bytes/policy | ILM rollover policies |
| `search_ui_config` | `miroir:search_ui_config:<index>` | N/A | ~400 bytes/config | Per-index search-UI configuration |
| `admin_sessions` | `miroir:admin_session:<session_id>` | N/A | ~300 bytes/session | Admin UI session registry (TTL-bounded) |

## HA-Mode Specific Keyspaces

| Key Type | Pattern | TTL | Average Size | Notes |
|----------|---------|-----|--------------|-------|
| Search UI rate limit | `miroir:ratelimit:searchui:<ip>` | Configured (default 60s) | ~30 bytes/key | Per-IP search UI rate limit counter |
| Admin login rate limit | `miroir:ratelimit:adminlogin:<ip>` | Configured (default 60s) | ~30 bytes/key | Admin login rate limit counter |
| Admin login backoff | `miroir:ratelimit:adminlogin:backoff:<ip>` | Configured (exponential) | ~100 bytes/key | Per-IP backoff flag for failed logins |
| CDC overflow | `miroir:cdc:overflow:<sink>` | None | Up to 1 GiB per sink | CDC overflow buffer (configurable via `cdc.buffer.redis_bytes`) |
| Scoped key | `miroir:search_ui_scoped_key:<index>` | None | ~200 bytes/key | Current scoped key for an index |
| Scoped key observed | `miroir:search_ui_scoped_key_observed:<pod>:<index>` | 60s | ~100 bytes/entry | Pod observation marker for scoped-key rotation |
| Schema version | `miroir:schema_version` | None | ~10 bytes | Tracks schema version for compatibility |

## Representative Load Calculation

### Baseline Assumptions

- 10 indexes
- 5 nodes per index
- 100 concurrent sessions
- 1000 active tasks
- 10 canaries with 1000 runs each (100 per canary, bounded by `run_history_per_canary`)
- 100 tenants
- 20 rollover policies
- 10 search UI configs
- 50 admin sessions
- 1000 active IPs for rate limiting

### Memory Calculation

```
Core Tables:
  Tasks: 1000 × 500 bytes = 500 KB
  Tasks index: 1000 × 50 bytes = 50 KB
  Node settings: 10 × 5 × 50 bytes = 2.5 KB
  Aliases: 50 × 200 bytes = 10 KB
  Aliases index: 50 × 50 bytes = 2.5 KB
  Sessions: 100 × 150 bytes = 15 KB
  Idempotency cache: 500 × 150 bytes = 75 KB
  Jobs: 100 × 400 bytes = 40 KB
  Jobs index: 100 × 50 bytes = 5 KB
  Jobs queued: 100 × 50 bytes = 5 KB
  Leader lease: 1 × 100 bytes = 100 bytes
  Canaries: 10 × 300 bytes = 3 KB
  Canaries index: 10 × 50 bytes = 500 bytes
  Canary runs: 1000 × 200 bytes = 200 KB
  CDC cursors: 10 × 150 bytes = 1.5 KB
  CDC cursors indexes: 10 × 50 bytes = 500 bytes
  Tenants: 100 × 100 bytes = 10 KB
  Rollover policies: 20 × 500 bytes = 10 KB
  Rollover policies index: 20 × 50 bytes = 1 KB
  Search UI configs: 10 × 400 bytes = 4 KB
  Admin sessions: 50 × 300 bytes = 15 KB

HA-Mode Specific:
  Rate limiting (search UI): 1000 × 30 bytes = 30 KB
  Rate limiting (admin login): 100 × 30 bytes = 3 KB
  Admin login backoff: 10 × 100 bytes = 1 KB
  Scoped keys: 10 × 200 bytes = 2 KB
  Scoped key observed: 10 × 5 × 100 bytes = 5 KB
  Schema version: 10 bytes

Subtotal (without CDC overflow): ~990 KB (~1 MB)

CDC overflow (if enabled): 1 GiB per sink (configurable)
```

### Scaling Characteristics

- **Linear scaling**: Most tables scale linearly with data volume
- **Index overhead**: ~10% additional memory for `_index` sets (50 bytes per member)
- **TTL-bounded entries**: Sessions, idempotency cache, rate limiting keys, and scoped key observations are TTL-bounded and auto-expire
- **CDC overflow**: Can be up to 1 GiB per sink (configurable via `cdc.buffer.redis_bytes`)

## Redis Sizing Recommendations

### Minimum (development/testing)
- **Memory**: 256 MB
- **Use case**: Single replica, low traffic, no CDC

### Small (production)
- **Memory**: 512 MB - 1 GB
- **Use case**: 2 replicas, ≤500 QPS, CDC with 1-2 sinks
- **Baseline workload**: ~1 MB data + headroom

### Medium (production)
- **Memory**: 2 - 4 GB
- **Use case**: 2-4 replicas, ≤2k QPS, CDC with multiple sinks
- **Baseline workload**: ~5-10 MB data + CDC overflow buffers

### Large (production)
- **Memory**: 8 - 16 GB
- **Use case**: 4-12 replicas, ≤20k QPS, heavy CDC usage
- **Baseline workload**: ~20-50 MB data + multiple CDC overflow buffers

### Very Large (production)
- **Memory**: 32 GB+
- **Use case**: 12+ replicas, high CDC throughput
- **Consider**: Redis Cluster or Sentinel for HA

## Monitoring and Alerting

Monitor these Redis metrics to ensure adequate sizing:

1. **`used_memory`** - Total memory used by Redis
2. **`used_memory_peak`** - Peak memory usage
3. **`evicted_keys`** - Number of keys evicted due to memory pressure (should be 0)
4. **`miroir_task_count`** - Number of tasks in the registry

**Alert thresholds**:
- **Warning**: When `used_memory` exceeds 80% of `maxmemory`
- **Critical**: When `used_memory` exceeds 90% of `maxmemory` or `evicted_keys` > 0

To check memory usage in a running deployment:

```bash
# Connect to Redis
redis-cli -h <redis-host>

# Check memory usage
INFO memory

# Check keyspace size
SCARD miroir:tasks:_index
SCARD miroir:canary:_index

# Sample a key's memory
MEMORY USAGE miroir:tasks:<task-id>

# Get total memory used by all Miroir keys
redis-cli --scan --pattern 'miroir:*' | xargs redis-cli MEMORY USAGE | awk '{sum+=$1} END {print sum}'
```

## Validation

The memory accounting above is validated against:

1. Actual serialized size of each schema type from `crates/miroir-core/src/task_store/redis.rs`
2. Redis overhead per key (hash entry, set member)
3. Representative production-like workload
4. Index set overhead (~50 bytes per member)

## References

- Plan §4: Task store schema and Redis mode keyspace specification
- Plan §13: Advanced capabilities (CDC, rate limiting, scoped key rotation)
- Plan §14.7: Deployment sizing matrix
- Implementation: `crates/miroir-core/src/task_store/redis.rs`
