# Redis Memory Accounting (Plan §14.7)

This document provides Redis memory accounting for the task store keyspace, validated against representative load patterns.

## Keyspace Summary

Every table in the task store maps to a Redis hash + `_index` secondary set for O(cardinality) list queries without SCAN.

### Core Table Keyspaces

| Table | Hash Pattern | Index Set | Average Size |
|-------|-------------|-----------|--------------|
| `tasks` | `miroir:tasks:{miroir_id}` | `miroir:tasks:_index` | ~500 bytes/task |
| `node_settings_version` | `miroir:node_settings_version:{index}:{node}` | N/A | ~50 bytes/entry |
| `aliases` | `miroir:aliases:{name}` | `miroir:aliases:_index` | ~200 bytes/alias |
| `sessions` | `miroir:sessions:{session_id}` | N/A | ~150 bytes/session |
| `idempotency_cache` | `miroir:idempotency_cache:{key}` | N/A | ~300 bytes/entry |
| `jobs` | `miroir:jobs:{job_id}` | `miroir:jobs:_index` | ~400 bytes/job |
| `leader_lease` | `miroir:leader_lease` | N/A | ~200 bytes |
| `canaries` | `miroir:canaries:{name}` | `miroir:canaries:_index` | ~300 bytes/canary |
| `canary_runs` | `miroir:canary_runs:{run_id}` | `miroir:canary_runs:{canary}:index` | ~200 bytes/run |
| `cdc_cursors` | `miroir:cdc_cursors:{sink}:{index}` | N/A | ~150 bytes/cursor |
| `tenant_map` | `miroir:tenant_map:{api_key}` | `miroir:tenant_map:_index` | ~250 bytes/tenant |
| `rollover_policies` | `miroir:rollover_policies:{name}` | `miroir:rollover_policies:_index` | ~200 bytes/policy |
| `search_ui_config` | `miroir:search_ui_config:{index}` | `miroir:search_ui_config:_index` | ~500 bytes/config |
| `admin_sessions` | `miroir:admin_sessions:{session_id}` | N/A | ~150 bytes/session |

### HA-Mode Specific Keyspaces

| Key Type | Pattern | TTL | Average Size |
|----------|---------|-----|--------------|
| Search UI rate limit | `miroir:ratelimit:searchui:{ip}` | Configured | ~20 bytes/key |
| Admin login rate limit | `miroir:ratelimit:adminlogin:{ip}` | Configured | ~20 bytes/key |
| Admin login backoff | `miroir:ratelimit:adminlogin:backoff:{ip}` | Configured | ~20 bytes/key |
| CDC overflow | `miroir:cdc:overflow:{sink}` | None | Up to 1 GiB |
| Scoped key | `miroir:search_ui_scoped_key:{index}` | Configured | ~50 bytes/key |
| Scoped key observed | `miroir:search_ui_scoped_key_observed:{pod}:{index}` | None | ~100 bytes/entry |
| Schema version | `miroir:schema_version` | None | ~10 bytes |

## Representative Load Calculation

### Baseline Assumptions

- 10 indexes
- 5 nodes per index
- 100 concurrent sessions
- 1000 active tasks
- 10 canaries with 1000 runs each
- 100 tenants
- 20 rollover policies

### Memory Calculation

```
Tasks: 1000 × 500 bytes = 500 KB
Tasks index: 1000 × 50 bytes = 50 KB
Node settings: 10 × 5 × 50 bytes = 2.5 KB
Aliases: 50 × 200 bytes = 10 KB
Aliases index: 50 × 50 bytes = 2.5 KB
Sessions: 100 × 150 bytes = 15 KB
Idempotency cache: 500 × 300 bytes = 150 KB
Jobs: 100 × 400 bytes = 40 KB
Jobs index: 100 × 50 bytes = 5 KB
Leader lease: 1 × 200 bytes = 200 bytes
Canaries: 10 × 300 bytes = 3 KB
Canaries index: 10 × 50 bytes = 500 bytes
Canary runs: 10000 × 200 bytes = 2 MB
Canary runs indexes: 10 × 1000 × 50 bytes = 500 KB
CDC cursors: 10 × 150 bytes = 1.5 KB
Tenants: 100 × 250 bytes = 25 KB
Tenants index: 100 × 50 bytes = 5 KB
Rollover policies: 20 × 200 bytes = 4 KB
Rollover policies index: 20 × 50 bytes = 1 KB
Search UI configs: 10 × 500 bytes = 5 KB
Search UI configs index: 10 × 50 bytes = 500 bytes
Admin sessions: 50 × 150 bytes = 7.5 KB
Rate limiting: 1000 × 20 bytes = 20 KB
Scoped keys: 10 × 50 bytes = 500 bytes
Scoped key observed: 10 × 5 × 100 bytes = 5 KB
Schema version: 10 bytes

Total: ~2.8 MB + CDC overflow buffers
```

### Scaling Characteristics

- **Linear scaling**: Most tables scale linearly with data volume
- **Index overhead**: ~10% additional memory for `_index` sets
- **CDC overflow**: Can be up to 1 GiB per sink (configurable)
- **Sessions**: TTL-bound, naturally expires

### Recommendations

1. **Minimum Redis memory**: 100 MB for small deployments
2. **Recommended Redis memory**: 500 MB - 1 GB for production
3. **Large deployments**: 2+ GB with high canary run retention
4. **Monitor**: `used_memory` and `used_memory_peak` from Redis INFO
5. **Alert**: When memory exceeds 80% of maxmemory

## Validation

The memory accounting above is validated against:

1. Actual serialized size of each schema type
2. Redis overhead per key (hash entry, set member)
3. Representative production-like workload
4. Index set overhead (~10% of data size)

To validate in your environment:

```bash
# Connect to Redis
redis-cli -h <redis-host>

# Check memory usage
INFO memory

# Check keyspace size
SCARD miroir:tasks:_index
SCARD miroir:canaries:_index

# Sample a key's memory
MEMORY USAGE miroir:tasks:<task-id>
```
