# Redis Memory Accounting (Plan §14.7)

This document describes the Redis memory usage for Miroir's task store and related keyspace, providing operators with guidance for sizing Redis deployments.

## Overview

Miroir uses Redis as the backing store for all shared state when `taskStore.backend: redis`. The 14 SQLite tables from plan §4 are mapped to Redis keyspaces with the same semantics. Additionally, several Redis-specific keys are used for rate limiting, CDC overflow buffering, and scoped-key rotation coordination.

## Core Task Store Keyspace (14 tables)

### Table 1: `tasks`
- **Key pattern**: `miroir:tasks:<miroir_id>` (hash)
- **Index**: `miroir:tasks:_index` (set of all task IDs)
- **Memory per task**: ~500 bytes (typical)
  - `miroir_id`: 36 bytes (UUID format)
  - `created_at`, `started_at`, `finished_at`: 8 bytes each (i64)
  - `status`: 16 bytes
  - `node_tasks` (JSON): ~200 bytes (depends on node count)
  - `node_errors` (JSON): ~100 bytes (typical)
  - `error`: ~50 bytes (when present)
  - Overhead: ~100 bytes
- **Sizing**: For 10,000 concurrent tasks → ~5 MB
- **Growth**: Proportional to active task count; pruned by task_pruner

### Table 2: `node_settings_version`
- **Key pattern**: `miroir:node_settings_version:<index_uid>:<node_id>` (hash)
- **Index**: `miroir:node_settings_version:_index` (set)
- **Memory per entry**: ~150 bytes
  - `index_uid`: 32 bytes
  - `node_id`: 32 bytes
  - `version`: 8 bytes (i64)
  - `updated_at`: 8 bytes (i64)
  - Overhead: ~70 bytes
- **Sizing**: For 100 indexes × 10 nodes = 1,000 entries → ~150 KB
- **Growth**: Fixed per (index, node) pair; bounded

### Table 3: `aliases`
- **Key pattern**: `miroir:aliases:<name>` (hash)
- **Index**: `miroir:aliases:_index` (set)
- **Memory per alias**: ~400 bytes
  - `name`: 32 bytes
  - `kind`: 8 bytes
  - `current_uid`: 32 bytes
  - `target_uids` (JSON array): ~100 bytes
  - `version`: 8 bytes
  - `created_at`: 8 bytes
  - `history` (JSON array): ~200 bytes (depends on retention)
  - Overhead: ~12 bytes
- **Sizing**: For 100 aliases → ~40 KB
- **Growth**: Fixed; only grows when aliases are created

### Table 4: `sessions`
- **Key pattern**: `miroir:session:<session_id>` (hash)
- **Memory per session**: ~200 bytes
  - `session_id`: 36 bytes
  - `last_write_mtask_id`: 36 bytes
  - `last_write_at`: 8 bytes
  - `pinned_group`: 8 bytes
  - `min_settings_version`: 8 bytes
  - `ttl`: 8 bytes
  - Overhead: ~100 bytes
- **TTL**: Configurable (default: 300 seconds)
- **Sizing**: For 1,000 active sessions → ~200 KB
- **Growth**: Bounded by `session_pinning.max_sessions`

### Table 5: `idempotency_cache`
- **Key pattern**: `miroir:idemp:<key>` (hash)
- **Memory per entry**: ~150 bytes
  - `key`: 64 bytes (SHA256 of request)
  - `body_sha256`: 64 bytes (hex-encoded)
  - `miroir_task_id`: 36 bytes
  - `expires_at`: 8 bytes
  - Overhead: ~10 bytes
- **TTL**: Configurable (default: 60 seconds)
- **Sizing**: For 10,000 cached requests → ~1.5 MB
- **Growth**: Bounded by `idempotency.max_cached_keys`

### Table 6: `jobs`
- **Key pattern**: `miroir:jobs:<id>` (hash)
- **Index**: `miroir:jobs:_index` (set of all job IDs)
- **Queue**: `miroir:jobs:_queued` (set of queued job IDs for HPA signal)
- **Memory per job**: ~300 bytes
  - `id`: 36 bytes
  - `type`: 16 bytes
  - `params` (JSON): ~100 bytes
  - `state`: 16 bytes
  - `claimed_by`: 32 bytes
  - `claim_expires_at`: 8 bytes
  - `progress` (JSON): ~50 bytes
  - Overhead: ~50 bytes
- **Sizing**: For 1,000 jobs → ~300 KB
- **Growth**: Proportional to background workload

### Table 7: `leader_lease`
- **Key pattern**: `miroir:lease:<scope>` (string)
- **Memory per lease**: ~100 bytes
  - Value: holder pod ID (~32 bytes)
  - TTL metadata: ~68 bytes
- **TTL**: 10 seconds (renewed every 3 seconds)
- **Sizing**: For 10 concurrent scopes → ~1 KB
- **Growth**: Fixed; one per coordinated operation

### Table 8: `canaries`
- **Key pattern**: `miroir:canary:<id>` (hash)
- **Index**: `miroir:canary:_index` (set)
- **Memory per canary**: ~600 bytes
  - `id`: 32 bytes
  - `name`: 32 bytes
  - `index_uid`: 32 bytes
  - `interval_s`: 8 bytes
  - `query_json`: ~200 bytes
  - `assertions_json`: ~200 bytes
  - `enabled`: 1 byte
  - `created_at`: 8 bytes
  - Overhead: ~90 bytes
- **Sizing**: For 50 canaries → ~30 KB
- **Growth**: Fixed; only grows when canaries are created

### Table 9: `canary_runs`
- **Key pattern**: `miroir:canary_runs:<canary_id>` (sorted set)
- **Memory per run**: ~300 bytes
  - Score: 8 bytes (ran_at timestamp)
  - Value (JSON): ~250 bytes
  - Overhead: ~50 bytes
- **Retention**: Configurable (default: 100 runs per canary)
- **Sizing**: For 50 canaries × 100 runs = 5,000 runs → ~1.5 MB
- **Growth**: Bounded by `canary_runner.run_history_per_canary`

### Table 10: `cdc_cursors`
- **Key pattern**: `miroir:cdc_cursor:<sink_name>:<index_uid>` (string)
- **Index**: `miroir:cdc_cursor:_index:<sink_name>` (set)
- **Memory per cursor**: ~100 bytes
  - Value: last_event_seq (8 bytes as string)
  - Overhead: ~92 bytes
- **Sizing**: For 10 sinks × 100 indexes = 1,000 cursors → ~100 KB
- **Growth**: Fixed per (sink, index) pair

### Table 11: `tenant_map`
- **Key pattern**: `miroir:tenant_map:<hex_sha256>` (hash)
- **Memory per mapping**: ~150 bytes
  - `tenant_id`: 32 bytes
  - `group_id`: 8 bytes (when present)
  - Overhead: ~110 bytes
- **Sizing**: For 1,000 tenants → ~150 KB
- **Growth**: Fixed; one per API key

### Table 12: `rollover_policies`
- **Key pattern**: `miroir:rollover:<name>` (hash)
- **Index**: `miroir:rollover:_index` (set)
- **Memory per policy**: ~800 bytes
  - `name`: 32 bytes
  - `write_alias`: 32 bytes
  - `read_alias`: 32 bytes
  - `pattern`: 64 bytes
  - `triggers_json`: ~200 bytes
  - `retention_json`: ~200 bytes
  - `template_json`: ~200 bytes
  - `enabled`: 1 byte
  - Overhead: ~40 bytes
- **Sizing**: For 20 policies → ~16 KB
- **Growth**: Fixed; only grows when policies are created

### Table 13: `search_ui_config`
- **Key pattern**: `miroir:search_ui_config:<index_uid>` (hash)
- **Memory per config**: ~400 bytes
  - `index_uid`: 32 bytes
  - `config_json`: ~300 bytes
  - `updated_at`: 8 bytes
  - Overhead: ~60 bytes
- **Sizing**: For 50 indexes → ~20 KB
- **Growth**: Fixed; one per index

### Table 14: `admin_sessions`
- **Key pattern**: `miroir:admin_session:<session_id>` (hash)
- **Memory per session**: ~300 bytes
  - `session_id`: 36 bytes
  - `csrf_token`: 32 bytes
  - `admin_key_hash`: 64 bytes
  - `created_at`: 8 bytes
  - `expires_at`: 8 bytes
  - `revoked`: 1 byte
  - `user_agent`: ~50 bytes
  - `source_ip`: ~16 bytes
  - Overhead: ~85 bytes
- **TTL**: Configurable (default: 8 hours)
- **Sizing**: For 100 active admin sessions → ~30 KB
- **Growth**: Bounded by concurrent admin users

## Redis-Specific Keys

### Rate Limiting: Search UI
- **Key pattern**: `miroir:ratelimit:searchui:<ip>` (string counter)
- **Memory per bucket**: ~100 bytes
  - Value: counter (8 bytes as string)
  - TTL metadata: ~92 bytes
- **TTL**: 60 seconds (configurable via `search_ui.rate_limit.redis_ttl_s`)
- **Sizing**: **~20 MB per 10,000 active IPs**
  - Each active IP creates one bucket per 60-second window
  - Buckets auto-expire; steady state is proportional to active IP count
- **Growth**: Proportional to unique client IPs in the rate limit window

### Rate Limiting: Admin Login
- **Key pattern**: `miroir:ratelimit:adminlogin:<ip>` (string counter)
- **Backoff**: `miroir:ratelimit:adminlogin:backoff:<ip>` (hash)
- **Memory per IP**: ~300 bytes (when in backoff)
- **TTL**: Configurable (default: 300 seconds)
- **Sizing**: For 1,000 IPs with failed attempts → ~300 KB
- **Growth**: Bounded; only IPs with failed login attempts consume memory

### Scoped Key Rotation
- **Current key**: `miroir:search_ui_scoped_key:<index>` (hash)
- **Observations**: `miroir:search_ui_scoped_key_observed:<pod>:<index>` (hash)
- **Memory per index**: ~400 bytes (current) + ~100 bytes per pod (observations)
- **TTL**: Observations expire after 60 seconds
- **Sizing**: For 50 indexes × 10 pods = 500 observations → ~50 KB (transient)
- **Growth**: Fixed per index; observations are transient

### Live Pod Registry
- **Key**: `miroir:live_pods` (sorted set)
- **Memory per pod**: ~100 bytes
  - Member: pod ID (~32 bytes)
  - Score: timestamp (8 bytes)
  - Overhead: ~60 bytes
- **TTL**: 300 seconds (auto-refreshed)
- **Sizing**: For 24 pods → ~2.4 KB
- **Growth**: Fixed; proportional to pod count

### CDC Overflow Buffer
- **Key pattern**: `miroir:cdc:overflow:<sink_name>` (list)
- **Byte counter**: `miroir:cdc:overflow_bytes:<sink_name>` (string)
- **Memory**: Configurable via `cdc.buffer.redis_bytes` (default: 1 GiB per sink)
- **Sizing**: Per-sink budget; 1 GiB × 10 sinks = 10 GiB (worst case)
- **Growth**: Bounded by configuration; trimmed to budget

### Admin Session Revocation Pub/Sub
- **Channel**: `miroir:admin_session:revoked` (Pub/Sub)
- **Memory**: Negligible (Pub/Sub is connection-based, not stored)

## Total Memory Calculation

### Baseline (small deployment)
- 100 indexes, 10 nodes, 100 concurrent tasks, 10 active IPs
- **Core tables**: ~10 MB
- **Rate limiting**: ~20 KB (10 IPs)
- **CDC overflow**: 1 GiB (if enabled)
- **Total (without CDC)**: ~10 MB
- **Total (with CDC)**: ~1 GB

### Medium deployment
- 500 indexes, 50 nodes, 1,000 concurrent tasks, 1,000 active IPs
- **Core tables**: ~50 MB
- **Rate limiting**: ~20 MB (1,000 IPs)
- **CDC overflow**: 1 GiB per sink × 5 sinks = 5 GiB
- **Total (without CDC)**: ~70 MB
- **Total (with CDC)**: ~5 GB

### Large deployment
- 1,000 indexes, 100 nodes, 10,000 concurrent tasks, 10,000 active IPs
- **Core tables**: ~100 MB
- **Rate limiting**: ~20 MB (10,000 IPs)
- **CDC overflow**: 1 GiB per sink × 10 sinks = 10 GiB
- **Total (without CDC)**: ~120 MB
- **Total (with CDC)**: ~10 GB

## Redis Sizing Recommendations

### Minimum (development/testing)
- **Memory**: 256 MB
- **Use case**: Single replica, low traffic, no CDC

### Small (production)
- **Memory**: 512 MB - 1 GB
- **Use case**: 2 replicas, ≤500 QPS, CDC with 1-2 sinks

### Medium (production)
- **Memory**: 2 - 4 GB
- **Use case**: 2-4 replicas, ≤2k QPS, CDC with multiple sinks

### Large (production)
- **Memory**: 8 - 16 GB
- **Use case**: 4-12 replicas, ≤20k QPS, heavy CDC usage

### Very Large (production)
- **Memory**: 32 GB+
- **Use case**: 12+ replicas, high CDC throughput
- **Consider**: Redis Cluster or Sentinel for HA

## Monitoring

Monitor these Redis metrics to ensure adequate sizing:

1. **`used_memory`** - Total memory used by Redis
2. **`used_memory_peak`** - Peak memory usage
3. **`evicted_keys`** - Number of keys evicted due to memory pressure (should be 0)
4. **`miroir_task_count`** - Number of tasks in the registry
5. **`miroir_task_registry_size`** - Prometheus gauge for task count

Alert when `used_memory` exceeds 80% of `maxmemory`.

## References

- Plan §4: Task registry + persistence schema
- Plan §13: Advanced capabilities (CDC, rate limiting, etc.)
- Plan §14.7: Revised deployment sizing matrix
