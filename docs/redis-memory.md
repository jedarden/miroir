# Redis Memory Usage and Capacity Planning

This document describes Redis memory usage patterns for the Miroir task store and provides guidance for capacity planning (plan §14.7).

## Overview

Miroir uses Redis as an optional task store backend for multi-replica deployments. The keyspace is organized into 14 table-like structures plus auxiliary keys for rate limiting, CDC overflow buffering, and Pub/Sub.

## Redis Keyspace Organization

All keys use the `miroir:` prefix to avoid collisions with other applications using the same Redis instance.

### Table 1: `tasks` (Miroir task registry)

**Key pattern:** `miroir:tasks:<miroir_id>` (hash)
**Index:** `miroir:tasks:_index` (set)

| Field | Type | Example Size | Notes |
|-------|------|--------------|-------|
| miroir_id | string | ~30 bytes | UUIDv4 with "mtask-" prefix |
| created_at | string | ~10 bytes | Millisecond timestamp as string |
| status | string | ~10 bytes | "enqueued", "processing", "succeeded", "failed", "canceled" |
| node_tasks | string | ~50 bytes | JSON: `{"node-0":123}` (varies by node count) |
| node_errors | string | ~10 bytes | JSON object, often empty `{}` |
| error | string | 0-100 bytes | Optional error message |
| started_at | string | 0-10 bytes | Optional timestamp |
| finished_at | string | 0-10 bytes | Optional timestamp |
| index_uid | string | 0-50 bytes | Optional index identifier |
| task_type | string | 0-50 bytes | Optional task type identifier |

**Estimated per-task memory:** ~200-300 bytes (including Redis hash overhead)

**Index overhead:** ~40 bytes per task in the `_index` set

### Table 2: `node_settings_version`

**Key pattern:** `miroir:node_settings_version:<index_uid>:<node_id>` (hash)
**Index:** `miroir:node_settings_version:_index` (set)

| Field | Type | Example Size |
|-------|------|--------------|
| index_uid | string | ~20 bytes |
| node_id | string | ~20 bytes |
| version | string | ~10 bytes |
| updated_at | string | ~10 bytes |

**Estimated per-entry memory:** ~100-150 bytes

### Table 3: `aliases`

**Key pattern:** `miroir:aliases:<name>` (hash)
**Index:** `miroir:aliases:_index` (set)

| Field | Type | Example Size |
|-------|------|--------------|
| name | string | ~30 bytes |
| kind | string | ~10 bytes | "single" or "multi" |
| current_uid | string | 0-40 bytes | Optional |
| target_uids | string | 0-100 bytes | JSON array, optional |
| version | string | ~10 bytes |
| created_at | string | ~10 bytes |
| history | string | ~50 bytes | JSON array of {uid, flipped_at} |

**Estimated per-entry memory:** ~200-300 bytes

### Table 4: `sessions`

**Key pattern:** `miroir:session:<session_id>` (hash with EXPIRE)

| Field | Type | Example Size |
|-------|------|--------------|
| session_id | string | ~40 bytes |
| last_write_mtask_id | string | 0-40 bytes | Optional |
| last_write_at | string | 0-10 bytes | Optional |
| pinned_group | string | 0-10 bytes | Optional |
| min_settings_version | string | ~10 bytes |
| ttl | string | ~10 bytes |

**Estimated per-entry memory:** ~150-200 bytes

**Note:** Sessions have TTL set via Redis `EXPIRE` and are automatically garbage-collected.

### Table 5: `idempotency_cache`

**Key pattern:** `miroir:idemp:<key>` (hash with EXPIRE)

| Field | Type | Example Size |
|-------|------|--------------|
| key | string | ~50 bytes |
| body_sha256 | string | ~64 bytes | Hex-encoded SHA256 |
| miroir_task_id | string | ~40 bytes |
| expires_at | string | ~10 bytes |

**Estimated per-entry memory:** ~200-250 bytes

**Note:** Entries have TTL set via Redis `EXPIRE` and are automatically garbage-collected.

### Table 6: `jobs`

**Key pattern:** `miroir:jobs:<id>` (hash)
**Index:** `miroir:jobs:_index` (set)
**Queued:** `miroir:jobs:_queued` (set)

| Field | Type | Example Size |
|-------|------|--------------|
| id | string | ~40 bytes |
| type | string | ~30 bytes |
| params | string | ~100 bytes | JSON |
| state | string | ~20 bytes | "queued", "in_progress", "completed", "failed" |
| claimed_by | string | 0-20 bytes | Optional pod ID |
| claim_expires_at | string | 0-10 bytes | Optional timestamp |
| progress | string | ~50 bytes | JSON |

**Estimated per-entry memory:** ~300-400 bytes

### Table 7: `leader_lease`

**Key pattern:** `miroir:lease:<scope>` (string with EXPIRE)

**Estimated per-entry memory:** ~50-100 bytes (simple key-value with TTL)

**Note:** Leases use Redis `SET NX EX` for distributed coordination.

### Table 8: `canaries`

**Key pattern:** `miroir:canary:<id>` (hash)
**Index:** `miroir:canary:_index` (set)

| Field | Type | Example Size |
|-------|------|--------------|
| id | string | ~30 bytes |
| name | string | ~40 bytes |
| index_uid | string | ~30 bytes |
| interval_s | string | ~10 bytes |
| query_json | string | ~50 bytes | JSON |
| assertions_json | string | ~50 bytes | JSON |
| enabled | string | ~5 bytes | "0" or "1" |
| created_at | string | ~10 bytes |

**Estimated per-entry memory:** ~250-350 bytes

### Table 9: `canary_runs`

**Key pattern:** `miroir:canary_runs:<canary_id>` (sorted set, ZADD with score=ran_at)

**Value:** JSON serialization of run data (~100 bytes)
**Score:** ran_at timestamp

**Estimated per-run memory:** ~150-200 bytes (including ZSET overhead)

**Auto-pruning:** Sorted set is trimmed to `run_history_per_canary` (default 100) on each insert.

### Table 10: `cdc_cursors`

**Key pattern:** `miroir:cdc_cursor:<sink_name>:<index_uid>` (hash)
**Index:** `miroir:cdc_cursor:_index:<sink_name>` (set)

| Field | Type | Example Size |
|-------|------|--------------|
| sink_name | string | ~30 bytes |
| index_uid | string | ~30 bytes |
| last_event_seq | string | ~10 bytes |
| updated_at | string | ~10 bytes |

**Estimated per-entry memory:** ~120-150 bytes

### Table 11: `tenant_map`

**Key pattern:** `miroir:tenant_map:<hex_encoded_api_key_hash>` (hash)

| Field | Type | Example Size |
|-------|------|--------------|
| tenant_id | string | ~40 bytes |
| group_id | string | 0-10 bytes | Optional |

**Estimated per-entry memory:** ~80-120 bytes

### Table 12: `rollover_policies`

**Key pattern:** `miroir:rollover:<name>` (hash)
**Index:** `miroir:rollover:_index` (set)

| Field | Type | Example Size |
|-------|------|--------------|
| name | string | ~30 bytes |
| write_alias | string | ~30 bytes |
| read_alias | string | ~30 bytes |
| pattern | string | ~30 bytes |
| triggers_json | string | ~100 bytes | JSON |
| retention_json | string | ~100 bytes | JSON |
| template_json | string | ~200 bytes | JSON |
| enabled | string | ~5 bytes | "0" or "1" |

**Estimated per-entry memory:** ~400-600 bytes

### Table 13: `search_ui_config`

**Key pattern:** `miroir:search_ui_config:<index_uid>` (hash)

| Field | Type | Example Size |
|-------|------|--------------|
| index_uid | string | ~30 bytes |
| config_json | string | ~200 bytes | JSON |
| updated_at | string | ~10 bytes |

**Estimated per-entry memory:** ~250-300 bytes

### Table 14: `admin_sessions`

**Key pattern:** `miroir:admin_session:<session_id>` (hash with EXPIRE)

| Field | Type | Example Size |
|-------|------|--------------|
| session_id | string | ~40 bytes |
| csrf_token | string | ~40 bytes |
| admin_key_hash | string | ~64 bytes | Hashed admin key |
| created_at | string | ~10 bytes |
| expires_at | string | ~10 bytes |
| revoked | string | ~5 bytes | "0" or "1" |
| user_agent | string | 0-100 bytes | Optional |
| source_ip | string | 0-20 bytes | Optional |

**Estimated per-entry memory:** ~200-300 bytes

**Note:** Sessions have TTL set via Redis `EXPIRE` and are automatically garbage-collected.

## Auxiliary Keys

### Rate Limiting: Search UI

**Key pattern:** `miroir:ratelimit:searchui:<ip>` (string with EXPIRE)

**Estimated per-entry memory:** ~30-50 bytes (simple counter)

### Rate Limiting: Admin Login

**Key pattern:** `miroir:ratelimit:adminlogin:<ip>` (string with EXPIRE)
**Backoff pattern:** `miroir:ratelimit:adminlogin:backoff:<ip>` (hash with EXPIRE)

**Estimated per-entry memory:** ~30-100 bytes

### CDC Overflow Buffer

**Key pattern:** `miroir:cdc:overflow:<sink_name>` (list)
**Byte counter:** `miroir:cdc:overflow_bytes:<sink_name>` (string)

**Memory budget:** Configurable per sink (default 1 GiB)
**Elements:** Variable-size JSON blobs

### Search UI Scoped Keys

**Key pattern:** `miroir:search_ui_scoped_key:<index_uid>` (hash)
**Observation:** `miroir:search_ui_scoped_key_observed:<pod_id>:<index_uid>` (hash with EXPIRE, TTL 60s)

**Estimated per-entry memory:** ~200-300 bytes

### Live Pod Registry

**Key pattern:** `miroir:live_pods` (sorted set, ZADD with score=timestamp)

**Estimated per-pod memory:** ~50 bytes

### Pub/Sub: Session Revocation

**Channel:** `miroir:admin_session:revoked`

**Memory overhead:** Negligible (Pub/Sub is not persisted)

## Capacity Planning

### Memory Budget Estimation

For a typical production deployment with the following characteristics:

- **10,000 active tasks** (in-flight or recently completed)
- **1,000 concurrent sessions** (search UI + admin)
- **1,000 idempotency cache entries** (recent deduplication)
- **100 background jobs** (queued/in-progress)
- **10 leader leases** (coordinating reshard/rollover operations)
- **5 canaries** with 100-run history each
- **50 CDC cursors** (per-sink, per-index)
- **10 rollover policies**
- **20 search UI configs**

**Estimated memory usage:**

| Component | Count | Size per Item | Subtotal |
|-----------|-------|---------------|----------|
| Tasks | 10,000 | 250 bytes | ~2.5 MB |
| Tasks index | 10,000 | 40 bytes | ~400 KB |
| Sessions | 1,000 | 175 bytes | ~175 KB |
| Idempotency | 1,000 | 225 bytes | ~225 KB |
| Jobs | 100 | 350 bytes | ~35 KB |
| Leases | 10 | 75 bytes | ~1 KB |
| Canaries | 5 | 300 bytes | ~1.5 KB |
| Canary runs | 500 | 175 bytes | ~88 KB |
| CDC cursors | 50 | 135 bytes | ~7 KB |
| Rollover policies | 10 | 500 bytes | ~5 KB |
| Search UI configs | 20 | 275 bytes | ~5.5 KB |
| Scoped keys | 20 | 250 bytes | ~5 KB |
| Rate limiting | 2,000 | 40 bytes | ~80 KB |
| **Total** | | | **~3.5 MB** |

### Redis Memory Overhead

Redis adds memory overhead for:
- **Hash table overhead:** ~20-30% of raw data size
- **Pointer chasing:** Each key/value pair has pointers
- **Memory allocator fragmentation:** Varies by allocator

**Conservative estimate:** Multiply the raw data size by 1.5x for overhead.

**Recommended minimum for above workload:** ~6 MB

### Per-Pod Memory Growth

In multi-replica deployments:
- **Live pod registry:** ~50 bytes per pod
- **Scoped key observations:** ~250 bytes per pod per index with scoped keys

For 10 replicas with 20 scoped-key indexes: ~50 KB

### Monitoring

Monitor `miroir_cdc_redis_memory_bytes` (Prometheus metric exported by Miroir) which tracks the `used_memory` value from Redis `INFO` command.

**Alert thresholds (plan §14.7):**
- **Warning:** > 500 MB
- **Critical:** > 1 GB

If memory usage grows beyond thresholds:
1. Increase Redis memory limit
2. Review task pruning policy (reduce retention period)
3. Reduce idempotency cache TTL
4. Check for CDC overflow buffer growth (may indicate sink is down)

## Redis Configuration Recommendations

### maxmemory-policy

Recommended: `allkeys-lru` (evict least-recently-used keys when memory limit is reached)

This is safe for Miroir because:
- Tasks are eventually pruned to a retention window
- Sessions and idempotency entries have TTL and will naturally expire first
- Critical data (leader leases) are refreshed frequently and won't be evicted

### Persistence

For production deployments:
- **RDB snapshots:** Every 5-10 minutes is sufficient (tasks are source-of-truth in Meilisearch)
- **AOF:** Not required (acceptable to lose last few seconds of task updates on failover)

### Connection Pooling

Miroir uses `redis-rs` with `connection-manager` for automatic connection pooling and reconnection. No additional configuration needed.

### High Availability

For production multi-replica deployments:
- Use Redis Sentinel or Redis Cluster for HA
- Configure `taskStore.url` with Sentinel master name or Cluster endpoints
- Miroir's `connection-manager` handles failover automatically

## Testing

Run the integration test suite to verify memory usage under load:

```bash
cargo test -p miroir-core --features redis-store --test-threads=1 test_redis_memory_budget
```

This test inserts 10k tasks, 1k idempotency entries, and 1k sessions, verifying that the workload can be created successfully. In production, monitor actual RSS via `docker stats` or Kubernetes metrics.
