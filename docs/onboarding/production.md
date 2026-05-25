# Production Deployment Guide

This guide covers operational considerations for running Miroir in production.

## Sizing

Start with the [Deployment Sizing Guide](../horizontal-scaling/sizing.md) to determine the number of orchestrator pods and task store configuration for your workload.

## Resource envelope

Each orchestrator pod is designed for **2 vCPU / 3.75 GB RAM**. This matches common small-instance tiers across cloud providers:

- AWS: t3.medium
- GCP: e2-medium
- Hetzner: CX22
- Rackspace Spot: 2c/3.75GB

See [plan.md §14](../plan/plan.md#14-resource-envelope-and-horizontal-scaling) for the full resource budget breakdown.

## Task store selection

| Replicas | Recommended task store |
|----------|----------------------|
| 1 | SQLite (default) |
| 2+ | Redis (required) |

The Helm chart's `values.schema.json` enforces this requirement — it rejects configurations where `miroir.replicas > 1` and `taskStore.backend: sqlite`.

For Redis deployment modes:
- **Small deployments** (≤ 200 GB corpus): Single Redis instance is sufficient
- **Medium deployments** (≤ 1 TB corpus): Redis with persistence enabled
- **Large deployments** (≤ 5 TB corpus): Redis Cluster or Sentinel for HA

## Horizontal Pod Autoscaler

Enable HPA for production deployments with variable traffic:

```yaml
miroir:
  hpa:
    enabled: true
    minReplicas: 2
    maxReplicas: 24
```

HPA requires `taskStore.backend: redis` and `miroir.replicas >= 2`. The chart enforces these constraints.

See the [sizing matrix](../horizontal-scaling/sizing.md#sizing-matrix) for recommended replica ranges per workload tier.

## High availability

### Minimum HA configuration

- **2 orchestrator pods** (handles one pod failure without downtime)
- **Redis task store** (shared state across pods)
- **Replication factor ≥ 2** for Meilisearch (handles one node failure per shard group)

### Leader election

The orchestrator uses Redis-based leader election for operations that require exactly-one semantics (settings broadcast, ILM rollover). Leader lease TTL is 30s with heartbeat renewal at 10s intervals — a new leader is elected within 30s if the current leader fails.

### Pod disruption budgets

The chart includes a PodDisruptionBudget that ensures minimum availability during voluntary disruptions:

```yaml
minAvailable: 1
```

For stricter guarantees, adjust to `minAvailable: 2` or use percentage-based policies.

## Monitoring

### Resource pressure metrics

Key metrics to monitor:

| Metric | Meaning | Alert threshold |
|--------|---------|-----------------|
| `miroir_requests_in_flight` | Concurrent requests | > 80% of `server.max_concurrent_requests` |
| `miroir_background_queue_depth` | Pending background jobs | > 10 for > 5min |
| `miroir_task_store_latency_seconds` | Task store operation p99 | > 100ms |
| Container CPU | Orchestrator CPU utilization | > 80% sustained |
| Container memory | Orchestrator RAM usage | > 85% sustained |

See [plan.md §14.9](../plan/plan.md#149-resource-pressure-metrics-and-alerts) for the full alerting catalog.

### Search UI rate limiting

When `miroir.replicas > 1`, the Search UI rate limiter **must** use Redis backend:

```yaml
search_ui:
  rate_limit:
    backend: redis  # Required for multi-pod deployments
```

With `backend: local`, the effective cluster-wide rate is `per_ip × pod_count` because each pod maintains its own bucket table.

## Vertical scaling escape valve

If your environment cannot support horizontal scaling (e.g., single-node Kubernetes), you can use a larger pod size (e.g., 4 vCPU / 8 GB). This is supported but not recommended for production — horizontal scaling provides better fault isolation and elasticity.

See [plan.md §14.10](../plan/plan.md#1410-vertical-scaling-escape-valve) for trade-offs.

## Feature flags for memory-constrained deployments

Every advanced capability has an `enabled: true` knob that can be flipped off to reclaim memory:

| Feature | Memory savings | Config path |
|---------|---------------|-------------|
| Vector search | ~30 MB per pod | `vector_search.enabled` |
| Multi-search | ~5 MB per pod | `multi_search.enabled` |
| Shadow tee | ~50 MB per pod (5% sample) | `shadow.enabled` |
| CDC publisher | ~64 MB per pod | `cdc.enabled` |

See [plan.md §13](../plan/plan.md#13-feature-scaling-modes) for the full feature list and scaling modes.

## Backup and recovery

### Meilisearch data

Meilisearch persists index data to disk at `/meilisearch-data`. Backup strategy:

1. **Snapshot volumes** — Take volume snapshots before index operations (reshard, ILM rollover)
2. **Dump exports** — Use `/_indexes/{uid}/documents/export` for logical backups
3. **RG/RF awareness** — With replication factor > 1, you can survive node loss without data loss

### Task store (Redis)

Redis task store contains:
- Idempotency keys (ephemeral, TTL 24h)
- Session pinning state (ephemeral, TTL per session)
- Background job queues (rebuildable from operations log)
- Leader lease (ephemeral, auto-recoverable)

Redis data is **rebuildable** — a fresh Redis instance is acceptable after failure. The orchestrator will reconstruct queues and lease state on startup.

For persistence:
- Enable RDB snapshots (`save 900 1`, `save 300 10`, `save 60 10000`)
- Or use AOF (`appendonly yes`) for stronger durability

### Configuration

Store your Helm values in git. The chart supports external secrets for sensitive values (API keys, JWT secrets).

## Upgrade path

See [versioning-policy.md](../versioning-policy.md) for backward compatibility commitments.

Upgrade procedure:
1. Read the changelog for breaking changes
2. Test in non-production first
3. Roll out orchestrator pods first (Meilisearch API compatibility)
4. Update Meilisearch nodes independently

## Troubleshooting

For comprehensive troubleshooting of common issues, see the [Troubleshooting Guide](../troubleshooting.md) and [Diagnostic Playbook](../troubleshooting/diagnostics.md).

### Orchestrator pods stuck in CrashLoopBack

Check task store connectivity:
```bash
kubectl logs -n search deployment/miroir --tail 100 | grep -i task
```

If Redis is unreachable, verify `taskStore.backend` and connection settings.

### High memory usage

Identify the culprit:
1. Check `miroir_idempotency_cache_entries` — idempotency cache may be oversized
2. Check `miroir_session_pinning_active_sessions` — session pinning may be leaking
3. Review feature flags — disable unused capabilities

### HPA not scaling

Verify HPA metrics are reporting:
```bash
kubectl get hpa -n search
```

If metrics are `<unknown>`, install or configure the Metrics Server / Prometheus Adapter.

## Further reading

- [Deployment Sizing Guide](../horizontal-scaling/sizing.md) — Corpus/QPS → pod count matrix
- [plan.md §14](../plan/plan.md#14-resource-envelope-and-horizontal-scaling) — Full resource envelope documentation
- [plan.md §13](../plan/plan.md#13-feature-scaling-modes) — Per-feature scaling behavior
