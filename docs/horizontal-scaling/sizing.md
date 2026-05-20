# Deployment Sizing Guide

This guide provides a sizing matrix for provisioning Miroir orchestrator pods based on corpus size and query throughput. Meilisearch node sizing follows the guidance in [plan.md §6](../../plan/plan.md) independently.

## Sizing matrix

| Corpus | Peak QPS | Orchestrator pods | Task store |
|--------|----------|-------------------|------------|
| ≤ 10 GB | ≤ 500 | 2 (HA) | Redis (or SQLite if replicas=1) |
| ≤ 50 GB | ≤ 2 k | 2–4 (HPA) | Redis |
| ≤ 200 GB | ≤ 5 k | 4–8 (HPA) | Redis |
| ≤ 1 TB | ≤ 20 k | 8–12 (HPA) | Redis |
| ≤ 5 TB | ≤ 100 k | 12–24 (HPA) | Redis (clustered or Sentinel) |

**Orchestrator pod specification:** 2 vCPU / 3.75 GB per pod.

**Key insight:** Orchestrator count scales with *query throughput*, while Meilisearch node count scales with *corpus size* and replication factor. These are orthogonal axes.

## Task-store memory accounting

When Redis is the task store, it backs shared state for multiple subsystems:

- **Idempotency replay keys** — request deduplication cache
- **Session pinning rows** — per-session sticky routing state
- **Alias cache** — atomic index alias mappings
- **Background job queue** — chunked dump import, reshard backfill jobs
- **Leader lease** — HA coordinator election state
- **CDC overflow buffer** — change-data-capture spill-over (when configured)
- **Search UI rate-limit buckets** — IP-based rate limiter state

Add **~20 MB for search UI rate-limit buckets per 10k active IPs** on top of the task-store baseline when `search_ui.rate_limit.backend: redis`. Bucket rows auto-expire per `redis_ttl_s`, keeping the footprint bounded even under IP-scan or spray attacks.

## Worked example: ≤ 200 GB / ≤ 5 k QPS

**From the table:** 4–8 orchestrator pods (HPA-scaled between these limits), Redis task store.

### Memory budget validation

Per [plan.md §14.2](../plan/plan.md#142-per-pod-memory-budget), steady-state per-pod memory at default config:

| Component | Budget |
|-----------|--------|
| Baseline (runtime + pools) | ~330 MB |
| Request/response buffers (p99 concurrent) | 200 MB |
| Task registry + idempotency + sessions | 250 MB |
| Feature overhead (§13.11–21 features) | ~200 MB |
| Allocator headroom | ~800 MB |
| **Steady-state total (idle background)** | **~1.2 GB** |
| **With one heavy background job active** | **~1.7 GB** |

At 4–8 pods, cluster-wide steady-state memory is 4.8–9.6 GB, which fits within a single Redis instance (allocate 4–8 GB for Redis at this tier). Heavy background jobs only run on the pod that claimed them from the shared queue — not all pods simultaneously — so the per-pod budget stays within envelope.

### Query throughput validation

Per [plan.md §14.3](../../plan/plan.md#143-per-pod-cpu-budget):

- **Small searches** (1 KB responses): ~3 kQPS per pod at 70% CPU
- **Large searches** (10 KB responses): ~1 kQPS per pod at 70% CPU

At 5 kQPS peak with 4–8 pods:
- Each pod handles 625–1250 QPS
- This is within the 1–3 kQPS per-pod envelope
- HPA will scale up toward 8 pods during sustained peak

### Result

The ≤ 200 GB / ≤ 5 k QPS row is validated: 4–8 orchestrator pods with Redis task store can handle the workload within the 2 vCPU / 3.75 GB per-pod resource envelope.

## When to escalate

If your workload exceeds the ≤ 5 TB / ≤ 100 k QPS tier, or if you're hitting resource limits despite following this matrix, consider:

1. **Vertical scaling escape valve** — For constrained environments, a single larger pod (e.g., 4 vCPU / 8 GB) is supported but not recommended for production. See [plan.md §14.10](../../plan/plan.md#1410-vertical-scaling-escape-valve).

2. **Redis clustering** — At very high scale, consider Redis Cluster or Sentinel for the task store to improve availability and distribute memory pressure.

3. **Tune feature flags** — Disable unused capabilities (e.g., multi-search, vector search, shadow tee) to reclaim memory. Every feature in [plan.md §13](../../plan/plan.md#13-feature-scaling-modes) has an `enabled: true` knob that can be flipped off.

4. **Consult the metrics** — Use the resource-pressure metrics in [plan.md §14.9](../../plan/plan.md#149-resource-pressure-metrics-and-alerts) to identify the bottleneck (CPU, memory, request queue depth, or background job backlog).
