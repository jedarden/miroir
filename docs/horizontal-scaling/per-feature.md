# Per-Feature Scaling Behavior

This document maps every advanced capability (plan §13.x) to its horizontal scaling mode. Operators use this reference to answer: *Is feature X horizontally safe? Does it need Redis? A work queue? Nothing?*

## Scaling mode summary

| Mode | Description | Examples |
|------|-------------|----------|
| **A** | Shard-partitioned (rendezvous hash ownership) | Anti-entropy, drift reconciler, TTL sweeper, canaries |
| **B** | Leader-only (singleton coordinator) | Reshard, rebalancer, 2PC settings broadcast, ILM |
| **C** | Work-queued (chunked jobs) | Dump import, reshard backfill |
| **Stateless per-request** | No coordination needed | Hedged requests, multi-search, vector search |
| **Per-pod** | Each pod independently | Plan cache, EWMA scores, admin UI serving |

Mode A/B/C are implemented in beads [`miroir-m9q.3`](https://github.com/jedarden/miroir/bead/miroir-m9q.3), [`miroir-m9q.4`](https://github.com/jedarden/miroir/bead/miroir-m9q.4), and [`miroir-m9q.5`](https://github.com/jedarden/miroir/bead/miroir-m9q.5) respectively.

## Feature-to-mode mapping

| Capability | Scaling mode | Notes | Bead |
|------------|-------------|-------|------|
| §13.1 Online resharding | B (leader) + C (backfill queue) | Leader owns phase state machine; any pod consumes backfill chunks. Bounded-memory backfill via paginated `filter=_miroir_shard={id}`. | [`miroir-uhj.1`](https://github.com/jedarden/miroir/bead/miroir-uhj.1) |
| §13.2 Hedged requests | Stateless per-request | No coordination needed — each pod hedges its own requests. | TBD |
| §13.3 Adaptive replica selection | Per-pod EWMA | Each pod's scores are local; pods converge independently. Slight divergence is harmless. | TBD |
| §13.4 Shard-aware query planner | Per-request | Pure function of filter. Plan cache is per-pod. | TBD |
| §13.5 Two-phase settings broadcast | B (leader) | Leader issues PATCH and verifies. Drift reconciler runs in mode A. | [`miroir-uhj.5`](https://github.com/jedarden/miroir/bead/miroir-uhj.5) |
| §13.6 Session pinning | Shared-state per-pod cache | Session row lives in task store (Redis); per-pod LRU caches it. Any pod can serve a session. | [`miroir-uhj.6`](https://github.com/jedarden/miroir/bead/miroir-uhj.6) |
| §13.7 Atomic index aliases | Shared state | Alias table in task store. All pods read same table with short TTL cache. | [`miroir-uhj.7`](https://github.com/jedarden/miroir/bead/miroir-uhj.7) |
| §13.8 Anti-entropy reconciler | A (shard-partitioned) | Each pod fingerprints its owned shards. Naturally horizontal. | [`miroir-uhj.8`](https://github.com/jedarden/miroir/bead/miroir-uhj.8) |
| §13.9 Streaming dump import | C (chunked jobs) | 500 GB dump → chunks → pods consume from queue; HPA scales on queue depth. | [`miroir-uhj.9`](https://github.com/jedarden/miroir/bead/miroir-uhj.9) |
| §13.10 Idempotency + coalescing | Per-pod + shared fallback | Idempotency cache per-pod with task-store lookup on miss — a retry on a different pod still dedups. Coalescing is per-pod only (acceptable: identical concurrent queries hitting different pods each issue their own scatter, which is rare and bounded by pod count). | TBD |
| §13.11 Multi-search | Stateless per-request | Sub-queries fan out using existing scatter infrastructure; each sub-query is independently routed. | TBD |
| §13.12 Vector / hybrid search | Stateless per-request | Merger uses more memory per request (see sizing.md vector over-fetch scratch row); no cross-pod coordination. | TBD |
| §13.13 CDC publisher | Per-pod publishers with shared cursors | `cdc_cursors` in the task store serialize cursor advancement via compare-and-swap; each pod publishes its own shard of events. Overflow buffer in Redis is shared across pods. | TBD |
| §13.14 TTL sweeper | A (shard-partitioned) | Each pod sweeps only its rendezvous-owned shards; no duplicate deletes across pods. | TBD |
| §13.15 Tenant affinity | Stateless per-request | Hash-or-explicit routing decision; no shared state on the hot path (tenant map LRU is per-pod). | TBD |
| §13.16 Shadow tee | Stateless per-request | Each pod independently decides (per its local `sample_rate` RNG) whether to shadow a given request. | TBD |
| §13.17 ILM rollover | B (leader-only) | Serialized alias flips + index create/delete; exactly one pod runs the daily policy evaluator at a time. | TBD |
| §13.18 Canary runner | A (shard-partitioned) | Each canary ID is rendezvous-owned by exactly one pod per interval; no duplicate canary runs. | TBD |
| §13.19 Admin UI | Per-pod | Any pod serves the SPA; stateful sections read the shared task store. | TBD |
| §13.20 Explain API | Stateless per-request | Pure function of request + topology + config; no cross-pod coordination. | TBD |
| §13.21 Search UI | Per-pod (SPA + static assets); rate limiter needs shared state | Any pod serves the SPA. **Rate-limiter requirement:** multi-pod deployments MUST set `search_ui.rate_limit.backend: redis` — see forced-mode constraints below. | TBD |

**Note.** TTL sweeper (§13.14), CDC publisher (§13.13), and canary runner (§13.18) are all Mode-A partitioned across pods; each pod consumes only its share of the workload, so the memory budgets in [sizing.md](sizing.md) for these features scale with `1/pod_count`.

## Forced-mode constraints

The Helm chart's `values.schema.json` enforces horizontal-scaling correctness by rejecting invalid configurations. These rules prevent operators from accidentally configuring a feature in a mode that doesn't scale safely.

| Rule | Constraint | Rationale |
|------|------------|-----------|
| **Rule 0** | `taskStore.backend: redis` requires `miroir.replicas > 1` | HA mode requires multiple replicas; a single replica with Redis wastes resources without gaining availability. |
| **Rule 1** | `miroir.replicas > 1` requires `taskStore.backend: redis` | SQLite is single-writer and cannot be shared across pods. Multi-pod deployments must use Redis for shared state. |
| **Rule 2** | `hpa.enabled: true` requires `replicas >= 2` AND `taskStore.backend: redis` | HPA is meaningless for a single pod (horizontal scaling requires multiple replicas). HPA also requires Redis for the shared metrics and job queue. |
| **Rule 3** | `search_ui.rate_limit.backend: local` rejected when `miroir.replicas > 1` | With `backend: local`, each pod counts rate limits independently, so the effective cluster-wide rate is `per_ip × pod_count`. An attacker can bypass the limit by rotating across pods. Redis provides a shared bucket. |
| **Rule 4** | `admin_ui.rate_limit.backend: local` rejected when `miroir.replicas > 1` | Same rationale as Rule 3 — per-pod rate limiting defeats the purpose when there are multiple pods serving the admin UI. |

These schema rejections are enforced at `helm install`/`helm upgrade` time. The rules live in `charts/miroir/values.schema.json` under the `allOf` section.

## Related documentation

- [Deployment Sizing Guide](sizing.md) — Workload tiers and orchestrator pod counts
- [Single-Pod Mode](single-pod.md) — When to use a single oversized pod instead of horizontal scaling
- [Production Deployment Guide](../onboarding/production.md) — Operational considerations and monitoring
- [Plan §14.6](../plan/plan.md#146-per-feature-scaling-behavior) — Full design specification
- [Plan §14.5](../plan/plan.md#145-horizontal-scaling-background-work) — Mode A/B/C implementation details
