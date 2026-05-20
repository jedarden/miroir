# Single-Pod Mode

Single-pod mode runs Miroir with a single orchestrator pod using SQLite as the task store. This is a **vertical scaling escape valve** for dev clusters, very small deployments, or constrained environments.

## Configuration

```yaml
miroir:
  replicas: 1
  resources:
    limits:
      cpu: "4"
      memory: "8Gi"
    requests:
      cpu: "2"
      memory: "4Gi"

taskStore:
  backend: sqlite
  path: /data/miroir-tasks.db

hpa:
  enabled: false
```

## Memory multiplier behavior

When provisioning a single oversized pod, the memory budgets scale linearly by the multiplier relative to the baseline (2 vCPU / 3.75 GB):

| Memory limit | Multiplier | `idempotency.max_cached_keys` | `session_pinning.max_sessions` | `task_registry.cache_size` |
|--------------|------------|------------------------------|-------------------------------|----------------------------|
| 3.75 GB | 1× (baseline) | 1,000,000 | 100,000 | 10,000 |
| 7.5 GB | 2× | 2,000,000 | 200,000 | 20,000 |
| 8 GB | ~2.13× | 2,130,000 | 213,000 | 21,300 |

**Operator override:** Instead of relying on auto-scaling, operators may explicitly set these values in their config:

```yaml
idempotency:
  max_cached_keys: 2000000

session_pinning:
  max_sessions: 200000

task_registry:
  cache_size: 20000
```

## Trade-offs

Single-pod mode is **supported but not recommended for production**:

| Aspect | Multi-pod (recommended) | Single-pod |
|--------|------------------------|------------|
| **Fault tolerance** | Survives pod loss; zero-downtime rollouts | Pod loss = full outage; rolling updates cause downtime |
| **Query throughput** | Scales horizontally via HPA | Limited to single pod capacity |
| **Memory pressure** | Distributed across pods | All budgets on one pod |
| **Operational complexity** | Requires Redis task store | Simpler (SQLite only) |

## When to use

- **Dev clusters** — Local testing, development environments
- **Very small deployments** — Low query throughput, small corpus
- **Constrained environments** — Limited node capacity, resource quotas
- **Edge deployments** — Single-node Kubernetes clusters

## Migration to multi-pod

To migrate from single-pod to multi-pod:

1. Provision a Redis instance (or enable the chart's Redis)
2. Change `taskStore.backend: redis`
3. Set `miroir.replicas: 2`
4. Enable `hpa.enabled: true` (optional)
5. Apply the updated Helm release

Data migration is automatic — the orchestrator will replicate SQLite state to Redis on startup.

## Reference

See [plan.md §14.10](../plan/plan.md#1410-vertical-scaling-escape-valve) for the full design specification.
