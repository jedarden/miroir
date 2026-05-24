# Miroir

[![CI](https://img.shields.io/badge/CI-Argo%20Workflows-blue)](https://github.com/jedarden/miroir/actions)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![SemVer](https://img.shields.io/badge/SemVer-2.0.0-blue)](https://semver.org/spec/v2.0.0.html)

**Multi-node Index Replication Orchestrator, Integrated Rebalancing**

Miroir is a RAID-like orchestration layer for Meilisearch. It stripes a large index across a fleet of small-RAM Meilisearch nodes with a configurable replication factor, fans out search queries across all shards, and rebalances shard assignments when nodes are added or removed — all using the Meilisearch Community Edition.

## The Problem

Meilisearch loads its entire index into memory-mapped LMDB files. A large index that exceeds a single server's available RAM cannot run on that server. The Enterprise Edition's native sharding is gated behind a commercial license. Miroir solves this without it.

## How It Works

```
Client
  │
  ▼
Miroir Orchestrator
  ├── Write path: hash(doc_id) → assign to shard → write to R replicas
  ├── Read path:  scatter query to all shards → gather → merge ranked results
  └── Rebalance: on node add/remove → recompute assignments → migrate minimum shards

Meilisearch Nodes (N instances, each holding a subset of shards)
  node-0   node-1   node-2   ...   node-N
```

## Replication Factor

Analogous to software RAID — configurable per deployment:

| RF | Redundancy | Node failures tolerated | Capacity |
|----|-----------|------------------------|----------|
| 1  | None (stripe only) | 0 | 100% of fleet |
| 2  | One replica | 1 per shard group | 50% of fleet |
| 3  | Two replicas | 2 per shard group | 33% of fleet |

## Key Components

- **Orchestrator** — proxy that handles shard routing, scatter-gather, result merging, and topology management
- **Shard router** — consistent hash function (Rendezvous/HRW) mapping document IDs to node assignments; minimal reshuffling on topology change
- **Rebalancer** — on node add/remove, recomputes assignments and migrates only the shards that changed owners; surviving replicas serve reads during rebuild
- **Result merger** — normalizes and merges ranked result sets from multiple shards into a single coherent response

## Feature Matrix

Miroir implements 21 advanced capabilities (plan §13) that sit entirely within the orchestrator layer. Every Meilisearch node runs **unmodified Community Edition** — no patches, no forks, no custom builds.

| Capability | Description | Default |
|------------|-------------|---------|
| §13.1 Online resharding | Change shard count without reindex via shadow index | on |
| §13.2 Hedged requests | Tail-latency mitigation via duplicate requests to alternate replicas | on |
| §13.3 Adaptive replica selection | EWMA-based routing to lowest-latency nodes | on |
| §13.4 Shard-aware query planner | Narrow fan-out for PK-constrained searches | on |
| §13.5 Two-phase settings broadcast | Atomic settings changes with verification | on |
| §13.6 Read-your-writes | Session pinning for immediate consistency | on |
| §13.7 Atomic index aliases | Blue-green reindexing and multi-target aliases | on |
| §13.8 Anti-entropy reconciler | Continuous shard repair and drift detection | on |
| §13.9 Streaming dump import | Route documents during import (no broadcast) | on |
| §13.10 Idempotency keys | Request deduplication and query coalescing | on |
| §13.11 Multi-search | Batch API for multiple queries in one round-trip | on |
| §13.12 Vector + hybrid search | Over-fetch with RRF/convex merging for correct global ranking | on |
| §13.13 CDC stream | Change data capture to webhook/NATS/Kafka/internal queue | on |
| §13.14 Document TTL | Automatic expiration with background sweeper | on |
| §13.15 Tenant affinity | Route tenant queries to dedicated replica groups | on |
| §13.16 Traffic shadow | Async request tee to shadow cluster with diff analysis | on |
| §13.17 ILM | Rolling time-series indexes with rollover policies | on |
| §13.18 Canary queries | Synthetic queries with golden assertions for relevance testing | on |
| §13.19 Admin Web UI | Embedded SPA for topology, config, query debugging, operations | on |
| §13.20 Query explain | Debug routing decisions and warnings without executing | on |
| §13.21 End-user Search UI | Embedded instant-search SPA with facets, keyboard nav, i18n | on |

See [`docs/plan/plan.md#13-advanced-capabilities`](docs/plan/plan.md#13-advanced-capabilities) for detailed design of each capability.

## Stability

Miroir is currently in development (v0.x). Starting with v1.0, the project provides backward-compatibility commitments for the Meilisearch API layer, `miroir-ctl` CLI, config file schema, and Helm chart values.

See [`docs/versioning-policy.md`](docs/versioning-policy.md) for the full versioning policy, including what constitutes a breaking change and the deprecation process.

## Documentation

- [Design Plan](docs/plan/plan.md) — Complete architecture, protocol, and capability specifications
- [CHANGELOG.md](CHANGELOG.md) — Release notes and version history
- [Helm Chart](charts/miroir/) — Production deployment on Kubernetes
- [Deployment Guides](docs/onboarding/) — Production setup, sizing, and operational considerations
- [Migration Runbook](docs/migration_runbook.md) — Paths from single-node Meilisearch to Miroir

## Quick Start

Get Miroir running locally in 5 minutes with Docker Compose:

```bash
# Clone the repository
git clone https://github.com/jedarden/miroir.git
cd miroir

# Start the development stack (3 Meilisearch nodes + 1 Miroir orchestrator)
docker compose -f examples/docker-compose-dev.yml up -d

# Verify health
curl http://localhost:7700/health
# Expected: {"status":"available"}

# Index documents (Meilisearch-compatible API)
curl -X POST http://localhost:7700/indexes/movies/documents \
  -H "Authorization: Bearer dev-key" \
  -H "Content-Type: application/json" \
  -d '[{"id": 1, "title": "Inception"}, {"id": 2, "title": "Interstellar"}]'

# Search
curl -X POST http://localhost:7700/indexes/movies/search \
  -H "Authorization: Bearer dev-key" \
  -H "Content-Type: application/json" \
  -d '{"q": "inception"}'

# Teardown (removes containers and volumes)
docker compose -f examples/docker-compose-dev.yml down -v
```

See [`examples/README.md`](examples/README.md) for more details on the development stack, configuration options, and troubleshooting.

## Production deployment

For production deployments, see the [Deployment Sizing Guide](docs/horizontal-scaling/sizing.md) to determine orchestrator pod count and task store configuration based on your corpus size and query throughput.

### When to use

- **Multi-pod with Redis** — Recommended for production. Horizontal scaling with 2+ orchestrator pods delivers fault tolerance (zero-downtime rollouts, pod-loss survival) and scales query throughput via HPA. See [Deployment Sizing Guide](docs/horizontal-scaling/sizing.md).

- **Single oversized pod** — Supported for dev clusters, very small deployments, or constrained environments. A single pod at 4 vCPU / 8 GB is validated but loses HA benefits (no zero-downtime rollouts, no pod-loss survival). See [Single-Pod Mode](docs/horizontal-scaling/single-pod.md).

- **Large index sharding** — When a single Meilisearch node cannot fit your corpus in RAM, Miroir stripes it across multiple nodes with configurable replication factor.

Additional production resources:
- [Production Deployment Guide](docs/onboarding/production.md) — Operational considerations, monitoring, and troubleshooting
- [Per-Feature Scaling Behavior](docs/horizontal-scaling/per-feature.md) — Which features need Redis, work queues, or nothing
- [Versioning Policy](docs/versioning-policy.md) — Backward compatibility commitments and upgrade guidance
