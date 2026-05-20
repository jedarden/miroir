# Miroir

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

## Stability

Miroir is currently in development (v0.x). Starting with v1.0, the project provides backward-compatibility commitments for the Meilisearch API layer, `miroir-ctl` CLI, config file schema, and Helm chart values.

See [`docs/versioning-policy.md`](docs/versioning-policy.md) for the full versioning policy, including what constitutes a breaking change and the deprecation process.

## Status

Design phase. See [`docs/`](docs/) for architecture detail.

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
- [Versioning Policy](docs/versioning-policy.md) — Backward compatibility commitments and upgrade guidance
