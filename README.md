# Miroir

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![SemVer](https://img.shields.io/badge/SemVer-2.0.0-blue)](https://semver.org/spec/v2.0.0.html)
[![Latest Release](https://img.shields.io/github/v/release/jedarden/miroir)](https://github.com/jedarden/miroir/releases)

**Multi-node Index Replication Orchestrator, Integrated Rebalancing**

Miroir is a RAID-like sharding and high-availability layer for Meilisearch Community Edition. It stripes a large index across a fleet of small-RAM Meilisearch nodes with a configurable replication factor, fans out search queries across all shards, and rebalances shard assignments when nodes are added or removed — all while presenting a standard Meilisearch-compatible API to clients.

## What is Meilisearch?

[Meilisearch](https://www.meilisearch.com/) is an open-source, typo-tolerant search engine written in Rust. It is fast to set up, easy to integrate via REST API, and ships official SDKs for most languages. Meilisearch Community Edition (CE) is MIT-licensed and free to use; Meilisearch Enterprise adds commercial features including native multi-node sharding.

Miroir targets Meilisearch CE users who need horizontal scale without a commercial license.

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

Miroir uses **rendezvous hashing (HRW)** — the same algorithm used internally by Meilisearch Enterprise — to assign documents to shards. This minimizes reshuffling when the cluster topology changes: only the shards that change owners need to be migrated. All Meilisearch nodes run **unmodified Community Edition** — no patches, no forks, no custom builds.

The logical shard count `S` is fixed at index creation time. The node count `N` is fully elastic: you can add or remove nodes at any time and trigger a rebalance to redistribute shard ownership.

## Replication Factor

Analogous to software RAID — configurable per deployment:

| RF | Redundancy         | Node failures tolerated | Usable capacity |
|----|--------------------|-------------------------|-----------------|
| 1  | None (stripe only) | 0                       | 100% of fleet   |
| 2  | One replica        | 1 per shard group       | 50% of fleet    |
| 3  | Two replicas       | 2 per shard group       | 33% of fleet    |

Miroir also supports **replica groups (RG)** — independent sets of replicas that can serve read traffic in parallel, scaling read throughput horizontally beyond what a single RF set provides.

## Crates

Miroir is a three-crate Rust workspace:

| Crate | Description |
|-------|-------------|
| `miroir-core` | Core sharding logic: rendezvous hash router, rebalancer, scatter-gather engine, result merger |
| `miroir-proxy` | HTTP proxy server that presents the Meilisearch-compatible REST API to clients |
| `miroir-ctl` | CLI management tool for cluster operations: node management, rebalancing, status |

Static binaries compiled against musl libc; Docker images are built from scratch for minimal attack surface.

## Quick Start

Get Miroir running locally in 5 minutes with Docker Compose.

### `examples/docker-compose-dev.yml`

```yaml
services:
  meili-0:
    image: getmeili/meilisearch:v1.37.0
    environment:
      - MEILI_MASTER_KEY=dev-node-key
  meili-1:
    image: getmeili/meilisearch:v1.37.0
    environment:
      - MEILI_MASTER_KEY=dev-node-key
  meili-2:
    image: getmeili/meilisearch:v1.37.0
    environment:
      - MEILI_MASTER_KEY=dev-node-key
  miroir:
    build:
      context: ..
      dockerfile: Dockerfile
    image: miroir-dev:latest
    environment:
      - MIROIR_MASTER_KEY=dev-key
      - MIROIR_NODE_MASTER_KEY=dev-node-key
    ports:
      - "7700:7700"
    volumes:
      - ./examples/dev-config.yaml:/etc/miroir/config.yaml:ro
```

### `examples/dev-config.yaml`

```yaml
master_key: "dev-key"
node_master_key: "dev-node-key"
shards: 16
replication_factor: 1
replica_groups: 1
nodes:
  - id: "meili-0"
    address: "http://meili-0:7700"
    replica_group: 0
  - id: "meili-1"
    address: "http://meili-1:7700"
    replica_group: 0
  - id: "meili-2"
    address: "http://meili-2:7700"
    replica_group: 0
task_store:
  backend: sqlite
  path: /data/miroir-tasks.db
server:
  port: 7700
```

### Running

```bash
# Clone the repository
git clone https://github.com/jedarden/miroir.git
cd miroir

# Start the development stack (3 Meilisearch nodes + 1 Miroir orchestrator)
docker compose -f examples/docker-compose-dev.yml up -d

# Verify health
curl http://localhost:7700/health
# Expected: {"status":"available"}

# Index documents (standard Meilisearch API — no changes needed)
curl -X POST http://localhost:7700/indexes/movies/documents \
  -H "Authorization: Bearer dev-key" \
  -H "Content-Type: application/json" \
  -d '[{"id":1,"title":"The Matrix"},{"id":2,"title":"Inception"}]'

# Search
curl -X POST http://localhost:7700/indexes/movies/search \
  -H "Authorization: Bearer dev-key" \
  -H "Content-Type: application/json" \
  -d '{"q":"matrix"}'

# Teardown (removes containers and volumes)
docker compose -f examples/docker-compose-dev.yml down -v
```

See [`examples/README.md`](examples/README.md) for more details on the development stack, configuration options, and troubleshooting.

## miroir-ctl

`miroir-ctl` is the CLI management tool for operating a Miroir cluster.

```bash
# Cluster status — node health, shard distribution, replication state
miroir-ctl status

# List nodes
miroir-ctl nodes list

# Add a node to the cluster (does not trigger rebalance automatically)
miroir-ctl nodes add --id meili-3 --address http://meili-3:7700 --group 0

# Remove a node — migrates its shards away before removal
miroir-ctl nodes remove meili-1

# Trigger a rebalance (after adding/removing nodes)
miroir-ctl rebalance

# Watch rebalance progress
miroir-ctl rebalance status --watch
```

## SDK Configuration

Miroir is a drop-in replacement at the network layer. Migrating from a single Meilisearch node requires only changing the endpoint URL. All index operations, document CRUD, and search queries work without modification.

### Python

```python
# Before — single-node Meilisearch
client = meilisearch.Client('https://old-meili.example.com', 'api-key')

# After — Miroir
client = meilisearch.Client('https://search.example.com', 'miroir-master-key')
```

### TypeScript / JavaScript

```typescript
// Before — single-node Meilisearch
const client = new MeiliSearch({
  host: 'https://old-meili.example.com',
  apiKey: 'api-key'
})

// After — Miroir
const client = new MeiliSearch({
  host: 'https://search.example.com',
  apiKey: 'miroir-master-key'
})
```

### Go

```go
// Before — single-node Meilisearch
client := meilisearch.NewClient(meilisearch.ClientConfig{
    Host:   "https://old-meili.example.com",
    APIKey: "api-key",
})

// After — Miroir
client := meilisearch.NewClient(meilisearch.ClientConfig{
    Host:   "https://search.example.com",
    APIKey: "miroir-master-key",
})
```

That's it — no other code changes required. Miroir presents the same Meilisearch-compatible API surface to all official SDKs.

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

## Production Deployment

For production deployments, see the [Deployment Sizing Guide](docs/horizontal-scaling/sizing.md) to determine orchestrator pod count and task store configuration based on your corpus size and query throughput.

### When to use

- **Multi-pod with Redis** — Recommended for production. Horizontal scaling with 2+ orchestrator pods delivers fault tolerance (zero-downtime rollouts, pod-loss survival) and scales query throughput via HPA. See [Deployment Sizing Guide](docs/horizontal-scaling/sizing.md).

- **Single oversized pod** — Supported for dev clusters, very small deployments, or constrained environments. A single pod at 4 vCPU / 8 GB is validated but loses HA benefits (no zero-downtime rollouts, no pod-loss survival). See [Single-Pod Mode](docs/horizontal-scaling/single-pod.md).

- **Large index sharding** — When a single Meilisearch node cannot fit your corpus in RAM, Miroir stripes it across multiple nodes with configurable replication factor.

Additional production resources:
- [Production Deployment Guide](docs/onboarding/production.md) — Operational considerations, monitoring, and troubleshooting
- [Per-Feature Scaling Behavior](docs/horizontal-scaling/per-feature.md) — Which features need Redis, work queues, or nothing
- [Versioning Policy](docs/versioning-policy.md) — Backward compatibility commitments and upgrade guidance

## Stability

Miroir is currently in active development (v0.x). The API, configuration schema, and CLI flags may change between minor versions. Starting with v1.0, the project provides backward-compatibility commitments for:

- The Meilisearch-compatible API layer
- The `miroir-ctl` CLI interface
- The config file schema
- The Helm chart values

See [`docs/versioning-policy.md`](docs/versioning-policy.md) for the full versioning policy, including what constitutes a breaking change and the deprecation process.

## Documentation

- [Design Plan](docs/plan/plan.md) — Complete architecture, protocol, and capability specifications
- [CHANGELOG.md](CHANGELOG.md) — Release notes and version history
- [API Compatibility](docs/notes/api-compatibility.md) — Meilisearch API coverage and extensions
- [Helm Chart](charts/miroir/) — Production deployment on Kubernetes
- [Deployment Guides](docs/onboarding/) — Production setup, sizing, and operational considerations
- [Migration Runbook](docs/migration_runbook.md) — Paths from single-node Meilisearch to Miroir
- [Troubleshooting Guide](docs/troubleshooting.md) — Common issues and diagnostic playbook

## Community

- [Issues](https://github.com/jedarden/miroir/issues) — Bug reports and feature requests
- [Discussions](https://github.com/jedarden/miroir/discussions) — Q&A and design discussions
- [Contributing](CONTRIBUTING.md) — Development workflow and code submission guidelines

## License

MIT License — see [LICENSE](LICENSE) for details.
