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
