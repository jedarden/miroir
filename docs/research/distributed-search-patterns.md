# Distributed Search Patterns — Research for Miroir

Miroir is a RAID-like orchestration layer for Meilisearch CE: it must distribute documents across N nodes, scatter search queries, merge ranked results, and handle topology changes without relying on Meilisearch Pro/Enterprise. This document surveys how production distributed search systems solve those same problems.

---

## 1. Elasticsearch / OpenSearch

### Sharding Model

Each index is divided into a fixed number of primary shards at creation time. Shard count is **immutable** after index creation (without reindex). Documents are routed to a shard by:

```
shard = hash(routing_value) % number_of_primary_shards
```

The default `routing_value` is `_id`. A custom routing key can be set per-document or per-request, enabling co-location of related documents (important for aggregations that must not cross shard boundaries).

Each primary shard has zero or more replica shards. Replicas are placed on different nodes than their primary (enforced by the allocator). Replicas serve both read traffic and failover.

### Cluster State and Routing Table

The elected master node maintains the **cluster state**: a data structure containing the routing table (which shard lives on which node), index mappings, and node membership. Every node holds a copy. The master propagates cluster state updates via a publish/acknowledge protocol — no node acts on a routing change until the master confirms a quorum of nodes has acknowledged it.

When a node joins or leaves, the **ShardsAllocator** is invoked:
- Assigns orphaned primary shards to surviving nodes.
- Rebalances shard counts across nodes toward equal distribution.
- Respects constraints: no primary and its replica on the same node; allocation awareness attributes (rack, zone).
- Concurrent rebalancing is throttled (`cluster.routing.allocation.cluster_concurrent_rebalances`).

Shard recovery after failover requires replaying the translog (write-ahead log) from the last checkpoint, or copying segments from another replica.

### Scatter-Gather at the HTTP Layer

Any node can act as a **coordinating node** (the default role for nodes receiving client requests). The coordinating node:

1. **Query phase**: Sends the query to one copy (primary or replica, selected by adaptive replica selection) of every relevant shard. Each shard executes the query locally against its Lucene index, producing a priority queue of (doc_id, score) pairs of size `from + size`. Returns only IDs and scores — no source documents.

2. **Merge phase**: The coordinating node collects all priority queues, merges them globally, and selects the top `from + size` results. This requires sorting across `num_shards × (from + size)` entries, making deep pagination expensive.

3. **Fetch phase**: The coordinating node issues a multi-get to each shard that owns one of the selected top-N documents to retrieve `_source` and highlighting. This is a second round-trip.

4. **Response**: Assembled result sent to client.

Total round trips: 2 (query phase + fetch phase), plus the initial coordination overhead.

### The Global IDF Problem

BM25 scoring depends on IDF (Inverse Document Frequency), which is computed **per shard** by default using only that shard's local term statistics. If shards have unequal or unrepresentative document distributions, identical documents in different shards will receive different scores, breaking relevance ranking.

Two mitigations:

- **`dfs_query_then_fetch`**: Adds a pre-query round trip to all shards to gather global term statistics. The coordinating node merges them and sends global IDF values alongside the query phase request. Correct scores, higher latency.
- **Single-shard indexes**: Eliminates the problem; limits horizontal scaling.

Meilisearch CE uses its own ranking pipeline (not BM25), but a Miroir orchestration layer will face the same conceptual issue: ranking criteria computed locally on each shard node may not be globally comparable.

### Async Task Coordination

ES/OS do not expose explicit async task management at the search layer — searches are synchronous. For index operations (reindex, force-merge), ES exposes a Task API (`GET _tasks`). Long-running ops return a `task_id`; clients poll or use `wait_for_completion=false`.

### Topology Change Handling

- **Node joins**: Shards are proactively rebalanced to the new node. Concurrent shard moves are rate-limited.
- **Node leaves (graceful)**: `POST _cluster/reroute` can manually move shards before the node is removed.
- **Node fails**: Master detects via missed heartbeats (default 30s). Promotes a replica to primary immediately. Replication then rebuilds the lost replica elsewhere.
- **Routing table visibility**: Clients can call `GET _cluster/state` to observe current shard placement, or use `GET _cat/shards` for a human-readable view.

### OpenSearch Extensions

OpenSearch adds **Segment Replication**: instead of re-indexing documents on each replica, only Lucene segment files are copied from primary to replicas. This reduces CPU on replicas and increases indexing throughput at the cost of higher network I/O. Replicas that fall behind can serve slightly stale results during catch-up.

OpenSearch also supports **Search Replicas** (read-only, cannot be promoted to primary), allowing separate scaling of read vs. write capacity.

---

## 2. Typesense Cluster Mode

### Architecture: Full Replication, No Sharding

Typesense clusters are **not sharded**. Every node holds the entire dataset. This is a deliberate design choice: it simplifies consistency and eliminates scatter-gather complexity.

Consensus is managed via **Raft**: one leader, all writes go to the leader and are replicated to followers before acknowledgment. Reads can be served by any node (the node that receives the request serves it locally — no cross-node fan-out).

Fault tolerance: an N-node cluster tolerates ⌊(N-1)/2⌋ failures. Minimum 3 nodes for any HA.

### Implications

- No scatter-gather, no result merging — each node's answer is authoritative.
- Write throughput does not scale horizontally (all writes route to leader).
- Dataset size is bounded by single-node memory (Typesense is in-memory with on-disk persistence).
- Reads scale linearly: load balancers distribute reads across all nodes.

### What Miroir Can Learn

Typesense's model is the **anti-pattern** for Miroir's use case. Miroir exists specifically because CE Meilisearch cannot replicate across nodes. Typesense shows that replication-without-sharding is simpler to implement but hits dataset size walls. Miroir must shard (to distribute dataset across nodes) rather than replicate (to serve the same dataset from multiple nodes), or combine both.

### Search Delivery Network (SDN)

Typesense Cloud's geo-distributed SDN replicates the full dataset to multiple regions. Queries route to the nearest regional node. This is a pure read-scale / latency-optimization play, not capacity scaling.

---

## 3. Quickwit (Tantivy-based)

### Architecture: Stateless Searchers + Object Storage

Quickwit is the most architecturally relevant reference for Miroir. It separates storage from compute: all index data lives on object storage (S3/GCS), and searcher nodes are **stateless**. Any searcher can handle any query for any index. No data movement is required when the searcher pool scales.

Key components:
- **Indexers**: Consume from ingest API or Kafka, write immutable **splits** to object storage. A split is a self-contained Lucene index file bundle (UUID-identified, with a hotcache sidecar enabling ~60ms open time on S3).
- **Metastore**: Centralized catalog (PostgreSQL in production) storing split metadata: time ranges, tag filters, shard assignment.
- **Control Plane**: Schedules indexing tasks. Validates desired vs. actual indexing plans every 3 seconds; rebuilds plans every minute.
- **Searchers**: Stateless nodes. Any searcher acts as root or leaf depending on which node receives the request.

### Scatter-Gather

1. Root searcher queries the metastore to identify relevant splits (filtered by time range, tags, etc.).
2. Root distributes split evaluation tasks to leaf searchers using **rendezvous hashing** for split-to-searcher affinity. This maximizes cache hit rate: the same split is always sent to the same searcher, which has likely already downloaded it from S3.
3. Leaves execute the Tantivy query locally, return partial results.
4. Root merges partial results and returns the final response.

### Topology Changes

Because splits are immutable and stored in object storage, adding or removing searcher nodes requires no data movement. The rendezvous hash assignment shifts, slightly changing which splits land on which searchers (cache misses on the affected splits), but the system remains correct immediately.

### Lessons for Miroir

- **Split/shard immutability** is a key design win: topology changes become cache-miss events, not data migration events.
- **Rendezvous hashing for caching locality** is proven in production: same key → same node under stable conditions, graceful rebalancing on change.
- **Root/leaf terminology**: any node can be either, based on who received the request. No dedicated coordinator role.
- **Metastore as source of truth**: all nodes read the same catalog; no distributed routing table to keep in sync.

---

## 4. ZincSearch / Zinc

ZincSearch is a lightweight Elasticsearch alternative written in Go using Bluge as the indexing library. It has **no distributed/cluster mode** — it is a single-node system. The GitHub discussion from 2021 confirms clustering was planned but not delivered; the project has since pivoted to OpenObserve (formerly Zinc).

**Lesson for Miroir**: ZincSearch is not a model. Mentioned here for completeness.

---

## 5. Redis Search (RediSearch) in Cluster Mode

### Hash Slot Distribution

Redis Cluster partitions the keyspace into **16,384 hash slots** using CRC16. Each primary shard owns a contiguous range of slots. Slot assignment is stored in the cluster topology map, propagated via gossip protocol. Documents (Redis keys) land in a slot deterministically; the cluster topology map tells each node which slot ranges it owns.

### Distributed Search in Redis Cluster

RediSearch in cluster mode requires the **Redis Enterprise** version for cross-shard search coordination. OSS Redis Cluster does not support cross-slot multi-key operations transparently for search queries.

The coordinator pattern: a single node receives the search request, fans out to every shard, collects top-K results per shard, merges by score, and returns the global top-K. Each shard computes scores locally using its local index statistics (same IDF problem as Elasticsearch).

### Hash Tags

Redis supports `{tag}` in key names to force co-location of related keys in the same slot. This is used to group documents that must be queried together (e.g., for multi-key operations). The Miroir equivalent: ensuring that documents belonging to a single logical index land in predictable, co-located positions.

### Gossip Protocol for Topology

Nodes exchange cluster state via gossip (a subset of nodes pinged every 100ms). Failure detection uses a two-step process: `PFAIL` (suspected failure by one node) → `FAIL` (quorum confirms failure). Slot reassignment after failure is manual or handled by Redis Sentinel/Cluster auto-failover.

---

## 6. Cross-Cutting Lessons for Miroir

### Scatter-Gather Implementation

All systems implement some variation of:
1. Fan-out query to all shards (or all relevant shards, filtered by metadata).
2. Each shard returns a local top-K (IDs + scores).
3. Coordinator merges top-K lists into global top-K.
4. (Optional) Second round-trip to fetch full document bodies for the final result set.

For Meilisearch CE, the federated multi-search API (`/multi-search` with `federation`) handles step 3 internally when queries target the same node. For Miroir, the orchestration layer must implement step 3 across nodes.

Meilisearch's `federationOptions.weight` allows score weighting per sub-query. Miroir can use this to normalize scores from shards with different document counts.

### Result Merging

Two main strategies:

**Score-based merging**: Collect (doc_id, score) from each shard, sort globally by score, take top-K. Requires scores to be on a comparable scale across shards. Problem: local IDF/statistics cause score inflation on small shards.

**Rank-based merging (Reciprocal Rank Fusion)**: Each shard returns a ranked list. RRF score: `1 / (k + rank)` summed across shards for each document. Immune to score scale differences. Works well when scores are not globally normalized. Used by OpenSearch hybrid search. Recommended for Miroir because Meilisearch's ranking is opaque and shard-local.

### Async Task Coordination

For index operations (document ingestion, settings changes) that must be applied across N shards:
- ES: synchronous writes with replication factor; tasks API for long ops.
- Quickwit: control plane maintains desired vs. actual state; periodic reconciliation loop.

Miroir needs a task tracking mechanism for write operations: every write fanned out to N shards should return a global task ID; the orchestrator polls each shard's task status and aggregates into a single status for the caller.

Meilisearch already returns a `taskUid` per operation. Miroir's task layer: fan out → collect N `taskUid` values → persist as a composite task → poll each shard → aggregate `status` (succeeded when all succeeded, failed if any failed).

### Topology Change Handling

| System | Node Add | Node Remove | Data Movement |
|--------|----------|-------------|---------------|
| Elasticsearch | Auto rebalance (shard migration) | Manual drain or auto | Yes — shard copies physically moved |
| Typesense | Raft replication to new node | Quorum maintained | Yes — full dataset replicated |
| Quickwit | Hash rebalance, cache misses | Hash rebalance, cache misses | No — data stays in object storage |
| Redis Cluster | Manual slot migration | Manual slot migration | Yes — slot key migration |

For Miroir, document distribution is determined at write time by hashing the document primary key. Adding a node changes the hash space, requiring **rebalancing**: documents on over-assigned nodes must be re-indexed to under-assigned nodes. This is the most operationally complex part of any distributed search system.

Mitigation strategies:
- **Virtual shards** (more logical shards than nodes): redistribute virtual shards to new nodes without moving individual documents; only shard-level granularity moves.
- **Rendezvous hashing**: minimizes documents reassigned when a node is added/removed; only 1/N fraction of keys reassign.
- **Read-repair / background rebalance**: keep old placement serving reads while new placement is populated; cut over after new shard is fully indexed.

### Coordinating Node Role

All systems designate one node per query as coordinator (Elasticsearch calls it the coordinating node; Quickwit calls it the root node). Any node can play this role. The coordinator:
- Knows the topology (routing table or metastore query).
- Fans out the query.
- Merges results.
- Returns the response.

For Miroir, the orchestration layer itself is the coordinator — it sits in front of all Meilisearch nodes and implements the scatter-gather loop.
