# Miroir — Implementation Plan

**Multi-node Index Replication Orchestrator, Integrated Rebalancing**

Miroir is a RAID-like sharding and high-availability layer for Meilisearch Community Edition. It stripes a large index across a fleet of Meilisearch nodes, fans out search queries across all shards, merges ranked results, and rebalances shard assignments when nodes are added or removed — all without Meilisearch Enterprise.

---

## Table of Contents

1. [Design Principles](#1-design-principles)
2. [Architecture](#2-architecture)
3. [Document, Index, and Search Lifecycle](#3-document-index-and-search-lifecycle)
4. [Implementation](#4-implementation)
5. [API Surface and Compatibility](#5-api-surface-and-compatibility)
6. [Deployment](#6-deployment)
7. [CI/CD](#7-cicd)
8. [Testing](#8-testing)
9. [Secrets Handling](#9-secrets-handling)
10. [Observability](#10-observability)
11. [Onboarding](#11-onboarding)
12. [Delivered Artifacts](#12-delivered-artifacts)
13. [Advanced Capabilities](#13-advanced-capabilities)
14. [Resource Envelope and Horizontal Scaling](#14-resource-envelope-and-horizontal-scaling)
15. [Open Problems](#15-open-problems)

---

## 1. Design Principles

1. **Invisible federation** — clients talk to one endpoint using the standard Meilisearch API. The sharding topology is entirely hidden. No SDK changes, no query changes, no schema changes.
2. **No Enterprise dependency** — all functionality is built on Meilisearch Community Edition (MIT licensed).
3. **Rendezvous hashing** — same algorithm Meilisearch Enterprise uses internally; minimal reshuffling on topology change, natural RF > 1 support.
4. **RF-configurable redundancy** — RF=1 for maximum capacity, RF=2 for one-node-loss tolerance, RF=3 for two-node-loss tolerance.
5. **Graceful degradation** — when a shard is unavailable, return partial results with a clear header rather than failing the entire request.
6. **Static binaries, scratch images** — musl compilation + scratch Docker base for minimal attack surface and trivial deployment.
7. **GitOps first** — all deployment configuration committed to `jedarden/declarative-config`; ArgoCD drives all cluster changes.
8. **Fixed per-pod resource envelope** — each Miroir orchestrator pod fits within **2 vCPU / 3.75 GB RAM**. When aggregate workload exceeds this envelope, scale **horizontally** by adding pods, never vertically beyond the envelope. The request path is strictly stateless; background work partitions across pods via shard-partitioned ownership, leader election, or a shared job queue. See Section 14.

### Key constraints

- **Logical shard count (S) is fixed at index creation** — this is the granularity of the hash space, not the number of physical machines. It determines how finely the keyspace is divided. Changing S requires a full reindex. Choose generously, per group: `S = max_nodes_per_group_ever × 8`. S is scoped to a single replica group because rendezvous assignment runs within each group independently (see Section 2); adding groups multiplies throughput without consuming additional S headroom.
- **Node count (N) is fully elastic** — nodes can be added or removed at any time without a full reindex. Adding a node to a group migrates only the `~1/(Ng+1)` fraction of that group's documents whose shard assignments change, where Ng is the number of nodes in the affected group. Documents in other groups are untouched. Removing a node migrates only that node's documents to surviving nodes within its own group. The rest of the corpus is untouched.
- All Meilisearch nodes must have **identical index settings** at all times. Miroir enforces this by broadcasting settings atomically.
- Documents **must** have an explicit primary key declared at index creation. Miroir cannot shard-route without it.

---

## 2. Architecture

### Core model

Miroir treats a fleet of Meilisearch CE instances the way software RAID treats a disk array, with an additional dimension for read throughput: **replica groups**.

- The logical index is divided into **S shards** — a fixed hash-space granularity chosen at index creation
- The node fleet is divided into **RG replica groups** — independent pools each holding a complete copy of all shards
- Within each group, each shard is held by **RF nodes** (intra-group replication factor, for HA within the group)
- Every write fans out to all `RG × RF` target nodes (one per shard per group replica)
- Every search query is routed to exactly **one replica group** — the query fans out only to that group's covering set, not the entire fleet
- Read throughput scales with RG: doubling the groups doubles query capacity
- Storage capacity scales with total nodes: adding a node to a group reduces each node's share within that group
- The **node fleet is elastic**: nodes can be added to existing groups (capacity scaling) or new groups can be added (throughput scaling), both without a full reindex
- When the fleet topology changes, the orchestrator rebalances only the affected shards within the affected group

```
Client
  │
  ▼
Miroir Orchestrator
  ├── Write path: hash(doc_id) → shard → fan out to RF nodes in EACH group (RG × RF total)
  ├── Read path:  pick one group → scatter to that group's covering set → merge results
  └── Rebalance: on node add/remove within a group → migrate minimum shards within that group

  Replica Group 0              Replica Group 1
  ┌──────────────────┐        ┌──────────────────┐
  │ node-0  node-1   │        │ node-3  node-4   │     ...  (RG groups)
  │ node-2           │        │ node-5           │
  └──────────────────┘        └──────────────────┘
  (each group holds all S shards, distributed across its nodes)
```

### Replication factor

| RF | Redundancy | Node failures tolerated | Capacity |
|----|-----------|------------------------|----------|
| 1  | None (stripe only) | 0 | 100% of fleet |
| 2  | One replica | 1 per shard group | 50% of fleet |
| 3  | Two replicas | 2 per shard group | 33% of fleet |

### Shard routing — Rendezvous hashing (HRW) within groups

Miroir uses **Rendezvous hashing (HRW)** to assign shards to nodes. The assignment is scoped **within each replica group** — the rendezvous hash picks the top-RF nodes from a group's node list, not the global node list. This guarantees that every group independently forms a complete covering set with no cross-group shard overlap.

```
nodes_in_group(g)           → the subset of nodes belonging to replica group g
assign_shard(shard_id, g)   → top RF nodes from nodes_in_group(g)
                              by score(hash(shard_id || node_id))

write_targets(shard_id)     → [assign_shard(shard_id, g) for g in 0..RG]
                              (flat union across all groups — RG × RF nodes total)

search_group(query_seq)     → query_seq % RG   (round-robin across groups)
search_nodes(query)         → covering_set within search_group(query)
```

Why group-scoped assignment matters: if rendezvous ran over the global node list, both replicas of a shard might land on nodes in the same group, leaving the other group without coverage of that shard. Scoping to the group prevents this.

Properties:
- On node addition within a group: only `~1/(Ng+1)` of that group's documents migrate, where Ng = nodes per group
- On node removal within a group: only `~RF/Ng` of that group's documents migrate
- Both operations leave other groups and all unaffected shards completely untouched
- No virtual nodes needed — rendezvous is naturally uniform
- Deterministic — any component can compute assignments from the same node list; no coordination required

The hash function is **twox-hash (xxhash family)** — the same function Meilisearch Enterprise uses for document sharding.

### Write path

1. Client sends `POST /indexes/{index}/documents` with a batch of documents
2. Orchestrator extracts the document primary key for each document
3. For each document: `shard_id = hash(doc_id) % S`
4. **Inject `_miroir_shard`**: add `"_miroir_shard": shard_id` to each document before forwarding. This field is stored on the node and used during rebalancing to retrieve documents by shard without a full scan. It is stripped from all API responses before returning to clients.
5. For each replica group g in `0..RG`: compute `assign_shard(shard_id, g)` → the RF nodes within that group that own this shard
6. Fan out: send each document to all `RG × RF` target nodes in parallel (grouped into per-node batches)
7. Apply the two-rule quorum described below; set `X-Miroir-Degraded` on any group that missed its per-group quorum
8. Return Miroir task ID to client

**Write quorum — two rules.**

1. **Per-group quorum.** A group is successful for a shard when at least `floor(RF/2) + 1` of its RF target nodes ACK enqueue. (RF=1 → 1 ACK; RF=2 → 2 ACKs; RF=3 → 2 ACKs; RF=4 → 3 ACKs.)
2. **Write success.** The write returns HTTP 200 if **at least one group** met its per-group quorum for the shard. The response carries `X-Miroir-Degraded` whenever **any** group did not meet its quorum for any shard in the batch (listing the affected groups and shards). HTTP 503 with `miroir_no_quorum` is returned only when **no group** met its per-group quorum for a given shard.

Examples:

| Topology | Per-group quorum | Write succeeds when |
|----------|------------------|---------------------|
| RG=1, RF=1 | 1 of 1 | that one node ACKs |
| RG=1, RF=2 | 2 of 2 | both replicas ACK |
| RG=1, RF=3 | 2 of 3 | any 2 of the 3 replicas ACK |
| RG=2, RF=1 | 1 of 1 per group | at least one of the two groups' single node ACKs |
| RG=2, RF=3 | 2 of 3 per group | either group reaches 2-of-3 ACKs |

Documents whose replicas did not ACK are not lost: any group that accepted the write becomes the authoritative source, and the anti-entropy reconciler (§13.8) subsequently repairs any replica that missed the write on the next pass. This composition — best-effort fan-out, per-shard success when at least one group reaches quorum, plus continuous reconciliation — is the long-term safety net and the concrete expression of Design Principle 5 (graceful degradation) for the write path. This not-lost guarantee depends on `anti_entropy.enabled: true` (§13.8, default on). When anti-entropy is disabled, writes that don't reach every replica remain permanently divergent; operators running with anti-entropy off must either require full-replica-set writes (e.g., stricter client retries) or provide their own reconciliation.

**`_miroir_shard` is a reserved field name**: clients must not use a field with this name. Miroir rejects any document that already contains `_miroir_shard` with a `miroir_reserved_field` error.

**Index settings and schema changes**: broadcast to all nodes in all groups. Index creation additionally broadcasts a settings update to add `_miroir_shard` to `filterableAttributes` on every node — this is required for efficient rebalancing.

### Read path

1. Client sends `POST /indexes/{index}/search`
2. Orchestrator selects a **replica group** for this query: `group = query_sequence_number % RG` (round-robin ensures even load across groups). The `query_sequence_number` is a per-pod counter, not a cluster-wide one — under the multi-pod deployment of §14, cluster-wide balance across groups therefore depends on the Kubernetes Service load-balancer distributing incoming requests uniformly across Miroir pods (which it does for the default random / round-robin kube-proxy policies). See §14.4.
3. Within the selected group, build a **covering set**: one node per shard (round-robin across intra-group replicas for load balancing within the group)
4. Fan out the query to each node in the covering set in parallel, with `showRankingScore: true` appended
5. Each node executes a full Meilisearch search against its local document subset
6. **Result merge**: collect all hits, sort globally by `_rankingScore` descending, apply original `offset`/`limit`, strip `_rankingScore` if client did not request it, strip `_miroir_shard` always
7. **Facets**: sum per-value counts across the covering set nodes
8. **estimatedTotalHits**: sum across covering set nodes
9. **processingTimeMs**: report max across covering set nodes

The query only touches `N/RG` nodes (the nodes in the selected group), not the entire fleet. Adding more groups reduces the fan-out size per query and increases the number of queries that can run in parallel across the fleet.

**Score comparability**: `_rankingScore` values are comparable across shards only if all nodes have identical ranking rules, typo tolerance, attribute weights, and synonyms. Miroir enforces this via the settings broadcast requirement. Because all groups receive identical settings changes, inter-group score comparability is also maintained (relevant for degraded-mode fallback, below).

**Group unavailability fallback**: if the selected group has an unhealthy node and the shard's intra-group RF cannot cover it, Miroir can optionally fall back to a different group for that query. The result is still complete and correct — it just came from a different group. This is transparent to the client.

**offset/limit**: each node must return up to `offset + limit` results. The orchestrator applies the real offset/limit after global merge.

### Replica groups in practice

Replica groups make read throughput a first-class scaling dimension, independent of storage capacity.

```
Example: S=6 shards, RG=2 groups, RF=1 per group, 6 nodes total

Group 0: nodes [0, 1, 2]          Group 1: nodes [3, 4, 5]
  shard 0 → node 0                  shard 0 → node 3
  shard 1 → node 1                  shard 1 → node 4
  shard 2 → node 2                  shard 2 → node 5
  shard 3 → node 0                  shard 3 → node 3
  shard 4 → node 1                  shard 4 → node 4
  shard 5 → node 2                  shard 5 → node 5

Write for doc-X (shard 2): → node-2 (group 0) AND node-5 (group 1)  [2 writes]
Query 1: → group 0 → nodes [0, 1, 2]   [3 node fan-out]
Query 2: → group 1 → nodes [3, 4, 5]   [3 node fan-out]
Query 3: → group 0 → nodes [0, 1, 2]   [queries 1 and 3 run fully in parallel]
```

With RG=2, two queries can execute simultaneously with zero resource contention. Adding a third group triples throughput, and so on.

**Tradeoffs table**

| RG | RF | Copies/doc | Read throughput | Storage per node | HA (within group) |
|----|-----|-----------|----------------|-----------------|------------------|
| 1 | 1 | 1 | 1× | full | none |
| 1 | 2 | 2 | 1× (replicas share load) | half | 1 node failure |
| 1 | 3 | 3 | 1× | third | 2 node failures |
| 2 | 1 | 2 | **2×** | half per group | none |
| 2 | 2 | 4 | **2×** | quarter per group | 1 node failure per group |
| 3 | 1 | 3 | **3×** | third per group | none |
| RG | RF | RG×RF | **RG×** | 1/(RG×RF) | RF−1 per group |

`RF=1, RG=2` and `RF=2, RG=1` both store 2 copies of each document but behave differently:
- `RF=2, RG=1`: both copies in one group; each query uses that group (one query at a time per shard); tolerates 1 node failure within the group
- `RF=1, RG=2`: one copy in each group; queries alternate between groups (two fully parallel query streams); no intra-group HA

Choose RG for throughput, RF for HA. They compose independently.

**When to add a group vs. add a node to an existing group:**

- Add a node to an existing group when the bottleneck is **per-node storage or indexing CPU** — the rebalancer distributes shards more finely within the group, each node holds fewer documents
- Add a new group when the bottleneck is **search query throughput** — the new group handles a separate stream of queries independently; existing groups are unaffected

### Topology changes

All topology operations are scoped to a single replica group. Changes to one group do not affect other groups' data or query routing.

**Adding a node to an existing group (capacity scaling; movement: ~1/(Ng+1) of that group's documents):**
1. Assign the new node to a group (config `replica_group` field); mark `joining`
2. Recompute shard assignments within that group — approximately `S/(Ng+1)` shards now prefer the new node
3. Dual-write: new inbound writes for affected shards in this group go to both the old owner and the new node
4. Background migration: for each affected shard, use `GET /indexes/{uid}/documents?filter=_miroir_shard={id}&limit=1000&offset=...` to page through only that shard's documents on the source node; write each page to the new node
5. Once all pages are transferred, mark node `active`; stop dual-write; delete migrated shard from old node with `filter=_miroir_shard={id}`

Documents on unaffected shards, and all documents in other groups, are never touched.

**Adding a new replica group (throughput scaling; no migration of existing groups):**
1. Provision new nodes and assign them to a new group ID in config
2. Mark the new group `initializing` — queries are not yet routed to it
3. Background sync: for each shard, copy all documents from any healthy existing group to the new group's nodes using `filter=_miroir_shard={id}` pagination; new inbound writes fan out to the new group immediately
4. Once all shards are fully synced, mark the group `active` — queries begin routing to it in round-robin
5. Existing groups continue serving queries throughout; there is no read interruption

**Removing a node from a group (movement: ~RF/Ng of that group's documents):**
1. Mark node `draining`; stop routing writes for its shards to it
2. Recompute assignments within the group — affected shards reassigned to surviving nodes
3. Background migration: copy affected shards to new owners using `filter=_miroir_shard={id}`
4. Once complete, mark `removed`; operator deletes the pod and PVC

**Removing a replica group (decommission a query pool):**
1. Mark the group `draining` — queries stop routing to it immediately
2. The group's nodes can be decommissioned; no data migration needed (other groups hold the documents)
3. Remove nodes from config; operator deletes pods and PVCs

**Node failure (unplanned):**
1. Health check detects failure; mark `failed`, stop routing writes to it
2. If RF > 1 within the group: surviving replicas serve reads for affected shards — no migration needed immediately
3. For reads: if the failed node's shards have no intra-group replica, fall back to a healthy group for those shards
4. Schedule background replication to restore RF within the group; degrade to cross-group fallback until restored

### K8s deployment model

```
                    ┌─────────────────────┐
  Client ──────────►│  Miroir Orchestrator │  Deployment, replicas: 2+ via HPA (see §14.4)
                    │  (proxy + router)    │
                    └──────┬──────────────┘
          writes to both ──┴── reads from one group per query
             │                          │
     ┌───────┴──────┐          ┌────────┴──────┐
     │  Group 0     │          │  Group 1      │
     │  (query pool)│          │  (query pool) │
     ▼      ▼      ▼          ▼      ▼       ▼
  meili-0 meili-1 meili-2  meili-3 meili-4 meili-5   StatefulSet
  RWO PVC RWO PVC RWO PVC  RWO PVC RWO PVC RWO PVC   one PVC per pod
```

- Each Meilisearch node is a pod in a StatefulSet with its own RWO PVC
- All nodes in the same group together cover all shards; each query goes to one group only
- Orchestrator is stateless — routing derived deterministically from config; can run 1-2 replicas
- Redis required for shared task state when running 2 Miroir replicas

### Three independent scaling dimensions

Miroir has three knobs that interact but are independently adjustable:

| Parameter | What it controls | Fixed? | Change cost |
|-----------|-----------------|--------|-------------|
| S (shards) | hash-space granularity; sets the maximum useful node count per group | **Fixed at index creation** | Full reindex to change |
| RG (replica_groups) | number of independent query pools; search throughput multiplier | Elastic | Add group: background sync to new nodes; remove group: instant |
| RF (replication_factor) | intra-group copies per shard; HA within a group | Elastic | Add replica: migrate shard copies to new nodes; remove: delete from departed node |

**Choosing S:** base it on the maximum number of nodes you ever expect in a single group, not total nodes across all groups. Groups are independent — each group has `S / Ng` shards per node (where Ng = nodes in the group). The global node count is `RG × Ng`.

| Max nodes per group ever | Recommended S |
|-------------------------|---------------|
| 2–4 | 16 |
| 4–8 | 32 |
| 8–16 | 64 |
| 16–32 | 128 |

Rule of thumb: `S = max_nodes_per_group_ever × 8`. A fleet that starts at 2 nodes per group and grows to 60 per group needs S ≥ 480 (use 512). You can always add more groups (and multiply throughput) without any S constraint — groups are independent.

The only operation that requires a full reindex is increasing S. Changing RG or N (adding/removing groups or nodes) never requires a full reindex.

---

## 3. Document, Index, and Search Lifecycle

### Primary key requirement

Miroir routes every document via `hash(primary_key) % S`. The primary key must be known at ingest time. Miroir requires an explicit primary key on every index and rejects any document batch without a resolvable primary key before touching any node.

### Index lifecycle

**Create:** Broadcast to all N nodes. Every node creates an empty index with the same UID and settings. All nodes must be reachable; partial creation is rolled back.

**Settings changes:** Broadcast atomically. _(Superseded by two-phase broadcast — see §13.5. The sequential flow below remains available as `settings_broadcast.strategy: sequential` for legacy compatibility; `two_phase` is the default.)_ Applied sequentially with rollback on partial failure:
1. Apply to node-0, verify
2. Apply to node-1, verify
3. Continue through fleet
4. If any node fails: revert all previously applied nodes

A partial settings apply produces non-uniform ranking across shards and corrupts merged results. This is the highest-risk operation in the lifecycle — the driving motivation for the two-phase broadcast in §13.5.

**Delete:** Broadcast to all nodes.

**Stats:** `GET /indexes/{uid}/stats` fans out to all nodes; sum `numberOfDocuments` and merge `fieldDistribution`.

### Document lifecycle

**Ingest (add/replace):**

Per document:
1. Extract primary key value
2. `shard_id = hash(primary_key) % S`
3. Nodes = rendezvous top-RF for `shard_id`
4. Inject `"_miroir_shard": shard_id` into the document
5. Assign document to those nodes

Per batch — group documents by target node set, one batched request per node:
```
Original: [doc-A (shard 3), doc-B (shard 7), doc-C (shard 3), doc-D (shard 11)]

shard 3 → nodes [0,2]:  node-0 gets [doc-A,doc-C], node-2 gets [doc-A,doc-C]
shard 7 → nodes [1,2]:  node-1 gets [doc-B],        node-2 gets [doc-B]
shard 11 → nodes [0,1]: node-0 gets [doc-D],        node-1 gets [doc-D]

node-0 request: [doc-A, doc-C, doc-D]
node-1 request: [doc-B, doc-D]
node-2 request: [doc-A, doc-C, doc-B]
```

All node requests issued in parallel.

**Partial update:** Route identically to ingest — hash the primary key, send to same RF nodes.

**Delete by ID:** Hash primary key → find shard → send delete to all RF replicas.

**Delete by filter:** Broadcast to all N nodes (cannot be shard-routed).

**Get by ID:** Hash primary key → read from any one replica (round-robin).

### Task ID reconciliation

Every Meilisearch write returns a `taskUid`. Nodes have independent sequences. Miroir presents a unified task namespace:

```
Client write
  → fan out to K nodes
  → each node returns local taskUid (node-0: 42, node-1: 17, node-2: 88)
  → orchestrator generates Miroir task ID (e.g., mtask-00391)
  → stores mapping: mtask-00391 → {node-0: 42, node-1: 17, node-2: 88}
  → returns mtask-00391 to client

GET /tasks/mtask-00391
  → poll all mapped node tasks
  → "processing" if any node still processing
  → "succeeded" only when all nodes report "succeeded"
  → "failed" if any node failed, includes error detail
```

Task mappings are persisted durably (SQLite or Redis) to survive Miroir restarts.

### Partial availability during search

**`unavailable_shard_policy: partial`** (default): If a shard is completely unavailable, return best results from available shards with response header `X-Miroir-Degraded: shards=3,7,11`.

**`unavailable_shard_policy: error`**: Return HTTP 503 with JSON error body indicating unavailable shards.

---

## 4. Implementation

### Language and runtime

**Rust.** Rationale:
- Zero-cost async scatter-gather (Tokio task fan-out maps naturally to fan-out patterns)
- `twox-hash` crate provides the same hash family Meilisearch Enterprise uses — identical results provable
- Static musl binaries simplify Docker image production (scratch base, no libc)
- Existing operator toolchain (forge, NEEDLE, ARMOR, CLASP, SIGIL) is Rust — no new CI infrastructure

### Crate layout

```
miroir/
├── Cargo.toml                  (workspace)
├── crates/
│   ├── miroir-core/            (library: routing, merging, topology)
│   │   └── src/
│   │       ├── router.rs       (rendezvous hash, shard assignment)
│   │       ├── topology.rs     (node registry, health state machine)
│   │       ├── scatter.rs      (fan-out logic, covering set builder)
│   │       ├── merger.rs       (result merging, facet aggregation)
│   │       ├── task.rs         (task registry, ID reconciliation)
│   │       └── config.rs       (Config struct, validation)
│   ├── miroir-proxy/           (binary: HTTP proxy server)
│   │   └── src/
│   │       ├── routes/
│   │       │   ├── documents.rs
│   │       │   ├── search.rs
│   │       │   ├── indexes.rs
│   │       │   ├── settings.rs
│   │       │   ├── tasks.rs
│   │       │   ├── health.rs
│   │       │   └── admin.rs    (/_miroir/ management API)
│   │       ├── auth.rs
│   │       └── middleware.rs   (logging, tracing, metrics)
│   └── miroir-ctl/             (binary: CLI for topology management)
│       └── src/commands/
│           ├── status.rs
│           ├── node.rs
│           ├── rebalance.rs
│           ├── reshard.rs      (§13.1)
│           ├── verify.rs
│           ├── task.rs         (§11)
│           ├── dump.rs         (§13.9)
│           ├── alias.rs        (§13.7)
│           ├── canary.rs       (§13.18)
│           ├── ttl.rs          (§13.14)
│           ├── cdc.rs          (§13.13)
│           ├── shadow.rs       (§13.16)
│           ├── ui.rs           (§13.21 search UI config, §13.19 admin UI auth helpers)
│           ├── tenant.rs       (§13.15)
│           └── explain.rs      (§13.20)
```

### Key dependencies

| Crate | Purpose |
|-------|---------|
| `axum` | HTTP server framework |
| `tokio` | Async runtime (multi-threaded) |
| `reqwest` | HTTP client for node forwarding |
| `twox-hash` | xxhash (same as Meilisearch EE) |
| `serde` + `serde_json` | Serialization |
| `config` | YAML/TOML/env layered configuration |
| `rusqlite` | Task registry (embedded SQLite) |
| `prometheus` | Metrics exposition |
| `tracing` + `tracing-subscriber` | Structured logging and spans |
| `clap` | CLI argument parsing (`miroir-ctl`) |
| `uuid` | Miroir task ID generation |

### Rendezvous hash implementation

```rust
// miroir-core/src/router.rs
use twox_hash::XxHash64;
use std::hash::{Hash, Hasher};

pub fn score(shard_id: u32, node_id: &str) -> u64 {
    let mut h = XxHash64::with_seed(0);
    shard_id.hash(&mut h);
    node_id.hash(&mut h);
    h.finish()
}

/// Assign a shard to `rf` nodes within a single replica group.
/// `group_nodes` is the subset of nodes belonging to that group.
pub fn assign_shard_in_group(shard_id: u32, group_nodes: &[NodeId], rf: usize) -> Vec<NodeId> {
    let mut scored: Vec<(u64, &NodeId)> = group_nodes
        .iter()
        .map(|n| (score(shard_id, n.as_str()), n))
        .collect();
    scored.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    scored.into_iter().take(rf).map(|(_, n)| n.clone()).collect()
}

/// All write targets for a document: the RF nodes in EACH replica group.
pub fn write_targets(shard_id: u32, topology: &Topology) -> Vec<NodeId> {
    topology.groups().flat_map(|group| {
        assign_shard_in_group(shard_id, group.nodes(), topology.rf)
    }).collect()
}

/// Select the replica group for a query (round-robin by query counter).
pub fn query_group(query_seq: u64, replica_groups: u32) -> u32 {
    (query_seq % replica_groups as u64) as u32
}

/// The covering set for a search: one node per shard within the chosen group.
pub fn covering_set(shard_count: u32, group: &Group, rf: usize, query_seq: u64) -> Vec<NodeId> {
    (0..shard_count).map(|shard_id| {
        let replicas = assign_shard_in_group(shard_id, group.nodes(), rf);
        // rotate through replicas for intra-group load balancing
        replicas[(query_seq as usize) % replicas.len()].clone()
    }).collect::<std::collections::HashSet<_>>().into_iter().collect()
    // deduplicated: a node covering multiple shards appears once, searches all its shards
}

pub fn shard_for_key(primary_key: &str, shard_count: u32) -> u32 {
    let mut h = XxHash64::with_seed(0);
    primary_key.hash(&mut h);
    (h.finish() % shard_count as u64) as u32
}
```

All Miroir instances with the same topology (node list, group assignments, RF, S) independently compute identical write targets and covering sets. No coordination required — the routing table is purely deterministic.

### Result merger

The merger assumes `_rankingScore` values from different shards are comparable. This holds when all nodes have identical index settings — enforced by the settings broadcast.

```rust
// Pseudocode for merge()
fn merge(shards, offset, limit, client_requested_score) -> MergedSearchResult {
    // 1. Collect all hits with scores
    let mut all_hits: Vec<(f64, Hit)> = shards.flat_map(|s| s.hits_with_scores()).collect();
    // 2. Sort globally descending by score
    all_hits.sort_by(|a, b| b.0.partial_cmp(&a.0));
    // 3. Apply offset + limit
    let page = all_hits.skip(offset).take(limit);
    // 4. Strip _rankingScore if client did not request it
    if !client_requested_score { page.strip_ranking_scores() }
    // 5. Aggregate facets (sum per-value counts) and estimated_total_hits (sum)
    MergedSearchResult { hits: page, facets: merge_facets(shards), total: sum_totals(shards), ... }
}
```

### Task store schema (SQLite)

The task store persists all orchestrator state that must survive a pod restart. In single-replica deployments the backend is SQLite; in HA mode Redis replaces it (keyspace mapping below). Every table below is defined here and cross-referenced from the §13 / §14.5 section that consumes it.

**1. `tasks` — Miroir task registry (defined here; consumed throughout §2, §3, §13).**

```sql
CREATE TABLE tasks (
    miroir_id   TEXT PRIMARY KEY,
    created_at  INTEGER NOT NULL,
    status      TEXT NOT NULL,   -- enqueued | processing | succeeded | failed | canceled
    node_tasks  TEXT NOT NULL,   -- JSON: {"node-0": 42, "node-1": 17}
    error       TEXT
);
```

Background Tokio tasks poll node task statuses and update rows. Old tasks are pruned after a TTL (default 7 days) by the rendezvous-partitioned pruner of §14.5 Mode A.

**2. `node_settings_version` — per-(index, node) settings freshness (consumed by §13.5).**

```sql
CREATE TABLE node_settings_version (
    index_uid   TEXT NOT NULL,
    node_id     TEXT NOT NULL,
    version     INTEGER NOT NULL,   -- last cluster-wide settings_version this (index, node) pair verified
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (index_uid, node_id)
);
```

Advanced whenever a two-phase verify (§13.5 Phase 2) or drift-repair cycle succeeds for that pair. Covering-set construction for reads carrying `X-Miroir-Min-Settings-Version` excludes rows where `version < floor`.

**3. `aliases` — atomic index aliases (consumed by §13.7, extended for multi-target by §13.17).**

```sql
CREATE TABLE aliases (
    name          TEXT PRIMARY KEY,
    kind          TEXT NOT NULL,          -- 'single' | 'multi'
    current_uid   TEXT,                   -- non-null when kind='single'
    target_uids   TEXT,                   -- JSON array of UIDs; non-null when kind='multi'
    version       INTEGER NOT NULL,       -- monotonic flip counter
    created_at    INTEGER NOT NULL,
    history       TEXT NOT NULL           -- JSON array: last N prior states, bounded by aliases.history_retention
);
```

Two alias types are supported:
- **Single-target alias (`kind='single'`)** — one `current_uid`; client writes and reads both resolve to that UID; atomic flip via `PUT /_miroir/aliases/{name}`.
- **Multi-target alias (`kind='multi'`)** — `target_uids` is a JSON array of concrete UIDs; reads fan out across all of them via §13.11 multi-search and merge by `_rankingScore`; writes are rejected with `miroir_multi_alias_not_writable` (see §13.7). Multi-target aliases are managed exclusively by §13.17 ILM; direct operator edits are refused.

Alias flips update `current_uid` (single) or `target_uids` (multi) and `version` atomically; `history` is append-only within the retention bound.

**4. `sessions` — read-your-writes session pins (consumed by §13.6).**

```sql
CREATE TABLE sessions (
    session_id            TEXT PRIMARY KEY,
    last_write_mtask_id   TEXT,           -- nullable: session may exist before any write
    last_write_at         INTEGER,
    pinned_group          INTEGER,        -- group_id that first reached per-group quorum; nullable when pin cleared
    min_settings_version  INTEGER NOT NULL,
    ttl                   INTEGER NOT NULL -- expiry timestamp (ms since epoch); default 15m from last use
);
```

LRU-bounded by `session_pinning.max_sessions`. Rows past `ttl` are evicted lazily on access.

**5. `idempotency_cache` — write deduplication (consumed by §13.10).**

```sql
CREATE TABLE idempotency_cache (
    key              TEXT PRIMARY KEY,
    body_sha256      BLOB NOT NULL,
    miroir_task_id   TEXT NOT NULL,
    expires_at       INTEGER NOT NULL
);
```

TTL defaults to 24h (`idempotency.ttl_seconds`). LRU-bounded by `idempotency.max_cached_keys`.

**6. `jobs` — work-queued background jobs (consumed by §14.5 Mode C).**

```sql
CREATE TABLE jobs (
    id                 TEXT PRIMARY KEY,
    type               TEXT NOT NULL,   -- dump_import | reshard_backfill | ...
    params             TEXT NOT NULL,   -- JSON
    state              TEXT NOT NULL,   -- queued | in_progress | completed | failed
    claimed_by         TEXT,            -- pod_id of current claimant; nullable when queued
    claim_expires_at   INTEGER,         -- lease heartbeat expiry
    progress           TEXT NOT NULL    -- JSON: { bytes_processed, docs_routed, last_cursor, ... }
);
```

Claim semantics and chunking rules are §14.5 Mode C.

**7. `leader_lease` — singleton-coordinator lease (consumed by §14.5 Mode B).**

```sql
CREATE TABLE leader_lease (
    scope        TEXT PRIMARY KEY,   -- e.g. "reshard:<index>", "alias_flip:<name>", "settings_broadcast:<index>"
    holder       TEXT NOT NULL,      -- pod_id of current leader
    expires_at   INTEGER NOT NULL    -- renewed every 3s with a 10s TTL
);
```

For SQLite deployments a single-replica advisory lock substitutes; the row is still written so recovery can read the last committed phase state after leader loss.

**8. `canaries` — canary definitions (consumed by §13.18).**

```sql
CREATE TABLE canaries (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    index_uid      TEXT NOT NULL,
    interval_s     INTEGER NOT NULL,
    query_json     TEXT NOT NULL,     -- JSON: the canary query body
    assertions_json TEXT NOT NULL,    -- JSON: array of assertion specs
    enabled        INTEGER NOT NULL,  -- 0 | 1
    created_at     INTEGER NOT NULL
);
```

Created and modified via `POST /_miroir/canaries`. The canary runner (Mode A, §14.5) reads this table on each scheduling tick.

**9. `canary_runs` — canary run history (consumed by §13.18).**

```sql
CREATE TABLE canary_runs (
    canary_id              TEXT NOT NULL,
    ran_at                 INTEGER NOT NULL,
    status                 TEXT NOT NULL,    -- pass | fail | error
    latency_ms             INTEGER NOT NULL,
    failed_assertions_json TEXT,             -- JSON array or NULL when pass
    PRIMARY KEY (canary_id, ran_at)
);
```

Bounded by `canary_runner.run_history_per_canary` (default 100) — older rows are pruned on insert.

**10. `cdc_cursors` — per-sink per-index CDC cursor (consumed by §13.13).**

```sql
CREATE TABLE cdc_cursors (
    sink_name       TEXT NOT NULL,
    index_uid       TEXT NOT NULL,
    last_event_seq  INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (sink_name, index_uid)
);
```

Advanced when a sink ACKs successful delivery of an event batch. `GET /_miroir/changes?since=…` reads against this same monotonic sequence. Shadow-diff state (§13.16) is explicitly **not** persisted — it lives in the in-memory ring buffer of size `shadow.diff_buffer_size`.

**11. `tenant_map` — API-key → tenant mapping for `tenant_affinity.mode: api_key` (consumed by §13.15).**

```sql
CREATE TABLE tenant_map (
    api_key_hash  BLOB PRIMARY KEY,   -- sha256(api_key)
    tenant_id     TEXT NOT NULL,
    group_id      INTEGER             -- nullable: NULL falls through to hash(tenant_id) % RG
);
```

Loaded lazily on first request per key and cached per-pod. Only consulted when `tenant_affinity.mode: api_key`.

**12. `rollover_policies` — ILM rollover policies (consumed by §13.17).**

```sql
CREATE TABLE rollover_policies (
    name           TEXT PRIMARY KEY,
    write_alias    TEXT NOT NULL,
    read_alias     TEXT NOT NULL,
    pattern        TEXT NOT NULL,      -- e.g. "logs-{YYYY-MM-DD}"
    triggers_json  TEXT NOT NULL,      -- JSON: { max_docs, max_age, max_size_gb }
    retention_json TEXT NOT NULL,      -- JSON: { keep_indexes }
    template_json  TEXT NOT NULL,      -- JSON: { primary_key, settings_ref }
    enabled        INTEGER NOT NULL    -- 0 | 1
);
```

Read by the daily leader-coordinated ILM job (Mode B, §14.5) during each `ilm.check_interval_s` tick.

**13. `search_ui_config` — per-index search-UI configuration (consumed by §13.21).**

```sql
CREATE TABLE search_ui_config (
    index_uid    TEXT PRIMARY KEY,
    config_json  TEXT NOT NULL,        -- JSON: the `search_ui:` entry as described in §13.21
    updated_at   INTEGER NOT NULL
);
```

Written by `POST /_miroir/ui/search/{index}/config`; read by the embedded SPA at page-load time.

**14. `admin_sessions` — Admin UI session registry (consumed by §13.19 login/logout, §4 `/_miroir/admin/*`).**

```sql
CREATE TABLE admin_sessions (
    session_id      TEXT PRIMARY KEY,
    csrf_token      TEXT NOT NULL,
    admin_key_hash  TEXT NOT NULL,    -- sha256 of admin key used at login
    created_at      INTEGER NOT NULL,
    expires_at      INTEGER NOT NULL,
    revoked         INTEGER NOT NULL DEFAULT 0,
    user_agent      TEXT,
    source_ip       TEXT
);
CREATE INDEX admin_sessions_expires ON admin_sessions(expires_at);
```

Written by `POST /_miroir/admin/login`; `revoked` is flipped to `1` by `POST /_miroir/admin/logout`. Every cookie-authenticated request re-checks `revoked` so logout propagates across pods without requiring in-memory invalidation. Rows past `expires_at` are evicted lazily on access and by the rendezvous-partitioned pruner of §14.5 Mode A.

**Redis mode (HA).** For multi-replica Miroir each SQLite table is represented as a Redis keyspace, with the same field semantics:

- `miroir:tasks:<miroir_id>` (hash); list-wide iteration via the set `miroir:tasks:_index`.
- `miroir:node_settings_version:<index>:<node_id>` (hash); set `miroir:node_settings_version:_index`.
- `miroir:aliases:<name>` (hash); set `miroir:aliases:_index` backs "list all" (§13.7 `GET /_miroir/aliases`).
- `miroir:session:<session_id>` (hash) with `EXPIRE` matching `session_pinning.ttl_seconds`.
- `miroir:idemp:<key>` (hash) with `EXPIRE` matching `idempotency.ttl_seconds`.
- `miroir:jobs:<id>` (hash); queue-depth set `miroir:jobs:_queued` feeds the HPA signal of §14.5.
- `miroir:lease:<scope>` (string, `SET NX EX 10` renewed every 3s) per §14.5 Mode B.
- `miroir:canary:<id>` (hash); set `miroir:canary:_index` backs `GET /_miroir/canaries/status`.
- `miroir:canary_runs:<canary_id>` (sorted set keyed by `ran_at`, value = JSON row); `ZREMRANGEBYRANK` trims to `canary_runner.run_history_per_canary`.
- `miroir:cdc_cursor:<sink>:<index>` (string, integer seq).
- `miroir:tenant_map:<sha256_key>` (hash); no secondary index — lookups are always by exact key hash.
- `miroir:rollover:<name>` (hash); set `miroir:rollover:_index` backs the ILM leader's scan.
- `miroir:search_ui_config:<index>` (hash).
- `miroir:search_ui_scoped_key:<index>` (hash, fields `{primary_uid, previous_uid, rotated_at, generation}`); no automatic TTL — state is long-lived until the next rotation. Written by the rotation leader (Mode B, §14.5) and read by every pod on each forwarded SPA request; see §13.21 "Scoped-key rotation coordination".
- `miroir:search_ui_scoped_key_observed:<pod>:<index>` (hash, fields `{generation, observed_at}`) with `EXPIRE` 60s, refreshed on each use of the scoped key. The revocation safety gate reads these keys to decide when every live peer has observed the new generation; see §13.21 "Revocation safety gate". The rotation leader-lease itself is already covered by the generic `miroir:lease:<scope>` entry above with scope `search_ui_key_rotation:<index>`.
- `miroir:admin_session:<session_id>` (hash, fields `{csrf_token, admin_key_hash, created_at, expires_at, revoked, user_agent, source_ip}`) with `EXPIRE` matching `admin_ui.session_ttl_s`. Logout sets `revoked: 1` on the key (kept until `EXPIRE` fires) so every pod's cookie verification re-checks revocation on each request; a Redis Pub/Sub channel `miroir:admin_session:revoked` is additionally notified so in-memory caches can invalidate immediately rather than waiting for the next round-trip.
- `miroir:ratelimit:searchui:<ip>` (string counter) with `EXPIRE` per `search_ui.rate_limit.redis_ttl_s` (default 60s); bounded by active IP count; see §13.21.
- `miroir:ratelimit:adminlogin:<ip>` (string counter) with `EXPIRE` 60s; bounded by active attacker IP count. Backs the `POST /_miroir/admin/login` 10/minute-per-IP rate limit when `miroir.replicas > 1`; Helm `values.schema.json` rejects local-only admin-login rate limiting in HA. A companion key `miroir:ratelimit:adminlogin:backoff:<ip>` (hash `{failed_count, next_allowed_at}`) implements the exponential-backoff clause — after 5 consecutive failed attempts the backoff doubles per attempt (10m, 20m, 40m, …) until a valid login resets the counter.
- `miroir:cdc:overflow:<sink>` (list); bounded by `cdc.buffer.redis_bytes` (default 1 GiB per sink); see §13.13.

TTLs are set on the keyspaces marked above; list-wide queries are served from the `_index` secondary sets so no `SCAN` is required on the hot path. None of the new tables use Redis `EXPIRE` — they are all long-lived configuration or bounded-by-cardinality history.

### Configuration schema

```yaml
miroir:
  master_key: ""              # client-facing API key (env: MIROIR_MASTER_KEY)
  node_master_key: ""         # key Miroir uses on nodes (env: MIROIR_NODE_MASTER_KEY)
  shards: 64
  replication_factor: 2       # production recommended; Helm chart dev default is 1

  task_store:
    backend: sqlite            # sqlite | redis
    path: /data/miroir-tasks.db
    url: ""                    # for redis: redis://host:6379

  admin:
    enabled: true
    api_key: ""                # env: MIROIR_ADMIN_API_KEY

  # Number of independent query pools. Each group holds all shards.
  # Writes go to all groups; each query goes to exactly one group.
  # This sample shows a two-group production topology. The default when unset is 1.
  replica_groups: 2

  nodes:
  - id: "meili-0"              # stable, unique — never reuse after removal
    address: "http://meili-0.search.svc:7700"
    replica_group: 0           # which group this node belongs to
  - id: "meili-1"
    address: "http://meili-1.search.svc:7700"
    replica_group: 0
  - id: "meili-2"
    address: "http://meili-2.search.svc:7700"
    replica_group: 0
  - id: "meili-3"
    address: "http://meili-3.search.svc:7700"
    replica_group: 1
  - id: "meili-4"
    address: "http://meili-4.search.svc:7700"
    replica_group: 1
  - id: "meili-5"
    address: "http://meili-5.search.svc:7700"
    replica_group: 1

  health:
    interval_ms: 5000
    timeout_ms: 2000
    unhealthy_threshold: 3
    recovery_threshold: 2

  scatter:
    node_timeout_ms: 5000
    retry_on_timeout: true
    unavailable_shard_policy: partial   # partial | error

  rebalancer:
    auto_rebalance_on_recovery: true
    max_concurrent_migrations: 4
    migration_timeout_s: 3600

  server:
    port: 7700
    bind: "0.0.0.0"
    max_body_bytes: 104857600   # 100 MiB
```

**Note on `scatter.retry_on_timeout`.** When this flag is `true` (the default), Miroir dedups retries **orchestrator-side** — Meilisearch CE has no idempotency layer, and §13 requires unmodified CE nodes. Internally, Miroir maintains a per-target-node retry cache keyed by `sha256(document_batch_bytes || target_node_id || client_idempotency_key_or_mtask_id)`, with the last observed terminal response stored against each key. When a write times out and a retry fires, Miroir first checks whether it has already dispatched the same key to that node; if yes and the node returned a terminal status, Miroir returns the cached response instead of re-dispatching, so no duplicate node-side task is ever created. If the prior dispatch has no recorded terminal status (still in flight or unknown), Miroir resends as normal and reconciles against the node's subsequent task listing. This avoids duplicate writes without any node cooperation. See §13.10 for the same retry cache (it is a single mechanism shared across subsystems) and §14.2 for how this cache's memory budget is allocated.

All secrets are read from environment variables in production — never baked into config files or images.

### Admin API (`/_miroir/`)

Reserved path prefix for management operations, not proxied to nodes. Gated by the admin API key.

All admin endpoints accept either `Authorization: Bearer <MIROIR_ADMIN_API_KEY>` or `X-Admin-Key: <MIROIR_ADMIN_API_KEY>`. The bearer form is preferred; `X-Admin-Key` is primarily used by the Admin UI (§13.19) for browser fetches where setting the `Authorization` header is inconvenient. Endpoints marked **client-facing** below (e.g. `/multi-search`, `/indexes/{uid}/explain`) use the regular `master_key` rather than the admin key. The `/_miroir/` prefix is cluster-internal only — no ingress rule exposes it publicly.

| Endpoint | Description |
|----------|-------------|
| **Topology** | |
| `GET /_miroir/topology` | Node list, health state, shard assignments |
| `GET /_miroir/shards` | Shard → node mapping table |
| `GET /_miroir/ready` | Readiness check (used by K8s readiness probe) |
| **Nodes** | |
| `POST /_miroir/nodes` | Add a node (triggers rebalance) |
| `DELETE /_miroir/nodes/{id}` | Drain and remove a node |
| `POST /_miroir/nodes/{id}/drain` | Begin draining shards off a node without removing it (see §6 Scaling) |
| **Rebalance** | |
| `POST /_miroir/rebalance` | Manually trigger rebalance |
| `GET /_miroir/rebalance/status` | Current rebalance progress |
| **Reshard (§13.1)** | |
| `POST /_miroir/indexes/{uid}/reshard` | Begin online resharding to a new S (body: `{"new_shards": N, "throttle_docs_per_sec": R}`) |
| `GET /_miroir/indexes/{uid}/reshard/status` | Current phase and backfill progress |
| **Aliases (§13.7)** | |
| `POST /_miroir/aliases` | Create an alias (body: `{"name": "...", "target": "..."}`) |
| `GET /_miroir/aliases` | List all aliases |
| `GET /_miroir/aliases/{name}` | Current target plus flip history |
| `PUT /_miroir/aliases/{name}` | Atomically flip an alias to a new target (body: `{"target": "..."}`) |
| `DELETE /_miroir/aliases/{name}` | Delete an alias (underlying index untouched) |
| **Dumps (§13.9)** | |
| `POST /_miroir/dumps/import` | Streaming dump import (multipart body with the `.dump` file; returns `{"miroir_task_id": "..."}`) |
| `GET /_miroir/dumps/import/{id}/status` | Progress and phase of a streaming import job |
| **Observability** | |
| `GET /_miroir/metrics` | Prometheus metrics (auth-gated mirror of `:9090/metrics`) |
| **CDC (§13.13)** | |
| `GET /_miroir/changes?since={cursor}&index={uid}` | Long-poll change stream for the `internal` queue sink; `cursor` is a monotonic per-index sequence number, `index` is required. Returns a bounded batch plus the next cursor. |
| **TTL (§13.14)** | |
| `POST /_miroir/indexes/{uid}/ttl-policy` | Set or update per-index TTL sweep policy. Body: `{"sweep_interval_s": N, "max_deletes_per_sweep": M, "enabled": bool}`. Overrides the global `ttl.per_index_overrides` entry. |
| **Shadow (§13.16)** | |
| `GET /_miroir/shadow/diff` | Read the in-memory diff ring buffer. Query: `?target={name}&limit=N&since_id=X&kind={hits,ranking,latency,error}`. Ring buffer size bounded by `shadow.diff_buffer_size`. |
| **Canaries (§13.18)** | |
| `POST /_miroir/canaries` | Create or modify a canary definition (body matches the `canaries:` YAML schema in §13.18). |
| `GET /_miroir/canaries/status` | List canaries with the last N runs each (bounded by `canary_runner.run_history_per_canary`), pass/fail counts, and last-failure detail. |
| `POST /_miroir/canaries/capture` | Record the next M production queries + responses as golden pairs; body: `{"index": "...", "count": M, "name_prefix": "..."}`. |
| **Search UI config (§13.21)** | |
| `POST /_miroir/ui/search/{index}/config` | Set the per-index search-UI configuration (title, facets, sort options, templates, etc.). Body matches the `search_ui:` entry schema in §13.21. |
| `GET /_miroir/ui/search/locale/{lang}.json` | Public (no auth) locale fetch for the search UI. Returns translations JSON; served with long `max-age`. This endpoint is the one exception to the "admin-gated under `/_miroir/`" rule — it is whitelisted so unauthenticated end-user SPAs can load strings. |
| **Search UI (§13.21)** | |
| `GET /_miroir/ui/search/{index}/session` | JWT session-minting entry point for the end-user search UI. Returns `{token, expires_at, index, rate_limit}`. Auth: `public` mode — IP rate-limited (no credentials); `shared_key` — requires `X-Search-UI-Key`; `oauth_proxy` — requires upstream auth headers (e.g. `X-Forwarded-User`, `X-Forwarded-Groups`). See §13.21. |
| `POST /_miroir/ui/search/{index}/beacon` | Receives client-emitted analytics/latency beacons from the embedded SPA. Body: `{type: "click_through" \| "latency", event_id: <uuid>, query: <string>, result_id?: <string>, position?: <int>, duration_ms?: <int>, timestamp: <int ms>}`. Auth: same JWT as other search UI endpoints (`Authorization: Bearer <jwt>` with `beacon` scope — see §13.21 JWT claims). Rate-limited per `search_ui.rate_limit` (shares the `miroir:ratelimit:searchui:<ip>` bucket with queries). Emits a CDC event with `type: click_through` (always emitted) or `type: latency` (subject to `cdc.emit_internal_writes`). `event_id` is used as the dedup key in the idempotency cache (matches the §13.10 idempotency pattern without requiring the `Idempotency-Key` header — `event_id` plays the same role). See §13.21. |
| `POST /_miroir/ui/search/{index}/rotate-scoped-key` | Manually trigger scoped-key rotation for the search UI (normally automatic `scoped_key_rotate_before_expiry_days` before expiry). Body: `{force: boolean}` — `force=true` bypasses the `scoped_key_rotate_before_expiry_days` timing gate and rotates immediately regardless of how recent the current key is; `force=false` (or omitted) honors the timing gate and is a no-op if the current key is not yet within the rotation window. Auth: admin-gated (`Authorization: Bearer <admin_key>` or `X-Admin-Key`). Response: `{old_key_id, new_key_id, generation, rotated_at}`. See §13.21 "Scoped-key rotation coordination". |
| **Admin UI (§13.19)** | |
| `GET /_miroir/admin` | Embedded single-page admin UI. Authenticates via admin API key (`Authorization: Bearer` or `X-Admin-Key`), or a session cookie after a login form. Static assets served from `rust-embed`. |
| `POST /_miroir/admin/login` | Exchange admin credentials for a session cookie. Body: `{admin_key}`. On success sets `Set-Cookie: miroir_admin_session=<sealed>; HttpOnly; Secure; SameSite=Strict` (sealed with `ADMIN_SESSION_SEAL_KEY` — see §9) and returns `{csrf_token}`; the session row is persisted to `admin_sessions` so logout on any pod is visible to all pods. Rate limit 10/minute per source IP, backed by `miroir:ratelimit:adminlogin:<ip>` when `miroir.replicas > 1` (required — Helm `values.schema.json` rejects local-only admin login rate-limiting in HA). Failed-login exponential backoff: after 5 consecutive failures from the same IP, the backoff window doubles per attempt (10m, 20m, 40m, …) up to a 24h cap, tracked in `miroir:ratelimit:adminlogin:backoff:<ip>`; a successful login resets both counters. |
| `POST /_miroir/admin/logout` | Invalidates the session cookie server-side (revokes the sealed session ID) and clears it client-side (`Set-Cookie: miroir_admin_session=; Max-Age=0`). |
| **Public UI (§13.21)** | |
| `GET /ui/search/{index}` | Embedded single-page search UI for end users. Auth model configurable per deployment via `search_ui.auth.mode` — see §13.21. Rate-limited per source IP. This is the one public SPA entry point served outside `/_miroir/`. |

**Client API extensions.** Two client-facing additions sit alongside the admin table above but are **not** admin-gated — they use the normal `master_key` authorization:

- `POST /multi-search` — batch query API (§13.11). Body: `{"queries": [ {indexUid, q, ...}, ... ]}`. Each query is scattered independently and returned in input order.
- `POST /indexes/{uid}/explain` — query plan API (§13.20). Same body as `/search`. Returns the orchestrator's resolved plan without executing; `?execute=true` additionally runs the plan and returns the real result.

Both endpoints are listed here for discoverability; their routing behavior is summarized in the §5 "Routing by operation type" buckets.

### Rebalancer

Runs as a background Tokio task. Only one Miroir instance runs the rebalancer at a time (advisory lock in SQLite/Redis).

The rebalancer relies on `_miroir_shard` being a filterable attribute on every node (set at index creation). This allows targeted retrieval of exactly the documents belonging to a given shard without scanning the entire node index.

**Migration flow for node addition:**
1. Compute `affected_shards`: shards where the new node enters the top-RF (expected count: `~S/(N+1)`)
2. Begin dual-write: route new inbound writes for affected shards to both old owner and new node
3. For each affected shard, paginate source node using the shard filter — no full scan required:
   ```
   GET /indexes/{uid}/documents?filter=_miroir_shard={shard_id}&limit=1000&offset=0
   GET /indexes/{uid}/documents?filter=_miroir_shard={shard_id}&limit=1000&offset=1000
   ... (until exhausted)
   ```
4. Write each page to the new node (documents already carry `_miroir_shard`)
5. Once all pages transferred, mark node `active` for those shards
6. Stop dual-write; delete the migrated shard from the old node:
   ```
   POST /indexes/{uid}/documents/delete  {"filter": "_miroir_shard = {shard_id}"}
   ```
7. Documents on unaffected shards are never read, written, or touched

**Performance**: with S=64 shards, N=3 nodes, adding node-3 migrates `~16` shards. Each migration reads only that shard's documents (roughly `total_docs / 64` documents per shard), not the full node corpus. Total data moved is approximately `total_docs / 4` — the theoretical optimum for a 4-node cluster.

---

## 5. API Surface and Compatibility

Miroir exposes the Meilisearch REST API verbatim. All request and response shapes, status codes, error formats, and headers must match the Meilisearch spec exactly.

### Routing by operation type

**Broadcast to all nodes:**
- `POST /indexes` — create index
- `PATCH /indexes/{uid}` — update index settings
- `DELETE /indexes/{uid}` — delete index
- All `POST /indexes/{uid}/settings/*` — settings changes
- `DELETE /indexes/{uid}/documents` with filter body
- `POST /keys`, `PATCH /keys/{key}`, `DELETE /keys/{key}`

**Shard-routed writes:**
- `POST /indexes/{uid}/documents` — route each doc by hash(pk) to RF nodes
- `PUT /indexes/{uid}/documents` — same
- `DELETE /indexes/{uid}/documents/{id}` — route to owning shard RF nodes
- `DELETE /indexes/{uid}/documents` by IDs array — route each ID to its shard

**Scatter-gather reads:**
- `POST /indexes/{uid}/search` — fan out to covering set, merge results
- `GET /indexes/{uid}/documents` — fan out, merge, paginate globally
- `GET /indexes/{uid}/documents/{id}` — route to any replica of owning shard
- `POST /multi-search` — per sub-query fan-out; each sub-query follows its own scatter path (§13.11)

**Aggregated reads:**
- `GET /indexes/{uid}/stats` — sum `numberOfDocuments`, merge `fieldDistribution`
- `GET /stats` — aggregate across all indexes and all nodes

**Orchestrator-local:**
- `GET /health` — Miroir's own health (not proxied)
- `GET /version` — return Meilisearch version from any healthy node
- `GET /tasks`, `GET /tasks/{uid}` — Miroir task registry
- `POST /indexes/{uid}/explain` — no node call by default; `?execute=true` promotes to a normal scatter-gather read (§13.20)

### Error format

All errors use the Meilisearch error shape:
```json
{"message": "...", "code": "...", "type": "invalid_request", "link": "..."}
```

Miroir-specific error codes (added to the Meilisearch error vocabulary):
- `miroir_primary_key_required` — document batch without resolvable primary key
- `miroir_no_quorum` — no replica group met its per-group quorum for a shard; see §2 write path (HTTP 503)
- `miroir_shard_unavailable` — one or more shards fully unavailable (error policy only)
- `miroir_reserved_field` — document contains a reserved field name. Currently covers `_miroir_shard` (always reserved) and `_miroir_updated_at` (reserved only when the anti-entropy reconciler of §13.8 is enabled; otherwise client-supplied values in that field are preserved and passed through untouched).
- `miroir_idempotency_key_reused` — `Idempotency-Key` reused with a different request body (HTTP 409). See §13.10.
- `miroir_settings_version_stale` — no covering set could be assembled after excluding nodes whose `node_settings_version` is below the client's `X-Miroir-Min-Settings-Version` floor (HTTP 503). See §13.5.
- `miroir_multi_alias_not_writable` — client write targeted a multi-target alias managed by §13.17 ILM (HTTP 409). See §13.7.
- `miroir_jwt_invalid` — bearer token parsed as a JWT but failed validation (bad signature, expired, wrong `kid`/`alg`, or malformed claims) (HTTP 401). See §13.21 and "Bearer token dispatch" below.
- `miroir_jwt_scope_denied` — JWT signature, expiry, and index-binding are valid, but the token's `scope` array does not include the action for this method+path combination, or the `idx` claim does not match the target index (HTTP 403). Enforced orchestrator-side before any node call; see §13.21.
- `miroir_invalid_auth` — credentials did not match any expected key (master, admin, or valid search UI JWT) on an auth-gated endpoint (HTTP 401). See "Bearer token dispatch" below.

### Reserved fields

Miroir reserves a small set of field names on documents. Any field beginning with the `_miroir_` prefix is owned by the orchestrator and must not be present in client-submitted documents when it is reserved in the current configuration. Attempts to write a document containing a currently-reserved field are rejected with `miroir_reserved_field`. All reserved fields are **always stripped** from API responses (search hits, `GET /documents/{id}`, `GET /documents` listings) before returning to clients.

| Field | Type | Reserved when | Purpose |
|-------|------|---------------|---------|
| `_miroir_shard` | integer | Always (unconditional) | Shard identity injected at write; stored as a filterable attribute; used by the rebalancer (Section 4) and anti-entropy reconciler (§13.8) for targeted shard-level retrieval. |
| `_miroir_updated_at` | integer (ms since epoch) | Only when `anti_entropy.enabled: true` (§13.8) | Logical write timestamp used as the authoritative-version tie-breaker during anti-entropy repair. |
| `_miroir_expires_at` | integer (ms since epoch) | Only when `ttl.enabled: true` (§13.14) | Per-document expiration timestamp consumed by the TTL sweeper. Added to `filterableAttributes` automatically at index creation via the two-phase settings broadcast when TTL is enabled. |

When anti-entropy is disabled, `_miroir_updated_at` is not reserved: client values in a field of that name are preserved and passed through untouched. When anti-entropy is enabled, clients must not submit `_miroir_updated_at`; the orchestrator stamps it on every write and rejects client-supplied values with `miroir_reserved_field`.

When TTL is disabled, `_miroir_expires_at` is not reserved and client values pass through untouched. When TTL is enabled, clients set `_miroir_expires_at` on documents that should expire; the orchestrator does not stamp it, but rejects reads that attempt to project it (it is always stripped from responses) and the sweeper (§13.14) deletes documents whose `_miroir_expires_at <= now_ms`.

### Custom HTTP headers

Miroir adds a small set of custom headers on top of the Meilisearch REST API. All Miroir-specific headers use the `X-Miroir-` prefix; the one exception is `Idempotency-Key`, which follows the widely recognized cross-vendor convention. Clients that do not recognize these headers MUST ignore them — Meilisearch-compatibility is preserved. Unless otherwise noted, the headers are safe to strip or forward through intermediaries.

| Header | Direction | Semantics | Defined in |
|--------|-----------|-----------|------------|
| `X-Miroir-Degraded` | Response | Write or read completed with reduced redundancy. For writes: lists groups or shards that missed per-group quorum (§2 write path). For reads: lists shards that could not be covered. Absence of the header means full coverage. | §2 write path, §3 `unavailable_shard_policy` |
| `X-Miroir-Settings-Version` | Response | Monotonically increasing version of the committed index settings. Stamped on responses after a successful two-phase settings commit. Clients may echo this value back as `X-Miroir-Min-Settings-Version` on subsequent reads for session-consistent behavior. | §13.5 |
| `X-Miroir-Min-Settings-Version` | Request | Client-supplied floor (`u64`) for settings freshness. Request header; the orchestrator excludes nodes with `node_settings_version` below this floor when building a covering set (§13.5), and returns HTTP 503 `miroir_settings_version_stale` only if no covering set can be assembled after exclusions. Intended to be populated from a previously observed `X-Miroir-Settings-Version`. | §13.5 |
| `X-Miroir-Settings-Inconsistent` | Response | Warning: the response was served while a two-phase settings broadcast was mid-flight (propose / verify window). Scores across shards may not be comparable until commit completes. | §13.5 |
| `X-Miroir-Session` | Request and response | Opaque client-chosen session UUID enabling read-your-writes semantics. Miroir pins subsequent reads to the group that ACKd the session's latest write and/or blocks until its task succeeds, per `session_pinning.wait_strategy`. | §13.6 |
| `Idempotency-Key` | Request | Client-supplied UUID de-duplicating retried writes. Body-hash-matched hits return the original `miroir_task_id`; body mismatches return HTTP 409 `miroir_idempotency_key_reused`. Also used internally for the synthesized key that makes `scatter.retry_on_timeout` safe (Section 4). | §13.10 |
| `X-Miroir-Over-Fetch` | Request | Per-request override of `vector_search.over_fetch_factor`. Integer ≥ 1. Raises per-shard `limit` to `requested_limit × factor` so the global reranker can recover correct ordering on sparse semantic matches. Ignored for pure-keyword queries. | §13.12 |
| `X-Miroir-Tenant` | Request | Tenant identifier used when `tenant_affinity.mode: header` to pin reads to `group = hash(tenant_id) % RG` (or the group assigned in `static_map`). Writes still fan out to all groups. Unknown tenants follow `tenant_affinity.fallback`. Ignored unless tenant affinity is enabled in `header` mode. | §13.15 |
| `X-Admin-Key` | Request | Alternative to `Authorization: Bearer <admin_key>` for admin endpoints and the admin UI. Equivalent in scope and gating; useful for browser fetches where the `Authorization` header is inconvenient. Rejected with HTTP 401 if the key does not match `MIROIR_ADMIN_API_KEY`. | §13.19 |
| `X-CSRF-Token` | Request | Admin UI CSRF double-submit token; required on state-changing requests from the Admin UI session cookie path. Rejected with HTTP 403 if absent or mismatched against the token bound to the session cookie. See §9 CSRF posture. | §13.19 |
| `X-Search-UI-Key` | Request | Shared key for `search_ui.auth.mode: shared_key`; required to mint a search UI JWT session via `GET /_miroir/ui/search/{index}/session`. Rejected with HTTP 401 if absent or mismatched. See §13.21. | §13.21 |

### Bearer token dispatch

Three token types can appear on `Authorization: Bearer <value>` simultaneously — the `master_key`, the `admin_key`, and a search UI JWT. Miroir resolves them deterministically in the following order:

0. **Dispatch-exempt check (pre-empts all other rules).** If the request's (method, path) is in the exhaustive dispatch-exempt list in rule 5, the endpoint's handler runs **directly** — rules 1–4 do NOT execute, regardless of whether `Authorization` parses as a JWT, matches an opaque key, or is absent entirely. A valid search UI JWT sent to, say, `GET /_miroir/metrics` is not subject to the rule-1 scope check because `/_miroir/metrics` is dispatch-exempt and its handler decides auth on its own terms.

1. **JWT-shape probe.** Miroir first attempts to parse the bearer token as a JWT (three dot-separated base64url segments with a `kid`/`alg` header). If parsing succeeds, the token is validated as a search UI JWT (signature against the active JWT secret(s) per §9 "JWT signing-secret rotation", exp/nbf, `kid`, `idx` claim, `scope` claim). A parseable-but-invalid signature/exp/kid returns HTTP 401 `miroir_jwt_invalid` — it does **not** fall through to the opaque-token paths below. Signature-valid tokens then pass through the orchestrator-level scope check: the request's (method + path) must correspond to an action in `scope` and the target index must equal `idx`, otherwise HTTP 403 `miroir_jwt_scope_denied` is returned before any node is called. See §13.21.
2. **Admin-path opaque-token match.** If parsing fails (opaque token) and the request path begins with `/_miroir/`, Miroir matches the token against `admin_key`. The following dispatch-exempt exceptions apply (see rule 5 below for the exhaustive list and semantics):
   - `/_miroir/metrics` — admin-key-optional (honored when present, not required).
   - `/_miroir/ui/search/locale/*` — unauthenticated; does not consult any key.
   - `POST /_miroir/admin/login` — credentials carried in the request body as `{admin_key}`, not in `Authorization`; dispatch is bypassed and the login handler performs its own constant-time credential comparison and IP-based rate-limiting (`miroir:ratelimit:adminlogin:<ip>`, §4).
   - `GET /_miroir/ui/search/{index}/session` — authentication depends on `search_ui.auth.mode`: `public` is unauthenticated and rate-limited by source IP; `shared_key` requires the `X-Search-UI-Key` header (NOT a bearer token); `oauth_proxy` requires upstream auth-proxy headers such as `X-Forwarded-User` / `X-Forwarded-Groups`. The handler performs whichever check applies to the active mode.
3. **Master-key match.** Otherwise (opaque token on a non-`/_miroir/` path), Miroir matches against `master_key`.
4. **Mismatch.** All three paths that require auth return HTTP 401 with `miroir_invalid_auth` on mismatch. Requests that omit `Authorization` entirely on an auth-gated endpoint also return `miroir_invalid_auth` — except on the dispatch-exempt endpoints listed in rule 5, where the handler's own auth logic determines the response.
5. **Dispatch-exempt endpoints.** Endpoints marked as **dispatch-exempt** skip the auth dispatch chain above entirely; their handlers implement their own auth logic (body-based for admin login, mode-based for the search UI session endpoint, rate-limited for public paths, optional for metrics). "Dispatch-exempt" here means "the bearer-token dispatch does not attempt to match `admin_key` / `master_key` against the `Authorization` header for this method+path; the endpoint's own handler performs whatever auth is required." The full exhaustive list:
   - `GET /_miroir/metrics` — admin-key-optional; handler accepts the request with or without `Authorization: Bearer <admin_key>`.
   - `GET /_miroir/ui/search/locale/*` — unauthenticated public locale fetch for the end-user SPA.
   - `POST /_miroir/admin/login` — credentials in body (`{admin_key}`); handler does constant-time comparison plus `miroir:ratelimit:adminlogin:<ip>` rate-limiting and exponential backoff (§4).
   - `GET /_miroir/ui/search/{index}/session` — auth per `search_ui.auth.mode` (`public` / `shared_key` via `X-Search-UI-Key` header / `oauth_proxy` via upstream-proxy headers); see §13.21.
   - `GET /ui/search/{index}` — public SPA entry point (static assets); IP rate-limited only.

`X-Admin-Key` (admin endpoints) and `X-Search-UI-Key` (session-minting endpoint) are evaluated independently of the bearer chain above — they short-circuit directly into their respective code paths.

---

## 6. Deployment

### Helm chart structure

```
charts/miroir/
├── Chart.yaml
├── values.yaml
├── values.schema.json
├── templates/
│   ├── _helpers.tpl
│   ├── miroir-deployment.yaml
│   ├── miroir-service.yaml
│   ├── miroir-headless.yaml
│   ├── miroir-configmap.yaml
│   ├── miroir-secret.yaml
│   ├── miroir-hpa.yaml
│   ├── miroir-pvc.yaml         (optional; rendered only when cdc.buffer.primary=pvc or cdc.buffer.overflow=pvc — §13.13)
│   ├── meilisearch-statefulset.yaml
│   ├── meilisearch-service.yaml
│   ├── redis-deployment.yaml   (when taskStore.backend=redis)
│   ├── serviceaccount.yaml
│   └── NOTES.txt
└── tests/
    └── connection-test.yaml
```

### Key values

> **Dev vs. production defaults.** The Helm chart ships "dev-sized" defaults: a single Miroir pod, SQLite task store, `replicationFactor: 1`, no Redis, and HPA disabled. These defaults boot a working single-pod install for evaluation and CI. **For production**, override to `miroir.replicas: 2` (minimum), `miroir.replicationFactor: 2`, `redis.enabled: true`, `taskStore.backend: redis`, and `miroir.hpa.enabled: true`. The config-file schema in Section 4 (`replication_factor: 2`, Redis-backed task store) reflects the production story; `values.schema.json` rejects incompatible combinations (e.g., `replicas > 1` with `taskStore.backend: sqlite`). §14 sizing, the HPA in §14.4, and the §14.7 baseline row all assume the production configuration.

```yaml
miroir:
  image:
    repository: ghcr.io/jedarden/miroir
    tag: ""          # defaults to Chart.appVersion
  replicas: 1        # dev default: override to 2+ in production (requires taskStore.backend=redis)
  shards: 64
  replicationFactor: 1   # dev default: override to 2 in production (intra-group HA replicas per shard)
  replicaGroups: 1       # dev default: override to 2 in production (independent query pools; 1 = no throughput scaling)
  existingSecret: "" # name of K8s Secret with masterKey, nodeMasterKey, adminApiKey
  scatter:
    unavailableShardPolicy: partial
  hpa:
    enabled: false       # dev default; production: true (see §14.4)
  ingress:
    enabled: false
    host: search.example.com
  serviceMonitor:
    enabled: false

meilisearch:
  image:
    repository: getmeili/meilisearch
    tag: v1.37.0
  # Total nodes = replicaGroups × nodesPerGroup.
  # The chart partitions nodes into groups automatically (node-0..Ng-1 → group 0, etc.).
  replicas: 2           # dev default: 1 group × 2 nodes = 2. Production: replicaGroups × nodesPerGroup (e.g., 4 for RG=2, Ng=2)
  nodesPerGroup: 2      # derived from replicas / replicaGroups if omitted
  existingSecret: ""
  persistence:
    size: 50Gi
  podAntiAffinity:
    enabled: true
    topologyKey: kubernetes.io/hostname

redis:
  enabled: false
```

### ConfigMap — node list from StatefulSet DNS

The Miroir ConfigMap is generated from Helm values. Node addresses use StatefulSet stable DNS:

```
http://<release>-meili-<n>.<release>-meili-headless.<namespace>.svc.cluster.local:7700
```

No manual node list management required; scaling `meilisearch.replicas` automatically generates the correct node addresses.

### Service topology

*Production topology shown. Dev default: 1 Miroir pod, 2 Meilisearch pods, HPA disabled (see Helm values above).*

```
External clients → Service: miroir (ClusterIP / Ingress, :7700)
                     ↓
               Deployment: miroir (replicas: 2+ via HPA, see §14.4)
               ┌──────────┬──────────┬──────────┐
               ▼          ▼          ▼
         meili-0      meili-1      meili-2    StatefulSet (headless)
```

Additional services:
- `miroir-metrics` (ClusterIP, :9090) — Prometheus scrape target
- `miroir-headless` (Headless) — stable DNS for Miroir orchestrator pod discovery (§14.5)
- `<name>-meili-headless` (Headless) — stable DNS for StatefulSet pods

### ArgoCD Application

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: miroir-<instance>
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/jedarden/declarative-config
    targetRevision: HEAD
    path: k8s/<cluster>/miroir/<instance>
    helm:
      valueFiles: [values.yaml]
  destination:
    server: https://kubernetes.default.svc
    namespace: <namespace>
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
    - CreateNamespace=true
    - ServerSideApply=true
```

### Scaling

**Vertical (increase node RAM):** Update `meilisearch.resources.limits.memory` and PVC size; ArgoCD syncs; StatefulSet rolling update. No shard migration needed.

**Horizontal (add nodes):**
1. Increment `meilisearch.replicas`
2. ArgoCD provisions the new pod and PVC
3. Register the new node: `POST /_miroir/nodes` (or update config)
4. Rebalancer migrates affected shards to the new node
5. Monitor: `GET /_miroir/rebalance/status`

**Scale down (remove a node):**
1. `POST /_miroir/nodes/{id}/drain` — rebalancer migrates shards off
2. Once drain is complete, decrement `meilisearch.replicas`
3. Manually delete the released PVC

### Resource sizing reference

Node RAM is sized by the data each node holds: `(corpus_size / S) × shards_per_node × 1.5`. With group-scoped assignment, each node in a group holds `S / nodes_per_group` shards, each containing `corpus_size / S` data → `corpus_size / nodes_per_group × 1.5`.

| Corpus | RG | RF | Nodes/group | Total nodes | Node RAM | Node storage | S |
|--------|----|----|-------------|-------------|----------|--------------|---|
| ≤ 10 GB | 1 | 1 | 2 | 2 | 8 GB | 20 GB | 16 |
| ≤ 10 GB | 2 | 1 | 2 | 4 | 8 GB | 20 GB | 16 |
| ≤ 50 GB | 1 | 2 | 3 | 3 | 32 GB | 50 GB | 32 |
| ≤ 50 GB | 2 | 1 | 3 | 6 | 16 GB | 50 GB | 32 |
| ≤ 200 GB | 2 | 1 | 4 | 8 | 64 GB | 100 GB | 64 |
| ≤ 200 GB | 4 | 1 | 3 | 12 | 128 GB | 100 GB | 32 |
| ≤ 1 TB | 2 | 1 | 6 | 12 | 256 GB | 200 GB | 64 |

When choosing between more groups or more nodes per group: more groups means higher search throughput with the same per-node storage; more nodes per group means lower per-node storage with the same throughput.

### Upgrade procedure

**Miroir version:** Update `miroir.image.tag`; standard Deployment rolling update; zero downtime.

**Meilisearch version:** Update `meilisearch.image.tag`; StatefulSet rolling update (one pod at a time); with RF > 1, shards remain covered. Verify all nodes healthy after rollout via `GET /_miroir/topology`.

### ESO secret integration

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: miroir-secrets
  namespace: search
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: openbao-backend
    kind: ClusterSecretStore
  target:
    name: miroir-secrets
    creationPolicy: Owner
  data:
  - secretKey: masterKey
    remoteRef:
      key: kv/search/miroir
      property: master_key
  - secretKey: nodeMasterKey
    remoteRef:
      key: kv/search/miroir
      property: node_master_key
  - secretKey: adminApiKey
    remoteRef:
      key: kv/search/miroir
      property: admin_api_key
```

---

## 7. CI/CD

### Versioning policy

Miroir follows **Semantic Versioning 2.0.0** strictly.

| Change type | Version bump |
|-------------|-------------|
| Breaking API or config change | MAJOR |
| New feature, backward-compatible | MINOR |
| Bug fix, internal improvement | PATCH |

**Pre-1.0 policy:** MINOR bumps may include breaking changes.

Tags follow `v0.1.0` convention. CI triggers on any tag matching `v[0-9]+.[0-9]+.[0-9]+*`.

### Docker image tags

For tag `v0.3.2`:
```
ghcr.io/jedarden/miroir:v0.3.2   (exact, immutable)
ghcr.io/jedarden/miroir:0.3       (minor float, mutable)
ghcr.io/jedarden/miroir:0         (major float, mutable)
ghcr.io/jedarden/miroir:latest    (latest stable, mutable)
```

Pre-release tags do not update `latest` or the float tags. CI builds on `main`:
```
ghcr.io/jedarden/miroir:main-<short-sha>
```

### Argo Workflows template

Lives at: `jedarden/declarative-config → k8s/iad-ci/argo-workflows/miroir-ci.yaml`

Synced automatically by ArgoCD app `argo-workflows-ns-iad-ci`.

```yaml
apiVersion: argoproj.io/v1alpha1
kind: WorkflowTemplate
metadata:
  name: miroir-ci
  namespace: argo-workflows
spec:
  entrypoint: ci-pipeline
  serviceAccountName: argo-workflow-executor

  volumeClaimTemplates:
  - metadata:
      name: workspace
    spec:
      accessModes: [ReadWriteOnce]
      resources:
        requests:
          storage: 8Gi

  arguments:
    parameters:
    - name: repo
      value: "https://github.com/jedarden/miroir.git"
    - name: revision
      value: "main"
    - name: tag
      value: ""

  templates:

  - name: ci-pipeline
    dag:
      tasks:
      - name: checkout
        template: git-checkout
      - name: lint
        template: cargo-lint
        dependencies: [checkout]
      - name: test
        template: cargo-test
        dependencies: [checkout]
      - name: build-binary
        template: cargo-build
        dependencies: [lint, test]
      - name: docker-build
        template: docker-build-push
        dependencies: [build-binary]
        when: "\"{{workflow.parameters.tag}}\" != \"\""
      - name: github-release
        template: create-github-release
        dependencies: [docker-build]
        when: "\"{{workflow.parameters.tag}}\" != \"\""

  - name: git-checkout
    container:
      image: alpine/git:2.43.0
      command: [sh, -c]
      args:
      - git clone --depth=1 --branch {{workflow.parameters.revision}} {{workflow.parameters.repo}} /workspace/src
      volumeMounts:
      - name: workspace
        mountPath: /workspace

  - name: cargo-lint
    container:
      image: rust:1.87-slim
      command: [sh, -c]
      args:
      - cd /workspace/src && cargo fmt --all -- --check && cargo clippy --all-targets --all-features -- -D warnings
      volumeMounts:
      - name: workspace
        mountPath: /workspace

  - name: cargo-test
    container:
      image: rust:1.87-slim
      command: [sh, -c]
      args:
      - cd /workspace/src && cargo test --all --all-features 2>&1
      volumeMounts:
      - name: workspace
        mountPath: /workspace
      resources:
        requests:
          cpu: "2"
          memory: "4Gi"

  - name: cargo-build
    container:
      image: rust:1.87-slim
      command: [sh, -c]
      args:
      - |
        apt-get update -q && apt-get install -qy musl-tools
        rustup target add x86_64-unknown-linux-musl
        cd /workspace/src
        cargo build --release --target x86_64-unknown-linux-musl -p miroir-proxy
        cargo build --release --target x86_64-unknown-linux-musl -p miroir-ctl
        mkdir -p /workspace/artifacts
        cp target/x86_64-unknown-linux-musl/release/miroir-proxy /workspace/artifacts/miroir-proxy-linux-amd64
        cp target/x86_64-unknown-linux-musl/release/miroir-ctl   /workspace/artifacts/miroir-ctl-linux-amd64
        cd /workspace/artifacts
        sha256sum miroir-proxy-linux-amd64 > miroir-proxy-linux-amd64.sha256
        sha256sum miroir-ctl-linux-amd64   > miroir-ctl-linux-amd64.sha256
      volumeMounts:
      - name: workspace
        mountPath: /workspace
      resources:
        requests:
          cpu: "4"
          memory: "8Gi"

  - name: docker-build-push
    container:
      image: gcr.io/kaniko-project/executor:v1.23.0
      args:
      - --context=/workspace/src
      - --dockerfile=/workspace/src/Dockerfile
      - --destination=ghcr.io/jedarden/miroir:{{workflow.parameters.tag}}
      - --destination=ghcr.io/jedarden/miroir:latest
      - --cache=true
      - --cache-repo=ghcr.io/jedarden/miroir/cache
      env:
      - name: DOCKER_CONFIG
        value: /kaniko/.docker
      volumeMounts:
      - name: workspace
        mountPath: /workspace
      - name: docker-config
        mountPath: /kaniko/.docker
    volumes:
    - name: docker-config
      secret:
        secretName: ghcr-credentials
        items:
        - key: .dockerconfigjson
          path: config.json

  - name: create-github-release
    container:
      image: ghcr.io/cli/cli:2.49.0
      command: [sh, -c]
      args:
      - |
        TAG={{workflow.parameters.tag}}
        cd /workspace/src
        NOTES=$(awk "/^## \[${TAG#v}\]/{found=1; next} found && /^## /{exit} found{print}" CHANGELOG.md)
        gh release create "$TAG" \
          --title "Miroir $TAG" \
          --notes "$NOTES" \
          /workspace/artifacts/miroir-proxy-linux-amd64 \
          /workspace/artifacts/miroir-proxy-linux-amd64.sha256 \
          /workspace/artifacts/miroir-ctl-linux-amd64 \
          /workspace/artifacts/miroir-ctl-linux-amd64.sha256
      env:
      - name: GH_TOKEN
        valueFrom:
          secretKeyRef:
            name: github-token
            key: token
      volumeMounts:
      - name: workspace
        mountPath: /workspace
```

### Dockerfile

```dockerfile
FROM scratch
COPY miroir-proxy-linux-amd64 /miroir-proxy
EXPOSE 7700 9090
ENTRYPOINT ["/miroir-proxy"]
CMD ["--config", "/etc/miroir/config.yaml"]
```

### Triggering CI manually

```bash
# Branch build (no release artifacts)
kubectl --kubeconfig=/home/coding/.kube/iad-ci.kubeconfig create -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: miroir-ci-manual-
  namespace: argo-workflows
spec:
  workflowTemplateRef:
    name: miroir-ci
  arguments:
    parameters:
    - name: revision
      value: main
EOF

# Release build
kubectl --kubeconfig=/home/coding/.kube/iad-ci.kubeconfig create -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: miroir-ci-release-
  namespace: argo-workflows
spec:
  workflowTemplateRef:
    name: miroir-ci
  arguments:
    parameters:
    - name: revision
      value: v0.1.0
    - name: tag
      value: v0.1.0
EOF
```

### Required CI secrets on iad-ci

| Secret | Namespace | Keys | Purpose |
|--------|-----------|------|---------|
| `ghcr-credentials` | argo-workflows | `.dockerconfigjson` | Push to ghcr.io |
| `github-token` | argo-workflows | `token` | Create GitHub Releases |

### CHANGELOG format

[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format. The CI release step extracts the relevant section automatically.

### Release checklist

- [ ] All tests pass on `main`
- [ ] `CHANGELOG.md` updated with new version section
- [ ] `Cargo.toml` workspace version bumped
- [ ] `Chart.yaml` `appVersion` updated
- [ ] Migration notes written if task store schema changed

Tag and push:
```bash
git tag -a v0.2.0 -m "Release v0.2.0"
git push origin v0.2.0
```

---

## 8. Testing

### Unit tests (`miroir-core`)

All `#[cfg(test)]` modules in `crates/miroir-core/`. Run via `cargo test --all` in CI on every commit.

**Router correctness:**
- Determinism: same shard always maps to same nodes given same node list
- Minimal reshuffling: adding a 4th node moves at most `2 × (1/4)` of shards
- Uniform distribution: with 64 shards and 3 nodes, each node holds 18–26 shards (RF=1)
- RF > 1 placement: top-2 nodes change minimally when a node is added or removed

**Result merger:**
- Global sort by `_rankingScore` across shards
- `offset + limit` applied after merge (not per-shard)
- `_rankingScore` stripped from response when client did not request it
- Facet counts summed across shards correctly (including keys unique to one shard)
- `estimatedTotalHits` summed across shards

**Task registry:**
- Mapping persists across SQLite open/close (simulated restart)
- Status aggregation: `succeeded` only when all node tasks succeed
- Pruning removes tasks older than TTL

**Primary key extraction:**
- Rejects documents without the declared primary key field
- Handles string and integer primary key values
- Handles nested key paths if supported

### Integration tests

Use `docker-compose` (3 Meilisearch nodes + Miroir). Live in `tests/integration/`. Run with `cargo test --test integration -- --test-threads=1`.

**Test cases:**
- **Document round-trip:** Index 1000 documents, retrieve each by ID — all must be found; verify documents distributed across all 3 nodes
- **Search covers all shards:** Index documents with unique keywords; search for each — every search returns exactly 1 hit
- **Facet aggregation:** 100 documents across 3 color values; facet counts must sum to 100
- **Offset/limit paging:** 50 documents with known scores; 5 pages of 10 must concatenate to the same order as a single limit=50 query, no duplicates or gaps
- **Settings broadcast:** Add synonyms; verify all 3 nodes have the synonyms; search via synonym returns results
- **Task polling:** Index a large batch; poll `GET /tasks/{id}` until `succeeded`; verify all documents searchable
- **Node failure with RF=2:** Index 500 documents; stop one node; search must still return all results; `X-Miroir-Degraded` header must not appear (surviving replicas cover all shards); restart node and verify full routing resumes

### API compatibility tests

Run the same test scenarios against both a real single-node Meilisearch instance and a Miroir instance. Responses must be semantically equivalent: same documents, same search results for equivalent queries, same error codes and shapes.

**SDK smoke tests** in `examples/sdk-tests/` (Python, JavaScript, Go, Rust):
1. Create index
2. Index documents
3. Search and verify results
4. Update settings
5. Delete index

**Error format parity:** Test every expected Meilisearch error code against both real Meilisearch and Miroir.

### Chaos tests

Manual scenarios in `tests/chaos/`:

| Scenario | Expected result |
|----------|-----------------|
| Kill 1 of 3 nodes (RF=2) | Continuous search; degraded writes warn via header |
| Kill 2 of 3 nodes (RF=2) | Shard loss; 503 or partial per policy |
| Kill 1 of 2 Miroir replicas | Zero client-visible downtime |
| Add `tc netem delay 500ms` to one node | Searches slow by at most max shard latency; no errors |
| Restart a killed node | Miroir detects recovery within health check interval, resumes routing |
| Kill a node mid-rebalance | Rebalancer pauses, resumes on recovery; no data loss |

### Performance benchmarks

`benches/` using `criterion`. Run with `cargo bench`.

| Benchmark | Target |
|-----------|--------|
| Rendezvous assignment (64 shards, 3 nodes, 10K docs) | < 1 ms total |
| Merger (1000 hits, 3 shards) | < 1 ms |
| End-to-end search latency vs. single-node | < 2× single-node |
| Ingest throughput (1000 docs through Miroir) | > 80% of single-node |

A PR that increases measured search latency by > 20% over the previous release triggers a review comment.

### Coverage policy

- `miroir-core`: ≥ 90% line coverage (measured by `cargo-tarpaulin`)
- All route handlers: integration test coverage for happy paths and key error paths
- Coverage reported in CI; gates merge from v1.0 forward

---

## 9. Secrets Handling

### Secret inventory

| Secret | Reads it | Notes |
|--------|---------|-------|
| `master_key` | Miroir proxy | Client-facing authentication |
| `node_master_key` | Miroir proxy | Miroir → Meilisearch node authentication |
| `meilisearch_master_key` | Meilisearch pods | Each node's own key (matches `node_master_key`) |
| `admin_api_key` | Operators, `miroir-ctl` | Gates `/_miroir/` endpoints |
| `ADMIN_SESSION_SEAL_KEY` | Miroir proxy | Symmetric key (64 bytes) used to seal/verify Admin UI session cookies (HMAC-SHA256 for integrity + XChaCha20-Poly1305 for confidentiality of the session ID). Rotate alongside `admin_api_key`. Env var loaded at pod startup; if unset, Miroir generates a random key at startup and logs a warning — multi-pod deployments **must** set the same value across all pods, otherwise cookies sealed on one pod fail verification on others and users are logged out on every request that hits a different pod. See §4 `admin_sessions` and §13.19. |
| `SEARCH_UI_JWT_SECRET` | Miroir proxy | Signs short-lived JWT session tokens minted by `GET /_miroir/ui/search/{index}/session` (§13.21). Required when `search_ui.enabled: true`; orchestrator refuses to start the search UI without it. |
| `search_ui_shared_key` | Miroir proxy, host app | Shared secret that host pages send as `X-Search-UI-Key` when `search_ui.auth.mode: shared_key`. Env var name is configurable via `search_ui.auth.shared_key_env`. Optional — required only in `shared_key` mode. |
| `ghcr_credentials` | Kaniko (iad-ci) | Image push to ghcr.io |
| `github_token` | gh CLI (iad-ci) | GitHub Release creation |
| `redis_password` | Miroir proxy | Redis task store auth (optional) |

### Key relationship models

**Model A — Shared master key (simple):**
```
Client API key = Miroir master_key = node_master_key = Meilisearch master key
```
Use when all components run in the same trusted namespace.

**Model B — Separated keys (recommended for production):**
```
Client:  Authorization: Bearer <miroir_master_key>
          ↓ Miroir validates, strips, re-signs
Nodes:   Authorization: Bearer <node_master_key>
```
Clients never learn node keys. Node credentials can be rotated without client changes.

### K8s Secret structure

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: miroir-secrets
  namespace: search
type: Opaque
stringData:
  masterKey: "<client-facing key>"
  nodeMasterKey: "<key for nodes>"
  adminApiKey: "<admin key>"
---
apiVersion: v1
kind: Secret
metadata:
  name: meilisearch-secrets
  namespace: search
type: Opaque
stringData:
  masterKey: "<matches nodeMasterKey>"
```

### Zero-downtime rotation (Model B)

**Terminology — this is the #1 source of confusion.** Meilisearch CE has exactly one **startup master key**, supplied via `MEILI_MASTER_KEY` at process start and fixed for the life of the process. It cannot be rotated without restarting the node. `POST /keys` does **not** create additional master keys; it creates **scoped child keys** whose validity is evaluated against the single startup master key. The `nodeMasterKey` that Miroir uses to talk to each Meilisearch node is actually an **admin-scoped API key** created via `POST /keys` (scoped with all actions on all indexes), not the startup master key itself. Zero-downtime rotation only applies to this admin-scoped key; the startup master key requires a separate restart window (see below).

**Rotation flow for the admin-scoped `nodeMasterKey` (zero-downtime):**

1. On each Meilisearch node, generate a new admin-scoped key via `POST /keys` (with actions `["*"]`, indexes `["*"]`, optional expiration). Both the old and new admin-scoped keys are now valid concurrently — this works because they are child keys of the same startup master key, not competing masters.
2. Update the ESO source / K8s Secret `miroir-secrets.nodeMasterKey` with the new admin-scoped key value.
3. Rolling-restart Miroir pods so each pod picks up the new key from the refreshed Secret. During rollout, old and new pods briefly coexist — each uses its own view of `nodeMasterKey` and both views authenticate successfully against the nodes.
4. Once all Miroir pods are on the new key, delete the old admin-scoped key on every node via `DELETE /keys/{old_key_uid}`.

**Rotating the Meilisearch startup master key (not zero-downtime):** Because it is fixed at process start, rotation requires restarting each Meilisearch pod with the new `MEILI_MASTER_KEY`. Admin-scoped child keys created under the old master key are invalidated by the restart and must be re-created against the new master key before step 1 of the flow above can run. This is a planned maintenance operation, not part of the zero-downtime flow.

### JWT signing-secret rotation

The search UI (§13.21) signs end-user JWT session tokens with an HMAC secret supplied via environment variable. Because tokens are short-lived (default `session_ttl_s: 900`, i.e. 15 min) but still long enough to straddle a rollout, Miroir supports a dual-secret overlap window so rotation is zero-downtime.

**Secret loading.** At startup Miroir reads:
- A **primary secret** from the env var named by `search_ui.auth.jwt_secret_env` (default `SEARCH_UI_JWT_SECRET`). Required when `search_ui.enabled: true` — orchestrator refuses to start the search UI without it.
- An **optional rollover secret** from the env var `SEARCH_UI_JWT_SECRET_PREVIOUS`. Present only during the rotation window; absent in steady state.

**Signing and validation behavior.**
- Newly-minted tokens (issued by `GET /_miroir/ui/search/{index}/session`) are always signed with the **primary** secret. The `kid` header carries a hash-prefix of the primary so clients and operators can identify which secret signed a given token.
- Incoming tokens are validated against BOTH secrets when both are present — the signature is accepted if either HMAC verifies. This allows sessions minted under the old secret to remain valid through the overlap window.
- If only the primary is loaded, only it is accepted (steady state).

**Rotation procedure (5 steps).**
1. Generate a new 64-byte random secret — e.g. `openssl rand -base64 64`.
2. Update the K8s Secret so that `SEARCH_UI_JWT_SECRET_PREVIOUS` = current primary, and `SEARCH_UI_JWT_SECRET` = the new secret.
3. Rolling-restart Miroir pods. After the rollout both secrets are active on every pod: newly-minted tokens carry the new signature; tokens minted by pre-rollout pods still verify via the previous secret.
4. Wait `session_ttl_s + buffer` (default 15 min + 5 min = 20 min) so that every token signed under the old secret has expired.
5. Remove `SEARCH_UI_JWT_SECRET_PREVIOUS` from the Secret and rolling-restart once more so only the new secret remains loaded.

**Cadence.** Recommended once per 90 days, or immediately on suspected leak. A leaked old secret can be revoked by shortening the overlap: set `SEARCH_UI_JWT_SECRET_PREVIOUS` to an empty string (effectively absent) and redeploy.

**Automated rotation.** A K8s CronJob can drive the procedure end-to-end by invoking `miroir-ctl ui rotate-jwt-secret`, which sequences the admin-API calls that update the Secret (via the configured secret backend — e.g. ESO or Sealed Secrets), trigger the two rolling restarts, and wait out the overlap window. The CronJob schedule defaults to quarterly; operators can disable automation by leaving the CronJob at `suspend: true` and invoking the command manually.

### CSRF posture

The Admin UI (§13.19) and the search UI session endpoint (§13.21) both have browser-initiated paths to state-changing requests, so CSRF must be addressed explicitly:

- **Admin UI sessions** use secure, HTTP-only, `SameSite=Strict` cookies (issued by the admin login form) with a separate CSRF token double-submitted via `X-CSRF-Token` header on any state-changing request (`POST`/`PUT`/`PATCH`/`DELETE`). The token is rotated on each login and bound to the session cookie; mismatch → 403.
- **Bearer tokens** (`Authorization: Bearer …`) bypass CSRF checks since they cannot be set by cross-origin forms or `<img>` tags — only explicit JavaScript from an allowed origin can attach them. Same for `X-Admin-Key` (non-simple header, forces CORS preflight).
- **Origin checks.** Admin UI enforces `admin_ui.allowed_origins` (default: `same-origin`) on the session endpoint and on any cookie-authenticated mutation. Search UI session endpoint enforces `search_ui.allowed_origins` (default `["*"]` in `public` mode, empty otherwise). Requests with a mismatched `Origin` header are rejected 403 before any auth check runs.
- The unauthenticated `GET /ui/search/{index}` SPA asset delivery is a static GET and carries no CSRF risk; all state-changing work is funneled through the session endpoint which applies the checks above.

### `miroir-ctl` credential handling

Reads admin API key from (in priority order):
1. `MIROIR_ADMIN_API_KEY` environment variable (matches the server-side env var defined in Section 4 `admin.api_key`)
2. `~/.config/miroir/credentials` TOML file
3. `--admin-key` flag (key visible in process list — avoid in scripts)

### What Miroir does not handle

- **Tenant tokens** — Meilisearch JWT tenant tokens are forwarded to nodes as-is. Nodes validate them independently. Works correctly when all nodes have identical settings.
- **Per-index API key scoping** — forwarded to nodes unchanged; enforced by nodes.
- **Key creation API** — broadcast to all nodes; requires all nodes available for atomic key creation.

---

## 10. Observability

### Health endpoints

**`GET /health`** — Meilisearch-compatible. Returns `{"status":"available"}` immediately. Kubernetes liveness probe.

**`GET /_miroir/ready`** — Returns 200 only after Miroir has loaded its config and verified a covering quorum of nodes is reachable. Returns 503 during startup. Kubernetes readiness probe.

**`GET /_miroir/topology`** — Full cluster state:
```json
{
  "shards": 64,
  "replication_factor": 2,
  "nodes": [
    {"id": "meili-0", "status": "healthy", "shard_count": 44, "last_seen_ms": 1200},
    {"id": "meili-1", "status": "healthy", "shard_count": 40, "last_seen_ms": 800},
    {"id": "meili-2", "status": "degraded", "shard_count": 48, "last_seen_ms": 12000, "error": "connection refused"}
  ],
  "degraded_node_count": 1,
  "rebalance_in_progress": false,
  "fully_covered": true
}
```

### Prometheus metrics

Miroir exposes the same metric set on two endpoints:
- **Main port (7700) — `/_miroir/metrics`** — requires the admin API key (`Authorization: Bearer $MIROIR_ADMIN_API_KEY`). Same data as `:9090/metrics`; useful for ad-hoc inspection from outside the cluster when the admin port is not exposed.
- **Metrics port (9090) — `/metrics`** — unauthenticated, bound only inside the pod network; this is the Prometheus scrape target and is what the ServiceMonitor below points at.

All metric names are prefixed `miroir_`.

**Requests:**
- `miroir_request_duration_seconds{method, path_template, status}` — histogram
- `miroir_requests_total{method, path_template, status}` — counter
- `miroir_requests_in_flight` — gauge; in-flight request count at the orchestrator — scraped by HPA for throughput-based scaling (§14.4)

**Node health:**
- `miroir_node_healthy{node_id}` — gauge (1/0)
- `miroir_node_request_duration_seconds{node_id, operation}` — histogram
- `miroir_node_errors_total{node_id, error_type}` — counter

**Shards:**
- `miroir_shard_coverage` — gauge (shards with ≥ 1 healthy replica)
- `miroir_degraded_shards_total` — gauge (shards with no healthy replica; 0 is healthy)
- `miroir_shard_distribution{node_id}` — gauge (shards assigned per node)

**Task registry:**
- `miroir_task_processing_age_seconds` — histogram (age of stuck tasks)
- `miroir_tasks_total{status}` — counter (terminal statuses)
- `miroir_task_registry_size` — gauge

**Scatter-gather:**
- `miroir_scatter_fan_out_size` — histogram (nodes queried per search)
- `miroir_scatter_partial_responses_total` — counter
- `miroir_scatter_retries_total` — counter (alternate replica retries)

**Rebalancer:**
- `miroir_rebalance_in_progress` — gauge (1/0)
- `miroir_rebalance_documents_migrated_total` — counter
- `miroir_rebalance_duration_seconds` — histogram

### Advanced capabilities metrics (§13.11–21)

The families below are registered by each advanced-capability subsystem when its feature flag is enabled. Metric names are prefixed `miroir_` like the core set.

**Multi-search (§13.11):**
- `miroir_multisearch_queries_per_batch` — histogram
- `miroir_multisearch_batches_total` — counter
- `miroir_multisearch_partial_failures_total` — counter
- `miroir_tenant_session_pin_override_total{tenant}` — counter (session-pin overrides of tenant affinity group; see §13.11)

**Vector search (§13.12):**
- `miroir_vector_search_over_fetched_total` — counter
- `miroir_vector_merge_strategy{strategy}` — counter (labels: `strategy=convex|rrf`)
- `miroir_vector_embedder_drift_total` — counter

**CDC (§13.13):**
- `miroir_cdc_events_published_total{sink, index}` — counter
- `miroir_cdc_lag_seconds{sink}` — gauge
- `miroir_cdc_buffer_bytes{sink}` — gauge
- `miroir_cdc_dropped_total{sink}` — counter
- `miroir_cdc_events_suppressed_total{origin}` — counter (internal-origin writes suppressed per tag; see §13.13)

**TTL (§13.14):**
- `miroir_ttl_documents_expired_total{index}` — counter
- `miroir_ttl_sweep_duration_seconds{index}` — histogram
- `miroir_ttl_pending_estimate{index}` — gauge

**Tenant affinity (§13.15):**
- `miroir_tenant_queries_total{tenant, group}` — counter
- `miroir_tenant_pinned_groups{tenant}` — gauge
- `miroir_tenant_fallback_total{reason}` — counter

**Traffic shadow (§13.16):**
- `miroir_shadow_diff_total{kind}` — counter (labels: `kind=hits|ranking|latency|error`)
- `miroir_shadow_kendall_tau` — histogram
- `miroir_shadow_latency_delta_seconds` — histogram
- `miroir_shadow_errors_total{target, side}` — counter

**ILM / rollover (§13.17):**
- `miroir_rollover_events_total{policy}` — counter
- `miroir_rollover_active_indexes{alias}` — gauge
- `miroir_rollover_documents_expired_total{policy}` — counter
- `miroir_rollover_last_action_seconds{policy}` — gauge

**Canaries (§13.18):**
- `miroir_canary_runs_total{canary, result}` — counter
- `miroir_canary_latency_ms{canary}` — histogram
- `miroir_canary_assertion_failures_total{canary, assertion_type}` — counter

**Admin UI (§13.19):**
- `miroir_admin_ui_sessions_total` — counter
- `miroir_admin_ui_action_total{action}` — counter
- `miroir_admin_ui_destructive_action_total{action}` — counter

**Explain (§13.20):**
- `miroir_explain_requests_total` — counter
- `miroir_explain_warnings_total{warning_type}` — counter
- `miroir_explain_execute_total` — counter

**Search UI (§13.21):**
- `miroir_search_ui_sessions_total` — counter
- `miroir_search_ui_queries_total{index}` — counter
- `miroir_search_ui_zero_hits_total{index}` — counter
- `miroir_search_ui_click_through_total{index}` — counter (emitted only when `search_ui.analytics.enabled: true`)
- `miroir_search_ui_p95_ms{index}` — gauge (end-user-observed latency, reported via client beacon)

Grafana dashboard panels for these families will be added to `dashboards/miroir-overview.json` when the relevant feature flag is enabled; until then they are scrape-only.

### Grafana dashboard

Provided at `dashboards/miroir-overview.json`. Panels:
1. Cluster health — degraded shards, node healthy table
2. Request rate — by path template
3. p50/p95/p99 latency
4. Node latency comparison — per-node histogram quantiles
5. Search overhead — Miroir vs. single-node Meilisearch ratio
6. Task lag — stuck task age
7. Shard distribution — imbalance detection
8. Rebalance activity

### ServiceMonitor

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: miroir
  namespace: search
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: miroir
      app.kubernetes.io/component: metrics
  endpoints:
  - port: metrics
    interval: 30s
    path: /metrics
```

### Alerting rules

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: miroir-alerts
  namespace: search
spec:
  groups:
  - name: miroir.availability
    rules:
    - alert: MiroirDegradedShards
      expr: miroir_degraded_shards_total > 0
      for: 2m
      labels:
        severity: warning
      annotations:
        summary: "Miroir has {{ $value }} shard(s) with no healthy replica"

    - alert: MiroirNodeDown
      expr: miroir_node_healthy == 0
      for: 5m
      labels:
        severity: warning
      annotations:
        summary: "Miroir node {{ $labels.node_id }} unhealthy for 5m"

    - alert: MiroirHighSearchLatency
      expr: |
        histogram_quantile(0.95,
          rate(miroir_request_duration_seconds_bucket{path_template="/indexes/{uid}/search"}[5m])
        ) > 2.0
      for: 5m
      labels:
        severity: warning
      annotations:
        summary: "Miroir p95 search latency exceeds 2s"

    - alert: MiroirTaskStuck
      expr: miroir_task_processing_age_seconds > 3600
      for: 10m
      labels:
        severity: warning
      annotations:
        summary: "Miroir has tasks stuck in processing for > 1 hour"

    - alert: MiroirRebalanceStuck
      expr: miroir_rebalance_in_progress == 1
      for: 2h
      labels:
        severity: warning
      annotations:
        summary: "Miroir rebalance running for over 2 hours"

    - alert: MiroirSettingsDivergence
      expr: |
        increase(miroir_settings_hash_mismatch_total[10m]) > 0
        and miroir_settings_drift_repair_total == 0
      for: 10m
      labels:
        severity: warning
      annotations:
        summary: "Miroir detected settings divergence on node(s) that was not auto-repaired"
        description: "Cross-reference §13.5 two-phase settings broadcast; drift was observed but the auto-repair counter did not advance, suggesting repair is disabled or failing."

    - alert: MiroirAntientropyMismatch
      expr: increase(miroir_antientropy_mismatches_found_total[18h]) > 0
      for: 18h
      labels:
        severity: warning
      annotations:
        summary: "Miroir anti-entropy reconciler found replica divergence"
        description: "See §13.8. Persistent divergence — 3 consecutive passes at default every 6h schedule (≈18h) indicate a real replica drift that auto-repair could not resolve."
```

The `MiroirSettingsDivergence` and `MiroirAntientropyMismatch` alerts are paired with the capabilities in §13.5 and §13.8 respectively — they fire only when the self-healing paths described there failed to close the gap on their own.

### Tracing (optional)

OpenTelemetry support (disabled by default):
```yaml
miroir:
  tracing:
    enabled: false
    endpoint: "http://tempo.monitoring.svc:4317"
    service_name: miroir
    sample_rate: 0.1
```

When enabled, each search request produces a trace with parallel spans for each node in the covering set — makes latency outliers immediately visible.

### Log format

Structured JSON to stdout:
```json
{
  "timestamp": "2026-05-01T12:00:00.000Z",
  "level": "info",
  "message": "search completed",
  "index": "products",
  "duration_ms": 42,
  "node_count": 3,
  "estimated_hits": 15420,
  "degraded": false
}
```

---

## 11. Onboarding

### Quick start (local, Docker Compose)

```bash
git clone https://github.com/jedarden/miroir.git
cd miroir
docker-compose -f examples/docker-compose-dev.yml up -d

# Verify
curl http://localhost:7700/health
# {"status":"available"}

# Index documents (identical to standard Meilisearch)
curl -X POST http://localhost:7700/indexes/movies/documents \
  -H "Authorization: Bearer dev-key" \
  -H "Content-Type: application/json" \
  -d '[{"id": 1, "title": "Inception"}, {"id": 2, "title": "Interstellar"}]'

# Search
curl -X POST http://localhost:7700/indexes/movies/search \
  -H "Authorization: Bearer dev-key" \
  -H "Content-Type: application/json" \
  -d '{"q": "inception"}'
```

`examples/docker-compose-dev.yml` starts 3 Meilisearch nodes and one Miroir instance. `examples/dev-config.yaml` configures 16 shards with RF=1 for local testing (matches the Helm chart dev default — see §6).

### Production deployment on Kubernetes

```bash
# 1. Add Helm repo
helm repo add miroir https://jedarden.github.io/miroir
helm repo update

# 2. Create namespace and secrets
kubectl create namespace search
kubectl -n search create secret generic miroir-secrets \
  --from-literal=masterKey="<strong-key>" \
  --from-literal=nodeMasterKey="<node-key>" \
  --from-literal=adminApiKey="<admin-key>"
kubectl -n search create secret generic meilisearch-secrets \
  --from-literal=masterKey="<node-key>"

# 3. Install
helm install search miroir/miroir \
  --namespace search \
  --values my-values.yaml \
  --wait

# 4. Create an index
curl -X POST https://search.example.com/indexes \
  -H "Authorization: Bearer <masterKey>" \
  -H "Content-Type: application/json" \
  -d '{"uid": "products", "primaryKey": "product_id"}'
```

### Migrating from single-node Meilisearch

**Option A — Dump and reload (recommended for < 10 GB):**
1. Export a dump from the existing instance (`POST /dumps`, wait for completion)
2. Deploy Miroir
3. Import the dump via Miroir (`POST /_miroir/dumps/import`) — by default Miroir uses `dump_import.mode: streaming` (§13.9), parsing the dump's NDJSON on the fly and routing each document to its owning shards' nodes during import. No cross-cluster broadcast, no post-import rebalance, and no stale documents to clean up.
4. For dump variants that Miroir cannot fully reconstruct via the public API, fall back to `dump_import.mode: broadcast` (legacy): this imports to every node and then requires `miroir-ctl rebalance` to delete non-owning copies. Discouraged because it transiently places 100% of the corpus on each node.

**Option B — Re-index from source (recommended for large corpora):**
Point your indexing pipeline at the Miroir endpoint and re-index from scratch. Clean shard distribution from the start.

**Option C — Live cutover:**
1. Deploy Miroir alongside the old instance
2. Dual-write to both until Miroir is caught up
3. Switch read traffic to Miroir; verify
4. Switch write traffic to Miroir only; decommission old instance

### SDK configuration

The only change is the endpoint URL:

```python
# Python — before
client = meilisearch.Client('https://old-meili.example.com', 'key')
# after
client = meilisearch.Client('https://search.example.com', 'miroir-key')
```

```typescript
// TypeScript — before
const client = new MeiliSearch({ host: 'https://old-meili.example.com', apiKey: 'key' })
// after
const client = new MeiliSearch({ host: 'https://search.example.com', apiKey: 'miroir-key' })
```

```go
// Go
client := meilisearch.NewClient(meilisearch.ClientConfig{Host: "https://search.example.com", APIKey: "miroir-key"})
```

No other application code changes.

### Common operations with `miroir-ctl`

```bash
# Cluster status
miroir-ctl status
# Nodes: 3 healthy, 0 degraded | Shards: 64 covered | Rebalance: idle

# Add a node (after scaling StatefulSet)
miroir-ctl node add --id meili-3 \
  --address http://search-meili-3.search-meili-headless.search.svc.cluster.local:7700

# Monitor rebalance
miroir-ctl rebalance status --watch

# Drain a node before scale-down
miroir-ctl node drain meili-2

# Verify all shards have required replicas
miroir-ctl verify

# Inspect a task
miroir-ctl task status mtask-00391
```

### Common issues

**"primary key required"** — Miroir requires an explicit primary key at index creation: `{"uid": "myindex", "primaryKey": "id"}`. Meilisearch's inference is not available.

**Search returns fewer results than expected** — Check `GET /_miroir/topology` for `degraded_node_count > 0`. A missing node means its shard coverage is lost. The `X-Miroir-Degraded` response header lists missing shards.

**Task polling stuck at "processing"** — A Meilisearch node may be stuck. `miroir-ctl task status <id>` shows per-node task status and which node is lagging. Query that node directly to diagnose.

---

## 12. Delivered Artifacts

### Binary releases (GitHub Releases)

Each release tag publishes to `jedarden/miroir` GitHub Releases:

| File | Description |
|------|-------------|
| `miroir-proxy-linux-amd64` | Static HTTP proxy binary |
| `miroir-proxy-linux-amd64.sha256` | SHA-256 checksum |
| `miroir-ctl-linux-amd64` | Static management CLI binary |
| `miroir-ctl-linux-amd64.sha256` | SHA-256 checksum |

Both binaries are statically compiled against musl libc — no runtime dependencies. linux/amd64 only in v0.x.

### Docker image

**Registry:** `ghcr.io/jedarden/miroir`
**Base:** `scratch` — contains only the static binary. Zero OS packages, no shell.
**Target size:** < 15 MB compressed.

OCI labels on every image:
```
org.opencontainers.image.source=https://github.com/jedarden/miroir
org.opencontainers.image.version=<semver>
org.opencontainers.image.revision=<git-sha>
org.opencontainers.image.licenses=MIT
```

### Helm chart

**Repository:** `https://jedarden.github.io/miroir` (GitHub Pages, `gh-pages` branch)
**OCI:** `ghcr.io/jedarden/charts/miroir` (for air-gapped environments)

Chart version tracks app version. A chart-only fix increments chart patch while keeping `appVersion` stable.

### Repository structure

```
jedarden/miroir/
├── Cargo.toml / Cargo.lock
├── Dockerfile
├── CHANGELOG.md
├── LICENSE (MIT)
├── README.md
├── charts/miroir/
├── crates/
│   ├── miroir-core/
│   ├── miroir-proxy/
│   └── miroir-ctl/
├── tests/
│   ├── integration/
│   └── chaos/
├── examples/
│   ├── docker-compose-dev.yml
│   ├── dev-config.yaml
│   └── sdk-tests/
│       ├── python/
│       ├── javascript/
│       ├── go/
│       └── rust/
├── dashboards/
│   └── miroir-overview.json
└── docs/
    ├── notes/
    ├── plan/
    └── research/
```

The Argo Workflows template lives in:
```
jedarden/declarative-config → k8s/iad-ci/argo-workflows/miroir-ci.yaml
```

### Documentation

`README.md` — project overview, quick start, feature matrix, link to full docs
`CHANGELOG.md` — all release notes in Keep a Changelog format
`docs/plan/plan.md` — this document (design rationale, architecture, full plan)
`examples/` — working configuration examples with comments
Helm chart `values.yaml` — inline documentation for every configurable value
`miroir-ctl --help` — all subcommands documented via clap

### Versioning commitments (from v1.0)

- Meilisearch API compatibility layer: no breaking changes in minor versions
- `miroir-ctl` CLI flags: no incompatible changes in minor versions
- Config file schema: backward-compatible in minor versions (new fields always optional with defaults)
- Helm chart values schema: backward-compatible in minor versions

---

## 13. Advanced Capabilities

This section specifies ten capabilities that sit entirely within the Miroir orchestrator layer. Every Meilisearch node continues to run **unmodified Community Edition** — no patches, no forks, no custom builds, no node-side plugins. Miroir only uses the public Meilisearch REST API when talking to nodes. Each subsection restates this constraint under **Compatibility** to make the invariant explicit.

Four capabilities here directly resolve issues in Open Problems (Section 15): online resharding (§13.1 ↔ OP#3), two-phase settings broadcast (§13.5 ↔ OP#4 drift / score comparability), anti-entropy reconciler (§13.8 ↔ OP#1 dual-write safety), and streaming dump import (§13.9 ↔ OP#5). The remaining six harden latency, correctness, and client ergonomics without introducing new node-side dependencies.

All capabilities are individually togglable via the configuration schema and default to conservative values. Feature flags have per-capability defaults given in the relevant subsection; operators who want pre-existing (pre-feature) behavior can set the flag to `false`. The defaults here were chosen because each feature is individually low-risk and improves a shipping deployment; however, a conservative rollout path is to set all §13 flags to `false`, then enable them incrementally after validating each.

### 13.1 Online resharding via shadow index

**Problem.** Sections 2 and 3 require S fixed at index creation because `hash(pk) % S` changes under S change, invalidating every document's shard assignment. Under-provisioned clusters face a full external reindex (Open Problem 3).

**Mechanism.** Resharding runs as a coordinated six-phase orchestrator operation:

1. **Shadow create.** Create index `{uid}__reshard_{S_new}` on every node with the new shard count, propagating the live index's settings via the two-phase broadcast (§13.5). The shadow is not addressable by clients.
2. **Dual-hash dual-write.** From the moment the shadow exists, every write to `{uid}` is also routed to the shadow using the new hash:
   ```
   shard_old = hash(pk) % S_old    → RF×RG nodes via old assignment
   shard_new = hash(pk) % S_new    → RF×RG nodes via new assignment
   ```
   Inject `_miroir_shard = shard_old` on live writes and `_miroir_shard = shard_new` on shadow writes. Each index sees a self-consistent shard field. Write volume to nodes approximately doubles during this phase.
3. **Backfill.** A background streamer pages every live-index shard using `filter=_miroir_shard={id}` — the same primitive the rebalancer already relies on. Each document is re-hashed under `S_new` and written to the shadow. Throttle and concurrency are configurable. The shadow-index dual-write (step 2) and the backfill writes are both tagged `_miroir_origin: reshard_backfill` so §13.13 CDC publishes exactly one event per client write (the live-index one); backfill and shadow writes are suppressed from CDC by default (see §13.13 "CDC event suppression").
4. **Verify.** Once backfill completes, the orchestrator runs a cross-index PK-set comparator between live and shadow. It iterates every shard of the live index and every shard of the shadow index (via `filter=_miroir_shard={id}` paginated scan, the same primitive §13.8 uses), streams primary keys and content fingerprints into side-by-side xxh3-keyed buckets, and asserts: (a) live PK set == shadow PK set, (b) for each PK, `content_hash_live == content_hash_shadow`. Any discrepancy logs the divergent PKs and fails the verify phase. This reuses §13.8's bucketed-Merkle machinery but with PK-keyed (not shard-keyed) bucketing so live and shadow can be compared across different S values — §13.8 itself is a within-shard reconciler that compares replicas of the same shard (same `_miroir_shard` value) and cannot be run directly across indexes with different S.
5. **Alias swap.** Atomic alias flip (§13.7) points `{uid}` at `{uid}__reshard_{S_new}`. Subsequent writes target only the new S; dual-write stops.
6. **Cleanup.** Live index retained for a configurable TTL (default 48h) for emergency rollback, then deleted.

**Failure handling.** Any failure before step 5 deletes the shadow and is invisible to clients. After step 5, rollback is a reverse alias flip to the retained live index.

**Compatibility.** Pure Meilisearch public API: `POST /indexes`, `POST /indexes/{uid}/documents`, `GET /indexes/{uid}/documents?filter=...`, `DELETE /indexes/{uid}`. No node modification.

**Config.**
```yaml
miroir:
  resharding:
    enabled: true
    backfill_concurrency: 4
    backfill_batch_size: 1000
    throttle_docs_per_sec: 0          # 0 = unlimited
    verify_before_swap: true
    retain_old_index_hours: 48
```

**CLI / Admin API.**
```
miroir-ctl reshard --index products --new-shards 256 --throttle 10000 [--dry-run]

POST /_miroir/indexes/{uid}/reshard  {"new_shards": 256, "throttle_docs_per_sec": 10000}
GET  /_miroir/indexes/{uid}/reshard/status
```

**Metrics.** `miroir_reshard_in_progress`, `miroir_reshard_phase` (0=idle, 1=shadow, 2=backfill, 3=verify, 4=swap, 5=cleanup), `miroir_reshard_documents_backfilled_total`.

**Caveats.** Doubles write volume during dual-write; transient storage footprint ≈ 2× corpus. Schedule during off-peak and monitor node disk pressure.

---

### 13.2 Hedged requests for tail-latency mitigation

**Problem.** A scatter-gather query's latency is bounded by the slowest responding shard. A single GC-paused or disk-throttled node poisons p99 across the whole fleet.

**Mechanism.** For each in-flight node request in a covering set, start a hedge timer at that node's rolling p95 latency (tracked by §13.3). If the timer fires before a response, issue a duplicate request to a different replica of the same shard (alternate intra-group RF replica, or — policy permitting — the same shard in a different replica group). The orchestrator races with `tokio::select!` and drops the loser; dropping the future aborts the in-flight HTTP connection on the Miroir side.

```rust
let primary = fetch(shard, replica_a);
let hedge   = sleep(p95_deadline).then(|| fetch(shard, replica_b));
let winner  = tokio::select! { r = primary => r, r = hedge => r };
// the losing future is dropped; its HTTP connection closes
```

Hedging applies to reads only:
- `POST /indexes/{uid}/search`
- `GET /indexes/{uid}/documents`
- `GET /indexes/{uid}/documents/{id}`

Writes are never hedged — duplicate writes produce duplicate Meilisearch tasks and, in auto-ID modes, duplicate documents. The idempotency layer (§13.10) is the write-side equivalent.

**Compatibility.** Nodes receive normal search/document-GET requests. The cancelled side completes briefly on the node before its connection drop is noticed — equivalent to any abandoned client.

**Config.**
```yaml
miroir:
  hedging:
    enabled: true
    p95_trigger_multiplier: 1.2       # hedge at 1.2× observed p95
    min_trigger_ms: 15                # never hedge sooner than this
    max_hedges_per_query: 2           # cap to avoid thundering herd
    cross_group_fallback: true        # last resort: hedge into another RG
```

**Metrics.** `miroir_hedge_fired_total{outcome="winner"|"loser"}`, `miroir_hedge_latency_savings_seconds` (histogram), `miroir_hedge_budget_exhausted_total`.

---

### 13.3 Adaptive replica selection (EWMA)

**Problem.** Round-robin intra-group replica selection treats a GC-thrashing node identically to a healthy one, and continues routing its full share of queries.

**Mechanism.** Each node carries a running score:

```
score(node) = α · latency_p95_ms + β · in_flight_count + γ · error_rate
```

with α, β, γ from config. All three inputs are EWMA-smoothed (default half-life 5s). The router selects the lowest-scoring eligible node with probability `1 − ε`; with probability `ε` (default 0.05) it picks uniformly at random to keep samples on recovering nodes.

Replaces the `query_seq`-based round-robin in `covering_set` (Section 2). The formula is local to each Miroir pod; scores may differ slightly across pods — each pod converges independently with no coordination required.

**Compatibility.** The router chooses among the same node set Section 2 already allows. Node API calls unchanged.

**Config.**
```yaml
miroir:
  replica_selection:
    strategy: adaptive              # adaptive | round_robin | random
    latency_weight: 1.0
    inflight_weight: 2.0
    error_weight: 10.0
    ewma_half_life_ms: 5000
    exploration_epsilon: 0.05
```

**Metrics.** `miroir_replica_selection_score{node_id}`, `miroir_replica_selection_exploration_total`.

**Degraded.** If all replicas of a shard score above an "excluded" threshold (default: 5× fleet median), the router falls back cross-group per the Section 2 group-unavailability fallback.

---

### 13.4 Shard-aware query planner for PK-constrained searches

**Problem.** Every search fans out to the full covering set (`N/RG` nodes). A filter like `user_id = "u123"` (when `user_id` is the primary key) is answerable by only one shard — Miroir still queries the whole group.

**Mechanism.** Before scatter, parse the search request's filter expression with a small grammar (`pest` or hand-rolled `nom`).

Narrowable patterns:
- `{pk} = "literal"` → 1 shard
- `{pk} IN ["a","b","c"]` → up to `len(list)` shards
- PK predicate `AND` other predicates → still narrowable (AND can only shrink the set)

Non-narrowable patterns:
- `OR` at the top level with non-PK branches
- Negation of a PK predicate
- PK `IN` list exceeding `max_pk_literals_narrowable`

The planner emits a reduced shard set; `covering_set` then includes only nodes owning those shards. For a single-literal PK filter, fan-out drops from `N/RG` nodes to `RF` nodes — or 1 with RF=1. The merger is unchanged: it already handles shards returning zero hits.

**Correctness.** A narrowable query's result set equals the full-fan-out result set: any document not on the narrowed shards cannot satisfy the PK filter (primary keys are unique per shard by definition).

**Compatibility.** Parsing happens at Miroir; the narrowed search request is a standard Meilisearch search payload on the targeted nodes.

**Config.**
```yaml
miroir:
  query_planner:
    enabled: true
    max_pk_literals_narrowable: 128
    log_plans: false
```

**Metrics.** `miroir_query_plan_narrowable_total{narrowed="yes"|"no"}`, `miroir_query_plan_fanout_size` (histogram), `miroir_query_plan_narrowing_ratio` (gauge).

---

### 13.5 Two-phase settings broadcast with verification

**Problem.** Section 3 flags settings broadcast as "the highest-risk operation in the lifecycle." The current sequential apply-with-rollback leaves a non-atomic window where some nodes have new settings and others have old, producing non-comparable `_rankingScore` values and corrupting merged search results (Open Problem 4).

**Mechanism.** Replace the sequential flow with propose / verify / commit. Meilisearch has no dry-run, so verification is read-back:

```
Phase 1 — Propose (parallel):
    for each node:
        PATCH /indexes/{uid}/settings   (new settings)
        task_uid = await
    wait for all task_uids to reach "succeeded"

Phase 2 — Verify (parallel):
    for each node:
        actual      = GET /indexes/{uid}/settings
        actual_hash = sha256(canonical_json(actual))
    all hashes must equal sha256(canonical_json(proposed))

Phase 3 — Commit:
    if verify ok: increment settings_version in task store;
                  stamp X-Miroir-Settings-Version on future responses
    if diverge:   reissue settings with exponential backoff (repair);
                  after max_retries, freeze writes on that index and
                  raise MiroirSettingsDivergence alert.
```

During phases 1–2 the orchestrator returns 202 with a "pending" status; clients poll `GET /tasks/{mtask_id}`. Reads during that window include an `X-Miroir-Settings-Inconsistent` warning header.

**Drift reconciler (always on).** A background task runs every `settings_drift_check.interval_s` (default 5m), hashing each node's settings and repairing mismatches. This catches out-of-band changes (direct access to a node) and cures drift without operator intervention.

**Client-pinned freshness (`X-Miroir-Min-Settings-Version`).** Clients that need read-your-settings semantics — for example, a UI that just applied a synonym update and wants subsequent searches to reflect it — may echo the last observed `X-Miroir-Settings-Version` back on reads via the `X-Miroir-Min-Settings-Version: <u64>` request header (§5). Miroir maintains `node_settings_version(index, node_id)` in the task store (see Section 4 task-store schema), advanced to the cluster-wide `settings_version` whenever that (index, node_id) pair completes a two-phase verify (§13.5 Phase 2) or a drift-repair cycle (the drift reconciler paragraph above). When the header is present, the min-settings-version check uses this per-node value; any node whose `node_settings_version < X-Miroir-Min-Settings-Version` is excluded from the covering set, and if no covering set can be assembled the request returns HTTP 503 `miroir_settings_version_stale`, signaling the client to retry. This gives clients an explicit, opt-in freshness floor without requiring session state (`X-Miroir-Session` of §13.6 remains the mechanism for read-your-writes on document data). The header is cheap to ignore: requests that omit it use the orchestrator's normal scatter routing.

**Compatibility.** Uses `PATCH /indexes/{uid}/settings` and `GET /indexes/{uid}/settings` — both public endpoints.

**Config.**
```yaml
miroir:
  settings_broadcast:
    strategy: two_phase             # two_phase | sequential (legacy)
    verify_timeout_s: 60
    max_repair_retries: 3
    freeze_writes_on_unrepairable: true
  settings_drift_check:
    interval_s: 300
    auto_repair: true
```

**Metrics.** `miroir_settings_broadcast_phase` (gauge), `miroir_settings_hash_mismatch_total`, `miroir_settings_drift_repair_total`, `miroir_settings_version` (gauge; increments only on successful commit).

---

### 13.6 Read-your-writes via session pinning

**Problem.** Miroir is eventually consistent per the task reconciliation model. Clients reading immediately after writing race against node task processing and frequently fail. SDKs work around this by polling task status — clumsy and error-prone.

**Mechanism.** Introduce a session identified by `X-Miroir-Session: <opaque-uuid>` (clients generate and persist the value). Session state in the task store:

```
session_id → {
    last_write_mtask_id: Option<String>,
    last_write_at: Instant,
    pinned_group: Option<GroupId>,     // the first group to reach per-group quorum wins the pin; ties broken by ascending group_id
    min_settings_version: u64,
}
```

If the pinned group later fails (e.g., all nodes in the group down), the session pin is cleared and subsequent reads in the session use normal routing; the recent write is still observable from any other group that ACKd.

**Semantics.**
- **Write + session header:** record `mtask_id` and the pinned group.
- **Read + session header with pending write:** route the read exclusively to `pinned_group`. Two wait strategies are available:
  - **`block`** — block the read at the orchestrator until the mapped node task reaches `succeeded` via `GET /tasks/{uid}` polling on each affected node in the pinned group (short-poll interval 25 ms with exponential backoff, capped by `max_wait_ms`). This is the only strategy that strictly guarantees the prior write is visible in the returned hits.
  - **`route_pin`** — route the read exclusively to `pinned_group` but do **not** wait for the write to propagate. The caller accepts that reads may return results that do not yet reflect the recent write, but will never see replicas that diverge from `pinned_group` (no cross-group inconsistency). Useful when "my own writes eventually, never stale from other groups" is sufficient.
- **Read + session header with no pending write:** session pin is released; normal routing.
- **Request without session header:** exactly today's behavior.

Session state has a TTL (default 15m) and an LRU bound.

**Compatibility.** Both strategies use only the public Meilisearch task API (`GET /tasks/{uid}`) — no node modification and no reliance on non-existent query parameters on search/document endpoints.

**Config.**
```yaml
miroir:
  session_pinning:
    enabled: true
    ttl_seconds: 900
    max_sessions: 100000
    wait_strategy: block             # block | route_pin
    max_wait_ms: 5000
```

**Metrics.** `miroir_session_active_count`, `miroir_session_pin_enforced_total`, `miroir_session_wait_duration_seconds` (histogram), `miroir_session_wait_timeout_total`.

---

### 13.7 Atomic index aliases for blue-green reindexing

**Problem.** Reindexing today requires either downtime (delete + recreate) or application-layer dual-writes. Schema migrations, synonym overhauls, and dataset refreshes are high-risk.

**Mechanism.** Introduce an alias layer in the orchestrator. An alias is a string name that resolves to one or more concrete Meilisearch index UIDs. Two alias kinds are supported; full schema in §4 task store `aliases` table:

- **Single-target alias** — one `current_uid`; client writes and reads resolve to that UID; atomic flip via `PUT /_miroir/aliases/{name}`.
- **Multi-target alias** — `target_uids` is a list of concrete UIDs; reads fan out across all of them via §13.11 multi-search and merge by `_rankingScore`; writes are rejected (see API semantics below); managed exclusively by §13.17 ILM, never by direct operator edit.

All client-facing operations accept either a concrete UID or an alias of either kind. Resolution happens at the proxy's routing step before any fan-out; an already-routed request completes against the UID(s) captured at route time, so alias flips never tear in-flight requests.

**API.**
```
POST   /_miroir/aliases                    body: {"name": "products", "target": "products_v3"}      # single-target
POST   /_miroir/aliases                    body: {"name": "logs-search", "targets": ["logs-20260418","logs-20260417"]}  # multi-target
GET    /_miroir/aliases                    list all
GET    /_miroir/aliases/{name}             current target(s) + history
PUT    /_miroir/aliases/{name}             atomic flip — body: {"target": "products_v4"} or {"targets": [...]}; kind must match existing alias
DELETE /_miroir/aliases/{name}
```

**Write semantics on multi-target aliases.** A client write (PUT/POST /documents, delete, etc.) against a multi-target alias returns `409 Conflict` with error code `miroir_multi_alias_not_writable` and a message pointing to the owning ILM policy. Reads transparently fan out.

**Reindex workflow.**
1. Create `products_v4` (new index, new settings, same primary key).
2. Populate `products_v4` via the application's indexing pipeline pointed at the concrete UID.
3. Verify `products_v4` via direct queries against the UID.
4. `PUT /_miroir/aliases/products {"target": "products_v4"}` — atomic.
5. Retain `products_v3` for rollback; delete after confirmation.

**Compatibility.** Aliases are purely an orchestrator concept. Meilisearch only ever sees concrete UIDs. No node modification.

**Interaction with §13.1.** Online resharding uses this layer — the final "alias swap" step is one `PUT /_miroir/aliases/{name}`.

**Config.**
```yaml
miroir:
  aliases:
    enabled: true
    history_retention: 10            # last N flips retained
    require_target_exists: true
```

**Metrics.** `miroir_alias_resolutions_total{alias}`, `miroir_alias_flips_total{alias}`.

---

### 13.8 Anti-entropy shard reconciler

**Problem.** Replicated shards can drift silently: a dropped write during a partition, a missed delete, a bit-flip, a bug. Without continuous reconciliation, replicas slowly diverge and merged results become non-deterministic (different replica selections return different hits). This also closes the Open Problem 1 dual-write race — any document the migration cutover misses is caught on the next pass.

**Mechanism.** A background reconciler runs per-shard on a schedule.

**Step 1 — Fingerprint.** For each replica of the shard:
```
iterate docs with filter=_miroir_shard={id} (paginated)
for each doc: hash(primary_key || canonical_content_hash)
fold into a Merkle root (or streaming xxh3 digest keyed by pk)
```
All replicas should produce the same root.

**Step 2 — Diff on mismatch.** Recompute per-bucket (e.g., pk-hash modulo 256) digests, locate divergent buckets, then enumerate divergent primary keys.

**Step 3 — Repair.** For each divergent pk:
```
read doc from each replica
if any replica's copy has _miroir_expires_at <= now:
    // expired — TTL-suspend rule: never resurrect
    DELETE the doc from every replica that still holds it
    tag the delete with _miroir_origin: antientropy so §13.13 CDC suppresses it
else:
    // standard "highest updated_at wins" rule
    pick authoritative version: highest _miroir_updated_at,
        with newest node task_uid as tie-breaker
    PUT the authoritative version to all replicas that disagree
    tag the write with _miroir_origin: antientropy so §13.13 CDC suppresses it
```

The expired-document branch implements the §13.14 interaction: before applying the "highest updated_at wins" rule, check `_miroir_expires_at`; if `<= now`, treat the document as expired and delete it from all replicas rather than rewriting it. This prevents resurrection of zombie documents when a late straggler write races a TTL sweep.

The reconciler is self-throttling: sleeps between shards, targets <2% per-node CPU by default. In a healthy cluster it spends almost all its time computing fingerprints and finds zero mismatches; cost is dominated by read bandwidth.

**New reserved field.** `_miroir_updated_at` (integer, milliseconds since epoch) is stamped by the orchestrator on every write and stripped from all client responses — same contract as `_miroir_shard` (Section 5 reserved fields). Existing documents lacking the field are treated as `updated_at = 0` until rewritten. Only injected when anti-entropy is enabled.

**Compatibility.** `GET /indexes/{uid}/documents?filter=...` and `PUT /indexes/{uid}/documents` on the public API. No node modification.

**Config.**
```yaml
miroir:
  anti_entropy:
    enabled: true
    schedule: "every 6h"
    shards_per_pass: 0               # 0 = all, N = throttle
    max_read_concurrency: 2
    fingerprint_batch_size: 1000
    auto_repair: true                # if false, alert only
    updated_at_field: _miroir_updated_at
```

**Metrics.** `miroir_antientropy_shards_scanned_total`, `miroir_antientropy_mismatches_found_total`, `miroir_antientropy_docs_repaired_total`, `miroir_antientropy_last_scan_completed_seconds` (gauge, age).

**Alert.** `miroir_antientropy_mismatches_found_total > 0` for 3 consecutive passes → manual investigation.

---

### 13.9 Streaming routed dump import

**Problem.** Importing a Meilisearch dump via Miroir today broadcasts every document to every node, transiently placing 100% of the corpus on each node (Open Problem 5). Unusable for corpora larger than a single node's disk.

**Mechanism.** Miroir intercepts dump-import requests and streams the dump's NDJSON through a per-document router:

```
client uploads dump → Miroir
    ↓
parse NDJSON incrementally
  (serde_json::StreamDeserializer on the request body)
    ↓
for each document:
    extract primary key
    shard_id = hash(pk) % S
    inject _miroir_shard = shard_id
    append to per-(target-node) buffer
    ↓
flush each per-node buffer in batches of batch_size
    via POST /indexes/{uid}/documents (normal ingest)
    ↓
track fan of node-task-uids in the task registry
    ↓
return one miroir_task_id to the client
```

Dump contents other than documents — index settings, `primaryKey`, keys — are applied via the two-phase settings broadcast (§13.5) before document streaming begins.

**Compatibility.** Nodes receive normal `POST /indexes/{uid}/documents` calls, identical to the Section 3 ingest path. The dump format itself is never sent to nodes. Fallback to legacy broadcast mode exists for dump variants Miroir cannot fully reconstruct, but is discouraged.

**Config.** This is the authoritative schema for dump import; other sections (§14.5 Mode C, §14.8) reference this block rather than duplicate it.
```yaml
miroir:
  dump_import:
    mode: streaming                  # streaming | broadcast (legacy)
    batch_size: 1000                 # documents per POST flushed to each target node
    parallel_target_writes: 8        # concurrent in-flight POSTs across target nodes
    memory_buffer_bytes: 134217728   # 128 MiB hard cap on in-memory buffered docs
    chunk_size_bytes: 268435456      # 256 MiB — input chunk size for the §14.5 Mode C
                                     # chunk-parallel coordinator; shared across pods via
                                     # the task store so large imports scale horizontally
```

**CLI.** `miroir-ctl dump import --file products.dump --index products`

**Admin API.** `POST /_miroir/dumps/import` (multipart body carrying the `.dump` file) returns `{"miroir_task_id": "..."}`; `GET /_miroir/dumps/import/{id}/status` returns the current phase and progress. The CLI is a thin wrapper over these endpoints.

**Metrics.** `miroir_dump_import_bytes_read_total`, `miroir_dump_import_documents_routed_total`, `miroir_dump_import_rate_docs_per_sec` (gauge), `miroir_dump_import_phase` (gauge).

**Note.** Dump *creation* (`POST /dumps` without body) — fan-out collect + merge — is symmetric and not an Open Problem.

---

### 13.10 Idempotency keys and request deduplication

**Problem.** HTTP retries, SDK retry loops, and at-least-once delivery from upstream queues produce duplicate writes. Without idempotency, retry-on-timeout creates duplicate tasks (and, in auto-ID modes, duplicate documents). Simultaneously, hot identical search queries waste a trivial caching opportunity.

**Mechanism — writes.** Accept header `Idempotency-Key: <uuid>` on all write endpoints. The task store maintains:

```
idempotency_cache: { key → (body_sha256, miroir_task_id, expires_at) }
```

On a write with `Idempotency-Key`:
- **key hits, body matches:** return the existing `miroir_task_id`, HTTP 200. The earlier task handles completion.
- **key hits, body differs:** HTTP 409 with error `miroir_idempotency_key_reused`.
- **key miss:** process normally; insert `(key, body_sha256, task_id, now + ttl)`.

TTL defaults to 24h. Cache is LRU-bounded (default cap ≈ 1M entries at ~100 B each ≈ 100 MB).

**Mechanism — reads (query coalescing).** Identical search bodies (canonicalized JSON + index + current `settings_version`) arriving within a configurable window (default 50 ms) share one upstream scatter:

```rust
pending_queries: DashMap<QueryFingerprint, broadcast::Receiver<Bytes>>
```

First caller fires the scatter; subsequent identical queries subscribe to its broadcast channel and receive the same response bytes. The window closes at response time; the next identical query starts a fresh scatter.

**Compatibility.** Entirely orchestrator-side. Requests that reach nodes are normal Meilisearch calls. Query coalescing is safe because Miroir's merge is deterministic for identical inputs, and the coalescing window is short enough that settings cannot change mid-window (settings changes bump `settings_version`, which is part of the fingerprint).

**New error code** (added to Section 5 vocabulary):
- `miroir_idempotency_key_reused` (HTTP 409)

**Config.**
```yaml
miroir:
  idempotency:
    enabled: true
    ttl_seconds: 86400
    max_cached_keys: 1000000
  query_coalescing:
    enabled: true
    window_ms: 50
    max_subscribers: 1000
    max_pending_queries: 10000
```

**Metrics.** `miroir_idempotency_hits_total{outcome="dedup"|"conflict"|"miss"}`, `miroir_idempotency_cache_size` (gauge), `miroir_query_coalesce_subscribers_total`, `miroir_query_coalesce_hits_total`.

---

### 13.11 Multi-search batch API

**Problem.** Real search UIs issue 5–20 queries per page render: main results, per-facet counts, autocomplete, related items, "did you mean?" suggestions. Today each is a separate round-trip. Meilisearch Enterprise has `/multi-search`; CE does not. Miroir delivers it by itself, entirely at the orchestrator.

**Mechanism.** `POST /multi-search` with `{"queries": [{indexUid, q, filter, ...}, ...]}`. Each query in the batch is scattered independently and in parallel; results are returned in input order with individual status codes. Each query uses the full pipeline — §13.4 planning, §13.3 adaptive replica selection, §13.2 hedging, §13.10 coalescing.

```
POST /multi-search
{
  "queries": [
    {"indexUid": "products", "q": "laptop", "limit": 20},
    {"indexUid": "products", "q": "laptop", "facets": ["brand"], "limit": 0},
    {"indexUid": "reviews",  "q": "laptop", "limit": 5}
  ]
}
→ 200 OK
{"results": [{...}, {...}, {...}]}
```

Queries targeting the same index + replica group share HTTP/2 connections to nodes and share per-node query-plan cache lookups. Queries targeting different indexes run fully in parallel. A single slow query does not block the others; each query carries its own deadline.

**Compatibility.** Each query in the batch translates to a standard `POST /indexes/{uid}/search` on Meilisearch CE. No node change.

**Config.**
```yaml
multi_search:
  enabled: true
  max_queries_per_batch: 100
  total_timeout_ms: 30000
  per_query_timeout_ms: 30000
```

**Metrics.** `miroir_multisearch_queries_per_batch` (histogram), `miroir_multisearch_batches_total`, `miroir_multisearch_partial_failures_total` (at least one query in a batch failed).

**Interaction with §13.6 session pinning and §13.15 tenant affinity.** A multi-search batch can span multiple indexes, but session pinning is per-index and tenant affinity is per-request — the rules compose as follows:

- **Session pinning (§13.6) is evaluated per sub-query.** Each sub-query in the batch independently consults the session's pending-writes map: sub-queries targeting an index with a pending write in the session respect the pin (routed to `pinned_group` for that sub-query); sub-queries for other indexes fall back to normal routing. Each sub-query independently may wait for its index's task to complete before executing.
- **Tenant affinity (§13.15) is evaluated per request.** The `X-Miroir-Tenant` header applies to the whole batch — every sub-query routes to the tenant's pinned group by default.
- **Conflict resolution: session pin wins.** If a specific sub-query's session pin selects a different group than the tenant's affinity group, the session pin takes precedence (strong consistency beats tenant isolation). The sub-query runs against the session-pinned group; a metric `miroir_tenant_session_pin_override_total{tenant}` records the override.
- **Explain output (§13.20) reflects the chosen routing for each sub-query separately** — the batched explain response contains one plan object per sub-query with its own `chosen_group`, `tenant_affinity_pinned`, and session-pin annotations.

---

### 13.12 Vector and hybrid search sharding

**Problem.** Meilisearch 1.6+ supports vector search and hybrid (BM25 + semantic) retrieval. Naïve top-K merging across shards produces wrong global rankings: a shard with few semantically-relevant documents returns low scores that compete badly against a dense shard's high scores. Sharded vector search requires a global rerank with over-fetch.

**Mechanism.**
- **Write path.** Vectors travel with the document body and are routed identically to text — `hash(pk) % S` determines shard. Each node stores the full vector for its documents. Embedder configuration (model, dimensions, API key, semanticRatio default) is a setting and subject to the two-phase broadcast (§13.5); any divergence across nodes produces incompatible embeddings and the reconciler repairs it.
- **Read path.** Scatter with **over-fetch factor** (default 3×). For a request with `limit: L`, each shard returns up to `L × over_fetch_factor` hits with both `_semanticScore` and `_rankingScore` (Meilisearch exposes both under hybrid). The merger combines into a single global score via Reciprocal Rank Fusion (RRF) or convex combination `(1 − α) · bm25 + α · semantic`, matching Meilisearch's own hybrid formula. Global sort by combined score, apply offset/limit.
- **Pure vector.** Same path; merger uses `_semanticScore` only.
- **Pure keyword.** Same path; merger uses `_rankingScore` only.

Over-fetching costs extra per-shard work but is the only way to produce correct global ranking for sparse semantic matches. Over-fetch factor is tunable per request via `X-Miroir-Over-Fetch`.

**Compatibility.** Uses Meilisearch native hybrid search: `POST /indexes/{uid}/search` with `hybrid: {embedder, semanticRatio}` and `showRankingScoreDetails: true`. No node change.

**Config.**
```yaml
vector_search:
  enabled: true
  over_fetch_factor: 3          # per-shard limit = requested limit × factor
  merge_strategy: convex        # convex | rrf
  hybrid_alpha_default: 0.5
  rrf_k: 60                     # RRF constant
```

**Metrics.** `miroir_vector_search_over_fetched_total`, `miroir_vector_merge_strategy{strategy}`, `miroir_vector_embedder_drift_total` (distinct embedders detected across nodes).

---

### 13.13 Change data capture (CDC) stream

**Problem.** Downstream consumers — cache invalidators, audit loggers, recommendation trainers, analytics pipelines, secondary indexes — need to know when documents change. Today users build brittle "tail the write API" proxies. Miroir can publish first-class.

**Mechanism.** On every successful write (after quorum ACK per §2), emit an event to configured sinks. Event shape:

```json
{
  "mtask_id": "mtask-039x1",
  "index": "products",
  "operation": "add" | "update" | "delete",
  "primary_keys": ["sku_123", "sku_124"],
  "shard_ids": [12, 47],
  "settings_version": 42,
  "timestamp": 1712345678901,
  "document": { "...": "..." }
}
```

Sinks (multiple allowed, runs in parallel):
- **webhook** — HTTP POST to a configured URL; batched (default 100 events or 1s); retries with exponential backoff
- **nats** — publish to subject `miroir.cdc.{index}`
- **kafka** — produce to topic `miroir.cdc.{index}`
- **internal queue** — `GET /_miroir/changes?since={cursor}&index={uid}` long-poll; cursor is a monotonic sequence number per index

Delivery is **at-least-once**; each event has a stable `event_id` for consumer-side deduplication. Per-sink cursors live in the task store. When a sink is unreachable, events buffer to a tiered in-memory → overflow queue (see config below) before dropping with a loud metric + alert. Document body is omitted by default for bandwidth; opt-in via `include_body: true` per sink.

**Compatibility.** Orchestrator-side write-path tap. No node change.

**Config.**
```yaml
cdc:
  enabled: true
  sinks:
    - type: webhook
      url: https://internal/cdc
      batch_size: 100
      batch_flush_ms: 1000
      include_body: false
      retry_max_s: 3600
    - type: nats
      url: nats://nats.messaging.svc:4222
      subject_prefix: miroir.cdc
  buffer:
    primary: memory             # memory | redis | pvc
    memory_bytes: 67108864      # 64 MiB — fits within the §14.2 per-pod budget
    overflow: redis             # redis | pvc | drop
    redis_bytes: 1073741824     # 1 GiB per-pod budget in Redis for overflow
```

**Buffer backend.** Miroir runs from a `scratch` container image with no writable filesystem by default, so the default primary buffer is in-memory. When overflow is set to `redis` (the default), Redis absorbs the spill — already a requirement when `replicas >= 2`, so no new dependency. For single-pod dev without Redis, operators opt into `primary: pvc` or `overflow: pvc`; the Helm chart then renders an optional `miroir-pvc.yaml` (see §6) and mounts it at `/data/cdc`. Setting `overflow: drop` disables spill entirely — events past the in-memory watermark increment `miroir_cdc_dropped_total{sink}` immediately. The §14.7 task-store-sizing implication is a **Redis budget +1 GiB per pod when CDC overflow is enabled**.

**CDC event suppression.** Not every write the orchestrator issues originates from a client — anti-entropy repairs, reshard backfill, TTL sweeps, and ILM rollover copies all route through the same write pipeline. Emitting a CDC event for each of these would flood subscribers with spurious events on every replica-drift repair, resharding pass, TTL purge, or index rotation. The CDC publisher therefore filters by an internal `_miroir_origin` tag that the orchestrator attaches (in-process, never persisted to the document body) to each internal write:

| Write source | `_miroir_origin` tag | Emitted by default? |
|--------------|---------------------|---------------------|
| Client write (normal PUT /documents) | _absent_ | **yes** — one event per client write |
| Anti-entropy repair (§13.8) | `antientropy` | no |
| Reshard backfill (§13.1 step 3) | `reshard_backfill` | no |
| Reshard dual-write shadow-index side (§13.1 step 2) | `reshard_backfill` | no (live-old side emits normally) |
| TTL sweeper delete (§13.14) | `ttl_expire` | no (opt-in via `cdc.emit_ttl_deletes: true`) |
| ILM rollover copy (§13.17) | `rollover` | no |

The `_miroir_origin` tag is an **internal orchestrator-side marker**, inspected only in the CDC publisher path — it is never stored on the document, never returned to clients, and never leaves the orchestrator process. Cross-references: §13.1 reshard, §13.8 anti-entropy, §13.14 TTL, §13.17 ILM.

Config knobs:

```yaml
cdc:
  emit_ttl_deletes: false       # set true to see TTL-driven deletes in the CDC stream
  emit_internal_writes: false   # set true to see anti-entropy / reshard / rollover writes (debug only)
```

**Metrics.** `miroir_cdc_events_published_total{sink, index}`, `miroir_cdc_lag_seconds{sink}`, `miroir_cdc_buffer_bytes{sink}`, `miroir_cdc_dropped_total{sink}`, `miroir_cdc_events_suppressed_total{origin}` (count of internal-origin writes suppressed per tag).

---

### 13.14 Document TTL and automatic expiration

**Problem.** Session data, log entries, cache documents, GDPR records — all need expiration. Today: cron jobs with filter-delete. Often forgotten, often broken, sometimes OOM.

**Mechanism.** A new reserved field `_miroir_expires_at` (integer, unix ms). Writers set it on documents that should expire. A background sweeper (Mode A, one pod owns a shard range via rendezvous) periodically deletes expired docs using the same filter primitive as the rebalancer:

```
for each owned shard s:
    POST /indexes/{uid}/documents/delete
    body: {"filter": "_miroir_shard = {s} AND _miroir_expires_at <= {now_ms}"}
```

Sweep cadence and batch size are configurable per index via `POST /_miroir/indexes/{uid}/ttl-policy`. Field is stripped from responses like other `_miroir_*` reserved fields and added to the reserved-fields contract in §5 (enabled only when TTL is on, otherwise client values pass through).

**Compatibility.** Uses existing filter-delete API. No node change. Requires `_miroir_expires_at` to be in `filterableAttributes` — Miroir adds this automatically at index creation when TTL is enabled, via the two-phase settings broadcast (§13.5).

**Config.**
```yaml
ttl:
  enabled: true
  sweep_interval_s: 300
  max_deletes_per_sweep: 10000
  expires_at_field: _miroir_expires_at
  per_index_overrides: {}           # map of index → {sweep_interval_s, max_deletes_per_sweep}
```

**Metrics.** `miroir_ttl_documents_expired_total{index}`, `miroir_ttl_sweep_duration_seconds{index}`, `miroir_ttl_pending_estimate{index}` (documents with `expires_at <= now` not yet swept).

**Interaction with §13.8 anti-entropy.** A naïve anti-entropy pass could resurrect expired documents: if a TTL sweep deletes a doc on replica A while a late straggler write with an older `_miroir_updated_at` lands on replica B, the "highest updated_at wins" rule would rewrite the zombie back to A. To prevent this:

- **TTL deletes fan out to ALL replicas atomically in one quorum write** — the same as any other delete. There is no per-replica staggering; the sweeper issues a filter-delete that the orchestrator routes to every replica of the shard simultaneously.
- **Anti-entropy's repair rule treats expired documents as logically deleted** regardless of whether any replica still holds a stale copy. When the repair path sees `_miroir_expires_at <= now_at_repair_time` on any surviving replica's copy, it **deletes from all replicas** rather than rewriting from the "authoritative" version.
- **The "highest `updated_at` wins" rule is suspended for expired documents.** Expiration dominates update-time — a non-expired older version is not promoted over a deletion caused by expiration.

This keeps TTL sweeps idempotent across replica drift and prevents resurrection on every anti-entropy pass. The repair rule change is described in §13.8 step 3.

---

### 13.15 Tenant-to-replica-group affinity

**Problem.** Noisy-neighbor isolation in multi-tenant deployments. Without isolation, one tenant's 10 kQPS spike degrades every other tenant's queries. Without Miroir, this forces operators to run fully separate clusters per tenant.

**Mechanism.** Tenant identity is resolved per request in one of three modes:
- **`header`** — read tenant ID from `X-Miroir-Tenant`; route to `group = hash(tenant_id) % RG`
- **`api_key`** — derive tenant from the inbound API key via a configured mapping table in the task store
- **`explicit`** — static map of tenant → group_id only; unknown tenants fall through to `fallback` routing

Writes always fan out to all groups (consistency invariant of §2 is preserved). Only the **read path** honors affinity: queries from a tenant pin to that tenant's group. A heavy tenant's queries only consume that group's capacity; other tenants' groups are unaffected.

For tenants that need guaranteed isolation, operators can mark specific groups as **dedicated** — no other tenants get routed there — and use the remaining groups as a shared pool.

**Compatibility.** Pure routing decision; no node change. Coexists with §13.3 adaptive selection (tenant pinning narrows the group; adaptive selection chooses within).

**Config.**
```yaml
tenant_affinity:
  enabled: true
  mode: header                    # header | api_key | explicit
  header_name: X-Miroir-Tenant
  fallback: hash                  # hash | random | reject
  static_map:
    enterprise-co: 0              # always group 0
    startup-inc: 1                # always group 1
  dedicated_groups: [0]           # group 0 reserved for mapped tenants only
```

**Metrics.** `miroir_tenant_queries_total{tenant, group}`, `miroir_tenant_pinned_groups{tenant}` (gauge), `miroir_tenant_fallback_total{reason}`.

---

### 13.16 Traffic shadow / teeing to a shadow cluster

**Problem.** Every settings change, ranking-rule tweak, Meilisearch upgrade, or Miroir config change carries risk. Validating against real production traffic is the only reliable way — but production is the scariest place to experiment. Teams want a production-fidelity staging signal without production-scale risk.

**Mechanism.** Configure one or more shadow targets (another Miroir or a standalone Meilisearch). For a configurable fraction of incoming requests, Miroir asynchronously dispatches the same request to the shadow **after** returning the primary's response to the client:

```
client ──→ Miroir ──→ primary cluster ──→ response to client (synchronous)
                 └──→ shadow cluster ──→ async diff worker
                                           ↓
                                  /_miroir/shadow/diff stream
                                  prometheus histograms
```

The diff worker compares responses:
- **hit set symmetric difference** — primary-only hits, shadow-only hits
- **ranking-order Kendall τ** — correlation between the two result orderings
- **latency Δ** — shadow vs. primary
- **error rate** — shadow errors while primary succeeded, and vice versa

Results are streamed to an in-memory ring buffer (queryable at `/_miroir/shadow/diff`) and summarized in Prometheus histograms. Clients never see shadow output; shadow failures never impact client latency.

**Config.**
```yaml
shadow:
  enabled: true
  targets:
    - name: staging
      url: http://miroir-staging.search.svc:7700
      api_key_env: SHADOW_API_KEY
      sample_rate: 0.05
      operations: [search, multi_search, explain]  # writes NEVER shadowed
  diff_buffer_size: 10000
  max_shadow_latency_ms: 5000        # cancel shadow if primary long-done
```

**Compatibility.** Primary and shadow both run standard Meilisearch CE. No node change on either side.

**Metrics.** `miroir_shadow_diff_total{kind}` where kind is `hits|ranking|latency|error`, `miroir_shadow_kendall_tau` (histogram), `miroir_shadow_latency_delta_seconds` (histogram), `miroir_shadow_errors_total{target, side}`.

---

### 13.17 Rolling time-series indexes (index lifecycle management)

**Problem.** Log, event, metric, and telemetry search is the largest single search-workload segment, and it has a distinct shape: heavy writes, read-by-recency, delete-oldest-first. Elasticsearch dominates that market largely because of its Index Lifecycle Management (ILM). Meilisearch CE has none. Miroir provides it by composing existing primitives.

**Mechanism.** A rollover policy is attached to an alias:

```yaml
rollover_policies:
  - name: logs-ilm
    write_alias: logs
    read_alias: logs-search
    pattern: "logs-{YYYY-MM-DD}"
    rollover_triggers:
      max_docs: 10_000_000
      max_age: "7d"
      max_size_gb: 50
    retention:
      keep_indexes: 30              # delete anything older
    index_template:
      primary_key: event_id
      settings_ref: logs-settings   # named settings profile
```

A daily leader-coordinated job (Mode B §14.5) evaluates every policy:
1. If any trigger on the current write-alias's index has fired, create a new index `logs-20260419` using the template (index + settings via §13.5 two-phase broadcast).
2. Atomic alias flip: `logs` → new index (§13.7). The old index remains but receives no new writes.
3. The read alias `logs-search` is a multi-alias that points at the last N indexes; multi-search (§13.11) fans reads across all of them and merges by `_rankingScore`.
4. Indexes older than `retention.keep_indexes` are deleted.

**Compatibility.** Every step uses existing public API: create index, apply settings, atomic alias flip, delete index. No node change.

**Config.**
```yaml
ilm:
  enabled: true
  check_interval_s: 3600
  safety_lock_older_than_days: 7    # refuse to delete indexes newer than this
  max_rollovers_per_check: 10
```

**Metrics.** `miroir_rollover_events_total{policy}`, `miroir_rollover_active_indexes{alias}`, `miroir_rollover_documents_expired_total{policy}`, `miroir_rollover_last_action_seconds{policy}`.

**Alias schema dependency.** ILM's `read_alias` (e.g. `logs-search`) is a **multi-target alias** in the §13.7 sense: it points at the last N rolled indexes and fans reads across them via §13.11 multi-search. The extended §4 `aliases` table schema (`kind` column; `target_uids` JSON array when `kind='multi'`) supports this directly, and §13.7 rejects client writes against multi-target aliases with `miroir_multi_alias_not_writable`. The `write_alias` (e.g. `logs`) remains a single-target alias that ILM atomically flips at each rollover. Only the ILM evaluator may create, modify, or delete the multi-target `read_alias`; direct `PUT /_miroir/aliases/{read_alias}` from operators is refused to prevent ILM state divergence.

---

### 13.18 Synthetic canary queries with golden assertions

**Problem.** The highest-risk failure mode in search is not a node crash (those are detected by metrics) — it is **silent relevance regression**. A settings change, a synonym typo, a stop-word edit, or a ranking-rule reorder can quietly ruin search quality while every metric looks fine. Operators discover it when users complain.

**Mechanism.** Operators register canaries — predefined queries with expected results — in config or via admin API. A background worker (Mode A) runs each canary on its schedule; assertion failures fire metrics and alerts.

```yaml
canaries:
  - name: product_inception
    index: products
    interval_s: 60
    query:
      q: "inception"
      limit: 10
      filter: null
    assertions:
      - type: top_hit_id
        value: "movie_inception"
      - type: top_k_contains
        k: 3
        ids: ["movie_inception", "movie_inception_2"]
      - type: min_hits
        value: 5
      - type: max_p95_ms
        value: 200
      - type: settings_version_at_least
        value: 42
      - type: must_not_contain_id
        ids: ["deprecated_item_1"]
```

`POST /_miroir/canaries` creates and modifies canaries. `GET /_miroir/canaries/status` returns the last N runs per canary with pass/fail history. Failed assertions include the actual observed value and the failing hit set.

Canaries can be **seeded from real traffic**: `POST /_miroir/canaries/capture` records the next M production queries + responses as golden pairs for review and promotion.

**Compatibility.** Standard Meilisearch searches. No node change.

**Config.**
```yaml
canary_runner:
  enabled: true
  max_concurrent_canaries: 10
  run_history_per_canary: 100
  emit_results_to_cdc: true         # canary results published as CDC events
```

**Metrics.** `miroir_canary_runs_total{canary, result}`, `miroir_canary_latency_ms{canary}`, `miroir_canary_assertion_failures_total{canary, assertion_type}`.

---

### 13.19 Default administration interface (Admin Web UI)

**Problem.** The Meilisearch ecosystem lacks a built-in control panel for CE users. Every operator eventually writes their own bespoke tooling. Miroir ships a great one, embedded in the binary, that handles topology, configuration, query debugging, and day-to-day operations.

**Mechanism.** A single-page application embedded in the Miroir binary via `rust-embed`. Served at `/_miroir/admin`. Authenticated by admin API key; supports session cookies after a login form or direct `X-Admin-Key` header. All data access goes through the existing admin API endpoints (Section 4 admin API, §13.X admin endpoints).

**Sections:**
- **Overview** — cluster health summary, degraded shard count, active rebalances/reshards, recent canary failures, CDC backlog
- **Topology** — node health table, shard coverage map, group membership, rebalance/reshard progress
- **Indexes** — list / create / delete indexes; view and edit settings with a live **2PC preview** showing the diff and fingerprint before commit (§13.5)
- **Aliases** — list / create / flip / delete aliases, with history timeline (§13.7)
- **Documents** — paginated document browser per index; filter builder; CSV/NDJSON import via drag-and-drop triggering §13.9 streaming import
- **Query Sandbox** — filter builder, sort builder, facet-request builder, instant-run with per-shard latency breakdown; one-click §13.20 explain; side-by-side diff vs. shadow (§13.16)
- **Tasks** — active and recent tasks; per-node breakdown; retry/cancel where applicable
- **Canaries** — list / create / edit / disable; pass-fail heatmap over time; seed-from-traffic flow (§13.18)
- **Shadow Diff** — live stream and aggregated summary from §13.16
- **CDC Inspector** — subscribe to a live tail of change events (§13.13) with filter by index/operation
- **Metrics** — embedded Grafana iframe OR direct Prometheus panel render
- **Settings** — read and edit Miroir's own config with restart hints for runtime-vs-reload knobs

**Design philosophy (beautiful and functional, desktop + mobile).**

- **Aesthetic.** Clean, modern, minimal chrome. Generous whitespace. Typographic hierarchy with a single sans-serif family (system-ui with Inter fallback). Neutral base palette with a single configurable accent color. Dark mode via `prefers-color-scheme` and a manual toggle. Smooth 200 ms CSS transitions for state changes; no jank.
- **Responsive breakpoints.**
  - **Mobile (< 640 px)** — single-column layout; collapsible top navigation (hamburger); tables transform into stacked cards with key-value rows; touch targets ≥ 44 × 44 px; gesture-friendly (swipe-to-dismiss on overlays); bottom-sheet modals.
  - **Tablet (640–1024 px)** — two-column with a compact sidebar; hybrid table/card layout; hover states gracefully degraded to tap feedback.
  - **Desktop (≥ 1024 px)** — three-pane where appropriate (nav / list / detail); dense tables with sortable columns; hover previews; keyboard shortcuts (⌘K palette, `/` to focus search, arrow keys to navigate tables).
- **Accessibility.** WCAG 2.2 AA: color contrast ≥ 4.5:1 for body, 3:1 for UI; full keyboard navigation; ARIA roles on every interactive element; focus rings on all focusables; screen-reader live regions for async updates; `prefers-reduced-motion` honored.
- **Performance.** Total initial payload ≤ 100 KB gzipped. Preact + vanilla CSS with custom properties (no Tailwind runtime). Code-split per section; lazy-load panels. Real-time updates via Server-Sent Events where applicable (task progress, canary results, CDC tail) — avoids polling churn on mobile batteries and expensive networks. Static assets cached with long `max-age`; HTML with `no-cache` so updates propagate instantly after a Miroir version bump.
- **Information density.** Keyboard power-user shortcuts on desktop; tap-friendly summaries on mobile. The same data, different progressive disclosure per viewport.
- **Trust & safety.** Every destructive action (delete index, flip alias, trigger reshard) requires a confirmation modal that echoes the target name the user must retype. An immutable on-screen activity log records every write operation with operator identity (from the admin key's label) and timestamp.

**Config.**
```yaml
admin_ui:
  enabled: true
  path: /_miroir/admin
  auth: key                     # key | oauth (future) | none (dev only)
  session_ttl_s: 3600
  read_only_mode: false         # if true, all write controls are hidden
  allowed_origins: [same-origin] # session endpoint Origin enforcement (see §9 CSRF posture)
  cors_allowed_origins: []      # empty = same-origin only
  csp_overrides:
    script_src: []              # additional script-src domains
    img_src: []                 # additional img-src domains
    connect_src: []             # additional connect-src for XHR targets
  theme:
    accent_color: "#2563eb"
    default_mode: auto          # auto | light | dark
  features:
    sandbox: true
    shadow_viewer: true
    cdc_inspector: true
```

**CORS / CSP rationale.** Both UIs ship with strict same-origin defaults. Operators embedding the search widget on a third-party domain MUST add that domain to `search_ui.cors_allowed_origins`. Operators serving images from a CDN MUST add the CDN origin to `csp_overrides.img_src`. The admin UI should almost never need `cors_allowed_origins` populated — it is meant for first-party operator access; the knob exists for operators running a separate dashboard host that embeds admin views via iframe. `csp_overrides.*` values are merged into the corresponding CSP directives at render time; they are additive only, never permissive replacements of the base template.

**Compatibility.** All UI actions hit Miroir's existing admin API surface. No new node-side endpoints. No Meilisearch modification.

**Metrics.** `miroir_admin_ui_sessions_total`, `miroir_admin_ui_action_total{action}`, `miroir_admin_ui_destructive_action_total{action}`.

---

### 13.20 Query explain API

**Problem.** "Why is this query slow?" is the #1 operational question. Debugging requires reading Miroir logs, tracing Meilisearch logs on each candidate shard, and guessing. Miroir already **knows** the full plan — it should return it on request.

**Mechanism.** `POST /indexes/{uid}/explain` takes the same body as `/search` and returns the plan without executing:

```json
{
  "resolved_uid": "products_v4",
  "plan": {
    "alias_resolution": {
      "from": "products",
      "to": "products_v4",
      "version": 7
    },
    "narrowed": true,
    "narrowing_reason": "pk filter: product_id IN [3 values]",
    "target_shards": [12, 47, 53],
    "chosen_group": {
      "id": 0,
      "reason": "lowest EWMA score (38 ms vs. group 1 at 52 ms)"
    },
    "target_nodes": {
      "12": "meili-1",
      "47": "meili-1",
      "53": "meili-2"
    },
    "hedging_armed": true,
    "hedge_trigger_ms": 22,
    "coalescing_eligible": true,
    "cache_candidate": false,
    "tenant_affinity_pinned": null,
    "estimated_p95_ms": 18,
    "settings_version": 42
  },
  "warnings": [
    "filter references `category` but `category` is not in filterableAttributes — full table scan on each shard",
    "offset + limit = 10500 triggers per-shard over-fetch; consider cursor pagination"
  ]
}
```

Everything is orchestrator-known; no node call is made by default. `?execute=true` executes the plan alongside returning it — operators get both the plan and the real result in one round-trip for debugging.

Warnings include: unfilterable attributes in filters, very large `offset + limit`, unbounded wildcard queries, settings drift detected, tenant affinity mismatch, shard-aware narrowing that was NOT possible (explain why).

**Auth scope.** `POST /indexes/{uid}/explain` accepts either `Authorization: Bearer <master_key>` (client-level) or `X-Admin-Key: <admin_key>` / `Authorization: Bearer <admin_key>`.

- With **master_key**, warnings are filtered to remove operator-only signals: `settings drift detected`, `tenant affinity mismatch`, and `node_settings_version < floor` are suppressed from the response. The plan structure is otherwise identical to the admin view.
- With **admin_key**, all warnings surface unredacted — operators debugging from the Admin UI (§13.19) see the full picture.
- Both auth forms return the same `plan` shape; the `target_nodes` field is always surfaced because it is already derivable from §13.4 narrowing patterns via the public topology endpoints and withholding it would provide no security benefit.

**Mid-broadcast behavior.** During a two-phase settings broadcast (§13.5 Phase 1 propose or Phase 2 verify/commit), explain returns consistent-if-slightly-stale results:

- `plan.settings_version` reflects the **last committed** version, not the in-flight proposed one. The response additionally includes `plan.broadcast_pending: true` with the proposed settings fingerprint so operators can see what is about to land.
- A warning `settings_broadcast_in_flight` is emitted carrying the expected commit time-to-live (e.g. `"commit in ~2.4s"`), sourced from the broadcast leader's phase timer.
- With `?execute=true` during an in-flight 2PC, the request is accepted and executed against the **last committed** settings (consistent across shards, slightly stale relative to the proposal). The response carries header `X-Miroir-Settings-Pending: true` to signal that a newer settings version is imminent; clients wanting the newer view should retry after the commit completes or set `X-Miroir-Min-Settings-Version` to the expected new version.

**Admin-UI integration.** The Query Sandbox (§13.19) exposes a one-click "Explain" action on any query. Output is rendered visually with shard-to-node arrows and color-coded warnings.

**Compatibility.** Purely orchestrator-side. No node change.

**Config.**
```yaml
explain:
  enabled: true
  max_warnings: 20
  allow_execute_parameter: true
```

**Metrics.** `miroir_explain_requests_total`, `miroir_explain_warnings_total{warning_type}`, `miroir_explain_execute_total`.

---

### 13.21 Default search interface (end-user search UI)

**Problem.** Developers integrating Meilisearch into a product need to build a frontend: search bar, results, facets, pagination, instant-search, mobile-friendly behavior. Even with client SDKs, this is weeks of careful UX work. For many use cases — internal tools, knowledge bases, docs search, catalog browsers, demos, MVPs — a great default UI is all that is needed. Miroir ships one.

**Mechanism.** A single-page application embedded in the Miroir binary via `rust-embed`. Served at `/ui/search/{index}`. Authentication is brokered by the orchestrator — see "Authentication model" below; the SPA never holds a Meilisearch master or node key. Configuration is per-index via `POST /_miroir/ui/search/{index}/config`:

**Authentication model.** The SPA never sees a Meilisearch master or node key. Instead Miroir brokers access through a two-layer credential chain:

1. **Scoped Meilisearch key (orchestrator-held, rotated).** When the search UI is first enabled for an index, the orchestrator creates a **scoped search-only key** on every Meilisearch node via `POST /keys` (admin action, performed on behalf of the operator). The key's actions are limited to `["search"]` and its `indexes` list is restricted to the UI-configured index (or the concrete UIDs behind a multi-target alias). The key carries a hard expiration of `scoped_key_max_age_days` (default 60d) and is automatically rotated by the orchestrator `scoped_key_rotate_before_expiry_days` (default 30d) before expiry: a new key is minted, promoted to primary, and the old key is revoked via `DELETE /keys/{old_uid}` only after all orchestrator pods have observed the rotation (same handoff pattern as the `nodeMasterKey` rotation in §9).

   **Scoped-key rotation coordination.** Rotation is a multi-pod handoff that must never revoke the old key while any peer is still serving requests against it. Coordination uses three pieces of shared state:
   - **Shared-key record.** A Redis hash at `miroir:search_ui_scoped_key:<index>` with fields `{primary_uid, previous_uid, rotated_at, generation}`. On every inbound SPA request, the pod substitutes `primary_uid` as the Meilisearch authorization when forwarding; it additionally accepts `previous_uid` as a valid fallback during the overlap window (in case a just-restarted pod still references the old UID it read a moment earlier). `generation` is a monotonic counter bumped on each rotation.
   - **Leader lease.** Rotation is driven by a single leader holding the `search_ui_key_rotation:<index>` lease (Mode B, §14.5). Only the leader mints the new scoped key, updates the hash, and later revokes `previous_uid`.
   - **Per-pod observation beacon.** Each pod writes `miroir:search_ui_scoped_key_observed:<pod>:<index> = {generation, observed_at}` with 60 s TTL, refreshed on every use of the scoped key. This gives the leader a reliable "who has seen what" view of the fleet.

   **Revocation safety gate.** Before the leader revokes `previous_uid`, it enumerates the live peer set (from the §14.5 Mode B peer-discovery channel) and checks that every live peer has reported observation of the new `generation` on its beacon. Only when the intersection covers the entire peer set does the leader call `DELETE /keys/{previous_uid}` on every Meilisearch node. A peer that disappears (pod restart / rescheduling) before reporting the new generation is tolerated — its next startup will read the hash fresh, skipping the old UID entirely. The leader waits up to `scoped_key_rotation_drain_s` (default 120 s) for stragglers before retrying on the next rotation tick.

   **Trigger.** Rotation is triggered automatically `scoped_key_rotate_before_expiry_days` before the current key's expiry. With the defaults (`scoped_key_max_age_days: 60`, `scoped_key_rotate_before_expiry_days: 30`), a key issued today expires in 60 days and the next rotation fires ~30 days from now. A manual trigger is available via `POST /_miroir/ui/search/{index}/rotate-scoped-key` (admin-gated); the endpoint simply bumps a "rotate-now" flag on the hash that the leader picks up on its next tick. The `force: true` path on that endpoint bypasses the `scoped_key_rotate_before_expiry_days` timing gate entirely and rotates immediately regardless of how recent the current key is.

   **Config validation.** The Helm chart's `values.schema.json` rejects configurations where `scoped_key_rotate_before_expiry_days >= scoped_key_max_age_days` at install time, with a clear error message — such a configuration would cause rotation to fire immediately (or before) key issuance, producing a continuous rotation loop. The two values must satisfy `0 < scoped_key_rotate_before_expiry_days < scoped_key_max_age_days`, typically with a comfortable margin (the defaults give a 30-day active window between issuance and the rotation trigger).
2. **Short-lived session token (browser-held).** The SPA retrieves a short-lived **session token** via `GET /_miroir/ui/search/{index}/session` which returns `{token, expires_at, index, rate_limit}`. The token is a JWT signed by the orchestrator's JWT secret (loaded from the env var named in `search_ui.auth.jwt_secret_env`, default `SEARCH_UI_JWT_SECRET`). Default TTL: 15 minutes. The SPA then calls `/indexes/{uid}/search` with `Authorization: Bearer <token>`; Miroir validates the JWT server-side, rejects it on signature/expiry/index-mismatch, and **substitutes the orchestrator's scoped Meilisearch key** before forwarding to nodes. The scoped node key never leaves the orchestrator. Rotation of the signing secret is documented in §9 "JWT signing-secret rotation".

   **Scope and index claims (defense-in-depth).** JWT claims include `scope: ["search", "multi_search", "beacon"]` (array of allowed action names) and `idx: <index_uid>` (the single index this session is bound to). The orchestrator validates on every request **before any node call**: the (method + path) must match an allowed action in `scope`, and the target index must equal `idx`. The mapping from (method, path) to action names is:
   - `POST /indexes/{idx}/search` → `search`
   - `POST /multi-search` → `multi_search` (every sub-query's `indexUid` must equal `idx`)
   - `POST /_miroir/ui/search/{idx}/beacon` → `beacon`

   Any other combination returns `miroir_jwt_scope_denied` (HTTP 403). This is a belt-and-braces check on top of the scoped Meilisearch key's own `actions: ["search"]` restriction: the orchestrator rejects disallowed combinations before the node sees the request, which matters for endpoints Meilisearch itself does not gate (e.g. the Miroir-side `beacon` endpoint).

   **JWT claim shape:**
   ```json
   {
     "iss": "miroir",
     "sub": "search-ui-session",
     "idx": "products",
     "scope": ["search", "multi_search", "beacon"],
     "exp": 1712345678,
     "iat": 1712344778,
     "kid": "key-id",
     "injected_filter": "tenant IN [\"g1\",\"g2\"]"
   }
   ```
   `injected_filter` is only present in `oauth_proxy` mode — see "Filter injection in `oauth_proxy` mode" below.
3. **Authentication modes** (selectable per deployment via `search_ui.auth.mode`):
   - **`public`** — `GET /_miroir/ui/search/{index}/session` is unauthenticated but rate-limited by source IP (default `10/minute`); suitable for truly public search where anyone on the internet should be able to query.
   - **`shared_key`** — client must include `X-Search-UI-Key: <shared_key>` (op-supplied, loaded from env var named by `search_ui.auth.shared_key_env`) to obtain a token; suitable for "somewhat private" deployments (internal tools reachable on VPN, staging previews, etc.). The shared key is embedded in first-party host pages by the operator; the SPA itself never hardcodes it.
   - **`oauth_proxy`** — the session endpoint expects upstream auth headers (`X-Forwarded-User`, `X-Forwarded-Groups` by default, overridable via `search_ui.auth.oauth_proxy.user_header` / `groups_header`) injected by an auth proxy like oauth2-proxy sitting in front of Miroir. Tokens carry the user and group claims so downstream filter injection (e.g. `tenant IN [user's groups]`) can enforce per-user access control — see "Filter injection in `oauth_proxy` mode" below. Requests missing the expected headers are rejected 401.
4. **Origin restriction.** The session endpoint honors `search_ui.allowed_origins` (default `["*"]` in `public` mode, empty in `shared_key` / `oauth_proxy` modes unless explicitly set); browser requests whose `Origin` header is not listed are rejected 403. `null` origin (file://) is always rejected outside `public` mode.
5. **Never embed any Meilisearch master or node key in the SPA, HTML responses, meta tags, or JavaScript globals.** Only the ephemeral JWT session token may reach the browser, and only by way of the session endpoint.

Cross-reference §9 Secrets Handling for the `SEARCH_UI_JWT_SECRET` and `search_ui_shared_key` inventory entries; the orchestrator reads both from env at startup and refuses to enable the search UI if `SEARCH_UI_JWT_SECRET` is missing.

**Per-index configuration.** The `search_ui` configuration is a list of per-index entries POSTed to `/_miroir/ui/search/{index}/config`. Example for a product catalog:

```yaml
search_ui:
  - index: products
    title: "Product Catalog"
    logo_url: "https://cdn.example.com/logo.svg"
    display_attributes: [name, brand, price, image_url]
    searchable_attributes_hint: [name, description]
    facets:
      - attribute: brand
        type: checkbox
        title: "Brand"
        sort: count_desc
      - attribute: price
        type: range
        min: 0
        max: 10000
        step: 10
        title: "Price"
      - attribute: rating
        type: star
        title: "Rating"
    sort_options:
      - { label: "Relevance", field: null }
      - { label: "Price: Low to High", field: "price:asc" }
      - { label: "Price: High to Low", field: "price:desc" }
      - { label: "Newest", field: "created_at:desc" }
    result_template: card            # card | list | grid | table | custom
    primary_key_field: sku
    hit_url_template: "https://shop.example.com/product/{sku}"
    thumbnail_field: image_url
    instant_search: true
    highlight: true
    typo_tolerance: true
    per_page_default: 24
    per_page_options: [12, 24, 48]
    empty_state_query_suggestions: true
    analytics_enabled: false
```

**Capabilities.**
- **Instant-search** with 150 ms debounce; uses §13.10 query coalescing so concurrent keystrokes across users collapse to a single scatter
- **Combined round-trip**: results + all facet counts fetched in a single `/multi-search` (§13.11) call per keystroke
- **URL state**: every query + filter + sort + page is encoded in the URL so results are bookmarkable and shareable (`?q=laptop&brand=apple&price=500-2000&sort=price:asc&page=2`)
- **Keyboard navigation**: `/` to focus the search box, arrow keys to move through results, `Enter` to open, `Esc` to clear
- **Highlighting** of matched terms in title and snippet (uses Meilisearch's native `_formatted` output)
- **Typo tolerance UI** with "did you mean" suggestions surfaced naturally when zero hits are returned
- **Empty state** with popular queries (seeded from §13.18 canaries or operator-defined suggestions)
- **Error states** with friendly, non-technical messaging and a "report a problem" link
- **Pagination** — infinite scroll on mobile, classic pagination on desktop (configurable)
- **Dark mode** via `prefers-color-scheme` and a manual toggle stored in `localStorage`
- **Internationalization** — bundled with English; operator-supplied locale JSONs load on demand via `GET /_miroir/ui/search/locale/{lang}.json`

**Design philosophy (beautiful and functional, desktop + mobile).**

- **Aesthetic.** Content-first: the UI recedes, the results come forward. Generous whitespace. Strong typographic hierarchy — one variable-weight sans-serif (system-ui → Inter → sans-serif). Subtle motion design: 180 ms fade + translate on result-card enter; no gratuitous animation. Rounded card corners (12 px), soft shadows (1–3 px blur), thin dividers. Single configurable accent color drives CTAs and highlights; the rest is a tight neutral grayscale. Imagery supported with graceful fallbacks — missing thumbnails degrade to an elegant placeholder with the first letter of the title.
- **Responsive breakpoints.**
  - **Mobile (< 640 px)** — single column; sticky search header; facets hidden behind a bottom-sheet drawer with a "Filter" button; result cards full-width with a 3:2 thumbnail on top; infinite scroll; touch targets ≥ 44 × 44 px; pull-to-refresh; back-gesture aware (browser back clears filters, not navigates away).
  - **Tablet (640–1024 px)** — two-column: collapsible facets rail on the left, results on the right; card grid of 2 columns; toggle between card and list view.
  - **Desktop (≥ 1024 px)** — three-column option: facets / results / optional preview pane; card grid of 3 or 4 columns; hover-preview on cards; keyboard shortcuts visible in footer; dense mode available.
  - **Large desktop (≥ 1440 px)** — max content width clamp at 1440 px centered, preventing awkward stretched layouts on ultra-wide monitors.
- **Accessibility.** WCAG 2.2 AA end-to-end: semantic HTML landmarks (`<search>`, `<main>`, `<nav>`); ARIA live region announces result counts after each query; all facets are labeled and reachable via Tab; focus rings always visible; color-blind-safe accent defaults; `prefers-reduced-motion` disables all motion; screen-reader tested flows for search, filter, and open-result.
- **Performance.**
  - Total initial payload ≤ 60 KB gzipped (HTML + JS + CSS + fonts)
  - **Preact** (not React) + vanilla CSS custom properties
  - No client-side bundler runtime; a single pre-built module
  - System fonts default; custom web fonts loaded with `font-display: swap` and only if explicitly configured
  - Images lazy-loaded with `loading="lazy"` and `decoding="async"`
  - Layout-shift-free skeleton loaders during instant-search keystrokes
  - Mobile network target: Lighthouse performance ≥ 95 on a mid-tier Android over 4G
  - SSR-free — fully static SPA, cache-able at the edge
- **Input modalities.** Touch on mobile (tap to select facet, tap card to open, pull-to-refresh), mouse on desktop (hover, right-click context menus optional), keyboard full coverage (`/`, arrows, Enter, Esc, `g f` to toggle filters).

**Embeddable modes.**
- **Iframe** — `<iframe src="https://search.example.com/ui/search/products?embed=true">` strips chrome and adapts to container width; `postMessage` events for height auto-resize and result-clicked.
- **Web component** — `<script src="https://search.example.com/ui/widget.js"></script>` + `<miroir-search index="products" accent="#2563eb"></miroir-search>` drops a fully-featured search into any page.
- **Headless mode** — `?headless=true` returns only the results container, no search input or facets, for use as part of a larger custom UI.

**Custom templates.** `result_template: custom` allows operators to POST an HTML template with `{{field}}` / `{{#if}}` Handlebars-style interpolation. Templates are stored in the task store and served to the SPA at runtime. Template errors are caught and logged; the UI falls back to the default card template rather than breaking.

**Security.**
- Content-Security-Policy restricts script and style origins
- All document fields are HTML-escaped before rendering (XSS-safe)
- Rate limiting per source IP (configurable; default 60 rpm)
- The SPA holds only a short-lived JWT session token; the search-scoped Meilisearch key is orchestrator-held and rotated (see "Authentication model" above) — never the master key, never the node key, never embedded in HTML
- CORS configurable per deployment (see `search_ui.cors_allowed_origins` and `admin_ui.cors_allowed_origins` below)

**Analytics (idempotent click-throughs).** When `search_ui.analytics.enabled: true`, the SPA emits a beacon on result click and on search-completion for end-user latency tracking via `POST /_miroir/ui/search/{index}/beacon` (see §4 Admin API table). Every click-through beacon carries an `event_id: <client-generated uuid>` in the body (§13.10 idempotency cache uses this field directly; the `Idempotency-Key` header is not required because the endpoint uses `event_id` as its dedup key); the browser synthesizes the UUID once per unique `(query, result_id, session)` tuple for click-throughs and `(session, minute_bucket)` for latency beacons, and reuses it on any retry — page refreshes that replay the same click from history do not double-count. The emitted CDC event (`type: click_through` or `type: latency`) uses `event_id` as its identity, and downstream consumers dedupe on that field. Latency events are subject to `cdc.emit_internal_writes` — operators who do not want end-user latency noise in their CDC stream can leave that flag at its default to suppress them. If the SPA cannot generate UUIDs (very old browsers without `crypto.randomUUID`), the orchestrator falls back to `hash(session_id || query || result_id || minute_bucket)` server-side. Event shape published to CDC:

```json
{
  "type": "click_through",
  "event_id": "<idempotency key>",
  "session_id": "<opaque session id>",
  "query": "<query string>",
  "result_id": "<primary key of clicked hit>",
  "result_position": 3,
  "index": "products",
  "timestamp": 1712345678901
}
```

**Compatibility.** Purely orchestrator-served. All data flows through the existing search API. Uses §13.11 multi-search for results + facets; §13.6 session pinning for read-your-writes if the host app is doing concurrent writes; §13.10 query coalescing for efficient instant-search.

**Config.**
```yaml
search_ui:
  enabled: true
  path: /ui/search
  widget_script_enabled: true
  embeddable: true

  auth:
    mode: public                    # public | shared_key | oauth_proxy
    shared_key_env: ""              # env var holding the shared key (shared_key mode)
    session_ttl_s: 900
    session_rate_limit: "10/minute"
    jwt_secret_env: SEARCH_UI_JWT_SECRET
    oauth_proxy:
      user_header: X-Forwarded-User
      groups_header: X-Forwarded-Groups
      filter_template: "tenant IN [{groups}]"   # null = disable filter injection
      attribute_map:                            # optional per-claim binding
        groups: groups_array
        user: user_id_string

  allowed_origins: ["*"]
  scoped_key_max_age_days: 60                  # hard expiry on the scoped Meilisearch key (upper bound on key lifetime)
  scoped_key_rotate_before_expiry_days: 30     # rotate this many days before the key's expiry (must be strictly less than scoped_key_max_age_days; see "Config validation" below)
  scoped_key_rotation_drain_s: 120     # wait this long after the new generation is in place before revoking the previous key; see "Revocation safety gate" above

  rate_limit:
    per_ip: "60/minute"
    backend: redis                  # redis | local (per-pod); redis required when replicas > 1
    redis_key_prefix: "miroir:ratelimit:searchui:"
    redis_ttl_s: 60

  cors_allowed_origins: []          # empty = same-origin only
  csp_overrides:
    script_src: []                  # additional script-src domains
    img_src: []                     # additional img-src domains
    connect_src: []                 # additional connect-src for XHR targets
  csp: "default-src 'self'; img-src 'self' https:; style-src 'self' 'unsafe-inline'"   # base template; any csp_overrides.* lists are merged into the corresponding directives at render time

  analytics:
    enabled: false
    sink: cdc                     # publishes click-throughs as CDC events
```

With `rate_limit.backend: local` and multi-pod deployments, the effective per-IP rate is `per_ip × pod_count` per minute cluster-wide (each pod maintains its own bucket table). For production deployments with `miroir.replicas > 1`, the Helm chart's `values.schema.json` rejects `backend: local` at install time, forcing Redis-backed shared state. The Redis bucket keys live under `redis_key_prefix` with `redis_ttl_s` TTL so memory stays bounded even under scan/spray attacks.

**Filter injection in `oauth_proxy` mode.** When `search_ui.auth.oauth_proxy.filter_template` is non-null, Miroir automatically injects the rendered filter into every `/indexes/{uid}/search` call made under the session, using Meilisearch filter-concatenation semantics (the rendered clause is ANDed with any user-supplied filter the SPA sends — `(<user filter>) AND (<injected filter>)`). The `{groups}` placeholder is replaced at session-mint time with the comma-joined values of the `groups_header` (properly JSON-array-escaped for the Meilisearch filter DSL, e.g. `["engineering","ops"]`); the rendered claim is baked into the JWT so every subsequent search under that session enforces the same filter without re-reading proxy headers. `{user}` is replaced with the `user_header` value. `attribute_map` lets operators rename the JWT claim keys exposed to observability tooling and to the client (read-only — the SPA cannot override them). When `filter_template` is null, Miroir performs no filter injection — operators relying on a reverse proxy or downstream filter-at-app layer for access control must enforce it themselves; Miroir still forwards the user/group claims as read-only JWT claims for observability but does not gate results on them.

**Metrics.** `miroir_search_ui_sessions_total`, `miroir_search_ui_queries_total{index}`, `miroir_search_ui_zero_hits_total{index}`, `miroir_search_ui_click_through_total{index}` (when analytics enabled), `miroir_search_ui_p95_ms{index}` (end-user-observed latency, measured via client beacon).

---

## 14. Resource Envelope and Horizontal Scaling

Miroir is designed to fit within a fixed per-pod resource envelope. If aggregate workload exceeds what one pod can serve, Miroir scales **out**, not up — the orchestrator is stateless on the request path and partitions background work across pods by explicit design.

### 14.1 Target envelope

Each orchestrator pod is sized for:

- **2 vCPU**
- **3.75 GB RAM**

This matches common small-instance tiers (AWS t3.medium, GCP e2-medium, Hetzner CX22, Rackspace Spot 2c/3.75GB). Meilisearch nodes are sized independently per Section 6 and are unaffected by this envelope.

### 14.2 Per-pod memory budget

Steady-state allocation at default config, assuming all advanced capabilities enabled:

| Component | Budget | Primary knob |
|-----------|--------|--------------|
| Rust/Tokio runtime + axum baseline | 80 MB | — |
| HTTP/2 connection pools to nodes | 50 MB | `connection_pool_per_node` |
| Request/response buffers (p99 concurrent) | 200 MB | `server.max_body_bytes`, `max_concurrent_requests` |
| Task registry cache | 100 MB | `task_registry.cache_size` |
| Idempotency cache (§13.10) | 100 MB | `idempotency.max_cached_keys` |
| Session pinning state (§13.6) | 50 MB | `session_pinning.max_sessions` |
| Query coalescing in-flight (§13.10) | 50 MB | `query_coalescing.max_subscribers` |
| Router + EWMA state (§13.3) | 20 MB | fixed per node |
| Query plan cache (§13.4) | 20 MB | fixed |
| Alias table (§13.7) | 10 MB | fixed |
| Metrics registry (prometheus) | 50 MB | fixed |
| Dump import buffer (§13.9, only while importing) | 128 MB | `dump_import.memory_buffer_bytes` |
| Anti-entropy fingerprinter (§13.8, only during pass) | 128 MB | `anti_entropy.max_read_concurrency` × batch |
| Multi-search batch state (§13.11) | ~5 MB | `multi_search.max_queries_per_batch` (per-request cache) |
| Vector search over-fetch scratch (§13.12) | ~30 MB | `vector_search.over_fetch_factor` (larger result buffers during merge) |
| CDC in-memory batch buffer (§13.13, only when CDC enabled) | 64 MB | `cdc.buffer.memory_bytes` |
| TTL sweeper state (§13.14) | ~5 MB | cursor per owned shard (negligible) |
| Tenant map LRU (§13.15, only when `mode: api_key`) | ~20 MB | `tenant_affinity.mode` |
| Shadow tee in-flight duplicates (§13.16) | varies (~50 MB worst case at 5% sample) | `shadow.targets[].sample_rate` × avg_response_size × concurrency |
| Rollover policy evaluator (§13.17) | negligible | fixed |
| Canary runner state (§13.18) | ~20 MB | `canary_runner.run_history_per_canary` (golden responses + run history LRU) |
| Admin UI embedded assets (§13.19) | ~10 MB | fixed (rust-embed compressed + decompressed working set) |
| Explain plan cache (§13.20) | ~10 MB | fixed |
| Search UI embedded assets (§13.21) | ~10 MB | fixed |
| Search UI rate-limiter state (§13.21) | ~20 MB (Redis-backed when `replicas > 1`) | `search_ui.rate_limit.per_ip` / `search_ui.rate_limit.backend` (IP buckets; see §13.21 rate-limiter shared-state note and §14.7 Redis memory accounting) |
| Allocator overhead + headroom for bursts | 800 MB | — |
| **Steady-state total (idle background)** | **~1.2 GB** | |
| **Total with one heavy background job active** | **~1.7 GB** | |

With all new §13.11–21 features enabled simultaneously, steady-state budget rises by roughly **200 MB** versus the pre-§13.11 baseline, staying within the 3.75 GB envelope but tightening the burst headroom. Operators can disable individual capabilities (see §13 preamble — every `enabled: true` knob can be flipped off) to reclaim memory on constrained deployments. Remaining ~2 GB of the 3.75 GB envelope absorbs request concurrency spikes and GC/allocator behavior. Heavy background jobs (dump import, reshard backfill) only run on pods that have claimed them from the shared queue — not every pod simultaneously — so total budget stays within envelope even cluster-wide.

### 14.3 Per-pod CPU budget

| Path | Cost | Scaling |
|------|------|---------|
| Scatter fan-out dispatch | ~5% of 1 core per 1 kQPS | Linear in RG × RF |
| JSON parse + re-serialize | ~30% of 1 core per 1 kQPS of 10 KB responses | Streaming bodies where possible |
| Global result merge | ~5% (min-heap; O(n log k)) | Dominated by hit count |
| Rendezvous hash per doc | <1% | Tiny xxhash cost |
| Filter DSL parse (§13.4) | <2% | Plan cached per (normalized filter, index) |
| Anti-entropy fingerprint (§13.8) | self-throttled to <2% | Configurable |
| Two-phase settings verify (§13.5) | negligible | Runs on settings change only |

Per-pod peak throughput: **~3 kQPS for small searches (1 KB responses)** or **~1 kQPS for large searches (10 KB responses)** at 70% CPU utilization. Beyond that, add pods.

### 14.4 Horizontal scaling: request path

The request path is strictly stateless. Any pod handles any request. Routing is deterministic from topology + config — every pod independently computes the same rendezvous assignment.

Kubernetes `Service` (ClusterIP) load-balances across pods; client affinity is not required. HPA scales the Deployment:

The HPA is only installed when `miroir.hpa.enabled: true` (default `false` in the dev chart values). Enabling the HPA requires `miroir.replicas >= 2` and `taskStore.backend: redis` — the chart's `values.schema.json` rejects configurations that violate either precondition, since SQLite is single-writer and an HPA that could scale to zero or one pod would defeat the production HA story.

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: miroir
  namespace: search
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: miroir
  minReplicas: 2
  maxReplicas: 24
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
    scaleUp:
      stabilizationWindowSeconds: 30
  metrics:
  - type: Resource
    resource:
      name: cpu
      target: { type: Utilization, averageUtilization: 70 }
  - type: Resource
    resource:
      name: memory
      target: { type: Utilization, averageUtilization: 75 }
  - type: Pods
    pods:
      metric:
        name: miroir_requests_in_flight
      target: { type: AverageValue, averageValue: "500" }
  - type: External
    external:
      metric:
        name: miroir_background_queue_depth
        selector:
          matchLabels:
            service: miroir
      target: { type: Value, value: "10" }
```

Adjust `minReplicas`/`maxReplicas` per the workload tier (§14.7). Metric-type choice matters:

- `miroir_requests_in_flight` is **per-pod** (each pod reports its own in-flight count) and uses `type: Pods` with `AverageValue` — HPA averages across pods and scales to keep the per-pod average at target.
- `miroir_background_queue_depth` is **global** (every pod reads the same value from the shared Redis `miroir:jobs:_queued` set) and therefore must use `type: External` with `type: Value` — dividing the single global backlog by a per-replica work-unit target. Using `type: Pods`/`AverageValue` here would scale monotonically to `maxReplicas` whenever any backlog exists, because the average never drops as pods are added.

Exposing both metrics to the HPA requires `prometheus-adapter` (or equivalent custom-/external-metrics API bridge), which is a chart prerequisite when the HPA is enabled.

With `replicas >= 2`, the task store backend **must** be Redis (Section 4 `task_store.backend: redis`). SQLite is single-writer and cannot be shared. The Helm chart enforces this: `taskStore.backend=sqlite` with `miroir.replicas > 1` fails values-schema validation.

### 14.5 Horizontal scaling: background work

Background work must be partitioned, never duplicated across pods. Three coordination modes cover all background tasks:

**Mode A — Shard-partitioned ownership (parallel by shard).**

Each pod owns a subset of shards for background processing. Assignment uses rendezvous hashing over the current peer set:

```
peers      = discover_peers()           // headless-Service DNS lookup
owns(s, p) = p == top1_by_score(hash(s || pid) for pid in peers)
```

Applies to:
- Anti-entropy reconciler (§13.8) — each pod fingerprints and repairs the shards it owns
- Settings drift check (§13.5) — each pod polls a subset of (index, node) settings-hash pairs
- Task registry pruner — each pod prunes tasks where it wins the rendezvous score for the task's `miroir_id`: `top1_by_score(hash(miroir_id || pid) for pid in peers) == self_pid`. Matches anti-entropy / drift-check partitioning; minimal reshuffling on scale events.

When the peer set changes (scale event, pod restart), rendezvous redistributes ownership with minimal reshuffling. No explicit handoff — the new owner runs the next scheduled pass. Transient double-work during a 15-second discovery window is harmless: anti-entropy is idempotent, settings-repair is idempotent.

**Mode B — Leader-only (singleton coordinator).**

Exactly one pod holds the leader lease at a time, backed by a lease row in the task store (Redis `SET NX EX 10` renewed every 3s; SQLite advisory lock for single-replica deployments). The leader runs operations that must be singleton or atomic:

- Reshard coordinator (§13.1) — one reshard per index at a time; cutover is atomic
- Rebalancer (existing Section 4) — already uses advisory lock
- Alias flip serializer (§13.7) — atomic per alias
- Two-phase settings broadcast (§13.5) — one settings change in flight per index

Leader loss mid-operation causes a pause; the new leader reads the persisted phase state from the task store and resumes from the last committed phase. All operations are idempotent by design and safe to resume at any phase boundary.

**Mode C — Work-queued (streaming jobs that outgrow a single pod).**

Heavy streaming operations — large dump imports (§13.9), large reshard backfills (§13.1) — can exceed a single pod's envelope. They are submitted as **jobs** to a queue in the task store; any pod claims a job with lease-based ownership:

```
job = {
    id: uuid,
    type: "dump_import" | "reshard_backfill",
    params: { ... },
    state: "queued" | "in_progress" | "completed" | "failed",
    claimed_by: pod_id | null,
    claim_expires_at: ts,              // heartbeat every 10s, timeout 30s
    progress: { bytes_processed, docs_routed, last_cursor, ... },
}
```

A large dump import is **split into chunks** on NDJSON line boundaries by the first pod that picks it up; chunks are re-enqueued as independent jobs. Each chunk is bounded by `dump_import.chunk_size_bytes` (default 256 MiB) so one chunk fits a pod's buffer. HPA reacts to queue depth: if `miroir_background_queue_depth > 0` and pods are at capacity, add pods; once queue drains, scale back down.

Reshard backfill partitions by shard-id range and uses the same chunked-job mechanism. Progress cursors are persisted per chunk so a crashed claim resumes at the last committed offset (idempotent via primary keys).

**Peer discovery.** All three modes rely on the current peer set. Mechanism:

- Kubernetes Downward API injects `POD_NAME` and `POD_IP` as env vars
- A headless Service (`miroir-headless`) with label selector on the Deployment exposes pod IPs via DNS SRV records
- Each pod refreshes its peer set every 15s via SRV lookup
- No external service registry required; no Kubernetes API calls from the pod

### 14.6 Per-feature scaling behavior

| Capability | Scaling mode | Notes |
|------------|-------------|-------|
| §13.1 Online resharding | B (leader) + C (backfill queue) | Leader owns phase state machine; any pod consumes backfill chunks. Bounded-memory backfill via paginated `filter=_miroir_shard={id}`. |
| §13.2 Hedged requests | stateless per-request | No coordination needed — each pod hedges its own requests. |
| §13.3 Adaptive replica selection | per-pod EWMA | Each pod's scores are local; pods converge independently. Slight divergence is harmless. |
| §13.4 Shard-aware query planner | per-request | Pure function of filter. Plan cache is per-pod. |
| §13.5 Two-phase settings broadcast | B (leader) | Leader issues PATCH and verifies. Drift reconciler runs in mode A. |
| §13.6 Session pinning | shared-state per-pod cache | Session row lives in task store (Redis); per-pod LRU caches it. Any pod can serve a session. |
| §13.7 Atomic index aliases | shared state | Alias table in task store. All pods read same table with short TTL cache. |
| §13.8 Anti-entropy reconciler | A (shard-partitioned) | Each pod fingerprints its owned shards. Naturally horizontal. |
| §13.9 Streaming dump import | C (chunked jobs) | 500 GB dump → chunks → pods consume from queue; HPA scales on queue depth. |
| §13.10 Idempotency + coalescing | per-pod + shared fallback | Idempotency cache per-pod with task-store lookup on miss — a retry on a different pod still dedups. Coalescing is per-pod only (acceptable: identical concurrent queries hitting different pods each issue their own scatter, which is rare and bounded by pod count). |
| §13.11 Multi-search | stateless per-request | Sub-queries fan out using existing scatter infrastructure; each sub-query is independently routed. |
| §13.12 Vector / hybrid search | stateless per-request | Merger uses more memory per request (see §14.2 vector over-fetch scratch row); no cross-pod coordination. |
| §13.13 CDC publisher | per-pod publishers with shared cursors | `cdc_cursors` in the task store serialize cursor advancement via compare-and-swap; each pod publishes its own shard of events. Overflow buffer in Redis is shared across pods. |
| §13.14 TTL sweeper | A (shard-partitioned) | Each pod sweeps only its rendezvous-owned shards; no duplicate deletes across pods. |
| §13.15 Tenant affinity | stateless per-request | Hash-or-explicit routing decision; no shared state on the hot path (tenant map LRU is per-pod). |
| §13.16 Shadow tee | stateless per-request | Each pod independently decides (per its local `sample_rate` RNG) whether to shadow a given request. |
| §13.17 ILM rollover | B (leader-only) | Serialized alias flips + index create/delete; exactly one pod runs the daily policy evaluator at a time. |
| §13.18 Canary runner | A (shard-partitioned) | Each canary ID is rendezvous-owned by exactly one pod per interval; no duplicate canary runs. |
| §13.19 Admin UI | per-pod | Any pod serves the SPA; stateful sections read the shared task store. |
| §13.20 Explain API | stateless per-request | Pure function of request + topology + config; no cross-pod coordination. |
| §13.21 Search UI | per-pod (SPA + static assets); rate limiter needs shared state | Any pod serves the SPA. **Rate-limiter requirement:** multi-pod deployments MUST set `search_ui.rate_limit.backend: redis` — `values.schema.json` rejects `backend: local` when `miroir.replicas > 1`. With `backend: local`, the effective cluster-wide rate is `per_ip × pod_count` because each pod counts independently. Redis rate-limit bucket memory sized at ~20 MB per 10k active IPs (§14.7). |

**Note.** TTL sweeper (§13.14), CDC publisher (§13.13), and canary runner (§13.18) are all Mode-A partitioned across pods; each pod consumes only its share of the workload, so the §14.2 memory rows for these features scale with `1/pod_count`.

### 14.7 Revised deployment sizing matrix

Each row gives the orchestrator pod count (2 vCPU / 3.75 GB each) for the stated workload. Meilisearch node sizing follows Section 6 independently.

| Corpus | Peak QPS | Orchestrator pods | Task store |
|--------|----------|-------------------|------------|
| ≤ 10 GB | ≤ 500 | 2 (HA) | Redis (or SQLite if replicas=1) |
| ≤ 50 GB | ≤ 2 k | 2–4 (HPA) | Redis |
| ≤ 200 GB | ≤ 5 k | 4–8 (HPA) | Redis |
| ≤ 1 TB | ≤ 20 k | 8–12 (HPA) | Redis |
| ≤ 5 TB | ≤ 100 k | 12–24 (HPA) | Redis (clustered or Sentinel) |

Orchestrator count scales with query throughput; Meilisearch node count scales with corpus size and RG/RF. They are orthogonal.

**Task-store / Redis memory accounting.** When Redis is the task store, it also backs shared state for idempotency replay keys, session pinning rows, alias cache, background job queue, leader lease, CDC overflow buffer (when configured), and — new in §13.21 — search UI rate-limit buckets. Add **~20 MB for search UI rate-limit buckets at 10k active IPs** on top of the task-store baseline when `search_ui.rate_limit.backend: redis`. Bucket rows auto-expire per `redis_ttl_s` so the footprint stays bounded under IP-scan/spray attacks.

### 14.8 Resource-aware configuration defaults

Every resource-sensitive knob has a default sized for the 2 vCPU / 3.75 GB envelope:

```yaml
miroir:
  server:
    max_body_bytes: 104857600          # 100 MiB per request
    max_concurrent_requests: 500
    request_timeout_ms: 30000

  connection_pool_per_node:
    max_idle: 32
    max_total: 128
    idle_timeout_s: 60

  task_registry:
    cache_size: 10000
    redis_pool_max: 50

  idempotency:
    max_cached_keys: 1000000           # ~100 MB
    ttl_seconds: 86400

  session_pinning:
    max_sessions: 100000               # ~50 MB

  query_coalescing:
    max_subscribers: 1000
    max_pending_queries: 10000

  # dump_import: see §13.9 for the full schema. Key horizontal-scaling knob
  # surfaced here is chunk_size_bytes (256 MiB, fits one pod's buffer) — the
  # chunk-parallel coordinator in §14.5 Mode C shards work at this granularity.

  anti_entropy:
    max_read_concurrency: 2
    fingerprint_batch_size: 1000

  resharding:
    backfill_concurrency: 4
    backfill_batch_size: 1000

  peer_discovery:
    service_name: "miroir-headless"
    refresh_interval_s: 15

  leader_election:
    enabled: true                      # auto-true when replicas > 1
    lease_ttl_s: 10
    renew_interval_s: 3
```

Kubernetes pod resource requests/limits:

```yaml
resources:
  requests:
    cpu: "500m"
    memory: "1Gi"
  limits:
    cpu: "2000m"
    memory: "3584Mi"                   # 3.5 GiB (leaves headroom under 3.75 GB node limit)
```

### 14.9 Resource-pressure metrics and alerts

New metrics to surface when a pod is near its envelope or the cluster is under-scaled:

```
miroir_memory_pressure             gauge  0=ok, 1=warn (>75%), 2=critical (>90%)
miroir_cpu_throttled_seconds_total counter  cgroup CPU throttling
miroir_request_queue_depth         gauge  pending requests
miroir_background_queue_depth{job_type}  gauge  per job type
miroir_peer_pod_count              gauge  observed peer set size
miroir_leader                      gauge  1 if this pod is leader, 0 otherwise
miroir_owned_shards_count          gauge  shards this pod owns (mode A)
```

Alerts:

```yaml
- alert: MiroirMemoryPressure
  expr: miroir_memory_pressure >= 2
  for: 5m
  annotations: { summary: "Pod near memory limit; HPA should scale" }

- alert: MiroirRequestQueueBacklog
  expr: miroir_request_queue_depth > 500
  for: 2m
  annotations: { summary: "Request queue backlog — add pods" }

- alert: MiroirBackgroundJobBacklog
  expr: miroir_background_queue_depth > 100
  for: 10m
  annotations: { summary: "Heavy background jobs queued — HPA should react" }

- alert: MiroirPeerDiscoveryGap
  expr: miroir_peer_pod_count < kube_deployment_status_replicas_ready{deployment="miroir"}
  for: 2m
  annotations: { summary: "Pods failing peer discovery — background work uneven" }

- alert: MiroirNoLeader
  expr: sum(miroir_leader) == 0
  for: 1m
  annotations: { summary: "No pod holds leader lease — background coordination stalled" }
```

### 14.10 Vertical scaling escape valve

The design target is 2 vCPU / 3.75 GB per pod. For edge cases (dev clusters, very small deployments, constrained environments), an operator **may** provision a single pod at a higher limit (e.g. 4 vCPU / 8 GB). All memory budgets scale linearly by multiplier and HPA may remain disabled.

This is supported but **not** the recommended production topology — horizontal scaling delivers better fault tolerance (zero-downtime rollouts, pod-loss survival) and avoids the all-eggs-one-basket risk of a single large pod. Single-large-pod mode is documented for completeness, not promoted as an equivalent option.

---

## 15. Open Problems

These are documented constraints, not blockers. Initial release ships with known limitations.

1. **Shard migration write safety** — Dual-write during migration must not lose documents that arrive exactly at the migration cutover boundary. Requires careful sequencing: new writes go to both old and new nodes until the new node is confirmed complete, then old node stops receiving those shards. Race condition analysis needed before implementation. **Status:** Verified empirically via chaos tests (`cutover_race.rs`). Loss rate: 0/1M writes with AE on + delta pass on; 0/50K with delta pass alone. Hard refusal policy blocks `skip_delta_pass + anti_entropy_enabled=false`. See `docs/trade-offs.md` for full results and rationale.

2. **Task state HA** — SQLite is single-writer. Running 2 Miroir replicas requires Redis. A future enhancement is a lightweight Raft-based in-process consensus so Redis is not required for HA mode.

3. **Resharding (S change) vs. node scaling (N change)** — These are distinct operations with very different costs. Adding or removing nodes (changing N) is always supported without a full reindex; rendezvous hashing plus the `_miroir_shard` filter migration moves only the affected fraction of documents. Increasing the logical shard count (changing S) is a different matter: it changes the hash function output, invalidating every document's current shard assignment, and requires a full reindex. Operators must choose S generously at index creation (`S = max_nodes_per_group_ever × 8`, per Section 2 — groups are independent and each group's rendezvous assignment is scoped to its own node list) to avoid ever needing to reshard. A cluster that starts at 2 nodes per group and grows to 60 nodes per group never needs to reshard, as long as S was set to ≥ 480 at creation (or 512 for a round power of two). Node fleet elasticity is unlimited within the chosen S, and adding replica groups never consumes S headroom. **Status:** Online resharding path now exists — see §13.1 (shadow-index dual-hash). The "choose S generously" guidance remains the recommended default because online resharding doubles transient storage and write load; treat §13.1 as a remediation, not a license to under-provision.

4. **Score normalization at scale** — `_rankingScore` is comparable across shards only when index settings are identical. Testing at scale with diverse document distributions is needed to validate that scores remain comparable in practice (e.g., do documents on a shard with very few matching results receive inflated scores?). **Status:** Settings-divergence risk is addressed by the two-phase broadcast and drift reconciler (§13.5). The open concern that remains is purely statistical — whether scores are comparable across shards with very different document-count distributions. Requires empirical validation at scale.

5. **Dump import distribution** — Importing a Meilisearch dump via Miroir historically broadcast all documents to all nodes, transiently placing 100% of documents on every node. **Status:** Addressed by streaming routed dump import (§13.9); documents are routed to owning nodes on the fly, never broadcast. Broadcast mode retained as a fallback for dump variants Miroir cannot fully reconstruct via the public API.

6. **arm64 support** — Not planned for v0.x. Added when K8s ARM node support is required.
