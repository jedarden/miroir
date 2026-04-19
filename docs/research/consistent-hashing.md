# Consistent Hashing Algorithms — Research for Miroir

Miroir must assign documents to nodes deterministically, support RF≥1 replicas per document (top-K nodes in the ordering), and handle dynamic topology (nodes added/removed at arbitrary positions). This document evaluates four consistent hashing algorithms against that use case.

---

## Problem Statement

Given:
- A set of N Meilisearch nodes
- A document primary key (string)
- A replication factor RF (integer ≥ 1)

Required:
- A deterministic function `nodes(key, topology) → [n₁, n₂, ..., n_RF]` — an ordered list of RF nodes that own the document
- When a node is added, minimize the fraction of keys that reassign
- When a node is removed, minimize the fraction of keys that reassign
- Support arbitrary removal (any node can fail, not just the last one added)
- Bounded lookup time acceptable; O(N) per key is acceptable for N ≤ 100

---

## 1. Rendezvous Hashing (HRW — Highest Random Weight)

**Origin**: Thaler & Ravishankar, University of Michigan, 1996/1998.

### Algorithm

For each key `k` and each node `n_i`:

```
weight(k, n_i) = hash(k ‖ n_i)
```

Assign the key to the node with the highest weight. For RF replicas, take the top-RF nodes by weight. The ordering is total and deterministic given the same node set.

```python
def assign(key, nodes, rf=1):
    scored = [(hash(key + node_id), node_id) for node_id in nodes]
    scored.sort(reverse=True)
    return [node_id for _, node_id in scored[:rf]]
```

The hash function is typically a fast, non-cryptographic hash applied to the concatenation (or tuple) of key and node identifier. `xxhash`, `SipHash`, and `MurmurHash3` are common choices. Quickwit uses `twox-hash` (xxHash family); Meilisearch's own sharding implementation uses `twox-hash` over the document primary key.

### Properties

**Lookup complexity**: O(N) — must evaluate all nodes to find the maximum. Negligible for N ≤ a few thousand; for N = 50 nodes this is ~50 hash operations per document assignment.

**Memory**: O(1) state — no data structure, just the list of node identifiers.

**Load distribution**: Perfectly uniform in expectation (each node receives 1/N of keys). No tuning required; no virtual nodes needed.

**Reshuffling on node add**: When a new node `n_new` is added, exactly the keys for which `weight(k, n_new)` is the maximum reassign. In expectation: 1/(N+1) of all keys. All other key assignments are unchanged — only the added node "steals" keys from existing nodes.

**Reshuffling on node remove**: When node `n_fail` is removed, only the keys that were assigned to `n_fail` are reassigned. In expectation: 1/N of all keys. Each affected key reassigns to whichever remaining node had the next-highest weight — already determined without any recomputation.

**Arbitrary removal**: Fully supported. Any node can be removed at any time. There is no structural constraint on removal order.

**Replica placement**: Natural extension — sort all nodes by weight, take top RF. The set of replicas changes minimally when the node list changes (only if the removed/added node was within the top-RF for a given key).

**Stability guarantee**: HRW achieves **optimal disruption** — the minimum theoretically possible number of key reassignments for any single node membership change.

### Implementation Complexity

Low. The algorithm is 5–10 lines of code with no special data structures. The only operational consideration is ensuring all nodes use the same node identifier string format (e.g., `"host:port"` or a stable UUID) — a mismatch causes split-brain document placement.

### Used In

- Quickwit (split-to-searcher assignment, stated in architecture docs)
- Meilisearch Enterprise sharding (rendezvous hashing with twox-hash on primary key)
- Apache Ignite (data distribution)
- Some CDN routing systems

---

## 2. Ring Consistent Hashing (Karger et al.)

**Origin**: Karger, Lehman, Leighton, Panigrahy, Levine, Lewin — MIT, 1997. "Consistent Hashing and Random Trees."

### Algorithm

Map both keys and nodes onto a circular hash ring [0, 2³²-1]. Each node is placed at one or more points on the ring (virtual nodes / vnodes). For a key, find the first node clockwise from `hash(key)`.

```python
# Build ring
ring = SortedDict()
for node in nodes:
    for v in range(vnodes_per_node):
        ring[hash(f"{node}:{v}")] = node

# Lookup
def assign(key):
    h = hash(key)
    idx = ring.bisect_right(h) % len(ring)
    return ring.peekitem(idx)[1]
```

For RF replicas: walk clockwise, collecting the next RF distinct physical nodes.

### Properties

**Lookup complexity**: O(log V) where V = total virtual nodes = N × vnodes_per_node. Typical: N=50 nodes, 150 vnodes each → 7500 ring positions → ~13 comparisons.

**Memory**: O(V) — must store the full sorted ring in memory. At 150 vnodes/node and 1000 nodes: ~600K entries at ~40 bytes each ≈ 24MB. Acceptable.

**Load distribution**: Depends critically on vnode count. With 150 vnodes/node, empirical standard deviation is approximately 10%. With 1000 vnodes/node, ~3.2%. Without vnodes (single point per node), load variance is extreme.

**Reshuffling on node add**: A new node claims keys between itself and the previous node on the ring. With vnodes, the affected arcs are scattered, so load is taken from many existing nodes proportionally. Expected reassignment: 1/(N+1) of keys (same as HRW in expectation).

**Reshuffling on node remove**: Keys from removed node's arcs reassign to the next node clockwise. Expected reassignment: 1/N of keys.

**Arbitrary removal**: Supported — remove any virtual node positions, remainder of ring adjusts.

**Replica placement**: Walk clockwise, skip duplicate physical nodes, collect RF. Implementation is slightly more complex than HRW: must ensure physical node deduplication.

### Tradeoffs vs. HRW

Ring hashing has higher implementation complexity (sorted ring data structure, vnode tuning, replica deduplication logic) and requires O(N × vnodes) memory. Load distribution is worse than HRW unless vnodes are set very high. The O(log V) lookup is faster than HRW's O(N) when N is very large (thousands of nodes), but for Miroir's expected scale (N ≤ 50) this advantage is irrelevant.

Ring hashing's main advantage is ecosystem familiarity and wide library availability (consistent-hash in every language).

### Used In

Amazon DynamoDB, Apache Cassandra, Riak, memcached, many CDNs.

---

## 3. Jump Consistent Hash (Google)

**Origin**: Lamping & Veach, Google, 2014. "A Fast, Minimal Memory, Consistent Hash Algorithm."

### Algorithm

Maps a 64-bit key to a bucket in [0, N) using a mathematical recurrence. No data structure; the result is a shard number (integer), not a node identifier.

```python
def jump_hash(key: int, num_buckets: int) -> int:
    b, j = -1, 0
    while j < num_buckets:
        b = j
        key = (key * 2862933555777941757 + 1) & 0xFFFFFFFFFFFFFFFF
        j = int((b + 1) * (1 << 31) / ((key >> 33) + 1))
    return b
```

### Properties

**Lookup complexity**: O(ln N) — the loop iterates approximately ln(N) times.

**Memory**: O(1) — no data structure. Fits in a few CPU registers.

**Load distribution**: Near-perfect. Standard deviation: 0.000000764%. The theoretical optimum.

**Reshuffling on node add**: Only 1/(N+1) of keys reassign. Only new-to-existing reassignments occur.

**Reshuffling on node remove**: **Critical limitation — arbitrary node removal is not supported.** Jump hash only permits removing the highest-numbered bucket. If node 3 of [0,1,2,3,4] fails, you cannot simply remove bucket 3; all keys in buckets 3 and 4 would be incorrectly routed. An indirection layer (bucket number → node) is required to handle arbitrary failure, which negates some of the simplicity advantage.

**Arbitrary removal**: Not natively supported. Requires an external indirection layer mapping bucket IDs to physical node IDs; when a node fails, its bucket must be remapped. This layer reintroduces the state management that jump hash was designed to avoid.

**Replica placement**: Cannot natively produce a top-RF ranking over arbitrary node sets. Would require calling jump_hash with modified bucket counts, which produces inconsistent replica sets across topology changes.

### Assessment for Miroir

Jump hash is unsuitable for Miroir in its pure form. Meilisearch nodes are heterogeneous infrastructure (Kubernetes pods, VMs) that can fail at arbitrary positions in the node set. The indirection layer required to support arbitrary removal essentially recreates a ring-like data structure. The perfect load distribution is appealing but does not compensate for the operational complexity.

### Used In

Google internal shard selection (with indirection layer), some data pipeline systems where bucket count is append-only.

---

## 4. Maglev Hashing (Google)

**Origin**: Eisenbud et al., Google, 2016. "Maglev: A Fast and Reliable Software Network Load Balancer."

### Algorithm

Precomputes a lookup table of size M (a large prime, e.g., 65537). Each backend generates a permutation of table positions using two hash functions. Backends fill the table round-robin according to their permutations. Lookup: `table[hash(key) % M]`.

```python
def build_table(backends, M=65537):
    # each backend has a permutation of [0..M)
    # fill table greedily
    table = [-1] * M
    next_pos = [0] * len(backends)
    perms = [compute_permutation(b, M) for b in backends]
    filled = 0
    while filled < M:
        for i, b in enumerate(backends):
            c = perms[i][next_pos[i]]
            next_pos[i] += 1
            while table[c] != -1:
                c = perms[i][next_pos[i]]
                next_pos[i] += 1
            table[c] = i
            filled += 1
            if filled == M:
                break
    return table
```

### Properties

**Lookup complexity**: O(1) — single hash + table lookup.

**Memory**: O(M) — lookup table of M integers. At M=65537 and 4 bytes/entry: ~256KB.

**Load distribution**: Even, with absolute imbalance ≤ 1 slot per backend pair.

**Reshuffling on node add**: Requires full table rebuild. The new backend takes over approximately M/N slots; the disruption is close to optimal but the rebuild cost is O(M × N).

**Reshuffling on node remove**: Also requires full table rebuild. Disruption is approximately minimal, but rebuild is O(M × N). For M=65537, N=50: ~3.3M operations per topology change — milliseconds on modern hardware, but a synchronous operation.

**Arbitrary removal**: Supported after table rebuild.

**Replica placement**: Not natively designed for RF > 1. Would require multiple independent tables or a secondary lookup scheme.

### Assessment for Miroir

Maglev is optimized for packet-rate load balancers where O(1) lookup at millions of lookups/second justifies the table complexity. Miroir's lookup rate is bounded by document ingestion and query rate — far below the threshold where O(N) HRW becomes a bottleneck. The added complexity (table rebuild on topology change, RF > 1 not supported natively) is not justified.

---

## Comparison Matrix

| Property | Rendezvous (HRW) | Ring (Karger) | Jump Hash | Maglev |
|----------|-----------------|---------------|-----------|--------|
| Lookup complexity | O(N) | O(log V) | O(ln N) | O(1) |
| Memory overhead | Minimal (node list only) | O(N × vnodes) | None | O(M) table |
| Load uniformity | Optimal | Good (tunable) | Near-perfect | Good |
| Node add disruption | Optimal (1/N+1) | Optimal | Optimal | Near-optimal |
| Node remove disruption | Optimal (1/N) | Optimal | Not supported | Near-optimal |
| Arbitrary remove | Yes | Yes | No | Yes (after rebuild) |
| RF > 1 replicas | Native (top-K sort) | Native (ring walk) | No | No |
| Implementation complexity | Very low | Medium | Very low | High |
| Topology change cost | O(1) (no data structure) | O(vnodes) sort insert/delete | N/A | O(M × N) rebuild |

---

## Recommendation for Miroir

**Use Rendezvous Hashing (HRW) with xxHash (twox-hash or xxHash3).**

Rationale:

1. **RF support is native**: Sorting all nodes by `hash(key ‖ node_id)` and taking the top RF is the canonical HRW extension. No additional mechanism needed. Miroir's primary requirement is RF ≥ 1 replica placement, and HRW provides this with zero extra complexity.

2. **Arbitrary topology changes**: Both node addition and arbitrary node removal are handled with optimal disruption. This is essential for a Kubernetes-hosted system where any node can be evicted.

3. **Correctness is trivial**: The algorithm has no data structures to corrupt or keep in sync across Miroir instances. Two Miroir orchestrators with the same list of node IDs will always agree on document placement.

4. **Scale is not a concern**: For N ≤ 100 nodes, O(N) per key lookup is ≤ 100 hash operations. At a write throughput of 10,000 docs/sec, that is 1M hash operations/sec — trivially fast on modern hardware.

5. **Precedent**: Quickwit uses rendezvous hashing for split-to-searcher assignment. Meilisearch Enterprise uses rendezvous hashing with twox-hash for document sharding. Both are directly analogous to Miroir's problem.

### Implementation Notes

- Use a stable, fast non-cryptographic hash: `xxhash3` (64-bit) or `SipHash-1-3`.
- Node identifier: use a stable string (e.g., `"host:port"` or a UUID assigned at cluster join) — never use an ordinal index, which would break on removal.
- Hash input: `hash(primary_key || node_id)` where `||` is concatenation or a length-prefixed encoding to avoid collisions (e.g., `hash(len(primary_key) as u16 + primary_key + node_id)`).
- RF = 1: primary assignment only.
- RF = 2: top 2 nodes → primary + 1 replica.
- RF = 3: top 3 nodes. For writes, fan out to all RF nodes; acknowledge when RF_write (configurable quorum, typically ceil(RF/2)+1) have confirmed.
- On topology change: recompute top-RF for each key in the rebalance plan. Keys where the new primary differs must be re-indexed to the new primary node. Keys where a replica position changes must be re-indexed to the new replica.

### Rebalance Scope Calculation

When adding node `n_new`:
```
affected_keys = {k : hash(k, n_new) > hash(k, current_primary(k))}
```
These keys must be migrated to `n_new` as their new primary. Expected size: |corpus| / (N+1).

When removing node `n_fail`:
```
affected_keys = {k : n_fail in top_RF(k, current_nodes)}
```
These keys must be re-indexed from their surviving replicas to the new node that fills the RF position. Expected size: |corpus| × RF / N.
