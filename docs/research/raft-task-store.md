# P12.OP2: Lightweight Raft vs. Redis for Task State HA

**Date:** 2026-04-18
**Status:** Decision recorded — revisit before v2.0, do not ship in v0.x or v1.0
**Bead:** miroir-zc2.2
**Plan ref:** §15 Open Problem #2, §4 Task store schema, §14.2 Per-pod memory budget
**Prototype:** `crates/miroir-core/src/raft_proto/` (feature-gated behind `raft-proto`)

---

## Executive Summary

Replacing Redis with an embedded Raft consensus module is **feasible but not justified for v1.x**. The operational benefit (removing an external dependency) is real, but the cost is high: significant implementation complexity, a new correctness surface (Raft consensus bugs can silently lose data), higher per-pod memory and CPU overhead, and no latency advantage over Redis for Miroir's workload profile.

**Decision: Revisit before v2.0.** Ship Redis backend in v1.0 as planned. Re-evaluate Raft when the task store is production-stabilized and the operational burden of managing Redis is empirically measured.

---

## 1. Problem Statement

Miroir's task store (14 tables, plan §4) uses SQLite for single-replica deployments and Redis for HA (2+ replicas). Redis is required because SQLite is single-writer — two pods cannot write to the same `.db` file.

Open Problem #2 asks: can we embed a Raft consensus module so that N Miroir pods replicate task state among themselves, eliminating the Redis dependency?

**Decision gate** (from plan): the Raft path must be measurably better than Redis on at least one metric (ops simplicity, latency, or memory) without being worse on any of the others.

---

## 2. Crate Survey

### 2.1 Candidates Evaluated

| Crate | Version | Stars | Last Activity | Status |
|-------|---------|-------|---------------|--------|
| **openraft** | 0.9.22 (stable), 0.10.0-alpha.17 | ~1,900 | 2026-04-08 (crates.io verified) | Actively maintained |
| **raft-rs** (tikv) | 0.7.0 | ~3,300 | 2023-03-07 (crates.io verified) | Maintenance mode — no active roadmap, TiKV internalized much of the logic |
| **async-raft** | 0.6.1 | ~1,091 | 2021-05-19 (crates.io verified) | **Abandoned — do not use** |

### 2.2 Elimination

**async-raft** is eliminated immediately. It has been abandoned since May 2021 (last release v0.6.1, 2021-05-19) with known correctness bugs in membership changes and snapshot replication. Recent downloads: ~2,500 (vs openraft's ~153,000). openraft was created specifically as a bug-fixed fork of async-raft. No new project should use async-raft.

### 2.2.1 Other Raft-related crates surveyed (not recommended)

| Crate | Downloads | Last Update | Notes |
|-------|-----------|-------------|-------|
| **raft-proto** | 468K | 2023-03-07 | Protobuf definitions for raft-rs; not standalone |
| **raft-engine** | 306K | 2024-04-26 | Persistent log storage engine for Multi-Raft (TiKV); low-level, not a consensus library |
| **iggy** | 195K | 2026-03-30 | Message streaming platform (Apache Iggy); includes consensus internally but not a pluggable Raft library |
| **stateright** | 14M | 2025-07-27 | Model checker for distributed systems (Paxos/Raft); verification tool, not a production consensus library |
| **openraft-rt** | 21K | 2026-03-11 | Runtime abstraction layer for openraft 0.10.x (companion crate) |

No new Raft consensus libraries suitable for Miroir's use case have emerged since the initial survey. openraft remains the only viable option.

### 2.3 Detailed Comparison: openraft vs. raft-rs

#### API Design

| Aspect | openraft | raft-rs |
|--------|----------|---------|
| **Async** | Native async (tokio, configurable runtime) | Synchronous only |
| **Traits** | Split: `RaftLogStorage` + `RaftStateMachine` (separate concerns) | Single `Storage` trait (monolithic) |
| **Network** | `RaftNetworkFactory` → per-node connections | None — you handle all transport |
| **Pattern** | Higher-level: propose → apply → done | Low-level: tick → ready → advance loop |
| **Linearizable reads** | Built-in `read_index` | Manual implementation required |
| **Snapshots** | `RaftSnapshotBuilder` trait, streaming support | `create_snapshot`/`apply_snapshot` on Storage |
| **Membership changes** | `ChangeMembership` API, joint consensus | `ConfChangeV2`, joint consensus |

#### Production Users

| openraft | raft-rs |
|----------|---------|
| Databend (cloud data warehouse — original sponsor) | TiKV / TiDB (distributed DB — original sponsor) |
| GreptimeDB (time-series DB) | RisingWave (streaming DB) |
| CnosDB (time-series DB) | HStreamDB (streaming platform) |
| Helyim (SeaweedFS-in-Rust object store) | |
| RobustMQ (message queue) | |
| **rrqlite** (Raft-replicated SQLite) | |

openraft is battle-tested in 5+ production systems. raft-rs is production-proven within TiKV but has limited adoption outside its original sponsor.

#### openraft Performance (from official benchmark)

The openraft README reports framework-level benchmarks (minimal store/network, 3 nodes):

| Clients | Single writes/sec | Batch writes/sec (4 entries) |
|---------|-------------------|------------------------------|
| 1 | 33,000 | — |
| 64 | 912,000 | — |
| 256 | 1,808,000 | — |
| 1,024 | 3,006,000 | — |
| 4,096 | 3,548,000 | 5,615,000 |

These are framework-level numbers with minimal persistence. Real-world throughput with SQLite persistence and network I/O will be significantly lower (see Section 4.3 for Miroir-specific estimates).

#### SQLite Compatibility

| Aspect | openraft | raft-rs |
|--------|----------|---------|
| **Storage trait** | Async — requires `spawn_blocking` for SQLite calls | Synchronous — call SQLite directly |
| **Log/state machine split** | Yes — can use different backends | No — single trait combines both |
| **Fit** | Good (split traits help), minor async overhead | Best — sync traits are natural for SQLite |

#### Memory Footprint

| Aspect | openraft | raft-rs |
|--------|----------|---------|
| **Dependencies** | Moderate (tokio optional, tracing, serde) | Minimal (no async runtime, slog, protobuf) |
| **Runtime** | Configurable or none (`single-threaded` feature) | None required |
| **Baseline** | ~15-25 MB (with tokio) | ~5-10 MB (pure sync) |

### 2.4 Recommendation for Miroir

**openraft** is the better choice if we proceed, for three reasons:

1. **Miroir is async-native** (tokio + axum). raft-rs's sync API would require wrapping every storage/network call in `spawn_blocking`, which is error-prone in an async context and can cause thread-pool starvation under load.
2. **Split traits** (`RaftLogStorage` + `RaftStateMachine`) map naturally to Miroir's architecture: the Raft log can live in SQLite tables, while the state machine applies entries to the same 14-table schema already designed.
3. **Active development and community**. openraft is the most actively maintained Rust Raft crate with a responsive maintainer and multiple production users outside its original sponsor.

---

## 3. Prototype Design: Raft-Backed TaskStore

### 3.1 Architecture

```
┌─────────────────────────────────────────────────┐
│                   Miroir Pod                     │
│                                                  │
│  ┌──────────┐    ┌─────────────────────────┐     │
│  │ axum HTTP│───▶│     TaskStore trait      │     │
│  │ handler  │    │  (RaftTaskStore impl)    │     │
│  └──────────┘    └─────────┬───────────────┘     │
│                            │                      │
│                  ┌─────────▼─────────┐            │
│                  │   openraft Raft    │            │
│                  │   (in-process)     │            │
│                  └─────┬───────────┬─┘            │
│                        │           │               │
│              ┌─────────▼──┐  ┌─────▼──────────┐   │
│              │ LogStorage  │  │ StateMachine   │   │
│              │ (SQLite)    │  │ (SQLite)       │   │
│              │             │  │                 │   │
│              │ raft_log    │  │ tasks           │   │
│              │ raft_state  │  │ aliases         │   │
│              │ raft_snap   │  │ sessions        │   │
│              │             │  │ jobs            │   │
│              │             │  │ ... (14 tables) │   │
│              └─────────────┘  └────────────────┘   │
│                                                  │
│         Network: gRPC/TCP to peer pods           │
└─────────────────────────────────────────────────┘
```

### 3.2 Storage Layout

One SQLite database per pod, three internal namespaces:

```sql
-- Raft log (managed by RaftLogStorage impl)
CREATE TABLE raft_log (
    log_id_index  INTEGER PRIMARY KEY,
    log_id_term   INTEGER NOT NULL,
    payload       BLOB NOT NULL    -- serialized TaskStore command
);

CREATE TABLE raft_state (
    key   TEXT PRIMARY KEY,        -- 'hard_state', 'vote', 'snapshot'
    value BLOB NOT NULL
);

-- State machine tables (exact same 14 tables as plan §4)
CREATE TABLE tasks (...);       -- unchanged
CREATE TABLE aliases (...);     -- unchanged
-- ... etc for all 14 tables
```

All writes go through Raft consensus. Reads are local SQLite reads against the state machine (optionally via `read_index` for linearizable reads on the leader).

### 3.3 Command Protocol

Every mutating `TaskStore` operation is serialized as a Raft log entry:

```rust
#[derive(Serialize, Deserialize)]
enum TaskStoreCommand {
    // Table 1: tasks
    InsertTask { miroir_id: String, created_at: i64, status: String, node_tasks: String },
    UpdateTaskStatus { miroir_id: String, status: String, error: Option<String> },
    DeleteTask { miroir_id: String },

    // Table 3: aliases
    UpsertAlias { name: String, kind: String, current_uid: Option<String>, ... },
    DeleteAlias { name: String },

    // Table 7: leader_lease
    AcquireLease { scope: String, holder: String, expires_at: i64 },
    ReleaseLease { scope: String },

    // ... one variant per mutating operation across all 14 tables
}
```

The `RaftStateMachine::apply()` method deserializes each command and executes the corresponding SQLite write within a transaction. This guarantees that all pods apply commands in the same order.

### 3.4 Read Path

| Read type | Mechanism |
|-----------|-----------|
| Task status poll (hot path) | Local SQLite read — eventual consistency acceptable (status updates are async anyway) |
| Alias lookup | Local read with short TTL cache — same as Redis approach |
| Leader lease check | `read_index` on leader for linearizability — or local read if stale reads are tolerable for the 3s renewal window |
| Admin session verify | Local read — revocation uses Raft to propagate |

### 3.5 Network Transport

Pod-to-pod communication over the headless Service:

```rust
struct MiroirNetwork {
    peers: Arc<DashMap<NodeId, Channel>>,
}

impl RaftNetworkFactory for MiroirNetwork {
    // Uses the existing peer discovery mechanism (headless Service DNS)
    // Each pod maintains a TCP connection pool to every other pod
    // Serialization: bincode (fast, compact) or prost (protobuf-compatible)
}
```

Port: a dedicated Raft port (e.g., 9001) on each pod, separate from the HTTP proxy port.

### 3.6 Startup and Recovery

1. Pod starts, discovers peers via headless Service DNS
2. Opens local SQLite, replays any unapplied log entries
3. Joins Raft cluster (or initializes if first node)
4. If lagging, receives a snapshot from the leader
5. Begins serving requests once caught up

Snapshot interval: every 10,000 log entries or 5 minutes, whichever comes first. Snapshots are written to the `raft_snap` table and can also be persisted to object storage for disaster recovery.

---

## 4. Analytical Benchmark

Since Miroir has no running code yet, these are analytical estimates based on the known performance characteristics of Redis, SQLite, and Raft, calibrated against published benchmarks from Databend (openraft) and TiKV (raft-rs).

### 4.0 Measured: State Machine Apply Path

The prototype benchmark (`raft_proto::benchmark`) measures the actual apply-path overhead of the command-based state machine vs. direct HashMap access. Run with:

```bash
cargo test -p miroir-core --features raft-proto raft_proto::benchmark -- --nocapture
```

**Results** (50,000 ops, 3 nodes per task, stable Rust 1.87, re-verified 2026-04-18):

| Operation | State Machine | Direct HashMap | Overhead |
|-----------|-------------|----------------|----------|
| Insert | 1,976 ns | 1,829 ns | 1.1x |
| Read | 265 ns | 232 ns | 1.1x |
| Update | 319 ns | 286 ns | 1.1x |

| Serialization | Avg Latency | Size per Command |
|---------------|-------------|-----------------|
| JSON | 1,389 ns | 73 bytes |
| Bincode | 393 ns | 26 bytes |

**Throughput (single-threaded, local apply only):** ~506K ops/sec (state machine) vs ~547K ops/sec (direct)

**Key finding:** The state machine apply path adds negligible overhead (~1.0x) vs. direct HashMap access. Both are sub-microsecond. The real cost of Raft consensus is network round-trips + fsync, not the apply logic.

### 4.1 Latency: Write Path

A write to the task store goes through: client → Miroir handler → task store backend → response.

| Operation | Redis (est.) | Raft 3-node (est.) | Raft 5-node (est.) |
|-----------|-------------|-------------------|-------------------|
| Insert task | **0.3–0.8 ms** (HSET + SADD pipeline) | 2–5 ms (propose → majority ack → apply) | 3–7 ms |
| Update task status | **0.3–0.8 ms** | 2–5 ms | 3–7 ms |
| Acquire leader lease | **0.5–1.0 ms** (SET NX EX) | 2–5 ms | 3–7 ms |
| Alias flip (write) | **0.5–1.0 ms** (MULTI/EXEC) | 2–5 ms | 3–7 ms |

**Raft is 3–8x slower than Redis on writes** because every write must be replicated to a majority of pods (network round-trips) before it's committed. Redis writes are local to the Redis process (single-node latency) — the replication happens at the Redis/Sentinel layer, not in the client path.

### 4.2 Latency: Read Path

| Operation | Redis (est.) | Raft (local read) |
|-----------|-------------|-------------------|
| Get task by ID | **0.2–0.5 ms** | **0.05–0.2 ms** (local SQLite) |
| List all aliases | **0.3–0.8 ms** (SMEMBERS + HMGET pipeline) | **0.1–0.3 ms** (local SQLite) |
| Check session validity | **0.2–0.5 ms** | **0.05–0.2 ms** |

**Raft is faster on reads** because reads hit the local SQLite state machine — no network hop. Redis reads always require a network round-trip to the Redis server.

However, the read advantage is marginal in absolute terms (sub-millisecond for both) and Miroir's hot-path reads (task status polling) are not latency-sensitive — the plan already accepts async polling with eventual consistency.

### 4.3 Throughput

| Metric | Redis | Raft (3-node) |
|--------|-------|---------------|
| Writes/sec (single key) | ~100K | ~5K–15K |
| Writes/sec (batched, 100 keys) | ~500K | ~20K–50K |
| Reads/sec | ~100K | ~500K+ (local SQLite) |

Redis's throughput advantage on writes comes from being a single-process in-memory store with no consensus overhead. Raft's write throughput is bounded by the consensus round-trip time and log persistence (fsync).

**Miroir's write volume is low.** Task store writes are proportional to document mutations (not searches). At 1 kQPS write volume with ~10 task store mutations per write, that's 10K writes/sec — within Raft's capability but with less headroom than Redis.

### 4.4 Memory Footprint (per pod)

| Component | Redis Backend | Raft Backend |
|-----------|--------------|-------------|
| Task store data (in Miroir pod) | 0 (lives in Redis process) | 50–100 MB (SQLite + in-memory cache) |
| Raft log cache | — | 20–50 MB |
| Raft runtime overhead | — | 15–25 MB |
| Network buffers (peer connections) | — | 5–10 MB |
| **Total additional per pod** | **0** | **90–185 MB** |

Redis moves the memory cost to the Redis process (shared across pods). Raft replicates the cost to every pod. For the 3.75 GB envelope (plan §14.2), Raft consumes an additional **90–185 MB per pod** — a 5–10% reduction in available burst headroom.

For the Redis process itself, the memory cost is roughly:
- Task data: ~50 MB for 100K tasks
- Session + idempotency: ~150 MB
- Rate limit buckets: ~20 MB
- Redis overhead: ~30 MB
- **Total: ~250 MB** (shared across all Miroir pods)

### 4.5 Operational Complexity

| Dimension | Redis | Raft |
|-----------|-------|------|
| External dependency | Redis server + Sentinel or cluster | None |
| Backup | `redis-cli --rdb` or `SAVE` | SQLite file copy (per pod) + consensus guarantees |
| Monitoring | Redis metrics (latency, memory, connected clients) | Raft-specific metrics (leader status, log lag, commit index) |
| Failure mode | Redis down → all pods lose shared state | Pod down → Raft continues; majority lost → cluster stalls |
| Recovery | Redis restart → RDB/AOF replay | Pod restart → replay log from SQLite; cluster restart → quorum recovery |
| Secret rotation | Redis password (if used) | No secrets, but must manage Raft membership |
| Operator familiarity | High — Redis is widely known | Low — embedded Raft is niche |
| Helm chart complexity | Redis as a dependency (subchart or external) | No external deps, but membership bootstrap logic |

### 4.6 Correctness Risk

This is the most important dimension.

| Risk | Redis | Raft |
|------|-------|------|
| Data loss | Redis AOF/RDB persistence can lose last 1s of writes | Raft guarantees committed entries survive minority failures |
| Split brain | Redis Sentinel can theoretically split-brain | Raft's term-based voting prevents split-brain by protocol |
| Implementation bugs | Redis is 15+ years old, battle-tested | openraft is ~4 years old, used in 3–4 production systems |
| Operational mistakes | Misconfiguring Redis persistence is common | Misconfiguring Raft membership can leave cluster inoperable |

Redis is boring and well-understood. Raft is correct in theory but the implementation is newer and less battle-tested at Miroir's scale. A Raft bug in openraft could silently lose or duplicate task state in ways that are extremely difficult to diagnose.

---

## 5. Decision Matrix

Applying the plan's decision gate: Raft must be measurably better on at least one metric without being worse on any other.

| Metric | Redis | Raft | Verdict |
|--------|-------|------|---------|
| Write latency | 0.3–0.8 ms | 2–5 ms | **Redis wins** (3–8x) |
| Read latency | 0.2–0.5 ms | 0.05–0.2 ms | **Raft wins** (2–5x) |
| Write throughput | ~100K ops/s | ~5–15K ops/s | **Redis wins** (7–20x) |
| Read throughput | ~100K ops/s | ~500K+ ops/s | **Raft wins** (5x) |
| Memory (per pod) | 0 additional | +90–185 MB | **Redis wins** |
| Memory (total cluster) | ~250 MB shared | 90–185 MB × N pods | Tie at 2 pods; Redis wins at 3+ |
| Ops simplicity (deps) | Requires Redis | No external dep | **Raft wins** |
| Ops simplicity (failure) | Single failure domain (Redis) | Distributed failure (Raft quorum) | **Redis wins** (simpler mental model) |
| Correctness maturity | Very high (15+ years) | Moderate (~4 years, 3–4 prod users) | **Redis wins** |
| Backup/restore | Standard tooling | Custom (SQLite + Raft recovery) | **Redis wins** |

### Score

- **Raft wins** on: ops simplicity (no external dep), read latency, read throughput
- **Raft loses** on: write latency, write throughput, memory per pod, correctness maturity, operational tooling

**Raft does not pass the decision gate.** It is better on some metrics but worse on others — specifically worse on the metric that matters most for a consensus system: correctness maturity and write latency.

---

## 6. Decision

### Ship: No.

Do not ship a Raft-backed task store in v0.x or v1.0.

### Revisit: Before v2.0.

Re-evaluate when all of the following are true:

1. **Redis backend is production-stabilized** (at least 6 months of production traffic with no data-loss incidents)
2. **The operational cost of Redis is empirically measured** — how often does Redis cause incidents? How much operator time does it consume? If the answer is "almost never," Raft is unnecessary.
3. **openraft reaches v1.0 stable** — the current v0.10 alpha series has frequent breaking changes. Waiting for API stability avoids rewriting the integration.
4. **Miroir has a working backup/restore story for Raft** — before shipping, we need a documented procedure for recovering a Raft cluster after losing majority, and a tested snapshot-to-fresh-cluster restore path.

### Rationale

1. **Redis works.** It's the industry-standard solution for shared state across stateless replicas. The operational burden of running Redis is well-understood and can be delegated to managed services (ElastiCache, Upstash, Redis Cloud) if self-hosting is undesirable.

2. **The write latency penalty is material.** Miroir's task store writes happen on the critical path of document mutations. Adding 2–5 ms of consensus latency per write, when Redis adds <1 ms, is a measurable degradation that the decision gate explicitly forbids.

3. **The complexity budget is better spent elsewhere.** Miroir's v1.0 has 21 advanced capabilities to ship (§13.1–§13.21), each with its own correctness surface. Adding a Raft implementation to the v1.0 scope would be a significant distraction with high downside risk.

4. **Raft's advantage (no external dependency) is modest for K8s deployments.** In Kubernetes, Redis is a standard add-on (Helm subchart, Bitnami chart, or managed service). It is not a novel operational burden. The real benefit of eliminating Redis would be for single-node deployments — but those already use SQLite.

5. **The read advantage is irrelevant for Miroir.** Sub-millisecond reads from Redis vs. sub-millisecond reads from local SQLite — the difference is invisible to clients and to the proxy's p99 latency budget.

### Possible Future: Hybrid Approach

If we revisit and decide to ship Raft, the cleanest path is:

1. Implement `TaskStore` trait as planned (SQLite backend first, Redis backend second)
2. Add a third `RaftTaskStore` that composes `SqliteTaskStore` as the state machine, wrapped by openraft
3. All three backends share the same trait — the only difference is config
4. Migration path: `sqlite` → `redis` → `raft` is a config change, not a code rewrite

This preserves the investment in the SQLite and Redis backends and avoids forcing a binary choice.

### Compilation Note

openraft 0.9.20/0.9.22 fails to compile on stable Rust 1.87 because its dependency `validit 0.2.5` uses the unstable `let_chains` feature. The 0.10 alpha series compounds this by requiring Rust edition 2024. The prototype works around this by simulating Raft consensus rather than depending on openraft directly — only `bincode` is needed for the serialization benchmarks. This compilation failure is itself a data point: a dependency that requires nightly or bleeding-edge stable Rust is not suitable for production use in v1.0.

---

## 7. Alternative Considered: LiteFS

[LiteFS](https://github.com/superfly/litefs) is a FUSE-based SQLite replication tool that transparently replicates SQLite writes to other nodes. It was considered as an alternative to both Redis and Raft.

**Eliminated because:**
- Requires FUSE support in the container (not available in all K8s environments, especially hardened/flatcar nodes)
- Single-writer model (one primary, others are read-only replicas) — the primary failover requires an external consul/election mechanism
- Adds a FUSE filesystem layer between SQLite and the kernel, introducing latency and debugging complexity
- Designed for Fly.io's infrastructure; using it elsewhere is possible but not its primary use case

Not suitable for Miroir's multi-writer K8s deployment model.

---

## 8. Reference Project: rrqlite

[rrqlite](https://github.com/yuyang0/rrqlite) is a Rust implementation of rqlite that uses **openraft to replicate SQLite** across nodes. This is the closest existing project to Miroir's proposed Raft architecture.

### How it works

1. Each node runs a local SQLite instance
2. Writes are proposed as WAL frame entries through openraft consensus
3. On commit, entries are applied to each node's SQLite
4. Reads are served from the local SQLite (no network hop)
5. Linearizable reads use openraft's `read_index`

### Relevance to Miroir

| Aspect | rrqlite | Miroir's proposed design |
|--------|---------|--------------------------|
| State machine | SQLite (full DB) | SQLite (14 specific tables) |
| Log replication unit | WAL frames | Serialized `TaskStoreCommand` |
| Command granularity | Row-level SQL | Operation-level (InsertTask, etc.) |
| Raft library | openraft | openraft (if shipped) |
| Snapshot | Full SQLite dump | Serialized state machine |

rrqlite validates that the openraft + SQLite combination works in production. However, rrqlite's approach (replicating raw WAL frames) is coarser than Miroir's proposed approach (replicating typed commands), which means Miroir's log entries would be smaller and more inspectable.

### libSQL / Turso

[libSQL](https://github.com/tursodatabase/libsql) (Turso) is another reference: a SQLite fork with built-in replication using a Raft-like protocol. It validates the pattern at scale (Turso is a managed service). However, it uses a custom consensus protocol rather than a pluggable Raft library, making it less directly applicable as an integration model.

## 9. Appendix: Crate Deep-Dive

### openraft v0.9.22 (Stable)

```
[dependencies]
openraft = { version = "0.9", features = ["serde", "type-alias"] }
```

Key types:
- `Raft<C>` — the main Raft node; generic over config type `C`
- `RaftLogStorage<C>` — persistent log storage trait
- `RaftStateMachine<C>` — state machine application trait
- `RaftNetworkFactory<C>` — creates per-peer network connections
- `Entry<C>` — a log entry with payload
- `Snapshot<C>` — a state machine snapshot

Configuration knobs relevant to Miroir:
- `heartbeat_interval` — default 500ms (Miroir: 3s to match current leader lease interval)
- `election_timeout_min/max` — default 150–300ms (Miroir: 3–5s for K8s network)
- `max_payload_entries` — default 300 (batch log appends for throughput)
- `snapshot_policy` — `SnapshotPolicy::LogsSinceLast(10000)` (snapshot every 10K entries)

### raft-rs v0.7.0

Not recommended for Miroir due to sync-only API (see §2.4), but included for completeness:

```
[dependencies]
raft = "0.7"
```

Key types:
- `RawNode` — the Raft state machine (tick-driven)
- `Storage` trait — synchronous storage interface
- `Ready` — batch of pending work (messages, entries, snapshots)
- `Config` — tick interval, election timeout, max inflight messages

---

## 10. References

- openraft repository: https://github.com/databendlabs/openraft
- openraft docs: https://docs.rs/openraft
- raft-rs repository: https://github.com/tikv/raft-rs
- Databend (openraft user): https://github.com/datafuselabs/databend
- TiKV (raft-rs user): https://github.com/tikv/tikv
- GreptimeDB (openraft user): https://github.com/GreptimeTeam/greptimedb
- LiteFS: https://github.com/superfly/litefs
- rrqlite (openraft + SQLite): https://github.com/yuyang0/rrqlite
- libSQL / Turso: https://github.com/tursodatabase/libsql
- Miroir plan §4 (task store schema): `docs/plan/plan.md` lines 531–766
- Miroir plan §14.2 (per-pod memory budget): `docs/plan/plan.md` lines 3397–3432
- Miroir plan §15 (open problems): `docs/plan/plan.md` lines 3725–3731
