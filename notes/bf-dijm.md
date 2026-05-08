# OP#2: Task State HA (Raft vs. Redis) - Summary

## Status: Deferred to v2.x

## Research Completed

### 1. Comprehensive Analysis (docs/research/raft-task-store.md)
- Crate survey: openraft vs raft-rs vs async-raft
- Decision matrix comparing Redis vs Raft on 9 dimensions
- Prototype architecture design
- Analytical benchmarks for latency, throughput, and memory

### 2. Decision
**Do not ship Raft in v1.x.** Revisit before v2.0.

**Rationale:**
- Raft is 3-8x slower on writes (2-5ms vs 0.3-0.8ms for Redis)
- Raft adds 90-185MB per-pod memory overhead
- openraft 0.9.20 fails to compile on stable Rust 1.87 (unstable `let_chains`)
- Correctness surface: Raft bugs can silently lose data
- Redis is battle-tested; operational burden is well-understood

### 3. Prototype Implementation (crates/miroir-core/src/raft_proto/)
- `RaftTaskRegistry`: Implements TaskRegistry trait
- `TaskStateMachine`: Command-based state machine
- `TaskStoreCommand`: Serialized operations for Raft log
- `benchmark.rs`: Performance measurements

### 4. Benchmarks
- State machine apply path: ~1,889ns (negligible overhead vs direct HashMap)
- Throughput: ~529K ops/sec (local apply only)
- Real cost is network + fsync consensus overhead (2-5ms per write)

## For v2.x Re-evaluation

### Prerequisites
1. Redis backend is production-stabilized (6+ months)
2. Operational cost of Redis is empirically measured
3. openraft reaches v1.0 stable
4. Backup/restore story for Raft is documented

### Implementation Path
1. Implement `TaskStore` trait (SQLite + Redis backends)
2. Add `RaftTaskStore` composing `SqliteTaskStore` as state machine
3. All three backends share the same trait
4. Migration path: config change, not code rewrite

### Migration Strategy (Redis → Raft)
1. Deploy new version with Raft support alongside Redis
2. Configure Raft cluster in parallel
3. Run migration job to copy Redis state to Raft
4. Switch reads to Raft (dual-write mode)
5. Switch writes to Raft
6. Decommission Redis

## References
- Research: docs/research/raft-task-store.md
- Prototype: crates/miroir-core/src/raft_proto/
- Plan ref: §15 Open Problem #2
