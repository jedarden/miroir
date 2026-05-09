# P12.OP2: Raft vs Redis Research Verification

**Bead:** miroir-zc2.2
**Date:** 2026-05-08
**Status:** Research verified — no changes needed

## Summary

The research document `docs/research/raft-task-store.md` already exists and is comprehensive. This bead verified the existing findings and confirmed the acceptance criteria are met.

## Research Document Status

### Contents Verified

1. **Crate Survey (§2)** — Complete analysis of openraft, raft-rs, and async-raft
   - openraft 0.9.20 recommended (async-native, split traits, active maintenance)
   - async-raft eliminated (abandoned since 2023)
   - raft-rs not recommended (sync-only API)

2. **Prototype Design (§3)** — Architecture documented
   - `RaftTaskStore` trait implementation design
   - Storage layout (raft_log, raft_state, state machine tables)
   - Command protocol (TaskStoreCommand enum)
   - Read path (local SQLite with optional read_index)
   - Network transport (pod-to-pod over headless Service)

3. **Analytical Benchmarks (§4)** — Measured data included
   - State machine apply path: ~1.0x overhead vs direct HashMap
   - Write latency: Raft 2-5ms vs Redis 0.3-0.8ms (3-8x slower)
   - Read latency: Raft 0.05-0.2ms vs Redis 0.2-0.5ms (2-5x faster)
   - Memory footprint: +90-185 MB per pod for Raft

4. **Decision Matrix (§5)** — Clear verdict
   - Raft wins on: ops simplicity (no external dep), read latency, read throughput
   - Raft loses on: write latency, write throughput, memory per pod, correctness maturity
   - **Does not pass decision gate** (worse on some metrics, not better on all)

5. **Decision (§6)** — Recorded
   - **Ship:** No (do not ship in v0.x or v1.0)
   - **Revisit:** Before v2.0 (when Redis is production-stabilized and operational cost is empirically measured)

6. **Additional Sections**
   - LiteFS alternative considered and eliminated
   - rrqlite reference project analyzed
   - Crate deep-dive with API details

## Prototype Code Status

The prototype code exists at `crates/miroir-core/src/raft_proto/`:
- `mod.rs` — RaftTaskRegistry implementation
- `state_machine.rs` — In-memory TaskStateMachine
- `command.rs` — TaskStoreCommand enum
- `benchmark.rs` — Benchmark harness

**Note:** The `raft-proto` feature is commented out in `Cargo.toml` because openraft 0.9.20 fails to compile on stable Rust 1.87 (dependency `validit 0.2.5` uses unstable `let_chains` feature). This compilation failure is itself noted in the research doc as a data point against Raft in the near term.

## Acceptance Criteria

- [x] Research doc published with prototype location referenced
  - `docs/research/raft-task-store.md` exists and is comprehensive
  - Prototype location: `crates/miroir-core/src/raft_proto/` (feature-gated)

- [x] Decision recorded: ship / don't ship / revisit when
  - Decision: "Revisit before v2.0, do not ship in v0.x or v1.0"
  - Rationale documented in §6 with 5 points supporting the decision

## Key Findings

1. **Redis is the right choice for v1.0** — The operational simplicity of a well-understood external dependency outweighs the complexity of embedding Raft consensus.

2. **Raft write latency is material** — 2-5ms per write vs <1ms for Redis. This is on the critical path for document mutations.

3. **Memory cost is non-trivial** — +90-185 MB per pod for Raft, which is 5-10% of the 3.75 GB envelope (plan §14.2).

4. **Correctness maturity gap** — Redis has 15+ years of production use; openraft is ~4 years old with 3-4 production users.

5. **Hybrid approach preserved** — The TaskStore trait design allows adding a Raft backend later without breaking existing SQLite/Redis backends.

## No Action Required

The existing research is comprehensive and complete. The decision is clearly recorded. The prototype code exists and is documented. No additional work is needed for this bead beyond verification.
