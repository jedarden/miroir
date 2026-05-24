# OP#3: Resharding (S-change) vs. Node Scaling (N-change) - COMPLETED

## Task Summary

Completed the remaining work for Open Problem #3 by documenting the trade-offs between S-change (resharding) and N-change (node scaling).

## What Already Existed

The codebase already had excellent implementation and validation:

1. **Benchmark resharding with realistic corpora** - `docs/benchmarks/resharding-load.md`
   - Comprehensive test matrix covering small/medium/large documents
   - Empirically validates the 2× transient storage and write load caveat
   - All scenarios confirm exactly 2.0× storage and dual-write amplification

2. **CLI schedule guidance for off-peak windows** - `crates/miroir-ctl/src/commands/reshard.rs`
   - Schedule window guard with time-based restrictions
   - `--schedule-window` flag to specify allowed windows
   - `--force` override with warnings
   - Dry run support

3. **Core resharding implementation** - `crates/miroir-core/src/reshard.rs`
   - Six-phase shadow-index operation (§13.1)
   - Configuration management
   - Load simulation and validation

## What Was Added

Added comprehensive "Resharding (S-Change) vs Node Scaling (N-Change)" section to `docs/trade-offs.md`:

- **Core distinction table**: Compares N-change (node scaling) vs S-change (resharding)
- **Node scaling explanation**: Lightweight operation using rendezvous hashing, moves only ~1/N of documents
- **Resharding explanation**: Heavy operation with 2× transient storage/write amplification
- **Decision matrix**: Helps operators choose the right approach based on symptoms
- **Capacity planning guidance**: S = max_nodes_per_group_ever × 8 formula with rationale
- **Operator guidance**: Steps to follow if resharding is necessary

## Key Insights

The main insight for operators: **Node scaling is lightweight; resharding is heavy.**

- Adding nodes: only ~1/N of documents move (those whose top-ranked node changes)
- Resharding: every document's shard assignment changes, requiring full dual-hash dual-write
- Prefer N-change over S-change whenever possible
- Choose S generously at index creation to avoid ever needing to reshard

## Commit

```
commit 1fa5187
OP#3: Document S-change (resharding) vs N-change (node scaling) trade-offs
```

## Status

**COMPLETED** - All OP#3 requirements addressed:
- ✅ Benchmark resharding operations with realistic document distributions
- ✅ Validate transient storage and write load multiplier assumptions
- ✅ Add CLI schedule guidance for off-peak reshard windows
- ✅ Document trade-offs between S-change (resharding) and N-change (node scaling)
