# bf-2ynu5 — Add per-node document-count query helper for reshard backfill (umbrella close-out)

Split-child of bf-49a17 (reshard backfill progress never populated). This was
the **foundation/umbrella** piece: a reusable mechanism to compute a reshard
source index's real document count, with no change to the reshard state machine.

The implementation landed as **four closed sibling/child beads**, each owning
one slice. This bead's own deliverable is the umbrella close-out: confirm the
children integrate, build, and pass, and that every acceptance criterion below
is met in the current tree. No code was authored under bf-2ynu5 directly.

## Child beads (all closed)

| Bead | Commit | What it owned |
|------|--------|---------------|
| bf-2r01e | f10a675 | Register `index_stats` module (`pub mod index_stats;` lib.rs:37) |
| bf-14wgq | 3dd2b75 | Add `ReshardExecutor::compute_source_document_count` (executor.rs:538) |
| bf-1s9od | 1523cae | Unit-test the executor helper against mockito nodes (executor.rs:970+) |
| bf-3r626 | 3b52aba | Migrate ILM off its duplicate stats code onto the shared module |

## Acceptance criteria — verified against current tree (2026-07-10)

1. **Helper queries each healthy source node's `GET /indexes/{uid}/stats` and
   returns an aggregated count.**
   `compute_source_document_count` (reshard/executor.rs:538) delegates to
   `index_stats::aggregate_index_stats` (index_stats.rs:130), which iterates the
   source `node_addresses`, GETs `/indexes/{uid}/stats`, and reduces
   `numberOfDocuments` with `max` (each address hosts a full replica, so `sum`
   would over-count by the replication factor — same reduction ILM relies on).
   A node that fails to respond (network / non-2xx) is logged and skipped; a 404
   (absent replica) counts as zero. "Healthy" = "responds successfully", matching
   the original `ilm::fetch_index_stats` precedent.

2. **Reuses/extracts the existing ilm.rs aggregation rather than duplicating.**
   The HTTP+reduce logic lives once in the shared `index_stats` module
   (index_stats.rs). ILM's old `fetch_index_stats` is gone; ilm.rs now calls
   `crate::index_stats::aggregate_index_stats` at :701 and :1067. Reshard and
   ILM therefore compute an index's count the *same* way.

3. **Unit-tested against a mock node client returning known per-node counts.**
   - Pure reduction policy `reduce_document_counts` (index_stats.rs:170), 9 tests.
   - Executor helper driven against mockito `GET /indexes/{uid}/stats` servers,
     4 tests (max across healthy; one failing node skipped; 404 tolerated;
     all-fail → 0). See "Test run" below.

4. **No change to the reshard state machine / backfill migration.**
   `start_backfill` (executor.rs:513) still initializes `total_documents: 0`
   (`// Will be updated`); `advance_backfill` (executor.rs:640) still accumulates
   `total_docs_in_shard` incrementally. `compute_source_document_count` is a pure
   additive helper that is *not* called from the state machine in this umbrella.
   Wiring the up-front denominator into `start_backfill` is intentionally a
   sibling bead's job.

## Test run (this close-out)

Disk pre-flight: 46 GB free on `/` (above the ~20 G build threshold) — no
`target/` cleanup needed.

```
$ cargo test -p miroir-core --lib index_stats
test result: ok. 9 passed; 0 failed

$ cargo test -p miroir-core --lib compute_source_doc_count
test result: ok. 4 passed; 0 failed
```

Whole `miroir-core` lib compiled clean (712 tests in the suite, no warnings).
Both seams green: the pure reduction and the real-reqwest transport against
mockito nodes.

## Conclusion

All four acceptance criteria are met and verified. The reshard backfill now has
the missing denominator (source index real document count, computed before the
first page is fetched) available via `compute_source_document_count`, computed
identically to ILM. Umbrella bf-2ynu5 closes; the remaining work (populating the
persisted `ReshardOperation.total_documents` / `start_backfill` wiring) is
tracked under other children of bf-49a17.
