# bf-21o8t — Status report for the 4 Mode A partitioning tests

Split-child of bf-2vj7m (STEP 4/4 — FINAL). Consumed Child 1 (bf-2y8op: test names +
lines) and Child 3 (bf-68dkx: captured output). No source changes — analysis only; this
note + comment #27 on the bead are the commit artifact.

## Method

Cross-checked each named test against BOTH the source
(`crates/miroir-core/src/anti_entropy.rs`) AND the captured Run 1 log
(`/tmp/miroir-core-test.log` — `cargo test -p miroir-core`, default features):
- confirmed the `#[tokio::test]` attribute (no `#[ignore]` on any of them);
- confirmed each function name appears as `... ok` in the output;
- confirmed the lib-unittests result line (log:765):
  `test result: ok. 750 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out`.

## Result — all 4 PASSED, none ignored, none absent

| # | Test (plan ref) | Function | src `#[tokio::test]` / `fn` | log line | Status |
|---|---|---|---|---|---|
| 1 | 3-pod exactly-one-owner | `test_mode_a_three_pods_each_shard_processed_once` | :1536 / :1537 | :71 `... ok` | **PASSED** |
| 2 | pod-reassignment | `test_mode_a_pod_reassignment` | :1660 / :1661 | :66 `... ok` | **PASSED** |
| 3 | partitioning-wiring | `test_mode_a_anti_entropy_partitioning` | :1756 / :1757 | :42 `... ok` | **PASSED** |
| 4a | bf-4t3o4 scale-down survivor stability | `test_mode_a_minimal_reshuffling_scale_down_survivor_stability` | :1845 / :1846 | :79 `... ok` | **PASSED** |
| 4b | bf-4t3o4 scale-up survivor stability | `test_mode_a_minimal_reshuffling_scale_up_survivor_stability` | :1926 / :1927 | :99 `... ok` | **PASSED** |

- **Ignored:** none. Lib-unittests binary reports `0 ignored`, and no `#[ignore]` attribute
  sits on any of the five functions.
- **Absent:** none. Every named function appears in the captured output as `... ok`.

## Caveats (from bf-68dkx, unchanged here)

- Results are from `cargo test -p miroir-core` (default features), NOT CI's own command.
  CI runs `cargo test --all --all-features`, which **compile-fails** `miroir-proxy`
  (`main.rs:822` E0384 — `runner` reassigned while immutable under
  `feature = "peer-discovery"`), so zero test binaries run there. Both defects
  (miroir-proxy E0384 + the unrelated doctest `peer_discovery.rs:16` that makes Run 1 exit
  101) are pre-existing on HEAD and independent of the current anti_entropy.rs /
  reshard/executor.rs working-tree changes.
- Item 4 ("bf-4t3o4's additions") is **two** functions. They live in module
  `mode_a_minimal_reshuffling_tests`, not `tests_mode_a_acceptance`, but they are exactly
  the rendezvous minimal-reshuffling-on-resize tests bf-4t3o4 added (surviving pods must
  keep the shards they already owned on 3→2 and 3→4 resize).

## Net for parent bf-2vj7m

All 4 named Mode A partitioning tests are present, not ignored, and PASS under
`cargo test -p miroir-core`. The repo's test redness is solely from the two pre-existing
compile/doctest defects, not from these tests or the current working-tree changes.
