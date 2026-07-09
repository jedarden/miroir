# bf-qvrmh — Empirically re-confirm Mode A partitioning tests execute (default vs feature config)

Step-1 exploration bead. Parent umbrella **bf-1zn8a**, grandparent **bf-30m2j**.

This bead is the empirical re-confirmation of what bf-1zn8a already mapped: do the
three inline Mode A acceptance tests in `anti_entropy.rs` actually *execute and pass*,
and under which cargo config? Re-verified fresh on 2026-07-09. Results agree with
bf-1zn8a's prior record.

## TL;DR

The task brief assumed the three tests "run WITHOUT the peer-discovery feature." They
do **not**. They are module-gated at `anti_entropy.rs:1532`:

```rust
#[cfg(all(test, feature = "peer-discovery"))]
mod tests_mode_a_acceptance {
```

- **Default config** (`cargo test -p miroir-core mode_a`): the three tests do **not
  compile/run** — `0 tests`, feature-gated out. Acceptance criterion 1 (show them
  executing in the *default* config) **cannot be met literally**, exactly as bf-1zn8a
  found.
- **Feature config** (`--features peer-discovery`): all three execute (not `#[ignore]`'d,
  not skipped) and **pass**. This is the genuine coverage evidence.

## Disk hygiene (~/CLAUDE.md)

```
$ df -BG --output=avail / | tail -1     (before)
17G                                       ← below ~20G threshold
$ du -sh ~/*/target | sort -rh           (biggest idle targets)
49G  /home/coding/pdftract/target        ← idle (lsof +D empty; no cargo/rustc refs)
47G  /home/coding/SIGIL/target
22G  /home/coding/miroir/target          ← DO NOT clear (building this)
$ rm -rf /home/coding/pdftract/target
$ df -BG --output=avail / | tail -1     (after)
65G
```

Cleared idle `pdftract/target` (49G) — not miroir-core's own target. 65G free afterward.

## RUN 1 — default config, `cargo test -p miroir-core mode_a`

```
$ cargo test -p miroir-core mode_a
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 13 filtered out
running 0 tests
…  (every binary reports "running 0 tests")
```

Every test binary reports `0 tests`; the three Mode A acceptance tests are **absent**
(feature-gated). Not `ignored` — they do not exist in the default build.

## RUN 2 — default config, `cargo test -p miroir-core --lib anti_entropy`

```
running 17 tests
test anti_entropy::tests::test_compute_content_hash_different_content ... ok
… (8 anti_entropy::tests::*, plus migration::tests::* and rebalancer_worker::anti_entropy_worker::tests::*)
test result: ok. 17 passed; 0 failed; 0 ignored; 0 measured; 704 filtered out
```

17 anti-entropy tests run and pass in default config — but these are the **non-Mode-A**
anti-entropy tests (content-hash, validate/safety-net, schedule parsing). None of the
three `test_mode_a_*` tests appear (feature-gated out).

## RUN 3 — feature config, `cargo test -p miroir-core --features peer-discovery --lib mode_a`  ← the three tests run here

```
running 16 tests
test anti_entropy::tests_mode_a_acceptance::test_mode_a_anti_entropy_partitioning ... ok
test anti_entropy::tests_mode_a_acceptance::test_mode_a_pod_reassignment ... ok
test mode_a_coordinator::tests::test_is_single_pod ... ok
test anti_entropy::tests_mode_a_acceptance::test_mode_a_three_pods_each_shard_processed_once ... ok
… (13 more mode_a_coordinator::tests::* unit tests)
test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 724 filtered out
```

All three inline acceptance tests **execute (not skipped) and pass**. (Other 13 of 16
are `mode_a_coordinator::tests::*` whose path matches the `mode_a` filter.)

## RUN 4 — feature config, `cargo test -p miroir-core --features peer-discovery --lib anti_entropy`

```
running 20 tests
… 17 from RUN 2 plus:
test anti_entropy::tests_mode_a_acceptance::test_mode_a_anti_entropy_partitioning ... ok
test anti_entropy::tests_mode_a_acceptance::test_mode_a_pod_reassignment ... ok
test anti_entropy::tests_mode_a_acceptance::test_mode_a_three_pods_each_shard_processed_once ... ok
test result: ok. 20 passed; 0 failed; 0 ignored; 0 measured; 720 filtered out
```

The three Mode A tests surface inside the `anti_entropy` filter only under the feature.

## The three inline tests — exact fn names + results

| Brief name | Full fn path | Line | Default cfg | Feature cfg |
|---|---|---|---|---|
| exactly-one-owner | `anti_entropy::tests_mode_a_acceptance::test_mode_a_three_pods_each_shard_processed_once` | `anti_entropy.rs:1541` | absent (gated) | **ok** |
| pod-reassignment | `anti_entropy::tests_mode_a_acceptance::test_mode_a_pod_reassignment` | `anti_entropy.rs:1665` | absent (gated) | **ok** |
| partitioning-wiring | `anti_entropy::tests_mode_a_acceptance::test_mode_a_anti_entropy_partitioning` | `anti_entropy.rs:1761` | absent (gated) | **ok** |

All three are `#[tokio::test]` (not `#[ignore]`). They use the synthetic peer-set path
`ModeACoordinator::set_peer_set_for_test` (`mode_a_coordinator.rs:306`) — no SRV/DNS,
no docker.

## Acceptance criteria (bf-qvrmh)

- [x] Free-disk check recorded before build (`df` 17G → cleared pdftract/target → 65G).
- [~] `cargo test -p miroir-core mode_a` output pasted — but it shows `0 tests` in the
      default config (the three tests are `feature = "peer-discovery"`-gated at
      `anti_entropy.rs:1532`, so they do NOT exist there). The honest default-config
      result is RUN 1. They execute and pass only under `--features peer-discovery`
      (RUN 3). This literal-criterion mismatch is itself the finding and matches
      bf-1zn8a's prior record.
- [x] `cargo test -p miroir-core anti_entropy` output pasted (default: 17 pass, Mode A
      absent — RUN 2; feature: 20 pass incl. the three Mode A — RUN 4).
- [x] Exact test fn names with results listed in the table above.

## Cross-ref

Fully consistent with `notes/bf-1zn8a.md` (parent). The deep coverage map and the
genuine remaining gaps (a: survivor-stability un-pinned; b: no 3→4 scale-up; c: no
pinned hash-vector test) live there — not re-derived here. bf-qvrmh only re-confirms
the empirical "do they run?" foundation.
