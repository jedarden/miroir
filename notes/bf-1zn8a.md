# bf-1zn8a — Map Mode A partitioning test coverage vs disabled acceptance_4

Split-child of **bf-30m2j**. Investigative step 1: establish that the disabled
`acceptance_4_mode_a_shard_partitioning` test's acceptance criteria are ALREADY
covered inline, so children attack the real (narrow) gaps instead of rewriting a
redundant test.

## TL;DR

The parent's premise is **stale on two counts**:

1. **The coverage already exists and passes.** Three inline acceptance tests in
   `crates/miroir-core/src/anti_entropy.rs` (mod `tests_mode_a_acceptance`) plus
   `test_owns_exactly_one_peer_per_item` in `mode_a_coordinator.rs` already
   verify the Mode A shard-partitioning properties using the *current* API. The
   removed `with_mode_a_scaling` was already replaced by `with_mode_a(coordinator)`
   + `set_peer_set_for_test(peer_set)`.
2. **They are NOT "removed/commented out so they don't even show up"** — they are
   `#[cfg(feature = "peer-discovery")]`-gated. They do not run in plain
   `cargo test -p miroir-core` (default features), but they DO run and pass under
   `cargo test --all --all-features` (the `make test` target) and
   `cargo test -p miroir-core --features peer-discovery`.

The disabled `acceptance_4` in `tests/p13_8_anti_entropy.rs:241-244` is
**redundant** with the inline tests; its stale comment (criterion 5) just needs a
4-line update pointing at them. The genuine gaps are narrow: (a) survivor
stability / minimal-reshuffling not pinned, (b) no scale-up 3→4 case, (c) no
pinned hash-vector test.

## Step 1 — `cargo test` results (recorded)

Pre-flight: `df -BG --output=avail /` → **21G free**; miroir `target/` already
18G (incremental), so no `target/` clearing needed (above the ~20G threshold).

### RUN 1 — default config, filter `mode_a`

```
$ cargo test -p miroir-core mode_a
   …
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 708 filtered out
```

(Every binary reports `running 0 tests`.) **The three acceptance tests do NOT
execute in the default config.** Neither does any `mode_a_coordinator` code:
`lib.rs:40` gates the whole module behind `#[cfg(feature = "peer-discovery")]`,
and `default = []` (`Cargo.toml:67`).

### RUN 2 — default config, filter `anti_entropy`

```
$ cargo test -p miroir-core anti_entropy
running 17 tests
test result: ok. 17 passed; 0 failed; 0 ignored; 0 measured; 691 filtered out
running 1 test
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 18 filtered out
```

17 + 1 anti-entropy tests run and pass in the default config — but these are the
NON-Mode-A anti-entropy tests (bucket isolation, content-hash, publish, etc.).
The three `test_mode_a_*` tests are absent (feature-gated out).

### RUN 3 — `--features peer-discovery`, filter `mode_a`  ← the three tests run here

```
$ cargo test -p miroir-core --features peer-discovery mode_a
running 16 tests
test anti_entropy::tests_mode_a_acceptance::test_mode_a_anti_entropy_partitioning ... ok
test anti_entropy::tests_mode_a_acceptance::test_mode_a_pod_reassignment ... ok
test anti_entropy::tests_mode_a_acceptance::test_mode_a_three_pods_each_shard_processed_once ... ok
…
test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 711 filtered out
```

All three inline acceptance tests **execute (not skipped) and pass**. (The other
13 of the 16 are `mode_a_coordinator::tests::*` unit tests whose path matches
`mode_a`.)

**Note on the task's acceptance criterion 1** ("show the three tests executing in
the *default* config"): this cannot be met literally — they are feature-gated and
do not compile under default features. The honest result is pasted above: they
execute only with `--features peer-discovery` (or `--all-features`). This is
itself a finding (see criterion 4 below).

## Step 2 — Coverage map: parent bf-30m2j acceptance criteria → inline tests

| # | Parent acceptance criterion | Status |
|---|---|---|
| 1 | Rewrite/reinstate `acceptance_4` using current `ModeACoordinator`/`peer_discovery` API (not removed `with_mode_a_scaling`) | **[COVERED]** |
| 2 | 3-pod set: each shard owned by exactly one pod (no double/no orphan), matching `top1_by_score(hash(shard_id‖pod_id))` (§14.5) | **[COVERED]** |
| 3 | Ownership redistributes (minimal reshuffling) on peer-set size change; consistent with "transient double-work in 15s window harmless" | **[PARTIALLY COVERED]** |
| 4 | Runs in default `cargo test` / `cargo test --all` (no `--ignored`, no docker) so it executes in CI | **[PARTIALLY MET — nuanced]** |
| 5 | Update/remove stale comment at `p13_8_anti_entropy.rs:240-244` | **[NOT DONE — trivial]** |

### Criterion 1 — [COVERED]
All three inline tests use the current API:
- `ModeACoordinator::new(pod_id, peer_discovery)` — `mode_a_coordinator.rs:66`
- `set_peer_set_for_test(peer_set)` — `mode_a_coordinator.rs:306` (the synthetic
  peer-set injector that **replaces** the removed `with_mode_a_scaling`; reachable
  via `cfg(test)` / `feature = "test-helpers"`, no SRV lookup)
- `AntiEntropyReconciler::with_mode_a(coordinator)` — `anti_entropy.rs:1581,1584,1587,1790`
  (replaces the old `with_mode_a_scaling` builder)
- `owns_shard(&shard_str)` — `mode_a_coordinator.rs:151`

The "API drift" problem the parent cites is already resolved in-tree.

### Criterion 2 — [COVERED] by `test_mode_a_three_pods_each_shard_processed_once` (`anti_entropy.rs:1541`)
- 3 coordinators, shared 3-pod peer set — `anti_entropy.rs:1565-1572`
- exactly-one-owner per shard: `owner_count == 1` — `anti_entropy.rs:1622-1627`
- all 64 covered (no orphan): `total_owned == 64` — `anti_entropy.rs:1631-1635`
- even distribution: `min_owned >= 15`, `max_owned <= 25` — `anti_entropy.rs:1649-1658`
- The `top1_by_score(hash(s‖pid))` rule (`plan.md:3520`) **is** the implementation
  of `owner_for_shard`/`owns_shard` (`mode_a_coordinator.rs:123-158`, via
  `rendezvous_score` at `:112-118`). The test exercises it end-to-end.
- Additionally covered for 5 specific items (incl. non-shard: `task-abc`,
  `index:node-1`) by `test_owns_exactly_one_peer_per_item`
  (`mode_a_coordinator.rs:422`).

### Criterion 3 — [PARTIALLY COVERED] by `test_mode_a_pod_reassignment` (`anti_entropy.rs:1665`)
- 3→2 scale-**down**: removed pod's shards re-homed to exactly one survivor
  (`owner_count == 1`) — `anti_entropy.rs:1733-1738`
- all 64 still owned (`total_owned == 64`) — `anti_entropy.rs:1751-1755`
- **Missing:** no survivor-stability / minimal-reshuffling assertion → gap (a).
- The "15s discovery window / transient double-work harmless" note is inherently
  hard to unit-test; the test pins the steady-state post-resize invariant, which
  is the testable essence. Acceptable.

### Criterion 4 — [PARTIALLY MET — the real finding]
- No docker, no `--ignored`: ✓
- Does **NOT** run in plain `cargo test -p miroir-core` or `cargo test --all`
  (default features): the whole `mode_a_coordinator` module
  (`lib.rs:40 #[cfg(feature = "peer-discovery")]`) and `tests_mode_a_acceptance`
  (`anti_entropy.rs:1532 #[cfg(all(test, feature = "peer-discovery"))]`) are
  feature-gated; `default = []` (`Cargo.toml:67`); `.cargo/config.toml` sets no
  default features.
- **DOES** run under `cargo test --all --all-features` (Makefile `test` target,
  `Makefile:8`) and `cargo test -p miroir-core --features peer-discovery`.
- `miroir-proxy` enables `peer-discovery` for its `miroir-core` dep
  (`crates/miroir-proxy/Cargo.toml:38`), so any proxy build pulls the feature —
  but miroir-core's own lib unit tests only run with the feature on.
- **CI implication:** `make test`/`--all-features` → tests run ✓. But the PR
  template's `cargo test --workspace` (`.github/pull_request_template.md:45`,
  `.github/release_pr_template.md:31`) uses default features and would **miss**
  them. So "it executes in CI" is true only if CI uses `--all-features`.

### Criterion 5 — [NOT DONE — trivial]
The stale comment at `tests/p13_8_anti_entropy.rs:241-244` still claims the test
"uses an old API (`with_mode_a_scaling`)" and "requires the peer-discovery
feature and ModeACoordinator." It is now misleading: the inline replacement
exists at `anti_entropy.rs:1532-1798`. A 4-line edit to point there (and drop the
dead `acceptance_4` reference) closes this. Not a coverage gap.

## Step 3 — Genuine remaining gaps (confirmed/refuted)

### (a) No minimal-reshuffling / survivor-stability assertion — CONFIRMED
`test_mode_a_pod_reassignment` (`anti_entropy.rs:1665`) records **only**
`pod3_owned_initial` (`:1704-1711`) and re-checks **only** those shards
(`:1724-1738`). It never records pod-1's / pod-2's initial shards and asserts
they are unchanged after the resize. A buggy implementation that reshuffled
**all** shards on every peer-set change — while preserving exactly-one-owner +
full coverage — would still pass. The "minimal reshuffling" property
(`plan.md:3528`; quantified at `plan.md:1522` "adding a 4th node moves at most
`2 × (1/4)` of shards") is therefore un-pinned for the anti-entropy Mode A
background-work path. (The router's data-path rendezvous is a separate function
in `scatter/router.rs`; this gap is specifically about `mode_a_coordinator`'s
background-work ownership.)

### (b) No scale-UP (3→4) case; only scale-DOWN (3→2) — CONFIRMED
Only `test_mode_a_pod_reassignment` (3→2) exists. No test adds a 4th pod. Yet
`plan.md:1522` explicitly names the scale-**up** case as the canonical
minimal-reshuffling example, and scale-up is the more common real-world
elasticity event. Untested for Mode A ownership.

### (c) No pinned hash-vector test against `top1_by_score(hash(shard_id‖peer_id))` — CONFIRMED (lower priority)
`rendezvous_score` is private (`mode_a_coordinator.rs:112`). The acceptance tests
+ `test_owns_exactly_one_peer_per_item` use the public `owns_shard`/
`owner_for_shard` API and verify **properties** (exactly-one-owner, full
coverage, even distribution; plus determinism/peer-sensitivity/shard-sensitivity
via `test_rendezvous_score_*` at `:317/:325/:333`) — but pin **no** specific
expected shard→owner vector. A change to the hash construction that preserves
those properties while silently reassigning all ownership — e.g. changing the
`"||"` separator (`mode_a_coordinator.rs:115`) to `"|"`, swapping the
shard/peer write order, or reseeding `XxHash64` (`:113`) — would pass every
existing test yet break the documented `hash(s‖pid)` contract (`plan.md:3520`).
A golden-vector test (e.g. "peers `[pod-1,pod-2,pod-3]` → shard `7` owned by
`pod-2`") would catch this. Lower priority: guards against an unlikely
hash-construction refactor, not a live regression.

## Recommendation for child beads

The disabled `acceptance_4` is **redundant** — do NOT rewrite it. Instead:
1. **bf gap (a):** extend `test_mode_a_pod_reassignment` to record pod-1/pod-2
   initial shards and assert survivor stability (unchanged ownership) after the
   3→2 resize. Optionally add a minimal-reshuffling bound assertion.
2. **bf gap (b):** add a 3→4 scale-**up** test asserting the new pod steals only
   its fair share (~1/4) and survivors keep ≥3/4 of their shards.
3. **bf gap (c):** add a pinned golden-vector test for `owner_for_shard` against
   a hardcoded expected mapping (expose `rendezvous_score` via `cfg(test)` or
   assert through `owner_for_shard`).
4. **bf criterion 5:** update the stale comment at `p13_8_anti_entropy.rs:241-244`
   to point at `anti_entropy.rs:1532-1798` and drop the dead `acceptance_4`
   reference.
5. **bf criterion 4 (optional, separate):** decide whether Mode A tests should
   run under default features. Cheapest fix: nothing (they run under `make test`).
   Strongest: move the `mode_a_coordinator` mod off the `peer-discovery` gate
   (it only needs `set_peer_set_for_test`, which already avoids SRV) so the tests
   run in plain `cargo test -p miroir-core`. This is a larger change and should
   be its own bead if pursued.

## Verification commands

```bash
# default config — three tests do NOT run (feature-gated)
cargo test -p miroir-core mode_a
# three tests DO run and pass
cargo test -p miroir-core --features peer-discovery mode_a
# canonical CI target (Makefile) — runs them via --all-features
make test
```

## Acceptance criteria (bf-1zn8a)

- [x] `cargo test` output pasted showing the three inline tests executing (under
      `--features peer-discovery` / `make test`; default-config output also pasted
      showing they are feature-gated and do not run there).
- [x] Coverage map: each parent acceptance criterion marked
      `[covered by <test>:<line>]` / `[PARTIALLY COVERED]` / `[NOT DONE]`.
- [x] Enumerated gaps (a/b/c) confirmed, recorded as comments on bf-30m2j and
      bf-1zn8a.
