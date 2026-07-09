# bf-59oct — Map each inline Mode A test → grandparent bf-30m2j acceptance criteria

Coverage-map bead. Parent umbrella **bf-1zn8a**, grandparent **bf-30m2j**.
Step 2 of the coverage investigation; builds on the empirical test-run evidence
in sibling **bf-qvrmh** (which re-confirmed the three tests execute + pass).

This note is the crisp, decisive, per-test assertion-cited coverage map: for each
of grandparent bf-30m2j's **five** acceptance criteria, which of the three inline
tests covers it (with the specific assertion + `file:line`), and where the genuine
gap is. The point is to prove the disabled `acceptance_4`'s criteria are already
covered inline so later siblings attack the *narrow* gap instead of rewriting a
redundant test.

## Test inventory (`crates/miroir-core/src/anti_entropy.rs`, mod `tests_mode_a_acceptance`)

Whole module is gated `#[cfg(all(test, feature = "peer-discovery"))]` @
`anti_entropy.rs:1532`. All three are plain `#[tokio::test]` (no `#[ignore]`),
run against the in-memory `crate::scatter::MockNodeClient` (no docker).

| Task shorthand | Actual fn | Location |
|---|---|---|
| exactly-one-owner | `test_mode_a_three_pods_each_shard_processed_once` | `anti_entropy.rs:1541` |
| pod-reassignment  | `test_mode_a_pod_reassignment`                     | `anti_entropy.rs:1665` |
| partitioning-wiring | `test_mode_a_anti_entropy_partitioning`          | `anti_entropy.rs:1761` |

All three exercise the **current** API, not the removed `with_mode_a_scaling`:
`ModeACoordinator::new` + `PeerDiscovery::new` + `PeerSet::new` +
`set_peer_set_for_test` (`mode_a_coordinator.rs:306`) + `with_mode_a`
(`anti_entropy.rs:199`). Assertions query `ModeACoordinator::owns_shard`
(`mode_a_coordinator.rs:151`) → `owner_for_shard` (`:123`, top-1 by score) →
`rendezvous_score` (`:112`) = `XxHash64(shard_id || "||" || peer_id)` — i.e.
exactly `top1_by_score(hash(s ‖ pid))` from plan §14.5 (`plan.md:3520`).

## Coverage map (grandparent bf-30m2j criteria 1-5)

### Criterion 1 — Rewrite/reinstate `acceptance_4` using current `ModeACoordinator`/`peer_discovery` API (not removed `with_mode_a_scaling`) — **[COVERED]**

The criterion's substance ("exercise the current API, not the deleted
`with_mode_a_scaling`") is satisfied by all three tests. No `with_mode_a_scaling`
symbol exists in the tree (only `with_mode_a` @ `anti_entropy.rs:199`):

- **partitioning-wiring** — `ModeACoordinator::new` @ `anti_entropy.rs:1768`;
  `with_mode_a(coordinator)` @ `anti_entropy.rs:1790`.
- **exactly-one-owner** — `ModeACoordinator::new` @ `anti_entropy.rs:1548/1555/1562`;
  `with_mode_a(coordinator_{1,2,3})` @ `anti_entropy.rs:1581/1584/1587`.
- **pod-reassignment** — `ModeACoordinator::new` @ `anti_entropy.rs:1672/1679/1686`.

The "API drift" the parent cites is already resolved in-tree; any rewrite of
`acceptance_4` would duplicate this. (The *literal act* of deleting the dead
`acceptance_4` is a cleanup, see criterion 5.)

### Criterion 2 — Each shard owned by EXACTLY ONE pod (no double-ownership, no orphan), matching `top1_by_score(hash(shard_id ‖ pod_id))` — **[COVERED by exactly-one-owner: anti_entropy.rs:1622-1635]**

- **No double-ownership:** per-shard `assert_eq!(owner_count, 1, …)` @
  `anti_entropy.rs:1622-1626` (counts how many of the 3 pods return
  `owns_shard == true` for each shard).
- **No orphan:** `assert_eq!(total_owned, 64, …)` @ `anti_entropy.rs:1631-1635`
  (sum of pod1/pod2/pod3 shard counts == 64).
- **Even distribution (supplementary):** `min_owned >= 15`, `max_owned <= 25` @
  `anti_entropy.rs:1649-1658`.
- **Hash match:** `owns_shard` resolves through `owner_for_shard` (top-1) +
  `rendezvous_score` = `XxHash64(shard_id ‖ peer_id)` (`mode_a_coordinator.rs:112-145`),
  the exact `top1_by_score(hash(s ‖ pid))` of plan §14.5.

Reinforced by **pod-reassignment** @ `anti_entropy.rs:1733-1737`
(`owner_count == 1` on pod-3's former shards) and `:1751-1755` (`total_owned == 64`).

### Criterion 3 — Ownership redistributes (minimal reshuffling) when peer set changes size — **[COVERED for the core; "minimal reshuffling" sub-clause is a narrow GAP]**

Core assertion covered by **pod-reassignment: anti_entropy.rs:1714-1755**:

- **Peer set shrinks 3→2:** `PeerSet::new(["pod-1","pod-2"])` +
  `set_peer_set_for_test` @ `anti_entropy.rs:1714-1721`.
- **Redistribution asserted:** every shard pod-3 previously owned (captured @
  `:1705-1711`) is now owned by exactly one of pod-1/pod-2 — `assert_eq!(owner_count, 1)`
  @ `anti_entropy.rs:1733-1737`.
- **Full coverage preserved:** `assert_eq!(total_owned, 64, …)` @
  `anti_entropy.rs:1751-1755`.

> **Narrow GAP (the "minimal reshuffling" sub-clause):** the test records only
> pod-3's initial shards (`:1705-1711`) and re-checks only those. It never records
> pod-1/pod-2's initial shards and asserts they are **unchanged** after the resize.
> A buggy impl that reshuffled *all* shards on every peer-set change — while still
> preserving exactly-one-owner + full coverage — would pass. This is parent
> bf-1zn8a's gap (a): survivor-stability / minimal-reshuffling un-pinned for the
> Mode A background-work ownership path. (Also no 3→4 scale-up case exists — gap (b).)

So: the literal criterion-3 head ("ownership redistributes when peer set changes
size") = **COVERED**; the parenthetical "minimal reshuffling" qualifier = **GAP**.

### Criterion 4 — Test runs in default `cargo test` without `--ignored` or docker — **[GAP for "default cargo test"; sub-parts covered]**

- **No `#[ignore]`:** ✓ — plain `#[tokio::test]` @ `anti_entropy.rs:1540/1664/1760`.
- **No docker:** ✓ — in-memory `MockNodeClient` @ `anti_entropy.rs:1577/1787`;
  synthetic peer set via `set_peer_set_for_test` (`mode_a_coordinator.rs:306`), no
  SRV/DNS.
- **"Default `cargo test`":** **GAP.** The module is
  `#[cfg(all(test, feature = "peer-discovery"))]` @ `anti_entropy.rs:1532`, and
  `peer-discovery` is a **non-default** feature (`default = []` @
  `crates/miroir-core/Cargo.toml:67`; `peer-discovery = ["trust-dns-resolver"]` @
  `:71`). Plain `cargo test -p miroir-core mode_a` compiles **0** of these tests
  (confirmed empirically by sibling bf-qvrmh, RUN 1). They execute + pass only
  under `cargo test -p miroir-core --features peer-discovery` or `make test`
  (`--all-features`, `Makefile:8`).

⇒ **This is the genuine remaining gap** the later siblings should resolve. It is
*not* "rewrite acceptance_4" — it is "make the Mode A partitioning tests
discoverable/runnable in default `cargo test`" (e.g. move the module off the
`peer-discovery` gate; the coordinator only needs `set_peer_set_for_test`, which
already avoids SRV). This is the real narrow gap, per bf-1zn8a recommendation #5.

### Criterion 5 — Update/remove stale comment at `crates/miroir-core/tests/p13_8_anti_entropy.rs:240-244` — **[orthogonal action item, NOT a coverage item]**

The inline tests live in `src/anti_entropy.rs`, not in the `tests/p13_8_anti_entropy.rs`
file. The stale comment @ `p13_8_anti_entropy.rs:241-244` still claims the disabled
`acceptance_4_mode_a_shard_partitioning` "uses an old API (`with_mode_a_scaling`)"
and "requires the peer-discovery feature and ModeACoordinator" — now misleading
since the live replacement exists at `anti_entropy.rs:1532-1798`. It cannot be
"covered" by the inline suite; it is a standalone ~4-line edit (point at the
inline module, drop the dead `acceptance_4` reference). Track as its own action
under bf-1zn8a recommendation #4 — **not** marked [GAP] against the inline tests.

## Verdict matrix

| # | Criterion | Verdict | Covering test : line | Key assertion |
|---|---|---|---|---|
| 1 | Current API (not `with_mode_a_scaling`) | **[COVERED]** | partitioning-wiring `:1790`; exactly-one-owner `:1581-1587`; pod-reassignment `:1672-1686` | `with_mode_a` / `ModeACoordinator::new` |
| 2 | Exactly-one owner, no double/orphan, `top1_by_score(hash(s‖pid))` | **[COVERED]** | exactly-one-owner `:1622-1635` | `owner_count == 1`, `total_owned == 64` |
| 3 | Redistribution on peer-set change (minimal reshuffling) | **[COVERED] core / [GAP] minimal-reshuffle** | pod-reassignment `:1714-1755` | `owner_count == 1` @ `:1733`, `total_owned == 64` @ `1751` |
| 4 | Runs in default `cargo test`, no `--ignored`/docker | **[GAP] (default `cargo test`); sub-parts ✓** | — | gated `#[cfg(… feature = "peer-discovery")]` @ `anti_entropy.rs:1532`, `default = []` `Cargo.toml:67` |
| 5 | Update stale comment `p13_8:240-244` | **orthogonal action item** | — | inline tests are in `src/`, not the `tests/` file |

## Bottom line

- **Criteria 1, 2:** fully covered inline.
- **Criterion 3:** core covered inline; the *minimal-reshuffling / survivor-stability*
  sub-clause is the genuine narrow gap (a) — the thing a follow-on sibling should
  add (record pod-1/pod-2 initial shards, assert unchanged after resize).
- **Criterion 4:** the *default-`cargo-test`* requirement is the other genuine gap —
  the suite is feature-gated behind non-default `peer-discovery`. Fixing
  discoverability (not rewriting a test) is the sibling work.
- **Criterion 5:** orthogonal cleanup edit, not a coverage item.

The disabled `acceptance_4` is **redundant** — do not rewrite it. Direct later
siblings at gaps (a) survivor-stability, (b) 3→4 scale-up, (c) pinned hash-vector,
(4) default-feature discoverability, and the criterion-5 comment edit.

## Cross-refs

- `notes/bf-qvrmh.md` — empirical "do the three tests run?" foundation (RUN 1-4).
- `notes/bf-1zn8a.md` — parent umbrella; full Step 2 map + gaps (a)/(b)/(c).
