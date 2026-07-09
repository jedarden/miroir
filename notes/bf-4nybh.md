# bf-4nybh — Confirm-or-refute the Mode A partitioning test gaps (step 3)

Investigation step 3 of umbrella **bf-1zn8a**. Independent re-verification of the
three candidate coverage gaps (a/b/c) so later siblings attack only REAL holes.

## TL;DR — all three CONFIRMED (genuine gaps)

| # | Candidate gap | Verdict | Key evidence |
|---|---|---|---|
| (a) | survivor-stability / minimal-reshuffling not pinned | **CONFIRMED** | `anti_entropy.rs:1665` records only `pod3_owned_initial`; never asserts survivors keep their shards |
| (b) | no scale-UP (3→4); only scale-DOWN (3→2) | **CONFIRMED** | only Mode A resize is 3→2 (`anti_entropy.rs:1714`); the one 3→4 test is a *different subsystem* |
| (c) | no pinned shard→owner golden vector vs `top1_by_score(hash(s‖pid))` | **CONFIRMED** | `owner_for_shard` is never called by any test; only boolean `owns_shard` is asserted |

**GENUINE gaps = { (a), (b), (c) }** — all three. Nothing was refuted as
already-covered.

---

## Verdict (a) — CONFIRMED: survivor-stability / minimal-reshuffling

`test_mode_a_pod_reassignment` (`crates/miroir-core/src/anti_entropy.rs:1665`)
goes 3→2 (kill pod-3) and asserts two things about pod-3's *old* shards:

- each previously-pod-3 shard is now owned by exactly one of pod-1/pod-2 —
  `owner_count == 1`, `anti_entropy.rs:1731-1737`
- all 64 shards are still owned — `total_owned == 64`, `anti_entropy.rs:1751-1755`

What it records initially is **only** `pod3_owned_initial`
(`anti_entropy.rs:1704-1711`). It never records pod-1's or pod-2's pre-removal
shards and never asserts they are unchanged afterward. A buggy implementation
that reshuffled **every** shard on each peer-set change — while still preserving
exactly-one-owner + full coverage — would pass this test.

The "minimal reshuffling" property is an explicit Mode A design invariant:
`mode_a_coordinator.rs:19` ("ownership with minimal reshuffling") and
`plan.md` §14.5 (cited at `mode_a_coordinator.rs:1`). Un-pinned for the
background-work ownership path. **CONFIRMED.**

## Verdict (b) — CONFIRMED: no Mode A scale-up

The only Mode A peer-set *mutation* anywhere is the 3→2 downscale
(`anti_entropy.rs:1714`, `peer_set_2pods`). A whole-crate grep for `"pod-4"` /
`peer_set_4pods` / `4pod` returns **nothing** — no Mode A test adds a 4th pod.

One apparent 3→4 scale-up test exists —
`p4_topology_chaos.rs:282` (`chaos_rebalance_optimal_movement`, adds `node-3`) —
but it does **not** cover Mode A and therefore does not refute the gap:

- It runs in the **Rebalancer / Topology** subsystem, which never references
  `ModeACoordinator` / `owner_for_shard` / `rendezvous_score`
  (`grep` of `rebalancer.rs` / `topology.rs` for those symbols → empty).
- It routes through `assign_shard_in_group` → router's `score()`
  (`router.rs:25`, `router.rs:12`), which is a **different hash construction**
  from Mode A's `rendezvous_score`:
  - router `score(shard_id: u32, node_id: &str)`: `XxHash64::with_seed(0)` →
    `shard_id.hash(h)` (raw `u32` bytes) → `node_id.hash(h)` (std `str` Hash,
    appends a `0xff` terminator). No separator.
  - Mode A `rendezvous_score(shard_id: &str, peer_id: &str)`
    (`mode_a_coordinator.rs:112`): `XxHash64::with_seed(0)` →
    `write(shard_id.as_bytes())` (decimal string, e.g. `"42"`) → `write(b"||")`
    → `write(peer_id.as_bytes())`. No `0xff` terminator.

  Different input encoding, different separator, different terminator ⇒ different
  scores, different code path. The p4 test validates the rebalancer's migration
  accounting, not Mode A rendezvous partitioning. **Mode A scale-up is genuinely
  untested. CONFIRMED.**

## Verdict (c) — CONFIRMED: no pinned shard→owner golden vector

`owner_for_shard` (`mode_a_coordinator.rs:123`) is the only API that returns a
*concrete* owner, yet it has exactly **one** call site — internal, inside
`owns_shard` (`mode_a_coordinator.rs:156`). No test calls it
(`grep -rn "owner_for_shard" --include=*.rs crates/ | grep -v fn` → only `:156`).

Every Mode A ownership test instead calls the boolean `owns_shard` and asserts
only *properties*:

- `test_owns_exactly_one_peer_per_item` (`mode_a_coordinator.rs:422`): exactly
  one pod owns each item — never *which* pod.
- `test_mode_a_three_pods_each_shard_processed_once`
  (`anti_entropy.rs:1541`): exactly-one-owner + full coverage + even spread.
- `test_rendezvous_score_*` (`mode_a_coordinator.rs:317/325/333`): determinism
  + peer-sensitivity + shard-sensitivity of the raw score — never a pinned
  owner.
- `tests/hash_fixtures.rs` only `println!`s (no `assert!`) and covers the
  router's key→shard *modulo* mapping, not Mode A ownership.

So no test pins a specific shard→owner mapping against the documented
`top1_by_score(hash(shard_id‖peer_id))` rule (`mode_a_coordinator.rs:112-118`).
A change to the hash construction that preserves those properties while silently
reassigning all ownership — e.g. swapping the `"||"` separator
(`mode_a_coordinator.rs:115`) to `"|"`, swapping the shard/peer write order, or
reseeding `XxHash64` (`:113`) — would pass every existing test yet break the
documented `hash(s‖pid)` contract. A golden-vector test (fixed `peer_set` +
hardcoded expected owner per shard) is the missing guard. **CONFIRMED.**

---

## Corroboration note

These verdicts independently reproduce the gap analysis already recorded in the
sibling note `notes/bf-1zn8a.md` (step 3, lines 153-208). This bead's added
value is the explicit, cited refutation-of-the-refutation for (b): the
`p4_topology_chaos` 3→4 test looks like it could cover scale-up, but it targets
the separate Rebalancer/Topology subsystem with router's `score()` — a different
hash construction — so it does **not** close the Mode A gap.

## Acceptance criteria (bf-4nybh)

- [x] Verdict for (a) minimal-reshuffling: **CONFIRMED** —
      `anti_entropy.rs:1665,1704-1711,1731-1737,1751-1755`; property at
      `mode_a_coordinator.rs:19`.
- [x] Verdict for (b) scale-up: **CONFIRMED** — only Mode A resize is 3→2
      (`anti_entropy.rs:1714`); no `pod-4` crate-wide; the p4 3→4 test
      (`p4_topology_chaos.rs:282`) uses router `score()` (`router.rs:12`) ≠
      `rendezvous_score` (`mode_a_coordinator.rs:112`) and never touches
      `ModeACoordinator`.
- [x] Verdict for (c) pinned hash vector: **CONFIRMED** — `owner_for_shard`
      (`mode_a_coordinator.rs:123`) never called by tests; only boolean
      `owns_shard` asserted; rule at `:112-118`.
- [x] Final GENUINE gaps = { (a), (b), (c) }.
