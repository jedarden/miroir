# bf-1ccqa — Post coverage map + gap analysis as comments (close-out)

Step 4 / close-out of the Mode A partitioning coverage investigation.
Parent umbrella: bf-1zn8a. Grandparent umbrella: bf-30m2j.

## What this bead did

Aggregated the now-closed sibling findings into ONE authoritative close-out
comment and posted it on both umbrellas so the record is durable and later
children attack the real (narrow) gaps instead of rewriting the redundant
disabled `acceptance_4` test.

- **Posted comment [19] on bf-30m2j** (grandparent) — corrects its stale premise
  that `acceptance_4` is the only/sole coverage.
- **Posted comment [20] on bf-1zn8a** (umbrella) — durable close-out record.

(These are distinct from the parent's earlier preliminary comments [17]/[18],
"investigative step 1", which predated the siblings closing. The close-out
comments cite the closed siblings bf-qvrmh / bf-59oct / bf-4nybh and their
notes files as the source of truth.)

## Sources aggregated

- bf-qvrmh — cargo test run evidence (notes/bf-qvrmh.md)
- bf-59oct — per-criterion coverage map (notes/bf-59oct.md)
- bf-4nybh — gap confirm/refute verdicts (notes/bf-4nybh.md)

## Line numbers re-verified against the current working tree before posting

- mode_a_coordinator.rs: `ModeACoordinator::new` :66, `rendezvous_score` :112
  (private), separator/seed :113/115, `owns_shard` :151,
  `set_peer_set_for_test` :306, `test_owns_exactly_one_peer_per_item` :422.
  Module is `#[cfg(feature = "peer-discovery")]` at lib.rs:42; `default = []`
  at Cargo.toml:66 — so these tests do NOT run in plain `cargo test`, only under
  `--features peer-discovery` / `make test` (--all-features). Criterion 4 is
  PARTIALLY met.
- anti_entropy.rs: three-pods test :1541 (owner_count==1 :1623, total_owned==64
  :1632), pod-reassignment :1665 (pod3_owned_initial :1705, re-check :1724,
  re-homed-to-one-survivor :1734, all-64-owned :1752), partitioning-wiring
  :1761, `with_mode_a` :199 (call sites :1581/1584/1587/1790).

## Genuine gaps (the narrow holes for later children — NOT a test rewrite)

- (a) No minimal-reshuffling / survivor-stability assertion in
  test_mode_a_pod_reassignment (anti_entropy.rs:1665). CONFIRMED.
- (b) No 3->4 scale-UP case; only 3->2 scale-down tested. CONFIRMED.
- (c) No pinned golden-vector test locking the top1_by_score(hash(s||pid))
  contract (rendezvous_score is private). CONFIRMED, lower priority.
- (5) Stale comment at p13_8_anti_entropy.rs:240-244 still references removed
  with_mode_a_scaling — trivial orthogonal edit.

Directive to children: do NOT rewrite acceptance_4 (redundant). Extend
pod-reassignment with survivor stability, add a scale-up test, add a golden
vector, and fix the stale comment.
