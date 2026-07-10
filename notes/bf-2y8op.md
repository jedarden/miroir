# bf-2y8op — CI test invocation & Mode A partitioning test locations

Read-only investigation (split-child of bf-2vj7m, STEP 1/4). No build, no test run.

## 1. CI test command

**CI config:** `k8s/argo-workflows/miroir-ci.yaml` — Argo `WorkflowTemplate` named `miroir-ci`
(`k8s/argo-workflows/miroir-ci.yaml:4`). This is the repo's actual CI; GitHub Actions are
disabled (no `.github/workflows/` dir exists).

The pipeline step `test` (`miroir-ci.yaml:43-44`) invokes the `cargo-test` template
(defined `miroir-ci.yaml:129`). The exact command CI runs, at **`miroir-ci.yaml:140`**:

```yaml
            export CARGO_TARGET_DIR=/workspace/target-test
            cargo test --all --all-features          # <-- line 140
```

**Answer: it is `--all`** (workspace-wide, with `--all-features`) — **NOT** `-p miroir-core`.

This matches the Makefile `test` target (`Makefile:8`: `cargo test --all --all-features`),
and the PR checklist (`cargo test --workspace`, `.github/pull_request_template.md:45`).

> Nuance for the 4 tests below: the `tests_mode_a_acceptance` module is `#[cfg(test)]` only
> — it was **ungated** from `#[cfg(all(test, feature = "peer-discovery"))]` by bf-1i588
> (`anti_entropy.rs:1518` note) and the `set_peer_set_for_test` infra was widened by bf-4t3o4
> specifically so these compile under `cargo test -p miroir-core` / `cargo test --all` with
> **no** `--features`. So all 4 DO execute under CI's `cargo test --all --all-features`.

## 2. The 4 Mode A partitioning tests

All in `crates/miroir-core/src/anti_entropy.rs`, inside `mod tests_mode_a_acceptance`
(`anti_entropy.rs:1528`, `#[cfg(test)]`). Lines below are the `async fn` definition line
(the `#[tokio::test]` attribute sits on the line immediately above).

| # | Test (bf-4t3o4 / plan ref) | Function name | `fn` line | task est. |
|---|---|---|---|---|
| 1 | 3-pod exactly-one-owner | `test_mode_a_three_pods_each_shard_processed_once` | **:1537** | ~:1541 ✓ |
| 2 | pod-reassignment | `test_mode_a_pod_reassignment` | **:1661** | ~:1665 ✓ |
| 3 | partitioning-wiring | `test_mode_a_anti_entropy_partitioning` | **:1757** | ~:1761 ✓ |
| 4a | bf-4t3o4 scale-down survivor stability | `test_mode_a_minimal_reshuffling_scale_down_survivor_stability` | **:1846** | — |
| 4b | bf-4t3o4 scale-up survivor stability | `test_mode_a_minimal_reshuffling_scale_up_survivor_stability` | **:1927** | — |

bf-4t3o4 (commit `0ade87a`, "test(mode-a): pin rendezvous minimal-reshuffling on peer-set
resize") added **two** tests — both the 3→2 (scale-down) and 3→4 (scale-up) peer-set resize
survivor-stability cases. They are the regression parent bf-30m2j cares about (surviving
pods must keep the shards they already owned, not just have the removed pod's shards
re-homed).

Attribute-line references: `#[tokio::test]` at :1536 / :1660 / :1756 / :1845 / :1926.

## Acceptance criteria

- [x] CI test command quoted (file path + line): `cargo test --all --all-features` at
      `k8s/argo-workflows/miroir-ci.yaml:140` → it is **`--all`**, not `-p miroir-core`.
- [x] Each partitioning test: function name + exact line recorded above (5 fns; bf-4t3o4
      added 2 of them).
