# bf-68dkx — Run cargo test -p miroir-core (default, non-ignored) + CI cmd, capture output

Split-child of bf-2vj7m (STEP 3/4). Depends on bf-58sk6 (compile-only SUCCESS).
No source changes produced — this note + the comment on the bead are the commit artifact.

Ran the FULL default test path (no `--ignored`, no docker-compose) and, per bf-2y8op,
CI's actual command (`cargo test --all --all-features`). Real cargo binary invoked directly
(`$HOME/.cargo/bin/cargo`) because the `~/.local/bin/cargo` wrapper discards stderr, and cargo
writes ALL pass/fail output to stderr (bf-58sk6 gotcha §3).

## Run 1 — `cargo test -p miroir-core` (default features): EXIT 101

**All executable tests pass. Only ONE doctest fails to compile.**

- Lib unittests: `test result: ok. 750 passed; 0 failed; 0 ignored` (incl. all 5 Mode A
  partitioning tests bf-2y8op located — `tests_mode_a_acceptance` compiled ungated).
- Every integration-test binary: `ok`.
- Single failure = doctest `peer_discovery.rs:16`:

```
error[E0609]: no field `peers` on type `Result<PeerSet, MiroirError>`
  --> crates/miroir-core/src/peer_discovery.rs:35:43
21 |     println!("Discovered {} peers", peers.peers.len());
   |                                           ^^^^^ unknown field
help: ... peers.unwrap().peers.len();
test result: FAILED. 1 passed; 1 failed; 6 ignored; ... ; finished in 1.25s
```

Cause: doc-example does `let peers = discovery.refresh().await;` (returns
`Result<PeerSet, MiroirError>`) then `peers.peers.len()`, treating the Result as a PeerSet.
`peer_discovery.rs` is NOT in the working-tree changes → pre-existing on HEAD.

## Run 2 — `cargo test --all --all-features` (CI's exact cmd): EXIT 101, DOES NOT COMPILE

More severe: CI's own command fails at **compile time**; **zero** test binaries run (no
`test result:` lines; 97-line log = build warnings + 1 error):

```
error[E0384]: cannot assign twice to immutable variable `runner`
   --> crates/miroir-proxy/src/main.rs:822:21
809 |             let runner = CanaryRunner::new(...)
822 |                     runner = runner.with_mode_a(coordinator.clone());
help: let mut runner = CanaryRunner::new(...)
error: could not compile `miroir-proxy` (bin "miroir-proxy") due to 1 previous error
```

Cause: inside `#[cfg(feature = "peer-discovery")]` (main.rs:819-823), `runner` is reassigned
but declared immutable. Only manifests with `--all-features` (enables `peer-discovery`), NOT in
Run 1's default-feature scope. Last touched by commit `73a29e1`; `main.rs` NOT in working-tree
changes → pre-existing on HEAD. Fix = `let mut runner` at main.rs:809.

## Acceptance criteria

- [x] `cargo test -p miroir-core` run with no `--ignored`, no docker-compose (EXIT 101; full
      output in comment #26 on the bead, incl. result-summary line).
- [x] Full test output (untruncated tail with result summary) pasted in comment #26.
- [x] Bonus: CI's actual `cargo test --all --all-features` also run → compile-fails miroir-proxy.

## Net for parent bf-2vj7m

Two distinct, independent, pre-existing defects (neither caused by the current anti_entropy.rs /
reshard/executor.rs working-tree changes):
1. Doctest `peer_discovery.rs:16` — stale doc example (Result vs PeerSet). Blocks Run 1.
2. `miroir-proxy/src/main.rs:822` E0384 — needs `let mut runner` under `feature = "peer-discovery"`.
   Blocks CI's `--all --all-features`. This is the one that makes CI itself RED.

Log files retained at `/tmp/miroir-core-test.log` (1244 lines) and `/tmp/miroir-all-test.log`
(97 lines) for this session.
