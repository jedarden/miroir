# bf-58sk6 — disk check + miroir-core compile-only

Split-child of bf-2vj7m (STEP 2/4). Depends on bf-2y8op (Child 1, CI-cmd investigation).
Isolates "does it build" from "do tests pass". No source changes produced — this note is the
commit artifact.

## 1. Disk hygiene (per ~/CLAUDE.md)

```
$ df -BG --output=avail / | tail -1
 26G
```

26G free on `/` — **above** the 20G pressure threshold. **No `target/` cleared.** Rationale:
CLAUDE.md says only clear an idle `target/` when free space drops *below* ~20G; clearing
otherwise destroys incremental-compilation caches and forces a slow cold rebuild. Largest
target dirs for reference: `~/miroir/target` 44G, `~/HOOP/target` 43G (neither touched).

## 2. Compile-only result — SUCCESS

```
$HOME/.cargo/bin/cargo test --no-run -p miroir-core
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.15s
    Executable unittests src/lib.rs (...)
    Executable tests/chaos.rs (...)
    ... (30 test binaries total) ...
PIPESTATUS=0
```

All 30 miroir-core test binaries (lib unittests + 29 integration tests) compiled cleanly.
**No compile errors.** Exit code 0.

One **benign warning** (not an error):
```
warning: unused import: `super::*`
   --> crates/miroir-core/src/anti_entropy.rs:1812:9
```
This is inside `mod tests_mode_a_acceptance` (the test module the parent bead bf-2y8op
documented). `anti_entropy.rs` is already in the working tree as modified (pre-existing),
so this warning is not introduced by this bead and was left untouched.

## 3. Gotcha for the test-running siblings (STEP 3/4): the cargo wrapper swallows stderr

`cargo` on this box is a wrapper (`/home/coding/.local/bin/cargo`, 788-byte bash script)
that runs the real cargo under `systemd-run --scope --user` with cgroup limits
(CPUQuota=200%, MemoryMax=6G, MemorySwapMax=0) — line 9 ends in `2>/dev/null`.

Cargo writes **all** its `Compiling`/`Finished` progress to **stderr**, which the wrapper
discards. So `cargo test ... > log 2>&1` produces an **empty log** even on success — only the
**exit code** (0 = success) is reliable through the wrapper. To see real build/test output,
invoke the real binary directly:

```bash
$HOME/.cargo/bin/cargo test --no-run -p miroir-core   # visible output
$HOME/.cargo/bin/cargo test -p miroir-core            # actually run tests, visible
```

Note: direct invocation bypasses the 6G MemoryMax cgroup. Fine for `--no-run` (done here);
for a full test run the sibling should decide wrapper-vs-direct based on whether it needs to
see pass/fail detail (which cargo also writes to stderr).
