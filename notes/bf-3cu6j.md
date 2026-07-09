# bf-3cu6j — Fix compile errors in changed code (step 4 of 4, FINAL)

Split-child of **bf-146g8**, step 4 of 4 (FINAL). **COMPILE FIXES ONLY.**

## Task

If step 3 (bf-13wyf) build already exited 0 → this is a **no-op**:
record "build was already green" and close. If it failed → fix *only*
compile errors in the changed code (search.rs / test / CHANGELOG), no
logic rewrite of `extract_source_ip` / rate-limit code, then re-run
`cargo build -p miroir-proxy` until exit 0.

## Result: BUILD ALREADY GREEN — NO-OP

Step 3 (bf-13wyf) recorded **exit 0**. This step re-verified directly rather
than trusting the prior note.

### Verification (real cargo, not the wrapper)

`$HOME/.cargo/bin/cargo` (symlink → rustup) — i.e. the *real* cargo, not the
PATH wrapper that redirects stderr to `/dev/null`.

1. Incremental build:
   ```
   Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.17s
   exit 0
   ```
2. Forced recompile to rule out a stale cache — `touch`ed
   `crates/miroir-proxy/src/routes/search.rs` (the file this task names) and
   rebuilt:
   ```
   Compiling miroir-proxy v0.1.0 (/home/coding/miroir/crates/miroir-proxy)
   Finished `dev` profile [unoptimized + debuginfo] target(s) in 8.84s
   exit 0
   ```
   The crate genuinely recompiled (8.84s) with **zero errors and zero warnings**.

### Why there is nothing to fix

- The working tree has **no uncommitted source changes** — only `.beads/`
  bookkeeping (`issues.jsonl`, `.needle-predispatch-sha`) and untracked trace
  dirs. No diff to `search.rs`, no new test, no CHANGELOG entry.
- Per bf-5izsg (step 2), the rate-limit *functionality* in `search.rs` is
  **pre-existing committed code** (~6 weeks old), not a new in-progress fix.
  There is no half-applied change that could be broken.
- The `touch` in step 2 above changed only mtime, not content — the working
  tree is unaffected.

## Acceptance criteria

- [x] `cargo build -p miroir-proxy` exits 0 (verified twice, incl. forced recompile).
- [x] No behavioral/logic rewrite of extraction or rate-limit code — none was
      needed; no compile errors existed to fix.

## Conclusion

Build is green. **No-op.** No code edits made in this step.
