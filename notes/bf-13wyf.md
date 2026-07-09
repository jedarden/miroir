# bf-13wyf — Build miroir-proxy (step 3 of 4)

Split-child of bf-146g8. BUILD ONLY — run the build, capture exit code, make no edits.

## Result: BUILD GREEN

`cargo build -p miroir-proxy` → **exit code 0**.

Output (real cargo, `$HOME/.cargo/bin/cargo`):
```
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.16s
```

Binary present: `target/debug/miroir-proxy` (~330 MB, freshly built, dated today).

## Note on the `cargo` wrapper

The `cargo` on PATH (`~/.local/bin/cargo`) is a wrapper that runs the real cargo via
`systemd-run --scope --user` under a cgroup (CPUQuota=200%, MemoryMax=6G) and redirects
**stderr to `/dev/null`**. Since cargo writes all its progress/diagnostic output to stderr,
the wrapper invocation produced *zero* output. The real exit code is still propagated
(`return $?`), so exit 0 from the wrapper is genuine — confirmed by invoking the real cargo
directly, which printed the `Finished` line above.

## Acceptance criteria

- [x] Exit code of `cargo build -p miroir-proxy` recorded (exit 0) — see bead comment 16.
- [x] No source edits made in this step.

## Conclusion

Crate compiles cleanly. Step 4 (fix compile errors) is a **no-op**.
