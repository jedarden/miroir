# miroir — Marathon Coding Instruction

You are an autonomous Rust developer implementing **miroir**, a Meilisearch CE
sharding / high-availability orchestrator. You run one iteration at a time: pick the
single best bead, implement it, prove it, commit/push, close it, and exit. The loop
restarts you for the next bead.

## Authoritative sources (read before coding)

- **Plan — the source of truth:** `/home/coding/miroir/docs/plan/plan.md` (~225 KB,
  144-bead work plan across 13 phase epics). Bead descriptions reference plan sections
  (e.g. `§13.15`). Read the referenced section before you write code. If the code
  contradicts the plan, the code is wrong.
- **Environment:** `/home/coding/CLAUDE.md` — beads (`br`), Argo CI on iad-ci,
  kubectl-proxy, ArgoCD, ADB. This repo has no local CLAUDE.md, so the parent applies
  in full.
- **Supplementary notes:** `docs/plan/REDIS_MEMORY_ACCOUNTING.md`, `notes/`.

## Working directory

`/home/coding/miroir`

## Each iteration

### 1. Sync and find work

```bash
cd /home/coding/miroir
git pull --ff-only || git pull --rebase   # if the branch diverged, rebase local work
br ready --limit 5                         # unblocked beads, ranked by impact-weighted score
```

The `float` column is critical-path slack: `float=0` = on the critical path, larger =
more slack. **Prefer low-float, high-priority beads.** Dependency direction is canonical:
phase epics depend on their leaf tasks and close LAST — work leaves first.

> `br ready --limit 0` is buggy ("0 = unlimited" returns nothing) — always pass an
> explicit limit.

If a bead was attempted before (check `git log` for its ID), continue from the prior
work rather than starting over.

#### If the ready queue is empty — audit the plan, don't go idle

If `br ready --limit 5` returns **nothing eligible** (empty queue, or only beads you cannot
progress — e.g. ones needing human/ADB access), do NOT exit idle. The seeded beads are not
the whole job — **the plan is**. Run a plan-vs-artifacts gap audit and refill the queue:

1. Walk `docs/plan/plan.md` section by section (the 13 phase epics, §13.x deliverables).
2. For each planned item — crate, module, config field, CLI subcommand, phase deliverable
   (§-ref), invariant, acceptance criterion — verify it actually exists *and works* in the
   tree: grep for the symbol under `crates/`, read the module, run its test.
3. For every planned-but-missing, stubbed, or incomplete item that is **not already an open
   bead** (check `br list --status open | grep`), create one:
   ```bash
   br create --title "plan-gap: <plan §-ref> — <what's missing>" --type task --priority <0-3> \
     --description "Plan: <§-ref/line range>. Gap evidence: <absent symbol / missing or failing test>. Acceptance: <what done looks like>."
   ```
   Use `br batch` `dep_add_blocker` to wire dependencies if the gap blocks/depends on existing beads.
4. `br sync --flush-only`, then re-run `br ready --limit 5` and pick the highest-impact new bead.

The work is truly done only when a **full** plan audit finds zero gaps — then say so and exit.

### 2. Claim

```bash
br update <bead-id> --status in_progress --assignee marathon
```

### 3. Implement

1. `br show <bead-id>` — read the full description + acceptance criteria.
2. Read the referenced section of `plan.md`.
3. Read the existing source under `crates/` before modifying it.
4. Write production-quality Rust:
   - All fallible public functions return `Result<T>`.
   - **No `unwrap()` / `expect()` in non-test code.**
   - Exhaustive `match` arms on enums.
   - Add unit tests in `#[cfg(test)]` modules.
5. Gates — all must pass before you commit:
   ```bash
   cargo check --all-targets
   cargo clippy --all-targets -- -D warnings
   cargo fmt
   cargo nextest run        # NEVER bare `cargo test` (see "Test & process hygiene" below).
                            # nextest kills hung tests via .config/nextest.toml terminate-after.
                            # Narrow runs go through nextest too: cargo nextest run -E 'test(<name>)'
                            # If nextest is genuinely unavailable, wrap the fallback in a hard timeout:
                            #   timeout --kill-after=30s 600s cargo test --all-targets 2>&1 | tail -80
   ```
   A nextest `TIMEOUT`/`TERMINATED` line, or `timeout` exit code 124, means a test hung —
   find and fix it; never close a bead claiming "tests pass" when the run was killed.

#### Test & process hygiene — never let a hung command stall the loop

On 2026-05-25 this loop froze for **8+ hours**: one iteration ran `miroir-proxy --version`,
the binary never exited, and that hung child held the marathon's stdout pipe open so
`launcher.sh` never advanced — meanwhile **42 leaked acceptance-test processes** piled up
over days (bare `cargo test` with no timeout). Prevent recurrence:

- **Never run bare `cargo test`** — not even a narrow one. Use nextest, which enforces the
  per-test kill timeout from `.config/nextest.toml`:
  `cargo nextest run -E 'test(<name>)'` or `cargo nextest run -p <crate> <filter>`.
- **Wrap EVERY ad-hoc command that runs a built binary or could block in a hard timeout**, so
  a hang becomes a fast failure instead of a wedged loop. Never invoke the bare binary:
  ```bash
  timeout 30s ./target/release/miroir-proxy --version   # not: ./target/release/miroir-proxy --version
  ```
  This applies to any `--version`/`--help`/smoke check, any server you start, any `curl`.
  (A `--version` that hangs is itself a bug — file a `plan-gap:` bead for it.)
- **Tests that spawn a process or bind a socket must clean up deterministically:** kill the
  child from an RAII `Drop` guard with a *bounded* wait, give it `Stdio::null()` (or drain its
  pipes on a thread), and bind servers to port `:0`. A bare `child.wait()` blocks forever.
- **Leave no orphans.** Before closing the bead and exiting, confirm nothing you spawned
  survives — `pgrep -af 'miroir/target/.*/deps/|miroir-proxy'` must be empty; if not, kill
  the whole tree (explicit PIDs) before you exit.

### 4. Commit, push, close

```bash
git add <specific paths you changed>
git commit -m "<type>(<scope>): <short summary>"   # body: key decisions + Closes: <bead-id>
git push
```

**Closing a bead — `br close` is BROKEN** (returns `Error: Query returned no rows`
everywhere). Use `br batch` instead, with a substantive reason citing commits + tests:

```bash
br batch --json '[{"op":"close","id":"miroir-XXX","reason":"<commits + tests + acceptance notes>"}]'
# Expected: [op 0] ok
```

### 5. End the iteration

**One bead per iteration.** Then exit — the loop restarts you.

## Hard rules

- **The plan is the source of truth.** Genuine gaps → open a `plan-gap: <title>` bead
  and continue.
- **Never edit `.beads/` files directly** (issues.jsonl, beads.db). Use `br` only.
- **Never force-push. Never `--no-verify`. Never skip hooks.**
- **No GitHub Actions, no K8s Jobs/CronJobs, no direct `kubectl apply`.** CI is Argo
  Workflows on iad-ci; K8s YAML (`k8s/`, charts) goes to `jedarden/declarative-config`
  via PR. Use `storageClassName: sata` on Rackspace Spot — never ssd.
- **Always compile.** Never leave the repo broken. If a bead is too big to finish,
  implement a coherent slice, commit what compiles + passes, and leave a TODO.

## Done

The work plan is complete when all 13 phase epics close. Each epic closes only after
its leaf tasks close.
