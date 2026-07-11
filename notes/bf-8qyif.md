# bf-8qyif — Hygiene sweep: purge tracked artifacts, dead CI workflows, doc drift

**Date:** 2026-07-11
**Follow-up to:** 2026-07-11 corpus-audit

## Checker

`~/.claude/skills/repo-hygiene/scripts/repo_hygiene.sh --json .`

## Outcome by fix category (one commit per category)

| Category | Status | Detail |
|----------|--------|--------|
| (a) `.gitignore` gaps | ✅ clean | No `gitignore-gaps` finding. `node_modules/`, `crates/miroir-proxy/admin-ui/dist/`, `/target/`, etc. already ignored — added in `998d974`. |
| (b) Tracked build artifacts | ✅ clean | `tracked-build-artifacts` = 0. admin-ui `dist/` build artifacts untracked in `6b70509`. |
| (c) Dead CI workflows | ✅ clean | No `.github/workflows/*.yml\|yaml` tracked — `dead-ci-workflows` = 0. Nothing to delete. |
| (d) README version / CI-badge drift | ⚠️ false positive (left unchanged) | See below. |

### Category (d) — false positive, intentionally not changed

The checker reported `readme-version-drift`: "README.md says '2.0.0' but latest tag is 'v0.1.0'".

The matched string is the **SemVer specification badge** on README line 4:

```
[![SemVer](https://img.shields.io/badge/SemVer-2.0.0-blue)](https://semver.org/spec/v2.0.0.html)
```

The `2.0.0` here is the **Semantic Versioning specification version** (the badge links to
`semver.org/spec/v2.0.0.html`), declaring that Miroir follows the SemVer 2.0.0 spec — it is
**not** the project's release version. The project's actual release version is shown by the
dynamic `Latest Release` badge on line 5 (`github/v/release/jedarden/miroir`), and README
line 276 confirms the project is at `v0.x`. Rewriting the badge to `v0.1.0` would corrupt a
legitimate spec badge and break its link. **No change made.** (Checker regex false positive:
it matches any `\d+.\d+.\d+` in a shields.io URL without semantic context.)

No GitHub Actions / CI badge exists in the README, so `readme-dead-ci-badges` is also clean.

## Remaining findings (out of scope for this bead)

- **`large-tracked-files`** (4, high): score-comparability corpus fixtures
  (`tests/benches/.../corpus/{corpus,shard-01}.jsonl` + `docs/research/...` copies, ~46–50 MB each).
  These are committed **test/research corpus data**, not build artifacts or binaries — outside
  the four fix categories and the acceptance criteria.
- **`dirty-working-tree`** (32) and **`stash-pileup`** (91): report-only context per task
  instructions — not acted on (no stash drops, no tree resets).

## Acceptance criteria

- tracked build artifacts = **0** ✅
- dead workflow files = **0** ✅
- gitignore gaps = **0** ✅

All met. This audit produced no source/artifact changes; the in-scope hygiene work was
completed and pushed in `998d974` + `6b70509`. This note is the required commit artifact.

## Final checker summary

```
findings:
  - large-tracked-files   (high, 4)   — corpus test fixtures, out of scope
  - readme-version-drift  (low,  1)   — false positive (SemVer spec badge), left unchanged
  - dirty-working-tree    (low, 32)   — report-only
  - stash-pileup          (low, 91)   — report-only
clean: false   (only the report-only / out-of-scope categories above remain)
```
