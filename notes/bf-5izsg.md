# bf-5izsg — Rate-limit fix presence verification (CODE PRE-FLIGHT)

Split-child of **bf-146g8**, step 2 of 4. Verification only — **no code
reimplemented** (per instructions). Outcome recorded below and flagged for the
parent.

## Premise under test (from bf-146g8)

> The fix is ALREADY in the working tree — `crates/miroir-proxy/src/routes/search.rs`
> + a search-ui rate-limit test + `CHANGELOG.md`.

## git state (working tree)

- `git status --short`: only `.beads/issues.jsonl` + `.needle-predispatch-sha`
  modified (bookkeeping). `.beads/traces/bf-146g8/` + `bf-5h6d3/` untracked.
- `git diff --stat`: only the two bookkeeping files (20 + 2 lines). **No code
  changes are uncommitted anywhere.**

## Per-element findings

### 1. `crates/miroir-proxy/src/routes/search.rs` — PRESENT, COMMITTED, no pending diff

- File exists (56 KB). Tracked; last commit `73a29e1` (~6 weeks ago).
- Rate-limit **logic is present and committed** (plan §4):
  - L192–236 and L1029–1073: "Check rate limit for search UI" → reads
    `state.config.search_ui.rate_limit.per_ip`, branches on `.backend`
    (`redis` → `check_rate_limit_search_ui`; else `local_search_ui_rate_limiter`),
    returns `429 TOO_MANY_REQUESTS` when exceeded.
- **But:** `git diff` on this file is empty — this is long-pre-existing
  functionality, **not** a new/pending fix.

### 2. search-ui rate-limit test — **ABSENT**

- No `search_ui_rate_limit.rs` anywhere in the repo (by name or by content grep).
- Closest test files are unrelated to a *search-ui* rate-limit test:
  - `crates/miroir-proxy/tests/p10_7_admin_login_rate_limit.rs` — **tracked/committed**,
    720 lines, 54 `admin` refs, **0** `search_ui` refs. Tests **admin login**
    rate limiting (plan §9), not the search-ui route.
  - `charts/miroir/tests/bad-search-ui-rate-limit-local-multi.yaml` — tracked/committed
    (~3 months ago). A Helm `values.schema.json` rejection test (rule 3: local
    rate-limit backend with replicas>1). Different layer than a proxy route test.
  - `crates/miroir-core/src/config/validate.rs:273 rejects_search_ui_rate_limit_redis_without_redis`
    — committed config-schema validation test, not a proxy route test.
- Definitive grep (`fn .*search.?ui.*rate`, `mod search_ui_rate`, etc.) over all
  `crates/**/*.rs` returns nothing relevant. **No search-ui rate-limit test exists,
  committed or uncommitted.**

### 3. `CHANGELOG.md` — PRESENT, COMMITTED, no rate-limit-fix entry, no pending diff

- Tracked; last commit `e348157` (~7 weeks ago).
- The `[Unreleased]` section is **empty** (Added/Changed/Deprecated/Removed/Fixed/Security all blank).
- Only "rate limit" mention is the pre-existing v0.1.0 line about
  `values.schema.json` rejecting incompatible configs ("local rate limits in
  multi-replica"). **No entry for a search-ui rate-limit fix.**
- `git diff` on the file is empty.

## Conclusion — premise NOT confirmed

The fix, **as a new/pending change** (search.rs diff + a search-ui rate-limit test
+ CHANGELOG entry), is **not present** in the working tree:

| Element | Expected | Actual | State |
|---|---|---|---|
| search.rs rate-limit change | pending/uncommitted | logic present but **committed ~6wk ago**, no diff | pre-existing |
| search-ui rate-limit test | present | **absent** (only admin-login test exists) | missing |
| CHANGELOG entry | present | **empty Unreleased**, no mention | missing |

The underlying search-ui rate-limit *functionality* in `search.rs` does exist and
is committed, but the accompanying test and CHANGELOG entry the parent expected do
not, and there is **no uncommitted diff** representing a fix-in-progress.

**Recommendation to parent (bf-146g8):** do not assume the fix is already done.
If a distinct search-ui rate-limit fix is required, it (test + CHANGELOG entry,
and any search.rs delta beyond the pre-existing committed logic) still needs to be
written. A build cycle is **not** warranted to "verify" a fix that isn't present.

## Verification method

`git status --short`; `git diff --stat`; `git ls-files --error-unmatch`;
`git log -1 -- <path>`; `find -iname '*rate*limit*'`; content grep over
`crates/**/*.rs` and `CHANGELOG.md`. Corrected an initial path error (proxy tests
live in `crates/miroir-proxy/tests/`, not `crates/miroir/tests/`) — that file is
tracked, not new.
