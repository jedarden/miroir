# bf-2guj4 — Verify no hardcoded unknown stubs and IP precedence matches admin_endpoints.rs

**Result: VERIFICATION FAILED — the rate-limit fix is NOT wired. All three acceptance criteria unmet.**
Bead type: verify-only (static/code verification — no build, no test execution, no code changes).
Scope: confirm the search-UI per-IP rate-limit fix is wired into `search_handler` / `search_multi_targets`
and matches the trusted-proxy pattern in `admin_endpoints.rs`.

## Summary

The original bug (parent `bf-4fk3l`: `source_ip` hardcoded to the literal `"unknown"`) is **still
present**. `extract_source_ip` does not exist anywhere in the codebase, neither handler calls any
source-IP extraction, and both handler bodies still contain `let source_ip = "unknown".to_string();`
with TODO comments. The per-IP search-UI rate limit is therefore still a global no-op: every client
maps to the single `"unknown"` bucket, so the configured per-IP limit collapses to one shared bucket.

This directly contradicts parent `bf-ml7fg`'s claim that "at split time the fix is ALREADY
IMPLEMENTED in the uncommitted working tree" (with `extract_source_ip` in search.rs, a `source_ip`
param on `search_multi_targets`, a `search_ui_rate_limit` test file, and a CHANGELOG trusted-proxy
note). None of those are present — see Evidence table.

## Acceptance-criteria check

### 1. "No hardcoded 'unknown' source_ip in handler bodies (only the fallback inside extract_source_ip)" — FAIL

Hardcoded stubs remain in **both** handler bodies:

- `crates/miroir-proxy/src/routes/search.rs:189-190` (inside `search_handler`):
  ```rust
  // TODO: Extract source IP from headers - need to add back HeaderMap extraction
  let source_ip = "unknown".to_string();
  ```
- `crates/miroir-proxy/src/routes/search.rs:1026-1027` (inside `search_multi_targets`):
  ```rust
  // TODO: Extract source IP from headers
  let source_ip = "unknown".to_string();
  ```

These are inline in the handler bodies (not inside any `extract_source_ip` function) and are
preceded by TODOs rather than a fallback comment — exactly the "re-collapse all clients to one
bucket" pattern the criterion forbids.

### 2. "Precedence order matches admin_endpoints.rs exactly" — FAIL (nothing to match)

There is no `extract_source_ip` in `search.rs` whose precedence could be compared:
`grep -rn "fn extract_source_ip" crates/` → no matches anywhere in the repo.
`git log -S "extract_source_ip" -- crates/miroir-proxy/src/routes/search.rs` → empty (it has never
existed in search.rs).

The reference trusted-proxy extraction that search.rs *should* use is inline at
`admin_endpoints.rs:1374-1382`:
```rust
// Extract source IP from X-Forwarded-For or X-Real-IP (trust proxy)
let source_ip = headers
    .get("x-forwarded-for")
    .and_then(|v| v.to_str().ok())
    .and_then(|s| s.split(',').next())          // first hop
    .or_else(|| headers.get("x-real-ip").and_then(|v| v.to_str().ok()))
    .unwrap_or("unknown")                        // documented fallback
    .trim()
    .to_string();
```
Precedence: X-Forwarded-For first hop → X-Real-IP → "unknown". An identical copy exists in
`search_ui.rs:171-179` and `session.rs:123-127`. `search.rs` uses none of them.

### 3. "Call chain search_handler -> extract_source_ip -> rate-limit calls is intact" — FAIL

- `search_handler` does **not** call `extract_source_ip`. It hardcodes `"unknown"` (line 190) and
  feeds it straight into the rate-limit call sites:
  - `redis.check_rate_limit_search_ui(&source_ip, limit, window_seconds)` — `search.rs:205`
  - `state.local_search_ui_rate_limiter.check(&source_ip, limit, window_seconds * 1000)` — `search.rs:229`
- `search_multi_targets` takes **no** `source_ip` parameter. Its signature (`search.rs:1016-1023`)
  is `(targets, body, Extension(state), session_id, client_requested_score, min_settings_version)`.
  Its caller `search_handler` (`search.rs:494-502`) passes no source IP. `search_multi_targets`
  hardcodes `"unknown"` (line 1027) and feeds it to the same two rate-limit call sites
  (`search.rs:1042`, `search.rs:1066`).

The call chain is broken at the first link.

## Evidence — parent `bf-ml7fg` "already implemented" claims vs. reality

| Parent claim | Reality |
|---|---|
| `extract_source_ip` fn in search.rs | Does not exist anywhere (`grep -rn "fn extract_source_ip" crates/` → 0) |
| search_handler calls it | `search.rs:190` hardcodes `"unknown"` with TODO |
| search_multi_targets takes a `source_ip` param | No such param; signature unchanged; `search.rs:1027` hardcodes `"unknown"` |
| new test file `search_ui_rate_limit` | Not present; only `tests/p10_7_admin_login_rate_limit.rs` exists |
| CHANGELOG entry with trusted-proxy note | Not present (CHANGELOG grep for trusted-proxy / source_ip / rate-limit → only an unrelated schema line) |
| fix in uncommitted working tree | `git status` shows search.rs unmodified; `git log -S "extract_source_ip" -- search.rs` → empty |

Sibling bead `bf-5izsg` ("Verify the rate-limit fix is present in the working tree") is marked
**closed** — that closure appears to be a false positive, since the fix is demonstrably absent.
Downstream bead `bf-42ec1` ("Commit the verified search-UI per-IP rate-limit fix") would therefore
have nothing verified to commit; `bf-3c4ss` ("Confirm source_ip_hash log isolation and close out
the rate-limit fix") likewise cannot proceed.

## What is needed to satisfy this bead (implementation — out of scope here)

This bead is verify-only (no build, no code changes per its scope), so no implementation was
performed. To satisfy the criteria, an implementation pass must:

1. Add the trusted-proxy extraction (copy `admin_endpoints.rs:1374-1382`) into `search.rs` — either
   as an `extract_source_ip(headers: &HeaderMap) -> String` helper or inline — and call it from
   `search_handler`, replacing the `search.rs:190` stub.
2. Add a `source_ip: String` parameter to `search_multi_targets` and thread the extracted IP from
   `search_handler` (call site `search.rs:494-502`), replacing the `search.rs:1027` stub.
3. Leave the `"unknown"` literal only as the `unwrap_or("unknown")` fallback **inside** the
   extraction (the single legitimate occurrence), matching `admin_endpoints.rs`.

## Outcome

Bead left **open** — acceptance criteria unmet, fix not present in working tree. Releasing for
retry / for the implementation bead to apply the missing fix. No code changed (verify-only).
