# p13_13 root-cause diagnosis — both failure signatures, 2026-09-11

Consolidated diagnosis of the three 2026-09-11 live-run failures of
`cargo test -p miroir-proxy --test p13_13_cache_flow_integration`
(0 passed / 10 failed in ~14s each). Compiled by task `miroir-b978b544` on
bead `miroir-1ec135a1` from split children `miroir-9ce8c5d7` (search.rs diff
vs signature A), `miroir-10842a18` (live status/body capture), and
`miroir-49ae723b` (settings PUT 405 vs meilisearch v1.8.3); written into the
repo by task `miroir-2895fd73`. The fix surface is aimed at fix bead
`miroir-fcc10451`.

Context: all three runs (04:56, 05:22, 05:52; preserved verbatim in
[the companion capture file](p13-13-live-run-logs-2026-09-11.md), committed in
`e9c5702`) ended 0 passed / 10 failed in ~14s with proxy readiness ~205ms
every time. Infrastructure is NOT the cause — both signatures are
deterministic code/API mismatches.

**Note on line numbers:** the three logs show shifted panic lines for the
same failures because the harness grew between runs (`fd1703f` 04:24,
`8c1b2db` 05:47, `7401f22` 06:22 — all after earlier captures). Canonical
numbers below are from the freshest 05:52 run; current-file anchors are given
alongside.

---

## Signature A — 9/10 tests: the FIRST proxy search POST returns 404 (route never existed)

Symptom (05:52 run, quoted from the companion capture file):

```
---- acceptance_1_cache_hit_bypasses_fan_out stdout ----
proxy ready in 205.30ms
thread 'acceptance_1_cache_hit_bypasses_fan_out' panicked at crates/miroir-proxy/tests/p13_13_cache_flow_integration.rs:508:5:
assertion failed: resp1.status().is_success()
```

Same assert-panic at the first search of acceptance_1/2/3/4/5/6/7/8/9
(05:52-log lines 508/622/738/858/959/1023/1068/1135/1196; current-file
first-search asserts are 508/622/738/858/973/1121/1166/1233/1294).
acceptance_8 adds the secondary JoinError re-panic (quoted):

```
thread 'acceptance_8_concurrent_cache_access' panicked at crates/miroir-proxy/tests/p13_13_cache_flow_integration.rs:1135:13:
assertion failed: resp.status().is_success()
thread 'acceptance_8_concurrent_cache_access' panicked at crates/miroir-proxy/tests/p13_13_cache_flow_integration.rs:1146:35:
called `Result::unwrap()` on an `Err` value: JoinError::Panic(Id(296), "assertion failed: resp.status().is_success()", ...)
```

**Root cause: route contract mismatch.** The suite POSTs the
Meilisearch-native path `/indexes/{uid}/search` to the proxy; the proxy serves
search ONLY at `POST /search/:index` and has no fallback, so axum answers 404
with an empty body on every test's very first search.

Evidence chain (each step verified live or in-source):

1. **Test call site** — `p13_13_cache_flow_integration.rs:501` (assert at 508):
   ```rust
   .post(format!("{}/indexes/products/search", setup.proxy_url))
   ```
2. **Proxy registration** — `crates/miroir-proxy/src/routes/search.rs:128`:
   ```rust
   axum::Router::new().route("/:index", axum::routing::post(search_handler))
   ```
   nested at `"/search"` in `crates/miroir-proxy/src/main.rs:852`. The indexes
   router (`crates/miroir-proxy/src/routes/indexes.rs:304-330`) registers
   `/`, `/:index`, `/:index/stats`, `/:index/settings` (+`/settings/*subpath`),
   `/:index/_preflight`, `/:index/explain`, `/:index/documents` — **no
   `/:index/search`**. `git log -S ':index/search' --
   crates/miroir-proxy/src/routes/indexes.rs` is EMPTY: the route never
   existed in history.
3. **Empirical capture** (child `miroir-10842a18`: live suite run with a
   temporary probe print at the 508 assert, reverted after capture):
   ```
   proxy ready in 205.14ms
   PROBE first-search status=404 Not Found body=
   ```
   on `POST http://127.0.0.1:17770/indexes/products/search` with valid bearer
   auth and body `{"q":"laptop","limit":10}`.
4. **Independent curl** (child `miroir-9ce8c5d7`) against a standing
   miroir-proxy:
   ```
   POST /indexes/products/search  ->  HTTP/1.1 404 Not Found
                                      x-request-id: 39aeaca4
                                      content-length: 0
   ```
   while `POST /search/products` on the same process was ACCEPTED by the
   router. The empty body + auth passing first (auth_middleware runs before
   routing) = router 404 — NOT upstream passthrough (Meilisearch errors are
   JSON `{"message","code","type","link"}`), NOT a proxy-internal 5xx, NOT
   auth (that would be 401).
5. **Symmetry note:** the proxy itself SPEAKS the native path outbound —
   `crates/miroir-proxy/src/main.rs:712` formats
   `"{}/indexes/{}/search"` for scatter calls to real nodes. The suite is
   addressing the proxy the way the proxy addresses upstreams.

**Ruled out — the uncommitted `search.rs` working-tree diff.** The diff
(26 insertions / 58 deletions, cache-key reuse rework) does NOT cause A.
Child `miroir-9ce8c5d7` read all five hunks: (1) a pure
`let mut result_cache_key: Option<...> = None;` declaration; (2) an infallible
`result_cache_key = Some(cache_key.clone());` capture; (3)+(4) deletions of
two clones consumed only by the deleted store block; (5) the ~50-line store
rewrite whose every path is error-swallowed (`if let Some(cache_key)`,
`if let Ok(response_bytes)`, insert Err → `debug!`) with no `?`/unwrap/early
return, running after response content is fixed. The status decisions (503 at
`search.rs:790`, 500 at `search.rs:910`, 200 at `search.rs:1008`) sit outside
every hunk. Structural argument: a handler panic would drop the connection and
fail at `.send().await.unwrap()`; the observed
`assertion failed: resp.status().is_success()` requires a well-formed non-2xx
response — the proxy deliberately answered 4xx.

**Scope of A:** 21
`.post(format!("{proxy}/indexes/.../search"))` call sites in
`p13_13_cache_flow_integration.rs` alone (501, 529, 615, 652, 731, 759, 851,
948, 957, 966, 1017, 1026, 1035, 1113, 1159, 1175, 1226, 1287, 1318, 1378,
1394); child `miroir-9ce8c5d7` counted 23 across the test tree. Other suites
also address the proxy with the native path: `integration_test.rs:158`,
`phase2_integration_test.rs:113` — and fixtures assume the path is real:
`p7_5_structured_logging.rs:68/235` expects the log line
"GET /indexes/products/search 200", `p10_6_csrf_posture.rs:403` classifies
`"/indexes/products/search"` as a real request path.

---

## Signature B — acceptance_10 only: settings PUT 405 (plain harness bug)

Symptom (05:52 run, quoted from the companion capture file):

```
---- acceptance_10_cache_with_complex_query stdout ----
proxy ready in 205.08ms
thread 'acceptance_10_cache_with_complex_query' panicked at crates/miroir-proxy/tests/p13_13_cache_flow_integration.rs:1268:10:
called `Result::unwrap()` on an `Err` value: Failed to update settings on http://localhost:33107: 405 Method Not Allowed
```

The other two runs show the identical 405 at their per-run ephemeral container
ports: `localhost:33077` (05:22 log), `localhost:33016` (04:56 log).

**Root cause: harness bug.** `set_node_settings`
(`p13_13_cache_flow_integration.rs:401`) PUTs `{url}/indexes/{uid}/settings`
DIRECTLY to each node (the `.put` at line 405; index management bypasses the
proxy per the comment at ~398-400), bails at line 412 on non-success, and the
suite's ONLY caller — acceptance_10's `filterableAttributes` setup at line
1361 — unwraps the Err into the panic.

**Why PUT 405s:** the spawned nodes are `getmeili/meilisearch:v1.8.3`
(testcontainers-modules 0.11.6 pins TAG v1.8.3 in its meilisearch module —
`testcontainers-modules = { version = "0.11", ... }` at
`crates/miroir-proxy/Cargo.toml:80`; the test uses
`Meilisearch::default().with_master_key(...)` with no `with_tag` override;
confirmed live: `GET /version` returns
`{"pkgVersion":"1.8.3","commitSha":"7d699532676b6ad3a692130fe721c88565cf7bc3"}`).
Meilisearch replaced PUT with PATCH on settings routes in v1.8.0. On v1.8.3
the aggregate settings route registers only patch/get/delete
(`meilisearch/src/routes/indexes/settings.rs` @ tag v1.8.3,
`generate_configure!` macro, lines 651-653: `web::patch().to(SeqHandler(update_all))`
/ `web::get()` / `web::delete()` — no `web::put`).

Empirical curl (child `miroir-49ae723b`, spawned v1.8.3 node):

```
PUT   /indexes/probe/settings -> 405 Method Not Allowed   (response header: allow: PATCH, GET, DELETE)
PATCH /indexes/probe/settings -> 202 Accepted             (settingsUpdate task)
```

Reproduced identically on the standing diagnosis-topology node. Nuance: only
the AGGREGATE settings route is PATCH-only on v1.8.3; per-setting sub-routes
(e.g. `/settings/search-cutoff-ms` via `make_setting_route!`) still accept
PUT — irrelevant to acceptance_10, which fails on the aggregate route.

**Ruled out: the proxy** — acceptance_10's settings update never touches it
(direct-to-node request). This is a one-token test fix, not a proxy bug.

---

## Other findings

1. **Readiness ~205ms is GENUINE, not a foreign-squatter artifact.** Child
   `miroir-10842a18` reproduced 205.14ms with a genuine harness-spawned proxy
   after verifying 17770/9090 free via `ss -ltnp`. The "~6s spawn time"
   expectation in the `wait_for_ready` doc comment (test file line 310) is
   stale. **Port-squatting caveat kept on record:** a stray diag miroir-proxy
   WAS found squatting `127.0.0.1:17770` + `0.0.0.0:9090` during the diagnosis
   window and it passes the exact-body `/health` check, so port-squatting
   remains a real hazard for this fixed-port suite (check `ss -ltnp` before
   live runs; a squatter would also answer readiness and poison a run) — but
   it cannot explain these failures: a squatter 404s the same unregistered
   route, so signature A reproduces on ANY build.
2. **`search.rs` working-tree diff disposition: do NOT revert it** as part of
   the fix — it is unrelated to both signatures; its only semantic change is
   cache-store correctness (store under the lookup-time key/version instead of
   re-deriving post-scatter).
3. **Fixing B alone does NOT make acceptance_10 pass:** its first search POST
   (line 1378) would then hit signature A's 404 exactly like the other nine
   tests. Both fixes are required for 10/10.

---

## Fix surface (for `miroir-fcc10451`)

BOTH sides, one change each:

1. **PROXY — signature A (the real fix):** register the Meilisearch-compatible
   route `POST /indexes/:index/search`.
   - **Option a (recommended):** add `.route("/:index/search", post(...))` to
     the router at `crates/miroir-proxy/src/routes/indexes.rs:304-330`,
     delegating to the existing handler (`search_handler`,
     `crates/miroir-proxy/src/routes/search.rs`; its current router sits at
     `search.rs:122-129`). Mind the path-param position change
     (`/search/:index` becomes `/indexes/:index/search` — middle segment).
   - **Option b:** nest a search sub-router under `/indexes` in
     `crates/miroir-proxy/src/main.rs:850-853` with an adapter for the moved
     param.
   - **Why the proxy, not the tests:** plan principle 1 — invisible
     federation, clients talk to one endpoint using the standard Meilisearch
     API (the suite speaks exactly that); 21 call sites in this suite plus
     native-path callers/fixtures in `integration_test.rs`,
     `phase2_integration_test.rs`, `p7_5_structured_logging.rs`,
     `p10_6_csrf_posture.rs` would all need edits otherwise; and the proxy
     already uses the native path outbound (`main.rs:712`), so accepting it
     inbound is symmetric.
2. **TEST HARNESS — signature B (one token):**
   `crates/miroir-proxy/tests/p13_13_cache_flow_integration.rs:405` `.put(` →
   `.patch(` inside `set_node_settings` (the bail at 412 and the caller at
   1361 stay unchanged).
3. **NOT the fix:** reverting the `search.rs` working-tree diff (ruled out
   above).

---

## Verification

The diagnosis above was independently re-verified end-to-end by
`miroir-b978b544` (attempt 2, 2026-09-11) after its first close was rejected
as verification-failed: every decisive claim was re-checked against the live
tree and every log quote re-checked verbatim against the still-on-disk /tmp
runs. Everything confirmed; diagnosis and fix surface unchanged. Re-verified
anchors (all exact):

- **Call sites:** the 21 listed native-path POSTs (18x `products` via
  `setup.proxy_url` at 501/529/615/652/851/948/957/966/1017/1026/1035/1113/
  1159/1175/1287/1318/1378/1394; 2x `books` at 731/759; 1x inline format at
  1226) and the acceptance_1 first-search assert at `:508`.
- **Harness:** `set_node_settings` at `:401`, the `.put` at `:405`, the
  non-success bail at `:412`, the sole caller (acceptance_10
  `filterableAttributes`) at `:1361`.
- **Proxy:** the search router registers ONLY
  `.route("/:index", axum::routing::post(search_handler))` at
  `crates/miroir-proxy/src/routes/search.rs:128`, nested at `"/search"`
  (`main.rs:852`); the indexes.rs router (304-330) still registers NO
  `/:index/search`; `git log -S ':index/search' -- crates/miroir-proxy/src/
  routes/indexes.rs` still returns nothing (route never existed); the outbound
  native-path `format!` sits at `main.rs:710-713`.
- **Log quotes** re-checked verbatim against the 05:52 run (readiness
  205.30ms; the nine assert panics at 508/622/738/858/959/1023/1068/1135 plus
  the JoinError re-panic at 1146; acceptance_10's unwrap at 1268 reading
  "Failed to update settings on http://localhost:33107: 405 Method Not
  Allowed"). All three runs are preserved byte-for-byte in
  [`p13-13-live-run-logs-2026-09-11.md`](p13-13-live-run-logs-2026-09-11.md)
  (commit `e9c5702`), which also pins each log to the test tree it ran.
- **Fixtures/claims:** `p7_5_structured_logging.rs:68` and `:235` expect the
  log line "GET /indexes/products/search 200"; `p10_6_csrf_posture.rs:403`
  asserts `!is_admin_path("/indexes/products/search")`; the `wait_for_ready`
  doc comment expecting a "~6s spawn time" is at test file `:310`;
  `testcontainers-modules = "0.11"` is at `crates/miroir-proxy/Cargo.toml:80`.

Anchors in this file were additionally spot-checked against the working tree
at documentation time by `miroir-2895fd73`; all matched.

---

## Addendum — final current-tree capture and final attribution (2026-09-11, post-consolidation)

After this note was committed (`ea704b7`), the `miroir-b6b71143` 4-way split
ran two more capture cycles and closed its final gate. The final re-capture
(`miroir-d50d1a47`, 2026-09-11T20:42:35Z) reproduced signature A's fingerprint
exactly against the current tree — HEAD `49deb4f` plus the uncommitted
`search.rs` diff — with the native-path POST answering
`HTTP/1.1 404 Not Found` (`x-request-id: 2617abe0`, `content-length: 0`,
empty body) while the control POST `/search/products` on the same process
answered `200 OK` with the full 55-byte degraded response. A third cycle
(`miroir-2c2c095a`, 21:37:52Z) reproduced it again; the final attribution
(child 4 `miroir-1b17291c`) and its refresh (`miroir-fc5fa7fd`) confirmed the
verdict above with ZERO divergence on every axis, including the definitive
route-table/git-history NO/NO for `POST /indexes/:index/search`.

Both verbatim logs of the final capture and both attribution notes are
preserved in
[p13-13-final-signature-a-capture-2026-09-11.md](p13-13-final-signature-a-capture-2026-09-11.md)
(task `miroir-3c9a5e90`); their scratch originals live at
`~/scratch/p13_13_first_search_capture.logs/capture-20260911T204230Z.log` and
`~/scratch/p13_13_first_search_capture.logs/proxy-logs-20260911T204230Z.log`
(sha256-pinned in that file), and the attribution text on bead
`miroir-b6b71143`. Nothing in this diagnosis changed as a result.
