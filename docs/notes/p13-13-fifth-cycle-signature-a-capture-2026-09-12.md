# p13_13 fifth-cycle signature-A capture — 2026-09-12 (00:05Z manual topology + control-path wedge)

Preservation copy of the FIFTH capture cycle: the capture run
2026-09-12T00:05:19Z by child 3 `miroir-2037d2ee` against the
manually-spawned topology of child 2 `miroir-454e8c77`, plus the attribution
note that final-gate child `miroir-f79b50e1` landed on bead `miroir-b6b71143`'s
notes (2026-09-12T00:29Z, the last child of the `miroir-b6b71143` re-split).
This cycle postdates the fourth-cycle docs commit (`09b7866`), so until this
file existed it survived only in the parent bead's notes and the gitignored
scratch log dir. Copied into the repo by task `miroir-7522b867`.

The cycle's two new datapoints (everything else re-confirms cycles 1-4):

1. **Native path — 8th consecutive identical fingerprint, first on a
   manually-spawned topology.** `POST /indexes/products/search` answered the
   usual router-miss `HTTP/1.1 404 Not Found` (`x-request-id: 6577bfec`,
   `content-length: 0`, empty body, no `allow`, no `content-type`) in <1 s.
2. **Control path — DIVERGED, independently confirming the fourth-cycle
   wedge.** `POST /search/products` was accepted and routed (open span
   `request_id abf9606e`, `path_template /search/:index`, still open on
   background-worker log lines at 00:09:47, 4.5 min after arrival) but NEVER
   answered: the attempt-1 curl was killed at teardown 00:20:36Z after 15:16
   elapsed with 0 bytes received, and a bounded retry on a fresh connection
   timed out (curl `(28)` after 150.002 s, 0 bytes). The process stayed live
   throughout — `GET /health` 200 in 14 ms at 00:13:02 (request_id
   `81f0188f`) and `POST /nope` 404 in 5 ms at 00:18:30 (request_id
   `2c9fc976`) on the same process DURING the wedge. This reproduces the
   fourth-cycle committed-harness divergence (`miroir-5ed697fa`, commit
   `09b7866`) on a different topology class, bringing the wedge to 8/8 across
   all attempts since 2026-09-11T22:27Z and weakening the anti-entropy-timing
   hypothesis (cycle 4 wedged a control ~15 ms into process life; cycles 1-3
   got fast 200s).

The signature-A verdict is NOT overturned — axum router 404 on
`POST /indexes/:index/search`, a route the proxy never served, now
five-cycle-stable. The umbrella decision (repoint the suite at
`/search/products` vs register the native route) stays with
`miroir-1ec135a1`.

Companions:
[p13-13-final-signature-a-capture-2026-09-11.md](p13-13-final-signature-a-capture-2026-09-11.md)
(cycles 1-2: 20:17Z / 20:42Z),
[p13-13-third-cycle-signature-a-capture-2026-09-11.md](p13-13-third-cycle-signature-a-capture-2026-09-11.md)
(21:37Z),
[p13-13-fourth-cycle-committed-harness-capture-2026-09-11.md](p13-13-fourth-cycle-committed-harness-capture-2026-09-11.md)
(22:27Z / 22:28:50Z / 23:03Z / 23:26:22Z — the first control-path divergence)
and
[p13-13-root-cause-diagnosis.md](p13-13-root-cause-diagnosis.md) (the
consolidated verdict).

What this file preserves, and who produced it:

| Artifact | Producer | When (UTC) | Original location |
|---|---|---|---|
| `capture-20260912T000519Z.log` | `miroir-2037d2ee` (child 3, fifth capture cycle of the `miroir-b6b71143` re-split) | 2026-09-12 00:05:19 | `~/scratch/p13_13_first_search_capture.logs/capture-20260912T000519Z.log` |
| `control-attempt2-20260912T000519Z.log` | same run (bounded control retry; epilogue annotated by `miroir-2037d2ee` at 00:21Z) | 2026-09-12 00:14:44–00:17:14 | `~/scratch/p13_13_first_search_capture.logs/control-attempt2-20260912T000519Z.log` |
| `proxy-logs-20260912T000519Z.log` | same run | 2026-09-12 00:05:19 | `~/scratch/p13_13_first_search_capture.logs/proxy-logs-20260912T000519Z.log` |

Capture topology (one run): pre-flight `ss` clean on 17770/9090 with listener
ownership verified; `target/debug/miroir-proxy` pid 2439395 on client
127.0.0.1:17770 / metrics 0.0.0.0:9090; one `getmeili/meilisearch:v1.8.3`
node (container `p13_13-topo-meili-20260911T235933Z`, cid
`e72cab803fe3…`) on host port 33180 (key via `MEILI_MASTER_KEY` env only);
tree = HEAD `09b7866` + the uncommitted `search.rs` diff (pre-dirty before
dispatch); **binary = the identical `target/debug/miroir-proxy` binary every
cycle since 20:42Z** (built 2026-09-11 16:42:12 local, no rebuild); request
auth `Authorization: Bearer test_master_key`, body `{"q":"laptop","limit":10}`.
Teardown verified by child 3: ports released, container removed, no proxy
process left.

Verbatim-ness: each log body below is byte-identical to its scratch original —
sha256 in the table below, between unique `BEGIN-LOG`/`END-LOG` markers so it
can be extracted and re-hashed. Note for verifiers: as with the fourth-cycle
capture logs, the capture log carries the curl wire output verbatim, i.e. the
HTTP response headers are CRLF-terminated (5 CR bytes) — extract and hash in
**binary** mode; a text-mode read (or any editor that normalizes CRLF)
silently strips them and the hash will not match.

## Capture fingerprints

| File | Bytes / lines | sha256 |
|---|---|---|
| `capture-20260912T000519Z.log` | 1766 / 29 | `6c83b6290aa2e3db954c4c518ea2b49e592a8a1b31ee01eb905958d8d6013c17` |
| `control-attempt2-20260912T000519Z.log` | 266 / 10 | `0fb725ef2f0c531ddd857ec195c5551d24094b1dea762331b7d3ad94e942ab9d` |
| `proxy-logs-20260912T000519Z.log` | 5056 / 22 | `5f1d2f24021ea8d5bfd35b05f2f9f5d93a8d1c0ddb68c6ef62e5e2874fe1684f` |

Fingerprint, verbatim (native half identical in shape to all seven prior
observations; the control half is this cycle's one divergence — no status
line, no headers, no body):

```
HTTP/1.1 404 Not Found
x-request-id: 6577bfec
content-length: 0
date: Sat, 12 Sep 2026 00:05:19 GMT
```

Proxy-log cross-check (same process): `request_id 6577bfec` WARN "POST 404
Not Found" `status=404 path_template=/indexes/products/search duration_ms=0`
— matches the response x-request-id. The control's span (`request_id
abf9606e`, `path_template /search/:index`) appears in background-worker log
lines at 00:09:47 still open, and no `miroir.request` completion line is ever
logged for it — the handler parked, not slow.

## Verbatim capture log — capture-20260912T000519Z.log

<!-- BEGIN-LOG: capture-20260912T000519Z.log -->
```text
=== p13_13 first-search capture 2026-09-12T00:05:19Z ===
proxy: pid 2439395  /home/coding/miroir/target/debug/miroir-proxy
ports: client 17770, metrics 9090
node:  getmeili/meilisearch:v1.8.3 cid=e72cab803fe32d88ca3e9c4351964863b7e7de8f23abcb5250ec81423bcb9d24 name=p13_13-topo-meili-20260911T235933Z host port 33180 (key via MEILI_MASTER_KEY env only)
auth:  Authorization: Bearer test_master_key   body: {"q":"laptop","limit":10}
git:   HEAD 09b78666c548665afb287649408383ed7b337903 search.rs:  M crates/miroir-proxy/src/routes/search.rs
bin:   built 2026-09-11 16:42:12.400900244 -0400
--- POST /indexes/products/search (path the p13_13 suite POSTs) ---
HTTP/1.1 404 Not Found
x-request-id: 6577bfec
content-length: 0
date: Sat, 12 Sep 2026 00:05:19 GMT



--- POST /search/products (control: registered route, same process) ---

--- POST /search/products attempt 1 EPILOGUE (annotated 2026-09-12T00:21Z) ---
The attempt-1 control curl above never received a response byte: the connection
ESTABLISHED at ~00:05:20, both socket queues stayed empty, and the proxy's own
span for the request (request_id abf9606e, path_template /search/:index) appears
in background-worker log lines at 00:09:47 still open. The curl was killed at
teardown 2026-09-12T00:20:36Z after 15:16 elapsed with 0 bytes received. Its
section above is therefore empty because there was literally nothing to write.
Bounded retry on a fresh connection: see control-attempt2-20260912T000519Z.log
(curl error 28, 150000 ms, 0 bytes received). /health on the same process
answered 200 in 14 ms at 00:13:02 (request_id 81f0188f) and POST /nope 404 in
5 ms (request_id 2c9fc976), so the process was live throughout.
=== end of capture (proxy pid 2439395 TERM'd 00:20:36Z; topology torn down) ===
```
<!-- END-LOG: capture-20260912T000519Z.log -->

The attempt-1 control section inside this log is empty because there was
literally nothing to write — no response byte ever arrived; the epilogue
annotating that (00:21Z) is part of the preserved file, as is the bounded
retry's pointer to the attempt-2 log below.

## Verbatim control retry log — control-attempt2-20260912T000519Z.log

<!-- BEGIN-LOG: control-attempt2-20260912T000519Z.log -->
```text
--- control attempt 2 (fresh connection, same process/key/body, bounded -m 150) ---
start: 2026-09-12T00:14:44Z
curl: (28) Operation timed out after 150002 milliseconds with 0 bytes received

real	2m30.010s
user	0m0.006s
sys	0m0.006s

rc=0
end: 2026-09-12T00:17:14Z
```
<!-- END-LOG: control-attempt2-20260912T000519Z.log -->

Reading the retry log: curl's own verdict is the `(28) Operation timed out
after 150002 milliseconds with 0 bytes received` line; the trailing `rc=0`
line is the wrapper's echo after the `time` block, not curl's exit status.
Start 00:14:44Z / end 00:17:14Z matches the note's "150.002 s" and the
15 s-client-cap failure shape first seen in the fourth cycle, now with a
150 s bound.

## Verbatim proxy log — proxy-logs-20260912T000519Z.log

<!-- BEGIN-LOG: proxy-logs-20260912T000519Z.log -->
```text
=== proxy stdout log, capture window 2026-09-12T00:19:08Z ===
source: /tmp/p13_13-topo.M5oBEi/proxy.stdout.log (proxy pid 2439395)
--- miroir.request lines (all HTTP requests the proxy completed) ---
{"timestamp":"2026-09-12T00:02:13.224630Z","level":"INFO","pod_id":"unknown","request_id":"60e4f6b1","message":"GET 200 OK","duration_ms":0,"status":200,"method":"GET","path_template":"/health","target":"miroir.request","span":{"method":"GET","path_template":"/health","pod_id":"unknown","request_id":"60e4f6b1","name":"request"}}
{"timestamp":"2026-09-12T00:02:13.231910Z","level":"INFO","pod_id":"unknown","request_id":"d6d04507","message":"GET 200 OK","duration_ms":0,"status":200,"method":"GET","path_template":"/health","target":"miroir.request","span":{"method":"GET","path_template":"/health","pod_id":"unknown","request_id":"d6d04507","name":"request"}}
{"timestamp":"2026-09-12T00:03:37.824271Z","level":"INFO","pod_id":"unknown","request_id":"f97ac16b","message":"GET 200 OK","duration_ms":0,"status":200,"method":"GET","path_template":"/health","target":"miroir.request","span":{"method":"GET","path_template":"/health","pod_id":"unknown","request_id":"f97ac16b","name":"request"}}
{"timestamp":"2026-09-12T00:05:19.999942Z","level":"WARN","pod_id":"unknown","request_id":"6577bfec","message":"POST 404 Not Found","duration_ms":0,"status":404,"method":"POST","path_template":"/indexes/products/search","target":"miroir.request","span":{"method":"POST","path_template":"/indexes/products/search","pod_id":"unknown","request_id":"6577bfec","name":"request"}}
{"timestamp":"2026-09-12T00:13:02.996957Z","level":"INFO","pod_id":"unknown","request_id":"81f0188f","message":"GET 200 OK","duration_ms":0,"status":200,"method":"GET","path_template":"/health","target":"miroir.request","span":{"method":"GET","path_template":"/health","pod_id":"unknown","request_id":"81f0188f","name":"request"}}
{"timestamp":"2026-09-12T00:18:30.048403Z","level":"WARN","pod_id":"unknown","request_id":"2c9fc976","message":"POST 404 Not Found","duration_ms":0,"status":404,"method":"POST","path_template":"/nope","target":"miroir.request","span":{"method":"POST","path_template":"/nope","pod_id":"unknown","request_id":"2c9fc976","name":"request"}}

--- full log from first capture request onward (2026-09-12T00:05:00Z+) ---
{"timestamp":"2026-09-12T00:05:19.999942Z","level":"WARN","pod_id":"unknown","request_id":"6577bfec","message":"POST 404 Not Found","duration_ms":0,"status":404,"method":"POST","path_template":"/indexes/products/search","target":"miroir.request","span":{"method":"POST","path_template":"/indexes/products/search","pod_id":"unknown","request_id":"6577bfec","name":"request"}}
{"timestamp":"2026-09-12T00:09:47.192214Z","level":"WARN","message":"peer discovery failed: discovery error: SRV lookup failed for _http._tcp.miroir-headless.default.svc.cluster.local: no record found for name: _http._tcp.miroir-headless.default.svc.cluster.local type: SRV class: IN","target":"miroir_core::mode_a_coordinator","span":{"method":"POST","path_template":"/search/:index","pod_id":"unknown","request_id":"abf9606e","name":"request"}}
{"timestamp":"2026-09-12T00:09:47.192458Z","level":"WARN","message":"failed to refresh peer set, using cached peers","error":"no peers discovered","target":"miroir_core::rebalancer_worker::drift_reconciler","span":{"method":"POST","path_template":"/search/:index","pod_id":"unknown","request_id":"abf9606e","name":"request"}}
{"timestamp":"2026-09-12T00:09:47.193618Z","level":"INFO","message":"pruner: deleted 0 tasks (Mode A), registry_size=0","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-12T00:13:02.996957Z","level":"INFO","pod_id":"unknown","request_id":"81f0188f","message":"GET 200 OK","duration_ms":0,"status":200,"method":"GET","path_template":"/health","target":"miroir.request","span":{"method":"GET","path_template":"/health","pod_id":"unknown","request_id":"81f0188f","name":"request"}}
{"timestamp":"2026-09-12T00:14:47.209367Z","level":"WARN","message":"peer discovery failed: discovery error: SRV lookup failed for _http._tcp.miroir-headless.default.svc.cluster.local: no record found for name: _http._tcp.miroir-headless.default.svc.cluster.local type: SRV class: IN","target":"miroir_core::mode_a_coordinator"}
{"timestamp":"2026-09-12T00:14:47.209523Z","level":"WARN","message":"failed to refresh peer set, using cached peers","error":"no peers discovered","target":"miroir_core::rebalancer_worker::drift_reconciler"}
{"timestamp":"2026-09-12T00:14:47.217559Z","level":"INFO","message":"pruner: deleted 0 tasks (Mode A), registry_size=0","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-12T00:18:30.048403Z","level":"WARN","pod_id":"unknown","request_id":"2c9fc976","message":"POST 404 Not Found","duration_ms":0,"status":404,"method":"POST","path_template":"/nope","target":"miroir.request","span":{"method":"POST","path_template":"/nope","pod_id":"unknown","request_id":"2c9fc976","name":"request"}}
--- stderr (empty throughout) ---
0 /tmp/p13_13-topo.M5oBEi/proxy.stderr.log
```
<!-- END-LOG: proxy-logs-20260912T000519Z.log -->

## The fifth-cycle attribution note — verbatim from bead miroir-b6b71143

The text below is the fifth-cycle block of bead `miroir-b6b71143`'s notes,
landed by final-gate child `miroir-f79b50e1` (2026-09-12T00:29Z) and copied
byte-for-byte: the fresh 404 capture, the decisive-header reasoning, child 1's
route/history verdict, the control-wedge divergence section, the root-cause
attribution, and the explicit statement that the umbrella decision stays with
`miroir-1ec135a1`.

```text
FINAL SIGNATURE A ATTRIBUTION — FIFTH CYCLE, FRESH EVIDENCE (final-gate child miroir-f79b50e1, 2026-09-12T00:29Z; children of this re-split, all closed: child 1 miroir-1828fe6e route/history audit, child 2 miroir-454e8c77 topology spawn, child 3 miroir-2037d2ee capture). VERDICT UNCHANGED: signature A — 9/10 p13_13 tests panicking at `assertion failed: resp.status().is_success()` (first occurrence crates/miroir-proxy/tests/p13_13_cache_flow_integration.rs:508) — is an axum ROUTER MISS (taxonomy: router 404, empty body, auth passed first) on POST /indexes/:index/search. This cycle's one genuinely new datapoint: the CONTROL path diverged on a manually-spawned topology, independently confirming the fourth-cycle committed-harness divergence already recorded (miroir-5ed697fa, commit 09b7866).

CLASSIFICATION: router 404 — exactly one of the four-way taxonomy; each alternative ruled out below.

FRESH CAPTURE, VERBATIM (child 3 miroir-2037d2ee, run 2026-09-12T00:05:19Z; logs ~/scratch/p13_13_first_search_capture.logs/{capture-20260912T000519Z.log, control-attempt2-20260912T000519Z.log, proxy-logs-20260912T000519Z.log}; topology from child 2 miroir-454e8c77: target/debug/miroir-proxy pid 2439395 on client 127.0.0.1:17770 / metrics 0.0.0.0:9090 — pre-flight ss clean on both ports, listener ownership verified — plus one getmeili/meilisearch:v1.8.3 node, container p13_13-topo-meili-20260911T235933Z, host port 33180, keyed via MEILI_MASTER_KEY env only; tree = HEAD 09b7866 with search.rs pre-dirty (M, present before dispatch), binary built 2026-09-11 16:42 — the identical binary every cycle since 20:42Z; Authorization: Bearer test_master_key, body {"q":"laptop","limit":10}):

Native path POST /indexes/products/search:
  HTTP/1.1 404 Not Found
  x-request-id: 6577bfec
  content-length: 0
  date: Sat, 12 Sep 2026 00:05:19 GMT
  (body: empty — 0 bytes; answered in <1s)

Proxy-log cross-check, same process (proxy-logs-20260912T000519Z.log): request_id 6577bfec WARN "POST 404 Not Found" status=404 path_template=/indexes/products/search duration_ms=0 — matches the response x-request-id. Teardown confirmed by child 3 (ports released, container removed, no proxy process left).

DECISIVE-HEADER REASONING on the fresh capture:
- Router 404, NOT auth 401: x-request-id PRESENT on the 404, and request_id_middleware is INNER to auth_middleware (main.rs:860-865 layer-order comment, outermost→innermost csrf → auth → extensions → session_pinning → request_id → telemetry), so the id is stamped only after auth passes. Scope correction vs cycles 1-3: they ALSO cited the control-200 as independent auth proof; this cycle the control never answered (divergence below), so the header test carries the exclusion alone. Corroborating liveness on the same process DURING the control wedge: GET /health → 200 in 14 ms (00:13:02, request_id 81f0188f); POST /nope → 404 in 5 ms (00:18:30, request_id 2c9fc976) — router and listener alive throughout.
- NOT 405: no `allow` header.
- NOT upstream passthrough: content-length: 0, NO content-type, NO {"message","code","type","link"} JSON envelope — a 0-byte body cannot be a Meilisearch error.
- NOT proxy-internal 5xx: the status is 404, and the proxy's own request line shows duration_ms=0 (response emitted at the router; no search handler work).
- The empty-body 404 is axum's default unmatched-route response; the main router registers no fallback (the only .fallback() is GET-scoped inside /ui/search, search_ui.rs:150).

CHILD 1 ROUTE VERDICT (miroir-1828fe6e; citations re-verified against the current tree by this gate at HEAD 09b7866): POST /indexes/:index/search servable at the current tree: NO. Ever a live endpoint at any commit in git history: NO. The /indexes nest (indexes.rs:304-330) registers only "/", "/:index" (GET/PATCH/DELETE), "/:index/stats", "/:index/settings[/...]", "/:index/_preflight", "/:index/explain", and the /:index/documents nest — no "/search" leaf, no catch-all. The only proxy search routes remain POST /search/:index (search.rs:128, nested at "/search" main.rs:852) and POST /multi-search (main.rs:846). Pickaxe ':index/search' over indexes.rs+main.rs: ZERO commits (default and --full-history); all three documented traps re-confirmed (a046c3a added "/:index/search" inside search.rs under the /search nest — served /search/{index}/search, never /indexes/...; scaffold 9b5cf0d "/:index/:sub" catch-all route-matched .../search but returned 501 unconditionally, removed by b2490ea; the removal lives in merge 1f686c6's tree resolution, invisible to default git log -S). The uncommitted search.rs diff adds/removes ZERO route-registering lines (re-verified by grep over the diff at this gate).

DIVERGENCE VS PRIOR CYCLES — STATED PER PATH:
- NATIVE PATH: NO divergence. Fingerprint IDENTICAL for the 8th consecutive native-path observation (scratch cycles 20:17Z / 20:42Z / 21:37Z; committed-harness runs 22:27Z / 22:28:50Z / 23:03Z / 23:26:22Z; this manual topology 00:05:19Z): 404, x-request-id present, content-length: 0, empty body, no allow, no content-type — only x-request-id and date differ.
- CONTROL PATH: DIVERGED from cycles 1-3. Cycles 1-3's same-process control answered in the same second as the native 404 (200 + content-type: application/json + x-miroir-degraded: shards=0..15 + 55-byte body). This cycle POST /search/products NEVER COMPLETED: connection accepted, zero response bytes; attempt 1 was still pending when killed at teardown after 15:16 elapsed, and a bounded retry on a fresh connection timed out (curl rc=28, 150.002 s, 0 bytes). The proxy DID accept and route it — open span request_id abf9606e, path_template /search/:index, visible still-open on background-worker log lines at 00:09:47 (4.5 min after arrival) — but logged no miroir.request completion and produced no search result, i.e. the handler parked, not slow. This independently reproduces the fourth-cycle committed-harness divergence (7/7 there, including a control fired ~15 ms into process life), bringing the wedge to 8/8 across all attempts since 22:27Z. It also WEAKENS child 3's own timing hypothesis (anti-entropy pass mid-flight at ~5.7 min topology age): cycle 4 wedged a control at ~15 ms of process life, while cycles 1-3 got fast 200s — spawn-age alone does not explain it. The wedge is a SECOND, DISTINCT failure mode from signature A and does not touch the router-miss diagnosis: it lives in the registered /search/:index handler, whereas the native path never reaches any handler.

ROOT CAUSE (attribution, unchanged and now five-cycle-stable): API-contract mismatch between the p13_13 suite and the proxy route table. The suite addresses the proxy with the Meilisearch-native path POST /indexes/products/search (all 10 acceptance tests, 23 POST sites, first at p13_13_cache_flow_integration.rs:501, failing assert at :508); the proxy serves search only at POST /search/:index and POST /multi-search and registers no fallback, so axum answers the suite's first search POST with its default empty-body 404 before any handler runs. Not auth (x-request-id present post-auth), not upstream (no error envelope), not a proxy-internal fault (404, not 5xx), not environment (reproduced identically across 8 observations on 8 distinct proxy processes with fresh node containers each time, under both the scratch and committed harness forms).

UMBRELLA DECISION: NOT MADE HERE — repoint the suite at /search/products vs register POST /indexes/:index/search remains with miroir-1ec135a1, per this gate's charter. One decision INPUT is hereby updated: cycles 1-3's notes justified remediation option (a) ("repoint the suite at /search/products — the contract that exists and demonstrably serves") on the strength of the fast degraded-200 control; since 22:27Z that endpoint does not complete on any attempted topology (8/8 wedge), so option (a) can no longer rest on the control-200 evidence and the control-path wedge needs its own diagnosis regardless of which option miroir-1ec135a1 picks.
```
