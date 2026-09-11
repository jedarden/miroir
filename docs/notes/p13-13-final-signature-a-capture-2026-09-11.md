# p13_13 final signature-A capture and final attribution — 2026-09-11

Preservation copy of the FINAL current-tree re-capture of the proxy's
first-search response (the 20:42Z cycle) and of the final signature-A
attribution, so the p13_13 diagnosis survives scratch cleanup and bead
checkpoint rotation. Companions:
[p13-13-root-cause-diagnosis.md](p13-13-root-cause-diagnosis.md) (the
consolidated verdict, commit `ea704b7`) and
[p13-13-live-run-logs-2026-09-11.md](p13-13-live-run-logs-2026-09-11.md) (the
three original 0/10 suite-run logs, commit `e9c5702`). Copied into the repo by
task `miroir-3c9a5e90`; nothing in the diagnosis changed.

What this file preserves, and who produced it:

| Artifact | Producer | When (UTC) | Original location |
|---|---|---|---|
| `capture-20260911T204230Z.log` | `miroir-d50d1a47` (child 2, current-tree re-capture, cycle 2 of the `miroir-b6b71143` 4-way split) | 2026-09-11 20:42:35 | `~/scratch/p13_13_first_search_capture.logs/capture-20260911T204230Z.log` |
| `proxy-logs-20260911T204230Z.log` | same run | 2026-09-11 20:42:35 | `~/scratch/p13_13_first_search_capture.logs/proxy-logs-20260911T204230Z.log` |
| `FINAL SIGNATURE A ATTRIBUTION` note | `miroir-1b17291c` (child 4, final gate; recorded 21:08) | 2026-09-11 | Notes field of bead `miroir-b6b71143` |
| `CONSOLIDATED SIGNATURE A ROOT CAUSE — REFRESH` note | `miroir-fc5fa7fd` (third capture cycle; recorded 21:50) | 2026-09-11 | Notes field of bead `miroir-b6b71143` |

Capture topology (both logs, one run): `target/debug/miroir-proxy` pid 2263583
on client 127.0.0.1:17770 / metrics 0.0.0.0:9090, one
`getmeili/meilisearch:v1.8.3` node on host port 33178 (key via
`MEILI_MASTER_KEY` env only); tree = HEAD `49deb4f` + the uncommitted
`search.rs` diff (binary built 20:42:12 local for this run); request auth
`Authorization: Bearer test_master_key`, body `{"q":"laptop","limit":10}`.

Verbatim-ness: each log body below is byte-identical to its scratch original —
sha256 in the table below, between unique `BEGIN-LOG`/`END-LOG` markers so it
can be extracted and re-hashed. The attribution section is the contiguous tail
of bead `miroir-b6b71143`'s Notes field (revision 16) starting at the
`FINAL SIGNATURE A ATTRIBUTION` note, between `BEGIN-NOTE`/`END-NOTE`
markers, unedited.

## Capture fingerprints

| File | Bytes / lines | sha256 |
|---|---|---|
| `capture-20260911T204230Z.log` | 1263 / 27 | `525a3a31b9e4bc341a396d6a82c1205a3648a9aa79492df40890a844c3bed019` |
| `proxy-logs-20260911T204230Z.log` | 8636 / 46 | `b70305fba76b3ae0337587f5421875cc23df29656b7d622f780d2a57d6bb29a9` |

Reading the capture in one line: the suite's native-path POST
`/indexes/products/search` answers `HTTP/1.1 404 Not Found` with
`x-request-id: 2617abe0` and a 0-byte body, while the control POST
`/search/products` on the same process answers `200 OK` with the full 55-byte
degraded response — a router miss on a route that does not exist, with auth,
upstream, and proxy internals all demonstrably healthy.

## Verbatim capture log — capture-20260911T204230Z.log

<!-- BEGIN-LOG: capture-20260911T204230Z.log -->
```text
=== p13_13 first-search capture 2026-09-11T20:42:35Z ===
proxy: pid 2263583  /home/coding/miroir/target/debug/miroir-proxy
ports: client 17770, metrics 9090
node:  getmeili/meilisearch:v1.8.3 cid=378fcee9daf7a76ba9799e3a377e1aee18dbb29bd9f5072ec88f63da20af9171 host port 33178 (key via MEILI_MASTER_KEY env only)
auth:  Authorization: Bearer test_master_key   body: {"q":"laptop","limit":10}
git:   HEAD 49deb4ff8253c0afff92b3e1644cc415cb7054d9 search.rs:  M crates/miroir-proxy/src/routes/search.rs
bin:   built 2026-09-11 16:42:12.400900244 -0400
--- POST /indexes/products/search (path the p13_13 suite POSTs) ---
HTTP/1.1 404 Not Found
x-request-id: 2617abe0
content-length: 0
date: Fri, 11 Sep 2026 20:42:35 GMT


--- POST /search/products (control: registered route, same process) ---
HTTP/1.1 200 OK
content-type: application/json
x-miroir-degraded: shards=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
x-request-id: e7cbc303
content-length: 55
date: Fri, 11 Sep 2026 20:42:35 GMT

{"estimatedTotalHits":0,"hits":[],"processingTimeMs":0}
--- capture summary ---
curl rc /indexes/products/search: 0 (0 = HTTP response received, any status)
curl rc /search/products:         0 (0 = HTTP response received, any status)
end of capture 2026-09-11T20:42:35Z
```
<!-- END-LOG: capture-20260911T204230Z.log -->

## Verbatim proxy log — proxy-logs-20260911T204230Z.log

<!-- BEGIN-LOG: proxy-logs-20260911T204230Z.log -->
```text
{"timestamp":"2026-09-11T20:42:34.724303Z","level":"INFO","message":"miroir-proxy starting","shards":16,"replication_factor":1,"replica_groups":1,"target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T20:42:34.725064Z","level":"WARN","message":"generated random ADMIN_SESSION_SEAL_KEY; multi-pod deployments must set this manually to a shared value","target":"miroir_proxy::admin_session","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T20:42:34.770304Z","level":"INFO","message":"CDC: background publisher started","target":"miroir_core::cdc"}
{"timestamp":"2026-09-11T20:42:34.770523Z","level":"INFO","message":"rebalancer worker task starting","pod_id":"unknown","target":"miroir_proxy"}
{"timestamp":"2026-09-11T20:42:34.770545Z","level":"INFO","message":"loading aliases from task store","target":"miroir_proxy"}
{"timestamp":"2026-09-11T20:42:34.770612Z","level":"INFO","message":"anti-entropy worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T20:42:34.770594Z","level":"INFO","message":"drift reconciler started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T20:42:34.770544Z","level":"INFO","message":"peer discovery disabled (not running in Kubernetes)","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T20:42:34.770638Z","level":"INFO","message":"ILM worker starting","target":"miroir_proxy"}
{"timestamp":"2026-09-11T20:42:34.770666Z","level":"INFO","message":"Mode C worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T20:42:34.770692Z","level":"INFO","message":"anti-entropy worker starting (Mode A coordination)","pod_id":"unknown","interval_s":21600,"target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T20:42:34.770697Z","level":"INFO","message":"drift reconciler starting (Mode A coordination)","pod_id":"unknown","target":"miroir_core::rebalancer_worker::drift_reconciler"}
{"timestamp":"2026-09-11T20:42:34.770732Z","level":"INFO","message":"ILM worker: starting evaluation loop","target":"miroir_core::ilm"}
{"timestamp":"2026-09-11T20:42:34.770729Z","level":"INFO","message":"resource-pressure metrics collection started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T20:42:34.770696Z","level":"INFO","message":"group sync worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T20:42:34.770740Z","level":"INFO","message":"Starting Mode C worker loop","target":"miroir_core::mode_c_worker"}
{"timestamp":"2026-09-11T20:42:34.770735Z","level":"INFO","message":"canary runner enabled but Redis not available - skipping","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T20:42:34.770780Z","level":"INFO","message":"Starting group sync worker","target":"miroir_core::group_sync_worker"}
{"timestamp":"2026-09-11T20:42:34.770829Z","level":"INFO","message":"loaded 0 aliases from task store","target":"miroir_core::alias"}
{"timestamp":"2026-09-11T20:42:34.770878Z","level":"INFO","message":"task registry TTL pruner started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T20:42:34.770902Z","level":"INFO","message":"aliases loaded successfully","count":0,"target":"miroir_proxy"}
{"timestamp":"2026-09-11T20:42:34.771110Z","level":"INFO","message":"pruner: starting with interval=300s ttl=604800s","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-11T20:42:34.771936Z","level":"INFO","message":"acquired Mode B leader lease","operation_type":"ilm","scope":"ilm","pod_id":"unknown","target":"miroir_core::mode_b_coordinator"}
{"timestamp":"2026-09-11T20:42:34.772096Z","level":"INFO","message":"rebalancer worker starting","pod_id":"unknown","target":"miroir_core::rebalancer_worker"}
{"timestamp":"2026-09-11T20:42:34.772462Z","level":"INFO","message":"pruner: deleted 0 tasks (Mode A), registry_size=0","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-11T20:42:34.774173Z","level":"INFO","message":"acquired leader lease","scope":"rebalance:default","pod_id":"unknown","target":"miroir_core::rebalancer_worker"}
{"timestamp":"2026-09-11T20:42:34.774732Z","level":"INFO","message":"node promoted to Active (was Joining)","node_id":"node-0","target":"miroir_proxy"}
{"timestamp":"2026-09-11T20:42:34.774904Z","level":"INFO","message":"ILM worker: acquired leadership, starting evaluation","target":"miroir_core::ilm"}
{"timestamp":"2026-09-11T20:42:34.775212Z","level":"INFO","message":"Service marked as ready","target":"miroir_proxy::routes::admin_endpoints"}
{"timestamp":"2026-09-11T20:42:34.780326Z","level":"INFO","message":"listening","main_addr":"127.0.0.1:17770","metrics_addr":"0.0.0.0:9090","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T20:42:34.975293Z","level":"INFO","message":"Service marked as ready","target":"miroir_proxy::routes::admin_endpoints"}
{"timestamp":"2026-09-11T20:42:35.175437Z","level":"INFO","message":"Service marked as ready","target":"miroir_proxy::routes::admin_endpoints"}
{"timestamp":"2026-09-11T20:42:35.213657Z","level":"INFO","pod_id":"unknown","request_id":"deb1aa8d","message":"GET 200 OK","duration_ms":0,"status":200,"method":"GET","path_template":"/health","target":"miroir.request","span":{"method":"GET","path_template":"/health","pod_id":"unknown","request_id":"deb1aa8d","name":"request"}}
{"timestamp":"2026-09-11T20:42:35.269979Z","level":"WARN","message":"peer discovery failed: discovery error: SRV lookup failed for _http._tcp.miroir-headless.default.svc.cluster.local: no record found for name: _http._tcp.miroir-headless.default.svc.cluster.local type: SRV class: IN","target":"miroir_core::mode_a_coordinator"}
{"timestamp":"2026-09-11T20:42:35.270030Z","level":"WARN","message":"failed to refresh peer set, using cached peers","error":"no peers discovered","target":"miroir_core::rebalancer_worker::drift_reconciler"}
{"timestamp":"2026-09-11T20:42:35.273709Z","level":"WARN","message":"peer discovery failed: discovery error: SRV lookup failed for _http._tcp.miroir-headless.default.svc.cluster.local: no record found for name: _http._tcp.miroir-headless.default.svc.cluster.local type: SRV class: IN","target":"miroir_core::mode_a_coordinator"}
{"timestamp":"2026-09-11T20:42:35.273758Z","level":"WARN","message":"failed to refresh peer set, using cached peers","error":"no peers discovered","target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T20:42:35.273770Z","level":"INFO","message":"starting anti-entropy pass","target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T20:42:35.273793Z","level":"INFO","message":"Anti-entropy pass starting: 16 shards to scan","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T20:42:35.274654Z","level":"WARN","pod_id":"unknown","request_id":"2617abe0","message":"POST 404 Not Found","duration_ms":0,"status":404,"method":"POST","path_template":"/indexes/products/search","target":"miroir.request","span":{"method":"POST","path_template":"/indexes/products/search","pod_id":"unknown","request_id":"2617abe0","name":"request"}}
{"timestamp":"2026-09-11T20:42:35.274709Z","level":"WARN","message":"Failed to fingerprint shard 0 on node node-0: topology error: fetch failed: HttpError { status: 404, body: \"{\\\"message\\\":\\\"Index `default` not found.\\\",\\\"code\\\":\\\"index_not_found\\\",\\\"type\\\":\\\"invalid_request\\\",\\\"link\\\":\\\"https://docs.meilisearch.com/errors#index_not_found\\\"}\" }","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T20:42:35.282217Z","level":"INFO","message":"search completed","index":"products","duration_ms":2,"node_count":16,"estimated_hits":0,"degraded":true,"target":"miroir.search","span":{"index":"\"products\"","jwt_claims":"None","session_id":"None","name":"search_handler"}}
{"timestamp":"2026-09-11T20:42:35.282360Z","level":"INFO","pod_id":"unknown","request_id":"e7cbc303","message":"POST 200 OK","duration_ms":2,"status":200,"method":"POST","path_template":"/search/:index","target":"miroir.request","span":{"method":"POST","path_template":"/search/:index","pod_id":"unknown","request_id":"e7cbc303","name":"request"}}
{"timestamp":"2026-09-11T20:42:35.287439Z","level":"INFO","message":"shutdown signal received, draining in-flight requests...","target":"miroir_proxy"}
{"timestamp":"2026-09-11T20:42:35.287495Z","level":"INFO","message":"shutdown signal received, draining in-flight requests...","target":"miroir_proxy"}
{"timestamp":"2026-09-11T20:42:35.772647Z","level":"INFO","message":"pruner: stopping during sleep","target":"miroir_core::task_pruner"}
```
<!-- END-LOG: proxy-logs-20260911T204230Z.log -->

## Final signature-A attribution — bead miroir-b6b71143 Notes, verbatim

The text below is copied unedited from the bead's Notes field, starting at the
`FINAL SIGNATURE A ATTRIBUTION` note (child 4 `miroir-1b17291c`, the final
gate of the 4-way split) and running through the
`CONSOLIDATED SIGNATURE A ROOT CAUSE — REFRESH` note (`miroir-fc5fa7fd`, third
capture cycle). It confirms the consolidated verdict in
[p13-13-root-cause-diagnosis.md](p13-13-root-cause-diagnosis.md) with zero
divergence and pins the NO/NO route-table/git-history answer. The third-cycle
capture it quotes (run 2026-09-11T21:37:57Z, `capture-20260911T213752Z.log` /
`proxy-logs-20260911T213752Z.log`; capture bead `miroir-2c2c095a`, refresh
bead `miroir-fc5fa7fd`) is preserved verbatim with sha256 pins in
[p13-13-third-cycle-signature-a-capture-2026-09-11.md](p13-13-third-cycle-signature-a-capture-2026-09-11.md)
(task `miroir-20f236c3`); its fingerprint is identical to the 20:42Z capture
preserved above.

<!-- BEGIN-NOTE: miroir-b6b71143 notes tail, revision 16 -->
```text
FINAL SIGNATURE A ATTRIBUTION (synthesis child miroir-1b17291c, 2026-09-11; final gate of the miroir-b6b71143 4-way split). Confirms the consolidated verdict above against children 1-3's fresh current-tree results. Divergence from the prior 49deb4f-era evidence: NONE, on every axis.

CLASSIFICATION — router 404 (exactly one of the four-way taxonomy; each alternative ruled out below).

Signature A: 9/10 p13_13 tests panic at their FIRST proxy search POST — `assertion failed: resp.status().is_success()` (first occurrence crates/miroir-proxy/tests/p13_13_cache_flow_integration.rs:508; call site :501 `.post(format!("{}/indexes/products/search", setup.proxy_url))`; 23 `.post` sites in this file, every one on the native path).

VERBATIM CAPTURE (child 2 miroir-d50d1a47, 2026-09-11T20:42:35Z, current working tree — HEAD 49deb4f + uncommitted search.rs diff, proxy rebuilt 20:42:12Z for this run; suite topology: target/debug/miroir-proxy pid 2263583, ports 17770/9090, one getmeili/meilisearch:v1.8.3 node host port 33178, key via MEILI_MASTER_KEY env only; Authorization: Bearer test_master_key, body {"q":"laptop","limit":10}; logs ~/scratch/p13_13_first_search_capture.logs/capture-20260911T204230Z.log and proxy-logs-20260911T204230Z.log):

Native path POST /indexes/products/search:
  HTTP/1.1 404 Not Found
  x-request-id: 2617abe0
  content-length: 0
  date: Fri, 11 Sep 2026 20:42:35 GMT
  (body: empty — 0 bytes)

Control POST /search/products, same process, same key/body:
  HTTP/1.1 200 OK
  content-type: application/json
  x-miroir-degraded: shards=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
  x-request-id: e7cbc303
  content-length: 55
  date: Fri, 11 Sep 2026 20:42:35 GMT

  {"estimatedTotalHits":0,"hits":[],"processingTimeMs":0}

The proxy's own log lines for both requests, same process (proxy-logs-20260911T204230Z.log): request_id 2617abe0 WARN "POST 404 Not Found" path_template=/indexes/products/search duration_ms=0; request_id e7cbc303 INFO "POST 200 OK" path_template=/search/:index duration_ms=2.

ROOT CAUSE: API-contract mismatch between the suite and the proxy route table. The suite addresses the proxy with the Meilisearch-native path POST /indexes/products/search; the proxy serves search only at POST /search/:index (crates/miroir-proxy/src/routes/search.rs:128, nested at "/search" main.rs:852) and POST /multi-search (main.rs:845), and the main router registers no fallback, so axum answers its default unmatched-route 404 with an empty body before any handler runs. Decisive-header reasoning: x-request-id PRESENT proves auth passed first (request_id_middleware is INNER to auth_middleware — main.rs layer order comment, outermost→innermost csrf→auth→extensions→session_pinning→request_id→telemetry, so an auth failure would carry no x-request-id); content-length: 0 with NO content-type and NO JSON envelope rules out upstream passthrough (Meilisearch errors are {"message","code","type","link"} — the same proxy log shows exactly that envelope in the anti-entropy line quoting the node's index_not_found, underscoring the contrast); no `allow` header rules out 405; status 404 rules out proxy-internal 5xx; the control 200 with the same bearer key independently rules out auth 401. Not a proxy fault, not a runtime fault, not an environment fault: the suite is calling a route that does not exist.

ROUTE TABLE / GIT HISTORY (child 3 miroir-a9adebd8; route citations re-verified against the current tree by this gate): POST /indexes/:index/search is servable at the current tree: NO. Was ever a live endpoint at any commit in git history: NO. The /indexes nest (indexes.rs:304-330) registers only "/", "/:index", "/:index/stats", "/:index/settings[/...]", "/:index/_preflight", "/:index/explain", "/:index/documents" — no "/search" leaf, no catch-all. The only POST search route is search.rs:128 under the "/search" nest. Whole-history pickaxe finds only the a046c3a addition of "/:index/search" inside search.rs — which was nested at "/search" and so served POST /search/:index/search, never /indexes/... — removed only via merge 1f686c6's tree resolution, which `git log -S` skips by default (the documented third trap); the scaffold-era "/:index/:sub" catch-all route-matched .../search but returned 501.

CHILD RESULTS AND DIVERGENCE:
- Child 1 miroir-9b6654b2 (env/artifact audit): ports 17770/9090 FREE (zero listeners, no stray foreign proxy); prior capture artifacts intact and readable (capture-20260911T201703Z.log). Environment clean → live reproduction trustworthy. Divergence: none.
- Child 2 miroir-d50d1a47 (re-capture): fingerprint IDENTICAL to the prior capture-20260911T201703Z.log — 404 empty-body native with x-request-id present / 200 degraded control, same header shape, same 55-byte body; only x-request-id and date differ. The uncommitted search.rs diff does NOT change the answer (it refactors the result-cache store key inside search_handler's scatter path; the 404 is a router miss that never reaches the handler, and the control's degraded 200 exits before the changed block). Teardown confirmed (ports released, container gone, no miroir-proxy process).
- Child 3 miroir-a9adebd8 (re-audit): verdict unchanged from miroir-ee6a1540 — not servable now, never was — plus the merge-commit explanation for the pickaxe showing no removal, and confirmation the uncommitted diff adds/removes ZERO route registrations.

STATUS: miroir-b6b71143's two acceptance criteria are met by the consolidated verdict above plus this note — verbatim status line + body + decisive headers quoted from BOTH captures, attribution to router 404 with evidence, and a definitive route-table/history NO/NO. Umbrella decision (repoint the suite at /search/products vs register the native route) remains with miroir-1ec135a1; this bead stays open.

CONSOLIDATED SIGNATURE A ROOT CAUSE — REFRESH (synthesis child miroir-fc5fa7fd, 2026-09-11; third capture cycle of this bead's 4-way split). Extends the miroir-1b17291c final attribution above with children 1-2's fresh current-tree results. Divergence from every prior cycle: NONE. VERDICT UNCHANGED: signature A — 9/10 p13_13 tests panicking at `assertion failed: resp.status().is_success()` (first occurrence crates/miroir-proxy/tests/p13_13_cache_flow_integration.rs:508) — is an axum ROUTER MISS (taxonomy: router 404, empty body, auth passed first) on POST /indexes/:index/search, a route the proxy has never served. Root cause: API-contract mismatch between the suite (written against the Meilisearch-native path) and the proxy route table (search served only at POST /search/:index and POST /multi-search).

FRESH CAPTURE, VERBATIM (child 2 miroir-2c2c095a, run 2026-09-11T21:37:57Z; logs ~/scratch/p13_13_first_search_capture.logs/capture-20260911T213752Z.log and proxy-logs-20260911T213752Z.log; topology: target/debug/miroir-proxy pid 2314388 on client :17770 / metrics :9090, one getmeili/meilisearch:v1.8.3 node on host port 33179 keyed via MEILI_MASTER_KEY env only; tree = HEAD 49deb4f with search.rs dirty (M), binary built 2026-09-11 16:42 local — no rebuild needed; pre-flight ss clean on 17770/9090; Authorization: Bearer test_master_key, body {"q":"laptop","limit":10}):

Native path POST /indexes/products/search:
  HTTP/1.1 404 Not Found
  x-request-id: dea3f7d7
  content-length: 0
  date: Fri, 11 Sep 2026 21:37:57 GMT
  (body: empty — 0 bytes)

Control POST /search/products, same process, same key/body:
  HTTP/1.1 200 OK
  content-type: application/json
  x-miroir-degraded: shards=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
  x-request-id: 31709cfa
  content-length: 55
  date: Fri, 11 Sep 2026 21:37:57 GMT

  {"estimatedTotalHits":0,"hits":[],"processingTimeMs":0}

Proxy request-log cross-check, same process (proxy-logs-20260911T213752Z.log): request_id dea3f7d7 WARN "POST 404 Not Found" status=404 path_template=/indexes/products/search duration_ms=0; request_id 31709cfa INFO "POST 200 OK" status=200 path_template=/search/:index duration_ms=3 — the response x-request-id values match the logged request_ids on BOTH requests.

CLASSIFICATION: router 404 — exactly one of the four-way taxonomy; each alternative ruled out on the fresh capture:
- NOT auth 401: x-request-id PRESENT on the 404, and request_id_middleware is INNER to auth_middleware (main.rs:858-894 layer stack, outermost→innermost csrf→auth→extensions→session_pinning→request_id→telemetry), so the header is stamped only after auth passes. Independently: the control answers 200 with the SAME bearer key on the same process.
- NOT upstream passthrough: content-length: 0, NO content-type, NO {"message","code","type","link"} JSON envelope — a 0-byte body cannot be a Meilisearch error.
- NOT 405: no `allow` header.
- NOT proxy-internal 5xx: the status is 404.
- The empty-body 404 is axum's default unmatched-route response; the main router registers no fallback (the only .fallback in the tree is GET, scoped inside /ui/search, search_ui.rs:150). Third independent reproduction: fingerprint identical across the 20:17:03Z (miroir-8f17b2d0, node port 33177), 20:42:30Z (miroir-d50d1a47, port 33178), and 21:37:52Z (this cycle, port 33179) captures — different processes and nodes each time, same header shape; only x-request-id and date differ. Child 2 also confirmed teardown (ports released, container removed, no proxy process left).

CHILD 1 ROUTE VERDICT (miroir-3b6d614e, re-verified 2026-09-11 at HEAD 49deb4f + uncommitted worktree): POST /indexes/:index/search servable at the current tree: NO. Ever a live endpoint at any commit in git history: NO. The /indexes nest (indexes.rs:309-329) registers only "/", "/:index" (GET/PATCH/DELETE), "/:index/stats", "/:index/settings[/...]", "/:index/_preflight", "/:index/explain", and the /:index/documents nest — no "/search" leaf, no catch-all, and no fallback on the main router. The only POST search route is search.rs:128 under the "/search" nest (main.rs:852), plus POST /multi-search (main.rs:845). Pickaxe for ':index/search' over indexes.rs and main.rs: ZERO commits (default and --full-history). All three documented traps re-confirmed: (1) a046c3a added "/:index/search" inside search.rs but under the /search nest — it served POST /search/{index}/search, never /indexes/...; (2) the scaffold-era "/:index/:sub" any-method catch-all route-MATCHED .../search but unconditionally returned 501 — no search ever executed; (3) the removal lives in merge 1f686c6's tree resolution and is invisible to default git log -S (route present in 1f686c6^2, absent in 1f686c6^1 and in 1f686c6). The uncommitted search.rs diff (26 insertions / 58 deletions, all five hunks inside search_handler body — the §13.10.1 result-cache store-key refactor) adds/removes ZERO route registrations; every other dirty file is non-route code, config, or CI. Scope note re-checked at this cycle: all 23 native-path proxy search POSTs in the suite (21 on /indexes/products/search, 2 on /indexes/books/search at :731/:759; first at :501) bypass the proxy's search handler entirely, so nothing behind the first POST is exercised.

STATUS: this bead's two acceptance criteria are met a third time by the verbatim quotes above plus child 1's NO/NO. Fingerprint stable across three capture cycles and three route audits (miroir-ee6a1540, miroir-a9adebd8, miroir-3b6d614e); the consolidated verdict above stands as final. Umbrella decision (repoint the suite at /search/products vs register POST /indexes/:index/search) remains with miroir-1ec135a1.
```
<!-- END-NOTE: miroir-b6b71143 notes tail, revision 16 -->
