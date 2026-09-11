# p13_13 final signature-A capture and final attribution — 2026-09-11

Preservation copy of the FINAL current-tree re-capture of the proxy's
first-search response (the 20:42Z cycle) and of the final signature-A
attribution, so the p13_13 diagnosis survives scratch cleanup and bead
checkpoint rotation. Companions:
[p13-13-root-cause-diagnosis.md](p13-13-root-cause-diagnosis.md) (the
consolidated verdict, commit `ea704b7`) and
[p13-13-live-run-logs-2026-09-11.md](p13-13-live-run-logs-2026-09-11.md) (the
three original 0/10 suite-run logs, commit `e9c5702`). Copied into the repo by
task `miroir-3c9a5e90`; nothing in the diagnosis changed. **Fourth-cycle
addendum (2026-09-11 ~22:27Z, task `miroir-5ed697fa`):** the committed
reproduction harness was run against the current tree and the native-path 404
reproduced exactly, but the control POST diverged — see
[Fourth cycle — committed-harness run and control-path divergence](#fourth-cycle--committed-harness-run-and-control-path-divergence-2026-09-11t222700z-below)
at the bottom of this file. The signature-A router-404 verdict is NOT
overturned; the reproducibility of the control-path 200 is. Two further
committed-harness runs reproduced the divergence exactly — the same-task
23:03Z retry, then run 23:26:22Z after the task was re-dispatched (its first
dispatch died before committing) — and both runs' logs are archived in
[p13-13-fourth-cycle-committed-harness-capture-2026-09-11.md](p13-13-fourth-cycle-committed-harness-capture-2026-09-11.md).

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

## Fourth cycle — committed-harness run and control-path divergence (2026-09-11T22:27:00Z, below)

Task `miroir-5ed697fa` (child 2's committed harness, fourth capture cycle of
the `miroir-b6b71143` empirical program). Purpose of the cycle: prove that the
**committed** script (`scripts/p13-13-first-search-capture.sh`, commit
`9a145e4`) reproduces the three-cycle fingerprint from the repo — which is
what the eventual remediator will rely on — instead of the scratch copy that
drove cycles 1-3.

**Result: DIVERGENCE — recorded verbatim; the cycle-1-3 verdict was not
force-fit onto it.** The native-path half of the fingerprint reproduced
exactly on every run. The control half did not: `POST /search/products`
returned **no response at all** — curl aborts at its 15 s client cap with 0
bytes received (`rc=28`), the proxy logs no `miroir.request` completion line
and no `miroir.search` "search completed" line for it, and the script exits 1
(its abort path) on every attempt. The wedge is terminal for the process and
reproduced 7/7 across the four committed-harness runs (22:27:00Z, 22:28:50Z,
the same-task retry 23:03:25Z, and the re-dispatch confirmation run
23:26:22Z) and three manual
diagnostic topologies, including a control fired ~15 ms into process life.

**Retry confirmation (2026-09-11T23:03:25Z, same task):** the third
committed-harness run reproduced the cycle's divergence exactly — native 404
intact (`x-request-id: 2cec2b41`), control rc=28 with 0 bytes received, proxy
log showing the identical no-completion shape — and its raw logs are
preserved byte-identical, sha256-pinned, in
[p13-13-fourth-cycle-committed-harness-capture-2026-09-11.md](p13-13-fourth-cycle-committed-harness-capture-2026-09-11.md).
No force-fit either way: the retry was run to confirm the divergence finding
before committing, and it did.

**Re-dispatch confirmation (2026-09-11T23:26:22Z, same task):** the task's
first dispatch died before committing any of this; on re-dispatch, a fresh
committed-harness run reproduced the divergence a fourth time — native 404
intact (`x-request-id: aa22ac8e`), control rc=28 with 0 bytes received, proxy
log showing the identical no-completion shape (last `miroir.request` line =
the native 404 at 23:26:23.404Z, request `aa22ac8e`; teardown SIGTERM at
23:26:38.427Z) — and its raw logs are archived byte-identical, sha256-pinned,
in
[p13-13-fourth-cycle-committed-harness-capture-2026-09-11.md](p13-13-fourth-cycle-committed-harness-capture-2026-09-11.md).
The old verdict was again not force-fit onto the fresh run, and the fresh run
again did not overturn the divergence.

### Runs produced by the committed script

| Run (UTC) | Log | Bytes / lines | sha256 | Outcome |
|---|---|---|---|---|
| 22:27:00 | `capture-20260911T222700Z.log` | 1012 / 20 | `eec37d21141896382ad69ca0b79878e645e2a9b83c9e8acb6a801d6a74a03cc5` | native 404 OK; control rc=28; script exit 1 |
| 22:27:00 | `proxy-logs-20260911T222700Z.log` | 8126 / 43 | `12bfb87309488d9410105485a12f41504907201f1662b1966d30dc9f10e52727` | no control completion line |
| 22:28:50 | `capture-20260911T222850Z.log` | 1012 / 20 | `c72d25b4d6251e47a4265e521295b8108209f29bbe5dc03d08609c77359f72bb` | native 404 OK; control rc=28; script exit 1 |
| 22:28:50 | `proxy-logs-20260911T222850Z.log` | 8126 / 43 | `277249a24a5b48cd3e90c97b0b243deaeea45f74936b9b470acc69af0a6b97ff` | no control completion line |
| 23:03:25 (retry) | `capture-20260911T230325Z.log` | 1012 / 20 | `b7a8b64e5b01d9631d5912b04cc4233b17d4c0dd260477bbaab5c05af9e2f1f7` | native 404 OK; control rc=28; script exit 1 |
| 23:03:25 (retry) | `proxy-logs-20260911T230325Z.log` | 8126 / 43 | `9aab23d3d797752bbb4dcd28abce6735ed3f65a2fc6c7799cc752e329371b11c` | no control completion line |
| 23:26:22 (re-dispatch) | `capture-20260911T232622Z.log` | 1012 / 20 | `e3f1b7ae08eba8c842de44075fc94bb25cf638fd3df61cddf08500483787c5a9` | native 404 OK; control rc=28; script exit 1 |
| 23:26:22 (re-dispatch) | `proxy-logs-20260911T232622Z.log` | 8126 / 43 | `09815163fc7dcd23f7a23b63f532ada8a7b937c9741bca2ab9e7cc9a8e5f861c` | no control completion line |

Runs 3 and 4's raw logs are also committed under
[docs/notes/p13-13-fourth-cycle-committed-harness-capture-2026-09-11.md](p13-13-fourth-cycle-committed-harness-capture-2026-09-11.md)
(byte-identical between `BEGIN-LOG`/`END-LOG` markers; the live
`scripts/p13-13-first-search-capture.logs/` dir is gitignored).

All four runs: pre-flight `ss` clean on 17770/9090; topology =
`target/debug/miroir-proxy` (pids 2360111 / 2361142 / 2390421 / 2411284,
client 127.0.0.1:17770 /
metrics 0.0.0.0:9090) + one `getmeili/meilisearch:v1.8.3` node (host ports
33180 / 33181 / 33184 / 33185, key via `MEILI_MASTER_KEY` env only); tree = HEAD `9a145e4`
+ the same uncommitted `search.rs` diff (26 insertions / 58 deletions) cycles
2-3 ran against; **binary = the identical `target/debug/miroir-proxy` binary
cycles 2-3 used** (mtime `2026-09-11 16:42:12.400900244 -0400` in every one of
the seven preserved capture headers — cycles 2 and 3, cycle-4 runs 1-4;
source mtimes predate the build, no rebuild needed); request
auth `Authorization: Bearer test_master_key`, body `{"q":"laptop","limit":10}`.
The committed script is behaviorally identical to the scratch original that
drove cycles 1-3 (`diff` shows only docblock/path-default refinements:
`REPO_DIR` inference and the `P13_13_CAPTURE_LOG_DIR` override).

### Verbatim fingerprint, expected vs observed

Native path `POST /indexes/products/search` — **UNCHANGED, reproduced on every
run** (run 1 verbatim; run 2 identical shape with `x-request-id: 1c83d8a7`,
`date: Fri, 11 Sep 2026 22:28:51 GMT`; retry run 3 identical shape with
`x-request-id: 2cec2b41`, `date: Fri, 11 Sep 2026 23:03:26 GMT`; re-dispatch
run 4 identical shape with `x-request-id: aa22ac8e`, `date: Fri, 11 Sep 2026
23:26:23 GMT`):

```
HTTP/1.1 404 Not Found
x-request-id: 87b7dda1
content-length: 0
date: Fri, 11 Sep 2026 22:27:02 GMT
```
(body: empty — 0 bytes. Router-miss signature intact: auth passed first —
`x-request-id` present; no content-type, no JSON envelope; no `allow`.)

Control `POST /search/products` — **DIVERGED**. Expected (cycles 1-3, 20:17Z /
20:42Z / 21:37Z):

```
HTTP/1.1 200 OK
content-type: application/json
x-miroir-degraded: shards=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
x-request-id: e7cbc303
content-length: 55
date: Fri, 11 Sep 2026 20:42:35 GMT

{"estimatedTotalHits":0,"hits":[],"processingTimeMs":0}
```

Observed (cycle 4, all four runs): **no status line, no headers, no body — the
request never completes.** Verbatim from the capture log:

```
--- POST /search/products (control: registered route, same process) ---

--- capture summary ---
curl rc /indexes/products/search: 0 (0 = HTTP response received, any status)
curl rc /search/products:         28 (0 = HTTP response received, any status)
```

(`curl: (28) Operation timed out after 15002 milliseconds with 0 bytes
received` — the TCP connection to the proxy was accepted, then no bytes ever
came back; 15.002 s wall.)

## Verbatim cycle-4 logs

### Verbatim capture log — capture-20260911T222700Z.log

<!-- BEGIN-LOG: capture-20260911T222700Z.log -->
```text
=== p13_13 first-search capture 2026-09-11T22:27:02Z ===
proxy: pid 2360111  /home/coding/miroir/target/debug/miroir-proxy
ports: client 17770, metrics 9090
node:  getmeili/meilisearch:v1.8.3 cid=450ab3684c28e848fd25f81d76eb66b10f95faef92809559d11e8432298c649e host port 33180 (key via MEILI_MASTER_KEY env only)
auth:  Authorization: Bearer test_master_key   body: {"q":"laptop","limit":10}
git:   HEAD 9a145e409088338f0ab39dd7ac5e235e575b6631 search.rs:  M crates/miroir-proxy/src/routes/search.rs
bin:   built 2026-09-11 16:42:12.400900244 -0400
--- POST /indexes/products/search (path the p13_13 suite POSTs) ---
HTTP/1.1 404 Not Found
x-request-id: 87b7dda1
content-length: 0
date: Fri, 11 Sep 2026 22:27:02 GMT


--- POST /search/products (control: registered route, same process) ---

--- capture summary ---
curl rc /indexes/products/search: 0 (0 = HTTP response received, any status)
curl rc /search/products:         28 (0 = HTTP response received, any status)
end of capture 2026-09-11T22:27:17Z
```
<!-- END-LOG: capture-20260911T222700Z.log -->

### Verbatim proxy log — proxy-logs-20260911T222700Z.log

Key reading: the control POST (`22:27:02.01+`) produces **no**
`"target":"miroir.request"` line and no `miroir.search` completion line — the
handler never returns before the teardown SIGTERM at `22:27:17.030`. The
anti-entropy pass fingerprints shard 0 and shard 1 (both `index_not_found`
against the node, ~100 ms apart) and then logs nothing further for the
remaining 15 s; the startup health monitor's node poll is likewise absent
after startup (see the node-side excerpt below — it polled exactly once).

<!-- BEGIN-LOG: proxy-logs-20260911T222700Z.log -->
```text
{"timestamp":"2026-09-11T22:27:01.454529Z","level":"INFO","message":"miroir-proxy starting","shards":16,"replication_factor":1,"replica_groups":1,"target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T22:27:01.455305Z","level":"WARN","message":"generated random ADMIN_SESSION_SEAL_KEY; multi-pod deployments must set this manually to a shared value","target":"miroir_proxy::admin_session","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T22:27:01.497282Z","level":"INFO","message":"CDC: background publisher started","target":"miroir_core::cdc"}
{"timestamp":"2026-09-11T22:27:01.497503Z","level":"INFO","message":"rebalancer worker task starting","pod_id":"unknown","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:27:01.497541Z","level":"INFO","message":"loading aliases from task store","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:27:01.497565Z","level":"INFO","message":"ILM worker starting","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:27:01.497519Z","level":"INFO","message":"peer discovery disabled (not running in Kubernetes)","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T22:27:01.497602Z","level":"INFO","message":"drift reconciler started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:27:01.497647Z","level":"INFO","message":"ILM worker: starting evaluation loop","target":"miroir_core::ilm"}
{"timestamp":"2026-09-11T22:27:01.497623Z","level":"INFO","message":"anti-entropy worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:27:01.497690Z","level":"INFO","message":"canary runner enabled but Redis not available - skipping","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T22:27:01.497696Z","level":"INFO","message":"drift reconciler starting (Mode A coordination)","pod_id":"unknown","target":"miroir_core::rebalancer_worker::drift_reconciler"}
{"timestamp":"2026-09-11T22:27:01.497683Z","level":"INFO","message":"Mode C worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:27:01.497757Z","level":"INFO","message":"resource-pressure metrics collection started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:27:01.497724Z","level":"INFO","message":"anti-entropy worker starting (Mode A coordination)","pod_id":"unknown","interval_s":21600,"target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T22:27:01.497737Z","level":"INFO","message":"group sync worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:27:01.497777Z","level":"INFO","message":"Starting Mode C worker loop","target":"miroir_core::mode_c_worker"}
{"timestamp":"2026-09-11T22:27:01.497814Z","level":"INFO","message":"Starting group sync worker","target":"miroir_core::group_sync_worker"}
{"timestamp":"2026-09-11T22:27:01.497822Z","level":"INFO","message":"loaded 0 aliases from task store","target":"miroir_core::alias"}
{"timestamp":"2026-09-11T22:27:01.497883Z","level":"INFO","message":"aliases loaded successfully","count":0,"target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:27:01.497929Z","level":"INFO","message":"task registry TTL pruner started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:27:01.498140Z","level":"INFO","message":"pruner: starting with interval=300s ttl=604800s","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-11T22:27:01.499029Z","level":"INFO","message":"acquired Mode B leader lease","operation_type":"ilm","scope":"ilm","pod_id":"unknown","target":"miroir_core::mode_b_coordinator"}
{"timestamp":"2026-09-11T22:27:01.499269Z","level":"INFO","message":"rebalancer worker starting","pod_id":"unknown","target":"miroir_core::rebalancer_worker"}
{"timestamp":"2026-09-11T22:27:01.499718Z","level":"INFO","message":"pruner: deleted 0 tasks (Mode A), registry_size=0","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-11T22:27:01.501141Z","level":"INFO","message":"acquired leader lease","scope":"rebalance:default","pod_id":"unknown","target":"miroir_core::rebalancer_worker"}
{"timestamp":"2026-09-11T22:27:01.502087Z","level":"INFO","message":"node promoted to Active (was Joining)","node_id":"node-0","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:27:01.502495Z","level":"INFO","message":"ILM worker: acquired leadership, starting evaluation","target":"miroir_core::ilm"}
{"timestamp":"2026-09-11T22:27:01.502753Z","level":"INFO","message":"Service marked as ready","target":"miroir_proxy::routes::admin_endpoints"}
{"timestamp":"2026-09-11T22:27:01.510932Z","level":"INFO","message":"listening","main_addr":"127.0.0.1:17770","metrics_addr":"0.0.0.0:9090","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T22:27:01.514148Z","level":"WARN","message":"peer discovery failed: discovery error: SRV lookup failed for _http._tcp.miroir-headless.default.svc.cluster.local: no record found for name: _http._tcp.miroir-headless.default.svc.cluster.local type: SRV class: IN","target":"miroir_core::mode_a_coordinator"}
{"timestamp":"2026-09-11T22:27:01.514206Z","level":"WARN","message":"failed to refresh peer set, using cached peers","error":"no peers discovered","target":"miroir_core::rebalancer_worker::drift_reconciler"}
{"timestamp":"2026-09-11T22:27:01.514720Z","level":"WARN","message":"peer discovery failed: discovery error: SRV lookup failed for _http._tcp.miroir-headless.default.svc.cluster.local: no record found for name: _http._tcp.miroir-headless.default.svc.cluster.local type: SRV class: IN","target":"miroir_core::mode_a_coordinator"}
{"timestamp":"2026-09-11T22:27:01.514776Z","level":"WARN","message":"failed to refresh peer set, using cached peers","error":"no peers discovered","target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T22:27:01.514799Z","level":"INFO","message":"starting anti-entropy pass","target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T22:27:01.514848Z","level":"INFO","message":"Anti-entropy pass starting: 16 shards to scan","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T22:27:01.516835Z","level":"WARN","message":"Failed to fingerprint shard 0 on node node-0: topology error: fetch failed: HttpError { status: 404, body: \"{\\\"message\\\":\\\"Index `default` not found.\\\",\\\"code\\\":\\\"index_not_found\\\",\\\"type\\\":\\\"invalid_request\\\",\\\"link\\\":\\\"https://docs.meilisearch.com/errors#index_not_found\\\"}\" }","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T22:27:01.620346Z","level":"WARN","message":"Failed to fingerprint shard 1 on node node-0: topology error: fetch failed: HttpError { status: 404, body: \"{\\\"message\\\":\\\"Index `default` not found.\\\",\\\"code\\\":\\\"index_not_found\\\",\\\"type\\\":\\\"invalid_request\\\",\\\"link\\\":\\\"https://docs.meilisearch.com/errors#index_not_found\\\"}\" }","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T22:27:01.946213Z","level":"INFO","pod_id":"unknown","request_id":"8a511185","message":"GET 200 OK","duration_ms":0,"status":200,"method":"GET","path_template":"/health","target":"miroir.request","span":{"method":"GET","path_template":"/health","pod_id":"unknown","request_id":"8a511185","name":"request"}}
{"timestamp":"2026-09-11T22:27:02.009706Z","level":"WARN","pod_id":"unknown","request_id":"87b7dda1","message":"POST 404 Not Found","duration_ms":0,"status":404,"method":"POST","path_template":"/indexes/products/search","target":"miroir.request","span":{"method":"POST","path_template":"/indexes/products/search","pod_id":"unknown","request_id":"87b7dda1","name":"request"}}
{"timestamp":"2026-09-11T22:27:17.030200Z","level":"INFO","message":"shutdown signal received, draining in-flight requests...","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:27:17.030273Z","level":"INFO","message":"shutdown signal received, draining in-flight requests...","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:27:17.501110Z","level":"INFO","message":"pruner: stopping during sleep","target":"miroir_core::task_pruner"}
```
<!-- END-LOG: proxy-logs-20260911T222700Z.log -->

### Verbatim capture log — capture-20260911T222850Z.log

<!-- BEGIN-LOG: capture-20260911T222850Z.log -->
```text
=== p13_13 first-search capture 2026-09-11T22:28:51Z ===
proxy: pid 2361142  /home/coding/miroir/target/debug/miroir-proxy
ports: client 17770, metrics 9090
node:  getmeili/meilisearch:v1.8.3 cid=f60a39257a23c07b75478f81908f94c5bb76798b79f0f7c07edfb3ee37099057 host port 33181 (key via MEILI_MASTER_KEY env only)
auth:  Authorization: Bearer test_master_key   body: {"q":"laptop","limit":10}
git:   HEAD 9a145e409088338f0ab39dd7ac5e235e575b6631 search.rs:  M crates/miroir-proxy/src/routes/search.rs
bin:   built 2026-09-11 16:42:12.400900244 -0400
--- POST /indexes/products/search (path the p13_13 suite POSTs) ---
HTTP/1.1 404 Not Found
x-request-id: 1c83d8a7
content-length: 0
date: Fri, 11 Sep 2026 22:28:51 GMT


--- POST /search/products (control: registered route, same process) ---

--- capture summary ---
curl rc /indexes/products/search: 0 (0 = HTTP response received, any status)
curl rc /search/products:         28 (0 = HTTP response received, any status)
end of capture 2026-09-11T22:29:06Z
```
<!-- END-LOG: capture-20260911T222850Z.log -->

### Verbatim proxy log — proxy-logs-20260911T222850Z.log

Same shape as the 22:27Z run: `request_id 1c83d8a7` is the last
`miroir.request` line (the native 404); the control POST that follows it
never logs a completion; anti-entropy stops after shard 1.

<!-- BEGIN-LOG: proxy-logs-20260911T222850Z.log -->
```text
{"timestamp":"2026-09-11T22:28:51.188330Z","level":"INFO","message":"miroir-proxy starting","shards":16,"replication_factor":1,"replica_groups":1,"target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T22:28:51.189115Z","level":"WARN","message":"generated random ADMIN_SESSION_SEAL_KEY; multi-pod deployments must set this manually to a shared value","target":"miroir_proxy::admin_session","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T22:28:51.230449Z","level":"INFO","message":"CDC: background publisher started","target":"miroir_core::cdc"}
{"timestamp":"2026-09-11T22:28:51.230666Z","level":"INFO","message":"loading aliases from task store","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:28:51.230706Z","level":"INFO","message":"rebalancer worker task starting","pod_id":"unknown","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:28:51.230754Z","level":"INFO","message":"drift reconciler started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:28:51.230705Z","level":"INFO","message":"peer discovery disabled (not running in Kubernetes)","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T22:28:51.230773Z","level":"INFO","message":"ILM worker starting","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:28:51.230849Z","level":"INFO","message":"drift reconciler starting (Mode A coordination)","pod_id":"unknown","target":"miroir_core::rebalancer_worker::drift_reconciler"}
{"timestamp":"2026-09-11T22:28:51.230827Z","level":"INFO","message":"anti-entropy worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:28:51.230870Z","level":"INFO","message":"canary runner enabled but Redis not available - skipping","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T22:28:51.230882Z","level":"INFO","message":"ILM worker: starting evaluation loop","target":"miroir_core::ilm"}
{"timestamp":"2026-09-11T22:28:51.230890Z","level":"INFO","message":"loaded 0 aliases from task store","target":"miroir_core::alias"}
{"timestamp":"2026-09-11T22:28:51.230885Z","level":"INFO","message":"Mode C worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:28:51.230914Z","level":"INFO","message":"anti-entropy worker starting (Mode A coordination)","pod_id":"unknown","interval_s":21600,"target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T22:28:51.230970Z","level":"INFO","message":"Starting Mode C worker loop","target":"miroir_core::mode_c_worker"}
{"timestamp":"2026-09-11T22:28:51.230975Z","level":"INFO","message":"aliases loaded successfully","count":0,"target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:28:51.231000Z","level":"INFO","message":"group sync worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:28:51.231054Z","level":"INFO","message":"Starting group sync worker","target":"miroir_core::group_sync_worker"}
{"timestamp":"2026-09-11T22:28:51.231057Z","level":"INFO","message":"resource-pressure metrics collection started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:28:51.231185Z","level":"INFO","message":"task registry TTL pruner started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:28:51.231414Z","level":"INFO","message":"pruner: starting with interval=300s ttl=604800s","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-11T22:28:51.232750Z","level":"INFO","message":"acquired Mode B leader lease","operation_type":"ilm","scope":"ilm","pod_id":"unknown","target":"miroir_core::mode_b_coordinator"}
{"timestamp":"2026-09-11T22:28:51.232976Z","level":"INFO","message":"rebalancer worker starting","pod_id":"unknown","target":"miroir_core::rebalancer_worker"}
{"timestamp":"2026-09-11T22:28:51.233799Z","level":"INFO","message":"node promoted to Active (was Joining)","node_id":"node-0","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:28:51.234368Z","level":"INFO","message":"acquired leader lease","scope":"rebalance:default","pod_id":"unknown","target":"miroir_core::rebalancer_worker"}
{"timestamp":"2026-09-11T22:28:51.234482Z","level":"INFO","message":"Service marked as ready","target":"miroir_proxy::routes::admin_endpoints"}
{"timestamp":"2026-09-11T22:28:51.234776Z","level":"INFO","message":"pruner: deleted 0 tasks (Mode A), registry_size=0","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-11T22:28:51.235764Z","level":"INFO","message":"ILM worker: acquired leadership, starting evaluation","target":"miroir_core::ilm"}
{"timestamp":"2026-09-11T22:28:51.243342Z","level":"INFO","message":"listening","main_addr":"127.0.0.1:17770","metrics_addr":"0.0.0.0:9090","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T22:28:51.243794Z","level":"WARN","message":"peer discovery failed: discovery error: SRV lookup failed for _http._tcp.miroir-headless.default.svc.cluster.local: no record found for name: _http._tcp.miroir-headless.default.svc.cluster.local type: SRV class: IN","target":"miroir_core::mode_a_coordinator"}
{"timestamp":"2026-09-11T22:28:51.243792Z","level":"WARN","message":"peer discovery failed: discovery error: SRV lookup failed for _http._tcp.miroir-headless.default.svc.cluster.local: no record found for name: _http._tcp.miroir-headless.default.svc.cluster.local type: SRV class: IN","target":"miroir_core::mode_a_coordinator"}
{"timestamp":"2026-09-11T22:28:51.243838Z","level":"WARN","message":"failed to refresh peer set, using cached peers","error":"no peers discovered","target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T22:28:51.243840Z","level":"WARN","message":"failed to refresh peer set, using cached peers","error":"no peers discovered","target":"miroir_core::rebalancer_worker::drift_reconciler"}
{"timestamp":"2026-09-11T22:28:51.243854Z","level":"INFO","message":"starting anti-entropy pass","target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T22:28:51.243885Z","level":"INFO","message":"Anti-entropy pass starting: 16 shards to scan","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T22:28:51.245673Z","level":"WARN","message":"Failed to fingerprint shard 0 on node node-0: topology error: fetch failed: HttpError { status: 404, body: \"{\\\"message\\\":\\\"Index `default` not found.\\\",\\\"code\\\":\\\"index_not_found\\\",\\\"type\\\":\\\"invalid_request\\\",\\\"link\\\":\\\"https://docs.meilisearch.com/errors#index_not_found\\\"}\" }","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T22:28:51.349478Z","level":"WARN","message":"Failed to fingerprint shard 1 on node node-0: topology error: fetch failed: HttpError { status: 404, body: \"{\\\"message\\\":\\\"Index `default` not found.\\\",\\\"code\\\":\\\"index_not_found\\\",\\\"type\\\":\\\"invalid_request\\\",\\\"link\\\":\\\"https://docs.meilisearch.com/errors#index_not_found\\\"}\" }","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T22:28:51.677526Z","level":"INFO","pod_id":"unknown","request_id":"8e19f193","message":"GET 200 OK","duration_ms":0,"status":200,"method":"GET","path_template":"/health","target":"miroir.request","span":{"method":"GET","path_template":"/health","pod_id":"unknown","request_id":"8e19f193","name":"request"}}
{"timestamp":"2026-09-11T22:28:51.738315Z","level":"WARN","pod_id":"unknown","request_id":"1c83d8a7","message":"POST 404 Not Found","duration_ms":0,"status":404,"method":"POST","path_template":"/indexes/products/search","target":"miroir.request","span":{"method":"POST","path_template":"/indexes/products/search","pod_id":"unknown","request_id":"1c83d8a7","name":"request"}}
{"timestamp":"2026-09-11T22:29:06.762526Z","level":"INFO","message":"shutdown signal received, draining in-flight requests...","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:29:06.762593Z","level":"INFO","message":"shutdown signal received, draining in-flight requests...","target":"miroir_proxy"}
{"timestamp":"2026-09-11T22:29:07.236253Z","level":"INFO","message":"pruner: stopping during sleep","target":"miroir_core::task_pruner"}
```
<!-- END-LOG: proxy-logs-20260911T222850Z.log -->

### Verbatim node-side evidence — manual diagnostic topology (22:31-22:41Z)

To locate the stall, a third topology was spawned manually (not by the
committed script; same rendered config, node `p13diag5ed-meili`, host port
33182, proxy pid 2364546) and the control POST was fired while the topology
was probed live. The node's own request log (ANSI stripped, `HTTP request`
lines only) shows the node stayed healthy and that the proxy's search fetch
**never arrives**:

<!-- BEGIN-LOG: p13diag5ed-meili node HTTP-request lines -->
```text
2026-09-11T22:31:49.394571Z  INFO HTTP request{method=GET host="127.0.0.1:33182" route=/health query_parameters= user_agent= status_code=200}: meilisearch: close time.busy=48.4µs time.idle=11.2µs
2026-09-11T22:31:49.410838Z  INFO HTTP request{method=GET host="127.0.0.1:33182" route=/indexes query_parameters= user_agent= status_code=200}: meilisearch: close time.busy=238µs time.idle=176µs
2026-09-11T22:31:49.411070Z  WARN HTTP request{method=GET host="127.0.0.1:33182" route=/indexes/default/documents query_parameters=filter=%7B%22_miroir_shard%22%3A0%7D&limit=1000&offset=0 user_agent= status_code=404 error=Index `default` not found.}: tracing_actix_web::middleware: Error encountered while processing the incoming HTTP request: ResponseError { code: 404, message: "Index `default` not found.", error_code: "index_not_found", error_type: "invalid_request", error_link: "https://docs.meilisearch.com/errors#index_not_found" }
2026-09-11T22:31:49.411116Z  INFO HTTP request{method=GET host="127.0.0.1:33182" route=/indexes/default/documents query_parameters=filter=%7B%22_miroir_shard%22%3A0%7D&limit=1000&offset=0 user_agent= status_code=404 error=Index `default` not found.}: meilisearch: close time.busy=360µs time.idle=185µs
2026-09-11T22:31:49.513678Z  WARN HTTP request{method=GET host="127.0.0.1:33182" route=/indexes/default/documents query_parameters=filter=%7B%22_miroir_shard%22%3A1%7D&limit=1000&offset=0 user_agent= status_code=404 error=Index `default` not found.}: tracing_actix_web::middleware: Error encountered while processing the incoming HTTP request: ResponseError { code: 404, message: "Index `default` not found.", error_code: "index_not_found", error_type: "invalid_request", error_link: "https://docs.meilisearch.com/errors#index_not_found" }
2026-09-11T22:31:49.513738Z  INFO HTTP request{method=GET host="127.0.0.1:33182" route=/indexes/default/documents query_parameters=filter=%7B%22_miroir_shard%22%3A1%7D&limit=1000&offset=0 user_agent= status_code=404 error=Index `default` not found.}: meilisearch: close time.busy=223µs time.idle=102µs
2026-09-11T22:31:52.883749Z  INFO HTTP request{method=GET host="127.0.0.1:33182" route=/health query_parameters= user_agent=curl/8.14.1 status_code=200}: meilisearch: close time.busy=90.3µs time.idle=27.0µs
2026-09-11T22:31:52.896000Z  WARN HTTP request{method=POST host="127.0.0.1:33182" route=/indexes/products/search query_parameters= user_agent=curl/8.14.1 status_code=404 error=Index `products` not found.}: tracing_actix_web::middleware: Error encountered while processing the incoming HTTP request: ResponseError { code: 404, message: "Index `products` not found.", error_code: "index_not_found", error_type: "invalid_request", error_link: "https://docs.meilisearch.com/errors#index_not_found" }
2026-09-11T22:31:52.896075Z  INFO HTTP request{method=POST host="127.0.0.1:33182" route=/indexes/products/search query_parameters= user_agent=curl/8.14.1 status_code=404 error=Index `products` not found.}: meilisearch: close time.busy=402µs time.idle=141µs
2026-09-11T22:35:23.011484Z  WARN HTTP request{method=GET host="127.0.0.1:33182" route=/indexes/products query_parameters= user_agent= status_code=404 error=Index `products` not found.}: tracing_actix_web::middleware: Error encountered while processing the incoming HTTP request: ResponseError { code: 404, message: "Index `products` not found.", error_code: "index_not_found", error_type: "invalid_request", error_link: "https://docs.meilisearch.com/errors#index_not_found" }
2026-09-11T22:35:23.011544Z  INFO HTTP request{method=GET host="127.0.0.1:33182" route=/indexes/products query_parameters= user_agent= status_code=404 error=Index `products` not found.}: meilisearch: close time.busy=366µs time.idle=140µs
2026-09-11T22:36:49.431097Z  INFO HTTP request{method=GET host="127.0.0.1:33182" route=/indexes query_parameters= user_agent= status_code=200}: meilisearch: close time.busy=231µs time.idle=183µs
2026-09-11T22:41:49.447841Z  INFO HTTP request{method=GET host="127.0.0.1:33182" route=/indexes query_parameters= user_agent= status_code=200}: meilisearch: close time.busy=256µs time.idle=246µs
```
<!-- END-LOG: p13diag5ed-meili node HTTP-request lines -->

Reading it (empty `user_agent` = the proxy's reqwest client; `curl/8.14.1` =
direct host probes fired during the hang):

- `22:31:49.394` — the proxy's health monitor polls the node's `/health`
  **once** and never again, despite `health.interval_ms: 200` (expected ~5
  polls/sec).
- `22:31:49.411 / .513` — anti-entropy fingerprints shards 0 and 1 (both
  answered, `index_not_found`), then no shard 2 fetch ever arrives.
- `22:31:52.883 / .896` — direct host probes during the control hang: the node
  answers `/health` 200 and a native search POST `index_not_found` in
  milliseconds. **The node is healthy; the stall is inside the proxy.**
- `22:35:23.011` — a request-scoped `GET /indexes/products` sent through the
  proxy (fired minutes into the wedge) is forwarded upstream and answered
  instantly — one-shot admin-path fetches still work while search handlers
  wedge.
- `22:36:49 / 22:41:49` — some periodic task still lists `/indexes` minutes
  into the wedge; the wedge is selective, not a proxy-wide freeze.

### Characterization established by this cycle

1. **Determinism tonight:** 7/7 wedge (harness 22:27:00Z, harness 22:28:50Z,
   manual 22:31:49Z, manual late-fire 22:35:23Z, manual early-fire 22:41:40Z,
   harness retry 23:03:25Z, harness re-dispatch 23:26:22Z —
   the early-fire manual run came ~15 ms after readiness, i.e. arrival timing
   is NOT the discriminator; the control wedges whenever it fires).
2. **Terminal per process:** a second control fired minutes later against the
   same wedged proxy also hangs; the process never recovers. `GET /health`
   (proxy) keeps answering instantly; router-served 404s keep working.
3. **Where it blocks:** the proxy accepts the connection and the task parks
   before issuing its upstream fetch (node never sees it); `ss` during the
   hang shows the proxy's pooled connections to the node idle-ESTABLISHED;
   `/proc/<pid>/task` shows all tokio workers parked (`futex_wait`/`epoll`),
   nothing spinning — a lost wakeup / unsatisfiable dependency, not a spin.
4. **What did NOT change:** the binary (byte-identical mtime to cycles 2-3),
   the source tree (only docs/harness commits since `49deb4f`; same
   `search.rs` diff), the script's behavior (diff = path defaults only), the
   node image (`d1fb40664b6d…` all day), host load (0.20), no proxy env vars.
5. **Correlated environment delta (mechanism NOT established):** the proxy's
   SRV peer-discovery lookups took ~250 ms each in cycle 2's log (pass start
   `20:42:34.7707` → first SRV warn `20:42:35.2699`) and fail in <1 ms in
   every cycle-4 run (e.g. warn at `.243794`, one ms after `listening` at
   `.243342`; re-dispatch run 4: warn `.910311` vs `listening` `.909317` —
   the same ~1 ms shape). The host resolver still costs ~250-310 ms per cold negative
   (`getent`), so the instant in-proxy failure originates in the proxy's own
   DNS stack, unexplained. Because the early-fire experiment rules out timing,
   this delta correlates with the behavior change but is not shown to cause
   it. Cycle 2's window simply never observed the wedge (its process lived
   ~1.3 s and its control completed before the wedge point), which is why
   cycles 1-3 recorded a clean 200.

### What this means for the diagnosis (no force-fit)

- **Signature A's verdict STANDS:** the suite's 23 proxy search POSTs are all
  on the native path; the native path still answers the exact router-miss 404
  (reproduced twice this cycle), so the suite still panics at
  `p13_13_cache_flow_integration.rs:508` with
  `assertion failed: resp.status().is_success()` for the same router-404
  reason. Nothing in this cycle contradicts
  [p13-13-root-cause-diagnosis.md](p13-13-root-cause-diagnosis.md).
- **What breaks is the control-path claim** the final attribution leans on
  ("control POST answers 200 on the same process — auth, upstream, and proxy
  internals all demonstrably healthy"). Tonight the control path itself wedges
  deterministically, so a suite repointed at `/search/products` — the umbrella
  decision pending with `miroir-1ec135a1` — would now HANG rather than pass.
  The umbrella decision needs this new datum: there is a second, distinct,
  previously-unobserved proxy defect (search-handler wedge; environment-
  sensitive; absent 20:17-21:37Z, deterministic 22:27Z+) that must be
  root-caused separately. This cycle deliberately does NOT guess its
  mechanism.

### Teardown verification

All four committed-harness runs: the script's own cleanup ran (exit 1 path) —
capture container removed, ports released, no proxy process (verified after
runs 1, 3, and 4 with `ss`/`docker ps`/`pgrep -x`; run 2's log shows the same
cleanup completing).
The three manual diagnostic topologies were torn down explicitly: both
`p13diag5ed*-meili` containers removed, manual proxy SIGTERM'd, `/tmp`
config dirs deleted. Final state re-checked after the re-dispatch run
(23:26Z): `ss -ltnp` shows nothing on
17770/9090, `pgrep -x miroir-proxy` empty, no `p13_13-capture`/`p13diag5ed`
containers.
