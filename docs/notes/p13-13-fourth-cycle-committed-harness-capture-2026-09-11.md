# p13_13 fourth-cycle committed-harness capture — 2026-09-11 (23:03Z retry + 23:26Z re-dispatch confirmation)

Preservation copy of the FOURTH capture cycle's retry-run logs: the run
produced by the **committed** reproduction harness
(`scripts/p13-13-first-search-capture.sh`, commit `9a145e4`) at
2026-09-11T23:03:25Z by task `miroir-5ed697fa` (child 2's committed harness,
fourth cycle of the `miroir-b6b71143` empirical program). This is the cycle's
third committed-harness-class run — the first two (22:27:00Z / 22:28:50Z, same
task, quoted verbatim in
[p13-13-final-signature-a-capture-2026-09-11.md](p13-13-final-signature-a-capture-2026-09-11.md))
were followed by this explicit retry to confirm the cycle's divergence finding
before it was committed. A fourth run follows below: after the task's first
dispatch died before committing any of this, the re-dispatched task re-ran the
committed harness at 23:26:22Z (same script, same binary, same tree) and its
logs are archived in this file too.

**The retry reproduced the divergence exactly** (7/7 wedge observations for
the cycle across four committed-harness runs and three manual diagnostic
topologies): native `POST /indexes/products/search` returned the router-miss
`HTTP/1.1 404 Not Found` (`content-length: 0`, empty body, `x-request-id:
2cec2b41`) — signature A intact — while control `POST /search/products`
returned **no response**: curl aborted at its 15 s client cap with 0 bytes
received (rc=28), the proxy logged no `miroir.request` completion line and no
`miroir.search` completion for it, and the script exited 1 via its abort path.
The verdict recorded by the fourth cycle (divergence on the control half only;
signature-A router-404 verdict NOT overturned) is unchanged.

Companions:
[p13-13-final-signature-a-capture-2026-09-11.md](p13-13-final-signature-a-capture-2026-09-11.md)
(the fourth-cycle section quotes the 22:27Z / 22:28:50Z runs verbatim),
[p13-13-third-cycle-signature-a-capture-2026-09-11.md](p13-13-third-cycle-signature-a-capture-2026-09-11.md)
(the 21:37Z pair),
[p13-13-root-cause-diagnosis.md](p13-13-root-cause-diagnosis.md) (the
consolidated verdict).

What this file preserves, and who produced it:

| Artifact | Producer | When (UTC) | Original location |
|---|---|---|---|
| `capture-20260911T230325Z.log` | `miroir-5ed697fa` (fourth cycle, committed-harness retry run) | 2026-09-11 23:03:25 | `scripts/p13-13-first-search-capture.logs/capture-20260911T230325Z.log` (gitignored live dir) |
| `proxy-logs-20260911T230325Z.log` | same run | 2026-09-11 23:03:42 | `scripts/p13-13-first-search-capture.logs/proxy-logs-20260911T230325Z.log` (gitignored live dir) |
| `capture-20260911T232622Z.log` | `miroir-5ed697fa` (fourth cycle, committed-harness re-dispatch confirmation run) | 2026-09-11 23:26:22 | `scripts/p13-13-first-search-capture.logs/capture-20260911T232622Z.log` (gitignored live dir) |
| `proxy-logs-20260911T232622Z.log` | same run | 2026-09-11 23:26:39 | `scripts/p13-13-first-search-capture.logs/proxy-logs-20260911T232622Z.log` (gitignored live dir) |

Capture topology (both logs, one run): pre-flight `ss` clean on 17770/9090;
`target/debug/miroir-proxy` pid 2390421 on client 127.0.0.1:17770 / metrics
0.0.0.0:9090, one `getmeili/meilisearch:v1.8.3` node (container
`p13_13-capture-meili-20260911T230325Z`, cid `23cc53d0111c…`) on host port
33184 (key via `MEILI_MASTER_KEY` env only); tree = HEAD `9a145e4` + the
uncommitted `search.rs` diff (26 insertions / 58 deletions); **binary = the
identical `target/debug/miroir-proxy` binary cycles 2-4 used** (mtime
`2026-09-11 16:42:12.400900244 -0400` in the capture header; source mtimes
predate the build, no rebuild); request auth `Authorization: Bearer
test_master_key`, body `{"q":"laptop","limit":10}`. Teardown verified after
the run: no `p13_13-capture-*` container, no `miroir-proxy` process, `ss`
shows nothing on 17770/9090.

Verbatim-ness: each log body below is byte-identical to its original in the
gitignored live log dir — sha256 in the table below, between unique
`BEGIN-LOG`/`END-LOG` markers so it can be extracted and re-hashed. Note for
verifiers: the capture logs carry the curl wire output verbatim, i.e. the
HTTP response headers are CRLF-terminated (5 CR bytes per capture log) —
extract and hash in **binary** mode; a text-mode read (or any editor that
normalizes CRLF) silently strips them and the hash will not match. (The
23:03Z capture-log block was initially committed CRLF-stripped and was
re-spliced byte-identically from the live original before this file was
committed; the other three blocks were committed byte-identical.)

## Capture fingerprints

| File | Bytes / lines | sha256 |
|---|---|---|
| `capture-20260911T230325Z.log` | 1012 / 20 | `b7a8b64e5b01d9631d5912b04cc4233b17d4c0dd260477bbaab5c05af9e2f1f7` |
| `proxy-logs-20260911T230325Z.log` | 8126 / 43 | `9aab23d3d797752bbb4dcd28abce6735ed3f65a2fc6c7799cc752e329371b11c` |
| `capture-20260911T232622Z.log` | 1012 / 20 | `e3f1b7ae08eba8c842de44075fc94bb25cf638fd3df61cddf08500483787c5a9` |
| `proxy-logs-20260911T232622Z.log` | 8126 / 43 | `09815163fc7dcd23f7a23b63f532ada8a7b937c9741bca2ab9e7cc9a8e5f861c` |

Fingerprint, run 3 verbatim (native half identical in shape to runs 1-2; the
control half is the run's one divergence — no status line, no headers, no
body):

```
HTTP/1.1 404 Not Found
x-request-id: 2cec2b41
content-length: 0
date: Fri, 11 Sep 2026 23:03:26 GMT
```

Proxy-log cross-check (run 3): the only `miroir.request` lines are the
readiness `GET /health 200` (`dd363ada`, 23:03:26.903Z) and the native
`POST 404 /indexes/products/search` (`2cec2b41`, 23:03:26.963Z) — the control
POST that follows never logs a completion line; anti-entropy fingerprints
shards 0 and 1 (both `index_not_found`, 23:03:26.477/.581Z) and then logs
nothing until the teardown SIGTERM at 23:03:41.987Z — the same shape as runs
1-2.

## Verbatim capture log — capture-20260911T230325Z.log

<!-- BEGIN-LOG: capture-20260911T230325Z.log -->
=== p13_13 first-search capture 2026-09-11T23:03:26Z ===
proxy: pid 2390421  /home/coding/miroir/target/debug/miroir-proxy
ports: client 17770, metrics 9090
node:  getmeili/meilisearch:v1.8.3 cid=23cc53d0111c477342ea157b6c4150ca198b732410124e372839fd1d4f29c762 host port 33184 (key via MEILI_MASTER_KEY env only)
auth:  Authorization: Bearer test_master_key   body: {"q":"laptop","limit":10}
git:   HEAD 9a145e409088338f0ab39dd7ac5e235e575b6631 search.rs:  M crates/miroir-proxy/src/routes/search.rs
bin:   built 2026-09-11 16:42:12.400900244 -0400
--- POST /indexes/products/search (path the p13_13 suite POSTs) ---
HTTP/1.1 404 Not Found
x-request-id: 2cec2b41
content-length: 0
date: Fri, 11 Sep 2026 23:03:26 GMT


--- POST /search/products (control: registered route, same process) ---

--- capture summary ---
curl rc /indexes/products/search: 0 (0 = HTTP response received, any status)
curl rc /search/products:         28 (0 = HTTP response received, any status)
end of capture 2026-09-11T23:03:41Z
<!-- END-LOG: capture-20260911T230325Z.log -->

## Verbatim proxy log — proxy-logs-20260911T230325Z.log

<!-- BEGIN-LOG: proxy-logs-20260911T230325Z.log -->
{"timestamp":"2026-09-11T23:03:26.414501Z","level":"INFO","message":"miroir-proxy starting","shards":16,"replication_factor":1,"replica_groups":1,"target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T23:03:26.415264Z","level":"WARN","message":"generated random ADMIN_SESSION_SEAL_KEY; multi-pod deployments must set this manually to a shared value","target":"miroir_proxy::admin_session","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T23:03:26.459301Z","level":"INFO","message":"CDC: background publisher started","target":"miroir_core::cdc"}
{"timestamp":"2026-09-11T23:03:26.459493Z","level":"INFO","message":"rebalancer worker task starting","pod_id":"unknown","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:03:26.459521Z","level":"INFO","message":"loading aliases from task store","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:03:26.459538Z","level":"INFO","message":"drift reconciler started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:03:26.459561Z","level":"INFO","message":"anti-entropy worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:03:26.459588Z","level":"INFO","message":"ILM worker starting","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:03:26.459526Z","level":"INFO","message":"peer discovery disabled (not running in Kubernetes)","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T23:03:26.459622Z","level":"INFO","message":"drift reconciler starting (Mode A coordination)","pod_id":"unknown","target":"miroir_core::rebalancer_worker::drift_reconciler"}
{"timestamp":"2026-09-11T23:03:26.459617Z","level":"INFO","message":"anti-entropy worker starting (Mode A coordination)","pod_id":"unknown","interval_s":21600,"target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T23:03:26.459618Z","level":"INFO","message":"Mode C worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:03:26.459670Z","level":"INFO","message":"ILM worker: starting evaluation loop","target":"miroir_core::ilm"}
{"timestamp":"2026-09-11T23:03:26.459641Z","level":"INFO","message":"group sync worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:03:26.459681Z","level":"INFO","message":"Starting Mode C worker loop","target":"miroir_core::mode_c_worker"}
{"timestamp":"2026-09-11T23:03:26.459686Z","level":"INFO","message":"resource-pressure metrics collection started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:03:26.459715Z","level":"INFO","message":"Starting group sync worker","target":"miroir_core::group_sync_worker"}
{"timestamp":"2026-09-11T23:03:26.459692Z","level":"INFO","message":"canary runner enabled but Redis not available - skipping","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T23:03:26.459752Z","level":"INFO","message":"loaded 0 aliases from task store","target":"miroir_core::alias"}
{"timestamp":"2026-09-11T23:03:26.459834Z","level":"INFO","message":"task registry TTL pruner started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:03:26.459821Z","level":"INFO","message":"aliases loaded successfully","count":0,"target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:03:26.460063Z","level":"INFO","message":"pruner: starting with interval=300s ttl=604800s","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-11T23:03:26.461067Z","level":"INFO","message":"acquired Mode B leader lease","operation_type":"ilm","scope":"ilm","pod_id":"unknown","target":"miroir_core::mode_b_coordinator"}
{"timestamp":"2026-09-11T23:03:26.461521Z","level":"INFO","message":"rebalancer worker starting","pod_id":"unknown","target":"miroir_core::rebalancer_worker"}
{"timestamp":"2026-09-11T23:03:26.461730Z","level":"INFO","message":"pruner: deleted 0 tasks (Mode A), registry_size=0","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-11T23:03:26.462747Z","level":"INFO","message":"node promoted to Active (was Joining)","node_id":"node-0","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:03:26.463037Z","level":"INFO","message":"acquired leader lease","scope":"rebalance:default","pod_id":"unknown","target":"miroir_core::rebalancer_worker"}
{"timestamp":"2026-09-11T23:03:26.463384Z","level":"INFO","message":"Service marked as ready","target":"miroir_proxy::routes::admin_endpoints"}
{"timestamp":"2026-09-11T23:03:26.464648Z","level":"INFO","message":"ILM worker: acquired leadership, starting evaluation","target":"miroir_core::ilm"}
{"timestamp":"2026-09-11T23:03:26.469629Z","level":"INFO","message":"listening","main_addr":"127.0.0.1:17770","metrics_addr":"0.0.0.0:9090","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T23:03:26.475613Z","level":"WARN","message":"peer discovery failed: discovery error: SRV lookup failed for _http._tcp.miroir-headless.default.svc.cluster.local: no record found for name: _http._tcp.miroir-headless.default.svc.cluster.local type: SRV class: IN","target":"miroir_core::mode_a_coordinator"}
{"timestamp":"2026-09-11T23:03:26.475658Z","level":"WARN","message":"failed to refresh peer set, using cached peers","error":"no peers discovered","target":"miroir_core::rebalancer_worker::drift_reconciler"}
{"timestamp":"2026-09-11T23:03:26.475789Z","level":"WARN","message":"peer discovery failed: discovery error: SRV lookup failed for _http._tcp.miroir-headless.default.svc.cluster.local: no record found for name: _http._tcp.miroir-headless.default.svc.cluster.local type: SRV class: IN","target":"miroir_core::mode_a_coordinator"}
{"timestamp":"2026-09-11T23:03:26.475845Z","level":"WARN","message":"failed to refresh peer set, using cached peers","error":"no peers discovered","target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T23:03:26.475865Z","level":"INFO","message":"starting anti-entropy pass","target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T23:03:26.475905Z","level":"INFO","message":"Anti-entropy pass starting: 16 shards to scan","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T23:03:26.477613Z","level":"WARN","message":"Failed to fingerprint shard 0 on node node-0: topology error: fetch failed: HttpError { status: 404, body: \"{\\\"message\\\":\\\"Index `default` not found.\\\",\\\"code\\\":\\\"index_not_found\\\",\\\"type\\\":\\\"invalid_request\\\",\\\"link\\\":\\\"https://docs.meilisearch.com/errors#index_not_found\\\"}\" }","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T23:03:26.581873Z","level":"WARN","message":"Failed to fingerprint shard 1 on node node-0: topology error: fetch failed: HttpError { status: 404, body: \"{\\\"message\\\":\\\"Index `default` not found.\\\",\\\"code\\\":\\\"index_not_found\\\",\\\"type\\\":\\\"invalid_request\\\",\\\"link\\\":\\\"https://docs.meilisearch.com/errors#index_not_found\\\"}\" }","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T23:03:26.903782Z","level":"INFO","pod_id":"unknown","request_id":"dd363ada","message":"GET 200 OK","duration_ms":0,"status":200,"method":"GET","path_template":"/health","target":"miroir.request","span":{"method":"GET","path_template":"/health","pod_id":"unknown","request_id":"dd363ada","name":"request"}}
{"timestamp":"2026-09-11T23:03:26.963908Z","level":"WARN","pod_id":"unknown","request_id":"2cec2b41","message":"POST 404 Not Found","duration_ms":0,"status":404,"method":"POST","path_template":"/indexes/products/search","target":"miroir.request","span":{"method":"POST","path_template":"/indexes/products/search","pod_id":"unknown","request_id":"2cec2b41","name":"request"}}
{"timestamp":"2026-09-11T23:03:41.987621Z","level":"INFO","message":"shutdown signal received, draining in-flight requests...","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:03:41.987694Z","level":"INFO","message":"shutdown signal received, draining in-flight requests...","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:03:42.463167Z","level":"INFO","message":"pruner: stopping during sleep","target":"miroir_core::task_pruner"}
<!-- END-LOG: proxy-logs-20260911T230325Z.log -->

## Re-dispatch confirmation run — 2026-09-11T23:26:22Z (fourth committed-harness run)

After the task's first dispatch died before committing anything, the
re-dispatched task re-ran the committed script (23:26:22Z pre-flight; capture
header 23:26:23Z). Result: the divergence reproduced a fourth time,
byte-shape identical to runs 1-3 — native 404 OK (`x-request-id: aa22ac8e`),
control rc=28 with 0 bytes received, script exit 1 via its abort path. Proxy
pid 2411284; node `p13_13-capture-meili-20260911T232622Z` (cid
`d05ecbed3ead…`) on host port 33185; same HEAD `9a145e4` + uncommitted
`search.rs` diff; same binary (mtime `2026-09-11 16:42:12.400900244 -0400` in
the capture header).

Fingerprint, run 4 verbatim (native half identical in shape to runs 1-3; the
control half is again the run's one divergence — no status line, no headers,
no body):

```
HTTP/1.1 404 Not Found
x-request-id: aa22ac8e
content-length: 0
date: Fri, 11 Sep 2026 23:26:23 GMT
```

Proxy-log cross-check (run 4): the only `miroir.request` lines are the
readiness `GET /health 200` (`b384ba00`, 23:26:23.342Z) and the native
`POST 404 /indexes/products/search` (`aa22ac8e`, 23:26:23.404Z) — the control
POST that follows never logs a completion line; anti-entropy fingerprints
shards 0 and 1 (both `index_not_found`, 23:26:22.911Z / 23:26:23.013Z) and
then logs nothing until the teardown SIGTERM at 23:26:38.427Z — the same
shape as runs 1-3. The cycle's SRV instant-fail delta is present again: first
SRV warn at 23:26:22.910311Z, ~1.0 ms after `listening` at 23:26:22.909317Z
(cycle 2: ~250 ms).

Teardown verified after the run: `docker ps -a` shows zero `p13_13-capture`
containers, `pgrep -x miroir-proxy` empty, `ss -ltnp` shows nothing on
17770/9090.

## Verbatim capture log — capture-20260911T232622Z.log

<!-- BEGIN-LOG: capture-20260911T232622Z.log -->
=== p13_13 first-search capture 2026-09-11T23:26:23Z ===
proxy: pid 2411284  /home/coding/miroir/target/debug/miroir-proxy
ports: client 17770, metrics 9090
node:  getmeili/meilisearch:v1.8.3 cid=d05ecbed3eada2a1467b7687869f69c91cebd84028604073c5aa69be4e3d4db6 host port 33185 (key via MEILI_MASTER_KEY env only)
auth:  Authorization: Bearer test_master_key   body: {"q":"laptop","limit":10}
git:   HEAD 9a145e409088338f0ab39dd7ac5e235e575b6631 search.rs:  M crates/miroir-proxy/src/routes/search.rs
bin:   built 2026-09-11 16:42:12.400900244 -0400
--- POST /indexes/products/search (path the p13_13 suite POSTs) ---
HTTP/1.1 404 Not Found
x-request-id: aa22ac8e
content-length: 0
date: Fri, 11 Sep 2026 23:26:23 GMT


--- POST /search/products (control: registered route, same process) ---

--- capture summary ---
curl rc /indexes/products/search: 0 (0 = HTTP response received, any status)
curl rc /search/products:         28 (0 = HTTP response received, any status)
end of capture 2026-09-11T23:26:38Z
<!-- END-LOG: capture-20260911T232622Z.log -->

## Verbatim proxy log — proxy-logs-20260911T232622Z.log

<!-- BEGIN-LOG: proxy-logs-20260911T232622Z.log -->
{"timestamp":"2026-09-11T23:26:22.853405Z","level":"INFO","message":"miroir-proxy starting","shards":16,"replication_factor":1,"replica_groups":1,"target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T23:26:22.854175Z","level":"WARN","message":"generated random ADMIN_SESSION_SEAL_KEY; multi-pod deployments must set this manually to a shared value","target":"miroir_proxy::admin_session","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T23:26:22.895678Z","level":"INFO","message":"CDC: background publisher started","target":"miroir_core::cdc"}
{"timestamp":"2026-09-11T23:26:22.895860Z","level":"INFO","message":"rebalancer worker task starting","pod_id":"unknown","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:26:22.895897Z","level":"INFO","message":"loading aliases from task store","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:26:22.895925Z","level":"INFO","message":"ILM worker starting","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:26:22.895874Z","level":"INFO","message":"peer discovery disabled (not running in Kubernetes)","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T23:26:22.895956Z","level":"INFO","message":"drift reconciler started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:26:22.896012Z","level":"INFO","message":"ILM worker: starting evaluation loop","target":"miroir_core::ilm"}
{"timestamp":"2026-09-11T23:26:22.896001Z","level":"INFO","message":"anti-entropy worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:26:22.896035Z","level":"INFO","message":"canary runner enabled but Redis not available - skipping","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T23:26:22.896025Z","level":"INFO","message":"Mode C worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:26:22.896058Z","level":"INFO","message":"drift reconciler starting (Mode A coordination)","pod_id":"unknown","target":"miroir_core::rebalancer_worker::drift_reconciler"}
{"timestamp":"2026-09-11T23:26:22.896092Z","level":"INFO","message":"resource-pressure metrics collection started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:26:22.896086Z","level":"INFO","message":"anti-entropy worker starting (Mode A coordination)","pod_id":"unknown","interval_s":21600,"target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T23:26:22.896113Z","level":"INFO","message":"Starting Mode C worker loop","target":"miroir_core::mode_c_worker"}
{"timestamp":"2026-09-11T23:26:22.896098Z","level":"INFO","message":"group sync worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:26:22.896188Z","level":"INFO","message":"Starting group sync worker","target":"miroir_core::group_sync_worker"}
{"timestamp":"2026-09-11T23:26:22.896202Z","level":"INFO","message":"loaded 0 aliases from task store","target":"miroir_core::alias"}
{"timestamp":"2026-09-11T23:26:22.896244Z","level":"INFO","message":"task registry TTL pruner started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:26:22.896271Z","level":"INFO","message":"aliases loaded successfully","count":0,"target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:26:22.896484Z","level":"INFO","message":"pruner: starting with interval=300s ttl=604800s","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-11T23:26:22.897427Z","level":"INFO","message":"acquired Mode B leader lease","operation_type":"ilm","scope":"ilm","pod_id":"unknown","target":"miroir_core::mode_b_coordinator"}
{"timestamp":"2026-09-11T23:26:22.897627Z","level":"INFO","message":"rebalancer worker starting","pod_id":"unknown","target":"miroir_core::rebalancer_worker"}
{"timestamp":"2026-09-11T23:26:22.897980Z","level":"INFO","message":"pruner: deleted 0 tasks (Mode A), registry_size=0","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-11T23:26:22.899483Z","level":"INFO","message":"acquired leader lease","scope":"rebalance:default","pod_id":"unknown","target":"miroir_core::rebalancer_worker"}
{"timestamp":"2026-09-11T23:26:22.900344Z","level":"INFO","message":"node promoted to Active (was Joining)","node_id":"node-0","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:26:22.900708Z","level":"INFO","message":"ILM worker: acquired leadership, starting evaluation","target":"miroir_core::ilm"}
{"timestamp":"2026-09-11T23:26:22.900942Z","level":"INFO","message":"Service marked as ready","target":"miroir_proxy::routes::admin_endpoints"}
{"timestamp":"2026-09-11T23:26:22.909317Z","level":"INFO","message":"listening","main_addr":"127.0.0.1:17770","metrics_addr":"0.0.0.0:9090","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T23:26:22.910311Z","level":"WARN","message":"peer discovery failed: discovery error: SRV lookup failed for _http._tcp.miroir-headless.default.svc.cluster.local: no record found for name: _http._tcp.miroir-headless.default.svc.cluster.local type: SRV class: IN","target":"miroir_core::mode_a_coordinator"}
{"timestamp":"2026-09-11T23:26:22.910366Z","level":"WARN","message":"failed to refresh peer set, using cached peers","error":"no peers discovered","target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T23:26:22.910383Z","level":"INFO","message":"starting anti-entropy pass","target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T23:26:22.910417Z","level":"INFO","message":"Anti-entropy pass starting: 16 shards to scan","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T23:26:22.910659Z","level":"WARN","message":"peer discovery failed: discovery error: SRV lookup failed for _http._tcp.miroir-headless.default.svc.cluster.local: no record found for name: _http._tcp.miroir-headless.default.svc.cluster.local type: SRV class: IN","target":"miroir_core::mode_a_coordinator"}
{"timestamp":"2026-09-11T23:26:22.910711Z","level":"WARN","message":"failed to refresh peer set, using cached peers","error":"no peers discovered","target":"miroir_core::rebalancer_worker::drift_reconciler"}
{"timestamp":"2026-09-11T23:26:22.911990Z","level":"WARN","message":"Failed to fingerprint shard 0 on node node-0: topology error: fetch failed: HttpError { status: 404, body: \"{\\\"message\\\":\\\"Index `default` not found.\\\",\\\"code\\\":\\\"index_not_found\\\",\\\"type\\\":\\\"invalid_request\\\",\\\"link\\\":\\\"https://docs.meilisearch.com/errors#index_not_found\\\"}\" }","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T23:26:23.013912Z","level":"WARN","message":"Failed to fingerprint shard 1 on node node-0: topology error: fetch failed: HttpError { status: 404, body: \"{\\\"message\\\":\\\"Index `default` not found.\\\",\\\"code\\\":\\\"index_not_found\\\",\\\"type\\\":\\\"invalid_request\\\",\\\"link\\\":\\\"https://docs.meilisearch.com/errors#index_not_found\\\"}\" }","target":"miroir_core::anti_entropy"}
{"timestamp":"2026-09-11T23:26:23.342435Z","level":"INFO","pod_id":"unknown","request_id":"b384ba00","message":"GET 200 OK","duration_ms":0,"status":200,"method":"GET","path_template":"/health","target":"miroir.request","span":{"method":"GET","path_template":"/health","pod_id":"unknown","request_id":"b384ba00","name":"request"}}
{"timestamp":"2026-09-11T23:26:23.404589Z","level":"WARN","pod_id":"unknown","request_id":"aa22ac8e","message":"POST 404 Not Found","duration_ms":0,"status":404,"method":"POST","path_template":"/indexes/products/search","target":"miroir.request","span":{"method":"POST","path_template":"/indexes/products/search","pod_id":"unknown","request_id":"aa22ac8e","name":"request"}}
{"timestamp":"2026-09-11T23:26:38.427681Z","level":"INFO","message":"shutdown signal received, draining in-flight requests...","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:26:38.427739Z","level":"INFO","message":"shutdown signal received, draining in-flight requests...","target":"miroir_proxy"}
{"timestamp":"2026-09-11T23:26:38.899332Z","level":"INFO","message":"pruner: stopping during sleep","target":"miroir_core::task_pruner"}
<!-- END-LOG: proxy-logs-20260911T232622Z.log -->
