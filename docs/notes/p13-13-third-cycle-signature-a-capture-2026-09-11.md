# p13_13 third-cycle signature-A capture — 2026-09-11 (21:37Z)

Preservation copy of the THIRD capture cycle's logs (run
2026-09-11T21:37:57Z; capture bead `miroir-2c2c095a`, child 2 of the third
`miroir-b6b71143` split cycle), quoted verbatim by the consolidated root-cause
refresh (`miroir-fc5fa7fd`), so every capture cited by the p13_13 diagnosis
survives scratch cleanup and bead checkpoint rotation. Companions:
[p13-13-final-signature-a-capture-2026-09-11.md](p13-13-final-signature-a-capture-2026-09-11.md)
(the 20:42Z pair and the final attribution notes, commit `72a260c`),
[p13-13-root-cause-diagnosis.md](p13-13-root-cause-diagnosis.md) (the
consolidated verdict, commit `ea704b7`) and
[p13-13-live-run-logs-2026-09-11.md](p13-13-live-run-logs-2026-09-11.md) (the
three original 0/10 suite-run logs, commit `e9c5702`). Copied into the repo by
task `miroir-20f236c3`; nothing in the diagnosis changed — the fingerprint is
identical to the 20:42Z capture preserved in the companion file.

What this file preserves, and who produced it:

| Artifact | Producer | When (UTC) | Original location |
|---|---|---|---|
| `capture-20260911T213752Z.log` | `miroir-2c2c095a` (child 2, third capture cycle of the `miroir-b6b71143` split) | 2026-09-11 21:37:57 | `~/scratch/p13_13_first_search_capture.logs/capture-20260911T213752Z.log` |
| `proxy-logs-20260911T213752Z.log` | same run | 2026-09-11 21:37:57 | `~/scratch/p13_13_first_search_capture.logs/proxy-logs-20260911T213752Z.log` |

Capture topology (both logs, one run): `target/debug/miroir-proxy` pid 2314388
on client 127.0.0.1:17770 / metrics 0.0.0.0:9090, one
`getmeili/meilisearch:v1.8.3` node on host port 33179 (key via
`MEILI_MASTER_KEY` env only); tree = HEAD `49deb4f` + the uncommitted
`search.rs` diff (binary built 2026-09-11 16:42 local — reused for this run,
no rebuild; pre-flight `ss` clean on 17770/9090 per the refresh note); request
auth `Authorization: Bearer test_master_key`, body `{"q":"laptop","limit":10}`.

Verbatim-ness: each log body below is byte-identical to its scratch original —
sha256 in the table below, between unique `BEGIN-LOG`/`END-LOG` markers so it
can be extracted and re-hashed.

## Capture fingerprints

| File | Bytes / lines | sha256 |
|---|---|---|
| `capture-20260911T213752Z.log` | 1263 / 27 | `319de4ac181afca8a6650b878e72a42387b0909a2494592ba18e5f1e932647cd` |
| `proxy-logs-20260911T213752Z.log` | 6823 / 39 | `d7ca8d59b6d71fbfd798710e0aca25a581d7734a2e808bd3109159f1e2aac4ac` |

Reading the capture in one line: the suite's native-path POST
`/indexes/products/search` answers `HTTP/1.1 404 Not Found` with
`x-request-id: dea3f7d7` and a 0-byte body, while the control POST
`/search/products` on the same process answers `200 OK` with the full 55-byte
degraded response — the third independent reproduction of the router-miss
fingerprint (20:17:03Z node port 33177, 20:42:35Z port 33178, 21:37:57Z port
33179; different processes and nodes each time, same header shape; only
`x-request-id` and date differ).

## Verbatim capture log — capture-20260911T213752Z.log

<!-- BEGIN-LOG: capture-20260911T213752Z.log -->
```text
=== p13_13 first-search capture 2026-09-11T21:37:57Z ===
proxy: pid 2314388  /home/coding/miroir/target/debug/miroir-proxy
ports: client 17770, metrics 9090
node:  getmeili/meilisearch:v1.8.3 cid=5d2b4dada029730cbd011928ffe6b79fb6a3cd9fd672e963ca75e6417291a31a host port 33179 (key via MEILI_MASTER_KEY env only)
auth:  Authorization: Bearer test_master_key   body: {"q":"laptop","limit":10}
git:   HEAD 49deb4ff8253c0afff92b3e1644cc415cb7054d9 search.rs:  M crates/miroir-proxy/src/routes/search.rs
bin:   built 2026-09-11 16:42:12.400900244 -0400
--- POST /indexes/products/search (path the p13_13 suite POSTs) ---
HTTP/1.1 404 Not Found
x-request-id: dea3f7d7
content-length: 0
date: Fri, 11 Sep 2026 21:37:57 GMT


--- POST /search/products (control: registered route, same process) ---
HTTP/1.1 200 OK
content-type: application/json
x-miroir-degraded: shards=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
x-request-id: 31709cfa
content-length: 55
date: Fri, 11 Sep 2026 21:37:57 GMT

{"estimatedTotalHits":0,"hits":[],"processingTimeMs":0}
--- capture summary ---
curl rc /indexes/products/search: 0 (0 = HTTP response received, any status)
curl rc /search/products:         0 (0 = HTTP response received, any status)
end of capture 2026-09-11T21:37:57Z
```
<!-- END-LOG: capture-20260911T213752Z.log -->

## Verbatim proxy log — proxy-logs-20260911T213752Z.log

<!-- BEGIN-LOG: proxy-logs-20260911T213752Z.log -->
```text
{"timestamp":"2026-09-11T21:37:56.470871Z","level":"INFO","message":"miroir-proxy starting","shards":16,"replication_factor":1,"replica_groups":1,"target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T21:37:56.471627Z","level":"WARN","message":"generated random ADMIN_SESSION_SEAL_KEY; multi-pod deployments must set this manually to a shared value","target":"miroir_proxy::admin_session","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T21:37:56.511263Z","level":"INFO","message":"CDC: background publisher started","target":"miroir_core::cdc"}
{"timestamp":"2026-09-11T21:37:56.511347Z","level":"INFO","message":"ILM worker starting","target":"miroir_proxy"}
{"timestamp":"2026-09-11T21:37:56.511329Z","level":"INFO","message":"rebalancer worker task starting","pod_id":"unknown","target":"miroir_proxy"}
{"timestamp":"2026-09-11T21:37:56.511370Z","level":"INFO","message":"ILM worker: starting evaluation loop","target":"miroir_core::ilm"}
{"timestamp":"2026-09-11T21:37:56.511351Z","level":"INFO","message":"loading aliases from task store","target":"miroir_proxy"}
{"timestamp":"2026-09-11T21:37:56.511326Z","level":"INFO","message":"peer discovery disabled (not running in Kubernetes)","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T21:37:56.511370Z","level":"INFO","message":"drift reconciler started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T21:37:56.511409Z","level":"INFO","message":"canary runner enabled but Redis not available - skipping","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T21:37:56.511416Z","level":"INFO","message":"drift reconciler starting (Mode A coordination)","pod_id":"unknown","target":"miroir_core::rebalancer_worker::drift_reconciler"}
{"timestamp":"2026-09-11T21:37:56.511468Z","level":"INFO","message":"Mode C worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T21:37:56.511499Z","level":"INFO","message":"Starting Mode C worker loop","target":"miroir_core::mode_c_worker"}
{"timestamp":"2026-09-11T21:37:56.511494Z","level":"INFO","message":"loaded 0 aliases from task store","target":"miroir_core::alias"}
{"timestamp":"2026-09-11T21:37:56.511494Z","level":"INFO","message":"group sync worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T21:37:56.511489Z","level":"INFO","message":"anti-entropy worker started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T21:37:56.511529Z","level":"INFO","message":"Starting group sync worker","target":"miroir_core::group_sync_worker"}
{"timestamp":"2026-09-11T21:37:56.511529Z","level":"INFO","message":"aliases loaded successfully","count":0,"target":"miroir_proxy"}
{"timestamp":"2026-09-11T21:37:56.511538Z","level":"INFO","message":"anti-entropy worker starting (Mode A coordination)","pod_id":"unknown","interval_s":21600,"target":"miroir_core::rebalancer_worker::anti_entropy_worker"}
{"timestamp":"2026-09-11T21:37:56.511543Z","level":"INFO","message":"resource-pressure metrics collection started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T21:37:56.511583Z","level":"INFO","message":"task registry TTL pruner started","target":"miroir_proxy"}
{"timestamp":"2026-09-11T21:37:56.511692Z","level":"INFO","message":"pruner: starting with interval=300s ttl=604800s","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-11T21:37:56.512375Z","level":"INFO","message":"acquired Mode B leader lease","operation_type":"ilm","scope":"ilm","pod_id":"unknown","target":"miroir_core::mode_b_coordinator"}
{"timestamp":"2026-09-11T21:37:56.512470Z","level":"INFO","message":"rebalancer worker starting","pod_id":"unknown","target":"miroir_core::rebalancer_worker"}
{"timestamp":"2026-09-11T21:37:56.512604Z","level":"INFO","message":"pruner: deleted 0 tasks (Mode A), registry_size=0","target":"miroir_core::task_pruner"}
{"timestamp":"2026-09-11T21:37:56.513765Z","level":"INFO","message":"acquired leader lease","scope":"rebalance:default","pod_id":"unknown","target":"miroir_core::rebalancer_worker"}
{"timestamp":"2026-09-11T21:37:56.514634Z","level":"INFO","message":"ILM worker: acquired leadership, starting evaluation","target":"miroir_core::ilm"}
{"timestamp":"2026-09-11T21:37:56.514938Z","level":"INFO","message":"node promoted to Active (was Joining)","node_id":"node-0","target":"miroir_proxy"}
{"timestamp":"2026-09-11T21:37:56.515164Z","level":"INFO","message":"Service marked as ready","target":"miroir_proxy::routes::admin_endpoints"}
{"timestamp":"2026-09-11T21:37:56.518323Z","level":"INFO","message":"listening","main_addr":"127.0.0.1:17770","metrics_addr":"0.0.0.0:9090","target":"miroir_proxy","span":{"pod_id":"unknown","name":"runtime"}}
{"timestamp":"2026-09-11T21:37:56.715520Z","level":"INFO","message":"Service marked as ready","target":"miroir_proxy::routes::admin_endpoints"}
{"timestamp":"2026-09-11T21:37:56.915470Z","level":"INFO","message":"Service marked as ready","target":"miroir_proxy::routes::admin_endpoints"}
{"timestamp":"2026-09-11T21:37:56.961566Z","level":"INFO","pod_id":"unknown","request_id":"de784aab","message":"GET 200 OK","duration_ms":0,"status":200,"method":"GET","path_template":"/health","target":"miroir.request","span":{"method":"GET","path_template":"/health","pod_id":"unknown","request_id":"de784aab","name":"request"}}
{"timestamp":"2026-09-11T21:37:57.019068Z","level":"WARN","pod_id":"unknown","request_id":"dea3f7d7","message":"POST 404 Not Found","duration_ms":0,"status":404,"method":"POST","path_template":"/indexes/products/search","target":"miroir.request","span":{"method":"POST","path_template":"/indexes/products/search","pod_id":"unknown","request_id":"dea3f7d7","name":"request"}}
{"timestamp":"2026-09-11T21:37:57.026818Z","level":"INFO","message":"search completed","index":"products","duration_ms":2,"node_count":16,"estimated_hits":0,"degraded":true,"target":"miroir.search","span":{"index":"\"products\"","jwt_claims":"None","session_id":"None","name":"search_handler"}}
{"timestamp":"2026-09-11T21:37:57.026963Z","level":"INFO","pod_id":"unknown","request_id":"31709cfa","message":"POST 200 OK","duration_ms":3,"status":200,"method":"POST","path_template":"/search/:index","target":"miroir.request","span":{"method":"POST","path_template":"/search/:index","pod_id":"unknown","request_id":"31709cfa","name":"request"}}
{"timestamp":"2026-09-11T21:37:57.032110Z","level":"INFO","message":"shutdown signal received, draining in-flight requests...","target":"miroir_proxy"}
{"timestamp":"2026-09-11T21:37:57.032139Z","level":"INFO","message":"shutdown signal received, draining in-flight requests...","target":"miroir_proxy"}
{"timestamp":"2026-09-11T21:37:57.512746Z","level":"INFO","message":"pruner: stopping during sleep","target":"miroir_core::task_pruner"}
```
<!-- END-LOG: proxy-logs-20260911T213752Z.log -->
