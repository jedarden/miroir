# p13_13 decision-input memo — signature A × the control wedge: remediation options

**For:** umbrella `miroir-1ec135a1` — the sole decision owner. This memo does **not**
make the decision; it reconciles the two failure modes and states each option's
viability so the umbrella can.
**Produced by:** `miroir-d12debdf` (terminal gate child 4/4 of the `miroir-b6b71143`
re-split; siblings: `miroir-7522b867` docs commit, `miroir-b32bcc98` wedge
reproduction, `miroir-514810ac` parked-stage localization).
**Tree:** 2026-09-15, HEAD `10e412d`. Working tree carries the long-standing
pre-dirty `search.rs` cache-key refactor (5 hunks, first at line 476 — zero
route-registering lines, verified by diff grep) plus non-code files; every route
citation below is identical in HEAD and working tree (all cited route lines
precede the first diff hunk). **No new capture cycles were run for this memo** —
everything cited already existed in bead notes, committed docs, or the tree.

---

## 1. Signature A — final attribution (five-cycle-stable)

### 1.1 Verdict

Signature A — 9/10 p13_13 tests panicking at their first proxy search POST,
`assertion failed: resp.status().is_success()` — is an **axum router miss**
(taxonomy: router 404, empty body, auth passed first) on
`POST /indexes/:index/search`. **Root cause: an API-contract mismatch between the
p13_13 suite and the proxy route table.** The suite addresses the proxy with the
Meilisearch-native path; the proxy serves search only at `POST /search/:index`
and `POST /multi-search` and registers no fallback on the main router, so axum
answers the suite's first search POST with its default empty-body 404 before any
handler runs. The verdict was reproduced identically across eight native-path
observations on eight distinct proxy processes (cycles 1–5) under both the
scratch and committed harness forms; it is not auth (x-request-id present
post-auth), not upstream (no error envelope), not a proxy-internal fault (404,
not 5xx), not environment.

### 1.2 Verbatim evidence — the fifth-cycle file

The final capture cycle is durably preserved in-repo by `miroir-7522b867`'s docs
commit `13b16c0`:

**`docs/notes/p13-13-fifth-cycle-signature-a-capture-2026-09-12.md`** — capture
run 2026-09-12T00:05:19Z (child `miroir-2037d2ee` on child `miroir-454e8c77`'s
manual topology; `miroir-f79b50e1`'s attribution note copied verbatim at lines
199–240). Verbatim fingerprint, lines 91–96:

```
HTTP/1.1 404 Not Found
x-request-id: 6577bfec
content-length: 0
date: Sat, 12 Sep 2026 00:05:19 GMT
```

Decisive headers: `x-request-id` **present** (auth passed — request_id_middleware
is inner to auth_middleware, main.rs:858–865 layer-order comment), `content-length: 0`,
**no** `allow` (not 405), **no** `content-type` and no
`{"message","code","type","link"}` envelope (not upstream passthrough). Proxy-log
cross-check in the same file (lines 98–103): `request_id 6577bfec` WARN "POST 404
Not Found" `path_template=/indexes/products/search duration_ms=0` — response
emitted at the router, no handler work. All three scratch logs are sha256-pinned
(lines 81–85) between BEGIN/END-LOG markers with the documented CRLF
binary-extraction caveat (lines 71–77). Companion files preserve cycles 1–4:
`p13-13-final-signature-a-capture-2026-09-11.md` (20:17Z / 20:42Z),
`p13-13-third-cycle-signature-a-capture-2026-09-11.md` (21:37Z),
`p13-13-fourth-cycle-committed-harness-capture-2026-09-11.md` (the four
committed-harness runs from 22:27Z). The consolidated verdict lives in
`docs/notes/p13-13-root-cause-diagnosis.md` and in `miroir-b6b71143`'s notes.

### 1.3 Route citations — re-verified at this memo's HEAD (`10e412d`)

- `crates/miroir-proxy/src/routes/search.rs:128` —
  `axum::Router::new().route("/:index", axum::routing::post(search_handler))`
  (router fn declared at :124); nested at `"/search"` by
  `crates/miroir-proxy/src/main.rs:852` (`.nest("/search", search::router::<UnifiedState>())`).
- Multi-search: `crates/miroir-proxy/src/main.rs:845-848`
  (`.route("/multi-search", post(multi_search::multi_search::<UnifiedState>),) // §13.11`).
- `/indexes` nest registered at `main.rs:850`; the nest's router block is
  `crates/miroir-proxy/src/routes/indexes.rs:304-330`, with registrations at
  **`indexes.rs:309-329`**: `"/"` (309), `"/:index"` GET/PATCH/DELETE (310–315),
  `"/:index/stats"` (316), `"/:index/settings"` (317–322),
  `"/:index/settings/*subpath"` (323–326), `"/:index/_preflight"` (327),
  `"/:index/explain"` (328), `"/:index/documents"` nest (329). **No `"/search"`
  leaf, no catch-all.**
- No fallback on the main router: the only `.fallback(` in the proxy source at
  HEAD is `crates/miroir-proxy/src/routes/search_ui.rs:150` — GET-scoped, inside
  the `/ui/search` nest.
- Suite side at HEAD: 23 native-path POST sites in
  `crates/miroir-proxy/tests/p13_13_cache_flow_integration.rs` (21 on
  `/indexes/products/search`, 2 on `/indexes/books/search` at :841 and :869).
  First site **:600**, inside `acceptance_1_cache_hit_bypasses_fan_out` (fn at
  :556); its failing assert `assert!(resp1.status().is_success())` at **:607**.
  (Earlier bead notes cite :501/:508 — those were true when written; subsequent
  `test(p13_13)` commits drifted the file. The 23-site count and the books split
  are unchanged.)

### 1.4 History verdict

`POST /indexes/:index/search` has **never been a live endpoint at any commit**.
Re-run at this HEAD: `git log -S ':index/search' -- crates/miroir-proxy/src/routes/indexes.rs crates/miroir-proxy/src/main.rs`
returns **zero commits** (default and `--full-history` agree per the prior audit
children `miroir-ee6a1540`, `miroir-a9adebd8`, `miroir-3b6d614e`,
`miroir-1828fe6e`). The three documented pickaxe traps stand: (1) `a046c3a`
added `"/:index/search"` *inside search.rs* but under the `/search` nest — it
served `POST /search/{index}/search`, never `/indexes/...`; (2) the scaffold-era
`"/:index/:sub"` catch-all in indexes.rs route-matched `.../search` but returned
501 unconditionally (removed by `b2490ea`); (3) the removal of (1) lives in merge
`1f686c6`'s tree resolution, invisible to default `git log -S`.

---

## 2. The control wedge — the second, distinct failure mode

`POST /search/:index` **is** registered and reached — and since 2026-09-11T22:27Z
it has parked inside `search_handler` on **every attempted capture: 10/10**.

### 2.1 Reproduction result (child 2/4, `miroir-b32bcc98`, cycle 6)

Two fresh reproductions on 2026-09-12 (binary unchanged since 2026-09-11 16:42;
pre-flight `ss` clean on 17770/9090; every curl bounded):

- **RUN A** — committed harness `scripts/p13-13-first-search-capture.sh`, 00:50:25Z:
  native 404 fingerprint identical for the 9th consecutive observation; control
  `curl: (28) ... timed out after 15002 milliseconds with 0 bytes received`.
- **RUN B** — manual during-wedge topology, 00:56:08–01:01:50Z: control fired
  00:59:49Z bound `-m 120` → `curl (28)` at 120.002 s, 0 bytes. Method innovation:
  the control was fired 80 s before a background tick so a tick landed inside the
  wedge window.

Count reconciliation: 8/8 at the fifth cycle (4 committed-harness cycle-4 runs +
3 cycle-4 manual diagnostic topologies + the cycle-5 manual capture) + 2 in
cycle 6 = **10/10**, as enumerated by the timeline child `miroir-d9bf6824`.
(`miroir-514810ac`'s localization note says "12 wedge instances"; the
explicitly-enumerated timeline count is 10 — this memo uses 10.) A fast-200
control has not occurred since 22:27Z; cycles 1–3 got three fast degraded-200s
from the *same binary*.

### 2.2 Parked stage (child 3/4, `miroir-514810ac` chain)

Log-provable bracket: the `/search/:index` span **is** entered
(`#[tracing::instrument]` at `search.rs:172`) and the terminal
`info!(… "search completed")` is never reached (literal at `search.rs:1169` at
HEAD; `miroir-514810ac`'s note recorded it as :1130-1131 — superseded by this
re-verified citation). Everything between is debug-level and the captures hold
zero DEBUG records, so logs alone cannot shrink the bracket.

Within the bracket, the named stage under the leading hypothesis is **stage 13's
write-preferring topology READ held across the entire DFS**: acquired at
`search.rs:656` at HEAD (`let topo = state.topology.read().await;`; worktree :667
— the note's clean-HEAD 656/worktree 667 pair checks out), held across the
stage-18 DFS scatter-gather await (`dfs_query_then_fetch_search(...).await`,
HEAD :892–895, `.await` at :895; worktree :901–904), released only at
`drop(topo)` HEAD :908 (worktree :917).

### 2.3 Mechanism hypothesis — R1

The request parks inside a **non-completing synchronous DFS tail** within that
read hold: `strategy.merge` (`core/scatter.rs:1123` → `core/merger.rs:96` trait
decl → `RrfStrategy::merge` :152 → `rrf_merge` :231, pure CPU), the sync serde
parse (`crates/miroir-proxy/src/client.rs:143`, outside the client-level
`.timeout` at `client.rs:25`), or `GlobalIdf::from_preflight_responses`
(`core/scatter.rs:1192`). A sync hang logs nothing and parks no await, which
matches the wedge signature (silent from arrival, span ambient — captured live
as request_id `3af56eda` straddling the 01:01:09Z tick ≥121 s). Because tokio's
RwLock is write-preferring, the held read freezes the health checker at its
`main.rs:956` `state.topology.write().await` (per-node probe
`client.get(&url).send().await` at `main.rs:988` happens inside the write hold),
while lock-free routes keep serving mid-wedge and no "node marked
Degraded/Failed" warn (`main.rs:1052`/`:1059`) ever fires.

Competitors are excluded with falsifiers: **R2** (the upstream
no-overall-deadline family) is hang-capable but capped by ~15 s of per-node
timeouts vs observed ≥121 s / ≥4m27s, with zero `miroir.node` warns in any wedge
window; **R3** (`search.rs` coalesced wait) and **R4** (`block_in_place`) are
dead per `miroir-bdab1a41`. R1 itself is **still unconfirmed** — its
confirmatory probe (health-probe cadence grep on the node log; ~2 min; outcome
table on `miroir-bdab1a41` section 4) has not been run. This memo did not run it
(no-new-cycles charter).

### 2.4 Proxy bug or environment artifact?

**A proxy-internal, per-request park — a proxy bug candidate, not an environment
artifact.** The environment was exonerated *during* the wedge (child 2 RUN B):
socket fully delivered then idle (211-byte request at t0, both queues 0/0,
`lastsnd/lastrcv/lastack` 114 s+ — not transport backpressure); router liveness
(`/health` 200 in ~5 ms, `/nope` 404 in ~5 ms at t+0.9/40/82/115 s); threads
constant at 12 (no pool exhaustion); the Meilisearch node answered direct
`/health` in 0.7–1.2 ms mid-wedge **and logged the proxy completing an outbound
`GET /indexes` → 200 in 259 µs at 01:01:09Z** — proxy→node HTTP works while the
handler parks. The park lives in the registered handler's own body.

### 2.5 The open axis (S7)

Why the same binary produced fast degraded-200s at 20:17–21:37Z and has wedged
on every attempt since 22:27Z is **unexplained by every candidate**, including
spawn-age (a cycle-4 control wedged ~15 ms into process life). S7 is the one
residual axis where a time- or environment-dependent *trigger* of the sync-tail
non-completion cannot yet be excluded — it is a trigger question, not a
park-location question.

---

## 3. Remediation options — updated viability

### (a) Repoint the suite at `POST /search/:index` — **BLOCKED by the wedge**

The contract exists and is registered (`search.rs:128` nested at `main.rs:852`),
and cycles 1–3's fast degraded-200 control (200 + `content-type: application/json`
+ `x-miroir-degraded: shards=0..15` + 55-byte body) was its original
justification. **That justification is dead**: the very endpoint the suite would
be repointed at has wedged 10/10 since 22:27Z, including ~15 ms into process
life, on both harness classes, with a during-wedge capture proving the park is
handler-internal. As-is, (a) trades the suite's current fast-fail 404 for a
probable indefinite hang at its first search POST (:600) — strictly worse for CI
(a hang needs an external kill and burns runner time; nothing behind the POST
gets exercised either way). (a) becomes viable only after the wedge is diagnosed
— at minimum R1's confirmatory probe — and fixed; what a post-fix run would
return (degraded-vs-full semantics for the suite's cache-flow assertions) is
unknown until then.

### (b) Register `POST /indexes/:index/search` in the `/indexes` nest — **fixes signature A outright; inherits the wedge**

Adding a `"/:index/search"` leaf to `indexes.rs:309-329` delegating to
`search_handler` eliminates the router-miss by construction: the suite's first
POST would route, and the five-cycle-stable 404 fingerprint disappears. But it
delegates to the **same handler that wedges** — child 2/3's evidence shows the
park is inside `search_handler`'s body and route-agnostic — so (b) likely
surfaces the **same wedge via the native path**: the suite's first POST would
enter `search_handler` and (under R1) park at the same stage, converting the 404
into a hang. It is also a proxy code change carrying a product-contract question
(is Meilisearch-native search compatibility a requirement?) that only the
umbrella can answer.

### Which option child 3's mechanism favors — and why

R1 does **not** separate the options on wedge exposure: the park is in the
handler body, so both (a) and (b) converge on identical exposure, and R1
therefore does not make (a) safe. To the extent the mechanism informs the
choice, it **favors (b), conditionally**, for two reasons:

1. **Same-surface fix.** R1 localizes the wedge to `search_handler`/`search.rs` —
   the same file a (b) registration touches. A single proxy-side change-set
   (register the native leaf + fix the stage-13 hold) would close both failure
   modes at the root and complete the proxy's already Meilisearch-shaped
   `/indexes` surface (stats/settings/documents/preflight/explain are all
   registered there today). Under (a), the proxy's contract stays
   Meilisearch-incompatible and the wedge still needs its own proxy-side fix —
   two workstreams instead of one.
2. **(a)'s evidentiary basis is gone.** (a) rested entirely on the fast
   degraded-200 control; that endpoint's 10/10 wedge record means repointing
   today yields a hung suite on the one path with a proven indefinite-park
   failure mode.

The binding caveat either way: **under R1, neither option delivers a green suite
until the wedge is fixed** (or its trigger understood via S7), and R1 is
unconfirmed until the pending health-probe cadence grep runs — if probes continue
through a wedge, the entire audited await set is cleared and the problem resets
to S7.

### The decision is not made here

Per charter, choosing between (a) and (b) — and sequencing them against the
wedge diagnosis — belongs to umbrella **`miroir-1ec135a1`** (open as of this
memo). This memo supplies the decision inputs only.

---

## 4. Attestation — `miroir-b6b71143`'s two acceptance criteria are met

| Acceptance criterion (verbatim intent) | Satisfying evidence |
|---|---|
| **AC1** — "A note on this bead quotes the captured status line and body (and any decisive headers, e.g. allow/content-length/x-request-id) verbatim." | **MET.** `miroir-b6b71143`'s notes quote both requests verbatim in every capture block: the consolidated verdict (`miroir-f87c46da`, 20:17Z native 404 + control 200 with full header lists and the 55-byte body), the final attribution (`miroir-1b17291c`, 20:42Z, incl. proxy-log cross-check), the third-cycle refresh (`miroir-fc5fa7fd`, 21:37Z), and the fifth-cycle block (`miroir-f79b50e1`, 00:05:19Z). Durably in-repo: the fingerprint block at `docs/notes/p13-13-fifth-cycle-signature-a-capture-2026-09-12.md` lines 91–96 (headers :92–95), the proxy-log cross-check at :98–103, sha256 pins at :81–85; cycles 1–4 in the companion files listed at :40–49. The wedge side is preserved too: the empty attempt-1 section with its epilogue (:126–137) and the rc=28 attempt-2 log (:146–168). |
| **AC2** — "The note attributes signature A to a root cause with evidence and states whether the failing path exists anywhere in the proxy's route table or git history." | **MET.** Attribution to the API-contract mismatch / axum router-404 with the four-way taxonomy exclusions is stated in the consolidated verdict and the fifth-cycle attribution note (fifth-cycle file lines 199–240), and restated as §1.1 of this memo. Route-table verdict **NO** / git-history verdict **NO**: stated by four audit children (`miroir-ee6a1540`, `miroir-a9adebd8`, `miroir-3b6d614e`, `miroir-1828fe6e`) and **re-verified at this memo's HEAD `10e412d`** in §1.3–1.4 (nest registrations enumerated with file:line; zero pickaxe commits; the three pickaxe traps). |

With this memo, `miroir-b6b71143`'s capture-and-attribute charter is complete on
both criteria; what remains open is decision work (`miroir-1ec135a1`) and wedge
diagnosis (R1 probe), not further signature-A capture cycles.

---

## 5. Verification appendix — what this memo ran at HEAD `10e412d`

- Route lines: `git show HEAD:` + grep/sed on `search.rs` (:124/:128),
  `main.rs` (:845-848, :850, :852), `indexes.rs` (:304-330); worktree `sed` on
  `search.rs:124-129` — identical (first dirty-diff hunk at 476;
  `git diff | grep -c '\.route(\|\.nest('` → **0**).
- Fallback: `git grep -n "\.fallback(" HEAD -- crates/miroir-proxy/src` → only
  `search_ui.rs:150`.
- History: `git log --oneline -S ':index/search' -- indexes.rs main.rs` → empty.
- Suite: grep for `indexes/products/search|indexes/books/search` → 23 sites
  (first :600, books :841/:869); enclosing fn `acceptance_1_cache_hit_bypasses_fan_out`
  at :556; `assert!(resp1.status().is_success())` at :607.
- Mechanism anchors re-verified: `search.rs` :172, :656/:667, :892-895/:901-904,
  :908/:917, :1169/:1137 (HEAD/worktree); `main.rs` :956, :988, :1052, :1059;
  `core/scatter.rs` :1123, :1192; `core/merger.rs` :96/:152/:231;
  `crates/miroir-proxy/src/client.rs` :25/:143.
- Beads read: `miroir-b6b71143` (parent, consolidated + five-cycle blocks),
  `miroir-b32bcc98` (child 2), `miroir-514810ac` (child 3),
  `miroir-d9bf6824` (wedge timeline, count enumeration),
  `miroir-1ec135a1` (umbrella, open). No capture cycle was spawned.
