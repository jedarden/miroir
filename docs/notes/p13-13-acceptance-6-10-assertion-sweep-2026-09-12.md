# p13_13 acceptance_6–10 never-firing assertion sweep (2026-09-12)

Bead: miroir-612b2549. Same two-layer sweep as the acceptance_1–5 child over
`crates/miroir-proxy/tests/p13_13_cache_flow_integration.rs` tests
`acceptance_6_graceful_handling_of_cache_connection_failure` through
`acceptance_10_cache_with_complex_query`: mechanical never-firing comparisons
first, then semantic vacuity, reasoning from `ResultCache` semantics
(`miroir-core/src/result_cache.rs`: TTL anchored at `insert`, expired entries
removed-and-missed on `get`, LRU cap, hits never refresh) and the proxy's
failed-op short-circuit behavior (`routes/search.rs`: cache `get` Err →
log-and-continue **without** miss accounting; `Err(_)` from cache ops never
aborts the request).

Source facts every pin below was re-verified against on this date:

- Cache key = (`effective_index`, canonicalized body, `settings_version`) at
  lookup time (`routes/search.rs` ~L483–510 in HEAD).
- `settings_broadcast.commit(index)` is `await`ed **before** the settings
  PATCH handler returns its 2xx (`routes/indexes.rs` Phase 3, ~L1117), so a
  version bump observed after an awaited PATCH is deterministic — the
  acceptance_9 version pin cannot race.
- `X-Miroir-Settings-Version` is emitted on the cached, coalesced, and
  scattered response paths whenever the version is > 0, and is absent
  otherwise (reads as 0 in the helper).
- Miss accounting: `Ok(None)` and cache-deserialize failure each increment
  `miroir_result_cache_misses_total`; a cache `get` transport `Err(_)`
  increments nothing (graceful degrade, unaccounted).

## Trap verification (from the dispatch's known-traps list)

| Trap | Status | Evidence |
|------|--------|----------|
| `if let Ok` around the settings PATCH swallows transport errors and skips the acceptance | fixed (pre-existing in working tree) | PATCH `send().await.unwrap()` + unconditional `assert!(settings_resp.status().is_success(), …)`; `grep 'if let Ok'` over the file returns nothing |
| whole-body equality implies a sub-field assert two lines below | already clean | In 6–10 every whole-body `assert_eq!(result1, result2)` is the **last** body assert; what follows are counter deltas. The two sub-field asserts that remain (`acceptance_6` hits len, `acceptance_8` results[0] len) are content pins in the *other* direction — equality between responses cannot imply them |
| TTL-expiry / settings-version invalidation invisible to own asserts → counter pins | fixed this cycle | acceptance_7: miss+scatter `+2` deltas. acceptance_9: miss+scatter `+2` deltas **plus** the new `version2 == version1 + 1` observation pin (the uncommitted working-tree hunk this bead landed — without it a stalled broadcast lets the entry expire naturally and the deltas pass with the invalidation regression present) |
| acceptance_8 deliberately counter-free | kept, commented | comment at the top of the test: ten concurrent identical queries land as an arbitrary coalesce/hit/miss mix, any split would flake; the pins are response consistency under the race |

## Per-test assertion table

Legend: each assertion → the named regression that makes it fail.
Nothing-found is the outcome for the mechanical layer; the one semantic fix
this bead landed is marked **[landed]**.

### acceptance_6_graceful_handling_of_cache_connection_failure

| Assertion | Regression that fails it |
|-----------|--------------------------|
| `resp.status().is_success()` | a cache-layer failure that aborts the request instead of degrading |
| `result["hits"].len() == 1` | fan-out returns wrong rows (content, independent of any equality) |
| `misses_after == misses_before + 1` | miss lookup never accounted — cache `Ok(None)` path regression (note: a `get` `Err(_)` degrades *without* accounting, so exactly this delta is what exposes it) |
| `scatter_after == scatter_before + 1` | the graceful path serves without gathering (fan-out bypassed) |

Honesty note kept from the prior pass: with the in-process `ResultCache` there
is no injectable connection failure end-to-end (every get/insert returns
`Ok`); real failure injection stays unit-level in
`p13_12_cache_failure_handling.rs`. This test pins the uncached flow shape,
and says so.

### acceptance_7_cache_ttl_expiration

| Assertion | Regression that fails it |
|-----------|--------------------------|
| `resp1`/`resp2` success | request aborted |
| `result1 == result2` | stored-body corruption / wrong-entry serve (round-trip fidelity; expiry itself is *not* its job — deltas below) |
| `misses_after == misses_before + 2` | entry outlived its TTL (expiry broken) → repeat is a hit → delta falls short at +1 |
| `scatter_after == scatter_before + 2` | unexpired entry served → bypass → flat at +1 (upstream-load side of the same regression) |

Timing verified: sleep measured from query 1's completion, TTL anchored at
insert (scatter end), 600 ms > 500 ms `ttl_ms`; a host stall only increases
margin. `ttl_ms: 500` is pinned in the generated config, not defaulted.

### acceptance_8_concurrent_cache_access

| Assertion | Regression that fails it |
|-----------|--------------------------|
| per-request success (×10, panics propagate via `handle.await`) | any request failed under concurrency |
| `first_result == result` (×9) | divergence between concurrent responses (race/partial-write/coalesce returning a mismatched body) |
| `results[0]["hits"].len() == 1` | all responses identically *wrong* (content pin; equality loop cannot imply it) |

Counters deliberately absent (coalesced in-flight duplicates make any
hit/miss split flake) — commented in-test, kept.

### acceptance_9_cache_invalidation_on_index_update

| Assertion | Regression that fails it |
|-----------|--------------------------|
| `resp1` success | request aborted |
| PATCH `send().unwrap()` + unconditional `status().is_success()` | transport error **or** non-2xx on the broadcast — previously skippable, now fatal (trap 1) |
| `version2 == version1 + 1` **[landed]** | broadcast stalled/never committed — the exact case where the deltas below pass with invalidation broken (entry expires naturally, repeat misses either way). Absent header reads 0 = pre-broadcast state |
| `result1 == result2` | wrong-entry serve / body corruption (not invalidation's job) |
| `misses_after == misses_before + 2` | stale entry survived the version-bump key change → hit → falls short at +1 |
| `scatter_after == scatter_before + 2` | same regression from the upstream-load side |

Complementarity: the version pin observes the bump; the miss delta proves the
bump actually entered the cache key. Neither covers the other's regression.

### acceptance_10_cache_with_complex_query

| Assertion | Regression that fails it |
|-----------|--------------------------|
| `resp1` success | request aborted |
| `result1["facetDistribution"].is_object()` | fan-out dropped facets (checked before any equality, on result1 alone — fires independently) |
| `scatter_after_first == scatter_before + 1` | complex query short-circuited or double-scattered (poisons the repeat comparison) |
| `result1 == result2` | stored-body corruption (not the caching claim's job) |
| `hits_after == hits_before + 1` | complex shape never stored / lookup key ≠ store key → repeat misses → flat. (Replaced the old `facetDistribution` self-compare, which was implied by the whole-body equality and could never fire) |
| `scatter_after == scatter_after_first` | a served hit that still scatters — passes the hit delta and body equality, only this counter exposes it |

## Outcome

- Mechanical layer: no never-firing comparison remains in acceptance_6–10.
- Semantic layer: one gap — acceptance_9's invalidation deltas were passable
  with the regression present (stalled broadcast) — closed by the
  settings-version observation pin, landed with this bead.
- All prior-pass comments re-verified against source; no comment correction
  needed beyond documenting the awaited-commit ordering (added to the test as
  a one-line note beside the version pin).

## Re-verification pass (this bead, 2026-09-12)

Every claim above was re-derived from source rather than carried forward:

- `result_cache.rs` `get`/`insert`: TTL anchored at insert
  (`CacheEntry::new` stamps `expires_at = now + ttl` inside `insert`);
  an expired entry is popped and returned as `Ok(None)` on the next `get`,
  so an expired repeat flows through the proxy's miss accounting; hits do
  not refresh expiry. This is what makes acceptance_7's `+2` deltas the
  expiry pin.
- `routes/search.rs` lookup path: `Ok(None)` and cache-deserialize failure
  each `inc_result_cache_misses()`; a cache `get` `Err(_)` logs and
  continues with **no** accounting; the hit branch returns before
  `record_scatter_fan_out`, which is what makes acceptance_10's flat
  `scatter_after == scatter_after_first` pin the bypass.
- `X-Miroir-Settings-Version` emission is `if version > 0` on the cached
  path and `if current_version > 0` on the scattered response — the
  absent-reads-as-0 helper comment holds.
- `routes/indexes.rs` Phase 3: `.commit(index)` is awaited inside
  `two_phase_settings_broadcast` before its `Ok(Json(...))` return, so the
  `version2 == version1 + 1` pin is ordered against the repeat and cannot
  flake on a late commit. The hash-mismatch repair backoff inside Phase 2
  (1 s·2^n, capped 60 s) is the concrete stall the pin defends against.
- `ttl_ms: 500` is written literally into the generated config (not
  defaulted), so acceptance_7's 600 ms overshoot margin is pinned.
- Trap 1 re-checked: `grep 'if let Ok'` over the test file returns nothing;
  the settings PATCH is `send().await.unwrap()` plus an unconditional
  `assert!(is_success)`.

One cosmetic correction landed in this pass: the version-pin comment block
had a broken line wrap (one 109-char line mid-sentence); rewrapped to match
the file's ~77-column comment style. No assertion changed.

**Verification scope, stated plainly:** this sweep is static. The deltas and
the version pin were validated against source semantics, not against a live
suite run — a live run is currently blocked upstream of this bead: the suite
POSTs `/indexes/{uid}/search`, which the proxy's router does not register
(search lives at `/search/:index` via `main.rs`'s `.nest("/search", ...)`),
so every acceptance fails at its first `is_success()` assert with an axum
404 (~15 s, all 10). That route-contract mismatch is the umbrella
investigation's finding (miroir-b6b71143) and affects acceptance_1–10
alike; fixing it is out of this bead's test-only scope. Under that mismatch
a live run produces no evidence about these assertions in either direction.

The working tree this pass verified against also carries an in-flight
production change in `routes/search.rs` (store path reusing the lookup-time
`CacheKey` instead of recomputing with a fresh `current_version()` read).
The sweep's claims hold identically under HEAD and under that change — it
alters which version the store lands under, not any counter or header
behavior the assertions read — and it belongs to its own bead, not this one.
