# P5.14 §13.14 TTL + Automatic Expiration — Implementation Summary

## Status: COMPLETE ✓

All acceptance criteria have been verified. The TTL implementation was already in place and all tests pass.

## Implementation Overview

### Core Components

1. **TtlManager** (`crates/miroir-core/src/ttl.rs`)
   - Background sweeper with configurable interval (default 300s)
   - Max deletes per sweep limit (default 10,000)
   - Per-index overrides supported
   - Metrics callbacks for Prometheus integration

2. **TtlWorker** (`crates/miroir-core/src/rebalancer_worker/ttl_worker.rs`)
   - Leader election via TaskStore
   - Single leader across cluster (scope: "ttl_sweeper")
   - Lease TTL: 10s with 2s renewal interval

3. **Admin API** (`crates/miroir-proxy/src/routes/admin_endpoints.rs`)
   - `POST /_miroir/indexes/{uid}/ttl-policy` — Set TTL policy override
   - `GET /_miroir/indexes/{uid}/ttl-policy` — Get TTL policy
   - Supports `sweep_interval_s`, `max_deletes_per_sweep`, `enabled`

4. **Reserved Field** (`crates/miroir-proxy/src/routes/documents.rs`)
   - `_miroir_expires_at` (integer unix ms) reserved when TTL enabled
   - Automatically added to `filterableAttributes` at index creation

5. **Field Stripping** (`crates/miroir-proxy/src/routes/search.rs`)
   - `_miroir_expires_at` stripped from search hits
   - `_miroir_shard` also stripped (plan §5)

6. **Anti-Entropy Integration** (`crates/miroir-core/src/anti_entropy.rs`)
   - Expired documents skipped during fingerprinting
   - Prevents zombie resurrection on AE pass (plan §13.14 interaction)

7. **CDC Integration** (`crates/miroir-core/src/cdc.rs`)
   - TTL deletes tagged with `origin="ttl_expire"`
   - Suppressed by default unless `emit_ttl_deletes: true`

## Acceptance Criteria Status

| Criterion | Status | Notes |
|-----------|--------|-------|
| Doc with `_miroir_expires_at = now - 1000` is gone after one sweep cycle | ✓ | Test: `test_expired_document_deleted_after_sweep` |
| TTL sweep + late straggler write: zombie doc does NOT reappear after AE pass | ✓ | Test: `test_anti_entropy_skips_expired_documents` |
| CDC subscribers see TTL deletes only when `emit_ttl_deletes: true` | ✓ | Tests: `test_cdc_ttl_delete_suppressed_by_default`, `test_cdc_ttl_delete_emitted_when_enabled` |
| `_miroir_expires_at` stripped from search hits | ✓ | Test: `test_expires_at_stripped_from_search_hits` |
| 10k-doc sweep respects `max_deletes_per_sweep` (doesn't exceed) | ✓ | Test: `test_max_deletes_per_sweep_limit` |

## Test Results

All 8 TTL tests pass:
```
running 8 tests
test test_anti_entropy_skips_expired_documents ... ok
test test_cdc_ttl_delete_emitted_when_enabled ... ok
test test_expired_document_deleted_after_sweep ... ok
test test_cdc_ttl_delete_suppressed_by_default ... ok
test test_expires_at_added_to_filterable_attributes ... ok
test test_expires_at_stripped_from_search_hits ... ok
test test_max_deletes_per_sweep_limit ... ok
test test_ttl_metrics_integration ... ok

test result: ok. 8 passed; 0 failed
```

## Sweep Algorithm (Mode A — plan §14.6)

Each pod sweeps only its rendezvous-owned shards:
```
for each owned shard s:
    filter = "_miroir_shard = {s} AND _miroir_expires_at <= {now_ms}"
    DELETE with origin="ttl_expire"
```

This ensures no duplicate deletes across pods.

## Configuration

```yaml
ttl:
  enabled: true
  sweep_interval_s: 300
  max_deletes_per_sweep: 10000
  expires_at_field: _miroir_expires_at
  per_index_overrides: {}
```

## Metrics

- `miroir_ttl_documents_expired_total{index}` — Documents deleted by TTL
- `miroir_ttl_sweep_duration_seconds{index}` — Sweep duration
- `miroir_ttl_pending_estimate{index}` — Estimated pending expired documents
