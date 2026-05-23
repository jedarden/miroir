# P5.8 §13.8 Anti-entropy shard reconciler - Verification Summary

## Bead: miroir-uhj.8

### Implementation Status: COMPLETE ✓

The anti-entropy shard reconciler (plan §13.8) is fully implemented and tested.

### Core Components

1. **`crates/miroir-core/src/anti_entropy.rs`** - Core reconciler
   - `AntiEntropyReconciler` with fingerprint → diff → repair pipeline
   - `ShardFingerprint` for Merkle tree fingerprints with bucket hashes
   - `ReplicaDiff` for divergence detection
   - `RepairAction` and `RepairReason` for repair tracking
   - TTL interaction: expired documents are deleted from all replicas
   - Mode A scaling: each pod scans rendezvous-owned shards
   - Metrics callbacks for Prometheus integration

2. **`crates/miroir-core/src/rebalancer_worker/anti_entropy_worker.rs`** - Background worker
   - `AntiEntropyWorker` with leader election via advisory lock
   - `HttpNodeClient` for node communication
   - Schedule parsing ("every 6h" format)
   - Leader lease management with renewal
   - Metrics integration

3. **`crates/miroir-core/src/config/advanced.rs`** - Configuration
   - `AntiEntropyConfig` with all required fields
   - Defaults: `enabled: true`, `schedule: "every 6h"`, `auto_repair: true`
   - `updated_at_field` and `expires_at_field` for TTL interaction

4. **`crates/miroir-proxy/src/routes/documents.rs`** - Write path integration
   - `_miroir_updated_at` stamping when `anti_entropy.enabled: true`
   - Reserved field rejection when enabled

5. **`crates/miroir-proxy/src/middleware.rs`** - Prometheus metrics
   - `miroir_antientropy_shards_scanned_total`
   - `miroir_antientropy_mismatches_found_total`
   - `miroir_antientropy_docs_repaired_total`
   - `miroir_antientropy_last_scan_completed_seconds`

6. **`charts/miroir/templates/miroir-prometheusrule.yaml`** - Alert
   - `MiroirAntientropyMismatch`: fires when `increase(miroir_antientropy_mismatches_found_total[18h]) > 0`
   - Corresponds to 3 consecutive passes at default 6h schedule

### Acceptance Criteria Status

- [x] Induce divergence on 1 shard; reconciler detects within `schedule` interval and repairs
  - Test: `test_acceptance_1_detect_and_repair_divergence` ✓

- [x] Expired-doc test: stale write with older `updated_at` does NOT resurrect
  - Test: `test_acceptance_2_expired_doc_no_resurrection` ✓

- [x] CDC subscribers do NOT see anti-entropy writes (filtered by `_miroir_origin`)
  - Test: `test_acceptance_3_cdc_suppression` ✓

- [x] Mode A: 3 pods, each owns ~1/3 of shards; runs exactly once per shard cluster-wide
  - Test: `test_acceptance_4_mode_a_shard_partitioning` ✓

### Test Results

All 9 tests pass:
```
running 9 tests
test test_acceptance_1_detect_and_repair_divergence ... ok
test test_acceptance_2_expired_doc_no_resurrection ... ok
test test_acceptance_3_cdc_suppression ... ok
test test_acceptance_4_mode_a_shard_partitioning ... ok
test test_authoritative_doc_selection ... ok
test test_authoritative_doc_selection_tiebreak ... ok
test test_bucket_isolation ... ok
test test_content_hash_excludes_internal_fields ... ok
test test_metrics_tracking ... ok
```

### Verification Date

2025-05-23
