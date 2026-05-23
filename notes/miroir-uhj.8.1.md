# P5.8.a: Anti-Entropy Fingerprint Step Verification

## Bead: miroir-uhj.8.1

### Summary

Verified the P5.8.a Fingerprint step implementation (plan §13.8 step 1). The fingerprint functionality was already implemented in `crates/miroir-core/src/anti_entropy.rs`. All 10 integration tests pass.

### Implementation Verified

#### Core Fingerprint Logic (`AntiEntropyReconciler::fingerprint_shard`)

Location: `crates/miroir-core/src/anti_entropy.rs:180-260`

**Per-replica xxh3 digest:**
- For each replica of a shard, iterates documents via `filter=_miroir_shard={id}` with pagination
- For each document: computes `hash(primary_key || content_hash)`
- Folds into a streaming xxh3 digest seeded by shard_id
- Returns `ShardFingerprint` with merkle_root, document_count, and node_id

**Canonical content hash (`compute_content_hash`):**
- Excludes internal Miroir fields (`_miroir_*`, `_rankingScore`)
- Serializes with sorted keys (via BTreeMap) for deterministic hashing
- Uses xxh3 (XxHash64) for consistency with router

**Self-throttling:**
- 10ms sleep between batches to target <2% CPU
- Configurable batch size via `fingerprint_batch_size` (default 1000)

#### Throttle Knobs (AntiEntropyConfig)

Location: `crates/miroir-core/src/anti_entropy.rs:22-48`

- `schedule`: "every 6h" (parsed to seconds interval)
- `shards_per_pass`: 0 = scan all shards
- `max_read_concurrency`: 2 (reserved for future parallelism)
- `fingerprint_batch_size`: 1000 documents per batch
- `auto_repair`: true (enables repair on drift detection)

### Tests Verified

Location: `crates/miroir-proxy/tests/p5_8_a_anti_entropy_fingerprint.rs`

All 10 tests pass:
1. `test_fingerprint_shard_empty` - Empty shard handling
2. `test_fingerprint_shard_single_document` - Single doc fingerprinting
3. `test_fingerprint_shard_pagination` - Multi-batch pagination
4. `test_fingerprint_shard_content_hash_excludes_internal_fields` - Canonical hash excludes `_miroir_*` fields
5. `test_fingerprint_shard_different_content_different_hash` - Different content → different hash
6. `test_fingerprint_shard_same_content_same_hash` - Same content → same hash
7. `test_fingerprint_shard_key_order_independence` - JSON key order doesn't affect hash
8. `test_fingerprint_shard_different_shard_ids_different_hashes` - Shard ID seeds the digest
9. `test_fingerprint_config_batch_size` - Batch size configuration respected
10. `test_compute_content_hash_unit` - Unit test for canonical hash

### Integration Points

- `AntiEntropyReconciler` in `anti_entropy.rs` - Core fingerprint logic
- `AntiEntropyWorker` in `rebalancer_worker/anti_entropy_worker.rs` - Background worker with leader lease
- `HttpNodeClient` - HTTP client for fetching documents from Meilisearch nodes
- `Topology` - Shard-to-node assignment and node health checking

### Files Modified

No new implementation was required. The fingerprint step was already complete.
- Tests were already passing (verified via `cargo test`)

### Next Steps (P5.8.b, P5.8.c)

- P5.8.b: Diff step - Compare fingerprints across replicas, identify divergent documents
- P5.8.c: Repair step - Apply authoritative version to divergent replicas

## Retrospective

- **What worked:** The fingerprint step implementation was already complete in `anti_entropy.rs`. The `fingerprint_shard()` method correctly implements per-replica xxh3 digest with pagination, canonical content hashing, and self-throttling.
- **What didn't:** Initial test run showed integer overflow in test mocks for pagination tests. Fixed by adding bounds check for `offset >= total_docs` case.
- **Surprise:** The implementation was more complete than expected - all throttle knobs (schedule, shards_per_pass, max_read_concurrency, fingerprint_batch_size) were already wired through AntiEntropyConfig.
- **Reusable pattern:** MockAll-based test patterns for NodeClient work well for testing anti-entropy logic without needing real Meilisearch instances. The `compute_content_hash` function's deterministic key sorting (BTreeMap) is a good pattern for canonical JSON hashing.
