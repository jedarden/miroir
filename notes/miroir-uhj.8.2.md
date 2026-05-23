# P5.8.b: Bucket-Granular Re-Digest for Anti-Entropy Diff Step

## Status: Already Implemented

P5.8.b (plan §13.8 step 2) was already fully implemented in `/home/coding/miroir/crates/miroir-core/src/anti_entropy.rs`.

## Implementation Details

### 1. Bucket Assignment (`bucket_for_primary_key()`, lines 171-175)
- Uses xxh3 hash of primary key with seed 0
- Modulo 256 to assign bucket (0-255)
- Each bucket isolates ~0.4% of PK space

### 2. Per-Bucket Hashing During Fingerprint (lines 224-226, 269-271, 284-287)
- Creates 256 separate hashers (one per bucket)
- Each document's hash is folded into both global digest AND its bucket digest
- Returns `ShardFingerprint` with `bucket_hashes: Vec<String>` (256 elements)

### 3. Divergent Bucket Detection (`diff_fingerprints()`, lines 307-335)
- Compares per-bucket hashes between replicas
- Returns list of divergent bucket IDs
- Falls back to treating all buckets as divergent if bucket_hashes not computed

### 4. Bucket-Specific PK Enumeration (`fetch_bucket_pks()`, lines 341-392)
- Fetches all documents in shard with pagination
- Filters to only documents in target bucket
- Returns map of PK → content_hash
- Uses 10ms throttling between batches

### 5. Bucket-Level Replica Comparison (`compare_bucket_replicas()`, lines 400-447)
- Fetches bucket PKs from both replicas
- Returns `ReplicaDiff` with:
  - `a_only_pks`: PKs only on replica A
  - `b_only_pks`: PKs only on replica B
  - `mismatched_pks`: PKs with different content hashes

### 6. Integration with Repair Flow (`repair_shard()`, lines 609-696)
- Uses `diff_fingerprints()` to find divergent buckets
- For each divergent bucket, calls `compare_bucket_replicas()`
- Currently only logs divergences (repair writes TODO: P5.8.c)

## Test Coverage

Comprehensive tests in `/home/coding/miroir/crates/miroir-proxy/tests/p5_8_b_anti_entropy_diff.rs`:
1. `test_bucket_for_primary_key_deterministic` - Verifies deterministic bucket assignment
2. `test_bucket_for_primary_key_distributes` - Verifies even distribution
3. `test_fingerprint_shard_includes_bucket_hashes` - Verifies per-bucket hash computation
4. `test_diff_fingerprints_identical` - Tests no divergence case
5. `test_diff_fingerprints_divergent_buckets` - Tests divergent bucket detection
6. `test_fetch_bucket_pks_filters_by_bucket` - Tests bucket filtering
7. `test_compare_bucket_replicas_no_divergence` - Tests identical buckets
8. `test_compare_bucket_replicas_a_only` - Tests PK only on replica A
9. `test_compare_bucket_replicas_b_only` - Tests PK only on replica B
10. `test_compare_bucket_replicas_mismatched_content` - Tests content hash mismatch
11. `test_diff_fingerprints_isolates_divergence` - Verifies ~0.4% isolation per bucket
12. `test_bucket_count_constant` - Verifies BUCKET_COUNT = 256

## Reusability for §13.1 Reshard Verify

The `bucket_for_primary_key()` function is public and documented for reuse in reshard verification (plan §13.1), where PK-keyed (not shard-keyed) bucketing is needed for cross-shard comparison.
