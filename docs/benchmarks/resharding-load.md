# Resharding Load Benchmark Results

Plan §15 Open Problem #3: Empirical validation of the 2× transient load caveat.

## Summary

Online resharding (§13.1) creates a shadow index alongside the live index, backfills documents, verifies, then swaps aliases. The plan states that this "doubles transient storage and write load." This benchmark confirms that estimate using the actual routing code with synthetic corpora.

**Conclusion: Storage amplification is exactly 2.0× and dual-write amplification is exactly 2.0× across all tested scenarios.** However, peak write amplification (dual-write + backfill) depends heavily on the backfill throttle rate relative to the incoming write rate and can reach 500× or more if the backfill runs unthrottled against a low-write corpus.

## Test Matrix

| Doc size | Corpus | Write rate | RG | RF | Nodes/group | Old→New shards | Backfill throttle |
|----------|--------|------------|----|----|-------------|----------------|-------------------|
| 1 KB | 10 GB | 100 dps | 2 | 1 | 3 | 64→128 | 10,000 dps |
| 10 KB | 100 GB | 1,000 dps | 2 | 2 | 4 | 64→128 | 10,000 dps |
| 1 MB | 1 TB | 10 dps | 2 | 1 | 4 | 64→128 | 5,000 dps |

## Results

### Scenario 1: Small documents, moderate corpus

| Metric | Value |
|--------|-------|
| Total documents | 10,485,760 |
| Normal storage (steady-state) | 20.00 GB |
| Peak storage (resharding) | 40.00 GB |
| **Storage amplification** | **2.00×** |
| Per-node normal | 3.91 GB |
| Per-node peak | 8.05 GB |
| Normal write rate | 200 writes/sec |
| Dual-write rate | 400 writes/sec (2.0×) |
| Peak write rate (bf+dw) | 20,400 writes/sec (102.0×) |
| Backfill duration | 17.5 min |
| Total bytes written | 20.40 GB |
| Old shard CV | 0.21% |
| New shard CV | 0.33% |

### Scenario 2: Medium documents, large corpus, RF=2

| Metric | Value |
|--------|-------|
| Total documents | 10,485,760 |
| Normal storage (steady-state) | 200.00 GB |
| Peak storage (resharding) | 400.00 GB |
| **Storage amplification** | **2.00×** |
| Per-node normal | 62.51 GB |
| Per-node peak | 118.73 GB |
| Normal write rate | 4,000 writes/sec |
| Dual-write rate | 8,000 writes/sec (2.0×) |
| Peak write rate (bf+dw) | 48,000 writes/sec (12.0×) |
| Backfill duration | 17.5 min |
| Total bytes written | 480.00 GB |
| Old shard CV | 0.21% |
| New shard CV | 0.33% |

### Scenario 3: Large blobs, very large corpus

| Metric | Value |
|--------|-------|
| Total documents | 1,048,576 |
| Normal storage (steady-state) | 2,048.00 GB |
| Peak storage (resharding) | 4,096.00 GB |
| **Storage amplification** | **2.00×** |
| Per-node normal | 319.90 GB |
| Per-node peak | 647.67 GB |
| Normal write rate | 20 writes/sec |
| Dual-write rate | 40 writes/sec (2.0×) |
| Peak write rate (bf+dw) | 10,040 writes/sec (502.0×) |
| Backfill duration | 3.5 min |
| Total bytes written | 2,056.19 GB |
| Old shard CV | 0.66% |
| New shard CV | 1.04% |

## Invariant Verification

| Invariant | S1 | S2 | S3 |
|-----------|-----|-----|-----|
| Storage amplification == 2.0× | PASS (2.00) | PASS (2.00) | PASS (2.00) |
| Dual-write amplification == 2.0× | PASS (2.00) | PASS (2.00) | PASS (2.00) |
| Hash distribution CV < 5% | PASS (0.21%, 0.33%) | PASS (0.21%, 0.33%) | PASS (0.66%, 1.04%) |

## Operator Guidance

1. **Storage**: Reserve exactly 2× your corpus size (× RG) before starting a reshard. No exceptions.
2. **Write load during dual-write**: Expect exactly 2× your normal write throughput. Plan headroom accordingly.
3. **Peak write load**: The backfill throttle dominates peak write load. Set `throttle_docs_per_sec` conservatively — aim for peak total writes ≤ 3× normal. Formula: `backfill_throttle_dps × RF × RG + write_rate × 2 × RF × RG`.
4. **Backfill duration**: At 10K docs/sec, a 10M doc corpus takes ~17 minutes. Scale linearly.
5. **Schedule window**: Use `resharding.allowed_windows` to restrict resharding to off-peak hours. The CLI refuses to start outside configured windows unless `--force` is given.

## Methodology

The simulation builds a synthetic topology matching the test parameters, then iterates every document in the corpus, routing it through the real `shard_for_key` (XxHash64) and `assign_shard_in_group` (rendezvous hash) functions. Storage and write amplification are computed from the resulting shard assignments.

Run with: `cargo run --bin bench-reshard-load`

Last run: 2026-04-18
