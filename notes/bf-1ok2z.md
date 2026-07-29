# bf-1ok2z: Backfill Ratio Ceiling Assertion - Complete

## Summary

The backfill ratio ceiling assertion work for bead bf-1ok2z is complete. All acceptance criteria have been met through existing test infrastructure that was implemented in related beads (bf-1rkrm, bf-2ki61, bf-ommg4).

## Acceptance Criteria Status

✅ **All criteria met:**

1. **Asserts every sampled backfill_progress() <= 1.0 + epsilon**
   - Implemented via `assert_backfill_ceiling_property()` helper (executor.rs:1896)
   - Uses `CEILING_EPSILON = 1e-9` for floating-point tolerance (executor.rs:1877)

2. **Covers upfront_total_known=true path**
   - Test: `new_path_backfill_progress_never_exceeds_one()` (executor.rs:2155)
   - Validates denominator is NOT inflated by per-shard totals
   - Covers executor.rs:665 branch (incorporate_shard_total short-circuit)

3. **Covers upfront_total_known=false legacy path**
   - Test: `legacy_path_backfill_progress_never_exceeds_one()` (executor.rs:2101)
   - Validates per-shard accumulation doesn't exceed 1.0
   - Covers executor.rs:671 branch (legacy denominator accumulation)

4. **cargo test passes**
   - Both tests pass: `legacy_path_backfill_progress_never_exceeds_one` and `new_path_backfill_progress_never_exceeds_one`
   - Verified with: `cargo test -p miroir-core -- backfill_progress_never_exceeds_one`

## Implementation Details

The ceiling assertion framework provides:

- **Unified assertion helper**: `assert_backfill_ceiling_property(ratios, path_name)`
  - Validates every ratio ≤ 1.0 + epsilon
  - Validates final ratio = 1.0 ± epsilon
  - Clear error messages with path name, shard index, and actual value

- **Two test functions** covering both code paths:
  - Legacy path: Uses `sample_legacy_multi_shard_backfill_progress()` (upfront_total_known=false)
  - New path: Uses `sample_multi_shard_backfill_progress()` (upfront_total_known=true)

- **Multi-shard sampling infrastructure**: Both tests sample backfill_progress() after each shard completion, creating realistic ratios across a 4-shard sequence (300+250+200+250 = 1000 docs)

## Related Work

This bead builds on:
- **bf-3q0yf** (closed): Child 1 - bookkeeping level test (non-zero + monotonic)
- **bf-34lmv** (closed): Multi-shard backfill progress sampling infrastructure
- **bf-2ki61** (closed): Legacy path ceiling assertion
- **bf-1rkrm** (closed): New path ceiling assertion  
- **bf-ommg4** (closed): Consolidation and unified framework

The work for bf-1ok2z's acceptance criteria was completed as part of these related beads, ensuring comprehensive coverage of the [0,1] ceiling property across both code paths.

## Verification

To verify the implementation:

```bash
cargo test -p miroir-core -- backfill_progress_never_exceeds_one
```

Expected output: Both tests pass (2 passed, 0 failed)
