# Bead bf-ommg4: Consolidated Ceiling Property Test Coverage

## Summary

Successfully consolidated both `upfront_total_known=true` and `upfront_total_known=false` code paths into a comprehensive test framework with consistent assertions and documentation.

## What Was Implemented

### 1. Unified Assertion Framework
Created `assert_backfill_ceiling_property()` helper function that:
- Verifies every sampled ratio respects the ceiling: `ratio ≤ 1.0 + CEILING_EPSILON`
- Ensures final ratio reaches completion: `|final_ratio - 1.0| < CEILING_EPSILON`
- Uses `CEILING_EPSILON = 1e-9` for floating-point tolerance
- Provides clear error messages with path name for debugging

### 2. Legacy Path Test (`legacy_path_backfill_progress_never_exceeds_one`)
Tests the `upfront_total_known=false` path where:
- Denominator starts at 0 and accumulates incrementally via `incorporate_shard_total`
- Risk: numerator can accumulate documents not yet counted in denominator
- Ceiling assertion catches any ratio > 1.0 during incremental accumulation
- Uses `sample_legacy_multi_shard_backfill_progress()` for 4-shard sequence

**Why ceiling matters for legacy path:**
- Without upfront total, denominator grows per-shard
- If numerator advances before denominator incorporates that shard's total, temporary ratio > 1.0
- This would indicate we've processed more documents than we've accounted for

### 3. New Path Test (`new_path_backfill_progress_never_exceeds_one`)
Tests the `upfront_total_known=true` path where:
- Denominator is seeded upfront from source index stats
- `incorporate_shard_total` is a no-op to prevent double-counting
- Risk: bypassing the no-op would inflate denominator with per-shard totals
- Uses existing `sample_multi_shard_backfill_progress()` for 4-shard sequence

**Why ceiling matters for new path:**
- Denominator fixed upfront; per-shard totals must NOT be added
- If `incorporate_shard_total` were bypassed, denominator would double-count
- This would make ratio < 1.0 at completion (not > 1.0), but violates correctness contract

### 4. Consistency and Documentation
- Both tests use the same unified assertion framework
- Clear doc comments explain each path's ceiling rationale
- Epsilon handling documented (`CEILING_EPSILON = 1e-9`)
- Test names clearly indicate which path they test

## Test Results

All tests pass successfully:
```
running 2 tests
test reshard::executor::tests::new_path_backfill_progress_never_exceeds_one ... ok
test reshard::executor::tests::legacy_path_backfill_progress_never_exceeds_one ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured
```

## Acceptance Criteria Met

- [x] Both paths tested with consistent framework
- [x] Clear documentation of why each path must not exceed 1.0
- [x] Epsilon handling documented (CEILING_EPSILON = 1e-9)
- [x] cargo test passes for all variants
- [x] Test names clearly indicate which path they test

## Files Modified

- `crates/miroir-core/src/reshard/executor.rs` (lines 1829-2179)
  - Added comprehensive ceiling property coverage section (bf-ommg4)
  - Created unified assertion helper `assert_backfill_ceiling_property()`
  - Implemented `legacy_path_backfill_progress_never_exceeds_one` test
  - Implemented `new_path_backfill_progress_never_exceeds_one` test
  - Documented rationale for both paths

## Implementation Strategy

Rather than a single parameterized test, the implementation uses two separate tests with:
1. A shared assertion helper for consistency
2. Path-specific sampling functions for clarity
3. Detailed documentation of each path's unique risks

This approach maintains readability while ensuring both code paths are thoroughly tested and documented.
