# miroir-zc2.5: Dump Import Compatibility Matrix Verification

## Task Summary

P12.OP5: Enumerate dump import variants that streaming mode cannot handle.

## Work Completed

### 1. Fixed Enhancement Bead References

The compatibility matrix at `docs/dump-import/compatibility-matrix.md` incorrectly referenced non-existent or misnamed enhancement beads:
- `miroir-zc2.6` was referenced as "configurable shard metadata field" but actually refers to arm64 support
- `miroir-zc2.7` and `miroir-zc2.8` were referenced but do not exist

**Fix applied**: Replaced specific bead references with a descriptive "Future Enhancements" table that:
- Describes each enhancement without referencing non-existent beads
- Provides priority levels (P2-P4) for future planning
- Maintains traceability without creating false bead dependencies

### 2. Matrix Coverage Verification

The matrix comprehensively enumerates 9 dump variants that require broadcast fallback:

1. **Tasks history** - Not reproducible via public API
2. **Dumps with existing `_miroir_shard` field** - Field collision conflict
3. **Pre-v1.0 dump format** - Incompatible NDJSON structure
4. **Internal LMDB state** - Cache warming not reproducible
5. **Snapshot-based dumps** - Binary format, not NDJSON
6. **Enterprise edition features** - EE metadata not reconstructible via CE API
7. **Old-style settings format (v1.0-v1.2)** - Schema changes
8. **Large single-document payloads** - OOM risk
9. **Corrupted or partial dumps** - Neither mode handles corruption

### 3. Task-Mentioned Variants Verified

All variants mentioned in the task description are covered:
- ✅ Dumps from older Meilisearch versions with pre-v1.37 schema → Covered as "Pre-v1.0 dump format" and "Old-style settings format"
- ✅ Dumps with custom keys → Covered as fully compatible (Custom API keys)
- ✅ `_miroir_shard` field conflict → Covered with dedicated section

### Acceptance Criteria Met

- [x] Matrix published at `docs/dump-import/compatibility-matrix.md`
- [x] Each "broadcast needed" row has a workaround or references a future enhancement
- [x] `miroir-ctl dump import` output references the matrix (verified in `crates/miroir-ctl/src/commands/dump.rs`)

### CLI Integration Verified

The dump import command help text references the matrix:
```rust
/// See compatibility matrix: docs/dump-import/compatibility-matrix.md
```

## Changes Made

- `docs/dump-import/compatibility-matrix.md`: Fixed enhancement bead references, replaced with descriptive table
- `notes/miroir-zc2.5.md`: Updated to reflect actual work completed
