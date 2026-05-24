# OP#5: Dump Import Distribution - Bead bf-3gfw Summary

## Overview

This bead addresses Open Problem #5 (Dump import distribution) by cataloging all dump variants and documenting clear guidance for when to use streaming vs broadcast import modes.

## Work Completed

### 1. Compatibility Matrix Documentation

Created comprehensive compatibility matrix at `docs/dump-import/compatibility-matrix.md` that documents:

**Fully Compatible Variants** (Streaming works):
- Standard documents NDJSON (Meilisearch v1.0+)
- Index settings (ranking rules, synonyms, filterable/sortable attributes, etc.)
- Primary key configuration
- Custom API keys with actions/indexes
- All Meilisearch versions from v1.0 through v1.37+
- Dictionary, proximity precision, embedders, faceting, pagination settings

**Requires Broadcast Fallback**:
- Tasks history (transient, not critical)
- Dumps with existing `_miroir_shard` field (conflict)
- Pre-v1.0 dump formats
- Internal LMDB state (not functionally significant)
- Snapshot-based dumps (`.ms.snapshot`)
- Enterprise edition features (sharding, replication)
- Old-style settings formats (v1.0-v1.2)
- Large single-document payloads (OOM risk)

### 2. Decision Tree

Documented clear operator guidance:

```
Is the dump a standard Meilisearch .dump file?
├─ No → Not supported (convert to .dump first)
└─ Yes → Does it contain `_miroir_shard` field?
    ├─ Yes → Use broadcast (or rename field)
    └─ No → Is it from Meilisearch v1.0+?
        ├─ No → Test with small subset first (may work)
        └─ Yes → Does it require EE features?
            ├─ Yes → Use broadcast
            └─ No → Use streaming (recommended)
```

### 3. Field Conflict Documentation

Documented the `_miroir_shard` field collision issue:
- Detection mechanism
- Auto-fallback behavior
- Workaround options
- Links to enhancement bead `miroir-zc2.6` (configurable shard field)

### 4. Configuration Schema

Configuration is already in place (`DumpImportConfig` in `crates/miroir-core/src/config/advanced.rs`):
```yaml
dump_import:
  mode: streaming                  # streaming | broadcast (legacy)
  batch_size: 1000
  parallel_target_writes: 8
  memory_buffer_bytes: 134217728   # 128 MiB
  chunk_size_bytes: 268435456      # 256 MiB
```

### 5. Metrics and Observability

Documented metrics for tracking fallback behavior:
- `miroir_dump_import_mode{mode="streaming"|"broadcast"}`
- `miroir_dump_import_fallback_total{reason="conflict"|"unsupported"|"manual"}`
- `miroir_dump_import_conflict_field_detected_total{field}`

## Implementation Status

**Documentation**: ✅ Complete
**Implementation**: ⚠️ Not yet implemented (see bead `miroir-zc2.5`)

The CLI command `miroir-ctl dump import` currently returns a "not yet implemented" error message pointing to bead `miroir-zc2.5`.

## Success Criteria Assessment

| Criterion | Status | Notes |
|-----------|--------|-------|
| Complete matrix of dump variants and their supported import modes | ✅ Complete | See `docs/dump-import/compatibility-matrix.md` |
| Clear operator guidance on when to use each mode | ✅ Complete | Decision tree documented |
| Streaming mode handles all common production dump variants | ⚠️ Pending | Requires implementation and testing |

## Related Enhancements

The compatibility matrix documents several future enhancements tracked as child beads of `miroir-zc2`:

- `miroir-zc2.6`: Configurable shard metadata field name (addresses `_miroir_shard` conflicts)
- `miroir-zc2.7`: Pre-import validation and field conflict detection
- `miroir-zc2.8`: EE-to-CE dump conversion tool

## Recommendations

1. **For operators**: Use `docs/dump-import/compatibility-matrix.md` as the authoritative reference for dump import compatibility
2. **For implementation**: See bead `miroir-zc2.5` for actual dump import implementation tracking
3. **For testing**: Once implemented, test streaming import against each variant in the compatibility matrix

## References

- Plan §13.9: Streaming routed dump import
- Plan §13.5: Two-phase settings broadcast
- Parent epic: `miroir-zc2` (Phase 12 — Open Problems + Research)
