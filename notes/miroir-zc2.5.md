# P12.OP5 (miroir-zc2.5): Dump Import Variants - Completion Summary

## Task

Plan §15 Open Problem #5: Enumerate what streaming mode can't handle for dump import.

## Findings

The compatibility matrix deliverable **already exists** and is comprehensive:
- **Location**: `docs/dump-import/compatibility-matrix.md`
- **Created**: In bead `bf-3gfw` (see `notes/bf-3gfw.md`)
- **Status**: Complete and production-ready

## Acceptance Criteria Verification

### ✅ 1. Matrix Published

The file `docs/dump-import/compatibility-matrix.md` exists with:
- Full compatibility matrix columns (Meilisearch Version, Dump Variant, Streaming Works?, Broadcast Needed?, Workaround)
- "Fully Compatible" section covering all standard dump variants
- "Requires Broadcast Fallback" section with 9 specific failure modes
- Version-specific notes for v1.37.0, v1.19-v1.36, v1.0-v1.18, and <v1.0

### ✅ 2. Workarounds and Enhancement Links

Each "broadcast needed" row has a workaround or enhancement bead link:

| Variant | Workaround | Enhancement Link |
|---------|-----------|------------------|
| Tasks history | Use broadcast if UID preservation needed | N/A (documented limitation) |
| `_miroir_shard` conflict | Rename field before dump | `miroir-zc2.6` |
| Pre-v1.0 format | Upgrade via vanilla Meilisearch | N/A |
| Internal LMDB state | Not functionally significant | N/A |
| Snapshot-based dumps | Convert to .dump first | N/A |
| EE features | Use broadcast or downgrade | `miroir-zc2.8` |
| Old settings (v1.0-v1.2) | Test with small dump | N/A |
| Large payloads | Use broadcast (fails gracefully) | N/A |
| Corrupted dumps | Repair via Meilisearch | N/A |

### ✅ 3. CLI Output References Matrix

The "CLI Output Reference" section documents the expected output format when falling back to broadcast:
```
⚠️  Falling back to broadcast mode
Reason: _miroir_shard field conflict detected
Impact: Transient 2× storage overhead during import
See: docs/dump-import/compatibility-matrix.md
```

## Failure Modes Addressed

All three potential failure modes from the task description are covered:

1. **Dumps from older Meilisearch versions with pre-v1.37 schema**
   - Covered by version-specific notes (v1.0-v1.18, v1.0-v1.2)
   - "Old-style settings format" row with workaround

2. **Dumps with custom keys (POST /keys) with indexes/actions not representable via public API**
   - Matrix confirms API keys are "fully reconstructible" for v1.37.0
   - Investigation confirms Meilisearch `POST /keys` API supports all valid key configurations
   - No edge cases requiring broadcast mode

3. **Dumps with snapshot-taken-mid-write where `_miroir_shard` conflicts**
   - Covered by "Dumps with existing `_miroir_shard` field" row
   - Field conflict detection documented with auto-fallback behavior

## Streaming Mode Limitations (Summary)

Streaming mode **cannot** reconstruct:
- Tasks history (transient data)
- Internal LMDB state (cache warming, etc.)
- Binary snapshot files (must convert to .dump)
- Enterprise edition metadata (sharding/replication)
- Dumps with `_miroir_shard` field conflicts

Streaming mode **can** reconstruct:
- All document data (NDJSON)
- All index settings (via two-phase broadcast)
- Primary key configuration
- Custom API keys (actions, indexes, expiration)
- All Meilisearch versions v1.0+

## Next Steps

The documentation is complete. The actual dump import implementation is tracked in:
- **Implementation bead**: `miroir-zc2.5` (this bead)
- **Enhancement beads**: `miroir-zc2.6`, `miroir-zc2.7`, `miroir-zc2.8`

The `miroir-ctl dump import` command currently returns "not yet implemented" with a reference to this bead. When implementing, use `docs/dump-import/compatibility-matrix.md` as the authoritative reference for compatibility decisions.

## References

- Compatibility Matrix: `docs/dump-import/compatibility-matrix.md`
- Plan §13.9: Streaming routed dump import
- Plan §13.5: Two-phase settings broadcast
- Prior work: `notes/bf-3gfw.md`
