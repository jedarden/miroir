# Dump Import Compatibility Matrix

## Overview

Miroir's streaming dump import (`mode: streaming`) reconstructs indexes by routing documents through the public API (`POST /indexes/{uid}/documents`) rather than sending the raw dump file to nodes. This approach enables horizontal scalability but cannot reproduce every possible dump variant.

This matrix identifies which dump variants are fully compatible with streaming mode, which require the broadcast fallback, and what workarounds exist.

## Streaming Mode Capabilities

Streaming mode can reconstruct:

| Component | How it's reconstructed | Notes |
|-----------|------------------------|-------|
| **Documents** | NDJSON parsed and routed via `POST /indexes/{uid}/documents` | Primary key extracted, shard calculated, `_miroir_shard` injected |
| **Index settings** | Two-phase settings broadcast (§13.5) via `PATCH /indexes/{uid}/settings` | Verified by hash comparison |
| **Primary key** | Set via `PUT /indexes/{uid}/settings primaryKey` | Applied before document streaming |
| **API keys** | Broadcast via `POST /keys` | Actions/indexes recreated from dump metadata |

## Compatibility Matrix

### Fully Compatible (Streaming Works)

| Meilisearch Version | Dump Variant | Streaming Works? | Notes |
|---------------------|--------------|------------------|-------|
| v1.0+ | Standard documents NDJSON | ✅ Yes | Core use case |
| v1.0+ | Index settings (ranking rules, synonyms, etc.) | ✅ Yes | Applied via two-phase broadcast |
| v1.0+ | Primary key configuration | ✅ Yes | Set before document ingest |
| v1.0+ | Custom API keys (actions, indexes) | ✅ Yes | Recreated via `POST /keys` |
| v1.5+ | Filterable/sortable attributes | ✅ Yes | Standard settings |
| v1.12+ | Dictionary settings | ✅ Yes | Standard settings |
| v1.19+ | Proximity precision settings | ✅ Yes | Standard settings |
| v1.26+ | Embedders (vector search) | ✅ Yes | Standard settings |
| v1.30+ | Faceting settings | ✅ Yes | Standard settings |
| v1.37+ | Pagination settings | ✅ Yes | Standard settings |

### Requires Broadcast Fallback

| Meilisearch Version | Dump Variant | Streaming Works? | Broadcast Needed? | Workaround |
|---------------------|--------------|------------------|-------------------|------------|
| Any | **Tasks history** | ❌ No | ✅ Yes | Tasks are transient; not critical for reconstruction. Use broadcast if task UID preservation is required. |
| Any | **Dumps with existing `_miroir_shard` field** | ⚠️ Conflict | ✅ Yes | **Conflict**: Miroir injects its own `_miroir_shard`. If the dump already contains this field from a previous Miroir instance, the injected value conflicts. |
| < v1.0 | **Pre-v1.0 dump format** | ⚠️ Maybe | ✅ Yes | Old dump formats may have incompatible NDJSON structure. Use Meilisearch to upgrade dumps first: restore to vanilla Meilisearch, create new dump. |
| Any | **Internal LMDB state** | ❌ No | ✅ Yes | Streaming reconstructs at API level; internal LMDB state (e.g., cache warming) is not reproducible. Not functionally significant. |
| Any | **Snapshot-based dumps (`.ms.snapshot`)** | ❌ No | ✅ Yes | Snapshots are binary LMDB copies, not NDJSON. Convert to dump first via Meilisearch: `POST /dumps`, then import. |
| Any | **Enterprise edition features (sharding, replication)** | ❌ No | ✅ Yes | EE-only dump metadata cannot be reconstructed via CE API. Use broadcast or downgrade to CE dump first. |
| v1.0 - v1.2 | **Old-style settings format** | ⚠️ Maybe | ✅ Yes | Early Meilisearch settings may have changed. Test with a small dump first. |
| Any | **Large single-document payloads** | ⚠️ Risk | ✅ Yes | Documents exceeding `memory_buffer_bytes` may cause OOM. Broadcast has same limitation but fails more gracefully. |
| Any | **Corrupted or partial dumps** | ❌ No | ❌ No | Neither mode handles corruption. Repair source via Meilisearch `meilisearch --import-dump` with validation. |

### Version-Specific Notes

#### Meilisearch v1.37.0 (Current Target)

- **Sharding/Replication metadata**: EE-only features in dumps cannot be reconstructed via CE API
- **API key format**: Stable; fully reconstructible
- **Settings schema**: Stable; fully reconstructible

#### Meilisearch v1.19.0 - v1.36.x

- **No EE sharding metadata** in dumps from CE
- **All settings reconstructible** via public API

#### Meilisearch v1.0.0 - v1.18.x

- **Older dump formats**: NDJSON structure stable, but settings may have changed
- **Recommendation**: Test with small subset first

#### Meilisearch < v1.0.0

- **Not officially supported** for streaming import
- **Workaround**: Restore to vanilla Meilisearch, create v1.0+ dump

## Field Conflicts

### `_miroir_shard` Field Collision

**Problem**: Miroir injects `_miroir_shard` into every document for routing. If the dump already contains this field (from a previous Miroir instance or user data), there's a conflict.

**Detection**: Streaming import detects existing `_miroir_shard` field and:
1. Logs a warning
2. Falls back to broadcast mode automatically

**Workaround**: If you control the schema:
1. Rename the existing field before dump creation
2. Or use a custom `shard_field` config (future enhancement)

See enhancement bead: `miroir-zc2.6` (configurable shard metadata field)

## Decision Tree: Use Streaming or Broadcast?

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

## Configuration

Force broadcast mode for specific imports:

```yaml
# miroir-ctl dump import --mode broadcast --file products.dump --index products
```

Or in config:

```yaml
miroir:
  dump_import:
    mode: streaming          # Default: streaming
    fallback_on_conflict: true  # Auto-fallback to broadcast on _miroir_shard conflict
```

## Metrics and Observability

When streaming import falls back to broadcast, the following metrics are emitted:

- `miroir_dump_import_mode{mode="streaming"|"broadcast"}` (gauge)
- `miroir_dump_import_fallback_total{reason="conflict"|"unsupported"|"manual"}` (counter)
- `miroir_dump_import_conflict_field_detected_total{field}` (counter)

## CLI Output Reference

When `miroir-ctl dump import` uses broadcast fallback, it outputs:

```
⚠️  Falling back to broadcast mode
Reason: _miroir_shard field conflict detected
Impact: Transient 2× storage overhead during import
See: docs/dump-import/compatibility-matrix.md
```

## Related Documentation

- [Plan §13.9: Streaming routed dump import](../plan/plan.md#139-streaming-routed-dump-import)
- [Plan §13.5: Two-phase settings broadcast](../plan/plan.md#135-two-phase-settings-broadcast)
- [CLI: miroir-ctl dump import](../cli/reference.md#dump-import)

## Enhancement Tracking

| Issue | Description | Status |
|-------|-------------|--------|
| `miroir-zc2.6` | Configurable shard metadata field name | Open |
| `miroir-zc2.7` | Pre-import validation and field conflict detection | Open |
| `miroir-zc2.8` | EE-to-CE dump conversion tool | Open |
