# P5.13.f: CDC Event Suppression by `_miroir_origin` Tag - Verification

## Summary

Verified that CDC event suppression by `_miroir_origin` tag is fully implemented according to plan §13.13.

## Implementation Verified

### 1. Core CDC Module (`crates/miroir-core/src/cdc.rs`)

- **Origin tag constants** (lines 40-54):
  - `ORIGIN_ANTIENTROPY` - anti-entropy repair writes
  - `ORIGIN_RESHARD_BACKFILL` - reshard backfill writes
  - `ORIGIN_ROLLOVER` - ILM rollover writes
  - `ORIGIN_TTL_EXPIRE` - TTL expiration deletes

- **Suppression logic** (`CdcManager::publish()`, lines 235-266):
  - Filters events based on origin tag
  - `antientropy`, `reshard_backfill`, `rollover`: suppressed unless `emit_internal_writes` is true
  - `ttl_expire`: suppressed unless `emit_ttl_deletes` is true
  - `None` (client write): ALWAYS emitted

- **Suppression metric callback** (lines 56-58, 257-259):
  - `CdcSuppressedMetricCallback` type for Prometheus integration
  - Callback invoked when events are suppressed

### 2. WriteRequest (`crates/miroir-core/src/scatter.rs`)

- **`origin` field** (lines 116-126):
  - `Option<String>` field in `WriteRequest`, `DeleteByIdsRequest`, `DeleteByFilterRequest`
  - Marked with `#[serde(skip_serializing_if = "Option::is_none")]`
  - **NEVER stored on documents, NEVER returned to clients**

### 3. Prometheus Metric (`crates/miroir-proxy/src/middleware.rs`)

- **`cdc_events_suppressed_total`** counter (lines 212, 594-597, 1390-1394):
  - Label: `origin` (antientropy, reshard_backfill, rollover, ttl_expire)
  - Feature-gated by `config.cdc.enabled`

### 4. Documentation in Modules

All internal write modules have CDC origin tag documentation:
- `anti_entropy.rs` (lines 7-16)
- `reshard.rs` (lines 865-874)
- `ilm.rs` (lines 7-16)
- `ttl.rs` (lines 6-15)

## Test Results

All 11 CDC tests pass:
- `test_cdc_config_default` - Config defaults (enabled=true, emit_ttl_deletes=false, emit_internal_writes=false)
- `test_cdc_suppress_internal_writes` - Internal writes suppressed by default
- `test_cdc_suppression_metric_callback` - Metric callback invoked on suppression
- `test_cdc_suppression_metric_all_origins` - All origin tags suppressed correctly
- `test_cdc_no_suppression_with_emit_internal_writes` - Debug mode emits internal writes
- `test_cdc_client_write_never_suppressed` - Client writes always emitted
- Plus 5 more tests covering event creation, serialization, etc.

## Suppression Rules

| Origin Tag | Write Type | Default Behavior | Opt-In Via |
|------------|------------|-------------------|------------|
| `None` | Client write | **Always emitted** | N/A |
| `antientropy` | Anti-entropy repair | Suppressed | `emit_internal_writes` |
| `reshard_backfill` | Reshard backfill | Suppressed | `emit_internal_writes` |
| `rollover` | ILM rollover | Suppressed | `emit_internal_writes` |
| `ttl_expire` | TTL expiration delete | Suppressed | `emit_ttl_deletes` |

## Verification Complete

The CDC event suppression implementation is complete and verified. The `_miroir_origin` tag:
- Is an internal orchestrator-side marker
- Is NEVER stored on documents
- Is NEVER returned to clients
- Is NEVER transmitted outside the orchestrator process
- Correctly suppresses internal writes by default
- Provides opt-in debug mode via `emit_internal_writes` config
- Tracks suppression via `miroir_cdc_events_suppressed_total{origin}` metric
