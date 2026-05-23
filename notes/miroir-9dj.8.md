# P2.8 Middleware: Structured Logging + Prometheus Metrics + Request IDs - Verification

## Summary

This task verified that the P2.8 middleware implementation was already complete in the codebase.

## Acceptance Criteria Verification

### ✓ 1. `curl localhost:9090/metrics` returns all listed metrics

**Verified by**: `middleware.rs` lines 404-435 define all required metrics:
- `miroir_request_duration_seconds{method, path_template, status}` (HistogramVec)
- `miroir_requests_total{method, path_template, status}` (CounterVec)
- `miroir_requests_in_flight` (Gauge)
- `miroir_scatter_fan_out_size` (Histogram)
- `miroir_scatter_partial_responses_total` (Counter)
- `miroir_scatter_retries_total` (Counter)
- `miroir_node_healthy` (GaugeVec)
- `miroir_node_request_duration_seconds` (HistogramVec)
- `miroir_node_errors_total` (CounterVec)

**Metrics server**: `main.rs` lines 684, 701-703 set up metrics router on port 9090.

### ✓ 2. `jq` parses every log line without error

**Verified by**: `p7_5_structured_logging.rs` tests:
- `test_json_logs_parseable_by_jq` - confirms JSON parseability
- `test_request_id_format_in_logs` - confirms request_id in logs
- All 17 P7.5 tests pass

**Implementation**: `main.rs` lines 258-281 configure tracing-subscriber with JSON formatter.

### ✓ 3. Request ID appears in response header and log entry

**Verified by**: `p7_5_structured_logging.rs` tests:
- `test_request_id_response_header` - confirms X-Request-Id header
- `test_request_id_appears_in_all_log_lines_within_request` - confirms trace correlation

**Implementation**: `middleware.rs`:
- `request_id_middleware` (lines 93-122) generates UUIDv7-based 8-char hex IDs
- `telemetry_middleware` (lines 1167-1269) adds request_id to logs and headers

### ✓ 4. High-cardinality defense: path_template never contains UUID/UID

**Verified by**: `middleware.rs` lines 1149-1160 (`extract_path_template`):
Uses `axum::extract::MatchedPath` which provides route templates like `/indexes/{uid}/search` instead of actual paths like `/indexes/products/search`.

## Test Results

All tests pass:
- 13 middleware unit tests - All pass
- 17 P7.5 structured logging tests - All pass
- 5 P7.1 core metrics tests - All pass
- 135 total miroir-proxy unit tests - All pass

## Implementation Already Complete

The P2.8 middleware was already implemented. No code changes were required - this was a verification task.
