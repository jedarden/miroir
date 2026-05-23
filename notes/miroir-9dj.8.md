# P2.8 Middleware: structured logging + prometheus metrics + request IDs

## Summary

Verified that the existing middleware implementation in `crates/miroir-proxy/src/middleware.rs` fully satisfies all acceptance criteria for P2.8.

## Implementation Status

### Request ID Generation ✓
- `request_id_middleware` generates RequestId using UUIDv7
- Hashes UUIDv7 to 8-character hex ID for compactness
- Adds `X-Request-Id` header to every response
- Preserves existing valid request IDs from client
- Replaces invalid request IDs with generated ones

### Structured JSON Logging ✓
- `telemetry_middleware` logs with tracing-subscriber JSON output
- Log format includes: timestamp, level, target, message, pod_id, request_id, duration_ms, status, method, path_template
- Search handler adds: index, node_count, estimated_hits, degraded
- `with_current_span(true)` ensures request_id propagates to all child events

### Prometheus Metrics ✓
All required metrics are implemented:
- `miroir_request_duration_seconds{method, path_template, status}` - HistogramVec
- `miroir_requests_total{method, path_template, status}` - CounterVec
- `miroir_requests_in_flight` - Gauge with InFlightGuard for proper decrement
- `miroir_scatter_fan_out_size` - Histogram
- `miroir_scatter_partial_responses_total` - Counter
- `miroir_scatter_retries_total` - Counter
- `miroir_node_healthy{node_id}` - GaugeVec
- `miroir_node_request_duration_seconds{node_id, operation}` - HistogramVec
- `miroir_node_errors_total{node_id, error_type}` - CounterVec

### Metrics Server ✓
- Separate axum listener on port 9090
- `/metrics` endpoint returns Prometheus exposition format
- No auth (bound to cluster network)

### High-Cardinality Defense ✓
- Uses Axum's `MatchedPath` extractor to get route template
- Example: `/indexes/{uid}/search` instead of `/indexes/products/search`
- Prevents high-cardinality labels from filling Prometheus

## Test Results

All tests passing:
- `test_all_core_metrics_registered` - Verifies all 18 core metrics are registered
- `test_json_logs_parseable_by_jq` - Verifies log lines are valid JSON
- `test_request_id_response_header` - Verifies X-Request-Id in responses
- `test_request_id_appears_in_all_log_lines_within_request` - Verifies trace correlation
- All 13 middleware lib tests passing
- All 17 structured logging integration tests passing
- All 5 core metrics tests passing

## Files Verified

- `crates/miroir-proxy/src/middleware.rs` - Core middleware implementation
- `crates/miroir-proxy/src/main.rs` - Metrics server setup on :9090
- `crates/miroir-proxy/src/routes/search.rs` - Search handler logging with plan §10 fields
- `crates/miroir-proxy/tests/p7_1_core_metrics.rs` - Core metrics acceptance tests
- `crates/miroir-proxy/tests/p7_5_structured_logging.rs` - Structured logging acceptance tests
