# P2.8 Middleware: Structured Logging + Prometheus Metrics + Request IDs

## Summary

Verified that the miroir-proxy middleware implementation meets all P2.8 acceptance criteria:

## Implementation Verified

### 1. Request ID Generation
- `RequestId::new()` generates 8-character hex IDs using UUIDv7 + hashing
- `request_id_middleware` injects `X-Request-Id` header on all responses
- Existing request IDs are preserved, invalid ones are replaced

### 2. Structured JSON Logging
- `tracing-subscriber` configured with JSON output in `main.rs`
- Log format follows plan §10: `timestamp`, `level`, `message`, `duration_ms`
- `request_id` appears in all log lines via span propagation
- Logs go to stdout, one JSON object per line

### 3. Prometheus Metrics
All required metrics implemented in `middleware.rs`:

**Request metrics:**
- `miroir_request_duration_seconds{method, path_template, status}` (Histogram)
- `miroir_requests_total{method, path_template, status}` (Counter)
- `miroir_requests_in_flight` (Gauge)

**Scatter metrics:**
- `miroir_scatter_fan_out_size` (Histogram)
- `miroir_scatter_partial_responses_total` (Counter)
- `miroir_scatter_retries_total` (Counter)

**Node metrics:**
- `miroir_node_healthy{node_id}` (Gauge)
- `miroir_node_request_duration_seconds{node_id, operation}` (Histogram)
- `miroir_node_errors_total{node_id, error_type}` (Counter)

### 4. Metrics Server
- Metrics server runs on `:9090` via `metrics_router()`
- `/metrics` endpoint returns Prometheus exposition format
- Separate listener from main API server

### 5. High-Cardinality Defense
- `extract_path_template()` uses Axum's `MatchedPath` extractor
- Path templates like `/indexes/{uid}/search` instead of actual UIDs
- Prevents Prometheus cardinality explosion from arbitrary IDs

## Acceptance Tests Passed

All 15 tests in `p2_8_middleware_acceptance_tests.rs` pass:
- `test_metrics_endpoint_returns_all_metrics` ✓
- `test_metrics_server_on_9090` ✓
- `test_log_lines_parse_as_json` ✓
- `test_log_format_matches_plan_section_10` ✓
- `test_request_id_in_response_header` ✓
- `test_request_id_propagates_from_request` ✓
- `test_path_template_prevents_high_cardinality` ✓
- `test_request_id_format` ✓
- `test_request_id_uniqueness` ✓
- `test_telemetry_middleware_updates_metrics` ✓
- `test_in_flight_gauge_increments_and_decrements` ✓
- `test_scatter_metrics_recorded` ✓
- `test_node_metrics_recorded` ✓
- `test_full_middleware_stack_integration` ✓
- `test_header_map_extensions` ✓

## Files Verified

- `crates/miroir-proxy/src/middleware.rs` - Complete middleware implementation
- `crates/miroir-proxy/src/main.rs` - Metrics server on :9090, JSON logging init
- `crates/miroir-proxy/tests/p2_8_middleware_acceptance_tests.rs` - All acceptance tests pass
- `crates/miroir-proxy/tests/p7_5_structured_logging.rs` - All structured logging tests pass
