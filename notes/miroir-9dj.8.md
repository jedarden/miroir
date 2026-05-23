# P2.8 Middleware: structured logging + prometheus metrics + request IDs

## Verification Summary

The middleware implementation in `crates/miroir-proxy/src/middleware.rs` is complete and meets all acceptance criteria:

### 1. Request ID Generation
- `RequestId::new()` generates UUIDv7-based 8-character hex IDs
- Uses `DefaultHasher` to hash full UUIDv7 for uniqueness even within same millisecond
- Implemented in `request_id_middleware` - adds `X-Request-Id` header to all responses

### 2. Structured JSON Logging (Plan §10)
- `telemetry_middleware` creates tracing span with: `request_id`, `pod_id`, `method`, `path_template`
- Log format matches plan §10: `timestamp`, `level`, `message`, `duration_ms`, `status`, `method`, `path_template`
- Logs go to stdout via `tracing-subscriber` with `fmt::layer().json()`
- All JSON log lines parse correctly with `jq`

### 3. Prometheus Metrics

**Request metrics:**
- `miroir_request_duration_seconds{method, path_template, status}` - histogram
- `miroir_requests_total{method, path_template, status}` - counter
- `miroir_requests_in_flight` - gauge with Drop guard

**Scatter metrics:**
- `miroir_scatter_fan_out_size` - histogram
- `miroir_scatter_partial_responses_total` - counter
- `miroir_scatter_retries_total` - counter

**Node metrics:**
- `miroir_node_healthy{node_id}` - gauge
- `miroir_node_request_duration_seconds{node_id, operation}` - histogram
- `miroir_node_errors_total{node_id, error_type}` - counter

### 4. Metrics Server on :9090
- `metrics_router()` creates axum router with `/metrics` endpoint
- Returns Prometheus text exposition format
- Separate listener from main API (bound to cluster network, no auth)

### 5. High-Cardinality Defense
- `extract_path_template()` uses `MatchedPath` extractor
- Returns route template (e.g., `/indexes/{uid}/search`) not actual path
- Prevents high-cardinality labels from OOMing Prometheus

## Test Results

- 13 middleware unit tests: PASSED
- 17 structured logging integration tests: PASSED

## Files Verified

- `crates/miroir-proxy/src/middleware.rs` - Complete implementation
- `crates/miroir-proxy/src/main.rs` - Middleware stack wiring
- `crates/miroir-proxy/Cargo.toml` - Dependencies (prometheus, uuid, tracing-subscriber)
- `crates/miroir-proxy/tests/p7_5_structured_logging.rs` - Integration tests
