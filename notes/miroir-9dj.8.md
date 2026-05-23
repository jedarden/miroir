# P2.8 Middleware: structured logging + prometheus metrics + request IDs

## Verification Summary

This bead verified that the middleware implementation in `crates/miroir-proxy/src/middleware.rs` already satisfies all acceptance criteria for P2.8.

## Acceptance Criteria Status

### 1. Request ID generation (UUIDv7 prefix short-hashed) ✅
- `RequestId::new()` generates 8-character hex IDs from UUIDv7
- `request_id_middleware` sets `X-Request-Id` header on every response
- Verified: Response headers contain 8-char hex request IDs

### 2. Structured JSON log per plan §10 shape ✅
- `telemetry_middleware` creates structured logs with tracing-subscriber JSON formatter
- Log fields: timestamp, level, message, duration_ms, request_id, pod_id, method, path_template, status
- Verified: `jq` successfully parses all log lines

### 3. Prometheus metrics ✅
All required metrics are defined in the `Metrics` struct:
- `miroir_request_duration_seconds{method, path_template, status}` - Histogram
- `miroir_requests_total{method, path_template, status}` - Counter
- `miroir_requests_in_flight` - Gauge
- Scatter metrics: `miroir_scatter_fan_out_size`, `miroir_scatter_partial_responses_total`, `miroir_scatter_retries_total`
- Node metrics: `miroir_node_healthy`, `miroir_node_request_duration_seconds`, `miroir_node_errors_total`

### 4. Metrics server on :9090 ✅
- `metrics_router()` provides `/metrics` endpoint
- Separate axum listener on port 9090 in `main.rs`
- Verified: `curl localhost:9090/metrics` returns all metrics

### 5. High-cardinality defense: path_template ✅
- `extract_path_template()` uses `MatchedPath` extractor
- Labels use template (e.g., `/indexes/{uid}/search`) instead of actual UIDs
- Verified: Metrics show `path_template="/health"` not actual paths

### 6. In-flight gauge with Drop guard ✅
- `InFlightGuard` increments on request start, decrements via `Drop`
- Ensures accurate count even if handler panics
- Verified: `miroir_requests_in_flight` metric exists and works

## Test Coverage

All tests pass:
- `p7_1_core_metrics.rs` - 5 tests passing
- `p7_5_structured_logging.rs` - 17 tests passing
- Unit tests in `middleware.rs` - all passing

## Manual Verification

```bash
# Started proxy, made request to /health
curl -D - localhost:7700/health
# Response header: x-request-id: b65bfa06

# Checked metrics endpoint
curl localhost:9090/metrics | grep miroir_request_duration
# Shows samples with method=GET,path_template=/health,status=200

# Verified log format
tail -1 /tmp/proxy.log | jq '.'
# Parses successfully, shows request_id matches response header
```

## Conclusion

No code changes were required. The P2.8 middleware implementation was already complete in the codebase.
