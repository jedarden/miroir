# P2.8 Middleware Verification

## Acceptance Criteria Verified

### 1. Metrics endpoint returns all listed metrics
- Request metrics: `miroir_request_duration_seconds{method,path_template,status}` ✓
- Request counter: `miroir_requests_total{method,path_template,status}` ✓
- In-flight gauge: `miroir_requests_in_flight` ✓
- Scatter metrics: `miroir_scatter_fan_out_size`, `miroir_scatter_partial_responses_total`, `miroir_scatter_retries_total` ✓
- Node metrics: `miroir_node_healthy`, `miroir_node_request_duration_seconds`, `miroir_node_errors_total` (defined, registered when nodes configured)

### 2. Log format is valid JSON
All log lines parseable by `jq` with structured fields:
- `timestamp`, `level`, `message`, `pod_id` (global)
- `request_id`, `duration_ms`, `status`, `method`, `path_template` (per-request)

### 3. Request ID propagation
- Response header: `x-request-id: c4deaedd` ✓
- Log entry: `"request_id": "c4deaedd"` ✓
- Generated via UUIDv7 with short-hashing (8 hex chars)

### 4. High-cardinality defense
- Metrics use `path_template="/health"` not actual path ✓
- Axum `MatchedPath` extractor provides route template

## Implementation Details

### Request ID Generation (middleware.rs:36-45)
- Uses `uuid::Uuid::now_v7()` for time-ordered uniqueness
- Hashes full UUID to produce 8-character hex ID
- Stored in request extensions for handler access

### Structured Logging (main.rs:246-286)
- `tracing-subscriber` with JSON formatter
- Global `pod_id` field via runtime span
- Per-request `request_id` via telemetry middleware span
- `target: "miroir.request"` for request logs

### Prometheus Metrics (middleware.rs:404-418)
- Separate `Metrics` struct with all required metrics
- `encode_metrics()` for text exposition format
- Metrics server on `:9090` (separate listener from main API)

### In-flight Tracking (middleware.rs:1118-1147)
- `InFlightGuard` increments on request start
- `Drop` trait ensures decrement even on panic

## Tests Passing
- All 13 middleware tests pass
- `test_metrics_creation` verifies all metrics are defined
- `test_request_id_middleware_*` tests verify header propagation
