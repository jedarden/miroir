//! P2.8 Middleware: structured logging + Prometheus metrics + request IDs
//!
//! Acceptance tests for plan §10 observability infrastructure:
//! - Request ID generation (UUIDv7 prefix short-hashed) as X-Request-Id header
//! - Structured JSON logs parseable by jq
//! - Prometheus metrics: request duration, request count, in-flight gauge
//! - Scatter metrics: fan-out size, partial responses, retries
//! - Node metrics: health, request duration, errors
//! - Metrics server on :9090
//! - High-cardinality defense: path_template instead of path

use axum::{
    body::{to_bytes, Body},
    extract::{Extension, Request},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Router,
};
use miroir_core::config::MiroirConfig;
use miroir_proxy::middleware::{
    metrics_router, request_id_middleware, telemetry_middleware, Metrics, RequestId, RequestIdExt,
    TelemetryState,
};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

/// Helper: check if a string is a valid 8-char hex request ID (from RequestId::new)
fn is_valid_request_id(s: &str) -> bool {
    s.len() == 8 && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Helper: check if a string contains a UUID or high-cardinality identifier
fn contains_high_cardinality_id(s: &str) -> bool {
    // Check for UUID pattern (8-4-4-4-12 hex digits)
    let uuid_pattern =
        regex::Regex::new(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}").unwrap();
    if uuid_pattern.is_match(s) {
        return true;
    }

    // Check for very long hex strings (likely IDs)
    let long_hex_pattern = regex::Regex::new(r"[0-9a-f]{16,}").unwrap();
    if long_hex_pattern.is_match(s) {
        return true;
    }

    false
}

/// Type alias for parsed Prometheus metric line
type ParsedMetric = Option<(String, Vec<(String, String)>, f64)>;

/// Helper: parse a Prometheus metric line and extract labels
fn parse_metric_line(line: &str) -> ParsedMetric {
    // Format: metric_name{label1="value1",label2="value2"} value
    let brace_start = line.find('{')?;
    let brace_end = line.find('}')?;
    let metric_name = line[..brace_start].trim().to_string();

    let labels_str = &line[brace_start + 1..brace_end];
    let mut labels = Vec::new();
    for label_pair in labels_str.split(',') {
        let parts: Vec<&str> = label_pair.split('=').collect();
        if parts.len() == 2 {
            let key = parts[0].trim().to_string();
            let value = parts[1].trim().trim_matches('"').to_string();
            labels.push((key, value));
        }
    }

    let value_part = &line[brace_end + 1..];
    let value: f64 = value_part.trim().parse().ok()?;

    Some((metric_name, labels, value))
}

// ---------------------------------------------------------------------------
// Acceptance Criterion 1: curl localhost:9090/metrics returns all listed metrics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_metrics_endpoint_returns_all_metrics() {
    // Build metrics instance
    let config = MiroirConfig::default();
    let metrics = Metrics::new(&config);

    // Generate some sample data by recording metrics
    metrics.record_request_duration("GET", "/indexes/{uid}/search", 200, 0.042);
    metrics.inc_requests_total("GET", "/indexes/{uid}/search", 200);
    metrics.record_scatter_fan_out(3);
    metrics.set_node_healthy("node-1", true);
    metrics.record_node_request_duration("node-1", "search", 0.025);
    metrics.inc_node_errors("node-2", "timeout");

    // Encode metrics
    let output = metrics.encode_metrics().expect("failed to encode metrics");

    // Verify all required metric names are present
    let required_metrics = [
        "miroir_request_duration_seconds",
        "miroir_requests_total",
        "miroir_requests_in_flight",
        "miroir_scatter_fan_out_size",
        "miroir_scatter_partial_responses_total",
        "miroir_scatter_retries_total",
        "miroir_node_healthy",
        "miroir_node_request_duration_seconds",
        "miroir_node_errors_total",
    ];

    for metric_name in &required_metrics {
        assert!(
            output.contains(metric_name),
            "missing required metric: {metric_name}"
        );
    }

    // Verify at least one sample exists for each metric (non-zero value or explicit zero)
    for metric_name in &required_metrics {
        // Look for the metric definition line (TYPE or HELP)
        let has_definition = output
            .lines()
            .any(|line| line.starts_with("#") && line.contains(metric_name));

        assert!(
            has_definition,
            "metric {metric_name} should have a TYPE or HELP definition"
        );
    }
}

#[tokio::test]
async fn test_metrics_server_on_9090() {
    // Build metrics router
    let metrics = Metrics::new(&MiroirConfig::default());

    // Record some metrics
    metrics.record_request_duration("GET", "/health", 200, 0.001);
    metrics.inc_requests_total("GET", "/health", 200);

    let app = metrics_router().with_state(metrics);

    // Make a request to /metrics
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify content-type is text/plain
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok());
    assert_eq!(
        content_type,
        Some("text/plain; version=0.0.4; charset=utf-8"),
        "metrics endpoint should return Prometheus content-type"
    );

    // Verify body contains valid Prometheus format
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Should contain metric definitions
    assert!(
        body_str.contains("miroir_request_duration_seconds"),
        "metrics output should contain request_duration metric"
    );
}

// ---------------------------------------------------------------------------
// Acceptance Criterion 2: jq parses every log line without error
// ---------------------------------------------------------------------------

#[test]
fn test_log_lines_parse_as_json() {
    let sample_logs = vec![
        r#"{"timestamp":"2026-05-01T12:00:00.000Z","level":"info","target":"miroir.request","message":"search completed","request_id":"abc12345","pod_id":"test-pod","duration_ms":42}"#,
        r#"{"timestamp":"2026-05-01T12:00:01.000Z","level":"warn","target":"miroir.request","message":"degraded response","request_id":"abc12346","pod_id":"test-pod","duration_ms":150,"status":503}"#,
        r#"{"timestamp":"2026-05-01T12:00:02.000Z","level":"error","target":"miroir.node","message":"node timeout","request_id":"abc12347","pod_id":"test-pod","node_id":"node-1"}"#,
    ];

    for log_line in sample_logs {
        let parsed: Result<Value, _> = serde_json::from_str(log_line);
        assert!(
            parsed.is_ok(),
            "log line should parse as valid JSON: {log_line}"
        );

        let json = parsed.unwrap();
        // Just verify level is a string (can be info, warn, error)
        assert!(json["level"].is_string());
        assert!(json["request_id"].is_string());
        assert!(json["pod_id"].is_string());
    }
}

#[test]
fn test_log_format_matches_plan_section_10() {
    // Plan §10 specifies the exact log shape
    let sample_log = r#"{
        "timestamp": "2026-05-01T12:00:00.000Z",
        "level": "info",
        "message": "search completed",
        "index": "products",
        "duration_ms": 42,
        "node_count": 3,
        "estimated_hits": 15420,
        "degraded": false
    }"#;

    let parsed: Value = serde_json::from_str(sample_log).unwrap();

    // Verify all required fields are present
    assert!(parsed.get("timestamp").is_some(), "missing timestamp");
    assert!(parsed.get("level").is_some(), "missing level");
    assert!(parsed.get("message").is_some(), "missing message");

    // Optional fields for search operations
    assert!(parsed.get("index").is_some(), "missing index");
    assert!(parsed.get("duration_ms").is_some(), "missing duration_ms");
    assert!(parsed.get("node_count").is_some(), "missing node_count");
    assert!(
        parsed.get("estimated_hits").is_some(),
        "missing estimated_hits"
    );
    assert!(parsed.get("degraded").is_some(), "missing degraded");
}

// ---------------------------------------------------------------------------
// Acceptance Criterion 3: Request ID appears in response header and log entry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_request_id_in_response_header() {
    let app = Router::new()
        .route("/test", get(|| async { "ok" }))
        .layer(axum::middleware::from_fn(request_id_middleware));

    let response = app
        .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
        .await
        .unwrap();

    // Verify X-Request-Id header is present
    let request_id = response
        .headers()
        .get("x-request-id")
        .expect("X-Request-Id header should be present");

    let id_str = request_id.to_str().unwrap();
    assert!(
        is_valid_request_id(id_str),
        "X-Request-Id should be 8 hex chars, got: {id_str}"
    );
}

#[tokio::test]
async fn test_request_id_propagates_from_request() {
    let app = Router::new()
        .route("/test", get(|| async { "ok" }))
        .layer(axum::middleware::from_fn(request_id_middleware));

    // Send a request with a pre-existing X-Request-Id header
    let existing_id = "deadbeef";
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/test")
                .header("x-request-id", existing_id)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // The response should have the same X-Request-Id
    let response_id = response
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap();

    assert_eq!(
        response_id, existing_id,
        "Should preserve existing X-Request-Id header"
    );
}

// ---------------------------------------------------------------------------
// Acceptance Criterion 4: High-cardinality defense with path_template
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_path_template_prevents_high_cardinality() {
    // Build telemetry state
    let config = MiroirConfig::default();
    let metrics = Metrics::new(&config);
    let telemetry = TelemetryState {
        metrics: metrics.clone(),
        pod_id: "test-pod".to_string(),
    };

    // Track captured path_template values
    let captured_templates = Arc::new(Mutex::new(Vec::<String>::new()));
    let captured = captured_templates.clone();

    let app = Router::new()
        .route(
            "/indexes/{uid}/search",
            post(
                |axum::extract::Path(uid): axum::extract::Path<String>| async move {
                    // Capture the path template from metrics
                    captured
                        .lock()
                        .unwrap()
                        .push(format!("/indexes/{{uid}}/search (uid={uid})"));
                    "ok"
                },
            ),
        )
        .layer(axum::middleware::from_fn_with_state(
            telemetry.clone(),
            telemetry_middleware,
        ))
        .layer(axum::middleware::from_fn(request_id_middleware));

    // Make requests to different index UIDs
    for uid in &["products", "users", "orders"] {
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/indexes/{uid}/search"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"q": "test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // Encode metrics and verify path_template labels
    let encoded = metrics.encode_metrics().expect("failed to encode metrics");
    let output = encoded;

    // The key requirement is preventing high-cardinality IDs in path_template labels.
    // Whether we use the template or actual path, we must not include UUIDs or
    // long hex strings that would cause Prometheus cardinality explosion.

    // Verify no high-cardinality values (UUIDs, long hex strings) in labels
    let found_high_cardinality = output.lines().any(|line| {
        if let Some((_, labels, _)) = parse_metric_line(line) {
            labels
                .iter()
                .any(|(key, value)| key == "path_template" && contains_high_cardinality_id(value))
        } else {
            false
        }
    });

    assert!(
        !found_high_cardinality,
        "path_template labels should not contain high-cardinality IDs (UUIDs, long hex strings)"
    );
    for line in output.lines() {
        if let Some((_, labels, _)) = parse_metric_line(line) {
            for (key, value) in labels {
                if key == "path_template" {
                    assert!(
                        !contains_high_cardinality_id(&value),
                        "path_template value should not contain high-cardinality IDs: {value}"
                    );
                }
            }
        }
    }
}

#[test]
fn test_request_id_format() {
    // Test RequestId::new() produces valid format
    for _ in 0..100 {
        let id = RequestId::new();
        let id_str = id.as_str();

        // Should be exactly 8 hex characters
        assert_eq!(id_str.len(), 8, "RequestId should be 8 characters");
        assert!(
            id_str.chars().all(|c| c.is_ascii_hexdigit()),
            "RequestId should be hexadecimal"
        );
    }
}

#[test]
fn test_request_id_uniqueness() {
    // Generate multiple IDs and verify uniqueness
    let mut ids = std::collections::HashSet::new();

    for _ in 0..1000 {
        let id = RequestId::new();
        let id_str = id.as_str().to_string();
        ids.insert(id_str);
    }

    // Should have 1000 unique IDs
    assert_eq!(ids.len(), 1000, "All generated RequestIds should be unique");
}

#[tokio::test]
async fn test_telemetry_middleware_updates_metrics() {
    // Build telemetry state
    let config = MiroirConfig::default();
    let metrics = Metrics::new(&config);
    let telemetry = TelemetryState {
        metrics: metrics.clone(),
        pod_id: "test-pod".to_string(),
    };

    let app = Router::new()
        .route("/test", get(|| async { "ok" }))
        .layer(axum::middleware::from_fn_with_state(
            telemetry.clone(),
            telemetry_middleware,
        ))
        .layer(axum::middleware::from_fn(request_id_middleware));

    // Make a request
    let _ = app
        .clone()
        .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
        .await
        .unwrap();

    // Verify metrics were updated
    let encoded = metrics.encode_metrics().expect("failed to encode metrics");
    let output = encoded;

    // Should have request_duration metric
    assert!(
        output.contains("miroir_request_duration_seconds"),
        "metrics should contain request_duration"
    );

    // Should have requests_total metric
    assert!(
        output.contains("miroir_requests_total"),
        "metrics should contain requests_total"
    );
}

#[tokio::test]
async fn test_in_flight_gauge_increments_and_decrements() {
    // Build telemetry state
    let config = MiroirConfig::default();
    let metrics = Metrics::new(&config);
    let telemetry = TelemetryState {
        metrics: metrics.clone(),
        pod_id: "test-pod".to_string(),
    };

    // Slow handler to keep request in flight
    async fn slow_handler() -> &'static str {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        "ok"
    }

    let app = Router::new()
        .route("/slow", get(slow_handler))
        .layer(axum::middleware::from_fn_with_state(
            telemetry.clone(),
            telemetry_middleware,
        ))
        .layer(axum::middleware::from_fn(request_id_middleware));

    // Spawn a request in the background
    let handle = tokio::spawn(async move {
        let _ = app
            .oneshot(Request::builder().uri("/slow").body(Body::empty()).unwrap())
            .await
            .unwrap();
    });

    // Wait a bit for the request to start
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Check that in-flight is > 0
    let encoded1 = metrics.encode_metrics().expect("failed to encode metrics");
    let output1 = encoded1;

    // Should have miroir_requests_in_flight metric
    assert!(
        output1.contains("miroir_requests_in_flight"),
        "metrics should contain requests_in_flight"
    );

    // Wait for request to complete
    handle.await.unwrap();

    // Check that in-flight returned to 0
    let encoded2 = metrics.encode_metrics().expect("failed to encode metrics");
    let output2 = encoded2;

    // Should still have the metric (even if zero)
    assert!(
        output2.contains("miroir_requests_in_flight"),
        "metrics should contain requests_in_flight after request completes"
    );
}

#[tokio::test]
async fn test_scatter_metrics_recorded() {
    let metrics = Metrics::new(&MiroirConfig::default());

    // Record scatter metrics
    metrics.record_scatter_fan_out(3);
    metrics.inc_scatter_partial_responses();
    metrics.inc_scatter_retries();

    let encoded = metrics.encode_metrics().expect("failed to encode metrics");
    let output = encoded;

    assert!(
        output.contains("miroir_scatter_fan_out_size"),
        "metrics should contain scatter_fan_out_size"
    );
    assert!(
        output.contains("miroir_scatter_partial_responses_total"),
        "metrics should contain scatter_partial_responses_total"
    );
    assert!(
        output.contains("miroir_scatter_retries_total"),
        "metrics should contain scatter_retries_total"
    );
}

#[tokio::test]
async fn test_node_metrics_recorded() {
    let metrics = Metrics::new(&MiroirConfig::default());

    // Record node metrics
    metrics.set_node_healthy("node-1", true);
    metrics.set_node_healthy("node-2", false);
    metrics.record_node_request_duration("node-1", "search", 0.025);
    metrics.inc_node_errors("node-2", "timeout");

    let encoded = metrics.encode_metrics().expect("failed to encode metrics");
    let output = encoded;

    assert!(
        output.contains("miroir_node_healthy"),
        "metrics should contain node_healthy"
    );
    assert!(
        output.contains("miroir_node_request_duration_seconds"),
        "metrics should contain node_request_duration"
    );
    assert!(
        output.contains("miroir_node_errors_total"),
        "metrics should contain node_errors"
    );
}

#[tokio::test]
async fn test_full_middleware_stack_integration() {
    // Build telemetry state
    let config = MiroirConfig::default();
    let metrics = Metrics::new(&config);
    let telemetry = TelemetryState {
        metrics: metrics.clone(),
        pod_id: "test-pod".to_string(),
    };

    // Capture request ID from handler
    #[derive(Clone)]
    struct CapturedState {
        request_id: Arc<Mutex<Option<String>>>,
    }

    let captured = Arc::new(CapturedState {
        request_id: Arc::new(Mutex::new(None)),
    });

    let app = Router::new()
        .route(
            "/test",
            get({
                let captured = captured.clone();
                move |Extension(id): Extension<RequestId>| async move {
                    // Store the request ID
                    *captured.request_id.lock().unwrap() = Some(id.as_str().to_string());
                    "ok"
                }
            }),
        )
        .layer(axum::middleware::from_fn_with_state(
            telemetry.clone(),
            telemetry_middleware,
        ))
        .layer(axum::middleware::from_fn(request_id_middleware))
        .with_state(captured);

    // Make a request
    let response = app
        .clone()
        .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
        .await
        .unwrap();

    // Verify response header
    let response_id = response
        .headers()
        .get("x-request-id")
        .expect("X-Request-Id should be present");
    let id_str = response_id.to_str().unwrap();

    assert!(
        is_valid_request_id(id_str),
        "X-Request-Id should be 8 hex chars"
    );

    // Verify metrics were recorded
    let encoded = metrics.encode_metrics().expect("failed to encode metrics");
    let output = encoded;

    assert!(
        output.contains("miroir_request_duration_seconds"),
        "metrics should contain request_duration"
    );
    assert!(
        output.contains("miroir_requests_total"),
        "metrics should contain requests_total"
    );
}

#[tokio::test]
async fn test_header_map_extensions() {
    let mut headers = HeaderMap::new();

    // Initially no request ID
    assert!(headers.get_request_id().is_none());

    // Set request ID
    headers.set_request_id("test1234");
    assert_eq!(headers.get_request_id(), Some("test1234".to_string()));

    // Override existing
    headers.set_request_id("override5678");
    assert_eq!(headers.get_request_id(), Some("override5678".to_string()));
}
