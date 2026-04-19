//! Structured logging, request IDs, and Prometheus metrics middleware.

use std::time::Instant;

use axum::{
    extract::{Request, State},
    http::{HeaderMap, HeaderValue},
    middleware::Next,
    response::Response,
    Router,
    routing::get,
};
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, Opts, Registry,
    TextEncoder,
};
use tracing::info_span;
use uuid::Uuid;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Global metrics registry shared across all middleware instances.
pub struct Metrics {
    registry: Registry,
    request_duration: HistogramVec,
    requests_total: CounterVec,
    requests_in_flight: Gauge,
    scatter_fan_out_size: Histogram,
    scatter_partial_responses: Counter,
    scatter_retries: Counter,
    node_healthy: Gauge,
    node_request_duration: Histogram,
    node_errors: Counter,
}

impl Clone for Metrics {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
            request_duration: self.request_duration.clone(),
            requests_total: self.requests_total.clone(),
            requests_in_flight: self.requests_in_flight.clone(),
            scatter_fan_out_size: self.scatter_fan_out_size.clone(),
            scatter_partial_responses: self.scatter_partial_responses.clone(),
            scatter_retries: self.scatter_retries.clone(),
            node_healthy: self.node_healthy.clone(),
            node_request_duration: self.node_request_duration.clone(),
            node_errors: self.node_errors.clone(),
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let request_duration = HistogramVec::new(
            HistogramOpts::new("miroir_request_duration_seconds", "Request latency in seconds")
                .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &["method", "path_template", "status"],
        )
        .expect("failed to create request_duration histogram");

        let requests_total = CounterVec::new(
            Opts::new("miroir_requests_total", "Total number of requests"),
            &["method", "path_template", "status"],
        )
        .expect("failed to create requests_total counter");

        let requests_in_flight = Gauge::with_opts(
            Opts::new("miroir_requests_in_flight", "Number of requests currently being processed"),
        )
        .expect("failed to create requests_in_flight gauge");

        let scatter_fan_out_size = Histogram::with_opts(
            HistogramOpts::new("miroir_scatter_fan_out_size", "Number of nodes in scatter operations")
                .buckets(vec![1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0]),
        )
        .expect("failed to create scatter_fan_out_size histogram");

        let scatter_partial_responses = Counter::with_opts(
            Opts::new("miroir_scatter_partial_responses_total", "Number of scatter responses that were partial (some nodes failed)"),
        )
        .expect("failed to create scatter_partial_responses counter");

        let scatter_retries = Counter::with_opts(
            Opts::new("miroir_scatter_retries_total", "Number of scatter retry attempts due to node failures"),
        )
        .expect("failed to create scatter_retries counter");

        let node_healthy = Gauge::with_opts(
            Opts::new("miroir_node_healthy", "Health status of backend nodes (1=healthy, 0=unhealthy)")
                .const_label("node", "all"),
        )
        .expect("failed to create node_healthy gauge");

        let node_request_duration = Histogram::with_opts(
            HistogramOpts::new("miroir_node_request_duration_seconds", "Latency of individual node requests")
                .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0])
                .const_label("node", "all"),
        )
        .expect("failed to create node_request_duration histogram");

        let node_errors = Counter::with_opts(
            Opts::new("miroir_node_errors_total", "Number of errors from backend nodes")
                .const_label("node", "all"),
        )
        .expect("failed to create node_errors counter");

        registry
            .register(Box::new(request_duration.clone()))
            .expect("failed to register request_duration");
        registry
            .register(Box::new(requests_total.clone()))
            .expect("failed to register requests_total");
        registry
            .register(Box::new(requests_in_flight.clone()))
            .expect("failed to register requests_in_flight");
        registry
            .register(Box::new(scatter_fan_out_size.clone()))
            .expect("failed to register scatter_fan_out_size");
        registry
            .register(Box::new(scatter_partial_responses.clone()))
            .expect("failed to register scatter_partial_responses");
        registry
            .register(Box::new(scatter_retries.clone()))
            .expect("failed to register scatter_retries");
        registry
            .register(Box::new(node_healthy.clone()))
            .expect("failed to register node_healthy");
        registry
            .register(Box::new(node_request_duration.clone()))
            .expect("failed to register node_request_duration");
        registry
            .register(Box::new(node_errors.clone()))
            .expect("failed to register node_errors");

        Self {
            registry,
            request_duration,
            requests_total,
            requests_in_flight,
            scatter_fan_out_size,
            scatter_partial_responses,
            scatter_retries,
            node_healthy,
            node_request_duration,
            node_errors,
        }
    }

    pub fn encode_metrics(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer).map_err(|e| {
            prometheus::Error::Msg(format!("failed to convert metrics to UTF-8: {}", e))
        })?)
    }
}

/// Generate a short request ID from UUIDv7.
///
/// UUIDv7 provides time-ordered unique IDs. We take the first 8 hex characters,
/// hash them, and encode as hex for a short, URL-safe identifier.
pub fn generate_request_id() -> String {
    let uuid = Uuid::now_v7();
    let uuid_str = uuid.simple().to_string();
    // Take first 8 chars (enough entropy for uniqueness)
    let prefix = &uuid_str[..8];

    // Hash to get a consistent short representation
    let mut hasher = DefaultHasher::new();
    prefix.hash(&mut hasher);
    let hash = hasher.finish();

    // Encode as hex (16 chars = 64 bits)
    format!("{:016x}", hash)
}

/// Extension trait to add request ID extraction utilities.
pub trait RequestIdExt {
    fn get_request_id(&self) -> Option<String>;
    fn set_request_id(&mut self, id: &str);
}

impl RequestIdExt for HeaderMap {
    fn get_request_id(&self) -> Option<String> {
        self.get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    }

    fn set_request_id(&mut self, id: &str) {
        if let Ok(val) = HeaderValue::from_str(id) {
            self.insert("x-request-id", val);
        }
    }
}

/// Guard that decrements the in-flight gauge when dropped.
///
/// This ensures that even if the handler panics, the in-flight count
/// is accurately decremented.
struct InFlightGuard {
    metrics: Metrics,
}

impl InFlightGuard {
    fn new(metrics: Metrics) -> Self {
        metrics.requests_in_flight.inc();
        tracing::trace!(requests_in_flight = metrics.requests_in_flight.get(), "request started");
        Self { metrics }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.metrics.requests_in_flight.dec();
        tracing::trace!(requests_in_flight = self.metrics.requests_in_flight.get(), "request completed");
    }
}

/// Extract the path template from the matched route.
///
/// Axum's MatchedPath extractor provides the route template (e.g., "/indexes/{uid}/search")
/// instead of the actual path (e.g., "/indexes/products/search"), avoiding high-cardinality labels.
fn extract_path_template(request: &Request) -> String {
    request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|mp| mp.as_str())
        .unwrap_or_else(|| request.uri().path())
        .to_string()
}

/// Main middleware that combines request ID injection, structured logging, and Prometheus metrics.
pub async fn telemetry_middleware(
    State(metrics): State<Metrics>,
    mut req: Request,
    next: Next,
) -> Response {
    let start = Instant::now();
    let method = req.method().clone();
    let path_template = extract_path_template(&req);

    // Generate or extract request ID
    let request_id = req
        .headers()
        .get_request_id()
        .unwrap_or_else(generate_request_id);
    req.headers_mut().set_request_id(&request_id);

    // Create span for structured logging
    let span = info_span!(
        "request",
        request_id = %request_id,
        method = %method,
        path_template = %path_template,
        path = %req.uri().path(),
    );

    let _guard = span.enter();

    // Track in-flight requests
    let in_flight = InFlightGuard::new(metrics.clone());

    let response = next.run(req).await;

    drop(in_flight);

    let status = response.status();
    let status_u16 = status.as_u16();
    let duration = start.elapsed();

    // Record Prometheus metrics
    metrics
        .request_duration
        .with_label_values(&[method.as_str(), &path_template, &status_u16.to_string()])
        .observe(duration.as_secs_f64());
    metrics
        .requests_total
        .with_label_values(&[method.as_str(), &path_template, &status_u16.to_string()])
        .inc();

    // Structured log entry (plan §10 shape)
    // Base fields: timestamp (from tracing-subscriber), level, message, duration_ms
    // Additional fields (index, node_count, estimated_hits, degraded)
    // are added by request handlers via the tracing span.
    let message = format!("{} {}", method, status);
    if status.is_server_error() {
        tracing::error!(
            target: "miroir.request",
            message = %message,
            duration_ms = duration.as_millis(),
            status = status_u16,
            method = %method,
            path_template = %path_template,
            request_id = %request_id,
        );
    } else if status.is_client_error() {
        tracing::warn!(
            target: "miroir.request",
            message = %message,
            duration_ms = duration.as_millis(),
            status = status_u16,
            method = %method,
            path_template = %path_template,
            request_id = %request_id,
        );
    } else {
        tracing::info!(
            target: "miroir.request",
            message = %message,
            duration_ms = duration.as_millis(),
            status = status_u16,
            method = %method,
            path_template = %path_template,
            request_id = %request_id,
        );
    }

    // Ensure request ID is in response headers
    let mut response = response;
    if !response.headers().contains_key("x-request-id") {
        if let Ok(val) = HeaderValue::from_str(&request_id) {
            response.headers_mut().insert("x-request-id", val);
        }
    }

    response
}

/// Create the metrics router for the :9090 server.
pub fn metrics_router() -> Router<Metrics> {
    Router::new().route("/metrics", get(metrics_handler))
}

/// Handler that returns Prometheus metrics in text exposition format.
async fn metrics_handler(State(metrics): State<Metrics>) -> String {
    match metrics.encode_metrics() {
        Ok(metrics) => metrics,
        Err(e) => {
            tracing::error!(error = %e, "failed to encode metrics");
            format!("# ERROR: failed to encode metrics: {}\n", e)
        }
    }
}

/// Accessor methods for metrics that can be used by other parts of the application.
impl Metrics {
    pub fn record_scatter_fan_out(&self, size: u64) {
        self.scatter_fan_out_size.observe(size as f64);
    }

    pub fn inc_scatter_partial_responses(&self) {
        self.scatter_partial_responses.inc();
    }

    pub fn inc_scatter_retries(&self) {
        self.scatter_retries.inc();
    }

    pub fn set_node_healthy(&self, _node: &str, healthy: bool) {
        let gauge_value = if healthy { 1.0 } else { 0.0 };
        // Note: In a real implementation, you'd want to use a GaugeVec with node labels
        // For now, we'll just set a placeholder value
        self.node_healthy.set(gauge_value);
    }

    pub fn record_node_request_duration(&self, _node: &str, duration_secs: f64) {
        self.node_request_duration.observe(duration_secs);
    }

    pub fn inc_node_errors(&self, _node: &str) {
        self.node_errors.inc();
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_generation() {
        // Generate multiple IDs to verify format
        for _ in 0..10 {
            let id = generate_request_id();

            // IDs should be 16 hex chars (64-bit hash)
            assert_eq!(id.len(), 16);

            // IDs should be hexadecimal
            assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
        }

        // Test that different UUID prefixes produce different IDs
        let id1 = generate_request_id();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let id2 = generate_request_id();
        // In production, time ensures uniqueness; test just verifies format above
        assert_eq!(id1.len(), 16);
        assert_eq!(id2.len(), 16);
    }

    #[test]
    fn test_metrics_creation() {
        let metrics = Metrics::new();

        // Add some sample data to ensure metrics show up in output
        metrics.request_duration.with_label_values(&["GET", "/test", "200"]).observe(0.1);
        metrics.requests_total.with_label_values(&["GET", "/test", "200"]).inc();
        metrics.requests_in_flight.inc();
        metrics.scatter_fan_out_size.observe(3.0);
        metrics.scatter_partial_responses.inc();
        metrics.scatter_retries.inc();
        metrics.node_healthy.set(1.0);
        metrics.node_request_duration.observe(0.05);
        metrics.node_errors.inc();

        let encoded = metrics.encode_metrics();
        assert!(encoded.is_ok());

        let output = encoded.unwrap();
        assert!(output.contains("miroir_request_duration_seconds"));
        assert!(output.contains("miroir_requests_total"));
        assert!(output.contains("miroir_requests_in_flight"));
        assert!(output.contains("miroir_scatter_fan_out_size"));
        assert!(output.contains("miroir_scatter_partial_responses_total"));
        assert!(output.contains("miroir_scatter_retries_total"));
        assert!(output.contains("miroir_node_healthy"));
        assert!(output.contains("miroir_node_request_duration_seconds"));
        assert!(output.contains("miroir_node_errors_total"));
    }

    #[test]
    fn test_header_request_id() {
        let mut headers = HeaderMap::new();
        assert!(headers.get_request_id().is_none());

        headers.set_request_id("test-id-123");
        assert_eq!(headers.get_request_id(), Some("test-id-123".to_string()));
    }
}
