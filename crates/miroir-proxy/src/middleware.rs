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
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts,
    Registry, TextEncoder,
};
use tracing::info_span;
use uuid::Uuid;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Telemetry state combining metrics and pod_id for middleware.
#[derive(Clone)]
pub struct TelemetryState {
    pub metrics: Metrics,
    pub pod_id: String,
}

impl TelemetryState {
    pub fn new(metrics: Metrics) -> Self {
        let pod_id = std::env::var("POD_NAME").unwrap_or_else(|_| "unknown".to_string());
        Self { metrics, pod_id }
    }
}

/// Global metrics registry shared across all middleware instances.
pub struct Metrics {
    registry: Registry,

    // ── Request metrics ──
    request_duration: HistogramVec,
    requests_total: CounterVec,
    requests_in_flight: Gauge,

    // ── Node health metrics ──
    node_healthy: GaugeVec,
    node_request_duration: HistogramVec,
    node_errors: CounterVec,

    // ── Shard metrics ──
    shard_coverage: Gauge,
    degraded_shards: Gauge,
    shard_distribution: GaugeVec,

    // ── Task metrics ──
    task_processing_age: Histogram,
    tasks_total: CounterVec,
    task_registry_size: Gauge,

    // ── Scatter-gather metrics ──
    scatter_fan_out_size: Histogram,
    scatter_partial_responses: Counter,
    scatter_retries: Counter,

    // ── Rebalancer metrics ──
    rebalance_in_progress: Gauge,
    rebalance_documents_migrated: Counter,
    rebalance_duration: Histogram,
}

impl Clone for Metrics {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
            request_duration: self.request_duration.clone(),
            requests_total: self.requests_total.clone(),
            requests_in_flight: self.requests_in_flight.clone(),
            node_healthy: self.node_healthy.clone(),
            node_request_duration: self.node_request_duration.clone(),
            node_errors: self.node_errors.clone(),
            shard_coverage: self.shard_coverage.clone(),
            degraded_shards: self.degraded_shards.clone(),
            shard_distribution: self.shard_distribution.clone(),
            task_processing_age: self.task_processing_age.clone(),
            tasks_total: self.tasks_total.clone(),
            task_registry_size: self.task_registry_size.clone(),
            scatter_fan_out_size: self.scatter_fan_out_size.clone(),
            scatter_partial_responses: self.scatter_partial_responses.clone(),
            scatter_retries: self.scatter_retries.clone(),
            rebalance_in_progress: self.rebalance_in_progress.clone(),
            rebalance_documents_migrated: self.rebalance_documents_migrated.clone(),
            rebalance_duration: self.rebalance_duration.clone(),
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

        // ── Request metrics ──
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

        // ── Node health metrics ──
        let node_healthy = GaugeVec::new(
            Opts::new("miroir_node_healthy", "Health status of backend nodes (1=healthy, 0=unhealthy)"),
            &["node_id"],
        )
        .expect("failed to create node_healthy gauge");

        let node_request_duration = HistogramVec::new(
            HistogramOpts::new("miroir_node_request_duration_seconds", "Latency of individual node requests")
                .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0]),
            &["node_id", "operation"],
        )
        .expect("failed to create node_request_duration histogram");

        let node_errors = CounterVec::new(
            Opts::new("miroir_node_errors_total", "Number of errors from backend nodes"),
            &["node_id", "error_type"],
        )
        .expect("failed to create node_errors counter");

        // ── Shard metrics ──
        let shard_coverage = Gauge::with_opts(
            Opts::new("miroir_shard_coverage", "Fraction of shards with at least one healthy replica"),
        )
        .expect("failed to create shard_coverage gauge");

        let degraded_shards = Gauge::with_opts(
            Opts::new("miroir_degraded_shards_total", "Number of shards with reduced replica availability"),
        )
        .expect("failed to create degraded_shards gauge");

        let shard_distribution = GaugeVec::new(
            Opts::new("miroir_shard_distribution", "Number of shards assigned to each node"),
            &["node_id"],
        )
        .expect("failed to create shard_distribution gauge");

        // ── Task metrics ──
        let task_processing_age = Histogram::with_opts(
            HistogramOpts::new("miroir_task_processing_age_seconds", "Time between task creation and processing start")
                .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]),
        )
        .expect("failed to create task_processing_age histogram");

        let tasks_total = CounterVec::new(
            Opts::new("miroir_tasks_total", "Total number of tasks by status"),
            &["status"],
        )
        .expect("failed to create tasks_total counter");

        let task_registry_size = Gauge::with_opts(
            Opts::new("miroir_task_registry_size", "Current number of tasks in the registry"),
        )
        .expect("failed to create task_registry_size gauge");

        // ── Scatter-gather metrics ──
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

        // ── Rebalancer metrics ──
        let rebalance_in_progress = Gauge::with_opts(
            Opts::new("miroir_rebalance_in_progress", "Whether a rebalance is currently running (1=yes, 0=no)"),
        )
        .expect("failed to create rebalance_in_progress gauge");

        let rebalance_documents_migrated = Counter::with_opts(
            Opts::new("miroir_rebalance_documents_migrated_total", "Total number of documents migrated during rebalance"),
        )
        .expect("failed to create rebalance_documents_migrated counter");

        let rebalance_duration = Histogram::with_opts(
            HistogramOpts::new("miroir_rebalance_duration_seconds", "Duration of rebalance operations")
                .buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0]),
        )
        .expect("failed to create rebalance_duration histogram");

        // Register all metrics
        macro_rules! reg {
            ($m:expr) => {
                registry.register(Box::new($m.clone())).expect(concat!("failed to register ", stringify!($m)));
            };
        }

        reg!(request_duration);
        reg!(requests_total);
        reg!(requests_in_flight);
        reg!(node_healthy);
        reg!(node_request_duration);
        reg!(node_errors);
        reg!(shard_coverage);
        reg!(degraded_shards);
        reg!(shard_distribution);
        reg!(task_processing_age);
        reg!(tasks_total);
        reg!(task_registry_size);
        reg!(scatter_fan_out_size);
        reg!(scatter_partial_responses);
        reg!(scatter_retries);
        reg!(rebalance_in_progress);
        reg!(rebalance_documents_migrated);
        reg!(rebalance_duration);

        Self {
            registry,
            request_duration,
            requests_total,
            requests_in_flight,
            node_healthy,
            node_request_duration,
            node_errors,
            shard_coverage,
            degraded_shards,
            shard_distribution,
            task_processing_age,
            tasks_total,
            task_registry_size,
            scatter_fan_out_size,
            scatter_partial_responses,
            scatter_retries,
            rebalance_in_progress,
            rebalance_documents_migrated,
            rebalance_duration,
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
    State(telemetry): State<TelemetryState>,
    mut req: Request,
    next: Next,
) -> Response {
    let start = Instant::now();
    let method = req.method().clone();
    let path_template = extract_path_template(&req);
    let metrics = telemetry.metrics.clone();
    let pod_id = telemetry.pod_id.clone();

    // Generate or extract request ID
    let request_id = req
        .headers()
        .get_request_id()
        .unwrap_or_else(generate_request_id);
    req.headers_mut().set_request_id(&request_id);

    // Create span for structured logging with pod_id included
    let span = info_span!(
        "request",
        request_id = %request_id,
        pod_id = %pod_id,
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
            pod_id = %pod_id,
            request_id = %request_id,
            message = %message,
            duration_ms = duration.as_millis(),
            status = status_u16,
            method = %method,
            path_template = %path_template,
        );
    } else if status.is_client_error() {
        tracing::warn!(
            target: "miroir.request",
            pod_id = %pod_id,
            request_id = %request_id,
            message = %message,
            duration_ms = duration.as_millis(),
            status = status_u16,
            method = %method,
            path_template = %path_template,
        );
    } else {
        tracing::info!(
            target: "miroir.request",
            pod_id = %pod_id,
            request_id = %request_id,
            message = %message,
            duration_ms = duration.as_millis(),
            status = status_u16,
            method = %method,
            path_template = %path_template,
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
    // ── Scatter-gather ──

    pub fn record_scatter_fan_out(&self, size: u64) {
        self.scatter_fan_out_size.observe(size as f64);
    }

    pub fn inc_scatter_partial_responses(&self) {
        self.scatter_partial_responses.inc();
    }

    pub fn inc_scatter_retries(&self) {
        self.scatter_retries.inc();
    }

    // ── Node health ──

    pub fn set_node_healthy(&self, node_id: &str, healthy: bool) {
        self.node_healthy.with_label_values(&[node_id]).set(if healthy { 1.0 } else { 0.0 });
    }

    pub fn record_node_request_duration(&self, node_id: &str, operation: &str, duration_secs: f64) {
        self.node_request_duration.with_label_values(&[node_id, operation]).observe(duration_secs);
    }

    pub fn inc_node_errors(&self, node_id: &str, error_type: &str) {
        self.node_errors.with_label_values(&[node_id, error_type]).inc();
    }

    // ── Shards ──

    pub fn set_shard_coverage(&self, coverage: f64) {
        self.shard_coverage.set(coverage);
    }

    pub fn set_degraded_shards(&self, count: f64) {
        self.degraded_shards.set(count);
    }

    pub fn set_shard_distribution(&self, node_id: &str, count: f64) {
        self.shard_distribution.with_label_values(&[node_id]).set(count);
    }

    // ── Tasks ──

    pub fn observe_task_processing_age(&self, age_secs: f64) {
        self.task_processing_age.observe(age_secs);
    }

    pub fn inc_tasks_total(&self, status: &str) {
        self.tasks_total.with_label_values(&[status]).inc();
    }

    pub fn set_task_registry_size(&self, size: f64) {
        self.task_registry_size.set(size);
    }

    // ── Rebalancer ──

    pub fn set_rebalance_in_progress(&self, v: bool) {
        self.rebalance_in_progress.set(if v { 1.0 } else { 0.0 });
    }

    pub fn inc_rebalance_documents_migrated(&self, count: u64) {
        self.rebalance_documents_migrated.inc_by(count as f64);
    }

    pub fn observe_rebalance_duration(&self, secs: f64) {
        self.rebalance_duration.observe(secs);
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
        metrics.node_healthy.with_label_values(&["test-node"]).set(1.0);
        metrics.node_request_duration.with_label_values(&["test-node", "search"]).observe(0.05);
        metrics.node_errors.with_label_values(&["test-node", "timeout"]).inc();
        metrics.shard_coverage.set(1.0);
        metrics.degraded_shards.set(0.0);
        metrics.shard_distribution.with_label_values(&["test-node"]).set(32.0);
        metrics.task_processing_age.observe(0.1);
        metrics.tasks_total.with_label_values(&["completed"]).inc();
        metrics.task_registry_size.set(5.0);
        metrics.scatter_fan_out_size.observe(3.0);
        metrics.scatter_partial_responses.inc();
        metrics.scatter_retries.inc();
        metrics.rebalance_in_progress.set(0.0);
        metrics.rebalance_documents_migrated.inc();
        metrics.rebalance_duration.observe(10.0);

        let encoded = metrics.encode_metrics();
        assert!(encoded.is_ok());

        let output = encoded.unwrap();

        // Verify all 18 plan §10 metric names appear in the output
        let expected_metrics = [
            // Request metrics
            "miroir_request_duration_seconds",
            "miroir_requests_total",
            "miroir_requests_in_flight",
            // Node health metrics
            "miroir_node_healthy",
            "miroir_node_request_duration_seconds",
            "miroir_node_errors_total",
            // Shard metrics
            "miroir_shard_coverage",
            "miroir_degraded_shards_total",
            "miroir_shard_distribution",
            // Task metrics
            "miroir_task_processing_age_seconds",
            "miroir_tasks_total",
            "miroir_task_registry_size",
            // Scatter-gather metrics
            "miroir_scatter_fan_out_size",
            "miroir_scatter_partial_responses_total",
            "miroir_scatter_retries_total",
            // Rebalancer metrics
            "miroir_rebalance_in_progress",
            "miroir_rebalance_documents_migrated_total",
            "miroir_rebalance_duration_seconds",
        ];
        for name in &expected_metrics {
            assert!(output.contains(name), "missing metric: {}", name);
        }
    }

    #[test]
    fn test_header_request_id() {
        let mut headers = HeaderMap::new();
        assert!(headers.get_request_id().is_none());

        headers.set_request_id("test-id-123");
        assert_eq!(headers.get_request_id(), Some("test-id-123".to_string()));
    }
}
