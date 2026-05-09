//! Tracing/logging + Prometheus middleware

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use crate::state::ProxyState;
use std::time::Instant;
use prometheus::{Counter, Histogram, IntGauge, Registry, TextEncoder, HistogramOpts, Encoder};

/// Prometheus metrics registry.
#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    requests_total: Counter,
    request_duration_seconds: Histogram,
    requests_in_flight: IntGauge,
    degraded_requests_total: Counter,
    no_quorum_requests_total: Counter,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        // Create and register metrics
        let requests_total = Counter::new(
            "miroir_requests_total",
            "Total number of requests"
        ).unwrap();

        let request_duration_seconds = Histogram::with_opts(HistogramOpts {
            common_opts: prometheus::Opts::new(
                "miroir_request_duration_seconds",
                "Request duration in seconds"
            ),
            buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
        }).unwrap();

        let requests_in_flight = IntGauge::new(
            "miroir_requests_in_flight",
            "Current number of requests in flight"
        ).unwrap();

        let degraded_requests_total = Counter::new(
            "miroir_degraded_requests_total",
            "Total number of degraded requests"
        ).unwrap();

        let no_quorum_requests_total = Counter::new(
            "miroir_no_quorum_requests_total",
            "Total number of requests that failed with no quorum"
        ).unwrap();

        // Register all metrics
        registry.register(Box::new(requests_total.clone())).unwrap();
        registry.register(Box::new(request_duration_seconds.clone())).unwrap();
        registry.register(Box::new(requests_in_flight.clone())).unwrap();
        registry.register(Box::new(degraded_requests_total.clone())).unwrap();
        registry.register(Box::new(no_quorum_requests_total.clone())).unwrap();

        Self {
            registry,
            requests_total,
            request_duration_seconds,
            requests_in_flight,
            degraded_requests_total,
            no_quorum_requests_total,
        }
    }

    /// Record a request with labels.
    pub fn record_request(&self, _method: &str, _path: &str, status: u16, duration_secs: f64) {
        self.requests_total.inc();
        self.request_duration_seconds.observe(duration_secs);

        // Check for no quorum (503 status)
        if status == 503 {
            self.no_quorum_requests_total.inc();
        }
    }

    /// Record a degraded request.
    pub fn record_degraded(&self, _method: &str, _path: &str) {
        self.degraded_requests_total.inc();
    }

    /// Increment requests in flight.
    pub fn inc_in_flight(&self) {
        self.requests_in_flight.inc();
    }

    /// Decrement requests in flight.
    pub fn dec_in_flight(&self) {
        self.requests_in_flight.dec();
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Tracing middleware that logs each request.
pub async fn tracing_middleware(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let start = Instant::now();

    let response = next.run(req).await;

    let duration = start.elapsed();
    let status = response.status();

    tracing::info!(
        method = %method,
        uri = %uri,
        status = status.as_u16(),
        duration_ms = duration.as_millis(),
        "request completed"
    );

    response
}

/// Prometheus metrics middleware.
pub async fn prometheus_middleware(
    State(state): State<ProxyState>,
    req: Request,
    next: Next,
) -> Response {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();

    state.metrics.inc_in_flight();
    let start = Instant::now();

    let response = next.run(req).await;

    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16();

    // Record metrics
    state.metrics.record_request(&method, &path, status, duration);
    state.metrics.dec_in_flight();

    // Check for degraded header
    if response.headers().get("X-Miroir-Degraded").is_some() {
        state.metrics.record_degraded(&method, &path);
    }

    response
}

/// Export metrics in Prometheus text format.
pub fn export_metrics(metrics: &Metrics) -> String {
    let encoder = TextEncoder::new();
    let metric_families = metrics.registry.gather();
    let mut buffer = Vec::new();

    encoder.encode(&metric_families, &mut buffer).unwrap();

    String::from_utf8(buffer).unwrap_or_else(|_| "# Failed to encode metrics\n".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = Metrics::new();
        assert!(!metrics.registry.gather().is_empty());
    }

    #[test]
    fn test_export_metrics() {
        let metrics = Metrics::new();
        let output = export_metrics(&metrics);
        assert!(output.contains("miroir_requests_total"));
        assert!(output.contains("miroir_request_duration_seconds"));
        assert!(output.contains("miroir_requests_in_flight"));
    }

    #[test]
    fn test_metrics_default() {
        let metrics = Metrics::default();
        assert!(!metrics.registry.gather().is_empty());
    }
}
