//! Tracing/logging + Prometheus middleware

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use crate::state::ProxyState;
use std::time::Instant;
use prometheus::{Counter, Histogram, IntGauge, Registry, TextEncoder};
use once_cell::sync::Lazy;

/// Prometheus metrics registry.
#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        // Register all metrics
        registry.register(Box::new(REQUESTS_TOTAL.clone())).unwrap();
        registry.register(Box::new(REQUEST_DURATION_SECONDS.clone())).unwrap();
        registry.register(Box::new(REQUESTS_IN_FLIGHT.clone())).unwrap();
        registry.register(Box::new(DEGRADED_REQUESTS_TOTAL.clone())).unwrap();
        registry.register(Box::new(NO_QUORUM_REQUESTS_TOTAL.clone())).unwrap();

        Self { registry }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Total number of requests.
static REQUESTS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    Counter::new("miroir_requests_total", "Total number of requests").unwrap()
});

/// Request duration in seconds.
static REQUEST_DURATION_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(prometheus::HistogramOpts {
        common_name: "miroir_request_duration_seconds".to_string(),
        help: "Request duration in seconds".to_string(),
        buckets: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
    })
    .unwrap()
});

/// Current number of requests in flight.
static REQUESTS_IN_FLIGHT: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::new("miroir_requests_in_flight", "Current number of requests in flight").unwrap()
});

/// Total number of degraded requests.
static DEGRADED_REQUESTS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    Counter::new("miroir_degraded_requests_total", "Total number of degraded requests").unwrap()
});

/// Total number of requests that failed with no quorum.
static NO_QUORUM_REQUESTS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    Counter::new("miroir_no_quorum_requests_total", "Total number of requests that failed with no quorum").unwrap()
});

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
    State(_state): State<ProxyState>,
    req: Request,
    next: Next,
) -> Response {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();

    REQUESTS_IN_FLIGHT.inc();
    let start = Instant::now();

    let response = next.run(req).await;

    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16();

    // Record metrics
    REQUESTS_TOTAL
        .with_label_values(&[&method, &path, &status.to_string()])
        .inc();

    REQUEST_DURATION_SECONDS
        .with_label_values(&[&method, &path])
        .observe(duration);

    REQUESTS_IN_FLIGHT.dec();

    // Check for degraded header
    if response.headers().get("X-Miroir-Degraded").is_some() {
        DEGRADED_REQUESTS_TOTAL
            .with_label_values(&[&method, &path])
            .inc();
    }

    // Check for no quorum (503 status with specific error code)
    if status == 503 {
        DEGRADED_REQUESTS_TOTAL
            .with_label_values(&[&method, &path])
            .inc();
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
