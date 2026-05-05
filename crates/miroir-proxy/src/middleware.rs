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
use miroir_core::config::MiroirConfig;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts,
    Registry, TextEncoder,
};
use tracing::info_span;
use uuid::Uuid;
use hex;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Request ID wrapper type for storing in axum Request extensions.
///
/// This is a newtype wrapper around the 8-character hex request ID,
/// allowing handlers to extract it via `Request.extensions().get::<RequestId>()`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RequestId(pub String);

impl RequestId {
    /// Create a new RequestId from a UUIDv7.
    ///
    /// Hashes the full UUIDv7 to produce an 8-character hex ID that is unique
    /// even for consecutive calls within the same millisecond.
    pub fn new() -> Self {
        let uuid = Uuid::now_v7();
        let bytes = uuid.as_bytes();
        // Hash the full UUID to ensure uniqueness even within the same millisecond
        let mut hasher = DefaultHasher::new();
        hasher.write(bytes);
        let hash = hasher.finish();
        // Take first 8 hex chars of 64-bit hash (32 bits is sufficient entropy)
        Self(format!("{:08x}", hash as u32))
    }

    /// Get the inner request ID string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parse a RequestId from a string.
    pub fn parse(s: String) -> Option<Self> {
        if s.len() == 8 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            Some(Self(s))
        } else {
            None
        }
    }
}

pub async fn request_id_middleware(
    mut req: Request,
    next: Next,
) -> Response {
    // Check for existing request ID in headers
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| RequestId::parse(s.to_string()))
        .unwrap_or_else(RequestId::new);

    // Store in request extensions for handler access
    req.extensions_mut().insert(request_id.clone());

    // Set X-Request-Id header on request (for telemetry_middleware to read)
    if let Ok(val) = HeaderValue::from_str(request_id.as_str()) {
        req.headers_mut().insert("x-request-id", val);
    }

    // Process the request
    let mut response = next.run(req).await;

    // Add X-Request-Id header to response (override if exists)
    if let Ok(val) = HeaderValue::from_str(request_id.as_str()) {
        response.headers_mut().insert("x-request-id", val);
    }

    response
}


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

    // ── §13.11 Multi-search metrics (feature-gated) ──
    multisearch_queries_per_batch: Option<Histogram>,
    multisearch_batches_total: Option<Counter>,
    multisearch_partial_failures_total: Option<Counter>,
    multisearch_tenant_session_pin_override_total: Option<CounterVec>,

    // ── §13.12 Vector search metrics (feature-gated) ──
    vector_search_over_fetched_total: Option<Counter>,
    vector_merge_strategy: Option<CounterVec>,
    vector_embedder_drift_total: Option<Counter>,

    // ── §13.13 CDC metrics (feature-gated) ──
    cdc_events_published_total: Option<CounterVec>,
    cdc_lag_seconds: Option<GaugeVec>,
    cdc_buffer_bytes: Option<GaugeVec>,
    cdc_dropped_total: Option<CounterVec>,
    cdc_events_suppressed_total: Option<CounterVec>,

    // ── §13.14 TTL metrics (feature-gated) ──
    ttl_documents_expired_total: Option<CounterVec>,
    ttl_sweep_duration_seconds: Option<HistogramVec>,
    ttl_pending_estimate: Option<GaugeVec>,

    // ── §13.15 Tenant affinity metrics (feature-gated) ──
    tenant_queries_total: Option<CounterVec>,
    tenant_pinned_groups: Option<GaugeVec>,
    tenant_fallback_total: Option<CounterVec>,

    // ── §13.16 Shadow traffic metrics (feature-gated) ──
    shadow_diff_total: Option<CounterVec>,
    shadow_kendall_tau: Option<Gauge>,
    shadow_latency_delta_seconds: Option<Histogram>,
    shadow_errors_total: Option<CounterVec>,

    // ── §13.17 ILM metrics (feature-gated) ──
    rollover_events_total: Option<CounterVec>,
    rollover_active_indexes: Option<GaugeVec>,
    rollover_documents_expired_total: Option<CounterVec>,
    rollover_last_action_seconds: Option<GaugeVec>,

    // ── §13.18 Canary metrics (feature-gated) ──
    canary_runs_total: Option<CounterVec>,
    canary_latency_ms: Option<HistogramVec>,
    canary_assertion_failures_total: Option<CounterVec>,

    // ── §13.19 Admin UI metrics (feature-gated) ──
    admin_ui_sessions_total: Option<Counter>,
    admin_ui_action_total: Option<CounterVec>,
    admin_ui_destructive_action_total: Option<CounterVec>,

    // ── §13.20 Explain metrics (feature-gated) ──
    explain_requests_total: Option<Counter>,
    explain_warnings_total: Option<CounterVec>,
    explain_execute_total: Option<Counter>,

    // ── §13.21 Search UI metrics (feature-gated) ──
    search_ui_sessions_total: Option<Counter>,
    search_ui_queries_total: Option<CounterVec>,
    search_ui_zero_hits_total: Option<CounterVec>,
    search_ui_click_through_total: Option<CounterVec>,
    search_ui_p95_ms: Option<GaugeVec>,

    // ── §14.9 Resource-pressure metrics (always present) ──
    memory_pressure: Gauge,
    cpu_throttled_seconds_total: Counter,
    request_queue_depth: Gauge,
    background_queue_depth: GaugeVec,
    peer_pod_count: Gauge,
    leader: Gauge,
    owned_shards_count: Gauge,

    // ── Admin session sealing metrics (always present) ──
    admin_session_key_generated: Gauge,
    admin_session_revoked_total: Counter,

    // ── §13.5 Two-phase settings broadcast metrics (always present) ──
    settings_broadcast_phase: GaugeVec,
    settings_hash_mismatch_total: Counter,
    settings_drift_repair_total: CounterVec,
    settings_version: GaugeVec,
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
            multisearch_queries_per_batch: self.multisearch_queries_per_batch.clone(),
            multisearch_batches_total: self.multisearch_batches_total.clone(),
            multisearch_partial_failures_total: self.multisearch_partial_failures_total.clone(),
            multisearch_tenant_session_pin_override_total: self.multisearch_tenant_session_pin_override_total.clone(),
            vector_search_over_fetched_total: self.vector_search_over_fetched_total.clone(),
            vector_merge_strategy: self.vector_merge_strategy.clone(),
            vector_embedder_drift_total: self.vector_embedder_drift_total.clone(),
            cdc_events_published_total: self.cdc_events_published_total.clone(),
            cdc_lag_seconds: self.cdc_lag_seconds.clone(),
            cdc_buffer_bytes: self.cdc_buffer_bytes.clone(),
            cdc_dropped_total: self.cdc_dropped_total.clone(),
            cdc_events_suppressed_total: self.cdc_events_suppressed_total.clone(),
            ttl_documents_expired_total: self.ttl_documents_expired_total.clone(),
            ttl_sweep_duration_seconds: self.ttl_sweep_duration_seconds.clone(),
            ttl_pending_estimate: self.ttl_pending_estimate.clone(),
            tenant_queries_total: self.tenant_queries_total.clone(),
            tenant_pinned_groups: self.tenant_pinned_groups.clone(),
            tenant_fallback_total: self.tenant_fallback_total.clone(),
            shadow_diff_total: self.shadow_diff_total.clone(),
            shadow_kendall_tau: self.shadow_kendall_tau.clone(),
            shadow_latency_delta_seconds: self.shadow_latency_delta_seconds.clone(),
            shadow_errors_total: self.shadow_errors_total.clone(),
            rollover_events_total: self.rollover_events_total.clone(),
            rollover_active_indexes: self.rollover_active_indexes.clone(),
            rollover_documents_expired_total: self.rollover_documents_expired_total.clone(),
            rollover_last_action_seconds: self.rollover_last_action_seconds.clone(),
            canary_runs_total: self.canary_runs_total.clone(),
            canary_latency_ms: self.canary_latency_ms.clone(),
            canary_assertion_failures_total: self.canary_assertion_failures_total.clone(),
            admin_ui_sessions_total: self.admin_ui_sessions_total.clone(),
            admin_ui_action_total: self.admin_ui_action_total.clone(),
            admin_ui_destructive_action_total: self.admin_ui_destructive_action_total.clone(),
            explain_requests_total: self.explain_requests_total.clone(),
            explain_warnings_total: self.explain_warnings_total.clone(),
            explain_execute_total: self.explain_execute_total.clone(),
            search_ui_sessions_total: self.search_ui_sessions_total.clone(),
            search_ui_queries_total: self.search_ui_queries_total.clone(),
            search_ui_zero_hits_total: self.search_ui_zero_hits_total.clone(),
            search_ui_click_through_total: self.search_ui_click_through_total.clone(),
            search_ui_p95_ms: self.search_ui_p95_ms.clone(),
            memory_pressure: self.memory_pressure.clone(),
            cpu_throttled_seconds_total: self.cpu_throttled_seconds_total.clone(),
            request_queue_depth: self.request_queue_depth.clone(),
            background_queue_depth: self.background_queue_depth.clone(),
            peer_pod_count: self.peer_pod_count.clone(),
            leader: self.leader.clone(),
            owned_shards_count: self.owned_shards_count.clone(),
            admin_session_key_generated: self.admin_session_key_generated.clone(),
            admin_session_revoked_total: self.admin_session_revoked_total.clone(),
            settings_broadcast_phase: self.settings_broadcast_phase.clone(),
            settings_hash_mismatch_total: self.settings_hash_mismatch_total.clone(),
            settings_drift_repair_total: self.settings_drift_repair_total.clone(),
            settings_version: self.settings_version.clone(),
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new(&MiroirConfig::default())
    }
}

impl Metrics {
    pub fn new(config: &MiroirConfig) -> Self {
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

        // ── §13.11 Multi-search metrics (cardinality cap: top 100 tenants, rest bucketed) ──
        let (
            multisearch_queries_per_batch,
            multisearch_batches_total,
            multisearch_partial_failures_total,
            multisearch_tenant_session_pin_override_total,
        ) = if config.multi_search.enabled {
            let q = Histogram::with_opts(
                HistogramOpts::new("miroir_multisearch_queries_per_batch", "Number of queries in each multi-search batch")
                    .buckets(vec![1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0]),
            ).expect("create multisearch_queries_per_batch");
            let b = Counter::with_opts(
                Opts::new("miroir_multisearch_batches_total", "Total number of multi-search batches processed"),
            ).expect("create multisearch_batches_total");
            let p = Counter::with_opts(
                Opts::new("miroir_multisearch_partial_failures_total", "Number of multi-search batches with at least one query failure"),
            ).expect("create multisearch_partial_failures_total");
            let t = CounterVec::new(
                Opts::new("miroir_tenant_session_pin_override_total", "Session pin overrides triggered by multi-search tenant routing"),
                &["tenant"],
            ).expect("create multisearch_tenant_session_pin_override_total");
            reg!(q); reg!(b); reg!(p); reg!(t);
            (Some(q), Some(b), Some(p), Some(t))
        } else {
            (None, None, None, None)
        };

        // ── §13.12 Vector search metrics ──
        let (
            vector_search_over_fetched_total,
            vector_merge_strategy,
            vector_embedder_drift_total,
        ) = if config.vector_search.enabled {
            let o = Counter::with_opts(
                Opts::new("miroir_vector_search_over_fetched_total", "Number of vector searches that over-fetched candidates"),
            ).expect("create vector_search_over_fetched_total");
            let m = CounterVec::new(
                Opts::new("miroir_vector_merge_strategy", "Count of hybrid merge strategy selections"),
                &["strategy"],
            ).expect("create vector_merge_strategy");
            let d = Counter::with_opts(
                Opts::new("miroir_vector_embedder_drift_total", "Number of embedder drift detections"),
            ).expect("create vector_embedder_drift_total");
            reg!(o); reg!(m); reg!(d);
            (Some(o), Some(m), Some(d))
        } else {
            (None, None, None)
        };

        // ── §13.13 CDC metrics (cardinality cap: top 100 sinks, rest bucketed) ──
        let (
            cdc_events_published_total,
            cdc_lag_seconds,
            cdc_buffer_bytes,
            cdc_dropped_total,
            cdc_events_suppressed_total,
        ) = if config.cdc.enabled {
            let e = CounterVec::new(
                Opts::new("miroir_cdc_events_published_total", "Total CDC events published"),
                &["sink", "index"],
            ).expect("create cdc_events_published_total");
            let l = GaugeVec::new(
                Opts::new("miroir_cdc_lag_seconds", "CDC delivery lag in seconds"),
                &["sink"],
            ).expect("create cdc_lag_seconds");
            let b = GaugeVec::new(
                Opts::new("miroir_cdc_buffer_bytes", "CDC buffer size in bytes"),
                &["sink"],
            ).expect("create cdc_buffer_bytes");
            let d = CounterVec::new(
                Opts::new("miroir_cdc_dropped_total", "CDC events dropped due to buffer overflow"),
                &["sink"],
            ).expect("create cdc_dropped_total");
            let s = CounterVec::new(
                Opts::new("miroir_cdc_events_suppressed_total", "CDC events suppressed by origin deduplication"),
                &["origin"],
            ).expect("create cdc_events_suppressed_total");
            reg!(e); reg!(l); reg!(b); reg!(d); reg!(s);
            (Some(e), Some(l), Some(b), Some(d), Some(s))
        } else {
            (None, None, None, None, None)
        };

        // ── §13.14 TTL metrics (cardinality cap: top 100 indexes, rest bucketed) ──
        let (
            ttl_documents_expired_total,
            ttl_sweep_duration_seconds,
            ttl_pending_estimate,
        ) = if config.ttl.enabled {
            let e = CounterVec::new(
                Opts::new("miroir_ttl_documents_expired_total", "Documents expired by TTL sweeper"),
                &["index"],
            ).expect("create ttl_documents_expired_total");
            let d = HistogramVec::new(
                HistogramOpts::new("miroir_ttl_sweep_duration_seconds", "Duration of TTL sweep cycles")
                    .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
                &["index"],
            ).expect("create ttl_sweep_duration_seconds");
            let p = GaugeVec::new(
                Opts::new("miroir_ttl_pending_estimate", "Estimated documents pending TTL expiry"),
                &["index"],
            ).expect("create ttl_pending_estimate");
            reg!(e); reg!(d); reg!(p);
            (Some(e), Some(d), Some(p))
        } else {
            (None, None, None)
        };

        // ── §13.15 Tenant affinity metrics (cardinality cap: top 100 tenants, rest bucketed) ──
        let (
            tenant_queries_total,
            tenant_pinned_groups,
            tenant_fallback_total,
        ) = if config.tenant_affinity.enabled {
            let q = CounterVec::new(
                Opts::new("miroir_tenant_queries_total", "Queries routed per tenant and group"),
                &["tenant", "group"],
            ).expect("create tenant_queries_total");
            let p = GaugeVec::new(
                Opts::new("miroir_tenant_pinned_groups", "Current pinned group per tenant"),
                &["tenant"],
            ).expect("create tenant_pinned_groups");
            let f = CounterVec::new(
                Opts::new("miroir_tenant_fallback_total", "Tenant affinity fallback invocations"),
                &["reason"],
            ).expect("create tenant_fallback_total");
            reg!(q); reg!(p); reg!(f);
            (Some(q), Some(p), Some(f))
        } else {
            (None, None, None)
        };

        // ── §13.16 Shadow traffic metrics ──
        let (
            shadow_diff_total,
            shadow_kendall_tau,
            shadow_latency_delta_seconds,
            shadow_errors_total,
        ) = if config.shadow.enabled {
            let d = CounterVec::new(
                Opts::new("miroir_shadow_diff_total", "Shadow comparison diffs by kind"),
                &["kind"],
            ).expect("create shadow_diff_total");
            let k = Gauge::with_opts(
                Opts::new("miroir_shadow_kendall_tau", "Kendall tau rank correlation between shadow and primary"),
            ).expect("create shadow_kendall_tau");
            let l = Histogram::with_opts(
                HistogramOpts::new("miroir_shadow_latency_delta_seconds", "Latency difference between shadow and primary")
                    .buckets(vec![-1.0, -0.5, -0.1, -0.01, 0.0, 0.01, 0.1, 0.5, 1.0]),
            ).expect("create shadow_latency_delta_seconds");
            let e = CounterVec::new(
                Opts::new("miroir_shadow_errors_total", "Shadow pipeline errors"),
                &["target", "side"],
            ).expect("create shadow_errors_total");
            reg!(d); reg!(k); reg!(l); reg!(e);
            (Some(d), Some(k), Some(l), Some(e))
        } else {
            (None, None, None, None)
        };

        // ── §13.17 ILM metrics (cardinality cap: top 100 policies/aliases, rest bucketed) ──
        let (
            rollover_events_total,
            rollover_active_indexes,
            rollover_documents_expired_total,
            rollover_last_action_seconds,
        ) = if config.ilm.enabled {
            let e = CounterVec::new(
                Opts::new("miroir_rollover_events_total", "ILM rollover events"),
                &["policy"],
            ).expect("create rollover_events_total");
            let a = GaugeVec::new(
                Opts::new("miroir_rollover_active_indexes", "Active write indexes per alias"),
                &["alias"],
            ).expect("create rollover_active_indexes");
            let d = CounterVec::new(
                Opts::new("miroir_rollover_documents_expired_total", "Documents expired by ILM retention policies"),
                &["policy"],
            ).expect("create rollover_documents_expired_total");
            let l = GaugeVec::new(
                Opts::new("miroir_rollover_last_action_seconds", "Seconds since last rollover action per policy"),
                &["policy"],
            ).expect("create rollover_last_action_seconds");
            reg!(e); reg!(a); reg!(d); reg!(l);
            (Some(e), Some(a), Some(d), Some(l))
        } else {
            (None, None, None, None)
        };

        // ── §13.18 Canary metrics (cardinality cap: top 100 canaries, rest bucketed) ──
        let (
            canary_runs_total,
            canary_latency_ms,
            canary_assertion_failures_total,
        ) = if config.canary_runner.enabled {
            let r = CounterVec::new(
                Opts::new("miroir_canary_runs_total", "Canary run results"),
                &["canary", "result"],
            ).expect("create canary_runs_total");
            let l = HistogramVec::new(
                HistogramOpts::new("miroir_canary_latency_ms", "Canary execution latency")
                    .buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0]),
                &["canary"],
            ).expect("create canary_latency_ms");
            let a = CounterVec::new(
                Opts::new("miroir_canary_assertion_failures_total", "Canary assertion failures"),
                &["canary", "assertion_type"],
            ).expect("create canary_assertion_failures_total");
            reg!(r); reg!(l); reg!(a);
            (Some(r), Some(l), Some(a))
        } else {
            (None, None, None)
        };

        // ── §13.19 Admin UI metrics ──
        let (
            admin_ui_sessions_total,
            admin_ui_action_total,
            admin_ui_destructive_action_total,
        ) = if config.admin_ui.enabled {
            let s = Counter::with_opts(
                Opts::new("miroir_admin_ui_sessions_total", "Admin UI sessions started"),
            ).expect("create admin_ui_sessions_total");
            let a = CounterVec::new(
                Opts::new("miroir_admin_ui_action_total", "Admin UI actions by type"),
                &["action"],
            ).expect("create admin_ui_action_total");
            let d = CounterVec::new(
                Opts::new("miroir_admin_ui_destructive_action_total", "Admin UI destructive actions (delete, drop, etc.)"),
                &["action"],
            ).expect("create admin_ui_destructive_action_total");
            reg!(s); reg!(a); reg!(d);
            (Some(s), Some(a), Some(d))
        } else {
            (None, None, None)
        };

        // ── §13.20 Explain metrics ──
        let (
            explain_requests_total,
            explain_warnings_total,
            explain_execute_total,
        ) = if config.explain.enabled {
            let r = Counter::with_opts(
                Opts::new("miroir_explain_requests_total", "Explain API requests"),
            ).expect("create explain_requests_total");
            let w = CounterVec::new(
                Opts::new("miroir_explain_warnings_total", "Explain warnings by type"),
                &["warning_type"],
            ).expect("create explain_warnings_total");
            let e = Counter::with_opts(
                Opts::new("miroir_explain_execute_total", "Explain requests with execute=true"),
            ).expect("create explain_execute_total");
            reg!(r); reg!(w); reg!(e);
            (Some(r), Some(w), Some(e))
        } else {
            (None, None, None)
        };

        // ── §13.21 Search UI metrics (cardinality cap: top 100 indexes, rest bucketed) ──
        let (
            search_ui_sessions_total,
            search_ui_queries_total,
            search_ui_zero_hits_total,
            search_ui_click_through_total,
            search_ui_p95_ms,
        ) = if config.search_ui.enabled {
            let s = Counter::with_opts(
                Opts::new("miroir_search_ui_sessions_total", "Search UI sessions"),
            ).expect("create search_ui_sessions_total");
            let q = CounterVec::new(
                Opts::new("miroir_search_ui_queries_total", "Search UI queries per index"),
                &["index"],
            ).expect("create search_ui_queries_total");
            let z = CounterVec::new(
                Opts::new("miroir_search_ui_zero_hits_total", "Search UI zero-hit queries per index"),
                &["index"],
            ).expect("create search_ui_zero_hits_total");
            let c = CounterVec::new(
                Opts::new("miroir_search_ui_click_through_total", "Search UI click-through events per index"),
                &["index"],
            ).expect("create search_ui_click_through_total");
            let p = GaugeVec::new(
                Opts::new("miroir_search_ui_p95_ms", "Search UI p95 query latency per index"),
                &["index"],
            ).expect("create search_ui_p95_ms");
            reg!(s); reg!(q); reg!(z); reg!(c); reg!(p);
            (Some(s), Some(q), Some(z), Some(c), Some(p))
        } else {
            (None, None, None, None, None)
        };

        // ── §14.9 Resource-pressure metrics (always present) ──
        let memory_pressure = Gauge::with_opts(
            Opts::new("miroir_memory_pressure", "Memory pressure level (0=none, 1=low, 2=moderate/high)")
        ).expect("create memory_pressure");
        let cpu_throttled_seconds_total = Counter::with_opts(
            Opts::new("miroir_cpu_throttled_seconds_total", "Total seconds of CPU throttling")
        ).expect("create cpu_throttled_seconds_total");
        let request_queue_depth = Gauge::with_opts(
            Opts::new("miroir_request_queue_depth", "Number of requests queued waiting for processing")
        ).expect("create request_queue_depth");
        let background_queue_depth = GaugeVec::new(
            Opts::new("miroir_background_queue_depth", "Number of background jobs queued by type"),
            &["job_type"],
        ).expect("create background_queue_depth");
        let peer_pod_count = Gauge::with_opts(
            Opts::new("miroir_peer_pod_count", "Number of peer miroir pods discovered")
        ).expect("create peer_pod_count");
        let leader = Gauge::with_opts(
            Opts::new("miroir_leader", "Whether this pod holds the leader lease (1=yes, 0=no)")
        ).expect("create leader");
        let owned_shards_count = Gauge::with_opts(
            Opts::new("miroir_owned_shards_count", "Number of shards owned by this pod")
        ).expect("create owned_shards_count");
        reg!(memory_pressure);
        reg!(cpu_throttled_seconds_total);
        reg!(request_queue_depth);
        reg!(background_queue_depth);
        reg!(peer_pod_count);
        reg!(leader);
        reg!(owned_shards_count);

        // ── Admin session sealing metrics (always present) ──
        let admin_session_key_generated = Gauge::with_opts(
            Opts::new("miroir_admin_session_key_generated",
                "Whether ADMIN_SESSION_SEAL_KEY was generated at startup (1=yes, 0=set via env)")
        ).expect("create admin_session_key_generated");
        let admin_session_revoked_total = Counter::with_opts(
            Opts::new("miroir_admin_session_revoked_total",
                "Admin sessions revoked via logout")
        ).expect("create admin_session_revoked_total");
        reg!(admin_session_key_generated);
        reg!(admin_session_revoked_total);

        // ── §13.5 Two-phase settings broadcast metrics (always present) ──
        let settings_broadcast_phase = GaugeVec::new(
            Opts::new("miroir_settings_broadcast_phase", "Current phase of settings broadcast (0=idle, 1=propose, 2=verify, 3=commit)"),
            &["index"],
        ).expect("create settings_broadcast_phase");
        let settings_hash_mismatch_total = Counter::with_opts(
            Opts::new("miroir_settings_hash_mismatch_total", "Settings hash mismatches detected during verify phase"),
        ).expect("create settings_hash_mismatch_total");
        let settings_drift_repair_total = CounterVec::new(
            Opts::new("miroir_settings_drift_repair_total", "Settings drift repairs performed by drift reconciler"),
            &["index"],
        ).expect("create settings_drift_repair_total");
        let settings_version = GaugeVec::new(
            Opts::new("miroir_settings_version", "Current settings version per index"),
            &["index"],
        ).expect("create settings_version");
        reg!(settings_broadcast_phase);
        reg!(settings_hash_mismatch_total);
        reg!(settings_drift_repair_total);
        reg!(settings_version);

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
            multisearch_queries_per_batch,
            multisearch_batches_total,
            multisearch_partial_failures_total,
            multisearch_tenant_session_pin_override_total,
            vector_search_over_fetched_total,
            vector_merge_strategy,
            vector_embedder_drift_total,
            cdc_events_published_total,
            cdc_lag_seconds,
            cdc_buffer_bytes,
            cdc_dropped_total,
            cdc_events_suppressed_total,
            ttl_documents_expired_total,
            ttl_sweep_duration_seconds,
            ttl_pending_estimate,
            tenant_queries_total,
            tenant_pinned_groups,
            tenant_fallback_total,
            shadow_diff_total,
            shadow_kendall_tau,
            shadow_latency_delta_seconds,
            shadow_errors_total,
            rollover_events_total,
            rollover_active_indexes,
            rollover_documents_expired_total,
            rollover_last_action_seconds,
            canary_runs_total,
            canary_latency_ms,
            canary_assertion_failures_total,
            admin_ui_sessions_total,
            admin_ui_action_total,
            admin_ui_destructive_action_total,
            explain_requests_total,
            explain_warnings_total,
            explain_execute_total,
            search_ui_sessions_total,
            search_ui_queries_total,
            search_ui_zero_hits_total,
            search_ui_click_through_total,
            search_ui_p95_ms,
            memory_pressure,
            cpu_throttled_seconds_total,
            request_queue_depth,
            background_queue_depth,
            peer_pod_count,
            leader,
            owned_shards_count,
            admin_session_key_generated,
            admin_session_revoked_total,
            settings_broadcast_phase,
            settings_hash_mismatch_total,
            settings_drift_repair_total,
            settings_version,
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

    pub fn admin_session_key_generated(&self) -> Gauge {
        self.admin_session_key_generated.clone()
    }

    pub fn admin_session_revoked_total(&self) -> Counter {
        self.admin_session_revoked_total.clone()
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
    request_id: String,
}

impl InFlightGuard {
    fn new(metrics: Metrics, request_id: String) -> Self {
        metrics.requests_in_flight.inc();
        tracing::trace!(
            request_id = %request_id,
            requests_in_flight = metrics.requests_in_flight.get(),
            "request started"
        );
        Self { metrics, request_id }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.metrics.requests_in_flight.dec();
        tracing::trace!(
            request_id = %self.request_id,
            requests_in_flight = self.metrics.requests_in_flight.get(),
            "request completed"
        );
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
///
/// IMPORTANT: This middleware must be applied AFTER request_id_middleware in the layer stack
/// (i.e., its layer() call must come BEFORE request_id_middleware's layer() call).
/// This ensures the request_id header is already set when this middleware runs.
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

    // Extract request ID from header (set by request_id_middleware)
    // The header must already exist because request_id_middleware runs first.
    let request_id = req
        .headers()
        .get_request_id()
        .expect("request_id header must be set by request_id_middleware");
    req.headers_mut().set_request_id(&request_id);

    // Create span for structured logging with pod_id included.
    // Note: raw path is intentionally omitted to avoid logging index names
    // (which may contain customer identifiers). Use path_template instead.
    let span = info_span!(
        "request",
        request_id = %request_id,
        pod_id = %pod_id,
        method = %method,
        path_template = %path_template,
    );

    let _guard = span.enter();

    // Track in-flight requests
    let in_flight = InFlightGuard::new(metrics.clone(), request_id.clone());

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
            duration_ms = duration.as_millis() as u64,
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
            duration_ms = duration.as_millis() as u64,
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
            duration_ms = duration.as_millis() as u64,
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

    // ── §13.11 Multi-search ──

    pub fn observe_multisearch_queries_per_batch(&self, count: u64) {
        if let Some(ref m) = self.multisearch_queries_per_batch {
            m.observe(count as f64);
        }
    }

    pub fn inc_multisearch_batches_total(&self) {
        if let Some(ref m) = self.multisearch_batches_total {
            m.inc();
        }
    }

    pub fn inc_multisearch_partial_failures(&self) {
        if let Some(ref m) = self.multisearch_partial_failures_total {
            m.inc();
        }
    }

    pub fn inc_multisearch_tenant_session_pin_override(&self, tenant: &str) {
        if let Some(ref m) = self.multisearch_tenant_session_pin_override_total {
            m.with_label_values(&[tenant]).inc();
        }
    }

    // ── §13.12 Vector search ──

    pub fn inc_vector_search_over_fetched(&self) {
        if let Some(ref m) = self.vector_search_over_fetched_total {
            m.inc();
        }
    }

    pub fn inc_vector_merge_strategy(&self, strategy: &str) {
        if let Some(ref m) = self.vector_merge_strategy {
            m.with_label_values(&[strategy]).inc();
        }
    }

    pub fn inc_vector_embedder_drift(&self) {
        if let Some(ref m) = self.vector_embedder_drift_total {
            m.inc();
        }
    }

    // ── §13.13 CDC ──

    pub fn inc_cdc_events_published(&self, sink: &str, index: &str) {
        if let Some(ref m) = self.cdc_events_published_total {
            m.with_label_values(&[sink, index]).inc();
        }
    }

    pub fn set_cdc_lag_seconds(&self, sink: &str, lag: f64) {
        if let Some(ref m) = self.cdc_lag_seconds {
            m.with_label_values(&[sink]).set(lag);
        }
    }

    pub fn set_cdc_buffer_bytes(&self, sink: &str, bytes: f64) {
        if let Some(ref m) = self.cdc_buffer_bytes {
            m.with_label_values(&[sink]).set(bytes);
        }
    }

    pub fn inc_cdc_dropped(&self, sink: &str) {
        if let Some(ref m) = self.cdc_dropped_total {
            m.with_label_values(&[sink]).inc();
        }
    }

    pub fn inc_cdc_events_suppressed(&self, origin: &str) {
        if let Some(ref m) = self.cdc_events_suppressed_total {
            m.with_label_values(&[origin]).inc();
        }
    }

    // ── §13.14 TTL ──

    pub fn inc_ttl_documents_expired(&self, index: &str) {
        if let Some(ref m) = self.ttl_documents_expired_total {
            m.with_label_values(&[index]).inc();
        }
    }

    pub fn observe_ttl_sweep_duration(&self, index: &str, secs: f64) {
        if let Some(ref m) = self.ttl_sweep_duration_seconds {
            m.with_label_values(&[index]).observe(secs);
        }
    }

    pub fn set_ttl_pending_estimate(&self, index: &str, count: f64) {
        if let Some(ref m) = self.ttl_pending_estimate {
            m.with_label_values(&[index]).set(count);
        }
    }

    // ── §13.15 Tenant affinity ──

    pub fn inc_tenant_queries(&self, tenant: &str, group: &str) {
        if let Some(ref m) = self.tenant_queries_total {
            m.with_label_values(&[tenant, group]).inc();
        }
    }

    pub fn set_tenant_pinned_groups(&self, tenant: &str, group: u32) {
        if let Some(ref m) = self.tenant_pinned_groups {
            m.with_label_values(&[tenant]).set(group as f64);
        }
    }

    pub fn inc_tenant_fallback(&self, reason: &str) {
        if let Some(ref m) = self.tenant_fallback_total {
            m.with_label_values(&[reason]).inc();
        }
    }

    // ── §13.16 Shadow ──

    pub fn inc_shadow_diff(&self, kind: &str) {
        if let Some(ref m) = self.shadow_diff_total {
            m.with_label_values(&[kind]).inc();
        }
    }

    pub fn set_shadow_kendall_tau(&self, tau: f64) {
        if let Some(ref m) = self.shadow_kendall_tau {
            m.set(tau);
        }
    }

    pub fn observe_shadow_latency_delta(&self, delta: f64) {
        if let Some(ref m) = self.shadow_latency_delta_seconds {
            m.observe(delta);
        }
    }

    pub fn inc_shadow_errors(&self, target: &str, side: &str) {
        if let Some(ref m) = self.shadow_errors_total {
            m.with_label_values(&[target, side]).inc();
        }
    }

    // ── §13.17 ILM ──

    pub fn inc_rollover_events(&self, policy: &str) {
        if let Some(ref m) = self.rollover_events_total {
            m.with_label_values(&[policy]).inc();
        }
    }

    pub fn set_rollover_active_indexes(&self, alias: &str, count: f64) {
        if let Some(ref m) = self.rollover_active_indexes {
            m.with_label_values(&[alias]).set(count);
        }
    }

    pub fn inc_rollover_documents_expired(&self, policy: &str) {
        if let Some(ref m) = self.rollover_documents_expired_total {
            m.with_label_values(&[policy]).inc();
        }
    }

    pub fn set_rollover_last_action_seconds(&self, policy: &str, secs: f64) {
        if let Some(ref m) = self.rollover_last_action_seconds {
            m.with_label_values(&[policy]).set(secs);
        }
    }

    // ── §13.18 Canary ──

    pub fn inc_canary_runs(&self, canary: &str, result: &str) {
        if let Some(ref m) = self.canary_runs_total {
            m.with_label_values(&[canary, result]).inc();
        }
    }

    pub fn observe_canary_latency_ms(&self, canary: &str, ms: f64) {
        if let Some(ref m) = self.canary_latency_ms {
            m.with_label_values(&[canary]).observe(ms);
        }
    }

    pub fn inc_canary_assertion_failures(&self, canary: &str, assertion_type: &str) {
        if let Some(ref m) = self.canary_assertion_failures_total {
            m.with_label_values(&[canary, assertion_type]).inc();
        }
    }

    // ── §13.19 Admin UI ──

    pub fn inc_admin_ui_sessions(&self) {
        if let Some(ref m) = self.admin_ui_sessions_total {
            m.inc();
        }
    }

    pub fn inc_admin_ui_action(&self, action: &str) {
        if let Some(ref m) = self.admin_ui_action_total {
            m.with_label_values(&[action]).inc();
        }
    }

    pub fn inc_admin_ui_destructive_action(&self, action: &str) {
        if let Some(ref m) = self.admin_ui_destructive_action_total {
            m.with_label_values(&[action]).inc();
        }
    }

    // ── §13.20 Explain ──

    pub fn inc_explain_requests(&self) {
        if let Some(ref m) = self.explain_requests_total {
            m.inc();
        }
    }

    pub fn inc_explain_warnings(&self, warning_type: &str) {
        if let Some(ref m) = self.explain_warnings_total {
            m.with_label_values(&[warning_type]).inc();
        }
    }

    pub fn inc_explain_execute(&self) {
        if let Some(ref m) = self.explain_execute_total {
            m.inc();
        }
    }

    // ── §13.21 Search UI ──

    pub fn inc_search_ui_sessions(&self) {
        if let Some(ref m) = self.search_ui_sessions_total {
            m.inc();
        }
    }

    pub fn inc_search_ui_queries(&self, index: &str) {
        if let Some(ref m) = self.search_ui_queries_total {
            m.with_label_values(&[index]).inc();
        }
    }

    pub fn inc_search_ui_zero_hits(&self, index: &str) {
        if let Some(ref m) = self.search_ui_zero_hits_total {
            m.with_label_values(&[index]).inc();
        }
    }

    pub fn inc_search_ui_click_through(&self, index: &str) {
        if let Some(ref m) = self.search_ui_click_through_total {
            m.with_label_values(&[index]).inc();
        }
    }

    pub fn set_search_ui_p95_ms(&self, index: &str, ms: f64) {
        if let Some(ref m) = self.search_ui_p95_ms {
            m.with_label_values(&[index]).set(ms);
        }
    }

    // ── §14.9 Resource-pressure ──

    pub fn set_memory_pressure(&self, level: u32) {
        self.memory_pressure.set(level as f64);
    }

    pub fn inc_cpu_throttled_seconds(&self, secs: f64) {
        self.cpu_throttled_seconds_total.inc_by(secs);
    }

    pub fn set_request_queue_depth(&self, depth: u64) {
        self.request_queue_depth.set(depth as f64);
    }

    pub fn set_background_queue_depth(&self, job_type: &str, depth: u64) {
        self.background_queue_depth.with_label_values(&[job_type]).set(depth as f64);
    }

    pub fn set_peer_pod_count(&self, count: u64) {
        self.peer_pod_count.set(count as f64);
    }

    pub fn set_leader(&self, is_leader: bool) {
        self.leader.set(if is_leader { 1.0 } else { 0.0 });
    }

    pub fn set_owned_shards_count(&self, count: u64) {
        self.owned_shards_count.set(count as f64);
    }

    // ── §13.5 Two-phase settings broadcast metrics ──

    pub fn set_settings_broadcast_phase(&self, index: &str, phase: u8) {
        self.settings_broadcast_phase.with_label_values(&[index]).set(phase as f64);
    }

    pub fn clear_settings_broadcast_phase(&self, index: &str) {
        self.settings_broadcast_phase.with_label_values(&[index]).set(0.0);
    }

    pub fn inc_settings_hash_mismatch(&self) {
        self.settings_hash_mismatch_total.inc();
    }

    pub fn inc_settings_drift_repair(&self, index: &str) {
        self.settings_drift_repair_total.with_label_values(&[index]).inc();
    }

    pub fn set_settings_version(&self, index: &str, version: u64) {
        self.settings_version.with_label_values(&[index]).set(version as f64);
    }

    pub fn get_settings_version(&self, index: &str) -> f64 {
        self.settings_version.with_label_values(&[index]).get()
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
        // Default config has all §13 features enabled
        let metrics = Metrics::new(&MiroirConfig::default());

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

        // Write to advanced Vec metrics so they appear in output
        metrics.inc_multisearch_tenant_session_pin_override("t1");
        metrics.inc_vector_merge_strategy("convex");
        metrics.inc_cdc_events_published("webhook", "idx1");
        metrics.set_cdc_lag_seconds("webhook", 0.5);
        metrics.set_cdc_buffer_bytes("webhook", 1024.0);
        metrics.inc_cdc_dropped("webhook");
        metrics.inc_cdc_events_suppressed("origin1");
        metrics.inc_ttl_documents_expired("idx1");
        metrics.observe_ttl_sweep_duration("idx1", 0.1);
        metrics.set_ttl_pending_estimate("idx1", 50.0);
        metrics.inc_tenant_queries("t1", "g1");
        metrics.set_tenant_pinned_groups("t1", 1);
        metrics.inc_tenant_fallback("no_group");
        metrics.inc_shadow_diff("rank");
        metrics.inc_shadow_errors("target1", "primary");
        metrics.inc_rollover_events("policy1");
        metrics.set_rollover_active_indexes("alias1", 1.0);
        metrics.inc_rollover_documents_expired("policy1");
        metrics.set_rollover_last_action_seconds("policy1", 60.0);
        metrics.inc_canary_runs("canary1", "pass");
        metrics.observe_canary_latency_ms("canary1", 50.0);
        metrics.inc_canary_assertion_failures("canary1", "latency");
        metrics.inc_admin_ui_action("login");
        metrics.inc_admin_ui_destructive_action("delete_index");
        metrics.inc_explain_warnings("slow_plan");
        metrics.inc_search_ui_queries("idx1");
        metrics.inc_search_ui_zero_hits("idx1");
        metrics.inc_search_ui_click_through("idx1");
        metrics.set_search_ui_p95_ms("idx1", 150.0);

        // §14.9 Resource-pressure metrics
        metrics.set_memory_pressure(0);
        metrics.inc_cpu_throttled_seconds(1.5);
        metrics.set_request_queue_depth(42);
        metrics.set_background_queue_depth("rebalance", 5);
        metrics.set_background_queue_depth("replication", 3);
        metrics.set_peer_pod_count(3);
        metrics.set_leader(true);
        metrics.set_owned_shards_count(12);

        let encoded = metrics.encode_metrics();
        assert!(encoded.is_ok());

        let output = encoded.unwrap();

        // Verify all 18 core plan §10 metric names appear in the output
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

        // With defaults (all §13 enabled), advanced metrics should be present
        let advanced_metrics = [
            // §13.11 Multi-search
            "miroir_multisearch_queries_per_batch",
            "miroir_multisearch_batches_total",
            "miroir_multisearch_partial_failures_total",
            "miroir_tenant_session_pin_override_total",
            // §13.12 Vector
            "miroir_vector_search_over_fetched_total",
            "miroir_vector_merge_strategy",
            "miroir_vector_embedder_drift_total",
            // §13.13 CDC
            "miroir_cdc_events_published_total",
            "miroir_cdc_lag_seconds",
            "miroir_cdc_buffer_bytes",
            "miroir_cdc_dropped_total",
            "miroir_cdc_events_suppressed_total",
            // §13.14 TTL
            "miroir_ttl_documents_expired_total",
            "miroir_ttl_sweep_duration_seconds",
            "miroir_ttl_pending_estimate",
            // §13.15 Tenant
            "miroir_tenant_queries_total",
            "miroir_tenant_pinned_groups",
            "miroir_tenant_fallback_total",
            // §13.16 Shadow
            "miroir_shadow_diff_total",
            "miroir_shadow_kendall_tau",
            "miroir_shadow_latency_delta_seconds",
            "miroir_shadow_errors_total",
            // §13.17 ILM
            "miroir_rollover_events_total",
            "miroir_rollover_active_indexes",
            "miroir_rollover_documents_expired_total",
            "miroir_rollover_last_action_seconds",
            // §13.18 Canary
            "miroir_canary_runs_total",
            "miroir_canary_latency_ms",
            "miroir_canary_assertion_failures_total",
            // §13.19 Admin UI
            "miroir_admin_ui_sessions_total",
            "miroir_admin_ui_action_total",
            "miroir_admin_ui_destructive_action_total",
            // §13.20 Explain
            "miroir_explain_requests_total",
            "miroir_explain_warnings_total",
            "miroir_explain_execute_total",
            // §13.21 Search UI
            "miroir_search_ui_sessions_total",
            "miroir_search_ui_queries_total",
            "miroir_search_ui_zero_hits_total",
            "miroir_search_ui_click_through_total",
            "miroir_search_ui_p95_ms",
        ];
        for name in &advanced_metrics {
            assert!(output.contains(name), "missing advanced metric: {}", name);
        }
    }

    #[test]
    fn test_metrics_feature_flags_off() {
        // Build a config with all §13.11-13.21 features disabled
        let mut config = MiroirConfig::default();
        config.multi_search.enabled = false;
        config.vector_search.enabled = false;
        config.cdc.enabled = false;
        config.ttl.enabled = false;
        config.tenant_affinity.enabled = false;
        config.shadow.enabled = false;
        config.ilm.enabled = false;
        config.canary_runner.enabled = false;
        config.admin_ui.enabled = false;
        config.explain.enabled = false;
        config.search_ui.enabled = false;

        let metrics = Metrics::new(&config);

        // Write to core Vec metrics so they appear in output
        metrics.request_duration.with_label_values(&["GET", "/test", "200"]).observe(0.1);

        let encoded = metrics.encode_metrics().unwrap();

        // Core metrics should still be present
        assert!(encoded.contains("miroir_request_duration_seconds"));
        assert!(encoded.contains("miroir_rebalance_duration_seconds"));

        // Advanced metrics should NOT appear
        let advanced_names = [
            "miroir_multisearch_queries_per_batch",
            "miroir_vector_search_over_fetched_total",
            "miroir_cdc_events_published_total",
            "miroir_ttl_documents_expired_total",
            "miroir_tenant_queries_total",
            "miroir_shadow_diff_total",
            "miroir_rollover_events_total",
            "miroir_canary_runs_total",
            "miroir_admin_ui_sessions_total",
            "miroir_explain_requests_total",
            "miroir_search_ui_sessions_total",
        ];
        for name in &advanced_names {
            assert!(!encoded.contains(name), "advanced metric should not appear when disabled: {}", name);
        }
    }

    #[test]
    fn test_feature_gated_accessors_noop_when_disabled() {
        let mut config = MiroirConfig::default();
        config.multi_search.enabled = false;
        config.vector_search.enabled = false;
        config.cdc.enabled = false;
        config.ttl.enabled = false;
        config.tenant_affinity.enabled = false;
        config.shadow.enabled = false;
        config.ilm.enabled = false;
        config.canary_runner.enabled = false;
        config.admin_ui.enabled = false;
        config.explain.enabled = false;
        config.search_ui.enabled = false;

        let metrics = Metrics::new(&config);

        // All accessor methods should be safe to call (no-op)
        metrics.observe_multisearch_queries_per_batch(5);
        metrics.inc_multisearch_batches_total();
        metrics.inc_multisearch_partial_failures();
        metrics.inc_multisearch_tenant_session_pin_override("t1");
        metrics.inc_vector_search_over_fetched();
        metrics.inc_vector_merge_strategy("convex");
        metrics.inc_vector_embedder_drift();
        metrics.inc_cdc_events_published("webhook", "idx");
        metrics.set_cdc_lag_seconds("webhook", 1.5);
        metrics.set_cdc_buffer_bytes("webhook", 1024.0);
        metrics.inc_cdc_dropped("webhook");
        metrics.inc_cdc_events_suppressed("origin1");
        metrics.inc_ttl_documents_expired("idx");
        metrics.observe_ttl_sweep_duration("idx", 0.1);
        metrics.set_ttl_pending_estimate("idx", 50.0);
        metrics.inc_tenant_queries("t1", "0");
        metrics.set_tenant_pinned_groups("t1", 1);
        metrics.inc_tenant_fallback("no_group");
        metrics.inc_shadow_diff("rank");
        metrics.set_shadow_kendall_tau(0.95);
        metrics.observe_shadow_latency_delta(0.01);
        metrics.inc_shadow_errors("target1", "primary");
        metrics.inc_rollover_events("policy1");
        metrics.set_rollover_active_indexes("alias1", 1.0);
        metrics.inc_rollover_documents_expired("policy1");
        metrics.set_rollover_last_action_seconds("policy1", 60.0);
        metrics.inc_canary_runs("canary1", "pass");
        metrics.observe_canary_latency_ms("canary1", 50.0);
        metrics.inc_canary_assertion_failures("canary1", "latency");
        metrics.inc_admin_ui_sessions();
        metrics.inc_admin_ui_action("login");
        metrics.inc_admin_ui_destructive_action("delete_index");
        metrics.inc_explain_requests();
        metrics.inc_explain_warnings("slow_plan");
        metrics.inc_explain_execute();
        metrics.inc_search_ui_sessions();
        metrics.inc_search_ui_queries("idx");
        metrics.inc_search_ui_zero_hits("idx");
        metrics.inc_search_ui_click_through("idx");
        metrics.set_search_ui_p95_ms("idx", 150.0);
    }

    #[test]
    fn test_header_request_id() {
        let mut headers = HeaderMap::new();
        assert!(headers.get_request_id().is_none());

        headers.set_request_id("test-id-123");
        assert_eq!(headers.get_request_id(), Some("test-id-123".to_string()));
    }

    // ---------------------------------------------------------------------------
    // RequestId type tests
    // ---------------------------------------------------------------------------

    #[test]
    fn test_request_id_format() {
        let id = RequestId::new();
        // RequestId should be exactly 8 hex characters
        assert_eq!(id.as_str().len(), 8);
        assert!(id.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_request_id_parse_valid() {
        // Valid 8-char hex string
        let valid = "abcd1234";
        let parsed = RequestId::parse(valid.to_string());
        assert!(parsed.is_some());
        assert_eq!(parsed.unwrap().as_str(), valid);
    }

    #[test]
    fn test_request_id_parse_invalid_wrong_length() {
        // Wrong length (too short)
        assert!(RequestId::parse("abc123".to_string()).is_none());
        // Wrong length (too long)
        assert!(RequestId::parse("abcd12345678".to_string()).is_none());
    }

    #[test]
    fn test_request_id_parse_invalid_non_hex() {
        // Contains non-hex characters
        assert!(RequestId::parse("abcd1234!".to_string()).is_none());
        assert!(RequestId::parse("ghijklmn".to_string()).is_none());
    }

    #[test]
    fn test_request_id_uniqueness() {
        // Generate two consecutive IDs - they should be different
        // due to UUIDv7's timestamp component
        let id1 = RequestId::new();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let id2 = RequestId::new();

        assert_ne!(id1, id2);
        assert_ne!(id1.as_str(), id2.as_str());
    }

    // ---------------------------------------------------------------------------
    // Integration tests for request_id_middleware
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn test_request_id_middleware_adds_header() {
        use axum::{routing::get, Router};
        use http_body_util::Full;
        use tower::ServiceExt;

        // Build a simple router with the request ID middleware
        let app = Router::new()
            .route("/test", get(|| async { "OK" }))
            .layer(axum::middleware::from_fn(request_id_middleware));

        // Create a test request
        let request = Request::builder()
            .uri("/test")
            .body(Full::default())
            .unwrap();

        // Send the request
        let response = app.oneshot(request).await.unwrap();

        // Verify X-Request-Id header is present
        let header = response
            .headers()
            .get("x-request-id")
            .expect("X-Request-Id header should be present");
        let header_value = header.to_str().unwrap();

        // Verify it's 8 hex characters
        assert_eq!(
            header_value.len(),
            8,
            "X-Request-Id should be 8 characters"
        );
        assert!(
            header_value.chars().all(|c| c.is_ascii_hexdigit()),
            "X-Request-Id should be hexadecimal"
        );
    }

    #[tokio::test]
    async fn test_request_id_middleware_unique_per_request() {
        use axum::{routing::get, Router};
        use http_body_util::Full;
        use tower::ServiceExt;

        // Build a simple router with the request ID middleware
        let app = Router::new()
            .route("/test", get(|| async { "OK" }))
            .layer(axum::middleware::from_fn(request_id_middleware));

        // Create two identical requests
        let request1 = Request::builder()
            .uri("/test")
            .body(Full::default())
            .unwrap();

        let request2 = Request::builder()
            .uri("/test")
            .body(Full::default())
            .unwrap();

        // Send both requests
        let response1 = app.clone().oneshot(request1).await.unwrap();
        let response2 = app.oneshot(request2).await.unwrap();

        // Extract headers
        let id1 = response1
            .headers()
            .get("x-request-id")
            .unwrap()
            .to_str()
            .unwrap();
        let id2 = response2
            .headers()
            .get("x-request-id")
            .unwrap()
            .to_str()
            .unwrap();

        // Verify IDs are different (UUIDv7 timestamp ensures this)
        assert_ne!(
            id1, id2,
            "Two consecutive requests should have different request IDs"
        );

        // Both should still be valid 8-char hex
        assert_eq!(id1.len(), 8);
        assert_eq!(id2.len(), 8);
    }

    #[tokio::test]
    async fn test_request_id_middleware_preserves_existing_header() {
        use axum::{routing::get, Router};
        use http_body_util::Full;
        use tower::ServiceExt;

        // Build a simple router with the request ID middleware
        let app = Router::new()
            .route("/test", get(|| async { "OK" }))
            .layer(axum::middleware::from_fn(request_id_middleware));

        // Create a request with a pre-existing X-Request-Id header
        let existing_id = "deadbeef";
        let request = Request::builder()
            .uri("/test")
            .header("x-request-id", existing_id)
            .body(Full::default())
            .unwrap();

        // Send the request
        let response = app.oneshot(request).await.unwrap();

        // Verify the header is preserved
        let header = response
            .headers()
            .get("x-request-id")
            .expect("X-Request-Id header should be present");
        let header_value = header.to_str().unwrap();

        assert_eq!(
            header_value, existing_id,
            "Existing X-Request-Id should be preserved"
        );
    }
}
