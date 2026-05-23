//! §13 Advanced capabilities configuration structs.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// 13.1  Online resharding
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReshardingConfig {
    pub enabled: bool,
    pub backfill_concurrency: u32,
    pub backfill_batch_size: u32,
    pub throttle_docs_per_sec: u32,
    pub verify_before_swap: bool,
    pub retain_old_index_hours: u32,
    /// Allowed schedule windows in `"HH:MM-HH:MM UTC"` format.
    /// Empty means any time is allowed (no restriction).
    pub allowed_windows: Vec<String>,
}

impl Default for ReshardingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            backfill_concurrency: 4,
            backfill_batch_size: 1000,
            throttle_docs_per_sec: 0,
            verify_before_swap: true,
            retain_old_index_hours: 48,
            allowed_windows: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// 13.2  Hedged requests
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct HedgingConfig {
    pub enabled: bool,
    pub p95_trigger_multiplier: f64,
    pub min_trigger_ms: u64,
    pub max_hedges_per_query: u32,
    pub cross_group_fallback: bool,
}

impl Default for HedgingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            p95_trigger_multiplier: 1.2,
            min_trigger_ms: 15,
            max_hedges_per_query: 2,
            cross_group_fallback: true,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.3  Adaptive replica selection (EWMA)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplicaSelectionConfig {
    /// `adaptive`, `round_robin`, or `random`.
    pub strategy: String,
    pub latency_weight: f64,
    pub inflight_weight: f64,
    pub error_weight: f64,
    pub ewma_half_life_ms: u64,
    pub exploration_epsilon: f64,
}

impl Default for ReplicaSelectionConfig {
    fn default() -> Self {
        Self {
            strategy: "adaptive".into(),
            latency_weight: 1.0,
            inflight_weight: 2.0,
            error_weight: 10.0,
            ewma_half_life_ms: 5000,
            exploration_epsilon: 0.05,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.4  Shard-aware query planner
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryPlannerConfig {
    pub enabled: bool,
    pub max_pk_literals_narrowable: u32,
    pub log_plans: bool,
}

impl Default for QueryPlannerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_pk_literals_narrowable: 128,
            log_plans: false,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.5  Two-phase settings broadcast
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SettingsBroadcastConfig {
    /// `two_phase` or `sequential` (legacy).
    pub strategy: String,
    pub verify_timeout_s: u64,
    pub max_repair_retries: u32,
    pub freeze_writes_on_unrepairable: bool,
}

impl Default for SettingsBroadcastConfig {
    fn default() -> Self {
        Self {
            strategy: "two_phase".into(),
            verify_timeout_s: 60,
            max_repair_retries: 3,
            freeze_writes_on_unrepairable: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SettingsDriftCheckConfig {
    pub interval_s: u64,
    pub auto_repair: bool,
}

impl Default for SettingsDriftCheckConfig {
    fn default() -> Self {
        Self {
            interval_s: 300,
            auto_repair: true,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.6  Session pinning (read-your-writes)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionPinningConfig {
    pub enabled: bool,
    pub ttl_seconds: u64,
    pub max_sessions: u32,
    /// `block` or `route_pin`.
    pub wait_strategy: String,
    pub max_wait_ms: u64,
}

impl Default for SessionPinningConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_seconds: 900,
            max_sessions: 100_000,
            wait_strategy: "block".into(),
            max_wait_ms: 5000,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.7  Index aliases
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AliasesConfig {
    pub enabled: bool,
    pub history_retention: u32,
    pub require_target_exists: bool,
}

impl Default for AliasesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            history_retention: 10,
            require_target_exists: true,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.8  Anti-entropy shard reconciler
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AntiEntropyConfig {
    pub enabled: bool,
    pub schedule: String,
    pub shards_per_pass: u32,
    pub max_read_concurrency: u32,
    pub fingerprint_batch_size: u32,
    pub auto_repair: bool,
    pub updated_at_field: String,
    /// TTL expires_at field name (plan §13.14 interaction).
    pub expires_at_field: String,
    /// Whether TTL is enabled (plan §13.14 interaction).
    pub ttl_enabled: bool,
}

impl Default for AntiEntropyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            schedule: "every 6h".into(),
            shards_per_pass: 0,
            max_read_concurrency: 2,
            fingerprint_batch_size: 1000,
            auto_repair: true,
            updated_at_field: "_miroir_updated_at".into(),
            expires_at_field: "_miroir_expires_at".into(),
            ttl_enabled: false,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.9  Streaming dump import
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DumpImportConfig {
    /// `streaming` or `broadcast` (legacy).
    pub mode: String,
    pub batch_size: u32,
    pub parallel_target_writes: u32,
    pub memory_buffer_bytes: u64,
    pub chunk_size_bytes: u64,
}

impl Default for DumpImportConfig {
    fn default() -> Self {
        Self {
            mode: "streaming".into(),
            batch_size: 1000,
            parallel_target_writes: 8,
            memory_buffer_bytes: 134_217_728, // 128 MiB
            chunk_size_bytes: 268_435_456,    // 256 MiB
        }
    }
}

// ---------------------------------------------------------------------------
// 13.10  Idempotency keys
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct IdempotencyConfig {
    pub enabled: bool,
    pub ttl_seconds: u64,
    pub max_cached_keys: u32,
}

impl Default for IdempotencyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_seconds: 86400,
            max_cached_keys: 1_000_000,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.10  Query coalescing (paired with idempotency)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryCoalescingConfig {
    pub enabled: bool,
    pub window_ms: u64,
    pub max_subscribers: u32,
    pub max_pending_queries: u32,
}

impl Default for QueryCoalescingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            window_ms: 50,
            max_subscribers: 1000,
            max_pending_queries: 10000,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.11  Multi-search batch API
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MultiSearchConfig {
    pub enabled: bool,
    pub max_queries_per_batch: u32,
    pub total_timeout_ms: u64,
    pub per_query_timeout_ms: u64,
}

impl Default for MultiSearchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_queries_per_batch: 100,
            total_timeout_ms: 30000,
            per_query_timeout_ms: 30000,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.12  Vector / hybrid search
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct VectorSearchConfig {
    pub enabled: bool,
    pub over_fetch_factor: u32,
    /// `convex` or `rrf`.
    pub merge_strategy: String,
    pub hybrid_alpha_default: f64,
    pub rrf_k: u32,
}

impl Default for VectorSearchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            over_fetch_factor: 3,
            merge_strategy: "convex".into(),
            hybrid_alpha_default: 0.5,
            rrf_k: 60,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.13  Change data capture (CDC)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CdcConfig {
    pub enabled: bool,
    pub emit_ttl_deletes: bool,
    pub emit_internal_writes: bool,
    pub sinks: Vec<CdcSinkConfig>,
    pub buffer: CdcBufferConfig,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            emit_ttl_deletes: false,
            emit_internal_writes: false,
            sinks: Vec::new(),
            buffer: CdcBufferConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CdcSinkConfig {
    /// `webhook`, `nats`, `kafka`, or `internal`.
    #[serde(rename = "type")]
    pub sink_type: String,
    pub url: String,
    pub batch_size: u32,
    pub batch_flush_ms: u64,
    pub include_body: bool,
    pub retry_max_s: u64,
    /// NATS-specific.
    pub subject_prefix: Option<String>,
}

impl Default for CdcSinkConfig {
    fn default() -> Self {
        Self {
            sink_type: "webhook".into(),
            url: String::new(),
            batch_size: 100,
            batch_flush_ms: 1000,
            include_body: false,
            retry_max_s: 3600,
            subject_prefix: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CdcBufferConfig {
    /// `memory`, `redis`, or `pvc`.
    pub primary: String,
    pub memory_bytes: u64,
    /// `redis`, `pvc`, or `drop`.
    pub overflow: String,
    pub redis_bytes: u64,
}

impl Default for CdcBufferConfig {
    fn default() -> Self {
        Self {
            primary: "memory".into(),
            memory_bytes: 67_108_864, // 64 MiB
            overflow: "redis".into(),
            redis_bytes: 1_073_741_824, // 1 GiB
        }
    }
}

// ---------------------------------------------------------------------------
// 13.14  Document TTL
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TtlConfig {
    pub enabled: bool,
    pub sweep_interval_s: u64,
    pub max_deletes_per_sweep: u32,
    pub expires_at_field: String,
    pub per_index_overrides: HashMap<String, TtlOverride>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TtlOverride {
    pub sweep_interval_s: u64,
    pub max_deletes_per_sweep: u32,
}

impl Default for TtlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sweep_interval_s: 300,
            max_deletes_per_sweep: 10000,
            expires_at_field: "_miroir_expires_at".into(),
            per_index_overrides: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// 13.15  Tenant-to-replica-group affinity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TenantAffinityConfig {
    pub enabled: bool,
    /// `header`, `api_key`, or `explicit`.
    pub mode: String,
    pub header_name: String,
    /// `hash`, `random`, or `reject`.
    pub fallback: String,
    pub static_map: HashMap<String, u32>,
    pub dedicated_groups: Vec<u32>,
}

impl Default for TenantAffinityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: "header".into(),
            header_name: "X-Miroir-Tenant".into(),
            fallback: "hash".into(),
            static_map: HashMap::new(),
            dedicated_groups: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// 13.16  Traffic shadow
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ShadowConfig {
    pub enabled: bool,
    pub targets: Vec<ShadowTargetConfig>,
    pub diff_buffer_size: u32,
    pub max_shadow_latency_ms: u64,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            targets: Vec::new(),
            diff_buffer_size: 10000,
            max_shadow_latency_ms: 5000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ShadowTargetConfig {
    pub name: String,
    pub url: String,
    pub api_key_env: String,
    pub sample_rate: f64,
    pub operations: Vec<String>,
}

impl Default for ShadowTargetConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            url: String::new(),
            api_key_env: String::new(),
            sample_rate: 0.05,
            operations: vec!["search".into(), "multi_search".into(), "explain".into()],
        }
    }
}

// ---------------------------------------------------------------------------
// 13.17  Index lifecycle management (ILM)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct IlmConfig {
    pub enabled: bool,
    pub check_interval_s: u64,
    pub safety_lock_older_than_days: u32,
    pub max_rollovers_per_check: u32,
}

impl Default for IlmConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval_s: 3600,
            safety_lock_older_than_days: 7,
            max_rollovers_per_check: 10,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.18  Synthetic canary queries
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CanaryRunnerConfig {
    pub enabled: bool,
    pub max_concurrent_canaries: u32,
    pub run_history_per_canary: u32,
    pub emit_results_to_cdc: bool,
}

impl Default for CanaryRunnerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent_canaries: 10,
            run_history_per_canary: 100,
            emit_results_to_cdc: true,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.19  Admin Web UI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AdminUiRateLimitConfig {
    pub per_ip: String,
    /// `redis` or `local`.
    pub backend: String,
    pub redis_key_prefix: String,
    pub redis_ttl_s: u64,
    pub failed_attempt_threshold: u32,
    pub backoff_start_minutes: u64,
    pub backoff_max_hours: u64,
}

impl Default for AdminUiRateLimitConfig {
    fn default() -> Self {
        Self {
            per_ip: "10/minute".into(),
            backend: "redis".into(),
            redis_key_prefix: "miroir:ratelimit:adminlogin:".into(),
            redis_ttl_s: 60,
            failed_attempt_threshold: 5,
            backoff_start_minutes: 10,
            backoff_max_hours: 24,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AdminUiConfig {
    pub enabled: bool,
    pub path: String,
    /// `key`, `oauth` (future), or `none` (dev only).
    pub auth: String,
    pub session_ttl_s: u64,
    pub read_only_mode: bool,
    pub allowed_origins: Vec<String>,
    pub cors_allowed_origins: Vec<String>,
    pub csp: String,
    pub csp_overrides: CspOverridesConfig,
    pub theme: AdminUiThemeConfig,
    pub features: AdminUiFeaturesConfig,
    pub rate_limit: AdminUiRateLimitConfig,
}

impl Default for AdminUiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: "/_miroir/admin".into(),
            auth: "key".into(),
            session_ttl_s: 3600,
            read_only_mode: false,
            allowed_origins: vec!["same-origin".into()],
            cors_allowed_origins: Vec::new(),
            csp: "default-src 'self'; script-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'".into(),
            csp_overrides: CspOverridesConfig::default(),
            theme: AdminUiThemeConfig::default(),
            features: AdminUiFeaturesConfig::default(),
            rate_limit: AdminUiRateLimitConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct CspOverridesConfig {
    pub script_src: Vec<String>,
    pub img_src: Vec<String>,
    pub connect_src: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AdminUiThemeConfig {
    pub accent_color: String,
    /// `auto`, `light`, or `dark`.
    pub default_mode: String,
}

impl Default for AdminUiThemeConfig {
    fn default() -> Self {
        Self {
            accent_color: "#2563eb".into(),
            default_mode: "auto".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AdminUiFeaturesConfig {
    pub sandbox: bool,
    pub shadow_viewer: bool,
    pub cdc_inspector: bool,
}

impl Default for AdminUiFeaturesConfig {
    fn default() -> Self {
        Self {
            sandbox: true,
            shadow_viewer: true,
            cdc_inspector: true,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.20  Query explain API
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ExplainConfig {
    pub enabled: bool,
    pub max_warnings: u32,
    pub allow_execute_parameter: bool,
}

impl Default for ExplainConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_warnings: 20,
            allow_execute_parameter: true,
        }
    }
}

// ---------------------------------------------------------------------------
// 13.21  Search UI (end-user)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchUiConfig {
    pub enabled: bool,
    pub path: String,
    pub widget_script_enabled: bool,
    pub embeddable: bool,
    pub auth: SearchUiAuthConfig,
    pub allowed_origins: Vec<String>,
    pub scoped_key_max_age_days: u32,
    pub scoped_key_rotate_before_expiry_days: u32,
    pub scoped_key_rotation_drain_s: u64,
    pub rate_limit: SearchUiRateLimitConfig,
    pub cors_allowed_origins: Vec<String>,
    pub csp_overrides: CspOverridesConfig,
    pub csp: String,
    pub analytics: SearchUiAnalyticsConfig,
}

impl Default for SearchUiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: "/ui/search".into(),
            widget_script_enabled: true,
            embeddable: true,
            auth: SearchUiAuthConfig::default(),
            allowed_origins: vec!["*".into()],
            scoped_key_max_age_days: 60,
            scoped_key_rotate_before_expiry_days: 30,
            scoped_key_rotation_drain_s: 120,
            rate_limit: SearchUiRateLimitConfig::default(),
            cors_allowed_origins: Vec::new(),
            csp_overrides: CspOverridesConfig::default(),
            csp: "default-src 'self'; img-src 'self' https:; style-src 'self' 'unsafe-inline'"
                .into(),
            analytics: SearchUiAnalyticsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchUiAuthConfig {
    /// `public`, `shared_key`, or `oauth_proxy`.
    pub mode: String,
    pub shared_key_env: String,
    pub session_ttl_s: u64,
    pub session_rate_limit: String,
    pub jwt_secret_env: String,
    pub jwt_secret_previous_env: String,
    pub jwt_rotation_buffer_s: u64,
    pub oauth_proxy: OAuthProxyConfig,
}

impl Default for SearchUiAuthConfig {
    fn default() -> Self {
        Self {
            mode: "public".into(),
            shared_key_env: String::new(),
            session_ttl_s: 900,
            session_rate_limit: "10/minute".into(),
            jwt_secret_env: "SEARCH_UI_JWT_SECRET".into(),
            jwt_secret_previous_env: "SEARCH_UI_JWT_SECRET_PREVIOUS".into(),
            jwt_rotation_buffer_s: 300,
            oauth_proxy: OAuthProxyConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct OAuthProxyConfig {
    pub user_header: String,
    pub groups_header: String,
    pub filter_template: Option<String>,
    pub attribute_map: HashMap<String, String>,
}

impl Default for OAuthProxyConfig {
    fn default() -> Self {
        Self {
            user_header: "X-Forwarded-User".into(),
            groups_header: "X-Forwarded-Groups".into(),
            filter_template: Some("tenant IN [{groups}]".into()),
            attribute_map: {
                let mut m = HashMap::new();
                m.insert("groups".into(), "groups_array".into());
                m.insert("user".into(), "user_id_string".into());
                m
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchUiRateLimitConfig {
    pub per_ip: String,
    /// `redis` or `local`.
    pub backend: String,
    pub redis_key_prefix: String,
    pub redis_ttl_s: u64,
}

impl Default for SearchUiRateLimitConfig {
    fn default() -> Self {
        Self {
            per_ip: "60/minute".into(),
            backend: "redis".into(),
            redis_key_prefix: "miroir:ratelimit:searchui:".into(),
            redis_ttl_s: 60,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchUiAnalyticsConfig {
    pub enabled: bool,
    /// `cdc` (publishes click-throughs as CDC events).
    pub sink: String,
}

impl Default for SearchUiAnalyticsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sink: "cdc".into(),
        }
    }
}

// ---------------------------------------------------------------------------
// 13.22  Rebalancer (P4.1 background worker)
// ---------------------------------------------------------------------------

/// Rebalancer configuration (plan §4 Phase 4.1).
///
/// The rebalancer is a background Tokio task that orchestrates shard migration
/// during topology changes (node add/drain/fail/recover). Uses leader lease to
/// ensure only one pod runs the rebalancer at a time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RebalancerConfig {
    /// Enable or disable the rebalancer background worker.
    pub enabled: bool,
    /// Maximum concurrent shard migrations (plan §14.2 memory budget).
    pub max_concurrent_migrations: usize,
    /// Check interval for topology changes (milliseconds).
    pub check_interval_ms: u64,
    /// Leader lease TTL (milliseconds) — must be longer than check_interval.
    pub leader_lease_ttl_ms: u64,
    /// Batch size for document migration pagination.
    pub migration_batch_size: u32,
}

impl Default for RebalancerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent_migrations: 4,
            check_interval_ms: 5000,
            leader_lease_ttl_ms: 15000,
            migration_batch_size: 1000,
        }
    }
}

// ---------------------------------------------------------------------------
// §10 OpenTelemetry tracing
// ---------------------------------------------------------------------------

/// OpenTelemetry distributed tracing configuration (plan §10).
///
/// When enabled, every search produces a trace with parallel spans for each node
/// in the covering set. A slow node shows up as an outlier span in Tempo.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TracingConfig {
    /// Enable or disable OTel tracing. Default: false (zero overhead when disabled).
    pub enabled: bool,
    /// OTLP endpoint (e.g., "http://tempo.monitoring.svc:4317" for gRPC).
    pub endpoint: String,
    /// Service name for trace identification.
    pub service_name: String,
    /// Head-based sampling rate (0.0 to 1.0). 0.1 = ~10% of requests traced.
    pub sample_rate: f64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://tempo.monitoring.svc:4317".into(),
            service_name: "miroir".into(),
            sample_rate: 0.1,
        }
    }
}
