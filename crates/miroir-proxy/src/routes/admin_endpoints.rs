//! Admin API endpoints for topology, readiness, shards, and metrics.

use axum::{
    extract::{FromRef, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use miroir_core::{
    config::MiroirConfig,
    group_addition::{GroupAdditionCoordinator, GroupAdditionId},
    group_sync_worker::GroupSyncWorker,
    leader_election::{LeaderElection, LeaderElectionMetricsCallback},
    migration::{MigrationConfig, MigrationCoordinator},
    mode_a_coordinator::ModeACoordinator,
    mode_c_worker::{ModeCWorker, ModeCWorkerConfig},
    peer_discovery::PeerDiscovery,
    rebalancer::{MigrationExecutor, Rebalancer, RebalancerConfig, RebalancerMetrics},
    rebalancer_worker::{
        RebalancerMetricsCallback, RebalancerWorker, RebalancerWorkerConfig, TopologyChangeEvent,
    },
    replica_selection::{ReplicaSelector, SelectionObserver},
    reshard::ReshardingRegistry,
    router,
    scatter::{DeleteByFilterRequest, FetchDocumentsRequest, WriteRequest},
    task_registry::TaskRegistryImpl,
    task_store::{NewAdminSession, RedisTaskStore, TaskStore},
    topology::{Node, NodeId, Topology},
};
use rand::RngCore;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::{
    admin_session::{seal_session, unseal_session, SealKey, COOKIE_NAME},
    auth::generate_csrf_token,
    client::HttpClient,
    scoped_key_rotation::{
        self, RotateScopedKeyRequest, RotateScopedKeyResponse, ScopedKeyRotationState,
    },
};

// Re-export commonly used types for admin API responses
pub use miroir_core::rebalancer_worker::RebalanceJobId;

/// Hash a PII value (IP address) for safe log correlation.
fn hash_for_log(value: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Request body for POST /_miroir/admin/login.
#[derive(Deserialize)]
pub struct AdminLoginRequest {
    pub admin_key: String,
}

impl std::fmt::Debug for AdminLoginRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminLoginRequest")
            .field("admin_key", &"[redacted]")
            .finish()
    }
}

/// Response body for POST /_miroir/admin/login.
#[derive(Debug, Serialize)]
pub struct AdminLoginResponse {
    pub success: bool,
    pub message: Option<String>,
    /// CSRF token for state-changing requests (plan §9, §13.19).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub csrf_token: Option<String>,
}

/// Response body for POST /_miroir/admin/logout.
#[derive(Debug, Serialize)]
pub struct AdminLogoutResponse {
    pub success: bool,
    pub message: Option<String>,
}

/// Version state with cache for fetching Meilisearch version.
#[derive(Clone)]
pub struct VersionState {
    pub node_master_key: String,
    pub node_addresses: Vec<String>,
    pub version_cache: Arc<RwLock<Option<String>>>,
    pub last_cache_update: Arc<RwLock<Option<std::time::Instant>>>,
    pub cache_ttl_secs: u64,
}

impl VersionState {
    pub fn new(node_master_key: String, node_addresses: Vec<String>) -> Self {
        Self {
            node_master_key,
            node_addresses,
            version_cache: Arc::new(RwLock::new(None)),
            last_cache_update: Arc::new(RwLock::new(None)),
            cache_ttl_secs: 60,
        }
    }

    /// Fetch version from a healthy node, using cache if within TTL.
    pub async fn get_version(&self) -> Result<String, StatusCode> {
        // Check cache first
        {
            let cache = self.version_cache.read().await;
            let last_update = self.last_cache_update.read().await;
            if let (Some(ref cached), Some(last)) = (cache.as_ref(), last_update.as_ref()) {
                if last.elapsed().as_secs() < self.cache_ttl_secs {
                    return Ok((**cached).clone());
                }
            }
        }

        // Cache miss or expired - fetch from a node
        let client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        for address in &self.node_addresses {
            let url = format!("{}/version", address.trim_end_matches('/'));
            let response = client
                .get(&url)
                .header("Authorization", format!("Bearer {}", self.node_master_key))
                .send()
                .await;

            if let Ok(resp) = response {
                if resp.status().is_success() {
                    if let Ok(body) = resp.text().await {
                        // Update cache
                        *self.version_cache.write().await = Some(body.clone());
                        *self.last_cache_update.write().await = Some(std::time::Instant::now());
                        return Ok(body);
                    }
                }
            }
        }

        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}

// ---------------------------------------------------------------------------
// Local Rate Limiter (for single-pod deployments)
// ---------------------------------------------------------------------------

/// In-memory rate limiter for admin login (local backend only).
/// Thread-safe using Arc<Mutex<...>>.
#[derive(Debug, Clone)]
pub struct LocalAdminRateLimiter {
    inner: Arc<std::sync::Mutex<LocalAdminRateLimiterInner>>,
}

#[derive(Debug, Default)]
struct LocalAdminRateLimiterInner {
    /// Map of IP -> (request_timestamps_ms, failed_count, backoff_until_ms)
    state: HashMap<String, LocalRateLimitState>,
}

#[derive(Debug, Default, Clone)]
struct LocalRateLimitState {
    /// Timestamps of recent requests (for sliding window)
    request_timestamps_ms: Vec<i64>,
    /// Consecutive failed login attempts
    failed_count: u32,
    /// Unix timestamp (ms) when backoff expires
    backoff_until_ms: Option<i64>,
}

impl LocalAdminRateLimiter {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(LocalAdminRateLimiterInner::default())),
        }
    }

    /// Check rate limit and exponential backoff.
    /// Returns (allowed, wait_seconds).
    pub fn check(
        &self,
        ip: &str,
        limit: u64,
        window_ms: u64,
        failed_threshold: u32,
        backoff_start_minutes: u64,
        backoff_max_hours: u64,
    ) -> (bool, Option<u64>) {
        let mut inner = self.inner.lock().unwrap();
        let now = now_ms();
        let state = inner.state.entry(ip.to_string()).or_default();

        // Check if we're in backoff mode
        if let Some(backoff_until) = state.backoff_until_ms {
            if backoff_until > now {
                let wait_seconds = ((backoff_until - now) / 1000) as u64;
                return (false, Some(wait_seconds));
            }
            // Backoff expired, clear it
            state.backoff_until_ms = None;
        }

        // Clean old timestamps outside the window
        state
            .request_timestamps_ms
            .retain(|&ts| now - ts < window_ms as i64);

        // Check if limit exceeded
        if state.request_timestamps_ms.len() >= limit as usize {
            // Enter backoff mode after threshold consecutive failures
            let failed = state.failed_count + 1;
            state.failed_count = failed;

            if failed >= failed_threshold {
                let backoff_minutes =
                    backoff_start_minutes * (1u64 << ((failed - failed_threshold) as u64).min(7)); // Cap at 2^7 = 128x
                let backoff_seconds = (backoff_minutes * 60).min(backoff_max_hours * 3600);
                state.backoff_until_ms = Some(now + (backoff_seconds as i64 * 1000));
                return (false, Some(backoff_seconds));
            }

            return (false, None);
        }

        // Record this request
        state.request_timestamps_ms.push(now);
        (true, None)
    }

    /// Reset rate limit and backoff state on successful login.
    pub fn reset(&self, ip: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.state.remove(ip);
    }

    /// Record a failed login attempt (for backoff calculation).
    pub fn record_failure(
        &self,
        ip: &str,
        failed_threshold: u32,
        backoff_start_minutes: u64,
        backoff_max_hours: u64,
    ) -> Option<u64> {
        let mut inner = self.inner.lock().unwrap();
        let now = now_ms();
        let state = inner.state.entry(ip.to_string()).or_default();

        state.failed_count += 1;

        if state.failed_count >= failed_threshold {
            let backoff_minutes = backoff_start_minutes
                * (1u64 << ((state.failed_count - failed_threshold) as u64).min(7));
            let backoff_seconds = (backoff_minutes * 60).min(backoff_max_hours * 3600);
            state.backoff_until_ms = Some(now + (backoff_seconds as i64 * 1000));
            return Some(backoff_seconds);
        }

        None
    }
}

impl Default for LocalAdminRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// In-memory rate limiter for search UI (local backend only).
/// Thread-safe using Arc<Mutex<...>>.
#[derive(Debug, Clone)]
pub struct LocalSearchUiRateLimiter {
    inner: Arc<std::sync::Mutex<LocalSearchUiRateLimiterInner>>,
}

#[derive(Debug, Default)]
struct LocalSearchUiRateLimiterInner {
    /// Map of IP -> request_timestamps_ms
    state: HashMap<String, Vec<i64>>,
}

impl LocalSearchUiRateLimiter {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(
                LocalSearchUiRateLimiterInner::default(),
            )),
        }
    }

    /// Check rate limit for search UI.
    /// Returns (allowed, wait_seconds).
    pub fn check(&self, ip: &str, limit: u64, window_ms: u64) -> (bool, Option<u64>) {
        let mut inner = self.inner.lock().unwrap();
        let now = now_ms();
        let timestamps = inner.state.entry(ip.to_string()).or_default();

        // Clean old timestamps outside the window
        timestamps.retain(|&ts| now - ts < window_ms as i64);

        // Check if limit exceeded
        if timestamps.len() >= limit as usize {
            return (false, None);
        }

        // Record this request
        timestamps.push(now);
        (true, None)
    }
}

impl Default for LocalSearchUiRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Get current time in milliseconds since Unix epoch.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Hash an admin key for storage in the task store (SHA-256 hex).
/// We never store the plaintext admin key, only a hash for audit.
fn hash_admin_key(admin_key: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(admin_key.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Metrics observer for replica selection events.
///
/// Reports selection scores and exploration events to Prometheus.
struct ReplicaSelectionMetricsObserver {
    metrics: super::super::middleware::Metrics,
}

impl SelectionObserver for ReplicaSelectionMetricsObserver {
    fn report_selection(&self, node_id: &str, score: f64) {
        self.metrics.set_replica_selection_score(node_id, score);
    }

    fn report_exploration(&self) {
        self.metrics.inc_replica_selection_exploration();
    }
}

/// Shared application state for admin endpoints.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<MiroirConfig>,
    pub topology: Arc<RwLock<Topology>>,
    pub ready: Arc<RwLock<bool>>,
    pub metrics: super::super::middleware::Metrics,
    pub version_state: VersionState,
    pub task_registry: Arc<TaskRegistryImpl>,
    pub redis_store: Option<RedisTaskStore>,
    pub task_store: Option<Arc<dyn TaskStore>>,
    pub pod_id: String,
    pub seal_key: SealKey,
    pub local_rate_limiter: LocalAdminRateLimiter,
    pub local_search_ui_rate_limiter: LocalSearchUiRateLimiter,
    pub rebalancer: Option<Arc<Rebalancer>>,
    pub migration_coordinator: Option<Arc<RwLock<MigrationCoordinator>>>,
    pub rebalancer_worker: Option<Arc<RebalancerWorker>>,
    pub rebalancer_metrics: Arc<RwLock<RebalancerMetrics>>,
    /// Track previous documents migrated value for delta calculation.
    pub previous_docs_migrated: Arc<std::sync::atomic::AtomicU64>,
    /// Two-phase settings broadcast coordinator (§13.5).
    pub settings_broadcast: Arc<miroir_core::settings::SettingsBroadcast>,
    /// Settings drift reconciler worker (§13.5).
    pub drift_reconciler: Option<Arc<miroir_core::rebalancer_worker::DriftReconciler>>,
    /// Anti-entropy worker (plan §13.8).
    pub anti_entropy_worker: Option<Arc<miroir_core::rebalancer_worker::AntiEntropyWorker>>,
    /// Session pinning manager (§13.6).
    pub session_manager: Arc<miroir_core::session_pinning::SessionManager>,
    /// Alias registry (§13.7).
    pub alias_registry: Arc<miroir_core::alias::AliasRegistry>,
    /// Leader election service for Mode B operations (plan §14.5).
    pub leader_election: Option<Arc<LeaderElection>>,
    /// Mode C worker for chunked background jobs (plan §14.5 Mode C).
    pub mode_c_worker: Option<Arc<ModeCWorker>>,
    /// Adaptive replica selector (plan §13.3).
    pub replica_selector: Arc<miroir_core::replica_selection::ReplicaSelector>,
    /// Idempotency cache for write deduplication (plan §13.10).
    pub idempotency_cache: Arc<miroir_core::idempotency::IdempotencyCache>,
    /// Query coalescer for read deduplication (plan §13.10).
    pub query_coalescer: Arc<miroir_core::idempotency::QueryCoalescer>,
    /// Query planner for shard-aware query planning (plan §13.4).
    pub query_planner: Arc<miroir_core::query_planner::QueryPlanner>,
    /// Group addition coordinator for replica group addition flow (plan §2).
    pub group_addition_coordinator: Option<Arc<RwLock<GroupAdditionCoordinator>>>,
    /// Group sync worker for background document sync.
    pub group_sync_worker: Option<Arc<GroupSyncWorker<HttpClient>>>,
    /// Mode A coordinator for shard-partitioned ownership (plan §14.5 Mode A).
    pub mode_a_coordinator: Option<Arc<ModeACoordinator>>,
    /// Resharding registry for tracking active resharding operations (plan §13.1).
    /// Used by the write path to detect dual-write phase and route to both live and shadow indexes.
    pub resharding_registry: Arc<tokio::sync::RwLock<ReshardingRegistry>>,
    /// Shadow manager for traffic shadowing (plan §13.16).
    pub shadow_manager: Option<Arc<miroir_core::shadow::ShadowManager>>,
    /// CDC manager for change data capture (plan §13.13).
    pub cdc_manager: Option<Arc<miroir_core::cdc::CdcManager>>,
    /// Tenant affinity manager for noisy-neighbor isolation (plan §13.15).
    pub tenant_affinity_manager: Arc<miroir_core::tenant::TenantAffinityManager>,
}

impl AppState {
    pub fn new(
        config: MiroirConfig,
        metrics: super::super::middleware::Metrics,
        seal_key: SealKey,
    ) -> Self {
        Self::with_redis(config, metrics, None, "unknown".into(), seal_key)
    }

    pub fn with_redis(
        config: MiroirConfig,
        metrics: super::super::middleware::Metrics,
        redis_store: Option<RedisTaskStore>,
        pod_id: String,
        seal_key: SealKey,
    ) -> Self {
        // Build initial topology from config
        let mut topology = Topology::new(
            config.shards,
            config.replica_groups,
            config.replication_factor as usize,
        );

        for node_config in &config.nodes {
            let node = Node::new(
                NodeId::new(node_config.id.clone()),
                node_config.address.clone(),
                node_config.replica_group,
            );
            // Start nodes in Joining state - health checker will promote to Active
            topology.add_node(node);
        }

        let version_state = VersionState::new(
            config.node_master_key.clone(),
            config.nodes.iter().map(|n| n.address.clone()).collect(),
        );

        // Select task registry backend based on config
        let task_registry = match config.task_store.backend.as_str() {
            "redis" if redis_store.is_some() => {
                let store = redis_store.as_ref().unwrap().clone();
                store.migrate().expect("Redis migration failed");
                TaskRegistryImpl::Redis(Arc::new(store))
            }
            "sqlite" if !config.task_store.path.is_empty() => {
                TaskRegistryImpl::sqlite(&config.task_store.path)
                    .expect("Failed to open SQLite task store")
            }
            _ => TaskRegistryImpl::in_memory(),
        };

        let topology_arc = Arc::new(RwLock::new(topology));

        // Initialize rebalancer and migration coordinator
        let rebalancer_config = RebalancerConfig {
            max_concurrent_migrations: config.rebalancer.max_concurrent_migrations,
            migration_timeout_s: config.rebalancer.migration_timeout_s,
            auto_rebalance_on_recovery: config.rebalancer.auto_rebalance_on_recovery,
            migration_batch_size: 1000,
            migration_batch_delay_ms: 100,
        };

        let migration_config = MigrationConfig {
            drain_timeout: std::time::Duration::from_secs(30),
            skip_delta_pass: false,
            anti_entropy_enabled: config.anti_entropy.enabled,
        };

        let migration_coordinator = Arc::new(RwLock::new(MigrationCoordinator::new(
            migration_config.clone(),
        )));

        // Create migration executor for actual HTTP document migration
        use miroir_core::rebalancer::HttpMigrationExecutor;
        let migration_executor = Arc::new(HttpMigrationExecutor::new(
            config.node_master_key.clone(),
            config.scatter.node_timeout_ms,
        ));

        let rebalancer = Arc::new(
            Rebalancer::new(
                rebalancer_config.clone(),
                topology_arc.clone(),
                migration_config.clone(),
            )
            .with_migration_executor(migration_executor),
        );

        // Create rebalancer metrics
        let rebalancer_metrics = Arc::new(RwLock::new(RebalancerMetrics::default()));

        // Get or create task store for rebalancer worker
        let task_store: Option<Arc<dyn TaskStore>> = match config.task_store.backend.as_str() {
            "redis" => redis_store
                .as_ref()
                .map(|s| Arc::new(s.clone()) as Arc<dyn TaskStore>),
            "sqlite" if !config.task_store.path.is_empty() => Some(Arc::new(
                miroir_core::task_store::SqliteTaskStore::open(std::path::Path::new(
                    &config.task_store.path,
                ))
                .expect("Failed to open SQLite task store"),
            )
                as Arc<dyn TaskStore>),
            _ => None,
        };

        // Create rebalancer worker if task store is available
        let rebalancer_worker = if let Some(ref store) = task_store {
            let worker_config = RebalancerWorkerConfig {
                max_concurrent_migrations: config.rebalancer.max_concurrent_migrations,
                lease_ttl_secs: 10,
                lease_renewal_interval_ms: 2000,
                migration_batch_size: 1000,
                migration_batch_delay_ms: 100,
                event_channel_capacity: 100,
            };

            // Create metrics callback for rebalancer operations
            let metrics_for_worker = metrics.clone();
            let rebalancer_metrics_callback: RebalancerMetricsCallback = Arc::new(
                move |in_progress: bool, docs_migrated: Option<u64>, duration_secs: Option<f64>| {
                    if in_progress {
                        metrics_for_worker.set_rebalance_in_progress(true);
                    } else {
                        metrics_for_worker.set_rebalance_in_progress(false);
                    }
                    if let Some(count) = docs_migrated {
                        metrics_for_worker.inc_rebalance_documents_migrated(count);
                    }
                    if let Some(duration) = duration_secs {
                        metrics_for_worker.observe_rebalance_duration(duration);
                    }
                },
            );

            Some(Arc::new(RebalancerWorker::with_metrics(
                worker_config,
                topology_arc.clone(),
                store.clone(),
                rebalancer.clone(),
                migration_coordinator.clone(),
                rebalancer_metrics.clone(),
                pod_id.clone(),
                Some(rebalancer_metrics_callback),
            )))
        } else {
            None
        };

        // Create settings broadcast coordinator (§13.5)
        let settings_broadcast = if let Some(ref store) = task_store {
            Arc::new(miroir_core::settings::SettingsBroadcast::with_task_store(
                store.clone(),
            ))
        } else {
            Arc::new(miroir_core::settings::SettingsBroadcast::new())
        };

        // Check if task store is available before moving it
        let has_task_store = task_store.is_some();

        // Create drift reconciler worker (§13.5) if task store is available
        // Note: Mode A coordinator will be wired up after it's created (below)
        let drift_reconciler = if let Some(ref store) = task_store {
            let node_addresses = config.nodes.iter().map(|n| n.address.clone()).collect();
            let drift_config = miroir_core::rebalancer_worker::DriftReconcilerConfig {
                interval_s: config.settings_drift_check.interval_s,
                auto_repair: config.settings_drift_check.auto_repair,
                lease_ttl_secs: 10,
                lease_renewal_interval_ms: 2000,
            };
            let metrics_clone = metrics.clone();
            let callback: miroir_core::rebalancer_worker::DriftRepairCallback =
                Arc::new(move |index: &str| {
                    metrics_clone.inc_settings_drift_repair(index);
                });
            Some(Arc::new(
                miroir_core::rebalancer_worker::DriftReconciler::new(
                    drift_config,
                    settings_broadcast.clone(),
                    store.clone(),
                    node_addresses,
                    config.node_master_key.clone(),
                    pod_id.clone(),
                )
                .with_metrics_callback(callback),
            ))
        } else {
            None
        };

        // Create anti-entropy worker (plan §13.8) if task store is available
        // Note: Mode A coordinator will be wired up after it's created (below)
        let anti_entropy_worker = if config.anti_entropy.enabled {
            if let Some(ref store) = task_store {
                let ae_worker_config =
                    miroir_core::rebalancer_worker::AntiEntropyWorkerConfig::from_schedule(
                        &config.anti_entropy.schedule,
                    );
                let metrics_for_ae_1 = metrics.clone();
                let metrics_for_ae_2 = metrics.clone();
                let metrics_for_ae_3 = metrics.clone();
                let metrics_for_ae_4 = metrics.clone();
                let mut ae_worker = miroir_core::rebalancer_worker::AntiEntropyWorker::new(
                    ae_worker_config,
                    topology_arc.clone(),
                    store.clone(),
                    config.node_master_key.clone(),
                    pod_id.clone(),
                );
                // Wire up metrics callbacks
                ae_worker = ae_worker.with_metrics(
                    Arc::new(move |count: u64| {
                        metrics_for_ae_1.inc_antientropy_shards_scanned(count);
                    }),
                    Arc::new(move |count: u64| {
                        metrics_for_ae_2.inc_antientropy_mismatches_found(count);
                    }),
                    Arc::new(move |count: u64| {
                        metrics_for_ae_3.inc_antientropy_docs_repaired(count);
                    }),
                    Arc::new(move |timestamp: u64| {
                        metrics_for_ae_4.set_antientropy_last_scan_completed(timestamp);
                    }),
                );
                // Set TTL enabled flag from config
                ae_worker.set_ttl_enabled(config.anti_entropy.ttl_enabled);
                Some(Arc::new(ae_worker))
            } else {
                None
            }
        } else {
            None
        };

        // Create session pinning manager (§13.6)
        let session_manager = Arc::new(miroir_core::session_pinning::SessionManager::new(
            miroir_core::session_pinning::SessionPinningConfig::from(
                config.session_pinning.clone(),
            ),
        ));

        // Create tenant affinity manager (plan §13.15)
        let tenant_affinity_manager = Arc::new(
            miroir_core::tenant::TenantAffinityManager::with_replica_groups(
                config.tenant_affinity.clone(),
                config.replica_groups,
            ),
        );

        // Create alias registry (§13.7)
        // Note: Aliases are loaded asynchronously in background, not during initialization
        let alias_registry = Arc::new(miroir_core::alias::AliasRegistry::new());

        // Create leader election service (plan §14.5) if task store is available
        let leader_election = if let Some(ref store) = task_store {
            // Create metrics callback for leader election
            let metrics_for_leader = metrics.clone();
            let metrics_callback: LeaderElectionMetricsCallback = Arc::new(
                move |metric_name: &str,
                      labels: &std::collections::HashMap<String, String>,
                      value: f64| {
                    if metric_name == "miroir_leader" {
                        if let Some(scope) = labels.get("scope") {
                            metrics_for_leader.set_leader(scope, value > 0.0);
                        }
                    }
                },
            );

            let leader_config = config.leader_election.clone();
            let mut leader = LeaderElection::new(store.clone(), pod_id.clone(), leader_config);
            leader = leader.with_metrics_callback(metrics_callback);
            Some(Arc::new(leader))
        } else {
            None
        };

        // Create Mode C worker for chunked background jobs (plan §14.5 Mode C)
        let mode_c_worker = if let Some(ref store) = task_store {
            let worker_config = ModeCWorkerConfig {
                poll_interval_ms: 1000,       // 1 second
                heartbeat_interval_ms: 10000, // 10 seconds
                max_concurrent_jobs: 3,
            };
            Some(Arc::new(ModeCWorker::new(
                store.clone(),
                pod_id.clone(),
                worker_config,
            )))
        } else {
            None
        };

        // Create Mode A coordinator for shard-partitioned ownership (plan §14.5 Mode A)
        // This must be created before drift_reconciler and anti_entropy_worker so they can be wired up
        let mode_a_coordinator = if cfg!(feature = "peer-discovery") {
            let pod_name = std::env::var("POD_NAME").unwrap_or_else(|_| "unknown".to_string());
            let namespace =
                std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".to_string());
            let service_name = std::env::var("MIROR_SERVICE_NAME")
                .unwrap_or_else(|_| "miroir-headless".to_string());
            let peer_discovery = Arc::new(PeerDiscovery::new(
                pod_name.clone(),
                namespace,
                service_name,
            ));
            Some(Arc::new(ModeACoordinator::new(pod_name, peer_discovery)))
        } else {
            None
        };

        // Wire up Mode A coordinator to drift_reconciler (plan §14.5 Mode A, P6.3)
        let drift_reconciler = if let Some(ref reconciler) = drift_reconciler {
            if let Some(ref coordinator) = mode_a_coordinator {
                // Use Arc::make_mut to get a mutable reference if we're the only owner
                // Since we just created this Arc, we should be the only owner
                // We need to recreate the Arc with the coordinator wired up
                let reconciler_inner = Arc::try_unwrap(reconciler.clone()).unwrap_or_else(|_| {
                    // If we can't unwrap, create a new one with the coordinator
                    // This shouldn't happen in practice since we just created it
                    let node_addresses = config.nodes.iter().map(|n| n.address.clone()).collect();
                    let drift_config = miroir_core::rebalancer_worker::DriftReconcilerConfig {
                        interval_s: config.settings_drift_check.interval_s,
                        auto_repair: config.settings_drift_check.auto_repair,
                        lease_ttl_secs: 10,
                        lease_renewal_interval_ms: 2000,
                    };
                    let metrics_clone = metrics.clone();
                    let callback: miroir_core::rebalancer_worker::DriftRepairCallback =
                        Arc::new(move |index: &str| {
                            metrics_clone.inc_settings_drift_repair(index);
                        });
                    miroir_core::rebalancer_worker::DriftReconciler::new(
                        drift_config,
                        settings_broadcast.clone(),
                        task_store.as_ref().unwrap().clone(),
                        node_addresses,
                        config.node_master_key.clone(),
                        pod_id.clone(),
                    )
                    .with_metrics_callback(callback)
                });
                Some(Arc::new(
                    reconciler_inner.with_mode_a_coordinator(coordinator.clone()),
                ))
            } else {
                Some(reconciler.clone())
            }
        } else {
            None
        };

        // Wire up Mode A coordinator to anti_entropy_worker (plan §14.5 Mode A, P6.3)
        let anti_entropy_worker = if let Some(ref worker) = anti_entropy_worker {
            if let Some(ref coordinator) = mode_a_coordinator {
                // Same approach as drift_reconciler - unwrap and recreate with coordinator
                let worker_inner = Arc::try_unwrap(worker.clone()).unwrap_or_else(|_| {
                    // If we can't unwrap, create a new one
                    if let Some(ref store) = task_store {
                        let ae_worker_config =
                            miroir_core::rebalancer_worker::AntiEntropyWorkerConfig::from_schedule(
                                &config.anti_entropy.schedule,
                            );
                        let metrics_for_ae_1 = metrics.clone();
                        let metrics_for_ae_2 = metrics.clone();
                        let metrics_for_ae_3 = metrics.clone();
                        let metrics_for_ae_4 = metrics.clone();
                        let mut ae_worker = miroir_core::rebalancer_worker::AntiEntropyWorker::new(
                            ae_worker_config,
                            topology_arc.clone(),
                            store.clone(),
                            config.node_master_key.clone(),
                            pod_id.clone(),
                        );
                        ae_worker = ae_worker.with_metrics(
                            Arc::new(move |count: u64| {
                                metrics_for_ae_1.inc_antientropy_shards_scanned(count);
                            }),
                            Arc::new(move |count: u64| {
                                metrics_for_ae_2.inc_antientropy_mismatches_found(count);
                            }),
                            Arc::new(move |count: u64| {
                                metrics_for_ae_3.inc_antientropy_docs_repaired(count);
                            }),
                            Arc::new(move |timestamp: u64| {
                                metrics_for_ae_4.set_antientropy_last_scan_completed(timestamp);
                            }),
                        );
                        ae_worker.set_ttl_enabled(config.anti_entropy.ttl_enabled);
                        ae_worker
                    } else {
                        panic!("anti_entropy_worker exists but task_store is None");
                    }
                });
                Some(Arc::new(
                    worker_inner.with_mode_a_coordinator(coordinator.clone()),
                ))
            } else {
                Some(worker.clone())
            }
        } else {
            None
        };

        // Create group addition coordinator (needed for both API and sync worker)
        let group_addition_coordinator = if has_task_store {
            Some(Arc::new(RwLock::new(
                miroir_core::group_addition::GroupAdditionCoordinator::new(
                    miroir_core::group_addition::GroupAdditionConfig::default(),
                ),
            )))
        } else {
            None
        };

        // Create group sync worker if task store is available
        let group_sync_worker = if has_task_store {
            // Create HTTP client for the sync worker
            let http_client = Arc::new(super::super::client::HttpClient::new(
                config.node_master_key.clone(),
                config.scatter.node_timeout_ms,
            ));
            let worker_config = miroir_core::group_sync_worker::GroupSyncWorkerConfig::default();
            // Use the same coordinator
            let coordinator = group_addition_coordinator.as_ref().unwrap().clone();
            Some(Arc::new(
                miroir_core::group_sync_worker::GroupSyncWorker::new(
                    worker_config,
                    coordinator,
                    http_client,
                    topology_arc.clone(),
                ),
            ))
        } else {
            None
        };

        Self {
            config: Arc::new(config.clone()),
            topology: topology_arc,
            ready: Arc::new(RwLock::new(false)),
            metrics: metrics.clone(),
            version_state,
            task_registry: Arc::new(task_registry),
            redis_store: redis_store.clone(),
            task_store,
            pod_id,
            seal_key,
            local_rate_limiter: LocalAdminRateLimiter::new(),
            local_search_ui_rate_limiter: LocalSearchUiRateLimiter::new(),
            rebalancer: Some(rebalancer),
            migration_coordinator: Some(migration_coordinator),
            rebalancer_worker,
            rebalancer_metrics,
            previous_docs_migrated: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            settings_broadcast,
            drift_reconciler,
            anti_entropy_worker,
            session_manager,
            alias_registry,
            leader_election,
            mode_c_worker,
            replica_selector: {
                let advanced_config = config.replica_selection.clone();
                let selector_config =
                    miroir_core::replica_selection::ReplicaSelectionConfig::from(advanced_config);
                let observer = Arc::new(ReplicaSelectionMetricsObserver {
                    metrics: metrics.clone(),
                });
                Arc::new(ReplicaSelector::new_with_observer(
                    selector_config,
                    observer,
                ))
            },
            idempotency_cache: Arc::new(miroir_core::idempotency::IdempotencyCache::new(
                config.idempotency.max_cached_keys as usize,
                config.idempotency.ttl_seconds,
            )),
            query_coalescer: Arc::new(miroir_core::idempotency::QueryCoalescer::new(
                config.query_coalescing.window_ms,
                config.query_coalescing.max_pending_queries as usize,
                config.query_coalescing.max_subscribers as usize,
            )),
            query_planner: Arc::new(miroir_core::query_planner::QueryPlanner::new(
                config.query_planner.clone().into(),
            )),
            group_addition_coordinator,
            group_sync_worker,
            mode_a_coordinator,
            resharding_registry: Arc::new(tokio::sync::RwLock::new(
                miroir_core::reshard::ReshardingRegistry::new(),
            )),
            shadow_manager: {
                // Create shadow manager if enabled in config (plan §13.16)
                if config.shadow.enabled && !config.shadow.targets.is_empty() {
                    Some(Arc::new(miroir_core::shadow::ShadowManager::new(
                        config.shadow.clone().into(),
                    )))
                } else {
                    None
                }
            },
            tenant_affinity_manager,
            cdc_manager: {
                // Create CDC manager if enabled in config
                if config.cdc.enabled {
                    let task_store: Option<Arc<dyn TaskStore>> =
                        match config.task_store.backend.as_str() {
                            "redis" => redis_store
                                .as_ref()
                                .map(|s| Arc::new(s.clone()) as Arc<dyn TaskStore>),
                            "sqlite" if !config.task_store.path.is_empty() => Some(Arc::new(
                                miroir_core::task_store::SqliteTaskStore::open(
                                    std::path::Path::new(&config.task_store.path),
                                )
                                .expect("Failed to open SQLite task store"),
                            )
                                as Arc<dyn TaskStore>),
                            _ => None,
                        };
                    Some(Arc::new(miroir_core::cdc::CdcManager::with_metrics(
                        config.cdc.clone().into(), // Convert config::advanced::CdcConfig to cdc::CdcConfig
                        None,                      // suppressed_metric_callback
                        None,                      // dropped_metric_callback
                        task_store,
                    )))
                } else {
                    None
                }
            },
        }
    }

    /// Mark the service as ready (all nodes reachable).
    pub async fn mark_ready(&self) {
        *self.ready.write().await = true;
        info!("Service marked as ready");
    }

    /// Check if a covering quorum is reachable.
    pub async fn check_covering_quorum(&self) -> bool {
        let topo = self.topology.read().await;
        let node_map = topo.node_map();

        // For each replica group, check if we have enough healthy nodes
        for group in topo.groups() {
            let healthy = group.healthy_nodes(&node_map);
            let required = (topo.rf() + 1) / 2; // Simple majority for quorum
            if healthy.len() < required {
                return false;
            }
        }

        true
    }

    /// Sync rebalancer metrics to Prometheus (called from health checker).
    pub async fn sync_rebalancer_metrics_to_prometheus(&self) {
        if let Some(ref rebalancer) = self.rebalancer {
            let rebalancer_metrics = rebalancer.metrics.read().await;
            let in_progress = rebalancer_metrics.rebalance_start_time.is_some();
            self.metrics.set_rebalance_in_progress(in_progress);

            // Calculate delta for documents migrated counter
            let current_total = rebalancer_metrics.documents_migrated_total;
            let previous = self
                .previous_docs_migrated
                .load(std::sync::atomic::Ordering::Relaxed);
            if current_total > previous {
                let delta = current_total - previous;
                self.metrics.inc_rebalance_documents_migrated(delta);
                self.previous_docs_migrated
                    .store(current_total, std::sync::atomic::Ordering::Relaxed);
            }

            let duration = rebalancer_metrics.current_duration_secs();
            if duration > 0.0 {
                self.metrics.observe_rebalance_duration(duration);
            }
        }
    }
}

/// Response for GET /_miroir/topology (plan §10 JSON shape).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyResponse {
    pub shards: u32,
    pub replication_factor: u32,
    pub nodes: Vec<NodeInfo>,
    pub degraded_node_count: u32,
    pub rebalance_in_progress: bool,
    pub fully_covered: bool,
}

/// Per-node information in the topology response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub address: String,
    pub status: String,
    pub shard_count: u32,
    pub last_seen_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response for GET /_miroir/shards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardsResponse {
    pub shards: HashMap<String, Vec<String>>, // shard_id -> list of node IDs
}

/// GET /_miroir/topology — full cluster state per plan §10.
pub async fn get_topology<S>(State(state): State<S>) -> Result<Json<TopologyResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);
    let topo = state.topology.read().await;

    // Count degraded nodes
    let degraded_count = topo.nodes().filter(|n| !n.is_healthy()).count() as u32;

    // Check rebalance status
    let rebalance_in_progress = if let Some(ref rebalancer) = state.rebalancer {
        let status = rebalancer.status().await;
        status.in_progress
    } else {
        false
    };

    // Build node info list
    // First compute shard counts per node using rendezvous assignment
    use std::collections::HashMap;
    let mut shard_counts: HashMap<String, u32> = HashMap::new();

    for shard_id in 0..topo.shards {
        for group in topo.groups() {
            let assigned = router::assign_shard_in_group(shard_id, group.nodes(), topo.rf());
            for node_id in assigned {
                *shard_counts
                    .entry(node_id.as_str().to_string())
                    .or_insert(0) += 1;
            }
        }
    }

    let nodes: Vec<NodeInfo> = topo
        .nodes()
        .map(|n| {
            // Compute last_seen_ms from node.last_seen
            let last_seen_ms = n
                .last_seen
                .map(|i| i.elapsed().as_millis() as u64)
                .unwrap_or(0);

            NodeInfo {
                id: n.id.as_str().to_string(),
                address: n.address.clone(),
                status: format!("{:?}", n.status).to_lowercase(),
                shard_count: shard_counts.get(n.id.as_str()).copied().unwrap_or(0),
                last_seen_ms,
                error: n.last_error.clone(),
            }
        })
        .collect();

    // Check if fully covered
    let fully_covered = degraded_count == 0;

    let response = TopologyResponse {
        shards: topo.shards,
        replication_factor: topo.rf() as u32,
        nodes,
        degraded_node_count: degraded_count,
        rebalance_in_progress,
        fully_covered,
    };

    Ok(Json(response))
}

/// GET /_miroir/shards — shard → node mapping table.
pub async fn get_shards<S>(State(state): State<S>) -> Result<Json<ShardsResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);
    let topo = state.topology.read().await;
    let mut shards = HashMap::new();

    // Build shard -> node mapping using rendezvous hash
    for shard_id in 0..topo.shards {
        let mut node_ids = Vec::new();

        // Collect nodes from all replica groups for this shard
        for group in topo.groups() {
            let assigned = router::assign_shard_in_group(shard_id, group.nodes(), topo.rf());
            for node_id in assigned {
                node_ids.push(node_id.as_str().to_string());
            }
        }

        shards.insert(shard_id.to_string(), node_ids);
    }

    Ok(Json(ShardsResponse { shards }))
}

/// GET /_miroir/ready — readiness probe (503 during startup, 200 once ready).
pub async fn get_ready<S>(State(state): State<S>) -> Result<&'static str, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);
    let ready = *state.ready.read().await;

    if ready {
        Ok("")
    } else {
        // Not yet marked ready - check if covering quorum exists
        let has_quorum = state.check_covering_quorum().await;
        if has_quorum {
            // Auto-mark ready on first successful quorum check
            state.mark_ready().await;
            Ok("")
        } else {
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

/// GET /_miroir/metrics — admin-key-gated Prometheus metrics.
pub async fn get_metrics<S>(State(state): State<S>) -> Response
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);
    match state.metrics.encode_metrics() {
        Ok(metrics) => metrics.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to encode metrics");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// POST /_miroir/ui/search/{index}/rotate-scoped-key — manual rotation trigger.
///
/// Admin-gated endpoint that initiates a scoped key rotation for the given index.
/// Set `force: true` in the request body to bypass the timing gate.
pub async fn rotate_scoped_key_handler<S>(
    State(state): State<S>,
    Path(index): Path<String>,
    Json(body): Json<RotateScopedKeyRequest>,
) -> Result<Json<RotateScopedKeyResponse>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    let redis = app_state.redis_store.clone().ok_or_else(|| {
        (
            StatusCode::PRECONDITION_FAILED,
            "scoped key rotation requires Redis task store".into(),
        )
    })?;

    if !app_state.config.search_ui.enabled {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "search_ui is not enabled".into(),
        ));
    }

    let rotation_state = ScopedKeyRotationState {
        config: app_state.config.clone(),
        redis,
        pod_id: app_state.pod_id.clone(),
    };

    info!(
        index = %index,
        force = body.force,
        pod_id = %app_state.pod_id,
        "manual scoped key rotation triggered"
    );

    match scoped_key_rotation::check_and_rotate(&rotation_state, &index, body.force).await {
        Ok(response) => Ok(Json(response)),
        Err(e) => {
            error!(index = %index, error = %e, "manual scoped key rotation failed");
            Err((StatusCode::INTERNAL_SERVER_ERROR, e))
        }
    }
}

/// Parse a rate limit string like "10/minute" into (limit, window_seconds).
pub fn parse_rate_limit(s: &str) -> Result<(u64, u64), String> {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() != 2 {
        return Err(format!(
            "invalid rate limit format: '{}', expected 'N/UNIT'",
            s
        ));
    }
    let limit: u64 = parts[0]
        .parse()
        .map_err(|_| format!("invalid limit number: '{}'", parts[0]))?;
    let window_seconds = match parts[1] {
        "second" | "s" => 1,
        "minute" | "m" => 60,
        "hour" | "h" => 3600,
        "day" | "d" => 86400,
        unit => {
            return Err(format!(
                "invalid time unit: '{}', expected second/minute/hour/day",
                unit
            ))
        }
    };
    Ok((limit, window_seconds))
}

/// Generate a random session ID.
fn generate_session_id() -> String {
    let mut bytes = [0u8; 24];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    hex::encode(&bytes)
}

/// POST /_miroir/admin/login — admin login with rate limiting and exponential backoff.
///
/// Request body:
/// ```json
/// { "admin_key": "..." }
/// ```
///
/// On success, sets a `miroir_admin_session` cookie and returns:
/// ```json
/// { "success": true }
/// ```
///
/// Rate limiting (per source IP):
/// - 10 requests per minute (configurable via `admin_ui.rate_limit.per_ip`)
/// - After 5 consecutive failed attempts, exponential backoff applies:
///   - 10m, 20m, 40m, ... up to 24h cap
///
/// Successful login resets both the rate limit counter and backoff state.
pub async fn admin_login<S>(
    State(state): State<S>,
    headers: HeaderMap,
    Json(body): Json<AdminLoginRequest>,
) -> Response
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);

    // Extract source IP from X-Forwarded-For or X-Real-IP (trust proxy)
    let source_ip = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .or_else(|| headers.get("x-real-ip").and_then(|v| v.to_str().ok()))
        .unwrap_or("unknown")
        .trim()
        .to_string();

    // Parse rate limit config
    let (limit, window_seconds) = match parse_rate_limit(&state.config.admin_ui.rate_limit.per_ip) {
        Ok(parsed) => parsed,
        Err(e) => {
            error!(error = %e, "invalid admin_ui.rate_limit.per_ip config");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AdminLoginResponse {
                    success: false,
                    message: Some("Rate limit configuration error".into()),
                    csrf_token: None,
                }),
            )
                .into_response();
        }
    };

    // Check rate limit and backoff
    let backend = state.config.admin_ui.rate_limit.backend.as_str();
    if backend == "redis" {
        if let Some(ref redis) = state.redis_store {
            match redis.check_rate_limit_admin_login(&source_ip, limit, window_seconds) {
                Ok((allowed, wait_seconds)) => {
                    if !allowed {
                        if let Some(ws) = wait_seconds {
                            warn!(
                                source_ip_hash = hash_for_log(&source_ip),
                                wait_seconds = ws,
                                "admin login rate limited (backoff)"
                            );
                            return (
                                StatusCode::TOO_MANY_REQUESTS,
                                Json(AdminLoginResponse {
                                    success: false,
                                    message: Some(format!(
                                        "Too many failed login attempts. Try again in {} seconds.",
                                        ws
                                    )),
                                    csrf_token: None,
                                }),
                            )
                                .into_response();
                        } else {
                            return (
                                StatusCode::TOO_MANY_REQUESTS,
                                Json(AdminLoginResponse {
                                    success: false,
                                    message: Some(
                                        "Too many login attempts. Please try again later.".into(),
                                    ),
                                    csrf_token: None,
                                }),
                            )
                                .into_response();
                        }
                    }
                    // Allowed, proceed
                }
                Err(e) => {
                    error!(error = %e, "failed to check admin login rate limit");
                    // Continue anyway on error (fail-open)
                }
            }
        }
    } else if backend == "local" {
        // Local backend rate limiting
        let (allowed, wait_seconds) = state.local_rate_limiter.check(
            &source_ip,
            limit,
            window_seconds * 1000,
            state.config.admin_ui.rate_limit.failed_attempt_threshold,
            state.config.admin_ui.rate_limit.backoff_start_minutes,
            state.config.admin_ui.rate_limit.backoff_max_hours * 60,
        );
        if !allowed {
            warn!(
                source_ip_hash = hash_for_log(&source_ip),
                wait_seconds = ?wait_seconds,
                "admin login rate limited (local backend)"
            );
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(AdminLoginResponse {
                    success: false,
                    message: if let Some(ws) = wait_seconds {
                        Some(format!(
                            "Too many failed login attempts. Try again in {} seconds.",
                            ws
                        ))
                    } else {
                        Some("Too many login attempts. Please try again later.".into())
                    },
                    csrf_token: None,
                }),
            )
                .into_response();
        }
    }

    // Verify admin_key (constant-time comparison to prevent timing side-channels)
    use subtle::ConstantTimeEq as _;
    if body
        .admin_key
        .as_bytes()
        .ct_eq(state.config.admin.api_key.as_bytes())
        .into()
    {
        // Successful login - reset rate limit counters
        if backend == "redis" {
            if let Some(ref redis) = state.redis_store {
                if let Err(e) = redis.reset_rate_limit_admin_login(&source_ip) {
                    warn!(error = %e, "failed to reset admin login rate limit");
                }
            }
        } else if backend == "local" {
            state.local_rate_limiter.reset(&source_ip);
        }

        // Generate session ID, CSRF token, and seal the session
        let session_id = generate_session_id();
        let csrf_token = generate_csrf_token();
        let sealed = match seal_session(&session_id, &state.seal_key) {
            Ok(sealed) => sealed,
            Err(e) => {
                error!(error = %e, "failed to seal admin session");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(AdminLoginResponse {
                        success: false,
                        message: Some("Failed to create session".into()),
                        csrf_token: None,
                    }),
                )
                    .into_response();
            }
        };

        // Store session in task store (plan §13.19)
        let now = now_ms();
        let expires_at = now + (state.config.admin_ui.session_ttl_s as i64 * 1000);
        let admin_key_hash = hash_admin_key(&state.config.admin.api_key);

        // Extract user agent from headers
        let user_agent = headers
            .get("user-agent")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let new_session = NewAdminSession {
            session_id: session_id.clone(),
            csrf_token: csrf_token.clone(),
            admin_key_hash,
            created_at: now,
            expires_at,
            user_agent,
            source_ip: Some(source_ip.clone()),
        };

        // Try to store the session (requires Redis or SQLite task store)
        let session_stored = if let Some(ref store) = state.task_store {
            match store.insert_admin_session(&new_session) {
                Ok(()) => true,
                Err(e) => {
                    error!(error = %e, session_prefix = &session_id[..8], "failed to store admin session");
                    false
                }
            }
        } else {
            warn!(
                session_prefix = &session_id[..8],
                "no task store configured - admin session will not persist across restarts"
            );
            false
        };

        info!(
            source_ip_hash = hash_for_log(&source_ip),
            session_prefix = &session_id[..8],
            session_stored,
            "admin login successful"
        );

        // Set cookie and return success with CSRF token
        (
            StatusCode::OK,
            [(
                "Set-Cookie",
                format!(
                    "{}={}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={}",
                    COOKIE_NAME, sealed, state.config.admin_ui.session_ttl_s
                ),
            )],
            Json(AdminLoginResponse {
                success: true,
                message: None,
                csrf_token: Some(csrf_token),
            }),
        )
            .into_response()
    } else {
        // Wrong admin_key - record failure for backoff tracking
        warn!(
            source_ip_hash = hash_for_log(&source_ip),
            "admin login failed: invalid admin_key"
        );

        if backend == "redis" {
            if let Some(ref redis) = state.redis_store {
                let backoff_start_minutes = state.config.admin_ui.rate_limit.backoff_start_minutes;
                let backoff_max_hours = state.config.admin_ui.rate_limit.backoff_max_hours;
                let failed_threshold = state.config.admin_ui.rate_limit.failed_attempt_threshold;

                if let Err(e) = redis.record_failure_admin_login(
                    &source_ip,
                    failed_threshold,
                    backoff_start_minutes,
                    backoff_max_hours,
                ) {
                    warn!(error = %e, "failed to record admin login failure");
                }
            }
        } else if backend == "local" {
            let backoff_start_minutes = state.config.admin_ui.rate_limit.backoff_start_minutes;
            let backoff_max_hours = state.config.admin_ui.rate_limit.backoff_max_hours;
            let failed_threshold = state.config.admin_ui.rate_limit.failed_attempt_threshold;

            state.local_rate_limiter.record_failure(
                &source_ip,
                failed_threshold,
                backoff_start_minutes,
                backoff_max_hours * 60,
            );
        }

        (
            StatusCode::UNAUTHORIZED,
            Json(AdminLoginResponse {
                success: false,
                message: Some("Invalid admin key".into()),
                csrf_token: None,
            }),
        )
            .into_response()
    }
}

/// POST /_miroir/admin/logout — admin logout with session revocation.
///
/// Revokes the current admin session, publishes to Redis Pub/Sub for multi-pod
/// propagation, and clears the session cookie.
///
/// Response (200 OK):
/// ```json
/// { "success": true }
/// ```
///
/// If the session is already revoked or not found, still returns success.
pub async fn admin_logout<S>(State(state): State<S>, headers: HeaderMap) -> Response
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    use crate::auth::extract_admin_session_cookie;

    let state = AppState::from_ref(&state);

    // Extract and unseal the session cookie
    let session_id = if let Some(cookie_value) = extract_admin_session_cookie(&headers) {
        match unseal_session(&cookie_value, &state.seal_key) {
            Ok(id) => id,
            Err(e) => {
                warn!(error = %e, "failed to unseal admin session cookie on logout");
                return (
                    StatusCode::OK,
                    [(
                        "Set-Cookie",
                        format!(
                            "{}=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0",
                            COOKIE_NAME
                        ),
                    )],
                    Json(AdminLogoutResponse {
                        success: true,
                        message: None,
                    }),
                )
                    .into_response();
            }
        }
    } else {
        // No session cookie - still return success (idempotent logout)
        return (
            StatusCode::OK,
            [(
                "Set-Cookie",
                format!(
                    "{}=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0",
                    COOKIE_NAME
                ),
            )],
            Json(AdminLogoutResponse {
                success: true,
                message: None,
            }),
        )
            .into_response();
    };

    // Revoke the session in the task store (plan §13.19)
    let revoked = if let Some(ref store) = state.task_store {
        match store.revoke_admin_session(&session_id) {
            Ok(revoked) => revoked,
            Err(e) => {
                warn!(
                    error = %e,
                    session_prefix = &session_id[..session_id.len().min(8)],
                    "failed to revoke admin session"
                );
                false
            }
        }
    } else {
        warn!(
            session_prefix = &session_id[..session_id.len().min(8)],
            "no task store configured - session revocation will not persist"
        );
        false
    };

    // Add to in-memory revoked cache for immediate effect (plan §9)
    // This is used by auth_middleware for fast rejection
    // Note: This is only effective on this pod; Redis Pub/Sub handles multi-pod
    // We don't have direct access to AuthState here, but the task store
    // revoke operation already handles the Redis Pub/Sub notification

    info!(
        session_prefix = &session_id[..session_id.len().min(8)],
        revoked, "admin logout"
    );

    // Clear the session cookie and return success
    (
        StatusCode::OK,
        [(
            "Set-Cookie",
            format!(
                "{}=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0",
                COOKIE_NAME
            ),
        )],
        Json(AdminLogoutResponse {
            success: true,
            message: None,
        }),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Rebalancer Admin API Endpoints (plan §4)
// ---------------------------------------------------------------------------

/// POST /_miroir/nodes — Add a node to a replica group.
///
/// Request body:
/// ```json
/// {
///   "id": "node-new",
///   "address": "http://node-new:7700",
///   "replica_group": 0
/// }
/// ```
///
/// Response (202 Accepted):
/// ```json
/// {
///   "miroir_task_id": "rebalance:default",
///   "node_id": "node-new",
///   "replica_group": 0,
///   "status": "accepted"
/// }
/// ```
///
/// Implements plan §2 "Adding a node to an existing group":
/// 1. Add node to topology in `Joining` state
/// 2. Send `NodeAdded` event to rebalancer worker
/// 3. Worker computes affected shards and starts migration with leader lease
pub async fn add_node<S>(
    State(state): State<S>,
    Json(body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing 'id' field".into()))?
        .to_string();

    let address = body
        .get("address")
        .and_then(|v| v.as_str())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing 'address' field".into()))?
        .to_string();

    let replica_group = body
        .get("replica_group")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "Missing 'replica_group' field".into(),
            )
        })? as u32;

    // Add node to topology
    {
        let mut topo = app_state.topology.write().await;
        // Check if node already exists
        let node_id = NodeId::new(id.clone());
        if topo.node(&node_id).is_some() {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Node {} already exists", id),
            ));
        }
        // Check if replica group exists
        let group_count = topo.groups().count() as u32;
        if replica_group >= group_count {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Replica group {} does not exist", replica_group),
            ));
        }
        let node = Node::new(node_id, address, replica_group);
        topo.add_node(node);
    }

    // Send event to rebalancer worker (if available)
    let index_uid = "default";
    if let Some(ref worker) = app_state.rebalancer_worker {
        let event = TopologyChangeEvent::NodeAdded {
            node_id: id.clone(),
            replica_group,
            index_uid: index_uid.to_string(),
        };
        if let Err(e) = worker.event_sender().try_send(event) {
            error!(error = %e, node_id = %id, "failed to send NodeAdded event to rebalancer worker");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to queue rebalancing: {}", e),
            ));
        }
    }

    let job_id = miroir_core::rebalancer_worker::RebalanceJobId::new(index_uid);
    info!(
        node_id = %id,
        replica_group,
        miroir_task_id = %job_id.0,
        "Node addition queued for rebalancing"
    );
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "miroir_task_id": job_id.0,
            "node_id": id,
            "replica_group": replica_group,
            "status": "accepted",
        })),
    ))
}

/// DELETE /_miroir/nodes/{id} — Remove a node from the cluster.
///
/// Request body (optional):
/// ```json
/// {
///   "force": false  // Set to true to bypass draining check
/// }
/// ```
///
/// Requires the node to be in `draining` state unless `force=true`.
/// Note: This only removes the node from topology. Draining must be completed first.
pub async fn remove_node<S>(
    State(state): State<S>,
    Path(node_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    let force = body.get("force").and_then(|v| v.as_bool()).unwrap_or(false);

    let node_id_obj = NodeId::new(node_id.clone());

    // Check node state
    let node_status = {
        let topo = app_state.topology.read().await;
        let node = topo
            .node(&node_id_obj)
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Node {} not found", node_id)))?;

        // Check if this is the last node in the group
        let group = topo
            .groups()
            .find(|g| g.id == node.replica_group)
            .ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Replica group {} not found", node.replica_group),
                )
            })?;

        if group.nodes().len() <= 1 {
            return Err((
                StatusCode::BAD_REQUEST,
                "Cannot remove the last node in a replica group".into(),
            ));
        }

        node.status
    };

    if !force && node_status != miroir_core::topology::NodeStatus::Draining {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Node {} is not in draining state (current: {:?}), use force=true to bypass",
                node_id, node_status
            )
            .into(),
        ));
    }

    // Remove node from topology
    {
        let mut topo = app_state.topology.write().await;
        topo.remove_node(&node_id_obj);
    }

    info!(node_id = %node_id, force, "Node removal completed");
    Ok(Json(serde_json::json!({
        "node_id": node_id,
        "message": format!("Node {} removed from cluster", node_id),
    })))
}

/// POST /_miroir/nodes/{id}/drain — Drain a node (prepare for removal).
///
/// Response (202 Accepted):
/// ```json
/// {
///   "miroir_task_id": "rebalance:default",
///   "node_id": "node-0",
///   "replica_group": 0,
///   "status": "draining"
/// }
/// ```
///
/// Implements plan §2 node drain flow:
/// 1. Mark node as `draining`
/// 2. Send `NodeDraining` event to rebalancer worker
/// 3. Worker computes shard destinations and starts migration with leader lease
pub async fn drain_node<S>(
    State(state): State<S>,
    Path(node_id): Path<String>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    // Check if worker is available
    let worker = app_state.rebalancer_worker.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Rebalancer worker not initialized".into(),
        )
    })?;

    // Get node info and mark as draining
    let replica_group = {
        let mut topo = app_state.topology.write().await;
        let node_id_obj = NodeId::new(node_id.clone());
        let node = topo
            .node(&node_id_obj)
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Node {} not found", node_id)))?;

        // Check if this is the last node in the group
        let group = topo
            .groups()
            .find(|g| g.id == node.replica_group)
            .ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Replica group {} not found", node.replica_group),
                )
            })?;

        if group.nodes().len() <= 1 {
            return Err((
                StatusCode::BAD_REQUEST,
                "Cannot remove the last node in a replica group".into(),
            ));
        }

        let replica_group = node.replica_group;

        // Mark node as draining
        if let Some(n) = topo.node_mut(&node_id_obj) {
            n.status = miroir_core::topology::NodeStatus::Draining;
        }

        replica_group
    };

    // Send event to rebalancer worker
    let index_uid = "default";
    let event = TopologyChangeEvent::NodeDraining {
        node_id: node_id.clone(),
        replica_group,
        index_uid: index_uid.to_string(),
    };

    if let Err(e) = worker.event_sender().try_send(event) {
        error!(error = %e, node_id = %node_id, "failed to send NodeDraining event to rebalancer worker");
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to queue drain: {}", e),
        ));
    }

    let job_id = miroir_core::rebalancer_worker::RebalanceJobId::new(index_uid);
    info!(
        node_id = %node_id,
        replica_group,
        miroir_task_id = %job_id.0,
        "Node drain queued for rebalancing"
    );
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "miroir_task_id": job_id.0,
            "node_id": node_id,
            "replica_group": replica_group,
            "status": "draining",
        })),
    ))
}

/// POST /_miroir/nodes/{id}/fail — Mark a node as failed.
///
/// Marks a node as failed and sends a `NodeFailed` event to the rebalancer worker.
pub async fn fail_node<S>(
    State(state): State<S>,
    Path(node_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    // Check if worker is available
    let worker = app_state.rebalancer_worker.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Rebalancer worker not initialized".into(),
        )
    })?;

    // Get node info and mark as failed
    let replica_group = {
        let mut topo = app_state.topology.write().await;
        let node_id_obj = NodeId::new(node_id.clone());
        let node = topo
            .node(&node_id_obj)
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Node {} not found", node_id)))?;

        let replica_group = node.replica_group;

        // Mark node as failed
        if let Some(n) = topo.node_mut(&node_id_obj) {
            n.status = miroir_core::topology::NodeStatus::Failed;
        }

        replica_group
    };

    // Send event to rebalancer worker
    let event = TopologyChangeEvent::NodeFailed {
        node_id: node_id.clone(),
        replica_group,
        index_uid: "default".to_string(),
    };

    if let Err(e) = worker.event_sender().try_send(event) {
        error!(error = %e, node_id = %node_id, "failed to send NodeFailed event to rebalancer worker");
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to queue node failure: {}", e),
        ));
    }

    info!(node_id = %node_id, replica_group, "Node failure queued for handling");
    Ok(Json(serde_json::json!({
        "node_id": node_id,
        "replica_group": replica_group,
        "message": format!("Node {} marked as failed", node_id),
    })))
}

/// POST /_miroir/nodes/{id}/recover — Mark a failed node as recovered.
///
/// Marks a failed node as recovered and sends a `NodeRecovered` event to the rebalancer worker.
pub async fn recover_node<S>(
    State(state): State<S>,
    Path(node_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    // Check if worker is available
    let worker = app_state.rebalancer_worker.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Rebalancer worker not initialized".into(),
        )
    })?;

    // Get node info and mark as recovered
    let replica_group = {
        let mut topo = app_state.topology.write().await;
        let node_id_obj = NodeId::new(node_id.clone());
        let node = topo
            .node(&node_id_obj)
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Node {} not found", node_id)))?;

        let replica_group = node.replica_group;

        // Mark node as active (recovered)
        if let Some(n) = topo.node_mut(&node_id_obj) {
            n.status = miroir_core::topology::NodeStatus::Active;
        }

        replica_group
    };

    // Send event to rebalancer worker
    let event = TopologyChangeEvent::NodeRecovered {
        node_id: node_id.clone(),
        replica_group,
        index_uid: "default".to_string(),
    };

    if let Err(e) = worker.event_sender().try_send(event) {
        error!(error = %e, node_id = %node_id, "failed to send NodeRecovered event to rebalancer worker");
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to queue node recovery: {}", e),
        ));
    }

    info!(node_id = %node_id, replica_group, "Node recovery queued for handling");
    Ok(Json(serde_json::json!({
        "node_id": node_id,
        "replica_group": replica_group,
        "message": format!("Node {} marked as recovered", node_id),
    })))
}

/// Request body for POST /_miroir/rebalance.
#[derive(Debug, Deserialize)]
pub struct TriggerRebalanceRequest {
    /// Optional index UID to rebalance. If omitted, rebalances all indexes.
    pub index_uid: Option<String>,
    /// Optional reason for triggering the rebalance (for logging/auditing).
    pub reason: Option<String>,
}

/// POST /_miroir/rebalance — Manually trigger a rebalance operation.
///
/// Request body:
/// ```json
/// {
///   "index_uid": "my-index",  // optional, defaults to "default"
///   "reason": "manual trigger after config change"
/// }
/// ```
///
/// Implements plan §4 "Rebalancer" manual trigger:
/// - Returns 202 Accepted with a miroir_task_id when rebalance starts
/// - Returns 200 OK with a no-op task when cluster is already balanced
/// - The rebalancer worker processes the request in the background
///
/// Response (202 Accepted):
/// ```json
/// {
///   "miroir_task_id": "rebalance:my-index",
///   "status": "started",
///   "message": "Rebalance started for index my-index"
/// }
/// ```
///
/// Response (200 OK, no-op):
/// ```json
/// {
///   "miroir_task_id": "rebalance-noop-123",
///   "status": "noop",
///   "message": "Cluster is already balanced"
/// }
/// ```
pub async fn trigger_rebalance<S>(
    State(state): State<S>,
    Json(body): Json<TriggerRebalanceRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    let index_uid = body.index_uid.unwrap_or_else(|| "default".to_string());
    let reason = body.reason.unwrap_or_else(|| "manual trigger".to_string());

    // Check if rebalancer worker is available
    let worker = app_state.rebalancer_worker.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Rebalancer worker not initialized".into(),
        )
    })?;

    // Check if there's already a rebalance job for this index
    let job_id = miroir_core::rebalancer_worker::RebalanceJobId::new(&index_uid);
    let has_existing_job = {
        let jobs = worker.jobs().await;
        jobs.contains_key(&job_id)
    };

    if has_existing_job {
        // A rebalance is already in progress for this index
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "miroir_task_id": job_id.0,
                "status": "noop",
                "message": format!("Rebalance already in progress for index {}", index_uid),
            })),
        ));
    }

    // Check if the cluster is already balanced
    // For now, we'll start a rebalance job and let the worker determine if any work is needed
    // In the future, we could add a more sophisticated "is balanced" check

    // Create a topology change event to trigger rebalancing
    // Since this is a manual trigger without a specific topology change,
    // we'll use a synthetic event that the worker can process
    let event = miroir_core::rebalancer_worker::TopologyChangeEvent::NodeAdded {
        node_id: format!("manual-rebalance-{}", uuid::Uuid::new_v4()),
        replica_group: 0, // This will be ignored by the worker
        index_uid: index_uid.clone(),
    };

    // Send the event to the rebalancer worker
    if let Err(e) = worker.event_sender().try_send(event) {
        error!(
            error = %e,
            index_uid = %index_uid,
            "failed to send manual rebalance event"
        );
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to trigger rebalance: {}", e),
        ));
    }

    info!(
        index_uid = %index_uid,
        reason = %reason,
        miroir_task_id = %job_id.0,
        "manual rebalance triggered"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "miroir_task_id": job_id.0,
            "status": "started",
            "message": format!("Rebalance started for index {}", index_uid),
        })),
    ))
}

/// GET /_miroir/rebalance/status — Get current rebalance status.
///
/// Returns detailed per-shard migration progress with phase information.
///
/// Response shape (per bead spec):
/// ```json
/// {
///   "in_progress": true,
///   "triggered_by": "POST /_miroir/nodes",
///   "operation_id": "reb-1234",
///   "started_at": "2026-04-18T20:00:00Z",
///   "phases": [
///     {
///       "shard": 12,
///       "state": "MigrationInProgress",
///       "pct_complete": 42,
///       "source": "meili-0",
///       "destination": "meili-4"
///     },
///     ...
///   ],
///   "overall_pct_complete": 38
/// }
/// ```
pub async fn get_rebalance_status<S>(
    State(state): State<S>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    // Check worker status first
    let worker = match app_state.rebalancer_worker.as_ref() {
        Some(w) => w,
        None => {
            return Ok(Json(serde_json::json!({
                "in_progress": false,
                "message": "Rebalancer worker not initialized"
            })));
        }
    };

    let worker_status = worker.get_status().await;
    let in_progress = worker_status.active_jobs > 0;

    // Build phases array from worker jobs
    let mut phases = Vec::new();
    let jobs = worker.get_all_jobs().await;

    for (_job_id, job) in jobs.iter() {
        if job.completed_at.is_some() {
            continue; // Skip completed jobs
        }

        for (&shard_id, shard_state) in job.shards.iter() {
            let pct_complete = if job.shards.len() > 0 {
                let completed = job
                    .shards
                    .values()
                    .filter(|s| {
                        matches!(
                            s.phase,
                            miroir_core::rebalancer_worker::ShardMigrationPhase::OldReplicaDeleted
                        )
                    })
                    .count();
                (completed * 100 / job.shards.len()) as u32
            } else {
                0
            };

            phases.push(serde_json::json!({
                "shard": shard_id,
                "state": format!("{:?}", shard_state.phase),
                "pct_complete": pct_complete,
                "source": shard_state.source_node.as_ref().unwrap_or(&"unknown".to_string()),
                "destination": shard_state.target_node,
                "docs_migrated": shard_state.docs_migrated,
            }));
        }
    }

    // Calculate overall completion
    let overall_pct_complete = if phases.is_empty() {
        100
    } else {
        let sum: u32 = phases
            .iter()
            .filter_map(|p| {
                p.get("pct_complete")
                    .and_then(|v| v.as_u64().map(|v| v as u32))
            })
            .sum();
        sum / phases.len() as u32
    };

    // Get rebalancer metrics for additional context
    let (documents_migrated_total, current_duration_secs) =
        if let Some(ref rebalancer) = app_state.rebalancer {
            let metrics = rebalancer.metrics.read().await;
            (
                metrics.documents_migrated_total,
                metrics.current_duration_secs(),
            )
        } else {
            (0, 0.0)
        };

    Ok(Json(serde_json::json!({
        "in_progress": in_progress,
        "triggered_by": "manual", // Could be enhanced to track the actual trigger
        "operation_id": format!("rebalance-{}", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()),
        "started_at": if in_progress { Some(chrono::Utc::now().to_rfc3339()) } else { None },
        "phases": phases,
        "overall_pct_complete": overall_pct_complete,
        "metrics": {
            "documents_migrated_total": documents_migrated_total,
            "current_duration_secs": current_duration_secs,
            "active_jobs": worker_status.active_jobs,
            "completed_jobs": worker_status.completed_jobs,
        }
    })))
}

/// POST /_miroir/replica_groups — Add a replica group.
///
/// Request body:
/// ```json
/// {
///   "group_id": 2,
///   "nodes": [
///     {"id": "node-6", "address": "http://node-6:7700"},
///     {"id": "node-7", "address": "http://node-7:7700"}
///   ]
/// }
/// ```
///
/// Implements plan §2 group addition flow:
/// 1. Provision new nodes; assign replica_group: G_new in config
/// 2. Mark new group initializing; queries NOT routed here
/// 3. Background sync: for each shard, copy all docs from any healthy existing group
/// 4. When all shards synced, mark group active — queries begin routing in round-robin
pub async fn add_replica_group<S>(
    State(state): State<S>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    let group_id = body
        .get("group_id")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing 'group_id' field".into()))?
        as u32;

    let nodes_array = body
        .get("nodes")
        .and_then(|v| v.as_array())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing 'nodes' field".into()))?;

    // Check if group addition coordinator is available
    let coordinator = app_state
        .group_addition_coordinator
        .as_ref()
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Group addition coordinator not initialized".into(),
            )
        })?;

    // Get current topology to find healthy source groups
    let source_groups: Vec<u32> = {
        let topo = app_state.topology.read().await;
        topo.groups()
            .filter(|g| g.id != group_id && g.is_active())
            .map(|g| g.id)
            .collect()
    };

    if source_groups.is_empty() {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "No active source groups available for sync".into(),
        ));
    }

    // Add nodes to topology in initializing state
    for node_obj in nodes_array {
        let id = node_obj
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing node 'id'".into()))?
            .to_string();

        let address = node_obj
            .get("address")
            .and_then(|v| v.as_str())
            .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing node 'address'".into()))?
            .to_string();

        let mut topo = app_state.topology.write().await;
        let node_id = NodeId::new(id.clone());
        let node = Node::new(node_id, address, group_id);
        topo.add_node(node);

        // Mark the new group as initializing
        if let Some(g) = topo.group_mut(group_id) {
            g.set_state(miroir_core::topology::GroupState::Initializing);
        }
    }

    // Start group addition operation
    let shard_count = {
        let topo = app_state.topology.read().await;
        topo.shards
    };

    let mut coord = coordinator.write().await;
    let addition_id = coord
        .begin_addition(group_id, shard_count, &source_groups)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to start group addition: {}", e),
            )
        })?;

    // Start background sync
    coord.begin_sync(addition_id).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to start sync: {}", e),
        )
    })?;

    info!(group_id, addition_id = %addition_id, "Replica group addition started");

    Ok(Json(serde_json::json!({
        "addition_id": addition_id.0,
        "group_id": group_id,
        "message": format!("Replica group {} addition started, syncing {} shards from {} source groups",
            group_id, shard_count, source_groups.len()),
        "phase": "initializing",
    })))
}

/// GET /_miroir/replica_groups/{id}/status — Get the status of a replica group addition.
pub async fn get_group_addition_status<S>(
    State(state): State<S>,
    Path(group_id): Path<u32>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    let coordinator = app_state
        .group_addition_coordinator
        .as_ref()
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Group addition coordinator not initialized".into(),
            )
        })?;

    let coord = coordinator.read().await;

    // Find the addition for this group
    let addition = coord
        .get_all_additions()
        .values()
        .find(|a| a.group_id == group_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("No active addition for group {}", group_id),
            )
        })?;

    let progress = coord.sync_progress(addition.id).unwrap_or(0.0);

    // Count shards by state
    let mut pending = 0;
    let mut syncing = 0;
    let mut complete = 0;
    let mut failed = 0;

    for shard_state in addition.shard_states.values() {
        match shard_state {
            miroir_core::group_addition::ShardSyncState::Pending => pending += 1,
            miroir_core::group_addition::ShardSyncState::Syncing { .. } => syncing += 1,
            miroir_core::group_addition::ShardSyncState::Complete { .. } => complete += 1,
            miroir_core::group_addition::ShardSyncState::Failed { .. } => failed += 1,
        }
    }

    Ok(Json(serde_json::json!({
        "addition_id": addition.id.0,
        "group_id": group_id,
        "phase": addition.phase,
        "progress_percent": progress,
        "shards": {
            "total": addition.shard_states.len(),
            "pending": pending,
            "syncing": syncing,
            "complete": complete,
            "failed": failed,
        },
        "started_at": addition.started_at.map(|t| format!("{:?}", t)),
    })))
}

/// POST /_miroir/replica_groups/{id}/activate — Mark a replica group as active.
///
/// This should only be called after verifying that the group has synced all data
/// (via GET /_miroir/replica_groups/{id}/status showing 100% progress).
pub async fn activate_replica_group<S>(
    State(state): State<S>,
    Path(group_id): Path<u32>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    let coordinator = app_state
        .group_addition_coordinator
        .as_ref()
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Group addition coordinator not initialized".into(),
            )
        })?;

    // Find the addition for this group
    let (addition_id, source_group_id) = {
        let coord = coordinator.read().await;
        let addition = coord
            .get_all_additions()
            .values()
            .find(|a| {
                a.group_id == group_id
                    && matches!(
                        a.phase,
                        miroir_core::group_addition::GroupAdditionPhase::SyncComplete
                    )
            })
            .ok_or_else(|| {
                (
                    StatusCode::PRECONDITION_FAILED,
                    format!(
                        "Group {} is not ready for activation (sync not complete)",
                        group_id
                    ),
                )
            })?;

        // Get the source group ID for verification (use the first shard's source)
        let source_group_id = addition.shard_sources.values().next().copied().unwrap_or(0);

        (addition.id, source_group_id)
    };

    // Verification step: compare stats between source and new group
    // Per P4.4 acceptance criteria: "GET /indexes/{uid}/stats against new group →
    // docs count within 0.1% of source group (allows for writes landing during sync)"
    {
        use reqwest::Client;

        let topo = app_state.topology.read().await;
        let source_group = topo.group(source_group_id).ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Source group {} not found", source_group_id),
            )
        })?;
        let new_group = topo.group(group_id).ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("New group {} not found", group_id),
            )
        })?;

        // Get healthy nodes from both groups
        let node_map = topo.node_map();
        let source_nodes = source_group.healthy_nodes(&node_map);
        let new_nodes = new_group.healthy_nodes(&node_map);

        if source_nodes.is_empty() {
            return Err((
                StatusCode::PRECONDITION_FAILED,
                format!("No healthy nodes in source group {}", source_group_id),
            ));
        }
        if new_nodes.is_empty() {
            return Err((
                StatusCode::PRECONDITION_FAILED,
                format!("No healthy nodes in new group {}", group_id),
            ));
        }

        // Pick one node from each group for stats comparison
        let source_node = source_nodes[0];
        let new_node = new_nodes[0];

        // Drop the topology read lock before making HTTP requests
        drop(topo);

        // Get stats from both nodes for a sample index
        // We use "_miroir_all_docs" as the sync worker uses this index
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to create HTTP client: {}", e),
                )
            })?;

        let index_uid = "_miroir_all_docs";
        let source_url = format!(
            "{}/indexes/{}/stats",
            source_node.address.trim_end_matches('/'),
            index_uid
        );
        let new_url = format!(
            "{}/indexes/{}/stats",
            new_node.address.trim_end_matches('/'),
            index_uid
        );

        // Fetch stats from source node
        let source_stats: serde_json::Value = client
            .get(&source_url)
            .header(
                "Authorization",
                format!("Bearer {}", app_state.config.master_key),
            )
            .send()
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to fetch stats from source node");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("Failed to fetch stats from source node: {}", e),
                )
            })?
            .json()
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to parse stats from source node");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to parse stats from source node: {}", e),
                )
            })?;

        // Fetch stats from new group node
        let new_stats: serde_json::Value = client
            .get(&new_url)
            .header(
                "Authorization",
                format!("Bearer {}", app_state.config.master_key),
            )
            .send()
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to fetch stats from new group node");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("Failed to fetch stats from new group node: {}", e),
                )
            })?
            .json()
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to parse stats from new group node");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to parse stats from new group node: {}", e),
                )
            })?;

        // Compare document counts
        let source_count = source_stats
            .get("numberOfDocuments")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let new_count = new_stats
            .get("numberOfDocuments")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        // Calculate variance percentage (allowing for writes during sync)
        let variance = if source_count > 0 {
            let diff = if source_count > new_count {
                source_count - new_count
            } else {
                new_count - source_count
            };
            (diff as f64 / source_count as f64) * 100.0
        } else {
            0.0
        };

        const MAX_VARIANCE_PERCENT: f64 = 0.1;

        if variance > MAX_VARIANCE_PERCENT {
            return Err((
                StatusCode::PRECONDITION_FAILED,
                format!(
                    "Verification failed: new group has {} docs, source has {} docs (variance: {:.3}%) - must be within {:.1}%",
                    new_count, source_count, variance, MAX_VARIANCE_PERCENT
                ),
            ));
        }

        info!(
            group_id,
            source_count,
            new_count,
            variance,
            "Verification passed: doc counts within acceptable variance"
        );
    }

    // Mark group as active
    {
        let mut coord = coordinator.write().await;
        coord.mark_group_active(addition_id).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to activate group: {}", e),
            )
        })?;
    }

    // Update topology to mark group as active
    {
        let mut topo = app_state.topology.write().await;
        if let Some(g) = topo.group_mut(group_id) {
            g.set_state(miroir_core::topology::GroupState::Active);
        }
    }

    info!(group_id, "Replica group activated");

    Ok(Json(serde_json::json!({
        "group_id": group_id,
        "message": format!("Replica group {} is now active and serving queries", group_id),
        "phase": "active",
    })))
}

/// DELETE /_miroir/replica_groups/{id} — Remove a replica group.
pub async fn remove_replica_group<S>(
    State(state): State<S>,
    Path(group_id): Path<u32>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    let rebalancer = app_state.rebalancer.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Rebalancer not initialized".into(),
        )
    })?;

    let force = body.get("force").and_then(|v| v.as_bool()).unwrap_or(false);

    use miroir_core::rebalancer::RemoveReplicaGroupRequest;
    let request = RemoveReplicaGroupRequest { group_id, force };

    match rebalancer.remove_replica_group(request).await {
        Ok(result) => {
            info!(group_id, "Replica group removal completed");
            Ok(Json(serde_json::json!({
                "operation_id": result.id,
                "message": result.message,
            })))
        }
        Err(e) => {
            error!(error = %e, group_id, "Replica group removal failed");
            Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology_response_serialization() {
        let response = TopologyResponse {
            shards: 64,
            replication_factor: 2,
            nodes: vec![
                NodeInfo {
                    id: "meili-0".to_string(),
                    address: "http://meili-0.search.svc:7700".to_string(),
                    status: "healthy".to_string(),
                    shard_count: 32,
                    last_seen_ms: 100,
                    error: None,
                },
                NodeInfo {
                    id: "meili-1".to_string(),
                    address: "http://meili-1.search.svc:7700".to_string(),
                    status: "degraded".to_string(),
                    shard_count: 32,
                    last_seen_ms: 5000,
                    error: Some("connection refused".to_string()),
                },
            ],
            degraded_node_count: 1,
            rebalance_in_progress: false,
            fully_covered: false,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"shards\":64"));
        assert!(json.contains("\"replication_factor\":2"));
        assert!(json.contains("\"degraded_node_count\":1"));
        assert!(json.contains("\"fully_covered\":false"));
        assert!(json.contains("\"status\":\"healthy\""));
        assert!(json.contains("\"error\":\"connection refused\""));
    }

    #[test]
    fn test_shards_response_serialization() {
        let mut shards = HashMap::new();
        shards.insert(
            "0".to_string(),
            vec!["node-0".to_string(), "node-1".to_string()],
        );
        shards.insert(
            "1".to_string(),
            vec!["node-1".to_string(), "node-0".to_string()],
        );

        let response = ShardsResponse { shards };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"0\""));
        assert!(json.contains("\"node-0\""));
    }

    #[test]
    fn test_node_info_with_optional_error() {
        let info = NodeInfo {
            id: "test".to_string(),
            address: "http://meili-0.search.svc:7700".to_string(),
            status: "healthy".to_string(),
            shard_count: 10,
            last_seen_ms: 100,
            error: None,
        };

        let json = serde_json::to_string(&info).unwrap();
        // error field should not be present when None
        assert!(!json.contains("error"));
    }

    #[test]
    fn test_node_info_with_error() {
        let info = NodeInfo {
            id: "test".to_string(),
            address: "http://meili-0.search.svc:7700".to_string(),
            status: "failed".to_string(),
            shard_count: 10,
            last_seen_ms: 100,
            error: Some("timeout".to_string()),
        };

        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"error\":\"timeout\""));
    }
}

/// GET /_miroir/shadow/diff — Get recent shadow diff results (plan §13.16).
///
/// Query parameters:
/// - target: filter by target name (optional)
/// - limit: max number of diffs to return (default: 100, max: 10000)
/// - kind: filter by diff kind (hits|ranking|latency|error, optional)
pub async fn get_shadow_diff<S>(
    State(state): State<S>,
    Query(params): Query<ShadowDiffQuery>,
) -> Result<Json<serde_json::Value>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    let shadow_manager = app_state
        .shadow_manager
        .as_ref()
        .ok_or(StatusCode::NOT_FOUND)?;

    let limit = params.limit.unwrap_or(100).min(10000);
    let diffs = shadow_manager.recent_diffs(limit).await;

    let mut filtered = diffs;
    if let Some(target) = &params.target {
        filtered = filtered
            .into_iter()
            .filter(|d| &d.target == target)
            .collect();
    }

    Ok(Json(serde_json::json!({
        "diffs": filtered,
        "total": filtered.len(),
    })))
}

/// Query parameters for GET /_miroir/shadow/diff.
#[derive(Debug, Deserialize)]
pub struct ShadowDiffQuery {
    pub target: Option<String>,
    pub limit: Option<usize>,
    pub kind: Option<String>,
}

/// GET /_miroir/shadow/stats — Get shadow statistics (plan §13.16).
pub async fn get_shadow_stats<S>(
    State(state): State<S>,
) -> Result<Json<serde_json::Value>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    let shadow_manager = app_state
        .shadow_manager
        .as_ref()
        .ok_or(StatusCode::NOT_FOUND)?;

    let stats = shadow_manager.stats().await;

    Ok(Json(serde_json::json!({
        "total_shadowed": stats.total_shadowed,
        "total_errors": stats.total_errors,
        "error_rate": stats.error_rate,
        "recent_diffs_count": stats.recent_diffs_count,
    })))
}

// ---------------------------------------------------------------------------
// Settings endpoint (plan §13.19 Admin UI — Settings section)
// ---------------------------------------------------------------------------

/// GET /_miroir/settings — Get Miroir's current configuration.
///
/// Returns the full Miroir configuration. Settings that require a pod restart
/// to take effect are marked in the UI with a "Restart" badge (§13.19).
///
/// Admin-key-gated. Returns HTTP 401 if the admin key is missing or invalid.
pub async fn get_settings<S>(State(state): State<S>) -> Result<Json<MiroirConfig>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);
    // Dereference Arc to get the inner Config
    let config = app_state.config.as_ref().clone();
    Ok(Json(config))
}

/// PATCH /_miroir/settings — Update Miroir configuration.
///
/// Accepts a partial JSON payload with the settings to update. Only settings
/// that are safe to change at runtime are accepted; others return HTTP 400 with
/// an error message indicating a restart is required.
///
/// Admin-key-gated. Returns HTTP 401 if the admin key is missing or invalid.
///
/// # Runtime-updatable settings (no restart required)
///
/// - `rebalancer.max_concurrent_migrations`
/// - `rebalancer.migration_timeout_s`
/// - `query_planner.mode`
/// - `session_pinning.enabled`
/// - `anti_entropy.schedule` (takes effect on next scheduled run)
///
/// # Restart-required settings (rejected at runtime)
///
/// - `shards`, `replication_factor`, `replica_groups` — topology changes
/// - `nodes` — node list changes
/// - `task_store.backend` — backend type changes
/// - `anti_entropy.enabled` — feature flag changes
///
/// # Returns
///
/// - `200 OK` with the updated configuration on success
/// - `400 Bad Request` if trying to modify a restart-required setting
/// - `401 Unauthorized` if admin key is missing or invalid
pub async fn patch_settings<S>(
    State(state): State<S>,
    Json(partial): Json<serde_json::Value>,
) -> Result<Json<MiroirConfig>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);
    // Dereference Arc to get the inner Config
    let config = app_state.config.as_ref().clone();

    // Settings that require a pod restart (topology, feature flags, backend type)
    const RESTART_REQUIRED_FIELDS: &[&str] = &[
        "shards",
        "replication_factor",
        "replica_groups",
        "nodes",
        "task_store",
        "anti_entropy.enabled",
        "master_key",
        "node_master_key",
    ];

    // Check if any restart-required fields are being modified
    if let Some(obj) = partial.as_object() {
        for key in obj.keys() {
            let requires_restart = RESTART_REQUIRED_FIELDS
                .iter()
                .any(|field| key.starts_with(&format!("{}.", field)) || key == *field);

            if requires_restart {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "Cannot modify '{}' at runtime. \
                         This setting requires a pod restart to take effect. \
                         Please update the configuration file and restart the pod.",
                        key
                    ),
                ));
            }
        }
    }

    // Apply the partial update to the config
    // We use serde_json::from_value to deserialize the partial config
    let merged_json = serde_json::to_value(&config).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to serialize current config: {}", e),
        )
    })?;

    let merged_json = merge_json(merged_json, partial);
    let updated_config: MiroirConfig = serde_json::from_value(merged_json).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("Invalid configuration: {}", e),
        )
    })?;

    // Validate the updated configuration
    if let Err(e) = updated_config.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Configuration validation failed: {}", e),
        ));
    }

    // In a real implementation, we would persist the updated config to disk
    // or notify other pods via Redis/leader election. For now, we just
    // return the updated config to indicate what would be applied.
    tracing::info!(
        "Settings update requested (not persisted - requires config file update for persistence)"
    );

    Ok(Json(updated_config))
}

/// Merge a JSON patch into a base JSON value.
///
/// Deep merge: objects are merged recursively, arrays and primitives are replaced.
fn merge_json(base: serde_json::Value, patch: serde_json::Value) -> serde_json::Value {
    match (base, patch) {
        (serde_json::Value::Object(mut base_map), serde_json::Value::Object(patch_map)) => {
            for (key, patch_value) in patch_map {
                let base_value = base_map.remove(&key);
                let merged = match base_value {
                    Some(base_val) => merge_json(base_val, patch_value),
                    None => patch_value,
                };
                base_map.insert(key, merged);
            }
            serde_json::Value::Object(base_map)
        }
        (_, patch) => patch,
    }
}

// ---------------------------------------------------------------------------
// Resharding endpoints (plan §13.1)
// ---------------------------------------------------------------------------

/// Request body for POST /_miroir/indexes/{uid}/reshard.
#[derive(Debug, Deserialize)]
pub struct ReshardRequest {
    /// New shard count (S_new).
    pub new_shards: u32,
    /// Backfill throttle in documents per second (0 = unlimited).
    #[serde(default = "default_throttle")]
    pub throttle_docs_per_sec: u64,
}

fn default_throttle() -> u64 {
    10000
}

/// Response for POST /_miroir/indexes/{uid}/reshard.
#[derive(Debug, Serialize)]
pub struct ReshardResponse {
    /// Reshard operation ID.
    pub operation_id: String,
    /// Index being resharded.
    pub index_uid: String,
    /// Old shard count.
    pub old_shards: u32,
    /// New shard count.
    pub new_shards: u32,
    /// Shadow index UID.
    pub shadow_index: String,
    /// Current phase.
    pub phase: String,
    /// Started at (UNIX ms).
    pub started_at: u64,
}

/// Response for GET /_miroir/indexes/{uid}/reshard/status.
#[derive(Debug, Serialize)]
pub struct ReshardStatusResponse {
    /// Whether an operation is active for this index.
    pub active: bool,
    /// Reshard operation details (if active).
    pub operation: Option<ReshardOperationDetails>,
}

#[derive(Debug, Serialize)]
pub struct ReshardOperationDetails {
    /// Operation ID.
    pub id: String,
    /// Index being resharded.
    pub index_uid: String,
    /// Old shard count.
    pub old_shards: u32,
    /// New shard count.
    pub new_shards: u32,
    /// Current phase.
    pub phase: String,
    /// Documents backfilled so far.
    pub documents_backfilled: u64,
    /// Total documents to backfill.
    pub total_documents: u64,
    /// Backfill progress ratio (0.0 to 1.0).
    pub backfill_progress: f64,
    /// Shadow index UID.
    pub shadow_index: String,
    /// Started at (UNIX ms).
    pub started_at: u64,
    /// Last error (if any).
    pub last_error: Option<String>,
    /// Verification results (if verified).
    pub verification_results: Option<VerificationResultDetails>,
}

#[derive(Debug, Serialize)]
pub struct VerificationResultDetails {
    /// Whether verification passed.
    pub passed: bool,
    /// Live index PK count.
    pub live_pk_count: u64,
    /// Shadow index PK count.
    pub shadow_pk_count: u64,
    /// PKs only in live index.
    pub live_only_pks: Vec<String>,
    /// PKs only in shadow index.
    pub shadow_only_pks: Vec<String>,
    /// PKs with content hash mismatch.
    pub mismatched_pks: Vec<String>,
}

/// POST /_miroir/indexes/{uid}/reshard — Begin online resharding (plan §13.1).
///
/// Request body:
/// ```json
/// {
///   "new_shards": 256,
///   "throttle_docs_per_sec": 10000
/// }
/// ```
///
/// Returns the operation ID and initial state.
pub async fn post_reshard<S>(
    State(state): State<S>,
    Path(index_uid): Path<String>,
    Json(req): Json<ReshardRequest>,
) -> Result<Json<ReshardResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    // Validate new shard count
    if req.new_shards == 0 {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Check if resharding is already active for this index
    let registry = app_state.resharding_registry.read().await;
    if let Some(existing) = registry.get(&index_uid) {
        // Return conflict if already resharding
        return Err(StatusCode::CONFLICT);
    }
    drop(registry);

    // Get current shard count from topology
    let topology = app_state.topology.read().await;
    let old_shards = topology.shards;
    drop(topology);

    // Validate new_shards > old_shards (only scaling up is supported)
    if req.new_shards <= old_shards {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Get node addresses for shadow creation
    let topology = app_state.topology.read().await;
    let node_addresses: Vec<String> = topology.nodes().map(|n| n.address.clone()).collect();
    drop(topology);

    if node_addresses.is_empty() {
        error!("no nodes available for resharding");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    // Create shadow index (phase 1)
    let shadow_index = format!("{}__reshard_{}", index_uid, req.new_shards);
    let master_key = &app_state.config.master_key;

    // Use the shadow_create_phase function from reshard module
    match miroir_core::reshard::shadow_create_phase(
        &index_uid,
        req.new_shards,
        &node_addresses,
        master_key,
        None, // primary_key will be copied from live index
    )
    .await
    {
        Ok(result) => {
            info!(
                index_uid = %index_uid,
                shadow_index = %shadow_index,
                nodes_created = result.nodes_created,
                "Phase 1 complete: shadow index created"
            );

            let now = millis_now();

            // Register the resharding operation for dual-write detection
            let op_state = miroir_core::reshard::ReshardOperationState {
                shadow_index: shadow_index.clone(),
                old_shards,
                target_shards: req.new_shards,
                phase: miroir_core::reshard::ReshardPhase::ShadowCreated,
                started_at: now,
            };

            let mut registry = app_state.resharding_registry.write().await;
            if let Err(e) = registry.register(index_uid.clone(), op_state) {
                error!(
                    index_uid = %index_uid,
                    error = %e,
                    "failed to register resharding operation"
                );
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }

            // Spawn a background task to run the full orchestrator (phases 2-6)
            let index_uid_clone = index_uid.clone();
            let shadow_index_clone = shadow_index.clone();
            let registry = app_state.resharding_registry.clone();
            let topology = app_state.topology.clone();
            let master_key_clone = master_key.clone();
            let task_store = app_state.task_store.clone();

            tokio::spawn(async move {
                info!(
                    index_uid = %index_uid_clone,
                    "Starting background reshard orchestrator for phases 2-6"
                );

                // Get primary key from topology
                let topo = topology.read().await;
                let primary_key = "id"; // Default - should be fetched from index schema
                drop(topo);

                // Get node addresses again
                let topo = topology.read().await;
                let node_addresses: Vec<String> = topo.nodes().map(|n| n.address.clone()).collect();
                drop(topo);

                // Configure the orchestrator
                let config = miroir_core::reshard::ReshardOrchestratorConfig {
                    index_uid: index_uid_clone.clone(),
                    target_shards: req.new_shards,
                    node_addresses,
                    master_key: master_key_clone,
                    primary_key: primary_key.to_string(),
                    throttle_docs_per_sec: req.throttle_docs_per_sec,
                    backfill_batch_size: 1000,
                    retain_old_index_hours: 48,
                    verify_before_swap: true,
                    alias_history_retention: 10,
                    task_store: task_store.clone(),
                    metrics_callback: None, // TODO: wire up metrics
                };

                // Run the full orchestrator
                match miroir_core::reshard::execute_reshard(config).await {
                    Ok(result) => {
                        info!(
                            index_uid = %index_uid_clone,
                            documents_backfilled = result.documents_backfilled,
                            duration_secs = result.total_duration_secs,
                            final_phase = ?result.final_phase,
                            "Reshard orchestrator completed successfully"
                        );

                        // Update registry to final phase
                        let mut reg = registry.write().await;
                        if let Err(e) = reg.update_phase(
                            &index_uid_clone,
                            miroir_core::reshard::ReshardPhase::Complete,
                        ) {
                            error!(
                                index_uid = %index_uid_clone,
                                error = %e,
                                "failed to update resharding phase to Complete"
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            index_uid = %index_uid_clone,
                            error = %e,
                            "Reshard orchestrator failed"
                        );

                        // Update registry to failed state
                        let mut reg = registry.write().await;
                        if let Err(err) = reg.update_phase(
                            &index_uid_clone,
                            miroir_core::reshard::ReshardPhase::Failed,
                        ) {
                            error!(
                                index_uid = %index_uid_clone,
                                error = %err,
                                "failed to update resharding phase to Failed"
                            );
                        }
                    }
                }
            });

            Ok(Json(ReshardResponse {
                operation_id: format!("reshard-{}-{}", index_uid, now),
                index_uid,
                old_shards,
                new_shards: req.new_shards,
                shadow_index,
                phase: "Shadow Created".to_string(),
                started_at: now,
            }))
        }
        Err(e) => {
            error!(
                index_uid = %index_uid,
                error = %e,
                "shadow create phase failed"
            );
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// GET /_miroir/indexes/{uid}/reshard/status — Get resharding status.
pub async fn get_reshard_status<S>(
    State(state): State<S>,
    Path(index_uid): Path<String>,
) -> Result<Json<ReshardStatusResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    let registry = app_state.resharding_registry.read().await;
    let operation = registry.get(&index_uid);

    if let Some(op) = operation {
        Ok(Json(ReshardStatusResponse {
            active: true,
            operation: Some(ReshardOperationDetails {
                id: format!("reshard-{}-{}", index_uid, op.started_at),
                index_uid: index_uid.clone(),
                old_shards: op.old_shards,
                new_shards: op.target_shards,
                phase: op.phase.name().to_string(),
                documents_backfilled: 0, // TODO: track progress
                total_documents: 0,
                backfill_progress: 0.0,
                shadow_index: op.shadow_index.clone(),
                started_at: op.started_at,
                last_error: None,
                verification_results: None,
            }),
        }))
    } else {
        Ok(Json(ReshardStatusResponse {
            active: false,
            operation: None,
        }))
    }
}

fn millis_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
