//! Admin API endpoints for topology, readiness, shards, and metrics.

use axum::{
    extract::{FromRef, Path, State},
    http::{HeaderMap, StatusCode},
    Json,
    response::{IntoResponse, Response},
};
use miroir_core::{
    config::MiroirConfig,
    migration::{MigrationConfig, MigrationCoordinator},
    rebalancer::{MigrationExecutor, Rebalancer, RebalancerConfig, RebalancerMetrics},
    rebalancer_worker::{RebalancerWorker, RebalancerWorkerConfig},
    router,
    scatter::{DeleteByFilterRequest, FetchDocumentsRequest, FetchDocumentsResponse, WriteRequest},
    task_registry::TaskRegistryImpl,
    task_store::{RedisTaskStore, TaskStore},
    topology::{Node, NodeId, Topology},
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, error, warn};
use reqwest::Client;

use crate::{
    admin_session::{seal_session, COOKIE_NAME, SealKey},
    client::HttpClient,
    scoped_key_rotation::{self, ScopedKeyRotationState, RotateScopedKeyRequest, RotateScopedKeyResponse},
};

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
        state.request_timestamps_ms.retain(|&ts| now - ts < window_ms as i64);

        // Check if limit exceeded
        if state.request_timestamps_ms.len() >= limit as usize {
            // Enter backoff mode after threshold consecutive failures
            let failed = state.failed_count + 1;
            state.failed_count = failed;

            if failed >= failed_threshold {
                let backoff_minutes = backoff_start_minutes * (1u64 << ((failed - failed_threshold) as u64).min(7)); // Cap at 2^7 = 128x
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
    pub fn record_failure(&self, ip: &str, failed_threshold: u32, backoff_start_minutes: u64, backoff_max_hours: u64) -> Option<u64> {
        let mut inner = self.inner.lock().unwrap();
        let now = now_ms();
        let state = inner.state.entry(ip.to_string()).or_default();

        state.failed_count += 1;

        if state.failed_count >= failed_threshold {
            let backoff_minutes = backoff_start_minutes * (1u64 << ((state.failed_count - failed_threshold) as u64).min(7));
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
            inner: Arc::new(std::sync::Mutex::new(LocalSearchUiRateLimiterInner::default())),
        }
    }

    /// Check rate limit for search UI.
    /// Returns (allowed, wait_seconds).
    pub fn check(
        &self,
        ip: &str,
        limit: u64,
        window_ms: u64,
    ) -> (bool, Option<u64>) {
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

        let migration_coordinator = Arc::new(RwLock::new(
            MigrationCoordinator::new(migration_config.clone())
        ));

        // Create migration executor for actual HTTP document migration
        use miroir_core::rebalancer::HttpMigrationExecutor;
        let migration_executor = Arc::new(HttpMigrationExecutor::new(
            config.node_master_key.clone(),
            config.scatter.node_timeout_ms,
        ));

        let rebalancer = Arc::new(Rebalancer::new(
            rebalancer_config.clone(),
            topology_arc.clone(),
            migration_config.clone(),
        ).with_migration_executor(migration_executor));

        // Create rebalancer metrics
        let rebalancer_metrics = Arc::new(RwLock::new(RebalancerMetrics::default()));

        // Get or create task store for rebalancer worker
        let task_store: Option<Arc<dyn TaskStore>> = match config.task_store.backend.as_str() {
            "redis" => {
                redis_store.as_ref().map(|s| Arc::new(s.clone()) as Arc<dyn TaskStore>)
            }
            "sqlite" if !config.task_store.path.is_empty() => {
                Some(Arc::new(miroir_core::task_store::SqliteTaskStore::open(
                    std::path::Path::new(&config.task_store.path)
                ).expect("Failed to open SQLite task store")) as Arc<dyn TaskStore>)
            }
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
            Some(Arc::new(RebalancerWorker::new(
                worker_config,
                topology_arc.clone(),
                store.clone(),
                rebalancer.clone(),
                migration_coordinator.clone(),
                rebalancer_metrics.clone(),
                pod_id.clone(),
            )))
        } else {
            None
        };

        // Create settings broadcast coordinator (§13.5)
        let settings_broadcast = if let Some(ref store) = task_store {
            Arc::new(miroir_core::settings::SettingsBroadcast::with_task_store(store.clone()))
        } else {
            Arc::new(miroir_core::settings::SettingsBroadcast::new())
        };

        Self {
            config: Arc::new(config),
            topology: topology_arc,
            ready: Arc::new(RwLock::new(false)),
            metrics,
            version_state,
            task_registry: Arc::new(task_registry),
            redis_store,
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
            let previous = self.previous_docs_migrated.load(std::sync::atomic::Ordering::Relaxed);
            if current_total > previous {
                let delta = current_total - previous;
                self.metrics.inc_rebalance_documents_migrated(delta);
                self.previous_docs_migrated.store(current_total, std::sync::atomic::Ordering::Relaxed);
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
    let nodes: Vec<NodeInfo> = topo
        .nodes()
        .map(|n| NodeInfo {
            id: n.id.as_str().to_string(),
            address: n.address.clone(),
            status: format!("{:?}", n.status).to_lowercase(),
            shard_count: 0, // TODO: compute from routing table
            last_seen_ms: 0, // TODO: track last health check time
            error: None,     // TODO: populate from last health check error
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
        return Err(format!("invalid rate limit format: '{}', expected 'N/UNIT'", s));
    }
    let limit: u64 = parts[0].parse()
        .map_err(|_| format!("invalid limit number: '{}'", parts[0]))?;
    let window_seconds = match parts[1] {
        "second" | "s" => 1,
        "minute" | "m" => 60,
        "hour" | "h" => 3600,
        "day" | "d" => 86400,
        unit => return Err(format!("invalid time unit: '{}', expected second/minute/hour/day", unit)),
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
                }),
            ).into_response();
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
                                }),
                            ).into_response();
                        } else {
                            return (
                                StatusCode::TOO_MANY_REQUESTS,
                                Json(AdminLoginResponse {
                                    success: false,
                                    message: Some("Too many login attempts. Please try again later.".into()),
                                }),
                            ).into_response();
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
                }),
            ).into_response();
        }
    }

    // Verify admin_key (constant-time comparison to prevent timing side-channels)
    use subtle::ConstantTimeEq as _;
    if body.admin_key.as_bytes().ct_eq(state.config.admin.api_key.as_bytes()).into() {
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

        // Generate session ID and seal it
        let session_id = generate_session_id();
        let sealed = match seal_session(&session_id, &state.seal_key) {
            Ok(sealed) => sealed,
            Err(e) => {
                error!(error = %e, "failed to seal admin session");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(AdminLoginResponse {
                        success: false,
                        message: Some("Failed to create session".into()),
                    }),
                ).into_response();
            }
        };

        info!(
            source_ip_hash = hash_for_log(&source_ip),
            session_prefix = &session_id[..8],
            "admin login successful"
        );

        // Set cookie and return success
        (
            StatusCode::OK,
            [
                ("Set-Cookie", format!("{}={}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={}",
                    COOKIE_NAME, sealed, state.config.admin_ui.session_ttl_s)),
            ],
            Json(AdminLoginResponse {
                success: true,
                message: None,
            }),
        ).into_response()
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
            }),
        ).into_response()
    }
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
pub async fn add_node<S>(
    State(state): State<S>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    let id = body.get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing 'id' field".into()))?
        .to_string();

    let address = body.get("address")
        .and_then(|v| v.as_str())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing 'address' field".into()))?
        .to_string();

    let replica_group = body.get("replica_group")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing 'replica_group' field".into()))?
        as u32;

    // Get index_uid from body or use default
    let index_uid = body.get("index_uid")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();

    // Add node to topology
    {
        let mut topo = app_state.topology.write().await;
        let node = miroir_core::topology::Node::new(
            miroir_core::topology::NodeId::new(id.clone()),
            address.clone(),
            replica_group,
        );
        topo.add_node(node);
    }

    // Send event to rebalancer worker if available
    if let Some(ref worker) = app_state.rebalancer_worker {
        use miroir_core::rebalancer_worker::TopologyChangeEvent;
        let event = TopologyChangeEvent::NodeAdded {
            node_id: id.clone(),
            replica_group,
            index_uid: index_uid.clone(),
        };
        let _ = worker.event_sender().try_send(event);
        info!(node_id = %id, replica_group, "Sent NodeAdded event to rebalancer worker");
    }

    info!(node_id = %id, replica_group, "Node addition initiated");
    Ok(Json(serde_json::json!({
        "node_id": id,
        "replica_group": replica_group,
        "index_uid": index_uid,
        "message": "Node addition initiated - rebalancer worker will handle migration",
    })))
}

/// DELETE /_miroir/nodes/{id} — Remove a node from the cluster.
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

    let force = body.get("force")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Check node status
    let (node_status, replica_group) = {
        let topo = app_state.topology.read().await;
        let node = topo.node(&miroir_core::topology::NodeId::new(node_id.clone()))
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Node {} not found", node_id)))?;
        (node.status, node.replica_group)
    };

    if !force && !matches!(node_status, miroir_core::topology::NodeStatus::Draining) {
        return Err((StatusCode::BAD_REQUEST, format!(
            "Node {} is not in draining state (current: {:?}), use force=true to bypass",
            node_id, node_status
        )));
    }

    // Remove node from topology
    {
        let mut topo = app_state.topology.write().await;
        topo.remove_node(&miroir_core::topology::NodeId::new(node_id.clone()));
    }

    info!(node_id = %node_id, "Node removal completed");
    Ok(Json(serde_json::json!({
        "node_id": node_id,
        "message": "Node removed from cluster",
    })))
}

/// POST /_miroir/nodes/{id}/drain — Drain a node (prepare for removal).
pub async fn drain_node<S>(
    State(state): State<S>,
    Path(node_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    // Check if node exists and get its replica group
    let (node_exists, replica_group) = {
        let topo = app_state.topology.read().await;
        let node = topo.node(&miroir_core::topology::NodeId::new(node_id.clone()));
        match node {
            Some(n) => {
                if n.status == miroir_core::topology::NodeStatus::Draining {
                    return Err((StatusCode::CONFLICT, format!("Node {} is already draining", node_id)));
                }
                (true, n.replica_group)
            }
            None => return Err((StatusCode::NOT_FOUND, format!("Node {} not found", node_id))),
        }
    };

    // Mark node as draining
    {
        let mut topo = app_state.topology.write().await;
        let node_id_obj = miroir_core::topology::NodeId::new(node_id.clone());
        if let Some(node) = topo.node_mut(&node_id_obj) {
            node.status = miroir_core::topology::NodeStatus::Draining;
        }
    }

    // Send event to rebalancer worker if available
    if let Some(ref worker) = app_state.rebalancer_worker {
        use miroir_core::rebalancer_worker::TopologyChangeEvent;
        let event = TopologyChangeEvent::NodeDraining {
            node_id: node_id.clone(),
            replica_group,
            index_uid: "default".to_string(),
        };
        let _ = worker.event_sender().try_send(event);
        info!(node_id = %node_id, replica_group, "Sent NodeDraining event to rebalancer worker");
    }

    info!(node_id = %node_id, replica_group, "Node drain initiated");
    Ok(Json(serde_json::json!({
        "node_id": node_id,
        "replica_group": replica_group,
        "message": "Node drain initiated - rebalancer worker will handle migration",
    })))
}

/// GET /_miroir/rebalance/status — Get current rebalance status.
pub async fn get_rebalance_status<S>(
    State(state): State<S>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    // Get rebalancer status if available
    let rebalancer_status = if let Some(ref rebalancer) = app_state.rebalancer {
        let status = rebalancer.status().await;
        let metrics = rebalancer.metrics.read().await;
        Some(serde_json::json!({
            "in_progress": status.in_progress,
            "operations": status.operations,
            "migrations": status.migrations,
            "metrics": {
                "documents_migrated_total": metrics.documents_migrated_total,
                "active_migrations": metrics.active_migrations,
                "current_duration_secs": metrics.current_duration_secs(),
            },
        }))
    } else {
        None
    };

    // Get worker status if available
    let worker_status = if let Some(ref worker) = app_state.rebalancer_worker {
        Some(worker.get_status().await)
    } else {
        None
    };

    Ok(Json(serde_json::json!({
        "rebalancer": rebalancer_status,
        "worker": worker_status,
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
pub async fn add_replica_group<S>(
    State(state): State<S>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let app_state = AppState::from_ref(&state);

    let rebalancer = app_state.rebalancer.as_ref()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "Rebalancer not initialized".into()))?;

    let group_id = body.get("group_id")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing 'group_id' field".into()))?
        as u32;

    let nodes_array = body.get("nodes")
        .and_then(|v| v.as_array())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing 'nodes' field".into()))?;

    let mut nodes = Vec::new();
    for node_obj in nodes_array {
        let id = node_obj.get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing node 'id'".into()))?
            .to_string();

        let address = node_obj.get("address")
            .and_then(|v| v.as_str())
            .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing node 'address'".into()))?
            .to_string();

        use miroir_core::rebalancer::GroupNodeSpec;
        nodes.push(GroupNodeSpec { id, address });
    }

    use miroir_core::rebalancer::AddReplicaGroupRequest;
    let request = AddReplicaGroupRequest { group_id, nodes };

    match rebalancer.add_replica_group(request).await {
        Ok(result) => {
            info!(group_id, "Replica group addition completed");
            Ok(Json(serde_json::json!({
                "operation_id": result.id,
                "message": result.message,
            })))
        }
        Err(e) => {
            error!(error = %e, group_id, "Replica group addition failed");
            Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        }
    }
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

    let rebalancer = app_state.rebalancer.as_ref()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "Rebalancer not initialized".into()))?;

    let force = body.get("force")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

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
        shards.insert("0".to_string(), vec!["node-0".to_string(), "node-1".to_string()]);
        shards.insert("1".to_string(), vec!["node-1".to_string(), "node-0".to_string()]);

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
