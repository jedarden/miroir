//! Admin API endpoints for topology, readiness, shards, and metrics.

use axum::{
    extract::{FromRef, Path, State},
    http::{HeaderMap, StatusCode},
    Json,
    response::{IntoResponse, Response},
};
use miroir_core::{
    config::MiroirConfig,
    router,
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
    pub pod_id: String,
    pub seal_key: SealKey,
    pub local_rate_limiter: LocalAdminRateLimiter,
    pub local_search_ui_rate_limiter: LocalSearchUiRateLimiter,
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

        Self {
            config: Arc::new(config),
            topology: Arc::new(RwLock::new(topology)),
            ready: Arc::new(RwLock::new(false)),
            metrics,
            version_state,
            task_registry: Arc::new(task_registry),
            redis_store,
            pod_id,
            seal_key,
            local_rate_limiter: LocalAdminRateLimiter::new(),
            local_search_ui_rate_limiter: LocalSearchUiRateLimiter::new(),
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
        rebalance_in_progress: false, // TODO: track rebalance state
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
