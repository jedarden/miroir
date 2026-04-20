use axum::{
    extract::FromRef,
    routing::get,
    Router,
};
use miroir_core::{
    config::MiroirConfig,
    topology::NodeStatus,
};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, registry, util::SubscriberInitExt};

mod admin_session;
mod auth;
mod client;
mod middleware;
mod otel;
mod routes;
mod scoped_key_rotation;

use admin_session::SealKey;
use auth::AuthState;
use middleware::{Metrics, metrics_router, TelemetryState};
use routes::{
    admin, admin_endpoints, health, indexes, keys, search, settings, tasks, version,
};
use scoped_key_rotation::ScopedKeyRotationState;

/// Unified application state containing all shared state.
#[derive(Clone)]
struct UnifiedState {
    auth: AuthState,
    metrics: Metrics,
    admin: admin_endpoints::AppState,
    pod_id: String,
    redis_store: Option<miroir_core::task_store::RedisTaskStore>,
}

impl UnifiedState {
    fn new(config: MiroirConfig) -> Self {
        let metrics = Metrics::new(&config);

        let master_key = std::env::var("MIROIR_MASTER_KEY")
            .unwrap_or_else(|_| config.master_key.clone());

        let admin_key = std::env::var("MIROIR_ADMIN_API_KEY")
            .unwrap_or_else(|_| config.admin.api_key.clone());

        let jwt_primary = if config.search_ui.enabled {
            std::env::var(&config.search_ui.auth.jwt_secret_env).ok()
        } else {
            None
        };

        let jwt_previous = std::env::var(&config.search_ui.auth.jwt_secret_previous_env)
            .ok()
            .filter(|v| !v.is_empty());

        let seal_key = SealKey::from_env_or_generate();

        // Set the key-generated gauge before constructing AuthState
        // so the metric is accurate from the first scrape.
        metrics.admin_session_key_generated().set(if seal_key.is_generated() { 1.0 } else { 0.0 });

        let pod_id = std::env::var("POD_NAME").unwrap_or_else(|_| "unknown".to_string());

        // Create Redis task store if backend is redis (must happen before AppState
        // so redis_store and pod_id are available to admin endpoints).
        let redis_store = if config.task_store.backend == "redis" && !config.task_store.url.is_empty() {
            let url = config.task_store.url.clone();
            Some(
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(
                        miroir_core::task_store::RedisTaskStore::open(&url)
                    )
                })
                .expect("Failed to connect to Redis for scoped key rotation"),
            )
        } else {
            None
        };

        let auth = AuthState {
            master_key,
            admin_key: admin_key.clone(),
            jwt_primary,
            jwt_previous,
            seal_key: seal_key.clone(),
            revoked_sessions: std::sync::Arc::new(dashmap::DashMap::new()),
            admin_session_revoked_total: metrics.admin_session_revoked_total(),
        };

        let admin = admin_endpoints::AppState::with_redis(
            config.clone(),
            metrics.clone(),
            redis_store.clone(),
            pod_id.clone(),
            seal_key.clone(),
        );

        Self { auth, metrics, admin, pod_id, redis_store }
    }
}

// Implement FromRef so that admin_endpoints::AppState can be extracted from UnifiedState
impl FromRef<UnifiedState> for admin_endpoints::AppState {
    fn from_ref(state: &UnifiedState) -> Self {
        Self {
            config: state.admin.config.clone(),
            topology: state.admin.topology.clone(),
            ready: state.admin.ready.clone(),
            metrics: state.admin.metrics.clone(),
            version_state: state.admin.version_state.clone(),
            task_registry: state.admin.task_registry.clone(),
            redis_store: state.redis_store.clone(),
            pod_id: state.pod_id.clone(),
            seal_key: state.auth.seal_key.clone(),
            local_rate_limiter: admin_endpoints::LocalAdminRateLimiter::new(),
        }
    }
}

// Implement FromRef so that TelemetryState can be extracted from UnifiedState
impl FromRef<UnifiedState> for TelemetryState {
    fn from_ref(state: &UnifiedState) -> Self {
        TelemetryState {
            metrics: state.metrics.clone(),
            pod_id: state.pod_id.clone(),
        }
    }
}

// Implement FromRef so that CsrfState can be extracted from UnifiedState
impl FromRef<UnifiedState> for auth::CsrfState {
    fn from_ref(state: &UnifiedState) -> Self {
        auth::CsrfState {
            auth: state.auth.clone(),
            redis_store: state.redis_store.clone(),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load configuration (file → env → CLI overlay)
    let config = MiroirConfig::load()
        .map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?;

    // Initialize structured JSON logging (plan §10 format)
    // Fields on every line: timestamp, level, target, message, pod_id
    // Per-request fields (request_id) are added by telemetry middleware span.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let pod_id = std::env::var("POD_NAME").unwrap_or_else(|_| "unknown".to_string());

    // Build subscriber - conditionally add OTel layer
    // Note: We rebuild the layers in each branch because the types differ
    // OTel layer must be applied to the bare registry first
    if let Some(otel_layer) = otel::init_otel_layer(&config) {
        let json_layer = tracing_subscriber::fmt::layer()
            .json()
            .with_target(true)
            .with_current_span(true)
            .with_span_list(false);
        // Apply OTel layer to registry first, then add filter and json layer
        registry()
            .with(otel_layer)
            .with(filter)
            .with(json_layer)
            .init();
    } else {
        let json_layer = tracing_subscriber::fmt::layer()
            .json()
            .with_target(true)
            .with_current_span(true)
            .with_span_list(false);
        registry()
            .with(filter)
            .with(json_layer)
            .init();
    }

    // Set pod_id as a global default field so it appears on every log line.
    // This is done via a separate info span that is entered once and never
    // left — its fields propagate to all child spans and events.
    let _pod_span = tracing::info_span!("runtime", pod_id = %pod_id).entered();

    info!(
        shards = config.shards,
        replication_factor = config.replication_factor,
        replica_groups = config.replica_groups,
        "miroir-proxy starting"
    );

    // Validate critical secrets at startup (plan §9: "orchestrator refuses to
    // start the search UI without it").
    if config.search_ui.enabled {
        let jwt_env = &config.search_ui.auth.jwt_secret_env;
        match std::env::var(jwt_env) {
            Ok(v) if !v.is_empty() => {}
            _ => {
                anyhow::bail!(
                    "search_ui is enabled but {} is not set — refusing to start. \
                     Either set the env var or disable search_ui (search_ui.enabled: false)",
                    jwt_env
                );
            }
        }
    }

    // Build unified state
    let state = UnifiedState::new(config.clone());

    // Start health checker background task
    let health_checker_state = state.admin.clone();
    tokio::spawn(async move {
        run_health_checker(health_checker_state).await;
    });

    // Start scoped key rotation background task (requires Redis)
    if let Some(ref redis) = state.redis_store {
        let rotation_state = ScopedKeyRotationState {
            config: state.admin.config.clone(),
            redis: redis.clone(),
            pod_id: state.pod_id.clone(),
        };
        tokio::spawn(async move {
            scoped_key_rotation::run_scoped_key_rotator(rotation_state).await;
        });
    }

    // Build the main app router with UnifiedState
    let app = Router::new()
        .route("/health", get(health::get_health))
        .route("/version", get(version::get_version::<UnifiedState>))
        .route("/stats", get(indexes::global_stats_handler))
        .nest("/_miroir", admin::router::<UnifiedState>())
        .nest("/indexes", indexes::router::<UnifiedState>())
        .nest("/keys", keys::router::<UnifiedState>())
        .nest("/search", search::router::<UnifiedState>())
        .nest("/settings", settings::router::<UnifiedState>())
        .nest("/tasks", tasks::router::<UnifiedState>())
        .layer(axum::extract::DefaultBodyLimit::max(
            config.server.max_body_bytes as usize,
        ))
        .layer(axum::Extension(state.admin.config.clone()))
        .layer(axum::Extension(std::sync::Arc::new(state.admin.clone())))
        .layer(axum::middleware::from_fn_with_state(
            state.auth.clone(),
            auth::auth_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            auth::CsrfState {
                auth: state.auth.clone(),
                redis_store: state.redis_store.clone(),
            },
            auth::csrf_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            TelemetryState {
                metrics: state.metrics.clone(),
                pod_id: state.pod_id.clone(),
            },
            middleware::telemetry_middleware,
        ))
        .with_state(state.clone());

    let main_addr: SocketAddr = format!("{}:{}", config.server.bind, config.server.port)
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid bind address: {}", e))?;
    let metrics_addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], 9090));

    info!(
        main_addr = %main_addr,
        metrics_addr = %metrics_addr,
        "listening"
    );

    // Create listeners
    let main_listener = tokio::net::TcpListener::bind(main_addr).await?;
    let metrics_listener = tokio::net::TcpListener::bind(metrics_addr).await?;

    // Spawn main server with graceful shutdown
    let main_server = axum::serve(main_listener, app)
        .with_graceful_shutdown(shutdown_signal());

    // Spawn metrics server with graceful shutdown
    let metrics_app = metrics_router().with_state(state.metrics.clone());
    let metrics_server = axum::serve(metrics_listener, metrics_app)
        .with_graceful_shutdown(shutdown_signal());

    // Run both servers concurrently
    let (main_result, metrics_result) = tokio::join!(
        main_server,
        metrics_server
    );

    // Check for errors
    if let Err(e) = main_result {
        error!("main server error: {}", e);
    }
    if let Err(e) = metrics_result {
        error!("metrics server error: {}", e);
    }

    Ok(())
}

/// Background health checker - promotes nodes to Active when reachable.
async fn run_health_checker(state: admin_endpoints::AppState) {
    let mut interval = tokio::time::interval(Duration::from_millis(
        state.config.health.interval_ms,
    ));

    loop {
        interval.tick().await;

        let mut topo = state.topology.write().await;
        let mut all_healthy = true;

        // Collect node IDs to iterate
        let node_ids: Vec<_> = topo.nodes().map(|n| n.id.clone()).collect();

        for node_id in node_ids {
            // Get current node status
            let current_status = topo.node(&node_id).map(|n| n.status);

            // Skip nodes that are already Active/Healthy
            if let Some(NodeStatus::Active) | Some(NodeStatus::Healthy) = current_status {
                continue;
            }

            // Get node address
            let node_address = match topo.node(&node_id) {
                Some(n) => n.address.clone(),
                None => {
                    all_healthy = false;
                    continue;
                }
            };

            // Try to reach the node
            let client = match reqwest::Client::builder()
                .timeout(Duration::from_millis(state.config.health.timeout_ms))
                .build()
            {
                Ok(c) => c,
                Err(_) => {
                    all_healthy = false;
                    continue;
                }
            };

            let url = format!("{}/health", node_address.trim_end_matches('/'));
            let result = client.get(&url).send().await;

            if result.is_ok() && result.unwrap().status().is_success() {
                // Node is reachable - promote to Active
                if let Some(node) = topo.node_mut(&node_id) {
                    let _ = node.transition_to(NodeStatus::Active);
                    info!(node_id = %node_id, "node promoted to Active");
                }
            } else {
                all_healthy = false;
            }
        }

        // Mark ready once all configured nodes are reachable
        if all_healthy && !state.config.nodes.is_empty() {
            state.mark_ready().await;
        }
    }
}

/// Graceful shutdown signal handler.
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>;

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received, draining in-flight requests...");

    // Shutdown OpenTelemetry to flush any pending traces
    otel::shutdown_otel();
}
