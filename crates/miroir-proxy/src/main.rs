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
use tracing_subscriber::EnvFilter;

mod auth;
mod client;
mod middleware;
mod routes;

use auth::AuthState;
use middleware::{Metrics, metrics_router};
use routes::{
    admin, admin_endpoints, documents, health, indexes, search, settings, tasks, version,
};

/// Unified application state containing all shared state.
#[derive(Clone)]
struct UnifiedState {
    auth: AuthState,
    metrics: Metrics,
    admin: admin_endpoints::AppState,
}

impl UnifiedState {
    fn new(config: MiroirConfig) -> Self {
        let metrics = Metrics::new();

        let master_key = std::env::var("MIROIR_MASTER_KEY")
            .unwrap_or_else(|_| config.master_key.clone());

        let admin_key = std::env::var("MIROIR_ADMIN_API_KEY")
            .unwrap_or_else(|_| config.admin.api_key.clone());

        let auth = AuthState {
            master_key,
            admin_key: admin_key.clone(),
        };

        let admin = admin_endpoints::AppState::new(config.clone(), metrics.clone());

        Self { auth, metrics, admin }
    }
}

// Implement FromRef so that admin_endpoints::AppState can be extracted from UnifiedState
impl FromRef<UnifiedState> for admin_endpoints::AppState {
    fn from_ref(state: &UnifiedState) -> Self {
        state.admin.clone()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load configuration (file → env → CLI overlay)
    let config = MiroirConfig::load()
        .map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?;

    // Initialize structured JSON logging (plan §10 format)
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .with_current_span(false)
        .with_span_list(false)
        .init();

    info!(
        shards = config.shards,
        replication_factor = config.replication_factor,
        replica_groups = config.replica_groups,
        "miroir-proxy starting"
    );

    // Build unified state
    let state = UnifiedState::new(config.clone());

    // Start health checker background task
    let health_checker_state = state.admin.clone();
    tokio::spawn(async move {
        run_health_checker(health_checker_state).await;
    });

    // Build the main app router with UnifiedState
    let app = Router::new()
        .route("/health", get(health::get_health))
        .route("/version", get(version::get_version::<UnifiedState>))
        .nest("/_miroir", admin::router::<UnifiedState>())
        .nest("/indexes", indexes::router::<UnifiedState>())
        .nest("/documents", documents::router::<UnifiedState>())
        .nest("/search", search::router::<UnifiedState>())
        .nest("/settings", settings::router::<UnifiedState>())
        .nest("/tasks", tasks::router::<UnifiedState>())
        .layer(axum::extract::DefaultBodyLimit::max(
            config.server.max_body_bytes as usize,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.auth.clone(),
            auth::auth_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.metrics.clone(),
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
}
