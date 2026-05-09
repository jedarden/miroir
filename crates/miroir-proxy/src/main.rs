use axum::{routing::get, Router};
use miroir_core::config::MiroirConfig;
use std::net::SocketAddr;
use tokio::signal;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod auth;
mod client;
mod error_response;
mod middleware;
mod retry_cache;
mod routes;
mod scatter;
mod state;
mod task_manager;

use routes::{admin, documents, health, indexes, search, settings, tasks};
use state::ProxyState;
use auth::auth_middleware;
use middleware::{prometheus_middleware, tracing_middleware};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    info!("miroir-proxy starting");

    // Load configuration from file + environment
    let config = MiroirConfig::load().map_err(|e| anyhow::anyhow!("config load failed: {}", e))?;

    info!(
        "loaded config: {} shards, RF={}, RG={}, {} nodes",
        config.shards,
        config.replication_factor,
        config.replica_groups,
        config.nodes.len()
    );

    // Build shared application state
    let state = ProxyState::new(config).map_err(|e| anyhow::anyhow!("state init failed: {}", e))?;

    // Build router with all routes
    let app = Router::new()
        .route("/health", get(health::get_health))
        .route("/version", get(health::get_version))
        .nest("/indexes", indexes::router())
        .nest("/documents", documents::router())
        .nest("/search", search::router())
        .nest("/settings", settings::router())
        .nest("/tasks", tasks::router())
        .nest("/admin", admin::router())
        .nest("/_miroir", admin::miroir_router())
        .layer(axum::extract::DefaultBodyLimit::max(
            state.config.server.max_body_bytes as usize,
        ))
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth_middleware))
        .layer(axum::middleware::from_fn_with_state(state.clone(), prometheus_middleware))
        .layer(axum::middleware::from_fn(tracing_middleware))
        .with_state(state);

    let main_addr = SocketAddr::from((
        state.config.server.bind.parse::<std::net::IpAddr>()?,
        state.config.server.port,
    ));
    let metrics_addr = SocketAddr::from(([0, 0, 0, 0], 9090));

    info!("listening on {}", main_addr);
    info!("metrics on {}", metrics_addr);

    // Metrics server (prometheus format) - with state
    let metrics_router = Router::new()
        .route("/metrics", get(admin::get_metrics))
        .with_state(state.clone());
    let metrics_listener = tokio::net::TcpListener::bind(metrics_addr).await?;
    let metrics_server = axum::serve(metrics_listener, metrics_router);

    // Main server
    let main_listener = tokio::net::TcpListener::bind(main_addr).await?;
    let main_server = axum::serve(main_listener, app);

    tokio::select! {
        _ = main_server => {}
        _ = metrics_server => {}
        _ = shutdown_signal() => {}
    }

    Ok(())
}

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
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received");
}
