use axum::{routing::get, Router};
use std::net::SocketAddr;
use tokio::signal;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod auth;
mod client;
mod middleware;
mod routes;

use auth::AuthState;
use middleware::{Metrics, metrics_router};
use routes::{admin, documents, health, indexes, search, settings, tasks};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize structured JSON logging
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .with_current_span(false)
        .with_span_list(false)
        .init();

    info!("miroir-proxy starting");

    let metrics = Metrics::new();

    let auth_state = AuthState {
        master_key: std::env::var("MIROIR_MASTER_KEY").unwrap_or_default(),
        admin_key: std::env::var("MIROIR_ADMIN_API_KEY").unwrap_or_default(),
    };

    // Build the main app with auth + telemetry middleware
    // Auth middleware runs first (outer), telemetry wraps it.
    // Both use from_fn_with_state so router state stays ().
    let app = Router::new()
        .route("/health", get(health::get_health))
        .nest("/indexes", indexes::router())
        .nest("/documents", documents::router())
        .nest("/search", search::router())
        .nest("/settings", settings::router())
        .nest("/tasks", tasks::router())
        .nest("/admin", admin::router())
        .nest("/_miroir", admin::router())
        .layer(axum::extract::DefaultBodyLimit::max(10 * 1024 * 1024))
        .layer(axum::middleware::from_fn_with_state(
            auth_state,
            auth::auth_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            metrics.clone(),
            middleware::telemetry_middleware,
        ))
        .with_state(());

    let main_addr = SocketAddr::from(([0, 0, 0, 0], 7700));
    let metrics_addr = SocketAddr::from(([0, 0, 0, 0], 9090));

    info!("listening on {}", main_addr);
    info!("metrics on {}", metrics_addr);

    let main_server = axum::serve(tokio::net::TcpListener::bind(main_addr).await?, app);

    let metrics_app = metrics_router().with_state(metrics);
    let metrics_server = axum::serve(tokio::net::TcpListener::bind(metrics_addr).await?, metrics_app);

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
