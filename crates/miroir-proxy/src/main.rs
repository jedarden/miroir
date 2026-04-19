use axum::{routing::get, Router};
use std::net::SocketAddr;
use tokio::signal;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod auth;
mod middleware;
mod routes;

use routes::{admin, documents, health, indexes, search, settings, tasks};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    info!("miroir-proxy starting");

    let app = Router::new()
        .route("/health", get(health::get_health))
        .nest("/indexes", indexes::router())
        .nest("/documents", documents::router())
        .nest("/search", search::router())
        .nest("/settings", settings::router())
        .nest("/tasks", tasks::router())
        .nest("/admin", admin::router())
        .layer(axum::extract::DefaultBodyLimit::max(10 * 1024 * 1024));

    let main_addr = SocketAddr::from(([0, 0, 0, 0], 7700));
    let metrics_addr = SocketAddr::from(([0, 0, 0, 0], 9090));

    info!("listening on {}", main_addr);

    let main_server = axum::serve(tokio::net::TcpListener::bind(main_addr).await?, app);

    let metrics_server = axum::serve(
        tokio::net::TcpListener::bind(metrics_addr).await?,
        Router::new().route("/metrics", get(|| async { "prometheus metrics\n" })),
    );

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
