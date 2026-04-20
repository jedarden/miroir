//! Admin router for /_miroir/* endpoints.
//!
//! This router requires `admin_endpoints::AppState` to be provided via `.with_state()`.

use axum::{
    extract::FromRef,
    routing::{get, post},
    Router,
};
use super::{admin_endpoints, session};

/// Create the admin router with all /_miroir/* endpoints.
///
/// Returns a stateless router that must be given a state via `.with_state()`
/// before use. The state type must implement `FromRef` to extract
/// `admin_endpoints::AppState`.
pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    admin_endpoints::AppState: FromRef<S>,
{
    Router::new()
        // Admin session endpoints (plan §9, §13.19)
        .route("/admin/login", post(session::admin_login::<S>))
        .route("/admin/session", get(session::admin_session::<S>))
        .route("/admin/logout", post(session::admin_logout::<S>))
        // Search UI session endpoint (plan §9, §13.21)
        .route(
            "/ui/search/{index}/session",
            get(session::search_ui_session::<S>),
        )
        // Admin API endpoints
        .route("/topology", get(admin_endpoints::get_topology::<S>))
        .route("/shards", get(admin_endpoints::get_shards::<S>))
        .route("/ready", get(admin_endpoints::get_ready::<S>))
        .route("/metrics", get(admin_endpoints::get_metrics::<S>))
        .route(
            "/ui/search/{index}/rotate-scoped-key",
            post(admin_endpoints::rotate_scoped_key_handler),
        )
}
