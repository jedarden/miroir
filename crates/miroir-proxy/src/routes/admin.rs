//! Admin router for /_miroir/* endpoints.
//!
//! This router requires `admin_endpoints::AppState` to be provided via `.with_state()`.

use axum::{
    extract::FromRef,
    routing::get,
    Router,
};
use super::admin_endpoints;

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
        .route("/topology", get(admin_endpoints::get_topology::<S>))
        .route("/shards", get(admin_endpoints::get_shards::<S>))
        .route("/ready", get(admin_endpoints::get_ready::<S>))
        .route("/metrics", get(admin_endpoints::get_metrics::<S>))
}
