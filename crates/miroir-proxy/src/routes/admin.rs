//! Admin router for /_miroir/* endpoints.
//!
//! This router requires `admin_endpoints::AppState` to be provided via `.with_state()`.

use super::{admin_endpoints, aliases, canary, cdc, explain, session};
use axum::{
    extract::FromRef,
    routing::{delete, get, post, put},
    Router,
};

/// Create the admin router with all /_miroir/* endpoints.
///
/// Returns a stateless router that must be given a state via `.with_state()`
/// before use. The state type must implement `FromRef` to extract
/// `admin_endpoints::AppState`.
pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    admin_endpoints::AppState: FromRef<S>,
    aliases::AliasState: FromRef<S>,
    explain::ExplainState: FromRef<S>,
    canary::CanaryState: FromRef<S>,
    std::sync::Arc<miroir_core::cdc::CdcManager>: FromRef<S>,
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
        // Alias management (plan §13.7)
        .route("/aliases", get(aliases::list_aliases::<S>))
        .route("/aliases/{name}", post(aliases::create_alias::<S>))
        .route("/aliases/{name}", get(aliases::get_alias::<S>))
        .route("/aliases/{name}", put(aliases::update_alias::<S>))
        .route("/aliases/{name}", delete(aliases::delete_alias::<S>))
        // Canary management (plan §13.18)
        .route("/canaries", post(canary::create_canary::<S>))
        .route("/canaries", get(canary::get_canary_status::<S>))
        .route("/canaries/{id}", get(canary::get_canary::<S>))
        .route("/canaries/{id}", put(canary::update_canary::<S>))
        .route("/canaries/{id}", delete(canary::delete_canary::<S>))
        .route("/canaries/capture", post(canary::start_capture::<S>))
        .route("/canaries/captured", get(canary::get_captured::<S>))
        .route(
            "/canaries/from-capture/{index}",
            post(canary::create_from_capture::<S>),
        )
        // Explain endpoint (plan §13.20)
        .route(
            "/indexes/{index}/explain",
            post(explain::explain_search::<S>),
        )
        // Node management (plan §2 node addition flow)
        .route("/nodes", post(admin_endpoints::add_node::<S>))
        .route("/nodes/{id}", delete(admin_endpoints::remove_node::<S>))
        .route("/nodes/{id}/drain", post(admin_endpoints::drain_node::<S>))
        .route("/nodes/{id}/fail", post(admin_endpoints::fail_node::<S>))
        .route(
            "/nodes/{id}/recover",
            post(admin_endpoints::recover_node::<S>),
        )
        // Rebalancer status
        .route(
            "/rebalance/status",
            get(admin_endpoints::get_rebalance_status::<S>),
        )
        // Replica group management
        .route(
            "/replica_groups",
            post(admin_endpoints::add_replica_group::<S>),
        )
        .route(
            "/replica_groups/{id}",
            delete(admin_endpoints::remove_replica_group::<S>),
        )
        // Shadow traffic endpoints (plan §13.16)
        .route("/shadow/diff", get(admin_endpoints::get_shadow_diff::<S>))
        .route("/shadow/stats", get(admin_endpoints::get_shadow_stats::<S>))
        // CDC changes endpoint (plan §13.13)
        .route("/changes", get(cdc::get_changes::<S>))
}
