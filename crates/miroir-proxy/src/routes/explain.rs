//! Query explain API endpoint (plan §13.20).

use axum::{
    extract::{FromRef, Path, State},
    http::StatusCode,
    routing::post,
    Json, Router,
};
use miroir_core::{
    config::MiroirConfig,
    explainer::{Explainer, SearchQueryExplanation},
    topology::Topology,
};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Search query for explanation (re-export from core).
pub type SearchQuery = SearchQueryExplanation;

/// Explain state.
#[derive(Clone)]
pub struct ExplainState {
    pub config: Arc<MiroirConfig>,
    pub topology: Arc<RwLock<Topology>>,
}

/// POST /indexes/{index}/explain — explain a search query without executing it.
///
/// Request body matches /search but returns the execution plan instead of results.
/// Plan §13.20: "Why is this query slow?" debugging.
pub async fn explain_search<S>(
    State(state): State<ExplainState>,
    Path(index): Path<String>,
    Json(query): Json<SearchQuery>,
) -> Result<Json<miroir_core::explainer::QueryExplanation>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    ExplainState: FromRef<S>,
{
    let explainer = Explainer::new(state.config.as_ref().clone());
    let topology = state.topology.read().await;

    // TODO: Get actual settings_version from task store
    let settings_version = 1;

    let explanation = explainer.explain(&index, &query, &topology, settings_version, None);

    Ok(Json(explanation))
}

/// Router for explain endpoints.
pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    ExplainState: FromRef<S>,
{
    Router::new()
}
