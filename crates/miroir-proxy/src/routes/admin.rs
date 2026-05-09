//! Admin endpoints: /admin/* and /_miroir/*

use axum::{
    extract::State,
    Json,
    routing::get,
    Router,
};
use miroir_core::scatter::Scatter;
use crate::error_response::ErrorResponse;
use crate::middleware::export_metrics;
use crate::state::ProxyState;
use serde::Serialize;

/// Router for /admin/* endpoints.
pub fn router() -> Router<ProxyState> {
    Router::new()
        .route("/stats", get(get_stats))
}

/// Router for /_miroir/* internal endpoints.
pub fn miroir_router() -> Router<ProxyState> {
    Router::new()
        .route("/ready", get(crate::routes::health::get_ready))
        .route("/topology", get(get_topology))
        .route("/shards", get(get_shards))
        .route("/metrics", get(get_metrics))
}

#[derive(Serialize)]
pub struct StatsResponse {
    pub indexes: u64,
    pub documents: u64,
    pub fields_distribution: serde_json::Value,
}

/// GET /admin/stats - Aggregate stats across all nodes.
pub async fn get_stats(
    State(state): State<ProxyState>,
) -> Result<Json<StatsResponse>, ErrorResponse> {
    let topology = state.topology().await;

    // Broadcast stats request to all nodes
    let all_nodes: Vec<_> = topology.nodes().map(|n| n.id.clone()).collect();

    if all_nodes.is_empty() {
        return Ok(Json(StatsResponse {
            indexes: 0,
            documents: 0,
            fields_distribution: serde_json::json!({}),
        }));
    }

    // Use scatter to get stats from all nodes
    let scatter_req = miroir_core::scatter::ScatterRequest {
        body: Vec::new(),
        headers: Vec::new(),
        method: "GET".to_string(),
        path: "/stats".to_string(),
    };

    let scatter = crate::scatter::HttpScatter::new(
        (*state.client).clone(),
        state.config.scatter.node_timeout_ms,
    );

    let result = scatter
        .scatter(
            &topology,
            all_nodes,
            scatter_req,
            miroir_core::config::UnavailableShardPolicy::Partial,
        )
        .await
        .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

    // Aggregate stats from all successful responses
    let mut total_indexes = 0u64;
    let mut total_documents = 0u64;
    let merged_fields: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();

    for response in result.responses {
        if let Ok(stats) = serde_json::from_slice::<serde_json::Value>(&response.body) {
            if let Some(indexes) = stats.get("databaseSize").and_then(|v| v.as_u64()) {
                total_indexes += indexes;
            }
            if let Some(docs) = stats.get("indexes").and_then(|i| i.as_object()) {
                for (_index_name, index_stats) in docs {
                    if let Some(number_of_documents) = index_stats.get("numberOfDocuments").and_then(|v| v.as_u64()) {
                        total_documents += number_of_documents;
                    }
                }
            }
        }
    }

    Ok(Json(StatsResponse {
        indexes: total_indexes,
        documents: total_documents,
        fields_distribution: serde_json::Value::Object(merged_fields),
    }))
}

/// GET /_miroir/topology - Return cluster topology information.
pub async fn get_topology(State(state): State<ProxyState>) -> Json<serde_json::Value> {
    let health = state.get_node_health().await;

    Json(serde_json::json!({
        "replication_factor": state.config.replication_factor,
        "replica_groups": state.config.replica_groups,
        "shards": state.config.shards,
        "nodes": health,
    }))
}

/// GET /_miroir/shards - Return shard assignment information.
pub async fn get_shards(State(state): State<ProxyState>) -> Json<serde_json::Value> {
    let assignments = state.get_shard_assignments().await;

    Json(serde_json::json!({
        "shards": state.config.shards,
        "replication_factor": state.config.replication_factor,
        "replica_groups": state.config.replica_groups,
        "assignments": assignments,
    }))
}

/// GET /_miroir/metrics - Return Prometheus metrics (admin-key gated).
pub async fn get_metrics(State(state): State<ProxyState>) -> String {
    export_metrics(&state.metrics)
}
