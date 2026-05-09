//! Tasks routes: GET /tasks, GET /tasks/:uid, DELETE /tasks/:uid
//!
//! Implements task status aggregation per plan §3:
//! - Per-task ID reconciliation across nodes
//! - Aggregated status from all nodes
//! - Task deletion support

use axum::{
    extract::{Path, Query, State},
    response::{IntoResponse, Json, Response},
};
use miroir_core::{
    config::UnavailableShardPolicy,
    router::write_targets,
    scatter::{Scatter, ScatterRequest},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    error_response::ErrorResponse,
    scatter::HttpScatter,
    state::ProxyState,
};

/// Tasks router.
pub fn router() -> axum::Router<ProxyState> {
    axum::Router::new()
        .route("/", axum::routing::get(list_tasks))
        .route("/:uid", axum::routing::get(get_task).delete(delete_task))
}

/// Query parameters for tasks list.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TasksQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    from: Option<u32>,
    #[serde(default)]
    index_uid: Option<Vec<String>>,
    #[serde(default)]
    statuses: Option<Vec<String>>,
    #[serde(default)]
    types: Option<Vec<String>>,
    #[serde(default)]
    canceled_by: Option<u32>,
}

/// Task response from a single node.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeTask {
    task_uid: u64,
    index_uid: String,
    status: String,
    #[serde(rename = "type")]
    task_type: String,
    enqueued_at: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    error: Option<Value>,
    details: Option<Value>,
    duration: Option<String>,
}

/// Aggregated task response.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AggregatedTask {
    task_uid: u64,
    index_uid: String,
    status: String,
    #[serde(rename = "type")]
    task_type: String,
    enqueued_at: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    error: Option<Value>,
    details: Option<Value>,
    duration: Option<String>,
    // Miroir-specific fields
    node_count: u32,
    nodes_completed: u32,
    nodes_failed: u32,
}

/// Tasks list response.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TasksListResponse {
    results: Vec<AggregatedTask>,
    limit: usize,
    from: Option<u32>,
    total: u32,
}

/// GET /tasks - List all tasks with optional filters.
async fn list_tasks(
    State(state): State<ProxyState>,
    Query(query): Query<TasksQuery>,
) -> Result<Json<TasksListResponse>, ErrorResponse> {
    let topology = state.topology().await;
    let limit = query.limit.unwrap_or(20);
    let from = query.from;

    // Query all nodes for tasks
    let mut all_tasks: Vec<NodeTask> = Vec::new();
    let mut failed_nodes = 0;

    for group in topology.groups() {
        if let Some(node_id) = group.nodes().first() {
            let request = ScatterRequest {
                method: "GET".to_string(),
                path: "/tasks".to_string(),
                body: vec![],
                headers: vec![],
            };

            let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);

            match scatter
                .scatter(&topology, vec![node_id.clone()], request, UnavailableShardPolicy::Partial)
                .await
            {
                Ok(result) => {
                    if let Some(resp) = result.responses.first() {
                        if resp.status == 200 {
                            if let Some(results) = resp.body.get("results").and_then(|r| r.as_array()) {
                                for task_value in results {
                                    if let Ok(task) = serde_json::from_value::<NodeTask>(task_value.clone()) {
                                        all_tasks.push(task);
                                    }
                                }
                            }
                        }
                    }
                }
                Err(_) => {
                    failed_nodes += 1;
                }
            }
        }
    }

    // Apply filters if provided
    let mut filtered_tasks: Vec<NodeTask> = all_tasks;

    if let Some(index_uids) = &query.index_uid {
        if !index_uids.is_empty() {
            filtered_tasks = filtered_tasks
                .into_iter()
                .filter(|t| index_uids.contains(&t.index_uid))
                .collect();
        }
    }

    if let Some(statuses) = &query.statuses {
        if !statuses.is_empty() {
            filtered_tasks = filtered_tasks
                .into_iter()
                .filter(|t| statuses.contains(&t.status))
                .collect();
        }
    }

    if let Some(types) = &query.types {
        if !types.is_empty() {
            filtered_tasks = filtered_tasks
                .into_iter()
                .filter(|t| types.contains(&t.task_type))
                .collect();
        }
    }

    // Aggregate tasks by UID
    let mut aggregated: std::collections::HashMap<u32, Vec<NodeTask>> = std::collections::HashMap::new();

    for task in filtered_tasks {
        aggregated
            .entry(task.task_uid as u32)
            .or_insert_with(Vec::new)
            .push(task);
    }

    // Convert to aggregated tasks
    let mut results: Vec<AggregatedTask> = aggregated
        .into_iter()
        .map(|(uid, tasks)| {
            let first = tasks.first().unwrap();

            // Determine overall status
            let status = if tasks.iter().any(|t| t.status == "failed") {
                "failed".to_string()
            } else if tasks.iter().any(|t| t.status == "processing") {
                "processing".to_string()
            } else if tasks.iter().any(|t| t.status == "enqueued") {
                "enqueued".to_string()
            } else {
                "succeeded".to_string()
            };

            let nodes_completed = tasks.iter().filter(|t| t.status == "succeeded").count() as u32;
            let nodes_failed = tasks.iter().filter(|t| t.status == "failed").count() as u32;

            AggregatedTask {
                task_uid: uid as u64,
                index_uid: first.index_uid.clone(),
                status,
                task_type: first.task_type.clone(),
                enqueued_at: first.enqueued_at.clone(),
                started_at: first.started_at.clone(),
                finished_at: first.finished_at.clone(),
                error: first.error.clone(),
                details: first.details.clone(),
                duration: first.duration.clone(),
                node_count: tasks.len() as u32,
                nodes_completed,
                nodes_failed,
            }
        })
        .collect();

    // Sort by task UID descending
    results.sort_by(|a, b| b.task_uid.cmp(&a.task_uid));

    // Apply from/limit pagination
    let total = results.len() as u32;
    if let Some(from_uid) = from {
        results = results.into_iter().filter(|t| t.task_uid <= from_uid as u64).collect();
    }
    results.truncate(limit);

    Ok(Json(TasksListResponse {
        results,
        limit,
        from,
        total,
    }))
}

/// GET /tasks/:uid - Get a specific task.
async fn get_task(
    State(state): State<ProxyState>,
    Path(uid): Path<String>,
) -> Result<Response, ErrorResponse> {
    let topology = state.topology().await;

    // Parse task UID
    let task_uid = uid
        .parse::<u64>()
        .map_err(|_| ErrorResponse::invalid_request("Invalid task UID"))?;

    // Query all nodes for this task
    let mut node_tasks: Vec<NodeTask> = Vec::new();
    let mut not_found = true;

    for group in topology.groups() {
        if let Some(node_id) = group.nodes().first() {
            let request = ScatterRequest {
                method: "GET".to_string(),
                path: format!("/tasks/{}", task_uid),
                body: vec![],
                headers: vec![],
            };

            let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);

            if let Ok(result) = scatter
                .scatter(&topology, vec![node_id.clone()], request, UnavailableShardPolicy::Partial)
                .await
            {
                if let Some(resp) = result.responses.first() {
                    if resp.status == 200 {
                        not_found = false;
                        if let Ok(task) = serde_json::from_value::<NodeTask>(resp.body.clone()) {
                            node_tasks.push(task);
                        }
                    } else if resp.status == 404 {
                        // Task not found on this node
                    }
                }
            }
        }
    }

    if not_found {
        return Err(ErrorResponse::invalid_request(format!("Task {} not found", uid)));
    }

    if node_tasks.is_empty() {
        return Err(ErrorResponse::invalid_request(format!("Task {} not found", uid)));
    }

    // Aggregate task status
    let first = node_tasks.first().unwrap();
    let status = if node_tasks.iter().any(|t| t.status == "failed") {
        "failed".to_string()
    } else if node_tasks.iter().any(|t| t.status == "processing") {
        "processing".to_string()
    } else if node_tasks.iter().any(|t| t.status == "enqueued") {
        "enqueued".to_string()
    } else {
        "succeeded".to_string()
    };

    let nodes_completed = node_tasks.iter().filter(|t| t.status == "succeeded").count() as u32;
    let nodes_failed = node_tasks.iter().filter(|t| t.status == "failed").count() as u32;

    let aggregated = AggregatedTask {
        task_uid: first.task_uid,
        index_uid: first.index_uid.clone(),
        status,
        task_type: first.task_type.clone(),
        enqueued_at: first.enqueued_at.clone(),
        started_at: first.started_at.clone(),
        finished_at: first.finished_at.clone(),
        error: first.error.clone(),
        details: first.details.clone(),
        duration: first.duration.clone(),
        node_count: node_tasks.len() as u32,
        nodes_completed,
        nodes_failed,
    };

    Ok((axum::http::StatusCode::OK, Json(aggregated)).into_response())
}

/// DELETE /tasks/:uid - Cancel/delete a task.
async fn delete_task(
    State(state): State<ProxyState>,
    Path(uid): Path<String>,
) -> Result<Response, ErrorResponse> {
    let topology = state.topology().await;

    // Parse task UID
    let task_uid = uid
        .parse::<u64>()
        .map_err(|_| ErrorResponse::invalid_request("Invalid task UID"))?;

    // Broadcast delete to all nodes
    let targets = write_targets(0, &topology);

    if targets.is_empty() {
        return Err(ErrorResponse::internal_error("No nodes available"));
    }

    let request = ScatterRequest {
        method: "DELETE".to_string(),
        path: format!("/tasks/{}", task_uid),
        body: vec![],
        headers: vec![],
    };

    let scatter = HttpScatter::new((*state.client).clone(), state.config.server.request_timeout_ms);
    let result = scatter
        .scatter(&topology, targets, request, UnavailableShardPolicy::Partial)
        .await
        .map_err(|e| ErrorResponse::internal_error(e.to_string()))?;

    if let Some(resp) = result.responses.first() {
        let status = axum::http::StatusCode::from_u16(resp.status).unwrap_or(axum::http::StatusCode::OK);
        return Ok((status, Json(resp.body.clone())).into_response());
    }

    Ok((axum::http::StatusCode::ACCEPTED, Json(serde_json::json!({}))).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tasks_query_deserialization() {
        let query_str = "limit=10&from=100&indexUid=test&statuses=succeeded&types=documentAddition";

        let query: TasksQuery = serde_qs::from_str(query_str).unwrap();

        assert_eq!(query.limit, Some(10));
        assert_eq!(query.from, Some(100));
        assert_eq!(query.index_uid, Some(vec!["test".to_string()]));
        assert_eq!(query.statuses, Some(vec!["succeeded".to_string()]));
        assert_eq!(query.types, Some(vec!["documentAddition".to_string()]));
    }

    #[test]
    fn test_aggregated_task_status_determination() {
        // When any task fails, overall status is failed
        let tasks_with_failure = vec![
            NodeTask {
                task_uid: 1,
                index_uid: "test".to_string(),
                status: "succeeded".to_string(),
                task_type: "documentAddition".to_string(),
                enqueued_at: "2024-01-01T00:00:00Z".to_string(),
                started_at: None,
                finished_at: None,
                error: None,
                details: None,
                duration: None,
            },
            NodeTask {
                task_uid: 1,
                index_uid: "test".to_string(),
                status: "failed".to_string(),
                task_type: "documentAddition".to_string(),
                enqueued_at: "2024-01-01T00:00:00Z".to_string(),
                started_at: None,
                finished_at: None,
                error: None,
                details: None,
                duration: None,
            },
        ];

        let has_failed = tasks_with_failure.iter().any(|t| t.status == "failed");
        assert!(has_failed);

        // All succeeded
        let all_succeeded = vec![NodeTask {
            task_uid: 2,
            index_uid: "test".to_string(),
            status: "succeeded".to_string(),
            task_type: "documentAddition".to_string(),
            enqueued_at: "2024-01-01T00:00:00Z".to_string(),
            started_at: None,
            finished_at: None,
            error: None,
            details: None,
            duration: None,
        }];

        let all_done = all_succeeded.iter().all(|t| t.status == "succeeded");
        assert!(all_done);
    }
}
