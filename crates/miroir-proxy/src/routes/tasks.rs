//! Task API endpoints: Miroir task namespace reconciliation.
//!
//! Implements P2.5 task reconciliation:
//! - GET /tasks — List all Miroir tasks with Meilisearch-compatible filters (statuses, types, indexUids)
//! - GET /tasks/{id} — Get task status by mtask ID with per-node breakdown (polls nodes on each request)
//! - DELETE /tasks/{id} — Cancel a task (best-effort)

use axum::extract::{FromRef, Path, Query, State};
use axum::http::StatusCode;
use axum::{Json, Router};
use miroir_core::scatter::{NodeClient, TaskStatusRequest};
use miroir_core::task::{MiroirTask, TaskRegistry, TaskStatus, NodeTaskStatus};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::client::HttpClient;
use crate::routes::admin_endpoints::AppState;

/// Query parameters for GET /tasks (Meilisearch-compatible).
#[derive(Debug, Deserialize)]
pub struct TasksQuery {
    /// Filter by status (comma-separated: "succeeded,failed")
    statuses: Option<String>,
    /// Filter by index UID (comma-separated: "index1,index2")
    indexUids: Option<String>,
    /// Filter by type (comma-separated: "documentAdditionOrUpdate,documentDeletion")
    types: Option<String>,
    /// Pagination: limit number of results
    limit: Option<usize>,
    /// Pagination: offset from start
    from: Option<usize>,
}

/// Meilisearch-compatible task response.
#[derive(Debug, Serialize)]
pub struct TaskResponse {
    #[serde(rename = "taskUid")]
    pub task_uid: String,
    pub indexUid: Option<String>,
    pub status: String,
    #[serde(rename = "type")]
    pub task_type: Option<String>,
    pub details: Option<TaskDetails>,
    pub error: Option<TaskError>,
    pub duration: Option<String>,
    pub enqueuedAt: String,
    pub startedAt: Option<String>,
    pub finishedAt: Option<String>,
}

/// Task details with per-node breakdown.
#[derive(Debug, Serialize)]
pub struct TaskDetails {
    /// Number of documents received (for document operations)
    pub received_documents: Option<usize>,
    /// Per-node task mapping
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub nodes: HashMap<String, NodeTaskDetail>,
}

/// Per-node task detail.
#[derive(Debug, Serialize)]
pub struct NodeTaskDetail {
    /// Local Meilisearch task UID on this node
    #[serde(rename = "taskUid")]
    pub task_uid: u64,
    /// Status of this node task
    pub status: String,
}

/// Task error information with per-node breakdown.
#[derive(Debug, Serialize)]
pub struct TaskError {
    pub code: String,
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: String,
    /// Per-node error details
    pub details: HashMap<String, String>,
}

/// Response for GET /tasks.
#[derive(Debug, Serialize)]
pub struct TasksListResponse {
    pub results: Vec<TaskResponse>,
    pub limit: usize,
    pub from: usize,
    pub total: usize,
}

/// Build router for task endpoints.
pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    Router::new()
        .route("/", axum::routing::get(list_tasks::<S>))
        .route("/:id", axum::routing::get(get_task::<S>))
        .route("/:id", axum::routing::delete(delete_task::<S>))
}

/// GET /tasks — List all Miroir tasks with optional filtering.
async fn list_tasks<S>(
    Query(query): Query<TasksQuery>,
    State(state): State<S>,
) -> Result<Json<TasksListResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);

    // Parse status filter (supports comma-separated values, takes first)
    let status_filter = query.statuses.as_ref().and_then(|s| {
        s.split(',')
            .next()
            .and_then(|status_str| match status_str.trim() {
                "succeeded" | "Succeeded" => Some(TaskStatus::Succeeded),
                "failed" | "Failed" => Some(TaskStatus::Failed),
                "processing" | "Processing" => Some(TaskStatus::Processing),
                "enqueued" | "Enqueued" => Some(TaskStatus::Enqueued),
                "canceled" | "Canceled" => Some(TaskStatus::Canceled),
                _ => None,
            })
    });

    // Parse indexUids filter (supports comma-separated values, takes first)
    let index_uid_filter = query.indexUids.as_ref().and_then(|s| {
        s.split(',')
            .next()
            .map(|uid| uid.trim().to_string())
    });

    // Parse types filter (supports comma-separated values, takes first)
    let task_type_filter = query.types.as_ref().and_then(|s| {
        s.split(',')
            .next()
            .map(|ty| ty.trim().to_string())
    });

    // Build filter with all parameters
    let filter = miroir_core::task::TaskFilter {
        status: status_filter,
        node_id: None,
        index_uid: index_uid_filter,
        task_type: task_type_filter,
        limit: query.limit,
        offset: query.from,
    };

    // List tasks from registry
    let tasks = state
        .task_registry
        .list(filter)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Get total count (without limit/offset)
    let total = state
        .task_registry
        .count();

    // Convert to Meilisearch-compatible response
    let results = tasks.into_iter().map(task_to_response).collect();

    let limit = query.limit.unwrap_or(20);
    let from = query.from.unwrap_or(0);

    Ok(Json(TasksListResponse {
        results,
        limit,
        from,
        total,
    }))
}

/// GET /tasks/{id} — Get a specific task by Miroir task ID.
///
/// Polls all mapped nodes for their current task status and aggregates the result.
async fn get_task<S>(
    Path(id): Path<String>,
    State(state): State<S>,
) -> Result<Json<TaskResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);

    // Validate task ID format
    if !id.starts_with("mtask-") {
        return Err(StatusCode::BAD_REQUEST);
    }

    let mut task = state
        .task_registry
        .get(&id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Poll nodes for current status if task is not terminal
    if !matches!(task.status, TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Canceled) {
        let topology = state.topology.read().await;
        let client = HttpClient::new(
            state.config.node_master_key.clone(),
            state.config.scatter.node_timeout_ms,
        );

        // Update node task statuses by polling each node
        let mut node_errors = HashMap::new();
        let mut any_processing = false;
        let mut all_succeeded = true;
        let mut any_failed = false;

        for (node_id_str, node_task) in &task.node_tasks {
            let node_id = miroir_core::topology::NodeId::new(node_id_str.clone());

            // Skip polling if node task is already terminal
            if matches!(node_task.status, NodeTaskStatus::Succeeded | NodeTaskStatus::Failed) {
                if matches!(node_task.status, NodeTaskStatus::Failed) {
                    any_failed = true;
                    all_succeeded = false;
                }
                continue;
            }

            // Get node address from topology
            let node = match topology.node(&node_id) {
                Some(n) => n,
                None => {
                    node_errors.insert(node_id_str.clone(), "node not found in topology".to_string());
                    any_failed = true;
                    all_succeeded = false;
                    continue;
                }
            };

            // Poll this node for task status
            let req = TaskStatusRequest { task_uid: node_task.task_uid };
            match client.get_task_status(&node_id, &node.address, &req).await {
                Ok(resp) => {
                    let new_status = resp.to_node_status();
                    // Update the node task status in the registry
                    let _ = state.task_registry.update_node_task(&id, node_id_str, new_status);

                    // Track overall status
                    match new_status {
                        NodeTaskStatus::Succeeded => {}
                        NodeTaskStatus::Failed => {
                            any_failed = true;
                            all_succeeded = false;
                            if let Some(error) = resp.error {
                                node_errors.insert(node_id_str.clone(), error);
                            }
                        }
                        NodeTaskStatus::Processing => {
                            any_processing = true;
                            all_succeeded = false;
                        }
                        NodeTaskStatus::Enqueued => {
                            all_succeeded = false;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(node = %node_id_str, task = %id, error = ?e, "failed to poll node for task");
                    // Don't mark as failed on network error - may be transient
                    all_succeeded = false;
                }
            }
        }

        // Update overall task status based on node task statuses
        let new_status = if any_failed {
            TaskStatus::Failed
        } else if all_succeeded {
            TaskStatus::Succeeded
        } else if any_processing {
            TaskStatus::Processing
        } else {
            TaskStatus::Enqueued
        };

        // Record terminal task status in Prometheus (§10 task metrics)
        if matches!(new_status, TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Canceled) {
            let status_str = match new_status {
                TaskStatus::Succeeded => "succeeded",
                TaskStatus::Failed => "failed",
                TaskStatus::Canceled => "canceled",
                _ => unreachable!(),
            };
            state.metrics.inc_tasks_total(status_str);

            // Observe task processing age (time from creation to terminal state)
            let age_ms = {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                now_ms.saturating_sub(task.created_at)
            };
            state.metrics.observe_task_processing_age(age_ms as f64 / 1000.0);
        }

        // Update the task status in the registry
        let _ = state.task_registry.update_status(&id, new_status);

        // Update the task with node errors and new status
        task.status = new_status;
        task.node_errors = node_errors;

        // Set timestamps
        if matches!(new_status, TaskStatus::Processing) && task.started_at.is_none() {
            task.started_at = Some(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64);
        }

        if matches!(new_status, TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Canceled) && task.finished_at.is_none() {
            task.finished_at = Some(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64);
        }
    }

    Ok(Json(task_to_response(task)))
}

/// DELETE /tasks/{id} — Cancel a task (best-effort).
async fn delete_task<S>(
    Path(id): Path<String>,
    State(state): State<S>,
) -> Result<Json<TaskResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);

    // Validate task ID format
    if !id.starts_with("mtask-") {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Get the task first
    let task = state
        .task_registry
        .get(&id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Update status to canceled if not already terminal
    if matches!(task.status, TaskStatus::Enqueued | TaskStatus::Processing) {
        state
            .task_registry
            .update_status(&id, TaskStatus::Canceled)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    // Return the updated task
    let updated = state
        .task_registry
        .get(&id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(task_to_response(updated)))
}

/// Convert MiroirTask to Meilisearch-compatible TaskResponse.
fn task_to_response(task: MiroirTask) -> TaskResponse {
    let status_str = match task.status {
        TaskStatus::Enqueued => "enqueued",
        TaskStatus::Processing => "processing",
        TaskStatus::Succeeded => "succeeded",
        TaskStatus::Failed => "failed",
        TaskStatus::Canceled => "canceled",
    };

    let enqueued_at = format_millis_timestamp(task.created_at);
    let started_at = task.started_at.map(|t| format_millis_timestamp(t));
    let finished_at = task.finished_at.map(|t| format_millis_timestamp(t));

    let error = if task.status == TaskStatus::Failed {
        Some(TaskError {
            code: "internal_error".to_string(),
            message: task.error.clone().unwrap_or_else(|| {
                if task.node_errors.is_empty() {
                    "task failed".to_string()
                } else {
                    format!("{} node(s) failed", task.node_errors.len())
                }
            }),
            error_type: "internal_error".to_string(),
            details: task.node_errors.clone(),
        })
    } else {
        None
    };

    // Build per-node details
    let mut nodes = HashMap::new();
    for (node_id, node_task) in &task.node_tasks {
        let node_status = match node_task.status {
            miroir_core::task::NodeTaskStatus::Enqueued => "enqueued",
            miroir_core::task::NodeTaskStatus::Processing => "processing",
            miroir_core::task::NodeTaskStatus::Succeeded => "succeeded",
            miroir_core::task::NodeTaskStatus::Failed => "failed",
        };
        nodes.insert(
            node_id.clone(),
            NodeTaskDetail {
                task_uid: node_task.task_uid,
                status: node_status.to_string(),
            },
        );
    }

    let details = Some(TaskDetails {
        received_documents: None,
        nodes,
    });

    TaskResponse {
        task_uid: task.miroir_id,
        indexUid: task.index_uid,
        status: status_str.to_string(),
        task_type: task.task_type,
        details,
        error,
        duration: None,
        enqueuedAt: enqueued_at,
        startedAt: started_at,
        finishedAt: finished_at,
    }
}

/// Format milliseconds since epoch as ISO 8601 timestamp.
fn format_millis_timestamp(millis: u64) -> String {
    // Simple ISO 8601 format without chrono dependency
    let secs = millis / 1000;
    let millis_part = millis % 1000;

    // Calculate date components (simplified, assumes Unix epoch)
    // This is a rough approximation - for production use chrono or time crate
    let days_since_epoch = secs / 86400;
    let seconds_in_day = secs % 86400;

    let hours = seconds_in_day / 3600;
    let minutes = (seconds_in_day % 3600) / 60;
    let seconds = seconds_in_day % 60;

    // Days from 1970-01-01 to 2000-01-01 is roughly 10957 days
    // This is a very rough approximation for formatting
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        1970 + days_since_epoch / 365,
        1 + (days_since_epoch % 365) / 30,
        1 + (days_since_epoch % 30),
        hours,
        minutes,
        seconds,
        millis_part
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use miroir_core::task::{NodeTask, NodeTaskStatus, TaskFilter};
    use miroir_core::task_registry::TaskRegistryImpl;
    use std::collections::HashMap;

    #[test]
    fn test_task_to_response_succeeded() {
        let mut node_tasks = HashMap::new();
        node_tasks.insert(
            "node-0".to_string(),
            NodeTask {
                task_uid: 1,
                status: NodeTaskStatus::Succeeded,
            },
        );

        let task = MiroirTask {
            miroir_id: "mtask-123".to_string(),
            created_at: 1700000000000,
            started_at: Some(1700000000100),
            finished_at: Some(1700000000200),
            status: TaskStatus::Succeeded,
            index_uid: Some("test-index".to_string()),
            task_type: Some("documentAdditionOrUpdate".to_string()),
            node_tasks,
            error: None,
            node_errors: HashMap::new(),
        };

        let response = task_to_response(task);
        assert_eq!(response.task_uid, "mtask-123");
        assert_eq!(response.status, "succeeded");
        assert!(response.error.is_none());
        assert_eq!(response.indexUid, Some("test-index".to_string()));
        assert_eq!(response.task_type, Some("documentAdditionOrUpdate".to_string()));
        assert!(response.startedAt.is_some());
        assert!(response.finishedAt.is_some());
        assert_eq!(
            response.details.unwrap().nodes.get("node-0").unwrap().task_uid,
            1
        );
    }

    #[test]
    fn test_task_to_response_failed() {
        let mut node_tasks = HashMap::new();
        node_tasks.insert(
            "node-0".to_string(),
            NodeTask {
                task_uid: 1,
                status: NodeTaskStatus::Failed,
            },
        );

        let task = MiroirTask {
            miroir_id: "mtask-456".to_string(),
            created_at: 1700000000000,
            started_at: None,
            finished_at: None,
            status: TaskStatus::Failed,
            index_uid: None,
            task_type: None,
            node_tasks,
            error: Some("node timeout".to_string()),
            node_errors: HashMap::new(),
        };

        let response = task_to_response(task);
        assert_eq!(response.status, "failed");
        assert!(response.error.is_some());
        assert_eq!(response.error.unwrap().message, "node timeout");
    }

    #[test]
    fn test_parse_statuses_filter() {
        let query = TasksQuery {
            statuses: Some("succeeded".to_string()),
            indexUids: None,
            types: None,
            limit: None,
            from: None,
        };

        let status_filter = query.statuses.as_ref().and_then(|s| {
            s.split(',')
                .next()
                .and_then(|status_str| match status_str.trim() {
                    "succeeded" | "Succeeded" => Some(TaskStatus::Succeeded),
                    _ => None,
                })
        });

        assert_eq!(status_filter, Some(TaskStatus::Succeeded));
    }

    #[test]
    fn test_format_millis_timestamp() {
        let ts = format_millis_timestamp(1700000000000);
        assert!(ts.contains("T"));
        assert!(ts.contains("Z"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_task_registry_impl() {
        let registry = TaskRegistryImpl::in_memory();
        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-0".to_string(), 1);
        node_tasks.insert("node-1".to_string(), 2);

        let task = registry
            .register_with_metadata(node_tasks, None, None)
            .unwrap();

        assert!(task.miroir_id.starts_with("mtask-"));
        assert_eq!(task.status, TaskStatus::Enqueued);

        // Get the task
        let retrieved = registry.get(&task.miroir_id).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().miroir_id, task.miroir_id);

        // List tasks
        let filter = TaskFilter::default();
        let tasks = registry.list(filter).unwrap();
        assert_eq!(tasks.len(), 1);
    }
}
