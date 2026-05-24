//! Task registry: unified task namespace across all Meilisearch nodes.

use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Task registry: manages the unified task namespace.
#[async_trait::async_trait]
pub trait TaskRegistry: Send + Sync {
    /// Register a new Miroir task that fans out to multiple nodes.
    fn register(&self, node_tasks: HashMap<String, u64>) -> Result<MiroirTask> {
        self.register_with_metadata(node_tasks, None, None)
    }

    /// Register a new Miroir task with index UID and task type.
    fn register_with_metadata(
        &self,
        node_tasks: HashMap<String, u64>,
        index_uid: Option<String>,
        task_type: Option<String>,
    ) -> Result<MiroirTask>;

    /// Get a task by its Miroir ID.
    fn get(&self, miroir_id: &str) -> Result<Option<MiroirTask>>;

    /// Update the status of a Miroir task.
    fn update_status(&self, miroir_id: &str, status: TaskStatus) -> Result<()>;

    /// Update node task status.
    fn update_node_task(
        &self,
        miroir_id: &str,
        node_id: &str,
        node_status: NodeTaskStatus,
    ) -> Result<()>;

    /// List tasks with optional filtering.
    fn list(&self, filter: TaskFilter) -> Result<Vec<MiroirTask>>;

    /// Count total tasks in the registry.
    fn count(&self) -> usize;
}

/// A Miroir task: unified view of a fan-out write operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiroirTask {
    /// Unique Miroir task ID (UUID).
    pub miroir_id: String,

    /// Creation timestamp (Unix millis).
    pub created_at: u64,

    /// Start timestamp (Unix millis).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<u64>,

    /// Finish timestamp (Unix millis).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<u64>,

    /// Current task status.
    pub status: TaskStatus,

    /// Index UID for this task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_uid: Option<String>,

    /// Task type (documentAdditionOrUpdate, documentDeletion, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_type: Option<String>,

    /// Map of node ID to local Meilisearch task UID.
    pub node_tasks: HashMap<String, NodeTask>,

    /// Error message if the task failed.
    pub error: Option<String>,

    /// Per-node error details (node_id -> error message).
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub node_errors: HashMap<String, String>,
}

/// Status of a Miroir task.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    /// Task is enqueued.
    Enqueued,

    /// Task is being processed.
    Processing,

    /// Task completed successfully.
    Succeeded,

    /// Task failed.
    Failed,

    /// Task was canceled.
    Canceled,
}

/// A node task: local Meilisearch task reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTask {
    /// Local Meilisearch task UID.
    pub task_uid: u64,

    /// Current status of this node task.
    pub status: NodeTaskStatus,
}

/// Status of a node task.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeTaskStatus {
    /// Task is enqueued on the node.
    Enqueued,

    /// Task is processing on the node.
    Processing,

    /// Task succeeded on the node.
    Succeeded,

    /// Task failed on the node.
    Failed,
}

/// Filter for listing tasks (Meilisearch-compatible).
#[derive(Debug, Clone, Default)]
pub struct TaskFilter {
    /// Filter by status.
    pub status: Option<TaskStatus>,

    /// Filter by node ID.
    pub node_id: Option<String>,

    /// Filter by index UID (Meilisearch-compatible).
    pub index_uid: Option<String>,

    /// Filter by task type (Meilisearch-compatible).
    pub task_type: Option<String>,

    /// Maximum number of results.
    pub limit: Option<usize>,

    /// Offset for pagination.
    pub offset: Option<usize>,
}

/// Default stub implementation of TaskRegistry.
#[derive(Debug, Clone, Default)]
pub struct StubTaskRegistry;

impl TaskRegistry for StubTaskRegistry {
    fn register_with_metadata(
        &self,
        _node_tasks: HashMap<String, u64>,
        _index_uid: Option<String>,
        _task_type: Option<String>,
    ) -> Result<MiroirTask> {
        Ok(MiroirTask {
            miroir_id: Uuid::new_v4().to_string(),
            created_at: 0,
            started_at: None,
            finished_at: None,
            status: TaskStatus::Enqueued,
            index_uid: None,
            task_type: None,
            node_tasks: HashMap::new(),
            error: None,
            node_errors: HashMap::new(),
        })
    }

    fn get(&self, _miroir_id: &str) -> Result<Option<MiroirTask>> {
        Ok(None)
    }

    fn update_status(&self, _miroir_id: &str, _status: TaskStatus) -> Result<()> {
        Ok(())
    }

    fn update_node_task(
        &self,
        _miroir_id: &str,
        _node_id: &str,
        _node_status: NodeTaskStatus,
    ) -> Result<()> {
        Ok(())
    }

    fn list(&self, _filter: TaskFilter) -> Result<Vec<MiroirTask>> {
        Ok(Vec::new())
    }

    fn count(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_register_returns_enqueued_task() {
        let stub = StubTaskRegistry;
        let task = stub.register(HashMap::new()).unwrap();
        assert_eq!(task.status, TaskStatus::Enqueued);
        assert!(!task.miroir_id.is_empty());
        assert!(task.node_tasks.is_empty());
        assert!(task.error.is_none());
    }

    #[test]
    fn stub_get_returns_none() {
        let stub = StubTaskRegistry;
        assert!(stub.get("any-id").unwrap().is_none());
    }

    #[test]
    fn stub_update_status_is_ok() {
        let stub = StubTaskRegistry;
        stub.update_status("any", TaskStatus::Succeeded).unwrap();
    }

    #[test]
    fn stub_update_node_task_is_ok() {
        let stub = StubTaskRegistry;
        stub.update_node_task("any", "node-0", NodeTaskStatus::Succeeded)
            .unwrap();
    }

    #[test]
    fn stub_list_returns_empty() {
        let stub = StubTaskRegistry;
        let tasks = stub.list(TaskFilter::default()).unwrap();
        assert!(tasks.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stub_task_registry_register() {
        let registry = StubTaskRegistry;
        let mut node_tasks = HashMap::new();
        node_tasks.insert("node1".to_string(), 123);

        let task = registry.register(node_tasks).unwrap();
        assert!(!task.miroir_id.is_empty());
        assert_eq!(task.status, TaskStatus::Enqueued);
        assert!(task.node_tasks.is_empty());
        assert!(task.error.is_none());
    }

    #[test]
    fn test_stub_task_registry_get() {
        let registry = StubTaskRegistry;
        let result = registry.get("test-id").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_stub_task_registry_update_status() {
        let registry = StubTaskRegistry;
        let result = registry.update_status("test-id", TaskStatus::Succeeded);
        assert!(result.is_ok());
    }

    #[test]
    fn test_stub_task_registry_update_node_task() {
        let registry = StubTaskRegistry;
        let result = registry.update_node_task("test-id", "node1", NodeTaskStatus::Succeeded);
        assert!(result.is_ok());
    }

    #[test]
    fn test_stub_task_registry_list() {
        let registry = StubTaskRegistry;
        let filter = TaskFilter::default();
        let result = registry.list(filter).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_task_status_equality() {
        assert_eq!(TaskStatus::Enqueued, TaskStatus::Enqueued);
        assert_ne!(TaskStatus::Enqueued, TaskStatus::Processing);
        assert_ne!(TaskStatus::Succeeded, TaskStatus::Failed);
    }

    #[test]
    fn test_node_task_status_equality() {
        assert_eq!(NodeTaskStatus::Enqueued, NodeTaskStatus::Enqueued);
        assert_ne!(NodeTaskStatus::Processing, NodeTaskStatus::Succeeded);
        assert_ne!(NodeTaskStatus::Failed, NodeTaskStatus::Succeeded);
    }

    #[test]
    fn test_task_filter_default() {
        let filter = TaskFilter::default();
        assert!(filter.status.is_none());
        assert!(filter.node_id.is_none());
        assert!(filter.limit.is_none());
        assert!(filter.offset.is_none());
    }

    #[test]
    fn test_task_filter_with_fields() {
        let filter = TaskFilter {
            status: Some(TaskStatus::Processing),
            node_id: Some("node1".to_string()),
            limit: Some(10),
            offset: Some(5),
        };
        assert_eq!(filter.status, Some(TaskStatus::Processing));
        assert_eq!(filter.node_id, Some("node1".to_string()));
        assert_eq!(filter.limit, Some(10));
        assert_eq!(filter.offset, Some(5));
    }

    #[test]
    fn test_miroir_task_creation() {
        let mut node_tasks = HashMap::new();
        node_tasks.insert(
            "node1".to_string(),
            NodeTask {
                task_uid: 123,
                status: NodeTaskStatus::Enqueued,
            },
        );

        let task = MiroirTask {
            miroir_id: "test-id".to_string(),
            created_at: 1234567890,
            status: TaskStatus::Processing,
            node_tasks,
            error: None,
        };

        assert_eq!(task.miroir_id, "test-id");
        assert_eq!(task.created_at, 1234567890);
        assert_eq!(task.status, TaskStatus::Processing);
        assert_eq!(task.node_tasks.len(), 1);
        assert!(task.error.is_none());
    }

    #[test]
    fn test_miroir_task_with_error() {
        let task = MiroirTask {
            miroir_id: "failed-task".to_string(),
            created_at: 0,
            status: TaskStatus::Failed,
            node_tasks: HashMap::new(),
            error: Some("Something went wrong".to_string()),
        };

        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(task.error, Some("Something went wrong".to_string()));
    }
}
