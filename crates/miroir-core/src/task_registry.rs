//! In-memory task registry: manages Miroir task namespace.
//!
//! Phase 2 implementation: in-memory only (Phase 3 adds persistence).

use crate::Result;
use crate::task::{MiroirTask, NodeTask, NodeTaskStatus, TaskStatus, TaskFilter};
use crate::error::MiroirError;
use crate::scatter::NodeClient;
use crate::topology::{Topology, NodeId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// In-memory task registry implementation.
#[derive(Debug, Clone)]
pub struct InMemoryTaskRegistry {
    tasks: Arc<RwLock<HashMap<String, MiroirTask>>>,
}

/// Trait for node polling operations.
/// Allows the task registry to poll nodes without tight coupling to HTTP client.
#[async_trait::async_trait]
pub trait NodePoller: Send + Sync {
    /// Poll a single node for task status.
    async fn poll_node_task(
        &self,
        node_id: &NodeId,
        address: &str,
        task_uid: u64,
    ) -> std::result::Result<NodeTaskStatus, String>;
}

/// Node poller implementation using a NodeClient and Topology.
pub struct ClientNodePoller<C: NodeClient> {
    client: Arc<C>,
    topology: Arc<Topology>,
}

impl<C: NodeClient> ClientNodePoller<C> {
    /// Create a new node poller with the given client and topology.
    pub fn new(client: Arc<C>, topology: Arc<Topology>) -> Self {
        Self { client, topology }
    }
}

#[async_trait::async_trait]
impl<C: NodeClient> NodePoller for ClientNodePoller<C> {
    async fn poll_node_task(
        &self,
        node_id: &NodeId,
        address: &str,
        task_uid: u64,
    ) -> std::result::Result<NodeTaskStatus, String> {
        use crate::scatter::TaskStatusRequest;

        let req = TaskStatusRequest { task_uid };
        self.client
            .get_task_status(node_id, address, &req)
            .await
            .map(|resp| resp.to_node_status())
            .map_err(|e| format!("{:?}", e))
    }
}

impl InMemoryTaskRegistry {
    /// Create a new in-memory task registry.
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new task with the given node tasks.
    pub async fn register_async(&self, node_tasks: HashMap<String, u64>) -> Result<MiroirTask> {
        self.register_async_with_metadata(node_tasks, None, None).await
    }

    /// Register a new task with the given node tasks and metadata.
    pub async fn register_async_with_metadata(
        &self,
        node_tasks: HashMap<String, u64>,
        index_uid: Option<String>,
        task_type: Option<String>,
    ) -> Result<MiroirTask> {
        let miroir_id = format!("mtask-{}", Uuid::new_v4());
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| MiroirError::Task(format!("clock error: {}", e)))?
            .as_millis() as u64;

        let mut tasks = HashMap::new();
        for (node_id, task_uid) in node_tasks {
            tasks.insert(node_id, NodeTask {
                task_uid,
                status: NodeTaskStatus::Enqueued,
            });
        }

        let task = MiroirTask {
            miroir_id: miroir_id.clone(),
            created_at,
            started_at: None,
            finished_at: None,
            status: TaskStatus::Enqueued,
            index_uid,
            task_type,
            node_tasks: tasks,
            error: None,
            node_errors: HashMap::new(),
        };

        // Insert the task
        {
            let mut registry = self.tasks.write().await;
            registry.insert(miroir_id.clone(), task.clone());
        }

        // Spawn a background task to poll for status updates (simulated for Phase 2)
        let registry = self.clone();
        let miroir_id_clone = miroir_id.clone();
        tokio::spawn(async move {
            registry.poll_task_status_simulated(&miroir_id_clone).await;
        });

        Ok(task)
    }

    /// Register a new task with the given node tasks and metadata, with real node polling.
    ///
    /// This version takes a NodePoller implementation to actually poll nodes for status updates.
    pub async fn register_with_poller<P: NodePoller + 'static>(
        &self,
        node_tasks: HashMap<String, u64>,
        index_uid: Option<String>,
        task_type: Option<String>,
        poller: Arc<P>,
    ) -> Result<MiroirTask> {
        let miroir_id = format!("mtask-{}", Uuid::new_v4());
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| MiroirError::Task(format!("clock error: {}", e)))?
            .as_millis() as u64;

        let mut tasks = HashMap::new();
        for (node_id, task_uid) in node_tasks {
            tasks.insert(node_id.clone(), NodeTask {
                task_uid,
                status: NodeTaskStatus::Enqueued,
            });
        }

        let task = MiroirTask {
            miroir_id: miroir_id.clone(),
            created_at,
            started_at: None,
            finished_at: None,
            status: TaskStatus::Enqueued,
            index_uid,
            task_type,
            node_tasks: tasks.clone(),
            error: None,
            node_errors: HashMap::new(),
        };

        // Insert the task
        {
            let mut registry = self.tasks.write().await;
            registry.insert(miroir_id.clone(), task.clone());
        }

        // Spawn a background task to poll for status updates using real node polling
        let registry = self.clone();
        let miroir_id_clone = miroir_id.clone();
        tokio::spawn(async move {
            registry.poll_task_status_with_poller(&miroir_id_clone, poller).await;
        });

        Ok(task)
    }

    /// Get task by ID (async version).
    pub async fn get_async(&self, miroir_id: &str) -> Option<MiroirTask> {
        let tasks = self.tasks.read().await;
        tasks.get(miroir_id).cloned()
    }

    /// Delete a task from the registry.
    pub async fn delete(&self, miroir_id: &str) -> Result<bool> {
        let mut tasks = self.tasks.write().await;
        Ok(tasks.remove(miroir_id).is_some())
    }

    /// Count total tasks in the registry.
    pub async fn count(&self) -> usize {
        let tasks = self.tasks.read().await;
        tasks.len()
    }

    /// Prune old tasks (in-memory only, for Phase 3 this will use durable storage).
    pub async fn prune_old_tasks(&self, _cutoff_ms: u64) -> Result<usize> {
        // In-memory implementation: no pruning in Phase 2
        // Phase 3 will add durable storage and pruning
        Ok(0)
    }

    /// Update the overall task status based on node task statuses.
    pub async fn update_overall_status(&self, miroir_id: &str) -> Result<bool> {
        let mut tasks = self.tasks.write().await;
        let task = match tasks.get(miroir_id) {
            Some(t) => t.clone(),
            None => return Ok(false),
        };

        // Determine overall status from node tasks
        let mut all_succeeded = true;
        let mut any_failed = false;
        let mut all_terminal = true;

        for (_node_id, node_task) in &task.node_tasks {
            match node_task.status {
                NodeTaskStatus::Enqueued | NodeTaskStatus::Processing => {
                    all_terminal = false;
                    all_succeeded = false;
                }
                NodeTaskStatus::Succeeded => {}
                NodeTaskStatus::Failed => {
                    any_failed = true;
                }
            }
        }

        let new_status = if any_failed {
            TaskStatus::Failed
        } else if all_terminal && all_succeeded {
            TaskStatus::Succeeded
        } else if !all_terminal {
            TaskStatus::Processing
        } else {
            TaskStatus::Enqueued
        };

        if new_status != task.status {
            if let Some(t) = tasks.get_mut(miroir_id) {
                t.status = new_status;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Poll node tasks to update the overall Miroir task status.
    /// Uses exponential backoff: 25ms → 50 → 100 → ... → 1s cap.
    ///
    /// Phase 2: Simulates node polling (tasks complete after ~500ms)
    /// Phase 3: Will poll actual nodes via HttpClient using topology
    async fn poll_task_status_simulated(&self, miroir_id: &str) {
        let mut delay_ms = 25u64;
        let max_delay_ms = 1000u64;

        loop {
            // Get the current task state
            let task = self.get_async(miroir_id).await;

            let task = match task {
                Some(t) => t,
                None => return, // Task was deleted
            };

            // Check if we've reached a terminal state
            if matches!(task.status, TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Canceled) {
                return;
            }

            // In a real implementation, we would query the nodes here
            // For Phase 2, we simulate status progression
            // Phase 3 will add actual node polling via HttpClient

            // Check each node task's status
            let mut all_terminal = true;
            for (_node_id, node_task) in &task.node_tasks {
                match node_task.status {
                    NodeTaskStatus::Enqueued | NodeTaskStatus::Processing => {
                        all_terminal = false;
                    }
                    NodeTaskStatus::Succeeded | NodeTaskStatus::Failed => {}
                }
            }

            // For testing purposes, simulate tasks completing
            // In production, this would poll actual nodes
            if !all_terminal && delay_ms >= 500 {
                // Simulate completion for testing
                let mut tasks = self.tasks.write().await;
                if let Some(t) = tasks.get_mut(miroir_id) {
                    for (_node_id, node_task) in &mut t.node_tasks {
                        if matches!(node_task.status, NodeTaskStatus::Enqueued | NodeTaskStatus::Processing) {
                            node_task.status = NodeTaskStatus::Succeeded;
                        }
                    }
                    // Update overall status
                    let mut all_succeeded = true;
                    let mut any_failed = false;
                    for (_node_id, node_task) in &t.node_tasks {
                        match node_task.status {
                            NodeTaskStatus::Succeeded => {}
                            NodeTaskStatus::Failed => any_failed = true,
                            NodeTaskStatus::Enqueued | NodeTaskStatus::Processing => {
                                all_succeeded = false;
                            }
                        }
                    }
                    if any_failed {
                        t.status = TaskStatus::Failed;
                    } else if all_succeeded {
                        t.status = TaskStatus::Succeeded;
                    } else {
                        t.status = TaskStatus::Processing;
                    }
                    // Set finished timestamp for terminal states
                    if matches!(t.status, TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Canceled) {
                        t.finished_at = Some(std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64);
                    }
                }
                return;
            }

            // Exponential backoff with cap
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            delay_ms = (delay_ms * 2).min(max_delay_ms);
        }
    }

    /// Poll node tasks to update the overall Miroir task status, using real node polling.
    /// Uses exponential backoff: 25ms → 50 → 100 → ... → 1s cap.
    async fn poll_task_status_with_poller<P: NodePoller>(&self, miroir_id: &str, poller: Arc<P>) {
        let mut delay_ms = 25u64;
        let max_delay_ms = 1000u64;

        loop {
            // Get the current task state
            let task = self.get_async(miroir_id).await;

            let task = match task {
                Some(t) => t,
                None => return, // Task was deleted
            };

            // Check if we've reached a terminal state
            if matches!(task.status, TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Canceled) {
                return;
            }

            // Collect node IDs and task UIDs to poll
            let node_polls: Vec<(NodeId, u64)> = task.node_tasks
                .iter()
                .filter(|(_, nt)| !matches!(nt.status, NodeTaskStatus::Succeeded | NodeTaskStatus::Failed))
                .map(|(node_id, nt)| (NodeId::new(node_id.clone()), nt.task_uid))
                .collect();

            if node_polls.is_empty() {
                // All node tasks are terminal, update overall status
                let mut tasks = self.tasks.write().await;
                if let Some(t) = tasks.get_mut(miroir_id) {
                    let mut all_succeeded = true;
                    let mut any_failed = false;
                    for (_node_id, node_task) in &t.node_tasks {
                        match node_task.status {
                            NodeTaskStatus::Succeeded => {}
                            NodeTaskStatus::Failed => any_failed = true,
                            NodeTaskStatus::Enqueued | NodeTaskStatus::Processing => {
                                all_succeeded = false;
                            }
                        }
                    }
                    if any_failed {
                        t.status = TaskStatus::Failed;
                    } else if all_succeeded {
                        t.status = TaskStatus::Succeeded;
                    } else {
                        t.status = TaskStatus::Processing;
                    }
                    // Set finished timestamp for terminal states
                    if matches!(t.status, TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Canceled) {
                        t.finished_at = Some(std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64);
                    }
                }
                return;
            }

            // Poll each node for status
            let mut node_statuses = HashMap::new();
            for (node_id, task_uid) in &node_polls {
                // Get node address from topology (would need topology here)
                // For now, use a mock address - in production, this would come from the topology
                let address = format!("http://{}", node_id.as_str());

                match poller.poll_node_task(&node_id, &address, *task_uid).await {
                    Ok(status) => {
                        node_statuses.insert(node_id.clone(), status);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to poll node {} for task {}: {}", node_id, task_uid, e);
                        // On poll failure, keep the current status but mark for potential degradation
                    }
                }
            }

            // Update node task statuses
            {
                let mut tasks = self.tasks.write().await;
                if let Some(t) = tasks.get_mut(miroir_id) {
                    for (node_id, status) in node_statuses {
                        if let Some(node_task) = t.node_tasks.get_mut(node_id.as_str()) {
                            node_task.status = status;
                        }
                    }

                    // Update started_at timestamp if moving to processing
                    if t.status == TaskStatus::Enqueued {
                        let any_processing = t.node_tasks.values().any(|nt| {
                            matches!(nt.status, NodeTaskStatus::Processing)
                        });
                        if any_processing && t.started_at.is_none() {
                            t.started_at = Some(std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64);
                            t.status = TaskStatus::Processing;
                        }
                    }
                }
            }

            // Exponential backoff with cap
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            delay_ms = (delay_ms * 2).min(max_delay_ms);
        }
    }

    /// List tasks with optional filtering (Meilisearch-compatible).
    pub async fn list_async(&self, filter: &TaskFilter) -> Result<Vec<MiroirTask>> {
        let guard = self.tasks.read().await;
        let mut result: Vec<MiroirTask> = guard.values().cloned().collect();

        // Apply status filter
        if let Some(status) = filter.status {
            result.retain(|t| t.status == status);
        }

        // Apply index_uid filter
        if let Some(index_uid) = &filter.index_uid {
            result.retain(|t| t.index_uid.as_ref().map_or(false, |uid| uid == index_uid));
        }

        // Apply task_type filter
        if let Some(task_type) = &filter.task_type {
            result.retain(|t| t.task_type.as_ref().map_or(false, |ty| ty == task_type));
        }

        // Apply offset
        if let Some(offset) = filter.offset {
            if offset < result.len() {
                result = result[offset..].to_vec();
            } else {
                result.clear();
            }
        }

        // Apply limit
        if let Some(limit) = filter.limit {
            result.truncate(limit);
        }

        Ok(result)
    }
}

impl Default for InMemoryTaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Stub TaskRegistry implementation for compatibility.
/// This delegates to the async methods via tokio::task::block_in_place.
#[async_trait::async_trait]
impl crate::task::TaskRegistry for InMemoryTaskRegistry {
    fn register_with_metadata(
        &self,
        node_tasks: HashMap<String, u64>,
        index_uid: Option<String>,
        task_type: Option<String>,
    ) -> Result<MiroirTask> {
        let registry = self.clone();
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|e| MiroirError::Task(format!("runtime error: {}", e)))?;
            rt.block_on(async move {
                registry.register_async_with_metadata(node_tasks, index_uid, task_type).await
            })
        })
    }

    fn get(&self, miroir_id: &str) -> Result<Option<MiroirTask>> {
        let registry = self.clone();
        let miroir_id = miroir_id.to_string();
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|e| MiroirError::Task(format!("runtime error: {}", e)))?;
            rt.block_on(async move {
                Ok(registry.get_async(&miroir_id).await)
            })
        })
    }

    fn update_status(&self, miroir_id: &str, status: TaskStatus) -> Result<()> {
        let registry = self.clone();
        let miroir_id = miroir_id.to_string();
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|e| MiroirError::Task(format!("runtime error: {}", e)))?;
            rt.block_on(async move {
                let mut tasks = registry.tasks.write().await;
                if let Some(task) = tasks.get_mut(&miroir_id) {
                    task.status = status;
                }
                Ok(())
            })
        })
    }

    fn update_node_task(
        &self,
        miroir_id: &str,
        node_id: &str,
        node_status: NodeTaskStatus,
    ) -> Result<()> {
        let registry = self.clone();
        let miroir_id = miroir_id.to_string();
        let node_id = node_id.to_string();
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|e| MiroirError::Task(format!("runtime error: {}", e)))?;
            rt.block_on(async move {
                let mut tasks = registry.tasks.write().await;
                if let Some(task) = tasks.get_mut(&miroir_id) {
                    if let Some(node_task) = task.node_tasks.get_mut(&node_id) {
                        node_task.status = node_status;
                    }
                }
                Ok(())
            })
        })
    }

    fn list(&self, filter: TaskFilter) -> Result<Vec<MiroirTask>> {
        let registry = self.clone();
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|e| MiroirError::Task(format!("runtime error: {}", e)))?;
            rt.block_on(async move {
                registry.list_async(&filter).await
            })
        })
    }

    fn count(&self) -> usize {
        let registry = self.clone();
        tokio::task::block_in_place(|| {
            let rt = match tokio::runtime::Handle::try_current() {
                Ok(rt) => rt,
                Err(_) => return 0,
            };
            rt.block_on(async move {
                registry.count().await
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::TaskRegistry;

    #[test]
    fn test_in_memory_register_creates_task() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let registry = InMemoryTaskRegistry::new();
        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-0".to_string(), 1);
        node_tasks.insert("node-1".to_string(), 2);

        let task = rt.block_on(async {
            registry.register_async(node_tasks).await
        }).unwrap();
        assert!(task.miroir_id.starts_with("mtask-"));
        assert_eq!(task.status, TaskStatus::Enqueued);
        assert_eq!(task.node_tasks.len(), 2);
    }

    #[test]
    fn test_in_memory_get_returns_task() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let registry = InMemoryTaskRegistry::new();
        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-0".to_string(), 1);

        let task = rt.block_on(async {
            registry.register_async(node_tasks).await
        }).unwrap();
        let retrieved = rt.block_on(async {
            registry.get_async(&task.miroir_id).await
        });
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().miroir_id, task.miroir_id);
    }

    #[test]
    fn test_in_memory_list_filters_by_status() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let registry = InMemoryTaskRegistry::new();

        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-0".to_string(), 1);

        let (task1, task2) = rt.block_on(async {
            let t1 = registry.register_async(node_tasks.clone()).await.unwrap();
            let t2 = registry.register_async(node_tasks).await.unwrap();
            (t1, t2)
        });

        // Update task1 to succeeded - must be done within runtime context
        let task1_id = task1.miroir_id.clone();
        rt.block_on(async {
            let mut tasks = registry.tasks.write().await;
            if let Some(t) = tasks.get_mut(&task1_id) {
                t.status = TaskStatus::Succeeded;
            }
        });

        let filter = TaskFilter {
            status: Some(TaskStatus::Succeeded),
            node_id: None,
            index_uid: None,
            task_type: None,
            limit: None,
            offset: None,
        };

        let tasks = rt.block_on(async {
            registry.list_async(&filter).await
        }).unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].miroir_id, task1.miroir_id);
    }

    #[test]
    fn test_in_memory_update_node_task() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let registry = InMemoryTaskRegistry::new();
        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-0".to_string(), 1);

        let task = rt.block_on(async {
            registry.register_async(node_tasks).await
        }).unwrap();

        // Update node task to succeeded - must be done within runtime context
        let task_id = task.miroir_id.clone();
        rt.block_on(async {
            let mut tasks = registry.tasks.write().await;
            if let Some(t) = tasks.get_mut(&task_id) {
                if let Some(nt) = t.node_tasks.get_mut("node-0") {
                    nt.status = NodeTaskStatus::Succeeded;
                }
            }
        });

        let retrieved = rt.block_on(async {
            registry.get_async(&task.miroir_id).await
        }).unwrap();
        assert_eq!(retrieved.node_tasks.get("node-0").unwrap().status, NodeTaskStatus::Succeeded);
    }

    #[test]
    fn test_update_overall_status() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let registry = InMemoryTaskRegistry::new();
        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-0".to_string(), 1);
        node_tasks.insert("node-1".to_string(), 2);

        let task = rt.block_on(async {
            registry.register_async(node_tasks).await
        }).unwrap();

        // Mark one node as succeeded, one as processing - must be done within runtime context
        let task_id = task.miroir_id.clone();
        rt.block_on(async {
            let mut tasks = registry.tasks.write().await;
            if let Some(t) = tasks.get_mut(&task_id) {
                if let Some(nt) = t.node_tasks.get_mut("node-0") {
                    nt.status = NodeTaskStatus::Succeeded;
                }
                if let Some(nt) = t.node_tasks.get_mut("node-1") {
                    nt.status = NodeTaskStatus::Processing;
                }
            }
        });

        // Overall status should still be enqueued/processing
        let updated = rt.block_on(async {
            registry.update_overall_status(&task.miroir_id).await
        }).unwrap();
        assert!(updated);

        let retrieved = rt.block_on(async {
            registry.get_async(&task.miroir_id).await
        }).unwrap();
        assert_eq!(retrieved.status, TaskStatus::Processing);
    }

    #[test]
    fn test_in_memory_list_filters_by_index_uid() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let registry = InMemoryTaskRegistry::new();

        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-0".to_string(), 1);

        let _task1 = rt.block_on(async {
            registry.register_async_with_metadata(
                node_tasks.clone(),
                Some("index-a".to_string()),
                Some("documentAdditionOrUpdate".to_string())
            ).await
        }).unwrap();
        let _task2 = rt.block_on(async {
            registry.register_async_with_metadata(
                node_tasks.clone(),
                Some("index-b".to_string()),
                Some("documentAdditionOrUpdate".to_string())
            ).await
        }).unwrap();

        // Filter by index_uid
        let filter = TaskFilter {
            status: None,
            node_id: None,
            index_uid: Some("index-a".to_string()),
            task_type: None,
            limit: None,
            offset: None,
        };

        let tasks = rt.block_on(async {
            registry.list_async(&filter).await
        }).unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].index_uid, Some("index-a".to_string()));
    }

    #[test]
    fn test_in_memory_list_filters_by_task_type() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let registry = InMemoryTaskRegistry::new();

        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-0".to_string(), 1);

        let _task1 = rt.block_on(async {
            registry.register_async_with_metadata(
                node_tasks.clone(),
                Some("test-index".to_string()),
                Some("documentAdditionOrUpdate".to_string())
            ).await
        }).unwrap();
        let _task2 = rt.block_on(async {
            registry.register_async_with_metadata(
                node_tasks.clone(),
                Some("test-index".to_string()),
                Some("documentDeletion".to_string())
            ).await
        }).unwrap();

        // Filter by task_type
        let filter = TaskFilter {
            status: None,
            node_id: None,
            index_uid: None,
            task_type: Some("documentAdditionOrUpdate".to_string()),
            limit: None,
            offset: None,
        };

        let tasks = rt.block_on(async {
            registry.list_async(&filter).await
        }).unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].task_type, Some("documentAdditionOrUpdate".to_string()));
    }

    #[test]
    fn test_exponential_backoff_simulation() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let registry = InMemoryTaskRegistry::new();
        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-0".to_string(), 1);
        node_tasks.insert("node-1".to_string(), 2);
        node_tasks.insert("node-2".to_string(), 3);

        let task = rt.block_on(async {
            registry.register_async(node_tasks).await
        }).unwrap();

        // Wait for task to complete (simulated exponential backoff: 25 + 50 + 100 + 200 + 400 = 775ms)
        rt.block_on(async {
            tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        });

        let retrieved = rt.block_on(async {
            registry.get_async(&task.miroir_id).await
        }).unwrap();
        assert_eq!(retrieved.status, TaskStatus::Succeeded);
        assert!(retrieved.finished_at.is_some());
    }

    #[test]
    fn test_miroir_task_id_format() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let registry = InMemoryTaskRegistry::new();
        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-0".to_string(), 1);

        let task = rt.block_on(async {
            registry.register_async(node_tasks).await
        }).unwrap();
        assert!(task.miroir_id.starts_with("mtask-"));
        // UUID format: 8-4-4-4-12 hex digits
        let uuid_part = &task.miroir_id[6..];
        assert_eq!(uuid_part.len(), 36);
        assert_eq!(&task.miroir_id[5..6], "-");
    }

    #[test]
    fn test_multiple_filters_combined() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let registry = InMemoryTaskRegistry::new();

        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-0".to_string(), 1);

        // Create tasks with different combinations
        let _task1 = rt.block_on(async {
            registry.register_async_with_metadata(
                node_tasks.clone(),
                Some("index-a".to_string()),
                Some("documentAdditionOrUpdate".to_string())
            ).await
        }).unwrap();
        let task2 = rt.block_on(async {
            registry.register_async_with_metadata(
                node_tasks.clone(),
                Some("index-b".to_string()),
                Some("documentDeletion".to_string())
            ).await
        }).unwrap();

        // Mark task2 as succeeded - must be done within runtime context
        let task2_id = task2.miroir_id.clone();
        rt.block_on(async {
            let mut tasks = registry.tasks.write().await;
            if let Some(t) = tasks.get_mut(&task2_id) {
                t.status = TaskStatus::Succeeded;
            }
        });

        // Filter by both index_uid and status
        let filter = TaskFilter {
            status: Some(TaskStatus::Succeeded),
            node_id: None,
            index_uid: Some("index-b".to_string()),
            task_type: Some("documentDeletion".to_string()),
            limit: None,
            offset: None,
        };

        let tasks = rt.block_on(async {
            registry.list_async(&filter).await
        }).unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].miroir_id, task2.miroir_id);
    }
}
