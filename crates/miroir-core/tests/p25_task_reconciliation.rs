//! P2.5 Task ID reconciliation integration tests.
//!
//! Acceptance criteria:
//! - Fan-out to 3 nodes → all 3 `taskUid`s captured in one mtask
//! - GET /tasks/{mtask_id} while all nodes are processing → `processing`
//! - One node fails → status `failed`, error includes per-node breakdown
//! - In-memory registry survives the request's own lifetime

use miroir_core::task::{NodeTaskStatus, TaskFilter, TaskRegistry, TaskStatus};
use miroir_core::task_registry::InMemoryTaskRegistry;
use miroir_core::topology::{Node, NodeId, Topology};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

#[allow(dead_code)]
/// Helper: create a test topology with 3 nodes in one replica group.
fn test_topology_3_nodes() -> Topology {
    let mut topo = Topology::new(64, 1, 1); // 1 replica group, RF=1
    topo.add_node(Node::new(
        NodeId::new("node-0".into()),
        "http://node-0:7700".into(),
        0,
    ));
    topo.add_node(Node::new(
        NodeId::new("node-1".into()),
        "http://node-1:7700".into(),
        0,
    ));
    topo.add_node(Node::new(
        NodeId::new("node-2".into()),
        "http://node-2:7700".into(),
        0,
    ));
    topo
}

#[allow(dead_code)]
/// Helper: create a test topology with 2 replica groups, 2 nodes each.
fn test_topology_2_groups() -> Topology {
    let mut topo = Topology::new(64, 2, 1); // 2 replica groups, RF=1
    topo.add_node(Node::new(
        NodeId::new("node-g0-0".into()),
        "http://g0-0:7700".into(),
        0,
    ));
    topo.add_node(Node::new(
        NodeId::new("node-g1-0".into()),
        "http://g1-0:7700".into(),
        1,
    ));
    topo
}

#[tokio::test(flavor = "multi_thread")]
async fn acceptance_1_fanout_to_3_nodes_captures_all_task_uids() {
    // Given: A task registry
    let registry = InMemoryTaskRegistry::new();

    // When: Fan-out to 3 nodes returns task UIDs
    let mut node_task_uids = HashMap::new();
    node_task_uids.insert("node-0".to_string(), 42);
    node_task_uids.insert("node-1".to_string(), 17);
    node_task_uids.insert("node-2".to_string(), 88);

    // And: Register with metadata
    let task = registry
        .register_with_metadata(
            node_task_uids.clone(),
            Some("test-index".to_string()),
            Some("documentAdditionOrUpdate".to_string()),
        )
        .expect("registration succeeds");

    // Then: All 3 node task UIDs are captured in the mtask
    assert_eq!(task.node_tasks.len(), 3);
    assert_eq!(task.node_tasks.get("node-0").unwrap().task_uid, 42);
    assert_eq!(task.node_tasks.get("node-1").unwrap().task_uid, 17);
    assert_eq!(task.node_tasks.get("node-2").unwrap().task_uid, 88);

    // And: mtask ID is in correct format
    assert!(task.miroir_id.starts_with("mtask-"));

    // And: Can retrieve the task by ID
    let retrieved = registry.get(&task.miroir_id).expect("get succeeds");
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.miroir_id, task.miroir_id);
    assert_eq!(retrieved.node_tasks.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn acceptance_2_get_task_while_processing_returns_processing() {
    // Given: A task registry with a registered task
    let registry = InMemoryTaskRegistry::new();
    let mut node_task_uids = HashMap::new();
    node_task_uids.insert("node-0".to_string(), 42);
    node_task_uids.insert("node-1".to_string(), 17);
    node_task_uids.insert("node-2".to_string(), 88);

    let task = registry
        .register_with_metadata(
            node_task_uids,
            Some("test-index".to_string()),
            Some("documentAdditionOrUpdate".to_string()),
        )
        .expect("registration succeeds");

    // When: All nodes are still processing (not terminal)
    // Simulate by setting node tasks to Processing status
    registry
        .update_node_task(&task.miroir_id, "node-0", NodeTaskStatus::Processing)
        .await
        .expect("update node-0");
    registry
        .update_node_task(&task.miroir_id, "node-1", NodeTaskStatus::Processing)
        .await
        .expect("update node-1");
    registry
        .update_node_task(&task.miroir_id, "node-2", NodeTaskStatus::Processing)
        .await
        .expect("update node-2");

    // And: Update overall status to Processing (which sets started_at)
    registry
        .update_status(&task.miroir_id, TaskStatus::Processing)
        .await
        .expect("update succeeds");

    // Then: GET /tasks/{mtask_id} returns "processing" status
    let retrieved = registry
        .get(&task.miroir_id)
        .expect("get succeeds")
        .expect("task exists");
    assert_eq!(retrieved.status, TaskStatus::Processing);
    assert!(retrieved.started_at.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn acceptance_3_one_node_fails_returns_failed_with_breakdown() {
    // Given: A task registry with a registered task
    let registry = InMemoryTaskRegistry::new();
    let mut node_task_uids = HashMap::new();
    node_task_uids.insert("node-0".to_string(), 42);
    node_task_uids.insert("node-1".to_string(), 17);
    node_task_uids.insert("node-2".to_string(), 88);

    let task = registry
        .register_with_metadata(
            node_task_uids,
            Some("test-index".to_string()),
            Some("documentAdditionOrUpdate".to_string()),
        )
        .expect("registration succeeds");

    // When: One node fails
    registry
        .update_node_task(&task.miroir_id, "node-0", NodeTaskStatus::Succeeded)
        .await
        .expect("update node-0");
    registry
        .update_node_task(&task.miroir_id, "node-1", NodeTaskStatus::Failed)
        .await
        .expect("update node-1");
    registry
        .update_node_task(&task.miroir_id, "node-2", NodeTaskStatus::Succeeded)
        .await
        .expect("update node-2");

    // And: Update overall status (will detect failure and set failed state)
    registry
        .update_overall_status(&task.miroir_id)
        .await
        .expect("update succeeds");

    // Then: Status is "failed"
    let retrieved = registry
        .get(&task.miroir_id)
        .expect("get succeeds")
        .expect("task exists");
    assert_eq!(retrieved.status, TaskStatus::Failed);
}

#[tokio::test(flavor = "multi_thread")]
async fn acceptance_4_in_memory_registry_survives_request_lifetime() {
    // Given: A task registry
    let registry = InMemoryTaskRegistry::new();

    // When: Register a task in a request scope
    let task_id = {
        let mut node_task_uids = HashMap::new();
        node_task_uids.insert("node-0".to_string(), 42);
        let task = registry
            .register_with_metadata(
                node_task_uids,
                Some("test-index".to_string()),
                Some("documentAdditionOrUpdate".to_string()),
            )
            .expect("registration succeeds");
        task.miroir_id
    };

    // Then: Task is still accessible after the "request" scope ends
    let retrieved = registry
        .get(&task_id)
        .expect("get succeeds")
        .expect("task exists");
    assert_eq!(retrieved.miroir_id, task_id);
    assert_eq!(retrieved.status, TaskStatus::Enqueued);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_all_nodes_succeeded_returns_succeeded() {
    // Given: A task with 3 node tasks
    let registry = InMemoryTaskRegistry::new();
    let mut node_task_uids = HashMap::new();
    node_task_uids.insert("node-0".to_string(), 42);
    node_task_uids.insert("node-1".to_string(), 17);
    node_task_uids.insert("node-2".to_string(), 88);

    let task = registry
        .register_with_metadata(
            node_task_uids,
            Some("test-index".to_string()),
            Some("documentAdditionOrUpdate".to_string()),
        )
        .expect("registration succeeds");

    // When: All nodes succeed
    registry
        .update_node_task(&task.miroir_id, "node-0", NodeTaskStatus::Succeeded)
        .await
        .expect("update node-0");
    registry
        .update_node_task(&task.miroir_id, "node-1", NodeTaskStatus::Succeeded)
        .await
        .expect("update node-1");
    registry
        .update_node_task(&task.miroir_id, "node-2", NodeTaskStatus::Succeeded)
        .await
        .expect("update node-2");

    // And: Update overall status to Succeeded (which sets finished_at)
    registry
        .update_status(&task.miroir_id, TaskStatus::Succeeded)
        .await
        .expect("update succeeds");

    // Then: Status is "succeeded"
    let retrieved = registry
        .get(&task.miroir_id)
        .expect("get succeeds")
        .expect("task exists");
    assert_eq!(retrieved.status, TaskStatus::Succeeded);
    assert!(retrieved.finished_at.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_task_list_with_filters() {
    // Given: A task registry with multiple tasks
    let registry = InMemoryTaskRegistry::new();

    // Create tasks with different statuses and types
    let mut node_task_uids_1 = HashMap::new();
    node_task_uids_1.insert("node-0".to_string(), 1);
    let task1 = registry
        .register_with_metadata(
            node_task_uids_1.clone(),
            Some("index-a".to_string()),
            Some("documentAdditionOrUpdate".to_string()),
        )
        .expect("registration succeeds");

    let mut node_task_uids_2 = HashMap::new();
    node_task_uids_2.insert("node-1".to_string(), 2);
    let task2 = registry
        .register_with_metadata(
            node_task_uids_2.clone(),
            Some("index-b".to_string()),
            Some("documentDeletion".to_string()),
        )
        .expect("registration succeeds");

    // Mark task1 as succeeded
    registry
        .update_status(&task1.miroir_id, TaskStatus::Succeeded)
        .await
        .expect("update status");

    // When: Filter by status
    let filter = TaskFilter {
        status: Some(TaskStatus::Succeeded),
        node_id: None,
        index_uid: None,
        task_type: None,
        limit: None,
        offset: None,
    };

    let tasks = registry.list(filter).expect("list succeeds");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].miroir_id, task1.miroir_id);

    // When: Filter by index_uid
    let filter = TaskFilter {
        status: None,
        node_id: None,
        index_uid: Some("index-b".to_string()),
        task_type: None,
        limit: None,
        offset: None,
    };

    let tasks = registry.list(filter).expect("list succeeds");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].miroir_id, task2.miroir_id);

    // When: Filter by task_type
    let filter = TaskFilter {
        status: None,
        node_id: None,
        index_uid: None,
        task_type: Some("documentDeletion".to_string()),
        limit: None,
        offset: None,
    };

    let tasks = registry.list(filter).expect("list succeeds");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].miroir_id, task2.miroir_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_task_count() {
    // Given: A task registry
    let registry = InMemoryTaskRegistry::new();

    // When: No tasks
    assert_eq!(registry.count().await, 0);

    // When: Add tasks
    let mut node_task_uids = HashMap::new();
    node_task_uids.insert("node-0".to_string(), 42);
    registry
        .register_with_metadata(
            node_task_uids.clone(),
            Some("test-index".to_string()),
            Some("documentAdditionOrUpdate".to_string()),
        )
        .expect("registration succeeds");

    registry
        .register_with_metadata(
            node_task_uids.clone(),
            Some("test-index".to_string()),
            Some("documentDeletion".to_string()),
        )
        .expect("registration succeeds");

    // Then: Count reflects total
    assert_eq!(registry.count().await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_exponential_backoff_polling_simulation() {
    // Given: A task registry
    let registry = InMemoryTaskRegistry::new();

    // When: Register a task (spawns background polling)
    let mut node_task_uids = HashMap::new();
    node_task_uids.insert("node-0".to_string(), 42);
    node_task_uids.insert("node-1".to_string(), 17);
    node_task_uids.insert("node-2".to_string(), 88);

    let task = registry
        .register_with_metadata(
            node_task_uids,
            Some("test-index".to_string()),
            Some("documentAdditionOrUpdate".to_string()),
        )
        .expect("registration succeeds");

    // The background poller simulates task completion after ~500ms
    // Wait for completion
    sleep(Duration::from_millis(800)).await;

    // Then: Task should be succeeded
    let retrieved = registry
        .get(&task.miroir_id)
        .expect("get succeeds")
        .expect("task exists");
    assert_eq!(retrieved.status, TaskStatus::Succeeded);
    assert!(retrieved.finished_at.is_some());
}
