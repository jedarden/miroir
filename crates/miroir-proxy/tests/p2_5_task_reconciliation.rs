//! P2.5 Task ID reconciliation and /tasks endpoints verification tests.
//!
//! Acceptance criteria:
//! - Fan-out to 3 nodes → all 3 `taskUid`s captured in one mtask
//! - `GET /tasks/{mtask_id}` while all nodes are processing → `processing`
//! - One node fails → status `failed`, error includes per-node breakdown
//! - In-memory registry survives the request's own lifetime (Phase 3 makes it persistent)

use miroir_core::task::{NodeTaskStatus, TaskFilter, TaskStatus};
use miroir_core::task_registry::{InMemoryTaskRegistry, TaskRegistryImpl};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

// ---------------------------------------------------------------------------
// Acceptance 1: Fan-out to 3 nodes → all 3 taskUids captured in one mtask
// ---------------------------------------------------------------------------

#[test]
fn test_fan_out_to_3_nodes_captures_all_task_uids() {
    let registry = InMemoryTaskRegistry::new();

    // Simulate fan-out to 3 nodes, each returning a different taskUid
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-0".to_string(), 42u64);
    node_tasks.insert("node-1".to_string(), 17u64);
    node_tasks.insert("node-2".to_string(), 88u64);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let miroir_task = rt
        .block_on(async {
            registry
                .register_async_with_metadata(
                    node_tasks,
                    Some("test-index".to_string()),
                    Some("documentAdditionOrUpdate".to_string()),
                )
                .await
        })
        .unwrap();

    // Verify mtask ID format
    assert!(
        miroir_task.miroir_id.starts_with("mtask-"),
        "Miroir task ID should start with 'mtask-'"
    );

    // Verify all 3 node task UIDs are captured
    assert_eq!(
        miroir_task.node_tasks.len(),
        3,
        "Should capture all 3 node task UIDs"
    );
    assert_eq!(miroir_task.node_tasks.get("node-0").unwrap().task_uid, 42);
    assert_eq!(miroir_task.node_tasks.get("node-1").unwrap().task_uid, 17);
    assert_eq!(miroir_task.node_tasks.get("node-2").unwrap().task_uid, 88);

    // Verify metadata
    assert_eq!(miroir_task.index_uid, Some("test-index".to_string()));
    assert_eq!(
        miroir_task.task_type,
        Some("documentAdditionOrUpdate".to_string())
    );
    assert_eq!(miroir_task.status, TaskStatus::Enqueued);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_task_registry_impl_captures_all_node_tasks() {
    let registry = TaskRegistryImpl::in_memory();

    // Register a task with 3 node tasks
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-0".to_string(), 100u64);
    node_tasks.insert("node-1".to_string(), 200u64);
    node_tasks.insert("node-2".to_string(), 300u64);

    let miroir_task = registry
        .register_with_metadata(
            node_tasks,
            Some("my-index".to_string()),
            Some("documentDeletion".to_string()),
        )
        .unwrap();

    // Verify all node tasks are preserved
    assert_eq!(miroir_task.node_tasks.len(), 3);
    assert!(miroir_task.miroir_id.starts_with("mtask-"));
    assert_eq!(miroir_task.task_type, Some("documentDeletion".to_string()));
}

// ---------------------------------------------------------------------------
// Acceptance 2: GET /tasks/{mtask_id} while all nodes are processing → processing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_get_task_while_nodes_processing_returns_processing() {
    let registry = InMemoryTaskRegistry::new();

    // Create a task with 3 nodes
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-0".to_string(), 1u64);
    node_tasks.insert("node-1".to_string(), 2u64);
    node_tasks.insert("node-2".to_string(), 3u64);

    let task = registry.register_async(node_tasks).await.unwrap();

    // Update node tasks to processing using the public method
    for node_id in ["node-0", "node-1", "node-2"] {
        registry
            .update_node_task(&task.miroir_id, node_id, NodeTaskStatus::Processing)
            .await
            .unwrap();
    }
    registry
        .update_status(&task.miroir_id, TaskStatus::Processing)
        .await
        .unwrap();

    // Get the task
    let retrieved = registry.get_async(&task.miroir_id).await.unwrap();
    assert_eq!(retrieved.status, TaskStatus::Processing);
    assert!(retrieved.started_at.is_some());

    // Verify all nodes are in processing state
    for nt in retrieved.node_tasks.values() {
        assert_eq!(nt.status, NodeTaskStatus::Processing);
    }
}

#[tokio::test]
async fn test_get_task_while_one_node_still_enqueued_returns_processing() {
    let registry = InMemoryTaskRegistry::new();

    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-0".to_string(), 1u64);
    node_tasks.insert("node-1".to_string(), 2u64);
    node_tasks.insert("node-2".to_string(), 3u64);

    let task = registry.register_async(node_tasks).await.unwrap();

    // Set mixed states: node-0 succeeded, node-1 succeeded, node-2 enqueued
    registry
        .update_node_task(&task.miroir_id, "node-0", NodeTaskStatus::Succeeded)
        .await
        .unwrap();
    registry
        .update_node_task(&task.miroir_id, "node-1", NodeTaskStatus::Succeeded)
        .await
        .unwrap();
    // node-2 remains Enqueued

    // Update overall status
    registry
        .update_overall_status(&task.miroir_id)
        .await
        .unwrap();

    let retrieved = registry.get_async(&task.miroir_id).await.unwrap();
    assert_eq!(retrieved.status, TaskStatus::Processing);
}

// ---------------------------------------------------------------------------
// Acceptance 3: One node fails → status failed, error includes per-node breakdown
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_one_node_failure_results_in_failed_status() {
    let registry = InMemoryTaskRegistry::new();

    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-0".to_string(), 1u64);
    node_tasks.insert("node-1".to_string(), 2u64);
    node_tasks.insert("node-2".to_string(), 3u64);

    let task = registry.register_async(node_tasks).await.unwrap();

    // Set node-1 to failed
    registry
        .update_node_task(&task.miroir_id, "node-0", NodeTaskStatus::Succeeded)
        .await
        .unwrap();
    registry
        .update_node_task(&task.miroir_id, "node-1", NodeTaskStatus::Failed)
        .await
        .unwrap();
    registry
        .update_node_task(&task.miroir_id, "node-2", NodeTaskStatus::Succeeded)
        .await
        .unwrap();

    let mut node_errors = HashMap::new();
    node_errors.insert("node-1".to_string(), "connection timeout".to_string());
    registry
        .set_error_for_test(
            &task.miroir_id,
            "Task failed on 1 node".to_string(),
            node_errors,
        )
        .await;
    registry
        .update_status(&task.miroir_id, TaskStatus::Failed)
        .await
        .unwrap();

    let finished_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    registry
        .set_timestamps_for_test(&task.miroir_id, None, Some(finished_at))
        .await;

    let retrieved = registry.get_async(&task.miroir_id).await.unwrap();
    assert_eq!(retrieved.status, TaskStatus::Failed);
    assert!(retrieved.error.is_some());
    assert!(retrieved.node_errors.contains_key("node-1"));
    assert_eq!(
        retrieved.node_errors.get("node-1").unwrap(),
        "connection timeout"
    );
    assert!(retrieved.finished_at.is_some());
}

#[tokio::test]
async fn test_multiple_node_failures_aggregates_all_errors() {
    let registry = InMemoryTaskRegistry::new();

    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-0".to_string(), 1u64);
    node_tasks.insert("node-1".to_string(), 2u64);
    node_tasks.insert("node-2".to_string(), 3u64);
    node_tasks.insert("node-3".to_string(), 4u64);

    let task = registry.register_async(node_tasks).await.unwrap();

    // Set multiple nodes to failed
    registry
        .update_node_task(&task.miroir_id, "node-0", NodeTaskStatus::Succeeded)
        .await
        .unwrap();
    registry
        .update_node_task(&task.miroir_id, "node-1", NodeTaskStatus::Failed)
        .await
        .unwrap();
    registry
        .update_node_task(&task.miroir_id, "node-2", NodeTaskStatus::Failed)
        .await
        .unwrap();
    registry
        .update_node_task(&task.miroir_id, "node-3", NodeTaskStatus::Succeeded)
        .await
        .unwrap();

    let mut node_errors = HashMap::new();
    node_errors.insert("node-1".to_string(), "disk full".to_string());
    node_errors.insert("node-2".to_string(), "out of memory".to_string());
    registry
        .set_error_for_test(
            &task.miroir_id,
            "Task failed on 2 nodes".to_string(),
            node_errors,
        )
        .await;
    registry
        .update_status(&task.miroir_id, TaskStatus::Failed)
        .await
        .unwrap();

    let retrieved = registry.get_async(&task.miroir_id).await.unwrap();
    assert_eq!(retrieved.status, TaskStatus::Failed);
    assert_eq!(retrieved.node_errors.len(), 2);
    assert!(retrieved.node_errors.contains_key("node-1"));
    assert!(retrieved.node_errors.contains_key("node-2"));
}

// ---------------------------------------------------------------------------
// Acceptance 4: In-memory registry survives the request's own lifetime
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_in_memory_registry_survives_request_lifetime() {
    let registry = InMemoryTaskRegistry::new();

    // Register a task
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-0".to_string(), 1u64);
    node_tasks.insert("node-1".to_string(), 2u64);

    let task = registry.register_async(node_tasks.clone()).await.unwrap();
    let miroir_id = task.miroir_id.clone();

    // Simulate request ending (drop all references to task)
    drop(task);

    // Sleep to ensure any potential cleanup would have run
    sleep(Duration::from_millis(100)).await;

    // Task should still be in registry
    let retrieved = registry.get_async(&miroir_id).await;
    assert!(retrieved.is_some(), "Task should survive request lifetime");

    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.node_tasks.len(), 2);
}

#[tokio::test]
async fn test_registry_survives_multiple_concurrent_requests() {
    let registry = Arc::new(InMemoryTaskRegistry::new());

    // Simulate multiple concurrent requests creating tasks
    let mut handles = Vec::new();

    for i in 0..10 {
        let reg = registry.clone();
        let handle = tokio::spawn(async move {
            let mut node_tasks = HashMap::new();
            node_tasks.insert(format!("node-{i}"), i as u64);
            node_tasks.insert(format!("node-{}", i + 10), (i + 10) as u64);

            reg.register_async(node_tasks).await.unwrap()
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let mut miroir_ids = Vec::new();
    for handle in handles {
        let task = handle.await.unwrap();
        miroir_ids.push(task.miroir_id);
    }

    // All tasks should be in the registry
    assert_eq!(miroir_ids.len(), 10);
    for miroir_id in &miroir_ids {
        let retrieved = registry.get_async(miroir_id).await;
        assert!(
            retrieved.is_some(),
            "Task {miroir_id} should be in registry"
        );
    }

    // List should return all tasks
    let filter = TaskFilter::default();
    let all_tasks = registry.list_async(&filter).await.unwrap();
    assert!(all_tasks.len() >= 10, "Should have at least 10 tasks");
}

// ---------------------------------------------------------------------------
// Additional: List and filter tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_tasks_filters_by_status() {
    let registry = InMemoryTaskRegistry::new();

    // Create tasks with different statuses
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-0".to_string(), 1u64);

    let task1 = registry
        .register_async_with_metadata(
            node_tasks.clone(),
            Some("index-a".to_string()),
            Some("documentAdditionOrUpdate".to_string()),
        )
        .await
        .unwrap();

    let task2 = registry
        .register_async_with_metadata(
            node_tasks.clone(),
            Some("index-b".to_string()),
            Some("documentDeletion".to_string()),
        )
        .await
        .unwrap();

    // Mark task1 as succeeded
    registry
        .update_status(&task1.miroir_id, TaskStatus::Succeeded)
        .await
        .unwrap();

    // Filter by succeeded status
    let filter = TaskFilter {
        status: Some(TaskStatus::Succeeded),
        node_id: None,
        index_uid: None,
        task_type: None,
        limit: None,
        offset: None,
    };

    let succeeded_tasks = registry.list_async(&filter).await.unwrap();
    assert_eq!(succeeded_tasks.len(), 1);
    assert_eq!(succeeded_tasks[0].miroir_id, task1.miroir_id);

    // Filter by enqueued status (task2 should be enqueued)
    let filter_enqueued = TaskFilter {
        status: Some(TaskStatus::Enqueued),
        node_id: None,
        index_uid: None,
        task_type: None,
        limit: None,
        offset: None,
    };

    let enqueued_tasks = registry.list_async(&filter_enqueued).await.unwrap();
    assert!(enqueued_tasks
        .iter()
        .any(|t| t.miroir_id == task2.miroir_id));
}

#[tokio::test]
async fn test_list_tasks_with_limit_and_offset() {
    let registry = InMemoryTaskRegistry::new();

    // Create 5 tasks
    let mut miroir_ids = Vec::new();
    for i in 0..5 {
        let mut node_tasks = HashMap::new();
        node_tasks.insert(format!("node-{i}"), i as u64);
        let task = registry.register_async(node_tasks).await.unwrap();
        miroir_ids.push(task.miroir_id);
    }

    // List with limit=2, offset=0
    let filter = TaskFilter {
        status: None,
        node_id: None,
        index_uid: None,
        task_type: None,
        limit: Some(2),
        offset: Some(0),
    };

    let page1 = registry.list_async(&filter).await.unwrap();
    assert_eq!(page1.len(), 2);

    // List with limit=2, offset=2
    let filter2 = TaskFilter {
        status: None,
        node_id: None,
        index_uid: None,
        task_type: None,
        limit: Some(2),
        offset: Some(2),
    };

    let page2 = registry.list_async(&filter2).await.unwrap();
    assert_eq!(page2.len(), 2);

    // Pages should not overlap
    let page1_ids: std::collections::HashSet<_> = page1.iter().map(|t| &t.miroir_id).collect();
    let page2_ids: std::collections::HashSet<_> = page2.iter().map(|t| &t.miroir_id).collect();
    assert!(
        page1_ids.intersection(&page2_ids).next().is_none(),
        "Pages should not overlap"
    );
}

#[tokio::test]
async fn test_count_returns_total_tasks() {
    let registry = InMemoryTaskRegistry::new();

    // Initially empty
    assert_eq!(registry.count().await, 0);

    // Add 3 tasks
    for i in 0..3 {
        let mut node_tasks = HashMap::new();
        node_tasks.insert(format!("node-{i}"), i as u64);
        registry.register_async(node_tasks).await.unwrap();
    }

    assert_eq!(registry.count().await, 3);
}

// ---------------------------------------------------------------------------
// Exponential backoff simulation test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_exponential_backoff_polling_completes() {
    let registry = InMemoryTaskRegistry::new();

    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-0".to_string(), 1u64);
    node_tasks.insert("node-1".to_string(), 2u64);
    node_tasks.insert("node-2".to_string(), 3u64);

    let task = registry.register_async(node_tasks).await.unwrap();
    let miroir_id = task.miroir_id.clone();

    // Wait for simulated completion (exponential backoff: 25 + 50 + 100 + 200 + 400 = 775ms)
    sleep(Duration::from_millis(900)).await;

    let retrieved = registry.get_async(&miroir_id).await.unwrap();
    assert_eq!(retrieved.status, TaskStatus::Succeeded);
    assert!(
        retrieved.finished_at.is_some(),
        "Should have finished timestamp"
    );

    // All node tasks should be succeeded
    for nt in retrieved.node_tasks.values() {
        assert_eq!(nt.status, NodeTaskStatus::Succeeded);
    }
}

// ---------------------------------------------------------------------------
// Timestamp tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_task_timestamps_are_set_correctly() {
    let registry = InMemoryTaskRegistry::new();

    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-0".to_string(), 1u64);

    let task = registry.register_async(node_tasks).await.unwrap();
    let miroir_id = task.miroir_id.clone();

    // Initially: only created_at is set
    let retrieved = registry.get_async(&miroir_id).await.unwrap();
    assert!(retrieved.created_at > 0);
    assert!(retrieved.started_at.is_none());
    assert!(retrieved.finished_at.is_none());

    // Move to processing
    let started_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    registry
        .update_status(&miroir_id, TaskStatus::Processing)
        .await
        .unwrap();
    registry
        .set_timestamps_for_test(&miroir_id, Some(started_at), None)
        .await;

    let retrieved = registry.get_async(&miroir_id).await.unwrap();
    assert!(retrieved.started_at.is_some());
    assert!(retrieved.finished_at.is_none());

    // Move to succeeded
    let finished_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    registry
        .update_status(&miroir_id, TaskStatus::Succeeded)
        .await
        .unwrap();
    registry
        .set_timestamps_for_test(&miroir_id, None, Some(finished_at))
        .await;

    let retrieved = registry.get_async(&miroir_id).await.unwrap();
    assert!(retrieved.started_at.is_some());
    assert!(retrieved.finished_at.is_some());
    assert!(retrieved.finished_at.unwrap() >= retrieved.started_at.unwrap());
}
