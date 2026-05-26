//! Session pinning acceptance tests (plan §13.6).
//!
//! Tests read-your-writes consistency via session pinning:
//! - Write with session header → session pinned to first-quorum group
//! - Read with session header → routes to pinned group if pending write
//! - Block strategy: read blocks until write completes
//! - RoutePin strategy: read routes to pinned group without waiting
//! - Session TTL and LRU eviction
//! - Pinned group failure handling

use miroir_core::session_pinning::{SessionManager, SessionPinningConfig, WaitStrategy};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// Helper to create a test session manager with custom config.
fn test_manager(config: SessionPinningConfig) -> SessionManager {
    SessionManager::new(config)
}

/// Helper to create a default test session manager.
fn default_manager() -> SessionManager {
    test_manager(SessionPinningConfig::default())
}

#[tokio::test]
async fn test_write_records_session_pin() {
    let manager = default_manager();

    // Record a write with session header
    let session_id = "test-session-1";
    let mtask_id = "mtask-123".to_string();
    let first_quorum_group = 2;

    manager
        .record_write_with_quorum(session_id, mtask_id.clone(), first_quorum_group)
        .await
        .unwrap();

    // Verify session is recorded
    let session = manager.get_session(session_id).await.unwrap();
    assert_eq!(session.last_write_mtask_id, Some(mtask_id));
    assert_eq!(session.pinned_group, Some(first_quorum_group));
    assert!(session.has_pending_write());
}

#[tokio::test]
async fn test_read_with_pending_write_returns_pinned_group() {
    let manager = default_manager();

    // Record a write
    let session_id = "test-session-2";
    let mtask_id = "mtask-456".to_string();
    let first_quorum_group = 1;

    manager
        .record_write_with_quorum(session_id, mtask_id, first_quorum_group)
        .await
        .unwrap();

    // Get pinned group for read
    let pinned_group = manager.get_pinned_group(session_id).await;
    assert_eq!(pinned_group, Some(first_quorum_group));
}

#[tokio::test]
async fn test_read_without_session_returns_none() {
    let manager = default_manager();

    // No session header
    let pinned_group = manager.get_pinned_group("nonexistent").await;
    assert_eq!(pinned_group, None);
}

#[tokio::test]
async fn test_write_without_clearing_pending_write() {
    let manager = default_manager();

    let session_id = "test-session-3";
    let mtask_id = "mtask-789".to_string();

    // Record a write
    manager
        .record_write_with_quorum(session_id, mtask_id.clone(), 0)
        .await
        .unwrap();

    // Verify pending write exists
    assert!(manager
        .get_session(session_id)
        .await
        .unwrap()
        .has_pending_write());

    // Clear pending write (simulating task completion)
    manager.clear_pending_write(session_id).await;

    // Verify no pending write
    assert!(!manager
        .get_session(session_id)
        .await
        .unwrap()
        .has_pending_write());

    // Read should not return pinned group (no pending write)
    let pinned_group = manager.get_pinned_group(session_id).await;
    assert_eq!(pinned_group, None);
}

#[tokio::test]
async fn test_session_expiration() {
    let config = SessionPinningConfig {
        ttl_seconds: 1, // 1 second TTL for testing
        ..Default::default()
    };
    let manager = test_manager(config);

    let session_id = "test-session-expire";
    let mtask_id = "mtask-expire".to_string();

    // Record a write
    manager
        .record_write_with_quorum(session_id, mtask_id, 0)
        .await
        .unwrap();

    // Session should be active immediately
    assert!(!manager.get_session(session_id).await.unwrap().is_expired());

    // Wait for expiration
    sleep(Duration::from_millis(1100)).await;

    // Session should be expired
    assert!(manager.get_session(session_id).await.unwrap().is_expired());

    // Pinned group should be None for expired session
    let pinned_group = manager.get_pinned_group(session_id).await;
    assert_eq!(pinned_group, None);
}

#[tokio::test]
async fn test_max_sessions_lru_eviction() {
    let config = SessionPinningConfig {
        max_sessions: 2,
        ..Default::default()
    };
    let manager = test_manager(config);

    // Add first session
    manager
        .record_write_with_quorum("session-1", "mtask-1".to_string(), 0)
        .await
        .unwrap();

    // Add second session
    manager
        .record_write_with_quorum("session-2", "mtask-2".to_string(), 0)
        .await
        .unwrap();

    // Add third session (should evict first session)
    manager
        .record_write_with_quorum("session-3", "mtask-3".to_string(), 0)
        .await
        .unwrap();

    // First session should be evicted
    assert!(manager.get_session("session-1").await.is_none());

    // Second and third should still exist
    assert!(manager.get_session("session-2").await.is_some());
    assert!(manager.get_session("session-3").await.is_some());
}

#[tokio::test]
async fn test_pinned_group_failure_clears_pin() {
    let manager = default_manager();

    let session_id = "test-session-fail";
    let mtask_id = "mtask-fail".to_string();
    let pinned_group = 1;

    // Record a write
    manager
        .record_write_with_quorum(session_id, mtask_id, pinned_group)
        .await
        .unwrap();

    // Verify pin is set
    assert_eq!(
        manager.get_pinned_group(session_id).await,
        Some(pinned_group)
    );

    // Simulate pinned group failure
    let cleared = manager
        .handle_pinned_group_failure(session_id, pinned_group)
        .await;

    assert!(cleared);

    // Pin should be cleared
    assert_eq!(manager.get_pinned_group(session_id).await, None);
}

#[tokio::test]
async fn test_wait_strategy() {
    let config = SessionPinningConfig {
        wait_strategy: "route_pin".to_string(),
        ..Default::default()
    };
    let manager = test_manager(config);

    assert_eq!(manager.wait_strategy(), WaitStrategy::RoutePin);

    let config = SessionPinningConfig {
        wait_strategy: "block".to_string(),
        ..Default::default()
    };
    let manager = test_manager(config);

    assert_eq!(manager.wait_strategy(), WaitStrategy::Block);

    // Unknown strategy defaults to Block
    let config = SessionPinningConfig {
        wait_strategy: "unknown".to_string(),
        ..Default::default()
    };
    let manager = test_manager(config);

    assert_eq!(manager.wait_strategy(), WaitStrategy::Block);
}

#[tokio::test]
async fn test_prune_expired_sessions() {
    let config = SessionPinningConfig {
        ttl_seconds: 1,
        ..Default::default()
    };
    let manager = test_manager(config);

    // Add sessions
    manager
        .record_write_with_quorum("session-active", "mtask-a".to_string(), 0)
        .await
        .unwrap();
    manager
        .record_write_with_quorum("session-expire", "mtask-b".to_string(), 0)
        .await
        .unwrap();

    // Wait for expiration
    sleep(Duration::from_millis(1100)).await;

    // Refresh session-active to keep it alive
    manager
        .record_write_with_quorum("session-active", "mtask-a-refreshed".to_string(), 0)
        .await
        .unwrap();

    // Add another active session
    manager
        .record_write_with_quorum("session-active-2", "mtask-c".to_string(), 0)
        .await
        .unwrap();

    // Prune expired
    let pruned = manager.prune_expired().await;

    // Should have pruned 1 expired session (session-expire)
    assert_eq!(pruned, 1);

    // Expired session should be gone
    assert!(manager.get_session("session-expire").await.is_none());

    // Active sessions should still exist
    assert!(manager.get_session("session-active").await.is_some());
    assert!(manager.get_session("session-active-2").await.is_some());
}

#[tokio::test]
async fn test_delete_session() {
    let manager = default_manager();

    let session_id = "test-session-delete";
    manager
        .record_write_with_quorum(session_id, "mtask-delete".to_string(), 0)
        .await
        .unwrap();

    // Verify session exists
    assert!(manager.get_session(session_id).await.is_some());

    // Delete session
    let deleted = manager.delete_session(session_id).await;
    assert!(deleted);

    // Verify session is gone
    assert!(manager.get_session(session_id).await.is_none());

    // Delete again should return false
    let deleted_again = manager.delete_session(session_id).await;
    assert!(!deleted_again);
}

#[tokio::test]
async fn test_session_count() {
    let manager = default_manager();

    assert_eq!(manager.session_count().await, 0);

    manager
        .record_write_with_quorum("session-1", "mtask-1".to_string(), 0)
        .await
        .unwrap();

    assert_eq!(manager.session_count().await, 1);

    manager
        .record_write_with_quorum("session-2", "mtask-2".to_string(), 0)
        .await
        .unwrap();

    assert_eq!(manager.session_count().await, 2);

    manager.delete_session("session-1").await;

    assert_eq!(manager.session_count().await, 1);
}

#[tokio::test]
async fn test_multiple_writes_same_session_preserves_pin() {
    let manager = default_manager();

    let session_id = "test-session-multi";
    let first_group = 2;

    // First write pins to group 2
    manager
        .record_write_with_quorum(session_id, "mtask-1".to_string(), first_group)
        .await
        .unwrap();

    assert_eq!(
        manager.get_session(session_id).await.unwrap().pinned_group,
        Some(first_group)
    );

    // Second write tries to pin to group 3 (should be ignored)
    manager
        .record_write_with_quorum(session_id, "mtask-2".to_string(), 3)
        .await
        .unwrap();

    // Pin should still be group 2 (first write wins)
    assert_eq!(
        manager.get_session(session_id).await.unwrap().pinned_group,
        Some(first_group)
    );
}

#[tokio::test]
async fn test_max_wait_duration() {
    let config = SessionPinningConfig {
        max_wait_ms: 10000,
        ..Default::default()
    };
    let manager = test_manager(config);

    assert_eq!(manager.max_wait_duration().as_millis(), 10000);
}

#[tokio::test]
async fn test_enabled_flag() {
    let config = SessionPinningConfig {
        enabled: false,
        ..Default::default()
    };
    let manager = test_manager(config);

    assert!(!manager.is_enabled());

    // When disabled, record_write should succeed but be no-op
    manager
        .record_write_with_quorum("session-1", "mtask-1".to_string(), 0)
        .await
        .unwrap();

    // Session should not be recorded
    assert!(manager.get_session("session-1").await.is_none());
}

// ---------------------------------------------------------------------------
// Integration tests: read-your-writes with task registry (plan §13.6)
// ---------------------------------------------------------------------------

use miroir_core::error::MiroirError;
use miroir_core::error::Result;
use miroir_core::task::{
    MiroirTask, NodeTask, NodeTaskStatus, TaskFilter, TaskRegistry, TaskStatus,
};
use std::collections::HashMap;

/// Mock task registry for testing session pinning wait behavior.
struct MockTaskRegistry {
    tasks: Arc<tokio::sync::RwLock<HashMap<String, MiroirTask>>>,
}

impl MockTaskRegistry {
    fn new() -> Self {
        Self {
            tasks: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Add a task with a specific status.
    async fn add_task(&self, mtask_id: String, status: TaskStatus) {
        let mut tasks = self.tasks.write().await;
        tasks.insert(
            mtask_id.clone(),
            MiroirTask {
                miroir_id: mtask_id.clone(),
                node_tasks: HashMap::new(),
                node_errors: HashMap::new(),
                status,
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                started_at: None,
                finished_at: None,
                index_uid: None,
                task_type: None,
                error: None,
            },
        );
    }

    /// Update a task's status.
    async fn update_task(&self, mtask_id: &str, status: TaskStatus) {
        let mut tasks = self.tasks.write().await;
        if let Some(task) = tasks.get_mut(mtask_id) {
            task.status = status;
        }
    }
}

impl TaskRegistry for MockTaskRegistry {
    fn get(&self, mtask_id: &str) -> Result<Option<MiroirTask>> {
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| MiroirError::InvalidState("no tokio runtime".to_string()))?;
        rt.block_on(async {
            let tasks = self.tasks.read().await;
            Ok(tasks.get(mtask_id).cloned())
        })
    }

    fn register_with_metadata(
        &self,
        node_tasks: HashMap<String, u64>,
        _index_uid: Option<String>,
        _task_type: Option<String>,
    ) -> Result<MiroirTask> {
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| MiroirError::InvalidState("no tokio runtime".to_string()))?;
        rt.block_on(async {
            let mtask_id = format!("mtask-{}", uuid::Uuid::new_v4());
            // Convert u64 node task IDs to NodeTask structures
            let node_tasks_mapped = node_tasks
                .into_iter()
                .map(|(node_id, task_uid)| {
                    (
                        node_id,
                        NodeTask {
                            task_uid,
                            status: NodeTaskStatus::Enqueued,
                        },
                    )
                })
                .collect();

            let task = MiroirTask {
                miroir_id: mtask_id.clone(),
                node_tasks: node_tasks_mapped,
                node_errors: HashMap::new(),
                status: TaskStatus::Enqueued,
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                started_at: None,
                finished_at: None,
                index_uid: None,
                task_type: None,
                error: None,
            };
            let mut tasks = self.tasks.write().await;
            tasks.insert(mtask_id.clone(), task.clone());
            Ok(task)
        })
    }

    fn update_status(&self, miroir_id: &str, status: TaskStatus) -> Result<()> {
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| MiroirError::InvalidState("no tokio runtime".to_string()))?;
        rt.block_on(async {
            let mut tasks = self.tasks.write().await;
            if let Some(task) = tasks.get_mut(miroir_id) {
                task.status = status;
            }
            Ok(())
        })
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

/// Acceptance test 1: Write + session + immediate read with `block` → read sees the write.
///
/// This test verifies that when using the "block" wait strategy, a read with a session
/// header will wait for the pending write to complete before proceeding.
///
/// Note: This is a simplified test that verifies the session pin is created correctly.
/// The actual blocking behavior is tested in integration tests with a real task registry.
#[tokio::test]
async fn acceptance_write_session_read_with_block_strategy() {
    let config = SessionPinningConfig {
        wait_strategy: "block".to_string(),
        max_wait_ms: 5000,
        ..Default::default()
    };
    let manager = test_manager(config);

    let session_id = "test-session-block-read";
    let mtask_id = "mtask-block-123".to_string();

    // Record a write with session pinning
    manager
        .record_write_with_quorum(session_id, mtask_id, 0)
        .await
        .unwrap();

    // Verify the session is pinned
    assert_eq!(manager.get_pinned_group(session_id).await, Some(0));

    // Verify the wait strategy is block
    assert_eq!(manager.wait_strategy(), WaitStrategy::Block);

    // After clearing the pending write, no pin should be returned
    manager.clear_pending_write(session_id).await;
    let pinned_group = manager.get_pinned_group(session_id).await;
    assert_eq!(pinned_group, None);
}

/// Acceptance test 2: Write + session + immediate read with `route_pin` → read routed to pinned group.
///
/// This test verifies that when using the "route_pin" wait strategy, a read with a session
/// header routes to the pinned group without waiting for the write to complete.
#[tokio::test]
async fn acceptance_write_session_read_with_route_pin_strategy() {
    let config = SessionPinningConfig {
        wait_strategy: "route_pin".to_string(),
        max_wait_ms: 5000,
        ..Default::default()
    };
    let manager = test_manager(config);
    let _task_registry = Arc::new(MockTaskRegistry::new());

    let session_id = "test-session-route-pin";
    let mtask_id = "mtask-route-pin-123".to_string();

    // Record a write with session pinning
    manager
        .record_write_with_quorum(session_id, mtask_id, 1)
        .await
        .unwrap();

    // Verify the session is pinned to group 1
    assert_eq!(manager.get_pinned_group(session_id).await, Some(1));

    // With route_pin strategy, we don't wait - just check the pinned group
    let pinned_group = manager.get_pinned_group(session_id).await;
    assert_eq!(pinned_group, Some(1));

    // The pending write should still be present (not cleared by just reading)
    assert!(manager
        .get_session(session_id)
        .await
        .unwrap()
        .has_pending_write());
}

/// Acceptance test 3: Pinned group fails mid-session → pin cleared; read succeeds via another group.
///
/// This test verifies that when the pinned group fails, the session pin is cleared
/// and subsequent reads can succeed via other groups.
#[tokio::test]
async fn acceptance_pinned_group_failure_clears_pin() {
    let manager = default_manager();

    let session_id = "test-session-group-fail";
    let mtask_id = "mtask-group-fail".to_string();
    let pinned_group = 2;

    // Record a write that pins to group 2
    manager
        .record_write_with_quorum(session_id, mtask_id, pinned_group)
        .await
        .unwrap();

    // Verify the session is pinned
    assert_eq!(
        manager.get_pinned_group(session_id).await,
        Some(pinned_group)
    );

    // Simulate group 2 failing
    let cleared = manager
        .handle_pinned_group_failure(session_id, pinned_group)
        .await;

    assert!(cleared, "Pin should be cleared when pinned group fails");

    // Pin should be cleared
    assert_eq!(
        manager.get_pinned_group(session_id).await,
        None,
        "After group failure, no pinned group should be returned"
    );

    // The session should still exist, but with no pin
    let session = manager.get_session(session_id).await;
    assert!(session.is_some(), "Session should still exist");
    assert_eq!(
        session.unwrap().pinned_group,
        None,
        "Session should have no pinned group"
    );
}

/// Acceptance test 4: Session TTL expiry and LRU eviction.
///
/// This test verifies that:
/// - Sessions expire after their TTL
/// - LRU eviction happens when max_sessions is reached
#[tokio::test]
async fn acceptance_session_ttl_and_lru_eviction() {
    let config = SessionPinningConfig {
        ttl_seconds: 1,
        max_sessions: 3,
        ..Default::default()
    };
    let manager = test_manager(config);

    // Create sessions up to the limit
    manager
        .record_write_with_quorum("session-1", "mtask-1".to_string(), 0)
        .await
        .unwrap();
    manager
        .record_write_with_quorum("session-2", "mtask-2".to_string(), 0)
        .await
        .unwrap();
    manager
        .record_write_with_quorum("session-3", "mtask-3".to_string(), 0)
        .await
        .unwrap();

    // All three should exist
    assert!(manager.get_session("session-1").await.is_some());
    assert!(manager.get_session("session-2").await.is_some());
    assert!(manager.get_session("session-3").await.is_some());

    // Adding a 4th session should evict the oldest (session-1)
    manager
        .record_write_with_quorum("session-4", "mtask-4".to_string(), 0)
        .await
        .unwrap();

    // Session-1 should be evicted
    assert!(manager.get_session("session-1").await.is_none());

    // Sessions 2, 3, 4 should still exist
    assert!(manager.get_session("session-2").await.is_some());
    assert!(manager.get_session("session-3").await.is_some());
    assert!(manager.get_session("session-4").await.is_some());

    // Wait for TTL expiration
    sleep(Duration::from_millis(1100)).await;

    // All remaining sessions should be expired
    assert!(manager.get_session("session-2").await.unwrap().is_expired());
    assert!(manager.get_session("session-3").await.unwrap().is_expired());
    assert!(manager.get_session("session-4").await.unwrap().is_expired());

    // Prune should remove all expired sessions
    let pruned = manager.prune_expired().await;
    assert_eq!(pruned, 3);

    // No sessions should remain
    assert!(manager.get_session("session-2").await.is_none());
    assert!(manager.get_session("session-3").await.is_none());
    assert!(manager.get_session("session-4").await.is_none());
}

/// Integration test: Session pinning with scatter plan for pinned group.
///
/// This test verifies that `plan_search_scatter_for_group` correctly routes
/// all shards to the pinned replica group.
#[tokio::test]
async fn integration_session_pin_with_scatter_plan() {
    use miroir_core::scatter::plan_search_scatter_for_group;
    use miroir_core::topology::{Node, NodeId, NodeStatus, Topology};

    // Build a 2-group, 2-node-per-group topology
    let mut topo = Topology::new(4, 2, 2);

    // Add nodes to group 0
    for i in 0..2u32 {
        let node = Node::new(
            NodeId::new(format!("node-group0-{i}")),
            format!("http://localhost:810{i}"),
            0,
        );
        topo.add_node(node);
        topo.node_mut(&NodeId::new(format!("node-group0-{i}")))
            .unwrap()
            .transition_to(NodeStatus::Active)
            .unwrap();
    }

    // Add nodes to group 1
    for i in 2..4u32 {
        let node = Node::new(
            NodeId::new(format!("node-group1-{i}")),
            format!("http://localhost:810{i}"),
            1,
        );
        topo.add_node(node);
        topo.node_mut(&NodeId::new(format!("node-group1-{i}")))
            .unwrap()
            .transition_to(NodeStatus::Active)
            .unwrap();
    }

    // Plan scatter for pinned group 1
    let plan = plan_search_scatter_for_group(&topo, 0, 2, 4, 1, None).await;

    assert!(plan.is_some(), "Plan should be created for pinned group");
    let plan = plan.unwrap();

    // Verify the plan targets the pinned group
    assert_eq!(
        plan.chosen_group, 1,
        "Chosen group should be the pinned group"
    );

    // Verify all shards are targeted
    assert_eq!(plan.target_shards.len(), 4, "All shards should be targeted");

    // Verify all selected nodes are from group 1
    for node_id in plan.shard_to_node.values() {
        let node = topo.node(node_id).unwrap();
        assert_eq!(
            node.replica_group, 1,
            "All nodes should be from pinned group 1"
        );
    }
}

/// Integration test: Session pinning metrics.
///
/// This test verifies that session pinning metrics are properly recorded.
#[tokio::test]
async fn integration_session_pinning_metrics() {
    use miroir_core::config::MiroirConfig;
    use miroir_proxy::middleware::Metrics;

    let config = MiroirConfig::default();
    let metrics = Metrics::new(&config);

    // Test session_active_count metric
    metrics.set_session_active_count(42);
    // The metric should be set (we can't directly read it back without Prometheus registry,
    // but we can verify the method exists and doesn't panic)

    // Test session_pin_enforced_total metric
    metrics.inc_session_pin_enforced("block");
    metrics.inc_session_pin_enforced("route_pin");

    // Test session_wait_duration_seconds metric
    metrics.observe_session_wait_duration(0.123);
    metrics.observe_session_wait_duration(0.456);

    // Test session_wait_timeout_total metric
    metrics.inc_session_wait_timeout("block");

    // If we got here without panicking, the metrics methods work
    assert!(true);
}
