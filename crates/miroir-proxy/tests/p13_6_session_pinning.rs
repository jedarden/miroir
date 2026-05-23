//! Session pinning acceptance tests (plan §13.6).
//!
//! Tests read-your-writes consistency via session pinning:
//! - Write with session header → session pinned to first-quorum group
//! - Read with session header → routes to pinned group if pending write
//! - Block strategy: read blocks until write completes
//! - RoutePin strategy: read routes to pinned group without waiting
//! - Session TTL and LRU eviction
//! - Pinned group failure handling

use std::sync::Arc;
use tokio::time::{sleep, Duration};
use miroir_core::session_pinning::{SessionManager, SessionPinningConfig, WaitStrategy};

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
    assert!(manager.get_session(session_id).await.unwrap().has_pending_write());

    // Clear pending write (simulating task completion)
    manager.clear_pending_write(session_id).await;

    // Verify no pending write
    assert!(!manager.get_session(session_id).await.unwrap().has_pending_write());

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
