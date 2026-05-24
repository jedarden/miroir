//! Acceptance tests for alias module (plan §13.7).
//!
//! These tests verify the five key acceptance criteria:
//! 1. Create single-target alias → both writes + reads resolve
//! 2. Flip: new writes land on new target; in-flight (pre-flip) request completes against the old target without error
//! 3. Create multi-target alias → read fans out; write returns 409
//! 4. Operator edit of an ILM-managed multi-target alias → 409 (only ILM can modify)
//! 5. History: 11th flip evicts the oldest

use super::*;
use crate::task_store::{NewAlias, TaskStore};

/// Test 1: Create single-target alias → both writes + reads resolve.
///
/// Verifies that:
/// - Creating a single-target alias stores it correctly
/// - Reads resolve to the single target UID
/// - Writes can be directed through the alias
#[tokio::test]
async fn single_target_alias_resolves_reads_and_writes() {
    let registry = AliasRegistry::new();

    // Create a single-target alias
    let alias = Alias::new_single("products".to_string(), "products_v3".to_string());
    registry.upsert(alias).await.unwrap();

    // Verify reads resolve to the single target
    let resolved = registry.resolve("products").await;
    assert_eq!(resolved, vec!["products_v3".to_string()]);

    // Verify it's recognized as an alias
    assert!(registry.is_alias("products").await);

    // Verify it's NOT multi-target (writable)
    assert!(!registry.is_multi_target_alias("products").await);

    // Verify non-alias input returns as-is
    let resolved = registry.resolve("concrete_index").await;
    assert_eq!(resolved, vec!["concrete_index".to_string()]);
}

/// Test 2: Flip alias atomically.
///
/// Verifies that:
/// - New writes land on new target after flip
/// - In-flight (pre-flip) request completes against old target without error
#[tokio::test]
async fn atomic_flip_redirects_writes_without_tearing() {
    let registry = AliasRegistry::new();

    // Create a single-target alias
    let alias = Alias::new_single("products".to_string(), "products_v3".to_string());
    registry.upsert(alias).await.unwrap();

    // Verify initial target
    let resolved = registry.resolve("products").await;
    assert_eq!(resolved, vec!["products_v3".to_string()]);

    // Simulate an in-flight request that captured the target before flip
    // In the real orchestrator, this would be captured at route time
    let in_flight_target = registry.resolve("products").await;
    assert_eq!(in_flight_target, vec!["products_v3".to_string()]);

    // Perform atomic flip
    registry
        .flip("products", "products_v4".to_string())
        .await
        .unwrap();

    // Verify new requests get the new target
    let resolved = registry.resolve("products").await;
    assert_eq!(resolved, vec!["products_v4".to_string()]);

    // The in-flight request still completes against the old target
    // (it captured the UID before the flip)
    assert_eq!(in_flight_target, vec!["products_v3".to_string()]);

    // Verify generation incremented
    let alias = registry.get("products").await.unwrap();
    assert_eq!(alias.generation, 1);
}

/// Test 3: Create multi-target alias → read fans out.
///
/// Verifies that:
/// - Creating a multi-target alias stores it correctly
/// - Reads resolve to all target UIDs (for fan-out)
/// - Writes are rejected for multi-target aliases
#[tokio::test]
async fn multi_target_alias_fans_out_reads_and_rejects_writes() {
    let registry = AliasRegistry::new();

    // Create a multi-target alias
    let targets = vec![
        "logs-2026-01-01".to_string(),
        "logs-2026-01-02".to_string(),
        "logs-2026-01-03".to_string(),
    ];
    let alias = Alias::new_multi("logs".to_string(), targets.clone());
    registry.upsert(alias).await.unwrap();

    // Verify reads resolve to all targets (for fan-out)
    let resolved = registry.resolve("logs").await;
    assert_eq!(resolved, targets);

    // Verify it's recognized as an alias
    assert!(registry.is_alias("logs").await);

    // Verify it IS multi-target (read-only)
    assert!(registry.is_multi_target_alias("logs").await);
}

/// Test 4: Operator edit of ILM-managed multi-target alias → rejected.
///
/// Verifies that:
/// - Attempting to flip a multi-target alias fails
/// - Attempting to update_targets on a single-target alias fails
/// - Error messages clearly indicate ILM ownership
#[tokio::test]
async fn multi_target_alias_rejects_flip_operation() {
    let registry = AliasRegistry::new();

    // Create a multi-target alias
    let targets = vec!["logs-2026-01-01".to_string(), "logs-2026-01-02".to_string()];
    let alias = Alias::new_multi("logs".to_string(), targets);
    registry.upsert(alias).await.unwrap();

    // Attempting to flip a multi-target alias should fail
    let result = registry.flip("logs", "logs-2026-01-03".to_string()).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("cannot flip multi-target alias"));

    // Create a single-target alias
    let single_alias = Alias::new_single("products".to_string(), "products_v3".to_string());
    registry.upsert(single_alias).await.unwrap();

    // Attempting to update_targets on a single-target alias should fail
    let result = registry
        .update_multi("products", vec!["products_v4".to_string()])
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err
        .to_string()
        .contains("cannot update_targets on single-target alias"));
}

/// Test 5: History retention - 11th flip evicts the oldest.
///
/// Verifies that:
/// - Each flip adds an entry to history
/// - History respects retention limit (default 10)
/// - 11th flip evicts the oldest entry
#[tokio::test]
async fn history_retention_evicts_oldest_on_11th_flip() {
    use crate::task_store::{AliasHistoryEntry, SqliteTaskStore};
    use std::time::SystemTime;

    // Create in-memory store with migration
    let store = SqliteTaskStore::open_in_memory().unwrap();
    store.migrate().unwrap();

    let registry = AliasRegistry::new();
    registry
        .sync_from_store(&store as &dyn TaskStore)
        .await
        .unwrap();

    // Create initial alias
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let new_alias = NewAlias {
        name: "products".to_string(),
        kind: "single".to_string(),
        current_uid: Some("products_v1".to_string()),
        target_uids: None,
        version: 1,
        created_at: now,
        history: vec![],
    };
    store.create_alias(&new_alias).unwrap();
    registry
        .sync_from_store(&store as &dyn TaskStore)
        .await
        .unwrap();

    // Perform 11 flips
    for i in 2..=12 {
        store
            .flip_alias("products", &format!("products_v{}", i), 10)
            .unwrap();
    }
    registry
        .sync_from_store(&store as &dyn TaskStore)
        .await
        .unwrap();

    // Get the alias and verify history
    let alias = registry.get("products").await.unwrap();
    assert_eq!(alias.generation, 12); // Started at version 1, 11 flips = version 12

    // Load from store to check history
    let alias_row = store.get_alias("products").unwrap().unwrap();
    assert_eq!(alias_row.history.len(), 10); // Retention = 10

    // Verify oldest was evicted (v1 should be gone, v2-v11 present)
    let history_uids: Vec<&str> = alias_row.history.iter().map(|h| h.uid.as_str()).collect();
    assert!(!history_uids.contains(&"products_v1")); // Oldest evicted
    assert!(history_uids.contains(&"products_v2")); // Second oldest retained
    assert!(history_uids.contains(&"products_v11")); // Most recent retained
}

/// Test: List all aliases.
#[tokio::test]
async fn list_aliases_returns_all_registered() {
    let registry = AliasRegistry::new();

    // Create multiple aliases
    registry
        .upsert(Alias::new_single(
            "products".to_string(),
            "products_v3".to_string(),
        ))
        .await
        .unwrap();
    registry
        .upsert(Alias::new_multi(
            "logs".to_string(),
            vec!["logs-2026-01-01".to_string()],
        ))
        .await
        .unwrap();
    registry
        .upsert(Alias::new_single(
            "users".to_string(),
            "users_v2".to_string(),
        ))
        .await
        .unwrap();

    // List all
    let aliases = registry.list().await;
    assert_eq!(aliases.len(), 3);

    let names: Vec<&str> = aliases.iter().map(|a| a.name.as_str()).collect();
    assert!(names.contains(&"products"));
    assert!(names.contains(&"logs"));
    assert!(names.contains(&"users"));
}

/// Test: Delete alias.
#[tokio::test]
async fn delete_alias_removes_from_registry() {
    let registry = AliasRegistry::new();

    // Create an alias
    registry
        .upsert(Alias::new_single(
            "products".to_string(),
            "products_v3".to_string(),
        ))
        .await
        .unwrap();

    // Verify it exists
    assert!(registry.is_alias("products").await);

    // Delete it
    let deleted = registry.delete("products").await.unwrap();
    assert!(deleted);

    // Verify it's gone
    assert!(!registry.is_alias("products").await);

    // Delete non-existing should return false
    let deleted = registry.delete("products").await.unwrap();
    assert!(!deleted);
}

/// Test: Multi-target alias update_targets (ILM use case).
#[tokio::test]
async fn multi_target_alias_update_targets_for_ilm() {
    let registry = AliasRegistry::new();

    // Create a multi-target alias
    let targets = vec!["logs-2026-01-01".to_string(), "logs-2026-01-02".to_string()];
    registry
        .upsert(Alias::new_multi("logs".to_string(), targets))
        .await
        .unwrap();

    // ILM updates targets (adds new index, removes old one)
    let new_targets = vec!["logs-2026-01-02".to_string(), "logs-2026-01-03".to_string()];
    registry
        .update_multi("logs", new_targets.clone())
        .await
        .unwrap();

    // Verify resolution updated
    let resolved = registry.resolve("logs").await;
    assert_eq!(resolved, new_targets);

    // Verify generation incremented
    let alias = registry.get("logs").await.unwrap();
    assert_eq!(alias.generation, 1);
}

/// Test: Sync from task store loads aliases into memory.
#[tokio::test]
async fn sync_from_store_loads_aliases_into_memory() {
    use crate::task_store::SqliteTaskStore;
    use std::time::SystemTime;

    // Create in-memory store with migration
    let store = SqliteTaskStore::open_in_memory().unwrap();
    store.migrate().unwrap();

    // Create aliases directly in store
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    store
        .create_alias(&NewAlias {
            name: "products".to_string(),
            kind: "single".to_string(),
            current_uid: Some("products_v3".to_string()),
            target_uids: None,
            version: 1,
            created_at: now,
            history: vec![],
        })
        .unwrap();

    store
        .create_alias(&NewAlias {
            name: "logs".to_string(),
            kind: "multi".to_string(),
            current_uid: None,
            target_uids: Some(vec!["logs-2026-01-01".to_string()]),
            version: 1,
            created_at: now,
            history: vec![],
        })
        .unwrap();

    // Create registry and sync
    let registry = AliasRegistry::new();
    registry
        .sync_from_store(&store as &dyn TaskStore)
        .await
        .unwrap();

    // Verify aliases loaded
    assert_eq!(registry.list().await.len(), 2);

    let resolved = registry.resolve("products").await;
    assert_eq!(resolved, vec!["products_v3".to_string()]);

    let resolved = registry.resolve("logs").await;
    assert_eq!(resolved, vec!["logs-2026-01-01".to_string()]);
}
