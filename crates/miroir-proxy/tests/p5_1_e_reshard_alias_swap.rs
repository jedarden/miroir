//! P5.1.e: Resharding alias swap integration tests.
//!
//! Tests the alias swap step (plan §13.1 step 5):
//! - Atomic alias flip from live index to shadow index
//! - Dual-write stops after flip
//! - Rollback via reverse alias flip
//! - History retention for rollback
//!
//! This is the cutover phase that runs after verification completes,
//! making the new shadow index the live target for all client traffic.

use miroir_core::reshard::{alias_swap_phase, AliasSwapError, AliasSwapResult};
use miroir_core::task_store::{AliasHistoryEntry, NewAlias, TaskStore};
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn test_alias_swap_phase_flips_alias_to_shadow() {
    // Verify that alias_swap_phase correctly flips alias to shadow index
    let store =
        miroir_core::task_store::SqliteTaskStore::open(std::path::Path::new(":memory:")).unwrap();
    store.migrate().unwrap();

    // Create initial alias pointing at live index
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let initial_alias = NewAlias {
        name: "products".to_string(),
        kind: "single".to_string(),
        current_uid: Some("products".to_string()),
        target_uids: None,
        version: 1,
        created_at: now,
        history: vec![],
    };

    store.create_alias(&initial_alias).unwrap();

    // Perform alias swap to shadow index
    let result = alias_swap_phase("products", "products__reshard_128", &store, 10)
        .await
        .expect("alias swap should succeed");

    // Verify result structure
    assert_eq!(result.alias_name, "products");
    assert_eq!(result.old_target, "products");
    assert_eq!(result.new_target, "products__reshard_128");
    assert_eq!(result.new_version, 2); // Version incremented
    assert!(result.flipped_at > 0);

    // Verify alias was flipped in store
    let updated = store.get_alias("products").unwrap().unwrap();
    assert_eq!(
        updated.current_uid,
        Some("products__reshard_128".to_string())
    );
    assert_eq!(updated.version, 2);
    assert_eq!(updated.history.len(), 1);
    assert_eq!(updated.history[0].uid, "products");
}

#[tokio::test]
async fn test_alias_swap_phase_records_history_for_rollback() {
    // Verify that alias flip records history for rollback capability
    let store =
        miroir_core::task_store::SqliteTaskStore::open(std::path::Path::new(":memory:")).unwrap();
    store.migrate().unwrap();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let initial_alias = NewAlias {
        name: "orders".to_string(),
        kind: "single".to_string(),
        current_uid: Some("orders_v3".to_string()),
        target_uids: None,
        version: 5,
        created_at: now - 3600, // 1 hour ago
        history: vec![],
    };

    store.create_alias(&initial_alias).unwrap();

    // Perform swap
    alias_swap_phase("orders", "orders__reshard_256", &store, 10)
        .await
        .expect("alias swap should succeed");

    // Verify history was recorded
    let updated = store.get_alias("orders").unwrap().unwrap();
    assert_eq!(updated.history.len(), 1);
    assert_eq!(updated.history[0].uid, "orders_v3");
    assert!(updated.history[0].flipped_at > 0);

    // Rollback would be: flip_alias("orders", "orders_v3", ...)
}

#[tokio::test]
async fn test_alias_swap_phase_fails_on_nonexistent_alias() {
    // Verify that alias swap fails when alias doesn't exist
    let store =
        miroir_core::task_store::SqliteTaskStore::open(std::path::Path::new(":memory:")).unwrap();
    store.migrate().unwrap();

    let result = alias_swap_phase("nonexistent", "nonexistent__reshard_128", &store, 10).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        AliasSwapError::AliasNotFound(name) => assert_eq!(name, "nonexistent"),
        _ => panic!("expected AliasNotFound error"),
    }
}

#[tokio::test]
async fn test_alias_swap_phase_fails_on_multi_target_alias() {
    // Verify that alias swap fails for multi-target aliases (ILM-managed)
    let store =
        miroir_core::task_store::SqliteTaskStore::open(std::path::Path::new(":memory:")).unwrap();
    store.migrate().unwrap();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let multi_alias = NewAlias {
        name: "logs".to_string(),
        kind: "multi".to_string(),
        current_uid: None,
        target_uids: Some(vec![
            "logs-2026-01-01".to_string(),
            "logs-2026-01-02".to_string(),
        ]),
        version: 1,
        created_at: now,
        history: vec![],
    };

    store.create_alias(&multi_alias).unwrap();

    let result = alias_swap_phase("logs", "logs-2026-01-03", &store, 10).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        AliasSwapError::NotSingleTargetAlias(name) => assert_eq!(name, "logs"),
        _ => panic!("expected NotSingleTargetAlias error"),
    }
}

#[tokio::test]
async fn test_alias_swap_phase_history_retention() {
    // Verify that history retention limits the number of retained entries
    let store =
        miroir_core::task_store::SqliteTaskStore::open(std::path::Path::new(":memory:")).unwrap();
    store.migrate().unwrap();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    // Create alias with existing history
    let mut history = vec![];
    for i in 1..=5 {
        history.push(AliasHistoryEntry {
            uid: format!("v{}", i),
            flipped_at: now - (5 - i) * 100,
        });
    }

    let existing_alias = NewAlias {
        name: "current".to_string(),
        kind: "single".to_string(),
        current_uid: Some("v5".to_string()),
        target_uids: None,
        version: 5,
        created_at: now - 500,
        history,
    };

    store.create_alias(&existing_alias).unwrap();

    // Swap with retention of 5 (should keep all 5 old + 1 new = 6, then evict oldest to keep 5)
    alias_swap_phase("current", "v6", &store, 5)
        .await
        .expect("alias swap should succeed");

    let updated = store.get_alias("current").unwrap().unwrap();
    assert_eq!(updated.history.len(), 5); // Retention limit enforced
    assert_eq!(updated.history[0].uid, "v2"); // v1 evicted
    assert_eq!(updated.history[4].uid, "v5"); // Previous target
}

#[tokio::test]
async fn test_alias_swap_error_display() {
    // Verify that AliasSwapError has proper display formatting
    let errors = vec![
        AliasSwapError::AliasNotFound("test".to_string()),
        AliasSwapError::NotSingleTargetAlias("multi".to_string()),
        AliasSwapError::FlipFailed("database error".to_string()),
        AliasSwapError::LookupFailed("store unavailable".to_string()),
        AliasSwapError::TaskStoreUnavailable("no connection".to_string()),
    ];

    for error in errors {
        let msg = format!("{}", error);
        assert!(!msg.is_empty(), "error message should not be empty");
    }
}

#[tokio::test]
async fn test_alias_swap_phase_is_idempotent() {
    // Verify that calling alias_swap_phase multiple times with same target is safe
    let store =
        miroir_core::task_store::SqliteTaskStore::open(std::path::Path::new(":memory:")).unwrap();
    store.migrate().unwrap();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let initial_alias = NewAlias {
        name: "test".to_string(),
        kind: "single".to_string(),
        current_uid: Some("test_v1".to_string()),
        target_uids: None,
        version: 1,
        created_at: now,
        history: vec![],
    };

    store.create_alias(&initial_alias).unwrap();

    // First swap
    alias_swap_phase("test", "test_v2", &store, 10)
        .await
        .expect("first swap should succeed");

    let alias = store.get_alias("test").unwrap().unwrap();
    assert_eq!(alias.current_uid, Some("test_v2".to_string()));
    assert_eq!(alias.version, 2);

    // Second swap (to same target - no-op but increments version)
    alias_swap_phase("test", "test_v2", &store, 10)
        .await
        .expect("second swap should succeed");

    let alias = store.get_alias("test").unwrap().unwrap();
    assert_eq!(alias.current_uid, Some("test_v2".to_string()));
    assert_eq!(alias.version, 3); // Version still increments
}

#[tokio::test]
async fn test_alias_swap_phase_returns_result_with_correct_fields() {
    // Verify that AliasSwapResult contains all expected fields
    let store =
        miroir_core::task_store::SqliteTaskStore::open(std::path::Path::new(":memory:")).unwrap();
    store.migrate().unwrap();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let initial_alias = NewAlias {
        name: "widgets".to_string(),
        kind: "single".to_string(),
        current_uid: Some("widgets_v1".to_string()),
        target_uids: None,
        version: 1,
        created_at: now,
        history: vec![],
    };

    store.create_alias(&initial_alias).unwrap();

    let result = alias_swap_phase("widgets", "widgets__reshard_64", &store, 10)
        .await
        .expect("alias swap should succeed");

    // Verify all fields are populated correctly
    assert_eq!(result.alias_name, "widgets");
    assert_eq!(result.old_target, "widgets_v1");
    assert_eq!(result.new_target, "widgets__reshard_64");
    assert_eq!(result.new_version, 2);
    assert!(result.flipped_at > 0);
    assert!(
        result.flipped_at
            <= SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
    );
}
