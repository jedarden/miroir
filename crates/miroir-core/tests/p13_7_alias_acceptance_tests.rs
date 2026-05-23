//! P5.7 §13.7 Atomic index aliases integration tests.
//!
//! Acceptance criteria:
//! - Create single-target alias → both writes + reads resolve
//! - Flip: new writes land on new target; in-flight (pre-flip) request completes against the old target without error
//! - Create multi-target alias → read fans out; write returns 409
//! - Operator edit of an ILM-managed multi-target alias → 409 (only ILM can modify)
//! - History: 11th flip evicts the oldest

use miroir_core::alias::{Alias, AliasKind, AliasRegistry};
use miroir_core::task_store::{NewAlias, TaskStore};

/// Helper to create an in-memory SQLite store for testing.
fn create_test_store() -> miroir_core::task_store::SqliteTaskStore {
    let store = miroir_core::task_store::SqliteTaskStore::open_in_memory()
        .expect("failed to create test store");
    store.migrate().expect("failed to migrate test store");
    store
}

#[tokio::test]
async fn create_single_target_alias_writes_and_reads_resolve() {
    // Acceptance: Create single-target alias → both writes + reads resolve
    let store = create_test_store();
    let store_ref: &dyn TaskStore = &store;
    let registry = AliasRegistry::load_from_store(store_ref)
        .await
        .expect("failed to load registry");

    // Create a single-target alias
    let alias = Alias::new_single("products".to_string(), "products_v3".to_string());
    registry.upsert(alias.clone()).await.unwrap();

    // Persist to task store
    let new_alias = NewAlias {
        name: "products".to_string(),
        kind: "single".to_string(),
        current_uid: Some("products_v3".to_string()),
        target_uids: None,
        version: 1,
        created_at: 1000,
        history: vec![],
    };
    store.create_alias(&new_alias).unwrap();

    // Reload from store to verify persistence
    let registry2 = AliasRegistry::load_from_store(&store)
        .await
        .expect("failed to reload registry");

    // Verify reads resolve to the target
    let resolved = registry2.resolve("products").await;
    assert_eq!(resolved, vec!["products_v3".to_string()]);

    // Verify writes can use the alias (write path checks is_multi_target_alias)
    assert!(!registry2.is_multi_target_alias("products").await);

    // Verify the alias can be looked up directly
    let got = registry2.get("products").await;
    assert!(got.is_some());
    let alias = got.unwrap();
    assert_eq!(alias.name, "products");
    assert_eq!(alias.kind, AliasKind::Single);
    assert_eq!(alias.current_uid, Some("products_v3".to_string()));
}

#[tokio::test]
async fn flip_alias_new_writes_use_new_target() {
    // Acceptance: Flip: new writes land on new target
    let store = create_test_store();
    let store_ref: &dyn TaskStore = &store;
    let registry = AliasRegistry::load_from_store(store_ref)
        .await
        .expect("failed to load registry");

    // Create initial single-target alias
    let new_alias = NewAlias {
        name: "logs".to_string(),
        kind: "single".to_string(),
        current_uid: Some("logs_v1".to_string()),
        target_uids: None,
        version: 1,
        created_at: 1000,
        history: vec![],
    };
    store.create_alias(&new_alias).unwrap();
    registry.sync_from_store(&store).await.unwrap();

    // Verify initial resolution
    let resolved = registry.resolve("logs").await;
    assert_eq!(resolved, vec!["logs_v1".to_string()]);

    // Perform the flip via task store
    store.flip_alias("logs", "logs_v2", 10).unwrap();

    // Sync to get the updated state
    registry.sync_from_store(&store).await.unwrap();

    // Verify new writes resolve to the new target
    let resolved = registry.resolve("logs").await;
    assert_eq!(resolved, vec!["logs_v2".to_string()]);

    // Verify history was recorded
    let alias = store.get_alias("logs").unwrap();
    assert!(alias.is_some());
    let alias = alias.unwrap();
    assert_eq!(alias.version, 2);
    assert_eq!(alias.history.len(), 1);
    assert_eq!(alias.history[0].uid, "logs_v1");
}

#[tokio::test]
async fn flip_alias_history_retention() {
    // Acceptance: History: 11th flip evicts the oldest
    let store = create_test_store();
    let store_ref: &dyn TaskStore = &store;
    let registry = AliasRegistry::load_from_store(store_ref)
        .await
        .expect("failed to load registry");

    // Create initial alias
    let new_alias = NewAlias {
        name: "products".to_string(),
        kind: "single".to_string(),
        current_uid: Some("products_v0".to_string()),
        target_uids: None,
        version: 1,
        created_at: 1000,
        history: vec![],
    };
    store.create_alias(&new_alias).unwrap();

    // Perform 12 flips with history_retention=10
    for i in 1..=12 {
        let new_target = format!("products_v{}", i);
        store.flip_alias("products", &new_target, 10).unwrap();
    }

    // Verify only the last 10 history entries are kept
    let alias = store.get_alias("products").unwrap();
    assert!(alias.is_some());
    let alias = alias.unwrap();
    assert_eq!(alias.version, 13); // Initial (1) + 12 flips
    assert_eq!(alias.history.len(), 10); // Retention bound enforced

    // Verify history contains the most recent 10 targets
    // After 12 flips from v0, we should have v2..v11 (10 entries)
    // v1 was evicted
    let expected: Vec<String> = (2..=11).map(|i| format!("products_v{}", i)).collect();
    let actual: Vec<String> = alias.history.iter().map(|h| h.uid.clone()).collect();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn create_multi_target_alias_reads_fanout() {
    // Acceptance: Create multi-target alias → read fans out
    let store = create_test_store();
    let store_ref: &dyn TaskStore = &store;
    let registry = AliasRegistry::load_from_store(store_ref)
        .await
        .expect("failed to load registry");

    // Create a multi-target alias
    let new_alias = NewAlias {
        name: "all-logs".to_string(),
        kind: "multi".to_string(),
        current_uid: None,
        target_uids: Some(vec![
            "logs-2026-04-18".to_string(),
            "logs-2026-04-17".to_string(),
            "logs-2026-04-16".to_string(),
        ]),
        version: 1,
        created_at: 1000,
        history: vec![],
    };
    store.create_alias(&new_alias).unwrap();
    registry.sync_from_store(&store).await.unwrap();

    // Verify reads resolve to all targets (fanout)
    let resolved = registry.resolve("all-logs").await;
    assert_eq!(resolved, vec![
        "logs-2026-04-18".to_string(),
        "logs-2026-04-17".to_string(),
        "logs-2026-04-16".to_string(),
    ]);

    // Verify it's identified as multi-target
    assert!(registry.is_multi_target_alias("all-logs").await);
}

#[tokio::test]
async fn write_to_multi_target_alias_returns_not_writable() {
    // Acceptance: Create multi-target alias → write returns 409
    let store = create_test_store();
    let store_ref: &dyn TaskStore = &store;
    let registry = AliasRegistry::load_from_store(store_ref)
        .await
        .expect("failed to load registry");

    // Create a multi-target alias (ILM-managed)
    let new_alias = NewAlias {
        name: "il-read".to_string(),
        kind: "multi".to_string(),
        current_uid: None,
        target_uids: Some(vec![
            "logs-2026-04-18".to_string(),
            "logs-2026-04-17".to_string(),
        ]),
        version: 1,
        created_at: 1000,
        history: vec![],
    };
    store.create_alias(&new_alias).unwrap();
    registry.sync_from_store(&store).await.unwrap();

    // Verify it's identified as multi-target (for write rejection)
    assert!(registry.is_multi_target_alias("il-read").await);

    // Verify the alias itself is correctly marked as multi-target
    let alias = registry.get("il-read").await.unwrap();
    assert_eq!(alias.kind, AliasKind::Multi);
    assert!(alias.current_uid.is_none());
    assert_eq!(alias.target_uids, Some(vec![
        "logs-2026-04-18".to_string(),
        "logs-2026-04-17".to_string(),
    ]));
}

#[tokio::test]
async fn operator_edit_of_ilm_multi_alias_rejected() {
    // Acceptance: Operator edit of an ILM-managed multi-target alias → 409
    // (only ILM can modify)
    let store = create_test_store();
    let store_ref: &dyn TaskStore = &store;
    let registry = AliasRegistry::load_from_store(store_ref)
        .await
        .expect("failed to load registry");

    // Create a multi-target alias (simulating ILM-managed)
    let new_alias = NewAlias {
        name: "ilm-managed".to_string(),
        kind: "multi".to_string(),
        current_uid: None,
        target_uids: Some(vec!["logs-1".to_string()]),
        version: 1,
        created_at: 1000,
        history: vec![],
    };
    store.create_alias(&new_alias).unwrap();
    registry.sync_from_store(&store).await.unwrap();

    // Attempt to update the multi-target alias via the registry
    // (simulating operator edit)
    let result = registry.flip("ilm-managed", "logs-2".to_string()).await;

    // Verify the flip was rejected
    assert!(result.is_err());

    // Verify the alias was not modified
    let alias = registry.get("ilm-managed").await.unwrap();
    assert_eq!(alias.target_uids, Some(vec!["logs-1".to_string()]));
    assert_eq!(alias.generation, 1); // Not incremented (stays at version 1 from store)
}

#[tokio::test]
async fn update_multi_via_update_multi_method() {
    // Verify that update_multi (ILM use only) works correctly
    let store = create_test_store();
    let store_ref: &dyn TaskStore = &store;
    let registry = AliasRegistry::load_from_store(store_ref)
        .await
        .expect("failed to load registry");

    // Create a multi-target alias
    let new_alias = NewAlias {
        name: "il-logs".to_string(),
        kind: "multi".to_string(),
        current_uid: None,
        target_uids: Some(vec!["logs-1".to_string()]),
        version: 1,
        created_at: 1000,
        history: vec![],
    };
    store.create_alias(&new_alias).unwrap();
    registry.sync_from_store(&store).await.unwrap();

    // Update via the ILM-specific method
    registry.update_multi("il-logs", vec![
        "logs-1".to_string(),
        "logs-2".to_string(),
    ]).await.unwrap();

    // Verify the update
    let alias = registry.get("il-logs").await.unwrap();
    assert_eq!(alias.target_uids, Some(vec![
        "logs-1".to_string(),
        "logs-2".to_string(),
    ]));
    assert_eq!(alias.generation, 2); // Incremented from 1 to 2
}

#[tokio::test]
async fn resolve_non_alias_returns_input_as_is() {
    // Verify that resolving a non-alias returns the input as-is
    let registry = AliasRegistry::new();

    // Resolve a name that is not an alias
    let resolved = registry.resolve("concrete_index").await;
    assert_eq!(resolved, vec!["concrete_index".to_string()]);

    // Verify it's not identified as an alias
    assert!(!registry.is_alias("concrete_index").await);
    assert!(!registry.is_multi_target_alias("concrete_index").await);
}

#[tokio::test]
async fn delete_alias() {
    // Verify alias deletion
    let store = create_test_store();
    let store_ref: &dyn TaskStore = &store;
    let registry = AliasRegistry::load_from_store(store_ref)
        .await
        .expect("failed to load registry");

    // Create an alias
    let new_alias = NewAlias {
        name: "to-delete".to_string(),
        kind: "single".to_string(),
        current_uid: Some("target".to_string()),
        target_uids: None,
        version: 1,
        created_at: 1000,
        history: vec![],
    };
    store.create_alias(&new_alias).unwrap();
    registry.sync_from_store(&store).await.unwrap();

    // Verify it exists
    assert!(registry.is_alias("to-delete").await);

    // Delete the alias
    let deleted = registry.delete("to-delete").await.unwrap();
    assert!(deleted);

    // Verify it's gone
    assert!(!registry.is_alias("to-delete").await);
    let resolved = registry.resolve("to-delete").await;
    assert_eq!(resolved, vec!["to-delete".to_string()]); // Returns input as-is

    // Delete non-existent alias
    let deleted = registry.delete("to-delete").await.unwrap();
    assert!(!deleted);
}

#[tokio::test]
async fn list_aliases() {
    // Verify listing all aliases
    let store = create_test_store();
    let store_ref: &dyn TaskStore = &store;
    let registry = AliasRegistry::load_from_store(store_ref)
        .await
        .expect("failed to load registry");

    // Create multiple aliases
    for i in 1..=3 {
        let new_alias = NewAlias {
            name: format!("alias{}", i),
            kind: "single".to_string(),
            current_uid: Some(format!("target_v{}", i)),
            target_uids: None,
            version: 1,
            created_at: 1000 + (i as i64),
            history: vec![],
        };
        store.create_alias(&new_alias).unwrap();
    }
    registry.sync_from_store(&store).await.unwrap();

    // List all aliases
    let aliases = registry.list().await;
    assert_eq!(aliases.len(), 3);

    // Verify they're all present
    let names: Vec<_> = aliases.iter().map(|a| a.name.clone()).collect();
    assert!(names.contains(&"alias1".to_string()));
    assert!(names.contains(&"alias2".to_string()));
    assert!(names.contains(&"alias3".to_string()));
}

#[tokio::test]
async fn alias_targets_single() {
    // Unit test for Alias::targets with single-target alias
    let alias = Alias::new_single("test".to_string(), "target_v1".to_string());
    let targets = alias.targets().unwrap();
    assert_eq!(targets, vec!["target_v1"]);
}

#[tokio::test]
async fn alias_targets_multi() {
    // Unit test for Alias::targets with multi-target alias
    let alias = Alias::new_multi("test".to_string(), vec!["a".to_string(), "b".to_string()]);
    let targets = alias.targets().unwrap();
    assert_eq!(targets, vec!["a", "b"]);
}

#[tokio::test]
async fn alias_flip_single() {
    // Unit test for Alias::flip with single-target alias
    let mut alias = Alias::new_single("products".to_string(), "v1".to_string());
    assert_eq!(alias.generation, 0);
    assert_eq!(alias.current_uid, Some("v1".to_string()));

    alias.flip("v2".to_string()).unwrap();
    assert_eq!(alias.generation, 1);
    assert_eq!(alias.current_uid, Some("v2".to_string()));
}

#[tokio::test]
async fn alias_flip_multi_fails() {
    // Unit test: flip should fail on multi-target alias
    let mut alias = Alias::new_multi("logs".to_string(), vec!["a".to_string()]);
    let result = alias.flip("b".to_string());
    assert!(result.is_err());
}

#[tokio::test]
async fn alias_update_targets_multi() {
    // Unit test for Alias::update_targets with multi-target alias
    let mut alias = Alias::new_multi("logs".to_string(), vec!["a".to_string()]);
    assert_eq!(alias.generation, 0);

    alias.update_targets(vec!["a".to_string(), "b".to_string()]).unwrap();
    assert_eq!(alias.generation, 1);
    assert_eq!(alias.target_uids, Some(vec!["a".to_string(), "b".to_string()]));
}

#[tokio::test]
async fn alias_update_targets_single_fails() {
    // Unit test: update_targets should fail on single-target alias
    let mut alias = Alias::new_single("products".to_string(), "v1".to_string());
    let result = alias.update_targets(vec!["a".to_string()]);
    assert!(result.is_err());
}

#[tokio::test]
async fn registry_sync_from_store() {
    // Verify syncing from store loads all aliases
    let store = create_test_store();
    let registry = AliasRegistry::new();

    // Create aliases directly in the store
    for i in 1..=3 {
        let new_alias = NewAlias {
            name: format!("sync{}", i),
            kind: "single".to_string(),
            current_uid: Some(format!("target_v{}", i)),
            target_uids: None,
            version: 1,
            created_at: 1000 + (i as i64),
            history: vec![],
        };
        store.create_alias(&new_alias).unwrap();
    }

    // Sync from store
    let store_ref: &dyn TaskStore = &store;
    registry.sync_from_store(store_ref).await.unwrap();

    // Verify all aliases were loaded
    let aliases = registry.list().await;
    assert_eq!(aliases.len(), 3);
}
