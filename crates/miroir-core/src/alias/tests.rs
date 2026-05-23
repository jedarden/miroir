//! Unit tests for the alias module.

use super::*;

#[test]
fn test_alias_kind_display() {
    assert_eq!(serde_json::to_string(&AliasKind::Single).unwrap(), r#""single""#);
    assert_eq!(serde_json::to_string(&AliasKind::Multi).unwrap(), r#""multi""#);
}

#[test]
fn test_alias_new_single() {
    let alias = Alias::new_single("my-alias".to_string(), "index-1".to_string());
    assert_eq!(alias.name, "my-alias");
    assert_eq!(alias.kind, AliasKind::Single);
    assert_eq!(alias.current_uid, Some("index-1".to_string()));
    assert!(alias.target_uids.is_none());
    assert_eq!(alias.generation, 0);
}

#[test]
fn test_alias_new_multi() {
    let alias = Alias::new_multi("my-alias".to_string(), vec!["index-1".to_string(), "index-2".to_string()]);
    assert_eq!(alias.name, "my-alias");
    assert_eq!(alias.kind, AliasKind::Multi);
    assert!(alias.current_uid.is_none());
    assert_eq!(alias.target_uids, Some(vec!["index-1".to_string(), "index-2".to_string()]));
    assert_eq!(alias.generation, 0);
}

#[test]
fn test_alias_is_multi_target() {
    let single = Alias::new_single("test".to_string(), "idx".to_string());
    assert!(!single.is_multi_target());

    let multi = Alias::new_multi("test".to_string(), vec!["idx1".to_string(), "idx2".to_string()]);
    assert!(multi.is_multi_target());
}

#[test]
fn test_alias_targets_single() {
    let alias = Alias::new_single("test".to_string(), "idx1".to_string());
    let targets = alias.targets().unwrap();
    assert_eq!(targets, vec!["idx1".to_string()]);
}

#[test]
fn test_alias_targets_multi() {
    let alias = Alias::new_multi("test".to_string(), vec!["idx1".to_string(), "idx2".to_string()]);
    let targets = alias.targets().unwrap();
    assert_eq!(targets, vec!["idx1".to_string(), "idx2".to_string()]);
}

#[test]
fn test_alias_flip() {
    let mut alias = Alias::new_single("test".to_string(), "idx1".to_string());
    assert_eq!(alias.generation, 0);

    alias.flip("idx2".to_string()).unwrap();
    assert_eq!(alias.current_uid, Some("idx2".to_string()));
    assert_eq!(alias.generation, 1);
}

#[test]
fn test_alias_flip_multi_fails() {
    let mut alias = Alias::new_multi("test".to_string(), vec!["idx1".to_string()]);
    let result = alias.flip("idx2".to_string());
    assert!(result.is_err());
}

#[test]
fn test_alias_update_targets() {
    let mut alias = Alias::new_multi("test".to_string(), vec!["idx1".to_string()]);
    alias.update_targets(vec!["idx2".to_string(), "idx3".to_string()]).unwrap();
    assert_eq!(alias.target_uids, Some(vec!["idx2".to_string(), "idx3".to_string()]));
    assert_eq!(alias.generation, 1);
}

#[tokio::test]
async fn test_alias_registry_default() {
    let registry = AliasRegistry::default();
    assert!(!registry.is_alias("test").await);
}

#[tokio::test]
async fn test_alias_registry_resolve_unknown() {
    let registry = AliasRegistry::new();
    let targets = registry.resolve("concrete-index").await;
    assert_eq!(targets, vec!["concrete-index".to_string()]);
}

#[tokio::test]
async fn test_alias_registry_upsert_and_get() {
    let registry = AliasRegistry::new();
    let alias = Alias::new_single("test".to_string(), "idx1".to_string());
    registry.upsert(alias).await.unwrap();

    let retrieved = registry.get("test").await.unwrap();
    assert_eq!(retrieved.name, "test");
    assert_eq!(retrieved.current_uid, Some("idx1".to_string()));
}

#[tokio::test]
async fn test_alias_registry_delete() {
    let registry = AliasRegistry::new();
    let alias = Alias::new_single("test".to_string(), "idx1".to_string());
    registry.upsert(alias).await.unwrap();

    assert!(registry.delete("test").await.unwrap());
    assert!(!registry.delete("test").await.unwrap());
}

#[tokio::test]
async fn test_alias_registry_flip() {
    let registry = AliasRegistry::new();
    let alias = Alias::new_single("test".to_string(), "idx1".to_string());
    registry.upsert(alias).await.unwrap();

    registry.flip("test", "idx2".to_string()).await.unwrap();

    let retrieved = registry.get("test").await.unwrap();
    assert_eq!(retrieved.current_uid, Some("idx2".to_string()));
    assert_eq!(retrieved.generation, 1);
}
