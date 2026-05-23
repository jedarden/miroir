//! Atomic index aliases for blue-green reindexing (plan §13.7).
//!
//! This module implements the alias layer that allows atomic index flips
//! without downtime. Aliases resolve to one or more concrete Meilisearch
//! index UIDs, supporting both single-target (writable) and multi-target
//! (read-only, used by ILM) aliases.

use crate::error::{MiroirError, Result};
use crate::task_store::{AliasRow, AliasHistoryEntry, TaskStore};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn, error};

/// Alias kind: single-target (writable) or multi-target (read-only, ILM-managed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AliasKind {
    Single,
    Multi,
}

/// A single alias record from the task store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alias {
    /// Alias name (the key clients use).
    pub name: String,
    /// `single` or `multi`.
    pub kind: AliasKind,
    /// Current target UID (only set when kind=single).
    pub current_uid: Option<String>,
    /// Target UIDs as JSON array (only set when kind=multi).
    pub target_uids: Option<Vec<String>>,
    /// Generation incremented on each flip.
    pub generation: u64,
    /// Created at timestamp.
    pub created_at: u64,
    /// Last updated timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<u64>,
}

impl Alias {
    /// Create a new single-target alias.
    pub fn new_single(name: String, target_uid: String) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            name,
            kind: AliasKind::Single,
            current_uid: Some(target_uid),
            target_uids: None,
            generation: 0,
            created_at: now,
            updated_at: Some(now),
        }
    }

    /// Create a new multi-target alias.
    pub fn new_multi(name: String, target_uids: Vec<String>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            name,
            kind: AliasKind::Multi,
            current_uid: None,
            target_uids: Some(target_uids),
            generation: 0,
            created_at: now,
            updated_at: Some(now),
        }
    }

    /// Check if this alias is multi-target (read-only, ILM-managed).
    pub fn is_multi_target(&self) -> bool {
        matches!(self.kind, AliasKind::Multi)
    }

    /// Get the effective target UIDs for this alias.
    pub fn targets(&self) -> Result<Vec<String>> {
        match self.kind {
            AliasKind::Single => {
                let uid = self.current_uid.as_ref()
                    .ok_or_else(|| MiroirError::InvalidState("single alias missing current_uid".into()))?;
                Ok(vec![uid.clone()])
            }
            AliasKind::Multi => {
                let uids = self.target_uids.as_ref()
                    .ok_or_else(|| MiroirError::InvalidState("multi alias missing target_uids".into()))?;
                Ok(uids.clone())
            }
        }
    }

    /// Flip this alias to a new target (single-target only).
    pub fn flip(&mut self, new_target: String) -> Result<()> {
        if self.kind != AliasKind::Single {
            return Err(MiroirError::InvalidState("cannot flip multi-target alias".into()));
        }
        self.current_uid = Some(new_target);
        self.generation += 1;
        self.updated_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs());
        Ok(())
    }

    /// Update multi-target alias UIDs (ILM-only).
    pub fn update_targets(&mut self, new_targets: Vec<String>) -> Result<()> {
        if self.kind != AliasKind::Multi {
            return Err(MiroirError::InvalidState("cannot update_targets on single-target alias".into()));
        }
        self.target_uids = Some(new_targets);
        self.generation += 1;
        self.updated_at = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs());
        Ok(())
    }
}

/// In-memory alias registry with task-store persistence.
#[derive(Clone)]
pub struct AliasRegistry {
    aliases: Arc<RwLock<HashMap<String, Alias>>>,
}

impl AliasRegistry {
    /// Create a new empty alias registry.
    pub fn new() -> Self {
        Self {
            aliases: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new alias registry and load from task store.
    pub async fn load_from_store(task_store: &dyn TaskStore) -> Result<Self> {
        let registry = Self::new();
        registry.sync_from_store(task_store).await?;
        Ok(registry)
    }

    /// Sync aliases from the task store into memory.
    pub async fn sync_from_store(&self, task_store: &dyn TaskStore) -> Result<()> {
        let rows = task_store.list_aliases()?;
        let mut aliases = self.aliases.write().await;

        // Clear and reload from store
        aliases.clear();
        for row in rows {
            let alias = Alias {
                name: row.name.clone(),
                kind: match row.kind.as_str() {
                    "single" => AliasKind::Single,
                    "multi" => AliasKind::Multi,
                    _ => return Err(MiroirError::InvalidState(format!("invalid alias kind: {}", row.kind))),
                },
                current_uid: row.current_uid,
                target_uids: row.target_uids,
                generation: row.version as u64,
                created_at: row.created_at as u64,
                updated_at: None, // Task store doesn't track updated_at separately
            };
            aliases.insert(row.name, alias);
        }

        info!("loaded {} aliases from task store", aliases.len());
        Ok(())
    }

    /// Resolve an index UID or alias name to concrete target UIDs.
    ///
    /// If `input` is not a known alias, returns it as-is (treat as concrete UID).
    pub async fn resolve(&self, input: &str) -> Vec<String> {
        let aliases = self.aliases.read().await;
        match aliases.get(input) {
            Some(alias) => alias.targets().unwrap_or_else(|_| vec![input.to_string()]),
            None => vec![input.to_string()],
        }
    }

    /// Check if an input is an alias (vs a concrete UID).
    pub async fn is_alias(&self, input: &str) -> bool {
        self.aliases.read().await.contains_key(input)
    }

    /// Check if an input is a multi-target alias (for write rejection).
    pub async fn is_multi_target_alias(&self, input: &str) -> bool {
        self.aliases.read().await
            .get(input)
            .map(|a| a.is_multi_target())
            .unwrap_or(false)
    }

    /// Get a single alias by name.
    pub async fn get(&self, name: &str) -> Option<Alias> {
        self.aliases.read().await.get(name).cloned()
    }

    /// List all aliases.
    pub async fn list(&self) -> Vec<Alias> {
        self.aliases.read().await.values().cloned().collect()
    }

    /// Create or update an alias.
    pub async fn upsert(&self, alias: Alias) -> Result<()> {
        let mut aliases = self.aliases.write().await;
        aliases.insert(alias.name.clone(), alias);
        Ok(())
    }

    /// Delete an alias.
    pub async fn delete(&self, name: &str) -> Result<bool> {
        let mut aliases = self.aliases.write().await;
        Ok(aliases.remove(name).is_some())
    }

    /// Flip a single-target alias atomically.
    pub async fn flip(&self, name: &str, new_target: String) -> Result<()> {
        let mut aliases = self.aliases.write().await;
        let alias = aliases.get_mut(name)
            .ok_or_else(|| MiroirError::NotFound(format!("alias '{}' not found", name)))?;
        alias.flip(new_target)?;
        Ok(())
    }

    /// Update a multi-target alias (ILM use only).
    pub async fn update_multi(&self, name: &str, new_targets: Vec<String>) -> Result<()> {
        let mut aliases = self.aliases.write().await;
        let alias = aliases.get_mut(name)
            .ok_or_else(|| MiroirError::NotFound(format!("alias '{}' not found", name)))?;
        alias.update_targets(new_targets)?;
        Ok(())
    }
}

impl Default for AliasRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_single_alias() {
        let alias = Alias::new_single("products".into(), "products_v3".into());
        assert_eq!(alias.name, "products");
        assert_eq!(alias.kind, AliasKind::Single);
        assert_eq!(alias.current_uid, Some("products_v3".into()));
        assert_eq!(alias.generation, 0);
    }

    #[test]
    fn test_new_multi_alias() {
        let alias = Alias::new_multi("logs".into(), vec!["logs-20260418".into(), "logs-20260417".into()]);
        assert_eq!(alias.name, "logs");
        assert_eq!(alias.kind, AliasKind::Multi);
        assert_eq!(alias.target_uids, Some(vec!["logs-20260418".into(), "logs-20260417".into()]));
    }

    #[test]
    fn test_alias_targets_single() {
        let alias = Alias::new_single("test".into(), "target_v1".into());
        assert_eq!(alias.targets().unwrap(), vec!["target_v1"]);
    }

    #[test]
    fn test_alias_targets_multi() {
        let alias = Alias::new_multi("test".into(), vec!["a".into(), "b".into()]);
        assert_eq!(alias.targets().unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn test_alias_flip() {
        let mut alias = Alias::new_single("products".into(), "products_v3".into());
        assert_eq!(alias.generation, 0);
        alias.flip("products_v4".into()).unwrap();
        assert_eq!(alias.current_uid, Some("products_v4".into()));
        assert_eq!(alias.generation, 1);
    }

    #[test]
    fn test_alias_flip_multi_fails() {
        let mut alias = Alias::new_multi("logs".into(), vec!["a".into()]);
        assert!(alias.flip("b".into()).is_err());
    }

    #[tokio::test]
    async fn test_registry_resolve_unknown() {
        let registry = AliasRegistry::new();
        assert_eq!(registry.resolve("concrete_index").await, vec!["concrete_index"]);
    }

    #[tokio::test]
    async fn test_registry_resolve_alias() {
        let registry = AliasRegistry::new();
        let alias = Alias::new_single("products".into(), "products_v3".into());
        registry.upsert(alias).await.unwrap();
        assert_eq!(registry.resolve("products").await, vec!["products_v3"]);
    }

    #[tokio::test]
    async fn test_registry_flip() {
        let registry = AliasRegistry::new();
        let alias = Alias::new_single("products".into(), "products_v3".into());
        registry.upsert(alias).await.unwrap();
        registry.flip("products", "products_v4".into()).await.unwrap();
        assert_eq!(registry.resolve("products").await, vec!["products_v4"]);
    }

    #[tokio::test]
    async fn test_registry_delete() {
        let registry = AliasRegistry::new();
        let alias = Alias::new_single("products".into(), "products_v3".into());
        registry.upsert(alias).await.unwrap();
        assert!(registry.delete("products").await.unwrap());
        assert!(!registry.delete("products").await.unwrap());
        assert!(!registry.is_alias("products").await);
    }

    #[tokio::test]
    async fn test_multi_alias_update() {
        let registry = AliasRegistry::new();
        let alias = Alias::new_multi("logs".into(), vec!["logs-1".into()]);
        registry.upsert(alias).await.unwrap();
        registry.update_multi("logs", vec!["logs-1".into(), "logs-2".into()]).await.unwrap();
        assert_eq!(registry.resolve("logs").await, vec!["logs-1", "logs-2"]);
    }
}
