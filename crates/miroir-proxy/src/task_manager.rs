//! Task ID generation and reconciliation per plan §3.
//!
//! - Generates unique Miroir task IDs
//! - Tracks node task UIDs for reconciliation
//! - Aggregates task status across nodes

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

/// Task manager for generating and tracking tasks.
#[derive(Clone)]
pub struct TaskManager {
    /// Next task UID (sequential for Meilisearch compatibility)
    next_uid: Arc<AtomicU64>,
}

impl TaskManager {
    /// Create a new task manager.
    pub fn new() -> Self {
        Self {
            next_uid: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Generate a new task UID.
    pub fn next_uid(&self) -> u64 {
        self.next_uid.fetch_add(1, Ordering::SeqCst)
    }

    /// Generate a unique Miroir task ID (UUID-based).
    pub fn generate_miroir_task_id(&self) -> String {
        format!("mtask-{}", Uuid::new_v4())
    }
}

impl Default for TaskManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Task reconciliation state for tracking responses from nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskReconciliation {
    /// Miroir task ID.
    pub miroir_task_id: String,
    /// Task UID for client responses.
    pub task_uid: u64,
    /// Node task UIDs keyed by node ID.
    pub node_tasks: HashMap<String, u64>,
    /// Which groups met quorum.
    pub successful_groups: Vec<u32>,
    /// Which groups missed quorum.
    pub degraded_groups: Vec<u32>,
}

impl TaskReconciliation {
    /// Create a new task reconciliation state.
    pub fn new(miroir_task_id: String, task_uid: u64) -> Self {
        Self {
            miroir_task_id,
            task_uid,
            node_tasks: HashMap::new(),
            successful_groups: Vec::new(),
            degraded_groups: Vec::new(),
        }
    }

    /// Add a node task response.
    pub fn add_node_task(&mut self, node_id: String, task_uid: u64) {
        self.node_tasks.insert(node_id, task_uid);
    }

    /// Mark a group as successful (met quorum).
    pub fn mark_group_success(&mut self, group_id: u32) {
        if !self.successful_groups.contains(&group_id) {
            self.successful_groups.push(group_id);
        }
    }

    /// Mark a group as degraded (missed quorum).
    pub fn mark_group_degraded(&mut self, group_id: u32) {
        if !self.degraded_groups.contains(&group_id) {
            self.degraded_groups.push(group_id);
        }
    }

    /// Check if the task is degraded (any group missed quorum).
    pub fn is_degraded(&self) -> bool {
        !self.degraded_groups.is_empty()
    }

    /// Check if the task succeeded completely (all groups met quorum).
    pub fn is_full_success(&self) -> bool {
        self.degraded_groups.is_empty() && !self.successful_groups.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_manager_uid_generation() {
        let manager = TaskManager::new();
        let uid1 = manager.next_uid();
        let uid2 = manager.next_uid();

        assert_eq!(uid1, 1);
        assert_eq!(uid2, 2);
    }

    #[test]
    fn test_task_manager_miroir_id_generation() {
        let manager = TaskManager::new();
        let id1 = manager.generate_miroir_task_id();
        let id2 = manager.generate_miroir_task_id();

        assert!(id1.starts_with("mtask-"));
        assert!(id2.starts_with("mtask-"));
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_task_reconciliation() {
        let mut reconciliation = TaskReconciliation::new("mtask-123".to_string(), 42);

        reconciliation.add_node_task("node1".to_string(), 100);
        reconciliation.add_node_task("node2".to_string(), 101);

        assert_eq!(reconciliation.node_tasks.len(), 2);
        assert_eq!(reconciliation.node_tasks.get("node1"), Some(&100));
        assert_eq!(reconciliation.node_tasks.get("node2"), Some(&101));
    }

    #[test]
    fn test_task_reconciliation_groups() {
        let mut reconciliation = TaskReconciliation::new("mtask-123".to_string(), 42);

        reconciliation.mark_group_success(0);
        reconciliation.mark_group_success(1);
        reconciliation.mark_group_degraded(2);

        assert_eq!(reconciliation.successful_groups, vec![0, 1]);
        assert_eq!(reconciliation.degraded_groups, vec![2]);
        assert!(reconciliation.is_degraded());
    }

    #[test]
    fn test_task_reconciliation_full_success() {
        let mut reconciliation = TaskReconciliation::new("mtask-123".to_string(), 42);

        reconciliation.mark_group_success(0);
        reconciliation.mark_group_success(1);

        assert!(reconciliation.is_full_success());
        assert!(!reconciliation.is_degraded());
    }

    #[test]
    fn test_task_manager_default() {
        let manager = TaskManager::default();
        let uid = manager.next_uid();

        assert_eq!(uid, 1);
    }
}
