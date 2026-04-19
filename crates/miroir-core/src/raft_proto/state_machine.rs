//! In-memory state machine for the Raft-backed task store.
//!
//! This is the core of the Raft prototype: a deterministic state machine that
//! applies commands in Raft log order. Every replica applies the same commands
//! in the same order, converging to identical state.
//!
//! In a full implementation, this would implement openraft's `RaftStateMachine`
//! trait with `apply()`, `get_snapshot_builder()`, `install_snapshot()`, etc.
//! The state would be persisted to SQLite tables (plan §4). For benchmarking,
//! we use an in-memory HashMap to measure pure apply logic without I/O.

use crate::task::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::command::TaskStoreCommand;

/// Response from applying a command to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResponse {
    pub miroir_id: Option<String>,
    pub success: bool,
}

/// Snapshot data for Raft state transfer.
/// In production, this would be a serialized SQLite database.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Snapshot {
    pub tasks: HashMap<String, MiroirTask>,
    pub last_applied_log_index: u64,
}

/// In-memory task store state machine.
///
/// This is the "apply" side of the Raft state machine. Commands arrive in
/// strict log order; the machine applies them deterministically.
///
/// In openraft, this would implement `RaftStateMachine<MiroirRaft>`:
/// ```ignore
/// impl RaftStateMachine<MiroirRaft> for TaskStateMachine {
///     async fn apply(&mut self, entries: Vec<Entry<MiroirRaft>>) -> Vec<CommandResponse> { ... }
///     async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder { ... }
///     async fn install_snapshot(&mut self, meta: &SnapshotMeta, snapshot: Snapshot) -> ... { ... }
/// }
/// ```
pub struct TaskStateMachine {
    tasks: HashMap<String, MiroirTask>,
    last_applied_log_index: u64,
}

impl TaskStateMachine {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            last_applied_log_index: 0,
        }
    }

    /// Apply a command to the state machine. Must be deterministic.
    ///
    /// This is the performance-critical path. Every Raft-committed entry
    /// goes through here. The benchmark measures this method's latency.
    pub fn apply(&mut self, cmd: TaskStoreCommand) -> CommandResponse {
        self.last_applied_log_index += 1;

        match cmd {
            TaskStoreCommand::InsertTask { node_tasks } => {
                let miroir_id = uuid::Uuid::new_v4().to_string();
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                let task = MiroirTask {
                    miroir_id: miroir_id.clone(),
                    created_at: now,
                    status: TaskStatus::Enqueued,
                    node_tasks: node_tasks
                        .into_iter()
                        .map(|(node_id, uid)| {
                            (
                                node_id,
                                NodeTask {
                                    task_uid: uid,
                                    status: NodeTaskStatus::Enqueued,
                                },
                            )
                        })
                        .collect(),
                    error: None,
                };

                self.tasks.insert(miroir_id.clone(), task);
                CommandResponse {
                    miroir_id: Some(miroir_id),
                    success: true,
                }
            }

            TaskStoreCommand::UpdateTaskStatus { miroir_id, status } => {
                if let Some(task) = self.tasks.get_mut(&miroir_id) {
                    task.status = status;
                    CommandResponse {
                        miroir_id: Some(miroir_id),
                        success: true,
                    }
                } else {
                    CommandResponse {
                        miroir_id: Some(miroir_id),
                        success: false,
                    }
                }
            }

            TaskStoreCommand::UpdateNodeTask {
                miroir_id,
                node_id,
                node_status,
            } => {
                if let Some(task) = self.tasks.get_mut(&miroir_id) {
                    if let Some(nt) = task.node_tasks.get_mut(&node_id) {
                        nt.status = node_status;
                    }
                    // Auto-complete: if all node tasks are done, mark task as done
                    let all_done = task.node_tasks.values().all(|nt| {
                        matches!(
                            nt.status,
                            NodeTaskStatus::Succeeded | NodeTaskStatus::Failed
                        )
                    });
                    if all_done {
                        let any_failed = task
                            .node_tasks
                            .values()
                            .any(|nt| matches!(nt.status, NodeTaskStatus::Failed));
                        task.status = if any_failed {
                            TaskStatus::Failed
                        } else {
                            TaskStatus::Succeeded
                        };
                    }
                    CommandResponse {
                        miroir_id: Some(miroir_id),
                        success: true,
                    }
                } else {
                    CommandResponse {
                        miroir_id: Some(miroir_id),
                        success: false,
                    }
                }
            }

            TaskStoreCommand::SetTaskError { miroir_id, error } => {
                if let Some(task) = self.tasks.get_mut(&miroir_id) {
                    task.error = Some(error);
                    CommandResponse {
                        miroir_id: Some(miroir_id),
                        success: true,
                    }
                } else {
                    CommandResponse {
                        miroir_id: Some(miroir_id),
                        success: false,
                    }
                }
            }

            TaskStoreCommand::DeleteTask { miroir_id } => {
                self.tasks.remove(&miroir_id);
                CommandResponse {
                    miroir_id: Some(miroir_id),
                    success: true,
                }
            }
        }
    }

    pub fn get_task(&self, miroir_id: &str) -> Option<&MiroirTask> {
        self.tasks.get(miroir_id)
    }

    pub fn last_task(&self) -> Option<&MiroirTask> {
        self.tasks.values().last()
    }

    pub fn list_tasks(&self, filter: &TaskFilter) -> Vec<MiroirTask> {
        let mut tasks: Vec<&MiroirTask> = self
            .tasks
            .values()
            .filter(|t| {
                if let Some(status) = &filter.status {
                    if t.status != *status {
                        return false;
                    }
                }
                if let Some(node_id) = &filter.node_id {
                    if !t.node_tasks.contains_key(node_id) {
                        return false;
                    }
                }
                true
            })
            .collect();

        tasks.sort_by_key(|t| t.created_at);

        let offset = filter.offset.unwrap_or(0);
        let limit = filter.limit.unwrap_or(usize::MAX);

        tasks
            .into_iter()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            tasks: self.tasks.clone(),
            last_applied_log_index: self.last_applied_log_index,
        }
    }

    /// Restore from a snapshot (for Raft state transfer).
    pub fn restore(&mut self, snapshot: Snapshot) {
        self.tasks = snapshot.tasks;
        self.last_applied_log_index = snapshot.last_applied_log_index;
    }
}
