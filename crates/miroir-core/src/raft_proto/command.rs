//! Raft log command types for the task store state machine.
//!
//! Every mutating `TaskRegistry` operation is serialized as one of these commands
//! and replicated through Raft consensus before being applied to the state machine.
//!
//! In a full implementation backed by openraft, each variant maps to an
//! `openraft::EntryPayload::Normal(TaskStoreCommand)` log entry. The state machine's
//! `apply()` method deserializes and executes each command in log order.

use crate::task::{NodeTaskStatus, TaskStatus};
use serde::{Deserialize, Serialize};

/// A command that mutates the task store. Serialized into Raft log entries.
///
/// This is the minimal set for the prototype. The full implementation would have
/// ~20 variants covering all 14 tables from plan §4:
/// tasks, task_events, aliases, alias_history, index_settings, sessions,
/// leader_lease, jobs, job_steps, idempotency_cache, query_coalescing,
/// rate_limits, tenants, migration_state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskStoreCommand {
    // -- tasks table --
    /// Insert a new task with per-node Meilisearch task UIDs.
    InsertTask { node_tasks: Vec<(String, u64)> },

    /// Update a task's overall status.
    UpdateTaskStatus {
        miroir_id: String,
        status: TaskStatus,
    },

    /// Update a specific node's task status within a Miroir task.
    UpdateNodeTask {
        miroir_id: String,
        node_id: String,
        node_status: NodeTaskStatus,
    },

    /// Record an error on a failed task.
    SetTaskError { miroir_id: String, error: String },

    /// Delete a task (gc/cleanup).
    DeleteTask { miroir_id: String },
}
