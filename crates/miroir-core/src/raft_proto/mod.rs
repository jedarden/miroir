//! Research prototype: Raft-backed TaskRegistry architecture.
//!
//! This module is a **research artifact** for P12.OP2 (plan §15 Open Problem #2).
//! It demonstrates the architecture for replacing Redis with embedded Raft consensus
//! for task state replication across Miroir pods.
//!
//! **Not for production use.** Decision per `docs/research/raft-task-store.md`:
//! "revisit before v2.0, do not ship in v0.x or v1.0."
//!
//! ## Why self-contained instead of depending on openraft
//!
//! openraft 0.9.20 depends on `validit 0.2.5` which uses `let_chains` — an unstable
//! Rust feature not available on stable 1.87. This compilation failure is itself
//! a data point against Raft in the near term. The prototype simulates the Raft
//! architecture to benchmark the state machine apply path, which is the performance-
//! critical component.

pub mod benchmark;
pub mod command;
pub mod state_machine;

use crate::task::*;
use crate::Result;
use command::TaskStoreCommand;
use state_machine::TaskStateMachine;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Simulated Raft consensus overhead.
///
/// In a real Raft cluster, every write goes through:
/// 1. Serialize command → log entry
/// 2. Send to majority of peers (network RTT)
/// 3. Each peer persists to disk (fsync)
/// 4. Majority ACK → leader commits
/// 5. Apply to state machine
///
/// The network + fsync dominates. This constant represents the consensus overhead
/// based on published openraft benchmarks and typical K8s pod-to-pod latency.
#[allow(dead_code)]
const RAFT_CONSENSUS_OVERHEAD: Duration = Duration::from_micros(2500); // 2.5ms median

/// Redis network overhead (same cluster, pod-to-pod).
#[allow(dead_code)]
const REDIS_NETWORK_OVERHEAD: Duration = Duration::from_micros(500); // 0.5ms median

/// Raft-backed implementation of TaskRegistry.
///
/// Architecture:
/// - **Writes**: serialized as `TaskStoreCommand`, proposed to Raft cluster,
///   replicated to majority, then applied to local state machine.
/// - **Reads**: served from local state machine (eventual consistency).
///   Linearizable reads available via Raft's `read_index` if needed.
///
/// This impl bridges the sync `TaskRegistry` trait with the async Raft operations
/// that would happen in production. The state machine is the real code; the Raft
/// consensus layer is simulated for benchmarking purposes.
pub struct RaftTaskRegistry {
    state_machine: Arc<std::sync::Mutex<TaskStateMachine>>,
}

impl Default for RaftTaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftTaskRegistry {
    pub fn new() -> Self {
        Self {
            state_machine: Arc::new(std::sync::Mutex::new(TaskStateMachine::new())),
        }
    }

    /// Simulated Raft write: consensus overhead + state machine apply.
    /// Returns the apply latency (state machine only, consensus not measured here).
    pub fn write_with_consensus(
        &self,
        cmd: TaskStoreCommand,
    ) -> (Duration, state_machine::CommandResponse) {
        let start = std::time::Instant::now();
        let mut sm = self.state_machine.lock().unwrap();
        let resp = sm.apply(cmd);
        let apply_latency = start.elapsed();
        // In reality, total write = RAFT_CONSENSUS_OVERHEAD + apply_latency
        (apply_latency, resp)
    }
}

impl TaskRegistry for RaftTaskRegistry {
    fn register_with_metadata(
        &self,
        node_tasks: HashMap<String, u64>,
        index_uid: Option<String>,
        task_type: Option<String>,
    ) -> Result<MiroirTask> {
        let cmd = TaskStoreCommand::InsertTask {
            node_tasks: node_tasks.into_iter().collect(),
            index_uid,
            task_type,
        };
        let (_, resp) = self.write_with_consensus(cmd);
        let sm = self.state_machine.lock().unwrap();
        sm.get_task(resp.miroir_id.as_deref().unwrap())
            .cloned()
            .ok_or_else(|| crate::MiroirError::Task("task not found after insert".into()))
    }

    fn get(&self, miroir_id: &str) -> Result<Option<MiroirTask>> {
        let sm = self.state_machine.lock().unwrap();
        Ok(sm.get_task(miroir_id).cloned())
    }

    fn update_status(&self, miroir_id: &str, status: TaskStatus) -> Result<()> {
        let cmd = TaskStoreCommand::UpdateTaskStatus {
            miroir_id: miroir_id.to_string(),
            status,
        };
        self.write_with_consensus(cmd);
        Ok(())
    }

    fn update_node_task(
        &self,
        miroir_id: &str,
        node_id: &str,
        node_status: NodeTaskStatus,
    ) -> Result<()> {
        let cmd = TaskStoreCommand::UpdateNodeTask {
            miroir_id: miroir_id.to_string(),
            node_id: node_id.to_string(),
            node_status,
        };
        self.write_with_consensus(cmd);
        Ok(())
    }

    fn list(&self, filter: TaskFilter) -> Result<Vec<MiroirTask>> {
        let sm = self.state_machine.lock().unwrap();
        Ok(sm.list_tasks(&filter))
    }

    fn count(&self) -> usize {
        let sm = self.state_machine.lock().unwrap();
        sm.task_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_machine_insert_and_get() {
        let reg = RaftTaskRegistry::new();

        let node_tasks = vec![("node-1".to_string(), 42u64), ("node-2".to_string(), 43u64)]
            .into_iter()
            .collect();
        let task = reg.register(node_tasks).unwrap();

        assert_eq!(task.node_tasks.len(), 2);
        assert_eq!(task.node_tasks["node-1"].task_uid, 42);
        assert_eq!(task.status, TaskStatus::Enqueued);
    }

    #[test]
    fn state_machine_update_status() {
        let reg = RaftTaskRegistry::new();

        let node_tasks = vec![("node-1".to_string(), 1u64)].into_iter().collect();
        let task = reg.register(node_tasks).unwrap();
        let miroir_id = task.miroir_id.clone();

        reg.update_status(&miroir_id, TaskStatus::Processing)
            .unwrap();

        let updated = reg.get(&miroir_id).unwrap().unwrap();
        assert_eq!(updated.status, TaskStatus::Processing);
    }

    #[test]
    fn state_machine_list_with_filter() {
        let reg = RaftTaskRegistry::new();

        for i in 0..5 {
            let node_tasks = vec![("node-1".to_string(), i as u64)].into_iter().collect();
            reg.register(node_tasks).unwrap();
        }

        let all = reg.list(TaskFilter::default()).unwrap();
        assert_eq!(all.len(), 5);

        let limited = reg
            .list(TaskFilter {
                limit: Some(2),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn auto_complete_on_all_nodes_done() {
        let reg = RaftTaskRegistry::new();

        let node_tasks = vec![("node-1".to_string(), 1u64), ("node-2".to_string(), 2u64)]
            .into_iter()
            .collect();
        let task = reg.register(node_tasks).unwrap();
        let miroir_id = task.miroir_id.clone();

        reg.update_node_task(&miroir_id, "node-1", NodeTaskStatus::Succeeded)
            .unwrap();
        let mid = reg.get(&miroir_id).unwrap().unwrap();
        assert_eq!(mid.status, TaskStatus::Enqueued); // not all done yet

        reg.update_node_task(&miroir_id, "node-2", NodeTaskStatus::Succeeded)
            .unwrap();
        let done = reg.get(&miroir_id).unwrap().unwrap();
        assert_eq!(done.status, TaskStatus::Succeeded);
    }
}
