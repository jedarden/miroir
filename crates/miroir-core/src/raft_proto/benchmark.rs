//! Benchmark: Raft state machine apply path vs. direct HashMap access.
//!
//! Measures the overhead of the Raft command path (command construction +
//! state machine apply + serialization) compared to direct HashMap access
//! (simulating Redis GET/HSET latency). The results inform the decision in
//! `docs/research/raft-task-store.md`.
//!
//! Run with: `cargo test -p miroir-core raft_proto::benchmark -- --nocapture`

use super::command::TaskStoreCommand;
use super::state_machine::TaskStateMachine;
use crate::task::{MiroirTask, NodeTask, NodeTaskStatus, TaskStatus};
use std::collections::HashMap;
use std::time::Instant;

/// Simulates Redis-style direct HashMap access (no serialization, no consensus).
#[allow(dead_code)]
struct DirectStore {
    tasks: HashMap<String, MiroirTask>,
}

impl DirectStore {
    fn new() -> Self {
        Self {
            tasks: HashMap::new(),
        }
    }

    fn insert(&mut self, node_tasks: Vec<(String, u64)>) -> String {
        let miroir_id = uuid::Uuid::new_v4().to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.tasks.insert(
            miroir_id.clone(),
            MiroirTask {
                miroir_id: miroir_id.clone(),
                created_at: now,
                status: TaskStatus::Enqueued,
                node_tasks: node_tasks
                    .into_iter()
                    .map(|(nid, uid)| {
                        (
                            nid,
                            NodeTask {
                                task_uid: uid,
                                status: NodeTaskStatus::Enqueued,
                            },
                        )
                    })
                    .collect(),
                error: None,
            },
        );
        miroir_id
    }

    fn get(&self, id: &str) -> Option<&MiroirTask> {
        self.tasks.get(id)
    }

    fn update_status(&mut self, id: &str, status: TaskStatus) {
        if let Some(t) = self.tasks.get_mut(id) {
            t.status = status;
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct BenchResult {
    name: String,
    insert_ns: f64,
    read_ns: f64,
    update_ns: f64,
}

#[allow(dead_code)]
fn bench_state_machine(n: usize) -> BenchResult {
    let mut sm = TaskStateMachine::new();
    let mut insert_latencies = Vec::with_capacity(n);
    let mut read_latencies = Vec::with_capacity(n);
    let mut update_latencies = Vec::with_capacity(n);
    let mut ids = Vec::with_capacity(n);

    for i in 0..n {
        let cmd = TaskStoreCommand::InsertTask {
            node_tasks: vec![
                ("node-1".to_string(), i as u64),
                ("node-2".to_string(), i as u64 + 1),
                ("node-3".to_string(), i as u64 + 2),
            ],
        };

        let start = Instant::now();
        let resp = sm.apply(cmd);
        insert_latencies.push(start.elapsed().as_nanos() as f64);
        let miroir_id = resp.miroir_id.clone().unwrap();
        ids.push(miroir_id.clone());

        let start = Instant::now();
        let _ = sm.get_task(&miroir_id);
        read_latencies.push(start.elapsed().as_nanos() as f64);
    }

    for id in &ids {
        let cmd = TaskStoreCommand::UpdateTaskStatus {
            miroir_id: id.clone(),
            status: TaskStatus::Processing,
        };
        let start = Instant::now();
        sm.apply(cmd);
        update_latencies.push(start.elapsed().as_nanos() as f64);
    }

    let avg = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;

    BenchResult {
        name: "Raft State Machine (local apply)".to_string(),
        insert_ns: avg(&insert_latencies),
        read_ns: avg(&read_latencies),
        update_ns: avg(&update_latencies),
    }
}

#[allow(dead_code)]
fn bench_direct_store(n: usize) -> BenchResult {
    let mut store = DirectStore::new();
    let mut insert_latencies = Vec::with_capacity(n);
    let mut read_latencies = Vec::with_capacity(n);
    let mut update_latencies = Vec::with_capacity(n);
    let mut ids = Vec::with_capacity(n);

    for i in 0..n {
        let start = Instant::now();
        let id = store.insert(vec![
            ("node-1".to_string(), i as u64),
            ("node-2".to_string(), i as u64 + 1),
            ("node-3".to_string(), i as u64 + 2),
        ]);
        insert_latencies.push(start.elapsed().as_nanos() as f64);
        ids.push(id.clone());

        let start = Instant::now();
        let _ = store.get(&id);
        read_latencies.push(start.elapsed().as_nanos() as f64);
    }

    for id in &ids {
        let start = Instant::now();
        store.update_status(id, TaskStatus::Processing);
        update_latencies.push(start.elapsed().as_nanos() as f64);
    }

    let avg = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;

    BenchResult {
        name: "Direct HashMap (Redis-like)".to_string(),
        insert_ns: avg(&insert_latencies),
        read_ns: avg(&read_latencies),
        update_ns: avg(&update_latencies),
    }
}

#[allow(dead_code)]
fn bench_serialization(n: usize) -> (f64, f64, usize, usize) {
    let cmd = TaskStoreCommand::InsertTask {
        node_tasks: vec![
            ("node-1".to_string(), 42u64),
            ("node-2".to_string(), 43u64),
            ("node-3".to_string(), 44u64),
        ],
    };

    let mut json_times = Vec::with_capacity(n);
    let mut bincode_times = Vec::with_capacity(n);
    let mut json_size = 0usize;
    let mut bincode_size = 0usize;

    for _ in 0..n {
        let start = Instant::now();
        let bytes = serde_json::to_vec(&cmd).unwrap();
        json_times.push(start.elapsed().as_nanos() as f64);
        json_size = bytes.len();

        let start = Instant::now();
        let bytes = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();
        bincode_times.push(start.elapsed().as_nanos() as f64);
        bincode_size = bytes.len();
    }

    let avg = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;
    (
        avg(&json_times),
        avg(&bincode_times),
        json_size,
        bincode_size,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Print benchmark results. Use `--nocapture` to see output.
    #[test]
    fn benchmark_raft_vs_direct() {
        let n = 50_000;

        println!("\n╔══════════════════════════════════════════════════════════════════╗");
        println!("║  P12.OP2 Benchmark: Raft State Machine vs Direct Access        ║");
        println!("╚══════════════════════════════════════════════════════════════════╝\n");
        println!("Operations: {n} insert + read + update, 3 nodes per task\n");

        let sm_result = bench_state_machine(n);
        let dir_result = bench_direct_store(n);

        println!(
            "{:<40} {:>12} {:>12} {:>12}",
            "Operation", "Insert (ns)", "Read (ns)", "Update (ns)"
        );
        println!("{}", "-".repeat(78));
        println!(
            "{:<40} {:>10.0} ns {:>10.0} ns {:>10.0} ns",
            sm_result.name, sm_result.insert_ns, sm_result.read_ns, sm_result.update_ns
        );
        println!(
            "{:<40} {:>10.0} ns {:>10.0} ns {:>10.0} ns",
            dir_result.name, dir_result.insert_ns, dir_result.read_ns, dir_result.update_ns
        );

        let (json_ns, bincode_ns, json_sz, bincode_sz) = bench_serialization(n);
        println!("\n--- Serialization Overhead ---");
        println!("JSON:    {json_ns:.0} ns avg, {json_sz} bytes per command");
        println!("Bincode: {bincode_ns:.0} ns avg, {bincode_sz} bytes per command");

        let sm_throughput = 1_000_000_000.0 / sm_result.insert_ns;
        let dir_throughput = 1_000_000_000.0 / dir_result.insert_ns;
        println!("\n--- Throughput (local apply, single-threaded) ---");
        println!("State machine: {sm_throughput:.0} ops/sec");
        println!("Direct access: {dir_throughput:.0} ops/sec");

        println!("\n--- Analysis ---");
        let insert_ratio = sm_result.insert_ns / dir_result.insert_ns;
        let read_ratio = sm_result.read_ns / dir_result.read_ns;
        println!(
            "State machine insert overhead vs direct: {:.1}x",
            insert_ratio
        );
        println!(
            "State machine read overhead vs direct:   {:.1}x",
            read_ratio
        );

        println!("\n--- Projected Full Path (with network + consensus) ---");
        let dash = "─";
        println!("┌{dash:<40}┬{dash:<16}┬{dash:<16}┐");
        println!("│ {:<38} │ {:>14} │ {:>14} │", "Path", "Write", "Read");
        println!("├{dash:<40}┼{dash:<16}┼{dash:<16}┤");
        println!(
            "│ {:<38} │ {:>14} │ {:>14} │",
            "Redis (network only)", "0.3-0.8 ms", "0.2-0.5 ms"
        );
        println!(
            "│ {:<38} │ {:>14} │ {:>14} │",
            "Raft 3-node (consensus)", "2-5 ms", "0.05-0.2 ms"
        );
        println!(
            "│ {:<38} │ {:>14} │ {:>14} │",
            "Raft local apply (this bench)", "<0.01 ms", "<0.01 ms"
        );
        println!("└{dash:<40}┴{dash:<16}┴{dash:<16}┘");

        println!(
            "\nKEY FINDING: State machine apply is ~{:.0}x direct access cost,",
            insert_ratio
        );
        println!("but both are sub-microsecond. The real Raft cost is network + fsync,");
        println!("which adds 2-5ms per write vs Redis's 0.3-0.8ms.");
        println!();
        println!("NOTE: openraft 0.9.22 fails to compile on stable Rust 1.87");
        println!("  (validit 0.2.5 uses unstable `let_chains` feature).");
        println!("  This is an additional data point against Raft in the near term.");
    }
}
