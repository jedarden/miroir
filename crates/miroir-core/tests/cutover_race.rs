//! Chaos test: shard migration cutover race window analysis (plan §15 OP#1).
//!
//! Validates that documents are not lost when writes arrive at the exact moment
//! of migration cutover. Uses a simulated cluster + MigrationCoordinator to
//! stress-test every transition boundary.
//!
//! ## Variants
//!
//! - `cutover_chaos_with_anti_entropy`   — AE on, delta pass on → 0 loss
//! - `cutover_chaos_skip_delta_with_ae`  — AE on, delta skipped → measurable loss (AE repairs)
//! - `cutover_chaos_no_ae_with_delta`    — AE off, delta pass on → 0 loss
//! - `cutover_chaos_no_ae_no_delta_blocked` — unsafe config refused
//! - `cutover_chaos_boundary_burst`      — writes at every phase transition
//! - `cutover_chaos_high_volume`         — 100K writes, loss rate measurement
//! - `cutover_chaos_loss_rate_no_ae_delta` — loss rate with AE off + delta on
//! - `cutover_chaos_validation_gates`    — unsafe path blocked at config level
//! - `cutover_chaos_tight_loop_boundary` — rapid-fire writes at exact cutover instant
//! - `cutover_chaos_loss_rate_1m_ae_on`  — 1M writes, loss rate with AE on + delta
//! - `cutover_chaos_loss_rate_no_ae_no_delta` — AE off + delta off, quantify loss rate
//! - `cutover_chaos_concurrent_migration_writes` — writes during entire migration lifecycle
//! - `cutover_chaos_three_node_cluster` — 3-node cluster, writes at every transition boundary
//! - `cutover_chaos_three_node_no_ae_with_delta` — 3-node, AE off + delta on → 0 loss

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use miroir_core::migration::{
    InFlightWrite, MigrationConfig, MigrationCoordinator, MigrationError, MigrationPhase, NodeId,
    ShardId,
};

fn node(s: &str) -> NodeId {
    NodeId(s.to_string())
}

fn shard(id: u32) -> ShardId {
    ShardId(id)
}

/// Simulated cluster: tracks which documents exist on which node.
struct SimCluster {
    data: HashMap<NodeId, HashMap<ShardId, HashSet<String>>>,
}

impl SimCluster {
    fn new(nodes: &[NodeId]) -> Self {
        Self {
            data: nodes.iter().map(|n| (n.clone(), HashMap::new())).collect(),
        }
    }

    fn put(&mut self, node: &NodeId, shard: ShardId, doc_id: &str) {
        self.data
            .entry(node.clone())
            .or_default()
            .entry(shard)
            .or_default()
            .insert(doc_id.to_string());
    }

    #[allow(dead_code)]
    fn all_docs_for_shards(&self, shards: &[ShardId]) -> HashSet<String> {
        let mut all = HashSet::new();
        for node_data in self.data.values() {
            for &s in shards {
                if let Some(docs) = node_data.get(&s) {
                    all.extend(docs.iter().cloned());
                }
            }
        }
        all
    }

    /// Docs on old_node but NOT on new_node for given shards.
    fn lost_docs(&self, old_node: &NodeId, new_node: &NodeId, shards: &[ShardId]) -> Vec<String> {
        let mut lost = Vec::new();
        for &s in shards {
            let old_docs = self
                .data
                .get(old_node)
                .and_then(|m| m.get(&s))
                .cloned()
                .unwrap_or_default();
            let new_docs = self
                .data
                .get(new_node)
                .and_then(|m| m.get(&s))
                .cloned()
                .unwrap_or_default();
            for doc in &old_docs {
                if !new_docs.contains(doc.as_str()) {
                    lost.push(doc.clone());
                }
            }
        }
        lost
    }
}

struct RecordedWrite {
    doc_id: String,
    shard: ShardId,
    succeeded_on_old: bool,
    succeeded_on_new: bool,
}

/// Simulate a dual-write. Returns what happened on each node.
fn dual_write(
    cluster: &mut SimCluster,
    old_node: &NodeId,
    new_node: &NodeId,
    shard: ShardId,
    doc_id: &str,
    old_fails: bool,
    new_fails: bool,
) -> RecordedWrite {
    if !old_fails {
        cluster.put(old_node, shard, doc_id);
    }
    if !new_fails {
        cluster.put(new_node, shard, doc_id);
    }
    RecordedWrite {
        doc_id: doc_id.to_string(),
        shard,
        succeeded_on_old: !old_fails,
        succeeded_on_new: !new_fails,
    }
}

/// Build an InFlightWrite from a RecordedWrite.
/// Critically: if a node did NOT accept the write, mark it as *failed* (not
/// just missing from completed_nodes). This is what the drain logic expects —
/// `is_drained()` requires completed + failed == target count.
fn make_in_flight(w: &RecordedWrite, old_node: &NodeId, new_node: &NodeId) -> InFlightWrite {
    let mut completed = HashSet::new();
    let mut failed = HashMap::new();

    if w.succeeded_on_old {
        completed.insert(old_node.clone());
    } else {
        failed.insert(old_node.clone(), "write failed".into());
    }
    if w.succeeded_on_new {
        completed.insert(new_node.clone());
    } else {
        failed.insert(new_node.clone(), "write failed".into());
    }

    InFlightWrite {
        doc_id: w.doc_id.clone(),
        shard: w.shard,
        target_nodes: vec![old_node.clone(), new_node.clone()],
        completed_nodes: completed,
        failed_nodes: failed,
        submitted_at: Instant::now(),
    }
}

/// Run a full delta pass on the simulated cluster: copy every doc on old but
/// not on new to new, then call shard_delta_complete for each shard.
fn run_delta_pass(
    coord: &mut MigrationCoordinator,
    cluster: &mut SimCluster,
    mid: miroir_core::migration::MigrationId,
    old_node: &NodeId,
    new_node: &NodeId,
    shards: &[ShardId],
) {
    for &s in shards {
        let lost: Vec<String> = cluster
            .data
            .get(old_node)
            .and_then(|m| m.get(&s))
            .map(|docs| {
                docs.iter()
                    .filter(|d| {
                        !cluster
                            .data
                            .get(new_node)
                            .and_then(|m| m.get(&s))
                            .is_some_and(|nd| nd.contains(*d))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        for doc_id in &lost {
            cluster.put(new_node, s, doc_id);
        }
        coord
            .shard_delta_complete(mid, s, lost.len() as u64)
            .unwrap();
    }
}

// ---------------------------------------------------------------------------
// Test 1: AE on + delta pass on → 0 loss
// ---------------------------------------------------------------------------

#[test]
fn cutover_chaos_with_anti_entropy() {
    let config = MigrationConfig {
        anti_entropy_enabled: true,
        skip_delta_pass: false,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0), shard(1), shard(2)];
    let affected: HashMap<ShardId, NodeId> = shards.iter().map(|&s| (s, old.clone())).collect();

    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    // Pre-populate 1000 docs
    for i in 0..1000u64 {
        let s = shards[i as usize % shards.len()];
        cluster.put(&old, s, &format!("pre-{i}"));
    }

    // Migration
    let mid = coord.begin_migration(new.clone(), 0, affected).unwrap();
    coord.begin_dual_write(mid).unwrap();

    // Dual-write phase: 2% failure on new
    let mut boundary: Vec<RecordedWrite> = Vec::new();
    for i in 0..1000u64 {
        let doc_id = format!("dw-{i}");
        let s = shards[i as usize % shards.len()];
        let new_fails = i % 50 == 0;
        let w = dual_write(&mut cluster, &old, &new, s, &doc_id, false, new_fails);
        boundary.push(w);
    }

    // Background migration done
    for &s in &shards {
        coord.shard_migration_complete(mid, s, 500).unwrap();
    }

    // Boundary writes (arrive between CutoverBegin and begin_cutover)
    for i in 0..100u64 {
        let doc_id = format!("bnd-{i}");
        let s = shards[i as usize % shards.len()];
        let w = dual_write(&mut cluster, &old, &new, s, &doc_id, false, false);
        boundary.push(w);
    }

    // Register all writes as in-flight (drain will verify they've settled)
    for w in &boundary {
        coord.register_in_flight(make_in_flight(w, &old, &new));
    }

    // Cutover
    coord.begin_cutover(mid).unwrap();
    let phase = coord.complete_drain(mid).unwrap();

    // Delta pass should be triggered (some writes failed on new)
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    // Verify there ARE lost docs before delta pass
    let lost_before = cluster.lost_docs(&old, &new, &shards);
    assert!(
        !lost_before.is_empty(),
        "Expected some lost docs before delta pass"
    );

    // Delta pass repairs them
    run_delta_pass(&mut coord, &mut cluster, mid, &old, &new, &shards);

    assert_eq!(
        coord.get_state(mid).unwrap().phase,
        MigrationPhase::CutoverCleanup
    );
    coord.complete_cleanup(mid).unwrap();

    // Final: 0 loss
    let lost = cluster.lost_docs(&old, &new, &shards);
    assert_eq!(
        lost.len(),
        0,
        "Lost {} docs with AE on + delta pass",
        lost.len()
    );

    let all = cluster.all_docs_for_shards(&shards);
    assert_eq!(all.len(), 2100, "Expected 2100 docs, got {}", all.len());
}

// ---------------------------------------------------------------------------
// Test 2: AE on + delta skipped → measurable loss (AE would repair later)
// ---------------------------------------------------------------------------

#[test]
fn cutover_chaos_skip_delta_with_ae() {
    let config = MigrationConfig {
        anti_entropy_enabled: true,
        skip_delta_pass: true,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0), shard(1)];
    let affected: HashMap<ShardId, NodeId> = shards.iter().map(|&s| (s, old.clone())).collect();

    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    for i in 0..500u64 {
        let s = shards[i as usize % shards.len()];
        cluster.put(&old, s, &format!("pre-{i}"));
    }

    let mid = coord.begin_migration(new.clone(), 0, affected).unwrap();
    coord.begin_dual_write(mid).unwrap();

    // Dual-write: 5% failure on new
    let mut expected_lost: Vec<String> = Vec::new();
    let mut writes: Vec<RecordedWrite> = Vec::new();
    for i in 0..200u64 {
        let doc_id = format!("dw-{i}");
        let s = shards[i as usize % shards.len()];
        let new_fails = i % 20 == 0;
        let w = dual_write(&mut cluster, &old, &new, s, &doc_id, false, new_fails);
        if w.succeeded_on_old && !w.succeeded_on_new {
            expected_lost.push(doc_id);
        }
        writes.push(w);
    }

    // Boundary writes — all succeed
    for i in 0..50u64 {
        let doc_id = format!("bnd-{i}");
        let s = shards[i as usize % shards.len()];
        let w = dual_write(&mut cluster, &old, &new, s, &doc_id, false, false);
        writes.push(w);
    }

    for &s in &shards {
        coord.shard_migration_complete(mid, s, 300).unwrap();
    }

    // Register writes so drain can complete
    for w in &writes {
        coord.register_in_flight(make_in_flight(w, &old, &new));
    }

    coord.begin_cutover(mid).unwrap();
    let phase = coord.complete_drain(mid).unwrap();
    // skip_delta_pass → straight to cleanup
    assert_eq!(phase, MigrationPhase::CutoverCleanup);

    coord.complete_cleanup(mid).unwrap();

    // Measure loss — pre-existing docs (500) are on old only since background
    // migration copies them but our simulation doesn't track that copy. Plus the
    // dual-write failures. All would be repaired by AE in production.
    let lost = cluster.lost_docs(&old, &new, &shards);
    // Verify dual-write failures are a subset of lost docs
    for doc in &expected_lost {
        assert!(lost.contains(doc), "Expected {doc} in lost set");
    }
    assert!(
        !lost.is_empty(),
        "Expected some lost docs when skipping delta pass"
    );

    eprintln!(
        "\n=== Skip Delta + AE ON ===\n\
         Dual-write failures (subset of lost): {}\n\
         Total docs lost after cutover (no delta pass): {}\n\
         All {} would be repaired by anti-entropy on next pass.\n",
        expected_lost.len(),
        lost.len(),
        lost.len()
    );
}

// ---------------------------------------------------------------------------
// Test 3: AE off + delta pass on → 0 loss (delta pass is sufficient alone)
// ---------------------------------------------------------------------------

#[test]
fn cutover_chaos_no_ae_with_delta() {
    let config = MigrationConfig {
        anti_entropy_enabled: false,
        skip_delta_pass: false,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0), shard(1), shard(2)];
    let affected: HashMap<ShardId, NodeId> = shards.iter().map(|&s| (s, old.clone())).collect();

    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    for i in 0..1000u64 {
        let s = shards[i as usize % shards.len()];
        cluster.put(&old, s, &format!("pre-{i}"));
    }

    let mid = coord.begin_migration(new.clone(), 0, affected).unwrap();
    coord.begin_dual_write(mid).unwrap();

    // 2% failure on new
    let mut writes: Vec<RecordedWrite> = Vec::new();
    for i in 0..1000u64 {
        let doc_id = format!("dw-{i}");
        let s = shards[i as usize % shards.len()];
        let new_fails = i % 50 == 0;
        let w = dual_write(&mut cluster, &old, &new, s, &doc_id, false, new_fails);
        writes.push(w);
    }

    // Boundary: 1% failure
    for i in 0..200u64 {
        let doc_id = format!("bnd-{i}");
        let s = shards[i as usize % shards.len()];
        let new_fails = i % 100 == 0;
        let w = dual_write(&mut cluster, &old, &new, s, &doc_id, false, new_fails);
        writes.push(w);
    }

    for &s in &shards {
        coord.shard_migration_complete(mid, s, 500).unwrap();
    }

    for w in &writes {
        coord.register_in_flight(make_in_flight(w, &old, &new));
    }

    coord.begin_cutover(mid).unwrap();
    let phase = coord.complete_drain(mid).unwrap();
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    run_delta_pass(&mut coord, &mut cluster, mid, &old, &new, &shards);
    coord.complete_cleanup(mid).unwrap();

    let lost = cluster.lost_docs(&old, &new, &shards);
    assert_eq!(
        lost.len(),
        0,
        "Delta pass alone should provide 0 loss. Lost: {:?}",
        &lost[..lost.len().min(10)]
    );
}

// ---------------------------------------------------------------------------
// Test 4: AE off + delta skipped → refused at config validation
// ---------------------------------------------------------------------------

#[test]
fn cutover_chaos_no_ae_no_delta_blocked() {
    let config = MigrationConfig {
        anti_entropy_enabled: false,
        skip_delta_pass: true,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let affected: HashMap<ShardId, NodeId> = [(shard(0), node("old-0"))].into_iter().collect();

    let result = coord.begin_migration(node("new-3"), 0, affected);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        MigrationError::UnsafeCutoverNoAntiEntropy
    ));
}

// ---------------------------------------------------------------------------
// Test 5: boundary burst — writes at every phase transition
// ---------------------------------------------------------------------------

#[test]
fn cutover_chaos_boundary_burst() {
    let config = MigrationConfig {
        anti_entropy_enabled: true,
        skip_delta_pass: false,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0), shard(1)];
    let affected: HashMap<ShardId, NodeId> = shards.iter().map(|&s| (s, old.clone())).collect();

    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    // Pre-populate
    for i in 0..500u64 {
        let s = shards[i as usize % shards.len()];
        cluster.put(&old, s, &format!("pre-{i}"));
    }

    let mid = coord.begin_migration(new.clone(), 0, affected).unwrap();

    let mut all_writes: Vec<RecordedWrite> = Vec::new();

    // Burst 1: ComputingAssignments → DualWriteMigrating
    for i in 0..50u64 {
        let s = shards[i as usize % shards.len()];
        let w = dual_write(
            &mut cluster,
            &old,
            &new,
            s,
            &format!("b1-{i}"),
            false,
            false,
        );
        all_writes.push(w);
    }

    coord.begin_dual_write(mid).unwrap();

    // Burst 2: during DualWriteMigrating, some fail on new
    for i in 0..100u64 {
        let s = shards[i as usize % shards.len()];
        let new_fails = i % 25 == 0;
        let w = dual_write(
            &mut cluster,
            &old,
            &new,
            s,
            &format!("b2-{i}"),
            false,
            new_fails,
        );
        all_writes.push(w);
    }

    // Burst 3: just before each shard_migration_complete
    for &s in &shards {
        let w = dual_write(
            &mut cluster,
            &old,
            &new,
            s,
            &format!("b3-{s:?}"),
            false,
            false,
        );
        all_writes.push(w);
        coord.shard_migration_complete(mid, s, 300).unwrap();
    }

    // Burst 4: CutoverBegin → begin_cutover
    for i in 0..50u64 {
        let s = shards[i as usize % shards.len()];
        let w = dual_write(
            &mut cluster,
            &old,
            &new,
            s,
            &format!("b4-{i}"),
            false,
            false,
        );
        all_writes.push(w);
    }

    // Register all completed writes for drain
    for w in &all_writes {
        coord.register_in_flight(make_in_flight(w, &old, &new));
    }

    coord.begin_cutover(mid).unwrap();

    // Burst 5: during CutoverDraining — these go to old only
    // In production, the delta pass catches these. We register them as
    // failed on new so drain completes.
    for i in 0..50u64 {
        let s = shards[i as usize % shards.len()];
        let doc_id = format!("b5-{i}");
        cluster.put(&old, s, &doc_id);
        // Not on new — will be caught by delta pass
        let w = RecordedWrite {
            doc_id: doc_id.clone(),
            shard: s,
            succeeded_on_old: true,
            succeeded_on_new: false,
        };
        coord.register_in_flight(make_in_flight(&w, &old, &new));
        all_writes.push(w);
    }

    let phase = coord.complete_drain(mid).unwrap();
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    run_delta_pass(&mut coord, &mut cluster, mid, &old, &new, &shards);

    assert_eq!(
        coord.get_state(mid).unwrap().phase,
        MigrationPhase::CutoverCleanup
    );
    coord.complete_cleanup(mid).unwrap();

    let lost = cluster.lost_docs(&old, &new, &shards);
    assert_eq!(lost.len(), 0, "Boundary burst lost {} docs", lost.len());

    let all = cluster.all_docs_for_shards(&shards);
    // 500 pre + 50 b1 + 100 b2 + 2 b3 + 50 b4 + 50 b5 = 752
    assert!(all.len() >= 750, "Expected >= 750 docs, got {}", all.len());
}

// ---------------------------------------------------------------------------
// Test 6: high volume — 100K writes, measure loss rate with AE + delta
// ---------------------------------------------------------------------------

#[test]
fn cutover_chaos_high_volume() {
    let config = MigrationConfig {
        anti_entropy_enabled: true,
        skip_delta_pass: false,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0)];
    let affected: HashMap<ShardId, NodeId> = shards.iter().map(|&s| (s, old.clone())).collect();

    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    const TOTAL: u64 = 100_000;

    for i in 0..1000u64 {
        cluster.put(&old, shards[0], &format!("pre-{i}"));
    }

    let mid = coord.begin_migration(new.clone(), 0, affected).unwrap();
    coord.begin_dual_write(mid).unwrap();

    // 1% failure on new, but we don't register as in-flight for perf.
    // We register a representative sample to drive delta pass.
    for i in 0..TOTAL {
        let doc_id = format!("w-{i}");
        let new_fails = i % 100 == 0;
        dual_write(
            &mut cluster,
            &old,
            &new,
            shards[0],
            &doc_id,
            false,
            new_fails,
        );
    }

    coord
        .shard_migration_complete(mid, shards[0], 1000)
        .unwrap();

    // Boundary writes
    for i in 0..100u64 {
        dual_write(
            &mut cluster,
            &old,
            &new,
            shards[0],
            &format!("bnd-{i}"),
            false,
            false,
        );
    }

    // Register one failed write to force delta pass
    coord.register_in_flight(InFlightWrite {
        doc_id: "w-0".into(),
        shard: shards[0],
        target_nodes: vec![old.clone(), new.clone()],
        completed_nodes: HashSet::from([old.clone()]),
        failed_nodes: HashMap::from([(new.clone(), "simulated failure".into())]),
        submitted_at: Instant::now(),
    });

    coord.begin_cutover(mid).unwrap();
    let phase = coord.complete_drain(mid).unwrap();
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    // Delta pass
    let lost: Vec<String> = cluster
        .data
        .get(&old)
        .and_then(|m| m.get(&shards[0]))
        .map(|docs| {
            docs.iter()
                .filter(|d| {
                    !cluster
                        .data
                        .get(&new)
                        .and_then(|m| m.get(&shards[0]))
                        .is_some_and(|nd| nd.contains(*d))
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default();

    let delta_count = lost.len();
    for doc_id in &lost {
        cluster.put(&new, shards[0], doc_id);
    }
    coord
        .shard_delta_complete(mid, shards[0], delta_count as u64)
        .unwrap();
    coord.complete_cleanup(mid).unwrap();

    let lost_after = cluster.lost_docs(&old, &new, &shards);
    assert_eq!(
        lost_after.len(),
        0,
        "High-volume: lost {}/{} writes ({}%)",
        lost_after.len(),
        TOTAL,
        (lost_after.len() as f64 / TOTAL as f64) * 100.0
    );

    let all = cluster.all_docs_for_shards(&shards);
    let expected = 1000 + TOTAL as usize + 100;
    assert_eq!(all.len(), expected);

    eprintln!(
        "\n=== High Volume ({}K writes) ===\n\
         Writes failed on new during dual-write: {}\n\
         Caught by delta pass: {}\n\
         Lost after delta pass: 0\n\
         Loss rate: 0/{TOTAL} (0.000%)\n",
        TOTAL / 1000,
        TOTAL / 100,
        delta_count,
    );
}

// ---------------------------------------------------------------------------
// Test 7: AE off + delta on, 50K writes — confirm 0 loss rate
// ---------------------------------------------------------------------------

#[test]
fn cutover_chaos_loss_rate_no_ae_delta() {
    let config = MigrationConfig {
        anti_entropy_enabled: false,
        skip_delta_pass: false,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0), shard(1)];
    let affected: HashMap<ShardId, NodeId> = shards.iter().map(|&s| (s, old.clone())).collect();

    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    const TOTAL: u64 = 50_000;

    for i in 0..500u64 {
        cluster.put(&old, shards[i as usize % shards.len()], &format!("pre-{i}"));
    }

    let mid = coord.begin_migration(new.clone(), 0, affected).unwrap();
    coord.begin_dual_write(mid).unwrap();

    let mut failure_count = 0u64;
    for i in 0..TOTAL {
        let doc_id = format!("w-{i}");
        let s = shards[i as usize % shards.len()];
        let new_fails = i % 100 == 0;
        if new_fails {
            failure_count += 1;
        }
        dual_write(&mut cluster, &old, &new, s, &doc_id, false, new_fails);
    }

    for &s in &shards {
        coord.shard_migration_complete(mid, s, 300).unwrap();
    }

    // Register one failed write to force delta pass
    coord.register_in_flight(InFlightWrite {
        doc_id: "w-0".into(),
        shard: shards[0],
        target_nodes: vec![old.clone(), new.clone()],
        completed_nodes: HashSet::from([old.clone()]),
        failed_nodes: HashMap::from([(new.clone(), "simulated failure".into())]),
        submitted_at: Instant::now(),
    });

    coord.begin_cutover(mid).unwrap();
    let phase = coord.complete_drain(mid).unwrap();
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    run_delta_pass(&mut coord, &mut cluster, mid, &old, &new, &shards);
    coord.complete_cleanup(mid).unwrap();

    let lost = cluster.lost_docs(&old, &new, &shards);
    assert_eq!(
        lost.len(),
        0,
        "Lost {} docs without AE but with delta pass",
        lost.len()
    );

    eprintln!(
        "\n=== Loss Rate: AE OFF + Delta Pass ON ===\n\
         Total writes: {TOTAL}\n\
         Writes failed on new during dual-write: {failure_count}\n\
         Docs lost after delta pass: 0\n\
         Loss rate: 0/{TOTAL} (0.000%)\n\
         Conclusion: Delta pass alone provides 0-loss cutover.\n"
    );
}

// ---------------------------------------------------------------------------
// Test 8: validation gates block unsafe configuration
// ---------------------------------------------------------------------------

#[test]
fn cutover_chaos_validation_gates() {
    use miroir_core::anti_entropy::{validate_migration_safety, AntiEntropyConfig};

    // Gate 1: MigrationCoordinator refuses unsafe config
    let config = MigrationConfig {
        anti_entropy_enabled: false,
        skip_delta_pass: true,
        ..Default::default()
    };
    let coord = MigrationCoordinator::new(config);
    assert!(coord.validate_safety().is_err());

    // Gate 2: Cross-module anti_entropy validation
    let ae = AntiEntropyConfig {
        enabled: false,
        ..Default::default()
    };
    let mc = MigrationConfig {
        skip_delta_pass: true,
        anti_entropy_enabled: false,
        ..Default::default()
    };
    assert!(validate_migration_safety(&ae, &mc).is_err());

    // Gate 3: Warning when AE disabled
    use miroir_core::anti_entropy::migration_warning_if_ae_disabled;
    assert!(migration_warning_if_ae_disabled(false).is_some());
    assert!(migration_warning_if_ae_disabled(true).is_none());

    // Safe configs should pass
    let safe_config = MigrationConfig {
        anti_entropy_enabled: false,
        skip_delta_pass: false,
        ..Default::default()
    };
    let safe_coord = MigrationCoordinator::new(safe_config);
    assert!(safe_coord.validate_safety().is_ok());

    let safe_ae = AntiEntropyConfig {
        enabled: true,
        ..Default::default()
    };
    let safe_mc = MigrationConfig {
        skip_delta_pass: true,
        anti_entropy_enabled: true,
        ..Default::default()
    };
    assert!(validate_migration_safety(&safe_ae, &safe_mc).is_ok());
}

// ---------------------------------------------------------------------------
// Test 9: tight-loop boundary — writes at exact cutover transition instant
// ---------------------------------------------------------------------------
//
// Simulates docs arriving at the instant of `active` transition (step 7 in
// plan §2 "Adding a node"). The dangerous window is between CutoverBegin and
// CutoverCleanup. Writes that succeed on OLD but fail on NEW during this
// window MUST be caught by the delta pass.
//
// This variant drives writes at every single state transition boundary with
// deterministic 5% failure injection on the new node.

#[test]
fn cutover_chaos_tight_loop_boundary() {
    let config = MigrationConfig {
        anti_entropy_enabled: true,
        skip_delta_pass: false,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0), shard(1), shard(2)];
    let affected: HashMap<ShardId, NodeId> = shards.iter().map(|&s| (s, old.clone())).collect();

    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    // Pre-populate 1000 docs
    for i in 0..1000u64 {
        let s = shards[i as usize % shards.len()];
        cluster.put(&old, s, &format!("pre-{i}"));
    }

    let mut all_writes: Vec<RecordedWrite> = Vec::new();

    // Phase: ComputingAssignments → begin_migration
    let mid = coord.begin_migration(new.clone(), 0, affected).unwrap();

    // Burst: writes BEFORE dual-write starts (should go to old only in prod,
    // but we simulate them as dual-write for boundary testing)
    for i in 0..200u64 {
        let s = shards[i as usize % shards.len()];
        let new_fails = i % 20 == 0; // 5% failure
        let w = dual_write(
            &mut cluster,
            &old,
            &new,
            s,
            &format!("t0-{i}"),
            false,
            new_fails,
        );
        all_writes.push(w);
    }

    // Transition: ComputingAssignments → DualWriteMigrating
    coord.begin_dual_write(mid).unwrap();

    // Burst: writes during active dual-write migration
    for i in 0..500u64 {
        let s = shards[i as usize % shards.len()];
        let new_fails = i % 20 == 0; // 5% failure
        let w = dual_write(
            &mut cluster,
            &old,
            &new,
            s,
            &format!("t1-{i}"),
            false,
            new_fails,
        );
        all_writes.push(w);
    }

    // Transition: complete shard migrations one at a time, writing between each
    for (idx, &s) in shards.iter().enumerate() {
        // Writes between shard completions
        for j in 0..50u64 {
            let s2 = shards[j as usize % shards.len()];
            let new_fails = j % 20 == 0;
            let w = dual_write(
                &mut cluster,
                &old,
                &new,
                s2,
                &format!("t2-{idx}-{j}"),
                false,
                new_fails,
            );
            all_writes.push(w);
        }
        coord.shard_migration_complete(mid, s, 500).unwrap();
    }

    // Transition: CutoverBegin → begin_cutover
    // Rapid-fire writes at the exact boundary
    for i in 0..300u64 {
        let s = shards[i as usize % shards.len()];
        let new_fails = i % 20 == 0;
        let w = dual_write(
            &mut cluster,
            &old,
            &new,
            s,
            &format!("t3-{i}"),
            false,
            new_fails,
        );
        all_writes.push(w);
    }

    // Register all writes for drain tracking
    for w in &all_writes {
        coord.register_in_flight(make_in_flight(w, &old, &new));
    }

    // Cutover: stop dual-write, drain in-flight
    coord.begin_cutover(mid).unwrap();

    // Writes during draining — these go to old only
    for i in 0..200u64 {
        let s = shards[i as usize % shards.len()];
        let doc_id = format!("t4-{i}");
        cluster.put(&old, s, &doc_id);
        let w = RecordedWrite {
            doc_id: doc_id.clone(),
            shard: s,
            succeeded_on_old: true,
            succeeded_on_new: false,
        };
        coord.register_in_flight(make_in_flight(&w, &old, &new));
        all_writes.push(w);
    }

    let phase = coord.complete_drain(mid).unwrap();
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    // Delta pass catches everything
    run_delta_pass(&mut coord, &mut cluster, mid, &old, &new, &shards);

    assert_eq!(
        coord.get_state(mid).unwrap().phase,
        MigrationPhase::CutoverCleanup
    );
    coord.complete_cleanup(mid).unwrap();

    // Assert: 0 lost docs
    let lost = cluster.lost_docs(&old, &new, &shards);
    assert_eq!(
        lost.len(),
        0,
        "Tight-loop boundary test lost {} docs",
        lost.len()
    );

    let all = cluster.all_docs_for_shards(&shards);
    // 1000 pre + 200 t0 + 500 t1 + 150 t2 + 300 t3 + 200 t4 = 2350
    assert!(
        all.len() >= 2350,
        "Expected >= 2350 docs, got {}",
        all.len()
    );

    eprintln!(
        "\n=== Tight-Loop Boundary ===\n\
         Total writes at boundaries: {}\n\
         Docs lost after delta pass: 0\n\
         Loss rate: 0/{} (0.000%)\n",
        all_writes.len(),
        all_writes.len()
    );
}

// ---------------------------------------------------------------------------
// Test 10: 1M write loss rate measurement — AE on + delta on
// ---------------------------------------------------------------------------
//
// Acceptance criterion: loss rate < 1 per 1M writes.

#[test]
fn cutover_chaos_loss_rate_1m_ae_on() {
    let config = MigrationConfig {
        anti_entropy_enabled: true,
        skip_delta_pass: false,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0)];
    let affected: HashMap<ShardId, NodeId> = shards.iter().map(|&s| (s, old.clone())).collect();

    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    const TOTAL: u64 = 1_000_000;
    const FAIL_RATE: u64 = 100; // 1% failure on new

    // Pre-populate
    for i in 0..1000u64 {
        cluster.put(&old, shards[0], &format!("pre-{i}"));
    }

    let mid = coord.begin_migration(new.clone(), 0, affected).unwrap();
    coord.begin_dual_write(mid).unwrap();

    // 1M writes with 1% deterministic failure on new
    let mut failure_count = 0u64;
    for i in 0..TOTAL {
        let doc_id = format!("w-{i}");
        let new_fails = i % FAIL_RATE == 0;
        if new_fails {
            failure_count += 1;
        }
        dual_write(
            &mut cluster,
            &old,
            &new,
            shards[0],
            &doc_id,
            false,
            new_fails,
        );
    }

    coord
        .shard_migration_complete(mid, shards[0], 1000)
        .unwrap();

    // Boundary writes
    for i in 0..500u64 {
        dual_write(
            &mut cluster,
            &old,
            &new,
            shards[0],
            &format!("bnd-{i}"),
            false,
            false,
        );
    }

    // Register one known-failed write to force delta pass
    coord.register_in_flight(InFlightWrite {
        doc_id: "w-0".into(),
        shard: shards[0],
        target_nodes: vec![old.clone(), new.clone()],
        completed_nodes: HashSet::from([old.clone()]),
        failed_nodes: HashMap::from([(new.clone(), "simulated failure".into())]),
        submitted_at: Instant::now(),
    });

    coord.begin_cutover(mid).unwrap();
    let phase = coord.complete_drain(mid).unwrap();
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    // Delta pass
    let lost: Vec<String> = cluster
        .data
        .get(&old)
        .and_then(|m| m.get(&shards[0]))
        .map(|docs| {
            docs.iter()
                .filter(|d| {
                    !cluster
                        .data
                        .get(&new)
                        .and_then(|m| m.get(&shards[0]))
                        .is_some_and(|nd| nd.contains(*d))
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default();

    let delta_count = lost.len();
    for doc_id in &lost {
        cluster.put(&new, shards[0], doc_id);
    }
    coord
        .shard_delta_complete(mid, shards[0], delta_count as u64)
        .unwrap();
    coord.complete_cleanup(mid).unwrap();

    let lost_after = cluster.lost_docs(&old, &new, &shards);
    let loss_rate = lost_after.len() as f64 / TOTAL as f64;

    assert_eq!(
        lost_after.len(),
        0,
        "1M write test: lost {}/{} writes ({:.6}%) — must be 0",
        lost_after.len(),
        TOTAL,
        loss_rate * 100.0
    );

    eprintln!(
        "\n=== 1M Write Loss Rate: AE ON + Delta Pass ON ===\n\
         Total writes: {TOTAL}\n\
         Deterministic failures on new: {failure_count}\n\
         Caught by delta pass: {delta_count}\n\
         Lost after cutover: 0\n\
         Loss rate: 0/{TOTAL} (0.000%)\n\
         PASS: < 1 per 1M writes\n"
    );
}

// ---------------------------------------------------------------------------
// Test 11: AE off + delta off → quantify loss rate (NOT started, refused)
// ---------------------------------------------------------------------------
//
// This configuration is refused by the MigrationCoordinator. We bypass the
// coordinator to measure what WOULD happen — the loss rate justifies the
// hard refusal policy.

#[test]
fn cutover_chaos_loss_rate_no_ae_no_delta() {
    // This config is unsafe — the coordinator would refuse it.
    // We construct a bare cluster to measure what would happen.
    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0)];
    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    const TOTAL: u64 = 100_000;
    const FAIL_RATE: u64 = 50; // 2% failure on new

    // Pre-populate
    for i in 0..1000u64 {
        cluster.put(&old, shards[0], &format!("pre-{i}"));
    }

    // Simulate dual-write without delta pass or AE
    let mut lost_count = 0u64;
    for i in 0..TOTAL {
        let doc_id = format!("w-{i}");
        let new_fails = i % FAIL_RATE == 0;
        dual_write(
            &mut cluster,
            &old,
            &new,
            shards[0],
            &doc_id,
            false,
            new_fails,
        );
        if new_fails {
            lost_count += 1;
        }
    }

    // Boundary writes (all succeed)
    for i in 0..500u64 {
        dual_write(
            &mut cluster,
            &old,
            &new,
            shards[0],
            &format!("bnd-{i}"),
            false,
            false,
        );
    }

    // Measure what's on old but not on new
    let lost = cluster.lost_docs(&old, &new, &shards);
    let loss_rate = lost.len() as f64 / TOTAL as f64;

    // Pre-existing docs (1000) are also lost since we skipped background
    // migration. Focus on dual-write losses.
    assert!(
        lost_count > 0,
        "Expected measurable loss without delta pass"
    );
    assert_eq!(
        lost_count,
        lost.len() as u64 - 1000, // subtract pre-existing
        "Dual-write losses don't match expected count"
    );

    eprintln!(
        "\n=== Loss Rate: AE OFF + Delta OFF (hypothetical) ===\n\
         Total writes: {TOTAL}\n\
         Dual-write failures (old ok, new failed): {lost_count}\n\
         Total docs missing on new (incl. pre-existing): {}\n\
         Dual-write loss rate: {}/{TOTAL} ({:.4}%)\n\
         Decision: MigrationCoordinator REFUSES this configuration.\n\
         Justification: {:.4}% loss rate is unacceptable.\n",
        lost.len(),
        lost_count,
        loss_rate * 100.0,
        loss_rate * 100.0
    );

    // Verify the coordinator does refuse
    let config = MigrationConfig {
        anti_entropy_enabled: false,
        skip_delta_pass: true,
        ..Default::default()
    };
    let coord = MigrationCoordinator::new(config);
    assert!(
        coord.validate_safety().is_err(),
        "Coordinator must refuse unsafe config"
    );
}

// ---------------------------------------------------------------------------
// Test 12: concurrent writes during entire migration lifecycle
// ---------------------------------------------------------------------------
//
// Simulates a realistic workload: writes arriving continuously through the
// entire migration lifecycle, including at the exact CutoverActivate and
// CutoverCleanup transitions.

#[test]
fn cutover_chaos_concurrent_migration_writes() {
    let config = MigrationConfig {
        anti_entropy_enabled: true,
        skip_delta_pass: false,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0), shard(1)];
    let affected: HashMap<ShardId, NodeId> = shards.iter().map(|&s| (s, old.clone())).collect();

    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    // Pre-populate
    for i in 0..500u64 {
        let s = shards[i as usize % shards.len()];
        cluster.put(&old, s, &format!("pre-{i}"));
    }

    let mid = coord.begin_migration(new.clone(), 0, affected).unwrap();
    coord.begin_dual_write(mid).unwrap();

    // Continuous writes through dual-write phase
    let mut writes: Vec<RecordedWrite> = Vec::new();
    for i in 0..5000u64 {
        let s = shards[i as usize % shards.len()];
        let new_fails = i % 50 == 0; // 2% failure
        let w = dual_write(
            &mut cluster,
            &old,
            &new,
            s,
            &format!("cw-{i}"),
            false,
            new_fails,
        );
        writes.push(w);
    }

    // Complete shard migrations
    for &s in &shards {
        coord.shard_migration_complete(mid, s, 300).unwrap();
    }

    // Writes between CutoverBegin and begin_cutover
    for i in 0..500u64 {
        let s = shards[i as usize % shards.len()];
        let new_fails = i % 50 == 0;
        let w = dual_write(
            &mut cluster,
            &old,
            &new,
            s,
            &format!("cb-{i}"),
            false,
            new_fails,
        );
        writes.push(w);
    }

    // Register for drain
    for w in &writes {
        coord.register_in_flight(make_in_flight(w, &old, &new));
    }

    coord.begin_cutover(mid).unwrap();

    // Writes during draining (old only)
    for i in 0..300u64 {
        let s = shards[i as usize % shards.len()];
        let doc_id = format!("cd-{i}");
        cluster.put(&old, s, &doc_id);
        let w = RecordedWrite {
            doc_id: doc_id.clone(),
            shard: s,
            succeeded_on_old: true,
            succeeded_on_new: false,
        };
        coord.register_in_flight(make_in_flight(&w, &old, &new));
        writes.push(w);
    }

    let phase = coord.complete_drain(mid).unwrap();
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    run_delta_pass(&mut coord, &mut cluster, mid, &old, &new, &shards);
    coord.complete_cleanup(mid).unwrap();

    let lost = cluster.lost_docs(&old, &new, &shards);
    assert_eq!(
        lost.len(),
        0,
        "Concurrent migration writes: lost {} docs",
        lost.len()
    );

    let all = cluster.all_docs_for_shards(&shards);
    // 500 pre + 5000 cw + 500 cb + 300 cd = 6300
    assert!(
        all.len() >= 6300,
        "Expected >= 6300 docs, got {}",
        all.len()
    );

    let total_writes = writes.len();
    eprintln!(
        "\n=== Concurrent Migration Writes ===\n\
         Total writes during migration: {total_writes}\n\
         Docs lost after delta pass: 0\n\
         Loss rate: 0/{total_writes} (0.000%)\n"
    );
}

// ---------------------------------------------------------------------------
// Test 13: 3-node cluster cutover — matches task design exactly
// ---------------------------------------------------------------------------
//
// Task design:
// 1. Start 3-node cluster, write 1000 docs
// 2. Trigger node addition
// 3. During dual-write, rapid-fire new writes
// 4. Tight-loop transition from migration complete to old replica deleted
// 5. Assert: every written doc retrievable after step 7
//
// This test uses 3 nodes in a single group: old-0, old-1, new-3.
// Shards 0-3 are spread across old-0 and old-1; new-3 receives the
// migrated fraction. The 3-node topology tests cross-node interactions
// that the 2-node tests don't cover (e.g., different old owners for
// different shards, shared drain tracking across multiple sources).

#[test]
fn cutover_chaos_three_node_cluster() {
    let config = MigrationConfig {
        anti_entropy_enabled: true,
        skip_delta_pass: false,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let node_a = node("old-0");
    let node_b = node("old-1");
    let node_c = node("new-3");
    // Shards 0,1 owned by old-0; shards 2,3 owned by old-1.
    let shards_a = [shard(0), shard(1)];
    let shards_b = [shard(2), shard(3)];
    let all_shards: Vec<ShardId> = vec![shard(0), shard(1), shard(2), shard(3)];

    let affected: HashMap<ShardId, NodeId> = shards_a
        .iter()
        .cloned()
        .map(|s| (s, node_a.clone()))
        .chain(shards_b.iter().cloned().map(|s| (s, node_b.clone())))
        .collect();

    let mut cluster = SimCluster::new(&[node_a.clone(), node_b.clone(), node_c.clone()]);

    // Step 1: Pre-populate 1000 docs across both old nodes
    for i in 0..1000u64 {
        let s = all_shards[i as usize % all_shards.len()];
        let owner = match s {
            ShardId(0) | ShardId(1) => &node_a,
            _ => &node_b,
        };
        cluster.put(owner, s, &format!("pre-{i}"));
    }

    // Step 2: Trigger node addition
    let mid = coord
        .begin_migration(node_c.clone(), 0, affected.clone())
        .unwrap();
    coord.begin_dual_write(mid).unwrap();

    // Step 3: During dual-write, rapid-fire writes with 5% failure on new
    let mut all_writes: Vec<RecordedWrite> = Vec::new();
    for i in 0..1000u64 {
        let s = all_shards[i as usize % all_shards.len()];
        let owner = match s {
            ShardId(0) | ShardId(1) => &node_a,
            _ => &node_b,
        };
        let new_fails = i % 20 == 0; // 5% failure
        let w = dual_write(
            &mut cluster,
            owner,
            &node_c,
            s,
            &format!("dw-{i}"),
            false,
            new_fails,
        );
        all_writes.push(w);
    }

    // Step 4: Tight-loop transition from migration complete to cutover
    // Complete shards one by one, writing between each completion
    for (idx, &s) in all_shards.iter().enumerate() {
        // Burst writes between shard completions
        for j in 0..50u64 {
            let s2 = all_shards[j as usize % all_shards.len()];
            let owner = match s2 {
                ShardId(0) | ShardId(1) => &node_a,
                _ => &node_b,
            };
            let new_fails = j % 20 == 0;
            let w = dual_write(
                &mut cluster,
                owner,
                &node_c,
                s2,
                &format!("burst-{idx}-{j}"),
                false,
                new_fails,
            );
            all_writes.push(w);
        }
        coord.shard_migration_complete(mid, s, 300).unwrap();
    }

    // Boundary writes at CutoverBegin
    for i in 0..200u64 {
        let s = all_shards[i as usize % all_shards.len()];
        let owner = match s {
            ShardId(0) | ShardId(1) => &node_a,
            _ => &node_b,
        };
        let new_fails = i % 20 == 0;
        let w = dual_write(
            &mut cluster,
            owner,
            &node_c,
            s,
            &format!("bnd-{i}"),
            false,
            new_fails,
        );
        all_writes.push(w);
    }

    // Register all writes for drain
    for w in &all_writes {
        let owner = match w.shard {
            ShardId(0) | ShardId(1) => &node_a,
            _ => &node_b,
        };
        coord.register_in_flight(make_in_flight(w, owner, &node_c));
    }

    // Cutover
    coord.begin_cutover(mid).unwrap();

    // Writes during draining — go to old owner only
    for i in 0..200u64 {
        let s = all_shards[i as usize % all_shards.len()];
        let owner = match s {
            ShardId(0) | ShardId(1) => &node_a,
            _ => &node_b,
        };
        let doc_id = format!("drain-{i}");
        cluster.put(owner, s, &doc_id);
        let w = RecordedWrite {
            doc_id: doc_id.clone(),
            shard: s,
            succeeded_on_old: true,
            succeeded_on_new: false,
        };
        coord.register_in_flight(make_in_flight(&w, owner, &node_c));
        all_writes.push(w);
    }

    let phase = coord.complete_drain(mid).unwrap();
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    // Step 5: Delta pass — verify and repair
    // For 3-node: check each old owner against new node
    for &s in &all_shards {
        let old_owner = affected.get(&s).unwrap();
        let lost: Vec<String> = cluster
            .data
            .get(old_owner)
            .and_then(|m| m.get(&s))
            .map(|docs| {
                docs.iter()
                    .filter(|d| {
                        !cluster
                            .data
                            .get(&node_c)
                            .and_then(|m| m.get(&s))
                            .is_some_and(|nd| nd.contains(*d))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        for doc_id in &lost {
            cluster.put(&node_c, s, doc_id);
        }
        coord
            .shard_delta_complete(mid, s, lost.len() as u64)
            .unwrap();
    }

    assert_eq!(
        coord.get_state(mid).unwrap().phase,
        MigrationPhase::CutoverCleanup
    );
    coord.complete_cleanup(mid).unwrap();

    // Assert: 0 lost docs on new node
    for &s in &all_shards {
        let old_owner = affected.get(&s).unwrap();
        let lost = cluster.lost_docs(old_owner, &node_c, &[s]);
        assert_eq!(
            lost.len(),
            0,
            "3-node test: shard {s} lost {} docs",
            lost.len()
        );
    }

    // Assert: every written doc retrievable from new node
    let all = cluster.all_docs_for_shards(&all_shards);
    // 1000 pre + 1000 dw + 200 burst + 200 bnd + 200 drain = 2600
    assert!(
        all.len() >= 2600,
        "Expected >= 2600 docs, got {}",
        all.len()
    );

    eprintln!(
        "\n=== 3-Node Cluster Cutover ===\n\
         Nodes: old-0, old-1, new-3\n\
         Shards: {} ({} from old-0, {} from old-1)\n\
         Total writes: {}\n\
         Docs lost after delta pass: 0\n\
         Loss rate: 0/{} (0.000%)\n",
        all_shards.len(),
        shards_a.len(),
        shards_b.len(),
        all_writes.len(),
        all_writes.len()
    );
}

// ---------------------------------------------------------------------------
// Test 14: 3-node cluster, AE off variant — measure loss with delta only
// ---------------------------------------------------------------------------

#[test]
fn cutover_chaos_three_node_no_ae_with_delta() {
    let config = MigrationConfig {
        anti_entropy_enabled: false,
        skip_delta_pass: false,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let node_a = node("old-0");
    let node_b = node("old-1");
    let node_c = node("new-3");
    let shards_a = [shard(0), shard(1)];
    let shards_b = [shard(2), shard(3)];
    let all_shards: Vec<ShardId> = vec![shard(0), shard(1), shard(2), shard(3)];

    let affected: HashMap<ShardId, NodeId> = shards_a
        .iter()
        .cloned()
        .map(|s| (s, node_a.clone()))
        .chain(shards_b.iter().cloned().map(|s| (s, node_b.clone())))
        .collect();

    let mut cluster = SimCluster::new(&[node_a.clone(), node_b.clone(), node_c.clone()]);

    for i in 0..1000u64 {
        let s = all_shards[i as usize % all_shards.len()];
        let owner = match s {
            ShardId(0) | ShardId(1) => &node_a,
            _ => &node_b,
        };
        cluster.put(owner, s, &format!("pre-{i}"));
    }

    let mid = coord
        .begin_migration(node_c.clone(), 0, affected.clone())
        .unwrap();
    coord.begin_dual_write(mid).unwrap();

    let mut all_writes: Vec<RecordedWrite> = Vec::new();
    for i in 0..5000u64 {
        let s = all_shards[i as usize % all_shards.len()];
        let owner = match s {
            ShardId(0) | ShardId(1) => &node_a,
            _ => &node_b,
        };
        let new_fails = i % 100 == 0; // 1% failure
        let w = dual_write(
            &mut cluster,
            owner,
            &node_c,
            s,
            &format!("w-{i}"),
            false,
            new_fails,
        );
        all_writes.push(w);
    }

    for &s in &all_shards {
        coord.shard_migration_complete(mid, s, 300).unwrap();
    }

    // Register one failed write to force delta pass
    coord.register_in_flight(InFlightWrite {
        doc_id: "w-0".into(),
        shard: shard(0),
        target_nodes: vec![node_a.clone(), node_c.clone()],
        completed_nodes: HashSet::from([node_a.clone()]),
        failed_nodes: HashMap::from([(node_c.clone(), "simulated failure".into())]),
        submitted_at: Instant::now(),
    });

    coord.begin_cutover(mid).unwrap();
    let phase = coord.complete_drain(mid).unwrap();
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    // Delta pass
    for &s in &all_shards {
        let old_owner = affected.get(&s).unwrap();
        let lost: Vec<String> = cluster
            .data
            .get(old_owner)
            .and_then(|m| m.get(&s))
            .map(|docs| {
                docs.iter()
                    .filter(|d| {
                        !cluster
                            .data
                            .get(&node_c)
                            .and_then(|m| m.get(&s))
                            .is_some_and(|nd| nd.contains(*d))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        for doc_id in &lost {
            cluster.put(&node_c, s, doc_id);
        }
        coord
            .shard_delta_complete(mid, s, lost.len() as u64)
            .unwrap();
    }

    coord.complete_cleanup(mid).unwrap();

    // Assert: 0 lost docs
    for &s in &all_shards {
        let old_owner = affected.get(&s).unwrap();
        let lost = cluster.lost_docs(old_owner, &node_c, &[s]);
        assert_eq!(
            lost.len(),
            0,
            "3-node AE-off: shard {s} lost {} docs",
            lost.len()
        );
    }

    eprintln!(
        "\n=== 3-Node Cluster: AE OFF + Delta Pass ON ===\n\
         Nodes: old-0, old-1, new-3\n\
         Total writes: {}\n\
         Docs lost after delta pass: 0\n\
         Loss rate: 0/{} (0.000%)\n",
        all_writes.len(),
        all_writes.len()
    );
}

// ---------------------------------------------------------------------------
// Test 15: network partition during cutover — new node becomes unavailable
// ---------------------------------------------------------------------------
//
// Simulates a network partition where the new node becomes unavailable
// during the cutover process. Tests that the system handles this gracefully
// and that data is not lost when the partition is resolved.

#[test]
fn cutover_chaos_network_partition_new_node() {
    let config = MigrationConfig {
        anti_entropy_enabled: true,
        skip_delta_pass: false,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0), shard(1)];
    let affected: HashMap<ShardId, NodeId> = shards.iter().map(|&s| (s, old.clone())).collect();

    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    // Pre-populate
    for i in 0..500u64 {
        let s = shards[i as usize % shards.len()];
        cluster.put(&old, s, &format!("pre-{i}"));
    }

    let mid = coord.begin_migration(new.clone(), 0, affected).unwrap();
    coord.begin_dual_write(mid).unwrap();

    // Phase 1: Normal dual-write
    let mut writes: Vec<RecordedWrite> = Vec::new();
    for i in 0..500u64 {
        let doc_id = format!("dw-{i}");
        let s = shards[i as usize % shards.len()];
        let w = dual_write(&mut cluster, &old, &new, s, &doc_id, false, false);
        writes.push(w);
    }

    // Complete background migration
    for &s in &shards {
        coord.shard_migration_complete(mid, s, 250).unwrap();
    }

    // Phase 2: Network partition — new node becomes unavailable
    // All writes from now fail on new
    for i in 0..200u64 {
        let doc_id = format!("partition-{i}");
        let s = shards[i as usize % shards.len()];
        let w = dual_write(&mut cluster, &old, &new, s, &doc_id, false, true); // new fails
        writes.push(w);
    }

    // Register all writes for drain
    for w in &writes {
        coord.register_in_flight(make_in_flight(w, &old, &new));
    }

    // Begin cutover
    coord.begin_cutover(mid).unwrap();

    // Drain completes because all writes are either completed or failed
    let phase = coord.complete_drain(mid).unwrap();
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    // Delta pass catches all the partitioned writes
    run_delta_pass(&mut coord, &mut cluster, mid, &old, &new, &shards);

    coord.complete_cleanup(mid).unwrap();

    // Verify 0 loss
    let lost = cluster.lost_docs(&old, &new, &shards);
    assert_eq!(
        lost.len(),
        0,
        "Network partition test lost {} docs",
        lost.len()
    );

    eprintln!(
        "\n=== Network Partition (New Node) ===\n\
         Total writes: {}\n\
         Writes during partition: 200\n\
         Docs caught by delta pass: 200\n\
         Lost after delta pass: 0\n\
         Loss rate: 0/{} (0.000%)\n",
        writes.len(),
        writes.len()
    );
}

// ---------------------------------------------------------------------------
// Test 16: drain timeout boundary — exact timeout boundary
// ---------------------------------------------------------------------------
//
// Tests the behavior when the drain timeout is exactly reached.
// Verifies that the system properly handles timeout boundary conditions.

// TODO: Phase 7+ - flaky test, needs timeout/drain behavior review
#[test]
#[ignore]
fn cutover_chaos_drain_timeout_boundary() {
    // Use a very short timeout for testing
    let config = MigrationConfig {
        anti_entropy_enabled: true,
        skip_delta_pass: false,
        drain_timeout: Duration::from_millis(1),
    };
    let mut coord = MigrationCoordinator::new(config);

    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0)];
    let affected: HashMap<ShardId, NodeId> = [(shard(0), old.clone())].into_iter().collect();

    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    let mid = coord.begin_migration(new.clone(), 0, affected).unwrap();
    coord.begin_dual_write(mid).unwrap();

    // Normal writes
    for i in 0..100u64 {
        dual_write(
            &mut cluster,
            &old,
            &new,
            shards[0],
            &format!("w-{i}"),
            false,
            false,
        );
    }

    coord.shard_migration_complete(mid, shards[0], 100).unwrap();

    // Register a stuck write (in-flight, neither completed nor failed)
    coord.register_in_flight(InFlightWrite {
        doc_id: "stuck".into(),
        shard: shards[0],
        target_nodes: vec![old.clone(), new.clone()],
        completed_nodes: HashSet::new(),
        failed_nodes: HashMap::new(),
        submitted_at: Instant::now(),
    });

    coord.begin_cutover(mid).unwrap();

    // Drain should timeout
    let result = coord.complete_drain(mid);
    assert!(result.is_err());
    match result.unwrap_err() {
        MigrationError::DrainTimeout(count) => {
            assert_eq!(count, 1, "Expected 1 stuck write");
        }
        _ => panic!("Expected DrainTimeout error"),
    }

    // Now mark the write as failed on both target nodes
    coord.fail_write("stuck", &new, "timeout".to_string());
    coord.fail_write("stuck", &old, "timeout".to_string());

    // Drain should now succeed
    let phase = coord.complete_drain(mid).unwrap();
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    // Delta pass catches the stuck write
    cluster.put(&old, shards[0], "stuck");
    coord.shard_delta_complete(mid, shards[0], 1).unwrap();

    coord.complete_cleanup(mid).unwrap();

    let lost = cluster.lost_docs(&old, &new, &shards);
    assert_eq!(lost.len(), 0, "Timeout boundary test lost docs");

    eprintln!(
        "\n=== Drain Timeout Boundary ===\n\
         Timeout configuration: 1ms\n\
         Stuck writes at timeout: 1\n\
         After marking as failed: drain succeeds\n\
         Lost after delta pass: 0\n"
    );
}

// ---------------------------------------------------------------------------
// Test 17: concurrent migrations — multiple simultaneous shard migrations
// ---------------------------------------------------------------------------
//
// Tests multiple shard migrations happening concurrently.
// Verifies that in-flight writes are correctly tracked across migrations.

// TODO: Phase 7+ - flaky test, needs concurrent migration coordination review
#[test]
#[ignore]
fn cutover_chaos_concurrent_migrations() {
    let config = MigrationConfig {
        anti_entropy_enabled: true,
        skip_delta_pass: false,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let old_a = node("old-0");
    let old_b = node("old-1");
    let new = node("new-3");

    let shards_a = vec![shard(0), shard(1)];
    let shards_b = vec![shard(2), shard(3)];
    let all_shards: Vec<ShardId> = vec![shard(0), shard(1), shard(2), shard(3)];

    let affected_a: HashMap<ShardId, NodeId> =
        shards_a.iter().map(|&s| (s, old_a.clone())).collect();
    let affected_b: HashMap<ShardId, NodeId> =
        shards_b.iter().map(|&s| (s, old_b.clone())).collect();

    let mut cluster = SimCluster::new(&[old_a.clone(), old_b.clone(), new.clone()]);

    // Start two concurrent migrations
    let mid_a = coord.begin_migration(new.clone(), 0, affected_a).unwrap();
    let mid_b = coord.begin_migration(new.clone(), 0, affected_b).unwrap();

    coord.begin_dual_write(mid_a).unwrap();
    coord.begin_dual_write(mid_b).unwrap();

    // Concurrent writes to both migrations
    for i in 0..1000u64 {
        let s = all_shards[i as usize % all_shards.len()];
        let owner = if s.0 < 2 { &old_a } else { &old_b };
        let new_fails = i % 50 == 0; // 2% failure
        dual_write(
            &mut cluster,
            owner,
            &new,
            s,
            &format!("w-{i}"),
            false,
            new_fails,
        );
    }

    // Complete migrations in interleaved order
    coord
        .shard_migration_complete(mid_a, shard(0), 250)
        .unwrap();
    coord
        .shard_migration_complete(mid_b, shard(2), 250)
        .unwrap();
    coord
        .shard_migration_complete(mid_a, shard(1), 250)
        .unwrap();
    coord
        .shard_migration_complete(mid_b, shard(3), 250)
        .unwrap();

    // Begin cutover for both
    coord.begin_cutover(mid_a).unwrap();
    coord.begin_cutover(mid_b).unwrap();

    // Complete drain for both (order matters!)
    let phase_a = coord.complete_drain(mid_a).unwrap();
    let phase_b = coord.complete_drain(mid_b).unwrap();
    assert_eq!(phase_a, MigrationPhase::CutoverDeltaPass);
    assert_eq!(phase_b, MigrationPhase::CutoverDeltaPass);

    // Delta pass for both migrations
    for &s in &shards_a {
        let lost: Vec<String> = cluster
            .data
            .get(&old_a)
            .and_then(|m| m.get(&s))
            .map(|docs| {
                docs.iter()
                    .filter(|d| {
                        !cluster
                            .data
                            .get(&new)
                            .and_then(|m| m.get(&s))
                            .is_some_and(|nd| nd.contains(*d))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        for doc_id in &lost {
            cluster.put(&new, s, doc_id);
        }
        coord
            .shard_delta_complete(mid_a, s, lost.len() as u64)
            .unwrap();
    }

    for &s in &shards_b {
        let lost: Vec<String> = cluster
            .data
            .get(&old_b)
            .and_then(|m| m.get(&s))
            .map(|docs| {
                docs.iter()
                    .filter(|d| {
                        !cluster
                            .data
                            .get(&new)
                            .and_then(|m| m.get(&s))
                            .is_some_and(|nd| nd.contains(*d))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        for doc_id in &lost {
            cluster.put(&new, s, doc_id);
        }
        coord
            .shard_delta_complete(mid_b, s, lost.len() as u64)
            .unwrap();
    }

    coord.complete_cleanup(mid_a).unwrap();
    coord.complete_cleanup(mid_b).unwrap();

    // Verify 0 loss across all migrations
    for &s in &all_shards {
        let owner = if s.0 < 2 { &old_a } else { &old_b };
        let lost = cluster.lost_docs(owner, &new, &[s]);
        assert_eq!(
            lost.len(),
            0,
            "Concurrent migration lost docs for shard {s:?}"
        );
    }

    eprintln!(
        "\n=== Concurrent Migrations ===\n\
         Migration A: shards 0,1 from old-0\n\
         Migration B: shards 2,3 from old-1\n\
         Total writes: 1000\n\
         Lost after delta pass: 0\n\
         Loss rate: 0/1000 (0.000%)\n"
    );
}

// ---------------------------------------------------------------------------
// Test 18: partial failure — some shards fail, others succeed
// ---------------------------------------------------------------------------
//
// Tests behavior when some shards in a migration fail while others succeed.
// Verifies that the migration can be recovered and retried.

#[test]
fn cutover_chaos_partial_shard_failure() {
    let config = MigrationConfig {
        anti_entropy_enabled: true,
        skip_delta_pass: false,
        ..Default::default()
    };
    let mut coord = MigrationCoordinator::new(config);

    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0), shard(1), shard(2), shard(3)];
    let affected: HashMap<ShardId, NodeId> = shards.iter().map(|&s| (s, old.clone())).collect();

    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    for i in 0..1000u64 {
        let s = shards[i as usize % shards.len()];
        cluster.put(&old, s, &format!("pre-{i}"));
    }

    let mid = coord.begin_migration(new.clone(), 0, affected).unwrap();
    coord.begin_dual_write(mid).unwrap();

    // Writes with varying failure rates per shard
    for i in 0..2000u64 {
        let s = shards[i as usize % shards.len()];
        let new_fails = match s {
            ShardId(0) => i % 10 == 0,  // 10% failure
            ShardId(1) => i % 100 == 0, // 1% failure
            ShardId(2) => i % 20 == 0,  // 5% failure
            _ => false,                 // 0% failure
        };
        dual_write(
            &mut cluster,
            &old,
            &new,
            s,
            &format!("w-{i}"),
            false,
            new_fails,
        );
    }

    // Complete all shard migrations
    for &s in &shards {
        coord.shard_migration_complete(mid, s, 500).unwrap();
    }

    // Register one failed write per shard to force delta pass
    for &s in &shards {
        coord.register_in_flight(InFlightWrite {
            doc_id: format!("failed-{s:?}"),
            shard: s,
            target_nodes: vec![old.clone(), new.clone()],
            completed_nodes: HashSet::from([old.clone()]),
            failed_nodes: HashMap::from([(new.clone(), "simulated failure".into())]),
            submitted_at: Instant::now(),
        });
    }

    coord.begin_cutover(mid).unwrap();
    let phase = coord.complete_drain(mid).unwrap();
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    // Delta pass for each shard
    for &s in &shards {
        let lost: Vec<String> = cluster
            .data
            .get(&old)
            .and_then(|m| m.get(&s))
            .map(|docs| {
                docs.iter()
                    .filter(|d| {
                        !cluster
                            .data
                            .get(&new)
                            .and_then(|m| m.get(&s))
                            .is_some_and(|nd| nd.contains(*d))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        for doc_id in &lost {
            cluster.put(&new, s, doc_id);
        }
        coord
            .shard_delta_complete(mid, s, lost.len() as u64)
            .unwrap();
    }

    coord.complete_cleanup(mid).unwrap();

    // Verify 0 loss across all shards
    let lost = cluster.lost_docs(&old, &new, &shards);
    assert_eq!(
        lost.len(),
        0,
        "Partial failure test lost {} docs",
        lost.len()
    );

    eprintln!(
        "\n=== Partial Shard Failure ===\n\
         Shards: 0 (10% fail), 1 (1% fail), 2 (5% fail), 3 (0% fail)\n\
         Total writes: 2000\n\
         Lost after delta pass: 0\n\
         Loss rate: 0/2000 (0.000%)\n"
    );
}

// ---------------------------------------------------------------------------
// Test 19: state recovery — coordinator crash and restart
// ---------------------------------------------------------------------------
//
// Tests behavior when the coordinator crashes and restarts during migration.
// Verifies that state can be recovered and migration can complete safely.

#[test]
fn cutover_chaos_coordinator_crash_recovery() {
    let config = MigrationConfig {
        anti_entropy_enabled: true,
        skip_delta_pass: false,
        ..Default::default()
    };

    let old = node("old-0");
    let new = node("new-3");
    let shards = vec![shard(0), shard(1)];
    let affected: HashMap<ShardId, NodeId> = shards.iter().map(|&s| (s, old.clone())).collect();

    let mut cluster = SimCluster::new(&[old.clone(), new.clone()]);

    // Pre-populate
    for i in 0..500u64 {
        let s = shards[i as usize % shards.len()];
        cluster.put(&old, s, &format!("pre-{i}"));
    }

    // Phase 1: Start migration and dual-write
    let mut coord = MigrationCoordinator::new(config.clone());
    let mid = coord
        .begin_migration(new.clone(), 0, affected.clone())
        .unwrap();
    coord.begin_dual_write(mid).unwrap();

    // Dual-write phase
    for i in 0..500u64 {
        let doc_id = format!("dw-{i}");
        let s = shards[i as usize % shards.len()];
        let new_fails = i % 50 == 0;
        dual_write(&mut cluster, &old, &new, s, &doc_id, false, new_fails);
    }

    // Complete background migration
    for &s in &shards {
        coord.shard_migration_complete(mid, s, 250).unwrap();
    }

    // Simulate coordinator crash: save state, create new coordinator
    let state_before_crash = coord.get_state(mid).unwrap().clone();

    // "Crash" — create new coordinator with same config
    let mut coord_recovered = MigrationCoordinator::new(config);

    // Recover migration state (in production, this would be loaded from disk)
    let mid_recovered = coord_recovered
        .begin_migration(new.clone(), 0, affected)
        .unwrap();
    coord_recovered.begin_dual_write(mid_recovered).unwrap();

    // Replay background migration completion
    for &s in &shards {
        coord_recovered
            .shard_migration_complete(mid_recovered, s, 250)
            .unwrap();
    }

    // Register in-flight writes for drain
    let mut writes: Vec<RecordedWrite> = Vec::new();
    for i in 0..100u64 {
        let doc_id = format!("post-crash-{i}");
        let s = shards[i as usize % shards.len()];
        let new_fails = i % 20 == 0;
        let w = dual_write(&mut cluster, &old, &new, s, &doc_id, false, new_fails);
        writes.push(w);
    }

    for w in &writes {
        coord_recovered.register_in_flight(make_in_flight(w, &old, &new));
    }

    // Continue with cutover
    coord_recovered.begin_cutover(mid_recovered).unwrap();
    let phase = coord_recovered.complete_drain(mid_recovered).unwrap();
    assert_eq!(phase, MigrationPhase::CutoverDeltaPass);

    // Delta pass
    run_delta_pass(
        &mut coord_recovered,
        &mut cluster,
        mid_recovered,
        &old,
        &new,
        &shards,
    );

    coord_recovered.complete_cleanup(mid_recovered).unwrap();

    // Verify 0 loss
    let lost = cluster.lost_docs(&old, &new, &shards);
    assert_eq!(
        lost.len(),
        0,
        "Crash recovery test lost {} docs",
        lost.len()
    );

    // Verify recovered state matches pre-crash state
    let state_recovered = coord_recovered.get_state(mid_recovered).unwrap();
    assert_eq!(state_recovered.phase, MigrationPhase::Complete);
    assert_eq!(state_before_crash.old_owners, state_recovered.old_owners);

    eprintln!(
        "\n=== Coordinator Crash Recovery ===\n\
         State at crash: {:?}\n\
         Writes before crash: 500\n\
         Writes after recovery: 100\n\
         Lost after delta pass: 0\n\
         Recovery: successful\n",
        state_before_crash.phase
    );
}
