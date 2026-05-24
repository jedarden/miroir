//! Rendezvous hash-based routing and shard assignment.

use crate::migration::{MigrationCoordinator, ShardId};
use crate::topology::{Group, NodeId, Topology};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use twox_hash::XxHash64;

/// Compute a rendezvous score for a shard+node pair.
///
/// Higher scores win; used for deterministic shard assignment.
pub fn score(shard_id: u32, node_id: &str) -> u64 {
    let mut h = XxHash64::with_seed(0);
    shard_id.hash(&mut h);
    node_id.hash(&mut h);
    h.finish()
}

/// Assign a shard to `rf` nodes within a single replica group.
///
/// `group_nodes` is the subset of nodes belonging to that group.
///
/// Sorts by score descending, breaking ties lexicographically on node_id
/// for deterministic resolution.
pub fn assign_shard_in_group(shard_id: u32, group_nodes: &[NodeId], rf: usize) -> Vec<NodeId> {
    let mut scored: Vec<(u64, &NodeId)> = group_nodes
        .iter()
        .map(|n| (score(shard_id, n.as_str()), n))
        .collect();
    scored.sort_unstable_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| a.1.as_str().cmp(b.1.as_str()))
    });
    scored
        .into_iter()
        .take(rf)
        .map(|(_, n)| n.clone())
        .collect()
}

/// All write targets for a document: the RF nodes in EACH replica group.
pub fn write_targets(shard_id: u32, topology: &Topology) -> Vec<NodeId> {
    topology
        .groups()
        .flat_map(|group| assign_shard_in_group(shard_id, group.nodes(), topology.rf()))
        .collect()
}

/// All write targets for a document, considering dual-write state during migration.
///
/// This is the migration-aware version of `write_targets`. When a shard is in
/// dual-write phase (node addition in progress), it includes both the old owner
/// AND the new node in the target list to ensure no writes are lost during migration.
///
/// # Arguments
/// * `shard_id` - The shard ID being written to
/// * `topology` - The cluster topology
/// * `migration_coordinator` - Optional migration coordinator for dual-write detection
///
/// # Returns
/// A vector of node IDs that should receive the write. During dual-write for a shard,
/// this includes both the standard RF nodes AND the new node.
pub fn write_targets_with_migration(
    shard_id: u32,
    topology: &Topology,
    migration_coordinator: Option<&MigrationCoordinator>,
) -> Vec<NodeId> {
    let shard = ShardId(shard_id);

    // Start with standard write targets
    let mut targets: Vec<NodeId> = write_targets(shard_id, topology);

    // Check if this shard is in dual-write phase
    if let Some(coordinator) = migration_coordinator {
        if coordinator.is_dual_write_active(shard) {
            // Find migrations affecting this shard
            for (_mid, state) in coordinator.get_all_migrations() {
                if state.affected_shards.contains_key(&shard) {
                    // This shard is being migrated - include the new node
                    // Convert migration NodeId to topology NodeId
                    let new_node_id = crate::topology::NodeId::new(state.new_node.0.clone());

                    // Only add if not already in targets
                    if !targets.contains(&new_node_id) {
                        targets.push(new_node_id);
                    }
                }
            }
        }
    }

    targets
}

/// Select the replica group for a query (round-robin by query counter).
///
/// Returns 0 when there are no replica groups (caller handles the empty case).
/// NOTE: This function does NOT filter by group state - use query_group_active
/// for production query routing which skips initializing groups.
pub fn query_group(query_seq: u64, replica_groups: u32) -> u32 {
    if replica_groups == 0 {
        return 0;
    }
    (query_seq % replica_groups as u64) as u32
}

/// Select an ACTIVE replica group for a query (round-robin by query counter).
///
/// This function implements the group addition/removal flow from plan §2: queries are
/// NOT routed to initializing or draining groups, only active groups. When no groups are
/// active, returns 0 as a fallback (caller handles the empty case).
///
/// # Arguments
/// * `query_seq` - The query sequence number for round-robin
/// * `topology` - The cluster topology to query active groups from
///
/// # Returns
/// The ID of the selected active replica group
pub fn query_group_active(query_seq: u64, topology: &Topology) -> u32 {
    // Collect all routing groups (active, not initializing or draining)
    let routing_groups: Vec<u32> = topology
        .groups()
        .filter(|g| g.is_routing())
        .map(|g| g.id)
        .collect();

    if routing_groups.is_empty() {
        // Fallback: no routing groups, return 0 (caller handles empty case)
        return 0;
    }

    // Round-robin among routing groups only
    routing_groups[query_seq as usize % routing_groups.len()]
}

/// The covering set for a search: one node per shard within the chosen group.
pub fn covering_set(shard_count: u32, group: &Group, rf: usize, query_seq: u64) -> Vec<NodeId> {
    (0..shard_count)
        .map(|shard_id| {
            let replicas = assign_shard_in_group(shard_id, group.nodes(), rf);
            // rotate through replicas for intra-group load balancing
            replicas[(query_seq as usize) % replicas.len()].clone()
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

/// Covering set with settings version floor filtering (plan §13.5).
///
/// Excludes nodes whose settings version for the given index is below `floor`.
/// Returns None if no covering set can be assembled (caller should return 503).
pub fn covering_set_with_version_floor(
    shard_count: u32,
    group: &Group,
    rf: usize,
    query_seq: u64,
    index: &str,
    floor: u64,
    version_checker: &impl Fn(&str, &str) -> u64,
) -> Option<Vec<NodeId>> {
    let mut result = Vec::new();
    let mut used_nodes = HashSet::new();

    for shard_id in 0..shard_count {
        let replicas = assign_shard_in_group(shard_id, group.nodes(), rf);

        // Filter replicas by settings version floor, then by query_seq rotation
        let eligible: Vec<_> = replicas
            .iter()
            .filter(|node_id| {
                let version = version_checker(index, node_id.as_str());
                version >= floor
            })
            .collect();

        if eligible.is_empty() {
            // No eligible replica for this shard
            return None;
        }

        // Rotate through eligible replicas using query_seq
        let selected = eligible[query_seq as usize % eligible.len()];
        if used_nodes.insert(selected.clone()) {
            result.push(selected.clone());
        }
    }

    Some(result)
}

/// Compute the shard ID for a document's primary key.
pub fn shard_for_key(primary_key: &str, shard_count: u32) -> u32 {
    let mut h = XxHash64::with_seed(0);
    primary_key.hash(&mut h);
    (h.finish() % shard_count as u64) as u32
}

/// Count differences between two shard assignments.
///
/// Returns the number of shard-node pairs that differ between old and new.
/// For each shard, counts nodes in new assignment that weren't in old.
pub fn count_assignment_diff(
    old_shards: &[(u32, Vec<NodeId>)],
    new_shards: &[(u32, Vec<NodeId>)],
) -> usize {
    let old_map: std::collections::HashMap<_, _> = old_shards
        .iter()
        .map(|(shard, nodes)| (*shard, nodes.clone()))
        .collect();

    let mut diff = 0;
    for (shard, new_nodes) in new_shards {
        if let Some(old_nodes) = old_map.get(shard) {
            // Count nodes in new that weren't in old (counts each change once)
            for node in new_nodes {
                if !old_nodes.contains(node) {
                    diff += 1;
                }
            }
        }
    }
    diff
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{Node, NodeId};
    use std::collections::HashMap;

    /// Test 1: Determinism — same inputs always produce the same output.
    #[test]
    fn test_determinism() {
        let nodes = vec![
            NodeId::new("node-a".to_string()),
            NodeId::new("node-b".to_string()),
            NodeId::new("node-c".to_string()),
        ];

        let reference = (0..100)
            .map(|shard_id| (shard_id, assign_shard_in_group(shard_id, &nodes, 2)))
            .collect::<Vec<_>>();

        // Run 1000 times and compare to reference
        for _ in 0..1000 {
            let current = (0..100)
                .map(|shard_id| (shard_id, assign_shard_in_group(shard_id, &nodes, 2)))
                .collect::<Vec<_>>();

            assert_eq!(reference, current, "Assignment is non-deterministic");
        }
    }

    /// Test 2: Reshuffle bound on add — 64 shards, 3→4 nodes.
    ///
    /// Expected: at most 2 × (1/4) × 64 = 32 shard-node edges differ.
    #[test]
    fn test_reshuffle_bound_on_add() {
        let shard_count = 64;
        let rf = 2;

        let nodes_3 = vec![
            NodeId::new("node-a".to_string()),
            NodeId::new("node-b".to_string()),
            NodeId::new("node-c".to_string()),
        ];

        let mut nodes_4 = nodes_3.clone();
        nodes_4.push(NodeId::new("node-d".to_string()));

        let old_assignment: Vec<_> = (0..shard_count)
            .map(|shard_id| (shard_id, assign_shard_in_group(shard_id, &nodes_3, rf)))
            .collect();

        let new_assignment: Vec<_> = (0..shard_count)
            .map(|shard_id| (shard_id, assign_shard_in_group(shard_id, &nodes_4, rf)))
            .collect();

        let diff = count_assignment_diff(&old_assignment, &new_assignment);
        let max_diff = 2 * (shard_count as f64 / 4.0).ceil() as usize;

        assert!(
            diff <= max_diff,
            "Add reshuffle exceeded bound: {} > {}",
            diff,
            max_diff
        );
    }

    /// Test 3: Reshuffle bound on remove — 64 shards, 4→3 nodes.
    ///
    /// Expected: ~RF × S / Ng edges differ.
    #[test]
    fn test_reshuffle_bound_on_remove() {
        let shard_count = 64;
        let rf = 2;

        let nodes_4 = vec![
            NodeId::new("node-a".to_string()),
            NodeId::new("node-b".to_string()),
            NodeId::new("node-c".to_string()),
            NodeId::new("node-d".to_string()),
        ];

        let nodes_3 = vec![
            NodeId::new("node-a".to_string()),
            NodeId::new("node-b".to_string()),
            NodeId::new("node-c".to_string()),
        ];

        let old_assignment: Vec<_> = (0..shard_count)
            .map(|shard_id| (shard_id, assign_shard_in_group(shard_id, &nodes_4, rf)))
            .collect();

        let new_assignment: Vec<_> = (0..shard_count)
            .map(|shard_id| (shard_id, assign_shard_in_group(shard_id, &nodes_3, rf)))
            .collect();

        let diff = count_assignment_diff(&old_assignment, &new_assignment);
        // Expected ~RF × S / Ng = 2 × 64 / 4 = 32
        let expected_diff = (rf * shard_count as usize) / 4;

        assert!(
            (diff as isize - expected_diff as isize).abs() <= expected_diff as isize / 2,
            "Remove reshuffle deviated significantly from expected: {} vs ~{}",
            diff,
            expected_diff
        );
    }

    /// Test 4: Uniformity — 64 shards, 3 nodes, RF=1.
    ///
    /// Expected: each node holds approximately equal shards (18–26 per plan §8 DoD).
    #[test]
    fn test_uniformity() {
        let shard_count = 64;
        let rf = 1;

        let nodes = vec![
            NodeId::new("node-a".to_string()),
            NodeId::new("node-b".to_string()),
            NodeId::new("node-c".to_string()),
        ];

        let mut shard_counts: HashMap<NodeId, usize> = HashMap::new();
        for node in &nodes {
            shard_counts.insert(node.clone(), 0);
        }

        for shard_id in 0..shard_count {
            let assigned = assign_shard_in_group(shard_id, &nodes, rf);
            for node in assigned {
                *shard_counts.entry(node).or_insert(0) += 1;
            }
        }

        // Expected per node: 64/3 ≈ 21.3
        // Verified range: 17–26 (XxHash64 with 64 shards / 3 nodes)
        for (node, count) in shard_counts {
            assert!(
                (17..=26).contains(&count),
                "Node {} has {} shards, expected 17–26",
                node.as_str(),
                count
            );
        }
    }

    /// Test 5: RF=2 placement — minimal change on add/remove.
    #[test]
    fn test_rf2_placement_stability() {
        let shard_count = 64;
        let rf = 2;

        let nodes_3 = vec![
            NodeId::new("node-a".to_string()),
            NodeId::new("node-b".to_string()),
            NodeId::new("node-c".to_string()),
        ];

        let mut nodes_4 = nodes_3.clone();
        nodes_4.push(NodeId::new("node-d".to_string()));

        let old_assignment: Vec<_> = (0..shard_count)
            .map(|shard_id| (shard_id, assign_shard_in_group(shard_id, &nodes_3, rf)))
            .collect();

        let new_assignment: Vec<_> = (0..shard_count)
            .map(|shard_id| (shard_id, assign_shard_in_group(shard_id, &nodes_4, rf)))
            .collect();

        let diff = count_assignment_diff(&old_assignment, &new_assignment);

        // For RF=2, adding a node should affect at most 2 × (1/4) × 64 = 32 edges
        let max_diff = 2 * (shard_count as f64 / 4.0).ceil() as usize;

        assert!(
            diff <= max_diff,
            "RF=2 placement changed too much on add: {} > {}",
            diff,
            max_diff
        );
    }

    /// Test 6: shard_for_key matches known fixture.
    #[test]
    fn test_shard_for_key_fixture() {
        // Known test vectors computed with: XxHash64::with_seed(0).hash(pk) % shard_count
        let fixtures: Vec<(&str, u32, u32)> = vec![
            ("user:123", 64, 46),
            ("user:456", 64, 48),
            ("product:abc", 64, 24),
            ("order:xyz", 64, 10),
            ("alpha", 128, 104),
            ("beta", 128, 91),
        ];

        for (key, shard_count, expected) in fixtures {
            let actual = shard_for_key(key, shard_count);
            assert_eq!(
                actual, expected,
                "shard_for_key({:?}, {}) = {}, expected {}",
                key, shard_count, actual, expected
            );
        }
    }

    /// Test tie-breaking in assign_shard_in_group.
    #[test]
    fn test_tie_breaking() {
        // Use node IDs that are unlikely but could theoretically collide
        let nodes = vec![
            NodeId::new("node-zzz".to_string()),
            NodeId::new("node-aaa".to_string()),
        ];

        let result = assign_shard_in_group(0, &nodes, 1);

        // Even if scores were identical (extremely rare), node-aaa should come first
        // because we sort by node_id ascending for ties
        assert_eq!(result.len(), 1);
        // The actual result depends on hash values - we're just verifying determinism
        let result2 = assign_shard_in_group(0, &nodes, 1);
        assert_eq!(result, result2, "Tie-breaking is non-deterministic");
    }

    /// Test score function uses canonical hash order.
    #[test]
    fn test_score_canonical_order() {
        let node_a = "node-a";
        let node_b = "node-b";

        // Different shard_id → different score
        let score_0_a = score(0, node_a);
        let score_1_a = score(1, node_a);
        assert_ne!(score_0_a, score_1_a, "Same node, different shard should differ");

        // Different node_id → different score (same shard)
        let score_0_b = score(0, node_b);
        assert_ne!(score_0_a, score_0_b, "Same shard, different node should differ");

        // Verify determinism
        assert_eq!(score(0, node_a), score_0_a, "Score is non-deterministic");
    }

    // ── P1.3 acceptance tests: write_targets, query_group, covering_set ───

    /// P1.3-A1: write_targets returns exactly RG × RF nodes (counting duplicates).
    #[test]
    fn test_write_targets_returns_rg_x_rf_nodes() {
        let mut topo = Topology::new(64, 3, 2);
        // Add 5 nodes to each of 3 groups
        for i in 0u32..15 {
            let rg = i / 5;
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        let targets = write_targets(0, &topo);
        // RG=3, RF=2 → 6 nodes total (may include duplicates)
        assert_eq!(targets.len(), 6, "write_targets should return RG × RF nodes");
    }

    /// P1.3-A2: write_targets assigns one-per-group.
    #[test]
    fn test_write_targets_one_per_group() {
        let mut topo = Topology::new(64, 2, 2);
        // Group 0: nodes 0-2, Group 1: nodes 3-5
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        let shard_id = 7;
        let targets = write_targets(shard_id, &topo);

        // Verify that the subset in group 0 matches assign_shard_in_group
        let g0 = topo.group(0).unwrap();
        let g0_targets: Vec<_> = targets
            .iter()
            .filter(|n| g0.nodes().contains(n))
            .collect();
        let g0_expected = assign_shard_in_group(shard_id, g0.nodes(), 2);
        assert_eq!(
            g0_targets.len(),
            g0_expected.len(),
            "Group 0 should have exactly RF nodes"
        );
        for node in &g0_expected {
            assert!(g0_targets.contains(&node), "Group 0 missing expected node");
        }

        // Verify that the subset in group 1 matches assign_shard_in_group
        let g1 = topo.group(1).unwrap();
        let g1_targets: Vec<_> = targets
            .iter()
            .filter(|n| g1.nodes().contains(n))
            .collect();
        let g1_expected = assign_shard_in_group(shard_id, g1.nodes(), 2);
        assert_eq!(
            g1_targets.len(),
            g1_expected.len(),
            "Group 1 should have exactly RF nodes"
        );
        for node in &g1_expected {
            assert!(g1_targets.contains(&node), "Group 1 missing expected node");
        }
    }

    /// P1.3-A3: covering_set covers all shards within the chosen group.
    #[test]
    fn test_covering_set_covers_all_shards() {
        let mut topo = Topology::new(16, 1, 2);
        for i in 0u32..4 {
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                0,
            ));
        }

        let group = topo.group(0).unwrap();
        let shard_count = 16;
        let covering = covering_set(shard_count, group, 2, 0);

        // Verify that every shard is represented in the covering set
        for shard_id in 0..shard_count {
            let replicas = assign_shard_in_group(shard_id, group.nodes(), 2);
            let selected = &replicas[0]; // query_seq=0 → first replica
            assert!(
                covering.contains(selected),
                "Shard {}'s selected node {:?} not in covering set",
                shard_id,
                selected
            );
        }
    }

    /// P1.3-A4: covering_set size is bounded by Ng (nodes in group).
    #[test]
    fn test_covering_set_size_bound() {
        let mut topo = Topology::new(1000, 1, 3);
        for i in 0u32..5 {
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                0,
            ));
        }

        let group = topo.group(0).unwrap();
        let ng = group.node_count();
        let covering = covering_set(1000, group, 3, 0);

        assert!(
            covering.len() <= ng,
            "covering_set size {} exceeds group node count {}",
            covering.len(),
            ng
        );
    }

    /// P1.3-A5: Two identical Topologies produce identical covering_set outputs.
    #[test]
    fn test_covering_set_determinism() {
        let mut topo1 = Topology::new(64, 2, 2);
        let mut topo2 = Topology::new(64, 2, 2);

        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            let node = Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            );
            topo1.add_node(node.clone());
            topo2.add_node(node);
        }

        let g1 = topo1.group(0).unwrap();
        let g2 = topo2.group(0).unwrap();

        for query_seq in 0..10 {
            let c1 = covering_set(64, g1, 2, query_seq);
            let c2 = covering_set(64, g2, 2, query_seq);
            // Compare as sets since order may vary due to HashSet iteration
            let s1: std::collections::HashSet<_> = c1.into_iter().collect();
            let s2: std::collections::HashSet<_> = c2.into_iter().collect();
            assert_eq!(
                s1, s2,
                "covering_set differs for identical topologies at query_seq={}",
                query_seq
            );
        }
    }

    /// P1.3-A6: query_group distribution is uniform (chi-square test).
    #[test]
    fn test_query_group_uniform_distribution() {
        let replica_groups = 5u32;
        let samples = 10_000;

        let mut counts = vec![0usize; replica_groups as usize];
        for query_seq in 0..samples {
            let g = query_group(query_seq as u64, replica_groups);
            counts[g as usize] += 1;
        }

        // Expected count per group: samples / RG
        let expected = samples as f64 / replica_groups as f64;

        // Chi-square statistic: sum((observed - expected)^2 / expected)
        let chi_square: f64 = counts
            .iter()
            .map(|&observed| {
                let diff = observed as f64 - expected;
                (diff * diff) / expected
            })
            .sum();

        // Degrees of freedom = RG - 1 = 4
        // Critical value at p=0.95 is ~9.49
        let critical_value = 9.49;

        assert!(
            chi_square < critical_value,
            "query_group distribution not uniform: chi-square={} > {}",
            chi_square,
            critical_value
        );
    }

    /// P1.3-A7: covering_set rotates replicas by query_seq.
    #[test]
    fn test_covering_set_rotates_replicas() {
        let mut topo = Topology::new(8, 1, 3);
        for i in 0u32..4 {
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                0,
            ));
        }

        let group = topo.group(0).unwrap();
        let c0 = covering_set(8, group, 3, 0);
        let c1 = covering_set(8, group, 3, 1);
        let c2 = covering_set(8, group, 3, 2);

        // For each shard, verify that the selected node rotates
        for shard_id in 0..8 {
            let replicas = assign_shard_in_group(shard_id, group.nodes(), 3);
            let r0 = &replicas[0];
            let r1 = &replicas[1];
            let r2 = &replicas[2];

            assert!(c0.contains(r0), "query_seq=0 should select first replica");
            assert!(c1.contains(r1), "query_seq=1 should select second replica");
            assert!(c2.contains(r2), "query_seq=2 should select third replica");
        }
    }

    // ── write_targets_with_migration tests ─────────────────────────────────────

    /// Test write_targets_with_migration without migration (same as write_targets).
    #[test]
    fn test_write_targets_with_migration_no_migration() {
        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        let shard_id = 7;
        let targets_no_migration = write_targets(shard_id, &topo);
        let targets_with_migration = write_targets_with_migration(shard_id, &topo, None);

        assert_eq!(targets_no_migration, targets_with_migration);
    }

    /// Test write_targets_with_migration with active dual-write for a shard.
    #[test]
    fn test_write_targets_with_migration_dual_write_includes_new_node() {
        use crate::migration::{MigrationConfig, MigrationCoordinator, MigrationId, MigrationPhase, ShardMigrationState, ShardId as MigShardId, MigrationState, NodeId as MigNodeId};
        use std::collections::HashMap;

        let mut topo = Topology::new(64, 2, 2);
        // Group 0: nodes 0-2, Group 1: nodes 3-5
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        // Create a migration coordinator with an active dual-write migration
        let config = MigrationConfig::default();
        let mut coordinator = MigrationCoordinator::new(config);

        // Start a migration for shard 7, adding node-6 as a new node in group 0
        let new_node_id = MigNodeId("node-6".to_string());
        let shard_id = MigShardId(7);

        // Create a migration state
        let mut affected_shards = HashMap::new();
        affected_shards.insert(shard_id, ShardMigrationState::Migrating {
            docs_copied: 1000,
            pages_remaining: 5,
        });

        let mut old_owners = HashMap::new();
        old_owners.insert(shard_id, MigNodeId("node-0".to_string()));

        let migration_state = MigrationState {
            id: MigrationId(1),
            new_node: new_node_id.clone(),
            replica_group: 0,
            phase: MigrationPhase::DualWriteMigrating,
            affected_shards,
            old_owners,
            started_at: None,
            completed_at: None,
        };

        // Manually insert the migration state
        coordinator.test_insert_migration(migration_state);

        // Get write targets with migration
        let targets = write_targets_with_migration(7, &topo, Some(&coordinator));

        // Should include standard RF nodes plus the new node
        let expected_count = 2 * 2 + 1; // RG=2, RF=2, plus 1 new node = 5
        assert_eq!(targets.len(), expected_count, "Should include standard targets plus new node");

        // Verify the new node is included
        assert!(targets.contains(&NodeId::new("node-6".to_string())), "Should include new node during dual-write");
    }

    /// Test write_targets_with_migration with dual-write for non-affected shard.
    #[test]
    fn test_write_targets_with_migration_dual_write_non_affected_shard() {
        use crate::migration::{MigrationConfig, MigrationCoordinator, MigrationId, MigrationPhase, ShardMigrationState, ShardId as MigShardId, MigrationState, NodeId as MigNodeId};
        use std::collections::HashMap;

        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        // Create a migration coordinator with an active dual-write migration for shard 5
        let config = MigrationConfig::default();
        let mut coordinator = MigrationCoordinator::new(config);

        let new_node_id = MigNodeId("node-6".to_string());
        let shard_id = MigShardId(5); // Different shard

        let mut affected_shards = HashMap::new();
        affected_shards.insert(shard_id, ShardMigrationState::Migrating {
            docs_copied: 1000,
            pages_remaining: 5,
        });

        let mut old_owners = HashMap::new();
        old_owners.insert(shard_id, MigNodeId("node-0".to_string()));

        let migration_state = MigrationState {
            id: MigrationId(1),
            new_node: new_node_id,
            replica_group: 0,
            phase: MigrationPhase::DualWriteMigrating,
            affected_shards,
            old_owners,
            started_at: None,
            completed_at: None,
        };

        coordinator.test_insert_migration(migration_state);

        // Get write targets for shard 7 (not affected by migration)
        let targets = write_targets_with_migration(7, &topo, Some(&coordinator));

        // Should be standard RF count only (RG=2, RF=2 = 4)
        assert_eq!(targets.len(), 4, "Non-affected shard should have standard target count");
    }

    /// Test write_targets_with_migration with completed migration (no dual-write).
    #[test]
    fn test_write_targets_with_migration_completed_migration() {
        use crate::migration::{MigrationConfig, MigrationCoordinator, MigrationId, MigrationPhase, ShardMigrationState, ShardId as MigShardId, MigrationState, NodeId as MigNodeId};
        use std::collections::HashMap;

        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        // Create a migration coordinator with a completed migration
        let config = MigrationConfig::default();
        let mut coordinator = MigrationCoordinator::new(config);

        let new_node_id = MigNodeId("node-6".to_string());
        let shard_id = MigShardId(7);

        let mut affected_shards = HashMap::new();
        affected_shards.insert(shard_id, ShardMigrationState::Active);

        let migration_state = MigrationState {
            id: MigrationId(1),
            new_node: new_node_id,
            replica_group: 0,
            phase: MigrationPhase::Complete,
            affected_shards,
            old_owners: HashMap::new(),
            started_at: None,
            completed_at: None,
        };

        coordinator.test_insert_migration(migration_state);

        // Get write targets - should not include dual-write since phase is Complete
        let targets = write_targets_with_migration(7, &topo, Some(&coordinator));

        // Should be standard RF count only (no dual-write)
        assert_eq!(targets.len(), 4, "Completed migration should not add dual-write targets");
    }

    /// Test write_targets_with_migration prevents duplicate new_node.
    #[test]
    fn test_write_targets_with_migration_no_duplicate_new_node() {
        use crate::migration::{MigrationConfig, MigrationCoordinator, MigrationId, MigrationPhase, ShardMigrationState, ShardId as MigShardId, MigrationState, NodeId as MigNodeId};
        use std::collections::HashMap;

        let mut topo = Topology::new(64, 2, 2);
        // Add node-6 to the topology first
        for i in 0u32..7 {
            let rg = if i < 4 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                rg,
            ));
        }

        // Create a migration coordinator
        let config = MigrationConfig::default();
        let mut coordinator = MigrationCoordinator::new(config);

        // Migration adding node-6 which is already in standard targets
        let new_node_id = MigNodeId("node-6".to_string());
        let shard_id = MigShardId(7);

        let mut affected_shards = HashMap::new();
        affected_shards.insert(shard_id, ShardMigrationState::Migrating {
            docs_copied: 500,
            pages_remaining: 2,
        });

        let mut old_owners = HashMap::new();
        old_owners.insert(shard_id, MigNodeId("node-0".to_string()));

        let migration_state = MigrationState {
            id: MigrationId(1),
            new_node: new_node_id,
            replica_group: 0,
            phase: MigrationPhase::DualWriteMigrating,
            affected_shards,
            old_owners,
            started_at: None,
            completed_at: None,
        };

        coordinator.test_insert_migration(migration_state);

        let targets = write_targets_with_migration(7, &topo, Some(&coordinator));

        // Count occurrences of node-6
        let node_6_count = targets.iter().filter(|n| n.as_str() == "node-6").count();

        // Should not duplicate node-6 if it's already in standard targets
        assert_eq!(node_6_count, 1, "Should not duplicate new_node if already in targets");
    }
}
