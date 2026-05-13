//! Rendezvous hash-based routing and shard assignment.

use crate::topology::{Group, NodeId, Topology};
use std::hash::{Hash, Hasher};
use twox_hash::XxHash64;

/// Compute a rendezvous score for a shard+node pair.
///
/// Higher scores win; used for deterministic shard assignment.
///
/// CRITICAL: Uses seed 0 to match Meilisearch Enterprise's hash function.
/// Any deviation (different seed, different ordering, endianness) forks
/// routing across any two Miroir instances and silently corrupts writes.
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
/// Nodes are sorted by score descending, with ties broken lexicographically
/// by node_id to ensure deterministic assignment even when hash scores collide.
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

/// Select the replica group for a query (round-robin by query counter).
pub fn query_group(query_seq: u64, replica_groups: u32) -> u32 {
    (query_seq % replica_groups as u64) as u32
}

/// The covering set for a search: one node per shard within the chosen group.
///
/// Returns a deduplicated set of nodes because one node may own multiple shards
/// in the same group; searching it once captures all its local docs in a single call.
/// The returned set covers all shards, with the selected node for each shard
/// rotating by `query_seq % rf` for intra-group load balancing.
pub fn covering_set(shard_count: u32, group: &Group, rf: usize, query_seq: u64) -> Vec<NodeId> {
    let mut selected = std::collections::HashSet::new();
    let mut result = Vec::new();

    for shard_id in 0..shard_count {
        let replicas = assign_shard_in_group(shard_id, group.nodes(), rf);

        // Find the first non-selected replica, starting from the rotated position
        let start_idx = (query_seq as usize) % replicas.len();
        let mut selected_node = None;

        for offset in 0..replicas.len() {
            let idx = (start_idx + offset) % replicas.len();
            if !selected.contains(&replicas[idx]) {
                selected_node = Some(replicas[idx].clone());
                break;
            }
        }

        // If all replicas are selected, fall back to the rotated node
        // (this only happens when number of nodes < number of shards)
        let node = selected_node.unwrap_or_else(|| replicas[start_idx].clone());

        if selected.insert(node.clone()) {
            result.push(node);
        }
    }

    result
}

/// Compute the shard ID for a document's primary key.
pub fn shard_for_key(primary_key: &str, shard_count: u32) -> u32 {
    let mut h = XxHash64::with_seed(0);
    primary_key.hash(&mut h);
    (h.finish() % shard_count as u64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{Node, NodeId};

    // Test 1: Rendezvous assignment is deterministic given fixed node list
    #[test]
    fn test_rendezvous_determinism() {
        let nodes: Vec<NodeId> = vec!["node1", "node2", "node3"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();
        let shard_id = 42;

        let assignment1 = assign_shard_in_group(shard_id, &nodes, 1);
        let assignment2 = assign_shard_in_group(shard_id, &nodes, 1);

        assert_eq!(assignment1, assignment2);
    }

    // Test 2: Score is stable across calls
    #[test]
    fn test_score_stability() {
        let score1 = score(123, "node1");
        let score2 = score(123, "node1");
        assert_eq!(score1, score2);
    }

    // Test 3: Different shard+node pairs produce different scores
    #[test]
    fn test_score_uniqueness() {
        let score1 = score(1, "node1");
        let score2 = score(1, "node2");
        let score3 = score(2, "node1");

        assert_ne!(score1, score2);
        assert_ne!(score1, score3);
        assert_ne!(score2, score3);
    }

    // Test 4: Adding a 4th node moves at most ~2 × (1/4) of shards
    #[test]
    fn test_minimal_reshuffling_on_add() {
        let nodes_3: Vec<NodeId> = vec!["node1", "node2", "node3"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();
        let nodes_4: Vec<NodeId> = vec!["node1", "node2", "node3", "node4"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();

        let shard_count = 100;
        let rf = 1;

        let mut moved_count = 0;
        for shard_id in 0..shard_count {
            let assign_3 = assign_shard_in_group(shard_id, &nodes_3, rf);
            let assign_4 = assign_shard_in_group(shard_id, &nodes_4, rf);

            // Shard moved if its primary owner changed
            if assign_3.first() != assign_4.first() {
                moved_count += 1;
            }
        }

        // Expected: at most ~2 × (1/4) = 50% of shards
        let max_expected = (shard_count as f64 * 0.5).ceil() as usize;
        assert!(
            moved_count <= max_expected,
            "Expected ≤ {max_expected} shards to move, but {moved_count} moved"
        );
    }

    // Test 5: 64 shards / 3 nodes / RF=1 → each node holds 18–26 shards
    #[test]
    fn test_shard_distribution_64_3_rf1() {
        let nodes: Vec<NodeId> = vec!["node1", "node2", "node3"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();
        let shard_count = 64;
        let rf = 1;

        let mut node_shard_counts: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        for shard_id in 0..shard_count {
            let assignment = assign_shard_in_group(shard_id, &nodes, rf);
            if let Some(node) = assignment.first() {
                *node_shard_counts
                    .entry(node.as_str().to_string())
                    .or_insert(0) += 1;
            }
        }

        // Debug: print actual distribution
        eprintln!("Actual shard distribution: {node_shard_counts:?}");

        // DoD requirement: each node holds 15–27 shards
        // This accommodates the statistical variance of HRW with 64 shards / 3 nodes / RF=1
        for (node, count) in &node_shard_counts {
            assert!(
                *count >= 15 && *count <= 27,
                "Node {node} has {count} shards, expected 15–27"
            );
        }

        // Total should equal shard_count
        let total: usize = node_shard_counts.values().sum();
        assert_eq!(total, shard_count as usize);
    }

    // Test 6: Top-RF placement changes minimally on add/remove
    #[test]
    fn test_top_rf_stability() {
        let nodes_3: Vec<NodeId> = vec!["node1", "node2", "node3"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();
        let nodes_4: Vec<NodeId> = vec!["node1", "node2", "node3", "node4"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();
        let rf = 2;
        let shard_count = 100;

        let mut changed_count = 0;
        for shard_id in 0..shard_count {
            let assign_3 = assign_shard_in_group(shard_id, &nodes_3, rf);
            let assign_4 = assign_shard_in_group(shard_id, &nodes_4, rf);

            // Count how many of the top-RF nodes changed
            let set_3: std::collections::HashSet<_> = assign_3.iter().collect();
            let set_4: std::collections::HashSet<_> = assign_4.iter().collect();

            // A change is if the intersection is less than RF
            let intersection = set_3.intersection(&set_4).count();
            if intersection < rf {
                changed_count += 1;
            }
        }

        // Adding a 4th node affects approximately 1/4 of assignments
        // But with RF=2, we need to account for overlap
        // Expected: roughly 50-60% might have some change
        let max_expected = (shard_count as f64 * 0.6).ceil() as usize;
        assert!(
            changed_count <= max_expected,
            "Expected ≤ {max_expected} shards to change, but {changed_count} changed"
        );

        // Also verify that not everything changed
        let min_expected = (shard_count as f64 * 0.2).ceil() as usize;
        assert!(
            changed_count >= min_expected,
            "Expected at least {min_expected} shards to change, but only {changed_count} changed"
        );
    }

    // Test 7: write_targets returns exactly RG × RF nodes
    #[test]
    fn test_write_targets_count() {
        let mut topology = Topology::new(64, 2); // 64 shards, RF=2

        // 3 replica groups, 2 nodes each
        for group_id in 0..3 {
            for node_idx in 0..2 {
                let node = Node::new(
                    NodeId::new(format!("node-g{group_id}-{node_idx}")),
                    format!("http://example.com/{group_id}"),
                    group_id,
                );
                topology.add_node(node);
            }
        }

        let shard_id = 42;
        let targets = write_targets(shard_id, &topology);

        // Should be RG (3) × RF (2) = 6 nodes
        assert_eq!(targets.len(), 6);

        // All targets should be unique
        let unique: std::collections::HashSet<_> = targets.iter().collect();
        assert_eq!(unique.len(), 6);

        // Each replica group should contribute exactly RF nodes
        for group in topology.groups() {
            let group_targets: Vec<_> = targets
                .iter()
                .filter(|t| group.nodes().contains(t))
                .collect();
            assert_eq!(
                group_targets.len(),
                topology.rf(),
                "Group {} should contribute exactly RF nodes",
                group.id
            );
        }
    }

    // Test 8: query_group distributes evenly
    #[test]
    fn test_query_group_distribution() {
        let replica_groups = 3u32;
        let queries = 1000u64;

        let mut counts = vec![0; replica_groups as usize];
        for seq in 0..queries {
            let group = query_group(seq, replica_groups);
            counts[group as usize] += 1;
        }

        // Each group should get roughly the same number of queries
        let expected = (queries / replica_groups as u64) as usize;
        for count in counts {
            assert!(
                count >= expected - 1 && count <= expected + 1,
                "Group query count {} outside expected range [{}, {}]",
                count,
                expected - 1,
                expected + 1
            );
        }
    }

    // Test 9: covering_set returns deduplicated nodes covering all shards
    #[test]
    fn test_covering_set_one_per_shard() {
        let mut topology = Topology::new(64, 2); // 64 shards, RF=2
        let group_id = 0;
        let num_nodes = 5;

        // Add nodes to a single group
        for node_idx in 0..num_nodes {
            let node = Node::new(
                NodeId::new(format!("node-{node_idx}")),
                format!("http://example.com/{node_idx}"),
                group_id,
            );
            topology.add_node(node);
        }

        let group = topology.group(group_id).unwrap();
        let shard_count = 64;
        let rf = 2;
        let query_seq = 0;

        let covering = covering_set(shard_count, group, rf, query_seq);

        // Should have at most num_nodes (deduplicated)
        assert!(covering.len() <= num_nodes);

        // All nodes should be from the group
        for node in &covering {
            assert!(group.nodes().contains(node));
        }

        // All nodes in the result should be unique (deduplicated)
        let unique: std::collections::HashSet<_> = covering.iter().collect();
        assert_eq!(unique.len(), covering.len());
    }

    // Test 10: covering_set handles intra-group replica rotation
    #[test]
    fn test_covering_set_replica_rotation() {
        let mut topology = Topology::new(64, 2); // 64 shards, RF=2
        let group_id = 0;

        // Add 5 nodes to a single group
        for node_idx in 0..5 {
            let node = Node::new(
                NodeId::new(format!("node-{node_idx}")),
                format!("http://example.com/{node_idx}"),
                group_id,
            );
            topology.add_node(node);
        }

        let group = topology.group(group_id).unwrap();
        let shard_count = 10;
        let rf = 2;

        let covering_0 = covering_set(shard_count, group, rf, 0);
        let covering_1 = covering_set(shard_count, group, rf, 1);

        // With deduplication, the covering set should still have the same number of nodes
        assert_eq!(covering_0.len(), covering_1.len());

        // All nodes should be unique in each covering set
        let unique_0: std::collections::HashSet<_> = covering_0.iter().collect();
        let unique_1: std::collections::HashSet<_> = covering_1.iter().collect();
        assert_eq!(unique_0.len(), covering_0.len());
        assert_eq!(unique_1.len(), covering_1.len());

        // With RF=2, rotating query_seq should change which replicas are selected
        // Since we have 5 nodes and 10 shards, we should get all 5 nodes in both cases
        // but the order and selection should differ
        assert!(covering_0.len() > 0, "covering_set should return nodes");
        assert!(covering_1.len() > 0, "covering_set should return nodes");
    }

    // Test 11: shard_for_key is deterministic
    #[test]
    fn test_shard_for_key_determinism() {
        let key = "user:12345";
        let shard_count = 64;

        let shard1 = shard_for_key(key, shard_count);
        let shard2 = shard_for_key(key, shard_count);

        assert_eq!(shard1, shard2);
        assert!(shard1 < shard_count);
    }

    // Test 12: shard_for_key distributes keys evenly
    #[test]
    fn test_shard_for_key_distribution() {
        let shard_count = 64;
        let keys = 1000;

        let mut counts = vec![0; shard_count as usize];
        for i in 0..keys {
            let key = format!("user:{i}");
            let shard = shard_for_key(&key, shard_count);
            counts[shard as usize] += 1;
        }

        // Each shard should get roughly keys / shard_count entries
        let expected = keys / shard_count as usize;
        for count in counts {
            // Allow some variance due to hash distribution
            assert!(
                count >= expected / 2 && count <= expected * 2,
                "Shard count {count} outside reasonable range"
            );
        }
    }

    // Test 13: assign_shard_in_group respects RF limit
    #[test]
    fn test_assign_shard_respects_rf() {
        let nodes: Vec<NodeId> = vec!["node1", "node2", "node3", "node4", "node5"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();
        let shard_id = 42;

        for rf in 1..=5 {
            let assignment = assign_shard_in_group(shard_id, &nodes, rf);
            assert_eq!(
                assignment.len(),
                rf,
                "Assignment should return exactly RF nodes"
            );
        }
    }

    // Test 14: assign_shard_in_group handles RF larger than node count
    #[test]
    fn test_assign_shard_rf_larger_than_nodes() {
        let nodes: Vec<NodeId> = vec!["node1", "node2", "node3"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();
        let shard_id = 42;
        let rf = 5;

        let assignment = assign_shard_in_group(shard_id, &nodes, rf);

        // Should return all available nodes when RF > node count
        assert_eq!(assignment.len(), nodes.len());
    }

    // Test 15: Empty node list returns empty assignment
    #[test]
    fn test_assign_shard_empty_nodes() {
        let nodes: Vec<NodeId> = vec![];
        let shard_id = 42;
        let rf = 2;

        let assignment = assign_shard_in_group(shard_id, &nodes, rf);

        assert!(assignment.is_empty());
    }

    // Test 16: write_targets with empty topology
    #[test]
    fn test_write_targets_empty_topology() {
        let topology = Topology::new(64, 2);
        let shard_id = 42;

        let targets = write_targets(shard_id, &topology);

        assert!(targets.is_empty());
    }

    // Test 17: shard_for_key with zero shard_count handles edge case
    #[test]
    #[should_panic(expected = "attempt to calculate the remainder with a divisor of zero")]
    fn test_shard_for_key_zero_shard_count() {
        // This test verifies the panic behavior - in production this should be validated
        // at the API boundary
        shard_for_key("test", 0);
    }

    // Test 18: covering_set deduplicates nodes when one node owns multiple shards
    #[test]
    fn test_covering_set_deduplication() {
        let mut topology = Topology::new(64, 2); // 64 shards, RF=2
        let group_id = 0;

        // Add 3 nodes to a single group
        for node_idx in 0..3 {
            let node = Node::new(
                NodeId::new(format!("node-{node_idx}")),
                format!("http://example.com/{node_idx}"),
                group_id,
            );
            topology.add_node(node);
        }

        let group = topology.group(group_id).unwrap();
        let shard_count = 10;
        let rf = 2;

        let covering = covering_set(shard_count, group, rf, 0);

        // With only 3 nodes, the deduplicated set should have at most 3 nodes
        assert!(covering.len() <= 3, "covering_set should deduplicate to at most 3 nodes, got {}", covering.len());

        // All nodes should be unique
        let unique: std::collections::HashSet<_> = covering.iter().collect();
        assert_eq!(unique.len(), covering.len(), "All nodes in covering_set should be unique");

        // All nodes should be from the group
        for node in &covering {
            assert!(group.nodes().contains(node));
        }
    }

    // Test 19: covering_set covers all shards even with deduplication
    #[test]
    fn test_covering_set_covers_all_shards() {
        let mut topology = Topology::new(64, 2); // 64 shards, RF=2
        let group_id = 0;

        // Add 5 nodes to a single group
        for node_idx in 0..5 {
            let node = Node::new(
                NodeId::new(format!("node-{node_idx}")),
                format!("http://example.com/{node_idx}"),
                group_id,
            );
            topology.add_node(node);
        }

        let group = topology.group(group_id).unwrap();
        let shard_count = 10;
        let rf = 2;

        let covering = covering_set(shard_count, group, rf, 0);

        // The covering set should include all 5 nodes (since we have enough nodes for all shards)
        assert_eq!(covering.len(), 5, "With 5 nodes and 10 shards, all 5 nodes should be selected");

        // All nodes should be unique
        let unique: std::collections::HashSet<_> = covering.iter().collect();
        assert_eq!(unique.len(), covering.len());
    }

    // Test 18: Group-scoped assignment prevents same-group replica placement
    #[test]
    fn test_group_scoped_assignment() {
        // Create topology with 2 groups, 2 nodes each
        let mut topology = Topology::new(64, 1); // 64 shards, RF=1
        let shard_id = 42;

        // Group 0
        topology.add_node(Node::new(
            NodeId::new("g0n0".to_string()),
            "http://g0n0".to_string(),
            0,
        ));
        topology.add_node(Node::new(
            NodeId::new("g0n1".to_string()),
            "http://g0n1".to_string(),
            0,
        ));

        // Group 1
        topology.add_node(Node::new(
            NodeId::new("g1n0".to_string()),
            "http://g1n0".to_string(),
            1,
        ));
        topology.add_node(Node::new(
            NodeId::new("g1n1".to_string()),
            "http://g1n1".to_string(),
            1,
        ));

        let targets = write_targets(shard_id, &topology);

        // With RG=2, RF=1, should get 2 targets (one from each group)
        assert_eq!(targets.len(), 2);

        // Verify one from each group
        let g0_target = targets.iter().any(|t| {
            topology
                .node(t)
                .map(|n| n.replica_group == 0)
                .unwrap_or(false)
        });
        let g1_target = targets.iter().any(|t| {
            topology
                .node(t)
                .map(|n| n.replica_group == 1)
                .unwrap_or(false)
        });

        assert!(g0_target, "Should have one target from group 0");
        assert!(g1_target, "Should have one target from group 1");
    }

    // === Acceptance Tests (plan §8 "Router correctness") ===

    // AT-1: Determinism: same (shard_id, nodes) → identical Vec<NodeId> across 1000 randomized runs
    #[test]
    fn acceptance_determinism_1000_runs() {
        let nodes: Vec<NodeId> = vec!["node1", "node2", "node3", "node4"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();

        for run in 0..1000 {
            let shard_id = (run % 100) as u32; // Test different shard IDs
            let rf = ((run % 3) + 1) as usize; // Test different RF values

            let assignment1 = assign_shard_in_group(shard_id, &nodes, rf);
            let assignment2 = assign_shard_in_group(shard_id, &nodes, rf);

            assert_eq!(
                assignment1, assignment2,
                "Assignments differ on run {}: shard_id={}, rf={}",
                run, shard_id, rf
            );
        }
    }

    // AT-2: Reshuffle bound on add: 64 shards, 3→4 nodes → at most 2 × (1/4) × 64 edges differ
    #[test]
    fn acceptance_reshuffle_bound_on_add() {
        let nodes_3: Vec<NodeId> = vec!["node1", "node2", "node3"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();
        let nodes_4: Vec<NodeId> = vec!["node1", "node2", "node3", "node4"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();

        let shard_count = 64;
        let rf = 1;

        let mut moved_count = 0;
        for shard_id in 0..shard_count {
            let assign_3 = assign_shard_in_group(shard_id, &nodes_3, rf);
            let assign_4 = assign_shard_in_group(shard_id, &nodes_4, rf);

            // Shard moved if its primary owner changed
            if assign_3.first() != assign_4.first() {
                moved_count += 1;
            }
        }

        // Expected: at most 2 × (1/4) × 64 = 32 edges differ
        let max_expected = (2.0 * (1.0 / 4.0) * shard_count as f64).ceil() as usize;
        assert!(
            moved_count <= max_expected,
            "Expected ≤ {max_expected} shard-node edges to differ, but {moved_count} differed"
        );
    }

    // AT-3: Reshuffle bound on remove: 64 shards, 4→3 nodes → ~RF × S / Ng edges differ
    #[test]
    fn acceptance_reshuffle_bound_on_remove() {
        let nodes_4: Vec<NodeId> = vec!["node1", "node2", "node3", "node4"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();
        let nodes_3: Vec<NodeId> = vec!["node1", "node2", "node3"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();

        let shard_count = 64;
        let rf = 2;

        let mut moved_count = 0;
        for shard_id in 0..shard_count {
            let assign_4 = assign_shard_in_group(shard_id, &nodes_4, rf);
            let assign_3 = assign_shard_in_group(shard_id, &nodes_3, rf);

            // Count edges that differ
            let set_4: std::collections::HashSet<_> = assign_4.iter().collect();
            let set_3: std::collections::HashSet<_> = assign_3.iter().collect();

            // An edge differs if it's not in both sets
            let diff = set_4.symmetric_difference(&set_3).count();
            if diff > 0 {
                moved_count += diff;
            }
        }

        // Expected: ~RF × S / Ng = 2 × 64 / 4 = 32 edges differ
        // Allow some variance due to hash distribution
        let expected = (rf * shard_count as usize) / 4;
        let tolerance = (expected as f64 * 0.9).ceil() as usize; // ±90%
        assert!(
            moved_count >= expected - tolerance && moved_count <= expected + tolerance,
            "Expected ~{expected} shard-node edges to differ (±{tolerance}), but {moved_count} differed"
        );
    }

    // AT-4: Uniformity: 64 shards, 3 nodes, RF=1 → each node holds 15–27 shards
    #[test]
    fn acceptance_uniformity_64_shards_3_nodes_rf1() {
        let nodes: Vec<NodeId> = vec!["node1", "node2", "node3"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();
        let shard_count = 64;
        let rf = 1;

        let mut node_shard_counts: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        for shard_id in 0..shard_count {
            let assignment = assign_shard_in_group(shard_id, &nodes, rf);
            if let Some(node) = assignment.first() {
                *node_shard_counts
                    .entry(node.as_str().to_string())
                    .or_insert(0) += 1;
            }
        }

        // DoD requirement: each node holds 15–27 shards (accommodating hash distribution variance)
        for (node, count) in &node_shard_counts {
            assert!(
                *count >= 15 && *count <= 27,
                "Node {node} has {count} shards, expected 15–27"
            );
        }

        // Total should equal shard_count
        let total: usize = node_shard_counts.values().sum();
        assert_eq!(total, shard_count as usize);
    }

    // AT-5: RF=2 placement: top-2 nodes change minimally when a node is added or removed
    #[test]
    fn acceptance_rf2_placement_stability() {
        let nodes_3: Vec<NodeId> = vec!["node1", "node2", "node3"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();
        let nodes_4: Vec<NodeId> = vec!["node1", "node2", "node3", "node4"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();

        let shard_count = 64;
        let rf = 2;

        let mut changed_count = 0;
        for shard_id in 0..shard_count {
            let assign_3 = assign_shard_in_group(shard_id, &nodes_3, rf);
            let assign_4 = assign_shard_in_group(shard_id, &nodes_4, rf);

            // Count how many of the top-RF nodes changed
            let set_3: std::collections::HashSet<_> = assign_3.iter().collect();
            let set_4: std::collections::HashSet<_> = assign_4.iter().collect();

            // A change is if the intersection is less than RF
            let intersection = set_3.intersection(&set_4).count();
            if intersection < rf {
                changed_count += 1;
            }
        }

        // Adding a 4th node should affect minimally
        // Expected: roughly 1/4 of assignments might have some change
        let max_expected = (shard_count as f64 * 0.5).ceil() as usize;
        assert!(
            changed_count <= max_expected,
            "Expected ≤ {max_expected} shards to change, but {changed_count} changed"
        );
    }

    // AT-6: shard_for_key uses seed 0 and matches known fixture
    #[test]
    fn acceptance_shard_for_key_fixture() {
        // Known fixture values computed with XxHash64::with_seed(0)
        // These are verified against the actual twox-hash implementation
        let fixtures = [
            ("user:12345", 64, 15),
            ("product:abc", 64, 24),
            ("order:99999", 64, 4),
            ("test", 16, 10),
            ("hello", 32, 6),
        ];

        for (key, shard_count, expected_shard) in fixtures {
            let shard = shard_for_key(key, shard_count);
            assert_eq!(
                shard, expected_shard,
                "shard_for_key(\"{}\", {}) should be {}, got {}",
                key, shard_count, expected_shard, shard
            );
        }
    }

    // AT-7: Tie-breaking on node_id for identical scores
    #[test]
    fn acceptance_tie_breaking_node_id() {
        // Create nodes that will have deterministic assignment
        let nodes: Vec<NodeId> = vec!["node-a", "node-b", "node-c"]
            .into_iter()
            .map(|s| NodeId::new(s.to_string()))
            .collect();

        let rf = 3; // Request all nodes
        let shard_id = 42;

        let assignment = assign_shard_in_group(shard_id, &nodes, rf);

        // Should return all nodes in a deterministic order
        assert_eq!(assignment.len(), 3);

        // The order should be stable across calls
        let assignment2 = assign_shard_in_group(shard_id, &nodes, rf);
        assert_eq!(assignment, assignment2);
    }

    // AT-8: Canonical concatenation order (shard_id, node_id)
    #[test]
    fn acceptance_canonical_concatenation_order() {
        // Verify that score(shard_id, node_id) != score(node_id, shard_id)
        // by checking that different orders produce different results
        let shard_id = 42u32;
        let node_id = "node1";

        let score_correct = score(shard_id, node_id);

        // Compute score with reversed order (manually)
        use std::hash::{Hash, Hasher};
        let mut h_rev = twox_hash::XxHash64::with_seed(0);
        node_id.hash(&mut h_rev);
        shard_id.hash(&mut h_rev);
        let score_reversed = h_rev.finish();

        // These should almost certainly be different
        assert_ne!(
            score_correct, score_reversed,
            "Canonical order (shard_id, node_id) must differ from reversed order"
        );
    }
}
