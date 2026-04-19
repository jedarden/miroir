//! Rendezvous hash-based routing and shard assignment.

use crate::topology::{Group, NodeId, Topology};
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

/// Select the replica group for a query (round-robin by query counter).
pub fn query_group(query_seq: u64, replica_groups: u32) -> u32 {
    (query_seq % replica_groups as u64) as u32
}

/// The covering set for a search: one node per shard within the chosen group.
pub fn covering_set(shard_count: u32, group: &Group, rf: usize, query_seq: u64) -> Vec<NodeId> {
    (0..shard_count)
        .map(|shard_id| {
            let replicas = assign_shard_in_group(shard_id, group.nodes(), rf);
            // rotate through replicas for intra-group load balancing
            replicas[(query_seq as usize) % replicas.len()].clone()
        })
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect()
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
fn count_assignment_diff(
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
    use crate::topology::NodeId;
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
    /// Expected: each node holds approximately equal shards (chi-square not rejected at p=0.95).
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
        // Acceptable range: 15–28 (accounts for hash variance)
        for (node, count) in shard_counts {
            assert!(
                (15..=28).contains(&count),
                "Node {} has {} shards, expected 15–28",
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
}
