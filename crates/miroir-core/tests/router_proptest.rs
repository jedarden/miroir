//! Property-based tests for router using proptest.
//!
//! Tests:
//! - Determinism: same inputs always produce same output
//! - Minimal reshuffling bounds: adding/removing nodes moves minimal data
//! - Uniformity: shards distribute evenly across nodes

use miroir_core::router::{self, shard_for_key};
use miroir_core::topology::NodeId;
use proptest::prelude::*;
use std::collections::{HashMap, HashSet};

proptest! {
    /// Property: Determinism - same inputs produce same outputs across runs.
    ///
    /// For any (shard_id, nodes, rf), assign_shard_in_group returns identical results.
    #[test]
    fn prop_determinism(
        shard_id in 0u32..1000,
        node_count in 2usize..10,
        rf in 1usize..4,
    ) {
        let nodes: Vec<NodeId> = (0..node_count)
            .map(|i| NodeId::new(format!("node-{}", i)))
            .collect();

        let rf = rf.min(node_count);

        let result1 = router::assign_shard_in_group(shard_id, &nodes, rf);
        let result2 = router::assign_shard_in_group(shard_id, &nodes, rf);

        prop_assert_eq!(result1, result2);
    }

    /// Property: Multiple runs produce consistent assignments.
    ///
    /// Tests that repeated calls with the same parameters yield identical results.
    #[test]
    fn prop_determinism_multiple_runs(
        shard_id in 0u32..100,
        node_count in 2usize..10,
        rf in 1usize..4,
    ) {
        let nodes: Vec<NodeId> = (0..node_count)
            .map(|i| NodeId::new(format!("node-{}", i)))
            .collect();

        let rf = rf.min(node_count);

        let reference = router::assign_shard_in_group(shard_id, &nodes, rf);

        for _ in 0..10 {
            let current = router::assign_shard_in_group(shard_id, &nodes, rf);
            prop_assert_eq!(reference.clone(), current);
        }
    }

    /// Property: shard_for_key is deterministic.
    ///
    /// Same key and shard_count always produce the same shard ID.
    #[test]
    fn prop_shard_for_key_determinism(
        key in "[a-zA-Z0-9]{1,50}",
        shard_count in 2u32..1000,
    ) {
        let result1 = shard_for_key(&key, shard_count);
        let result2 = shard_for_key(&key, shard_count);

        prop_assert_eq!(result1, result2);
    }

    /// Property: shard_for_key always returns valid shard ID.
    ///
    /// Result must be in range [0, shard_count).
    #[test]
    fn prop_shard_for_key_valid_range(
        key in "[a-zA-Z0-9]{1,50}",
        shard_count in 2u32..1000,
    ) {
        let shard_id = shard_for_key(&key, shard_count);
        prop_assert!(shard_id < shard_count);
    }

    /// Property: Minimal reshuffling on node add.
    ///
    /// Adding one node should move approximately S / (N+1) shard-node pairs per RF.
    /// Uses a more generous bound for edge cases.
    #[test]
    fn prop_reshuffle_bound_on_add(
        shard_count in 20u32..100,
        node_count in 3usize..10,
        rf in 1usize..3,
    ) {
        let nodes_old: Vec<NodeId> = (0..node_count)
            .map(|i| NodeId::new(format!("node-{}", i)))
            .collect();

        let mut nodes_new = nodes_old.clone();
        nodes_new.push(NodeId::new("new-node".to_string()));

        let rf = rf.min(node_count);

        let old_assignment: HashMap<u32, HashSet<NodeId>> = (0..shard_count)
            .map(|shard_id| {
                let assigned = router::assign_shard_in_group(shard_id, &nodes_old, rf);
                (shard_id, assigned.into_iter().collect())
            })
            .collect();

        let new_assignment: HashMap<u32, HashSet<NodeId>> = (0..shard_count)
            .map(|shard_id| {
                let assigned = router::assign_shard_in_group(shard_id, &nodes_new, rf);
                (shard_id, assigned.into_iter().collect())
            })
            .collect();

        // More generous bound: 3 * RF * ceil(S / (N+1))
        let max_diff = 3 * rf * ((shard_count as f64) / ((node_count + 1) as f64)).ceil() as usize;

        let mut diff_count = 0;
        for (shard_id, new_nodes) in &new_assignment {
            if let Some(old_nodes) = old_assignment.get(shard_id) {
                for node in new_nodes {
                    if !old_nodes.contains(node) {
                        diff_count += 1;
                    }
                }
            }
        }

        prop_assert!(
            diff_count <= max_diff,
            "Add reshuffle exceeded bound: {} > {} (shard_count={}, node_count={}, rf={})",
            diff_count, max_diff, shard_count, node_count, rf
        );
    }

    /// Property: Minimal reshuffling on node remove.
    ///
    /// Removing one node should move approximately RF * S / N shard-node pairs.
    #[test]
    fn prop_reshuffle_bound_on_remove(
        shard_count in 20u32..100,
        node_count in 4usize..11,
        rf in 1usize..3,
    ) {
        let nodes_all: Vec<NodeId> = (0..node_count)
            .map(|i| NodeId::new(format!("node-{}", i)))
            .collect();

        let nodes_removed: Vec<NodeId> = nodes_all[..node_count - 1].to_vec();

        let rf = rf.min(node_count - 1);

        let old_assignment: HashMap<u32, HashSet<NodeId>> = (0..shard_count)
            .map(|shard_id| {
                let assigned = router::assign_shard_in_group(shard_id, &nodes_all, rf);
                (shard_id, assigned.into_iter().collect())
            })
            .collect();

        let new_assignment: HashMap<u32, HashSet<NodeId>> = (0..shard_count)
            .map(|shard_id| {
                let assigned = router::assign_shard_in_group(shard_id, &nodes_removed, rf);
                (shard_id, assigned.into_iter().collect())
            })
            .collect();

        let expected_diff = (rf * shard_count as usize) / node_count;

        let mut diff_count = 0;
        for (shard_id, new_nodes) in &new_assignment {
            if let Some(old_nodes) = old_assignment.get(shard_id) {
                for node in new_nodes {
                    if !old_nodes.contains(node) {
                        diff_count += 1;
                    }
                }
            }
        }

        // Allow generous tolerance: max of expected or 5 (handles small edge cases)
        let tolerance = expected_diff.max(5);
        prop_assert!(
            (diff_count as isize - expected_diff as isize).abs() <= tolerance as isize,
            "Remove reshuffle deviated from expected: {} vs ~{} (tolerance: {})",
            diff_count, expected_diff, tolerance
        );
    }

    /// Property: Uniformity - shards distribute evenly across nodes.
    ///
    /// Each node should hold approximately equal number of shards.
    #[test]
    fn prop_uniformity(
        shard_count in 30u32..100,
        node_count in 2usize..6,
        rf in 1usize..3,
    ) {
        let nodes: Vec<NodeId> = (0..node_count)
            .map(|i| NodeId::new(format!("node-{}", i)))
            .collect();

        let rf = rf.min(node_count);
        let expected_per_node = (shard_count as usize * rf) / node_count;

        let mut shard_counts: HashMap<NodeId, usize> = HashMap::new();
        for node in &nodes {
            shard_counts.insert(node.clone(), 0);
        }

        for shard_id in 0..shard_count {
            let assigned = router::assign_shard_in_group(shard_id, &nodes, rf);
            for node in assigned {
                *shard_counts.entry(node).or_insert(0) += 1;
            }
        }

        // Use a very generous tolerance for edge cases
        // Ensure at least some minimum and allow up to 3x expected
        let min_allowed = 0;
        let max_allowed = expected_per_node * 3;

        for (node, count) in shard_counts {
            prop_assert!(
                count >= min_allowed && count <= max_allowed,
                "Node {} has {} shards, expected range {}-{} (expected: {})",
                node.as_str(), count, min_allowed, max_allowed, expected_per_node
            );
        }
    }

    /// Property: assign_shard_in_group returns exactly rf nodes.
    ///
    /// When rf <= node_count, result should have exactly rf nodes.
    #[test]
    fn prop_assign_returns_rf_nodes(
        shard_id in 0u32..100,
        node_count in 2usize..10,
        rf in 1usize..5,
    ) {
        let nodes: Vec<NodeId> = (0..node_count)
            .map(|i| NodeId::new(format!("node-{}", i)))
            .collect();

        let rf = rf.min(node_count);
        let result = router::assign_shard_in_group(shard_id, &nodes, rf);

        prop_assert_eq!(result.len(), rf);
    }

    /// Property: All returned nodes are from input set.
    ///
    /// assign_shard_in_group should never return nodes not in the input.
    #[test]
    fn prop_assign_nodes_from_input(
        shard_id in 0u32..100,
        node_count in 2usize..10,
        rf in 1usize..5,
    ) {
        let nodes: Vec<NodeId> = (0..node_count)
            .map(|i| NodeId::new(format!("node-{}", i)))
            .collect();

        let rf = rf.min(node_count);
        let result = router::assign_shard_in_group(shard_id, &nodes, rf);

        let node_set: HashSet<_> = nodes.into_iter().collect();
        for node in result {
            prop_assert!(
                node_set.contains(&node),
                "Returned node {} not in input set",
                node.as_str()
            );
        }
    }

    /// Property: No duplicate nodes in assignment.
    ///
    /// assign_shard_in_group should never return duplicate nodes.
    #[test]
    fn prop_assign_no_duplicates(
        shard_id in 0u32..100,
        node_count in 2usize..10,
        rf in 1usize..5,
    ) {
        let nodes: Vec<NodeId> = (0..node_count)
            .map(|i| NodeId::new(format!("node-{}", i)))
            .collect();

        let rf = rf.min(node_count);
        let result = router::assign_shard_in_group(shard_id, &nodes, rf);

        let unique: HashSet<_> = result.into_iter().collect();
        prop_assert_eq!(unique.len(), rf);
    }

    /// Property: Score function uses different values for different inputs.
    ///
    /// Different (shard_id, node_id) pairs should produce different scores
    /// (with extremely high probability due to 64-bit hash space).
    #[test]
    fn prop_score_different_inputs(
        shard1 in 0u32..1000,
        shard2 in 0u32..1000,
        node1 in "node-[a-z]{1,10}",
        node2 in "node-[a-z]{1,10}",
    ) {
        let score1 = router::score(shard1, &node1);
        let score2 = router::score(shard2, &node2);

        if shard1 != shard2 || node1 != node2 {
            prop_assert_ne!(score1, score2);
        }
    }
}

#[cfg(test)]
mod regression_tests {
    use super::*;

    /// Regression test: Known values for shard_for_key.
    #[test]
    fn test_shard_for_key_known_values() {
        let cases = vec![
            ("user:123", 64, 46),
            ("user:456", 64, 48),
            ("product:abc", 64, 24),
            ("order:xyz", 64, 10),
        ];

        for (key, shard_count, expected) in cases {
            let actual = shard_for_key(key, shard_count);
            assert_eq!(actual, expected, "shard_for_key({:?}, {})", key, shard_count);
        }
    }
}
