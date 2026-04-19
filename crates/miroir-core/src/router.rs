//! Rendezvous hash-based routing and shard assignment.

use crate::topology::{Group, NodeId, Topology};
use twox_hash::XxHash64;
use std::hash::{Hash, Hasher};

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
pub fn assign_shard_in_group(shard_id: u32, group_nodes: &[NodeId], rf: usize) -> Vec<NodeId> {
    let mut scored: Vec<(u64, &NodeId)> = group_nodes
        .iter()
        .map(|n| (score(shard_id, n.as_str()), n))
        .collect();
    scored.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    scored.into_iter().take(rf).map(|(_, n)| n.clone()).collect()
}

/// All write targets for a document: the RF nodes in EACH replica group.
pub fn write_targets(shard_id: u32, topology: &Topology) -> Vec<NodeId> {
    topology.groups().flat_map(|group| {
        assign_shard_in_group(shard_id, group.nodes(), topology.rf())
    }).collect()
}

/// Select the replica group for a query (round-robin by query counter).
pub fn query_group(query_seq: u64, replica_groups: u32) -> u32 {
    (query_seq % replica_groups as u64) as u32
}

/// The covering set for a search: one node per shard within the chosen group.
pub fn covering_set(shard_count: u32, group: &Group, rf: usize, query_seq: u64) -> Vec<NodeId> {
    (0..shard_count).map(|shard_id| {
        let replicas = assign_shard_in_group(shard_id, group.nodes(), rf);
        // rotate through replicas for intra-group load balancing
        replicas[(query_seq as usize) % replicas.len()].clone()
    }).collect::<std::collections::HashSet<_>>().into_iter().collect()
}

/// Compute the shard ID for a document's primary key.
pub fn shard_for_key(primary_key: &str, shard_count: u32) -> u32 {
    let mut h = XxHash64::with_seed(0);
    primary_key.hash(&mut h);
    (h.finish() % shard_count as u64) as u32
}
