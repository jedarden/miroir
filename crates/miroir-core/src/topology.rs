//! Topology management: node registry, groups, and health state.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Unique identifier for a node.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
    /// Create a new NodeId.
    pub fn new(id: String) -> Self {
        Self(id)
    }

    /// Get the node ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Health status of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is healthy and serving traffic.
    Healthy,
    /// Node is degraded (intermittent failures, still serving traffic).
    Degraded,
    /// Node is active and fully operational (synonym for Healthy).
    Active,
    /// Node is joining the cluster (being provisioned).
    Joining,
    /// Node is draining (graceful shutdown, not accepting new writes).
    Draining,
    /// Node has failed (unplanned outage).
    Failed,
    /// Node has been removed from the cluster (tracked for migration).
    Removed,
}

impl NodeStatus {
    /// Check if a transition from `self` to `new_status` is valid.
    ///
    /// # State Transition Rules
    ///
    /// | From | To | Triggered by |
    /// |------|-----|-------------|
    /// | (new) | Joining | `POST /_miroir/nodes` |
    /// | Joining | Active | Migration complete |
    /// | Active | Draining | `POST /_miroir/nodes/{id}/drain` |
    /// | Draining | Removed | Migration complete |
    /// | Active/Draining | Failed | Health check detects |
    /// | Failed | Active | Health check recovery |
    /// | Active/Failed | Degraded | Partial health |
    /// | Degraded | Active | Health restored |
    pub fn can_transition_to(self, new_status: NodeStatus) -> bool {
        match (self, new_status) {
            // Initial state
            (NodeStatus::Joining, NodeStatus::Active) => true,

            // Normal operations
            (NodeStatus::Active, NodeStatus::Draining) => true,
            (NodeStatus::Draining, NodeStatus::Removed) => true,

            // Failure and recovery
            (NodeStatus::Active, NodeStatus::Failed) => true,
            (NodeStatus::Draining, NodeStatus::Failed) => true,
            (NodeStatus::Failed, NodeStatus::Active) => true,

            // Degradation
            (NodeStatus::Active, NodeStatus::Degraded) => true,
            (NodeStatus::Failed, NodeStatus::Degraded) => true,
            (NodeStatus::Degraded, NodeStatus::Active) => true,

            // Healthy <-> Active are bidirectional (synonyms)
            (NodeStatus::Healthy, NodeStatus::Active) => true,
            (NodeStatus::Active, NodeStatus::Healthy) => true,

            // Same state is always valid
            (s, t) if s == t => true,

            // All other transitions are invalid
            _ => false,
        }
    }

    /// Returns `true` if the node can accept writes for the given shard.
    ///
    /// # Write Eligibility Rules
    ///
    /// A node is write-eligible for a shard based on its status:
    ///
    /// | Status | Write Eligible | Notes |
    /// |--------|----------------|-------|
    /// | Healthy/Active | Yes | Normal operation |
    /// | Degraded | Yes | Partial failures, still accepting writes |
    /// | Joining | No | Being provisioned, not yet ready |
    /// | Draining | Conditional | Only for shards it still owns during migration |
    /// | Failed | No | Unavailable |
    /// | Removed | No | No longer in cluster |
    ///
    /// The `draining_shard` parameter should be `Some(shard_id)` if the node
    /// is in `Draining` status and the shard IS being actively migrated off this node
    /// (use `None` if the shard is not being drained or no shard is being checked).
    /// When `Some(...)`, the node is NOT eligible for writes.
    pub fn is_write_eligible_for(self, draining_shard: Option<u32>) -> bool {
        match self {
            NodeStatus::Healthy | NodeStatus::Active | NodeStatus::Degraded => true,
            NodeStatus::Joining | NodeStatus::Failed | NodeStatus::Removed => false,
            NodeStatus::Draining => !draining_shard.is_some(),
        }
    }
}

impl fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeStatus::Healthy => write!(f, "healthy"),
            NodeStatus::Degraded => write!(f, "degraded"),
            NodeStatus::Active => write!(f, "active"),
            NodeStatus::Joining => write!(f, "joining"),
            NodeStatus::Draining => write!(f, "draining"),
            NodeStatus::Failed => write!(f, "failed"),
            NodeStatus::Removed => write!(f, "removed"),
        }
    }
}

/// A single Meilisearch node in the topology.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Unique node identifier.
    pub id: NodeId,

    /// Node base URL / address.
    pub address: String,

    /// Current health status.
    pub status: NodeStatus,

    /// Replica group assignment (0-based).
    pub replica_group: u32,
}

impl Node {
    /// Create a new node.
    pub fn new(id: NodeId, address: String, replica_group: u32) -> Self {
        Self {
            id,
            address,
            status: NodeStatus::Joining,
            replica_group,
        }
    }

    /// Create a new node with a specific status.
    pub fn with_status(id: NodeId, address: String, replica_group: u32, status: NodeStatus) -> Self {
        Self {
            id,
            address,
            status,
            replica_group,
        }
    }

    /// Check if the node is healthy (can serve traffic).
    pub fn is_healthy(&self) -> bool {
        matches!(self.status, NodeStatus::Healthy | NodeStatus::Active)
    }

    /// Transition the node to a new status, validating the transition.
    ///
    /// Returns `Ok(())` if the transition is valid, `Err` otherwise.
    pub fn set_status(&mut self, new_status: NodeStatus) -> Result<(), TransitionError> {
        if self.status.can_transition_to(new_status) {
            self.status = new_status;
            Ok(())
        } else {
            Err(TransitionError {
                from: self.status,
                to: new_status,
            })
        }
    }

    /// Check if the node is eligible to receive writes for a specific shard.
    ///
    /// For nodes in `Draining` status, this depends on whether the shard is
    /// being actively migrated off this node. The caller should pass
    /// `Some(shard_id)` if the shard is being drained from this node.
    pub fn is_write_eligible_for(&self, shard_id: Option<u32>) -> bool {
        self.status.is_write_eligible_for(shard_id)
    }
}

/// Error returned when an invalid state transition is attempted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionError {
    pub from: NodeStatus,
    pub to: NodeStatus,
}

impl fmt::Display for TransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid state transition from {} to {}",
            self.from, self.to
        )
    }
}

impl std::error::Error for TransitionError {}

/// A replica group: an independent query pool.
///
/// Each group holds all S shards, distributed across its nodes.
/// Reads are routed to a single group per query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Group {
    /// Group identifier (0-based).
    pub id: u32,

    /// Nodes in this group.
    nodes: Vec<NodeId>,
}

impl Group {
    /// Create a new group.
    pub fn new(id: u32) -> Self {
        Self {
            id,
            nodes: Vec::new(),
        }
    }

    /// Add a node to this group.
    pub fn add_node(&mut self, node_id: NodeId) {
        if !self.nodes.contains(&node_id) {
            self.nodes.push(node_id);
        }
    }

    /// Get the node IDs in this group.
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    /// Get the number of nodes in this group.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get all healthy nodes in this group, looking them up from the topology.
    ///
    /// This requires access to the topology's node map to resolve NodeIds to Nodes.
    pub fn healthy_nodes<'a>(&'a self, all_nodes: &'a HashMap<NodeId, Node>) -> Vec<&'a Node> {
        self.nodes
            .iter()
            .filter_map(|node_id| all_nodes.get(node_id))
            .filter(|node| node.is_healthy())
            .collect()
    }
}

/// Cluster topology: groups, nodes, and health state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topology {
    /// All nodes in the cluster.
    nodes: HashMap<NodeId, Node>,

    /// Replica groups.
    groups: Vec<Group>,

    /// Replication factor (intra-group).
    rf: usize,

    /// Total number of logical shards (S).
    shards: u32,
}

impl Topology {
    /// Create a new empty topology.
    pub fn new(shards: u32, rf: usize) -> Self {
        Self {
            nodes: HashMap::new(),
            groups: Vec::new(),
            rf,
            shards,
        }
    }

    /// Add a node to the topology.
    pub fn add_node(&mut self, node: Node) {
        let group_id = node.replica_group as usize;

        // Ensure group exists
        while self.groups.len() <= group_id {
            self.groups.push(Group::new(self.groups.len() as u32));
        }

        self.groups[group_id].add_node(node.id.clone());
        self.nodes.insert(node.id.clone(), node);
    }

    /// Get a node by ID.
    pub fn node(&self, id: &NodeId) -> Option<&Node> {
        self.nodes.get(id)
    }

    /// Get a mutable reference to a node by ID.
    pub fn node_mut(&mut self, id: &NodeId) -> Option<&mut Node> {
        self.nodes.get_mut(id)
    }

    /// Get all nodes.
    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.values()
    }

    /// Get a group by ID.
    pub fn group(&self, id: u32) -> Option<&Group> {
        self.groups.get(id as usize)
    }

    /// Iterate over all groups.
    pub fn groups(&self) -> impl Iterator<Item = &Group> {
        self.groups.iter()
    }

    /// Get the replication factor.
    pub fn rf(&self) -> usize {
        self.rf
    }

    /// Get the number of shards.
    pub fn shards(&self) -> u32 {
        self.shards
    }

    /// Get the number of replica groups.
    pub fn replica_group_count(&self) -> u32 {
        self.groups.len() as u32
    }

    /// Get healthy nodes in a specific group.
    pub fn healthy_nodes_in_group(&self, group_id: u32) -> Vec<&Node> {
        self.group(group_id)
            .map(|g| g.healthy_nodes(&self.nodes))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Existing tests updated for address field ---

    #[test]
    fn test_node_is_healthy() {
        let mut node = Node::new(
            NodeId::new("node1".to_string()),
            "http://example.com".to_string(),
            0,
        );

        // Joining status is not healthy
        assert!(!node.is_healthy());

        // Healthy status is healthy
        node.status = NodeStatus::Healthy;
        assert!(node.is_healthy());

        // Active status is healthy (synonym for Healthy)
        node.status = NodeStatus::Active;
        assert!(node.is_healthy());

        // Degraded status is not healthy (intermittent failures)
        node.status = NodeStatus::Degraded;
        assert!(!node.is_healthy());

        // Draining status is not healthy
        node.status = NodeStatus::Draining;
        assert!(!node.is_healthy());

        // Failed status is not healthy
        node.status = NodeStatus::Failed;
        assert!(!node.is_healthy());

        // Removed status is not healthy
        node.status = NodeStatus::Removed;
        assert!(!node.is_healthy());
    }

    #[test]
    fn test_group_node_count() {
        let mut group = Group::new(0);
        assert_eq!(group.node_count(), 0);

        group.add_node(NodeId::new("node1".to_string()));
        assert_eq!(group.node_count(), 1);

        group.add_node(NodeId::new("node2".to_string()));
        assert_eq!(group.node_count(), 2);

        // Adding duplicate node doesn't increase count
        group.add_node(NodeId::new("node1".to_string()));
        assert_eq!(group.node_count(), 2);
    }

    #[test]
    fn test_topology_replica_group_count() {
        let mut topology = Topology::new(64, 2);

        // Empty topology has 0 groups
        assert_eq!(topology.replica_group_count(), 0);

        // Add nodes to group 0
        topology.add_node(Node::new(
            NodeId::new("node1".to_string()),
            "http://example.com".to_string(),
            0,
        ));
        assert_eq!(topology.replica_group_count(), 1);

        // Add nodes to group 1
        topology.add_node(Node::new(
            NodeId::new("node2".to_string()),
            "http://example.com".to_string(),
            1,
        ));
        assert_eq!(topology.replica_group_count(), 2);

        // Add more nodes to existing groups
        topology.add_node(Node::new(
            NodeId::new("node3".to_string()),
            "http://example.com".to_string(),
            0,
        ));
        assert_eq!(topology.replica_group_count(), 2);
    }

    #[test]
    fn test_topology_nodes_iter() {
        let mut topology = Topology::new(64, 1);

        topology.add_node(Node::new(
            NodeId::new("node1".to_string()),
            "http://example.com".to_string(),
            0,
        ));
        topology.add_node(Node::new(
            NodeId::new("node2".to_string()),
            "http://example.com".to_string(),
            1,
        ));

        let nodes: Vec<_> = topology.nodes().collect();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_topology_groups_iter() {
        let mut topology = Topology::new(64, 1);

        topology.add_node(Node::new(
            NodeId::new("node1".to_string()),
            "http://example.com".to_string(),
            0,
        ));
        topology.add_node(Node::new(
            NodeId::new("node2".to_string()),
            "http://example.com".to_string(),
            1,
        ));

        let groups: Vec<_> = topology.groups().collect();
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn test_node_id_from_string() {
        let id: NodeId = "test-node".to_string().into();
        assert_eq!(id.as_str(), "test-node");
    }

    #[test]
    fn test_node_id_as_ref() {
        let id = NodeId::new("test-node".to_string());
        let s: &str = id.as_ref();
        assert_eq!(s, "test-node");
    }

    // --- New tests for state transitions ---

    #[test]
    fn test_state_transition_joining_to_active() {
        assert!(NodeStatus::Joining.can_transition_to(NodeStatus::Active));
    }

    #[test]
    fn test_state_transition_active_to_draining() {
        assert!(NodeStatus::Active.can_transition_to(NodeStatus::Draining));
    }

    #[test]
    fn test_state_transition_draining_to_removed() {
        assert!(NodeStatus::Draining.can_transition_to(NodeStatus::Removed));
    }

    #[test]
    fn test_state_transition_active_to_failed() {
        assert!(NodeStatus::Active.can_transition_to(NodeStatus::Failed));
    }

    #[test]
    fn test_state_transition_draining_to_failed() {
        assert!(NodeStatus::Draining.can_transition_to(NodeStatus::Failed));
    }

    #[test]
    fn test_state_transition_failed_to_active() {
        assert!(NodeStatus::Failed.can_transition_to(NodeStatus::Active));
    }

    #[test]
    fn test_state_transition_active_to_degraded() {
        assert!(NodeStatus::Active.can_transition_to(NodeStatus::Degraded));
    }

    #[test]
    fn test_state_transition_failed_to_degraded() {
        assert!(NodeStatus::Failed.can_transition_to(NodeStatus::Degraded));
    }

    #[test]
    fn test_state_transition_degraded_to_active() {
        assert!(NodeStatus::Degraded.can_transition_to(NodeStatus::Active));
    }

    #[test]
    fn test_state_transition_healthy_active_bidirectional() {
        assert!(NodeStatus::Healthy.can_transition_to(NodeStatus::Active));
        assert!(NodeStatus::Active.can_transition_to(NodeStatus::Healthy));
    }

    #[test]
    fn test_state_transition_same_state() {
        for status in [
            NodeStatus::Healthy,
            NodeStatus::Degraded,
            NodeStatus::Active,
            NodeStatus::Joining,
            NodeStatus::Draining,
            NodeStatus::Failed,
            NodeStatus::Removed,
        ] {
            assert!(status.can_transition_to(status));
        }
    }

    #[test]
    fn test_state_transition_invalid_joining_to_draining() {
        // Joining node must become Active before Draining
        assert!(!NodeStatus::Joining.can_transition_to(NodeStatus::Draining));
    }

    #[test]
    fn test_state_transition_invalid_joining_to_failed() {
        // Joining node cannot fail (not yet active)
        assert!(!NodeStatus::Joining.can_transition_to(NodeStatus::Failed));
    }

    #[test]
    fn test_state_transition_invalid_removed_to_anything() {
        // Removed is terminal
        assert!(!NodeStatus::Removed.can_transition_to(NodeStatus::Active));
        assert!(!NodeStatus::Removed.can_transition_to(NodeStatus::Failed));
    }

    #[test]
    fn test_node_set_status_valid_transition() {
        let mut node = Node::new(
            NodeId::new("node1".to_string()),
            "http://example.com".to_string(),
            0,
        );
        assert_eq!(node.status, NodeStatus::Joining);

        assert!(node.set_status(NodeStatus::Active).is_ok());
        assert_eq!(node.status, NodeStatus::Active);
    }

    #[test]
    fn test_node_set_status_invalid_transition() {
        let mut node = Node::with_status(
            NodeId::new("node1".to_string()),
            "http://example.com".to_string(),
            0,
            NodeStatus::Removed,
        );

        let result = node.set_status(NodeStatus::Active);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.from, NodeStatus::Removed);
        assert_eq!(err.to, NodeStatus::Active);
        // Status unchanged
        assert_eq!(node.status, NodeStatus::Removed);
    }

    // --- New tests for write eligibility ---

    #[test]
    fn test_write_eligible_healthy() {
        assert!(NodeStatus::Healthy.is_write_eligible_for(None));
        assert!(NodeStatus::Healthy.is_write_eligible_for(Some(0)));
    }

    #[test]
    fn test_write_eligible_active() {
        assert!(NodeStatus::Active.is_write_eligible_for(None));
        assert!(NodeStatus::Active.is_write_eligible_for(Some(0)));
    }

    #[test]
    fn test_write_eligible_degraded() {
        assert!(NodeStatus::Degraded.is_write_eligible_for(None));
        assert!(NodeStatus::Degraded.is_write_eligible_for(Some(0)));
    }

    #[test]
    fn test_write_eligible_joining() {
        // Joining nodes are not write-eligible
        assert!(!NodeStatus::Joining.is_write_eligible_for(None));
        assert!(!NodeStatus::Joining.is_write_eligible_for(Some(0)));
    }

    #[test]
    fn test_write_eligible_failed() {
        // Failed nodes are not write-eligible
        assert!(!NodeStatus::Failed.is_write_eligible_for(None));
        assert!(!NodeStatus::Failed.is_write_eligible_for(Some(0)));
    }

    #[test]
    fn test_write_eligible_removed() {
        // Removed nodes are not write-eligible
        assert!(!NodeStatus::Removed.is_write_eligible_for(None));
        assert!(!NodeStatus::Removed.is_write_eligible_for(Some(0)));
    }

    #[test]
    fn test_write_eligible_draining_non_drained_shard() {
        // Draining node is eligible for writes in general (no specific shard being checked)
        assert!(NodeStatus::Draining.is_write_eligible_for(None));
        // When Some(shard_id) is passed, it means that shard is being drained, so NOT eligible
        assert!(!NodeStatus::Draining.is_write_eligible_for(Some(5)));
    }

    #[test]
    fn test_write_eligible_draining_drained_shard() {
        // Draining node is NOT eligible for writes to shards being migrated off
        assert!(!NodeStatus::Draining.is_write_eligible_for(Some(3)));
    }

    #[test]
    fn test_node_is_write_eligible_for() {
        let node = Node::with_status(
            NodeId::new("node1".to_string()),
            "http://example.com".to_string(),
            0,
            NodeStatus::Active,
        );
        assert!(node.is_write_eligible_for(Some(0)));
    }

    // --- New tests for healthy_nodes ---

    #[test]
    fn test_group_healthy_nodes() {
        let mut group = Group::new(0);
        let mut all_nodes = HashMap::new();

        let node1 = Node::with_status(
            NodeId::new("node1".to_string()),
            "http://node1".to_string(),
            0,
            NodeStatus::Active,
        );
        let node2 = Node::with_status(
            NodeId::new("node2".to_string()),
            "http://node2".to_string(),
            0,
            NodeStatus::Degraded,
        );
        let node3 = Node::with_status(
            NodeId::new("node3".to_string()),
            "http://node3".to_string(),
            0,
            NodeStatus::Failed,
        );

        group.add_node(node1.id.clone());
        group.add_node(node2.id.clone());
        group.add_node(node3.id.clone());

        all_nodes.insert(node1.id.clone(), node1);
        all_nodes.insert(node2.id.clone(), node2);
        all_nodes.insert(node3.id.clone(), node3);

        let healthy = group.healthy_nodes(&all_nodes);
        assert_eq!(healthy.len(), 1); // Only node1 (Active) is healthy
        assert_eq!(healthy[0].id.as_str(), "node1");
    }

    #[test]
    fn test_topology_shards() {
        let topology = Topology::new(128, 3);
        assert_eq!(topology.shards(), 128);
    }

    #[test]
    fn test_topology_healthy_nodes_in_group() {
        let mut topology = Topology::new(64, 2);

        topology.add_node(Node::with_status(
            NodeId::new("node1".to_string()),
            "http://node1".to_string(),
            0,
            NodeStatus::Active,
        ));
        topology.add_node(Node::with_status(
            NodeId::new("node2".to_string()),
            "http://node2".to_string(),
            0,
            NodeStatus::Failed,
        ));
        topology.add_node(Node::with_status(
            NodeId::new("node3".to_string()),
            "http://node3".to_string(),
            1,
            NodeStatus::Active,
        ));

        let healthy_group0 = topology.healthy_nodes_in_group(0);
        assert_eq!(healthy_group0.len(), 1);
        assert_eq!(healthy_group0[0].id.as_str(), "node1");

        let healthy_group1 = topology.healthy_nodes_in_group(1);
        assert_eq!(healthy_group1.len(), 1);
        assert_eq!(healthy_group1[0].id.as_str(), "node3");
    }

    // --- Test for node mutation ---

    #[test]
    fn test_topology_node_mut() {
        let mut topology = Topology::new(64, 1);

        topology.add_node(Node::new(
            NodeId::new("node1".to_string()),
            "http://node1".to_string(),
            0,
        ));

        let node_id = NodeId::new("node1".to_string());
        {
            let node = topology.node(&node_id).unwrap();
            assert_eq!(node.status, NodeStatus::Joining);
        }

        {
            let node = topology.node_mut(&node_id).unwrap();
            node.set_status(NodeStatus::Active).unwrap();
        }

        let node = topology.node(&node_id).unwrap();
        assert_eq!(node.status, NodeStatus::Active);
    }

    // --- Display tests ---

    #[test]
    fn test_node_status_display() {
        assert_eq!(NodeStatus::Healthy.to_string(), "healthy");
        assert_eq!(NodeStatus::Degraded.to_string(), "degraded");
        assert_eq!(NodeStatus::Active.to_string(), "active");
        assert_eq!(NodeStatus::Joining.to_string(), "joining");
        assert_eq!(NodeStatus::Draining.to_string(), "draining");
        assert_eq!(NodeStatus::Failed.to_string(), "failed");
        assert_eq!(NodeStatus::Removed.to_string(), "removed");
    }

    #[test]
    fn test_transition_error_display() {
        let err = TransitionError {
            from: NodeStatus::Joining,
            to: NodeStatus::Draining,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("invalid state transition"));
        assert!(msg.contains("joining"));
        assert!(msg.contains("draining"));
    }

    // --- Plan §4 YAML deserialization test ---
    // This test verifies that a Topology can be deserialized from YAML
    #[test]
    fn test_topology_deserialize_from_yaml() {
        // YAML matching plan §4 example structure (RG=2, 6 nodes, RF=2)
        let yaml = r#"
shards: 64
rf: 2
groups:
  - id: 0
    nodes:
      - "meili-0"
      - "meili-1"
      - "meili-2"
  - id: 1
    nodes:
      - "meili-3"
      - "meili-4"
      - "meili-5"
nodes:
  meili-0:
    id: "meili-0"
    address: "http://meili-0.search.svc:7700"
    replica_group: 0
    status: Active
  meili-1:
    id: "meili-1"
    address: "http://meili-1.search.svc:7700"
    replica_group: 0
    status: Active
  meili-2:
    id: "meili-2"
    address: "http://meili-2.search.svc:7700"
    replica_group: 0
    status: Active
  meili-3:
    id: "meili-3"
    address: "http://meili-3.search.svc:7700"
    replica_group: 1
    status: Active
  meili-4:
    id: "meili-4"
    address: "http://meili-4.search.svc:7700"
    replica_group: 1
    status: Active
  meili-5:
    id: "meili-5"
    address: "http://meili-5.search.svc:7700"
    replica_group: 1
    status: Active
"#;

        let topology: Topology = serde_yaml::from_str(yaml).expect("Failed to deserialize topology from YAML");

        // Verify topology properties
        assert_eq!(topology.shards(), 64, "S should be 64");
        assert_eq!(topology.rf(), 2, "RF should be 2");
        assert_eq!(topology.replica_group_count(), 2, "RG should be 2");

        // Verify groups() iterator returns RG groups in ascending order
        let groups: Vec<_> = topology.groups().collect();
        assert_eq!(groups.len(), 2, "Should have 2 groups");
        assert_eq!(groups[0].id, 0, "First group should be ID 0");
        assert_eq!(groups[1].id, 1, "Second group should be ID 1");

        // Verify each group holds exactly its configured nodes
        let group0_nodes = groups[0].nodes();
        assert_eq!(group0_nodes.len(), 3, "Group 0 should have 3 nodes");
        assert!(group0_nodes.contains(&NodeId::new("meili-0".to_string())));
        assert!(group0_nodes.contains(&NodeId::new("meili-1".to_string())));
        assert!(group0_nodes.contains(&NodeId::new("meili-2".to_string())));

        let group1_nodes = groups[1].nodes();
        assert_eq!(group1_nodes.len(), 3, "Group 1 should have 3 nodes");
        assert!(group1_nodes.contains(&NodeId::new("meili-3".to_string())));
        assert!(group1_nodes.contains(&NodeId::new("meili-4".to_string())));
        assert!(group1_nodes.contains(&NodeId::new("meili-5".to_string())));

        // Verify node addresses are correct
        let node0 = topology.node(&NodeId::new("meili-0".to_string())).unwrap();
        assert_eq!(node0.address, "http://meili-0.search.svc:7700");
        assert_eq!(node0.replica_group, 0);
        assert_eq!(node0.status, NodeStatus::Active);

        let node5 = topology.node(&NodeId::new("meili-5".to_string())).unwrap();
        assert_eq!(node5.address, "http://meili-5.search.svc:7700");
        assert_eq!(node5.replica_group, 1);
        assert_eq!(node5.status, NodeStatus::Active);
    }

    // --- Plan §4 YAML example test ---
    // This test verifies that a Topology can be correctly built from the plan §4 YAML
    // example structure (RG=2, 6 nodes, RF=2)
    #[test]
    fn test_topology_from_plan_section_4_yaml_structure() {
        // Plan §4 YAML example:
        // replica_groups: 2
        // shards: 64
        // replication_factor: 2
        // Plan §4 YAML example:
        // replica_groups: 2
        // shards: 64
        // replication_factor: 2
        // nodes:
        //   - id: "meili-0", address: "http://meili-0.search.svc:7700", replica_group: 0
        //   - id: "meili-1", address: "http://meili-1.search.svc:7700", replica_group: 0
        //   - id: "meili-2", address: "http://meili-2.search.svc:7700", replica_group: 0
        //   - id: "meili-3", address: "http://meili-3.search.svc:7700", replica_group: 1
        //   - id: "meili-4", address: "http://meili-4.search.svc:7700", replica_group: 1
        //   - id: "meili-5", address: "http://meili-5.search.svc:7700", replica_group: 1

        let mut topology = Topology::new(64, 2); // S=64, RF=2

        // Add group 0 nodes (meili-0, meili-1, meili-2)
        topology.add_node(Node::new(
            NodeId::new("meili-0".to_string()),
            "http://meili-0.search.svc:7700".to_string(),
            0,
        ));
        topology.add_node(Node::new(
            NodeId::new("meili-1".to_string()),
            "http://meili-1.search.svc:7700".to_string(),
            0,
        ));
        topology.add_node(Node::new(
            NodeId::new("meili-2".to_string()),
            "http://meili-2.search.svc:7700".to_string(),
            0,
        ));

        // Add group 1 nodes (meili-3, meili-4, meili-5)
        topology.add_node(Node::new(
            NodeId::new("meili-3".to_string()),
            "http://meili-3.search.svc:7700".to_string(),
            1,
        ));
        topology.add_node(Node::new(
            NodeId::new("meili-4".to_string()),
            "http://meili-4.search.svc:7700".to_string(),
            1,
        ));
        topology.add_node(Node::new(
            NodeId::new("meili-5".to_string()),
            "http://meili-5.search.svc:7700".to_string(),
            1,
        ));

        // Verify topology properties
        assert_eq!(topology.shards(), 64, "S should be 64");
        assert_eq!(topology.rf(), 2, "RF should be 2");
        assert_eq!(topology.replica_group_count(), 2, "RG should be 2");

        // Verify groups() iterator returns RG groups in ascending order
        let groups: Vec<_> = topology.groups().collect();
        assert_eq!(groups.len(), 2, "Should have 2 groups");
        assert_eq!(groups[0].id, 0, "First group should be ID 0");
        assert_eq!(groups[1].id, 1, "Second group should be ID 1");

        // Verify each group holds exactly its configured nodes
        let group0_nodes = groups[0].nodes();
        assert_eq!(group0_nodes.len(), 3, "Group 0 should have 3 nodes");
        assert!(group0_nodes.contains(&NodeId::new("meili-0".to_string())));
        assert!(group0_nodes.contains(&NodeId::new("meili-1".to_string())));
        assert!(group0_nodes.contains(&NodeId::new("meili-2".to_string())));

        let group1_nodes = groups[1].nodes();
        assert_eq!(group1_nodes.len(), 3, "Group 1 should have 3 nodes");
        assert!(group1_nodes.contains(&NodeId::new("meili-3".to_string())));
        assert!(group1_nodes.contains(&NodeId::new("meili-4".to_string())));
        assert!(group1_nodes.contains(&NodeId::new("meili-5".to_string())));

        // Verify node addresses are correct
        let node0 = topology.node(&NodeId::new("meili-0".to_string())).unwrap();
        assert_eq!(node0.address, "http://meili-0.search.svc:7700");
        assert_eq!(node0.replica_group, 0);

        let node5 = topology.node(&NodeId::new("meili-5".to_string())).unwrap();
        assert_eq!(node5.address, "http://meili-5.search.svc:7700");
        assert_eq!(node5.replica_group, 1);
    }
}
