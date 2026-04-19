//! Topology management: node registry, groups, and health state.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    /// Node is joining the cluster (being provisioned).
    Joining,
    /// Node is draining (graceful shutdown, not accepting new writes).
    Draining,
    /// Node has failed (unplanned outage).
    Failed,
}

/// A single Meilisearch node in the topology.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Unique node identifier.
    pub id: NodeId,

    /// Node base URL.
    pub url: String,

    /// Current health status.
    pub status: NodeStatus,

    /// Replica group assignment (0-based).
    pub replica_group: u32,
}

impl Node {
    /// Create a new node.
    pub fn new(id: NodeId, url: String, replica_group: u32) -> Self {
        Self {
            id,
            url,
            status: NodeStatus::Joining,
            replica_group,
        }
    }

    /// Check if the node is healthy (can serve traffic).
    pub fn is_healthy(&self) -> bool {
        matches!(self.status, NodeStatus::Healthy)
    }
}

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

    /// Get the nodes in this group.
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    /// Get the number of nodes in this group.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
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
}

impl Topology {
    /// Create a new empty topology.
    pub fn new(rf: usize) -> Self {
        Self {
            nodes: HashMap::new(),
            groups: Vec::new(),
            rf,
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

    /// Get the number of replica groups.
    pub fn replica_group_count(&self) -> u32 {
        self.groups.len() as u32
    }
}
