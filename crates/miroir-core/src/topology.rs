//! Topology management: node registry, groups, health state, and state machine.

use crate::error::{MiroirError, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::time::Instant;

/// Unique identifier for a node.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

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

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Health status of a node, with state-machine transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    /// Node is healthy and serving traffic.
    #[default]
    Healthy,
    /// Node is degraded (timeouts, not full disconnect) but still serving.
    Degraded,
    /// Node is draining — graceful shutdown, still serves shards it owns.
    Draining,
    /// Node has failed (unplanned outage).
    Failed,
    /// Node is joining the cluster (being provisioned).
    Joining,
    /// Node is active — fully operational after joining.
    Active,
    /// Node has been removed from the cluster.
    Removed,
}

impl NodeStatus {
    /// Attempt a state transition. Returns the new status on success.
    ///
    /// Legal transitions (plan §2 topology-change verbs):
    /// - (new) → Joining          (admin API: POST /_miroir/nodes)
    /// - Joining → Active         (migration complete)
    /// - Active → Draining        (admin API: POST /_miroir/nodes/{id}/drain)
    /// - Draining → Removed       (migration complete)
    /// - Active/Draining → Failed (health check detects failure)
    /// - Failed → Active          (health check recovery)
    /// - Active/Failed → Degraded (partial health: timeouts)
    /// - Degraded → Active        (health restored)
    pub fn transition_to(self, target: NodeStatus) -> Result<NodeStatus> {
        use NodeStatus::*;

        let legal = match (self, target) {
            // Normal lifecycle
            (Joining, Active) => true,
            (Active, Draining) => true,
            (Draining, Removed) => true,

            // Failure detection
            (Active, Failed) => true,
            (Draining, Failed) => true,

            // Recovery
            (Failed, Active) => true,

            // Degraded
            (Active, Degraded) => true,
            (Failed, Degraded) => true,
            (Degraded, Active) => true,

            // Idempotent
            (Active, Active)
            | (Failed, Failed)
            | (Degraded, Degraded)
            | (Joining, Joining)
            | (Draining, Draining) => true,

            // Healthy is an alias for Active in transitions
            (Healthy, _) | (_, Healthy) => false,

            _ => false,
        };

        if legal {
            Ok(target)
        } else {
            Err(MiroirError::Topology(format!(
                "illegal state transition: {:?} → {:?}",
                self, target
            )))
        }
    }

    /// Check if a node in this status is serving reads.
    pub fn is_readable(self) -> bool {
        matches!(
            self,
            NodeStatus::Active | NodeStatus::Healthy | NodeStatus::Degraded | NodeStatus::Draining
        )
    }

    /// Check if a node in this status can accept any writes unconditionally.
    pub fn is_active(self) -> bool {
        matches!(self, NodeStatus::Active | NodeStatus::Healthy | NodeStatus::Degraded)
    }
}

/// A single Meilisearch node in the topology.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Unique node identifier.
    pub id: NodeId,

    /// Node base URL (e.g., "http://meili-0.search.svc:7700").
    pub address: String,

    /// Current health status.
    #[serde(default)]
    pub status: NodeStatus,

    /// Replica group assignment (0-based).
    pub replica_group: u32,

    /// Instant of the last successful health check.
    #[serde(skip)]
    pub last_seen: Option<Instant>,

    /// Error message from the last failed health check.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl Node {
    /// Create a new node (starts in Joining state).
    pub fn new(id: NodeId, address: String, replica_group: u32) -> Self {
        Self {
            id,
            address,
            status: NodeStatus::Joining,
            replica_group,
            last_seen: None,
            last_error: None,
        }
    }

    /// Check if this node can receive writes for a shard.
    ///
    /// `shard_affected` is true when the shard is being migrated away from this
    /// node during a drain. Draining nodes still accept writes for shards they
    /// still own (`shard_affected = false`).
    pub fn is_write_eligible_for(&self, shard_affected: bool) -> bool {
        match self.status {
            NodeStatus::Active | NodeStatus::Healthy | NodeStatus::Degraded => true,
            NodeStatus::Draining => !shard_affected,
            NodeStatus::Joining | NodeStatus::Failed | NodeStatus::Removed => false,
        }
    }

    /// Check if the node is healthy (can serve traffic).
    pub fn is_healthy(&self) -> bool {
        matches!(
            self.status,
            NodeStatus::Active | NodeStatus::Healthy | NodeStatus::Degraded
        )
    }

    /// Attempt a state transition on this node.
    pub fn transition_to(&mut self, target: NodeStatus) -> Result<()> {
        self.status = self.status.transition_to(target)?;
        Ok(())
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

    /// Node IDs in this group.
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

    /// Get healthy (Active/Healthy/Degraded) nodes in this group.
    ///
    /// Requires the topology's node map to resolve node IDs to full `Node` refs.
    pub fn healthy_nodes<'a>(&self, node_map: &'a HashMap<NodeId, Node>) -> Vec<&'a Node> {
        self.nodes
            .iter()
            .filter_map(|id| node_map.get(id))
            .filter(|n| n.is_healthy())
            .collect()
    }
}

/// Cluster topology: groups, nodes, and health state.
///
/// Serializes/deserializes from plan §4 YAML format:
/// ```yaml
/// shards: 64
/// replica_groups: 2
/// rf: 1
/// nodes:
///   - id: "meili-0"
///     address: "http://meili-0:7700"
///     replica_group: 0
/// ```
///
/// Groups are derived from node `replica_group` fields and rebuilt automatically.
#[derive(Debug, Clone)]
pub struct Topology {
    /// Total number of logical shards.
    pub shards: u32,

    /// Number of replica groups.
    pub replica_groups: u32,

    /// Replication factor (intra-group).
    pub rf: usize,

    /// All nodes in the cluster.
    pub nodes: Vec<Node>,

    /// Derived group index (rebuilt from nodes).
    groups: Vec<Group>,

    /// Node ID → Vec index lookup.
    node_index: HashMap<NodeId, usize>,
}

impl Topology {
    /// Create a new empty topology.
    pub fn new(shards: u32, replica_groups: u32, rf: usize) -> Self {
        Self {
            shards,
            replica_groups,
            rf,
            nodes: Vec::new(),
            groups: (0..replica_groups).map(Group::new).collect(),
            node_index: HashMap::new(),
        }
    }

    /// Add a node to the topology.
    pub fn add_node(&mut self, node: Node) {
        let idx = self.nodes.len();
        self.node_index.insert(node.id.clone(), idx);
        let group_id = node.replica_group;
        self.nodes.push(node);
        self.rebuild_groups();
        self.replica_groups = self.replica_groups.max(group_id + 1);
    }

    /// Get a node by ID.
    pub fn node(&self, id: &NodeId) -> Option<&Node> {
        self.node_index.get(id).map(|&i| &self.nodes[i])
    }

    /// Get a mutable node by ID.
    pub fn node_mut(&mut self, id: &NodeId) -> Option<&mut Node> {
        self.node_index.get(id).copied().map(move |i| &mut self.nodes[i])
    }

    /// Iterate over all nodes.
    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter()
    }

    /// Get a group by ID.
    pub fn group(&self, id: u32) -> Option<&Group> {
        self.groups.get(id as usize)
    }

    /// Iterate over all groups in ascending order by ID.
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

    /// Build a HashMap<NodeId, Node> for use with Group::healthy_nodes.
    pub fn node_map(&self) -> HashMap<NodeId, Node> {
        self.nodes.iter().map(|n| (n.id.clone(), n.clone())).collect()
    }

    /// Remove a node from the topology.
    pub fn remove_node(&mut self, id: &NodeId) -> bool {
        if let Some(&idx) = self.node_index.get(id) {
            self.node_index.remove(id);
            self.nodes.remove(idx);
            // Rebuild indices
            self.node_index.clear();
            for (i, node) in self.nodes.iter().enumerate() {
                self.node_index.insert(node.id.clone(), i);
            }
            self.rebuild_groups();
            true
        } else {
            false
        }
    }

    /// Remove all nodes in a replica group and the group itself.
    pub fn remove_group(&mut self, group_id: u32) -> bool {
        // Find all nodes in this group
        let nodes_to_remove: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|n| n.replica_group == group_id)
            .map(|n| n.id.clone())
            .collect();

        if nodes_to_remove.is_empty() {
            return false;
        }

        // Remove all nodes in the group
        for node_id in nodes_to_remove {
            self.remove_node(&node_id);
        }

        true
    }

    fn rebuild_groups(&mut self) {
        let num_groups = self
            .nodes
            .iter()
            .map(|n| n.replica_group)
            .max()
            .map_or(self.replica_groups as usize, |m| (m as usize + 1).max(self.replica_groups as usize));

        self.groups = (0..num_groups).map(|i| Group::new(i as u32)).collect();
        for node in &self.nodes {
            if let Some(group) = self.groups.get_mut(node.replica_group as usize) {
                group.add_node(node.id.clone());
            }
        }
    }
}

impl Serialize for Topology {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct TopologyData {
            shards: u32,
            replica_groups: u32,
            rf: usize,
            nodes: Vec<Node>,
        }
        TopologyData {
            shards: self.shards,
            replica_groups: self.replica_groups,
            rf: self.rf,
            nodes: self.nodes.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Topology {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct TopologyData {
            shards: u32,
            #[serde(default)]
            replica_groups: u32,
            #[serde(alias = "replication_factor", default)]
            rf: usize,
            #[serde(default)]
            nodes: Vec<Node>,
        }

        let data = TopologyData::deserialize(deserializer)?;
        let mut topo = Self {
            shards: data.shards,
            replica_groups: data.replica_groups,
            rf: data.rf,
            nodes: data.nodes,
            groups: Vec::new(),
            node_index: HashMap::new(),
        };
        // Build lookup index
        for (i, node) in topo.nodes.iter().enumerate() {
            topo.node_index.insert(node.id.clone(), i);
        }
        // Derive replica_groups from nodes if not set
        if topo.replica_groups == 0 && !topo.nodes.is_empty() {
            topo.replica_groups = topo.nodes.iter().map(|n| n.replica_group).max().unwrap() + 1;
        }
        topo.rebuild_groups();
        Ok(topo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── YAML deserialization ──────────────────────────────────────────

    #[test]
    fn deserialize_plan_s4_yaml_example() {
        let yaml = r#"
shards: 64
replica_groups: 2
rf: 1
nodes:
  - id: "meili-0"
    address: "http://meili-0.search.svc:7700"
    replica_group: 0
  - id: "meili-1"
    address: "http://meili-1.search.svc:7700"
    replica_group: 0
  - id: "meili-2"
    address: "http://meili-2.search.svc:7700"
    replica_group: 0
  - id: "meili-3"
    address: "http://meili-3.search.svc:7700"
    replica_group: 1
  - id: "meili-4"
    address: "http://meili-4.search.svc:7700"
    replica_group: 1
  - id: "meili-5"
    address: "http://meili-5.search.svc:7700"
    replica_group: 1
"#;
        let topo: Topology = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(topo.shards, 64);
        assert_eq!(topo.replica_groups, 2);
        assert_eq!(topo.rf, 1);
        assert_eq!(topo.nodes.len(), 6);

        // All nodes default to Healthy (first variant, used as serde default)
        for node in &topo.nodes {
            assert_eq!(node.status, NodeStatus::Healthy);
        }
    }

    #[test]
    fn deserialize_with_replication_factor_alias() {
        let yaml = r#"
shards: 32
replica_groups: 1
replication_factor: 2
nodes:
  - id: "n0"
    address: "http://n0:7700"
    replica_group: 0
"#;
        let topo: Topology = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(topo.rf, 2);
    }

    // ── Groups iterator ───────────────────────────────────────────────

    #[test]
    fn groups_returns_rg_groups_in_ascending_order() {
        let topo = make_test_topology();
        let groups: Vec<&Group> = topo.groups().collect();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].id, 0);
        assert_eq!(groups[1].id, 1);
    }

    #[test]
    fn each_group_holds_exactly_its_configured_nodes() {
        let topo = make_test_topology();
        let g0 = topo.group(0).unwrap();
        let g1 = topo.group(1).unwrap();

        assert_eq!(g0.node_count(), 3);
        assert_eq!(g1.node_count(), 3);

        // Group 0: meili-{0,1,2}
        let g0_ids: Vec<&str> = g0.nodes().iter().map(|n| n.as_str()).collect();
        assert!(g0_ids.contains(&"meili-0"));
        assert!(g0_ids.contains(&"meili-1"));
        assert!(g0_ids.contains(&"meili-2"));

        // Group 1: meili-{3,4,5}
        let g1_ids: Vec<&str> = g1.nodes().iter().map(|n| n.as_str()).collect();
        assert!(g1_ids.contains(&"meili-3"));
        assert!(g1_ids.contains(&"meili-4"));
        assert!(g1_ids.contains(&"meili-5"));
    }

    #[test]
    fn topology_nodes_iterator() {
        let topo = make_test_topology();
        let all_nodes: Vec<&Node> = topo.nodes().collect();
        assert_eq!(all_nodes.len(), 6);
    }

    #[test]
    fn deserialize_auto_derives_replica_groups() {
        let yaml = r#"
shards: 32
rf: 1
nodes:
  - id: "n0"
    address: "http://n0:7700"
    replica_group: 2
  - id: "n1"
    address: "http://n1:7700"
    replica_group: 2
"#;
        let topo: Topology = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(topo.replica_groups, 3);
    }

    // ── State machine ─────────────────────────────────────────────────

    #[test]
    fn legal_transitions() {
        use NodeStatus::*;

        let cases: Vec<(NodeStatus, NodeStatus)> = vec![
            (Joining, Active),
            (Active, Draining),
            (Draining, Removed),
            (Active, Failed),
            (Draining, Failed),
            (Failed, Active),
            (Active, Degraded),
            (Failed, Degraded),
            (Degraded, Active),
            // Idempotent
            (Active, Active),
            (Failed, Failed),
            (Degraded, Degraded),
            (Joining, Joining),
            (Draining, Draining),
        ];

        for (from, to) in cases {
            assert!(
                from.transition_to(to).is_ok(),
                "expected {:?} → {:?} to succeed",
                from,
                to
            );
        }
    }

    #[test]
    fn illegal_transitions() {
        use NodeStatus::*;

        let cases: Vec<(NodeStatus, NodeStatus)> = vec![
            // Skip steps
            (Joining, Draining),
            (Joining, Failed),
            (Joining, Removed),
            (Joining, Degraded),
            // Can't go back from Draining to Active
            (Draining, Active),
            (Draining, Joining),
            // Can't recover to Joining
            (Active, Joining),
            (Failed, Joining),
            (Degraded, Joining),
            // Removed is terminal
            (Removed, Active),
            (Removed, Joining),
            (Removed, Failed),
            (Removed, Degraded),
            (Removed, Draining),
            // Healthy not used in transitions
            (Healthy, Active),
            (Active, Healthy),
            // More illegal paths
            (Failed, Draining),
            (Failed, Removed),
            (Degraded, Failed),
            (Degraded, Draining),
            (Degraded, Removed),
        ];

        for (from, to) in cases {
            let result = from.transition_to(to);
            assert!(
                result.is_err(),
                "expected {:?} → {:?} to be rejected, but got Ok({:?})",
                from,
                to,
                result.unwrap()
            );
        }
    }

    #[test]
    fn node_transition_method() {
        let mut node = Node::new(
            NodeId::new("n0".into()),
            "http://n0:7700".into(),
            0,
        );
        assert_eq!(node.status, NodeStatus::Joining);

        node.transition_to(NodeStatus::Active).unwrap();
        assert_eq!(node.status, NodeStatus::Active);

        // Illegal: Active → Joining
        assert!(node.transition_to(NodeStatus::Joining).is_err());
        assert_eq!(node.status, NodeStatus::Active); // unchanged
    }

    #[test]
    fn full_lifecycle_joining_to_removed() {
        let mut node = Node::new(
            NodeId::new("n0".into()),
            "http://n0:7700".into(),
            0,
        );
        assert_eq!(node.status, NodeStatus::Joining);

        node.transition_to(NodeStatus::Active).unwrap();
        assert_eq!(node.status, NodeStatus::Active);

        node.transition_to(NodeStatus::Draining).unwrap();
        assert_eq!(node.status, NodeStatus::Draining);

        node.transition_to(NodeStatus::Removed).unwrap();
        assert_eq!(node.status, NodeStatus::Removed);
    }

    #[test]
    fn failure_recovery_path() {
        let mut node = Node::new(NodeId::new("n0".into()), "http://n0:7700".into(), 0);
        node.transition_to(NodeStatus::Active).unwrap();
        node.transition_to(NodeStatus::Failed).unwrap();
        node.transition_to(NodeStatus::Degraded).unwrap();
        node.transition_to(NodeStatus::Active).unwrap();
        assert_eq!(node.status, NodeStatus::Active);
    }

    // ── Write eligibility ─────────────────────────────────────────────

    #[test]
    fn write_eligibility_correctness_table() {
        use NodeStatus::*;

        // (status, shard_affected, expected_write_eligible)
        let cases: Vec<(NodeStatus, bool, bool)> = vec![
            // Active/Healthy/Degraded: always eligible
            (Active, true, true),
            (Active, false, true),
            (Healthy, true, true),
            (Healthy, false, true),
            (Degraded, true, true),
            (Degraded, false, true),

            // Draining: eligible only for shards still owned (not affected)
            (Draining, false, true),
            (Draining, true, false),

            // Joining/Failed/Removed: never eligible
            (Joining, false, false),
            (Joining, true, false),
            (Failed, false, false),
            (Failed, true, false),
            (Removed, false, false),
            (Removed, true, false),
        ];

        for (status, shard_affected, expected) in cases {
            let node = Node {
                id: NodeId::new("test".into()),
                address: "http://test:7700".into(),
                replica_group: 0,
                status,
                last_seen: None,
                last_error: None,
            };
            let result = node.is_write_eligible_for(shard_affected);
            assert_eq!(
                result, expected,
                "is_write_eligible_for(shard_affected={}) with status {:?} = {}, expected {}",
                shard_affected, status, result, expected
            );
        }
    }

    // ── Group healthy_nodes ───────────────────────────────────────────

    #[test]
    fn healthy_nodes_returns_only_active_nodes() {
        let mut topo = make_test_topology();

        // Activate first 4 nodes, fail the 5th, leave 6th as Joining
        for i in 0..4 {
            topo.nodes[i].status = NodeStatus::Active;
        }
        topo.nodes[4].status = NodeStatus::Failed;
        topo.nodes[5].status = NodeStatus::Joining;

        let node_map = topo.node_map();
        let g0_healthy = topo.group(0).unwrap().healthy_nodes(&node_map);
        let g1_healthy = topo.group(1).unwrap().healthy_nodes(&node_map);

        // Group 0: nodes 0,1,2 all Active
        assert_eq!(g0_healthy.len(), 3);
        // Group 1: node 3 Active, node 4 Failed, node 5 Joining
        assert_eq!(g1_healthy.len(), 1);
        assert_eq!(g1_healthy[0].id, NodeId::new("meili-3".into()));
    }

    #[test]
    fn healthy_nodes_includes_degraded() {
        let mut topo = make_test_topology();
        topo.nodes[0].status = NodeStatus::Active;
        topo.nodes[1].status = NodeStatus::Degraded;
        topo.nodes[2].status = NodeStatus::Failed;

        let node_map = topo.node_map();
        let healthy = topo.group(0).unwrap().healthy_nodes(&node_map);
        assert_eq!(healthy.len(), 2);
    }

    // ── Topology serialization round-trip ─────────────────────────────

    #[test]
    fn topology_round_trip_yaml() {
        let mut topo = Topology::new(64, 2, 1);
        for i in 0..6 {
            let rg = if i < 3 { 0 } else { 1 };
            topo.add_node(Node::new(
                NodeId::new(format!("meili-{i}")),
                format!("http://meili-{i}.search.svc:7700"),
                rg,
            ));
        }

        let yaml = serde_yaml::to_string(&topo).unwrap();
        let topo2: Topology = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(topo2.shards, 64);
        assert_eq!(topo2.replica_groups, 2);
        assert_eq!(topo2.rf, 1);
        assert_eq!(topo2.nodes.len(), 6);
        assert_eq!(topo2.group(0).unwrap().node_count(), 3);
        assert_eq!(topo2.group(1).unwrap().node_count(), 3);
    }

    // ── NodeId conversions ────────────────────────────────────────────

    #[test]
    fn nodeid_from_string_and_as_ref() {
        let id: NodeId = "test-node".to_string().into();
        assert_eq!(id.as_str(), "test-node");
        assert_eq!(AsRef::<str>::as_ref(&id), "test-node");
    }

    #[test]
    fn nodeid_display_impl() {
        let id = NodeId::new("my-node".to_string());
        assert_eq!(format!("{}", id), "my-node");
    }

    // ── NodeStatus helpers ────────────────────────────────────────────

    #[test]
    fn is_readable_covers_all_statuses() {
        use NodeStatus::*;
        assert!(Active.is_readable());
        assert!(Healthy.is_readable());
        assert!(Degraded.is_readable());
        assert!(Draining.is_readable());
        assert!(!Failed.is_readable());
        assert!(!Joining.is_readable());
        assert!(!Removed.is_readable());
    }

    #[test]
    fn is_active_covers_all_statuses() {
        use NodeStatus::*;
        assert!(Active.is_active());
        assert!(Healthy.is_active());
        assert!(Degraded.is_active());
        assert!(!Draining.is_active());
        assert!(!Failed.is_active());
        assert!(!Joining.is_active());
        assert!(!Removed.is_active());
    }

    // ── Node::is_healthy ──────────────────────────────────────────────

    #[test]
    fn node_is_healthy_covers_all_statuses() {
        use NodeStatus::*;
        for (status, expected) in [
            (Active, true),
            (Healthy, true),
            (Degraded, true),
            (Draining, false),
            (Failed, false),
            (Joining, false),
            (Removed, false),
        ] {
            let node = Node {
                id: NodeId::new("test".into()),
                address: "http://test:7700".into(),
                replica_group: 0,
                status,
                last_seen: None,
                last_error: None,
            };
            assert_eq!(node.is_healthy(), expected, "{:?} is_healthy", status);
        }
    }

    // ── Group::add_node duplicate prevention ──────────────────────────

    #[test]
    fn group_add_node_prevents_duplicates() {
        let mut g = Group::new(0);
        g.add_node(NodeId::new("a".into()));
        g.add_node(NodeId::new("a".into()));
        g.add_node(NodeId::new("b".into()));
        assert_eq!(g.node_count(), 2);
    }

    // ── Topology with auto-derived replica_groups ─────────────────────

    #[test]
    fn topology_auto_derives_replica_groups_from_nodes() {
        let mut topo = Topology::new(64, 1, 1);
        topo.add_node(Node::new(NodeId::new("n0".into()), "http://n0:7700".into(), 0));
        topo.add_node(Node::new(NodeId::new("n1".into()), "http://n1:7700".into(), 2));
        // replica_groups should auto-derive to 3
        assert_eq!(topo.replica_groups, 3);
        assert!(topo.group(2).is_some());
    }

    #[test]
    fn topology_node_lookup() {
        let mut topo = make_test_topology();
        assert!(topo.node(&NodeId::new("meili-0".into())).is_some());
        assert!(topo.node(&NodeId::new("nonexistent".into())).is_none());

        // Mutate via node_mut
        let id = NodeId::new("meili-0".into());
        topo.node_mut(&id).unwrap().status = NodeStatus::Failed;
        assert_eq!(topo.node(&id).unwrap().status, NodeStatus::Failed);
    }

    #[test]
    fn topology_replica_group_count() {
        let topo = make_test_topology();
        assert_eq!(topo.replica_group_count(), 2);
    }

    // ── Helpers ───────────────────────────────────────────────────────

    fn make_test_topology() -> Topology {
        let mut topo = Topology::new(64, 2, 1);
        for i in 0..6 {
            let rg = if i < 3 { 0 } else { 1 };
            let mut node = Node::new(
                NodeId::new(format!("meili-{i}")),
                format!("http://meili-{i}.search.svc:7700"),
                rg,
            );
            // Default from Node::new is Joining, set to Active for tests
            node.status = NodeStatus::Active;
            topo.add_node(node);
        }
        topo
    }
}
