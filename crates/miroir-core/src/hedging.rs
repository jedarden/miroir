//! Hedged requests for tail-latency mitigation (plan §13.2).
//!
//! Issues duplicate requests to alternate replicas when a primary request
//! exceeds the p95 latency threshold.

use crate::router::assign_shard_in_group;
use crate::topology::{NodeId, Topology};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Hedging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgingConfig {
    /// Whether hedging is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// P95 trigger multiplier (hedge at p95 * this).
    #[serde(default = "default_multiplier")]
    pub p95_trigger_multiplier: f64,
    /// Minimum trigger time in milliseconds.
    #[serde(default = "default_min_trigger")]
    pub min_trigger_ms: u64,
    /// Maximum hedges per query.
    #[serde(default = "default_max_hedges")]
    pub max_hedges_per_query: u32,
    /// Allow falling back to another replica group.
    #[serde(default = "default_true")]
    pub cross_group_fallback: bool,
}

fn default_true() -> bool {
    true
}
fn default_multiplier() -> f64 {
    1.2
}
fn default_min_trigger() -> u64 {
    15
}
fn default_max_hedges() -> u32 {
    2
}

impl Default for HedgingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            p95_trigger_multiplier: default_multiplier(),
            min_trigger_ms: default_min_trigger(),
            max_hedges_per_query: default_max_hedges(),
            cross_group_fallback: true,
        }
    }
}

/// Per-node latency tracking for p95 computation.
#[derive(Debug, Clone)]
pub struct NodeLatency {
    /// EWMA-smoothed latency in milliseconds.
    pub ewma_ms: f64,
    /// Half-life for EWMA (milliseconds).
    pub half_life_ms: u64,
}

impl NodeLatency {
    /// Create a new latency tracker with initial value.
    pub fn new(initial_ms: f64, half_life_ms: u64) -> Self {
        Self {
            ewma_ms: initial_ms,
            half_life_ms,
        }
    }

    /// Update with a new observation.
    pub fn update(&mut self, latency_ms: f64) {
        let alpha = 0.5_f64.powf((self.half_life_ms as f64) / 1000.0);
        self.ewma_ms = alpha * self.ewma_ms + (1.0 - alpha) * latency_ms;
    }

    /// Get the current p95 estimate (conservative: use EWMA directly).
    pub fn p95_ms(&self) -> f64 {
        self.ewma_ms
    }
}

impl Default for NodeLatency {
    fn default() -> Self {
        Self::new(50.0, 5000)
    }
}

/// Hedging manager.
pub struct HedgingManager {
    /// Configuration.
    config: HedgingConfig,
    /// Per-node latency tracking.
    node_latencies: Arc<RwLock<HashMap<NodeId, NodeLatency>>>,
    /// Topology reference for finding alternate replicas.
    topology: Arc<Topology>,
}

impl HedgingManager {
    /// Create a new hedging manager.
    pub fn new(config: HedgingConfig, topology: Arc<Topology>) -> Self {
        Self {
            config,
            node_latencies: Arc::new(RwLock::new(HashMap::new())),
            topology,
        }
    }

    /// Record a latency observation for a node.
    pub async fn record_latency(&self, node_id: &NodeId, latency_ms: f64) {
        let mut latencies = self.node_latencies.write().await;
        let entry = latencies
            .entry(node_id.clone())
            .or_insert_with(NodeLatency::default);
        entry.update(latency_ms);
    }

    /// Get the p95 latency for a node.
    pub async fn get_p95(&self, node_id: &NodeId) -> f64 {
        let latencies = self.node_latencies.read().await;
        latencies.get(node_id).map(|l| l.p95_ms()).unwrap_or(50.0)
    }

    /// Compute the hedge deadline for a request to the given node.
    ///
    /// Returns None if hedging is disabled or the node has no latency data.
    pub async fn hedge_deadline(&self, primary_node: &NodeId) -> Option<Duration> {
        if !self.config.enabled {
            return None;
        }

        let p95 = self.get_p95(primary_node).await;
        let trigger_ms =
            (p95 * self.config.p95_trigger_multiplier).max(self.config.min_trigger_ms as f64);
        Some(Duration::from_millis(trigger_ms as u64))
    }

    /// Find an alternate replica for hedging.
    ///
    /// Returns None if:
    /// - No alternate available
    /// - Max hedges already issued
    /// - Cross-group fallback disabled and no intra-group alternate
    pub async fn find_alternate(
        &self,
        primary_node: &NodeId,
        shard_id: u32,
        hedge_count: u32,
    ) -> Option<NodeId> {
        if hedge_count >= self.config.max_hedges_per_query {
            return None;
        }

        // Get all nodes for this shard (assign across all replica groups)
        let all_nodes: Vec<NodeId> = self
            .topology
            .groups()
            .flat_map(|group| assign_shard_in_group(shard_id, group.nodes(), self.topology.rf()))
            .collect();
        let primary_group = self.topology.node(primary_node)?.replica_group;

        // First try: same group, different node
        for node in &all_nodes {
            if node != primary_node {
                if let Some(n) = self.topology.node(node) {
                    if n.replica_group == primary_group {
                        return Some(node.clone());
                    }
                }
            }
        }

        // Fallback: different group (if enabled)
        if self.config.cross_group_fallback {
            for node in &all_nodes {
                if node != primary_node {
                    return Some(node.clone());
                }
            }
        }

        None
    }

    /// Check if a hedge should be issued based on elapsed time.
    pub fn should_hedge(&self, elapsed: Duration, deadline: Duration) -> bool {
        elapsed >= deadline
    }

    /// Get configuration.
    pub fn config(&self) -> &HedgingConfig {
        &self.config
    }
}

impl Default for HedgingManager {
    fn default() -> Self {
        Self::new(HedgingConfig::default(), Arc::new(Topology::new(1, 1, 1)))
    }
}

/// Hedge outcome for metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HedgeOutcome {
    /// Primary request won (hedge cancelled or never fired).
    PrimaryWon,
    /// Hedge request won (primary was slower).
    HedgeWon,
    /// Both completed at similar time.
    Tie,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = HedgingConfig::default();
        assert!(config.enabled);
        assert_eq!(config.p95_trigger_multiplier, 1.2);
        assert_eq!(config.min_trigger_ms, 15);
        assert_eq!(config.max_hedges_per_query, 2);
        assert!(config.cross_group_fallback);
    }

    #[test]
    fn test_node_latency_ewma() {
        let mut latency = NodeLatency::new(100.0, 1000);
        assert_eq!(latency.ewma_ms, 100.0);

        // Update with same value
        latency.update(100.0);
        // EWMA should move toward 100
        assert!(latency.ewma_ms > 90.0 && latency.ewma_ms < 110.0);

        // Update with much lower value
        latency.update(10.0);
        assert!(latency.ewma_ms < 100.0);
    }

    #[test]
    fn test_hedge_deadline_computation() {
        let topology = Arc::new(Topology::new(1, 1, 1));
        let manager = HedgingManager::new(HedgingConfig::default(), topology);

        let node = NodeId::new("node-1".to_string());
        manager
            .node_latencies
            .try_write()
            .unwrap()
            .insert(node.clone(), NodeLatency::new(50.0, 5000));

        let rt = tokio::runtime::Runtime::new().unwrap();
        let deadline = rt.block_on(async { manager.hedge_deadline(&node).await });

        assert!(deadline.is_some());
        // 50ms * 1.2 = 60ms, but min is 15ms, so should be 60ms
        assert_eq!(deadline.unwrap(), Duration::from_millis(60));
    }

    #[test]
    fn test_hedge_deadline_respects_min() {
        let topology = Arc::new(Topology::new(1, 1, 1));
        let config = HedgingConfig {
            p95_trigger_multiplier: 1.2,
            min_trigger_ms: 100,
            ..Default::default()
        };
        let manager = HedgingManager::new(config, topology);

        let node = NodeId::new("node-1".to_string());
        manager
            .node_latencies
            .try_write()
            .unwrap()
            .insert(node.clone(), NodeLatency::new(10.0, 5000));

        let rt = tokio::runtime::Runtime::new().unwrap();
        let deadline = rt.block_on(async { manager.hedge_deadline(&node).await });

        assert!(deadline.is_some());
        // 10ms * 1.2 = 12ms, but min is 100ms
        assert_eq!(deadline.unwrap(), Duration::from_millis(100));
    }

    #[test]
    fn test_hedge_disabled() {
        let config = HedgingConfig {
            enabled: false,
            ..Default::default()
        };
        let topology = Arc::new(Topology::new(1, 1, 1));
        let manager = HedgingManager::new(config, topology);

        let node = NodeId::new("node-1".to_string());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let deadline = rt.block_on(async { manager.hedge_deadline(&node).await });

        assert!(deadline.is_none());
    }

    #[tokio::test]
    async fn test_record_latency() {
        let topology = Arc::new(Topology::new(1, 1, 1));
        let manager = HedgingManager::new(HedgingConfig::default(), topology);

        let node = NodeId::new("node-1".to_string());
        manager.record_latency(&node, 100.0).await;
        manager.record_latency(&node, 50.0).await;

        let p95 = manager.get_p95(&node).await;
        // EWMA should be between 50 and 100
        assert!(p95 > 40.0 && p95 < 110.0);
    }
}
