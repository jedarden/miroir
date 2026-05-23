//! Adaptive replica selection using EWMA scoring (plan §13.3).
//!
//! Replaces round-robin with latency-aware selection using EWMA-smoothed
//! metrics: latency p95, in-flight request count, and error rate.

use crate::error::{MiroirError, Result};
use crate::topology::{Group, NodeId};
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Replica selection strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionStrategy {
    /// EWMA-based adaptive selection.
    Adaptive,
    /// Round-robin selection.
    RoundRobin,
    /// Random selection.
    Random,
}

impl Default for SelectionStrategy {
    fn default() -> Self {
        Self::Adaptive
    }
}

/// Replica selection configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaSelectionConfig {
    /// Selection strategy.
    #[serde(default)]
    pub strategy: String,
    /// Latency weight in score computation.
    #[serde(default = "default_latency_weight")]
    pub latency_weight: f64,
    /// In-flight request weight.
    #[serde(default = "default_inflight_weight")]
    pub inflight_weight: f64,
    /// Error rate weight.
    #[serde(default = "default_error_weight")]
    pub error_weight: f64,
    /// EWMA half-life in milliseconds.
    #[serde(default = "default_ewma_half_life")]
    pub ewma_half_life_ms: u64,
    /// Exploration epsilon (probability of random selection).
    #[serde(default = "default_epsilon")]
    pub exploration_epsilon: f64,
}

fn default_latency_weight() -> f64 {
    1.0
}
fn default_inflight_weight() -> f64 {
    2.0
}
fn default_error_weight() -> f64 {
    10.0
}
fn default_ewma_half_life() -> u64 {
    5000
}
fn default_epsilon() -> f64 {
    0.05
}

impl Default for ReplicaSelectionConfig {
    fn default() -> Self {
        Self {
            strategy: "adaptive".into(),
            latency_weight: default_latency_weight(),
            inflight_weight: default_inflight_weight(),
            error_weight: default_error_weight(),
            ewma_half_life_ms: default_ewma_half_life(),
            exploration_epsilon: default_epsilon(),
        }
    }
}

/// Per-node metrics for adaptive selection.
#[derive(Debug, Clone)]
pub struct NodeMetrics {
    /// EWMA of latency p95 in milliseconds.
    pub latency_p95_ms: f64,
    /// Current in-flight request count.
    pub in_flight: u32,
    /// EWMA of error rate (0.0 to 1.0).
    pub error_rate: f64,
    /// EWMA half-life for updates.
    pub half_life_ms: u64,
    /// Last update timestamp.
    pub last_updated: Instant,
}

impl NodeMetrics {
    /// Create new metrics with initial values.
    pub fn new(initial_latency_ms: f64, half_life_ms: u64) -> Self {
        Self {
            latency_p95_ms: initial_latency_ms,
            in_flight: 0,
            error_rate: 0.0,
            half_life_ms,
            last_updated: Instant::now(),
        }
    }

    /// Update latency with EWMA smoothing.
    pub fn update_latency(&mut self, latency_ms: f64) {
        let alpha = 0.5_f64.powf((self.half_life_ms as f64) / 1000.0);
        self.latency_p95_ms = alpha * self.latency_p95_ms + (1.0 - alpha) * latency_ms;
        self.last_updated = Instant::now();
    }

    /// Update error rate with EWMA smoothing.
    pub fn update_error(&mut self, is_error: bool) {
        let alpha = 0.5_f64.powf((self.half_life_ms as f64) / 1000.0);
        let new_error = if is_error { 1.0 } else { 0.0 };
        self.error_rate = alpha * self.error_rate + (1.0 - alpha) * new_error;
        self.last_updated = Instant::now();
    }

    /// Increment in-flight count.
    pub fn increment_in_flight(&mut self) {
        self.in_flight += 1;
    }

    /// Decrement in-flight count.
    pub fn decrement_in_flight(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    /// Compute the composite score (lower is better).
    pub fn score(&self, config: &ReplicaSelectionConfig) -> f64 {
        config.latency_weight * self.latency_p95_ms
            + config.inflight_weight * (self.in_flight as f64)
            + config.error_weight * (self.error_rate * 1000.0)
    }
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self::new(50.0, 5000)
    }
}

/// Callback for reporting selection events.
pub trait SelectionObserver: Send + Sync {
    /// Called when a node is selected with its score.
    fn report_selection(&self, node_id: &str, score: f64);
    /// Called when exploration selects a random node.
    fn report_exploration(&self);
}

/// No-op observer for when metrics aren't needed.
struct NoOpObserver;
impl SelectionObserver for NoOpObserver {
    fn report_selection(&self, _node_id: &str, _score: f64) {}
    fn report_exploration(&self) {}
}

/// Replica selector.
pub struct ReplicaSelector {
    /// Configuration.
    config: ReplicaSelectionConfig,
    /// Per-node metrics.
    metrics: Arc<RwLock<HashMap<NodeId, NodeMetrics>>>,
    /// Round-robin counter (for round-robin strategy).
    rr_counter: Arc<RwLock<HashMap<String, u64>>>,
    /// Random number generator.
    rng: Arc<std::sync::Mutex<StdRng>>,
    /// Observer for selection events.
    observer: Arc<dyn SelectionObserver>,
}

impl ReplicaSelector {
    /// Create a new replica selector with a metrics observer.
    pub fn new_with_observer(config: ReplicaSelectionConfig, observer: Arc<dyn SelectionObserver>) -> Self {
        Self {
            config,
            metrics: Arc::new(RwLock::new(HashMap::new())),
            rr_counter: Arc::new(RwLock::new(HashMap::new())),
            rng: Arc::new(std::sync::Mutex::new(StdRng::from_entropy())),
            observer,
        }
    }

    /// Create a new replica selector without metrics.
    pub fn new(config: ReplicaSelectionConfig) -> Self {
        Self::new_with_observer(config, Arc::new(NoOpObserver))
    }

    /// Select a node from the given candidates.
    ///
    /// Returns the selected node ID, or None if candidates is empty.
    pub async fn select(&self, candidates: &[NodeId], group_id: u32) -> Option<NodeId> {
        if candidates.is_empty() {
            return None;
        }

        let strategy = self.parse_strategy();

        match strategy {
            SelectionStrategy::Adaptive => self.select_adaptive(candidates).await,
            SelectionStrategy::RoundRobin => self.select_round_robin(candidates, group_id as u64).await,
            SelectionStrategy::Random => self.select_random(candidates),
        }
    }

    /// Adaptive selection using EWMA scores.
    async fn select_adaptive(&self, candidates: &[NodeId]) -> Option<NodeId> {
        let metrics = self.metrics.read().await;

        // Exploration: with probability epsilon, pick randomly
        if self.should_explore() {
            self.observer.report_exploration();
            let selected = self.select_random(candidates);
            if let Some(ref node) = selected {
                let score = metrics
                    .get(node)
                    .map(|m| m.score(&self.config))
                    .unwrap_or(1000.0);
                self.observer.report_selection(node.as_str(), score);
            }
            return selected;
        }

        // Compute scores and collect all nodes with the minimum score
        let mut best_score = f64::INFINITY;
        let mut best_nodes: Vec<NodeId> = Vec::new();

        for node in candidates {
            let score = metrics
                .get(node)
                .map(|m| m.score(&self.config))
                .unwrap_or(1000.0); // High default for unknown nodes

            if score < best_score {
                best_score = score;
                best_nodes.clear();
                best_nodes.push(node.clone());
            } else if (score - best_score).abs() < 1e-10 {
                // Scores are essentially equal - add to tie list
                best_nodes.push(node.clone());
            }
        }

        // If multiple nodes have the same best score, pick randomly
        let selected = if best_nodes.len() == 1 {
            best_nodes.into_iter().next()
        } else {
            let idx = self.rng.lock().unwrap().gen_range(0..best_nodes.len());
            best_nodes.get(idx).cloned()
        };

        if let Some(ref node) = selected {
            self.observer.report_selection(node.as_str(), best_score);
        }

        selected
    }

    /// Round-robin selection.
    async fn select_round_robin(&self, candidates: &[NodeId], group_id: u64) -> Option<NodeId> {
        let key = format!("group_{}", group_id);
        let mut counter = self.rr_counter.write().await;
        let idx = *counter.entry(key.clone()).or_insert(0) as usize % candidates.len();
        *counter.get_mut(&key).unwrap() += 1;
        Some(candidates[idx].clone())
    }

    /// Random selection.
    fn select_random(&self, candidates: &[NodeId]) -> Option<NodeId> {
        if candidates.is_empty() {
            return None;
        }
        let idx = self
            .rng
            .lock()
            .unwrap()
            .gen_range(0..candidates.len());
        Some(candidates[idx].clone())
    }

    /// Check if we should explore (random selection).
    fn should_explore(&self) -> bool {
        let mut rng = self.rng.lock().unwrap();
        rng.gen::<f64>() < self.config.exploration_epsilon
    }

    /// Record a successful request (update latency).
    pub async fn record_success(&self, node: &NodeId, latency_ms: f64) {
        let mut metrics = self.metrics.write().await;
        let entry = metrics
            .entry(node.clone())
            .or_insert_with(NodeMetrics::default);
        entry.update_latency(latency_ms);
        entry.update_error(false);
        entry.decrement_in_flight();
    }

    /// Record a failed request.
    pub async fn record_error(&self, node: &NodeId, latency_ms: Option<f64>) {
        let mut metrics = self.metrics.write().await;
        let entry = metrics
            .entry(node.clone())
            .or_insert_with(NodeMetrics::default);
        if let Some(lat) = latency_ms {
            entry.update_latency(lat);
        }
        entry.update_error(true);
        entry.decrement_in_flight();
    }

    /// Record that a request is being sent to a node.
    pub async fn record_request_start(&self, node: &NodeId) {
        let mut metrics = self.metrics.write().await;
        let entry = metrics
            .entry(node.clone())
            .or_insert_with(NodeMetrics::default);
        entry.increment_in_flight();
    }

    /// Get metrics for a node.
    pub async fn get_metrics(&self, node: &NodeId) -> Option<NodeMetrics> {
        let metrics = self.metrics.read().await;
        metrics.get(node).cloned()
    }

    /// Parse the strategy from config string.
    fn parse_strategy(&self) -> SelectionStrategy {
        match self.config.strategy.as_str() {
            "round_robin" => SelectionStrategy::RoundRobin,
            "random" => SelectionStrategy::Random,
            _ => SelectionStrategy::Adaptive,
        }
    }
}

impl Clone for ReplicaSelector {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            metrics: Arc::clone(&self.metrics),
            rr_counter: Arc::clone(&self.rr_counter),
            rng: Arc::clone(&self.rng),
            observer: Arc::clone(&self.observer),
        }
    }
}

impl Default for ReplicaSelector {
    fn default() -> Self {
        Self::new(ReplicaSelectionConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = ReplicaSelectionConfig::default();
        assert_eq!(config.strategy, "adaptive");
        assert_eq!(config.latency_weight, 1.0);
        assert_eq!(config.inflight_weight, 2.0);
        assert_eq!(config.error_weight, 10.0);
    }

    #[test]
    fn test_node_metrics_score() {
        let mut metrics = NodeMetrics::new(50.0, 5000);
        assert_eq!(metrics.score(&ReplicaSelectionConfig::default()), 50.0);

        metrics.in_flight = 5;
        let score = metrics.score(&ReplicaSelectionConfig::default());
        // 50 * 1.0 + 5 * 2.0 = 60
        assert_eq!(score, 60.0);
    }

    #[test]
    fn test_node_metrics_ewma() {
        let mut metrics = NodeMetrics::new(100.0, 1000); // Short half-life

        metrics.update_latency(50.0);
        // Should move toward 50
        assert!(metrics.latency_p95_ms < 100.0 && metrics.latency_p95_ms > 40.0);

        metrics.update_error(true);
        assert!(metrics.error_rate > 0.0);

        metrics.update_error(false);
        // Error rate should decay
        let rate_before = metrics.error_rate;
        metrics.update_error(false);
        assert!(metrics.error_rate < rate_before);
    }

    #[tokio::test]
    async fn test_select_adaptive() {
        let selector = ReplicaSelector::new(ReplicaSelectionConfig::default());

        let node1 = NodeId::new("node-1".to_string());
        let node2 = NodeId::new("node-2".to_string());

        // Seed metrics by recording successful requests
        selector.record_success(&node1, 10.0).await;
        selector.record_success(&node2, 100.0).await;

        // Should select node-1 (lower score)
        let candidates = vec![node2.clone(), node1.clone()];
        let selected = selector.select(&candidates, 0).await;
        assert_eq!(selected, Some(node1));
    }

    #[tokio::test]
    async fn test_select_round_robin() {
        let config = ReplicaSelectionConfig {
            strategy: "round_robin".into(),
            ..Default::default()
        };
        let selector = ReplicaSelector::new(config);

        let node1 = NodeId::new("node-1".to_string());
        let node2 = NodeId::new("node-2".to_string());

        let candidates = vec![node1.clone(), node2.clone()];

        // First call should return node-1
        let selected = selector.select(&candidates, 0).await;
        assert_eq!(selected, Some(node1.clone()));

        // Second call should return node-2
        let selected = selector.select(&candidates, 0).await;
        assert_eq!(selected, Some(node2.clone()));

        // Third call should wrap to node-1
        let selected = selector.select(&candidates, 0).await;
        assert_eq!(selected, Some(node1));
    }

    #[tokio::test]
    async fn test_record_request_lifecycle() {
        let selector = ReplicaSelector::default();
        let node = NodeId::new("node-1".to_string());

        selector.record_request_start(&node).await;

        let metrics = selector.get_metrics(&node).await;
        assert!(metrics.is_some());
        assert_eq!(metrics.unwrap().in_flight, 1);

        // Record success decrements in-flight and updates latency
        selector.record_success(&node, 50.0).await;

        let metrics = selector.get_metrics(&node).await;
        assert!(metrics.is_some());
        // In-flight should be decremented (from 1 to 0)
        assert_eq!(metrics.unwrap().in_flight, 0);
    }

    #[tokio::test]
    async fn test_empty_candidates() {
        let selector = ReplicaSelector::default();
        let selected = selector.select(&[], 0).await;
        assert!(selected.is_none());
    }
}
