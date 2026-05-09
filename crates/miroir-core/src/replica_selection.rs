//! §13.3 Adaptive replica selection (EWMA).
//!
//! Selects lowest-scoring replica using latency, in-flight count, and error rate.

use crate::topology::NodeId;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Node score state.
#[derive(Debug, Clone)]
pub struct NodeScore {
    /// EWMA of latency in milliseconds.
    pub latency_ms: f64,
    /// Current in-flight request count.
    pub in_flight: u32,
    /// EWMA of error rate (0-1).
    pub error_rate: f64,
    /// Half-life for EWMA decay (seconds).
    pub half_life_ms: u64,
    /// Last update timestamp.
    pub last_updated: Instant,
}

impl NodeScore {
    pub fn new() -> Self {
        Self {
            latency_ms: 50.0,
            in_flight: 0,
            error_rate: 0.0,
            half_life_ms: 5000,
            last_updated: Instant::now(),
        }
    }

    /// Update latency sample.
    pub fn update_latency(&mut self, latency_ms: f64) {
        self.update();
        self.latency_ms = self.ewma(self.latency_ms, latency_ms);
    }

    /// Update error rate.
    pub fn update_error(&mut self, error: bool) {
        self.update();
        let new_rate = if error { 1.0 } else { 0.0 };
        self.error_rate = self.ewma(self.error_rate, new_rate);
    }

    /// Increment/decrement in-flight count.
    pub fn adjust_in_flight(&mut self, delta: i32) {
        self.in_flight = (self.in_flight as i32 + delta).max(0) as u32;
    }

    /// Compute combined score (lower is better).
    pub fn score(&self, weights: &ScoreWeights) -> f64 {
        weights.latency * self.latency_ms
            + weights.inflight * self.in_flight as f64
            + weights.error * self.error_rate * 1000.0
    }

    /// EWMA calculation.
    fn ewma(&self, old: f64, new: f64) -> f64 {
        let elapsed = self.last_updated.elapsed().as_millis() as f64;
        let alpha = 1.0 - 0.5_f64.powf(elapsed / self.half_life_ms as f64);
        (1.0 - alpha) * old + alpha * new
    }

    fn update(&mut self) {
        let _ = self.ewma(0.0, 0.0); // Force update of timestamp
        self.last_updated = Instant::now();
    }
}

/// Scoring weights.
#[derive(Debug, Clone)]
pub struct ScoreWeights {
    pub latency: f64,
    pub inflight: f64,
    pub error: f64,
}

impl Default for ScoreWeights {
    fn default() -> Self {
        Self {
            latency: 1.0,
            inflight: 2.0,
            error: 10.0,
        }
    }
}

/// Adaptive replica selector.
pub struct AdaptiveSelector {
    scores: Arc<RwLock<HashMap<NodeId, NodeScore>>>,
    weights: ScoreWeights,
    exploration_epsilon: f64,
}

impl AdaptiveSelector {
    pub fn new(weights: ScoreWeights, exploration_epsilon: f64) -> Self {
        Self {
            scores: Arc::new(RwLock::new(HashMap::new())),
            weights,
            exploration_epsilon,
        }
    }

    /// Select the best node from candidates.
    pub async fn select(&self, candidates: &[NodeId]) -> Option<NodeId> {
        if candidates.is_empty() {
            return None;
        }

        let scores = self.scores.read().await;
        let mut rng = rand::thread_rng();

        // Exploration: with epsilon probability, pick uniformly at random
        if rng.gen::<f64>() < self.exploration_epsilon {
            return Some(candidates[rng.gen_range(0..candidates.len())].clone());
        }

        // Exploitation: pick lowest-scoring node
        let mut best = None;
        let mut best_score = f64::INFINITY;

        for node_id in candidates {
            let score = scores.get(node_id)
                .map(|s| s.score(&self.weights))
                .unwrap_or(0.0);

            if score < best_score {
                best_score = score;
                best = Some(node_id);
            }
        }

        best.cloned()
    }

    /// Record request start (increment in-flight).
    pub async fn request_start(&self, node_id: &NodeId) {
        let mut scores = self.scores.write().await;
        let entry = scores.entry(node_id.clone()).or_insert_with(NodeScore::new);
        entry.adjust_in_flight(1);
    }

    /// Record request completion (update latency, decrement in-flight).
    pub async fn request_complete(&self, node_id: &NodeId, latency_ms: f64, error: bool) {
        let mut scores = self.scores.write().await;
        let entry = scores.entry(node_id.clone()).or_insert_with(NodeScore::new);
        entry.update_latency(latency_ms);
        entry.update_error(error);
        entry.adjust_in_flight(-1);
    }

    /// Get current score for a node.
    pub async fn score(&self, node_id: &NodeId) -> Option<f64> {
        let scores = self.scores.read().await;
        scores.get(node_id).map(|s| s.score(&self.weights))
    }
}
