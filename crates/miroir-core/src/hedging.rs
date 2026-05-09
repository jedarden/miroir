//! §13.2 Hedged requests for tail-latency mitigation.
//!
//! Starts duplicate requests to alternate replicas when primary is slow.

use crate::topology::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// EWMA-tracked latency statistics per node.
#[derive(Debug, Clone)]
pub struct NodeLatency {
    /// EWMA of p95 latency in milliseconds.
    pub p95_ms: f64,
    /// Half-life for EWMA decay (in seconds).
    pub half_life_s: f64,
    /// Last update timestamp.
    pub last_updated: Instant,
}

impl NodeLatency {
    pub fn new() -> Self {
        Self {
            p95_ms: 100.0, // Initial assumption
            half_life_s: 60.0,
            last_updated: Instant::now(),
        }
    }

    /// Update with a new latency sample.
    pub fn update(&mut self, latency_ms: f64) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_updated).as_secs_f64();
        self.last_updated = now;

        // EWMA decay factor
        let alpha = 1.0 - 0.5_f64.powf(elapsed / self.half_life_s);
        self.p95_ms = (1.0 - alpha) * self.p95_ms + alpha * latency_ms;
    }

    /// Get the hedge trigger deadline for this node.
    pub fn hedge_deadline(&self, multiplier: f64, min_ms: u64) -> Duration {
        let ms = (self.p95_ms * multiplier).max(min_ms as f64);
        Duration::from_millis(ms as u64)
    }
}

/// Hedging state manager.
#[derive(Debug)]
pub struct HedgingManager {
    /// Per-node latency tracking.
    latencies: Arc<RwLock<HashMap<NodeId, NodeLatency>>>,
    /// Hedge configuration.
    config: HedgingConfig,
}

#[derive(Debug, Clone)]
pub struct HedgingConfig {
    /// Hedge at this multiplier of observed p95.
    pub p95_trigger_multiplier: f64,
    /// Never hedge sooner than this (ms).
    pub min_trigger_ms: u64,
    /// Maximum hedges per query.
    pub max_hedges_per_query: u32,
    /// Allow cross-group fallback.
    pub cross_group_fallback: bool,
}

impl Default for HedgingConfig {
    fn default() -> Self {
        Self {
            p95_trigger_multiplier: 1.2,
            min_trigger_ms: 15,
            max_hedges_per_query: 2,
            cross_group_fallback: true,
        }
    }
}

impl HedgingManager {
    pub fn new(config: HedgingConfig) -> Self {
        Self {
            latencies: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Record a latency sample for a node.
    pub async fn record_latency(&self, node_id: &NodeId, latency_ms: f64) {
        let mut latencies = self.latencies.write().await;
        let entry = latencies.entry(node_id.clone()).or_insert_with(NodeLatency::new);
        entry.update(latency_ms);
    }

    /// Get the hedge deadline for a given node.
    pub async fn hedge_deadline(&self, node_id: &NodeId) -> Duration {
        let latencies = self.latencies.read().await;
        let entry = latencies.get(node_id);
        match entry {
            Some(latency) => latency.hedge_deadline(
                self.config.p95_trigger_multiplier,
                self.config.min_trigger_ms,
            ),
            None => Duration::from_millis(self.config.min_trigger_ms),
        }
    }

    /// Get current p95 latency for a node.
    pub async fn p95_latency_ms(&self, node_id: &NodeId) -> f64 {
        let latencies = self.latencies.read().await;
        latencies.get(node_id)
            .map(|l| l.p95_ms)
            .unwrap_or(100.0)
    }

    /// Get configuration.
    pub fn config(&self) -> &HedgingConfig {
        &self.config
    }
}
