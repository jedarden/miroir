//! Document TTL and automatic expiration (plan §13.14).
//!
//! Background sweeper deletes documents whose `_miroir_expires_at` field
//! is in the past.
//!
//! # CDC Origin Tag (plan §13.13)
//!
//! TTL expiration deletes must be tagged with `origin="ttl_expire"` so they are
//! suppressed from CDC by default (unless `emit_ttl_deletes` is true).
//!
//! When constructing delete requests for expired documents, set:
//! ```ignore
//! use miroir_core::cdc::ORIGIN_TTL_EXPIRE;
//! WriteRequest { ..., origin: Some(ORIGIN_TTL_EXPIRE.to_string()) }
//! ```

use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};

/// TTL configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlConfig {
    /// Whether TTL is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Sweep interval in seconds.
    #[serde(default = "default_interval")]
    pub sweep_interval_s: u64,
    /// Maximum deletes per sweep.
    #[serde(default = "default_max_deletes")]
    pub max_deletes_per_sweep: u32,
    /// Expires_at field name.
    #[serde(default = "default_field")]
    pub expires_at_field: String,
    /// Per-index overrides.
    #[serde(default)]
    pub per_index_overrides: HashMap<String, TtlOverride>,
}

/// Per-index TTL override.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlOverride {
    /// Sweep interval override.
    pub sweep_interval_s: u64,
    /// Max deletes override.
    pub max_deletes_per_sweep: u32,
}

fn default_true() -> bool {
    true
}
fn default_interval() -> u64 {
    300 // 5 minutes
}
fn default_max_deletes() -> u32 {
    10000
}
fn default_field() -> String {
    "_miroir_expires_at".into()
}

impl Default for TtlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sweep_interval_s: default_interval(),
            max_deletes_per_sweep: default_max_deletes(),
            expires_at_field: default_field(),
            per_index_overrides: HashMap::new(),
        }
    }
}

/// TTL sweeper state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlSweeperState {
    /// Last sweep timestamp.
    pub last_sweep_at: u64,
    /// Documents deleted in last sweep.
    pub last_sweep_deleted: u64,
    /// Indexes with pending expired documents.
    pub pending_indexes: Vec<String>,
}

/// TTL manager.
pub struct TtlManager {
    /// Configuration.
    config: TtlConfig,
    /// Sweeper state.
    state: Arc<RwLock<TtlSweeperState>>,
    /// Sweeper running flag.
    running: Arc<RwLock<bool>>,
}

impl TtlManager {
    /// Create a new TTL manager.
    pub fn new(config: TtlConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(TtlSweeperState {
                last_sweep_at: 0,
                last_sweep_deleted: 0,
                pending_indexes: Vec::new(),
            })),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the background sweeper.
    pub async fn start(&self) {
        let mut running = self.running.write().await;
        if *running {
            return; // Already running
        }
        *running = true;
        drop(running);

        let config = self.config.clone();
        let state = self.state.clone();
        let running_flag = self.running.clone();

        tokio::spawn(async move {
            let mut timer = interval(Duration::from_secs(config.sweep_interval_s));
            loop {
                timer.tick().await;

                // Check if still running
                {
                    let running = running_flag.read().await;
                    if !*running {
                        break;
                    }
                }

                // Run sweep
                if let Err(e) = Self::run_sweep(&config, &state).await {
                    tracing::error!("TTL sweep failed: {}", e);
                }
            }
        });
    }

    /// Stop the background sweeper.
    pub async fn stop(&self) {
        let mut running = self.running.write().await;
        *running = false;
    }

    /// Run a single sweep pass.
    async fn run_sweep(config: &TtlConfig, state: &Arc<RwLock<TtlSweeperState>>) -> Result<()> {
        let now_ms = millis_now();

        // In a real implementation, this would:
        // 1. Query each index for documents with expires_at <= now
        // 2. Delete them in batches
        // 3. Update the state

        tracing::debug!("TTL sweep running at {}", now_ms);

        let mut state = state.write().await;
        state.last_sweep_at = now_ms;
        state.last_sweep_deleted = 0; // Would be updated with actual count

        Ok(())
    }

    /// Get the current sweeper state.
    pub async fn state(&self) -> TtlSweeperState {
        self.state.read().await.clone()
    }

    /// Estimate pending expired documents for an index.
    ///
    /// In a real implementation, this would query the index with
    /// a filter to count documents with expires_at <= now.
    pub async fn estimate_pending(&self, _index: &str) -> Result<u64> {
        // Placeholder
        Ok(0)
    }
}

impl Default for TtlManager {
    fn default() -> Self {
        Self::new(TtlConfig::default())
    }
}

/// Get current UNIX timestamp in milliseconds.
fn millis_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = TtlConfig::default();
        assert!(config.enabled);
        assert_eq!(config.sweep_interval_s, 300);
        assert_eq!(config.max_deletes_per_sweep, 10000);
        assert_eq!(config.expires_at_field, "_miroir_expires_at");
    }

    #[tokio::test]
    async fn test_manager_state() {
        let manager = TtlManager::default();
        let state = manager.state().await;
        assert_eq!(state.last_sweep_at, 0);
        assert_eq!(state.last_sweep_deleted, 0);
    }

    #[tokio::test]
    async fn test_estimate_pending() {
        let manager = TtlManager::default();
        let pending = manager.estimate_pending("products").await.unwrap();
        assert_eq!(pending, 0);
    }
}
