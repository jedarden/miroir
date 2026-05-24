//! Traffic shadow / teeing — plan §13.16.
//!
//! Shadows a fraction of incoming requests to a shadow cluster for comparison.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;

/// Shadow target configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowTarget {
    /// Target name.
    pub name: String,
    /// Shadow cluster URL.
    pub url: String,
    /// API key environment variable.
    pub api_key_env: String,
    /// Sample rate (0.0 to 1.0).
    pub sample_rate: f64,
    /// Operations to shadow.
    pub operations: Vec<ShadowOperation>,
}

/// Operations that can be shadowed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowOperation {
    Search,
    MultiSearch,
    Explain,
}

/// Shadow diff result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowDiff {
    /// Target name.
    pub target: String,
    /// Query fingerprint.
    pub query_fingerprint: String,
    /// Timestamp (UNIX ms).
    pub timestamp_ms: u64,
    /// Primary result hit count.
    pub primary_hit_count: usize,
    /// Shadow result hit count.
    pub shadow_hit_count: usize,
    /// Hits only in primary.
    pub primary_only_hits: Vec<String>,
    /// Hits only in shadow.
    pub shadow_only_hits: Vec<String>,
    /// Kendall tau correlation (ranking similarity).
    pub kendall_tau: Option<f64>,
    /// Primary latency (ms).
    pub primary_latency_ms: u64,
    /// Shadow latency (ms).
    pub shadow_latency_ms: u64,
    /// Whether primary succeeded.
    pub primary_success: bool,
    /// Whether shadow succeeded.
    pub shadow_success: bool,
}

/// Shadow manager configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowConfig {
    /// Whether shadowing is enabled.
    pub enabled: bool,
    /// Configured targets.
    pub targets: Vec<ShadowTarget>,
    /// Diff buffer size.
    pub diff_buffer_size: usize,
    /// Maximum shadow latency (ms).
    pub max_shadow_latency_ms: u64,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            targets: Vec::new(),
            diff_buffer_size: 10000,
            max_shadow_latency_ms: 5000,
        }
    }
}

/// Shadow manager state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowState {
    /// Recent diff results (circular buffer).
    pub recent_diffs: VecDeque<ShadowDiff>,
    /// Total shadowed requests.
    pub total_shadowed: u64,
    /// Total shadow errors.
    pub total_errors: u64,
}

/// Shadow manager — handles request shadowing to staging clusters.
pub struct ShadowManager {
    /// Configuration.
    config: ShadowConfig,
    /// Shared state.
    state: Arc<RwLock<ShadowState>>,
    /// HTTP client for shadow requests.
    client: reqwest::Client,
}

impl ShadowManager {
    /// Create a new shadow manager.
    pub fn new(config: ShadowConfig) -> Self {
        let state = Arc::new(RwLock::new(ShadowState {
            recent_diffs: VecDeque::with_capacity(config.diff_buffer_size),
            total_shadowed: 0,
            total_errors: 0,
        }));

        Self {
            config,
            state,
            client: reqwest::Client::new(),
        }
    }

    /// Determine if a request should be shadowed to a target.
    pub fn should_shadow(&self, target: &ShadowTarget) -> bool {
        if !self.config.enabled {
            return false;
        }
        // Use RNG to determine if this request should be shadowed
        let random: f64 = rand::random();
        random < target.sample_rate
    }

    /// Shadow a search request to the target.
    pub async fn shadow_search(
        &self,
        target: &ShadowTarget,
        index_uid: &str,
        request_body: &serde_json::Value,
        primary_latency_ms: u64,
        primary_hit_count: usize,
    ) -> Result<ShadowDiff, ShadowError> {
        let start = std::time::Instant::now();

        // Build shadow request URL
        let url = format!(
            "{}/indexes/{}/search",
            target.url.trim_end_matches('/'),
            index_uid
        );

        // Send shadow request with timeout
        let result = tokio::time::timeout(
            tokio::time::Duration::from_millis(self.config.max_shadow_latency_ms),
            self.client.post(&url).json(request_body).send(),
        )
        .await;

        let shadow_latency_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(Ok(response)) => {
                let shadow_success = response.status().is_success();
                let shadow_hit_count = if shadow_success {
                    response
                        .json::<serde_json::Value>()
                        .await
                        .and_then(|v| {
                            Ok(v.get("hits")
                                .and_then(|h| h.as_array())
                                .map(|a| a.len())
                                .unwrap_or(0))
                        })
                        .unwrap_or(0)
                } else {
                    0
                };

                let diff = ShadowDiff {
                    target: target.name.clone(),
                    query_fingerprint: Self::fingerprint_request(request_body),
                    timestamp_ms: millis_now(),
                    primary_hit_count,
                    shadow_hit_count,
                    primary_only_hits: Vec::new(), // TODO: compute symmetric diff
                    shadow_only_hits: Vec::new(),
                    kendall_tau: None, // TODO: compute ranking correlation
                    primary_latency_ms,
                    shadow_latency_ms,
                    primary_success: true,
                    shadow_success,
                };

                // Add to state
                let mut state = self.state.write().await;
                state.total_shadowed += 1;
                state.recent_diffs.push_back(diff.clone());
                if state.recent_diffs.len() > self.config.diff_buffer_size {
                    state.recent_diffs.pop_front();
                }

                Ok(diff)
            }
            Ok(Err(e)) => {
                let mut state = self.state.write().await;
                state.total_shadowed += 1;
                state.total_errors += 1;

                Err(ShadowError::RequestError(e.to_string()))
            }
            Err(_) => {
                // Timeout
                let mut state = self.state.write().await;
                state.total_shadowed += 1;
                state.total_errors += 1;

                Err(ShadowError::Timeout)
            }
        }
    }

    /// Get recent shadow diffs.
    pub async fn recent_diffs(&self, limit: usize) -> Vec<ShadowDiff> {
        let state = self.state.read().await;
        state
            .recent_diffs
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get shadow statistics.
    pub async fn stats(&self) -> ShadowStats {
        let state = self.state.read().await;
        ShadowStats {
            total_shadowed: state.total_shadowed,
            total_errors: state.total_errors,
            error_rate: if state.total_shadowed > 0 {
                state.total_errors as f64 / state.total_shadowed as f64
            } else {
                0.0
            },
            recent_diffs_count: state.recent_diffs.len(),
        }
    }

    /// Generate a fingerprint for a request body (for deduplication).
    fn fingerprint_request(body: &serde_json::Value) -> String {
        use sha2::{Digest, Sha256};
        let json = serde_json::to_string(body).unwrap_or_default();
        let hash = Sha256::digest(json.as_bytes());
        format!("{:x}", hash)
    }
}

/// Shadow statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowStats {
    pub total_shadowed: u64,
    pub total_errors: u64,
    pub error_rate: f64,
    pub recent_diffs_count: usize,
}

/// Shadow error types.
#[derive(Debug, thiserror::Error)]
pub enum ShadowError {
    #[error("request error: {0}")]
    RequestError(String),
    #[error("timeout")]
    Timeout,
    #[error("target not found: {0}")]
    TargetNotFound(String),
}

/// Get current UNIX timestamp in milliseconds.
fn millis_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shadow_config_default() {
        let config = ShadowConfig::default();
        assert!(config.enabled);
        assert_eq!(config.diff_buffer_size, 10000);
        assert_eq!(config.max_shadow_latency_ms, 5000);
    }

    #[test]
    fn test_shadow_operation_serialization() {
        let op = ShadowOperation::Search;
        let json = serde_json::to_string(&op).unwrap();
        assert_eq!(json, "\"search\"");
    }

    #[test]
    fn test_fingerprint_request() {
        let body = serde_json::json!({"q": "test", "limit": 10});
        let fp1 = ShadowManager::fingerprint_request(&body);
        let fp2 = ShadowManager::fingerprint_request(&body);
        assert_eq!(fp1, fp2);

        let body2 = serde_json::json!({"q": "other", "limit": 10});
        let fp3 = ShadowManager::fingerprint_request(&body2);
        assert_ne!(fp1, fp3);
    }

    #[tokio::test]
    async fn test_shadow_manager_creation() {
        let config = ShadowConfig::default();
        let manager = ShadowManager::new(config);
        let stats = manager.stats().await;
        assert_eq!(stats.total_shadowed, 0);
        assert_eq!(stats.total_errors, 0);
    }

    #[test]
    fn test_should_shadow() {
        let config = ShadowConfig::default();
        let manager = ShadowManager::new(config);

        let target = ShadowTarget {
            name: "staging".into(),
            url: "http://staging:7700".into(),
            api_key_env: "SHADOW_KEY".into(),
            sample_rate: 0.5,
            operations: vec![ShadowOperation::Search],
        };

        // With sample_rate = 0.5, we should get varying results
        // Just test that it returns a boolean
        let _ = manager.should_shadow(&target);
    }
}
