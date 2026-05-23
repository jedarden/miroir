//! Anti-entropy worker background task (plan §13.8).
//!
//! Runs periodic anti-entropy passes to detect and repair replica drift:
//! - Acquires leader lease (only one pod runs anti-entropy)
//! - Parses schedule config to determine interval
//! - Runs fingerprint → diff → repair pipeline
//! - Self-throttles to <2% CPU target

use crate::anti_entropy::{AntiEntropyConfig, AntiEntropyReconciler};
use crate::scatter::{
    FetchDocumentsRequest, FetchDocumentsResponse, MockNodeClient, NodeClient, NodeError,
    PreflightRequest, PreflightResponse, SearchRequest,
};
use crate::task_store::TaskStore;
use crate::topology::{NodeId, Topology};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Configuration for the anti-entropy worker.
#[derive(Debug, Clone)]
pub struct AntiEntropyWorkerConfig {
    /// Schedule interval in seconds (parsed from "every 6h" format).
    pub interval_s: u64,
    /// Leader lease TTL in seconds.
    pub lease_ttl_secs: u64,
    /// Lease renewal interval in milliseconds.
    pub lease_renewal_interval_ms: u64,
}

impl Default for AntiEntropyWorkerConfig {
    fn default() -> Self {
        Self {
            interval_s: 6 * 3600, // 6 hours
            lease_ttl_secs: 10,
            lease_renewal_interval_ms: 2000,
        }
    }
}

impl AntiEntropyWorkerConfig {
    /// Parse schedule string to extract interval in seconds.
    ///
    /// Supports formats like "every 6h", "every 30m", "every 1h".
    /// Returns interval in seconds, or 21600 (6h) if parsing fails.
    pub fn from_schedule(schedule: &str) -> Self {
        let interval_s = parse_schedule_interval(schedule).unwrap_or(6 * 3600);
        Self {
            interval_s,
            ..Default::default()
        }
    }
}

/// Parse schedule interval string to seconds.
///
/// Examples:
/// - "every 6h" -> 21600
/// - "every 30m" -> 1800
/// - "every 1h" -> 3600
fn parse_schedule_interval(schedule: &str) -> Option<u64> {
    let schedule = schedule.trim().to_lowercase();

    // Match "every X[unit]" pattern
    if !schedule.starts_with("every ") {
        return None;
    }

    let rest = schedule[6..].trim();
    if rest.is_empty() {
        return None;
    }

    // Find the first non-digit character to split number from unit
    let mut num_end = 0;
    for (i, c) in rest.chars().enumerate() {
        if !c.is_ascii_digit() {
            num_end = i;
            break;
        }
    }

    if num_end == 0 {
        return None;
    }

    let num_str = &rest[..num_end];
    let unit = &rest[num_end..];

    let value: u64 = num_str.parse().ok()?;

    match unit {
        "s" | "sec" | "second" | "seconds" => Some(value),
        "m" | "min" | "minute" | "minutes" => Some(value * 60),
        "h" | "hour" | "hours" => Some(value * 3600),
        _ => None,
    }
}

/// HTTP-based node client for anti-entropy fingerprinting.
///
/// Implements the NodeClient trait for fetching documents from Meilisearch nodes
/// during anti-entropy passes.
#[derive(Clone)]
pub struct HttpNodeClient {
    /// Master key for authenticating with Meilisearch nodes.
    node_master_key: String,
    /// HTTP client for making requests.
    client: Client,
}

impl HttpNodeClient {
    /// Create a new HTTP node client.
    pub fn new(node_master_key: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client for anti-entropy");

        Self {
            node_master_key,
            client,
        }
    }
}

impl NodeClient for HttpNodeClient {
    async fn fetch_documents(
        &self,
        _node: &NodeId,
        address: &str,
        request: &FetchDocumentsRequest,
    ) -> Result<FetchDocumentsResponse, NodeError> {
        let filter_str = serde_json::to_string(&request.filter)
            .unwrap_or_else(|_| "".to_string());

        let url = if address.ends_with('/') {
            format!("{}indexes/{}/documents?filter={}&limit={}&offset={}",
                address,
                request.index_uid,
                urlencoding::encode(&filter_str),
                request.limit,
                request.offset
            )
        } else {
            format!("{}/indexes/{}/documents?filter={}&limit={}&offset={}",
                address,
                request.index_uid,
                urlencoding::encode(&filter_str),
                request.limit,
                request.offset
            )
        };

        let response = self.client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.node_master_key))
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("fetch failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_else(|_| "unable to read error".to_string());
            return Err(NodeError::HttpError { status: status.as_u16(), body });
        }

        let json: Value = response
            .json()
            .await
            .map_err(|e| NodeError::NetworkError(format!("parse failed: {}", e)))?;

        let results = json
            .get("results")
            .and_then(|v| v.as_array())
            .map(|v| v.clone())
            .unwrap_or_default();

        let total = json
            .get("total")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Ok(FetchDocumentsResponse {
            results,
            limit: request.limit,
            offset: request.offset,
            total,
        })
    }

    async fn search_node(
        &self,
        _node: &NodeId,
        address: &str,
        request: &SearchRequest,
    ) -> std::result::Result<Value, NodeError> {
        let url = if address.ends_with('/') {
            format!("{}indexes/{}/search", address, request.index_uid)
        } else {
            format!("{}/indexes/{}/search", address, request.index_uid)
        };

        let response = self.client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.node_master_key))
            .json(&request.body)
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("search failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_else(|_| "unable to read error".to_string());
            return Err(NodeError::HttpError { status: status.as_u16(), body });
        }

        response
            .json()
            .await
            .map_err(|e| NodeError::NetworkError(format!("parse response failed: {}", e)))
    }

    async fn preflight_node(
        &self,
        _node: &NodeId,
        address: &str,
        request: &PreflightRequest,
    ) -> std::result::Result<PreflightResponse, NodeError> {
        let url = if address.ends_with('/') {
            format!("{}indexes/{}/documents?limit={}", address, request.index_uid, 0)
        } else {
            format!("{}/indexes/{}/documents?limit={}", address, request.index_uid, 0)
        };

        let response = self.client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.node_master_key))
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("preflight failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_else(|_| "unable to read error".to_string());
            return Err(NodeError::HttpError { status: status.as_u16(), body });
        }

        let json: Value = response
            .json()
            .await
            .map_err(|e| NodeError::NetworkError(format!("parse response failed: {}", e)))?;

        let total_docs = json
            .get("total")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Ok(PreflightResponse {
            total_docs,
            avg_doc_length: 50.0,
            term_stats: HashMap::new(),
        })
    }
}

/// Anti-entropy background worker.
///
/// Runs periodic anti-entropy passes with leader election to ensure
/// only one pod runs the fingerprinting at a time.
pub struct AntiEntropyWorker {
    config: AntiEntropyWorkerConfig,
    reconciler: AntiEntropyReconciler<HttpNodeClient>,
    topology: Arc<RwLock<Topology>>,
    task_store: Arc<dyn TaskStore>,
    pod_id: String,
}

impl AntiEntropyWorker {
    /// Create a new anti-entropy worker.
    pub fn new(
        config: AntiEntropyWorkerConfig,
        topology: Arc<RwLock<Topology>>,
        task_store: Arc<dyn TaskStore>,
        node_master_key: String,
        pod_id: String,
    ) -> Self {
        let ae_config = AntiEntropyConfig {
            enabled: true,
            schedule: format!("every {}s", config.interval_s),
            index_uid: "default".to_string(),
            shards_per_pass: 0, // Scan all shards
            max_read_concurrency: 2,
            fingerprint_batch_size: 1000,
            auto_repair: true,
            updated_at_field: "_miroir_updated_at".to_string(),
            expires_at_field: "_miroir_expires_at".to_string(),
            ttl_enabled: false,
        };

        let node_client = HttpNodeClient::new(node_master_key);
        let reconciler = AntiEntropyReconciler::new(
            ae_config,
            topology.clone(),
            Arc::new(node_client),
        );

        Self {
            config,
            reconciler,
            topology,
            task_store,
            pod_id,
        }
    }

    /// Start the background worker.
    ///
    /// This runs in a loop:
    /// 1. Try to acquire leader lease (scope: anti_entropy)
    /// 2. If acquired, run anti-entropy pass
    /// 3. Renew lease periodically
    /// 4. If lease lost, go back to step 1
    pub async fn run(&self) {
        info!(
            pod_id = %self.pod_id,
            interval_s = self.config.interval_s,
            "anti-entropy worker starting"
        );

        let scope = "anti_entropy";

        loop {
            let now_ms = now_ms();
            let expires_at = now_ms + (self.config.lease_ttl_secs * 1000) as i64;

            // Try to acquire leader lease
            match tokio::task::spawn_blocking({
                let task_store = self.task_store.clone();
                let scope = scope.to_string();
                let pod_id = self.pod_id.clone();
                move || {
                    task_store.try_acquire_leader_lease(&scope, &pod_id, expires_at, now_ms)
                }
            })
            .await
            {
                Ok(Ok(true)) => {
                    info!(scope = %scope, pod_id = %self.pod_id, "acquired leader lease");

                    // We are the leader - run anti-entropy pass cycle
                    if let Err(e) = self.run_pass_cycle().await {
                        error!(error = %e, "anti-entropy pass cycle failed");
                    }
                }
                Ok(Ok(false)) => {
                    debug!(scope = %scope, "leader lease already held");
                }
                Ok(Err(e)) => {
                    error!(scope = %scope, error = %e, "failed to acquire leader lease");
                }
                Err(e) => {
                    error!(scope = %scope, error = %e, "spawn_blocking task failed");
                }
            }

            // Wait before retrying lease acquisition
            tokio::time::sleep(Duration::from_millis(
                self.config.lease_renewal_interval_ms,
            ))
            .await;
        }
    }

    /// Run a single anti-entropy pass cycle.
    ///
    /// This runs the pass immediately after acquiring lease, then waits
    /// for the configured interval before running again (if still leader).
    async fn run_pass_cycle(&self) -> Result<(), String> {
        let scope = "anti_entropy";
        let mut lease_renewal = tokio::time::interval(Duration::from_millis(
            self.config.lease_renewal_interval_ms,
        ));

        // Run anti-entropy pass immediately on acquiring lease
        self.run_single_pass().await?;

        // Then wait for interval or lease expiry
        let pass_interval = tokio::time::sleep(Duration::from_secs(self.config.interval_s));

        tokio::select! {
            _ = lease_renewal.tick() => {
                // Renew lease
                let now_ms = now_ms();
                let expires_at = now_ms + (self.config.lease_ttl_secs * 1000) as i64;

                match tokio::task::spawn_blocking({
                    let task_store = self.task_store.clone();
                    let scope = scope.to_string();
                    let pod_id = self.pod_id.clone();
                    move || {
                        task_store.renew_leader_lease(&scope, &pod_id, expires_at)
                    }
                })
                .await
                {
                    Ok(Ok(true)) => {
                        debug!(scope = %scope, "renewed leader lease");
                    }
                    Ok(Ok(false)) => {
                        info!(scope = %scope, "lost leader lease");
                        return Ok(());
                    }
                    Ok(Err(e)) => {
                        error!(scope = %scope, error = %e, "failed to renew leader lease");
                        return Err(e.to_string());
                    }
                    Err(e) => {
                        error!(scope = %scope, error = %e, "spawn_blocking task failed");
                        return Err(format!("spawn_blocking task failed: {}", e));
                    }
                }
            }
            _ = pass_interval => {
                // Interval passed - run anti-entropy pass
                self.run_single_pass().await?;
            }
        }

        Ok(())
    }

    /// Run a single anti-entropy pass.
    async fn run_single_pass(&self) -> Result<(), String> {
        info!("starting anti-entropy pass");

        match self.reconciler.run_pass().await {
            Ok(pass) => {
                info!(
                    shards_scanned = pass.shards_scanned,
                    shards_with_drift = pass.shards_with_drift,
                    repairs_performed = pass.repairs_performed,
                    errors = pass.errors.len(),
                    "anti-entropy pass completed"
                );

                if !pass.errors.is_empty() {
                    warn!(errors = ?pass.errors, "anti-entropy pass had errors");
                }

                Ok(())
            }
            Err(e) => {
                Err(format!("anti-entropy pass failed: {}", e))
            }
        }
    }
}

/// Get current time in milliseconds since Unix epoch.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_schedule_interval_hours() {
        assert_eq!(parse_schedule_interval("every 6h"), Some(21600));
        assert_eq!(parse_schedule_interval("every 1h"), Some(3600));
        assert_eq!(parse_schedule_interval("every 12h"), Some(43200));
    }

    #[test]
    fn test_parse_schedule_interval_minutes() {
        assert_eq!(parse_schedule_interval("every 30m"), Some(1800));
        assert_eq!(parse_schedule_interval("every 5m"), Some(300));
        assert_eq!(parse_schedule_interval("every 60m"), Some(3600));
    }

    #[test]
    fn test_parse_schedule_interval_seconds() {
        assert_eq!(parse_schedule_interval("every 60s"), Some(60));
        assert_eq!(parse_schedule_interval("every 300s"), Some(300));
    }

    #[test]
    fn test_parse_schedule_invalid() {
        assert_eq!(parse_schedule_interval("invalid"), None);
        assert_eq!(parse_schedule_interval("every"), None);
        assert_eq!(parse_schedule_interval("6h"), None);
    }

    #[test]
    fn test_parse_schedule_case_insensitive() {
        assert_eq!(parse_schedule_interval("EVERY 6H"), Some(21600));
        assert_eq!(parse_schedule_interval("Every 30M"), Some(1800));
    }

    #[test]
    fn test_worker_config_from_schedule() {
        let config = AntiEntropyWorkerConfig::from_schedule("every 6h");
        assert_eq!(config.interval_s, 21600);

        let config = AntiEntropyWorkerConfig::from_schedule("every 30m");
        assert_eq!(config.interval_s, 1800);
    }

    #[test]
    fn test_worker_config_default() {
        let config = AntiEntropyWorkerConfig::default();
        assert_eq!(config.interval_s, 6 * 3600);
        assert_eq!(config.lease_ttl_secs, 10);
        assert_eq!(config.lease_renewal_interval_ms, 2000);
    }
}
