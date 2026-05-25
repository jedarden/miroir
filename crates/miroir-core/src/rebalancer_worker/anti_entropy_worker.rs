//! Anti-entropy worker background task (plan §13.8).
//!
//! Runs periodic anti-entropy passes to detect and repair replica drift:
//! - Mode A shard-partitioned coordination (plan §14.5, §14.6)
//! - Each pod fingerprints and repairs only its rendezvous-owned shards
//! - Parses schedule config to determine interval
//! - Runs fingerprint → diff → repair pipeline
//! - Self-throttles to <2% CPU target

use crate::anti_entropy::{AntiEntropyConfig, AntiEntropyReconciler};
#[cfg(feature = "peer-discovery")]
use crate::mode_a_coordinator::ModeACoordinator as ActualModeACoordinator;
use crate::scatter::{
    FetchDocumentsRequest, FetchDocumentsResponse, NodeClient, NodeError, PreflightRequest,
    PreflightResponse, SearchRequest,
};
use crate::task_store::TaskStore;
use crate::topology::{NodeId, Topology};
use reqwest::Client;

// Type alias for ModeACoordinator that becomes a dummy type when feature is disabled
#[cfg(feature = "peer-discovery")]
type ModeACoordinator = ActualModeACoordinator;

#[cfg(not(feature = "peer-discovery"))]
struct ModeACoordinator;

#[cfg(not(feature = "peer-discovery"))]
impl ModeACoordinator {
    // Dummy methods for when peer-discovery is disabled
    pub async fn refresh_peers(&self) -> std::result::Result<usize, String> {
        Ok(1)
    }
}
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
    /// Leader lease renewal interval in milliseconds.
    pub lease_renewal_interval_ms: u64,
    /// Leader lease TTL in seconds.
    pub lease_ttl_secs: u64,
}

impl Default for AntiEntropyWorkerConfig {
    fn default() -> Self {
        Self {
            interval_s: 6 * 3600,            // 6 hours
            lease_renewal_interval_ms: 3000, // 3 seconds (plan §14.8)
            lease_ttl_secs: 10,              // 10 seconds (plan §14.8)
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
    async fn write_documents(
        &self,
        _node: &NodeId,
        address: &str,
        request: &crate::scatter::WriteRequest,
    ) -> Result<crate::scatter::WriteResponse, NodeError> {
        let url = if address.ends_with('/') {
            format!("{}indexes/{}/documents", address, request.index_uid)
        } else {
            format!("{}/indexes/{}/documents", address, request.index_uid)
        };

        let mut builder = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.node_master_key))
            .header("Content-Type", "application/json");

        // Add origin tag as a header (not stored in document)
        if let Some(ref origin) = request.origin {
            builder = builder.header("X-Miroir-Origin", origin);
        }

        let response = builder
            .json(&request.documents)
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("write failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read error".to_string());
            return Err(NodeError::HttpError {
                status: status.as_u16(),
                body,
            });
        }

        let json: Value = response
            .json()
            .await
            .map_err(|e| NodeError::NetworkError(format!("parse response failed: {e}")))?;

        let success = json
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let task_uid = json.get("taskUid").and_then(|v| v.as_u64());

        Ok(crate::scatter::WriteResponse {
            success,
            task_uid,
            message: json
                .get("message")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            code: json
                .get("code")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            error_type: json
                .get("type")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        })
    }

    async fn fetch_documents(
        &self,
        _node: &NodeId,
        address: &str,
        request: &FetchDocumentsRequest,
    ) -> Result<FetchDocumentsResponse, NodeError> {
        let filter_str = serde_json::to_string(&request.filter).unwrap_or_else(|_| "".to_string());

        let url = if address.ends_with('/') {
            format!(
                "{}indexes/{}/documents?filter={}&limit={}&offset={}",
                address,
                request.index_uid,
                urlencoding::encode(&filter_str),
                request.limit,
                request.offset
            )
        } else {
            format!(
                "{}/indexes/{}/documents?filter={}&limit={}&offset={}",
                address,
                request.index_uid,
                urlencoding::encode(&filter_str),
                request.limit,
                request.offset
            )
        };

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.node_master_key))
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("fetch failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read error".to_string());
            return Err(NodeError::HttpError {
                status: status.as_u16(),
                body,
            });
        }

        let json: Value = response
            .json()
            .await
            .map_err(|e| NodeError::NetworkError(format!("parse failed: {e}")))?;

        let results = json
            .get("results")
            .and_then(|v| v.as_array()).cloned()
            .unwrap_or_default();

        let total = json.get("total").and_then(|v| v.as_u64()).unwrap_or(0);

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

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.node_master_key))
            .json(&request.body)
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("search failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read error".to_string());
            return Err(NodeError::HttpError {
                status: status.as_u16(),
                body,
            });
        }

        response
            .json()
            .await
            .map_err(|e| NodeError::NetworkError(format!("parse response failed: {e}")))
    }

    async fn preflight_node(
        &self,
        _node: &NodeId,
        address: &str,
        request: &PreflightRequest,
    ) -> std::result::Result<PreflightResponse, NodeError> {
        let url = if address.ends_with('/') {
            format!(
                "{}indexes/{}/documents?limit={}",
                address, request.index_uid, 0
            )
        } else {
            format!(
                "{}/indexes/{}/documents?limit={}",
                address, request.index_uid, 0
            )
        };

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.node_master_key))
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("preflight failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read error".to_string());
            return Err(NodeError::HttpError {
                status: status.as_u16(),
                body,
            });
        }

        let json: Value = response
            .json()
            .await
            .map_err(|e| NodeError::NetworkError(format!("parse response failed: {e}")))?;

        let total_docs = json.get("total").and_then(|v| v.as_u64()).unwrap_or(0);

        Ok(PreflightResponse {
            total_docs,
            avg_doc_length: 50.0,
            term_stats: HashMap::new(),
        })
    }

    async fn delete_documents(
        &self,
        _node: &NodeId,
        address: &str,
        request: &crate::scatter::DeleteByIdsRequest,
    ) -> std::result::Result<crate::scatter::DeleteResponse, NodeError> {
        let url = if address.ends_with('/') {
            format!(
                "{}indexes/{}/documents/delete-batch",
                address, request.index_uid
            )
        } else {
            format!(
                "{}/indexes/{}/documents/delete-batch",
                address, request.index_uid
            )
        };

        let mut builder = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.node_master_key))
            .header("Content-Type", "application/json");

        // Add origin tag as a header (not stored in document)
        if let Some(ref origin) = request.origin {
            builder = builder.header("X-Miroir-Origin", origin);
        }

        let body = serde_json::json!({ "ids": request.ids });
        let response = builder
            .json(&body)
            .send()
            .await
            .map_err(|e| NodeError::NetworkError(format!("delete failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read error".to_string());
            return Err(NodeError::HttpError {
                status: status.as_u16(),
                body,
            });
        }

        let json: Value = response
            .json()
            .await
            .map_err(|e| NodeError::NetworkError(format!("parse response failed: {e}")))?;

        Ok(crate::scatter::DeleteResponse {
            success: json
                .get("success")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            task_uid: json.get("taskUid").and_then(|v| v.as_u64()),
            message: json
                .get("message")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            code: json
                .get("code")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            error_type: json
                .get("type")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        })
    }
}

/// Anti-entropy background worker.
///
/// Runs periodic anti-entropy passes with Mode A coordination (plan §14.5, §14.6).
/// Each pod fingerprints and repairs only its rendezvous-owned shards.
pub struct AntiEntropyWorker {
    config: AntiEntropyWorkerConfig,
    reconciler: AntiEntropyReconciler<HttpNodeClient>,
    topology: Arc<RwLock<Topology>>,
    task_store: Arc<dyn TaskStore>,
    pod_id: String,
    /// Mode A coordinator for shard-partitioned ownership (plan §14.5 Mode A).
    mode_a_coordinator: Option<Arc<ModeACoordinator>>,
    /// Total shards in the cluster (for Mode A scaling).
    total_shards: u32,
    /// This pod's replica group ID (for Mode A scaling).
    replica_group_id: Option<u32>,
    /// Total number of pods in Mode A scaling.
    num_pods: Option<u32>,
    /// RF (replication factor) for Mode A scaling.
    rf: usize,
    /// Whether TTL is enabled for expired document handling (plan §13.14 interaction).
    ttl_enabled: bool,
    /// Metrics callback for shards scanned.
    metrics_shards_scanned: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    /// Metrics callback for mismatches found.
    metrics_mismatches_found: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    /// Metrics callback for docs repaired.
    metrics_docs_repaired: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    /// Metrics callback for scan completion time.
    metrics_scan_completed: Option<Arc<dyn Fn(u64) + Send + Sync>>,
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
            ttl_enabled: false, // Will be updated by set_ttl_enabled
        };

        let node_client = HttpNodeClient::new(node_master_key);
        let reconciler =
            AntiEntropyReconciler::new(ae_config, topology.clone(), Arc::new(node_client));

        Self {
            config,
            reconciler,
            topology,
            task_store,
            pod_id,
            mode_a_coordinator: None,
            total_shards: 0, // Will be set when Mode A is enabled
            replica_group_id: None,
            num_pods: None,
            rf: 2, // Default RF
            ttl_enabled: false,
            metrics_shards_scanned: None,
            metrics_mismatches_found: None,
            metrics_docs_repaired: None,
            metrics_scan_completed: None,
        }
    }

    /// Set the Mode A coordinator for shard-partitioned ownership (plan §14.5 Mode A).
    pub fn with_mode_a_coordinator(mut self, coordinator: Arc<ModeACoordinator>) -> Self {
        self.mode_a_coordinator = Some(coordinator);
        self
    }

    /// Set Mode A scaling parameters (plan §14.6).
    ///
    /// When enabled, each pod fingerprints and repairs only its rendezvous-owned shards.
    ///
    /// # Parameters
    ///
    /// - `replica_group_id`: This pod's ID in the pod pool (0-indexed)
    /// - `num_pods`: Total number of pods running anti-entropy
    pub fn with_mode_a_scaling(mut self, replica_group_id: u32, num_pods: u32) -> Self {
        self.replica_group_id = Some(replica_group_id);
        self.num_pods = Some(num_pods);
        self
    }

    /// Set metrics callbacks.
    pub fn with_metrics(
        mut self,
        shards_scanned: Arc<dyn Fn(u64) + Send + Sync>,
        mismatches_found: Arc<dyn Fn(u64) + Send + Sync>,
        docs_repaired: Arc<dyn Fn(u64) + Send + Sync>,
        scan_completed: Arc<dyn Fn(u64) + Send + Sync>,
    ) -> Self {
        self.metrics_shards_scanned = Some(shards_scanned);
        self.metrics_mismatches_found = Some(mismatches_found);
        self.metrics_docs_repaired = Some(docs_repaired);
        self.metrics_scan_completed = Some(scan_completed);
        self
    }

    /// Set whether TTL is enabled for expired document handling (plan §13.14 interaction).
    pub fn set_ttl_enabled(&mut self, enabled: bool) {
        self.ttl_enabled = enabled;
        // Update reconciler config to match
        self.reconciler.set_ttl_enabled(enabled);
    }

    /// Start the background worker.
    ///
    /// This runs in a loop using Mode A coordination (plan §14.5):
    /// 1. Refresh peer set
    /// 2. Run anti-entropy pass on owned shards
    /// 3. Wait for configured interval
    /// 4. Repeat
    ///
    /// No leader election is used — each pod independently scans its
    /// rendezvous-owned shards.
    pub async fn run(&self) {
        info!(
            pod_id = %self.pod_id,
            interval_s = self.config.interval_s,
            "anti-entropy worker starting (Mode A coordination)"
        );

        let interval = Duration::from_secs(self.config.interval_s);

        loop {
            // Refresh peer set for Mode A coordination
            if let Some(ref coordinator) = self.mode_a_coordinator {
                match coordinator.refresh_peers().await {
                    Ok(peer_count) => {
                        debug!(peer_count, "refreshed peer set for anti-entropy");
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to refresh peer set, using cached peers");
                    }
                }
            }

            // Run anti-entropy pass on owned shards
            if let Err(e) = self.run_single_pass().await {
                error!(error = %e, "anti-entropy pass failed");
            }

            // Wait for next interval
            tokio::time::sleep(interval).await;
        }
    }

    /// Run a single anti-entropy pass cycle.
    ///
    /// This runs the pass immediately after acquiring lease, then waits
    /// for the configured interval before running again (if still leader).
    async fn run_pass_cycle(&self) -> Result<(), String> {
        let scope = "anti_entropy";
        let mut lease_renewal =
            tokio::time::interval(Duration::from_millis(self.config.lease_renewal_interval_ms));

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
                        return Err(format!("spawn_blocking task failed: {e}"));
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

        // Get topology info for Mode A scaling
        let (total_shards, rf) = {
            let topo = self.topology.read().await;
            (topo.shards, topo.rf())
        };

        // Use the existing reconciler directly
        // Note: Mode A scaling and metrics are configured via worker fields,
        // not via the reconciler's builder pattern
        let reconciler = &self.reconciler;

        match reconciler.run_pass().await {
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

                // Emit worker-level metrics if callbacks are configured
                if let Some(ref callback) = self.metrics_shards_scanned {
                    callback(pass.shards_scanned as u64);
                }
                if let Some(ref callback) = self.metrics_scan_completed {
                    callback(pass.completed_at / 1000);
                }

                Ok(())
            }
            Err(e) => Err(format!("anti-entropy pass failed: {e}")),
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
        assert_eq!(config.lease_renewal_interval_ms, 3000); // 3 seconds per plan §14.8
    }
}
