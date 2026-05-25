//! §13.18 Synthetic canary queries with golden assertions
//!
//! Uses Mode A coordination (plan §14.5) to partition canary execution across pods.
//! Each canary ID is rendezvous-owned by exactly one pod per interval, ensuring
//! no duplicate canary runs across the cluster.

#[cfg(feature = "peer-discovery")]
use crate::mode_a_coordinator::ModeACoordinator;
use crate::{
    error::{MiroirError, Result},
    task_store::{CanaryRow, NewCanary, NewCanaryRun, TaskStore},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Canary assertion types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryAssertion {
    TopHitId { value: String },
    TopKContains { k: usize, ids: Vec<String> },
    MinHits { value: usize },
    MaxP95Ms { value: u64 },
    SettingsVersionAtLeast { value: i64 },
    MustNotContainId { id: String },
}

/// Canary definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Canary {
    pub id: String,
    pub name: String,
    pub index_uid: String,
    pub interval_s: i64,
    pub query: SearchQuery,
    pub assertions: Vec<CanaryAssertion>,
    pub enabled: bool,
    pub created_at: i64,
}

/// Search query for canary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    #[serde(flatten)]
    pub params: HashMap<String, serde_json::Value>,
}

/// Canary run result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanaryRunResult {
    pub canary_id: String,
    pub ran_at: i64,
    pub status: CanaryStatus,
    pub latency_ms: i64,
    pub failed_assertions: Vec<AssertionFailure>,
    pub hit_count: usize,
    pub top_hit_id: Option<String>,
}

/// Canary run status
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryStatus {
    Passed,
    Failed,
    Error,
}

/// Assertion failure details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertionFailure {
    pub assertion_type: String,
    pub expected: serde_json::Value,
    pub actual: serde_json::Value,
    pub message: String,
}

/// Search response from Meilisearch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub hits: Vec<Hit>,
    pub estimated_total_hits: usize,
    pub processing_time_ms: usize,
    pub query: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hit {
    #[serde(flatten)]
    pub fields: HashMap<String, serde_json::Value>,
}

/// Search executor callback for canary queries.
pub type SearchExecutor = Arc<
    dyn Fn(
            &str,
            &SearchQuery,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SearchResponse>> + Send>>
        + Send
        + Sync,
>;

/// Metrics emitter callback for canary runs.
pub type MetricsEmitter = Arc<dyn Fn(&CanaryRunResult) + Send + Sync>;

/// Settings version checker callback.
pub type SettingsVersionChecker = Arc<dyn Fn(&str) -> Option<i64> + Send + Sync>;

/// Canary runner
pub struct CanaryRunner {
    store: Arc<dyn TaskStore>,
    running: Arc<RwLock<HashMap<String, bool>>>,
    max_concurrent: usize,
    run_history_limit: usize,
    search_executor: SearchExecutor,
    metrics_emitter: MetricsEmitter,
    settings_version_checker: SettingsVersionChecker,
    /// Mode A coordinator for partitioning canary execution (plan §14.5).
    #[cfg(feature = "peer-discovery")]
    mode_a_coordinator: Option<Arc<ModeACoordinator>>,
}

impl CanaryRunner {
    pub fn new(
        store: Arc<dyn TaskStore>,
        max_concurrent: usize,
        run_history_limit: usize,
        search_executor: SearchExecutor,
        metrics_emitter: MetricsEmitter,
        settings_version_checker: SettingsVersionChecker,
    ) -> Self {
        Self {
            store,
            running: Arc::new(RwLock::new(HashMap::new())),
            max_concurrent,
            run_history_limit,
            search_executor,
            metrics_emitter,
            settings_version_checker,
            #[cfg(feature = "peer-discovery")]
            mode_a_coordinator: None,
        }
    }

    /// Set Mode A coordinator for partitioning canary execution (plan §14.5).
    ///
    /// When enabled, each pod only runs canaries where it wins the rendezvous
    /// score for the canary ID: `top1_by_score(hash(canary_id || pid) for pid in peers)`.
    #[cfg(feature = "peer-discovery")]
    pub fn with_mode_a(mut self, coordinator: Arc<ModeACoordinator>) -> Self {
        self.mode_a_coordinator = Some(coordinator);
        self
    }

    /// Start the background canary runner
    pub async fn start(&self) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            interval.tick().await;
            if let Err(e) = self.run_due_canaries().await {
                tracing::error!("Canary runner error: {}", e);
            }
        }
    }

    /// Run all canaries that are due
    async fn run_due_canaries(&self) -> Result<()> {
        let canaries = self.store.list_canaries()?;
        let now = chrono::Utc::now().timestamp_millis();

        // Filter enabled canaries that are due
        let mut due_canaries = Vec::new();
        for canary in canaries {
            if !canary.enabled {
                continue;
            }

            // Mode A coordination: only run canaries owned by this pod
            #[cfg(feature = "peer-discovery")]
            if let Some(ref coordinator) = self.mode_a_coordinator {
                let owns_canary = coordinator.owns_task(&canary.id).await.unwrap_or(true); // Default to true if no coordinator
                if !owns_canary {
                    tracing::debug!(
                        canary_id = %canary.id,
                        "skipping canary not owned by this pod"
                    );
                    continue;
                }
            }

            // Check if already running
            let running = self.running.read().await;
            if running.get(&canary.id).copied().unwrap_or(false) {
                continue;
            }
            drop(running);

            // Check last run time
            let runs = self.store.get_canary_runs(&canary.id, 1)?;
            let should_run = if let Some(last_run) = runs.first() {
                let elapsed_ms = now - last_run.ran_at;
                elapsed_ms >= (canary.interval_s * 1000)
            } else {
                true
            };

            if should_run {
                due_canaries.push(canary);
            }
        }

        // Run up to max_concurrent canaries
        for canary in due_canaries.into_iter().take(self.max_concurrent) {
            let canary_id = canary.id.clone();
            let runner = self.clone_runner();

            // Mark as running
            {
                let mut running = self.running.write().await;
                running.insert(canary_id.clone(), true);
            }

            tokio::spawn(async move {
                let result = runner.run_canary(&canary).await;

                // Mark as not running
                {
                    let mut running = runner.running.write().await;
                    running.remove(&canary_id);
                }

                if let Err(e) = result {
                    tracing::error!("Canary {} failed: {}", canary_id, e);
                }
            });
        }

        Ok(())
    }

    /// Run a single canary
    async fn run_canary(&self, canary: &CanaryRow) -> Result<CanaryRunResult> {
        let start = Instant::now();
        let now = chrono::Utc::now().timestamp_millis();

        // Parse query
        let query: SearchQuery = serde_json::from_str(&canary.query_json)
            .map_err(|e| MiroirError::InvalidRequest(format!("Invalid canary query: {e}")))?;

        // Parse assertions
        let assertions: Vec<CanaryAssertion> = serde_json::from_str(&canary.assertions_json)
            .map_err(|e| {
                MiroirError::InvalidRequest(format!("Invalid canary assertions: {e}"))
            })?;

        // Execute the search query against the index
        // Note: This would need to be wired to the actual search client
        // For now, we simulate a search response
        let search_response = self.execute_search(&canary.index_uid, &query).await?;

        let latency_ms = start.elapsed().as_millis() as i64;

        // Evaluate assertions
        let mut failed_assertions = Vec::new();
        for assertion in &assertions {
            if let Some(failure) =
                self.evaluate_assertion(assertion, &search_response, latency_ms, &canary.index_uid)
            {
                failed_assertions.push(failure);
            }
        }

        let status = if failed_assertions.is_empty() {
            CanaryStatus::Passed
        } else {
            CanaryStatus::Failed
        };

        let result = CanaryRunResult {
            canary_id: canary.id.clone(),
            ran_at: now,
            status,
            latency_ms,
            failed_assertions,
            hit_count: search_response.hits.len(),
            top_hit_id: search_response.hits.first().and_then(|h| {
                h.fields
                    .get("id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            }),
        };

        // Store the run
        let new_run = NewCanaryRun {
            canary_id: canary.id.clone(),
            ran_at: now,
            status: serde_json::to_string(&result.status).unwrap_or_default(),
            latency_ms: result.latency_ms,
            failed_assertions_json: if result.failed_assertions.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&result.failed_assertions).unwrap_or_default())
            },
        };

        self.store
            .insert_canary_run(&new_run, self.run_history_limit)?;

        // Emit metrics
        self.emit_metrics(&result);

        Ok(result)
    }

    /// Execute a search query against the index
    async fn execute_search(&self, index_uid: &str, query: &SearchQuery) -> Result<SearchResponse> {
        // Call the search executor callback (async)
        (self.search_executor)(index_uid, query).await
    }

    /// Evaluate a single assertion
    fn evaluate_assertion(
        &self,
        assertion: &CanaryAssertion,
        response: &SearchResponse,
        latency_ms: i64,
        index_uid: &str,
    ) -> Option<AssertionFailure> {
        match assertion {
            CanaryAssertion::TopHitId { value } => {
                let top_hit_id = response
                    .hits
                    .first()
                    .and_then(|h| h.fields.get("id"))
                    .and_then(|v| v.as_str());

                let actual = top_hit_id.unwrap_or("");
                if actual != value {
                    return Some(AssertionFailure {
                        assertion_type: "top_hit_id".to_string(),
                        expected: serde_json::json!(value),
                        actual: serde_json::json!(actual),
                        message: format!("Top hit ID mismatch: expected {value}, got {actual}"),
                    });
                }
            }
            CanaryAssertion::TopKContains { k, ids } => {
                let top_k_ids: Vec<&str> = response
                    .hits
                    .iter()
                    .take(*k)
                    .filter_map(|h| h.fields.get("id"))
                    .filter_map(|v| v.as_str())
                    .collect();

                let missing: Vec<_> = ids
                    .iter()
                    .filter(|id| !top_k_ids.contains(&id.as_str()))
                    .collect();

                if !missing.is_empty() {
                    return Some(AssertionFailure {
                        assertion_type: "top_k_contains".to_string(),
                        expected: serde_json::json!(ids),
                        actual: serde_json::json!(top_k_ids),
                        message: format!("Top {k} missing IDs: {missing:?}"),
                    });
                }
            }
            CanaryAssertion::MinHits { value } => {
                if response.hits.len() < *value {
                    return Some(AssertionFailure {
                        assertion_type: "min_hits".to_string(),
                        expected: serde_json::json!(value),
                        actual: serde_json::json!(response.hits.len()),
                        message: format!(
                            "Hit count below minimum: {} < {}",
                            response.hits.len(),
                            value
                        ),
                    });
                }
            }
            CanaryAssertion::MaxP95Ms { value } => {
                if latency_ms as u64 > *value {
                    return Some(AssertionFailure {
                        assertion_type: "max_p95_ms".to_string(),
                        expected: serde_json::json!(value),
                        actual: serde_json::json!(latency_ms),
                        message: format!("Latency exceeded p95: {latency_ms}ms > {value}ms"),
                    });
                }
            }
            CanaryAssertion::SettingsVersionAtLeast { value } => {
                // Get current settings version for the index
                let current_version = (self.settings_version_checker)(index_uid).unwrap_or(0);

                if current_version < *value {
                    return Some(AssertionFailure {
                        assertion_type: "settings_version_at_least".to_string(),
                        expected: serde_json::json!(value),
                        actual: serde_json::json!(current_version),
                        message: format!(
                            "Settings version below minimum: {current_version} < {value}"
                        ),
                    });
                }
            }
            CanaryAssertion::MustNotContainId { id } => {
                let contains = response.hits.iter().any(|h| {
                    h.fields
                        .get("id")
                        .and_then(|v| v.as_str())
                        .map(|v| v == id.as_str())
                        .unwrap_or(false)
                });

                if contains {
                    return Some(AssertionFailure {
                        assertion_type: "must_not_contain_id".to_string(),
                        expected: serde_json::json!(null),
                        actual: serde_json::json!(id),
                        message: format!("Results contain forbidden ID: {id}"),
                    });
                }
            }
        }
        None
    }

    /// Emit metrics for a canary run
    fn emit_metrics(&self, result: &CanaryRunResult) {
        // Call the metrics emitter callback
        (self.metrics_emitter)(result);

        // Also log for observability
        match result.status {
            CanaryStatus::Passed => {
                tracing::info!(
                    canary = %result.canary_id,
                    latency_ms = result.latency_ms,
                    "Canary passed"
                );
            }
            CanaryStatus::Failed => {
                tracing::warn!(
                    canary = %result.canary_id,
                    latency_ms = result.latency_ms,
                    failures = result.failed_assertions.len(),
                    "Canary failed"
                );
            }
            CanaryStatus::Error => {
                tracing::error!(
                    canary = %result.canary_id,
                    "Canary error"
                );
            }
        }
    }

    /// Clone the runner for use in spawned tasks
    fn clone_runner(&self) -> Self {
        Self {
            store: self.store.clone(),
            running: self.running.clone(),
            max_concurrent: self.max_concurrent,
            run_history_limit: self.run_history_limit,
            search_executor: self.search_executor.clone(),
            metrics_emitter: self.metrics_emitter.clone(),
            settings_version_checker: self.settings_version_checker.clone(),
            #[cfg(feature = "peer-discovery")]
            mode_a_coordinator: self.mode_a_coordinator.clone(),
        }
    }
}

/// Create a canary from a definition
pub fn create_canary(
    id: String,
    name: String,
    index_uid: String,
    interval_s: i64,
    query: SearchQuery,
    assertions: Vec<CanaryAssertion>,
) -> Result<NewCanary> {
    let now = chrono::Utc::now().timestamp_millis();

    Ok(NewCanary {
        id: id.clone(),
        name,
        index_uid,
        interval_s,
        query_json: serde_json::to_string(&query).map_err(|e| {
            MiroirError::InvalidRequest(format!("Failed to serialize query: {e}"))
        })?,
        assertions_json: serde_json::to_string(&assertions).map_err(|e| {
            MiroirError::InvalidRequest(format!("Failed to serialize assertions: {e}"))
        })?,
        enabled: true,
        created_at: now,
    })
}

/// Capture a query from traffic as a canary candidate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapturedQuery {
    pub index_uid: String,
    pub query: SearchQuery,
    pub response: SearchResponse,
    pub timestamp: i64,
}

pub struct QueryCapture {
    queries: Arc<RwLock<Vec<CapturedQuery>>>,
    max_queries: usize,
}

impl QueryCapture {
    pub fn new(max_queries: usize) -> Self {
        Self {
            queries: Arc::new(RwLock::new(Vec::new())),
            max_queries,
        }
    }

    /// Capture a query for canary creation
    pub async fn capture(&self, index_uid: String, query: SearchQuery, response: SearchResponse) {
        let mut queries = self.queries.write().await;
        let now = chrono::Utc::now().timestamp_millis();

        queries.push(CapturedQuery {
            index_uid,
            query,
            response,
            timestamp: now,
        });

        // Keep only the most recent queries
        let len = queries.len();
        if len > self.max_queries {
            queries.drain(0..len - self.max_queries);
        }
    }

    /// Get captured queries
    pub async fn get_captured(&self) -> Vec<CapturedQuery> {
        self.queries.read().await.clone()
    }

    /// Clear captured queries
    pub async fn clear(&self) {
        self.queries.write().await.clear();
    }
}
