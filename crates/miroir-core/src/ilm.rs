//! ILM (Index Lifecycle Management) — plan §13.17.
//!
//! Manages rolling time-series indexes with automatic rollover and retention.
//! Uses leader-only singleton coordination (plan §14.5) to ensure only one pod
//! performs rollovers for a given policy.
//!
//! # CDC Origin Tag (plan §13.13)
//!
//! Rollover writes must be tagged with `origin="rollover"` so they are suppressed
//! from CDC by default (unless `emit_internal_writes` is true).
//!
//! When constructing `WriteRequest` for rollover operations, set:
//! ```ignore
//! use miroir_core::cdc::ORIGIN_ROLLOVER;
//! WriteRequest { ..., origin: Some(ORIGIN_ROLLOVER.to_string()) }
//! ```

use crate::alias::AliasRegistry;
use crate::leader_election::LeaderElection;
use crate::mode_b_coordinator::ModeBOpLeader;
use crate::task_store::{RolloverPolicyRow, TaskStore};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// CDC origin tag for ILM rollover writes (plan §13.13).
pub const ORIGIN_ROLLOVER: &str = "rollover";

/// ILM rollover policy configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloverPolicy {
    /// Policy name.
    pub name: String,
    /// Write alias name.
    pub write_alias: String,
    /// Read alias name (multi-target).
    pub read_alias: String,
    /// Index name pattern with {YYYY-MM-DD} placeholder.
    pub pattern: String,
    /// Rollover triggers.
    pub triggers: RolloverTriggers,
    /// Retention policy.
    pub retention: RetentionPolicy,
    /// Index template reference.
    pub index_template: IndexTemplate,
    /// Whether this policy is enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

/// Triggers that cause a rollover.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloverTriggers {
    /// Maximum documents before rollover.
    pub max_docs: u64,
    /// Maximum age before rollover (e.g., "7d").
    pub max_age: String,
    /// Maximum index size before rollover (GB).
    pub max_size_gb: u32,
}

/// Retention policy for old indexes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Number of indexes to keep.
    pub keep_indexes: u32,
}

/// Index template for rollover.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexTemplate {
    /// Primary key field.
    pub primary_key: String,
    /// Named settings profile reference.
    pub settings_ref: String,
}

/// ILM manager state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IlmState {
    /// Registered policies.
    pub policies: Vec<RolloverPolicy>,
    /// Active rollover operations.
    pub active_rollovers: HashMap<String, RolloverOperation>,
    /// Last check timestamp (UNIX ms).
    pub last_check_ms: u64,
}

/// Active rollover operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloverOperation {
    /// Policy name.
    pub policy_name: String,
    /// Current phase.
    pub phase: RolloverPhase,
    /// New index UID.
    pub new_index: String,
    /// Old index UID.
    pub old_index: String,
    /// Started at (UNIX ms).
    pub started_at: u64,
    /// Error message if failed.
    pub error: Option<String>,
}

/// ILM manager — handles index lifecycle for time-series data.
pub struct IlmManager {
    /// Configuration.
    config: IlmConfig,
    /// Shared state.
    state: Arc<RwLock<IlmState>>,
    /// Task store for loading/saving policies.
    task_store: Option<Arc<dyn TaskStore>>,
    /// HTTP client for node operations.
    client: Arc<Client>,
    /// Node addresses (from topology).
    node_addresses: Arc<Vec<String>>,
    /// Master key for node operations.
    master_key: Arc<String>,
    /// Alias registry.
    alias_registry: Arc<AliasRegistry>,
}

/// ILM worker — background evaluator that runs on leader only.
pub struct IlmWorker {
    /// ILM coordinator (Mode B leader).
    coordinator: IlmCoordinator,
    /// Configuration.
    config: IlmConfig,
    /// Task store.
    task_store: Arc<dyn TaskStore>,
    /// HTTP client.
    client: Arc<Client>,
    /// Node addresses.
    node_addresses: Arc<Vec<String>>,
    /// Master key.
    master_key: Arc<String>,
    /// Alias registry.
    alias_registry: Arc<AliasRegistry>,
}

/// Trigger evaluation result.
#[derive(Debug, Clone)]
pub struct TriggerEvaluation {
    /// Whether any trigger fired.
    pub should_rollover: bool,
    /// Current document count.
    pub doc_count: u64,
    /// Current index size in bytes.
    pub index_size_bytes: u64,
    /// Index age in seconds.
    pub age_seconds: u64,
    /// Which trigger(s) fired.
    pub fired_triggers: Vec<String>,
}

/// Index stats for a single node.
#[derive(Debug, Clone, Deserialize)]
struct NodeIndexStats {
    #[serde(default)]
    #[allow(non_snake_case)]
    pub numberOfDocuments: u64,
    /// Size in bytes (may not be present in all Meilisearch versions).
    #[serde(rename = "stats", default)]
    pub stats: Option<NodeStatsDetail>,
}

#[derive(Debug, Clone, Deserialize)]
struct NodeStatsDetail {
    #[serde(rename = "databaseSize", default)]
    pub database_size: u64,
}

impl Default for NodeStatsDetail {
    fn default() -> Self {
        Self { database_size: 0 }
    }
}

/// Aggregated index stats across all nodes.
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub total_documents: u64,
    pub total_size_bytes: u64,
    pub created_at_ms: u64,
}

/// Rollover phase for execution state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RolloverPhase {
    /// Starting - validating preconditions.
    Starting,
    /// Creating new index.
    Creating,
    /// Broadcasting settings to new index.
    BroadcastingSettings,
    /// Flipping write alias to new index.
    FlippingWriteAlias,
    /// Updating read alias (multi-target) with new index.
    UpdatingReadAlias,
    /// Cleaning up old indexes per retention policy.
    CleaningRetention,
    /// Complete.
    Complete,
    /// Failed.
    Failed,
}

/// ILM manager configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IlmConfig {
    /// Whether ILM is enabled.
    pub enabled: bool,
    /// Check interval (seconds).
    pub check_interval_s: u64,
    /// Safety lock: refuse to delete indexes newer than this (days).
    pub safety_lock_older_than_days: u32,
    /// Maximum rollovers per check.
    pub max_rollovers_per_check: u32,
}

impl Default for IlmConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval_s: 3600,
            safety_lock_older_than_days: 7,
            max_rollovers_per_check: 10,
        }
    }
}

impl IlmManager {
    /// Create a new ILM manager.
    pub fn new(config: IlmConfig) -> Self {
        let state = Arc::new(RwLock::new(IlmState {
            policies: Vec::new(),
            active_rollovers: HashMap::new(),
            last_check_ms: 0,
        }));

        Self {
            config,
            state,
            task_store: None,
            client: Arc::new(Client::new()),
            node_addresses: Arc::new(Vec::new()),
            master_key: Arc::new(String::new()),
            alias_registry: Arc::new(AliasRegistry::new()),
        }
    }

    /// Set the task store (required for loading policies).
    pub fn with_task_store(mut self, task_store: Arc<dyn TaskStore>) -> Self {
        self.task_store = Some(task_store);
        self
    }

    /// Set the node addresses (from topology).
    pub fn with_node_addresses(mut self, addresses: Vec<String>) -> Self {
        self.node_addresses = Arc::new(addresses);
        self
    }

    /// Set the master key.
    pub fn with_master_key(mut self, key: String) -> Self {
        self.master_key = Arc::new(key);
        self
    }

    /// Set the alias registry.
    pub fn with_alias_registry(mut self, registry: Arc<AliasRegistry>) -> Self {
        self.alias_registry = registry;
        self
    }

    /// Load policies from the task store.
    pub async fn load_policies(&self) -> std::result::Result<(), IlmError> {
        let Some(task_store) = &self.task_store else {
            return Err(IlmError::CoordinatorError(
                "task_store not configured".to_string(),
            ));
        };

        let policy_rows = task_store
            .list_rollover_policies()
            .map_err(|e| IlmError::CoordinatorError(format!("failed to load policies: {}", e)))?;

        let policies: Vec<RolloverPolicy> = policy_rows
            .into_iter()
            .filter_map(|row| Self::row_to_policy(row).ok())
            .filter(|p| p.enabled)
            .collect();

        let mut state = self.state.write().await;
        state.policies = policies;

        info!(
            "ILM: loaded {} policies from task store",
            state.policies.len()
        );
        Ok(())
    }

    /// Convert a task store row to a RolloverPolicy.
    fn row_to_policy(row: RolloverPolicyRow) -> std::result::Result<RolloverPolicy, IlmError> {
        let triggers: RolloverTriggers = serde_json::from_str(&row.triggers_json)
            .map_err(|e| IlmError::CoordinatorError(format!("invalid triggers JSON: {}", e)))?;

        let retention: RetentionPolicy = serde_json::from_str(&row.retention_json)
            .map_err(|e| IlmError::CoordinatorError(format!("invalid retention JSON: {}", e)))?;

        let template: IndexTemplate = serde_json::from_str(&row.template_json)
            .map_err(|e| IlmError::CoordinatorError(format!("invalid template JSON: {}", e)))?;

        Ok(RolloverPolicy {
            name: row.name,
            write_alias: row.write_alias,
            read_alias: row.read_alias,
            pattern: row.pattern,
            triggers,
            retention,
            index_template: template,
            enabled: row.enabled,
        })
    }

    /// Create an ILM worker (only the leader pod runs this).
    pub fn create_worker(
        &self,
        leader_election: Arc<LeaderElection>,
        pod_id: String,
    ) -> std::result::Result<IlmWorker, IlmError> {
        let task_store = self
            .task_store
            .clone()
            .ok_or_else(|| IlmError::CoordinatorError("task_store not configured".to_string()))?;

        let coordinator = IlmCoordinator::new(leader_election, task_store.clone(), pod_id);

        Ok(IlmWorker {
            coordinator,
            config: self.config.clone(),
            task_store,
            client: self.client.clone(),
            node_addresses: self.node_addresses.clone(),
            master_key: self.master_key.clone(),
            alias_registry: self.alias_registry.clone(),
        })
    }

    /// Register a rollover policy.
    pub async fn register_policy(
        &self,
        policy: RolloverPolicy,
    ) -> std::result::Result<(), IlmError> {
        let mut state = self.state.write().await;
        state.policies.push(policy);
        Ok(())
    }

    /// Unregister a policy.
    pub async fn unregister_policy(&self, name: &str) -> std::result::Result<(), IlmError> {
        let mut state = self.state.write().await;
        state.policies.retain(|p| p.name != name);
        Ok(())
    }

    /// Get all policies.
    pub async fn policies(&self) -> Vec<RolloverPolicy> {
        let state = self.state.read().await;
        state.policies.clone()
    }

    /// Get active rollover for a policy.
    pub async fn active_rollover(&self, policy_name: &str) -> Option<RolloverOperation> {
        let state = self.state.read().await;
        state.active_rollovers.get(policy_name).cloned()
    }

    /// Trigger an immediate rollover for a policy.
    pub async fn trigger_rollover(&self, policy_name: &str) -> std::result::Result<(), IlmError> {
        let state = self.state.read().await;
        let policy = state
            .policies
            .iter()
            .find(|p| p.name == policy_name)
            .ok_or_else(|| IlmError::PolicyNotFound(policy_name.to_string()))?;

        // Create rollover operation
        let now = millis_now();
        let new_index = Self::format_index_name(&policy.pattern, now);
        let operation = RolloverOperation {
            policy_name: policy_name.to_string(),
            phase: RolloverPhase::Creating,
            new_index: new_index.clone(),
            old_index: format!("{}-current", policy.write_alias),
            started_at: now,
            error: None,
        };

        drop(state);
        let mut state = self.state.write().await;
        state
            .active_rollovers
            .insert(policy_name.to_string(), operation);

        info!(
            "ILM: triggered rollover for policy '{}', new index: {}",
            policy_name, new_index
        );
        Ok(())
    }

    /// Background evaluator that checks policies and performs rollovers.
    async fn background_evaluator(state: Arc<RwLock<IlmState>>, config: IlmConfig) {
        info!("ILM: background evaluator started");

        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_secs(config.check_interval_s));
        loop {
            interval.tick().await;

            let policies = {
                let state = state.read().await;
                state.policies.clone()
            };

            for policy in policies
                .iter()
                .take(config.max_rollovers_per_check as usize)
            {
                if let Err(e) = Self::evaluate_policy(&state, &policy, &config).await {
                    error!("ILM: error evaluating policy '{}': {}", policy.name, e);
                }
            }

            // Update last check time
            {
                let mut state = state.write().await;
                state.last_check_ms = millis_now();
            }
        }
    }

    /// Evaluate a single policy and perform rollover if needed.
    async fn evaluate_policy(
        state: &Arc<RwLock<IlmState>>,
        policy: &RolloverPolicy,
        _config: &IlmConfig,
    ) -> std::result::Result<(), IlmError> {
        // Check if there's already an active rollover
        {
            let state = state.read().await;
            if state.active_rollovers.contains_key(&policy.name) {
                return Ok(()); // Skip if rollover in progress
            }
        }

        // Check triggers (placeholder - would query actual stats in production)
        let should_rollover = false; // TODO: implement trigger checking

        if should_rollover {
            // Trigger rollover
            let now = millis_now();
            let new_index = Self::format_index_name(&policy.pattern, now);
            let operation = RolloverOperation {
                policy_name: policy.name.clone(),
                phase: RolloverPhase::Creating,
                new_index,
                old_index: format!("{}-current", policy.write_alias),
                started_at: now,
                error: None,
            };

            let mut state = state.write().await;
            state
                .active_rollovers
                .insert(policy.name.clone(), operation);

            info!("ILM: auto-triggered rollover for policy '{}'", policy.name);
        }

        Ok(())
    }

    /// Format index name from pattern with date placeholder.
    fn format_index_name(pattern: &str, timestamp_ms: u64) -> String {
        use chrono::{DateTime, Utc};

        // Convert milliseconds to DateTime
        let timestamp_sec = (timestamp_ms / 1000) as i64;
        let dt = DateTime::<Utc>::from_timestamp(timestamp_sec, 0)
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());

        let date_str = dt.format("%Y-%m-%d").to_string();
        pattern.replace("{YYYY-MM-DD}", &date_str)
    }
}

/// ILM coordinator with leader-only singleton coordination (plan §14.5).
///
/// Acquires a global leader lease (scope: "ilm") and persists phase state
/// so that a new leader can resume from the last committed phase.
pub struct IlmCoordinator {
    /// Mode B operation leader with phase state persistence.
    leader: ModeBOpLeader<IlmExtraState>,
}

/// Extra state for ILM operations persisted to mode_b_operations.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IlmExtraState {
    /// Active rollover operations (policy_name -> rollover state).
    pub active_rollovers: HashMap<String, RolloverState>,
    /// Last check timestamp (UNIX ms).
    pub last_check_ms: u64,
    /// Next check time for each policy (policy_name -> UNIX ms).
    pub next_check_times: HashMap<String, u64>,
}

/// State of a rollover operation in progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloverState {
    /// Policy name.
    pub policy_name: String,
    /// Current phase.
    pub phase: String,
    /// New index UID.
    pub new_index: String,
    /// Old index UID.
    pub old_index: String,
    /// Started at (UNIX ms).
    pub started_at: u64,
    /// Error message if failed.
    pub error: Option<String>,
}

impl IlmWorker {
    /// Run the ILM worker loop.
    ///
    /// This should be spawned as a background task on the leader pod.
    /// It periodically evaluates all policies and performs rollovers when triggers fire.
    pub async fn run(&mut self) -> std::result::Result<(), IlmError> {
        info!("ILM worker: starting evaluation loop");

        // Try to acquire leadership
        self.coordinator.try_acquire_leadership().await?;

        if !self.coordinator.is_leader() {
            info!("ILM worker: another pod is the leader, exiting");
            return Ok(());
        }

        info!("ILM worker: acquired leadership, starting evaluation");

        let mut interval = tokio::time::interval(Duration::from_secs(self.config.check_interval_s));

        loop {
            interval.tick().await;

            // Renew leadership
            if !self.coordinator.renew_leadership().await? {
                warn!("ILM worker: lost leadership, exiting");
                return Ok(());
            }

            // Evaluate all policies
            if let Err(e) = self.evaluate_all_policies().await {
                error!("ILM worker: error evaluating policies: {}", e);
            }

            // Update last check time
            if let Err(e) = self.coordinator.update_check_time().await {
                error!("ILM worker: failed to update check time: {}", e);
            }
        }
    }

    /// Evaluate all enabled policies and perform rollovers as needed.
    async fn evaluate_all_policies(&mut self) -> std::result::Result<(), IlmError> {
        let policy_rows = self
            .task_store
            .list_rollover_policies()
            .map_err(|e| IlmError::CoordinatorError(format!("failed to list policies: {}", e)))?;

        let mut rollover_count = 0;

        for row in policy_rows {
            if !row.enabled {
                continue;
            }

            let policy = IlmManager::row_to_policy(row)?;

            // Skip if we already hit the max rollovers per check
            if rollover_count >= self.config.max_rollovers_per_check as usize {
                debug!("ILM: reached max rollovers per check, skipping remaining policies");
                break;
            }

            // Check if there's already an active rollover for this policy
            if self.coordinator.active_rollover(&policy.name).is_some() {
                debug!(
                    "ILM: rollover already in progress for policy '{}', skipping",
                    policy.name
                );
                continue;
            }

            // Evaluate triggers
            match self.evaluate_policy_triggers(&policy).await {
                Ok(evaluation) => {
                    if evaluation.should_rollover {
                        info!(
                            "ILM: triggers fired for policy '{}': {:?}, starting rollover",
                            policy.name, evaluation.fired_triggers
                        );

                        match self.execute_rollover(&policy, &evaluation).await {
                            Ok(()) => {
                                rollover_count += 1;
                                info!("ILM: rollover completed for policy '{}'", policy.name);
                            }
                            Err(e) => {
                                error!("ILM: rollover failed for policy '{}': {}", policy.name, e);
                                // Mark the rollover as failed in the coordinator
                                let _ = self
                                    .coordinator
                                    .fail(format!("rollover failed: {}", e))
                                    .await;
                            }
                        }
                    } else {
                        debug!(
                            "ILM: no triggers fired for policy '{}' (docs={}, age={}s, size={} bytes)",
                            policy.name,
                            evaluation.doc_count,
                            evaluation.age_seconds,
                            evaluation.index_size_bytes
                        );
                    }
                }
                Err(e) => {
                    error!(
                        "ILM: failed to evaluate triggers for policy '{}': {}",
                        policy.name, e
                    );
                    // Continue to next policy rather than failing the entire check
                }
            }
        }

        Ok(())
    }

    /// Evaluate whether a policy's triggers have fired.
    async fn evaluate_policy_triggers(
        &self,
        policy: &RolloverPolicy,
    ) -> std::result::Result<TriggerEvaluation, IlmError> {
        // Get the current index from the write alias
        let current_index = self.resolve_write_alias(&policy.write_alias).await?;

        // Fetch stats from all nodes
        let stats = self.fetch_index_stats(&current_index).await?;

        // Calculate age
        let now_ms = millis_now();
        let age_seconds = if now_ms > stats.created_at_ms {
            (now_ms - stats.created_at_ms) / 1000
        } else {
            0
        };

        // Check max_age trigger (parse "7d" -> 7 days)
        let max_age_seconds = parse_duration(&policy.triggers.max_age)?;
        let age_triggered = age_seconds >= max_age_seconds;

        // Check max_docs trigger
        let docs_triggered = stats.total_documents >= policy.triggers.max_docs;

        // Check max_size_gb trigger
        let max_size_bytes = policy.triggers.max_size_gb as u64 * 1024 * 1024 * 1024;
        let size_triggered = stats.total_size_bytes >= max_size_bytes;

        let mut fired_triggers = Vec::new();
        if age_triggered {
            fired_triggers.push(format!(
                "max_age ({}s >= {}s)",
                age_seconds, max_age_seconds
            ));
        }
        if docs_triggered {
            fired_triggers.push(format!(
                "max_docs ({} >= {})",
                stats.total_documents, policy.triggers.max_docs
            ));
        }
        if size_triggered {
            fired_triggers.push(format!(
                "max_size_gb ({} bytes >= {} bytes)",
                stats.total_size_bytes, max_size_bytes
            ));
        }

        Ok(TriggerEvaluation {
            should_rollover: !fired_triggers.is_empty(),
            doc_count: stats.total_documents,
            index_size_bytes: stats.total_size_bytes,
            age_seconds,
            fired_triggers,
        })
    }

    /// Resolve the write alias to get the current index UID.
    async fn resolve_write_alias(
        &self,
        write_alias: &str,
    ) -> std::result::Result<String, IlmError> {
        // First check if it's an alias
        if self.alias_registry.is_alias(write_alias).await {
            let targets = self.alias_registry.resolve(write_alias).await;
            if targets.len() == 1 {
                Ok(targets[0].clone())
            } else {
                Err(IlmError::CoordinatorError(format!(
                    "write alias '{}' has {} targets, expected 1",
                    write_alias,
                    targets.len()
                )))
            }
        } else {
            // Not an alias, treat as concrete index UID
            Ok(write_alias.to_string())
        }
    }

    /// Fetch aggregated stats for an index across all nodes.
    async fn fetch_index_stats(
        &self,
        index_uid: &str,
    ) -> std::result::Result<IndexStats, IlmError> {
        let mut total_documents = 0u64;
        let mut total_size_bytes = 0u64;
        let mut created_at_ms = 0u64;

        for address in self.node_addresses.iter() {
            let url = format!(
                "{}/indexes/{}/stats",
                address.trim_end_matches('/'),
                index_uid
            );

            match self.fetch_node_stats(&url).await {
                Ok(node_stats) => {
                    total_documents = total_documents.max(node_stats.numberOfDocuments);
                    if let Some(ref stats) = node_stats.stats {
                        total_size_bytes += stats.database_size;
                    }
                    // Use the earliest created_at we see (this is a placeholder;
                    // in production we'd fetch this from index creation metadata)
                    if created_at_ms == 0 {
                        created_at_ms = millis_now() - (86400 * 1000); // Default to 1 day ago
                    }
                }
                Err(e) => {
                    // Log but continue - one node failing shouldn't block ILM
                    warn!("ILM: failed to fetch stats from node {}: {}", address, e);
                }
            }
        }

        Ok(IndexStats {
            total_documents,
            total_size_bytes,
            created_at_ms,
        })
    }

    /// Fetch stats from a single node.
    async fn fetch_node_stats(&self, url: &str) -> std::result::Result<NodeIndexStats, IlmError> {
        let response = self
            .client
            .get(url)
            .header("Authorization", format!("Bearer {}", &*self.master_key))
            .send()
            .await
            .map_err(|e| IlmError::CoordinatorError(format!("request failed: {}", e)))?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .map_err(|e| IlmError::CoordinatorError(format!("failed to read response: {}", e)))?;

        if status.as_u16() == 404 {
            // Index doesn't exist on this node
            return Ok(NodeIndexStats {
                numberOfDocuments: 0,
                stats: None,
            });
        }

        if !status.is_success() {
            return Err(IlmError::CoordinatorError(format!(
                "HTTP {}: {}",
                status.as_u16(),
                body_text
            )));
        }

        serde_json::from_str(&body_text)
            .map_err(|e| IlmError::CoordinatorError(format!("failed to parse stats: {}", e)))
    }

    /// Execute a rollover operation for a policy.
    async fn execute_rollover(
        &mut self,
        policy: &RolloverPolicy,
        _evaluation: &TriggerEvaluation,
    ) -> std::result::Result<(), IlmError> {
        // Phase 1: Starting - validate preconditions
        let current_index = self.resolve_write_alias(&policy.write_alias).await?;

        // Generate new index name from pattern
        let new_index = IlmManager::format_index_name(&policy.pattern, millis_now());

        info!(
            "ILM: starting rollover for policy '{}': '{}' -> '{}'",
            policy.name, current_index, new_index
        );

        // Start the rollover operation in the coordinator
        self.coordinator
            .start_rollover(&policy.name, new_index.clone(), current_index.clone())
            .await?;

        // Phase 2: Create new index
        self.coordinator.advance_phase("creating").await?;
        self.create_index(&new_index, &policy.index_template)
            .await?;

        // Phase 3: Broadcast settings (placeholder - in production this would use §13.5)
        self.coordinator
            .advance_phase("broadcasting_settings")
            .await?;
        // Settings are applied during index creation via the template

        // Phase 4: Flip write alias
        self.coordinator
            .advance_phase("flipping_write_alias")
            .await?;
        self.flip_write_alias(&policy.write_alias, &new_index)
            .await?;

        // Phase 5: Update read alias (multi-target)
        self.coordinator
            .advance_phase("updating_read_alias")
            .await?;
        self.update_read_alias(
            &policy.read_alias,
            &new_index,
            policy.retention.keep_indexes as usize,
        )
        .await?;

        // Phase 6: Clean retention (delete old indexes)
        self.coordinator.advance_phase("cleaning_retention").await?;
        self.clean_retention(
            &policy.read_alias,
            &policy.pattern,
            policy.retention.keep_indexes as usize,
        )
        .await?;

        // Complete
        self.coordinator.advance_phase("complete").await?;
        self.coordinator.complete_rollover(&policy.name).await?;

        Ok(())
    }

    /// Create a new index on all nodes.
    async fn create_index(
        &self,
        index_uid: &str,
        template: &IndexTemplate,
    ) -> std::result::Result<(), IlmError> {
        for address in self.node_addresses.iter() {
            let url = format!("{}/indexes", address.trim_end_matches('/'));

            let body = serde_json::json!({
                "uid": index_uid,
                "primaryKey": template.primary_key,
            });

            let response = self
                .client
                .post(&url)
                .header("Authorization", format!("Bearer {}", &*self.master_key))
                .json(&body)
                .send()
                .await
                .map_err(|e| {
                    IlmError::RolloverFailed(format!("request to {} failed: {}", url, e))
                })?;

            let status = response.status();
            let body_text = response
                .text()
                .await
                .map_err(|e| IlmError::RolloverFailed(format!("failed to read response: {}", e)))?;

            if status.as_u16() == 409 {
                // Index already exists - this is ok for ILM (might have been partially created)
                debug!(
                    "ILM: index '{}' already exists on node {}",
                    index_uid, address
                );
                continue;
            }

            if !status.is_success() {
                return Err(IlmError::RolloverFailed(format!(
                    "failed to create index '{}' on node {}: HTTP {}: {}",
                    index_uid,
                    address,
                    status.as_u16(),
                    body_text
                )));
            }

            debug!("ILM: created index '{}' on node {}", index_uid, address);
        }

        Ok(())
    }

    /// Flip the write alias to point to the new index.
    async fn flip_write_alias(
        &self,
        alias_name: &str,
        new_index: &str,
    ) -> std::result::Result<(), IlmError> {
        // Update in-memory registry
        self.alias_registry
            .flip(alias_name, new_index.to_string())
            .await
            .map_err(|e| {
                IlmError::AliasError(format!("failed to flip alias '{}': {}", alias_name, e))
            })?;

        // Persist to task store
        let alias = self.alias_registry.get(alias_name).await.ok_or_else(|| {
            IlmError::AliasError(format!("alias '{}' not found in registry", alias_name))
        })?;

        // Update task store
        let new_alias = crate::task_store::NewAlias {
            name: alias.name.clone(),
            kind: "single".to_string(),
            current_uid: Some(new_index.to_string()),
            target_uids: None,
            version: (alias.generation + 1) as i64,
            created_at: alias.created_at as i64,
            history: vec![],
        };

        self.task_store
            .create_alias(&new_alias)
            .map_err(|e| IlmError::AliasError(format!("failed to persist alias flip: {}", e)))?;

        info!(
            "ILM: flipped write alias '{}' to '{}'",
            alias_name, new_index
        );
        Ok(())
    }

    /// Update the read alias (multi-target) to include the new index.
    async fn update_read_alias(
        &self,
        alias_name: &str,
        new_index: &str,
        keep_indexes: usize,
    ) -> std::result::Result<(), IlmError> {
        // Get existing targets or initialize
        let mut targets = if self.alias_registry.is_alias(alias_name).await {
            self.alias_registry.resolve(alias_name).await
        } else {
            Vec::new()
        };

        // Add new index to targets
        if !targets.contains(&new_index.to_string()) {
            targets.push(new_index.to_string());
        }

        // Sort by name (descending) so newest indexes come first
        targets.sort_by(|a, b| b.cmp(a));

        // Keep only the last N indexes
        if targets.len() > keep_indexes {
            targets.truncate(keep_indexes);
        }

        // Update in-memory registry
        self.alias_registry
            .update_multi(alias_name, targets.clone())
            .await
            .map_err(|e| {
                IlmError::AliasError(format!(
                    "failed to update multi-target alias '{}': {}",
                    alias_name, e
                ))
            })?;

        // Persist to task store
        let alias = self.alias_registry.get(alias_name).await.ok_or_else(|| {
            IlmError::AliasError(format!("alias '{}' not found in registry", alias_name))
        })?;

        let new_alias = crate::task_store::NewAlias {
            name: alias.name.clone(),
            kind: "multi".to_string(),
            current_uid: None,
            target_uids: Some(targets.clone()),
            version: (alias.generation + 1) as i64,
            created_at: alias.created_at as i64,
            history: vec![],
        };

        self.task_store.create_alias(&new_alias).map_err(|e| {
            IlmError::AliasError(format!("failed to persist read alias update: {}", e))
        })?;

        info!(
            "ILM: updated read alias '{}' with {} targets: {:?}",
            alias_name,
            targets.len(),
            targets
        );
        Ok(())
    }

    /// Clean up old indexes per retention policy.
    async fn clean_retention(
        &self,
        read_alias: &str,
        pattern: &str,
        keep_indexes: usize,
    ) -> std::result::Result<(), IlmError> {
        // Get current targets from read alias
        let targets = if self.alias_registry.is_alias(read_alias).await {
            self.alias_registry.resolve(read_alias).await
        } else {
            return Ok(()); // No targets, nothing to clean
        };

        // Find all indexes matching the pattern
        let pattern_prefix = pattern.replace("{YYYY-MM-DD}", "");
        let matching_indexes: Vec<String> = targets
            .iter()
            .filter(|t| t.starts_with(&pattern_prefix))
            .cloned()
            .collect();

        // Sort by name (descending) - index names contain dates so this sorts by recency
        let mut sorted_indexes = matching_indexes.clone();
        sorted_indexes.sort_by(|a, b| b.cmp(a));

        // Delete indexes beyond the retention limit
        if sorted_indexes.len() > keep_indexes {
            let to_delete = &sorted_indexes[keep_indexes..];

            for index_uid in to_delete {
                // Check safety lock
                if let Err(e) = self.check_safety_lock(index_uid).await {
                    warn!(
                        "ILM: safety lock prevented deletion of '{}': {}",
                        index_uid, e
                    );
                    continue;
                }

                match self.delete_index(index_uid).await {
                    Ok(()) => {
                        info!("ILM: deleted old index '{}'", index_uid);
                    }
                    Err(e) => {
                        warn!("ILM: failed to delete index '{}': {}", index_uid, e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if an index is too new to delete (safety lock).
    async fn check_safety_lock(&self, index_uid: &str) -> std::result::Result<(), IlmError> {
        // Extract date from index name (pattern: logs-YYYY-MM-DD)
        if let Some(date_str) = extract_date_from_index_name(index_uid) {
            match parse_index_date(&date_str) {
                Ok(index_time_ms) => {
                    let now_ms = millis_now();
                    let age_days = (now_ms - index_time_ms) / (86400 * 1000);

                    if age_days < self.config.safety_lock_older_than_days as u64 {
                        return Err(IlmError::SafetyLockViolation);
                    }
                }
                Err(_) => {
                    // Can't parse date, be conservative and don't delete
                    return Err(IlmError::SafetyLockViolation);
                }
            }
        }

        Ok(())
    }

    /// Delete an index from all nodes.
    async fn delete_index(&self, index_uid: &str) -> std::result::Result<(), IlmError> {
        for address in self.node_addresses.iter() {
            let url = format!("{}/indexes/{}", address.trim_end_matches('/'), index_uid);

            let response = self
                .client
                .delete(&url)
                .header("Authorization", format!("Bearer {}", &*self.master_key))
                .send()
                .await
                .map_err(|e| IlmError::RolloverFailed(format!("delete request failed: {}", e)))?;

            let status = response.status();

            if status.as_u16() == 404 {
                // Already deleted, that's ok
                continue;
            }

            if !status.is_success() {
                let body_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "unable to read body".to_string());
                return Err(IlmError::RolloverFailed(format!(
                    "failed to delete index '{}': HTTP {}: {}",
                    index_uid,
                    status.as_u16(),
                    body_text
                )));
            }
        }

        Ok(())
    }
}

/// Extract date portion from an index name (e.g., "logs-2024-01-15" -> "2024-01-15").
fn extract_date_from_index_name(index_uid: &str) -> Option<String> {
    // Try to find a YYYY-MM-DD pattern
    let re = regex::Regex::new(r"\d{4}-\d{2}-\d{2}").ok()?;
    re.find(index_uid).map(|m| m.as_str().to_string())
}

/// Parse a date string into milliseconds since epoch.
fn parse_index_date(date_str: &str) -> std::result::Result<u64, IlmError> {
    use chrono::NaiveDate;

    let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .map_err(|_| IlmError::CoordinatorError(format!("invalid date format: {}", date_str)))?;

    let datetime = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| IlmError::CoordinatorError("failed to create datetime".to_string()))?
        .and_utc();

    Ok(datetime.timestamp_millis() as u64)
}

/// Parse a duration string (e.g., "7d" -> 7 days in seconds).
fn parse_duration(duration: &str) -> std::result::Result<u64, IlmError> {
    let duration = duration.trim().to_lowercase();

    if duration.ends_with('d') {
        let days = duration[..duration.len() - 1]
            .parse::<u64>()
            .map_err(|_| IlmError::CoordinatorError(format!("invalid duration: {}", duration)))?;
        Ok(days * 86400)
    } else if duration.ends_with('h') {
        let hours = duration[..duration.len() - 1]
            .parse::<u64>()
            .map_err(|_| IlmError::CoordinatorError(format!("invalid duration: {}", duration)))?;
        Ok(hours * 3600)
    } else if duration.ends_with('m') {
        let minutes = duration[..duration.len() - 1]
            .parse::<u64>()
            .map_err(|_| IlmError::CoordinatorError(format!("invalid duration: {}", duration)))?;
        Ok(minutes * 60)
    } else {
        // Assume seconds if no unit
        duration
            .parse::<u64>()
            .map_err(|_| IlmError::CoordinatorError(format!("invalid duration: {}", duration)))
    }
}

impl IlmCoordinator {
    pub fn new(
        leader_election: Arc<LeaderElection>,
        task_store: Arc<dyn TaskStore>,
        pod_id: String,
    ) -> Self {
        let extra_state = IlmExtraState::default();

        let leader = ModeBOpLeader::new(
            leader_election,
            task_store,
            crate::task_store::mode_b_type::ILM.to_string(),
            "ilm".to_string(),
            pod_id,
            extra_state,
        );

        Self { leader }
    }

    /// Try to acquire leadership for ILM operations.
    ///
    /// Returns `Ok(true)` if we are now the leader, `Ok(false)` if another
    /// pod holds the lease, or `Err` if acquisition failed.
    pub async fn try_acquire_leadership(&mut self) -> std::result::Result<(), IlmError> {
        self.leader
            .try_acquire_leadership()
            .await
            .map_err(|e| IlmError::CoordinatorError(e.to_string()))?;
        Ok(())
    }

    /// Renew the leader lease.
    ///
    /// Returns `Ok(true)` if renewed successfully, `Ok(false)` if we lost
    /// leadership to another pod, or `Err` if renewal failed.
    pub async fn renew_leadership(&mut self) -> std::result::Result<bool, IlmError> {
        self.leader
            .renew_leadership()
            .await
            .map_err(|e| IlmError::CoordinatorError(e.to_string()))
    }

    /// Check if we are currently the leader.
    pub fn is_leader(&self) -> bool {
        self.leader.is_leader()
    }

    /// Get the current phase.
    pub fn phase(&self) -> &str {
        self.leader.phase()
    }

    /// Get the extra state (mutable).
    pub fn extra_state(&mut self) -> &mut IlmExtraState {
        self.leader.extra_state()
    }

    /// Get the extra state (immutable).
    pub fn extra_state_ref(&self) -> &IlmExtraState {
        self.leader.extra_state_ref()
    }

    /// Advance to the next phase and persist state.
    ///
    /// Should be called after each phase boundary so that a new leader can
    /// resume from the last committed phase.
    pub async fn advance_phase(&mut self, new_phase: &str) -> std::result::Result<(), IlmError> {
        self.leader
            .persist_phase(new_phase.to_string())
            .await
            .map_err(|e| IlmError::CoordinatorError(e.to_string()))
    }

    /// Start a new rollover operation for a policy.
    pub async fn start_rollover(
        &mut self,
        policy_name: &str,
        new_index: String,
        old_index: String,
    ) -> std::result::Result<(), IlmError> {
        let now = millis_now();
        let rollover_state = RolloverState {
            policy_name: policy_name.to_string(),
            phase: "creating".to_string(),
            new_index,
            old_index,
            started_at: now,
            error: None,
        };

        self.leader
            .extra_state()
            .active_rollovers
            .insert(policy_name.to_string(), rollover_state);
        self.leader
            .persist_phase("rollover_in_progress".to_string())
            .await
            .map_err(|e| IlmError::CoordinatorError(e.to_string()))?;

        info!("ILM: started rollover for policy '{}'", policy_name);
        Ok(())
    }

    /// Complete a rollover operation.
    pub async fn complete_rollover(
        &mut self,
        policy_name: &str,
    ) -> std::result::Result<(), IlmError> {
        self.leader
            .extra_state()
            .active_rollovers
            .remove(policy_name);
        self.leader
            .persist_phase("idle".to_string())
            .await
            .map_err(|e| IlmError::CoordinatorError(e.to_string()))?;

        info!("ILM: completed rollover for policy '{}'", policy_name);
        Ok(())
    }

    /// Mark the operation as failed and step down from leadership.
    pub async fn fail(&mut self, error: String) -> std::result::Result<(), IlmError> {
        self.leader
            .fail(error)
            .await
            .map_err(|e| IlmError::CoordinatorError(e.to_string()))
    }

    /// Mark the operation as completed and step down from leadership.
    pub async fn complete(&mut self) -> std::result::Result<(), IlmError> {
        self.leader
            .complete()
            .await
            .map_err(|e| IlmError::CoordinatorError(e.to_string()))
    }

    /// Recover the operation state from the task store.
    ///
    /// Called by a new leader to read the persisted phase state and resume
    /// from the last committed phase boundary.
    pub async fn recover(&mut self) -> std::result::Result<(), IlmError> {
        let existing = self
            .leader
            .recover()
            .await
            .map_err(|e| IlmError::CoordinatorError(e.to_string()))?;

        if let Some(ref op) = existing {
            info!(
                phase = %op.phase,
                active_rollovers = self.leader.extra_state_ref().active_rollovers.len(),
                "recovered ILM coordinator from persisted phase"
            );
        }

        Ok(())
    }

    /// Delete the operation state after completion.
    pub async fn delete(&self) -> std::result::Result<bool, IlmError> {
        self.leader
            .delete()
            .await
            .map_err(|e| IlmError::CoordinatorError(e.to_string()))
    }

    /// Update the last check time and persist.
    pub async fn update_check_time(&mut self) -> std::result::Result<(), IlmError> {
        self.leader.extra_state().last_check_ms = millis_now();
        self.leader
            .persist_phase(self.leader.phase().to_string())
            .await
            .map_err(|e| IlmError::CoordinatorError(e.to_string()))
    }

    /// Get the active rollover for a policy.
    pub fn active_rollover(&self, policy_name: &str) -> Option<RolloverState> {
        self.leader
            .extra_state_ref()
            .active_rollovers
            .get(policy_name)
            .cloned()
    }

    /// Get all active rollovers.
    pub fn active_rollovers(&self) -> HashMap<String, RolloverState> {
        self.leader.extra_state_ref().active_rollovers.clone()
    }
}

/// ILM error types.
#[derive(Debug, thiserror::Error)]
pub enum IlmError {
    #[error("policy not found: {0}")]
    PolicyNotFound(String),
    #[error("rollover failed: {0}")]
    RolloverFailed(String),
    #[error("alias error: {0}")]
    AliasError(String),
    #[error("safety lock violation: index is too new to delete")]
    SafetyLockViolation,
    #[error("coordinator error: {0}")]
    CoordinatorError(String),
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
    fn test_ilm_config_default() {
        let config = IlmConfig::default();
        assert!(config.enabled);
        assert_eq!(config.check_interval_s, 3600);
        assert_eq!(config.safety_lock_older_than_days, 7);
    }

    #[test]
    fn test_format_index_name() {
        let pattern = "logs-{YYYY-MM-DD}";
        let timestamp = 1704067200000; // 2024-01-01 00:00:00 UTC
        let result = IlmManager::format_index_name(pattern, timestamp);
        assert_eq!(result, "logs-2024-01-01");
    }

    #[test]
    fn test_rollover_phase_serialization() {
        let phase = RolloverPhase::Creating;
        let json = serde_json::to_string(&phase).unwrap();
        assert_eq!(json, "\"Creating\"");
    }

    #[tokio::test]
    async fn test_register_policy() {
        let config = IlmConfig::default();
        let manager = IlmManager::new(config);

        let policy = RolloverPolicy {
            name: "logs-ilm".into(),
            write_alias: "logs".into(),
            read_alias: "logs-search".into(),
            pattern: "logs-{YYYY-MM-DD}".into(),
            triggers: RolloverTriggers {
                max_docs: 10_000_000,
                max_age: "7d".into(),
                max_size_gb: 50,
            },
            retention: RetentionPolicy { keep_indexes: 30 },
            index_template: IndexTemplate {
                primary_key: "event_id".into(),
                settings_ref: "logs-settings".into(),
            },
            enabled: true,
        };

        assert!(manager.register_policy(policy).await.is_ok());
        let policies = manager.policies().await;
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].name, "logs-ilm");
    }

    #[tokio::test]
    async fn test_unregister_policy() {
        let config = IlmConfig::default();
        let manager = IlmManager::new(config);

        let policy = RolloverPolicy {
            name: "test-policy".into(),
            write_alias: "test".into(),
            read_alias: "test-search".into(),
            pattern: "test-{YYYY-MM-DD}".into(),
            triggers: RolloverTriggers {
                max_docs: 1000,
                max_age: "1d".into(),
                max_size_gb: 10,
            },
            retention: RetentionPolicy { keep_indexes: 7 },
            index_template: IndexTemplate {
                primary_key: "id".into(),
                settings_ref: "default".into(),
            },
            enabled: true,
        };

        manager.register_policy(policy).await.unwrap();
        assert_eq!(manager.policies().await.len(), 1);

        manager.unregister_policy("test-policy").await.unwrap();
        assert_eq!(manager.policies().await.len(), 0);
    }

    #[tokio::test]
    async fn test_trigger_rollover() {
        let config = IlmConfig::default();
        let manager = IlmManager::new(config);

        let policy = RolloverPolicy {
            name: "test-rollover".into(),
            write_alias: "logs".into(),
            read_alias: "logs-search".into(),
            pattern: "logs-{YYYY-MM-DD}".into(),
            triggers: RolloverTriggers {
                max_docs: 1000,
                max_age: "1d".into(),
                max_size_gb: 10,
            },
            retention: RetentionPolicy { keep_indexes: 7 },
            index_template: IndexTemplate {
                primary_key: "id".into(),
                settings_ref: "default".into(),
            },
            enabled: true,
        };

        manager.register_policy(policy).await.unwrap();
        assert!(manager.trigger_rollover("test-rollover").await.is_ok());

        let rollover = manager.active_rollover("test-rollover").await;
        assert!(rollover.is_some());
        assert_eq!(rollover.unwrap().phase, RolloverPhase::Creating);
    }

    #[test]
    fn test_ilm_error_policy_not_found() {
        let err = IlmError::PolicyNotFound("missing".into());
        assert!(err.to_string().contains("policy not found"));
    }

    #[test]
    fn test_ilm_error_safety_lock_violation() {
        let err = IlmError::SafetyLockViolation;
        assert!(err.to_string().contains("safety lock violation"));
    }
}
