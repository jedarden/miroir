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

use crate::leader_election::LeaderElection;
use crate::mode_b_coordinator::ModeBOpLeader;
use crate::task_store::TaskStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

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

/// Rollover phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RolloverPhase {
    Creating,
    FlippingAlias,
    UpdatingReadAlias,
    CleaningOld,
    Complete,
    Failed,
}

/// ILM manager — handles index lifecycle for time-series data.
pub struct IlmManager {
    /// Configuration.
    config: IlmConfig,
    /// Shared state.
    state: Arc<RwLock<IlmState>>,
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

        if config.enabled {
            // Spawn background evaluator
            let state_clone = state.clone();
            let config_clone = config.clone();
            tokio::spawn(async move {
                Self::background_evaluator(state_clone, config_clone).await;
            });
        }

        Self { config, state }
    }

    /// Register a rollover policy.
    pub async fn register_policy(&self, policy: RolloverPolicy) -> Result<(), IlmError> {
        let mut state = self.state.write().await;
        state.policies.push(policy);
        Ok(())
    }

    /// Unregister a policy.
    pub async fn unregister_policy(&self, name: &str) -> Result<(), IlmError> {
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
    pub async fn trigger_rollover(&self, policy_name: &str) -> Result<(), IlmError> {
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
    ) -> Result<(), IlmError> {
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

impl IlmCoordinator {
    /// Create a new ILM coordinator.
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
    pub async fn try_acquire_leadership(&mut self) -> Result<(), IlmError> {
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
    pub async fn renew_leadership(&mut self) -> Result<bool, IlmError> {
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
    pub async fn advance_phase(&mut self, new_phase: &str) -> Result<(), IlmError> {
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
    ) -> Result<(), IlmError> {
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
    pub async fn complete_rollover(&mut self, policy_name: &str) -> Result<(), IlmError> {
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
    pub async fn fail(&mut self, error: String) -> Result<(), IlmError> {
        self.leader
            .fail(error)
            .await
            .map_err(|e| IlmError::CoordinatorError(e.to_string()))
    }

    /// Mark the operation as completed and step down from leadership.
    pub async fn complete(&mut self) -> Result<(), IlmError> {
        self.leader
            .complete()
            .await
            .map_err(|e| IlmError::CoordinatorError(e.to_string()))
    }

    /// Recover the operation state from the task store.
    ///
    /// Called by a new leader to read the persisted phase state and resume
    /// from the last committed phase boundary.
    pub async fn recover(&mut self) -> Result<(), IlmError> {
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
    pub async fn delete(&self) -> Result<bool, IlmError> {
        self.leader
            .delete()
            .await
            .map_err(|e| IlmError::CoordinatorError(e.to_string()))
    }

    /// Update the last check time and persist.
    pub async fn update_check_time(&mut self) -> Result<(), IlmError> {
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
