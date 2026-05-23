//! Leader election service for Mode B background jobs (plan §14.5).
//!
//! Provides a generic leader election mechanism using the TaskStore's
//! leader_lease table (Table 7). Each Mode B operation acquires a scoped
//! lease (e.g., "reshard:my-index", "rebalance", "ilm") and renews it
//! periodically. If renewal fails, the leader steps down and a new pod
//! acquires the lease.
//!
//! ## Lease Scopes (plan §14.6)
//!
//! - `reshard:<index>` - Per-index shard migration coordinator
//! - `rebalance:<index>` or `rebalance` - Rebalancer worker
//! - `alias_flip:<name>` - Alias flip serializer
//! - `settings_broadcast:<index>` - Two-phase settings broadcast
//! - `ilm` - ILM evaluator
//! - `search_ui_key_rotation:<index>` - Scoped-key rotation
//!
//! ## Leader Loss Recovery
//!
//! All Mode B operations are designed to be idempotent and safe to resume
//! at phase boundaries. When a new leader acquires a lease, it reads the
//! persisted phase state from the task store and resumes from the last
//! committed phase.

use crate::config::LeaderElectionConfig;
use crate::task_store::{TaskStore, LeaderLeaseRow};
use crate::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Callback type for recording leader election metrics.
///
/// Called with:
/// - metric name (e.g., "miroir_leader")
/// - label map (e.g., {"scope": "reshard:my-index"})
/// - value (1.0 for leader, 0.0 for follower)
pub type LeaderElectionMetricsCallback = Arc<dyn Fn(&str, &HashMap<String, String>, f64) + Send + Sync>;

/// Leader election metrics for Prometheus emission.
#[derive(Debug, Clone, Default)]
pub struct LeaderElectionMetrics {
    /// Per-scope leader status (1 if this pod is leader, 0 otherwise).
    pub leader_status: HashMap<String, f64>,
    /// Timestamp when this pod became leader for each scope.
    pub leader_since: HashMap<String, Instant>,
    /// Number of lease acquisitions for each scope.
    pub acquisitions_total: HashMap<String, u64>,
    /// Number of lease renewals for each scope.
    pub renewals_total: HashMap<String, u64>,
    /// Number of lease losses for each scope.
    pub losses_total: HashMap<String, u64>,
}

impl LeaderElectionMetrics {
    /// Set leader status for a scope.
    pub fn set_leader_status(&mut self, scope: &str, is_leader: bool) {
        let value = if is_leader { 1.0 } else { 0.0 };
        self.leader_status.insert(scope.to_string(), value);

        if is_leader {
            self.leader_since
                .entry(scope.to_string())
                .or_insert_with(Instant::now);
        } else {
            self.leader_since.remove(scope);
        }
    }

    /// Record a lease acquisition for a scope.
    pub fn record_acquisition(&mut self, scope: &str) {
        *self.acquisitions_total.entry(scope.to_string()).or_insert(0) += 1;
    }

    /// Record a lease renewal for a scope.
    pub fn record_renewal(&mut self, scope: &str) {
        *self.renewals_total.entry(scope.to_string()).or_insert(0) += 1;
    }

    /// Record a lease loss for a scope.
    pub fn record_loss(&mut self, scope: &str) {
        *self.losses_total.entry(scope.to_string()).or_insert(0) += 1;
        self.leader_status.remove(scope);
        self.leader_since.remove(scope);
    }

    /// Get the current leader status for a scope.
    pub fn is_leader(&self, scope: &str) -> bool {
        self.leader_status
            .get(scope)
            .map(|&v| v == 1.0)
            .unwrap_or(false)
    }

    /// Get the time since this pod became leader for a scope.
    pub fn leader_duration(&self, scope: &str) -> Option<Duration> {
        self.leader_since.get(scope).map(|since| since.elapsed())
    }

    /// Emit metrics via callback.
    pub fn emit_metrics<F>(&self, mut callback: F)
    where
        F: FnMut(&str, &HashMap<String, String>, f64),
    {
        // Emit leader status for each scope
        for (scope, value) in &self.leader_status {
            let mut labels = HashMap::new();
            labels.insert("scope".to_string(), scope.clone());
            callback("miroir_leader", &labels, *value);
        }

        // Emit acquisition counts
        for (scope, count) in &self.acquisitions_total {
            let mut labels = HashMap::new();
            labels.insert("scope".to_string(), scope.clone());
            callback(
                "miroir_leader_acquisitions_total",
                &labels,
                *count as f64,
            );
        }

        // Emit renewal counts
        for (scope, count) in &self.renewals_total {
            let mut labels = HashMap::new();
            labels.insert("scope".to_string(), scope.clone());
            callback("miroir_leader_renewals_total", &labels, *count as f64);
        }

        // Emit loss counts
        for (scope, count) in &self.losses_total {
            let mut labels = HashMap::new();
            labels.insert("scope".to_string(), scope.clone());
            callback("miroir_leader_losses_total", &labels, *count as f64);
        }
    }
}

/// Default leader lease TTL in seconds (configurable).
const DEFAULT_LEASE_TTL_SECS: u64 = 10;

/// Default interval for lease renewal in seconds (configurable).
const DEFAULT_RENEW_INTERVAL_SECS: u64 = 3;

/// Leader election service.
///
/// Manages lease acquisition, renewal, and step-down for a specific scope.
/// Multiple leaders can run concurrently with different scopes.
#[derive(Clone)]
pub struct LeaderElection {
    /// Task store for lease operations.
    task_store: Arc<dyn TaskStore>,
    /// Pod identity (from POD_NAME env var or hostname).
    pod_id: String,
    /// Lease configuration.
    config: LeaderElectionConfig,
    /// Active leases (scope -> lease state).
    active_leases: Arc<RwLock<std::collections::HashMap<String, LeaseState>>>,
    /// Metrics for leader election.
    metrics: Arc<RwLock<LeaderElectionMetrics>>,
    /// Callback for recording Prometheus metrics.
    metrics_callback: Option<LeaderElectionMetricsCallback>,
}

/// State of an active lease.
#[derive(Debug, Clone)]
struct LeaseState {
    /// Scope of the lease.
    scope: String,
    /// When this lease was acquired.
    acquired_at: Instant,
    /// Last successful renewal time.
    last_renewal: Instant,
    /// Lease expiration time (milliseconds since Unix epoch).
    expires_at: i64,
}

impl LeaderElection {
    /// Create a new leader election service.
    pub fn new(
        task_store: Arc<dyn TaskStore>,
        pod_id: String,
        config: LeaderElectionConfig,
    ) -> Self {
        Self {
            task_store,
            pod_id,
            config,
            active_leases: Arc::new(RwLock::new(std::collections::HashMap::new())),
            metrics: Arc::new(RwLock::new(LeaderElectionMetrics::default())),
            metrics_callback: None,
        }
    }

    /// Set the metrics callback for Prometheus emission.
    pub fn with_metrics_callback(mut self, callback: LeaderElectionMetricsCallback) -> Self {
        self.metrics_callback = Some(callback);
        self
    }

    /// Get the pod ID for this leader election instance.
    pub fn pod_id(&self) -> &str {
        &self.pod_id
    }

    /// Get a reference to the metrics.
    pub async fn metrics(&self) -> LeaderElectionMetrics {
        self.metrics.read().await.clone()
    }

    /// Try to acquire a leader lease for the given scope.
    ///
    /// Returns `Ok(true)` if acquired, `Ok(false)` if already held by another pod,
    /// or `Err` if the operation fails.
    ///
    /// # Arguments
    ///
    /// * `scope` - Lease scope (e.g., "reshard:my-index", "rebalance")
    ///
    /// # Lease Semantics
    ///
    /// - If no lease exists or the lease is expired, we acquire it
    /// - If we already hold the lease, we extend it
    /// - If another pod holds the lease and it's not expired, we fail
    pub async fn try_acquire_async(&self, scope: &str) -> Result<bool> {
        let now_ms = now_ms();
        let ttl_secs = self.config.lease_ttl_s;
        let expires_at = now_ms + (ttl_secs * 1000) as i64;

        let acquired = self
            .task_store
            .try_acquire_leader_lease(scope, &self.pod_id, expires_at, now_ms)?;

        if acquired {
            debug!(scope, pod_id = %self.pod_id, "acquired leader lease");

            // Track the active lease
            let state = LeaseState {
                scope: scope.to_string(),
                acquired_at: Instant::now(),
                last_renewal: Instant::now(),
                expires_at,
            };
            let mut leases = self.active_leases.write().await;
            leases.insert(scope.to_string(), state);

            // Record metrics
            let mut metrics = self.metrics.write().await;
            metrics.set_leader_status(scope, true);
            metrics.record_acquisition(scope);
            self.emit_metrics(scope, 1.0);
        } else {
            debug!(scope, pod_id = %self.pod_id, "failed to acquire leader lease (held by another pod)");

            // Record metrics (not leader)
            let mut metrics = self.metrics.write().await;
            metrics.set_leader_status(scope, false);
            self.emit_metrics(scope, 0.0);
        }

        Ok(acquired)
    }

    /// Renew a leader lease we already hold.
    ///
    /// Returns `Ok(true)` if renewed successfully, `Ok(false)` if we no longer
    /// hold the lease (another pod stole it), or `Err` if the operation fails.
    ///
    /// # Arguments
    ///
    /// * `scope` - Lease scope to renew
    pub async fn renew_async(&self, scope: &str) -> Result<bool> {
        let now_ms = now_ms();
        let ttl_secs = self.config.lease_ttl_s;
        let expires_at = now_ms + (ttl_secs * 1000) as i64;

        let renewed = self
            .task_store
            .renew_leader_lease(scope, &self.pod_id, expires_at)?;

        if renewed {
            debug!(scope, pod_id = %self.pod_id, "renewed leader lease");

            // Update the active lease state
            let mut leases = self.active_leases.write().await;
            if let Some(state) = leases.get_mut(scope) {
                state.last_renewal = Instant::now();
                state.expires_at = expires_at;
            }

            // Record metrics
            let mut metrics = self.metrics.write().await;
            metrics.record_renewal(scope);
        } else {
            warn!(scope, pod_id = %self.pod_id, "failed to renew leader lease (lost to another pod)");

            // Remove from active leases
            let mut leases = self.active_leases.write().await;
            leases.remove(scope);

            // Record metrics (lost leadership)
            let mut metrics = self.metrics.write().await;
            metrics.record_loss(scope);
            self.emit_metrics(scope, 0.0);
        }

        Ok(renewed)
    }

    /// Step down from leadership for a scope.
    ///
    /// Deletes the lease row, allowing another pod to acquire it immediately.
    /// Returns `Ok(true)` if we held the lease and stepped down, `Ok(false)`
    /// if we didn't hold it, or `Err` if the operation fails.
    ///
    /// # Arguments
    ///
    /// * `scope` - Lease scope to step down from
    pub async fn step_down_async(&self, scope: &str) -> Result<bool> {
        let now_ms = now_ms();
        // Check if we hold the lease (and it's not expired)
        let current = self.task_store.get_leader_lease(scope)?;
        let held = current.as_ref()
            .map(|l| &l.holder == &self.pod_id && l.expires_at > now_ms)
            .unwrap_or(false);

        if held {
            // To step down, we set the expiration to the past
            // This allows another pod to acquire the lease immediately
            let past_expiration = now_ms - 1000;
            let _ = self
                .task_store
                .renew_leader_lease(scope, &self.pod_id, past_expiration);

            info!(scope, pod_id = %self.pod_id, "stepped down from leadership");

            // Record metrics (voluntarily stepping down)
            let mut metrics = self.metrics.write().await;
            metrics.set_leader_status(scope, false);
            self.emit_metrics(scope, 0.0);
        }

        // Remove from active leases regardless
        let mut leases = self.active_leases.write().await;
        leases.remove(scope);

        Ok(held)
    }

    /// Check if we currently hold the lease for a scope.
    ///
    /// Returns `true` if we hold the lease and it hasn't expired.
    ///
    /// # Arguments
    ///
    /// * `scope` - Lease scope to check
    pub fn is_leader(&self, scope: &str) -> bool {
        // Use try_read to avoid blocking in async contexts
        if let Ok(leases) = self.active_leases.try_read() {
            if let Some(state) = leases.get(scope) {
                // Check if the lease is still valid based on our local state
                let now_ms = now_ms();
                return now_ms < state.expires_at;
            }
        }
        false
    }

    /// Get the current lease holder for a scope.
    ///
    /// Returns `None` if no lease exists, or `Some(holder)` with the pod ID
    /// of the current lease holder.
    ///
    /// # Arguments
    ///
    /// * `scope` - Lease scope to query
    pub fn get_holder(&self, scope: &str) -> Result<Option<String>> {
        let lease = self.task_store.get_leader_lease(scope)?;
        Ok(lease.map(|l| l.holder))
    }

    // --- Blocking wrappers for backward compatibility ---

    /// Blocking wrapper for try_acquire_async.
    pub fn try_acquire(&self, scope: &str) -> Result<bool> {
        let handle = Handle::try_current()
            .map_err(|_| crate::MiroirError::InvalidState("no tokio runtime".to_string()))?;
        handle.block_on(self.try_acquire_async(scope))
    }

    /// Blocking wrapper for renew_async.
    pub fn renew(&self, scope: &str) -> Result<bool> {
        let handle = Handle::try_current()
            .map_err(|_| crate::MiroirError::InvalidState("no tokio runtime".to_string()))?;
        handle.block_on(self.renew_async(scope))
    }

    /// Blocking wrapper for step_down_async.
    pub fn step_down(&self, scope: &str) -> Result<bool> {
        let handle = Handle::try_current()
            .map_err(|_| crate::MiroirError::InvalidState("no tokio runtime".to_string()))?;
        handle.block_on(self.step_down_async(scope))
    }

    /// Run a leader election loop with a callback.
    ///
    /// This is the main entry point for Mode B operations. It:
    /// 1. Tries to acquire the lease
    /// 2. If acquired, runs the callback in a loop
    /// 3. Renews the lease periodically
    /// 4. If lease is lost, exits the callback and retries acquisition
    ///
    /// The callback should return `Ok(true)` to continue running, or `Ok(false)`
    /// to stop the leader loop.
    ///
    /// # Arguments
    ///
    /// * `scope` - Lease scope
    /// * `callback` - Function to call while holding the lease
    ///
    /// # Example
    ///
    /// ```ignore
    /// leader_election.run("reshard:my-index", |is_leader| async move {
    ///     if is_leader {
    ///         // Run the reshard coordinator
    ///         Ok(true) // Continue running
    ///     } else {
    ///         // Not the leader - wait
    ///         tokio::time::sleep(Duration::from_secs(1)).await;
    ///         Ok(true) // Continue retrying
    ///     }
    /// }).await?;
    /// ```
    pub async fn run<F, Fut>(&self, scope: &str, callback: F) -> Result<()>
    where
        F: Fn(bool) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<bool>> + Send,
    {
        let scope = scope.to_string();
        let renew_interval = Duration::from_secs(self.config.renew_interval_s);

        loop {
            // Try to acquire the lease
            let is_leader = tokio::task::spawn_blocking({
                let leader_election = self.clone();
                let scope = scope.clone();
                move || leader_election.try_acquire(&scope)
            })
            .await
            .unwrap_or(Ok(false))?;

            // Run the callback
            let continue_running = callback(is_leader).await?;

            if !continue_running {
                debug!(scope = %scope, "leader loop stopped by callback");
                return Ok(());
            }

            // If we're the leader, renew the lease periodically
            if is_leader {
                let renewed = tokio::task::spawn_blocking({
                    let leader_election = self.clone();
                    let scope = scope.clone();
                    move || leader_election.renew(&scope)
                })
                .await
                .unwrap_or(Ok(false))?;

                if !renewed {
                    warn!(scope = %scope, "lost leader lease during renewal");
                }
            }

            // Wait before the next iteration
            tokio::time::sleep(renew_interval).await;
        }
    }

    /// Start a background leader election task.
    ///
    /// This spawns a Tokio task that runs the leader election loop in the background.
    /// The task handle is returned, allowing the caller to manage the task lifecycle.
    ///
    /// # Arguments
    ///
    /// * `scope` - Lease scope
    /// * `callback` - Function to call while holding the lease
    pub fn spawn<F, Fut>(&self, scope: &str, callback: F) -> tokio::task::JoinHandle<Result<()>>
    where
        F: Fn(bool) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<bool>> + Send + 'static,
    {
        let leader_election = self.clone();
        let scope = scope.to_string();

        tokio::spawn(async move {
            leader_election.run(&scope, callback).await
        })
    }

    /// Get all active leases.
    ///
    /// Returns a map of scope to lease state for all currently held leases.
    pub async fn active_leases(&self) -> std::collections::HashMap<String, LeaseState> {
        self.active_leases.read().await.clone()
    }

    /// Emit metrics via callback.
    fn emit_metrics(&self, scope: &str, value: f64) {
        if let Some(ref callback) = self.metrics_callback {
            let mut labels = std::collections::HashMap::new();
            labels.insert("scope".to_string(), scope.to_string());
            labels.insert("pod_id".to_string(), self.pod_id.clone());
            callback("miroir_leader", &labels, value);
        }
    }

    /// Step down from all active leases.
    ///
    /// Useful for graceful shutdown.
    pub async fn step_down_all(&self) -> Result<()> {
        let scopes: Vec<String> = self
            .active_leases
            .read()
            .await
            .keys()
            .cloned()
            .collect();

        for scope in scopes {
            self.step_down_async(&scope).await?;
        }

        Ok(())
    }

    // --- Mode B operation state persistence (plan §14.5) ---

    /// Persist Mode B operation state for leader recovery.
    ///
    /// This should be called after each phase boundary so that a new leader
    /// can resume from the last committed phase.
    ///
    /// # Arguments
    ///
    /// * `operation` - The Mode B operation state to persist
    pub fn persist_mode_b_operation(&self, operation: &crate::task_store::ModeBOperation) -> Result<()> {
        self.task_store.upsert_mode_b_operation(operation)?;
        Ok(())
    }

    /// Recover Mode B operation state for leader resume.
    ///
    /// Called by a new leader to read the persisted phase state and resume
    /// from the last committed phase boundary.
    ///
    /// # Arguments
    ///
    /// * `scope` - The operation scope (e.g., "reshard:my-index")
    pub fn recover_mode_b_operation(&self, scope: &str) -> Result<Option<crate::task_store::ModeBOperation>> {
        self.task_store.get_mode_b_operation_by_scope(scope)
    }

    /// List Mode B operations by filter.
    ///
    /// Useful for recovery and cleanup.
    ///
    /// # Arguments
    ///
    /// * `filter` - Filter criteria for listing operations
    pub fn list_mode_b_operations(&self, filter: &crate::task_store::ModeBOperationFilter) -> Result<Vec<crate::task_store::ModeBOperation>> {
        self.task_store.list_mode_b_operations(filter)
    }

    /// Delete a Mode B operation state.
    ///
    /// Called after an operation completes or is explicitly cancelled.
    ///
    /// # Arguments
    ///
    /// * `operation_id` - The operation ID to delete
    pub fn delete_mode_b_operation(&self, operation_id: &str) -> Result<bool> {
        self.task_store.delete_mode_b_operation(operation_id)
    }

    /// Prune old completed Mode B operations.
    ///
    /// # Arguments
    ///
    /// * `cutoff_ms` - Operations with updated_at < cutoff_ms are eligible for pruning
    /// * `batch_size` - Maximum number of operations to delete in one call
    pub fn prune_mode_b_operations(&self, cutoff_ms: i64, batch_size: u32) -> Result<usize> {
        self.task_store.prune_mode_b_operations(cutoff_ms, batch_size)
    }
}

/// Get current time in milliseconds since Unix epoch.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod acceptance_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_store::SqliteTaskStore;

    fn test_leader_election() -> LeaderElection {
        let store = SqliteTaskStore::open_in_memory().unwrap();
        store.migrate().unwrap();

        let config = LeaderElectionConfig {
            enabled: true,
            lease_ttl_s: 10,
            renew_interval_s: 3,
        };

        LeaderElection::new(
            Arc::new(store),
            "pod-1".to_string(),
            config,
        )
    }

    #[tokio::test]
    async fn test_acquire_lease() {
        let leader = test_leader_election();

        // First acquisition should succeed
        assert!(leader.try_acquire_async("test-scope").await.unwrap());
        assert!(leader.is_leader("test-scope"));
    }

    #[tokio::test]
    async fn test_renew_lease() {
        let leader = test_leader_election();

        // Acquire the lease
        assert!(leader.try_acquire_async("test-scope").await.unwrap());

        // Renew should succeed
        assert!(leader.renew_async("test-scope").await.unwrap());
        assert!(leader.is_leader("test-scope"));
    }

    #[tokio::test]
    async fn test_steal_lease() {
        let store = SqliteTaskStore::open_in_memory().unwrap();
        store.migrate().unwrap();

        let config = LeaderElectionConfig {
            enabled: true,
            lease_ttl_s: 10,
            renew_interval_s: 3,
        };

        let store = Arc::new(store);
        let leader1 = LeaderElection::new(
            store.clone(),
            "pod-1".to_string(),
            config.clone(),
        );

        let leader2 = LeaderElection::new(
            store,
            "pod-2".to_string(),
            config,
        );

        // Leader 1 acquires the lease
        assert!(leader1.try_acquire_async("test-scope").await.unwrap());

        // Leader 2 cannot steal the lease (not expired)
        assert!(!leader2.try_acquire_async("test-scope").await.unwrap());

        // Leader 1 is still the leader
        assert!(leader1.is_leader("test-scope"));
        assert!(!leader2.is_leader("test-scope"));
    }

    #[tokio::test]
    async fn test_expired_lease_can_be_stolen() {
        let store = SqliteTaskStore::open_in_memory().unwrap();
        store.migrate().unwrap();

        let config = LeaderElectionConfig {
            enabled: true,
            lease_ttl_s: 1, // 1 second TTL
            renew_interval_s: 3,
        };

        let store = Arc::new(store);
        let leader1 = LeaderElection::new(
            store.clone(),
            "pod-1".to_string(),
            config.clone(),
        );

        let leader2 = LeaderElection::new(
            store,
            "pod-2".to_string(),
            config,
        );

        // Leader 1 acquires the lease
        assert!(leader1.try_acquire_async("test-scope").await.unwrap());

        // Wait for the lease to expire
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Leader 2 can now steal the expired lease
        assert!(leader2.try_acquire_async("test-scope").await.unwrap());
        assert!(!leader1.is_leader("test-scope"));
        assert!(leader2.is_leader("test-scope"));
    }

    #[tokio::test]
    async fn test_step_down() {
        let leader = test_leader_election();

        // Acquire the lease
        assert!(leader.try_acquire_async("test-scope").await.unwrap());
        assert!(leader.is_leader("test-scope"));

        // Step down
        assert!(leader.step_down_async("test-scope").await.unwrap());
        assert!(!leader.is_leader("test-scope"));

        // Step down again (should return false since we don't hold it)
        assert!(!leader.step_down_async("test-scope").await.unwrap());
    }

    #[tokio::test]
    async fn test_get_holder() {
        let store = SqliteTaskStore::open_in_memory().unwrap();
        store.migrate().unwrap();

        let config = LeaderElectionConfig {
            enabled: true,
            lease_ttl_s: 10,
            renew_interval_s: 3,
        };

        let store = Arc::new(store);
        let leader1 = LeaderElection::new(
            store.clone(),
            "pod-1".to_string(),
            config.clone(),
        );

        let leader2 = LeaderElection::new(
            store,
            "pod-2".to_string(),
            config,
        );

        // No lease holder initially
        assert!(leader1.get_holder("test-scope").unwrap().is_none());

        // Leader 1 acquires the lease
        leader1.try_acquire_async("test-scope").await.unwrap();

        // Check the holder
        assert_eq!(leader1.get_holder("test-scope").unwrap().as_deref(), Some("pod-1"));
        assert_eq!(leader2.get_holder("test-scope").unwrap().as_deref(), Some("pod-1"));
    }

    #[tokio::test]
    async fn test_multiple_scopes() {
        let leader = test_leader_election();

        // Acquire leases for different scopes
        assert!(leader.try_acquire_async("scope-1").await.unwrap());
        assert!(leader.try_acquire_async("scope-2").await.unwrap());
        assert!(leader.try_acquire_async("scope-3").await.unwrap());

        // Check that we're the leader for all scopes
        assert!(leader.is_leader("scope-1"));
        assert!(leader.is_leader("scope-2"));
        assert!(leader.is_leader("scope-3"));

        // Step down from one scope
        assert!(leader.step_down_async("scope-2").await.unwrap());

        // Check leader status
        assert!(leader.is_leader("scope-1"));
        assert!(!leader.is_leader("scope-2"));
        assert!(leader.is_leader("scope-3"));

        // Get active leases
        let active = leader.active_leases().await;
        assert_eq!(active.len(), 2);
        assert!(active.contains_key("scope-1"));
        assert!(active.contains_key("scope-3"));
    }

    #[tokio::test]
    async fn test_step_down_all() {
        let leader = test_leader_election();

        // Acquire leases for multiple scopes
        assert!(leader.try_acquire_async("scope-1").await.unwrap());
        assert!(leader.try_acquire_async("scope-2").await.unwrap());
        assert!(leader.try_acquire_async("scope-3").await.unwrap());

        // Step down from all
        leader.step_down_all().await.unwrap();

        // Check that we're not the leader for any scope
        assert!(!leader.is_leader("scope-1"));
        assert!(!leader.is_leader("scope-2"));
        assert!(!leader.is_leader("scope-3"));
    }
}
