//! Task store: unified persistence layer for Miroir (plan §4).
//!
//! This module provides a trait-based abstraction over two backends:
//! - SQLite: single-replica, file-based persistence
//! - Redis: multi-replica, distributed persistence
//!
//! Every table in plan §4 is represented here, enabling cross-cutting features
//! like §13 advanced capabilities and §14 HA mode.

mod error;
mod redis;
mod schema;
mod sqlite;

#[cfg(test)]
mod sqlite_tests;

pub use error::{Result, TaskStoreError};
pub use redis::RedisTaskStore;
pub use schema::*;
pub use sqlite::SqliteTaskStore;

use crate::config::TaskStoreConfig;
use std::sync::Arc;

/// Create a task store based on the provided configuration.
pub async fn create_task_store(config: &TaskStoreConfig) -> Result<Arc<dyn TaskStore>> {
    match config.backend.as_str() {
        "sqlite" => {
            let store = SqliteTaskStore::new(&config.path).await?;
            Ok(Arc::new(store))
        }
        "redis" => {
            let store = RedisTaskStore::new(&config.url).await?;
            Ok(Arc::new(store))
        }
        other => Err(TaskStoreError::InvalidBackend(other.to_string())),
    }
}

/// Unified task store trait.
///
/// All persistence operations flow through this trait, enabling backend
/// switching at runtime via `task_store.backend`.
#[async_trait::async_trait]
pub trait TaskStore: Send + Sync {
    // --- Schema management ---

    /// Initialize the schema (idempotent).
    async fn initialize(&self) -> Result<()>;

    /// Get the current schema version.
    async fn schema_version(&self) -> Result<i64>;

    // --- Tasks table (plan §4 table 1) ---

    /// Insert a new task.
    async fn task_insert(&self, task: &Task) -> Result<()>;

    /// Get a task by Miroir ID.
    async fn task_get(&self, miroir_id: &str) -> Result<Option<Task>>;

    /// Update task status.
    async fn task_update_status(&self, miroir_id: &str, status: TaskStatus) -> Result<()>;

    /// Update a node task within a Miroir task.
    async fn task_update_node(&self, miroir_id: &str, node_id: &str, task_uid: u64) -> Result<()>;

    /// List tasks with optional filtering.
    async fn task_list(&self, filter: &TaskFilter) -> Result<Vec<Task>>;

    // --- Node settings version (plan §4 table 2) ---

    /// Get the current settings version for a node.
    async fn node_settings_version_get(&self, index: &str, node_id: &str) -> Result<Option<i64>>;

    /// Set the settings version for a node.
    async fn node_settings_version_set(
        &self,
        index: &str,
        node_id: &str,
        version: i64,
    ) -> Result<()>;

    // --- Aliases (plan §4 table 3) ---

    /// Insert or update an alias.
    async fn alias_upsert(&self, alias: &Alias) -> Result<()>;

    /// Get an alias by name.
    async fn alias_get(&self, name: &str) -> Result<Option<Alias>>;

    /// Delete an alias.
    async fn alias_delete(&self, name: &str) -> Result<()>;

    /// List all aliases.
    async fn alias_list(&self) -> Result<Vec<Alias>>;

    // --- Sessions (plan §4 table 4) ---

    /// Insert or update a session.
    async fn session_upsert(&self, session: &Session) -> Result<()>;

    /// Get a session by ID.
    async fn session_get(&self, session_id: &str) -> Result<Option<Session>>;

    /// Delete a session.
    async fn session_delete(&self, session_id: &str) -> Result<()>;

    /// Delete all sessions for a given index.
    async fn session_delete_by_index(&self, index: &str) -> Result<()>;

    // --- Idempotency cache (plan §4 table 5) ---

    /// Check if a request key has been processed.
    async fn idempotency_check(&self, key: &str) -> Result<Option<IdempotencyEntry>>;

    /// Record a request key as processed.
    async fn idempotency_record(&self, entry: &IdempotencyEntry) -> Result<()>;

    /// Delete old idempotency entries.
    async fn idempotency_prune(&self, before_ts: u64) -> Result<u64>;

    // --- Jobs (plan §4 table 6) ---

    /// Enqueue a background job.
    async fn job_enqueue(&self, job: &Job) -> Result<()>;

    /// Dequeue a job for processing.
    async fn job_dequeue(&self, worker_id: &str) -> Result<Option<Job>>;

    /// Update job status.
    async fn job_update_status(
        &self,
        job_id: &str,
        status: JobState,
        result: Option<&str>,
    ) -> Result<()>;

    /// Get a job by ID.
    async fn job_get(&self, job_id: &str) -> Result<Option<Job>>;

    /// List jobs by status.
    async fn job_list(&self, status: Option<JobState>, limit: usize) -> Result<Vec<Job>>;

    // --- Leader lease (plan §4 table 7) ---

    /// Acquire or renew a leader lease.
    async fn leader_lease_acquire(&self, lease: &LeaderLease) -> Result<bool>;

    /// Release a leader lease.
    async fn leader_lease_release(&self, lease_id: &str) -> Result<()>;

    /// Get the current leader lease.
    async fn leader_lease_get(&self) -> Result<Option<LeaderLease>>;

    // --- Canaries (plan §4 table 8) ---

    /// Insert or update a canary definition.
    async fn canary_upsert(&self, canary: &Canary) -> Result<()>;

    /// Get a canary by name.
    async fn canary_get(&self, name: &str) -> Result<Option<Canary>>;

    /// Delete a canary.
    async fn canary_delete(&self, name: &str) -> Result<()>;

    /// List all canaries.
    async fn canary_list(&self) -> Result<Vec<Canary>>;

    // --- Canary runs (plan §4 table 9) ---

    /// Record a canary run.
    async fn canary_run_insert(&self, run: &CanaryRun) -> Result<()>;

    /// Get canary runs for a canary.
    async fn canary_run_list(&self, canary_name: &str, limit: usize) -> Result<Vec<CanaryRun>>;

    /// Delete old canary runs.
    async fn canary_run_prune(&self, before_ts: u64) -> Result<u64>;

    // --- CDC cursors (plan §4 table 10) ---

    /// Get the current CDC cursor for a sink/index pair.
    async fn cdc_cursor_get(&self, sink: &str, index: &str) -> Result<Option<CdcCursor>>;

    /// Set the CDC cursor for a sink/index pair.
    async fn cdc_cursor_set(&self, cursor: &CdcCursor) -> Result<()>;

    /// List all CDC cursors for a sink.
    async fn cdc_cursor_list(&self, sink: &str) -> Result<Vec<CdcCursor>>;

    // --- Tenant map (plan §4 table 11) ---

    /// Insert or update a tenant mapping.
    async fn tenant_upsert(&self, tenant: &Tenant) -> Result<()>;

    /// Get a tenant by API key.
    async fn tenant_get(&self, api_key: &str) -> Result<Option<Tenant>>;

    /// Delete a tenant.
    async fn tenant_delete(&self, api_key: &str) -> Result<()>;

    /// List all tenants.
    async fn tenant_list(&self) -> Result<Vec<Tenant>>;

    // --- Rollover policies (plan §4 table 12) ---

    /// Insert or update a rollover policy.
    async fn rollover_policy_upsert(&self, policy: &RolloverPolicy) -> Result<()>;

    /// Get a rollover policy by name.
    async fn rollover_policy_get(&self, name: &str) -> Result<Option<RolloverPolicy>>;

    /// Delete a rollover policy.
    async fn rollover_policy_delete(&self, name: &str) -> Result<()>;

    /// List all rollover policies.
    async fn rollover_policy_list(&self) -> Result<Vec<RolloverPolicy>>;

    // --- Search UI config (plan §4 table 13) ---

    /// Insert or update a search UI config.
    async fn search_ui_config_upsert(&self, config: &SearchUiConfig) -> Result<()>;

    /// Get a search UI config by index.
    async fn search_ui_config_get(&self, index: &str) -> Result<Option<SearchUiConfig>>;

    /// Delete a search UI config.
    async fn search_ui_config_delete(&self, index: &str) -> Result<()>;

    /// List all search UI configs.
    async fn search_ui_config_list(&self) -> Result<Vec<SearchUiConfig>>;

    // --- Admin sessions (plan §4 table 14) ---

    /// Insert or update an admin session.
    async fn admin_session_upsert(&self, session: &AdminSession) -> Result<()>;

    /// Get an admin session by ID.
    async fn admin_session_get(&self, session_id: &str) -> Result<Option<AdminSession>>;

    /// Delete an admin session.
    async fn admin_session_delete(&self, session_id: &str) -> Result<()>;

    /// Mark a session as revoked.
    async fn admin_session_revoke(&self, session_id: &str) -> Result<()>;

    /// Check if a session is revoked.
    async fn admin_session_is_revoked(&self, session_id: &str) -> Result<bool>;

    // --- Redis-specific rate limit keys (HA mode only) ---

    /// Increment a rate limit counter (Redis-only).
    async fn ratelimit_increment(&self, key: &str, window_s: u64, limit: u64)
        -> Result<(u64, u64)>;

    /// Set a rate limit backoff (Redis-only).
    async fn ratelimit_set_backoff(&self, key: &str, duration_s: u64) -> Result<()>;

    /// Check a rate limit backoff (Redis-only).
    async fn ratelimit_check_backoff(&self, key: &str) -> Result<Option<u64>>;

    // --- Redis-specific CDC overflow (HA mode only) ---

    /// Check if CDC overflow buffer exists (Redis-only).
    async fn cdc_overflow_check(&self, sink: &str) -> Result<bool>;

    /// Get CDC overflow buffer size (Redis-only).
    async fn cdc_overflow_size(&self, sink: &str) -> Result<u64>;

    /// Append to CDC overflow buffer (Redis-only).
    async fn cdc_overflow_append(&self, sink: &str, data: &[u8]) -> Result<()>;

    /// Clear CDC overflow buffer (Redis-only).
    async fn cdc_overflow_clear(&self, sink: &str) -> Result<()>;

    // --- Redis-specific scoped key rotation (HA mode only) ---

    /// Set a scoped key for an index (Redis-only).
    async fn scoped_key_set(&self, index: &str, key: &str, expires_at: u64) -> Result<()>;

    /// Get the current scoped key for an index (Redis-only).
    async fn scoped_key_get(&self, index: &str) -> Result<Option<String>>;

    /// Mark a scoped key as observed by a pod (Redis-only).
    async fn scoped_key_observe(&self, pod: &str, index: &str, key: &str) -> Result<()>;

    /// Check if a pod has observed a scoped key (Redis-only).
    async fn scoped_key_has_observed(&self, pod: &str, index: &str, key: &str) -> Result<bool>;

    // --- Health check ---

    /// Check if the store is healthy.
    async fn health_check(&self) -> Result<bool>;
}
