#[cfg(feature = "redis-store")]
mod redis;
mod sqlite;

#[cfg(feature = "redis-store")]
pub use redis::{RedisPool, RedisTaskStore, SearchUiScopedKey};
pub use sqlite::SqliteTaskStore;

use crate::Result;
use std::collections::HashMap;

/// Per-table store operations covering tables 1–14 from plan §4.
pub trait TaskStore: Send + Sync {
    // --- Lifecycle ---

    /// Run idempotent migrations for all tables. Safe to call on every startup.
    fn migrate(&self) -> Result<()>;

    // --- Table 1: tasks ---

    /// Insert a new task row.
    fn insert_task(&self, task: &NewTask) -> Result<()>;

    /// Get a task by miroir_id.
    fn get_task(&self, miroir_id: &str) -> Result<Option<TaskRow>>;

    /// Update a task's status.
    fn update_task_status(&self, miroir_id: &str, status: &str) -> Result<bool>;

    /// Update a node task within a task's node_tasks JSON.
    fn update_node_task(&self, miroir_id: &str, node_id: &str, task_uid: u64) -> Result<bool>;

    /// Set the error field on a task.
    fn set_task_error(&self, miroir_id: &str, error: &str) -> Result<bool>;

    /// List tasks with optional status filter and pagination.
    fn list_tasks(&self, filter: &TaskFilter) -> Result<Vec<TaskRow>>;

    /// Prune terminal tasks older than `cutoff_ms` (created_at < cutoff_ms
    /// AND status IN (succeeded, failed, canceled)). Returns number deleted.
    /// Limited to `batch_size` rows per call.
    fn prune_tasks(&self, cutoff_ms: i64, batch_size: u32) -> Result<usize>;

    /// List terminal tasks older than `cutoff_ms` with pagination (Mode A support).
    fn list_terminal_tasks_batch(
        &self,
        cutoff_ms: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<TaskRow>>;

    /// Delete tasks by miroir_id in a batch (Mode A support).
    fn delete_tasks_batch(&self, miroir_ids: &[&str]) -> Result<usize>;

    /// Count total rows in the tasks table (for the miroir_task_registry_size gauge).
    fn task_count(&self) -> Result<u64>;

    // --- Table 2: node_settings_version ---

    /// Upsert a settings version for (index_uid, node_id).
    fn upsert_node_settings_version(
        &self,
        index_uid: &str,
        node_id: &str,
        version: i64,
        updated_at: i64,
    ) -> Result<()>;

    /// Get the settings version for (index_uid, node_id).
    fn get_node_settings_version(
        &self,
        index_uid: &str,
        node_id: &str,
    ) -> Result<Option<NodeSettingsVersionRow>>;

    // --- Table 3: aliases ---

    /// Create a new alias.
    fn create_alias(&self, alias: &NewAlias) -> Result<()>;

    /// Upsert an alias (create or update). Used by ILM for multi-target alias updates.
    fn upsert_alias(&self, alias: &NewAlias) -> Result<()>;

    /// Get an alias by name.
    fn get_alias(&self, name: &str) -> Result<Option<AliasRow>>;

    /// Flip a single alias to a new current_uid, recording history.
    fn flip_alias(&self, name: &str, new_uid: &str, history_retention: usize) -> Result<bool>;

    /// Delete an alias.
    fn delete_alias(&self, name: &str) -> Result<bool>;

    /// List all aliases.
    fn list_aliases(&self) -> Result<Vec<AliasRow>>;

    // --- Table 4: sessions ---

    /// Create or replace a session.
    fn upsert_session(&self, session: &SessionRow) -> Result<()>;

    /// Get a session by id.
    fn get_session(&self, session_id: &str) -> Result<Option<SessionRow>>;

    /// Delete expired sessions.
    fn delete_expired_sessions(&self, now_ms: i64) -> Result<usize>;

    // --- Table 5: idempotency_cache ---

    /// Insert an idempotency cache entry.
    fn insert_idempotency_entry(&self, entry: &IdempotencyEntry) -> Result<()>;

    /// Look up an idempotency entry by key.
    fn get_idempotency_entry(&self, key: &str) -> Result<Option<IdempotencyEntry>>;

    /// Delete expired entries.
    fn delete_expired_idempotency_entries(&self, now_ms: i64) -> Result<usize>;

    // --- Table 6: jobs ---

    /// Insert a new job.
    fn insert_job(&self, job: &NewJob) -> Result<()>;

    /// Get a job by id.
    fn get_job(&self, id: &str) -> Result<Option<JobRow>>;

    /// Claim a queued job (CAS: only if still queued).
    fn claim_job(&self, id: &str, claimed_by: &str, claim_expires_at: i64) -> Result<bool>;

    /// Update job state and progress.
    fn update_job_progress(&self, id: &str, state: &str, progress: &str) -> Result<bool>;

    /// Renew a job claim (heartbeat).
    fn renew_job_claim(&self, id: &str, claim_expires_at: i64) -> Result<bool>;

    /// List jobs by state.
    fn list_jobs_by_state(&self, state: &str) -> Result<Vec<JobRow>>;

    /// Count jobs by state (for HPA queue depth metric).
    fn count_jobs_by_state(&self, state: &str) -> Result<u64>;

    /// List jobs with expired claims (for reclamation).
    fn list_expired_claims(&self, now_ms: i64) -> Result<Vec<JobRow>>;

    /// List all chunks for a parent job.
    fn list_jobs_by_parent(&self, parent_job_id: &str) -> Result<Vec<JobRow>>;

    /// Reclaim an expired job claim (reset to queued and clear claim fields).
    fn reclaim_job_claim(&self, id: &str, state: &str, progress: &str) -> Result<bool>;

    // --- Table 7: leader_lease ---

    /// Try to acquire a leader lease (CAS: only if expired or held by us).
    /// `now_ms` is the current time for expiry comparison.
    fn try_acquire_leader_lease(
        &self,
        scope: &str,
        holder: &str,
        expires_at: i64,
        now_ms: i64,
    ) -> Result<bool>;

    /// Renew a leader lease we already hold.
    /// `now_ms` is the current time for expiry comparison.
    fn renew_leader_lease(
        &self,
        scope: &str,
        holder: &str,
        expires_at: i64,
        now_ms: i64,
    ) -> Result<bool>;

    /// Get current lease holder for a scope.
    fn get_leader_lease(&self, scope: &str) -> Result<Option<LeaderLeaseRow>>;

    // --- Table 8: canaries ---

    /// Create or update a canary.
    fn upsert_canary(&self, canary: &NewCanary) -> Result<()>;

    /// Get a canary by id.
    fn get_canary(&self, id: &str) -> Result<Option<CanaryRow>>;

    /// List all canaries.
    fn list_canaries(&self) -> Result<Vec<CanaryRow>>;

    /// Delete a canary.
    fn delete_canary(&self, id: &str) -> Result<bool>;

    // --- Table 9: canary_runs ---

    /// Insert a canary run (auto-prunes to run_history_per_canary).
    fn insert_canary_run(&self, run: &NewCanaryRun, run_history_limit: usize) -> Result<()>;

    /// Get runs for a canary, most recent first.
    fn get_canary_runs(&self, canary_id: &str, limit: usize) -> Result<Vec<CanaryRunRow>>;

    // --- Table 10: cdc_cursors ---

    /// Upsert a CDC cursor for (sink_name, index_uid).
    fn upsert_cdc_cursor(&self, cursor: &NewCdcCursor) -> Result<()>;

    /// Get a CDC cursor by (sink_name, index_uid).
    fn get_cdc_cursor(&self, sink_name: &str, index_uid: &str) -> Result<Option<CdcCursorRow>>;

    /// List all CDC cursors for a sink.
    fn list_cdc_cursors(&self, sink_name: &str) -> Result<Vec<CdcCursorRow>>;

    // --- Table 11: tenant_map ---

    /// Insert a tenant mapping.
    fn insert_tenant_mapping(&self, mapping: &NewTenantMapping) -> Result<()>;

    /// Get tenant mapping by API key hash.
    fn get_tenant_mapping(&self, api_key_hash: &[u8]) -> Result<Option<TenantMapRow>>;

    /// Delete a tenant mapping.
    fn delete_tenant_mapping(&self, api_key_hash: &[u8]) -> Result<bool>;

    /// List all tenant mappings.
    fn list_tenant_mappings(&self) -> Result<Vec<TenantMapRow>>;

    // --- Table 12: rollover_policies ---

    /// Create or update a rollover policy.
    fn upsert_rollover_policy(&self, policy: &NewRolloverPolicy) -> Result<()>;

    /// Get a rollover policy by name.
    fn get_rollover_policy(&self, name: &str) -> Result<Option<RolloverPolicyRow>>;

    /// List all rollover policies.
    fn list_rollover_policies(&self) -> Result<Vec<RolloverPolicyRow>>;

    /// Delete a rollover policy.
    fn delete_rollover_policy(&self, name: &str) -> Result<bool>;

    // --- Table 13: search_ui_config ---

    /// Set search UI config for an index.
    fn upsert_search_ui_config(&self, config: &NewSearchUiConfig) -> Result<()>;

    /// Get search UI config for an index.
    fn get_search_ui_config(&self, index_uid: &str) -> Result<Option<SearchUiConfigRow>>;

    /// Delete search UI config for an index.
    fn delete_search_ui_config(&self, index_uid: &str) -> Result<bool>;

    // --- Table 14: admin_sessions ---

    /// Create an admin session.
    fn insert_admin_session(&self, session: &NewAdminSession) -> Result<()>;

    /// Get an admin session by id.
    fn get_admin_session(&self, session_id: &str) -> Result<Option<AdminSessionRow>>;

    /// Revoke a session (logout).
    fn revoke_admin_session(&self, session_id: &str) -> Result<bool>;

    /// Delete expired and revoked sessions (lazy eviction + pruner).
    fn delete_expired_admin_sessions(&self, now_ms: i64) -> Result<usize>;

    // --- Table 15: mode_b_operations ---

    /// Create or update a Mode B operation state.
    fn upsert_mode_b_operation(&self, operation: &ModeBOperation) -> Result<()>;

    /// Get a Mode B operation by ID.
    fn get_mode_b_operation(&self, operation_id: &str) -> Result<Option<ModeBOperation>>;

    /// Get the active Mode B operation for a scope (if any).
    fn get_mode_b_operation_by_scope(&self, scope: &str) -> Result<Option<ModeBOperation>>;

    /// List Mode B operations by type and/or status.
    fn list_mode_b_operations(&self, filter: &ModeBOperationFilter) -> Result<Vec<ModeBOperation>>;

    /// Delete a Mode B operation.
    fn delete_mode_b_operation(&self, operation_id: &str) -> Result<bool>;

    /// Delete old completed Mode B operations.
    fn prune_mode_b_operations(&self, cutoff_ms: i64, batch_size: u32) -> Result<usize>;

    // --- Table 15: search_ui_beacon (plan §13.21) ---

    /// Check if a beacon event_id has already been processed (idempotency).
    /// Returns true if the event_id is new (not yet processed), false if duplicate.
    /// If new, marks it as processed with a 24-hour TTL.
    fn check_and_mark_beacon_event(&self, index_uid: &str, event_id: &str) -> Result<bool>;

    // --- Table 16: ttl_policy (plan §13.14) ---

    /// Set or update TTL sweep policy for an index.
    fn upsert_ttl_policy(&self, policy: &NewTtlPolicy) -> Result<()>;

    /// Get TTL policy for an index.
    fn get_ttl_policy(&self, index_uid: &str) -> Result<Option<TtlPolicyRow>>;

    /// Delete TTL policy for an index.
    fn delete_ttl_policy(&self, index_uid: &str) -> Result<bool>;

    /// List all TTL policies.
    fn list_ttl_policies(&self) -> Result<Vec<TtlPolicyRow>>;
}

// --- Row types ---

/// New task to insert (table 1).
#[derive(Debug, Clone)]
pub struct NewTask {
    pub miroir_id: String,
    pub created_at: i64,
    pub status: String,
    pub node_tasks: HashMap<String, u64>,
    pub error: Option<String>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub index_uid: Option<String>,
    pub task_type: Option<String>,
    pub node_errors: HashMap<String, String>,
}

/// Task row from the DB (table 1).
#[derive(Debug, Clone)]
pub struct TaskRow {
    pub miroir_id: String,
    pub created_at: i64,
    pub status: String,
    pub node_tasks: HashMap<String, u64>,
    pub error: Option<String>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub index_uid: Option<String>,
    pub task_type: Option<String>,
    pub node_errors: HashMap<String, String>,
}

/// Node settings version row (table 2).
#[derive(Debug, Clone)]
pub struct NodeSettingsVersionRow {
    pub index_uid: String,
    pub node_id: String,
    pub version: i64,
    pub updated_at: i64,
}

/// New alias to create (table 3).
#[derive(Debug, Clone)]
pub struct NewAlias {
    pub name: String,
    pub kind: String,
    pub current_uid: Option<String>,
    pub target_uids: Option<Vec<String>>,
    pub version: i64,
    pub created_at: i64,
    pub history: Vec<AliasHistoryEntry>,
}

/// Alias row from the DB (table 3).
#[derive(Debug, Clone)]
pub struct AliasRow {
    pub name: String,
    pub kind: String,
    pub current_uid: Option<String>,
    pub target_uids: Option<Vec<String>>,
    pub version: i64,
    pub created_at: i64,
    pub history: Vec<AliasHistoryEntry>,
}

/// A single entry in alias history.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AliasHistoryEntry {
    pub uid: String,
    pub flipped_at: i64,
}

/// Session row (table 4).
#[derive(Debug, Clone)]
pub struct SessionRow {
    pub session_id: String,
    pub last_write_mtask_id: Option<String>,
    pub last_write_at: Option<i64>,
    pub pinned_group: Option<i64>,
    pub min_settings_version: i64,
    pub ttl: i64,
}

/// Idempotency cache entry (table 5).
#[derive(Debug, Clone)]
pub struct IdempotencyEntry {
    pub key: String,
    pub body_sha256: Vec<u8>,
    pub miroir_task_id: String,
    pub expires_at: i64,
}

/// New job to insert (table 6).
#[derive(Debug, Clone)]
pub struct NewJob {
    pub id: String,
    pub type_: String,
    pub params: String,
    pub state: String,
    pub progress: String,
    pub parent_job_id: Option<String>,
    pub chunk_index: Option<i64>,
    pub total_chunks: Option<i64>,
    pub created_at: i64,
}

/// Job row from the DB (table 6).
#[derive(Debug, Clone)]
pub struct JobRow {
    pub id: String,
    pub type_: String,
    pub params: String,
    pub state: String,
    pub claimed_by: Option<String>,
    pub claim_expires_at: Option<i64>,
    pub progress: String,
    pub parent_job_id: Option<String>,
    pub chunk_index: Option<i64>,
    pub total_chunks: Option<i64>,
    pub created_at: Option<i64>,
}

/// Leader lease row (table 7).
#[derive(Debug, Clone)]
pub struct LeaderLeaseRow {
    pub scope: String,
    pub holder: String,
    pub expires_at: i64,
}

/// Filter for listing tasks.
#[derive(Debug, Clone, Default)]
pub struct TaskFilter {
    pub status: Option<String>,
    pub index_uid: Option<String>,
    pub task_type: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

// --- Tables 8-14 row types (feature-flagged) ---

/// Canary definition row (table 8).
#[derive(Debug, Clone)]
pub struct CanaryRow {
    pub id: String,
    pub name: String,
    pub index_uid: String,
    pub interval_s: i64,
    pub query_json: String,
    pub assertions_json: String,
    pub enabled: bool,
    pub created_at: i64,
}

/// New or updated canary (table 8).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NewCanary {
    pub id: String,
    pub name: String,
    pub index_uid: String,
    pub interval_s: i64,
    pub query_json: String,
    pub assertions_json: String,
    pub enabled: bool,
    pub created_at: i64,
}

/// Canary run row (table 9).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CanaryRunRow {
    pub canary_id: String,
    pub ran_at: i64,
    pub status: String,
    pub latency_ms: i64,
    pub failed_assertions_json: Option<String>,
}

/// New canary run to insert (table 9).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NewCanaryRun {
    pub canary_id: String,
    pub ran_at: i64,
    pub status: String,
    pub latency_ms: i64,
    pub failed_assertions_json: Option<String>,
}

/// CDC cursor row (table 10).
#[derive(Debug, Clone)]
pub struct CdcCursorRow {
    pub sink_name: String,
    pub index_uid: String,
    pub last_event_seq: i64,
    pub updated_at: i64,
}

/// New or updated CDC cursor (table 10).
#[derive(Debug, Clone)]
pub struct NewCdcCursor {
    pub sink_name: String,
    pub index_uid: String,
    pub last_event_seq: i64,
    pub updated_at: i64,
}

/// Tenant map row (table 11).
#[derive(Debug, Clone)]
pub struct TenantMapRow {
    pub api_key_hash: Vec<u8>,
    pub tenant_id: String,
    pub group_id: Option<i64>,
}

/// New tenant mapping (table 11).
#[derive(Debug, Clone)]
pub struct NewTenantMapping {
    pub api_key_hash: Vec<u8>,
    pub tenant_id: String,
    pub group_id: Option<i64>,
}

/// Rollover policy row (table 12).
#[derive(Debug, Clone)]
pub struct RolloverPolicyRow {
    pub name: String,
    pub write_alias: String,
    pub read_alias: String,
    pub pattern: String,
    pub triggers_json: String,
    pub retention_json: String,
    pub template_json: String,
    pub enabled: bool,
}

/// New or updated rollover policy (table 12).
#[derive(Debug, Clone)]
pub struct NewRolloverPolicy {
    pub name: String,
    pub write_alias: String,
    pub read_alias: String,
    pub pattern: String,
    pub triggers_json: String,
    pub retention_json: String,
    pub template_json: String,
    pub enabled: bool,
}

/// Search UI config row (table 13).
#[derive(Debug, Clone)]
pub struct SearchUiConfigRow {
    pub index_uid: String,
    pub config_json: String,
    pub updated_at: i64,
}

/// New or updated search UI config (table 13).
#[derive(Debug, Clone)]
pub struct NewSearchUiConfig {
    pub index_uid: String,
    pub config_json: String,
    pub updated_at: i64,
}

/// Admin session row (table 14).
#[derive(Debug, Clone)]
pub struct AdminSessionRow {
    pub session_id: String,
    pub csrf_token: String,
    pub admin_key_hash: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub revoked: bool,
    pub user_agent: Option<String>,
    pub source_ip: Option<String>,
}

/// New admin session (table 14).
#[derive(Debug, Clone)]
pub struct NewAdminSession {
    pub session_id: String,
    pub csrf_token: String,
    pub admin_key_hash: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub user_agent: Option<String>,
    pub source_ip: Option<String>,
}

/// TTL policy row (table 16).
#[derive(Debug, Clone)]
pub struct TtlPolicyRow {
    pub index_uid: String,
    pub sweep_interval_s: i64,
    pub max_deletes_per_sweep: i64,
    pub enabled: bool,
    pub updated_at: i64,
}

/// New or updated TTL policy (table 16).
#[derive(Debug, Clone)]
pub struct NewTtlPolicy {
    pub index_uid: String,
    pub sweep_interval_s: i64,
    pub max_deletes_per_sweep: i64,
    pub enabled: bool,
    pub updated_at: i64,
}

/// Mode B operation state (table 15).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModeBOperation {
    pub operation_id: String,
    pub operation_type: String,
    pub scope: String,
    pub phase: String,
    pub phase_started_at: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub state_json: String,
    pub error: Option<String>,
    pub status: String,
    // Reshard-specific fields (nullable for other operation types)
    pub index_uid: Option<String>,
    pub old_shards: Option<i64>,
    pub target_shards: Option<i64>,
    pub shadow_index: Option<String>,
    pub documents_backfilled: Option<i64>,
    pub total_documents: Option<i64>,
}

/// Filter for listing Mode B operations.
#[derive(Debug, Clone, Default)]
pub struct ModeBOperationFilter {
    pub operation_type: Option<String>,
    pub scope: Option<String>,
    pub status: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Mode B operation status values.
pub mod mode_b_status {
    pub const RUNNING: &str = "running";
    pub const PAUSED: &str = "paused";
    pub const COMPLETED: &str = "completed";
    pub const FAILED: &str = "failed";
}

/// Mode B operation type values.
pub mod mode_b_type {
    pub const RESHARD: &str = "reshard";
    pub const REBALANCE: &str = "rebalance";
    pub const ALIAS_FLIP: &str = "alias_flip";
    pub const SETTINGS_BROADCAST: &str = "settings_broadcast";
    pub const ILM: &str = "ilm";
    pub const SCOPED_KEY_ROTATION: &str = "scoped_key_rotation";
}
