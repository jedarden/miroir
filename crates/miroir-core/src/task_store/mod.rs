mod sqlite;

pub use sqlite::SqliteTaskStore;

use crate::Result;
use std::collections::HashMap;

/// Per-table store operations covering tables 1–7 from plan §4.
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

    /// Get an alias by name.
    fn get_alias(&self, name: &str) -> Result<Option<AliasRow>>;

    /// Flip a single alias to a new current_uid, recording history.
    fn flip_alias(&self, name: &str, new_uid: &str, history_retention: usize) -> Result<bool>;

    /// Delete an alias.
    fn delete_alias(&self, name: &str) -> Result<bool>;

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
    fn renew_leader_lease(&self, scope: &str, holder: &str, expires_at: i64) -> Result<bool>;

    /// Get current lease holder for a scope.
    fn get_leader_lease(&self, scope: &str) -> Result<Option<LeaderLeaseRow>>;
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
}

/// Task row from the DB (table 1).
#[derive(Debug, Clone)]
pub struct TaskRow {
    pub miroir_id: String,
    pub created_at: i64,
    pub status: String,
    pub node_tasks: HashMap<String, u64>,
    pub error: Option<String>,
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
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}
