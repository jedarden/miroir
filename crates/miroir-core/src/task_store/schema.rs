//! Schema definitions for all 14 task store tables (plan §4).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Table 1: Tasks (plan §4)
// ============================================================================

/// A Miroir task: unified view of a fan-out write operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Unique Miroir task ID (UUID).
    pub miroir_id: String,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Current task status (enqueued | processing | succeeded | failed | canceled).
    pub status: TaskStatus,
    /// Map of node ID to local Meilisearch task UID (JSON: {"node-0": 42, "node-1": 17}).
    pub node_tasks: HashMap<String, u64>,
    /// Error message if the task failed.
    pub error: Option<String>,
}

/// Status of a Miroir task.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    /// Task is enqueued.
    Enqueued,
    /// Task is being processed.
    Processing,
    /// Task completed successfully.
    Succeeded,
    /// Task failed.
    Failed,
    /// Task was canceled.
    Canceled,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Enqueued => write!(f, "enqueued"),
            Self::Processing => write!(f, "processing"),
            Self::Succeeded => write!(f, "succeeded"),
            Self::Failed => write!(f, "failed"),
            Self::Canceled => write!(f, "canceled"),
        }
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "enqueued" => Ok(Self::Enqueued),
            "processing" => Ok(Self::Processing),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "canceled" => Ok(Self::Canceled),
            _ => Err(format!("invalid task status: {s}")),
        }
    }
}

/// Filter for listing tasks.
#[derive(Debug, Clone, Default)]
pub struct TaskFilter {
    /// Filter by status.
    pub status: Option<TaskStatus>,
    /// Maximum number of results.
    pub limit: Option<usize>,
    /// Offset for pagination.
    pub offset: Option<usize>,
}

// ============================================================================
// Table 2: Node settings version (plan §4)
// ============================================================================

/// Node settings version entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSettingsVersion {
    /// Index UID.
    pub index_uid: String,
    /// Node ID.
    pub node_id: String,
    /// Last cluster-wide settings_version this (index, node) pair verified.
    pub version: i64,
    /// Last update timestamp (Unix millis).
    pub updated_at: u64,
}

// ============================================================================
// Table 3: Aliases (plan §4)
// ============================================================================

/// Alias definition (single-target or multi-target).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alias {
    /// Alias name.
    pub name: String,
    /// Alias kind ('single' or 'multi').
    pub kind: AliasKind,
    /// Current target UID (non-null when kind='single').
    pub current_uid: Option<String>,
    /// JSON array of UIDs (non-null when kind='multi').
    pub target_uids: Option<Vec<String>>,
    /// Monotonic flip counter.
    pub version: i64,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// JSON array: last N prior states, bounded by aliases.history_retention.
    pub history: Vec<AliasHistoryEntry>,
}

/// Historical entry for an alias.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasHistoryEntry {
    /// The current_uid at this point in history (for single-target).
    pub current_uid: Option<String>,
    /// The target_uids at this point in history (for multi-target).
    pub target_uids: Option<Vec<String>>,
    /// The version at this point in history.
    pub version: i64,
    /// When this state was active.
    pub timestamp: u64,
}

/// Alias kind.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AliasKind {
    /// Single-target alias.
    Single,
    /// Multi-target alias.
    Multi,
}

impl std::fmt::Display for AliasKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Single => write!(f, "single"),
            Self::Multi => write!(f, "multi"),
        }
    }
}

impl std::str::FromStr for AliasKind {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "single" => Ok(Self::Single),
            "multi" => Ok(Self::Multi),
            _ => Err(format!("invalid alias kind: {s}")),
        }
    }
}

// ============================================================================
// Table 4: Sessions (plan §4)
// ============================================================================

/// Read-your-writes session pin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    /// Session ID (UUID).
    pub session_id: String,
    /// Miroir task ID of the last write (nullable: session may exist before any write).
    pub last_write_mtask_id: Option<String>,
    /// Timestamp of the last write (Unix millis).
    pub last_write_at: Option<u64>,
    /// group_id that first reached per-group quorum (nullable when pin cleared).
    pub pinned_group: Option<i64>,
    /// Minimum settings version for this session.
    pub min_settings_version: i64,
    /// Expiry timestamp (ms since epoch); default 15m from last use.
    pub ttl: u64,
}

// ============================================================================
// Table 5: Idempotency cache (plan §4)
// ============================================================================

/// Idempotency cache entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyEntry {
    /// Request key (hash of request content).
    pub key: String,
    /// SHA256 hash of request body (32 bytes).
    pub body_sha256: Vec<u8>,
    /// Associated Miroir task ID.
    pub miroir_task_id: String,
    /// Expiry timestamp (Unix millis).
    pub expires_at: u64,
}

// ============================================================================
// Table 6: Jobs (plan §4)
// ============================================================================

/// Background job entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Job ID (UUID).
    pub id: String,
    /// Job type (dump_import | reshard_backfill | ...).
    pub job_type: String,
    /// Job parameters (JSON).
    pub params: String,
    /// Job state (queued | in_progress | completed | failed).
    pub state: JobState,
    /// Pod ID of current claimant (nullable when queued).
    pub claimed_by: Option<String>,
    /// Lease heartbeat expiry (Unix millis).
    pub claim_expires_at: Option<u64>,
    /// Progress info (JSON: { bytes_processed, docs_routed, last_cursor, ... }).
    pub progress: String,
}

/// Job state.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobState {
    /// Job is queued.
    Queued,
    /// Job is in progress.
    InProgress,
    /// Job completed.
    Completed,
    /// Job failed.
    Failed,
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queued => write!(f, "queued"),
            Self::InProgress => write!(f, "in_progress"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

impl std::str::FromStr for JobState {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "queued" => Ok(Self::Queued),
            "in_progress" => Ok(Self::InProgress),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => Err(format!("invalid job state: {s}")),
        }
    }
}

// ============================================================================
// Table 7: Leader lease (plan §4)
// ============================================================================

/// Leader lease entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderLease {
    /// Lease scope (e.g. "reshard:<index>", "alias_flip:<name>", "settings_broadcast:<index>").
    pub scope: String,
    /// Pod ID of current leader.
    pub holder: String,
    /// Lease expiration timestamp (Unix millis).
    pub expires_at: u64,
}

// ============================================================================
// Table 8: Canaries (plan §4)
// ============================================================================

/// Canary definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Canary {
    /// Canary ID (UUID).
    pub id: String,
    /// Canary name.
    pub name: String,
    /// Index to query.
    pub index_uid: String,
    /// Interval between runs (seconds).
    pub interval_s: i64,
    /// Canary query body (JSON).
    pub query_json: String,
    /// Array of assertion specs (JSON).
    pub assertions_json: String,
    /// Whether the canary is enabled (0 | 1).
    pub enabled: i64,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
}

// ============================================================================
// Table 9: Canary runs (plan §4)
// ============================================================================

/// Canary run history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanaryRun {
    /// Canary ID.
    pub canary_id: String,
    /// Run timestamp (Unix millis).
    pub ran_at: u64,
    /// Run status (pass | fail | error).
    pub status: CanaryRunStatus,
    /// Latency in milliseconds.
    pub latency_ms: i64,
    /// JSON array of failed assertions (NULL when pass).
    pub failed_assertions_json: Option<String>,
}

/// Canary run status.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CanaryRunStatus {
    /// Run passed.
    Pass,
    /// Run failed.
    Fail,
    /// Run had error.
    Error,
}

impl std::fmt::Display for CanaryRunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pass => write!(f, "pass"),
            Self::Fail => write!(f, "fail"),
            Self::Error => write!(f, "error"),
        }
    }
}

impl std::str::FromStr for CanaryRunStatus {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "pass" => Ok(Self::Pass),
            "fail" => Ok(Self::Fail),
            "error" => Ok(Self::Error),
            _ => Err(format!("invalid canary run status: {s}")),
        }
    }
}

// ============================================================================
// Table 10: CDC cursors (plan §4)
// ============================================================================

/// CDC cursor entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcCursor {
    /// Sink name.
    pub sink_name: String,
    /// Index UID.
    pub index_uid: String,
    /// Current cursor position (last event sequence).
    pub last_event_seq: i64,
    /// Last update timestamp (Unix millis).
    pub updated_at: u64,
}

// ============================================================================
// Table 11: Tenant map (plan §4)
// ============================================================================

/// Tenant mapping (API key -> tenant).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    /// SHA256 hash of API key (32 bytes).
    pub api_key_hash: Vec<u8>,
    /// Tenant ID.
    pub tenant_id: String,
    /// Group ID (nullable: NULL falls through to hash(tenant_id) % RG).
    pub group_id: Option<i64>,
}

// ============================================================================
// Table 12: Rollover policies (plan §4)
// ============================================================================

/// ILM rollover policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloverPolicy {
    /// Policy name.
    pub name: String,
    /// Write alias.
    pub write_alias: String,
    /// Read alias.
    pub read_alias: String,
    /// Index pattern (e.g. "logs-{YYYY-MM-DD}").
    pub pattern: String,
    /// Triggers (JSON: { max_docs, max_age, max_size_gb }).
    pub triggers_json: String,
    /// Retention (JSON: { keep_indexes }).
    pub retention_json: String,
    /// Template (JSON: { primary_key, settings_ref }).
    pub template_json: String,
    /// Whether the policy is enabled (0 | 1).
    pub enabled: i64,
}

// ============================================================================
// Table 13: Search UI config (plan §4)
// ============================================================================

/// Search UI configuration for an index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchUiConfig {
    /// Index UID.
    pub index_uid: String,
    /// UI configuration (JSON).
    pub config_json: String,
    /// Last update timestamp (Unix millis).
    pub updated_at: u64,
}

// ============================================================================
// Table 14: Admin sessions (plan §4)
// ============================================================================

/// Admin UI session entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminSession {
    /// Session ID (UUID).
    pub session_id: String,
    /// CSRF token.
    pub csrf_token: String,
    /// SHA256 of admin key used at login.
    pub admin_key_hash: String,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Expiration timestamp (Unix millis).
    pub expires_at: u64,
    /// Whether the session is revoked (0 | 1).
    pub revoked: i64,
    /// User agent string.
    pub user_agent: Option<String>,
    /// Source IP address.
    pub source_ip: Option<String>,
}

// ============================================================================
// Schema version constant
// ============================================================================

/// Current schema version. Increment when table definitions change.
pub const SCHEMA_VERSION: i64 = 1;
