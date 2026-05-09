//! Schema definitions for all 14 task store tables (plan §4).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Table 1: Tasks
// ============================================================================

/// A Miroir task: unified view of a fan-out write operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Unique Miroir task ID (UUID).
    pub miroir_id: String,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Current task status.
    pub status: TaskStatus,
    /// Map of node ID to local Meilisearch task UID.
    pub node_tasks: HashMap<String, NodeTask>,
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

/// A node task: local Meilisearch task reference.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeTask {
    /// Local Meilisearch task UID.
    pub task_uid: u64,
    /// Current status of this node task.
    pub status: NodeTaskStatus,
}

/// Status of a node task.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeTaskStatus {
    /// Task is enqueued on the node.
    Enqueued,
    /// Task is processing on the node.
    Processing,
    /// Task succeeded on the node.
    Succeeded,
    /// Task failed on the node.
    Failed,
}

/// Filter for listing tasks.
#[derive(Debug, Clone, Default)]
pub struct TaskFilter {
    /// Filter by status.
    pub status: Option<TaskStatus>,
    /// Filter by node ID.
    pub node_id: Option<String>,
    /// Maximum number of results.
    pub limit: Option<usize>,
    /// Offset for pagination.
    pub offset: Option<usize>,
}

// ============================================================================
// Table 2: Node settings version
// ============================================================================

/// Node settings version entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSettingsVersion {
    /// Index name.
    pub index: String,
    /// Node ID.
    pub node_id: String,
    /// Current settings version.
    pub version: i64,
    /// Last update timestamp (Unix millis).
    pub updated_at: u64,
}

// ============================================================================
// Table 3: Aliases
// ============================================================================

/// Alias definition (single-target or multi-target).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alias {
    /// Alias name.
    pub name: String,
    /// Alias kind (single or multi).
    pub kind: AliasKind,
    /// Current target UID (single-target) or first target (multi-target).
    pub current_uid: Option<String>,
    /// Target UIDs (multi-target only).
    pub target_uids: Vec<String>,
    /// Alias version (for multi-target atomic updates).
    pub version: i64,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Last update timestamp (Unix millis).
    pub updated_at: u64,
}

/// Alias kind.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AliasKind {
    /// Single-target alias.
    Single,
    /// Multi-target alias.
    Multi,
}

// ============================================================================
// Table 4: Sessions
// ============================================================================

/// Read-your-writes session pin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    /// Session ID (UUID).
    pub session_id: String,
    /// Index name.
    pub index: String,
    /// Pinned settings version.
    pub settings_version: i64,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Expiration timestamp (Unix millis).
    pub expires_at: u64,
}

// ============================================================================
// Table 5: Idempotency cache
// ============================================================================

/// Idempotency cache entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyEntry {
    /// Request key (hash of request content).
    pub key: String,
    /// Response JSON.
    pub response: String,
    /// HTTP status code.
    pub status_code: u16,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
}

// ============================================================================
// Table 6: Jobs
// ============================================================================

/// Background job entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Job ID (UUID).
    pub job_id: String,
    /// Job type.
    pub job_type: String,
    /// Job parameters (JSON).
    pub parameters: String,
    /// Current job status.
    pub status: JobStatus,
    /// Worker ID currently processing the job.
    pub worker_id: Option<String>,
    /// Job result (JSON).
    pub result: Option<String>,
    /// Error message if the job failed.
    pub error: Option<String>,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Start timestamp (Unix millis).
    pub started_at: Option<u64>,
    /// Completion timestamp (Unix millis).
    pub completed_at: Option<u64>,
}

/// Job status.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobStatus {
    /// Job is enqueued.
    Enqueued,
    /// Job is being processed.
    Processing,
    /// Job completed successfully.
    Succeeded,
    /// Job failed.
    Failed,
    /// Job was canceled.
    Canceled,
}

// ============================================================================
// Table 7: Leader lease
// ============================================================================

/// Leader lease entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderLease {
    /// Lease ID (UUID).
    pub lease_id: String,
    /// Holder identity (pod ID).
    pub holder: String,
    /// Lease acquisition timestamp (Unix millis).
    pub acquired_at: u64,
    /// Lease expiration timestamp (Unix millis).
    pub expires_at: u64,
}

// ============================================================================
// Table 8: Canaries
// ============================================================================

/// Canary definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Canary {
    /// Canary name.
    pub name: String,
    /// Index to query.
    pub index: String,
    /// Query to run (Q string).
    pub query: String,
    /// Expected minimum result count.
    pub min_results: usize,
    /// Expected maximum result count.
    pub max_results: usize,
    /// Interval between runs (seconds).
    pub interval_s: u64,
    /// Whether the canary is enabled.
    pub enabled: bool,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Last update timestamp (Unix millis).
    pub updated_at: u64,
}

// ============================================================================
// Table 9: Canary runs
// ============================================================================

/// Canary run history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanaryRun {
    /// Run ID (UUID).
    pub run_id: String,
    /// Canary name.
    pub canary_name: String,
    /// Run timestamp (Unix millis).
    pub ran_at: u64,
    /// Whether the run passed.
    pub passed: bool,
    /// Actual result count.
    pub result_count: usize,
    /// Error message if the run failed.
    pub error: Option<String>,
    /// Latency in milliseconds.
    pub latency_ms: u64,
}

// ============================================================================
// Table 10: CDC cursors
// ============================================================================

/// CDC cursor entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcCursor {
    /// Sink name.
    pub sink: String,
    /// Index name.
    pub index: String,
    /// Current cursor position.
    pub cursor: String,
    /// Last update timestamp (Unix millis).
    pub updated_at: u64,
}

// ============================================================================
// Table 11: Tenant map
// ============================================================================

/// Tenant mapping (API key -> tenant).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    /// API key.
    pub api_key: String,
    /// Tenant ID.
    pub tenant_id: String,
    /// Tenant name.
    pub name: String,
    /// Tenant capabilities (JSON).
    pub capabilities: String,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Last update timestamp (Unix millis).
    pub updated_at: u64,
}

// ============================================================================
// Table 12: Rollover policies
// ============================================================================

/// ILM rollover policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloverPolicy {
    /// Policy name.
    pub name: String,
    /// Index pattern to apply the policy to.
    pub index_pattern: String,
    /// Maximum age for rollover (days).
    pub max_age_days: Option<u64>,
    /// Maximum size for rollover (bytes).
    pub max_size_bytes: Option<u64>,
    /// Maximum document count for rollover.
    pub max_docs: Option<u64>,
    /// Whether the policy is enabled.
    pub enabled: bool,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Last update timestamp (Unix millis).
    pub updated_at: u64,
}

// ============================================================================
// Table 13: Search UI config
// ============================================================================

/// Search UI configuration for an index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchUiConfig {
    /// Index name.
    pub index: String,
    /// UI configuration (JSON).
    pub config: String,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Last update timestamp (Unix millis).
    pub updated_at: u64,
}

// ============================================================================
// Table 14: Admin sessions
// ============================================================================

/// Admin UI session entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminSession {
    /// Session ID (UUID).
    pub session_id: String,
    /// User ID or username.
    pub user_id: String,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Expiration timestamp (Unix millis).
    pub expires_at: u64,
    /// Whether the session is revoked.
    pub revoked: bool,
}

// ============================================================================
// Schema version constant
// ============================================================================

/// Current schema version. Increment when table definitions change.
pub const SCHEMA_VERSION: i64 = 1;
