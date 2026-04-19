//! Error types for Miroir.

use thiserror::Error;

/// Result type alias for Miroir operations.
pub type Result<T> = std::result::Result<T, MiroirError>;

/// Core error type for Miroir.
#[derive(Error, Debug)]
pub enum MiroirError {
    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// Topology error.
    #[error("topology error: {0}")]
    Topology(String),

    /// Routing error.
    #[error("routing error: {0}")]
    Routing(String),

    /// Merge error.
    #[error("merge error: {0}")]
    Merge(String),

    /// Task registry error.
    #[error("task error: {0}")]
    Task(String),

    /// Task store error.
    #[error("task store error: {0}")]
    TaskStore(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// SQLite error.
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// Redis error.
    #[error("Redis error: {0}")]
    Redis(String),

    /// Schema version mismatch.
    #[error("schema version {store_version} is ahead of binary version {binary_version}; cannot safely start. Restore from backup or upgrade binary.")]
    SchemaVersionAhead {
        store_version: i64,
        binary_version: i64,
    },

    /// Migration error.
    #[error("migration error: {0}")]
    Migration(String),
}
