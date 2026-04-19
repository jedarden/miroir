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

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
