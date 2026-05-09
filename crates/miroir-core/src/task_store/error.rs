//! Task store error types.

use std::fmt;

/// Task store error type.
#[derive(Debug)]
pub enum TaskStoreError {
    /// Invalid backend specified.
    InvalidBackend(String),

    /// SQLite backend error.
    Sqlite(rusqlite::Error),

    /// Redis backend error.
    Redis(redis::RedisError),

    /// JSON serialization error.
    Json(serde_json::Error),

    /// Not found error.
    NotFound(String),

    /// Already exists error.
    AlreadyExists(String),

    /// Invalid data error.
    InvalidData(String),

    /// Internal error.
    Internal(String),
}

impl fmt::Display for TaskStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBackend(backend) => write!(f, "invalid backend: {}", backend),
            Self::Sqlite(err) => write!(f, "SQLite error: {}", err),
            Self::Redis(err) => write!(f, "Redis error: {}", err),
            Self::Json(err) => write!(f, "JSON error: {}", err),
            Self::NotFound(key) => write!(f, "not found: {}", key),
            Self::AlreadyExists(key) => write!(f, "already exists: {}", key),
            Self::InvalidData(msg) => write!(f, "invalid data: {}", msg),
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for TaskStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlite(err) => Some(err),
            Self::Redis(err) => Some(err),
            Self::Json(err) => Some(err),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for TaskStoreError {
    fn from(err: rusqlite::Error) -> Self {
        Self::Sqlite(err)
    }
}

impl From<redis::RedisError> for TaskStoreError {
    fn from(err: redis::RedisError) -> Self {
        Self::Redis(err)
    }
}

impl From<serde_json::Error> for TaskStoreError {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

/// Result type alias for task store operations.
pub type Result<T> = std::result::Result<T, TaskStoreError>;
