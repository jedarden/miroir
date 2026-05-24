//! Meilisearch-compatible error responses.
//!
//! Per plan §5, all errors must match the Meilisearch shape:
//! {"message": "...", "code": "...", "type": "...", "link": "..."}

use axum::{
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Serialize;

/// Meilisearch-compatible error response.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Human-readable error message.
    pub message: String,

    /// Machine-readable error code.
    pub code: String,

    /// Error type category.
    #[serde(rename = "type")]
    pub error_type: String,

    /// Documentation link.
    pub link: String,
}

impl ErrorResponse {
    /// Create a new error response.
    pub fn new(message: impl Into<String>, code: impl Into<String>) -> Self {
        let message = message.into();
        let code = code.into();

        // Determine error type from code
        let error_type = if code.starts_with("miroir_") {
            "invalid_request".to_string()
        } else if code.contains("index") {
            "index_creation".to_string()
        } else if code.contains("document") {
            "document".to_string()
        } else {
            "invalid_request".to_string()
        };

        Self {
            message,
            code,
            error_type,
            link: "https://docs.meilisearch.com/errors".to_string(),
        }
    }

    /// Create an error for missing primary key.
    pub fn primary_key_required(index: &str) -> Self {
        Self::new(
            format!("Index `{index}` does not have a primary key. A primary key must be declared when creating the index in order to use the document routes."),
            "miroir_primary_key_required",
        )
    }

    /// Create an error for no quorum.
    pub fn no_quorum(shard_id: u32) -> Self {
        Self::new(
            format!("No replica group met quorum for shard {shard_id}"),
            "miroir_no_quorum",
        )
    }

    /// Create an error for unavailable shard.
    #[allow(dead_code)]
    pub fn shard_unavailable(shard_id: u32) -> Self {
        Self::new(
            format!("Shard {shard_id} is unavailable"),
            "miroir_shard_unavailable",
        )
    }

    /// Create an error for reserved field usage.
    #[allow(dead_code)]
    pub fn reserved_field(field: &str) -> Self {
        Self::new(
            format!("Field `{field}` is reserved for internal use and cannot be used in documents",),
            "miroir_reserved_field",
        )
    }

    /// Create an error for index not found.
    pub fn index_not_found(uid: &str) -> Self {
        Self::new(format!("Index `{uid}` not found."), "index_not_found")
    }

    /// Create an error for invalid request.
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(message, "invalid_request")
    }

    /// Create an error for document not found.
    #[allow(dead_code)]
    pub fn document_not_found(id: &str) -> Self {
        Self::new(
            format!("Document with id `{id}` not found."),
            "document_not_found",
        )
    }

    /// Create an internal server error.
    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::new(message, "internal_error")
    }
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        let status = if self.code == "miroir_no_quorum" || self.code == "miroir_shard_unavailable" {
            StatusCode::SERVICE_UNAVAILABLE
        } else if self.code.contains("not_found") {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::BAD_REQUEST
        };

        (status, Json(self)).into_response()
    }
}

/// Convert MiroirError to ErrorResponse.
impl From<miroir_core::MiroirError> for ErrorResponse {
    fn from(err: miroir_core::MiroirError) -> Self {
        match err {
            miroir_core::MiroirError::Config(msg) => Self::new(msg, "invalid_configuration"),
            miroir_core::MiroirError::Topology(msg) => Self::new(msg, "invalid_topology"),
            miroir_core::MiroirError::Routing(msg) => Self::new(msg, "internal_error"),
            miroir_core::MiroirError::Merge(msg) => Self::new(msg, "internal_error"),
            miroir_core::MiroirError::Task(msg) => Self::new(msg, "task_error"),
            miroir_core::MiroirError::Io(err) => Self::new(err.to_string(), "internal_error"),
            miroir_core::MiroirError::Json(err) => Self::new(err.to_string(), "internal_error"),
            _ => Self::new(err.to_string(), "internal_error"),
        }
    }
}
