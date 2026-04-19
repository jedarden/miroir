//! Proxy error types — thin wrappers around miroir-core error infrastructure.

/// Alias so internal modules can write `ApiError::new(code, msg)`.
pub use miroir_core::MeilisearchError as ApiError;
