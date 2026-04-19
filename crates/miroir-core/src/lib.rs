//! Miroir core library
//!
//! Provides routing, merging, and topology logic for the Miroir distributed search proxy.

pub mod anti_entropy;
pub mod api_error;
pub mod config;
pub mod error;
pub mod merger;
pub mod migration;
pub mod reshard;
pub mod router;
pub mod schema_migrations;
pub mod scatter;
pub mod task;
pub mod task_pruner;
pub mod task_store;
pub mod topology;

#[cfg(feature = "raft-proto")]
pub mod raft_proto;

// Public re-exports
pub use api_error::{ErrorType, MeilisearchError, MiroirCode};
pub use error::{MiroirError, Result};
