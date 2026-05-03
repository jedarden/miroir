//! Miroir core library
//!
//! Provides routing, merging, and topology logic for the Miroir distributed search proxy.

pub mod alias;
pub mod anti_entropy;
pub mod api_error;
pub mod canary;
pub mod cdc;
pub mod config;
pub mod dump;
pub mod dump_import;
pub mod error;
pub mod explainer;
pub mod hedging;
pub mod idempotency;
pub mod ilm;
pub mod merger;
pub mod migration;
pub mod multi_search;
pub mod query_planner;
pub mod rebalancer;
pub mod replica_selection;
pub mod reshard;
pub mod router;
pub mod schema_migrations;
pub mod scatter;
pub mod session_pinning;
pub mod settings;
pub mod shadow;
pub mod task;
pub mod task_pruner;
pub mod task_registry;
pub mod task_store;
pub mod tenant;
pub mod topology;
pub mod ttl;

#[cfg(feature = "raft-proto")]
pub mod raft_proto;

// Public re-exports
pub use api_error::{ErrorType, MeilisearchError, MiroirCode};
pub use error::{MiroirError, Result};
