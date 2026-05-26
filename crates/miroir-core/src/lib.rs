//! Miroir core library
//!
//! Provides routing, merging, and topology logic for the Miroir distributed search proxy.

// Allow functions with many parameters - refactoring to use parameter structs
// would be a significant API change. These functions are well-documented.
#![allow(clippy::too_many_arguments)]
// Some unused variables are intentional (e.g., for future use or debug-only),
// or are part of complex async patterns where suppressing is cleaner than
// adding conditional compilation attributes throughout.
#![allow(unused_variables)]
#![allow(dead_code)]
// Additional test-specific allowances
#![cfg_attr(test, allow(clippy::useless_vec))]
#![cfg_attr(test, allow(non_snake_case))]
#![cfg_attr(test, allow(clippy::too_many_arguments))]
#![cfg_attr(test, allow(clippy::uninlined_format_args))]
#![cfg_attr(test, allow(clippy::needless_raw_string_hashes))]

pub mod alias;
pub mod anti_entropy;
pub mod api_error;
pub mod canary;
pub mod cdc;
pub mod config;
pub mod drift_reconciler;
pub mod dump;
pub mod dump_chunking;
pub mod dump_import;
pub mod error;
pub mod explainer;
pub mod group_addition;
pub mod group_sync_worker;
pub mod hedging;
pub mod idempotency;
pub mod ilm;
pub mod leader_election;
pub mod merger;
pub mod migration;
#[cfg(feature = "peer-discovery")]
pub mod mode_a_coordinator;
#[cfg(test)]
mod mode_b_acceptance_tests;
pub mod mode_b_coordinator;
#[cfg(test)]
mod mode_c_acceptance_tests;
pub mod mode_c_coordinator;
pub mod mode_c_worker;
pub mod multi_search;
#[cfg(feature = "peer-discovery")]
pub mod peer_discovery;
pub mod query_planner;
pub mod rebalancer;
pub mod rebalancer_worker;
pub mod replica_selection;
pub mod reshard;
pub mod reshard_chunking;
pub mod resource_pressure;
pub mod router;
pub mod scatter;
pub mod schema_migrations;
pub mod scoped_key_rotation;
pub mod score_comparability;
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
pub mod vector;

// Raft prototype temporarily disabled (openraft 0.9.22 fails on Rust 1.87)
// #[cfg(feature = "raft-proto")]
// pub mod raft_proto;

// Public re-exports
pub use api_error::{ErrorType, MeilisearchError, MiroirCode};
pub use error::{MiroirError, Result};
pub use scatter::VectorMode;
