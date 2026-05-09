//! Miroir core library
//!
//! Provides routing, merging, and topology logic for the Miroir distributed search proxy.

pub mod anti_entropy;
pub mod config;
pub mod error;
pub mod merger;
pub mod migration;
pub mod reshard;
pub mod router;
pub mod scatter;
pub mod score_comparability;
pub mod task;

// Task store backends (Phase 3) — gate behind feature flag
#[cfg(feature = "task-store")]
pub mod task_store;
pub mod topology;

// Raft prototype temporarily disabled (openraft 0.9.22 fails on Rust 1.87)
// #[cfg(feature = "raft-proto")]
// pub mod raft_proto;

// Public re-exports
pub use error::{MiroirError, Result};
