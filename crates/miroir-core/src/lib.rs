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
pub mod task;
pub mod topology;

#[cfg(feature = "raft-proto")]
pub mod raft_proto;

// Public re-exports
pub use error::{MiroirError, Result};
