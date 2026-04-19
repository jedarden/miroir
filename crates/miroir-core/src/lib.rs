//! Miroir core library
//!
//! Provides routing, merging, and topology logic for the Miroir distributed search proxy.

pub mod config;
pub mod error;
pub mod merger;
pub mod router;
pub mod scatter;
pub mod task;
pub mod topology;

// Public re-exports
pub use error::{MiroirError, Result};
