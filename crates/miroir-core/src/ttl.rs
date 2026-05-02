//! TTL (Time To Live) document support (future phase)

use serde::{Deserialize, Serialize};

/// TTL configuration (placeholder)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlConfig {
    pub index: String,
    pub ttl_seconds: u64,
}

/// Placeholder TTL manager
pub struct TtlManager;

impl TtlManager {
    pub fn new() -> Self {
        Self
    }
}
