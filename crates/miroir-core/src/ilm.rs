//! ILM (Index Lifecycle Management) support (future phase)

use serde::{Deserialize, Serialize};

/// ILM policy (placeholder)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IlmPolicy {
    pub index: String,
    pub retention_days: u32,
}

/// Placeholder ILM manager
pub struct IlmManager;

impl IlmManager {
    pub fn new() -> Self {
        Self
    }
}
