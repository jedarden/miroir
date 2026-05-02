//! Shadow indexing support (future phase)

use serde::{Deserialize, Serialize};

/// Shadow index configuration (placeholder)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowIndexConfig {
    pub source: String,
    pub shadow: String,
}

/// Placeholder shadow manager
pub struct ShadowManager;

impl ShadowManager {
    pub fn new() -> Self {
        Self
    }
}
