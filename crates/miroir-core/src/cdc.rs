//! CDC (Change Data Capture) support (future phase)

use serde::{Deserialize, Serialize};

/// CDC checkpoint (placeholder)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcCheckpoint {
    pub index: String,
    pub sequence: u64,
}

/// Placeholder CDC manager
pub struct CdcManager;

impl CdcManager {
    pub fn new() -> Self {
        Self
    }
}
