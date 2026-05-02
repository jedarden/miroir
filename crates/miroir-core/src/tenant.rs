//! Tenant isolation support (future phase)

use serde::{Deserialize, Serialize};

/// Tenant configuration (placeholder)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantConfig {
    pub id: String,
    pub index: String,
}

/// Placeholder tenant manager
pub struct TenantManager;

impl TenantManager {
    pub fn new() -> Self {
        Self
    }
}
