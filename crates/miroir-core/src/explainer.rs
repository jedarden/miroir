//! Query explain support (future phase)

use serde::{Deserialize, Serialize};

/// Query explanation (placeholder)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExplanation {
    pub index: String,
    pub plan: String,
}

/// Placeholder explainer
pub struct Explainer;

impl Explainer {
    pub fn new() -> Self {
        Self
    }
}
