//! Miroir configuration.

use serde::{Deserialize, Serialize};

/// Main Miroir configuration.
///
/// This struct represents the full configuration shape matching the plan §4 YAML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Shard count (fixed at index creation).
    pub shards: u32,

    /// Replication factor (elastic, intra-group copies per shard).
    pub replication_factor: usize,

    /// Number of replica groups (elastic, independent query pools).
    pub replica_groups: u32,

    /// Node configuration.
    pub nodes: Vec<NodeConfig>,

    /// Scatter configuration.
    pub scatter: ScatterConfig,

    /// Search UI configuration.
    #[serde(default)]
    pub search_ui: SearchUiConfig,
}

/// Configuration for a single node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique node identifier.
    pub id: String,

    /// Node base URL (e.g., <http://meilisearch-0.miroir:7700>).
    pub url: String,

    /// Replica group assignment (0-based).
    pub replica_group: u32,
}

/// Scatter (fan-out) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScatterConfig {
    /// Policy for handling unavailable shards.
    #[serde(default = "default_unavailable_shard_policy")]
    pub unavailable_shard_policy: UnavailableShardPolicy,
}

/// Policy for handling unavailable shards during scatter.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableShardPolicy {
    /// Return partial results from available nodes.
    Partial,

    /// Fail the request if any shard is unavailable.
    Fail,

    /// Fall back to another replica group for unavailable shards.
    Fallback,
}

fn default_unavailable_shard_policy() -> UnavailableShardPolicy {
    UnavailableShardPolicy::Partial
}

/// Search UI configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SearchUiConfig {
    /// Whether the search UI is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// CORS allowed origins.
    #[serde(default)]
    pub cors_allowed_origins: Vec<String>,
}
