mod advanced;
mod error;
mod load;
mod validate;

pub use error::ConfigError;

use serde::{Deserialize, Serialize};

/// Top-level configuration matching plan §4 YAML schema under `miroir:`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MiroirConfig {
    // --- Secrets (env-var overrides) ---
    /// Client-facing API key. Env override: `MIROIR_MASTER_KEY`.
    pub master_key: String,
    /// Key Miroir uses on Meilisearch nodes. Env override: `MIROIR_NODE_MASTER_KEY`.
    pub node_master_key: String,

    // --- Core topology ---
    /// Total number of logical shards.
    pub shards: u32,
    /// Replication factor (intra-group replicas per shard). Production: 2.
    pub replication_factor: u32,
    /// Number of independent query pools. Default 1; production: 2.
    pub replica_groups: u32,

    // --- Sub-structs ---
    pub nodes: Vec<NodeConfig>,
    pub task_store: TaskStoreConfig,
    pub admin: AdminConfig,
    pub health: HealthConfig,
    pub scatter: ScatterConfig,
    pub rebalancer: RebalancerConfig,
    pub server: ServerConfig,
    pub connection_pool_per_node: ConnectionPoolConfig,
    pub task_registry: TaskRegistryConfig,

    // --- §13 advanced capabilities ---
    pub resharding: advanced::ReshardingConfig,
    pub hedging: advanced::HedgingConfig,
    pub replica_selection: advanced::ReplicaSelectionConfig,
    pub query_planner: advanced::QueryPlannerConfig,
    pub settings_broadcast: advanced::SettingsBroadcastConfig,
    pub settings_drift_check: advanced::SettingsDriftCheckConfig,
    pub session_pinning: advanced::SessionPinningConfig,
    pub aliases: advanced::AliasesConfig,
    pub anti_entropy: advanced::AntiEntropyConfig,
    pub dump_import: advanced::DumpImportConfig,
    pub idempotency: advanced::IdempotencyConfig,
    pub query_coalescing: advanced::QueryCoalescingConfig,
    pub multi_search: advanced::MultiSearchConfig,
    pub vector_search: advanced::VectorSearchConfig,
    pub cdc: advanced::CdcConfig,
    pub ttl: advanced::TtlConfig,
    pub tenant_affinity: advanced::TenantAffinityConfig,
    pub shadow: advanced::ShadowConfig,
    pub ilm: advanced::IlmConfig,
    pub canary_runner: advanced::CanaryRunnerConfig,
    pub explain: advanced::ExplainConfig,
    pub admin_ui: advanced::AdminUiConfig,
    pub search_ui: advanced::SearchUiConfig,

    // --- §14 horizontal scaling ---
    pub peer_discovery: PeerDiscoveryConfig,
    pub leader_election: LeaderElectionConfig,
    pub hpa: HpaConfig,
}

impl Default for MiroirConfig {
    fn default() -> Self {
        Self {
            master_key: String::new(),
            node_master_key: String::new(),
            shards: 64,
            replication_factor: 2,
            replica_groups: 1,
            nodes: Vec::new(),
            task_store: TaskStoreConfig::default(),
            admin: AdminConfig::default(),
            health: HealthConfig::default(),
            scatter: ScatterConfig::default(),
            rebalancer: RebalancerConfig::default(),
            server: ServerConfig::default(),
            connection_pool_per_node: ConnectionPoolConfig::default(),
            task_registry: TaskRegistryConfig::default(),
            resharding: advanced::ReshardingConfig::default(),
            hedging: advanced::HedgingConfig::default(),
            replica_selection: advanced::ReplicaSelectionConfig::default(),
            query_planner: advanced::QueryPlannerConfig::default(),
            settings_broadcast: advanced::SettingsBroadcastConfig::default(),
            settings_drift_check: advanced::SettingsDriftCheckConfig::default(),
            session_pinning: advanced::SessionPinningConfig::default(),
            aliases: advanced::AliasesConfig::default(),
            anti_entropy: advanced::AntiEntropyConfig::default(),
            dump_import: advanced::DumpImportConfig::default(),
            idempotency: advanced::IdempotencyConfig::default(),
            query_coalescing: advanced::QueryCoalescingConfig::default(),
            multi_search: advanced::MultiSearchConfig::default(),
            vector_search: advanced::VectorSearchConfig::default(),
            cdc: advanced::CdcConfig::default(),
            ttl: advanced::TtlConfig::default(),
            tenant_affinity: advanced::TenantAffinityConfig::default(),
            shadow: advanced::ShadowConfig::default(),
            ilm: advanced::IlmConfig::default(),
            canary_runner: advanced::CanaryRunnerConfig::default(),
            explain: advanced::ExplainConfig::default(),
            admin_ui: advanced::AdminUiConfig::default(),
            search_ui: advanced::SearchUiConfig::default(),
            peer_discovery: PeerDiscoveryConfig::default(),
            leader_election: LeaderElectionConfig::default(),
            hpa: HpaConfig::default(),
        }
    }
}

impl MiroirConfig {
    /// Validate cross-field constraints. Returns `Ok(())` or a `ConfigError`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate::validate(self)
    }

    /// Layered loading: file → env overrides → CLI overrides.
    pub fn load() -> Result<Self, ConfigError> {
        load::load()
    }
}

/// A single Meilisearch node in the cluster topology.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeConfig {
    pub id: String,
    pub address: String,
    pub replica_group: u32,
}

/// Task store backend configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TaskStoreConfig {
    /// `sqlite` or `redis`.
    pub backend: String,
    /// Path to SQLite database file (sqlite backend).
    pub path: String,
    /// Redis URL (redis backend), e.g. `redis://host:6379`.
    pub url: String,
}

impl Default for TaskStoreConfig {
    fn default() -> Self {
        Self {
            backend: "sqlite".into(),
            path: "/data/miroir-tasks.db".into(),
            url: String::new(),
        }
    }
}

/// Admin API configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AdminConfig {
    pub enabled: bool,
    /// Env override: `MIROIR_ADMIN_API_KEY`.
    pub api_key: String,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            api_key: String::new(),
        }
    }
}

/// Health check configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct HealthConfig {
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub unhealthy_threshold: u32,
    pub recovery_threshold: u32,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            interval_ms: 5000,
            timeout_ms: 2000,
            unhealthy_threshold: 3,
            recovery_threshold: 2,
        }
    }
}

/// Scatter-gather query configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ScatterConfig {
    pub node_timeout_ms: u64,
    pub retry_on_timeout: bool,
    /// `partial` or `error`.
    pub unavailable_shard_policy: String,
}

impl Default for ScatterConfig {
    fn default() -> Self {
        Self {
            node_timeout_ms: 5000,
            retry_on_timeout: true,
            unavailable_shard_policy: "partial".into(),
        }
    }
}

/// Rebalancer configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RebalancerConfig {
    pub auto_rebalance_on_recovery: bool,
    pub max_concurrent_migrations: u32,
    pub migration_timeout_s: u64,
}

impl Default for RebalancerConfig {
    fn default() -> Self {
        Self {
            auto_rebalance_on_recovery: true,
            max_concurrent_migrations: 4,
            migration_timeout_s: 3600,
        }
    }
}

/// Server (HTTP listener) configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub port: u16,
    pub bind: String,
    pub max_body_bytes: u64,
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: u32,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

fn default_max_concurrent_requests() -> u32 {
    500
}
fn default_request_timeout_ms() -> u64 {
    30000
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 7700,
            bind: "0.0.0.0".into(),
            max_body_bytes: 104_857_600, // 100 MiB
            max_concurrent_requests: default_max_concurrent_requests(),
            request_timeout_ms: default_request_timeout_ms(),
        }
    }
}

/// HTTP/2 connection pool per-node settings (§14.8).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConnectionPoolConfig {
    pub max_idle: u32,
    pub max_total: u32,
    pub idle_timeout_s: u64,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_idle: 32,
            max_total: 128,
            idle_timeout_s: 60,
        }
    }
}

/// Task registry cache settings (§14.8).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TaskRegistryConfig {
    pub cache_size: u32,
    pub redis_pool_max: u32,
}

impl Default for TaskRegistryConfig {
    fn default() -> Self {
        Self {
            cache_size: 10000,
            redis_pool_max: 50,
        }
    }
}

/// Peer discovery via Kubernetes headless Service (§14.5).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PeerDiscoveryConfig {
    pub service_name: String,
    pub refresh_interval_s: u64,
}

impl Default for PeerDiscoveryConfig {
    fn default() -> Self {
        Self {
            service_name: "miroir-headless".into(),
            refresh_interval_s: 15,
        }
    }
}

/// Leader election for Mode B background jobs (§14.5).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LeaderElectionConfig {
    pub enabled: bool,
    pub lease_ttl_s: u64,
    pub renew_interval_s: u64,
}

impl Default for LeaderElectionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            lease_ttl_s: 10,
            renew_interval_s: 3,
        }
    }
}

/// Horizontal Pod Autoscaler settings (Helm-only, informational in config).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct HpaConfig {
    pub enabled: bool,
}

impl Default for HpaConfig {
    fn default() -> Self {
        Self { enabled: false }
    }
}
