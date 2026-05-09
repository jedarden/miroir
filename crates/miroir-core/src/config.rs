//! Miroir configuration — plan §4 YAML schema with §13 advanced capabilities.

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

/// Convenience alias.
pub type Config = MiroirConfig;

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

    /// Load from a specific file path with env-var overrides applied.
    pub fn load_from(path: &std::path::Path) -> Result<Self, ConfigError> {
        load::load_from(path)
    }

    /// Load from a YAML string (useful for testing).
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        load::from_yaml(yaml)
    }
}

// ---------------------------------------------------------------------------
// Core sub-structs (§4)
// ---------------------------------------------------------------------------

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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct HpaConfig {
    #[serde(default)]
    pub enabled: bool,
}

/// Policy for handling unavailable shards during scatter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableShardPolicy {
    /// Return partial results from available nodes.
    Partial,
    /// Fail the request if any shard is unavailable.
    Error,
    /// Fall back to another replica group for unavailable shards.
    Fallback,
}

impl Default for UnavailableShardPolicy {
    fn default() -> Self {
        Self::Partial
    }
}

impl std::fmt::Display for UnavailableShardPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Partial => write!(f, "partial"),
            Self::Error => write!(f, "error"),
            Self::Fallback => write!(f, "fallback"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns a minimal valid dev config (single-node, sqlite, RF=1).
    fn dev_config() -> MiroirConfig {
        MiroirConfig {
            replication_factor: 1,
            task_store: TaskStoreConfig {
                backend: "sqlite".into(),
                ..Default::default()
            },
            cdc: advanced::CdcConfig {
                buffer: advanced::CdcBufferConfig {
                    overflow: "drop".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
            search_ui: advanced::SearchUiConfig {
                rate_limit: advanced::SearchUiRateLimitConfig {
                    backend: "local".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn default_config_is_valid() {
        let cfg = MiroirConfig::default();
        // Default has replication_factor=2 with sqlite, which should fail
        // validation — but the struct itself should construct fine.
        assert_eq!(cfg.shards, 64);
        assert_eq!(cfg.replication_factor, 2);
        assert_eq!(cfg.replica_groups, 1);
        assert_eq!(cfg.task_store.backend, "sqlite");
    }

    #[test]
    fn minimal_yaml_deserializes() {
        let yaml = r#"
shards: 32
replication_factor: 1
nodes: []
"#;
        let cfg: MiroirConfig = serde_yaml::from_str(yaml).expect("deserialize");
        assert_eq!(cfg.shards, 32);
        assert_eq!(cfg.replication_factor, 1);
        // All §13 blocks should get defaults
        assert!(cfg.resharding.enabled);
        assert!(cfg.hedging.enabled);
        assert!(cfg.anti_entropy.enabled);
    }

    #[test]
    fn full_plan_example_deserializes() {
        let yaml = r#"
master_key: "test-key"
node_master_key: "node-key"
shards: 64
replication_factor: 2
replica_groups: 2
task_store:
  backend: redis
  url: "redis://redis:6379"
admin:
  enabled: true
nodes:
  - id: "meili-0"
    address: "http://meili-0.search.svc:7700"
    replica_group: 0
  - id: "meili-1"
    address: "http://meili-1.search.svc:7700"
    replica_group: 0
health:
  interval_ms: 5000
  timeout_ms: 2000
  unhealthy_threshold: 3
  recovery_threshold: 2
scatter:
  node_timeout_ms: 5000
  retry_on_timeout: true
  unavailable_shard_policy: partial
rebalancer:
  auto_rebalance_on_recovery: true
  max_concurrent_migrations: 4
  migration_timeout_s: 3600
server:
  port: 7700
  bind: "0.0.0.0"
  max_body_bytes: 104857600
leader_election:
  enabled: true
"#;
        let cfg: MiroirConfig = serde_yaml::from_str(yaml).expect("deserialize");
        assert_eq!(cfg.master_key, "test-key");
        assert_eq!(cfg.nodes.len(), 2);
        assert_eq!(cfg.replica_groups, 2);
        cfg.validate().expect("valid production config");
    }

    #[test]
    fn round_trip_yaml() {
        let original = MiroirConfig::default();
        let yaml = serde_yaml::to_string(&original).expect("serialize");
        let round_tripped: MiroirConfig = serde_yaml::from_str(&yaml).expect("deserialize");
        assert_eq!(original, round_tripped);
    }

    #[test]
    fn validation_rejects_ha_with_sqlite() {
        let mut cfg = dev_config();
        cfg.replication_factor = 2;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("redis"));
    }

    #[test]
    fn validation_rejects_zero_shards() {
        let mut cfg = dev_config();
        cfg.shards = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("shards"));
    }

    #[test]
    fn validation_rejects_duplicate_node_ids() {
        let mut cfg = dev_config();
        cfg.nodes = vec![
            NodeConfig {
                id: "n0".into(),
                address: "http://n0".into(),
                replica_group: 0,
            },
            NodeConfig {
                id: "n0".into(),
                address: "http://n0b".into(),
                replica_group: 0,
            },
        ];
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("duplicate"));
    }

    #[test]
    fn validation_rejects_node_outside_replica_groups() {
        let mut cfg = dev_config();
        cfg.nodes = vec![NodeConfig {
            id: "n0".into(),
            address: "http://n0".into(),
            replica_group: 5,
        }];
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("replica_group"));
    }

    #[test]
    fn validation_rejects_scoped_key_timing_inversion() {
        let mut cfg = dev_config();
        cfg.search_ui.scoped_key_max_age_days = 10;
        cfg.search_ui.scoped_key_rotate_before_expiry_days = 10;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("scoped_key"));
    }

    #[test]
    fn advanced_defaults_all_enabled() {
        let cfg = MiroirConfig::default();
        assert!(cfg.resharding.enabled);
        assert!(cfg.hedging.enabled);
        assert!(cfg.replica_selection.strategy == "adaptive");
        assert!(cfg.query_planner.enabled);
        assert!(cfg.settings_broadcast.strategy == "two_phase");
        assert!(cfg.session_pinning.enabled);
        assert!(cfg.aliases.enabled);
        assert!(cfg.anti_entropy.enabled);
        assert!(cfg.dump_import.mode == "streaming");
        assert!(cfg.idempotency.enabled);
        assert!(cfg.query_coalescing.enabled);
        assert!(cfg.multi_search.enabled);
        assert!(cfg.vector_search.enabled);
        assert!(cfg.cdc.enabled);
        assert!(cfg.ttl.enabled);
        assert!(cfg.tenant_affinity.enabled);
        assert!(cfg.shadow.enabled);
        assert!(cfg.ilm.enabled);
        assert!(cfg.canary_runner.enabled);
        assert!(cfg.explain.enabled);
        assert!(cfg.admin_ui.enabled);
        assert!(cfg.search_ui.enabled);
    }

    // Additional validation tests to improve coverage

    #[test]
    fn validation_rejects_hpa_with_sqlite() {
        let mut cfg = dev_config();
        cfg.hpa.enabled = true;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("hpa"));
    }

    #[test]
    fn validation_rejects_cdc_redis_overflow_with_sqlite() {
        let mut cfg = dev_config();
        cfg.cdc.buffer.overflow = "redis".into();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("cdc.buffer.overflow"));
    }

    #[test]
    fn validation_rejects_search_ui_rate_limit_redis_with_sqlite() {
        let mut cfg = dev_config();
        cfg.search_ui.rate_limit.backend = "redis".into();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("rate_limit"));
    }

    #[test]
    fn validation_rejects_replica_groups_without_leader_election() {
        let mut cfg = dev_config();
        cfg.replica_groups = 2;
        cfg.task_store.backend = "redis".into(); // Must be redis to test leader_election independently
        cfg.leader_election.enabled = false;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("leader_election"));
    }

    #[test]
    fn validation_rejects_tenant_affinity_dedicated_groups_out_of_range() {
        let mut cfg = dev_config();
        cfg.tenant_affinity.enabled = true;
        cfg.tenant_affinity.dedicated_groups = vec![0, 5]; // 5 is out of range (only 0-1 valid)
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("tenant_affinity"));
    }

    #[test]
    fn validation_rejects_tenant_affinity_static_map_out_of_range() {
        let mut cfg = dev_config();
        cfg.tenant_affinity.enabled = true;
        cfg.tenant_affinity.static_map.insert("tenant1".into(), 10); // 10 is out of range
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("tenant_affinity"));
    }

    #[test]
    fn validation_rejects_shadow_target_invalid_sample_rate_too_low() {
        let mut cfg = dev_config();
        cfg.shadow.targets.push(advanced::ShadowTargetConfig {
            name: "test".into(),
            url: "http://test".into(),
            api_key_env: String::new(),
            sample_rate: 0.0,
            operations: vec!["search".into()],
        });
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("sample_rate"));
    }

    #[test]
    fn validation_rejects_shadow_target_invalid_sample_rate_too_high() {
        let mut cfg = dev_config();
        cfg.shadow.targets.push(advanced::ShadowTargetConfig {
            name: "test".into(),
            url: "http://test".into(),
            api_key_env: String::new(),
            sample_rate: 1.5,
            operations: vec!["search".into()],
        });
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("sample_rate"));
    }

    #[test]
    fn validation_rejects_zero_server_port() {
        let mut cfg = dev_config();
        cfg.server.port = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("port"));
    }

    #[test]
    fn validation_rejects_zero_replication_factor() {
        let mut cfg = dev_config();
        cfg.replication_factor = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("replication_factor"));
    }

    #[test]
    fn validation_accepts_valid_shadow_target() {
        let mut cfg = dev_config();
        cfg.shadow.targets.push(advanced::ShadowTargetConfig {
            name: "test".into(),
            url: "http://test".into(),
            api_key_env: String::new(),
            sample_rate: 0.5,
            operations: vec!["search".into()],
        });
        cfg.validate().expect("valid shadow target");
    }

    #[test]
    fn validation_accepts_tenant_affinity_with_valid_groups() {
        let mut cfg = dev_config();
        cfg.replica_groups = 3;
        cfg.task_store.backend = "redis".into(); // Multi-group requires redis
        cfg.leader_election.enabled = true; // Multi-group requires leader election
        cfg.tenant_affinity.enabled = true;
        cfg.tenant_affinity.dedicated_groups = vec![0, 1, 2];
        cfg.tenant_affinity.static_map.insert("tenant1".into(), 0);
        cfg.validate().expect("valid tenant affinity");
    }

    #[test]
    fn validation_accepts_hpa_with_redis() {
        let mut cfg = dev_config();
        cfg.task_store.backend = "redis".into();
        cfg.hpa.enabled = true;
        cfg.validate().expect("valid hpa with redis");
    }
}
