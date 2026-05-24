//! Layered configuration loading: file → env-var overrides → CLI overrides.

use super::{ConfigError, MiroirConfig};

// The local `config` module shadows the external `config` crate.
// Use a crate-qualified path to reach the external config crate.
use ::config as ext_config;
use serde_yaml;

/// Default config file paths to search (in order).
const CONFIG_SEARCH_PATHS: &[&str] = &[
    "miroir.yaml",
    "miroir.yml",
    "config/miroir.yaml",
    "/etc/miroir/config.yaml",
];

/// Environment variable prefix for overrides.
const ENV_PREFIX: &str = "MIROIR";

/// Load configuration using layered approach:
/// 1. Search for config file in default paths
/// 2. Apply environment variable overrides (`MIROIR_*`)
/// 3. Returns validated config
pub fn load() -> Result<MiroirConfig, ConfigError> {
    let mut builder = ext_config::Config::builder();

    builder = builder.add_source(ext_config::Config::try_from(&MiroirConfig::default())?);

    for path in CONFIG_SEARCH_PATHS {
        if std::path::Path::new(path).exists() {
            builder = builder.add_source(ext_config::File::from(std::path::Path::new(path)));
            break;
        }
    }

    builder = builder.add_source(
        ext_config::Environment::with_prefix(ENV_PREFIX)
            .separator("_")
            .try_parsing(true),
    );

    let cfg: MiroirConfig = builder.build()?.try_deserialize()?;
    cfg.validate()?;
    Ok(cfg)
}

/// Load from a specific file path with env-var overrides applied.
pub fn load_from(path: &std::path::Path) -> Result<MiroirConfig, ConfigError> {
    let mut builder = ext_config::Config::builder();

    builder = builder.add_source(ext_config::Config::try_from(&MiroirConfig::default())?);
    builder = builder.add_source(ext_config::File::from(path));

    builder = builder.add_source(
        ext_config::Environment::with_prefix(ENV_PREFIX)
            .separator("_")
            .try_parsing(true),
    );

    let cfg: MiroirConfig = builder.build()?.try_deserialize()?;
    cfg.validate()?;
    Ok(cfg)
}

/// Load from a YAML string (useful for testing).
pub fn from_yaml(yaml: &str) -> Result<MiroirConfig, ConfigError> {
    let cfg: MiroirConfig = serde_yaml::from_str(yaml)?;
    cfg.validate()?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_yaml_valid_config() {
        let yaml = r#"
shards: 32
replication_factor: 1
cdc:
  enabled: false
search_ui:
  rate_limit:
    backend: local
nodes: []
"#;
        let cfg = from_yaml(yaml).expect("should parse valid config");
        assert_eq!(cfg.shards, 32);
        assert_eq!(cfg.replication_factor, 1);
    }

    #[test]
    fn test_from_yaml_with_nodes() {
        let yaml = r#"
shards: 64
replication_factor: 1
replica_groups: 2
task_store:
  backend: redis
  url: "redis://localhost:6379"
nodes:
  - id: "node1"
    address: "http://node1:7700"
    replica_group: 0
  - id: "node2"
    address: "http://node2:7700"
    replica_group: 1
"#;
        let cfg = from_yaml(yaml).expect("should parse config with nodes");
        assert_eq!(cfg.nodes.len(), 2);
        assert_eq!(cfg.nodes[0].id, "node1");
        assert_eq!(cfg.nodes[1].replica_group, 1);
    }

    #[test]
    fn test_from_yaml_invalid_yaml_fails() {
        let yaml = r#"
shards: 32
replication_factor: invalid
nodes: []
"#;
        let result = from_yaml(yaml);
        assert!(result.is_err(), "should fail on invalid YAML");
    }

    #[test]
    fn test_from_yaml_validation_fails_on_ha_with_sqlite() {
        let yaml = r#"
shards: 64
replication_factor: 2
nodes: []
"#;
        let result = from_yaml(yaml);
        assert!(result.is_err(), "should fail validation: RF=2 requires redis");
    }

    #[test]
    fn test_from_yaml_validation_fails_on_zero_shards() {
        let yaml = r#"
shards: 0
replication_factor: 1
nodes: []
"#;
        let result = from_yaml(yaml);
        assert!(result.is_err(), "should fail validation: zero shards");
    }

    #[test]
    fn test_from_yaml_with_all_sections() {
        let yaml = r#"
shards: 64
replication_factor: 1
replica_groups: 2
master_key: "test-key"
node_master_key: "node-key"
nodes:
  - id: "node1"
    address: "http://node1:7700"
    replica_group: 0
task_store:
  backend: redis
  url: "redis://localhost:6379"
admin:
  enabled: true
  api_key: "admin-key"
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
        let cfg = from_yaml(yaml).expect("should parse full config");
        assert_eq!(cfg.shards, 64);
        assert_eq!(cfg.master_key, "test-key");
        assert_eq!(cfg.admin.api_key, "admin-key");
        assert_eq!(cfg.health.interval_ms, 5000);
        assert_eq!(cfg.scatter.node_timeout_ms, 5000);
    }
}
