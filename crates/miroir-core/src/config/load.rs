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
            builder = builder.add_source(ext_config::File::with_name(path));
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
    builder = builder.add_source(ext_config::File::with_name(path.to_string_lossy().as_ref()));

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
