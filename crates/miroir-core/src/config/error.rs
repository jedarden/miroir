use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config file error: {0}")]
    File(#[from] std::io::Error),

    #[error("config parse error: {0}")]
    Parse(#[from] config::ConfigError),

    #[error("YAML serialization error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("validation error: {0}")]
    Validation(String),
}
