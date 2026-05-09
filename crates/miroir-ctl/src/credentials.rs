//! Admin API key credential loading.
//!
//! Priority order per plan §9:
//! 1. `MIROIR_ADMIN_API_KEY` environment variable
//! 2. `~/.config/miroir/credentials` TOML file
//! 3. `--admin-key` CLI flag (WARNING: visible in process list!)

use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

const ENV_VAR: &str = "MIROIR_ADMIN_API_KEY";
const CREDENTIALS_FILE: &str = "credentials";

/// Credentials loaded from `~/.config/miroir/credentials.toml`
#[derive(Deserialize)]
struct CredentialsFile {
    default: Option<CredentialsProfile>,
}

#[derive(Deserialize)]
struct CredentialsProfile {
    admin_api_key: Option<String>,
}

/// Error types for credential loading
#[derive(Debug)]
pub enum CredentialError {
    NotFound(String),
    IoError(std::io::Error),
    ParseError(String),
}

impl std::fmt::Display for CredentialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredentialError::NotFound(msg) => write!(f, "Credential not found: {msg}"),
            CredentialError::IoError(e) => write!(f, "IO error: {e}"),
            CredentialError::ParseError(msg) => write!(f, "Parse error: {msg}"),
        }
    }
}

impl std::error::Error for CredentialError {}

/// Load admin API key from the first available source.
///
/// Priority order:
/// 1. `MIROIR_ADMIN_API_KEY` environment variable
/// 2. `~/.config/miroir/credentials` TOML file (`default.admin_api_key`)
/// 3. Flag value (passed separately by caller)
///
/// Returns `Ok(Some(key))` if found, `Ok(None)` if no source has a key.
pub fn load_admin_key(flag_key: Option<String>) -> Result<Option<String>, CredentialError> {
    // Priority 1: Environment variable
    if let Ok(key) = std::env::var(ENV_VAR) {
        if !key.is_empty() {
            return Ok(Some(key));
        }
    }

    // Priority 2: Credentials file
    if let Some(key) = load_from_credentials_file()? {
        return Ok(Some(key));
    }

    // Priority 3: CLI flag
    if let Some(key) = flag_key {
        if !key.is_empty() {
            return Ok(Some(key));
        }
    }

    Ok(None)
}

/// Load admin key from `~/.config/miroir/credentials` TOML file.
fn load_from_credentials_file() -> Result<Option<String>, CredentialError> {
    let config_dir = dirs::config_dir().ok_or_else(|| {
        CredentialError::NotFound("Unable to determine config directory".to_string())
    })?;

    let miroir_config_dir = config_dir.join("miroir");
    let credentials_path = miroir_config_dir.join(CREDENTIALS_FILE);

    // Try .toml extension first, then plain name
    let paths = [
        credentials_path.with_extension("toml"),
        credentials_path.clone(),
    ];

    for path in &paths {
        if path.exists() {
            let contents = fs::read_to_string(path).map_err(CredentialError::IoError)?;

            let creds: CredentialsFile = toml::from_str(&contents)
                .map_err(|e| CredentialError::ParseError(format!("Invalid TOML: {e}")))?;

            if let Some(profile) = creds.default {
                if let Some(key) = profile.admin_api_key {
                    if !key.is_empty() {
                        return Ok(Some(key));
                    }
                }
            }

            // File exists but no key found
            return Ok(None);
        }
    }

    Ok(None)
}

/// Get the credentials file path for diagnostic messages
#[allow(dead_code)]
pub fn credentials_file_path() -> Option<PathBuf> {
    let config_dir = dirs::config_dir()?;
    let miroir_config_dir = config_dir.join("miroir");
    Some(
        miroir_config_dir
            .join(CREDENTIALS_FILE)
            .with_extension("toml"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;

    // Test isolation: each test gets its own temp config dir
    fn setup_test_config_dir(_test_name: &str) -> tempfile::TempDir {
        let temp_dir = tempfile::tempdir().unwrap();
        let miroir_dir = temp_dir.path().join("miroir");
        fs::create_dir_all(&miroir_dir).unwrap();
        temp_dir
    }

    fn write_credentials_file(dir: &std::path::Path, content: &str) {
        let creds_path = dir.join("credentials.toml");
        fs::write(creds_path, content).unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_env_var_takes_precedence() {
        // Ensure clean state at start of test
        env::remove_var(ENV_VAR);

        // Create a credentials file with a key
        let temp_dir = setup_test_config_dir("env_precedence");
        write_credentials_file(
            &temp_dir.path().join("miroir"),
            r#"
            [default]
            admin_api_key = "file-key-12345"
        "#,
        );

        // Mock env var (note: this affects the actual environment for the test process)
        env::set_var(ENV_VAR, "env-key-67890");

        // Env var should win even though file exists and flag is provided
        let result = load_admin_key(Some("flag-key-abcde".to_string())).unwrap();
        assert_eq!(result, Some("env-key-67890".to_string()));

        env::remove_var(ENV_VAR);
    }

    #[test]
    fn test_credentials_file_without_env() {
        let temp_dir = setup_test_config_dir("file_only");
        write_credentials_file(
            &temp_dir.path().join("miroir"),
            r#"
            [default]
            admin_api_key = "file-key-12345"
        "#,
        );

        // Since we can't mock dirs::config_dir(), we'll test the parsing logic directly
        let content = r#"
            [default]
            admin_api_key = "file-key-12345"
        "#;
        let creds: CredentialsFile = toml::from_str(content).unwrap();
        assert_eq!(
            creds.default.unwrap().admin_api_key.unwrap(),
            "file-key-12345"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_flag_as_fallback() {
        // No env var, no file - flag should be used
        env::remove_var(ENV_VAR);
        let result = load_admin_key(Some("flag-key-xyz".to_string())).unwrap();
        assert_eq!(result, Some("flag-key-xyz".to_string()));
    }

    #[test]
    #[serial_test::serial]
    fn test_no_credentials_returns_none() {
        env::remove_var(ENV_VAR);
        let result = load_admin_key(None as Option<String>).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    #[serial_test::serial]
    fn test_empty_key_is_ignored() {
        env::set_var(ENV_VAR, "");
        let result = load_admin_key(Some("flag-key".to_string())).unwrap();
        // Empty env var should be skipped, flag key used
        assert_eq!(result, Some("flag-key".to_string()));
        env::remove_var(ENV_VAR);
    }

    #[test]
    fn test_credentials_file_parsing() {
        // Test various TOML formats
        let valid_cases = vec![
            (
                r#"[default]
admin_api_key = "key123""#,
                "key123",
            ),
            (
                r#"[default]
  admin_api_key = "key456"
"#,
                "key456",
            ),
            (
                r#"[default]
admin_api_key = "key789"
[other]
admin_api_key = "other-key""#,
                "key789",
            ),
        ];

        for (toml, expected_key) in valid_cases {
            let creds: CredentialsFile = toml::from_str(toml).unwrap();
            assert_eq!(creds.default.unwrap().admin_api_key.unwrap(), expected_key);
        }
    }

    #[test]
    fn test_credentials_file_missing_default_section() {
        let toml = r#"[other]
admin_api_key = "other-key""#;
        let creds: CredentialsFile = toml::from_str(toml).unwrap();
        assert!(creds.default.is_none());
    }

    #[test]
    fn test_credentials_file_missing_key() {
        let toml = r#"[default]"#;
        let creds: CredentialsFile = toml::from_str(toml).unwrap();
        assert!(creds.default.unwrap().admin_api_key.is_none());
    }
}
