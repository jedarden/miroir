//! Anti-entropy reconciler module.
//!
//! Stub for plan §13.8 anti-entropy shard reconciler.
//! Full implementation will follow the fingerprint → diff → repair pipeline.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::migration::{MigrationConfig, MigrationError};

/// Anti-entropy configuration (plan §13.8).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AntiEntropyConfig {
    pub enabled: bool,
    pub schedule_cron: String,
    pub shards_per_pass: u32,
    pub max_read_concurrency: u32,
    pub fingerprint_batch_size: u32,
    pub auto_repair: bool,
    pub updated_at_field: String,
}

impl Default for AntiEntropyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            schedule_cron: "0 */6 * * *".to_string(),
            shards_per_pass: 0,
            max_read_concurrency: 2,
            fingerprint_batch_size: 1000,
            auto_repair: true,
            updated_at_field: "_miroir_updated_at".to_string(),
        }
    }
}

/// Validates that migration is safe given the anti-entropy configuration.
/// Returns Ok(()) if safe, Err with a descriptive message if not.
pub fn validate_migration_safety(
    ae_config: &AntiEntropyConfig,
    migration_config: &MigrationConfig,
) -> Result<(), MigrationError> {
    if migration_config.skip_delta_pass && !ae_config.enabled {
        return Err(MigrationError::UnsafeCutoverNoAntiEntropy);
    }
    Ok(())
}

/// Generates a warning if anti-entropy is disabled during active migration.
/// The caller should log this at warn level.
pub fn migration_warning_if_ae_disabled(ae_enabled: bool) -> Option<String> {
    if ae_enabled {
        return None;
    }
    Some(
        "Anti-entropy is disabled. Shard migration cutover relies on the delta pass \
         to prevent data loss at the cutover boundary. If delta pass is also skipped, \
         documents written during migration may be permanently lost."
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_safe_with_delta_pass() {
        let ae = AntiEntropyConfig {
            enabled: false,
            ..Default::default()
        };
        let mc = MigrationConfig {
            skip_delta_pass: false,
            ..Default::default()
        };
        assert!(validate_migration_safety(&ae, &mc).is_ok());
    }

    #[test]
    fn test_validate_unsafe_without_anti_entropy() {
        let ae = AntiEntropyConfig {
            enabled: false,
            ..Default::default()
        };
        let mc = MigrationConfig {
            skip_delta_pass: true,
            anti_entropy_enabled: false,
            ..Default::default()
        };
        assert!(validate_migration_safety(&ae, &mc).is_err());
    }

    #[test]
    fn test_validate_safe_with_anti_entropy_safety_net() {
        let ae = AntiEntropyConfig {
            enabled: true,
            ..Default::default()
        };
        let mc = MigrationConfig {
            skip_delta_pass: true,
            anti_entropy_enabled: true,
            ..Default::default()
        };
        assert!(validate_migration_safety(&ae, &mc).is_ok());
    }

    #[test]
    fn test_warning_when_ae_disabled() {
        assert!(migration_warning_if_ae_disabled(false).is_some());
        assert!(migration_warning_if_ae_disabled(true).is_none());
    }
}
