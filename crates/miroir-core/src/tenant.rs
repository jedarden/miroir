//! Tenant-to-replica-group affinity (plan §13.15).
//!
//! Provides noisy-neighbor isolation for multi-tenant deployments by
//! routing tenant queries to dedicated replica groups.

use crate::error::{MiroirError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Tenant affinity mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantMode {
    /// Read tenant ID from X-Miroir-Tenant header.
    Header,
    /// Derive tenant from API key via task store mapping.
    ApiKey,
    /// Static map only; unknown tenants use fallback.
    Explicit,
}

/// Fallback strategy for unknown tenants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FallbackStrategy {
    /// Route to hash(tenant_id) % RG.
    Hash,
    /// Route to a random group.
    Random,
    /// Reject the request with HTTP 403.
    Reject,
}

/// Tenant affinity configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantAffinityConfig {
    /// Whether tenant affinity is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Tenant resolution mode.
    #[serde(default = "default_mode")]
    pub mode: String,
    /// Header name for header mode.
    #[serde(default = "default_header_name")]
    pub header_name: String,
    /// Fallback strategy for unknown tenants.
    #[serde(default = "default_fallback")]
    pub fallback: String,
    /// Static tenant -> group mapping.
    #[serde(default)]
    pub static_map: HashMap<String, u32>,
    /// Groups reserved for mapped tenants only.
    #[serde(default)]
    pub dedicated_groups: Vec<u32>,
}

fn default_true() -> bool {
    true
}

fn default_mode() -> String {
    "header".into()
}

fn default_header_name() -> String {
    "X-Miroir-Tenant".into()
}

fn default_fallback() -> String {
    "hash".into()
}

impl Default for TenantAffinityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: default_mode(),
            header_name: default_header_name(),
            fallback: default_fallback(),
            static_map: HashMap::new(),
            dedicated_groups: Vec::new(),
        }
    }
}

impl TenantAffinityConfig {
    /// Parse the mode string.
    pub fn parse_mode(&self) -> TenantMode {
        match self.mode.as_str() {
            "header" => TenantMode::Header,
            "api_key" => TenantMode::ApiKey,
            "explicit" => TenantMode::Explicit,
            _ => TenantMode::Header,
        }
    }

    /// Parse the fallback strategy.
    pub fn parse_fallback(&self) -> FallbackStrategy {
        match self.fallback.as_str() {
            "hash" => FallbackStrategy::Hash,
            "random" => FallbackStrategy::Random,
            "reject" => FallbackStrategy::Reject,
            _ => FallbackStrategy::Hash,
        }
    }
}

/// Tenant resolution result.
#[derive(Debug, Clone)]
pub struct TenantResolution {
    /// Tenant ID (if resolved).
    pub tenant_id: Option<String>,
    /// Pinned replica group ID.
    pub pinned_group: Option<u32>,
    /// Whether this tenant is allowed (for dedicated groups).
    pub allowed: bool,
    /// Reason for the resolution.
    pub reason: String,
}

/// Tenant affinity manager.
pub struct TenantAffinityManager {
    /// Configuration.
    config: TenantAffinityConfig,
    /// API key -> tenant mapping (for api_key mode).
    api_key_map: Arc<RwLock<HashMap<String, String>>>,
    /// Metrics: queries per tenant.
    tenant_queries: Arc<RwLock<HashMap<String, u64>>>,
}

impl TenantAffinityManager {
    /// Create a new tenant affinity manager.
    pub fn new(config: TenantAffinityConfig) -> Self {
        Self {
            config,
            api_key_map: Arc::new(RwLock::new(HashMap::new())),
            tenant_queries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update the API key -> tenant mapping.
    pub async fn update_api_key_map(&self, map: HashMap<String, String>) {
        let mut api_key_map = self.api_key_map.write().await;
        *api_key_map = map;
    }

    /// Resolve tenant from headers.
    pub async fn resolve_from_headers(
        &self,
        headers: &HashMap<String, String>,
    ) -> Result<TenantResolution> {
        if !self.config.enabled {
            return Ok(TenantResolution {
                tenant_id: None,
                pinned_group: None,
                allowed: true,
                reason: "tenant affinity disabled".to_string(),
            });
        }

        let mode = self.config.parse_mode();
        let tenant_id = match mode {
            TenantMode::Header => {
                let tenant_id = headers.get(&self.config.header_name);
                match tenant_id {
                    Some(id) if !id.is_empty() => Some(id.clone()),
                    _ => None,
                }
            }
            TenantMode::ApiKey => {
                // For api_key mode, we'd look up the tenant from the API key
                // This would be done by the auth layer before calling this
                headers.get("x-miroir-tenant").cloned()
            }
            TenantMode::Explicit => {
                // Explicit mode only uses static_map
                headers.get(&self.config.header_name).cloned()
            }
        };

        let tenant_id = match tenant_id {
            Some(id) => id,
            None => {
                return Ok(TenantResolution {
                    tenant_id: None,
                    pinned_group: None,
                    allowed: true,
                    reason: "no tenant ID provided".to_string(),
                })
            }
        };

        // Check static map first
        if let Some(&group) = self.config.static_map.get(&tenant_id) {
            self.record_query(&tenant_id, group).await;
            return Ok(TenantResolution {
                tenant_id: Some(tenant_id.clone()),
                pinned_group: Some(group),
                allowed: true,
                reason: format!("static map -> group {}", group),
            });
        }

        // Check if this is a request for a dedicated group
        if !self.config.dedicated_groups.is_empty() {
            // For explicit mode, unknown tenants are rejected
            if mode == TenantMode::Explicit {
                let fallback = self.config.parse_fallback();
                return match fallback {
                    FallbackStrategy::Reject => Err(MiroirError::TenantNotAllowed {
                        tenant: tenant_id,
                        reason: "unknown tenant in explicit mode".to_string(),
                    }),
                    _ => Ok(TenantResolution {
                        tenant_id: Some(tenant_id.clone()),
                        pinned_group: None,
                        allowed: true,
                        reason: "unknown tenant, using fallback".to_string(),
                    }),
                };
            }
        }

        // Hash the tenant ID to a group
        let group = self.hash_tenant_to_group(&tenant_id);
        self.record_query(&tenant_id, group).await;

        Ok(TenantResolution {
            tenant_id: Some(tenant_id),
            pinned_group: Some(group),
            allowed: true,
            reason: format!("hash -> group {}", group),
        })
    }

    /// Hash tenant ID to a replica group.
    fn hash_tenant_to_group(&self, tenant_id: &str) -> u32 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        tenant_id.hash(&mut hasher);
        let hash = hasher.finish();
        // The actual group count will be provided by topology at routing time
        // For now, return a hash that can be modulo'd later
        hash as u32
    }

    /// Record a query for metrics.
    async fn record_query(&self, tenant_id: &str, group: u32) {
        let mut queries = self.tenant_queries.write().await;
        let key = format!("{}:{}", tenant_id, group);
        *queries.entry(key).or_insert(0) += 1;
    }

    /// Get query counts for a tenant.
    pub async fn get_tenant_queries(&self, tenant_id: &str) -> HashMap<u32, u64> {
        let queries = self.tenant_queries.read().await;
        let mut result = HashMap::new();
        for (key, count) in queries.iter() {
            if let Some((tid, group)) = key.split_once(':') {
                if tid == tenant_id {
                    if let Ok(g) = group.parse::<u32>() {
                        result.insert(g, *count);
                    }
                }
            }
        }
        result
    }

    /// Check if a group is dedicated.
    pub fn is_dedicated_group(&self, group: u32) -> bool {
        self.config.dedicated_groups.contains(&group)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = TenantAffinityConfig::default();
        assert!(config.enabled);
        assert_eq!(config.mode, "header");
        assert_eq!(config.header_name, "X-Miroir-Tenant");
        assert_eq!(config.fallback, "hash");
    }

    #[test]
    fn test_mode_parsing() {
        let config = TenantAffinityConfig {
            mode: "api_key".to_string(),
            ..Default::default()
        };
        assert_eq!(config.parse_mode(), TenantMode::ApiKey);
    }

    #[test]
    fn test_fallback_parsing() {
        let config = TenantAffinityConfig {
            fallback: "reject".to_string(),
            ..Default::default()
        };
        assert_eq!(config.parse_fallback(), FallbackStrategy::Reject);
    }

    #[tokio::test]
    async fn test_static_map_resolution() {
        let config = TenantAffinityConfig {
            enabled: true,
            mode: "header".to_string(),
            static_map: {
                let mut map = HashMap::new();
                map.insert("enterprise-co".to_string(), 0);
                map.insert("startup-inc".to_string(), 1);
                map
            },
            ..Default::default()
        };

        let manager = TenantAffinityManager::new(config);

        let mut headers = HashMap::new();
        headers.insert("X-Miroir-Tenant".to_string(), "enterprise-co".to_string());

        let resolution = manager.resolve_from_headers(&headers).await.unwrap();
        assert_eq!(resolution.tenant_id, Some("enterprise-co".to_string()));
        assert_eq!(resolution.pinned_group, Some(0));
        assert!(resolution.allowed);
    }

    #[tokio::test]
    async fn test_hash_based_resolution() {
        let config = TenantAffinityConfig {
            enabled: true,
            mode: "header".to_string(),
            ..Default::default()
        };

        let manager = TenantAffinityManager::new(config);

        let mut headers = HashMap::new();
        headers.insert("X-Miroir-Tenant".to_string(), "unknown-tenant".to_string());

        let resolution = manager.resolve_from_headers(&headers).await.unwrap();
        assert_eq!(resolution.tenant_id, Some("unknown-tenant".to_string()));
        assert!(resolution.pinned_group.is_some());
        assert!(resolution.allowed);
    }
}
