//! Tenant-to-replica-group affinity (plan §13.15).
//!
//! Provides noisy-neighbor isolation for multi-tenant deployments by
//! routing tenant queries to dedicated replica groups.

use crate::config::advanced::TenantAffinityConfig as Config;
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

impl Config {
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
    config: Config,
    /// API key -> tenant mapping (for api_key mode).
    api_key_map: Arc<RwLock<HashMap<String, String>>>,
    /// Metrics: queries per tenant.
    tenant_queries: Arc<RwLock<HashMap<String, u64>>>,
    /// Metrics: fallback counter by reason.
    fallback_count: Arc<RwLock<HashMap<String, u64>>>,
    /// Replica group count (for modulo in hash-based routing).
    replica_groups: Arc<RwLock<u32>>,
}

impl TenantAffinityManager {
    /// Create a new tenant affinity manager.
    pub fn new(config: Config) -> Self {
        Self {
            config,
            api_key_map: Arc::new(RwLock::new(HashMap::new())),
            tenant_queries: Arc::new(RwLock::new(HashMap::new())),
            fallback_count: Arc::new(RwLock::new(HashMap::new())),
            replica_groups: Arc::new(RwLock::new(1)),
        }
    }

    /// Update the replica group count (called when topology changes).
    pub async fn set_replica_groups(&self, count: u32) {
        let mut rg = self.replica_groups.write().await;
        *rg = count.max(1);
    }

    /// Update the API key -> tenant mapping.
    pub async fn update_api_key_map(&self, map: HashMap<String, String>) {
        let mut api_key_map = self.api_key_map.write().await;
        *api_key_map = map;
    }

    /// Resolve tenant from headers and determine the pinned replica group.
    ///
    /// # Arguments
    /// * `headers` - Request headers (may contain X-Miroir-Tenant)
    /// * `is_write` - True if this is a write operation (writes ignore affinity)
    ///
    /// # Returns
    /// A resolution indicating the tenant ID, pinned group, and whether allowed.
    pub async fn resolve_from_headers(
        &self,
        headers: &HashMap<String, String>,
        is_write: bool,
    ) -> Result<TenantResolution> {
        // Writes always fan out to all groups (consistency invariant)
        if is_write {
            return Ok(TenantResolution {
                tenant_id: None,
                pinned_group: None,
                allowed: true,
                reason: "write operation: fan out to all groups".to_string(),
            });
        }

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

        // Handle unknown tenant based on mode
        let fallback = self.config.parse_fallback();

        // For explicit mode, unknown tenants are handled by fallback policy
        if mode == TenantMode::Explicit {
            return match fallback {
                FallbackStrategy::Reject => {
                    self.record_fallback("explicit_unknown_tenant").await;
                    Err(MiroirError::TenantNotAllowed {
                        tenant: tenant_id,
                        reason: "unknown tenant in explicit mode".to_string(),
                    })
                }
                FallbackStrategy::Hash => {
                    let group = self.hash_tenant_to_group(&tenant_id).await?;
                    self.record_query(&tenant_id, group).await;
                    self.record_fallback("explicit_hash_fallback").await;
                    Ok(TenantResolution {
                        tenant_id: Some(tenant_id),
                        pinned_group: Some(group),
                        allowed: true,
                        reason: format!("explicit mode: hash fallback -> group {}", group),
                    })
                }
                FallbackStrategy::Random => {
                    let rg = *self.replica_groups.read().await;
                    let group = if rg > 0 {
                        rand::random::<u32>() % rg
                    } else {
                        0
                    };
                    self.record_query(&tenant_id, group).await;
                    self.record_fallback("explicit_random_fallback").await;
                    Ok(TenantResolution {
                        tenant_id: Some(tenant_id),
                        pinned_group: Some(group),
                        allowed: true,
                        reason: format!("explicit mode: random fallback -> group {}", group),
                    })
                }
            };
        }

        // Check dedicated groups constraint
        if !self.config.dedicated_groups.is_empty() {
            // For header/api_key modes with dedicated groups, hash first then check
            let group = self.hash_tenant_to_group(&tenant_id).await?;

            if self.config.dedicated_groups.contains(&group) {
                // This tenant hashed to a dedicated group but isn't in the static map
                // Apply fallback policy
                return match fallback {
                    FallbackStrategy::Reject => {
                        self.record_fallback("dedicated_group_reject").await;
                        Err(MiroirError::TenantNotAllowed {
                            tenant: tenant_id,
                            reason: format!(
                                "tenant routed to dedicated group {} but not in static map",
                                group
                            ),
                        })
                    }
                    FallbackStrategy::Hash => {
                        // Re-hash to a non-dedicated group by trying again with a salt
                        let salted_id = format!("{}-fallback", tenant_id);
                        let new_group = self.hash_tenant_to_group(&salted_id).await?;
                        self.record_query(&tenant_id, new_group).await;
                        self.record_fallback("dedicated_group_hash_fallback").await;
                        Ok(TenantResolution {
                            tenant_id: Some(tenant_id),
                            pinned_group: Some(new_group),
                            allowed: true,
                            reason: format!("dedicated group fallback: {} -> {}", group, new_group),
                        })
                    }
                    FallbackStrategy::Random => {
                        let rg = *self.replica_groups.read().await;
                        // Find a non-dedicated group
                        let new_group = if rg > 1 {
                            let mut candidate = rand::random::<u32>() % rg;
                            // If we landed on a dedicated group, try the next one
                            if self.config.dedicated_groups.contains(&candidate) {
                                candidate = (candidate + 1) % rg;
                            }
                            candidate
                        } else {
                            0
                        };
                        self.record_query(&tenant_id, new_group).await;
                        self.record_fallback("dedicated_group_random_fallback")
                            .await;
                        Ok(TenantResolution {
                            tenant_id: Some(tenant_id),
                            pinned_group: Some(new_group),
                            allowed: true,
                            reason: format!(
                                "dedicated group fallback: {} -> random {}",
                                group, new_group
                            ),
                        })
                    }
                };
            }

            self.record_query(&tenant_id, group).await;
            return Ok(TenantResolution {
                tenant_id: Some(tenant_id),
                pinned_group: Some(group),
                allowed: true,
                reason: format!("hash -> group {}", group),
            });
        }

        // No dedicated groups - standard hash-based routing
        let group = self.hash_tenant_to_group(&tenant_id).await?;
        self.record_query(&tenant_id, group).await;

        Ok(TenantResolution {
            tenant_id: Some(tenant_id),
            pinned_group: Some(group),
            allowed: true,
            reason: format!("hash -> group {}", group),
        })
    }

    /// Hash tenant ID to a replica group.
    ///
    /// Uses xxHash for fast, consistent hashing. The result is modulo'd by
    /// the replica group count to ensure the result is a valid group ID.
    async fn hash_tenant_to_group(&self, tenant_id: &str) -> Result<u32> {
        let replica_groups = *self.replica_groups.read().await;
        if replica_groups == 0 {
            return Err(MiroirError::InvalidState(
                "replica group count is zero".to_string(),
            ));
        }

        use twox_hash::XxHash64;
        let mut hasher = XxHash64::with_seed(0);
        tenant_id.hash(&mut hasher);
        let hash = hasher.finish();
        Ok((hash % replica_groups as u64) as u32)
    }

    /// Record a query for metrics.
    async fn record_query(&self, tenant_id: &str, group: u32) {
        let mut queries = self.tenant_queries.write().await;
        let key = format!("{}:{}", tenant_id, group);
        *queries.entry(key).or_insert(0) += 1;
    }

    /// Record a fallback event for metrics.
    async fn record_fallback(&self, reason: &str) {
        let mut fallbacks = self.fallback_count.write().await;
        *fallbacks.entry(reason.to_string()).or_insert(0) += 1;
    }

    /// Get query counts for a tenant (for metrics export).
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

    /// Get all tenant query counts (for metrics export).
    pub async fn get_all_tenant_queries(&self) -> HashMap<String, u64> {
        let queries = self.tenant_queries.read().await;
        queries.clone()
    }

    /// Get fallback counts by reason (for metrics export).
    pub async fn get_fallback_counts(&self) -> HashMap<String, u64> {
        let fallbacks = self.fallback_count.read().await;
        fallbacks.clone()
    }

    /// Check if a group is dedicated.
    pub fn is_dedicated_group(&self, group: u32) -> bool {
        self.config.dedicated_groups.contains(&group)
    }

    /// Get the configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_config() -> Config {
        Config {
            enabled: true,
            mode: "header".to_string(),
            header_name: "X-Miroir-Tenant".to_string(),
            fallback: "hash".to_string(),
            static_map: HashMap::new(),
            dedicated_groups: Vec::new(),
        }
    }

    #[test]
    fn test_mode_parsing() {
        let config = Config {
            mode: "api_key".to_string(),
            ..make_test_config()
        };
        assert_eq!(config.parse_mode(), TenantMode::ApiKey);
    }

    #[test]
    fn test_fallback_parsing() {
        let config = Config {
            fallback: "reject".to_string(),
            ..make_test_config()
        };
        assert_eq!(config.parse_fallback(), FallbackStrategy::Reject);
    }

    #[tokio::test]
    async fn test_static_map_resolution() {
        let mut static_map = HashMap::new();
        static_map.insert("enterprise-co".to_string(), 0);
        static_map.insert("startup-inc".to_string(), 1);

        let config = Config {
            static_map,
            ..make_test_config()
        };

        let manager = TenantAffinityManager::new(config);
        manager.set_replica_groups(2).await;

        let mut headers = HashMap::new();
        headers.insert("X-Miroir-Tenant".to_string(), "enterprise-co".to_string());

        let resolution = manager.resolve_from_headers(&headers, false).await.unwrap();
        assert_eq!(resolution.tenant_id, Some("enterprise-co".to_string()));
        assert_eq!(resolution.pinned_group, Some(0));
        assert!(resolution.allowed);
    }

    #[tokio::test]
    async fn test_hash_based_resolution() {
        let config = make_test_config();
        let manager = TenantAffinityManager::new(config);
        manager.set_replica_groups(3).await;

        let mut headers = HashMap::new();
        headers.insert("X-Miroir-Tenant".to_string(), "unknown-tenant".to_string());

        let resolution = manager.resolve_from_headers(&headers, false).await.unwrap();
        assert_eq!(resolution.tenant_id, Some("unknown-tenant".to_string()));
        assert!(resolution.pinned_group.is_some());
        assert!(resolution.allowed);
        // The group should be less than replica_groups
        assert!(resolution.pinned_group.unwrap() < 3);
    }

    #[tokio::test]
    async fn test_write_operation_ignores_affinity() {
        let config = make_test_config();
        let manager = TenantAffinityManager::new(config);

        let mut headers = HashMap::new();
        headers.insert("X-Miroir-Tenant".to_string(), "tenant-a".to_string());

        let resolution = manager.resolve_from_headers(&headers, true).await.unwrap();
        assert_eq!(resolution.tenant_id, None);
        assert_eq!(resolution.pinned_group, None);
        assert!(resolution.allowed);
        assert!(resolution.reason.contains("write operation"));
    }

    #[tokio::test]
    async fn test_explicit_mode_rejects_unknown() {
        let config = Config {
            mode: "explicit".to_string(),
            fallback: "reject".to_string(),
            ..make_test_config()
        };

        let manager = TenantAffinityManager::new(config);

        let mut headers = HashMap::new();
        headers.insert("X-Miroir-Tenant".to_string(), "unknown-tenant".to_string());

        let result = manager.resolve_from_headers(&headers, false).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            MiroirError::TenantNotAllowed { tenant, .. } => {
                assert_eq!(tenant, "unknown-tenant");
            }
            _ => panic!("expected TenantNotAllowed error"),
        }
    }

    #[tokio::test]
    async fn test_dedicated_group_reject() {
        let mut static_map = HashMap::new();
        static_map.insert("enterprise-co".to_string(), 0);

        let config = Config {
            static_map,
            dedicated_groups: vec![0],
            fallback: "reject".to_string(),
            ..make_test_config()
        };

        let manager = TenantAffinityManager::new(config);
        manager.set_replica_groups(2).await;

        // Unknown tenant that hashes to group 0 should be rejected
        let mut headers = HashMap::new();
        headers.insert("X-Miroir-Tenant".to_string(), "unknown-tenant".to_string());

        let result = manager.resolve_from_headers(&headers, false).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_dedicated_group_hash_fallback() {
        let mut static_map = HashMap::new();
        static_map.insert("enterprise-co".to_string(), 0);

        let config = Config {
            static_map,
            dedicated_groups: vec![0],
            fallback: "hash".to_string(),
            ..make_test_config()
        };

        let manager = TenantAffinityManager::new(config);
        manager.set_replica_groups(2).await;

        // Unknown tenant should be re-routed to non-dedicated group
        let mut headers = HashMap::new();
        headers.insert("X-Miroir-Tenant".to_string(), "unknown-tenant".to_string());

        let resolution = manager.resolve_from_headers(&headers, false).await.unwrap();
        assert_eq!(resolution.tenant_id, Some("unknown-tenant".to_string()));
        // Should not be group 0 (dedicated)
        assert_ne!(resolution.pinned_group, Some(0));
        assert!(resolution.reason.contains("fallback"));
    }

    #[tokio::test]
    async fn test_tenant_a_and_b_separate_groups() {
        let config = make_test_config();
        let manager = TenantAffinityManager::new(config.clone());
        manager.set_replica_groups(2).await;

        // Same tenant should always route to same group
        for _ in 0..10 {
            let mut headers = HashMap::new();
            headers.insert("X-Miroir-Tenant".to_string(), "tenant-a".to_string());

            let resolution = manager.resolve_from_headers(&headers, false).await.unwrap();
            assert_eq!(resolution.pinned_group, Some(0)); // "tenant-a" hashes to group 0 with xxHash
        }

        // Different tenant should route to different group
        let mut headers = HashMap::new();
        headers.insert("X-Miroir-Tenant".to_string(), "tenant-b".to_string());

        let resolution = manager.resolve_from_headers(&headers, false).await.unwrap();
        assert_eq!(resolution.pinned_group, Some(1)); // "tenant-b" hashes to group 1 with xxHash
    }

    #[tokio::test]
    async fn test_disabled_affinity() {
        let config = Config {
            enabled: false,
            ..make_test_config()
        };

        let manager = TenantAffinityManager::new(config);

        let mut headers = HashMap::new();
        headers.insert("X-Miroir-Tenant".to_string(), "tenant-a".to_string());

        let resolution = manager.resolve_from_headers(&headers, false).await.unwrap();
        assert_eq!(resolution.tenant_id, None);
        assert_eq!(resolution.pinned_group, None);
        assert!(resolution.reason.contains("disabled"));
    }

    #[tokio::test]
    async fn test_metrics_recording() {
        let config = make_test_config();
        let manager = TenantAffinityManager::new(config);
        manager.set_replica_groups(2).await;

        let mut headers = HashMap::new();
        headers.insert("X-Miroir-Tenant".to_string(), "tenant-a".to_string());

        for _ in 0..5 {
            manager.resolve_from_headers(&headers, false).await.unwrap();
        }

        let queries = manager.get_tenant_queries("tenant-a").await;
        assert_eq!(queries.get(&0).copied().unwrap_or(0), 5);
    }
}
