use crate::config::{ConfigError, MiroirConfig};

pub fn validate(cfg: &MiroirConfig) -> Result<(), ConfigError> {
    // replication_factor > 1 requires redis backend for HA
    if cfg.replication_factor > 1 && cfg.task_store.backend == "sqlite" {
        return Err(ConfigError::Validation(
            "replication_factor > 1 requires task_store.backend = 'redis' (SQLite is single-writer)".into(),
        ));
    }

    // replica_groups > 1 requires redis backend
    if cfg.replica_groups > 1 && cfg.task_store.backend == "sqlite" {
        return Err(ConfigError::Validation(
            "replica_groups > 1 requires task_store.backend = 'redis' (SQLite is single-writer)"
                .into(),
        ));
    }

    // Nodes must belong to a valid replica group
    if cfg.replica_groups > 0 {
        for node in &cfg.nodes {
            if node.replica_group >= cfg.replica_groups {
                return Err(ConfigError::Validation(format!(
                    "node '{}' has replica_group={} but only {} groups exist (0..{})",
                    node.id,
                    node.replica_group,
                    cfg.replica_groups,
                    cfg.replica_groups - 1
                )));
            }
        }
    }

    // Node IDs must be unique
    let mut seen_ids = std::collections::HashSet::new();
    for node in &cfg.nodes {
        if !seen_ids.insert(&node.id) {
            return Err(ConfigError::Validation(format!(
                "duplicate node id: '{}'",
                node.id
            )));
        }
    }

    // HPA enabled requires redis backend
    if cfg.hpa.enabled && cfg.task_store.backend == "sqlite" {
        return Err(ConfigError::Validation(
            "hpa.enabled = true requires task_store.backend = 'redis'".into(),
        ));
    }

    // Search UI scoped_key timing validation
    if cfg.search_ui.enabled {
        let max_age = cfg.search_ui.scoped_key_max_age_days;
        let rotate_before = cfg.search_ui.scoped_key_rotate_before_expiry_days;
        if rotate_before >= max_age {
            return Err(ConfigError::Validation(format!(
                "search_ui.scoped_key_rotate_before_expiry_days ({}) must be strictly less than scoped_key_max_age_days ({})",
                rotate_before, max_age
            )));
        }
    }

    // CDC overflow = redis requires redis backend
    if cfg.cdc.enabled && cfg.cdc.buffer.overflow == "redis" && cfg.task_store.backend != "redis" {
        return Err(ConfigError::Validation(
            "cdc.buffer.overflow = 'redis' requires task_store.backend = 'redis'".into(),
        ));
    }

    // Search UI rate_limit.backend = redis requires redis task store (when multi-pod)
    if cfg.search_ui.enabled
        && cfg.search_ui.rate_limit.backend == "redis"
        && cfg.task_store.backend != "redis"
    {
        return Err(ConfigError::Validation(
            "search_ui.rate_limit.backend = 'redis' requires task_store.backend = 'redis'".into(),
        ));
    }

    // Leader election should be enabled when replica_groups > 1
    if cfg.replica_groups > 1 && !cfg.leader_election.enabled {
        return Err(ConfigError::Validation(
            "leader_election.enabled must be true when replica_groups > 1".into(),
        ));
    }

    // Tenant affinity dedicated_groups must be within valid range
    if cfg.tenant_affinity.enabled {
        for g in &cfg.tenant_affinity.dedicated_groups {
            if *g >= cfg.replica_groups {
                return Err(ConfigError::Validation(format!(
                    "tenant_affinity.dedicated_groups contains {} but only {} groups (0..{})",
                    g,
                    cfg.replica_groups,
                    cfg.replica_groups - 1
                )));
            }
        }
        for (tenant, group) in &cfg.tenant_affinity.static_map {
            if *group >= cfg.replica_groups {
                return Err(ConfigError::Validation(format!(
                    "tenant_affinity.static_map: tenant '{}' maps to group {} but only {} groups (0..{})",
                    tenant,
                    group,
                    cfg.replica_groups,
                    cfg.replica_groups - 1
                )));
            }
        }
    }

    // Shadow targets must have valid sample_rate
    if cfg.shadow.enabled {
        for target in &cfg.shadow.targets {
            if target.sample_rate <= 0.0 || target.sample_rate > 1.0 {
                return Err(ConfigError::Validation(format!(
                    "shadow target '{}' has invalid sample_rate={} (must be 0 < rate <= 1)",
                    target.name, target.sample_rate
                )));
            }
        }
    }

    // Server port must be non-zero
    if cfg.server.port == 0 {
        return Err(ConfigError::Validation(
            "server.port must be non-zero".into(),
        ));
    }

    // shards must be non-zero
    if cfg.shards == 0 {
        return Err(ConfigError::Validation("shards must be non-zero".into()));
    }

    // replication_factor must be > 0
    if cfg.replication_factor == 0 {
        return Err(ConfigError::Validation(
            "replication_factor must be > 0".into(),
        ));
    }

    // CSP overrides must not contain wildcards (plan §9)
    // Wildcards are only allowed in the base template, not in overrides.
    // Overrides are additive and must be specific sources.
    for value in &cfg.admin_ui.csp_overrides.script_src {
        if value == "*" {
            return Err(ConfigError::Validation(
                "admin_ui.csp_overrides.script_src cannot contain wildcard '*'".into(),
            ));
        }
    }
    for value in &cfg.admin_ui.csp_overrides.img_src {
        if value == "*" {
            return Err(ConfigError::Validation(
                "admin_ui.csp_overrides.img_src cannot contain wildcard '*'".into(),
            ));
        }
    }
    for value in &cfg.admin_ui.csp_overrides.connect_src {
        if value == "*" {
            return Err(ConfigError::Validation(
                "admin_ui.csp_overrides.connect_src cannot contain wildcard '*'".into(),
            ));
        }
    }
    for value in &cfg.search_ui.csp_overrides.script_src {
        if value == "*" {
            return Err(ConfigError::Validation(
                "search_ui.csp_overrides.script_src cannot contain wildcard '*'".into(),
            ));
        }
    }
    for value in &cfg.search_ui.csp_overrides.img_src {
        if value == "*" {
            return Err(ConfigError::Validation(
                "search_ui.csp_overrides.img_src cannot contain wildcard '*'".into(),
            ));
        }
    }
    for value in &cfg.search_ui.csp_overrides.connect_src {
        if value == "*" {
            return Err(ConfigError::Validation(
                "search_ui.csp_overrides.connect_src cannot contain wildcard '*'".into(),
            ));
        }
    }

    // CORS allowed_origins must not contain wildcards for admin UI (plan §9).
    // Admin UI is a sensitive endpoint — wildcard CORS origins would allow
    // any site to make authenticated requests via user cookies.
    for value in &cfg.admin_ui.cors_allowed_origins {
        if value == "*" {
            return Err(ConfigError::Validation(
                "admin_ui.cors_allowed_origins cannot contain wildcard '*' \
                 (use specific origins or leave empty for same-origin)"
                    .into(),
            ));
        }
    }

    // Admin UI allowed_origins must not contain wildcard when admin UI is enabled (plan §9).
    // Use "same-origin" (the default) or list specific origins.
    if cfg.admin_ui.enabled {
        for value in &cfg.admin_ui.allowed_origins {
            if value == "*" {
                return Err(ConfigError::Validation(
                    "admin_ui.allowed_origins cannot contain wildcard '*' when admin_ui is enabled \
                     (use 'same-origin' or specific origins)".into(),
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{advanced, MiroirConfig, TaskStoreConfig};

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
    fn rejects_replica_groups_gt1_with_sqlite() {
        let mut cfg = dev_config();
        cfg.replica_groups = 2;
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("replica_groups > 1"));
    }

    #[test]
    fn rejects_hpa_enabled_with_sqlite() {
        let mut cfg = dev_config();
        cfg.hpa.enabled = true;
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("hpa.enabled"));
    }

    #[test]
    fn rejects_cdc_overflow_redis_without_redis() {
        let mut cfg = dev_config();
        cfg.cdc.enabled = true;
        cfg.cdc.buffer.overflow = "redis".into();
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("cdc.buffer.overflow"));
    }

    #[test]
    fn rejects_search_ui_rate_limit_redis_without_redis() {
        let mut cfg = dev_config();
        cfg.search_ui.enabled = true;
        cfg.search_ui.rate_limit.backend = "redis".into();
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("search_ui.rate_limit"));
    }

    #[test]
    fn rejects_replica_groups_gt1_without_leader_election() {
        let mut cfg = dev_config();
        cfg.task_store.backend = "redis".into();
        cfg.replica_groups = 2;
        cfg.leader_election.enabled = false;
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("leader_election"));
    }

    #[test]
    fn rejects_tenant_affinity_group_out_of_range() {
        let mut cfg = dev_config();
        cfg.tenant_affinity.enabled = true;
        cfg.tenant_affinity.dedicated_groups = vec![99];
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("tenant_affinity"));
    }

    #[test]
    fn rejects_tenant_affinity_static_map_out_of_range() {
        let mut cfg = dev_config();
        cfg.tenant_affinity.enabled = true;
        cfg.tenant_affinity.static_map.insert("tenant-a".into(), 99);
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("tenant_affinity.static_map"));
    }

    #[test]
    fn rejects_shadow_invalid_sample_rate() {
        let mut cfg = dev_config();
        cfg.shadow.enabled = true;
        cfg.shadow.targets = vec![advanced::ShadowTargetConfig {
            name: "t".into(),
            url: "http://t".into(),
            api_key_env: "MIROIR_SHADOW_KEY".into(),
            sample_rate: 0.0,
            operations: vec!["search".into()],
        }];
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("sample_rate"));
    }

    #[test]
    fn rejects_zero_server_port() {
        let mut cfg = dev_config();
        cfg.server.port = 0;
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("server.port"));
    }

    #[test]
    fn rejects_zero_replication_factor() {
        let mut cfg = dev_config();
        cfg.replication_factor = 0;
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("replication_factor"));
    }

    #[test]
    fn rejects_csp_overrides_wildcard_admin_ui() {
        let mut cfg = dev_config();
        cfg.admin_ui.csp_overrides.script_src = vec!["*".to_string()];
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("csp_overrides"));
        assert!(err.to_string().contains("wildcard"));
    }

    #[test]
    fn rejects_csp_overrides_wildcard_search_ui() {
        let mut cfg = dev_config();
        cfg.search_ui.csp_overrides.connect_src = vec!["*".to_string()];
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("csp_overrides"));
        assert!(err.to_string().contains("wildcard"));
    }

    #[test]
    fn rejects_admin_ui_cors_wildcard() {
        let mut cfg = dev_config();
        cfg.admin_ui.cors_allowed_origins = vec!["*".to_string()];
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("cors_allowed_origins"));
        assert!(err.to_string().contains("wildcard"));
    }

    #[test]
    fn rejects_admin_ui_allowed_origins_wildcard() {
        let mut cfg = dev_config();
        cfg.admin_ui.enabled = true;
        cfg.admin_ui.allowed_origins = vec!["*".to_string()];
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("allowed_origins"));
        assert!(err.to_string().contains("wildcard"));
    }

    #[test]
    fn allows_specific_cors_origins() {
        let mut cfg = dev_config();
        cfg.admin_ui.cors_allowed_origins = vec![
            "https://admin.example.com".to_string(),
            "https://ops.example.com".to_string(),
        ];
        cfg.admin_ui.allowed_origins = vec!["same-origin".to_string()];
        assert!(validate(&cfg).is_ok());
    }
}
