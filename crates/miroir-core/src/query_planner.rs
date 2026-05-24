//! Shard-aware query planner (plan §13.4).
//!
//! Parses filter expressions to determine if a query can be narrowed to
//! a subset of shards based on primary key constraints.

use crate::error::{MiroirError, Result};
use crate::router::shard_for_key;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Query planner configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlannerConfig {
    /// Whether the query planner is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Maximum PK literals in a narrowable IN clause.
    #[serde(default = "default_max_literals")]
    pub max_pk_literals_narrowable: u32,
    /// Whether to log query plans.
    #[serde(default = "default_log_plans")]
    pub log_plans: bool,
}

fn default_true() -> bool {
    true
}
fn default_max_literals() -> u32 {
    128
}
fn default_log_plans() -> bool {
    false
}

impl Default for QueryPlannerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_pk_literals_narrowable: default_max_literals(),
            log_plans: default_log_plans(),
        }
    }
}

/// Query plan result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    /// Whether the query was narrowable.
    pub narrowed: bool,
    /// Reason for narrowing (or not).
    pub reason: String,
    /// Target shard IDs (empty if not narrowed).
    pub target_shards: Vec<u32>,
    /// Warnings generated during planning.
    pub warnings: Vec<String>,
}

/// Query planner.
pub struct QueryPlanner {
    /// Configuration.
    config: QueryPlannerConfig,
    /// Primary key field name for each index.
    primary_keys: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, String>>>,
}

impl QueryPlanner {
    /// Create a new query planner.
    pub fn new(config: QueryPlannerConfig) -> Self {
        Self {
            config,
            primary_keys: std::sync::Arc::new(tokio::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
        }
    }

    /// Set the primary key field for an index.
    pub async fn set_primary_key(&self, index: String, pk_field: String) {
        let mut pks = self.primary_keys.write().await;
        pks.insert(index, pk_field);
    }

    /// Get the primary key field for an index.
    pub async fn get_primary_key(&self, index: &str) -> Option<String> {
        let pks = self.primary_keys.read().await;
        pks.get(index).cloned()
    }

    /// Plan a query given its filter expression and index.
    ///
    /// Returns a plan indicating whether the query can be narrowed to
    /// a subset of shards.
    pub async fn plan(&self, index: &str, filter: &Option<String>, shard_count: u32) -> QueryPlan {
        if !self.config.enabled {
            return QueryPlan {
                narrowed: false,
                reason: "query planner disabled".to_string(),
                target_shards: vec![],
                warnings: vec![],
            };
        }

        let filter = match filter {
            Some(f) => f,
            None => {
                return QueryPlan {
                    narrowed: false,
                    reason: "no filter specified".to_string(),
                    target_shards: vec![],
                    warnings: vec![],
                }
            }
        };

        // Try to parse the filter for PK constraints
        let pk_field = match self.get_primary_key(index).await {
            Some(pk) => pk,
            None => {
                return QueryPlan {
                    narrowed: false,
                    reason: "primary key not configured for index".to_string(),
                    target_shards: vec![],
                    warnings: vec![],
                }
            }
        };

        match self.parse_pk_constraints(filter, &pk_field) {
            Ok(PkConstraint::Eq(literal)) => {
                // Single PK equality -> narrow to 1 shard
                let shard_id = shard_for_key(&literal, shard_count);
                QueryPlan {
                    narrowed: true,
                    reason: format!("PK equality: {} = {}", pk_field, literal),
                    target_shards: vec![shard_id],
                    warnings: vec![],
                }
            }
            Ok(PkConstraint::In(literals))
                if literals.len() <= self.config.max_pk_literals_narrowable as usize =>
            {
                // PK IN list -> narrow to N shards
                let mut shard_ids: HashSet<u32> = HashSet::new();
                for literal in &literals {
                    shard_ids.insert(shard_for_key(literal, shard_count));
                }
                let mut shards: Vec<u32> = shard_ids.into_iter().collect();
                shards.sort_unstable();
                QueryPlan {
                    narrowed: true,
                    reason: format!("PK IN list: {} values", literals.len()),
                    target_shards: shards,
                    warnings: vec![],
                }
            }
            Ok(PkConstraint::In(literals)) => {
                // Too many literals for narrowing
                QueryPlan {
                    narrowed: false,
                    reason: format!(
                        "PK IN list too large: {} values exceeds maximum of {}",
                        literals.len(),
                        self.config.max_pk_literals_narrowable
                    ),
                    target_shards: vec![],
                    warnings: vec![],
                }
            }
            Err(e) => QueryPlan {
                narrowed: false,
                reason: format!("filter not narrowable: {}", e),
                target_shards: vec![],
                warnings: vec![],
            },
        }
    }

    /// Parse a filter expression for PK constraints.
    ///
    /// Returns the PK constraint if narrowable, or an error if not.
    fn parse_pk_constraints(&self, filter: &str, pk_field: &str) -> Result<PkConstraint> {
        // Simple regex-based parser for common patterns:
        // 1. "{pk_field}" = "literal"
        // 2. "{pk_field}" IN ["literal1", "literal2", ...]

        let filter = filter.trim();

        // Check for non-narrowable patterns FIRST (before trying to match)
        if filter.contains(" OR ") {
            return Err(MiroirError::InvalidState(
                "contains OR at top level".to_string(),
            ));
        }

        if filter.contains(&format!("{} != ", pk_field))
            || filter.contains(&format!("{}<>", pk_field))
        {
            return Err(MiroirError::InvalidState(
                "PK negation is not narrowable".to_string(),
            ));
        }

        // Try equality: pk = "literal"
        let eq_pattern = format!(r#"{}\s*=\s*["']([^"']+)["']"#, pk_field);
        if let Some(re) = regex::Regex::new(&eq_pattern).ok() {
            if let Some(caps) = re.captures(filter) {
                if let Some(literal) = caps.get(1) {
                    return Ok(PkConstraint::Eq(literal.as_str().to_string()));
                }
            }
        }

        // Try IN list: pk IN ["literal1", "literal2", ...]
        let in_pattern = format!(r#"{}\s+IN\s+\[(.+)\]"#, pk_field);
        if let Some(re) = regex::Regex::new(&in_pattern).ok() {
            if let Some(caps) = re.captures(filter) {
                if let Some(list) = caps.get(1) {
                    let literals = self.parse_string_list(list.as_str())?;
                    return Ok(PkConstraint::In(literals));
                }
            }
        }

        Err(MiroirError::InvalidState(
            "no PK constraint found".to_string(),
        ))
    }

    /// Parse a comma-separated list of string literals.
    fn parse_string_list(&self, input: &str) -> Result<Vec<String>> {
        let mut result = Vec::new();
        let mut current = String::new();
        let mut in_string = false;
        let mut escape = false;

        for ch in input.chars() {
            match ch {
                '\\' if in_string => {
                    escape = true;
                }
                '"' if in_string && !escape => {
                    in_string = false;
                    result.push(current.clone());
                    current.clear();
                }
                '"' if !in_string => {
                    in_string = true;
                }
                ',' if !in_string => {
                    // Skip
                }
                ' ' | '\t' | '\n' if !in_string => {
                    // Skip whitespace
                }
                ch => {
                    current.push(ch);
                    escape = false;
                }
            }
        }

        Ok(result)
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new(QueryPlannerConfig::default())
    }
}

/// Parsed PK constraint.
#[derive(Debug, Clone)]
enum PkConstraint {
    /// Single equality: pk = "literal"
    Eq(String),
    /// IN list: pk IN ["a", "b", ...]
    In(Vec<String>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = QueryPlannerConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_pk_literals_narrowable, 128);
        assert!(!config.log_plans);
    }

    #[tokio::test]
    async fn test_plan_disabled() {
        let config = QueryPlannerConfig {
            enabled: false,
            ..Default::default()
        };
        let planner = QueryPlanner::new(config);

        let plan = planner
            .plan("products", &Some("sku = \"abc\"".to_string()), 64)
            .await;

        assert!(!plan.narrowed);
        assert!(plan.reason.contains("disabled"));
    }

    #[tokio::test]
    async fn test_plan_pk_equality() {
        let planner = QueryPlanner::default();
        planner
            .set_primary_key("products".into(), "sku".into())
            .await;

        let plan = planner
            .plan("products", &Some("sku = \"abc123\"".to_string()), 64)
            .await;

        assert!(plan.narrowed);
        assert_eq!(plan.target_shards.len(), 1);
        assert!(plan.reason.contains("PK equality"));
    }

    #[tokio::test]
    async fn test_plan_no_filter() {
        let planner = QueryPlanner::default();
        let plan = planner.plan("products", &None, 64).await;

        assert!(!plan.narrowed);
        assert!(plan.reason.contains("no filter"));
    }

    #[tokio::test]
    async fn test_plan_or_not_narrowable() {
        let planner = QueryPlanner::default();
        planner
            .set_primary_key("products".into(), "sku".into())
            .await;

        let plan = planner
            .plan(
                "products",
                &Some("sku = \"abc\" OR category = \"books\"".to_string()),
                64,
            )
            .await;

        assert!(!plan.narrowed);
        assert!(plan.reason.contains("OR"));
    }

    #[tokio::test]
    async fn test_plan_no_pk_configured() {
        let planner = QueryPlanner::default();
        let plan = planner
            .plan("products", &Some("sku = \"abc\"".to_string()), 64)
            .await;

        assert!(!plan.narrowed);
        assert!(plan.reason.contains("primary key not configured"));
    }
}
