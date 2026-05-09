//! §13.4 Shard-aware query planner for PK-constrained searches.
//!
//! Parses filter expressions to narrow fan-out when primary key is constrained.

use crate::Result;
use serde::{Deserialize, Serialize};

/// Query plan result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    /// Whether the query was narrowable.
    pub narrowed: bool,
    /// Human-readable reason for narrowing (or not).
    pub reason: String,
    /// Target shard IDs (empty = full fan-out).
    pub target_shards: Vec<u32>,
    /// Original filter expression.
    pub filter: Option<String>,
}

/// Planner configuration.
#[derive(Debug, Clone)]
pub struct PlannerConfig {
    pub enabled: bool,
    pub max_pk_literals_narrowable: u32,
    pub log_plans: bool,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_pk_literals_narrowable: 128,
            log_plans: false,
        }
    }
}

/// Query planner.
pub struct QueryPlanner {
    config: PlannerConfig,
    primary_key: String,
    shard_count: u32,
}

impl QueryPlanner {
    pub fn new(primary_key: String, shard_count: u32, config: PlannerConfig) -> Self {
        Self {
            config,
            primary_key,
            shard_count,
        }
    }

    /// Plan a query based on its filter expression.
    pub fn plan(&self, filter: Option<&str>) -> QueryPlan {
        if !self.config.enabled {
            return QueryPlan {
                narrowed: false,
                reason: "planner disabled".to_string(),
                target_shards: vec![],
                filter: filter.map(|s| s.to_string()),
            };
        }

        let filter_expr = match filter {
            Some(f) => f,
            None => {
                return QueryPlan {
                    narrowed: false,
                    reason: "no filter".to_string(),
                    target_shards: vec![],
                    filter: None,
                };
            }
        };

        match self.try_narrow(filter_expr) {
            Ok(plan) => {
                if self.config.log_plans {
                    tracing::debug!(
                        pk = %self.primary_key,
                        narrowed = plan.narrowed,
                        reason = %plan.reason,
                        shards = ?plan.target_shards,
                        "Query plan"
                    );
                }
                plan
            }
            Err(e) => {
                tracing::warn!(
                    pk = %self.primary_key,
                    filter = %filter_expr,
                    error = %e,
                    "Query planning failed, using full fan-out"
                );
                QueryPlan {
                    narrowed: false,
                    reason: format!("parse error: {}", e),
                    target_shards: vec![],
                    filter: Some(filter_expr.to_string()),
                }
            }
        }
    }

    /// Attempt to narrow the query based on filter expression.
    fn try_narrow(&self, filter: &str) -> Result<QueryPlan> {
        // Simple filter parser for common patterns:
        // - "pk = \"value\""  -> single shard
        // - "pk IN [\"a\", \"b\", \"c\"]" -> multiple shards
        // - "pk = \"value\" AND other..." -> single shard
        // - "pk IN [...] AND other..." -> multiple shards
        // - "pk = \"x\" OR pk = \"y\"" -> NOT narrowable (different shards)

        let filter = filter.trim();

        // OR conditions are not narrowable (may target different shards)
        if filter.contains(" OR ") || filter.contains(" or ") {
            return Ok(QueryPlan {
                narrowed: false,
                reason: "OR condition not narrowable".to_string(),
                target_shards: vec![],
                filter: Some(filter.to_string()),
            });
        }

        // Check for PK equality
        if let Some(shard_id) = self.extract_pk_equality(filter)? {
            return Ok(QueryPlan {
                narrowed: true,
                reason: format!("pk filter: {} = \"...\"", self.primary_key),
                target_shards: vec![shard_id],
                filter: Some(filter.to_string()),
            });
        }

        // Check for PK IN clause
        if let Some(shard_ids) = self.extract_pk_in(filter)? {
            if shard_ids.len() > self.config.max_pk_literals_narrowable as usize {
                return Ok(QueryPlan {
                    narrowed: false,
                    reason: format!("pk IN list exceeds max_pk_literals_narrowable ({})", shard_ids.len()),
                    target_shards: vec![],
                    filter: Some(filter.to_string()),
                });
            }

            return Ok(QueryPlan {
                narrowed: true,
                reason: format!("pk filter: {} IN [{} values]", self.primary_key, shard_ids.len()),
                target_shards: shard_ids,
                filter: Some(filter.to_string()),
            });
        }

        // Check for PK filter with AND (narrowable if only AND branches)
        if let Some(plan) = self.extract_pk_and(filter)? {
            return Ok(plan);
        }

        // Not narrowable
        Ok(QueryPlan {
            narrowed: false,
            reason: "no pk-constrained filter".to_string(),
            target_shards: vec![],
            filter: Some(filter.to_string()),
        })
    }

    /// Extract shard ID from PK equality filter.
    /// Pattern: `{primary_key} = "literal"`
    fn extract_pk_equality(&self, filter: &str) -> Result<Option<u32>> {
        let pattern = format!("{} = \"", self.primary_key);

        // Check for exact match
        if let Some(pos) = filter.find(&pattern) {
            // Extract the value
            let start = pos + pattern.len();
            if let Some(end) = filter[start..].find('"') {
                let value = &filter[start..start + end];
                let shard_id = self.hash_to_shard(value);
                return Ok(Some(shard_id));
            }
        }

        Ok(None)
    }

    /// Extract shard IDs from PK IN clause.
    /// Pattern: `{primary_key} IN ["a", "b", "c"]`
    fn extract_pk_in(&self, filter: &str) -> Result<Option<Vec<u32>>> {
        let pattern = format!("{} IN [", self.primary_key);

        if let Some(pos) = filter.find(&pattern) {
            let start = pos + pattern.len();
            let mut shard_ids = Vec::new();
            let mut current = start;

            // Parse comma-separated values
            while current < filter.len() {
                // Skip whitespace
                while current < filter.len() && filter[current..].starts_with(' ') {
                    current += 1;
                }

                // Check for opening quote
                if !filter[current..].starts_with('"') {
                    break;
                }
                current += 1;

                // Find closing quote
                if let Some(end) = filter[current..].find('"') {
                    let value = &filter[current..current + end];
                    let shard_id = self.hash_to_shard(value);
                    shard_ids.push(shard_id);
                    current += end + 1;

                    // Skip comma
                    if current < filter.len() && filter[current..].starts_with(',') {
                        current += 1;
                    }
                } else {
                    break;
                }

                // Check for closing bracket
                while current < filter.len() && (filter[current..].starts_with(' ') || filter[current..].starts_with(']')) {
                    if filter[current..].starts_with(']') {
                        return Ok(if shard_ids.is_empty() { None } else { Some(shard_ids) });
                    }
                    current += 1;
                }
            }
        }

        Ok(None)
    }

    /// Extract plan from PK filter with AND.
    fn extract_pk_and(&self, filter: &str) -> Result<Option<QueryPlan>> {
        // Check if filter contains OR (not narrowable at top level)
        if filter.contains(" OR ") || filter.contains(" or ") {
            return Ok(None);
        }

        // Try to extract PK equality or IN from AND-clauses
        let parts: Vec<&str> = filter
            .split(" AND ")
            .flat_map(|s| s.split(" and "))
            .collect();

        for part in parts {
            let part = part.trim();
            if let Some(shard_id) = self.extract_pk_equality(part)? {
                return Ok(Some(QueryPlan {
                    narrowed: true,
                    reason: format!("pk filter with AND: {} = \"...\"", self.primary_key),
                    target_shards: vec![shard_id],
                    filter: Some(filter.to_string()),
                }));
            }
            if let Some(shard_ids) = self.extract_pk_in(part)? {
                return Ok(Some(QueryPlan {
                    narrowed: true,
                    reason: format!("pk filter with AND: {} IN [{} values]", self.primary_key, shard_ids.len()),
                    target_shards: shard_ids,
                    filter: Some(filter.to_string()),
                }));
            }
        }

        Ok(None)
    }

    /// Hash a primary key value to a shard ID.
    fn hash_to_shard(&self, pk: &str) -> u32 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        pk.hash(&mut hasher);
        (hasher.finish() as u32) % self.shard_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pk_equality_narrowing() {
        let planner = QueryPlanner::new("id".to_string(), 16, PlannerConfig::default());

        let plan = planner.plan(Some("id = \"test-doc\""));
        assert!(plan.narrowed);
        assert_eq!(plan.target_shards.len(), 1);
    }

    #[test]
    fn test_pk_in_narrowing() {
        let planner = QueryPlanner::new("id".to_string(), 16, PlannerConfig::default());

        let plan = planner.plan(Some("id IN [\"a\", \"b\", \"c\"]"));
        assert!(plan.narrowed);
        assert_eq!(plan.target_shards.len(), 3);
    }

    #[test]
    fn test_pk_and_narrowing() {
        let planner = QueryPlanner::new("id".to_string(), 16, PlannerConfig::default());

        let plan = planner.plan(Some("id = \"test\" AND category = \"books\""));
        assert!(plan.narrowed);
        assert_eq!(plan.target_shards.len(), 1);
    }

    #[test]
    fn test_or_not_narrowable() {
        let planner = QueryPlanner::new("id".to_string(), 16, PlannerConfig::default());

        let plan = planner.plan(Some("id = \"test\" OR id = \"other\""));
        assert!(!plan.narrowed);
        assert!(plan.reason.contains("no pk-constrained") || plan.target_shards.is_empty());
    }

    #[test]
    fn test_no_filter_not_narrowable() {
        let planner = QueryPlanner::new("id".to_string(), 16, PlannerConfig::default());

        let plan = planner.plan(None);
        assert!(!plan.narrowed);
    }
}
