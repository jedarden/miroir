//! Query explain API (plan §13.20).
//!
//! Explains how a search query will be executed without running it,
//! showing the chosen replica group, target nodes, and any warnings.

use crate::config::MiroirConfig;
use crate::router::shard_for_key;
use crate::topology::{Topology, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Query explanation response (plan §13.20).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExplanation {
    /// The resolved index UID (after alias resolution).
    pub resolved_uid: String,
    /// The execution plan.
    pub plan: ExplainPlan,
    /// Warnings about the query or configuration.
    pub warnings: Vec<Warning>,
}

/// Execution plan details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainPlan {
    /// Alias resolution, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias_resolution: Option<AliasResolution>,
    /// Whether the query was narrowed to a subset of shards.
    pub narrowed: bool,
    /// Reason for narrowing, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub narrowing_reason: Option<String>,
    /// Target shard IDs (empty if narrowed=false means all shards).
    pub target_shards: Vec<u32>,
    /// The chosen replica group for this query.
    pub chosen_group: ChosenGroup,
    /// Target node mapping: shard_id -> node_id.
    pub target_nodes: HashMap<String, String>,
    /// Whether hedging is armed for this query.
    pub hedging_armed: bool,
    /// Hedge trigger time in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hedge_trigger_ms: Option<u64>,
    /// Whether query coalescing is eligible.
    pub coalescing_eligible: bool,
    /// Whether this query is a cache candidate.
    pub cache_candidate: bool,
    /// Tenant affinity pinning, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_affinity_pinned: Option<GroupAffinity>,
    /// Estimated p95 latency in milliseconds.
    pub estimated_p95_ms: f64,
    /// Current settings version.
    pub settings_version: u64,
    /// Whether a settings broadcast is currently in flight.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broadcast_pending: Option<BroadcastPending>,
}

/// Alias resolution details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasResolution {
    /// The original alias name.
    pub from: String,
    /// The resolved index UID.
    pub to: String,
    /// The alias version.
    pub version: u64,
}

/// Chosen replica group details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChosenGroup {
    /// The group ID.
    pub id: u32,
    /// Why this group was chosen.
    pub reason: String,
}

/// Tenant affinity pinning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupAffinity {
    /// The tenant ID.
    pub tenant: String,
    /// The pinned group ID.
    pub group: u32,
}

/// Broadcast pending details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastPending {
    /// The proposed settings fingerprint.
    pub fingerprint: String,
    /// Expected time to commit (human-readable).
    pub commit_in: String,
}

/// Warning types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Warning {
    /// Filter references unfilterable attribute.
    UnfilterableAttribute {
        attribute: String,
        suggestion: String,
    },
    /// Very large offset+limit triggers per-shard over-fetch.
    LargeOffsetLimit {
        offset: usize,
        limit: usize,
        total: usize,
        suggestion: String,
    },
    /// Unbounded wildcard query.
    UnboundedWildcard {
        query: String,
    },
    /// Settings drift detected across nodes.
    SettingsDrift {
        index: String,
        versions: HashMap<NodeId, u64>,
    },
    /// Tenant affinity mismatch.
    TenantAffinityMismatch {
        tenant: String,
        expected_group: u32,
        actual_group: u32,
    },
    /// Shard-aware narrowing not possible.
    NarrowingNotPossible {
        reason: String,
    },
    /// Settings broadcast in flight.
    SettingsBroadcastInFlight {
        commit_in: String,
    },
}

/// Explainer for queries.
pub struct Explainer {
    config: MiroirConfig,
}

impl Explainer {
    /// Create a new explainer.
    pub fn new(config: MiroirConfig) -> Self {
        Self { config }
    }

    /// Explain a search query.
    ///
    /// Takes the same request body as `/search` but returns the plan
    /// without executing it.
    pub fn explain(
        &self,
        index_uid: &str,
        query: &SearchQueryExplanation,
        topology: &Topology,
        settings_version: u64,
        broadcast_pending: Option<&BroadcastPending>,
    ) -> QueryExplanation {
        let mut warnings = Vec::new();

        // Resolve alias (if applicable)
        let (resolved_uid, alias_resolution) = self.resolve_alias(index_uid, topology);

        // For now, we don't narrow queries - all shards are targeted
        // TODO: Integrate QueryPlanner when query planning is implemented
        let target_shards: Vec<u32> = (0..topology.shards).collect();
        let narrowed = false;
        let narrowing_reason: Option<String> = None;

        // Choose replica group
        let chosen_group = self.choose_group(topology, &query.tenant_id, settings_version);

        // Map shards to nodes
        let target_nodes = self.map_shards_to_nodes(&target_shards, chosen_group.id, topology);

        // Check for hedging
        let hedging_armed = self.config.hedging.enabled;
        let hedge_trigger_ms = if hedging_armed {
            Some(self.config.hedging.min_trigger_ms)
        } else {
            None
        };

        // Check coalescing eligibility
        let coalescing_eligible = self.config.query_coalescing.enabled;

        // Check cache candidate
        let cache_candidate = !query.filter.is_some() && query.q.is_some();

        // Estimate p95 latency
        let estimated_p95_ms = self.estimate_latency(topology, chosen_group.id, &target_shards);

        // Tenant affinity
        let tenant_affinity_pinned = query
            .tenant_id
            .as_ref()
            .and_then(|tenant| {
                self.resolve_tenant_affinity(tenant, topology)
                    .map(|group| GroupAffinity {
                        tenant: tenant.clone(),
                        group,
                    })
            });

        // Broadcast pending
        let broadcast_pending = broadcast_pending.cloned();

        // Add warnings based on query characteristics
        self.add_query_warnings(query, &mut warnings);

        QueryExplanation {
            resolved_uid,
            plan: ExplainPlan {
                alias_resolution,
                narrowed,
                narrowing_reason,
                target_shards,
                chosen_group,
                target_nodes,
                hedging_armed,
                hedge_trigger_ms,
                coalescing_eligible,
                cache_candidate,
                tenant_affinity_pinned,
                estimated_p95_ms,
                settings_version,
                broadcast_pending,
            },
            warnings,
        }
    }

    /// Resolve an alias to its target index.
    fn resolve_alias(
        &self,
        index_uid: &str,
        topology: &Topology,
    ) -> (String, Option<AliasResolution>) {
        // TODO: Look up alias in task store
        // For now, return as-is
        (index_uid.to_string(), None)
    }

    /// Choose a replica group for the query.
    fn choose_group(
        &self,
        topology: &Topology,
        tenant_id: &Option<String>,
        _settings_version: u64,
    ) -> ChosenGroup {
        if let Some(tenant) = tenant_id {
            if self.config.tenant_affinity.enabled {
                match self.config.tenant_affinity.mode.as_str() {
                    "header" => {
                        // Hash tenant to group
                        let group_id = self.hash_tenant_to_group(tenant, topology);
                        return ChosenGroup {
                            id: group_id,
                            reason: format!("tenant affinity: {}", tenant),
                        };
                    }
                    "explicit" => {
                        if let Some(&group_id) = self.config.tenant_affinity.static_map.get(tenant) {
                            return ChosenGroup {
                                id: group_id,
                                reason: format!("explicit tenant mapping: {}", tenant),
                            };
                        }
                    }
                    _ => {}
                }
            }
        }

        // Default: round-robin based on query sequence
        ChosenGroup {
            id: 0,
            reason: "default round-robin".to_string(),
        }
    }

    /// Hash a tenant ID to a replica group.
    fn hash_tenant_to_group(&self, tenant: &str, topology: &Topology) -> u32 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        tenant.hash(&mut hasher);
        (hasher.finish() as u32) % topology.replica_groups
    }

    /// Resolve tenant affinity to a group ID.
    fn resolve_tenant_affinity(&self, _tenant: &str, _topology: &Topology) -> Option<u32> {
        // TODO: Look up tenant mapping in task store
        None
    }

    /// Map target shards to their assigned nodes.
    fn map_shards_to_nodes(
        &self,
        shards: &[u32],
        group_id: u32,
        topology: &Topology,
    ) -> HashMap<String, String> {
        let mut target_nodes = HashMap::new();

        if let Some(group) = topology.group(group_id) {
            for &shard_id in shards {
                let assigned = crate::router::assign_shard_in_group(
                    shard_id,
                    group.nodes(),
                    topology.rf(),
                );
                if let Some(node_id) = assigned.first() {
                    target_nodes.insert(shard_id.to_string(), node_id.as_str().to_string());
                }
            }
        }

        target_nodes
    }

    /// Estimate p95 latency for the query.
    fn estimate_latency(&self, _topology: &Topology, _group_id: u32, _shards: &[u32]) -> f64 {
        // TODO: Use EWMA latency from replica selection
        50.0 // Default estimate
    }

    /// Add warnings based on query characteristics.
    fn add_query_warnings(&self, query: &SearchQueryExplanation, warnings: &mut Vec<Warning>) {
        // Check for very large offset+limit
        let total = query.offset.unwrap_or(0) + query.limit.unwrap_or(20);
        if total > 10000 {
            warnings.push(Warning::LargeOffsetLimit {
                offset: query.offset.unwrap_or(0),
                limit: query.limit.unwrap_or(20),
                total,
                suggestion: "consider cursor pagination for deep paging".to_string(),
            });
        }

        // Check for unbounded wildcard
        if let Some(ref q) = query.q {
            if q == "*" || q == "%" {
                warnings.push(Warning::UnboundedWildcard {
                    query: q.clone(),
                });
            }
        }
    }
}

/// Search query for explanation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQueryExplanation {
    /// Query string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub q: Option<String>,
    /// Filter expression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<serde_json::Value>,
    /// Sort criteria.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<serde_json::Value>>,
    /// Offset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    /// Limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Tenant ID (for affinity).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MiroirConfig;

    #[test]
    fn test_explain_basic_query() {
        let config = MiroirConfig::default();
        let explainer = Explainer::new(config);

        let topology = Topology::new(64, 2, 1);
        let query = SearchQueryExplanation {
            q: Some("test".to_string()),
            filter: None,
            sort: None,
            offset: None,
            limit: None,
            tenant_id: None,
        };

        let explanation = explainer.explain("products", &query, &topology, 1, None);

        assert_eq!(explanation.resolved_uid, "products");
        assert!(!explanation.plan.narrowed);
        assert_eq!(explanation.plan.target_shards.len(), 64);
    }
}
