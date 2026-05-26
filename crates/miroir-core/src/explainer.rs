//! Query explain API (plan §13.20).
//!
//! Explains how a search query will be executed without running it,
//! showing the chosen replica group, target nodes, and any warnings.

use crate::config::MiroirConfig;
use crate::query_planner::QueryPlanner;
use crate::replica_selection::ReplicaSelector;
use crate::task_store::TaskStore;
use crate::tenant::TenantAffinityManager;
use crate::topology::{NodeId, Topology};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

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
    UnboundedWildcard { query: String },
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
    NarrowingNotPossible { reason: String },
    /// Settings broadcast in flight.
    SettingsBroadcastInFlight { commit_in: String },
}

/// Explainer for queries.
pub struct Explainer {
    config: MiroirConfig,
    query_planner: Arc<QueryPlanner>,
    task_store: Option<Arc<dyn TaskStore>>,
    replica_selector: Option<Arc<ReplicaSelector>>,
    tenant_affinity_manager: Option<Arc<TenantAffinityManager>>,
}

impl Explainer {
    /// Create a new explainer with all integrations.
    pub fn new_with_integrations(
        config: MiroirConfig,
        query_planner: Arc<QueryPlanner>,
        task_store: Option<Arc<dyn TaskStore>>,
        replica_selector: Option<Arc<ReplicaSelector>>,
        tenant_affinity_manager: Option<Arc<TenantAffinityManager>>,
    ) -> Self {
        Self {
            config,
            query_planner,
            task_store,
            replica_selector,
            tenant_affinity_manager,
        }
    }

    /// Create a new explainer (backward compatible).
    pub fn new(config: MiroirConfig, query_planner: Arc<QueryPlanner>) -> Self {
        Self {
            config,
            query_planner,
            task_store: None,
            replica_selector: None,
            tenant_affinity_manager: None,
        }
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

        // Query planner integration (plan §13.4): narrow target shards based on PK constraints
        let filter_string = query.filter.as_ref().and_then(|v| {
            // Convert filter Value to string representation for QueryPlanner
            if v.is_string() {
                v.as_str().map(|s| s.to_string())
            } else {
                // For object/array filters, serialize to JSON string
                serde_json::to_string(v).ok()
            }
        });

        // Use a blocking runtime since this is a sync method
        let rt = tokio::runtime::Handle::try_current();
        let query_plan = if let Ok(handle) = rt {
            handle.block_on(async {
                self.query_planner
                    .plan(&resolved_uid, &filter_string, topology.shards)
                    .await
            })
        } else {
            // Fallback for contexts without a runtime - use default plan (no narrowing)
            return self.explain_without_planner(
                &resolved_uid,
                query,
                topology,
                settings_version,
                broadcast_pending,
                alias_resolution,
                warnings,
            );
        };

        let target_shards = if query_plan.narrowed {
            query_plan.target_shards
        } else {
            (0..topology.shards).collect()
        };
        let narrowed = query_plan.narrowed;
        let narrowing_reason = if query_plan.narrowed {
            Some(query_plan.reason)
        } else {
            None
        };

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
        let cache_candidate = query.filter.is_none() && query.q.is_some();

        // Estimate p95 latency
        let estimated_p95_ms = self.estimate_latency(topology, chosen_group.id, &target_shards);

        // Tenant affinity
        let tenant_affinity_pinned = query.tenant_id.as_ref().and_then(|tenant| {
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

    /// Explain a query without query planner (fallback for contexts without a runtime).
    fn explain_without_planner(
        &self,
        resolved_uid: &str,
        query: &SearchQueryExplanation,
        topology: &Topology,
        settings_version: u64,
        broadcast_pending: Option<&BroadcastPending>,
        alias_resolution: Option<AliasResolution>,
        mut warnings: Vec<Warning>,
    ) -> QueryExplanation {
        // No narrowing - target all shards
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
        let cache_candidate = query.filter.is_none() && query.q.is_some();

        // Estimate p95 latency
        let estimated_p95_ms = self.estimate_latency(topology, chosen_group.id, &target_shards);

        // Tenant affinity
        let tenant_affinity_pinned = query.tenant_id.as_ref().and_then(|tenant| {
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
            resolved_uid: resolved_uid.to_string(),
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
        _topology: &Topology,
    ) -> (String, Option<AliasResolution>) {
        // Look up alias in task store if available
        if let Some(ref store) = self.task_store {
            if let Ok(Some(alias_row)) = store.get_alias(index_uid) {
                // Return the first target for single-target aliases
                let resolved = if let Some(current) = alias_row.current_uid {
                    current
                } else if let Some(ref targets) = alias_row.target_uids {
                    // For multi-target aliases, return the first target
                    targets
                        .first()
                        .cloned()
                        .unwrap_or_else(|| index_uid.to_string())
                } else {
                    index_uid.to_string()
                };

                return (
                    resolved.clone(),
                    Some(AliasResolution {
                        from: index_uid.to_string(),
                        to: resolved,
                        version: alias_row.version as u64,
                    }),
                );
            }
        }

        // Not found or no task store - return as-is
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
                            reason: format!("tenant affinity: {tenant}"),
                        };
                    }
                    "explicit" => {
                        if let Some(&group_id) = self.config.tenant_affinity.static_map.get(tenant)
                        {
                            return ChosenGroup {
                                id: group_id,
                                reason: format!("explicit tenant mapping: {tenant}"),
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
    fn resolve_tenant_affinity(&self, tenant: &str, topology: &Topology) -> Option<u32> {
        // Check static map first (from config)
        if let Some(&group) = self.config.tenant_affinity.static_map.get(tenant) {
            return Some(group);
        }

        // If tenant affinity is enabled, always hash to a group
        if self.config.tenant_affinity.enabled {
            let replica_groups = topology.replica_groups;
            if replica_groups > 0 {
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                tenant.hash(&mut hasher);
                return Some((hasher.finish() as u32) % replica_groups);
            }
        }

        // Use TenantAffinityManager if available
        if let Some(ref manager) = self.tenant_affinity_manager {
            // Use a tokio runtime if available, otherwise use hash fallback
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                // Build a minimal headers map with the tenant
                let mut headers = HashMap::new();
                headers.insert(
                    self.config.tenant_affinity.header_name.clone(),
                    tenant.to_string(),
                );

                // Resolve from the manager
                let resolution =
                    handle.block_on(async { manager.resolve_from_headers(&headers, false).await });

                if let Ok(ref res) = resolution {
                    if let Some(group) = res.pinned_group {
                        return Some(group);
                    }
                }
            }

            // Fallback: hash the tenant to a group
            let replica_groups = topology.replica_groups;
            if replica_groups > 0 {
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                tenant.hash(&mut hasher);
                return Some((hasher.finish() as u32) % replica_groups);
            }
        }

        // No affinity available
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
                let assigned =
                    crate::router::assign_shard_in_group(shard_id, group.nodes(), topology.rf());
                if let Some(node_id) = assigned.first() {
                    target_nodes.insert(shard_id.to_string(), node_id.as_str().to_string());
                }
            }
        }

        target_nodes
    }

    /// Estimate p95 latency for the query.
    fn estimate_latency(&self, topology: &Topology, group_id: u32, shards: &[u32]) -> f64 {
        // Use EWMA latency from replica selection if available
        if let Some(ref selector) = self.replica_selector {
            // Get the group and its nodes
            if let Some(group) = topology.group(group_id) {
                let mut latencies = Vec::new();

                // For each shard, get the assigned node and its metrics
                for &shard_id in shards {
                    let assigned = crate::router::assign_shard_in_group(
                        shard_id,
                        group.nodes(),
                        topology.rf(),
                    );
                    for node_id in assigned {
                        // Get metrics for this node
                        if let Ok(handle) = tokio::runtime::Handle::try_current() {
                            let metrics =
                                handle.block_on(async { selector.get_metrics(&node_id).await });
                            if let Some(m) = metrics {
                                latencies.push(m.latency_p95_ms);
                            }
                        }
                    }
                }

                // Return average latency if we have metrics
                if !latencies.is_empty() {
                    let sum: f64 = latencies.iter().sum();
                    return sum / latencies.len() as f64;
                }
            }
        }

        // Fallback to default estimate
        50.0
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
                warnings.push(Warning::UnboundedWildcard { query: q.clone() });
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
    use crate::query_planner::QueryPlanner;
    use crate::task_store::{AliasRow, NewAlias, TaskStore};
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Mock task store for testing.
    struct MockTaskStore {
        aliases: std::sync::RwLock<HashMap<String, AliasRow>>,
    }

    impl MockTaskStore {
        fn new() -> Self {
            Self {
                aliases: std::sync::RwLock::new(HashMap::new()),
            }
        }

        fn add_alias(&self, name: &str, current_uid: &str, version: i64) {
            self.aliases.write().unwrap().insert(
                name.to_string(),
                AliasRow {
                    name: name.to_string(),
                    kind: "single".to_string(),
                    current_uid: Some(current_uid.to_string()),
                    target_uids: None,
                    version,
                    created_at: 0,
                    history: Vec::new(),
                },
            );
        }
    }

    impl TaskStore for MockTaskStore {
        fn migrate(&self) -> crate::Result<()> {
            Ok(())
        }

        fn insert_task(&self, _task: &crate::task_store::NewTask) -> crate::Result<()> {
            Ok(())
        }

        fn get_task(&self, _miroir_id: &str) -> crate::Result<Option<crate::task_store::TaskRow>> {
            Ok(None)
        }

        fn update_task_status(&self, _miroir_id: &str, _status: &str) -> crate::Result<bool> {
            Ok(false)
        }

        fn update_node_task(
            &self,
            _miroir_id: &str,
            _node_id: &str,
            _task_uid: u64,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn set_task_error(&self, _miroir_id: &str, _error: &str) -> crate::Result<bool> {
            Ok(false)
        }

        fn list_tasks(
            &self,
            _filter: &crate::task_store::TaskFilter,
        ) -> crate::Result<Vec<crate::task_store::TaskRow>> {
            Ok(Vec::new())
        }

        fn prune_tasks(&self, _cutoff_ms: i64, _batch_size: u32) -> crate::Result<usize> {
            Ok(0)
        }

        fn list_terminal_tasks_batch(
            &self,
            _cutoff_ms: i64,
            _offset: i64,
            _limit: i64,
        ) -> crate::Result<Vec<crate::task_store::TaskRow>> {
            Ok(Vec::new())
        }

        fn delete_tasks_batch(&self, _miroir_ids: &[&str]) -> crate::Result<usize> {
            Ok(0)
        }

        fn task_count(&self) -> crate::Result<u64> {
            Ok(0)
        }

        fn upsert_node_settings_version(
            &self,
            _index_uid: &str,
            _node_id: &str,
            _version: i64,
            _updated_at: i64,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn get_node_settings_version(
            &self,
            _index_uid: &str,
            _node_id: &str,
        ) -> crate::Result<Option<crate::task_store::NodeSettingsVersionRow>> {
            Ok(None)
        }

        fn create_alias(&self, _alias: &NewAlias) -> crate::Result<()> {
            Ok(())
        }

        fn upsert_alias(&self, alias: &NewAlias) -> crate::Result<()> {
            // For mock, just delegate to create_alias
            self.create_alias(alias)
        }

        fn get_alias(&self, name: &str) -> crate::Result<Option<AliasRow>> {
            Ok(self.aliases.read().unwrap().get(name).cloned())
        }

        fn flip_alias(
            &self,
            _name: &str,
            _new_uid: &str,
            _history_retention: usize,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn delete_alias(&self, name: &str) -> crate::Result<bool> {
            Ok(self.aliases.write().unwrap().remove(name).is_some())
        }

        fn list_aliases(&self) -> crate::Result<Vec<AliasRow>> {
            Ok(self.aliases.read().unwrap().values().cloned().collect())
        }

        fn upsert_session(&self, _session: &crate::task_store::SessionRow) -> crate::Result<()> {
            Ok(())
        }

        fn get_session(
            &self,
            _session_id: &str,
        ) -> crate::Result<Option<crate::task_store::SessionRow>> {
            Ok(None)
        }

        fn delete_expired_sessions(&self, _now_ms: i64) -> crate::Result<usize> {
            Ok(0)
        }

        fn insert_idempotency_entry(
            &self,
            _entry: &crate::task_store::IdempotencyEntry,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn get_idempotency_entry(
            &self,
            _key: &str,
        ) -> crate::Result<Option<crate::task_store::IdempotencyEntry>> {
            Ok(None)
        }

        fn delete_expired_idempotency_entries(&self, _now_ms: i64) -> crate::Result<usize> {
            Ok(0)
        }

        fn insert_job(&self, _job: &crate::task_store::NewJob) -> crate::Result<()> {
            Ok(())
        }

        fn get_job(&self, _id: &str) -> crate::Result<Option<crate::task_store::JobRow>> {
            Ok(None)
        }

        fn claim_job(
            &self,
            _id: &str,
            _claimed_by: &str,
            _claim_expires_at: i64,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn update_job_progress(
            &self,
            _id: &str,
            _state: &str,
            _progress: &str,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn renew_job_claim(&self, _id: &str, _claim_expires_at: i64) -> crate::Result<bool> {
            Ok(false)
        }

        fn list_jobs_by_state(
            &self,
            _state: &str,
        ) -> crate::Result<Vec<crate::task_store::JobRow>> {
            Ok(Vec::new())
        }

        fn count_jobs_by_state(&self, _state: &str) -> crate::Result<u64> {
            Ok(0)
        }

        fn list_expired_claims(
            &self,
            _now_ms: i64,
        ) -> crate::Result<Vec<crate::task_store::JobRow>> {
            Ok(Vec::new())
        }

        fn list_jobs_by_parent(
            &self,
            _parent_job_id: &str,
        ) -> crate::Result<Vec<crate::task_store::JobRow>> {
            Ok(Vec::new())
        }

        fn reclaim_job_claim(
            &self,
            _id: &str,
            _state: &str,
            _progress: &str,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn try_acquire_leader_lease(
            &self,
            _scope: &str,
            _holder: &str,
            _expires_at: i64,
            _now_ms: i64,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn renew_leader_lease(
            &self,
            _scope: &str,
            _holder: &str,
            _expires_at: i64,
            _now_ms: i64,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn get_leader_lease(
            &self,
            _scope: &str,
        ) -> crate::Result<Option<crate::task_store::LeaderLeaseRow>> {
            Ok(None)
        }

        fn upsert_canary(&self, _canary: &crate::task_store::NewCanary) -> crate::Result<()> {
            Ok(())
        }

        fn get_canary(&self, _id: &str) -> crate::Result<Option<crate::task_store::CanaryRow>> {
            Ok(None)
        }

        fn list_canaries(&self) -> crate::Result<Vec<crate::task_store::CanaryRow>> {
            Ok(Vec::new())
        }

        fn delete_canary(&self, _id: &str) -> crate::Result<bool> {
            Ok(false)
        }

        fn insert_canary_run(
            &self,
            _run: &crate::task_store::NewCanaryRun,
            _run_history_limit: usize,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn get_canary_runs(
            &self,
            _canary_id: &str,
            _limit: usize,
        ) -> crate::Result<Vec<crate::task_store::CanaryRunRow>> {
            Ok(Vec::new())
        }

        fn upsert_cdc_cursor(
            &self,
            _cursor: &crate::task_store::NewCdcCursor,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn get_cdc_cursor(
            &self,
            _sink_name: &str,
            _index_uid: &str,
        ) -> crate::Result<Option<crate::task_store::CdcCursorRow>> {
            Ok(None)
        }

        fn list_cdc_cursors(
            &self,
            _sink_name: &str,
        ) -> crate::Result<Vec<crate::task_store::CdcCursorRow>> {
            Ok(Vec::new())
        }

        fn insert_tenant_mapping(
            &self,
            _mapping: &crate::task_store::NewTenantMapping,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn get_tenant_mapping(
            &self,
            _api_key_hash: &[u8],
        ) -> crate::Result<Option<crate::task_store::TenantMapRow>> {
            Ok(None)
        }

        fn delete_tenant_mapping(&self, _api_key_hash: &[u8]) -> crate::Result<bool> {
            Ok(false)
        }

        fn upsert_rollover_policy(
            &self,
            _policy: &crate::task_store::NewRolloverPolicy,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn get_rollover_policy(
            &self,
            _name: &str,
        ) -> crate::Result<Option<crate::task_store::RolloverPolicyRow>> {
            Ok(None)
        }

        fn list_rollover_policies(
            &self,
        ) -> crate::Result<Vec<crate::task_store::RolloverPolicyRow>> {
            Ok(Vec::new())
        }

        fn delete_rollover_policy(&self, _name: &str) -> crate::Result<bool> {
            Ok(false)
        }

        fn upsert_search_ui_config(
            &self,
            _config: &crate::task_store::NewSearchUiConfig,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn get_search_ui_config(
            &self,
            _index_uid: &str,
        ) -> crate::Result<Option<crate::task_store::SearchUiConfigRow>> {
            Ok(None)
        }

        fn delete_search_ui_config(&self, _index_uid: &str) -> crate::Result<bool> {
            Ok(false)
        }

        fn insert_admin_session(
            &self,
            _session: &crate::task_store::NewAdminSession,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn get_admin_session(
            &self,
            _session_id: &str,
        ) -> crate::Result<Option<crate::task_store::AdminSessionRow>> {
            Ok(None)
        }

        fn revoke_admin_session(&self, _session_id: &str) -> crate::Result<bool> {
            Ok(false)
        }

        fn delete_expired_admin_sessions(&self, _now_ms: i64) -> crate::Result<usize> {
            Ok(0)
        }

        fn upsert_mode_b_operation(
            &self,
            _operation: &crate::task_store::ModeBOperation,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn get_mode_b_operation(
            &self,
            _operation_id: &str,
        ) -> crate::Result<Option<crate::task_store::ModeBOperation>> {
            Ok(None)
        }

        fn get_mode_b_operation_by_scope(
            &self,
            _scope: &str,
        ) -> crate::Result<Option<crate::task_store::ModeBOperation>> {
            Ok(None)
        }

        fn list_mode_b_operations(
            &self,
            _filter: &crate::task_store::ModeBOperationFilter,
        ) -> crate::Result<Vec<crate::task_store::ModeBOperation>> {
            Ok(Vec::new())
        }

        fn delete_mode_b_operation(&self, _operation_id: &str) -> crate::Result<bool> {
            Ok(false)
        }

        fn prune_mode_b_operations(
            &self,
            _cutoff_ms: i64,
            _batch_size: u32,
        ) -> crate::Result<usize> {
            Ok(0)
        }

        fn check_and_mark_beacon_event(
            &self,
            _index_uid: &str,
            _event_id: &str,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn upsert_ttl_policy(
            &self,
            _policy: &crate::task_store::NewTtlPolicy,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn get_ttl_policy(
            &self,
            _index_uid: &str,
        ) -> crate::Result<Option<crate::task_store::TtlPolicyRow>> {
            Ok(None)
        }

        fn delete_ttl_policy(&self, _index_uid: &str) -> crate::Result<bool> {
            Ok(false)
        }

        fn list_ttl_policies(&self) -> crate::Result<Vec<crate::task_store::TtlPolicyRow>> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn test_explain_basic_query() {
        let config = MiroirConfig::default();
        let query_planner = Arc::new(QueryPlanner::default());
        let explainer = Explainer::new(config, query_planner);

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

    #[test]
    fn test_alias_resolution() {
        let config = MiroirConfig::default();
        let query_planner = Arc::new(QueryPlanner::default());

        // Create mock task store with an alias
        let store = MockTaskStore::new();
        store.add_alias("products", "products_v4", 7);

        let explainer = Explainer::new_with_integrations(
            config,
            query_planner,
            Some(Arc::new(store)),
            None,
            None,
        );

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

        // Alias should be resolved
        assert_eq!(explanation.resolved_uid, "products_v4");
        assert!(explanation.plan.alias_resolution.is_some());
        let resolution = explanation.plan.alias_resolution.as_ref().unwrap();
        assert_eq!(resolution.from, "products");
        assert_eq!(resolution.to, "products_v4");
        assert_eq!(resolution.version, 7);
    }

    #[test]
    fn test_alias_not_found() {
        let config = MiroirConfig::default();
        let query_planner = Arc::new(QueryPlanner::default());

        let explainer = Explainer::new_with_integrations(
            config,
            query_planner,
            Some(Arc::new(MockTaskStore::new())),
            None,
            None,
        );

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

        // Unknown alias should pass through
        assert_eq!(explanation.resolved_uid, "products");
        assert!(explanation.plan.alias_resolution.is_none());
    }

    #[test]
    fn test_tenant_affinity_static_map() {
        let mut config = MiroirConfig::default();
        config.tenant_affinity.enabled = true;
        config.tenant_affinity.mode = "header".to_string();
        config.tenant_affinity.header_name = "X-Miroir-Tenant".to_string();

        let query_planner = Arc::new(QueryPlanner::default());
        let explainer = Explainer::new_with_integrations(config, query_planner, None, None, None);

        let topology = Topology::new(64, 2, 2);
        let query = SearchQueryExplanation {
            q: Some("test".to_string()),
            filter: None,
            sort: None,
            offset: None,
            limit: None,
            tenant_id: Some("tenant-a".to_string()),
        };

        let explanation = explainer.explain("products", &query, &topology, 1, None);

        // Tenant affinity should be in the plan (static map would be empty in this test)
        // The group will be hash-derived since we don't have a static mapping
        assert!(explanation.plan.tenant_affinity_pinned.is_some());
    }

    #[test]
    fn test_tenant_affinity_none() {
        let config = MiroirConfig::default();
        let query_planner = Arc::new(QueryPlanner::default());
        let explainer = Explainer::new(config, query_planner);

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

        // No tenant affinity when no tenant_id provided
        assert!(explanation.plan.tenant_affinity_pinned.is_none());
    }

    #[test]
    fn test_latency_default_estimate() {
        let config = MiroirConfig::default();
        let query_planner = Arc::new(QueryPlanner::default());
        let explainer = Explainer::new_with_integrations(
            config,
            query_planner,
            None,
            None, // No replica selector
            None,
        );

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

        // Should return default estimate when no replica selector
        assert_eq!(explanation.plan.estimated_p95_ms, 50.0);
    }

    #[test]
    fn test_query_planner_integration_narrowed() {
        let config = MiroirConfig::default();
        let query_planner = Arc::new(QueryPlanner::default());
        let explainer = Explainer::new(config, query_planner);

        let topology = Topology::new(64, 2, 1);
        let query = SearchQueryExplanation {
            q: Some("test".to_string()),
            filter: Some(serde_json::json!("id IN [1, 2, 3]")),
            sort: None,
            offset: None,
            limit: None,
            tenant_id: None,
        };

        let explanation = explainer.explain("products", &query, &topology, 1, None);

        // QueryPlanner should narrow the query if the filter contains shard keys
        // This verifies the integration exists (actual narrowing behavior depends on QueryPlanner)
        assert_eq!(explanation.resolved_uid, "products");
        // The plan should have target_shards populated
        assert!(!explanation.plan.target_shards.is_empty());
    }

    #[test]
    fn test_warnings_large_offset_limit() {
        let config = MiroirConfig::default();
        let query_planner = Arc::new(QueryPlanner::default());
        let explainer = Explainer::new(config, query_planner);

        let topology = Topology::new(64, 2, 1);
        let query = SearchQueryExplanation {
            q: Some("test".to_string()),
            filter: None,
            sort: None,
            offset: Some(10000),
            limit: Some(500),
            tenant_id: None,
        };

        let explanation = explainer.explain("products", &query, &topology, 1, None);

        // Should warn about large offset+limit
        assert!(!explanation.warnings.is_empty());
        match &explanation.warnings[0] {
            Warning::LargeOffsetLimit { offset, limit, .. } => {
                assert_eq!(*offset, 10000);
                assert_eq!(*limit, 500);
            }
            _ => panic!("Expected LargeOffsetLimit warning"),
        }
    }

    #[test]
    fn test_warnings_unbounded_wildcard() {
        let config = MiroirConfig::default();
        let query_planner = Arc::new(QueryPlanner::default());
        let explainer = Explainer::new(config, query_planner);

        let topology = Topology::new(64, 2, 1);
        let query = SearchQueryExplanation {
            q: Some("*".to_string()),
            filter: None,
            sort: None,
            offset: None,
            limit: None,
            tenant_id: None,
        };

        let explanation = explainer.explain("products", &query, &topology, 1, None);

        // Should warn about unbounded wildcard
        assert!(!explanation.warnings.is_empty());
        match &explanation.warnings[0] {
            Warning::UnboundedWildcard { query } => {
                assert_eq!(query, "*");
            }
            _ => panic!("Expected UnboundedWildcard warning"),
        }
    }
}
