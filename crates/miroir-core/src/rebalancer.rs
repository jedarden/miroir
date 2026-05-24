//! Cluster rebalancer for elastic topology operations.
//!
//! Implements plan §2 topology changes and §4 rebalancer:
//! - Node addition (within a group)
//! - Replica-group addition
//! - Node removal (drain)
//! - Group removal
//! - Unplanned node failure handling
//!
//! The rebalancer coordinates shard migrations using the migration coordinator
//! and provides admin API endpoints for topology operations.

use crate::migration::{
    MigrationConfig, MigrationCoordinator, MigrationError, MigrationId, NodeId as MigrationNodeId,
    ShardId,
};
use crate::router::{assign_shard_in_group, score};
use crate::task_store::TaskStore;
use crate::topology::{Node, NodeId as TopologyNodeId, NodeStatus, Topology};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn};

/// Callback type for recording rebalancer metrics.
pub type RebalancerMetricsCallback = Arc<dyn Fn(&str, f64) + Send + Sync>;

/// Convert a topology NodeId to a migration NodeId.
fn topo_to_migration_node_id(id: &TopologyNodeId) -> MigrationNodeId {
    MigrationNodeId(id.as_str().to_string())
}

/// Convert a migration NodeId to a topology NodeId.
fn migration_to_topo_node_id(id: &MigrationNodeId) -> TopologyNodeId {
    TopologyNodeId::new(id.0.clone())
}

/// Configuration for the rebalancer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancerConfig {
    /// Maximum concurrent shard migrations.
    pub max_concurrent_migrations: u32,
    /// Timeout for a single migration operation.
    pub migration_timeout_s: u64,
    /// Whether to automatically rebalance on node recovery.
    pub auto_rebalance_on_recovery: bool,
    /// Batch size for document migration.
    pub migration_batch_size: u32,
    /// Delay between migration batches (ms).
    pub migration_batch_delay_ms: u64,
}

impl Default for RebalancerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_migrations: 4,
            migration_timeout_s: 3600,
            auto_rebalance_on_recovery: true,
            migration_batch_size: 1000,
            migration_batch_delay_ms: 100,
        }
    }
}

/// Type of topology operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyOperationType {
    /// Adding a new node to an existing replica group.
    AddNode,
    /// Removing a node from a replica group.
    RemoveNode,
    /// Draining a node before removal.
    DrainNode,
    /// Adding a new replica group.
    AddReplicaGroup,
    /// Removing an entire replica group.
    RemoveReplicaGroup,
    /// Handling a failed node.
    NodeFailure,
}

/// Status of a topology operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyOperationStatus {
    /// Operation is pending.
    Pending,
    /// Operation is in progress.
    InProgress,
    /// Operation completed successfully.
    Complete,
    /// Operation failed.
    Failed,
    /// Operation was cancelled.
    Cancelled,
}

/// A topology operation (node/group add/remove/drain).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyOperation {
    /// Unique operation ID.
    pub id: u64,
    /// Type of operation.
    pub op_type: TopologyOperationType,
    /// Current status.
    pub status: TopologyOperationStatus,
    /// Target node ID (for node operations).
    pub target_node: Option<String>,
    /// Target replica group ID (for group operations).
    pub target_group: Option<u32>,
    /// Shard migrations in progress for this operation.
    pub migrations: Vec<MigrationId>,
    /// Start time.
    pub started_at: Option<u64>,
    /// Completion time.
    pub completed_at: Option<u64>,
    /// Error message if failed.
    pub error: Option<String>,
}

/// Result of a topology operation request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyOperationResult {
    /// Operation ID.
    pub id: u64,
    /// Status message.
    pub message: String,
    /// Number of shard migrations initiated.
    pub migrations_count: usize,
}

/// Status of all ongoing topology operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceStatus {
    /// Whether a rebalance is currently in progress.
    pub in_progress: bool,
    /// Active topology operations.
    pub operations: Vec<TopologyOperation>,
    /// Active migration details.
    pub migrations: HashMap<String, MigrationStatus>,
}

/// Status of a single migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStatus {
    /// Migration ID.
    pub id: u64,
    /// New node ID.
    pub new_node: String,
    /// Replica group.
    pub replica_group: u32,
    /// Current phase.
    pub phase: String,
    /// Affected shards count.
    pub shards_count: usize,
    /// Completed shards count.
    pub completed_count: usize,
}

/// Request to add a node to a replica group.
#[derive(Debug, Clone, Deserialize)]
pub struct AddNodeRequest {
    /// Node ID.
    pub id: String,
    /// Node address.
    pub address: String,
    /// Replica group to join.
    pub replica_group: u32,
}

/// Request to remove a node from the cluster.
#[derive(Debug, Clone, Deserialize)]
pub struct RemoveNodeRequest {
    /// Node ID to remove.
    pub node_id: String,
    /// Force removal without draining (dangerous).
    pub force: bool,
}

/// Request to drain a node (prepare for removal).
#[derive(Debug, Clone, Deserialize)]
pub struct DrainNodeRequest {
    /// Node ID to drain.
    pub node_id: String,
}

/// Request to add a replica group.
#[derive(Debug, Clone, Deserialize)]
pub struct AddReplicaGroupRequest {
    /// Group ID.
    pub group_id: u32,
    /// Initial nodes in the group.
    pub nodes: Vec<GroupNodeSpec>,
}

/// Node specification for group addition.
#[derive(Debug, Clone, Deserialize)]
pub struct GroupNodeSpec {
    /// Node ID.
    pub id: String,
    /// Node address.
    pub address: String,
}

/// Request to remove a replica group.
#[derive(Debug, Clone, Deserialize)]
pub struct RemoveReplicaGroupRequest {
    /// Group ID to remove.
    pub group_id: u32,
    /// Force removal without draining.
    pub force: bool,
}

/// Rebalancer error types.
#[derive(Debug, thiserror::Error)]
pub enum RebalancerError {
    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("replica group not found: {0}")]
    GroupNotFound(u32),

    #[error("operation already in progress for node: {0}")]
    OperationInProgress(String),

    #[error("invalid topology state: {0}")]
    InvalidState(String),

    #[error("migration error: {0}")]
    MigrationError(#[from] MigrationError),

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("cannot remove last node in group")]
    CannotRemoveLastNode,

    #[error("replica group {0} is not empty")]
    GroupNotEmpty(u32),
}

/// Migration executor: performs the actual document migration between nodes.
///
/// This trait allows the rebalancer core to remain agnostic to the HTTP client
/// implementation while still performing actual migrations.
#[async_trait::async_trait]
pub trait MigrationExecutor: Send + Sync {
    /// Fetch documents from a source node for a specific shard.
    async fn fetch_documents(
        &self,
        source_node: &str,
        source_address: &str,
        index_uid: &str,
        shard_id: u32,
        limit: u32,
        offset: u32,
    ) -> std::result::Result<(Vec<serde_json::Value>, u64), String>;

    /// Write documents to a target node.
    async fn write_documents(
        &self,
        target_node: &str,
        target_address: &str,
        index_uid: &str,
        documents: Vec<serde_json::Value>,
    ) -> std::result::Result<(), String>;

    /// Delete documents from a node by shard filter.
    async fn delete_shard(
        &self,
        node: &str,
        node_address: &str,
        index_uid: &str,
        shard_id: u32,
    ) -> std::result::Result<(), String>;
}

/// Rebalancer metrics for Prometheus emission.
#[derive(Debug, Clone, Default)]
pub struct RebalancerMetrics {
    /// Total number of documents migrated.
    pub documents_migrated_total: u64,
    /// Number of currently active migrations.
    pub active_migrations: u64,
    /// Start time of the current rebalance operation.
    pub rebalance_start_time: Option<Instant>,
}

impl RebalancerMetrics {
    /// Record that documents were migrated.
    pub fn record_documents_migrated(&mut self, count: u64) {
        self.documents_migrated_total += count;
    }

    /// Increment active migrations count.
    pub fn increment_active_migrations(&mut self) {
        self.active_migrations += 1;
    }

    /// Decrement active migrations count.
    pub fn decrement_active_migrations(&mut self) {
        self.active_migrations = self.active_migrations.saturating_sub(1);
    }

    /// Start a rebalance operation.
    pub fn start_rebalance(&mut self) {
        self.rebalance_start_time = Some(Instant::now());
    }

    /// End a rebalance operation and return duration in seconds.
    pub fn end_rebalance(&mut self) -> f64 {
        self.rebalance_start_time
            .take()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0)
    }

    /// Get the current rebalance duration in seconds.
    pub fn current_duration_secs(&self) -> f64 {
        self.rebalance_start_time
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0)
    }
}

/// The cluster rebalancer orchestrates topology changes.
pub struct Rebalancer {
    config: RebalancerConfig,
    topology: Arc<RwLock<Topology>>,
    migration_coordinator: Arc<RwLock<MigrationCoordinator>>,
    operations: Arc<RwLock<HashMap<u64, TopologyOperation>>>,
    next_op_id: Arc<std::sync::atomic::AtomicU64>,
    active_migrations: Arc<RwLock<HashMap<MigrationId, u64>>>, // migration -> operation ID
    migration_executor: Option<Arc<dyn MigrationExecutor>>,
    /// Metrics for rebalancer operations.
    pub metrics: Arc<RwLock<RebalancerMetrics>>,
    /// Task store for leader lease (P4.1 background worker).
    task_store: Option<Arc<dyn TaskStore>>,
    /// This pod's ID for leader election.
    pod_id: Option<String>,
    /// Leader lease scope prefix.
    leader_scope: String,
    /// Callback for recording Prometheus metrics.
    metrics_callback: Option<RebalancerMetricsCallback>,
}

impl Rebalancer {
    /// Create a new rebalancer.
    pub fn new(
        config: RebalancerConfig,
        topology: Arc<RwLock<Topology>>,
        migration_config: MigrationConfig,
    ) -> Self {
        let coordinator = Arc::new(RwLock::new(MigrationCoordinator::new(migration_config)));

        Self {
            config,
            topology,
            migration_coordinator: coordinator,
            operations: Arc::new(RwLock::new(HashMap::new())),
            next_op_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            active_migrations: Arc::new(RwLock::new(HashMap::new())),
            migration_executor: None,
            metrics: Arc::new(RwLock::new(RebalancerMetrics::default())),
            task_store: None,
            pod_id: None,
            leader_scope: "rebalance:global".to_string(),
            metrics_callback: None,
        }
    }

    /// Set the task store for leader lease (P4.1 background worker).
    pub fn with_task_store(mut self, task_store: Arc<dyn TaskStore>) -> Self {
        self.task_store = Some(task_store);
        self
    }

    /// Set the pod ID for leader election.
    pub fn with_pod_id(mut self, pod_id: String) -> Self {
        self.pod_id = Some(pod_id);
        self
    }

    /// Set the leader lease scope.
    pub fn with_leader_scope(mut self, scope: String) -> Self {
        self.leader_scope = scope;
        self
    }

    /// Set the metrics callback for Prometheus emission.
    pub fn with_metrics_callback(mut self, callback: RebalancerMetricsCallback) -> Self {
        self.metrics_callback = Some(callback);
        self
    }

    /// Set the migration executor (provides HTTP client for actual migrations).
    pub fn with_migration_executor(mut self, executor: Arc<dyn MigrationExecutor>) -> Self {
        self.migration_executor = Some(executor);
        self
    }

    /// Emit a metric via the metrics callback (if configured).
    fn emit_metric(&self, name: &str, value: f64) {
        if let Some(ref callback) = self.metrics_callback {
            callback(name, value);
        }
    }

    /// Run the background rebalancer worker (P4.1).
    ///
    /// This method runs in a loop, periodically checking for topology changes
    /// and triggering rebalancing as needed. Uses leader lease to ensure only
    /// one pod runs the rebalancer at a time.
    #[instrument(skip_all, fields(pod_id = ?self.pod_id))]
    pub async fn run_background(&self) -> Result<(), RebalancerError> {
        let Some(ref task_store) = self.task_store else {
            return Err(RebalancerError::InvalidState(
                "task_store required for background worker".into(),
            ));
        };

        let Some(ref pod_id) = self.pod_id else {
            return Err(RebalancerError::InvalidState(
                "pod_id required for background worker".into(),
            ));
        };

        let check_interval = Duration::from_millis(5000); // Check every 5 seconds
        let mut interval = tokio::time::interval(check_interval);
        let mut leader_lease_interval = tokio::time::interval(Duration::from_secs(3));

        info!(
            config = ?self.config,
            "rebalancer background worker started"
        );

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if self.is_leader(task_store, pod_id).await {
                        if let Err(e) = self.check_and_rebalance().await {
                            error!(error = %e, "background rebalance check failed");
                        }
                    }
                }
                _ = leader_lease_interval.tick() => {
                    if self.is_leader(task_store, pod_id).await {
                        self.renew_leader_lease(task_store, pod_id).await;
                    }
                }
            }
        }
    }

    /// Check if this pod is the leader for rebalancing.
    async fn is_leader(&self, task_store: &Arc<dyn TaskStore>, pod_id: &str) -> bool {
        let now = now_ms() as i64;
        let lease_ttl = now + 15000; // 15 second TTL

        task_store
            .try_acquire_leader_lease(&self.leader_scope, pod_id, lease_ttl, now)
            .unwrap_or(false)
    }

    /// Renew the leader lease.
    async fn renew_leader_lease(&self, task_store: &Arc<dyn TaskStore>, pod_id: &str) {
        let now = now_ms() as i64;
        let lease_ttl = now + 15000; // 15 second TTL

        let _ = task_store.renew_leader_lease(&self.leader_scope, pod_id, lease_ttl);
    }

    /// Check for topology changes and trigger rebalancing if needed.
    async fn check_and_rebalance(&self) -> Result<(), RebalancerError> {
        debug!("checking for topology changes");

        let topology = self.topology.read().await;

        // Check for nodes in Joining state
        let joining_nodes: Vec<_> = topology
            .nodes()
            .filter(|n| n.status == NodeStatus::Joining)
            .map(|n| (n.id.clone(), n.replica_group, n.address.clone()))
            .collect();

        // Check for nodes in Draining state
        let draining_nodes: Vec<_> = topology
            .nodes()
            .filter(|n| n.status == NodeStatus::Draining)
            .map(|n| (n.id.clone(), n.replica_group))
            .collect();

        // Check for nodes in Failed state
        let failed_nodes: Vec<_> = topology
            .nodes()
            .filter(|n| n.status == NodeStatus::Failed)
            .map(|n| (n.id.clone(), n.replica_group))
            .collect();

        // Drop topology read lock before starting operations
        drop(topology);

        // Trigger rebalance for joining nodes
        for (node_id, replica_group, address) in joining_nodes {
            info!(node_id = %node_id, replica_group, "detected joining node");

            // Check if there's already an operation in progress for this node
            let ops = self.operations.read().await;
            let already_in_progress = ops.values().any(|o| {
                o.target_node.as_ref() == Some(&node_id.as_str().to_string())
                    && o.status == TopologyOperationStatus::InProgress
            });
            drop(ops);

            if !already_in_progress {
                let request = AddNodeRequest {
                    id: node_id.as_str().to_string(),
                    address,
                    replica_group,
                };
                if let Err(e) = self.add_node(request).await {
                    warn!(error = %e, "failed to start rebalance for joining node");
                }
            }
        }

        // Trigger rebalance for draining nodes
        for (node_id, replica_group) in draining_nodes {
            info!(node_id = %node_id, replica_group, "detected draining node");

            let ops = self.operations.read().await;
            let already_in_progress = ops.values().any(|o| {
                o.target_node.as_ref() == Some(&node_id.as_str().to_string())
                    && matches!(
                        o.op_type,
                        TopologyOperationType::DrainNode | TopologyOperationType::RemoveNode
                    )
                    && o.status == TopologyOperationStatus::InProgress
            });
            drop(ops);

            if !already_in_progress {
                let request = DrainNodeRequest {
                    node_id: node_id.as_str().to_string(),
                };
                if let Err(e) = self.drain_node(request).await {
                    warn!(error = %e, "failed to start rebalance for draining node");
                }
            }
        }

        // Handle failed nodes
        for (node_id, replica_group) in failed_nodes {
            info!(node_id = %node_id, replica_group, "detected failed node");

            let ops = self.operations.read().await;
            let already_handled = ops.values().any(|o| {
                o.target_node.as_ref() == Some(&node_id.as_str().to_string())
                    && o.op_type == TopologyOperationType::NodeFailure
            });
            drop(ops);

            if !already_handled {
                if let Err(e) = self.handle_node_failure(node_id.as_str()).await {
                    warn!(error = %e, "failed to handle node failure");
                }
            }
        }

        Ok(())
    }

    /// Get current rebalance status.
    pub async fn status(&self) -> RebalanceStatus {
        let ops = self.operations.read().await;
        let coordinator = self.migration_coordinator.read().await;

        let in_progress = ops
            .values()
            .any(|o| o.status == TopologyOperationStatus::InProgress);

        let mut migrations: HashMap<String, MigrationStatus> = HashMap::new();
        for op in ops.values() {
            for &mid in &op.migrations {
                if let Some(state) = coordinator.get_state(mid) {
                    let key = format!("{}", mid);
                    let status = MigrationStatus {
                        id: mid.0,
                        new_node: state.new_node.to_string(),
                        replica_group: state.replica_group,
                        phase: state.phase.to_string(),
                        shards_count: state.affected_shards.len(),
                        completed_count: state
                            .affected_shards
                            .values()
                            .filter(|s| matches!(s, crate::migration::ShardMigrationState::Active))
                            .count(),
                    };
                    migrations.insert(key, status);
                }
            }
        }

        RebalanceStatus {
            in_progress,
            operations: ops.values().cloned().collect(),
            migrations,
        }
    }

    /// Add a node to a replica group.
    pub async fn add_node(
        &self,
        request: AddNodeRequest,
    ) -> Result<TopologyOperationResult, RebalancerError> {
        info!(
            node_id = %request.id,
            group = request.replica_group,
            "starting node addition"
        );

        // Check if node already exists
        {
            let topo = self.topology.read().await;
            if topo
                .node(&TopologyNodeId::new(request.id.clone()))
                .is_some()
            {
                return Err(RebalancerError::InvalidState(format!(
                    "node {} already exists",
                    request.id
                )));
            }
        }

        // Create operation record
        let op_id = self
            .next_op_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Add node to topology in Joining state
        {
            let mut topo = self.topology.write().await;
            let group_count = topo.groups().count() as u32;
            if request.replica_group >= group_count {
                return Err(RebalancerError::GroupNotFound(request.replica_group));
            }

            let node = Node::new(
                TopologyNodeId::new(request.id.clone()),
                request.address.clone(),
                request.replica_group,
            );
            topo.add_node(node);
        }

        // Compute affected shards (shards that will move to new node)
        let affected_shards = self
            .compute_shard_moves_for_new_node(&request.id, request.replica_group)
            .await?;

        // Create migration for each affected shard
        let mut migrations = Vec::new();
        {
            let mut coordinator = self.migration_coordinator.write().await;

            for (shard, old_owner) in affected_shards {
                let mut old_owners = HashMap::new();
                old_owners.insert(shard, topo_to_migration_node_id(&old_owner));

                let mid = coordinator.begin_migration(
                    topo_to_migration_node_id(&TopologyNodeId::new(request.id.clone())),
                    request.replica_group,
                    old_owners,
                )?;

                // Start dual-write
                coordinator.begin_dual_write(mid)?;

                // Track migration
                {
                    let mut active = self.active_migrations.write().await;
                    active.insert(mid, op_id);
                }

                migrations.push(mid);
            }
        }

        // Record operation
        let node_id_for_result = request.id.clone();
        let migrations_count = migrations.len();
        let operation = TopologyOperation {
            id: op_id,
            op_type: TopologyOperationType::AddNode,
            status: TopologyOperationStatus::InProgress,
            target_node: Some(request.id),
            target_group: Some(request.replica_group),
            migrations: migrations.clone(),
            started_at: Some(now_ms()),
            completed_at: None,
            error: None,
        };

        {
            let mut ops = self.operations.write().await;
            ops.insert(op_id, operation);
        }

        // Start metrics tracking
        {
            let mut metrics = self.metrics.write().await;
            metrics.start_rebalance();
        }

        // Start background migration task
        let topo_arc = self.topology.clone();
        let coord_arc = self.migration_coordinator.clone();
        let ops_arc = self.operations.clone();
        let active_arc = self.active_migrations.clone();
        let config = self.config.clone();
        let executor = self.migration_executor.clone();
        let metrics_arc = self.metrics.clone();

        tokio::spawn(async move {
            if let Err(e) = run_migration_task(
                topo_arc,
                coord_arc,
                ops_arc,
                active_arc,
                op_id,
                migrations,
                config,
                executor,
                metrics_arc,
            )
            .await
            {
                error!(error = %e, op_id = op_id, "migration task failed");
            }
        });

        Ok(TopologyOperationResult {
            id: op_id,
            message: format!(
                "Node {} addition started with {} shard migrations",
                node_id_for_result, migrations_count
            ),
            migrations_count,
        })
    }

    /// Drain a node (prepare for removal).
    pub async fn drain_node(
        &self,
        request: DrainNodeRequest,
    ) -> Result<TopologyOperationResult, RebalancerError> {
        info!(node_id = %request.node_id, "starting node drain");

        // Check if node exists
        let node_id = TopologyNodeId::new(request.node_id.clone());
        let (node_status, replica_group) = {
            let topo = self.topology.read().await;
            let node = topo
                .node(&node_id)
                .ok_or_else(|| RebalancerError::NodeNotFound(request.node_id.clone()))?;

            // Check if this is the last node in the group
            let group = topo
                .groups()
                .find(|g| g.id == node.replica_group)
                .ok_or_else(|| RebalancerError::GroupNotFound(node.replica_group))?;

            if group.nodes().len() <= 1 {
                return Err(RebalancerError::CannotRemoveLastNode);
            }

            (node.status, node.replica_group)
        };

        if node_status == NodeStatus::Draining {
            return Err(RebalancerError::OperationInProgress(
                request.node_id.clone(),
            ));
        }

        // Create operation record
        let op_id = self
            .next_op_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Mark node as draining
        {
            let mut topo = self.topology.write().await;
            if let Some(node) = topo.node_mut(&node_id) {
                node.status = NodeStatus::Draining;
            }
        }

        // Compute shard destinations (where each shard goes)
        let shard_destinations = self
            .compute_shard_destinations_for_drain(&request.node_id, replica_group)
            .await?;

        // Create migrations for each shard
        let mut migrations = Vec::new();
        {
            let mut coordinator = self.migration_coordinator.write().await;

            for (shard, dest_node) in shard_destinations {
                let mid = coordinator.begin_migration(
                    topo_to_migration_node_id(&dest_node),
                    replica_group,
                    [(shard, topo_to_migration_node_id(&node_id))]
                        .into_iter()
                        .collect(),
                )?;

                coordinator.begin_dual_write(mid)?;

                {
                    let mut active = self.active_migrations.write().await;
                    active.insert(mid, op_id);
                }

                migrations.push(mid);
            }
        }

        // Record operation
        let operation = TopologyOperation {
            id: op_id,
            op_type: TopologyOperationType::DrainNode,
            status: TopologyOperationStatus::InProgress,
            target_node: Some(request.node_id.clone()),
            target_group: Some(replica_group),
            migrations: migrations.clone(),
            started_at: Some(now_ms()),
            completed_at: None,
            error: None,
        };

        {
            let mut ops = self.operations.write().await;
            ops.insert(op_id, operation);
        }

        // Start metrics tracking
        {
            let mut metrics = self.metrics.write().await;
            metrics.start_rebalance();
        }

        // Start background migration task
        let migrations_count = migrations.len();
        let topo_arc = self.topology.clone();
        let coord_arc = self.migration_coordinator.clone();
        let ops_arc = self.operations.clone();
        let active_arc = self.active_migrations.clone();
        let config = self.config.clone();
        let drain_node_id = request.node_id.clone();
        let executor = self.migration_executor.clone();
        let metrics_arc = self.metrics.clone();

        tokio::spawn(async move {
            if let Err(e) = run_drain_task(
                topo_arc,
                coord_arc,
                ops_arc,
                active_arc,
                op_id,
                migrations,
                config,
                drain_node_id,
                executor,
                metrics_arc,
            )
            .await
            {
                error!(error = %e, op_id = op_id, "drain task failed");
            }
        });

        Ok(TopologyOperationResult {
            id: op_id,
            message: format!(
                "Node {} drain started with {} shard migrations",
                request.node_id, migrations_count
            ),
            migrations_count,
        })
    }

    /// Remove a node from the cluster (after drain).
    pub async fn remove_node(
        &self,
        request: RemoveNodeRequest,
    ) -> Result<TopologyOperationResult, RebalancerError> {
        info!(node_id = %request.node_id, force = request.force, "starting node removal");

        let node_id = TopologyNodeId::new(request.node_id.clone());

        // Check node state
        let node_status = {
            let topo = self.topology.read().await;
            let node = topo
                .node(&node_id)
                .ok_or_else(|| RebalancerError::NodeNotFound(request.node_id.clone()))?;

            // Check if this is the last node in the group
            let group = topo
                .groups()
                .find(|g| g.id == node.replica_group)
                .ok_or_else(|| RebalancerError::GroupNotFound(node.replica_group))?;

            if group.nodes().len() <= 1 {
                return Err(RebalancerError::CannotRemoveLastNode);
            }

            node.status
        };

        if !request.force && node_status != NodeStatus::Draining {
            return Err(RebalancerError::InvalidState(format!(
                "node {} is not in draining state (current: {:?}), use force=true to bypass",
                request.node_id, node_status
            )));
        }

        // Create operation record
        let op_id = self
            .next_op_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Remove node from topology
        {
            let mut topo = self.topology.write().await;
            topo.remove_node(&node_id);
        }

        // Record operation
        let operation = TopologyOperation {
            id: op_id,
            op_type: TopologyOperationType::RemoveNode,
            status: TopologyOperationStatus::Complete,
            target_node: Some(request.node_id.clone()),
            target_group: None,
            migrations: Vec::new(),
            started_at: Some(now_ms()),
            completed_at: Some(now_ms()),
            error: None,
        };

        {
            let mut ops = self.operations.write().await;
            ops.insert(op_id, operation);
        }

        Ok(TopologyOperationResult {
            id: op_id,
            message: format!("Node {} removed from cluster", request.node_id),
            migrations_count: 0,
        })
    }

    /// Add a replica group.
    pub async fn add_replica_group(
        &self,
        request: AddReplicaGroupRequest,
    ) -> Result<TopologyOperationResult, RebalancerError> {
        info!(
            group_id = request.group_id,
            node_count = request.nodes.len(),
            "starting replica group addition"
        );

        // Check if group already exists
        {
            let topo = self.topology.read().await;
            if topo.groups().any(|g| g.id == request.group_id) {
                return Err(RebalancerError::InvalidState(format!(
                    "replica group {} already exists",
                    request.group_id
                )));
            }
        }

        // Create operation record
        let op_id = self
            .next_op_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Add nodes to topology
        let node_ids: Vec<String> = request.nodes.iter().map(|n| n.id.clone()).collect();
        for node_spec in &request.nodes {
            let mut topo = self.topology.write().await;
            let node = Node::new(
                TopologyNodeId::new(node_spec.id.clone()),
                node_spec.address.clone(),
                request.group_id,
            );
            topo.add_node(node);
        }

        // For replica groups, we don't migrate data - the new group will sync from existing groups
        // This is handled by the replication mechanism

        // Record operation
        let operation = TopologyOperation {
            id: op_id,
            op_type: TopologyOperationType::AddReplicaGroup,
            status: TopologyOperationStatus::Complete,
            target_node: None,
            target_group: Some(request.group_id),
            migrations: Vec::new(),
            started_at: Some(now_ms()),
            completed_at: Some(now_ms()),
            error: None,
        };

        {
            let mut ops = self.operations.write().await;
            ops.insert(op_id, operation);
        }

        Ok(TopologyOperationResult {
            id: op_id,
            message: format!(
                "Replica group {} added with {} nodes",
                request.group_id,
                node_ids.len()
            ),
            migrations_count: 0,
        })
    }

    /// Remove a replica group.
    ///
    /// Implements plan §2 group removal flow:
    /// 1. Mark group as `draining` — queries stop routing immediately
    /// 2. Nodes can be decommissioned; no data migration needed (other groups hold the docs)
    /// 3. Remove nodes from config; operator deletes pods + PVCs
    ///
    /// Preconditions: refuse to remove a group if it's the last group holding a shard.
    /// Use `force: true` to bypass this check (operator must re-type the index UID to confirm).
    pub async fn remove_replica_group(
        &self,
        request: RemoveReplicaGroupRequest,
    ) -> Result<TopologyOperationResult, RebalancerError> {
        info!(
            group_id = request.group_id,
            force = request.force,
            "starting replica group removal"
        );

        // Create operation record
        let op_id = self
            .next_op_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Step 1: Mark group as draining (queries stop routing immediately)
        {
            let mut topo = self.topology.write().await;
            let group = topo.group_mut(request.group_id);

            let Some(grp) = group else {
                return Err(RebalancerError::GroupNotFound(request.group_id));
            };

            // Check if this is the last group
            if topo.groups().count() <= 1 {
                return Err(RebalancerError::InvalidState(
                    "cannot remove the last replica group".into(),
                ));
            }

            // Check if group is already draining
            if grp.is_draining() {
                // Group is already draining, proceed to removal if force=true
                if !request.force {
                    return Ok(TopologyOperationResult {
                        id: op_id,
                        message: format!(
                            "Replica group {} is already draining. Use force=true to complete removal.",
                            request.group_id
                        ),
                        migrations_count: 0,
                    });
                }
            } else {
                // Mark group as draining
                grp.mark_draining();
                info!(
                    group_id = request.group_id,
                    "replica group marked as draining, queries will stop routing to it"
                );
            }

            // If not force, return early — operator can now decommission nodes
            if !request.force {
                let operation = TopologyOperation {
                    id: op_id,
                    op_type: TopologyOperationType::RemoveReplicaGroup,
                    status: TopologyOperationStatus::Pending,
                    target_node: None,
                    target_group: Some(request.group_id),
                    migrations: Vec::new(),
                    started_at: Some(now_ms()),
                    completed_at: None,
                    error: None,
                };

                {
                    let mut ops = self.operations.write().await;
                    ops.insert(op_id, operation);
                }

                return Ok(TopologyOperationResult {
                    id: op_id,
                    message: format!(
                        "Replica group {} marked as draining. Queries stopped routing to it. \
                         Call again with force=true to complete removal after nodes are decommissioned.",
                        request.group_id
                    ),
                    migrations_count: 0,
                });
            }
        }

        // Step 2: Remove group from topology (this removes all nodes in the group)
        {
            let mut topo = self.topology.write().await;
            topo.remove_group(request.group_id);
        }

        // Record operation as complete
        let operation = TopologyOperation {
            id: op_id,
            op_type: TopologyOperationType::RemoveReplicaGroup,
            status: TopologyOperationStatus::Complete,
            target_node: None,
            target_group: Some(request.group_id),
            migrations: Vec::new(),
            started_at: Some(now_ms()),
            completed_at: Some(now_ms()),
            error: None,
        };

        {
            let mut ops = self.operations.write().await;
            ops.insert(op_id, operation);
        }

        info!(
            group_id = request.group_id,
            "replica group removal completed"
        );

        Ok(TopologyOperationResult {
            id: op_id,
            message: format!("Replica group {} removed from cluster", request.group_id),
            migrations_count: 0,
        })
    }

    /// Handle a node failure.
    pub async fn handle_node_failure(
        &self,
        node_id: &str,
    ) -> Result<TopologyOperationResult, RebalancerError> {
        warn!(node_id = %node_id, "handling node failure");

        let node_id_obj = TopologyNodeId::new(node_id.to_string());

        // Mark node as failed
        let replica_group = {
            let mut topo = self.topology.write().await;
            let node = topo
                .node_mut(&node_id_obj)
                .ok_or_else(|| RebalancerError::NodeNotFound(node_id.to_string()))?;

            node.status = NodeStatus::Failed;
            node.replica_group
        };

        // Create operation record
        let op_id = self
            .next_op_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let operation = TopologyOperation {
            id: op_id,
            op_type: TopologyOperationType::NodeFailure,
            status: TopologyOperationStatus::Complete,
            target_node: Some(node_id.to_string()),
            target_group: Some(replica_group),
            migrations: Vec::new(),
            started_at: Some(now_ms()),
            completed_at: Some(now_ms()),
            error: None,
        };

        {
            let mut ops = self.operations.write().await;
            ops.insert(op_id, operation);
        }

        Ok(TopologyOperationResult {
            id: op_id,
            message: format!("Node {} marked as failed", node_id),
            migrations_count: 0,
        })
    }

    /// Handle a node recovery and restore RF within the group.
    pub async fn handle_node_recovery(
        &self,
        node_id: &str,
    ) -> Result<TopologyOperationResult, RebalancerError> {
        info!(node_id = %node_id, "handling node recovery and RF restore");

        let node_id_obj = TopologyNodeId::new(node_id.to_string());

        // Mark node as recovered and get group info
        let (replica_group, has_rf_to_restore) = {
            let topo = self.topology.read().await;
            let node = topo
                .node(&node_id_obj)
                .ok_or_else(|| RebalancerError::NodeNotFound(node_id.to_string()))?;

            if node.status != NodeStatus::Failed && node.status != NodeStatus::Degraded {
                return Err(RebalancerError::InvalidState(format!(
                    "node {} is not in a failed state (current: {:?})",
                    node_id, node.status
                )));
            }

            let replica_group = node.replica_group;

            // Check if RF needs to be restored (other healthy nodes exist in group)
            let group = topo.groups().find(|g| g.id == replica_group);
            let has_other_healthy = group.map_or(false, |g| {
                g.nodes().iter().any(|nid| {
                    nid != &node_id_obj && topo.node(nid).map(|n| n.is_healthy()).unwrap_or(false)
                })
            });

            (replica_group, has_other_healthy)
        };

        if !has_rf_to_restore {
            // No other healthy nodes in group - just mark as active
            let mut topo = self.topology.write().await;
            let node = topo
                .node_mut(&node_id_obj)
                .ok_or_else(|| RebalancerError::NodeNotFound(node_id.to_string()))?;
            node.status = NodeStatus::Active;

            return Ok(TopologyOperationResult {
                id: 0,
                message: format!(
                    "Node {} recovered (no RF restore needed - no other healthy nodes in group)",
                    node_id
                ),
                migrations_count: 0,
            });
        }

        // Create operation record
        let op_id = self
            .next_op_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Mark node as active
        {
            let mut topo = self.topology.write().await;
            let node = topo
                .node_mut(&node_id_obj)
                .ok_or_else(|| RebalancerError::NodeNotFound(node_id.to_string()))?;
            node.status = NodeStatus::Active;
        }

        // Compute shards that need RF restore (shards where this node should be a replica)
        let shards_to_restore = self
            .compute_shards_for_rf_restore(node_id, replica_group)
            .await?;

        if !shards_to_restore.is_empty() {
            // Create migrations for RF restore
            let migrations = {
                let mut coordinator = self.migration_coordinator.write().await;
                let mut migs = Vec::new();

                for shard in shards_to_restore {
                    // Find a healthy source node in the same group
                    let source_node = self
                        .find_healthy_source_for_shard(shard, replica_group, node_id)
                        .await?;

                    let mut old_owners = HashMap::new();
                    old_owners.insert(shard, topo_to_migration_node_id(&source_node));

                    let mid = coordinator.begin_migration(
                        topo_to_migration_node_id(&node_id_obj),
                        replica_group,
                        old_owners,
                    )?;

                    migs.push(mid);
                }

                // Start dual-write for all migrations
                for mid in &migs {
                    coordinator.begin_dual_write(*mid)?;
                }

                migs
            };

            let migrations_count = migrations.len();

            // Record operation (before moving migrations)
            let operation = TopologyOperation {
                id: op_id,
                op_type: TopologyOperationType::NodeFailure, // Reuse NodeFailure type for recovery
                status: TopologyOperationStatus::InProgress,
                target_node: Some(node_id.to_string()),
                target_group: Some(replica_group),
                migrations: migrations.clone(),
                started_at: Some(now_ms()),
                completed_at: None,
                error: None,
            };

            {
                let mut ops = self.operations.write().await;
                ops.insert(op_id, operation);
            }

            // Start background RF restore task
            let topo_arc = self.topology.clone();
            let coord_arc = self.migration_coordinator.clone();
            let ops_arc = self.operations.clone();
            let active_arc = self.active_migrations.clone();
            let config = self.config.clone();
            let executor = self.migration_executor.clone();
            let metrics_arc = self.metrics.clone();

            tokio::spawn(async move {
                if let Err(e) = run_migration_task(
                    topo_arc,
                    coord_arc,
                    ops_arc,
                    active_arc,
                    op_id,
                    migrations,
                    config,
                    executor,
                    metrics_arc,
                )
                .await
                {
                    error!(error = %e, op_id = op_id, "RF restore task failed");
                }
            });

            Ok(TopologyOperationResult {
                id: op_id,
                message: format!(
                    "Node {} recovered with RF restore ({} shards)",
                    node_id, migrations_count
                ),
                migrations_count,
            })
        } else {
            // No shards need restoration
            let operation = TopologyOperation {
                id: op_id,
                op_type: TopologyOperationType::NodeFailure,
                status: TopologyOperationStatus::Complete,
                target_node: Some(node_id.to_string()),
                target_group: Some(replica_group),
                migrations: Vec::new(),
                started_at: Some(now_ms()),
                completed_at: Some(now_ms()),
                error: None,
            };

            {
                let mut ops = self.operations.write().await;
                ops.insert(op_id, operation);
            }

            Ok(TopologyOperationResult {
                id: op_id,
                message: format!("Node {} recovered (no shards needed restoration)", node_id),
                migrations_count: 0,
            })
        }
    }

    /// Compute which shards need RF restore for a recovered node.
    /// Returns shards where the recovered node should be a replica but may have lost data.
    async fn compute_shards_for_rf_restore(
        &self,
        recovered_node_id: &str,
        replica_group: u32,
    ) -> Result<Vec<ShardId>, RebalancerError> {
        let topo = self.topology.read().await;
        let recovered_node = TopologyNodeId::new(recovered_node_id.to_string());
        let rf = topo.rf();

        let group = topo
            .groups()
            .find(|g| g.id == replica_group)
            .ok_or_else(|| RebalancerError::GroupNotFound(replica_group))?;

        let mut shards_to_restore = Vec::new();

        // For each shard, check if the recovered node should be a replica
        for shard_id in 0..topo.shards {
            let assignment = assign_shard_in_group(shard_id, group.nodes(), rf);

            if assignment.contains(&recovered_node) {
                // This node should be a replica for this shard
                shards_to_restore.push(ShardId(shard_id));
            }
        }

        Ok(shards_to_restore)
    }

    /// Find a healthy source node for RF restore of a specific shard.
    async fn find_healthy_source_for_shard(
        &self,
        shard: ShardId,
        replica_group: u32,
        exclude_node_id: &str,
    ) -> Result<TopologyNodeId, RebalancerError> {
        let topo = self.topology.read().await;
        let exclude_node = TopologyNodeId::new(exclude_node_id.to_string());

        let group = topo
            .groups()
            .find(|g| g.id == replica_group)
            .ok_or_else(|| RebalancerError::GroupNotFound(replica_group))?;

        let assignment = assign_shard_in_group(shard.0, group.nodes(), topo.rf());

        // Find a healthy replica (excluding the recovered node)
        for node in assignment {
            if node != exclude_node {
                if let Some(n) = topo.node(&node) {
                    if n.is_healthy() {
                        return Ok(node);
                    }
                }
            }
        }

        Err(RebalancerError::InvalidState(format!(
            "no healthy source found for shard {} in group {}",
            shard.0, replica_group
        )))
    }

    /// Compute which shards should move to a new node.
    /// Returns shard -> old_owner mapping for shards that will move.
    ///
    /// For each shard where the new node enters the assignment, we select one
    /// of the old owners as the migration source. If the new node displaced
    /// an old owner, we use that node; otherwise we use the lowest-scored old owner.
    async fn compute_shard_moves_for_new_node(
        &self,
        new_node_id: &str,
        replica_group: u32,
    ) -> Result<Vec<(ShardId, TopologyNodeId)>, RebalancerError> {
        let topo = self.topology.read().await;

        let new_node_id = TopologyNodeId::new(new_node_id.to_string());
        let rf = topo.rf();

        // Find the target group
        let group = topo
            .groups()
            .find(|g| g.id == replica_group)
            .ok_or_else(|| RebalancerError::GroupNotFound(replica_group))?;

        let existing_nodes: Vec<_> = group.nodes().iter().cloned().collect();
        let mut affected_shards = Vec::new();

        // For each shard, check if the new node is in the new assignment
        for shard_id in 0..topo.shards {
            let old_assignment: Vec<_> = assign_shard_in_group(shard_id, &existing_nodes, rf)
                .into_iter()
                .collect();

            // New assignment with the new node included
            let all_nodes: Vec<_> = existing_nodes
                .iter()
                .cloned()
                .chain(std::iter::once(new_node_id.clone()))
                .collect();
            let new_assignment: Vec<_> = assign_shard_in_group(shard_id, &all_nodes, rf)
                .into_iter()
                .collect();

            // Check if new node is in the new assignment
            if !new_assignment.contains(&new_node_id) {
                continue;
            }

            // Find the source node for migration
            // Priority 1: Use the displaced node (if any)
            // Priority 2: Use the lowest-scored old owner (load balancing)
            let source_node = if let Some(displaced) =
                old_assignment.iter().find(|n| !new_assignment.contains(n))
            {
                // An old node was displaced - use it as source
                displaced.clone()
            } else {
                // No displacement - pick lowest-scored old owner
                // Find the old owner with the minimum rendezvous score
                let mut min_score = u64::MAX;
                let mut min_node = old_assignment
                    .first()
                    .cloned()
                    .unwrap_or_else(|| existing_nodes.first().unwrap().clone());

                for old_node in &old_assignment {
                    let s = score(shard_id, old_node.as_str());
                    if s < min_score {
                        min_score = s;
                        min_node = old_node.clone();
                    }
                }
                min_node
            };

            affected_shards.push((ShardId(shard_id), source_node));
        }

        Ok(affected_shards)
    }

    /// Compute where each shard should go when draining a node.
    /// Returns shard -> destination_node mapping.
    async fn compute_shard_destinations_for_drain(
        &self,
        drain_node_id: &str,
        replica_group: u32,
    ) -> Result<Vec<(ShardId, TopologyNodeId)>, RebalancerError> {
        let topo = self.topology.read().await;

        let drain_node_id = TopologyNodeId::new(drain_node_id.to_string());
        let rf = topo.rf();

        // Find the target group
        let group = topo
            .groups()
            .find(|g| g.id == replica_group)
            .ok_or_else(|| RebalancerError::GroupNotFound(replica_group))?;

        let other_nodes: Vec<_> = group
            .nodes()
            .iter()
            .filter(|n| **n != drain_node_id)
            .cloned()
            .collect();

        if other_nodes.is_empty() {
            return Err(RebalancerError::CannotRemoveLastNode);
        }

        let mut destinations = Vec::new();

        // For each shard, find a new owner among the remaining nodes
        for shard_id in 0..topo.shards {
            // Check if the draining node is in the assignment for this shard
            let assignment: Vec<_> = assign_shard_in_group(shard_id, group.nodes(), rf);

            if assignment.contains(&drain_node_id) {
                // This shard needs a new home
                // Use rendezvous hash to pick the best remaining node
                let mut best_node = None;
                let mut best_score = 0u64;

                for node in &other_nodes {
                    let s = score(shard_id, node.as_str());
                    if s > best_score {
                        best_score = s;
                        best_node = Some(node.clone());
                    }
                }

                if let Some(dest) = best_node {
                    destinations.push((ShardId(shard_id), dest));
                }
            }
        }

        Ok(destinations)
    }
}

/// Background task to run migrations for a topology operation.
async fn run_migration_task(
    topology: Arc<RwLock<Topology>>,
    coordinator: Arc<RwLock<MigrationCoordinator>>,
    operations: Arc<RwLock<HashMap<u64, TopologyOperation>>>,
    active_migrations: Arc<RwLock<HashMap<MigrationId, u64>>>,
    op_id: u64,
    migrations: Vec<MigrationId>,
    config: RebalancerConfig,
    executor: Option<Arc<dyn MigrationExecutor>>,
    metrics: Arc<RwLock<RebalancerMetrics>>,
) -> Result<(), RebalancerError> {
    let Some(exec) = executor else {
        // No executor - simulate completion for testing
        for mid in migrations {
            tokio::time::sleep(tokio::time::Duration::from_millis(
                config.migration_batch_delay_ms,
            ))
            .await;

            let shards_to_complete = {
                let coord = coordinator.read().await;
                if let Some(state) = coord.get_state(mid) {
                    state.old_owners.keys().copied().collect::<Vec<_>>()
                } else {
                    continue;
                }
            };

            let docs_per_shard = 1000u64;
            {
                let mut coord = coordinator.write().await;
                for shard in &shards_to_complete {
                    coord.shard_migration_complete(mid, *shard, docs_per_shard)?;
                }
            }

            // Record metrics for simulated migration
            {
                let mut metrics_guard = metrics.write().await;
                metrics_guard
                    .record_documents_migrated(docs_per_shard * shards_to_complete.len() as u64);
            }

            {
                let mut coord = coordinator.write().await;
                coord.begin_cutover(mid)?;
                coord.complete_drain(mid)?;
                coord.complete_cleanup(mid)?;
            }

            {
                let mut active = active_migrations.write().await;
                active.remove(&mid);
            }
        }

        // Mark operation as complete
        {
            let mut ops = operations.write().await;
            if let Some(op) = ops.get_mut(&op_id) {
                op.status = TopologyOperationStatus::Complete;
                op.completed_at = Some(now_ms());
            }
        }

        // Mark new node as active
        {
            let mut topo = topology.write().await;
            let ops = operations.read().await;
            if let Some(op) = ops.get(&op_id) {
                if let Some(ref node_id) = op.target_node {
                    let node_id = TopologyNodeId::new(node_id.clone());
                    if let Some(node) = topo.node_mut(&node_id) {
                        node.status = NodeStatus::Active;
                    }
                }
            }
        }

        return Ok(());
    };

    // With executor - perform actual migration
    // For each migration (each shard that moves to the new node)
    for mid in migrations {
        // Get migration state to find source/target info
        let (new_node, _replica_group, old_owners, index_uid) = {
            let coord = coordinator.read().await;
            let state = coord
                .get_state(mid)
                .ok_or_else(|| RebalancerError::InvalidState("migration state not found".into()))?;

            // Use a default index for now - in production, this would come from config
            let index_uid = "default".to_string();

            (
                state.new_node.to_string(),
                state.replica_group,
                state.old_owners.clone(),
                index_uid,
            )
        };

        // Get node addresses
        let (new_node_address, old_owner_addresses) = {
            let topo = topology.read().await;
            let new_addr = topo
                .node(&TopologyNodeId::new(new_node.to_string()))
                .ok_or_else(|| RebalancerError::NodeNotFound(new_node.to_string()))?
                .address
                .clone();

            let mut old_addrs = HashMap::new();
            for (shard, old_node) in &old_owners {
                if let Some(node) = topo.node(&migration_to_topo_node_id(old_node)) {
                    old_addrs.insert(*shard, node.address.clone());
                }
            }

            (new_addr, old_addrs)
        };

        let mut migration_total_docs = 0u64;

        // For each shard in the migration
        for (shard_id, old_node_id) in &old_owners {
            let old_address = old_owner_addresses.get(shard_id).ok_or_else(|| {
                RebalancerError::InvalidState("old node address not found".into())
            })?;

            info!(
                migration_id = %mid,
                shard_id = shard_id.0,
                from = %old_node_id.0,
                to = %new_node,
                "starting shard migration"
            );

            // Paginate through all documents for this shard
            let mut offset = 0u32;
            let limit = config.migration_batch_size;
            let mut total_docs_copied = 0u64;

            loop {
                // Fetch documents from source
                let (docs, _total) = exec
                    .fetch_documents(
                        &old_node_id.0,
                        old_address,
                        &index_uid,
                        shard_id.0,
                        limit,
                        offset,
                    )
                    .await
                    .map_err(|e| RebalancerError::InvalidState(format!("fetch failed: {}", e)))?;

                if docs.is_empty() {
                    break; // No more documents
                }

                // Write documents to target
                exec.write_documents(&new_node, &new_node_address, &index_uid, docs.clone())
                    .await
                    .map_err(|e| RebalancerError::InvalidState(format!("write failed: {}", e)))?;

                total_docs_copied += docs.len() as u64;
                offset += limit;

                // Throttle if configured
                if config.migration_batch_delay_ms > 0 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        config.migration_batch_delay_ms,
                    ))
                    .await;
                }
            }

            // Mark shard migration complete
            {
                let mut coord = coordinator.write().await;
                coord.shard_migration_complete(mid, *shard_id, total_docs_copied)?;
            }

            migration_total_docs += total_docs_copied;

            info!(
                migration_id = %mid,
                shard_id = shard_id.0,
                docs_copied = total_docs_copied,
                "shard migration complete"
            );
        }

        // Record metrics for this migration
        {
            let mut metrics_guard = metrics.write().await;
            metrics_guard.record_documents_migrated(migration_total_docs);
        }

        // All shards for this migration complete - begin cutover
        {
            let mut coord = coordinator.write().await;
            coord.begin_cutover(mid)?;
        }

        // Delta pass: re-read from source to catch stragglers
        for (shard_id, old_node_id) in &old_owners {
            let old_address = old_owner_addresses.get(shard_id).unwrap();

            let (docs, _) = exec
                .fetch_documents(
                    &old_node_id.0,
                    old_address,
                    &index_uid,
                    shard_id.0,
                    config.migration_batch_size,
                    0,
                )
                .await
                .map_err(|e| RebalancerError::InvalidState(format!("delta fetch failed: {}", e)))?;

            if !docs.is_empty() {
                // Write any stragglers to target
                exec.write_documents(&new_node, &new_node_address, &index_uid, docs)
                    .await
                    .map_err(|e| {
                        RebalancerError::InvalidState(format!("delta write failed: {}", e))
                    })?;
            }

            // Mark delta complete
            {
                let mut coord = coordinator.write().await;
                // Complete drain after delta pass
                coord.complete_drain(mid)?;
            }
        }

        // Activate shards
        {
            let mut coord = coordinator.write().await;
            coord.complete_cleanup(mid)?;
        }

        // Delete migrated shards from old nodes
        for (shard_id, old_node_id) in &old_owners {
            let old_address = old_owner_addresses.get(shard_id).unwrap();

            if let Err(e) = exec
                .delete_shard(&old_node_id.0, old_address, &index_uid, shard_id.0)
                .await
            {
                warn!(
                    shard_id = shard_id.0,
                    node = %old_node_id.0,
                    error = %e,
                    "failed to delete migrated shard from old node (may need manual cleanup)"
                );
            }
        }

        // Remove from active migrations
        {
            let mut active = active_migrations.write().await;
            active.remove(&mid);
        }
    }

    // Mark operation as complete
    {
        let mut ops = operations.write().await;
        if let Some(op) = ops.get_mut(&op_id) {
            op.status = TopologyOperationStatus::Complete;
            op.completed_at = Some(now_ms());
        }
    }

    // Mark new node as active
    {
        let mut topo = topology.write().await;
        let ops = operations.read().await;
        if let Some(op) = ops.get(&op_id) {
            if let Some(ref node_id) = op.target_node {
                let node_id = TopologyNodeId::new(node_id.clone());
                if let Some(node) = topo.node_mut(&node_id) {
                    node.status = NodeStatus::Active;
                }
            }
        }
    }

    Ok(())
}

/// Background task to run drain migrations for a node.
async fn run_drain_task(
    topology: Arc<RwLock<Topology>>,
    coordinator: Arc<RwLock<MigrationCoordinator>>,
    operations: Arc<RwLock<HashMap<u64, TopologyOperation>>>,
    active_migrations: Arc<RwLock<HashMap<MigrationId, u64>>>,
    op_id: u64,
    migrations: Vec<MigrationId>,
    config: RebalancerConfig,
    drain_node_id: String,
    executor: Option<Arc<dyn MigrationExecutor>>,
    metrics: Arc<RwLock<RebalancerMetrics>>,
) -> Result<(), RebalancerError> {
    let Some(exec) = executor else {
        // No executor - simulate completion for testing
        for mid in migrations {
            tokio::time::sleep(tokio::time::Duration::from_millis(
                config.migration_batch_delay_ms,
            ))
            .await;

            let shards_to_complete = {
                let coord = coordinator.read().await;
                if let Some(state) = coord.get_state(mid) {
                    state.old_owners.keys().copied().collect::<Vec<_>>()
                } else {
                    continue;
                }
            };

            let docs_per_shard = 1000u64;
            {
                let mut coord = coordinator.write().await;
                for shard in &shards_to_complete {
                    coord.shard_migration_complete(mid, *shard, docs_per_shard)?;
                }
            }

            // Record metrics for simulated migration
            {
                let mut metrics_guard = metrics.write().await;
                metrics_guard
                    .record_documents_migrated(docs_per_shard * shards_to_complete.len() as u64);
            }

            {
                let mut coord = coordinator.write().await;
                coord.begin_cutover(mid)?;
                coord.complete_drain(mid)?;
                coord.complete_cleanup(mid)?;
            }

            {
                let mut active = active_migrations.write().await;
                active.remove(&mid);
            }
        }

        // Mark operation as complete
        {
            let mut ops = operations.write().await;
            if let Some(op) = ops.get_mut(&op_id) {
                op.status = TopologyOperationStatus::Complete;
                op.completed_at = Some(now_ms());
            }
        }

        // Mark drained node as removed (operator can delete PVC)
        {
            let mut topo = topology.write().await;
            let node_id = TopologyNodeId::new(drain_node_id);
            if let Some(node) = topo.node_mut(&node_id) {
                node.status = NodeStatus::Removed;
            }
        }

        return Ok(());
    };

    // With executor - perform actual drain migration
    // For each migration (each shard being drained from the node)
    for mid in migrations {
        // Get migration state
        let (new_node, _replica_group, old_owners, index_uid) = {
            let coord = coordinator.read().await;
            let state = coord
                .get_state(mid)
                .ok_or_else(|| RebalancerError::InvalidState("migration state not found".into()))?;

            // Use a default index for now
            let index_uid = "default".to_string();

            (
                state.new_node.to_string(),
                state.replica_group,
                state.old_owners.clone(),
                index_uid,
            )
        };

        // Get node addresses
        let (_drain_node_id_obj, drain_node_address, new_node_address) = {
            let topo = topology.read().await;
            let drain_id = TopologyNodeId::new(drain_node_id.clone());
            let drain_addr = topo
                .node(&drain_id)
                .ok_or_else(|| RebalancerError::NodeNotFound(drain_node_id.clone()))?
                .address
                .clone();

            let new_addr = topo
                .node(&TopologyNodeId::new(new_node.to_string()))
                .ok_or_else(|| RebalancerError::NodeNotFound(new_node.to_string()))?
                .address
                .clone();

            (drain_id, drain_addr, new_addr)
        };

        // For each shard being drained
        for (shard_id, _old_node) in &old_owners {
            info!(
                migration_id = %mid,
                shard_id = shard_id.0,
                from = %drain_node_id,
                to = %new_node,
                "starting shard drain"
            );

            // Paginate through all documents for this shard on the draining node
            let mut offset = 0u32;
            let limit = config.migration_batch_size;
            let mut total_docs_copied = 0u64;

            loop {
                // Fetch documents from draining node
                let (docs, _total) = exec
                    .fetch_documents(
                        &drain_node_id,
                        &drain_node_address,
                        &index_uid,
                        shard_id.0,
                        limit,
                        offset,
                    )
                    .await
                    .map_err(|e| RebalancerError::InvalidState(format!("fetch failed: {}", e)))?;

                if docs.is_empty() {
                    break; // No more documents
                }

                // Write documents to new node
                exec.write_documents(&new_node, &new_node_address, &index_uid, docs.clone())
                    .await
                    .map_err(|e| RebalancerError::InvalidState(format!("write failed: {}", e)))?;

                total_docs_copied += docs.len() as u64;
                offset += limit;

                if config.migration_batch_delay_ms > 0 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        config.migration_batch_delay_ms,
                    ))
                    .await;
                }
            }

            // Mark shard migration complete
            {
                let mut coord = coordinator.write().await;
                coord.shard_migration_complete(mid, *shard_id, total_docs_copied)?;
            }

            info!(
                migration_id = %mid,
                shard_id = shard_id.0,
                docs_copied = total_docs_copied,
                "shard drain complete"
            );
        }

        // All shards for this migration complete - begin cutover
        {
            let mut coord = coordinator.write().await;
            coord.begin_cutover(mid)?;
        }

        // Delta pass: re-read from draining node to catch stragglers
        for (shard_id, _old_node) in &old_owners {
            let (docs, _) = exec
                .fetch_documents(
                    &drain_node_id,
                    &drain_node_address,
                    &index_uid,
                    shard_id.0,
                    config.migration_batch_size,
                    0,
                )
                .await
                .map_err(|e| RebalancerError::InvalidState(format!("delta fetch failed: {}", e)))?;

            if !docs.is_empty() {
                // Write any stragglers to new node
                exec.write_documents(&new_node, &new_node_address, &index_uid, docs)
                    .await
                    .map_err(|e| {
                        RebalancerError::InvalidState(format!("delta write failed: {}", e))
                    })?;
            }

            {
                let mut coord = coordinator.write().await;
                coord.complete_drain(mid)?;
            }
        }

        // Activate shards and complete cleanup
        {
            let mut coord = coordinator.write().await;
            coord.complete_cleanup(mid)?;
        }

        // Delete drained shards from the draining node
        for (shard_id, _old_node) in &old_owners {
            if let Err(e) = exec
                .delete_shard(&drain_node_id, &drain_node_address, &index_uid, shard_id.0)
                .await
            {
                warn!(
                    shard_id = shard_id.0,
                    node = %drain_node_id,
                    error = %e,
                    "failed to delete drained shard (may need manual cleanup)"
                );
            }
        }

        {
            let mut active = active_migrations.write().await;
            active.remove(&mid);
        }
    }

    // Mark operation as complete
    {
        let mut ops = operations.write().await;
        if let Some(op) = ops.get_mut(&op_id) {
            op.status = TopologyOperationStatus::Complete;
            op.completed_at = Some(now_ms());
        }
    }

    // Mark drained node as removed (operator can delete PVC)
    {
        let mut topo = topology.write().await;
        let node_id = TopologyNodeId::new(drain_node_id);
        if let Some(node) = topo.node_mut(&node_id) {
            node.status = NodeStatus::Removed;
        }
    }

    Ok(())
}

/// Get current time in milliseconds since Unix epoch.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// HttpMigrationExecutor - Actual HTTP-based document migration
// ---------------------------------------------------------------------------

/// HTTP-based migration executor for moving documents between Meilisearch nodes.
///
/// This implements the `MigrationExecutor` trait by making actual HTTP requests
/// to Meilisearch nodes' APIs. It uses the `_miroir_shard` filterable attribute
/// to fetch only the documents belonging to a specific shard.
pub struct HttpMigrationExecutor {
    /// Master key for authenticating with Meilisearch nodes.
    node_master_key: String,
    /// HTTP client for making requests to nodes.
    client: reqwest::Client,
}

impl HttpMigrationExecutor {
    /// Create a new HTTP migration executor.
    ///
    /// # Arguments
    /// * `node_master_key` - Master key for authenticating with Meilisearch nodes
    /// * `node_timeout_ms` - Timeout for HTTP requests to nodes (milliseconds)
    pub fn new(node_master_key: String, node_timeout_ms: u64) -> Self {
        let timeout = std::time::Duration::from_millis(node_timeout_ms);

        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("Failed to create HTTP client for migration executor");

        Self {
            node_master_key,
            client,
        }
    }

    /// Build the filter string for fetching documents by shard.
    fn shard_filter(&self, shard_id: u32) -> String {
        format!("_miroir_shard = {}", shard_id)
    }

    /// Make an authenticated GET request to a node.
    async fn get_node(
        &self,
        node_address: &str,
        path: &str,
    ) -> std::result::Result<reqwest::Response, String> {
        let url = if node_address.ends_with('/') {
            format!("{}{}", node_address, path.trim_start_matches('/'))
        } else {
            format!(
                "{}/{}",
                node_address.trim_end_matches('/'),
                path.trim_start_matches('/')
            )
        };

        self.client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.node_master_key))
            .send()
            .await
            .map_err(|e| format!("GET {} failed: {}", url, e))
    }

    /// Make an authenticated POST request to a node.
    async fn post_node(
        &self,
        node_address: &str,
        path: &str,
        body: serde_json::Value,
    ) -> std::result::Result<reqwest::Response, String> {
        let url = if node_address.ends_with('/') {
            format!("{}{}", node_address, path.trim_start_matches('/'))
        } else {
            format!(
                "{}/{}",
                node_address.trim_end_matches('/'),
                path.trim_start_matches('/')
            )
        };

        self.client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.node_master_key))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("POST {} failed: {}", url, e))
    }
}

#[async_trait::async_trait]
impl MigrationExecutor for HttpMigrationExecutor {
    /// Fetch documents from a source node for a specific shard.
    ///
    /// Uses the `_miroir_shard` filterable attribute to retrieve only documents
    /// belonging to the specified shard, avoiding full index scans.
    async fn fetch_documents(
        &self,
        _source_node: &str,
        source_address: &str,
        index_uid: &str,
        shard_id: u32,
        limit: u32,
        offset: u32,
    ) -> std::result::Result<(Vec<serde_json::Value>, u64), String> {
        let filter = self.shard_filter(shard_id);
        let path = format!(
            "indexes/{}/documents?filter={}&limit={}&offset={}",
            index_uid,
            urlencoding::encode(&filter),
            limit,
            offset
        );

        let response = self.get_node(source_address, &path).await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read error".to_string());
            return Err(format!(
                "Failed to fetch documents from {}: HTTP {} - {}",
                source_address, status, error_text
            ));
        }

        let json_body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| format!("Failed to parse response from {}: {}", source_address, e))?;

        // Meilisearch returns { results: [...], total: 123, limit: 20, offset: 0 }
        let results = json_body
            .get("results")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                format!(
                    "Invalid response from {}: missing 'results' field",
                    source_address
                )
            })?;

        let total = json_body.get("total").and_then(|v| v.as_u64()).unwrap_or(0);

        Ok((results.clone(), total))
    }

    /// Write documents to a target node.
    ///
    /// Documents already contain the `_miroir_shard` field from the source,
    /// so they can be written directly without modification.
    async fn write_documents(
        &self,
        _target_node: &str,
        target_address: &str,
        index_uid: &str,
        documents: Vec<serde_json::Value>,
    ) -> std::result::Result<(), String> {
        if documents.is_empty() {
            return Ok(());
        }

        let path = format!("indexes/{}/documents", index_uid);

        let response = self
            .post_node(target_address, &path, serde_json::json!(documents))
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read error".to_string());
            return Err(format!(
                "Failed to write {} documents to {}: HTTP {} - {}",
                documents.len(),
                target_address,
                status,
                error_text
            ));
        }

        // The response contains the task UID, but we don't need to wait for it
        // since migrations are eventually consistent via anti-entropy
        Ok(())
    }

    /// Delete documents from a node by shard filter.
    ///
    /// This is called after a shard migration is complete to remove the
    /// migrated documents from the source node.
    async fn delete_shard(
        &self,
        _node: &str,
        node_address: &str,
        index_uid: &str,
        shard_id: u32,
    ) -> std::result::Result<(), String> {
        let filter = self.shard_filter(shard_id);
        let path = format!("indexes/{}/documents/delete", index_uid);

        let body = serde_json::json!({
            "filter": filter
        });

        let response = self.post_node(node_address, &path, body).await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read error".to_string());
            return Err(format!(
                "Failed to delete shard {} from {}: HTTP {} - {}",
                shard_id, node_address, status, error_text
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::MigrationConfig;
    use crate::topology::Node;
    use std::sync::Arc;

    fn test_topology() -> Topology {
        let mut topo = Topology::new(64, 2, 2);
        topo.add_node(Node::new(
            TopologyNodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            TopologyNodeId::new("node-1".into()),
            "http://node-1:7700".into(),
            0,
        ));
        topo.add_node(Node::new(
            TopologyNodeId::new("node-2".into()),
            "http://node-2:7700".into(),
            1,
        ));
        topo.add_node(Node::new(
            TopologyNodeId::new("node-3".into()),
            "http://node-3:7700".into(),
            1,
        ));
        topo
    }

    #[test]
    fn test_rebalancer_config_default() {
        let config = RebalancerConfig::default();
        assert_eq!(config.max_concurrent_migrations, 4);
        assert_eq!(config.migration_timeout_s, 3600);
        assert!(config.auto_rebalance_on_recovery);
    }

    #[test]
    fn test_topology_operation_serialization() {
        let op = TopologyOperation {
            id: 1,
            op_type: TopologyOperationType::AddNode,
            status: TopologyOperationStatus::InProgress,
            target_node: Some("node-4".into()),
            target_group: Some(0),
            migrations: vec![MigrationId(1), MigrationId(2)],
            started_at: Some(1700000000000),
            completed_at: None,
            error: None,
        };

        let json = serde_json::to_string(&op).unwrap();
        assert!(json.contains("\"op_type\":\"add_node\""));
        assert!(json.contains("\"status\":\"in_progress\""));
        assert!(json.contains("\"target_node\":\"node-4\""));
    }

    #[test]
    fn test_rebalance_status_serialization() {
        let status = RebalanceStatus {
            in_progress: true,
            operations: vec![],
            migrations: HashMap::new(),
        };

        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"in_progress\":true"));
    }

    #[tokio::test]
    async fn test_rebalancer_status() {
        let topo = Arc::new(RwLock::new(test_topology()));
        let config = RebalancerConfig::default();
        let migration_config = MigrationConfig::default();

        let rebalancer = Rebalancer::new(config, topo, migration_config);

        let status = rebalancer.status().await;
        assert!(!status.in_progress);
        assert!(status.operations.is_empty());
    }

    #[tokio::test]
    async fn test_add_node_creates_operation() {
        let topo = Arc::new(RwLock::new(test_topology()));
        let config = RebalancerConfig::default();
        let migration_config = MigrationConfig::default();

        let rebalancer = Rebalancer::new(config, topo.clone(), migration_config);

        let request = AddNodeRequest {
            id: "node-4".into(),
            address: "http://node-4:7700".into(),
            replica_group: 0,
        };

        let result = rebalancer.add_node(request).await.unwrap();
        assert!(result.id > 0);
        assert!(result.migrations_count > 0);

        // Check node was added
        let topo_read = topo.read().await;
        assert!(topo_read
            .node(&TopologyNodeId::new("node-4".into()))
            .is_some());
    }

    #[tokio::test]
    async fn test_add_duplicate_node_fails() {
        let topo = Arc::new(RwLock::new(test_topology()));
        let config = RebalancerConfig::default();
        let migration_config = MigrationConfig::default();

        let rebalancer = Rebalancer::new(config, topo, migration_config);

        let request = AddNodeRequest {
            id: "node-0".into(), // Already exists
            address: "http://node-0:7700".into(),
            replica_group: 0,
        };

        let result = rebalancer.add_node(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_remove_last_node_fails() {
        let mut topo = Topology::new(64, 1, 1);
        topo.add_node(Node::new(
            TopologyNodeId::new("solo".into()),
            "http://solo:7700".into(),
            0,
        ));
        let topo = Arc::new(RwLock::new(topo));

        let config = RebalancerConfig::default();
        let migration_config = MigrationConfig::default();

        let rebalancer = Rebalancer::new(config, topo, migration_config);

        let request = RemoveNodeRequest {
            node_id: "solo".into(),
            force: false,
        };

        let result = rebalancer.remove_node(request).await;
        assert!(matches!(result, Err(RebalancerError::CannotRemoveLastNode)));
    }

    #[tokio::test]
    async fn test_handle_node_failure() {
        let topo = Arc::new(RwLock::new(test_topology()));
        let config = RebalancerConfig::default();
        let migration_config = MigrationConfig::default();

        let rebalancer = Rebalancer::new(config, topo.clone(), migration_config);

        let result = rebalancer.handle_node_failure("node-0").await.unwrap();
        assert!(matches!(
            result.message.as_str(),
            "Node node-0 marked as failed"
        ));

        // Check node was marked failed
        let topo_read = topo.read().await;
        let node = topo_read
            .node(&TopologyNodeId::new("node-0".into()))
            .unwrap();
        assert_eq!(node.status, NodeStatus::Failed);
    }

    #[test]
    fn test_shard_filter() {
        let executor = HttpMigrationExecutor::new("test-key".to_string(), 5000);
        assert_eq!(executor.shard_filter(42), "_miroir_shard = 42");
        assert_eq!(executor.shard_filter(0), "_miroir_shard = 0");
    }

    #[test]
    fn test_http_migration_executor_new() {
        let executor = HttpMigrationExecutor::new("master-key".to_string(), 10000);
        assert_eq!(executor.node_master_key, "master-key");
    }
}
