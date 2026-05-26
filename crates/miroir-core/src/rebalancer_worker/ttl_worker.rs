//! TTL worker — background sweeper for document expiration (plan §13.14).
//!
//! Each pod runs a TTL sweeper that deletes expired documents from its
//! rendezvous-owned shards (Mode A scaling). Uses leader election to ensure
//! only one pod performs the sweep per index.
//!
//! ## Sweep Algorithm
//!
//! For each owned shard:
//! 1. Build filter: `_miroir_shard = {s} AND _miroir_expires_at <= {now_ms}`
//! 2. POST /indexes/{uid}/documents/delete with the filter
//! 3. Track deleted documents via metrics
//!
//! ## Origin Tagging (plan §13.13)
//!
//! TTL deletes are tagged with `origin="ttl_expire"` so they are suppressed
//! from CDC by default unless `emit_ttl_deletes` is true.

use crate::cdc::ORIGIN_TTL_EXPIRE;
use crate::error::{MiroirError, Result};
use crate::scatter::{DeleteByFilterRequest, NodeClient};
use crate::task_store::TaskStore;
use crate::ttl::{TtlConfig, TtlManager};
use crate::topology::{Topology, NodeId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// TTL worker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlWorkerConfig {
    /// Leader lease TTL in seconds.
    pub lease_ttl_secs: u64,
    /// Leader lease renewal interval in milliseconds.
    pub lease_renewal_interval_ms: u64,
    /// Replica group ID for this pod.
    pub replica_group_id: u32,
    /// Total shards in the cluster.
    pub total_shards: u32,
    /// Replication factor.
    pub rf: usize,
}

impl TtlWorkerConfig {
    /// Create a new TTL worker config.
    pub fn new(
        replica_group_id: u32,
        total_shards: u32,
        rf: usize,
    ) -> Self {
        Self {
            lease_ttl_secs: 10,
            lease_renewal_interval_ms: 2000,
            replica_group_id,
            total_shards,
            rf,
        }
    }
}

/// TTL worker — runs the background TTL sweeper with leader election.
pub struct TtlWorker<C: NodeClient> {
    /// TTL manager that performs the actual sweeps.
    manager: TtlManager<C>,
    /// Task store for leader election.
    task_store: Arc<dyn TaskStore>,
    /// Worker configuration.
    config: TtlWorkerConfig,
    /// Pod ID for leader election.
    pod_id: String,
    /// Running flag.
    running: Arc<RwLock<bool>>,
}

impl<C: NodeClient> TtlWorker<C> {
    /// Create a new TTL worker.
    pub fn new(
        config: TtlWorkerConfig,
        ttl_config: TtlConfig,
        topology: Arc<RwLock<Topology>>,
        node_client: Arc<C>,
        task_store: Arc<dyn TaskStore>,
        pod_id: String,
    ) -> Self {
        let manager = TtlManager::new(
            ttl_config,
            topology,
            node_client,
            config.total_shards,
            config.replica_group_id,
            config.rf,
        );

        Self {
            manager,
            task_store,
            config,
            pod_id,
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Create a new TTL worker with metrics callbacks.
    pub fn with_metrics(
        config: TtlWorkerConfig,
        ttl_config: TtlConfig,
        topology: Arc<RwLock<Topology>>,
        node_client: Arc<C>,
        task_store: Arc<dyn TaskStore>,
        pod_id: String,
        metrics_expired: Box<dyn Fn(u64) + Send + Sync>,
        metrics_duration: Box<dyn Fn(f64) + Send + Sync>,
    ) -> Self {
        let manager = TtlManager::new(
            ttl_config,
            topology,
            node_client,
            config.total_shards,
            config.replica_group_id,
            config.rf,
        ).with_metrics(metrics_expired, metrics_duration);

        Self {
            manager,
            task_store,
            config,
            pod_id,
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the TTL worker background task.
    pub async fn run(&self) -> Result<()> {
        *self.running.write().await = true;

        info!(
            pod_id = %self.pod_id,
            replica_group_id = self.config.replica_group_id,
            total_shards = self.config.total_shards,
            "TTL worker started"
        );

        // Start the TTL manager background sweeper
        self.manager.start().await;

        let scope = "ttl_sweeper";

        loop {
            let now_ms = now_ms();
            let expires_at = now_ms + (self.config.lease_ttl_secs * 1000) as i64;

            // Try to acquire leader lease
            match tokio::task::spawn_blocking({
                let task_store = self.task_store.clone();
                let scope = scope.to_string();
                let pod_id = self.pod_id.clone();
                move || {
                    task_store.try_acquire_leader_lease(&scope, &pod_id, expires_at, now_ms)
                }
            })
            .await
            {
                Ok(Ok(true)) => {
                    info!(
                        pod_id = %self.pod_id,
                        scope,
                        "TTL worker acquired leader lease"
                    );

                    // We are the leader - run leader loop with lease renewal
                    if let Err(e) = self.run_leader_loop().await {
                        error!(error = %e, "TTL worker leader loop failed");
                    }
                }
                Ok(Ok(false)) => {
                    debug!("Failed to acquire TTL worker leader lease: already held");
                }
                Ok(Err(e)) => {
                    error!("Failed to acquire TTL worker leader lease: {}", e);
                }
                Err(e) => {
                    error!("Failed to spawn leader lease acquisition: {}", e);
                }
            }

            // Check if still running
            {
                let running = self.running.read().await;
                if !*running {
                    info!("TTL worker stopping");
                    return Ok(());
                }
            }

            // Wait before retrying lease acquisition
            tokio::time::sleep(Duration::from_millis(
                self.config.lease_renewal_interval_ms,
            ))
            .await;
        }
    }

    /// Run the leader loop with lease renewal.
    async fn run_leader_loop(&self) -> Result<()> {
        let scope = "ttl_sweeper";
        let mut lease_renewal = tokio::time::interval(Duration::from_millis(
            self.config.lease_renewal_interval_ms,
        ));

        loop {
            // Check if still running
            {
                let running = self.running.read().await;
                if !*running {
                    info!("TTL worker leader stopping");
                    return Ok(());
                }
            }

            tokio::select! {
                _ = lease_renewal.tick() => {
                    // Renew lease
                    let now_ms = now_ms();
                    let expires_at = now_ms + (self.config.lease_ttl_secs * 1000) as i64;

                    match tokio::task::spawn_blocking({
                        let task_store = self.task_store.clone();
                        let scope = scope.to_string();
                        let pod_id = self.pod_id.clone();
                        move || {
                            task_store.renew_leader_lease(&scope, &pod_id, expires_at, now_ms)
                        }
                    })
                    .await
                    {
                        Ok(Ok(true)) => {
                            debug!("TTL worker leader lease renewed");
                        }
                        Ok(Ok(false)) => {
                            info!("TTL worker lost leader lease");
                            return Ok(());
                        }
                        Ok(Err(e)) => {
                            error!("Failed to renew TTL worker leader lease: {}", e);
                            return Err(MiroirError::TaskStore(e.to_string()));
                        }
                        Err(e) => {
                            error!("Failed to spawn lease renewal: {}", e);
                            return Err(MiroirError::TaskStore(e.to_string()));
                        }
                    }
                }
            }
        }
    }

    /// Stop the TTL worker.
    pub async fn stop(&self) {
        *self.running.write().await = false;
        self.manager.stop().await;
    }

    /// Get the TTL manager state.
    pub async fn state(&self) -> crate::ttl::TtlSweeperState {
        self.manager.state().await
    }
}

/// Get current time in milliseconds since Unix epoch.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scatter::MockNodeClient;
    use crate::topology::{Node, Topology};

    fn make_test_topology() -> Topology {
        let mut topo = Topology::new(64, 2, 2);
        for i in 0u32..3 {
            let mut node = Node::new(
                NodeId::new(format!("node-{i}")),
                format!("http://node-{i}:7700"),
                i % 2,
            );
            node.status = crate::topology::NodeStatus::Active;
            topo.add_node(node);
        }
        topo
    }

    #[tokio::test]
    async fn test_ttl_worker_config() {
        let config = TtlWorkerConfig::new(0, 64, 2);
        assert_eq!(config.replica_group_id, 0);
        assert_eq!(config.total_shards, 64);
        assert_eq!(config.rf, 2);
    }

    #[tokio::test]
    async fn test_ttl_worker_creation() {
        let topo = Arc::new(RwLock::new(make_test_topology()));
        let client = Arc::new(MockNodeClient::default());
        let task_store = Arc::new(crate::task_store::SqliteTaskStore::open_in_memory().unwrap());
        task_store.migrate().unwrap();
        let ttl_config = TtlConfig::default();

        let config = TtlWorkerConfig::new(0, 64, 2);
        let worker = TtlWorker::new(
            config,
            ttl_config,
            topo,
            client,
            task_store,
            "test-pod".to_string(),
        );

        let state = worker.state().await;
        assert_eq!(state.last_sweep_at, 0);
        assert_eq!(state.last_sweep_deleted, 0);
    }
}
