//! Background worker for syncing documents to a new replica group.
//!
//! Implements the document sync phase of group addition (plan §2 step 3):
//! - For each shard, copy all docs from any healthy existing group
//! - Uses pagination with filter=_miroir_shard={id}
//! - Tracks progress via GroupAdditionCoordinator
//! - Pauses/resumes per Phase 6 Mode C

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn};

use crate::group_addition::{GroupAdditionCoordinator, GroupAdditionId};
use crate::migration::ShardId;
use crate::scatter::FetchDocumentsRequest;
use crate::topology::{NodeId, Topology};
use crate::Result;

/// Configuration for the group sync worker.
#[derive(Debug, Clone)]
pub struct GroupSyncWorkerConfig {
    /// Interval between sync iterations.
    pub sync_interval: Duration,
    /// Timeout for individual fetch operations.
    pub fetch_timeout: Duration,
    /// Maximum retries for failed fetches.
    pub max_retries: usize,
}

impl Default for GroupSyncWorkerConfig {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_millis(100),
            fetch_timeout: Duration::from_secs(30),
            max_retries: 3,
        }
    }
}

/// A job representing a single shard sync operation.
#[derive(Debug, Clone)]
pub struct ShardSyncJob {
    pub addition_id: GroupAdditionId,
    pub shard_id: ShardId,
    pub source_group: u32,
    pub offset: u32,
    pub limit: u32,
    pub total_estimated: u64,
    pub docs_copied: u64,
    pub retries: usize,
}

/// NodeClient trait for fetching documents during sync.
#[allow(async_fn_in_trait)]
pub trait SyncNodeClient: Send + Sync {
    /// Fetch documents from a node with pagination.
    async fn fetch_documents(
        &self,
        node: &NodeId,
        address: &str,
        request: &FetchDocumentsRequest,
    ) -> std::result::Result<serde_json::Value, String>;

    /// Write documents to a node.
    async fn write_documents(
        &self,
        node: &NodeId,
        address: &str,
        index_uid: &str,
        documents: Vec<serde_json::Value>,
    ) -> std::result::Result<(), String>;
}

/// The group sync worker handles background document sync for group addition.
pub struct GroupSyncWorker<C: SyncNodeClient> {
    config: GroupSyncWorkerConfig,
    coordinator: Arc<RwLock<GroupAdditionCoordinator>>,
    node_client: Arc<C>,
    topology: Arc<RwLock<Topology>>,
}

impl<C: SyncNodeClient> GroupSyncWorker<C> {
    pub fn new(
        config: GroupSyncWorkerConfig,
        coordinator: Arc<RwLock<GroupAdditionCoordinator>>,
        node_client: Arc<C>,
        topology: Arc<RwLock<Topology>>,
    ) -> Self {
        Self {
            config,
            coordinator,
            node_client,
            topology,
        }
    }

    /// Run a single sync iteration for all active group additions.
    #[instrument(skip_all, fields(additions_count))]
    pub async fn sync_iteration(&self) -> Result<usize> {
        let coordinator = self.coordinator.read().await;
        let additions = coordinator.get_all_additions().clone();
        drop(coordinator);

        let mut jobs_completed = 0;

        for (addition_id, state) in additions {
            // Only sync additions in Syncing phase
            if !matches!(
                state.phase,
                crate::group_addition::GroupAdditionPhase::Syncing
            ) {
                continue;
            }

            // Find pending shards to sync
            let pending_shards: Vec<_> = state
                .shard_states
                .iter()
                .filter(|(_, s)| matches!(s, crate::group_addition::ShardSyncState::Syncing { .. }))
                .map(|(shard, s)| (*shard, s.clone()))
                .collect();

            if pending_shards.is_empty() {
                continue;
            }

            // Sync each pending shard
            for (shard_id, shard_state) in pending_shards {
                let source_group = match shard_state {
                    crate::group_addition::ShardSyncState::Syncing { source_group, .. } => {
                        source_group
                    }
                    _ => continue,
                };

                match self.sync_shard(addition_id, shard_id, source_group).await {
                    Ok(docs_copied) => {
                        jobs_completed += 1;
                        let mut coord = self.coordinator.write().await;
                        if docs_copied > 0 {
                            // Update progress
                            let _ = coord.shard_sync_progress(addition_id, shard_id, docs_copied);
                        }
                    }
                    Err(e) => {
                        warn!(
                            addition_id = %addition_id,
                            shard_id = shard_id.0,
                            error = %e,
                            "Shard sync failed, will retry"
                        );
                    }
                }
            }
        }

        Ok(jobs_completed)
    }

    /// Sync a single shard from source group to new group.
    #[instrument(skip_all, fields(addition_id, shard_id, source_group))]
    async fn sync_shard(
        &self,
        addition_id: GroupAdditionId,
        shard_id: ShardId,
        source_group: u32,
    ) -> Result<u64> {
        let topology = self.topology.read().await;
        let page_size = self.coordinator.read().await.config().sync_page_size;

        // Get source group
        let source = topology.group(source_group).ok_or_else(|| {
            crate::error::MiroirError::Topology(format!("source group {} not found", source_group))
        })?;

        // Get target group (the new group being added)
        let target_group_id = {
            let coord = self.coordinator.read().await;
            let state = coord.get_state(addition_id).ok_or_else(|| {
                crate::error::MiroirError::Topology(format!("addition {} not found", addition_id))
            })?;
            state.group_id
        };

        let target = topology.group(target_group_id).ok_or_else(|| {
            crate::error::MiroirError::Topology(format!(
                "target group {} not found",
                target_group_id
            ))
        })?;

        // Find healthy nodes in source and target groups
        let node_map = topology.node_map();
        let source_healthy = source.healthy_nodes(&node_map);
        let target_healthy = target.healthy_nodes(&node_map);

        let source_node = source_healthy.first().ok_or_else(|| {
            crate::error::MiroirError::Topology(format!(
                "no healthy nodes in source group {}",
                source_group
            ))
        })?;

        let target_node = target_healthy.first().ok_or_else(|| {
            crate::error::MiroirError::Topology(format!(
                "no healthy nodes in target group {}",
                target_group_id
            ))
        })?;

        let source_node_id = source_node.id.clone();
        let source_node_address = source_node.address.clone();
        let target_node_id = target_node.id.clone();
        let target_node_address = target_node.address.clone();

        drop(topology);

        // Sync documents with pagination
        let mut offset = 0u32;
        let mut total_copied = 0u64;
        let mut has_more = true;

        while has_more {
            let filter_value = serde_json::json!(shard_id.0);

            let fetch_req = FetchDocumentsRequest {
                index_uid: "_miroir_all_docs".to_string(),
                filter: serde_json::json!({"_miroir_shard": filter_value}),
                limit: page_size,
                offset,
            };

            // Fetch from source
            let docs = tokio::time::timeout(
                self.config.fetch_timeout,
                self.node_client
                    .fetch_documents(&source_node_id, &source_node_address, &fetch_req),
            )
            .await
            .map_err(|_| {
                crate::error::MiroirError::Routing(format!(
                    "fetch timeout for shard {} from group {}",
                    shard_id, source_group
                ))
            })?
            .map_err(|e| {
                crate::error::MiroirError::Routing(format!(
                    "fetch failed for shard {}: {}",
                    shard_id, e
                ))
            })?;

            // Parse response
            let results = docs
                .get("results")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    crate::error::MiroirError::Routing(
                        "invalid response: missing results array".to_string(),
                    )
                })?;

            let total = docs.get("total").and_then(|v| v.as_u64()).unwrap_or(0);

            if results.is_empty() {
                has_more = false;
                break;
            }

            // Write to target
            self.node_client
                .write_documents(
                    &target_node_id,
                    &target_node_address,
                    &fetch_req.index_uid,
                    results.clone(),
                )
                .await
                .map_err(|e| {
                    crate::error::MiroirError::Routing(format!(
                        "write failed for shard {}: {}",
                        shard_id, e
                    ))
                })?;

            let count = results.len() as u64;
            total_copied += count;

            debug!(
                addition_id = %addition_id,
                shard_id = shard_id.0,
                offset,
                count,
                total_copied,
                total,
                "Synced page"
            );

            // Check if we're done
            has_more = (offset as u64 + count) < total;
            offset += page_size;
        }

        // Mark shard as complete
        let mut coord = self.coordinator.write().await;
        coord
            .shard_sync_complete(addition_id, shard_id, total_copied)
            .map_err(|e| {
                crate::error::MiroirError::Topology(format!("failed to mark shard complete: {}", e))
            })?;

        info!(
            addition_id = %addition_id,
            shard_id = shard_id.0,
            total_copied,
            "Shard sync complete"
        );

        Ok(total_copied)
    }

    /// Check for sync timeout and fail additions that have exceeded the limit.
    #[instrument(skip_all)]
    pub async fn check_timeouts(&self) -> Result<()> {
        let coord = self.coordinator.write().await;
        let additions = coord.get_all_additions().clone();
        drop(coord);

        let timeout = Duration::from_secs(3600); // 1 hour default

        for (addition_id, state) in additions {
            if !matches!(
                state.phase,
                crate::group_addition::GroupAdditionPhase::Syncing
            ) {
                continue;
            }

            if let Some(started_at) = state.started_at {
                if started_at.elapsed() > timeout {
                    warn!(
                        addition_id = %addition_id,
                        group_id = state.group_id,
                        "Group addition sync timeout"
                    );

                    let mut coord = self.coordinator.write().await;
                    let _ = coord
                        .fail_addition(addition_id, format!("sync timeout after {:?}", timeout));
                }
            }
        }

        Ok(())
    }

    /// Run the sync worker loop continuously.
    pub async fn run(&self) -> Result<()> {
        info!("Starting group sync worker");

        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.config.sync_interval) => {
                    if let Err(e) = self.sync_iteration().await {
                        error!("Sync iteration failed: {}", e);
                    }

                    if let Err(e) = self.check_timeouts().await {
                        error!("Timeout check failed: {}", e);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::group_addition::{GroupAdditionConfig, GroupAdditionCoordinator};
    use crate::scatter::FetchDocumentsResponse;
    use std::sync::Arc;

    // Mock node client for testing
    struct MockSyncClient {
        fetch_responses: Arc<RwLock<HashMap<(NodeId, String), serde_json::Value>>>,
        write_calls: Arc<RwLock<Vec<(NodeId, String, Vec<serde_json::Value>)>>>,
    }

    #[allow(unused_variables)]
    impl SyncNodeClient for MockSyncClient {
        async fn fetch_documents(
            &self,
            node: &NodeId,
            address: &str,
            request: &FetchDocumentsRequest,
        ) -> std::result::Result<serde_json::Value, String> {
            let key = (
                node.clone(),
                format!("{}-{}-{}", request.index_uid, request.offset, request.limit),
            );
            let responses = self.fetch_responses.read().await;
            Ok(responses.get(&key).cloned().unwrap_or_else(|| {
                serde_json::json!({
                    "results": [],
                    "limit": request.limit,
                    "offset": request.offset,
                    "total": 0
                })
            }))
        }

        async fn write_documents(
            &self,
            node: &NodeId,
            address: &str,
            index_uid: &str,
            documents: Vec<serde_json::Value>,
        ) -> std::result::Result<(), String> {
            let mut calls = self.write_calls.write().await;
            calls.push((node.clone(), index_uid.to_string(), documents));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_sync_shard_empty() {
        let config = GroupAdditionConfig::default();
        let coord = Arc::new(RwLock::new(GroupAdditionCoordinator::new(config)));
        let fetch_responses = Arc::new(RwLock::new(HashMap::new()));
        let write_calls = Arc::new(RwLock::new(Vec::new()));

        let client = Arc::new(MockSyncClient {
            fetch_responses,
            write_calls,
        });

        let topology = Arc::new(RwLock::new(Topology::new(16, 2, 1)));

        // Add nodes to groups
        topology.write().await.add_node(Node::new(
            NodeId::new("source-0".to_string()),
            "http://source-0:7700".to_string(),
            0,
        ));
        topology.write().await.add_node(Node::new(
            NodeId::new("target-0".to_string()),
            "http://target-0:7700".to_string(),
            1,
        ));

        // Activate nodes
        {
            let mut topo = topology.write().await;
            topo.node_mut(&NodeId::new("source-0".to_string()))
                .unwrap()
                .status = crate::topology::NodeStatus::Active;
            topo.node_mut(&NodeId::new("target-0".to_string()))
                .unwrap()
                .status = crate::topology::NodeStatus::Active;
        }

        let worker = GroupSyncWorker::new(
            GroupSyncWorkerConfig::default(),
            coord.clone(),
            client,
            topology,
        );

        // Start addition
        let id = coord.write().await.begin_addition(1, 16, &[0]).unwrap();
        coord.write().await.begin_sync(id).unwrap();

        // Sync a shard (empty source)
        let result = worker.sync_shard(id, ShardId(0), 0).await;

        // Should succeed with 0 docs
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_worker_config_default() {
        let config = GroupSyncWorkerConfig::default();
        assert_eq!(config.sync_interval, Duration::from_millis(100));
        assert_eq!(config.fetch_timeout, Duration::from_secs(30));
        assert_eq!(config.max_retries, 3);
    }
}
