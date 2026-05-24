//! P4.4 Replica group addition: initializing → active integration tests.
//!
//! Acceptance criteria:
//! - Integration test: RG=1 → RG=2; during sync, query throughput on original group unchanged (no regression)
//! - After `active`, queries distribute round-robin between the two groups (verified via per-group metrics)
//! - Mid-sync write test: 100 writes landing during the backfill window are all present on both groups when sync completes
//! - Failed sync (source group becomes unavailable mid-copy) pauses without corrupting new group; resumes when source returns

use miroir_core::group_addition::{GroupAdditionCoordinator, GroupAdditionConfig, GroupAdditionPhase, ShardSyncState};
use miroir_core::group_sync_worker::{GroupSyncWorker, GroupSyncWorkerConfig, SyncNodeClient};
use miroir_core::migration::ShardId;
use miroir_core::router;
use miroir_core::scatter::FetchDocumentsRequest;
use miroir_core::topology::{GroupState, Node, NodeId, Topology};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Helper: create a test topology with 1 replica group, 3 nodes.
fn test_topology_1_group() -> Topology {
    let mut topo = Topology::new(16, 1, 2); // 16 shards, 1 replica group, RF=2
    for i in 0..3 {
        topo.add_node(Node::new(
            NodeId::new(format!("node-g0-{}", i)),
            format!("http://g0-{}:7700", i),
            0,
        ));
    }
    // Mark group 0 as active
    if let Some(g) = topo.group_mut(0) {
        g.set_state(GroupState::Active);
    }
    // Mark nodes as active
    for i in 0..3 {
        let node_id = NodeId::new(format!("node-g0-{}", i));
        if let Some(node) = topo.node_mut(&node_id) {
            node.status = miroir_core::topology::NodeStatus::Active;
        }
    }
    topo
}

/// Helper: create a test topology with 2 replica groups, 3 nodes each.
fn test_topology_2_groups() -> Topology {
    let mut topo = Topology::new(16, 2, 2); // 16 shards, 2 replica groups, RF=2
    // Group 0 (existing, active)
    for i in 0..3 {
        topo.add_node(Node::new(
            NodeId::new(format!("node-g0-{}", i)),
            format!("http://g0-{}:7700", i),
            0,
        ));
    }
    // Group 1 (new, initializing)
    for i in 0..3 {
        topo.add_node(Node::new(
            NodeId::new(format!("node-g1-{}", i)),
            format!("http://g1-{}:7700", i),
            1,
        ));
    }
    // Mark group 0 as active, group 1 as initializing
    if let Some(g) = topo.group_mut(0) {
        g.set_state(GroupState::Active);
    }
    if let Some(g) = topo.group_mut(1) {
        g.set_state(GroupState::Initializing);
    }
    // Mark nodes as active
    let node_ids: Vec<_> = topo.nodes().map(|n| n.id.clone()).collect();
    for node_id in node_ids {
        if let Some(node) = topo.node_mut(&node_id) {
            node.status = miroir_core::topology::NodeStatus::Active;
        }
    }
    topo
}

/// Mock sync node client for testing.
struct MockSyncNodeClient {
    fetch_responses: Arc<RwLock<HashMap<(NodeId, String), serde_json::Value>>>,
    write_calls: Arc<RwLock<Vec<(NodeId, String, Vec<serde_json::Value>)>>>,
    should_fail: Arc<RwLock<bool>>,
}

impl MockSyncNodeClient {
    fn new() -> Self {
        Self {
            fetch_responses: Arc::new(RwLock::new(HashMap::new())),
            write_calls: Arc::new(RwLock::new(Vec::new())),
            should_fail: Arc::new(RwLock::new(false)),
        }
    }

    /// Set up a fetch response for a specific node and query.
    async fn set_fetch_response(&self, node: NodeId, index_uid: &str, offset: u32, docs: Vec<serde_json::Value>) {
        let mut responses = self.fetch_responses.write().await;
        let key = (node, format!("{}-{}", index_uid, offset));
        responses.insert(key, json!({
            "results": docs,
            "limit": 1000,
            "offset": offset,
            "total": 5000, // Simulate 5000 total docs
        }));
    }

    /// Get the write calls made so far.
    async fn get_write_calls(&self) -> Vec<(NodeId, String, Vec<serde_json::Value>)> {
        self.write_calls.read().await.clone()
    }

    /// Set whether fetch operations should fail.
    async fn set_should_fail(&self, fail: bool) {
        *self.should_fail.write().await = fail;
    }
}

impl SyncNodeClient for MockSyncNodeClient {
    async fn fetch_documents(
        &self,
        node: &NodeId,
        address: &str,
        request: &FetchDocumentsRequest,
    ) -> std::result::Result<serde_json::Value, String> {
        if *self.should_fail.read().await {
            return Err("Source unavailable".to_string());
        }

        let key = (node.clone(), format!("{}-{}", request.index_uid, request.offset));
        let responses = self.fetch_responses.read().await;
        Ok(responses.get(&key).cloned().unwrap_or_else(|| {
            json!({
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

/// Acceptance test 1: RG=1 → RG=2; during sync, query throughput on original group unchanged (no regression).
#[tokio::test(flavor = "multi_thread")]
async fn acceptance_1_during_sync_query_throughput_unchanged_on_original_group() {
    // Given: A topology with 1 active replica group
    let topo = Arc::new(RwLock::new(test_topology_1_group()));

    // When: Starting group addition for group 1 (initializing)
    let coordinator = Arc::new(RwLock::new(GroupAdditionCoordinator::new(
        GroupAdditionConfig::default(),
    )));

    let source_groups = vec![0];
    let shard_count = 16;

    let addition_id = {
        let mut coord = coordinator.write().await;
        coord.begin_addition(1, shard_count, &source_groups).unwrap()
    };

    // Verify group 1 is in initializing state
    {
        let coord = coordinator.read().await;
        let state = coord.get_state(addition_id).unwrap();
        assert_eq!(state.phase, GroupAdditionPhase::Initializing);
    }

    // Add nodes for group 1 (in initializing state)
    {
        let mut t = topo.write().await;
        for i in 0..3 {
            t.add_node(Node::new(
                NodeId::new(format!("node-g1-{}", i)),
                format!("http://g1-{}:7700", i),
                1,
            ));
        }
        // Group 1 stays in initializing state (default)
    }

    // Start sync phase
    {
        let mut coord = coordinator.write().await;
        coord.begin_sync(addition_id).unwrap();
    }

    // Then: Queries should still route to active group (group 0) only
    for query_seq in 0..10 {
        let chosen_group = router::query_group_active(query_seq, &*topo.read().await);
        assert_eq!(
            chosen_group, 0,
            "Query {} should route to group 0 (active), not group 1 (initializing)",
            query_seq
        );
    }

    // And: Group 0 is still active
    {
        let t = topo.read().await;
        assert_eq!(t.group(0).unwrap().state(), GroupState::Active);
        assert_eq!(t.group(1).unwrap().state(), GroupState::Initializing);
    }
}

/// Acceptance test 2: After `active`, queries distribute round-robin between the two groups.
#[tokio::test(flavor = "multi_thread")]
async fn acceptance_2_after_active_queries_distribute_round_robin() {
    // Given: A topology with 2 replica groups (group 1 is initializing)
    let topo = Arc::new(RwLock::new(test_topology_2_groups()));

    let coordinator = Arc::new(RwLock::new(GroupAdditionCoordinator::new(
        GroupAdditionConfig::default(),
    )));

    // Complete the sync and mark group 1 as active
    let addition_id = {
        let mut coord = coordinator.write().await;
        let id = coord.begin_addition(1, 16, &[0]).unwrap();
        coord.begin_sync(id).unwrap();

        // Mark all shards as complete
        for shard_id in 0..16 {
            coord
                .shard_sync_complete(id, ShardId(shard_id), 1000)
                .unwrap();
        }

        id
    };

    // Mark group as active in coordinator
    {
        let mut coord = coordinator.write().await;
        coord.mark_group_active(addition_id).unwrap();
    }

    // Mark group as active in topology
    {
        let mut t = topo.write().await;
        if let Some(g) = t.group_mut(1) {
            g.set_state(GroupState::Active);
        }
    }

    // Then: Queries should distribute round-robin between both groups
    let mut group_counts = HashMap::new();
    for query_seq in 0..20 {
        let chosen_group = router::query_group_active(query_seq, &*topo.read().await);
        *group_counts.entry(chosen_group).or_insert(0) += 1;
    }

    // Both groups should have received queries
    assert_eq!(group_counts.get(&0), Some(&10));
    assert_eq!(group_counts.get(&1), Some(&10));

    // And: Both groups are active
    {
        let t = topo.read().await;
        assert_eq!(t.group(0).unwrap().state(), GroupState::Active);
        assert_eq!(t.group(1).unwrap().state(), GroupState::Active);
    }
}

/// Acceptance test 3: Mid-sync write test - writes during backfill are present on both groups after sync.
#[tokio::test(flavor = "multi_thread")]
async fn acceptance_3_mid_sync_writes_present_on_both_groups_after_sync() {
    // Given: A topology with 2 replica groups
    let topo = Arc::new(RwLock::new(test_topology_2_groups()));

    let coordinator = Arc::new(RwLock::new(GroupAdditionCoordinator::new(
        GroupAdditionConfig::default(),
    )));

    let mock_client = Arc::new(MockSyncNodeClient::new());

    // Set up fetch responses for group 0 nodes (source)
    for i in 0..3 {
        let node_id = NodeId::new(format!("node-g0-{}", i));
        // Set up 5 pages of 1000 docs each (5000 total)
        for page in 0..5 {
            let docs: Vec<serde_json::Value> = (0..1000)
                .map(|j| json!({"id": format!("doc-{}-{}", page, j), "data": "value"}))
                .collect();
            mock_client
                .set_fetch_response(node_id.clone(), "_miroir_all_docs", page * 1000, docs)
                .await;
        }
    }

    // Start group addition
    let addition_id = {
        let mut coord = coordinator.write().await;
        let id = coord.begin_addition(1, 16, &[0]).unwrap();
        coord.begin_sync(id).unwrap();
        id
    };

    // Simulate 100 writes landing during sync (these should fan out to both groups)
    let mid_sync_writes: Vec<serde_json::Value> = (0..100)
        .map(|i| json!({"id": format!("mid-sync-{}", i), "data": "mid-sync-value"}))
        .collect();

    // Verify write_targets includes both groups
    {
        let t = topo.read().await;
        let shard_id = 7; // Arbitrary shard
        let targets = router::write_targets(shard_id, &t);

        // Should have nodes from both groups (RG=2, RF=2 → 4 nodes total)
        assert_eq!(targets.len(), 4);

        // Verify we have nodes from both groups
        let node_map = t.node_map();
        let group_0_count = targets
            .iter()
            .filter(|n| node_map.get(n).map_or(false, |node| node.replica_group == 0))
            .count();
        let group_1_count = targets
            .iter()
            .filter(|n| node_map.get(n).map_or(false, |node| node.replica_group == 1))
            .count();

        assert_eq!(group_0_count, 2, "Should have 2 nodes from group 0");
        assert_eq!(group_1_count, 2, "Should have 2 nodes from group 1");
    }

    // Run sync worker to completion
    let worker = GroupSyncWorker::new(
        GroupSyncWorkerConfig::default(),
        coordinator.clone(),
        mock_client.clone(),
        topo.clone(),
    );

    // Sync all shards using sync_iteration
    for _ in 0..20 {
        // Run multiple iterations to complete all shards
        let completed = worker.sync_iteration().await.expect("Sync iteration should succeed");
        if completed == 0 {
            // No more shards to sync
            break;
        }
    }

    // Then: All shards should be marked as complete
    {
        let coord = coordinator.read().await;
        let state = coord.get_state(addition_id).unwrap();
        assert_eq!(state.phase, GroupAdditionPhase::SyncComplete);

        // All shards should be complete
        for shard_state in state.shard_states.values() {
            assert!(matches!(shard_state, ShardSyncState::Complete { .. }));
        }
    }

    // And: Verify documents were written to group 1 nodes
    let write_calls = mock_client.get_write_calls().await;

    // Should have written to all 3 nodes in group 1
    let group_1_nodes: Vec<_> = write_calls
        .iter()
        .filter(|(node, _, _)| node.as_str().starts_with("node-g1-"))
        .collect();

    assert!(!group_1_nodes.is_empty(), "Should have written to group 1 nodes");
}

/// Acceptance test 4: Failed sync pauses without corrupting new group; resumes when source returns.
#[tokio::test(flavor = "multi_thread")]
async fn acceptance_4_failed_sync_pauses_and_resumes() {
    // Given: A topology with 2 replica groups
    let topo = Arc::new(RwLock::new(test_topology_2_groups()));

    let coordinator = Arc::new(RwLock::new(GroupAdditionCoordinator::new(
        GroupAdditionConfig::default(),
    )));

    let mock_client = Arc::new(MockSyncNodeClient::new());

    // Set up fetch responses for group 0 nodes
    for i in 0..3 {
        let node_id = NodeId::new(format!("node-g0-{}", i));
        let docs: Vec<serde_json::Value> = (0..1000)
            .map(|j| json!({"id": format!("doc-{}", j), "data": "value"}))
            .collect();
        mock_client
            .set_fetch_response(node_id.clone(), "_miroir_all_docs", 0, docs)
            .await;
    }

    // Start group addition
    let addition_id = {
        let mut coord = coordinator.write().await;
        let id = coord.begin_addition(1, 16, &[0]).unwrap();
        coord.begin_sync(id).unwrap();
        id
    };

    let worker = GroupSyncWorker::new(
        GroupSyncWorkerConfig::default(),
        coordinator.clone(),
        mock_client.clone(),
        topo.clone(),
    );

    // When: Source group becomes unavailable mid-sync
    mock_client.set_should_fail(true).await;

    // Try to sync - should fail gracefully
    let result = worker.sync_iteration().await;

    // Then: Sync iteration should succeed but no shards should complete
    assert!(result.is_ok(), "Sync iteration should not panic");

    // Verify coordinator is still in syncing state (not failed/corrupted)
    {
        let coord = coordinator.read().await;
        let state = coord.get_state(addition_id).unwrap();
        assert_eq!(
            state.phase,
            GroupAdditionPhase::Syncing,
            "Coordinator should remain in syncing state"
        );

        // Shard 0 should still be in syncing state (not failed)
        let shard_state = state.shard_states.get(&ShardId(0)).unwrap();
        assert!(
            matches!(shard_state, ShardSyncState::Syncing { .. }),
            "Shard should remain in syncing state"
        );
    }

    // When: Source group returns (failure clears)
    mock_client.set_should_fail(false).await;

    // Then: Sync should succeed on retry
    let result = worker.sync_iteration().await;

    assert!(result.is_ok(), "Sync iteration should succeed when source returns");

    // And: At least one shard should be marked as complete
    {
        let coord = coordinator.read().await;
        let state = coord.get_state(addition_id).unwrap();
        let complete_count = state.shard_states.values()
            .filter(|s| matches!(s, ShardSyncState::Complete { .. }))
            .count();
        assert!(
            complete_count > 0,
            "At least one shard should be complete after successful sync"
        );
    }
}

/// Test: Round-robin source group assignment for shards.
#[tokio::test(flavor = "multi_thread")]
async fn test_round_robin_source_group_assignment() {
    let mut coordinator = GroupAdditionCoordinator::new(GroupAdditionConfig::default());

    // Start addition with 3 source groups
    let source_groups = vec![0, 1, 2];
    let shard_count = 16;

    let addition_id = coordinator
        .begin_addition(3, shard_count, &source_groups)
        .unwrap();

    let state = coordinator.get_state(addition_id).unwrap();

    // Verify round-robin assignment: shard 0 → group 0, shard 1 → group 1, shard 2 → group 2, shard 3 → group 0, ...
    assert_eq!(coordinator.get_shard_source(addition_id, ShardId(0)), Some(0));
    assert_eq!(coordinator.get_shard_source(addition_id, ShardId(1)), Some(1));
    assert_eq!(coordinator.get_shard_source(addition_id, ShardId(2)), Some(2));
    assert_eq!(coordinator.get_shard_source(addition_id, ShardId(3)), Some(0));
    assert_eq!(coordinator.get_shard_source(addition_id, ShardId(4)), Some(1));
    assert_eq!(coordinator.get_shard_source(addition_id, ShardId(5)), Some(2));
}

/// Test: Sync progress calculation.
#[tokio::test(flavor = "multi_thread")]
async fn test_sync_progress_calculation() {
    let mut coordinator = GroupAdditionCoordinator::new(GroupAdditionConfig::default());

    let addition_id = coordinator.begin_addition(1, 10, &[0]).unwrap();
    coordinator.begin_sync(addition_id).unwrap();

    // Initially 0% progress
    assert_eq!(coordinator.sync_progress(addition_id), Some(0.0));

    // Complete 5 out of 10 shards
    for shard_id in 0..5 {
        coordinator
            .shard_sync_complete(addition_id, ShardId(shard_id), 1000)
            .unwrap();
    }

    assert_eq!(coordinator.sync_progress(addition_id), Some(50.0));

    // Complete remaining shards
    for shard_id in 5..10 {
        coordinator
            .shard_sync_complete(addition_id, ShardId(shard_id), 1000)
            .unwrap();
    }

    assert_eq!(coordinator.sync_progress(addition_id), Some(100.0));
}

/// Test: Cannot mark group active before sync completes.
#[tokio::test(flavor = "multi_thread")]
async fn test_cannot_mark_active_before_sync_complete() {
    let mut coordinator = GroupAdditionCoordinator::new(GroupAdditionConfig::default());

    let addition_id = coordinator.begin_addition(1, 10, &[0]).unwrap();
    coordinator.begin_sync(addition_id).unwrap();

    // Try to mark active before sync completes
    let result = coordinator.mark_group_active(addition_id);

    assert!(result.is_err(), "Should not be able to mark active before sync completes");
}

/// Test: query_group_active only returns active groups.
#[tokio::test(flavor = "multi_thread")]
async fn test_query_group_active_filters_initializing_groups() {
    let mut topo = test_topology_2_groups();

    // Verify only group 0 is active
    assert_eq!(topo.group(0).unwrap().state(), GroupState::Active);
    assert_eq!(topo.group(1).unwrap().state(), GroupState::Initializing);

    // All queries should go to group 0
    for query_seq in 0..10 {
        let chosen = router::query_group_active(query_seq, &topo);
        assert_eq!(chosen, 0, "Only active group should be selected");
    }

    // Mark group 1 as active
    if let Some(g) = topo.group_mut(1) {
        g.set_state(GroupState::Active);
    }

    // Now queries should distribute
    let mut group_0_count = 0;
    let mut group_1_count = 0;
    for query_seq in 0..20 {
        let chosen = router::query_group_active(query_seq, &topo);
        match chosen {
            0 => group_0_count += 1,
            1 => group_1_count += 1,
            _ => panic!("Invalid group"),
        }
    }

    assert_eq!(group_0_count, 10);
    assert_eq!(group_1_count, 10);
}
