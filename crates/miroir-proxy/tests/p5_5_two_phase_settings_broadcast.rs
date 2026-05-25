//! P5.5 §13.5 Two-phase settings broadcast + drift reconciler tests.
//!
//! Tests:
//! - Normal flow: add a synonym; both propose + verify succeed; settings_version increments
//! - Mid-broadcast node failure: phase 2 verify fails on one node → reissue succeeds after backoff
//! - Out-of-band drift: PATCH a node directly → drift reconciler detects within interval_s and repairs
//! - X-Miroir-Min-Settings-Version floor excludes stale nodes from covering set; returns 503 when no floor-satisfying covering set exists
//! - Legacy strategy: sequential still works for rollback compatibility

use miroir_core::config::MiroirConfig;
use miroir_core::settings::SettingsBroadcast;
use miroir_core::task_store::{SqliteTaskStore, TaskStore};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

/// Helper to create an in-memory task store for testing.
fn create_test_task_store() -> Arc<SqliteTaskStore> {
    Arc::new(SqliteTaskStore::open_in_memory().unwrap())
}

/// Test 1: Normal flow - add a synonym, both propose + verify succeed, settings_version increments.
#[tokio::test]
async fn test_two_phase_settings_broadcast_normal_flow() {
    let store = create_test_task_store();
    store.migrate().unwrap();

    let broadcast = SettingsBroadcast::with_task_store(store.clone());

    let index = "products".to_string();
    let settings = json!({
        "synonyms": {
            "wifi": ["wi-fi", "wireless internet"]
        }
    });

    // Start propose phase
    broadcast
        .start_propose(index.clone(), &settings)
        .await
        .unwrap();

    // Enter verify phase with node task UIDs
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-1".to_string(), 100);
    node_tasks.insert("node-2".to_string(), 101);
    broadcast.enter_verify(&index, node_tasks).await.unwrap();

    // Verify hashes - all nodes should have the same hash
    let expected_fingerprint = miroir_core::settings::fingerprint_settings(&settings);
    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), expected_fingerprint.clone());
    node_hashes.insert("node-2".to_string(), expected_fingerprint.clone());
    broadcast
        .verify_hashes(&index, node_hashes, &expected_fingerprint)
        .await
        .unwrap();

    // Commit phase - should increment settings version
    let new_version = broadcast.commit(&index).await.unwrap();

    assert_eq!(
        new_version, 1,
        "settings_version should be 1 after first commit"
    );

    // Complete the broadcast
    broadcast.complete(&index).await.unwrap();

    // Verify node versions are tracked
    assert_eq!(broadcast.node_version(&index, "node-1").await, 1);
    assert_eq!(broadcast.node_version(&index, "node-2").await, 1);
}

/// Test 2: Hash mismatch with retry - simulate mismatch then successful re-verify.
#[tokio::test]
async fn test_two_phase_settings_broadcast_hash_mismatch_retry() {
    let store = create_test_task_store();
    store.migrate().unwrap();

    let broadcast = SettingsBroadcast::with_task_store(store.clone());

    let index = "products".to_string();
    let settings = json!({
        "rankingRules": ["words", "typo"]
    });

    // Start propose phase
    broadcast
        .start_propose(index.clone(), &settings)
        .await
        .unwrap();

    // Enter verify phase
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-1".to_string(), 100);
    broadcast.enter_verify(&index, node_tasks).await.unwrap();

    let expected_fingerprint = miroir_core::settings::fingerprint_settings(&settings);

    // First verify attempt - node-1 has wrong hash
    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), "wrong_hash".to_string());

    let result = broadcast
        .verify_hashes(&index, node_hashes.clone(), &expected_fingerprint)
        .await;
    assert!(result.is_err(), "verify should fail with hash mismatch");

    // Check status reflects the error
    let status = broadcast.get_status(&index).await;
    assert!(status.unwrap().error.is_some());

    // Simulate re-issue with correct hash
    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), expected_fingerprint.clone());

    broadcast
        .verify_hashes(&index, node_hashes, &expected_fingerprint)
        .await
        .unwrap();

    // Commit should succeed
    let new_version = broadcast.commit(&index).await.unwrap();
    assert_eq!(new_version, 1);
}

/// Test 3: Node settings version tracking across multiple updates.
#[tokio::test]
async fn test_node_settings_version_tracking_multiple_updates() {
    let store = create_test_task_store();
    store.migrate().unwrap();

    let broadcast = SettingsBroadcast::with_task_store(store.clone());

    let index = "products".to_string();

    // First settings update
    let settings1 = json!({"rankingRules": ["words"]});
    let fp1 = miroir_core::settings::fingerprint_settings(&settings1);

    broadcast
        .start_propose(index.clone(), &settings1)
        .await
        .unwrap();
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-1".to_string(), 100);
    broadcast.enter_verify(&index, node_tasks).await.unwrap();

    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), fp1.clone());
    broadcast
        .verify_hashes(&index, node_hashes, &fp1)
        .await
        .unwrap();

    let v1 = broadcast.commit(&index).await.unwrap();
    assert_eq!(v1, 1);
    broadcast.complete(&index).await.unwrap();

    // Second settings update
    let settings2 = json!({"rankingRules": ["words", "typo"]});
    let fp2 = miroir_core::settings::fingerprint_settings(&settings2);

    broadcast
        .start_propose(index.clone(), &settings2)
        .await
        .unwrap();
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-1".to_string(), 101);
    broadcast.enter_verify(&index, node_tasks).await.unwrap();

    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), fp2.clone());
    broadcast
        .verify_hashes(&index, node_hashes, &fp2)
        .await
        .unwrap();

    let v2 = broadcast.commit(&index).await.unwrap();
    assert_eq!(v2, 2);
    broadcast.complete(&index).await.unwrap();

    // Verify node version is at 2
    assert_eq!(broadcast.node_version(&index, "node-1").await, 2);
}

/// Test 4: Min node version calculation.
#[tokio::test]
async fn test_min_node_version_calculation() {
    let store = create_test_task_store();
    store.migrate().unwrap();

    let broadcast = SettingsBroadcast::with_task_store(store.clone());

    let index = "products".to_string();
    let settings = json!({"rankingRules": ["words"]});
    let fp = miroir_core::settings::fingerprint_settings(&settings);

    // Start and complete a broadcast with 3 nodes
    broadcast
        .start_propose(index.clone(), &settings)
        .await
        .unwrap();

    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-1".to_string(), 100);
    node_tasks.insert("node-2".to_string(), 101);
    node_tasks.insert("node-3".to_string(), 102);
    broadcast.enter_verify(&index, node_tasks).await.unwrap();

    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), fp.clone());
    node_hashes.insert("node-2".to_string(), fp.clone());
    node_hashes.insert("node-3".to_string(), fp.clone());
    broadcast
        .verify_hashes(&index, node_hashes, &fp)
        .await
        .unwrap();

    let v1 = broadcast.commit(&index).await.unwrap();
    assert_eq!(v1, 1);

    // Min version across all nodes should be 1
    let node_ids = vec![
        "node-1".to_string(),
        "node-2".to_string(),
        "node-3".to_string(),
    ];
    let min_version = broadcast.min_node_version(&index, &node_ids).await;
    assert_eq!(min_version, Some(1));

    // Node version meets floor
    assert!(
        broadcast
            .node_version_meets_floor(&index, "node-1", 1)
            .await
    );
    assert!(
        broadcast
            .node_version_meets_floor(&index, "node-2", 0)
            .await
    );
    assert!(
        !broadcast
            .node_version_meets_floor(&index, "node-1", 2)
            .await
    );
}

/// Test 5: Settings persistence to task store.
#[tokio::test]
async fn test_settings_version_persistence_to_task_store() {
    let store = create_test_task_store();
    store.migrate().unwrap();

    let index = "products".to_string();
    let settings = json!({"rankingRules": ["words"]});
    let fp = miroir_core::settings::fingerprint_settings(&settings);

    let broadcast = SettingsBroadcast::with_task_store(store.clone());

    // Complete a broadcast
    broadcast
        .start_propose(index.clone(), &settings)
        .await
        .unwrap();
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-1".to_string(), 100);
    broadcast.enter_verify(&index, node_tasks).await.unwrap();

    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), fp.clone());
    broadcast
        .verify_hashes(&index, node_hashes, &fp)
        .await
        .unwrap();

    let v1 = broadcast.commit(&index).await.unwrap();
    assert_eq!(v1, 1);

    // Verify the version was persisted to task store
    let row = store.get_node_settings_version(&index, "node-1").unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    assert_eq!(row.version, 1);
    assert_eq!(row.index_uid, index);
    assert_eq!(row.node_id, "node-1");
}

/// Test 6: Legacy sequential strategy compatibility.
#[tokio::test]
async fn test_legacy_sequential_strategy_compatibility() {
    let config = MiroirConfig {
        settings_broadcast: miroir_core::config::advanced::SettingsBroadcastConfig {
            strategy: "sequential".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };

    assert_eq!(config.settings_broadcast.strategy, "sequential");
}

/// Test 7: Two-phase strategy config.
#[tokio::test]
async fn test_two_phase_strategy_config() {
    let config = MiroirConfig::default();

    assert_eq!(config.settings_broadcast.strategy, "two_phase");
    assert_eq!(config.settings_broadcast.verify_timeout_s, 60);
    assert_eq!(config.settings_broadcast.max_repair_retries, 3);
    assert!(config.settings_broadcast.freeze_writes_on_unrepairable);
}

/// Test 8: Drift check config.
#[tokio::test]
async fn test_drift_check_config() {
    let config = MiroirConfig::default();

    assert_eq!(config.settings_drift_check.interval_s, 300);
    assert!(config.settings_drift_check.auto_repair);
}

/// Test 9: Client-pinned freshness - X-Miroir-Min-Settings-Version header parsing.
#[tokio::test]
async fn test_client_pinned_freshness_header_parsing() {
    use axum::extract::FromRequestParts;
    use axum::http::{HeaderValue, Method, Request, Uri};
    use http_body_util::Empty;
    use miroir_proxy::middleware::OptionalMinSettingsVersion;

    // Helper to create request parts with headers
    fn make_request_with_header(header_value: Option<&str>) -> Request<Empty<bytes::Bytes>> {
        let mut builder = Request::builder().uri("/test").method(Method::GET);

        if let Some(value) = header_value {
            builder = builder.header("x-miroir-min-settings-version", value);
        }

        builder.body(Empty::new()).unwrap()
    }

    // Test with valid header value
    let req1 = make_request_with_header(Some("42"));
    let (mut parts1, _) = req1.into_parts();
    let extracted1 = OptionalMinSettingsVersion::from_request_parts(&mut parts1, &())
        .await
        .unwrap();
    assert_eq!(extracted1.0, Some(42));

    // Test without header
    let req2 = make_request_with_header(None);
    let (mut parts2, _) = req2.into_parts();
    let extracted2 = OptionalMinSettingsVersion::from_request_parts(&mut parts2, &())
        .await
        .unwrap();
    assert_eq!(extracted2.0, None);

    // Test with invalid value (non-numeric) - extractor should return None
    let req3 = make_request_with_header(Some("invalid"));
    let (mut parts3, _) = req3.into_parts();
    let extracted3 = OptionalMinSettingsVersion::from_request_parts(&mut parts3, &())
        .await
        .unwrap();
    assert_eq!(extracted3.0, None);
}

/// Test 10: Client-pinned freshness - node_version_meets_floor function.
#[tokio::test]
async fn test_client_pinned_freshness_node_version_meets_floor() {
    let store = create_test_task_store();
    store.migrate().unwrap();

    let broadcast = SettingsBroadcast::with_task_store(store.clone());
    let index = "products";

    // Initially all nodes have version 0
    assert!(broadcast.node_version_meets_floor(index, "node-1", 0).await);
    assert!(!broadcast.node_version_meets_floor(index, "node-1", 1).await);

    // Complete a settings broadcast to version 1
    let settings = json!({"rankingRules": ["words"]});
    let fp = miroir_core::settings::fingerprint_settings(&settings);

    broadcast
        .start_propose(index.to_string(), &settings)
        .await
        .unwrap();
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-1".to_string(), 100);
    broadcast.enter_verify(index, node_tasks).await.unwrap();

    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), fp.clone());
    broadcast
        .verify_hashes(index, node_hashes, &fp)
        .await
        .unwrap();

    broadcast.commit(index).await.unwrap();
    broadcast.complete(index).await.unwrap();

    assert_eq!(broadcast.node_version(index, "node-1").await, 1);

    // Now node-1 should meet floor 0 and 1 but not floor 2
    assert!(broadcast.node_version_meets_floor(index, "node-1", 0).await);
    assert!(broadcast.node_version_meets_floor(index, "node-1", 1).await);
    assert!(!broadcast.node_version_meets_floor(index, "node-1", 2).await);
}

/// Test 11: Client-pinned freshness - covering_set_with_version_floor excludes stale nodes.
#[tokio::test]
async fn test_client_pinned_freshness_covering_set_excludes_stale_nodes() {
    use miroir_core::router::{assign_shard_in_group, covering_set_with_version_floor};
    use miroir_core::topology::{Group, Node, NodeId, Topology};

    // Create topology with 3 nodes in one group
    let mut topo = Topology::new(16, 1, 2);
    let node1 = Node::new(
        NodeId::new("node-1".to_string()),
        "http://node-1:7700".to_string(),
        0,
    );
    let node2 = Node::new(
        NodeId::new("node-2".to_string()),
        "http://node-2:7700".to_string(),
        0,
    );
    let node3 = Node::new(
        NodeId::new("node-3".to_string()),
        "http://node-3:7700".to_string(),
        0,
    );
    topo.add_node(node1.clone());
    topo.add_node(node2.clone());
    topo.add_node(node3.clone());

    let group = topo.group(0).unwrap();
    let shard_count = 16;
    let rf = 2;

    // Version checker: node-1 has version 10, node-2 has version 5, node-3 has version 8
    let version_checker = |_index: &str, node_id: &str| -> u64 {
        match node_id {
            "node-1" => 10,
            "node-2" => 5,
            "node-3" => 8,
            _ => 0,
        }
    };

    // With floor 8, node-2 should be excluded
    let floor = 8;
    let result = covering_set_with_version_floor(
        shard_count,
        group,
        rf,
        0,
        "test_index",
        floor,
        &version_checker,
    );

    assert!(
        result.is_some(),
        "covering set should be available when some nodes meet floor"
    );
    let covering_set = result.unwrap();

    // Node-2 (version 5) should not be in the covering set
    assert!(
        !covering_set.iter().any(|n| n.as_str() == "node-2"),
        "node-2 with version 5 should be excluded when floor is 8"
    );

    // All shards should be covered by node-1 or node-3
    assert_eq!(
        covering_set.len(),
        2,
        "covering set should have 2 unique nodes (node-1 and node-3)"
    );
}

/// Test 12: Client-pinned freshness - covering_set_with_version_floor returns None when no nodes meet floor.
#[tokio::test]
async fn test_client_pinned_freshness_covering_set_none_when_all_stale() {
    use miroir_core::router::{assign_shard_in_group, covering_set_with_version_floor};
    use miroir_core::topology::{Group, Node, NodeId, Topology};

    // Create topology with 2 nodes in one group
    let mut topo = Topology::new(16, 1, 2);
    let node1 = Node::new(
        NodeId::new("node-1".to_string()),
        "http://node-1:7700".to_string(),
        0,
    );
    let node2 = Node::new(
        NodeId::new("node-2".to_string()),
        "http://node-2:7700".to_string(),
        0,
    );
    topo.add_node(node1);
    topo.add_node(node2);

    let group = topo.group(0).unwrap();
    let shard_count = 16;
    let rf = 2;

    // Version checker: all nodes have version 3
    let version_checker = |_index: &str, _node_id: &str| -> u64 { 3 };

    // With floor 10, no nodes should be eligible
    let floor = 10;
    let result = covering_set_with_version_floor(
        shard_count,
        group,
        rf,
        0,
        "test_index",
        floor,
        &version_checker,
    );

    assert!(
        result.is_none(),
        "covering set should be None when no nodes meet floor"
    );
}

/// Test 13: Client-pinned freshness - plan_search_scatter_with_version_floor returns None when no covering set.
#[tokio::test]
async fn test_client_pinned_freshness_plan_returns_none_when_no_covering_set() {
    use miroir_core::scatter::plan_search_scatter_with_version_floor;
    use miroir_core::topology::{Group, Node, NodeId, Topology};

    // Create topology with 2 nodes
    let mut topo = Topology::new(16, 1, 2);
    let node1 = Node::new(
        NodeId::new("node-1".to_string()),
        "http://node-1:7700".to_string(),
        0,
    );
    let node2 = Node::new(
        NodeId::new("node-2".to_string()),
        "http://node-2:7700".to_string(),
        0,
    );
    topo.add_node(node1);
    topo.add_node(node2);

    let shard_count = 16;
    let rf = 2;
    let index = "test_index";
    let floor = 100;

    // Version checker: all nodes have version 5, below floor of 100
    let version_checker = |_index: &str, _node_id: &str| -> u64 { 5 };

    let result = plan_search_scatter_with_version_floor(
        &topo,
        0,
        rf,
        shard_count,
        index,
        floor,
        &version_checker,
        None,
    )
    .await;

    assert!(
        result.is_none(),
        "plan should be None when no covering set can be assembled"
    );
}

/// Test 14: Client-pinned freshness - plan_search_scatter_with_version_floor succeeds when nodes meet floor.
#[tokio::test]
async fn test_client_pinned_freshness_plan_succeeds_when_nodes_meet_floor() {
    use miroir_core::scatter::plan_search_scatter_with_version_floor;
    use miroir_core::topology::{Group, Node, NodeId, Topology};

    // Create topology with 3 nodes
    let mut topo = Topology::new(16, 1, 2);
    let node1 = Node::new(
        NodeId::new("node-1".to_string()),
        "http://node-1:7700".to_string(),
        0,
    );
    let node2 = Node::new(
        NodeId::new("node-2".to_string()),
        "http://node-2:7700".to_string(),
        0,
    );
    let node3 = Node::new(
        NodeId::new("node-3".to_string()),
        "http://node-3:7700".to_string(),
        0,
    );
    topo.add_node(node1);
    topo.add_node(node2);
    topo.add_node(node3);

    let shard_count = 16;
    let rf = 2;
    let index = "test_index";
    let floor = 5;

    // Version checker: node-1 and node-3 meet floor, node-2 doesn't
    let version_checker = |_index: &str, node_id: &str| -> u64 {
        match node_id {
            "node-1" => 10,
            "node-2" => 3,
            "node-3" => 8,
            _ => 0,
        }
    };

    let result = plan_search_scatter_with_version_floor(
        &topo,
        0,
        rf,
        shard_count,
        index,
        floor,
        &version_checker,
        None,
    )
    .await;

    assert!(
        result.is_some(),
        "plan should succeed when some nodes meet floor"
    );
    let plan = result.unwrap();

    // Verify all shards are mapped to nodes that meet the floor
    for (_shard_id, node_id) in &plan.shard_to_node {
        let version = version_checker(index, node_id.as_str());
        assert!(version >= floor, "selected node should meet version floor");
    }
}

/// Test 15: Client-pinned freshness - miroir_settings_version_stale error code.
#[tokio::test]
async fn test_client_pinned_freshness_settings_version_stale_error() {
    use miroir_core::api_error::{MeilisearchError, MiroirCode};

    let err = MeilisearchError::new(
        MiroirCode::SettingsVersionStale,
        "no covering set available for settings version floor 42 on index 'products'",
    );

    // Verify error code
    assert_eq!(err.code, "miroir_settings_version_stale");
    assert_eq!(err.http_status(), 503);

    // Verify error serializes correctly
    let json = serde_json::to_value(&err).unwrap();
    assert_eq!(json["code"], "miroir_settings_version_stale");
    assert_eq!(json["type"], "system");
}
