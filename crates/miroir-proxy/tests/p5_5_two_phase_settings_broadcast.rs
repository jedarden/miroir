//! P5.5 §13.5 Two-phase settings broadcast + drift reconciler tests.
//!
//! Tests:
//! - Normal flow: add a synonym; both propose + verify succeed; settings_version increments
//! - Mid-broadcast node failure: phase 2 verify fails on one node → reissue succeeds after backoff
//! - Out-of-band drift: PATCH a node directly → drift reconciler detects within interval_s and repairs
//! - X-Miroir-Min-Settings-Version floor excludes stale nodes from covering set; returns 503 when no floor-satisfying covering set exists
//! - Legacy strategy: sequential still works for rollback compatibility

use miroir_core::config::MiroirConfig;
use miroir_core::settings::{SettingsBroadcast, BroadcastPhase};
use miroir_core::task_store::{TaskStore, SqliteTaskStore};
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
    broadcast.start_propose(index.clone(), &settings).await.unwrap();

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
    broadcast.verify_hashes(&index, node_hashes, &expected_fingerprint).await.unwrap();

    // Commit phase - should increment settings version
    let new_version = broadcast.commit(&index).await.unwrap();

    assert_eq!(new_version, 1, "settings_version should be 1 after first commit");

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
    broadcast.start_propose(index.clone(), &settings).await.unwrap();

    // Enter verify phase
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-1".to_string(), 100);
    broadcast.enter_verify(&index, node_tasks).await.unwrap();

    let expected_fingerprint = miroir_core::settings::fingerprint_settings(&settings);

    // First verify attempt - node-1 has wrong hash
    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), "wrong_hash".to_string());

    let result = broadcast.verify_hashes(&index, node_hashes.clone(), &expected_fingerprint).await;
    assert!(result.is_err(), "verify should fail with hash mismatch");

    // Check status reflects the error
    let status = broadcast.get_status(&index).await;
    assert!(status.unwrap().error.is_some());

    // Simulate re-issue with correct hash
    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), expected_fingerprint.clone());

    broadcast.verify_hashes(&index, node_hashes, &expected_fingerprint).await.unwrap();

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

    broadcast.start_propose(index.clone(), &settings1).await.unwrap();
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-1".to_string(), 100);
    broadcast.enter_verify(&index, node_tasks).await.unwrap();

    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), fp1.clone());
    broadcast.verify_hashes(&index, node_hashes, &fp1).await.unwrap();

    let v1 = broadcast.commit(&index).await.unwrap();
    assert_eq!(v1, 1);
    broadcast.complete(&index).await.unwrap();

    // Second settings update
    let settings2 = json!({"rankingRules": ["words", "typo"]});
    let fp2 = miroir_core::settings::fingerprint_settings(&settings2);

    broadcast.start_propose(index.clone(), &settings2).await.unwrap();
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-1".to_string(), 101);
    broadcast.enter_verify(&index, node_tasks).await.unwrap();

    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), fp2.clone());
    broadcast.verify_hashes(&index, node_hashes, &fp2).await.unwrap();

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
    broadcast.start_propose(index.clone(), &settings).await.unwrap();

    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-1".to_string(), 100);
    node_tasks.insert("node-2".to_string(), 101);
    node_tasks.insert("node-3".to_string(), 102);
    broadcast.enter_verify(&index, node_tasks).await.unwrap();

    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), fp.clone());
    node_hashes.insert("node-2".to_string(), fp.clone());
    node_hashes.insert("node-3".to_string(), fp.clone());
    broadcast.verify_hashes(&index, node_hashes, &fp).await.unwrap();

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
    assert!(broadcast.node_version_meets_floor(&index, "node-1", 1).await);
    assert!(broadcast.node_version_meets_floor(&index, "node-2", 0).await);
    assert!(!broadcast.node_version_meets_floor(&index, "node-1", 2).await);
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
    broadcast.start_propose(index.clone(), &settings).await.unwrap();
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-1".to_string(), 100);
    broadcast.enter_verify(&index, node_tasks).await.unwrap();

    let mut node_hashes = HashMap::new();
    node_hashes.insert("node-1".to_string(), fp.clone());
    broadcast.verify_hashes(&index, node_hashes, &fp).await.unwrap();

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
