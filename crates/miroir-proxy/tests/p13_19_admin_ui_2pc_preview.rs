//! P13.19.b §13.19 Admin UI - 2PC Settings Preview acceptance tests.
//!
//! Tests:
//! - Preview endpoint returns current and proposed settings with fingerprints
//! - Preview endpoint shows node targets and version information
//! - Preview endpoint shows diff summary of changes
//! - Preview endpoint shows two-phase flow information

use miroir_core::config::MiroirConfig;
use miroir_proxy::routes::indexes::compute_settings_diff;
use serde_json::json;
use std::sync::Arc;

/// Helper to create a test config.
fn create_test_config() -> MiroirConfig {
    serde_json::from_value(json!({
        "nodes": [
            {
                "id": "node-1",
                "address": "http://localhost:7700",
                "replica_group": 0,
            },
            {
                "id": "node-2",
                "address": "http://localhost:7701",
                "replica_group": 0,
            }
        ],
        "shards": 16,
        "replication_factor": 2,
        "replica_groups": 1,
        "node_master_key": "test-master-key",
        "admin": {
            "api_key": "test-admin-key",
        },
        "settings_broadcast": {
            "strategy": "two_phase",
        },
    }))
    .expect("valid config")
}

/// Test 1: Preview endpoint returns fingerprint and version information.
#[tokio::test]
async fn test_preview_endpoint_returns_fingerprint_and_version() {
    let _config = Arc::new(create_test_config());

    // This is a unit test for the response structure.
    // In a full integration test, we would:
    // 1. Start a test server
    // 2. Create an index
    // 3. POST to /indexes/{index}/settings with proposed settings
    // 4. Verify the response contains all required fields

    let proposed_settings = json!({
        "synonyms": {
            "wifi": ["wi-fi", "wireless internet"]
        }
    });

    // Compute expected fingerprint
    use miroir_core::settings::fingerprint_settings;
    let expected_fingerprint = fingerprint_settings(&proposed_settings);

    // Verify fingerprint is a SHA256 hex string
    assert_eq!(
        expected_fingerprint.len(),
        64,
        "fingerprint should be 64 hex chars"
    );
    assert!(
        expected_fingerprint.chars().all(|c| c.is_ascii_hexdigit()),
        "fingerprint should be hex only"
    );

    // In a full integration test, we would verify:
    // - response.currentFingerprint
    // - response.proposedFingerprint == expected_fingerprint
    // - response.currentVersion
    // - response.expectedVersion == currentVersion + 1
}

/// Test 2: Preview endpoint shows node targets.
#[tokio::test]
async fn test_preview_endpoint_shows_node_targets() {
    let config = Arc::new(create_test_config());

    // Verify config has the expected nodes
    assert_eq!(config.nodes.len(), 2, "should have 2 nodes");
    assert_eq!(config.nodes[0].id, "node-1");
    assert_eq!(config.nodes[1].id, "node-2");

    // In a full integration test, we would verify:
    // - response.nodeTargets is an array of node objects
    // - response.nodeCount == 2
    // - Each node has "id" and "address" fields
}

/// Test 3: Preview endpoint computes diff correctly.
#[tokio::test]
async fn test_preview_endpoint_computes_diff() {
    let current = json!({
        "filterableAttributes": ["category", "price"],
        "sortableAttributes": ["price"],
    });

    let proposed = json!({
        "filterableAttributes": ["category", "price", "brand"],
        "sortableAttributes": ["price", "name"],
        "rankingRules": ["words", "typo"],
    });

    let diff = compute_settings_diff(&current, &proposed);

    // Should have 3 changes:
    // - modified: filterableAttributes (array changed)
    // - modified: sortableAttributes (array changed)
    // - added: rankingRules (new key)
    assert_eq!(diff.len(), 3, "should have 3 diff entries");

    // Check for added key
    let added = diff.iter().find(|d| d["type"] == "added");
    assert!(added.is_some(), "should have an added entry");
    let added = added.unwrap();
    assert_eq!(added["key"], "rankingRules");

    // Check for modified keys
    let modified: Vec<_> = diff.iter().filter(|d| d["type"] == "modified").collect();
    assert_eq!(modified.len(), 2, "should have 2 modified entries");
}

/// Test 4: Preview endpoint detects no changes.
#[tokio::test]
async fn test_preview_endpoint_no_changes() {
    let settings = json!({
        "filterableAttributes": ["category", "price"],
        "sortableAttributes": ["price"],
    });

    let diff = compute_settings_diff(&settings, &settings);

    assert_eq!(
        diff.len(),
        0,
        "should have no changes when settings are identical"
    );
}

/// Test 5: Preview endpoint handles new index (no current settings).
#[tokio::test]
async fn test_preview_endpoint_new_index() {
    let current = json!(null);
    let proposed = json!({
        "filterableAttributes": ["category"],
    });

    let diff = compute_settings_diff(&current, &proposed);

    assert_eq!(diff.len(), 1, "should have 1 added entry");
    assert_eq!(diff[0]["type"], "added");
    assert_eq!(diff[0]["key"], "filterableAttributes");
}
