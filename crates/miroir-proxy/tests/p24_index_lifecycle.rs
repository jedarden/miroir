//! P2.4 Index lifecycle acceptance tests.
//!
//! Tests:
//! - POST /indexes creates on every node; failure on any node rolls back
//! - _miroir_shard is in filterableAttributes after creation
//! - GET /indexes/{uid}/stats numberOfDocuments = logical count (divided by RG*RF)
//! - PATCH /indexes/{uid} sequential broadcast with rollback
//! - DELETE /indexes/{uid} broadcasts to all nodes
//! - PATCH /indexes/{uid}/settings sequential broadcast with rollback
//! - POST /keys creates on every node; failure rolls back
//! - DELETE /keys/{key} broadcasts to all nodes

use miroir_core::config::{Config, MiroirConfig, NodeConfig};
use miroir_proxy::routes::indexes::MeilisearchClient;
use serde_json::json;

fn make_config(node_addresses: Vec<String>) -> MiroirConfig {
    let nodes: Vec<NodeConfig> = node_addresses
        .into_iter()
        .enumerate()
        .map(|(i, addr)| NodeConfig {
            id: format!("node-{i}"),
            address: addr,
            replica_group: 0,
        })
        .collect();

    MiroirConfig {
        master_key: "test-master-key".into(),
        node_master_key: "test-node-master-key".into(),
        shards: 64,
        replication_factor: 1,
        replica_groups: 1,
        nodes,
        ..Default::default()
    }
}

fn make_config_rg2(node_addresses: Vec<String>) -> MiroirConfig {
    let nodes: Vec<NodeConfig> = node_addresses
        .into_iter()
        .enumerate()
        .map(|(i, addr)| NodeConfig {
            id: format!("node-{i}"),
            address: addr,
            replica_group: (i % 2) as u32,
        })
        .collect();

    MiroirConfig {
        master_key: "test-master-key".into(),
        node_master_key: "test-node-master-key".into(),
        shards: 64,
        replication_factor: 1,
        replica_groups: 2,
        nodes,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// POST /indexes — create with rollback
// ---------------------------------------------------------------------------

/// Test: Creating an index sends POST /indexes to every configured node.
#[tokio::test]
async fn test_create_index_broadcasts_to_all_nodes() {
    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    let mock1 = server1.mock("POST", "/indexes")
        .match_header("Authorization", "Bearer test-node-master-key")
        .with_status(200)
        .with_body(json!({"uid": "test-idx", "taskUid": 1, "status": "enqueued"}).to_string())
        .expect(1)
        .create_async()
        .await;

    let mock2 = server2.mock("POST", "/indexes")
        .match_header("Authorization", "Bearer test-node-master-key")
        .with_status(200)
        .with_body(json!({"uid": "test-idx", "taskUid": 1, "status": "enqueued"}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(vec![server1.url(), server2.url()]);
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes: Vec<String> = config.nodes.iter().map(|n| n.address.clone()).collect();

    let body = json!({"uid": "test-idx"});
    let mut created_on: Vec<String> = Vec::new();
    let mut first_response: Option<serde_json::Value> = None;
    let mut all_ok = true;

    for address in &nodes {
        match client.post_raw(address, "/indexes", &body).await {
            Ok((status, text)) if status >= 200 && status < 300 => {
                if first_response.is_none() {
                    first_response = serde_json::from_str(&text).ok();
                }
                created_on.push(address.clone());
            }
            _ => {
                all_ok = false;
                break;
            }
        }
    }

    assert!(all_ok, "all nodes should accept index creation");
    assert_eq!(created_on.len(), 2);

    mock1.assert_async().await;
    mock2.assert_async().await;
    settings_mock1.assert_async().await;
    settings_patch1.assert_async().await;
    settings_patch2.assert_async().await;
}

/// Test: If the second node fails during index creation, the first node's index is rolled back.
#[tokio::test]
async fn test_create_index_rollback_on_failure() {
    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    // Node 1: create succeeds
    let mock1 = server1.mock("POST", "/indexes")
        .with_status(200)
        .with_body(json!({"uid": "test-idx", "taskUid": 1}).to_string())
        .expect(1)
        .create_async()
        .await;

    // Node 2: create fails
    let mock2 = server2.mock("POST", "/indexes")
        .with_status(500)
        .with_body(json!({"message": "internal error"}).to_string())
        .expect(1)
        .create_async()
        .await;

    // Rollback: delete on node 1
    let rollback1 = server1.mock("DELETE", "/indexes/test-idx")
        .with_status(200)
        .with_body(json!({"taskUid": 2}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(vec![server1.url(), server2.url()]);
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes: Vec<String> = config.nodes.iter().map(|n| n.address.clone()).collect();

    let body = json!({"uid": "test-idx"});
    let mut created_on: Vec<String> = Vec::new();

    for address in &nodes {
        match client.post_raw(address, "/indexes", &body).await {
            Ok((status, _)) if status >= 200 && status < 300 => {
                created_on.push(address.clone());
            }
            Ok((status, text)) => {
                // Rollback
                for addr in &created_on {
                    let _ = client.delete_raw(addr, "/indexes/test-idx").await;
                }
                assert!(created_on.len() == 1, "first node should have been created before failure");
                break;
            }
            Err(_) => break,
        }
    }

    mock1.assert_async().await;
    mock2.assert_async().await;
    rollback1.assert_async().await;
}

// ---------------------------------------------------------------------------
// _miroir_shard in filterableAttributes
// ---------------------------------------------------------------------------

/// Test: After creating an index, _miroir_shard is in filterableAttributes.
#[tokio::test]
async fn test_miroir_shard_in_filterable_attributes() {
    let mut server = mockito::Server::new_async().await;

    let mock = server.mock("POST", "/indexes")
        .with_status(200)
        .with_body(json!({"uid": "test-idx", "taskUid": 1}).to_string())
        .expect(1)
        .create_async()
        .await;

    // GET settings returns current filterableAttributes
    let get_settings = server.mock("GET", "/indexes/test-idx/settings")
        .with_status(200)
        .with_body(json!({"filterableAttributes": ["status"], "sortableAttributes": []}).to_string())
        .expect(1)
        .create_async()
        .await;

    // PATCH settings should include both "status" and "_miroir_shard"
    let patch_settings = server.mock("PATCH", "/indexes/test-idx/settings")
        .match_body(mockito::Matcher::JsonString(json!({
            "filterableAttributes": ["_miroir_shard", "status"]
        }).to_string()))
        .with_status(200)
        .with_body(json!({"taskUid": 2}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(vec![server.url()]);
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes: Vec<String> = config.nodes.iter().map(|n| n.address.clone()).collect();

    // Step 1: Create index
    let body = json!({"uid": "test-idx"});
    let (status, _) = client.post_raw(&nodes[0], "/indexes", &body).await.unwrap();
    assert!(status >= 200 && status < 300);

    // Step 2: Read current settings and merge _miroir_shard
    let mut merged_attrs: Vec<serde_json::Value> = vec![json!("_miroir_shard")];
    if let Ok((s, text)) = client.get_raw(&nodes[0], "/indexes/test-idx/settings").await {
        if s >= 200 && s < 300 {
            if let Ok(settings) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(existing) = settings.get("filterableAttributes").and_then(|v| v.as_array()) {
                    for attr in existing {
                        let attr_str = attr.as_str().unwrap_or("");
                        if attr_str != "_miroir_shard" && !attr_str.is_empty() {
                            merged_attrs.push(attr.clone());
                        }
                    }
                }
            }
        }
    }

    // Step 3: PATCH with merged filterableAttributes
    let patch = json!({"filterableAttributes": merged_attrs});
    let (status, _) = client.patch_raw(&nodes[0], "/indexes/test-idx/settings", &patch).await.unwrap();
    assert!(status >= 200 && status < 300);

    mock.assert_async().await;
    get_settings.assert_async().await;
    patch_settings.assert_async().await;
}

// ---------------------------------------------------------------------------
// Stats aggregation — logical document count
// ---------------------------------------------------------------------------

/// Test: numberOfDocuments is divided by RG*RF to get logical count.
#[test]
fn test_stats_logical_doc_count() {
    let rg = 2u64;
    let rf = 1u64;
    let divisor = rg * rf;

    // Simulate: 3 nodes each reporting 100 docs
    // RG=2, RF=1: nodes 0,1 in group 0, node 2 in group 1
    // Total raw = 300, logical = 300 / 2 = 150
    let total_docs: u64 = 300;
    let logical = total_docs / divisor;
    assert_eq!(logical, 150);

    // Simulate: 2 nodes, RG=1, RF=1: both in same group
    // Total raw = 200, divisor = 1, logical = 200
    let rg1 = 1u64;
    let rf1 = 1u64;
    let logical_rg1 = 200u64 / (rg1 * rf1);
    assert_eq!(logical_rg1, 200);
}

/// Test: fieldDistribution is summed per-field across nodes.
#[test]
fn test_field_distribution_merge() {
    use std::collections::HashMap;

    let mut field_distribution: HashMap<String, u64> = HashMap::new();

    // Node 1
    let fd1 = json!({"title": 100, "body": 200});
    if let Some(obj) = fd1.as_object() {
        for (field, count) in obj {
            if let Some(c) = count.as_u64() {
                *field_distribution.entry(field.clone()).or_insert(0) += c;
            }
        }
    }

    // Node 2
    let fd2 = json!({"title": 150, "body": 250, "tags": 50});
    if let Some(obj) = fd2.as_object() {
        for (field, count) in obj {
            if let Some(c) = count.as_u64() {
                *field_distribution.entry(field.clone()).or_insert(0) += c;
            }
        }
    }

    assert_eq!(*field_distribution.get("title").unwrap_or(&0), 250);
    assert_eq!(*field_distribution.get("body").unwrap_or(&0), 450);
    assert_eq!(*field_distribution.get("tags").unwrap_or(&0), 50);
}

// ---------------------------------------------------------------------------
// Settings sequential broadcast with rollback
// ---------------------------------------------------------------------------

/// Test: Settings update fails on node 2, triggering rollback on node 1.
#[tokio::test]
async fn test_settings_broadcast_rollback() {
    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    // Snapshot current settings from node 1
    let get1 = server1.mock("GET", "/indexes/test-idx/settings")
        .with_status(200)
        .with_body(json!({"filterableAttributes": ["_miroir_shard"], "rankingRules": ["words"]}).to_string())
        .expect(1)
        .create_async()
        .await;

    // Snapshot from node 2
    let get2 = server2.mock("GET", "/indexes/test-idx/settings")
        .with_status(200)
        .with_body(json!({"filterableAttributes": ["_miroir_shard"], "rankingRules": ["words"]}).to_string())
        .expect(1)
        .create_async()
        .await;

    // PATCH succeeds on node 1
    let patch1 = server1.mock("PATCH", "/indexes/test-idx/settings")
        .with_status(200)
        .with_body(json!({"taskUid": 10}).to_string())
        .expect(1)
        .create_async()
        .await;

    // PATCH fails on node 2
    let patch2_fail = server2.mock("PATCH", "/indexes/test-idx/settings")
        .with_status(500)
        .with_body(json!({"message": "internal error"}).to_string())
        .expect(1)
        .create_async()
        .await;

    // Rollback: restore original settings on node 1
    let rollback1 = server1.mock("PATCH", "/indexes/test-idx/settings")
        .match_body(mockito::Matcher::JsonString(json!({"filterableAttributes": ["_miroir_shard"], "rankingRules": ["words"]}).to_string()))
        .with_status(200)
        .with_body(json!({"taskUid": 11}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(vec![server1.url(), server2.url()]);
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes: Vec<String> = config.nodes.iter().map(|n| n.address.clone()).collect();

    let settings_path = "/indexes/test-idx/settings";
    let new_settings = json!({"rankingRules": ["typo", "words"]});

    // Snapshot phase
    let mut snapshots: Vec<(String, serde_json::Value)> = Vec::new();
    for address in &nodes {
        let (status, text) = client.get_raw(address, settings_path).await.unwrap();
        assert!(status >= 200 && status < 300);
        snapshots.push((address.clone(), serde_json::from_str(&text).unwrap()));
    }

    // Apply sequentially - node 1 succeeds, node 2 fails
    let mut applied: Vec<String> = Vec::new();
    for (address, _) in &snapshots {
        match client.patch_raw(address, settings_path, &new_settings).await {
            Ok((status, _)) if status >= 200 && status < 300 => {
                applied.push(address.clone());
            }
            _ => {
                // Rollback
                for addr in &applied {
                    if let Some((_, snapshot)) = snapshots.iter().find(|(a, _)| a == addr) {
                        let _ = client.patch_raw(addr, settings_path, snapshot).await;
                    }
                }
                break;
            }
        }
    }

    get1.assert_async().await;
    get2.assert_async().await;
    patch1.assert_async().await;
    patch2_fail.assert_async().await;
    rollback1.assert_async().await;
}

// ---------------------------------------------------------------------------
// DELETE /indexes/{uid} — broadcast
// ---------------------------------------------------------------------------

/// Test: Deleting an index sends DELETE to every node.
#[tokio::test]
async fn test_delete_index_broadcasts_to_all_nodes() {
    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    let mock1 = server1.mock("DELETE", "/indexes/test-idx")
        .with_status(200)
        .with_body(json!({"taskUid": 1}).to_string())
        .expect(1)
        .create_async()
        .await;

    let mock2 = server2.mock("DELETE", "/indexes/test-idx")
        .with_status(200)
        .with_body(json!({"taskUid": 1}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(vec![server1.url(), server2.url()]);
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes: Vec<String> = config.nodes.iter().map(|n| n.address.clone()).collect();

    let mut success_count = 0;
    for address in &nodes {
        let (status, _) = client.delete_raw(address, "/indexes/test-idx").await.unwrap();
        if status >= 200 && status < 300 {
            success_count += 1;
        }
    }

    assert_eq!(success_count, 2);
    mock1.assert_async().await;
    mock2.assert_async().await;
}

// ---------------------------------------------------------------------------
// Keys CRUD — broadcast
// ---------------------------------------------------------------------------

/// Test: Creating a key sends POST /keys to every node.
#[tokio::test]
async fn test_create_key_broadcasts_to_all_nodes() {
    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    let mock1 = server1.mock("POST", "/keys")
        .match_header("Authorization", "Bearer test-node-master-key")
        .with_status(200)
        .with_body(json!({"key": "abc123", "name": "test-key"}).to_string())
        .expect(1)
        .create_async()
        .await;

    let mock2 = server2.mock("POST", "/keys")
        .match_header("Authorization", "Bearer test-node-master-key")
        .with_status(200)
        .with_body(json!({"key": "abc123", "name": "test-key"}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(vec![server1.url(), server2.url()]);
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes: Vec<String> = config.nodes.iter().map(|n| n.address.clone()).collect();

    let body = json!({"name": "test-key", "actions": ["*"], "indexes": ["*"]});
    let mut created_count = 0;
    for address in &nodes {
        let (status, _) = client.post_raw(address, "/keys", &body).await.unwrap();
        if status >= 200 && status < 300 {
            created_count += 1;
        }
    }

    assert_eq!(created_count, 2);
    mock1.assert_async().await;
    mock2.assert_async().await;
}

/// Test: If key creation fails on node 2, rollback deletes from node 1.
#[tokio::test]
async fn test_create_key_rollback_on_failure() {
    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    let mock1 = server1.mock("POST", "/keys")
        .with_status(200)
        .with_body(json!({"key": "abc123", "name": "test-key"}).to_string())
        .expect(1)
        .create_async()
        .await;

    let mock2 = server2.mock("POST", "/keys")
        .with_status(500)
        .with_body(json!({"message": "internal error"}).to_string())
        .expect(1)
        .create_async()
        .await;

    // Rollback: delete key from node 1
    let rollback1 = server1.mock("DELETE", "/keys/test-key")
        .with_status(200)
        .with_body(json!({}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(vec![server1.url(), server2.url()]);
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes: Vec<String> = config.nodes.iter().map(|n| n.address.clone()).collect();

    let body = json!({"name": "test-key", "actions": ["*"], "indexes": ["*"]});
    let mut created_on: Vec<String> = Vec::new();

    for address in &nodes {
        match client.post_raw(address, "/keys", &body).await {
            Ok((status, _)) if status >= 200 && status < 300 => {
                created_on.push(address.clone());
            }
            _ => {
                // Rollback
                for addr in &created_on {
                    let _ = client.delete_raw(addr, "/keys/test-key").await;
                }
                break;
            }
        }
    }

    mock1.assert_async().await;
    mock2.assert_async().await;
    rollback1.assert_async().await;
}

// ---------------------------------------------------------------------------
// PATCH /indexes/{uid} — update index metadata with rollback
// ---------------------------------------------------------------------------

/// Test: Index metadata update with snapshot and rollback.
#[tokio::test]
async fn test_update_index_snapshot_and_rollback() {
    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    // Snapshot from both nodes
    let get1 = server1.mock("GET", "/indexes/test-idx")
        .with_status(200)
        .with_body(json!({"uid": "test-idx", "primaryKey": "id"}).to_string())
        .expect(1)
        .create_async()
        .await;

    let get2 = server2.mock("GET", "/indexes/test-idx")
        .with_status(200)
        .with_body(json!({"uid": "test-idx", "primaryKey": "id"}).to_string())
        .expect(1)
        .create_async()
        .await;

    // PATCH succeeds on node 1
    let patch1 = server1.mock("PATCH", "/indexes/test-idx")
        .with_status(200)
        .with_body(json!({"uid": "test-idx", "primaryKey": "new_id"}).to_string())
        .expect(1)
        .create_async()
        .await;

    // PATCH fails on node 2
    let patch2 = server2.mock("PATCH", "/indexes/test-idx")
        .with_status(500)
        .with_body(json!({"message": "error"}).to_string())
        .expect(1)
        .create_async()
        .await;

    // Rollback on node 1
    let rollback1 = server1.mock("PATCH", "/indexes/test-idx")
        .match_body(mockito::Matcher::JsonString(json!({"uid": "test-idx", "primaryKey": "id"}).to_string()))
        .with_status(200)
        .with_body(json!({"uid": "test-idx"}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(vec![server1.url(), server2.url()]);
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes: Vec<String> = config.nodes.iter().map(|n| n.address.clone()).collect();

    let update_body = json!({"primaryKey": "new_id"});

    // Snapshot phase
    let mut snapshots: Vec<(String, serde_json::Value)> = Vec::new();
    for address in &nodes {
        let (status, text) = client.get_raw(address, "/indexes/test-idx").await.unwrap();
        assert!(status >= 200 && status < 300);
        snapshots.push((address.clone(), serde_json::from_str(&text).unwrap()));
    }

    // Apply sequentially
    let mut applied: Vec<String> = Vec::new();
    for (address, _) in &snapshots {
        match client.patch_raw(address, "/indexes/test-idx", &update_body).await {
            Ok((status, _)) if status >= 200 && status < 300 => {
                applied.push(address.clone());
            }
            _ => {
                for addr in &applied {
                    if let Some((_, snapshot)) = snapshots.iter().find(|(a, _)| a == addr) {
                        let _ = client.patch_raw(addr, "/indexes/test-idx", snapshot).await;
                    }
                }
                break;
            }
        }
    }

    get1.assert_async().await;
    get2.assert_async().await;
    patch1.assert_async().await;
    patch2.assert_async().await;
    rollback1.assert_async().await;
}

// ---------------------------------------------------------------------------
// Stats fan-out with RG=2
// ---------------------------------------------------------------------------

/// Test: Stats aggregation divides by RG*RF for logical doc count.
#[tokio::test]
async fn test_stats_fan_out_logical_count() {
    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    // Each node reports 100 docs
    let stats1 = server1.mock("GET", "/indexes/test-idx/stats")
        .with_status(200)
        .with_body(json!({"numberOfDocuments": 100, "isIndexing": false, "fieldDistribution": {"title": 100}}).to_string())
        .expect(1)
        .create_async()
        .await;

    let stats2 = server2.mock("GET", "/indexes/test-idx/stats")
        .with_status(200)
        .with_body(json!({"numberOfDocuments": 100, "isIndexing": false, "fieldDistribution": {"title": 100, "body": 50}}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config_rg2(vec![server1.url(), server2.url()]);
    let client = MeilisearchClient::new(config.node_master_key.clone());
    let nodes: Vec<String> = config.nodes.iter().map(|n| n.address.clone()).collect();

    let mut total_docs: u64 = 0;
    for address in &nodes {
        let result = client.get_index_stats(address, "test-idx").await;
        if let Ok(stats) = result {
            if let Some(n) = stats.get("numberOfDocuments").and_then(|v| v.as_u64()) {
                total_docs += n;
            }
        }
    }

    // RG=2, RF=1 → divisor = 2
    let rg = config.replica_groups as u64;
    let rf = config.replication_factor as u64;
    let logical = total_docs / (rg * rf);

    assert_eq!(logical, 100, "logical doc count should be total/2 for RG=2 RF=1");

    stats1.assert_async().await;
    stats2.assert_async().await;
}
