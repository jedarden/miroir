//! Integration tests for Miroir with docker-compose.
//!
//! These tests run against a real Miroir + Meilisearch stack started via docker-compose.
//! Tests cover:
//! - Document round-trip (1000 docs)
//! - Search coverage across all shards (unique-keyword test)
//! - Facet aggregation
//! - Offset/limit paging
//! - Settings broadcast
//! - Task polling
//! - Node failure with RF=2
//!
//! Run with:
//!   cargo test --test integration -- --test-threads=1
//!
//! Prerequisites:
//!   docker compose -f examples/docker-compose-dev.yml up -d

use serde_json::json;
use std::collections::HashSet;
use std::time::Duration;

/// Base URL for Miroir API (from docker-compose)
const MIROIR_BASE_URL: &str = "http://localhost:7700";

/// Master key for authentication (from dev-config.yaml)
const MASTER_KEY: &str = "dev-key";

/// HTTP client for making requests
#[derive(Clone)]
struct HttpClient {
    client: reqwest::Client,
    base_url: String,
    master_key: String,
}

impl HttpClient {
    fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            base_url: MIROIR_BASE_URL.to_string(),
            master_key: MASTER_KEY.to_string(),
        }
    }

    async fn get(&self, path: &str) -> Result<(u16, String), String> {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .send()
            .await
            .map_err(|e| format!("GET {url} failed: {e}"))?;

        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| format!("Failed to read body: {e}"))?;
        Ok((status, body))
    }

    async fn post(&self, path: &str, body: &serde_json::Value) -> Result<(u16, String), String> {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .map_err(|e| format!("POST {url} failed: {e}"))?;

        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| format!("Failed to read body: {e}"))?;
        Ok((status, body))
    }

    async fn patch(&self, path: &str, body: &serde_json::Value) -> Result<(u16, String), String> {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .patch(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .map_err(|e| format!("PATCH {url} failed: {e}"))?;

        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| format!("Failed to read body: {e}"))?;
        Ok((status, body))
    }

    async fn delete(&self, path: &str) -> Result<(u16, String), String> {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .delete(&url)
            .header("Authorization", format!("Bearer {}", self.master_key))
            .send()
            .await
            .map_err(|e| format!("DELETE {url} failed: {e}"))?;

        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| format!("Failed to read body: {e}"))?;
        Ok((status, body))
    }
}

/// Wait for a task to complete by polling the task endpoint
async fn wait_for_task(
    client: &HttpClient,
    _index_uid: &str,
    task_uid: u64,
    timeout_secs: u64,
) -> Result<serde_json::Value, String> {
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout {
        let (status, body) = client.get(&format!("/tasks/{task_uid}")).await?;
        if status == 200 {
            if let Ok(task) = serde_json::from_str::<serde_json::Value>(&body) {
                if let Some(status_str) = task.get("status").and_then(|v| v.as_str()) {
                    if status_str == "succeeded" {
                        return Ok(task);
                    } else if status_str == "failed" {
                        return Err(format!("Task failed: {body}"));
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(format!(
        "Task {task_uid} timed out after {timeout_secs} seconds"
    ))
}

/// Generate test documents with unique keywords for shard coverage testing
fn generate_test_documents(count: usize) -> Vec<serde_json::Value> {
    let colors = ["red", "green", "blue"];
    (0..count)
        .map(|i| {
            json!({
                "id": i,
                "title": format!("Document {}", i),
                "keyword": format!("keyword_{}", i % 16), // 16 unique keywords for 16 shards
                "color": colors[i % 3], // For facet testing
                "score": i % 100,
                "shard_hint": format!("shard_{}", i % 16), // Help verify shard distribution
            })
        })
        .collect()
}

/// Test 1: Document round-trip (1000 docs)
#[tokio::test]
async fn test_document_round_trip() {
    let client = HttpClient::new();

    // Create index
    let create_body = json!({
        "uid": "round_trip_test",
        "primaryKey": "id"
    });

    let (status, body) = client.post("/indexes", &create_body).await.unwrap();
    assert!(
        status == 202 || status == 200,
        "Index creation failed: {body}"
    );

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "round_trip_test", task_uid, 30)
        .await
        .unwrap();

    // Index 1000 documents
    let docs = generate_test_documents(1000);
    let (status, body) = client
        .post("/indexes/round_trip_test/documents", &json!(docs))
        .await
        .unwrap();
    assert_eq!(status, 202, "Document indexing failed: {body}");

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "round_trip_test", task_uid, 60)
        .await
        .unwrap();

    // Verify all 1000 documents are retrievable by ID
    for i in 0..1000 {
        let (status, body) = client
            .get(&format!("/indexes/round_trip_test/documents/{i}"))
            .await
            .unwrap();
        assert_eq!(status, 200, "Failed to fetch document {i}: {body}");

        let doc: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            doc.get("id").and_then(|v| v.as_i64()),
            Some(i64::from(i))
        );
        assert_eq!(
            doc.get("title").and_then(|v| v.as_str()),
            Some(format!("Document {i}").as_str())
        );
    }

    // Clean up
    let _ = client.delete("/indexes/round_trip_test").await;
}

/// Test 2: Search covers all shards (unique-keyword test)
#[tokio::test]
async fn test_search_shard_coverage() {
    let client = HttpClient::new();

    // Create index
    let create_body = json!({
        "uid": "shard_coverage_test",
        "primaryKey": "id"
    });

    let (status, body) = client.post("/indexes", &create_body).await.unwrap();
    assert!(
        status == 202 || status == 200,
        "Index creation failed: {body}"
    );

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "shard_coverage_test", task_uid, 30)
        .await
        .unwrap();

    // Configure filterable attributes to enable shard filtering
    let settings_body = json!({
        "filterableAttributes": ["keyword", "shard_hint", "color"]
    });

    let (status, body) = client
        .patch("/indexes/shard_coverage_test/settings", &settings_body)
        .await
        .unwrap();
    assert!(
        status == 202 || status == 200,
        "Settings update failed: {body}"
    );

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "shard_coverage_test", task_uid, 30)
        .await
        .unwrap();

    // Index documents
    let docs = generate_test_documents(1000);
    let (status, body) = client
        .post("/indexes/shard_coverage_test/documents", &json!(docs))
        .await
        .unwrap();
    assert_eq!(status, 202, "Document indexing failed: {body}");

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "shard_coverage_test", task_uid, 60)
        .await
        .unwrap();

    // Search for each unique keyword and verify we find all documents with that keyword
    let mut found_keywords: HashSet<String> = HashSet::new();

    for shard_id in 0..16 {
        let keyword = format!("keyword_{shard_id}");
        let search_body = json!({
            "q": keyword,
            "filter": format!("keyword = {}", keyword)
        });

        let (status, body) = client
            .post("/indexes/shard_coverage_test/search", &search_body)
            .await
            .unwrap();
        assert_eq!(
            status, 200,
            "Search failed for keyword {keyword}: {body}"
        );

        let response: serde_json::Value = serde_json::from_str(&body).unwrap();
        let hits = response.get("hits").and_then(|v| v.as_array()).unwrap();

        // Each keyword should appear in ~62-63 documents (1000 / 16)
        assert!(
            hits.len() >= 60 && hits.len() <= 65,
            "Expected ~62 documents for keyword {}, got {}",
            keyword,
            hits.len()
        );

        found_keywords.insert(keyword);
    }

    // Verify we found all 16 keywords
    assert_eq!(
        found_keywords.len(),
        16,
        "Expected to find all 16 keywords, found {}",
        found_keywords.len()
    );

    // Clean up
    let _ = client.delete("/indexes/shard_coverage_test").await;
}

/// Test 3: Facet aggregation (3 colors, sum = 100)
#[tokio::test]
async fn test_facet_aggregation() {
    let client = HttpClient::new();

    // Create index
    let create_body = json!({
        "uid": "facet_test",
        "primaryKey": "id"
    });

    let (status, body) = client.post("/indexes", &create_body).await.unwrap();
    assert!(
        status == 202 || status == 200,
        "Index creation failed: {body}"
    );

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "facet_test", task_uid, 30)
        .await
        .unwrap();

    // Configure filterable attributes
    let settings_body = json!({
        "filterableAttributes": ["color"]
    });

    let (status, body) = client
        .patch("/indexes/facet_test/settings", &settings_body)
        .await
        .unwrap();
    assert!(
        status == 202 || status == 200,
        "Settings update failed: {body}"
    );

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "facet_test", task_uid, 30)
        .await
        .unwrap();

    // Index exactly 100 documents with known color distribution
    let colors = ["red", "green", "blue"];
    let docs: Vec<serde_json::Value> = (0..100)
        .map(|i| {
            json!({
                "id": i,
                "color": colors[i % 3],
            })
        })
        .collect();

    let (status, body) = client
        .post("/indexes/facet_test/documents", &json!(docs))
        .await
        .unwrap();
    assert_eq!(status, 202, "Document indexing failed: {body}");

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "facet_test", task_uid, 30)
        .await
        .unwrap();

    // Search with facet distribution
    let search_body = json!({
        "q": "",
        "facets": ["color"]
    });

    let (status, body) = client
        .post("/indexes/facet_test/search", &search_body)
        .await
        .unwrap();
    assert_eq!(status, 200, "Facet search failed: {body}");

    let response: serde_json::Value = serde_json::from_str(&body).unwrap();
    let facet_distribution = response
        .get("facetDistribution")
        .and_then(|v| v.as_object())
        .and_then(|o| o.get("color"))
        .and_then(|v| v.as_object())
        .expect("No facet distribution found");

    let red_count = facet_distribution
        .get("red")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let green_count = facet_distribution
        .get("green")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let blue_count = facet_distribution
        .get("blue")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let total = red_count + green_count + blue_count;
    assert_eq!(
        total, 100,
        "Expected total of 100 documents across colors, got {total}"
    );

    // Each color should have approximately equal distribution (33-34 each)
    assert!(
        (32..=35).contains(&red_count),
        "Expected ~33 red documents, got {red_count}"
    );
    assert!(
        (32..=35).contains(&green_count),
        "Expected ~33 green documents, got {green_count}"
    );
    assert!(
        (32..=35).contains(&blue_count),
        "Expected ~33 blue documents, got {blue_count}"
    );

    // Clean up
    let _ = client.delete("/indexes/facet_test").await;
}

/// Test 4: Offset/limit paging
#[tokio::test]
async fn test_offset_limit_paging() {
    let client = HttpClient::new();

    // Create index
    let create_body = json!({
        "uid": "paging_test",
        "primaryKey": "id"
    });

    let (status, body) = client.post("/indexes", &create_body).await.unwrap();
    assert!(
        status == 202 || status == 200,
        "Index creation failed: {body}"
    );

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "paging_test", task_uid, 30)
        .await
        .unwrap();

    // Index documents
    let docs: Vec<serde_json::Value> = (0..100)
        .map(|i| {
            json!({
                "id": i,
                "title": format!("Document {}", i),
            })
        })
        .collect();

    let (status, body) = client
        .post("/indexes/paging_test/documents", &json!(docs))
        .await
        .unwrap();
    assert_eq!(status, 202, "Document indexing failed: {body}");

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "paging_test", task_uid, 30)
        .await
        .unwrap();

    // Test paging with offset and limit
    let page_size = 20;
    let mut all_ids: Vec<i64> = Vec::new();

    for page in 0..5 {
        let offset = page * page_size;
        let search_body = json!({
            "q": "",
            "limit": page_size,
            "offset": offset,
            "sort": ["id:asc"]
        });

        let (status, body) = client
            .post("/indexes/paging_test/search", &search_body)
            .await
            .unwrap();
        assert_eq!(status, 200, "Search failed for page {page}: {body}");

        let response: serde_json::Value = serde_json::from_str(&body).unwrap();
        let hits = response.get("hits").and_then(|v| v.as_array()).unwrap();

        for hit in hits {
            if let Some(id) = hit.get("id").and_then(|v| v.as_i64()) {
                all_ids.push(id);
            }
        }

        // Last page might have fewer results
        let expected_count = if page < 4 { page_size } else { 0 };
        if page < 4 {
            assert_eq!(
                hits.len(),
                expected_count,
                "Expected {} results on page {}, got {}",
                expected_count,
                page,
                hits.len()
            );
        }
    }

    // Verify we got all 100 IDs and they're unique and sequential
    assert_eq!(
        all_ids.len(),
        100,
        "Expected 100 total documents, got {}",
        all_ids.len()
    );
    assert_eq!(
        all_ids,
        (0..100).collect::<Vec<_>>(),
        "Documents are not in correct order"
    );

    // Clean up
    let _ = client.delete("/indexes/paging_test").await;
}

/// Test 5: Settings broadcast
#[tokio::test]
async fn test_settings_broadcast() {
    let client = HttpClient::new();

    // Create index
    let create_body = json!({
        "uid": "settings_test",
        "primaryKey": "id"
    });

    let (status, body) = client.post("/indexes", &create_body).await.unwrap();
    assert!(
        status == 202 || status == 200,
        "Index creation failed: {body}"
    );

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "settings_test", task_uid, 30)
        .await
        .unwrap();

    // Update settings
    let settings_body = json!({
        "searchableAttributes": ["title", "description"],
        "filterableAttributes": ["color", "size"],
        "sortableAttributes": ["id", "score"],
        "rankingRules": ["words", "typo", "proximity", "attribute", "sort", "exactness"]
    });

    let (status, body) = client
        .patch("/indexes/settings_test/settings", &settings_body)
        .await
        .unwrap();
    assert!(
        status == 202 || status == 200,
        "Settings update failed: {body}"
    );

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "settings_test", task_uid, 30)
        .await
        .unwrap();

    // Verify settings were applied
    let (status, body) = client.get("/indexes/settings_test/settings").await.unwrap();
    assert_eq!(status, 200, "Failed to get settings: {body}");

    let settings: serde_json::Value = serde_json::from_str(&body).unwrap();

    let searchable = settings
        .get("searchableAttributes")
        .and_then(|v| v.as_array())
        .unwrap();
    assert_eq!(searchable.len(), 2, "Expected 2 searchable attributes");
    assert!(searchable.iter().any(|v| v.as_str() == Some("title")));
    assert!(searchable.iter().any(|v| v.as_str() == Some("description")));

    let filterable = settings
        .get("filterableAttributes")
        .and_then(|v| v.as_array())
        .unwrap();
    assert_eq!(filterable.len(), 2, "Expected 2 filterable attributes");
    assert!(filterable.iter().any(|v| v.as_str() == Some("color")));
    assert!(filterable.iter().any(|v| v.as_str() == Some("size")));

    // Clean up
    let _ = client.delete("/indexes/settings_test").await;
}

/// Test 6: Task polling
#[tokio::test]
async fn test_task_polling() {
    let client = HttpClient::new();

    // Create index
    let create_body = json!({
        "uid": "task_polling_test",
        "primaryKey": "id"
    });

    let (status, body) = client.post("/indexes", &create_body).await.unwrap();
    assert!(
        status == 202 || status == 200,
        "Index creation failed: {body}"
    );

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    // Poll task until completion
    let task = wait_for_task(&client, "task_polling_test", task_uid, 30)
        .await
        .unwrap();

    // Verify task structure
    assert_eq!(task.get("uid").and_then(|v| v.as_u64()), Some(task_uid));
    assert_eq!(
        task.get("status").and_then(|v| v.as_str()),
        Some("succeeded")
    );
    assert_eq!(
        task.get("type").and_then(|v| v.as_str()),
        Some("indexCreation")
    );

    // Index documents and poll
    let docs: Vec<serde_json::Value> = (0..10)
        .map(|i| {
            json!({
                "id": i,
                "title": format!("Document {}", i),
            })
        })
        .collect();

    let (status, body) = client
        .post("/indexes/task_polling_test/documents", &json!(docs))
        .await
        .unwrap();
    assert_eq!(status, 202, "Document indexing failed: {body}");

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    let task = wait_for_task(&client, "task_polling_test", task_uid, 30)
        .await
        .unwrap();
    assert_eq!(
        task.get("status").and_then(|v| v.as_str()),
        Some("succeeded")
    );
    assert_eq!(
        task.get("type").and_then(|v| v.as_str()),
        Some("documentAdditionOrUpdate")
    );

    // Clean up
    let _ = client.delete("/indexes/task_polling_test").await;
}

/// Test 7: Health check
#[tokio::test]
async fn test_health_check() {
    let client = HttpClient::new();

    let (status, body) = client.get("/health").await.unwrap();
    assert_eq!(status, 200, "Health check failed: {body}");

    let health: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        health.get("status").and_then(|v| v.as_str()),
        Some("available")
    );
}

/// Test 8: Direct Meilisearch node access (for debugging)
#[tokio::test]
async fn test_direct_meilisearch_access() {
    // Access Meilisearch node 0 directly
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let response = client
        .get("http://localhost:7701/health")
        .header("Authorization", "Bearer dev-node-key")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status().as_u16(), 200);
}

/// Test 9: Node failure with RF=2
///
/// This test requires the RF=2 docker-compose stack:
///   docker compose -f examples/docker-compose-dev-rf2.yml up -d
///
/// The test:
/// 1. Indexes documents with RF=2 (each document replicated to both groups)
/// 2. Stops a Meilisearch node mid-test (docker stop)
/// 3. Verifies that searches still work using remaining replicas
/// 4. Restarts the node and verifies recovery
#[tokio::test]
#[ignore] // Run with: cargo test --test integration test_node_failure_rf2 -- --ignored
async fn test_node_failure_rf2() {
    // Use port 7710 for RF=2 stack
    let client = HttpClient {
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to create HTTP client"),
        base_url: "http://localhost:7710".to_string(),
        master_key: MASTER_KEY.to_string(),
    };

    // Create index
    let create_body = json!({
        "uid": "node_failure_test",
        "primaryKey": "id"
    });

    let (status, body) = client.post("/indexes", &create_body).await.unwrap();
    assert!(
        status == 202 || status == 200,
        "Index creation failed: {body}"
    );

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "node_failure_test", task_uid, 30)
        .await
        .unwrap();

    // Index 100 documents
    let docs: Vec<serde_json::Value> = (0..100)
        .map(|i| {
            json!({
                "id": i,
                "title": format!("Document {}", i),
                "group": i % 2, // Track which group this document belongs to
            })
        })
        .collect();

    let (status, body) = client
        .post("/indexes/node_failure_test/documents", &json!(docs))
        .await
        .unwrap();
    assert_eq!(status, 202, "Document indexing failed: {body}");

    let task_uid: u64 = serde_json::from_str::<serde_json::Value>(&body)
        .unwrap()
        .get("taskUid")
        .and_then(|v| v.as_u64())
        .unwrap();

    wait_for_task(&client, "node_failure_test", task_uid, 60)
        .await
        .unwrap();

    // Verify all documents are searchable
    let search_body = json!({
        "q": "",
        "limit": 100
    });

    let (status, body) = client
        .post("/indexes/node_failure_test/search", &search_body)
        .await
        .unwrap();
    assert_eq!(status, 200, "Search failed: {body}");

    let response: serde_json::Value = serde_json::from_str(&body).unwrap();
    let hits = response.get("hits").and_then(|v| v.as_array()).unwrap();
    assert_eq!(
        hits.len(),
        100,
        "Expected 100 documents before node failure"
    );

    // Stop meili-1 (a node in replica group 0)
    // This simulates a node failure - RF=2 means we still have 1 replica in group 0
    let output = std::process::Command::new("docker")
        .args(["stop", "miroir-meili-1"])
        .output()
        .expect("Failed to stop container");

    assert!(
        output.status.success(),
        "Failed to stop meili-1: {:?}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Wait for Miroir to detect the failure
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Search should still work with degraded availability
    // Some documents may be missing if they were only on the failed node
    let (status, body) = client
        .post("/indexes/node_failure_test/search", &search_body)
        .await
        .unwrap();
    assert_eq!(status, 200, "Search failed after node failure: {body}");

    let response: serde_json::Value = serde_json::from_str(&body).unwrap();
    let hits_after = response.get("hits").and_then(|v| v.as_array()).unwrap();

    // With RF=2 and 1 node down, we should still get results
    // (each document has 2 replicas, 1 in each group)
    // Since we stopped a node in group 0, documents with replicas on that node
    // should still be accessible via the other replica in group 1
    assert!(
        !hits_after.is_empty(),
        "Expected some results after node failure"
    );

    // Restart the node
    let output = std::process::Command::new("docker")
        .args(["start", "miroir-meili-1"])
        .output()
        .expect("Failed to start container");

    assert!(
        output.status.success(),
        "Failed to start meili-1: {:?}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Wait for the node to recover and Miroir to detect it
    tokio::time::sleep(Duration::from_secs(10)).await;

    // After recovery, all documents should be accessible again
    let (status, body) = client
        .post("/indexes/node_failure_test/search", &search_body)
        .await
        .unwrap();
    assert_eq!(status, 200, "Search failed after node recovery: {body}");

    let response: serde_json::Value = serde_json::from_str(&body).unwrap();
    let hits_recovered = response.get("hits").and_then(|v| v.as_array()).unwrap();
    assert_eq!(
        hits_recovered.len(),
        100,
        "Expected all 100 documents after node recovery"
    );

    // Clean up
    let _ = client.delete("/indexes/node_failure_test").await;
}
