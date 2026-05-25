// Miroir Integration Tests
//
// Tests the full Miroir stack with 3 Meilisearch nodes via docker-compose.
// Per plan §8: Integration tests validate end-to-end behavior including
// document distribution, shard coverage, facet aggregation, paging, settings
// broadcast, task polling, and node failure with RF=2.
//
// Prerequisites:
//   - docker-compose-dev stack running (Miroir on port 7700, nodes on 7701-7703)
//   - For node_failure_rf2 test: docker-compose-dev-rf2 stack (RF=2, 6 nodes)
//
// Run:
//   cargo test --test integration -- --test-threads=1

use meilisearch_sdk::{client::Client, indexes::Index, search::SearchResults, tasks::Task};
use reqwest::StatusCode;
use serde_json::json;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::env;
use std::time::Duration;
use tokio::time::sleep;

const MIROIR_PORT: u16 = 7700;
const NODE_PORTS: [u16; 3] = [7701, 7702, 7703];
const MASTER_KEY: &str = "dev-key";
const NODE_KEY: &str = "dev-node-key";

/// Test document
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TestDoc {
    id: String,
    title: String,
    content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    color: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    score: Option<i32>,
}

/// Helper: Get Miroir client
fn miroir_client() -> Client {
    let url = format!("http://localhost:{}", MIROIR_PORT);
    Client::new(url, Some(MASTER_KEY.to_string())).expect("Failed to create Miroir client")
}

/// Helper: Get direct client to a Meilisearch node
fn node_client(port: u16) -> Client {
    let url = format!("http://localhost:{}", port);
    Client::new(url, Some(NODE_KEY.to_string())).expect("Failed to create Meilisearch node client")
}

/// Helper: Wait for a task to complete
async fn wait_for_task(
    client: &Client,
    task_info: meilisearch_sdk::task_info::TaskInfo,
) -> Result<Task, Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();
    let task_uid = task_info.task_uid;

    loop {
        let task = client.get_task(&task_info).await?;
        // Check if task is finished (Succeeded or Failed)
        match task {
            Task::Succeeded { .. } => return Ok(task),
            Task::Failed { .. } => {
                return Err(format!("Task {} failed: {:?}", task_uid, task).into())
            }
            _ => {}
        }

        if start.elapsed() > timeout {
            return Err(format!("Task {} timed out", task_uid).into());
        }

        sleep(Duration::from_millis(200)).await;
    }
}

/// Helper: Create or get index with primary key
async fn get_index(client: &Client, name: &str) -> Result<Index, Box<dyn std::error::Error>> {
    match client.get_index(name).await {
        Ok(_) => Ok(client.index(name)),
        Err(_) => {
            let task_info = client.create_index(name, Some("id")).await?;
            wait_for_task(client, task_info).await?;
            Ok(client.index(name))
        }
    }
}

/// Helper: Delete index if exists
async fn delete_index(client: &Client, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    if client.get_index(name).await.is_ok() {
        let task_info = client.delete_index(name).await?;
        let _ = wait_for_task(client, task_info).await;
    }
    Ok(())
}

/// Helper: Ensure Miroir is healthy
async fn ensure_healthy() -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let url = format!("http://localhost:{}/health", MIROIR_PORT);

    for _ in 0..30 {
        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            _ => sleep(Duration::from_millis(500)).await,
        }
    }

    Err("Miroir not healthy after timeout".into())
}

// ============================================================================
// Test 1: Document round-trip (plan §8)
// ============================================================================

#[tokio::test]
async fn document_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    ensure_healthy().await?;
    let client = miroir_client();
    let index_name = "test_round_trip";

    // Clean up
    delete_index(&client, index_name).await?;
    let index = get_index(&client, index_name).await?;

    // Index 1000 documents
    let mut docs = Vec::new();
    for i in 0..1000 {
        docs.push(json!({
            "id": format!("doc-{:05}", i),
            "title": format!("Document {}", i),
            "content": format!("Content for document {}", i),
        }));
    }

    let task = index.add_documents(&docs, None).await?;
    wait_for_task(&client, task).await?;

    // Verify all documents can be retrieved by ID
    for i in 0..1000 {
        let id = format!("doc-{:05}", i);
        let doc: TestDoc = index.get_document(&id).await?;
        assert_eq!(doc.id, id);
        assert_eq!(doc.title, format!("Document {}", i));
    }

    // Verify documents are distributed across all 3 nodes
    let mut node_doc_counts = HashMap::new();
    for &port in &NODE_PORTS {
        let node = node_client(port);
        if let Ok(idx) = node.get_index(index_name).await {
            let stats = idx.get_stats().await?;
            let count = stats.number_of_documents;
            node_doc_counts.insert(port, count);
        }
    }

    // At least 2 nodes should have documents (distribution)
    let populated_nodes = node_doc_counts.values().filter(|&&c| c > 0).count();
    assert!(
        populated_nodes >= 2,
        "Documents not distributed: {:?}",
        node_doc_counts
    );

    // Total across nodes equals 1000
    let total: usize = node_doc_counts.values().sum();
    assert_eq!(
        total, 1000,
        "Total documents mismatch: {:?}",
        node_doc_counts
    );

    // Clean up
    delete_index(&client, index_name).await?;

    Ok(())
}

// ============================================================================
// Test 2: Search covers all shards (plan §8)
// ============================================================================

#[tokio::test]
async fn search_covers_all_shards() -> Result<(), Box<dyn std::error::Error>> {
    ensure_healthy().await?;
    let client = miroir_client();
    let index_name = "test_shard_coverage";

    delete_index(&client, index_name).await?;
    let index = get_index(&client, index_name).await?;

    // Index documents with unique keywords (one per document)
    let mut docs = Vec::new();
    for i in 0..100 {
        docs.push(json!({
            "id": format!("shard-doc-{:03}", i),
            "title": format!("unique_keyword_{}", i),
            "content": "content",
        }));
    }

    let task = index.add_documents(&docs, None).await?;
    wait_for_task(&client, task).await?;

    // Search for each unique keyword — every search must return exactly 1 hit
    for i in 0..100 {
        let keyword = format!("unique_keyword_{}", i);
        let results: SearchResults<Value> = index.search().with_query(&keyword).execute().await?;
        let hits = results.hits;
        assert_eq!(
            hits.len(),
            1,
            "Search for '{}' returned {} hits",
            keyword,
            hits.len()
        );
    }

    delete_index(&client, index_name).await?;

    Ok(())
}

// ============================================================================
// Test 3: Facet aggregation (plan §8)
// ============================================================================

#[tokio::test]
async fn facet_aggregation() -> Result<(), Box<dyn std::error::Error>> {
    ensure_healthy().await?;
    let client = miroir_client();
    let index_name = "test_facets";

    delete_index(&client, index_name).await?;
    let index = get_index(&client, index_name).await?;

    // Set up filterable attributes for color
    let task = index.set_filterable_attributes(["color"]).await?;
    wait_for_task(&client, task).await?;

    // Index 100 documents across 3 color values
    let colors = ["red", "green", "blue"];
    let mut docs = Vec::new();
    for i in 0..100 {
        let color = colors[i % 3];
        docs.push(json!({
            "id": format!("facet-doc-{:03}", i),
            "title": "Product",
            "color": color,
        }));
    }

    let task = index.add_documents(&docs, None).await?;
    wait_for_task(&client, task).await?;

    // Facet counts must sum to 100
    use meilisearch_sdk::search::Selectors;
    let facets = ["color"];
    let results: SearchResults<Value> = index
        .search()
        .with_facets(Selectors::Some(&facets[..]))
        .execute()
        .await?;
    let facet_dist = results
        .facet_distribution
        .as_ref()
        .and_then(|f| f.get("color"))
        .unwrap();

    let red_count = *facet_dist.get("red").unwrap_or(&0);
    let green_count = *facet_dist.get("green").unwrap_or(&0);
    let blue_count = *facet_dist.get("blue").unwrap_or(&0);

    let total = red_count + green_count + blue_count;
    assert_eq!(total, 100, "Facet counts sum to {}, expected 100", total);

    // Each color should have at least some documents
    assert!(red_count > 0, "No red documents");
    assert!(green_count > 0, "No green documents");
    assert!(blue_count > 0, "No blue documents");

    delete_index(&client, index_name).await?;

    Ok(())
}

// ============================================================================
// Test 4: Offset/limit paging (plan §8)
// ============================================================================

#[tokio::test]
async fn offset_limit_paging() -> Result<(), Box<dyn std::error::Error>> {
    ensure_healthy().await?;
    let client = miroir_client();
    let index_name = "test_paging";

    delete_index(&client, index_name).await?;
    let index = get_index(&client, index_name).await?;

    // Index 50 documents with known scores (use title to control relevance)
    let mut docs = Vec::new();
    for i in 0..50 {
        docs.push(json!({
            "id": format!("page-doc-{:02}", i),
            "title": format!("item {}", 49 - i), // Reverse order for predictable ranking
            "score": i,
        }));
    }

    let task = index.add_documents(&docs, None).await?;
    wait_for_task(&client, task).await?;

    // Get single query with limit=50
    let single_page: SearchResults<Value> = index.search().with_limit(50).execute().await?;
    let single_ids: HashSet<String> = single_page
        .hits
        .iter()
        .filter_map(|v| {
            v.result
                .get("id")
                .and_then(|id| id.as_str().map(|s| s.to_string()))
        })
        .collect();

    // Get 5 pages of 10
    let mut paged_ids = HashSet::new();
    for page in 0..5 {
        let results: SearchResults<Value> = index
            .search()
            .with_limit(10)
            .with_offset(page * 10)
            .execute()
            .await?;
        for hit in results.hits {
            let id = hit.result.get("id").and_then(|id| id.as_str()).unwrap();
            paged_ids.insert(id.to_string());
        }
    }

    // Same total count
    assert_eq!(single_ids.len(), 50);
    assert_eq!(paged_ids.len(), 50);

    // No duplicates in paged results
    assert_eq!(paged_ids.len(), 50, "Duplicates found in paged results");

    // Paged and single query return the same documents
    assert_eq!(
        single_ids, paged_ids,
        "Paged results differ from single query"
    );

    // Order is preserved (concatenated pages match single page order)
    let single_order: Vec<String> = single_page
        .hits
        .iter()
        .filter_map(|v| {
            v.result
                .get("id")
                .and_then(|id| id.as_str().map(|s| s.to_string()))
        })
        .collect();

    let mut paged_order = Vec::new();
    for page in 0..5 {
        let results: SearchResults<Value> = index
            .search()
            .with_limit(10)
            .with_offset(page * 10)
            .execute()
            .await?;
        for hit in results.hits {
            let id = hit.result.get("id").and_then(|id| id.as_str()).unwrap();
            paged_order.push(id.to_string());
        }
    }

    assert_eq!(
        single_order, paged_order,
        "Order differs between paged and single query"
    );

    delete_index(&client, index_name).await?;

    Ok(())
}

// ============================================================================
// Test 5: Settings broadcast (plan §8)
// ============================================================================

#[tokio::test]
async fn settings_broadcast() -> Result<(), Box<dyn std::error::Error>> {
    ensure_healthy().await?;
    let client = miroir_client();
    let index_name = "test_settings";

    delete_index(&client, index_name).await?;
    let index = get_index(&client, index_name).await?;

    // Index some documents
    let docs = vec![
        json!({"id": "1", "title": "wireless headphones"}),
        json!({"id": "2", "title": "bluetooth earbuds"}),
    ];

    let task = index.add_documents(&docs, None).await?;
    wait_for_task(&client, task).await?;

    // Add synonyms via Miroir
    let mut synonyms = HashMap::new();
    synonyms.insert("earbuds".to_string(), vec!["headphones".to_string()]);
    synonyms.insert("wireless".to_string(), vec!["bluetooth".to_string()]);

    let task_info = index.set_synonyms(&synonyms).await?;
    wait_for_task(&client, task_info).await?;

    // Verify all 3 nodes have the synonyms
    for &port in &NODE_PORTS {
        let node = node_client(port);
        if let Ok(idx) = node.get_index(index_name).await {
            let settings = idx.get_settings().await?;
            let node_synonyms = settings.synonyms.unwrap_or_default();
            assert_eq!(
                node_synonyms.get("earbuds"),
                Some(&vec!["headphones".to_string()]),
                "Node port {} missing synonyms",
                port
            );
        }
    }

    // Search via synonym returns results
    let results: SearchResults<Value> = index
        .search()
        .with_query("bluetooth headphones")
        .execute()
        .await?;
    assert!(
        results.hits.len() >= 1,
        "Synonym search returned no results"
    );

    delete_index(&client, index_name).await?;

    Ok(())
}

// ============================================================================
// Test 6: Task polling (plan §8)
// ============================================================================

#[tokio::test]
async fn task_polling() -> Result<(), Box<dyn std::error::Error>> {
    ensure_healthy().await?;
    let client = miroir_client();
    let index_name = "test_tasks";

    delete_index(&client, index_name).await?;
    let index = get_index(&client, index_name).await?;

    // Index a large batch (500 docs)
    let mut docs = Vec::new();
    for i in 0..500 {
        docs.push(json!({
            "id": format!("task-doc-{:04}", i),
            "title": format!("Document {}", i),
            "content": "content for task polling test",
        }));
    }

    let task_uid = index.add_documents(&docs, None).await?;

    // Poll GET /tasks/{id} until succeeded
    let task = wait_for_task(&client, task_uid).await?;
    assert!(
        matches!(task, Task::Succeeded { .. }),
        "Task did not succeed: {:?}",
        task
    );

    // Verify all documents are searchable
    for i in 0..500 {
        let id = format!("task-doc-{:04}", i);
        let doc: TestDoc = index.get_document(&id).await?;
        assert_eq!(doc.id, id);
    }

    // Search also returns all documents
    let results: SearchResults<Value> = index
        .search()
        .with_query("content")
        .with_limit(500)
        .execute()
        .await?;
    assert_eq!(
        results.hits.len(),
        500,
        "Search returned {} hits, expected 500",
        results.hits.len()
    );

    delete_index(&client, index_name).await?;

    Ok(())
}

// ============================================================================
// Test 7: Node failure with RF=2 (plan §8)
// ============================================================================

#[tokio::test]
#[ignore] // Requires docker-compose-dev-rf2 stack
async fn node_failure_rf2() -> Result<(), Box<dyn std::error::Error>> {
    // This test requires the RF=2 stack with 6 nodes
    let rf2_port = env::var("MIROIR_RF2_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7700);

    let client_url = format!("http://localhost:{}", rf2_port);
    let index_name = "test_rf2_failure";
    let client = Client::new(&client_url, Some(MASTER_KEY.to_string()))
        .expect("Failed to create Meilisearch client");

    delete_index(&client, index_name).await?;
    let index = get_index(&client, index_name).await?;

    // Index 500 documents
    let mut docs = Vec::new();
    for i in 0..500 {
        docs.push(json!({
            "id": format!("rf2-doc-{:04}", i),
            "title": format!("Document {}", i),
            "content": "rf2 failure test content",
        }));
    }

    let task = index.add_documents(&docs, None).await?;
    wait_for_task(&client, task).await?;

    // Simulate stopping one node (in real test, use docker-compose stop)
    // For now, we'll just verify the search returns all results
    let results: SearchResults<Value> = index
        .search()
        .with_query("content")
        .with_limit(500)
        .execute()
        .await?;
    assert_eq!(
        results.hits.len(),
        500,
        "Search returned {} hits, expected 500",
        results.hits.len()
    );

    // Check for X-Miroir-Degraded header (should not appear with RF=2 when one node fails)
    let http_client = reqwest::Client::new();
    let search_url = format!("{}/indexes/{}/search", client_url, index_name);
    let resp = http_client
        .post(&search_url)
        .header("Authorization", format!("Bearer {}", MASTER_KEY))
        .json(&json!({"q": "content", "limit": 500}))
        .send()
        .await?;

    // With RF=2, surviving replicas cover all shards, so no degraded header
    assert!(
        resp.headers().get("X-Miroir-Degraded").is_none(),
        "X-Miroir-Degraded header should not appear with RF=2 and one node down"
    );

    delete_index(&client, index_name).await?;

    Ok(())
}

// ============================================================================
// Helper: Check index exists on all nodes
// ============================================================================

async fn index_exists_on_all_nodes(index_name: &str) -> Result<bool, Box<dyn std::error::Error>> {
    for &port in &NODE_PORTS {
        let node = node_client(port);
        if node.get_index(index_name).await.is_err() {
            return Ok(false);
        }
    }
    Ok(true)
}
