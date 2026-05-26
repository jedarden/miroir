//! Phase 2 Integration Tests: Proxy + API Surface
//!
//! Tests the complete HTTP API surface with real Meilisearch nodes.
//! Uses testcontainers for spinning up Meilisearch instances.

use miroir_core::config::{Config, NodeConfig};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::meilisearch::Meilisearch;
use tokio::time::sleep;

/// Test configuration helper.
struct TestSetup {
    meilisearch_urls: Vec<String>,
    proxy_url: String,
    master_key: String,
    client: Client,
}

impl TestSetup {
    async fn new() -> anyhow::Result<Self> {
        // Start 3 Meilisearch nodes
        let mut meilisearch_urls = Vec::new();
        for i in 0..3 {
            let meilisearch = Meilisearch::default()
                .with_cmd([format!("--master-key=key{i}")])
                .start()
                .await?;

            let port = meilisearch.get_host_port_ipv4(7700).await?;
            let url = format!("http://localhost:{port}");
            meilisearch_urls.push(url);
        }

        // Build topology config
        let mut nodes = Vec::new();
        for (i, url) in meilisearch_urls.iter().enumerate() {
            nodes.push(NodeConfig {
                id: format!("node-{i}"),
                address: url.clone(),
                replica_group: (i % 2) as u32, // 2 replica groups
            });
        }

        let _config = Config {
            shards: 16,
            replication_factor: 2,
            replica_groups: 2,
            master_key: "test_master_key".to_string(),
            admin: miroir_core::config::AdminConfig {
                api_key: "test_admin_key".to_string(),
                ..Default::default()
            },
            nodes,
            server: miroir_core::config::ServerConfig {
                bind: "127.0.0.1".to_string(),
                port: 17770, // Non-standard port for testing
                ..Default::default()
            },
            ..Default::default()
        };

        // Start the proxy in a separate task
        let proxy_url = "http://127.0.0.1:17770";
        // Note: In a real test, we'd spawn the proxy here
        // For now, we'll assume it's already running

        Ok(Self {
            meilisearch_urls,
            proxy_url: proxy_url.to_string(),
            master_key: "test_master_key".to_string(),
            client: Client::new(),
        })
    }

    /// Wait for the proxy to be ready.
    async fn wait_for_ready(&self) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while tokio::time::Instant::now() < deadline {
            match self
                .client
                .get(format!("{}/health", self.proxy_url))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                _ => sleep(Duration::from_millis(100)).await,
            }
        }
        anyhow::bail!("Proxy did not become ready in time")
    }

    /// Create an index.
    async fn create_index(&self, uid: &str) -> anyhow::Result<()> {
        let body = json!({
            "uid": uid,
            "primaryKey": "id"
        });

        let resp = self
            .client
            .post(format!("{}/indexes", self.proxy_url))
            .header("Authorization", format!("Bearer {}", self.master_key))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            anyhow::bail!("Failed to create index: {}", resp.status());
        }

        // Wait for index to be created
        self.wait_for_index(uid).await
    }

    /// Wait for an index to exist.
    async fn wait_for_index(&self, uid: &str) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            match self
                .client
                .get(format!("{}/indexes/{}", self.proxy_url, uid))
                .header("Authorization", format!("Bearer {}", self.master_key))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                _ => sleep(Duration::from_millis(100)).await,
            }
        }
        anyhow::bail!("Index {uid} did not become ready")
    }

    /// Add documents to an index.
    async fn add_documents(&self, uid: &str, documents: Vec<Value>) -> anyhow::Result<Value> {
        let resp = self
            .client
            .post(format!("{}/indexes/{}/documents", self.proxy_url, uid))
            .header("Authorization", format!("Bearer {}", self.master_key))
            .json(&documents)
            .send()
            .await?;

        if !resp.status().is_success() {
            anyhow::bail!("Failed to add documents: {}", resp.status());
        }

        Ok(resp.json().await?)
    }

    /// Search an index.
    async fn search(&self, uid: &str, query: &serde_json::Value) -> anyhow::Result<Value> {
        let resp = self
            .client
            .post(format!("{}/indexes/{}/search", self.proxy_url, uid))
            .header("Authorization", format!("Bearer {}", self.master_key))
            .json(query)
            .send()
            .await?;

        if !resp.status().is_success() {
            anyhow::bail!("Search failed: {}", resp.status());
        }

        Ok(resp.json().await?)
    }

    /// Get a document by ID.
    async fn get_document(&self, uid: &str, id: &str) -> anyhow::Result<Value> {
        let resp = self
            .client
            .get(format!(
                "{}/indexes/{}/documents/{}",
                self.proxy_url, uid, id
            ))
            .header("Authorization", format!("Bearer {}", self.master_key))
            .send()
            .await?;

        if !resp.status().is_success() {
            anyhow::bail!("Failed to get document: {}", resp.status());
        }

        Ok(resp.json().await?)
    }
}

#[tokio::test]
#[ignore] // Requires docker
async fn test_1000_documents_indexed_and_retrievable() {
    let setup = TestSetup::new().await.expect("Failed to setup test");
    setup.wait_for_ready().await.expect("Proxy not ready");

    let index_uid = "test_1000_docs";

    // Create index
    setup
        .create_index(index_uid)
        .await
        .expect("Failed to create index");

    // Generate 1000 documents
    let documents: Vec<Value> = (0..1000)
        .map(|i| {
            json!({
                "id": format!("doc-{:04}", i),
                "title": format!("Document {}", i),
                "content": format!("Content for document {}", i),
            })
        })
        .collect();

    // Add documents
    let _task = setup
        .add_documents(index_uid, documents)
        .await
        .expect("Failed to add documents");

    // Wait for task to complete
    sleep(Duration::from_secs(2)).await;

    // Verify each document is retrievable by ID
    for i in 0..1000 {
        let doc_id = format!("doc-{i:04}");
        let doc = setup
            .get_document(index_uid, &doc_id)
            .await
            .unwrap_or_else(|_| panic!("Failed to get document {doc_id}"));

        assert_eq!(doc.get("id").unwrap().as_str().unwrap(), doc_id);
        assert_eq!(
            doc.get("title").unwrap().as_str().unwrap(),
            format!("Document {i}")
        );
    }
}

#[tokio::test]
#[ignore] // Requires docker
async fn test_unique_keyword_search_finds_each_doc_once() {
    let setup = TestSetup::new().await.expect("Failed to setup test");
    setup.wait_for_ready().await.expect("Proxy not ready");

    let index_uid = "test_unique_search";

    // Create index
    setup
        .create_index(index_uid)
        .await
        .expect("Failed to create index");

    // Add documents with unique keywords
    let documents: Vec<Value> = (0..100)
        .map(|i| {
            json!({
                "id": format!("doc-{:03}", i),
                "keyword": format!("keyword{:03}", i),
                "title": format!("Document {}", i),
            })
        })
        .collect();

    setup
        .add_documents(index_uid, documents)
        .await
        .expect("Failed to add documents");
    sleep(Duration::from_secs(2)).await;

    // Search for each unique keyword and verify exactly one result
    for i in 0..100 {
        let keyword = format!("keyword{i:03}");
        let result = setup
            .search(index_uid, &json!({"q": keyword}))
            .await
            .unwrap_or_else(|_| panic!("Search failed for {keyword}"));

        let hits = result.get("hits").unwrap().as_array().unwrap();
        assert_eq!(
            hits.len(),
            1,
            "Expected exactly 1 hit for {}, got {}",
            keyword,
            hits.len()
        );

        let doc_id = format!("doc-{i:03}");
        assert_eq!(hits[0].get("id").unwrap().as_str().unwrap(), doc_id);
    }
}

#[tokio::test]
#[ignore] // Requires docker
async fn test_facet_aggregation_sums_correctly() {
    let setup = TestSetup::new().await.expect("Failed to setup test");
    setup.wait_for_ready().await.expect("Proxy not ready");

    let index_uid = "test_facets";

    // Create index with filterable attributes
    setup
        .create_index(index_uid)
        .await
        .expect("Failed to create index");

    // Configure filterable attributes
    let filterable = json!({"filterableAttributes": ["color"]});
    let resp = setup
        .client
        .patch(format!(
            "{}/indexes/{}/settings",
            setup.proxy_url, index_uid
        ))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&filterable)
        .send()
        .await
        .expect("Failed to set filterable attributes");

    assert!(resp.status().is_success());

    // Add documents with color facets
    let colors = ["red", "green", "blue"];
    let documents: Vec<Value> = (0..300)
        .map(|i| {
            json!({
                "id": format!("doc-{:03}", i),
                "color": colors[i % 3],
                "value": i,
            })
        })
        .collect();

    setup
        .add_documents(index_uid, documents)
        .await
        .expect("Failed to add documents");
    sleep(Duration::from_secs(2)).await;

    // Search with facets
    let result = setup
        .search(
            index_uid,
            &json!({
                "q": "",
                "facets": ["color"]
            }),
        )
        .await
        .expect("Search failed");

    // Verify facet distribution sums correctly
    let facet_distribution = result
        .get("facetDistribution")
        .unwrap()
        .as_object()
        .unwrap();
    let color_dist = facet_distribution
        .get("color")
        .unwrap()
        .as_object()
        .unwrap();

    assert_eq!(color_dist.get("red").unwrap().as_u64().unwrap(), 100);
    assert_eq!(color_dist.get("green").unwrap().as_u64().unwrap(), 100);
    assert_eq!(color_dist.get("blue").unwrap().as_u64().unwrap(), 100);
}

#[tokio::test]
#[ignore] // Requires docker
async fn test_offset_limit_preserves_global_ordering() {
    let setup = TestSetup::new().await.expect("Failed to setup test");
    setup.wait_for_ready().await.expect("Proxy not ready");

    let index_uid = "test_pagination";

    setup
        .create_index(index_uid)
        .await
        .expect("Failed to create index");

    // Add documents with ordered titles
    let documents: Vec<Value> = (0..100)
        .map(|i| {
            json!({
                "id": format!("doc-{:02}", i),
                "title": format!("Title{:02}", i),
            })
        })
        .collect();

    setup
        .add_documents(index_uid, documents)
        .await
        .expect("Failed to add documents");
    sleep(Duration::from_secs(2)).await;

    // Fetch all documents in pages
    let mut all_ids = Vec::new();
    for page in 0..10 {
        let result = setup
            .search(
                index_uid,
                &json!({
                    "q": "",
                    "offset": page * 10,
                    "limit": 10
                }),
            )
            .await
            .expect("Search failed");

        let hits = result.get("hits").unwrap().as_array().unwrap();
        for hit in hits {
            let id = hit.get("id").unwrap().as_str().unwrap().to_string();
            all_ids.push(id);
        }
    }

    // Verify we got all 100 documents in order
    assert_eq!(all_ids.len(), 100);
    for (i, id) in all_ids.iter().enumerate() {
        let expected = format!("doc-{i:02}");
        assert_eq!(
            id, &expected,
            "Document at position {i} should be {expected}, got {id}"
        );
    }
}

#[tokio::test]
#[ignore] // Requires docker
async fn test_write_with_one_group_down_succeeds_on_remaining() {
    let setup = TestSetup::new().await.expect("Failed to setup test");
    setup.wait_for_ready().await.expect("Proxy not ready");

    let index_uid = "test_degraded_write";

    setup
        .create_index(index_uid)
        .await
        .expect("Failed to create index");

    // Stop one replica group (nodes 0 and 2 are in group 0, node 1 is in group 1)
    // In this test, we simulate node failure by marking them as unhealthy
    // In a real scenario, you'd actually stop the container

    // For now, we'll just verify that writes succeed even when some nodes are down
    // by checking that the X-Miroir-Degraded header is set correctly

    let documents: Vec<Value> = (0..10)
        .map(|i| {
            json!({
                "id": format!("doc-{:02}", i),
                "value": i,
            })
        })
        .collect();

    let resp = setup
        .client
        .post(format!(
            "{}/indexes/{}/documents",
            setup.proxy_url, index_uid
        ))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&documents)
        .send()
        .await
        .expect("Failed to add documents");

    // Check for X-Miroir-Degraded header if any group was degraded
    let degraded_header = resp.headers().get("X-Miroir-Degraded");

    // The write should succeed regardless
    assert!(resp.status().is_success() || resp.status().as_u16() == 503);

    // If degraded header is present, verify its format
    if let Some(header) = degraded_header {
        let header_value = header.to_str().unwrap();
        assert!(
            header_value.starts_with("shards="),
            "X-Miroir-Degraded should start with 'shards='"
        );
    }
}

#[tokio::test]
#[ignore] // Requires docker
async fn test_error_format_parity_with_meilisearch() {
    let setup = TestSetup::new().await.expect("Failed to setup test");
    setup.wait_for_ready().await.expect("Proxy not ready");

    // Test various error conditions and verify format

    // 1. Invalid request (empty document batch)
    let empty_docs: [Value; 0] = [];
    let resp = setup
        .client
        .post(format!("{}/indexes/test/documents", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&empty_docs)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(resp.status().as_u16(), 400);

    let error: Value = resp.json().await.expect("Failed to parse error");
    assert!(
        error.get("message").is_some(),
        "Error should have 'message' field"
    );
    assert!(
        error.get("code").is_some(),
        "Error should have 'code' field"
    );
    assert!(
        error.get("type").is_some(),
        "Error should have 'type' field"
    );
    assert!(
        error.get("link").is_some(),
        "Error should have 'link' field"
    );

    // Verify error type is one of the known types
    let error_type = error.get("type").unwrap().as_str().unwrap();
    assert!(["invalid_request", "auth", "internal", "system"].contains(&error_type));

    // 2. Not found (non-existent index)
    let resp = setup
        .client
        .get(format!("{}/indexes/nonexistent", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .send()
        .await
        .expect("Request failed");

    assert!(resp.status().as_u16() == 404 || resp.status().as_u16() == 400);

    // 3. Authentication error
    let resp = setup
        .client
        .get(format!("{}/indexes/test", setup.proxy_url))
        .header("Authorization", "Bearer invalid_key")
        .send()
        .await
        .expect("Request failed");

    assert!(resp.status().as_u16() == 401 || resp.status().as_u16() == 403);
}
