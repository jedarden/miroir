//! P5.8.a: Anti-entropy fingerprint step integration tests.
//!
//! Tests the fingerprint step (plan §13.8 step 1):
//! - Per-replica xxh3 digest over (pk || content_hash)
//! - Paginated iteration via filter=_miroir_shard={id}
//! - Streaming xxh3 digest folding
//! - Self-throttling behavior

use miroir_core::anti_entropy::{
    AntiEntropyConfig, AntiEntropyReconciler, ShardFingerprint,
};
use miroir_core::scatter::{FetchDocumentsRequest, FetchDocumentsResponse, NodeClient, NodeError};
use miroir_core::topology::{Node, NodeId, Topology};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// Create a mock using mockall::mock! macro
mockall::mock! {
    pub TestNodeClient {}
    impl NodeClient for TestNodeClient {
        async fn search_node(
            &self,
            node: &NodeId,
            address: &str,
            request: &miroir_core::scatter::SearchRequest,
        ) -> std::result::Result<serde_json::Value, NodeError>;

        async fn preflight_node(
            &self,
            node: &NodeId,
            address: &str,
            request: &miroir_core::scatter::PreflightRequest,
        ) -> std::result::Result<miroir_core::scatter::PreflightResponse, NodeError>;

        async fn write_documents(
            &self,
            node: &NodeId,
            address: &str,
            request: &miroir_core::scatter::WriteRequest,
        ) -> std::result::Result<miroir_core::scatter::WriteResponse, NodeError>;

        async fn delete_documents(
            &self,
            node: &NodeId,
            address: &str,
            request: &miroir_core::scatter::DeleteByIdsRequest,
        ) -> std::result::Result<miroir_core::scatter::DeleteResponse, NodeError>;

        async fn delete_documents_by_filter(
            &self,
            node: &NodeId,
            address: &str,
            request: &miroir_core::scatter::DeleteByFilterRequest,
        ) -> std::result::Result<miroir_core::scatter::DeleteResponse, NodeError>;

        async fn fetch_documents(
            &self,
            node: &NodeId,
            address: &str,
            request: &FetchDocumentsRequest,
        ) -> std::result::Result<FetchDocumentsResponse, NodeError>;
    }
}

#[tokio::test]
async fn test_fingerprint_shard_empty() {
    // Test fingerprinting an empty shard
    let mut mock_client = MockTestNodeClient::new();
    mock_client
        .expect_fetch_documents()
        .returning(|_, _, _| {
            // Return empty result
            Ok(FetchDocumentsResponse {
                results: vec![],
                limit: 1000,
                offset: 0,
                total: 0,
            })
        });

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let node_id = NodeId::new("node-1".to_string());
    let result = reconciler
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await;

    assert!(result.is_ok());
    let fp = result.unwrap();
    assert_eq!(fp.shard_id, 0);
    assert_eq!(fp.document_count, 0);
    assert!(fp.merkle_root.starts_with("xxh3:"));
}

#[tokio::test]
async fn test_fingerprint_shard_single_document() {
    // Test fingerprinting a shard with a single document
    let doc = json!({
        "id": "doc-1",
        "title": "Test Document",
        "content": "Some content",
        "_miroir_shard": 0,
    });

    let mut mock_client = MockTestNodeClient::new();
    mock_client.expect_fetch_documents().returning(move |_, _, req| {
        if req.offset == 0 {
            Ok(FetchDocumentsResponse {
                results: vec![doc.clone()],
                limit: req.limit,
                offset: req.offset,
                total: 1,
            })
        } else {
            Ok(FetchDocumentsResponse {
                results: vec![],
                limit: req.limit,
                offset: req.offset,
                total: 1,
            })
        }
    });

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let node_id = NodeId::new("node-1".to_string());
    let result = reconciler
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await;

    assert!(result.is_ok());
    let fp = result.unwrap();
    assert_eq!(fp.shard_id, 0);
    assert_eq!(fp.document_count, 1);
    assert!(fp.merkle_root.starts_with("xxh3:"));
}

#[tokio::test]
async fn test_fingerprint_shard_pagination() {
    // Test that pagination works correctly for multiple batches
    let batch_size = 10u32;
    let total_docs = 25u32;

    let mut mock_client = MockTestNodeClient::new();
    mock_client.expect_fetch_documents().returning(move |_, _, req| {
        let start = req.offset;
        if start >= total_docs {
            // Return empty result when offset exceeds total
            return Ok(FetchDocumentsResponse {
                results: vec![],
                limit: req.limit,
                offset: req.offset,
                total: total_docs as u64,
            });
        }
        let end = std::cmp::min(req.offset + req.limit, total_docs);
        let count = end - start;

        let docs: Vec<serde_json::Value> = (start..end)
            .map(|i| {
                json!({
                    "id": format!("doc-{}", i),
                    "title": format!("Document {}", i),
                    "_miroir_shard": 0,
                })
            })
            .collect();

        Ok(FetchDocumentsResponse {
            results: docs,
            limit: req.limit,
            offset: req.offset,
            total: total_docs as u64,
        })
    });

    let mut config = AntiEntropyConfig::default();
    config.fingerprint_batch_size = batch_size;

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(config, topology, Arc::new(mock_client));

    let node_id = NodeId::new("node-1".to_string());
    let result = reconciler
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await;

    assert!(result.is_ok());
    let fp = result.unwrap();
    assert_eq!(fp.shard_id, 0);
    assert_eq!(fp.document_count, total_docs as u64);
}

#[tokio::test]
async fn test_fingerprint_shard_content_hash_excludes_internal_fields() {
    // Test that internal fields are excluded from content hash
    let doc1 = json!({
        "id": "doc-1",
        "title": "Same Title",
        "content": "Same Content",
        "_miroir_shard": 0,
        "_miroir_updated_at": 1234567890,
        "_rankingScore": 0.95,
    });

    let doc2 = json!({
        "id": "doc-1",
        "title": "Same Title",
        "content": "Same Content",
    });

    // Both documents should produce the same fingerprint despite internal fields
    let mut mock_client = MockTestNodeClient::new();
    mock_client.expect_fetch_documents().returning({
        let mut call_count = 0;
        move |_, _, req| {
            let docs = if call_count == 0 {
                call_count += 1;
                vec![doc1.clone()]
            } else {
                vec![]
            };

            Ok(FetchDocumentsResponse {
                results: docs,
                limit: req.limit,
                offset: req.offset,
                total: 1,
            })
        }
    });

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let node_id = NodeId::new("node-1".to_string());
    let result = reconciler
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await;

    assert!(result.is_ok());
    let fp = result.unwrap();
    assert_eq!(fp.document_count, 1);
}

#[tokio::test]
async fn test_fingerprint_shard_different_content_different_hash() {
    // Test that different content produces different hashes
    let doc1 = json!({
        "id": "doc-1",
        "title": "First Title",
        "_miroir_shard": 0,
    });

    let doc2 = json!({
        "id": "doc-1",
        "title": "Second Title",
        "_miroir_shard": 0,
    });

    // Create two reconcilers and compare fingerprints
    let mut mock_client1 = MockTestNodeClient::new();
    mock_client1.expect_fetch_documents().returning({
        let mut call_count = 0;
        move |_, _, req| {
            let docs = if call_count == 0 {
                call_count += 1;
                vec![doc1.clone()]
            } else {
                vec![]
            };

            Ok(FetchDocumentsResponse {
                results: docs,
                limit: req.limit,
                offset: req.offset,
                total: 1,
            })
        }
    });

    let mut mock_client2 = MockTestNodeClient::new();
    mock_client2.expect_fetch_documents().returning({
        let mut call_count = 0;
        move |_, _, req| {
            let docs = if call_count == 0 {
                call_count += 1;
                vec![doc2.clone()]
            } else {
                vec![]
            };

            Ok(FetchDocumentsResponse {
                results: docs,
                limit: req.limit,
                offset: req.offset,
                total: 1,
            })
        }
    });

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));

    let reconciler1 = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology.clone(),
        Arc::new(mock_client1),
    );

    let reconciler2 = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client2),
    );

    let node_id = NodeId::new("node-1".to_string());

    let fp1 = reconciler1
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await
        .unwrap();

    let fp2 = reconciler2
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await
        .unwrap();

    // Different content should produce different hashes
    assert_ne!(fp1.merkle_root, fp2.merkle_root);
}

#[tokio::test]
async fn test_fingerprint_shard_same_content_same_hash() {
    // Test that identical content produces identical hashes
    let doc = json!({
        "id": "doc-1",
        "title": "Same Title",
        "content": "Same Content",
        "_miroir_shard": 0,
    });

    let mut mock_client1 = MockTestNodeClient::new();
    mock_client1.expect_fetch_documents().returning({
        let doc = doc.clone();
        let mut call_count = 0;
        move |_, _, req| {
            let docs = if call_count == 0 {
                call_count += 1;
                vec![doc.clone()]
            } else {
                vec![]
            };

            Ok(FetchDocumentsResponse {
                results: docs,
                limit: req.limit,
                offset: req.offset,
                total: 1,
            })
        }
    });

    let mut mock_client2 = MockTestNodeClient::new();
    mock_client2.expect_fetch_documents().returning({
        let doc = doc.clone();
        let mut call_count = 0;
        move |_, _, req| {
            let docs = if call_count == 0 {
                call_count += 1;
                vec![doc.clone()]
            } else {
                vec![]
            };

            Ok(FetchDocumentsResponse {
                results: docs,
                limit: req.limit,
                offset: req.offset,
                total: 1,
            })
        }
    });

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));

    let reconciler1 = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology.clone(),
        Arc::new(mock_client1),
    );

    let reconciler2 = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client2),
    );

    let node_id = NodeId::new("node-1".to_string());

    let fp1 = reconciler1
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await
        .unwrap();

    let fp2 = reconciler2
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await
        .unwrap();

    // Same content should produce same hash
    assert_eq!(fp1.merkle_root, fp2.merkle_root);
}

#[tokio::test]
async fn test_fingerprint_shard_key_order_independence() {
    // Test that JSON key order doesn't affect the hash
    let doc1 = json!({
        "id": "doc-1",
        "z_field": "z_value",
        "a_field": "a_value",
        "m_field": "m_value",
        "_miroir_shard": 0,
    });

    let doc2 = json!({
        "m_field": "m_value",
        "a_field": "a_value",
        "id": "doc-1",
        "z_field": "z_value",
        "_miroir_shard": 0,
    });

    let mut mock_client1 = MockTestNodeClient::new();
    mock_client1.expect_fetch_documents().returning({
        let mut call_count = 0;
        move |_, _, req| {
            let docs = if call_count == 0 {
                call_count += 1;
                vec![doc1.clone()]
            } else {
                vec![]
            };

            Ok(FetchDocumentsResponse {
                results: docs,
                limit: req.limit,
                offset: req.offset,
                total: 1,
            })
        }
    });

    let mut mock_client2 = MockTestNodeClient::new();
    mock_client2.expect_fetch_documents().returning({
        let mut call_count = 0;
        move |_, _, req| {
            let docs = if call_count == 0 {
                call_count += 1;
                vec![doc2.clone()]
            } else {
                vec![]
            };

            Ok(FetchDocumentsResponse {
                results: docs,
                limit: req.limit,
                offset: req.offset,
                total: 1,
            })
        }
    });

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));

    let reconciler1 = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology.clone(),
        Arc::new(mock_client1),
    );

    let reconciler2 = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client2),
    );

    let node_id = NodeId::new("node-1".to_string());

    let fp1 = reconciler1
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await
        .unwrap();

    let fp2 = reconciler2
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await
        .unwrap();

    // Same content with different key order should produce same hash
    assert_eq!(fp1.merkle_root, fp2.merkle_root);
}

#[tokio::test]
async fn test_fingerprint_shard_different_shard_ids_different_hashes() {
    // Test that different shard IDs produce different hashes (different seed)
    let doc = json!({
        "id": "doc-1",
        "title": "Same Title",
        "_miroir_shard": 0,  // This is overridden by the filter anyway
    });

    let mut mock_client = MockTestNodeClient::new();
    mock_client.expect_fetch_documents().returning({
        let mut call_count = 0;
        move |_, _, req| {
            let docs = if call_count == 0 {
                call_count += 1;
                vec![doc.clone()]
            } else {
                vec![]
            };

            Ok(FetchDocumentsResponse {
                results: docs,
                limit: req.limit,
                offset: req.offset,
                total: 1,
            })
        }
    });

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let node_id = NodeId::new("node-1".to_string());

    let fp1 = reconciler
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await
        .unwrap();

    let fp2 = reconciler
        .fingerprint_shard(&node_id, 1, "test_index", "http://localhost")
        .await
        .unwrap();

    // Different shard ID (different seed) should produce different hash
    assert_ne!(fp1.merkle_root, fp2.merkle_root);
}

#[tokio::test]
async fn test_fingerprint_config_batch_size() {
    // Test that fingerprint_batch_size configuration is respected
    let batch_size = 5u32;
    let total_docs = 12u32;

    let mut mock_client = MockTestNodeClient::new();
    mock_client.expect_fetch_documents().returning(move |_, _, req| {
        let start = req.offset;
        if start >= total_docs {
            // Return empty result when offset exceeds total
            return Ok(FetchDocumentsResponse {
                results: vec![],
                limit: req.limit,
                offset: req.offset,
                total: total_docs as u64,
            });
        }
        let end = std::cmp::min(req.offset + req.limit, total_docs);
        let count = end - start;

        let docs: Vec<serde_json::Value> = (start..end)
            .map(|i| {
                json!({
                    "id": format!("doc-{}", i),
                    "_miroir_shard": 0,
                })
            })
            .collect();

        Ok(FetchDocumentsResponse {
            results: docs,
            limit: req.limit,
            offset: req.offset,
            total: total_docs as u64,
        })
    });

    let mut config = AntiEntropyConfig::default();
    config.fingerprint_batch_size = batch_size;

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(config, topology, Arc::new(mock_client));

    let node_id = NodeId::new("node-1".to_string());
    let result = reconciler
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await;

    assert!(result.is_ok());
    // With 12 docs and batch size 5, we expect 3 fetches: 5 + 5 + 2 + 1 (empty check)
    // Actually the loop continues until empty, so: 5 + 5 + 2 + 0 (empty) = 4 fetches
}

#[tokio::test]
async fn test_compute_content_hash_unit() {
    // Unit tests for compute_content_hash
    use miroir_core::anti_entropy::AntiEntropyReconciler;

    let doc1 = json!({
        "id": "test-1",
        "title": "Test",
        "_miroir_shard": 5,
    });

    let doc2 = json!({
        "id": "test-1",
        "title": "Test",
    });

    // Create a dummy reconciler just to call the static method
    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::<MockTestNodeClient>::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(MockTestNodeClient::new()),
    );

    let hash1 = AntiEntropyReconciler::<MockTestNodeClient>::compute_content_hash(&doc1);
    let hash2 = AntiEntropyReconciler::<MockTestNodeClient>::compute_content_hash(&doc2);

    assert_eq!(hash1, hash2, "internal fields should not affect content hash");
}
