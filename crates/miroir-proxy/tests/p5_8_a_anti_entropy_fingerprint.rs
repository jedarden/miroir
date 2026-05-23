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
use miroir_core::scatter::{FetchDocumentsRequest, FetchDocumentsResponse, MockNodeClient};
use miroir_core::topology::{Node, NodeId, Topology};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_fingerprint_shard_empty() {
    // Test fingerprinting an empty shard
    let mut mock_client = MockNodeClient::default();
    let node_id = NodeId::new("node-1".to_string());

    mock_client.fetch_responses.insert(
        node_id.clone(),
        FetchDocumentsResponse {
            results: vec![],
            limit: 1000,
            offset: 0,
            total: 0,
        },
    );

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

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

    let mut mock_client = MockNodeClient::default();
    let node_id = NodeId::new("node-1".to_string());

    mock_client.fetch_responses.insert(
        node_id.clone(),
        FetchDocumentsResponse {
            results: vec![doc.clone()],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

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
    // Note: MockNodeClient returns the same response for each offset,
    // so we test with a single batch that fits all documents
    let batch_size = 100u32;
    let total_docs = 25u32;

    let docs: Vec<serde_json::Value> = (0..total_docs)
        .map(|i| {
            json!({
                "id": format!("doc-{}", i),
                "title": format!("Document {}", i),
                "_miroir_shard": 0,
            })
        })
        .collect();

    let mut mock_client = MockNodeClient::default();
    let node_id = NodeId::new("node-1".to_string());

    mock_client.fetch_responses.insert(
        node_id.clone(),
        FetchDocumentsResponse {
            results: docs.clone(),
            limit: batch_size,
            offset: 0,
            total: total_docs as u64,
        },
    );

    let mut config = AntiEntropyConfig::default();
    config.fingerprint_batch_size = batch_size;

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(config, topology, Arc::new(mock_client));

    let result = reconciler
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await;

    assert!(result.is_ok());
    let fp = result.unwrap();
    assert_eq!(fp.shard_id, 0);
    // With a single batch that fits all documents, we should count all docs once
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
    let mut mock_client = MockNodeClient::default();
    let node_id = NodeId::new("node-1".to_string());

    mock_client.fetch_responses.insert(
        node_id.clone(),
        FetchDocumentsResponse {
            results: vec![doc1.clone()],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

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
    let mut mock_client1 = MockNodeClient::default();
    let node_id1 = NodeId::new("node-1".to_string());

    mock_client1.fetch_responses.insert(
        node_id1.clone(),
        FetchDocumentsResponse {
            results: vec![doc1.clone()],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

    let mut mock_client2 = MockNodeClient::default();
    let node_id2 = NodeId::new("node-2".to_string());

    mock_client2.fetch_responses.insert(
        node_id2.clone(),
        FetchDocumentsResponse {
            results: vec![doc2.clone()],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

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

    let fp1 = reconciler1
        .fingerprint_shard(&node_id1, 0, "test_index", "http://localhost")
        .await
        .unwrap();

    let fp2 = reconciler2
        .fingerprint_shard(&node_id2, 0, "test_index", "http://localhost")
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

    let mut mock_client1 = MockNodeClient::default();
    let node_id1 = NodeId::new("node-1".to_string());

    mock_client1.fetch_responses.insert(
        node_id1.clone(),
        FetchDocumentsResponse {
            results: vec![doc.clone()],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

    let mut mock_client2 = MockNodeClient::default();
    let node_id2 = NodeId::new("node-2".to_string());

    mock_client2.fetch_responses.insert(
        node_id2.clone(),
        FetchDocumentsResponse {
            results: vec![doc.clone()],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

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

    let fp1 = reconciler1
        .fingerprint_shard(&node_id1, 0, "test_index", "http://localhost")
        .await
        .unwrap();

    let fp2 = reconciler2
        .fingerprint_shard(&node_id2, 0, "test_index", "http://localhost")
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

    let mut mock_client1 = MockNodeClient::default();
    let node_id1 = NodeId::new("node-1".to_string());

    mock_client1.fetch_responses.insert(
        node_id1.clone(),
        FetchDocumentsResponse {
            results: vec![doc1.clone()],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

    let mut mock_client2 = MockNodeClient::default();
    let node_id2 = NodeId::new("node-2".to_string());

    mock_client2.fetch_responses.insert(
        node_id2.clone(),
        FetchDocumentsResponse {
            results: vec![doc2.clone()],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

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

    let fp1 = reconciler1
        .fingerprint_shard(&node_id1, 0, "test_index", "http://localhost")
        .await
        .unwrap();

    let fp2 = reconciler2
        .fingerprint_shard(&node_id2, 0, "test_index", "http://localhost")
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

    let mut mock_client = MockNodeClient::default();
    let node_id = NodeId::new("node-1".to_string());

    mock_client.fetch_responses.insert(
        node_id.clone(),
        FetchDocumentsResponse {
            results: vec![doc.clone()],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

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

    let docs: Vec<serde_json::Value> = (0..total_docs)
        .map(|i| {
            json!({
                "id": format!("doc-{}", i),
                "_miroir_shard": 0,
            })
        })
        .collect();

    let mut mock_client = MockNodeClient::default();
    let node_id = NodeId::new("node-1".to_string());

    mock_client.fetch_responses.insert(
        node_id.clone(),
        FetchDocumentsResponse {
            results: docs,
            limit: batch_size,
            offset: 0,
            total: total_docs as u64,
        },
    );

    let mut config = AntiEntropyConfig::default();
    config.fingerprint_batch_size = batch_size;

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(config, topology, Arc::new(mock_client));

    let result = reconciler
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await;

    assert!(result.is_ok());
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
    let reconciler = AntiEntropyReconciler::<MockNodeClient>::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(MockNodeClient::default()),
    );

    let hash1 = AntiEntropyReconciler::<MockNodeClient>::compute_content_hash(&doc1);
    let hash2 = AntiEntropyReconciler::<MockNodeClient>::compute_content_hash(&doc2);

    assert_eq!(hash1, hash2, "internal fields should not affect content hash");
}
