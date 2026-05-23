//! P5.8.b: Anti-entropy diff step integration tests.
//!
//! Tests the diff step (plan §13.8 step 2):
//! - Per-bucket (pk-hash % 256) digest computation
//! - Divergent bucket identification
//! - Divergent primary key enumeration within buckets
//! - Reused by §13.1 reshard verify with PK-keyed bucketing

use miroir_core::anti_entropy::{
    AntiEntropyConfig, AntiEntropyReconciler, ReplicaDiff, ShardFingerprint, BUCKET_COUNT,
};
use miroir_core::scatter::{FetchDocumentsRequest, FetchDocumentsResponse, NodeClient, NodeError};
use miroir_core::topology::{Node, NodeId, Topology};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Test-specific node client that returns predefined responses.
#[derive(Clone)]
struct TestNodeClient {
    responses: Arc<std::sync::Mutex<HashMap<NodeId, Vec<serde_json::Value>>>>,
}

impl TestNodeClient {
    fn new() -> Self {
        Self {
            responses: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    fn set_response(&self, node_id: &NodeId, docs: Vec<serde_json::Value>) {
        self.responses.lock().unwrap().insert(node_id.clone(), docs);
    }
}

impl Default for TestNodeClient {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeClient for TestNodeClient {
    fn search_node(
        &self,
        _node: &NodeId,
        _address: &str,
        _request: &miroir_core::scatter::SearchRequest,
    ) -> impl std::future::Future<Output = std::result::Result<serde_json::Value, NodeError>> + Send {
        async move {
            Ok(json!({"hits": [], "estimatedTotalHits": 0, "processingTimeMs": 0}))
        }
    }

    fn preflight_node(
        &self,
        _node: &NodeId,
        _address: &str,
        _request: &miroir_core::scatter::PreflightRequest,
    ) -> impl std::future::Future<Output = std::result::Result<miroir_core::scatter::PreflightResponse, NodeError>> + Send {
        async move {
            Ok(miroir_core::scatter::PreflightResponse {
                total_docs: 0,
                avg_doc_length: 0.0,
                term_stats: HashMap::new(),
            })
        }
    }

    fn write_documents(
        &self,
        _node: &NodeId,
        _address: &str,
        _request: &miroir_core::scatter::WriteRequest,
    ) -> impl std::future::Future<Output = std::result::Result<miroir_core::scatter::WriteResponse, NodeError>> + Send {
        async move {
            Ok(miroir_core::scatter::WriteResponse {
                success: true,
                task_uid: None,
                message: None,
                code: None,
                error_type: None,
            })
        }
    }

    fn delete_documents(
        &self,
        _node: &NodeId,
        _address: &str,
        _request: &miroir_core::scatter::DeleteByIdsRequest,
    ) -> impl std::future::Future<Output = std::result::Result<miroir_core::scatter::DeleteResponse, NodeError>> + Send {
        async move {
            Ok(miroir_core::scatter::DeleteResponse {
                success: true,
                task_uid: None,
                message: None,
                code: None,
                error_type: None,
            })
        }
    }

    fn delete_documents_by_filter(
        &self,
        _node: &NodeId,
        _address: &str,
        _request: &miroir_core::scatter::DeleteByFilterRequest,
    ) -> impl std::future::Future<Output = std::result::Result<miroir_core::scatter::DeleteResponse, NodeError>> + Send {
        async move {
            Ok(miroir_core::scatter::DeleteResponse {
                success: true,
                task_uid: None,
                message: None,
                code: None,
                error_type: None,
            })
        }
    }

    fn fetch_documents(
        &self,
        node: &NodeId,
        _address: &str,
        request: &FetchDocumentsRequest,
    ) -> impl std::future::Future<Output = std::result::Result<FetchDocumentsResponse, NodeError>> + Send {
        let responses = self.responses.clone();
        let node = node.clone();
        async move {
            let docs = responses.lock().unwrap().get(&node).cloned().unwrap_or_default();
            let total = docs.len() as u64;

            // Apply pagination
            let start = request.offset as usize;
            let end = (start + request.limit as usize).min(docs.len());
            let page = if start < docs.len() {
                docs[start..end].to_vec()
            } else {
                vec![]
            };

            Ok(FetchDocumentsResponse {
                results: page,
                limit: request.limit,
                offset: request.offset,
                total,
            })
        }
    }

    fn get_task_status(
        &self,
        _node: &NodeId,
        _address: &str,
        _request: &miroir_core::scatter::TaskStatusRequest,
    ) -> impl std::future::Future<Output = std::result::Result<miroir_core::scatter::TaskStatusResponse, NodeError>> + Send {
        async move {
            Ok(miroir_core::scatter::TaskStatusResponse {
                task_uid: _request.task_uid,
                status: "succeeded".to_string(),
                error: None,
                error_type: None,
            })
        }
    }
}

#[tokio::test]
async fn test_bucket_for_primary_key_deterministic() {
    // Test that bucket assignment is deterministic
    let pk = "test-primary-key-123";
    let bucket1 = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key(pk);
    let bucket2 = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key(pk);

    assert_eq!(bucket1, bucket2, "bucket assignment should be deterministic");
    assert!(bucket1 < BUCKET_COUNT, "bucket ID should be in range");
}

#[tokio::test]
async fn test_bucket_for_primary_key_distributes() {
    // Test that different keys distribute across buckets
    let mut buckets = std::collections::HashSet::new();

    for i in 0..1000 {
        let pk = format!("key-{}", i);
        let bucket = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key(&pk);
        buckets.insert(bucket);
    }

    // With 1000 keys and 256 buckets, we should hit many buckets
    // (statistically, almost all of them)
    assert!(buckets.len() > 200, "keys should distribute across many buckets");
}

#[tokio::test]
async fn test_fingerprint_shard_includes_bucket_hashes() {
    // Test that fingerprinting now computes per-bucket hashes
    let doc1 = json!({
        "id": "doc-1",
        "title": "First",
        "_miroir_shard": 0,
    });

    let doc2 = json!({
        "id": "doc-2",
        "title": "Second",
        "_miroir_shard": 0,
    });

    let mock_client = TestNodeClient::new();
    let node_id = NodeId::new("node-1".to_string());
    mock_client.set_response(&node_id, vec![doc1, doc2]);

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let result = reconciler
        .fingerprint_shard(&node_id, 0, "test_index", "http://localhost")
        .await
        .unwrap();

    assert_eq!(result.bucket_hashes.len(), BUCKET_COUNT);
    // Each bucket hash should be a valid xxh3 hash string
    for bucket_hash in &result.bucket_hashes {
        assert!(bucket_hash.starts_with("xxh3:"));
    }
}

#[tokio::test]
async fn test_diff_fingerprints_identical() {
    // Test diff with identical fingerprints (no divergence)
    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::<TestNodeClient>::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(TestNodeClient::new()),
    );

    let fp = ShardFingerprint {
        shard_id: 0,
        node_id: "node-1".to_string(),
        merkle_root: "xxh3:123".to_string(),
        document_count: 100,
        bucket_hashes: (0..BUCKET_COUNT).map(|_| "xxh3:abc".to_string()).collect(),
    };

    let divergent = reconciler.diff_fingerprints(&fp, &fp);
    assert!(divergent.is_empty(), "identical fingerprints should have no divergence");
}

#[tokio::test]
async fn test_diff_fingerprints_divergent_buckets() {
    // Test diff with divergent buckets
    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::<TestNodeClient>::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(TestNodeClient::new()),
    );

    let mut fp_a = ShardFingerprint {
        shard_id: 0,
        node_id: "node-a".to_string(),
        merkle_root: "xxh3:123".to_string(),
        document_count: 100,
        bucket_hashes: (0..BUCKET_COUNT).map(|_| "xxh3:abc".to_string()).collect(),
    };

    let mut fp_b = ShardFingerprint {
        shard_id: 0,
        node_id: "node-b".to_string(),
        merkle_root: "xxh3:456".to_string(),
        document_count: 100,
        bucket_hashes: (0..BUCKET_COUNT).map(|_| "xxh3:abc".to_string()).collect(),
    };

    // Make buckets 5, 10, 15 divergent
    fp_b.bucket_hashes[5] = "xxh3:different".to_string();
    fp_b.bucket_hashes[10] = "xxh3:different".to_string();
    fp_b.bucket_hashes[15] = "xxh3:different".to_string();

    let divergent = reconciler.diff_fingerprints(&fp_a, &fp_b);

    assert_eq!(divergent.len(), 3);
    assert!(divergent.contains(&5));
    assert!(divergent.contains(&10));
    assert!(divergent.contains(&15));
}

#[tokio::test]
async fn test_fetch_bucket_pks_filters_by_bucket() {
    // Test that fetch_bucket_pks only returns PKs in the target bucket
    let doc1 = json!({ "id": "key-1", "title": "Doc 1", "_miroir_shard": 0 });
    let doc2 = json!({ "id": "key-2", "title": "Doc 2", "_miroir_shard": 0 });
    let doc3 = json!({ "id": "key-3", "title": "Doc 3", "_miroir_shard": 0 });

    // Determine which bucket each key belongs to
    let bucket_1 = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key("key-1");
    let bucket_2 = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key("key-2");
    let bucket_3 = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key("key-3");

    let mock_client = TestNodeClient::new();
    let node_id = NodeId::new("node-1".to_string());
    mock_client.set_response(&node_id, vec![doc1, doc2, doc3]);

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    // Fetch PKs for bucket_1 - should only contain key-1
    let result = reconciler
        .fetch_bucket_pks(&node_id, 0, bucket_1, "test_index", "http://localhost")
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert!(result.contains_key("key-1"));
    assert!(!result.contains_key("key-2"));
    assert!(!result.contains_key("key-3"));
}

#[tokio::test]
async fn test_compare_bucket_replicas_no_divergence() {
    // Test comparing identical buckets
    let doc = json!({ "id": "key-1", "title": "Same", "_miroir_shard": 0 });
    let bucket_id = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key("key-1");

    let mock_client = TestNodeClient::new();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());
    mock_client.set_response(&node_a, vec![doc.clone()]);
    mock_client.set_response(&node_b, vec![doc]);

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let diff = reconciler
        .compare_bucket_replicas(
            0,
            bucket_id,
            &node_a,
            "http://localhost",
            &node_b,
            "http://localhost",
            "test_index",
        )
        .await
        .unwrap();

    assert_eq!(diff.shard_id, 0);
    assert!(diff.a_only_pks.is_empty());
    assert!(diff.b_only_pks.is_empty());
    assert!(diff.mismatched_pks.is_empty());
}

#[tokio::test]
async fn test_compare_bucket_replicas_a_only() {
    // Test PK only exists on replica A
    let doc_a = json!({ "id": "key-only-a", "title": "Only A", "_miroir_shard": 0 });
    let bucket_id = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key("key-only-a");

    let mock_client = TestNodeClient::new();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());
    mock_client.set_response(&node_a, vec![doc_a]);
    // Node B has no documents

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let diff = reconciler
        .compare_bucket_replicas(
            0,
            bucket_id,
            &node_a,
            "http://localhost",
            &node_b,
            "http://localhost",
            "test_index",
        )
        .await
        .unwrap();

    assert_eq!(diff.a_only_pks.len(), 1);
    assert_eq!(diff.a_only_pks[0], "key-only-a");
    assert!(diff.b_only_pks.is_empty());
    assert!(diff.mismatched_pks.is_empty());
}

#[tokio::test]
async fn test_compare_bucket_replicas_b_only() {
    // Test PK only exists on replica B
    let doc_b = json!({ "id": "key-only-b", "title": "Only B", "_miroir_shard": 0 });
    let bucket_id = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key("key-only-b");

    let mock_client = TestNodeClient::new();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());
    // Node A has no documents
    mock_client.set_response(&node_b, vec![doc_b]);

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let diff = reconciler
        .compare_bucket_replicas(
            0,
            bucket_id,
            &node_a,
            "http://localhost",
            &node_b,
            "http://localhost",
            "test_index",
        )
        .await
        .unwrap();

    assert!(diff.a_only_pks.is_empty());
    assert_eq!(diff.b_only_pks.len(), 1);
    assert_eq!(diff.b_only_pks[0], "key-only-b");
    assert!(diff.mismatched_pks.is_empty());
}

#[tokio::test]
async fn test_compare_bucket_replicas_mismatched_content() {
    // Test same PK but different content (different content hash)
    let doc_a = json!({ "id": "key-mismatch", "title": "Version A", "_miroir_shard": 0 });
    let doc_b = json!({ "id": "key-mismatch", "title": "Version B", "_miroir_shard": 0 });
    let bucket_id = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key("key-mismatch");

    let mock_client = TestNodeClient::new();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());
    mock_client.set_response(&node_a, vec![doc_a]);
    mock_client.set_response(&node_b, vec![doc_b]);

    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let diff = reconciler
        .compare_bucket_replicas(
            0,
            bucket_id,
            &node_a,
            "http://localhost",
            &node_b,
            "http://localhost",
            "test_index",
        )
        .await
        .unwrap();

    assert!(diff.a_only_pks.is_empty());
    assert!(diff.b_only_pks.is_empty());
    assert_eq!(diff.mismatched_pks.len(), 1);
    assert_eq!(diff.mismatched_pks[0], "key-mismatch");
}

#[tokio::test]
async fn test_diff_fingerprints_isolates_divergence() {
    // Test that divergent buckets isolate to ~0.4% of PK space
    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::<TestNodeClient>::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(TestNodeClient::new()),
    );

    // Create a fingerprint with 100 divergent buckets
    let mut fp_a = ShardFingerprint {
        shard_id: 0,
        node_id: "node-a".to_string(),
        merkle_root: "xxh3:123".to_string(),
        document_count: 10000,
        bucket_hashes: (0..BUCKET_COUNT).map(|_| "xxh3:same".to_string()).collect(),
    };

    let mut fp_b = ShardFingerprint {
        shard_id: 0,
        node_id: "node-b".to_string(),
        merkle_root: "xxh3:456".to_string(),
        document_count: 10000,
        bucket_hashes: (0..BUCKET_COUNT).map(|_| "xxh3:same".to_string()).collect(),
    };

    // Make 100 buckets divergent
    for i in 0..100 {
        fp_b.bucket_hashes[i] = format!("xxh3:divergent-{}", i);
    }

    let divergent = reconciler.diff_fingerprints(&fp_a, &fp_b);

    assert_eq!(divergent.len(), 100);

    // Each divergent bucket represents ~1/256 (≈0.4%) of PK space
    // 100 buckets ≈ 39% of total PK space
    let isolation_ratio = divergent.len() as f64 / BUCKET_COUNT as f64;
    assert!((isolation_ratio - (100.0 / 256.0)).abs() < 0.01);
}

#[tokio::test]
async fn test_bucket_count_constant() {
    // Verify BUCKET_COUNT is 256 as specified in the plan
    assert_eq!(BUCKET_COUNT, 256);
}

// ---------------------------------------------------------------------------
// Cross-index bucket comparison tests (plan §13.1 reshard verification)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compare_index_buckets_identical() {
    // Test cross-index comparison with identical content
    let doc1 = json!({ "id": "key-1", "title": "Same", "_miroir_shard": 0 });
    let doc2 = json!({ "id": "key-2", "title": "Same", "_miroir_shard": 1 });
    let doc3 = json!({ "id": "key-3", "title": "Same", "_miroir_shard": 0 });

    let mock_client = TestNodeClient::new();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());

    // Both nodes have the same documents
    mock_client.set_response(&node_a, vec![doc1.clone(), doc2.clone(), doc3.clone()]);
    mock_client.set_response(&node_b, vec![doc1, doc2, doc3]);

    let topology = Arc::new(RwLock::new(Topology::new(2, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let diff = reconciler
        .compare_index_buckets(
            &node_a,
            "http://localhost",
            "index_a",
            2, // shard_count_a
            &node_b,
            "http://localhost",
            "index_b",
            2, // shard_count_b
        )
        .await
        .unwrap();

    assert!(diff.a_only_pks.is_empty());
    assert!(diff.b_only_pks.is_empty());
    assert!(diff.mismatched_pks.is_empty());
}

#[tokio::test]
async fn test_compare_index_buckets_a_only() {
    // Test cross-index comparison with documents only in index A
    let doc_a = json!({ "id": "key-only-a", "title": "Only A", "_miroir_shard": 0 });

    let mock_client = TestNodeClient::new();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());

    mock_client.set_response(&node_a, vec![doc_a]);
    // Node B has no documents

    let topology = Arc::new(RwLock::new(Topology::new(2, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let diff = reconciler
        .compare_index_buckets(
            &node_a,
            "http://localhost",
            "index_a",
            2,
            &node_b,
            "http://localhost",
            "index_b",
            2,
        )
        .await
        .unwrap();

    assert_eq!(diff.a_only_pks.len(), 1);
    assert_eq!(diff.a_only_pks[0], "key-only-a");
    assert!(diff.b_only_pks.is_empty());
    assert!(diff.mismatched_pks.is_empty());
}

#[tokio::test]
async fn test_compare_index_buckets_b_only() {
    // Test cross-index comparison with documents only in index B
    let doc_b = json!({ "id": "key-only-b", "title": "Only B", "_miroir_shard": 0 });

    let mock_client = TestNodeClient::new();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());

    // Node A has no documents
    mock_client.set_response(&node_b, vec![doc_b]);

    let topology = Arc::new(RwLock::new(Topology::new(2, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let diff = reconciler
        .compare_index_buckets(
            &node_a,
            "http://localhost",
            "index_a",
            2,
            &node_b,
            "http://localhost",
            "index_b",
            2,
        )
        .await
        .unwrap();

    assert!(diff.a_only_pks.is_empty());
    assert_eq!(diff.b_only_pks.len(), 1);
    assert_eq!(diff.b_only_pks[0], "key-only-b");
    assert!(diff.mismatched_pks.is_empty());
}

#[tokio::test]
async fn test_compare_index_buckets_mismatched_content() {
    // Test cross-index comparison with same PK but different content
    let doc_a = json!({ "id": "key-mismatch", "title": "Version A", "_miroir_shard": 0 });
    let doc_b = json!({ "id": "key-mismatch", "title": "Version B", "_miroir_shard": 0 });

    let mock_client = TestNodeClient::new();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());

    mock_client.set_response(&node_a, vec![doc_a]);
    mock_client.set_response(&node_b, vec![doc_b]);

    let topology = Arc::new(RwLock::new(Topology::new(2, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let diff = reconciler
        .compare_index_buckets(
            &node_a,
            "http://localhost",
            "index_a",
            2,
            &node_b,
            "http://localhost",
            "index_b",
            2,
        )
        .await
        .unwrap();

    assert!(diff.a_only_pks.is_empty());
    assert!(diff.b_only_pks.is_empty());
    assert_eq!(diff.mismatched_pks.len(), 1);
    assert_eq!(diff.mismatched_pks[0], "key-mismatch");
}

#[tokio::test]
async fn test_compare_index_buckets_across_different_shard_counts() {
    // Test that PK-keyed bucketing works across different shard counts
    // This is the key requirement for reshard verification (plan §13.1 step 4)

    // Same PK but different shard assignments due to different S values
    // With S=16: hash("key-1") % 16 = some_shard_old
    // With S=32: hash("key-1") % 32 = some_shard_new
    // But PK-keyed bucket (hash("key-1") % 256) should be the same

    let doc_old_shard = json!({ "id": "key-reshard", "title": "Same", "_miroir_shard": 5 });
    let doc_new_shard = json!({ "id": "key-reshard", "title": "Same", "_miroir_shard": 21 });

    let mock_client = TestNodeClient::new();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());

    // Simulate live index (S=16) and shadow index (S=32)
    mock_client.set_response(&node_a, vec![doc_old_shard]);
    mock_client.set_response(&node_b, vec![doc_new_shard]);

    let topology = Arc::new(RwLock::new(Topology::new(32, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let diff = reconciler
        .compare_index_buckets(
            &node_a,
            "http://localhost",
            "live_index",   // S=16
            16,
            &node_b,
            "http://localhost",
            "shadow_index",  // S=32
            32,
        )
        .await
        .unwrap();

    // Even though the documents are in different shards, they should match
    // because PK-keyed bucketing is independent of shard count
    assert!(diff.a_only_pks.is_empty(), "PK should exist in both indexes");
    assert!(diff.b_only_pks.is_empty(), "PK should exist in both indexes");
    assert!(diff.mismatched_pks.is_empty(), "Content should be identical");
}

#[tokio::test]
async fn test_compare_index_buckets_multiple_divergent_buckets() {
    // Test that divergence is isolated to specific buckets
    let doc1_a = json!({ "id": "bucket-0-key-a", "title": "In A", "_miroir_shard": 0 });
    let doc2_a = json!({ "id": "bucket-5-key-a", "title": "In A", "_miroir_shard": 0 });
    let doc1_b = json!({ "id": "bucket-0-key-b", "title": "In B", "_miroir_shard": 0 });
    let doc2_b = json!({ "id": "bucket-5-key-b", "title": "In B", "_miroir_shard": 0 });

    // Determine which buckets these keys belong to
    let bucket_0_key_a = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key("bucket-0-key-a");
    let bucket_5_key_a = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key("bucket-5-key-a");
    let bucket_0_key_b = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key("bucket-0-key-b");
    let bucket_5_key_b = AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key("bucket-5-key-b");

    let mock_client = TestNodeClient::new();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());

    mock_client.set_response(&node_a, vec![doc1_a, doc2_a]);
    mock_client.set_response(&node_b, vec![doc1_b, doc2_b]);

    let topology = Arc::new(RwLock::new(Topology::new(2, 1, 1)));
    let reconciler = AntiEntropyReconciler::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(mock_client),
    );

    let diff = reconciler
        .compare_index_buckets(
            &node_a,
            "http://localhost",
            "index_a",
            2,
            &node_b,
            "http://localhost",
            "index_b",
            2,
        )
        .await
        .unwrap();

    // Each key should only exist in one index
    assert_eq!(diff.a_only_pks.len(), 2);
    assert_eq!(diff.b_only_pks.len(), 2);
    assert!(diff.mismatched_pks.is_empty());

    // Verify the divergent keys are in different buckets
    let divergent_buckets: std::collections::HashSet<_> = diff
        .a_only_pks
        .iter()
        .chain(diff.b_only_pks.iter())
        .map(|pk| AntiEntropyReconciler::<TestNodeClient>::bucket_for_primary_key(pk))
        .collect();

    assert!(divergent_buckets.contains(&bucket_0_key_a));
    assert!(divergent_buckets.contains(&bucket_5_key_a));
    assert!(divergent_buckets.contains(&bucket_0_key_b));
    assert!(divergent_buckets.contains(&bucket_5_key_b));
}
