//! P5.8.b: Anti-entropy diff step integration tests.
//!
//! Tests the diff step (plan §13.8 step 2):
//! - Per-bucket (pk-hash % 256) digest computation
//! - Divergent bucket identification
//! - Divergent primary key enumeration within buckets
//! - Reused by §13.1 reshard verify with PK-keyed bucketing

use miroir_core::anti_entropy::{
    AntiEntropyConfig, AntiEntropyReconciler, ShardFingerprint, BUCKET_COUNT,
};
use miroir_core::scatter::{FetchDocumentsResponse, MockNodeClient};
use miroir_core::topology::{NodeId, Topology};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_bucket_for_primary_key_deterministic() {
    // Test that bucket assignment is deterministic
    let pk = "test-primary-key-123";
    let bucket1 = AntiEntropyReconciler::<MockNodeClient>::bucket_for_primary_key(pk);
    let bucket2 = AntiEntropyReconciler::<MockNodeClient>::bucket_for_primary_key(pk);

    assert_eq!(
        bucket1, bucket2,
        "bucket assignment should be deterministic"
    );
    assert!(bucket1 < BUCKET_COUNT, "bucket ID should be in range");
}

#[tokio::test]
async fn test_bucket_for_primary_key_distributes() {
    // Test that different keys distribute across buckets
    let mut buckets = std::collections::HashSet::new();

    for i in 0..1000 {
        let pk = format!("key-{i}");
        let bucket = AntiEntropyReconciler::<MockNodeClient>::bucket_for_primary_key(&pk);
        buckets.insert(bucket);
    }

    // With 1000 keys and 256 buckets, we should hit many buckets
    // (statistically, almost all of them)
    assert!(
        buckets.len() > 200,
        "keys should distribute across many buckets"
    );
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

    let mut mock_client = MockNodeClient::default();
    let node_id = NodeId::new("node-1".to_string());

    mock_client.fetch_responses.insert(
        node_id.clone(),
        FetchDocumentsResponse {
            results: vec![doc1, doc2],
            limit: 1000,
            offset: 0,
            total: 2,
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
    let reconciler = AntiEntropyReconciler::<MockNodeClient>::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(MockNodeClient::default()),
    );

    let fp = ShardFingerprint {
        shard_id: 0,
        node_id: "node-1".to_string(),
        merkle_root: "xxh3:123".to_string(),
        document_count: 100,
        bucket_hashes: (0..BUCKET_COUNT).map(|_| "xxh3:abc".to_string()).collect(),
    };

    let divergent = reconciler.diff_fingerprints(&fp, &fp);
    assert!(
        divergent.is_empty(),
        "identical fingerprints should have no divergence"
    );
}

#[tokio::test]
async fn test_diff_fingerprints_divergent_buckets() {
    // Test diff with divergent buckets
    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::<MockNodeClient>::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(MockNodeClient::default()),
    );

    let fp_a = ShardFingerprint {
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
async fn test_diff_fingerprints_isolates_divergence() {
    // Test that divergent buckets isolate to ~0.4% of PK space
    let topology = Arc::new(RwLock::new(Topology::new(1, 1, 1)));
    let reconciler = AntiEntropyReconciler::<MockNodeClient>::new(
        AntiEntropyConfig::default(),
        topology,
        Arc::new(MockNodeClient::default()),
    );

    // Create a fingerprint with 100 divergent buckets
    let fp_a = ShardFingerprint {
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
        fp_b.bucket_hashes[i] = format!("xxh3:divergent-{i}");
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

    let mut mock_client = MockNodeClient::default();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());

    // Both nodes have the same documents
    mock_client.fetch_responses.insert(
        node_a.clone(),
        FetchDocumentsResponse {
            results: vec![doc1.clone(), doc2.clone(), doc3.clone()],
            limit: 1000,
            offset: 0,
            total: 3,
        },
    );

    mock_client.fetch_responses.insert(
        node_b.clone(),
        FetchDocumentsResponse {
            results: vec![doc1, doc2, doc3],
            limit: 1000,
            offset: 0,
            total: 3,
        },
    );

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

    let mut mock_client = MockNodeClient::default();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());

    mock_client.fetch_responses.insert(
        node_a.clone(),
        FetchDocumentsResponse {
            results: vec![doc_a],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

    // Node B has no documents
    mock_client.fetch_responses.insert(
        node_b.clone(),
        FetchDocumentsResponse {
            results: vec![],
            limit: 1000,
            offset: 0,
            total: 0,
        },
    );

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

    let mut mock_client = MockNodeClient::default();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());

    // Node A has no documents
    mock_client.fetch_responses.insert(
        node_a.clone(),
        FetchDocumentsResponse {
            results: vec![],
            limit: 1000,
            offset: 0,
            total: 0,
        },
    );

    mock_client.fetch_responses.insert(
        node_b.clone(),
        FetchDocumentsResponse {
            results: vec![doc_b],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

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

    let mut mock_client = MockNodeClient::default();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());

    mock_client.fetch_responses.insert(
        node_a.clone(),
        FetchDocumentsResponse {
            results: vec![doc_a],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

    mock_client.fetch_responses.insert(
        node_b.clone(),
        FetchDocumentsResponse {
            results: vec![doc_b],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

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

    let mut mock_client = MockNodeClient::default();
    let node_a = NodeId::new("node-a".to_string());
    let node_b = NodeId::new("node-b".to_string());

    // Simulate live index (S=16) and shadow index (S=32)
    mock_client.fetch_responses.insert(
        node_a.clone(),
        FetchDocumentsResponse {
            results: vec![doc_old_shard],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

    mock_client.fetch_responses.insert(
        node_b.clone(),
        FetchDocumentsResponse {
            results: vec![doc_new_shard],
            limit: 1000,
            offset: 0,
            total: 1,
        },
    );

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
            "live_index", // S=16
            16,
            &node_b,
            "http://localhost",
            "shadow_index", // S=32
            32,
        )
        .await
        .unwrap();

    // Even though the documents are in different shards, they should match
    // because PK-keyed bucketing is independent of shard count
    assert!(
        diff.a_only_pks.is_empty(),
        "PK should exist in both indexes"
    );
    assert!(
        diff.b_only_pks.is_empty(),
        "PK should exist in both indexes"
    );
    assert!(
        diff.mismatched_pks.is_empty(),
        "Content should be identical"
    );
}
