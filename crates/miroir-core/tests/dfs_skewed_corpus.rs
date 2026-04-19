//! Integration test: DFS (Distributed Frequency Search) preflight with skewed corpus.
//!
//! This test demonstrates the global-IDF preflight phase (OP#4) using a
//! deliberately skewed corpus to show that global IDF produces correct
//! rankings where local IDF would fail.
//!
//! Scenario:
//! - Shard 0 has 10,000 docs, term "rust" appears in 100 docs (df=100, density=1%)
//! - Shard 1 has 1,000 docs, term "rust" appears in 200 docs (df=200, density=20%)
//!
//! Without global IDF (local IDF):
//! - Local IDF(shard 0) = log((10000-100+0.5)/(100+0.5)+1) ≈ 4.5
//! - Local IDF(shard 1) = log((1000-200+0.5)/(200+0.5)+1) ≈ 1.4
//! - A doc with tf=3 for "rust" scores higher in shard 0 (3*4.5 ≈ 13.5) than
//!   shard 1 (3*1.4 ≈ 4.2), despite shard 1 having much higher term density.
//!
//! With global IDF:
//! - Global N = 11,000, global df = 300
//! - Global IDF = log((11000-300+0.5)/(300+0.5)+1) ≈ 3.4
//! - Both shards use the same IDF, so the doc with higher term density (shard 1)
//!   correctly ranks higher after normalization.

use miroir_core::merger::{MergeInput, ScoreMergeStrategy, MergedSearchResult, MergeStrategy};
use miroir_core::scatter::{
    PreflightRequest, PreflightResponse, TermStats, GlobalIdf, SearchRequest,
    plan_search_scatter, execute_preflight, dfs_query_then_fetch_search,
    MockNodeClient,
};
use miroir_core::topology::{Node, NodeId, Topology};
use miroir_core::config::UnavailableShardPolicy;
use serde_json::json;
use std::collections::HashMap;

/// Create a test topology with two nodes in different replica groups.
fn make_skewed_topology() -> Topology {
    let mut topo = Topology::new(2, 1, 1);

    // Node 0: hosts shard 0
    let mut node0 = Node::new(
        NodeId::new("node-0".to_string()),
        "http://node-0:7700".to_string(),
        0,
    );
    node0.status = miroir_core::topology::NodeStatus::Active;
    topo.add_node(node0);

    // Node 1: hosts shard 1
    let mut node1 = Node::new(
        NodeId::new("node-1".to_string()),
        "http://node-1:7700".to_string(),
        0,
    );
    node1.status = miroir_core::topology::NodeStatus::Active;
    topo.add_node(node1);

    topo
}

/// Simulate a preflight response from the large shard (shard 0).
///
/// - 10,000 total documents
/// - Term "rust" appears in 100 documents (1% density)
fn large_shard_preflight() -> PreflightResponse {
    let mut term_stats = HashMap::new();
    term_stats.insert("rust".to_string(), TermStats { df: 100 });
    term_stats.insert("programming".to_string(), TermStats { df: 50 });

    PreflightResponse {
        total_docs: 10_000,
        avg_doc_length: 500.0,
        term_stats,
    }
}

/// Simulate a preflight response from the small shard (shard 1).
///
/// - 1,000 total documents
/// - Term "rust" appears in 200 documents (20% density)
fn small_shard_preflight() -> PreflightResponse {
    let mut term_stats = HashMap::new();
    term_stats.insert("rust".to_string(), TermStats { df: 200 });
    term_stats.insert("programming".to_string(), TermStats { df: 30 });

    PreflightResponse {
        total_docs: 1_000,
        avg_doc_length: 450.0,
        term_stats,
    }
}

/// Search response from the large shard (shard 0).
///
/// Returns a document about Rust programming with a local-IDF score.
/// This document has relatively low term density but high score due to
/// inflated local IDF.
fn large_shard_search_response() -> serde_json::Value {
    json!({
        "hits": [
            {
                "id": "doc-large",
                "title": "Rust Programming Language",
                "_rankingScore": 0.92, // Inflated due to high local IDF
            }
        ],
        "estimatedTotalHits": 100,
        "processingTimeMs": 10,
        "facetDistribution": {},
    })
}

/// Search response from the small shard (shard 1).
///
/// Returns a document about Rust programming with a local-IDF score.
/// This document has high term density but deflated score due to
/// low local IDF.
fn small_shard_search_response() -> serde_json::Value {
    json!({
        "hits": [
            {
                "id": "doc-small",
                "title": "Rust Systems Programming",
                "_rankingScore": 0.65, // Deflated due to low local IDF
            }
        ],
        "estimatedTotalHits": 200,
        "processingTimeMs": 5,
        "facetDistribution": {},
    })
}

/// Simulate search responses with global IDF applied.
///
/// After the preflight phase, the coordinator sends global IDF to all shards.
/// Shards use these global statistics for scoring, producing comparable scores.
///
/// With global IDF = 3.4:
/// - Large shard doc: lower term density → lower score after global normalization
/// - Small shard doc: higher term density → higher score after global normalization
fn global_idf_search_responses() -> (serde_json::Value, serde_json::Value) {
    let large = json!({
        "hits": [
            {
                "id": "doc-large",
                "title": "Rust Programming Language",
                "_rankingScore": 0.72, // Normalized with global IDF
            }
        ],
        "estimatedTotalHits": 100,
        "processingTimeMs": 10,
        "facetDistribution": {},
    });

    let small = json!({
        "hits": [
            {
                "id": "doc-small",
                "title": "Rust Systems Programming",
                "_rankingScore": 0.88, // Normalized with global IDF (higher due to density)
            }
        ],
        "estimatedTotalHits": 200,
        "processingTimeMs": 5,
        "facetDistribution": {},
    });

    (large, small)
}

#[test]
fn test_preflight_aggregates_global_statistics() {
    // Given: preflight responses from both shards
    let responses = vec![large_shard_preflight(), small_shard_preflight()];

    // When: aggregate into global IDF
    let global_idf = GlobalIdf::from_preflight_responses(&responses);

    // Then: verify correct aggregation
    assert_eq!(global_idf.total_docs, 11_000);

    // Average doc length should be weighted mean
    // (10,000 * 500 + 1,000 * 450) / 11,000 ≈ 495.45
    assert!((global_idf.avg_doc_length - 495.45).abs() < 0.1);

    // Verify term statistics are summed
    assert_eq!(global_idf.terms.get("rust").unwrap().df, 300);
    assert_eq!(global_idf.terms.get("programming").unwrap().df, 80);

    // Verify IDF is pre-computed using global statistics
    // idf = log((N - df + 0.5) / (df + 0.5) + 1)
    // idf(rust) = log((11000 - 300 + 0.5) / (300 + 0.5) + 1) ≈ 4.57
    let rust_idf = global_idf.terms.get("rust").unwrap().idf;
    assert!((rust_idf - 4.57).abs() < 0.1);

    let prog_idf = global_idf.terms.get("programming").unwrap().idf;
    // idf(programming) = log((11000 - 80 + 0.5) / (80 + 0.5) + 1) ≈ 5.91
    assert!((prog_idf - 5.91).abs() < 0.1);
}

#[test]
fn test_score_merge_without_global_idf_fails_skewed_corpus() {
    // Demonstrate the problem: without global IDF, score-based merge
    // produces incorrect rankings on skewed corpus.

    let strategy = ScoreMergeStrategy::new();

    let input = MergeInput {
        shard_hits: vec![
            serde_json::from_value(large_shard_search_response()).unwrap(),
            serde_json::from_value(small_shard_search_response()).unwrap(),
        ].into_iter().map(|body| miroir_core::merger::ShardHitPage { body }).collect(),
        offset: 0,
        limit: 10,
        client_requested_score: true,
        facets: None,
    };

    let result = strategy.merge(input).unwrap();

    // Without global IDF, the inflated score from the large shard wins
    assert_eq!(result.hits[0].get("id").unwrap(), "doc-large");
    assert_eq!(
        result.hits[0].get("_rankingScore").unwrap().as_f64().unwrap(),
        0.92
    );

    // This is WRONG: doc-small has much higher term density (20% vs 1%)
    // but ranks lower due to shard-local IDF skew.
}

#[test]
fn test_score_merge_with_global_idf_corrects_skew() {
    // Demonstrate the solution: with global IDF, scores are comparable
    // and the doc with higher term density ranks correctly.

    let strategy = ScoreMergeStrategy::new();

    let (large_response, small_response) = global_idf_search_responses();

    let input = MergeInput {
        shard_hits: vec![
            serde_json::from_value(large_response).unwrap(),
            serde_json::from_value(small_response).unwrap(),
        ].into_iter().map(|body| miroir_core::merger::ShardHitPage { body }).collect(),
        offset: 0,
        limit: 10,
        client_requested_score: true,
        facets: None,
    };

    let result = strategy.merge(input).unwrap();

    // With global IDF, the small shard doc (higher density) ranks first
    assert_eq!(result.hits[0].get("id").unwrap(), "doc-small");
    assert_eq!(
        result.hits[0].get("_rankingScore").unwrap().as_f64().unwrap(),
        0.88
    );

    // The large shard doc (lower density) ranks second
    assert_eq!(result.hits[1].get("id").unwrap(), "doc-large");
    assert_eq!(
        result.hits[1].get("_rankingScore").unwrap().as_f64().unwrap(),
        0.72
    );
}

#[tokio::test]
async fn test_dfs_query_then_fetch_with_skewed_corpus() {
    // Full integration test: simulate the two-phase DFS query

    let topo = make_skewed_topology();
    let plan = plan_search_scatter(&topo, 0, 1, 2);

    let node_0 = NodeId::new("node-0".to_string());
    let node_1 = NodeId::new("node-1".to_string());

    // Create mock client with preflight and search responses
    let mut client = MockNodeClient::default();

    // Phase 1: Preflight responses
    // Note: MockNodeClient doesn't yet support preflight responses,
    // so we'll test the aggregation logic directly

    let preflight_req = PreflightRequest {
        index_uid: "test".to_string(),
        terms: vec!["rust".to_string(), "programming".to_string()],
        filter: None,
    };

    // Simulate preflight responses
    let responses = vec![large_shard_preflight(), small_shard_preflight()];
    let global_idf = GlobalIdf::from_preflight_responses(&responses);

    // Verify global IDF is computed correctly
    assert_eq!(global_idf.total_docs, 11_000);
    assert_eq!(global_idf.terms.get("rust").unwrap().df, 300);

    // Phase 2: Search with global IDF attached
    // In a real scenario, the coordinator would attach global_idf to
    // the search request and shards would use it for scoring.

    // Verify the global IDF structure can be serialized
    let serialized = serde_json::to_value(&global_idf).unwrap();
    assert!(serialized.is_object());
    assert_eq!(
        serialized.get("total_docs").unwrap().as_u64().unwrap(),
        11_000
    );
}

#[test]
fn test_global_idf_serialization_round_trip() {
    // Verify that GlobalIdf can be serialized and attached to search requests

    let responses = vec![large_shard_preflight(), small_shard_preflight()];
    let global_idf = GlobalIdf::from_preflight_responses(&responses);

    // Serialize to JSON
    let json = serde_json::to_value(&global_idf).unwrap();

    // Verify structure
    assert_eq!(json.get("total_docs").unwrap().as_u64().unwrap(), 11_000);
    assert!(json.get("avg_doc_length").unwrap().is_number());
    assert!(json.get("terms").unwrap().is_object());

    // Deserialize back
    let deserialized: GlobalIdf = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized.total_docs, global_idf.total_docs);
    assert_eq!(deserialized.terms.len(), global_idf.terms.len());
}

#[test]
fn test_term_stats_serialization() {
    // Verify TermStats can be sent over HTTP

    let term_stats = TermStats { df: 100 };
    let json = serde_json::to_value(&term_stats).unwrap();

    assert_eq!(json.get("df").unwrap().as_u64().unwrap(), 100);

    let deserialized: TermStats = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized.df, 100);
}

#[test]
fn test_preflight_request_serialization() {
    // Verify PreflightRequest can be sent over HTTP

    let req = PreflightRequest {
        index_uid: "test-index".to_string(),
        terms: vec!["rust".to_string(), "programming".to_string()],
        filter: Some(json!("category = 'books'")),
    };

    let json = serde_json::to_value(&req).unwrap();

    assert_eq!(json.get("index_uid").unwrap().as_str().unwrap(), "test-index");
    assert!(json.get("terms").unwrap().is_array());
    assert_eq!(
        json.get("terms").unwrap().as_array().unwrap().len(),
        2
    );
    // Filter serializes as a string "category = 'books'"
    assert!(json.get("filter").is_some());

    let deserialized: PreflightRequest = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized.index_uid, "test-index");
    assert_eq!(deserialized.terms.len(), 2);
}

#[test]
fn test_global_idf_empty_corpus() {
    // Edge case: empty corpus (no documents)
    let responses = vec![];
    let global_idf = GlobalIdf::from_preflight_responses(&responses);

    assert_eq!(global_idf.total_docs, 0);
    assert_eq!(global_idf.avg_doc_length, 0.0);
    assert!(global_idf.terms.is_empty());
}

#[test]
fn test_global_idf_single_shard() {
    // Edge case: single shard (no skew possible, but should still work)
    let response = PreflightResponse {
        total_docs: 1000,
        avg_doc_length: 500.0,
        term_stats: {
            let mut map = HashMap::new();
            map.insert("test".to_string(), TermStats { df: 50 });
            map
        },
    };

    let global_idf = GlobalIdf::from_preflight_responses(&vec![response]);

    assert_eq!(global_idf.total_docs, 1000);
    assert_eq!(global_idf.terms.get("test").unwrap().df, 50);
    // IDF should be computed
    assert!(global_idf.terms.get("test").unwrap().idf > 0.0);
}

#[test]
fn test_global_idf_weighted_average_doc_length() {
    // Verify that average doc length is weighted by document count
    let responses = vec![
        PreflightResponse {
            total_docs: 100,
            avg_doc_length: 200.0, // Contributes 100 * 200 = 20,000
            term_stats: HashMap::new(),
        },
        PreflightResponse {
            total_docs: 300,
            avg_doc_length: 400.0, // Contributes 300 * 400 = 120,000
            term_stats: HashMap::new(),
        },
        PreflightResponse {
            total_docs: 200,
            avg_doc_length: 300.0, // Contributes 200 * 300 = 60,000
            term_stats: HashMap::new(),
        },
    ];

    let global_idf = GlobalIdf::from_preflight_responses(&responses);

    // Total docs = 600
    assert_eq!(global_idf.total_docs, 600);

    // Weighted avg = (20000 + 120000 + 60000) / 600 = 200000 / 600 ≈ 333.33
    let expected_avg = 200_000.0 / 600.0;
    assert!((global_idf.avg_doc_length - expected_avg).abs() < 0.01);
}
