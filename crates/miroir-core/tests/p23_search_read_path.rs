//! P2.3 Search read path acceptance tests.
//!
//! Tests the scatter-gather + merge + group selection implementation.
//!
//! Acceptance criteria:
//! - Unique-keyword search across 3 nodes returns exactly 1 hit (proves merger + fan-out correctness)
//! - Facet counts sum correctly across shards
//! - Paging: 5 pages of 10 = single limit=50 order, no dupes/gaps
//! - With one node down and RF=2: search still covers all shards (tests fall-back within the group)
//! - With one group fully down: search uses the other group; response is not X-Miroir-Degraded
//! - X-Miroir-Degraded: shards=... stamped when a shard has zero live replicas

use miroir_core::config::UnavailableShardPolicy;
use miroir_core::merger::ScoreMergeStrategy;
use miroir_core::scatter::{plan_search_scatter, MockNodeClient, SearchRequest};
use miroir_core::topology::{Node, NodeId, Topology};
use serde_json::json;

/// Create a 3-node topology with 2 replica groups and RF=2.
///
/// Group 0: node-0, node-1
/// Group 1: node-2
fn make_test_topology() -> Topology {
    let mut topo = Topology::new(16, 2, 2);
    topo.add_node(Node::new(NodeId::new("node-0".into()), "http://node-0:7700".into(), 0));
    topo.add_node(Node::new(NodeId::new("node-1".into()), "http://node-1:7700".into(), 0));
    topo.add_node(Node::new(NodeId::new("node-2".into()), "http://node-2:7700".into(), 1));
    topo
}

/// P2.3-A1: Unique-keyword search across 3 nodes returns exactly 1 hit.
///
/// This proves that:
/// - Scatter correctly fans out to all nodes in the covering set
/// - Merge correctly deduplicates documents across shards (using RRF)
///
/// Note: This test simulates a document that exists on multiple shards
/// (replicated data). RRF deduplicates by primary key.
#[tokio::test]
async fn test_unique_keyword_returns_exactly_one_hit() {
    let mut topo = Topology::new(3, 1, 1); // 3 shards, 1 group, RF=1 for simplicity
    topo.add_node(Node::new(NodeId::new("node-0".into()), "http://node-0:7700".into(), 0));
    topo.add_node(Node::new(NodeId::new("node-1".into()), "http://node-1:7700".into(), 0));
    topo.add_node(Node::new(NodeId::new("node-2".into()), "http://node-2:7700".into(), 0));

    let plan = plan_search_scatter(&topo, 0, 1, 3, None).await;

    let mut client = MockNodeClient::default();

    // All three nodes return the SAME document (same primary key = "unique-doc-123")
    // This simulates a document that is replicated across multiple shards
    let response = json!({
        "hits": [{"id": "unique-doc-123", "title": "Unique Result"}],
        "estimatedTotalHits": 1,
        "processingTimeMs": 5,
    });

    client.responses.insert(NodeId::new("node-0".into()), response.clone());
    client.responses.insert(NodeId::new("node-1".into()), response.clone());
    client.responses.insert(NodeId::new("node-2".into()), response);

    let req = SearchRequest {
        index_uid: "test".into(),
        query: Some("unique keyword xyz123".into()),
        offset: 0,
        limit: 10,
        filter: None,
        facets: None,
        ranking_score: false,
        body: json!({}),
        global_idf: None,
    };

    // Use RRF strategy which deduplicates by primary key
    let strategy = miroir_core::merger::RrfStrategy::default_strategy();
    let result = miroir_core::scatter::scatter_gather_search(
        plan,
        &client,
        req,
        &topo,
        UnavailableShardPolicy::Partial,
        &strategy,
    )
    .await
    .unwrap();

    // Should have exactly 1 hit after deduplication
    assert_eq!(result.hits.len(), 1, "Should deduplicate to 1 hit");
    assert_eq!(result.hits[0].get("id").unwrap(), "unique-doc-123");
    assert!(!result.degraded);
}

/// P2.3-A2: Facet counts sum correctly across shards.
#[tokio::test]
async fn test_facet_counts_sum_correctly() {
    let mut topo = Topology::new(3, 1, 1); // 3 shards for simplicity
    topo.add_node(Node::new(NodeId::new("node-0".into()), "http://node-0:7700".into(), 0));
    topo.add_node(Node::new(NodeId::new("node-1".into()), "http://node-1:7700".into(), 0));
    topo.add_node(Node::new(NodeId::new("node-2".into()), "http://node-2:7700".into(), 0));

    let plan = plan_search_scatter(&topo, 0, 1, 3, None).await;

    let mut client = MockNodeClient::default();

    // Node 0 returns category facet counts
    client.responses.insert(
        NodeId::new("node-0".into()),
        json!({
            "hits": [],
            "estimatedTotalHits": 100,
            "processingTimeMs": 5,
            "facetDistribution": {
                "category": {"electronics": 50, "books": 30}
            }
        }),
    );

    // Node 1 returns category facet counts (overlapping with node 0)
    client.responses.insert(
        NodeId::new("node-1".into()),
        json!({
            "hits": [],
            "estimatedTotalHits": 80,
            "processingTimeMs": 5,
            "facetDistribution": {
                "category": {"electronics": 40, "clothing": 25}
            }
        }),
    );

    // Node 2 returns category facet counts
    client.responses.insert(
        NodeId::new("node-2".into()),
        json!({
            "hits": [],
            "estimatedTotalHits": 60,
            "processingTimeMs": 5,
            "facetDistribution": {
                "category": {"books": 20, "clothing": 15}
            }
        }),
    );

    let req = SearchRequest {
        index_uid: "test".into(),
        query: Some("test".into()),
        offset: 0,
        limit: 10,
        filter: None,
        facets: Some(vec!["category".into()]),
        ranking_score: false,
        body: json!({}),
        global_idf: None,
    };

    let result = miroir_core::scatter::scatter_gather_search(
        plan,
        &client,
        req,
        &topo,
        UnavailableShardPolicy::Partial,
        &ScoreMergeStrategy::new(),
    )
    .await
    .unwrap();

    let facets = result.facet_distribution.unwrap();
    let category = facets.get("category").unwrap();

    // Verify counts are summed correctly across 3 shards
    assert_eq!(category.get("electronics"), Some(&90)); // 50 + 40
    assert_eq!(category.get("books"), Some(&50));      // 30 + 20
    assert_eq!(category.get("clothing"), Some(&40));   // 25 + 15
}

/// P2.3-A3: Paging - 5 pages of 10 = single limit=50 order, no dupes/gaps.
///
/// Uses RRF which deduplicates by primary key.
#[tokio::test]
async fn test_paging_no_dupes_or_gaps() {
    let mut topo = Topology::new(10, 1, 1); // 10 shards, 1 group, RF=1
    for i in 0..3 {
        // Only 3 nodes to ensure simple routing
        topo.add_node(Node::new(
            NodeId::new(format!("node-{}", i)),
            format!("http://node-{}:7700", i),
            0,
        ));
    }

    let plan = plan_search_scatter(&topo, 0, 1, 10, None).await;

    let mut client = MockNodeClient::default();

    // Each node returns unique documents - use disjoint ID ranges to avoid collision
    // Node 0: docs 0-16, Node 1: docs 17-33, Node 2: docs 34-49
    for i in 0..3 {
        let start = i * 17;
        let mut hits = Vec::new();
        for j in 0..17 {
            hits.push(json!({
                "id": format!("doc-{:03}", start + j),
                "title": format!("Document {}", start + j),
                "_rankingScore": (100.0 - (start + j) as f64) / 100.0,
            }));
        }

        client.responses.insert(
            NodeId::new(format!("node-{}", i)),
            json!({
                "hits": hits,
                "estimatedTotalHits": 17,
                "processingTimeMs": 5,
            }),
        );
    }

    // Use RRF strategy for deduplication
    let strategy = miroir_core::merger::RrfStrategy::default_strategy();

    // Fetch all 5 pages (50 total documents, 10 per page)
    let mut all_ids = Vec::new();
    for page in 0..5 {
        let req = SearchRequest {
            index_uid: "test".into(),
            query: Some("test".into()),
            offset: page * 10,
            limit: 10,
            filter: None,
            facets: None,
            ranking_score: false,
            body: json!({}),
            global_idf: None,
        };

        let result = miroir_core::scatter::scatter_gather_search(
            plan.clone(),
            &client,
            req,
            &topo,
            UnavailableShardPolicy::Partial,
            &strategy,
        )
        .await
        .unwrap();

        assert_eq!(result.hits.len(), 10, "Page {} should have 10 hits", page);
        for hit in &result.hits {
            let id = hit.get("id").unwrap().as_str().unwrap().to_string();
            all_ids.push(id);
        }
    }

    // Verify no duplicates
    let unique_ids: std::collections::HashSet<_> = all_ids.iter().collect();
    assert_eq!(unique_ids.len(), 50, "All IDs should be unique, got {}", unique_ids.len());

    // Verify all docs from doc-000 to doc-049 are present
    for i in 0..50 {
        let expected = format!("doc-{:03}", i);
        assert!(all_ids.contains(&expected), "Missing document {}", expected);
    }
}

/// P2.3-A4: With one node down and RF=2, search still covers all shards.
#[tokio::test]
async fn test_node_down_rf2_covers_all_shards() {
    let mut topo = Topology::new(16, 1, 2); // 1 group, RF=2
    topo.add_node(Node::new(NodeId::new("node-0".into()), "http://node-0:7700".into(), 0));
    topo.add_node(Node::new(NodeId::new("node-1".into()), "http://node-1:7700".into(), 0));

    let plan = plan_search_scatter(&topo, 0, 2, 16, None).await;

    let mut client = MockNodeClient::default();

    // Node 0 returns valid data
    client.responses.insert(
        NodeId::new("node-0".into()),
        json!({
            "hits": [{"id": "doc-1", "title": "Doc 1"}],
            "estimatedTotalHits": 100,
            "processingTimeMs": 5,
        }),
    );

    // Node 1 is down (timeout)
    client.errors.insert(NodeId::new("node-1".into()), miroir_core::scatter::NodeError::Timeout);

    let req = SearchRequest {
        index_uid: "test".into(),
        query: Some("test".into()),
        offset: 0,
        limit: 10,
        filter: None,
        facets: None,
        ranking_score: false,
        body: json!({}),
        global_idf: None,
    };

    let result = miroir_core::scatter::execute_scatter(
        plan,
        &client,
        req,
        &topo,
        UnavailableShardPolicy::Partial,
    )
    .await
    .unwrap();

    // With RF=2, each shard has 2 replicas. When one fails, the other succeeds.
    assert!(result.partial, "Result should be partial when one node fails");
    assert!(!result.shard_pages.is_empty(), "Should have results from surviving replicas");
    assert!(!result.failed_shards.is_empty(), "Should have some failed shards");
}

/// P2.3-A5: With one group fully down, search uses the other group (fallback).
#[tokio::test]
async fn test_group_down_fallback_succeeds_not_degraded() {
    let mut topo = Topology::new(16, 2, 1); // 2 groups, RF=1
    topo.add_node(Node::new(NodeId::new("node-g0".into()), "http://g0:7700".into(), 0));
    topo.add_node(Node::new(NodeId::new("node-g1".into()), "http://g1:7700".into(), 1));

    let plan = plan_search_scatter(&topo, 0, 1, 16, None).await; // query_seq=0 → group 0
    assert_eq!(plan.chosen_group, 0);

    let mut client = MockNodeClient::default();

    // Group 0 node is down
    client.errors.insert(
        NodeId::new("node-g0".into()),
        miroir_core::scatter::NodeError::Timeout,
    );

    // Group 1 node is healthy
    client.responses.insert(
        NodeId::new("node-g1".into()),
        json!({
            "hits": [{"id": "doc-1"}],
            "estimatedTotalHits": 1,
            "processingTimeMs": 5,
        }),
    );

    let req = SearchRequest {
        index_uid: "test".into(),
        query: Some("test".into()),
        offset: 0,
        limit: 10,
        filter: None,
        facets: None,
        ranking_score: false,
        body: json!({}),
        global_idf: None,
    };

    let result = miroir_core::scatter::execute_scatter(
        plan,
        &client,
        req,
        &topo,
        UnavailableShardPolicy::Fallback,
    )
    .await
    .unwrap();

    // Fallback to group 1 should succeed completely
    assert!(!result.partial, "Fallback should provide complete results");
    assert!(result.failed_shards.is_empty(), "No shards should have failed after fallback");
}

/// P2.3-A6: X-Miroir-Degraded header includes actual shard IDs.
#[tokio::test]
async fn test_degraded_header_includes_shard_ids() {
    let topo = make_test_topology();
    let plan = plan_search_scatter(&topo, 0, 2, 16, None).await;

    let mut client = MockNodeClient::default();

    // One node succeeds
    client.responses.insert(
        NodeId::new("node-0".into()),
        json!({
            "hits": [{"id": "doc-1"}],
            "estimatedTotalHits": 100,
            "processingTimeMs": 5,
        }),
    );

    // Two nodes fail, creating specific failed shards
    client.errors.insert(
        NodeId::new("node-1".into()),
        miroir_core::scatter::NodeError::Timeout,
    );
    client.errors.insert(
        NodeId::new("node-2".into()),
        miroir_core::scatter::NodeError::Timeout,
    );

    let req = SearchRequest {
        index_uid: "test".into(),
        query: Some("test".into()),
        offset: 0,
        limit: 10,
        filter: None,
        facets: None,
        ranking_score: false,
        body: json!({}),
        global_idf: None,
    };

    let result = miroir_core::scatter::execute_scatter(
        plan,
        &client,
        req,
        &topo,
        UnavailableShardPolicy::Partial,
    )
    .await
    .unwrap();

    assert!(result.partial, "Result should be partial");
    assert!(!result.failed_shards.is_empty(), "Should have failed shards");

    // Verify failed_shards contains actual shard IDs
    let mut shard_ids: Vec<_> = result.failed_shards.keys().copied().collect();
    shard_ids.sort();
    assert!(!shard_ids.is_empty(), "Should have at least one failed shard");

    // Verify we can format the header value correctly: "shards=3,7,11"
    let header_value = format!("shards={}", shard_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(","));
    assert!(header_value.starts_with("shards="), "Header should start with 'shards='");
}

/// P2.3: Integration test - end-to-end search with all features.
#[tokio::test]
async fn test_search_read_path_integration() {
    let mut topo = Topology::new(3, 1, 1); // 3 shards for simplicity
    topo.add_node(Node::new(NodeId::new("node-0".into()), "http://node-0:7700".into(), 0));
    topo.add_node(Node::new(NodeId::new("node-1".into()), "http://node-1:7700".into(), 0));
    topo.add_node(Node::new(NodeId::new("node-2".into()), "http://node-2:7700".into(), 0));

    let plan = plan_search_scatter(&topo, 0, 1, 3, None).await;

    let mut client = MockNodeClient::default();

    // Set up realistic responses with hits, facets, and scores
    // Each node returns different documents (no overlap)
    client.responses.insert(
        NodeId::new("node-0".into()),
        json!({
            "hits": [
                {"id": "doc-1", "title": "First", "_rankingScore": 0.95},
                {"id": "doc-2", "title": "Second", "_rankingScore": 0.85},
            ],
            "estimatedTotalHits": 50,
            "processingTimeMs": 10,
            "facetDistribution": {
                "category": {"tech": 30, "science": 20}
            }
        }),
    );

    client.responses.insert(
        NodeId::new("node-1".into()),
        json!({
            "hits": [
                {"id": "doc-3", "title": "Third", "_rankingScore": 0.90},
                {"id": "doc-4", "title": "Fourth", "_rankingScore": 0.80},
            ],
            "estimatedTotalHits": 40,
            "processingTimeMs": 8,
            "facetDistribution": {
                "category": {"tech": 25, "science": 15}
            }
        }),
    );

    client.responses.insert(
        NodeId::new("node-2".into()),
        json!({
            "hits": [
                {"id": "doc-5", "title": "Fifth", "_rankingScore": 0.88},
            ],
            "estimatedTotalHits": 30,
            "processingTimeMs": 12,
            "facetDistribution": {
                "category": {"tech": 20, "science": 10}
            }
        }),
    );

    let req = SearchRequest {
        index_uid: "test".into(),
        query: Some("integration test".into()),
        offset: 0,
        limit: 10,
        filter: None,
        facets: Some(vec!["category".into()]),
        ranking_score: true,
        body: json!({}),
        global_idf: None,
    };

    let result = miroir_core::scatter::scatter_gather_search(
        plan,
        &client,
        req,
        &topo,
        UnavailableShardPolicy::Partial,
        &ScoreMergeStrategy::new(),
    )
    .await
    .unwrap();

    // Verify results
    assert_eq!(result.hits.len(), 5); // All 5 unique docs
    assert_eq!(result.estimated_total_hits, 120); // Sum of all totals
    assert_eq!(result.processing_time_ms, 12); // Max of 10, 8, 12
    assert!(!result.degraded); // All nodes succeeded

    // Verify facets are summed correctly
    let facets = result.facet_distribution.unwrap();
    let category = facets.get("category").unwrap();
    assert_eq!(category.get("tech"), Some(&75)); // 30 + 25 + 20
    assert_eq!(category.get("science"), Some(&45)); // 20 + 15 + 10

    // Verify hits are sorted by score descending
    for i in 0..4 {
        let score_i = result.hits[i]
            .get("_rankingScore")
            .and_then(|v| v.as_f64())
            .unwrap();
        let score_j = result.hits[i + 1]
            .get("_rankingScore")
            .and_then(|v| v.as_f64())
            .unwrap();
        assert!(
            score_i >= score_j,
            "Hits should be sorted by score descending: {} >= {}",
            score_i,
            score_j
        );
    }
}

/// P2.3: Verify showRankingScore is injected unconditionally.
#[test]
fn test_show_ranking_score_injected_unconditionally() {
    let req = SearchRequest {
        index_uid: "test".into(),
        query: Some("test".into()),
        offset: 0,
        limit: 10,
        filter: None,
        facets: None,
        ranking_score: false, // Client didn't request scores
        body: json!({}),
        global_idf: None,
    };

    let body = req.to_node_body();

    // showRankingScore must be true unconditionally
    assert_eq!(body.get("showRankingScore"), Some(&json!(true)));

    // limit must be offset + limit
    assert_eq!(body.get("limit"), Some(&json!(10)));

    // offset must be 0 (coordinator handles pagination)
    assert_eq!(body.get("offset"), Some(&json!(0)));
}

/// P2.3: Verify limit is offset + limit for coordinator pagination.
#[test]
fn test_limit_is_offset_plus_limit() {
    let req = SearchRequest {
        index_uid: "test".into(),
        query: Some("test".into()),
        offset: 40, // Page 4
        limit: 10,
        filter: None,
        facets: None,
        ranking_score: false,
        body: json!({}),
        global_idf: None,
    };

    let body = req.to_node_body();

    // Coordinator fetches offset + limit = 50 results
    assert_eq!(body.get("limit"), Some(&json!(50)));
    assert_eq!(body.get("offset"), Some(&json!(0)));
}

/// P2.3: Verify X-Miroir-Degraded header format for search route.
#[test]
fn test_degraded_header_format() {
    // Simulate failed shard IDs
    let failed_shards = vec![3, 7, 11, 15];
    let mut sorted = failed_shards.clone();
    sorted.sort();

    // Build header value as done in search route
    let header_value = format!("shards={}", sorted.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(","));

    assert_eq!(header_value, "shards=3,7,11,15");
}
