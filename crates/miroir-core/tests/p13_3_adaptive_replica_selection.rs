//! P5.3 §13.3 Adaptive replica selection (EWMA-based) acceptance tests.
//!
//! Tests the EWMA-scored replica selection that replaces round-robin:
//! - score(node) = α · latency_p95_ms + β · in_flight_count + γ · error_rate
//! - Router picks lowest-scoring eligible node with probability 1 − ε
//! - With ε (default 0.05) picks uniformly random for exploration

use miroir_core::replica_selection::{ReplicaSelectionConfig, ReplicaSelector};
use miroir_core::topology::{Group, NodeId};
use std::time::Duration;

/// Create a test selector with default config.
fn test_selector() -> ReplicaSelector {
    ReplicaSelector::new(ReplicaSelectionConfig::default())
}

/// Create a selector with custom half-life for faster test convergence.
fn fast_test_selector(half_life_ms: u64) -> ReplicaSelector {
    ReplicaSelector::new(ReplicaSelectionConfig {
        ewma_half_life_ms: half_life_ms,
        exploration_epsilon: 0.05,
        ..Default::default()
    })
}

/// Create a test group with 3 nodes.
fn test_group_3() -> Group {
    let mut group = Group::new(0);
    group.add_node(NodeId::new("node-0".to_string()));
    group.add_node(NodeId::new("node-1".to_string()));
    group.add_node(NodeId::new("node-2".to_string()));
    group
}

/// Simulate queries to measure node selection distribution.
async fn simulate_selections(
    selector: &ReplicaSelector,
    candidates: &[NodeId],
    group_id: u32,
    count: usize,
) -> std::collections::HashMap<String, usize> {
    let mut distribution = std::collections::HashMap::new();
    for _ in 0..count {
        if let Some(node) = selector.select(candidates, group_id).await {
            *distribution.entry(node.as_str().to_string()).or_insert(0) += 1;
        }
    }
    distribution
}

// ─────────────────────────────────────────────────────────────
// P5.3-A1: Traffic shifts away from degraded node within 2× half-life
// ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn p5_3_a1_degraded_node_receives_less_traffic() {
    let selector = fast_test_selector(100); // 100ms half-life for fast test
    let group = test_group_3();
    let candidates: Vec<NodeId> = group.nodes().to_vec();

    let node0 = &candidates[0];
    let node1 = &candidates[1];
    let node2 = &candidates[2];

    // Initialize all nodes with similar good latency (50ms)
    selector.record_success(node0, 50.0).await;
    selector.record_success(node1, 50.0).await;
    selector.record_success(node2, 50.0).await;

    // Run initial queries to establish baseline
    let baseline = simulate_selections(&selector, &candidates, 0, 200).await;

    // Verify roughly uniform distribution initially
    let count0 = *baseline.get("node-0").unwrap_or(&0);
    let count1 = *baseline.get("node-1").unwrap_or(&0);
    let count2 = *baseline.get("node-2").unwrap_or(&0);

    // Each node should get ~33% (allow 20-45% for randomness)
    let _expected = 200 / 3;
    assert!(
        (20..=90).contains(&count0),
        "node-0 baseline count {} out of expected range 20-90",
        count0
    );
    assert!(
        (20..=90).contains(&count1),
        "node-1 baseline count {} out of expected range 20-90",
        count1
    );
    assert!(
        (20..=90).contains(&count2),
        "node-2 baseline count {} out of expected range 20-90",
        count2
    );

    // Induce degradation on node-1: 200ms latency
    for _ in 0..10 {
        selector.record_success(node1, 200.0).await;
    }

    // Wait for EWMA to propagate (2× half-life = 200ms)
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Run queries after degradation
    let degraded = simulate_selections(&selector, &candidates, 0, 200).await;

    let degraded_count1 = *degraded.get("node-1").unwrap_or(&0);
    let degraded_count0 = *degraded.get("node-0").unwrap_or(&0);
    let degraded_count2 = *degraded.get("node-2").unwrap_or(&0);

    // Node-1 should receive significantly less traffic
    // With latency_weight=1.0, node-1 score ~200 vs ~50 for others
    // Expect node-1 to get <15% of traffic
    assert!(
        degraded_count1 < 30,
        "degraded node-1 still receiving too much traffic: {}",
        degraded_count1
    );

    // Healthy nodes should receive more traffic
    assert!(
        degraded_count0 > 50,
        "healthy node-0 not receiving enough traffic: {}",
        degraded_count0
    );
    assert!(
        degraded_count2 > 50,
        "healthy node-2 not receiving enough traffic: {}",
        degraded_count2
    );
}

// ─────────────────────────────────────────────────────────────
// P5.3-A2: Node recovers after latency clears; distribution returns to uniform
// ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn p5_3_a2_degraded_node_recovers() {
    let selector = fast_test_selector(100); // 100ms half-life
    let group = test_group_3();
    let candidates: Vec<NodeId> = group.nodes().to_vec();

    let node0 = &candidates[0];
    let node1 = &candidates[1];
    let node2 = &candidates[2];

    // Initialize with good latency
    selector.record_success(node0, 50.0).await;
    selector.record_success(node1, 50.0).await;
    selector.record_success(node2, 50.0).await;

    // Degrade node-1 with 200ms latency
    for _ in 0..10 {
        selector.record_success(node1, 200.0).await;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify degradation worked
    let degraded_dist = simulate_selections(&selector, &candidates, 0, 100).await;
    let degraded_count1 = *degraded_dist.get("node-1").unwrap_or(&0);
    assert!(
        degraded_count1 < 20,
        "node-1 should be degraded, got {} selections",
        degraded_count1
    );

    // Clear latency: record good responses for node-1
    for _ in 0..10 {
        selector.record_success(node1, 50.0).await;
    }

    // Wait for recovery (2× half-life)
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Run queries after recovery
    let recovered = simulate_selections(&selector, &candidates, 0, 300).await;

    let recovered_count0 = *recovered.get("node-0").unwrap_or(&0);
    let recovered_count1 = *recovered.get("node-1").unwrap_or(&0);
    let recovered_count2 = *recovered.get("node-2").unwrap_or(&0);

    // All nodes should have roughly equal distribution (~1/3 each)
    let expected = 300 / 3;
    let tolerance = (expected as f64 * 0.4) as usize; // 40% tolerance for exploration

    assert!(
        (recovered_count1 as isize - expected as isize).abs() <= tolerance as isize,
        "node-1 recovered count {} not close to expected {} (tolerance {})",
        recovered_count1, expected, tolerance
    );
    assert!(
        (recovered_count0 as isize - expected as isize).abs() <= tolerance as isize,
        "node-0 count {} not close to expected {} (tolerance {})",
        recovered_count0, expected, tolerance
    );
    assert!(
        (recovered_count2 as isize - expected as isize).abs() <= tolerance as isize,
        "node-2 count {} not close to expected {} (tolerance {})",
        recovered_count2, expected, tolerance
    );
}

// ─────────────────────────────────────────────────────────────
// P5.3-A3: Exploration ensures degraded node still gets some traffic (ε = 0.05)
//
// With epsilon=0.05 and 3 nodes, degraded node gets ~1.7% (5%/3) of traffic
// via random exploration. This ensures continued monitoring for recovery.
// ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn p5_3_a3_exploration_samples_degraded_node() {
    let selector = ReplicaSelector::new(ReplicaSelectionConfig {
        ewma_half_life_ms: 100,
        exploration_epsilon: 0.05, // 5% exploration
        ..Default::default()
    });

    let group = test_group_3();
    let candidates: Vec<NodeId> = group.nodes().to_vec();

    let node0 = &candidates[0];
    let node1 = &candidates[1];
    let node2 = &candidates[2];

    // Initialize: node-0 and node-1 healthy, node-2 severely degraded
    for _ in 0..10 {
        selector.record_success(node0, 50.0).await;
        selector.record_success(node1, 50.0).await;
        selector.record_success(node2, 500.0).await; // Very high latency
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Run 1000 queries
    let dist = simulate_selections(&selector, &candidates, 0, 1000).await;

    let count0 = *dist.get("node-0").unwrap_or(&0);
    let count1 = *dist.get("node-1").unwrap_or(&0);
    let count2 = *dist.get("node-2").unwrap_or(&0);

    println!("Distribution: node-0={}, node-1={}, node-2={}", count0, count1, count2);

    // Node-2 is severely degraded but should still get some traffic via exploration
    // With 5% epsilon and random exploration across 3 nodes:
    // - 5% of 1000 = 50 explorations total
    // - 1/3 of explorations go to node-2 ≈ 17 selections
    // Allow range 5-30 for statistical variance (3 sigma)
    assert!(
        (5..=30).contains(&count2),
        "exploration not working: degraded node-2 got {} selections, expected ~17 (range 5-30)",
        count2
    );

    // Healthy nodes should split the remaining ~95%
    let healthy_total = count0 + count1;
    assert!(
        healthy_total >= 900,
        "healthy nodes didn't get enough traffic: {}",
        healthy_total
    );

    // Each healthy node should get roughly half of remaining
    let expected_healthy = 475; // ~95% / 2
    let tolerance = 100;
    assert!(
        (count0 as isize - expected_healthy).abs() <= tolerance,
        "node-0 count {} not close to expected {}",
        count0, expected_healthy
    );
    assert!(
        (count1 as isize - expected_healthy).abs() <= tolerance,
        "node-1 count {} not close to expected {}",
        count1, expected_healthy
    );
}

// ─────────────────────────────────────────────────────────────
// P5.3-A4: Round-robin fallback mode works identically to Phase 1 baseline
// ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn p5_3_a4_round_robin_fallback() {
    let selector = ReplicaSelector::new(ReplicaSelectionConfig {
        strategy: "round_robin".to_string(),
        ewma_half_life_ms: 5000,
        ..Default::default()
    });

    let group = test_group_3();
    let candidates: Vec<NodeId> = group.nodes().to_vec();

    // Test round-robin order
    let first = selector.select(&candidates, 0).await;
    let second = selector.select(&candidates, 0).await;
    let third = selector.select(&candidates, 0).await;
    let fourth = selector.select(&candidates, 0).await;

    // Should cycle through candidates in order
    assert_eq!(first, candidates.get(0).cloned());
    assert_eq!(second, candidates.get(1).cloned());
    assert_eq!(third, candidates.get(2).cloned());
    assert_eq!(fourth, candidates.get(0).cloned()); // Wrap around
}

// ─────────────────────────────────────────────────────────────
// Additional tests for EWMA behavior
// ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn ewma_converges_toward_new_values() {
    let selector = fast_test_selector(50);
    let group = test_group_3();
    let node = &group.nodes()[0];

    // Start with 100ms latency
    selector.record_success(node, 100.0).await;
    let metrics1 = selector.get_metrics(node).await;
    assert!(metrics1.is_some());
    let latency1 = metrics1.unwrap().latency_p95_ms;
    assert!((90.0..=110.0).contains(&latency1));

    // Shift to 50ms
    for _ in 0..5 {
        selector.record_success(node, 50.0).await;
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let metrics2 = selector.get_metrics(node).await;
    let latency2 = metrics2.unwrap().latency_p95_ms;

    // Should move toward 50
    assert!(latency2 < latency1, "EWMA should decrease toward new value");
    assert!(latency2 > 40.0, "EWMA shouldn't overshoot too much");
}

#[tokio::test]
async fn in_flight_count_affects_score() {
    let selector = test_selector();
    let group = test_group_3();
    let candidates: Vec<NodeId> = group.nodes().to_vec();

    let node0 = &candidates[0];
    let node1 = &candidates[1];

    // Both have same latency initially
    selector.record_success(node0, 50.0).await;
    selector.record_success(node1, 50.0).await;

    // Add in-flight requests to node-0
    for _ in 0..5 {
        selector.record_request_start(node0).await;
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // With inflight_weight=2.0, node-0 score should be higher
    let metrics0 = selector.get_metrics(node0).await.unwrap();
    let metrics1 = selector.get_metrics(node1).await.unwrap();

    let config = ReplicaSelectionConfig::default();
    let score0 = metrics0.score(&config);
    let score1 = metrics1.score(&config);

    assert!(
        score0 > score1,
        "node-0 with in-flight requests should have higher score: {} > {}",
        score0,
        score1
    );
}

#[tokio::test]
async fn error_rate_affects_score() {
    let selector = test_selector();
    let group = test_group_3();
    let candidates: Vec<NodeId> = group.nodes().to_vec();

    let node0 = &candidates[0];
    let node1 = &candidates[1];

    // Both have same latency initially
    selector.record_success(node0, 50.0).await;
    selector.record_success(node1, 50.0).await;

    // Record errors for node-0
    for _ in 0..5 {
        selector.record_error(node0, Some(50.0)).await;
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // With error_weight=10.0, node-0 score should be higher
    let metrics0 = selector.get_metrics(node0).await.unwrap();
    let metrics1 = selector.get_metrics(node1).await.unwrap();

    let config = ReplicaSelectionConfig::default();
    let score0 = metrics0.score(&config);
    let score1 = metrics1.score(&config);

    assert!(
        score0 > score1,
        "node-0 with errors should have higher score: {} > {}",
        score0,
        score1
    );

    // Verify error_rate is set
    assert!(metrics0.error_rate > 0.0);
    assert_eq!(metrics1.error_rate, 0.0);
}

#[tokio::test]
async fn test_exploration_epsilon_configurable() {
    // Test with epsilon = 0.0 (no exploration)
    let selector_no_explore = ReplicaSelector::new(ReplicaSelectionConfig {
        exploration_epsilon: 0.0,
        ewma_half_life_ms: 100,
        ..Default::default()
    });

    let group = test_group_3();
    let candidates: Vec<NodeId> = group.nodes().to_vec();

    let node0 = &candidates[0];
    let node1 = &candidates[1];
    let node2 = &candidates[2];

    // Make node-2 severely degraded
    selector_no_explore.record_success(node0, 50.0).await;
    selector_no_explore.record_success(node1, 50.0).await;
    selector_no_explore.record_success(node2, 500.0).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // With no exploration, node-2 should get almost no traffic
    let dist = simulate_selections(&selector_no_explore, &candidates, 0, 200).await;
    let count2 = *dist.get("node-2").unwrap_or(&0);

    assert_eq!(
        count2, 0,
        "with epsilon=0.0, degraded node should get zero traffic"
    );
}

#[tokio::test]
async fn test_random_strategy() {
    let selector = ReplicaSelector::new(ReplicaSelectionConfig {
        strategy: "random".to_string(),
        ..Default::default()
    });

    let group = test_group_3();
    let candidates: Vec<NodeId> = group.nodes().to_vec();

    // Run many selections to test uniformity
    let mut counts = std::collections::HashMap::new();
    for _ in 0..1000 {
        if let Some(node) = selector.select(&candidates, 0).await {
            *counts.entry(node.as_str().to_string()).or_insert(0) += 1;
        }
    }

    // Each node should get roughly 1/3 of selections
    let expected = 1000 / 3;
    for (node, count) in &counts {
        let diff = (*count as isize - expected as isize).abs();
        assert!(
            diff <= 100, // Allow 10% variance
            "{}: got {} selections, expected ~{}",
            node, count, expected
        );
    }
}
