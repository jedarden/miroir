//! P7.1 Core metrics families acceptance tests.
//!
//! Verifies that all plan §10 core metrics are properly registered and accessible.

use miroir_core::config::MiroirConfig;
use miroir_proxy::middleware::Metrics;

#[test]
fn test_all_core_metrics_registered() {
    let config = MiroirConfig::default();
    let metrics = Metrics::new(&config);

    // Add sample data to ensure metrics appear in output
    // Request metrics
    metrics.record_request_duration("GET", "/indexes/{uid}/search", 200, 0.05);
    metrics.inc_requests_total("POST", "/indexes/{uid}/documents", 201);

    // Node metrics
    metrics.set_node_healthy("node-1", true);
    metrics.record_node_request_duration("node-1", "search", 0.05);
    metrics.inc_node_errors("node-1", "timeout");

    // Shard metrics - use f64 for values
    metrics.set_shard_coverage(1.0);
    metrics.set_degraded_shards(0.0);
    metrics.set_shard_distribution("node-1", 32.0);

    // Task metrics
    metrics.observe_task_processing_age(0.1);
    metrics.inc_tasks_total("completed");
    metrics.set_task_registry_size(5.0);

    // Scatter-gather metrics
    metrics.record_scatter_fan_out(3);
    metrics.inc_scatter_partial_responses();
    metrics.inc_scatter_retries();

    // Rebalancer metrics
    metrics.set_rebalance_in_progress(false);
    metrics.inc_rebalance_documents_migrated(100);
    metrics.observe_rebalance_duration(10.0);

    let encoded = metrics.encode_metrics().expect("failed to encode metrics");
    let output = encoded.as_str();

    // Verify all 18 core plan §10 metric names appear in the output
    // Check for either HELP, TYPE, or data lines for each metric
    let expected_metrics = [
        // Request metrics (3)
        "miroir_request_duration_seconds",
        "miroir_requests_total",
        "miroir_requests_in_flight",
        // Node health metrics (3)
        "miroir_node_healthy",
        "miroir_node_request_duration_seconds",
        "miroir_node_errors_total",
        // Shard metrics (3)
        "miroir_shard_coverage",
        "miroir_degraded_shards_total",
        "miroir_shard_distribution",
        // Task metrics (3)
        "miroir_task_processing_age_seconds",
        "miroir_tasks_total",
        "miroir_task_registry_size",
        // Scatter-gather metrics (3)
        "miroir_scatter_fan_out_size",
        "miroir_scatter_partial_responses_total",
        "miroir_scatter_retries_total",
        // Rebalancer metrics (3)
        "miroir_rebalance_in_progress",
        "miroir_rebalance_documents_migrated_total",
        "miroir_rebalance_duration_seconds",
    ];

    for name in &expected_metrics {
        // Check for HELP or TYPE line (metadata) OR data line for the metric
        let has_metadata = output.contains(&format!("# HELP {}", name))
            || output.contains(&format!("# TYPE {}", name));
        let has_data = output.lines().any(|line| line.starts_with(name));
        assert!(
            has_metadata || has_data,
            "missing core metric: {} (no HELP/TYPE metadata or data lines found)\nOutput snippet:\n{}",
            name,
            output.lines().take(50).collect::<Vec<_>>().join("\n")
        );
    }
}

#[test]
fn test_scatter_fan_out_metric_records_correctly() {
    let config = MiroirConfig::default();
    let metrics = Metrics::new(&config);

    // Record a scatter operation that hit 3 nodes
    metrics.record_scatter_fan_out(3);

    let encoded = metrics.encode_metrics().expect("failed to encode metrics");
    let output = encoded.as_str();

    // Verify the metric is present
    assert!(
        output.contains("miroir_scatter_fan_out_size"),
        "miroir_scatter_fan_out_size metric not found"
    );

    // Verify the sample value is recorded (histogram should have a count > 0)
    // Check for the _count metric which shows total observations
    assert!(
        output.contains("miroir_scatter_fan_out_size_count")
            && output
                .lines()
                .any(|line| line.contains("miroir_scatter_fan_out_size_count")
                    && !line.contains(" 0")),
        "Expected histogram with non-zero count not found. Output:\n{}",
        output
    );
}

#[test]
fn test_node_health_metrics_have_correct_labels() {
    let config = MiroirConfig::default();
    let metrics = Metrics::new(&config);

    // Set node health for two nodes
    metrics.set_node_healthy("node-1", true);
    metrics.set_node_healthy("node-2", false);

    let encoded = metrics.encode_metrics().expect("failed to encode metrics");
    let output = encoded.as_str();

    // Verify node_id label is present and values are correct
    assert!(
        output.contains("miroir_node_healthy")
            && output.contains("node-1")
            && output.contains(" 1"),
        "Expected node-1 healthy metric not found. Output:\n{}",
        output
    );
    assert!(
        output.contains("miroir_node_healthy")
            && output.contains("node-2")
            && output.contains(" 0"),
        "Expected node-2 unhealthy metric not found. Output:\n{}",
        output
    );
}

#[test]
fn test_node_request_duration_has_operation_label() {
    let config = MiroirConfig::default();
    let metrics = Metrics::new(&config);

    // Record a node request duration
    metrics.record_node_request_duration("node-1", "search", 0.05);

    let encoded = metrics.encode_metrics().expect("failed to encode metrics");
    let output = encoded.as_str();

    // Verify operation label is present (format may vary: "node-1","search" or node_id="node-1",operation="search")
    assert!(
        output.contains("miroir_node_request_duration_seconds") &&
        output.contains("node-1") &&
        output.contains("search"),
        "Expected node request duration metric with node_id and operation labels not found. Output:\n{}",
        output
    );
}

#[test]
fn test_task_metrics_have_status_label() {
    let config = MiroirConfig::default();
    let metrics = Metrics::new(&config);

    // Increment task counters
    metrics.inc_tasks_total("completed");
    metrics.inc_tasks_total("failed");

    let encoded = metrics.encode_metrics().expect("failed to encode metrics");
    let output = encoded.as_str();

    // Verify status label is present
    assert!(
        output.contains("miroir_tasks_total")
            && output.contains("completed")
            && output.contains("failed"),
        "Expected tasks_total metric with status labels not found. Output:\n{}",
        output
    );
}
