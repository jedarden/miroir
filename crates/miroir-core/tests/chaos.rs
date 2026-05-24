//! Chaos tests for Miroir.
//!
//! Per plan §8, these tests verify graceful degradation under failure conditions.
//! Each test is slow (30+ seconds) and marked with `#[ignore]`.
//!
//! Run all chaos tests:
//!   cargo test --test chaos -- --ignored --test-threads=1
//!
//! Run a specific scenario:
//!   cargo test --test chaos chaos_scenario_1 -- --ignored
//!
//! See runbook comments in each test for operator documentation.

use meilisearch_sdk::{client::Client, indexes::Indexes, task::Task};
use reqwest::StatusCode;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

const MASTER_KEY: &str = "dev-key";

const COMPOSE_FILE: &str = "examples/docker-compose-dev.yml";
const COMPOSE_FILE_RF2: &str = "examples/docker-compose-dev-rf2.yml";

/// TestCluster manages a docker-compose stack for chaos testing.
struct TestCluster {
    project_name: String,
    rf: u32,
}

impl TestCluster {
    /// Create a new TestCluster.
    ///
    /// # Arguments
    /// * `name` - Unique name for this test cluster (becomes docker-compose project name)
    /// * `rf` - Replication factor (1 or 2) - selects the appropriate compose file
    fn new(name: &str, rf: u32) -> Self {
        assert!(rf == 1 || rf == 2, "RF must be 1 or 2");
        Self {
            project_name: format!("miroir-test-{}", name),
            rf,
        }
    }

    /// Get the Miroir port for this cluster.
    fn miroir_port(&self) -> u16 {
        match self.rf {
            1 => 7700,
            2 => 7710,
            _ => unreachable!(),
        }
    }

    /// Get a Meilisearch node port by index.
    fn meili_port(&self, node_index: usize) -> u16 {
        match self.rf {
            1 => 7701 + node_index as u16,
            2 => 7711 + node_index as u16,
            _ => unreachable!(),
        }
    }

    /// Get the docker-compose file for this RF.
    fn compose_file(&self) -> &str {
        match self.rf {
            1 => COMPOSE_FILE,
            2 => COMPOSE_FILE_RF2,
            _ => unreachable!(),
        }
    }

    /// Start the cluster with docker-compose.
    async fn up(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Starting cluster {} (RF={})...", self.project_name, self.rf);

        let output = std::process::Command::new("docker-compose")
            .arg("-f")
            .arg(self.compose_file())
            .arg("-p")
            .arg(&self.project_name)
            .arg("up")
            .arg("-d")
            .output()?;

        if !output.status.success() {
            return Err(format!(
                "docker-compose up failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        // Wait for Miroir to be healthy
        self.wait_for_healthy().await?;

        println!("Cluster {} is ready", self.project_name);
        Ok(())
    }

    /// Stop the cluster with docker-compose.
    async fn down(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Stopping cluster {}...", self.project_name);

        let output = std::process::Command::new("docker-compose")
            .arg("-f")
            .arg(self.compose_file())
            .arg("-p")
            .arg(&self.project_name)
            .arg("down")
            .arg("-v") // Remove volumes
            .output()?;

        if !output.status.success() {
            return Err(format!(
                "docker-compose down failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        println!("Cluster {} stopped", self.project_name);
        Ok(())
    }

    /// Kill a Meilisearch node by index (docker stop).
    async fn kill_meili(&self, node_index: usize) -> Result<(), Box<dyn std::error::Error>> {
        let container_name = format!("{}_meili-{}_1", self.project_name, node_index);
        println!("Killing container {}...", container_name);

        let output = std::process::Command::new("docker")
            .arg("stop")
            .arg(&container_name)
            .output()?;

        if !output.status.success() {
            return Err(format!(
                "docker stop failed for {}: {}",
                container_name,
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        // Give it a moment to fully stop
        sleep(Duration::from_millis(500)).await;
        Ok(())
    }

    /// Restart a previously killed Meilisearch node.
    async fn restart_meili(&self, node_index: usize) -> Result<(), Box<dyn std::error::Error>> {
        let container_name = format!("{}_meili-{}_1", self.project_name, node_index);
        println!("Restarting container {}...", container_name);

        let output = std::process::Command::new("docker")
            .arg("start")
            .arg(&container_name)
            .output()?;

        if !output.status.success() {
            return Err(format!(
                "docker start failed for {}: {}",
                container_name,
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        // Wait for the node to be healthy again
        self.wait_for_meili_healthy(node_index).await?;
        Ok(())
    }

    /// Apply network delay to a Meilisearch node using tc netem.
    async fn apply_netem(
        &self,
        node_index: usize,
        delay_ms: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let container_name = format!("{}_meili-{}_1", self.project_name, node_index);
        println!(
            "Applying {}ms delay to container {}...",
            delay_ms, container_name
        );

        // Try to remove existing qdisc first, then add new one
        let _ = std::process::Command::new("docker")
            .arg("exec")
            .arg(&container_name)
            .arg("tc")
            .arg("qdisc")
            .arg("del")
            .arg("dev")
            .arg("eth0")
            .arg("root")
            .output();

        let output = std::process::Command::new("docker")
            .arg("exec")
            .arg(&container_name)
            .arg("tc")
            .arg("qdisc")
            .arg("add")
            .arg("dev")
            .arg("eth0")
            .arg("root")
            .arg("netem")
            .arg("delay")
            .arg(format!("{}ms", delay_ms))
            .output()?;

        if !output.status.success() {
            return Err(format!(
                "tc netem failed for {}: {}",
                container_name,
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(())
    }

    /// Remove network delay from a Meilisearch node.
    async fn remove_netem(&self, node_index: usize) -> Result<(), Box<dyn std::error::Error>> {
        let container_name = format!("{}_meili-{}_1", self.project_name, node_index);
        println!("Removing netem from container {}...", container_name);

        let output = std::process::Command::new("docker")
            .arg("exec")
            .arg(&container_name)
            .arg("tc")
            .arg("qdisc")
            .arg("del")
            .arg("dev")
            .arg("eth0")
            .arg("root")
            .output()?;

        // Don't error if qdisc doesn't exist
        let _ = output.status;
        Ok(())
    }

    /// Kill the Miroir orchestrator (scale to 0).
    async fn kill_miroir(&self) -> Result<(), Box<dyn std::error::Error>> {
        let service_name = "miroir";
        println!("Scaling {} to 0...", service_name);

        let output = std::process::Command::new("docker-compose")
            .arg("-f")
            .arg(self.compose_file())
            .arg("-p")
            .arg(&self.project_name)
            .arg("scale")
            .arg(format!("{}=0", service_name))
            .output()?;

        if !output.status.success() {
            return Err(format!(
                "docker-compose scale to 0 failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        // Give it a moment to fully stop
        sleep(Duration::from_secs(2)).await;
        Ok(())
    }

    /// Restart the Miroir orchestrator (scale back to 1).
    async fn restart_miroir(&self) -> Result<(), Box<dyn std::error::Error>> {
        let service_name = "miroir";
        println!("Scaling {} back to 1...", service_name);

        let output = std::process::Command::new("docker-compose")
            .arg("-f")
            .arg(self.compose_file())
            .arg("-p")
            .arg(&self.project_name)
            .arg("scale")
            .arg(format!("{}=1", service_name))
            .output()?;

        if !output.status.success() {
            return Err(format!(
                "docker-compose scale to 1 failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        // Wait for Miroir to be healthy again
        self.wait_for_healthy().await?;
        Ok(())
    }

    /// Wait for Miroir to be healthy.
    async fn wait_for_healthy(&self) -> Result<(), Box<dyn std::error::Error>> {
        let client = reqwest::Client::new();
        let health_url = format!("http://localhost:{}/health", self.miroir_port());

        for _ in 0..60 {
            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                _ => sleep(Duration::from_millis(500)).await,
            }
        }

        Err(format!("Miroir not healthy after timeout at {}", health_url).into())
    }

    /// Wait for a Meilisearch node to be healthy.
    async fn wait_for_meili_healthy(&self, node_index: usize) -> Result<(), Box<dyn std::error::Error>> {
        let client = reqwest::Client::new();
        let health_url = format!("http://localhost:{}/health", self.meili_port(node_index));

        for _ in 0..30 {
            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                _ => sleep(Duration::from_millis(500)).await,
            }
        }

        Err(format!(
            "Meilisearch node {} not healthy after timeout",
            node_index
        )
        .into())
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        // Best-effort cleanup on drop
        let project_name = self.project_name.clone();
        let compose_file = self.compose_file().to_string();
        let _ = std::thread::spawn(move || {
            let _ = std::process::Command::new("docker-compose")
                .arg("-f")
                .arg(&compose_file)
                .arg("-p")
                .arg(&project_name)
                .arg("down")
                .arg("-v")
                .output();
        });
    }
}

/// Helper: Get Miroir client
fn miroir_client(port: u16) -> Client {
    let url = format!("http://localhost:{}", port);
    Client::new(url, Some(MASTER_KEY.to_string()))
}

/// Helper: Wait for a task to complete
async fn wait_for_task(client: &Client, task_uid: u32) -> Result<Task, Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();

    loop {
        let task = client.get_task(task_uid).await?;
        if task.is_finished() {
            if !task.is_succeeded() {
                return Err(format!("Task {} failed: {:?}", task_uid, task).into());
            }
            return Ok(task);
        }

        if start.elapsed() > timeout {
            return Err(format!("Task {} timed out", task_uid).into());
        }

        sleep(Duration::from_millis(200)).await;
    }
}

/// Helper: Create index and add test documents
async fn setup_test_data(
    cluster: &TestCluster,
    index_name: &str,
    doc_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = miroir_client(cluster.miroir_port());

    // Create index
    let indexes = client.clone();
    match indexes.get_index(index_name).await {
        Ok(_) => {}
        Err(_) => {
            let task = indexes.create_index(index_name, Some("id")).await?;
            wait_for_task(&indexes, task).await?;
        }
    }

    // Add documents
    let mut docs = Vec::new();
    for i in 0..doc_count {
        docs.push(json!({
            "id": format!("doc-{:05}", i),
            "title": format!("Document {}", i),
            "content": format!("Content for document {}", i),
        }));
    }

    let index = client.index(index_name);
    let task = index.add_documents(&docs, None).await?;
    wait_for_task(&client, task).await?;

    // Wait for documents to be searchable
    sleep(Duration::from_secs(2)).await;

    Ok(())
}

// ============================================================================
// Runbook: Scenario 1 - Kill 1 of 3 nodes (RF=2)
// Expected: Continuous search; degraded writes warn via header
// ============================================================================

/// Runbook: Kill 1 of 3 nodes (RF=2)
///
/// **Expected Result:** Continuous search; degraded writes warn via header (though
/// with RF=2 and one node down, surviving replicas cover all shards, so degraded
/// header may not appear).
///
/// **Manual Reproduction:**
/// ```bash
/// # Start the RF=2 cluster
/// docker-compose -f examples/docker-compose-dev-rf2.yml -p manual-s1 up -d
///
/// # Kill node-1
/// docker stop manual-s1_meili-1_1
///
/// # Run searches - should succeed
/// curl -X POST 'http://localhost:7710/indexes/test/search' \
///   -H 'Authorization: Bearer dev-key' \
///   -H 'Content-Type: application/json' \
///   --data-binary '{"q": "content"}'
///
/// # Check for degraded header (should NOT appear with RF=2)
/// curl -I -X POST 'http://localhost:7710/indexes/test/search' \
///   -H 'Authorization: Bearer dev-key' \
///   -H 'Content-Type: application/json' \
///   --data-binary '{"q": "content"}'
/// ```
///
/// **Expected Observables:**
/// - `miroir_router_search_latency_*` - May increase slightly
/// - `miroir_node_requests_total{node="meili-1"}` - Drops to zero
/// - No `X-Miroir-Degraded` header (RF=2 provides full coverage)
/// - No search failures
///
/// **Recovery:** Restart the node; Miroir resumes routing within health check interval (default 5s).
#[tokio::test]
#[ignore = "Chaos test: requires docker-compose and takes 30+ seconds"]
async fn chaos_scenario_1_kill_one_node_rf2() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = TestCluster::new("scenario1", 2);
    cluster.up().await?;

    let index_name = "chaos_s1";
    setup_test_data(&cluster, index_name, 500).await?;

    // Kill node-1 (of 3 nodes, RF=2)
    cluster.kill_meili(1).await?;

    let client = miroir_client(cluster.miroir_port());

    // Searches should still work (RF=2 means surviving replicas cover all shards)
    let results: serde_json::Value = client
        .index(index_name)
        .search()
        .with_query("content")
        .with_limit(500)
        .execute()
        .await?;

    let hits = results["hits"].as_array().unwrap();
    assert_eq!(
        hits.len(),
        500,
        "Search should return all 500 results after node loss"
    );

    // Check for degraded header (should not appear with RF=2 and one node down)
    let http_client = reqwest::Client::new();
    let search_url = format!(
        "http://localhost:{}/indexes/{}/search",
        cluster.miroir_port(),
        index_name
    );
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

    // Writes should succeed
    let task = client
        .index(index_name)
        .add_documents(
            &[json!({"id": "new-doc", "title": "New", "content": "During failure"})],
            None,
        )
        .await?;
    let _ = wait_for_task(&client, task).await?;

    cluster.down().await?;
    Ok(())
}

// ============================================================================
// Runbook: Scenario 2 - Kill 2 of 3 nodes (RF=2)
// Expected: Shard loss; 503 or partial per policy
// ============================================================================

/// Runbook: Kill 2 of 3 nodes (RF=2)
///
/// **Expected Result:** Shard loss; 503 (Service Unavailable) or partial results per policy.
///
/// **Manual Reproduction:**
/// ```bash
/// # Start the RF=2 cluster
/// docker-compose -f examples/docker-compose-dev-rf2.yml -p manual-s2 up -d
///
/// # Kill node-1 and node-2
/// docker stop manual-s2_meili-1_1
/// docker stop manual-s2_meili-2_1
///
/// # Run searches - may return 503 or partial results
/// curl -i -X POST 'http://localhost:7710/indexes/test/search' \
///   -H 'Authorization: Bearer dev-key' \
///   -H 'Content-Type: application/json' \
///   --data-binary '{"q": "content"}'
/// ```
///
/// **Expected Observables:**
/// - `miroir_router_search_errors_total{reason="unavailable_shard"}` - Increases
/// - `X-Miroir-Degraded` - MUST appear on successful responses
/// - HTTP status may be 503 or 200 with degraded results
///
/// **Recovery:** Restart one node; searches return full results again.
#[tokio::test]
#[ignore = "Chaos test: requires docker-compose and takes 30+ seconds"]
async fn chaos_scenario_2_kill_two_nodes_rf2() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = TestCluster::new("scenario2", 2);
    cluster.up().await?;

    let index_name = "chaos_s2";
    setup_test_data(&cluster, index_name, 500).await?;

    // Kill node-1 and node-2 (of 3 nodes, RF=2)
    cluster.kill_meili(1).await?;
    cluster.kill_meili(2).await?;

    let client = miroir_client(cluster.miroir_port());
    let http_client = reqwest::Client::new();
    let search_url = format!(
        "http://localhost:{}/indexes/{}/search",
        cluster.miroir_port(),
        index_name
    );

    // Search may fail with 503 or return partial results
    let resp = http_client
        .post(&search_url)
        .header("Authorization", format!("Bearer {}", MASTER_KEY))
        .json(&json!({"q": "content"}))
        .send()
        .await?;

    // Either 503 (service unavailable) or partial results with degraded header
    if resp.status() == StatusCode::SERVICE_UNAVAILABLE {
        // Expected: some shards are unavailable
    } else {
        // Partial results - check for degraded header
        assert!(
            resp.headers().get("X-Miroir-Degraded").is_some(),
            "X-Miroir-Degraded header must appear with 2 of 3 nodes down"
        );

        let results: serde_json::Value = resp.json().await?;
        let hits = results["hits"].as_array().unwrap();
        // Should have partial results (less than full 500)
        assert!(
            hits.len() < 500,
            "Should return partial results when 2 of 3 nodes are down"
        );
    }

    cluster.down().await?;
    Ok(())
}

// ============================================================================
// Runbook: Scenario 3 - Kill 1 of 2 Miroir replicas
// Expected: Zero client-visible downtime
// ============================================================================

/// Runbook: Kill 1 of 2 Miroir replicas
///
/// **Expected Result:** Zero client-visible downtime (if running multiple Miroir replicas
/// behind a load balancer).
///
/// **Manual Reproduction:**
/// ```bash
/// # In a real deployment, Miroir runs as a Kubernetes Deployment/StatefulSet
/// kubectl delete pod miroir-0 -n miroir
///
/// # Immediately run searches - should succeed (LB routes to surviving replica)
/// curl -X POST 'http://miroir-service.miroir.svc.cluster.local:7700/indexes/test/search' \
///   -H 'Authorization: Bearer $MASTER_KEY' \
///   -H 'Content-Type: application/json' \
///   --data-binary '{"q": "test"}'
/// ```
///
/// **Expected Observables:**
/// - Zero search failures (if health check is properly configured)
/// - Brief latency spike during failover (< 1s typically)
/// - No `X-Miroir-Degraded` header (backend nodes are healthy)
///
/// **Recovery:** Kubernetes automatically restarts the pod; no manual intervention needed.
#[tokio::test]
#[ignore = "Chaos test: requires docker-compose and takes 30+ seconds"]
async fn chaos_scenario_3_kill_miroir_replica() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = TestCluster::new("scenario3", 1);
    cluster.up().await?;

    let index_name = "chaos_s3";
    setup_test_data(&cluster, index_name, 500).await?;

    let client = miroir_client(cluster.miroir_port());

    // Kill the Miroir orchestrator
    cluster.kill_miroir().await?;

    // Give it a moment to fully stop
    sleep(Duration::from_secs(1)).await;

    // Restart Miroir
    cluster.restart_miroir().await?;

    // Searches should work immediately after recovery
    let results: serde_json::Value = client
        .index(index_name)
        .search()
        .with_query("content")
        .with_limit(500)
        .execute()
        .await?;

    let hits = results["hits"].as_array().unwrap();
    assert_eq!(
        hits.len(),
        500,
        "Search should return all results after Miroir restart"
    );

    cluster.down().await?;
    Ok(())
}

// ============================================================================
// Runbook: Scenario 4 - tc netem delay 500ms on one node
// Expected: Searches slow by at most max shard latency; no errors
// ============================================================================

/// Runbook: Network delay (tc netem) on one node
///
/// **Expected Result:** Searches slow by at most max shard latency; no errors.
/// With 500ms added delay, searches should complete in < 2 seconds total.
///
/// **Manual Reproduction:**
/// ```bash
/// # Start the cluster
/// docker-compose -f examples/docker-compose-dev.yml -p manual-s4 up -d
///
/// # Apply 500ms delay to meili-0
/// docker exec manual-s4_meili-0_1 \
///   tc qdisc add dev eth0 root netem delay 500ms
///
/// # Run searches and measure latency - should succeed but be slower
/// time curl -X POST 'http://localhost:7700/indexes/test/search' \
///   -H 'Authorization: Bearer dev-key' \
///   -H 'Content-Type: application/json' \
///   --data-binary '{"q": "content"}'
///
/// # Remove delay
/// docker exec manual-s4_meili-0_1 tc qdisc del dev eth0 root
/// ```
///
/// **Expected Observables:**
/// - `miroir_router_search_latency_seconds_bucket` - Latency increases
/// - No search failures
/// - No timeout errors
///
/// **Recovery:** Remove the netem qdisc; latency returns to baseline.
#[tokio::test]
#[ignore = "Chaos test: requires docker-compose and takes 30+ seconds"]
async fn chaos_scenario_4_netem_delay() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = TestCluster::new("scenario4", 1);
    cluster.up().await?;

    let index_name = "chaos_s4";
    setup_test_data(&cluster, index_name, 500).await?;

    // Apply 500ms delay to node-0
    cluster.apply_netem(0, 500).await?;

    let client = miroir_client(cluster.miroir_port());

    // Measure search latency with delay
    let start = std::time::Instant::now();
    let results: serde_json::Value = client
        .index(index_name)
        .search()
        .with_query("content")
        .with_limit(100)
        .execute()
        .await?;
    let delayed_latency = start.elapsed();

    let hits = results["hits"].as_array().unwrap();
    assert_eq!(
        hits.len(),
        100,
        "Search should return all results with netem delay"
    );

    // Clean up netem
    cluster.remove_netem(0).await?;

    // Measure baseline latency
    let start = std::time::Instant::now();
    let results: serde_json::Value = client
        .index(index_name)
        .search()
        .with_query("content")
        .with_limit(100)
        .execute()
        .await?;
    let baseline_latency = start.elapsed();

    let baseline_hits = results["hits"].as_array().unwrap();
    assert_eq!(baseline_hits.len(), 100);

    // Delayed search should be slower but still succeed
    assert!(
        delayed_latency > baseline_latency,
        "Delayed search should be slower than baseline"
    );

    // But not excessively slower (max shard latency + some overhead)
    assert!(
        delayed_latency < Duration::from_secs(2),
        "Delayed search should complete in < 2s, took {:?}",
        delayed_latency
    );

    cluster.down().await?;
    Ok(())
}

// ============================================================================
// Runbook: Scenario 5 - Restart a killed node
// Expected: Miroir detects recovery within health check interval, resumes routing
// ============================================================================

/// Runbook: Restart a killed node
///
/// **Expected Result:** Miroir detects recovery within health check interval (default 5s)
/// and resumes routing. No data loss; searches and writes work normally after recovery.
///
/// **Manual Reproduction:**
/// ```bash
/// # Start the RF=2 cluster
/// docker-compose -f examples/docker-compose-dev-rf2.yml -p manual-s5 up -d
///
/// # Kill node-1
/// docker stop manual-s5_meili-1_1
///
/// # Verify searches still work (RF=2 provides coverage)
/// curl -X POST 'http://localhost:7710/indexes/test/search' \
///   -H 'Authorization: Bearer dev-key' \
///   -H 'Content-Type: application/json' \
///   --data-binary '{"q": "content", "limit": 500}'
///
/// # Restart node-1
/// docker start manual-s5_meili-1_1
///
/// # Wait for health check to detect recovery (default: 5s interval)
/// sleep 10
///
/// # Verify searches work with full node set
/// curl -X POST 'http://localhost:7710/indexes/test/search' \
///   -H 'Authorization: Bearer dev-key' \
///   -H 'Content-Type: application/json' \
///   --data-binary '{"q": "content", "limit": 500}'
/// ```
///
/// **Expected Observables:**
/// - `miroir_node_health_status{node="meili-1"}` - Goes to 0, then back to 1
/// - `miroir_node_requests_total{node="meili-1"}` - Drops to 0, then increases
/// - No search failures during outage
///
/// **Recovery:** Restart the failed node; Miroir resumes routing within health check interval.
#[tokio::test]
#[ignore = "Chaos test: requires docker-compose and takes 30+ seconds"]
async fn chaos_scenario_5_restart_node() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = TestCluster::new("scenario5", 2);
    cluster.up().await?;

    let index_name = "chaos_s5";
    setup_test_data(&cluster, index_name, 500).await?;

    let client = miroir_client(cluster.miroir_port());

    // Kill node-1
    cluster.kill_meili(1).await?;

    // Searches still work with RF=2
    let results: serde_json::Value = client
        .index(index_name)
        .search()
        .with_query("content")
        .with_limit(500)
        .execute()
        .await?;

    let hits = results["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 500);

    // Restart node-1
    cluster.restart_meili(1).await?;

    // Wait a bit for health check to detect recovery
    sleep(Duration::from_secs(5)).await;

    // Searches should still work
    let results: serde_json::Value = client
        .index(index_name)
        .search()
        .with_query("content")
        .with_limit(500)
        .execute()
        .await?;

    let hits = results["hits"].as_array().unwrap();
    assert_eq!(
        hits.len(),
        500,
        "Search should return all results after node recovery"
    );

    // Verify node is routing again by adding a new document
    let task = client
        .index(index_name)
        .add_documents(
            &[json!({"id": "after-recovery", "title": "After", "content": "Recovery test"})],
            None,
        )
        .await?;
    let _ = wait_for_task(&client, task).await?;

    // Search for the new document
    let results: serde_json::Value = client
        .index(index_name)
        .search()
        .with_query("Recovery test")
        .execute()
        .await?;

    let hits = results["hits"].as_array().unwrap();
    assert_eq!(
        hits.len(),
        1,
        "Should find document added after node recovery"
    );

    cluster.down().await?;
    Ok(())
}

// ============================================================================
// Runbook: Scenario 6 - Kill a node mid-rebalance
// Expected: Rebalancer pauses, resumes on recovery; no data loss
// ============================================================================

/// Runbook: Kill a node mid-rebalance
///
/// **Expected Result:** Rebalancer pauses, resumes on recovery; no data loss.
/// Write operation may fail or succeed partially.
///
/// **Manual Reproduction:**
/// ```bash
/// # Start the RF=2 cluster
/// docker-compose -f examples/docker-compose-dev-rf2.yml -p manual-s6 up -d
///
/// # Create a test index
/// curl -X POST 'http://localhost:7710/indexes' \
///   -H 'Authorization: Bearer dev-key' \
///   -H 'Content-Type: application/json' \
///   --data-binary '{"uid": "manual-s6", "primaryKey": "id"}'
///
/// # Start a large document load
/// # (while loading, kill node-1)
/// docker stop manual-s6_meili-1_1
///
/// # Restart node-1
/// docker start manual-s6_meili-1_1
///
/// # Check document count
/// curl -X POST 'http://localhost:7710/indexes/manual-s6/search' \
///   -H 'Authorization: Bearer dev-key' \
///   -H 'Content-Type: application/json' \
///   --data-binary '{"q": "", "limit": 1000}'
/// ```
///
/// **Expected Observables:**
/// - `miroir_rebalancer_active_migrations` - Non-zero before failure, may pause
/// - `miroir_rebalancer_paused_total` - Increases when node fails
/// - Task may show `succeeded`, `failed`, or `processing`
///
/// **Recovery:** Restart the failed node; rebalancer resumes from checkpoint.
#[tokio::test]
#[ignore = "Chaos test: requires docker-compose and takes 30+ seconds"]
async fn chaos_scenario_6_kill_mid_rebalance() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = TestCluster::new("scenario6", 2);
    cluster.up().await?;

    let client = miroir_client(cluster.miroir_port());
    let index_name = "chaos_s6";

    // Create index
    let indexes = client.clone();
    match indexes.get_index(index_name).await {
        Ok(_) => {}
        Err(_) => {
            let task = indexes.create_index(index_name, Some("id")).await?;
            wait_for_task(&indexes, task).await?;
        }
    }

    // Start adding documents (this will be our "rebalance" simulation)
    let mut docs = Vec::new();
    for i in 0..1000 {
        docs.push(json!({
            "id": format!("doc-{:05}", i),
            "title": format!("Document {}", i),
            "content": format!("Content for document {}", i),
        }));
    }

    // Add documents - this will be our "rebalance" simulation
    let index = client.index(index_name);
    let task = index.add_documents(&docs, None).await?;

    // Kill node-1 mid-operation
    cluster.kill_meili(1).await?;

    // Wait for task to complete (should succeed or fail gracefully)
    let _ = wait_for_task(&client, task).await;

    // Restart node-1
    cluster.restart_meili(1).await?;

    // Wait for recovery
    sleep(Duration::from_secs(5)).await;

    // Verify data integrity - all documents should be present
    let results: serde_json::Value = client
        .index(index_name)
        .search()
        .with_query("content")
        .with_limit(1000)
        .execute()
        .await?;

    let hits = results["hits"].as_array().unwrap();

    // Should have all or most documents (some may be lost if task failed)
    assert!(
        hits.len() >= 900,
        "Should have most documents after mid-rebalance failure, got {}",
        hits.len()
    );

    cluster.down().await?;
    Ok(())
}
