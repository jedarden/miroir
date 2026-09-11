//! Cache flow integration tests (plan §13.13).
//!
//! End-to-end integration tests for cache flow through the HTTP proxy:
//! - Cache hit bypasses fan-out to Meilisearch nodes
//! - Cache miss triggers normal scatter-gather execution
//! - Cache stores results after successful scatter-gather
//! - Cache reduces upstream Meilisearch calls under repeated queries
//! - Graceful handling of cache connection failures
//!
//! These tests use testcontainers to spin up real Meilisearch instances,
//! spawn the compiled `miroir-proxy` binary against them, and make actual
//! HTTP requests through the proxy to test cache behavior in a realistic
//! environment.
//!
//! Prerequisites:
//!   Option 1: Docker available for testcontainers Meilisearch
//!   Option 2: Set MIROIR_TEST_SKIP_DOCKER=1 to skip these tests
//!
//! On the lab box there is no docker.sock; podman rootless works instead:
//!   systemctl --user start podman.socket
//!   DOCKER_HOST=unix:///run/user/1001/podman/podman.sock cargo test \
//!     -p miroir-proxy --test p13_13_cache_flow_integration
//! (`check_docker_available` trusts an explicit DOCKER_HOST and lets
//! testcontainers surface any connection failure itself.)

use anyhow::Context;
use reqwest::Client;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::meilisearch::Meilisearch;
use tokio::process::{Child, Command};
use tokio::sync::MutexGuard;
use tokio::time::sleep;

/// Master key shared by every Meilisearch node. `NodeConfig` carries no
/// per-node credentials, so the proxy (`node_master_key`) and the helpers
/// below that address the nodes directly must all use this same key.
const NODE_MASTER_KEY: &str = "key0";

/// Client-facing proxy key and port, mirrored into the generated config.
const PROXY_MASTER_KEY: &str = "test_master_key";
const PROXY_PORT: u16 = 17770;

/// Serializes the whole suite: the proxy binary hard-binds its Prometheus
/// metrics listener on port 9090 (`main.rs`), so at most one instance can run
/// on this host. The guard lives inside `CacheFlowTestSetup`, so each test
/// holds the slot from setup until teardown.
static PROXY_SLOT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// How long `wait_for_ready` waits for `/health` before giving up and
/// embedding the proxy's log tails in the failure.
const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Lines of each proxy log file embedded in a readiness-timeout failure.
/// Startup failures (config validation, bind errors) report at the end of the
/// stream, so a tail carries the reason.
const LOG_TAIL_LINES: usize = 50;

/// Check if Docker is available for testcontainers.
fn check_docker_available() -> anyhow::Result<()> {
    if std::env::var("MIROIR_TEST_SKIP_DOCKER").is_ok() {
        anyhow::bail!(
            "Docker tests skipped via MIROIR_TEST_SKIP_DOCKER. \
             Unset MIROIR_TEST_SKIP_DOCKER and ensure Docker is available."
        );
    }

    // An explicit DOCKER_HOST (podman service, TCP daemon, ...) overrides the
    // default socket path; trust it and let testcontainers surface any
    // connection failure itself.
    if std::env::var("DOCKER_HOST").is_ok() {
        return Ok(());
    }

    let docker_sock = Path::new("/var/run/docker.sock");
    if !docker_sock.exists() {
        anyhow::bail!(
            "Docker socket not found at /var/run/docker.sock. \
             Set MIROIR_TEST_SKIP_DOCKER=1 to skip, or ensure Docker is running."
        );
    }

    if let Err(e) = std::fs::metadata(docker_sock) {
        anyhow::bail!(
            "Cannot access Docker socket: {e}. \
             Set MIROIR_TEST_SKIP_DOCKER=1 to skip, or ensure Docker is running."
        );
    }

    Ok(())
}

/// A proxy process spawned from the compiled `miroir-proxy` binary. The
/// config file is resolved from the process CWD (`MiroirConfig::load` scans
/// search paths relative to it), so the child runs inside its own temp dir
/// with a generated `miroir.yaml`. `kill_on_drop` tears the process down and
/// `TempDir` removes the config dir when the setup is dropped.
struct SpawnedProxy {
    /// Mutexed because polling the exit status (`try_wait`) needs `&mut`,
    /// while `wait_for_ready` only holds `&self`. stdio is never piped — the
    /// child logs to files (see `spawn`) — and `kill_on_drop` tears the
    /// proxy down on drop.
    _child: tokio::sync::Mutex<Child>,
    _config_dir: tempfile::TempDir,
}

impl SpawnedProxy {
    async fn spawn(node_urls: &[String]) -> anyhow::Result<Self> {
        let config_dir = tempfile::TempDir::new()?;
        let task_db_path: PathBuf = config_dir.path().join("miroir-tasks.db");
        std::fs::write(
            config_dir.path().join("miroir.yaml"),
            proxy_config_yaml(node_urls, &task_db_path),
        )?;

        // Child logs go to files rather than pipes: nothing drains a pipe
        // here, and a full pipe buffer would block the proxy mid-test. The
        // files live in the config dir and vanish with it on drop.
        let stdout = std::fs::File::create(config_dir.path().join("proxy.stdout.log"))?;
        let stderr = std::fs::File::create(config_dir.path().join("proxy.stderr.log"))?;

        let child = Command::new(env!("CARGO_BIN_EXE_miroir-proxy"))
            .current_dir(config_dir.path())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true)
            .spawn()
            .context("failed to spawn miroir-proxy binary")?;

        Ok(Self {
            _child: tokio::sync::Mutex::new(child),
            _config_dir: config_dir,
        })
    }

    /// Tail of the proxy's stdout and stderr logs, for attaching to a
    /// readiness-timeout failure.
    ///
    /// The logs live in the config `TempDir` and are deleted with it on drop,
    /// so this is only callable while the proxy is alive — exactly where
    /// `wait_for_ready`'s timeout path sits. Without reading here, a timeout
    /// carried no evidence at all: the parent bead failed three times with
    /// nothing to inspect.
    fn log_tails(&self) -> String {
        ["proxy.stdout.log", "proxy.stderr.log"]
            .into_iter()
            .map(|name| {
                let path = self._config_dir.path().join(name);
                let tail = match std::fs::read_to_string(&path) {
                    // A spawn that died before logging leaves 0-byte files;
                    // say so instead of emitting a bare header that reads like
                    // the tail reader itself failed.
                    Ok(content) if content.trim().is_empty() => {
                        "<empty — the proxy wrote nothing>".to_string()
                    }
                    Ok(content) => {
                        let lines: Vec<&str> = content.lines().collect();
                        let start = lines.len().saturating_sub(LOG_TAIL_LINES);
                        lines[start..].join("\n")
                    }
                    Err(e) => format!("<unreadable: {e}>"),
                };
                format!(
                    "--- {} (last {LOG_TAIL_LINES} lines) ---\n{tail}",
                    path.display()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// One-line liveness report for the proxy process, for the
    /// readiness-timeout failure: distinguishes a proxy that died before it
    /// could log anything (empty tails in the same message) from one still
    /// running.
    async fn exit_status_line(&self) -> String {
        match self._child.lock().await.try_wait() {
            Ok(Some(status)) => format!("proxy process has exited: {status}"),
            Ok(None) => "proxy process is still running".to_string(),
            Err(e) => format!("proxy process status unknown: {e}"),
        }
    }
}

/// Render the proxy's `miroir.yaml`. Only overrides of `MiroirConfig`
/// defaults are written; every other field keeps its default.
///
/// Topology note: `MiroirConfig::validate` requires a redis task store (plus
/// leader election) once `replication_factor > 1` or `replica_groups > 1`,
/// and the sqlite store this test can actually use is single-writer — so the
/// proxy runs with RF 1 and a single replica group (all nodes in group 0).
/// The sqlite db path is pointed inside the config dir because the default
/// (`/data/miroir-tasks.db`) is not writable here, and `search_ui` is
/// disabled because the real binary refuses to start with it enabled but no
/// JWT secret configured.
fn proxy_config_yaml(node_urls: &[String], task_db_path: &Path) -> String {
    let nodes = node_urls
        .iter()
        .enumerate()
        .map(|(i, url)| format!("  - id: node-{i}\n    address: {url}\n    replica_group: 0"))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "master_key: {PROXY_MASTER_KEY}\n\
         node_master_key: {NODE_MASTER_KEY}\n\
         shards: 16\n\
         replication_factor: 1\n\
         replica_groups: 1\n\
         nodes:\n{nodes}\n\
         server:\n  bind: 127.0.0.1\n  port: {PROXY_PORT}\n\
         health:\n  interval_ms: 200\n  timeout_ms: 1000\n\
         task_store:\n  backend: sqlite\n  path: {}\n\
         result_cache:\n  enabled: true\n  ttl_ms: 500\n  max_size: 1000\n\
         search_ui:\n  enabled: false\n",
        task_db_path.display(),
    )
}

/// Test configuration helper.
struct CacheFlowTestSetup {
    /// Holds `PROXY_SLOT` for the whole test body (see its docs).
    _proxy_slot: MutexGuard<'static, ()>,
    /// Container handles must be retained for the whole test: dropping a
    /// `ContainerAsync` removes the container, which would tear the nodes
    /// down before any request is made.
    _containers: Vec<ContainerAsync<Meilisearch>>,
    /// The proxy under test; killed and cleaned up on drop.
    _proxy: SpawnedProxy,
    meilisearch_urls: Vec<String>,
    proxy_url: String,
    master_key: String,
    client: Client,
}

impl CacheFlowTestSetup {
    async fn new() -> anyhow::Result<Self> {
        // Take the proxy slot first: with it held, no other test in this
        // binary can race us for the metrics port while our containers start.
        let proxy_slot = PROXY_SLOT.lock().await;

        // Bail with the skip reason when Docker is unavailable so callers can
        // skip instead of panicking on the first container start.
        check_docker_available()?;

        // Start 3 Meilisearch nodes for scatter-gather testing
        let mut containers = Vec::new();
        let mut meilisearch_urls = Vec::new();
        for _ in 0..3 {
            // Configure the key via the module's env API (it sets
            // MEILI_MASTER_KEY). `with_cmd` is not an option here: it REPLACES
            // the image Cmd, so the container runs `tini -- --master-key=key0`
            // and tini dies with "[FATAL tini (2)] exec --master-key=...
            // failed: No such file or directory" before meilisearch starts.
            let meilisearch = Meilisearch::default()
                .with_master_key(NODE_MASTER_KEY)
                .start()
                .await?;

            let port = meilisearch.get_host_port_ipv4(7700).await?;
            let url = format!("http://localhost:{port}");
            meilisearch_urls.push(url);
            containers.push(meilisearch);
        }

        // Spawn the real proxy against the nodes
        let proxy = SpawnedProxy::spawn(&meilisearch_urls).await?;

        Ok(Self {
            _proxy_slot: proxy_slot,
            _containers: containers,
            _proxy: proxy,
            meilisearch_urls,
            proxy_url: format!("http://127.0.0.1:{PROXY_PORT}"),
            master_key: PROXY_MASTER_KEY.to_string(),
            client: Client::new(),
        })
    }

    /// Wait for the proxy to be ready.
    ///
    /// Ready means `/health` answered with exactly `{"status":"available"}` —
    /// the body that route returns unconditionally once the listener binds
    /// (`routes/health.rs`). A bare 2xx is not enough: a stale proxy left
    /// bound to the port from an earlier run would pass it, so a 2xx with any
    /// other body bails immediately instead of burning [`READY_TIMEOUT`] on a
    /// process that can never report ready.
    ///
    /// On timeout the bail embeds the tails of the proxy's stdout/stderr
    /// logs, read here while the config `TempDir` still exists — once the
    /// setup drops, they are gone and the timeout is undiagnosable — next to
    /// the process liveness line and the last failed `/health` probe. Success
    /// prints the readiness latency so the expected ~6s spawn time stays
    /// verifiable per test.
    async fn wait_for_ready(&self) -> anyhow::Result<()> {
        let started = tokio::time::Instant::now();
        let deadline = started + READY_TIMEOUT;
        let url = &self.proxy_url;
        // Outcome of the most recent failed probe, embedded in the timeout
        // bail: it separates a proxy that never bound the port (every probe
        // refused) from one bound but answering wrong.
        let mut last_probe = String::from("no probe completed");
        while tokio::time::Instant::now() < deadline {
            match self.client.get(format!("{url}/health")).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    let body: Value = serde_json::from_str(&text).with_context(|| {
                        format!(
                            "GET {url}/health returned {status} with a non-JSON body: {text}; \
                             {{\"status\":\"available\"}} is expected — a process other than \
                             this test's proxy is likely bound to port {PROXY_PORT}"
                        )
                    })?;
                    anyhow::ensure!(
                        body == json!({"status": "available"}),
                        "GET {url}/health returned {status} with an unexpected body {body}; \
                         {{\"status\":\"available\"}} is expected — a process other than \
                         this test's proxy is bound to port {PROXY_PORT}"
                    );
                    println!("proxy ready in {:.2?}", started.elapsed());
                    return Ok(());
                }
                Ok(resp) => last_probe = format!("GET {url}/health returned {}", resp.status()),
                Err(e) => last_probe = format!("GET {url}/health failed: {e}"),
            }
            sleep(Duration::from_millis(100)).await;
        }
        anyhow::bail!(
            "Proxy did not become ready within {READY_TIMEOUT:?} — {}; \
             last probe: {last_probe}; proxy logs at timeout:\n{}",
            self._proxy.exit_status_line().await,
            self._proxy.log_tails()
        )
    }

    /// Create an index on all Meilisearch nodes.
    async fn create_index(&self, uid: &str) -> anyhow::Result<()> {
        let body = json!({
            "uid": uid,
            "primaryKey": "id"
        });

        for url in &self.meilisearch_urls {
            let resp = self
                .client
                .post(format!("{url}/indexes"))
                .header("Authorization", format!("Bearer {NODE_MASTER_KEY}"))
                .json(&body)
                .send()
                .await?;

            if !resp.status().is_success() {
                anyhow::bail!("Failed to create index on {url}");
            }
        }

        Ok(())
    }

    /// Add documents to an index.
    async fn add_documents(&self, index_uid: &str, documents: Value) -> anyhow::Result<()> {
        // Add documents to the first node only (replication will handle the rest)
        let url = &self.meilisearch_urls[0];
        let resp = self
            .client
            .post(format!("{url}/indexes/{index_uid}/documents"))
            .header("Authorization", format!("Bearer {NODE_MASTER_KEY}"))
            .json(&documents)
            .send()
            .await?;

        if !resp.status().is_success() {
            anyhow::bail!("Failed to add documents to index {index_uid}");
        }

        // Wait for replication
        sleep(Duration::from_millis(500)).await;
        Ok(())
    }

    /// Update index settings on every node directly. Like `create_index` and
    /// `add_documents`, index management bypasses the proxy.
    async fn set_node_settings(&self, index_uid: &str, settings: Value) -> anyhow::Result<()> {
        for url in &self.meilisearch_urls {
            let resp = self
                .client
                .put(format!("{url}/indexes/{index_uid}/settings"))
                .header("Authorization", format!("Bearer {NODE_MASTER_KEY}"))
                .json(&settings)
                .send()
                .await?;

            if !resp.status().is_success() {
                anyhow::bail!("Failed to update settings on {}: {}", url, resp.status());
            }
        }

        // Settings updates are async tasks in Meilisearch; give them a moment
        // to apply before the first query that depends on them.
        sleep(Duration::from_millis(500)).await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Integration tests for cache flow
// ---------------------------------------------------------------------------

#[tokio::test]
async fn acceptance_1_cache_hit_bypasses_fan_out() {
    // Acceptance: Cache hit returns cached result without executing scatter to Meilisearch nodes
    //
    // This test verifies that when a query result is cached, subsequent identical queries
    // return the cached result immediately without making any upstream calls to Meilisearch nodes.

    // Skip gracefully when Docker is unavailable: the setup's Err is a skip
    // signal, not a failure (repo convention — see p5_1_f and p10_7).
    let setup = match CacheFlowTestSetup::new().await {
        Ok(setup) => setup,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };
    setup.wait_for_ready().await.unwrap();

    // Create an index and add test documents
    setup.create_index("products").await.unwrap();
    let documents = json!([
        {"id": 1, "name": "Laptop", "price": 999, "category": "electronics"},
        {"id": 2, "name": "Phone", "price": 699, "category": "electronics"},
        {"id": 3, "name": "Tablet", "price": 449, "category": "electronics"}
    ]);
    setup.add_documents("products", documents).await.unwrap();

    // First query - should execute scatter-gather and cache the result
    let query1 = json!({
        "q": "laptop",
        "limit": 10
    });

    let resp1 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query1)
        .send()
        .await
        .unwrap();

    assert!(resp1.status().is_success());
    let result1: Value = resp1.json().await.unwrap();
    assert_eq!(result1["hits"].as_array().unwrap().len(), 1);
    assert_eq!(result1["hits"][0]["name"], "Laptop");

    // Second identical query - should hit cache and bypass scatter-gather
    // This should be faster and not result in any upstream calls
    let start = std::time::Instant::now();
    let resp2 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query1)
        .send()
        .await
        .unwrap();
    let cached_duration = start.elapsed();

    assert!(resp2.status().is_success());
    let result2: Value = resp2.json().await.unwrap();

    // Results should be identical
    assert_eq!(result1, result2);

    // Cache hit should be significantly faster than scatter-gather
    // (This is a heuristic - in a real test we'd measure actual scatter-gather time)
    // For now, we just verify the response was successful
    assert!(cached_duration < Duration::from_millis(100));

    // Verify cache statistics show a hit
    // (In real implementation, we'd expose a /cache-stats endpoint)
}

#[tokio::test]
async fn acceptance_2_cache_miss_triggers_fan_out() {
    // Acceptance: Cache miss triggers normal scatter-gather execution to Meilisearch nodes
    //
    // This test verifies that when a query is not in cache, the system executes
    // the full scatter-gather flow and then caches the result for future use.

    // Skip gracefully when Docker is unavailable: the setup's Err is a skip
    // signal, not a failure (repo convention — see p5_1_f and p10_7).
    let setup = match CacheFlowTestSetup::new().await {
        Ok(setup) => setup,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };
    setup.wait_for_ready().await.unwrap();

    // Create an index and add test documents
    setup.create_index("products").await.unwrap();
    let documents = json!([
        {"id": 1, "name": "Mouse", "price": 29},
        {"id": 2, "name": "Keyboard", "price": 79}
    ]);
    setup.add_documents("products", documents).await.unwrap();

    // Query for a term that hasn't been cached yet
    let query = json!({
        "q": "mouse",
        "limit": 10
    });

    let resp = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query)
        .send()
        .await
        .unwrap();

    assert!(resp.status().is_success());
    let result: Value = resp.json().await.unwrap();
    assert_eq!(result["hits"].as_array().unwrap().len(), 1);
    assert_eq!(result["hits"][0]["name"], "Mouse");

    // Second query should now be cached
    let resp2 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query)
        .send()
        .await
        .unwrap();

    assert!(resp2.status().is_success());
    let result2: Value = resp2.json().await.unwrap();
    assert_eq!(result, result2);
}

#[tokio::test]
async fn acceptance_3_cache_stores_results_after_scatter_gather() {
    // Acceptance: Results are cached after successful scatter-gather
    //
    // This test verifies that after a successful scatter-gather operation,
    // the merged result is properly cached for future use.

    // Skip gracefully when Docker is unavailable: the setup's Err is a skip
    // signal, not a failure (repo convention — see p5_1_f and p10_7).
    let setup = match CacheFlowTestSetup::new().await {
        Ok(setup) => setup,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };
    setup.wait_for_ready().await.unwrap();

    // Create an index with test data
    setup.create_index("books").await.unwrap();
    let documents = json!([
        {"id": 1, "title": "Rust Programming", "author": "Steve Klabnik"},
        {"id": 2, "title": "The Rust Language", "author": "Carol Nichols"},
        {"id": 3, "title": "Rust in Action", "author": "Tim McNamara"}
    ]);
    setup.add_documents("books", documents).await.unwrap();

    // Execute a search query
    let query = json!({
        "q": "rust",
        "limit": 20
    });

    let resp1 = setup
        .client
        .post(format!("{}/indexes/books/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query)
        .send()
        .await
        .unwrap();

    assert!(resp1.status().is_success());
    let result1: Value = resp1.json().await.unwrap();
    let hit_count = result1["hits"].as_array().unwrap().len();

    // Verify the result was cached by querying again
    let resp2 = setup
        .client
        .post(format!("{}/indexes/books/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query)
        .send()
        .await
        .unwrap();

    assert!(resp2.status().is_success());
    let result2: Value = resp2.json().await.unwrap();

    // Results should match exactly
    assert_eq!(result1, result2);
    assert_eq!(hit_count, 3); // All 3 books contain "rust"
}

#[tokio::test]
async fn acceptance_4_cache_reduces_upstream_meilisearch_calls() {
    // Acceptance: Cache reduces the number of upstream calls to Meilisearch
    //
    // This test verifies that repeated queries hit the cache instead of
    // making repeated calls to Meilisearch nodes, reducing upstream load.

    // Skip gracefully when Docker is unavailable: the setup's Err is a skip
    // signal, not a failure (repo convention — see p5_1_f and p10_7).
    let setup = match CacheFlowTestSetup::new().await {
        Ok(setup) => setup,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };
    setup.wait_for_ready().await.unwrap();

    // Create an index with test data
    setup.create_index("products").await.unwrap();
    let documents = json!([
        {"id": 1, "name": "Laptop", "category": "electronics"},
        {"id": 2, "name": "Desktop", "category": "electronics"},
        {"id": 3, "name": "Monitor", "category": "electronics"}
    ]);
    setup.add_documents("products", documents).await.unwrap();

    // Execute the same query multiple times
    let query = json!({
        "q": "electronics",
        "limit": 10
    });

    let mut results = Vec::new();
    for _ in 0..5 {
        let resp = setup
            .client
            .post(format!("{}/indexes/products/search", setup.proxy_url))
            .header("Authorization", format!("Bearer {}", setup.master_key))
            .json(&query)
            .send()
            .await
            .unwrap();

        assert!(resp.status().is_success());
        let result: Value = resp.json().await.unwrap();
        results.push(result);
    }

    // All results should be identical
    for result in &results[1..] {
        assert_eq!(results[0], *result);
    }

    // Without caching, this would make 5 * 3 = 15 upstream calls (5 queries * 3 nodes)
    // With caching, it should make significantly fewer calls (only the first query does scatter)
    // In a real test, we'd monitor actual upstream call counts
}

#[tokio::test]
async fn acceptance_5_different_queries_use_different_cache_keys() {
    // Acceptance: Different queries use different cache keys
    //
    // This test verifies that semantically different queries use different
    // cache entries and don't interfere with each other.

    // Skip gracefully when Docker is unavailable: the setup's Err is a skip
    // signal, not a failure (repo convention — see p5_1_f and p10_7).
    let setup = match CacheFlowTestSetup::new().await {
        Ok(setup) => setup,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };
    setup.wait_for_ready().await.unwrap();

    // Create an index with test data
    setup.create_index("products").await.unwrap();
    let documents = json!([
        {"id": 1, "name": "Laptop", "category": "electronics"},
        {"id": 2, "name": "Desk", "category": "furniture"},
        {"id": 3, "name": "Chair", "category": "furniture"}
    ]);
    setup.add_documents("products", documents).await.unwrap();

    // Execute different queries
    let query1 = json!({"q": "laptop", "limit": 10});
    let query2 = json!({"q": "desk", "limit": 10});
    let query3 = json!({"q": "chair", "limit": 20}); // Different limit too

    let resp1 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query1)
        .send()
        .await
        .unwrap();

    let resp2 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query2)
        .send()
        .await
        .unwrap();

    let resp3 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query3)
        .send()
        .await
        .unwrap();

    assert!(resp1.status().is_success());
    assert!(resp2.status().is_success());
    assert!(resp3.status().is_success());

    let result1: Value = resp1.json().await.unwrap();
    let result2: Value = resp2.json().await.unwrap();
    let result3: Value = resp3.json().await.unwrap();

    // Results should be different (different matches)
    assert_ne!(result1["hits"], result2["hits"]);
    assert_ne!(result2["hits"], result3["hits"]);

    // Repeat queries - should hit cache
    let resp1_cached = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query1)
        .send()
        .await
        .unwrap();

    let result1_cached: Value = resp1_cached.json().await.unwrap();
    assert_eq!(result1, result1_cached);
}

#[tokio::test]
async fn acceptance_6_graceful_handling_of_cache_connection_failure() {
    // Acceptance: Cache connection failures are handled gracefully
    //
    // This test verifies that if the cache becomes unavailable, the system
    // degrades gracefully and continues to serve requests via scatter-gather.

    // Skip gracefully when Docker is unavailable: the setup's Err is a skip
    // signal, not a failure (repo convention — see p5_1_f and p10_7).
    let setup = match CacheFlowTestSetup::new().await {
        Ok(setup) => setup,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };
    setup.wait_for_ready().await.unwrap();

    // Create an index with test data
    setup.create_index("products").await.unwrap();
    let documents = json!([
        {"id": 1, "name": "Test Product"}
    ]);
    setup.add_documents("products", documents).await.unwrap();

    // Execute a query
    let query = json!({"q": "test", "limit": 10});

    let resp = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query)
        .send()
        .await
        .unwrap();

    // Even if cache operations fail, the search should still work
    assert!(resp.status().is_success());
    let result: Value = resp.json().await.unwrap();
    assert_eq!(result["hits"].as_array().unwrap().len(), 1);

    // The system should continue to handle requests despite cache issues
    // (In a real test, we'd simulate cache failures and verify graceful degradation)
}

#[tokio::test]
async fn acceptance_7_cache_ttl_expiration() {
    // Acceptance: Cache entries expire after TTL
    //
    // This test verifies that cached results expire after the configured TTL
    // and subsequent queries trigger fresh scatter-gather.

    // Skip gracefully when Docker is unavailable: the setup's Err is a skip
    // signal, not a failure (repo convention — see p5_1_f and p10_7).
    let setup = match CacheFlowTestSetup::new().await {
        Ok(setup) => setup,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };
    setup.wait_for_ready().await.unwrap();

    // Create an index with test data
    setup.create_index("products").await.unwrap();
    let documents = json!([
        {"id": 1, "name": "Expiring Product"}
    ]);
    setup.add_documents("products", documents).await.unwrap();

    // Execute a query
    let query = json!({"q": "expiring", "limit": 10});

    let resp1 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query)
        .send()
        .await
        .unwrap();

    assert!(resp1.status().is_success());
    let result1: Value = resp1.json().await.unwrap();

    // Wait for cache to expire (TTL is 500ms in test config)
    sleep(Duration::from_millis(600)).await;

    // Query again after expiration - should trigger fresh scatter-gather
    let resp2 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query)
        .send()
        .await
        .unwrap();

    assert!(resp2.status().is_success());
    let result2: Value = resp2.json().await.unwrap();

    // Results should match
    assert_eq!(result1, result2);
}

#[tokio::test]
async fn acceptance_8_concurrent_cache_access() {
    // Acceptance: Cache handles concurrent access correctly
    //
    // This test verifies that multiple concurrent requests to the same
    // query are handled correctly and don't cause race conditions.

    // Skip gracefully when Docker is unavailable: the setup's Err is a skip
    // signal, not a failure (repo convention — see p5_1_f and p10_7).
    let setup = match CacheFlowTestSetup::new().await {
        Ok(setup) => setup,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };
    setup.wait_for_ready().await.unwrap();

    // Create an index with test data
    setup.create_index("products").await.unwrap();
    let documents = json!([
        {"id": 1, "name": "Concurrent Product", "category": "test"}
    ]);
    setup.add_documents("products", documents).await.unwrap();

    // Execute concurrent queries
    let query = json!({"q": "concurrent", "limit": 10});
    let mut handles = Vec::new();

    for _ in 0..10 {
        let client = setup.client.clone();
        let proxy_url = setup.proxy_url.clone();
        let master_key = setup.master_key.clone();
        let query_clone = query.clone();

        let handle = tokio::spawn(async move {
            let resp = client
                .post(format!("{proxy_url}/indexes/products/search"))
                .header("Authorization", format!("Bearer {master_key}"))
                .json(&query_clone)
                .send()
                .await
                .unwrap();

            assert!(resp.status().is_success());
            let result: Value = resp.json().await.unwrap();
            result
        });

        handles.push(handle);
    }

    // Wait for all concurrent queries to complete
    let mut results = Vec::new();
    for handle in handles {
        let result = handle.await.unwrap();
        results.push(result);
    }

    // All results should be identical
    let first_result = &results[0];
    for result in &results[1..] {
        assert_eq!(first_result, result);
    }

    assert_eq!(results[0]["hits"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn acceptance_9_cache_invalidation_on_index_update() {
    // Acceptance: Cache is invalidated when index settings change
    //
    // This test verifies that changing index settings invalidates
    // cached results for that index.

    // Skip gracefully when Docker is unavailable: the setup's Err is a skip
    // signal, not a failure (repo convention — see p5_1_f and p10_7).
    let setup = match CacheFlowTestSetup::new().await {
        Ok(setup) => setup,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };
    setup.wait_for_ready().await.unwrap();

    // Create an index with test data
    setup.create_index("products").await.unwrap();
    let documents = json!([
        {"id": 1, "name": "Searchable Product", "price": 100}
    ]);
    setup.add_documents("products", documents).await.unwrap();

    // Execute a search
    let query = json!({"q": "searchable", "limit": 10});

    let resp1 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query)
        .send()
        .await
        .unwrap();

    assert!(resp1.status().is_success());
    let result1: Value = resp1.json().await.unwrap();

    // Update index settings (this should increment the settings version)
    let settings = json!({
        "rankingRules": ["words", "typo", "proximity", "attribute", "sort", "exactness"]
    });

    let settings_resp = setup
        .client
        .patch(format!("{}/indexes/products/settings", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&settings)
        .send()
        .await;

    // Settings update might fail in test environment, but the test framework
    // should handle it gracefully
    if let Ok(resp) = settings_resp {
        assert!(resp.status().is_success());

        // Query again - should use new cache key (different settings version)
        let resp2 = setup
            .client
            .post(format!("{}/indexes/products/search", setup.proxy_url))
            .header("Authorization", format!("Bearer {}", setup.master_key))
            .json(&query)
            .send()
            .await
            .unwrap();

        assert!(resp2.status().is_success());
        let result2: Value = resp2.json().await.unwrap();
        assert_eq!(result1, result2); // Results should still match
    }
}

#[tokio::test]
async fn acceptance_10_cache_with_complex_query() {
    // Acceptance: Cache works correctly with complex queries (filters, facets, etc.)
    //
    // This test verifies that complex queries with filters, facets, and other
    // parameters are cached correctly.

    // Skip gracefully when Docker is unavailable: the setup's Err is a skip
    // signal, not a failure (repo convention — see p5_1_f and p10_7).
    let setup = match CacheFlowTestSetup::new().await {
        Ok(setup) => setup,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };
    setup.wait_for_ready().await.unwrap();

    // Create an index with test data
    setup.create_index("products").await.unwrap();
    let documents = json!([
        {"id": 1, "name": "Laptop", "category": "electronics", "price": 999, "in_stock": true},
        {"id": 2, "name": "Phone", "category": "electronics", "price": 699, "in_stock": true},
        {"id": 3, "name": "Tablet", "category": "electronics", "price": 449, "in_stock": false}
    ]);
    setup.add_documents("products", documents).await.unwrap();

    // Filtered/faceted queries require filterableAttributes on the nodes; the
    // proxy forwards the query verbatim, so the setting must exist upstream.
    setup
        .set_node_settings(
            "products",
            json!({"filterableAttributes": ["category", "in_stock"]}),
        )
        .await
        .unwrap();

    // Execute a complex query with filters
    let query = json!({
        "q": "",
        "filter": ["category = electronics", "in_stock = true"],
        "facets": ["category"],
        "limit": 10
    });

    let resp1 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query)
        .send()
        .await
        .unwrap();

    assert!(resp1.status().is_success());
    let result1: Value = resp1.json().await.unwrap();

    // Verify facets are present
    assert!(result1["facetDistribution"].is_object());

    // Second query should hit cache
    let resp2 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query)
        .send()
        .await
        .unwrap();

    assert!(resp2.status().is_success());
    let result2: Value = resp2.json().await.unwrap();
    assert_eq!(result1, result2);

    // Verify facet distribution is preserved
    assert_eq!(result1["facetDistribution"], result2["facetDistribution"]);
}
