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
//! `check_docker_available` tries the default `/var/run/docker.sock` first;
//! where that socket does not exist, podman rootless works instead:
//!   systemctl --user start podman.socket
//!   DOCKER_HOST=unix:///run/user/1001/podman/podman.sock cargo test \
//!     -p miroir-proxy --test p13_13_cache_flow_integration
//! (an explicit DOCKER_HOST is trusted, letting testcontainers surface any
//! connection failure itself.)
//!
//! Live runs hard-bind host ports 17770 (client) and 9090 (Prometheus), and
//! `PROXY_SLOT` serializes only this process — a `miroir-proxy` left running
//! outside the suite (e.g. a scratch debugging session) holds both ports and
//! poisons the readiness check. Check `ss -ltnp | grep -E '17770|9090'`
//! before a live run; with one held, use MIROIR_TEST_SKIP_DOCKER=1 instead.

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

/// Fixed Prometheus listener port (`main.rs` binds 0.0.0.0:9090 regardless of
/// `server.bind`); the reason the whole suite serializes on `PROXY_SLOT`.
const PROXY_METRICS_PORT: u16 = 9090;

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
/// defaults are written; every other field keeps its serde default
/// (`replica_groups` 1, sqlite task store, result cache `enabled` +
/// `max_size`). The exception is `result_cache.ttl_ms`, written at the
/// default 500 anyway: a pin, not an override — the expiry and hit-timing
/// asserts below (acceptance_7's 600 ms sleep, every "well inside the
/// TTL" repeat) are calibrated to it, so it must not start tracking
/// future default drift.
///
/// Topology note: `MiroirConfig::validate` requires a redis task store (plus
/// leader election) once `replication_factor > 1` or `replica_groups > 1`,
/// and the sqlite store this test can actually use is single-writer — so the
/// proxy runs with RF 1 and a single replica group (all nodes in group 0).
/// The sqlite db path is pointed inside the config dir because the default
/// (`/data/miroir-tasks.db`) is not writable here, and `search_ui` is
/// disabled because the real binary refuses to start with it enabled but no
/// JWT secret configured. `cdc.buffer.overflow` is pinned to `drop` because
/// the default (`redis`) fails validation against a sqlite task store — the
/// same pairing the config crate's own dev fixture uses.
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
         nodes:\n{nodes}\n\
         server:\n  bind: 127.0.0.1\n  port: {PROXY_PORT}\n\
         health:\n  interval_ms: 200\n  timeout_ms: 1000\n\
         task_store:\n  path: {}\n\
         result_cache:\n  ttl_ms: 500\n\
         cdc:\n  buffer:\n    overflow: drop\n\
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

/// Read one unlabelled Prometheus series value from the proxy's metrics
/// listener (`main.rs` binds it on 0.0.0.0:9090 regardless of `server.bind`).
///
/// The name must match a whole exposition token, so
/// `miroir_scatter_fan_out_size_count` cannot collide with the same
/// histogram's `_bucket`/`_sum` lines, nor with `# HELP`/`# TYPE` metadata.
/// An absent series reads as 0 — the counter floor — so a proxy that stops
/// exposing the metric, or a foreign process serving the port, surfaces as a
/// failed delta assertion in the caller, never as a silently-passing check.
/// (A dead endpoint never reaches the fallback: it errors the read outright.)
async fn proxy_counter(client: &Client, metric: &str) -> anyhow::Result<u64> {
    let body = client
        .get(format!("http://127.0.0.1:{PROXY_METRICS_PORT}/metrics"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    for line in body.lines() {
        let mut tokens = line.split_whitespace();
        if tokens.next() == Some(metric) {
            if let Some(value) = tokens.next().and_then(|v| v.parse::<u64>().ok()) {
                return Ok(value);
            }
        }
    }
    Ok(0)
}

/// `X-Miroir-Settings-Version` from a search response, absent reading as 0:
/// the proxy omits the header entirely until the first settings-broadcast
/// commit lifts the global version above 0 (`routes/search.rs` adds it
/// whenever the version is > 0, on the cached and scattered paths alike).
/// Only acceptance_9 needs it: its deltas are only a valid invalidation pin
/// if the broadcast actually bumped the version, and the header pair is what
/// observes that bump.
fn response_settings_version(resp: &reqwest::Response) -> u64 {
    resp.headers()
        .get("X-Miroir-Settings-Version")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
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

    // Counter baselines: the acceptance is pinned by deltas against these,
    // not by body equality — identical rows come back on both paths whether
    // or not the cache short-circuits the fan-out.
    let hits_before = proxy_counter(&setup.client, "miroir_result_cache_hits_total")
        .await
        .unwrap();
    let scatter_before = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();

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

    // The uncached query must fan out exactly once: a flat count means the
    // fan-out never ran (unwired metric, or the miss path wrongly
    // short-circuited), a doubled one a retry/double scatter — either
    // invalidates the bypass comparison below.
    let scatter_after_first = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();
    assert_eq!(
        scatter_after_first,
        scatter_before + 1,
        "the uncached first query must trigger exactly one scatter-gather fan-out"
    );

    // Second identical query - should hit cache and bypass scatter-gather
    let resp2 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query1)
        .send()
        .await
        .unwrap();

    assert!(resp2.status().is_success());
    let result2: Value = resp2.json().await.unwrap();

    // Cached body must round-trip unchanged; on its own this cannot detect a
    // bypassed cache (both paths return the same rows) — the deltas below do.
    assert_eq!(result1, result2);

    // The repeat query must be recorded as a hit, not folded into a fresh
    // miss. The 500 ms ttl_ms in the generated config is load-bearing: the
    // entry is written during query 1 and hits never refresh it, so a host
    // stall past the TTL reads here exactly like a lookup regression.
    let hits_after = proxy_counter(&setup.client, "miroir_result_cache_hits_total")
        .await
        .unwrap();
    assert_eq!(
        hits_after,
        hits_before + 1,
        "the repeat query must be served as a cache hit, not recorded as a miss"
    );

    // A served hit must short-circuit the fan-out: a hit that still scatters
    // is exactly the bypass regression this test pins, and the identical
    // bodies above cannot expose it — only this flat counter can.
    let scatter_after_second = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();
    assert_eq!(
        scatter_after_second, scatter_after_first,
        "a served cache hit must bypass the scatter-gather fan-out"
    );
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

    // Counter baselines: the miss-side acceptance is pinned by deltas against
    // these — body equality alone holds whether the flow fanned out or was
    // served from a cache.
    let misses_before = proxy_counter(&setup.client, "miroir_result_cache_misses_total")
        .await
        .unwrap();
    let hits_before = proxy_counter(&setup.client, "miroir_result_cache_hits_total")
        .await
        .unwrap();
    let scatter_before = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();

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

    // The uncached query is recorded as a miss and triggers the full
    // scatter-gather fan-out — the two halves of this acceptance. A flat miss
    // count means the query was wrongly served as a hit (or a lookup error
    // recorded neither counter); a flat or doubled scatter count means the
    // miss path short-circuited or scattered twice.
    let misses_after_first = proxy_counter(&setup.client, "miroir_result_cache_misses_total")
        .await
        .unwrap();
    assert_eq!(
        misses_after_first,
        misses_before + 1,
        "the uncached query must be recorded as a cache miss"
    );
    let scatter_after_first = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();
    assert_eq!(
        scatter_after_first,
        scatter_before + 1,
        "a cache miss must trigger the scatter-gather fan-out"
    );

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
    // Body equality holds on both paths (see acceptance_1); the hit delta
    // below is what proves the miss actually filed a serving entry.
    assert_eq!(result, result2);
    // The 500 ms ttl_ms is load-bearing as in acceptance_1 — the entry is
    // written during the first query and hits never refresh it, so a host
    // stall past the TTL reads here exactly like a store regression.
    let hits_after_second = proxy_counter(&setup.client, "miroir_result_cache_hits_total")
        .await
        .unwrap();
    assert_eq!(
        hits_after_second,
        hits_before + 1,
        "the entry stored after the miss must serve the repeat query as a hit"
    );

    // The served hit must short-circuit the fan-out here too: a hit that still
    // scatters passes every delta above (the hit is recorded, the bodies
    // identical), so without this flat counter the bypass regression would
    // surface only in acceptance_1's body.
    let scatter_after_second = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();
    assert_eq!(
        scatter_after_second, scatter_after_first,
        "a served cache hit must bypass the scatter-gather fan-out"
    );
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

    // Counter baselines: the caching acceptance is pinned by deltas against
    // these — identical bodies come back whether the scatter-gather result
    // was stored and replayed or the repeat re-ran it as a second miss.
    let hits_before = proxy_counter(&setup.client, "miroir_result_cache_hits_total")
        .await
        .unwrap();
    let scatter_before = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();

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

    // The scatter-gather must run exactly once before there is anything to
    // cache: a flat count means the query short-circuited or the fan-out is
    // unwired, a doubled one a retry — either poisons the repeat comparison
    // below. Pinning this read also keeps the flat assert after the repeat
    // from holding vacuously.
    let scatter_after_first = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();
    assert_eq!(
        scatter_after_first,
        scatter_before + 1,
        "the query must trigger exactly one scatter-gather fan-out"
    );

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

    // Results should match exactly; on its own this cannot detect a bypassed
    // cache (both paths return the same rows) — the deltas below do.
    assert_eq!(result1, result2);
    // All 3 books contain "rust": a scatter-gather merge that drops or
    // duplicates rows changes this count on the very body being cached.
    assert_eq!(hit_count, 3);

    // The entry stored after the scatter-gather must serve the repeat as a
    // hit: an insert that never landed or was evicted early leaves the
    // repeat a second miss. The 500 ms ttl_ms is load-bearing as in
    // acceptance_1 — the entry is written during query 1 and hits never
    // refresh it, so a host stall past the TTL reads here exactly like a
    // store regression.
    let hits_after = proxy_counter(&setup.client, "miroir_result_cache_hits_total")
        .await
        .unwrap();
    assert_eq!(
        hits_after,
        hits_before + 1,
        "the result stored after scatter-gather must serve the repeat query as a cache hit"
    );

    // A served hit must short-circuit the fan-out: a hit that still scatters
    // passes every bound above (the hit is recorded, the bodies identical),
    // so without this flat counter the bypass regression would surface only
    // in acceptance_1's body.
    let scatter_after_second = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();
    assert_eq!(
        scatter_after_second, scatter_after_first,
        "a served cache hit must bypass the scatter-gather fan-out"
    );
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

    // Counter baselines: the upstream-load acceptance is pinned by deltas
    // against these — the identical-bodies loop below holds whether or not
    // the cache actually reduces the fan-outs.
    let hits_before = proxy_counter(&setup.client, "miroir_result_cache_hits_total")
        .await
        .unwrap();
    let scatter_before = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();

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

    // All results should be identical. Body equality holds on both paths
    // (see acceptance_1): the repeats return the stored body, a re-scatter
    // reproduces it — the deltas below are what count the upstream calls.
    for result in &results[1..] {
        assert_eq!(results[0], *result);
    }

    // Four of the five queries must be served as hits: query 1 misses and
    // stores, queries 2-5 replay the entry. A repeat that leaks past the
    // cache — entry never stored, evicted early, or a host stall past the
    // 500 ms ttl_ms, which is anchored at the query-1 insert and never
    // refreshed on a hit — is recorded as a miss instead and this delta
    // falls short.
    let hits_after = proxy_counter(&setup.client, "miroir_result_cache_hits_total")
        .await
        .unwrap();
    assert_eq!(
        hits_after,
        hits_before + 4,
        "four of the five identical queries must be served as cache hits"
    );

    // Without caching this loop fans out five times (5 x 3 = 15 upstream node
    // calls); with it, exactly once. This count pins the acceptance on its
    // own: a hit that still scatters passes the hits delta above (the hit is
    // recorded), and a short-circuited first query leaves the count flat —
    // either moves this off the single fan-out.
    let scatter_after = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();
    assert_eq!(
        scatter_after,
        scatter_before + 1,
        "only the first of the five identical queries may fan out to the Meilisearch nodes"
    );
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

    // query4 shares query2's text and differs only by limit: the pair that
    // makes the limit dimension of the cache key load-bearing. A key built
    // from the query text alone collides these two, query4 arrives as a hit
    // replayed from query2's entry, and the miss delta below falls short —
    // no row truncation needed, the delta fires on the collision itself.
    let query4 = json!({"q": "desk", "limit": 20});

    // Counter baselines: the distinct-key acceptance is pinned by deltas
    // against these. The row asserts below cannot see a collision that
    // serves equal rows, and the repeat bodies hold whether the entries
    // replay or the repeats re-run as second misses.
    let misses_before = proxy_counter(&setup.client, "miroir_result_cache_misses_total")
        .await
        .unwrap();
    let hits_before = proxy_counter(&setup.client, "miroir_result_cache_hits_total")
        .await
        .unwrap();
    let scatter_before = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();

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

    let resp4 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query4)
        .send()
        .await
        .unwrap();

    assert!(resp1.status().is_success());
    assert!(resp2.status().is_success());
    assert!(resp3.status().is_success());
    assert!(resp4.status().is_success());

    let result1: Value = resp1.json().await.unwrap();
    let result2: Value = resp2.json().await.unwrap();
    let result3: Value = resp3.json().await.unwrap();
    let result4: Value = resp4.json().await.unwrap();

    // Four distinct queries must each land as their own miss: the queries
    // run sequentially, so under a key collision the colliding query is a
    // hit replayed from the entry it merged with and this delta falls
    // short — query 2 against query 1's entry under a text-ignoring key,
    // query 4 against query 2's under a limit-ignoring key. This is the
    // bound that stays observable when a collision serves equal rows
    // (query 4's rows equal query 2's) — the row asserts below go blind
    // there.
    let misses_after_first_pass = proxy_counter(&setup.client, "miroir_result_cache_misses_total")
        .await
        .unwrap();
    assert_eq!(
        misses_after_first_pass,
        misses_before + 4,
        "each of the four distinct queries must be its own cache miss; a hit means a foreign entry was served"
    );

    // Each distinct query must fan out exactly once: a flat count means one
    // of the four short-circuited or the fan-out is unwired, a doubled one
    // a retry — either poisons the flat hit-bypass comparison at the end.
    let scatter_after_first_pass =
        proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
            .await
            .unwrap();
    assert_eq!(
        scatter_after_first_pass,
        scatter_before + 4,
        "each of the four uncached queries must fan out to the Meilisearch nodes exactly once"
    );

    // Distinct queries must return distinct rows. A collision merging query
    // 2 into query 1's entry makes the first pair go equal, so this does
    // fire — but only while the collided entries carry different rows; the
    // miss delta above is the detector that survives equal rows.
    assert_ne!(result1["hits"], result2["hits"]);
    assert_ne!(result2["hits"], result3["hits"]);
    // No row assert separates query 4 from query 2: one Desk document means
    // neither limit truncates and the two row sets are equal by
    // construction — the limit dimension of the key is pinned by the miss
    // delta above alone.

    // Repeat all four queries — each must be served from its own entry
    let resp1_cached = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query1)
        .send()
        .await
        .unwrap();

    let resp2_cached = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query2)
        .send()
        .await
        .unwrap();

    let resp3_cached = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query3)
        .send()
        .await
        .unwrap();

    let resp4_cached = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query4)
        .send()
        .await
        .unwrap();

    assert!(resp1_cached.status().is_success());
    assert!(resp2_cached.status().is_success());
    assert!(resp3_cached.status().is_success());
    assert!(resp4_cached.status().is_success());

    let result1_cached: Value = resp1_cached.json().await.unwrap();
    let result2_cached: Value = resp2_cached.json().await.unwrap();
    let result3_cached: Value = resp3_cached.json().await.unwrap();
    let result4_cached: Value = resp4_cached.json().await.unwrap();

    // The repeats must round-trip their stored bodies. Equality holds on
    // both paths for an identical repeat (a re-scatter reproduces the rows,
    // see acceptance_1); what this does catch is a repeat served out of a
    // foreign entry — another query's rows fail the compare. (For pair 4
    // this is blind by construction: query 4's stored rows equal query 2's,
    // so a serve out of query 2's entry reads identical — the first-pass
    // miss delta is what keeps the two entries distinct.)
    assert_eq!(result1, result1_cached);
    assert_eq!(result2, result2_cached);
    assert_eq!(result3, result3_cached);
    assert_eq!(result4, result4_cached);

    // All four repeats must be recorded as hits: each query filed its own
    // entry in the first pass, and the 500 ms ttl_ms is load-bearing as in
    // acceptance_1 — hits never refresh an entry, so a host stall past the
    // TTL reads here exactly like a lookup regression.
    let hits_after = proxy_counter(&setup.client, "miroir_result_cache_hits_total")
        .await
        .unwrap();
    assert_eq!(
        hits_after,
        hits_before + 4,
        "each of the four repeats must be served as a hit from its own cache entry"
    );

    // A served hit must short-circuit the fan-out: a hit that still scatters
    // passes the hit delta and the identical bodies above, so only this flat
    // counter exposes the bypass regression.
    let scatter_after_repeats = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();
    assert_eq!(
        scatter_after_repeats, scatter_after_first_pass,
        "a served cache hit must bypass the scatter-gather fan-out"
    );
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

    // Counter baselines: with the in-process ResultCache there is no failure
    // to inject here — every get/insert path returns Ok (result_cache.rs), so
    // real failure injection stays unit-level in
    // p13_12_cache_failure_handling.rs. What this test can pin end-to-end is
    // the flow an uncached request takes through the cache layer; the deltas
    // below, not the body asserts (which hold on any healthy proxy), are its
    // regression sensitivity.
    let misses_before = proxy_counter(&setup.client, "miroir_result_cache_misses_total")
        .await
        .unwrap();
    let scatter_before = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();

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

    // The request must survive the cache layer as a clean miss into a full
    // fan-out: a flat miss delta means the lookup never ran (a cache-layer
    // regression that aborts or bypasses accounting while still serving), and
    // a flat scatter delta means the graceful path served without gathering.
    // A failure that aborted the request outright already fails the status
    // assert above — these pin the degrade-and-continue shape.
    let misses_after = proxy_counter(&setup.client, "miroir_result_cache_misses_total")
        .await
        .unwrap();
    assert_eq!(
        misses_after,
        misses_before + 1,
        "the uncached query must be recorded as a cache miss, not lost at the cache layer"
    );
    let scatter_after = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();
    assert_eq!(
        scatter_after,
        scatter_before + 1,
        "the request must continue into a full scatter-gather fan-out despite the cache layer"
    );
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

    // Counter baselines: the expiry acceptance is pinned by deltas against
    // these — identical bodies hold whether the repeat was re-scattered or
    // served from an entry that never expired.
    let misses_before = proxy_counter(&setup.client, "miroir_result_cache_misses_total")
        .await
        .unwrap();
    let scatter_before = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();

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

    // Wait for cache to expire (TTL is 500ms in test config). The sleep is
    // measured from query 1's completion — the entry is inserted at its
    // scatter end — so 600 ms always overshoots the 500 ms ttl_ms, and a
    // host stall only makes the entry more expired, never less.
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

    // Results should match. On its own this cannot detect an entry that
    // outlived its TTL — both paths return the same rows — the deltas below
    // do.
    assert_eq!(result1, result2);

    // The repeat must miss again: query 1 is the cold miss, and the repeat
    // looks up an entry written before the 600 ms sleep — past the
    // 500 ms ttl_ms, and an expired entry is a miss by definition. A repeat
    // served as a hit means the entry survived its TTL (expiry broken) and
    // this delta falls short at +1.
    let misses_after = proxy_counter(&setup.client, "miroir_result_cache_misses_total")
        .await
        .unwrap();
    assert_eq!(
        misses_after,
        misses_before + 2,
        "the post-TTL repeat must be a cache miss, not a hit on the expired entry"
    );

    // The expired lookup must re-scatter: a repeat served from an unexpired
    // entry bypasses the fan-out entirely and leaves this flat at +1.
    let scatter_after = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();
    assert_eq!(
        scatter_after,
        scatter_before + 2,
        "the post-TTL repeat must trigger a fresh scatter-gather fan-out"
    );
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

    // No cache-counter bounds in this test, deliberately: ten identical
    // queries fired concurrently land as an arbitrary mix of coalesced
    // waits, hits, and misses (the proxy coalesces in-flight duplicates),
    // so any hit/miss split would flake the run. What this acceptance pins
    // is response consistency under that race — the identical-body compare
    // below.

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

    // Counter baselines: the invalidation acceptance is pinned by deltas
    // against these — identical bodies hold whether the post-change repeat
    // was a fresh miss or a stale entry served under the old version.
    let misses_before = proxy_counter(&setup.client, "miroir_result_cache_misses_total")
        .await
        .unwrap();
    let scatter_before = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();

    let resp1 = setup
        .client
        .post(format!("{}/indexes/products/search", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&query)
        .send()
        .await
        .unwrap();

    assert!(resp1.status().is_success());
    let version1 = response_settings_version(&resp1);
    let result1: Value = resp1.json().await.unwrap();

    // Update index settings (this should increment the settings version)
    let settings = json!({
        "rankingRules": ["words", "typo", "proximity", "attribute", "sort", "exactness"]
    });

    // The broadcast must actually be reached: swallowing a transport error
    // here used to skip the entire acceptance while the test still passed.
    // A proxy that answered query 1 but won't take the PATCH is a broken
    // run, not a pass — and the status itself must be success, because the
    // cache key is versioned by the commit a successful broadcast performs.
    let settings_resp = setup
        .client
        .patch(format!("{}/indexes/products/settings", setup.proxy_url))
        .header("Authorization", format!("Bearer {}", setup.master_key))
        .json(&settings)
        .send()
        .await
        .unwrap();
    assert!(
        settings_resp.status().is_success(),
        "the settings broadcast must succeed: its commit is what versions the cache key"
    );

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
    let version2 = response_settings_version(&resp2);
    let result2: Value = resp2.json().await.unwrap();

    // The bump the deltas below lean on must be observed, not assumed: a 2xx
    // PATCH commits a +1 settings-version bump (`indexes.rs` Phase 3) and
    // every search response reports the version it was served under, so this
    // pair pins the commit itself. The commit is awaited inside the PATCH
    // handler before its 2xx is returned, so the bump is ordered before the
    // repeat below — this assert cannot flake against a late commit.
    // Without it a broadcast that stalls past query 1's 500 ms ttl_ms
    // (e.g. one hash-mismatch repair, whose backoff
    // is >=1 s) lets the entry expire naturally, the repeat misses either
    // way, and the miss/scatter deltas below pass with the invalidation
    // regression present. Absent reads as 0 — the proxy omits the header
    // while the version is still 0, which is exactly the pre-broadcast state
    // query 1 was served under.
    assert_eq!(
        version2,
        version1 + 1,
        "the settings broadcast must bump the settings version the search responses report"
    );

    // Results should still match. On its own this cannot detect a stale
    // entry surviving the settings change — one document means both paths
    // return the same row — the deltas below do.
    assert_eq!(result1, result2);

    // The repeat must miss: the broadcast commit bumped the settings
    // version inside the cache key, so the lookup cannot see query 1's
    // entry. A hit means the stale entry survived invalidation and this
    // delta falls short at +1 (query 1 is the cold miss).
    let misses_after = proxy_counter(&setup.client, "miroir_result_cache_misses_total")
        .await
        .unwrap();
    assert_eq!(
        misses_after,
        misses_before + 2,
        "the repeat after a settings commit must miss into a fresh cache key, not hit the stale entry"
    );

    // A stale hit would also have bypassed the fan-out: the re-scatter pins
    // the same invalidation regression from the upstream-load side.
    let scatter_after = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();
    assert_eq!(
        scatter_after,
        scatter_before + 2,
        "the post-invalidation repeat must re-scatter instead of serving the stale entry"
    );
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

    // Counter baselines: the caching half of the acceptance is pinned by
    // deltas against these — identical bodies hold whether the repeat was
    // replayed from the entry or re-ran as a second miss.
    let hits_before = proxy_counter(&setup.client, "miroir_result_cache_hits_total")
        .await
        .unwrap();
    let scatter_before = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();

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

    // The complex query must fan out exactly once before there is anything
    // to cache: a flat count means the query short-circuited or the fan-out
    // is unwired, a doubled one a retry — either poisons the repeat
    // comparison below. (acceptance_1's pin, repeated for the
    // filtered/faceted shape.)
    let scatter_after_first = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();
    assert_eq!(
        scatter_after_first,
        scatter_before + 1,
        "the complex query must trigger exactly one scatter-gather fan-out"
    );

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

    // The repeat must round-trip the stored body, facet distribution
    // included. Equality holds on both paths for an identical repeat (a
    // re-scatter reproduces the rows) — the deltas below are what pin the
    // "cached" half of the acceptance.
    assert_eq!(result1, result2);

    // The repeat must be served as a hit: query 1 filed the complex body —
    // filter, facets, and empty q all canonicalized into the key — at its
    // scatter end, and the repeat lands well inside the 500 ms ttl_ms,
    // which is anchored at that insert as in acceptance_1. A repeat that
    // misses means the complex query never stored or looked up under a
    // different key than it stored, and this delta falls short. (The old
    // facetDistribution self-compare could not do this: it is implied by
    // the whole-body equality above and can never fail on its own.)
    let hits_after = proxy_counter(&setup.client, "miroir_result_cache_hits_total")
        .await
        .unwrap();
    assert_eq!(
        hits_after,
        hits_before + 1,
        "the identical complex repeat must be served as a cache hit"
    );

    // A served hit must short-circuit the fan-out: a hit that still
    // scatters passes the hit delta and the identical bodies above, so only
    // this flat counter exposes the bypass regression.
    let scatter_after = proxy_counter(&setup.client, "miroir_scatter_fan_out_size_count")
        .await
        .unwrap();
    assert_eq!(
        scatter_after, scatter_after_first,
        "a served cache hit must bypass the scatter-gather fan-out"
    );
}
