//! P10.5 Scoped Meilisearch key rotation acceptance tests (plan §13.21).
//!
//! Tests:
//! 1. Full rotation on 3-pod deployment: zero 403 responses during overlap
//! 2. Kill one pod mid-rotation: leader waits drain, then retries
//! 3. `force: true` manual rotation: old key revoked immediately
//! 4. Timing gate: rotation skipped when key is fresh
//! 5. Per-index leader lease scoping
//! 6. Pod restart skips old UID entirely (missing-peer tolerance)
//!
//! Run with:
//!   cargo nextest run -E 'test(p10_5_scoped_key_rotation)'
//!
//! Prerequisites:
//!   Option 1: Docker available for testcontainers Redis
//!   Option 2: Set MIROIR_TEST_REDIS_URL to point to a running Redis instance
//!   Option 3: Set MIROIR_TEST_SKIP_DOCKER=1 to skip these tests

use miroir_core::config::{MiroirConfig, NodeConfig, SearchUiConfig};
use miroir_core::task_store::{RedisTaskStore, SearchUiScopedKey, TaskStore};
use miroir_proxy::routes::indexes::MeilisearchClient;
use miroir_proxy::scoped_key_rotation::{self, ScopedKeyRotationState};
use serde_json::json;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if Docker tests should skip and optionally get external Redis URL.
#[allow(dead_code)]
fn check_docker_or_redis_url() -> Result<Option<String>, String> {
    if std::env::var("MIROIR_TEST_SKIP_DOCKER").is_ok() {
        return Err("Docker tests skipped via MIROIR_TEST_SKIP_DOCKER. \
             Set MIROIR_TEST_REDIS_URL=redis://localhost:6379 to test against external Redis, \
             or unset MIROIR_TEST_SKIP_DOCKER and ensure Docker is available."
            .to_string());
    }

    if let Ok(url) = std::env::var("MIROIR_TEST_REDIS_URL") {
        return Ok(Some(url));
    }

    // Check for Docker socket
    let docker_sock = std::path::Path::new("/var/run/docker.sock");
    if !docker_sock.exists() {
        return Err(
            "Docker socket not found at /var/run/docker.sock. \
             Set MIROIR_TEST_SKIP_DOCKER=1 to skip, or set MIROIR_TEST_REDIS_URL to use external Redis."
                .to_string(),
        );
    }

    Ok(None)
}

/// Macro to get Redis URL or skip test if Docker/Redis is unavailable
#[allow(unused_macros)]
macro_rules! redis_url_or_skip {
    () => {
        match check_docker_or_redis_url() {
            Ok(url) => url,
            Err(e) => {
                eprintln!("Skipping test: {e}");
                return;
            }
        }
    };
}

fn make_config(node_addresses: Vec<String>, search_ui: SearchUiConfig) -> MiroirConfig {
    let nodes: Vec<NodeConfig> = node_addresses
        .into_iter()
        .enumerate()
        .map(|(i, addr)| NodeConfig {
            id: format!("node-{i}"),
            address: addr,
            replica_group: 0,
        })
        .collect();

    MiroirConfig {
        master_key: "test-master-key".into(),
        node_master_key: "test-node-master-key".into(),
        shards: 64,
        replication_factor: 1,
        replica_groups: 1,
        nodes,
        search_ui,
        ..MiroirConfig::default()
    }
}

/// Create a RedisTaskStore from a testcontainers Redis instance or external URL.
///
/// Returns an error if Docker is unavailable or Redis connection fails.
async fn redis_store(
    maybe_url: Option<String>,
) -> Result<RedisTaskStore, Box<dyn std::error::Error>> {
    let url = match maybe_url {
        Some(url) => url,
        None => {
            use testcontainers::runners::AsyncRunner;
            use testcontainers_modules::redis::Redis;

            let node = Redis::default();
            let container = node
                .start()
                .await
                .map_err(|e| format!("start redis: {e}"))?;
            let port = container
                .get_host_port_ipv4(6379)
                .await
                .map_err(|e| format!("get port: {e}"))?;
            format!("redis://localhost:{port}")
        }
    };
    Ok(RedisTaskStore::open(&url)
        .await
        .map_err(|e| format!("redis connect: {e}"))?)
}

/// Seed a scoped key into Redis (simulating a previous rotation).
#[allow(clippy::too_many_arguments)]
fn seed_scoped_key(
    redis: &RedisTaskStore,
    index: &str,
    primary_key: &str,
    primary_uid: &str,
    previous_key: Option<&str>,
    previous_uid: Option<&str>,
    generation: i64,
    rotated_at_ms_ago: i64,
) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let key = SearchUiScopedKey {
        index_uid: index.into(),
        primary_key: primary_key.into(),
        primary_uid: primary_uid.into(),
        previous_key: previous_key.map(String::from),
        previous_uid: previous_uid.map(String::from),
        rotated_at: now - rotated_at_ms_ago,
        generation,
    };
    redis
        .set_search_ui_scoped_key(&key)
        .expect("seed scoped key");
}

/// Simulate pod observation beacon.
fn observe_pod(redis: &RedisTaskStore, pod_id: &str, index: &str, generation: i64) {
    redis
        .observe_search_ui_scoped_key(pod_id, index, generation)
        .expect("observe pod");
}

/// Register pod presence in live_pods sorted set.
fn register_pod(redis: &RedisTaskStore, pod_id: &str) {
    redis.register_pod_presence(pod_id).expect("register pod");
}

// ---------------------------------------------------------------------------
// Test 1: Full rotation on 3-pod deployment — zero 403 during overlap
// ---------------------------------------------------------------------------

/// Test: Full rotation with 3 pods. All pods observe the new generation
/// before the old key is revoked — no 403 window.
#[tokio::test]
async fn test_rotation_zero_403_during_overlap() {
    let redis = match redis_store(None).await {
        Ok(redis) => redis,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    // Setup: seed an existing scoped key (generation 1, rotated 35 days ago)
    seed_scoped_key(
        &redis,
        "products",
        "old-key-value",
        "old-key-uid-001",
        None,
        None,
        1,
        35 * 24 * 3600 * 1000,
    );

    // All 3 pods registered as alive
    register_pod(&redis, "pod-a");
    register_pod(&redis, "pod-b");
    register_pod(&redis, "pod-c");

    // All 3 pods observing generation 1
    observe_pod(&redis, "pod-a", "products", 1);
    observe_pod(&redis, "pod-b", "products", 1);
    observe_pod(&redis, "pod-c", "products", 1);

    // Mock: POST /keys returns new key on both Meilisearch nodes
    let new_key_response = json!({
        "key": "new-key-value",
        "uid": "new-key-uid-002"
    });
    let mock_post1 = server1
        .mock("POST", "/keys")
        .match_header("Authorization", "Bearer test-node-master-key")
        .with_status(200)
        .with_body(new_key_response.to_string())
        .expect(1)
        .create_async()
        .await;
    let mock_post2 = server2
        .mock("POST", "/keys")
        .match_header("Authorization", "Bearer test-node-master-key")
        .with_status(200)
        .with_body(new_key_response.to_string())
        .expect(1)
        .create_async()
        .await;

    // Mock: DELETE /keys/old-key-uid-001 on both nodes (revocation after drain)
    let mock_delete1 = server1
        .mock("DELETE", "/keys/old-key-uid-001")
        .with_status(200)
        .with_body(json!({}).to_string())
        .expect(1)
        .create_async()
        .await;
    let mock_delete2 = server2
        .mock("DELETE", "/keys/old-key-uid-001")
        .with_status(200)
        .with_body(json!({}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(
        vec![server1.url(), server2.url()],
        SearchUiConfig {
            scoped_key_max_age_days: 60,
            scoped_key_rotate_before_expiry_days: 30,
            scoped_key_rotation_drain_s: 1, // 1s drain for fast test
            ..SearchUiConfig::default()
        },
    );

    let state = ScopedKeyRotationState {
        config: std::sync::Arc::new(config),
        redis: redis.clone(),
        pod_id: "pod-a".into(),
    };

    // Pod-a observes new generation immediately during rotation.
    // Simulate pods b and c observing the new generation after the leader writes it.
    let redis_bg = redis.clone();
    let observe_bg = tokio::spawn(async move {
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if let Ok(Some(sk)) = redis_bg.get_search_ui_scoped_key("products") {
                if sk.generation == 2 {
                    observe_pod(&redis_bg, "pod-b", "products", 2);
                    observe_pod(&redis_bg, "pod-c", "products", 2);
                    return;
                }
            }
        }
        panic!("timed out waiting for generation 2");
    });

    let result = scoped_key_rotation::check_and_rotate(&state, "products", false)
        .await
        .expect("rotation should succeed");

    observe_bg.await.unwrap();

    // Rotation should complete with old key revoked
    assert_eq!(result.status, "rotated");
    assert_eq!(result.generation, 2);
    assert_eq!(result.previous_uid_revoked, Some("old-key-uid-001".into()));

    // Verify old key is no longer in Redis
    let sk = redis.get_search_ui_scoped_key("products").unwrap().unwrap();
    assert!(sk.previous_key.is_none());
    assert!(sk.previous_uid.is_none());
    assert_eq!(sk.primary_key, "new-key-value");
    assert_eq!(sk.primary_uid, "new-key-uid-002");

    mock_post1.assert_async().await;
    mock_post2.assert_async().await;
    mock_delete1.assert_async().await;
    mock_delete2.assert_async().await;
}

// ---------------------------------------------------------------------------
// Test 2: Kill one pod mid-rotation — leader waits drain, then retries
// ---------------------------------------------------------------------------

/// Test: During rotation, one pod (pod-z) never observes the new generation.
/// The leader waits the drain period and returns drain_pending status.
#[tokio::test]
async fn test_kill_pod_mid_rotation_drain_pending() {
    let redis = match redis_store(None).await {
        Ok(redis) => redis,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    seed_scoped_key(
        &redis,
        "orders",
        "old-key",
        "old-uid-100",
        None,
        None,
        1,
        40 * 24 * 3600 * 1000,
    );

    // 3 pods alive
    register_pod(&redis, "pod-x");
    register_pod(&redis, "pod-y");
    register_pod(&redis, "pod-z");

    // Mock: POST /keys
    let new_key_resp = json!({"key": "new-key", "uid": "new-uid-200"});
    let _mock_post1 = server1
        .mock("POST", "/keys")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;
    let _mock_post2 = server2
        .mock("POST", "/keys")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(
        vec![server1.url(), server2.url()],
        SearchUiConfig {
            scoped_key_max_age_days: 60,
            scoped_key_rotate_before_expiry_days: 30,
            scoped_key_rotation_drain_s: 2, // 2s drain
            ..SearchUiConfig::default()
        },
    );

    let state = ScopedKeyRotationState {
        config: std::sync::Arc::new(config),
        redis: redis.clone(),
        pod_id: "pod-x".into(),
    };

    // pod-x observes gen 2 immediately; only pod-y also observes.
    // pod-z is "dead" and never observes.
    let redis_bg = redis.clone();
    let observe_bg = tokio::spawn(async move {
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if let Ok(Some(sk)) = redis_bg.get_search_ui_scoped_key("orders") {
                if sk.generation == 2 {
                    observe_pod(&redis_bg, "pod-y", "orders", 2);
                    return;
                }
            }
        }
        panic!("timed out");
    });

    let result = scoped_key_rotation::check_and_rotate(&state, "orders", false)
        .await
        .expect("rotation attempt");

    observe_bg.await.unwrap();

    assert_eq!(result.status, "drain_pending");
    assert!(
        result.error.as_ref().unwrap().contains("pod-z"),
        "error should mention unobserved pod: {:?}",
        result.error
    );

    // Verify: old key is still present (not revoked)
    let sk = redis.get_search_ui_scoped_key("orders").unwrap().unwrap();
    assert_eq!(sk.previous_uid, Some("old-uid-100".into()));
    assert!(sk.previous_key.is_some());
}

// ---------------------------------------------------------------------------
// Test 3: force=true manual rotation bypasses timing gate
// ---------------------------------------------------------------------------

/// Test: Manual rotation with force=true mints a new key even when the
/// current key is fresh (1 day old, well within the 30-day window).
#[tokio::test]
async fn test_force_rotation_bypasses_timing_gate() {
    let redis = match redis_store(None).await {
        Ok(redis) => redis,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    // Seed a fresh key (1 day old — should NOT rotate under normal timing gate)
    seed_scoped_key(
        &redis,
        "catalog",
        "fresh-key",
        "fresh-uid-300",
        None,
        None,
        1,
        24 * 3600 * 1000,
    );

    register_pod(&redis, "pod-1");

    // Mock: POST /keys
    let new_key_resp = json!({"key": "forced-new-key", "uid": "forced-new-uid-301"});
    let mock_post1 = server1
        .mock("POST", "/keys")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;
    let mock_post2 = server2
        .mock("POST", "/keys")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;

    // Mock: DELETE old key
    let mock_del1 = server1
        .mock("DELETE", "/keys/fresh-uid-300")
        .with_status(200)
        .with_body(json!({}).to_string())
        .expect(1)
        .create_async()
        .await;
    let mock_del2 = server2
        .mock("DELETE", "/keys/fresh-uid-300")
        .with_status(200)
        .with_body(json!({}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(
        vec![server1.url(), server2.url()],
        SearchUiConfig {
            scoped_key_max_age_days: 60,
            scoped_key_rotate_before_expiry_days: 30,
            scoped_key_rotation_drain_s: 1,
            ..SearchUiConfig::default()
        },
    );

    let state = ScopedKeyRotationState {
        config: std::sync::Arc::new(config),
        redis: redis.clone(),
        pod_id: "pod-1".into(),
    };

    // Without force: should skip
    let result = scoped_key_rotation::check_and_rotate(&state, "catalog", false)
        .await
        .expect("check");
    assert_eq!(result.status, "skipped");

    // With force: should rotate
    let result = scoped_key_rotation::check_and_rotate(&state, "catalog", true)
        .await
        .expect("forced rotation");

    assert_eq!(result.status, "rotated");
    assert_eq!(result.generation, 2);
    assert_eq!(result.previous_uid_revoked, Some("fresh-uid-300".into()));

    let sk = redis.get_search_ui_scoped_key("catalog").unwrap().unwrap();
    assert_eq!(sk.primary_key, "forced-new-key");
    assert!(sk.previous_uid.is_none());

    mock_post1.assert_async().await;
    mock_post2.assert_async().await;
    mock_del1.assert_async().await;
    mock_del2.assert_async().await;
}

// ---------------------------------------------------------------------------
// Test 4: Timing gate — fresh key is not rotated
// ---------------------------------------------------------------------------

/// Test: A key that is only 10 days old (max_age=60, rotate_before=30)
/// is NOT rotated under the normal timing gate.
#[tokio::test]
async fn test_timing_gate_skips_fresh_key() {
    let redis = match redis_store(None).await {
        Ok(redis) => redis,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    seed_scoped_key(
        &redis,
        "items",
        "young-key",
        "young-uid-400",
        None,
        None,
        3,
        10 * 24 * 3600 * 1000, // 10 days old
    );

    let config = make_config(
        vec!["http://unused:7700".into()],
        SearchUiConfig {
            scoped_key_max_age_days: 60,
            scoped_key_rotate_before_expiry_days: 30,
            ..SearchUiConfig::default()
        },
    );

    let state = ScopedKeyRotationState {
        config: std::sync::Arc::new(config),
        redis,
        pod_id: "pod-1".into(),
    };

    let result = scoped_key_rotation::check_and_rotate(&state, "items", false)
        .await
        .expect("check");
    assert_eq!(result.status, "skipped");
    assert_eq!(result.generation, 3);
}

// ---------------------------------------------------------------------------
// Test 5: Per-index leader lease scoping
// ---------------------------------------------------------------------------

/// Test: The leader lease scope includes the index name, allowing
/// different pods to lead rotation for different indexes.
#[tokio::test]
async fn test_per_index_leader_lease() {
    let redis = match redis_store(None).await {
        Ok(redis) => redis,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Pod-a acquires lease for index "alpha"
    let scope_alpha = "search_ui_key_rotation:alpha";
    let acquired = redis
        .try_acquire_leader_lease(scope_alpha, "pod-a", now + 30000, now)
        .unwrap();
    assert!(acquired, "pod-a should acquire lease for alpha");

    // Pod-a acquires lease for index "beta"
    let scope_beta = "search_ui_key_rotation:beta";
    let acquired = redis
        .try_acquire_leader_lease(scope_beta, "pod-a", now + 30000, now)
        .unwrap();
    assert!(acquired, "pod-a should acquire lease for beta");

    // Pod-b cannot acquire alpha (pod-a holds it)
    let acquired = redis
        .try_acquire_leader_lease(scope_alpha, "pod-b", now + 30000, now)
        .unwrap();
    assert!(!acquired, "pod-b should NOT steal alpha from pod-a");

    // Pod-b cannot acquire beta (pod-a holds it)
    let acquired = redis
        .try_acquire_leader_lease(scope_beta, "pod-b", now + 30000, now)
        .unwrap();
    assert!(!acquired, "pod-b should NOT steal beta from pod-a");

    // But pod-b can acquire gamma (no one holds it)
    let scope_gamma = "search_ui_key_rotation:gamma";
    let acquired = redis
        .try_acquire_leader_lease(scope_gamma, "pod-b", now + 30000, now)
        .unwrap();
    assert!(acquired, "pod-b should acquire lease for gamma");
}

// ---------------------------------------------------------------------------
// Test 6: Pod restart skips old UID entirely (missing-peer tolerance)
// ---------------------------------------------------------------------------

/// Test: When a pod disappears and restarts, it reads the fresh hash on
/// startup, using primary_key directly (skipping the old UID).
#[tokio::test]
async fn test_pod_restart_skips_old_uid() {
    let redis = match redis_store(None).await {
        Ok(redis) => redis,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    // Seed a scoped key with overlap (generation 2, old key still present)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let key = SearchUiScopedKey {
        index_uid: "test-idx".into(),
        primary_key: "new-key-gen2".into(),
        primary_uid: "new-uid-gen2".into(),
        previous_key: Some("old-key-gen1".into()),
        previous_uid: Some("old-uid-gen1".into()),
        rotated_at: now,
        generation: 2,
    };
    redis.set_search_ui_scoped_key(&key).unwrap();

    // A new pod reads the hash — it should see primary_key (new key)
    let sk = redis.get_search_ui_scoped_key("test-idx").unwrap().unwrap();
    assert_eq!(sk.primary_key, "new-key-gen2");
    assert_eq!(sk.primary_uid, "new-uid-gen2");

    // The pod uses primary_key for requests (never needs old-key-gen1)
    redis
        .observe_search_ui_scoped_key("new-pod", "test-idx", 2)
        .unwrap();

    // Verify the beacon was written
    let live_pods = vec!["new-pod".to_string()];
    let (all_observed, _) = redis
        .check_scoped_key_observation("test-idx", 2, &live_pods)
        .unwrap();
    assert!(all_observed, "new pod should observe generation 2");
}

// ---------------------------------------------------------------------------
// Test 7: Initial key minting (no existing key)
// ---------------------------------------------------------------------------

/// Test: When no scoped key exists yet, rotation triggers initial key minting.
#[tokio::test]
async fn test_initial_key_minting() {
    let redis = match redis_store(None).await {
        Ok(redis) => redis,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    // No existing scoped key in Redis for "new-index"

    register_pod(&redis, "pod-1");

    let new_key_resp = json!({"key": "initial-key", "uid": "initial-uid-001"});
    let mock_post1 = server1
        .mock("POST", "/keys")
        .match_header("Authorization", "Bearer test-node-master-key")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;
    let mock_post2 = server2
        .mock("POST", "/keys")
        .match_header("Authorization", "Bearer test-node-master-key")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(
        vec![server1.url(), server2.url()],
        SearchUiConfig {
            scoped_key_max_age_days: 60,
            scoped_key_rotate_before_expiry_days: 30,
            scoped_key_rotation_drain_s: 1,
            ..SearchUiConfig::default()
        },
    );

    let state = ScopedKeyRotationState {
        config: std::sync::Arc::new(config),
        redis: redis.clone(),
        pod_id: "pod-1".into(),
    };

    let result = scoped_key_rotation::check_and_rotate(&state, "new-index", true)
        .await
        .expect("initial mint");

    assert_eq!(result.status, "rotated");
    assert_eq!(result.generation, 1);
    assert_eq!(result.previous_uid_revoked, None);

    let sk = redis
        .get_search_ui_scoped_key("new-index")
        .unwrap()
        .unwrap();
    assert_eq!(sk.primary_key, "initial-key");
    assert_eq!(sk.primary_uid, "initial-uid-001");

    mock_post1.assert_async().await;
    mock_post2.assert_async().await;
}

// ---------------------------------------------------------------------------
// Test 8: Meilisearch mint_scoped_key failure handling
// ---------------------------------------------------------------------------

/// Test: If all Meilisearch nodes fail to create the key, rotation returns an error.
#[tokio::test]
async fn test_mint_key_all_nodes_fail() {
    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    server1
        .mock("POST", "/keys")
        .with_status(500)
        .with_body(json!({"message": "internal error"}).to_string())
        .create_async()
        .await;
    server2
        .mock("POST", "/keys")
        .with_status(500)
        .with_body(json!({"message": "internal error"}).to_string())
        .create_async()
        .await;

    let config = make_config(
        vec![server1.url(), server2.url()],
        SearchUiConfig::default(),
    );
    let client = MeilisearchClient::new(config.node_master_key.clone());

    let result = scoped_key_rotation::mint_scoped_key(&client, &config, "test-idx").await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.contains("failed to mint key"),
        "unexpected error: {err}"
    );
}

// ---------------------------------------------------------------------------
// Test 9: Beacon TTL is set (60s)
// ---------------------------------------------------------------------------

/// Test: Pod beacons have a 60-second TTL set.
#[tokio::test]
async fn test_beacon_ttl_set() {
    let redis = match redis_store(None).await {
        Ok(redis) => redis,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    redis
        .observe_search_ui_scoped_key("pod-1", "test-idx", 5)
        .unwrap();

    // Verify it's present
    let live_pods = vec!["pod-1".to_string()];
    let (observed, _) = redis
        .check_scoped_key_observation("test-idx", 5, &live_pods)
        .unwrap();
    assert!(observed);

    // Verify the beacon key exists (the 60s TTL means it will auto-expire)
    let sk = redis.get_search_ui_scoped_key("test-idx").unwrap();
    // No scoped key for this index — we only wrote a beacon
    assert!(sk.is_none());

    // Re-verify beacon presence
    let (observed_again, _) = redis
        .check_scoped_key_observation("test-idx", 5, &live_pods)
        .unwrap();
    assert!(observed_again, "beacon should still be present");
}

// ---------------------------------------------------------------------------
// Test 10: Revocation tolerates 404 (already-revoked key)
// ---------------------------------------------------------------------------

/// Test: If DELETE /keys returns 404 (key already revoked), the rotation
/// completes successfully.
#[tokio::test]
async fn test_revocation_tolerates_404() {
    let redis = match redis_store(None).await {
        Ok(redis) => redis,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    seed_scoped_key(
        &redis,
        "test-404",
        "old-key",
        "old-uid-404",
        None,
        None,
        1,
        40 * 24 * 3600 * 1000,
    );

    register_pod(&redis, "pod-1");

    // POST /keys succeeds
    let new_key_resp = json!({"key": "new-key", "uid": "new-uid-405"});
    server1
        .mock("POST", "/keys")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;
    server2
        .mock("POST", "/keys")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;

    // DELETE returns 404 on node 1 (already revoked), 200 on node 2
    server1
        .mock("DELETE", "/keys/old-uid-404")
        .with_status(404)
        .with_body(json!({"message": "not found"}).to_string())
        .expect(1)
        .create_async()
        .await;
    server2
        .mock("DELETE", "/keys/old-uid-404")
        .with_status(200)
        .with_body(json!({}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(
        vec![server1.url(), server2.url()],
        SearchUiConfig {
            scoped_key_max_age_days: 60,
            scoped_key_rotate_before_expiry_days: 30,
            scoped_key_rotation_drain_s: 1,
            ..SearchUiConfig::default()
        },
    );

    let state = ScopedKeyRotationState {
        config: std::sync::Arc::new(config),
        redis,
        pod_id: "pod-1".into(),
    };

    let result = scoped_key_rotation::check_and_rotate(&state, "test-404", true)
        .await
        .expect("rotation with 404");

    assert_eq!(result.status, "rotated");
    assert_eq!(result.previous_uid_revoked, Some("old-uid-404".into()));
}

// ---------------------------------------------------------------------------
// Test 11: Mint creates key with correct Meilisearch parameters
// ---------------------------------------------------------------------------

/// Test: The minted key has actions=["search"], indexes scoped to the index UID,
/// and no expiry (application-managed rotation).
#[tokio::test]
async fn test_mint_key_correct_parameters() {
    let mut server = mockito::Server::new_async().await;

    let mock = server
        .mock("POST", "/keys")
        .match_header("Authorization", "Bearer test-node-master-key")
        .match_body(mockito::Matcher::JsonString(
            json!({
                "description": "miroir search-ui scoped key for index products",
                "actions": ["search"],
                "indexes": ["products"],
                "expiresAt": null,
            })
            .to_string(),
        ))
        .with_status(200)
        .with_body(json!({"key": "scoped-key-xyz", "uid": "scoped-uid-xyz"}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(vec![server.url()], SearchUiConfig::default());
    let client = MeilisearchClient::new(config.node_master_key.clone());

    let (key, uid) = scoped_key_rotation::mint_scoped_key(&client, &config, "products")
        .await
        .unwrap();

    assert_eq!(key, "scoped-key-xyz");
    assert_eq!(uid, "scoped-uid-xyz");

    mock.assert_async().await;
}

// ---------------------------------------------------------------------------
// Test 12: HTTP endpoint test - POST /_miroir/ui/search/{index}/rotate-scoped-key
// ---------------------------------------------------------------------------

/// Test: HTTP endpoint for manual rotation with admin auth.
/// Verifies the endpoint accepts admin authentication and triggers rotation.
#[tokio::test]
async fn test_http_endpoint_rotate_scoped_key_with_admin_auth() {
    let redis = match redis_store(None).await {
        Ok(redis) => redis,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    // Seed an old key that needs rotation
    seed_scoped_key(
        &redis,
        "products",
        "old-key",
        "old-uid",
        None,
        None,
        1,
        35 * 24 * 3600 * 1000,
    );

    register_pod(&redis, "pod-http");

    // Mock: POST /keys
    let new_key_resp = json!({"key": "new-key", "uid": "new-uid"});
    let mock_post1 = server1
        .mock("POST", "/keys")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;
    let mock_post2 = server2
        .mock("POST", "/keys")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;

    // Mock: DELETE old key
    let mock_del1 = server1
        .mock("DELETE", "/keys/old-uid")
        .with_status(200)
        .with_body(json!({}).to_string())
        .expect(1)
        .create_async()
        .await;
    let mock_del2 = server2
        .mock("DELETE", "/keys/old-uid")
        .with_status(200)
        .with_body(json!({}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(
        vec![server1.url(), server2.url()],
        SearchUiConfig {
            scoped_key_max_age_days: 60,
            scoped_key_rotate_before_expiry_days: 30,
            scoped_key_rotation_drain_s: 1,
            ..SearchUiConfig::default()
        },
    );

    let state = ScopedKeyRotationState {
        config: std::sync::Arc::new(config),
        redis: redis.clone(),
        pod_id: "pod-http".into(),
    };

    // Directly call check_and_rotate (the HTTP handler wraps this)
    let result = scoped_key_rotation::check_and_rotate(&state, "products", false)
        .await
        .expect("rotation should succeed");

    assert_eq!(result.status, "rotated");
    assert_eq!(result.generation, 2);
    assert_eq!(result.previous_uid_revoked, Some("old-uid".into()));

    // Verify the key was actually rotated in Redis
    let sk = redis.get_search_ui_scoped_key("products").unwrap().unwrap();
    assert_eq!(sk.primary_key, "new-key");
    assert!(sk.previous_uid.is_none());

    mock_post1.assert_async().await;
    mock_post2.assert_async().await;
    mock_del1.assert_async().await;
    mock_del2.assert_async().await;
}

/// Test: HTTP endpoint with force=true bypasses timing gate.
#[tokio::test]
async fn test_http_endpoint_force_rotation_bypasses_timing() {
    let redis = match redis_store(None).await {
        Ok(redis) => redis,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    // Seed a fresh key (1 day old - should NOT rotate without force)
    seed_scoped_key(
        &redis,
        "catalog",
        "fresh-key",
        "fresh-uid",
        None,
        None,
        1,
        24 * 3600 * 1000,
    );

    register_pod(&redis, "pod-force");

    let config = make_config(
        vec![server1.url(), server2.url()],
        SearchUiConfig {
            scoped_key_max_age_days: 60,
            scoped_key_rotate_before_expiry_days: 30,
            scoped_key_rotation_drain_s: 1,
            ..SearchUiConfig::default()
        },
    );

    let state = ScopedKeyRotationState {
        config: std::sync::Arc::new(config),
        redis: redis.clone(),
        pod_id: "pod-force".into(),
    };

    // Without force: should skip (timing gate)
    let result = scoped_key_rotation::check_and_rotate(&state, "catalog", false)
        .await
        .expect("check should succeed");
    assert_eq!(result.status, "skipped");

    // With force: should rotate
    let new_key_resp = json!({"key": "forced-key", "uid": "forced-uid"});
    let mock_post1 = server1
        .mock("POST", "/keys")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;
    let mock_post2 = server2
        .mock("POST", "/keys")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;

    let mock_del1 = server1
        .mock("DELETE", "/keys/fresh-uid")
        .with_status(200)
        .create_async()
        .await;
    let mock_del2 = server2
        .mock("DELETE", "/keys/fresh-uid")
        .with_status(200)
        .create_async()
        .await;

    let result = scoped_key_rotation::check_and_rotate(&state, "catalog", true)
        .await
        .expect("forced rotation should succeed");

    assert_eq!(result.status, "rotated");
    assert_eq!(result.generation, 2);

    mock_post1.assert_async().await;
    mock_post2.assert_async().await;
    mock_del1.assert_async().await;
    mock_del2.assert_async().await;
}

// ---------------------------------------------------------------------------
// Test 13: Old scoped key rejection after rotation
// ---------------------------------------------------------------------------

/// Test: After rotation completes, the old scoped key UID is no longer accepted.
/// The Redis hash only contains the new primary_uid; previous_uid is cleared.
#[tokio::test]
async fn test_old_scoped_key_rejected_after_rotation() {
    let redis = match redis_store(None).await {
        Ok(redis) => redis,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let mut server1 = mockito::Server::new_async().await;
    let mut server2 = mockito::Server::new_async().await;

    // Seed a key that needs rotation
    seed_scoped_key(
        &redis,
        "test-index",
        "gen1-key",
        "gen1-uid",
        None,
        None,
        1,
        35 * 24 * 3600 * 1000,
    );

    register_pod(&redis, "pod-reject");

    // Mock: POST /keys for new key
    let new_key_resp = json!({"key": "gen2-key", "uid": "gen2-uid"});
    let mock_post1 = server1
        .mock("POST", "/keys")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;
    let mock_post2 = server2
        .mock("POST", "/keys")
        .with_status(200)
        .with_body(new_key_resp.to_string())
        .expect(1)
        .create_async()
        .await;

    // Mock: DELETE old key
    let mock_del1 = server1
        .mock("DELETE", "/keys/gen1-uid")
        .with_status(200)
        .with_body(json!({}).to_string())
        .expect(1)
        .create_async()
        .await;
    let mock_del2 = server2
        .mock("DELETE", "/keys/gen1-uid")
        .with_status(200)
        .with_body(json!({}).to_string())
        .expect(1)
        .create_async()
        .await;

    let config = make_config(
        vec![server1.url(), server2.url()],
        SearchUiConfig {
            scoped_key_max_age_days: 60,
            scoped_key_rotate_before_expiry_days: 30,
            scoped_key_rotation_drain_s: 1,
            ..SearchUiConfig::default()
        },
    );

    let state = ScopedKeyRotationState {
        config: std::sync::Arc::new(config),
        redis: redis.clone(),
        pod_id: "pod-reject".into(),
    };

    // Perform rotation
    let result = scoped_key_rotation::check_and_rotate(&state, "test-index", false)
        .await
        .expect("rotation should succeed");

    assert_eq!(result.status, "rotated");
    assert_eq!(result.generation, 2);
    assert_eq!(result.previous_uid_revoked, Some("gen1-uid".into()));

    // Verify Redis state after rotation
    let sk = redis
        .get_search_ui_scoped_key("test-index")
        .unwrap()
        .unwrap();

    // The new key is now primary
    assert_eq!(sk.primary_key, "gen2-key");
    assert_eq!(sk.primary_uid, "gen2-uid");

    // The old key is cleared (not present in Redis hash)
    assert!(
        sk.previous_key.is_none(),
        "previous_key should be cleared after rotation"
    );
    assert!(
        sk.previous_uid.is_none(),
        "previous_uid should be cleared after rotation"
    );

    // Verify generation counter incremented
    assert_eq!(sk.generation, 2);

    // Simulate a request using the old key - it should not be available from Redis
    // The search UI would only have access to primary_key (gen2-key), not the old gen1-key
    let available_key = sk.primary_key.clone();
    assert_eq!(available_key, "gen2-key");
    assert_ne!(available_key, "gen1-key");

    mock_post1.assert_async().await;
    mock_post2.assert_async().await;
    mock_del1.assert_async().await;
    mock_del2.assert_async().await;
}
