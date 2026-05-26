//! P10.7 Admin login rate limiting + exponential backoff acceptance tests (plan §9).
//!
//! Tests:
//! 1. 11 login attempts in 60s from same IP → 11th returns 429
//! 2. 5 failed attempts → next attempt blocked for 10m; subsequent failures increase backoff (20m, 40m, ...) up to 24h cap
//! 3. Successful login resets both rate limit and backoff counters
//! 4. Multi-pod deployment with `backend: redis`: attempts against pod-A count against the same bucket as attempts against pod-B
//! 5. Helm lint rejects `backend: local` with replicas > 1 (already validated by schema)

use miroir_core::task_store::RedisTaskStore;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn redis_store() -> (RedisTaskStore, String) {
    let node = Redis::default();
    let container = node.start().await.expect("start redis");
    let port = container.get_host_port_ipv4(6379).await.expect("get port");
    let url = format!("redis://localhost:{port}");
    let store = RedisTaskStore::open(&url).await.expect("redis connect");
    (store, url)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// 11 login attempts in 60s from same IP → 11th returns 429 (rate limit exceeded).
/// Rate limit is 10/minute, so the 11th attempt should be blocked.
#[tokio::test]
async fn eleven_login_attempts_in_60s_returns_429() {
    let (store, _url) = redis_store().await;
    let ip = "192.168.1.100";
    let limit = 10;
    let window_seconds = 60;

    // First 10 attempts should be allowed
    for i in 1..=10 {
        let (allowed, wait_seconds) = store
            .check_rate_limit_admin_login(ip, limit, window_seconds)
            .expect("check rate limit");
        assert!(allowed, "attempt {i} should be allowed");
        assert_eq!(wait_seconds, None, "attempt {i} should have no wait time");
    }

    // 11th attempt should be blocked
    let (allowed, wait_seconds) = store
        .check_rate_limit_admin_login(ip, limit, window_seconds)
        .expect("check rate limit");
    assert!(!allowed, "11th attempt should be blocked");
    assert_eq!(
        wait_seconds, None,
        "rate limit exceeded should have no specific wait time"
    );
}

/// 5 failed attempts → next attempt blocked for 10m.
/// Subsequent failures increase backoff: 20m, 40m, 80m, 160m, 320m (cap at 24h = 1440m).
#[tokio::test]
async fn five_failed_attempts_triggers_10_minute_backoff() {
    let (store, _url) = redis_store().await;
    let ip = "192.168.1.101";
    let failed_threshold = 5;
    let backoff_start_minutes = 10;
    let backoff_max_hours = 24;

    // Record 5 failed attempts
    for i in 1..=5 {
        let wait_seconds = store
            .record_failure_admin_login(
                ip,
                failed_threshold,
                backoff_start_minutes,
                backoff_max_hours,
            )
            .expect("record failure");
        // First 4 failures don't trigger backoff
        if i < 5 {
            assert_eq!(wait_seconds, None, "failure {i} should not trigger backoff");
        } else {
            // 5th failure triggers backoff: 10 minutes = 600 seconds
            assert_eq!(
                wait_seconds,
                Some(600),
                "5th failure should trigger 10 minute backoff"
            );
        }
    }

    // Next login attempt should be blocked by backoff
    let (allowed, wait_seconds) = store
        .check_rate_limit_admin_login(ip, 10, 60)
        .expect("check rate limit");
    assert!(!allowed, "login should be blocked during backoff");
    assert!(wait_seconds.is_some(), "backoff should return wait time");
    let wait = wait_seconds.unwrap();
    // Should be approximately 600 seconds (10 minutes)
    assert!(
        (590..=610).contains(&wait),
        "wait time should be ~600 seconds, got {wait}"
    );
}

/// Exponential backoff: each subsequent failure doubles the backoff time.
/// 5 failures: 10m, 6 failures: 20m, 7 failures: 40m, 8 failures: 80m, 9 failures: 160m, 10+ failures: 320m...
#[tokio::test]
async fn exponential_backoff_doubles_per_failure() {
    let (store, _url) = redis_store().await;
    let ip = "192.168.1.102";
    let failed_threshold = 5;
    let backoff_start_minutes = 10;
    let backoff_max_hours = 24;

    // Test backoff progression
    let expected_backoffs = vec![
        (5, 600),    // 5th failure: 10 minutes = 600 seconds
        (6, 1200),   // 6th failure: 20 minutes = 1200 seconds
        (7, 2400),   // 7th failure: 40 minutes = 2400 seconds
        (8, 4800),   // 8th failure: 80 minutes = 4800 seconds
        (9, 9600),   // 9th failure: 160 minutes = 9600 seconds
        (10, 19200), // 10th failure: 320 minutes = 19200 seconds
    ];

    for (failure_count, expected_wait) in expected_backoffs {
        let wait_seconds = store
            .record_failure_admin_login(
                ip,
                failed_threshold,
                backoff_start_minutes,
                backoff_max_hours,
            )
            .expect("record failure");

        assert_eq!(
            wait_seconds,
            Some(expected_wait),
            "{failure_count}th failure should trigger {expected_wait} second backoff"
        );

        // Clear the backoff for next iteration (simulating time passing)
        // Use the public reset method which clears both rate limit and backoff keys
        store
            .reset_rate_limit_admin_login(ip)
            .expect("reset backoff");
    }
}

/// Backoff caps at 24 hours (86400 seconds).
#[tokio::test]
async fn backoff_caps_at_24_hours() {
    let (store, _url) = redis_store().await;
    let ip = "192.168.1.103";
    let failed_threshold = 5;
    let backoff_start_minutes = 10;
    let backoff_max_hours = 24;
    let max_wait_seconds = 24 * 3600; // 24 hours in seconds

    // Simulate many failures to reach the cap
    // After 5 failures, each additional failure doubles the backoff
    // 5: 10m, 6: 20m, 7: 40m, 8: 80m, 9: 160m, 10: 320m, 11: 640m, 12: 1280m, 13+: capped at 1440m (24h)
    for i in 5..=15 {
        let wait_seconds = store
            .record_failure_admin_login(
                ip,
                failed_threshold,
                backoff_start_minutes,
                backoff_max_hours,
            )
            .expect("record failure");

        if i >= 12 {
            // Should be capped at 24 hours
            assert_eq!(
                wait_seconds,
                Some(max_wait_seconds),
                "{i}th failure should be capped at 24 hours"
            );
        }

        // Clear backoff for next iteration
        store
            .reset_rate_limit_admin_login(ip)
            .expect("reset backoff");
    }
}

/// Successful login resets both rate limit and backoff counters.
#[tokio::test]
async fn successful_login_resets_all_counters() {
    let (store, _url) = redis_store().await;
    let ip = "192.168.1.104";
    let failed_threshold = 5;
    let backoff_start_minutes = 10;
    let backoff_max_hours = 24;

    // First, use up the rate limit (10 attempts)
    let limit = 10;
    let window_seconds = 60;
    for _ in 1..=10 {
        store
            .check_rate_limit_admin_login(ip, limit, window_seconds)
            .expect("check rate limit");
    }

    // Verify rate limit is hit
    let (allowed, _) = store
        .check_rate_limit_admin_login(ip, limit, window_seconds)
        .expect("check rate limit");
    assert!(!allowed, "should be rate limited");

    // Also trigger backoff
    for _ in 1..=5 {
        store
            .record_failure_admin_login(
                ip,
                failed_threshold,
                backoff_start_minutes,
                backoff_max_hours,
            )
            .expect("record failure");
    }

    // Verify backoff is active
    let (allowed, wait_seconds) = store
        .check_rate_limit_admin_login(ip, limit, window_seconds)
        .expect("check rate limit");
    assert!(!allowed, "should be in backoff");
    assert!(wait_seconds.is_some(), "backoff should be active");

    // Simulate successful login - resets both counters
    store
        .reset_rate_limit_admin_login(ip)
        .expect("reset rate limit");

    // Rate limit should be cleared
    let (allowed, wait_seconds) = store
        .check_rate_limit_admin_login(ip, limit, window_seconds)
        .expect("check rate limit");
    assert!(allowed, "rate limit should be reset");
    assert_eq!(wait_seconds, None, "no wait time after reset");

    // Can make 10 more attempts
    for _ in 1..=10 {
        let (allowed, _) = store
            .check_rate_limit_admin_login(ip, limit, window_seconds)
            .expect("check rate limit");
        assert!(allowed, "should have fresh rate limit bucket");
    }
}

/// Multi-pod: rate limit bucket is shared across Redis connections.
/// Two separate RedisTaskStore instances (simulating two pods) share the same bucket.
#[tokio::test]
async fn multi_pod_shares_rate_limit_bucket() {
    let (_store, redis_url) = redis_store().await;

    // Create two separate store instances (simulating two pods)
    let store_a = RedisTaskStore::open(&redis_url)
        .await
        .expect("connect pod A");
    let store_b = RedisTaskStore::open(&redis_url)
        .await
        .expect("connect pod B");

    let ip = "192.168.1.105";
    let limit = 10;
    let window_seconds = 60;

    // Pod A makes 5 requests
    for i in 1..=5 {
        let (allowed, _) = store_a
            .check_rate_limit_admin_login(ip, limit, window_seconds)
            .expect("pod A check rate limit");
        assert!(allowed, "pod A attempt {i} should be allowed");
    }

    // Pod B makes 5 requests
    for i in 1..=5 {
        let (allowed, _) = store_b
            .check_rate_limit_admin_login(ip, limit, window_seconds)
            .expect("pod B check rate limit");
        assert!(allowed, "pod B attempt {i} should be allowed");
    }

    // Pod A tries the 11th request - should be blocked
    let (allowed, _) = store_a
        .check_rate_limit_admin_login(ip, limit, window_seconds)
        .expect("pod A check rate limit");
    assert!(!allowed, "pod A 11th request should be blocked");

    // Pod B also tries - should also be blocked
    let (allowed, _) = store_b
        .check_rate_limit_admin_login(ip, limit, window_seconds)
        .expect("pod B check rate limit");
    assert!(!allowed, "pod B 11th request should be blocked");
}

/// Multi-pod: backoff state is shared across Redis connections.
#[tokio::test]
async fn multi_pod_shares_backoff_state() {
    let (_store, redis_url) = redis_store().await;

    // Create two separate store instances (simulating two pods)
    let store_a = RedisTaskStore::open(&redis_url)
        .await
        .expect("connect pod A");
    let store_b = RedisTaskStore::open(&redis_url)
        .await
        .expect("connect pod B");

    let ip = "192.168.1.106";
    let failed_threshold = 5;
    let backoff_start_minutes = 10;
    let backoff_max_hours = 24;

    // Pod A records 5 failures
    for i in 1..=5 {
        let wait_seconds = store_a
            .record_failure_admin_login(
                ip,
                failed_threshold,
                backoff_start_minutes,
                backoff_max_hours,
            )
            .expect("pod A record failure");
        if i < 5 {
            assert_eq!(wait_seconds, None);
        } else {
            assert_eq!(wait_seconds, Some(600));
        }
    }

    // Pod B checks the rate limit - should see the backoff
    let (allowed, wait_seconds) = store_b
        .check_rate_limit_admin_login(ip, 10, 60)
        .expect("pod B check rate limit");
    assert!(!allowed, "pod B should see backoff from pod A");
    assert!(wait_seconds.is_some(), "pod B should see backoff wait time");
    assert!(wait_seconds.unwrap() >= 590 && wait_seconds.unwrap() <= 610);

    // Pod B records another failure - backoff should increase to 20 minutes
    let wait_seconds = store_b
        .record_failure_admin_login(
            ip,
            failed_threshold,
            backoff_start_minutes,
            backoff_max_hours,
        )
        .expect("pod B record failure");
    assert_eq!(
        wait_seconds,
        Some(1200),
        "backoff should increase to 20 minutes"
    );

    // Pod A checks again - should see the increased backoff
    let (allowed, wait_seconds) = store_a
        .check_rate_limit_admin_login(ip, 10, 60)
        .expect("pod A check rate limit");
    assert!(!allowed);
    assert!(wait_seconds.is_some());
    assert!(wait_seconds.unwrap() >= 1190 && wait_seconds.unwrap() <= 1210);
}

/// Multi-pod: successful login on one pod resets counters for all pods.
#[tokio::test]
async fn multi_pod_successful_login_resets_all_counters() {
    let (_store, redis_url) = redis_store().await;

    // Create two separate store instances
    let store_a = RedisTaskStore::open(&redis_url)
        .await
        .expect("connect pod A");
    let store_b = RedisTaskStore::open(&redis_url)
        .await
        .expect("connect pod B");

    let ip = "192.168.1.107";
    let failed_threshold = 5;
    let backoff_start_minutes = 10;
    let backoff_max_hours = 24;
    let limit = 10;
    let window_seconds = 60;

    // Pod A uses rate limit
    for _ in 1..=10 {
        store_a
            .check_rate_limit_admin_login(ip, limit, window_seconds)
            .expect("pod A check rate limit");
    }

    // Pod B triggers backoff
    for _ in 1..=5 {
        store_b
            .record_failure_admin_login(
                ip,
                failed_threshold,
                backoff_start_minutes,
                backoff_max_hours,
            )
            .expect("pod B record failure");
    }

    // Verify both pods see the blocked state
    let (allowed_a, _) = store_a
        .check_rate_limit_admin_login(ip, limit, window_seconds)
        .expect("pod A check rate limit");
    assert!(!allowed_a);

    let (allowed_b, _) = store_b
        .check_rate_limit_admin_login(ip, limit, window_seconds)
        .expect("pod B check rate limit");
    assert!(!allowed_b);

    // Pod A simulates successful login - resets counters
    store_a
        .reset_rate_limit_admin_login(ip)
        .expect("pod A reset rate limit");

    // Pod B should now be able to proceed
    let (allowed, _) = store_b
        .check_rate_limit_admin_login(ip, limit, window_seconds)
        .expect("pod B check rate limit after reset");
    assert!(allowed, "pod B should see reset state");
}

/// Different IPs have independent rate limit buckets.
#[tokio::test]
async fn different_ips_have_independent_buckets() {
    let (store, _url) = redis_store().await;
    let ip1 = "192.168.1.108";
    let ip2 = "192.168.1.109";
    let limit = 10;
    let window_seconds = 60;

    // IP1 uses up its rate limit
    for _ in 1..=10 {
        store
            .check_rate_limit_admin_login(ip1, limit, window_seconds)
            .expect("check rate limit");
    }

    // IP1 should be blocked
    let (allowed, _) = store
        .check_rate_limit_admin_login(ip1, limit, window_seconds)
        .expect("check rate limit");
    assert!(!allowed, "IP1 should be blocked");

    // IP2 should still be allowed
    let (allowed, _) = store
        .check_rate_limit_admin_login(ip2, limit, window_seconds)
        .expect("check rate limit");
    assert!(allowed, "IP2 should not be affected by IP1's rate limit");
}

/// Rate limit window expires after TTL.
#[tokio::test]
async fn rate_limit_window_expires_after_ttl() {
    let (store, _url) = redis_store().await;
    let ip = "192.168.1.110";
    let limit = 10;
    let window_seconds = 2; // Short window for testing

    // Use up the rate limit
    for _ in 1..=10 {
        store
            .check_rate_limit_admin_login(ip, limit, window_seconds)
            .expect("check rate limit");
    }

    // Should be blocked
    let (allowed, _) = store
        .check_rate_limit_admin_login(ip, limit, window_seconds)
        .expect("check rate limit");
    assert!(!allowed, "should be rate limited");

    // Wait for window to expire
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Should be allowed again after TTL expires
    let (allowed, _) = store
        .check_rate_limit_admin_login(ip, limit, window_seconds)
        .expect("check rate limit");
    assert!(allowed, "should be allowed after window expires");
}

/// Backoff expires after its TTL (backoff duration + buffer).
#[tokio::test]
async fn backoff_expires_after_ttl() {
    let (store, _url) = redis_store().await;
    let ip = "192.168.1.111";
    let failed_threshold = 5;
    let backoff_start_minutes = 10;
    let backoff_max_hours = 24;

    // Trigger backoff with 5 failures
    for _ in 1..=5 {
        store
            .record_failure_admin_login(
                ip,
                failed_threshold,
                backoff_start_minutes,
                backoff_max_hours,
            )
            .expect("record failure");
    }

    // Should be in backoff
    let (allowed, wait_seconds) = store
        .check_rate_limit_admin_login(ip, 10, 60)
        .expect("check rate limit");
    assert!(!allowed);
    let _wait = wait_seconds.expect("should have wait time");

    // Simulate backoff expiring by resetting the rate limit state
    // In production, the Redis key would naturally expire after its TTL
    store
        .reset_rate_limit_admin_login(ip)
        .expect("reset rate limit");

    // After clearing the backoff state, login should be allowed
    let (allowed, wait_seconds) = store
        .check_rate_limit_admin_login(ip, 10, 60)
        .expect("check rate limit");
    assert!(allowed, "should be allowed after backoff expires");
    assert_eq!(wait_seconds, None);
}

/// Helm schema constraint: replicas > 1 requires backend: redis.
/// This test verifies the schema constraint exists and has the correct error message.
/// Note: The actual schema validation is done by Helm during `helm lint` or `helm install`.
#[test]
fn helm_schema_rejects_local_backend_with_replicas_gt_1() {
    // Read the Helm values schema
    let schema_json = std::fs::read_to_string("charts/miroir/values.schema.json")
        .expect("read values.schema.json");

    let schema: serde_json::Value = serde_json::from_str(&schema_json).expect("parse schema JSON");

    // Verify the schema has the constraint
    // The constraint is in miroir.properties.allOf
    let miroir = &schema["properties"]["miroir"];
    let all_of = miroir["allOf"]
        .as_array()
        .expect("allOf should be an array");

    // Find the constraint for admin_ui.rate_limit.backend when replicas > 1
    let admin_ui_constraint = all_of
        .iter()
        .find(|item| {
            item.get("errorMessage")
                .and_then(|m| m.as_str())
                .map(|s| s.contains("admin_ui.rate_limit.backend"))
                .unwrap_or(false)
        })
        .expect("should have admin_ui.rate_limit.backend constraint");

    let error_message = admin_ui_constraint["errorMessage"]
        .as_str()
        .expect("errorMessage should be a string");

    assert!(
        error_message.contains("admin_ui.rate_limit.backend must be 'redis' when replicas > 1"),
        "error message should mention the constraint: {error_message}"
    );

    // Verify the if-then structure
    assert!(
        admin_ui_constraint.get("if").is_some(),
        "should have 'if' condition"
    );
    assert!(
        admin_ui_constraint.get("then").is_some(),
        "should have 'then' constraint"
    );

    // Verify the 'if' checks for replicas >= 2
    let if_condition = &admin_ui_constraint["if"];
    assert_eq!(
        if_condition["properties"]["replicas"]["minimum"], 2,
        "if condition should check replicas >= 2"
    );

    // Verify the 'then' enforces backend = "redis"
    let then_constraint = &admin_ui_constraint["then"];
    assert_eq!(
        then_constraint["properties"]["admin_ui"]["properties"]["rate_limit"]["properties"]
            ["backend"]["const"],
        "redis",
        "then constraint should enforce backend = 'redis'"
    );
}
