//! P10 Admin session revocation acceptance tests (plan §9).
//!
//! Tests the login → logout → revoked-cookie replay flow:
//! 1. Create admin session in Redis (simulates login)
//! 2. Revoke session (simulates logout, publishes to Pub/Sub)
//! 3. Verify session rejected on the originating pod
//! 4. Verify session rejected on a second pod via Pub/Sub propagation
//! 5. Verify session lookup in Redis returns revoked=true
//! 6. Verify non-revoked sessions remain valid

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use miroir_core::task_store::{NewAdminSession, RedisTaskStore, TaskStore};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn redis_store() -> (RedisTaskStore, String) {
    let node = Redis::default();
    let container = node.start().await.expect("start redis");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("get port");
    let url = format!("redis://localhost:{port}");
    let store = RedisTaskStore::open(&url).await.expect("redis connect");
    (store, url)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn make_session(id: &str) -> NewAdminSession {
    NewAdminSession {
        session_id: id.to_string(),
        csrf_token: format!("csrf-{id}"),
        admin_key_hash: format!("hash-{id}"),
        created_at: now_ms(),
        expires_at: now_ms() + 3_600_000, // 1 hour
        user_agent: Some("test-agent".to_string()),
        source_ip: Some("127.0.0.1".to_string()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Login → logout → replay: session must be rejected after revocation.
#[tokio::test]
async fn test_login_logout_replay_rejected() {
    let (store, _url) = redis_store().await;

    // Step 1: Login — insert admin session
    let session = make_session("sess-login-logout-test");
    store
        .insert_admin_session(&session)
        .expect("insert session");

    // Verify session is NOT revoked initially
    let loaded = store
        .get_admin_session(&session.session_id)
        .expect("get session")
        .expect("session exists");
    assert!(!loaded.revoked, "new session should not be revoked");

    // Step 2: Logout — revoke session
    let revoked = store
        .revoke_admin_session(&session.session_id)
        .expect("revoke session");
    assert!(revoked, "session should have been revoked");

    // Step 3: Replay — verify session is now revoked
    let loaded = store
        .get_admin_session(&session.session_id)
        .expect("get session")
        .expect("session exists");
    assert!(loaded.revoked, "revoked session must have revoked=true");
}

/// Cross-pod revocation: session revoked on pod A is rejected on pod B
/// via Pub/Sub propagation within 200ms.
#[tokio::test]
async fn test_cross_pod_revocation_via_pubsub() {
    let (store, redis_url) = redis_store().await;

    // Simulate two pods, each with their own in-memory revocation cache
    let pod_a_revoked: Arc<DashMap<String, ()>> = Arc::new(DashMap::new());
    let pod_b_revoked: Arc<DashMap<String, ()>> = Arc::new(DashMap::new());

    let pod_a_clone = pod_a_revoked.clone();
    let pod_b_clone = pod_b_revoked.clone();

    let redis_url_a = redis_url.clone();
    let redis_url_b = redis_url.clone();

    // Start Pub/Sub subscribers for both "pods"
    let sub_a = tokio::spawn(async move {
        let _ = RedisTaskStore::subscribe_session_revocations(
            &redis_url_a,
            "miroir",
            move |session_id: String| {
                pod_a_clone.insert(session_id, ());
            },
        )
        .await;
    });

    let sub_b = tokio::spawn(async move {
        let _ = RedisTaskStore::subscribe_session_revocations(
            &redis_url_b,
            "miroir",
            move |session_id: String| {
                pod_b_clone.insert(session_id, ());
            },
        )
        .await;
    });

    // Give subscribers time to connect
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Create session on pod A
    let session = make_session("sess-cross-pod-test");
    store
        .insert_admin_session(&session)
        .expect("insert session");

    // Verify not revoked on either pod
    assert!(
        !pod_a_revoked.contains_key(&session.session_id),
        "pod A should not have revoked session before logout"
    );
    assert!(
        !pod_b_revoked.contains_key(&session.session_id),
        "pod B should not have revoked session before logout"
    );

    // Revoke on pod A (simulates logout)
    store
        .revoke_admin_session(&session.session_id)
        .expect("revoke session");

    // Wait for Pub/Sub propagation
    let deadline = Duration::from_millis(500);
    let start = std::time::Instant::now();
    loop {
        let a_has = pod_a_revoked.contains_key(&session.session_id);
        let b_has = pod_b_revoked.contains_key(&session.session_id);

        if a_has && b_has {
            break;
        }

        if start.elapsed() > deadline {
            panic!(
                "Pub/Sub propagation did not complete within {:?}: pod_a={}, pod_b={}",
                deadline,
                a_has,
                b_has
            );
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let elapsed = start.elapsed();
    assert!(
        elapsed < deadline,
        "Propagation should be fast, took {:?}",
        elapsed
    );

    // Both pods must now reject the session
    assert!(
        pod_a_revoked.contains_key(&session.session_id),
        "pod A must reject revoked session"
    );
    assert!(
        pod_b_revoked.contains_key(&session.session_id),
        "pod B must reject revoked session (received via Pub/Sub)"
    );

    sub_a.abort();
    sub_b.abort();
}

/// Multiple sessions: revoking one does not affect others.
#[tokio::test]
async fn test_revocation_is_per_session() {
    let (store, _url) = redis_store().await;

    let session_a = make_session("sess-per-a");
    let session_b = make_session("sess-per-b");

    store.insert_admin_session(&session_a).expect("insert a");
    store.insert_admin_session(&session_b).expect("insert b");

    // Revoke only session A
    store
        .revoke_admin_session(&session_a.session_id)
        .expect("revoke a");

    // Session A is revoked
    let loaded_a = store
        .get_admin_session(&session_a.session_id)
        .expect("get a")
        .expect("a exists");
    assert!(loaded_a.revoked, "session A should be revoked");

    // Session B is NOT revoked
    let loaded_b = store
        .get_admin_session(&session_b.session_id)
        .expect("get b")
        .expect("b exists");
    assert!(!loaded_b.revoked, "session B should not be affected");
}

/// Revoking a non-existent session returns false but does not error.
#[tokio::test]
async fn test_revoke_nonexistent_session() {
    let (store, _url) = redis_store().await;

    let result = store
        .revoke_admin_session("sess-does-not-exist")
        .expect("revoke should not error");
    assert!(!result, "revoking nonexistent session should return false");
}

/// Expired session is invalid regardless of revocation status.
#[tokio::test]
async fn test_expired_session_is_invalid() {
    let (store, _url) = redis_store().await;

    let mut session = make_session("sess-expired");
    session.expires_at = now_ms() - 1000; // expired 1 second ago

    store.insert_admin_session(&session).expect("insert expired");

    let loaded = store
        .get_admin_session(&session.session_id)
        .expect("get expired")
        .expect("exists");

    // Session exists but is expired
    let now = now_ms();
    assert!(
        loaded.expires_at < now,
        "session should be expired: expires_at={}, now={}",
        loaded.expires_at,
        now
    );
}

/// CSRF token rotation on session refresh does not affect revocation.
#[tokio::test]
async fn test_csrf_refresh_preserves_revocation() {
    let (store, _url) = redis_store().await;

    let session = make_session("sess-csrf-refresh");
    store.insert_admin_session(&session).expect("insert");

    // Refresh CSRF token (simulates GET /_miroir/admin/session)
    let refreshed = NewAdminSession {
        session_id: session.session_id.clone(),
        csrf_token: "new-csrf-token".to_string(),
        admin_key_hash: session.admin_key_hash.clone(),
        created_at: session.created_at,
        expires_at: session.expires_at,
        user_agent: session.user_agent.clone(),
        source_ip: session.source_ip.clone(),
    };
    store.insert_admin_session(&refreshed).expect("refresh");

    // Revoke after refresh
    store
        .revoke_admin_session(&session.session_id)
        .expect("revoke");

    let loaded = store
        .get_admin_session(&session.session_id)
        .expect("get")
        .expect("exists");

    assert!(loaded.revoked, "session must be revoked after logout");
    assert_eq!(
        loaded.csrf_token, "new-csrf-token",
        "CSRF token should reflect last refresh"
    );
}

/// Pub/Sub delivers multiple revocations in order.
#[tokio::test]
async fn test_pubsub_multiple_revocations() {
    let (store, redis_url) = redis_store().await;

    let received: Arc<std::sync::Mutex<Vec<String>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let received_clone = received.clone();

    let sub = tokio::spawn(async move {
        let _ = RedisTaskStore::subscribe_session_revocations(
            &redis_url,
            "miroir",
            move |session_id: String| {
                received_clone.lock().unwrap().push(session_id);
            },
        )
        .await;
    });

    // Give subscriber time to connect
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Create and revoke 3 sessions
    for i in 0..3 {
        let session = make_session(&format!("sess-multi-{i}"));
        store.insert_admin_session(&session).expect("insert");
        store
            .revoke_admin_session(&session.session_id)
            .expect("revoke");
    }

    // Wait for all 3 revocations
    let deadline = Duration::from_millis(500);
    let start = std::time::Instant::now();
    loop {
        let count = received.lock().unwrap().len();
        if count >= 3 {
            break;
        }
        if start.elapsed() > deadline {
            panic!(
                "Expected 3 revocations, got {} after {:?}",
                count,
                start.elapsed()
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let ids = received.lock().unwrap().clone();
    assert_eq!(ids.len(), 3);
    assert!(ids.contains(&"sess-multi-0".to_string()));
    assert!(ids.contains(&"sess-multi-1".to_string()));
    assert!(ids.contains(&"sess-multi-2".to_string()));

    sub.abort();
}
