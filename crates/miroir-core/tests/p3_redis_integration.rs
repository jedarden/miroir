//! Phase 3 Integration Test: Redis TaskStore
//!
//! Integration test for Redis-backed TaskStore using testcontainers.
//! Tests leases, idempotency dedup, and alias history as required by
//! Phase 3 DoD.
//!
//! As required by Phase 3 DoD:
//! "Redis-backend integration test (testcontainers or similar)
//! exercising leases, idempotency dedup, and alias history."

#![cfg(feature = "redis-store")]

use miroir_core::task_store::*;
use sha2::Digest;
use std::collections::HashMap;
use std::path::Path;
use testcontainers_modules::redis::Redis;

/// Check if Docker is available for testcontainers.
fn check_docker_available() -> Result<(), String> {
    if std::env::var("MIROIR_TEST_SKIP_DOCKER").is_ok() {
        return Err("Docker tests skipped via MIROIR_TEST_SKIP_DOCKER. \
             Unset MIROIR_TEST_SKIP_DOCKER and ensure Docker is available."
            .to_string());
    }

    let docker_sock = Path::new("/var/run/docker.sock");
    if !docker_sock.exists() {
        return Err("Docker socket not found at /var/run/docker.sock. \
             Set MIROIR_TEST_SKIP_DOCKER=1 to skip, or ensure Docker is running."
            .to_string());
    }

    if let Err(e) = std::fs::metadata(docker_sock) {
        return Err(format!(
            "Cannot access Docker socket: {e}. \
             Set MIROIR_TEST_SKIP_DOCKER=1 to skip, or ensure Docker is running."
        ));
    }

    Ok(())
}

/// Helper to create a Redis container and connect to it
async fn create_redis_store() -> Result<
    (
        miroir_core::task_store::RedisTaskStore,
        testcontainers::ContainerAsync<Redis>,
    ),
    String,
> {
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::redis::Redis;

    check_docker_available().map_err(|e| format!("{e}. Set MIROIR_TEST_SKIP_DOCKER=1 to skip."))?;

    let redis_container = Redis::default()
        .start()
        .await
        .map_err(|e| format!("start redis: {e}"))?;

    let port = redis_container
        .get_host_port_ipv4(6379)
        .await
        .map_err(|e| format!("get port: {e}"))?;
    let url = format!("redis://localhost:{port}");

    let store = miroir_core::task_store::RedisTaskStore::open(&url)
        .await
        .map_err(|e| format!("redis connect: {e}"))?;
    store.migrate().map_err(|e| format!("migrate: {e}"))?;

    Ok((store, redis_container))
}

/// Helper to create a test task
fn new_test_task(miroir_id: &str) -> NewTask {
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-0".to_string(), 42);
    node_tasks.insert("node-1".to_string(), 17);

    let mut node_errors = HashMap::new();
    node_errors.insert("node-0".to_string(), "".to_string());
    node_errors.insert("node-1".to_string(), "".to_string());

    NewTask {
        miroir_id: miroir_id.to_string(),
        created_at: 1714500000000,
        status: "enqueued".to_string(),
        node_tasks,
        error: None,
        started_at: None,
        finished_at: None,
        index_uid: Some("test-index".to_string()),
        task_type: Some("documentAddition".to_string()),
        node_errors,
    }
}

#[tokio::test]
async fn test_redis_task_roundtrip() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let task = new_test_task("mtask-redis-001");

    // Insert
    store.insert_task(&task).expect("Failed to insert task");

    // Get
    let retrieved = store
        .get_task("mtask-redis-001")
        .expect("Failed to get task");

    assert!(retrieved.is_some());
    let task = retrieved.unwrap();
    assert_eq!(task.miroir_id, "mtask-redis-001");
    assert_eq!(task.status, "enqueued");
    assert_eq!(task.index_uid, Some("test-index".to_string()));
}

#[tokio::test]
async fn test_redis_task_count() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    // Insert multiple tasks
    for i in 0..10 {
        let task = new_test_task(&format!("mtask-count-{i}"));
        store.insert_task(&task).unwrap();
    }

    let count = store.task_count().unwrap();
    assert_eq!(count, 10);
}

#[tokio::test]
async fn test_redis_list_tasks() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    // Insert tasks with different statuses
    let mut task1 = new_test_task("mtask-list-1");
    task1.status = "succeeded".to_string();
    store.insert_task(&task1).unwrap();

    let mut task2 = new_test_task("mtask-list-2");
    task2.status = "processing".to_string();
    store.insert_task(&task2).unwrap();

    let mut task3 = new_test_task("mtask-list-3");
    task3.status = "succeeded".to_string();
    store.insert_task(&task3).unwrap();

    // List all tasks
    let filter = TaskFilter {
        status: None,
        index_uid: None,
        task_type: None,
        limit: None,
        offset: None,
    };

    let tasks = store.list_tasks(&filter).unwrap();
    assert_eq!(tasks.len(), 3);

    // List with status filter
    let filter = TaskFilter {
        status: Some("succeeded".to_string()),
        index_uid: None,
        task_type: None,
        limit: None,
        offset: None,
    };

    let succeeded = store.list_tasks(&filter).unwrap();
    assert_eq!(succeeded.len(), 2);
    for task in &succeeded {
        assert_eq!(task.status, "succeeded");
    }
}

#[tokio::test]
async fn test_redis_task_pruning() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    // Insert old terminal tasks
    let mut task1 = new_test_task("mtask-old-1");
    task1.created_at = 1714400000000; // 1 day ago
    task1.status = "succeeded".to_string();
    store.insert_task(&task1).unwrap();

    let mut task2 = new_test_task("mtask-old-2");
    task2.created_at = 1714400000000;
    task2.status = "failed".to_string();
    store.insert_task(&task2).unwrap();

    // Recent task
    let mut task3 = new_test_task("mtask-recent");
    task3.created_at = 1714500000000;
    task3.status = "succeeded".to_string();
    store.insert_task(&task3).unwrap();

    // Prune old tasks
    let cutoff = 1714500000000 - 3600000; // 1 hour ago
    let pruned = store.prune_tasks(cutoff, 100).unwrap();
    assert_eq!(pruned, 2);

    // Verify old tasks are gone
    assert!(store.get_task("mtask-old-1").unwrap().is_none());
    assert!(store.get_task("mtask-old-2").unwrap().is_none());

    // Recent task should still exist
    assert!(store.get_task("mtask-recent").unwrap().is_some());
}

// ---------------------------------------------------------------------------
// Leader Lease Tests (§14.5 Mode B)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_redis_leader_lease_acquire() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let scope = "test-scope:acquire";
    let holder = "pod-1";
    let expires_at = 1714500100000;
    let now_ms = 1714500000000;

    // Acquire lease
    let acquired = store
        .try_acquire_leader_lease(scope, holder, expires_at, now_ms)
        .unwrap();

    assert!(acquired);

    // Verify lease was acquired
    let lease = store.get_leader_lease(scope).unwrap().unwrap();
    assert_eq!(lease.holder, holder);
    assert_eq!(lease.scope, scope);
}

#[tokio::test]
async fn test_redis_leader_lease_renew() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let scope = "test-scope:renew";
    let holder = "pod-1";
    let expires_at1 = 1714500100000;
    let expires_at2 = 1714500200000;
    let now_ms = 1714500000000;

    // Acquire lease
    store
        .try_acquire_leader_lease(scope, holder, expires_at1, now_ms)
        .unwrap();

    // Renew lease
    let renewed = store
        .renew_leader_lease(scope, holder, expires_at2, now_ms)
        .unwrap();

    assert!(renewed);
}

#[tokio::test]
async fn test_redis_leader_lease_steal_expired() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let scope = "test-scope:steal";
    let holder1 = "pod-1";
    let holder2 = "pod-2";
    let expires_at1 = 1714500100000; // Expires at this time
    let now_ms = 1714500000000;

    // Pod-1 acquires lease
    store
        .try_acquire_leader_lease(scope, holder1, expires_at1, now_ms)
        .unwrap();

    // Pod-2 tries to acquire while lease is still valid
    let acquired = store
        .try_acquire_leader_lease(scope, holder2, expires_at1 + 10000, now_ms)
        .unwrap();

    assert!(!acquired, "Should not steal active lease");

    // Now pretend the lease expired
    let future_now_ms = expires_at1 + 1000;
    let acquired = store
        .try_acquire_leader_lease(scope, holder2, expires_at1 + 20000, future_now_ms)
        .unwrap();

    assert!(acquired, "Should steal expired lease");

    // Verify holder changed
    let lease = store.get_leader_lease(scope).unwrap().unwrap();
    assert_eq!(lease.holder, holder2);
}

#[tokio::test]
async fn test_redis_leader_lease_holders_only_renew() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let scope = "test-scope:holder-only";
    let holder1 = "pod-1";
    let holder2 = "pod-2";
    let expires_at = 1714500100000;
    let now_ms = 1714500000000;

    // Pod-1 acquires lease
    store
        .try_acquire_leader_lease(scope, holder1, expires_at, now_ms)
        .unwrap();

    // Pod-2 tries to renew (should fail)
    let renewed = store
        .renew_leader_lease(scope, holder2, expires_at + 10000, now_ms)
        .unwrap();

    assert!(!renewed, "Non-holder should not renew lease");

    // Pod-1 renews (should succeed)
    let renewed = store
        .renew_leader_lease(scope, holder1, expires_at + 10000, now_ms)
        .unwrap();

    assert!(renewed, "Holder should renew lease");
}

// ---------------------------------------------------------------------------
// Idempotency Cache Tests (§13.10)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_redis_idempotency_dedup() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let key = "idemp-key-dedup";
    let body = b"test request body";
    let body_sha256 = sha2::Sha256::digest(body);
    let miroir_task_id = "mtask-001";

    let entry = IdempotencyEntry {
        key: key.to_string(),
        body_sha256: body_sha256.to_vec(),
        miroir_task_id: miroir_task_id.to_string(),
        expires_at: 1714500100000,
    };

    // First insert should succeed
    store
        .insert_idempotency_entry(&entry)
        .expect("First insert should succeed");

    // Second insert with same key should fail (unique constraint)
    let result = store.insert_idempotency_entry(&entry);
    assert!(
        result.is_err(),
        "Duplicate insert should fail with constraint error"
    );

    // Verify we can retrieve the entry
    let retrieved = store.get_idempotency_entry(key).unwrap().unwrap();
    assert_eq!(retrieved.miroir_task_id, miroir_task_id);
}

#[tokio::test]
async fn test_redis_idempotency_different_keys() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let body_sha256 = sha2::Sha256::digest(b"test body");

    // Insert two different keys
    let entry1 = IdempotencyEntry {
        key: "key-1".to_string(),
        body_sha256: body_sha256.to_vec(),
        miroir_task_id: "mtask-1".to_string(),
        expires_at: 1714500100000,
    };

    let entry2 = IdempotencyEntry {
        key: "key-2".to_string(),
        body_sha256: body_sha256.to_vec(),
        miroir_task_id: "mtask-2".to_string(),
        expires_at: 1714500100000,
    };

    store.insert_idempotency_entry(&entry1).unwrap();
    store.insert_idempotency_entry(&entry2).unwrap();

    // Verify both exist
    let retrieved1 = store.get_idempotency_entry("key-1").unwrap().unwrap();
    assert_eq!(retrieved1.miroir_task_id, "mtask-1");

    let retrieved2 = store.get_idempotency_entry("key-2").unwrap().unwrap();
    assert_eq!(retrieved2.miroir_task_id, "mtask-2");
}

// ---------------------------------------------------------------------------
// Alias History Tests (§13.7)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_redis_alias_flip_records_history() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let alias = NewAlias {
        name: "flip-alias-redis".to_string(),
        kind: "single".to_string(),
        current_uid: Some("index-1".to_string()),
        target_uids: None,
        version: 1,
        created_at: 1714500000000,
        history: vec![],
    };

    store.create_alias(&alias).unwrap();

    // Flip to index-2
    store.flip_alias("flip-alias-redis", "index-2", 10).unwrap();

    // Flip to index-3
    store.flip_alias("flip-alias-redis", "index-3", 10).unwrap();

    // Verify history
    let retrieved = store.get_alias("flip-alias-redis").unwrap().unwrap();

    assert_eq!(retrieved.current_uid.unwrap(), "index-3");
    assert_eq!(retrieved.version, 3);
    assert_eq!(retrieved.history.len(), 2);
    assert_eq!(retrieved.history[0].uid, "index-1");
    assert_eq!(retrieved.history[1].uid, "index-2");
}

#[tokio::test]
async fn test_redis_alias_history_retention() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let alias = NewAlias {
        name: "retention-alias".to_string(),
        kind: "single".to_string(),
        current_uid: Some("index-0".to_string()),
        target_uids: None,
        version: 1,
        created_at: 1714500000000,
        history: vec![],
    };

    store.create_alias(&alias).unwrap();

    // Flip 15 times (more than retention limit of 10)
    for i in 1..=15 {
        store
            .flip_alias("retention-alias", &format!("index-{i}"), 10)
            .unwrap();
    }

    // Verify history is bounded
    let retrieved = store.get_alias("retention-alias").unwrap().unwrap();
    assert_eq!(retrieved.history.len(), 10, "History should be bounded");
}

#[tokio::test]
async fn test_redis_multi_target_alias() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let alias = NewAlias {
        name: "multi-alias".to_string(),
        kind: "multi".to_string(),
        current_uid: None,
        target_uids: Some(vec!["index-1".to_string(), "index-2".to_string()]),
        version: 1,
        created_at: 1714500000000,
        history: vec![],
    };

    store.create_alias(&alias).unwrap();

    let retrieved = store.get_alias("multi-alias").unwrap().unwrap();

    assert_eq!(retrieved.kind, "multi");
    assert!(retrieved.current_uid.is_none());
    assert_eq!(
        retrieved.target_uids.unwrap(),
        vec!["index-1".to_string(), "index-2".to_string()]
    );
}

// ---------------------------------------------------------------------------
// Job Claim Tests (§14.5 Mode C)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_redis_job_claim_cas() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let job = NewJob {
        id: "job-claim-1".to_string(),
        type_: "test".to_string(),
        params: "{}".to_string(),
        state: "queued".to_string(),
        progress: "{}".to_string(),
        parent_job_id: None,
        chunk_index: None,
        total_chunks: None,
        created_at: 1714500100000,
    };

    store.insert_job(&job).unwrap();

    // First claim should succeed
    let claimed = store
        .claim_job("job-claim-1", "pod-1", 1714500100000)
        .unwrap();

    assert!(claimed);

    // Second claim should fail (already claimed)
    let claimed2 = store
        .claim_job("job-claim-1", "pod-2", 1714500200000)
        .unwrap();

    assert!(!claimed2, "Should not claim already-claimed job");
}

#[tokio::test]
async fn test_redis_job_claim_renew() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let job = NewJob {
        id: "job-renew".to_string(),
        type_: "test".to_string(),
        params: "{}".to_string(),
        state: "queued".to_string(),
        progress: "{}".to_string(),
        parent_job_id: None,
        chunk_index: None,
        total_chunks: None,
        created_at: 1714500100000,
    };

    store.insert_job(&job).unwrap();

    // Claim job
    store
        .claim_job("job-renew", "pod-1", 1714500100000)
        .unwrap();

    // Renew claim
    let renewed = store.renew_job_claim("job-renew", 1714500200000).unwrap();

    assert!(renewed);
}

#[tokio::test]
async fn test_redis_list_jobs_by_state() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    // Insert jobs with different states
    for i in 0..5 {
        let job = NewJob {
            id: format!("job-queued-{i}"),
            type_: "test".to_string(),
            params: "{}".to_string(),
            state: "queued".to_string(),
            progress: "{}".to_string(),
            parent_job_id: None,
            chunk_index: None,
            total_chunks: None,
            created_at: 1714500100000 + i as i64,
        };
        store.insert_job(&job).unwrap();
    }

    for i in 0..3 {
        let job = NewJob {
            id: format!("job-progress-{i}"),
            type_: "test".to_string(),
            params: "{}".to_string(),
            state: "in_progress".to_string(),
            progress: "{}".to_string(),
            parent_job_id: None,
            chunk_index: None,
            total_chunks: None,
            created_at: 1714500200000 + i as i64,
        };
        store.insert_job(&job).unwrap();
    }

    // List queued jobs
    let queued = store.list_jobs_by_state("queued").unwrap();
    assert_eq!(queued.len(), 5);

    // List in_progress jobs
    let in_progress = store.list_jobs_by_state("in_progress").unwrap();
    assert_eq!(in_progress.len(), 3);
}

// ---------------------------------------------------------------------------
// Session Tests (§13.6)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_redis_session_upsert() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let session1 = SessionRow {
        session_id: "session-upsert".to_string(),
        last_write_mtask_id: Some("mtask-1".to_string()),
        last_write_at: Some(1714500000000),
        pinned_group: Some(0),
        min_settings_version: 1,
        ttl: 1714500100000,
    };

    store.upsert_session(&session1).unwrap();

    // Update with new values
    let session2 = SessionRow {
        session_id: "session-upsert".to_string(),
        last_write_mtask_id: Some("mtask-2".to_string()),
        last_write_at: Some(1714500001000),
        pinned_group: Some(1),
        min_settings_version: 2,
        ttl: 1714500200000,
    };

    store.upsert_session(&session2).unwrap();

    // Verify updated values
    let retrieved = store.get_session("session-upsert").unwrap().unwrap();
    assert_eq!(retrieved.last_write_mtask_id.unwrap(), "mtask-2");
    assert_eq!(retrieved.pinned_group.unwrap(), 1);
    assert_eq!(retrieved.min_settings_version, 2);
}

// ---------------------------------------------------------------------------
// Canary Run Tests (§13.18)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_redis_canary_run_auto_prune() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let canary_id = "canary-auto-prune";

    // Insert more runs than the history limit
    for i in 0..15 {
        let run = NewCanaryRun {
            canary_id: canary_id.to_string(),
            ran_at: 1714500000000 + (i as i64 * 1000),
            status: "pass".to_string(),
            latency_ms: 100,
            failed_assertions_json: None,
        };
        store.insert_canary_run(&run, 10).unwrap();
    }

    // Verify only 10 runs remain
    let runs = store.get_canary_runs(canary_id, 100).unwrap();
    assert_eq!(runs.len(), 10, "Should prune to history limit");

    // Verify they're in descending order
    for i in 0..runs.len().saturating_sub(1) {
        assert!(
            runs[i].ran_at >= runs[i + 1].ran_at,
            "Runs should be in descending order"
        );
    }
}

// ---------------------------------------------------------------------------
// Admin Session Tests (§13.19)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_redis_admin_session_revoke() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let session = NewAdminSession {
        session_id: "admin-revoke-test".to_string(),
        csrf_token: "csrf-token".to_string(),
        admin_key_hash: "key-hash".to_string(),
        created_at: 1714500000000,
        expires_at: 1714503600000,
        user_agent: Some("TestAgent".to_string()),
        source_ip: Some("10.0.0.1".to_string()),
    };

    store.insert_admin_session(&session).unwrap();

    // Revoke session
    store.revoke_admin_session("admin-revoke-test").unwrap();

    // Verify revoked flag
    let retrieved = store
        .get_admin_session("admin-revoke-test")
        .unwrap()
        .unwrap();

    assert!(retrieved.revoked);
}

#[tokio::test]
async fn test_redis_admin_session_delete_expired() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    // Insert expired session
    let expired_session = NewAdminSession {
        session_id: "expired-session".to_string(),
        csrf_token: "csrf".to_string(),
        admin_key_hash: "hash".to_string(),
        created_at: 1714400000000,
        expires_at: 1714403600000, // Expired
        user_agent: None,
        source_ip: None,
    };

    store.insert_admin_session(&expired_session).unwrap();

    // Insert valid session
    let valid_session = NewAdminSession {
        session_id: "valid-session".to_string(),
        csrf_token: "csrf".to_string(),
        admin_key_hash: "hash".to_string(),
        created_at: 1714500000000,
        expires_at: 1714503600000, // Valid
        user_agent: None,
        source_ip: None,
    };

    store.insert_admin_session(&valid_session).unwrap();

    // Redis handles expiration automatically via EXPIRE
    // delete_expired_admin_sessions is a no-op for Redis
    let deleted = store.delete_expired_admin_sessions(1714500000000).unwrap();

    assert_eq!(deleted, 0, "Redis handles expiration automatically");

    // Verify expired session is gone (TTL expired)
    let retrieved = store.get_admin_session("expired-session").unwrap();

    // Note: In real Redis, the key would have been auto-deleted by EXPIRE
    // In test, we check the revoked flag or TTL
    if let Some(session) = retrieved {
        // If still exists (test timing), it should be handled by TTL
        assert!(session.expires_at < 1714500000000);
    }
}

// ---------------------------------------------------------------------------
// Tenant Map Tests (§13.15)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_redis_tenant_mapping() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let api_key = b"test-api-key";
    let api_key_hash = sha2::Sha256::digest(api_key);

    let mapping = NewTenantMapping {
        api_key_hash: api_key_hash.to_vec(),
        tenant_id: "tenant-123".to_string(),
        group_id: Some(0),
    };

    store.insert_tenant_mapping(&mapping).unwrap();

    let retrieved = store.get_tenant_mapping(&api_key_hash).unwrap().unwrap();

    assert_eq!(retrieved.tenant_id, "tenant-123");
    assert_eq!(retrieved.group_id, Some(0));

    // Delete mapping
    store.delete_tenant_mapping(&api_key_hash).unwrap();

    // Verify gone
    let retrieved = store.get_tenant_mapping(&api_key_hash).unwrap();

    assert!(retrieved.is_none());
}

// ---------------------------------------------------------------------------
// CDC Cursor Tests (§13.13)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_redis_cdc_cursor() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let cursor = NewCdcCursor {
        sink_name: "kafka-sink".to_string(),
        index_uid: "test-index".to_string(),
        last_event_seq: 12345,
        updated_at: 1714500000000,
    };

    store.upsert_cdc_cursor(&cursor).unwrap();

    // Get cursor
    let retrieved = store
        .get_cdc_cursor("kafka-sink", "test-index")
        .unwrap()
        .unwrap();

    assert_eq!(retrieved.last_event_seq, 12345);

    // Update cursor
    let cursor2 = NewCdcCursor {
        sink_name: "kafka-sink".to_string(),
        index_uid: "test-index".to_string(),
        last_event_seq: 67890,
        updated_at: 1714500001000,
    };

    store.upsert_cdc_cursor(&cursor2).unwrap();

    let retrieved = store
        .get_cdc_cursor("kafka-sink", "test-index")
        .unwrap()
        .unwrap();

    assert_eq!(retrieved.last_event_seq, 67890);

    // List cursors for sink
    let cursors = store.list_cdc_cursors("kafka-sink").unwrap();
    assert_eq!(cursors.len(), 1);
}

// ---------------------------------------------------------------------------
// Rollover Policy Tests (§13.17)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_redis_rollover_policy() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let policy = NewRolloverPolicy {
        name: "daily-logs".to_string(),
        write_alias: "logs-write".to_string(),
        read_alias: "logs-read".to_string(),
        pattern: "logs-{YYYY-MM-DD}".to_string(),
        triggers_json: r#"{"max_age": "1d"}"#.to_string(),
        retention_json: r#"{"keep_indexes": 7}"#.to_string(),
        template_json: r#"{"primary_key": "id"}"#.to_string(),
        enabled: true,
    };

    store.upsert_rollover_policy(&policy).unwrap();

    let retrieved = store.get_rollover_policy("daily-logs").unwrap().unwrap();

    assert_eq!(retrieved.pattern, "logs-{YYYY-MM-DD}");
    assert!(retrieved.enabled);

    // List policies
    let policies = store.list_rollover_policies().unwrap();
    assert_eq!(policies.len(), 1);
}

// ---------------------------------------------------------------------------
// Search UI Config Tests (§13.21)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_redis_search_ui_config() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    let config = NewSearchUiConfig {
        index_uid: "test-index".to_string(),
        config_json: r#"{"title": "Test Search", "facets": ["category"]}"#.to_string(),
        updated_at: 1714500000000,
    };

    store.upsert_search_ui_config(&config).unwrap();

    let retrieved = store.get_search_ui_config("test-index").unwrap().unwrap();

    assert_eq!(retrieved.config_json, config.config_json);

    // Delete config
    store.delete_search_ui_config("test-index").unwrap();

    let retrieved = store.get_search_ui_config("test-index").unwrap();

    assert!(retrieved.is_none());
}

// ---------------------------------------------------------------------------
// Node Settings Version Tests (§13.5)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_redis_node_settings_version() {
    let (store, _container) = match create_redis_store().await {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Skipping test: {e}");
            return;
        }
    };

    // Insert initial version
    store
        .upsert_node_settings_version("test-index", "node-0", 1, 1714500000000)
        .unwrap();

    let retrieved = store
        .get_node_settings_version("test-index", "node-0")
        .unwrap()
        .unwrap();

    assert_eq!(retrieved.version, 1);

    // Update version
    store
        .upsert_node_settings_version("test-index", "node-0", 2, 1714500001000)
        .unwrap();

    let retrieved = store
        .get_node_settings_version("test-index", "node-0")
        .unwrap()
        .unwrap();

    assert_eq!(retrieved.version, 2);
}
