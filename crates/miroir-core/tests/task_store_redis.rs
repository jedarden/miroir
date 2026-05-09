//! Redis integration tests for the task store.
//! Phase 3 feature — uses testcontainers to spin up a real Redis instance.

#![cfg(feature = "task-store")]

use miroir_core::task_store::*;
use miroir_core::task_store::{RedisTaskStore, TaskStore};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper function to create a Redis container and connect to it.
async fn create_redis_store() -> Arc<RedisTaskStore> {
    let docker = testcontainers::runners::AsyncRunner::default();
    let redis_image = testcontainers::GenericImage::new("redis", "7.2-alpine");
    let node = docker.start(redis_image).await;
    let port = node.get_host_port_ipv4(6379).await;
    let url = format!("redis://127.0.0.1:{}", port);

    let store = RedisTaskStore::new(&url).await.unwrap();
    store.initialize().await.unwrap();
    Arc::new(store)
}

/// Integration test: task insert/get round-trip with Redis backend.
#[tokio::test]
async fn redis_task_insert_get_roundtrip() {
    let store = create_redis_store().await;

    let task = Task {
        miroir_id: "redis-test-1".to_string(),
        created_at: 1234567890,
        status: TaskStatus::Enqueued,
        node_tasks: HashMap::new(),
        error: None,
    };

    // Insert
    store.task_insert(&task).await.unwrap();

    // Get
    let retrieved = store.task_get("redis-test-1").await.unwrap().unwrap();

    assert_eq!(retrieved.miroir_id, task.miroir_id);
    assert_eq!(retrieved.created_at, task.created_at);
    assert_eq!(retrieved.status, task.status);
    assert_eq!(retrieved.node_tasks, task.node_tasks);
    assert_eq!(retrieved.error, task.error);
}

/// Integration test: leader lease acquisition with Redis backend.
#[tokio::test]
async fn redis_leader_lease_acquire_renew() {
    let store = create_redis_store().await;

    let lease1 = LeaderLease {
        lease_id: "redis-lease-1".to_string(),
        holder: "pod-1".to_string(),
        acquired_at: 1234567890,
        expires_at: 1234599999, // 3 hours later
    };

    // Acquire
    let acquired = store.leader_lease_acquire(&lease1).await.unwrap();
    assert!(acquired);

    // Get current lease
    let current = store.leader_lease_get().await.unwrap().unwrap();
    assert_eq!(current.lease_id, lease1.lease_id);
    assert_eq!(current.holder, lease1.holder);

    // Try to acquire again (should fail - lease still valid)
    let lease2 = LeaderLease {
        lease_id: "redis-lease-2".to_string(),
        holder: "pod-2".to_string(),
        acquired_at: 1234570000,
        expires_at: 1234600000,
    };

    let acquired2 = store.leader_lease_acquire(&lease2).await.unwrap();
    assert!(!acquired2);

    // Release
    store.leader_lease_release("redis-lease-1").await.unwrap();

    // Now acquisition should succeed
    let acquired3 = store.leader_lease_acquire(&lease2).await.unwrap();
    assert!(acquired3);
}

/// Integration test: idempotency cache with Redis TTL.
#[tokio::test]
async fn redis_idempotency_cache_ttl() {
    let store = create_redis_store().await;

    let entry = IdempotencyEntry {
        key: "redis-req-123".to_string(),
        response: "{\"result\":\"ok\"}".to_string(),
        status_code: 200,
        created_at: 1234567890,
    };

    // Record
    store.idempotency_record(&entry).await.unwrap();

    // Check
    let retrieved = store
        .idempotency_check("redis-req-123")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(retrieved.key, entry.key);
    assert_eq!(retrieved.response, entry.response);
    assert_eq!(retrieved.status_code, entry.status_code);

    // Wait for TTL to expire (1 hour default, but we can't wait that long in tests)
    // Instead, verify the prune operation is a no-op for Redis (handled by TTL automatically)
    let pruned = store.idempotency_prune(2000000000).await.unwrap();
    assert_eq!(pruned, 0);
}

/// Integration test: Redis-specific rate limit operations.
#[tokio::test]
async fn redis_ratelimit_increment() {
    let store = create_redis_store().await;

    let key = "test-ip-1";

    // First increment
    let (count, ttl) = store.ratelimit_increment(key, 60, 10).await.unwrap();
    assert_eq!(count, 1);
    assert!(ttl > 0 && ttl <= 60);

    // Second increment
    let (count2, ttl2) = store.ratelimit_increment(key, 60, 10).await.unwrap();
    assert_eq!(count2, 2);
    assert!(ttl2 > 0 && ttl2 <= 60);
}

/// Integration test: Redis-specific rate limit backoff.
#[tokio::test]
async fn redis_ratelimit_backoff() {
    let store = create_redis_store().await;

    let key = "test-ip-backoff";

    // Set backoff
    store.ratelimit_set_backoff(key, 10).await.unwrap();

    // Check backoff
    let backoff = store.ratelimit_check_backoff(key).await.unwrap();
    assert!(backoff.is_some());
    assert!(backoff.unwrap() > 0 && backoff.unwrap() <= 10);
}

/// Integration test: Redis-specific CDC overflow operations.
#[tokio::test]
async fn redis_cdc_overflow() {
    let store = create_redis_store().await;

    let sink = "kafka-test";

    // Check overflow buffer (should not exist initially)
    let exists = store.cdc_overflow_check(sink).await.unwrap();
    assert!(!exists);

    // Append data
    let data = b"test-data";
    store.cdc_overflow_append(sink, data).await.unwrap();

    // Check size
    let size = store.cdc_overflow_size(sink).await.unwrap();
    assert_eq!(size, data.len() as u64);

    // Check overflow buffer exists now
    let exists2 = store.cdc_overflow_check(sink).await.unwrap();
    assert!(exists2);

    // Clear
    store.cdc_overflow_clear(sink).await.unwrap();

    // Verify cleared
    let exists3 = store.cdc_overflow_check(sink).await.unwrap();
    assert!(!exists3);
}

/// Integration test: Redis-specific scoped key operations.
#[tokio::test]
async fn redis_scoped_key_rotation() {
    let store = create_redis_store().await;

    let index = "test-index";
    let key = "key-v1";

    // Set scoped key
    let expires_at = chrono::Utc::now().timestamp_millis() as u64 + 60000;
    store.scoped_key_set(index, key, expires_at).await.unwrap();

    // Get scoped key
    let retrieved = store.scoped_key_get(index).await.unwrap();
    assert_eq!(retrieved.as_deref(), Some(key));

    // Mark as observed by a pod
    let pod = "pod-1";
    store.scoped_key_observe(pod, index, key).await.unwrap();

    // Check if pod has observed the key
    let observed = store
        .scoped_key_has_observed(pod, index, key)
        .await
        .unwrap();
    assert!(observed);

    // Check that a different key is not observed
    let observed2 = store
        .scoped_key_has_observed(pod, index, "key-v2")
        .await
        .unwrap();
    assert!(!observed2);
}

/// Integration test: alias upsert and list with Redis backend.
#[tokio::test]
async fn redis_alias_upsert_list() {
    let store = create_redis_store().await;

    let alias = Alias {
        name: "redis-alias".to_string(),
        kind: AliasKind::Single,
        current_uid: Some("index-1".to_string()),
        target_uids: vec!["index-1".to_string()],
        version: 1,
        created_at: 1234567890,
        updated_at: 1234567890,
    };

    // Insert
    store.alias_upsert(&alias).await.unwrap();

    // Get
    let retrieved = store.alias_get("redis-alias").await.unwrap().unwrap();
    assert_eq!(retrieved.name, alias.name);
    assert_eq!(retrieved.kind, alias.kind);

    // List
    let aliases = store.alias_list().await.unwrap();
    assert_eq!(aliases.len(), 1);
    assert_eq!(aliases[0].name, "redis-alias");

    // Delete
    store.alias_delete("redis-alias").await.unwrap();

    // Verify deleted
    let retrieved2 = store.alias_get("redis-alias").await.unwrap();
    assert!(retrieved2.is_none());
}

/// Integration test: job enqueue/dequeue with Redis backend.
#[tokio::test]
async fn redis_job_enqueue_dequeue() {
    let store = create_redis_store().await;

    let job1 = Job {
        job_id: "redis-job-1".to_string(),
        job_type: "test".to_string(),
        parameters: "{}".to_string(),
        status: JobStatus::Enqueued,
        worker_id: None,
        result: None,
        error: None,
        created_at: 1234567890,
        started_at: None,
        completed_at: None,
    };

    let job2 = Job {
        job_id: "redis-job-2".to_string(),
        job_type: "test".to_string(),
        parameters: "{}".to_string(),
        status: JobStatus::Enqueued,
        worker_id: None,
        result: None,
        error: None,
        created_at: 1234567891,
        started_at: None,
        completed_at: None,
    };

    // Enqueue
    store.job_enqueue(&job1).await.unwrap();
    store.job_enqueue(&job2).await.unwrap();

    // Dequeue (should get job-1 first - FIFO)
    let dequeued = store.job_dequeue("worker-1").await.unwrap().unwrap();
    assert_eq!(dequeued.job_id, "redis-job-1");
    assert_eq!(dequeued.status, JobStatus::Processing);
    assert_eq!(dequeued.worker_id, Some("worker-1".to_string()));

    // Update job status
    store
        .job_update_status("redis-job-1", JobStatus::Succeeded, Some("{\"ok\":true}"))
        .await
        .unwrap();

    let updated = store.job_get("redis-job-1").await.unwrap().unwrap();
    assert_eq!(updated.status, JobStatus::Succeeded);
    assert_eq!(updated.result, Some("{\"ok\":true}".to_string()));
    assert!(updated.completed_at.is_some());
}

/// Integration test: session management with Redis backend.
#[tokio::test]
async fn redis_session_management() {
    let store = create_redis_store().await;

    let session = Session {
        session_id: "redis-session-1".to_string(),
        index: "products".to_string(),
        settings_version: 5,
        created_at: 1234567890,
        expires_at: 1234654290, // 24 hours later
    };

    // Insert session
    store.session_upsert(&session).await.unwrap();

    // Get session
    let retrieved = store.session_get("redis-session-1").await.unwrap().unwrap();
    assert_eq!(retrieved.session_id, session.session_id);
    assert_eq!(retrieved.index, session.index);
    assert_eq!(retrieved.settings_version, session.settings_version);

    // Delete session
    store.session_delete("redis-session-1").await.unwrap();

    let retrieved2 = store.session_get("redis-session-1").await.unwrap();
    assert!(retrieved2.is_none());
}

/// Integration test: canary run history with Redis backend.
#[tokio::test]
async fn redis_canary_run_history() {
    let store = create_redis_store().await;

    let canary = Canary {
        name: "redis-canary".to_string(),
        index: "test-index".to_string(),
        query: "*".to_string(),
        min_results: 1,
        max_results: 1000,
        interval_s: 60,
        enabled: true,
        created_at: 1234567890,
        updated_at: 1234567890,
    };

    store.canary_upsert(&canary).await.unwrap();

    // Insert runs
    let run1 = CanaryRun {
        run_id: "redis-run-1".to_string(),
        canary_name: "redis-canary".to_string(),
        ran_at: 1234567890,
        passed: true,
        result_count: 100,
        error: None,
        latency_ms: 50,
    };

    let run2 = CanaryRun {
        run_id: "redis-run-2".to_string(),
        canary_name: "redis-canary".to_string(),
        ran_at: 1234567950,
        passed: false,
        result_count: 0,
        error: Some("no results".to_string()),
        latency_ms: 45,
    };

    store.canary_run_insert(&run1).await.unwrap();
    store.canary_run_insert(&run2).await.unwrap();

    // List runs
    let runs = store.canary_run_list("redis-canary", 10).await.unwrap();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].run_id, "redis-run-1"); // Most recent first
    assert_eq!(runs[1].run_id, "redis-run-2");
}

/// Integration test: admin session with Redis backend.
#[tokio::test]
async fn redis_admin_session_revocation() {
    let store = create_redis_store().await;

    let session = AdminSession {
        session_id: "redis-admin-session".to_string(),
        user_id: "user-1".to_string(),
        created_at: 1234567890,
        expires_at: 1234654290, // 24 hours later
        revoked: false,
    };

    // Insert session
    store.admin_session_upsert(&session).await.unwrap();

    // Get session
    let retrieved = store
        .admin_session_get("redis-admin-session")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.session_id, session.session_id);
    assert!(!retrieved.revoked);

    // Revoke session
    store
        .admin_session_revoke("redis-admin-session")
        .await
        .unwrap();

    let is_revoked = store
        .admin_session_is_revoked("redis-admin-session")
        .await
        .unwrap();
    assert!(is_revoked);
}

/// Integration test: health check with Redis backend.
#[tokio::test]
async fn redis_health_check() {
    let store = create_redis_store().await;
    let healthy = store.health_check().await.unwrap();
    assert!(healthy);
}

/// Integration test: schema version with Redis backend.
#[tokio::test]
async fn redis_schema_version() {
    let store = create_redis_store().await;
    let version = store.schema_version().await.unwrap();
    assert_eq!(version, SCHEMA_VERSION);
}
