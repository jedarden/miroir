//! Phase 3 Integration Test: SQLite Restart Survivability
//!
//! Integration test that verifies task status survives a pod restart.
//! This simulates a restart by closing and reopening the SQLite handle
//! between operations.
//!
//! As required by Phase 3 DoD:
//! "Integration test: restart an orchestrator pod mid-task-poll;
//! task status survives (simulate by opening/closing the SQLite handle
//! between operations)."

use miroir_core::task_store::*;
use miroir_core::Result;
use sha2::Digest;
use std::collections::HashMap;
use tempfile::NamedTempFile;

/// Helper to create a new store from a file path
fn open_store(path: &std::path::Path) -> Result<miroir_core::task_store::SqliteTaskStore> {
    let store = SqliteTaskStore::open(path)?;
    store.migrate()?;
    Ok(store)
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

#[test]
fn test_task_survives_restart() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();

    // Phase 1: Insert a task before "restart"
    {
        let store = open_store(path).unwrap();
        let task = new_test_task("mtask-001");
        store.insert_task(&task).unwrap();
    } // Store closes here (simulates restart)

    // Phase 2: After "restart", verify task still exists
    {
        let store = open_store(path).unwrap();
        let retrieved = store.get_task("mtask-001").unwrap();

        assert!(retrieved.is_some(), "Task should survive restart");
        let task = retrieved.unwrap();
        assert_eq!(task.miroir_id, "mtask-001");
        assert_eq!(task.status, "enqueued");
        assert_eq!(task.index_uid, Some("test-index".to_string()));
    }
}

#[test]
fn test_task_update_survives_restart() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();

    // Insert initial task
    {
        let store = open_store(path).unwrap();
        let task = new_test_task("mtask-002");
        store.insert_task(&task).unwrap();
    }

    // Update task status
    {
        let store = open_store(path).unwrap();
        store.update_task_status("mtask-002", "processing").unwrap();
    }

    // Verify status persisted after restart
    {
        let store = open_store(path).unwrap();
        let task = store.get_task("mtask-002").unwrap().unwrap();
        assert_eq!(task.status, "processing");
    }
}

#[test]
fn test_node_task_update_survives_restart() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();

    // Insert initial task
    {
        let store = open_store(path).unwrap();
        let task = new_test_task("mtask-003");
        store.insert_task(&task).unwrap();
    }

    // Update node task mapping
    {
        let store = open_store(path).unwrap();
        store.update_node_task("mtask-003", "node-0", 100).unwrap();
    }

    // Verify node task mapping persisted after restart
    {
        let store = open_store(path).unwrap();
        let task = store.get_task("mtask-003").unwrap().unwrap();
        assert_eq!(task.node_tasks.get("node-0"), Some(&100));
        assert_eq!(task.node_tasks.get("node-1"), Some(&17)); // unchanged
    }
}

#[test]
fn test_multiple_tables_survive_restart() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();

    // Insert data into multiple tables
    {
        let store = open_store(path).unwrap();

        // Table 1: tasks
        let task = new_test_task("mtask-004");
        store.insert_task(&task).unwrap();

        // Table 2: node_settings_version
        store
            .upsert_node_settings_version("test-index", "node-0", 5, 1714500000000)
            .unwrap();

        // Table 3: aliases
        let alias = NewAlias {
            name: "test-alias".to_string(),
            kind: "single".to_string(),
            current_uid: Some("target-index".to_string()),
            target_uids: None,
            version: 1,
            created_at: 1714500000000,
            history: vec![],
        };
        store.create_alias(&alias).unwrap();

        // Table 4: sessions
        let session = SessionRow {
            session_id: "session-123".to_string(),
            last_write_mtask_id: Some("mtask-004".to_string()),
            last_write_at: Some(1714500000000),
            pinned_group: Some(0),
            min_settings_version: 1,
            ttl: 1714500100000,
        };
        store.upsert_session(&session).unwrap();

        // Table 5: idempotency_cache
        let body_sha256 = sha2::Sha256::digest(b"test body");
        let entry = IdempotencyEntry {
            key: "idemp-key-1".to_string(),
            body_sha256: body_sha256.to_vec(),
            miroir_task_id: "mtask-004".to_string(),
            expires_at: 1714500100000,
        };
        store.insert_idempotency_entry(&entry).unwrap();

        // Table 6: jobs
        let job = NewJob {
            id: "job-1".to_string(),
            type_: "dump_import".to_string(),
            params: "{}".to_string(),
            state: "queued".to_string(),
            progress: "{}".to_string(),
            parent_job_id: None,
            chunk_index: None,
            total_chunks: None,
            created_at: 1714500000000,
        };
        store.insert_job(&job).unwrap();

        // Table 7: leader_lease
        store
            .try_acquire_leader_lease("test-scope", "pod-1", 1714500100000, 1714500000000)
            .unwrap();

        // Table 8: canaries
        let canary = NewCanary {
            id: "canary-1".to_string(),
            name: "test-canary".to_string(),
            index_uid: "test-index".to_string(),
            interval_s: 300,
            query_json: r#"{"q": "test"}"#.to_string(),
            assertions_json: r#"[]"#.to_string(),
            enabled: true,
            created_at: 1714500000000,
        };
        store.upsert_canary(&canary).unwrap();

        // Table 9: canary_runs
        let run = NewCanaryRun {
            canary_id: "canary-1".to_string(),
            ran_at: 1714500000000,
            status: "pass".to_string(),
            latency_ms: 50,
            failed_assertions_json: None,
        };
        store.insert_canary_run(&run, 100).unwrap();

        // Table 10: cdc_cursors
        let cursor = NewCdcCursor {
            sink_name: "kafka-output".to_string(),
            index_uid: "test-index".to_string(),
            last_event_seq: 12345,
            updated_at: 1714500000000,
        };
        store.upsert_cdc_cursor(&cursor).unwrap();

        // Table 11: tenant_map
        let api_key_hash = sha2::Sha256::digest(b"test-api-key");
        let mapping = NewTenantMapping {
            api_key_hash: api_key_hash.to_vec(),
            tenant_id: "tenant-1".to_string(),
            group_id: Some(0),
        };
        store.insert_tenant_mapping(&mapping).unwrap();

        // Table 12: rollover_policies
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

        // Table 13: search_ui_config
        let config = NewSearchUiConfig {
            index_uid: "test-index".to_string(),
            config_json: r#"{"title": "Test"}"#.to_string(),
            updated_at: 1714500000000,
        };
        store.upsert_search_ui_config(&config).unwrap();

        // Table 14: admin_sessions
        let admin_session = NewAdminSession {
            session_id: "admin-session-1".to_string(),
            csrf_token: "csrf-token-123".to_string(),
            admin_key_hash: "key-hash".to_string(),
            created_at: 1714500000000,
            expires_at: 1714503600000,
            user_agent: Some("TestAgent".to_string()),
            source_ip: Some("10.0.0.1".to_string()),
        };
        store.insert_admin_session(&admin_session).unwrap();
    }

    // Verify all data persisted after restart
    {
        let store = open_store(path).unwrap();

        // Verify tasks
        let task = store.get_task("mtask-004").unwrap().unwrap();
        assert_eq!(task.miroir_id, "mtask-004");

        // Verify node_settings_version
        let version = store
            .get_node_settings_version("test-index", "node-0")
            .unwrap()
            .unwrap();
        assert_eq!(version.version, 5);

        // Verify aliases
        let alias = store.get_alias("test-alias").unwrap().unwrap();
        assert_eq!(alias.current_uid.unwrap(), "target-index");

        // Verify sessions
        let session = store.get_session("session-123").unwrap().unwrap();
        assert_eq!(session.session_id, "session-123");

        // Verify idempotency_cache
        let entry = store.get_idempotency_entry("idemp-key-1").unwrap().unwrap();
        assert_eq!(entry.miroir_task_id, "mtask-004");

        // Verify jobs
        let job = store.get_job("job-1").unwrap().unwrap();
        assert_eq!(job.type_, "dump_import");

        // Verify leader_lease
        let lease = store.get_leader_lease("test-scope").unwrap().unwrap();
        assert_eq!(lease.holder, "pod-1");

        // Verify canaries
        let canary = store.get_canary("canary-1").unwrap().unwrap();
        assert_eq!(canary.name, "test-canary");

        // Verify canary_runs
        let runs = store.get_canary_runs("canary-1", 10).unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status, "pass");

        // Verify cdc_cursors
        let cursor = store
            .get_cdc_cursor("kafka-output", "test-index")
            .unwrap()
            .unwrap();
        assert_eq!(cursor.last_event_seq, 12345);

        // Verify tenant_map
        let api_key_hash = sha2::Sha256::digest(b"test-api-key");
        let mapping = store.get_tenant_mapping(&api_key_hash).unwrap().unwrap();
        assert_eq!(mapping.tenant_id, "tenant-1");

        // Verify rollover_policies
        let policy = store.get_rollover_policy("daily-logs").unwrap().unwrap();
        assert_eq!(policy.pattern, "logs-{YYYY-MM-DD}");

        // Verify search_ui_config
        let config = store.get_search_ui_config("test-index").unwrap().unwrap();
        assert_eq!(config.config_json, r#"{"title": "Test"}"#);

        // Verify admin_sessions
        let admin_session = store.get_admin_session("admin-session-1").unwrap().unwrap();
        assert_eq!(admin_session.csrf_token, "csrf-token-123");
    }
}

#[test]
fn test_task_pruning_survives_restart() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();

    // Insert old terminal tasks
    {
        let store = open_store(path).unwrap();

        // Old succeeded task
        let mut task1 = new_test_task("mtask-old-1");
        task1.created_at = 1714400000000; // 1 day ago
        task1.status = "succeeded".to_string();
        store.insert_task(&task1).unwrap();

        // Old failed task
        let mut task2 = new_test_task("mtask-old-2");
        task2.created_at = 1714400000000; // 1 day ago
        task2.status = "failed".to_string();
        store.insert_task(&task2).unwrap();

        // Recent task (should not be pruned)
        let mut task3 = new_test_task("mtask-recent");
        task3.created_at = 1714500000000; // now
        task3.status = "succeeded".to_string();
        store.insert_task(&task3).unwrap();
    }

    // Prune old tasks (cutoff: anything older than 1 hour ago)
    {
        let store = open_store(path).unwrap();
        let cutoff = 1714500000000 - 3600000; // 1 hour ago
        let pruned = store.prune_tasks(cutoff, 100).unwrap();
        assert_eq!(pruned, 2); // Two old tasks pruned
    }

    // Verify pruning persisted after restart
    {
        let store = open_store(path).unwrap();

        // Old tasks should be gone
        assert!(store.get_task("mtask-old-1").unwrap().is_none());
        assert!(store.get_task("mtask-old-2").unwrap().is_none());

        // Recent task should still exist
        assert!(store.get_task("mtask-recent").unwrap().is_some());
    }
}

#[test]
fn test_task_count_survives_restart() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();

    // Insert tasks
    {
        let store = open_store(path).unwrap();
        for i in 0..10 {
            let task = new_test_task(&format!("mtask-count-{i}"));
            store.insert_task(&task).unwrap();
        }
    }

    // Verify count after restart
    {
        let store = open_store(path).unwrap();
        let count = store.task_count().unwrap();
        assert_eq!(count, 10);
    }
}

#[test]
fn test_list_tasks_survives_restart() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();

    // Insert tasks with different statuses
    {
        let store = open_store(path).unwrap();

        let mut task1 = new_test_task("mtask-list-1");
        task1.status = "succeeded".to_string();
        store.insert_task(&task1).unwrap();

        let mut task2 = new_test_task("mtask-list-2");
        task2.status = "processing".to_string();
        store.insert_task(&task2).unwrap();

        let mut task3 = new_test_task("mtask-list-3");
        task3.status = "succeeded".to_string();
        store.insert_task(&task3).unwrap();
    }

    // List tasks with filter after restart
    {
        let store = open_store(path).unwrap();

        let filter = TaskFilter {
            status: Some("succeeded".to_string()),
            index_uid: None,
            task_type: None,
            limit: None,
            offset: None,
        };

        let tasks = store.list_tasks(&filter).unwrap();
        assert_eq!(tasks.len(), 2);

        // All should be succeeded
        for task in &tasks {
            assert_eq!(task.status, "succeeded");
        }
    }
}

#[test]
fn test_schema_version_persisted() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();

    // Initial migration
    {
        let _store = open_store(path).unwrap();
        // migrate() is called in open_store()
    }

    // Verify schema version after restart
    {
        // The schema version should be persisted
        let _conn = rusqlite::Connection::open(path).unwrap();
        let version: Option<i64> = _conn
            .query_row("SELECT MAX(version) FROM schema_versions", [], |row| {
                row.get(0)
            })
            .unwrap();

        // Should have a version (not None)
        assert!(version.is_some());
        // Should be at least 3 (our current migration version)
        assert!(version.unwrap() >= 3);
    }
}

#[test]
fn test_migration_not_reapplied() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();

    // First open applies migrations
    {
        let _store = open_store(path).unwrap();
    }

    // Second open should not re-apply migrations (idempotent)
    {
        let _store = open_store(path).unwrap();
    }

    // Verify schema_versions only has entries for migrations applied once
    {
        let conn = rusqlite::Connection::open(path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM schema_versions", [], |row| row.get(0))
            .unwrap();

        // Should have exactly 3 migrations (001, 002, 003)
        assert_eq!(count, 3);
    }
}

#[test]
fn test_alias_history_survives_restart() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path();

    // Create alias
    {
        let store = open_store(path).unwrap();
        let alias = NewAlias {
            name: "flip-alias".to_string(),
            kind: "single".to_string(),
            current_uid: Some("index-1".to_string()),
            target_uids: None,
            version: 1,
            created_at: 1714500000000,
            history: vec![],
        };
        store.create_alias(&alias).unwrap();
    }

    // Flip alias
    {
        let store = open_store(path).unwrap();
        store.flip_alias("flip-alias", "index-2", 10).unwrap();
    }

    // Verify history persisted after restart
    {
        let store = open_store(path).unwrap();
        let alias = store.get_alias("flip-alias").unwrap().unwrap();

        assert_eq!(alias.current_uid.unwrap(), "index-2");
        assert_eq!(alias.version, 2);
        assert_eq!(alias.history.len(), 1);
        assert_eq!(alias.history[0].uid, "index-1");
    }
}
