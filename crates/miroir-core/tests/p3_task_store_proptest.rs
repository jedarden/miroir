//! Phase 3 Property Tests for TaskStore (SQLite backend)
//!
//! Property tests for (insert, get) round-trip and (upsert, list) semantics
//! on the SQLite backend as required by Phase 3 DoD.

use miroir_core::task_store::*;
use miroir_core::Result;
use proptest::prelude::*;
use std::collections::HashMap;
use tempfile::NamedTempFile;

/// Helper to create an in-memory SQLite store for testing
fn create_test_store() -> Result<miroir_core::task_store::SqliteTaskStore> {
    let store = SqliteTaskStore::open_in_memory()?;
    store.migrate()?;
    Ok(store)
}

/// Helper to create a test task
fn new_test_task(miroir_id: String) -> NewTask {
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-0".to_string(), 42);
    node_tasks.insert("node-1".to_string(), 17);

    let mut node_errors = HashMap::new();
    node_errors.insert("node-0".to_string(), "".to_string());
    node_errors.insert("node-1".to_string(), "".to_string());

    NewTask {
        miroir_id,
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

// ---------------------------------------------------------------------------
// Property Tests (Table 1: tasks)
// ---------------------------------------------------------------------------

proptest! {
    /// Property: insert_task followed by get_task returns the same data (round-trip)
    #[test]
    fn prop_task_roundtrip(
        miroir_id in "[a-z0-9-]{10,30}",
        status in "enqueued|processing|succeeded|failed|canceled",
        index_uid in "[a-z]{5,15}",
        task_type in "documentAddition|documentUpdate|settingsUpdate|indexCreation"
    ) {
        let store = create_test_store().unwrap();

        let mut task = new_test_task(miroir_id.clone());
        task.status = status.clone();
        task.index_uid = Some(index_uid.clone());
        task.task_type = Some(task_type.clone());

        // Insert the task
        store.insert_task(&task).unwrap();

        // Get it back
        let retrieved = store.get_task(&miroir_id).unwrap().unwrap();

        // Verify round-trip
        prop_assert_eq!(retrieved.miroir_id, task.miroir_id);
        prop_assert_eq!(retrieved.status, task.status);
        prop_assert_eq!(retrieved.index_uid, task.index_uid);
        prop_assert_eq!(retrieved.task_type, task.task_type);
        prop_assert_eq!(retrieved.node_tasks, task.node_tasks);
        prop_assert_eq!(retrieved.error, task.error);
    }

    /// Property: list_tasks returns tasks in descending created_at order
    #[test]
    fn prop_list_tasks_ordering(count in 1..20usize) {
        let store = create_test_store().unwrap();

        let mut inserted_tasks = Vec::new();
        for i in 0..count {
            let miroir_id = format!("task-{}", i);
            let mut task = new_test_task(miroir_id.clone());
            task.created_at = 1714500000000 + (i as i64 * 1000);

            store.insert_task(&task).unwrap();
            inserted_tasks.push(task);
        }

        // List all tasks
        let filter = TaskFilter {
            status: None,
            index_uid: None,
            task_type: None,
            limit: None,
            offset: None,
        };

        let retrieved = store.list_tasks(&filter).unwrap();

        // Verify count matches
        prop_assert_eq!(retrieved.len(), count);

        // Verify descending order by created_at
        for i in 0..retrieved.len().saturating_sub(1) {
            prop_assert!(retrieved[i].created_at >= retrieved[i+1].created_at);
        }
    }

    /// Property: list_tasks with status filter returns only matching tasks
    #[test]
    fn prop_list_tasks_filter_by_status(
        tasks in prop::collection::vec(
            (("[a-z0-9-]{10,20}", "enqueued|processing|succeeded|failed|canceled")),
            1..20
        )
    ) {
        let store = create_test_store().unwrap();

        // Insert tasks with various statuses
        for (miroir_id, status) in &tasks {
            let mut task = new_test_task(miroir_id.clone());
            task.status = status.clone();
            store.insert_task(&task).unwrap();
        }

        // Filter by each status type
        for status in &["enqueued", "processing", "succeeded", "failed", "canceled"] {
            let filter = TaskFilter {
                status: Some(status.to_string()),
                index_uid: None,
                task_type: None,
                limit: None,
                offset: None,
            };

            let retrieved = store.list_tasks(&filter).unwrap();

            // All returned tasks should have the requested status
            for task in &retrieved {
                prop_assert_eq!(&task.status, *status);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Property Tests (Table 2: node_settings_version)
// ---------------------------------------------------------------------------

proptest! {
    /// Property: upsert_node_settings_version followed by get returns same data
    #[test]
    fn prop_node_settings_version_roundtrip(
        index_uid in "[a-z]{5,15}",
        node_id in "node-[0-9]{1,3}",
        version in 0i64..1000i64
    ) {
        let store = create_test_store().unwrap();

        let updated_at = 1714500000000;

        // Upsert
        store.upsert_node_settings_version(&index_uid, &node_id, version, updated_at).unwrap();

        // Get
        let retrieved = store.get_node_settings_version(&index_uid, &node_id).unwrap().unwrap();

        prop_assert_eq!(retrieved.index_uid, index_uid);
        prop_assert_eq!(retrieved.node_id, node_id);
        prop_assert_eq!(retrieved.version, version);
        prop_assert_eq!(retrieved.updated_at, updated_at);
    }

    /// Property: upsert updates existing entry (upsert semantics)
    #[test]
    fn prop_node_settings_version_upsert(
        index_uid in "[a-z]{5,15}",
        node_id in "node-[0-9]{1,3}",
        version1 in 0i64..100i64,
        version2 in 100i64..200i64
    ) {
        prop_assume!(version1 != version2);
        let store = create_test_store().unwrap();

        let updated_at1 = 1714500000000;
        let updated_at2 = 1714500001000;

        // Insert
        store.upsert_node_settings_version(&index_uid, &node_id, version1, updated_at1).unwrap();

        // Update
        store.upsert_node_settings_version(&index_uid, &node_id, version2, updated_at2).unwrap();

        // Verify only one entry exists with updated values
        let retrieved = store.get_node_settings_version(&index_uid, &node_id).unwrap().unwrap();

        prop_assert_eq!(retrieved.version, version2);
        prop_assert_eq!(retrieved.updated_at, updated_at2);
    }
}

// ---------------------------------------------------------------------------
// Property Tests (Table 3: aliases)
// ---------------------------------------------------------------------------

proptest! {
    /// Property: create_alias followed by get_alias returns same data
    #[test]
    fn prop_alias_roundtrip(
        name in "[a-z]{5,20}",
        kind in "single|multi",
        current_uid in "[a-z]{5,15}",
        version in 1i64..100i64
    ) {
        let store = create_test_store().unwrap();

        let target_uids = if kind == "multi" {
            Some(vec!["index-1".to_string(), "index-2".to_string()])
        } else {
            None
        };

        let alias = NewAlias {
            name: name.clone(),
            kind: kind.to_string(),
            current_uid: if kind == "single" { Some(current_uid.clone()) } else { None },
            target_uids,
            version,
            created_at: 1714500000000,
            history: vec![],
        };

        store.create_alias(&alias).unwrap();

        let retrieved = store.get_alias(&name).unwrap().unwrap();

        prop_assert_eq!(retrieved.name, alias.name);
        prop_assert_eq!(retrieved.kind, alias.kind);
        prop_assert_eq!(retrieved.current_uid, alias.current_uid);
        prop_assert_eq!(retrieved.version, alias.version);
    }

    /// Property: flip_alias increments version and records history
    #[test]
    fn prop_alias_flip_increments_version(
        name in "[a-z]{5,20}",
        uid1 in "[a-z]{5,15}",
        uid2 in "[a-z]{5,15}"
    ) {
        prop_assume!(uid1 != uid2);
        let store = create_test_store().unwrap();

        let alias = NewAlias {
            name: name.clone(),
            kind: "single".to_string(),
            current_uid: Some(uid1.clone()),
            target_uids: None,
            version: 1,
            created_at: 1714500000000,
            history: vec![],
        };

        store.create_alias(&alias).unwrap();

        // Flip to new UID
        store.flip_alias(&name, &uid2, 10).unwrap();

        let retrieved = store.get_alias(&name).unwrap().unwrap();

        prop_assert_eq!(retrieved.current_uid.as_ref().unwrap(), &uid2);
        prop_assert_eq!(retrieved.version, 2);
        prop_assert_eq!(retrieved.history.len(), 1);
        prop_assert_eq!(retrieved.history[0].uid, uid1);
    }
}

// ---------------------------------------------------------------------------
// Property Tests (Table 4: sessions)
// ---------------------------------------------------------------------------

proptest! {
    /// Property: upsert_session followed by get_session returns same data
    #[test]
    fn prop_session_roundtrip(
        session_id in "[a-z0-9]{20,40}",
        mtask_id in "mtask-[0-9]{5}",
        pinned_group in 0i64..5i64
    ) {
        let store = create_test_store().unwrap();

        let session = SessionRow {
            session_id: session_id.clone(),
            last_write_mtask_id: Some(mtask_id.clone()),
            last_write_at: Some(1714500000000),
            pinned_group: Some(pinned_group),
            min_settings_version: 1,
            ttl: 1714500100000,
        };

        store.upsert_session(&session).unwrap();

        let retrieved = store.get_session(&session_id).unwrap().unwrap();

        prop_assert_eq!(retrieved.session_id, session.session_id);
        prop_assert_eq!(retrieved.last_write_mtask_id, session.last_write_mtask_id);
        prop_assert_eq!(retrieved.pinned_group, session.pinned_group);
        prop_assert_eq!(retrieved.min_settings_version, session.min_settings_version);
    }

    /// Property: upsert_session updates existing session (upsert semantics)
    #[test]
    fn prop_session_upsert(
        session_id in "[a-z0-9]{20,40}"
    ) {
        let store = create_test_store().unwrap();

        let session1 = SessionRow {
            session_id: session_id.clone(),
            last_write_mtask_id: Some("mtask-1".to_string()),
            last_write_at: Some(1714500000000),
            pinned_group: Some(0),
            min_settings_version: 1,
            ttl: 1714500100000,
        };

        let session2 = SessionRow {
            session_id: session_id.clone(),
            last_write_mtask_id: Some("mtask-2".to_string()),
            last_write_at: Some(1714500001000),
            pinned_group: Some(1),
            min_settings_version: 2,
            ttl: 1714500200000,
        };

        store.upsert_session(&session1).unwrap();
        store.upsert_session(&session2).unwrap();

        let retrieved = store.get_session(&session_id).unwrap().unwrap();

        // Should have the second session's values
        prop_assert_eq!(retrieved.last_write_mtask_id.unwrap(), "mtask-2");
        prop_assert_eq!(retrieved.pinned_group.unwrap(), 1);
        prop_assert_eq!(retrieved.min_settings_version, 2);
    }
}

// ---------------------------------------------------------------------------
// Property Tests (Table 5: idempotency_cache)
// ---------------------------------------------------------------------------

proptest! {
    /// Property: insert_idempotency_entry followed by get returns same data
    #[test]
    fn prop_idempotency_roundtrip(
        key in "[a-z0-9-]{20,50}",
        miroir_task_id in "mtask-[0-9]{5}"
    ) {
        let store = create_test_store().unwrap();

        let body_sha256 = sha2::Sha256::digest(b"test body");
        let entry = IdempotencyEntry {
            key: key.clone(),
            body_sha256: body_sha256.to_vec(),
            miroir_task_id: miroir_task_id.clone(),
            expires_at: 1714500100000,
        };

        store.insert_idempotency_entry(&entry).unwrap();

        let retrieved = store.get_idempotency_entry(&key).unwrap().unwrap();

        prop_assert_eq!(retrieved.key, key);
        prop_assert_eq!(retrieved.body_sha256, body_sha256.to_vec());
        prop_assert_eq!(retrieved.miroir_task_id, miroir_task_id);
    }
}

// ---------------------------------------------------------------------------
// Property Tests (Table 6: jobs)
// ---------------------------------------------------------------------------

proptest! {
    /// Property: insert_job followed by get_job returns same data
    #[test]
    fn prop_job_roundtrip(
        id in "job-[a-z0-9-]{10,30}",
        type_ in "dump_import|reshard_backfill|canary_run",
        state in "queued|in_progress|completed|failed"
    ) {
        let store = create_test_store().unwrap();

        let job = NewJob {
            id: id.clone(),
            type_: type_.clone(),
            params: r#"{"test": "param"}"#.to_string(),
            state: state.clone(),
            progress: r#"{"status": "starting"}"#.to_string(),
        };

        store.insert_job(&job).unwrap();

        let retrieved = store.get_job(&id).unwrap().unwrap();

        prop_assert_eq!(retrieved.id, id);
        prop_assert_eq!(retrieved.type_, type_);
        prop_assert_eq!(retrieved.state, state);
    }

    /// Property: claim_job only succeeds when state is 'queued' (CAS semantics)
    #[test]
    fn prop_job_claim_cas(
        id in "job-[a-z0-9-]{10,30}"
    ) {
        let store = create_test_store().unwrap();

        let job = NewJob {
            id: id.clone(),
            type_: "test".to_string(),
            params: "{}".to_string(),
            state: "queued".to_string(),
            progress: "{}".to_string(),
        };

        store.insert_job(&job).unwrap();

        // First claim should succeed
        let claimed = store.claim_job(&id, "pod-1", 1714500100000).unwrap();
        prop_assert!(claimed);

        // Second claim should fail (already claimed)
        let claimed2 = store.claim_job(&id, "pod-2", 1714500200000).unwrap();
        prop_assert!(!claimed2);
    }
}

// ---------------------------------------------------------------------------
// Property Tests (Table 7: leader_lease)
// ---------------------------------------------------------------------------

proptest! {
    /// Property: try_acquire_leader_lease with new scope succeeds
    #[test]
    fn prop_leader_lease_acquire(
        scope in "[a-z]{5,20}:[a-z]{5,20}",
        holder in "pod-[0-9]{1,3}"
    ) {
        let store = create_test_store().unwrap();

        let expires_at = 1714500100000;
        let now_ms = 1714500000000;

        let acquired = store.try_acquire_leader_lease(&scope, &holder, expires_at, now_ms).unwrap();

        prop_assert!(acquired);

        let retrieved = store.get_leader_lease(&scope).unwrap().unwrap();

        prop_assert_eq!(retrieved.scope, scope);
        prop_assert_eq!(retrieved.holder, holder);
        prop_assert_eq!(retrieved.expires_at, expires_at);
    }

    /// Property: renew_leader_lease only succeeds if we hold the lease
    #[test]
    fn prop_leader_lease_renew(
        scope in "[a-z]{5,20}:[a-z]{5,20}"
    ) {
        let store = create_test_store().unwrap();

        let holder1 = "pod-1";
        let holder2 = "pod-2";
        let expires_at1 = 1714500100000;
        let expires_at2 = 1714500200000;
        let now_ms = 1714500000000;

        // Acquire with holder1
        store.try_acquire_leader_lease(&scope, holder1, expires_at1, now_ms).unwrap();

        // Renew with holder1 should succeed
        let renewed = store.renew_leader_lease(&scope, holder1, expires_at2).unwrap();
        prop_assert!(renewed);

        // Renew with holder2 should fail
        let renewed2 = store.renew_leader_lease(&scope, holder2, expires_at2).unwrap();
        prop_assert!(!renewed2);
    }
}

// ---------------------------------------------------------------------------
// Property Tests (Tables 8-14: Feature tables)
// ---------------------------------------------------------------------------

proptest! {
    /// Property: upsert_canary followed by get_canary returns same data
    #[test]
    fn prop_canary_roundtrip(
        id in "[a-z0-9-]{10,30}",
        name in "[a-z]{5,20}",
        index_uid in "[a-z]{5,15}",
        interval_s in 30i64..3600i64
    ) {
        let store = create_test_store().unwrap();

        let canary = NewCanary {
            id: id.clone(),
            name: name.clone(),
            index_uid: index_uid.clone(),
            interval_s,
            query_json: r#"{"q": "test"}"#.to_string(),
            assertions_json: r#"[{"type": "min_hits", "value": 1}]"#.to_string(),
            enabled: true,
            created_at: 1714500000000,
        };

        store.upsert_canary(&canary).unwrap();

        let retrieved = store.get_canary(&id).unwrap().unwrap();

        prop_assert_eq!(retrieved.id, id);
        prop_assert_eq!(retrieved.name, name);
        prop_assert_eq!(retrieved.index_uid, index_uid);
        prop_assert_eq!(retrieved.interval_s, interval_s);
        prop_assert_eq!(retrieved.enabled, true);
    }

    /// Property: insert_canary_run with auto-prune keeps only N most recent runs
    #[test]
    fn prop_canary_run_pruning(
        canary_id in "[a-z0-9-]{10,20}"
    ) {
        let store = create_test_store().unwrap();
        let history_limit = 5;

        // Insert more runs than the limit
        for i in 0..10 {
            let run = NewCanaryRun {
                canary_id: canary_id.clone(),
                ran_at: 1714500000000 + (i as i64 * 1000),
                status: "pass".to_string(),
                latency_ms: 100,
                failed_assertions_json: None,
            };
            store.insert_canary_run(&run, history_limit).unwrap();
        }

        // Should only have 5 runs (the most recent)
        let runs = store.get_canary_runs(&canary_id, 100).unwrap();

        prop_assert_eq!(runs.len(), 5);

        // Verify they're in descending order by ran_at
        for i in 0..runs.len().saturating_sub(1) {
            prop_assert!(runs[i].ran_at >= runs[i+1].ran_at);
        }
    }

    /// Property: upsert_cdc_cursor followed by get_cdc_cursor returns same data
    #[test]
    fn prop_cdc_cursor_roundtrip(
        sink_name in "[a-z]{5,15}",
        index_uid in "[a-z]{5,15}",
        last_event_seq in 0i64..10000i64
    ) {
        let store = create_test_store().unwrap();

        let cursor = NewCdcCursor {
            sink_name: sink_name.clone(),
            index_uid: index_uid.clone(),
            last_event_seq,
            updated_at: 1714500000000,
        };

        store.upsert_cdc_cursor(&cursor).unwrap();

        let retrieved = store.get_cdc_cursor(&sink_name, &index_uid).unwrap().unwrap();

        prop_assert_eq!(retrieved.sink_name, sink_name);
        prop_assert_eq!(retrieved.index_uid, index_uid);
        prop_assert_eq!(retrieved.last_event_seq, last_event_seq);
    }

    /// Property: upsert_rollover_policy followed by get_rollover_policy returns same data
    #[test]
    fn prop_rollover_policy_roundtrip(
        name in "[a-z]{5,20}",
        write_alias in "[a-z]{5,15}",
        read_alias in "[a-z]{5,15}",
        pattern in "[a-z-]{5,30}"
    ) {
        let store = create_test_store().unwrap();

        let policy = NewRolloverPolicy {
            name: name.clone(),
            write_alias: write_alias.clone(),
            read_alias: read_alias.clone(),
            pattern: pattern.clone(),
            triggers_json: r#"{"max_age": "7d"}"#.to_string(),
            retention_json: r#"{"keep_indexes": 5}"#.to_string(),
            template_json: r#"{"primary_key": "id"}"#.to_string(),
            enabled: true,
        };

        store.upsert_rollover_policy(&policy).unwrap();

        let retrieved = store.get_rollover_policy(&name).unwrap().unwrap();

        prop_assert_eq!(retrieved.name, name);
        prop_assert_eq!(retrieved.write_alias, write_alias);
        prop_assert_eq!(retrieved.read_alias, read_alias);
        prop_assert_eq!(retrieval.pattern, pattern);
        prop_assert_eq!(retrieval.enabled, true);
    }

    /// Property: upsert_search_ui_config followed by get_search_ui_config returns same data
    #[test]
    fn prop_search_ui_config_roundtrip(
        index_uid in "[a-z]{5,15}"
    ) {
        let store = create_test_store().unwrap();

        let config_json = r#"{"title": "Test Search"}"#.to_string();
        let config = NewSearchUiConfig {
            index_uid: index_uid.clone(),
            config_json: config_json.clone(),
            updated_at: 1714500000000,
        };

        store.upsert_search_ui_config(&config).unwrap();

        let retrieved = store.get_search_ui_config(&index_uid).unwrap().unwrap();

        prop_assert_eq!(retrieved.index_uid, index_uid);
        prop_assert_eq!(retrieval.config_json, config_json);
    }

    /// Property: insert_admin_session followed by get_admin_session returns same data
    #[test]
    fn prop_admin_session_roundtrip(
        session_id in "[a-z0-9-]{30,60}",
        csrf_token in "[a-z0-9-]{30,60}"
    ) {
        let store = create_test_store().unwrap();

        let session = NewAdminSession {
            session_id: session_id.clone(),
            csrf_token: csrf_token.clone(),
            admin_key_hash: "abc123".to_string(),
            created_at: 1714500000000,
            expires_at: 1714503600000,
            user_agent: Some("Mozilla/5.0".to_string()),
            source_ip: Some("10.0.0.1".to_string()),
        };

        store.insert_admin_session(&session).unwrap();

        let retrieved = store.get_admin_session(&session_id).unwrap().unwrap();

        prop_assert_eq!(retrieved.session_id, session_id);
        prop_assert_eq!(retrieved.csrf_token, csrf_token);
        prop_assert_eq!(retrieval.revoked, false);
        prop_assert_eq!(retrieved.user_agent, session.user_agent);
        prop_assert_eq!(retrieved.source_ip, session.source_ip);
    }

    /// Property: revoke_admin_session sets revoked to true
    #[test]
    fn prop_admin_session_revoke(
        session_id in "[a-z0-9-]{30,60}"
    ) {
        let store = create_test_store().unwrap();

        let session = NewAdminSession {
            session_id: session_id.clone(),
            csrf_token: "csrf-token".to_string(),
            admin_key_hash: "abc123".to_string(),
            created_at: 1714500000000,
            expires_at: 1714503600000,
            user_agent: None,
            source_ip: None,
        };

        store.insert_admin_session(&session).unwrap();

        // Revoke
        store.revoke_admin_session(&session_id).unwrap();

        let retrieved = store.get_admin_session(&session_id).unwrap().unwrap();

        prop_assert_eq!(retrieval.revoked, true);
    }
}
