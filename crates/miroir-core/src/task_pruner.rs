//! Background TTL pruner for the tasks table (plan §4, Phase 3).
//!
//! Phase 6 §14.5 Mode A: Each pod prunes tasks where it wins the rendezvous
//! score for the task's `miroir_id`. This partitions pruning work across pods
//! without coordination overhead. Single-pod deployments still use the
//! advisory lock via `leader_lease` table.

use crate::config::TaskRegistryConfig;
use crate::task_store::TaskStore;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// Prometheus-style gauge exposed per plan §10.
/// Updated by the pruner after each cycle.
static TASK_REGISTRY_SIZE: AtomicU64 = AtomicU64::new(0);

/// Read the current `miroir_task_registry_size` gauge value.
pub fn task_registry_size() -> u64 {
    TASK_REGISTRY_SIZE.load(Ordering::Relaxed)
}

/// Advisory lock scope used by the pruner.
const LOCK_SCOPE: &str = "pruner:task_ttl";

/// Holder identity for this pruner instance.
fn holder_id() -> String {
    format!("pruner-{}", std::process::id())
}

/// Run a single pruner iteration. Returns the number of tasks deleted.
///
/// **Mode A (multi-pod)**: Each pod prunes only the tasks it owns via rendezvous hashing.
/// No advisory lock needed — ownership is deterministic from miroir_id.
///
/// **Legacy (single-pod)**: Uses advisory lock to ensure only one pod prunes.
///
/// # Arguments
///
/// * `store` - Task store
/// * `cfg` - Task registry configuration
/// * `mode_a_owner_fn` - Optional Mode A ownership function: `fn(miroir_id: &str) -> bool`
///                      If provided, only prunes tasks where this returns true.
pub fn prune_once<F>(
    store: &dyn TaskStore,
    cfg: &TaskRegistryConfig,
    mode_a_owner_fn: Option<F>,
) -> usize
where
    F: Fn(&str) -> bool,
{
    // Mode A: No lock needed, partition by miroir_id
    if let Some(owner_fn) = mode_a_owner_fn {
        return prune_inner_mode_a(store, cfg, owner_fn);
    }

    // Legacy: Use advisory lock for single-pod deployments
    let holder = holder_id();
    let now = now_ms();
    let lease_duration_ms = (cfg.prune_interval_s * 1000) + 30_000; // interval + 30s buffer
    let expires_at = now + lease_duration_ms as i64;

    // Step 1: advisory lock
    let acquired = match store.try_acquire_leader_lease(LOCK_SCOPE, &holder, expires_at, now) {
        Ok(true) => true,
        Ok(false) => {
            debug!("pruner: another instance holds the lock, skipping");
            return 0;
        }
        Err(e) => {
            error!("pruner: failed to acquire lock: {e}");
            return 0;
        }
    };

    let result = prune_inner(store, cfg);

    // Release lock
    if acquired {
        if let Err(e) = store.renew_leader_lease(LOCK_SCOPE, &holder, now) {
            warn!("pruner: failed to release lock: {e}");
        }
    }

    result
}

/// Mode A pruning: partition tasks by miroir_id ownership.
fn prune_inner_mode_a<F>(store: &dyn TaskStore, cfg: &TaskRegistryConfig, owner_fn: F) -> usize
where
    F: Fn(&str) -> bool,
{
    let now = now_ms();
    let cutoff = now - (cfg.ttl_seconds * 1000) as i64;

    debug!(
        "pruner: running Mode A with cutoff={cutoff}, batch_size={}",
        cfg.prune_batch_size
    );

    let mut total_deleted = 0usize;
    let mut offset = 0i64;
    let batch_size = cfg.prune_batch_size as i64;

    loop {
        // List tasks in batches
        match store.list_terminal_tasks_batch(cutoff, offset, batch_size) {
            Ok(tasks) => {
                if tasks.is_empty() {
                    break;
                }

                // Filter to only tasks we own
                let owned_tasks: Vec<_> = tasks
                    .iter()
                    .filter(|t| owner_fn(&t.miroir_id))
                    .map(|t| t.miroir_id.as_str())
                    .collect();

                if !owned_tasks.is_empty() {
                    match store.delete_tasks_batch(&owned_tasks) {
                        Ok(deleted) => {
                            total_deleted += deleted;
                        }
                        Err(e) => {
                            error!("pruner: delete batch failed: {e}");
                            break;
                        }
                    }
                }

                if tasks.len() < cfg.prune_batch_size as usize {
                    break; // no more rows
                }
                offset += batch_size;
            }
            Err(e) => {
                error!("pruner: list tasks failed: {e}");
                break;
            }
        }
    }

    // Update gauge
    match store.task_count() {
        Ok(count) => {
            TASK_REGISTRY_SIZE.store(count, Ordering::Relaxed);
            info!("pruner: deleted {total_deleted} tasks (Mode A), registry_size={count}");
        }
        Err(e) => {
            error!("pruner: failed to count tasks: {e}");
        }
    }

    total_deleted
}

fn prune_inner(store: &dyn TaskStore, cfg: &TaskRegistryConfig) -> usize {
    let now = now_ms();
    let cutoff = now - (cfg.ttl_seconds * 1000) as i64;

    debug!(
        "pruner: running with cutoff={cutoff}, batch_size={}",
        cfg.prune_batch_size
    );

    let mut total_deleted = 0usize;
    loop {
        match store.prune_tasks(cutoff, cfg.prune_batch_size) {
            Ok(deleted) => {
                total_deleted += deleted;
                if deleted < cfg.prune_batch_size as usize {
                    break; // no more rows to prune
                }
            }
            Err(e) => {
                error!("pruner: delete batch failed: {e}");
                break;
            }
        }
    }

    // Update gauge
    match store.task_count() {
        Ok(count) => {
            TASK_REGISTRY_SIZE.store(count, Ordering::Relaxed);
            info!("pruner: deleted {total_deleted} tasks, registry_size={count}");
        }
        Err(e) => {
            error!("pruner: failed to count tasks: {e}");
        }
    }

    total_deleted
}

/// Spawn a background thread that runs `prune_once` on a fixed interval.
///
/// Call this once at startup. The thread is daemon-like: it exits when
/// the returned `PrunerHandle` is dropped or the process exits.
///
/// # Arguments
///
/// * `store` - Task store
/// * `cfg` - Task registry configuration
/// * `mode_a_owner_fn` - Optional Mode A ownership function for multi-pod deployments
pub fn spawn_pruner<F>(
    store: Arc<dyn TaskStore>,
    cfg: TaskRegistryConfig,
    mode_a_owner_fn: Option<F>,
) -> PrunerHandle
where
    F: Fn(&str) -> bool + Send + 'static,
{
    let interval = Duration::from_secs(cfg.prune_interval_s);
    let stop = std::sync::atomic::AtomicBool::new(false);
    let stop_flag = Arc::new(stop);

    let flag_ref = Arc::clone(&stop_flag);
    let handle = thread::Builder::new()
        .name("miroir-task-pruner".into())
        .spawn(move || {
            info!(
                "pruner: starting with interval={}s ttl={}s",
                cfg.prune_interval_s, cfg.ttl_seconds
            );
            loop {
                if flag_ref.load(Ordering::Relaxed) {
                    info!("pruner: stopping");
                    break;
                }
                let start = Instant::now();
                // Call prune_once with the ownership function
                if let Some(ref owner_fn) = mode_a_owner_fn {
                    prune_once(store.as_ref(), &cfg, Some(owner_fn));
                } else {
                    prune_once(store.as_ref(), &cfg, None::<fn(&str) -> bool>);
                }
                let elapsed = start.elapsed();
                if elapsed < interval {
                    // Sleep in small increments to check stop flag
                    let remaining = interval - elapsed;
                    let check_interval = Duration::from_secs(1);
                    let mut slept = Duration::ZERO;
                    while slept < remaining {
                        if flag_ref.load(Ordering::Relaxed) {
                            info!("pruner: stopping during sleep");
                            return;
                        }
                        let sleep_dur = remaining - slept;
                        let sleep_dur = sleep_dur.min(check_interval);
                        thread::sleep(sleep_dur);
                        slept += sleep_dur;
                    }
                }
            }
        })
        .expect("failed to spawn pruner thread");

    PrunerHandle {
        handle: Some(handle),
        stop_flag,
    }
}

/// Handle to the background pruner thread. Dropping this signals the
/// pruner to stop and joins the thread.
pub struct PrunerHandle {
    handle: Option<thread::JoinHandle<()>>,
    stop_flag: Arc<std::sync::atomic::AtomicBool>,
}

impl PrunerHandle {
    /// Signal the pruner to stop and wait for it to finish.
    pub fn stop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for PrunerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TaskRegistryConfig;
    use crate::task_store::{NewTask, SqliteTaskStore, TaskStore};
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Serialize tests that read/write the global `TASK_REGISTRY_SIZE` gauge.
    static GAUGE_LOCK: Mutex<()> = Mutex::new(());

    fn test_store() -> SqliteTaskStore {
        let store = SqliteTaskStore::open_in_memory().unwrap();
        store.migrate().unwrap();
        store
    }

    fn default_cfg() -> TaskRegistryConfig {
        TaskRegistryConfig::default()
    }

    /// Helper: insert a task with given id, created_at, status.
    fn insert_task(store: &dyn TaskStore, id: &str, created_at: i64, status: &str) {
        store
            .insert_task(&NewTask {
                miroir_id: id.to_string(),
                created_at,
                status: status.to_string(),
                node_tasks: HashMap::new(),
                error: None,
                started_at: None,
                finished_at: None,
                index_uid: None,
                task_type: None,
                node_errors: HashMap::new(),
            })
            .unwrap();
    }

    /// Helper: return current time in ms.
    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// Acceptance: After insert of 10k terminal tasks with created_at = now - 8d,
    /// next pruner cycle drops all 10k.
    #[test]
    fn pruner_deletes_10k_old_terminal_tasks() {
        let _lock = GAUGE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = test_store();
        let eight_days_ms: i64 = 8 * 24 * 3600 * 1000;
        let old_time = now() - eight_days_ms;

        // Insert 10k terminal tasks at old_time
        for i in 0..10_000 {
            let status = match i % 3 {
                0 => "succeeded",
                1 => "failed",
                _ => "canceled",
            };
            insert_task(&store, &format!("old-{i}"), old_time, status);
        }

        assert_eq!(store.task_count().unwrap(), 10_000);

        let mut cfg = default_cfg();
        cfg.ttl_seconds = 7 * 24 * 3600; // 7 days
        let deleted = prune_once(&store, &cfg, None::<fn(&str) -> bool>);

        assert_eq!(deleted, 10_000);
        assert_eq!(store.task_count().unwrap(), 0);
        assert_eq!(task_registry_size(), 0);
    }

    /// Acceptance: A single in-flight `processing` task at created_at = now - 10d is preserved.
    #[test]
    fn pruner_preserves_processing_tasks() {
        let _lock = GAUGE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = test_store();
        let ten_days_ms: i64 = 10 * 24 * 3600 * 1000;
        let old_time = now() - ten_days_ms;

        // Insert an old processing task
        insert_task(&store, "processing-old", old_time, "processing");

        // Also insert old terminal tasks that should be deleted
        insert_task(&store, "succeeded-old", old_time, "succeeded");
        insert_task(&store, "failed-old", old_time, "failed");

        assert_eq!(store.task_count().unwrap(), 3);

        let cfg = default_cfg();
        let deleted = prune_once(&store, &cfg, None::<fn(&str) -> bool>);

        assert_eq!(deleted, 2);
        assert!(store.get_task("processing-old").unwrap().is_some());
        assert!(store.get_task("succeeded-old").unwrap().is_none());
        assert!(store.get_task("failed-old").unwrap().is_none());
        assert_eq!(store.task_count().unwrap(), 1);
    }

    /// Acceptance: Pruner advisory lock prevents two instances pruning simultaneously.
    #[test]
    fn advisory_lock_prevents_concurrent_pruning() {
        let _lock = GAUGE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = test_store();
        let ten_days_ms: i64 = 10 * 24 * 3600 * 1000;
        let old_time = now() - ten_days_ms;

        // Insert old tasks
        for i in 0..100 {
            insert_task(&store, &format!("old-{i}"), old_time, "succeeded");
        }

        let cfg = default_cfg();

        // Manually acquire the lock as another instance
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let other_holder = "other-pruner-999";
        store
            .try_acquire_leader_lease(LOCK_SCOPE, other_holder, now + 600_000, now)
            .unwrap();

        // prune_once should see the lock held and skip
        let deleted = prune_once(&store, &cfg, None::<fn(&str) -> bool>);
        assert_eq!(deleted, 0);
        // Tasks should still be there
        assert_eq!(store.task_count().unwrap(), 100);
    }

    /// Acceptance: miroir_task_registry_size gauge drops after a prune cycle.
    #[test]
    fn gauge_drops_after_prune() {
        let _lock = GAUGE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = test_store();
        let ten_days_ms: i64 = 10 * 24 * 3600 * 1000;
        let old_time = now() - ten_days_ms;

        // Insert 5 old + 5 recent tasks
        for i in 0..5 {
            insert_task(&store, &format!("old-{i}"), old_time, "succeeded");
        }
        for i in 0..5 {
            insert_task(&store, &format!("new-{i}"), now(), "succeeded");
        }

        assert_eq!(store.task_count().unwrap(), 10);

        let cfg = default_cfg();
        prune_once(&store, &cfg, None::<fn(&str) -> bool>);

        // Gauge should reflect remaining tasks
        assert_eq!(task_registry_size(), 5);
        assert_eq!(store.task_count().unwrap(), 5);
    }

    /// Test that pruner respects batch_size — multiple iterations needed.
    #[test]
    fn pruner_batches_correctly() {
        let _lock = GAUGE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = test_store();
        let ten_days_ms: i64 = 10 * 24 * 3600 * 1000;
        let old_time = now() - ten_days_ms;

        for i in 0..25 {
            insert_task(&store, &format!("old-{i}"), old_time, "succeeded");
        }

        let mut cfg = default_cfg();
        cfg.prune_batch_size = 10; // small batch
        let deleted = prune_once(&store, &cfg, None::<fn(&str) -> bool>);

        assert_eq!(deleted, 25); // all deleted via multiple batches
        assert_eq!(store.task_count().unwrap(), 0);
    }

    /// Acceptance: spawn_pruner runs in background, PrunerHandle::stop joins cleanly.
    #[test]
    fn spawn_pruner_runs_and_stops() {
        let _lock = GAUGE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = Arc::new(test_store());
        let ten_days_ms: i64 = 10 * 24 * 3600 * 1000;
        let old_time = now() - ten_days_ms;

        for i in 0..5 {
            insert_task(store.as_ref(), &format!("old-{i}"), old_time, "succeeded");
        }

        let mut cfg = default_cfg();
        cfg.prune_interval_s = 1;
        let mut handle = spawn_pruner(store.clone(), cfg, None::<fn(&str) -> bool>);

        // Give the pruner a moment to run at least one cycle
        thread::sleep(Duration::from_millis(200));
        handle.stop();

        // Old tasks should be pruned
        assert_eq!(store.task_count().unwrap(), 0);
    }

    /// Acceptance: dropping PrunerHandle signals stop and joins.
    #[test]
    fn pruner_handle_drop_stops_thread() {
        let _lock = GAUGE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = Arc::new(test_store());
        let mut cfg = default_cfg();
        cfg.prune_interval_s = 600; // long interval so it sleeps in the loop
        {
            let _handle = spawn_pruner(store, cfg, None::<fn(&str) -> bool>);
            // handle dropped here
        }
        // Thread should have stopped — if this hangs, the test will time out
    }
}
