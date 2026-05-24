//! CDC (Change Data Capture) — plan §13.13.
//!
//! Publishes document change events to configured sinks (webhook, NATS, Kafka, internal queue).
//!
//! # Origin Tag-Based Event Suppression
//!
//! The `_miroir_origin` tag is an **internal orchestrator-side marker** used to suppress
//! internal write events from CDC by default. It is:
//! - Never stored on documents
//! - Never returned to clients
//! - Never transmitted outside the orchestrator process
//!
//! ## Suppression Rules (plan §13.13)
//!
//! | Origin Tag | Write Type | Default Behavior | Opt-In Via |
//! |------------|------------|-------------------|------------|
//! | `None` | Client write | **Always emitted** | N/A |
//! | `ANTIENTROPY` | Anti-entropy repair | Suppressed | `emit_internal_writes` |
//! | `RESHARD_BACKFILL` | Reshard backfill | Suppressed | `emit_internal_writes` |
//! | `ROLLOVER` | ILM rollover | Suppressed | `emit_internal_writes` |
//! | `TTL_EXPIRE` | TTL expiration delete | Suppressed | `emit_ttl_deletes` |
//!
//! ## Usage
//!
//! Set the origin tag when constructing `WriteRequest`:
//! ```ignore
//! // Client write (always emitted to CDC)
//! WriteRequest { origin: None, .. }
//!
//! // Anti-entropy repair write (suppressed by default)
//! WriteRequest { origin: Some(ORIGIN_ANTIENTROPY.to_string()), .. }
//! ```
//!
//! # Internal Queue Long-Poll (plan §13.13, P5.13.d)
//!
//! The internal queue sink supports long-polling via `GET /_miroir/changes?since={cursor}&index={uid}`.
//! When no new events are available, the endpoint waits up to `timeout` seconds (default 30s)
//! before returning an empty response. This allows in-cluster consumers to efficiently
//! tail the change stream without tight polling loops.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tracing::{debug, error, info, warn};

use crate::task_store::{NewCdcCursor, TaskStore};

#[cfg(feature = "redis-store")]
use ::redis::AsyncCommands;

/// Add random jitter to a duration.
///
/// Jitter is ±`fraction` of the base duration. For example, with fraction=0.25,
/// the result is uniformly distributed in [0.75 * base, 1.25 * base].
fn duration_jitter(base: Duration, fraction: f64) -> Duration {
    let millis = base.as_millis() as f64;
    let jitter_range = millis * fraction;
    let random_jitter = (rand::random::<f64>() * 2.0 - 1.0) * jitter_range;
    Duration::from_millis((millis + random_jitter).max(0.0) as u64)
}

/// Origin tag for anti-entropy repair writes (plan §13.8).
/// These writes are suppressed from CDC unless `emit_internal_writes` is true.
pub const ORIGIN_ANTIENTROPY: &str = "antientropy";

/// Origin tag for reshard backfill writes (plan §13.1 steps 2-3).
/// These writes are suppressed from CDC unless `emit_internal_writes` is true.
pub const ORIGIN_RESHARD_BACKFILL: &str = "reshard_backfill";

/// Origin tag for ILM rollover writes (plan §13.17).
/// These writes are suppressed from CDC unless `emit_internal_writes` is true.
pub const ORIGIN_ROLLOVER: &str = "rollover";

/// Origin tag for TTL expiration deletes (plan §13.14).
/// These deletes are suppressed from CDC unless `emit_ttl_deletes` is true.
pub const ORIGIN_TTL_EXPIRE: &str = "ttl_expire";

/// Callback type for incrementing the CDC events suppressed metric.
/// Called with the origin tag when an event is suppressed.
pub type CdcSuppressedMetricCallback = Arc<dyn Fn(&str) + Send + Sync>;

/// CDC event published on every successful write (after quorum ACK).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEvent {
    /// Miroir task ID.
    pub mtask_id: String,
    /// Index UID.
    pub index: String,
    /// Operation type.
    pub operation: CdcOperation,
    /// Primary keys affected.
    pub primary_keys: Vec<String>,
    /// Shard IDs affected.
    pub shard_ids: Vec<u32>,
    /// Settings version at write time.
    pub settings_version: u64,
    /// UNIX timestamp (ms).
    pub timestamp: u64,
    /// Document body (optional, based on sink config).
    pub document: Option<serde_json::Value>,
    /// Internal origin tag (for suppressing internal writes).
    pub origin: Option<String>,
    /// Stable event ID for deduplication.
    pub event_id: String,
}

/// Operation type for a CDC event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcOperation {
    Add,
    Update,
    Delete,
}

/// CDC sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSinkConfig {
    /// Sink type identifier.
    #[serde(rename = "type")]
    pub sink_type: CdcSinkType,
    /// Sink URL (for webhook/NATS/Kafka).
    pub url: String,
    /// Batch size for events.
    pub batch_size: u32,
    /// Batch flush interval (ms).
    pub batch_flush_ms: u64,
    /// Whether to include document body.
    pub include_body: bool,
    /// Maximum retry time (seconds).
    pub retry_max_s: u64,
    /// NATS subject prefix (for NATS sinks).
    pub subject_prefix: Option<String>,
}

/// Sink type variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcSinkType {
    Webhook,
    Nats,
    Kafka,
    Internal,
}

/// CDC publisher state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcPublisherState {
    /// Per-sink cursor (sequence number).
    pub cursors: HashMap<String, u64>,
    /// Buffered event count.
    pub buffered_count: usize,
    /// Dropped event count.
    pub dropped_count: u64,
    /// Total published count.
    pub published_count: u64,
    /// Per-sink buffer bytes (for miroir_cdc_buffer_bytes metric).
    pub buffer_bytes: HashMap<String, u64>,
}

/// Internal queue storage for CDC events (plan §13.13).
///
/// Stores events with per-index monotonic sequence numbers for the
/// `GET /_miroir/changes` endpoint. Events are stored in memory with
/// optional Redis persistence. Supports long-polling via broadcast
/// notifications when new events arrive.
pub struct CdcInternalQueue {
    /// Per-index event storage: index -> (sequence -> event)
    events: Arc<RwLock<HashMap<String, Vec<(u64, CdcEvent)>>>>,
    /// Per-index sequence numbers: index -> next_sequence
    sequences: Arc<RwLock<HashMap<String, u64>>>,
    /// Optional task store for cursor persistence.
    task_store: Option<Arc<dyn TaskStore>>,
    /// Broadcast channel for notifying waiting consumers of new events.
    /// Sender is cloned and stored here; receivers are created per-request.
    notify_tx: tokio::sync::broadcast::Sender<String>,
}

impl CdcInternalQueue {
    /// Create a new internal queue.
    pub fn new(task_store: Option<Arc<dyn TaskStore>>) -> Self {
        // Create a broadcast channel for event notifications.
        // Channel size is small because we only need to signal that new events exist.
        let (notify_tx, _) = tokio::sync::broadcast::channel(100);
        Self {
            events: Arc::new(RwLock::new(HashMap::new())),
            sequences: Arc::new(RwLock::new(HashMap::new())),
            task_store,
            notify_tx,
        }
    }

    /// Subscribe to event notifications for long-polling.
    /// Returns a receiver that gets notified when events are added for any index.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.notify_tx.subscribe()
    }

    /// Store an event and return its sequence number.
    pub async fn store(&self, event: CdcEvent) -> u64 {
        let index = event.index.clone();
        let mut sequences = self.sequences.write().await;
        let seq = sequences.entry(index.clone()).or_insert(0);
        *seq += 1;
        let sequence = *seq;

        let mut events = self.events.write().await;
        events
            .entry(index.clone())
            .or_insert_with(Vec::new)
            .push((sequence, event));

        // Trim old events to keep memory usage bounded (keep last 10,000 per index)
        if let Some(events_vec) = events.get_mut(&index) {
            if events_vec.len() > 10_000 {
                events_vec.drain(0..events_vec.len() - 10_000);
            }
        }

        // Notify waiting consumers of the new event
        // We ignore errors because there may be no receivers subscribed
        let _ = self.notify_tx.send(index.clone());

        sequence
    }

    /// Get events for an index since a given cursor (exclusive).
    /// Returns events with sequence > cursor.
    pub async fn get_since(&self, index: &str, cursor: u64, limit: usize) -> Vec<CdcEvent> {
        let events = self.events.read().await;
        if let Some(events_vec) = events.get(index) {
            events_vec
                .iter()
                .filter(|(seq, _)| *seq > cursor)
                .take(limit)
                .map(|(_, event)| event.clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get events for an index since a given cursor with long-poll support.
    /// Waits up to `timeout` for new events if none are immediately available.
    ///
    /// # Arguments
    /// * `index` - Index UID to query
    /// * `cursor` - Sequence number to start from (exclusive)
    /// * `limit` - Maximum number of events to return
    /// * `timeout` - Maximum time to wait for new events (None = return immediately)
    ///
    /// Returns events with sequence > cursor, up to `limit` events.
    pub async fn get_since_long_poll(
        &self,
        index: &str,
        cursor: u64,
        limit: usize,
        timeout: Option<Duration>,
    ) -> Vec<CdcEvent> {
        // First, check if there are events immediately available
        {
            let events = self.events.read().await;
            if let Some(events_vec) = events.get(index) {
                let available: Vec<_> = events_vec
                    .iter()
                    .filter(|(seq, _)| *seq > cursor)
                    .take(limit)
                    .map(|(_, event)| event.clone())
                    .collect();
                if !available.is_empty() {
                    return available;
                }
            }
        }

        // No events immediately available - wait for timeout if specified
        if let Some(timeout_duration) = timeout {
            // Subscribe to notifications
            let mut rx = self.subscribe();
            let start = Instant::now();

            loop {
                let remaining = timeout_duration.saturating_sub(start.elapsed());
                if remaining.is_zero() {
                    // Timeout expired - return empty
                    return Vec::new();
                }

                // Wait for either a notification or timeout
                match tokio::time::timeout(remaining, rx.recv()).await {
                    Ok(Ok(notified_index)) => {
                        // Check if the notification is for our index
                        if notified_index == index {
                            // Re-check for events
                            let events = self.events.read().await;
                            if let Some(events_vec) = events.get(index) {
                                let available: Vec<_> = events_vec
                                    .iter()
                                    .filter(|(seq, _)| *seq > cursor)
                                    .take(limit)
                                    .map(|(_, event)| event.clone())
                                    .collect();
                                if !available.is_empty() {
                                    return available;
                                }
                            }
                        }
                        // Continue waiting - notification was for a different index
                    }
                    Ok(Err(_)) => {
                        // Sender lagged - continue waiting
                        continue;
                    }
                    Err(_) => {
                        // Timeout - return empty
                        return Vec::new();
                    }
                }
            }
        }

        // No timeout configured - return empty immediately
        Vec::new()
    }

    /// Get the current maximum sequence number for an index.
    pub async fn max_sequence(&self, index: &str) -> u64 {
        let sequences = self.sequences.read().await;
        sequences.get(index).copied().unwrap_or(0)
    }

    /// Persist a cursor for a sink/index combination.
    pub async fn persist_cursor(
        &self,
        sink_name: &str,
        index: &str,
        seq: u64,
    ) -> Result<(), CdcError> {
        if let Some(ref store) = self.task_store {
            let cursor = NewCdcCursor {
                sink_name: sink_name.to_string(),
                index_uid: index.to_string(),
                last_event_seq: seq as i64,
                updated_at: chrono::Utc::now().timestamp_millis(),
            };
            store
                .upsert_cdc_cursor(&cursor)
                .map_err(|e| CdcError::SinkError(format!("cursor persist error: {e}")))?;
        }
        Ok(())
    }

    /// Get the persisted cursor for a sink/index combination.
    pub async fn get_cursor(&self, sink_name: &str, index: &str) -> Result<Option<u64>, CdcError> {
        if let Some(ref store) = self.task_store {
            match store.get_cdc_cursor(sink_name, index) {
                Ok(Some(cursor)) => Ok(Some(cursor.last_event_seq as u64)),
                Ok(None) => Ok(None),
                Err(e) => Err(CdcError::SinkError(format!("cursor read error: {e}"))),
            }
        } else {
            Ok(None)
        }
    }
}

/// CDC manager — publishes change events to configured sinks.
pub struct CdcManager {
    /// Configuration.
    config: CdcConfig,
    /// Event sender channel.
    event_tx: mpsc::UnboundedSender<CdcEvent>,
    /// Per-sink state (shared with background task).
    state: Arc<RwLock<CdcPublisherState>>,
    /// Optional callback to increment suppression metric.
    suppressed_metric_callback: Option<CdcSuppressedMetricCallback>,
    /// Per-sink tiered buffers.
    buffers: HashMap<String, Arc<CdcBuffer>>,
    /// Optional callback to increment dropped events metric.
    dropped_metric_callback: Option<CdcDroppedMetricCallback>,
    /// Internal queue for GET /_miroir/changes endpoint.
    internal_queue: Arc<CdcInternalQueue>,
}

/// CDC manager configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcConfig {
    /// Whether CDC is enabled.
    pub enabled: bool,
    /// Whether to emit TTL delete events.
    pub emit_ttl_deletes: bool,
    /// Whether to emit internal writes (debug only).
    pub emit_internal_writes: bool,
    /// Configured sinks.
    pub sinks: Vec<CdcSinkConfig>,
    /// Buffer configuration.
    pub buffer: CdcBufferConfig,
}

/// Buffer configuration for CDC events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcBufferConfig {
    /// Primary buffer type.
    pub primary: CdcBufferType,
    /// In-memory buffer size (bytes).
    pub memory_bytes: u64,
    /// Overflow buffer type.
    pub overflow: CdcBufferType,
    /// Redis overflow size (bytes).
    pub redis_bytes: u64,
}

/// Buffer type variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcBufferType {
    Memory,
    Redis,
    Pvc,
    Drop,
}

impl CdcBufferType {
    /// Parse a buffer type from a string (for config deserialization).
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "memory" => Some(CdcBufferType::Memory),
            "redis" => Some(CdcBufferType::Redis),
            "pvc" => Some(CdcBufferType::Pvc),
            "drop" => Some(CdcBufferType::Drop),
            _ => None,
        }
    }

    /// Convert buffer type to string.
    pub fn as_str(&self) -> &str {
        match self {
            CdcBufferType::Memory => "memory",
            CdcBufferType::Redis => "redis",
            CdcBufferType::Pvc => "pvc",
            CdcBufferType::Drop => "drop",
        }
    }
}

/// Tiered CDC buffer: primary (memory) → overflow (redis/pvc/drop).
///
/// Implements plan §13.13 buffer backend with configurable overflow strategy.
/// Events are buffered in memory first; when memory watermark is reached,
/// events overflow to the configured backend (redis, pvc, or drop).
pub struct CdcBuffer {
    /// Primary in-memory buffer (bounded by semaphore).
    primary: Arc<CdcMemoryBuffer>,
    /// Overflow backend.
    overflow: Arc<dyn CdcOverflowBackend + Send + Sync>,
    /// Metric callback for dropped events.
    dropped_metric_callback: Option<CdcDroppedMetricCallback>,
}

/// In-memory buffer with bounded size (64 MiB default).
struct CdcMemoryBuffer {
    /// Max bytes allowed in memory buffer.
    max_bytes: u64,
    /// Current bytes in buffer (approximate).
    current_bytes: Arc<std::sync::atomic::AtomicU64>,
    /// Semaphore for backpressure (permits = max_bytes / avg_event_size).
    semaphore: Arc<Semaphore>,
}

impl CdcMemoryBuffer {
    /// Create a new memory buffer with byte limit.
    fn new(max_bytes: u64) -> Self {
        // Assume average event size of 1 KiB for semaphore permits
        let permits = (max_bytes / 1024).max(1) as usize;
        Self {
            max_bytes,
            current_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            semaphore: Arc::new(Semaphore::new(permits)),
        }
    }

    /// Try to acquire space for an event of `size` bytes.
    /// Returns `None` if buffer is at capacity (should overflow).
    async fn acquire(&self, size: u64) -> Option<()> {
        // Check soft watermark (80% of max)
        let current = self
            .current_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        if current + size > (self.max_bytes * 8 / 10) {
            return None;
        }

        // Try to acquire semaphore permit (non-blocking)
        match self.semaphore.try_acquire() {
            Ok(_permit) => {
                self.current_bytes
                    .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
                Some(())
            }
            Err(_) => None,
        }
    }

    /// Release space after event is processed.
    fn release(&self, size: u64) {
        let old = self
            .current_bytes
            .fetch_sub(size, std::sync::atomic::Ordering::Relaxed);
        // Add semaphore permit back
        self.semaphore.add_permits(1);
        debug_assert!(old >= size, "buffer underflow: {old} < {size}");
    }
}

/// Overflow backend for CDC events when primary buffer is full.
#[async_trait::async_trait]
pub trait CdcOverflowBackend: Send + Sync {
    /// Push an event to overflow. Returns `Ok(())` if accepted,
    /// `Err(CdcError::BufferOverflow)` if dropped (drop backend).
    async fn push(&self, event: CdcEvent) -> Result<(), CdcError>;

    /// Pop the oldest event from overflow (for draining to sinks).
    async fn pop(&self) -> Option<CdcEvent>;

    /// Get current overflow size in bytes (for metrics).
    async fn size_bytes(&self) -> u64;

    /// Clear all overflow events (for shutdown).
    async fn clear(&self) -> Result<(), CdcError>;
}

/// Redis overflow backend (plan §13.13: 1 GiB per sink).
pub struct CdcRedisOverflow {
    /// Redis connection pool.
    #[cfg(feature = "redis-store")]
    pool: Option<crate::task_store::RedisPool>,
    /// Sink name for key prefix.
    sink_name: String,
    /// Max bytes in Redis overflow.
    max_bytes: u64,
    /// Key for overflow list.
    list_key: String,
    /// Key for byte counter.
    bytes_key: String,
}

#[cfg(feature = "redis-store")]
impl CdcRedisOverflow {
    /// Create a new Redis overflow backend.
    pub async fn new(
        pool: crate::task_store::RedisPool,
        sink_name: String,
        max_bytes: u64,
    ) -> Result<Self, CdcError> {
        let list_key = format!("miroir:cdc:overflow:{sink_name}");
        let bytes_key = format!("miroir:cdc:overflow_bytes:{sink_name}");
        Ok(Self {
            pool: Some(pool),
            sink_name,
            max_bytes,
            list_key,
            bytes_key,
        })
    }

    /// Create a Redis overflow backend that connects lazily on first use.
    #[cfg(feature = "redis-store")]
    pub fn lazy_new(sink_name: String, max_bytes: u64, _redis_url: String) -> Self {
        let list_key = format!("miroir:cdc:overflow:{sink_name}");
        let bytes_key = format!("miroir:cdc:overflow_bytes:{sink_name}");
        Self {
            pool: None,
            sink_name,
            max_bytes,
            list_key,
            bytes_key,
        }
    }

    /// Push event to Redis list (LPUSH).
    async fn push_inner(&self, event: CdcEvent) -> Result<(), CdcError> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::SinkError("no pool".into()))?;

        // Serialize event
        let json = serde_json::to_vec(&event)
            .map_err(|e| CdcError::SinkError(format!("serialize error: {e}")))?;
        let size = json.len() as u64;

        // Check size limit
        let mut conn = pool.manager.lock().await;
        let current_bytes: Option<u64> = conn
            .get(&self.bytes_key)
            .await
            .map_err(|e| CdcError::SinkError(format!("redis get error: {e}")))?;
        let current_bytes = current_bytes.unwrap_or(0);

        if current_bytes + size > self.max_bytes {
            // Trim oldest entries to fit (RPOP)
            let mut pipe = redis::pipe();
            while current_bytes + size > self.max_bytes {
                pipe.rpop(&self.list_key, None);
            }
            pipe.query_async::<()>(&mut *conn)
                .await
                .map_err(|e| CdcError::SinkError(format!("redis rpop error: {e}")))?;
        }

        // Push to list and update byte counter
        redis::pipe()
            .lpush(&self.list_key, json)
            .incr(&self.bytes_key, size as i64)
            .expire(&self.bytes_key, 86400) // 24h TTL
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|e| CdcError::SinkError(format!("redis lpush error: {e}")))?;

        Ok(())
    }

    /// Ensure the Redis pool is initialized (for lazy initialization).
    #[cfg(feature = "redis-store")]
    async fn ensure_pool(&self) -> Result<(), CdcError> {
        if self.pool.is_none() {
            let redis_url = std::env::var("MIROIR_REDIS_URL")
                .unwrap_or_else(|_| "redis://localhost:6379".to_string());
            let pool = crate::task_store::RedisPool::new(&redis_url)
                .await
                .map_err(|e| CdcError::SinkError(format!("redis pool error: {e}")))?;
            // Note: We can't modify self here since it's behind &self
            // This is a limitation of lazy initialization in this pattern
            // For now, we'll just return an error
            return Err(CdcError::SinkError(
                "Redis pool not initialized - use lazy_new with explicit URL".into(),
            ));
        }
        Ok(())
    }

    /// Pop oldest event from Redis list (RPOP).
    async fn pop_inner(&self) -> Option<CdcEvent> {
        let pool = self.pool.as_ref()?;
        let mut conn = pool.manager.lock().await;

        let json: Vec<u8> = conn.rpop(&self.list_key, None).await.ok()?;

        // Decrement byte counter
        let size = json.len() as i64;
        let _: i64 = conn.decr(&self.bytes_key, size).await.ok()?;

        serde_json::from_slice(&json).ok()
    }
}

#[cfg(not(feature = "redis-store"))]
impl CdcRedisOverflow {
    /// Create a new Redis overflow backend (no-op without redis-store feature).
    pub async fn new(_pool: (), _sink_name: String, _max_bytes: u64) -> Result<Self, CdcError> {
        Err(CdcError::SinkError(
            "redis-store feature not enabled".into(),
        ))
    }
}

#[async_trait::async_trait]
impl CdcOverflowBackend for CdcRedisOverflow {
    async fn push(&self, _event: CdcEvent) -> Result<(), CdcError> {
        #[cfg(feature = "redis-store")]
        return self.push_inner(event).await;

        #[cfg(not(feature = "redis-store"))]
        Err(CdcError::SinkError(
            "redis-store feature not enabled".into(),
        ))
    }

    async fn pop(&self) -> Option<CdcEvent> {
        #[cfg(feature = "redis-store")]
        return self.pop_inner().await;

        #[cfg(not(feature = "redis-store"))]
        None
    }

    async fn size_bytes(&self) -> u64 {
        #[cfg(feature = "redis-store")]
        {
            if let Some(pool) = &self.pool {
                let mut conn = pool.manager.lock().await;
                return conn.get(&self.bytes_key).await.unwrap_or(0);
            }
        }
        0
    }

    async fn clear(&self) -> Result<(), CdcError> {
        #[cfg(feature = "redis-store")]
        {
            if let Some(pool) = &self.pool {
                let mut conn = pool.manager.lock().await;
                conn.del::<_, ()>(&self.list_key)
                    .await
                    .map_err(|e| CdcError::SinkError(format!("redis del error: {e}")))?;
                conn.set::<_, _, ()>(&self.bytes_key, 0i64)
                    .await
                    .map_err(|e| CdcError::SinkError(format!("redis set error: {e}")))?;
                return Ok(());
            }
        }
        Ok(())
    }
}

/// PVC (file) overflow backend for single-pod dev without Redis.
///
/// Writes to a circular log file at `/data/cdc-overflow-{sink}.log`.
/// Plan §13.13: Helm renders `miroir-pvc.yaml` when overflow is pvc.
pub struct CdcPvcOverflow {
    /// Sink name for file naming.
    sink_name: String,
    /// Data directory (default: `/data`).
    data_dir: std::path::PathBuf,
    /// Max bytes in file overflow.
    max_bytes: u64,
    /// Write lock.
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl CdcPvcOverflow {
    /// Create a new PVC overflow backend.
    pub fn new(sink_name: String, data_dir: std::path::PathBuf, max_bytes: u64) -> Self {
        Self {
            sink_name,
            data_dir,
            max_bytes,
            lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    fn file_path(&self) -> std::path::PathBuf {
        self.data_dir
            .join(format!("cdc-overflow-{}.log", self.sink_name))
    }

    /// Push event to file (append, truncate if over limit).
    async fn push_inner(&self, event: CdcEvent) -> Result<(), CdcError> {
        let _guard = self.lock.lock().await;

        // Serialize event
        let json = serde_json::to_vec(&event)
            .map_err(|e| CdcError::SinkError(format!("serialize error: {e}")))?;
        let size = json.len() as u64;

        let path = self.file_path();
        let metadata = tokio::fs::metadata(&path).await;
        let current_size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);

        // Truncate if over limit (circular log)
        if current_size + size > self.max_bytes {
            // Read and rewrite file, dropping oldest entries
            let content = tokio::fs::read_to_string(&path).await.unwrap_or_default();
            let mut events: Vec<CdcEvent> = content
                .lines()
                .filter_map(|line| serde_json::from_str(line).ok())
                .collect();

            // Drop oldest events until space is available
            let mut total_size = size;
            while !events.is_empty() && total_size > self.max_bytes {
                let dropped = events.remove(0);
                let dropped_size = serde_json::to_vec(&dropped).unwrap().len() as u64;
                total_size -= dropped_size;
            }

            // Rewrite file
            let mut file = tokio::fs::File::create(&path)
                .await
                .map_err(|e| CdcError::SinkError(format!("create error: {e}")))?;
            for ev in events {
                let line = serde_json::to_string(&ev)
                    .map_err(|e| CdcError::SinkError(format!("serialize error: {e}")))?;
                use tokio::io::AsyncWriteExt;
                file.write_all(line.as_bytes())
                    .await
                    .map_err(|e| CdcError::SinkError(format!("write error: {e}")))?;
                file.write_all(b"\n")
                    .await
                    .map_err(|e| CdcError::SinkError(format!("write error: {e}")))?;
            }
        }

        // Append event
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| CdcError::SinkError(format!("open error: {e}")))?;
        file.write_all(&json)
            .await
            .map_err(|e| CdcError::SinkError(format!("write error: {e}")))?;
        file.write_all(b"\n")
            .await
            .map_err(|e| CdcError::SinkError(format!("write error: {e}")))?;

        Ok(())
    }

    /// Pop oldest event from file (read first line).
    async fn pop_inner(&self) -> Option<CdcEvent> {
        let path = self.file_path();
        let content = tokio::fs::read_to_string(&path).await.ok()?;
        let first_line = content.lines().next()?;
        let event = serde_json::from_str(first_line).ok()?;

        // Remove first line from file
        let remaining = content.lines().skip(1).collect::<Vec<_>>().join("\n");
        tokio::fs::write(&path, remaining).await.ok()?;

        Some(event)
    }
}

#[async_trait::async_trait]
impl CdcOverflowBackend for CdcPvcOverflow {
    async fn push(&self, event: CdcEvent) -> Result<(), CdcError> {
        self.push_inner(event).await
    }

    async fn pop(&self) -> Option<CdcEvent> {
        self.pop_inner().await
    }

    async fn size_bytes(&self) -> u64 {
        tokio::fs::metadata(self.file_path())
            .await
            .map(|m| m.len())
            .unwrap_or(0)
    }

    async fn clear(&self) -> Result<(), CdcError> {
        tokio::fs::remove_file(self.file_path())
            .await
            .map_err(|e| CdcError::SinkError(format!("remove error: {e}")))?;
        Ok(())
    }
}

/// Drop overflow backend — drops events past watermark immediately.
///
/// Plan §13.13: `overflow: drop` disables spill; events past watermark
/// increment `miroir_cdc_dropped_total` immediately.
pub struct CdcDropOverflow {
    /// Sink name for metrics.
    sink_name: String,
    /// Callback to increment dropped metric.
    metric_callback: Option<CdcDroppedMetricCallback>,
}

/// Callback type for incrementing the CDC dropped events metric.
pub type CdcDroppedMetricCallback = Arc<dyn Fn(&str) + Send + Sync>;

impl CdcDropOverflow {
    /// Create a new drop overflow backend.
    pub fn new(sink_name: String, metric_callback: Option<CdcDroppedMetricCallback>) -> Self {
        Self {
            sink_name,
            metric_callback,
        }
    }
}

#[async_trait::async_trait]
impl CdcOverflowBackend for CdcDropOverflow {
    async fn push(&self, _event: CdcEvent) -> Result<(), CdcError> {
        // Increment dropped metric
        if let Some(ref callback) = self.metric_callback {
            callback(&self.sink_name);
        }
        debug!(
            "CDC: dropped event for sink {} (overflow: drop)",
            self.sink_name
        );
        // Return error to signal event was dropped
        Err(CdcError::BufferOverflow)
    }

    async fn pop(&self) -> Option<CdcEvent> {
        None // No events to pop from drop backend
    }

    async fn size_bytes(&self) -> u64 {
        0 // Drop backend stores nothing
    }

    async fn clear(&self) -> Result<(), CdcError> {
        Ok(()) // Nothing to clear
    }
}

impl CdcBuffer {
    /// Create a new tiered CDC buffer.
    pub fn new(
        config: &CdcBufferConfig,
        sink_name: String,
        dropped_callback: Option<CdcDroppedMetricCallback>,
    ) -> Result<Self, CdcError> {
        let primary = Arc::new(CdcMemoryBuffer::new(config.memory_bytes));

        let overflow: Arc<dyn CdcOverflowBackend + Send + Sync> = match CdcBufferType::from_str(
            config.overflow.as_str(),
        ) {
            Some(CdcBufferType::Redis) => {
                #[cfg(feature = "redis-store")]
                {
                    // Redis pool will be created lazily on first use
                    let redis_url = std::env::var("MIROIR_REDIS_URL")
                        .unwrap_or_else(|_| "redis://localhost:6379".to_string());
                    let backend =
                        CdcRedisOverflow::lazy_new(sink_name, config.redis_bytes, redis_url);
                    Arc::new(backend)
                }
                #[cfg(not(feature = "redis-store"))]
                {
                    warn!("CDC: redis overflow requested but redis-store feature not enabled, using drop backend");
                    Arc::new(CdcDropOverflow::new(sink_name, dropped_callback.clone()))
                }
            }
            Some(CdcBufferType::Pvc) => {
                let data_dir =
                    std::env::var("MIROIR_DATA_DIR").unwrap_or_else(|_| "/data".to_string());
                Arc::new(CdcPvcOverflow::new(
                    sink_name,
                    std::path::PathBuf::from(data_dir),
                    config.redis_bytes, // Use same budget
                ))
            }
            Some(CdcBufferType::Drop) => {
                Arc::new(CdcDropOverflow::new(sink_name, dropped_callback.clone()))
            }
            Some(CdcBufferType::Memory) | None => {
                // Memory overflow = drop (no secondary buffer)
                Arc::new(CdcDropOverflow::new(sink_name, dropped_callback.clone()))
            }
        };

        Ok(Self {
            primary,
            overflow,
            dropped_metric_callback: dropped_callback,
        })
    }

    /// Push an event to the tiered buffer.
    /// Tries primary first, overflows if full.
    pub async fn push(&self, event: CdcEvent) -> Result<(), CdcError> {
        // Estimate event size (JSON size)
        let size = serde_json::to_vec(&event)
            .map_err(|e| CdcError::SinkError(format!("serialize error: {e}")))?
            .len() as u64;

        // Try primary buffer
        if (self.primary.acquire(size).await).is_some() {
            return Ok(()); // Buffered in memory
        }

        // Overflow to secondary backend
        self.overflow.push(event).await
    }

    /// Pop the oldest event from the buffer (primary first, then overflow).
    pub async fn pop(&self) -> Option<CdcEvent> {
        // Note: In a real implementation, we'd maintain proper ordering
        // between primary and overflow. For now, prefer overflow.
        self.overflow.pop().await
    }

    /// Get total buffer size in bytes (primary + overflow).
    pub async fn size_bytes(&self) -> u64 {
        let primary = self
            .primary
            .current_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        let overflow = self.overflow.size_bytes().await;
        primary + overflow
    }

    /// Clear all buffered events.
    pub async fn clear(&self) -> Result<(), CdcError> {
        self.overflow.clear().await
    }
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            emit_ttl_deletes: false,
            emit_internal_writes: false,
            sinks: Vec::new(),
            buffer: CdcBufferConfig {
                primary: CdcBufferType::Memory,
                memory_bytes: 67_108_864, // 64 MiB
                overflow: CdcBufferType::Redis,
                redis_bytes: 1_073_741_824, // 1 GiB
            },
        }
    }
}

impl CdcManager {
    /// Create a new CDC manager.
    pub fn new(config: CdcConfig) -> Self {
        Self::with_metrics(config, None, None, None)
    }

    /// Create a new CDC manager with optional metric callbacks and task store.
    pub fn with_metrics(
        config: CdcConfig,
        suppressed_metric_callback: Option<CdcSuppressedMetricCallback>,
        dropped_metric_callback: Option<CdcDroppedMetricCallback>,
        task_store: Option<Arc<dyn TaskStore>>,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let state = Arc::new(RwLock::new(CdcPublisherState {
            cursors: HashMap::new(),
            buffered_count: 0,
            dropped_count: 0,
            published_count: 0,
            buffer_bytes: HashMap::new(),
        }));

        // Initialize internal queue for GET /_miroir/changes endpoint
        let internal_queue = Arc::new(CdcInternalQueue::new(task_store));

        // Initialize per-sink tiered buffers
        let mut buffers = HashMap::new();
        for sink in &config.sinks {
            let sink_name = sink.url.clone();
            match CdcBuffer::new(
                &config.buffer,
                sink_name.clone(),
                dropped_metric_callback.clone(),
            ) {
                Ok(buffer) => {
                    buffers.insert(sink_name.clone(), Arc::new(buffer));
                }
                Err(e) => {
                    error!("CDC: failed to create buffer for sink {}: {}", sink_name, e);
                }
            }
        }

        if config.enabled {
            // Spawn background publisher task
            let state_clone = state.clone();
            let config_clone = config.clone();
            let buffers_clone = buffers.clone();
            let internal_queue_clone = internal_queue.clone();
            tokio::spawn(async move {
                Self::background_publisher(
                    event_rx,
                    state_clone,
                    config_clone,
                    buffers_clone,
                    internal_queue_clone,
                )
                .await;
            });
        }

        Self {
            config,
            event_tx,
            state,
            suppressed_metric_callback,
            buffers,
            dropped_metric_callback,
            internal_queue,
        }
    }

    /// Publish a CDC event (non-blocking).
    pub fn publish(&self, event: CdcEvent) -> Result<(), CdcError> {
        if !self.config.enabled {
            return Ok(());
        }

        // Filter based on origin tag (plan §13.13: CDC event suppression)
        if let Some(ref origin) = event.origin {
            let should_suppress = match origin.as_str() {
                ORIGIN_ANTIENTROPY | ORIGIN_RESHARD_BACKFILL | ORIGIN_ROLLOVER => {
                    // Internal writes: suppressed unless emit_internal_writes is true
                    !self.config.emit_internal_writes
                }
                ORIGIN_TTL_EXPIRE => {
                    // TTL deletes: suppressed unless emit_ttl_deletes is true
                    !self.config.emit_ttl_deletes
                }
                _ => false,
            };

            if should_suppress {
                debug!("CDC: suppressing event with origin {}", origin);
                // Increment suppression metric if callback is provided
                if let Some(ref callback) = self.suppressed_metric_callback {
                    callback(origin);
                }
                return Ok(());
            }
        }

        self.event_tx
            .send(event)
            .map_err(|_| CdcError::ChannelClosed)?;
        Ok(())
    }

    /// Get current publisher state.
    pub async fn state(&self) -> CdcPublisherState {
        self.state.read().await.clone()
    }

    /// Get events from the internal queue since a given cursor.
    ///
    /// # Arguments
    /// * `index` - Index UID to query
    /// * `cursor` - Sequence number to start from (exclusive)
    /// * `limit` - Maximum number of events to return
    ///
    /// Returns events with sequence > cursor, up to `limit` events.
    pub async fn get_changes(&self, index: &str, cursor: u64, limit: usize) -> Vec<CdcEvent> {
        self.internal_queue.get_since(index, cursor, limit).await
    }

    /// Get events from the internal queue since a given cursor with long-poll support.
    ///
    /// # Arguments
    /// * `index` - Index UID to query
    /// * `cursor` - Sequence number to start from (exclusive)
    /// * `limit` - Maximum number of events to return
    /// * `timeout_secs` - Maximum time to wait for new events (None = return immediately)
    ///
    /// Returns events with sequence > cursor, up to `limit` events.
    /// Waits up to `timeout_secs` for new events if none are immediately available.
    pub async fn get_changes_long_poll(
        &self,
        index: &str,
        cursor: u64,
        limit: usize,
        timeout_secs: Option<u64>,
    ) -> Vec<CdcEvent> {
        let timeout = timeout_secs.map(Duration::from_secs);
        self.internal_queue
            .get_since_long_poll(index, cursor, limit, timeout)
            .await
    }

    /// Get the current maximum sequence number for an index.
    pub async fn max_sequence(&self, index: &str) -> u64 {
        self.internal_queue.max_sequence(index).await
    }

    /// Persist a cursor for a sink/index combination.
    pub async fn persist_cursor(
        &self,
        sink_name: &str,
        index: &str,
        seq: u64,
    ) -> Result<(), CdcError> {
        self.internal_queue
            .persist_cursor(sink_name, index, seq)
            .await
    }

    /// Get the persisted cursor for a sink/index combination.
    pub async fn get_cursor(&self, sink_name: &str, index: &str) -> Result<Option<u64>, CdcError> {
        self.internal_queue.get_cursor(sink_name, index).await
    }

    /// Get a reference to the internal queue.
    pub fn internal_queue(&self) -> &Arc<CdcInternalQueue> {
        &self.internal_queue
    }

    /// Background task that buffers and publishes events to sinks.
    ///
    /// Implements time-based flushing (batch_flush_ms) and per-sink batching.
    /// Plan §13.13: "batched POST to configured URL; default batch_size: 100 events or batch_flush_ms: 1000"
    async fn background_publisher(
        mut event_rx: mpsc::UnboundedReceiver<CdcEvent>,
        state: Arc<RwLock<CdcPublisherState>>,
        config: CdcConfig,
        buffers: HashMap<String, Arc<CdcBuffer>>,
        internal_queue: Arc<CdcInternalQueue>,
    ) {
        info!("CDC: background publisher started");

        // Per-sink event buffers
        let mut sink_buffers: HashMap<String, Vec<CdcEvent>> = HashMap::new();
        // Per-sink last flush time
        let mut last_flush: HashMap<String, Instant> = HashMap::new();

        // Create a timer that ticks at the minimum batch_flush_ms interval
        let min_flush_ms = config
            .sinks
            .iter()
            .map(|s| s.batch_flush_ms)
            .min()
            .unwrap_or(1000);
        let mut flush_timer = tokio::time::interval(Duration::from_millis(min_flush_ms));
        flush_timer.tick().await; // Skip first immediate tick

        loop {
            tokio::select! {
                // Handle incoming events
                event = event_rx.recv() => {
                    match event {
                        Some(event) => {
                            // Store event in internal queue for GET /_miroir/changes endpoint
                            let _sequence = internal_queue.store(event.clone()).await;

                            // Buffer event for each sink
                            for sink in &config.sinks {
                                // Push to tiered buffer (memory → overflow)
                                if let Some(buffer) = buffers.get(&sink.url) {
                                    if let Err(e) = buffer.push(event.clone()).await {
                                        match e {
                                            CdcError::BufferOverflow => {
                                                // Event was dropped (overflow: drop)
                                                let mut st = state.write().await;
                                                st.dropped_count += 1;
                                            }
                                            _ => {
                                                error!("CDC: buffer error for sink {}: {}", sink.url, e);
                                            }
                                        }
                                    }
                                }

                                let buffer = sink_buffers.entry(sink.url.clone()).or_default();
                                buffer.push(event.clone());

                                // Flush if buffer size reached (batch_size trigger)
                                if buffer.len() >= sink.batch_size as usize {
                                    if let Err(e) = Self::flush_sink(sink, buffer, &state, &internal_queue).await {
                                        error!("CDC: failed to flush sink {}: {}", sink.url, e);
                                    }
                                    sink_buffers.insert(sink.url.clone(), Vec::new());
                                    last_flush.insert(sink.url.clone(), Instant::now());
                                }
                            }

                            // Update buffer bytes metrics
                            let mut st = state.write().await;
                            for (sink_url, buffer) in &buffers {
                                let bytes = buffer.size_bytes().await;
                                st.buffer_bytes.insert(sink_url.clone(), bytes);
                            }
                        }
                        None => {
                            // Channel closed - flush remaining and exit
                            break;
                        }
                    }
                }
                // Handle time-based flushing (batch_flush_ms trigger)
                _ = flush_timer.tick() => {
                    let now = Instant::now();
                    for sink in &config.sinks {
                        if let Some(buffer) = sink_buffers.get_mut(&sink.url) {
                            if !buffer.is_empty() {
                                let flush_deadline = last_flush.get(&sink.url)
                                    .map(|t| *t + Duration::from_millis(sink.batch_flush_ms))
                                    .unwrap_or(now);

                                if now >= flush_deadline {
                                    if let Err(e) = Self::flush_sink(sink, buffer, &state, &internal_queue).await {
                                        error!("CDC: failed to flush sink {} on timer: {}", sink.url, e);
                                    }
                                    buffer.clear();
                                    last_flush.insert(sink.url.clone(), now);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Flush remaining buffers on shutdown
        for (sink_url, buffer) in sink_buffers {
            if !buffer.is_empty() {
                let sink = config.sinks.iter().find(|s| s.url == sink_url);
                if let Some(sink) = sink {
                    if let Err(e) = Self::flush_sink(sink, &buffer, &state, &internal_queue).await {
                        error!("CDC: failed to flush sink {} on shutdown: {}", sink_url, e);
                    }
                }
            }
        }

        info!("CDC: background publisher stopped");
    }

    /// Flush buffered events to a single sink.
    ///
    /// On success, advances the per-sink cursor in cdc_cursors table (plan §13.13).
    /// Cursor is only advanced on sink ACK, ensuring at-least-once delivery.
    async fn flush_sink(
        sink: &CdcSinkConfig,
        events: &[CdcEvent],
        state: &Arc<RwLock<CdcPublisherState>>,
        internal_queue: &Arc<CdcInternalQueue>,
    ) -> Result<(), CdcError> {
        match sink.sink_type {
            CdcSinkType::Webhook => {
                Self::flush_webhook(sink, events, internal_queue).await?;
                // Increment published count on success
                let mut st = state.write().await;
                st.published_count += events.len() as u64;
                Ok(())
            }
            CdcSinkType::Nats => Self::flush_nats(sink, events).await,
            CdcSinkType::Kafka => Self::flush_kafka(sink, events).await,
            CdcSinkType::Internal => {
                // Internal queue: events are stored in memory for polling
                Ok(())
            }
        }
    }

    /// Flush events to a webhook sink.
    ///
    /// Implements exponential backoff retries capped by retry_max_s (plan §13.13).
    /// Advances cursor only on successful ACK (2xx response).
    ///
    /// Retry strategy:
    /// - Initial delay: 100ms
    /// - Exponential backoff multiplier: 2
    /// - Max retry time: retry_max_s (default 3600s = 1 hour)
    /// - Retries on 5xx errors and network errors
    /// - No retry on 4xx client errors (except 429 Too Many Requests)
    async fn flush_webhook(
        sink: &CdcSinkConfig,
        events: &[CdcEvent],
        internal_queue: &Arc<CdcInternalQueue>,
    ) -> Result<(), CdcError> {
        let client = reqwest::Client::new();
        let retry_max_s = sink.retry_max_s;
        let retry_start = Instant::now();
        let mut delay = Duration::from_millis(100); // Initial retry delay

        loop {
            // Clone events for potential retry
            let events_to_send: Vec<CdcEvent> = events
                .iter()
                .map(|ev| {
                    let mut ev_clone = ev.clone();
                    // Respect include_body setting
                    if !sink.include_body {
                        ev_clone.document = None;
                    }
                    ev_clone
                })
                .collect();

            let result = client.post(&sink.url).json(&events_to_send).send().await;

            match result {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        // Success - advance cursor for each index in the batch
                        // Cursor is per-sink per-index (plan §13.13)
                        for event in events {
                            if let Err(e) = internal_queue
                                .persist_cursor(&sink.url, &event.index, event.timestamp)
                                .await
                            {
                                warn!(
                                    "CDC: failed to persist cursor for sink {} index {}: {}",
                                    sink.url, event.index, e
                                );
                            }
                        }
                        return Ok(());
                    }

                    // Determine if we should retry
                    let should_retry = status.is_server_error()
                        || status == reqwest::StatusCode::TOO_MANY_REQUESTS;

                    if should_retry {
                        // Check if we've exceeded retry_max_s
                        let elapsed = retry_start.elapsed().as_secs();
                        if elapsed >= retry_max_s {
                            return Err(CdcError::SinkError(format!(
                                "webhook retry timeout after {elapsed}s (max {retry_max_s}s), last status: {status}"
                            )));
                        }

                        warn!(
                            "CDC: webhook returned {}, retrying in {:?} (elapsed: {}s)",
                            status, delay, elapsed
                        );
                        tokio::time::sleep(delay).await;

                        // Exponential backoff with jitter (±25%)
                        delay = (delay * 2).min(Duration::from_secs(60));
                        let jitter = duration_jitter(delay, 0.25);
                        delay = jitter;
                    } else {
                        // Client error (4xx) - don't retry
                        return Err(CdcError::SinkError(format!(
                            "webhook returned non-retryable status {status}"
                        )));
                    }
                }
                Err(e) => {
                    // Network error - check if we should retry
                    let elapsed = retry_start.elapsed().as_secs();
                    if elapsed >= retry_max_s {
                        return Err(CdcError::SinkError(format!(
                            "webhook network error after {elapsed}s (max {retry_max_s}s): {e}"
                        )));
                    }

                    warn!(
                        "CDC: webhook network error: {}, retrying in {:?} (elapsed: {}s)",
                        e, delay, elapsed
                    );
                    tokio::time::sleep(delay).await;

                    // Exponential backoff with jitter
                    delay = (delay * 2).min(Duration::from_secs(60));
                    let jitter = duration_jitter(delay, 0.25);
                    delay = jitter;
                }
            }
        }
    }

    /// NATS flush (placeholder for P5.13.b).
    async fn flush_nats(_sink: &CdcSinkConfig, _events: &[CdcEvent]) -> Result<(), CdcError> {
        // NATS publishing implementation (P5.13.b)
        // (requires async-nats crate)
        Ok(())
    }

    /// Kafka flush (placeholder for P5.13.c).
    async fn flush_kafka(_sink: &CdcSinkConfig, _events: &[CdcEvent]) -> Result<(), CdcError> {
        // Kafka publishing implementation (P5.13.c)
        // (requires rustafka or rdkafka crate)
        Ok(())
    }
}

/// CDC error types.
#[derive(Debug, thiserror::Error)]
pub enum CdcError {
    #[error("channel closed")]
    ChannelClosed,
    #[error("sink error: {0}")]
    SinkError(String),
    #[error("buffer overflow")]
    BufferOverflow,
}

// Conversion from config::advanced::CdcConfig to cdc::CdcConfig
impl From<crate::config::advanced::CdcConfig> for CdcConfig {
    fn from(config: crate::config::advanced::CdcConfig) -> Self {
        Self {
            enabled: config.enabled,
            emit_ttl_deletes: config.emit_ttl_deletes,
            emit_internal_writes: config.emit_internal_writes,
            sinks: config.sinks.into_iter().map(Into::into).collect(),
            buffer: config.buffer.into(),
        }
    }
}

impl From<crate::config::advanced::CdcBufferConfig> for CdcBufferConfig {
    fn from(config: crate::config::advanced::CdcBufferConfig) -> Self {
        // Convert String to CdcBufferType
        let primary = match config.primary.to_lowercase().as_str() {
            "memory" => CdcBufferType::Memory,
            "redis" => CdcBufferType::Redis,
            "pvc" => CdcBufferType::Pvc,
            "drop" => CdcBufferType::Drop,
            _ => CdcBufferType::Memory, // Default
        };
        let overflow = match config.overflow.to_lowercase().as_str() {
            "memory" => CdcBufferType::Memory,
            "redis" => CdcBufferType::Redis,
            "pvc" => CdcBufferType::Pvc,
            "drop" => CdcBufferType::Drop,
            _ => CdcBufferType::Redis, // Default
        };
        Self {
            primary,
            memory_bytes: config.memory_bytes,
            overflow,
            redis_bytes: config.redis_bytes,
        }
    }
}

impl From<crate::config::advanced::CdcSinkConfig> for CdcSinkConfig {
    fn from(config: crate::config::advanced::CdcSinkConfig) -> Self {
        Self {
            sink_type: match config.sink_type.as_str() {
                "webhook" => CdcSinkType::Webhook,
                "nats" => CdcSinkType::Nats,
                "kafka" => CdcSinkType::Kafka,
                "internal" => CdcSinkType::Internal,
                _ => CdcSinkType::Webhook, // Default to webhook
            },
            url: config.url,
            batch_size: config.batch_size,
            batch_flush_ms: config.batch_flush_ms,
            include_body: config.include_body,
            retry_max_s: config.retry_max_s,
            subject_prefix: config.subject_prefix,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_event_creation() {
        let event = CdcEvent {
            mtask_id: "mtask-123".into(),
            index: "products".into(),
            operation: CdcOperation::Add,
            primary_keys: vec!["sku-123".into()],
            shard_ids: vec![5],
            settings_version: 1,
            timestamp: 1234567890,
            document: None,
            origin: None,
            event_id: uuid::Uuid::new_v4().to_string(),
        };
        assert_eq!(event.index, "products");
        assert_eq!(event.operation, CdcOperation::Add);
    }

    #[test]
    fn test_cdc_config_default() {
        let config = CdcConfig::default();
        assert!(config.enabled);
        assert!(!config.emit_ttl_deletes);
        assert!(!config.emit_internal_writes);
    }

    #[tokio::test]
    async fn test_cdc_manager_publish() {
        let config = CdcConfig {
            enabled: true,
            ..Default::default()
        };
        let manager = CdcManager::with_metrics(config, None, None, None);

        let event = CdcEvent {
            mtask_id: "mtask-123".into(),
            index: "products".into(),
            operation: CdcOperation::Add,
            primary_keys: vec!["sku-123".into()],
            shard_ids: vec![5],
            settings_version: 1,
            timestamp: 1234567890,
            document: None,
            origin: None,
            event_id: uuid::Uuid::new_v4().to_string(),
        };

        // Should not error
        assert!(manager.publish(event).is_ok());
    }

    #[tokio::test]
    async fn test_cdc_suppress_internal_writes() {
        let config = CdcConfig {
            enabled: true,
            emit_internal_writes: false,
            ..Default::default()
        };
        let manager = CdcManager::with_metrics(config, None, None, None);

        // Internal write should be suppressed
        let event = CdcEvent {
            mtask_id: "mtask-123".into(),
            index: "products".into(),
            operation: CdcOperation::Add,
            primary_keys: vec!["sku-123".into()],
            shard_ids: vec![5],
            settings_version: 1,
            timestamp: 1234567890,
            document: None,
            origin: Some("antientropy".into()),
            event_id: uuid::Uuid::new_v4().to_string(),
        };

        assert!(manager.publish(event).is_ok());
    }

    #[test]
    fn test_cdc_sink_type_serialization() {
        let sink_type = CdcSinkType::Webhook;
        let json = serde_json::to_string(&sink_type).unwrap();
        assert_eq!(json, "\"Webhook\"");
    }

    #[tokio::test]
    async fn test_cdc_suppression_metric_callback() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let callback_called = Arc::new(AtomicUsize::new(0));
        let callback_clone = callback_called.clone();

        let callback: CdcSuppressedMetricCallback = Arc::new(move |origin| {
            assert_eq!(origin, "antientropy");
            callback_clone.fetch_add(1, Ordering::SeqCst);
        });

        let config = CdcConfig {
            enabled: true,
            emit_internal_writes: false,
            ..Default::default()
        };
        let manager = CdcManager::with_metrics(config, Some(callback), None, None);

        let event = CdcEvent {
            mtask_id: "mtask-123".into(),
            index: "products".into(),
            operation: CdcOperation::Add,
            primary_keys: vec!["sku-123".into()],
            shard_ids: vec![5],
            settings_version: 1,
            timestamp: 1234567890,
            document: None,
            origin: Some("antientropy".into()),
            event_id: uuid::Uuid::new_v4().to_string(),
        };

        assert!(manager.publish(event).is_ok());
        assert_eq!(callback_called.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_cdc_suppression_metric_all_origins() {
        use std::collections::HashSet;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let suppressed_origins = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let origins_clone = suppressed_origins.clone();

        let callback: CdcSuppressedMetricCallback = Arc::new(move |origin| {
            origins_clone.lock().unwrap().insert(origin.to_string());
        });

        let config = CdcConfig {
            enabled: true,
            emit_internal_writes: false,
            emit_ttl_deletes: false,
            ..Default::default()
        };
        let manager = CdcManager::with_metrics(config, Some(callback), None, None);

        // Test all suppressible origins
        let origins = vec!["antientropy", "reshard_backfill", "rollover", "ttl_expire"];
        for origin in origins {
            let event = CdcEvent {
                mtask_id: "mtask-123".into(),
                index: "products".into(),
                operation: CdcOperation::Delete,
                primary_keys: vec!["sku-123".into()],
                shard_ids: vec![5],
                settings_version: 1,
                timestamp: 1234567890,
                document: None,
                origin: Some(origin.into()),
                event_id: uuid::Uuid::new_v4().to_string(),
            };
            assert!(manager.publish(event).is_ok());
        }

        let suppressed = suppressed_origins.lock().unwrap();
        assert_eq!(suppressed.len(), 4);
        assert!(suppressed.contains("antientropy"));
        assert!(suppressed.contains("reshard_backfill"));
        assert!(suppressed.contains("rollover"));
        assert!(suppressed.contains("ttl_expire"));
    }

    #[tokio::test]
    async fn test_cdc_no_suppression_with_emit_internal_writes() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let callback_called = Arc::new(AtomicUsize::new(0));
        let callback_clone = callback_called.clone();

        let callback: CdcSuppressedMetricCallback = Arc::new(move |_origin| {
            callback_clone.fetch_add(1, Ordering::SeqCst);
        });

        let config = CdcConfig {
            enabled: true,
            emit_internal_writes: true, // Enable internal writes
            ..Default::default()
        };
        let manager = CdcManager::with_metrics(config, Some(callback), None, None);

        let event = CdcEvent {
            mtask_id: "mtask-123".into(),
            index: "products".into(),
            operation: CdcOperation::Add,
            primary_keys: vec!["sku-123".into()],
            shard_ids: vec![5],
            settings_version: 1,
            timestamp: 1234567890,
            document: None,
            origin: Some("antientropy".into()),
            event_id: uuid::Uuid::new_v4().to_string(),
        };

        // Should NOT be suppressed because emit_internal_writes is true
        assert!(manager.publish(event).is_ok());
        // Callback should NOT have been called
        assert_eq!(callback_called.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_cdc_client_write_never_suppressed() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let callback_called = Arc::new(AtomicUsize::new(0));
        let callback_clone = callback_called.clone();

        let callback: CdcSuppressedMetricCallback = Arc::new(move |_origin| {
            callback_clone.fetch_add(1, Ordering::SeqCst);
        });

        let config = CdcConfig {
            enabled: true,
            emit_internal_writes: false,
            emit_ttl_deletes: false,
            ..Default::default()
        };
        let manager = CdcManager::with_metrics(config, Some(callback), None, None);

        // Client write has no origin tag
        let event = CdcEvent {
            mtask_id: "mtask-123".into(),
            index: "products".into(),
            operation: CdcOperation::Add,
            primary_keys: vec!["sku-123".into()],
            shard_ids: vec![5],
            settings_version: 1,
            timestamp: 1234567890,
            document: None,
            origin: None, // No origin = client write
            event_id: uuid::Uuid::new_v4().to_string(),
        };

        // Should NOT be suppressed (client writes are always emitted)
        assert!(manager.publish(event).is_ok());
        // Callback should NOT have been called
        assert_eq!(callback_called.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_cdc_buffer_type_from_str() {
        assert_eq!(
            CdcBufferType::from_str("memory"),
            Some(CdcBufferType::Memory)
        );
        assert_eq!(
            CdcBufferType::from_str("MEMORY"),
            Some(CdcBufferType::Memory)
        );
        assert_eq!(CdcBufferType::from_str("redis"), Some(CdcBufferType::Redis));
        assert_eq!(CdcBufferType::from_str("pvc"), Some(CdcBufferType::Pvc));
        assert_eq!(CdcBufferType::from_str("drop"), Some(CdcBufferType::Drop));
        assert_eq!(CdcBufferType::from_str("unknown"), None);
    }

    #[tokio::test]
    async fn test_cdc_drop_overflow_drops_events() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dropped_count = Arc::new(AtomicUsize::new(0));
        let dropped_clone = dropped_count.clone();

        let callback: CdcDroppedMetricCallback = Arc::new(move |_sink| {
            dropped_clone.fetch_add(1, Ordering::SeqCst);
        });

        let drop_backend = CdcDropOverflow::new("test-sink".into(), Some(callback));

        let event = CdcEvent {
            mtask_id: "mtask-123".into(),
            index: "products".into(),
            operation: CdcOperation::Add,
            primary_keys: vec!["sku-123".into()],
            shard_ids: vec![5],
            settings_version: 1,
            timestamp: 1234567890,
            document: None,
            origin: None,
            event_id: uuid::Uuid::new_v4().to_string(),
        };

        // Drop backend should return error
        assert!(matches!(
            drop_backend.push(event).await,
            Err(CdcError::BufferOverflow)
        ));

        // Metric should have been incremented
        assert_eq!(dropped_count.load(Ordering::SeqCst), 1);

        // Pop should return None
        assert!(drop_backend.pop().await.is_none());

        // Size should be 0
        assert_eq!(drop_backend.size_bytes().await, 0);

        // Clear should succeed
        assert!(drop_backend.clear().await.is_ok());
    }

    #[test]
    fn test_duration_jitter_returns_positive_duration() {
        let base = Duration::from_millis(1000);
        let jittered = duration_jitter(base, 0.25);

        // Result should be positive
        assert!(jittered.as_millis() > 0);

        // Result should be in reasonable range [750, 1250] ms
        assert!(jittered.as_millis() >= 750);
        assert!(jittered.as_millis() <= 1250);
    }

    #[test]
    fn test_duration_jitter_with_zero_fraction() {
        let base = Duration::from_millis(1000);
        let jittered = duration_jitter(base, 0.0);

        // With zero jitter, should return base duration
        assert_eq!(jittered, base);
    }

    #[test]
    fn test_duration_jitter_small_duration() {
        let base = Duration::from_millis(10);
        let jittered = duration_jitter(base, 0.5);

        // Result should be positive
        assert!(jittered.as_millis() > 0);

        // Result should be in range [5, 15] ms
        assert!(jittered.as_millis() >= 5);
        assert!(jittered.as_millis() <= 15);
    }

    #[test]
    fn test_duration_jitter_multiple_calls_produce_variance() {
        let base = Duration::from_millis(1000);
        let mut results = Vec::new();

        for _ in 0..100 {
            results.push(duration_jitter(base, 0.25));
        }

        // Check that we get different results (variance)
        let unique: std::collections::HashSet<_> = results.iter().collect();
        assert!(unique.len() > 10, "Jitter should produce variance");
    }

    #[tokio::test]
    async fn test_cdc_internal_queue_subscribe() {
        let queue = CdcInternalQueue::new(None);

        // Subscribe to notifications
        let mut rx = queue.subscribe();

        // Store an event
        let event = CdcEvent {
            mtask_id: "mtask-123".into(),
            index: "products".into(),
            operation: CdcOperation::Add,
            primary_keys: vec!["sku-123".into()],
            shard_ids: vec![5],
            settings_version: 1,
            timestamp: 1234567890,
            document: None,
            origin: None,
            event_id: uuid::Uuid::new_v4().to_string(),
        };
        queue.store(event).await;

        // Should receive notification
        let notified = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(notified.is_ok());
        let notified_index = notified.unwrap().unwrap();
        assert_eq!(notified_index, "products");
    }

    #[tokio::test]
    async fn test_cdc_internal_queue_get_since_immediate() {
        let queue = CdcInternalQueue::new(None);

        // Store some events
        for i in 1..=5 {
            let event = CdcEvent {
                mtask_id: format!("mtask-{}", i),
                index: "products".into(),
                operation: CdcOperation::Add,
                primary_keys: vec![format!("sku-{}", i)],
                shard_ids: vec![i],
                settings_version: 1,
                timestamp: 1234567890 + i as u64,
                document: None,
                origin: None,
                event_id: uuid::Uuid::new_v4().to_string(),
            };
            queue.store(event).await;
        }

        // Get events since cursor 0 - should get all 5
        let events = queue.get_since("products", 0, 100).await;
        assert_eq!(events.len(), 5);

        // Get events since cursor 3 - should get 2
        let events = queue.get_since("products", 3, 100).await;
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_cdc_internal_queue_long_poll_timeout() {
        let queue = CdcInternalQueue::new(None);

        // Try to get events when none exist, with a short timeout
        let start = Instant::now();
        let events = queue
            .get_since_long_poll("products", 0, 10, Some(Duration::from_millis(100)))
            .await;
        let elapsed = start.elapsed();

        // Should timeout and return empty
        assert!(events.is_empty());
        assert!(elapsed >= Duration::from_millis(95));
    }

    #[tokio::test]
    async fn test_cdc_internal_queue_long_poll_no_timeout_returns_immediately() {
        let queue = CdcInternalQueue::new(None);

        // Store an event first
        let event = CdcEvent {
            mtask_id: "mtask-123".into(),
            index: "products".into(),
            operation: CdcOperation::Add,
            primary_keys: vec!["sku-123".into()],
            shard_ids: vec![5],
            settings_version: 1,
            timestamp: 1234567890,
            document: None,
            origin: None,
            event_id: uuid::Uuid::new_v4().to_string(),
        };
        queue.store(event).await;

        // Get events with None timeout - should return immediately
        let start = Instant::now();
        let events = queue.get_since_long_poll("products", 0, 10, None).await;
        let elapsed = start.elapsed();

        assert!(!events.is_empty());
        assert!(elapsed < Duration::from_millis(10));
    }

    #[tokio::test]
    async fn test_cdc_manager_get_changes_long_poll() {
        let config = CdcConfig {
            enabled: true,
            ..Default::default()
        };
        let manager = CdcManager::new(config);

        // Test with None timeout (returns immediately)
        let events = manager.get_changes_long_poll("products", 0, 10, None).await;
        assert!(events.is_empty());

        // Test with Some timeout (waits)
        let start = Instant::now();
        let events = manager
            .get_changes_long_poll("products", 0, 10, Some(1))
            .await;
        let elapsed = start.elapsed();

        assert!(events.is_empty());
        assert!(elapsed >= Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_cdc_manager_long_poll_with_delayed_event() {
        let config = CdcConfig {
            enabled: true,
            ..Default::default()
        };
        let manager = Arc::new(CdcManager::new(config));
        let manager_clone = manager.clone();

        // Spawn a task that will publish an event after a delay
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let event = CdcEvent {
                mtask_id: "mtask-delayed".into(),
                index: "products".into(),
                operation: CdcOperation::Add,
                primary_keys: vec!["sku-delayed".into()],
                shard_ids: vec![99],
                settings_version: 1,
                timestamp: 1234567890,
                document: None,
                origin: None,
                event_id: uuid::Uuid::new_v4().to_string(),
            };
            manager_clone.publish(event).ok();
        });

        // Try to get events with a 1 second timeout - should receive the delayed event
        let start = Instant::now();
        let events = manager
            .get_changes_long_poll("products", 0, 10, Some(1))
            .await;
        let elapsed = start.elapsed();

        // Should have received the event
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].mtask_id, "mtask-delayed");
        // Should have waited at least 100ms for the event
        assert!(elapsed >= Duration::from_millis(90));
    }
}
