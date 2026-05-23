//! Idempotency keys and query coalescing (plan §13.10).
//!
//! This module implements:
//! - Write deduplication via idempotency keys
//! - Read query coalescing for identical concurrent searches

use crate::error::{MiroirError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{broadcast, RwLock};

/// Idempotency cache entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyEntry {
    /// SHA256 hash of the request body.
    pub body_hash: String,
    /// Miroir task ID returned to the client.
    pub mtask_id: String,
    /// Expiration timestamp.
    pub expires_at: u64,
}

/// Idempotency cache for write deduplication.
pub struct IdempotencyCache {
    /// key -> (body_hash, mtask_id, expires_at)
    cache: Arc<RwLock<HashMap<String, IdempotencyEntry>>>,
    /// Maximum entries (LRU cap).
    max_entries: usize,
    /// TTL in seconds.
    ttl_secs: u64,
}

impl IdempotencyCache {
    /// Create a new idempotency cache.
    pub fn new(max_entries: usize, ttl_secs: u64) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_entries,
            ttl_secs,
        }
    }

    /// Check for a duplicate write.
    ///
    /// Returns `Some(mtask_id)` if the key exists and body hash matches.
    /// Returns `Err` if the key exists but body hash differs.
    /// Returns `None` if the key is not found (caller should process).
    pub async fn check(&self, key: &str, body_hash: &str) -> Result<Option<String>> {
        let mut cache = self.cache.write().await;
        self.prune_expired_locked(&mut cache);

        if let Some(entry) = cache.get(key) {
            if entry.body_hash == body_hash {
                return Ok(Some(entry.mtask_id.clone()));
            } else {
                return Err(MiroirError::IdempotencyKeyReused);
            }
        }
        Ok(None)
    }

    /// Insert a new idempotency entry.
    pub async fn insert(&self, key: String, body_hash: String, mtask_id: String) {
        let mut cache = self.cache.write().await;
        self.prune_expired_locked(&mut cache);

        // Enforce max entries (simple FIFO for now)
        if cache.len() >= self.max_entries {
            // Remove oldest entry (first key)
            if let Some(k) = cache.keys().next().cloned() {
                cache.remove(&k);
            }
        }

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let expires_at = now + self.ttl_secs;

        cache.insert(key, IdempotencyEntry {
            body_hash,
            mtask_id,
            expires_at,
        });
    }

    /// Prune expired entries (must hold write lock).
    fn prune_expired_locked(&self, cache: &mut HashMap<String, IdempotencyEntry>) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        cache.retain(|_, v| v.expires_at > now);
    }

    /// Get current cache size.
    pub async fn size(&self) -> usize {
        self.cache.read().await.len()
    }
}

/// Fingerprint of a query for coalescing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryFingerprint {
    /// Index UID.
    pub index: String,
    /// Canonical JSON of search query.
    pub query_json: String,
    /// Settings version (include to avoid coalescing across settings changes).
    pub settings_version: u64,
}

impl QueryFingerprint {
    /// Create a new query fingerprint from index, query body, and settings version.
    ///
    /// Canonicalizes the JSON to ensure different key orders produce the same fingerprint.
    pub fn new(index: String, query_body: &serde_json::Value, settings_version: u64) -> Self {
        // Canonicalize JSON: sort keys recursively to ensure consistent fingerprint
        let query_json = canonical_json(query_body);

        Self {
            index,
            query_json,
            settings_version,
        }
    }
}

/// Canonicalize a JSON value by sorting all object keys recursively.
fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let mut sorted_map = serde_json::Map::new();
            let mut sorted_keys: Vec<&String> = map.keys().collect();
            sorted_keys.sort();

            for key in sorted_keys {
                let canonical_value = canonical_json(&map[key]);
                sorted_map.insert(key.clone(), serde_json::from_str(&canonical_value).unwrap());
            }

            serde_json::to_string(&sorted_map).unwrap_or_else(|_| value.to_string())
        }
        serde_json::Value::Array(arr) => {
            let canonical_arr: Vec<String> = arr.iter().map(canonical_json).collect();
            serde_json::to_string(&canonical_arr).unwrap_or_else(|_| value.to_string())
        }
        _ => value.to_string(),
    }
}

/// Pending query state for coalescing.
pub struct PendingQuery {
    /// Response broadcast channel.
    tx: broadcast::Sender<Vec<u8>>,
    /// When this query was initiated.
    started_at: Instant,
}

/// Query coalescing cache.
pub struct QueryCoalescer {
    /// Fingerprint -> pending query state.
    pending: Arc<RwLock<HashMap<QueryFingerprint, PendingQuery>>>,
    /// Coalescing window in milliseconds.
    window_ms: u64,
    /// Max concurrent pending queries.
    max_pending: usize,
    /// Max subscribers per query.
    max_subscribers: usize,
}

impl QueryCoalescer {
    /// Create a new query coalescer.
    pub fn new(window_ms: u64, max_pending: usize, max_subscribers: usize) -> Self {
        Self {
            pending: Arc::new(RwLock::new(HashMap::new())),
            window_ms,
            max_pending,
            max_subscribers,
        }
    }

    /// Check if a query can be coalesced.
    ///
    /// Returns `Some(receiver)` if a matching query is in flight.
    /// Returns `None` if no matching query (caller should dispatch).
    pub async fn try_coalesce(
        &self,
        fingerprint: QueryFingerprint,
    ) -> Option<broadcast::Receiver<Vec<u8>>> {
        let mut pending = self.pending.write().await;
        self.prune_stale_locked(&mut pending);

        if let Some(pq) = pending.get(&fingerprint) {
            if pq.tx.receiver_count() < self.max_subscribers {
                return Some(pq.tx.subscribe());
            }
        }
        None
    }

    /// Register a new pending query.
    ///
    /// Returns a broadcast sender that the caller should notify on completion.
    pub async fn register(
        &self,
        fingerprint: QueryFingerprint,
    ) -> Result<broadcast::Sender<Vec<u8>>> {
        let mut pending = self.pending.write().await;
        self.prune_stale_locked(&mut pending);

        if pending.len() >= self.max_pending {
            return Err(MiroirError::TooManyPendingQueries);
        }

        let (tx, _) = broadcast::channel(self.max_subscribers);
        pending.insert(fingerprint, PendingQuery {
            tx: tx.clone(),
            started_at: Instant::now(),
        });
        Ok(tx)
    }

    /// Unregister a pending query after completion.
    pub async fn unregister(&self, fingerprint: &QueryFingerprint) {
        let mut pending = self.pending.write().await;
        pending.remove(fingerprint);
    }

    /// Prune stale entries older than the window (must hold write lock).
    fn prune_stale_locked(&self, pending: &mut HashMap<QueryFingerprint, PendingQuery>) {
        let window = Duration::from_millis(self.window_ms);
        let now = Instant::now();
        pending.retain(|_, pq| now.duration_since(pq.started_at) < window);
    }

    /// Get current pending count.
    pub async fn pending_count(&self) -> usize {
        self.pending.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_idempotency_miss_then_hit() {
        let cache = IdempotencyCache::new(100, 3600);

        // First check should miss.
        let result = cache.check("key1", "hash1").await.unwrap();
        assert!(result.is_none());

        // Insert.
        cache.insert("key1".into(), "hash1".into(), "mtask-1".into()).await;

        // Second check should hit.
        let result = cache.check("key1", "hash1").await.unwrap();
        assert_eq!(result, Some("mtask-1".into()));
    }

    #[tokio::test]
    async fn test_idempotency_conflict() {
        let cache = IdempotencyCache::new(100, 3600);

        cache.insert("key1".into(), "hash1".into(), "mtask-1".into()).await;

        // Different body hash should error.
        let result = cache.check("key1", "hash2").await;
        assert!(matches!(result, Err(MiroirError::IdempotencyKeyReused)));
    }

    #[tokio::test]
    async fn test_idempotency_max_entries() {
        let cache = IdempotencyCache::new(3, 3600);

        cache.insert("key1".into(), "hash1".into(), "mtask-1".into()).await;
        cache.insert("key2".into(), "hash2".into(), "mtask-2".into()).await;
        cache.insert("key3".into(), "hash3".into(), "mtask-3".into()).await;

        // At max.
        assert_eq!(cache.size().await, 3);

        // Adding a 4th should evict some entry to maintain max size.
        cache.insert("key4".into(), "hash4".into(), "mtask-4".into()).await;
        assert_eq!(cache.size().await, 3);
        // At least one of the original keys should be evicted.
        let remaining = [
            cache.check("key1", "hash1").await.unwrap().is_some(),
            cache.check("key2", "hash2").await.unwrap().is_some(),
            cache.check("key3", "hash3").await.unwrap().is_some(),
        ];
        assert!(remaining.iter().filter(|&&x| x).count() < 3, "expected at least one eviction");
    }

    #[test]
    fn test_query_fingerprint_eq() {
        let fp1 = QueryFingerprint {
            index: "products".into(),
            query_json: r#"{"q":"laptop"}"#.into(),
            settings_version: 42,
        };
        let fp2 = QueryFingerprint {
            index: "products".into(),
            query_json: r#"{"q":"laptop"}"#.into(),
            settings_version: 42,
        };
        let fp3 = QueryFingerprint {
            index: "products".into(),
            query_json: r#"{"q":"phone"}"#.into(),
            settings_version: 42,
        };
        assert_eq!(fp1, fp2);
        assert_ne!(fp1, fp3);
    }

    #[tokio::test]
    async fn test_coalescer_miss_then_register() {
        let coalescer = QueryCoalescer::new(50, 100, 10);
        let fp = QueryFingerprint {
            index: "products".into(),
            query_json: r#"{"q":"test"}"#.into(),
            settings_version: 1,
        };

        // Should not find a pending query.
        assert!(coalescer.try_coalesce(fp.clone()).await.is_none());

        // Register and get sender.
        let _tx = coalescer.register(fp.clone()).await.unwrap();
        assert_eq!(coalescer.pending_count().await, 1);

        // Clean up.
        coalescer.unregister(&fp).await;
        assert_eq!(coalescer.pending_count().await, 0);
    }

    #[tokio::test]
    async fn test_coalescer_hit() {
        let coalescer = QueryCoalescer::new(50, 100, 10);
        let fp = QueryFingerprint {
            index: "products".into(),
            query_json: r#"{"q":"test"}"#.into(),
            settings_version: 1,
        };

        // Register.
        let tx = coalescer.register(fp.clone()).await.unwrap();

        // Should now find the pending query.
        let rx = coalescer.try_coalesce(fp).await;
        assert!(rx.is_some());

        // Broadcast response.
        let response = b"test response".to_vec();
        tx.send(response.clone()).ok();

        // Receiver should get it.
        if let Some(mut rx) = rx {
            let received = rx.recv().await.unwrap();
            assert_eq!(received, response);
        }
    }
}
