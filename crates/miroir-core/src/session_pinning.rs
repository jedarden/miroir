//! Session pinning for read-your-writes consistency (plan §13.6).
//!
//! Clients provide X-Miroir-Session header; Miroir tracks pending writes
//! and routes subsequent reads to the pinned replica group.

use crate::error::{MiroirError, Result};
use crate::task::{TaskRegistry, TaskStatus};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Session pinning configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionPinningConfig {
    /// Whether session pinning is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Session TTL in seconds.
    #[serde(default = "default_ttl")]
    pub ttl_seconds: u64,
    /// Maximum number of sessions.
    #[serde(default = "default_max_sessions")]
    pub max_sessions: u32,
    /// Wait strategy: "block" or "route_pin".
    #[serde(default = "default_wait_strategy")]
    pub wait_strategy: String,
    /// Maximum wait time in milliseconds.
    #[serde(default = "default_max_wait")]
    pub max_wait_ms: u64,
}

fn default_true() -> bool {
    true
}
fn default_ttl() -> u64 {
    900
}
fn default_max_sessions() -> u32 {
    100_000
}
fn default_wait_strategy() -> String {
    "block".into()
}
fn default_max_wait() -> u64 {
    5000
}

impl Default for SessionPinningConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_seconds: default_ttl(),
            max_sessions: default_max_sessions(),
            wait_strategy: default_wait_strategy(),
            max_wait_ms: default_max_wait(),
        }
    }
}

impl From<crate::config::advanced::SessionPinningConfig> for SessionPinningConfig {
    fn from(config: crate::config::advanced::SessionPinningConfig) -> Self {
        Self {
            enabled: config.enabled,
            ttl_seconds: config.ttl_seconds,
            max_sessions: config.max_sessions,
            wait_strategy: config.wait_strategy,
            max_wait_ms: config.max_wait_ms,
        }
    }
}

/// Session state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionState {
    /// Last write miroir task ID (if any).
    pub last_write_mtask_id: Option<String>,
    /// Last write timestamp.
    pub last_write_at: u64,
    /// Pinned replica group ID.
    pub pinned_group: Option<u32>,
    /// Minimum settings version observed by this session.
    pub min_settings_version: u64,
    /// Session created at.
    pub created_at: u64,
    /// Session expires at.
    pub expires_at: u64,
}

impl SessionState {
    /// Check if this session is expired.
    pub fn is_expired(&self) -> bool {
        let now = millis_now();
        now > self.expires_at
    }

    /// Check if there's a pending write.
    pub fn has_pending_write(&self) -> bool {
        self.last_write_mtask_id.is_some()
    }
}

/// Session pinning manager.
pub struct SessionManager {
    /// Configuration.
    config: SessionPinningConfig,
    /// Session ID -> Session state (IndexMap maintains insertion order for LRU).
    sessions: Arc<RwLock<IndexMap<String, SessionState>>>,
    /// Per-index pending writes (session_id -> mtask_id).
    pending_writes: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
}

impl SessionManager {
    /// Create a new session manager.
    pub fn new(config: SessionPinningConfig) -> Self {
        Self {
            config,
            sessions: Arc::new(RwLock::new(IndexMap::new())),
            pending_writes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record a write for a session.
    ///
    /// Returns the group ID that was pinned (first to reach quorum).
    ///
    /// This method should be called AFTER per-group quorum is achieved,
    /// with the `first_quorum_group` being the first group to reach quorum.
    pub async fn record_write_with_quorum(
        &self,
        session_id: &str,
        mtask_id: String,
        first_quorum_group: u32,
    ) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let now = millis_now();
        let expires_at = now + (self.config.ttl_seconds * 1000);

        let mut sessions = self.sessions.write().await;

        // Enforce max sessions (simple FIFO - remove oldest entry when at capacity)
        if sessions.len() >= self.config.max_sessions as usize {
            // Remove oldest entry (first key in HashMap)
            if let Some(key) = sessions.keys().next().cloned() {
                sessions.remove(&key);
                debug!(session_id = %key, "evicted oldest session to enforce max_sessions");
            }
        }

        // Get or create session
        let session = sessions.entry(session_id.to_string()).or_insert(SessionState {
            last_write_mtask_id: None,
            last_write_at: 0,
            pinned_group: None,
            min_settings_version: 0,
            created_at: now,
            expires_at,
        });

        // Update session state
        session.last_write_mtask_id = Some(mtask_id.clone());
        session.last_write_at = now;
        session.expires_at = expires_at;

        // Pin the group if not already pinned (first write wins)
        if session.pinned_group.is_none() {
            session.pinned_group = Some(first_quorum_group);
            info!(
                session_id = %session_id,
                mtask_id = %mtask_id,
                pinned_group = first_quorum_group,
                "session pinned to first-quorum group"
            );
        } else {
            debug!(
                session_id = %session_id,
                existing_group = session.pinned_group,
                new_quorum_group = first_quorum_group,
                "session already pinned, ignoring new quorum group"
            );
        }

        // Track pending write per session
        let mut pending = self.pending_writes.write().await;
        pending.entry(session_id.to_string())
            .or_insert_with(HashMap::new)
            .insert(mtask_id.clone(), first_quorum_group.to_string());

        Ok(())
    }

    /// Wait for a pending write to complete (block strategy).
    ///
    /// Polls the task registry until the write succeeds or times out.
    pub async fn wait_for_write_completion<T: TaskRegistry + ?Sized>(
        &self,
        session_id: &str,
        task_registry: &Arc<T>,
        max_wait: Duration,
    ) -> Result<bool> {
        let session = {
            let sessions = self.sessions.read().await;
            sessions.get(session_id).cloned()
        };

        let session = session.ok_or_else(|| {
            MiroirError::InvalidRequest("session not found".to_string())
        })?;

        let mtask_id = session.last_write_mtask_id.ok_or_else(|| {
            MiroirError::InvalidRequest("no pending write for session".to_string())
        })?;

        let start = SystemTime::now();
        let mut poll_delay = 25; // Start with 25ms

        loop {
            // Check task status
            if let Ok(Some(task)) = task_registry.get(&mtask_id) {
                match task.status {
                    TaskStatus::Succeeded => {
                        // Clear pending write state
                        self.clear_pending_write(session_id).await;
                        return Ok(true);
                    }
                    TaskStatus::Failed | TaskStatus::Canceled => {
                        // Clear pending write state even on failure
                        self.clear_pending_write(session_id).await;
                        return Ok(false);
                    }
                    _ => {}
                }
            }

            // Check timeout
            let elapsed = start.elapsed().unwrap_or(Duration::ZERO);
            if elapsed >= max_wait {
                warn!(
                    session_id = %session_id,
                    mtask_id = %mtask_id,
                    elapsed_ms = elapsed.as_millis(),
                    max_wait_ms = max_wait.as_millis(),
                    "session pin wait timeout"
                );
                return Err(MiroirError::InvalidState("session pin wait timeout".to_string()));
            }

            // Exponential backoff with cap
            tokio::time::sleep(Duration::from_millis(poll_delay)).await;
            poll_delay = std::cmp::min(poll_delay * 2, 500); // Cap at 500ms
        }
    }

    /// Get the pinned group for a session (if any).
    ///
    /// Returns None if:
    /// - Session doesn't exist
    /// - Session has no pending write
    /// - Session is expired
    pub async fn get_pinned_group(&self, session_id: &str) -> Option<u32> {
        if !self.config.enabled {
            return None;
        }

        let sessions = self.sessions.read().await;
        let session = sessions.get(session_id)?;

        if session.is_expired() {
            return None;
        }

        if !session.has_pending_write() {
            return None;
        }

        session.pinned_group
    }

    /// Clear the pending write state for a session.
    ///
    /// Called when the write task completes.
    pub async fn clear_pending_write(&self, session_id: &str) {
        let mut pending = self.pending_writes.write().await;
        pending.remove(session_id);

        // Also clear the last_write_mtask_id in the session
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            session.last_write_mtask_id = None;
        }
    }

    /// Get session state.
    pub async fn get_session(&self, session_id: &str) -> Option<SessionState> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).cloned()
    }

    /// Delete a session.
    pub async fn delete_session(&self, session_id: &str) -> bool {
        let mut sessions = self.sessions.write().await;
        let mut pending = self.pending_writes.write().await;
        pending.remove(session_id);
        sessions.remove(session_id).is_some()
    }

    /// Clean up expired sessions.
    pub async fn prune_expired(&self) -> usize {
        let mut sessions = self.sessions.write().await;
        let mut pending = self.pending_writes.write().await;

        let now = millis_now();
        let mut to_remove = Vec::new();

        for (id, session) in sessions.iter() {
            if session.is_expired() {
                to_remove.push(id.clone());
            }
        }

        for id in &to_remove {
            sessions.remove(id);
            pending.remove(id);
        }

        to_remove.len()
    }

    /// Get current session count.
    pub async fn session_count(&self) -> usize {
        let sessions = self.sessions.read().await;
        sessions.len()
    }

    /// Handle pinned group failure - clear the pin for a session.
    ///
    /// Called when the pinned group for a session becomes unavailable.
    /// Subsequent reads will use normal routing.
    pub async fn handle_pinned_group_failure(&self, session_id: &str, failed_group: u32) -> bool {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            if session.pinned_group == Some(failed_group) {
                info!(
                    session_id = %session_id,
                    failed_group,
                    "clearing session pin due to group failure"
                );
                session.pinned_group = None;
                // Also clear pending write state since we can't guarantee visibility
                session.last_write_mtask_id = None;
                return true;
            }
        }
        false
    }

    /// Get the wait strategy.
    pub fn wait_strategy(&self) -> WaitStrategy {
        match self.config.wait_strategy.as_str() {
            "block" => WaitStrategy::Block,
            "route_pin" => WaitStrategy::RoutePin,
            _ => WaitStrategy::Block,
        }
    }

    /// Get max wait duration.
    pub fn max_wait_duration(&self) -> Duration {
        Duration::from_millis(self.config.max_wait_ms)
    }

    /// Update the session active count metric.
    pub fn update_metrics(&self, active_count_fn: impl FnOnce(usize)) {
        let count = self.sessions.blocking_read().len();
        active_count_fn(count);
    }

    /// Check if session pinning is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

/// Wait strategy for reads with pending writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitStrategy {
    /// Block the read until the write completes.
    Block,
    /// Route to pinned group but don't wait for write.
    RoutePin,
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new(SessionPinningConfig::default())
    }
}

/// Get current UNIX timestamp in milliseconds.
fn millis_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = SessionPinningConfig::default();
        assert!(config.enabled);
        assert_eq!(config.ttl_seconds, 900);
        assert_eq!(config.max_sessions, 100_000);
        assert_eq!(config.wait_strategy, "block");
        assert_eq!(config.max_wait_ms, 5000);
    }

    #[tokio::test]
    async fn test_record_write() {
        let manager = SessionManager::default();
        manager
            .record_write_with_quorum("session-1", "mtask-123".into(), 0)
            .await
            .unwrap();

        let session = manager.get_session("session-1").await.unwrap();
        assert_eq!(session.last_write_mtask_id, Some("mtask-123".into()));
        assert_eq!(session.pinned_group, Some(0));
        assert!(session.has_pending_write());
    }

    #[tokio::test]
    async fn test_pinned_group() {
        let manager = SessionManager::default();
        manager
            .record_write_with_quorum("session-1", "mtask-123".into(), 2)
            .await
            .unwrap();

        let pinned = manager.get_pinned_group("session-1").await;
        assert_eq!(pinned, Some(2));
    }

    #[tokio::test]
    async fn test_clear_pending_write() {
        let manager = SessionManager::default();
        manager
            .record_write_with_quorum("session-1", "mtask-123".into(), 0)
            .await
            .unwrap();

        manager.clear_pending_write("session-1").await;

        let session = manager.get_session("session-1").await.unwrap();
        assert!(!session.has_pending_write());

        let pinned = manager.get_pinned_group("session-1").await;
        assert_eq!(pinned, None); // No pending write = no pin
    }

    #[tokio::test]
    async fn test_max_sessions() {
        let config = SessionPinningConfig {
            max_sessions: 2,
            ..Default::default()
        };
        let manager = SessionManager::new(config);

        manager
            .record_write_with_quorum("session-1", "mtask-1".into(), 0)
            .await
            .unwrap();
        manager
            .record_write_with_quorum("session-2", "mtask-2".into(), 0)
            .await
            .unwrap();
        manager
            .record_write_with_quorum("session-3", "mtask-3".into(), 0)
            .await
            .unwrap();

        // session-1 should be evicted (FIFO)
        assert!(manager.get_session("session-1").await.is_none());
        assert!(manager.get_session("session-2").await.is_some());
        assert!(manager.get_session("session-3").await.is_some());
    }

    #[tokio::test]
    async fn test_wait_strategy() {
        let config = SessionPinningConfig {
            wait_strategy: "route_pin".into(),
            ..Default::default()
        };
        let manager = SessionManager::new(config);
        assert_eq!(manager.wait_strategy(), WaitStrategy::RoutePin);
    }

    #[test]
    fn test_wait_strategy_parse() {
        let config = SessionPinningConfig {
            wait_strategy: "block".into(),
            ..Default::default()
        };
        let manager = SessionManager::new(config);
        assert_eq!(manager.wait_strategy(), WaitStrategy::Block);

        let config = SessionPinningConfig {
            wait_strategy: "unknown".into(),
            ..Default::default()
        };
        let manager = SessionManager::new(config);
        assert_eq!(manager.wait_strategy(), WaitStrategy::Block); // Default
    }
}
