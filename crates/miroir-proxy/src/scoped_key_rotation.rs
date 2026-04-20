//! Scoped Meilisearch key rotation (plan §13.21).
//!
//! Implements the leader-based rotation sequence:
//! 1. Mint new scoped key via Meilisearch `POST /keys`
//! 2. Write new generation to Redis hash
//! 3. Wait for all live pods to observe new generation (beacon check)
//! 4. Drain wait for stragglers
//! 5. `DELETE /keys/{previous_uid}` on all Meilisearch nodes
//! 6. Clear previous from Redis hash

use miroir_core::config::MiroirConfig;
use miroir_core::task_store::{RedisTaskStore, SearchUiScopedKey, TaskStore};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn, error};

use crate::routes::indexes::MeilisearchClient;

/// State for the scoped key rotation background task.
#[derive(Clone)]
pub struct ScopedKeyRotationState {
    pub config: Arc<MiroirConfig>,
    pub redis: RedisTaskStore,
    pub pod_id: String,
}

/// Response body for the manual rotation endpoint.
#[derive(serde::Serialize)]
pub struct RotateScopedKeyResponse {
    pub status: String,
    pub index_uid: String,
    pub generation: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_uid_revoked: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request body for the manual rotation endpoint.
#[derive(serde::Deserialize)]
pub struct RotateScopedKeyRequest {
    /// If true, bypass the timing gate and rotate immediately.
    #[serde(default)]
    pub force: bool,
}

/// Run the background scoped key rotation loop.
pub async fn run_scoped_key_rotator(state: ScopedKeyRotationState) {
    if !state.config.search_ui.enabled {
        return;
    }

    let check_interval = Duration::from_secs(3600); // Check every hour
    let mut interval = tokio::time::interval(check_interval);

    info!("scoped key rotation background task started");

    loop {
        interval.tick().await;

        // Refresh our own pod presence heartbeat
        if let Err(e) = state.redis.register_pod_presence(&state.pod_id) {
            warn!(pod_id = %state.pod_id, error = %e, "failed to register pod presence");
        }

        // Try to acquire the rotation leader lease
        let lease_scope = "search_ui_key_rotation";
        let lease_now = now_ms();
        let lease_ttl_ms = (state.config.leader_election.lease_ttl_s as i64) * 1000;
        let expires_at = lease_now + lease_ttl_ms;

        match state.redis.try_acquire_leader_lease(lease_scope, &state.pod_id, expires_at, lease_now) {
            Ok(true) => {}
            Ok(false) => continue, // Another pod holds the lease
            Err(e) => {
                warn!(error = %e, "failed to acquire rotation leader lease");
                continue;
            }
        }

        // We are the leader — check each index for rotation need
        let indexes = discover_scoped_indexes(&state).await;
        for index_uid in indexes {
            if let Err(e) = check_and_rotate(&state, &index_uid, false).await {
                error!(index = %index_uid, error = %e, "scoped key rotation failed");
            }
        }

        // Renew the lease
        let _ = state.redis.renew_leader_lease(
            lease_scope,
            &state.pod_id,
            now_ms() + lease_ttl_ms,
        );
    }
}

/// Check if a scoped key needs rotation and perform it if so.
pub async fn check_and_rotate(
    state: &ScopedKeyRotationState,
    index_uid: &str,
    force: bool,
) -> Result<RotateScopedKeyResponse, String> {
    let current = state.redis.get_search_ui_scoped_key(index_uid)
        .map_err(|e| format!("redis read failed: {e}"))?;

    // Timing gate check (skip if force)
    if !force {
        if !should_rotate(&current, &state.config) {
            return Ok(RotateScopedKeyResponse {
                status: "skipped".into(),
                index_uid: index_uid.into(),
                generation: current.as_ref().map(|k| k.generation).unwrap_or(0),
                previous_uid_revoked: None,
                error: None,
            });
        }
    }

    // Step 1: Mint new scoped key via Meilisearch POST /keys
    let client = MeilisearchClient::new(state.config.node_master_key.clone());
    let (new_key, new_uid) = mint_scoped_key(&client, &state.config, index_uid).await?;

    // Step 2: Write new generation to Redis
    let new_generation = current.as_ref().map(|k| k.generation + 1).unwrap_or(1);
    let previous_uid = current.as_ref().map(|k| k.primary_uid.clone());
    let previous_key = current.as_ref().map(|k| k.primary_key.clone());

    let scoped_key = SearchUiScopedKey {
        index_uid: index_uid.into(),
        primary_key: new_key.clone(),
        primary_uid: new_uid.clone(),
        previous_key,
        previous_uid: previous_uid.clone(),
        rotated_at: now_ms(),
        generation: new_generation,
    };

    state.redis.set_search_ui_scoped_key(&scoped_key)
        .map_err(|e| format!("redis write failed: {e}"))?;

    info!(
        index = %index_uid,
        generation = new_generation,
        "new scoped key minted, waiting for pod observation"
    );

    // Step 3: Observe our own beacon immediately
    state.redis.observe_search_ui_scoped_key(
        &state.pod_id,
        index_uid,
        new_generation,
    ).map_err(|e| format!("beacon write failed: {e}"))?;

    // Step 4: Wait for drain period and check beacons
    let drain_s = state.config.search_ui.scoped_key_rotation_drain_s;
    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(drain_s);

    let mut previous_revoked: Option<String> = None;

    loop {
        // Get live pods
        let live_pods = state.redis.get_live_pods()
            .map_err(|e| format!("get_live_pods failed: {e}"))?;

        // Check if all live pods have observed the new generation
        let (all_observed, unobserved) = state.redis.check_scoped_key_observation(
            index_uid,
            new_generation,
            &live_pods,
        ).map_err(|e| format!("beacon check failed: {e}"))?;

        if all_observed {
            info!(
                index = %index_uid,
                generation = new_generation,
                "all live pods observed new generation, revoking previous key"
            );

            // Step 6: Delete previous key from Meilisearch and clear from Redis
            if let Some(ref prev_uid) = previous_uid {
                if let Err(e) = revoke_previous_key(&client, &state.config, prev_uid).await {
                    warn!(previous_uid = %prev_uid, error = %e, "failed to revoke previous key, will retry");
                } else {
                    previous_revoked = Some(prev_uid.clone());
                    // Clear previous from Redis
                    if let Err(e) = state.redis.clear_scoped_key_previous(index_uid) {
                        warn!(error = %e, "failed to clear previous key from redis");
                    }
                    info!(index = %index_uid, previous_uid = %prev_uid, "previous scoped key revoked");
                }
            }

            return Ok(RotateScopedKeyResponse {
                status: "rotated".into(),
                index_uid: index_uid.into(),
                generation: new_generation,
                previous_uid_revoked: previous_revoked,
                error: None,
            });
        }

        // Not all pods have caught up yet
        if tokio::time::Instant::now() >= drain_deadline {
            warn!(
                index = %index_uid,
                generation = new_generation,
                unobserved = ?unobserved,
                "drain wait expired, {} pods still unobserved — will retry on next tick",
                unobserved.len()
            );

            return Ok(RotateScopedKeyResponse {
                status: "drain_pending".into(),
                index_uid: index_uid.into(),
                generation: new_generation,
                previous_uid_revoked: None,
                error: Some(format!(
                    "drain wait expired: {} pods unobserved: {}",
                    unobserved.len(),
                    unobserved.join(", ")
                )),
            });
        }

        // Wait before rechecking (every 10 seconds)
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

/// Check if rotation should happen based on timing gate.
fn should_rotate(current: &Option<SearchUiScopedKey>, config: &MiroirConfig) -> bool {
    let Some(key) = current else {
        // No key exists yet — need to mint initial key
        return true;
    };

    let max_age_ms = (config.search_ui.scoped_key_max_age_days as i64) * 24 * 3600 * 1000;
    let rotate_before_ms = (config.search_ui.scoped_key_rotate_before_expiry_days as i64) * 24 * 3600 * 1000;
    let age = now_ms() - key.rotated_at;

    // Rotate if key has been alive long enough that it will expire within rotate_before_days
    age >= (max_age_ms - rotate_before_ms)
}

/// Mint a new scoped Meilisearch key via POST /keys on all nodes.
async fn mint_scoped_key(
    client: &MeilisearchClient,
    config: &MiroirConfig,
    index_uid: &str,
) -> Result<(String, String), String> {
    let description = format!("miroir search-ui scoped key for index {}", index_uid);
    let body = serde_json::json!({
        "description": description,
        "actions": ["search"],
        "indexes": [index_uid],
        "expiresAt": null,
    });

    let mut created_key: Option<String> = None;
    let mut created_uid: Option<String> = None;
    let mut errors = Vec::new();

    for node in &config.nodes {
        match client.post_raw(&node.address, "/keys", &body).await {
            Ok((status, text)) if status >= 200 && status < 300 => {
                if created_key.is_none() {
                    let resp: serde_json::Value = serde_json::from_str(&text)
                        .map_err(|e| format!("parse key response: {e}"))?;
                    created_key = resp.get("key").and_then(|v| v.as_str()).map(String::from);
                    created_uid = resp.get("uid").and_then(|v| v.as_str()).map(String::from);
                }
            }
            Ok((status, text)) => {
                errors.push(format!("{}: HTTP {} — {}", node.id, status, text));
            }
            Err(e) => {
                errors.push(format!("{}: {}", node.id, e));
            }
        }
    }

    let key = created_key.ok_or_else(|| format!("failed to mint key on any node: {}", errors.join("; ")))?;
    let uid = created_uid.ok_or_else(|| String::from("key created but no uid returned"))?;

    Ok((key, uid))
}

/// Revoke a previous scoped key from all Meilisearch nodes.
async fn revoke_previous_key(
    client: &MeilisearchClient,
    config: &MiroirConfig,
    previous_uid: &str,
) -> Result<(), String> {
    let path = format!("/keys/{}", previous_uid);
    let mut errors = Vec::new();

    for node in &config.nodes {
        match client.delete_raw(&node.address, &path).await {
            Ok((_status, _text)) if _status >= 200 && _status < 300 => {}
            Ok((status, text)) => {
                // 404 is fine — key was already revoked or never existed
                if status != 404 {
                    errors.push(format!("{}: HTTP {} — {}", node.id, status, text));
                }
            }
            Err(e) => {
                errors.push(format!("{}: {}", node.id, e));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!("partial revoke failure: {}", errors.join("; ")))
    }
}

/// Discover which indexes have scoped keys (or should have them).
/// For now, we look at existing keys in Redis. New indexes get initial
/// keys on their first search request.
async fn discover_scoped_indexes(state: &ScopedKeyRotationState) -> Vec<String> {
    // Scan for existing scoped key hashes
    state.redis.list_scoped_key_indexes()
        .unwrap_or_default()
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

    #[test]
    fn should_rotate_no_key() {
        let config = MiroirConfig::default();
        assert!(should_rotate(&None, &config));
    }

    #[test]
    fn should_rotate_old_key() {
        let config = MiroirConfig {
            search_ui: miroir_core::config::SearchUiConfig {
                scoped_key_max_age_days: 60,
                scoped_key_rotate_before_expiry_days: 30,
                ..Default::default()
            },
            ..MiroirConfig::default()
        };

        // Key created 31 days ago — should rotate (max_age - rotate_before = 30 days)
        let key = SearchUiScopedKey {
            index_uid: "test".into(),
            primary_key: "key".into(),
            primary_uid: "uid".into(),
            previous_key: None,
            previous_uid: None,
            rotated_at: now_ms() - (31 * 24 * 3600 * 1000),
            generation: 1,
        };

        assert!(should_rotate(&Some(key), &config));
    }

    #[test]
    fn should_not_rotate_fresh_key() {
        let config = MiroirConfig {
            search_ui: miroir_core::config::SearchUiConfig {
                scoped_key_max_age_days: 60,
                scoped_key_rotate_before_expiry_days: 30,
                ..Default::default()
            },
            ..MiroirConfig::default()
        };

        // Key created 10 days ago — should NOT rotate yet
        let key = SearchUiScopedKey {
            index_uid: "test".into(),
            primary_key: "key".into(),
            primary_uid: "uid".into(),
            previous_key: None,
            previous_uid: None,
            rotated_at: now_ms() - (10 * 24 * 3600 * 1000),
            generation: 1,
        };

        assert!(!should_rotate(&Some(key), &config));
    }
}
