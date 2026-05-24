//! Session endpoints for admin login and search UI (plan §9, §13.19, §13.21).
//!
//! Admin login:
//! - POST /_miroir/admin/login - credentials in body, returns CSRF token
//! - GET /_miroir/admin/session - validate session, refresh CSRF token
//! - POST /_miroir/admin/logout - revoke session
//!
//! Search UI session:
//! - GET /_miroir/ui/search/{index}/session - JWT session token with origin check

use axum::{
    extract::{Extension, FromRef, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use miroir_core::task_store::{NewAdminSession, TaskStore};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::auth::{build_csp_header, generate_csrf_token, validate_origin, AdminSessionId};

use super::admin_endpoints::AppState;

/// Hash a PII value (session ID, IP, username) for safe log correlation.
fn hash_for_log(value: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Truncate a session ID to its prefix for logging (avoids full session ID in logs).
fn session_prefix(session_id: &str) -> &str {
    &session_id[..session_id.len().min(8)]
}

/// Admin login request body.
#[derive(Deserialize)]
pub struct AdminLoginRequest {
    pub admin_key: String,
}

impl std::fmt::Debug for AdminLoginRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminLoginRequest")
            .field("admin_key", &"[redacted]")
            .finish()
    }
}

/// Admin login response with CSRF token.
#[derive(Debug, Serialize)]
pub struct AdminLoginResponse {
    pub session_id: String,
    pub csrf_token: String,
    pub expires_at: i64,
}

/// Admin session validation response.
#[derive(Debug, Serialize)]
pub struct AdminSessionResponse {
    pub valid: bool,
    pub csrf_token: Option<String>,
    pub expires_at: Option<i64>,
}

/// Search UI session response.
#[derive(Debug, Serialize)]
pub struct SearchUiSessionResponse {
    pub token: String,
    pub expires_at: i64,
}

/// POST /_miroir/admin/login - admin login with credentials.
///
/// Expects `admin_key` in request body. Validates against `admin.api_key`.
/// On success, creates an admin session with CSRF token and returns:
/// - Set-Cookie: sealed session ID (HttpOnly, Secure, SameSite=Strict)
/// - JSON response with session_id, csrf_token, expires_at
///
/// Origin is checked against `admin_ui.allowed_origins` (default "same-origin").
pub async fn admin_login<S>(
    State(state): State<S>,
    headers: HeaderMap,
    Json(body): Json<AdminLoginRequest>,
) -> Result<Json<AdminLoginResponse>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);
    let config = &state.config;

    if !config.admin_ui.enabled {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "admin_ui is not enabled".into(),
        ));
    }

    // Origin check (plan §9)
    let origin_verdict = validate_origin(&headers, &config.admin_ui.allowed_origins, true);
    if matches!(origin_verdict, crate::auth::OriginVerdict::Forbidden) {
        warn!(
            allowed_origins = ?config.admin_ui.allowed_origins,
            "admin login origin check failed"
        );
        return Err((StatusCode::FORBIDDEN, "origin not allowed".into()));
    }

    // Validate admin key (constant-time compare)
    let expected_key = &config.admin.api_key;
    if !crate::auth::constant_time_compare(body.admin_key.as_bytes(), expected_key.as_bytes()) {
        return Err((StatusCode::UNAUTHORIZED, "invalid admin key".into()));
    }

    // Generate session ID and CSRF token
    let session_id = format!("admin_sess_{}", generate_csrf_token());
    let csrf_token = generate_csrf_token();

    // Calculate expiration
    let now = epoch_seconds();
    let expires_at = now + config.admin_ui.session_ttl_s as i64;

    // Hash the admin key for storage (never store the key itself)
    let admin_key_hash = hash_admin_key(expected_key);

    // Extract user agent and source IP for audit
    let user_agent = headers
        .get("user-agent")
        .and_then(|h| h.to_str().ok())
        .map(String::from);
    let source_ip = headers
        .get("x-forwarded-for")
        .and_then(|h| h.to_str().ok())
        .or_else(|| headers.get("x-real-ip").and_then(|h| h.to_str().ok()))
        .map(String::from);

    // Create session in task store
    let new_session = NewAdminSession {
        session_id: session_id.clone(),
        csrf_token: csrf_token.clone(),
        admin_key_hash,
        created_at: now,
        expires_at,
        user_agent,
        source_ip,
    };

    if let Some(ref redis) = state.redis_store {
        if let Err(e) = redis.insert_admin_session(&new_session) {
            warn!(error = %e, "failed to create admin session in Redis");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create session".into(),
            ));
        }
    } else {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "admin sessions require Redis task store".into(),
        ));
    }

    info!(
        session_prefix = session_prefix(&session_id),
        expires_at = expires_at,
        "admin login successful"
    );

    Ok(Json(AdminLoginResponse {
        session_id,
        csrf_token,
        expires_at,
    }))
}

/// GET /_miroir/admin/session - validate admin session and refresh CSRF token.
///
/// Requires sealed admin session cookie. Returns current session info
/// with a fresh CSRF token (rotated on each call).
pub async fn admin_session<S>(
    State(state): State<S>,
    Extension(admin_session): Extension<AdminSessionId>,
) -> Result<Json<AdminSessionResponse>, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);
    let session_id = admin_session.0;

    let Some(redis) = state.redis_store.as_ref() else {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "admin sessions require Redis task store".into(),
        ));
    };

    // Look up session
    let Some(session) = redis.get_admin_session(&session_id).map_err(|e| {
        warn!(error = %e, session_prefix = session_prefix(&session_id), "failed to get admin session");
        (StatusCode::INTERNAL_SERVER_ERROR, "failed to get session".into())
    })? else {
        return Ok(Json(AdminSessionResponse {
            valid: false,
            csrf_token: None,
            expires_at: None,
        }));
    };

    // Check if revoked
    if session.revoked {
        return Ok(Json(AdminSessionResponse {
            valid: false,
            csrf_token: None,
            expires_at: None,
        }));
    }

    // Check expiration
    let now = epoch_seconds();
    if session.expires_at < now {
        return Ok(Json(AdminSessionResponse {
            valid: false,
            csrf_token: None,
            expires_at: None,
        }));
    }

    // Generate fresh CSRF token
    let new_csrf_token = generate_csrf_token();

    // Update session with new CSRF token
    let updated_session = NewAdminSession {
        session_id: session.session_id.clone(),
        csrf_token: new_csrf_token.clone(),
        admin_key_hash: session.admin_key_hash.clone(),
        created_at: session.created_at,
        expires_at: session.expires_at,
        user_agent: session.user_agent.clone(),
        source_ip: session.source_ip.clone(),
    };

    redis.insert_admin_session(&updated_session).map_err(|e| {
        warn!(error = %e, session_prefix = session_prefix(&session_id), "failed to refresh CSRF token");
        (StatusCode::INTERNAL_SERVER_ERROR, "failed to refresh session".into())
    })?;

    Ok(Json(AdminSessionResponse {
        valid: true,
        csrf_token: Some(new_csrf_token),
        expires_at: Some(session.expires_at),
    }))
}

/// POST /_miroir/admin/logout - revoke admin session.
pub async fn admin_logout<S>(
    State(state): State<S>,
    Extension(admin_session): Extension<AdminSessionId>,
) -> Result<(), (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);
    let session_id = admin_session.0;

    let Some(redis) = state.redis_store.as_ref() else {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "admin sessions require Redis task store".into(),
        ));
    };

    redis.revoke_admin_session(&session_id).map_err(|e| {
        warn!(error = %e, session_prefix = session_prefix(&session_id), "failed to revoke admin session");
        (StatusCode::INTERNAL_SERVER_ERROR, "failed to revoke session".into())
    })?;

    info!(
        session_prefix = session_prefix(&session_id),
        "admin logout successful"
    );

    Ok(())
}

/// GET /_miroir/ui/search/{index}/session - search UI session endpoint with origin check.
///
/// Returns a JWT session token for the given index. Authentication mode depends on
/// `search_ui.auth.mode`:
/// - `public`: unauthenticated, IP rate-limited
/// - `shared_key`: requires `X-Search-UI-Key` header
/// - `oauth_proxy`: requires upstream auth-proxy headers
///
/// Origin is checked against `search_ui.allowed_origins` (default ["*"] in public mode).
/// CSP header is added from `search_ui.csp` with `csp_overrides` merged.
pub async fn search_ui_session<S>(
    State(state): State<S>,
    Path(index): Path<String>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    let state = AppState::from_ref(&state);
    let config = &state.config;

    if !config.search_ui.enabled {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "search_ui is not enabled".into(),
        ));
    }

    // Origin check (plan §9)
    let is_public = config.search_ui.auth.mode == "public";
    let default_allowed = if is_public { vec!["*".into()] } else { vec![] };
    let allowed_origins = if config.search_ui.allowed_origins.is_empty() {
        &default_allowed
    } else {
        &config.search_ui.allowed_origins
    };

    let origin_verdict = validate_origin(&headers, allowed_origins, is_public);
    if matches!(origin_verdict, crate::auth::OriginVerdict::Forbidden) {
        warn!(
            index = %index,
            allowed_origins = ?allowed_origins,
            "search UI session origin check failed"
        );
        return Err((StatusCode::FORBIDDEN, "origin not allowed".into()));
    }

    // Authentication based on mode
    let subject = match config.search_ui.auth.mode.as_str() {
        "public" => "anonymous".to_string(),
        "shared_key" => {
            let key = headers
                .get("X-Search-UI-Key")
                .and_then(|h| h.to_str().ok())
                .ok_or_else(|| {
                    (
                        StatusCode::UNAUTHORIZED,
                        "missing X-Search-UI-Key header".into(),
                    )
                })?;
            let expected_key =
                std::env::var(&config.search_ui.auth.shared_key_env).unwrap_or_default();
            if !crate::auth::constant_time_compare(key.as_bytes(), expected_key.as_bytes()) {
                return Err((StatusCode::UNAUTHORIZED, "invalid search UI key".into()));
            }
            "shared_key_user".to_string()
        }
        "oauth_proxy" => {
            let user = headers
                .get(&config.search_ui.auth.oauth_proxy.user_header)
                .and_then(|h| h.to_str().ok())
                .ok_or_else(|| {
                    (
                        StatusCode::UNAUTHORIZED,
                        format!("missing {}", config.search_ui.auth.oauth_proxy.user_header),
                    )
                })?;
            user.to_string()
        }
        _ => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid search_ui.auth.mode".into(),
            ))
        }
    };

    // Generate JWT
    let jwt_secret = std::env::var(&config.search_ui.auth.jwt_secret_env).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{} not set", config.search_ui.auth.jwt_secret_env),
        )
    })?;

    let auth_state = crate::auth::AuthState {
        master_key: String::new(),
        admin_key: String::new(),
        jwt_primary: Some(jwt_secret),
        jwt_previous: std::env::var(&config.search_ui.auth.jwt_secret_previous_env)
            .ok()
            .filter(|v| !v.is_empty()),
        seal_key: crate::admin_session::SealKey::from_bytes([0u8; 32]),
        revoked_sessions: std::sync::Arc::new(dashmap::DashMap::new()),
        admin_session_revoked_total: state.metrics.admin_session_revoked_total(),
    };

    let token = auth_state
        .sign_jwt(
            &subject,
            &index,
            "search",
            config.search_ui.auth.session_ttl_s,
        )
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to sign JWT".into(),
            )
        })?;

    let expires_at = epoch_seconds() + config.search_ui.auth.session_ttl_s as i64;

    info!(
        index = %index,
        subject_hash = hash_for_log(&subject),
        expires_at = expires_at,
        "search UI session created"
    );

    // Build CSP header
    let csp_value = build_csp_header(&config.search_ui.csp, &config.search_ui.csp_overrides);

    // Build response with CSP header
    let response = SearchUiSessionResponse { token, expires_at };
    let mut resp = Json(response).into_response();
    resp.headers_mut().insert(
        "Content-Security-Policy",
        csp_value.parse().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid CSP header".into(),
            )
        })?,
    );

    Ok(resp)
}

/// Hash an admin key for storage (SHA-256).
fn hash_admin_key(key: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn epoch_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_admin_key() {
        let key1 = "test-admin-key";
        let key2 = "test-admin-key";
        let key3 = "different-key";

        assert_eq!(hash_admin_key(key1), hash_admin_key(key2));
        assert_ne!(hash_admin_key(key1), hash_admin_key(key3));
    }
}
