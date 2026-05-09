//! Bearer-token dispatch per plan §5
//!
//! Implements rules 2-5 for master-key/admin-key bearer dispatch:
//! - Rule 2: master-key can access all endpoints (full admin access)
//! - Rule 3: admin-key can access admin-only endpoints (/admin/*, /_miroir/*)
//! - Rule 4: No bearer token → public endpoints only (/health, /version)
//! - Rule 5: Invalid token → 403 Forbidden

use axum::{
    extract::State,
    http::HeaderMap,
    middleware::Next,
    response::Response,
};
use crate::state::ProxyState;
use crate::error_response::ErrorResponse;

/// Token kind determined from the bearer token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    /// Master key - full access to all endpoints.
    Master,
    /// Admin key - access to admin endpoints only.
    Admin,
}

/// Authentication result from bearer token validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthResult {
    /// Valid token with its kind.
    Valid(TokenKind),
    /// No bearer token present.
    None,
    /// Invalid bearer token.
    Invalid,
}

/// Validate bearer token against the configured keys.
pub fn validate_bearer_token(headers: &HeaderMap, state: &ProxyState) -> AuthResult {
    let auth_header = match headers.get("authorization") {
        Some(h) => h,
        None => return AuthResult::None,
    };

    let auth_str = match auth_header.to_str() {
        Ok(s) => s,
        Err(_) => return AuthResult::Invalid,
    };

    let token = match auth_str.strip_prefix("Bearer ") {
        Some(t) => t,
        None => return AuthResult::None,
    };

    // Check master key first (rule 2)
    if state.is_valid_master_key(token) {
        return AuthResult::Valid(TokenKind::Master);
    }

    // Check admin key (rule 3)
    if state.is_valid_admin_key(token) {
        return AuthResult::Valid(TokenKind::Admin);
    }

    // Invalid token (rule 5)
    AuthResult::Invalid
}

/// Check if a path requires authentication.
pub fn requires_auth(path: &str) -> bool {
    // Public endpoints (rule 4)
    if path == "/health" || path == "/version" {
        return false;
    }

    true
}

/// Check if a path requires admin access.
pub fn requires_admin(path: &str) -> bool {
    // Admin endpoints (rule 3)
    if path.starts_with("/admin/") || path.starts_with("/_miroir/") {
        return true;
    }

    // /metrics endpoint requires admin
    if path == "/metrics" {
        return true;
    }

    false
}

/// Authentication middleware.
///
/// Enforces bearer token validation per plan §5 rules 2-5.
pub async fn auth_middleware(
    State(state): State<ProxyState>,
    req: axum::extract::Request,
    next: Next,
) -> Result<Response, ErrorResponse> {
    let path = req.uri().path();

    // Check if authentication is required
    if !requires_auth(path) {
        return Ok(next.run(req).await);
    }

    // Validate bearer token
    let auth_result = validate_bearer_token(req.headers(), &state);

    match auth_result {
        AuthResult::Valid(TokenKind::Master) => {
            // Master key has full access (rule 2)
            Ok(next.run(req).await)
        }
        AuthResult::Valid(TokenKind::Admin) => {
            // Admin key can only access admin endpoints (rule 3)
            if requires_admin(path) {
                Ok(next.run(req).await)
            } else {
                Err(ErrorResponse::new(
                    "Admin key cannot access this endpoint. Use master key.",
                    "invalid_api_key",
                ))
            }
        }
        AuthResult::None => {
            // No bearer token → 401 (rule 4)
            Err(ErrorResponse::new(
                "Missing Authorization header",
                "missing_authorization_header",
            ))
        }
        AuthResult::Invalid => {
            // Invalid token → 403 (rule 5)
            Err(ErrorResponse::new(
                "Invalid API key",
                "invalid_api_key",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miroir_core::config::MiroirConfig;
    use miroir_core::topology::{Node, NodeId};

    fn test_state() -> ProxyState {
        let mut config = MiroirConfig::default();
        config.master_key = "test-master-key".to_string();
        config.admin.api_key = "test-admin-key".to_string();
        config.nodes = vec![];

        let mut topology = miroir_core::topology::Topology::new(1, 1);
        topology.add_node(Node::new(
            NodeId::new("test-node".to_string()),
            "http://localhost:7700".to_string(),
            0,
        ));

        ProxyState {
            config: std::sync::Arc::new(config),
            topology: std::sync::Arc::new(tokio::sync::RwLock::new(topology)),
            client: std::sync::Arc::new(crate::client::NodeClient::new(
                "node-key".to_string(),
                &Default::default(),
            )),
            query_seq: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            master_key: std::sync::Arc::new("test-master-key".to_string()),
            admin_key: std::sync::Arc::new("test-admin-key".to_string()),
            metrics: std::sync::Arc::new(crate::middleware::Metrics::new()),
            task_manager: std::sync::Arc::new(crate::task_manager::TaskManager::new()),
            retry_cache: std::sync::Arc::new(crate::retry_cache::RetryCache::new(
                std::time::Duration::from_secs(60),
            )),
        }
    }

    fn make_headers(token: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(t) = token {
            headers.insert("authorization", format!("Bearer {t}").parse().unwrap());
        }
        headers
    }

    #[test]
    fn test_validate_master_key() {
        let state = test_state();
        let headers = make_headers(Some("test-master-key"));

        let result = validate_bearer_token(&headers, &state);
        assert_eq!(result, AuthResult::Valid(TokenKind::Master));
    }

    #[test]
    fn test_validate_admin_key() {
        let state = test_state();
        let headers = make_headers(Some("test-admin-key"));

        let result = validate_bearer_token(&headers, &state);
        assert_eq!(result, AuthResult::Valid(TokenKind::Admin));
    }

    #[test]
    fn test_validate_invalid_key() {
        let state = test_state();
        let headers = make_headers(Some("wrong-key"));

        let result = validate_bearer_token(&headers, &state);
        assert_eq!(result, AuthResult::Invalid);
    }

    #[test]
    fn test_validate_no_token() {
        let state = test_state();
        let headers = make_headers(None);

        let result = validate_bearer_token(&headers, &state);
        assert_eq!(result, AuthResult::None);
    }

    #[test]
    fn test_requires_auth() {
        assert!(!requires_auth("/health"));
        assert!(!requires_auth("/version"));
        assert!(requires_auth("/indexes"));
        assert!(requires_auth("/search"));
        assert!(requires_auth("/admin/stats"));
    }

    #[test]
    fn test_requires_admin() {
        assert!(requires_admin("/admin/stats"));
        assert!(requires_admin("/_miroir/topology"));
        assert!(requires_admin("/metrics"));
        assert!(!requires_admin("/indexes"));
        assert!(!requires_admin("/search"));
    }
}
