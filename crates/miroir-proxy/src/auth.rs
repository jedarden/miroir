//! Bearer-token dispatch per plan §5 rules 0–5.
//!
//! Three token types can appear on `Authorization: Bearer <value>` simultaneously:
//! the `master_key`, the `admin_key`, and a search UI JWT. Miroir resolves them
//! deterministically in the order specified by §5.

use axum::{
    extract::{Request, State},
    http::{HeaderMap, Method},
    middleware::Next,
    response::{IntoResponse, Response},
};
use miroir_core::{MeilisearchError, MiroirCode};
use subtle::ConstantTimeEq;

// ---------------------------------------------------------------------------
// Auth state (shared via axum State)
// ---------------------------------------------------------------------------

/// Configuration needed by the bearer-token dispatch chain.
#[derive(Clone, Debug)]
pub struct AuthState {
    pub master_key: String,
    pub admin_key: String,
}

// ---------------------------------------------------------------------------
// Dispatch verdict
// ---------------------------------------------------------------------------

/// Result of the bearer-token dispatch chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthVerdict {
    /// Request is dispatch-exempt (rule 0); handler decides auth.
    Exempt,
    /// Authenticated with the given token kind.
    Authenticated(TokenKind),
    /// Bearer token looked like a JWT but failed validation (rule 1).
    JwtInvalid,
    /// JWT was signature-valid but scope was insufficient (rule 1).
    JwtScopeDenied,
    /// No matching key / missing Authorization (rule 4).
    InvalidAuth,
}

/// Which key or token type satisfied authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenKind {
    MasterKey,
    AdminKey,
    /// Phase 5 §13.21 will flesh this out.
    Jwt,
}

impl AuthVerdict {
    pub fn is_allowed(&self) -> bool {
        matches!(self, AuthVerdict::Exempt | AuthVerdict::Authenticated(_))
    }
}

// ---------------------------------------------------------------------------
// Rule 0 — dispatch-exempt check
// ---------------------------------------------------------------------------

/// Returns true when `(method, path)` is in the exhaustive dispatch-exempt list
/// (plan §5 rule 5). Exempt endpoints run their handler directly; rules 1–4
/// are never consulted.
pub fn is_dispatch_exempt(method: &Method, path: &str) -> bool {
    // `GET /_miroir/metrics` — admin-key-optional
    if method == Method::GET && path == "/_miroir/metrics" {
        return true;
    }

    // `GET /_miroir/ui/search/locale/*` — unauthenticated public locale fetch
    if method == Method::GET {
        if let Some(rest) = path.strip_prefix("/_miroir/ui/search/locale/") {
            // Must have at least one path segment after the prefix
            return !rest.is_empty() && !rest.contains("//");
        }
    }

    // `POST /_miroir/admin/login` — credentials in body
    if method == Method::POST && path == "/_miroir/admin/login" {
        return true;
    }

    // `GET /_miroir/ui/search/{index}/session` — auth per search_ui.auth.mode
    if method == Method::GET {
        if let Some(rest) = path.strip_prefix("/_miroir/ui/search/") {
            let segments: Vec<&str> = rest.split('/').collect();
            if segments.len() == 2 && segments[1] == "session" && !segments[0].is_empty() {
                return true;
            }
        }
    }

    // `GET /ui/search/{index}` — public SPA entry point
    if method == Method::GET {
        if let Some(rest) = path.strip_prefix("/ui/search/") {
            // Single non-empty segment (the index name)
            return !rest.is_empty() && !rest.contains('/');
        }
    }

    false
}

// ---------------------------------------------------------------------------
// Rule 1 — JWT-shape probe (Phase 2 stub)
// ---------------------------------------------------------------------------

/// Returns true if `token` has the structural shape of a JWT (three
/// dot-separated base64url segments). Phase 5 §13.21 will add full
/// signature / claim validation; Phase 2 just needs the shape probe
/// to distinguish JWTs from opaque tokens.
pub fn probe_jwt_shape(token: &str) -> bool {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return false;
    }
    // Each segment should be non-empty and look like base64url
    parts.iter().all(|s| {
        !s.is_empty()
            && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '=')
    })
}

// ---------------------------------------------------------------------------
// Helper — admin path check
// ---------------------------------------------------------------------------

/// Returns true if `path` starts with `/_miroir/` (admin surface).
pub fn is_admin_path(path: &str) -> bool {
    path.starts_with("/_miroir/")
}

// ---------------------------------------------------------------------------
// Constant-time opaque key comparison
// ---------------------------------------------------------------------------

/// Constant-time comparison of an opaque token against an expected key.
/// Prevents timing side-channels on secret key values.
pub fn constant_time_compare(token: &[u8], expected: &[u8]) -> bool {
    token.ct_eq(expected).into()
}

// ---------------------------------------------------------------------------
// Core dispatch — rules 0–4
// ---------------------------------------------------------------------------

/// Execute the full bearer-token dispatch chain for a request.
///
/// `bearer_token` is the raw value after stripping `"Bearer "` from the
/// `Authorization` header (may be `None` if the header is absent).
pub fn dispatch_bearer(
    method: &Method,
    path: &str,
    bearer_token: Option<&str>,
    state: &AuthState,
) -> AuthVerdict {
    // Rule 0 — dispatch-exempt endpoints skip all auth checks
    if is_dispatch_exempt(method, path) {
        return AuthVerdict::Exempt;
    }

    let token = match bearer_token {
        Some(t) => t,
        None => return AuthVerdict::InvalidAuth, // Rule 4 — missing auth
    };

    // Rule 1 — JWT-shape probe
    if probe_jwt_shape(token) {
        // Phase 2 stub: treat as "not-yet-implemented" JWT.
        // Phase 5 §13.21 will add signature validation, exp/nbf, kid, idx, scope.
        // For now, any parseable-but-unsupported JWT returns JwtInvalid.
        return AuthVerdict::JwtInvalid;
    }

    // Rule 2 — admin-path opaque-token match
    if is_admin_path(path) {
        if constant_time_compare(token.as_bytes(), state.admin_key.as_bytes()) {
            return AuthVerdict::Authenticated(TokenKind::AdminKey);
        }
        return AuthVerdict::InvalidAuth; // Rule 4
    }

    // Rule 3 — master-key match (non-admin paths)
    if constant_time_compare(token.as_bytes(), state.master_key.as_bytes()) {
        return AuthVerdict::Authenticated(TokenKind::MasterKey);
    }

    // Rule 4 — mismatch
    AuthVerdict::InvalidAuth
}

// ---------------------------------------------------------------------------
// X-Admin-Key short-circuit
// ---------------------------------------------------------------------------

/// Check the `X-Admin-Key` header for admin endpoints.
/// Returns `true` if the header is present and matches `admin_key`.
/// Evaluated independently of the bearer chain — short-circuits directly.
pub fn check_x_admin_key(headers: &HeaderMap, admin_key: &[u8]) -> bool {
    match headers.get("X-Admin-Key").and_then(|v| v.to_str().ok()) {
        Some(key) => constant_time_compare(key.as_bytes(), admin_key),
        None => false,
    }
}

// ---------------------------------------------------------------------------
// Axum middleware
// ---------------------------------------------------------------------------

/// Extract the bearer token from `Authorization: Bearer <value>`.
fn extract_bearer(headers: &HeaderMap) -> Option<&str> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    auth.strip_prefix("Bearer ")
}

/// Axum middleware implementing the bearer-token dispatch chain (plan §5).
pub async fn auth_middleware(
    State(state): State<AuthState>,
    req: Request,
    next: Next,
) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    // Rule 0 — dispatch-exempt: skip everything, handler decides auth
    if is_dispatch_exempt(&method, &path) {
        return next.run(req).await;
    }

    // X-Admin-Key short-circuit for admin endpoints
    if is_admin_path(&path) && check_x_admin_key(req.headers(), state.admin_key.as_bytes()) {
        return next.run(req).await;
    }

    // Extract bearer token
    let bearer = extract_bearer(req.headers());

    // Run the dispatch chain
    let verdict = dispatch_bearer(&method, &path, bearer, &state);

    match verdict {
        AuthVerdict::Authenticated(_) | AuthVerdict::Exempt => next.run(req).await,
        AuthVerdict::JwtInvalid => MeilisearchError::new(
            MiroirCode::JwtInvalid,
            "The provided JWT is invalid or expired.",
        )
        .into_response(),
        AuthVerdict::JwtScopeDenied => MeilisearchError::new(
            MiroirCode::JwtScopeDenied,
            "The provided JWT does not grant access to this resource.",
        )
        .into_response(),
        AuthVerdict::InvalidAuth => MeilisearchError::new(
            MiroirCode::InvalidAuth,
            "The provided authorization is invalid.",
        )
        .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Rate-limit hook types (Phase 2 in-memory stub, Phase 6 multi-pod)
// ---------------------------------------------------------------------------

/// Rate-limit bucket key types wired into the dispatch chain.
/// Phase 2 keeps these as in-memory counters; Phase 6 will back them
/// with the task store (Redis/SQLite).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RateLimitBucket {
    /// `miroir:ratelimit:adminlogin:<ip>`
    AdminLogin(String),
    /// `miroir:ratelimit:searchui:<ip>`
    SearchUi(String),
}

/// In-memory rate limiter (Phase 2 stub). Always returns `Ok(())` — actual
/// enforcement is deferred to Phase 6 multi-pod. The hook is wired here so
/// handlers can call `limiter.check()` without cfg-gating.
#[derive(Debug, Clone, Default)]
pub struct RateLimiter;

impl RateLimiter {
    pub fn check(&self, _bucket: &RateLimitBucket) -> Result<(), ()> {
        Ok(()) // Phase 2: always allow
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state() -> AuthState {
        AuthState {
            master_key: "master-key-123".to_string(),
            admin_key: "admin-key-456".to_string(),
        }
    }

    // -----------------------------------------------------------------------
    // Rule 0 — dispatch-exempt tests
    // -----------------------------------------------------------------------

    #[test]
    fn exempt_get_metrics() {
        assert!(is_dispatch_exempt(&Method::GET, "/_miroir/metrics"));
    }

    #[test]
    fn exempt_post_metrics_not_exempt() {
        assert!(!is_dispatch_exempt(&Method::POST, "/_miroir/metrics"));
    }

    #[test]
    fn exempt_get_locale_star() {
        assert!(is_dispatch_exempt(&Method::GET, "/_miroir/ui/search/locale/en-US"));
        assert!(is_dispatch_exempt(&Method::GET, "/_miroir/ui/search/locale/fr"));
    }

    #[test]
    fn exempt_get_locale_no_variant_not_exempt() {
        assert!(!is_dispatch_exempt(&Method::GET, "/_miroir/ui/search/locale/"));
    }

    #[test]
    fn exempt_post_admin_login() {
        assert!(is_dispatch_exempt(&Method::POST, "/_miroir/admin/login"));
    }

    #[test]
    fn exempt_get_admin_login_not_exempt() {
        assert!(!is_dispatch_exempt(&Method::GET, "/_miroir/admin/login"));
    }

    #[test]
    fn exempt_get_session() {
        assert!(is_dispatch_exempt(&Method::GET, "/_miroir/ui/search/products/session"));
    }

    #[test]
    fn exempt_get_session_no_index_not_exempt() {
        assert!(!is_dispatch_exempt(&Method::GET, "/_miroir/ui/search//session"));
    }

    #[test]
    fn exempt_get_search_ui_spa() {
        assert!(is_dispatch_exempt(&Method::GET, "/ui/search/products"));
    }

    #[test]
    fn exempt_get_search_ui_no_index_not_exempt() {
        assert!(!is_dispatch_exempt(&Method::GET, "/ui/search/"));
    }

    #[test]
    fn exempt_post_search_ui_not_exempt() {
        assert!(!is_dispatch_exempt(&Method::POST, "/ui/search/products"));
    }

    #[test]
    fn exempt_non_matching_path_not_exempt() {
        assert!(!is_dispatch_exempt(&Method::GET, "/indexes/products"));
        assert!(!is_dispatch_exempt(&Method::POST, "/indexes"));
        assert!(!is_dispatch_exempt(&Method::GET, "/_miroir/other"));
    }

    // -----------------------------------------------------------------------
    // Rule 0 — exempt endpoints skip auth entirely
    // -----------------------------------------------------------------------

    #[test]
    fn exempt_endpoint_ignores_master_key() {
        let state = test_state();
        // Even with correct master_key, exempt endpoint returns Exempt
        let verdict = dispatch_bearer(
            &Method::GET,
            "/_miroir/metrics",
            Some("master-key-123"),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::Exempt);
    }

    #[test]
    fn exempt_endpoint_ignores_admin_key() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::POST,
            "/_miroir/admin/login",
            Some("admin-key-456"),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::Exempt);
    }

    #[test]
    fn exempt_endpoint_ignores_wrong_key() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::GET,
            "/_miroir/metrics",
            Some("wrong-key"),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::Exempt);
    }

    #[test]
    fn exempt_endpoint_ignores_missing_auth() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::GET,
            "/_miroir/metrics",
            None,
            &state,
        );
        assert_eq!(verdict, AuthVerdict::Exempt);
    }

    #[test]
    fn exempt_locale_ignores_all_tokens() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::GET,
            "/_miroir/ui/search/locale/en-US",
            Some("master-key-123"),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::Exempt);
    }

    #[test]
    fn exempt_session_ignores_all_tokens() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::GET,
            "/_miroir/ui/search/products/session",
            Some("admin-key-456"),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::Exempt);
    }

    #[test]
    fn exempt_spa_ignores_all_tokens() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::GET,
            "/ui/search/products",
            None,
            &state,
        );
        assert_eq!(verdict, AuthVerdict::Exempt);
    }

    // -----------------------------------------------------------------------
    // Rule 1 — JWT-shape probe
    // -----------------------------------------------------------------------

    #[test]
    fn jwt_shape_probe_accepts_valid_shape() {
        assert!(probe_jwt_shape("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc123"));
    }

    #[test]
    fn jwt_shape_probe_rejects_two_parts() {
        assert!(!probe_jwt_shape("part1.part2"));
    }

    #[test]
    fn jwt_shape_probe_rejects_four_parts() {
        assert!(!probe_jwt_shape("a.b.c.d"));
    }

    #[test]
    fn jwt_shape_probe_rejects_empty() {
        assert!(!probe_jwt_shape(""));
    }

    #[test]
    fn jwt_shape_probe_rejects_opaque_token() {
        assert!(!probe_jwt_shape("admin-key-456"));
    }

    #[test]
    fn jwt_on_non_admin_path_returns_jwt_invalid() {
        let state = test_state();
        let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc123";
        let verdict = dispatch_bearer(
            &Method::GET,
            "/indexes/products",
            Some(jwt),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::JwtInvalid);
    }

    #[test]
    fn jwt_on_admin_path_returns_jwt_invalid() {
        let state = test_state();
        let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc123";
        let verdict = dispatch_bearer(
            &Method::GET,
            "/_miroir/some/admin/endpoint",
            Some(jwt),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::JwtInvalid);
    }

    // -----------------------------------------------------------------------
    // Rule 2 — admin-path opaque-token match (admin_key only)
    // -----------------------------------------------------------------------

    #[test]
    fn admin_path_matches_admin_key() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::GET,
            "/_miroir/some/endpoint",
            Some("admin-key-456"),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::Authenticated(TokenKind::AdminKey));
    }

    #[test]
    fn admin_path_rejects_master_key() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::GET,
            "/_miroir/some/endpoint",
            Some("master-key-123"),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::InvalidAuth);
    }

    #[test]
    fn admin_path_rejects_wrong_key() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::GET,
            "/_miroir/some/endpoint",
            Some("wrong-key"),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::InvalidAuth);
    }

    #[test]
    fn admin_path_rejects_missing_auth() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::GET,
            "/_miroir/some/endpoint",
            None,
            &state,
        );
        assert_eq!(verdict, AuthVerdict::InvalidAuth);
    }

    // -----------------------------------------------------------------------
    // Rule 3 — master-key match (non-admin paths only)
    // -----------------------------------------------------------------------

    #[test]
    fn non_admin_path_matches_master_key() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::POST,
            "/indexes/products/documents",
            Some("master-key-123"),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::Authenticated(TokenKind::MasterKey));
    }

    #[test]
    fn non_admin_path_rejects_admin_key() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::POST,
            "/indexes/products/documents",
            Some("admin-key-456"),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::InvalidAuth);
    }

    #[test]
    fn non_admin_path_rejects_wrong_key() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::POST,
            "/indexes/products/documents",
            Some("wrong-key"),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::InvalidAuth);
    }

    #[test]
    fn non_admin_path_rejects_missing_auth() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::POST,
            "/indexes/products/documents",
            None,
            &state,
        );
        assert_eq!(verdict, AuthVerdict::InvalidAuth);
    }

    // -----------------------------------------------------------------------
    // Rule 4 — missing auth → 401 miroir_invalid_auth
    // -----------------------------------------------------------------------

    #[test]
    fn missing_auth_on_gated_endpoint_returns_invalid_auth() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::POST,
            "/indexes",
            None,
            &state,
        );
        assert_eq!(verdict, AuthVerdict::InvalidAuth);
    }

    // -----------------------------------------------------------------------
    // X-Admin-Key short-circuit
    // -----------------------------------------------------------------------

    #[test]
    fn x_admin_key_matches_admin_key() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Admin-Key", "admin-key-456".parse().unwrap());
        assert!(check_x_admin_key(&headers, b"admin-key-456"));
    }

    #[test]
    fn x_admin_key_rejects_wrong_key() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Admin-Key", "wrong-key".parse().unwrap());
        assert!(!check_x_admin_key(&headers, b"admin-key-456"));
    }

    #[test]
    fn x_admin_key_missing_header() {
        let headers = HeaderMap::new();
        assert!(!check_x_admin_key(&headers, b"admin-key-456"));
    }

    // -----------------------------------------------------------------------
    // Constant-time comparison
    // -----------------------------------------------------------------------

    #[test]
    fn constant_time_eq_matching() {
        assert!(constant_time_compare(b"hello", b"hello"));
    }

    #[test]
    fn constant_time_eq_not_matching() {
        assert!(!constant_time_compare(b"hello", b"world"));
    }

    #[test]
    fn constant_time_eq_different_lengths() {
        assert!(!constant_time_compare(b"short", b"much-longer-value"));
    }

    #[test]
    fn constant_time_eq_empty() {
        assert!(constant_time_compare(b"", b""));
    }

    /// Timing-injection harness: verify no measurable delta between
    /// "all bytes wrong" and "one byte wrong" comparisons at the same length.
    /// Uses many iterations to detect statistical differences; constant-time
    /// ops should show negligible difference.
    #[test]
    fn constant_time_no_timing_leak() {
        use std::time::Instant;

        let expected = b"admin-key-456";
        let all_wrong = b"xxxxxxxxxxxxx"; // same length, all bytes wrong
        let one_wrong = b"admin-key-457"; // same length, one byte different

        let iterations = 100_000u64;

        let start = Instant::now();
        for _ in 0..iterations {
            let _ = constant_time_compare(all_wrong, expected);
        }
        let all_wrong_duration = start.elapsed();

        let start = Instant::now();
        for _ in 0..iterations {
            let _ = constant_time_compare(one_wrong, expected);
        }
        let one_wrong_duration = start.elapsed();

        // The ratio should be close to 1.0 for constant-time comparison.
        // We allow 2x tolerance to account for system noise but anything
        // significantly different would indicate a timing leak.
        let ratio = all_wrong_duration.as_secs_f64() / one_wrong_duration.as_secs_f64();
        assert!(
            ratio > 0.5 && ratio < 2.0,
            "Timing ratio {} suggests non-constant-time comparison: all_wrong={:?}, one_wrong={:?}",
            ratio,
            all_wrong_duration,
            one_wrong_duration,
        );
    }

    // -----------------------------------------------------------------------
    // Bearer extraction
    // -----------------------------------------------------------------------

    #[test]
    fn extract_bearer_valid() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer my-token".parse().unwrap());
        assert_eq!(extract_bearer(&headers), Some("my-token"));
    }

    #[test]
    fn extract_bearer_missing_header() {
        let headers = HeaderMap::new();
        assert_eq!(extract_bearer(&headers), None);
    }

    #[test]
    fn extract_bearer_wrong_scheme() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Basic dXNlcjpwYXNz".parse().unwrap());
        assert_eq!(extract_bearer(&headers), None);
    }

    // -----------------------------------------------------------------------
    // Admin path detection
    // -----------------------------------------------------------------------

    #[test]
    fn admin_path_detected() {
        assert!(is_admin_path("/_miroir/metrics"));
        assert!(is_admin_path("/_miroir/admin/login"));
        assert!(is_admin_path("/_miroir/ui/search/locale/en"));
    }

    #[test]
    fn non_admin_path_not_detected() {
        assert!(!is_admin_path("/indexes/products"));
        assert!(!is_admin_path("/search/products"));
        assert!(!is_admin_path("/health"));
    }

    // -----------------------------------------------------------------------
    // Rate limiter stub
    // -----------------------------------------------------------------------

    #[test]
    fn rate_limiter_always_allows() {
        let limiter = RateLimiter;
        assert!(limiter.check(&RateLimitBucket::AdminLogin("127.0.0.1".into())).is_ok());
        assert!(limiter.check(&RateLimitBucket::SearchUi("10.0.0.1".into())).is_ok());
    }

    // -----------------------------------------------------------------------
    // AuthVerdict helpers
    // -----------------------------------------------------------------------

    #[test]
    fn verdict_is_allowed() {
        assert!(AuthVerdict::Exempt.is_allowed());
        assert!(AuthVerdict::Authenticated(TokenKind::MasterKey).is_allowed());
        assert!(AuthVerdict::Authenticated(TokenKind::AdminKey).is_allowed());
        assert!(!AuthVerdict::JwtInvalid.is_allowed());
        assert!(!AuthVerdict::JwtScopeDenied.is_allowed());
        assert!(!AuthVerdict::InvalidAuth.is_allowed());
    }

    // -----------------------------------------------------------------------
    // Integration-style: all exempt endpoints have test coverage
    // -----------------------------------------------------------------------

    #[test]
    fn all_rule5_exempt_endpoints_covered() {
        // Every row in plan §5 rule 5 exempt list tested for dispatch exemption
        let cases = vec![
            (Method::GET, "/_miroir/metrics"),
            (Method::GET, "/_miroir/ui/search/locale/en-US"),
            (Method::GET, "/_miroir/ui/search/locale/fr"),
            (Method::POST, "/_miroir/admin/login"),
            (Method::GET, "/_miroir/ui/search/products/session"),
            (Method::GET, "/_miroir/ui/search/users/session"),
            (Method::GET, "/ui/search/products"),
            (Method::GET, "/ui/search/users"),
        ];
        for (method, path) in cases {
            assert!(
                is_dispatch_exempt(&method, path),
                "Expected ({}, {}) to be dispatch-exempt",
                method,
                path,
            );
        }
    }
}
