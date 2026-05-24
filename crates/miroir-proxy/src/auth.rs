//! Bearer-token dispatch per plan §5 rules 0–5.
//!
//! Three token types can appear on `Authorization: Bearer <value>` simultaneously:
//! the `master_key`, the `admin_key`, and a search UI JWT. Miroir resolves them
//! deterministically in the order specified by §5.
//!
//! JWT signing-secret rotation (plan §9):
//! - Primary secret (`SEARCH_UI_JWT_SECRET`) signs new tokens; `kid` header identifies it.
//! - Optional previous secret (`SEARCH_UI_JWT_SECRET_PREVIOUS`) is present only during
//!   the rotation overlap window. Validation accepts either secret.

use axum::{
    extract::{FromRef, Request, State},
    http::{HeaderMap, Method},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use miroir_core::{task_store::TaskStore, MeilisearchError, MiroirCode};
use prometheus::Counter;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;
use subtle::ConstantTimeEq;

use crate::admin_session::{self, SealKey};

type HmacSha256 = Hmac<Sha256>;

/// Extension carried in the request after successful cookie unseal.
/// Handlers extract this to look up the session in the task store.
#[derive(Debug, Clone)]
pub struct AdminSessionId(pub String);

/// State for CSRF middleware, combining AuthState with task store access.
#[derive(Clone)]
pub struct CsrfState {
    pub auth: AuthState,
    pub redis_store: Option<miroir_core::task_store::RedisTaskStore>,
}

// ---------------------------------------------------------------------------
// JWT claims (plan §13.21)
// ---------------------------------------------------------------------------

/// Claims embedded in a search UI JWT session token (plan §13.21).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Issuer — always "miroir".
    pub iss: String,
    /// Subject — "search-ui-session" or user identifier in oauth_proxy mode.
    pub sub: String,
    /// Index this token grants access to.
    pub idx: String,
    /// Granted scope — array of allowed action names.
    pub scope: Vec<String>,
    /// Issued-at timestamp (seconds since epoch).
    pub iat: u64,
    /// Expiration timestamp (seconds since epoch).
    pub exp: u64,
}

/// Key ID embedded in the JWT header to identify which secret signed it.
const KID_PRIMARY: &str = "primary";
const KID_PREVIOUS: &str = "previous";

/// JWT header (always HS256).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JwtHeader {
    alg: String,
    kid: String,
    typ: String,
}

// ---------------------------------------------------------------------------
// Minimal HS256 JWT encode / decode (no external JWT crate needed)
// ---------------------------------------------------------------------------

/// Encode and sign a JWT with the given secret.
fn jwt_encode(header: &JwtHeader, claims: &JwtClaims, secret: &[u8]) -> Result<String, String> {
    let header_json = serde_json::to_string(header).map_err(|e| e.to_string())?;
    let claims_json = serde_json::to_string(claims).map_err(|e| e.to_string())?;

    let header_b64 = URL_SAFE_NO_PAD.encode(header_json.as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(claims_json.as_bytes());

    let signing_input = format!("{}.{}", header_b64, payload_b64);

    let mut mac = HmacSha256::new_from_slice(secret).map_err(|e| format!("HMAC init: {}", e))?;
    mac.update(signing_input.as_bytes());
    let sig = mac.finalize().into_bytes();
    let sig_b64 = URL_SAFE_NO_PAD.encode(sig);

    Ok(format!("{}.{}.{}", header_b64, payload_b64, sig_b64))
}

/// Decode and verify a JWT with the given secret. Returns (header, claims).
fn jwt_decode(token: &str, secret: &[u8]) -> Result<(JwtHeader, JwtClaims), JwtValidationError> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(JwtValidationError::Malformed);
    }

    let header_bytes = URL_SAFE_NO_PAD
        .decode(parts[0])
        .map_err(|_| JwtValidationError::Malformed)?;
    let header: JwtHeader =
        serde_json::from_slice(&header_bytes).map_err(|_| JwtValidationError::Malformed)?;

    if header.alg != "HS256" {
        return Err(JwtValidationError::Malformed);
    }

    // Verify signature
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let mut mac = HmacSha256::new_from_slice(secret).unwrap();
    mac.update(signing_input.as_bytes());
    let expected_sig = mac.finalize().into_bytes();

    let actual_sig = URL_SAFE_NO_PAD
        .decode(parts[2])
        .map_err(|_| JwtValidationError::InvalidSignature)?;

    use subtle::ConstantTimeEq as _;
    let sig_valid: bool = actual_sig.ct_eq(&expected_sig).into();
    if !sig_valid {
        return Err(JwtValidationError::InvalidSignature);
    }

    // Decode claims
    let claims_bytes = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|_| JwtValidationError::Malformed)?;
    let claims: JwtClaims =
        serde_json::from_slice(&claims_bytes).map_err(|_| JwtValidationError::Malformed)?;

    // Check expiration with 30s leeway
    let now = epoch_seconds();
    if claims.exp + 30 < now {
        return Err(JwtValidationError::Expired);
    }

    Ok((header, claims))
}

// ---------------------------------------------------------------------------
// Auth state (shared via axum State)
// ---------------------------------------------------------------------------

/// Configuration needed by the bearer-token dispatch chain.
#[derive(Clone)]
pub struct AuthState {
    pub master_key: String,
    pub admin_key: String,
    /// HMAC secret for signing/validating search UI JWTs (primary).
    pub jwt_primary: Option<String>,
    /// Optional previous secret active during rotation overlap window.
    pub jwt_previous: Option<String>,
    /// Key for sealing/unsealing admin session cookies (XChaCha20-Poly1305).
    pub seal_key: SealKey,
    /// In-memory set of revoked admin session IDs (populated on logout, Pub/Sub).
    pub revoked_sessions: Arc<DashMap<String, ()>>,
    /// Counter for revoked admin sessions (miroir_admin_session_revoked_total).
    pub admin_session_revoked_total: Counter,
}

impl std::fmt::Debug for AuthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthState")
            .field("master_key", &"[redacted]")
            .field("admin_key", &"[redacted]")
            .field("jwt_primary", &self.jwt_primary.as_ref().map(|_| "[set]"))
            .field("jwt_previous", &self.jwt_previous.as_ref().map(|_| "[set]"))
            .field("seal_key", &self.seal_key)
            .field("revoked_sessions", &self.revoked_sessions.len())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// JWT signing / validation helpers
// ---------------------------------------------------------------------------

impl AuthState {
    /// Create a new signed JWT session token for the given index (plan §13.21).
    /// Always signs with the primary secret; `kid` header identifies it.
    /// Scope defaults to ["search", "multi_search", "beacon"] for search UI sessions.
    pub fn sign_jwt(&self, sub: &str, idx: &str, scope: &[&str], ttl_s: u64) -> Option<String> {
        let secret = self.jwt_primary.as_ref()?;
        let now = epoch_seconds();
        let claims = JwtClaims {
            iss: "miroir".to_string(),
            sub: sub.to_string(),
            idx: idx.to_string(),
            scope: scope.iter().map(|s| s.to_string()).collect(),
            iat: now,
            exp: now + ttl_s,
        };
        let header = JwtHeader {
            alg: "HS256".to_string(),
            kid: KID_PRIMARY.to_string(),
            typ: "JWT".to_string(),
        };
        jwt_encode(&header, &claims, secret.as_bytes()).ok()
    }

    /// Validate a JWT string against either the primary or previous secret.
    /// Returns the parsed claims if validation succeeds, or an error.
    pub fn validate_jwt(&self, token: &str) -> Result<JwtClaims, JwtValidationError> {
        // Try primary secret first
        if let Some(ref secret) = self.jwt_primary {
            match jwt_decode(token, secret.as_bytes()) {
                Ok((_header, claims)) => return Ok(claims),
                Err(JwtValidationError::Expired) => return Err(JwtValidationError::Expired),
                _ => {} // signature mismatch — try previous
            }
        }

        // Try previous secret (rotation overlap window)
        if let Some(ref secret) = self.jwt_previous {
            if secret.is_empty() {
                return Err(JwtValidationError::PreviousSecretEmpty);
            }
            match jwt_decode(token, secret.as_bytes()) {
                Ok((_header, claims)) => return Ok(claims),
                Err(JwtValidationError::Expired) => return Err(JwtValidationError::Expired),
                _ => {} // signature mismatch
            }
        }

        Err(JwtValidationError::InvalidSignature)
    }
}

/// Errors returned by JWT validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JwtValidationError {
    /// Token structure is invalid or alg is not HS256.
    Malformed,
    /// HMAC signature did not match any loaded secret.
    InvalidSignature,
    /// Token has expired.
    Expired,
    /// `SEARCH_UI_JWT_SECRET_PREVIOUS` is set to the empty string (leak response).
    PreviousSecretEmpty,
    /// Token scope does not permit this (method, path) or idx claim mismatch.
    ScopeDenied,
}

// ---------------------------------------------------------------------------
// CSRF token generation (plan §9)
// ---------------------------------------------------------------------------

/// Generate a cryptographically random CSRF token.
/// Returns a URL-safe base64-encoded 32-byte token.
pub fn generate_csrf_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(&bytes)
}

/// Extract the CSRF token from the `X-CSRF-Token` header.
pub fn extract_csrf_token(headers: &HeaderMap) -> Option<String> {
    headers.get("X-CSRF-Token")?.to_str().ok().map(String::from)
}

/// Constant-time comparison of CSRF tokens.
pub fn constant_time_csrf_compare(token: &str, expected: &str) -> bool {
    constant_time_compare(token.as_bytes(), expected.as_bytes())
}

/// Validate a CSRF token against the expected session token.
/// Returns Ok(()) if the token matches, or a CsrfMismatch error.
pub fn validate_csrf_token(provided: &str, expected: &str) -> Result<(), MiroirCode> {
    if constant_time_csrf_compare(provided, expected) {
        Ok(())
    } else {
        Err(MiroirCode::CsrfMismatch)
    }
}

// ---------------------------------------------------------------------------
// Origin validation (plan §9)
// ---------------------------------------------------------------------------

/// Result of origin validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OriginVerdict {
    /// Origin is allowed.
    Allowed,
    /// Origin is not in the allowed list.
    Forbidden,
    /// No Origin/Referer header present (for same-origin requests).
    Missing,
}

/// Validate the Origin header against the allowed origins list.
/// Handles special "same-origin" value by comparing against the Host header.
pub fn validate_origin(
    headers: &HeaderMap,
    allowed_origins: &[String],
    is_same_origin_by_default: bool,
) -> OriginVerdict {
    // Try Origin header first (preferred for POST/DELETE/PUT)
    let origin = headers.get("origin").and_then(|h| h.to_str().ok());

    // Fall back to Referer header (for navigational requests)
    let referer = origin.or_else(|| headers.get("referer").and_then(|h| h.to_str().ok()));

    let provided_origin = match referer {
        Some(o) => o,
        None => {
            // No Origin or Referer header - for same-origin requests, this is acceptable
            return if is_same_origin_by_default {
                OriginVerdict::Missing
            } else {
                OriginVerdict::Forbidden
            };
        }
    };

    // Strip path from Referer to get origin
    let provided_origin = if let Some(ref_hdr) = headers.get("referer") {
        if let Ok(ref_val) = ref_hdr.to_str() {
            // Find the first '/' after "https://" (skip the first 8 chars: "https://")
            if let Some(idx) = ref_val
                .chars()
                .enumerate()
                .skip(8)
                .find(|(_, c)| *c == '/')
                .map(|(i, _)| i)
            {
                &ref_val[..idx]
            } else {
                ref_val
            }
        } else {
            provided_origin
        }
    } else {
        provided_origin
    };

    // Check against allowed origins
    for allowed in allowed_origins {
        // Special "same-origin" value - compare against Host header
        if allowed == "same-origin" {
            if let Some(host) = headers.get("host").and_then(|h| h.to_str().ok()) {
                // Construct origin from scheme (https) and host
                let same_origin = format!("https://{}", host);
                if provided_origin == same_origin || provided_origin == host {
                    return OriginVerdict::Allowed;
                }
            }
        } else if allowed == "*" {
            // Wildcard allows any origin
            return OriginVerdict::Allowed;
        } else if provided_origin == allowed {
            return OriginVerdict::Allowed;
        }
    }

    OriginVerdict::Forbidden
}

// ---------------------------------------------------------------------------
// CSP header builder (plan §9)
// ---------------------------------------------------------------------------

/// Build a CSP header value by merging base template with overrides.
/// Overrides are merged additively - they never replace the base template.
pub fn build_csp_header(
    base_template: &str,
    overrides: &miroir_core::config::CspOverridesConfig,
) -> String {
    let mut directives: Vec<(String, Vec<String>)> = base_template
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|directive| {
            let parts: Vec<&str> = directive.splitn(2, ' ').collect();
            if parts.len() == 2 {
                (parts[0].to_lowercase(), vec![parts[1].to_string()])
            } else {
                (parts[0].to_lowercase(), vec![])
            }
        })
        .collect();

    // Helper to merge overrides into a directive
    let merge_into =
        |directives: &mut Vec<(String, Vec<String>)>, name: &str, values: &[String]| {
            if values.is_empty() {
                return;
            }
            let name_lower = name.to_lowercase();
            if let Some(entry) = directives.iter_mut().find(|(n, _)| n == &name_lower) {
                // Append to existing directive
                entry.1.extend(values.iter().cloned());
                entry.1.dedup(); // Remove duplicates
            } else {
                // Add new directive
                directives.push((name_lower, values.to_vec()));
            }
        };

    // Merge each override category
    merge_into(&mut directives, "script-src", &overrides.script_src);
    merge_into(&mut directives, "img-src", &overrides.img_src);
    merge_into(&mut directives, "connect-src", &overrides.connect_src);

    // Rebuild CSP string
    directives
        .into_iter()
        .map(|(name, values)| {
            if values.is_empty() {
                name
            } else {
                format!("{} {}", name, values.join(" "))
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
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
    /// JWT validated against primary or previous secret.
    Jwt,
    /// Admin session cookie — sealed session ID validated against task store.
    AdminSession,
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
    // `GET /health` — unauthenticated liveness probe (Meilisearch-compatible)
    if method == Method::GET && path == "/health" {
        return true;
    }

    // `GET /version` — unauthenticated version endpoint (Meilisearch-compatible)
    if method == Method::GET && path == "/version" {
        return true;
    }

    // `GET /_miroir/ready` — unauthenticated readiness probe (plan §10)
    if method == Method::GET && path == "/_miroir/ready" {
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
// Rule 1 — JWT-shape probe
// ---------------------------------------------------------------------------

/// Returns true if `token` has the structural shape of a JWT (three
/// dot-separated base64url segments).
pub fn probe_jwt_shape(token: &str) -> bool {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return false;
    }
    // Each segment should be non-empty and look like base64url
    parts.iter().all(|s| {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '=')
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
// Scope and index validation (plan §13.21 defense-in-depth)
// ---------------------------------------------------------------------------

/// Action name for a given (method, path) combination per plan §13.21.
/// Returns the scope action name if the path is a search UI endpoint, None otherwise.
fn action_for_method_path(method: &Method, path: &str) -> Option<&'static str> {
    // POST /indexes/{idx}/search → "search"
    if method == Method::POST {
        if let Some(rest) = path.strip_prefix("/indexes/") {
            if let Some(idx_rest) = rest.strip_suffix("/search") {
                // Ensure the middle part is a valid index uid (non-empty, no slashes)
                if !idx_rest.is_empty() && !idx_rest.contains('/') {
                    return Some("search");
                }
            }
        }
    }

    // POST /multi-search → "multi_search"
    if method == Method::POST && path == "/multi-search" {
        return Some("multi_search");
    }

    // POST /_miroir/ui/search/{idx}/beacon → "beacon"
    if method == Method::POST {
        if let Some(rest) = path.strip_prefix("/_miroir/ui/search/") {
            if let Some(idx_rest) = rest.strip_suffix("/beacon") {
                if !idx_rest.is_empty() && !idx_rest.contains('/') {
                    return Some("beacon");
                }
            }
        }
    }

    None
}

/// Validate JWT scope and index claims against the request (plan §13.21).
/// Returns Ok(()) if the (method, path) is allowed by the scope and idx claim,
/// or Err(JwtScopeDenied) if the validation fails.
pub fn validate_jwt_scope(
    claims: &JwtClaims,
    method: &Method,
    path: &str,
) -> Result<(), JwtValidationError> {
    // Determine the required action for this (method, path)
    let Some(required_action) = action_for_method_path(method, path) else {
        // This endpoint doesn't require scope validation
        return Ok(());
    };

    // Check if the required action is in the scope
    if !claims.scope.contains(&required_action.to_string()) {
        return Err(JwtValidationError::ScopeDenied);
    }

    // For multi_search, we need to validate that every sub-query's indexUid matches idx
    // This is handled later in the request handler since we need to parse the body.
    // For search and beacon, validate the index in the path matches the claim.
    if required_action == "search" || required_action == "beacon" {
        let expected_idx = &claims.idx;
        let actual_idx = if required_action == "search" {
            // Extract index from /indexes/{idx}/search
            path.strip_prefix("/indexes/")
                .and_then(|rest| rest.strip_suffix("/search"))
        } else {
            // Extract index from /_miroir/ui/search/{idx}/beacon
            path.strip_prefix("/_miroir/ui/search/")
                .and_then(|rest| rest.strip_suffix("/beacon"))
        };

        if actual_idx != Some(expected_idx) {
            return Err(JwtValidationError::ScopeDenied);
        }
    }

    Ok(())
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

    // Rule 1 — JWT-shape probe, then full validation including scope check
    if probe_jwt_shape(token) {
        match state.validate_jwt(token) {
            Ok(claims) => {
                // Defense-in-depth: validate scope and index claims (plan §13.21)
                match validate_jwt_scope(&claims, method, path) {
                    Ok(()) => return AuthVerdict::Authenticated(TokenKind::Jwt),
                    Err(JwtValidationError::ScopeDenied) => {
                        return AuthVerdict::JwtScopeDenied;
                    }
                    Err(_) => return AuthVerdict::JwtInvalid,
                }
            }
            Err(JwtValidationError::PreviousSecretEmpty) => {
                return AuthVerdict::JwtInvalid;
            }
            Err(_) => {
                return AuthVerdict::JwtInvalid;
            }
        }
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

/// Extract the sealed admin session cookie value from the Cookie header.
pub fn extract_admin_session_cookie(headers: &HeaderMap) -> Option<String> {
    let cookie_header = headers.get("cookie")?.to_str().ok()?;
    for pair in cookie_header.split(';') {
        let pair = pair.trim();
        if let Some(value) = pair.strip_prefix(&format!("{}=", admin_session::COOKIE_NAME)) {
            return Some(value.to_string());
        }
    }
    None
}

/// Unseal an admin session cookie, returning the session ID.
pub fn unseal_admin_cookie(
    cookie_value: &str,
    key: &SealKey,
) -> Result<String, admin_session::SealError> {
    admin_session::unseal_session(cookie_value, key)
}

/// Axum middleware implementing the bearer-token dispatch chain (plan §5).
pub async fn auth_middleware(State(state): State<AuthState>, req: Request, next: Next) -> Response {
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

    // Admin session cookie check for admin endpoints (plan §9, §13.19).
    // If a sealed admin session cookie is present, unseal it and authenticate.
    // Revoked sessions are rejected immediately.
    if is_admin_path(&path) {
        if let Some(cookie_value) = extract_admin_session_cookie(req.headers()) {
            match unseal_admin_cookie(&cookie_value, &state.seal_key) {
                Ok(session_id) => {
                    // Check revocation cache (populated on logout + Pub/Sub).
                    if state.revoked_sessions.contains_key(&session_id) {
                        return MeilisearchError::new(
                            MiroirCode::InvalidAuth,
                            "Admin session has been revoked.",
                        )
                        .into_response();
                    }
                    let mut req = req;
                    req.extensions_mut().insert(AdminSessionId(session_id));
                    return next.run(req).await;
                }
                Err(e) => {
                    // Cookie tampering or wrong seal key (e.g. cross-pod key
                    // mismatch in HA).  Log a warning so operators can diagnose
                    // ADMIN_SESSION_SEAL_KEY divergence across pods.
                    tracing::warn!(
                        path = %path,
                        error = %e,
                        "admin session cookie unseal failed — tampered cookie or cross-pod key mismatch"
                    );
                }
            }
        }
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
// CSRF validation middleware (plan §9)
// ---------------------------------------------------------------------------

/// CSRF middleware that validates `X-CSRF-Token` on state-changing requests.
///
/// Bypasses CSRF check when:
/// - Request is authenticated via Bearer token (not admin session cookie)
/// - X-Admin-Key header is present
/// - Request method is safe (GET, HEAD, OPTIONS)
/// - Path is dispatch-exempt
///
/// For admin session cookie auth, requires `X-CSRF-Token` header to match
/// the token stored in the session.
pub async fn csrf_middleware(State(state): State<CsrfState>, req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    // Skip CSRF for safe methods
    if matches!(method, Method::GET | Method::HEAD | Method::OPTIONS) {
        return next.run(req).await;
    }

    // Skip CSRF for non-admin paths
    if !is_admin_path(&path) {
        return next.run(req).await;
    }

    // Skip CSRF for dispatch-exempt endpoints
    if is_dispatch_exempt(&method, &path) {
        return next.run(req).await;
    }

    // Skip CSRF if X-Admin-Key is present (bypasses CSRF)
    if check_x_admin_key(req.headers(), state.auth.admin_key.as_bytes()) {
        return next.run(req).await;
    }

    // Check if authenticated via admin session cookie
    let has_session_cookie = extract_admin_session_cookie(req.headers()).is_some();
    let has_bearer_token = extract_bearer(req.headers()).is_some();

    // CSRF only applies to session-cookie auth, not bearer tokens
    if !has_session_cookie || has_bearer_token {
        return next.run(req).await;
    }

    // Extract CSRF token from header
    let csrf_token = match extract_csrf_token(req.headers()) {
        Some(token) => token,
        None => {
            return MeilisearchError::new(
                MiroirCode::MissingCsrf,
                "CSRF token is required for state-changing requests.",
            )
            .into_response();
        }
    };

    // Get session ID from extensions (set by auth_middleware)
    let session_id = match req.extensions().get::<AdminSessionId>() {
        Some(id) => id.0.clone(),
        None => {
            // Session cookie was present but auth_middleware didn't set AdminSessionId
            // This means the session was invalid/expired/revoked
            return MeilisearchError::new(
                MiroirCode::InvalidAuth,
                "Admin session is invalid or expired.",
            )
            .into_response();
        }
    };

    // Validate CSRF token against session
    let Some(redis_store) = state.redis_store.as_ref() else {
        return MeilisearchError::new(
            MiroirCode::InvalidAuth,
            "Admin sessions require Redis task store.",
        )
        .into_response();
    };

    let session = match redis_store.get_admin_session(&session_id) {
        Ok(Some(s)) => s,
        Ok(None) => {
            return MeilisearchError::new(MiroirCode::InvalidAuth, "Admin session not found.")
                .into_response();
        }
        Err(e) => {
            tracing::warn!(error = %e, session_prefix = &session_id[..session_id.len().min(8)], "failed to get admin session for CSRF validation");
            return MeilisearchError::new(MiroirCode::InvalidAuth, "Failed to validate session.")
                .into_response();
        }
    };

    // Check if revoked
    if session.revoked {
        return MeilisearchError::new(MiroirCode::InvalidAuth, "Admin session has been revoked.")
            .into_response();
    }

    // Check expiration
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    if session.expires_at < now {
        return MeilisearchError::new(MiroirCode::InvalidAuth, "Admin session has expired.")
            .into_response();
    }

    // Constant-time compare CSRF tokens
    if !constant_time_csrf_compare(&csrf_token, &session.csrf_token) {
        return MeilisearchError::new(
            MiroirCode::CsrfMismatch,
            "CSRF token does not match the session token.",
        )
        .into_response();
    }

    next.run(req).await
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
// Helpers
// ---------------------------------------------------------------------------

fn epoch_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> SealKey {
        SealKey::from_bytes([42u8; 32])
    }

    fn test_state() -> AuthState {
        AuthState {
            master_key: "master-key-123".to_string(),
            admin_key: "admin-key-456".to_string(),
            jwt_primary: None,
            jwt_previous: None,
            seal_key: test_key(),
            revoked_sessions: Arc::new(DashMap::new()),
            admin_session_revoked_total: Counter::with_opts(prometheus::Opts::new(
                "test_revoked_total",
                "test",
            ))
            .unwrap(),
        }
    }

    fn test_state_with_jwt() -> AuthState {
        AuthState {
            master_key: "master-key-123".to_string(),
            admin_key: "admin-key-456".to_string(),
            jwt_primary: Some("test-secret-primary-key-32byte".to_string()),
            jwt_previous: None,
            seal_key: test_key(),
            revoked_sessions: Arc::new(DashMap::new()),
            admin_session_revoked_total: Counter::with_opts(prometheus::Opts::new(
                "test_revoked_total",
                "test",
            ))
            .unwrap(),
        }
    }

    fn test_state_with_dual_jwt() -> AuthState {
        AuthState {
            master_key: "master-key-123".to_string(),
            admin_key: "admin-key-456".to_string(),
            jwt_primary: Some("test-secret-primary-key-32byte".to_string()),
            jwt_previous: Some("test-secret-previous-key-32byte".to_string()),
            seal_key: test_key(),
            revoked_sessions: Arc::new(DashMap::new()),
            admin_session_revoked_total: Counter::with_opts(prometheus::Opts::new(
                "test_revoked_total",
                "test",
            ))
            .unwrap(),
        }
    }

    // -----------------------------------------------------------------------
    // Rule 0 — dispatch-exempt tests
    // -----------------------------------------------------------------------

    #[test]
    fn get_metrics_requires_admin_key() {
        assert!(!is_dispatch_exempt(&Method::GET, "/_miroir/metrics"));
        assert!(!is_dispatch_exempt(&Method::POST, "/_miroir/metrics"));
    }

    #[test]
    fn exempt_get_locale_star() {
        assert!(is_dispatch_exempt(
            &Method::GET,
            "/_miroir/ui/search/locale/en-US"
        ));
        assert!(is_dispatch_exempt(
            &Method::GET,
            "/_miroir/ui/search/locale/fr"
        ));
    }

    #[test]
    fn exempt_get_locale_no_variant_not_exempt() {
        assert!(!is_dispatch_exempt(
            &Method::GET,
            "/_miroir/ui/search/locale/"
        ));
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
        assert!(is_dispatch_exempt(
            &Method::GET,
            "/_miroir/ui/search/products/session"
        ));
    }

    #[test]
    fn exempt_get_session_no_index_not_exempt() {
        assert!(!is_dispatch_exempt(
            &Method::GET,
            "/_miroir/ui/search//session"
        ));
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

    #[test]
    fn exempt_get_miroir_ready() {
        assert!(is_dispatch_exempt(&Method::GET, "/_miroir/ready"));
        assert!(!is_dispatch_exempt(&Method::POST, "/_miroir/ready"));
    }

    #[test]
    fn exempt_get_health() {
        assert!(is_dispatch_exempt(&Method::GET, "/health"));
        assert!(!is_dispatch_exempt(&Method::POST, "/health"));
    }

    #[test]
    fn exempt_get_version() {
        assert!(is_dispatch_exempt(&Method::GET, "/version"));
        assert!(!is_dispatch_exempt(&Method::POST, "/version"));
    }

    // -----------------------------------------------------------------------
    // Rule 0 — exempt endpoints skip auth entirely
    // -----------------------------------------------------------------------

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
        let verdict = dispatch_bearer(&Method::GET, "/ui/search/products", None, &state);
        assert_eq!(verdict, AuthVerdict::Exempt);
    }

    #[test]
    fn exempt_ready_ignores_all_tokens() {
        let state = test_state();
        let verdict = dispatch_bearer(&Method::GET, "/_miroir/ready", Some("bogus-token"), &state);
        assert_eq!(verdict, AuthVerdict::Exempt);
    }

    #[test]
    fn exempt_health_ignores_all_tokens() {
        let state = test_state();
        let verdict = dispatch_bearer(&Method::GET, "/health", Some("bogus-token"), &state);
        assert_eq!(verdict, AuthVerdict::Exempt);
    }

    #[test]
    fn exempt_health_with_no_token() {
        let state = test_state();
        let verdict = dispatch_bearer(&Method::GET, "/health", None, &state);
        assert_eq!(verdict, AuthVerdict::Exempt);
    }

    #[test]
    fn exempt_version_ignores_all_tokens() {
        let state = test_state();
        let verdict = dispatch_bearer(&Method::GET, "/version", Some("bogus-token"), &state);
        assert_eq!(verdict, AuthVerdict::Exempt);
    }

    #[test]
    fn exempt_version_with_no_token() {
        let state = test_state();
        let verdict = dispatch_bearer(&Method::GET, "/version", None, &state);
        assert_eq!(verdict, AuthVerdict::Exempt);
    }

    // -----------------------------------------------------------------------
    // /_miroir/metrics requires admin key (not exempt)
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_requires_admin_key() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::GET,
            "/_miroir/metrics",
            Some("admin-key-456"),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::Authenticated(TokenKind::AdminKey));
    }

    #[test]
    fn metrics_rejects_master_key() {
        let state = test_state();
        let verdict = dispatch_bearer(
            &Method::GET,
            "/_miroir/metrics",
            Some("master-key-123"),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::InvalidAuth);
    }

    #[test]
    fn metrics_rejects_missing_auth() {
        let state = test_state();
        let verdict = dispatch_bearer(&Method::GET, "/_miroir/metrics", None, &state);
        assert_eq!(verdict, AuthVerdict::InvalidAuth);
    }

    // -----------------------------------------------------------------------
    // Rule 1 — JWT-shape probe
    // -----------------------------------------------------------------------

    #[test]
    fn jwt_shape_probe_accepts_valid_shape() {
        assert!(probe_jwt_shape(
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc123"
        ));
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
    fn jwt_on_non_admin_path_with_no_secret_returns_jwt_invalid() {
        let state = test_state(); // no JWT secrets configured
        let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc123";
        let verdict = dispatch_bearer(&Method::GET, "/indexes/products", Some(jwt), &state);
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
    // JWT signing and validation — primary secret
    // -----------------------------------------------------------------------

    #[test]
    fn sign_and_validate_primary_jwt() {
        let state = test_state_with_jwt();
        let token = state
            .sign_jwt("user1", "products", &["search"], 900)
            .unwrap();

        let claims = state.validate_jwt(&token).unwrap();
        assert_eq!(claims.sub, "user1");
        assert_eq!(claims.idx, "products");
        assert_eq!(claims.scope, vec!["search"]);
    }

    #[test]
    fn signed_jwt_authenticates_via_dispatch() {
        let state = test_state_with_jwt();
        let token = state
            .sign_jwt("user1", "products", &["search"], 900)
            .unwrap();

        let verdict = dispatch_bearer(&Method::GET, "/indexes/products", Some(&token), &state);
        assert_eq!(verdict, AuthVerdict::Authenticated(TokenKind::Jwt));
    }

    #[test]
    fn expired_jwt_returns_jwt_invalid() {
        let state = test_state_with_jwt();
        let now = epoch_seconds();
        let claims = JwtClaims {
            iss: "miroir".to_string(),
            sub: "user1".to_string(),
            idx: "products".to_string(),
            scope: vec!["search".to_string()],
            iat: now - 3600,
            exp: now - 100, // expired well beyond 30s leeway
        };
        let header = JwtHeader {
            alg: "HS256".to_string(),
            kid: KID_PRIMARY.to_string(),
            typ: "JWT".to_string(),
        };
        let token = jwt_encode(
            &header,
            &claims,
            state.jwt_primary.as_ref().unwrap().as_bytes(),
        )
        .unwrap();

        let result = state.validate_jwt(&token);
        assert_eq!(result, Err(JwtValidationError::Expired));
    }

    #[test]
    fn tampered_signature_returns_invalid_signature() {
        let state = test_state_with_jwt();
        let mut token = state
            .sign_jwt("user1", "products", &["search"], 900)
            .unwrap();
        // Tamper with the signature
        let parts: Vec<&str> = token.split('.').collect();
        token = format!("{}.{}.tampered_sig", parts[0], parts[1]);

        let result = state.validate_jwt(&token);
        assert_eq!(result, Err(JwtValidationError::InvalidSignature));
    }

    // -----------------------------------------------------------------------
    // JWT dual-secret rotation validation
    // -----------------------------------------------------------------------

    #[test]
    fn rotation_old_token_validates_via_previous_secret() {
        let primary = "test-secret-primary-key-32byte";
        let previous = "test-secret-previous-key-32byte";

        // Sign token with the previous secret
        let now = epoch_seconds();
        let claims = JwtClaims {
            iss: "miroir".to_string(),
            sub: "user1".to_string(),
            idx: "products".to_string(),
            scope: vec!["search".to_string()],
            iat: now,
            exp: now + 900,
        };
        let header = JwtHeader {
            alg: "HS256".to_string(),
            kid: KID_PREVIOUS.to_string(),
            typ: "JWT".to_string(),
        };
        let old_token = jwt_encode(&header, &claims, previous.as_bytes()).unwrap();

        // Simulate rotation — new primary, old primary as previous
        let state = AuthState {
            master_key: "m".to_string(),
            admin_key: "a".to_string(),
            jwt_primary: Some(primary.to_string()),
            jwt_previous: Some(previous.to_string()),
            seal_key: test_key(),
            revoked_sessions: Arc::new(DashMap::new()),
            admin_session_revoked_total: Counter::with_opts(prometheus::Opts::new(
                "test_revoked_total",
                "test",
            ))
            .unwrap(),
        };

        // Old token should still validate via previous secret
        let validated = state.validate_jwt(&old_token).unwrap();
        assert_eq!(validated.sub, "user1");

        // And dispatch should authenticate it
        let verdict = dispatch_bearer(&Method::GET, "/indexes/products", Some(&old_token), &state);
        assert_eq!(verdict, AuthVerdict::Authenticated(TokenKind::Jwt));
    }

    #[test]
    fn rotation_new_token_validates_via_primary_secret() {
        let state = test_state_with_dual_jwt();
        let new_token = state.sign_jwt("user2", "orders", &["search"], 900).unwrap();

        let validated = state.validate_jwt(&new_token).unwrap();
        assert_eq!(validated.sub, "user2");
        assert_eq!(validated.idx, "orders");
    }

    #[test]
    fn rotation_wrong_secret_returns_invalid_signature() {
        let state = AuthState {
            master_key: "m".to_string(),
            admin_key: "a".to_string(),
            jwt_primary: Some("correct-secret-key-32bytes-long!!!".to_string()),
            jwt_previous: Some("previous-secret-key-32bytes-long!!".to_string()),
            seal_key: test_key(),
            revoked_sessions: Arc::new(DashMap::new()),
            admin_session_revoked_total: Counter::with_opts(prometheus::Opts::new(
                "test_revoked_total",
                "test",
            ))
            .unwrap(),
        };

        // Token signed with a completely different secret
        let now = epoch_seconds();
        let claims = JwtClaims {
            iss: "miroir".to_string(),
            sub: "user1".to_string(),
            idx: "products".to_string(),
            scope: vec!["search".to_string()],
            iat: now,
            exp: now + 900,
        };
        let header = JwtHeader {
            alg: "HS256".to_string(),
            kid: KID_PRIMARY.to_string(),
            typ: "JWT".to_string(),
        };
        let token = jwt_encode(
            &header,
            &claims,
            "wrong-secret-key-32bytes-long!!!!".as_bytes(),
        )
        .unwrap();

        let result = state.validate_jwt(&token);
        assert_eq!(result, Err(JwtValidationError::InvalidSignature));
    }

    #[test]
    fn leak_response_empty_previous_rejects_old_tokens() {
        let state = AuthState {
            master_key: "m".to_string(),
            admin_key: "a".to_string(),
            jwt_primary: Some("new-primary-secret-key-32bytes!!".to_string()),
            jwt_previous: Some(String::new()), // empty = leak response
            seal_key: test_key(),
            revoked_sessions: Arc::new(DashMap::new()),
            admin_session_revoked_total: Counter::with_opts(prometheus::Opts::new(
                "test_revoked_total",
                "test",
            ))
            .unwrap(),
        };

        let result = state.validate_jwt("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.fake");
        assert_eq!(result, Err(JwtValidationError::PreviousSecretEmpty));
    }

    #[test]
    fn rotation_after_step5_steady_state_previous_removed() {
        let primary = "final-primary-secret-key-32bytes";
        let state = AuthState {
            master_key: "m".to_string(),
            admin_key: "a".to_string(),
            jwt_primary: Some(primary.to_string()),
            jwt_previous: None,
            seal_key: test_key(),
            revoked_sessions: Arc::new(DashMap::new()),
            admin_session_revoked_total: Counter::with_opts(prometheus::Opts::new(
                "test_revoked_total",
                "test",
            ))
            .unwrap(),
        };

        // Tokens signed with current primary work
        let token = state
            .sign_jwt("user1", "products", &["search"], 900)
            .unwrap();
        assert!(state.validate_jwt(&token).is_ok());

        // Old tokens signed with now-removed previous fail
        let old_claims = JwtClaims {
            iss: "miroir".to_string(),
            sub: "user1".to_string(),
            idx: "products".to_string(),
            scope: vec!["search".to_string()],
            iat: epoch_seconds() - 100,
            exp: epoch_seconds() + 800,
        };
        let old_header = JwtHeader {
            alg: "HS256".to_string(),
            kid: KID_PREVIOUS.to_string(),
            typ: "JWT".to_string(),
        };
        let old_token = jwt_encode(
            &old_header,
            &old_claims,
            "old-previous-secret-now-removed".as_bytes(),
        )
        .unwrap();
        assert!(state.validate_jwt(&old_token).is_err());
    }

    // -----------------------------------------------------------------------
    // End-to-end rotation scenario
    // -----------------------------------------------------------------------

    #[test]
    fn full_rotation_e2e() {
        let secret_v1 = "version-1-secret-key-32bytes-long!";
        let secret_v2 = "version-2-secret-key-32bytes-long!";

        // Pre-rotation: only v1
        let pre = AuthState {
            master_key: "m".into(),
            admin_key: "a".into(),
            jwt_primary: Some(secret_v1.into()),
            jwt_previous: None,
            seal_key: test_key(),
            revoked_sessions: Arc::new(DashMap::new()),
            admin_session_revoked_total: Counter::with_opts(prometheus::Opts::new(
                "test_revoked_total",
                "test",
            ))
            .unwrap(),
        };
        let token_v1 = pre.sign_jwt("alice", "idx", &["search"], 900).unwrap();
        assert!(pre.validate_jwt(&token_v1).is_ok());

        // During rotation: v2 primary, v1 previous
        let during = AuthState {
            master_key: "m".into(),
            admin_key: "a".into(),
            jwt_primary: Some(secret_v2.into()),
            jwt_previous: Some(secret_v1.into()),
            seal_key: test_key(),
            revoked_sessions: Arc::new(DashMap::new()),
            admin_session_revoked_total: Counter::with_opts(prometheus::Opts::new(
                "test_revoked_total",
                "test",
            ))
            .unwrap(),
        };
        // Old token still validates
        assert!(during.validate_jwt(&token_v1).is_ok());
        // New tokens work too
        let token_v2 = during.sign_jwt("bob", "idx", &["search"], 900).unwrap();
        assert!(during.validate_jwt(&token_v2).is_ok());

        // Post-rotation: only v2
        let post = AuthState {
            master_key: "m".into(),
            admin_key: "a".into(),
            jwt_primary: Some(secret_v2.into()),
            jwt_previous: None,
            seal_key: test_key(),
            revoked_sessions: Arc::new(DashMap::new()),
            admin_session_revoked_total: Counter::with_opts(prometheus::Opts::new(
                "test_revoked_total",
                "test",
            ))
            .unwrap(),
        };
        // New token still works
        assert!(post.validate_jwt(&token_v2).is_ok());
        // Old token is rejected (signed with v1, no previous loaded)
        assert!(post.validate_jwt(&token_v1).is_err());
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
        let verdict = dispatch_bearer(&Method::GET, "/_miroir/some/endpoint", None, &state);
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
        let verdict = dispatch_bearer(&Method::POST, "/indexes/products/documents", None, &state);
        assert_eq!(verdict, AuthVerdict::InvalidAuth);
    }

    // -----------------------------------------------------------------------
    // Rule 4 — missing auth → 401 miroir_invalid_auth
    // -----------------------------------------------------------------------

    #[test]
    fn missing_auth_on_gated_endpoint_returns_invalid_auth() {
        let state = test_state();
        let verdict = dispatch_bearer(&Method::POST, "/indexes", None, &state);
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
    #[test]
    fn constant_time_no_timing_leak() {
        use std::time::Instant;

        let expected = b"admin-key-456";
        let all_wrong = b"xxxxxxxxxxxxx";
        let one_wrong = b"admin-key-457";

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
        assert!(limiter
            .check(&RateLimitBucket::AdminLogin("127.0.0.1".into()))
            .is_ok());
        assert!(limiter
            .check(&RateLimitBucket::SearchUi("10.0.0.1".into()))
            .is_ok());
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
        let cases = vec![
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

    // -----------------------------------------------------------------------
    // Scope and index validation tests (plan §13.21)
    // -----------------------------------------------------------------------

    #[test]
    fn scope_validation_allows_search_on_matching_index() {
        let claims = JwtClaims {
            iss: "miroir".to_string(),
            sub: "user1".to_string(),
            idx: "products".to_string(),
            scope: vec![
                "search".to_string(),
                "multi_search".to_string(),
                "beacon".to_string(),
            ],
            iat: epoch_seconds(),
            exp: epoch_seconds() + 900,
        };

        let result = validate_jwt_scope(&claims, &Method::POST, "/indexes/products/search");
        assert!(result.is_ok());
    }

    #[test]
    fn scope_validation_denies_search_on_different_index() {
        let claims = JwtClaims {
            iss: "miroir".to_string(),
            sub: "user1".to_string(),
            idx: "products".to_string(),
            scope: vec![
                "search".to_string(),
                "multi_search".to_string(),
                "beacon".to_string(),
            ],
            iat: epoch_seconds(),
            exp: epoch_seconds() + 900,
        };

        let result = validate_jwt_scope(&claims, &Method::POST, "/indexes/orders/search");
        assert_eq!(result, Err(JwtValidationError::ScopeDenied));
    }

    #[test]
    fn scope_validation_denies_missing_scope_action() {
        let claims = JwtClaims {
            iss: "miroir".to_string(),
            sub: "user1".to_string(),
            idx: "products".to_string(),
            scope: vec!["beacon".to_string()], // missing "search"
            iat: epoch_seconds(),
            exp: epoch_seconds() + 900,
        };

        let result = validate_jwt_scope(&claims, &Method::POST, "/indexes/products/search");
        assert_eq!(result, Err(JwtValidationError::ScopeDenied));
    }

    #[test]
    fn scope_validation_allows_multi_search() {
        let claims = JwtClaims {
            iss: "miroir".to_string(),
            sub: "user1".to_string(),
            idx: "products".to_string(),
            scope: vec![
                "search".to_string(),
                "multi_search".to_string(),
                "beacon".to_string(),
            ],
            iat: epoch_seconds(),
            exp: epoch_seconds() + 900,
        };

        let result = validate_jwt_scope(&claims, &Method::POST, "/multi-search");
        assert!(result.is_ok());
    }

    #[test]
    fn scope_validation_denies_multi_search_without_scope() {
        let claims = JwtClaims {
            iss: "miroir".to_string(),
            sub: "user1".to_string(),
            idx: "products".to_string(),
            scope: vec!["search".to_string()], // missing "multi_search"
            iat: epoch_seconds(),
            exp: epoch_seconds() + 900,
        };

        let result = validate_jwt_scope(&claims, &Method::POST, "/multi-search");
        assert_eq!(result, Err(JwtValidationError::ScopeDenied));
    }

    #[test]
    fn scope_validation_allows_beacon_on_matching_index() {
        let claims = JwtClaims {
            iss: "miroir".to_string(),
            sub: "user1".to_string(),
            idx: "products".to_string(),
            scope: vec![
                "search".to_string(),
                "multi_search".to_string(),
                "beacon".to_string(),
            ],
            iat: epoch_seconds(),
            exp: epoch_seconds() + 900,
        };

        let result =
            validate_jwt_scope(&claims, &Method::POST, "/_miroir/ui/search/products/beacon");
        assert!(result.is_ok());
    }

    #[test]
    fn scope_validation_denies_beacon_on_different_index() {
        let claims = JwtClaims {
            iss: "miroir".to_string(),
            sub: "user1".to_string(),
            idx: "products".to_string(),
            scope: vec![
                "search".to_string(),
                "multi_search".to_string(),
                "beacon".to_string(),
            ],
            iat: epoch_seconds(),
            exp: epoch_seconds() + 900,
        };

        let result = validate_jwt_scope(&claims, &Method::POST, "/_miroir/ui/search/orders/beacon");
        assert_eq!(result, Err(JwtValidationError::ScopeDenied));
    }

    #[test]
    fn scope_validation_skips_non_scoped_endpoints() {
        let claims = JwtClaims {
            iss: "miroir".to_string(),
            sub: "user1".to_string(),
            idx: "products".to_string(),
            scope: vec!["search".to_string()],
            iat: epoch_seconds(),
            exp: epoch_seconds() + 900,
        };

        // Endpoints that don't require scope validation should pass
        assert!(validate_jwt_scope(&claims, &Method::GET, "/indexes/products").is_ok());
        assert!(validate_jwt_scope(&claims, &Method::POST, "/indexes/products/documents").is_ok());
        assert!(validate_jwt_scope(&claims, &Method::GET, "/_miroir/admin/settings").is_ok());
    }

    #[test]
    fn dispatch_with_jwt_scope_denied_returns_scope_denied_verdict() {
        let state = test_state_with_jwt();
        let token = state
            .sign_jwt("user1", "products", &["search"], 900)
            .unwrap();

        // Token should be valid, but trying to use it on a different index should fail
        let verdict = dispatch_bearer(
            &Method::POST,
            "/indexes/orders/search",
            Some(&token),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::JwtScopeDenied);
    }

    #[test]
    fn dispatch_with_jwt_correct_index_and_scope_succeeds() {
        let state = test_state_with_jwt();
        let token = state
            .sign_jwt(
                "user1",
                "products",
                &["search", "multi_search", "beacon"],
                900,
            )
            .unwrap();

        let verdict = dispatch_bearer(
            &Method::POST,
            "/indexes/products/search",
            Some(&token),
            &state,
        );
        assert_eq!(verdict, AuthVerdict::Authenticated(TokenKind::Jwt));
    }
}
