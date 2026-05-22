# P2.7 Auth: Bearer-Token Dispatch Implementation

## Summary

Implemented the bearer-token dispatch chain per plan §5 rules 0-5 with full test coverage. The implementation supports three token types simultaneously: `master_key`, `admin_key`, and search UI JWTs.

## Implementation Details

### Rule 0 - Dispatch-Exempt Check (`is_dispatch_exempt`)

Endpoints that bypass all auth checks (handler decides auth):
- `GET /health` - unauthenticated liveness probe (Meilisearch-compatible)
- `GET /version` - unauthenticated version endpoint (Meilisearch-compatible)
- `GET /_miroir/ready` - unauthenticated readiness probe (plan §10)
- `GET /_miroir/ui/search/locale/*` - unauthenticated public locale fetch
- `POST /_miroir/admin/login` - credentials in body
- `GET /_miroir/ui/search/{index}/session` - auth per `search_ui.auth.mode`
- `GET /ui/search/{index}` - public SPA entry point

**Note:** `GET /_miroir/metrics` requires admin_key (NOT exempt)

### Rule 1 - JWT-Shape Probe

- `probe_jwt_shape()`: Checks if token has JWT structure (three dot-separated base64url segments)
- `validate_jwt()`: Full JWT validation with:
  - HMAC-SHA256 signature verification
  - Expiration checking with 30s leeway
  - Support for dual secrets during rotation (primary + previous)
  - Key ID (`kid`) header to identify which secret signed the token

### Rule 2 - Admin-Path Opaque-Token Match

- `is_admin_path()`: Checks if path starts with `/_miroir/`
- For admin paths: only `admin_key` is accepted via Bearer token
- Master key is explicitly rejected on admin paths

### Rule 3 - Master-Key Match

- For non-admin paths: only `master_key` is accepted via Bearer token
- Admin key is explicitly rejected on non-admin paths

### Rule 4 - Mismatch

- Missing Authorization header on auth-gated endpoints → 401 `miroir_invalid_auth`
- Invalid token → 401 `miroir_invalid_auth`
- JWT-shaped token that fails validation → 401 `miroir_jwt_invalid`

### X-Admin-Key Short-Circuit

- `check_x_admin_key()`: Independent header check for admin endpoints
- Provides alternative auth mechanism for admin operations
- Uses constant-time comparison

## Security Features

### Constant-Time Comparison

- All opaque-token comparisons use `subtle::ConstantTimeEq`
- Prevents timing side-channel attacks on secret key values
- Test `constant_time_no_timing_leak` verifies no measurable delta between "all bytes wrong" and "one byte wrong"

### Rate-Limit Hooks

- `RateLimitBucket` enum defines bucket key types:
  - `AdminLogin(String)` - `miroir:ratelimit:adminlogin:<ip>`
  - `SearchUi(String)` - `miroir:ratelimit:searchui:<ip>`
- Phase 2 uses in-memory stub (always allows)
- Phase 6 will back with task store (Redis/SQLite)

## Test Coverage

All acceptance criteria verified with unit tests:

1. ✅ Every row in plan §5 rule 5 exempt list has a unit test
2. ✅ Opaque token on `/_miroir/*` matches only admin_key; never master_key
3. ✅ Opaque token on other paths matches only master_key; never admin_key
4. ✅ Missing Authorization on auth-gated endpoints → 401 `miroir_invalid_auth`
5. ✅ `X-Admin-Key` alone gates admin endpoints equivalently to Bearer admin_key
6. ✅ Constant-time compare: timing-injection harness shows no measurable delta

**Total tests:** 62 auth tests, all passing

## Configuration

### AuthState Structure

```rust
pub struct AuthState {
    pub master_key: String,
    pub admin_key: String,
    pub jwt_primary: Option<String>,        // SEARCH_UI_JWT_SECRET
    pub jwt_previous: Option<String>,       // SEARCH_UI_JWT_SECRET_PREVIOUS
    pub seal_key: SealKey,                  // For admin session cookies
    pub revoked_sessions: Arc<DashMap<String, ()>>,
    pub admin_session_revoked_total: Counter,
}
```

### Environment Variables

- `MIROIR_MASTER_KEY` - Client-facing API key (overrides config)
- `MIROIR_ADMIN_API_KEY` - Admin API key (overrides config)
- `SEARCH_UI_JWT_SECRET` - JWT signing secret (primary)
- `SEARCH_UI_JWT_SECRET_PREVIOUS` - JWT signing secret (rotation overlap)
- `ADMIN_SESSION_SEAL_KEY` - Admin session cookie sealing key

## Files Modified

- `crates/miroir-proxy/src/auth.rs` - Core bearer-token dispatch implementation
- `crates/miroir-proxy/src/main.rs` - AuthState initialization and middleware wiring

## JWT Rotation Support

The implementation supports seamless JWT secret rotation:

1. **Pre-rotation**: Only primary secret active
2. **During rotation**: New primary + old primary as previous
3. **Post-rotation**: Only new primary (previous removed)

Tokens signed with either secret are accepted during the rotation window. Old tokens continue to work until they expire naturally.

## Middleware Stack

The `auth_middleware` is correctly positioned in the middleware stack:

1. `csrf_middleware` - runs first
2. `auth_middleware` - bearer-token dispatch
3. Extension layers
4. `request_id_middleware` - sets X-Request-Id header
5. `telemetry_middleware` - reads X-Request-Id, creates tracing span

## Integration with Admin Sessions

The dispatch chain integrates with admin session cookies:

1. If a sealed admin session cookie is present, it is unsealed
2. Session ID is checked against revocation cache (Pub/Sub sync)
3. Valid sessions are authenticated without requiring Bearer token
4. CSRF validation applies to session-based auth (not Bearer tokens)
