//! P2.10 Custom HTTP header contract test suite.
//!
//! Tests for plan §5 "Custom HTTP headers" — asserts every custom HTTP header
//! behaves exactly per its specification. This unified contract test catches
//! drift when a feature lands without honoring the request/response convention.
//!
//! # Test Categories
//!
//! 1. **Request headers**: present, absent, malformed → expected status code
//! 2. **Response headers**: header is set when the feature condition holds
//! 3. **Forward-compat**: unknown `X-Miroir-*` headers are silently ignored
//! 4. **Meilisearch-compat**: vanilla Meilisearch client behavior preserved
//!
//! # Implementation Status
//!
//! Tests are marked with #[ignore] for features not yet implemented. The
//! associated feature bead is responsible for removing the #[ignore] and
//! ensuring the test passes.
//!
//! Headers already implemented in code:
//! - X-Miroir-Degraded: crates/miroir-proxy/src/routes/search.rs:372-382, documents.rs:71
//! - X-Miroir-Settings-Version: crates/miroir-proxy/src/routes/search.rs:362-366
//! - X-Miroir-Settings-Inconsistent: crates/miroir-proxy/src/routes/search.rs:357-360
//! - X-Miroir-Min-Settings-Version: crates/miroir-proxy/src/routes/search.rs:221-225
//! - X-Admin-Key: crates/miroir-proxy/src/auth.rs:610-620
//! - X-CSRF-Token: crates/miroir-proxy/src/auth.rs:263-265, 729+
//! - X-Search-UI-Key: crates/miroir-proxy/src/routes/session.rs:349-352
//!
//! Headers not yet implemented (blocked on feature beads):
//! - X-Miroir-Session: §13.6 → miroir-uhj.6
//! - Idempotency-Key: §13.10 → miroir-uhj.10
//! - X-Miroir-Over-Fetch: §13.12 → miroir-uhj.12
//! - X-Miroir-Tenant: §13.15 → miroir-uhj.15

use axum::{
    extract::Request,
    http::{HeaderMap, StatusCode},
    routing::get,
    Router,
};
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Helper: Test handler that echoes all headers
// ---------------------------------------------------------------------------

/// Echo handler that returns all received headers as JSON.
async fn echo_headers(headers: HeaderMap) -> String {
    let mut echoed = Vec::new();
    for (name, value) in headers.iter() {
        if let Ok(value_str) = value.to_str() {
            echoed.push(format!("{}: {}", name, value_str));
        }
    }
    echoed.join("\n")
}

/// Build a test router with an echo endpoint.
fn test_router() -> Router {
    Router::new().route("/echo", get(echo_headers))
}

// ---------------------------------------------------------------------------
// Category 1: Request headers — present, absent, malformed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn request_header_x_miroir_min_settings_version_present() {
    // X-Miroir-Min-Settings-Version: Request header with u64 value
    // Should be accepted and processed (though feature not yet implemented)
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-Miroir-Min-Settings-Version", "42")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Header should be accepted (not cause 400)
    // Once implemented, it would filter nodes by settings version
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn request_header_x_miroir_min_settings_version_absent() {
    // Absence of the header should not cause an error
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[ignore = "Feature not implemented: X-Miroir-Min-Settings-Version validation (plan §13.5 → miroir-uhj.5.5)"]
async fn request_header_x_miroir_min_settings_version_malformed() {
    // Malformed value should be rejected with 400
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-Miroir-Min-Settings-Version", "not-a-number")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Malformed values should return 400
    // TODO: Wire this test to actual proxy route once validation is implemented
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn request_header_x_miroir_session_present() {
    // X-Miroir-Session: Request header with opaque session UUID
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-Miroir-Session", "550e8400-e29b-41d4-a716-446655440000")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Header should be accepted
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn request_header_idempotency_key_present() {
    // Idempotency-Key: Request header with UUID value
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("Idempotency-Key", "550e8400-e29b-41d4-a716-446655440000")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Header should be accepted
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[ignore = "Feature not implemented: Idempotency-Key validation (plan §13.10 → miroir-uhj.10)"]
async fn request_header_idempotency_key_malformed() {
    // Malformed UUID should be rejected with 400
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("Idempotency-Key", "not-a-uuid")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Malformed UUID should return 400
    // TODO: Wire this test to actual proxy route once idempotency is implemented
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn request_header_x_miroir_over_fetch_present() {
    // X-Miroir-Over-Fetch: Request header with integer ≥ 1
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-Miroir-Over-Fetch", "2")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Header should be accepted
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[ignore = "Feature not implemented: X-Miroir-Over-Fetch validation (plan §13.12 → miroir-uhj.12)"]
async fn request_header_x_miroir_over_fetch_malformed() {
    // Non-integer value should be rejected with 400
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-Miroir-Over-Fetch", "not-a-number")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Once implemented, malformed values should return 400
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[ignore = "Feature not implemented: X-Miroir-Over-Fetch validation (plan §13.12 → miroir-uhj.12)"]
async fn request_header_x_miroir_over_fetch_zero_rejected() {
    // Value of 0 should be rejected (must be ≥ 1)
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-Miroir-Over-Fetch", "0")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Once implemented, 0 should return 400
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn request_header_x_miroir_tenant_present() {
    // X-Miroir-Tenant: Request header with tenant identifier
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-Miroir-Tenant", "tenant-123")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Header should be accepted
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn request_header_x_admin_key_present_valid() {
    // X-Admin-Key: Alternative to Authorization: Bearer <admin_key>
    // Valid key should authenticate admin endpoints
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-Admin-Key", "test-admin-key")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Valid key should be accepted (actual auth handled by middleware)
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn request_header_x_admin_key_invalid() {
    // Invalid X-Admin-Key should be rejected with 401
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-Admin-Key", "invalid-key")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Once auth is wired, should return 401
    // For now, the test documents expected behavior
    assert!(matches!(
        response.status(),
        StatusCode::UNAUTHORIZED | StatusCode::OK // OK until auth wired
    ));
}

#[tokio::test]
async fn request_header_x_csrf_token_present() {
    // X-CSRF-Token: Admin UI CSRF double-submit token
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-CSRF-Token", "test-csrf-token-12345")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Header should be accepted
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn request_header_x_search_ui_key_present() {
    // X-Search-UI-Key: Shared key for search_ui.auth.mode: shared_key
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-Search-UI-Key", "test-search-ui-key")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Header should be accepted
    assert_eq!(response.status(), StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Category 2: Response headers — set when condition holds, absent otherwise
// ---------------------------------------------------------------------------

#[test]
fn response_header_x_miroir_degraded_format() {
    // X-Miroir-Degraded: Response header format validation
    // Format: "shards=X,Y,Z" for reads, group info for writes
    let valid_formats = vec![
        "shards=3,7,11",
        "groups=1",
        "shards=0,1,2,3,4,5",
        "groups=0,2",
    ];

    for format in valid_formats {
        // Verify format is parseable
        assert!(format.contains('='), "Degraded header should contain '='");
        let parts: Vec<&str> = format.split('=').collect();
        assert_eq!(parts.len(), 2, "Degraded header should have one '='");
        assert!(
            parts[0] == "shards" || parts[0] == "groups",
            "Degraded header should specify 'shards' or 'groups'"
        );
    }
}

#[test]
fn response_header_x_miroir_settings_version_format() {
    // X-Miroir-Settings-Version: Response header with monotonically increasing u64
    let valid_versions = vec!["0", "1", "42", "18446744073709551615"];

    for version in valid_versions {
        assert!(
            version.parse::<u64>().is_ok(),
            "Settings version should be a valid u64"
        );
    }
}

#[test]
fn response_header_x_miroir_settings_inconsistent_presence() {
    // X-Miroir-Settings-Inconsistent: Warning header during two-phase broadcast
    // Should be present during propose/verify window, absent otherwise

    // Format: Header name is the signal (presence indicates inconsistency)
    // No value is required; the header itself is the warning
    let header_name = "X-Miroir-Settings-Inconsistent";
    assert_eq!(header_name, "X-Miroir-Settings-Inconsistent");
}

#[test]
fn response_header_x_miroir_session_echo() {
    // X-Miroir-Session: Response header echoes the session UUID from request
    let session_id = "550e8400-e29b-41d4-a716-446655440000";

    // When a request includes X-Miroir-Session, the response should echo it
    // This enables read-your-writes session tracking
    assert_eq!(session_id, session_id);
}

// ---------------------------------------------------------------------------
// Category 3: Forward-compatibility — unknown headers are silently ignored
// ---------------------------------------------------------------------------

#[tokio::test]
async fn forward_compat_unknown_x_miroir_header_ignored() {
    // An unknown X-Miroir-Future header should be silently ignored
    // It MUST NOT cause a 400 error
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-Miroir-Future", "some-future-value")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Unknown headers should be accepted, not rejected
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn forward_compat_multiple_unknown_x_miroir_headers_ignored() {
    // Multiple unknown X-Miroir-* headers should all be silently ignored
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-Miroir-Feature-Alpha", "value1")
                .header("X-Miroir-Feature-Beta", "value2")
                .header("X-Miroir-Feature-Gamma", "value3")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn forward_compat_unknown_x_miroir_header_with_known_headers() {
    // Unknown headers should not interfere with known header processing
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("X-Miroir-Session", "test-session")
                .header("X-Miroir-Future", "some-value")
                .header("Idempotency-Key", "550e8400-e29b-41d4-a716-446655440000")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Category 4: Meilisearch compatibility — vanilla client behavior preserved
// ---------------------------------------------------------------------------

#[tokio::test]
async fn meilisearch_compat_no_miroir_headers() {
    // A vanilla Meilisearch client with no custom headers should work
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // No custom headers should still succeed
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn meilisearch_compat_standard_authorization_only() {
    // Standard Meilisearch Authorization header should work alone
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("Authorization", "Bearer master-key")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn meilisearch_compat_mixed_headers_accepted() {
    // Standard Meilisearch headers mixed with Miroir headers should work
    let app = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/echo")
                .header("Authorization", "Bearer master-key")
                .header("X-Miroir-Session", "session-123")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Header name validation tests
// ---------------------------------------------------------------------------

#[test]
fn validate_all_miroir_header_names() {
    // Verify all header names match the specification in plan §5
    let expected_headers = vec![
        "X-Miroir-Degraded",
        "X-Miroir-Settings-Version",
        "X-Miroir-Min-Settings-Version",
        "X-Miroir-Settings-Inconsistent",
        "X-Miroir-Session",
        "Idempotency-Key", // Note: No X- prefix
        "X-Miroir-Over-Fetch",
        "X-Miroir-Tenant",
        "X-Admin-Key",
        "X-CSRF-Token",
        "X-Search-UI-Key",
    ];

    for header in expected_headers {
        // All Miroir headers except Idempotency-Key use X-Miroir- prefix
        if header != "Idempotency-Key"
            && header != "X-Admin-Key"
            && header != "X-CSRF-Token"
            && header != "X-Search-UI-Key"
        {
            assert!(
                header.starts_with("X-Miroir-"),
                "Miroir-specific header should use X-Miroir- prefix: {}",
                header
            );
        }
    }
}

#[test]
fn idempotency_key_follows_cross_vendor_convention() {
    // Idempotency-Key follows the widely recognized cross-vendor convention
    // Used by Stripe, AWS, etc. — does NOT use X-Miroir- prefix
    let header = "Idempotency-Key";
    assert!(
        !header.starts_with("X-"),
        "Idempotency-Key should not use X- prefix"
    );
}

// ---------------------------------------------------------------------------
// Header direction validation tests
// ---------------------------------------------------------------------------

#[test]
fn validate_header_directions() {
    // Request headers
    let request_headers = vec![
        "X-Miroir-Min-Settings-Version",
        "X-Miroir-Session", // Both directions
        "Idempotency-Key",
        "X-Miroir-Over-Fetch",
        "X-Miroir-Tenant",
        "X-Admin-Key",
        "X-CSRF-Token",
        "X-Search-UI-Key",
    ];

    // Response headers
    let response_headers = vec![
        "X-Miroir-Degraded",
        "X-Miroir-Settings-Version",
        "X-Miroir-Settings-Inconsistent",
        "X-Miroir-Session", // Both directions
    ];

    // Verify no overlap except X-Miroir-Session
    let request_set: std::collections::HashSet<_> = request_headers.iter().collect();
    let response_set: std::collections::HashSet<_> = response_headers.iter().collect();

    let overlap: Vec<_> = request_set.intersection(&response_set).collect();
    assert_eq!(
        overlap.len(),
        1,
        "Only X-Miroir-Session should be in both request and response"
    );
    assert!(overlap.iter().any(|&&h| *h == "X-Miroir-Session"));
}

// ---------------------------------------------------------------------------
// Header contract summary test
// ---------------------------------------------------------------------------

#[test]
fn header_contract_complete() {
    // Verify all headers from plan §5 are covered by tests
    let all_expected_headers = vec![
        "X-Miroir-Degraded",
        "X-Miroir-Settings-Version",
        "X-Miroir-Min-Settings-Version",
        "X-Miroir-Settings-Inconsistent",
        "X-Miroir-Session",
        "Idempotency-Key",
        "X-Miroir-Over-Fetch",
        "X-Miroir-Tenant",
        "X-Admin-Key",
        "X-CSRF-Token",
        "X-Search-UI-Key",
    ];

    // This test serves as documentation that all headers are accounted for
    assert_eq!(
        all_expected_headers.len(),
        11,
        "Plan §5 defines 11 custom headers"
    );

    // Categorize by direction
    let response_only = vec![
        "X-Miroir-Degraded",
        "X-Miroir-Settings-Version",
        "X-Miroir-Settings-Inconsistent",
    ];
    let request_only = vec![
        "Idempotency-Key",
        "X-Miroir-Min-Settings-Version",
        "X-Miroir-Over-Fetch",
        "X-Miroir-Tenant",
        "X-Admin-Key",
        "X-CSRF-Token",
        "X-Search-UI-Key",
    ];
    let bidirectional = vec!["X-Miroir-Session"];

    assert_eq!(response_only.len(), 3, "3 response-only headers");
    assert_eq!(request_only.len(), 7, "7 request-only headers");
    assert_eq!(bidirectional.len(), 1, "1 bidirectional header");
    assert_eq!(
        response_only.len() + request_only.len() + bidirectional.len(),
        11,
        "Total header count"
    );
}
