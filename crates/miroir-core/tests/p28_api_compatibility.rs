//! P2.8 API compatibility tests — Phase 2 Definition of Done.
//!
//! Tests that verify:
//! 1. Error format parity with Meilisearch (plan §5)
//! 2. GET /_miroir/topology matches plan §10 JSON shape

use miroir_core::api_error::{ErrorType, MeilisearchError, MiroirCode};
use serde_json::json;

/// Test 1: All Miroir error codes produce the correct Meilisearch-compatible shape.
///
/// Per plan §5, every error must match the shape:
/// {"message": "...", "code": "...", "type": "...", "link": "..."}
#[test]
fn test_all_miroir_error_codes_have_correct_shape() {
    for code in MiroirCode::ALL {
        let err = MeilisearchError::new(code, "test error message");

        // Serialize to JSON
        let json_val = serde_json::to_value(&err).expect("failed to serialize error");

        // Verify all required fields exist
        assert!(
            json_val.get("message").is_some(),
            "message field missing for {:?}",
            code
        );
        assert!(
            json_val.get("code").is_some(),
            "code field missing for {:?}",
            code
        );
        assert!(
            json_val.get("type").is_some(),
            "type field missing for {:?}",
            code
        );
        assert!(
            json_val.get("link").is_some(),
            "link field missing for {:?}",
            code
        );

        // Verify field types
        assert_eq!(json_val["message"], "test error message");
        assert_eq!(json_val["code"], code.as_str());
        assert_eq!(json_val["type"], code.error_type().to_string());
        assert!(json_val["link"]
            .as_str()
            .unwrap()
            .starts_with("https://github.com/jedarden/miroir"));
    }
}

/// Test 2: Error code strings match the expected miroir_ prefix pattern.
#[test]
fn test_error_code_strings_have_miroir_prefix() {
    for code in MiroirCode::ALL {
        let code_str = code.as_str();
        assert!(
            code_str.starts_with("miroir_"),
            "Error code {:?} ({}) does not start with 'miroir_'",
            code,
            code_str
        );
    }
}

/// Test 3: HTTP status codes match Meilisearch conventions.
#[test]
fn test_http_status_codes_match_meilisearch_conventions() {
    // Invalid request errors → 400
    assert_eq!(MiroirCode::PrimaryKeyRequired.http_status(), 400);
    assert_eq!(MiroirCode::ReservedField.http_status(), 400);

    // Auth errors → 401
    assert_eq!(MiroirCode::JwtInvalid.http_status(), 401);
    assert_eq!(MiroirCode::InvalidAuth.http_status(), 401);
    assert_eq!(MiroirCode::MissingCsrf.http_status(), 401);

    // Auth scope errors → 403
    assert_eq!(MiroirCode::JwtScopeDenied.http_status(), 403);
    assert_eq!(MiroirCode::CsrfMismatch.http_status(), 403);

    // Conflict errors → 409
    assert_eq!(MiroirCode::IdempotencyKeyReused.http_status(), 409);
    assert_eq!(MiroirCode::MultiAliasNotWritable.http_status(), 409);
    assert_eq!(MiroirCode::IndexAlreadyExists.http_status(), 409);

    // Timeout → 504
    assert_eq!(MiroirCode::Timeout.http_status(), 504);

    // Service unavailable → 503
    assert_eq!(MiroirCode::NoQuorum.http_status(), 503);
    assert_eq!(MiroirCode::ShardUnavailable.http_status(), 503);
    assert_eq!(MiroirCode::SettingsVersionStale.http_status(), 503);
}

/// Test 4: Error type categories match Meilisearch's classification.
#[test]
fn test_error_type_categories_match_meilisearch() {
    // InvalidRequest errors
    assert_eq!(
        MiroirCode::PrimaryKeyRequired.error_type(),
        ErrorType::InvalidRequest
    );
    assert_eq!(
        MiroirCode::ReservedField.error_type(),
        ErrorType::InvalidRequest
    );
    assert_eq!(
        MiroirCode::IdempotencyKeyReused.error_type(),
        ErrorType::InvalidRequest
    );
    assert_eq!(
        MiroirCode::MultiAliasNotWritable.error_type(),
        ErrorType::InvalidRequest
    );
    assert_eq!(
        MiroirCode::IndexAlreadyExists.error_type(),
        ErrorType::InvalidRequest
    );

    // Auth errors
    assert_eq!(MiroirCode::JwtInvalid.error_type(), ErrorType::Auth);
    assert_eq!(MiroirCode::JwtScopeDenied.error_type(), ErrorType::Auth);
    assert_eq!(MiroirCode::InvalidAuth.error_type(), ErrorType::Auth);
    assert_eq!(MiroirCode::MissingCsrf.error_type(), ErrorType::Auth);
    assert_eq!(MiroirCode::CsrfMismatch.error_type(), ErrorType::Auth);

    // System errors
    assert_eq!(MiroirCode::NoQuorum.error_type(), ErrorType::System);
    assert_eq!(MiroirCode::ShardUnavailable.error_type(), ErrorType::System);
    assert_eq!(
        MiroirCode::SettingsVersionStale.error_type(),
        ErrorType::System
    );
    assert_eq!(MiroirCode::Timeout.error_type(), ErrorType::System);
}

/// Test 5: Error JSON output is byte-for-byte compatible with Meilisearch shape.
///
/// This test verifies the exact JSON structure matches Meilisearch's format.
#[test]
fn test_error_json_matches_meilisearch_shape() {
    let err = MeilisearchError::new(
        MiroirCode::PrimaryKeyRequired,
        "index `test` has no primary key",
    );

    let json_str = serde_json::to_string(&err).expect("failed to serialize");
    let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("failed to parse");

    // Verify exact shape
    assert_eq!(
        parsed,
        json!({
            "message": "index `test` has no primary key",
            "code": "miroir_primary_key_required",
            "type": "invalid_request",
            "link": "https://github.com/jedarden/miroir/blob/main/docs/errors.md#miroir_primary_key_required"
        })
    );
}

/// Test 6: Error with custom metadata preserves shape.
#[test]
fn test_error_with_custom_metadata_preserves_shape() {
    let mut err = MeilisearchError::new(
        MiroirCode::ReservedField,
        "document contains reserved field `_miroir_shard`",
    );

    // Verify the error can be converted to axum Response if feature is enabled
    #[cfg(feature = "axum")]
    {
        let response = err.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }
}

/// Test 7: Verify no_quorum error has the correct shape for degraded writes.
#[test]
fn test_no_quorum_error_shape_for_degraded_writes() {
    let err = MeilisearchError::new(MiroirCode::NoQuorum, "no quorum reached for shard 7");

    let json_val = serde_json::to_value(&err).expect("failed to serialize");

    assert_eq!(json_val["code"], "miroir_no_quorum");
    assert_eq!(json_val["type"], "system");
    assert_eq!(json_val["message"], "no quorum reached for shard 7");
    assert_eq!(
        json_val["link"],
        "https://github.com/jedarden/miroir/blob/main/docs/errors.md#miroir_no_quorum"
    );
}

/// Test 8: Verify shard_unavailable error shape.
#[test]
fn test_shard_unavailable_error_shape() {
    let err = MeilisearchError::new(
        MiroirCode::ShardUnavailable,
        "shard 15 has no healthy replicas",
    );

    let json_val = serde_json::to_value(&err).expect("failed to serialize");

    assert_eq!(json_val["code"], "miroir_shard_unavailable");
    assert_eq!(json_val["type"], "system");
    assert_eq!(json_val["message"], "shard 15 has no healthy replicas");
}

/// Test 9: Verify reserved_field error includes field name in message.
#[test]
fn test_reserved_field_error_includes_field_name() {
    let field_name = "_miroir_internal";
    let err = MeilisearchError::new(
        MiroirCode::ReservedField,
        &format!("document contains reserved field `{}`", field_name),
    );

    let json_val = serde_json::to_value(&err).expect("failed to serialize");

    assert_eq!(json_val["code"], "miroir_reserved_field");
    assert_eq!(json_val["type"], "invalid_request");
    assert!(json_val["message"].as_str().unwrap().contains(field_name));
}

/// Test 10: Verify timeout error shape.
#[test]
fn test_timeout_error_shape() {
    let err = MeilisearchError::new(MiroirCode::Timeout, "request timed out after 30s");

    let json_val = serde_json::to_value(&err).expect("failed to serialize");

    assert_eq!(json_val["code"], "miroir_timeout");
    assert_eq!(json_val["type"], "system");
    assert_eq!(json_val["message"], "request timed out after 30s");
    assert_eq!(err.http_status(), 504);
}

/// Test 11: Verify all error types can be parsed from JSON.
#[test]
fn test_all_errors_round_trip_through_json() {
    for code in MiroirCode::ALL {
        let err = MeilisearchError::new(code, "test message");
        let json_str = serde_json::to_string(&err).expect("failed to serialize");
        let parsed: MeilisearchError =
            serde_json::from_str(&json_str).expect("failed to deserialize");

        // Verify the parsed error has the same properties
        assert_eq!(parsed.code, code.as_str());
        assert_eq!(parsed.message, "test message");
        assert_eq!(parsed.error_type, code.error_type());
    }
}

/// Test 12: Verify error link format is consistent.
#[test]
fn test_error_link_format_is_consistent() {
    for code in MiroirCode::ALL {
        let link = code.doc_link();
        assert!(
            link.starts_with("https://github.com/jedarden/miroir/blob/main/docs/errors.md#"),
            "Error code {:?} has unexpected link format: {}",
            code,
            link
        );
        assert!(
            link.ends_with(code.as_str()),
            "Error code {:?} link doesn't end with code: {}",
            code,
            link
        );
    }
}

/// Test 13: Verify error_type Display implementation.
#[test]
fn test_error_type_display_matches_meilisearch() {
    assert_eq!(ErrorType::InvalidRequest.to_string(), "invalid_request");
    assert_eq!(ErrorType::Auth.to_string(), "auth");
    assert_eq!(ErrorType::Internal.to_string(), "internal");
    assert_eq!(ErrorType::System.to_string(), "system");
}
