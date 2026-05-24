//! Error format parity tests: Verify Miroir errors match Meilisearch shape byte-for-byte.
//!
//! These tests use the actual error responses from Miroir and verify they match
//! the Meilisearch error format specification:
//! ```json
//! {
//!   "message": "human readable message",
//!   "code": "error_code",
//!   "type": "invalid_request|auth|internal|system",
//!   "link": "https://docs.meilisearch.com/errors#..."
//! }
//! ```

use miroir_core::api_error::{MiroirCode, MeilisearchError};

#[test]
fn test_miroir_error_shape_matches_meilisearch() {
    // Test each MiroirCode variant
    const ALL_CODES: [MiroirCode; 14] = [
        MiroirCode::PrimaryKeyRequired,
        MiroirCode::NoQuorum,
        MiroirCode::ShardUnavailable,
        MiroirCode::ReservedField,
        MiroirCode::IdempotencyKeyReused,
        MiroirCode::SettingsVersionStale,
        MiroirCode::MultiAliasNotWritable,
        MiroirCode::JwtInvalid,
        MiroirCode::JwtScopeDenied,
        MiroirCode::InvalidAuth,
        MiroirCode::MissingCsrf,
        MiroirCode::CsrfMismatch,
        MiroirCode::IndexAlreadyExists,
        MiroirCode::Timeout,
    ];

    for code in ALL_CODES {
        let error = MeilisearchError::new(code, "test message");

        // Serialize to JSON
        let json = serde_json::to_value(&error).expect("Failed to serialize error");

        // Verify required fields exist
        assert!(json.get("message").is_some(), "Error must have 'message' field");
        assert!(json.get("code").is_some(), "Error must have 'code' field");
        assert!(json.get("type").is_some(), "Error must have 'type' field");
        assert!(json.get("link").is_some(), "Error must have 'link' field");

        // Verify field types
        assert!(json.get("message").unwrap().is_string(), "'message' must be a string");
        assert!(json.get("code").unwrap().is_string(), "'code' must be a string");
        assert!(json.get("type").unwrap().is_string(), "'type' must be a string");
        assert!(json.get("link").unwrap().is_string(), "'link' must be a string");

        // Verify type is one of the allowed values
        let error_type = json.get("type").unwrap().as_str().unwrap();
        assert!(
            matches!(error_type, "invalid_request" | "auth" | "internal" | "system"),
            "Error type must be one of: invalid_request, auth, internal, system"
        );

        // Verify code has miroir_ prefix
        let error_code = json.get("code").unwrap().as_str().unwrap();
        assert!(
            error_code.starts_with("miroir_"),
            "Miroir error codes must have 'miroir_' prefix, got: {}",
            error_code
        );

        // Verify link format
        let link = json.get("link").unwrap().as_str().unwrap();
        assert!(
            link.starts_with("https://github.com/jedarden/miroir"),
            "Link must point to Miroir docs"
        );
    }
}

#[test]
fn test_error_http_status_codes() {
    let test_cases = vec![
        (MiroirCode::PrimaryKeyRequired, 400, "invalid_request"),
        (MiroirCode::ReservedField, 400, "invalid_request"),
        (MiroirCode::JwtInvalid, 401, "auth"),
        (MiroirCode::InvalidAuth, 401, "auth"),
        (MiroirCode::MissingCsrf, 401, "auth"),
        (MiroirCode::JwtScopeDenied, 403, "auth"),
        (MiroirCode::CsrfMismatch, 403, "auth"),
        (MiroirCode::IdempotencyKeyReused, 409, "invalid_request"),
        (MiroirCode::MultiAliasNotWritable, 409, "invalid_request"),
        (MiroirCode::IndexAlreadyExists, 409, "invalid_request"),
        (MiroirCode::Timeout, 504, "system"),
        (MiroirCode::NoQuorum, 503, "system"),
        (MiroirCode::ShardUnavailable, 503, "system"),
        (MiroirCode::SettingsVersionStale, 503, "system"),
    ];

    for (code, expected_status, expected_type) in test_cases {
        let _error = MeilisearchError::new(code, "test message");

        assert_eq!(code.http_status(), expected_status,
            "HTTP status for {:?} should be {}, got {}",
            code, expected_status, code.http_status());

        assert_eq!(code.error_type().to_string(), expected_type,
            "Error type for {:?} should be {}, got {:?}",
            code, expected_type, code.error_type());
    }
}

#[test]
fn test_error_serialization_is_deterministic() {
    let error = MeilisearchError::new(MiroirCode::NoQuorum, "test message");

    // Serialize multiple times
    let json1 = serde_json::to_string(&error).unwrap();
    let json2 = serde_json::to_string(&error).unwrap();

    // Should be byte-identical
    assert_eq!(json1, json2, "Error serialization must be deterministic");

    // Parse and verify structure
    let parsed: serde_json::Value = serde_json::from_str(&json1).unwrap();

    assert_eq!(parsed.get("message").unwrap(), "test message");
    assert_eq!(parsed.get("code").unwrap(), "miroir_no_quorum");
    assert_eq!(parsed.get("type").unwrap(), "system");
}

#[test]
fn test_forwarded_meilisearch_error_parsing() {
    // Test that we can parse forwarded Meilisearch errors
    let meilisearch_error_json = r#"{
        "message": "Index not found",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    }"#;

    let parsed = MeilisearchError::forwarded(meilisearch_error_json);

    assert!(parsed.is_some(), "Should successfully parse Meilisearch error");
    let error = parsed.unwrap();

    assert_eq!(error.message, "Index not found");
    assert_eq!(error.code, "index_not_found");
    assert_eq!(error.error_type.to_string(), "invalid_request");
}

#[test]
fn test_invalid_json_is_not_parsed_as_meilisearch_error() {
    let invalid_json = r#"{"not": "an error"}"#;

    let parsed = MeilisearchError::forwarded(invalid_json);

    assert!(parsed.is_none(), "Should not parse invalid JSON as Meilisearch error");
}

#[test]
fn test_error_code_roundtrip() {
    // Test that code strings can be parsed back to MiroirCode
    const ALL_CODES: [MiroirCode; 14] = [
        MiroirCode::PrimaryKeyRequired,
        MiroirCode::NoQuorum,
        MiroirCode::ShardUnavailable,
        MiroirCode::ReservedField,
        MiroirCode::IdempotencyKeyReused,
        MiroirCode::SettingsVersionStale,
        MiroirCode::MultiAliasNotWritable,
        MiroirCode::JwtInvalid,
        MiroirCode::JwtScopeDenied,
        MiroirCode::InvalidAuth,
        MiroirCode::MissingCsrf,
        MiroirCode::CsrfMismatch,
        MiroirCode::IndexAlreadyExists,
        MiroirCode::Timeout,
    ];

    for code in ALL_CODES {
        let code_str = code.as_str();
        let parsed = MiroirCode::from_code_str(code_str);

        assert_eq!(parsed, Some(code), "Code '{}' should roundtrip to {:?}", code_str, code);
    }
}

#[test]
fn test_unknown_code_returns_none() {
    assert!(MiroirCode::from_code_str("unknown_code").is_none());
    assert!(MiroirCode::from_code_str("miroir_unknown").is_none());
    assert!(MiroirCode::from_code_str("").is_none());
}

#[test]
fn test_miroir_primary_key_required_error() {
    let error = MeilisearchError::new(MiroirCode::PrimaryKeyRequired, "primary key is required");

    let json = serde_json::to_value(&error).unwrap();

    assert_eq!(json.get("code").unwrap(), "miroir_primary_key_required");
    assert_eq!(json.get("type").unwrap(), "invalid_request");
    assert_eq!(json.get("message").unwrap(), "primary key is required");
}

#[test]
fn test_miroir_no_quorum_error() {
    let error = MeilisearchError::new(MiroirCode::NoQuorum, "insufficient nodes available");

    let json = serde_json::to_value(&error).unwrap();

    assert_eq!(json.get("code").unwrap(), "miroir_no_quorum");
    assert_eq!(json.get("type").unwrap(), "system");
    assert_eq!(json.get("message").unwrap(), "insufficient nodes available");
}

#[test]
fn test_miroir_shard_unavailable_error() {
    let error = MeilisearchError::new(MiroirCode::ShardUnavailable, "shard 5 is unavailable");

    let json = serde_json::to_value(&error).unwrap();

    assert_eq!(json.get("code").unwrap(), "miroir_shard_unavailable");
    assert_eq!(json.get("type").unwrap(), "system");
}

#[test]
fn test_miroir_reserved_field_error() {
    let error = MeilisearchError::new(
        MiroirCode::ReservedField,
        "field '_miroir_internal' is reserved"
    );

    let json = serde_json::to_value(&error).unwrap();

    assert_eq!(json.get("code").unwrap(), "miroir_reserved_field");
    assert_eq!(json.get("type").unwrap(), "invalid_request");
}

#[test]
fn test_miroir_timeout_error() {
    let error = MeilisearchError::new(MiroirCode::Timeout, "operation timed out");

    let json = serde_json::to_value(&error).unwrap();

    assert_eq!(json.get("code").unwrap(), "miroir_timeout");
    assert_eq!(json.get("type").unwrap(), "system");
    assert_eq!(error.http_status(), 504);
}

#[test]
fn test_error_message_preserves_content() {
    let messages = vec![
        "simple message",
        "message with: special chars",
        "message with\nnewlines",
        "message with \"quotes\"",
        "message with unicode: 🎉",
    ];

    for msg in messages {
        let error = MeilisearchError::new(MiroirCode::NoQuorum, msg);
        assert_eq!(error.message, msg);

        let json = serde_json::to_value(&error).unwrap();
        assert_eq!(json.get("message").unwrap(), msg);
    }
}

#[test]
fn test_all_miroir_codes_are_documented() {
    // Verify all error codes have proper documentation links
    const ALL_CODES: [MiroirCode; 14] = [
        MiroirCode::PrimaryKeyRequired,
        MiroirCode::NoQuorum,
        MiroirCode::ShardUnavailable,
        MiroirCode::ReservedField,
        MiroirCode::IdempotencyKeyReused,
        MiroirCode::SettingsVersionStale,
        MiroirCode::MultiAliasNotWritable,
        MiroirCode::JwtInvalid,
        MiroirCode::JwtScopeDenied,
        MiroirCode::InvalidAuth,
        MiroirCode::MissingCsrf,
        MiroirCode::CsrfMismatch,
        MiroirCode::IndexAlreadyExists,
        MiroirCode::Timeout,
    ];

    for code in ALL_CODES {
        let error = MeilisearchError::new(code, "test");
        let json = serde_json::to_value(&error).unwrap();

        let link = json.get("link").unwrap().as_str().unwrap();

        // Verify link contains the error code
        assert!(
            link.contains(&format!("#{}", code.as_str())),
            "Documentation link for {:?} should reference the error code: {}",
            code, link
        );

        // Verify link is a valid URL
        assert!(link.starts_with("https://"));
    }
}

#[test]
fn test_error_response_includes_all_required_fields() {
    // Create an error and verify it has all required fields
    let error = MeilisearchError::new(MiroirCode::NoQuorum, "test message");

    // Verify struct has all fields
    assert!(!error.message.is_empty());
    assert!(!error.code.is_empty());
    assert!(error.link.is_some());

    // Verify serialized JSON has all fields
    let json = serde_json::to_value(&error).unwrap();

    let obj = json.as_object().unwrap();
    assert!(obj.contains_key("message"));
    assert!(obj.contains_key("code"));
    assert!(obj.contains_key("type"));
    assert!(obj.contains_key("link"));

    // Verify link is not null
    assert!(obj.get("link").unwrap().is_string());
}

#[test]
fn test_error_type_classification() {
    // Test that error types are correctly classified

    // invalid_request errors
    let invalid_request_codes = vec![
        MiroirCode::PrimaryKeyRequired,
        MiroirCode::ReservedField,
        MiroirCode::IdempotencyKeyReused,
        MiroirCode::MultiAliasNotWritable,
        MiroirCode::IndexAlreadyExists,
    ];

    for code in invalid_request_codes {
        assert_eq!(code.error_type(), miroir_core::api_error::ErrorType::InvalidRequest);
        assert_eq!(code.error_type().to_string(), "invalid_request");
    }

    // auth errors
    let auth_codes = vec![
        MiroirCode::JwtInvalid,
        MiroirCode::JwtScopeDenied,
        MiroirCode::InvalidAuth,
        MiroirCode::MissingCsrf,
        MiroirCode::CsrfMismatch,
    ];

    for code in auth_codes {
        assert_eq!(code.error_type(), miroir_core::api_error::ErrorType::Auth);
        assert_eq!(code.error_type().to_string(), "auth");
    }

    // system errors
    let system_codes = vec![
        MiroirCode::NoQuorum,
        MiroirCode::ShardUnavailable,
        MiroirCode::SettingsVersionStale,
        MiroirCode::Timeout,
    ];

    for code in system_codes {
        assert_eq!(code.error_type(), miroir_core::api_error::ErrorType::System);
        assert_eq!(code.error_type().to_string(), "system");
    }
}

#[test]
fn test_http_status_matches_error_type() {
    // Verify HTTP status codes match error type conventions

    // 4xx errors should be InvalidRequest or Auth
    let client_error_codes = vec![
        (MiroirCode::PrimaryKeyRequired, 400),
        (MiroirCode::ReservedField, 400),
        (MiroirCode::JwtInvalid, 401),
        (MiroirCode::InvalidAuth, 401),
        (MiroirCode::MissingCsrf, 401),
        (MiroirCode::JwtScopeDenied, 403),
        (MiroirCode::CsrfMismatch, 403),
        (MiroirCode::IdempotencyKeyReused, 409),
        (MiroirCode::IndexAlreadyExists, 409),
    ];

    for (code, expected_status) in client_error_codes {
        let status = code.http_status();
        assert!(status >= 400 && status < 500,
            "Client error {:?} should have 4xx status, got {}",
            code, status);
        assert_eq!(status, expected_status);
    }

    // 5xx errors should be System
    let server_error_codes = vec![
        (MiroirCode::NoQuorum, 503),
        (MiroirCode::ShardUnavailable, 503),
        (MiroirCode::SettingsVersionStale, 503),
        (MiroirCode::Timeout, 504),
    ];

    for (code, expected_status) in server_error_codes {
        let status = code.http_status();
        assert!(status >= 500 && status < 600,
            "Server error {:?} should have 5xx status, got {}",
            code, status);
        assert_eq!(status, expected_status);
    }
}
