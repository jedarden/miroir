//! Meilisearch-compatible API error shape and Miroir-specific error codes.
//!
//! All errors use the Meilisearch error shape:
//! ```json
//! {"message": "...", "code": "...", "type": "invalid_request", "link": "..."}
//! ```
//!
//! Miroir-specific codes live under the `miroir_` prefix so that existing
//! Meilisearch SDKs' "unknown error" branches handle them safely.

use serde::Serialize;

#[cfg(feature = "axum")]
use axum::{http::{StatusCode, header}, response::{IntoResponse, Response}};

/// Error type categories matching Meilisearch's classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorType {
    InvalidRequest,
    Auth,
    Internal,
    System,
}

/// Miroir-specific error codes with associated metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiroirCode {
    PrimaryKeyRequired,
    NoQuorum,
    ShardUnavailable,
    ReservedField,
    IdempotencyKeyReused,
    SettingsVersionStale,
    MultiAliasNotWritable,
    JwtInvalid,
    JwtScopeDenied,
    InvalidAuth,
    MissingCsrf,
    CsrfMismatch,
}

impl MiroirCode {
    /// All variants, used for iteration in tests.
    #[cfg(test)]
    const ALL: [MiroirCode; 12] = [
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
    ];

    /// Returns the error code string (e.g., `"miroir_no_quorum"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PrimaryKeyRequired => "miroir_primary_key_required",
            Self::NoQuorum => "miroir_no_quorum",
            Self::ShardUnavailable => "miroir_shard_unavailable",
            Self::ReservedField => "miroir_reserved_field",
            Self::IdempotencyKeyReused => "miroir_idempotency_key_reused",
            Self::SettingsVersionStale => "miroir_settings_version_stale",
            Self::MultiAliasNotWritable => "miroir_multi_alias_not_writable",
            Self::JwtInvalid => "miroir_jwt_invalid",
            Self::JwtScopeDenied => "miroir_jwt_scope_denied",
            Self::InvalidAuth => "miroir_invalid_auth",
            Self::MissingCsrf => "miroir_missing_csrf",
            Self::CsrfMismatch => "miroir_csrf_mismatch",
        }
    }

    /// Returns the Meilisearch-compatible error type category.
    pub fn error_type(&self) -> ErrorType {
        match self {
            Self::PrimaryKeyRequired
            | Self::ReservedField
            | Self::IdempotencyKeyReused
            | Self::MultiAliasNotWritable => ErrorType::InvalidRequest,

            Self::JwtInvalid | Self::JwtScopeDenied | Self::InvalidAuth | Self::MissingCsrf | Self::CsrfMismatch => ErrorType::Auth,

            Self::NoQuorum | Self::ShardUnavailable | Self::SettingsVersionStale => {
                ErrorType::System
            }
        }
    }

    /// Returns the HTTP status code for this error.
    pub fn http_status(&self) -> u16 {
        match self {
            Self::PrimaryKeyRequired | Self::ReservedField => 400,
            Self::JwtInvalid | Self::InvalidAuth | Self::MissingCsrf => 401,
            Self::JwtScopeDenied | Self::CsrfMismatch => 403,
            Self::IdempotencyKeyReused | Self::MultiAliasNotWritable => 409,
            Self::NoQuorum | Self::ShardUnavailable | Self::SettingsVersionStale => 503,
        }
    }

    /// Generates the documentation link for this error code.
    pub fn doc_link(&self) -> String {
        format!(
            "https://github.com/jedarden/miroir/blob/main/docs/errors.md#{}",
            self.as_str()
        )
    }

    /// Parse a code string back to a [`MiroirCode`].
    pub fn from_code_str(s: &str) -> Option<Self> {
        match s {
            "miroir_primary_key_required" => Some(Self::PrimaryKeyRequired),
            "miroir_no_quorum" => Some(Self::NoQuorum),
            "miroir_shard_unavailable" => Some(Self::ShardUnavailable),
            "miroir_reserved_field" => Some(Self::ReservedField),
            "miroir_idempotency_key_reused" => Some(Self::IdempotencyKeyReused),
            "miroir_settings_version_stale" => Some(Self::SettingsVersionStale),
            "miroir_multi_alias_not_writable" => Some(Self::MultiAliasNotWritable),
            "miroir_jwt_invalid" => Some(Self::JwtInvalid),
            "miroir_jwt_scope_denied" => Some(Self::JwtScopeDenied),
            "miroir_invalid_auth" => Some(Self::InvalidAuth),
            "miroir_missing_csrf" => Some(Self::MissingCsrf),
            "miroir_csrf_mismatch" => Some(Self::CsrfMismatch),
            _ => None,
        }
    }
}

/// Meilisearch-compatible error response shape.
///
/// Both Miroir-specific and forwarded Meilisearch-native errors use this shape
/// so that existing SDK error handling branches remain functional.
#[derive(Debug, Clone, thiserror::Error, Serialize)]
#[error("{message}")]
pub struct MeilisearchError {
    pub message: String,
    pub code: String,
    #[serde(rename = "type")]
    pub error_type: ErrorType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
}

impl MeilisearchError {
    /// Create a new miroir-specific error with auto-generated doc link.
    pub fn new(code: MiroirCode, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: code.as_str().to_string(),
            error_type: code.error_type(),
            link: Some(code.doc_link()),
        }
    }

    /// Create from a forwarded Meilisearch node error response body.
    ///
    /// Returns `None` if the body cannot be parsed as a Meilisearch error shape.
    /// When forwarding, the caller should use the original HTTP status code from
    /// the node response rather than deriving it from the error type.
    pub fn forwarded(json: &str) -> Option<Self> {
        #[derive(serde::Deserialize)]
        struct Raw {
            message: String,
            code: String,
            #[serde(rename = "type")]
            error_type: ErrorType,
            link: Option<String>,
        }

        let raw: Raw = serde_json::from_str(json).ok()?;
        Some(Self {
            message: raw.message,
            code: raw.code,
            error_type: raw.error_type,
            link: raw.link,
        })
    }

    /// Derive the HTTP status code for a miroir-generated error.
    ///
    /// For forwarded Meilisearch errors, prefer using the original node HTTP
    /// status code instead.
    pub fn http_status(&self) -> u16 {
        MiroirCode::from_code_str(&self.code)
            .map(|c| c.http_status())
            .unwrap_or_else(|| match self.error_type {
                ErrorType::InvalidRequest => 400,
                ErrorType::Auth => 401,
                ErrorType::Internal => 500,
                ErrorType::System => 503,
            })
    }
}

#[cfg(feature = "axum")]
impl IntoResponse for MeilisearchError {
    fn into_response(self) -> Response {
        let status = self.http_status();

        let body = serde_json::to_string(&self).unwrap_or_else(|_| {
            r#"{"message":"internal error","code":"internal","type":"internal"}"#.to_string()
        });

        (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Per-code JSON shape tests ------------------------------------------------

    #[test]
    fn miroir_primary_key_required_shape() {
        let err = MeilisearchError::new(
            MiroirCode::PrimaryKeyRequired,
            "primary key required for index `movies`",
        );
        let json: serde_json::Value = serde_json::to_value(&err).unwrap();

        assert_eq!(json["code"], "miroir_primary_key_required");
        assert_eq!(json["message"], "primary key required for index `movies`");
        assert_eq!(json["type"], "invalid_request");
        assert!(json["link"].as_str().unwrap().contains("miroir_primary_key_required"));
        assert_eq!(err.http_status(), 400);
    }

    #[test]
    fn miroir_no_quorum_shape() {
        let err = MeilisearchError::new(
            MiroirCode::NoQuorum,
            "no replica group met quorum for shard 3",
        );
        let json: serde_json::Value = serde_json::to_value(&err).unwrap();

        assert_eq!(json["code"], "miroir_no_quorum");
        assert_eq!(json["type"], "system");
        assert!(json["link"].as_str().unwrap().contains("miroir_no_quorum"));
        assert_eq!(err.http_status(), 503);
    }

    #[test]
    fn miroir_shard_unavailable_shape() {
        let err = MeilisearchError::new(
            MiroirCode::ShardUnavailable,
            "shard 7 is fully unavailable",
        );
        let json: serde_json::Value = serde_json::to_value(&err).unwrap();

        assert_eq!(json["code"], "miroir_shard_unavailable");
        assert_eq!(json["type"], "system");
        assert!(json["link"].as_str().unwrap().contains("miroir_shard_unavailable"));
        assert_eq!(err.http_status(), 503);
    }

    #[test]
    fn miroir_reserved_field_shape() {
        let err = MeilisearchError::new(
            MiroirCode::ReservedField,
            "document contains reserved field `_miroir_shard`",
        );
        let json: serde_json::Value = serde_json::to_value(&err).unwrap();

        assert_eq!(json["code"], "miroir_reserved_field");
        assert_eq!(json["type"], "invalid_request");
        assert!(json["link"].as_str().unwrap().contains("miroir_reserved_field"));
        assert_eq!(err.http_status(), 400);
    }

    #[test]
    fn miroir_idempotency_key_reused_shape() {
        let err = MeilisearchError::new(
            MiroirCode::IdempotencyKeyReused,
            "Idempotency-Key already used with a different request body",
        );
        let json: serde_json::Value = serde_json::to_value(&err).unwrap();

        assert_eq!(json["code"], "miroir_idempotency_key_reused");
        assert_eq!(json["type"], "invalid_request");
        assert!(json["link"].as_str().unwrap().contains("miroir_idempotency_key_reused"));
        assert_eq!(err.http_status(), 409);
    }

    #[test]
    fn miroir_settings_version_stale_shape() {
        let err = MeilisearchError::new(
            MiroirCode::SettingsVersionStale,
            "no covering set after excluding stale nodes",
        );
        let json: serde_json::Value = serde_json::to_value(&err).unwrap();

        assert_eq!(json["code"], "miroir_settings_version_stale");
        assert_eq!(json["type"], "system");
        assert!(json["link"].as_str().unwrap().contains("miroir_settings_version_stale"));
        assert_eq!(err.http_status(), 503);
    }

    #[test]
    fn miroir_multi_alias_not_writable_shape() {
        let err = MeilisearchError::new(
            MiroirCode::MultiAliasNotWritable,
            "write to multi-target ILM alias `all-indices` is not allowed",
        );
        let json: serde_json::Value = serde_json::to_value(&err).unwrap();

        assert_eq!(json["code"], "miroir_multi_alias_not_writable");
        assert_eq!(json["type"], "invalid_request");
        assert!(json["link"].as_str().unwrap().contains("miroir_multi_alias_not_writable"));
        assert_eq!(err.http_status(), 409);
    }

    #[test]
    fn miroir_jwt_invalid_shape() {
        let err = MeilisearchError::new(
            MiroirCode::JwtInvalid,
            "JWT signature verification failed",
        );
        let json: serde_json::Value = serde_json::to_value(&err).unwrap();

        assert_eq!(json["code"], "miroir_jwt_invalid");
        assert_eq!(json["type"], "auth");
        assert!(json["link"].as_str().unwrap().contains("miroir_jwt_invalid"));
        assert_eq!(err.http_status(), 401);
    }

    #[test]
    fn miroir_jwt_scope_denied_shape() {
        let err = MeilisearchError::new(
            MiroirCode::JwtScopeDenied,
            "token scope does not include `documents.add` for index `movies`",
        );
        let json: serde_json::Value = serde_json::to_value(&err).unwrap();

        assert_eq!(json["code"], "miroir_jwt_scope_denied");
        assert_eq!(json["type"], "auth");
        assert!(json["link"].as_str().unwrap().contains("miroir_jwt_scope_denied"));
        assert_eq!(err.http_status(), 403);
    }

    #[test]
    fn miroir_invalid_auth_shape() {
        let err = MeilisearchError::new(
            MiroirCode::InvalidAuth,
            "credentials did not match any expected key",
        );
        let json: serde_json::Value = serde_json::to_value(&err).unwrap();

        assert_eq!(json["code"], "miroir_invalid_auth");
        assert_eq!(json["type"], "auth");
        assert!(json["link"].as_str().unwrap().contains("miroir_invalid_auth"));
        assert_eq!(err.http_status(), 401);
    }

    #[test]
    fn miroir_missing_csrf_shape() {
        let err = MeilisearchError::new(
            MiroirCode::MissingCsrf,
            "CSRF token is required for state-changing requests.",
        );
        let json: serde_json::Value = serde_json::to_value(&err).unwrap();

        assert_eq!(json["code"], "miroir_missing_csrf");
        assert_eq!(json["type"], "auth");
        assert!(json["link"].as_str().unwrap().contains("miroir_missing_csrf"));
        assert_eq!(err.http_status(), 401);
    }

    #[test]
    fn miroir_csrf_mismatch_shape() {
        let err = MeilisearchError::new(
            MiroirCode::CsrfMismatch,
            "CSRF token does not match the session token.",
        );
        let json: serde_json::Value = serde_json::to_value(&err).unwrap();

        assert_eq!(json["code"], "miroir_csrf_mismatch");
        assert_eq!(json["type"], "auth");
        assert!(json["link"].as_str().unwrap().contains("miroir_csrf_mismatch"));
        assert_eq!(err.http_status(), 403);
    }

    // -- Meilisearch-native error forwarding --------------------------------------

    #[test]
    fn forwarded_meilisearch_error_preserved_verbatim() {
        let node_body = r#"{
            "message": "Index `movies` not found.",
            "code": "index_not_found",
            "type": "invalid_request",
            "link": "https://docs.meilisearch.com/errors#index_not_found"
        }"#;

        let err = MeilisearchError::forwarded(node_body).unwrap();
        assert_eq!(err.message, "Index `movies` not found.");
        assert_eq!(err.code, "index_not_found");
        assert_eq!(err.error_type, ErrorType::InvalidRequest);
        assert_eq!(
            err.link.as_deref(),
            Some("https://docs.meilisearch.com/errors#index_not_found")
        );
    }

    #[test]
    fn forwarded_meilisearch_error_without_link() {
        let node_body = r#"{
            "message": "bad request",
            "code": "bad_request",
            "type": "invalid_request"
        }"#;

        let err = MeilisearchError::forwarded(node_body).unwrap();
        assert_eq!(err.code, "bad_request");
        assert!(err.link.is_none());
    }

    #[test]
    fn forwarded_invalid_json_returns_none() {
        assert!(MeilisearchError::forwarded("not json").is_none());
        assert!(MeilisearchError::forwarded("{}").is_none());
    }

    #[test]
    fn forwarded_error_serializes_back_to_same_shape() {
        let node_body = r#"{
            "message": "Index `movies` not found.",
            "code": "index_not_found",
            "type": "invalid_request",
            "link": "https://docs.meilisearch.com/errors#index_not_found"
        }"#;

        let err = MeilisearchError::forwarded(node_body).unwrap();
        let roundtripped = serde_json::to_string(&err).unwrap();
        let original: serde_json::Value = serde_json::from_str(node_body).unwrap();
        let result: serde_json::Value = serde_json::from_str(&roundtripped).unwrap();

        assert_eq!(original, result);
    }

    // -- HTTP status code mapping -------------------------------------------------

    #[test]
    fn all_codes_map_to_correct_http_status() {
        let expected: Vec<(MiroirCode, u16)> = vec![
            (MiroirCode::PrimaryKeyRequired, 400),
            (MiroirCode::ReservedField, 400),
            (MiroirCode::JwtInvalid, 401),
            (MiroirCode::InvalidAuth, 401),
            (MiroirCode::MissingCsrf, 401),
            (MiroirCode::JwtScopeDenied, 403),
            (MiroirCode::CsrfMismatch, 403),
            (MiroirCode::IdempotencyKeyReused, 409),
            (MiroirCode::MultiAliasNotWritable, 409),
            (MiroirCode::NoQuorum, 503),
            (MiroirCode::ShardUnavailable, 503),
            (MiroirCode::SettingsVersionStale, 503),
        ];

        for (code, status) in expected {
            assert_eq!(
                code.http_status(),
                status,
                "{:?} should map to {}",
                code,
                status
            );
        }
    }

    #[test]
    fn error_http_status_matches_code_status() {
        for variant in MiroirCode::ALL {
            let err = MeilisearchError::new(variant, "test");
            assert_eq!(
                err.http_status(),
                variant.http_status(),
                "MeilisearchError::http_status mismatch for {:?}",
                variant
            );
        }
    }

    // -- Display impl via thiserror -----------------------------------------------

    #[test]
    fn display_shows_message() {
        let err = MeilisearchError::new(MiroirCode::NoQuorum, "no quorum reached");
        assert_eq!(format!("{}", err), "no quorum reached");
    }

    // -- Round-trip JSON serialization --------------------------------------------

    #[test]
    fn roundtrip_miroir_error() {
        let original = MeilisearchError::new(MiroirCode::ShardUnavailable, "shard 5 down");
        let json = serde_json::to_string(&original).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["message"], "shard 5 down");
        assert_eq!(parsed["code"], "miroir_shard_unavailable");
        assert_eq!(parsed["type"], "system");
        assert!(parsed["link"].as_str().unwrap().contains("miroir_shard_unavailable"));
    }

    // -- Code string round-trip ---------------------------------------------------

    #[test]
    fn code_string_roundtrip() {
        for variant in MiroirCode::ALL {
            let s = variant.as_str();
            assert!(s.starts_with("miroir_"), "{} doesn't start with miroir_", s);
            assert_eq!(MiroirCode::from_code_str(s), Some(variant));
        }
    }

    #[test]
    fn from_code_str_unknown_returns_none() {
        assert_eq!(MiroirCode::from_code_str("index_not_found"), None);
        assert_eq!(MiroirCode::from_code_str(""), None);
        assert_eq!(MiroirCode::from_code_str("miroir_unknown"), None);
    }

    // -- Error type coverage: every type has at least one code ---------------------

    #[test]
    fn all_error_types_covered() {
        let types: std::collections::HashSet<ErrorType> =
            MiroirCode::ALL.iter().map(|c| c.error_type()).collect();
        assert!(types.contains(&ErrorType::InvalidRequest));
        assert!(types.contains(&ErrorType::Auth));
        assert!(types.contains(&ErrorType::System));
    }
}
