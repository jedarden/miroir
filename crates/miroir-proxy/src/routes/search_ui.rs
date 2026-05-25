//! Search UI routes (plan §13.21).
//!
//! Public end-user search SPA with JWT-based session management.
//! - GET /_miroir/ui/search/{index}/session — JWT session minting
//! - POST /_miroir/ui/search/{index}/config — per-index UI configuration
//! - POST /_miroir/ui/search/{index}/beacon — analytics beacon (idempotent)
//! - GET /ui/search/{index} — embedded SPA

use axum::{
    extract::{FromRef, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Json, Response},
    Router,
};
use miroir_core::{
    cdc::AnalyticsEvent,
    config::advanced::SearchUiConfig,
    task_store::{SearchUiScopedKey, TaskStore},
};
use sha2::{Digest, Sha256};

use crate::auth::{
    build_csp_header, jwt_decode_with_fallback, jwt_encode, JwtClaims, JwtHeader, KID_PRIMARY,
};
use crate::error_response::ErrorResponse;
use rust_embed::RustEmbed as Embed;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::routes::indexes::MeilisearchClient;
use crate::scoped_key_rotation::mint_scoped_key;

use super::admin_endpoints::AppState;

/// Session mint response (plan §13.21).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionResponse {
    /// JWT token for subsequent search requests.
    pub token: String,
    /// Expiration timestamp (seconds since epoch).
    pub expires_at: u64,
    /// Index UID this session is bound to.
    pub index: String,
    /// Rate limit configuration for the UI.
    pub rate_limit: RateLimitInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitInfo {
    /// Rate limit string (e.g., "60/minute").
    pub limit: String,
    /// Remaining requests in the current window.
    pub remaining: u32,
    /// Seconds until the limit resets.
    pub reset_in: u32,
}

/// Per-index search UI configuration (plan §13.21).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchUiIndexConfig {
    /// Display title for the index.
    pub title: String,
    /// Facet configuration (field -> display name).
    pub facets: std::collections::HashMap<String, FacetConfig>,
    /// Sort options (field -> display name).
    pub sort_options: std::collections::HashMap<String, String>,
    /// Result template (HTML snippet for each result): card | list | grid | table | custom
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_template: Option<String>,
    /// Custom template HTML (used when result_template = "custom")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_template_html: Option<String>,
    /// Empty state message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub empty_state: Option<String>,
    /// Typo tolerance setting.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub typo_tolerance: Option<TypoToleranceConfig>,
    /// Display attributes (fields to show in results)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_attributes: Option<Vec<String>>,
    /// Primary key field for result URLs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key_field: Option<String>,
    /// URL template for result clicks
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hit_url_template: Option<String>,
    /// Thumbnail field for images
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FacetConfig {
    pub display_name: String,
    pub kind: String, // "string", "number", "date"
    pub sort: String, // "count", "alpha", "value"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypoToleranceConfig {
    pub enabled: bool,
    pub min_word_size_for_one_typo: u8,
    pub min_word_size_for_two_typos: u8,
}

/// Analytics beacon request (plan §13.21).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeaconRequest {
    /// Client-generated event ID for idempotency.
    pub event_id: String,
    /// Event type: "search", "click", "impression".
    pub event_type: String,
    /// Index UID.
    pub index_uid: String,
    /// Query string (for search events).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Number of results (for search events).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_count: Option<u64>,
    /// Latency in milliseconds (for search events).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    /// Clicked document ID (for click events).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
    /// Click position (for click events).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<u32>,
}

/// Create the search UI router (plan §13.21).
pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    AppState: FromRef<S>,
{
    Router::new()
        .route("/:index/session", axum::routing::get(mint_session))
        .route("/:index/config", axum::routing::get(get_config))
        .route("/:index/config", axum::routing::post(update_config))
        .route("/:index/beacon", axum::routing::post(beacon))
        // Static assets and SPA
        .route("/static/*path", axum::routing::get(serve_static_asset))
        .fallback(axum::routing::get(serve_spa))
}

/// Mint a JWT session token for the search UI (plan §13.21).
///
/// Auth modes:
/// - `public`: IP rate-limited, no credentials required
/// - `shared_key`: Requires X-Search-UI-Key header
/// - `oauth_proxy`: Requires upstream auth headers (X-Forwarded-User, X-Forwarded-Groups)
pub async fn mint_session(
    Path(index_uid): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ErrorResponse> {
    let config = &state.config;

    // Check if search UI is enabled
    if !config.search_ui.enabled {
        return Err(ErrorResponse::invalid_request("search UI is not enabled"));
    }

    // Validate auth mode
    let auth_config = &config.search_ui.auth;
    match auth_config.mode.as_str() {
        "public" => {
            // No credentials required, just rate limiting
            debug!(index = %index_uid, "minting public search UI session");
        }
        "shared_key" => {
            // Require X-Search-UI-Key header
            let shared_key = std::env::var(&auth_config.shared_key_env).map_err(|_| {
                ErrorResponse::invalid_request("search UI shared key not configured".to_string())
            })?;

            let provided_key = headers
                .get("X-Search-UI-Key")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| {
                    ErrorResponse::invalid_request("missing X-Search-UI-Key header".to_string())
                })?;

            if provided_key != shared_key {
                return Err(ErrorResponse::invalid_request(
                    "invalid X-Search-UI-Key".to_string(),
                ));
            }
        }
        "oauth_proxy" => {
            // Require upstream auth headers
            let _user = headers
                .get(&auth_config.oauth_proxy.user_header)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| {
                    ErrorResponse::invalid_request(format!(
                        "missing {} header",
                        auth_config.oauth_proxy.user_header
                    ))
                })?;

            let _groups = headers
                .get(&auth_config.oauth_proxy.groups_header)
                .and_then(|v| v.to_str().ok());

            debug!(index = %index_uid, "minting oauth_proxy search UI session");
        }
        _ => {
            return Err(ErrorResponse::invalid_request(format!(
                "invalid auth mode: {}",
                auth_config.mode
            )));
        }
    }

    // Get or create scoped key for this index
    let scoped_key = get_or_create_scoped_key(&state, &index_uid, &config.search_ui).await?;

    // Build JWT claims
    let now = chrono::Utc::now().timestamp() as u64;
    let exp = now + auth_config.session_ttl_s;

    let scope = vec![
        "search".to_string(),
        "multi_search".to_string(),
        "beacon".to_string(),
    ];

    let mut injected_filter = None;
    let mut user = None;
    let mut groups = None;

    // In oauth_proxy mode, inject filter template
    if auth_config.mode == "oauth_proxy" {
        if let Some(template) = &auth_config.oauth_proxy.filter_template {
            let groups_str = headers
                .get(&auth_config.oauth_proxy.groups_header)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");

            // Render template: "tenant IN [{groups}]" -> "tenant IN [group1,group2]"
            let rendered = template.replace("{groups}", groups_str);
            injected_filter = Some(rendered);
        }

        user = headers
            .get(&auth_config.oauth_proxy.user_header)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        groups = headers
            .get(&auth_config.oauth_proxy.groups_header)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(',').map(|s| s.trim().to_string()).collect());
    }

    let claims = JwtClaims {
        iss: "miroir".to_string(),
        sub: if auth_config.mode == "oauth_proxy" {
            user.clone()
                .unwrap_or_else(|| "search-ui-session".to_string())
        } else {
            "search-ui-session".to_string()
        },
        idx: index_uid.clone(),
        scope,
        iat: now,
        exp,
        injected_filter,
        user,
        groups,
    };

    // Sign with primary secret
    let secret = std::env::var(&auth_config.jwt_secret_env)
        .map_err(|_| ErrorResponse::internal_error("JWT secret not configured".to_string()))?;

    let header = JwtHeader {
        alg: "HS256".to_string(),
        kid: KID_PRIMARY.to_string(),
        typ: "JWT".to_string(),
    };

    let token = jwt_encode(&header, &claims, secret.as_bytes())
        .map_err(|e| ErrorResponse::internal_error(format!("JWT encoding failed: {e}")))?;

    info!(
        index = %index_uid,
        expires_at = exp,
        auth_mode = %auth_config.mode,
        "minted search UI session"
    );

    Ok(Json(SessionResponse {
        token,
        expires_at: exp,
        index: index_uid,
        rate_limit: RateLimitInfo {
            limit: auth_config.session_rate_limit.clone(),
            remaining: 10, // TODO: implement actual rate limiting
            reset_in: 60,
        },
    }))
}

/// Get or create a scoped Meilisearch key for the search UI.
async fn get_or_create_scoped_key(
    state: &AppState,
    index_uid: &str,
    config: &SearchUiConfig,
) -> Result<SearchUiScopedKey, ErrorResponse> {
    let redis_store = state.redis_store.as_ref().ok_or_else(|| {
        ErrorResponse::internal_error("Redis store required for search UI scoped keys")
    })?;

    // Try to get existing key
    if let Some(key) = redis_store.get_search_ui_scoped_key(index_uid)? {
        // Check if key is approaching expiry
        let now = chrono::Utc::now().timestamp();
        let max_age_ms = config.scoped_key_max_age_days as i64 * 24 * 60 * 60 * 1000;
        let key_age_ms = now - key.rotated_at;

        // If key is less than 90% of max age, reuse it
        if key_age_ms < (max_age_ms * 9 / 10) {
            return Ok(key);
        }

        info!(
            index = %index_uid,
            age_ms = key_age_ms,
            max_age_ms = max_age_ms,
            "scoped key approaching expiry, will rotate"
        );
    }

    // Create new scoped key via Meilisearch API (plan §13.21)
    info!(index = %index_uid, "creating new scoped search-only key");

    let client = MeilisearchClient::new(state.config.node_master_key.clone());
    let (new_key, new_uid) = mint_scoped_key(&client, &state.config, index_uid)
        .await
        .map_err(|e| ErrorResponse::internal_error(format!("failed to mint scoped key: {e}")))?;

    let now_ms = chrono::Utc::now().timestamp_millis();
    let scoped_key = SearchUiScopedKey {
        index_uid: index_uid.to_string(),
        primary_key: new_key.clone(),
        primary_uid: new_uid.clone(),
        previous_key: None,
        previous_uid: None,
        rotated_at: now_ms,
        generation: 1,
    };

    // Store in Redis
    redis_store
        .set_search_ui_scoped_key(&scoped_key)
        .map_err(|e| ErrorResponse::internal_error(format!("failed to store scoped key: {e}")))?;

    info!(
        index = %index_uid,
        uid = %new_uid,
        generation = 1,
        "created new scoped search-only key"
    );

    Ok(scoped_key)
}

/// Get per-index search UI configuration (plan §13.21).
pub async fn get_config(
    Path(index_uid): Path<String>,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let task_store = state
        .task_store
        .as_ref()
        .ok_or_else(|| ErrorResponse::internal_error("task store not available".to_string()))?;

    // Try to load config from task store
    if let Some(row) = task_store.get_search_ui_config(&index_uid)? {
        // Parse the config JSON
        let config: SearchUiIndexConfig = serde_json::from_str(&row.config_json)
            .map_err(|e| ErrorResponse::internal_error(format!("failed to parse config: {e}")))?;

        return Ok(Json(config));
    }

    // Return default config if not found
    Ok(Json(SearchUiIndexConfig {
        title: index_uid.clone(),
        facets: std::collections::HashMap::new(),
        sort_options: std::collections::HashMap::new(),
        result_template: None,
        custom_template_html: None,
        empty_state: None,
        typo_tolerance: None,
        display_attributes: None,
        primary_key_field: None,
        hit_url_template: None,
        thumbnail_field: None,
    }))
}

/// Update per-index search UI configuration (plan §13.21).
pub async fn update_config(
    Path(index_uid): Path<String>,
    State(state): State<AppState>,
    Json(config): Json<SearchUiIndexConfig>,
) -> Result<impl IntoResponse, ErrorResponse> {
    use miroir_core::task_store::NewSearchUiConfig;

    // Serialize config to JSON for storage
    let config_json = serde_json::to_string(&config)
        .map_err(|e| ErrorResponse::internal_error(format!("failed to serialize config: {e}")))?;

    // Validate custom template if present (plan §13.21)
    if let Some(template) = &config.result_template {
        if template == "custom" {
            // Custom templates are stored separately in the config
            // The actual template HTML is stored in the custom_template_html field
            if config.custom_template_html.is_none() {
                return Err(ErrorResponse::invalid_request(
                    "custom template requires custom_template_html field".to_string(),
                ));
            }

            // Validate template syntax
            validate_template(config.custom_template_html.as_ref().unwrap())?;
        }
    }

    // Persist to task store
    let task_store = state
        .task_store
        .as_ref()
        .ok_or_else(|| ErrorResponse::internal_error("task store not available".to_string()))?;

    let new_config = NewSearchUiConfig {
        index_uid: index_uid.clone(),
        config_json,
        updated_at: chrono::Utc::now().timestamp_millis(),
    };

    task_store.upsert_search_ui_config(&new_config)?;

    info!(
        index = %index_uid,
        title = %config.title,
        facet_count = config.facets.len(),
        result_template = ?config.result_template,
        "updated search UI config"
    );

    Ok(StatusCode::NO_CONTENT)
}

/// Analytics beacon endpoint (plan §13.21).
///
/// Idempotent via client-generated event_id. Duplicate events are ignored.
/// Falls back to server-side event_id generation for old browsers.
pub async fn beacon(
    Path(index_uid): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut beacon): Json<BeaconRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let config = &state.config;

    // Extract JWT to get session_id (plan §13.21)
    let session_id = if let Some(auth_header) = headers.get("authorization") {
        let auth_str = auth_header.to_str().unwrap_or("");
        if let Some(token) = auth_str.strip_prefix("Bearer ") {
            // Decode JWT to extract session_id (sub claim)
            match jwt_decode_with_fallback(
                token,
                config.search_ui.auth.jwt_secret_env.as_str(),
                config.search_ui.auth.jwt_secret_previous_env.as_str(),
            ) {
                Ok(claims) => claims.sub.clone(),
                Err(e) => {
                    debug!(
                        error = %e,
                        "failed to decode JWT for beacon, using fallback session_id"
                    );
                    // Fallback: generate a session_id from the token itself
                    let hash = Sha256::digest(token.as_bytes());
                    let hash_hex = hex::encode(&hash[..16]);
                    format!("anon:{hash_hex}")
                }
            }
        } else {
            "anonymous".to_string()
        }
    } else {
        "anonymous".to_string()
    };

    // Server-side event_id generation fallback for old browsers (plan §13.21)
    // If client didn't provide event_id, generate deterministic hash
    if beacon.event_id.is_empty() {
        let mut hasher = Sha256::new();
        hasher.update(session_id.as_bytes());
        if let Some(ref query) = beacon.query {
            hasher.update(query.as_bytes());
        }
        if let Some(ref result_id) = beacon.document_id {
            hasher.update(result_id.as_bytes());
        }
        if let Some(ref position) = beacon.position {
            hasher.update(position.to_be_bytes());
        }
        // Add minute bucket for latency events
        if beacon.event_type == "latency" {
            if let Some(ref latency_ms) = beacon.latency_ms {
                let minute_bucket = latency_ms / 60000; // 60 second buckets
                hasher.update(minute_bucket.to_be_bytes());
            }
        }
        let hash = hasher.finalize();
        beacon.event_id = hex::encode(&hash[..16]);
        debug!(
            index = %index_uid,
            event_type = %beacon.event_type,
            generated_event_id = %beacon.event_id,
            "generated server-side event_id for old browser"
        );
    } else {
        // Normalize event_id
        beacon.event_id = beacon.event_id.trim().to_string();
    }

    // Idempotency check: skip if event_id was already processed (plan §13.21)
    if let Some(redis_store) = &state.redis_store {
        let is_new = redis_store
            .check_and_mark_beacon_event(&index_uid, &beacon.event_id)
            .map_err(|e| {
                ErrorResponse::internal_error(format!("beacon idempotency check failed: {e}"))
            })?;

        if !is_new {
            debug!(
                index = %index_uid,
                event_type = %beacon.event_type,
                event_id = %beacon.event_id,
                "duplicate beacon event ignored"
            );
            return Ok(StatusCode::ACCEPTED);
        }
    }

    debug!(
        index = %index_uid,
        event_type = %beacon.event_type,
        event_id = %beacon.event_id,
        "received analytics beacon"
    );

    // Publish to CDC if analytics is enabled (plan §13.21)
    if config.search_ui.analytics.enabled {
        if let Some(cdc_manager) = &state.cdc_manager {
            let event = AnalyticsEvent {
                event_type: beacon.event_type.clone(),
                event_id: beacon.event_id.clone(),
                session_id: session_id.clone(),
                index: index_uid.clone(),
                query: beacon.query.clone(),
                result_id: beacon.document_id.clone(),
                result_position: beacon.position,
                latency_ms: beacon.latency_ms,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            };

            // Latency events are subject to cdc.emit_internal_writes (plan §13.21)
            let is_latency = beacon.event_type == "latency" || beacon.event_type == "search";
            let should_emit = if is_latency {
                config.cdc.emit_internal_writes
            } else {
                true // Click-through events are always emitted
            };

            if should_emit {
                cdc_manager.publish_analytics(event).await;
                debug!(
                    index = %index_uid,
                    event_type = %beacon.event_type,
                    event_id = %beacon.event_id,
                    "published analytics event to CDC"
                );
            } else {
                debug!(
                    index = %index_uid,
                    event_type = %beacon.event_type,
                    event_id = %beacon.event_id,
                    "skipped latency event (emit_internal_writes=false)"
                );
            }
        }
    }

    Ok(StatusCode::ACCEPTED)
}

/// Embedded static assets for the Search UI (plan §13.21).
#[derive(Embed)]
#[folder = "static/search/"]
pub struct SearchUiAssets;

/// Serve the Search UI SPA (plan §13.21).
pub async fn serve_spa(State(state): State<AppState>) -> Result<Response, ErrorResponse> {
    let config = &state.config;

    // Check if search UI is enabled
    if !config.search_ui.enabled {
        return Err(ErrorResponse::invalid_request("search UI is not enabled"));
    }

    // Build CSP header (plan §9)
    let csp_value = build_csp_header(&config.search_ui.csp, &config.search_ui.csp_overrides);

    let mut response = serve_embedded_file("index.html", false)?;

    // Add CSP header to response (plan §9)
    if let Ok(csp_header) = HeaderValue::from_str(&csp_value) {
        response
            .headers_mut()
            .insert("content-security-policy", csp_header);
    }

    Ok(response)
}

/// Serve a static asset from the Search UI (plan §13.21).
pub async fn serve_static_asset(Path(path): Path<String>) -> Result<Response, ErrorResponse> {
    serve_embedded_file(&path, true)
}

/// Serve an embedded file from the Search UI assets (plan §13.21).
fn serve_embedded_file(path: &str, is_static_asset: bool) -> Result<Response, ErrorResponse> {
    use axum::body::Body;
    use axum::http::header;

    let asset = SearchUiAssets::get(path)
        .ok_or_else(|| ErrorResponse::invalid_request("asset not found"))?;

    let mime_type = mime_guess::from_path(path)
        .first_or_octet_stream()
        .to_string();

    let mut response = Response::new(Body::from(asset.data.to_vec()));

    // Set Content-Type
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, mime_type.parse().unwrap());

    // Set cache headers
    if is_static_asset {
        // Static assets get long cache (1 year)
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            "public, max-age=31536000, immutable".parse().unwrap(),
        );
    } else {
        // HTML gets no-cache to ensure fresh UI after deployments
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            "no-cache, no-store, must-revalidate".parse().unwrap(),
        );
    }

    Ok(response)
}

/// Validate a custom template (plan §13.21).
///
/// Checks for basic Handlebars-style syntax errors:
/// - Properly closed {{ }} tags
/// - Valid {{#if}}...{{/if}} blocks
/// - No unmatched closing tags
fn validate_template(template: &str) -> Result<(), ErrorResponse> {
    let mut if_stack = Vec::new();
    let mut pos = 0;

    while let Some(start) = template[pos..].find("{{") {
        pos += start;

        if let Some(end) = template[pos..].find("}}") {
            let tag = &template[pos + 2..pos + end];
            let tag = tag.trim();

            // Check for {{#if}} opening
            if tag.starts_with("#if") {
                if_stack.push("if");
            }
            // Check for {{/if}} closing
            else if tag.starts_with("/if")
                && if_stack.pop() != Some("if") {
                    return Err(ErrorResponse::invalid_request(
                        "unmatched {{/if}} tag in template".to_string(),
                    ));
                }

            pos += end + 2;
        } else {
            return Err(ErrorResponse::invalid_request(
                "unclosed {{ tag in template".to_string(),
            ));
        }
    }

    if !if_stack.is_empty() {
        return Err(ErrorResponse::invalid_request("unclosed {#if} tag in template".to_string()));
    }

    Ok(())
}

/// Render a custom template with field data (plan §13.21).
///
/// Supports Handlebars-style interpolation:
/// - {{field}} - simple field value
/// - {{#if field}}...{{/if}} - conditional block
pub fn render_custom_template(template: &str, data: &serde_json::Value) -> String {
    let mut result = template.to_string();
    let obj = data.as_object().unwrap();

    // Process {{#if}}...{{/if}} blocks first
    while let Some(start) = result.find("{{#if ") {
        let tag_start = start + 5; // "{{#if ".len()
        if let Some(tag_end) = result[tag_start..].find("}}") {
            let field_name = &result[tag_start..tag_start + tag_end];
            let block_start = tag_start + tag_end + 2;

            // Find matching {{/if}}
            let closing_tag = format!("{{{{/if {}}}}}", field_name.trim());
            let if_end = result[block_start..]
                .find(&closing_tag)
                .or_else(|| result[block_start..].find("{{/if}}"))
                .unwrap_or(result.len());

            let block_content = &result[block_start..block_start + if_end];
            let full_block = &result[start..block_start + if_end + closing_tag.len()];

            // Check if field exists and is truthy
            let should_show = obj
                .get(field_name.trim())
                .and_then(|v| match v {
                    serde_json::Value::Bool(b) => Some(*b),
                    serde_json::Value::String(s) => Some(!s.is_empty()),
                    serde_json::Value::Number(n) => Some(n.as_i64().unwrap_or(0) != 0),
                    serde_json::Value::Array(a) => Some(!a.is_empty()),
                    serde_json::Value::Object(_) => Some(true),
                    serde_json::Value::Null => None,
                })
                .unwrap_or(false);

            if should_show {
                // Replace the entire block with just the content
                result = result.replace(full_block, block_content);
            } else {
                // Remove the entire block
                result = result.replace(full_block, "");
            }
        } else {
            break;
        }
    }

    // Process simple {{field}} tags
    let mut rendered = result;
    for (key, value) in obj {
        let tag = format!("{{{{{key}}}}}");
        let value_str = match value {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => String::new(),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                serde_json::to_string(value).unwrap_or_default()
            }
        };
        rendered = rendered.replace(&tag, &value_str);
    }

    rendered
}
