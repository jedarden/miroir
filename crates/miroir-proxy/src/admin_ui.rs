//! Admin Web UI module (plan §13.19).
//!
//! Serves a single-page admin application embedded in the binary via `rust-embed`.
//! The UI is accessible at `/_miroir/admin` and provides cluster management,
//! topology viewing, index configuration, and operational debugging.

use axum::{
    body::Body,
    extract::{FromRef, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::Response,
    Extension,
};
use miroir_core::config::MiroirConfig;
use rust_embed::RustEmbed;

use crate::auth::{build_csp_header, AdminSessionId};
use crate::routes::admin_endpoints;

/// Embedded static assets for the Admin Web UI.
///
/// All UI files (HTML, CSS, JS) are embedded in the binary at compile time.
/// In development, assets are served from the `admin-ui/dist/` directory.
/// In production, they are baked into the binary.
#[derive(RustEmbed)]
#[folder = "admin-ui/dist/"]
#[exclude = "*.swp"]
#[exclude = "*.DS_Store"]
#[exclude = ".git"]
pub struct AdminUiAssets;

/// Serve the Admin Web UI.
///
/// This handler serves the embedded SPA. For HTML requests, it returns
/// `index.html` which bootstraps the Preact application. For static assets
/// (CSS, JS, images), it serves the embedded file directly.
///
/// # Authentication
///
/// Access requires either:
/// - `Authorization: Bearer <MIROIR_ADMIN_API_KEY>` header
/// - `X-Admin-Key: <MIROIR_ADMIN_API_KEY>` header
/// - A valid session cookie (set by `/admin/login`)
///
/// # Response caching
///
/// Static assets are served with long `max-age` cache headers (1 year).
/// HTML is served with `no-cache` to ensure fresh UI after deployments.
pub async fn serve_admin_ui<S>(
    State(state): State<S>,
    headers: HeaderMap,
    axum::extract::Path(path): axum::extract::Path<String>,
    Extension(admin_session): Extension<Option<AdminSessionId>>,
) -> Result<Response, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    admin_endpoints::AppState: FromRef<S>,
{
    let admin_state = admin_endpoints::AppState::from_ref(&state);

    // Check authentication - X-Admin-Key, Authorization: Bearer header, or session cookie
    let is_authorized = check_admin_auth(&headers, &admin_state.config) || admin_session.is_some();

    if !is_authorized {
        return Err(StatusCode::UNAUTHORIZED);
    }

    // Determine the file to serve
    // Empty path or "/" means serve index.html
    let path = if path.is_empty() || path == "/" {
        "index.html"
    } else {
        // Remove leading slash if present
        let path = path.strip_prefix('/').unwrap_or(&path);
        // If no extension, serve index.html (SPA routing)
        if path.contains('.') {
            path
        } else {
            "index.html"
        }
    };

    // Determine if this is a static asset (has file extension)
    let is_static_asset = path.contains('.');

    // Build CSP header (plan §9)
    let csp_value = build_csp_header(
        &admin_state.config.admin_ui.csp,
        &admin_state.config.admin_ui.csp_overrides,
    );

    let mut response = serve_embedded_file(path, is_static_asset)?;

    // Add CSP header to response (plan §9)
    if let Ok(csp_header) = HeaderValue::from_str(&csp_value) {
        response
            .headers_mut()
            .insert(header::CONTENT_SECURITY_POLICY, csp_header);
    }

    Ok(response)
}

/// Serve an embedded file from the Admin UI assets.
///
/// # Arguments
///
/// * `path` - The file path within the `admin-ui/dist/` directory
/// * `is_static_asset` - Whether this is a static asset (CSS, JS, etc.) vs HTML
///
/// # Returns
///
/// - `Ok(Response)` with the file contents and appropriate cache headers
/// - `Err(StatusCode::NOT_FOUND)` if the file doesn't exist
fn serve_embedded_file(path: &str, is_static_asset: bool) -> Result<Response, StatusCode> {
    let asset = AdminUiAssets::get(path).ok_or(StatusCode::NOT_FOUND)?;

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

/// Check admin authentication from headers.
///
/// Accepts either `X-Admin-Key: <key>` or `Authorization: Bearer <key>`.
/// The key must match the configured admin API key.
fn check_admin_auth(headers: &HeaderMap, config: &MiroirConfig) -> bool {
    // Check X-Admin-Key header
    if let Some(x_admin_key) = headers.get("X-Admin-Key") {
        if let Ok(key) = x_admin_key.to_str() {
            return key == config.admin.api_key;
        }
    }

    // Check Authorization: Bearer header
    if let Some(auth) = headers.get("authorization") {
        if let Ok(auth_str) = auth.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                return token == config.admin.api_key;
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use miroir_core::config::MiroirConfig;

    #[test]
    fn test_serve_embedded_file_not_found() {
        let result = serve_embedded_file("nonexistent.html", false);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_check_admin_auth_with_x_admin_key() {
        let config = MiroirConfig::default();
        let mut headers = HeaderMap::new();
        headers.insert("X-Admin-Key", config.admin.api_key.parse().unwrap());

        assert!(check_admin_auth(&headers, &config));
    }

    #[test]
    fn test_check_admin_auth_with_bearer_token() {
        let config = MiroirConfig::default();
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            format!("Bearer {}", config.admin.api_key).parse().unwrap(),
        );

        assert!(check_admin_auth(&headers, &config));
    }

    #[test]
    fn test_check_admin_auth_with_wrong_key() {
        let config = MiroirConfig::default();
        let mut headers = HeaderMap::new();
        headers.insert("X-Admin-Key", "wrong-key".parse().unwrap());

        assert!(!check_admin_auth(&headers, &config));
    }

    #[test]
    fn test_check_admin_auth_with_no_header() {
        let config = MiroirConfig::default();
        let headers = HeaderMap::new();

        assert!(!check_admin_auth(&headers, &config));
    }

    #[test]
    fn test_serve_embedded_file_index_html() {
        let result = serve_embedded_file("index.html", false);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.headers().get("Content-Type").unwrap(), "text/html");
        assert_eq!(
            response.headers().get("Cache-Control").unwrap(),
            "no-cache, no-store, must-revalidate"
        );
    }

    #[test]
    fn test_serve_embedded_file_static_asset() {
        let result = serve_embedded_file("app.js", true);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert!(response
            .headers()
            .get("Content-Type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("javascript"));
        assert_eq!(
            response.headers().get("Cache-Control").unwrap(),
            "public, max-age=31536000, immutable"
        );
    }
}
