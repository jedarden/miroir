//! Search UI module for serving embedded static assets (plan §13.21).
//!
//! Serves the end-user search SPA embedded in the binary via `rust-embed`.
//! The UI is accessible at `/ui/search/{index}` and provides instant search,
//! faceted navigation, and result highlighting.

use axum::{
    body::Body,
    extract::{FromRef, State},
    http::{header, HeaderValue, StatusCode},
    response::Response,
};
use rust_embed::RustEmbed;

use crate::auth::build_csp_header;

// Re-export for use in the handler
pub use crate::routes::admin_endpoints;

/// Embedded static assets for the Search UI.
///
/// All UI files (HTML, CSS, JS) are embedded in the binary at compile time.
/// Assets are served from the `static/search/` directory.
#[derive(RustEmbed)]
#[folder = "static/search/"]
#[exclude = "*.swp"]
#[exclude = "*.DS_Store"]
#[exclude = ".git"]
struct SearchUiAssets;

/// Embedded widget script for web component (plan §13.21).
#[derive(RustEmbed)]
#[folder = "static/"]
#[include = "widget.js"]
struct SearchUiWidget;

/// Serve the Search UI SPA (plan §13.21).
///
/// This handler serves the embedded search SPA at `/ui/search/{index}`.
/// For HTML requests, it returns `index.html` which bootstraps the search application.
/// For static assets (CSS, JS, images), it serves the embedded file directly.
///
/// # Query Parameters
///
/// - `embed=true` - Strip chrome (header/footer) for iframe embedding
/// - `headless=true` - Return only results container, no search input or facets
///
/// # Response caching
///
/// Static assets are served with long `max-age` cache headers (1 year).
/// HTML is served with `no-cache` to ensure fresh UI after deployments.
pub async fn serve_search_ui<S>(
    State(state): State<S>,
    axum::extract::Path(index): axum::extract::Path<String>,
) -> Result<Response, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    admin_endpoints::AppState: FromRef<S>,
{
    let app_state = admin_endpoints::AppState::from_ref(&state);
    let config = &app_state.config;

    // Check if search UI is enabled
    if !config.search_ui.enabled {
        return Err(StatusCode::NOT_FOUND);
    }

    // Build CSP header (plan §9)
    let csp_value = build_csp_header(&config.search_ui.csp, &config.search_ui.csp_overrides);

    let mut response = serve_embedded_file("index.html", false)?;

    // Add CSP header to response (plan §9)
    if let Ok(csp_header) = HeaderValue::from_str(&csp_value) {
        response
            .headers_mut()
            .insert(header::CONTENT_SECURITY_POLICY, csp_header);
    }

    Ok(response)
}

/// Serve a static asset from the Search UI (plan §13.21).
///
/// Handles requests for `/ui/search/static/{path}` and serves embedded files.
pub async fn serve_search_ui_asset(
    axum::extract::Path(path): axum::extract::Path<String>,
) -> Result<Response, StatusCode> {
    serve_embedded_file(&path, true)
}

/// Serve the web component widget script (plan §13.21).
///
/// Handles requests for `/ui/widget.js` and serves the embedded widget script.
pub async fn serve_widget() -> Result<Response, StatusCode> {
    let asset = SearchUiWidget::get("widget.js").ok_or(StatusCode::NOT_FOUND)?;

    let mut response = Response::new(Body::from(asset.data.to_vec()));

    // Set Content-Type
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/javascript; charset=utf-8".parse().unwrap(),
    );

    // Set cache headers (1 year)
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "public, max-age=31536000, immutable".parse().unwrap(),
    );

    Ok(response)
}

/// Serve an embedded file from the Search UI assets.
///
/// # Arguments
///
/// * `path` - The file path within the `static/search/` directory
/// * `is_static_asset` - Whether this is a static asset (CSS, JS, etc.) vs HTML
///
/// # Returns
///
/// - `Ok(Response)` with the file contents and appropriate cache headers
/// - `Err(StatusCode::NOT_FOUND)` if the file doesn't exist
fn serve_embedded_file(path: &str, is_static_asset: bool) -> Result<Response, StatusCode> {
    let asset = SearchUiAssets::get(path).ok_or(StatusCode::NOT_FOUND)?;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serve_embedded_file_not_found() {
        let result = serve_embedded_file("nonexistent.html", false);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), StatusCode::NOT_FOUND);
    }
}
