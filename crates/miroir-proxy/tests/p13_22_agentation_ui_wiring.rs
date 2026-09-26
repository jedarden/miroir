//! P13.22 Agentation toolbar wiring — Admin UI entry-point harness (child 1 of 5).
//!
//! This file is the TEST HARNESS for the Agentation wiring umbrella
//! (miroir-f836b0e4, admin entry point miroir-078af814): an in-process
//! `tower::ServiceExt::oneshot` app that serves the Admin UI exactly as
//! production does, over `GET /_miroir/admin`. The harness test pins the
//! entry point itself — `200` with a `text/html` body. On top of it, the
//! second test pins wiring requirement (a): the served `<head>` closes its
//! `<script type="importmap">` before the Agentation module script opens,
//! and the map declares the `agentation` specifier (later children add the
//! remaining wiring assertions).
//!
//! Production wiring being mirrored:
//! - `main.rs:849` nests the admin router under `/_miroir`
//! - `src/routes/admin.rs:29-30` route `GET /admin` and `GET /admin/*path` to
//!   `admin_ui::serve_admin_ui`, which serves `index.html` out of the
//!   `admin-ui/dist/` assets embedded via `AdminUiAssets` (RustEmbed)
//!
//! # Status — live
//!
//! This harness originally landed `#[ignore]`d: two product-code defects made
//! `GET /_miroir/admin` answer `500` (bead miroir-927eeb98, confirmed
//! empirically by running this exact harness). The handler extracted
//! `Extension<Option<AdminSessionId>>`, a type nothing in the production
//! middleware chain ever inserts — axum 0.7's `Extension` does a strict
//! `TypeId` lookup, so `/_miroir/admin*` rejected with 500 "Missing request
//! extension"; and the bare `/admin` route carried no path capture while the
//! handler demanded `Path<String>`, rejecting with 500 "Wrong number of path
//! arguments". Both are fixed in product code — the handler now extracts
//! `Option<Extension<AdminSessionId>>` and `Option<Path<String>>` (bead
//! miroir-d6dfcbfd, commit `01967c1`) — so the test below runs and is the
//! acceptance contract every later Agentation wiring assertion builds on.
//!
//! No real ports are bound: the rest of this suite hard-binds 17770/9090 and a
//! stray listener poisons live runs (see p13_13), so every request here goes
//! through `Router::oneshot` against an in-process router.

use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::get,
    Router,
};
use miroir_core::config::MiroirConfig;
use miroir_proxy::admin_session::{SealKey, KEY_LEN};
use miroir_proxy::admin_ui::serve_admin_ui;
use miroir_proxy::middleware::Metrics;
use miroir_proxy::routes::admin_endpoints::AppState;
use serde_json::json;
use tower::ServiceExt;

/// Test config in the same shape as the other admin tests, except
/// `task_store`: the default backend is SQLite at `/data/miroir-tasks.db`,
/// which a test process cannot open — force the in-memory backend instead.
fn create_test_config() -> MiroirConfig {
    serde_json::from_value(json!({
        "nodes": [
            {
                "id": "node-1",
                "address": "http://localhost:7700",
                "replica_group": 0,
            },
            {
                "id": "node-2",
                "address": "http://localhost:7701",
                "replica_group": 0,
            }
        ],
        "shards": 16,
        "replication_factor": 2,
        "replica_groups": 1,
        "node_master_key": "test-master-key",
        "admin": {
            "api_key": "test-admin-key",
        },
        "task_store": {
            "backend": "memory",
            "path": "",
        },
    }))
    .expect("valid config")
}

/// Build an app serving the Admin UI at `/_miroir/admin` exactly as production
/// does: the admin router nested under `/_miroir` (`main.rs:849`), with the
/// production entry-point routes (`src/routes/admin.rs:29-30`) handled by
/// `admin_ui::serve_admin_ui` over the RustEmbed'ed `admin-ui/dist/` assets.
///
/// `AppState` stands in for the private `UnifiedState` (whose `FromRef` impls
/// live in the binary crate and are invisible to integration tests): the
/// handler only requires `admin_endpoints::AppState: FromRef<S>`, which holds
/// trivially when `S` *is* `AppState`.
///
/// No session extension is installed — matching production's `X-Admin-Key`
/// path, where `auth_middleware` inserts nothing into request extensions.
fn admin_app() -> Router {
    let config = create_test_config();
    let metrics = Metrics::new(&config);
    let state = AppState::new(config, metrics, SealKey::from_bytes([42u8; KEY_LEN]));

    Router::new().nest(
        "/_miroir",
        Router::new()
            .route("/admin", get(serve_admin_ui::<AppState>))
            .route("/admin/*path", get(serve_admin_ui::<AppState>))
            .with_state(state),
    )
}

/// Fetch the served admin UI, returning `(status, headers, body)`.
///
/// Authenticates the way the documented header path does (`X-Admin-Key`
/// matching the configured admin API key).
async fn serve_admin_ui_response() -> (StatusCode, axum::http::HeaderMap, String) {
    let response = admin_app()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/_miroir/admin")
                .header("X-Admin-Key", "test-admin-key")
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("infallible response");

    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    (status, headers, String::from_utf8_lossy(&body).into_owned())
}

/// The Admin UI entry point answers `200` with a `text/html` body: this is the
/// page the Agentation toolbar mounts into, so every later wiring assertion
/// starts from this harness fetch.
#[tokio::test]
async fn p13_22_admin_entry_point_serves_html() {
    let (status, headers, html) = serve_admin_ui_response().await;

    assert_eq!(
        status,
        StatusCode::OK,
        "GET /_miroir/admin should serve the admin UI, got body:\n{html}"
    );

    let content_type = headers
        .get("content-type")
        .unwrap_or_else(|| panic!("GET /_miroir/admin must serve a Content-Type header"))
        .to_str()
        .expect("Content-Type header is valid UTF-8");
    assert!(
        content_type.starts_with("text/html"),
        "GET /_miroir/admin must serve text/html, got: {content_type}"
    );
    assert!(
        !html.is_empty(),
        "GET /_miroir/admin must serve a non-empty HTML body"
    );
}

/// Wiring requirement (a): the import map is closed before the Agentation
/// module script opens, and the map declares the `agentation` specifier.
///
/// Browsers only honour an import map for module loads that *begin after* the
/// map has been fully processed, so this is a byte-offset ordering assertion —
/// the `</script>` ending the `<script type="importmap">` block must sit at or
/// before the offset where the module script's tag opens — not a mere presence
/// check. A map that is still open, or that appears after the module script,
/// is ignored and the bare-specifier `import('agentation')` would throw.
///
/// Both lookups are scoped to the `<head>`, where the served page puts them.
/// The module script is located by its own `<script type="module">` tag and
/// confirmed by its `import('agentation')` bootstrap call — deliberately NOT
/// by the esm.sh URL, which also appears inside the import map itself.
#[tokio::test]
async fn p13_22_import_map_closed_before_agentation_module_script() {
    let (status, _headers, html) = serve_admin_ui_response().await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET /_miroir/admin should serve the admin UI, got body:\n{html}"
    );

    // Scope every lookup to the <head>: the import map and the Agentation
    // bootstrap both live there, and nothing outside it may satisfy the
    // ordering contract.
    let head_start = html
        .find("<head")
        .expect("served admin HTML must contain a <head> element");
    let head_end = html[head_start..]
        .find("</head>")
        .expect("served admin HTML must close its <head>")
        + head_start;
    let head = &html[head_start..head_end];

    // The import map: its opening tag through its OWN closing </script>.
    let import_map_open = head
        .find("<script type=\"importmap\">")
        .expect("admin <head> must contain a <script type=\"importmap\"> block");
    let import_map_close = head[import_map_open..]
        .find("</script>")
        .expect("the import map script must be closed within <head>")
        + import_map_open;
    let import_map_closed_at = import_map_close + "</script>".len();

    // The Agentation module script: located by its own tag ...
    let module_open = head
        .find("<script type=\"module\">")
        .expect("admin <head> must contain the Agentation module script");
    let module_close = head[module_open..]
        .find("</script>")
        .expect("the module script must be closed within <head>")
        + module_open;
    // ... and confirmed by its bootstrap call, not by the esm.sh URL (which
    // also appears inside the import map above).
    let module_body = &head[module_open..module_close];
    assert!(
        module_body.contains("import('agentation')"),
        "the <script type=\"module\"> at head offset {module_open} must bootstrap \
         Agentation via import('agentation'), got body:\n{module_body}"
    );

    // The ordering contract itself: the import map is FULLY CLOSED (its
    // </script> has ended) at or before the offset where the module script
    // OPENS. Any later and the browser would ignore the map for the
    // import('agentation') module load.
    assert!(
        import_map_closed_at <= module_open,
        "the import map must be closed (head offset {import_map_closed_at}) before \
         the Agentation module script opens (head offset {module_open}); browsers \
         ignore an import map for module loads that begin before it"
    );

    // The map must parse as JSON (browsers reject import maps that fail to
    // parse) and must declare the "agentation" specifier pointing at the
    // pinned esm.sh module with react/react-dom externalized — the
    // ?external=react,react-dom query is what keeps the toolbar from loading
    // second, unbundled copies of React.
    let import_map_body =
        &head[import_map_open + "<script type=\"importmap\">".len()..import_map_close];
    let map: serde_json::Value = serde_json::from_str(import_map_body).unwrap_or_else(|err| {
        panic!("import map body must be valid JSON: {err}\n{import_map_body}")
    });
    let agentation_url = map["imports"]["agentation"]
        .as_str()
        .expect("import map must declare an \"agentation\" specifier under imports");
    assert_eq!(
        agentation_url, "https://esm.sh/agentation@3.0.2?external=react,react-dom",
        "the \"agentation\" specifier must point at the pinned esm.sh module with \
         react/react-dom externalized, got: {agentation_url}"
    );
}
