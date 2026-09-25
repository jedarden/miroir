//! P13.22 Agentation toolbar wiring — Admin UI entry-point harness (child 1 of 5).
//!
//! This file is the TEST HARNESS for the Agentation wiring umbrella
//! (miroir-f836b0e4, admin entry point miroir-078af814): an in-process
//! `tower::ServiceExt::oneshot` app that serves the Admin UI exactly as
//! production does, over `GET /_miroir/admin`. This child pins only the
//! harness itself — that the entry point answers `200` with a `text/html`
//! body; later children add the Agentation wiring assertions on top of it.
//!
//! Production wiring being mirrored:
//! - `main.rs:849` nests the admin router under `/_miroir`
//! - `src/routes/admin.rs:29-30` route `GET /admin` and `GET /admin/*path` to
//!   `admin_ui::serve_admin_ui`, which serves `index.html` out of the
//!   `admin-ui/dist/` assets embedded via `AdminUiAssets` (RustEmbed)
//!
//! # CURRENTLY BLOCKED — the harness is sound; production wiring is not
//!
//! The harness test below is `#[ignore]`d because `GET /_miroir/admin` cannot
//! answer `200` against the current product code. Two independent product-code
//! defects, both confirmed empirically 2026-09-13 (bead miroir-927eeb98) by
//! running this exact harness:
//!
//! 1. `admin_ui.rs:53` extracts `Extension<Option<AdminSessionId>>`. Nothing in
//!    the production middleware chain ever inserts `Option<AdminSessionId>` —
//!    `auth_middleware` inserts plain `AdminSessionId` (auth.rs:852, session
//!    cookie path only), and no other site inserts the `Option` type. axum
//!    0.7's `Extension` does a strict `TypeId` lookup, so every request to
//!    `/_miroir/admin*` rejects with 500 "Missing request extension: Extension
//!    of type `core::option::Option<miroir_proxy::auth::AdminSessionId>` was
//!    not found". The correct extractor is `Option<Extension<AdminSessionId>>`.
//!    Confirmed via `/_miroir/admin/index.html` (which routes around defect 2):
//!    500 with exactly that message.
//! 2. `admin.rs:29` registers the bare `/admin` route with no path parameter,
//!    but the handler extracts `Path<String>` (admin_ui.rs:52). axum rejects
//!    with 500 "Wrong number of path arguments for `Path`. Expected 1 but got
//!    0" — this rejection fires before the extension lookup, so it is what
//!    `GET /_miroir/admin` returns today (confirmed). Even with defect 1 fixed,
//!    the exact entry point would still 500 until the route/handler mismatch is
//!    fixed. Today `/admin/*path` does NOT serve either — it hits defect 1.
//!
//! The harness itself is verified sound: a one-off probe inserting the missing
//! extension (`.layer(Extension(None::<AdminSessionId>))`) and fetching the
//! path-param route returned `200 OK`, `text/html`, the full 67,991-byte
//! embedded `index.html`. Both defects and that soundness were re-confirmed
//! empirically at HEAD `54d4ee8` on 2026-09-25 (bead miroir-927eeb98): the
//! entry point still 500s with the path-arity rejection, the path route still
//! 500s with the missing-extension rejection, and the extension probe still
//! yields `200 OK` `text/html` at 67,991 bytes. Nothing in this harness needs
//! to change once both
//! product defects are fixed — remove the `#[ignore]`; this test is the
//! acceptance contract.
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
///
/// Ignored until the two product defects in the module docs are fixed
/// (extractor `Extension<Option<AdminSessionId>>` → `Option<Extension<..>>`,
/// and `Path<String>` on the parameterless `/admin` route); until then this
/// request 500s in production wiring exactly as it does here.
#[tokio::test]
#[ignore = "blocked on product fixes (bead miroir-927eeb98): admin_ui.rs Extension<Option<AdminSessionId>> is never inserted (500), and bare /admin route cannot satisfy Path<String> (500)"]
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
