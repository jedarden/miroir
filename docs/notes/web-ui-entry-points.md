# Miroir Proxy Web UI Entry Points

Inventory of every HTML (and web-component JS) entry point the proxy actually
serves, as of 2026-09-10. Written for the Agentation umbrella work
(miroir-c08da2a8): child beads should wire feedback tooling into these served
UIs, and this file is the authoritative list of what is live versus dead.

## Served HTML entry points

### Admin SPA — `/_miroir/admin`

| | |
|---|---|
| **Routes** | `GET /_miroir/admin` and `GET /_miroir/admin/*path` (`crates/miroir-proxy/src/routes/admin.rs:29-30`, nested under `/_miroir` in `main.rs`) |
| **Handler** | `admin_ui::serve_admin_ui` (`crates/miroir-proxy/src/admin_ui.rs`) |
| **Embedding** | `#[derive(RustEmbed)] #[folder = "admin-ui/dist/"]` — `AdminUiAssets` (`admin_ui.rs:25-30`) |
| **HTML file** | `crates/miroir-proxy/admin-ui/dist/index.html` |
| **Assets** | `app.js`, `styles.css` in the same folder, fetched as `/_miroir/admin/app.js` and `/_miroir/admin/styles.css` |
| **Auth** | `X-Admin-Key` / `Authorization: Bearer` admin API key, or the session cookie set by `POST /_miroir/admin/login` |
| **SPA fallback** | Any `*path` without a `.` serves `index.html`; paths with an extension serve the embedded file directly |

Admin login is **not** an HTML page: `POST /_miroir/admin/login`
(`routes/session.rs::admin_login`) returns JSON plus a CSRF/session cookie.

### Search SPA — `/ui/search`

| | |
|---|---|
| **Routes** | `GET /ui/search/{index}` (router fallback) and `GET /ui/search/static/*path` (`crates/miroir-proxy/src/routes/search_ui.rs:138-151`, nested under `/ui/search` in `main.rs`) |
| **Handler** | `search_ui::serve_spa` / `search_ui::serve_static_asset` (`routes/search_ui.rs`) |
| **Embedding** | `#[derive(Embed)] #[folder = "static/search/"]` — `SearchUiAssets` (`routes/search_ui.rs:670-673`) |
| **HTML file** | `crates/miroir-proxy/static/search/index.html` |
| **Assets** | `search.css`, `search.js` in the same folder, fetched as `/ui/search/static/search.css` and `/ui/search/static/search.js`; `test_idempotency_key.js` is embedded but is a test harness file, not referenced by the page |
| **Auth** | Session JWT minted by `GET /_miroir/ui/search/{index}/session`; gated by `search_ui.enabled` config |

### Web component widget (JS, not HTML)

| | |
|---|---|
| **Route** | `GET /ui/widget.js` (`main.rs:857`) |
| **Handler** | `search_ui_serve::serve_widget` (`crates/miroir-proxy/src/search_ui_serve.rs`) |
| **Embedding** | `#[derive(RustEmbed)] #[folder = "static/"] #[include = "widget.js"]` — `SearchUiWidget` (`search_ui_serve.rs:32-35`) |
| **File** | `crates/miroir-proxy/static/widget.js` |

## Dead duplicate serving path (kept, for awareness)

`search_ui_serve.rs` also contains `SearchUiAssets`
(`#[folder = "static/search/"]`) with handlers `serve_search_ui`,
`serve_search_ui_asset`, and a private `serve_embedded_file`. All are
`#[allow(dead_code)]` and **no route reaches them** — they duplicate
`routes/search_ui.rs`, which is the wired implementation. Only `serve_widget`
from this module is routed. Do not confuse the two when looking for the live
search UI code path.

## Removed legacy pages

`crates/miroir-proxy/static/admin/` (`index.html`, `login.html`, `admin.js`,
`admin.css`) was removed in the same change that introduced this inventory. It
was unreachable:

- No `RustEmbed`/`include_dir` folder referenced `static/admin/`.
- No axum route served any file from it. `GET /_miroir/admin` resolves to the
  embedded `admin-ui/dist/index.html` SPA, not the legacy page, and the login
  endpoint is JSON-only.
- The only inbound references were self-referential (`index.html` loaded
  `admin.js`/`admin.css`; `admin.js` redirected to `login.html`) plus
  point-in-time mentions in historical `notes/miroir-uhj-phase5-*.md`
  summaries, which record what existed at the time and were left as-is.

The legacy pages were also functionally broken had they been served: they
referenced `/_miroir/admin/static/admin.css` and
`/_miroir/admin/static/admin.js`, which the `/_miroir/admin/*path` handler
would resolve against `admin-ui/dist/`, where those files do not exist.
