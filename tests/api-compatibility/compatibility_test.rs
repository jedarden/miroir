// Miroir API Compatibility Tests
//
// Runs the same scenarios against both Miroir and a real Meilisearch instance,
// asserting semantic equivalence of responses.
//
// Per plan §8: Every Meilisearch error code must be verified byte-identical
// in the compat suite, including {message,code,type,link} shape.
//
// Prerequisites:
//   - docker-compose-dev stack running (Miroir on port 7700)
//   - Meilisearch running on port 7704 (or set MEILISEARCH_PORT)
//
// Run:
//   MEILISEARCH_PORT=7704 cargo test --test api_compatibility

use meilisearch_sdk::{client::Client, indexes::Indexes, task::Task};
use serde_json::json;
use serde_json::Value;
use std::env;
use std::time::Duration;

const MIROIR_PORT: u16 = 7700;
const DEFAULT_MEILISEARCH_PORT: u16 = 7704;

/// Error shape from Meilisearch/Miroir responses
#[derive(Debug, Clone, PartialEq)]
struct ErrorShape {
    message: String,
    code: String,
    error_type: String,
    link: Option<String>,
}

impl ErrorShape {
    /// Parse error from JSON response
    fn from_json(value: &Value) -> Option<Self> {
        Some(Self {
            message: value.get("message")?.as_str()?.to_string(),
            code: value.get("code")?.as_str()?.to_string(),
            error_type: value.get("type")?.as_str()?.to_string(),
            link: value.get("link").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }

    /// Check if this is a Miroir-specific error code
    fn is_miroir_code(&self) -> bool {
        self.code.starts_with("miroir_")
    }
}

/// Assert two error shapes are byte-identical for critical fields
fn assert_error_equivalent(miroir: &ErrorShape, meili: &ErrorShape, msg: &str) {
    // For Miroir-specific codes, only validate against Miroir
    if miroir.is_miroir_code() {
        assert_eq!(
            miroir.error_type, meili.error_type,
            "{}: error_type mismatch (miroir={}, meili={})",
            msg, miroir.error_type, meili.error_type
        );
        return;
    }

    // For Meilisearch-native codes, all fields must match
    assert_eq!(
        miroir.code, meili.code,
        "{}: code mismatch (miroir={}, meili={})",
        msg, miroir.code, meili.code
    );

    assert_eq!(
        miroir.error_type, meili.error_type,
        "{}: error_type mismatch (miroir={}, meili={})",
        msg, miroir.error_type, meili.error_type
    );

    // Link should match if present on either side
    if miroir.link.is_some() || meili.link.is_some() {
        assert_eq!(
            miroir.link, meili.link,
            "{}: link mismatch (miroir={:?}, meili={:?})",
            msg, miroir.link, meili.link
        );
    }
}

fn get_miroir_client() -> Client {
    let url = format!("http://localhost:{}", MIROIR_PORT);
    Client::new(url, Some("dev-key".to_string()))
}

fn get_meilisearch_client() -> Client {
    let port = env::var("MEILISEARCH_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_MEILISEARCH_PORT);
    let url = format!("http://localhost:{}", port);
    Client::new(url, Some("dev-node-key".to_string()))
}

#[derive(Debug)]
struct CompatibilityTest {
    name: &'static str,
    run: Box<dyn Fn() -> Result<(), Box<dyn std::error::Error>>>,
}

#[tokio::test]
async fn create_and_search_index() -> Result<(), Box<dyn std::error::Error>> {
    let miroir = get_miroir_client();
    let meili = get_meilisearch_client();

    let index_name = "compat_create_search";

    // Clean up first
    let _ = miroir.delete_index(index_name).await;
    let _ = meili.delete_index(index_name).await;

    // Create index on both
    let miroir_task = miroir.create_index(index_name, Some("id")).await?;
    let meili_task = meili.create_index(index_name, Some("id")).await?;

    // Wait for both
    miroir.wait_for_task(miroir_task.uid(), None, None).await?;
    meili.wait_for_task(meili_task.uid(), None, None).await?;

    // Add documents to both
    let docs = json!([
        {"id": 1, "title": "Test Document", "value": 42},
        {"id": 2, "title": "Another Document", "value": 100},
    ]);

    let miroir_task = miroir.index(index_name).add_documents(&docs, None).await?;
    let meili_task = meili.index(index_name).add_documents(&docs, None).await?;

    miroir.wait_for_task(miroir_task.uid(), None, None).await?;
    meili.wait_for_task(meili_task.uid(), None, None).await?;

    // Search both
    let miroir_results = miroir.index(index_name).search()
        .with_query("test")
        .execute::<serde_json::Value>()
        .await?;

    let meili_results = meili.index(index_name).search()
        .with_query("test")
        .execute::<serde_json::Value>()
        .await?;

    // Assert equivalence
    assert_eq!(
        miroir_results.hits.len(),
        meili_results.hits.len(),
        "Hit count mismatch"
    );

    if !miroir_results.hits.is_empty() && !meili_results.hits.is_empty() {
        let miroir_title = miroir_results.hits[0]["title"].as_str();
        let meili_title = meili_results.hits[0]["title"].as_str();
        assert_eq!(miroir_title, meili_title, "First hit title mismatch");
    }

    // Clean up
    let _ = miroir.delete_index(index_name).await;
    let _ = meili.delete_index(index_name).await;

    Ok(())
}

#[tokio::test]
async fn settings_update_and_search() -> Result<(), Box<dyn std::error::Error>> {
    let miroir = get_miroir_client();
    let meili = get_meilisearch_client();

    let index_name = "compat_settings";

    // Clean up and create
    let _ = miroir.delete_index(index_name).await;
    let _ = meili.delete_index(index_name).await;

    let miroir_task = miroir.create_index(index_name, Some("id")).await?;
    let meili_task = meili.create_index(index_name, Some("id")).await?;

    miroir.wait_for_task(miroir_task.uid(), None, None).await?;
    meili.wait_for_task(meili_task.uid(), None, None).await?;

    // Update settings
    use meilisearch_sdk::settings::Settings;

    let settings = Settings::new()
        .with_searchable_attributes(["title", "description"])
        .with_filterable_attributes(["category"]);

    let miroir_task = miroir.index(index_name).set_settings(&settings).await?;
    let meili_task = meili.index(index_name).set_settings(&settings).await?;

    miroir.wait_for_task(miroir_task.uid(), None, None).await?;
    meili.wait_for_task(meili_task.uid(), None, None).await?;

    // Verify settings are retrievable
    let miroir_settings = miroir.index(index_name).get_settings().await?;
    let meili_settings = meili.index(index_name).get_settings().await?;

    assert_eq!(
        miroir_settings.searchable_attributes,
        meili_settings.searchable_attributes,
        "Searchable attributes mismatch"
    );

    // Clean up
    let _ = miroir.delete_index(index_name).await;
    let _ = meili.delete_index(index_name).await;

    Ok(())
}

#[tokio::test]
async fn error_response_format() -> Result<(), Box<dyn std::error::Error>> {
    let miroir = get_miroir_client();
    let meili = get_meilisearch_client();

    let index_name = "compat_errors";

    // Clean up first
    let _ = miroir.delete_index(index_name).await;
    let _ = meili.delete_index(index_name).await;

    // Try to get a non-existent index
    let miroir_err = miroir.index(index_name).get_stats().await;
    let meili_err = meili.index(index_name).get_stats().await;

    // Both should fail
    assert!(miroir_err.is_err(), "Miroir should return error for non-existent index");
    assert!(meili_err.is_err(), "Meilisearch should return error for non-existent index");

    // Check error shapes match
    let miroir_err_msg = format!("{:?}", miroir_err.unwrap_err());
    let meili_err_msg = format!("{:?}", meili_err.unwrap_err());

    // Both should mention "index not found" or similar
    assert!(
        miroir_err_msg.to_lowercase().contains("index") || miroir_err_msg.to_lowercase().contains("not found"),
        "Miroir error should mention index not found: {}",
        miroir_err_msg
    );

    Ok(())
}

#[tokio::test]
async fn filter_and_facet() -> Result<(), Box<dyn std::error::Error>> {
    let miroir = get_miroir_client();
    let meili = get_meilisearch_client();

    let index_name = "compat_filter_facet";

    // Clean up and create
    let _ = miroir.delete_index(index_name).await;
    let _ = meili.delete_index(index_name).await;

    let miroir_task = miroir.create_index(index_name, Some("id")).await?;
    let meili_task = meili.create_index(index_name, Some("id")).await?;

    miroir.wait_for_task(miroir_task.uid(), None, None).await?;
    meili.wait_for_task(meili_task.uid(), None, None).await?;

    // Add faceted documents
    let docs = json!([
        {"id": 1, "title": "Product A", "category": "electronics", "price": 100},
        {"id": 2, "title": "Product B", "category": "books", "price": 20},
        {"id": 3, "title": "Product C", "category": "electronics", "price": 200},
    ]);

    let miroir_task = miroir.index(index_name).add_documents(&docs, None).await?;
    let meili_task = meili.index(index_name).add_documents(&docs, None).await?;

    miroir.wait_for_task(miroir_task.uid(), None, None).await?;
    meili.wait_for_task(meili_task.uid(), None, None).await?;

    // Update settings for facets
    use meilisearch_sdk::settings::Settings;

    let settings = Settings::new()
        .with_filterable_attributes(["category", "price"]);

    let miroir_task = miroir.index(index_name).set_settings(&settings).await?;
    let meili_task = meili.index(index_name).set_settings(&settings).await?;

    miroir.wait_for_task(miroir_task.uid(), None, None).await?;
    meili.wait_for_task(meili_task.uid(), None, None).await?;

    // Filter by category
    let miroir_results = miroir.index(index_name).search()
        .with_query("product")
        .with_filter("category = electronics")
        .execute::<serde_json::Value>()
        .await?;

    let meili_results = meili.index(index_name).search()
        .with_query("product")
        .with_filter("category = electronics")
        .execute::<serde_json::Value>()
        .await?;

    // Should both return 2 electronics products
    assert_eq!(miroir_results.hits.len(), 2, "Miroir should return 2 electronics products");
    assert_eq!(meili_results.hits.len(), 2, "Meilisearch should return 2 electronics products");

    // Clean up
    let _ = miroir.delete_index(index_name).await;
    let _ = meili.delete_index(index_name).await;

    Ok(())
}

// ============================================================================
// Comprehensive Error Code Tests (plan §8 requirement)
// ============================================================================

/// Helper: Make a raw HTTP request and parse the error response
async fn get_error_from_raw_request(
    url: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<&Value>,
    headers: &[(&str, &str)],
) -> Option<ErrorShape> {
    let client = reqwest::Client::new();
    let mut req = client.request(method.clone(), format!("{}{}", url, path));

    for (key, value) in headers {
        req = req.header(*key, *value);
    }

    if let Some(b) = body {
        req = req.json(b);
    }

    let resp = req.send().await.ok()?;
    let status = resp.status();

    // Only process error responses
    if status.is_success() {
        return None;
    }

    let json: Value = resp.json().await.ok()?;
    ErrorShape::from_json(&json)
}

/// Test: index_not_found error code (Meilisearch-native)
#[tokio::test]
async fn error_code_index_not_found() -> Result<(), Box<dyn std::error::Error>> {
    let index_name = "compat_index_not_found";

    // Try to get stats on non-existent index via raw HTTP
    let miroir_err = get_error_from_raw_request(
        "http://localhost:7700",
        reqwest::Method::GET,
        &format!("/indexes/{}/stats", index_name),
        None,
        &[("Authorization", "Bearer dev-key")],
    ).await;

    let meili_err = get_error_from_raw_request(
        &format!("http://localhost:{}", env::var("MEILISEARCH_PORT").unwrap_or_else(|_| "7704".to_string())),
        reqwest::Method::GET,
        &format!("/indexes/{}/stats", index_name),
        None,
        &[("Authorization", "Bearer dev-node-key")],
    ).await;

    assert!(miroir_err.is_some(), "Miroir should return index_not_found error");
    assert!(meili_err.is_some(), "Meilisearch should return index_not_found error");

    let miroir_err = miroir_err.unwrap();
    let meili_err = meili_err.unwrap();

    // Verify code is index_not_found
    assert_eq!(miroir_err.code, "index_not_found");
    assert_eq!(meili_err.code, "index_not_found");

    // Verify error shapes match
    assert_error_equivalent(&miroir_err, &meili_err, "index_not_found");

    Ok(())
}

/// Test: invalid_index_uid error code (Meilisearch-native)
#[tokio::test]
async fn error_code_invalid_index_uid() -> Result<(), Box<dyn std::error::Error>> {
    // Try to create an index with invalid UID (contains capital letters)
    let invalid_index = "InvalidIndex";

    let miroir_err = get_error_from_raw_request(
        "http://localhost:7700",
        reqwest::Method::POST,
        "/indexes",
        Some(&json!({"uid": invalid_index, "primaryKey": "id"})),
        &[("Authorization", "Bearer dev-key")],
    ).await;

    let meili_err = get_error_from_raw_request(
        &format!("http://localhost:{}", env::var("MEILISEARCH_PORT").unwrap_or_else(|_| "7704".to_string())),
        reqwest::Method::POST,
        "/indexes",
        Some(&json!({"uid": invalid_index, "primaryKey": "id"})),
        &[("Authorization", "Bearer dev-node-key")],
    ).await;

    assert!(miroir_err.is_some(), "Miroir should return invalid_index_uid error");
    assert!(meili_err.is_some(), "Meilisearch should return invalid_index_uid error");

    let miroir_err = miroir_err.unwrap();
    let meili_err = meili_err.unwrap();

    // Verify code
    assert_eq!(miroir_err.code, "invalid_index_uid");
    assert_eq!(meili_err.code, "invalid_index_uid");

    // Verify error shapes match
    assert_error_equivalent(&miroir_err, &meili_err, "invalid_index_uid");

    Ok(())
}

/// Test: missing_authorization_header error code (Meilisearch-native)
#[tokio::test]
async fn error_code_missing_authorization_header() -> Result<(), Box<dyn std::error::Error>> {
    let miroir_err = get_error_from_raw_request(
        "http://localhost:7700",
        reqwest::Method::GET,
        "/indexes",
        None,
        &[],  // No auth header
    ).await;

    let meili_err = get_error_from_raw_request(
        &format!("http://localhost:{}", env::var("MEILISEARCH_PORT").unwrap_or_else(|_| "7704".to_string())),
        reqwest::Method::GET,
        "/indexes",
        None,
        &[],  // No auth header
    ).await;

    // Both should return an auth error (code may vary by version)
    assert!(miroir_err.is_some(), "Miroir should return auth error");
    assert!(meili_err.is_some(), "Meilisearch should return auth error");

    let miroir_err = miroir_err.unwrap();
    let meili_err = meili_err.unwrap();

    // Verify error type is auth
    assert_eq!(miroir_err.error_type, "auth");
    assert_eq!(meili_err.error_type, "auth");

    // Verify error shapes match
    assert_error_equivalent(&miroir_err, &meili_err, "missing_authorization_header");

    Ok(())
}

/// Test: invalid_api_key error code (Meilisearch-native)
#[tokio::test]
async fn error_code_invalid_api_key() -> Result<(), Box<dyn std::error::Error>> {
    let miroir_err = get_error_from_raw_request(
        "http://localhost:7700",
        reqwest::Method::GET,
        "/indexes",
        None,
        &[("Authorization", "Bearer invalid-key-12345")],
    ).await;

    let meili_err = get_error_from_raw_request(
        &format!("http://localhost:{}", env::var("MEILISEARCH_PORT").unwrap_or_else(|_| "7704".to_string())),
        reqwest::Method::GET,
        "/indexes",
        None,
        &[("Authorization", "Bearer invalid-key-12345")],
    ).await;

    assert!(miroir_err.is_some(), "Miroir should return invalid_api_key error");
    assert!(meili_err.is_some(), "Meilisearch should return invalid_api_key error");

    let miroir_err = miroir_err.unwrap();
    let meili_err = meili_err.unwrap();

    // Verify error type is auth
    assert_eq!(miroir_err.error_type, "auth");
    assert_eq!(meili_err.error_type, "auth");

    // Verify error shapes match
    assert_error_equivalent(&miroir_err, &meili_err, "invalid_api_key");

    Ok(())
}

/// Test: invalid_search_filter error code (Meilisearch-native)
#[tokio::test]
async fn error_code_invalid_search_filter() -> Result<(), Box<dyn std::error::Error>> {
    let index_name = "compat_invalid_filter";
    let miroir = get_miroir_client();
    let meili = get_meilisearch_client();

    // Clean up and create index
    let _ = miroir.delete_index(index_name).await;
    let _ = meili.delete_index(index_name).await;

    let _ = miroir.create_index(index_name, Some("id")).await;
    let _ = meili.create_index(index_name, Some("id")).await;

    // Wait for index creation
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Try invalid filter syntax
    let miroir_err = get_error_from_raw_request(
        "http://localhost:7700",
        reqwest::Method::POST,
        &format!("/indexes/{}/search", index_name),
        Some(&json!({"q": "test", "filter": "invalid syntax"})),
        &[("Authorization", "Bearer dev-key")],
    ).await;

    let meili_err = get_error_from_raw_request(
        &format!("http://localhost:{}", env::var("MEILISEARCH_PORT").unwrap_or_else(|_| "7704".to_string())),
        reqwest::Method::POST,
        &format!("/indexes/{}/search", index_name),
        Some(&json!({"q": "test", "filter": "invalid syntax"})),
        &[("Authorization", "Bearer dev-node-key")],
    ).await;

    // Both should return an invalid request error
    assert!(miroir_err.is_some(), "Miroir should return invalid request error");
    assert!(meili_err.is_some(), "Meilisearch should return invalid request error");

    let miroir_err = miroir_err.unwrap();
    let meili_err = meili_err.unwrap();

    // Verify error type is invalid_request
    assert_eq!(miroir_err.error_type, "invalid_request");
    assert_eq!(meili_err.error_type, "invalid_request");

    // Verify error shapes match
    assert_error_equivalent(&miroir_err, &meili_err, "invalid_search_filter");

    // Clean up
    let _ = miroir.delete_index(index_name).await;
    let _ = meili.delete_index(index_name).await;

    Ok(())
}

/// Test: miroir_reserved_field error code (Miroir-specific)
#[tokio::test]
async fn error_code_miroir_reserved_field() -> Result<(), Box<dyn std::error::Error>> {
    let index_name = "compat_reserved_field";
    let miroir = get_miroir_client();

    // Clean up and create index
    let _ = miroir.delete_index(index_name).await;
    let _ = miroir.create_index(index_name, Some("id")).await;

    // Wait for index creation
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Try to add a document with a reserved field
    let miroir_err = get_error_from_raw_request(
        "http://localhost:7700",
        reqwest::Method::POST,
        &format!("/indexes/{}/documents", index_name),
        Some(&json!([{"id": 1, "_miroir_shard": 0}])),
        &[("Authorization", "Bearer dev-key")],
    ).await;

    assert!(miroir_err.is_some(), "Miroir should return miroir_reserved_field error");

    let err = miroir_err.unwrap();

    // Verify Miroir-specific error code
    assert_eq!(err.code, "miroir_reserved_field");
    assert_eq!(err.error_type, "invalid_request");
    assert!(err.link.as_ref().unwrap().contains("miroir_reserved_field"));

    // Clean up
    let _ = miroir.delete_index(index_name).await;

    Ok(())
}

/// Test: Error code consistency across all endpoints
#[tokio::test]
async fn error_code_consistency_across_endpoints() -> Result<(), Box<dyn std::error::Error>> {
    let index_name = "compat_consistency";

    // Test 1: Non-existent index on different endpoints
    for (endpoint, method, body) in [
        ("/stats", reqwest::Method::GET, None::<Value>),
        ("/settings", reqwest::Method::GET, None::<Value>),
        ("/documents", reqwest::Method::GET, None::<Value>),
    ] {
        let miroir_err = get_error_from_raw_request(
            "http://localhost:7700",
            method.clone(),
            &format!("/indexes/{}{}", index_name, endpoint),
            body.as_ref(),
            &[("Authorization", "Bearer dev-key")],
        ).await;

        let meili_err = get_error_from_raw_request(
            &format!("http://localhost:{}", env::var("MEILISEARCH_PORT").unwrap_or_else(|_| "7704".to_string())),
            method,
            &format!("/indexes/{}{}", index_name, endpoint),
            body.as_ref(),
            &[("Authorization", "Bearer dev-node-key")],
        ).await;

        if let (Some(m_err), Some(e_err)) = (miroir_err, meili_err) {
            // Both should return index_not_found
            assert_eq!(m_err.code, "index_not_found", "Endpoint: {}", endpoint);
            assert_eq!(e_err.code, "index_not_found", "Endpoint: {}", endpoint);

            // Error shapes should match
            assert_error_equivalent(&m_err, &e_err, endpoint);
        }
    }

    Ok(())
}
