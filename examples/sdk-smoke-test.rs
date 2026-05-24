// Miroir SDK smoke test — Rust
// Tests: create index, add documents, search, update settings, delete index
//
// Requirements:
//   Add to Cargo.toml:
//     [dependencies]
//     meilisearch-sdk = "0.27"
//     serde = { version = "1.0", features = ["derive"] }
//     tokio = { version = "1", features = ["full"] }
//
// Run against docker-compose-dev:
//   MIROIR_URL=http://localhost:7700 MIROIR_MASTER_KEY=dev-key cargo run --example rust_smoke_test

use meilisearch_sdk::{client::Client, indexes::Indexes, settings::Settings, task::Task};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Serialize, Deserialize)]
struct Document {
    id: u64,
    title: String,
    author: String,
    year: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("MIROIR_URL").unwrap_or_else(|_| "http://localhost:7700".to_string());
    let api_key = env::var("MIROIR_MASTER_KEY").unwrap_or_else(|_| "dev-key".to_string());

    println!("=== Miroir Rust SDK Smoke Test ===");
    println!("Target: {}", url);

    let client = Client::new(url, Some(api_key));
    let index_name = "test_rust_sdk";

    // Clean up any existing test index
    let _ = client.delete_index(index_name).await;
    println!("✓ Cleaned up existing index '{}'", index_name);

    // 1. Create index
    println!("\n1. Creating index...");
    let task: Task = client
        .create_index(index_name, Some("id"))
        .await?;
    println!("   ✓ Created index '{}' with primary key 'id' (task {})", index_name, task.uid());

    // Wait for index creation
    client.wait_for_task(task.uid(), None, None).await?;

    // 2. Add documents
    println!("\n2. Adding documents...");
    let documents = vec![
        Document {
            id: 1,
            title: "The Great Gatsby".to_string(),
            author: "F. Scott Fitzgerald".to_string(),
            year: 1925,
        },
        Document {
            id: 2,
            title: "To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
            year: 1960,
        },
        Document {
            id: 3,
            title: "1984".to_string(),
            author: "George Orwell".to_string(),
            year: 1949,
        },
    ];

    let task: Task = client.index(index_name).add_documents(&documents, None).await?;
    println!("   ✓ Added {} documents (task {})", documents.len(), task.uid());

    // Wait for indexing
    client.wait_for_task(task.uid(), None, None).await?;

    // 3. Search
    println!("\n3. Searching...");
    let index = client.index(index_name);
    let results = index.search().with_query("gatsby").execute::<Document>().await?;

    println!("   ✓ Found {} hits for 'gatsby'", results.hits.len());

    assert_eq!(results.hits.len(), 1, "Expected 1 hit");
    assert_eq!(results.hits[0].title, "The Great Gatsby");

    // 4. Update settings
    println!("\n4. Updating settings...");
    let task: Task = index
        .set_settings(&Settings::new()
            .with_searchable_attributes(["title", "author"])
            .with_filterable_attributes(["year"]))
        .await?;
    println!("   ✓ Updated settings (task {})", task.uid());

    // Wait for settings
    client.wait_for_task(task.uid(), None, None).await?;

    // 5. Delete index
    println!("\n5. Deleting index...");
    let task: Task = client.delete_index(index_name).await?;
    println!("   ✓ Deleted index '{}' (task {})", index_name, task.uid());

    println!("\n=== All Rust SDK tests passed! ===");
    Ok(())
}
