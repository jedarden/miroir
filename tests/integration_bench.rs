//! Integration benchmarks for end-to-end performance.
//!
//! These benchmarks require a running docker-compose stack:
//! ```bash
//! cd examples && docker-compose -f docker-compose-dev.yml up -d
//! ```
//!
//! Targets (plan §8):
//! - End-to-end search latency vs single-node: < 2× single-node
//! - Ingest throughput (1000 docs through Miroir): > 80% of single-node
//!
//! Run with:
//! ```bash
//! cargo test --test integration_bench -- --nocapture --test-threads=1
//! ```

use meilisearch_sdk::{Client, Index};
use serde_json::json;
use std::time::{Duration, Instant};

const MIROIR_URL: &str = "http://localhost:7700";
const STANDALONE_URL: &str = "http://localhost:7704";
const MASTER_KEY: &str = "dev-node-key";

/// Helper to create a test document with a unique ID.
fn make_doc(i: usize) -> serde_json::Value {
    json!({
        "id": format!("doc-{:06}", i),
        "title": format!("Document {}", i),
        "content": format!("This is the content of document {}", i),
        "category": ["tech", "science", "art"][i % 3],
        "tags": vec![
            format!("tag-{}", i % 10),
            format!("tag-{}", (i + 1) % 10),
        ],
        "timestamp": i as u64,
    })
}

/// Helper to wait for an index to be processed.
async fn wait_for_index(index: &Index, expected_docs: usize) -> Result<(), Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(30);
    let start = Instant::now();

    while start.elapsed() < timeout {
        let stats = index.get_stats().await?;
        if stats.number_of_documents == expected_docs as u64 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(format!("Index processing timeout: expected {} docs", expected_docs).into())
}

/// Benchmark: End-to-end search latency comparison.
///
/// Measures search latency through Miroir vs standalone Meilisearch.
/// Target: Miroir latency < 2× standalone (plan §8).
#[tokio::test]
#[ignore] // Run with --ignored
async fn bench_e2e_search_latency() -> Result<(), Box<dyn std::error::Error>> {
    let num_docs = 1000;
    let num_queries = 100;

    // Setup Miroir index
    let miroir_client = Client::new(MIROIR_URL, MASTER_KEY);
    let miroir_index = miroir_client.create_index("bench-search-miroir", Some("id")).await?;
    let miroir_index = miroir_client.index("bench-search-miroir");

    // Setup standalone index
    let standalone_client = Client::new(STANDALONE_URL, MASTER_KEY);
    let standalone_index = standalone_client.create_index("bench-search-standalone", Some("id")).await?;
    let standalone_index = standalone_client.index("bench-search-standalone");

    // Index documents in both
    let docs: Vec<_> = (0..num_docs).map(make_doc).collect();
    miroir_index.add_documents(&docs, None).await?;
    standalone_index.add_documents(&docs, None).await?;

    // Wait for processing
    wait_for_index(&miroir_index, num_docs).await?;
    wait_for_index(&standalone_index, num_docs).await?;

    // Warmup queries
    for i in 0..5 {
        let _ = miroir_index.search().with_query(format!("document {}", i)).execute().await?;
        let _ = standalone_index.search().with_query(format!("document {}", i)).execute().await?;
    }

    // Benchmark searches
    let mut miroir_times = Vec::with_capacity(num_queries);
    let mut standalone_times = Vec::with_capacity(num_queries);

    for i in 0..num_queries {
        // Miroir search
        let start = Instant::now();
        let _ = miroir_index.search().with_query(format!("document {}", i)).execute().await?;
        miroir_times.push(start.elapsed());

        // Standalone search
        let start = Instant::now();
        let _ = standalone_index.search().with_query(format!("document {}", i)).execute().await?;
        standalone_times.push(start.elapsed());
    }

    // Calculate statistics
    let miroir_avg: Duration = miroir_times.iter().sum::<Duration>() / num_queries as u32;
    let standalone_avg: Duration = standalone_times.iter().sum::<Duration>() / num_queries as u32;

    let ratio = miroir_avg.as_micros() as f64 / standalone_avg.as_micros() as f64;

    println!("\n=== End-to-End Search Latency Benchmark ===");
    println!("Documents: {}", num_docs);
    println!("Queries: {}", num_queries);
    println!("Miroir average: {:?}", miroir_avg);
    println!("Standalone average: {:?}", standalone_avg);
    println!("Ratio (Miroir/Standalone): {:.2}×", ratio);

    // Check target: < 2× single-node
    assert!(
        ratio < 2.0,
        "Search latency ratio {:.2}× exceeds 2× target (plan §8)",
        ratio
    );

    println!("✓ PASSED: Search latency < 2× standalone");

    // Cleanup
    let _ = miroir_client.delete_index("bench-search-miroir").await;
    let _ = standalone_client.delete_index("bench-search-standalone").await;

    Ok(())
}

/// Benchmark: Ingest throughput comparison.
///
/// Measures time to index 1000 documents through Miroir vs standalone.
/// Target: Miroir throughput > 80% of standalone (plan §8).
#[tokio::test]
#[ignore] // Run with --ignored
async fn bench_ingest_throughput() -> Result<(), Box<dyn std::error::Error>> {
    let num_docs = 1000;

    // Setup Miroir index
    let miroir_client = Client::new(MIROIR_URL, MASTER_KEY);
    let miroir_index = miroir_client.create_index("bench-ingest-miroir", Some("id")).await?;
    let miroir_index = miroir_client.index("bench-ingest-miroir");

    // Setup standalone index
    let standalone_client = Client::new(STANDALONE_URL, MASTER_KEY);
    let standalone_index = standalone_client.create_index("bench-ingest-standalone", Some("id")).await?;
    let standalone_index = standalone_client.index("bench-ingest-standalone");

    // Prepare documents
    let docs: Vec<_> = (0..num_docs).map(make_doc).collect();

    // Benchmark Miroir ingest
    let start = Instant::now();
    miroir_index.add_documents(&docs, None).await?;
    wait_for_index(&miroir_index, num_docs).await?;
    let miroir_duration = start.elapsed();

    // Benchmark standalone ingest
    let start = Instant::now();
    standalone_index.add_documents(&docs, None).await?;
    wait_for_index(&standalone_index, num_docs).await?;
    let standalone_duration = start.elapsed();

    // Calculate throughput
    let miroir_throughput = num_docs as f64 / miroir_duration.as_secs_f64();
    let standalone_throughput = num_docs as f64 / standalone_duration.as_secs_f64();
    let ratio = miroir_throughput / standalone_throughput;

    println!("\n=== Ingest Throughput Benchmark ===");
    println!("Documents: {}", num_docs);
    println!("Miroir: {:.2} docs/sec ({:?} total)", miroir_throughput, miroir_duration);
    println!("Standalone: {:.2} docs/sec ({:?} total)", standalone_throughput, standalone_duration);
    println!("Ratio (Miroir/Standalone): {:.2}%", ratio * 100.0);

    // Check target: > 80% of standalone
    assert!(
        ratio >= 0.8,
        "Ingest throughput ratio {:.2}% is below 80% target (plan §8)",
        ratio * 100.0
    );

    println!("✓ PASSED: Ingest throughput > 80% of standalone");

    // Cleanup
    let _ = miroir_client.delete_index("bench-ingest-miroir").await;
    let _ = standalone_client.delete_index("bench-ingest-standalone").await;

    Ok(())
}

/// Benchmark: Batch search performance.
///
/// Tests concurrent search throughput with multiple queries.
#[tokio::test]
#[ignore] // Run with --ignored
async fn bench_concurrent_search() -> Result<(), Box<dyn std::error::Error>> {
    let num_docs = 1000;
    let num_concurrent = 50;

    // Setup index
    let miroir_client = Client::new(MIROIR_URL, MASTER_KEY);
    let miroir_index = miroir_client.create_index("bench-concurrent", Some("id")).await?;
    let miroir_index = miroir_client.index("bench-concurrent");

    let docs: Vec<_> = (0..num_docs).map(make_doc).collect();
    miroir_index.add_documents(&docs, None).await?;
    wait_for_index(&miroir_index, num_docs).await?;

    // Concurrent searches
    let start = Instant::now();
    let mut tasks = Vec::new();

    for i in 0..num_concurrent {
        let index = miroir_index.clone();
        tasks.push(tokio::spawn(async move {
            index
                .search()
                .with_query(format!("document {}", i))
                .execute()
                .await
        }));
    }

    let results = futures::future::join_all(tasks).await;
    let duration = start.elapsed();

    let successful = results.iter().filter(|r| r.is_ok()).count();

    println!("\n=== Concurrent Search Benchmark ===");
    println!("Documents: {}", num_docs);
    println!("Concurrent queries: {}", num_concurrent);
    println!("Duration: {:?}", duration);
    println!("Successful: {}/{}", successful, num_concurrent);
    println!("Throughput: {:.2} queries/sec", num_concurrent as f64 / duration.as_secs_f64());

    assert_eq!(successful, num_concurrent, "All searches should succeed");

    // Cleanup
    let _ = miroir_client.delete_index("bench-concurrent").await;

    Ok(())
}

/// Benchmark: Faceted search performance.
///
/// Tests facet aggregation across shards.
#[tokio::test]
#[ignore] // Run with --ignored
async fn bench_faceted_search() -> Result<(), Box<dyn std::error::Error>> {
    let num_docs = 1000;

    // Setup index with filterable attributes
    let miroir_client = Client::new(MIROIR_URL, MASTER_KEY);
    let miroir_index = miroir_client.create_index("bench-facets", Some("id")).await?;
    let miroir_index = miroir_client.index("bench-facets");

    // Set filterable attributes
    miroir_index
        .set_filterable_attributes(["category", "tags"])
        .await?;

    let docs: Vec<_> = (0..num_docs).map(make_doc).collect();
    miroir_index.add_documents(&docs, None).await?;
    wait_for_index(&miroir_index, num_docs).await?;

    // Benchmark faceted searches
    let start = Instant::now();
    let result = miroir_index
        .search()
        .with_query("document")
        .with_facet_filters(["category = tech"])
        .execute()
        .await?;
    let duration = start.elapsed();

    println!("\n=== Faceted Search Benchmark ===");
    println!("Documents: {}", num_docs);
    println!("Faceted search duration: {:?}", duration);
    println!("Results: {}", result.hits.len());

    // Cleanup
    let _ = miroir_client.delete_index("bench-facets").await;

    Ok(())
}

/// Benchmark: Pagination performance.
///
/// Tests deep pagination performance.
#[tokio::test]
#[ignore] // Run with --ignored
async fn bench_pagination() -> Result<(), Box<dyn std::error::Error>> {
    let num_docs = 1000;
    let page_size = 20;
    let num_pages = 10;

    // Setup index
    let miroir_client = Client::new(MIROIR_URL, MASTER_KEY);
    let miroir_index = miroir_client.create_index("bench-pagination", Some("id")).await?;
    let miroir_index = miroir_client.index("bench-pagination");

    let docs: Vec<_> = (0..num_docs).map(make_doc).collect();
    miroir_index.add_documents(&docs, None).await?;
    wait_for_index(&miroir_index, num_docs).await?;

    // Benchmark pagination
    let start = Instant::now();
    let mut total_hits = 0;

    for page in 0..num_pages {
        let result = miroir_index
            .search()
            .with_query("document")
            .with_limit(page_size)
            .with_offset(page * page_size)
            .execute()
            .await?;

        total_hits += result.hits.len();
    }

    let duration = start.elapsed();

    println!("\n=== Pagination Benchmark ===");
    println!("Documents: {}", num_docs);
    println!("Pages: {}", num_pages);
    println!("Page size: {}", page_size);
    println!("Duration: {:?}", duration);
    println!("Total hits retrieved: {}", total_hits);
    println!("Throughput: {:.2} pages/sec", num_pages as f64 / duration.as_secs_f64());

    assert_eq!(total_hits, num_pages * page_size, "Should retrieve all hits");

    // Cleanup
    let _ = miroir_client.delete_index("bench-pagination").await;

    Ok(())
}
