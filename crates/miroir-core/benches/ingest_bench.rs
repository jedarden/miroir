//! Ingest throughput benchmark.
//!
//! Measures the throughput of document ingestion through Miroir
//! compared to direct ingestion into a single Meilisearch node.
//!
//! Target: Miroir throughput > 80% of single-node throughput (plan §8).
//!
//! NOTE: This benchmark uses simulated latencies for development.
//! For real measurements, integration tests with live Meilisearch are required.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use miroir_core::router::{shard_for_key, write_targets};
use miroir_core::topology::{Node, NodeId, Topology};
use std::time::Duration;

fn create_test_documents(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("product_{i}")).collect()
}

fn create_test_topology(node_count: u32, shards: u32, rf: usize, rg: u32) -> Topology {
    let mut topo = Topology::new(shards, rg, rf);
    for g in 0..rg {
        for i in 0..node_count {
            topo.add_node(Node::new(
                NodeId::new(format!("node-g{g}-{i}")),
                format!("http://localhost:{}", 7701 + (g * node_count + i) as u16),
                g,
            ));
        }
    }
    topo
}

/// Simulated single-node ingestion latency.
#[inline(never)]
fn simulate_single_node_ingest(docs: &[String]) -> Duration {
    // Single-node per-doc latency: ~0.9ms
    let per_doc = 900_000u64;
    let overhead = 1_000_000u64;

    // Parallelism speedup for larger batches
    let parallelism = if docs.len() > 100 { 0.7 } else { 1.0 };

    Duration::from_nanos(((overhead + per_doc * docs.len() as u64) as f64 * parallelism) as u64)
}

/// Simulated Miroir ingestion latency.
#[inline(never)]
fn simulate_miroir_ingest(docs: &[String], topo: &Topology) -> Duration {
    // Miroir per-doc latency with fanout overhead
    let sharding = 10_000u64;
    let batching = 50_000u64;
    let overhead = 2_000_000u64;
    let per_doc_network = 500_000u64;
    let per_doc_index = 300_000u64;

    let rf = topo.rf() as u64;
    let rg = topo.replica_group_count() as u64;

    // Fanout multiplier
    let fanout = 1.0 + ((rf * rg - 1) as f64 * 0.1);

    let per_doc = sharding + (per_doc_network * rf) + (per_doc_index * rf);
    let total = batching + overhead + ((per_doc * docs.len() as u64) as f64 * fanout) as u64;

    // Parallelism helps for larger batches
    let parallelism = if docs.len() > 100 { 0.75 } else { 1.0 };

    Duration::from_nanos((total as f64 * parallelism) as u64)
}

fn bench_single_node_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_node_ingest");

    for doc_count in [100, 500, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*doc_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(doc_count),
            doc_count,
            |b, count| {
                let docs = create_test_documents(*count);
                b.iter(|| black_box(simulate_single_node_ingest(black_box(&docs))));
            },
        );
    }

    group.finish();
}

fn bench_miroir_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("miroir_ingest");

    for (docs, shards, nodes) in [
        (100, 16, 2),
        (100, 32, 3),
        (500, 32, 3),
        (1000, 64, 3),
        (1000, 64, 5),
        (5000, 64, 5),
    ]
    .iter()
    {
        group.throughput(Throughput::Elements(*docs as u64));
        group.bench_with_input(
            BenchmarkId::new("docs_shards_nodes", format!("{docs}_{shards}_{nodes}")),
            &(docs, shards, nodes),
            |b, (count, shards, nodes)| {
                let docs = create_test_documents(**count);
                let topo = create_test_topology(**nodes, **shards, 2, 1);
                b.iter(|| black_box(simulate_miroir_ingest(black_box(&docs), black_box(&topo))));
            },
        );
    }

    group.finish();
}

fn bench_throughput_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_ratio");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(20);

    let doc_count = 1000;
    let docs = create_test_documents(doc_count);
    let topo = create_test_topology(3, 64, 2, 1);

    group.bench_function("compare_1000_docs", |b| {
        b.iter(|| {
            let single = simulate_single_node_ingest(&docs);
            let miroir = simulate_miroir_ingest(&docs, &topo);
            let ratio = (miroir.as_nanos() as f64) / (single.as_nanos() as f64);
            black_box((single, miroir, ratio));
        });
    });

    group.finish();
}

fn bench_sharding_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("sharding_overhead");

    let docs = create_test_documents(10000);
    let topo = create_test_topology(3, 64, 2, 1);

    group.bench_function("hash_10k_docs", |b| {
        b.iter(|| {
            for doc in &docs {
                let shard_id = shard_for_key(doc, 64);
                let targets = write_targets(shard_id, &topo);
                black_box(targets);
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_node_ingest,
    bench_miroir_ingest,
    bench_throughput_ratio,
    bench_sharding_overhead
);
criterion_main!(benches);
