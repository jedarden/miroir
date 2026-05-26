//! End-to-end search latency benchmark.
//!
//! Measures the total latency of a search request through Miroir
//! compared to a single-node Meilisearch instance.
//!
//! Target: Miroir latency < 2× single-node latency (plan §8).
//!
//! NOTE: This benchmark uses simulated latencies for development.
//! For real measurements, integration tests with live Meilisearch are required.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use miroir_core::topology::{Node, NodeId, Topology};
use std::time::Duration;

/// Simulated single-node search latency.
/// In real integration, this would hit a live Meilisearch instance.
#[inline(never)]
fn simulate_single_node_search(query: &str) -> Duration {
    // Baseline single-node latency: ~5-15ms
    let base = 5_000_000u64; // 5ms
    let per_term = 500_000u64; // 0.5ms per query term

    Duration::from_nanos(base + (query.len() as u64 * per_term))
}

/// Simulated Miroir scatter-gather search latency.
#[inline(never)]
fn simulate_miroir_search(query: &str, _topo: &Topology) -> Duration {
    // Miroir latency components:
    // - Scatter overhead: ~1ms
    // - Network to nodes: ~1ms each way (parallelized)
    // - Per-node search: ~5ms (parallel, so max not sum)
    // - Gather + merge: ~1-2ms
    // Total: ~8-10ms baseline

    let scatter = 1_000_000u64;
    let network = 2_000_000u64; // round trip
    let search = 5_000_000u64; // parallel across nodes
    let merge = 1_500_000u64;
    let per_term = 200_000u64; // less than single-node due to parallelism

    Duration::from_nanos(scatter + network + search + merge + (query.len() as u64 * per_term))
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

fn bench_single_node_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_node_baseline");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for term_count in [1, 2, 3, 5].iter() {
        let query = "laptop ".repeat(*term_count);
        group.bench_with_input(BenchmarkId::new("terms", term_count), &query, |b, q| {
            b.iter(|| black_box(simulate_single_node_search(black_box(q))));
        });
    }

    group.finish();
}

fn bench_miroir_scatter_gather(c: &mut Criterion) {
    let mut group = c.benchmark_group("miroir_scatter_gather");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for (shards, nodes) in [(16, 3), (32, 3), (64, 3), (64, 5)].iter() {
        let topo = create_test_topology(*nodes, *shards, 2, 1);
        let query = "laptop search";

        group.bench_with_input(
            BenchmarkId::new("shards_nodes", format!("{shards}_{nodes}")),
            &(query, topo),
            |b, (q, t)| {
                b.iter(|| black_box(simulate_miroir_search(black_box(q), black_box(t))));
            },
        );
    }

    group.finish();
}

fn bench_latency_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency_ratio");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(30);

    let topo = create_test_topology(3, 64, 2, 1);

    for term_count in [1, 2, 3, 5].iter() {
        let query = "laptop ".repeat(*term_count);

        group.bench_with_input(BenchmarkId::new("terms", term_count), &query, |b, q| {
            b.iter(|| {
                let single = black_box(simulate_single_node_search(q));
                let miroir = black_box(simulate_miroir_search(q, &topo));
                let ratio = (miroir.as_nanos() as f64) / (single.as_nanos() as f64);
                black_box((single, miroir, ratio));
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_node_baseline,
    bench_miroir_scatter_gather,
    bench_latency_ratio
);
criterion_main!(benches);
