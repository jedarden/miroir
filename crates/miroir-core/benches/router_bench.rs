//! Criterion benchmarks for router rendezvous assignment.
//!
//! Target (plan §8):
//! - Rendezvous assignment (64 shards, 3 nodes, 10K docs) < 1 ms total

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use miroir_core::router::{self, shard_for_key};
use miroir_core::topology::NodeId;

const TARGET_SHARDS: u32 = 64;
const TARGET_NODES: usize = 3;
const TARGET_DOCS: usize = 10_000;

/// Benchmark: shard_for_key for a single document.
fn bench_shard_for_key(c: &mut Criterion) {
    let key = "document:user:12345:post:abcdef";

    c.bench_function("shard_for_key_single", |b| {
        b.iter(|| {
            black_box(shard_for_key(black_box(key), black_box(TARGET_SHARDS)));
        });
    });
}

/// Benchmark: shard_for_key for multiple documents (simulating batch assignment).
fn bench_shard_for_key_batch(c: &mut Criterion) {
    let keys: Vec<String> = (0..TARGET_DOCS)
        .map(|i| format!("document:user:{}:post:{}", i % 100, i))
        .collect();

    c.bench_function("shard_for_key_10k_docs", |b| {
        b.iter(|| {
            let _ = keys
                .iter()
                .map(|k| black_box(shard_for_key(black_box(k), black_box(TARGET_SHARDS))))
                .collect::<Vec<_>>();
        });
    });
}

/// Benchmark: assign_shard_in_group for a single shard.
fn bench_assign_shard_single(c: &mut Criterion) {
    let nodes: Vec<NodeId> = (0..TARGET_NODES)
        .map(|i| NodeId::new(format!("node-{i}")))
        .collect();

    c.bench_function("assign_shard_in_group_single", |b| {
        b.iter(|| {
            black_box(router::assign_shard_in_group(
                black_box(0),
                black_box(&nodes),
                black_box(2),
            ));
        });
    });
}

/// Benchmark: assign_shard_in_group for all shards.
fn bench_assign_shard_all(c: &mut Criterion) {
    let nodes: Vec<NodeId> = (0..TARGET_NODES)
        .map(|i| NodeId::new(format!("node-{i}")))
        .collect();

    c.bench_function("assign_shard_in_group_64_shards", |b| {
        b.iter(|| {
            let _ = (0..TARGET_SHARDS)
                .map(|shard_id| {
                    black_box(router::assign_shard_in_group(
                        black_box(shard_id),
                        black_box(&nodes),
                        black_box(2),
                    ))
                })
                .collect::<Vec<_>>();
        });
    });
}

/// Benchmark: Full document routing pipeline.
///
/// This benchmarks the complete path: hash key -> get shard -> assign nodes.
/// For 10K documents with 64 shards and 3 nodes (RF=2).
fn bench_full_routing_pipeline(c: &mut Criterion) {
    let docs: Vec<String> = (0..TARGET_DOCS)
        .map(|i| format!("document:user:{}:post:{}", i % 100, i))
        .collect();

    let nodes: Vec<NodeId> = (0..TARGET_NODES)
        .map(|i| NodeId::new(format!("node-{i}")))
        .collect();

    // Pre-compute shard assignments
    let shard_assignments: Vec<Vec<NodeId>> = (0..TARGET_SHARDS)
        .map(|shard_id| router::assign_shard_in_group(shard_id, &nodes, 2))
        .collect();

    c.bench_function("full_routing_10k_docs", |b| {
        b.iter(|| {
            let _ = docs
                .iter()
                .map(|doc_key| {
                    let shard_id = shard_for_key(black_box(doc_key), black_box(TARGET_SHARDS));
                    black_box(&shard_assignments[shard_id as usize])
                })
                .collect::<Vec<_>>();
        });
    });
}

/// Benchmark: Varying shard counts.
fn bench_varying_shard_count(c: &mut Criterion) {
    let nodes: Vec<NodeId> = (0..TARGET_NODES)
        .map(|i| NodeId::new(format!("node-{i}")))
        .collect();

    let mut group = c.benchmark_group("varying_shard_count");
    for shard_count in [8, 16, 32, 64, 128, 256].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(shard_count),
            shard_count,
            |b, &sc| {
                b.iter(|| {
                    let _ = (0..sc)
                        .map(|shard_id| {
                            black_box(router::assign_shard_in_group(
                                black_box(shard_id),
                                black_box(&nodes),
                                black_box(2),
                            ))
                        })
                        .collect::<Vec<_>>();
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Varying node counts.
fn bench_varying_node_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("varying_node_count");
    for node_count in [2, 3, 4, 5, 8, 10].iter() {
        let nodes: Vec<NodeId> = (0..*node_count)
            .map(|i| NodeId::new(format!("node-{i}")))
            .collect();

        group.bench_with_input(
            BenchmarkId::from_parameter(node_count),
            node_count,
            |b, &nc| {
                b.iter(|| {
                    let _ = (0..TARGET_SHARDS)
                        .map(|shard_id| {
                            black_box(router::assign_shard_in_group(
                                black_box(shard_id),
                                black_box(&nodes),
                                black_box(2.min(nc)),
                            ))
                        })
                        .collect::<Vec<_>>();
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Varying replication factors.
fn bench_varying_rf(c: &mut Criterion) {
    let nodes: Vec<NodeId> = (0..10)
        .map(|i| NodeId::new(format!("node-{i}")))
        .collect();

    let mut group = c.benchmark_group("varying_rf");
    for rf in [1, 2, 3, 5].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(rf), rf, |b, &rf_val| {
            b.iter(|| {
                let _ = (0..TARGET_SHARDS)
                    .map(|shard_id| {
                        black_box(router::assign_shard_in_group(
                            black_box(shard_id),
                            black_box(&nodes),
                            black_box(rf_val),
                        ))
                    })
                    .collect::<Vec<_>>();
            });
        });
    }
    group.finish();
}

/// Benchmark: Score function directly.
fn bench_score(c: &mut Criterion) {
    let node = "node-1";

    c.bench_function("score_single", |b| {
        b.iter(|| {
            black_box(router::score(black_box(42), black_box(node)));
        });
    });
}

criterion_group!(
    benches,
    bench_shard_for_key,
    bench_shard_for_key_batch,
    bench_assign_shard_single,
    bench_assign_shard_all,
    bench_full_routing_pipeline,
    bench_varying_shard_count,
    bench_varying_node_count,
    bench_varying_rf,
    bench_score
);
criterion_main!(benches);
