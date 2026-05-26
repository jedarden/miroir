//! Rendezvous hash assignment benchmark (plan §8).
//!
//! Target: Rendezvous assignment (64 shards, 3 nodes, 10K docs) < 1 ms total

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use miroir_core::router::{assign_shard_in_group, score, shard_for_key};
use miroir_core::topology::{Node, NodeId, Topology};

fn create_topology_with_nodes(node_count: u32, shards: u32, rf: usize) -> Topology {
    let mut topo = Topology::new(shards, 1, rf);
    for i in 0..node_count {
        topo.add_node(Node::new(
            NodeId::new(format!("node-{}", i)),
            format!("http://node-{}:7700", i),
            0,
        ));
    }
    topo
}

fn bench_shard_for_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_for_key");
    group.throughput(Throughput::Elements(1));

    for shards in [16, 32, 64, 128] {
        group.bench_with_input(BenchmarkId::from_parameter(shards), &shards, |b, &shards| {
            b.iter(|| {
                black_box(shard_for_key(black_box("user-12345"), shards));
            });
        });
    }
    group.finish();
}

fn bench_score(c: &mut Criterion) {
    let mut group = c.benchmark_group("score");
    group.throughput(Throughput::Elements(1));

    group.bench_function("score_computation", |b| {
        let shard_id = 42;
        let node_id = "node-5";
        b.iter(|| {
            black_box(score(black_box(shard_id), black_box(node_id)));
        });
    });
    group.finish();
}

fn bench_assign_shard_in_group(c: &mut Criterion) {
    let mut group = c.benchmark_group("assign_shard_in_group");

    for (shards, nodes, rf) in [(64, 3, 2), (64, 10, 2), (128, 20, 3)] {
        let _topo = create_topology_with_nodes(nodes, shards, rf);
        let node_ids: Vec<NodeId> = (0..nodes).map(|i| NodeId::new(format!("node-{}", i))).collect();

        group.bench_with_input(
            BenchmarkId::new("assign_shard_in_group", format!("s{}_n{}_rf{}", shards, nodes, rf)),
            &(shards, rf, &node_ids),
            |b, (_shards, rf, node_ids)| {
                b.iter(|| {
                    for shard_id in 0..shards {
                        black_box(assign_shard_in_group(black_box(shard_id), black_box(node_ids), *rf));
                    }
                });
            },
        );
    }
    group.finish();
}

fn bench_rendezvous_assignment_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("rendezvous_batch");
    group.sample_size(100);

    for doc_count in [1_000, 5_000, 10_000] {
        let _topo = create_topology_with_nodes(3, 64, 2);
        let node_ids: Vec<NodeId> = (0..3).map(|i| NodeId::new(format!("node-{}", i))).collect();
        let keys: Vec<String> = (0..doc_count).map(|i| format!("doc-{}", i)).collect();

        group.throughput(Throughput::Elements(doc_count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(doc_count), &doc_count, |b, &_doc_count| {
            b.iter(|| {
                for key in &keys {
                    let shard_id = shard_for_key(key, 64);
                    black_box(assign_shard_in_group(black_box(shard_id), black_box(&node_ids), 2));
                }
            });
        });
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(10));
    targets = bench_shard_for_key, bench_score, bench_assign_shard_in_group, bench_rendezvous_assignment_batch
);
criterion_main!(benches);
