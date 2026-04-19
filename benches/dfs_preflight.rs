//! Benchmark for DFS (Distributed Frequency Search) preflight overhead.
//!
//! This benchmark measures the latency overhead of the preflight phase
//! in the dfs_query_then_fetch pattern. The preflight phase:
//! 1. Sends term-frequency requests to all shards
//! 2. Aggregates global IDF at the coordinator
//! 3. Returns global IDF for use in the actual search phase
//!
//! The overhead is primarily determined by:
//! - Number of shards (parallel requests)
//! - Number of query terms (serial queries per shard in current implementation)
//! - Network latency to each shard

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use miroir_core::config::UnavailableShardPolicy;
use miroir_core::scatter::{
    execute_preflight, plan_search_scatter, PreflightRequest, MockNodeClient, PreflightResponse, TermStats,
};
use miroir_core::topology::{Node, NodeId, Topology};
use std::collections::HashMap;

fn create_topology_with_nodes(node_count: u32, shards: u32) -> Topology {
    let mut topo = Topology::new(shards, 1, 1);
    for i in 0..node_count {
        topo.add_node(Node::new(
            NodeId::new(format!("node-{}", i)),
            format!("http://node-{}:7700", i),
            0,
        ));
    }
    topo
}

fn create_mock_client(
    node_count: u32,
    total_docs_per_shard: u64,
    df_per_term: u64,
    term_count: usize,
) -> MockNodeClient {
    let mut client = MockNodeClient::default();

    for i in 0..node_count {
        let mut term_stats = HashMap::new();
        for j in 0..term_count {
            term_stats.insert(format!("term{}", j), TermStats { df: df_per_term });
        }

        client.preflight_responses.insert(
            NodeId::new(format!("node-{}", i)),
            PreflightResponse {
                total_docs: total_docs_per_shard,
                avg_doc_length: 50.0,
                term_stats,
            },
        );
    }

    client
}

fn bench_preflight(c: &mut Criterion) {
    let mut group = c.benchmark_group("dfs_preflight");

    // Benchmark varying shard counts (fixed terms = 5)
    for shard_count in [4, 8, 16, 32, 64].iter() {
        let node_count = *shard_count; // One node per shard for this benchmark
        let topo = create_topology_with_nodes(node_count, *shard_count);
        let plan = plan_search_scatter(&topo, 0, 1, *shard_count);
        let client = create_mock_client(node_count, 10_000, 1_000, 5);

        let req = PreflightRequest {
            index_uid: "test".into(),
            terms: (0..5).map(|i| format!("term{}", i)).collect(),
            filter: None,
        };

        group.bench_with_input(
            BenchmarkId::new("shards", shard_count),
            shard_count,
            |b, _| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| execute_preflight(black_box(&plan), black_box(&client), black_box(&req), black_box(&topo)));
            },
        );
    }

    group.finish();
}

fn bench_preflight_term_counts(c: &mut Criterion) {
    let mut group = c.benchmark_group("dfs_preflight_terms");

    // Benchmark varying term counts (fixed shards = 16)
    let shard_count = 16;
    for term_count in [1, 3, 5, 10, 20].iter() {
        let topo = create_topology_with_nodes(shard_count, shard_count);
        let plan = plan_search_scatter(&topo, 0, 1, shard_count);
        let client = create_mock_client(shard_count, 10_000, 1_000, *term_count);

        let req = PreflightRequest {
            index_uid: "test".into(),
            terms: (0..*term_count).map(|i| format!("term{}", i)).collect(),
            filter: None,
        };

        group.bench_with_input(
            BenchmarkId::new("terms", term_count),
            term_count,
            |b, _| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| execute_preflight(black_box(&plan), black_box(&client), black_box(&req), black_box(&topo)));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_preflight, bench_preflight_term_counts);
criterion_main!(benches);
