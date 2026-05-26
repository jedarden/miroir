//! Result merger benchmark (plan §8).
//!
//! Target: Merger (1000 hits, 3 shards) < 1 ms

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use miroir_core::merger;
use serde_json::json;

fn create_shard_response(shard_id: u32, count: usize, score_offset: f64) -> serde_json::Value {
    let hits: Vec<serde_json::Value> = (0..count)
        .map(|i| {
            json!({
                "_rankingScore": score_offset - (i as f64 * 0.001),
                "id": shard_id as u64 * 1000 + i as u64,
                "title": format!("Document {}", shard_id as u64 * 1000 + i as u64),
            })
        })
        .collect();

    json!({
        "hits": hits,
        "processingTimeMs": 10,
        "limit": count,
        "offset": 0,
        "estimatedTotalHits": count * 3,
        "indexUid": format!("test-index-{}", shard_id),
    })
}

fn create_shard_hit_page(shard_id: u32, count: usize, score_offset: f64) -> merger::ShardHitPage {
    let body = create_shard_response(shard_id, count, score_offset);
    merger::ShardHitPage { body }
}

fn bench_merge_hits(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_hits");

    for (shard_count, hits_per_shard) in [(2, 500), (3, 333), (3, 1000), (5, 200), (10, 100)] {
        let total_hits = shard_count * hits_per_shard;
        group.throughput(Throughput::Elements(total_hits as u64));

        group.bench_with_input(
            BenchmarkId::new("merge_hits", format!("s{}_h{}", shard_count, hits_per_shard)),
            &(shard_count, hits_per_shard),
            |b, &(shard_count, hits_per_shard)| {
                let shard_hits: Vec<merger::ShardHitPage> = (0..shard_count)
                    .map(|i| create_shard_hit_page(i as u32, hits_per_shard, 1.0 - (i as f64 * 0.01)))
                    .collect();

                let input = merger::MergeInput {
                    shard_hits,
                    offset: 0,
                    limit: 10,
                    client_requested_score: false,
                    facets: None,
                    failed_shards: vec![],
                    vector_mode: miroir_core::scatter::VectorMode::KeywordOnly,
                    vector_config: None,
                };

                b.iter(|| {
                    black_box(merger::merge(black_box(input.clone())));
                });
            },
        );
    }
    group.finish();
}

fn bench_full_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_merge");

    // Plan §8 target: 1000 hits, 3 shards < 1 ms
    let shard_hits: Vec<merger::ShardHitPage> = (0..3)
        .map(|i| create_shard_hit_page(i as u32, 334, 1.0 - (i as f64 * 0.01)))
        .collect();

    group.throughput(Throughput::Elements(1002));
    group.bench_function("plan_target_1000_hits_3_shards", |b| {
        let input = merger::MergeInput {
            shard_hits: shard_hits.clone(),
            offset: 0,
            limit: 20,
            client_requested_score: false,
            facets: None,
            failed_shards: vec![],
            vector_mode: miroir_core::scatter::VectorMode::KeywordOnly,
            vector_config: None,
        };

        b.iter(|| {
            black_box(merger::merge(black_box(input.clone())));
        });
    });

    // Larger dataset: 5000 hits, 5 shards
    let large_shard_hits: Vec<merger::ShardHitPage> = (0..5)
        .map(|i| create_shard_hit_page(i as u32, 1000, 1.0 - (i as f64 * 0.01)))
        .collect();

    group.throughput(Throughput::Elements(5000));
    group.bench_function("large_5000_hits_5_shards", |b| {
        let input = merger::MergeInput {
            shard_hits: large_shard_hits.clone(),
            offset: 0,
            limit: 100,
            client_requested_score: false,
            facets: None,
            failed_shards: vec![],
            vector_mode: miroir_core::scatter::VectorMode::KeywordOnly,
            vector_config: None,
        };

        b.iter(|| {
            black_box(merger::merge(black_box(input.clone())));
        });
    });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(10));
    targets = bench_merge_hits, bench_full_merge
);
criterion_main!(benches);
