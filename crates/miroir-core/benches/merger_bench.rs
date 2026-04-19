//! Criterion benchmarks for merger.
//!
//! Target (plan §8):
//! - Merger (1000 hits, 3 shards) < 1 ms

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use miroir_core::merger::{merge, MergeInput, ShardHitPage};
use serde_json::json;

const TARGET_HITS: usize = 1000;
const TARGET_SHARDS: usize = 3;

/// Helper to create a hit document.
fn make_hit(id: &str, score: f64) -> serde_json::Value {
    json!({
        "id": id,
        "title": format!("Document {}", id),
        "_rankingScore": score,
        "_miroir_shard": id.parse::<u32>().unwrap_or(0) % TARGET_SHARDS as u32,
    })
}

/// Helper to create a shard response with hits.
fn make_shard_response(
    hits: Vec<serde_json::Value>,
    total_hits: u64,
    processing_time: u64,
) -> ShardHitPage {
    ShardHitPage {
        body: json!({
            "hits": hits,
            "estimatedTotalHits": total_hits,
            "processingTimeMs": processing_time,
            "facetDistribution": {
                "category": {
                    "electronics": 50,
                    "books": 30,
                },
            },
        }),
    }
}

/// Benchmark: Merge 1000 hits from 3 shards.
///
/// This is the primary benchmark target for plan §8.
/// Each shard returns ~333 hits, globally sorted, with offset=0, limit=1000.
fn bench_merge_1000_hits_3_shards(c: &mut Criterion) {
    let hits_per_shard = TARGET_HITS / TARGET_SHARDS;

    let shard_hits: Vec<ShardHitPage> = (0..TARGET_SHARDS)
        .map(|shard_id| {
            let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                .map(|i| {
                    let id = shard_id * hits_per_shard + i;
                    let score = (TARGET_HITS - id) as f64 / TARGET_HITS as f64;
                    make_hit(&id.to_string(), score)
                })
                .collect();
            make_shard_response(hits, hits_per_shard as u64, 15)
        })
        .collect();

    let input = MergeInput {
        shard_hits,
        offset: 0,
        limit: TARGET_HITS,
        client_requested_score: false,
        facets: None,
    };

    c.bench_function("merge_1000_hits_3_shards", |b| {
        b.iter(|| {
            black_box(merge(black_box(input.clone()))).unwrap();
        });
    });
}

/// Benchmark: Merge with varying hit counts.
fn bench_varying_hit_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("varying_hit_count");

    for hit_count in [100, 500, 1000, 5000, 10000].iter() {
        let hits_per_shard = hit_count / TARGET_SHARDS;

        let shard_hits: Vec<ShardHitPage> = (0..TARGET_SHARDS)
            .map(|shard_id| {
                let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                    .map(|i| {
                        let id = shard_id * hits_per_shard + i;
                        let score = (*hit_count - id) as f64 / *hit_count as f64;
                        make_hit(&id.to_string(), score)
                    })
                    .collect();
                make_shard_response(hits, hits_per_shard as u64, 15)
            })
            .collect();

        let input = MergeInput {
            shard_hits,
            offset: 0,
            limit: *hit_count,
            client_requested_score: false,
            facets: None,
        };

        group.bench_with_input(BenchmarkId::from_parameter(hit_count), hit_count, |b, _| {
            b.iter(|| {
                black_box(merge(black_box(input.clone()))).unwrap();
            });
        });
    }
    group.finish();
}

/// Benchmark: Merge with varying shard counts.
fn bench_varying_shard_count(c: &mut Criterion) {
    let total_hits = TARGET_HITS;
    let mut group = c.benchmark_group("varying_shard_count");

    for shard_count in [1, 2, 3, 5, 10].iter() {
        let hits_per_shard = total_hits / shard_count;

        let shard_hits: Vec<ShardHitPage> = (0..*shard_count)
            .map(|shard_id| {
                let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                    .map(|i| {
                        let id = shard_id * hits_per_shard + i;
                        let score = (total_hits - id) as f64 / total_hits as f64;
                        make_hit(&id.to_string(), score)
                    })
                    .collect();
                make_shard_response(hits, hits_per_shard as u64, 15)
            })
            .collect();

        let input = MergeInput {
            shard_hits,
            offset: 0,
            limit: total_hits,
            client_requested_score: false,
            facets: None,
        };

        group.bench_with_input(BenchmarkId::from_parameter(shard_count), shard_count, |b, _| {
            b.iter(|| {
                black_box(merge(black_box(input.clone()))).unwrap();
            });
        });
    }
    group.finish();
}

/// Benchmark: Merge with offset/limit pagination.
fn bench_pagination(c: &mut Criterion) {
    let hits_per_shard = TARGET_HITS / TARGET_SHARDS;

    let shard_hits: Vec<ShardHitPage> = (0..TARGET_SHARDS)
        .map(|shard_id| {
            let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                .map(|i| {
                    let id = shard_id * hits_per_shard + i;
                    let score = (TARGET_HITS - id) as f64 / TARGET_HITS as f64;
                    make_hit(&id.to_string(), score)
                })
                .collect();
            make_shard_response(hits, hits_per_shard as u64, 15)
        })
        .collect();

    let mut group = c.benchmark_group("pagination");

    // Test different (offset, limit) combinations
    let cases = vec![
        (0, 10, "first_page"),
        (10, 10, "second_page"),
        (100, 10, "deep_page"),
        (0, 100, "large_page"),
        (0, 500, "half_result"),
    ];

    for (offset, limit, name) in cases {
        let input = MergeInput {
            shard_hits: shard_hits.clone(),
            offset,
            limit,
            client_requested_score: false,
            facets: None,
        };

        group.bench_function(name, |b| {
            b.iter(|| {
                black_box(merge(black_box(input.clone()))).unwrap();
            });
        });
    }
    group.finish();
}

/// Benchmark: Merge with facets.
fn bench_with_facets(c: &mut Criterion) {
    let hits_per_shard = TARGET_HITS / TARGET_SHARDS;

    let shard_hits: Vec<ShardHitPage> = (0..TARGET_SHARDS)
        .map(|shard_id| {
            let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                .map(|i| {
                    let id = shard_id * hits_per_shard + i;
                    let score = (TARGET_HITS - id) as f64 / TARGET_HITS as f64;
                    make_hit(&id.to_string(), score)
                })
                .collect();
            ShardHitPage {
                body: json!({
                    "hits": hits,
                    "estimatedTotalHits": hits_per_shard as u64,
                    "processingTimeMs": 15,
                    "facetDistribution": {
                        "category": {
                            "electronics": 50,
                            "books": 30,
                            "clothing": 20,
                        },
                        "brand": {
                            "apple": 25,
                            "samsung": 15,
                            "sony": 10,
                        },
                    },
                }),
            }
        })
        .collect();

    let input = MergeInput {
        shard_hits,
        offset: 0,
        limit: TARGET_HITS,
        client_requested_score: false,
        facets: Some(vec!["category".to_string(), "brand".to_string()]),
    };

    c.bench_function("merge_with_facets", |b| {
        b.iter(|| {
            black_box(merge(black_box(input.clone()))).unwrap();
        });
    });
}

/// Benchmark: Merge with score preservation.
fn bench_with_score_preservation(c: &mut Criterion) {
    let hits_per_shard = TARGET_HITS / TARGET_SHARDS;

    let shard_hits: Vec<ShardHitPage> = (0..TARGET_SHARDS)
        .map(|shard_id| {
            let hits: Vec<serde_json::Value> = (0..hits_per_shard)
                .map(|i| {
                    let id = shard_id * hits_per_shard + i;
                    let score = (TARGET_HITS - id) as f64 / TARGET_HITS as f64;
                    make_hit(&id.to_string(), score)
                })
                .collect();
            make_shard_response(hits, hits_per_shard as u64, 15)
        })
        .collect();

    let input = MergeInput {
        shard_hits,
        offset: 0,
        limit: TARGET_HITS,
        client_requested_score: true,
        facets: None,
    };

    c.bench_function("merge_with_score", |b| {
        b.iter(|| {
            black_box(merge(black_box(input.clone()))).unwrap();
        });
    });
}

/// Benchmark: Merge with degraded shards (simulating node failures).
fn bench_degraded_response(c: &mut Criterion) {
    let hits_per_shard = TARGET_HITS / TARGET_SHARDS;

    let shard_hits: Vec<ShardHitPage> = vec![
        // Healthy shard
        make_shard_response(
            (0..hits_per_shard)
                .map(|i| make_hit(&i.to_string(), (TARGET_HITS - i) as f64 / TARGET_HITS as f64))
                .collect(),
            hits_per_shard as u64,
            15,
        ),
        // Failed shard
        ShardHitPage {
            body: json!({
                "success": false,
                "message": "node unavailable",
            }),
        },
        // Another healthy shard
        make_shard_response(
            (hits_per_shard..2 * hits_per_shard)
                .map(|i| make_hit(&i.to_string(), (TARGET_HITS - i) as f64 / TARGET_HITS as f64))
                .collect(),
            hits_per_shard as u64,
            15,
        ),
    ];

    let input = MergeInput {
        shard_hits,
        offset: 0,
        limit: TARGET_HITS,
        client_requested_score: false,
        facets: None,
    };

    c.bench_function("merge_degraded", |b| {
        b.iter(|| {
            black_box(merge(black_box(input.clone()))).unwrap();
        });
    });
}

criterion_group!(
    benches,
    bench_merge_1000_hits_3_shards,
    bench_varying_hit_count,
    bench_varying_shard_count,
    bench_pagination,
    bench_with_facets,
    bench_with_score_preservation,
    bench_degraded_response
);
criterion_main!(benches);
