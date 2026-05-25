# Performance Benchmarks

This document describes Miroir's performance benchmark infrastructure, as defined in plan §8.

## Running Benchmarks Locally

### Unit Benchmarks (criterion)

Run all unit benchmarks:

```bash
cargo bench -p miroir-core
```

Run specific benchmark suites:

```bash
cargo bench -p miroir-core --bench router_bench
cargo bench -p miroir-core --bench merger_bench
```

View HTML reports:

```bash
open target/criterion/*/report/index.html
```

### Integration Benchmarks

Integration benchmarks require a running docker-compose stack:

```bash
cd examples && docker-compose -f docker-compose-dev.yml up -d
```

Run integration benchmarks:

```bash
cargo test --test integration_bench -- --nocapture --test-threads=1
```

Run only integration benchmarks (skip unit tests):

```bash
cargo test --test integration_bench -- --ignored
```

## Benchmark Targets (Plan §8)

| Benchmark | Target | Status |
|-----------|--------|--------|
| Rendezvous (64 shards, 3 nodes, 10K docs) | < 1 ms | ✅ ~384 µs |
| Merger (1000 hits, 3 shards) | < 1 ms | ⚠️ ~1.07 ms |
| End-to-end search latency vs. single-node | < 2× single-node | 🔄 Pending verification |
| Ingest throughput (1000 docs through Miroir) | > 80% single-node | 🔄 Pending verification |

## CI Integration

### Benchmark Scripts

- `scripts/bench-ci.sh` - CI runner that executes all benchmarks and saves results
- `scripts/bench-compare.sh` - Regression gate that compares results against baseline

### Regression Gate

The CI pipeline runs benchmarks on every PR and compares against the main branch baseline. Any benchmark showing > 20% slowdown triggers a review comment.

To use critcmp manually:

```bash
# Install critcmp
cargo install critcmp

# Export baseline from main
cargo bench -p miroir-core --bench router_bench -- --save-baseline main
critcmp --export baseline.json target/criterion

# Compare PR results
cargo bench -p miroir-core --bench router_bench
critcmp baseline.json target/criterion
```

### Argo Workflow Integration

Benchmarks run as part of the CI/CD pipeline on iad-ci via Argo Workflows. The workflow:

1. Runs `scripts/bench-ci.sh` on main branch to establish baseline
2. Runs `scripts/bench-ci.sh` on PR branch
3. Runs `scripts/bench-compare.sh` to detect regressions
4. Posts comment on PR if regressions detected

## Benchmark Suites

### Router Benchmarks (`benches/router_bench.rs`)

Tests the rendezvous hash-based shard assignment:

- `shard_for_key_single` - Single document shard computation
- `shard_for_key_10k_docs` - Batch shard computation for 10K documents
- `assign_shard_in_group_64_shards` - Assign all 64 shards to nodes
- `full_routing_10k_docs` - Complete routing pipeline (hash → shard → nodes)
- `varying_shard_count` - Performance with 8, 16, 32, 64, 128, 256 shards
- `varying_node_count` - Performance with 2, 3, 4, 5, 8, 10 nodes
- `varying_rf` - Performance with replication factors 1, 2, 3, 5
- `score_single` - Raw score function performance

### Merger Benchmarks (`benches/merger_bench.rs`)

Tests result merging from multiple shards:

- `merge_1000_hits_3_shards` - Primary target: merge 1000 hits from 3 shards
- `varying_hit_count` - Performance with 100, 500, 1000, 5000, 10000 hits
- `varying_shard_count` - Performance with 1, 2, 3, 5, 10 shards
- `pagination` - Deep pagination performance (offset/limit)
- `with_facets` - Facet aggregation performance
- `with_score` - Score preservation overhead
- `degraded` - Performance with failed shards

### Integration Benchmarks (`tests/integration_bench.rs`)

End-to-end performance with real Meilisearch nodes:

- `bench_e2e_search_latency` - Search latency vs standalone (< 2× target)
- `bench_ingest_throughput` - Ingest throughput vs standalone (> 80% target)
- `bench_concurrent_search` - Concurrent search throughput
- `bench_faceted_search` - Faceted search performance
- `bench_pagination` - Deep pagination performance

## Performance Tips

### Improving Router Performance

- The `full_routing_10k_docs` benchmark shows ~384 µs for 10K documents
- Pre-compute shard assignments for hot paths
- Use batch operations when routing multiple documents

### Improving Merger Performance

- The `merge_1000_hits_3_shards` benchmark shows ~1.07 ms
- Limit facets to only those requested by the client
- Consider offset/limit early to avoid processing unnecessary hits

### Integration Test Performance

- Ensure docker-compose stack is healthy before running
- Use `--test-threads=1` to avoid race conditions
- Allow 30+ seconds for document processing

## Adding New Benchmarks

1. Create a new benchmark file in `crates/miroir-core/benches/`
2. Add it to `crates/miroir-core/Cargo.toml`:

```toml
[[bench]]
name = "your_bench"
harness = false
```

3. Use criterion for benchmarking:

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_something(c: &mut Criterion) {
    c.bench_function("something", |b| {
        b.iter(|| {
            black_box(your_function());
        });
    });
}

criterion_group!(benches, bench_something);
criterion_main!(benches);
```

4. Run with `cargo bench -p miroir-core --bench your_bench`
