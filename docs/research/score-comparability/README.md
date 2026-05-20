# Score Comparability Benchmark

Tests whether `_rankingScore` values from different shards are comparable when documents are distributed unevenly across shards.

## Problem Statement

Meilisearch's ranking pipeline computes scores using local statistics (term frequency, document frequency). When shards have very different document distributions, identical queries may return scores that aren't directly comparable, leading to incorrect merged rankings.

## Experiment Design

1. **Ground truth**: Single Meilisearch index with all documents
2. **Distributed setup**: Same documents sharded across N nodes with intentional skew
3. **Measurement**: Kendall tau (τ) between merged distributed results and ground truth
4. **Pass criterion**: τ ≥ 0.95 on average across 10k random queries

## Corpus Structure

- 100,000 documents total
- 10 shards (shard 0 = normal, shard 1 = 100× normal, shard 9 = 0.01× normal)
- Documents have: id, title, content (synthetic text), category (for filtering)
- 50 unique terms distributed across documents with varying frequencies

## Directory Layout

- `corpus/`: Test document sets (JSONL)
- `queries/`: Generated query sets for experiments
- `results/`: Experimental results and analysis

## Running Experiments

See individual experiment scripts in `results/` directories.
