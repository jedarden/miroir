#!/usr/bin/env python3
"""
Simulate score comparability experiments using a simplified BM25-like model.

This simulation demonstrates the theoretical issue with local statistics:
- Each shard computes IDF using only its local document frequencies
- When shards have very different document distributions, scores diverge
- The merger then produces incorrect global rankings

BM25 components:
- IDF(q) = log((N - df(q) + 0.5) / (df(q) + 0.5))
  where N = total docs in shard/index, df(q) = docs containing term q
- TF(q,d) = f(q,d) * (k1 + 1) / (f(q,d) + k1 * (1 - b + b * |d| / avgdl))
  where f(q,d) = term frequency in doc, |d| = doc length, avgdl = avg doc length
- Score = sum over query terms: IDF(q) * TF(q,d)

For this simulation:
- We use simplified TF = term frequency
- We focus on IDF divergence (the main issue)
- We compute global IDF for ground truth, local IDF per shard for distributed
"""

import argparse
import json
import math
import random
from pathlib import Path
from typing import Dict, List, Set, Tuple
from collections import defaultdict


def load_corpus(corpus_dir: Path) -> Tuple[List[Dict], Dict]:
    """Load corpus and metadata."""
    with open(corpus_dir / "metadata.json") as f:
        metadata = json.load(f)

    docs = []
    with open(corpus_dir / "corpus.jsonl") as f:
        for line in f:
            docs.append(json.loads(line))

    return docs, metadata


def load_queries(query_file: Path) -> List[Dict]:
    """Load query set."""
    queries = []
    with open(query_file) as f:
        for line in f:
            queries.append(json.loads(line))
    return queries


def tokenize(text: str) -> Set[str]:
    """Simple tokenizer."""
    return set(text.lower().split())


def compute_global_stats(docs: List[Dict]) -> Tuple[Dict, int, float]:
    """
    Compute global index statistics.

    Returns:
    - df: term -> document frequency (how many docs contain the term)
    - N: total document count
    - avgdl: average document length
    """
    df = defaultdict(int)
    total_length = 0

    for doc in docs:
        text = f"{doc['title']} {doc['content']}".lower()
        terms = set(text.split())
        for term in terms:
            df[term] += 1
        total_length += len(text.split())

    N = len(docs)
    avgdl = total_length / N if N > 0 else 0

    return dict(df), N, avgdl


def compute_shard_stats(shard_docs: List[Dict]) -> Tuple[Dict, int, float]:
    """Compute statistics for a single shard (same as global but scoped)."""
    return compute_global_stats(shard_docs)


def idf_global(df: int, N: int) -> float:
    """
    Standard IDF formula used by most search engines.

    IDF = log((N - df + 0.5) / (df + 0.5) + 1)
    """
    if df == 0:
        return 0.0
    return math.log((N - df + 0.5) / (df + 0.5) + 1.0)


def score_document_bm25(
    doc: Dict,
    query_terms: Set[str],
    df: Dict,
    N: int,
    avgdl: float,
    k1: float = 1.2,
    b: float = 0.75,
) -> float:
    """
    Compute BM25 score for a document.

    Simplified: we use the IDF component which is the source of the
    score comparability problem. TF is kept simple (term frequency).
    """
    text = f"{doc['title']} {doc['content']}".lower()
    words = text.split()
    doc_length = len(words)

    # Count term frequencies
    tf = defaultdict(int)
    for word in words:
        tf[word] += 1

    score = 0.0
    for term in query_terms:
        if term not in df:
            continue

        # IDF component
        idf = idf_global(df[term], N)

        # TF component (simplified)
        term_freq = tf.get(term, 0)
        tf_norm = term_freq * (k1 + 1) / (term_freq + k1 * (1 - b + b * doc_length / avgdl))

        score += idf * tf_norm

    return score


def simulate_search(
    docs: List[Dict],
    query: Dict,
    stats: Tuple[Dict, int, float],
    limit: int = 100,
) -> Dict:
    """Simulate search on a single index/shard."""
    df, N, avgdl = stats
    query_terms = tokenize(query["q"])

    scores = []
    for doc in docs:
        # Apply filter if present
        if query.get("filter"):
            category_filter = query["filter"].split("=")[1].strip()
            if doc["category"] != category_filter:
                continue

        score = score_document_bm25(doc, query_terms, df, N, avgdl)

        if score > 0:
            scores.append((doc, score))

    # Sort by score descending
    scores.sort(key=lambda x: x[1], reverse=True)

    hits = []
    for doc, score in scores[:limit]:
        hits.append({
            "id": doc["id"],
            "title": doc["title"],
            "score": score,
        })

    return {
        "query_id": query["id"],
        "type": query.get("type", "unknown"),
        "q": query["q"],
        "filter": query.get("filter"),
        "hits": hits,
        "total_hits": len(scores),
    }


def simulate_distributed_search(
    shards: Dict[int, List[Dict]],
    shard_stats: Dict[int, Tuple[Dict, int, float]],
    query: Dict,
    limit: int = 100,
) -> Dict:
    """
    Simulate distributed search across shards.

    This is where the score comparability issue manifests:
    - Each shard computes scores using LOCAL statistics
    - The merger combines results assuming scores are comparable
    - When shards have different document distributions, this breaks
    """
    query_terms = tokenize(query["q"])

    # Collect top results from each shard
    # Each shard returns offset + limit results (we use limit * 2 for safety)
    per_shard_limit = limit * 2
    all_hits = []

    for shard_id, docs in shards.items():
        df, N, avgdl = shard_stats[shard_id]

        # Apply filter at shard level
        if query.get("filter"):
            category_filter = query["filter"].split("=")[1].strip()
            filtered_docs = [d for d in docs if d["category"] == category_filter]
        else:
            filtered_docs = docs

        scores = []
        for doc in filtered_docs:
            score = score_document_bm25(doc, query_terms, df, N, avgdl)
            if score > 0:
                scores.append((doc, score, shard_id))

        scores.sort(key=lambda x: x[1], reverse=True)
        all_hits.extend(scores[:per_shard_limit])

    # Merge: global sort by score (ASSUMING SCORES ARE COMPARABLE)
    all_hits.sort(key=lambda x: x[1], reverse=True)

    hits = []
    for doc, score, shard_id in all_hits[:limit]:
        hits.append({
            "id": doc["id"],
            "title": doc["title"],
            "score": score,
            "shard": shard_id,
        })

    return {
        "query_id": query["id"],
        "type": query.get("type", "unknown"),
        "q": query["q"],
        "filter": query.get("filter"),
        "hits": hits,
        "total_hits": len(all_hits),
        "shards_queried": list(shards.keys()),
    }


RRF_K = 60  # RRF constant, matching merger.rs


def simulate_distributed_search_rrf(
    shards: Dict[int, List[Dict]],
    shard_stats: Dict[int, Tuple[Dict, int, float]],
    query: Dict,
    limit: int = 100,
) -> Dict:
    """
    Simulate distributed search using Reciprocal Rank Fusion.

    RRF score for a document: sum over shards of 1/(k + rank + 1)
    where rank is 0-based position in shard's result list.

    This avoids the score comparability issue entirely because
    RRF only uses rank position, not raw scores.
    """
    query_terms = tokenize(query["q"])
    per_shard_limit = limit * 2

    # Accumulate RRF scores per document
    rrf_scores: Dict[str, float] = defaultdict(float)
    doc_info: Dict[str, Tuple[Dict, int]] = {}  # id -> (doc, shard_id)

    for shard_id, docs in shards.items():
        df, N, avgdl = shard_stats[shard_id]

        if query.get("filter"):
            category_filter = query["filter"].split("=")[1].strip()
            filtered_docs = [d for d in docs if d["category"] == category_filter]
        else:
            filtered_docs = docs

        scores = []
        for doc in filtered_docs:
            score = score_document_bm25(doc, query_terms, df, N, avgdl)
            if score > 0:
                scores.append((doc, score))

        scores.sort(key=lambda x: x[1], reverse=True)

        for rank, (doc, _score) in enumerate(scores[:per_shard_limit]):
            doc_id = doc["id"]
            rrf_contribution = 1.0 / (RRF_K + rank + 1)
            rrf_scores[doc_id] += rrf_contribution
            if doc_id not in doc_info:
                doc_info[doc_id] = (doc, shard_id)

    # Sort by RRF score descending
    sorted_docs = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)

    hits = []
    for doc_id, rrf_score in sorted_docs[:limit]:
        doc, shard_id = doc_info[doc_id]
        hits.append({
            "id": doc_id,
            "title": doc["title"],
            "score": rrf_score,
            "shard": shard_id,
        })

    return {
        "query_id": query["id"],
        "type": query.get("type", "unknown"),
        "q": query["q"],
        "filter": query.get("filter"),
        "hits": hits,
        "total_hits": len(sorted_docs),
        "shards_queried": list(shards.keys()),
        "merge_strategy": "rrf",
    }


def run_experiment(
    corpus_dir: Path,
    query_file: Path,
    output_dir: Path,
    shard_count: int = 10,
    limit: int = 100,
) -> Dict:
    """Run the full experiment."""
    print("Loading corpus...")
    docs, metadata = load_corpus(corpus_dir)

    print(f"  Total documents: {len(docs)}")
    print(f"  Shard count: {shard_count}")

    # Load per-shard data
    shards = {}
    for i in range(shard_count):
        shard_file = corpus_dir / f"shard-{i:02d}.jsonl"
        if shard_file.exists():
            shard_docs = []
            with open(shard_file) as f:
                for line in f:
                    shard_docs.append(json.loads(line))
            shards[i] = shard_docs

    print(f"  Loaded {len(shards)} shards")

    # Compute statistics
    print("\nComputing statistics...")
    global_stats = compute_global_stats(docs)
    print(f"  Global: N={global_stats[1]}, avgdl={global_stats[2]:.1f}")

    shard_stats = {}
    for shard_id, shard_docs in shards.items():
        stats = compute_shard_stats(shard_docs)
        shard_stats[shard_id] = stats
        print(f"  Shard {shard_id}: N={stats[1]}, avgdl={stats[2]:.1f}")

    # Load queries
    print(f"\nLoading queries from {query_file}...")
    queries = load_queries(query_file)
    print(f"  {len(queries)} queries")

    # Run experiments
    output_dir.mkdir(parents=True, exist_ok=True)

    ground_truth_file = output_dir / "ground-truth.jsonl"
    distributed_file = output_dir / "distributed.jsonl"
    rrf_file = output_dir / "distributed-rrf.jsonl"

    print(f"\nRunning experiments...")

    with open(ground_truth_file, "w") as gt_f, \
         open(distributed_file, "w") as dist_f, \
         open(rrf_file, "w") as rrf_f:
        for i, query in enumerate(queries):
            if (i + 1) % 1000 == 0:
                print(f"  Processed {i + 1} queries...")

            # Ground truth: single index with global statistics
            gt_result = simulate_search(docs, query, global_stats, limit)
            gt_f.write(json.dumps(gt_result) + "\n")

            # Distributed: each shard uses local statistics (score-based merge)
            dist_result = simulate_distributed_search(
                shards, shard_stats, query, limit
            )
            dist_f.write(json.dumps(dist_result) + "\n")

            # RRF: rank-based merge (no score comparability needed)
            rrf_result = simulate_distributed_search_rrf(
                shards, shard_stats, query, limit
            )
            rrf_f.write(json.dumps(rrf_result) + "\n")

    print(f"  Completed {len(queries)} queries")
    print(f"\nResults saved to:")
    print(f"  {ground_truth_file}")
    print(f"  {distributed_file}")
    print(f"  {rrf_file}")

    # Save experiment metadata
    exp_meta = {
        "corpus_dir": str(corpus_dir),
        "query_file": str(query_file),
        "shard_count": shard_count,
        "limit": limit,
        "total_queries": len(queries),
        "merge_strategies": ["score", "rrf"],
        "rrf_k": RRF_K,
        "global_stats": {"N": global_stats[1], "avgdl": global_stats[2]},
        "shard_stats": {
            str(k): {"N": v[1], "avgdl": v[2]}
            for k, v in shard_stats.items()
        },
    }

    with open(output_dir / "experiment.json", "w") as f:
        json.dump(exp_meta, f, indent=2)

    return exp_meta


def main():
    parser = argparse.ArgumentParser(description="Simulate score comparability experiments")
    parser.add_argument("--corpus", type=str, default="corpus/",
                       help="Corpus directory")
    parser.add_argument("--queries", type=str, default="queries/queries.jsonl",
                       help="Query file")
    parser.add_argument("--output", type=str,
                       default="results/",
                       help="Output directory")
    parser.add_argument("--shards", type=int, default=10, help="Number of shards")
    parser.add_argument("--limit", type=int, default=100, help="Results per query")

    args = parser.parse_args()

    corpus_dir = Path(args.corpus)
    output_dir = Path(args.output)

    # Generate corpus if needed
    if not (corpus_dir / "corpus.jsonl").exists():
        print("Corpus not found. Generating...")
        import subprocess
        subprocess.run([
            "python3",
            corpus_dir / "generate.py",
            "--count", "100000",
            "--shards", str(args.shards),
        ], check=True)

    # Generate queries if needed
    query_file = Path(args.queries)
    if not query_file.exists():
        print("Queries not found. Generating...")
        import subprocess
        queries_dir = query_file.parent
        subprocess.run([
            "python3",
            queries_dir / "generate.py",
            "--total", "10000",
        ], check=True)

    # Run experiment
    run_experiment(corpus_dir, query_file, output_dir, args.shards, args.limit)

    print("\nTo compare results, run:")
    print(f"  python3 {output_dir}/compare.py {output_dir}/ground-truth.jsonl {output_dir}/distributed.jsonl --verbose")
    print(f"  python3 {output_dir}/compare.py {output_dir}/ground-truth.jsonl {output_dir}/distributed-rrf.jsonl --verbose")


if __name__ == "__main__":
    main()
